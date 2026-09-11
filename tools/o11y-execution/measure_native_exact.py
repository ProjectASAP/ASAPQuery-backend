#!/usr/bin/env python3
"""Measure a fresh native VictoriaMetrics baseline without a backend proxy."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import time

import calibrate_runtime as calibration
import replay as runner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--victoriametrics', type=Path, required=True)
    parser.add_argument('--metrics', type=Path, required=True)
    parser.add_argument('--queries', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cpu-affinity', required=True)
    parser.add_argument('--port', type=int, default=19450)
    parser.add_argument('--repetitions', type=int, default=60)
    parser.add_argument('--exact-cache-bytes', type=int, default=268435456)
    parser.add_argument('--disable-result-cache', action='store_true')
    args = parser.parse_args()
    if args.repetitions < 1 or args.exact_cache_bytes <= 0:
        parser.error('positive repetitions and cache budget required')
    cpus = {int(value) for value in args.cpu_affinity.split(',')}
    inventory = calibration.input_inventory(args.metrics)
    corpus = json.loads(args.queries.read_text())
    if not corpus.get('upstream_revision') or not corpus.get('queries'):
        raise ValueError('versioned nonempty corpus required')
    args.output.mkdir(parents=True, exist_ok=False)
    url = f'http://127.0.0.1:{args.port}'
    command = calibration.exact_service_command(args, args.output, args.port)
    report = {'engine':'victoriametrics','command':command,'result_cache_disabled':args.disable_result_cache,
              'data_sha256':hashlib.sha256(args.metrics.read_bytes()).hexdigest(),
              'binary_sha256':hashlib.sha256(args.victoriametrics.read_bytes()).hexdigest(),
              'samples':sum(inventory[0].values()),'queries':{},'phases':{},
              'scope':'native engine only; first workload query follows full input visibility validation'}
    child = None
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with (args.output/'native.log').open('w') as log:
            child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                     preexec_fn=lambda: os.sched_setaffinity(0, cpus))
        children = {'native_exact':child}
        calibration.wait_ready(url+'/health',child)
        before = calibration.snapshots(children)
        report['startup'] = before
        started = time.perf_counter_ns()
        runner.PROCESS_IDS.clear()
        runner.PROCESS_IDS.update({'native_exact':child.pid})
        runner.ingest_sample_file(args.metrics,[url],args.output)
        flush = runner._http_request(url+'/internal/force_flush')
        runner.write_json(args.output/'exact-flush.json',flush)
        if flush['http_status'] != 200:
            raise RuntimeError('native flush failed')
        calibration.verify_vm_visibility(url,inventory,args.output)
        after = calibration.snapshots(children)
        report['phases']['ingest_and_build'] = calibration.phase(args.output,'ingest_and_build',before,after,time.perf_counter_ns()-started)
        for query in corpus['queries']:
            before, started = calibration.snapshots(children), time.perf_counter_ns()
            records = []
            for repeat in range(args.repetitions):
                result = runner._http_request(url+'/api/v1/query?'+calibration.query_parameters(query,args.disable_result_cache))
                records.append({**query,'repetition':repeat,**result})
                if result['http_status'] != 200 or result['response'].get('status') != 'success':
                    raise RuntimeError('native query failed: '+query['query'])
            after = calibration.snapshots(children)
            measurement = calibration.phase(args.output,'query-'+query['id'],before,after,time.perf_counter_ns()-started)
            records_path = args.output/('queries-'+query['id']+'.json')
            runner.write_json(records_path,records)
            report['queries'][query['id']] = {**measurement,'evaluations':len(records),'raw_measurement_file':str(records_path.resolve())}
        report['final_processes'] = calibration.snapshots(children)
        child.terminate()
        child.wait(timeout=30)
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        report['lifecycle_cpu_ns'] = int((usage_after.ru_utime+usage_after.ru_stime-usage_before.ru_utime-usage_before.ru_stime)*1e9)
        report['storage_after_shutdown_bytes'] = calibration.file_bytes(args.output/'exact-data')
        runner.write_json(args.output/'measurement.json',report)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=30)


if __name__ == '__main__':
    main()
