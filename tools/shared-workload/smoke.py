#!/usr/bin/env python3
"""Provision isolated native/fallback containers and run the full comparison path."""
import argparse
import copy
import json
from pathlib import Path
import shlex
import subprocess
import time
import uuid

import benefits
import experiment


def fixture(output, compiler, backend):
    # Keep event times recent enough for native TSDB ingestion, and all SQL
    # panes aligned. MetricsQL and PromQL are compared to their own references.
    start = ((int(time.time()) - 60) // 4) * 4000 + 1000
    query = 'sum_over_time(data[4s])'
    queries = [{'name': 'temporal_sum', 'promql': query, 'metricsql': query,
                'clickhouse_sql': f'SELECT sum(value) AS value FROM raw_samples WHERE ts_ms >= {start + 4000} AND ts_ms < {start + 8000}'}]
    queries.append({'name': 'temporal_sum_plus_one', 'promql': query + ' + 1', 'metricsql': query + ' + 1',
                    'clickhouse_sql': queries[0]['clickhouse_sql'].replace('sum(value)', 'sum(value) + 1')})
    manifest = {'end_ms': start + 7000, 'queries': queries}
    experiment.save(output / 'manifest.json', manifest)
    batches = []
    for index in range(2):
        om, sql = output / f'batch-{index}.openmetrics', output / f'batch-{index}.jsonl'
        samples = [(start + step * 1000, step + 1) for step in range(index * 4, (index + 1) * 4)]
        om.write_text(''.join(f'data {value} {ts / 1000:.3f}\n' for ts, value in samples) + '# EOF\n')
        sql.write_text(''.join(json.dumps({'ts_ms': ts, 'value': value}) + '\n' for ts, value in samples))
        batches.append({'openmetrics': str(om), 'jsonl': str(sql), 'start_ms': start + index * 4000, 'end_ms': start + (index + 1) * 4000})
    root = Path(__file__).resolve().parents[2]
    template = json.loads((root / 'docs/examples/asapquery-planning-snapshot.json').read_text())
    now = int(time.time() * 1000)
    template['query_workload']['repeating_queries'] = [{
        'query': query, 'demand': {'fixed_interval_at': {'interval': 4000, 'evaluation_phase': 0}},
        'requirements': {'accuracy': 'implicit_exact', 'response_latency': 'unspecified'},
        'predictability': {'predictable': {'known_at': None}},
        'time_selection': {'scope': 'real_time', 'lookback': 4000, 'as_of': None}}]
    extra = copy.deepcopy(template['query_workload']['repeating_queries'][0])
    extra['query'] += ' + 1'
    template['query_workload']['repeating_queries'].append(extra)
    template['environment'].update(observed_at_unix_ms=now, activation_unix_ms=now, max_evidence_age_ms=600000)
    implementation = template['implementation']
    implementation.update(evidence_observed_at_unix_ms=now, evidence_valid_for_ms=600000,
                          source_sample_interval_ms=1000, query_staleness_margin_ms=10000)
    implementation['implementation_cost'].update(observed_at_unix_ms=now, valid_for_ms=600000)
    # These declared costs are a functional-test seed, not measured calibration.
    # The experiment reports observed execution costs independently of them.
    template['implementation']['implementation_cost']['model_version'] = 'functional-smoke-declared-costs'
    for engine in ('prometheus', 'victoriametrics'):
        snapshot = copy.deepcopy(template)
        # Canonical workload uses promql; compile_metricsql selects the native frontend.
        snapshot['query_workload']['language'] = 'promql'
        experiment.save(output / f'{engine}.json', snapshot)
    # Read the pinned revision from Cargo.lock, the same source as build.rs.
    import re
    lock = (root / 'Cargo.lock').read_text()
    revision = re.search(r'ASAPPlanner\?rev=[^#]+#([a-f0-9]{40})', lock).group(1)
    sql = {'envelope': {'plan_id': 731, 'plan_version': 1, 'generated_at_unix_ms': now,
                       'activation_unix_ms': now, 'expiry_unix_ms': None,
                       'backend_compat': 'asap-query-backend.v1', 'planner_revision': revision,
                       'capability_snapshot_id': 'comparison-functional-smoke'},
           'tables': {'raw_samples': {'columns': [
               {'name': 'ts_ms', 'dtype': 'int64', 'nullable': False, 'table': None},
               {'name': 'value', 'dtype': 'float64', 'nullable': False, 'table': None}],
               'time_index': 0, 'group_keys': []}}, 'accuracy': 'Exact',
           'queries': [{'sql': q['clickhouse_sql'].replace(str(start + 4000), str(start)).replace(str(start + 8000), str(start + 4000)),
                        'start_ms': start, 'end_ms': start + 4000, 'cumulative': False} for q in queries]}
    experiment.save(output / 'clickhouse.json', sql)
    # This query is deliberately absent from all three publications. Successful
    # native fallback must remain distinct from the warm coverage above.
    queries.append({'name': 'unplanned_window', 'promql': 'sum_over_time(data[3s])',
                    'metricsql': 'sum_over_time(data[3s])',
                    'clickhouse_sql': f'SELECT sum(value) AS value FROM raw_samples WHERE ts_ms >= {start + 5000} AND ts_ms < {start + 8000}'})
    experiment.save(output / 'manifest.json', manifest)
    return {'compiler': str(compiler), 'data_plane': str(backend), 'manifest': str(output / 'manifest.json'),
            'table': 'raw_samples', 'columns_sql': 'ts_ms Int64, value Float64', 'batches': batches,
            'repetitions': 5, 'engines': {e: {'planning_input': str(output / f'{e}.json')} for e in benefits.ENGINES}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker', default='docker', help='Docker argv prefix, e.g. "sudo -n docker"')
    parser.add_argument('--compiler', type=Path, required=True)
    parser.add_argument('--data-plane', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    for engine in benefits.ENGINES:
        parser.add_argument('--' + engine + '-image', required=True, help='immutable local image ID or digest')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    config = fixture(output, args.compiler.resolve(), args.data_plane.resolve())
    docker = shlex.split(args.docker)
    names, inspections = [], []
    prometheus_config = output / 'prometheus.yml'
    prometheus_config.write_text('global:\n  scrape_interval: 1h\nscrape_configs: []\n')
    try:
        for engine in benefits.ENGINES:
            image = getattr(args, engine + '_image')
            # Resolve once; run this immutable ID, never a moving tag twice.
            image_id = subprocess.check_output(docker + ['image', 'inspect', image, '--format', '{{.Id}}'], text=True).strip()
            for arm in ('native', 'fallback'):
                name = 'asap-benefits-' + uuid.uuid4().hex[:12]
                internal = {'prometheus': 9090, 'clickhouse': 8123, 'victoriametrics': 8428}[engine]
                command = docker + ['run', '-d', '--name', name, '--pull', 'never', '--cpus', '2', '--memory', '2g',
                                    '-p', f'127.0.0.1::{internal}']
                if engine == 'prometheus':
                    command += ['-v', f'{prometheus_config}:/etc/prometheus/prometheus.yml:ro']
                if engine == 'clickhouse':
                    command += ['-e', 'CLICKHOUSE_USER=asap_test', '-e', 'CLICKHOUSE_PASSWORD=asap_test_local']
                command += [image_id]
                if engine == 'prometheus':
                    command += ['--config.file=/etc/prometheus/prometheus.yml', '--web.enable-remote-write-receiver']
                if engine == 'victoriametrics':
                    command += ['-search.latencyOffset=0s']
                started = time.perf_counter_ns()
                subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
                names.append(name)
                info = json.loads(subprocess.check_output(docker + ['inspect', name]))[0]
                inspections.append(info)
                mapped = info['NetworkSettings']['Ports'][f'{internal}/tcp'][0]['HostPort']
                endpoint = {'url': f'http://127.0.0.1:{mapped}', 'pid': info['State']['Pid']}
                if engine == 'clickhouse':
                    endpoint.update(user='asap_test', password='asap_test_local')
                config['engines'][engine][arm] = endpoint
                path = '/ping' if engine == 'clickhouse' else ('/-/ready' if engine == 'prometheus' else '/health')
                deadline = time.monotonic() + 60
                while benefits.http(endpoint['url'] + path, timeout=1)['status'] != 200:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f'{engine} readiness timeout')
                    time.sleep(.2)
                process = experiment.process_snapshot(endpoint['pid'])
                endpoint['startup_cost'] = {'wall_ns': time.perf_counter_ns() - started, 'cpu_ns': process['cpu_ns'] if process else None}
        experiment.save(output / 'containers.json', inspections)
        experiment.save(output / 'experiment.json', config)
        report = experiment.run(config, output / 'run')
        print(json.dumps({'passed': report['passed'], 'summary': report['summary']}, indent=2))
        valid = report['passed']
        for record in report['records']:
            expected_route = 'exact_fallback' if record['name'] == 'unplanned_window' else 'warm'
            expected_value = {'temporal_sum': 26, 'temporal_sum_plus_one': 27, 'unplanned_window': 21}[record['name']]
            for engine, result in record['engines'].items():
                valid &= result['execution'] == expected_route
                for arm in ('native', 'asap'):
                    body = result[arm]['body']
                    values = [r['value'] for r in body['data']] if engine == 'clickhouse' else [float(r['value'][1]) for r in body['data']['result']]
                    valid &= values == [expected_value]
        experiment.save(output / 'acceptance.json', {'passed': bool(valid), 'expected': 'two warm queries and one unplanned fallback per engine, with known nonempty values'})
        if not valid:
            raise SystemExit('smoke requires correct values, warm supported queries, and explicit fallback for the unplanned query')
    finally:
        for name in reversed(names):
            logs = subprocess.run(docker + ['logs', name], capture_output=True, text=True)
            (output / f'{name}.log').write_text(logs.stdout + logs.stderr)
            subprocess.run(docker + ['rm', '-f', name], stdout=subprocess.DEVNULL, check=False)


if __name__ == '__main__':
    main()
