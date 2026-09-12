#!/usr/bin/env python3
"""Compile, install, materialize, and compare three native/ASAP deployments.

Native and fallback services must be separately provisioned, empty, isolated
instances. This runner owns its three backend children and a fresh SQL database.
"""
import argparse
import base64
from contextlib import contextmanager
import hashlib
import itertools
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import time
import uuid

import benefits
from compare import process_snapshot, process_delta
from replay import iter_samples, encode_write
from process_lifecycle import stop


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def require(result):
    if result.get('status') is None or not 200 <= result['status'] < 300:
        raise RuntimeError(str(result))
    return result.get('body')


def post(url, value=None):
    return require(benefits.http(url, json.dumps(value).encode() if value is not None else b'',
                                 {'Content-Type': 'application/json'}))


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Measurements:
    def __init__(self, path):
        self.path, self.rows = path, []

    @contextmanager
    def phase(self, engine, arm, phase, pids):
        before = [process_snapshot(pid) for pid in pids]
        started = time.perf_counter_ns()
        row = {'engine': engine, 'arm': arm, 'phase': phase, 'complete': False}
        try:
            yield row
            row['complete'] = True
        finally:
            after = [process_snapshot(pid) for pid in pids]
            deltas = [process_delta(a, b) for a, b in zip(before, after)]
            row.update(wall_ns=time.perf_counter_ns() - started, processes=deltas,
                       cpu_ns=sum(v['cpu_ns'] for v in deltas) if deltas and all(deltas) else None)
            self.rows.append(row)
            save(self.path, self.rows)


def sql(endpoint, database, statement):
    import urllib.parse
    return require(benefits.http(endpoint['url'].rstrip('/') + '/?' + urllib.parse.urlencode({'database': database}),
                                 statement.encode(), endpoint.get('headers'), timeout=120))


def remote_write(endpoint, path):
    with Path(path).open() as source:
        rows = iter_samples(source)
        while batch := list(itertools.islice(rows, 5000)):
            require(benefits.http(endpoint['url'].rstrip('/') + '/api/v1/write', encode_write(batch), {
                **endpoint.get('headers', {}), 'Content-Type': 'application/x-protobuf',
                'Content-Encoding': 'snappy', 'X-Prometheus-Remote-Write-Version': '0.1.0'}, timeout=120))


def load_sql(endpoint, database, table, path):
    with Path(path).open() as source:
        while batch := list(itertools.islice(source, 5000)):
            sql(endpoint, database, f'INSERT INTO {table} FORMAT JSONEachRow\n' + ''.join(batch))


def wait_ready(api, child):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise RuntimeError('backend exited before readiness; inspect backend.log')
        if benefits.http(api + '/api/v1/health', timeout=1)['status'] == 200:
            return
        time.sleep(.1)
    raise TimeoutError('backend readiness timeout')


def backfill(api, install, database, table, start_ms, end_ms):
    for identity, definition in install['summary_catalog']['materializations'].items():
        pane_ms = definition['window_layout']['pane_secs'] * 1000
        if (start_ms - definition['pane_origin_ms']) % pane_ms or (end_ms - start_ms) % pane_ms:
            raise ValueError('backfill input range must align with materialized panes')
        post(api + '/api/v1/db/backfill', {'agg_id': int(identity), 'start_ms': start_ms, 'end_ms': end_ms,
             'source': {'ClickHouse': {'database': database, 'table': table}}, 'windows_total': (end_ms - start_ms) // pane_ms})
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        jobs = require(benefits.http(api + '/api/v1/db/backfill/jobs'))
        statuses = [job['status'] for job in jobs['jobs']]
        if 'failed' in statuses:
            raise RuntimeError(str(jobs))
        if not install['summary_catalog']['materializations'] or (statuses and all(s == 'complete' for s in statuses)):
            return jobs
        time.sleep(.1)
    raise TimeoutError('backfill completion timeout')


def validate(config):
    urls, pids = [], []
    if not config.get('batches'):
        raise ValueError('at least one input batch is required')
    for engine in benefits.ENGINES:
        for arm in ('native', 'fallback'):
            endpoint = config['engines'][engine][arm]
            urls.append(endpoint['url'].rstrip('/'))
            if endpoint.get('user'):
                credential = endpoint['user'] + ':' + endpoint.get('password', '')
                endpoint.setdefault('headers', {})['Authorization'] = 'Basic ' + base64.b64encode(credential.encode()).decode()
            if endpoint.get('pid'):
                pids.append(endpoint['pid'])
    if len(urls) != len(set(urls)) or len(pids) != len(set(pids)):
        raise ValueError('native and fallback arms require distinct isolated service URLs and PIDs')
    if not config['table'].replace('_', '').isalnum() or config['table'][0].isdigit():
        raise ValueError('table must be a simple SQL identifier')


def run(config, output):
    validate(config)
    # Validate the complete metric stream before any external write.
    def sample_lines():
        for batch in config['batches']:
            with Path(batch['openmetrics']).open() as source:
                yield from source
    sample_count = sum(1 for _ in iter_samples(sample_lines()))
    output.mkdir(parents=True, exist_ok=False)
    measurements = Measurements(output / 'phases.json')
    children, logs, databases, deployments, endpoints = [], [], [], {}, {}
    database = 'asap_comparison_' + uuid.uuid4().hex
    artifacts = [config['compiler'], config['data_plane'], config['manifest']]
    artifacts += [e['planning_input'] for e in config['engines'].values()]
    artifacts += [b[k] for b in config['batches'] for k in ('openmetrics', 'jsonl')]
    save(output / 'provenance.json', {'inputs': {str(p): sha256(p) for p in artifacts},
         'database': database, 'sample_count': sample_count, 'scope': 'finite input replay; native service startup included only when supplied by provisioner; includes incremental materialization batches and fallback; no sustained-load or planner-optimality claim'})
    for engine in benefits.ENGINES:
        for arm in ('native', 'fallback'):
            startup = config['engines'][engine][arm].get('startup_cost')
            if startup:
                measurements.rows.append(dict(startup, engine=engine, arm=arm, phase='native_service_startup', complete=True))
    lifetime_before = {engine: {arm: process_snapshot(config['engines'][engine][arm].get('pid'))
                       for arm in ('native', 'fallback')} for engine in benefits.ENGINES}
    try:
        for engine in benefits.ENGINES:
            settings = config['engines'][engine]
            folder = output / engine
            folder.mkdir()
            planning_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            with measurements.phase(engine, 'asap', 'planning', []) as phase:
                compiled = subprocess.run([config['compiler'], engine, settings['planning_input']],
                    capture_output=True, text=True, timeout=300)
                (folder / 'planning.stderr').write_text(compiled.stderr)
                compiled.check_returncode()
                plan = json.loads(compiled.stdout)
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                phase['compiler_cpu_ns'] = round((after.ru_utime + after.ru_stime - planning_before.ru_utime - planning_before.ru_stime) * 1e9)
                save(folder / 'planning.json', plan)
            install = plan['install']
            save(folder / 'install.json', install)
            api_port, query_port = port(), port()
            api = f'http://127.0.0.1:{api_port}'
            command = [config['data_plane'], '--http-port', str(api_port), '--output-dir', str(folder / 'state'),
                       '--precompute-allowed-lateness-ms', '0', '--precompute-flush-interval-ms', '25',
                       '--persistence-delete-older-than-secs', '0']
            fallback = settings['fallback']
            if engine == 'clickhouse':
                bootstrap = folder / 'bootstrap.yaml'
                bootstrap.write_text('aggregations: []\n')
                command += ['--streaming-config', str(bootstrap), '--clickhouse-http-port', str(query_port),
                    '--clickhouse-url', fallback['url'], '--clickhouse-database', database,
                    '--clickhouse-backfill-database', database, '--clickhouse-backfill-table', config['table'],
                    '--enable-backfill-worker']
                for key in ('user', 'password'):
                    if fallback.get(key):
                        command += ['--clickhouse-' + key, fallback[key]]
                for arm in ('native', 'fallback'):
                    endpoint = settings[arm]
                    with measurements.phase(engine, arm, 'schema', [endpoint.get('pid')]):
                        sql(endpoint, 'default', f'CREATE DATABASE {database}')
                        databases.append(endpoint)
                        sql(endpoint, database, f'CREATE TABLE {config["table"]} ({config["columns_sql"]}) ENGINE = MergeTree ORDER BY tuple()')
            else:
                command += ['--profile', 'asapquery', '--physical-plan', str(folder / 'install.json'),
                            '--prometheus-server', fallback['url'], '--forward-unsupported-queries']
                if engine == 'victoriametrics':
                    command += ['--victoriametrics-http-port', str(query_port), '--victoriametrics-url', fallback['url']]
            log = (folder / 'backend.log').open('w')
            logs.append(log)
            start = time.perf_counter_ns()
            child = subprocess.Popen(command, stdout=log, stderr=log)
            children.append(child)
            wait_ready(api, child)
            if engine == 'clickhouse':
                post(api + '/api/v1/physical-plan', install)
                envelope = install['precompute_plan']['envelope']
                post(api + '/api/v1/physical-plan/activate', {k: envelope[k] for k in ('plan_id', 'plan_version')})
            status = require(benefits.http(api + '/api/v1/physical-plan/status'))
            save(folder / 'status.json', status)
            envelope = install['precompute_plan']['envelope']
            if not any(p['phase'] == 'active' and all(p[k] == envelope[k] for k in ('plan_id', 'plan_version')) for p in status['plans']):
                raise RuntimeError('compiled generation was not activated')
            startup = process_snapshot(child.pid)
            measurements.rows.append({'engine': engine, 'arm': 'asap', 'phase': 'startup_install', 'complete': True,
                 'wall_ns': time.perf_counter_ns() - start, 'cpu_ns': startup['cpu_ns'] if startup else None})
            endpoints[engine] = {'native': dict(settings['native'], database=database, pids=[settings['native'].get('pid')]),
                'asap': {'url': api if engine == 'prometheus' else f'http://127.0.0.1:{query_port}',
                         'database': database, 'headers': fallback.get('headers', {}) if engine == 'clickhouse' else {},
                         'pids': [child.pid, fallback.get('pid')]}}
            deployments[engine] = {'api': api, 'install': install, 'pid': child.pid}
        for index, batch in enumerate(config['batches']):
            phase_name = 'build' if index == 0 else 'maintenance'
            for engine in benefits.ENGINES:
                settings, deployment = config['engines'][engine], deployments[engine]
                with measurements.phase(engine, 'native', phase_name, [settings['native'].get('pid')]):
                    if engine == 'clickhouse':
                        load_sql(settings['native'], database, config['table'], batch['jsonl'])
                    else:
                        remote_write(settings['native'], batch['openmetrics'])
                        if engine == 'victoriametrics':
                            post(settings['native']['url'].rstrip('/') + '/internal/force_flush')
                with measurements.phase(engine, 'asap', phase_name, [deployment['pid'], settings['fallback'].get('pid')]):
                    if engine == 'clickhouse':
                        load_sql(settings['fallback'], database, config['table'], batch['jsonl'])
                    else:
                        remote_write(settings['fallback'], batch['openmetrics'])
                        if engine == 'victoriametrics':
                            post(settings['fallback']['url'].rstrip('/') + '/internal/force_flush')
                        remote_write({'url': deployment['api']}, batch['openmetrics'])
                        if index == len(config['batches']) - 1:
                            drained = post(deployment['api'] + '/api/v1/precompute/drain')
                            if drained.get('complete') is not True:
                                raise RuntimeError('finite materialization drain incomplete')
                        else:
                            # Final drain is the completion barrier; no mid-stream drain may seal input.
                            time.sleep(.1)
        # The current backfill admission boundary treats historical outputs as
        # first-seen input. A later second job is rejected. Use one complete,
        # non-overlapping finite backfill; do not weaken the live overlap guard.
        deployment = deployments['clickhouse']
        with measurements.phase('clickhouse', 'asap', 'materialization',
                                [deployment['pid'], config['engines']['clickhouse']['fallback'].get('pid')]):
            backfill(deployment['api'], deployment['install'], database, config['table'],
                     config['batches'][0]['start_ms'], config['batches'][-1]['end_ms'])
        manifest = json.loads(Path(config['manifest']).read_text())
        with (output / 'requests.jsonl').open('w') as trace:
            def record(row):
                trace.write(json.dumps(row) + '\n')
                trace.flush()
            report = benefits.run(endpoints, manifest, config.get('repetitions', 10), config.get('timeout', 30),
                                  config.get('relative_tolerance', 1e-9), config.get('absolute_tolerance', 1e-12), record_sink=record)
        save(output / 'phases.json', measurements.rows)
        from costs import summarize_costs
        lifetime = {}
        from costs import complete_sum
        for engine in benefits.ENGINES:
            settings = config['engines'][engine]
            after = {arm: process_snapshot(settings[arm].get('pid')) for arm in ('native', 'fallback')}
            delta = {arm: process_delta(lifetime_before[engine][arm], after[arm]) for arm in after}
            backend = process_snapshot(deployments[engine]['pid'])
            compiler_cpu = [p['compiler_cpu_ns'] for p in measurements.rows if p['engine'] == engine and p['phase'] == 'planning']
            lifetime[engine] = {
                'native_cpu_ns': (after['native']['cpu_ns'] if settings['native'].get('startup_cost') and after['native']
                                  else delta['native']['cpu_ns'] if delta['native'] else None),
                'asap_plus_fallback_and_planner_cpu_ns': complete_sum([
                    backend['cpu_ns'] if backend else None,
                    (after['fallback']['cpu_ns'] if settings['fallback'].get('startup_cost') and after['fallback']
                     else delta['fallback']['cpu_ns'] if delta['fallback'] else None), *compiler_cpu]),
                'final_processes': dict(after, backend=backend),
                'backend_state_directory_bytes': sum(p.stat().st_size for p in (output / engine / 'state').rglob('*') if p.is_file()),
                'native_storage_bytes': storage_bytes(settings['native'].get('storage_path')),
                'fallback_storage_bytes': storage_bytes(settings['fallback'].get('storage_path')),
            }
        report['costs'] = summarize_costs(measurements.rows, report['records'], lifetime)
        report['costs']['clickhouse']['cpu_break_even'] = {'refreshes': None, 'reason': 'SQL continuous summary maintenance is not measured; one finite backfill after loading all batches'}
        save(output / 'report.json', report)
        return report
    except Exception as error:
        save(output / 'failure.json', {'error': str(error), 'type': type(error).__name__})
        raise
    finally:
        cleanup_errors = []
        for child in reversed(children):
            try:
                stop(child)
            except Exception as error:
                cleanup_errors.append(str(error))
        for log in logs:
            log.close()
        for endpoint in databases:
            try:
                sql(endpoint, 'default', f'DROP DATABASE {database}')
            except Exception as error:
                cleanup_errors.append(str(error))
        save(output / 'cleanup.json', {'errors': cleanup_errors})
        if cleanup_errors:
            raise RuntimeError('experiment cleanup incomplete; inspect cleanup.json')


def storage_bytes(path):
    if path is None:
        return None
    try:
        return sum(p.stat().st_size for p in Path(path).rglob('*') if p.is_file()) if Path(path).is_dir() else None
    except OSError:
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = run(json.loads(args.config.read_text()), args.output.resolve())
    raise SystemExit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
