#!/usr/bin/env python3
"""Execute the original SQL corpus against automatic publications and native CH."""
import argparse
import base64
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def process_metrics(pid):
    if not pid:
        return None
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    status = {}
    for line in Path(f'/proc/{pid}/status').read_text().splitlines():
        if line.startswith(('VmRSS:', 'VmHWM:')):
            name, value, _ = line.split()
            status[name[:-1]] = int(value) * 1024
    return {'cpu_ticks': int(stat[11]) + int(stat[12]), 'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'), **status}


def cpu_delta(before, after):
    return None if before is None or after is None else after['cpu_ticks'] - before['cpu_ticks']


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('matrix', type=Path)
    ap.add_argument('backend', type=Path)
    ap.add_argument('output', type=Path)
    ap.add_argument('--clickhouse', default='http://127.0.0.1:28123')
    ap.add_argument('--database', default='asap_o11y27_eval')
    ap.add_argument('--ids', help='comma-separated subset; the installed Planner choice is unchanged')
    ap.add_argument('--repetitions', type=int, default=0)
    ap.add_argument('--container-image', help='pin an image digest to enforce matched2CPU/4GiB budgets')
    ap.add_argument('--clickhouse-pid', type=int)
    ap.add_argument('--only-backend', action='store_true')
    ap.add_argument('--expected-result', type=Path)
    args = ap.parse_args()
    if args.only_backend and (not args.expected_result or not args.ids or ',' in args.ids):
        ap.error('--only-backend requires one --ids value and --expected-result')
    args.output.mkdir(parents=True, exist_ok=False)
    user, password = os.environ['CLICKHOUSE_USER'], os.environ['CLICKHOUSE_PASSWORD']
    auth = 'Basic ' + base64.b64encode(f'{user}:{password}'.encode()).decode()

    def request(url, body=None, data=None):
        headers = {'Authorization': auth}
        if data is not None:
            body = json.dumps(data).encode()
            headers['Content-Type'] = 'application/json'
        if isinstance(body, str):
            body = body.encode()
        start = time.perf_counter_ns()
        try:
            response = urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers), timeout=180)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            result = {'status': response.status, 'headers': {key.lower(): value for key, value in response.headers.items()}, 'body': response.read().decode()}
        result['elapsed_ns'] = time.perf_counter_ns() - start
        return result

    def require(result):
        if not 200 <= result['status'] < 300:
            raise RuntimeError(str(result))
        return json.loads(result['body']) if result['body'] else None

    results = []
    for query in json.loads(args.matrix.read_text())['queries']:
        if args.ids and query['id'] not in args.ids.split(','):
            continue
        directory = args.output / query['id']
        directory.mkdir()
        bootstrap = directory / 'bootstrap.yaml'
        bootstrap.write_text('aggregations: []\n')
        api, sql = port(), port()
        command = [str(args.backend.resolve()), '--streaming-config', str(bootstrap), '--http-port', str(api), '--clickhouse-http-port', str(sql), '--clickhouse-url', args.clickhouse, '--clickhouse-database', args.database, '--clickhouse-backfill-table', 'raw_samples', '--clickhouse-backfill-database', args.database, '--clickhouse-user', user, '--clickhouse-password', password, '--enable-backfill-worker', '--precompute-allowed-lateness-ms', '0', '--precompute-flush-interval-ms', '50', '--persistence-delete-older-than-secs', '0', '--output-dir', str(directory / 'state')]
        container = None
        if args.container_image:
            container = f'asap-o11y27-{args.output.name}-{query["id"]}'
            runtime = args.backend.resolve().parent.parent
            command = ['docker', 'run', '--name', container, '--user', f'{os.getuid()}:{os.getgid()}', '--network', 'host', '--pid', 'host', '--cpuset-cpus', '60,61', '--cpus', '2', '--memory', '4g', '-v', f'{runtime}:{runtime}:ro', '-v', f'{args.output.resolve()}:{args.output.resolve()}', '--entrypoint', command[0], args.container_image, *command[1:]]
        with (directory / 'backend.log').open('w') as log:
            process = subprocess.Popen(command, stdout=log, stderr=log, env=dict(os.environ, RUST_LOG='data_plane=info'))
            try:
                base = f'http://127.0.0.1:{api}'
                for _ in range(200):
                    if process.poll() is not None:
                        raise RuntimeError('backend exited: inspect backend.log')
                    try:
                        if request(base + '/api/v1/health')['status'] == 200:
                            break
                    except OSError:
                        pass
                    time.sleep(.05)
                else:
                    raise RuntimeError('backend did not start')
                backend_pid = int(subprocess.check_output(['docker', 'inspect', container, '--format', '{{.State.Pid}}'])) if container else process.pid
                build_before = {'backend': process_metrics(backend_pid), 'clickhouse': process_metrics(args.clickhouse_pid)}
                build_started = time.perf_counter_ns()
                jobs = None
                if query['publication'] == 'pass':
                    install = query['install']
                    require(request(base + '/api/v1/physical-plan', data=install))
                    envelope = install['precompute_plan']['envelope']
                    require(request(base + '/api/v1/physical-plan/activate', data={'plan_id': envelope['plan_id'], 'plan_version': envelope['plan_version']}))
                    for identity in install['summary_catalog']['materializations']:
                        require(request(base + '/api/v1/db/backfill', data={'agg_id': int(identity), 'start_ms': query['window_start_ms'], 'end_ms': query['window_end_ms'], 'source': {'ClickHouse': {'database': args.database, 'table': 'raw_samples'}}, 'windows_total': 1}))
                    for _ in range(2400):
                        jobs = require(request(base + '/api/v1/db/backfill/jobs'))
                        statuses = [job['status'] for job in jobs['jobs']]
                        if statuses and all(status == 'complete' for status in statuses):
                            break
                        if 'failed' in statuses:
                            raise RuntimeError(str(jobs))
                        time.sleep(.25)
                    else:
                        raise RuntimeError('backfill timed out')
                build_ns = time.perf_counter_ns() - build_started
                build_after = {'backend': process_metrics(backend_pid), 'clickhouse': process_metrics(args.clickhouse_pid)}
                params = urllib.parse.urlencode({'database': args.database, 'default_format': 'JSON', 'max_threads': 2, 'use_query_cache': 0})
                if args.only_backend:
                    reference = json.loads(args.expected_result.read_text())
                    if reference['id'] != query['id']:
                        raise ValueError('reference query identity mismatch')
                    exact = dict(reference['exact'], elapsed_ns=None)
                else:
                    exact = request(args.clickhouse + '/?' + params, query['sql'])
                actual = request(f'http://127.0.0.1:{sql}/?' + params, query['sql'])
                equal = False
                if actual['status'] == exact['status'] == 200:
                    left, right = json.loads(actual['body']), json.loads(exact['body'])
                    equal = left['meta'] == right['meta'] and left['data'] == right['data']
                result = {'id': query['id'], 'publication': query['publication'], 'route': actual['headers'].get('x-asap-execution', 'unmarked'), 'execution': actual['headers'].get('x-asap-execution', 'unmarked') if actual['status'] == 200 else 'failed', 'equal': equal, 'install_and_backfill_ns': build_ns, 'backfill_jobs': jobs, 'backend': actual, 'exact': exact, 'timing_scope': 'debug correctness run, not performance evidence'}
                result['build_resources'] = {'before': build_before, 'after': build_after}
                result['state_directory_bytes'] = sum(path.stat().st_size for path in (directory / 'state').rglob('*') if path.is_file())
                if args.repetitions:
                    result['timing_scope'] = ('ASAP-only deployment replay against a prior validated reference; ' if args.only_backend else 'alternating backend/native replay; ') + 'source-data ingestion and offline planning costs are not measured here'
                    if args.only_backend:
                        result['exact_reference_path'] = str(args.expected_result.resolve())
                    counts = {'warm': 0, 'equal': 0, 'failed': 0}
                    with (directory / 'requests.jsonl').open('w') as trace:
                        for repeat in range(args.repetitions):
                            pair = {}
                            for route in (['backend'] if args.only_backend else (['backend', 'exact'] if repeat % 2 == 0 else ['exact', 'backend'])):
                                before_backend, before_ch = process_metrics(backend_pid), process_metrics(args.clickhouse_pid)
                                endpoint = f'http://127.0.0.1:{sql}/' if route == 'backend' else args.clickhouse + '/'
                                measured = request(endpoint + '?' + params, query['sql'])
                                after_backend, after_ch = process_metrics(backend_pid), process_metrics(args.clickhouse_pid)
                                trace.write(json.dumps({'repeat': repeat, 'route': route, 'response': measured, 'backend_cpu_ticks': cpu_delta(before_backend, after_backend), 'clickhouse_cpu_ticks': cpu_delta(before_ch, after_ch), 'backend_process': after_backend, 'clickhouse_process': after_ch}) + '\n')
                                pair[route] = measured
                            if args.only_backend:
                                pair['exact'] = exact
                            counts['warm'] += pair['backend']['headers'].get('x-asap-execution') == 'warm'
                            if pair['backend']['status'] == pair['exact']['status'] == 200:
                                left, right = json.loads(pair['backend']['body']), json.loads(pair['exact']['body'])
                                counts['equal'] += left['meta'] == right['meta'] and left['data'] == right['data']
                            else:
                                counts['failed'] += 1
                    result['repeated'] = counts
                    result['query_resources_after'] = {'backend': process_metrics(backend_pid), 'clickhouse': process_metrics(args.clickhouse_pid)}
                if container:
                    inspection = json.loads(subprocess.check_output(['docker', 'inspect', container]))[0]
                    result['container'] = {'image': inspection['Image'], 'cpu_affinity': inspection['HostConfig']['CpusetCpus'], 'nano_cpus': inspection['HostConfig']['NanoCpus'], 'memory_limit_bytes': inspection['HostConfig']['Memory']}
                (directory / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
                results.append(result)
                print(query['id'], result['execution'], equal, flush=True)
            except Exception as error:
                result = {'id': query['id'], 'publication': query['publication'], 'execution': 'failed', 'error': str(error)}
                results.append(result)
                (directory / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
                print(query['id'], 'failed', str(error)[:200], flush=True)
            finally:
                if container:
                    subprocess.run(['docker', 'stop', '-t', '10', container], stdout=subprocess.DEVNULL, check=False)
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        if args.repetitions and (not results or any(result.get('repeated') != {'warm': args.repetitions, 'equal': args.repetitions, 'failed': 0} for result in results)):
            raise SystemExit('Formal replay failed: every requested repetition must be warm and equal')

if __name__ == '__main__':
    main()
