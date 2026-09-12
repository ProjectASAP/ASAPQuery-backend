#!/usr/bin/env python3
"""Compare three native/ASAP pairs without hiding failures or fallback execution."""
import argparse
import json
import math
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'o11y-execution'))
from compare import compare_results, distribution, process_snapshot, process_delta

ENGINES = ('prometheus', 'clickhouse', 'victoriametrics')


def http(url, data=None, headers=None, timeout=30):
    start = time.perf_counter_ns()
    try:
        try:
            stream = urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=timeout)
        except urllib.error.HTTPError as error:
            stream = error
        with stream:
            raw = stream.read().decode()
            try:
                body = json.loads(raw)
            except ValueError:
                body = raw
            result = {'status': stream.code, 'headers': {k.lower(): v for k, v in stream.headers.items()}, 'body': body}
    except (OSError, ValueError) as error:
        result = {'status': None, 'headers': {}, 'error': str(error)}
    return dict(result, elapsed_ns=time.perf_counter_ns() - start)


def _query_endpoint(engine, endpoint, query, evaluation_ms, timeout):
    endpoint = {'url': endpoint} if isinstance(endpoint, str) else endpoint
    url = endpoint['url'].rstrip('/')
    headers = endpoint.get('headers', {})
    if engine == 'clickhouse':
        sql = query['clickhouse_sql'].replace('{eval_ms}', str(evaluation_ms))
        sql = sql.replace('{lookback_ms}', str(query.get('lookback_ms', 300000)))
        # The workload supplies a SELECT without a FORMAT clause. JSON retains
        # ClickHouse type metadata, including its integer-quoting contract.
        return http(url + '/?' + urllib.parse.urlencode({'database': endpoint['database']}),
                    (sql.rstrip().rstrip(';') + ' FORMAT JSON').encode(), headers, timeout)
    expression = query['promql' if engine == 'prometheus' else 'metricsql']
    return http(url + '/api/v1/query?' + urllib.parse.urlencode(
        {'query': expression, 'time': evaluation_ms / 1000, **({'nocache': 1} if engine == 'victoriametrics' else {})}), headers=headers, timeout=timeout)


def query_endpoint(engine, endpoint, query, evaluation_ms, timeout):
    pids = endpoint.get('pids', []) if isinstance(endpoint, dict) else []
    before = [process_snapshot(pid) for pid in pids]
    result = _query_endpoint(engine, endpoint, query, evaluation_ms, timeout)
    after = [process_snapshot(pid) for pid in pids]
    deltas = [process_delta(a, b) for a, b in zip(before, after)]
    result['process_resources'] = deltas
    result['cpu_ns'] = sum(d['cpu_ns'] for d in deltas) if deltas and all(deltas) else None
    return result


def sql_compare(actual, expected, relative, absolute):
    if actual.get('meta') != expected.get('meta'):
        return {'equal': False, 'reason': 'SQL column names or types differ'}
    a, b = actual.get('data'), expected.get('data')
    if not isinstance(a, list) or not isinstance(b, list):
        return {'equal': False, 'reason': 'missing SQL result rows'}
    errors = []
    def equal(x, y):
        if isinstance(x, (int, float)) and not isinstance(x, bool) and isinstance(y, (int, float)) and not isinstance(y, bool):
            if not math.isfinite(x) or not math.isfinite(y):
                return False
            errors.append(abs(x - y))
            return math.isclose(x, y, rel_tol=relative, abs_tol=absolute)
        if type(x) is not type(y):
            return False
        if isinstance(x, dict):
            return x.keys() == y.keys() and all(equal(x[k], y[k]) for k in x)
        if isinstance(x, list):
            return len(x) == len(y) and all(equal(i, j) for i, j in zip(x, y))
        return x == y
    # SQL without ORDER BY promises a multiset. Preserve duplicate rows.
    sort_key = lambda row: json.dumps(row, sort_keys=True)
    matched = equal(sorted(a, key=sort_key), sorted(b, key=sort_key))
    return {'equal': matched, 'max_absolute_error': max(errors, default=None),
            'expected_rows': len(b), 'actual_rows': len(a)}


def assess(engine, native, asap, relative=1e-9, absolute=1e-12):
    comparison = {'equal': False, 'reason': 'endpoint failed'}
    if native.get('status') == asap.get('status') == 200:
        try:
            comparison = (sql_compare(asap['body'], native['body'], relative, absolute) if engine == 'clickhouse'
                          else compare_results(asap['body'], native['body'], relative, absolute))
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            comparison = {'equal': False, 'reason': str(error)}
    route = asap.get('headers', {}).get('x-asap-execution', 'unknown') if asap.get('status') == 200 else 'failed'
    passed = comparison['equal'] and route != 'failed'
    return {'native': native, 'asap': asap, 'comparison': comparison, 'passed': passed,
            'execution': route, 'accelerated': passed and route == 'warm'}


def compare_round(endpoints, query, evaluation_ms, timeout, relative=1e-9, absolute=1e-12, reverse=False):
    results = {}
    for engine in ENGINES:
        responses = {}
        for route in (('asap', 'native') if reverse else ('native', 'asap')):
            start = time.perf_counter_ns()
            try:
                responses[route] = query_endpoint(engine, endpoints[engine][route], query, evaluation_ms, timeout)
            except Exception as error:
                responses[route] = {'status': None, 'headers': {}, 'error': str(error),
                                    'elapsed_ns': time.perf_counter_ns() - start}
        results[engine] = assess(engine, responses['native'], responses['asap'], relative, absolute)
    return results


def summarize(records):
    summary = {}
    for engine in ENGINES:
        rows = [record['engines'][engine] for record in records]
        passed = bool(rows) and all(r['passed'] for r in rows)
        summary[engine] = {'requests': len(rows), 'passed': passed,
                           'equal': sum(r['passed'] for r in rows),
                           'accelerated': sum(r['accelerated'] for r in rows),
                           'execution_counts': {k: sum(r['execution'] == k for r in rows)
                                                for k in ('warm', 'hybrid', 'exact_fallback', 'failed', 'unknown')},
                           'native_latency': distribution([r['native']['elapsed_ns'] for r in rows]),
                           'asap_latency': distribution([r['asap']['elapsed_ns'] for r in rows]),
                           'native_over_asap_latency_ratio': (sum(r['native']['elapsed_ns'] for r in rows) /
                                                             sum(r['asap']['elapsed_ns'] for r in rows)) if passed else None}
    return summary


def run(endpoints, manifest, repetitions, timeout, relative=1e-9, absolute=1e-12, record_sink=None):
    if repetitions < 1 or not manifest['queries']:
        raise ValueError('nonempty workload and positive repetitions required')
    if timeout <= 0 or not math.isfinite(timeout) or any(not math.isfinite(v) or v < 0 for v in (relative, absolute)):
        raise ValueError('timeout and tolerances must be finite and valid')
    records = []
    for repeat in range(repetitions):
        for index, query in enumerate(manifest['queries']):
            evaluation_ms = query.get('evaluation_ms', manifest['end_ms'])
            record = {'query_index': index, 'name': query.get('name', str(index)), 'repeat': repeat,
                      'evaluation_ms': evaluation_ms,
                      'engines': compare_round(endpoints, query, evaluation_ms, timeout, relative, absolute, bool(repeat % 2))}
            records.append(record)
            if record_sink:
                record_sink(record)
    summary = summarize(records)
    return {'schema_version': 1, 'passed': all(v['passed'] for v in summary.values()),
            'numeric_tolerance': {'relative': relative, 'absolute': absolute},
            'timing_scope': 'serial alternating HTTP requests; fixed evaluation times; not a concurrency/throughput measurement',
            'summary': summary, 'records': records}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('endpoints', 'manifest', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--repetitions', type=int, default=10)
    parser.add_argument('--timeout', type=float, default=30)
    parser.add_argument('--relative-tolerance', type=float, default=1e-9)
    parser.add_argument('--absolute-tolerance', type=float, default=1e-12)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / 'requests.jsonl').open('w') as trace:
        def record(row):
            trace.write(json.dumps(row) + '\n')
            trace.flush()
        report = run(json.loads(args.endpoints.read_text()), json.loads(args.manifest.read_text()),
                     args.repetitions, args.timeout, args.relative_tolerance, args.absolute_tolerance, record_sink=record)
    (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    raise SystemExit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
