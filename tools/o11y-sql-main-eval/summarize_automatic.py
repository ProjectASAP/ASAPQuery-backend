#!/usr/bin/env python3
"""Validate paired raw requests and summarize fixed-window release trials."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('root', type=Path)
    ap.add_argument('output', type=Path)
    args = ap.parse_args()
    summaries = []
    for trace in sorted(args.root.glob('trial-*/q*/requests.jsonl')):
        result = json.loads(trace.with_name('result.json').read_text())
        rows = [json.loads(line) for line in trace.read_text().splitlines()]
        pairs = {}
        for row in rows:
            pair = pairs.setdefault(row['repeat'], {})
            if row['route'] in pair:
                raise ValueError('duplicate route/repetition')
            pair[row['route']] = row
        for pair in pairs.values():
            if set(pair) != {'backend', 'exact'}:
                raise ValueError('incomplete pair')
            left, right = pair['backend']['response'], pair['exact']['response']
            if left['status'] != 200 or right['status'] != 200:
                raise ValueError('unsuccessful response')
            if left['headers'].get('x-asap-execution') != 'warm':
                raise ValueError('backend did not execute warm')
            left, right = json.loads(left['body']), json.loads(right['body'])
            if left['meta'] != right['meta'] or left['data'] != right['data']:
                raise ValueError('decoded result mismatch')
        if len(pairs) != 1000 or set(pairs) != set(range(1000)):
            raise ValueError('expected1000 complete repetitions')
        routes = {}
        for route in ['backend', 'exact']:
            selected = [row for row in rows if row['route'] == route]
            latency = [row['response']['elapsed_ns'] / 1e6 for row in selected]
            hz = selected[0]['backend_process']['clock_ticks_per_second']
            routes[route] = {
                'requests': len(selected), 'median_ms': statistics.median(latency),
                'p95_ms': sorted(latency)[949],
                'request_time_rate_per_second': len(selected) / (sum(latency) / 1000),
                'backend_cpu_s_during_requests': sum(row['backend_cpu_ticks'] for row in selected) / hz,
                'clickhouse_cpu_s_during_requests': sum(row['clickhouse_cpu_ticks'] for row in selected) / hz,
                'max_observed_backend_rss_bytes': max(row['backend_process']['VmRSS'] for row in selected),
                'max_observed_clickhouse_rss_bytes': max(row['clickhouse_process']['VmRSS'] for row in selected),
            }
        before, after = result['build_resources']['before'], result['build_resources']['after']
        build_cpu = {name: (after[name]['cpu_ticks'] - before[name]['cpu_ticks']) / after[name]['clock_ticks_per_second'] for name in before}
        summary = {'query': result['id'], 'trial': trace.parent.parent.name,
                   'numeric_pairs_equal': len(pairs), 'warm_requests': len(pairs),
                   'latency_speedup': routes['exact']['median_ms'] / routes['backend']['median_ms'],
                   'routes': routes, 'install_and_backfill_s': result['install_and_backfill_ns'] / 1e9,
                   'build_cpu_s': build_cpu, 'process_after_build': after,
                   'process_after_queries': result['query_resources_after'],
                   'state_directory_bytes_at_build': result['state_directory_bytes'],
                   'state_directory_bytes_after_stop': sum(path.stat().st_size for path in trace.parent.joinpath('state').rglob('*') if path.is_file()),
                   'first_post_build_request_ms': {name: result[name]['elapsed_ns'] / 1e6 for name in ['backend', 'exact']},
                   'backend_budget': result['container'],
                   'clickhouse_budget': json.loads(trace.parent.parent.joinpath('clickhouse-runtime.json').read_text()),
                   'raw_trace': str(trace.resolve()), 'raw_sha256': hashlib.sha256(trace.read_bytes()).hexdigest()}
        for name in ['backend_budget', 'clickhouse_budget']:
            budget = summary[name]
            if (budget['cpu_affinity'], budget['nano_cpus'], budget['memory_limit_bytes']) != ('60,61',2000000000,4294967296):
                raise ValueError('resource budget mismatch')
        summaries.append(summary)
    if len(summaries) != 9:
        raise ValueError('expected9 completed trials')
    args.output.write_text(json.dumps({'scope': 'fixed-window repeated queries; excludes common source ingestion and offline planning cost', 'trials': summaries}, indent=2) + '\n')
    for q in ['q05','q06','q23']:
        group = [s for s in summaries if s['query'] == q]
        print(q, 'backend_ms', [round(s['routes']['backend']['median_ms'],3) for s in group], 'exact_ms', [round(s['routes']['exact']['median_ms'],3) for s in group], 'speedup', [round(s['latency_speedup'],2) for s in group])

if __name__ == '__main__':
    main()
