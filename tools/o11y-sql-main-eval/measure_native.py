#!/usr/bin/env python3
"""Fresh native-only replay for deployment CPU/RSS comparison."""
import argparse
import base64
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request
from run_automatic import process_metrics


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('matrix', type=Path)
    ap.add_argument('output', type=Path)
    ap.add_argument('--query', default='q05')
    ap.add_argument('--pid', type=int, required=True)
    ap.add_argument('--expected', type=Path, required=True)
    args = ap.parse_args()
    query = next(q for q in json.loads(args.matrix.read_text())['queries'] if q['id'] == args.query)
    reference = json.loads(args.expected.read_text())
    if reference['id'] != args.query:
        raise ValueError('reference identity mismatch')
    expected = json.loads(reference['exact']['body'])
    args.output.mkdir(parents=True, exist_ok=False)
    auth = 'Basic ' + base64.b64encode(f"{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}".encode()).decode()
    url = 'http://127.0.0.1:28123/?' + urllib.parse.urlencode({'database': 'asap_o11y27_eval', 'default_format': 'JSON', 'max_threads': 2, 'use_query_cache': 0})
    before = process_metrics(args.pid)
    with (args.output / 'requests.jsonl').open('w') as trace:
        for repeat in range(1000):
            start = time.perf_counter_ns()
            with urllib.request.urlopen(urllib.request.Request(url, data=query['sql'].encode(), headers={'Authorization': auth}), timeout=180) as response:
                body = response.read().decode()
                status = response.status
            elapsed = time.perf_counter_ns() - start
            actual = json.loads(body)
            if status != 200 or actual['meta'] != expected['meta'] or actual['data'] != expected['data']:
                raise ValueError('native result mismatch')
            trace.write(json.dumps({'repeat': repeat, 'status': status, 'elapsed_ns': elapsed, 'body': body, 'process': process_metrics(args.pid)}) + '\n')
    result = {'query': args.query, 'requests_equal': 1000, 'process_before': before, 'process_after': process_metrics(args.pid), 'scope': 'fresh native process, persisted source data; common initial ingestion excluded'}
    (args.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(args.query, 'native-only', 1000, flush=True)

if __name__ == '__main__':
    main()
