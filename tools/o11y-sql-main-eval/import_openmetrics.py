#!/usr/bin/env python3
"""Import unchanged OpenMetrics samples into an isolated ClickHouse table."""
import argparse
import base64
import collections
from decimal import Decimal
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import urllib.parse
import urllib.request

LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)=("(?:\\.|[^"\\])*")(?:,|$)')


def parse_series(token):
    if '{' not in token:
        return token, {}
    metric, tail = token.split('{', 1)
    if not tail.endswith('}'):
        raise ValueError('unterminated series labels')
    labels, offset = {}, 0
    for match in LABEL.finditer(tail[:-1]):
        if match.start() != offset or match[1] in labels:
            raise ValueError('invalid or duplicate series label')
        labels[match[1]] = json.loads(match[2])
        offset = match.end()
    if offset != len(tail) - 1:
        raise ValueError('unparsed series labels')
    return metric, labels


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('source', type=Path)
    ap.add_argument('output', type=Path, help='lossless JSONEachRow gzip artifact')
    ap.add_argument('--url', default='http://127.0.0.1:28123')
    ap.add_argument('--database', default='asap_o11y27_eval')
    args = ap.parse_args()
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', args.database):
        ap.error('database must be a simple identifier')
    credentials = f"{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}"
    auth = 'Basic ' + base64.b64encode(credentials.encode()).decode()

    def request(body, database=None, compressed=False):
        url = args.url + '/?' + urllib.parse.urlencode({'database': database or 'default'})
        headers = {'Authorization': auth}
        if compressed:
            headers['Content-Encoding'] = 'gzip'
        req = urllib.request.Request(url, data=body, headers=headers)
        with urllib.request.urlopen(req, timeout=120) as response:
            return response.read()

    request(f'CREATE DATABASE IF NOT EXISTS `{args.database}`'.encode())
    request(b'CREATE TABLE IF NOT EXISTS raw_samples (metric String, labels Map(String,String), ts_ms Int64, value Float64) ENGINE=MergeTree ORDER BY (metric,ts_ms)', args.database)
    if int(request(b'SELECT count() FROM raw_samples', args.database)):
        raise RuntimeError('destination contains samples; use a fresh database')
    if args.output.exists():
        raise RuntimeError('output artifact already exists; choose a new path')
    source_sha = hashlib.sha256()
    counts, series, pending = collections.Counter(), {}, []
    lower = upper = None
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def flush():
        if pending:
            request(gzip.compress(b'INSERT INTO raw_samples FORMAT JSONEachRow\n' + b''.join(pending), compresslevel=1), args.database, True)
            pending.clear()

    with args.source.open('rb') as source, gzip.open(args.output, 'wb', compresslevel=1) as output:
        for raw in source:
            source_sha.update(raw)
            if not raw.strip() or raw.startswith(b'#'):
                continue
            token, value, timestamp = raw.decode().strip().rsplit(None, 2)
            if token not in series:
                series[token] = parse_series(token)
            metric, labels = series[token]
            milliseconds = Decimal(timestamp) * 1000
            if milliseconds != milliseconds.to_integral_value():
                raise ValueError('sub-millisecond timestamp cannot enter the Int64 millisecond schema')
            ts = int(milliseconds)
            number = float(value)
            if not math.isfinite(number):
                raise ValueError('nonfinite sample requires an explicit ClickHouse transport contract')
            record = (json.dumps({'metric': metric, 'labels': labels, 'ts_ms': ts, 'value': number}, separators=(',', ':'), allow_nan=False) + '\n').encode()
            output.write(record)
            pending.append(record)
            counts[metric] += 1
            lower = ts if lower is None else min(lower, ts)
            upper = ts if upper is None else max(upper, ts)
            if len(pending) >= 50_000:
                flush()
        flush()
    actual = int(request(b'SELECT count() FROM raw_samples', args.database))
    if actual != sum(counts.values()):
        raise RuntimeError(f'row count differs: source {sum(counts.values())}, ClickHouse {actual}')
    manifest = {'source': str(args.source.resolve()), 'source_sha256': source_sha.hexdigest(), 'output': str(args.output.resolve()), 'output_bytes': args.output.stat().st_size, 'database': args.database, 'table': 'raw_samples', 'samples': actual, 'series': len(series), 'start_ms': lower, 'end_ms': upper, 'metric_samples': counts, 'transformation': 'unchanged metric names, labels, Float64 samples, and integral-millisecond timestamps; no query-answer ingestion'}
    args.output.with_suffix('.manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest))

if __name__ == '__main__':
    main()
