#!/usr/bin/env python3
"""Project full-row-distinct Alibaba observations to timestamped binary replay.

Record layout, little endian: timestamp_ms:u32, upstream:u32, downstream:u32,
latency_ms:f64. Missing upstream is u32::MAX; invalid latency is NaN. Missing
downstream observations are counted and excluded. Original trace time is kept.
"""
import argparse
import concurrent.futures
import gzip
import hashlib
import json
import math
from pathlib import Path
import shutil
import time

import duckdb
import numpy as np
from prepare_alibaba import prepare


def project(root, index, discard_new_intermediates=False):
    output = root / f"observations_{index}.bin.gz"
    manifest = root / f"observations_{index}.json"
    if manifest.exists():
        record = json.loads(manifest.read_text())
        if output.exists() and output.stat().st_size == record['replay_bytes']:
            return record
        raise ValueError(f"invalid existing replay: {output}")
    if shutil.disk_usage(root).free < 12 * 1024**3:
        raise RuntimeError('disk reserve below 12 GiB')
    source = prepare(root, index)
    started = time.perf_counter()
    connection = duckdb.connect()
    connection.execute("SET threads=2")
    connection.execute("SET memory_limit='8GB'")
    connection.execute("SET max_temp_directory_size='4GB'")
    temp = root / f"project-temp-{index}"
    temp.mkdir(exist_ok=True)
    connection.execute("SET temp_directory=?", [str(temp)])
    connection.read_parquet(str(root / f"CallGraph_{index}.parquet")).create_view('raw')
    connection.execute('CREATE TABLE distinct_rows AS SELECT DISTINCT * FROM raw')
    distinct = connection.execute('SELECT count(*) FROM distinct_rows').fetchone()[0]
    connection.execute("""CREATE VIEW typed AS SELECT *,
        try_cast(timestamp AS BIGINT) AS ts,
        CASE WHEN regexp_full_match(dm,'MS_(0|[1-9][0-9]*)') THEN try_cast(substr(dm,4) AS UINTEGER) END AS downstream,
        CASE WHEN regexp_full_match(um,'MS_(0|[1-9][0-9]*)') THEN try_cast(substr(um,4) AS UINTEGER) END AS upstream,
        try_cast(rt AS DOUBLE) AS latency FROM distinct_rows""")
    invalid_times = connection.execute('SELECT count(*) FROM typed WHERE ts IS NULL OR ts < ? OR ts >= ?', [index*180000,(index+1)*180000]).fetchone()[0]
    if invalid_times:
        raise ValueError(f'{invalid_times} timestamps outside archive interval; cannot assume cross-file disjointness')
    missing_dm = connection.execute('SELECT count(*) FROM typed WHERE downstream IS NULL').fetchone()[0]
    connection.execute('CREATE VIEW usable AS SELECT * FROM typed WHERE downstream IS NOT NULL')
    invalid_rt, zeros, missing_um, expected = connection.execute("""SELECT
        count(*) FILTER(WHERE latency IS NULL OR NOT isfinite(latency) OR latency<0),
        count(*) FILTER(WHERE latency=0),count(*) FILTER(WHERE upstream IS NULL), count(*) FROM usable""").fetchone()
    query = """SELECT ts::UINTEGER AS timestamp, coalesce(upstream,4294967295)::UINTEGER AS upstream,
        downstream, CASE WHEN latency IS NOT NULL AND isfinite(latency) AND latency>=0
        THEN latency ELSE 'NaN'::DOUBLE END AS latency
        FROM usable ORDER BY ts,traceid,service,rpc_id,rpctype,um,uminstanceid,interface,dm,dminstanceid,rt"""
    cursor = connection.execute(query).fetch_record_batch(262144)
    count = 0
    digest = hashlib.sha256()
    dtype = np.dtype([('timestamp','<u4'),('upstream','<u4'),('downstream','<u4'),('latency','<f8')])
    partial = output.with_suffix('.gz.partial')
    with partial.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=1, mtime=0) as compressed:
        for batch in cursor:
            records = np.empty(batch.num_rows, dtype=dtype)
            for name in dtype.names:
                records[name] = batch.column(name).to_numpy(zero_copy_only=False)
            content = records.tobytes()
            digest.update(content)
            compressed.write(content)
            count += batch.num_rows
    if count != expected:
        raise ValueError('projected row count mismatch')
    partial.rename(output)
    connection.close()
    record = {'schema_version':1, 'event_semantics':'full-row-distinct call observations, not unique logical requests',
              'index':index,'source':source,'distinct_full_rows':distinct,
              'exact_duplicate_rows_removed':source['parsed_rows']-distinct,
              'missing_downstream_rows_removed':missing_dm,'events':count,
              'invalid_latency_rows_retained_for_counts':invalid_rt,'zero_latency_rows':zeros,
              'missing_upstream_rows_retained_for_service_queries':missing_um,
              'replay_bytes':output.stat().st_size,'uncompressed_sha256':digest.hexdigest(),
              'projection_seconds':time.perf_counter()-started}
    manifest.write_text(json.dumps(record,indent=2)+'\n')
    # Only intermediates created by this workflow beyond the retained audit hour.
    # They are recoverable from the recorded public URL and archive SHA-256.
    if discard_new_intermediates and index>=20:
        (root/f'CallGraph_{index}.parquet').unlink()
        (root/f'CallGraph_{index}.tar.gz').unlink()
    print(json.dumps({k:v for k,v in record.items() if k!='source'}),flush=True)
    return record


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--intervals',type=int,default=240)
    parser.add_argument('--workers',type=int,default=2)
    parser.add_argument('--discard-new-intermediates',action='store_true')
    args=parser.parse_args()
    if args.intervals<1 or not 1<=args.workers<=2:
        parser.error('invalid interval/worker count')
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        records=list(pool.map(lambda i:project(args.directory,i,args.discard_new_intermediates),range(args.intervals)))
    summary={'status':'prepared_not_evaluated','files':records,'events':sum(r['events'] for r in records),
             'replay_bytes':sum(r['replay_bytes'] for r in records)}
    (args.directory/f'replay-manifest-{args.intervals}.json').write_text(json.dumps(summary,indent=2)+'\n')


if __name__=='__main__':
    main()
