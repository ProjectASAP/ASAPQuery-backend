#!/usr/bin/env python3
"""Download complete official intervals and audit call identity before replay.

This is data preparation, not an evaluation result. No deduplication policy is
silently imposed: conflicting observations of a call are reported first.
Requires pyarrow and duckdb. Archives and lossless Parquet remain reproducible.
"""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import time

import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

BASE = "https://aliopentrace.oss-cn-beijing.aliyuncs.com/v2022MicroservicesTraces/CallGraph"
COLUMNS = ["timestamp", "traceid", "service", "rpc_id", "rpctype", "um",
           "uminstanceid", "interface", "dm", "dminstanceid", "rt"]


def prepare(root, index):
    archive = root / f"CallGraph_{index}.tar.gz"
    parquet = root / f"CallGraph_{index}.parquet"
    manifest = root / f"CallGraph_{index}.manifest.json"
    if manifest.exists():
        record = json.loads(manifest.read_text())
        if parquet.exists() and archive.exists():
            return record
        raise ValueError(f"incomplete cached interval: {index}")
    if shutil.disk_usage(root).free < 12 * 1024**3:
        raise RuntimeError("less than 12 GiB disk reserve; stopping without deleting data")
    started = time.perf_counter()
    url = f"{BASE}/CallGraph_{index}.tar.gz"
    if not archive.exists():
        partial = archive.with_suffix(".partial")
        subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error",
                        "--retry", "3", "--max-time", "600", "--output", str(partial), url], check=True)
        partial.rename(archive)
    sha = hashlib.sha256()
    with archive.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024**2), b""):
            sha.update(block)
    rows = 0
    malformed = {"count": 0, "examples": []}

    def invalid_row(row):
        malformed["count"] += 1
        if len(malformed["examples"]) < 5:
            malformed["examples"].append({"expected_columns": row.expected_columns,
                                           "actual_columns": row.actual_columns,
                                           "text": row.text})
        return "skip"

    partial_parquet = parquet.with_suffix(".parquet.partial")
    schema = pa.schema([(name, pa.string()) for name in COLUMNS])
    with tarfile.open(archive, "r|gz") as tar, pq.ParquetWriter(partial_parquet, schema, compression="zstd") as out:
        members = 0
        for member in tar:
            if not member.isfile():
                continue
            if member.name != f"CallGraph_{index}.csv":
                raise ValueError(f"unexpected archive member {member.name}")
            members += 1
            reader = csv.open_csv(tar.extractfile(member),
                                  read_options=csv.ReadOptions(block_size=8 * 1024**2),
                                  parse_options=csv.ParseOptions(invalid_row_handler=invalid_row),
                                  convert_options=csv.ConvertOptions(column_types=schema, strings_can_be_null=False))
            if reader.schema != schema:
                raise ValueError(f"unexpected schema {reader.schema}")
            for batch in reader:
                rows += batch.num_rows
                out.write_batch(batch)
        if members != 1:
            raise ValueError("expected exactly one CSV member")
    partial_parquet.rename(parquet)
    record = {"index": index, "url": url, "sha256": sha.hexdigest(),
              "compressed_bytes": archive.stat().st_size, "parquet_bytes": parquet.stat().st_size,
              "parsed_rows": rows, "malformed_rows": malformed,
              "preparation_seconds": time.perf_counter() - started}
    manifest.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record), flush=True)
    return record


def audit(root, records):
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='64GB'")
    con.execute("SET max_temp_directory_size='8GB'")
    temp = root / "duckdb-temp"
    temp.mkdir(exist_ok=True)
    con.execute("SET temp_directory=?", [str(temp)])
    files = [str(root / f"CallGraph_{r['index']}.parquet") for r in records]
    con.read_parquet(files).create_view("raw")
    con.execute("CREATE VIEW typed AS SELECT *, try_cast(timestamp AS BIGINT) AS ts, try_cast(rt AS DOUBLE) AS latency FROM raw")

    def rows(sql):
        cursor = con.execute(sql)
        names = [x[0] for x in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    result = {"status": "audit_only_not_performance_results", "files": records,
              "summary": rows("""SELECT count(*) AS raw_rows, min(ts) AS min_timestamp,
                   max(ts) AS max_timestamp, count(DISTINCT dm) AS downstream_services,
                   count(DISTINCT dminstanceid) AS downstream_instances,
                   count(DISTINCT (um,dm)) AS service_edges,
                   count(*) FILTER(WHERE ts IS NULL) AS invalid_timestamps,
                   count(*) FILTER(WHERE latency IS NULL OR NOT isfinite(latency) OR latency < 0) AS invalid_latencies,
                   count(*) FILTER(WHERE latency = 0) AS zero_latencies,
                   count(*) FILTER(WHERE dm IN ('UNKNOWN','', '(?)') OR dm IS NULL) AS missing_downstream
                   FROM typed""")[0],
              "per_minute": rows("SELECT ts//60000 AS minute, count(*) AS records, count(DISTINCT dm) AS keys FROM typed GROUP BY 1 ORDER BY 1"),
              "call_identity": rows("""WITH calls AS (
                   SELECT traceid, rpc_id, count(*) AS n,
                          count(DISTINCT (service,um,dm)) AS edges,
                          count(DISTINCT (timestamp,rt)) AS measurements,
                          count(DISTINCT (uminstanceid,dminstanceid)) AS instances
                   FROM typed GROUP BY traceid,rpc_id)
                   SELECT count(*) AS trace_rpc_pairs, sum(n-1) AS repeated_pair_rows,
                          count(*) FILTER(WHERE edges>1) AS conflicting_service_edges,
                          count(*) FILTER(WHERE measurements>1) AS conflicting_measurements,
                          count(*) FILTER(WHERE instances>1) AS conflicting_instances
                   FROM calls""")[0],
              "conflict_examples": rows("""SELECT traceid,rpc_id,count(*) AS n,
                   count(DISTINCT (service,um,dm)) AS edges,
                   count(DISTINCT (timestamp,rt)) AS measurements
                   FROM typed GROUP BY traceid,rpc_id HAVING edges>1 OR measurements>1
                   ORDER BY traceid,rpc_id LIMIT 5""")}
    destination = root / f"audit-{records[0]['index']}-{records[-1]['index']}.json"
    destination.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"audit": str(destination), "summary": result["summary"], "call_identity": result["call_identity"]}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--intervals", type=int, default=20, help="Each official file spans 3 minutes")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.start_index < 0 or args.intervals < 1 or not 1 <= args.workers <= 4:
        parser.error("invalid interval/worker count")
    args.directory.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        records = list(pool.map(lambda i: prepare(args.directory, i), range(args.start_index, args.start_index + args.intervals)))
    audit(args.directory, records)


if __name__ == "__main__":
    main()
