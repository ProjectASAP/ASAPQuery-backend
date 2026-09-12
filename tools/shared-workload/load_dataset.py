#!/usr/bin/env python3
"""Load the same normalized JSONL samples into isolated evaluation services."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "o11y-execution"))
from replay import encode_write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--remote-write", action="append", required=True,
                        help="full write URL; repeat for baseline and ASAP receivers")
    parser.add_argument("--clickhouse", required=True, help="HTTP URL including isolated database parameter")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("receipt already exists; refusing to load again")
    # Read and validate the completed manifest before any remote mutations.
    manifest = json.loads((args.data / "data-manifest.json").read_text())
    path = args.data / "samples.jsonl"
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != manifest["sha256"]["samples.jsonl"]:
        raise ValueError("data hash does not match manifest")
    def post(url, body, headers=None):
        with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=headers or {}), timeout=None) as response:
            response.read()
    def batch(rows):
        encoded = encode_write([({**r["labels"], "__name__": r["metric"]}, r["value"], r["ts_ms"]) for r in rows])
        for endpoint in args.remote_write:
            post(endpoint, encoded, {"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy",
                                     "X-Prometheus-Remote-Write-Version": "0.1.0"})
        post(args.clickhouse, ("INSERT INTO raw_samples FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows)).encode())
    with path.open() as source:
        pending = []
        for line in source:
            pending.append(json.loads(line))
            if len(pending) == 1000:
                batch(pending)
                pending = []
        if pending:
            batch(pending)
    with args.output.open("x") as out:
        json.dump({"data": manifest, "remote_write": args.remote_write, "clickhouse": args.clickhouse,
                   "scope": "all configured endpoints accepted batches; not proof of summary readiness"}, out, indent=2)


if __name__ == "__main__":
    main()
