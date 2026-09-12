#!/usr/bin/env python3
"""Summarize recorded service latency without claiming unmeasured system speedup."""
import argparse
from collections import defaultdict
import json
from pathlib import Path

from accuracy_suite import comparison


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row["query_id"]].append(row)
    result = {}
    for query, records in groups.items():
        engines = {engine for row in records for engine in row["measurements"]}
        result[query] = {
            "occurrences": len(records), "passed": sum(r["passed"] for r in records),
            "warm_occurrences": sum(r.get("execution") == ["warm", "warm"] for r in records),
            "latency": {engine: comparison.distribution([r["measurements"][engine]["latency_ns"]
                        for r in records if engine in r["measurements"]]) for engine in sorted(engines)},
            "victoriametrics_equal": sum(r.get("victoriametrics", {}).get("equal", False) for r in records),
        }
    return {"queries": result, "scope": "sequential HTTP latency including failures; no throughput or total-system speedup claim"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.input.open() as source:
        result = summarize(json.loads(line) for line in source)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
