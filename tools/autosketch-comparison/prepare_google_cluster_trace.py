#!/usr/bin/env python3
"""Convert official instance_usage Parquet to deterministic pane/key replay TSV."""
import argparse
import json
import pathlib

import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--pane-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.pane_seconds <= 0:
        parser.error("pane seconds must be positive")
    table = pq.read_table(args.input, columns=["start_time", "collection_id"])
    if table["start_time"].null_count or table["collection_id"].null_count:
        raise ValueError("identity/timestamp nulls are not allowed")
    start = table["start_time"].to_pylist()
    collections = table["collection_id"].to_pylist()
    minimum = min(start)
    pane_us = args.pane_seconds * 1_000_000
    identities = {value: index for index, value in enumerate(sorted(set(collections)))}
    rows = sorted(((timestamp - minimum) // pane_us, identities[key])
                  for timestamp, key in zip(start, collections))
    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as target:
        for pane, key in rows:
            target.write(f"{pane}\t{key}\n")
    metadata = {
        "input": str(pathlib.Path(args.input).resolve()),
        "rows": len(rows),
        "keys": len(identities),
        "pane_seconds": args.pane_seconds,
        "minimum_start_time_us": minimum,
        "maximum_start_time_us": max(start),
        "panes_including_gaps": rows[-1][0] + 1,
        "nonempty_panes": len(set(pane for pane, _ in rows)),
        "mapping": "collection IDs sorted numerically and replaced by dense zero-based IDs",
    }
    with pathlib.Path(args.metadata).open("x", encoding="utf-8") as target:
        json.dump(metadata, target, indent=2)
        target.write("\n")


if __name__ == "__main__":
    main()
