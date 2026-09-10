#!/usr/bin/env python3
"""Extract one unchanged OpenMetrics series for the bounded SQL max probe."""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--series", required=True, help="exact metric{labels} token")
    args = parser.parse_args()
    metric = args.series.split("{", 1)[0]
    digest = hashlib.sha256()
    count = 0
    start_ms = None
    end_ms = None
    with args.input.open("rb") as source, args.output.open("w") as output:
        for raw in source:
            digest.update(raw)
            line = raw.decode().strip()
            if not line or line.startswith("#"):
                continue
            fields = line.rsplit(None, 2)
            if len(fields) != 3 or fields[0] != args.series:
                continue
            timestamp_ms = int(Decimal(fields[2]) * 1000)
            value = float(fields[1])
            output.write(json.dumps({"metric": metric, "labels": args.series,
                                     "ts_ms": timestamp_ms, "value": value},
                                    allow_nan=False) + "\n")
            start_ms = timestamp_ms if start_ms is None else min(start_ms, timestamp_ms)
            end_ms = timestamp_ms if end_ms is None else max(end_ms, timestamp_ms)
            count += 1
    if not count:
        raise SystemExit("no matching samples")
    provenance = {"source": str(args.input.resolve()), "source_sha256": digest.hexdigest(),
                  "output": str(args.output.resolve()), "series": args.series,
                  "samples": count, "start_ms": start_ms, "end_ms": end_ms,
                  "transformation": "one exact series selection; unchanged values and timestamps"}
    args.output.with_suffix(".provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps(provenance))


if __name__ == "__main__":
    main()
