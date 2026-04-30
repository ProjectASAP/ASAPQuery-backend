#!/usr/bin/env python3
"""seed_cold_store.py — pre-populate benchmarks/cold-store/ with raw JSONL
samples in the layout the ASAP ColdFallback adapter expects.

Layout (per asap-query-engine/src/drivers/query/fallback/cold_store/format.rs):
    raw/<metric>/YYYY/MM/DD/HH/part-NNNNNN.jsonl

Each line is one RawSample:
    {"ts_ms": <i64>, "labels": {"k":"v",...}, "value": <f64>}

We seed enough data for the W2 ad-hoc queries to come back non-empty:
the queries hit `sensor_reading` with various label filters; we generate
N series across {region, service, host, pattern} that match what the
fake-exporters produce in the live path.

Usage:
    python benchmarks/scripts/seed_cold_store.py [--root DIR] [--hours 2] [--series 30]
"""

import argparse
import json
import os
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ROOT = os.path.join(HERE, "..", "cold-store")
PATTERNS = ["constant", "linear-up", "linear-down", "sine", "sine-noise", "step", "exp-up"]
REGIONS = ["region0", "region1", "region2"]
SERVICES = ["svc0", "svc1", "svc2", "svc3", "svc4"]
HOSTS = ["host0", "host1"]


def hour_dir(root: str, metric: str, ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return os.path.join(
        root, "raw", metric,
        f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}", f"{dt.hour:02d}",
    )


def write_part(path: str, samples: list[dict], part_idx: int) -> None:
    os.makedirs(path, exist_ok=True)
    fname = os.path.join(path, f"part-{part_idx:06d}.jsonl")
    with open(fname, "w") as f:
        for s in samples:
            f.write(json.dumps(s, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed local-FS cold-store with raw JSONL")
    parser.add_argument("--root", default=DEFAULT_ROOT)
    parser.add_argument("--metric", default="sensor_reading")
    parser.add_argument("--hours", type=int, default=2, help="how many hour-buckets back from now to seed")
    parser.add_argument("--samples-per-series-per-hour", type=int, default=60,
                        help="one sample per minute by default")
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)
    hour_ms = 3600 * 1000

    # Build the series cross-product. Bound it so we don't blow up the FS.
    series = []
    for pattern in PATTERNS:
        for region in REGIONS:
            for service in SERVICES[:3]:  # 3 services per region keeps fixture small
                for host in HOSTS[:1]:
                    series.append({
                        "pattern": pattern, "region": region,
                        "service": service, "host": host,
                    })

    print(f"[seed] generating {len(series)} series x {args.hours} hours x "
          f"{args.samples_per_series_per_hour} samples")

    total_lines = 0
    for h in range(args.hours):
        bucket_start_ms = now_ms - (args.hours - h) * hour_ms
        # Group samples by hour-bucket dir; within a bucket, write one part.
        buckets: dict[str, list[dict]] = {}
        step_ms = hour_ms // args.samples_per_series_per_hour
        for i in range(args.samples_per_series_per_hour):
            ts_ms = bucket_start_ms + i * step_ms
            for idx, labels in enumerate(series):
                # deterministic-but-varied value
                v = float((idx * 7 + i * 3) % 1000)
                sample = {
                    "ts_ms": ts_ms,
                    "labels": dict(sorted(labels.items())),
                    "value": v,
                }
                key = hour_dir(args.root, args.metric, ts_ms)
                buckets.setdefault(key, []).append(sample)

        for path, samples in buckets.items():
            write_part(path, samples, part_idx=h)
            total_lines += len(samples)

    print(f"[seed] wrote {total_lines} JSONL samples under {args.root}")


if __name__ == "__main__":
    main()
