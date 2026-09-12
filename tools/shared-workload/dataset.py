#!/usr/bin/env python3
"""Create matched Prometheus-client exposition and ClickHouse rows."""
import argparse
import csv
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path


def synthetic(groups, members, start_ms, duration_ms):
    for step in range(duration_ms // 100 + 1):
        for group in range(groups):
            for member in range(members):
                labels = {"label_0": f"g{group:06d}", "label_1": str(member),
                          "job": "fake-metrics", "instance": "synthetic:8000"}
                ts = start_ms + step * 100
                yield "fake_metric", labels, ts, float(1 + group % 19 + member + (step % 31) / 31)
                yield "fake_metric_counter_total", labels, ts, float((step % 997) * (member + 1))


def trace(path, dataset):
    with path.open() as source:
        if dataset == "google":
            # Existing ASAPCollector mapper JSONL preserves original trace identities.
            for line in source:
                row = json.loads(line)
                if row["metric"] != "google_cluster_cpu_rate":
                    continue
                labels = row["attributes"]
                if not {"service", "task", "host"} <= labels.keys():
                    raise ValueError("Google mapper record is missing service/task/host identity")
                ts = Decimal(str(row["timestamp_ms"]))
                if ts != int(ts):
                    raise ValueError("sub-millisecond trace timestamp")
                yield row["metric"], labels, int(ts), float(row["value"])
        else:
            # Official Alibaba 2018 container_usage.csv has no header.
            for row in csv.reader(source):
                if len(row) != 11:
                    raise ValueError("expected 11 columns from Alibaba 2018 container_usage.csv")
                value = float(row[3])
                if value < 0 or value == 101 or not math.isfinite(value):
                    raise ValueError("invalid Alibaba CPU utilization; select/clean source explicitly")
                ts = Decimal(row[2]) * 1000
                if ts != int(ts):
                    raise ValueError("sub-millisecond trace timestamp")
                yield "alibaba_container_cpu_util", {"machine_id": row[1], "container_id": row[0]}, int(ts), value


def write(root, records, max_samples, provenance=None):
    from prometheus_client import CollectorRegistry
    from prometheus_client.core import Metric
    from prometheus_client.openmetrics.exposition import generate_latest
    count, first, last = 0, None, None
    latest = {}
    root.mkdir(parents=True, exist_ok=False)
    with (root / "samples.openmetrics").open("wb") as prom, (root / "samples.jsonl").open("w") as sql:
        for metric, labels, ts, value in records:
            if count >= max_samples:
                raise ValueError("sample budget exceeded; partial output must not be loaded")
            if not math.isfinite(value) or not isinstance(ts, int) or ts < 0:
                raise ValueError("samples must be finite and have nonnegative integer timestamps")
            key = (metric, tuple(sorted(labels.items())))
            if key in latest and ts <= latest[key]:
                raise ValueError("duplicate or out-of-order sample within a series")
            latest[key] = ts
            family = Metric(metric, "Evaluation sample", "unknown")
            family.add_sample(metric, labels, value, timestamp=ts / 1000)
            class Collector:
                def collect(self):
                    yield family
            registry = CollectorRegistry()
            registry.register(Collector())
            # Historical sample stream, not repeated HELP/TYPE declarations.
            prom.write(b"".join(line + b"\n" for line in generate_latest(registry).splitlines()
                                if not line.startswith(b"#")))
            sql.write(json.dumps(dict(metric=metric, labels=labels, ts_ms=ts, value=value)) + "\n")
            first = min(first, ts) if first is not None else ts
            last = max(last, ts) if last is not None else ts
            count += 1
        prom.write(b"# EOF\n")
    if not count:
        raise ValueError("empty dataset")
    hashes = {}
    for file in (root / "samples.openmetrics", root / "samples.jsonl"):
        digest = hashlib.sha256()
        with file.open("rb") as data:
            for block in iter(lambda: data.read(1024 * 1024), b""):
                digest.update(block)
        hashes[file.name] = digest.hexdigest()
    metadata = dict(samples=count, series=len(latest), start_ms=first, end_ms=last,
                    sha256=hashes, provenance=provenance or {})
    (root / "data-manifest.json").write_text(json.dumps(metadata, indent=2))
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("synthetic", "google", "alibaba"), required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--groups", type=int, default=10)
    parser.add_argument("--members", type=int, default=4)
    parser.add_argument("--start-ms", type=int, default=1700000000000)
    parser.add_argument("--duration-ms", type=int, default=120000)
    parser.add_argument("--max-samples", type=int, default=1000000)
    args = parser.parse_args()
    if min(args.groups, args.members, args.max_samples) < 1 or args.duration_ms < 0 or args.duration_ms % 100:
        parser.error("counts must be positive and duration nonnegative")
    if args.dataset != "synthetic" and not args.input:
        parser.error("trace profiles require --input")
    if args.dataset == "synthetic" and (args.duration_ms // 100 + 1) * args.groups * args.members * 2 > args.max_samples:
        parser.error("synthetic cell exceeds --max-samples; increase budget explicitly")
    records = synthetic(args.groups, args.members, args.start_ms, args.duration_ms) if args.dataset == "synthetic" else trace(args.input, args.dataset)
    provenance = {"dataset": args.dataset, "generator": "prometheus_client", "groups": args.groups if args.dataset == "synthetic" else None,
                  "members": args.members if args.dataset == "synthetic" else None,
                  "scrape_ms": 100 if args.dataset == "synthetic" else None}
    if args.input:
        digest = hashlib.sha256()
        with args.input.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        provenance.update(input=str(args.input.resolve()), input_sha256=digest.hexdigest())
    print(json.dumps(write(args.output, records, args.max_samples, provenance)))


if __name__ == "__main__":
    main()
