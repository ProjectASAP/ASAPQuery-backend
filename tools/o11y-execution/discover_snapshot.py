#!/usr/bin/env python3
"""Register the complete real corpus for candidate discovery, never cost selection.

Unit discovery costs are an explicit uncalibrated enumeration seed. Version 2
without quotes cannot select/deploy. Replace implementation evidence with measured
calibration and re-export candidates before producing final deployment quotes.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import time
from replay import parse_samples


def duration_ms(text):
    units = {"ms": 1, "s": 1000, "m": 60000, "h": 3600000, "d": 86400000, "w": 604800000, "y": 31536000000}
    return sum(int(n) * units[u] for n, u in re.findall(r"(\d+)(ms|[smhdwy])", text))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval-ms", type=int, default=60000, help="declared repeated-query experiment demand")
    parser.add_argument("--repetitions", type=int, default=20, help="query evaluations per bounded replay batch")
    args = parser.parse_args()
    corpus = json.loads(args.corpus.read_text())
    rows = parse_samples(args.metrics.read_text().splitlines())
    snapshot = json.loads(args.template.read_text())
    now = int(time.time() * 1000)
    input_span = (rows[-1][2] - rows[0][2]) / 1000
    horizon = args.repetitions * args.interval_ms / 1000
    if input_span <= 0 or horizon <= 0 or args.interval_ms <= 0:
        raise ValueError("positive input horizon and recurrence required")
    def evidence(value):
        return {"value": value, "source": "observed", "observed_at_ms": now, "valid_for_ms": 86400000}
    data = snapshot["data_workload"]
    data.update(ingestion_volume=evidence(len(rows)), ingestion_rate=evidence(len(rows)/horizon),
                input_cardinality=evidence(len({tuple(sorted(labels.items())) for labels, _, _ in rows})))
    data["ingestion_rate"]["source"] = "derived"
    registrations, query_audit = [], []
    frequencies = Counter(row["query"] for row in corpus["queries"])
    for query in dict.fromkeys(row["query"] for row in corpus["queries"]):
        windows = [duration_ms(x) for x in re.findall(r"\[([0-9a-z]+)(?::[^\]]*)?\]", query)]
        lookback = max(windows, default=300000)
        if args.interval_ms % frequencies[query]:
            raise ValueError("base interval must divide exactly by query occurrence frequency")
        interval = args.interval_ms // frequencies[query]
        registrations.append({"query": query, "demand": {"fixed_interval": interval},
                              "requirements": {"accuracy": {"explicit": "Exact"}, "response_latency": "unspecified"},
                              "predictability": {"predictable": {"known_at": None}},
                              "time_selection": {"scope": "real_time", "lookback": lookback, "as_of": None}})
        query_audit.append({"query": query, "window_lookback_ms": lookback,
                            "occurrence_count": frequencies[query], "expected_evaluations": frequencies[query] * args.repetitions,
                            "declared_interval_ms": interval,
                            "lookback_method": "largest explicit range; instant selector defaults to Prometheus 5m; original offsets/subqueries preserved in query"})
    snapshot["query_workload"].update(repeating_queries=registrations, data_workload=data, query_batch=None)
    snapshot["snapshot_version"] = 2
    snapshot.pop("workload_cost_evidence", None)
    implementation = snapshot["implementation"]
    implementation.update(evidence_observed_at_unix_ms=now, evidence_valid_for_ms=86400000, horizon_seconds=horizon)
    implementation["lifecycle_costs"] = dict.fromkeys(("build", "maintenance_per_update", "read", "retention_per_second", "retirement"), 1.0)
    implementation["implementation_cost"].update(model_version="UNCALIBRATED-enumeration-only", observed_at_unix_ms=now,
        valid_for_ms=86400000, horizon_seconds=horizon, cpu_cost=1.0, peak_memory_bytes=0, network_bytes=0,
        storage_bytes=0, source_scan_bytes=0, weighted_cost=1.0)
    snapshot["environment"].update(observed_at_unix_ms=now, activation_unix_ms=rows[0][2], max_evidence_age_ms=86400000,
                                  capability_snapshot_id="o11y-backend-local-calibration-v1")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2) + "\n")
    args.output.with_suffix(".provenance.json").write_text(json.dumps({
        "purpose": "candidate discovery only; uncalibrated costs are NOT execution quotes or benefit evidence",
        "input_sha256": hashlib.sha256(args.metrics.read_bytes()).hexdigest(),
        "corpus_sha256": hashlib.sha256(args.corpus.read_bytes()).hexdigest(),
        "upstream_revision": corpus["upstream_revision"], "sample_count": len(rows),
        "series_count": data["input_cardinality"]["value"], "first_timestamp_ms": rows[0][2], "last_timestamp_ms": rows[-1][2],
        "declared_query_interval_ms": args.interval_ms, "repetitions": args.repetitions,
        "input_span_seconds": input_span, "cost_horizon_seconds": horizon,
        "event_time_ingestion_rate": len(rows) / input_span,
        "derived_bounded_replay_ingestion_rate": len(rows) / horizon,
        "cost_scope": "bounded accelerated replay: entire finite input once plus declared query repetitions; logical horizon is not wall-clock residency",
        "accuracy": "exact",
        "queries": query_audit}, indent=2) + "\n")


if __name__ == "__main__":
    main()
