#!/usr/bin/env python3
"""Turn isolated, inclusive candidate CPU measurements into complete cost quotes.

This provider selects no winner. All candidate measurements include backend and
its exact service, when used. Horizon CPU includes installation, raw ingestion,
state building/updating, residency and retirement. Query CPU includes every
operator, fallback, and response encoding. Inclusive totals are attributed once;
zero entries mean subsumed work, never an assertion of free execution. Memory
and storage remain separately reported quantities, not invented CPU conversions.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def nonnegative(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite nonnegative measured CPU nanoseconds")
    return value


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def calibrate(candidates, measurements, data_snapshot_id, observed_at_unix_ms, valid_for_ms):
    identity = candidates.get("compiler_identity")
    if not identity or measurements.get("compiler_identity") != identity:
        raise ValueError("candidate and measurement compiler identities differ or are missing")
    if measurements.get("units") != "cpu_ns" or measurements.get("data_snapshot_id") != data_snapshot_id:
        raise ValueError("measurement units or data identity mismatch")
    by_id = {}
    for row in measurements["candidates"]:
        key = row["plan_id"]
        if key in by_id:
            raise ValueError("duplicate measured candidate")
        by_id[key] = row
    quotes, attribution, unavailable = [], [], []
    for artifact in candidates["candidates"]:
        if "manifest" not in artifact:
            unavailable.append(artifact)
            continue
        manifest = artifact["manifest"]
        pid = manifest["plan_id"]
        measured = by_id.get(pid)
        if not measured or not measured.get("executable"):
            unavailable.append({"plan_id": pid, "reason": (measured or {}).get("unavailable_reason", "not measured")})
            continue
        if measured.get("manifest") != manifest:
            raise ValueError(f"candidate {pid}: measured manifest differs")
        if measured.get("horizon_seconds") != manifest["horizon_seconds"]:
            raise ValueError(f"candidate {pid}: measured horizon differs")
        required_phases = {"install", "ingest_and_build", "residency", "retirement"}
        phases = measured.get("horizon_phases", {})
        if set(phases) != required_phases:
            raise ValueError(f"candidate {pid}: incomplete horizon phases")
        horizon_cpu = sum(nonnegative(phases[p]["cpu_ns"], p) for p in required_phases)
        if any(not phases[p].get("raw_measurement_file") for p in required_phases):
            raise ValueError("each phase requires a raw measurement artifact")
        costs = {key: 0.0 for key in manifest["components"]}
        horizon_keys = sorted(k for k, v in manifest["components"].items() if v["unit"] == "horizon")
        if not horizon_keys:
            raise ValueError("no horizon component for inclusive setup/upkeep CPU")
        costs[horizon_keys[0]] = horizon_cpu
        allocation = {horizon_keys[0]: {"inclusive_of": horizon_keys, "cpu_ns": horizon_cpu}}
        query_measurements = measured.get("queries", {})
        if set(query_measurements) != set(manifest["workload"]):
            raise ValueError(f"candidate {pid}: incomplete query execution coverage")
        for qid in manifest["workload"]:
            row = query_measurements[qid]
            count = row["evaluations"]
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ValueError("query evaluation count must be positive")
            if row.get("classification") not in ("warm", "hybrid", "exact_fallback") or not row.get("correct"):
                raise ValueError(f"candidate {pid}: {qid} failed execution/correctness validation")
            if not row.get("raw_measurement_file"):
                raise ValueError("each query requires a raw measurement artifact")
            keys = sorted(k for k in costs if k.startswith(f"query:{qid}:") or k == f"result:{qid}")
            if not keys or any(manifest["components"][k]["unit"] != "query_evaluation" for k in keys):
                raise ValueError("missing per-query components")
            cpu = nonnegative(row["cpu_ns"], qid) / count
            costs[keys[0]] = cpu
            allocation[keys[0]] = {"inclusive_of": keys, "cpu_ns_per_evaluation": cpu}
        quotes.append({"manifest": manifest, "executable": True, "unit_costs": costs})
        attribution.append({"plan_id": pid, "allocation": allocation,
                            "resources": measured.get("resources"),
                            "scope": "backend plus any exact fallback service; complete inclusive CPU; memory/storage reported separately"})
    return ({**identity, "data_snapshot_id": data_snapshot_id, "model_version": "measured-inclusive-cpu-ns-v1",
             "observed_at_unix_ms": observed_at_unix_ms, "valid_for_ms": valid_for_ms, "quotes": quotes},
            {"attribution": attribution, "unavailable": unavailable})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    snapshot = json.loads(args.snapshot.read_text())
    evidence, audit = calibrate(json.loads(args.candidates.read_text()), json.loads(args.measurements.read_text()),
                                "sha256:" + digest(args.metrics), snapshot["environment"]["observed_at_unix_ms"],
                                snapshot["environment"]["max_evidence_age_ms"])
    snapshot["snapshot_version"] = 2
    snapshot["workload_cost_evidence"] = evidence
    args.output.write_text(json.dumps(snapshot, indent=2, allow_nan=False) + "\n")
    args.output.with_suffix(".calibration.json").write_text(json.dumps(audit, indent=2, allow_nan=False) + "\n")
    if not evidence["quotes"]:
        raise SystemExit("no completely measured executable candidates; audit saved, selection must fail closed")


if __name__ == "__main__":
    main()
