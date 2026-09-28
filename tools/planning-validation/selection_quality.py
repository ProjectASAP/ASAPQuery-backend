#!/usr/bin/env python3
"""Audit measured plan selection. Missing evidence fails; this does not collect telemetry."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, name, positive=False):
    require(isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and (value > 0 if positive else value >= 0),
            f"invalid {name}")
    return value


def artifact(root, ref):
    path = (root / ref["path"]).resolve()
    require(path.is_relative_to(root.resolve()), "artifact outside experiment directory")
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == ref["sha256"], "artifact checksum mismatch")
    return raw


def evaluate(root, experiment):
    require(experiment["schema_version"] == 1, "unsupported experiment version")
    require(experiment["workload_kind"] == "real_trace", "real trace required")
    require(experiment["scope"] in ("single_query", "full_workload"), "unknown workload scope")
    require(experiment["objective"] == "total_cpu_seconds", "CPU objective required")
    start, end = experiment["evaluation_interval_ms"]
    require(number(start, "start") < number(end, "end"), "empty evaluation interval")
    require(number(experiment["calibration_end_ms"], "calibration end") <= start,
            "calibration overlaps evaluation")
    for key in ("machine", "backend_revision", "planner_revision"):
        require(bool(experiment["environment"].get(key)), f"missing environment {key}")
    horizon = number(experiment["horizon_seconds"], "horizon", positive=True)
    latency = number(experiment["limits"]["p95_latency_ms"], "latency limit", positive=True)
    memory = number(experiment["limits"]["peak_memory_bytes"], "memory limit", positive=True)
    # Hashes provide artifact integrity, not an authenticity certificate.
    require(bool(artifact(root, experiment["trace"])), "empty trace")
    evidence_contents = {}
    for name in ("statistics", "accuracy", "resources"):
        evidence = experiment["evidence"][name]
        require(evidence["origin"] == "measured", f"{name} is not measured evidence")
        require(evidence["workload_sha256"] == experiment["trace"]["sha256"],
                f"{name} workload mismatch")
        observed = number(evidence["observed_at_ms"], "observation time")
        require(observed <= start and observed + number(evidence["valid_for_ms"], "validity") >= end,
                f"{name} expired or from the future")
        evidence_contents[name] = json.loads(artifact(root, evidence))
    report = json.loads(artifact(root, experiment["plan"]))["cost_comparison"]
    resources = evidence_contents["resources"]
    require(resources["model_version"] == report["model_version"], "priced resource model mismatch")
    require(resources["data_snapshot_id"] == report["data_snapshot_id"]
            == evidence_contents["statistics"]["data_snapshot_id"], "priced observation generation mismatch")
    require(resources["objective"] == experiment["objective"], "priced objective mismatch")
    for key in ("backend_revision", "planner_revision"):
        require(resources[key] == experiment["environment"][key], f"priced {key} mismatch")
    candidates = report["candidate_evaluations"]
    # Runtime constraints can be evaluated only for candidates admitted to pricing.
    admitted = [c for c in candidates if c["status"] in ("selected", "unselected")]
    require(bool(admitted), "no admitted candidates")
    for candidate in candidates:
        if candidate not in admitted:
            require(bool(candidate.get("unavailable_reason")), "unexplained candidate rejection")
    def identity(candidate):
        return candidate.get("physical_candidate_id") or candidate.get("candidate_id")
    selected = [identity(c) for c in admitted if c["status"] == "selected"]
    require(len(selected) == 1 and selected[0], "one identified selected candidate required")
    expected = {identity(c) for c in admitted}
    require(None not in expected, "candidate identity missing")
    predictions = {}
    for candidate in admitted:
        key = identity(candidate)
        cost = number(candidate["total_cost"], "predicted cost")
        require(key not in predictions or predictions[key] == cost, "conflicting candidate predictions")
        predictions[key] = cost
    runs = experiment["candidates"]
    require(len(runs) == len({r["candidate_id"] for r in runs}), "duplicate candidate measurements")
    require({r["candidate_id"] for r in runs} == expected, "incomplete candidate measurements")
    results = []
    for run in runs:
        measured = json.loads(artifact(root, run["measurements"]))
        require(measured["candidate_id"] == run["candidate_id"], "measurement candidate mismatch")
        require(measured["environment"] == experiment["environment"], "measurement environment mismatch")
        require(measured["workload_sha256"] == experiment["trace"]["sha256"], "measurement workload mismatch")
        require(measured["horizon_seconds"] == horizon, "measurement horizon mismatch")
        trials = measured["trials"]
        require(len(trials) >= 3, "at least three measured repetitions required")
        cpus, latencies, memories = [], [], []
        accurate = True
        for trial in trials:
            cpus.append(number(trial["total_cpu_seconds"], "total CPU"))
            latencies.append(number(trial["p95_latency_ms"], "p95 latency"))
            memories.append(number(trial["peak_memory_bytes"], "peak memory"))
            correctness = json.loads(artifact(root, trial["correctness"]))
            require(correctness["candidate_id"] == run["candidate_id"], "correctness candidate mismatch")
            require(correctness["workload_sha256"] == experiment["trace"]["sha256"], "correctness workload mismatch")
            require(type(correctness["passed"]) is bool, "correctness must be boolean")
            accurate &= correctness["passed"]
        results.append({"candidate_id": run["candidate_id"], "median_cpu_seconds": statistics.median(cpus),
                        "observed_cpu_range": [min(cpus), max(cpus)],
                        "predicted_cost": predictions[run["candidate_id"]], "accurate": accurate,
                        "within_limits": max(latencies) <= latency and max(memories) <= memory})
    feasible = [r for r in results if r["accurate"] and r["within_limits"]]
    require(bool(feasible), "no measured feasible candidate")
    best = min(r["median_cpu_seconds"] for r in feasible)
    winner = next(r for r in results if r["candidate_id"] == selected[0])
    regret = winner["median_cpu_seconds"] - best
    # Report raw ranges; they are not confidence intervals or a tolerance waiver.
    return {"scope": experiment["scope"], "objective": experiment["objective"],
            "selected_candidate": selected[0], "candidate_results": results,
            "selection_regret_cpu_seconds": regret,
            "passed": all(r["accurate"] for r in results) and winner["within_limits"] and regret <= 0,
            "interpretation": "repeated-run median comparison, not proof of a production optimum"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("experiment", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = evaluate(args.experiment.parent, json.loads(args.experiment.read_text()))
    except (KeyError, ValueError, OSError, TypeError) as error:
        result = {"passed": False, "status": "incomplete_or_invalid_evidence", "reason": str(error)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
