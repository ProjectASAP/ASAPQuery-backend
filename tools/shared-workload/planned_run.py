#!/usr/bin/env python3
"""Run one PromQL acceptance case through the production compiler and owned services."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare(data, manifest, query_id, snapshot, end_ms, repetitions):
    metadata = json.loads((data / "data-manifest.json").read_text())
    if metadata.get("provenance", {}).get("dataset") != manifest["dataset"]:
        raise ValueError("dataset profile mismatch")
    selected = [q for q in manifest["queries"] if q["id"] == query_id and q["status"] == "ready"]
    if len(selected) != 1:
        raise ValueError("select exactly one ready PromQL query ID")
    query = selected[0]
    for name, digest in metadata["sha256"].items():
        if sha256(data / name) != digest:
            raise ValueError("dataset hash mismatch")
    end = metadata["end_ms"] if end_ms is None else end_ms
    start = end - (repetitions - 1) * query["interval_ms"]
    if repetitions < 1 or end > metadata["end_ms"] or start < metadata["start_ms"]:
        raise ValueError("repetition interval exceeds loaded data")
    if query["interval_ms"] != 1000 and start - query["window_ms"] < metadata["start_ms"]:
        raise ValueError("first query lacks full temporal history")
    registered = snapshot["query_workload"]
    if registered.get("query_batch") or {q["query"] for q in registered["repeating_queries"]} != {query["promql"]}:
        raise ValueError("costed snapshot must register exactly the selected query")
    if snapshot.get("snapshot_version") != 2 or not snapshot.get("workload_cost_evidence", {}).get("quotes"):
        raise ValueError("measured complete-workload cost evidence is required; discovery/demo costs are not deployment quotes")
    return query, {"upstream_revision": "shared-workload:" + hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
                   "queries": [{"id": query_id, "query": query["promql"], "eval_timestamp_ms": end}]}


def acceptance(folder):
    replay = folder / "trial-1/replay"
    readiness = json.loads((replay / "summary-readiness.json").read_text())
    rows = json.loads((replay / "queries.json").read_text())
    comparison = json.loads((replay / "comparison.json").read_text())
    valid = bool(rows) and readiness["complete"] and all(
        r["execution"] == "warm" and r.get("comparison", {}).get("equal")
        and r["execution_provenance"].get("summary_readout_evaluations", 0) > 0
        and r["execution_provenance"].get("exact_subquery_rpcs") == 0
        and r.get("pair_order") == "backend_first" for r in rows)
    return {"promql_chain_passed": valid, "occurrences": len(rows), "summary_readiness": readiness,
            "pair": comparison["all_requests"], "planning_resources": comparison["planning_resources"],
            "phase_resources": comparison["phase_resources"], "storage": comparison["storage"],
            "isolated_baseline_service": comparison["isolated_baseline_service"],
            "eligible_for_full_system_benefit": False,
            "limitations": ["PromQL-only chain; SQL moving-time installation and MetricsQL orchestration are not covered",
                            "owned baseline/fallback stores are distinct, but runs share a host and overlap in wall time",
                            "resource phases are partial lifecycle evidence, not independently matched full system costs",
                            "post-drain readiness probes warm ASAP caches before measured requests"],
            "artifacts": str(replay.resolve())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("data", "manifest", "snapshot", "compiler", "data-plane", "prometheus", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--query-id", required=True)
    parser.add_argument("--cpu-affinity", required=True)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--end-ms", type=int)
    parser.add_argument("--base-port", type=int, default=29410)
    args = parser.parse_args()
    query, corpus = prepare(args.data, json.loads(args.manifest.read_text()), args.query_id,
                            json.loads(args.snapshot.read_text()), args.end_ms, args.repetitions)
    args.output.mkdir(parents=True, exist_ok=False)
    corpus_path = args.output / "corpus.json"
    save(corpus_path, corpus)
    results = args.output / "run"
    command = [sys.executable, str(Path(__file__).resolve().parents[1] / "o11y-execution/run_comparison.py"),
               "--metrics", str((args.data / "samples.openmetrics").resolve()), "--queries", str(corpus_path.resolve()),
               "--snapshot", str(args.snapshot.resolve()), "--compiler", str(args.compiler.resolve()),
               "--data-plane", str(args.data_plane.resolve()), "--prometheus", str(args.prometheus.resolve()),
               "--output", str(results.resolve()), "--cpu-affinity", args.cpu_affinity,
               "--base-port", str(args.base_port), "--trials", "1", "--repetitions", str(args.repetitions),
               "--evaluation-step-ms", str(query["interval_ms"]), "--backend-first", "--wait-for-completion", "--require-summary-ready"]
    save(args.output / "command.json", command)
    # No subprocess deadline. Fresh services are owned and shut down only after replay returns.
    completed = subprocess.run(command)
    if completed.returncode:
        save(args.output / "acceptance.json", {"promql_chain_passed": False, "returncode": completed.returncode,
                                               "eligible_for_full_system_benefit": False, "artifacts": str(results.resolve())})
        return completed.returncode
    report = acceptance(results)
    save(args.output / "acceptance.json", report)
    return 0 if report["promql_chain_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
