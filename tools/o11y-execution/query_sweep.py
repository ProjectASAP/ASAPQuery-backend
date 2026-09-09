#!/usr/bin/env python3
"""Fresh paired repetition sweeps of a normally selected, explicitly scoped workload."""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def trial_summary(folder):
    report = json.loads((folder / "replay/comparison.json").read_text())
    batch = json.loads((folder / "replay/query-batch-resources.json").read_text())
    phases = report["process_phases"]
    def cpu(snapshot, names):
        values = [snapshot.get(name, {}).get("cpu_ns") if snapshot.get(name) else None for name in names]
        return sum(values) if all(value is not None for value in values) else None
    sides = {"backend_plus_fallback": ["backend", "fallback_service"], "prometheus": ["exact_service"]}
    result = {"correctness_and_latency": report["all_requests"], "execution_details": report["by_execution_detail"],
              "estimated_selection": report["estimated_cost"], "units": "cpu_ns", "sides": {},
              "scope": "Setup includes process startup, planning on backend side, full input admission/build; all process background CPU charged. Retirement excluded."}
    for side, names in sides.items():
        setup = cpu(phases["after_ingest_and_drain"], names)
        total = cpu(phases["after_queries"], names)
        if side == "backend_plus_fallback":
            planning = report["planning_resources"].get("cpu_ns")
            setup = setup + planning if setup is not None and planning is not None else None
            total = total + planning if total is not None and planning is not None else None
        query = cpu(batch["resources"], names)
        result["sides"][side] = {"setup_and_all_updates_cpu_ns": setup, "setup_update_query_cpu_ns": total,
            "query_batch_cpu_ns": query, "first_pass_cpu_ns": cpu(batch["first_pass_resources"], names),
            "repeated_queries_cpu_ns": cpu(batch["repeat_resources"], names),
            "query_cpu_censored": query is None or query < 10 * batch["cpu_tick_ns"]}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("prometheus", "metrics", "queries", "snapshot", "compiler", "data-plane", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--cpu-affinity", required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--advance-step-ms", type=int, default=60000)
    parser.add_argument("--base-port", type=int, default=19600)
    parser.add_argument("--scope", choices=("full_upstream_corpus", "original_occurrence_subset", "derived_subquery_child"), required=True)
    args = parser.parse_args()
    if args.advance_step_ms <= 0 or args.trials < 1:
        parser.error("advancing step and trials must be positive")
    # Check the complete advancing grid against actual retained input before starting services.
    from replay import parse_samples
    samples = parse_samples(args.metrics.read_text().splitlines())
    first, last = min(row[2] for row in samples), max(row[2] for row in samples)
    corpus = json.loads(args.queries.read_text())
    for row in corpus["queries"]:
        if not first <= row["eval_timestamp_ms"] - 99 * args.advance_step_ms <= row["eval_timestamp_ms"] <= last:
            parser.error("100-repeat advancing grid must stay within input timestamps; reduce step or use a later endpoint")
    del samples
    args.output.mkdir(parents=True, exist_ok=False)
    cases = []
    for mode in ("fixed", "advancing"):
        for repetitions in (1, 5, 20, 100):
            folder = args.output / f"{mode}-{repetitions}"
            command = [sys.executable, str(Path(__file__).with_name("run_comparison.py"))]
            for name in ("prometheus", "metrics", "queries", "snapshot", "compiler", "data_plane"):
                command += ["--" + name.replace("_", "-"), str(getattr(args, name).resolve())]
            command += ["--output", str(folder.resolve()), "--trials", str(args.trials),
                        "--repetitions", str(repetitions), "--cpu-affinity", args.cpu_affinity,
                        "--base-port", str(args.base_port + len(cases) * args.trials * 3),
                        "--batch-resources", "--evaluation-step-ms", str(args.advance_step_ms if mode == "advancing" else 0)]
            cases.append({"mode": mode, "repetitions": repetitions, "command": command})
    manifest = {"scope": args.scope, "cases": cases, "input_timestamp_ms": [first, last],
                "selection": "Each fresh case invokes normal Planner selection from the supplied measured snapshot; no winning artifact override",
                "demand_scope": "Repetition sweep amortizes the supplied selected workload; it does not recalibrate or reselect cost quotes for each repetition count",
                "advancing_scope": "Same complete data preloaded on both sides; query time advances ending at original timestamp, not continuous ingestion",
                "cold_scope": "First pass of fresh process, not evicted OS cache; repeat pass reported separately",
                "coverage": "Derived child cases are supplementary and never replace full 28-occurrence regression"}
    (args.output / "sweep.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for case in cases:
        subprocess.run(case["command"], check=True)
        folder = Path(case["command"][case["command"].index("--output") + 1])
        summary = {"case": case, "trials": [trial_summary(folder / f"trial-{trial}") for trial in range(1, args.trials + 1)]}
        (folder / "sweep-summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
