#!/usr/bin/env python3
"""Summarize starter-runner JSON without discarding infeasible/test failures."""

import argparse
import json
from collections import Counter
from pathlib import Path
from statistics import mean


def summarize(report):
    if report["schema_version"] != 1 or not report["runs"]:
        raise ValueError("expected nonempty version-1 report")
    rows = []
    for method in ("autosketch_adapted", "asapplanner_erp_selector", "grid_oracle"):
        outcomes = [next(o for o in run["outcomes"] if o["method"] == method)
                    for run in report["runs"]]
        selected = [o for o in outcomes if o["selected"] is not None]
        errors = [o["held_out"]["max_normalized_additive_error"] for o in selected]
        rows.append({
            "method": method,
            "runs": len(outcomes),
            "calibration_feasible": len(selected),
            "selected_families": dict(Counter(o["selected"].get("family", "cms") for o in selected)),
            "held_out_pass": sum(o["held_out_pass"] is True for o in outcomes),
            "held_out_fail": sum(o["held_out_pass"] is False for o in outcomes),
            "no_feasible_configuration": len(outcomes) - len(selected),
            "mean_selected_counter_bytes": mean(o["held_out"]["counter_bytes"]
                                                for o in selected) if selected else None,
            "max_held_out_error": max(errors) if errors else None,
        })
    return {
        "backend_revision": report["args"]["backend_revision"],
        "debug_assertions": report["debug_assertions"],
        "events": report["args"]["events"],
        "cardinality": report["args"]["cardinality"],
        "zipf": report["args"]["zipf"],
        "epsilon": report["args"]["epsilon"],
        "memory_budget_bytes": report["args"].get("memory_budget_bytes"),
        "sketches": report["args"].get("sketches", ["cms"]),
        "mean_search_table_evaluations": mean(len(r["search"]["visited"])
                                             for r in report["runs"]),
        "mean_full_grid_calibration_seconds": mean(r["calibration_wall_seconds"]
                                                   for r in report["runs"]),
        "outcomes": rows,
        "interpretation": "Shared-table smoke evaluation, not a system performance claim."
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    print(json.dumps([summarize(json.loads(path.read_text())) for path in args.reports], indent=2))
