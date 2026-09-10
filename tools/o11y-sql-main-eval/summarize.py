#!/usr/bin/env python3
"""Join current-main stage and runtime evidence into the acceptance matrix."""
import argparse
import json
from pathlib import Path


def short_reason(reason: str) -> str:
    reason = reason.replace("SQL lowering failed: DataFusion error: ", "")
    reason = reason.replace("SQL lowering failed: invalid QueryPlan: ", "")
    return reason.split("\n", 1)[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--planner", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    planner = json.loads(args.planner.read_text())
    runtime = {row["id"]: row for row in json.loads(args.runtime.read_text())["queries"]}
    rows = []
    for stage in planner["queries"]:
        route = runtime[stage["id"]]
        rows.append({
            "id": stage["id"],
            "operators": stage["operators"],
            "parser_planner": stage["parser_planner"],
            "publication": stage.get("publication", "not_reached"),
            "data_plane_route": route["route"],
            "classification": route["route"],
            "reason": short_reason(stage.get("reason", route.get("reason", ""))),
        })
    counts = {name: sum(row["classification"] == name for row in rows)
              for name in ("warm", "partial_hybrid", "exact_fallback", "failure")}
    args.output.write_text(json.dumps({
        "schema_version": 1,
        "baseline_commit": "791f7d7b0feab3827e7e6f5100e65ef29aec68de",
        "corpus_count": len(rows),
        "counts": counts,
        "queries": rows,
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
