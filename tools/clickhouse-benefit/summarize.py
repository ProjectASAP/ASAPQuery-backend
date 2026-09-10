#!/usr/bin/env python3
"""Summarize a q05 probe while retaining its execution and planning limitations."""

import argparse
import json
import math
import statistics
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    args = parser.parse_args()
    data = json.loads(args.artifact.read_text())
    ticks = data["clock_ticks_per_second"]
    routes = {}
    for route in ("warm", "exact"):
        rows = [row for row in data["requests"] if row["route"] == route]
        latencies = sorted(row["elapsed_ns"] / 1e6 for row in rows)
        cpu = {}
        for process in ("backend", "clickhouse"):
            available = [row for row in rows if row.get(f"{process}_before") and row.get(f"{process}_after")]
            cpu[process + "_cpu_ms"] = (
                sum(row[f"{process}_after"]["cpu_ticks"] - row[f"{process}_before"]["cpu_ticks"]
                    for row in available) * 1000 / ticks
                if len(available) == len(rows) else None
            )
        routes[route] = {"requests": len(rows), "median_ms": statistics.median(latencies),
                         "p95_ms": latencies[math.ceil(len(latencies) * .95) - 1],
                         "all_http_200": all(row["status"] == 200 for row in rows),
                         "execution_modes": sorted({str(row["execution"]) for row in rows}), **cpu}
    summary = {"artifact": str(args.artifact.resolve()), "git_head": data["git_head"],
               "input": data.get("input"), "planning_scope": data.get("planning_scope"),
               "routes": routes, "latency_speedup": routes["exact"]["median_ms"] / routes["warm"]["median_ms"],
               "first_query": data.get("first_query"), "build_phase": data["build_phase"],
               "query_phase": data["query_phase"],
               "clickhouse_table": data.get("clickhouse_table"),
               "limitations": data["limitations"] + ["CPU counters include server background work and have scheduler-tick precision"]}
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
