#!/usr/bin/env python3
"""Markdown tables from report.json files: one resource table over arms and
workloads, and one per-query table per workload."""
import json
import sys
from pathlib import Path


def fmt(v, digits=1):
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def main(paths):
    reports = [(Path(p).parent.name, json.loads(Path(p).read_text())) for p in paths]
    print("| Workload | Arm | CPU core-s (total) | CPU core-s (query phase) | Peak RSS MiB (sum) | Forwarded | Refused |")
    print("|---|---|---:|---:|---:|---:|---:|")
    for name, r in reports:
        for arm, a in r["arms"].items():
            refused = sum(q["refused_or_failed"] for q in a["queries"].values())
            print(f"| {name} | {arm} | {fmt(a['cpu_core_seconds'])} | {fmt(a['cpu_core_seconds_query_phase'])} "
                  f"| {a['peak_rss_bytes_sum'] / 2**20:.0f} | {a['forwarded']} | {refused} |")
    for name, r in reports:
        print(f"\n**{name}** ({r['series']} series)\n")
        arms = list(r["arms"])
        print("| Query | " + " | ".join(f"{a} p50/p99 ms | {a} max err | {a} meets" for a in arms) + " |")
        print("|---|" + "---:|---:|:-:|" * len(arms))
        for qid in r["arms"][arms[0]]["queries"]:
            cells = []
            for a in arms:
                q = r["arms"][a]["queries"].get(qid)
                if q is None:
                    cells += ["–", "–", "–"]
                    continue
                if q["refused_or_failed"] == q["evaluations"]:
                    cells += ["refused", "–", "no"]
                    continue
                err = q["max_rel_error"]
                if q["min_recall"] is not None and q["min_recall"] < 1:
                    err_cell = f"{fmt(err, 3)} (recall {fmt(q['min_recall'], 2)})"
                else:
                    err_cell = fmt(err, 3)
                cells += [f"{fmt(q['p50_ms'])} / {fmt(q['p99_ms'])}", err_cell,
                          "yes" if q["meets_target"] else "no"]
            print(f"| {qid} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
