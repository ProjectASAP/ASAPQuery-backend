#!/usr/bin/env python3
"""Markdown tables from report.json files (absolute numbers): per workload
and arm, CPU, memory, storage, ingest and query latency; and per query,
latency and error against Prometheus."""
import json
import sys
from pathlib import Path

ARM_NAMES = {
    "prometheus": "Prometheus",
    "asapquery": "ASAPQuery",
    "backend": "Backend (joint plan)",
    "backend_alone": "Backend (per-query plans merged)",
}


def fmt(v, digits=1):
    if v is None:
        return "–"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def mib(b):
    return f"{b / 2**20:.0f}"


def main(paths):
    reports = [(Path(p).parent.name.split(".")[2], json.loads(Path(p).read_text())) for p in paths]
    print("| Workload | Arm | CPU core-s total (engine / its Prometheus) | CPU core-s query phase | "
          "Peak RSS MiB (engine / its Prometheus) | Disk MiB | Ingest ms per 1 s tick p50 / p99 | "
          "Query ms p50 / p99 (all evaluations) | Refused |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, r in reports:
        for arm, a in r["arms"].items():
            procs = a["processes"]
            engine = [k for k in procs if not k.startswith("feeder") and k != "prometheus"]
            prom = procs.get("prometheus", {})
            if arm == "prometheus":
                cpu = f"{fmt(prom.get('cpu_core_seconds'))}"
                rss = mib(prom.get("peak_rss_bytes", 0))
            else:
                e_cpu = sum(procs[k].get("cpu_core_seconds", 0) for k in engine)
                e_rss = sum(procs[k].get("peak_rss_bytes", 0) for k in engine)
                cpu = f"{fmt(e_cpu + prom.get('cpu_core_seconds', 0))} ({fmt(e_cpu)} / {fmt(prom.get('cpu_core_seconds'))})"
                rss = f"{mib(e_rss + prom.get('peak_rss_bytes', 0))} ({mib(e_rss)} / {mib(prom.get('peak_rss_bytes', 0))})"
            disk = sum((a.get("storage_bytes") or {}).values())
            ing = a["ingest"].get("feeder0", {})
            lat = [q for q in a["queries"].values()]
            refused = sum(q["refused_or_failed"] for q in lat)
            p50, p99 = a.get("query_p50_ms"), a.get("query_p99_ms")
            print(f"| {name} | {ARM_NAMES.get(arm, arm)} | {cpu} | {fmt(a['cpu_core_seconds_query_phase'])} | {rss} "
                  f"| {mib(disk)} | {fmt(ing.get('tick_p50_ms'))} / {fmt(ing.get('tick_p99_ms'))} "
                  f"| {fmt(p50)} / {fmt(p99)} | {refused} |")
    for name, r in reports:
        print(f"\n**{name}** ({r['series']} series; error = max relative error vs Prometheus over all evaluations, "
              f"median in parentheses; meets = ≥70% of evaluations within 5%)\n")
        arms = list(r["arms"])
        print("| Query | " + " | ".join(f"{ARM_NAMES.get(a, a)} p50 / p99 ms | error | meets" for a in arms) + " |")
        print("|---|" + "---:|---:|:-:|" * len(arms))
        for qid in r["arms"][arms[0]]["queries"]:
            cells = []
            for a in arms:
                q = r["arms"][a]["queries"].get(qid)
                if q is None or q["refused_or_failed"] == q["evaluations"]:
                    cells += ["refused", "–", "no"]
                    continue
                err = f"{fmt(q['max_rel_error'], 3)} ({fmt(q.get('median_rel_error'), 3)})"
                if q["min_recall"] is not None and q["min_recall"] < 1:
                    err += f", recall {fmt(q['min_recall'], 2)}"
                if q["refused_or_failed"]:
                    err += f", {q['refused_or_failed']}/{q['evaluations']} refused"
                cells += [f"{fmt(q['p50_ms'])} / {fmt(q['p99_ms'])}", err,
                          "yes" if q["meets_target"] else "no"]
            print(f"| {qid} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1:])
