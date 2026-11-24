#!/usr/bin/env python3
"""The presentation dashboard's Planner row: ASAPPlanner's plan report
(asap_planner --report-file, cost model 3 lowest_mean_latency) as metrics
for node-exporter's textfile collector.

    planner_report_to_textfile.py --report report.json --cost-inputs cost_inputs.json \
        --queries queries.json --planning planning.json --textfile monitoring/textfile/asap_demo_planner.prom

  asap_demo_planner_limit{resource}          cpu_cores, memory_bytes, disk_bytes (the cost inputs' ClusterLimits)
  asap_demo_planner_predicted{resource}      peak_cpu_cores, peak_memory_bytes, disk_bytes of the chosen plan
  asap_demo_planner_binds{resource}          1 when the prediction is at least --binds-at of the limit
  asap_demo_planner_mean_latency_seconds     evaluations-weighted mean of the per-query latencies
  asap_demo_planner_query_latency_seconds{query,part}   lateness, queueing, precompute, execution
  asap_demo_planner_recommended_threads{role}            ingest, precompute, query
  asap_demo_planner_precompute_load          fraction of the precompute threads' time spent refreshing
  asap_demo_planner_planning_seconds, asap_demo_planner_planning_peak_rss_bytes   (--planning: wall, peak RSS)
"""
import argparse
import json
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", required=True)
    ap.add_argument("--cost-inputs", required=True)
    ap.add_argument("--queries", required=True, help="queries.json, for the query names in workload order")
    ap.add_argument("--planning", help='{"planning_seconds": .., "planning_peak_rss_bytes": ..}')
    ap.add_argument("--binds-at", type=float, default=0.9)
    ap.add_argument("--textfile", required=True)
    args = ap.parse_args()
    report = json.loads(Path(args.report).read_text())
    usage = report["predicted_usage"]
    limits = json.loads(Path(args.cost_inputs).read_text())["cost_model"]["cluster_capacity"]
    names = [q["id"].split("-", 1)[0] + " " + q["promql"] for q in json.loads(Path(args.queries).read_text())["queries"]]
    lines = []

    def m(name, labels, value):
        if value is None:
            return
        lbl = ",".join(f'{k}="{str(v).replace(chr(92), chr(92) * 2).replace(chr(34), chr(92) + chr(34))}"'
                       for k, v in labels.items())
        lines.append(f"{name}{{{lbl}}} {value}" if lbl else f"{name} {value}")

    for resource, limit, predicted in (("cpu_cores", limits.get("cpu_cores"), usage["peak_cpu_cores"]),
                                       ("memory_bytes", limits["memory_bytes"], usage["peak_memory_bytes"]),
                                       ("disk_bytes", limits["disk_bytes"], usage["disk_bytes"])):
        m("asap_demo_planner_limit", {"resource": resource}, limit)
        m("asap_demo_planner_predicted", {"resource": resource}, predicted)
        if limit:
            m("asap_demo_planner_binds", {"resource": resource}, int(predicted >= args.binds_at * limit))
    total_w, total = 0.0, 0.0
    for i, q in enumerate(usage.get("per_query", [])):
        name = names[i] if i < len(names) else f"q{i + 1}"
        parts = {"lateness": q.get("allowed_lateness_ms", 0), "queueing": q.get("precompute_queueing_ms", 0),
                 "precompute": q.get("precompute_execution_ms", 0), "execution": q.get("query_execution_ms", 0)}
        for part, ms in parts.items():
            m("asap_demo_planner_query_latency_seconds", {"query": name, "part": part}, ms / 1e3)
        latency = q.get("latency_ms") or sum(parts.values())
        w = q.get("evaluations_per_second", 0)
        total_w += w
        total += w * latency
    if total_w:
        m("asap_demo_planner_mean_latency_seconds", {}, total / total_w / 1e3)
    for role, threads in (usage.get("recommended_configuration") or {}).items():
        m("asap_demo_planner_recommended_threads", {"role": role}, threads.get("recommended_threads"))
    m("asap_demo_planner_precompute_load", {}, usage.get("precompute_load"))
    if args.planning:
        p = json.loads(Path(args.planning).read_text())
        m("asap_demo_planner_planning_seconds", {}, p.get("planning_seconds"))
        m("asap_demo_planner_planning_peak_rss_bytes", {}, p.get("planning_peak_rss_bytes"))
    out = Path(args.textfile)
    tmp = out.with_name("." + out.name + ".tmp")
    tmp.write_text("\n".join(lines) + "\n")
    tmp.rename(out)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
