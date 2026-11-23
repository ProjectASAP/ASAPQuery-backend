"""gen_seed862.py: harness manifests (workloads/seed862-<set>-joint.json) from ASAPPlanner's dag-viewer
PlanningWorkloads (tools/dag-viewer/workloads/random-mixed-seed-862*/, all files of a set joined, distinct
(query, interval) pairs): metrics data (gauge) / data_total (counter), labels region/service/job/label_0
(SERIES=10000 -> label_0 = 50), 10 Hz."""
import os
import copy, json, re, sys
base = json.load(open(os.environ["ASAPQ_BENCH"] + "/workloads/dashboard-heavy.json"))
UNITS = {"s": 1000, "m": 60000, "h": 3600000}
CAP = [("quantile_over_time", "quantile"), ("topk", "topk"), ("sum_over_time", "sum"), ("increase", "sum"),
       ("rate", "sum"), ("avg_over_time", "avg"), ("max_over_time", "max"), ("min_over_time", "min"), ("count_over_time", "count")]
for src, name in (("random-mixed-seed-862", "seed862-base-joint"), ("random-mixed-seed-862-long", "seed862-long-joint")):
    j = json.load(open(os.environ["ASAPQ_BENCH"] + f"/tmp/seed862/{src}/joint.json"))
    w = copy.deepcopy(base)
    w["id"], w.pop("metric_names", None)
    w["description"] = (f"ASAPPlanner dag-viewer {src} (origin/main), all 10 workload files joined as one multi-team "
                        f"dashboard: distinct (query, interval) pairs.")
    qs = []
    for i, rq in enumerate(j["query_workload"]["repeating_queries"], 1):
        p = rq["query"]; iv = rq["demand"]["fixed_interval_at"]["interval"]
        lb = max(int(a) * UNITS[b] for a, b in re.findall(r"\[(\d+)([smh])\]", p))
        metric = "data_total" if "data_total" in p else "data"
        cap = next((c for f, c in CAP if f in p), "sum")
        slug = re.sub(r"[^a-z0-9]+", "-", p.lower())[:40].strip("-")
        rqe = {"accuracy_sla": round(1 - rq["requirements"]["accuracy"]["max_error"], 6), "capability": cap,
               "grouping_labels": [], "id": f"q{i:02d}/rqe", "interval_ms": iv, "lookback_ms": lb, "metric": metric}
        if cap == "quantile":
            rqe["phi"] = float(re.search(r"quantile_over_time\(([\d.]+)", p).group(1))
        qs.append({"grouping": [], "id": f"q{i:02d}-{slug}", "interval_ms": iv, "lookback_ms": lb, "promql": p,
                   "metrics": [metric], "rqes": [rqe], "sql": None, "support": "common-promql"})
    w["id"] = name; w["queries"] = qs
    w["notes"] = [f"source: ASAPPlanner origin/main tools/dag-viewer/workloads/{src}/workload-01..10.json joined"]
    json.dump(w, open(os.environ["ASAPQ_BENCH"] + f"/workloads/{name}.json", "w"), indent=2, sort_keys=True)
    print(name, len(qs), "queries; windows", sorted({q["lookback_ms"] // 1000 for q in qs}), "intervals", sorted({q["interval_ms"] for q in qs}))
