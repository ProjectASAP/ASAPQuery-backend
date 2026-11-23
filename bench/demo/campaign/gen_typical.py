"""Write workloads/typical-dashboard*.json (harness manifest format)."""
import os
import copy, json, sys
base = json.load(open(os.environ["ASAPQ_BENCH"] + "/workloads/dashboard-heavy.json"))
M = 60_000
# id, promql, metric, capability, grouping, lookback_ms, refresh_ms
PANELS = [
    ("p01-rate-job-5m", "sum by (job) (rate(data_total[5m]))", "data_total", "sum", ["job"], 5*M, 10_000),
    ("p02-increase-region-1h", "sum by (region) (increase(data_total[1h]))", "data_total", "sum", ["region"], 60*M, 30_000),
    ("p03-avg-job-5m", "avg by (job) (avg_over_time(data[5m]))", "data", "avg", ["job"], 5*M, 10_000),
    ("p04-max-job-15m", "max by (job) (max_over_time(data[15m]))", "data", "max", ["job"], 15*M, 15_000),
    ("p05-topk-rate-5m", "topk(5, sum by (job) (rate(data_total[5m])))", "data_total", "topk", ["job"], 5*M, 15_000),
    ("p06-quantile-job", "quantile by (job) (0.99, data)", "data", "quantile", ["job"], 5*M, 10_000),
    ("p07-quantile-ot-job-15m", "avg by (job) (quantile_over_time(0.99, data[15m]))", "data", "quantile", ["job"], 15*M, 20_000),
    ("p08-avg-service-30m", "avg by (region, service) (avg_over_time(data[30m]))", "data", "avg", ["region", "service"], 30*M, 30_000),
    ("p09-rate-service-1h", "sum by (service) (rate(data_total[1h]))", "data_total", "sum", ["service"], 60*M, 30_000),
    ("p10-max-region-1h", "max by (region) (max_over_time(data[1h]))", "data", "max", ["region"], 60*M, 30_000),
    ("p11-topk-increase-15m", "topk(10, sum by (service, job) (increase(data_total[15m])))", "data_total", "topk", ["service", "job"], 15*M, 20_000),
    ("p12-quantile-ot-region-1h", "avg by (region) (quantile_over_time(0.5, data[1h]))", "data", "quantile", ["region"], 60*M, 30_000),
]
def make(wid, sps, refresh_override, keep, desc):
    w = copy.deepcopy(base)
    w["id"] = wid
    w["description"] = desc
    w["samples_per_sec_per_series"] = sps
    w["data_facts"]["scrape_interval_ms"] = 1000 // sps
    qs = []
    for pid, promql, metric, cap, grouping, lb, refresh in PANELS:
        if keep is not None and pid not in keep:
            continue
        iv = refresh_override or refresh
        rqe = {"accuracy_sla": 0.95, "capability": cap, "grouping_labels": grouping, "id": pid + "/rqe",
               "interval_ms": iv, "lookback_ms": lb, "metric": metric}
        if cap == "quantile":
            rqe["phi"] = 0.99 if "0.99" in promql else 0.5
        qs.append({"grouping": grouping, "id": pid, "interval_ms": iv, "lookback_ms": lb, "promql": promql,
                   "rqes": [rqe], "sql": None, "support": "common-promql"})
    w["queries"] = qs
    return w
keep = None if len(sys.argv) < 2 else set(sys.argv[1].split(","))
D = os.environ["ASAPQ_BENCH"] + "/workloads/"
variants = [
    ("typical-dashboard", 1, None, "Typical dashboard: ~10 panels over data (gauge) and data_total (counter), aggregated by job/service/region, windows 5m-1h, refresh 10-30 s, 1 sample/s per series."),
    ("typical-dashboard-60s", 1, 60_000, "typical-dashboard's panels refreshed every 60 s at 1 sample/s per series: more samples per stored window."),
    ("typical-dashboard-60s-10hz", 10, 60_000, "typical-dashboard's panels refreshed every 60 s at 10 samples/s per series."),
]
for wid, sps, ro, desc in variants:
    json.dump(make(wid, sps, ro, keep, desc), open(D + wid + ".json", "w"), indent=2, sort_keys=True)
    print(D + wid + ".json")
