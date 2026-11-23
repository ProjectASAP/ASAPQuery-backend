"""Write workloads/asap-demo-complex.json (windows x1/6 for the e2e smoke) and
asap-demo-complex-full.json (the original windows, for planning)."""
import os
import copy, json, re
base = json.load(open(os.environ["ASAPQ_BENCH"] + "/workloads/dashboard-heavy.json"))
HIER = [{"name": "region", "cardinality": 4}, {"name": "service", "cardinality": 5, "child_of": "region"},
        {"name": "job", "cardinality": 10, "child_of": "service"}]
def metric(kind, extra_labels, n):
    labels = copy.deepcopy(HIER) + extra_labels
    vals = ({"value": {"zipf": 1.1, "keys": n}, "dist": {"pareto": 2.0}} if kind == "gauge"
            else {"increment": {"zipf": 1.1, "keys": n}})
    return {"type": kind, "labels": labels, "samples_per_sec_per_series": 10, "values": vals}
inst = [{"name": "instance", "cardinality": 5, "child_of": "job"}]
le = [{"name": "le", "cardinality": 8, "values": ["0.005", "0.01", "0.05", "0.1", "0.5", "1", "5", "+Inf"]}]
EXTRA = {
    "http_requests_total": metric("counter", [], 200),
    "capacity_rps": metric("gauge", [], 200),
    "cache_hits_total": metric("counter", inst, 1000),
    "cache_lookups_total": metric("counter", inst, 1000),
    "http_request_duration_seconds_bucket": metric("counter", le, 1600),
    "queue_depth": metric("gauge", inst, 1000),
}
QUERIES = [  # id, promql (original windows), metrics, capability
    ("A-error-ratio", "(increase(http_errors_total[6h]) / on(job) group_left() increase(http_requests_total[6h])) > 0.01",
     ["http_errors_total", "http_requests_total"], "sum"),
    ("B-capacity", "(rate(http_requests_total[30s]) * 100 / on(job) group_left() max_over_time(capacity_rps[6h])) >= 80",
     ["http_requests_total", "capacity_rps"], "max"),
    ("C-cache-hit", "(avg by (region, service, job) (rate(cache_hits_total[60s]) / rate(cache_lookups_total[60s])) * 100) >= 90",
     ["cache_hits_total", "cache_lookups_total"], "sum"),
    ("D-count-regions", "count by (region) (sum by (region, service) (increase(http_requests_total[30m])) > 1000)",
     ["http_requests_total"], "sum"),
    ("E-quantile", "quantile_over_time(0.99, request_latency_seconds[6h])", ["request_latency_seconds"], "quantile"),
    ("G1-topk", "topk(10, increase(http_requests_total[1h]))", ["http_requests_total"], "topk"),
    ("H-histogram", "histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[1h])))",
     ["http_request_duration_seconds_bucket"], "quantile"),
    ("I-max-sum", "sum by (region) (max by (region, service) (max_over_time(queue_depth[1h])))", ["queue_depth"], "max"),
    ("J-max-offset", "max by (job) (max_over_time(queue_depth[1h])) > max by (job) (max_over_time(queue_depth[1h] offset 1h))",
     ["queue_depth"], "max"),
    ("K-ratio", "sum(increase(http_errors_total[1h])) / sum(increase(http_requests_total[1h]))",
     ["http_errors_total", "http_requests_total"], "sum"),
]
UNITS = {"s": 1000, "m": 60000, "h": 3600000}
def scale(promql, f):
    def rep(m):
        ms = int(m.group(1)) * UNITS[m.group(2)] // f
        return (m.group(0)[0] if m.group(0)[0] in "[ " else "") + (f"{ms // 60000}m" if ms % 60000 == 0 else f"{ms // 1000}s")
    out = re.sub(r"\[(\d+)([smh])\]", lambda m: "[" + rep(m)[1:] + "]" if False else "[" + _fmt(int(m.group(1)) * UNITS[m.group(2)] // f) + "]", promql)
    return re.sub(r"offset (\d+)([smh])", lambda m: "offset " + _fmt(int(m.group(1)) * UNITS[m.group(2)] // f), out)
def _fmt(ms):
    return f"{ms // 3600000}h" if ms % 3600000 == 0 else f"{ms // 60000}m" if ms % 60000 == 0 else f"{ms // 1000}s"
def lookback(promql):
    w = max(int(a) * UNITS[b] for a, b in re.findall(r"\[(\d+)([smh])\]", promql))
    off = max([int(a) * UNITS[b] for a, b in re.findall(r"offset (\d+)([smh])", promql)] or [0])
    return w + off
def make(f, wid, desc):
    w = copy.deepcopy(base)
    w["id"], w["description"] = wid, desc
    w["metric_names"] = {"data": "request_latency_seconds", "data_total": "http_errors_total"}
    w["extra_metrics"] = EXTRA
    qs = []
    for qid, promql, metrics, cap in QUERIES:
        p = scale(promql, f) if f != 1 else promql
        lb = lookback(p)
        rqe = {"accuracy_sla": 0.95, "capability": cap, "grouping_labels": [], "id": qid + "/rqe", "interval_ms": 15000,
               "lookback_ms": lb, "metric": metrics[0]}
        if cap == "quantile":
            rqe["phi"] = 0.99
        qs.append({"grouping": [], "id": qid, "interval_ms": 15000, "lookback_ms": lb, "promql": p, "metrics": metrics,
                   "rqes": [rqe], "sql": None, "support": "common-promql"})
    w["queries"] = qs
    # Semantically valid data, the same for both engines (feeder `realistic` kinds).
    w["value_overrides"] = {
        "http_request_duration_seconds_bucket": {"kind": "histogram", "obs_max": 20, "obs_s": 1.1,
                                                 "median": 0.05, "sigma": 1.0},
        "cache_hits_total": {"kind": "paired", "of": "cache_lookups_total", "ratio_min": 0.5, "ratio_max": 0.99},
        "http_errors_total": {"kind": "paired", "of": "http_requests_total", "ratio_min": 0.0, "ratio_max": 0.02},
    }
    w["notes"] = [
        "Data are made semantically valid for both engines alike (FEEDER_VALUES=zipf plus value_overrides):",
        "http_request_duration_seconds_bucket: per sample interval of each (region, service, job) a Zipf(1.1) number "
        "of observations in 1..20, each a log-normal latency (median 0.05 s, sigma 1.0); every bucket counts the "
        "observations <= its le, so buckets are cumulative and monotonic across le (+Inf = all); no resets.",
        "cache_hits_total = per sample floor(cache_lookups_total increment x a per-series hit ratio in [0.5, 0.99]), "
        "resetting with lookups: hits <= lookups always.",
        "http_errors_total (5 label_0 series per job) = per sample floor(http_requests_total increment of the job x a "
        "per-series ratio in [0, 0.02]), resetting with requests: errors summed per job <= 10% of requests.",
        "Other counters: Zipf(1.1, N=1e6) integer increments, 5% of series reset every 30-120 min; gauges: Zipf "
        "integers x an integer per-series multiplier.",
        "asap-demo-complex: windows x1/6 of the original (6h->1h, 1h->10m, 30m->5m, 60s->10s, 30s->5s, offset 1h->10m).",
    ]
    return w
D = os.environ["ASAPQ_BENCH"] + "/workloads/"
json.dump(make(6, "asap-demo-complex", "Complex dashboard/alert shapes over generic HTTP/cache/queue metrics; windows x1/6 "
                  "(6h->1h, 1h->10m, 30m->5m, 60s->10s, 30s->5s, offset 1h->10m) for a short e2e smoke."),
          open(D + "asap-demo-complex.json", "w"), indent=2, sort_keys=True)
json.dump(make(1, "asap-demo-complex-full", "asap-demo-complex with the original windows (planning only)."),
          open(D + "asap-demo-complex-full.json", "w"), indent=2, sort_keys=True)
for q in make(6, "x", "")["queries"]:
    print(q["id"], q["lookback_ms"] // 1000, q["promql"])
