"""Write workloads/demo48.json: the 48-core demo workload,
the starting scale of the VictoriaMetrics scale sweep.

All metrics at 10 Hz with integer Zipf values (FEEDER_VALUES=zipf: s=1.1, N=1e6), declared
value ranges, semantically valid data (value_overrides: errors <= requests, hits <= lookups,
cumulative histogram buckets; 5% of counters reset every 30-120 min).

Scale: SERIES=40000 gives label_0 = 200 (data, data_total: 40k series each); every label marked
`scale_with_label0` gets round(label_0 x factor) values, so all metrics scale together.
Metrics joined on(job) as the right side of a group_left (http_requests_total in A,
capacity_rps in B) must have one series per job, so they have 200 series (not 10k)."""
import os
import copy
import json
import re

base = json.load(open(os.environ["ASAPQ_BENCH"] + "/workloads/dashboard-heavy.json"))
HIER = [{"name": "region", "cardinality": 4}, {"name": "service", "cardinality": 5, "child_of": "region"},
        {"name": "job", "cardinality": 10, "child_of": "service"}]
LE = [{"name": "le", "cardinality": 8, "values": ["0.005", "0.01", "0.05", "0.1", "0.5", "1", "5", "+Inf"]}]


def metric(kind, labels):
    n = 1
    for l in labels:
        n *= l["cardinality"]
    vals = ({"value": {"zipf": 1.1, "keys": n}, "dist": {"pareto": 2.0}} if kind == "gauge"
            else {"increment": {"zipf": 1.1, "keys": n}})
    return {"type": kind, "labels": copy.deepcopy(labels), "samples_per_sec_per_series": 10, "values": vals}


def scaled(name, card, factor, **kw):
    return {"name": name, "cardinality": card, "scale_with_label0": factor, **kw}


INST = HIER + [scaled("instance", 50, 0.25, child_of="job")]          # 10k at label_0 = 200
EXTRA = {
    "http_requests_total": metric("counter", HIER),                       # one per job (joined on job)
    "http_errors_total": metric("counter", INST),
    "capacity_rps": metric("gauge", HIER),                                # one per job (joined on job)
    "cache_hits_total": metric("counter", INST),
    "cache_lookups_total": metric("counter", INST),
    "queue_depth": metric("gauge", INST),
    "request_latency_seconds": metric("gauge", INST),
    "http_request_duration_seconds_bucket": metric("counter", INST + LE),
    "power": metric("gauge", [{"name": "region", "cardinality": 4},
                              scaled("host", 2500, 12.5, child_of="region")]),
    "delivery_latency_bucket": metric("counter", [
        {"name": "region", "cardinality": 4}, scaled("host", 250, 1.25, child_of="region"),
        {"name": "kind", "cardinality": 10, "values": ["written", "read", "queued", "retried", "dropped",
                                                       "acked", "nacked", "expired", "deferred", "skipped"]}] + LE),
    "calc_duration_bucket": metric("counter", [
        {"name": "env", "cardinality": 2, "values": ["prod", "staging"]},
        {"name": "service", "cardinality": 50}, scaled("instance", 100, 0.5, child_of="service")] + LE),
    "data_latency_bucket": metric("counter", [
        {"name": "env", "cardinality": 2, "values": ["prod", "staging"]},
        {"name": "app", "cardinality": 20}, scaled("instance", 250, 1.25, child_of="app")] + LE),
}
HIST = {"kind": "histogram", "obs_max": 20, "obs_s": 1.1, "median": 0.05, "sigma": 1.0}
VALUE_OVERRIDES = {
    "http_request_duration_seconds_bucket": HIST, "delivery_latency_bucket": HIST,
    "calc_duration_bucket": HIST, "data_latency_bucket": HIST,
    "cache_hits_total": {"kind": "paired", "of": "cache_lookups_total", "ratio_min": 0.5, "ratio_max": 0.99},
    "http_errors_total": {"kind": "paired", "of": "http_requests_total", "ratio_min": 0.0, "ratio_max": 0.02},
}
# L4: 256 hosts of regions 0 and 1 (128 each), as one alternation.
L4_HOSTS = "|".join(f"{r}\\\\.{i}" for r in (0, 1) for i in range(128))
QUERIES = [  # id, promql, interval s, metrics, capability, range
    ("q01-quantile-1h", "quantile_over_time(0.99, data[1h])", 10, ["data"], "quantile", None),
    ("q02-topk-rate-1h", "topk(10, sum by (region, service, job) (rate(data_total[1h])))", 15, ["data_total"], "topk", None),
    ("q03-max-30m", "max_over_time(data[30m])", 10, ["data"], "max", None),
    ("q04-increase-1h", "sum by (region, service) (increase(data_total[1h]))", 15, ["data_total"], "sum", None),
    ("q05-quantile-1h-range", "quantile_over_time(0.99, data[1h])", 15, ["data"], "quantile",
     {"step_ms": 15000, "span_ms": 3600000}),
    ("A-error-ratio", "(increase(http_errors_total[6h]) / on(job) group_left() increase(http_requests_total[6h])) > 0.01",
     15, ["http_errors_total", "http_requests_total"], "sum", None),
    ("B-capacity", "(rate(http_requests_total[30s]) * 100 / on(job) group_left() max_over_time(capacity_rps[6h])) >= 80",
     15, ["http_requests_total", "capacity_rps"], "max", None),
    ("C-cache-hit", "(avg by (region, service, job) (rate(cache_hits_total[60s]) / rate(cache_lookups_total[60s])) * 100) >= 90",
     15, ["cache_hits_total", "cache_lookups_total"], "sum", None),
    ("D-count-regions", "count by (region) (sum by (region, service) (increase(http_requests_total[30m])) > 1000)",
     15, ["http_requests_total"], "sum", None),
    ("E-quantile", "quantile_over_time(0.99, request_latency_seconds[6h])", 15, ["request_latency_seconds"], "quantile", None),
    ("G1-topk", "topk(10, increase(http_requests_total[1h]))", 15, ["http_requests_total"], "topk", None),
    ("H-histogram", "histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[1h])))",
     15, ["http_request_duration_seconds_bucket"], "quantile", None),
    ("I-max-sum", "sum by (region) (max by (region, service) (max_over_time(queue_depth[1h])))", 15, ["queue_depth"], "max", None),
    ("J-max-offset", "max by (job) (max_over_time(queue_depth[1h])) > max by (job) (max_over_time(queue_depth[1h] offset 1h))",
     15, ["queue_depth"], "max", None),
    ("K-ratio", "sum(increase(http_errors_total[1h])) / sum(increase(http_requests_total[1h]))",
     15, ["http_errors_total", "http_requests_total"], "sum", None),
    ("L1-stddev", 'stddev_over_time(power{host=~".*"}[6h])', 15, ["power"], "stddev", None),
    ("L2-max-regex", 'quantile_over_time(1., power{host=~"2\\\\..*"}[6h])', 15, ["power"], "quantile", None),
    ("L3-quantile", 'quantile_over_time(0.99, power{host=~".*"}[6h])', 15, ["power"], "quantile", None),
    ("L4-hosts", f'sum by (le) (rate(delivery_latency_bucket{{region=~"0|1", host=~"{L4_HOSTS}", kind="written"}}[40s]))',
     15, ["delivery_latency_bucket"], "sum", None),
    ("L5-label-replace", 'histogram_quantile(0.95, sum by (le, svc) (rate(label_replace(calc_duration_bucket{env="prod"}, '
                         '"svc", "$1", "service", "([0-9])[0-9]*")[1m]))) > 2', 15, ["calc_duration_bucket"], "quantile", None),
    # L6: the form `(...) offset 30s` after the aggregation is not valid PromQL; the offset is on
    # the selector, which means the same.
    ("L6-offset", 'histogram_quantile(0.95, sum by (le, app, instance) (rate(data_latency_bucket{app=~"1.*", '
                  'instance=~".*\\\\.1.*", env="prod"}[30s] offset 30s)))', 15, ["data_latency_bucket"], "quantile", None),
]
UNITS = {"s": 1000, "m": 60000, "h": 3600000}
VM_ONLY = ("L1-stddev", "L5-label-replace")


def lookback(promql):
    w = max(int(a) * UNITS[b] for a, b in re.findall(r"\[(\d+)([smh])\]", promql))
    off = max([int(a) * UNITS[b] for a, b in re.findall(r"offset (\d+)([smh])", promql)] or [0])
    return w + off


# 2026-11-22: the demo run is 3 h from zero; every window is capped at 3h so each query
# reads a full window by the end. The original text stays in `promql_original`.
WINDOW_CAP = "3h"


def cap_windows(promql):
    return re.sub(r"\[(\d+)h\]", lambda m: f"[{WINDOW_CAP}]" if int(m.group(1)) > 3 else m.group(0), promql)


w = copy.deepcopy(base)
w["id"] = "demo48"
w["description"] = ("48-core demo workload: 21 queries over data/data_total and HTTP, cache, "
                    "queue, power and histogram metrics, 10 Hz integer Zipf data.")
w.pop("metric_names", None)
w["extra_metrics"] = EXTRA
w["value_overrides"] = VALUE_OVERRIDES
qs = []
for qid, promql_original, interval_s, metrics, cap, rng in QUERIES:
    promql = cap_windows(promql_original)
    lb = lookback(promql)
    rqe = {"accuracy_sla": 0.95, "capability": cap, "grouping_labels": [], "id": qid + "/rqe",
           "interval_ms": interval_s * 1000, "lookback_ms": lb, "metric": metrics[0]}
    if cap == "quantile":
        rqe["phi"] = 0.99
    q = {"grouping": [], "id": qid, "interval_ms": interval_s * 1000, "lookback_ms": lb, "promql": promql,
         "metrics": metrics, "rqes": [rqe], "sql": None, "support": "common-promql"}
    if promql != promql_original:
        q["promql_original"] = promql_original
    if rng:
        q["range"] = rng
    # L1 (stddev_over_time: no plan without raw data) and L5 (MetricsQL-only implicit subquery
    # rate(label_replace(...)[1m])) run on VictoriaMetrics only; ASAP plans the other 19
    if qid in VM_ONLY:
        q["engines"] = ["victoriametrics", "vmcluster"]
    qs.append(q)
w["queries"] = qs
w["notes"] = [
    "http_requests_total and capacity_rps have one series per job (200): A and B join them on(job) as the "
    "group_left right side, which needs one series per job.",
    "Histograms (http_request_duration_seconds_bucket, delivery_latency_bucket, calc_duration_bucket, "
    "data_latency_bucket): per sample a Zipf(1.1) number of observations in 1..20, log-normal (median 0.05, "
    "sigma 1); buckets cumulative over le.",
    "cache_hits_total <= cache_lookups_total (per-series ratio 0.5-0.99); http_errors_total = a 0-2% share of its "
    "job's http_requests_total increments.",
    "Other counters: Zipf(1.1, N=1e6) integer increments, 5% of series reset every 30-120 min; gauges: Zipf "
    "integers x an integer per-series multiplier.",
    "L5 uses MetricsQL's implicit subquery rate(label_replace(...)[1m]).",
    "L6's offset is on the selector (the same meaning as an offset after the aggregation, which is not valid PromQL). "
    "L1 and L5 run on VictoriaMetrics only (engines field); ASAP plans and answers the other 19 queries.",
    "The run is 3 h from zero; windows longer than 3h (A, B, E, L1, L2, L3: 6h) are capped "
    "at 3h; the original query text is in promql_original.",
]
json.dump(w, open(os.environ["ASAPQ_BENCH"] + "/workloads/demo48.json", "w"), indent=2, sort_keys=True)
tot = 0
for name, spec in EXTRA.items():
    n = 1
    for l in spec["labels"]:
        n *= l["cardinality"]
    tot += n
    print(f"{name:40s} {n:8d} series")
print(f"extra {tot} + data/data_total 2 x 40000 = {tot + 80000} series, {10 * (tot + 80000) / 1e6:.2f}M samples/s")
for q in qs:
    print(q["id"], q["lookback_ms"] // 1000, q["promql"][:90])
