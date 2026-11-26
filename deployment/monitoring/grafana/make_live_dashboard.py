#!/usr/bin/env python3
"""Write dashboards/asap-vs-vm-live.json, the live demo's dashboard: one row
per planned query with three time-series panels side by side (ASAP |
VictoriaMetrics | Prometheus exact), each issuing the same PromQL as a real
/api/v1/query_range; then request latency per data source over time, and
CPU/RSS of the engines as bars.

    make_live_dashboard.py --queries eval/demo/live/prep-.../queries.json

Every query panel requests the plan's grid: the time range is the plan's
span (relative time), the step is the plan's step (the query's min interval;
max data points high enough that Grafana never picks a coarser one), and the
range is shifted back (`--shift-s`) so its end is past the data plane's
allowed lateness: Grafana aligns start and end down to a multiple of the
step, so the newest step is at least shift - step + ... behind now and its
windows are closed.
"""
import argparse
import json
from pathlib import Path

DS = {
    "asap": {"type": "prometheus", "uid": "live-asap"},
    "vm": {"type": "prometheus", "uid": "live-vm"},
    "exact": {"type": "prometheus", "uid": "live-exact"},
    "mon": {"type": "prometheus", "uid": "monprom"},
}
TITLES = {"asap": "ASAP", "vm": "VictoriaMetrics", "exact": "Prometheus (exact)"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--queries", required=True, help="the live prep's queries.json")
    ap.add_argument("--shift-s", type=int, default=6, help="panels end at now - this (the query delay)")
    ap.add_argument("--no-exact", action="store_true", help="no Prometheus (exact) column")
    ap.add_argument("--exact-step-s", type=int, default=0,
                    help="step of the Prometheus (exact) panels if not the plan's (a multiple of it), "
                         "when exact evaluation at every plan step is too slow to refresh")
    ap.add_argument("--out", default=str(Path(__file__).parent / "dashboards" / "asap-vs-vm-live.json"))
    args = ap.parse_args()
    queries = json.load(open(args.queries))["queries"]
    # The range queries' grid (queries.json `range`; the first one's); a workload of instant queries
    # refreshes at its shortest interval over the last hour.
    rng = next((q["range"] for q in queries if q.get("range")), None) or \
        {"step_ms": min(q["interval_ms"] for q in queries), "span_ms": 3_600_000}
    step_s, span_s = rng["step_ms"] // 1000, rng["span_ms"] // 1000
    steps = span_s // step_s + 1
    panels, ids = [], iter(range(1, 1000))
    y = 0

    def add(p):
        p["id"] = next(ids)
        panels.append(p)

    def row(title):
        nonlocal y
        add({"type": "row", "title": title, "collapsed": False, "panels": [],
             "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})
        y += 1

    text = f"""### ASAP vs VictoriaMetrics, live

One paced feeder (real time) sends the same samples by Remote Write to three engines, each on its own cpuset:
**ASAP** (`data_plane --no-forward`, executing ASAPPlanner's plan, planned with `range_evaluation` step {step_s} s, span {span_s // 60} min),
**VictoriaMetrics** single-node (no stream aggregation) and **Prometheus 3.5** (exact reference).

Each row is one planned query; the three panels send the same PromQL as `query_range` over the last {span_s // 60} min
at a {step_s} s step ({steps} steps), shifted {args.shift_s} s back so every step's windows are closed, refreshed every {step_s} s.
ASAP answers each step from the plan's ingest-time views; it refuses a request it cannot answer from the plan
(shown as a panel error). VictoriaMetrics' rate/increase do not extrapolate, so its values may differ from Prometheus by design.
A query planned for instant evaluation (no `range` in queries.json) shows each engine's answers at every accuracy check
(on the query's evaluation grid, where ASAP answers it); a VictoriaMetrics-only query (queries.json `engines`) has no ASAP panel."""
    add({"type": "text", "title": "What runs", "gridPos": {"h": 7, "w": 24, "x": 0, "y": y},
         "options": {"mode": "markdown", "content": text}})
    y += 7

    for q in queries:
        row(f"{q['id']}: {q['promql']}")
        arms = ("asap", "vm") if args.no_exact else ("asap", "vm", "exact")
        if "asap" not in q.get("engines", ("asap",)):
            arms = tuple(a for a in arms if a != "asap")
        width = 24 // len(arms)
        if not q.get("range"):
            # ASAP answers an instant query only at a time on the query's evaluation grid, and Grafana's
            # instant time is not aligned: show the answers the accuracy check (eval/demo/live/accuracy.py) records
            # at each grid time instead (asap_demo_answer; the reference's largest series past 20 outputs).
            mon_ds = DS["mon"]
            for i, arm in enumerate(a for a in arms if a != "exact"):
                engine = {"asap": "asap", "vm": "victoriametrics"}[arm]
                add({"type": "timeseries",
                     "title": f"{TITLES[arm]} (instant, every {q['interval_ms'] // 1000} s; answers at each accuracy check)",
                     "datasource": mon_ds, "gridPos": {"h": 8, "w": 12, "x": 12 * i, "y": y},
                     "timeFrom": f"{span_s}s",
                     "targets": [{"datasource": mon_ds, "refId": "A", "range": True, "instant": False,
                                  "expr": f'asap_demo_answer{{engine="{engine}", query="{q["id"]}"}}',
                                  "legendFormat": "{{series}}", "editorMode": "code"}],
                     "fieldConfig": {"defaults": {"custom": {"drawStyle": "line", "lineWidth": 1,
                                                             "showPoints": "never"}}, "overrides": []},
                     "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
                                 "tooltip": {"mode": "multi", "sort": "desc"}}})
            y += 8
            continue
        q_step = q["range"]["step_ms"] // 1000
        q_span = q["range"]["span_ms"] // 1000
        for i, arm in enumerate(arms):
            arm_step = args.exact_step_s if arm == "exact" and args.exact_step_s else q_step
            add({
                "type": "timeseries",
                "title": TITLES[arm] + (f" (every {arm_step} s)" if arm_step != q_step else ""),
                "datasource": DS[arm],
                "gridPos": {"h": 8, "w": width, "x": width * i, "y": y},
                "timeFrom": f"{q_span}s", "timeShift": f"{args.shift_s}s",
                "interval": f"{arm_step}s", "maxDataPoints": 11000,
                "targets": [{"datasource": DS[arm], "refId": "A", "expr": q["promql"], "range": True,
                             "instant": False, "interval": f"{arm_step}s", "intervalFactor": 1,
                             "legendFormat": "__auto", "editorMode": "code"}],
                "fieldConfig": {"defaults": {"custom": {"drawStyle": "line", "lineWidth": 1, "showPoints": "never",
                                                        "spanNulls": False}}, "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
                            "tooltip": {"mode": "multi", "sort": "desc"}},
            })
        y += 8

    row("Request latency per data source (over time) and answers")
    mon = DS["mon"]
    lat = {"unit": "s", "custom": {"scaleDistribution": {"type": "log", "log": 10}, "showPoints": "auto"}}
    add({"type": "timeseries", "title": "query_range latency as the client sees it (mean per minute, via the proxies)",
         "datasource": mon, "gridPos": {"h": 9, "w": 12, "x": 0, "y": y},
         "targets": [{"datasource": mon, "refId": "A", "legendFormat": "{{datasource}}: {{query}}",
                      "expr": 'sum by (datasource, query) (rate(live_proxy_request_seconds_sum{path="/api/v1/query_range"}[1m]))'
                              ' / sum by (datasource, query) (rate(live_proxy_request_seconds_count{path="/api/v1/query_range"}[1m]))'},
                     {"datasource": mon, "refId": "B", "legendFormat": "Grafana -> {{datasource}}",
                      "expr": 'sum by (datasource) (rate(grafana_datasource_request_duration_seconds_sum{datasource=~"ASAP|VictoriaMetrics|Prometheus \\\\(exact\\\\)"}[1m]))'
                              ' / sum by (datasource) (rate(grafana_datasource_request_duration_seconds_count{datasource=~"ASAP|VictoriaMetrics|Prometheus \\\\(exact\\\\)"}[1m]))'}],
         "fieldConfig": {"defaults": lat, "overrides": []},
         "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max"]}}})
    add({"type": "timeseries", "title": "query_range latency inside each engine (mean per minute, own metrics)",
         "datasource": mon, "gridPos": {"h": 9, "w": 12, "x": 12, "y": y},
         "targets": [{"datasource": mon, "refId": "A", "legendFormat": "ASAP: {{query}}",
                      "expr": 'rate(asap_range_query_latency_seconds_sum{job="asap_live"}[1m])'
                              ' / rate(asap_range_query_latency_seconds_count{job="asap_live"}[1m])'},
                     {"datasource": mon, "refId": "B", "legendFormat": "VictoriaMetrics: all queries",
                      "expr": 'rate(vm_request_duration_seconds_sum{job="vm_live", path="/api/v1/query_range"}[1m])'
                              ' / rate(vm_request_duration_seconds_count{job="vm_live", path="/api/v1/query_range"}[1m])'}],
         "fieldConfig": {"defaults": lat, "overrides": []},
         "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max"]}}})
    y += 9
    add({"type": "bargauge", "title": "query_range requests over the time range, by data source and HTTP status",
         "datasource": mon, "gridPos": {"h": 7, "w": 12, "x": 0, "y": y},
         "targets": [{"datasource": mon, "refId": "A", "instant": True, "range": False,
                      "legendFormat": "{{datasource}} {{status}}",
                      "expr": 'sum by (datasource, status) (increase(live_proxy_requests_total{path="/api/v1/query_range"}[$__range]))'}],
         "fieldConfig": {"defaults": {"unit": "short", "min": 0, "decimals": 0, "color": {"mode": "palette-classic"}},
                         "overrides": [{"matcher": {"id": "byRegexp", "options": ".* (4|5)[0-9][0-9]$"},
                                        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]}]},
         "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                     "reduceOptions": {"calcs": ["lastNotNull"]}}})
    add({"type": "bargauge", "title": "ASAP ingestion state now",
         "datasource": mon, "gridPos": {"h": 7, "w": 12, "x": 12, "y": y},
         "targets": [{"datasource": mon, "refId": "A", "instant": True, "range": False, "legendFormat": "{{__name__}}",
                      "expr": '{job="asap_live", __name__=~"asap_(precompute_backlog_windows|watermark_lag_seconds|samples_queued|query_time_results)"}'},
                     {"datasource": mon, "refId": "B", "instant": True, "range": False,
                      "legendFormat": "query-time results reused (hits) over the range",
                      "expr": 'sum(increase(asap_query_time_result_hits_total{job="asap_live"}[$__range]))'},
                     {"datasource": mon, "refId": "C", "instant": True, "range": False,
                      "legendFormat": "query-time results computed over the range",
                      "expr": 'sum(increase(asap_query_time_result_computes_total{job="asap_live"}[$__range]))'}],
         "fieldConfig": {"defaults": {"unit": "short", "min": 0, "color": {"mode": "palette-classic"}}},
         "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                     "reduceOptions": {"calcs": ["lastNotNull"]}}})
    y += 7

    row("CPU and memory: data_plane vs victoria-metrics (bars)")
    bars = {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
            "reduceOptions": {"calcs": ["lastNotNull"]}}
    for i, (title, expr, unit) in enumerate([
        ("CPU cores used, average over the time range",
         'sum by (groupname) (rate(namedprocess_namegroup_cpu_seconds_total{groupname=~"live-.*"}[$__range]))', "short"),
        ("CPU core-seconds over the time range",
         'sum by (groupname) (increase(namedprocess_namegroup_cpu_seconds_total{groupname=~"live-.*"}[$__range]))', "s"),
        ("Resident memory now",
         'sum by (groupname) (namedprocess_namegroup_memory_bytes{memtype="resident", groupname=~"live-.*"})', "bytes"),
    ]):
        add({"type": "bargauge", "title": title, "datasource": mon, "gridPos": {"h": 8, "w": 8, "x": 8 * i, "y": y},
             "targets": [{"datasource": mon, "refId": "A", "instant": True, "range": False,
                          "legendFormat": "{{groupname}}", "expr": expr}],
             "fieldConfig": {"defaults": {"unit": unit, "min": 0, "color": {"mode": "palette-classic"}}},
             "options": bars})
    y += 8

    dashboard = {
        "uid": "asap-vs-vm-live", "title": "ASAP vs VictoriaMetrics (live)", "tags": ["asap", "live"],
        "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": True,
        "refresh": f"{step_s}s", "time": {"from": f"now-{span_s}s", "to": "now"},
        # Grafana refuses refresh intervals below its min_refresh_interval (5 s by default).
        "timepicker": {"refresh_intervals": [f"{step_s}s", "30s", "1m", "5m"]},
        "annotations": {"list": [{"name": "live run events", "enable": True, "iconColor": "orange",
                                 "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                 "target": {"type": "tags", "tags": ["live"], "limit": 100, "matchAny": True}}]},
        "panels": panels,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(dashboard, indent=1) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
