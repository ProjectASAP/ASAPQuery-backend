#!/usr/bin/env python3
"""Write dashboards/asap-vs-vm.json, the Grafana dashboard of the ASAP data
plane vs VictoriaMetrics demo. Rerun after editing; Grafana reloads it.

Sources (all through the monitoring Prometheus, datasource uid `monprom`):
  asap_bench_*         harness run.py --metrics-port (live) and
  asap_bench_report_*  demo_table.py --textfile-dir (after a run; node-exporter)
  namedprocess_*       process-exporter
  asap_*               the data plane's GET /metrics (names assumed; regex matched)
  vm_*, go_*, process_* VictoriaMetrics' /metrics

No percentile panels and no CPU-over-time lines: CPU and memory are bars.
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "monprom"}
RUN = 'run="$run"'
panels = []
_id = [0]


def pid():
    _id[0] += 1
    return _id[0]


def target(expr, ref="A", legend=None, instant=False, fmt=None):
    t = {"datasource": DS, "expr": expr, "refId": ref, "range": not instant, "instant": instant}
    if legend:
        t["legendFormat"] = legend
    if fmt:
        t["format"] = fmt
    return t


def row(title, y):
    panels.append({"type": "row", "title": title, "id": pid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})


def panel(kind, title, grid, targets, description="", **extra):
    p = {"type": kind, "title": title, "id": pid(), "datasource": DS, "gridPos": grid,
         "targets": targets, "description": description}
    p.update(extra)
    panels.append(p)


def matrix_bars(title, grid, expr, column, unit, stacked, description):
    """Bars per arm/component (the `bar` label), one segment or bar per `column`."""
    panel("barchart", title, grid, [target(expr, instant=True, fmt="table")], description,
          transformations=[
              {"id": "groupingToMatrix", "options": {"columnField": column, "rowField": "bar",
                                                     "valueField": "Value", "emptyValue": "zero"}},
              {"id": "organize", "options": {"renameByName": {f"bar\\{column}": "arm / component"}}},
          ],
          fieldConfig={"defaults": {"unit": unit, "min": 0, "color": {"mode": "palette-classic"}},
                       "overrides": []},
          options={"orientation": "horizontal", "stacking": "normal" if stacked else "none",
                   "groupWidth": 0.7, "barWidth": 0.9, "showValue": "auto",
                   "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                   "xField": "arm / component", "tooltip": {"mode": "multi"}})


TEXT = """### ASAP data plane vs VictoriaMetrics

```
 paced feeder (seed 862, real time) ──Remote Write──▶  arm under test  ◀── harness run.py: /api/v1/query
   data (gauge), data_total (counter)                                     at each query's evaluation times
```

| arm | what runs | answers |
|---|---|---|
| **asap** | ASAPQuery-backend `data_plane --no-forward`, installing the ASAPPlanner plan (planned offline, once) | every answer from the plan's ingest-time views; nothing forwarded |
| **victoriametrics** | VictoriaMetrics single-node, **no stream aggregation**, default caches on | MetricsQL over raw samples; difference from Prometheus shown, not an error (rate/increase do not extrapolate) |
| **prometheus** | Prometheus 3.5.0 alone | the exact reference for accuracy |

Arms run one after another on the same feed (same seed and offsets from the feed start), each arm's engine on its own cpuset with the same core count; feeder, client and monitoring on the other cores.
Ingest phase = from the arm's start to its first query (fills the longest window); query phase = after.
Instant and range-query (`/api/v1/query_range`, Grafana-style step and span) runs are separate runs: pick one with `run`.
CPU = core-seconds per process (harness /proc sampling); memory = peak and steady (median over the query phase) RSS.
Planning time and memory are a one-time, offline cost and are never in an arm's CPU or RSS.
"""

y = 0
panel("text", "Arms and workload", {"h": 9, "w": 16, "x": 0, "y": y}, [],
      options={"mode": "markdown", "content": TEXT})
panel("stat", "ASAPPlanner planning (one-time, offline)", {"h": 9, "w": 8, "x": 16, "y": y}, [
    target(f'asap_bench_report_planning_seconds{{{RUN}}}', "A", "wall", instant=True),
    target(f'asap_bench_report_planning_cpu_seconds{{{RUN}}}', "B", "CPU core-s", instant=True),
    target(f'asap_bench_report_planning_peak_rss_bytes{{{RUN}}}', "C", "peak RSS", instant=True),
], "Planning the workload's joint plan with the backend's control plane, before the run.",
      fieldConfig={"defaults": {"unit": "s", "decimals": 2},
                   "overrides": [{"matcher": {"id": "byFrameRefID", "options": "C"},
                                  "properties": [{"id": "unit", "value": "bytes"}]}]},
      options={"reduceOptions": {"calcs": ["lastNotNull"]}, "textMode": "value_and_name",
               "colorMode": "none", "graphMode": "none", "orientation": "vertical"})
y += 9

row("CPU and memory per component (absolute)", y)
y += 1
matrix_bars(
    "CPU core-seconds per component: ingest vs query phase", {"h": 11, "w": 12, "x": 0, "y": y},
    f'max by (bar, phase) (asap_bench_report_cpu_seconds{{{RUN}}}) '
    f'or max by (bar, phase) (last_over_time(asap_bench_component_cpu_seconds{{{RUN}}}[$__range]))',
    "phase", "s", True,
    "Per process of each arm (feeders and the harness are alike on every arm). From the run report, "
    "or the harness' live values while the run is going.")
matrix_bars(
    "Memory per component: peak and steady RSS", {"h": 11, "w": 12, "x": 12, "y": y},
    f'max by (bar, kind) (asap_bench_report_rss_bytes{{{RUN}}}) '
    f'or label_replace(max by (bar) (last_over_time(asap_bench_component_peak_rss_bytes{{{RUN}}}[$__range])), '
    f'"kind", "peak", "", "")',
    "kind", "bytes", False,
    "Peak = highest sampled RSS; steady = median RSS over the query phase (report only).")
y += 11
panel("bargauge", "CPU core-seconds per process over the dashboard range (process-exporter)",
      {"h": 8, "w": 12, "x": 0, "y": y},
      [target('sum by (groupname) (increase(namedprocess_namegroup_cpu_seconds_total[$__range]))',
              legend="{{groupname}}", instant=True)],
      "Independent check of the harness' numbers, from process-exporter.",
      fieldConfig={"defaults": {"unit": "s", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
panel("bargauge", "Resident memory per process now (process-exporter)",
      {"h": 8, "w": 12, "x": 12, "y": y},
      [target('sum by (groupname) (namedprocess_namegroup_memory_bytes{memtype="resident"})',
              legend="{{groupname}}", instant=True)],
      fieldConfig={"defaults": {"unit": "bytes", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 8
matrix_bars(
    "Disk bytes written per component: ingest vs query phase", {"h": 9, "w": 12, "x": 0, "y": y},
    f'max by (bar, phase) (asap_bench_report_disk_write_bytes{{{RUN}}})',
    "phase", "bytes", True,
    "write_bytes from /proc/<pid>/io (bytes sent to the storage layer). The data plane persists its "
    "materialized views and VictoriaMetrics its data on the same disk.")
panel("bargauge", "Disk bytes written per process over the dashboard range (process-exporter)",
      {"h": 9, "w": 12, "x": 12, "y": y},
      [target('sum by (groupname) (increase(namedprocess_namegroup_write_bytes_total[$__range]))',
              legend="{{groupname}}", instant=True)],
      fieldConfig={"defaults": {"unit": "bytes", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 9

row("Query latency", y)
y += 1
panel("timeseries", "Latency of every evaluation, per query and arm (log scale)",
      {"h": 12, "w": 24, "x": 0, "y": y},
      [target(f'asap_bench_query_latency_seconds{{{RUN}}}', legend="{{arm}} {{query}}")],
      "The latest evaluation's latency, scraped every second from the harness.",
      fieldConfig={"defaults": {"unit": "s", "custom": {
          "scaleDistribution": {"type": "log", "log": 10}, "drawStyle": "line",
          "showPoints": "always", "pointSize": 4, "lineWidth": 1, "spanNulls": False}}, "overrides": []},
      options={"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max"]},
               "tooltip": {"mode": "multi"}})
y += 12

row("Input", y)
y += 1
panel("timeseries", "Input rate (samples/s)", {"h": 9, "w": 12, "x": 0, "y": y}, [
    target('sum by (arm) (rate(vm_rows_inserted_total{type="promremotewrite"}[30s]))', "A",
           "victoriametrics ingested"),
    target('sum by (arm) (rate({job="asap_data_plane", __name__=~"asap_.*(sample|row).*_total"}[30s]))', "B",
           "{{arm}} ingested"),
    target(f'max by (arm) (asap_bench_feed_target_rows_per_second{{{RUN}}})', "C", "{{arm}} feeder target"),
], "Samples per second received by each arm, and what the feeder sends.",
      fieldConfig={"defaults": {"unit": "short", "min": 0}, "overrides": []})
panel("timeseries", "ASAP data plane backlog over time", {"h": 9, "w": 6, "x": 12, "y": y}, [
    target('sum by (__name__) ({job="asap_data_plane", __name__=~"asap_.*(backlog|queue|queued|pending).*"})',
           "A", "{{__name__}}"),
    target('sum(vm_pending_rows{job="victoriametrics"})', "B", "victoriametrics pending_rows"),
], "Work the data plane has received but not yet applied (asap_* backlog/queue gauges, matched by "
   "pattern). Bounded backlog = the arm keeps up with the input; growth marks the ingest limit.",
      fieldConfig={"defaults": {"unit": "short", "min": 0}, "overrides": []})
panel("bargauge", "Stored bytes now", {"h": 9, "w": 6, "x": 18, "y": y}, [
    target('sum by (__name__) ({job="asap_data_plane", __name__=~"asap_.*bytes"})', "A", "{{__name__}}",
           instant=True),
    target('sum(vm_data_size_bytes{job="victoriametrics"})', "B", "victoriametrics data_size_bytes", instant=True),
], "The data plane's raw/view/query-time-result bytes and VictoriaMetrics' storage size.",
      fieldConfig={"defaults": {"unit": "bytes", "min": 0}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 9
panel("bargauge", "ASAP query-time results over the dashboard range: hits, misses, computes",
      {"h": 7, "w": 24, "x": 0, "y": y}, [
          target('sum by (metric) (increase(label_replace({job="asap_data_plane", '
                 '__name__=~"asap_.*(hit|miss|compute).*_total"}, "metric", "$1", "__name__", "asap_(.*)_total")'
                 '[$__range:5s]))', legend="{{metric}}", instant=True),
      ], "From data_plane GET /metrics: every asap_*hit*/miss*/compute*_total counter (query-time results "
         "shared across queries of one evaluation are computed once; hits are reuses).",
      fieldConfig={"defaults": {"unit": "short", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 9

row("CPU breakdown", y)
y += 1
panel("bargauge", "ASAP data plane: CPU seconds by phase (its own metrics)", {"h": 9, "w": 8, "x": 0, "y": y}, [
    target('sum by (metric, phase) (increase(label_replace({job="asap_data_plane", '
           '__name__=~"asap_.*cpu_seconds_total"}, "metric", "$1", "__name__", "asap_(.*)_cpu_seconds_total")'
           '[$__range:5s]))', legend="{{metric}} {{phase}}", instant=True),
], "From data_plane GET /metrics: every asap_*cpu_seconds_total counter, by its phase label if any.",
      fieldConfig={"defaults": {"unit": "s", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
matrix_bars(
    "ASAP data plane: CPU by thread, ingest vs query (harness /proc)", {"h": 9, "w": 8, "x": 8, "y": y},
    f'max by (bar, phase) (label_replace(asap_bench_report_thread_cpu_seconds{{{RUN}, arm="asap"}}, '
    f'"bar", "$1", "thread", "(.*)"))',
    "phase", "s", True, "Per thread name of the data_plane process (tokio workers, engine).")
panel("bargauge", "VictoriaMetrics: CPU and time by activity (its own metrics)",
      {"h": 9, "w": 8, "x": 16, "y": y}, [
          target('sum(increase(process_cpu_seconds_total{job="victoriametrics"}[$__range]))', "A",
                 "CPU total", instant=True),
          target('sum(increase(go_gc_cpu_seconds_total{job="victoriametrics"}[$__range]))', "B",
                 "CPU in Go GC", instant=True),
          target('sum(increase(vm_request_duration_seconds_sum{job="victoriametrics", '
                 'path="/api/v1/write"}[$__range]))', "C", "wall time in /api/v1/write", instant=True),
          target('sum(increase(vm_request_duration_seconds_sum{job="victoriametrics", '
                 'path="/api/v1/query"}[$__range]))', "D", "wall time in /api/v1/query", instant=True),
      ], "VictoriaMetrics exports total and GC CPU, and wall time per HTTP path (not CPU). "
         "eval/profile/profile_query_phase.sh gives the function-level CPU profile.",
      fieldConfig={"defaults": {"unit": "s", "min": 0, "color": {"mode": "palette-classic"}}},
      options={"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
               "reduceOptions": {"calcs": ["lastNotNull"]}})
y += 9

row("Answers and accuracy (run report)", y)
y += 1
answer_exprs = [
    ("A", "asap_bench_report_query_evaluations", "evaluations"),
    ("B", "asap_bench_report_query_failed", "failed"),
    ("C", "asap_bench_report_query_latency_mean_seconds", "mean latency"),
    ("D", "asap_bench_report_query_latency_max_seconds", "max latency"),
    ("E", "asap_bench_report_query_max_rel_error", "max rel error vs Prometheus"),
    ("F", "asap_bench_report_query_pass_ratio", "evaluations within 5%"),
    ("G", "asap_bench_report_query_meets_target", "meets target"),
    ("H", "asap_bench_report_query_max_rel_diff_vs_prometheus", "VM diff vs Prometheus (not an error)"),
]
panel("table", "Per query and arm", {"h": 12, "w": 24, "x": 0, "y": y},
      [target(f'max by (arm, query) ({m}{{{RUN}}})', ref, instant=True, fmt="table")
       for ref, m, _ in answer_exprs],
      "ASAP is checked against Prometheus' exact answers (5% target, failure probability 0.3); "
      "VictoriaMetrics' difference is shown only.",
      transformations=[
          {"id": "merge", "options": {}},
          {"id": "organize", "options": {
              "excludeByName": {"Time": True},
              "indexByName": {"query": 0, "arm": 1},
              "renameByName": {f"Value #{ref}": name for ref, _, name in answer_exprs}}},
          {"id": "sortBy", "options": {"sort": [{"field": "query"}]}},
      ],
      fieldConfig={"defaults": {}, "overrides": [
          {"matcher": {"id": "byRegexp", "options": ".*latency"}, "properties": [{"id": "unit", "value": "s"}]},
          {"matcher": {"id": "byRegexp", "options": ".*(error|within|diff).*"},
           "properties": [{"id": "unit", "value": "percentunit"}, {"id": "decimals", "value": 2}]},
          {"matcher": {"id": "byName", "options": "meets target"},
           "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
               "0": {"text": "NO", "color": "red"}, "1": {"text": "yes", "color": "green"}}}]},
                          {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
      ]})
y += 12

dashboard = {
    "uid": "asap-vs-vm",
    "title": "ASAP data plane vs VictoriaMetrics",
    "tags": ["asap", "benchmark"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "refresh": "5s",
    "time": {"from": "now-3h", "to": "now"},
    "templating": {"list": [{
        "name": "run", "label": "run", "type": "query", "datasource": DS,
        "query": {"query": 'label_values({__name__=~"asap_bench_.*"}, run)', "refId": "run"},
        "definition": 'label_values({__name__=~"asap_bench_.*"}, run)',
        "refresh": 2, "sort": 2, "includeAll": False, "multi": False,
    }]},
    "panels": panels,
}
out = Path(__file__).parent / "dashboards" / "asap-vs-vm.json"
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(dashboard, indent=1) + "\n")
print(out)
