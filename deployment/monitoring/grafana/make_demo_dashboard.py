#!/usr/bin/env python3
"""Write dashboards/asap-demo.json, the presentation dashboard: ASAP vs
VictoriaMetrics only (no Prometheus), resource usage only (no prices).

    make_demo_dashboard.py --prep eval/demo/live/prep-asap-demo-s10000

Layout (docs/design/demo-plan.md sections 8-9 of the backend): the time range starts
at the run's start (--run-start-ms), so every total counts from second 0.
  What                       the workload, scale, cores, versions, query delay, and the plan
  1 Cost from the start      CPU core-seconds (and cores now), memory (total, working set, anon),
                             disk written, requests by outcome, goodput
  2 Per query                one row per query in queries.json: latency p50/p99 (from the load's
                             histograms), and error vs the reference; a VictoriaMetrics-only query
                             (queries.json `engines`) shows VictoriaMetrics alone
  3 Ingestion keeps up       feeder lag, answer freshness
  4 Fairness                 request classes, VictoriaMetrics self-protection, OOM/ENOSPC
"""
import argparse
import datetime
import json
from collections import defaultdict
from pathlib import Path

ASAP = {"type": "prometheus", "uid": "live-asap"}
VM = {"type": "prometheus", "uid": "live-vm"}
MON = {"type": "prometheus", "uid": "monprom"}
EXP = "from 3-round experiment"
COLORS = {"ASAP": "green", "VictoriaMetrics": "purple"}


def plan_summary(prep, queries):
    """The plan in a few lines: each materialized view (sketch or exact,
    window, which queries read it), nodes shared by several queries, and
    the one-time planning cost."""
    plan = json.loads((prep / "backend" / "plan.json").read_text())
    nodes = {n["id"]: n for n in plan["dag"]["nodes"]}
    producers = defaultdict(list)
    for e in plan["dag"]["edges"]:
        producers[e["consumer"]].append(e["producer"])
    ids = {q["promql"]: q["id"].split("-", 1)[0] for q in queries}
    texts = plan.get("query_texts") or []
    reached = defaultdict(set)
    for qi, root in enumerate(plan["dag"]["roots"]):
        name = ids.get(texts[qi], f"q{qi + 1}") if qi < len(texts) else f"q{qi + 1}"
        stack = [root if isinstance(root, int) else root.get("node")]
        while stack:
            n = stack.pop()
            if name in reached[n]:
                continue
            reached[n].add(name)
            stack.extend(producers[n])
    lines = []
    for n in nodes.values():
        lifetime = n.get("lifetime")
        if not (isinstance(lifetime, dict) and "AcrossEvaluations" in lifetime):
            continue
        payload = next(iter(n["payload"].get("ASAP", {}).values()), {}) if "ASAP" in n["payload"] else {}
        st = (payload or {}).get("summary_type") or {}
        width = (payload or {}).get("tumbling_window_width_ms")
        if "Sketch" in st:
            params = next(iter((n.get("parameters") or {}).values()), {})
            kind = f"sketch: {st['Sketch']}" + "".join(f", {k} {v}" for k, v in params.items())
        elif "ExactAggregate" in st:
            agg = st["ExactAggregate"]
            kind = f"exact: {agg[0] if isinstance(agg, list) else agg}"
        else:
            kind = json.dumps(st) or "view"
        lines.append(f"- view {n['id']}: **{kind}**" + (f", {width // 1000} s windows" if width else "")
                     + f", read by {', '.join(sorted(reached[n['id']]))}")
    shared = [i for i, qs in reached.items() if len(qs) > 1 and i in nodes
              and "Scan" not in json.dumps(nodes[i]["payload"])[:30]]
    lines.insert(0, f"- **{len(lines)}** materialized views, built at ingestion (no raw data kept for queries); "
                    f"**{len(shared)}** node(s) shared by several queries")
    summary = prep / "backend" / "plan_joint_summary.json"
    if summary.exists():
        j = json.loads(summary.read_text())
        lines.append(f"- planning (offline, once): **{j['planning_seconds']:.2f} s**, "
                     f"{j.get('planning_cpu_seconds', 0):.2f} core-s, "
                     f"peak memory **{j.get('planning_peak_rss_bytes', 0) / 2**20:.0f} MiB**")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prep", required=True, type=Path)
    ap.add_argument("--shift-s", type=int, default=6, help="the query delay (panels end at now - this)")
    ap.add_argument("--cores", type=int, default=12, help="cores per engine (shown in the text)")
    ap.add_argument("--memory-bytes", type=int, help="the enforced memory limit per engine (page cache included)")
    ap.add_argument("--disk-bytes", type=int, help="the size of each engine's own data filesystem")
    ap.add_argument("--vm-version", default="v1.156.0")
    ap.add_argument("--run-start-ms", type=int, help="the dashboard's time range starts here (cumulative totals)")
    ap.add_argument("--planner-dir", type=Path, help="eval/demo/live/planner: report.json, cost_inputs.json, planning.json")
    ap.add_argument("--out", default=str(Path(__file__).parent / "dashboards" / "asap-demo.json"))
    args = ap.parse_args()
    queries = json.loads((args.prep / "queries.json").read_text())["queries"]
    manifest = json.loads((args.prep / "manifest.json").read_text())
    # The dashboard refreshes at the shortest query interval; a range query's span and step are shown
    # in the query table.
    step_s = min(q["interval_ms"] for q in queries) // 1000
    series = manifest["data_facts"]["series"]["leaf_count"]
    rate = manifest["samples_per_sec_per_series"]
    names = manifest.get("metric_names", {})
    base_names = list(names.values()) or ["data", "data_total"]
    extra = manifest.get("extra_metrics", {})

    def cardinality(metric):
        n = 1
        for label in metric.get("labels", []):
            n *= label["cardinality"]
        return n

    # Every metric's series and samples per second (the base metrics have `series` leaves each).
    per_metric = {m: (series, rate) for m in base_names}
    per_metric.update({m: (cardinality(v), v.get("samples_per_sec_per_series", rate)) for m, v in extra.items()})
    total_series = sum(n for n, _ in per_metric.values())
    total_rate = sum(n * r for n, r in per_metric.values())
    rates = sorted({r for _, r in per_metric.values()})
    delay = args.shift_s
    panels, y = [], 0
    ids = iter(range(1, 10_000))

    def add(p, h):
        p["id"] = next(ids)
        p["gridPos"]["y"] = y
        p["gridPos"]["h"] = h
        panels.append(p)

    def row(title):
        nonlocal y
        add({"type": "row", "title": title, "collapsed": False, "panels": [], "gridPos": {"w": 24, "x": 0}}, 1)
        y += 1

    def t(expr, legend="", ref="A", instant=True, fmt=None):
        d = {"datasource": MON, "refId": ref, "expr": expr, "legendFormat": legend,
             "instant": instant, "range": not instant}
        if fmt:
            d["format"] = fmt
        return d

    eng_colors = [{"matcher": {"id": "byRegexp", "options": f"^({n}|{n.lower()}).*"},
                   "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
                  for n, c in COLORS.items()]
    ref_line = {"matcher": {"id": "byName", "options": f"query delay ({delay} s)"},
                "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}},
                               {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                               {"id": "custom.lineWidth", "value": 2}]}

    def bars(title, x, w, targets, unit, desc="", overrides=None):
        return {"type": "bargauge", "title": title, "description": desc, "datasource": MON,
                "gridPos": {"x": x, "w": w}, "targets": targets,
                "fieldConfig": {"defaults": {"unit": unit, "min": 0, "color": {"mode": "palette-classic"}},
                                "overrides": overrides if overrides is not None else eng_colors},
                "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                            "reduceOptions": {"calcs": ["lastNotNull"]}}}

    def stat(title, x, w, targets, unit, desc="", decimals=2, thresholds=None, mappings=None, text="value_and_name"):
        defaults = {"unit": unit, "decimals": decimals}
        if thresholds:
            defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        if mappings:
            defaults["mappings"] = mappings
        return {"type": "stat", "title": title, "description": desc, "datasource": MON, "gridPos": {"x": x, "w": w},
                "targets": targets, "fieldConfig": {"defaults": defaults, "overrides": []},
                "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "textMode": text,
                            "colorMode": "value" if thresholds else "none", "graphMode": "none",
                            "orientation": "vertical"}}

    def timeseries(title, x, w, targets, unit, desc="", log=False, minimum=None, maximum=None, overrides=None,
                   points=False):
        custom = {"lineWidth": 2}
        if log:
            custom["scaleDistribution"] = {"type": "log", "log": 10}
        if points:
            custom.update(showPoints="always", pointSize=4)
        defaults = {"unit": unit, "custom": custom}
        if minimum is not None:
            defaults["min"] = minimum
        if maximum is not None:
            defaults["max"] = maximum
        return {"type": "timeseries", "title": title, "description": desc, "datasource": MON,
                "gridPos": {"x": x, "w": w}, "targets": targets,
                "fieldConfig": {"defaults": defaults, "overrides": overrides if overrides is not None else eng_colors},
                "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                            "tooltip": {"mode": "multi"}}}

    # ---- What
    def short(q):
        return q["id"].split("-", 1)[0]

    def dur(ms):
        return f"{ms // 3600000} h" if ms % 3600000 == 0 else f"{ms // 60000} min" if ms % 60000 == 0 else f"{ms // 1000} s"

    def how(q):
        if q.get("range"):
            return f"range: last {dur(q['range']['span_ms'])} at {dur(q['range']['step_ms'])} steps, every {dur(q['interval_ms'])}"
        return f"instant, every {dur(q['interval_ms'])}"

    def runs_on(q):
        return "VictoriaMetrics only" if "asap" not in q.get("engines", ("asap",)) else "both"

    def cell(promql, limit=110):  # long matchers (e.g. host lists) cut, so the table stays one line per query
        text = promql if len(promql) <= limit else promql[:limit - 1] + "…"
        return text.replace("|", chr(92) + "|")

    qlines = "\n".join(f"| {short(q)} | {dur(q['window_ms'])} | {how(q)} | {runs_on(q)} | `{cell(q['promql'])}` |"
                       for q in queries)
    metrics = [f"`{m}`" for m in per_metric]
    if args.memory_bytes:
        limits = (f"**Same enforced limits for each engine:** **{args.cores} cores** (cgroup cpu.max, on its own "
                  f"{args.cores}-vCPU cpuset), **{args.memory_bytes / 2**30:.0f} GiB memory** (memory.max, page cache "
                  f"included)" + (f", a **{args.disk_bytes / 2**30:.0f} GiB** data filesystem" if args.disk_bytes else "")
                  + "; feeders, the reference VictoriaMetrics and monitoring run on other CPUs.")
    else:
        limits = f"Each engine runs on its own **{args.cores}-core** cpuset."
    text = f"""**Same input, same queries, same cores.** {total_series:,} series in {len(per_metric)} metrics ({', '.join(metrics)}), {'/'.join(str(r) for r in rates)} sample/s per series ({total_rate:,} samples/s per engine), sent in real time by the same paced feeders per engine with identical samples. {limits} **ASAP**: `data_plane` executing ASAPPlanner's plan (nothing forwarded). **VictoriaMetrics** {args.vm_version} single-node, default settings, no stream aggregation. Every query is sent at its own interval by the simulated users, **{delay} s** after its evaluation time; before a window fills, both engines answer from the samples there are (as PromQL does).

| | window | asked | engines | query |
|---|---|---|---|---|
{qlines}

**Plan:** {plan_summary(args.prep, queries).replace(chr(10) + '- ', ' · ').lstrip('- ')}"""
    what_h = 9 + len(queries)   # text, the query table (a grid unit per row) and the plan line
    add({"type": "text", "title": "What", "gridPos": {"x": 0, "w": 24},
         "options": {"mode": "markdown", "content": text}}, what_h)
    y += what_h

    # ---- 1 Cost from the start of the run
    row("1 · Cost from the start of the run (totals since second 0, side by side)")
    add({"type": "text", "title": "", "gridPos": {"x": 0, "w": 24}, "options": {"mode": "markdown", "content":
        "Both engines start from empty and ingest in real time while every query runs from the start; each "
        "engine's total cost is counted from second 0 (ASAP: ingest, precompute, persistence and queries; "
        "VictoriaMetrics: ingest, background merges and queries). Counters are the engines' cgroups and "
        "processes, so a restart (annotated) resets the process ones."}}, 2)
    y += 2
    cg = {"asap": "asap", "victoriametrics": "victoriametrics"}
    add(timeseries("CPU core-seconds since the start", 0, 12, [
        t('max by (engine) (live_cgroup_cpu_seconds_total)', "{{engine}}", "A", False)], "s",
        "cgroup cpu.stat usage, cumulative.", minimum=0), 8)
    add(stat("CPU cores now (1 min average)", 12, 6, [
        t('max by (engine) (rate(live_cgroup_cpu_seconds_total[1m]))', "{{engine}}")], "short", "", 2), 4)
    add(stat("CPU core-seconds total", 18, 6, [
        t('max by (engine) (live_cgroup_cpu_seconds_total)', "{{engine}}")], "s", "", 0), 4)
    panels[-2]["gridPos"].update(h=4); panels[-1]["gridPos"].update(h=4)
    add(stat("CPU core-seconds per accurate on-time answer", 12, 12, [
        t('max by (engine) (live_cgroup_cpu_seconds_total) / on(engine) '
          'sum by (engine) (live_load_requests_total{outcome="answered_ok"})', "{{engine}}")], "s",
        "Total CPU since the start divided by the goodput count so far.", 2), 0)
    panels[-1]["gridPos"].update(y=y + 4, h=4)
    y += 8
    add(timeseries("Memory: total, working set and anon", 0, 12, [
        t('live_cgroup_memory_bytes{kind="current"}', "{{engine}} total", "C", False),
        t('live_cgroup_memory_bytes{kind="working_set"}', "{{engine}} working set", "A", False),
        t('live_cgroup_memory_bytes{kind="anon"}', "{{engine}} anon", "B", False)], "bytes",
        "total = the cgroup's memory.current (anon, page cache and kernel; what memory.max limits); "
        "working set = memory.current - inactive_file; anon = memory.stat anon (the harness definitions).",
        minimum=0, overrides=eng_colors + [
            {"matcher": {"id": "byRegexp", "options": ".* total$"},
             "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 6]}}]},
            {"matcher": {"id": "byRegexp", "options": ".* anon$"},
             "properties": [{"id": "custom.lineStyle", "value": {"fill": "dot"}}]}]), 8)
    add(timeseries("Disk bytes written since the start", 12, 12, [
        t('max by (engine) (live_data_fs_written_bytes_total)', "{{engine}}", "A", False)], "bytes",
        "Bytes the engine's cgroup wrote to its own data filesystem (cgroup io.stat wbytes of that device). "
        "The cgroups are made anew at each run's start, so this counts from second 0 and survives process "
        "restarts.",
        minimum=0), 8)
    y += 8
    add(timeseries("Requests since the start, by outcome", 0, 12, [
        t('sum by (engine, outcome) (live_load_requests_total)', "{{engine}} {{outcome}}", "A", False)], "short",
        "Cumulative request classes: answered_ok (on time and within 5%), inaccurate (missing samples / "
        "approximation), slow, rejected, timeout, partial.", minimum=0,
        overrides=[{"matcher": {"id": "byRegexp", "options": ".*(timeout|rejected|partial|inaccurate.*|slow)$"},
                    "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 6]}}]}]), 8)
    add(timeseries("Goodput: accurate on-time answers per second", 12, 12, [
        t('sum by (engine) (rate(live_load_requests_total{outcome="answered_ok"}[1m]))', "{{engine}}", "A", False),
        t('sum by (engine) (rate(live_load_requests_total[1m]))', "{{engine}} offered", "B", False)], "reqps",
        minimum=0), 8)
    y += 8

    # ---- 2 Per query
    row("2 · Per query: latency (p50, p99) and error")
    for q in queries:
        sel = f'query="{q["id"]}"'   # eval/demo/live/query_load.py labels by query id
        hq = lambda phi, eng: (f'histogram_quantile({phi}, sum by (le) (rate(live_load_latency_seconds_bucket'
                               f'{{engine="{eng}", {sel}}}[5m])))')
        name = short(q)
        asap_runs = "asap" in q.get("engines", ("asap",))
        not_run = "" if asap_runs else " ASAP: not run (VictoriaMetrics-only query)."
        lat_targets = ([t(hq(0.5, "asap"), "ASAP p50", "A", False), t(hq(0.99, "asap"), "ASAP p99", "B", False)]
                       if asap_runs else []) + [
            t(hq(0.5, "victoriametrics"), "VictoriaMetrics p50", "C", False),
            t(hq(0.99, "victoriametrics"), "VictoriaMetrics p99", "D", False)]
        err_targets = ([t(f'asap_demo_accuracy_max_rel_error{{engine="asap", query="{q["id"]}"}}', "ASAP error", "A",
                          False)] if asap_runs else []) + [
            t(f'asap_demo_accuracy_max_rel_error{{engine="victoriametrics", query="{q["id"]}"}}',
              "VictoriaMetrics error", "B", False)]
        # One row per query: latency on the left, error on the right.
        add(timeseries(f"{name} · latency · {q['promql']}", 0, 12, lat_targets, "s",
            f"Latency p50/p99 over 5 min of answered requests from the simulated users (log scale); "
            f"{how(q)}.{not_run}", log=True,
            overrides=eng_colors + [
                {"matcher": {"id": "byRegexp", "options": ".*p99$"},
                 "properties": [{"id": "custom.lineWidth", "value": 1},
                                {"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}}]}]), 7)
        add(timeseries(f"{name} · error · {q['promql']}", 12, 12, err_targets, "percentunit",
            "Max relative error against the reference VictoriaMetrics at each check (a missing series counts as "
            f"100%), first checked once the reference holds the query's whole {dur(q['window_ms'])} window. "
            f"Axis: 0 to at least the 5% target, capped at 100%.{not_run}", minimum=0, maximum=1), 7)
        panels[-1]["fieldConfig"]["defaults"]["custom"]["axisSoftMax"] = 0.05
        # Checked only once the reference holds the query's whole window (accuracy.py).
        panels[-1]["fieldConfig"]["defaults"]["noValue"] = (
            "not checked yet: first check once the reference holds the query's whole window "
            f"({dur(q['window_ms'])})")
        y += 7

    # ---- 3 Ingestion keeps up
    row("3 · Does ingestion keep up?")
    add(timeseries("Ingest lag: how late each engine's feeder runs (log scale)", 0, 12,
                   [t('max by (engine) (live_feeder_start_lag_seconds{job="live_ingest"})', "{{engine}}", "A", False),
                    t(f"vector({delay})", f"query delay ({delay} s)", "B", False)], "s",
                   "How late the feeder started its latest tick because the engine had not taken the previous ones.",
                   log=True, minimum=0.001, overrides=eng_colors + [ref_line]), 8)
    add(timeseries("Answer freshness: age of the newest sample an answer can see (log scale)", 12, 12,
                   [t('max by (engine) (live_answer_freshness_seconds{job="live_ingest"})', "{{engine}}", "A", False),
                    t(f"vector({delay})", f"query delay ({delay} s)", "B", False)], "s",
                   "now - the newest visible sample: ASAP its watermark lag, VictoriaMetrics max(timestamp(gauge)) "
                   "asked now. Below the query delay, every answer sees all samples up to its evaluation time.",
                   log=True, minimum=0.001, overrides=eng_colors + [ref_line]), 8)
    y += 8

    # ---- 4 Fairness
    row("4 · Fairness: request classes, self-protection, limits")
    add(bars("Requests since the start, by class", 0, 12, [
        t('sum by (engine, outcome) (live_load_requests_total)', "{{engine}} {{outcome}}")], "short", "",
        eng_colors + [{"matcher": {"id": "byRegexp", "options": ".*(timeout|rejected|partial|inaccurate.*|slow)$"},
                       "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]}]), 8)
    add(bars("VictoriaMetrics self-protection since the start", 12, 12, [
        t('sum(vm_concurrent_select_limit_reached_total{job="vm_live"})', "select concurrency limit reached", "A"),
        t('sum(vm_concurrent_select_limit_timeout_total{job="vm_live"})', "select queued past maxQueueDuration", "B"),
        t('sum(vm_concurrent_insert_limit_reached_total{job="vm_live"})', "insert concurrency limit reached", "C"),
        t('sum(vm_rows_ignored_total{job="vm_live"})', "rows ignored", "D"),
        t('sum by (engine) (live_cgroup_memory_events_total{event="oom_kill"})', "{{engine}} OOM kills", "E"),
        t('sum by (engine) (live_enospc_total)', "{{engine}} ENOSPC", "F")], "short",
        "Counters since each process started.", []), 8)
    y += 8

    dashboard = {"uid": "asap-demo", "title": "ASAP vs VictoriaMetrics", "tags": ["asap", "demo"],
                 "timezone": "utc", "schemaVersion": 39, "version": 1, "editable": True,
                 "refresh": f"{step_s}s",
                 # An absolute start as ISO-8601 UTC: Grafana 12 does not read epoch milliseconds here.
                 "time": {"from": datetime.datetime.fromtimestamp(args.run_start_ms / 1000, tz=datetime.timezone.utc)
                          .strftime("%Y-%m-%dT%H:%M:%S.000Z") if args.run_start_ms else "now-6h", "to": "now"},
                 "timepicker": {"refresh_intervals": [f"{step_s}s", "30s", "1m", "5m"]},
                 "annotations": {"list": [{"name": "live run events", "enable": True, "iconColor": "orange",
                                           "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                                           "target": {"type": "tags", "tags": ["live"], "limit": 100,
                                                      "matchAny": True}}]},
                 "panels": panels}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(dashboard, indent=1) + "\n")
    print(args.out)


if __name__ == "__main__":
    main()
