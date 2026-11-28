#!/usr/bin/env python3
"""The ASAP data plane vs VictoriaMetrics demo table, from report.py's
report.json: absolute numbers per arm and per component (CPU core-seconds
in the ingest and query phases, peak and steady RSS), per query latency
(mean, max) and accuracy, and ASAPPlanner's planning as a separate one-time
line (offline: never in an arm's CPU or RSS).

Accuracy: the ASAP arm against the run's reference (report.py --reference;
by default VictoriaMetrics' answers, and for rate/increase queries its
untimed rate_prometheus/increase_prometheus answers), 5% target, failure
probability 0.3, over the verified evaluations (an evaluation whose
reference timed out or failed is unverified and counted apart).

With --textfile-dir it also writes the same numbers as Prometheus metrics
(`asap_bench_report_*`) for node-exporter's textfile collector, which the
Grafana dashboard reads.
"""
import argparse
import json
import re
from pathlib import Path

DEMO_ARMS = ("asap", "asapquery", "victoriametrics", "vmcluster", "prometheus")
ARM_TITLES = {
    "asap": "ASAP data plane (ASAPPlanner plan, --no-forward)",
    "asapquery": "ASAPQuery query_engine_rust (its own planner's plan, forwards to its own VictoriaMetrics)",
    "victoriametrics": "VictoriaMetrics single-node (no stream aggregation)",
    "vmcluster": "VictoriaMetrics cluster (vmstorage, vmselect, vminsert; same total budget)",
    "prometheus": "Prometheus 3.5.0 (optional arm)",
}
MIB = 2**20
REF_TITLES = {"victoriametrics": "the post-run VictoriaMetrics reference", "prometheus": "Prometheus"}


def fmt(v, spec=".1f"):
    return "-" if v is None else format(v, spec)


def points_text(q):
    """Points (series, or series x step for a range query) within 5% of
    Prometheus, and those on one side only."""
    p = q.get("points")
    if not p:
        return "no points compared"
    text = f"points within 5% {p['within_target']}/{p['exact']}"
    if p["missing"] or p["extra"]:
        text += f" (missing {p['missing']}, extra {p['extra']})"
    return text


def disk(c, d):
    """Disk bytes written or read by a process over both phases (io counters)."""
    return sum(c.get(f"{phase}_phase_disk_{d}_bytes", 0) for phase in ("ingest", "query"))


def planning_line(prep):
    path = prep / "backend" / "plan_joint_summary.json"
    if not path.exists():
        return None, "planning: no plan_joint_summary.json"
    j = json.loads(path.read_text())
    if j.get("error"):
        return j, f"planning (ASAPPlanner, offline, one-time): FAILED: {j['error']}"
    parts = [f"{j['planning_seconds']:.2f} s wall"]
    if j.get("planning_cpu_seconds") is not None:
        parts.append(f"{j['planning_cpu_seconds']:.2f} core-s CPU")
    if j.get("planning_peak_rss_bytes"):
        parts.append(f"peak RSS {j['planning_peak_rss_bytes'] / MIB:.0f} MiB")
    if j.get("nodes"):
        parts.append(f"{j['nodes']} plan nodes")
    return j, "planning (ASAPPlanner, offline, one-time, not in any arm): " + ", ".join(parts)


# The data plane's query-time result and backlog counters (names assumed
# `asap_*`, matched by pattern) and VictoriaMetrics' main counters.
ENGINE_COUNTER = re.compile(r"^(asap_\S*(hit|miss|compute|backlog|queue|pending|bytes)\S*"
                            r"|vm_rows_inserted_total\{type=\"promremotewrite\"\}|vm_pending_rows\S*"
                            r"|vm_data_size_bytes\S*|vm_cache_size_bytes\{type=\"promql/rollupResult\"\})\s")


def engine_counters(path):
    """(series, value) of the counters above from an end-of-arm /metrics snapshot."""
    if not path.exists():
        return []
    out = []
    for text in path.read_text(errors="replace").splitlines():
        if ENGINE_COUNTER.match(text + " "):
            name, _, value = text.rpartition(" ")
            try:
                out.append((name, float(value)))
            except ValueError:
                pass
    return out


def prom_lines(run, report, planning):
    out = []

    def m(name, labels, value):
        if value is None:
            return
        lbl = ",".join(f'{k}="{v}"' for k, v in {"run": run, **labels}.items())
        out.append(f"{name}{{{lbl}}} {value}")

    m("asap_bench_report_info", {"series": report["series"], "mode": report.get("mode", "instant")}, 1)
    for arm, r in report["arms"].items():
        for role, c in r["components"].items():
            bar = f"{arm}/{role}"
            for phase in ("ingest", "query"):
                m("asap_bench_report_cpu_seconds", {"arm": arm, "component": role, "bar": bar, "phase": phase},
                  c[f"{phase}_phase_cpu_s"])
            m("asap_bench_report_rss_bytes", {"arm": arm, "component": role, "bar": bar, "kind": "peak"},
              c["peak_rss_bytes"])
            m("asap_bench_report_rss_bytes", {"arm": arm, "component": role, "bar": bar, "kind": "steady"},
              c["steady_rss_bytes"])
            for phase in ("ingest", "query"):
                m("asap_bench_report_disk_write_bytes", {"arm": arm, "component": role, "bar": bar, "phase": phase},
                  c.get(f"{phase}_phase_disk_write_bytes"))
            for thread, t in c["threads"].items():
                for phase in ("ingest", "query"):
                    m("asap_bench_report_thread_cpu_seconds",
                      {"arm": arm, "component": role, "thread": thread, "phase": phase}, t[f"{phase}_phase_cpu_s"])
        fl = r.get("feeder_lag") or {}
        m("asap_bench_report_feeder_max_lag_seconds", {"arm": arm}, fl.get("query_phase_max_start_lag_s"))
        m("asap_bench_report_feeder_overrun_ticks", {"arm": arm}, fl.get("query_phase_overrun_ticks"))
        if fl.get("ingest_falling_behind") is not None:
            m("asap_bench_report_ingest_falling_behind", {"arm": arm}, int(fl["ingest_falling_behind"]))
        for qid, q in r["queries"].items():
            labels = {"arm": arm, "query": qid}
            m("asap_bench_report_query_latency_mean_seconds", labels, None if q["mean_ms"] is None else q["mean_ms"] / 1e3)
            m("asap_bench_report_query_latency_max_seconds", labels, None if q["max_ms"] is None else q["max_ms"] / 1e3)
            m("asap_bench_report_query_evaluations", labels, q["evaluations"])
            m("asap_bench_report_query_failed", labels, q["refused_or_failed"])
            if q.get("share_points_within_target") is not None:
                m("asap_bench_report_query_points_within_target_ratio", labels, q["share_points_within_target"])
            diff_key = next((k for k in q if k.startswith("max_rel_diff_vs_")), None)
            if q.get("is_reference"):
                pass
            elif diff_key:
                # Kept as asap_bench_report_query_max_rel_diff_vs_prometheus for the dashboards.
                m("asap_bench_report_query_max_rel_diff_vs_prometheus", labels, q.get(diff_key))
            else:
                m("asap_bench_report_query_max_rel_error", labels, q.get("max_rel_error"))
                verified = q.get("verified_evaluations", q.get("evaluations"))
                if q.get("passing_evaluations") is not None and verified:
                    m("asap_bench_report_query_pass_ratio", labels, q["passing_evaluations"] / verified)
                if q.get("meets_target") is not None:
                    m("asap_bench_report_query_meets_target", labels, int(bool(q["meets_target"])))
                m("asap_bench_report_query_unverified_evaluations", labels, q.get("unverified_evaluations"))
    if planning and not planning.get("error"):
        m("asap_bench_report_planning_seconds", {}, planning["planning_seconds"])
        m("asap_bench_report_planning_cpu_seconds", {}, planning.get("planning_cpu_seconds"))
        m("asap_bench_report_planning_peak_rss_bytes", {}, planning.get("planning_peak_rss_bytes"))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prep", required=True, type=Path)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--textfile-dir", type=Path,
                        help="write asap_bench_report_<run>.prom here (node-exporter textfile collector)")
    args = parser.parse_args()
    report = json.loads((args.run / "report.json").read_text())
    if any(q.get("range") for q in json.loads((args.prep / "queries.json").read_text())["queries"]):
        report["mode"] = "range"
    planning, line = planning_line(args.prep)
    arms = [a for a in DEMO_ARMS if a in report["arms"]]

    queries_json = json.loads((args.prep / "queries.json").read_text())
    rng = next((q["range"] for q in queries_json["queries"] if q.get("range")), None)
    mode = (f"RANGE queries (/api/v1/query_range, step {rng['step_ms'] / 1e3:g} s, span {rng['span_ms'] / 1e3:g} s)"
            if rng else "instant queries (/api/v1/query)")
    delays = sorted({r.get("query_delay_s") for r in report["arms"].values() if r.get("query_delay_s") is not None})
    lines = [f"run {args.run.name}: {report['series']} series, {mode}"
             + (f", queries sent {', '.join(f'{d:g}' for d in delays)} s after their evaluation time" if delays else ""),
             ""]
    lines.append("CPU (core-seconds) and memory per component")
    lines.append(f"  {'arm':16} {'component':17} {'cpu ingest':>11} {'cpu query':>10} {'cpu total':>10} "
                 f"{'peak RSS':>9} {'steady RSS':>11} {'disk write':>11} {'disk read':>10}")
    for arm in arms:
        r = report["arms"][arm]
        tot = [0.0, 0.0, 0, 0]
        for role, c in sorted(r["components"].items()):
            lines.append(
                f"  {arm:16} {role:17} {c['ingest_phase_cpu_s']:11.1f} {c['query_phase_cpu_s']:10.1f} "
                f"{c['ingest_phase_cpu_s'] + c['query_phase_cpu_s']:10.1f} "
                f"{c['peak_rss_bytes'] / MIB:6.0f} MiB {(c['steady_rss_bytes'] or 0) / MIB:7.0f} MiB "
                f"{disk(c, 'write') / MIB:7.1f} MiB {disk(c, 'read') / MIB:6.1f} MiB")
            if not role.startswith("feeder") and role != "harness":
                tot[0] += c["ingest_phase_cpu_s"]
                tot[1] += c["query_phase_cpu_s"]
                tot[2] += c["peak_rss_bytes"]
                tot[3] += c["steady_rss_bytes"] or 0
        lines.append(f"  {arm:16} {'= engine total':17} {tot[0]:11.1f} {tot[1]:10.1f} {tot[0] + tot[1]:10.1f} "
                     f"{tot[2] / MIB:6.0f} MiB {tot[3] / MIB:7.0f} MiB   ({ARM_TITLES[arm]})")
    lines.append("  (engine total excludes the feeders and the harness, which every arm has alike)")
    lines.append("")
    lines.append("Ingest during the query phase (the engine's feeder)")
    for arm in arms:
        fl = report["arms"][arm].get("feeder_lag") or {}
        lag = fl.get("query_phase_max_start_lag_s")
        flag = ""
        if fl.get("ingest_falling_behind"):
            flag = f"  INGEST FALLING BEHIND (max lag over the {fl['query_delay_s']:g} s query delay)"
        lines.append(f"  {arm:16} max feeder start lag {fmt(lag, '.2f')} s, "
                     f"{fl.get('query_phase_overrun_ticks', '-')} overrun ticks{flag}")
    lines.append("")
    if any(report["arms"][a].get("memory") for a in arms):
        lines.append("Memory per engine (cgroup v2; steady = query-phase median). Headline: working set "
                     "(memory.current - inactive_file) and anon; the cgroup total includes reclaimable page cache.")
        lines.append(f"  {'arm':16} {'working set':>11} {'anon':>9} {'RSS':>9} {'cgroup cur':>10} {'peak':>9} "
                     f"{'act file':>9} {'inact file':>10} {'dirty+wb':>9} {'kernel':>8} {'PSI some/full avg10':>20} "
                     f"{'data dir cached':>15}")
        mib = lambda b: "-" if b is None else f"{b / 2**20:.0f} MiB"  # noqa: E731
        for arm in arms:
            m = report["arms"][arm].get("memory") or {}
            engine_rss = sum((c or {}).get("steady_rss_bytes", 0) for r, c in
                             (report["arms"][arm].get("components") or {}).items()
                             if not r.startswith("feeder") and r != "harness") or None
            pc = m.get("data_dir_page_cache_bytes") or {}
            pcs = sum(v for v in pc.values() if isinstance(v, int)) if pc else None
            dwb = None
            if m.get("file_dirty_steady_bytes") is not None:
                dwb = m["file_dirty_steady_bytes"] + (m.get("file_writeback_steady_bytes") or 0)
            peak = m.get("memory_peak_bytes", m.get("current_peak_bytes"))
            psi = f"{m.get('psi_some_avg10', '-')}/{m.get('psi_full_avg10', '-')}"
            lines.append(f"  {arm:16} {mib(m.get('working_set_steady_bytes')):>11} {mib(m.get('anon_steady_bytes')):>9} "
                         f"{mib(engine_rss):>9} {mib(m.get('current_steady_bytes')):>10} {mib(peak):>9} "
                         f"{mib(m.get('active_file_steady_bytes')):>9} {mib(m.get('inactive_file_steady_bytes')):>10} "
                         f"{mib(dwb):>9} {mib(m.get('kernel_steady_bytes')):>8} {psi:>20} {mib(pcs):>15}")
        lim = next((report["arms"][a].get("limits") for a in arms if report["arms"][a].get("limits")), None)
        if lim:
            lines.append(f"  enforced on every engine (planner limits): cpu.max {lim.get('cpu_cores')} cores, "
                         f"memory.max {mib(lim.get('memory_bytes'))}, data filesystem {mib(lim.get('disk_bytes'))}"
                         + (f", io.max {lim['io_max']}" if lim.get("io_max") else ""))
        for arm in arms:
            r = report["arms"][arm]
            ev = (r.get("cgroup_end") or {}).get("memory.events", "")
            evd = dict(l.split() for l in ev.splitlines() if len(l.split()) == 2)
            du = sum((v or {}).get("used_bytes", 0) for v in (r.get("data_dir_usage") or {}).values()) or None
            dead = {k: v for k, v in (r.get("engine_exit_codes") or {}).items() if v is not None}
            if ev or du or dead or r.get("enospc_in_logs"):
                lines.append(f"  {arm:16} oom {evd.get('oom', '-')}, oom_kill {evd.get('oom_kill', '-')}, "
                             f"high {evd.get('high', '-')}, max {evd.get('max', '-')}; data fs used {mib(du)}; "
                             f"ENOSPC in logs {r.get('enospc_in_logs', 0)}"
                             + (f"; ENGINE EXITED {dead}" if dead else ""))
        lines.append("")
    if any(report["arms"][a].get("classes") for a in arms):
        lines.append("Offered load and outcome (every request classified; SLO = the query's interval; "
                     "goodput = answered_ok per second of the query phase)")
        lines.append(f"  {'arm':16} {'offered':>8} {'ok':>7} {'inacc:miss':>10} {'inacc:approx':>12} {'slow':>6} "
                     f"{'rejected':>8} {'timeout':>7} {'partial':>7} {'unverif':>7} {'goodput/s':>9} {'share':>6}")
        for arm in arms:
            r = report["arms"][arm]
            c = r.get("classes") or {}
            lines.append(f"  {arm:16} {r.get('offered_requests', 0):8} {c.get('answered_ok', 0):7} "
                         f"{c.get('inaccurate_missing_samples', 0):10} {c.get('inaccurate_approximation', 0):12} "
                         f"{c.get('slow', 0):6} {c.get('rejected', 0):8} {c.get('timeout', 0):7} {c.get('partial', 0):7} "
                         f"{c.get('unverified', 0):7} {fmt(r.get('goodput_per_s'), '9.2f')} "
                         f"{fmt(r.get('goodput_fraction'), '6.0%')}")
        for arm in arms:
            vis = [q.get("visible_fraction") for q in report["arms"][arm]["queries"].values() if q.get("visible_fraction")]
            if vis:
                lines.append(f"  {arm:16} visible fraction at T (per query: min / p10 / median): " + "; ".join(
                    f"{v['min']:.2f}/{v['p10']:.2f}/{v['median']:.2f}" for v in vis))
        for arm in arms:
            ec = (report["arms"][arm].get("engine_counters") or {}).get("query") or {}
            interesting = {k: v for k, v in ec.items() if v and any(t in k for t in (
                "limit_reached", "limit_timeout", "slow_queries", "request_errors", "select_current", "insert_current",
                "rows_ignored", "too_long", "dropped", "outcome", "refus", "backlog", "queued", "watermark",
                "rollupResult", "rollup_result"))}
            if interesting:
                lines.append(f"  {arm:16} self-protection / ingest counters over the query phase:")
                for k, v in sorted(interesting.items()):
                    lines.append(f"  {'':16}   {k} {v:g}")
        lines.append("")
    comp_path = args.run / "compression.json"
    if comp_path.exists():
        comp = json.loads(comp_path.read_text())
        g = comp.get("gorilla_reference") or {}
        lines.append("Compression (bits per sample; data: " +
                     str(json.loads((args.prep / "manifest.json").read_text())["data_facts"].get("feeder_values") or "original random doubles") + ")")
        lines.append(f"  VictoriaMetrics on disk: {fmt(comp.get('victoriametrics_disk_bits_per_sample'), '.2f')} bits/sample "
                     f"({fmt((comp.get('victoriametrics_storage_bytes') or 0) / 2**20, '.0f')} MiB for "
                     f"{comp.get('victoriametrics_stored_samples', 0):,.0f} samples)")
        for metric, v in g.items():
            lines.append(f"  Gorilla reference codec, {metric}: {v['gorilla_bits_per_sample']:.2f} bits/sample "
                         f"({v['series']} series, {v['samples']:,} samples)")
        if comp.get("asap_view_bits_per_ingested_sample") is not None:
            lines.append(f"  ASAP views (summaries, not samples): {comp['asap_view_bits_per_ingested_sample']:.2f} bits in memory, "
                         f"{comp.get('asap_persisted_view_bits_per_ingested_sample', 0):.2f} bits persisted per ingested sample")
        lines.append("")
    lines.append("Per query: latency (ms) and accuracy")
    users = max((q.get("users", 1) for a in arms for q in report["arms"][a]["queries"].values()), default=1)
    if users > 1:
        lines.append(f"  ({users} simulated users: n counts every user's request; failed includes timeouts)")
    lines.append(f"  {'query':20} {'arm':16} {'n':>4} {'mean ms':>9} {'max ms':>9} {'failed':>6} {'t/o':>4}  accuracy")
    queries = sorted({q for a in arms for q in report["arms"][a]["queries"]})
    for qid in queries:
        for arm in arms:
            q = report["arms"][arm]["queries"].get(qid)
            if q is None:
                continue
            diff_key = next((k for k in q if k.startswith("max_rel_diff_vs_")), None)
            if q.get("is_reference"):
                acc = "reference"
                if q.get("unverified_evaluations"):
                    acc += f"; {q['unverified_evaluations']} evaluations without a reference (timeout/failure)"
            elif q["refused_or_failed"] == q["evaluations"] and q["evaluations"]:
                acc = "no answer: " + "; ".join(q.get("refusal_reasons") or ["?"])[:100]
            elif diff_key:
                acc = (f"max diff vs {REF_TITLES.get(diff_key[16:], diff_key[16:])} {fmt(q[diff_key], '.2%')}, "
                       f"{points_text(q)} (a difference, not an error)")
            else:
                verified = q.get("verified_evaluations", q["evaluations"])
                acc = (f"max rel error {fmt(q.get('max_rel_error'), '.2%')}, {points_text(q)}, "
                       f"{q['passing_evaluations']}/{verified} verified evaluations pass, "
                       f"meets target: {({True: 'yes', False: 'NO'}).get(q['meets_target'], 'unverified')}")
                if q.get("unverified_evaluations"):
                    acc += f", {q['unverified_evaluations']} unverified"
            lines.append(f"  {qid:20} {arm:16} {q['evaluations']:4} {fmt(q['mean_ms'], '9.1f')} "
                         f"{fmt(q['max_ms'], '9.1f')} {q['refused_or_failed']:6} {q.get('timeouts', 0):4}  {acc}")
    lines.append("")
    lines.append("Engine counters at the end of each arm (its own /metrics)")
    for arm in arms:
        cpus = (report["arms"][arm].get("cpusets") or {})
        if cpus:
            lines.append(f"  {arm:16} cpusets: " + ", ".join(f"{k}={v}" for k, v in sorted(cpus.items())))
        for name, value in engine_counters(args.run / arm / "engine_metrics_end.txt"):
            lines.append(f"  {arm:16} {name} {value:g}")
    lines.append("")
    lines.append(line)
    text = "\n".join(lines) + "\n"
    (args.run / "demo_table.txt").write_text(text)
    print(text, end="")
    if args.textfile_dir:
        args.textfile_dir.mkdir(parents=True, exist_ok=True)
        # One file per run, so the dashboard's `run` variable lists every run.
        name = "".join(ch if ch.isalnum() else "_" for ch in args.run.name)
        tmp = args.textfile_dir / f".asap_bench_report_{name}.tmp"
        tmp.write_text("\n".join(prom_lines(args.run.name, report, planning)) + "\n")
        tmp.rename(args.textfile_dir / f"asap_bench_report_{name}.prom")


if __name__ == "__main__":
    main()
