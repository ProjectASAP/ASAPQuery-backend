#!/usr/bin/env python3
"""Summarize a run: per arm, CPU and peak RSS of every process, forwards,
and per query p50/p99 latency and error against exact.

Exact is the Prometheus-alone arm's answer at the same offset from the
feed's start (every arm receives identical data relative to its start).
Target (accuracy_sla 0.95): max_error 0.05 with failure probability 0.3.
An evaluation passes when it returns the exact series and every value is
within 5% relative error; for a top-k query (topk, rate) when it returns
at least 95% of the exact top-k series and their values are within 5%.
A query meets its target when at least 70% of its evaluations pass.

VictoriaMetrics is reported for performance only: its answers' difference
from Prometheus is recorded (`max_rel_diff_vs_prometheus`) but not checked
against the target, since MetricsQL's rate/increase do not extrapolate to
the window's edges as PromQL's do (and its quantile_over_time interpolates
differently), so an exact VM answer differs from Prometheus' by design.
"""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path

MAX_ERROR = 0.05
FAILURE_PROBABILITY = 0.3
MIN_RECALL = 0.95


def series(result, origin_ms=0):
    """{key: value} of a successful answer. An instant vector is keyed by
    its labels; a range matrix by (labels, step time - origin_ms), so arms
    run one after another (each on its own feed start S) are compared at
    the same offset from S, step by step."""
    if not isinstance(result, dict) or result.get("status") != "success":
        return None
    if "data" not in result:  # result not kept (run.py ANSWER_RESULTS_EVERY): not checked
        return None
    out = {}
    for s in result["data"]["result"]:
        labels = tuple(sorted((k, v) for k, v in s["metric"].items() if k != "__name__"))
        if "values" in s:  # a range query's matrix: one value per (series, step)
            for ts, v in s["values"]:
                out[(labels, round(float(ts) * 1000) - origin_ms)] = float(v)
        else:
            out[labels] = float(s["value"][1])
    return out


def at_reference_steps(actual, exact):
    """A range answer restricted to the steps the reference has (the post-run
    VictoriaMetrics reference verifies sampled steps of a range request:
    first, middle, last); an instant answer unchanged."""
    if not actual or not exact:
        return actual
    if not all(isinstance(k, tuple) and len(k) == 2 and isinstance(k[1], int) for k in exact):
        return actual
    steps = {k[1] for k in exact}
    return {k: v for k, v in actual.items() if k[1] in steps}


def rel(a, e):
    if a == e:
        return 0.0
    return abs(a - e) / max(abs(e), 1e-12)


def evaluate(actual, exact, topk):
    """(passes, max relative error or None, recall, points) of one
    evaluation. Points are (series) for an instant query and (series, step)
    for a range query: compared (in both), missing (in exact only), extra
    (in the answer only), within the target, and the exact count."""
    if actual is None or exact is None:
        return False, None, None, None
    common = actual.keys() & exact.keys()
    errors = [rel(actual[k], exact[k]) for k in common]
    worst = max(errors) if errors else None
    recall = len(common) / len(exact) if exact else 1.0
    points = {"compared": len(common), "missing": len(exact.keys() - actual.keys()),
              "extra": len(actual.keys() - exact.keys()),
              "within_target": sum(1 for e in errors if e <= MAX_ERROR), "exact": len(exact)}
    if topk:
        return recall >= MIN_RECALL and (worst or 0.0) <= MAX_ERROR, worst, recall, points
    return actual.keys() == exact.keys() and (worst or 0.0) <= MAX_ERROR, worst, recall, points


def percentile(values, p):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return values[min(len(values) - 1, max(0, math.ceil(p / 100 * len(values)) - 1))]


# What each process of an arm is.
COMPONENT_NAMES = {
    "prometheus": "Prometheus",
    "asapquery": "ASAPQuery query_engine_rust (in-process precompute + query engine)",
    "data_plane": "backend data_plane (Remote Write decode, precompute, query engine)",
    "feeder0": "feeder 0 (to the arm's engine)",
    "feeder1": "feeder 1 (to the arm's Prometheus)",
    "harness": "harness (run.py: scheduler and query client)",
    "victoria-metrics": "VictoriaMetrics single-node (Remote Write ingest, storage, MetricsQL)",
}
# vmcluster (2026-11-22): VictoriaMetrics cluster, classified like the single-node arm.
ARMS = ("prometheus", "asapquery", "backend", "backend_alone", "asap", "victoriametrics", "vmcluster")
VM_ARMS = ("victoriametrics", "vmcluster")


def load_reference(reference, arm_dirs, arms, queries):
    """{(query, offset_ms): answer or None} and a note.

    victoriametrics: the post-run, untimed answers of a VictoriaMetrics
    started on the arm's kept data (accuracy_vs_vm.py ->
    victoriametrics/reference.jsonl): the query unchanged, rate/increase as
    rate_prometheus/increase_prometheus. VictoriaMetrics' in-run answers are
    never the truth (its ingestion can fall behind under query load). A
    missing or failed reference is None: the evaluation is unverified.
    (victoriametrics/reference_answers.jsonl from run.py
    VM_REFERENCE_QUERIES=1 is read when there is no reference.jsonl; it is
    asked in the run and covers rate/increase queries only.)
    prometheus: the Prometheus arm's answers."""
    exact = {}
    if reference not in arms:
        return exact, f"no {reference} arm: accuracy not checked"
    rows, resources = load(arm_dirs[reference])
    start = resources["start_ms"]
    if reference == "prometheus":
        for row in rows:
            exact[(row["query"], row["offset_ms"])] = series(row["result"], start) if row["status"] == 200 else None
        return exact, "Ground truth: the Prometheus 3.5.0 arm's answers."
    ref_path = next((arm_dirs[reference] / f for f in ("reference.jsonl", "reference_answers.jsonl")
                     if (arm_dirs[reference] / f).exists()), None)
    if ref_path:
        for line in open(ref_path):
            ref = json.loads(line)
            ok = ref.get("status") == 200 and (ref.get("result") or {}).get("status") == "success"
            exact[(ref["query"], ref["offset_ms"])] = series(ref["result"], start) if ok else None
    note = ("Ground truth: VictoriaMetrics asked after the run, untimed, on its stored data (accuracy_vs_vm.py): "
            "the query unchanged, rate/increase as rate_prometheus/increase_prometheus. An evaluation without "
            "a reference is unverified.")
    if not ref_path:
        note += " No reference file in this run: every evaluation is unverified (run accuracy_vs_vm.py)."
    return exact, note


def components(resources):
    """Per process: CPU by phase, peak RSS (VmHWM where the process was
    alive at the end), steady RSS, and CPU by thread name."""
    out = {}
    for role, c in (resources.get("components") or {}).items():
        peak = resources["usage_at_end"].get(role, {}).get("peak_rss_bytes") or c["peak_rss_bytes"]
        out[role] = {**c, "name": COMPONENT_NAMES.get(role, role), "peak_rss_bytes": peak}
    return out


def ingest(arm_dir, query_phase_first_tick=None):
    """Per feeder (feeder0 sends to the arm's engine, feeder1 to its
    Prometheus): Remote Write time per 1 s tick (p50/p99 ms), achieved rows/s
    and failed requests; and in the query phase (ticks from
    `query_phase_first_tick`): the largest start lag (how far behind its
    schedule a tick started: the engine not taking samples in time) and the
    ticks whose sending took over a second (overruns)."""
    out = {}
    for path in sorted(arm_dir.glob("feeder*-stats.json")):
        stats = json.loads(path.read_text())
        ticks = stats.get("ticks", [])
        elapsed = [t["elapsed_ms"] for t in ticks]
        in_query = [t for t in ticks if query_phase_first_tick is not None and t.get("tick", 0) >= query_phase_first_tick]
        out[path.name.split("-")[0]] = {
            "query_phase_max_start_lag_ms": max((t.get("start_lag_ms", 0) for t in in_query), default=None),
            "query_phase_overrun_ticks": sum(1 for t in in_query if t.get("elapsed_ms", 0) > 1000) if in_query else None,
            "tick_p50_ms": percentile(elapsed, 50),
            "tick_p99_ms": percentile(elapsed, 99),
            "achieved_rows_per_sec": stats.get("achieved_rows_per_sec"),
            "rows_sent": stats.get("rows_sent"),
            "failed_requests": stats.get("failed_requests"),
            "overrun_ticks": stats.get("overrun_ticks"),
        }
    return out


def load(arm_dir):
    rows = [json.loads(l) for l in open(arm_dir / "answers.jsonl")]
    return rows, json.loads((arm_dir / "resources.json").read_text())


CLASSES = ("answered_ok", "inaccurate_missing_samples", "inaccurate_approximation", "slow", "rejected",
           "timeout", "partial", "unverified")


def classify(arm, qrows, q, exact, extras, probes, start_ms, topk):
    """Fairness classification (2026-11-21) of every request of one query on one arm:
    answered_ok (200, latency <= the query's interval = the SLO, within the target
    against the reference), inaccurate (cause: missing samples when the engine's
    visible fraction at T was < 0.999, else approximation), slow, rejected (HTTP
    4xx/5xx), timeout, partial (isPartial), unverified (no reference).
    The victoriametrics arm is checked against VictoriaMetrics' own MetricsQL
    answer for rate/increase (`result_vm`), so its inaccuracy is missing samples
    or limits, not MetricsQL's rate definition."""
    out = {c: 0 for c in CLASSES}
    vis_all = []
    for r in qrows:
        res = r.get("result") or {}
        err = str(res.get("error", ""))
        if r["status"] is None and "timed out" in err:
            c = "timeout"
        elif r["status"] != 200 or res.get("status") != "success":
            c = "rejected"
        elif res.get("isPartial"):
            c = "partial"
        elif r["latency_ms"] is not None and r["latency_ms"] > q["interval_ms"]:
            c = "slow"
        elif res.get("omitted"):  # on time, result not kept (ANSWER_RESULTS_EVERY): not checked
            c = "unverified"
        else:
            key = (r["query"], r["offset_ms"])
            ref = exact.get(key)
            ex = extras.get(key, {})
            if arm in VM_ARMS and ex.get("result_vm") is not None:
                ref = series(ex["result_vm"], start_ms_vm(extras))
            if ref is None:
                c = "unverified"
            else:
                ok, _, _, _ = evaluate(at_reference_steps(series(res, start_ms), ref), ref, topk)
                if ok:
                    c = "answered_ok"
                else:
                    vis = visible_fraction(arm, probes.get(key), ex, q, r["offset_ms"])
                    c = "inaccurate_missing_samples" if (vis is not None and vis < 0.999) else "inaccurate_approximation"
        out[c] += 1
    for key, p in probes.items():
        if key[0] == q["id"]:
            v = visible_fraction(arm, p, extras.get(key, {}), q, key[1])
            if v is not None:
                vis_all.append(v)
    return out, vis_all


def start_ms_vm(extras):
    return extras.get("__v0__", 0)


def visible_fraction(arm, probe, extra, q, offset_ms):
    if not probe:
        return None
    if arm in VM_ARMS:
        rc = extra.get("ref_count")
        return (probe["count"] / rc) if (rc and probe.get("count") is not None) else None
    vu = probe.get("visible_until_offset_ms")
    if vu is None:
        return None
    w = q.get("window_ms", q.get("lookback_ms"))
    return max(0.0, min(1.0, (vu - (offset_ms - w)) / w))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prep", required=True, type=Path)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--reference", choices=("auto", "victoriametrics", "prometheus"), default="auto",
                        help="ground truth: VictoriaMetrics (rate/increase via rate_prometheus/"
                             "increase_prometheus, reference.jsonl) or the Prometheus arm; auto = "
                             "VictoriaMetrics when its arm ran, else Prometheus")
    parser.add_argument("--arm-dir", action="append", default=[], metavar="ARM=DIR",
                        help="take ARM from DIR instead of RUN/ARM, e.g. a later backend-only "
                             "rerun (run.py --arms backend) against this run's other arms")
    args = parser.parse_args()
    arm_dirs = {a: args.run / a for a in ARMS}
    for override in args.arm_dir:
        name, path = override.split("=", 1)
        arm_dirs[name] = Path(path)
    queries = {q["id"]: q for q in json.loads((args.prep / "queries.json").read_text())["queries"]}
    arms = [a for a, d in arm_dirs.items() if (d / "resources.json").exists()]
    reference = args.reference
    if reference == "auto":
        reference = "victoriametrics" if "victoriametrics" in arms else "prometheus"
    exact, reference_note = load_reference(reference, arm_dirs, arms, queries)
    # Fairness extras from the post-run reference: ref_count, result_vm (by (query, offset)).
    extras = {}
    if "victoriametrics" in arm_dirs and (arm_dirs["victoriametrics"] / "reference.jsonl").exists():
        extras["__v0__"] = json.loads((arm_dirs["victoriametrics"] / "resources.json").read_text())["start_ms"]
        for line in open(arm_dirs["victoriametrics"] / "reference.jsonl"):
            row = json.loads(line)
            extras[(row["query"], row["offset_ms"])] = {k: row[k] for k in ("ref_count", "result_vm") if k in row}
    diff_only = {a for a in ("victoriametrics", "vmcluster", "prometheus") if a in arms}
    report = {
        "series": json.loads((args.prep / "queries.json").read_text())["series"],
        "arm_dirs": {a: str(arm_dirs[a]) for a in arms},
        "reference": reference,
        "notes": [
            reference_note,
            "ASAPQuery runs its own archived pilot plan (sketch-bench's cheapest p95 plan); "
            "its sketch parameters (e.g. DDSketch alpha 0.1, CMS-heap) can miss the 5% target "
            "(up to about 9% in the smoke runs). That is kept as the comparison point.",
            "Every arm receives identical data relative to its own feed start; answers are compared "
            "at the same offset from it.",
        ],
        "arms": {},
    }
    for arm in arms:
        rows, resources = load(arm_dirs[arm])
        arm_probes = {}
        if (arm_dirs[arm] / "probes.jsonl").exists():
            for line in open(arm_dirs[arm] / "probes.jsonl"):
                pr = json.loads(line)
                arm_probes[(pr["query"], pr["offset_ms"])] = pr
        usage = resources["usage_at_end"]
        engine_roles = [r for r in usage if not r.startswith("feeder")]
        by_query = defaultdict(list)
        for row in rows:
            by_query[row["query"]].append(row)
        per_query = {}
        for qid, qrows in sorted(by_query.items()):
            topk = queries[qid]["promql"].startswith("topk")
            refs = [None if (r.get("result") or {}).get("omitted") else exact.get((qid, r["offset_ms"])) for r in qrows]
            verified = sum(1 for ref in refs if ref is not None)
            checks = [evaluate(at_reference_steps(series(r["result"], resources["start_ms"]), ref), ref, topk)
                      for r, ref in zip(qrows, refs)]
            points = {k: sum(c[3][k] for c in checks if c[3]) for k in
                      ("compared", "missing", "extra", "within_target", "exact")}
            passes = sum(c[0] for c in checks)
            errors = [c[1] for c in checks if c[1] is not None]
            latencies = [r["latency_ms"] for r in qrows if r["latency_ms"] is not None]
            per_query[qid] = {
                "evaluations": len(qrows),
                "mean_ms": sum(latencies) / len(latencies) if latencies else None,
                "max_ms": max(latencies, default=None),
                "min_ms": min(latencies, default=None),
                "p50_ms": percentile([r["latency_ms"] for r in qrows], 50),
                "p99_ms": percentile([r["latency_ms"] for r in qrows], 99),
                # Refused (no-forward) or failed requests: no answer to check.
                "refused_or_failed": sum(1 for r in qrows if r["status"] != 200),
                "refusal_reasons": sorted({str(r["result"].get("error"))[:200] for r in qrows
                                           if r["status"] != 200 or r["result"].get("status") != "success"}),
                "answered_by_plan": sum(1 for r in qrows if r["execution"] == "plan"),
                "forwarded": sum(1 for r in qrows if r["execution"] == "forwarded"),
                "forward_reasons": sorted({r["forward_reason"] for r in qrows if r["forward_reason"]}),
                "max_rel_error": max(errors, default=None),
                "median_rel_error": percentile(errors, 50),
                "p99_rel_error": percentile(errors, 99),
                "min_recall": min((c[2] for c in checks if c[2] is not None), default=None),
                "passing_evaluations": passes,
                # Over the verified evaluations (those with a reference answer).
                "meets_target": (passes >= (1 - FAILURE_PROBABILITY) * verified) if verified else None,
                "verified_evaluations": verified,
                # Point by point over every evaluation: (series) or (series, step).
                "points": points,
                "share_points_within_target": points["within_target"] / points["exact"] if points["exact"] else None,
            }
            # Evaluations with no reference answer (it timed out or failed).
            per_query[qid]["unverified_evaluations"] = sum(1 for ref in refs if ref is None)
            cls, vis = classify(arm, qrows, queries[qid], exact, extras, arm_probes, resources["start_ms"], topk)
            per_query[qid]["classes"] = cls
            per_query[qid]["visible_fraction"] = ({"n": len(vis), "min": min(vis), "p10": sorted(vis)[len(vis) // 10],
                                                   "median": sorted(vis)[len(vis) // 2]} if vis else None)
            # QUERY_USERS: every user's request is a row; aggregate above, and per user here.
            def timed_out(r):
                return r["status"] is None and "timed out" in str((r.get("result") or {}).get("error", ""))
            per_query[qid]["timeouts"] = sum(1 for r in qrows if timed_out(r))
            users = sorted({r.get("user", 0) for r in qrows})
            per_query[qid]["users"] = len(users)
            if len(users) > 1:
                per_user = {}
                for u in users:
                    ur = [r for r in qrows if r.get("user", 0) == u]
                    lat = [r["latency_ms"] for r in ur if r["latency_ms"] is not None]
                    per_user[str(u)] = {"requests": len(ur), "mean_ms": sum(lat) / len(lat) if lat else None,
                                        "p99_ms": percentile(lat, 99), "max_ms": max(lat, default=None),
                                        "refused_or_failed": sum(1 for r in ur if r["status"] != 200),
                                        "timeouts": sum(1 for r in ur if timed_out(r))}
                per_query[qid]["per_user"] = per_user
            if arm == reference and reference == "prometheus":
                q = per_query[qid]
                q["is_reference"] = True
                for k in ("max_rel_error", "median_rel_error", "p99_rel_error", "points",
                          "share_points_within_target", "min_recall"):
                    q.pop(k, None)
                q["passing_evaluations"] = None
                q["meets_target"] = None
            elif arm in diff_only:
                q = per_query[qid]
                # A difference from the reference, not an error (e.g. VictoriaMetrics vs
                # Prometheus: MetricsQL's rate/increase do not extrapolate).
                q[f"max_rel_diff_vs_{reference}"] = q.pop("max_rel_error")
                q[f"median_rel_diff_vs_{reference}"] = q.pop("median_rel_error")
                q.pop("p99_rel_error")
                q["series_match_evaluations"] = passes
                q["passing_evaluations"] = None
                q["meets_target"] = None
        tot = {c: sum(q.get("classes", {}).get(c, 0) for q in per_query.values()) for c in CLASSES}
        offered = sum(tot.values())
        qphase = resources.get("query_phase_s") or 0
        report["arms"][arm] = {
            # Fairness (2026-11-21): every offered request classified; goodput =
            # answered_ok per second of the query phase, and as a share of offered.
            "offered_requests": offered,
            "classes": tot,
            "goodput_per_s": (tot["answered_ok"] / qphase) if qphase else None,
            "goodput_fraction": (tot["answered_ok"] / offered) if offered else None,
            # cgroup v2 memory of the arm's engine (anon, file cache, shmem; peak and
            # query-phase median) and its data dir's page-cache residency (run.py).
            "memory": resources.get("memory"),
            "engine_counters": resources.get("engine_counters"),
            "limits": resources.get("limits"),
            "cgroup_end": resources.get("cgroup_end"),
            "disk_limit": resources.get("disk_limit"),
            "data_dir_usage": resources.get("data_dir_usage"),
            "enospc_in_logs": resources.get("enospc_in_logs"),
            "engine_exit_codes": resources.get("engine_exit_codes"),
            "cpu_core_seconds": sum(usage[r].get("cpu_core_seconds", 0) for r in engine_roles),
            "peak_rss_bytes_sum": sum(usage[r].get("peak_rss_bytes", 0) for r in engine_roles),
            "processes": usage,
            "feeder_cpu_core_seconds": sum(usage[r].get("cpu_core_seconds", 0)
                                           for r in usage if r.startswith("feeder")),
            "cpu_core_seconds_query_phase": sum(
                usage[r].get("cpu_core_seconds", 0)
                - resources["usage_at_query_phase_start"].get(r, {}).get("cpu_core_seconds", 0)
                for r in engine_roles),
            # Backend: answers its data plane marked forwarded. ASAPQuery: answers
            # its log names a destination other than asap (the readiness probe
            # is one). Also Prometheus' own query count during the queries.
            "forwarded": (sum(q["forwarded"] for q in per_query.values())
                          if arm.startswith("backend") or arm == "asap"
                          else sum(v for k, v in resources.get("asapquery_answer_destinations", {}).items()
                                   if k != "asap")),
            "asapquery_answer_destinations": resources.get("asapquery_answer_destinations"),
            "prometheus_query_requests_during_queries": resources["prometheus_query_requests_during_queries"],
            "wall_seconds": resources["wall_seconds"],
            "storage_bytes": resources.get("storage_bytes"),
            "query_p50_ms": percentile([r["latency_ms"] for r in rows], 50),
            "query_p99_ms": percentile([r["latency_ms"] for r in rows], 99),
            "data_plane_status": resources.get("data_plane_status"),
            # Queries from the start (run.py QUERIES_FROM_START): every tick is in the query phase.
            "ingest": ingest(arm_dirs[arm], 0 if resources.get("queries_from_start")
                             else max(q["lookback_ms"] for q in queries.values()) // 1000),
            "components": components(resources),
            "loadavg_at_end": resources["loadavg_at_end"],
            "cpusets": resources.get("cpusets"),
            "query_delay_s": resources.get("query_delay_s"),
            "queries": per_query,
        }
        # The engine's feeder (feeder0) in the query phase: an arm whose
        # ingestion falls behind by more than the query delay answers
        # without the newest samples.
        # Several feeders (sharded tables, 2026-11-22): the worst of them.
        feeders = [f for k, f in report["arms"][arm]["ingest"].items() if k.startswith("feeder")]
        lags = [f.get("query_phase_max_start_lag_ms") for f in feeders if f.get("query_phase_max_start_lag_ms") is not None]
        overruns = [f.get("query_phase_overrun_ticks") for f in feeders if f.get("query_phase_overrun_ticks") is not None]
        f0 = {"query_phase_max_start_lag_ms": max(lags) if lags else None,
              "query_phase_overrun_ticks": sum(overruns) if overruns else None}
        lag_ms = f0.get("query_phase_max_start_lag_ms")
        delay_s = resources.get("query_delay_s")
        report["arms"][arm]["feeder_lag"] = {
            "query_phase_max_start_lag_s": None if lag_ms is None else lag_ms / 1e3,
            "query_phase_overrun_ticks": f0.get("query_phase_overrun_ticks"),
            "query_delay_s": delay_s,
            "ingest_falling_behind": (None if lag_ms is None or delay_s is None
                                      else lag_ms / 1e3 > delay_s),
        }
    (args.run / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    for note in report["notes"]:
        print("note:", note)
    for arm, r in report["arms"].items():
        print(f"== {arm}: cpu {r['cpu_core_seconds']:.1f} core-s (query phase "
              f"{r['cpu_core_seconds_query_phase']:.1f}), peak RSS {r['peak_rss_bytes_sum'] / 2**20:.0f} MiB, "
              f"forwarded {r['forwarded']}, wall {r['wall_seconds']:.0f} s")
        for role, c in r["components"].items():
            print(f"   [{role:10}] cpu ingest {c['ingest_phase_cpu_s']:7.1f} s, query {c['query_phase_cpu_s']:7.1f} s; "
                  f"peak RSS {c['peak_rss_bytes'] / 2**20:6.0f} MiB, steady RSS "
                  f"{(c['steady_rss_bytes'] or 0) / 2**20:6.0f} MiB  {c['name']}")
            top = sorted(c["threads"].items(), key=lambda kv: -(kv[1]["ingest_phase_cpu_s"] + kv[1]["query_phase_cpu_s"]))[:4]
            for name, t in top:
                print(f"       thread {name:18} ingest {t['ingest_phase_cpu_s']:7.1f} s, query {t['query_phase_cpu_s']:7.1f} s")
        for qid, q in r["queries"].items():
            err = q.get("max_rel_error", q.get("max_rel_diff_vs_prometheus"))
            print(f"   {qid:18} n={q['evaluations']:3} p50={q['p50_ms'] or 0:8.2f} p99={q['p99_ms'] or 0:8.2f} ms "
                  f"err={err} recall={q.get('min_recall')} pass={q['passing_evaluations']} "
                  f"meets={q['meets_target']} plan={q['answered_by_plan']} fwd={q['forwarded']} "
                  f"refused={q['refused_or_failed']}")


if __name__ == "__main__":
    main()
