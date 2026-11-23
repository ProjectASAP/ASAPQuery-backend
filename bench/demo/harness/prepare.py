#!/usr/bin/env python3
"""Prepare one random workload for the three-arm run.

Writes, under --out:
  manifest.json        the manifest, its label_0 cardinality scaled to --label0
  feeder_table.json    the paced feeder's workload table (data, data_total)
  queries.json         the PromQL queries and their schedule
  asapquery/           ASAPQuery's own planner configs (the archived pilot)
  backend/             per-query PlanningWorkloads and plans, and the merged plan

The same scaled manifest feeds every arm and every planner.
"""
import argparse
import copy
import json
import os
import shutil
import subprocess

from common import BENCH, BIN, WORKLOADS, manifest_path, pilot_dir, read_json, write_json
from merge_plans import merge



def _asapquery_function(path, name):
    """`name` from ASAPQuery's module at `path`, without importing the module
    (its imports pull in the whole experiment framework)."""
    import ast

    tree = ast.parse(path.read_text())
    (node,) = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    namespace = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


# ASAPQuery's own rendering of a manifest's data facts as a feeder table.
paced_feeder_table = _asapquery_function(
    WORKLOADS.parent / "random_workload_artifacts.py", "paced_feeder_table")

# accuracy_sla 0.95 -> max_error 0.05; failure probability as the run allows.
FAILURE_PROBABILITY = 0.3
# Everything built at ingestion, as ASAPQuery does: no raw data and no
# query-time result kept by the backend for the plan to read.
CAPABILITIES = {
    "supported_summaries": None,
    "raw_data_retained": False,
    "query_time_results_retained": False,
}


# ASAPPlanner #780: `SourceFacts::values_non_negative`. A backend pinned
# before it rejects the field, so the converter declares the gauge's sign
# only when the backend's Planner has it.
GAUGE_SIGN_COMMIT = "7565c47e"


def backend_planner_has_gauge_sign():
    import re

    cargo = (BENCH / "backend/Cargo.toml").read_text()
    rev = re.search(r'asap-planner = \{[^}]*rev = "([0-9a-f]+)"', cargo).group(1)
    return subprocess.run(
        ["git", "-C", os.environ["ASAP_PLANNER_DIR"], "merge-base", "--is-ancestor", GAUGE_SIGN_COMMIT, rev],
        capture_output=True,
    ).returncode == 0


# Range mode (--range STEP_S:SPAN_S[:REFRESH_S]): every query is a range
# request [t - span, t] at `step`, sent every `refresh` (default: step), as a
# Grafana panel refreshing on that interval; planned with the workload's
# `range_evaluation` {step_ms, span_ms}. None: instant queries.
RANGE = None  # {"step_ms", "span_ms", "refresh_ms"}


def parse_range(text):
    parts = [int(float(x) * 1000) for x in text.split(":")]
    step, span = parts[0], parts[1]
    refresh = parts[2] if len(parts) > 2 else step
    assert step > 0 and span % step == 0, "span must be a multiple of step"
    return {"step_ms": step, "span_ms": span, "refresh_ms": refresh}


def query_interval_ms(q):
    return RANGE["refresh_ms"] if RANGE else q["interval_ms"]


def query_range(q):
    """The query's range request {step_ms, span_ms}: --range for every query, else the
    manifest query's own `range` (2026-11-22, demo48: one range panel among instant
    queries), else None."""
    if RANGE:
        return {"step_ms": RANGE["step_ms"], "span_ms": RANGE["span_ms"]}
    return q.get("range")


def planning_workload(manifest, ids):
    """The Planner's `PlanningWorkload` (JSON, ASAPPlanner main's format:
    `fixed_interval_at` demand, `column_signs`) for the manifest's PromQL
    queries `ids`, mapped as ASAPPlanner's `asapquery_random_workload`
    example maps them: each query repeats at its interval on the grid the
    harness evaluates on (phase 0); max_error = 1 - accuracy_sla with
    FAILURE_PROBABILITY; every metric ingests `leaf_count` series at
    `scrape_interval_ms`; `data` (Pareto) is declared non-negative."""
    facts = manifest["data_facts"]
    leaf_count = facts["series"]["leaf_count"]
    interval = facts["scrape_interval_ms"]
    chosen = [q for q in manifest["queries"] if q["id"] in ids]
    # Every metric a query reads: its RQEs' metrics plus an optional `metrics`
    # list (multi-metric queries: joins, ratios).
    read = sorted({r["metric"] for q in chosen for r in q["rqes"]} | {m for q in chosen for m in q.get("metrics", [])})
    sources = {}
    extra = manifest.get("extra_metrics", {})
    for name in read:
        if name in extra:
            sources[name] = extra_metric_facts(extra[name])
            mags = (facts.get("column_magnitudes") or {}).get(name)
            if mags and os.environ.get("DECLARE_MAGNITUDES", "1") == "1":
                sources[name]["column_magnitudes"] = mags
            if extra[name]["type"] == "gauge" and os.environ.get("FEEDER_VALUES", "").startswith(("zipf", "realistic")):
                sources[name]["column_signs"] = {"value": "non_negative"}
            continue
        # A manifest's `metric_names` renames the generator's metrics
        # ({"data": "lat", "data_total": "reqs"}); data facts keep the
        # generator's names.
        m = facts["metrics"][{v: k for k, v in manifest.get("metric_names", {}).items()}.get(name, name)]
        fact = {"series_count": leaf_count, "metric_type": m["kind"], "sample_interval": interval}
        if m["kind"] != "counter" and any(x in m.get("value_shape", "") for x in ("pareto", "lognormal", "smooth", "zipf")):
            fact["column_signs"] = {"value": "non_negative"}
        mags = (facts.get("column_magnitudes") or {}).get(name)
        if mags and os.environ.get("DECLARE_MAGNITUDES", "1") == "1":
            fact["column_magnitudes"] = mags
        sources[name] = fact
    return {
        "query_workload": {
            "language": "promql",
            "query_batch": None,
            "repeating_queries": [
                {
                    "query": q["promql"],
                    "demand": {"fixed_interval_at": {"interval": query_interval_ms(q), "evaluation_phase": 0}},
                    "requirements": {
                        "accuracy": {"max_error": round(1 - q["rqes"][0]["accuracy_sla"], 12),
                                     "failure_probability": FAILURE_PROBABILITY},
                        "max_latency_ms": None,
                    },
                    "predictability": {"predictable": {"known_at": None}},
                    **({"range_evaluation": query_range(q)} if query_range(q) else {}),
                }
                for q in chosen
            ],
        },
        "data_workload": {
            "arrival": {"ingesting": {"rows_per_second": sum(
                f["series_count"] * 1000.0 / f["sample_interval"] for f in sources.values())}},
            "sources": sources,
        },
    }


def extra_metric_series(spec):
    count = 1
    for label in spec["labels"]:
        count *= label["cardinality"]
    return count


def extra_metric_facts(spec):
    """Planner source facts of an `extra_metrics` entry (a paced-feeder
    metric spec): series, type, sample interval, and a Pareto gauge's sign."""
    fact = {"series_count": extra_metric_series(spec), "metric_type": spec["type"],
            "sample_interval": int(1000 / spec.get("samples_per_sec_per_series", 1))}
    if spec["type"] == "gauge" and "pareto" in spec.get("values", {}).get("dist", {}):
        fact["column_signs"] = {"value": "non_negative"}
    return fact


def declared_magnitudes(manifest, table):
    """The value ranges the feeder can produce, per metric (2026-11-21): the
    planner prices DDSketch states from `column_magnitudes` {"value": {min_abs,
    max_abs}}; without them it assumes [1, 1e9] and misprices wide values.
    Ranges follow the feeder's value mode (feeder table `values.realistic`) and
    the series' Zipf scales (paced feeder `zipf_scales`: rank r gets
    r^-theta · n / sum r^-theta); counters cover the readings of a feed of
    MAGNITUDE_FEED_S (default lookback + 2 h)."""
    import math
    out = {}
    feed_s = float(os.environ.get("MAGNITUDE_FEED_S", "0")) or \
        (max(q["lookback_ms"] for q in manifest["queries"]) / 1000 + 7200)
    for name, spec in table["workloads"][0]["e2e"]["metrics"].items():
        if spec.get("type") not in ("gauge", "counter"):
            continue
        vals = spec["values"]
        skew = (vals.get("value") or vals.get("increment"))
        n, theta = skew["keys"], skew["zipf"]
        norm = n / sum((r + 1) ** -theta for r in range(n))
        smax, smin = norm, (n ** -theta) * norm
        sps = spec["samples_per_sec_per_series"]
        samples = feed_s * sps
        real = vals.get("realistic")
        kind = (real or {}).get("kind")
        if spec["type"] == "gauge":
            if kind == "zipf" and real.get("integer", True):
                lo, hi = 1.0, real["n"] * max(1, round(smax))
            elif kind == "zipf":
                lo, hi = max(0.01, smin), real["n"] * smax
            elif kind == "latency":
                spread = math.exp(6 * real.get("sigma", 0.5))
                lo = max(0.01, real.get("median", 50) * smin * (1 - real.get("drift", 0.2)) / spread)
                hi = real.get("median", 50) * smax * (1 + real.get("drift", 0.2)) * spread
            elif kind == "smooth":
                a = real.get("amplitude", 0.2) + real.get("noise", 0.02)
                lo, hi = max(0.01, real.get("level", 100) * smin * (1 - a)), real.get("level", 100) * smax * (1 + a)
            else:  # original: Pareto(shape) x scale, up to the 1e-12 upper quantile
                shape = vals["dist"]["pareto"]
                lo, hi = smin, smax * (1e-12) ** (-1 / shape)
        else:
            if kind == "paired":  # a share of its base counter: at most the base's readings
                base = table["workloads"][0]["e2e"]["metrics"][real["of"]]["values"]["realistic"]
                zn, zs = base["n"], base["s"]
                h = sum(k ** -zs for k in range(1, min(zn, 10**6) + 1))
                mean = sum(k ** (1 - zs) for k in range(1, min(zn, 10**6) + 1)) / h
                lo, hi = 1.0, samples * mean * 1.5 * real.get("ratio_max", 1.0)
            elif kind == "histogram":  # at most obs_max observations per sample
                lo, hi = 1.0, samples * real.get("obs_max", 20)
            elif kind == "zipf_counter":
                zn, zs = real["n"], real["s"]
                h = sum(k ** -zs for k in range(1, min(zn, 10**6) + 1))
                mean = sum(k ** (1 - zs) for k in range(1, min(zn, 10**6) + 1)) / h
                lo, hi = 1.0, samples * mean * 1.5
            elif kind == "counter":
                lo, hi = 1.0, samples * 2 * max(1.0, real.get("rate", 1.0) * smax)
            else:  # original: (n + 1) · scale
                lo, hi = smin, samples * smax
        out[name] = {"value": {"min_abs": float(lo), "max_abs": float(hi)}}
    return out


def scaled_manifest(manifest, label0):
    manifest = copy.deepcopy(manifest)
    # `extra_metrics`: paced-feeder metric specs sent beside the generator's
    # metrics; a label marked `scale_with_label0` gets --label0 values.
    for spec in manifest.get("extra_metrics", {}).values():
        for label in spec["labels"]:
            # true: label_0's cardinality; a number (2026-11-22, demo48): round(label_0 x it).
            factor = label.pop("scale_with_label0", False)
            if factor and label0:
                label["cardinality"] = max(1, round(label0 * float(factor)))
        keys = extra_metric_series(spec)
        for v in spec["values"].values():
            if isinstance(v, dict) and "keys" in v:
                v["keys"] = keys
    leaf_count = 1
    for label in manifest["labels"]:
        if label["name"] == "label_0" and label0:
            label["cardinality"] = label0
        leaf_count *= label["cardinality"]
    manifest["data_facts"]["series"]["leaf_count"] = leaf_count
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--set", required=True,
                        help="seed-862, seed-862-x5, seed-862-long, or demo (../workloads/*.json)")
    parser.add_argument("--workload", required=True, help="workload-01 .. workload-10, or dashboard-heavy")
    parser.add_argument("--label0", type=int, help="label_0 values per job (scales the series count)")
    parser.add_argument("--out", required=True)
    parser.add_argument("--backend-only", action="store_true",
                        help="redo only backend/ (e.g. after a backend re-pin), keeping the rest")
    parser.add_argument("--range", metavar="STEP_S:SPAN_S[:REFRESH_S]",
                        help="range-query group: /api/v1/query_range over [t - span, t] at step, sent "
                             "every refresh (default step), e.g. 15:3600; planned with range_evaluation")
    parser.add_argument("--skip-plan", action="store_true",
                        help="no backend/ planning (runs of the prometheus and victoriametrics arms only)")
    args = parser.parse_args()
    global RANGE
    RANGE = parse_range(args.range) if args.range else None
    out = __import__("pathlib").Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    manifest = scaled_manifest(read_json(manifest_path(args.set, args.workload)), args.label0)
    promql = [q for q in manifest["queries"] if q["promql"]]
    if args.backend_only:
        assert read_json(out / "manifest.json") == manifest, "a different scale or workload"
        prepare_backend(out, [q for q in promql if "asap" in q.get("engines", ["asap"])])
        print(out)
        return
    write_json(out / "manifest.json", manifest)

    table = paced_feeder_table(manifest)
    # The SQL `events` records go to neither Prometheus nor the backend.
    del table["workloads"][0]["e2e"]["metrics"]["events"]
    metrics = table["workloads"][0]["e2e"]["metrics"]
    for old, new in manifest.get("metric_names", {}).items():
        metrics[new] = metrics.pop(old)
    metrics.update(copy.deepcopy(manifest.get("extra_metrics", {})))
    # FEEDER_VALUES (2026-11-21): "realistic" (or "realistic-smooth") replaces the
    # independent random doubles with realistic values, the same for every arm:
    # `data` latency-like (log-normal ms, 2 decimals; "-smooth": a smooth gauge,
    # sinusoid + smooth noise, 2 decimals), `data_total` integer increments with
    # occasional resets (5% of the series every 30-120 min). Needs the paced feeder
    # with `values.realistic` (FEEDER_BIN). Unset: the original values.
    mode = os.environ.get("FEEDER_VALUES", "")
    if mode in ("zipf", "zipf-scaled"):
        # FEEDER_VALUES=zipf (2026-11-21, the rerun default): gauge values Zipf(s)
        # over the integers 1..N times the series' scale (2 decimals); counter
        # increments Zipf(s) integers over 1..N with the same 5% resets. ZIPF_S
        # (default 1.1) and ZIPF_N (default 1e6) are the knobs.
        zs, zn = float(os.environ.get("ZIPF_S", "1.1")), int(float(os.environ.get("ZIPF_N", "1e6")))
        metrics = table["workloads"][0]["e2e"]["metrics"]
        # Every gauge and counter of the table (the generator's two, renamed or not,
        # and any `extra_metrics`).
        for mname, mspec in metrics.items():
            if mspec.get("type") == "gauge":
                v = mspec["values"]
                v.pop("dist", None)
                # zipf (default): integer values k * max(1, round(series scale)); zipf-scaled:
                # k * scale rounded to 2 decimals (non-integers; compresses far worse).
                v["realistic"] = {"kind": "zipf", "s": zs, "n": zn, "decimals": 2, "integer": mode == "zipf"}
            elif mspec.get("type") == "counter":
                mspec["values"]["realistic"] = {"kind": "zipf_counter", "s": zs, "n": zn,
                                                "reset_fraction": 0.05, "reset_min_s": 1800, "reset_max_s": 7200}
        # `value_overrides` (2026-11-21): per-metric realistic specs that make the data
        # semantically valid, e.g. histogram buckets cumulative over `le`
        # ({"kind": "histogram"}), hits <= lookups and errors <= requests
        # ({"kind": "paired", "of": <base counter>, "ratio_min", "ratio_max"}).
        for oname, ospec in manifest.get("value_overrides", {}).items():
            metrics[oname]["values"]["realistic"] = copy.deepcopy(ospec)
        facts = manifest["data_facts"]["metrics"]
        facts.setdefault("data", {})["value_shape"] = (f"zipf_integers_s{zs}_n{zn}_integer_series_multiplier"
                                                       if mode == "zipf" else f"zipf_integers_s{zs}_n{zn}_scaled_2dp")
        facts.setdefault("data_total", {})["resets"] = "occasional (5% of series every 30-120 min)"
        facts["data_total"]["increment_shape"] = f"zipf_integers_s{zs}_n{zn}"
        manifest["data_facts"]["feeder_values"] = f"{mode} s={zs} N={zn}"
        write_json(out / "manifest.json", manifest)
    elif mode.startswith("realistic"):
        metrics = table["workloads"][0]["e2e"]["metrics"]
        if "data" in metrics:
            v = metrics["data"]["values"]
            v.pop("dist", None)
            v["realistic"] = ({"kind": "smooth", "level": 100.0, "amplitude": 0.2, "period_s": 3600, "noise": 0.02,
                               "noise_period_s": 30, "decimals": 2} if mode == "realistic-smooth" else
                              {"kind": "latency", "median": 50.0, "sigma": 0.5, "drift": 0.2, "drift_period_s": 600,
                               "decimals": 2})
        if "data_total" in metrics:
            metrics["data_total"]["values"]["realistic"] = {"kind": "counter", "rate": 1.0, "reset_fraction": 0.05,
                                                            "reset_min_s": 1800, "reset_max_s": 7200}
        facts = manifest["data_facts"]["metrics"]
        facts.setdefault("data", {})["value_shape"] = ("smooth_sinusoid_noise_2dp" if mode == "realistic-smooth"
                                                       else "lognormal_latency_ms_2dp")
        facts.setdefault("data_total", {})["resets"] = "occasional (5% of series every 30-120 min)"
        facts["data_total"]["increment_shape"] = "integer_jittered_rank_scaled"
        manifest["data_facts"]["feeder_values"] = mode
        write_json(out / "manifest.json", manifest)
    manifest["data_facts"]["column_magnitudes"] = declared_magnitudes(manifest, table)
    write_json(out / "manifest.json", manifest)
    write_json(out / "feeder_table.json", table)
    # FEEDER_SHARDS=N (2026-11-22, demo48): N feeder tables (feeder_table.<i>.json), the
    # metrics split by samples/s (a paired metric with its base), one feeder process each.
    # Values do not change: the feeder seeds each metric from its name.
    for old in out.glob("feeder_table.*.json"):
        old.unlink()
    shards = int(os.environ.get("FEEDER_SHARDS", "1"))
    if shards > 1:
        allm = table["workloads"][0]["e2e"]["metrics"]
        def series_of(spec):
            n = 1
            for l in spec.get("labels", []):
                n *= l["cardinality"]
            return n
        groups = {}
        for name, spec in allm.items():
            real = (spec.get("values") or {}).get("realistic") or {}
            root = real.get("of", name) if real.get("kind") == "paired" else name
            groups.setdefault(root, []).append(name)
        def rate(names):
            return sum(series_of(allm[n]) * allm[n].get("samples_per_sec_per_series", 1) for n in names
                       if "labels" in allm[n])
        bins = [[] for _ in range(shards)]
        for names in sorted(groups.values(), key=rate, reverse=True):
            min(bins, key=lambda b: sum(rate(g) for g in b)).append(names)
        for i, b in enumerate(bins):
            t = copy.deepcopy(table)
            t["workloads"][0]["e2e"]["metrics"] = {n: allm[n] for g in b for n in g}
            write_json(out / f"feeder_table.{i}.json", t)
            print(f"feeder shard {i}: {sum(rate(g) for g in b):,.0f} samples/s: {[n for g in b for n in g]}")

    write_json(
        out / "queries.json",
        {
            "series": manifest["data_facts"]["series"]["leaf_count"],
            "queries": [
                {
                    "id": q["id"],
                    "template": q["id"].split("-", 1)[1],
                    "promql": q["promql"],
                    "interval_ms": query_interval_ms(q),
                    # The data a request reads: its window, plus the span in range mode.
                    "lookback_ms": q["lookback_ms"] + (query_range(q) or {}).get("span_ms", 0),
                    "window_ms": q["lookback_ms"],
                    **({"range": query_range(q)} if query_range(q) else {}),
                    **({"engines": q["engines"]} if q.get("engines") else {}),
                }
                for q in promql
            ],
            "excluded_sql": [q["id"] for q in manifest["queries"] if not q["promql"]],
        },
    )

    # ASAPQuery's archived pilot plan (only ASAPQuery's own workloads have one).
    if pilot_dir(args.set, args.workload).exists():
        asapq = out / "asapquery"
        asapq.mkdir(exist_ok=True)
        for name in ("streaming_config.yaml", "inference_config.yaml"):
            shutil.copy(pilot_dir(args.set, args.workload) / name, asapq / name)

    if not args.skip_plan:
        # A query with `engines` not naming asap (2026-11-22, demo48: L1, L5) is not planned;
        # the asap arm does not send it (run.py).
        prepare_backend(out, [q for q in promql if "asap" in q.get("engines", ["asap"])])
    print(out)


def control_plane_flags():
    """The flags this control plane build takes (older builds take
    `--cost-model`, newer ones `--cost-inputs` and `--report`)."""
    try:
        return subprocess.run([BIN["control_plane"], "--help"], capture_output=True, text=True).stdout
    except OSError:
        return ""


def plan(workload, plan_path, backend):
    """Plan `workload` with the backend's control plane, offline. Returns
    {planning_seconds, planning_cpu_seconds, planning_peak_rss_bytes, error}:
    the planner's one-time cost, never charged to an arm."""
    import time

    flags = control_plane_flags()
    cmd = [BIN["control_plane"], "--workload", workload, "--out", plan_path,
           "--deployment-capabilities", backend / "deployment_capabilities.json"]
    # Accuracy guarantees are enforced (a quantile gets a DDSketch whose
    # guarantee meets the 5% target); IGNORE_ACCURACY_GUARANTEES=1 compares
    # candidates on cost alone, as the earlier MVP runs did.
    if os.environ.get("IGNORE_ACCURACY_GUARANTEES") == "1":
        cmd.append("--ignore-accuracy-guarantees")
    if "--cost-model" in flags:
        cmd += ["--cost-model", "aws-fargate-us-east-1"]
    # PLANNER_COST_INPUTS=<CostInputs JSON> (2026-11-21, the memory-budget config):
    # Stage 3's cost model and limits, e.g. cost model 3 LowestMeanLatency with a
    # memory limit. Recorded in plan_joint_summary.json as cost_inputs.
    if os.environ.get("PLANNER_COST_INPUTS"):
        if "--cost-inputs" not in flags:
            raise SystemExit("PLANNER_COST_INPUTS set but this control_plane has no --cost-inputs")
        cmd += ["--cost-inputs", os.environ["PLANNER_COST_INPUTS"]]
    if "--report" in flags:
        cmd += ["--report", str(plan_path).replace(".json", ".report.json")]
    started = time.monotonic()
    proc = subprocess.Popen([str(c) for c in cmd], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = proc.stdout.read()
    # wait4 gives this child's own CPU time and peak RSS (ru_maxrss, KiB).
    _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    out = {
        "planning_seconds": time.monotonic() - started,
        "planning_cpu_seconds": usage.ru_utime + usage.ru_stime,
        "planning_peak_rss_bytes": usage.ru_maxrss * 1024,
        "error": None,
        "cost_inputs": os.environ.get("PLANNER_COST_INPUTS"),
    }
    if proc.returncode != 0:
        out["error"] = (output.strip().splitlines() or ["?"])[-1]
    return out


def report_summary(report_path):
    """Stage 3's prediction for the selected plan (control_plane --report)."""
    if not report_path.exists():
        return {}
    report = read_json(report_path)
    usage = report.get("predicted_usage") or {}
    return {
        "cost_per_second": report.get("cost_per_second"),
        "predicted_cpu_core_seconds_per_second": usage.get("cpu_core_seconds_per_second"),
        "predicted_peak_memory_bytes": usage.get("peak_memory_bytes"),
    }


def selection_summary(plan_json):
    """What the plan selected; tolerant of both plan formats (the older
    backend plan with `selection`, and the Planner's `InstallablePlan`
    {dag, query_texts})."""
    selection = plan_json.get("selection") or {}
    usage = selection.get("predicted_usage") or {}
    dag = plan_json.get("dag") or {}
    nodes = (dag.get("dag") or dag).get("nodes") or []
    return {
        "selected_candidate_id": selection.get("selected_candidate_id"),
        "cost_per_second": selection.get("cost_per_second"),
        "queries_accepted_without_guarantee": selection.get("queries_accepted_without_guarantee", []),
        "predicted_cpu_core_seconds_per_second": usage.get("cpu_core_seconds_per_second"),
        "predicted_peak_memory_bytes": usage.get("peak_memory_bytes"),
        "nodes": len(nodes),
        "queries": len(plan_json.get("query_texts") or []) or None,
        "stored_outputs": len(plan_json.get("stored_outputs") or []),
        "query_time_raw_windows": len(plan_json.get("query_time_raw_windows") or []),
    }


def prepare_backend(out, promql):
    """Each query planned alone by the backend's own control plane (and the
    plans merged), then the whole PromQL workload planned jointly. The data
    plane installs the joint plan (`plan.json`); `plan_alone_merged.json` is
    the merge of the per-query plans."""
    backend = out / "backend"
    if backend.exists():
        shutil.rmtree(backend)
    (backend / "workloads").mkdir(parents=True, exist_ok=True)
    (backend / "plans").mkdir(exist_ok=True)
    capabilities = dict(CAPABILITIES)
    # FINAL_QUERY_RESULTS_CACHED=1: the data plane keeps each planned query's answer per
    # evaluation time (its result cache; backend control_plane >= 243fece5 declares it), so a
    # range request's earlier steps are read from the cache. Off by default (older Planners
    # reject the field).
    if os.environ.get("FINAL_QUERY_RESULTS_CACHED") == "1":
        capabilities["final_query_results_cached"] = True
    write_json(backend / "deployment_capabilities.json", capabilities)

    def emit(workload, ids):
        # WORKLOAD_EMITTER=converter: ASAPPlanner's example binary, for a
        # backend pinned to a Planner before `fixed_interval_at`.
        if os.environ.get("WORKLOAD_EMITTER", "python") == "python":
            write_json(workload, planning_workload(read_json(out / "manifest.json"), set(ids)))
            return
        subprocess.run(
            [BIN["converter"], out / "manifest.json", "--query", ",".join(ids),
             "--failure-probability", str(FAILURE_PROBABILITY), "--emit-workload", workload,
             *([] if backend_planner_has_gauge_sign() else ["--undeclared-gauge-sign"])],
            check=True, stdout=subprocess.DEVNULL,
        )

    plans, alone = [], {}
    for q in promql:
        workload = backend / "workloads" / f"{q['id']}.json"
        emit(workload, [q["id"]])
        plan_path = backend / "plans" / f"{q['id']}.json"
        alone[q["id"]] = plan(workload, plan_path, backend)
        if alone[q["id"]]["error"] is None:
            plan_json = read_json(plan_path)
            alone[q["id"]].update(selection_summary(plan_json))
            alone[q["id"]].update(report_summary(plan_path.with_suffix(".report.json")))
            plans.append(plan_json)
    write_json(backend / "plan_alone.json", alone)
    write_json(backend / "plan_failures.json", {k: v["error"] for k, v in alone.items() if v["error"]})
    try:
        write_json(backend / "plan_alone_merged.json", merge(plans))
    except Exception as e:  # noqa: BLE001 - merge_plans.py reads the older plan format
        print(f"plan_alone_merged.json not written: {e!r}")

    workload = backend / "workloads" / "joint.json"
    emit(workload, [q["id"] for q in promql])
    joint = plan(workload, backend / "plan_joint.json", backend)
    if joint["error"] is None:
        joint.update(selection_summary(read_json(backend / "plan_joint.json")))
        joint.update(report_summary(backend / "plan_joint.report.json"))
        shutil.copy(backend / "plan_joint.json", backend / "plan.json")
    write_json(backend / "plan_joint_summary.json", joint)
    print(json.dumps({"alone": {k: v.get("error") or "ok" for k, v in alone.items()},
                      "joint": joint}, indent=1))


if __name__ == "__main__":
    main()
