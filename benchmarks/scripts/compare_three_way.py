#!/usr/bin/env python3
"""compare_three_way.py — three-way evaluation across ASAP / Prom / VM.

Inputs (default locations under benchmarks/reports/):
    asap_promql.json   asap_adhoc.json
    prom_promql.json   prom_adhoc.json
    vm_promql.json     vm_adhoc.json
    concurrency_sweep.csv

Output: benchmarks/reports/three_way_eval.md, with:
  - Table 1: per-query latency P50/P99 across ASAP-sketch / ASAP-adhoc /
    Prom / VM
  - Table 2: per-query relative error of ASAP-sketch vs Prom (ground truth)
  - Table 3: throughput-vs-concurrency tabular CDF data (read straight
    from the CSV produced by run_concurrency_sweep.py)
  - Capability matrix: which queries each backend supports

Usage:
    python benchmarks/scripts/compare_three_way.py [--reports-dir DIR] [--output FILE]
"""

import argparse
import csv
import json
import os
import statistics
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REPORTS = os.path.join(HERE, "..", "reports")
DEFAULT_OUTPUT = os.path.join(DEFAULT_REPORTS, "three_way_eval.md")


def percentile(values, pct):
    if not values:
        return float("nan")
    s = sorted(values)
    n = len(s)
    if n == 1:
        return s[0]
    idx = (pct / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def valid(latencies):
    return [x for x in (latencies or []) if x is not None]


def load_json(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def label_key(metric):
    return json.dumps(metric, sort_keys=True)


def relative_error(a, b):
    denom = max(abs(b), 1e-9)
    return abs(a - b) / denom


def compare_results(prom_data, asap_data):
    """Same shape-comparison logic as compare.py — returns max relative error."""
    if not prom_data and not asap_data:
        return 0.0
    if not prom_data or not asap_data:
        return None
    pmap, amap = {}, {}
    for entry in prom_data:
        try:
            pmap[label_key(entry.get("metric", {}))] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            pass
    for entry in asap_data:
        try:
            amap[label_key(entry.get("metric", {}))] = float(entry["value"][1])
        except (KeyError, IndexError, ValueError, TypeError):
            pass
    if not pmap or not amap:
        return None
    if len(pmap) == 1 and len(amap) == 1:
        return relative_error(next(iter(amap.values())), next(iter(pmap.values())))
    max_err = 0.0
    for k, pv in pmap.items():
        if k in amap:
            max_err = max(max_err, relative_error(amap[k], pv))
    return max_err


def fmt(v, suffix=""):
    if v is None or (isinstance(v, float) and v != v):
        return "n/a"
    if isinstance(v, float):
        return f"{v:.1f}{suffix}"
    return f"{v}{suffix}"


def per_query_latency_row(qid, asap, prom, vm):
    def stats(d):
        if not d or qid not in d.get("results", {}):
            return ("n/a", "n/a", "missing")
        r = d["results"][qid]
        if r.get("status") != "success":
            return ("n/a", "n/a", "error")
        lats = valid(r.get("latencies_ms", []))
        if not lats:
            return ("n/a", "n/a", "no-samples")
        return (f"{percentile(lats, 50):.1f}", f"{percentile(lats, 99):.1f}", "ok")

    a = stats(asap)
    p = stats(prom)
    v = stats(vm)
    return {"id": qid, "asap": a, "prom": p, "vm": v}


def build_table_1(asap_promql, asap_adhoc, prom_promql, prom_adhoc, vm_promql, vm_adhoc):
    """Per-query P50/P99 for both workloads. ASAP gets two columns: sketch (W1)
    + adhoc-fallback (W2). Prom and VM each have one column (the same path)."""
    lines = ["## Table 1 — Per-query latency (P50 / P99, milliseconds)\n"]
    lines.append("Workloads: W1 = sketch path (promql_suite); W2 = adhoc/fallback path (adhoc_suite). "
                 "ASAP-sketch is W1 against ASAP; ASAP-adhoc is W2 against ASAP (cold-store + Prom fallback). "
                 "Prom / VM run both suites natively.\n")
    lines.append("| Query | Workload | ASAP P50 | ASAP P99 | Prom P50 | Prom P99 | VM P50 | VM P99 |")
    lines.append("|-------|:--------:|:--------:|:--------:|:--------:|:--------:|:------:|:------:|")

    def emit(workload_label, asap_data, prom_data, vm_data):
        if not asap_data:
            return
        for qid in asap_data["results"].keys():
            row = per_query_latency_row(qid, asap_data, prom_data, vm_data)
            lines.append(
                f"| {qid} | {workload_label} | "
                f"{row['asap'][0]} | {row['asap'][1]} | "
                f"{row['prom'][0]} | {row['prom'][1]} | "
                f"{row['vm'][0]} | {row['vm'][1]} |"
            )

    emit("W1", asap_promql, prom_promql, vm_promql)
    emit("W2", asap_adhoc, prom_adhoc, vm_adhoc)
    lines.append("")
    return "\n".join(lines)


def build_table_2(asap_promql, prom_promql):
    """Relative error of ASAP-sketch vs Prom ground truth on W1."""
    lines = ["## Table 2 — Relative error of ASAP-sketch vs Prometheus (ground truth)\n"]
    lines.append("Per-query max relative error on W1 (sketch path). Prom is treated as ground truth. "
                 "Higher error is expected for approximate (quantile) queries; exact aggregations "
                 "(sum/avg/max/min) should be near-zero.\n")
    lines.append("| Query | Approximate | Max Rel Error | Notes |")
    lines.append("|-------|:-----------:|:-------------:|-------|")
    if not (asap_promql and prom_promql):
        lines.append("| _missing inputs_ | | | |")
        return "\n".join(lines) + "\n"

    for qid, ar in asap_promql["results"].items():
        pr = prom_promql["results"].get(qid, {})
        approx = "yes" if ar.get("approximate") else "no"
        if ar.get("status") != "success" or pr.get("status") != "success":
            err_s, note = "n/a", "non-success status"
        else:
            err = compare_results(pr.get("data", []), ar.get("data", []))
            err_s = "n/a" if err is None else f"{err:.4f}"
            note = ""
        lines.append(f"| {qid} | {approx} | {err_s} | {note} |")
    lines.append("")
    return "\n".join(lines)


def build_table_3(reports_dir):
    csv_path = os.path.join(reports_dir, "concurrency_sweep.csv")
    lines = ["## Table 3 — Throughput vs. Concurrency\n"]
    lines.append("Per-backend throughput (queries/s) and tail latency at fixed concurrency levels. "
                 "Workload: promql_suite, round-robin. Source: `concurrency_sweep.csv`.\n")
    if not os.path.exists(csv_path):
        lines.append(f"_no concurrency_sweep.csv found at {csv_path} — run run_concurrency_sweep.py first._\n")
        return "\n".join(lines)

    lines.append("| Backend | Concurrency | Total Queries | Throughput (qps) | P50 (ms) | P99 (ms) |")
    lines.append("|---------|:-----------:|:-------------:|:----------------:|:--------:|:--------:|")
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            lines.append(
                f"| {row['backend']} | {row['concurrency']} | {row['total_queries']} | "
                f"{row['throughput_qps']} | {row['p50_ms']} | {row['p99_ms']} |"
            )
    lines.append("")
    return "\n".join(lines)


def build_capability_matrix(asap_promql, asap_adhoc, prom_promql, prom_adhoc, vm_promql, vm_adhoc):
    """Capability matrix: a backend 'supports' a query if the corresponding run
    returned status=success with a non-empty data array on at least one
    iteration. ASAP shows the *path* used (sketch/cold/prom-forward) when known."""
    lines = ["## Capability Matrix\n"]
    lines.append("✓ = backend returned a non-empty success response. - = error/empty/missing. "
                 "ASAP path column: `sketch` for W1 queries; for W2, the `fallback` declared in "
                 "`adhoc_suite.json` (`cold_store` or `prometheus`).\n")
    lines.append("| Query | Workload | ASAP | ASAP path | Prom | VM |")
    lines.append("|-------|:--------:|:----:|:---------:|:----:|:--:|")

    def supports(d, qid):
        if not d or qid not in d.get("results", {}):
            return "-"
        r = d["results"][qid]
        if r.get("status") == "success" and r.get("data"):
            return "✓"
        return "-"

    def emit(workload, asap_data, prom_data, vm_data, default_path):
        if not asap_data:
            return
        for qid, ar in asap_data["results"].items():
            path = ar.get("fallback") or default_path
            lines.append(
                f"| {qid} | {workload} | "
                f"{supports(asap_data, qid)} | {path} | "
                f"{supports(prom_data, qid)} | {supports(vm_data, qid)} |"
            )

    emit("W1", asap_promql, prom_promql, vm_promql, "sketch")
    emit("W2", asap_adhoc, prom_adhoc, vm_adhoc, "fallback")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Three-way ASAP / Prom / VM eval")
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    asap_promql = load_json(os.path.join(args.reports_dir, "asap_promql.json"))
    asap_adhoc  = load_json(os.path.join(args.reports_dir, "asap_adhoc.json"))
    prom_promql = load_json(os.path.join(args.reports_dir, "prom_promql.json"))
    prom_adhoc  = load_json(os.path.join(args.reports_dir, "prom_adhoc.json"))
    vm_promql   = load_json(os.path.join(args.reports_dir, "vm_promql.json"))
    vm_adhoc    = load_json(os.path.join(args.reports_dir, "vm_adhoc.json"))

    now = datetime.now(timezone.utc).isoformat()
    sections = [
        f"# Three-Way Query Benchmark — ASAP / Prometheus / VictoriaMetrics\n",
        f"_Generated: {now}_\n",
        "## Backends\n",
        "- **ASAP**: ASAPQuery-backend (sketch path for W1; cold-store + Prometheus forwarding for W2)",
        "- **Prom**: Prometheus baseline (ground truth, exact)",
        "- **VM**: VictoriaMetrics (industry comparison, exact, compressed TSDB)\n",
        build_table_1(asap_promql, asap_adhoc, prom_promql, prom_adhoc, vm_promql, vm_adhoc),
        build_table_2(asap_promql, prom_promql),
        build_table_3(args.reports_dir),
        build_capability_matrix(asap_promql, asap_adhoc, prom_promql, prom_adhoc, vm_promql, vm_adhoc),
        "---",
        "_Generated by `benchmarks/scripts/compare_three_way.py`._",
    ]
    report = "\n".join(sections) + "\n"

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        f.write(report)
    print(report)
    print(f"\n[compare_three_way] wrote {args.output}")


if __name__ == "__main__":
    main()
