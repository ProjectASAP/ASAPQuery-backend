#!/usr/bin/env python3
"""run_asap_workloads.py — runs both promql_suite (W1) and adhoc_suite (W2)
against the ASAP query engine, recording per-query latency samples
(median + P99 across iterations).

Mirrors the JSON shape produced by run_asap.py so compare_three_way.py and
the existing compare.py can ingest both. Writes:
    benchmarks/reports/asap_promql.json   (W1)
    benchmarks/reports/asap_adhoc.json    (W2)

Usage:
    python benchmarks/scripts/run_asap_workloads.py \
        [--asap-url URL] \
        [--iterations N] \
        [--reports-dir DIR]
"""

import argparse
import json
import os
import statistics
import time
import urllib.parse
from datetime import datetime, timezone

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
QUERIES_DIR = os.path.join(HERE, "..", "queries")
DEFAULT_REPORTS_DIR = os.path.join(HERE, "..", "reports")
DEFAULT_ITERATIONS = 3


def percentile(values: list[float], pct: float) -> float:
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


def query_once(base_url: str, expr: str, ts: float) -> tuple[dict, float]:
    encoded = urllib.parse.quote(expr, safe="")
    url = f"{base_url}/api/v1/query?query={encoded}&time={ts}"
    t0 = time.monotonic()
    resp = requests.get(url, timeout=30)
    latency_ms = (time.monotonic() - t0) * 1000.0
    resp.raise_for_status()
    return resp.json(), latency_ms


def run_suite(
    suite_path: str, base_url: str, iterations: int, label: str
) -> dict:
    with open(suite_path) as f:
        suite = json.load(f)

    results: dict[str, dict] = {}
    now = time.time()

    for q in suite["queries"]:
        qid = q["id"]
        expr = q["expr"]
        approximate = q.get("approximate", False)
        kind = q.get("kind")
        fallback = q.get("fallback")

        latencies: list[float] = []
        last_data: list = []
        last_error = None
        last_status = "success"

        print(f"[{label}] {qid}: {expr}")
        for run in range(1, iterations + 1):
            try:
                payload, lat = query_once(base_url, expr, now)
                latencies.append(lat)
                if payload.get("status") == "success":
                    last_data = payload.get("data", {}).get("result", [])
                    last_status = "success"
                    last_error = None
                else:
                    last_status = "error"
                    last_error = payload.get("error", "unknown error")
                    last_data = []
                print(f"  run {run}/{iterations}: {lat:.1f} ms  status={last_status}")
            except Exception as exc:  # noqa: BLE001
                last_status = "error"
                last_error = str(exc)
                last_data = []
                print(f"  run {run}/{iterations}: ERROR — {exc}")

        valid = [x for x in latencies if x is not None]
        results[qid] = {
            "status": last_status,
            "approximate": approximate,
            "kind": kind,
            "fallback": fallback,
            "latencies_ms": latencies,
            "median_ms": statistics.median(valid) if valid else None,
            "p99_ms": percentile(valid, 99) if valid else None,
            "data": last_data,
            "error": last_error,
        }

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "asap_url": base_url,
        "suite": os.path.basename(suite_path),
        "iterations": iterations,
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run W1 (promql) + W2 (adhoc) workloads against ASAP")
    parser.add_argument("--asap-url", default="http://localhost:8088")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    args = parser.parse_args()

    os.makedirs(args.reports_dir, exist_ok=True)

    promql_out = os.path.join(args.reports_dir, "asap_promql.json")
    adhoc_out = os.path.join(args.reports_dir, "asap_adhoc.json")

    print("=" * 70)
    print("[asap] Workload W1 — promql_suite (sketch path)")
    print("=" * 70)
    promql = run_suite(
        os.path.join(QUERIES_DIR, "promql_suite.json"),
        args.asap_url, args.iterations, "asap-w1",
    )
    with open(promql_out, "w") as f:
        json.dump(promql, f, indent=2)
    print(f"[asap] W1 saved to {promql_out}")

    print()
    print("=" * 70)
    print("[asap] Workload W2 — adhoc_suite (cold-store / Prom fallback path)")
    print("=" * 70)
    adhoc = run_suite(
        os.path.join(QUERIES_DIR, "adhoc_suite.json"),
        args.asap_url, args.iterations, "asap-w2",
    )
    with open(adhoc_out, "w") as f:
        json.dump(adhoc, f, indent=2)
    print(f"[asap] W2 saved to {adhoc_out}")


if __name__ == "__main__":
    main()
