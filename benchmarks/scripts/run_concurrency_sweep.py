#!/usr/bin/env python3
"""run_concurrency_sweep.py — throughput-vs-concurrency sweep.

For each backend (asap/prom/vm) and each concurrency level C in
{1, 4, 16, 64}, hammer the endpoint with C parallel worker threads,
each issuing the promql_suite in a tight loop for `--duration` seconds.
Records total queries served / wallclock = throughput, plus P50/P99
per-query latency.

Output:
    benchmarks/reports/concurrency_sweep.csv
columns: backend, concurrency, total_queries, throughput_qps, p50_ms, p99_ms

Usage:
    python benchmarks/scripts/run_concurrency_sweep.py \
        [--asap-url URL] \
        [--prom-url URL] \
        [--vm-url URL] \
        [--duration 60] \
        [--concurrency 1,4,16,64] \
        [--backends asap,prom,vm] \
        [--output FILE]
"""

import argparse
import csv
import json
import os
import threading
import time
import urllib.parse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
QUERIES_DIR = os.path.join(HERE, "..", "queries")
DEFAULT_OUTPUT = os.path.join(HERE, "..", "reports", "concurrency_sweep.csv")
DEFAULT_DURATION = 60
DEFAULT_CONCURRENCIES = [1, 4, 16, 64]


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


def hammer(url_base: str, queries: list[str], deadline: float, latencies: deque, lock: threading.Lock) -> int:
    """One worker thread: round-robin through queries until deadline."""
    session = requests.Session()
    count = 0
    i = 0
    n = len(queries)
    while time.monotonic() < deadline:
        expr = queries[i % n]
        i += 1
        encoded = urllib.parse.quote(expr, safe="")
        url = f"{url_base}/api/v1/query?query={encoded}"
        t0 = time.monotonic()
        try:
            r = session.get(url, timeout=30)
            lat = (time.monotonic() - t0) * 1000.0
            if r.status_code == 200:
                with lock:
                    latencies.append(lat)
                count += 1
        except Exception:  # noqa: BLE001
            pass
    return count


def sweep_one(backend: str, url: str, queries: list[str], concurrencies: list[int], duration: int) -> list[dict]:
    rows = []
    for c in concurrencies:
        print(f"[sweep] backend={backend} concurrency={c} duration={duration}s")
        latencies: deque[float] = deque()
        lock = threading.Lock()
        deadline = time.monotonic() + duration
        wallclock_start = time.monotonic()
        with ThreadPoolExecutor(max_workers=c) as pool:
            futures = [pool.submit(hammer, url, queries, deadline, latencies, lock) for _ in range(c)]
            total = sum(f.result() for f in as_completed(futures))
        wallclock = time.monotonic() - wallclock_start
        lat_list = list(latencies)
        qps = total / wallclock if wallclock > 0 else 0.0
        p50 = percentile(lat_list, 50)
        p99 = percentile(lat_list, 99)
        print(f"  -> total={total} qps={qps:.1f} p50={p50:.1f}ms p99={p99:.1f}ms")
        rows.append({
            "backend": backend,
            "concurrency": c,
            "total_queries": total,
            "throughput_qps": round(qps, 2),
            "p50_ms": round(p50, 2) if p50 == p50 else "",
            "p99_ms": round(p99, 2) if p99 == p99 else "",
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Throughput-vs-concurrency sweep across all 3 backends")
    parser.add_argument("--asap-url", default="http://localhost:8088")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--vm-url", default="http://localhost:8428")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION)
    parser.add_argument(
        "--concurrency",
        default=",".join(str(c) for c in DEFAULT_CONCURRENCIES),
        help="Comma-separated concurrency levels",
    )
    parser.add_argument(
        "--backends",
        default="asap,prom,vm",
        help="Comma-separated backends to sweep",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    concurrencies = [int(x) for x in args.concurrency.split(",") if x.strip()]
    backends = [x.strip() for x in args.backends.split(",") if x.strip()]

    with open(os.path.join(QUERIES_DIR, "promql_suite.json")) as f:
        suite = json.load(f)
    queries = [q["expr"] for q in suite["queries"]]

    backend_urls = {
        "asap": args.asap_url,
        "prom": args.prom_url,
        "vm": args.vm_url,
    }

    all_rows = []
    for backend in backends:
        if backend not in backend_urls:
            print(f"[sweep] skipping unknown backend: {backend}")
            continue
        all_rows.extend(sweep_one(backend, backend_urls[backend], queries, concurrencies, args.duration))

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["backend", "concurrency", "total_queries", "throughput_qps", "p50_ms", "p99_ms"],
        )
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"\n[sweep] wrote {len(all_rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
