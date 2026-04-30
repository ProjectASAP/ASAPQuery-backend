#!/usr/bin/env python3
"""run_prom.py — runs both W1 (promql_suite.json) and W2 (adhoc_suite.json)
against a Prometheus baseline. Output JSON shape matches run_asap_workloads.py
so compare_three_way.py can ingest it.

Writes:
    benchmarks/reports/prom_promql.json
    benchmarks/reports/prom_adhoc.json

Usage:
    python benchmarks/scripts/run_prom.py \
        [--prometheus-url URL] \
        [--iterations N] \
        [--reports-dir DIR]
"""

import argparse
import os

from run_asap_workloads import (  # noqa: E402  -- intentional sibling-import
    DEFAULT_ITERATIONS,
    DEFAULT_REPORTS_DIR,
    QUERIES_DIR,
    run_suite,
)
import json


def main() -> None:
    parser = argparse.ArgumentParser(description="Run W1+W2 against Prometheus baseline")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    args = parser.parse_args()

    os.makedirs(args.reports_dir, exist_ok=True)

    for suite_name, out_name, label in [
        ("promql_suite.json", "prom_promql.json", "prom-w1"),
        ("adhoc_suite.json", "prom_adhoc.json", "prom-w3"),
    ]:
        print("=" * 70)
        print(f"[prom] {label} — {suite_name}")
        print("=" * 70)
        result = run_suite(
            os.path.join(QUERIES_DIR, suite_name),
            args.prometheus_url, args.iterations, label,
        )
        # Tag the URL field for downstream tools that grep on it.
        result["prometheus_url"] = result.pop("asap_url")
        out_path = os.path.join(args.reports_dir, out_name)
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"[prom] saved to {out_path}")


if __name__ == "__main__":
    main()
