#!/usr/bin/env python3
"""run_vm.py — runs both W1 (promql_suite.json) and W2 (adhoc_suite.json)
against VictoriaMetrics. VM exposes a Prometheus-compatible /api/v1/query
endpoint on port 8428, so the same query_once helper from run_asap_workloads
works unmodified.

Writes:
    benchmarks/reports/vm_promql.json
    benchmarks/reports/vm_adhoc.json

Usage:
    python benchmarks/scripts/run_vm.py \
        [--vm-url URL] \
        [--iterations N] \
        [--reports-dir DIR]
"""

import argparse
import json
import os

from run_asap_workloads import (  # noqa: E402
    DEFAULT_ITERATIONS,
    DEFAULT_REPORTS_DIR,
    QUERIES_DIR,
    run_suite,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run W1+W2 against VictoriaMetrics")
    parser.add_argument("--vm-url", default="http://localhost:8428")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    args = parser.parse_args()

    os.makedirs(args.reports_dir, exist_ok=True)

    for suite_name, out_name, label in [
        ("promql_suite.json", "vm_promql.json", "vm-w1"),
        ("adhoc_suite.json", "vm_adhoc.json", "vm-w3"),
    ]:
        print("=" * 70)
        print(f"[vm] {label} — {suite_name}")
        print("=" * 70)
        result = run_suite(
            os.path.join(QUERIES_DIR, suite_name),
            args.vm_url, args.iterations, label,
        )
        result["vm_url"] = result.pop("asap_url")
        out_path = os.path.join(args.reports_dir, out_name)
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"[vm] saved to {out_path}")


if __name__ == "__main__":
    main()
