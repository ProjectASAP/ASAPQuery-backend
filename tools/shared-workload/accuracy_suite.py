#!/usr/bin/env python3
"""Dataset-specific query manifests and repeated HTTP accuracy comparisons."""
import argparse
import importlib.util
import json
import math
import time
from pathlib import Path
import urllib.parse
import urllib.request

from generate import WINDOWS, queries
import resources

spec = importlib.util.spec_from_file_location(
    "accuracy_compare", Path(__file__).resolve().parents[1] / "o11y-execution/compare.py")
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)

# Trace profiles use the normalized raw_samples table, not the original CSV.
PROFILES = {
    "synthetic": ("fake_metric", "label_0", "g000000", "fake_metric_counter_total"),
    "google": ("google_cluster_cpu_rate", "service", "job-1234567890", None),
    "alibaba": ("alibaba_container_cpu_util", "machine_id", "m_1", None),
}


def corpus(dataset, filter_value=None):
    metric, group, default_filter, counter = PROFILES[dataset]
    filter_value = default_filter if filter_value is None else filter_value
    # Avoid inserting unescaped labels into PromQL or SQL templates.
    if not filter_value or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.:" for c in filter_value):
        raise ValueError("filter value must be a nonempty simple trace identifier")
    output = []
    for window in WINDOWS:
        for filtered in (False, True):
            for query in queries(window, filtered):
                if query["interval_ms"] == 1000 and window != "1m":
                    continue  # Spatial queries have no T parameter.
                is_counter = query["name"].startswith(("rate", "increase"))
                selected_metric = counter if is_counter and counter else metric
                def bind(expression):
                    if expression is None:
                        return None
                    import re
                    return re.sub(r"\bdata\b", selected_metric, expression).replace(
                        "label_0", group).replace("g000000", filter_value)
                row = {**query, "id": f"{dataset}/{window}/{filtered}/{query['name']}",
                       "dataset": dataset, "window": window,
                       "window_ms": WINDOWS[window], "metric": selected_metric,
                       "status": "not_applicable" if is_counter and counter is None else "ready",
                       "reason": "source CPU utilization is a gauge; no native cumulative counter" if is_counter and counter is None else None}
                for key in ("promql", "clickhouse_sql", "metricsql", "metricsql_prometheus_variant", "topk_input_promql"):
                    if key in row:
                        row[key] = bind(row[key])
                if "topk_group_labels" in row:
                    row["topk_group_labels"] = [group]
                output.append(row)
    return {"schema_version": 1, "dataset": dataset, "queries": output,
            "cardinalities": [10 ** i for i in range(1, 7)],
            "scrape_ms": 100 if dataset == "synthetic" else None,
            "trace_sampling": "preserve original timestamps; no implicit resampling",
            "coverage": "requested ten families plus representative count, increase and nested compositions; not exhaustive AnyAgg/binary operators"}


def request(url, query, timestamp, sql=False, timeout=60):
    if sql:
        req = urllib.request.Request(url, data=(query + " FORMAT JSONEachRow").encode())
    else:
        req = urllib.request.Request(url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode(
            {"query": query, "time": timestamp / 1000}))
    with urllib.request.urlopen(req, timeout=timeout) as response:
        headers = dict(response.headers.items())
        if sql:
            rows = [json.loads(line) for line in response if line.strip()]
            body = {"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": row["labels"], "value": [timestamp / 1000, str(row["value"])]} for row in rows]}}
        else:
            body = json.load(response)
    return body, headers


def evaluate(actual, expected, rtol, atol):
    result = comparison.compare_results(actual, expected, rtol, atol)
    _, _, samples = comparison.result_samples(expected)
    if not samples:
        result.update(equal=False, reason="empty oracle cannot establish accuracy")
    return result


def run(args):
    manifest = json.loads(args.manifest.read_text())
    loaded = json.loads(args.loaded_data.read_text())["data"]
    scale = manifest.get("scale")
    if scale:
        if (loaded.get("provenance", {}).get("scale_plan") != scale
                or loaded["samples"] != scale["total_samples"]
                or loaded["series"] != scale["total_series"]):
            raise ValueError("loaded data does not match the requested scale plan")
        args.start_ms = scale["evaluation_start_ms"] if args.start_ms is None else args.start_ms
        args.end_ms = scale["evaluation_end_ms"] if args.end_ms is None else args.end_ms
        if (args.start_ms, args.end_ms) != (scale["evaluation_start_ms"], scale["evaluation_end_ms"]):
            raise ValueError("evaluation must cover the planned repetition interval")
    if args.start_ms is None or args.end_ms is None or args.end_ms < args.start_ms:
        raise ValueError("provide valid start/end timestamps or a scale-bound query manifest")
    if loaded.get("provenance", {}).get("dataset") != manifest["dataset"]:
        raise ValueError("query and loaded dataset profiles differ")
    if args.end_ms > loaded["end_ms"] or args.start_ms < loaded["start_ms"]:
        raise ValueError("evaluation outside loaded history")
    results = []
    component_sets = json.loads(args.components.read_text()) if args.components else {}
    for components in component_sets.values():
        resources.validate(components)
    def measured(engine, url, expression, timestamp, sql=False):
        components = component_sets.get(engine, {})
        before = resources.snapshot(components)
        start = time.perf_counter_ns()
        try:
            body, headers = request(url, expression, timestamp, sql, getattr(args, "timeout_seconds", 60))
            return body, headers
        finally:
            timing[engine] = {"latency_ns": time.perf_counter_ns() - start,
                              "resources": resources.delta(before, resources.snapshot(components))}
    with args.output.open("x") as output:
        for query in manifest["queries"]:
            if args.query_name and query["name"] not in args.query_name:
                continue
            if query["status"] != "ready":
                continue
            for timestamp in range(args.start_ms, args.end_ms + 1, query["interval_ms"]):
                row = {"query_id": query["id"], "evaluation_ms": timestamp,
                       "scale": scale,
                       "dataset_sha256": loaded["sha256"], "interval_ms": query["interval_ms"],
                       "schedule": "sequential historical replay, not wall-clock load"}
                timing = {}
                try:
                    if query["interval_ms"] != 1000 and timestamp - query["window_ms"] < loaded["start_ms"]:
                        raise ValueError("full temporal window not present in loaded history")
                    sql = query["clickhouse_sql"].replace("{eval_ms}", str(timestamp)).replace("{lookback_ms}", "300000")
                    prom, _ = measured("prometheus", args.prometheus, query["promql"], timestamp)
                    exact_sql, _ = measured("clickhouse", args.clickhouse, sql, timestamp, sql=True)
                    vm, _ = measured("victoriametrics", args.victoriametrics, query["metricsql"], timestamp)
                    actual_prom, ph = measured("asap_promql", args.asap_prometheus, query["promql"], timestamp)
                    actual_sql, sh = measured("asap_sql", args.asap_clickhouse, sql, timestamp, sql=True)
                    row.update(oracle_parity=evaluate(exact_sql, prom, 1e-9, 1e-12),
                               victoriametrics=evaluate(vm, prom, 1e-9, 1e-12),
                               promql=evaluate(actual_prom, prom, args.rtol, args.atol),
                               sql=evaluate(actual_sql, exact_sql, args.rtol, args.atol),
                               responses={"prometheus": prom, "clickhouse": exact_sql,
                                          "victoriametrics": vm, "asap_promql": actual_prom, "asap_sql": actual_sql},
                               headers={"promql": ph, "sql": sh})
                    # Preserve evidence; never infer warm execution from numerical equality.
                    def route(body, headers):
                        h = {k.lower(): v for k, v in headers.items()}
                        if h.get("x-asap-execution"):
                            return h["x-asap-execution"]
                        return "warm" if "data_source: asap_query" in body.get("infos", []) else "unknown"
                    routes = [route(actual_prom, ph), route(actual_sql, sh)]
                    row["execution"] = routes
                    row["passed"] = all(row[k]["equal"] for k in ("oracle_parity", "promql", "sql"))
                    if args.require_warm:
                        row["passed"] &= all(route == "warm" for route in routes)
                except Exception as error:
                    row.update(passed=False, error=str(error))
                row["measurements"] = timing
                results.append(row["passed"])
                output.write(json.dumps(row, allow_nan=False) + "\n")
                output.flush()
    return 0 if results and all(results) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    emit = commands.add_parser("manifest")
    emit.add_argument("--dataset", choices=PROFILES, required=True)
    emit.add_argument("--filter-value")
    emit.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("run")
    check.add_argument("--manifest", type=Path, required=True)
    check.add_argument("--loaded-data", type=Path, required=True)
    check.add_argument("--output", type=Path, required=True)
    for endpoint in ("prometheus", "victoriametrics", "clickhouse", "asap-prometheus", "asap-clickhouse"):
        check.add_argument("--" + endpoint, required=True)
    check.add_argument("--start-ms", type=int)
    check.add_argument("--end-ms", type=int)
    check.add_argument("--query-name", action="append", default=[])
    check.add_argument("--rtol", type=float, default=1e-9)
    check.add_argument("--atol", type=float, default=1e-12)
    check.add_argument("--require-warm", action="store_true")
    check.add_argument("--components", type=Path)
    check.add_argument("--timeout-seconds", type=float, default=60,
                       help="same HTTP timeout for all five engines; configure server limits separately")
    args = parser.parse_args()
    if args.command == "manifest":
        with args.output.open("x") as out:
            json.dump(corpus(args.dataset, args.filter_value), out, indent=2)
        return 0
    if any(not math.isfinite(v) or v < 0 for v in (args.rtol, args.atol)):
        parser.error("tolerances must be finite and nonnegative")
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error("timeout must be finite and positive")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
