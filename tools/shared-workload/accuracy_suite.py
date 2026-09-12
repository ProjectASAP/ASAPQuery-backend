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
import urllib.error

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


def request(url, query, timestamp, sql=False, timeout=None):
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


def execution(body, headers):
    h = {k.lower(): v for k, v in headers.items()}
    if h.get("x-asap-execution"):
        return h["x-asap-execution"]
    return "warm" if "data_source: asap_query" in body.get("infos", []) else "unknown"


def compare_pair(endpoints, backend, baseline, rtol, atol):
    actual, exact = endpoints.get(backend, {}), endpoints.get(baseline, {})
    result = {"backend": backend, "baseline": baseline,
              "execution": actual.get("execution", "unavailable"),
              "eligible_for_query_comparison": False,
              "eligible_for_benefit_conclusion": False,
              "full_cost": None,
              "cost_reason": "replay has no isolated complete-lifecycle cost evidence"}
    if not actual.get("success") or not exact.get("success"):
        result["correctness"] = {"equal": False, "comparable": False,
                                 "reason": "paired endpoint missing or failed"}
        return result
    try:
        result["correctness"] = evaluate(actual["response"], exact["response"], rtol, atol)
    except (ValueError, KeyError, TypeError) as error:
        result["correctness"] = {"equal": False, "comparable": False, "reason": str(error)}
    result["eligible_for_query_comparison"] = (result["correctness"]["equal"] and result["execution"] == "warm")
    result["latency_ns"] = {"backend": actual["latency_ns"], "baseline": exact["latency_ns"]}
    result["query_latency_ratio"] = (exact["latency_ns"] / actual["latency_ns"]
                                      if result["eligible_for_query_comparison"] and actual["latency_ns"] else None)
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
        record = {"success": False}
        try:
            # No client deadline: wait for the service to return, including slow natives.
            body, headers = request(url, expression, timestamp, sql, timeout=None)
            record.update(success=body.get("status") == "success", response=body, headers=headers,
                          execution=execution(body, headers) if engine.startswith("asap_") else "native")
            if not record["success"]:
                record["error"] = {"kind": "api_error", "message": str(body.get("error", body))}
        except Exception as error:
            record["error"] = {"kind": "timeout" if isinstance(error, TimeoutError) else type(error).__name__,
                               "message": str(error)}
            if isinstance(error, urllib.error.HTTPError):
                record["error"].update(http_status=error.code, body=error.read().decode(errors="replace"))
                record["headers"] = dict(error.headers.items())
        finally:
            record["latency_ns"] = time.perf_counter_ns() - start
            record["resources"] = resources.delta(before, resources.snapshot(components))
        return record
    with args.output.open("x") as output, args.output.with_name(args.output.name + ".endpoints.jsonl").open("x") as journal:
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
                row["endpoints"] = {}
                try:
                    if query["interval_ms"] != 1000 and timestamp - query["window_ms"] < loaded["start_ms"]:
                        raise ValueError("full temporal window not present in loaded history")
                    sql = query["clickhouse_sql"].replace("{eval_ms}", str(timestamp)).replace("{lookback_ms}", "300000")
                    jobs = [("asap_promql", args.asap_prometheus, query["promql"], False),
                            ("asap_sql", args.asap_clickhouse, sql, True)]
                    if getattr(args, "asap_metricsql", None):
                        jobs.append(("asap_metricsql", args.asap_metricsql, query["metricsql"], False))
                    jobs += [("prometheus", args.prometheus, query["promql"], False),
                             ("clickhouse", args.clickhouse, sql, True),
                             ("victoriametrics", args.victoriametrics, query["metricsql"], False)]
                    row["request_order"] = [job[0] for job in jobs]
                    for engine, url, expression, is_sql in jobs:
                        record = measured(engine, url, expression, timestamp, is_sql)
                        row["endpoints"][engine] = record
                        # Persist each completed request even if the next service never returns.
                        journal.write(json.dumps({"query_id": query["id"], "evaluation_ms": timestamp,
                                                  "engine": engine, **record}, allow_nan=False) + "\n")
                        journal.flush()
                    ep = row["endpoints"]
                    row["pairs"] = {name: compare_pair(ep, backend, baseline, args.rtol, args.atol)
                                    for name, backend, baseline in (
                                        ("promql", "asap_promql", "prometheus"),
                                        ("sql", "asap_sql", "clickhouse"),
                                        ("metricsql", "asap_metricsql", "victoriametrics"))}
                    row["oracle_parity"] = compare_pair(ep, "clickhouse", "prometheus", 1e-9, 1e-12)["correctness"]
                    row["victoriametrics"] = compare_pair(ep, "victoriametrics", "prometheus", 1e-9, 1e-12)["correctness"]
                    row["sql"], row["promql"] = [row["pairs"][name]["correctness"] for name in ("sql", "promql")]
                    if not row["oracle_parity"]["equal"]:
                        row["pairs"]["sql"].update(eligible_for_query_comparison=False, query_latency_ratio=None,
                                                    oracle_reason="ClickHouse/Prometheus translation parity unavailable or failed")
                    row["execution"] = [ep[name].get("execution", "failed") for name in ("asap_promql", "asap_sql")]
                    required = getattr(args, "required_pairs", None) or ["promql", "sql", "metricsql"]
                    row["acceptance_scope"] = {"required_pairs": required, "full_benefit_acceptance": False}
                    row["passed"] = all(row["pairs"][name]["correctness"]["equal"] and
                                        (not args.require_warm or row["pairs"][name]["eligible_for_query_comparison"])
                                        for name in required)
                    if "sql" in required:
                        row["passed"] &= row["oracle_parity"]["equal"]
                except Exception as error:
                    row.update(passed=False, error=str(error))
                row["responses"] = {k: v["response"] for k, v in row["endpoints"].items() if "response" in v}
                row["headers"] = {k: v.get("headers", {}) for k, v in row["endpoints"].items()}
                row["measurements"] = {k: {"latency_ns": v["latency_ns"], "resources": v["resources"]}
                                       for k, v in row["endpoints"].items()}
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
    check.add_argument("--asap-metricsql", help="ASAP MetricsQL service paired with native VM; absent means unvalidated VM pair")
    check.add_argument("--required-pair", dest="required_pairs", choices=("promql", "sql", "metricsql"), action="append",
                       help="explicit partial acceptance scope; default requires all three pairs")
    check.add_argument("--start-ms", type=int)
    check.add_argument("--end-ms", type=int)
    check.add_argument("--query-name", action="append", default=[])
    check.add_argument("--rtol", type=float, default=1e-9)
    check.add_argument("--atol", type=float, default=1e-12)
    check.add_argument("--require-warm", action="store_true")
    check.add_argument("--components", type=Path)
    args = parser.parse_args()
    if args.command == "manifest":
        with args.output.open("x") as out:
            json.dump(corpus(args.dataset, args.filter_value), out, indent=2)
        return 0
    if any(not math.isfinite(v) or v < 0 for v in (args.rtol, args.atol)):
        parser.error("tolerances must be finite and nonnegative")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
