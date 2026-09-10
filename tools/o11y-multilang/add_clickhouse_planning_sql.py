#!/usr/bin/env python3
"""Add audited ASAPPlanner SQL rewrites without touching exact ClickHouse SQL."""

import argparse
import json
from pathlib import Path


EVAL = "{eval_ms}"


def temporal(metric: str, intent: str, window_ms: int, by: str | None = None) -> str:
    # Temporal aggregates must retain a complete series key. `metric` is
    # constant under the predicate but remains part of raw_samples identity.
    keys = f"metric, labels, {by}" if by else "metric, labels"
    inner_cols = f"metric, labels, {by}" if by else "metric, labels"
    inner = (
        f"SELECT {inner_cols}, asap_{intent}(value, ts_ms, {window_ms}) AS v "
        f"FROM raw_samples WHERE metric='{metric}' GROUP BY {keys}"
    )
    if by:
        return f"SELECT {by}, sum(v) AS value FROM ({inner}) GROUP BY {by}"
    return f"SELECT sum(v) AS value FROM ({inner})"


def ratio(left: str, right: str, intent: str, window_ms: int, by: str | None = None) -> str:
    l = temporal(left, intent, window_ms, by)
    r = temporal(right, intent, window_ms, by)
    if by:
        return (
            f"SELECT a.{by}, a.value/b.value AS value FROM ({l}) a INNER JOIN ({r}) b "
            f"ON a.{by}=b.{by}"
        )
    # Both inputs have exactly one row. A literal join key states that scalar
    # relationship explicitly and avoids pretending that per-series labels match.
    return (
        f"SELECT a.value/b.value AS value FROM (SELECT 1 AS k, value FROM ({l})) a "
        f"INNER JOIN (SELECT 1 AS k, value FROM ({r})) b ON a.k=b.k"
    )


def max_window(metric: str, window_ms: int) -> str:
    return (
        "SELECT labels, max(value) AS value FROM raw_samples "
        f"WHERE metric='{metric}' AND ts_ms>{{start_ms}} AND ts_ms<={{end_ms}} "
        "GROUP BY labels ORDER BY labels"
    )


def mappings() -> dict[str, tuple[str, str]]:
    inc6 = ratio("backend_http_5xx_total", "backend_http_requests_total", "increase", 21_600_000, "job")
    return {
        "q01": (f"SELECT * FROM ({inc6}) ORDER BY value DESC, job", "supported"),
        "q02": (f"SELECT * FROM ({inc6}) ORDER BY value DESC, job LIMIT 1", "supported"),
        "q03": (ratio("payment_service_http_5xx_total", "payment_service_http_requests_total", "increase", 3_600_000), "supported"),
        "q04": ("", "typed_unsupported: offset windows are not represented in SQL runtime bindings"),
        "q05": (max_window("cache_refresh_lag_seconds", 43_200_000), "supported"),
        "q06": (max_window("user_service_cache_refresh_lag_seconds", 43_200_000), "supported"),
        "q07": ("", "typed_unsupported: instant-vector last-sample selection is not a summary aggregate"),
        "q08": (temporal("backend_process_cpu_seconds_total", "rate", 3_600_000), "supported"),
        "q09": ("", "typed_unsupported: instant-vector last-sample selection is not a summary aggregate"),
        "q10": ("", "typed_unsupported: PromQL subquery evaluation grid is not represented by the SQL DAG"),
        "q11": (f"SELECT * FROM ({temporal('backend_process_cpu_seconds_total', 'rate', 3_600_000, 'job')}) ORDER BY value DESC, job LIMIT 2", "supported"),
        "q12": ("", "typed_unsupported: instant-vector last-sample selection is not a summary aggregate"),
        "q13": ("", "typed_unsupported: PromQL subquery evaluation grid is not represented by the SQL DAG"),
        "q14": (temporal("backend_http_requests_total", "rate", 300_000), "supported"),
        "q15": (temporal("backend_http_requests_total", "rate", 300_000, "job"), "supported"),
        "q16": (ratio("backend_http_5xx_total", "backend_http_requests_total", "increase", 3_600_000), "supported"),
        "q17": (f"SELECT * FROM ({inc6}) ORDER BY value DESC, job LIMIT 1", "supported"),
        "q18": (f"SELECT * FROM ({temporal('backend_http_5xx_total', 'increase', 86_400_000, 'job')}) WHERE value>0", "supported"),
        "q19": (temporal("order_service_http_requests_total", "rate", 300_000), "supported"),
        "q20": ("", "typed_unsupported: offset windows are not represented in SQL runtime bindings"),
        "q21": ("", "typed_unsupported: classic-histogram interpolation has no canonical SQL DAG operator"),
        "q22": (temporal("order_service_http_requests_total", "rate", 300_000), "supported"),
        "q23": (f"SELECT * FROM ({max_window('backend_retry_backlog_depth', 21_600_000)}) ORDER BY value DESC, labels LIMIT 2", "supported"),
        "q24": ("", "typed_unsupported: PromQL subquery evaluation grid is not represented by the SQL DAG"),
        "q25": ("", "typed_unsupported: vector/scalar label semantics are not represented by relational joins"),
        "q26": (f"SELECT * FROM ({temporal('backend_process_cpu_seconds_total', 'rate', 21_600_000, 'job')}) ORDER BY value DESC, job LIMIT 1", "supported"),
        "q27": ("", "typed_unsupported: PromQL subquery evaluation grid and last-sample selection are not represented"),
    }


def transform(document: dict) -> dict:
    table = mappings()
    assert {q["id"] for q in document["queries"]} == set(table)
    for query in document["queries"]:
        exact_before = query["clickhouse_sql"]
        planning_sql, status = table[query["id"]]
        query["clickhouse_planning_sql"] = planning_sql or None
        query["clickhouse_planning_status"] = status
        query["clickhouse_summary_requirements"] = requirements(query["id"])
        assert query["clickhouse_sql"] == exact_before
    return document


def requirements(query_id: str) -> list[dict]:
    specs = {
        "q01": [("backend_http_5xx_total", "increase", 21600, ["labels", "job"]), ("backend_http_requests_total", "increase", 21600, ["labels", "job"])],
        "q02": [("backend_http_5xx_total", "increase", 21600, ["labels", "job"]), ("backend_http_requests_total", "increase", 21600, ["labels", "job"])],
        "q03": [("payment_service_http_5xx_total", "increase", 3600, ["labels"]), ("payment_service_http_requests_total", "increase", 3600, ["labels"])],
        "q05": [("cache_refresh_lag_seconds", "max", 43200, ["labels"])],
        "q06": [("user_service_cache_refresh_lag_seconds", "max", 43200, ["labels"])],
        "q08": [("backend_process_cpu_seconds_total", "increase", 3600, ["labels"])],
        "q11": [("backend_process_cpu_seconds_total", "increase", 3600, ["labels", "job"])],
        "q14": [("backend_http_requests_total", "increase", 300, ["labels"])],
        "q15": [("backend_http_requests_total", "increase", 300, ["labels", "job"]),],
        "q16": [("backend_http_5xx_total", "increase", 3600, ["labels"]), ("backend_http_requests_total", "increase", 3600, ["labels"])],
        "q17": [("backend_http_5xx_total", "increase", 21600, ["labels", "job"]), ("backend_http_requests_total", "increase", 21600, ["labels", "job"])],
        "q18": [("backend_http_5xx_total", "increase", 86400, ["labels", "job"])],
        "q19": [("order_service_http_requests_total", "increase", 300, ["labels"])],
        "q22": [("order_service_http_requests_total", "increase", 300, ["labels"])],
        "q23": [("backend_retry_backlog_depth", "max", 21600, ["labels"])],
        "q26": [("backend_process_cpu_seconds_total", "increase", 21600, ["labels", "job"])],
    }
    return [
        {"metric": metric, "aggregation": aggregation, "window_seconds": window, "group_by": group_by}
        for metric, aggregation, window, group_by in specs.get(query_id, [])
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    source = json.loads(args.input.read_text())
    args.output.write_text(json.dumps(transform(source), indent=2) + "\n")


if __name__ == "__main__":
    main()
