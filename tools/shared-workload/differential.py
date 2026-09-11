#!/usr/bin/env python3
"""Compare generated query values on already loaded isolated fixture engines."""
import argparse
import base64
import json
import math
import os
import urllib.parse
import urllib.request
from pathlib import Path


def get_json(url, params):
    with urllib.request.urlopen(url + "/api/v1/query?" + urllib.parse.urlencode(params)) as response:
        result = json.load(response)
    if result["status"] != "success":
        raise ValueError(result)
    return unique_vector((item["metric"], item["value"][1]) for item in result["data"]["result"])


def unique_vector(rows):
    vector = {}
    for labels, value in rows:
        key = tuple(sorted(labels.items()))
        if key in vector:
            raise ValueError(f"duplicate output label set: {dict(key)}")
        vector[key] = float(value)
    return vector


def failure(error):
    return dict(error=error.read().decode() if hasattr(error, "read") else str(error))


def compare(expected, actual):
    if expected.keys() != actual.keys():
        return dict(equal=False, reason="label sets differ", expected_count=len(expected), actual_count=len(actual))
    differences = [dict(labels=dict(key), expected=expected[key], actual=actual[key])
                   for key in expected if not math.isclose(expected[key], actual[key], rel_tol=1e-9, abs_tol=1e-12)]
    return dict(equal=not differences, differences=differences[:5])


def compare_topk(population, actual, group_labels, k):
    groups = {}
    selected = {}
    for labels, value in population.items():
        group = tuple((name, dict(labels).get(name)) for name in group_labels)
        groups.setdefault(group, {})[labels] = value
    for labels, value in actual.items():
        if labels not in population or not math.isclose(value, population[labels], rel_tol=1e-9, abs_tol=1e-12):
            return dict(equal=False, reason="TopK returned an unknown series or incorrect score")
        group = tuple((name, dict(labels).get(name)) for name in group_labels)
        selected.setdefault(group, {})[labels] = value
    for group, candidates in groups.items():
        output = selected.get(group, {})
        if len(output) != min(k, len(candidates)):
            return dict(equal=False, reason="TopK group cardinality differs")
        cutoff = sorted(candidates.values(), reverse=True)[min(k, len(candidates)) - 1]
        if any(value < cutoff for value in output.values()) or any(
                value > cutoff and labels not in output for labels, value in candidates.items()):
            return dict(equal=False, reason="TopK violated the cutoff ordering")
    return dict(equal=True, equivalence="TopK cutoff admissibility; ties may select different labels")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--prometheus", required=True)
    parser.add_argument("--victoriametrics", required=True)
    parser.add_argument("--clickhouse", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--loaded-data-manifest", type=Path, action="append", default=[])
    parser.add_argument("--evaluation-ms", type=int)
    parser.add_argument("--query-name", action="append", default=[])
    args = parser.parse_args()
    if not args.database.replace("_", "").isalnum():
        parser.error("database must be an identifier")
    manifest = json.loads(args.manifest.read_text())
    evaluation_ms = args.evaluation_ms if args.evaluation_ms is not None else manifest["end_ms"]
    records = []
    for query in manifest["queries"]:
        if args.query_name and query["name"] not in args.query_name:
            continue
        record = dict(name=query["name"], window=query["window"], filtered=query["filtered"])
        params = dict(query=query["promql"], time=evaluation_ms / 1000)
        try:
            baseline = get_json(args.prometheus, params)
            population = get_json(args.prometheus, dict(params, query=query["topk_input_promql"])) if "topk_input_promql" in query else None
            def evaluate(actual):
                return compare_topk(population, actual, query["topk_group_labels"], query["topk_k"]) if population is not None else compare(baseline, actual)
            record["prometheus_rows"] = len(baseline)
            for route, expression in (("victoriametrics", query["metricsql"]),
                                      ("victoriametrics_prometheus_variant", query["metricsql_prometheus_variant"])):
                try:
                    record[route] = evaluate(get_json(
                        args.victoriametrics, dict(params, query=expression)))
                except Exception as error:
                    record[route] = failure(error)
            if query["clickhouse_sql"]:
                sql = (query["clickhouse_sql"].replace("{eval_ms}", str(evaluation_ms))
                       .replace("{lookback_ms}", str(manifest["semantics"]["lookback_ms"]))
                       + " FORMAT JSONEachRow")
                headers = {}
                user = os.environ.get("CLICKHOUSE_USER")
                if user:
                    credential = user + ":" + os.environ.get("CLICKHOUSE_PASSWORD", "")
                    headers["Authorization"] = "Basic " + base64.b64encode(credential.encode()).decode()
                request = urllib.request.Request(args.clickhouse + "/?" + urllib.parse.urlencode(
                    dict(database=args.database)), data=sql.encode(), headers=headers)
                with urllib.request.urlopen(request) as response:
                    rows = [json.loads(line) for line in response if line.strip()]
                actual = unique_vector((row["labels"], row["value"]) for row in rows)
                record["clickhouse"] = evaluate(actual)
            else:
                record["clickhouse"] = dict(status="unsupported")
        except Exception as error:
            record["error"] = error.read().decode() if hasattr(error, "read") else str(error)
        records.append(record)
    args.output.write_text(json.dumps(dict(manifest=str(args.manifest.resolve()),
                                          evaluation_ms=evaluation_ms,
                                          loaded_data_manifests=[str(path.resolve()) for path in args.loaded_data_manifest],
                                          numeric_tolerance=dict(relative=1e-9, absolute=1e-12),
                                          results=records), indent=2))
    print(json.dumps({engine: sum(record.get(engine, {}).get("equal", False) for record in records)
                      for engine in ("clickhouse", "victoriametrics", "victoriametrics_prometheus_variant")}))


if __name__ == "__main__":
    main()
