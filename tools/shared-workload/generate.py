#!/usr/bin/env python3
"""Generate bounded deterministic sensitivity data and a cross-engine query manifest."""
import argparse
import hashlib
import json
from pathlib import Path

WINDOWS = {"1m": 60_000, "10m": 600_000, "1h": 3_600_000,
           "6h": 21_600_000, "24h": 86_400_000}
QUANTILES = (.5, .75, .9, .95, .99)


def quantile_sql(q):
    # PromQL interpolates between adjacent sorted samples at rank q*(n-1).
    rank = f"({q} * (length(samples) - 1))"
    lower = f"samples[toUInt64(floor({rank})) + 1]"
    upper = f"samples[toUInt64(ceil({rank})) + 1]"
    return f"({lower} + ({upper} - {lower}) * ({rank} - floor({rank})))"


def counter_sql(temporal, window_ms, rate):
    samples = f"SELECT labels, arraySort(x -> x.1, groupArray((ts_ms, value))) AS samples {temporal}"
    edges = ("SELECT labels, samples, length(samples) AS n, samples[1].1 AS first_ts, "
             "samples[n].1 AS last_ts, samples[1].2 AS first_value, "
             "samples[n].2 AS last_value FROM (" + samples + ") WHERE n >= 2")
    corrected = ("SELECT *, (last_ts-first_ts)/1000. AS sampled, sampled/(n-1) AS average, "
                 "last_value-first_value + arraySum(i -> if(samples[i].2 < samples[i-1].2, "
                 "samples[i-1].2, 0.), range(2,n+1)) AS corrected, "
                 f"(first_ts - ({{eval_ms}} - {window_ms}))/1000. AS start_gap, "
                 "({eval_ms}-last_ts)/1000. AS end_gap FROM (" + edges + ")")
    extrapolated = ("SELECT labels, corrected, sampled, "
                    "least(if(start_gap >= average*1.1, average/2, start_gap), "
                    "if(corrected > 0, sampled*(first_value/corrected), inf)) AS start_extra, "
                    "if(end_gap >= average*1.1, average/2, end_gap) AS end_extra "
                    "FROM (" + corrected + ")")
    divisor = f" / {window_ms / 1000.}" if rate else ""
    return ("SELECT labels, corrected*((sampled+start_extra+end_extra)/sampled)"
            f"{divisor} AS value FROM ({extrapolated})")


def queries(window, filtered=False):
    suffix = '{label_0="g000000"}' if filtered else ""
    selector = "data" + suffix
    predicate = " AND labels['label_0'] = 'g000000'" if filtered else ""
    scan = f"FROM raw_samples WHERE metric = 'data'{predicate}"
    instant = (f"SELECT labels, argMax(value, ts_ms) AS value {scan} "
               "AND ts_ms > {eval_ms} - {lookback_ms} AND ts_ms <= {eval_ms} GROUP BY labels")
    temporal = (f"{scan} AND ts_ms > {{eval_ms}} - {WINDOWS[window]} "
                "AND ts_ms <= {eval_ms} GROUP BY labels")
    by_group = "map('label_0', labels['label_0'])"
    result = []

    def add(name, promql, sql, cadence=60_000, **extra):
        compatible = promql.replace("rate(", "rate_prometheus(").replace("increase(", "increase_prometheus(")
        if name != "spatial_topk":
            compatible = f'label_del({compatible}, "__name__")'
        result.append(dict(name=name, promql=promql, metricsql=promql,
                           metricsql_prometheus_variant=compatible,
                           clickhouse_sql=sql, interval_ms=cadence,
                           translation_status="supported" if sql else "unsupported",
                           filtered=filtered, **extra))

    for agg in ("sum", "count"):
        function = "sum(value)" if agg == "sum" else "toFloat64(count())"
        add(f"spatial_{agg}", f"{agg} by(label_0)({selector})",
            f"SELECT {by_group} AS labels, {function} AS value FROM ({instant}) GROUP BY labels", 1000)
    add("spatial_topk", f"topk by(label_0)(3, {selector})",
        f"SELECT mapUpdate(labels, map('__name__', 'data')) AS labels, value FROM ({instant}) "
        "ORDER BY value DESC LIMIT 3 BY labels['label_0']", 1000,
        tie_policy="Equal-score cutoff membership may differ; strict label comparison reports such differences.")
    for q in QUANTILES:
        grouped = (f"SELECT {by_group} AS labels, arraySort(groupArray(value)) AS samples "
                   f"FROM ({instant}) GROUP BY labels")
        add(f"spatial_quantile_{q}", f"quantile by(label_0)({q}, {selector})",
            f"SELECT labels, {quantile_sql(q)} AS value FROM ({grouped})", 1000)
    grouped_sum = f"SELECT {by_group} AS labels, sum(value) AS value FROM ({instant}) GROUP BY labels"
    add("nested_spatial_sum", f"sum(sum by(label_0)({selector}))",
        f"SELECT map() AS labels, sum(value) AS value FROM ({grouped_sum}) HAVING count() > 0", 1000)
    add("nested_spatial_topk", f"topk(3, sum by(label_0)({selector}))",
        f"SELECT labels, value FROM ({grouped_sum}) ORDER BY value DESC LIMIT 3", 1000)
    for agg in ("sum", "count"):
        function = "sum(value)" if agg == "sum" else "toFloat64(count())"
        sql = f"SELECT labels, {function} AS value {temporal}"
        prom = f"{agg}_over_time({selector}[{window}])"
        add(f"temporal_{agg}", prom, sql)
        add(f"temporal_{agg}_spatial_sum", f"sum by(label_0)({prom})",
            f"SELECT {by_group} AS labels, sum(value) AS value FROM ({sql}) GROUP BY labels")
        add(f"temporal_{agg}_spatial_topk", f"topk by(label_0)(3, {prom})",
            f"SELECT labels, value FROM ({sql}) ORDER BY value DESC LIMIT 3 BY labels['label_0']",
            topk_input_promql=prom, topk_group_labels=["label_0"], topk_k=3)
    quantiles = {}
    for q in QUANTILES:
        sql = (f"SELECT labels, {quantile_sql(q)} AS value FROM "
               f"(SELECT labels, arraySort(groupArray(value)) AS samples {temporal})")
        quantiles[q] = sql
        add(f"temporal_quantile_{q}", f"quantile_over_time({q}, {selector}[{window}])", sql)
    add("quantile_ratio", f"quantile_over_time(0.9, {selector}[{window}]) / "
        f"quantile_over_time(0.5, {selector}[{window}])",
        f"SELECT a.labels AS labels, a.value / b.value AS value FROM ({quantiles[.9]}) a "
        f"INNER JOIN ({quantiles[.5]}) b ON a.labels = b.labels")
    for agg in ("rate", "increase"):
        prom = f"{agg}({selector}[{window}])"
        sql = counter_sql(temporal, WINDOWS[window], agg == "rate")
        add(agg, prom, sql)
        add(f"{agg}_spatial_sum", f"sum by(label_0)({prom})",
            f"SELECT {by_group} AS labels, sum(value) AS value FROM ({sql}) GROUP BY labels")
        add(f"{agg}_spatial_topk", f"topk by(label_0)(3, {prom})",
            f"SELECT labels, value FROM ({sql}) ORDER BY value DESC LIMIT 3 BY labels['label_0']")
    return result


def manifest(groups, members, duration_ms, scrape_ms):
    return dict(schema_version=1, purpose="synthetic sensitivity; separate from real o11y coverage",
                cardinality_definition="label_0 groups; total series = groups * members",
                groups=groups, members_per_group=members, total_series=groups * members,
                duration_ms=duration_ms, scrape_ms=scrape_ms,
                total_samples=(duration_ms // scrape_ms + 1) * groups * members,
                semantics=dict(temporal_bounds="(eval_ms - window_ms, eval_ms]",
                               instant_bounds="(eval_ms - lookback_ms, eval_ms] latest per series (Prometheus 3.14)",
                               values="finite nonnegative counter with deterministic resets",
                               excluded="NaN, Inf, stale markers, duplicate timestamps",
                               quantile="linear interpolation at q*(n-1)",
                               equality="labels exact; numeric tolerance must be reported; not bit-strict",
                               lookback_ms=300_000),
                queries=[dict(window=window, window_ms=ms, **query)
                         for window, ms in WINDOWS.items() for filtered in (False, True)
                         for query in queries(window, filtered)])


def write_data(directory, groups, members, duration_ms, scrape_ms, start_ms, reset_steps):
    """Stream in timestamp order; memory does not grow with the requested dataset."""
    paths = [directory / "samples.prom", directory / "samples.jsonl", directory / "samples.openmetrics"]
    hashes = [hashlib.sha256() for _ in paths]
    with paths[0].open("xb") as prom, paths[1].open("xb") as sql, paths[2].open("xb") as om:
        for step in range(duration_ms // scrape_ms + 1):
            timestamp = start_ms + step * scrape_ms
            for group in range(groups):
                for member in range(members):
                    labels = {"label_0": f"g{group:06d}", "member": f"m{member:04d}"}
                    # Different slopes and reset phases make grouped TopK nondegenerate.
                    phase = (step + member * 17 + group * 13) % reset_steps
                    value = phase * (member + 1) + group % 97
                    label_text = ",".join(f'{key}="{value}"' for key, value in labels.items())
                    lines = [f"data{{{label_text}}} {value} {timestamp}\n".encode(),
                             (json.dumps(dict(metric="data", labels=labels, ts_ms=timestamp,
                                              value=value), separators=(",", ":")) + "\n").encode(),
                             f"data{{{label_text}}} {value} {timestamp // 1000}.{timestamp % 1000:03d}\n".encode()]
                    for handle, digest, line in zip((prom, sql, om), hashes, lines):
                        handle.write(line)
                        digest.update(line)
        om.write(b"# EOF\n")
        hashes[2].update(b"# EOF\n")
    return [dict(path=str(path.resolve()), bytes=path.stat().st_size, sha256=digest.hexdigest())
            for path, digest in zip(paths, hashes)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--groups", type=int, default=10)
    parser.add_argument("--members", type=int, default=4)
    parser.add_argument("--duration-ms", type=int, default=60_000)
    parser.add_argument("--scrape-ms", type=int, default=100)
    parser.add_argument("--start-ms", type=int, default=1_700_000_000_000)
    parser.add_argument("--max-samples", type=int, default=1_000_000)
    parser.add_argument("--reset-steps", type=int, default=100_003)
    parser.add_argument("--data", action="store_true", help="Actually stream data; default writes manifests only")
    args = parser.parse_args()
    if min(args.groups, args.members, args.scrape_ms, args.reset_steps) <= 0 or args.duration_ms < 0:
        parser.error("groups, members, scrape must be positive; duration must be nonnegative")
    if args.duration_ms % args.scrape_ms:
        parser.error("duration must be an exact multiple of scrape_ms")
    selected = manifest(args.groups, args.members, args.duration_ms, args.scrape_ms)
    selected.update(start_ms=args.start_ms, end_ms=args.start_ms + args.duration_ms,
                    reset_steps=args.reset_steps)
    if args.data and selected["total_samples"] > args.max_samples:
        parser.error(f"{selected['total_samples']} samples exceed explicit --max-samples budget")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.data:
        selected["artifacts"] = write_data(args.output, args.groups, args.members,
                                           args.duration_ms, args.scrape_ms, args.start_ms, args.reset_steps)
    grid = [dict(groups=10 ** exponent, members=args.members, duration=window,
                 total_series=10 ** exponent * args.members,
                 total_samples=(ms // args.scrape_ms + 1) * 10 ** exponent * args.members)
            for exponent in range(1, 7) for window, ms in WINDOWS.items()]
    with (args.output / "manifest.json").open("x") as handle:
        json.dump(selected, handle, indent=2)
    with (args.output / "grid.json").open("x") as handle:
        json.dump(grid, handle, indent=2)


if __name__ == "__main__":
    main()
