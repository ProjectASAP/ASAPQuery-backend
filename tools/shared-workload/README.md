# Shared aggregation sensitivity workload

For dataset-specific fake-client, Google and Alibaba expressions, matched HTTP
accuracy/latency replay and component/system resource accounting, see
[Accuracy E2E](ACCURACY_E2E.md).

This developer evaluation tool emits identical timestamped samples for Prometheus,
VictoriaMetrics, and ClickHouse, with a query manifest. This synthetic sensitivity
study supplements the original o11y workload; it does not replace its coverage or
demonstrate an ASAP benefit by itself.

`label_0` cardinality means groups, with four distinct `member` series per group by
default. A million groups therefore means four million series. Samples arrive
every 100 ms. `grid.json` lists all requested group counts (10 through 1,000,000)
and durations (1m, 10m, 1h, 6h, 24h), including their actual sample counts. The largest
default cell has 3,456,004,000,000 samples. Generation is opt-in and has an explicit
sample budget; writing a manifest does not run this entire grid.

```sh
python3 tools/shared-workload/generate.py \
  --output /mydata/shared-workload-study/example \
  --groups 10 --members 4 --duration-ms 60000 --scrape-ms 100 --data
```

The output contains:

- `manifest.json`: query expressions, cadence, bounds, dataset size, and hashes.
- `grid.json`: proposed scale matrix; these are planned scales, not completed runs.
- `samples.prom`: millisecond timestamps for VictoriaMetrics text import.
- `samples.openmetrics`: second timestamps for `promtool tsdb create-blocks-from openmetrics`.
- `samples.jsonl`: ClickHouse JSONEachRow input for the schema below.

```sql
CREATE TABLE raw_samples (
    metric String, labels Map(String, String), ts_ms Int64, value Float64
) ENGINE = MergeTree ORDER BY (metric, ts_ms);
```

Generation streams in timestamp order and does not retain the dataset in memory.
Existing output files are never overwritten. `--reset-steps 97` exercises repeated
counter resets in a one-minute fixture. Different member slopes and reset phases
make grouped quantile/TopK nondegenerate. The generator uses finite, nonnegative
values; it does not cover NaN, infinity, stale markers, or duplicate timestamps.

Instant selectors choose the latest sample per series within the left-exclusive
lookback interval. Temporal windows use `(evaluation - window, evaluation]`.
Spatial count counts series; temporal count counts samples, not distinct values.
Quantiles use linear interpolation. Rate/increase SQL corrects counter resets,
estimates zero crossings, and applies boundary extrapolation; it is not a naive
last-minus-first calculation. Query cadence is 1s for spatial queries and 1m for
temporal queries. The differential command checks a single evaluation timestamp;
it does not execute the cadence schedule or measure performance.

Prometheus versions matter: 2.55.1 includes the left range boundary on this fixture;
3.14.0 implements the exclusive boundary used here. VictoriaMetrics also has
different native counter and metric-name semantics. The manifest keeps native
`metricsql` and a separately labeled `metricsql_prometheus_variant` using
`rate_prometheus`/`increase_prometheus` and explicit name-label removal. This variant
is an experiment, not a guarantee of parity. See the
[VictoriaMetrics function reference](https://docs.victoriametrics.com/victoriametrics/metricsql/).

Load the same files into isolated databases before comparing:

```sh
python3 tools/shared-workload/differential.py \
  --manifest /mydata/shared-workload-study/example/manifest.json \
  --loaded-data-manifest /mydata/shared-workload-study/example/manifest.json \
  --output /mydata/shared-workload-study/example/differential.json \
  --prometheus http://localhost:29090 --victoriametrics http://localhost:28428 \
  --clickhouse http://localhost:18123 --database isolated_fixture
```

Supply ClickHouse credentials through `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`.
`--evaluation-ms` tests gaps and lookback boundaries; repeat `--query-name` to limit
the probe. Repeat `--loaded-data-manifest` when the loaded history combines files.
The report preserves each engine's differences and rejects duplicate output label
sets. Labels are compared exactly, including metric names; numeric tolerance is
1e-9 relative and 1e-12 absolute. Temporal sum/count followed by TopK validates
cutoff admissibility against the full population; other TopK ties use strict
comparison and can report valid cutoff membership differences. The report is not a bit-strict
correctness claim, and its exit status does not imply all comparisons matched.

The generated corpus covers the explicitly requested shapes and representative
nested spatial and temporal-to-spatial compositions. It does not enumerate every
possible `AnyAgg op AnyAgg`, MetricsQL extension, filter operator, or subquery.
Larger-window queries against short fixtures have partial history; do not report
them as full-window performance experiments. Planner candidates, installed catalog
IDs, warm/hybrid/fallback provenance, and resource measurements must be recorded by
the backend experiment driver when consuming this manifest.
