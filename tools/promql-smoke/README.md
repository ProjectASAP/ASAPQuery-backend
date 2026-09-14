# PromQL aggregation and rollup smoke tests

Small fixtures for **Prometheus 3.5.0**: **14 aggregation operators**, **25 range-vector
functions**, **105 queries**, **14 series**, **64 samples**. Every query has explicit
expected labels and values in [cases.json](cases.json), plus a note explaining the
case. `null` means a missing sample, not zero.

“Rollup” here means every registered Prometheus function accepting a
`ValueTypeMatrix` argument. This is function-name coverage using float samples,
not full PromQL conformance: native histograms, mixed sample types, staleness,
and all combinations of modifiers are not covered. Instant-vector histogram
helpers and scalar/math/label functions are outside this scope.

The catalog is extracted from the **v3.5.0** official parser registry, with source
URLs and SHA-256 hashes in [catalog.json](catalog.json). Generation fails if any
catalog entry has no case. `--verify-catalog` also downloads the pinned sources
and verifies both their hashes and their registered names against the catalog.

| Category | Functions |
| --- | --- |
| Aggregations | `sum`, `avg`, `count`, `min`, `max`, `group`, `stddev`, `stdvar`, `topk`, `bottomk`, `count_values`, `quantile` |
| Experimental aggregations | `limitk`, `limit_ratio` |
| Time-window aggregations | `avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`, `count_over_time`, `quantile_over_time`, `stddev_over_time`, `stdvar_over_time`, `last_over_time`, `present_over_time` |
| Other range-vector functions | `absent_over_time`, `changes`, `delta`, `deriv`, `idelta`, `increase`, `irate`, `predict_linear`, `rate`, `resets` |
| Experimental range-vector functions | `double_exponential_smoothing`, `mad_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time` |

The extra cases cover empty inputs, grouping, repeated values, sparse sampling,
single-sample ranges, counter resets and zero-point extrapolation, left-open
window boundaries, interpolated p99, tied timestamp extrema, and nonfinite values.
The two original gauges remain `1,2,3,4,5` and `10,20,30,40,50`.

## Run official reference tests

From the repository root, using the official **3.5.0** promtool binary:

```bash
python3 tools/promql-smoke/run.py --promtool /path/to/promtool --verify-catalog
```

This checks the binary version and runs both suites, enabling
`promql-experimental-functions` only for the experimental suite. Ordinary runs
can omit `--verify-catalog` to work offline. Outputs default to
`/tmp/asap-promql-smoke`; change that with `--output-dir`.

If using Docker, generate once and run both suites:

```bash
python3 tools/promql-smoke/run.py --verify-catalog

docker run --rm -v /tmp/asap-promql-smoke:/tests:ro --entrypoint promtool \
  prom/prometheus:v3.5.0 test rules /tests/rules.test.yml

docker run --rm -v /tmp/asap-promql-smoke:/tests:ro --entrypoint promtool \
  prom/prometheus:v3.5.0 --enable-feature=promql-experimental-functions \
  test rules /tests/experimental.test.yml
```

The Python `--promtool` runner additionally saves per-case JUnit results, logs,
and `reference-results.json`. It returns nonzero on failure. Expected finite
values use promtool's one-bit floating-point tolerance.

Promtool 3.5 does not consider NaN equal to NaN. That one reference case uses
`x != bool x` to prove the result is NaN; its original expression and raw NaN
expectations are retained for planner and HTTP tests. The override is explicit
in `cases.json`. The infinity cases compare raw values directly.

## Bind and execute through ASAPPlanner and the backend

The workspace pins the published planner commit
`f27b16a747e5d7fcd70a5510075c0cd062f0dcea`
([ASAPPlanner PR #413](https://github.com/ProjectASAP/ASAPPlanner/pull/413)).
This commit applies PR #413 on top of the backend’s existing planner pin
`029ff2fe041172c94c2d32c90b185bc83c5e8a57`, preserving its interfaces.
No adjacent planner checkout or local Cargo patch is required. The planner
preserves `irate`, series-count semantics, and special quantile parameters in
the canonical tree.

```bash
cargo +1.98.0 test --locked -p data_plane --test promql_exact_execution
cargo +1.98.0 run --locked -p control_plane --example promql_smoke -- --require-bound

# Save canonical trees, exact plans, and the original summary binder diagnostics.
cargo +1.98.0 run --locked -p control_plane --example promql_smoke -- \
  --json --require-bound > /tmp/asap-promql-smoke/planner-results.json
```

`ExactPromqlPlan::bind` calls the backend's `parse_query_expr_canonical` with
`AccuracyTarget::Exact`, then compiles that canonical tree into typed native
kernels. `BOUND_EXACT` means an executable exact plan; unsupported shapes are
`EXACT_BIND_REJECTED`. `--require-bound` fails on any rejection. Original sketch
binder diagnostics remain under `summary_status`; they do not determine exact
execution support.

The Rust execution test evaluates all 105 queries against raw timestamped float
samples and compares full labels, values, and requested ordering with the same
fixture expectations verified by official Prometheus. Additional regression tests
cover invalid snapshots, unsupported modifiers, equal-valued series counts, and
negative sampling ratios.

## Run the local native HTTP smoke server

In one terminal:

```bash
cargo +1.98.0 run --locked -p data_plane --example promql_exact_smoke
```

In another:

```bash
python3 tools/promql-smoke/run.py --backend-url http://127.0.0.1:18081
```

This example loads `cases.json` directly and serves `/api/v1/query` and
`/api/v1/query_range` using the canonical binder and backend exact executor.
It performs no Prometheus forwarding. Responses identify `data_source: asap_exact`.
Optional positional arguments are the fixture path and listening address.
This is a local test entry point; production storage and routing are not wired
into this new raw-sample execution path. Exact plans require raw samples, which
cannot in general be reconstructed from sketches.

## Compare backend HTTP results

After loading `samples.openmetrics` through the deployment's ingest path:

```bash
python3 tools/promql-smoke/run.py --backend-url http://127.0.0.1:8080
```

The runner does **not** load data or install a plan into the backend. It queries
all cases, including experimental ones, and reports unsupported queries as
failures. Fixture timestamps are in seconds, beginning at `1788825600`, sampled
every 60 seconds, and evaluated at `1788825840`. The official promtool suite uses
relative times starting at zero; `ts_of_*` expected values are shifted to epoch
time for HTTP comparisons.

The comparator checks all labels including metric names, result type, the full
series set, evaluation timestamps, finite values (`rtol=atol=1e-12`), NaN/Inf,
and explicitly requested topk/bottomk ordering. Missing series are not replaced
with zero. Full responses, including provenance annotations, are saved in
`backend-results.json`. A matching response can still be a fallback; inspect its
provenance separately. Approximate sketch answers can fail these exact checks.

## Validation

```bash
python3 -m unittest discover -s tools/promql-smoke -p 'test_*.py' -v
```

[RESULTS.md](RESULTS.md) records the observed official and planner results from
2026-09-13. It is a snapshot, not a substitute for rerunning after changes.

Official sources:
[operators](https://prometheus.io/docs/prometheus/3.5/querying/operators/),
[functions](https://prometheus.io/docs/prometheus/3.5/querying/functions/),
[function registry](https://github.com/prometheus/prometheus/blob/v3.5.0/promql/parser/functions.go),
[aggregation registry](https://github.com/prometheus/prometheus/blob/v3.5.0/promql/parser/lex.go).
