# Native/ASAP execution comparison

For developers running reproducible experiments. `experiment.py` compiles the
supplied workload through the real control plane, starts three backend processes,
verifies the installed generation is active, loads native and fallback services,
materializes summaries, and invokes all six query endpoints. Each ASAP result is
compared with its own native engine; MetricsQL is not assumed to equal PromQL.

## Run the isolated functional smoke

Build `control_plane`'s `compile_comparison_workload` example and `data_plane`.
The workspace's sibling Collector and sketchlib checkouts must match the backend
build requirements. Supply local immutable Docker image IDs or digests:

```sh
cargo build -p control_plane --example compile_comparison_workload
cargo build -p data_plane --bin data_plane
python3 tools/shared-workload/smoke.py \
  --compiler "$PWD/target/debug/examples/compile_comparison_workload" \
  --data-plane "$PWD/target/debug/data_plane" \
  --prometheus-image "$PROMETHEUS_IMAGE" \
  --clickhouse-image "$CLICKHOUSE_IMAGE" \
  --victoriametrics-image "$VICTORIAMETRICS_IMAGE" \
  --output /tmp/asap-comparison-smoke
```

Use `--docker 'sudo -n docker'` if your Docker access requires it. The smoke owns
six temporary native/fallback containers, each limited to two CPUs and 2 GiB,
and removes them on exit. ASAP processes run on the host: this smoke checks
functionality, not equal-budget performance. It uses declared planning-cost
inputs, explicitly marked as functional seeds, not empirical calibration.

Two related sum queries exercise joint SQL selection, initial and incremental
materialization, and reuse of the installed SQL plan at a later window. Success
requires correct **warm** execution for every supported request in every engine,
and correct **exact_fallback** execution for a third, uninstalled window. Expected
values are checked independently so empty native/ASAP results cannot pass.

## Run a supplied workload

`smoke.py` retains `experiment.json`, planning inputs, and the query manifest as
an editable input example. For another run, supply fresh, empty, isolated native
and fallback instances; container URLs in a completed smoke are no longer live.

```sh
python3 tools/shared-workload/experiment.py \
  --config experiment.json --output /tmp/asap-comparison-run
```

Configuration uses absolute paths for `compiler`, `data_plane`, `manifest`, each
engine's `planning_input`, and each input batch's `openmetrics` and `jsonl` files.
Prometheus and VictoriaMetrics inputs are `BackendLocalPlanningSnapshot` values;
the MetricsQL compiler entry point selects the native frontend. SQL input is a
`ClickHouseSqlAutomaticWorkload`, without forced families or materialization IDs.
Version 2 snapshot cost-evidence requirements still apply.

Each engine has distinct `native` and `fallback` endpoints with `url`, optional
local `pid`, optional `storage_path`, and optional HTTP `headers`. ClickHouse also
accepts `user` and `password`. The runner creates and drops its own fresh SQL
database on both instances; `table` and `columns_sql` describe its input table.
Do not put a database qualifier in workload SQL: the runner supplies its database.
All provided planning inputs must match the batch schema and event times.

`batches` is an ordered, non-overlapping initial batch followed by zero or more
maintenance batches. SQL `start_ms`/`end_ms` specify contiguous input ranges aligned with the installed
pane layout. SQL performs one complete backfill after loading all batches: the
current backend rejects later repeated backfills after first-seen metadata appears.
SQL source-load maintenance is measured, but continuous summary maintenance and
its CPU break-even are explicitly unavailable. OpenMetrics timestamps are
seconds. Only the final metrics batch drains and seals input. Validation should
use finite samples with strictly increasing timestamps per series.

Optional `relative_tolerance` and `absolute_tolerance` control numeric value
comparison (defaults `1e-9` and `1e-12`). They are recorded in the report; they
do not establish quantile rank-error or Top-K membership guarantees.

The query manifest has `end_ms` and nonempty `queries`; each query provides
`name`, `promql`, native `metricsql`, and `clickhouse_sql` without a FORMAT clause.
An optional `evaluation_ms` overrides the manifest time. SQL accepts `{eval_ms}`
and `{lookback_ms}` placeholders. SQL column metadata and result row multisets
are compared; ordering-sensitive workloads need a separate order contract.

For already prepared deployments, `benefits.py --endpoints ENDPOINTS.json
--manifest MANIFEST.json --output OUTPUT` runs only comparison. Its endpoints
map each engine to `native` and `asap` URLs (ClickHouse also needs `database`).
It makes no setup or maintenance measurement claim.

## Interpret the artifacts

- `planning.json`, `install.json`, and `status.json`: actual selected and active
  plans, source revisions, and selection traces.
- `requests.jsonl`: independently retained native/ASAP responses, latency,
  execution provenance, and process CPU deltas. A timeout does not skip another
  endpoint. Every engine's mismatch contributes to a failing exit status.
- `phases.json`: planning, startup/install, initial build, and incremental
  maintenance, and SQL materialization. Metrics maintenance includes ingest and materialization; SQL materialization is one separately charged finite backfill. The final drain
  is the metrics completion barrier; intermediate batches are not independent
  steady-state maintenance measurements.
- `report.json`: correctness, warm/hybrid/fallback counts, serial query latency,
  phase CPU, full observation-interval CPU, process RSS, storage when available,
  and conditional CPU break-even refresh count. Missing resource measurements
  stay null. Native startup is included only if supplied by the provisioner.
- `cleanup.json`: cleanup failures, if any.

ASAP CPU includes its fallback service and compiler. Phase sums exclude gaps;
observation-interval CPU includes idle/background work during other arms too.
RSS samples and process lifetime high-water marks are not summary-state memory.
CPU break-even assumes the same query mix and measured maintenance per dashboard refresh;
failed comparisons, missing CPU, and non-positive net savings yield no estimate.
Correct fallback can contribute to deployment-level timing, but only `warm`
counts as summary-only acceleration. These finite serial runs do not measure
sustained ingestion, concurrent throughput, or cost-model optimality.
