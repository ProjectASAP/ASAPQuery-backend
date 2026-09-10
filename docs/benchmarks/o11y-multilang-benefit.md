# O11y multi-language benefit experiment

This benchmark keeps all 27 source occurrences in the denominator. `tools/o11y-multilang/corpus.json` preserves each PromQL expression byte-for-byte as MetricsQL and assigns the matching ClickHouse SQL lookup. The SQL mapping reads a table produced from the same raw samples by a Prometheus-semantics loader; it must never ingest Prometheus/VM query answers. This indirection is required because native ClickHouse aggregates do not by themselves reproduce Prometheus counter reset, boundary extrapolation, staleness, subquery-grid, or histogram rules.

## Gate order

1. Validate corpus count, IDs, source hash, metric schema, evaluation timestamps, and raw-data hash.
2. Run both official ASAPPlanner frontends. Record parser, canonicalization, planner, compiler, publication validator, executor, adapter, and fallback independently for every occurrence. Unsupported rows remain failures/fallbacks in the denominator.
3. Start fresh Prometheus, VictoriaMetrics, and ClickHouse stores. Load identical raw samples. Build `o11y_prometheus_semantic_eval` from raw samples and verify it against Prometheus before timing.
4. Run fresh baseline and ASAP trials with alternating request order, fixed CPU affinity, separate storage paths, finite-ingest drain, and identical evaluation timestamps.
5. Compare full label sets, timestamps, values, warnings, response types, execution provenance, latency, CPU, RSS/HWM, storage bytes, ingest cost, and plan lifecycle cost.

A run is invalid if the semantic SQL loader is absent, its oracle comparison fails, any service shares a mutable storage directory, an unsupported row disappears from results, or an ASAP success lacks typed execution provenance.

## Prepared commands

```bash
python3 tools/o11y-multilang/audit_coverage.py \
  --corpus tools/o11y-multilang/corpus.json \
  --output artifacts/o11y-multilang/coverage.json \
  --dry-run-command 'cargo test -p control_plane metricsql_compilation_publishes_an_independent_sidecar_entry' \
  --dry-run-command 'cargo test -p control_plane clickhouse'
```

The checked-in fresh-process runner provisions isolated stores, reloads the raw
fixture, alternates requests, and records process CPU ticks and RSS around the
query interval.  The observed three-trial result is in
`tools/o11y-multilang/final-fresh-trials`: all five engines completed 405/405
requests.  Both ASAP language listeners used exact fallback for 405/405; the
27-query corpus therefore demonstrates fallback overhead, not acceleration.
Median latency was 2.39 ms for VM and 5.04 ms through the MetricsQL fallback,
and 56.72 ms for ClickHouse and 59.19 ms through the SQL fallback.

The SQL oracle is valid for 27/27 rows.  The MetricsQL frontend accepts 12/27,
the early planner accepts 7/27, and the production compiler and publication
validator accept q03 and q16 (2/27).  Requests using each published sidecar
still routed to exact fallback with an empty SummaryStore.  These failures stay
in the denominator.  Direct VM differs strictly from Prometheus for nine query
IDs; three are label-name retention and six are range/increase/subquery numeric
semantics, so no cross-language semantic claim is made for those rows.

The independent synthetic supported-SQL evidence runs the real ClickHouse
reader, BackfillService, SummaryStore, and a published shared DAG containing
Filter, Project, global Sort, and Limit.  It passed differential correctness in
three fresh ClickHouse trials.  It executes one query per trial through the
accelerator object, so it is lifecycle correctness evidence and is not reported
as a latency benefit estimate.

Per-query rows use this schema:

```json
{"id":"q01","language":"metricsql|clickhouse_sql","mode":"baseline|asap","trial":1,"repetition":0,"stage":"parser|canonical|planner|publication|validator|executor|adapter|fallback","status":"warm|exact_fallback|failed","typed_reason":null,"http_status":200,"latency_ns":0,"cpu_ns":0,"response_sha256":"...","comparable":true,"mismatch":null}
```
