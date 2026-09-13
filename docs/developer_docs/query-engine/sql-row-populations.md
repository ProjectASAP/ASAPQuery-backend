# Maintained SQL row populations

SQL `Rows` candidates now have a deployable snapshot-maintenance executor. The
Planner owns membership, grouping, value-column and readout semantics. The backend
lowers `MaintainPopulation → ReadPopulation` to `ReadTablePopulation`; it does not
infer rules from SQL text. Existing SQL window summaries use their existing path.

The initial implementation polls complete ClickHouse table scans. The complete source schema is checked against native `SELECT *` metadata, so an
outdated catalog cannot omit TopK output columns. It preserves a
multiset of rows, including duplicates. An UPDATE appears as old rows disappearing
and new rows appearing; DELETE retracts disappeared rows. It does not invent a
primary key, consume an unreliable timestamp watermark, or assume append-only
input. Each successful complete snapshot publishes a new epoch atomically. An
unchanged population reuses its sorted group state. Quantile readouts and descending
TopK limits share this state, with the largest requested k represented in the
Planner population contract.

## Deployment and reads

Use `/api/v1/clickhouse-plan/table-populations/cost-manifests` with the existing
`ClickHouseSqlAutomaticWorkload` as `workload`, plus an explicit maintenance policy:

```json
{
  "maintenance": {
    "database": "default",
    "refresh_interval_ms": 1000,
    "max_snapshot_age_ms": 5000,
    "max_rows": 100000,
    "max_bytes": 67108864
  },
  "horizon_seconds": 3600,
  "query_evaluations": {"SELECT quantileExactInclusive(0.9)(value) FROM samples": 3600},
  "capability_snapshot_id": "measurement-generation",
  "backend_compat": "<deployment compatibility>"
}
```

The endpoint exports a maintained alternative and a native alternative. Provide
complete `WorkloadCostEvidence` for both manifests to
`/api/v1/clickhouse-plan/table-populations/compile-and-publish`, in a
`TablePopulationDeployment` containing `workload`, `evidence`, and
`max_evidence_age_ms`. Selection includes source scans/transfers, reconciliation,
sorting, residency, retirement, readouts and native fallback. Missing, ambiguous,
stale or incomplete quotes cannot silently select the maintained alternative.

For JSONCompact output, set `output_format_json_quote_64bit_integers=0` or `1`
explicitly; the response preserves native UInt64 COUNT metadata and the requested
integer quoting.

The SQL HTTP request must explicitly specify the installed `database` parameter
and `X-ASAP-Max-Snapshot-Age-Ms`, accepting at least the installed maximum age.
Requests without this acceptance use the native backend. Per-request authentication
or SQL execution settings also use native execution, so a background source cannot
silently read under a different user or settings context.

The first eligible read starts one shared polling task and falls back while cold.
Subsequent reads use the committed snapshot, without making a source request per
readout. The HTTP reader enforces a response byte limit while streaming, in addition to
ClickHouse result limits. The configured memory budget reserves conservative space
for the response (1/64), staged members (1/4), the previous epoch, JSON decoding and
group caches. Age starts before the source SELECT, so scan time counts against the
freshness budget. Failed, oversized, invalid or timed-out refreshes invalidate
readiness; stale state is never represented as an empty table. Plan-generation
changes stop the old polling task. State is rebuilt after process restart.

## SQL semantics and limits

The executor admits direct aggregate readouts with column projections and
`SELECT * ... ORDER BY value DESC LIMIT k`; scalar projection wrappers retain
native execution. The current Planner contract requires a non-null Float64 value column. Nullable
value aggregates and other numeric types are not admitted. Result columns are
limited to Int64, Float64, String and Bool (COUNT preserves native UInt64 output). Nullable grouping keys
retain SQL grouping semantics. Equal-valued rows remain separate quantile/count
members. SQL `ORDER BY value DESC LIMIT k` preserves complete rows; this is not
ClickHouse `topK(k)(x)`, which counts frequent values.

Quantiles use inclusive linear interpolation, with `quantileExactInclusive(q)(x)`
as the exact native reference. The SQL frontend also recognizes ClickHouse's
parametric `quantile(q)(x)` as the existing mathematical quantile intent, whose
accuracy is set by `AccuracyTarget`, as with `approx_percentile_cont`. This does
not claim equality to a particular randomized reservoir result. Discrete
`quantileExact` is not normalized to inclusive interpolation.

Empty ungrouped COUNT is zero, SUM is zero, and AVG/quantile is NaN (JSON encodes
nonfinite output according to the ClickHouse result format). Empty grouped
aggregates and TopK return no rows. Nonfinite inputs and overflowing intermediate
sums require native evaluation instead of publishing a false finite result.

This executor maintains snapshots, not a database CDC log. It provides the
explicitly accepted bounded-stale contract, not transaction-level freshness.
Polling still scans and transfers the full selected population; sharing amortizes
that work across readouts between refreshes. Whether this beats native execution
must be measured and reflected in the full workload quotes. Persisted recovery,
transaction-log CDC and primary-key incremental maintenance remain separate work.

## Verification

The disposable ClickHouse 26.8.2.7 integration test compares 44 results and full
native metadata: six quantiles including 0 and 1, Sum, Avg, Count, and TopK 2/4,
across initial rows, UPDATE, deletion of one equal-valued member, and an empty
table. It also checks missing/insufficient freshness acceptance, explicit JSON
integer policy, bounded source responses, schema drift/recovery and failed refresh
fallback. The final run made 12 background source-fetch attempts, including the
failure scenarios; each readout batch reused the maintained population.

Run against a disposable server (the test creates and drops `sql_row_population`):

```sh
ASAP_SQL_ROWS_CLICKHOUSE_URL=http://127.0.0.1:38123 \
  cargo +1.98.0 test -p data_plane --lib \
  real_clickhouse_population_updates_deletes_and_shared_reads -- --ignored --nocapture
```

Workspace library tests pass (2,109 tests); the external-server test above is
ignored by the ordinary suite and was executed separately. Workspace all-target
Clippy passes with warnings denied. This verifies behavior and shared source
maintenance; it is not a latency or throughput benchmark.
