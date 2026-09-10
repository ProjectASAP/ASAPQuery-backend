# ClickHouse SQL support

Audience: developers extending the ClickHouse protocol adapter, SQL planning,
or the shared physical-plan runtime.

ASAPQuery exposes an optional ClickHouse-compatible HTTP listener. The listener
accepts ClickHouse GET and raw POST queries, preserves ClickHouse response
formats, and routes unsupported or unavailable accelerated plans to the exact
ClickHouse endpoint.

## Ownership and publication

ClickHouse does not have a separate plan catalog or activation lifecycle. The
control plane lowers SQL through ASAPPlanner, compiles the selected post-ASAP
DAG into a language-tagged `QueryPlanEntry`, and publishes it in the same
`PhysicalPlanInstallRequest` as the SummaryCatalog, PrecomputePlan,
TransmissionPlan, and PromQL or MetricsQL entries.

```text
ASAPPlanner SQL frontend
          |
          v
QueryPlanEntry { language: click_house_sql, DAG, fixed evaluation range }
          |
          v
PhysicalPlanInstallRequest
  SummaryCatalog + PrecomputePlan + TransmissionPlan + QueryPlan
          |
          v
one stage / one activate / one ActivePhysicalPlan snapshot
          |
          +-- PromQL and MetricsQL lookup
          `-- ClickHouse SQL lookup and typed table encoding
```

`QueryPlan::catalog_key` namespaces non-PromQL identities, so equivalent
canonical identities from different languages cannot overwrite each other.
Every SQL summary leaf uses the normal `MaterializationBinding`. Installation
therefore validates the materialization ID, descriptor references, physical
pane duration, and `pane_origin_ms` through the authoritative SummaryCatalog.
Any invalid SQL entry rejects the complete candidate snapshot before
activation; the active generation remains unchanged.

The query listener snapshots `HotReloadActivePhysicalPlan` once per request.
It uses the SQL parsing context and the matching `QueryPlanEntry` from that
same snapshot, reads SummaryStore state, executes relational operators, and
encodes the requested ClickHouse format. It does not replan, choose a different
summary, or maintain a cloned executable catalog.

## HTTP surfaces

The data-plane listener supports `/`, `/ping`, and standard ClickHouse query
parameters. Configure it with:

- `ASAP_CLICKHOUSE_HTTP_PORT`
- `ASAP_CLICKHOUSE_URL`
- `ASAP_CLICKHOUSE_DATABASE`

The control plane exposes
`POST /api/v1/clickhouse-plan/compile-and-publish`. That endpoint compiles a SQL
workload and uses the ordinary physical-plan stage and activate endpoints. The
data plane has no ClickHouse-specific stage or activate endpoints and no SQL
plan token or startup sidecar bundle.

Each workload query may carry two ClickHouse-private SQL strings:

- `sql` is the exact request template. It is also the query sent to ClickHouse
  when coverage or execution requires fallback.
- `planning_sql`, when present, is a semantically equivalent rewrite restricted
  to ASAPPlanner's SQL surface. When absent, `sql` is planned directly.

This split is useful for exact ClickHouse expressions containing tuple access,
array lambdas, or engine-specific functions. The control plane never guesses
that a simpler aggregate is equivalent. Workload generation owns that explicit
equivalence assertion. The data plane binds the normalized exact template to
the published DAG without reparsing ClickHouse-only syntax. Whitespace around
the request and one trailing semicolon are ignored; the SQL body otherwise has
to match exactly, so a different predicate or literal fails closed to
ClickHouse.

## Execution and fallback

The accelerated path supports the relational operations represented by the
shared executable DAG, including summary readout, filter, project, arithmetic,
sort, and limit. Results are encoded as `TabSeparated`, `JSONEachRow`, or
`JSON` without converting through Prometheus result types.

Catalog misses, SQL canonicalization failures, unsupported formats, incomplete
pane coverage, and execution errors fail closed to exact ClickHouse. The proxy
preserves the upstream status, safe headers, and response body.

### `asap_last` boundary

`asap_last(value, timestamp, window_ms)` is intentionally unavailable on the
accelerated path. The SQL frontend must reject it during planning and the HTTP
boundary must send the original, equivalent ClickHouse SQL to exact fallback.
It must not map `asap_last` to `argMax`, `MinMax`, `Sum`, or `Increase`.

The current shared contracts cannot represent its state without acquiring new
meaning:

| Existing contract | Why it cannot represent `asap_last` |
| --- | --- |
| `ExactAggregate(MinMax)` / `MinMaxAccumulator` | Chooses the minimum or maximum **value**, rather than the value from the greatest event timestamp. |
| `ExactAggregate(Increase)` / `IncreaseAccumulator` | Owns counter reset, extrapolation, and rate/increase semantics. Although its implementation retains a last sample, publishing a gauge-last descriptor as `Increase` would make capability matching and persisted payload identity false. |
| `ExactAggregate(Sum)` | Loses both the last value and its event timestamp. |
| Opaque sketch payload | Still requires a shared `SketchAlgorithm` and corresponding merge/readout semantics; it is not an untyped extension slot. |
| Raw sample storage | Can answer an exact scan but is not a mergeable materialized summary and cannot back a `SummaryAgg` DAG node. |

The `AggregationType`, `Statistic`, accumulator factory, SDS capability,
materialization metadata, persistence decoder, pane merger, and DAG readout all
dispatch through closed shared vocabularies. A ClickHouse-only plan sidecar can
name a private operation, but it cannot cause SummaryStore to build, decode,
merge, or query an otherwise unknown payload. Encoding private last state under
an existing type would silently change that type's identity and is forbidden.

Full acceleration therefore requires an explicitly approved contract
extension. The smallest coherent design is:

```text
SqlLast plan leaf (ClickHouse-owned syntax and result binding)
  -> exact timestamped-value materialization descriptor
  -> per-series LastState { event_timestamp, value }
  -> pane/shard merge by greatest event_timestamp
  -> finalize value, retaining the declared series-key columns
```

The table catalog must prove `(timestamp, complete series identity)` is unique,
as it does for SQL rate/increase planning. This removes equal-timestamp
ambiguity within one series. Missing identity proof, duplicate timestamps,
non-numeric values, partial window coverage, mismatched payload identity, or an
unknown decoder must fail closed. `argMax(arg, val)` remains the existing SQL
extension with its original row-selection semantics and is not rewritten to
this operation.

Adding only `SqlLastPlanEntry` is insufficient. An implementation would also
need a distinct persisted payload identity, accumulator factory and serializer,
single- and multi-series materialization, pane merge, coverage-aware readout,
and capability validation. Those are changes to the shared SDS/ingest contract,
so they are outside the compatibility constraint for this work.

ClickHouse can also provide samples to the queued backfill service through the
explicit `clickhouse://configured` source marker. Backfill populates the same
SummaryStore instances used by other ingest sources; it does not introduce a
second storage or catalog lifecycle.

## Verification

Focused tests cover language-isolated lookup, atomic install rejection for an
invalid SQL binding, SummaryStore readout through relational operators, typed
encoding, incomplete-coverage fallback, exact proxy behavior, and the optional
ClickHouse backfill reader. The differential integration test runs when
`CLICKHOUSE_URL` points to a real ClickHouse server.
