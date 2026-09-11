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

## Execution and fallback

The accelerated path supports the relational operations represented by the
shared executable DAG, including summary readout, filter, project, arithmetic,
sort, and limit. Results are encoded as `TabSeparated`, `JSONEachRow`, or
`JSON` without converting through Prometheus result types.

Catalog misses, SQL canonicalization failures, unsupported formats, incomplete
pane coverage, and execution errors fail closed to exact ClickHouse. The proxy
preserves the upstream status, safe headers, and response body.

ClickHouse can also provide samples to the queued backfill service through the
typed `ClickHouse { database, table }` source. Backfill populates the same
SummaryStore instances used by other ingest sources; it does not introduce a
second storage or catalog lifecycle.

SQL materializations can carry a shared `TablePopulation` conjunction of typed
column/literal comparisons. Its canonical identity is stored in the catalog and
included in the materialization fingerprint together with table and value-column
identity. These predicates do not use the PromQL label-filter normalizer. The
reader takes its value projection and population from the installed materialization,
binds literal values as ClickHouse parameters, and accepts either encoded series
labels or a `Map(String,String)` label column. The requested database/table must
match the deployment and installed source respectively.
An absent or empty table population means every row in the table's time interval;
the output metric name never becomes an implicit SQL predicate. Table sources
reject legacy PromQL spatial filters.

The installed `table_timestamp_column` names a Unix-millisecond column and is
shared as `DataDescriptor.timestamp_column`. It enters both identities and is
checked against the Planner source schema's `time_index`. Backfill uses this
installed projection even when the deployment default names a different column.
Legacy table definitions without a timestamp projection cannot be bound or
backfilled; republish them with the explicit column. Time-series definitions
retain their existing timestamp semantics and identity.
SQL fingerprints now include the explicit table/value source, so existing SQL
materializations must be republished and rebuilt; their old state is not reused
under the new identity. Legacy PromQL fingerprints remain unchanged.

SQL timestamp comparisons retain their exact integer-millisecond inclusivity.
For example, `t <= 1999` and `t < 2000` identify the same half-open interval;
`t <= 2000` does not. A source range differing from the installed fixed evaluation
is rejected. Whole-second panes cannot yet cover an arbitrary inclusive boundary
fragment; those queries require exact fallback until the compiler can compose
boundary exact reads with summary interiors. Backfill still buffers a requested
window and has not demonstrated bounded memory at large window/cardinality scale.

## Verification

Focused tests cover language-isolated lookup, atomic install rejection for an
invalid SQL binding, SummaryStore readout through relational operators, typed
encoding, incomplete-coverage fallback, exact proxy behavior, and the optional
ClickHouse backfill reader. The differential integration test runs when
`CLICKHOUSE_URL` points to a real ClickHouse server.
