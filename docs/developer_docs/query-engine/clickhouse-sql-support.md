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

## Verification

Focused tests cover language-isolated lookup, atomic install rejection for an
invalid SQL binding, SummaryStore readout through relational operators, typed
encoding, incomplete-coverage fallback, exact proxy behavior, and the optional
ClickHouse backfill reader. The differential integration test runs when
`CLICKHOUSE_URL` points to a real ClickHouse server.
