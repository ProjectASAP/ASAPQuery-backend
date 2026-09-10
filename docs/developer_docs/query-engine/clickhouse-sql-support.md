# ClickHouse SQL support

Status: implementation design

## Current implementation

The backend now has an independent ClickHouse HTTP listener and exact
fallback, an ASAPPlanner SQL entry point, a generation-aware SQL plan catalog,
SDS foreign-key validation and staged/active ACKs, and a shared relational DAG
execution path. `Project`, `Filter`, global `Sort`, and `Limit` execute above
summary readout; unsupported scalar expressions and relational shapes fail
closed to ClickHouse. SQL results are encoded as typed `JSON`, `JSONEachRow`,
or `TabSeparated` responses.

At startup, `--clickhouse-plan-bundle` (or
`ASAP_CLICKHOUSE_PLAN_BUNDLE`) installs the independently published table
schemas, SDS snapshot reference, SQL templates, execution windows, and allowed
materialization fingerprints. Omitting the bundle keeps the listener in exact
proxy mode. Historical ClickHouse rows can be replayed through
`ClickHouseReader` into the existing isolated backfill worker.

The same bundle can be published without restarting the backend. POST it to
`/api/v1/clickhouse-plan/stage`; the backend validates its SDS snapshot and all
SQL-plan descriptor foreign keys before returning a `staged` ACK. Then POST
`{"plan_id": ..., "plan_version": ...}` to
`/api/v1/clickhouse-plan/activate`. Activation switches the SQL plan generation
and its schema binder together and returns an `active` ACK. These endpoints are
served only on the independent ClickHouse listener.
Set `ASAP_CLICKHOUSE_PLAN_TOKEN` (or `--clickhouse-plan-token`) to require
`Authorization: Bearer <token>` on both publication operations.

The executable subset is intentionally narrower than ClickHouse SQL. Joins,
window functions, partitioned ranking, unsupported scalar functions, missing
plans, missing materializations, and incomplete coverage route to exact
ClickHouse without changing PromQL behavior or SDS semantics.

## Goals and invariants

ASAPQuery-backend exposes a ClickHouse-compatible HTTP endpoint, uses
ASAPPlanner's SQL frontend and canonical query DAG, executes the same physical
`SummaryNode` DAG used by PromQL, and forwards queries it cannot answer to an
exact ClickHouse backend.

ClickHouse support preserves the meaning and supported domain of every existing
PromQL-facing and shared data structure. It does not add SQL states to PromQL
request/result types, turn SDS into a query-plan protocol, or alter SDS
descriptor identity.

## Architecture

```mermaid
flowchart TB
    PReq[PromQL request] --> PB[PromQL frontend / binder]
    SReq[ClickHouse SQL request] --> SB[SQL frontend / binder]
    PB --> CD[Canonical Query DAG]
    SB --> CD
    CD --> Planner[ASAPPlanner physical planning]
    Planner --> PD[Shared Physical Summary DAG]
    PD --> Exec[Shared DAG executor]
    Exec --> Lookup[SDS descriptor lookup]
    Lookup --> Store[SummaryStore]
    Exec --> PR[PromQL result adapter]
    Exec --> SR[ClickHouse result adapter]
    PB -->|cannot bind or execute| PF[Prometheus / Thanos fallback]
    SB -->|cannot bind or execute| SF[ClickHouse fallback]
```

ASAPPlanner owns both frontends and the language-independent canonical and
post-ASAP DAGs. The backend owns serving-time binding, materialized-state
resolution, DAG execution, protocol rendering, and fallback selection.

SDS describes summary semantics, fidelity, encoding compatibility, and stable
identity. A physical DAG leaf resolves its required summary against the active
SDS registry. Query fingerprints, fallback targets, and complete query DAGs do
not become part of SDS.

## Module boundaries

```text
data_plane/src/query_engines/
├── canonical/
│   ├── executor.rs
│   ├── context.rs
│   ├── result.rs
│   ├── sds_resolver.rs
│   └── operators/
├── asap_promql_query_engine/
│   ├── promql_binder.rs
│   └── prometheus_result_adapter.rs
└── asap_clickhouse_query_engine/
    ├── request.rs
    ├── sql_binder.rs
    ├── clickhouse_result_adapter.rs
    └── fallback.rs
```

Compatibility modules may remain at their current paths. Moving PromQL files is
not required and must not change public `ASAPQueryEngine` behavior.

The canonical executor consumes ASAPPlanner's existing
`planner_types::post_asap::SummaryNode`. There is no backend-owned parallel SQL
physical DAG. Language adapters convert common execution outcomes to their own
protocol contracts.

## Request and result boundaries

PromQL continues using its current request, range request, vector, matrix, and
Prometheus response types. ClickHouse owns independent request and table-result
types, including database, query ID, settings, format, Arrow schema, and record
batches.

The canonical executor retains enough schema and grouping information for both
adapters. Existing PromQL wrappers continue returning their original types.

## Planning and publication

The backend control plane sends SQL workload and a ClickHouse schema catalog to
ASAPPlanner's SQL frontend. Both SQL and PromQL lower to `QueryExpr`, pass
through ASAP-aware mapping, and produce `SummaryNode`.

Planning produces two separately owned outputs:

1. Required summaries are published through the existing SDS lifecycle.
2. Query DAG templates are stored in a runtime query-plan catalog.

The catalog records a query fingerprint, output contract, and the existing
serializable, compiler-bound `QueryPlanEntry` physical DAG. It references SDS
descriptors without extending them.
Publication validates every descriptor reference against the candidate SDS
snapshot before atomically activating the catalog generation.

Serving looks up the published executable by fingerprint. It does not rerun
planning, select summary families, or search for replacement materializations.

## Execution and fallback

The shared executor performs candidate resolution, fetch, merge, and readout.
Its structured errors are interpreted at the language boundary: PromQL falls
back to Prometheus or Thanos, while SQL falls back to ClickHouse using the
original SQL text.

SQL falls back for unknown fingerprints, unsupported syntax, invalid binding,
missing or incompatible SDS descriptors, incomplete coverage, stale or gapped
state, unsupported output semantics, or execution/resource failure. Metadata,
health, DDL, and other non-accelerated ClickHouse requests pass through.

## Milestones

### 1. Independent ClickHouse protocol and fallback

- Add an independent ClickHouse HTTP path.
- Support raw POST, GET query parameters, database, query ID, settings, `/ping`,
  and Grafana response formats.
- Preserve upstream status, headers, and body during exact fallback.
- Verify Grafana health, metadata discovery, and SQL pass-through.

This milestone does not depend on DAG execution or change PromQL types.

### 2. Shared canonical planning

- Send SQL workloads and ClickHouse schemas from the backend control plane to
  ASAPPlanner's SQL frontend.
- Consume canonical `QueryExpr` and physical `SummaryNode` output.
- Publish required summaries through SDS.
- Install DAG templates in a separate, generation-aware query-plan catalog.
- Reject catalogs whose DAGs reference unavailable SDS descriptors.

### 3. Shared canonical DAG execution

- Promote the existing `SummaryExecutor` implementation into a canonical
  execution module without changing its PromQL wrapper.
- Add SQL binding and query fingerprint lookup.
- Execute SQL-produced `SummaryNode` DAGs with the shared executor.
- Add ClickHouse table-result conversion and serializers.
- Apply coverage-aware ClickHouse fallback.
- Add ClickHouse ingest/backfill and differential tests against an exact server.

## Verification

Existing PromQL tests must pass unchanged. ClickHouse coverage includes protocol
and forwarding tests, SQL frontend-to-`SummaryNode` tests, SDS reference
validation, shared-executor parity, time-boundary and type tests, Grafana smoke
tests, and a real-ClickHouse differential end-to-end test.

Run the optional real-server protocol and Grafana smoke suite with:

```bash
CLICKHOUSE_URL=http://127.0.0.1:8123 \
  cargo test -p data_plane --test clickhouse_differential_e2e -- --nocapture
```

The test compares proxy and exact responses for a deterministic SQL query,
ClickHouse version discovery, database discovery, and table discovery.

## Architecture trace

| Required flow | Production implementation and evidence |
|---|---|
| Independent protocol | The optional ClickHouse listener owns request parsing, result encoding, `/ping`, authentication forwarding, and exact ClickHouse fallback. It does not use PromQL request or response types. |
| Control plane SQL frontend | `POST /api/v1/clickhouse-plan/compile-and-publish` accepts SQL workload plus table schemas. `compile_clickhouse_workload` calls ASAPPlanner's ClickHouse SQL frontend, producing canonical `QueryExpr` and the shared physical `SummaryNode` DAG. |
| Compiler-bound catalog | The control plane compiles the selected `SummaryNode` into a language-neutral executable payload. `SqlPlanEntry` owns `canonical_sql`; the unchanged PromQL `QueryPlanEntry` continues to own `canonical_promql`. Every read leaf is bound to a catalog `MaterializationId`; the SQL catalog remains separate from SDS. |
| Two-phase publication | `BackendClient::publish_clickhouse_plan` sends the complete bundle to the ClickHouse listener's stage endpoint, waits for its ACK, and then activates the same `(plan_id, plan_version)`. Optional bearer authentication applies to both requests. |
| Runtime execution | Fingerprint lookup returns the published SQL entry and its executable payload. Runtime never reparses SQL or reruns summary selection; it invokes the shared materialization-bound physical DAG executor. |
| SDS and SummaryStore | Stage and serving both validate `MaterializationBinding` through the authoritative `SummaryCatalog`. Store reads resolve the bound fingerprint to SIDs and read SummaryStore panes. Missing descriptors, SIDs, or coverage fail closed. |
| Internal relation and ClickHouse result | Shared execution produces labeled timestamp/value rows. The ClickHouse boundary converts these rows to typed Arrow batches and encodes `TabSeparated`, `JSONEachRow`, or `JSON`. |
| Language fallback | Every catalog miss, validation failure, coverage gap, unsupported DAG, or encoding failure returns to the ClickHouse adapter, which forwards the original SQL to exact ClickHouse. PromQL retains its existing Prometheus/Thanos fallback. |
| Ingest and backfill | Configuring `ASAP_CLICKHOUSE_BACKFILL_TABLE` installs `ClickHouseReader` in the existing BackfillService lifecycle. Jobs opt in with `clickhouse://configured`; other source variants keep the default reader factory. |
| Semantic invariants | No PromQL request/result type or SDS descriptor was extended. `QueryPlanEntry.canonical_promql` and its serialized lookup contract remain PromQL-only; SQL identity lives only in `SqlPlanEntry`. The resolver, executor, SummaryCatalog, and SummaryStore retain their existing meanings. |

The three milestones map directly to the trace: independent listener and exact
fallback cover milestone 1; control-plane compilation, compiler binding, and
two-phase catalog publication cover milestone 2; published-DAG execution,
descriptor resolution, typed output, coverage fallback, and configured
ClickHouse backfill cover milestone 3.

## Operator coverage and failure accounting

Every request remains in the denominator. The ClickHouse listener records a
typed `failure_stage` and `failure_reason` before exact fallback. Stages are
`publication/catalog_miss`, `planner/planning_failed`,
`validator/incomplete_coverage`, `executor/execution_failed`, and
`adapter/unsupported_format`. Control-plane HTTP status separately distinguishes
SQL frontend/canonicalization (`400`), publication transport or ACK (`502`),
and successful activation (`200`). Exact fallback errors return `502`.

| Operator/shape | ASAPPlanner SQL frontend | Published backend runtime |
|---|---|---|
| Scan, time range, aggregate summary selection | Supported when schema and summary family are known | Bound materialization read, merge, estimate, exact readout, scalar/binary value, and reduce-sum nodes are supported |
| Top-level projection | Canonicalized and preserved above the aggregate | Published `Project` evaluates positional scalar expressions and produces the planner-declared typed schema |
| Filter | Canonicalized when ASAPPlanner can type it | Published `Filter` evaluates the canonical predicate over summary readout rows |
| Global sort/limit | Canonicalized | Published global `Sort` and `Limit` execute in the shared relation chain; partitioned sort is typed unsupported |
| Join, window function, partitioned rank | Unsupported for acceleration | Never published; exact ClickHouse receives the original SQL |
| DDL, metadata, system query, unsupported scalar function/cast | Outside accelerated workload planning | Catalog miss or explicit compile rejection; exact ClickHouse |
| Missing SDS descriptor/materialization | N/A | Stage/activation validator rejection |
| Missing or gapped panes | N/A | `validator/incomplete_coverage`, then exact ClickHouse |
| Unsupported ClickHouse output format | N/A | `adapter/unsupported_format`, then exact ClickHouse |

This matrix is deliberately conservative. Unsupported rows are counted as
fallbacks and are never silently treated as accelerated successes.
