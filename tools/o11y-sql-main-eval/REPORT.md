# ClickHouse SQL 27-query acceptance matrix on current `main`

This report records a fresh run against backend commit
`791f7d7b0feab3827e7e6f5100e65ef29aec68de`. That commit contains the merged
shared-DAG ClickHouse path from #589. The corpus is the 27-query o11y corpus
previously used by #560, but none of #560's classifications or runtime output
is reused. Its SHA-256 is recorded in `artifacts/corpus.sha256`.

## Result

| Mode | Queries |
|---|---:|
| warm | 0 |
| partial hybrid | 0 |
| exact fallback | 27 |
| request failure | 0 |

The parser and Planner accept q05, q06, and q23 and select an exact MinMax
summary followed by shared relational Project/Sort/Limit nodes. Publication
rejects all three because the `metric = ...` table predicate has no canonical
population binding in the summary catalog. The other 24 queries fail closed in
the SQL frontend. Every query was sent through the production ClickHouse HTTP
listener with an installed, validated current-generation catalog and a real
ClickHouse 26.8.2.7 exact backend. The accelerator returns `Planning` for the 24
frontend misses and `CatalogMiss` for the three entries whose publication was
refused. The listener then executed exact ClickHouse: all 27 responses were HTTP
200 and byte-for-byte equal to a direct request sent to the same exact backend.
The artifacts record `fallback_requested`, `exact_executed`, `exact_success`,
both HTTP statuses, response provenance headers, and the returned result.

The machine-readable per-query evidence is in `artifacts/matrix.json`. Full
canonical AST, selected post-ASAP tree, publication result, and data-plane
routing reason are retained in the other JSON artifacts.

## Operator and dialect gaps

The first failing stage is more useful than assigning each query to its intended
PromQL operator: most SQL mappings encode Prometheus semantics with nested
ClickHouse-native expressions, so the frontend cannot reach the logical
aggregation operator.

| First gap | Count | Queries |
|---|---:|---|
| Nested SELECT/alias parsing (`found: ,`) | 7 | q08, q13, q14, q19, q20, q22, q24 |
| Qualified derived-table columns (`found: .`) | 4 | q01, q02, q17, q25 |
| Empty `map()` / derived alias parsing (`found: )`) | 3 | q03, q04, q16 |
| Metric population predicate cannot bind to SDS | 3 | q05, q06, q23 |
| Alias parsing after derived relation (`found: labels`) | 3 | q11, q18, q26 |
| ClickHouse modulo operator/function | 2 | q10, q27 |
| `mapConcat` | 1 | q07 |
| Zero-argument `map()` | 1 | q09 |
| Map indexing | 1 | q12 |
| Array indexing | 1 | q15 |
| Tuple field access | 1 | q21 |

The corpus contains 13 `sum`, 11 `rate`, 8 `increase`, 8 division, 8 `topk`,
6 `max_over_time`, 4 subquery, 2 offset, and one each of sorting, comparison,
histogram quantile, scalar, and `avg_over_time` workloads. These counts describe
the intended workload; they are not claims of SQL support. Only the three
MinMax workloads currently reach post-ASAP selection.

The SDS predicate failure is the smallest production gap, but accepting it
requires a typed population identity for table predicates and matching
ClickHouse backfill/ingest routing. Encoding the predicate as an ad hoc string
inside the runner would produce a catalog entry that the producer cannot safely
maintain, so this evaluation keeps the production fail-closed behavior.

## Data-structure drift audit

The merged production path has one backend query-plan identity and node model:

- `asap_types::QueryLanguage` is the backend-wide language tag and is re-exported
  by `control_plane::query_plan`; storage adapters also re-export the same type.
- `QueryNodeId`, `QueryPlanNode`, `MaterializationBinding`, `PhysicalGrouping`,
  `ExternalExactRequest`, and `FixedEvaluationRange` each have one definition in
  `control_plane::query_plan`. ClickHouse compilation and both data-plane engines
  consume those definitions directly.
- Summary identities are `asap_types::sds::SummaryDefinitionId` end to end.
  `MaterializationBinding` references that ID and validation resolves it through
  the authoritative `SummaryCatalog` before installation.
- Relational node schemas use ASAPPlanner's `SummarySchema` in the published
  shared DAG and in data-plane execution. `ClickHouseRelation` is a runtime row
  carrier and does not define a second published schema contract.
- `ClickHousePlanningContext` is embedded in the shared `QueryPlan`; there is no
  ClickHouse sidecar plan catalog, activation generation, materialization ID, or
  parallel execution-node enum on `main`.

Two similarly named language types remain across the Planner boundary:
`planner_types::workload::QueryLanguage` describes Planner workload input and
`asap_types::QueryLanguage` identifies backend serving entries. Their variants
and roles differ, and conversion happens at compilation; they are not parallel
backend catalogs. The SQL-specific request/response, table row adapter, schema
catalog, and exact HTTP backend types are protocol adapters around the shared
plan.

The `data_plane::query_engines::canonical` module is production code: the SQL
relational adapter uses its common executor and `RelationalAdapter` trait. Its
comment calls the underlying PromQL file location a compatibility location,
but the exposed types are shared and no stale SQL-only duplicate was found.
The large hand-built plans in ClickHouse accelerator tests are test fixtures;
they mirror wire values to exercise coverage, typed exact leaves, joins, and
relational nodes, but they are not a production planning path.

## Reproduction

From a checkout descended from the recorded baseline:

```bash
tools/o11y-sql-main-eval/reproduce.sh
```

Artifacts:

- `artifacts/frontend-planner-publication.json`
- `artifacts/data-plane-routing.json`
- `artifacts/matrix.json`
- `artifacts/baseline.txt`
- `artifacts/corpus.sha256`

The run validates exact fallback execution and response equality through the
production HTTP listener against live ClickHouse. It does not claim accelerated
versus exact value equality or performance benefit because no query is published
into a warm or hybrid state on this corpus.
