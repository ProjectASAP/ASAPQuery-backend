# ClickHouse mixed differential E2E — 2026-09-10

Base: `origin/main` at `791f7d7b0feab3827e7e6f5100e65ef29aec68de`.
The real ClickHouse server was `http://127.0.0.1:18123`; credentials came from
the running test container and are not recorded in this artifact.

## Result

The mixed DAG produced the same `TabSeparated` result as the equivalent full
ClickHouse query:

```text
1970-01-01T00:00:02	0.5
```

The mixed execution used these nodes in the installed shared `QueryPlanEntry`:

```text
SummaryStore ReadMaterialization -> ExactReadout --+
                                                    +-> RelationalJoin -> Project
ClickHouse ExternalExact --------------------------+
```

The production startup path initially failed this acceptance condition.
`main.rs` gave the HTTP server a `ClickHouseHttpFallback`, but did not give the
same exact backend to `CatalogClickHouseAccelerator`. Every reachable
`ExternalExact` node therefore returned `endpoint unavailable`, causing the
whole request to fall back. The corrected startup wiring shares that backend
with the accelerator.

The real backfill lifecycle test also exposed two fixture errors: the backfill
materialization omitted the fixture's `pane_origin_ms`, and the readout fixture
registered an additional empty series when it was meant to consume only
backfilled state. Aligning the physical identity and avoiding the empty series
makes the test exercise the persisted SummaryStore data.

## Route matrix

| Route | Evidence | Result |
|---|---|---|
| warm summary | `catalog_hit_executes_bound_summary_store_dag_and_encodes_typed_result` | pass; SummaryStore result `50.0` |
| hybrid | `real_clickhouse_external_leaf_matches_exact_query_in_mixed_dag` | pass; real ExternalExact + local Join/Project equals full exact query |
| incomplete coverage | `incomplete_summary_store_coverage_falls_back` | pass; explicit `IncompleteCoverage` |
| full exact fallback | `clickhouse_differential_e2e` | pass; proxy bytes and status equal ClickHouse for four queries |
| ClickHouse backfill to warm | `real_clickhouse_reader_enters_backfill_service_lifecycle` | pass; reader populates the same SummaryStore read by the SQL DAG |

## Architecture audit

- SQL compilation publishes language-tagged entries into the shared
  `QueryPlan`, together with the authoritative `SummaryCatalog`,
  `PrecomputePlan`, and `TransmissionPlan`.
- Summary leaves use shared `MaterializationBinding` values. Installation
  validates their catalog identity, pane duration, and pane origin before one
  `ActivePhysicalPlan` snapshot becomes visible.
- Runtime lookup snapshots that shared physical plan, reads the shared
  `SketchStore`/SummaryStore, and executes `ExternalExact`, `RelationalJoin`,
  and relational operations from the shared `QueryPlanNode` graph.
- `ClickHouseSqlCatalog` is the ASAPPlanner table-schema input used for SQL
  lowering and canonicalization. It is not a second executable-plan catalog.
- No ClickHouse-specific stage, activate, token, or startup sidecar production
  path remains. The only production startup defect found was the missing exact
  backend injection fixed here.
- ClickHouse-specific result and fallback enums stay at the protocol boundary;
  they do not duplicate query-plan ownership or summary execution types.

## Commands

With `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` set:

```bash
cargo test -p data_plane \
  query_engines::asap_clickhouse_query_engine::accelerator::tests -- --nocapture
cargo test -p data_plane --test clickhouse_differential_e2e -- --nocapture
cargo check -p data_plane --bin data_plane
```

All commands passed. Raw logs from this run are in
`/tmp/asap-clickhouse-mixed-e2e-20260910/` on the evaluation host.
