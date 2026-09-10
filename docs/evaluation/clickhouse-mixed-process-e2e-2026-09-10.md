# ClickHouse mixed process differential E2E (2026-09-10)

This test closes the process-level gap left by the runtime differential in PR #596. It starts with SQL and a control-plane workload; it never edits a `QueryPlanEntry` fixture.

## Path under test

1. `control_plane::clickhouse::compile_clickhouse_workload` compiles the SQL into the shared QueryPlan DAG.
2. The produced publication contains `ReadMaterialization`, `ExternalExact`, `RelationalJoin`, and a relational `Project` operation.
3. The test starts the real `data_plane` binary with its production ClickHouse HTTP listener and real ClickHouse exact backend.
4. It stages and activates the compiler-produced publication through `/api/v1/physical-plan`.
5. It requests real ClickHouse backfill and waits for SummaryStore completion.
6. It changes the summarized source rows from `2, 3` to `999, 999` after taking the exact baseline.
7. It queries the production listener and requires `x-asap-execution: hybrid` plus byte-for-byte equality with the pre-mutation exact baseline (`2000\t0.5\n`).

The source mutation is the fallback guard: a full exact fallback would return `2000\t199.8\n`, so it cannot satisfy the equality assertion.

## Compiler gap fixed

The relational compiler previously preserved direct exact table scans as `ExactFallback`, and ClickHouse workload compilation rejected every plan containing that node. Predicate-free table scan cuts are now lowered to typed `ExternalExact` requests with quoted identifiers, the planned relational schema, and bounded `from`/`to` parameters. Other exact cuts continue to fail closed.

## Reproduction

```sh
CARGO_TARGET_DIR=/path/to/shared-target \
  cargo test -p control_plane compiles_summary_joined_with_exact_table_into_mixed_dag

CLICKHOUSE_URL=http://127.0.0.1:18123 \
CLICKHOUSE_USER="$CLICKHOUSE_USER" \
CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
CARGO_TARGET_DIR=/path/to/shared-target \
  cargo test -p data_plane --test clickhouse_differential_e2e -- --nocapture
```

Observed result: the compiler regression passed, and both real ClickHouse differential tests passed. Raw earlier investigation logs remain in `/tmp/asap-clickhouse-mixed-e2e-20260910/` on the test host.
