# Bound-query SDS migration validation

Planner dependency: `c27cd14b8e052ce1f4641c619488ad539ad71f56` (#462).
Scope: installed bound queries. Ad-hoc semantic discovery is not implemented.

[Actual #728 plans for human review](../issue754-human-review/README.md) retain
all ten selected JSON/DOT exports and describe operators, dependencies, windows,
sort expressions and persisted boundaries. Human review is pending.

## Correctness changes

- Semantic definitions are independent of deployed output IDs. The persisted
  Planner description retains value transformations and excludes execution
  placement, temporary node IDs and downstream consumers.
- Framed OTel writes select their declared output before content validation.
  Previously two equivalent hot/rebuild outputs made content matching ambiguous
  (HTTP 422); snapshot/delta caches also lacked output isolation.
- A valid output ID cannot authorize another aggregate state format. The old
  path accepted a Max state under an installed Sum output.
- Recovery validates persisted semantic definitions, both bound identities and
  the installed generation. Old metadata is not assigned invented semantics.
- Storage addresses contain plan, output, group and window. Semantic IDs validate
  state and do not select another deployed producer.

## Implementation entry points

- [Semantic definitions and references](../../../crates/asap_types/src/sds.rs)
  and [catalog validation](../../../crates/asap_types/src/summary_catalog.rs).
- [Compiler catalog binding](../../../control_plane/src/physical/summary_catalog.rs).
- [Bound query resolution](../../../data_plane/src/query_engines/asap_query_engine/catalog_resolver.rs).
- [Durable metadata](../../../data_plane/src/storage_engines/sketch_db/persistence/metadata.rs)
  and [recovery](../../../data_plane/src/storage_engines/sketch_db/persistence/recovery.rs).
- [Production bound-output E2E](../../../data_plane/tests/promql_differential_process_e2e.rs).

## Metadata compatibility

Catalog schema 4 stores immutable semantic definitions separately from deployed
outputs. The storage integration uses durable metadata schema 5. Old metadata
without the required semantic identity cannot be silently rebound; rebuild those
outputs. Payload-codec compatibility does not imply metadata compatibility.

## Checks

The migration was verified at successive stack layers, then at the stack tip:

- Planner types: 217 unit tests, integration/doc tests, strict Clippy.
- Stack-tip core: 118 type tests, 447 control-plane tests, binary tests and both
  Level 1 issue-754 integration tests.
- Shared query layer: 892 data-plane unit tests.
- Stack-tip production process E2E: 19 compatibility tests and 2 differential
  tests, including restart without re-registration, same-definition hot/rebuild
  isolation, group isolation and missing-pane rejection.
- Differential/benefit runner: 16 integration tests.
- Workspace/all-target strict Clippy passes; the final changed transport target
  also passes strict Clippy after its fixture correction.
- All seven Level 2 container differential cases pass: single-rate-temporal,
  sparse-checkout-temporal, aggregations, aggregations-dense-cadence, issue-702,
  issue-702-one-second and issue-754. Raw reports are in [level2](level2/).

The workspace sweep also runs the production controller-to-backend OTLP/PromQL
process test, four sketch-family process oracle tests and monitor integration
checks. Its first completed sweep exposed an old imported-state fixture that
copied a Planner semantic closure without the producer DAG; strict installation
correctly rejected it. That fixture now describes the synthetic state it actually
imports and rejects derived inputs. Its CMS cases no longer assume total-count
planning must select a frequency sketch, or label CMS payloads as HLL/CountSketch.
The corrected target passes all 13 tests on its separate rerun. All workspace
test targets have therefore been checked, but this is a complete sweep plus a
corrective rerun, not one unbroken green command. See [test results](test-results.txt).

Standalone ClickHouse integration tests early-return when `CLICKHOUSE_URL` is
unset; this workspace run does not establish those optional tests. The container
Level 3 run below does use an actual ClickHouse baseline.

These are automated local checks, not a manual production deployment attestation.

## Local Level 3 result: not passed

The retained [semantic report](issue754-benefit.semantic.json) passes all ten
queries. The [benefit report](issue754-benefit.json) uses 3 warmups and 100 trials
against Prometheus, VictoriaMetrics and ClickHouse with unchanged thresholds.

The overall benefit gate **fails**:

- The host does not provide the required cgroup peak-memory readings; all four
  targets report `memoryPeakBytes: null`. No current-memory proxy was substituted.
- Six backend query p95 values exceed VictoriaMetrics: temporal sum, temporal
  quantile, temporal rate, grouped rate, top-k rate and quantile ratio.
- CPU comparisons pass. No claim of a complete performance pass is made.

This run used the migration's code before the final serialization-neutral boxing
of the configured semantic enum (code-equivalent to `3cc717f5`). It is not an
exact-final-head performance certification. The raw reports, selected plan and
planning snapshot are retained so this failure is reviewable; performance
thresholds were not relaxed.

## Reproduce the workspace checks

```sh
cargo build --locked -p control_plane --bin control_plane -p data_plane --bin data_plane
ASAP_E2E_CONTROL_PLANE_BIN="$PWD/target/debug/control_plane" \
  cargo test --workspace --no-fail-fast --locked -- --test-threads=1
cargo clippy --workspace --all-targets --locked -- -D warnings
```

If using a custom `CARGO_TARGET_DIR`, point `ASAP_E2E_CONTROL_PLANE_BIN` to that
directory's `debug/control_plane`. The local runs used two build jobs and disabled
debug information and incremental artifacts. Level 2 uses the repository's
container differential runner; Level 3 uses its unchanged benefit thresholds.
