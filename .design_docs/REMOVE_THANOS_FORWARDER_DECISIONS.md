# Remove `ASAP_THANOS_QUERY_URL` and the Thanos forwarder — scoping decisions

Issue: #746. Status: **pending approval** — scoping complete, no implementation started.

## Background

`ASAP_THANOS_QUERY_URL` is the only switch that builds a `ThanosQueryEngine`
in production (`data_plane/src/main.rs`, via `thanos_engine_from_env`).
Nothing in the repo (compose, CI, scripts) sets it, so today the archive slot
always gets `NoDataArchiveEngine` (empty results), or nothing (503) when
`ASAP_REQUIRE_ARCHIVE_ENGINE=1`.

## Decisions

### Q1. Delete `ThanosQueryEngine` entirely, or only the env-var wiring?

- **Context:** outside the env wiring, the engine is built only by 4 tests in
  `drivers/query/servers/http.rs`. `thanos_query_engine/forward.rs` is ~1.2k
  lines; removing only the env var would leave it as dead code.
- **Decision:** delete entirely: the `thanos_query_engine/` module, its
  re-exports in `query_engines/mod.rs`, the 4 `http.rs` tests, the `main.rs`
  registration block, and the doc comments that point to it.

### Q2. Keep the `"thanos_query"` engine ID?

- **Context:** the ID is a wire contract, not just the engine's name. The
  control plane emits it in routing plans (`control_plane/src/emit/backend_wire.rs`).
  `StorageBackend::GorillaObjectStore` maps to and from it. `NoDataArchiveEngine`
  registers under it. Clients select it with `X-ASAP-Engine` / `?engine=`, and
  ~10 HTTP tests check for `data_source: thanos_query`.
- **Decision (SUPERSEDED by Q10/Q11):** originally "keep the id unchanged".
  Q10 removes the `StorageBackend::GorillaObjectStore` variant and the id
  outright, and Q11 makes `thanos_query` a hard parse error like any other
  unknown engine.

### Q3. Keep or remove `ASAP_REQUIRE_ARCHIVE_ENGINE`?

- **Context:** with Thanos gone, this flag only chooses between the empty-result
  stub (default) and no engine (503 `NoEngineRegistered`). Nothing in the repo
  sets it.
- **Decision:** remove it. The archive slot always gets `NoDataArchiveEngine`.
- **Revisited, decision unchanged:** the Prometheus fallback (`PrometheusHttpFallback`,
  from `--prometheus-server`) is a separate path and isn't touched. But the HTTP
  handler reaches it only after `EngineRouter` gives up. Behavior today:
  - **Instant queries:** router-dispatched queries (`Exact` header, or a metric
    resolved to `GorillaObjectStore` / `DoubleWrite`) get `Ok(empty)` from
    `NoDataArchiveEngine::execute`. The Prometheus fallback is never reached.
    `process_via_router` has no fallback call even on `NoEngineRegistered`, so
    this flag didn't route those to Prometheus either.
  - **Range queries:** `NoDataArchiveEngine` doesn't implement `execute_range`
    (the trait default is `CapabilityMiss`), so range misses already reach the
    Prometheus fallback (`http.rs:2393`, `:2429`).
  Nothing sets the flag today, so current behavior doesn't change. See Q5,
  which removes the archive routing itself.
- **Cleanup:** remove the stale `ASAP_GORILLA_S3_*` mentions in `no_data_archive.rs`.

### Q4. The Gorilla merger's read path depends on `ThanosQueryEngine`. Proceed?

- **Context:** `gorilla-merger/` (Go) and `.design_docs/GORILLA_MERGER_DESIGN.md`
  plan for cold/archive reads to go backend → `ThanosQueryEngine` → thanos-query
  → merger StoreAPI + S3. The design's cleanup step says "set
  `ASAP_THANOS_QUERY_URL`". After this change, cold queries always return empty,
  even when the merger is running.
- **Decision:** proceed. The merger/thanos-query read path is shelved. Update
  the merger design doc to say the backend read path has been removed. To bring
  it back, restore `thanos_query_engine/` from git history (see #746).

### Q5. Keep or remove the archive routing in `EngineRouter` / `http.rs`?

- **Context:** with only the stub in the archive slot, archive routing
  (`Exact` → archive only, ASAP `CapabilityMiss` → archive failover,
  warm-retention range split) yields empty results that look successful.
  The archive routing touches:
  - the policy table (`routing/capability_matching.rs::compatible_storage_backends`)
  - `RangeTier` and the `execute_range*` tier variants (`routing/query_engine_routing.rs`)
  - `classify_range_tier` (`http.rs:2188`)
  - the `X-ASAP-Accuracy` header, which only matters for archive routing (`http.rs:48`)
  - the archive overrides in `resolve_metric_storage` (topk with no heap sid,
    `sum_over_time` on ExactAgg, `http.rs:946-1000`)
  - the ASAP engine's `archive_engine` warm+archive stitch (`asap_query_engine/engine.rs`),
    never wired in production
  - the never-incremented cold/hot counters (`drivers/query/fallback/metrics.rs`)
  - the control plane's `thanos_query` routing target (`control_plane/src/emit/backend_wire.rs:107-217`)
  - ~40 tests
  Estimated at ~2–2.5k lines, ~70% of them tests.
- **Decision:** remove the archive routing. (Follow-up questions: Q6+.)

### Q6. What serves a query the ASAP tier can't answer, once there is no archive tier?

- **Context:** today such a query hits the archive stub — `Ok(empty)` for instant,
  `CapabilityMiss` for range — and then sometimes Prometheus.
- **Decision:** the Prometheus fallback when `--prometheus-server` is configured,
  otherwise the adapter's "unsupported query" response. This is what the
  approximate `SketchStore` instant path already does (`http.rs:1328`) and what
  range misses effectively do today. The router becomes ASAP-only apart from the
  `X-ASAP-Engine` override; the archive failover, `RangeTier` and the policy
  table's archive arms go away. Today's silent empty instant results become real
  answers from Prometheus.

### Q7. What does `X-ASAP-Accuracy: exact` mean without an archive tier?

- **Context:** the header's only effect today is routing — `Exact` skips the
  sketch tier for the archive (`http.rs:827-833`). Missing / `approximate` →
  `Epsilon(0.01)`; a typo → 400. (`AccuracyTarget::Exact` elsewhere — ClickHouse
  accelerator, control-plane planner — is unrelated and stays.)
- **Decision:** keep the header; `exact` now means "skip the sketch tier, go
  straight to the Prometheus fallback" (→ "unsupported" when no fallback is
  configured). Preserves the caller contract, keeps validation and the 400 for
  typos, and keeps the 4 header tests meaningful.

### Q8. Should the control plane stop emitting `thanos_query` routing targets?

- **Context:** `build_routing_entry` (`control_plane/src/emit/backend_wire.rs:107-217`)
  gives every metric an `asap_query` default plus a `thanos_query` target claiming
  `histogram_quantile`, `delta`, `deriv`, `absent`, `rate_post_hoc`, plus `topk`
  (no CountSketch) and `count` (no HLL/CMS). Deliberate planner output, but the
  data plane will have nothing to fill that slot.
- **Decision:** stop emitting it. Metrics get only the `asap_query` default; the
  claimed shapes miss on the ASAP tier and go to Prometheus per Q6 — same
  destination, minus the pretend hop. Keep `parse_engine_string` accepting
  `thanos_query` so older stored plans still load (archive targets ignored),
  covering rolling deploys in both directions. Keep the informational
  `asap_tier_native_shapes` field. Touches the control-plane tests asserting
  those targets.

### Q9. The two archive overrides in `resolve_metric_storage` (`http.rs:946-1000`)?

- **Context:** both exist so a query the sketch tier can't answer reaches the
  router instead of dying on the direct sketch path: `topk` with no heap-bearing
  sid, and `sum_over_time` over counter deltas (#301, where `ExactAgg(Sum)` sids
  can't reconstruct Σ-of-cumulative-samples). They are workarounds for the direct
  path having no fallback — exactly what Q6 now provides.
- **Decision:** delete both overrides and their helpers (`query_is_sum_over_time`,
  `metric_has_frequency_topk_sid`). These queries take the normal path: ASAP
  `CapabilityMiss` → Prometheus, a real exact answer instead of an empty archive
  result.
- **Behavior change to flag:** #301's fix now depends on `--prometheus-server`.
  Without it, `sum_over_time` gets "unsupported" instead of an empty result.

### Q10. Delete `NoDataArchiveEngine`, and what about `GorillaObjectStore` / `"thanos_query"`?

- **Context:** with nothing routed to the archive slot the stub has no callers;
  `with_archive_query_engine` is identical to `with_query_engine`.
- **Decision:** delete `NoDataArchiveEngine`, `DATA_SOURCE_ID_NO_DATA_ARCHIVE`,
  `with_archive_query_engine` and the `main.rs` archive block — **and** remove
  the `StorageBackend::GorillaObjectStore` variant, `ENGINE_ID_THANOS_QUERY` and
  its entry in `CANONICAL_QUERY_ENGINE_IDS` (`crates/asap_types/src/storage_backend.rs`).
  Supersedes Q2. Touches ~25 `backend_storage_routing` tests that use the variant
  as a test value, and ~10 http tests asserting `data_source: thanos_query`
  (updated to `asap_query` / Prometheus, not deleted).

### Q11. How should the backend treat a `thanos_query` target in an incoming plan?

- **Context:** `parse_engine_string` (`backend_storage_routing.rs:731`) errors on
  unknown engines, and that error rejects the whole routing document.
- **Decision:** hard-reject, same as any other unknown engine — no legacy-skip
  path. Rolling deploys are explicitly not a concern: there are no production
  deployments of this system. Deploy the control plane (Q8) and backend together.

### Q12. How does this land?

- **Decision:** one PR, commits in dependency order, each building and passing
  tests on its own:
  1. delete `ThanosQueryEngine` + env wiring (Q1, Q3, Q4)
  2. remove data-plane archive routing — policy table, `RangeTier`,
     `classify_range_tier`, `resolve_metric_storage` overrides (Q5, Q6, Q9)
  3. redefine the accuracy header (Q7)
  4. remove `NoDataArchiveEngine`, the `GorillaObjectStore` variant and the id
     (Q10, Q11)
  5. stop emitting archive targets from the control plane (Q8)
  6. delete the dead cold counters, update the merger design doc
- **Verification:** workspace `cargo test` + `cargo clippy` (removing the enum
  variant surfaces non-exhaustive matches). Existing tests are updated, not
  deleted; no new tests planned.

### Q13. Which e2e suites gate this?

- **Decision:** the full `data_plane/tests` suite, not just the two in the blast
  radius (`e2e_controller_plans_and_backend_serves.rs` for the Q8/Q11 plan wire,
  `backend_process_e2e.rs` for the Q1/Q10 startup path).
- **Prerequisite:** `clickhouse_differential_e2e.rs` and
  `clickhouse_q05_process_e2e.rs` need a live ClickHouse (`CLICKHOUSE_URL`,
  `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`). If it isn't reachable those two are
  reported as skipped/failing infra rather than silently dropped.

## Out of scope

- `ASAP_LEGACY_DUAL_WRITE`, `ASAP_SKETCH_RETENTION_MS`,
  `ASAP_SNAPSHOT_MAX_WINDOW_LAG_SECS`.
- Renaming the `thanos_query` engine ID (follow-up).
- Deleting `gorilla-merger/` itself.
