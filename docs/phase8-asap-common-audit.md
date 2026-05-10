# Phase 8 — `asap-common` Audit and Removal

## Audit table

Files under `asap-common/dependencies/rs/`. Bucket (a) = backend-internal,
(b) = wire-shared. Consumers = crates whose `src/` actually contains a
`use <crate>::...` (Cargo dep alone is insufficient).

| File / crate | Bucket | Consumers |
|---|---|---|
| `asap_types/src/*.rs` (12 files) | (a) | `query_engine_rust` |
| `asap_otel_proto/{build.rs,src/lib.rs,proto/**}` | (a) | `query_engine_rust` |
| `promql_utilities/**/*.rs` (8 files) | (a) | `query_engine_rust`, `asap_types` |
| `datafusion_summary_library/**/*.rs` (7 files) | (a) | `query_engine_rust` |

**Key finding.** Despite the orchestrator spec hinting these might be
wire-shared with the collector edge, the empirical reality is:
- `ASAPCollector/{asap-precompute-rs,asap-gorilla-rust}`: zero
  `use asap_types::*` / zero Cargo dep.
- `ASAPCollector/controller`: zero Cargo dep; only three doc comments
  (`sketch_algebra/capability_matching.rs`, `config/asapquery_backend.rs`,
  `config/stage_config.rs`) that *reference* the paths in prose. No
  real symbol use.

Every public type in `asap-common/dependencies/rs/` is consumed only by
`asap-query-engine`. Bucket (b) is empty.

## Moves

- **(a) backend-internal:** all 4 crates moved verbatim from
  `asap-common/dependencies/rs/<x>` → `crates/<x>` inside
  `ASAPQuery-backend`. They stay as workspace members (cleaner than
  collapsing into `asap-query-engine/src/`: `asap_otel_proto` has a
  `build.rs` + .proto tree; `promql_utilities` is self-contained;
  `asap_types` itself path-deps `promql_utilities`).
- **(b) wire-shared:** **no `asap-wire-types` crate created.** No
  shared wire types exist. Per the conservative-bucket constraint
  ("if ambiguous, put in wire-types"), an empty wire crate would be
  pure overhead.
- Counts: 32 .rs files (across 4 workspace crates) → backend-internal;
  0 → `asap-wire-types`.

## Cargo.toml changes

- `ASAPQuery-backend/Cargo.toml`: dropped `asap-common/...` workspace
  members and legacy `compare_*` test members; added the four
  `crates/<x>` paths; lifted `asap_otel_proto` into
  `[workspace.dependencies]`.
- `ASAPQuery-backend/asap-query-engine/Cargo.toml`: replaced
  `asap_otel_proto = { path = "../asap-common/..." }` with
  `asap_otel_proto.workspace = true`. Other workspace path-deps
  unchanged.
- `ASAPCollector/{asap-precompute-rs,asap-gorilla-rust,controller}/Cargo.toml`:
  **untouched** (none had an `asap_types` / `asap-common` dep).
- `asap-common/` removed (`rm -rf`).

## Build verification (release)

- `ASAPQuery-backend`: `Compiling query_engine_rust v0.1.0
  (.../asap-query-engine)` → `Finished release profile [optimized]
  target(s) in 4m 24s`.
- `ASAPCollector/asap-precompute-rs`: `Finished release in 0.11s`.
- `ASAPCollector/asap-gorilla-rust`: `Finished release in 0.03s`.
- `ASAPCollector/controller`: `136 warnings` (pre-existing dead-code
  lints, unrelated); `Finished release in 0.40s`.

`grep -r "asap-common\|asap_common"` is **clean of Rust code and
Cargo manifests**. Remaining hits: (i) `docs/**.md`, `README.md`,
`TODO.md`, (ii) one prose comment in
`asap-query-engine/src/drivers/query/servers/http.rs`, (iii)
`asap-query-engine/Dockerfile` `COPY asap-common ./asap-common`.
The Dockerfile and docs are explicitly out of scope ("Only Rust");
flag both as follow-ups.

## Tricky cases / compromises

1. **`asap-wire-types` skipped.** Building an empty crate would be
   architectural cargo-cult. If a future edge crate needs a
   serialized type shared with the backend, lift just that type into
   a new `crates/asap-wire-types/` at that point.
2. **Crate-level moves, not file-level.** Splitting types inside
   `asap_types` would have broken the internal `pub use` re-exports
   (`StorageBackend`, `AccuracyTarget`,
   `compatible_storage_backends`) that `asap-query-engine` imports as
   `asap_types::Foo`. Preserving crate identity preserves every
   existing `use asap_types::...` line.
3. **Stale Dockerfile / docs.** `asap-query-engine/Dockerfile` still
   `COPY asap-common ./asap-common` — image builds will fail until
   docker work in a later phase. Rust builds are unaffected.
