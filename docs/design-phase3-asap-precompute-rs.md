# Phase 3 step 3: backend consumes `asap-precompute-rs`

## Status

Implemented. PR: `phase3/backend-consumes-asap-precompute-rs`.

## Context

The ASAP edge-framework migration (see
`docs/design-asap-edge-framework.md` in `ASAPCollector`, plus ADR-0002)
factors the SHARED ingest runtime — windowing, snapshot caching,
envelope encoding, sketch reconstruction, sketch merge — into a
host-neutral crate
[`asap-precompute-rs`](https://github.com/ProjectASAP/ASAPCollector/tree/main/asap-precompute-rs).
Every Rust **edge** runtime (Vector adapter, OTAP-Rust, Arrow-backed
shims, Telegraf input) consumes it, and so does this backend.

Before Phase 3 step 3, the backend's ingest path inlined that shared
logic:

- `precompute_operators/{ddsketch,kll,hll,countsketch,countmin}_accumulator.rs`
  each had a `from_sketchlib_proto_bytes` that decoded
  `SketchEnvelope` proto and dispatched the
  `sketch_state` oneof by hand (the same dispatch was duplicated five
  times, once per sketch).
- `drivers/ingest/otel.rs::decode_modified_otlp_sketch_bytes` routed
  full-state OTLP sketch bytes through those per-accumulator
  decoders.
- The merge path in `*::merge_with` (used by the
  `precompute_engine`'s pane-folding logic) called into
  `asap_sketchlib::sketches::*::merge_refs(...)` directly.

Independently, `asap-precompute-rs` was building the same envelope
decode + state extraction + reconstruction logic — for agents.

## Decision

Backend's ingest path **consumes asap-precompute-rs** for the
shared work. Backend's QUERY-side engine (PromQL aggregation,
storage, query planning) stays put.

### What moved into asap-precompute-rs (i.e. no longer
backend-internal)

| Concern | Where the shared logic lives now |
| --- | --- |
| Envelope wire-format runtime view | `asap_precompute_rs::envelope::{SketchEnvelope, Encoding, SketchType}` |
| Envelope proto decode + oneof dispatch | `asap_precompute_rs::envelope::ProtoSketchEnvelope` (re-export of the prost type) plus the per-wrapper `decode_envelope` helpers |
| Sketch reconstruction from envelope payload (DDSketch + KLL) | `asap_precompute_rs::sketches::{DDSketchWrapper, KLLWrapper}::Sketch::apply_delta` |
| Sketch merge | `asap_precompute_rs::Sketch::merge` (round-trip via `snapshot` + `apply_delta`) |
| `Sketch` / `QuantileSketch` / `CardinalitySketch` / `FrequencySketch` traits | `asap_precompute_rs::precompute::*` |

### What stays in `ASAPQuery-backend`

- **Query-side engine**: PromQL aggregation
  (`engines/{logical,physical}`), storage (`stores/`), query
  planning (`asap-planner-rs`), DataFusion bridge (`tests/datafusion`).
- **Per-accumulator query-side surface**:
  `precompute_operators/*_accumulator.rs` keeps the `AggregateCore`,
  `query_statistic`, `MergeableAccumulator`,
  `SerializableToSink` impls, and the per-sketch JSON output. These
  are query-side, not ingest-side.
- **Sparse-delta application**:
  `precompute_operators/*::apply_proto_delta_bytes` and
  `drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes` stay
  because `asap_sketchlib` doesn't yet expose the `compute_delta`
  family upstream — `asap-precompute-rs`'s wrappers fall back to
  "always full" delta encoding (see
  `asap-precompute-rs/src/sketches/mod.rs` "API surface caveats").
  When the upstream `compute_delta` lands, this path will collapse
  into the same delegation pattern the full-state path uses today.
- **MSGPACK** (`*::from_msgpack_bytes`) — alternative wire format the
  asap-precompute-rs edge runtime doesn't emit. Stays for the
  Strategy-A (vendored modified-OTLP) path that hard-codes msgpack
  for some sketches.

### Bridge module

`asap-query-engine/src/precompute_operators/edge_runtime_adapter.rs`:

- Re-exports the asap-precompute-rs runtime view types (one canonical
  `SketchEnvelope`, `Encoding`, `SketchType`, `Sketch`,
  `QuantileSketch`, `CardinalitySketch`, `FrequencySketch`).
- `unwrap_envelope_state(bytes) -> Option<SketchState>`: the
  shared envelope-decode + oneof-extract path that all five
  accumulators used to inline. Single source of truth.
- `reconstruct_via_runtime(SketchType, bytes) -> ReconstructedSketch`:
  uses `asap-precompute-rs`'s `Sketch::apply_delta` to decode an
  envelope and reconstruct the underlying
  `asap_sketchlib::sketches::*` state. Wired for DDSketch + KLL
  today.
- `encode_ddsketch_envelope(&DdSketch) -> Vec<u8>`: emits the
  canonical envelope shape that `asap-precompute-rs`'s wrapper
  emits, byte-for-byte.
- `merge_ddsketches_via_runtime(&DdSketch, &DdSketch) -> DdSketch`:
  routes through `asap-precompute-rs::Sketch::merge`. Result is
  byte-identical to `DdSketch::merge_refs(&[a, b])` because both
  call the same underlying merge logic.

### Wire-up

`drivers/ingest/otel.rs::decode_modified_otlp_sketch_bytes`'s
`ENCODING_PROTO` branch is the entry point for full-state
modified-OTLP sketch envelopes. The DDSketch and KLL arms now
delegate to `edge_runtime_adapter::reconstruct_via_runtime`. HLL /
CountSketch / CountMinSketch keep using the backend's existing
per-accumulator decoder until upstream byte parity (issue #243)
lands.

### Cargo deps

- `asap_sketchlib`: bumped to `branch = "main"` (post-PR-#39 module
  renames; PRs #40/#41/#42 land DDSketch/KLL/CountSketch byte parity).
- `asap-precompute-rs`: new path-dep
  (`{ path = "../../ASAPCollector/asap-precompute-rs" }`) — the
  asap-precompute-rs crate itself path-deps `asap_sketchlib`, so
  cloning ASAPCollector via cargo's git source fails to resolve the
  path. Path-dep mirrors how `asap-precompute-rs` consumes
  `asap_sketchlib` for the same reason.
- Workspace `[patch."https://github.com/ProjectASAP/asap_sketchlib"]`
  redirects the git-sourced `asap_sketchlib` (used by backend) to
  the local checkout, so the type identities at the
  `asap-precompute-rs` ↔ backend boundary unify.

## Acceptance tests

`asap-query-engine/tests/edge_runtime_consumes_precompute_rs.rs`:

- **DDSketch round-trip** through asap-precompute-rs runtime is
  byte-identical with the input envelope.
- **DDSketch structural** assertions (count, alpha within bounds).
- **DDSketch end-to-end**: envelope → backend `AggregateCore` →
  `query_statistic(Quantile)` returns a value within the configured
  `α` of the true median.
- **DDSketch back-snapshot** through asap-precompute-rs is
  byte-identical with the input envelope (closes the encode side of
  the round-trip).
- **KLL round-trip** via asap-precompute-rs runtime is byte-identical
  with the input envelope.
- **KLL structural** assertions (k, items count).
- **HLL / CountSketch / CountMinSketch round-trips** are present
  but gated `#[ignore = "blocked on ASAPCollector#243 HLL/CS/CMS
  byte parity"]` — they will start passing automatically when issue
  #243 lands without code changes here.

## Consequences

### Positive

- Single source of truth for envelope decode + state extraction +
  sketch reconstruction across agents and backend.
- Adding a new sketch only requires one place (asap-precompute-rs)
  to gain envelope-handling support; the backend gets it for free
  via `edge_runtime_adapter::reconstruct_via_runtime`.
- The `Sketch` / `QuantileSketch` / `CardinalitySketch` /
  `FrequencySketch` trait family exposes a uniform interface for
  envelope-shaped sketches, which the per-platform Strategy-B
  adapters (Telegraf / Vector / OTAP) will reuse.

### Negative

- The path-dep on a sibling repo means CI must clone `ASAPCollector`
  next to `ASAPQuery-backend`. Resolved by the README pointer; a
  follow-up will revisit the dep style once the
  `asap-precompute-rs` Cargo.toml flips its own `asap_sketchlib`
  pin to a git URL (so cargo can resolve a single git source).

### Deferred

- HLL / CountSketch / CountMinSketch byte parity (issue #243).
- Sparse-delta `compute_delta` upstream (currently in `sketchlib-go`
  only, not `asap_sketchlib`). Until that lands the typed-delta
  apply path stays in this repo.
- MSGPACK delta encoding (`ENCODING_MSGPACK_DELTA = 4`).
