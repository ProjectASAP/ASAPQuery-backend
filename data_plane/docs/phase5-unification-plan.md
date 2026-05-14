# Phase-5 unification plan: SchemaRegistry → SketchIndex, agg_id → sid, MutableEpoch dedup

This is a planning doc, not an implementation. It explains how three
in-flight Phase-5 migrations close out together, and what the data
plane looks like after.

**Status as of May 2026:** all three migrations have landed their
"new side" — `SketchIndex`, `SketchInstanceMetadata`, `sid`,
generic `MutableEpoch<P>` — but the "old side" is still the live
production path. Concretely:

- 45 files reference `aggregation_id`, 48 reference `agg_id`
  workspace-wide. Only 12 files mention `sid`.
- Ingest barrier `SchemaRegistry::is_writable(agg_id)` is the §6.3
  write-side gate, called by every OTLP ingest at
  `data_plane/src/drivers/ingest/otel.rs:526,591,615,1049,1062`.
- Query path emits `aggregation_id_for_key` / `aggregation_id_for_value`
  on the wire response (asap_query_engine/engine.rs:1019,1064).
- `data_plane/src/stores/sketch_db/store/{global,per_key}.rs` still
  use the non-generic legacy `MutableEpoch` / `SealedEpoch` from
  `store/common.rs`. The generic `MutableEpoch<P>` in
  `index/epoch_columnar.rs` is used only by `SketchIndex`.
- There is a literal `// DEPRECATED: aggregation_id-keyed write — remove`
  comment at `drivers/ingest/otel.rs:1054`, confirming the migration is
  acknowledged but not finished.

## The three overlapping migrations

### M1 — Lifecycle metadata fold: `AggSchema` → `SketchInstanceMetadata`

Today two structs describe overlapping per-aggregation metadata at
two granularities:

| | `AggSchema` (sketch_db/schema/) | `SketchInstanceMetadata` (sketch_db/index/) |
|---|---|---|
| Primary key | `agg_id: u64` | `sid: u64` |
| Identity fields | metric_name, grouping_labels | metric_name, group-by KEY set, sketch_type, sketch_config, accuracy_bound |
| Lifecycle | `AggStatus { Active / Retired / Expired }`, retired_at_ms, expires_at_ms | none |
| Source of truth | `StreamingConfig` reconciliation | OTLP-ingest registration |

After M1, `SketchInstanceMetadata` carries the lifecycle fields and
`SchemaRegistry` becomes `SketchInstanceRegistry` (or folds into
`SketchIndex::instances`). The `is_writable(sid)` gate uses sid;
the §6.3 invariant is preserved.

### M2 — Identifier replacement: `agg_id` → `sid`

`agg_id: u64` is a hash of `(metric, agg_type, grouping_labels)`
computed in `crates/asap_types/src/aggregation_config.rs` at
config-load time. It is stable across restarts because the inputs
are stable.

`sid: u64` is assigned at OTLP-ingest registration time (collector
gateway), travels in the wire format, and is canonical from that
point forward.

The migration order matters:
1. Both ids carry simultaneously through the pipeline (already
   happens — wire format has both).
2. Switch the §6.3 barrier from `is_writable(agg_id)` to
   `is_writable(sid)` — requires `SketchInstanceRegistry` keyed by
   sid. (M1 prerequisite.)
3. Switch the store keys: `SketchStore::insert_precomputed_output_batch`
   currently keys on `aggregation_id`; flip to sid.
4. Switch the query path: drop `aggregation_id_for_key /
   aggregation_id_for_value` on the wire response in favor of sid.
5. Drop the `aggregation_id` field from `AggregationConfig` and
   `PrecomputedOutput` (wire-format change — requires coordinated
   collector release).

### M3 — Storage primitive dedup: legacy `MutableEpoch` → generic `MutableEpoch<P>`

The `index/epoch_columnar.rs::MutableEpoch<P>` is a generic version
of `store/common.rs::MutableEpoch` lifted from the legacy code and
parameterized on payload type `P`. Six storage optimizations preserved
(see `INDEX_DESIGN.md`).

Today the legacy uses `P = Arc<dyn AggregateCore>` (trait-object
dispatch). The new `SketchIndex` uses `P = SketchSampleState` (typed
bytes + encoding tag — no dyn dispatch, no Arc cloning).

Two paths to the dedup:

- **Eager M3:** rewrite `store/{global,per_key}.rs` to use
  `MutableEpoch<Arc<dyn AggregateCore>>` and delete the legacy copy.
  ~3–5 hours of careful work on the hot ingest + query path, with
  regression risk in subtle hot-path behavior.
- **Deferred M3:** add a deprecation banner to `store/common.rs`; let
  the legacy version die naturally when M2 + M1 close out. After M1
  + M2, the path-of-record is `SketchIndex`-resident sketch state
  (P = `SketchSampleState`), and the trait-object `AggregateCore`
  payload type becomes vestigial. The `store/{global,per_key}.rs`
  files either delete or become thin shims.

## Sequencing + dependencies

```
                           +-------------------+
                           |  M1: AggSchema    |
                           |  → InstanceMeta   |
                           +---------+---------+
                                     |
            (lifecycle fields land on sid-keyed instance metadata)
                                     |
                                     v
                           +-------------------+
                           |  M2: agg_id → sid |
                           |    (5 substeps)   |
                           +---------+---------+
                                     |
                  (wire format + store keys + query path all on sid)
                                     |
                                     v
                           +-------------------+
                           |  M3: dedup        |
                           |   MutableEpoch    |  ← happens by itself
                           +-------------------+
                            once `AggregateCore`-as-payload is gone
```

M1 blocks M2 (M2 needs sid-keyed lifecycle gate). M2 substeps 4 + 5
require a coordinated ASAPCollector release because the wire format
changes. M3 happens automatically once M2 lands; or can be done
eagerly any time, at the cost of working on the live hot path twice.

## Ordering against other in-flight chains

From the May 12 controller_todo doc:

- **Step Z legacy_expr retirement** (4 PRs, ~8000 LOC across ~340
  pattern-match sites) is orthogonal — operates on the control-plane-
  side intent algebra, not on data-plane identifiers. Can run in
  parallel.
- **Analyzer-unification α→ε** (5 PRs, 5–8 days) interacts only at
  the `Capability` enum surface in `sketch_db/index/`, which both
  the engine-side analyzer and the control-plane-side analyzer consume.
  Coordinate the `Capability` shape once at α; downstream is
  independent.

## Concrete unification work, file-by-file

### Phase A — M1 prep (does not change the wire format)

1. Add lifecycle fields to `SketchInstanceMetadata`:
   - `status: AggStatus`
   - `retired_at_ms: Option<u64>`
   - `expires_at_ms: Option<u64>`
2. Add `SketchIndex::is_writable(sid: u64) -> bool` mirroring
   `SchemaRegistry::is_writable(agg_id)`. Internally consult the
   status field on the matched `SketchInstanceMetadata`.
3. Add `SketchIndex::list_by_status(status: AggStatus) -> Vec<sid>`
   so eviction can drive off it.
4. Tests: replicate every `SchemaRegistry` test against `SketchIndex`.

### Phase B — M1 cutover

1. Switch `SchemaEvictionService` to call `SketchIndex` methods.
2. Switch ingest barrier `is_writable(agg_id)` → `is_writable(sid)`.
3. Delete `SchemaRegistry`, `AggSchema`, `AggStatus` from
   `sketch_db/schema/`. Folder remains for compat re-exports during
   transition; can be deleted once all callers migrated.

### Phase C — M2.1 (parallel-write)

Ingest emits both `agg_id` and `sid` on every precompute (already
the case today). Store accepts either as a key; internally maps
agg_id → sid via a side table. Query path resolves either.

### Phase D — M2.2 cutover

Store keys flip to sid. Wire format drops `aggregation_id` fields.
Coordinated release with ASAPCollector. After this lands,
`aggregation_id` is dead.

### Phase E — M3 freebie

`Arc<dyn AggregateCore>` payloads are no longer the path-of-record;
they only exist in the legacy `store/{global,per_key}.rs` code, which
either deletes (ASAP-tier sketch state now lives in
`SketchIndex.series.windows`) or becomes a thin adapter shim. The
`store/common.rs::MutableEpoch` duplication disappears with its only
caller.

## Estimate

- Phase A: 1 day (additive, low risk).
- Phase B: 1 day (cutover; SchemaRegistry deletion is touchy but
  mechanical).
- Phase C: 1 day (parallel-write is already partially in place).
- Phase D: 1 day + ASAPCollector PR coordination + integration
  testing window.
- Phase E: 0.5 day (mostly deletion).

Total: ~5 working days end-to-end, plus coordination overhead.

## Open questions for the reader

1. Does the wire format have a stability commitment that constrains
   Phase D? (Backwards-compat shim period? Versioned acceptance?)
2. After Phase E, does anything outside `SketchIndex` need to handle
   the trait-object `AggregateCore` payload? E.g., the
   `PrecomputeEngine` write path serializes via `SerializableToSink`
   — does that path move to typed bytes too?
3. What's the agreed retention model for `sid` after a schema
   transition? (Today: AggSchema lifecycle drops the agg_id when
   Expired. Post-migration: SketchIndex drops the sid. Same
   semantics, but verify nothing depends on the agg_id being
   reusable post-eviction.)
