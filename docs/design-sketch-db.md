# Sketch DB — Design Index

Entry point for the sketch DB design. Replaces the earlier monolithic
draft; the content lives in four companion docs so reviewers can load
only the parts relevant to them.

**Status** (2026-04): Phases 2, 3, 5 of the roadmap have landed; Phase 4
(semantic compaction), most of Phase 6 (metadata APIs), and Phase 7
(label postings) are pending. See the [Status table](#status) below.

---

## TL;DR

The sketch DB is a **materialized-view storage engine over streaming
metrics**. Each view — identified by a monotonic `agg_id` — pins one
aggregation definition (sketch type + parameters + grouping labels +
window size) for its lifetime and is maintained by two complementary
paths:

- **Incremental** (live ingest): window closes emit one row per
  `(agg_id, group_key, window)` with `origin: Native`.
- **Refresh** (backfill): when a reconfigure creates a new `agg_id`, a
  backfill job reads raw samples from the exact DB and fills in history
  with `origin: Backfilled{job_id}`.

Queries dispatch **by metric**, not by `agg_id`. The schema timeline
turns a `(metric, time-range)` request into one or more per-segment
sub-queries, each against a single agg. Every result carries an error
bound derived from the sketch type + parameters.

Six invariants worth remembering:

1. `agg_id` is immutable — reconfigure always mints a new id.
2. Writes are gated by a lifecycle barrier (`is_writable(agg_id)`).
3. Sealed windows are immutable; flush is async.
4. Typed aux columns (`count/sum/min/max`) stay exact even when the
   sketch is approximate.
5. Queries that span reconfigures always go through
   `timeline_for_metric`, never raw store scans.
6. Every query result carries accuracy metadata.

---

## Companion docs

| Doc | What's in it | When to read |
|---|---|---|
| [`design-sketch-db-core.md`](./design-sketch-db-core.md) | §1–8, §10, §14, §15 — the **as-built contract**: motivation, MV framing, principles, architecture, schema, per-agg lifecycle, schema timeline, incremental ingest, backfill, reconfigure workflow, API surface | Reviewing code or writing PRs that touch the sketch DB today |
| [`design-sketch-db-roadmap.md`](./design-sketch-db-roadmap.md) | §9 compaction, §11 admission control, §12 observability, §13 rollout, §16 phase plan, §17 hot-reload relationship, §18 open questions — **not-yet-implemented** subsystems | Planning future work or prioritizing |
| [`design-sketch-db-performance.md`](./design-sketch-db-performance.md) | §19 performance envelope vs Prom/VM + §20 Sketch Profiler library spec | Evaluating adoption or tuning parameters |
| [`design-sketch-db-pluggable.md`](./design-sketch-db-pluggable.md) | How to extract the sketch DB into its own library crate and optional gRPC binary | Planning the library/server split |
| [`design-simple-map-store-persistence.md`](./design-simple-map-store-persistence.md) | LSM-style parts-based persistence layer the sketch DB is built on | Touching the on-disk format |
| [`adding-a-new-sketch.md`](./adding-a-new-sketch.md) | Cross-repo recipe (sketchlib + DataCollector + backend) for extending the supported sketch list | Adding HLL/KLL/CMS/… variants |

---

## Section map (where each § lives)

Top-level section numbers are **kept stable across all sketch-DB docs**
so cross-references like "see §6.3 for the write barrier" remain valid
regardless of which file a reader is in.

| § | Title | File |
|---|---|---|
| 1 | Motivation | core |
| 2 | Sketches as materialized views | core |
| 3 | Design principles | core |
| 4 | Architecture (incl. storage tiers) | core |
| 5 | Storage schema | core |
| 6 | Per-`agg_id` schema and its lifetime | core |
| 7 | Schema timeline | core |
| 8 | Incremental view maintenance (live ingest) | core |
| 9 | Semantic compaction | **roadmap** |
| 10 | Refreshable view maintenance (backfill) | core |
| 11 | Admission control and isolation | **roadmap** |
| 12 | Observability and debugging | **roadmap** |
| 13 | Rollout and migration | **roadmap** |
| 14 | Full reconfigure workflow | core |
| 15 | API surface | core |
| 16 | Implementation phases | **roadmap** |
| 17 | Relationship to hot-reload PR #16 | **roadmap** |
| 18 | Open questions | **roadmap** |
| 19 | Performance envelope (incl. §19.9 accuracy bounds, §19.10 merge propagation) | **performance** |
| 20 | Sketch profiler library | **performance** |
| 21 | Related approaches: wavelets and ML models as materialized views | **performance** |

---

## Status

Summary of which design sections are actually implemented. "✅ done"
means the code matches the spec; "⚠️ partial" means the contract is
partially wired; "❌ not started" means spec only.

| § | Claim | Status | Evidence |
|---|---|---|---|
| 5.1 | `SketchEntry` with typed aux columns (count/sum/min/max) as first-class fields | ⚠️ partial | `PrecomputedOutput` carries origin but aux scalars live in the accumulator, not as record columns |
| 5.2 | Primary key `(agg_id, window_start, group_key)` | ✅ | `SimpleMapStore` per-agg bucketing + per-key |
| 5.2 | Secondary index `(agg_id, group_key, window_start)` | ❌ | |
| 5.3 | Label posting index (Roaring bitmaps) | ❌ | only label interning exists |
| 6.1–6.2 | `AggSchema`, `AggStatus{Active,Retired,Expired}` | ✅ | `stores/sketch_db/schema.rs` |
| 6.3 | Write-side schema barrier `is_writable(agg_id)` | ✅ | called in `ingest_handler.rs` |
| 6.4 | `AccuracyProfile` on schema | ✅ | `stores/sketch_db/accuracy.rs` |
| 7.2 | `timeline_for_metric(...)` | ✅ | `schema.rs:594` |
| 7.3 | Cross-schema query combiner | ✅ | `engines/timeline_dispatch.rs` |
| 8 | Incremental ingest (OTLP / Prometheus / VictoriaMetrics / Kafka drivers) | ✅ | `drivers/ingest/` |
| 8.4 | Watermark + lateness policy | ✅ | `allowed_lateness_ms` + `LateSampleHandlingPolicy` |
| 9 | Semantic compaction (LSM levels) | ❌ | comment-level only |
| 10.2 | Backfill job/source/status types + registry | ✅ | `stores/sketch_db/backfill.rs` |
| 10.3 | Separate backfill worker pool | ✅ | `backfill_service.rs`, `backfill_worker.rs` |
| 10.4 | Coverage tracking | ⚠️ partial | `Coverage` enum exists; not yet wired to query path (Phase 5f) |
| 10.5 | Deterministic rebuild | ⚠️ partial | processor scaffolded; end-to-end determinism pending |
| 10 | HTTP backfill trigger + list | ✅ | `/api/v1/db/backfill`, `/api/v1/db/backfill/jobs` |
| 11 | Per-agg quotas, idle eviction, live vs backfill isolation | ❌ | |
| 12.1 | Prometheus metrics suite | ⚠️ minimal | only `SAMPLES_BLOCKED_BY_SCHEMA_BARRIER` |
| 12.3–12.5 | Audit log, debug APIs, query EXPLAIN | ❌ | |
| 13.3 | Part format versioning | ✅ | `persist_format_versioning_tests.rs` |
| 13.1–13.2 | Feature flags + shadow mode | ⚠️ partial | no framework; noop backfill reader is a de-facto shadow |
| 14 | Reconfigure workflow | ✅ | end-to-end flow works via hot-reload + backfill |
| 15.1 | Store trait + query-engine API | ✅ | `stores/traits.rs` |
| 15.2 | Controller `/schemas`, `/timeline`, `/backfill`, `/backfill/jobs`, streaming-config swap, retire, expire | ✅ | `drivers/query/servers/http.rs` |
| 15.2 | `/stats`, `/cost_estimate`, `/pressure` | ❌ | planner uses hand-coded estimates |
| 20 | Sketch profiler library | ❌ | not built |

Biggest gaps, ranked by leverage: semantic compaction (§9), admission
control (§11), observability detail (§12), typed aux columns at the
storage layer (§5.1), label posting index (§5.3), sketch profiler (§20).

---

## Open questions for reviewers

The most valuable review comments land on these, collected in roadmap
§18:

1. Where do backfill-source hash seeds live? (determinism contract)
2. Controller behavior when backfill fails partway through
3. Exact-DB retention vs. maximum backfill horizon
4. Partial-coverage query semantics during in-progress backfill
5. PII / information leakage via debug APIs and TopK sketches
6. Multi-tenancy posture for v1 vs later
7. Single-writer → multi-writer replication

Pointers are in [`design-sketch-db-roadmap.md`](./design-sketch-db-roadmap.md#18-open-questions).
