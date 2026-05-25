# Sketch DB — Roadmap (Not-Yet-Implemented Subsystems)

> **Scope.** Design sections that are **intentionally ahead of the code** —
> they describe subsystems we expect to build but have not built yet, plus
> the migration/rollout thinking that applies when we do.
>
> **Status of sections in this file:** all ❌ not implemented or
> ⚠️ partial unless noted. Implemented behavior lives in
> [`design-sketch-db-core.md`](./design-sketch-db-core.md); the
> [index](./design-sketch-db.md) has the full status table.
>
> **Audience:** anyone planning future work, prioritizing, or deciding
> whether a proposal fits into the already-designed path.

Section numbers are kept the same as the original monolithic
`design-sketch-db.md` so cross-references from other docs keep
pointing at the same §-numbers.

---

## 9. Semantic compaction

This section describes **Tier 2's** semantic compaction — the
batched, LSM-level merge process that runs on disk-backed parts.
Tier 1 (PromSketch) implements the *same concept* differently:
continuous in-memory EH bucket merging, done inline as windows age,
without an explicit level structure. Tier 1 is especially effective
when the same series is queried over many different sub-windows
(core §4.1) because the EH lets every such query reuse the same
in-memory structure. The rest of this section is Tier 2 specific.

> **Status.** ❌ not implemented. The part format and the flusher exist
> (see [`design-simple-map-store-persistence.md`](./design-simple-map-store-persistence.md)),
> but there is no LSM-level structure, no semantic merge step, and no
> policy engine. Parts are flushed and age out via TTL; they are not
> compacted to coarser windows. Referenced only as comments in
> `stores/sketch_db/schema.rs`.

### 9.1 Level = temporal resolution

```
Level 0:  10s windows   (live ingest writes here)
Level 1:  1min windows   (merge 6 L0 entries)
Level 2:  5min windows   (merge 5 L1 entries)
Level 3:  1h windows     (merge 12 L2 entries)
Level 4:  1d windows     (merge 24 L3 entries)
```

Each compaction step takes N entries of the same `(agg_id, group_key)`
at level L and merges them into one entry at level L+1 covering a
bigger window. The merge call is the sketch type's `merge_with` trait
method.

Query cost scales with level: "last 1h" reads a level-2 entry; "last
30 days" reads 30 level-4 entries. The coarsest level is bounded by
`retention`.

### 9.2 Compaction policy per `AggStatus`

```rust
fn compaction_policy(schema: &AggSchema) -> Policy {
    match schema.status() {
        Active                           => Policy::FullCompact,
        Retired { expires_in: > 24h }    => Policy::CompactUpTo(Level::L2),
        Retired { expires_in: 1h..24h }  => Policy::CompactUpTo(Level::L1),
        Retired { expires_in: < 1h }     => Policy::NoCompact,
        Expired                          => Policy::Delete,
    }
}
```

Rationale: compaction pays off long-term (less storage, faster
queries), but once an agg is within hours of expiry, compacting it
further is wasted CPU. We gracefully unwind compaction as retirement
approaches.

### 9.3 No cross-`agg_id` compaction, ever

This is a hard invariant. Different agg_ids have different schemas and
their sketches are not generally mergeable. Compaction always groups
entries by `(agg_id, group_key)` and merges only within that.

---

## 11. Admission control and isolation

The core doc treats the sketch DB as if memory, CPU, and I/O were
unbounded. Production deployments are the opposite — a noisy agg_id
must not be able to starve others. This section enumerates what has to
be in place for tier storage and backfill to be production-safe.

> **Status.** ❌ not implemented. Today the write-side barrier
> (core §6.3) only checks schema status; there is no per-agg resource
> accounting. A misconfigured high-cardinality agg can currently
> exhaust memory.

### 11.1 Per-`agg_id` quotas

Each `AggSchema` carries quota limits that the store enforces at write
time:

```rust
struct AggQuotas {
    max_group_states: u64,     // hard cap on distinct (agg_id, group_key) pairs
    max_bytes_in_memory: u64,  // hard cap on live sketch bytes (Tier 1 + Tier 2 memtable)
    max_write_qps: u32,        // write-rate limit (token bucket)
}
```

Today's `persistence_memory_limit_mb` is a **global** cap; the design
moves it to per-agg so one misconfigured high-cardinality agg can't
evict everyone else's hot windows.

### 11.2 What happens when quotas are hit

Policy is per-agg, set by the control plane:

| Policy | Semantics | When to use |
|---|---|---|
| `Reject` | New group writes return error; existing groups continue | Exact-correctness metrics; control plane can react by upgrading quota or retiring the agg |
| `EvictLRU` | Least-recently-written group is dropped to make room | Dashboard metrics; losing old groups is acceptable |
| `Downsample` | Reduce sketch resolution in-place (e.g. halve CMS width) | When accuracy can degrade but coverage must continue |
| `Backpressure` | Return a retry-after signal to the ingest source | Coordinates with upstream — DataCollector can slow its emit cadence |

The write barrier (`is_writable`) extends to check quota, not just
schema existence.

### 11.3 High-cardinality GroupState idle eviction

Even within a quota, stale `GroupState` shells accumulate. A
`group_by=[user_id]` agg might see a user once then never again; the
current code keeps that empty GroupState forever.

Proposal: after every flush tick, evict `GroupState` entries whose
`previous_watermark_ms` is older than `idle_timeout` and whose pane
maps are empty. Next write to that `(agg_id, group_key)` recreates
a fresh state.

```rust
fn evict_idle_groups(&mut self, now_ms: i64) {
    let idle_cutoff = now_ms - self.idle_timeout_ms;
    self.group_states.retain(|_, gs| {
        !gs.active_panes.is_empty()
         || !gs.sketch_panes.is_empty()
         || gs.previous_watermark_ms > idle_cutoff
    });
}
```

This is cheap (one HashMap scan per flush tick) and orthogonal to
the `evict_orphaned_groups` introduced in PR #16.

### 11.4 Live vs backfill isolation

Core §10.3 covers separate worker pools. Additional requirements:

- **CPU share**: live ingest has hard latency SLA; backfill is
  latency-tolerant. Use OS priority or a cgroup per pool, not just
  thread count.
- **Exact-DB rate limiting**: a backfill job that scans 24h of
  Prometheus data can hit the Prom server hard enough to degrade
  live ingest (if both use the same Prom). Backfill reader must
  implement a configurable bytes/sec ceiling.
- **Backfill concurrency ceiling**: total concurrent backfill jobs
  across all aggs, not just per-agg. 100 simultaneously-upgrading
  metrics = 100 backfill jobs = potential write storm.

### 11.5 Global reservations

Some resources are inherently shared and need a global allocator:

- **Memory budget** for Tier 2 memtable + cache: 80% for live ingest,
  20% reserved for backfill (tunable). Backfill blocked if its
  share is full.
- **Disk budget** for Tier 2 parts: a high-water mark triggers
  accelerated TTL sweep or refuses new writes.
- **Exact-DB read budget**: per-deployment cap on bytes-per-second
  read from the exact DB across all backfill jobs.

These are hard caps. The control plane treats them as signals for
planning (e.g. don't plan a backfill if the exact-DB read budget is
saturated by other ongoing jobs).

---

## 12. Observability and debugging

A storage engine without visibility is unusable in production. This
section enumerates what must be observable and through what surface.

> **Status.** ⚠️ minimal. Only `SAMPLES_BLOCKED_BY_SCHEMA_BARRIER`
> exists today (`stores/sketch_db/metrics.rs`). The rest of this
> section is largely aspirational.

### 12.1 Metrics (Prometheus endpoint)

The sketch DB exposes a `/metrics` endpoint with per-agg_id labels:

```
sketch_db_writes_total{agg_id, tier, origin}
sketch_db_query_latency_seconds{agg_id, statistic}    (histogram)
sketch_db_query_coverage_ratio{agg_id}                 (fraction served from sketch)
sketch_db_parts_total{agg_id, level}
sketch_db_bytes_stored{agg_id, tier}
sketch_db_group_states{agg_id, worker_id}
sketch_db_compaction_lag_seconds{agg_id}
sketch_db_backfill_duration_seconds{agg_id}            (histogram)
sketch_db_backfill_bytes_read_total{agg_id, source}
sketch_db_quota_exceeded_total{agg_id, policy}
sketch_db_schema_transitions_total{from_status, to_status}
```

These feed both the Prometheus operator dashboard and the
control plane's `/api/v1/db/stats/*` endpoints (the control plane reads
aggregates across metrics; operators want per-instance detail).

### 12.2 Distributed tracing

A query from user → query engine → sketch DB → tier storage →
possibly exact-DB fallback crosses multiple components. Trace
context (W3C Trace Context) propagates on each hop:

```
Span: http_query (query engine entry)
  └─ Span: find_schema_timeline
  └─ Span: per_segment_dispatch
       ├─ Span: tier1_query (PromSketch)
       └─ Span: tier2_query (SketchStore)
            ├─ Span: part_scan
            └─ Span: sketch_merge
  └─ Span: combine_statistic
```

OpenTelemetry exporter → any OTLP-compatible backend. The backend's
own ingest path is the natural target so the traces land alongside
the metrics.

### 12.3 Structured audit log

Lifecycle events are logged at INFO with structured fields:

```json
{"event": "schema_create",      "agg_id": 17, "metric": "latency", "sketch_type": "KLL", "params": {...}}
{"event": "schema_retire",      "agg_id": 1,  "retired_at": "...", "expires_at": "..."}
{"event": "config_swap",        "added": [17], "removed": [1], "by": "control-plane-a"}
{"event": "backfill_start",     "job_id": 5,  "agg_id": 17, "time_range": [...], "source": "S3Gorilla"}
{"event": "backfill_complete",  "job_id": 5,  "windows_done": 8640, "duration_s": 47.2}
{"event": "quota_exceeded",     "agg_id": 42, "policy": "EvictLRU", "evicted_group": "..."}
{"event": "orphan_eviction",    "worker_id": 3, "agg_id": 1, "evicted_count": 1248}
```

This log is the forensic ground truth when something looks weird in
metrics.

### 12.4 Debug APIs

For on-call inspection:

```
GET /api/v1/db/debug/entry/{agg_id}/{group_key}/{window_start}
  → raw SketchEntry (schema-validated)

GET /api/v1/db/debug/coverage_map/{agg_id}
  → full list of (range, Coverage) for this agg
     (shows Native / Backfilled / BackfillInProgress / Missing)

GET /api/v1/db/debug/parts/{agg_id}
  → list of parts for this agg with (level, min_ts, max_ts, bytes)

GET /api/v1/db/debug/backfill_diff
  body: { agg_id, time_range }
  → compare what's in the store vs what a refresh from the exact DB
    would produce, without actually writing. Useful for catching
    drift between incremental and refresh paths.
```

The last one is particularly valuable — if live and refresh disagree,
the diff tells you which segments and by how much.

### 12.5 Query plan explain

Similar to `EXPLAIN` in SQL:

```
POST /api/v1/query?explain=true
  body: { metric, range, statistic }
  → {
      timeline_segments: [(agg_id, time_range, tier)],
      coverage_per_segment: [...],
      merge_plan: [...],
      fallback_segments: [...],
      estimated_latency_ms: ...,
    }
```

Operators and the control plane use this to understand why a query
went where it went.

---

## 13. Rollout and migration

Every phase in §16 crosses a live deployment. This section lists
the invariants that let each phase roll out incrementally without
breaking existing users.

> **Status.** ⚠️ partial. Part-format versioning is in place
> (`tests/persist_format_versioning_tests.rs`); a noop backfill
> reader factory provides a rough shadow-mode capability. A
> comprehensive feature-flag framework and full shadow-mode are not
> built.

### 13.1 Feature flags

Each phase gets a feature flag, off by default:

```rust
struct SketchDbFeatures {
    typed_aux_columns_enabled: bool,    // Phase 1
    schema_barrier_enabled: bool,        // Phase 2
    metric_routing_enabled: bool,        // Phase 3
    semantic_compaction_enabled: bool,   // Phase 4
    backfill_enabled: bool,              // Phase 5
    metadata_apis_enabled: bool,         // Phase 6
    postings_index_enabled: bool,        // Phase 7
}
```

Flipping a flag takes effect on the next write or query; no restart.
A flag-off rollback is always safe — the old code paths stay compiled
in and active when the corresponding flag is disabled.

### 13.2 Shadow mode

For Phases 1, 3, and 5 specifically, the new path can run in shadow:
compute the new answer alongside the old, diff the two, log
discrepancies, but return the old answer. Runs in production with
zero risk until enough confidence to flip the flag.

```rust
let old = old_path.query_range(...);
if features.shadow_mode_enabled {
    let new = new_path.query_range(...);
    metrics.shadow_diff(old, new);  // histogram of abs error
}
old
```

Shadow mode is also how we **validate Phase 5's deterministic
rebuild claim**: continuously compare a backfill output against live
incremental output for the same metric and assert the sketches are
within-accuracy equivalent.

### 13.3 Part format versioning

Phases 1, 2, 4, 7 change the on-disk part format. Each introduces a
`format_version: u16` field in the part header:

```
v1: count/sum/min/max as inline aux columns (Phase 1)
v2: + schema metadata + origin tag (Phase 2)
v3: + level metadata (Phase 4)
v4: + label postings section (Phase 7)
```

Readers dispatch on version. For forward compat: unknown fields in
a future version's part are skipped (protobuf-style). For backward
compat: at least two consecutive major versions are readable; older
parts age out via normal TTL.

### 13.4 Migration of existing deployments

**Schema bootstrap at Phase 2 rollout**: existing deployments don't
have `AggSchema` records. First startup after the upgrade:

1. Read current `StreamingConfig`.
2. For each `agg_id` in config, create an `AggSchema` with
   `created_at = now`, `status = Active`.
3. Persist to the schema store.

**Historical part interpretation**: old parts don't have origin tags.
Treat missing origin as `Native` (conservative — old data was never
backfilled).

**Coverage reconstruction** (at Phase 5 rollout): on startup, scan
existing parts per agg to populate the initial Coverage map. Slower
startup but one-time.

### 13.5 Rollback strategy

Each phase must be independently rollback-safe:

- **Forward flag off → backward compat** is the baseline: the new
  code doesn't run, old code still works.
- **Backward compat of persisted state**: if Phase 2 created schema
  records and Phase 2 then gets rolled back, the schema records
  remain on disk but are ignored. They don't corrupt the old path.
- **Phased-format parts stay readable** after rollback: a v2 part
  written while Phase 2 was on is still readable by v1-only code
  (the v2-specific fields are skipped).

---

## 16. Implementation phases

This design is intentionally larger than one PR. Suggested rollout:

**Phase 1 — Typed aux columns + query pushdown (small, high-leverage).**
Add `count` / `sum` / `min` / `max` as typed columns on each
`SketchEntry`. Implement `query_range_statistic` that scans the index
and computes these scalars without deserializing sketch bytes. No
schema timeline yet, no backfill yet.

**Phase 2 — Per-`agg_id` schema + write barrier.** ✅ done.
Introduce `AggSchema` with Active/Retired/Expired states. Wire the
ArcSwap config-swap handler to create/retire schemas. Add
`is_writable(agg_id)` barrier on the write path.

**Phase 3 — Schema timeline + metric-based query dispatch.** ✅ done.
Build the `metric_timelines` index. Change the query engine's primary
entry point from "by agg_id" to "by metric." Implement per-segment
dispatch with `combine_statistic` for combinable cases.

**Phase 4 — Semantic compaction.** ❌ not started.
Add level-aware compaction with sketch-type merge dispatch. Policy
table per `AggStatus`.

**Phase 5 — Backfill service.** ⚠️ most sub-phases landed (5a–5d);
5e (real rebuild logic) is scaffolded; 5f (coverage integration with
the query path) is pending.
Independent worker pool. `write_backfilled_window` bypass of
WindowManager. `Coverage` tracking. Control-plane-facing `/backfill`
endpoints.

**Phase 6 — Controller-facing metadata APIs.** ⚠️ partial.
`/stats`, `/timeline`, `/cost_estimate`, `/pressure`. This is what
lets the control plane's planner use real observations. Today only
`/schemas` and `/timeline` are implemented.

**Phase 7 — Secondary indexes.** ❌ not started.
Label postings for fast filter pushdown. Roaring bitmaps per
`(agg_id, label_name, label_value)`.

Phase 1 is largely orthogonal to the others and can ship as an
isolated improvement. Phases 2–3 are the conceptual core — everything
else builds on them. Phases 4–7 are optimizations.

---

## 17. Relationship to the hot-reload PR (PR #16)

PR #16 gets the runtime-reload machinery right: `ArcSwap` is shared,
all consumers read the current config at the same instant, orphaned
`GroupState` entries are evicted after their windows drain. It
establishes the monotonic-`aggregation_id` + time-TTL contract that
this design assumes.

PR #16 does **not** implement any of the sketch DB architecture
described in the core doc. In PR #16:

- There is no `AggSchema` metadata object; the store relies on
  `AggregationConfig` being in the current `StreamingConfig` plus
  time-based TTL to handle retirement.
- There is no schema timeline; queries that span a reconfigure
  boundary see a data cliff at the swap point. This is the known
  query-continuity gap that motivated Phase 3 / Phase 5 of this
  design.
- There is no backfill; once a new agg_id is created, its historical
  coverage grows from zero in real time.
- The storage layer still treats everything by `(agg_id, window,
  group_key)` — no sketch-aware compaction, no typed aux columns, no
  label postings.

So PR #16 was the **foundation** — hot-reload is a hard prerequisite
for everything above — but the sketch DB design is a much larger
program of work that continues to be delivered phase by phase.

---

## 18. Open questions

1. **Where do backfill-source hash seeds live?** To make Backfilled
   sketches byte-identical to what live ingest would have produced,
   the hash function seeds need to be reproducible. Proposal: include
   them in `AggregationConfig.parameters`. Requires a small
   sketchlib-go / sketchlib-rust change to accept an external seed.

2. **What does the control plane do when a backfill fails partway
   through?** Proposal: job_id is idempotent; control plane retries
   with exponential backoff; if persistent failure, proceed without
   the backfilled range (query engine falls back for that subrange).

3. **How much exact-DB retention is needed?** Exact-DB retention
   must be ≥ the longest `backfill horizon` the control plane ever
   requests, which is the longest query range users will issue that
   spans a reconfigure. If exact-DB retention is 7 days and queries
   never look back more than 24h, that's fine. Needs to be tracked
   as a deployment-level configuration — and the control plane should
   reject any upgrade plan whose backfill horizon exceeds current
   exact-DB retention.

4. **Can an in-progress backfill be query-visible with partial
   coverage?** Proposal: yes — `Coverage::BackfillInProgress` is a
   real state. The query engine can wait (if ETA is short), combine
   partial sketch with partial fallback, or serve from fallback only.
   Trade-off knob on the query level.

5. **Does the current `SketchStore` schema support all of Phase 1
   without disk migration?** Mostly. `count`/`sum`/`min`/`max` can be
   written as separate columns in the existing part format; readers
   that don't know about them can skip. The label posting index
   (Phase 7) needs a new on-disk structure and will require a new
   part version.

6. **Relationship to PromSketch?** *(Resolved — see core §4.1.)*
   PromSketch is **Tier 1** of the sketch DB, not a parallel system.
   It's an in-memory EH-backed storage backend specialized for
   short-retention sub-millisecond queries. Tier 2 is the
   precompute + LSM parts store for longer retention. The two share
   one schema lifecycle, one control plane API surface, one refresh
   path, and one query-engine routing layer. The control plane picks
   per-`agg_id` whether to materialize into Tier 1, Tier 2, or both.

7. **Agent clock skew.** `time_unix_nano` on every sample is
   agent-local. Agents with skewed clocks produce windows that don't
   align across agents; cross-agent merge silently blends samples
   that fall into different "real" time buckets. Worse, skew is
   frozen into both live sketches and exact-DB data, so even refresh
   doesn't fix it. Open questions: should the backend reject or
   correct samples with timestamps too far from wall clock? Should
   skew be surfaced as per-agent metadata the control plane can see?
   Probably a hard NTP-sync requirement on agents with metrics
   exposing skew per agent.

8. **Control plane state and HA.** The monotonic `aggregation_id`
   counter has to live somewhere that survives control plane restarts.
   Options: control plane persists its own state (requires a DB for
   the control plane); control plane reads `max(agg_id)` from backend's
   `/api/v1/db/schemas` at startup (simple but needs CAS for
   multi-control-plane-replica HA to avoid two control planes allocating
   the same id). Also: who wins if two control planes disagree on
   what the current `StreamingConfig` should be? Leader election
   or backend-side CAS is needed before HA is viable. This is out
   of scope for the sketch DB itself but affects the design
   contract.

9. **Timezone semantics for window boundaries.** All internal code
   uses Unix epoch. User PromQL queries like
   `quantile_over_time(m[1d]) by (day)` have an implicit "what is
   a day?" — strict 24h vs calendar day with DST transitions. The
   invariant in this design: **windows are Unix-epoch fixed
   intervals**, no calendar alignment. If a UI needs calendar-aware
   bucketing, it does so at the query-engine layer by aligning
   query start times to local midnight.

10. **PII / information leak via sketches and metadata.** CMS +
    heap implicitly exposes high-frequency label values (e.g.
    topk users). HLL cardinality is a sensitive metric in
    privacy-regulated contexts. The
    `/api/v1/db/debug/entry/*` APIs return raw sketch bytes that
    a skilled user might mine. Open: do we need per-metric PII
    tags that disable some debug APIs or redact label values in
    TopK output? Probably deferred until multi-tenancy is needed.

11. **Multi-tenancy.** The design assumes a single trust domain
    (one operator, one set of metrics). In a shared deployment:
    per-tenant quotas, per-tenant access control on
    `/api/v1/db/*` endpoints, per-tenant archive buckets, per-
    tenant billing. All doable but substantial work. Keeping
    single-tenant is a reasonable v1; the design should not
    preclude later multi-tenancy (specifically, `AggSchema`
    should be extensible with a `tenant_id` field without a
    format migration).

12. **Replication and cross-region HA.** Currently the sketch DB
    is single-writer. Read replicas (hot standby that ingests
    the WAL of new parts) are straightforward. Multi-writer
    (active-active across regions) is not — incremental MV
    maintenance with distributed writers requires consensus on
    window boundaries, which is a separate design effort. For
    now: single-writer, use a DR replica as fallback.
