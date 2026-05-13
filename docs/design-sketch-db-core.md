# Sketch DB — Core Design (As-Built Contract)

> **Scope.** Sections that describe the architecture and data contract the
> code today is built against. Subsystems that are still aspirational
> (compaction, admission control, full observability, rollout plan,
> performance envelope) live in the companion docs.
>
> **Status of sections in this file:** mostly ✅ implemented, with
> per-section notes where a design element is partial. See the
> [index](./design-sketch-db.md) for the complete status table across all
> sketch-DB docs.
>
> **Companion docs**
> - [`design-sketch-db.md`](./design-sketch-db.md) — index + status table + TL;DR
> - [`design-sketch-db-roadmap.md`](./design-sketch-db-roadmap.md) — §9, §11, §12, §13, §16, §17, §18 (not-yet-implemented)
> - [`design-sketch-db-performance.md`](./design-sketch-db-performance.md) — §19 (performance envelope) + §20 (sketch profiler)
> - [`design-sketch-db-pluggable.md`](./design-sketch-db-pluggable.md) — extracting the sketch DB into its own library / binary
> - [`design-simple-map-store-persistence.md`](./design-simple-map-store-persistence.md) — the LSM persistence layer this builds on
> - [`adding-a-new-sketch.md`](./adding-a-new-sketch.md) — cross-repo recipe for new sketch types

**Audience:** anyone reading the code, writing PRs, or reviewing behavior
changes that touch the sketch DB today.

---

## 1. Motivation

`SimpleMapStore` today is a KV store bucketed by `aggregation_id`. That was
enough for "write sketches, read them back by id." It is no longer enough once:

- The control plane reconfigures sketches at runtime in response to a changing
  query workload (parameter upgrades, new query patterns, retirement).
- Queries should transparently work across those reconfigurations, with no
  data cliff at the upgrade boundary.
- The store needs to give the control plane back real workload observations
  (bytes stored, query latency, merge cost) to drive cost-based planning.
- Persistent storage means there is no implicit cleanup mechanism — any data
  written to disk stays there until something explicitly retires it.

What's needed is a storage engine that treats sketches as first-class typed
values with per-type merge semantics, tracks schema per aggregation across
time, and supports refreshing itself from the exact-DB base relation when
the schema changes. In database terms: the sketches are **materialized
views** over raw sample data; the store is a materialized-view engine
specialized for sketch types.

The **exact DB** (the base relation the MVs are materialized over) may be
implemented by any long-term raw-sample store: S3 with Gorilla compression,
Prometheus / VictoriaMetrics via remote-write, ClickHouse, or any other
backend the deployment picks. The design treats it as a pluggable reader
behind a single interface — the sketch DB doesn't care which backend is
actually holding the raw bytes.

---

## 2. Sketches as materialized views

This section makes the materialized-view framing explicit, because every
later section in this doc is easier to reason about if you have the
classical database analogy in mind.

### 2.1 The view definition

An `AggregationConfig` is a materialized-view *definition*. Expanded:

```sql
-- Pseudocode for an AggregationConfig that computes a CMS over a metric
CREATE MATERIALIZED VIEW agg_17 AS
  SELECT
      time_bucket(INTERVAL '60 s', ts)        AS window_start,
      host                                    AS grouping_key,
      cms_build(value, rows => 3, cols => 1024) AS sketch,
      count(value)                            AS count,
      sum(value)                              AS sum,
      min(value)                              AS min,
      max(value)                              AS max
  FROM   raw_samples
  WHERE  metric = 'latency'
  GROUP BY 1, 2;
```

Read off the `AggregationConfig` fields:

| Config field | SQL analog |
|---|---|
| `metric` | `WHERE metric = …` in the view definition |
| `aggregation_type` + `parameters` | the aggregation function (`cms_build(…)`) |
| `grouping_labels` | `GROUP BY` columns |
| `aggregated_labels` | the inner keyed dimension of multi-subpopulation aggregators |
| `window_size` + `slide_interval` | `time_bucket(…)` + windowing |
| `spatial_filter` | extra `WHERE` clause conditions |

The base relation being materialized over is the stream of raw samples.
Incremental maintenance (§8) reads them from the live agent stream as
they arrive. Refresh (§10) reads them from the exact DB (whichever
backend is configured — S3/Gorilla, Prometheus, ClickHouse, etc.).

### 2.2 What a materialized view framing buys us

| Classical MV concept | Sketch DB counterpart |
|---|---|
| View definition | `AggregationConfig` |
| Base relation | Raw sample stream; exact DB for historical replay |
| Materialization | `SketchEntry` rows across Tier 1 / Tier 2 storage (§4.1) |
| View maintenance strategy | §8 (incremental) + §10 (refresh) |
| Schema evolution | §6 per-`agg_id` lifecycle, §7 schema timeline |
| `DROP MATERIALIZED VIEW` | Retirement + expiry |
| `REFRESH MATERIALIZED VIEW` | Backfill job from exact DB |
| `pg_matviews` / metadata catalog | `/api/v1/db/schemas`, §15 control plane APIs |
| Index on the MV for faster queries | Label postings index (§5.3 — roadmap), typed aux columns (§5.1 — partially implemented) |

The rest of this doc splits into two halves, mirroring the two standard
MV maintenance strategies:

- **§8. Incremental view maintenance** — the live ingest path, which
  updates the MV as new samples arrive. This is what the precompute
  engine already does today; the doc describes how it extends cleanly
  into a sketch-DB-aware design.
- **§10. Refreshable view maintenance** — the backfill service, which
  rebuilds the MV for a time range from the base relation (the exact DB).
  This is the piece that makes schema evolution painless.

Both strategies write to the same physical store and the same per-`agg_id`
namespace. The control plane chooses between them based on workload and
reconfigure cost (§10.6).

### 2.3 Why not just one strategy?

- **Incremental-only** is the status quo. It's cheap (per-sample update
  cost) but it has no way to handle reconfigure: once the view
  definition changes, old data in the MV is under the old schema and
  new data is under the new schema, with a discontinuity at the swap
  point. Sketches of different types or parameters can't be merged.
- **Refresh-only** would work but is wasteful. Rebuilding the MV from
  raw samples at every ingest would throw away the per-sample
  incremental cost advantage that's the whole point of having
  agent-side sketching.

Both together: incremental handles the steady state (99% of the time)
cheaply; refresh handles the rare reconfigure moment. This matches how
real warehouses deploy MVs — you never choose one maintenance strategy
forever, you choose per-view and per-situation.

---

## 3. Design principles

### 3.1 Principles

1. **Per-`aggregation_id` namespace isolation.** Each `agg_id` is its own
   logical namespace with a pinned schema (sketch type, parameters,
   grouping labels, window size). Writes are validated against that
   schema. Compaction never crosses `agg_id` boundaries.
2. **Schema timeline is first-class.** The store knows, for any metric,
   which `aggregation_id` served it during which time range. Query
   engine queries by metric; store handles the per-`agg_id` dispatch.
3. **Semantic compaction.** LSM levels represent decreasing temporal
   resolution. Compaction calls the sketch type's merge operator; level
   N+1 holds entries that are the merge of multiple level-N entries at
   a coarser window. *(Not yet implemented — see roadmap §9.)*
4. **Typed auxiliary columns.** `count`, `sum`, `min`, `max` are stored
   alongside the sketch bytes, not inside them. Queries that only need
   these scalar stats never pay the sketch deserialization cost.
   *(Partial — accumulators carry these; they are not yet first-class
   columns on the stored `PrecomputedOutput`. See §5.1.)*
5. **Exact DB is the source of truth.** Sketches are rebuildable from
   the exact-DB base relation (whichever backend is configured — S3 +
   Gorilla, Prometheus, ClickHouse, VictoriaMetrics, etc.). The store
   supports explicit backfill from the exact DB into any `agg_id` for
   any time range. The exact DB is the same storage that also serves
   the query-engine fallback path when a query cannot be answered from
   sketches.
6. **Monotonic, non-reused `aggregation_id`s.** A reconfigure that
   changes parameters is always "retire old id + create new id", never
   "mutate in place." This is a contract on the control plane; the store
   enforces it via a write-side schema barrier (§6.3).

---

## 4. Architecture

```
             ┌─────────────────────────────────────────────────┐
             │  Control plane                                   │
             │  - owns StreamingConfig (the set of agg_ids)    │
             │  - monotonic id allocator, never reuses         │
             │  - triggers backfill on reconfigure             │
             │  - reads workload metadata for cost model       │
             └──────────┬─────────────────────┬────────────────┘
                        │ StreamingConfig swap│ /api/v1/db/*
                        ▼                     ▼
             ┌─────────────────────────────────────────────────┐
             │  Sketch DB                                       │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Schema Timeline                          │    │
             │  │   per metric, ordered list of agg_ids   │    │
             │  │   with (created_at, retired_at, expiry) │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Per-agg_id storage (multi-tier)         │    │
             │  │   Tier 1: PromSketch (in-mem EH)        │    │
             │  │   Tier 2: precompute + LSM parts        │    │
             │  │   pinned schema (type, params, group)   │    │
             │  │   semantic compaction within a tier     │    │
             │  │   label posting index, typed aux cols   │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Backfill / REFRESH service              │    │
             │  │   reads raw from exact DB (pluggable:   │    │
             │  │     S3+Gorilla / Prometheus / CH / VM)  │    │
             │  │   rebuilds sketches per (agg_id, window)│    │
             │  │   writes to per-agg_id tier storage     │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Query Pushdown                           │    │
             │  │   timeline_for_metric(metric, range)    │    │
             │  │   per-segment dispatch to agg_ids       │    │
             │  │   merge + compute statistic in-store    │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             └──────────────┬──────────────────────────────────┘
                            ▲
                            │ query_metric / point / range_merge
                            │
             ┌─────────────────────────────────────────────────┐
             │  Query Engine (SimpleEngine)                     │
             │  - parses PromQL/SQL                            │
             │  - dispatches by metric, not agg_id             │
             │  - assembles results from DB's per-segment      │
             │    outputs                                       │
             └─────────────────────────────────────────────────┘
```

### 4.1 Storage tiers

The sketch DB is one logical entity with multiple physical storage
tiers. PromSketch and the precompute + LSM store are **not separate
systems** — they are tiers of the same sketch DB. Which tier an
`agg_id` lives on is part of its `AggSchema`, configurable by the
control plane.

| Tier | Implementation | Compaction mechanism | Retention | Query latency | Use case |
|---|---|---|---|---|---|
| **Tier 1** | PromSketch — in-memory EH-backed sketches over raw samples | **Continuous temporal compaction via the EH bucket structure itself** — fine-grained buckets for recent data, exponentially coarser buckets for older data, merged in place as windows age | seconds to minutes (bounded by memory) | sub-millisecond | live dashboards, alerts; **especially good when many sub-window queries target the same series** — e.g. a dashboard that asks for p50/p95/p99 over 1m, 5m, 15m, 1h all against the same metric. Tier 1 serves all of them from one EH structure with no redundant storage; Tier 2 would store one pre-merged sketch per (agg_id, window) and either duplicate the series across multiple agg_ids or rely on read-time merge. |
| **Tier 2** | Precompute engine + `SimpleMapStore` LSM parts (memory + disk) | **Batched semantic compaction** via LSM levels — roadmap §9 — N entries at level L merged into one at level L+1 at a coarser window | minutes to weeks (bounded by `persistence_delete_older_than_secs`) | milliseconds | most production queries, longer-horizon analysis |
| **Exact DB** | S3 + Gorilla / Prometheus / VictoriaMetrics / ClickHouse | Storage-native compression (Gorilla / columnar); no sketch-level merging | months+ (configurable, bounded by raw storage cost) | seconds to tens of seconds | fallback for uncovered queries, base relation for refresh |

The two sketch tiers implement the **same abstract concept** — "reduce
temporal resolution over time while preserving sketch-accuracy
bounds" — but at different points along a latency/complexity curve.
Tier 1 does it continuously in memory via EH bucket merges as windows
age; Tier 2 does it in background compaction with explicit LSM levels.
The control plane picks per-`agg_id` which mechanism matches the
workload: sub-ms live queries with short retention → Tier 1; weeks of
queryable history → Tier 2; both → both.

> **Status note.** Tier 1 (PromSketch) is currently dormant in the codebase;
> most of its integration points are commented out. Tier 2
> (`SimpleMapStore` + LSM persistence) is the active path. Tier
> selection fields on `AggregationConfig` are partially plumbed but not
> exercised end-to-end.

**Tier selection per `agg_id`.** A metric's `AggregationConfig` carries
a `tier` field: `Tier1Only`, `Tier2Only`, or `Both`. `Both` means
incremental maintenance writes to both tiers — Tier 1 for freshness,
Tier 2 for long retention. Query engine picks per-query based on the
query's time range and SLA.

**Tier promotion / demotion on reconfigure.** The control plane can
upgrade an `agg_id` from Tier 1 to Tier 2 as the workload justifies
the long retention cost. Schema-wise this is a regular reconfigure
(retire old id, create new id with new tier), and the Tier 2 backfill
path (§10) reads from the exact DB to populate history.

**The exact DB is not a tier.** It's the base relation every tier
materializes from. It's shown alongside the tiers in the table
because the query engine's fallback path also reads from it, so
operationally it looks like a third storage layer. But conceptually
it is "not sketch DB" — it holds raw samples, not sketches.

### 4.2 Why this framing

Before this framing: PromSketch (`stores/promsketch_store/`) and the
precompute-backed `SimpleMapStore` looked like two independent
sketch systems that the query engine had to route between. They had
overlapping but different semantics (incremental MV in both cases, but
different schema, different retention model, different hot-reload
story).

Unifying them as tiers of one DB means:

- One schema lifecycle (§6) covers both tiers; a tier upgrade is a
  regular reconfigure.
- One control plane API surface (§15) exposes stats across tiers.
- One refresh path (§10) can write to either tier.
- The query engine's routing logic (§7.3 per-segment dispatch) picks
  tier by the same mechanism it picks `agg_id` — the schema timeline
  already carries everything needed.

The two tiers' internal storage formats differ (EH-backed arrays vs
LSM parts), and that's fine — the sketch DB abstracts over them
through a common tier-backend trait. Internally each tier keeps its
own implementation details.

---

## 5. Storage schema

### 5.1 Per-entry record

```rust
struct SketchEntry {
    // Identity
    agg_id: u64,
    group_key: String,      // e.g. "service=auth;region=us-east"
    window_start: u64,      // business-time window bounds
    window_end: u64,

    // Typed auxiliary columns — scalars queryable without
    // deserializing sketch_bytes
    count: u64,
    sum: f64,
    min: f64,
    max: f64,

    // Sketch payload
    sketch_type: SketchType, // redundant with agg_id's schema; enables
                             // polymorphic reads and self-describing dumps
    sketch_bytes: Vec<u8>,

    // Provenance
    origin: EntryOrigin,     // Native | Backfilled { job_id }
    ingest_ts: u64,          // when this record was written
}

enum EntryOrigin {
    /// Written by the live ingest path — precompute worker flushed a
    /// window close.
    Native,
    /// Written by the backfill service from exact-DB raw data. The
    /// job_id lets the system correlate with the BackfillJob that
    /// produced it (for re-runs, debugging, idempotency).
    Backfilled { job_id: u64 },
}
```

> **Status.** `PrecomputedOutput` in the code carries `(agg_id, window,
> key, origin)` plus the accumulator (which internally holds the
> sketch + count/sum/min/max). The typed aux columns are **not**
> materialized as first-class fields on the entry today — they live
> inside the accumulator. Promoting them is a known improvement that
> saves a deserialize on scalar queries.

Why `count`/`sum`/`min`/`max` are not inside the sketch:

1. `sum_over_time`, `count_over_time`, `max_over_time`, `min_over_time`
   are the overwhelming majority of production queries. They should
   never pay the sketch-deserialize cost.
2. These stats are already tracked losslessly by DataCollector's
   processors (they live in the typed `*SketchDataPoint` proto fields
   as first-class numbers). We are preserving that typing end-to-end
   instead of forcing them to travel through the sketch blob.

### 5.2 Primary key and secondary index

Two access patterns dominate:

| Pattern | Source | Key order |
|---|---|---|
| "all groups at time T" | batch jobs, control plane workload scans | `(agg_id, window_start, group_key)` |
| "one group over time range" | dashboard queries, `quantile_over_time(…) by (svc)` | `(agg_id, group_key, window_start)` |

The store maintains the first as the primary index (SSTable sort order) and
the second as a secondary index. Primary order wins during compaction
(spatial locality across a single window close is the common ingest
pattern); secondary index is maintained via an auxiliary log that the
reader consults for single-group time scans.

> **Status.** Primary order is the shape of SimpleMapStore today
> (per-`agg_id` bucketing + per-key). A separate materialized
> secondary index is **not** implemented; single-group time scans
> iterate the primary structure.

### 5.3 Label posting index

For control plane workload queries like "how many distinct services does
metric `X` have?" and for label filter pushdown (`service=auth`), a
per-`agg_id` label posting index maps:

```
label_postings[agg_id]: HashMap<(label_name, label_value), RoaringBitmap<group_key_hash>>
```

Roaring bitmaps keep this compact even for high-cardinality labels.
Compared to scanning every group, a label match becomes an O(1) bitmap
lookup.

> **Status.** Not implemented. Only label interning exists. This is a
> future performance win for high-cardinality aggs; see roadmap §16
> "Phase 7" in [`design-sketch-db-roadmap.md`](./design-sketch-db-roadmap.md).

---

## 6. Per-`agg_id` schema and its lifetime

### 6.1 `AggSchema` — the pinned metadata

```rust
struct AggSchema {
    agg_id: u64,
    metric_name: String,                // which metric this agg serves
    sketch_type: SketchType,            // pinned for agg's lifetime
    parameters: HashMap<String, Value>, // pinned for agg's lifetime
    grouping_labels: Vec<String>,       // pinned for agg's lifetime
    window_size: u64,                   // pinned for agg's lifetime
    slide_interval: u64,                // pinned for agg's lifetime

    // Lifecycle
    created_at: u64,
    retired_at: Option<u64>,
    expires_at: Option<u64>,   // = retired_at + configured retention
}

enum AggStatus {
    /// Listed in current StreamingConfig. Writes accepted.
    Active,
    /// Removed from StreamingConfig but within retention. Writes
    /// rejected; reads allowed.
    Retired { expires_in: Duration },
    /// Past retention. Scheduled for deletion.
    Expired,
}
```

Implemented in `asap-query-engine/src/stores/sketch_db/schema.rs`
(`AggSchema`, `AggStatus`, `SchemaRegistry`).

### 6.2 Status transitions

```
┌─────────┐  control plane POSTs new StreamingConfig
│ (none)  │  with this agg_id included
└────┬────┘
     │ create_schema()
     ▼
┌─────────┐  control plane POSTs new StreamingConfig
│ Active  │  that removes this agg_id
└────┬────┘
     │ retire_schema()
     ▼                                  ┌──────────────┐
┌─────────┐  time passes until         │ Compaction    │
│ Retired │  retired_at + retention    │ policy drops  │
└────┬────┘                            │ to Partial /  │
     │ expire()                         │ None as       │
     ▼                                  │ expires_in    │
┌─────────┐  TTL sweep                  │ shrinks       │
│ Expired │ ─────► DELETE               └──────────────┘
└─────────┘
```

Each transition is atomic on the ArcSwap that carries `StreamingConfig`
plus an explicit DB call (no polling; Active→Retired happens when the
HTTP handler that processes the config swap sees the id missing from
the new config).

### 6.3 Write-side schema barrier

Every write to the store is gated by `is_writable(agg_id)`:

```rust
fn write_entry(&self, entry: SketchEntry) -> Result<(), WriteError> {
    let schema = self.schemas.get(entry.agg_id).ok_or(WriteError::UnknownAgg)?;
    match schema.status() {
        AggStatus::Active => { /* validate entry matches schema, write */ }
        AggStatus::Retired { .. } => Err(WriteError::RetiredAgg),
        AggStatus::Expired => Err(WriteError::ExpiredAgg),
    }
}
```

This is the authoritative "no writes after retirement" guarantee.
Even if a slow in-flight ingest batch routes stale data to the
retired agg_id, the store rejects it. Workers log the rejection and
drop the batch.

Implemented at `precompute_engine/ingest_handler.rs` — the ingest
path calls `state.schemas.is_writable(config.aggregation_id)` before
every write; the `SAMPLES_BLOCKED_BY_SCHEMA_BARRIER` counter tracks
barrier rejections.

### 6.4 Accuracy profile is part of the schema

Every sketch type has a **mathematically proven** error bound that
depends only on its parameters and the sketch family — not on the
input data. This means as soon as the control plane commits to an
`AggregationConfig`, the resulting MV's accuracy guarantees are
fixed and known. The schema metadata captures this:

```rust
struct AggSchema {
    // ... fields from §6.1 ...

    /// Error bound this MV can serve, derived purely from
    /// sketch_type + parameters. Cached on AggSchema creation so
    /// the query path can return it without recomputation.
    accuracy_profile: AccuracyProfile,
}

struct AccuracyProfile {
    /// Per-statistic answerability: which statistics this sketch
    /// can answer at all, and what the error bound is for each.
    per_statistic: HashMap<Statistic, ErrorBound>,
    /// Probability the bound holds (1 - δ for CMS, asymptotic 95%
    /// for KLL/HLL/DDSketch unless overridden, 1.0 for typed aux
    /// scalars).
    confidence: f64,
    /// What happens to the bound when N entries of this sketch are
    /// merged. Some sketch families preserve the bound on merge
    /// (CMS cell-wise sum, HLL register OR, KLL level merge with
    /// modest overhead); others widen it.
    merge_propagation: MergePropagation,
}

enum ErrorBound {
    /// Symmetric: |estimate − true| ≤ delta with prob ≥ confidence
    AbsoluteSymmetric { delta_fn: BoundFn },
    /// One-sided over-estimator (CMS): true ≤ estimate ≤ true + δ
    OneSidedOver { delta_fn: BoundFn },
    /// Multiplicative: |estimate − true| / true ≤ ε
    Relative { epsilon: f64 },
    /// Standard error: σ for normal approximation; UI picks z
    /// (e.g. 1.96 for 95% CI, 2.58 for 99%)
    StandardError { sigma_fn: BoundFn },
    /// Rank error (KLL): the returned quantile's true rank is
    /// within `epsilon` of the requested rank, with prob ≥ conf
    Rank { epsilon: f64 },
    /// Exact — typed aux columns (count/sum/min/max), no error
    Exact,
}

/// Functions that compute an absolute error from per-window data
/// (e.g. CMS frequency: δ = ε·||x||₁ depends on total mass in window).
type BoundFn = Box<dyn Fn(WindowStats) -> f64>;
```

The actual ε / δ / σ formulas per sketch family are listed in
[`design-sketch-db-performance.md`](./design-sketch-db-performance.md)
§19.9 (theoretical bounds appendix). The point of putting them on
`AggSchema` is so the control plane can reason about
"does this sketch satisfy my query's accuracy SLA?" at plan time —
and the query path can return the bound to the user without
recomputation.

This also ties into PromSketch (Tier 1) and Tier 2 having different
accuracy profiles for the same metric: a Tier 1 PromSketch over
recent samples might use a smaller-K KLL than the Tier 2 long-term
storage. The query engine can prefer Tier 1 for queries whose SLA
permits the looser bound, and Tier 2 for tighter SLA.

Implemented in `asap-query-engine/src/stores/sketch_db/accuracy.rs`.

---

## 7. Schema timeline — the key to query continuity

### 7.1 What it is

```rust
metric_timelines: HashMap<
    MetricName,
    BTreeMap<TimeRange, AggId>
>
```

For each metric, an ordered list of `(time_range, agg_id)` segments
covering its history. Non-overlapping by construction (the control plane
contract: a metric has one Active agg_id at a time, plus zero or more
Retired overlapping within retention).

Example for metric `latency`:

```
│── id=1 (CMS256) ──┤
                    │── id=17 (CMS1024) ──┤
                                          │── id=42 (KLL200) ──►
T=0                 T=day1                T=day3              now
```

### 7.2 `timeline_for_metric(metric, t1, t2) -> Vec<(AggId, TimeRange)>`

The single most important query-engine-facing API. Given a metric and a
time range, returns the agg_ids that cover the range (in time order),
each clipped to the query range.

```rust
timeline_for_metric("latency", day1-1h, day1+1h)
  → [(1,  [day1-1h, day1]),
     (17, [day1,    day1+1h])]
```

Query engine uses this to dispatch per-segment. DB provides it from an
in-memory BTree; cost is one HashMap lookup + BTree range scan,
nanoseconds. Implemented at `schema.rs`; used by
`query-engines/timeline_dispatch.rs`.

### 7.3 Per-segment query dispatch

When the query engine processes `count_over_time(latency[24h])` at a
moment spanning multiple segments:

```rust
fn query_metric(&self, metric: &str, range: (u64, u64),
                statistic: Statistic) -> QueryResult {
    let segments = self.timeline_for_metric(metric, range.0, range.1);
    let mut partials = Vec::new();
    for (agg_id, seg_range) in segments {
        // Check coverage — was this segment written by Native ingest,
        // is a Backfill in progress, or is the range not covered?
        match self.coverage(agg_id, seg_range) {
            Coverage::Complete => {
                partials.push(self.query_range(agg_id, seg_range, statistic));
            }
            Coverage::BackfillInProgress { pct } => {
                // Either wait (if eta short) or fall back
                partials.push(self.fallback_exact(metric, seg_range, statistic));
            }
            Coverage::Missing => {
                partials.push(self.fallback_exact(metric, seg_range, statistic));
            }
        }
    }
    combine_statistic(partials, statistic)
}
```

`combine_statistic` depends on the statistic:

| Statistic | Cross-segment combinability |
|---|---|
| Count, Sum, Min, Max, Cardinality (HLL) | Combinable (addition / max / HLL-OR) |
| Quantile, TopK | **Not combinable across different sketch types / parameters.** Returns `PartialResult { covered, missing }`. |

When a statistic is not combinable across schema changes, the user sees
a `PartialResult` that the query engine can render as a warning or a
fall-through to the exact DB for the missing segment. Crucially, this
failure mode is **explicit** — the user knows they are seeing a
schema-change artifact.

Implemented in `query-engines/timeline_dispatch.rs`. Coverage-driven branching
(the `match` in the snippet above) is present in skeleton form; the
`Coverage::BackfillInProgress` → wait-or-fallback policy is still a
follow-up (see roadmap §16 "Phase 5f").

---

## 8. Incremental view maintenance — the live ingest path

This is the MV maintenance strategy that the precompute engine already
implements today. The doc is just making explicit what the design is
and what additions are needed to fit cleanly into the sketch DB.

### 8.1 What "incremental" means for a sketch MV

Each newly-arriving sample (or pre-built short-window sketch from a
DataCollector processor) is folded into the MV by **merging into the
accumulator of the matching `(agg_id, group_key, window_start)`**.
This is exactly the semantics of incremental MV maintenance: the
change to the base relation (one new sample) propagates to the view
(one accumulator update) without re-materializing the view.

The key invariant that makes this tractable for sketches is that every
sketch type the pipeline supports is **mergeable** — the merge
operation is associative and commutative (modulo accuracy bounds for
probabilistic sketches), so the MV can be maintained by folding new
data into open accumulators regardless of order.

### 8.2 The pipeline

```
 raw sample / pre-built sketch arrives
             │
             ▼
┌────────────────────────────────┐
│ IngestState                    │
│   match by metric name         │
│   snapshot current StreamingConfig │   ← ArcSwap read, ~5ns
│   for each matching agg_id:    │
│     build (agg_id, group_key)  │
│     route to Worker            │
└─────────┬──────────────────────┘
          ▼
┌────────────────────────────────┐
│ Worker                         │
│   get_or_create_group_state    │
│     reads schema from ArcSwap  │
│     builds Accumulator per the │
│     MV's view definition       │
│   accumulate_one(sample)       │    ← incremental update
│     or merge_with(sketch)      │    ← associative fold
└─────────┬──────────────────────┘
          ▼
┌────────────────────────────────┐
│ Window close                   │
│   emit PrecomputedOutput       │
│   write SketchEntry to store   │
│     with origin = Native       │
└────────────────────────────────┘
```

Every step is O(1) per incoming sample (or O(sketch_size) per
incoming pre-built sketch). There is no scan of the base relation,
no re-materialization — pure incremental update.

### 8.3 Window close = MV row emission

The MV model clarifies what "flushing a window" means: it's the
*emission of an MV row*. The window manager determines when the row
is finalized; once finalized, the row is immutable in the store and
subject to further transformation only by semantic compaction
(roadmap §9) or by the full-refresh path (§10) replacing it. The MV
row carries:

- The grouping key values (matches the MV's `GROUP BY` columns)
- The window bounds (matches `time_bucket(…)`)
- The aggregated sketch (the `agg_fn(…)` output)
- The typed aux columns `count`/`sum`/`min`/`max` (covering pre-aggregated
  scalar queries without touching the sketch)
- `origin: Native` (distinguishes from Refresh-produced rows; see §10)

### 8.4 Watermark and lateness policy

Incremental MV maintenance against a streaming base relation has the
classical late-data problem: a sample arrives whose timestamp places
it in a window that has already been flushed. The current
`allowed_lateness_ms` and `LateDataPolicy` knobs implement a bounded
out-of-orderness policy: samples up to `watermark - allowed_lateness`
fold into open accumulators; older samples trigger the configured
policy (`Drop` or `ForwardToStore`).

In MV terms, this is the trade-off between "eventual MV consistency
with late-arriving base data" and "bounded MV commit latency." The
default (`Drop`) chooses bounded latency; `ForwardToStore` chooses
eventual consistency. A real sketch DB should surface this trade-off
as per-`AggSchema` policy rather than a global knob — some MVs need
strict correctness (financial / regulatory), some tolerate drops
(dashboards).

### 8.5 Interaction with reconfigure

Because the worker's schema lookup (in `get_or_create_group_state`)
reads the current `StreamingConfig` from ArcSwap on each
first-time-seen `(agg_id, group_key)` pair, the incremental path
transparently picks up newly-created `agg_id`s the moment they are
added to the config. This is what PR #16 delivers.

What the incremental path **cannot** do alone is fill in the MV
retroactively — once a window is closed under agg_id=1, it will never
be populated under agg_id=17, even if agg_id=17's view definition
would have produced a different row for that window. Populating the
new `agg_id` for historical windows is the job of the full-refresh
path (§10).

### 8.6 What's missing for a sketch-DB-grade incremental path

Most of the infrastructure is already there via the precompute
engine. The sketch-DB additions on top of it:

- **Typed aux columns** (§5.1) on every flushed entry, not just the
  sketch bytes. Already trivial to add; the processors already have
  `count`/`sum`/`min`/`max` in the typed OTLP fields.
- **`origin: Native` / `origin: Backfilled { job_id }` provenance
  tagging** to distinguish rows produced by incremental maintenance
  from rows produced by refresh. Needed to reason about coverage
  (§10.4) and for idempotent re-runs. ✅ implemented.
- **Schema-validating write barrier** (§6.3) enforcing that the
  incoming write matches the target `agg_id`'s pinned schema. ✅
  implemented.
- **Per-`agg_id` label postings update** as new group_key values
  appear (§5.3). Trivial — incremental update to a Roaring bitmap
  per first-seen group_key. ❌ not yet.

None of these change the fundamental data flow; they make the
incremental MV maintenance contract observable and enforceable.

---

<!-- §9 Semantic compaction moved to design-sketch-db-roadmap.md §9. -->

## 10. Refreshable view maintenance — the backfill path

Where §8 describes how the MV is maintained as new base data arrives,
this section describes how the MV is **re-materialized** from the base
relation on demand. In SQL terms: `REFRESH MATERIALIZED VIEW agg_17
FOR PERIOD (now - 24h, now) FROM exact_db`.

### 10.1 The core insight

The exact DB holds raw samples losslessly over its configured
retention. Any sketch for any time range within that retention can be
rebuilt from it. A reconfigure that breaks query continuity in the
sketch tier can be followed by a backfill that restores it.

```
Time →      T=0         T=upgrade-horizon        T=upgrade              now
             │                 │                      │                  │
             │                 │                      │                  │
id=1        [═══════ native (CMS256) ══════════════════]                 │
             │                 │                      │                  │
             │                 │                      │                  │
id=17        │                 [▒▒▒▒ backfilled ▒▒▒▒▒▒][══ native (KLL200) ══]
             │                 │                      │                  │
             │                 └── REFRESH reads from  │                  │
             │                     exact DB for this   │                  │
             │                     interval            │                  │
             │                 │                      │                  │
             ▼                 ▼                      ▼                  ▼
           ═══════════════ Exact DB (base relation) ══════════════════════
           (lossless raw samples — S3+Gorilla / Prometheus / CH / VM / …
            feeds the refresh path for the backfilled interval and the
            query engine's fallback for any query sketches can't answer)
```

Reading the diagram:
- **id=1** was the pre-upgrade `AggregationConfig` (CMS with width=256).
  It keeps its existing data across the upgrade; IngestState stops
  writing to it at `T=upgrade`. Its rows are still query-visible until
  `persistence_delete_older_than_secs` elapses, but they are not used
  for post-upgrade queries (the schema timeline in §7 routes queries
  to id=17 after the swap).
- **id=17 (native portion)** is live incremental MV maintenance (§8):
  every window close after `T=upgrade` emits a KLL(200) row with
  `origin = Native`.
- **id=17 (backfilled portion)** is the refresh output (§10.1–10.5):
  a backfill job reads raw samples from the exact DB for
  `[T=upgrade - horizon, T=upgrade)`, rebuilds KLL rows per
  `(group_key, window)`, and writes them to id=17 with
  `origin = Backfilled { job_id }`.

After backfill, id=17 covers the full query horizon. The user's query
"last 24h" is fully served from a single sketch, with a consistent
schema.

### 10.2 Backfill job model — a REFRESH transaction

```rust
struct BackfillJob {
    job_id: u64,
    agg_id: u64,
    time_range: (u64, u64),
    source: BackfillSource,
    status: BackfillStatus,
    started_at: u64,
    completed_at: Option<u64>,
    windows_done: u64,
    windows_total: u64,
}

/// Every variant reads raw samples for the requested metric and time
/// range and feeds them to the sketch builder. The DB picks a concrete
/// reader at job-dispatch time based on what the deployment has
/// configured as its exact DB. All variants implement a common
/// `RawSampleReader` trait internally so the rest of the backfill
/// code is source-agnostic.
enum BackfillSource {
    /// Gorilla-compressed files in S3 / MinIO / GCS, produced by
    /// DataCollector's gorillacol + S3 Files exporter.
    S3Gorilla { bucket: String, prefix: String },
    /// Prometheus (or VictoriaMetrics / Thanos / Cortex) via the
    /// HTTP range-query API.
    Prometheus { url: String },
    /// ClickHouse via native HTTP / SQL.
    ClickHouse { url: String, table: String },
    /// Rebuild from a different sketch (rare; only when types are
    /// compatible and the source sketch is lossless w.r.t. the target).
    /// Used for lossless schema widenings, e.g. CMS(256) → CMS(2048).
    OtherSketch { source_agg_id: u64 },
}

enum BackfillStatus {
    Queued,
    Running,
    Complete,
    Failed(String),
    Cancelled,
}
```

Implemented in `stores/sketch_db/backfill.rs` (types + registry) and
`backfill_service.rs` / `backfill_worker.rs` (worker pool).

### 10.3 Refresh is a separate worker pool

Live ingest and backfill must not starve each other:

- Live ingest has hard latency requirements (data must land in the
  current window before it closes).
- Backfill is latency-tolerant (it's catching up historical windows
  that are already closed) but CPU- and I/O-heavy.

Implementation: a dedicated `BackfillWorkerPool` with configurable
concurrency. Priority knob on each `BackfillJob`. Backfill writes go
through the same schema barrier (`is_writable(agg_id)`) as live writes
but via a distinct `write_backfilled_window()` entry point that
bypasses `WindowManager` (the window is already closed, we're just
populating it).

### 10.4 Coverage tracking

```rust
enum Coverage {
    /// This time range is fully covered by either Native writes or
    /// completed Backfilled writes. Query proceeds against sketch.
    Complete,
    /// A backfill job is in progress for this range.
    BackfillInProgress { job_id: u64, pct: f64 },
    /// This range is not covered at all.
    Missing,
}

fn coverage(&self, agg_id: u64, range: (u64, u64)) -> Coverage
```

Cached in memory keyed by `agg_id`, updated as Native writes land and
as Backfill jobs complete. `Coverage` enum exists in code; integration
with the query path is a follow-up (see roadmap §16 "Phase 5f").

### 10.5 Deterministic rebuild

For backfill to produce "the same sketch we would have built live,"
the sketch construction must be deterministic. That means:

- The hash function seed for CMS / CountSketch / HLL must be part of
  `AggregationConfig.parameters` and stable across Native and Backfill
  paths.
- The KLL / DDSketch sampling decisions must be deterministic from
  the input sample order. This constrains how raw samples are read
  from the exact DB — they must be replayed in the same order they
  were ingested live.
- Exact-DB writes from the live data plane therefore need to preserve
  ingest order within a window (whether the exact DB is S3+Gorilla,
  Prometheus, or any other backend).

This is an invariant the Gorilla processor / S3 exporter must
guarantee. Not hard, but it has to be designed in. The rebuild
processor scaffolding is in place at `backfill_processor.rs`;
end-to-end determinism is still being validated.

### 10.6 When to use incremental vs refresh

The two maintenance strategies (§8, §10) are not alternatives — they
cover different situations and the control plane should pick per
situation.

| Situation | Strategy | Why |
|---|---|---|
| Steady-state live ingest | Incremental (§8) | O(1) per sample, no scan of base relation |
| New `agg_id` created, no historical need | Incremental only | Nothing to refresh from |
| New `agg_id` created, need query continuity across the reconfigure | Incremental **+** Refresh from exact DB over the query horizon | Incremental covers \[now, future\]; refresh covers \[now-horizon, now\] |
| Recovery from a store corruption or a bug in past sketch builds | Refresh | MV is known wrong; rebuild authoritatively from base |
| Onboarding a metric with historical raw data already in the exact DB | Refresh only (until catches up), then Incremental | Much cheaper than streaming a week of historical data through the live ingest path |
| Metric with very low query rate, reconfigure | Incremental only, fall back to exact DB for historical queries | Refresh cost > fallback cost at low QPS |

The control plane decides by comparing estimated costs:

```
cost_of_refresh = exact_db_bytes_to_scan * read_$_per_byte
                + cpu_seconds * cpu_$
cost_of_fallback = expected_queries_to_exact_DB_during_retention
                 * query_latency * query_$
if cost_of_fallback > cost_of_refresh: trigger refresh
```

The `/api/v1/db/cost_estimate` endpoint (§15) is what the control plane
asks to get each side of this inequality.

### 10.7 Relationship to classical MV refresh

Warehouses have two common refresh modes:

- **`REFRESH MATERIALIZED VIEW` (blocking)** — the MV is locked, its
  contents replaced, readers wait. Not viable here: the MV is being
  actively queried by dashboards.
- **`REFRESH MATERIALIZED VIEW CONCURRENTLY`** — rebuild into a
  shadow table, atomic swap when done. Readers see the old version
  until the swap.

This design is closer to the *concurrent* model, with a twist:
refresh is scoped to a time range, not the whole MV. Until a refresh
job completes, the query engine serves the covered portion from the
not-yet-refreshed state (which may be a different `agg_id`, or may
be partial `Native` coverage) and the uncovered portion via
fallback. After the swap (marking the range as `BackfillComplete`),
queries transparently start reading the refreshed data.

Unlike a warehouse's `CONCURRENTLY REFRESH`, the refresh here is
**incremental per window**, not all-or-nothing per MV. A job writing
24 hours of backfilled KLL entries can mark each 10-second window
complete independently. Queries that straddle the in-progress
boundary see a split coverage map (some windows Complete, some
BackfillInProgress), and the query engine handles the split per
§7.3's per-segment dispatch.

---

<!-- §11 Admission control, §12 Observability, §13 Rollout moved to design-sketch-db-roadmap.md. -->

## 14. The full reconfigure workflow

```
T=0:     Config = {1: CMS(256)}
         DB schemas = {1: Active}
         Live: writing to id=1

T=swap:  Control plane decides to upgrade to KLL(200).
         1. Allocates new id=17.
         2. Dual-write phase begins.
            POST /api/v1/streaming-config { 1, 17 }
            DB: schema 17 created Active; schema 1 still Active.
            IngestState routes to both id=1 and id=17.
            (Both sketches accumulating the same underlying samples.)

T=swap+Δ:
         3. Control plane triggers backfill to close the historical gap
            for id=17:
            POST /api/v1/db/backfill {
              agg_id: 17,
              time_range: (swap - query_horizon, swap),
              source: S3Gorilla, // or Prometheus, ClickHouse — whichever backs the exact DB
              priority: High,
            }
         Backfill service reads raw from S3, rebuilds KLL per window
         per group, writes to id=17 with Origin = Backfilled.

         During this phase, a query for "last 24h":
         - segments for [now-24h, swap] hit id=1 (CMS, native data)
         - segments for [swap, now] hit id=17 (KLL, native data)
         - If the statistic is combinable (Count, Sum, Min, Max, Card),
           cross-segment combine works.
         - If not (Quantile, TopK), segment [now-24h, swap] returns
           PartialResult; engine falls back for that segment.

T=backfill_done:
         id=17 now covers [swap - query_horizon, now] completely.
         A query for "last 24h" hits id=17 end-to-end, single schema.

T=backfill_done+ε:
         4. Control plane removes id=1 from StreamingConfig.
            POST /api/v1/streaming-config { 17 }
            DB: schema 1 transitions Active → Retired.
            IngestState stops routing to id=1.
            Worker evict_orphaned_groups reaps (agg_id=1) GroupStates.

T=swap + retention:
         5. Schema 1 expires.
            DB TTL sweep (Phase 3 of the flusher) deletes all id=1
            data from disk based on max_ts.

         Final state: Config = {17}, DB schemas = {17: Active}.
         No tombstones, no dangling references, no agg_id-specific
         deletion needed — time-based TTL handles cleanup on a
         schema-change-oblivious index.
```

Every step is idempotent. If the control plane crashes mid-workflow, the
DB state at any point is a valid state; the control plane resumes from
wherever it left off by reading `/api/v1/db/schemas`.

---

## 15. API surface

### 15.1 Query-engine-facing API

Today the query engine says "give me data for `agg_id = 1`." In the
sketch DB it says "give me `count_over_time` for metric `latency` in
this time range" and the DB does the rest.

```rust
/// Primary query-engine entry point. Dispatches by metric, not agg_id.
fn query_metric(
    &self,
    metric: &str,
    group_filter: Option<LabelFilter>,
    time_range: (u64, u64),
    statistic: Statistic,
) -> QueryResult;

/// Point read for a known (agg_id, group_key, window). Used by
/// diagnostic tooling and by tests; not the main production path.
fn point_read(
    &self,
    agg_id: u64,
    group_key: &str,
    window_start: u64,
) -> Option<SketchEntry>;

/// Streaming scan for batch workloads. Iterator yields entries in
/// primary-key order.
fn scan(
    &self,
    agg_id: u64,
    range: (u64, u64),
    predicate: Option<LabelFilter>,
) -> impl Iterator<Item = SketchEntry>;
```

`QueryResult` carries the **estimate**, an **error bound**, the
**confidence** with which the bound holds, and **provenance** about
what data sources produced the answer. This is a first-class part of
the query contract, not an optional debug field — sketches are
approximate by design and the cost of returning the bound alongside
the value is essentially zero.

```rust
struct QueryResult {
    /// The point estimate.
    estimate: ResultValue,
    /// Mathematically-derived error bound for `estimate`. See §6.4
    /// for the AccuracyProfile that produces this, and
    /// design-sketch-db-performance.md §19.9 for the per-sketch-type
    /// formulas.
    error_bound: ErrorBound,
    /// Probability the bound holds. CMS uses (1 - δ); KLL/DDSketch
    /// use the asymptotic confidence implied by their parameters
    /// (default ~95%); HLL likewise; typed aux columns are 1.0
    /// (exact). Cross-segment combinations multiply or take the min
    /// depending on independence assumptions
    /// (design-sketch-db-performance.md §19.10).
    confidence: f64,
    /// Where the answer came from — sketch family used, how many
    /// entries were merged, whether any segment was backfilled,
    /// whether any segment fell back to the exact DB.
    provenance: Provenance,
}

enum ResultValue {
    /// Single value (Count, Quantile, Sum, Min, Max, Cardinality).
    Scalar(f64),
    /// Time series — per-window value.
    Series(Vec<(u64, f64)>),
    /// TopK and similar multi-value answers.
    Vector(Vec<(String, f64)>),
    /// Some or all of the requested range could not be served from
    /// sketches. Caller decides whether to fall back.
    Partial {
        served: Box<ResultValue>,
        missing: Vec<(u64, u64)>,
        reason: PartialReason,
    },
}

struct Provenance {
    sketch_types: Vec<SketchType>,    // single entry if homogeneous
    windows_merged: u32,               // how many stored entries the answer aggregated
    segments_combined: u32,            // schema-timeline segments crossed
    backfilled_segments: u32,          // 0 = no historical refresh
    fallback_segments: u32,            // 0 = all from sketch DB
    archive_authoritative: bool,       // true if sketches were rebuilt from archive
}
```

The `ErrorBound` enum mirrors §6.4 and is consumed without
recomputation — `AccuracyProfile.per_statistic[stat]` returns the
right variant directly.

**Why every result carries this**: it lets clients (dashboards,
alerts, downstream systems) make informed decisions: an alert can
require `confidence ≥ 0.99` before firing; a dashboard can render
error bars without doing a second query; a downstream consumer can
decide to refetch from the exact DB if the bound is too wide for
its use case.

**Cost**: an extra ~50 bytes per query response. Computation is
O(1) per query. Negligible compared to the actual sketch merge.

### 15.2 Control-plane-facing API

```
GET  /api/v1/db/schemas?status=<Active|Retired|Expired>
  → list of AggSchema objects

GET  /api/v1/db/schemas/{agg_id}
  → one AggSchema + current Coverage per time range

GET  /api/v1/db/timeline/{metric}
  → schema timeline for a metric

GET  /api/v1/db/stats/{agg_id}
  → {
      bytes_stored,
      entry_count,
      avg_sketch_size,
      queries_served,
      p50_query_latency_ms,
      p99_query_latency_ms,
      last_queried_at,
      avg_merge_cost_per_query,
    }

POST /api/v1/db/backfill
  body: { agg_id, time_range, source, priority }
  → { job_id }

GET  /api/v1/db/backfill/{job_id}
  → { status, progress, eta }

DELETE /api/v1/db/backfill/{job_id}
  → cancel running job

POST /api/v1/db/cost_estimate
  body: { metric, sketch_type, parameters, grouping, window_size }
  → estimated { bytes_per_sec, cpu_per_sec, storage_per_day }

GET  /api/v1/db/pressure
  → { write_queue_depth, compaction_lag, memory_used, memory_limit }
```

These are the primitives the control plane uses to close its planning
loop. Without them the control plane plans in the blind; with them it
can run cost-based optimization with real observations.

> **Status.** `GET /api/v1/db/schemas`, `/timeline`, `POST
> /api/v1/db/backfill`, `GET /api/v1/db/backfill/jobs`,
> `POST /api/v1/streaming-config` swap, retire/expire are implemented
> in `drivers/query/servers/http.rs`. `/stats`, `/cost_estimate`, and
> `/pressure` are still aspirational — the planner cost model currently
> uses hand-coded estimates rather than live store observations (see
> [`design-sketch-db-performance.md`](./design-sketch-db-performance.md)
> §20 "Sketch profiler library" for the eventual source of those
> numbers).

---

<!-- §16 phases, §17 hot-reload, §18 open questions in roadmap doc.
     §19 perf, §20 profiler in performance doc.
     §21 (below) is in core because it extends the MV framing from §2. -->

## 21. Related approaches: wavelets and ML models as materialized views

The MV framing in §2 treats sketches as "precomputed,
incrementally-maintainable summaries of a base relation." That
description is broader than sketches — wavelets and (some) ML models
fit it too. This section positions the sketch DB design against those
neighbours so future extensions can reason about which of them slot in
cleanly and which require contract changes.

The framing holds whenever five properties are present:

1. A **base relation** the summary is derived from.
2. **Deterministic derivation** (given parameters).
3. Either **incremental** or **refresh** maintenance semantics.
4. **Queries answerable without rescanning the base.**
5. A **known accuracy contract** (how wrong the answer can be).

Sketches hit all five. Wavelets and ML models hit some but not all —
the pattern of misses determines what it would take to treat them as
first-class citizens of the sketch DB.

### 21.1 Side-by-side comparison

| Dimension | Sketches (CMS / HLL / KLL / DDSketch) | Wavelets (DWT / Haar + thresholding) | ML models |
|---|---|---|---|
| Base relation | Raw sample stream | Signal / time series | Training set |
| Mergeable (monoid) | **Yes, by design** — associative + commutative merge is a defining property | **Partially** — Haar on aligned dyadic intervals merges cleanly; general DWT does not | **Rarely** — only linear / moment-based things (online PCA via covariance sums, linear regression normal equations, naive Bayes w/ conjugate priors). Neural nets are not monoids: training on A then B ≠ B then A |
| Incremental update cost | O(1) per sample | Amortized O(log n) for online / sliding DWT | Variable; SGD continuations risk catastrophic forgetting |
| Refresh cost | Cheap (replay stream) | Moderate (one DWT pass) | **Huge** — full retraining is why RAG exists as a workaround |
| Accuracy bound | **Provable, closed-form** from parameters (ε, δ, K, α, m) | Provable — L2 error bounded by discarded coefficient energy (Parseval) | **Empirical**, data-dependent; PAC / conformal bounds exist but are much weaker and narrower |
| Query classes answered | Fixed at design: count, sum, quantile, top-k, cardinality | Fixed: range sums, heavy hitters, point queries, wavelet-domain features | **Open-ended** — whatever the training objective was |
| View-definition formalism | An aggregation function + parameters | A basis transform + threshold | Training objective + architecture + hyperparameters + seed |

### 21.2 Wavelets — a sibling sketch family

Wavelets are essentially an alternative sketch family. The classical
AQP line of work
(Garofalakis, Gibbons, Matias, Vitter — "Approximate Query Processing
via Wavelets," VLDB 1998 onward) treats them as a direct alternative
to randomized sketches for range-sum and heavy-hitter workloads.

Compared to CMS / KLL:

- **Strength**: wavelets exploit signal structure. On smooth or
  low-entropy signals (diurnal telemetry, time-of-day patterns,
  histograms that concentrate on a few modes) thresholded wavelets
  produce dramatically smaller representations than sketches of
  comparable accuracy.
- **Weakness**: on high-entropy / uniform data their advantage
  disappears, because the thresholded coefficient set doesn't shrink.
- **Operational fit**: Haar wavelets on aligned dyadic intervals merge
  cleanly, which means a Haar-based agg could use the same
  `(agg_id, group_key, window)` storage as a CMS agg with only
  modest changes to the merge trait. Non-Haar wavelets would require
  either strict window alignment or a refresh-only maintenance
  strategy.

**Takeaway**: if a future `SketchType::Wavelet` were added, the
existing sketch-DB contracts (schema timeline, backfill, accuracy
profile, tier storage) generalize without structural change. The
`AccuracyProfile::ErrorBound` enum already has room for an
L2-energy-based variant.

### 21.3 ML models — MVs with weaker contracts

ML models fit the MV framing in the loose sense: they are precomputed,
queryable, compressed summaries of a base relation. But the sketch
DB's **four operational contracts** weaken or vanish:

- **Mergeability** (§7.3 cross-segment combine) — lost for
  non-linear models. Only linear or moment-based models (running
  PCA, linear regression normal equations, conjugate Bayes,
  streaming k-means coresets) retain a monoid structure and can
  meaningfully merge across segments or time ranges.
- **Closed-form accuracy bound** (§6.4) — lost. Replaced by
  empirical validation + sometimes conformal prediction bands. The
  `AccuracyProfile` contract would need a new `Empirical` variant
  that carries calibration data rather than a parameterized formula.
- **Deterministic rebuild** (§10.5) — weak. Training is
  conditionally deterministic (seed + hyperparams + batch order +
  hardware), but reproducing bit-identical outputs across hardware is
  a well-known open problem in ML engineering.
- **Cheap refresh** — gone. Refresh cost is the dominant operational
  concern for large models; the two-tier maintenance strategy
  (incremental + refresh) that makes sketch-DB reconfigure painless
  does not translate — continual training and fine-tuning are poor
  substitutes for sketch refresh.

The interesting **sub-class** that does fit is linear / additive
models:

- **Online PCA / streaming covariance** — monoid via covariance sum;
  bounded error via eigenvalue bounds. Functionally a sketch.
- **Linear regression (normal equations form)** — `XᵀX` and `Xᵀy`
  accumulators merge by summation; bounds follow from standard linear
  algebra. Functionally a sketch.
- **Coreset-based clustering** (BIRCH, k-means coresets) — mergeable
  by construction; accuracy is a multiplicative factor on the optimal
  clustering cost.
- **Naive Bayes with conjugate priors** — parameter updates are
  additive in sufficient statistics.

Each of these could be added to the sketch DB as a `SketchType`
variant without changing the core contract. They would share schema
timeline, backfill, accuracy profile, and tier storage with the
existing sketches.

Neural / tree-ensemble / LLM models would require a parallel design
with weaker contracts: no merge, refresh-only maintenance,
empirical-only accuracy. That's closer to a **model registry** than a
sketch DB, and the open research literature
(Kraska et al. — SageDB; Hilprecht et al. — DeepDB; Yang et al. —
NeuroCard; DBEst / DBEst++) explores exactly that split. A pragmatic
integration path would be: host model artifacts beside sketches under
the same `agg_id` lifecycle and HTTP surface, but use a separate
storage engine internally — the `SketchDb` facade
(see [`design-sketch-db-pluggable.md`](./design-sketch-db-pluggable.md))
makes this kind of backend swap mechanical.

### 21.4 Practical implication for the sketch DB design

The sketch DB's architecture — `agg_id` lifecycle, schema timeline,
backfill-from-base, accuracy-as-metadata — generalizes without change
to:

1. **Sketches** (today).
2. **Wavelets** (mostly; Haar is trivial, general DWT needs window
   alignment).
3. **Linear-ish ML summaries** (PCA, linear regression, coresets,
   conjugate-prior Bayes).

It does **not** generalize cleanly to neural / tree-ensemble models
without relaxing the mergeability and closed-form-bound contracts.
Keeping those relaxations out of the core, and introducing them in a
companion "model view" subsystem if the need arises, preserves the
properties that make the sketch DB's behavior predictable.
