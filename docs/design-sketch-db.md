# Design: Sketch DB — Storage Engine as a Refreshable Materialized-View DB

**Status:** Draft / exploration. No code in this PR implements this beyond the
hot-reload plumbing; the doc is here so the design is reviewable alongside the
hot-reload work it builds on.

**Audience:** anyone asking "if `SimpleMapStore` is going to be the long-term
sketch storage engine, what does it need to look like so that a changing
controller workload and a query engine sharing it don't step on each other?"

**Companion docs:**
- [`design-simple-map-store-persistence.md`](design-simple-map-store-persistence.md) — the current LSM-style parts-based persistence layer this builds on
- [DataCollector#153 pipeline query catalog](https://github.com/ProjectASAP/DataCollector/pull/153) — the pipeline-level dual-lane architecture (sketch lane + Gorilla/S3 archive lane) that this design depends on

---

## 1. Motivation

`SimpleMapStore` today is a KV store bucketed by `aggregation_id`. That was
enough for "write sketches, read them back by id." It is no longer enough once:

- The controller reconfigures sketches at runtime in response to a changing
  query workload (parameter upgrades, new query patterns, retirement).
- Queries should transparently work across those reconfigurations, with no
  data cliff at the upgrade boundary.
- The store needs to give the controller back real workload observations
  (bytes stored, query latency, merge cost) to drive cost-based planning.
- Persistent storage means there is no implicit cleanup mechanism — any data
  written to disk stays there until something explicitly retires it.

What's needed is a storage engine that treats sketches as first-class typed
values with per-type merge semantics, tracks schema per aggregation across
time, and supports refreshing itself from the archive when the schema
changes. In database terms: the sketches are **materialized views** over
the Gorilla/S3 archive; the store is a materialized-view engine specialized
for sketch types.

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

The base relation being materialized over is the stream of raw samples —
from the live agent stream in the normal case, and from the Gorilla/S3
archive when the view is being refreshed (§9).

### 2.2 What a materialized view framing buys us

| Classical MV concept | Sketch DB counterpart |
|---|---|
| View definition | `AggregationConfig` |
| Base relation | Raw sample stream (agent ingest + archive) |
| Materialization | `SketchEntry` rows in `SimpleMapStore` |
| View maintenance strategy | §8 (incremental) + §9 (refresh) |
| Schema evolution | §5 per-`agg_id` lifecycle, §6 schema timeline |
| `DROP MATERIALIZED VIEW` | Retirement + expiry |
| `REFRESH MATERIALIZED VIEW` | Backfill job from archive |
| `pg_matviews` / metadata catalog | `/api/v1/db/schemas`, §10 controller APIs |
| Index on the MV for faster queries | Label postings index (§4.3), typed aux columns (§4.1) |

The rest of this doc splits into two halves, mirroring the two standard
MV maintenance strategies:

- **§8. Incremental view maintenance** — the live ingest path, which
  updates the MV as new samples arrive. This is what the precompute
  engine already does today; the doc describes how it extends cleanly
  into a sketch-DB-aware design.
- **§9. Refreshable view maintenance** — the backfill service, which
  rebuilds the MV for a time range from the base relation (the archive).
  This is the piece that makes schema evolution painless.

Both strategies write to the same physical store and the same per-`agg_id`
namespace. The controller chooses between them based on workload and
reconfigure cost (§9.5).

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

## 3. Design principles (numbering scheme for the rest of the doc)

The section numbers below apply to the rest of the document after the
MV framing is established:

- §3 Design principles
- §4 Architecture
- §5 Storage schema
- §6 Per-`agg_id` schema and its lifetime
- §7 Schema timeline
- §8 Incremental view maintenance (live ingest)
- §9 Semantic compaction
- §10 Refreshable view maintenance (backfill)
- §11 Full reconfigure workflow
- §12 API surface
- §13 Implementation phases
- §14 Relationship to the hot-reload PR
- §15 Open questions

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
   a coarser window.
4. **Typed auxiliary columns.** `count`, `sum`, `min`, `max` are stored
   alongside the sketch bytes, not inside them. Queries that only need
   these scalar stats never pay the sketch deserialization cost.
5. **Archive is the source of truth.** Sketches are rebuildable from
   the Gorilla/S3 archive. The store supports explicit backfill from
   archive into any `agg_id` for any time range.
6. **Monotonic, non-reused `aggregation_id`s.** A reconfigure that
   changes parameters is always "retire old id + create new id", never
   "mutate in place." This is a contract on the controller; the store
   enforces it via a write-side schema barrier.

---

## 4. Architecture

```
             ┌─────────────────────────────────────────────────┐
             │  Controller                                      │
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
             │  │ Per-agg_id storage                      │    │
             │  │   pinned schema (type, params, group)   │    │
             │  │   LSM parts with semantic compaction    │    │
             │  │   label posting index                    │    │
             │  │   typed aux columns                      │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Backfill service                        │    │
             │  │   reads raw from Gorilla/S3 archive     │    │
             │  │   rebuilds sketches per (agg_id, window)│    │
             │  │   writes to per-agg_id storage          │    │
             │  └────────────────────────────────────────┘    │
             │                                                  │
             │  ┌────────────────────────────────────────┐    │
             │  │ Query Pushdown                           │    │
             │  │   timeline_for_metric(metric, range)    │    │
             │  │   per-segment dispatch to agg_ids       │    │
             │  │   merge + compute statistic in-store    │    │
             │  └────────────────────────────────────────┘    │
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

---

## 5. Storage schema

### 4.1 Per-entry record

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
    /// Written by the backfill service from archive raw data. The
    /// job_id lets the system correlate with the BackfillJob that
    /// produced it (for re-runs, debugging, idempotency).
    Backfilled { job_id: u64 },
}
```

Why `count`/`sum`/`min`/`max` are not inside the sketch:

1. `sum_over_time`, `count_over_time`, `max_over_time`, `min_over_time`
   are the overwhelming majority of production queries. They should
   never pay the sketch-deserialize cost.
2. These stats are already tracked losslessly by DataCollector's
   processors (they live in the typed `*SketchDataPoint` proto fields
   as first-class numbers). We are preserving that typing end-to-end
   instead of forcing them to travel through the sketch blob.

### 4.2 Primary key and secondary index

Two access patterns dominate:

| Pattern | Source | Key order |
|---|---|---|
| "all groups at time T" | batch jobs, controller workload scans | `(agg_id, window_start, group_key)` |
| "one group over time range" | dashboard queries, `quantile_over_time(…) by (svc)` | `(agg_id, group_key, window_start)` |

The store maintains the first as the primary index (SSTable sort order) and
the second as a secondary index. Primary order wins during compaction
(spatial locality across a single window close is the common ingest
pattern); secondary index is maintained via an auxiliary log that the
reader consults for single-group time scans.

### 4.3 Label posting index

For controller workload queries like "how many distinct services does
metric `X` have?" and for label filter pushdown (`service=auth`), a
per-`agg_id` label posting index maps:

```
label_postings[agg_id]: HashMap<(label_name, label_value), RoaringBitmap<group_key_hash>>
```

Roaring bitmaps keep this compact even for high-cardinality labels.
Compared to scanning every group, a label match becomes an O(1) bitmap
lookup.

---

## 6. Per-`agg_id` schema and its lifetime

### 5.1 `AggSchema` — the pinned metadata

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

### 5.2 Status transitions

```
┌─────────┐  controller POSTs new StreamingConfig
│ (none)  │  with this agg_id included
└────┬────┘
     │ create_schema()
     ▼
┌─────────┐  controller POSTs new StreamingConfig
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

### 5.3 Write-side schema barrier

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

---

## 7. Schema timeline — the key to query continuity

### 6.1 What it is

```rust
metric_timelines: HashMap<
    MetricName,
    BTreeMap<TimeRange, AggId>
>
```

For each metric, an ordered list of `(time_range, agg_id)` segments
covering its history. Non-overlapping by construction (the controller
contract: a metric has one Active agg_id at a time, plus zero or more
Retired overlapping within retention).

Example for metric `latency`:

```
│── id=1 (CMS256) ──┤
                    │── id=17 (CMS1024) ──┤
                                          │── id=42 (KLL200) ──►
T=0                 T=day1                T=day3              now
```

### 6.2 `timeline_for_metric(metric, t1, t2) -> Vec<(AggId, TimeRange)>`

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
nanoseconds.

### 6.3 Per-segment query dispatch

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
subject to further transformation only by semantic compaction (§9)
or by the full-refresh path (§10) replacing it. The MV row carries:

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
  (§10.4) and for idempotent re-runs.
- **Schema-validating write barrier** (§6.3) enforcing that the
  incoming write matches the target `agg_id`'s pinned schema. Today
  the write path implicitly trusts the router; the sketch DB makes
  the schema contract explicit at store entry.
- **Per-`agg_id` label postings update** as new group_key values
  appear (§5.3). Trivial — incremental update to a Roaring bitmap
  per first-seen group_key.

None of these change the fundamental data flow; they make the
incremental MV maintenance contract observable and enforceable.

---

## 9. Semantic compaction

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

## 10. Refreshable view maintenance — the backfill path

Where §8 describes how the MV is maintained as new base data arrives,
this section describes how the MV is **re-materialized** from the base
relation on demand. In SQL terms: `REFRESH MATERIALIZED VIEW agg_17
FOR PERIOD (now - 24h, now) FROM s3_archive`.

### 10.1 The core insight

Because the archive lane keeps raw data losslessly, we can rebuild any
sketch for any time range from the archive. A reconfigure that breaks
query continuity in the sketch tier can be followed by a backfill that
restores it.

```
T=0        T=upgrade                       now
│          │                                │
├──── id=1 (CMS256) data ────────┤          ← existing sketch data
│          │                                │
│          ├──── id=17 (KLL200) native ─┤  ← new sketch data from live ingest
│          │                                │
│    ┌─────┴────── backfill ──────┐         ← rebuild id=17 from archive
│    │     │                       │         for [T=upgrade - horizon, T=upgrade]
│    │     │                       │
│    ↓     ↓                       ↓
Archive (Gorilla / S3)
```

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

enum BackfillSource {
    /// Read raw samples from Gorilla-compressed S3 parts.
    S3Archive { bucket: String, prefix: String },
    /// Read raw samples from the exact backend DB.
    ExactDB { url: String },
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
as Backfill jobs complete.

### 10.5 Deterministic rebuild

For backfill to produce "the same sketch we would have built live,"
the sketch construction must be deterministic. That means:

- The hash function seed for CMS / CountSketch / HLL must be part of
  `AggregationConfig.parameters` and stable across Native and Backfill
  paths.
- The KLL / DDSketch sampling decisions must be deterministic from
  the input sample order. This constrains how raw samples are read
  from archive — they must be replayed in the same order they were
  ingested live.
- Archive writes from DataCollector therefore need to preserve ingest
  order within a window.

This is an invariant the Gorilla processor / S3 exporter must
guarantee. Not hard, but it has to be designed in.

### 10.6 When to use incremental vs refresh

The two maintenance strategies (§8, §10) are not alternatives — they
cover different situations and the controller should pick per
situation.

| Situation | Strategy | Why |
|---|---|---|
| Steady-state live ingest | Incremental (§8) | O(1) per sample, no scan of base relation |
| New `agg_id` created, no historical need | Incremental only | Nothing to refresh from |
| New `agg_id` created, need query continuity across the reconfigure | Incremental **+** Refresh from archive over the query horizon | Incremental covers \[now, future\]; refresh covers \[now-horizon, now\] |
| Recovery from a store corruption or a bug in past sketch builds | Refresh | MV is known wrong; rebuild authoritatively from base |
| Onboarding a metric with historical raw data already in archive | Refresh only (until catches up), then Incremental | Much cheaper than streaming a week of archive through the live ingest path |
| Metric with very low query rate, reconfigure | Incremental only, fall back to exact DB for historical queries | Refresh cost > fallback cost at low QPS |

The controller decides by comparing estimated costs:

```
cost_of_refresh = archive_bytes_to_scan * s3_read_$
                + cpu_seconds * cpu_$
cost_of_fallback = expected_queries_to_exact_DB_during_retention
                 * query_latency * query_$
if cost_of_fallback > cost_of_refresh: trigger refresh
```

The `/api/v1/db/cost_estimate` endpoint (§12) is what the controller
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

## 11. The full reconfigure workflow

```
T=0:     Config = {1: CMS(256)}
         DB schemas = {1: Active}
         Live: writing to id=1

T=swap:  Controller decides to upgrade to KLL(200).
         1. Allocates new id=17.
         2. Dual-write phase begins.
            POST /api/v1/streaming-config { 1, 17 }
            DB: schema 17 created Active; schema 1 still Active.
            IngestState routes to both id=1 and id=17.
            (Both sketches accumulating the same underlying samples.)

T=swap+Δ:
         3. Controller triggers backfill to close the historical gap
            for id=17:
            POST /api/v1/db/backfill {
              agg_id: 17,
              time_range: (swap - query_horizon, swap),
              source: S3Archive,
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
         4. Controller removes id=1 from StreamingConfig.
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

Every step is idempotent. If the controller crashes mid-workflow, the
DB state at any point is a valid state; the controller resumes from
wherever it left off by reading `/api/v1/db/schemas`.

---

## 12. API surface

### 10.1 Query-engine-facing API

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

`QueryResult`:

```rust
enum QueryResult {
    /// Single value (Count, Quantile, Sum, Min, Max, Cardinality).
    Scalar { value: f64, covered: Coverage },
    /// Time series — per-window value.
    Series { points: Vec<(u64, f64)>, covered: Coverage },
    /// TopK and similar multi-value answers.
    Vector { entries: Vec<(String, f64)>, covered: Coverage },
    /// Some or all of the requested range could not be served from
    /// sketches. Caller decides whether to fall back.
    Partial {
        served: QueryResult,
        missing: Vec<(u64, u64)>,
        reason: PartialReason,
    },
}
```

### 10.2 Controller-facing API

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

These are the primitives the controller uses to close its planning
loop. Without them the controller plans in the blind; with them it
can run cost-based optimization with real observations.

---

## 13. Implementation phases

This design is intentionally larger than one PR. Suggested rollout:

**Phase 1 — Typed aux columns + query pushdown (small, high-leverage).**
Add `count` / `sum` / `min` / `max` as typed columns on each
`SketchEntry`. Implement `query_range_statistic` that scans the index
and computes these scalars without deserializing sketch bytes. No
schema timeline yet, no backfill yet.

**Phase 2 — Per-`agg_id` schema + write barrier.**
Introduce `AggSchema` with Active/Retired/Expired states. Wire the
ArcSwap config-swap handler to create/retire schemas. Add
`is_writable(agg_id)` barrier on the write path.

**Phase 3 — Schema timeline + metric-based query dispatch.**
Build the `metric_timelines` index. Change the query engine's primary
entry point from "by agg_id" to "by metric." Implement per-segment
dispatch with `combine_statistic` for combinable cases.

**Phase 4 — Semantic compaction.**
Add level-aware compaction with sketch-type merge dispatch. Policy
table per `AggStatus`.

**Phase 5 — Backfill service.**
Independent worker pool. `write_backfilled_window` bypass of
WindowManager. `Coverage` tracking. Controller-facing `/backfill`
endpoints.

**Phase 6 — Controller-facing metadata APIs.**
`/stats`, `/timeline`, `/cost_estimate`, `/pressure`. This is what
lets the controller's planner use real observations.

**Phase 7 — Secondary indexes.**
Label postings for fast filter pushdown. Roaring bitmaps per
`(agg_id, label_name, label_value)`.

Phase 1 is largely orthogonal to the others and can ship as an
isolated improvement. Phases 2–3 are the conceptual core — everything
else builds on them. Phases 4–7 are optimizations.

---

## 14. Relationship to the hot-reload PR (PR #16)

PR #16 gets the runtime-reload machinery right: `ArcSwap` is shared,
all consumers read the current config at the same instant, orphaned
`GroupState` entries are evicted after their windows drain. It
establishes the monotonic-`aggregation_id` + time-TTL contract that
this design assumes.

PR #16 does **not** implement any of the sketch DB architecture
described here. In PR #16:

- There is no `AggSchema` metadata object; the store relies on
  `AggregationConfig` being in the current `StreamingConfig` plus
  time-based TTL to handle retirement.
- There is no schema timeline; queries that span a reconfigure
  boundary see a data cliff at the swap point. This is the known
  query-continuity gap that motivates Phase 3 / Phase 5 of this
  design.
- There is no backfill; once a new agg_id is created, its historical
  coverage grows from zero in real time. The user's intuition that
  backfill from archive is the right answer is captured here as the
  eventual design target but is not implemented yet.
- The storage layer still treats everything by `(agg_id, window,
  group_key)` — no sketch-aware compaction, no typed aux columns, no
  label postings.

So PR #16 is the **foundation** — hot-reload is a hard prerequisite
for everything above — but the sketch DB design is a much larger
program of work.

---

## 15. Open questions

1. **Where do backfill-source hash seeds live?** To make Backfilled
   sketches byte-identical to what live ingest would have produced,
   the hash function seeds need to be reproducible. Proposal: include
   them in `AggregationConfig.parameters`. Requires a small
   sketchlib-go / sketchlib-rust change to accept an external seed.

2. **What does the controller do when a backfill fails partway
   through?** Proposal: job_id is idempotent; controller retries
   with exponential backoff; if persistent failure, proceed without
   the backfilled range (query engine falls back for that subrange).

3. **How much archive retention is needed?** Archive retention must
   be ≥ the longest `backfill horizon` the controller ever requests,
   which is the longest query range users will issue that spans a
   reconfigure. If archive retention is 7 days and queries never look
   back more than 24h, that's fine. Needs to be tracked as a
   deployment-level configuration.

4. **Can an in-progress backfill be query-visible with partial
   coverage?** Proposal: yes — `Coverage::BackfillInProgress` is a
   real state. The query engine can wait (if ETA is short), combine
   partial sketch with partial fallback, or serve from fallback only.
   Trade-off knob on the query level.

5. **Does the current `SimpleMapStore` schema support all of Phase 1
   without disk migration?** Mostly. `count`/`sum`/`min`/`max` can be
   written as separate columns in the existing part format; readers
   that don't know about them can skip. The label posting index
   (Phase 7) needs a new on-disk structure and will require a new
   part version.

6. **Relationship to PromSketch?** PromSketch (the exponential-
   histogram-based in-memory sketch store) serves a different tier
   (live queries against raw samples). The sketch DB described here
   serves precomputed materialized sketches. The query engine's
   routing logic picks between them: PromSketch for recent fine-grained
   live queries, sketch DB for long-horizon precomputed queries,
   exact fallback for anything else. All three should share the
   schema-timeline / backfill concepts so that the controller can
   reason about them uniformly.
