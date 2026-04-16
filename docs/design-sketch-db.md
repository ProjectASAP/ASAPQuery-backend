# Design: Sketch DB — Storage Engine as a Refreshable Materialized-View DB

**Status:** Draft / exploration. No code in this PR implements this beyond the
hot-reload plumbing; the doc is here so the design is reviewable alongside the
hot-reload work it builds on.

**Audience:** anyone asking "if `SimpleMapStore` is going to be the long-term
sketch storage engine, what does it need to look like so that a changing
controller workload and a query engine sharing it don't step on each other?"

**Companion docs:**
- [`design-simple-map-store-persistence.md`](design-simple-map-store-persistence.md) — the current LSM-style parts-based persistence layer this builds on
- [`promsketch-integration.md`](../asap-query-engine/docs/promsketch-integration.md) — the PromSketch subsystem, retrospectively positioned by this doc as Tier 1 of the sketch DB (see §4.1)
- [DataCollector#153 pipeline query catalog](https://github.com/ProjectASAP/DataCollector/pull/153) — the pipeline-level dual-path architecture (sketch path + exact-DB path) that this design depends on

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
| `pg_matviews` / metadata catalog | `/api/v1/db/schemas`, §15 controller APIs |
| Index on the MV for faster queries | Label postings index (§5.3), typed aux columns (§5.1) |

The rest of this doc splits into two halves, mirroring the two standard
MV maintenance strategies:

- **§8. Incremental view maintenance** — the live ingest path, which
  updates the MV as new samples arrive. This is what the precompute
  engine already does today; the doc describes how it extends cleanly
  into a sketch-DB-aware design.
- **§9. Refreshable view maintenance** — the backfill service, which
  rebuilds the MV for a time range from the base relation (the exact DB).
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
- §4 Architecture (§4.1 storage tiers)
- §5 Storage schema
- §6 Per-`agg_id` schema and its lifetime
- §7 Schema timeline
- §8 Incremental view maintenance (live ingest)
- §9 Semantic compaction
- §10 Refreshable view maintenance (backfill)
- §11 Admission control and isolation
- §12 Observability and debugging
- §13 Rollout and migration
- §14 Full reconfigure workflow
- §15 API surface
- §16 Implementation phases
- §17 Relationship to the hot-reload PR
- §18 Open questions

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
5. **Exact DB is the source of truth.** Sketches are rebuildable from
   the exact-DB base relation (whichever backend is configured — S3 +
   Gorilla, Prometheus, ClickHouse, VictoriaMetrics, etc.). The store
   supports explicit backfill from the exact DB into any `agg_id` for
   any time range. The exact DB is the same storage that also serves
   the query-engine fallback path when a query cannot be answered from
   sketches.
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
controller.

| Tier | Implementation | Compaction mechanism | Retention | Query latency | Use case |
|---|---|---|---|---|---|
| **Tier 1** | PromSketch — in-memory EH-backed sketches over raw samples | **Continuous temporal compaction via the EH bucket structure itself** — fine-grained buckets for recent data, exponentially coarser buckets for older data, merged in place as windows age | seconds to minutes (bounded by memory) | sub-millisecond | live dashboards, alerts; **especially good when many sub-window queries target the same series** — e.g. a dashboard that asks for p50/p95/p99 over 1m, 5m, 15m, 1h all against the same metric. Tier 1 serves all of them from one EH structure with no redundant storage; Tier 2 would store one pre-merged sketch per (agg_id, window) and either duplicate the series across multiple agg_ids or rely on read-time merge. |
| **Tier 2** | Precompute engine + `SimpleMapStore` LSM parts (memory + disk) | **Batched semantic compaction** via LSM levels — §9 — N entries at level L merged into one at level L+1 at a coarser window | minutes to weeks (bounded by `persistence_delete_older_than_secs`) | milliseconds | most production queries, longer-horizon analysis |
| **Exact DB** | S3 + Gorilla / Prometheus / VictoriaMetrics / ClickHouse | Storage-native compression (Gorilla / columnar); no sketch-level merging | months+ (configurable, bounded by raw storage cost) | seconds to tens of seconds | fallback for uncovered queries, base relation for refresh |

The two sketch tiers implement the **same abstract concept** — "reduce
temporal resolution over time while preserving sketch-accuracy
bounds" — but at different points along a latency/complexity curve.
Tier 1 does it continuously in memory via EH bucket merges as windows
age; Tier 2 does it in background compaction with explicit LSM levels.
The controller picks per-`agg_id` which mechanism matches the
workload: sub-ms live queries with short retention → Tier 1; weeks of
queryable history → Tier 2; both → both.

**Tier selection per `agg_id`.** A metric's `AggregationConfig` carries
a `tier` field: `Tier1Only`, `Tier2Only`, or `Both`. `Both` means
incremental maintenance writes to both tiers — Tier 1 for freshness,
Tier 2 for long retention. Query engine picks per-query based on the
query's time range and SLA.

**Tier promotion / demotion on reconfigure.** The controller can
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
- One controller API surface (§15) exposes stats across tiers.
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
    /// Written by the backfill service from exact-DB raw data. The
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

This section describes **Tier 2's** semantic compaction — the
batched, LSM-level merge process that runs on disk-backed parts.
Tier 1 (PromSketch) implements the *same concept* differently:
continuous in-memory EH bucket merging, done inline as windows age,
without an explicit level structure. Tier 1 is especially effective
when the same series is queried over many different sub-windows
(§4.1) because the EH lets every such query reuse the same in-memory
structure. The rest of this section is Tier 2 specific.

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
  from the exact DB — they must be replayed in the same order they
  were ingested live.
- Exact-DB writes from the live data plane therefore need to preserve
  ingest order within a window (whether the exact DB is S3+Gorilla,
  Prometheus, or any other backend).

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
| New `agg_id` created, need query continuity across the reconfigure | Incremental **+** Refresh from exact DB over the query horizon | Incremental covers \[now, future\]; refresh covers \[now-horizon, now\] |
| Recovery from a store corruption or a bug in past sketch builds | Refresh | MV is known wrong; rebuild authoritatively from base |
| Onboarding a metric with historical raw data already in the exact DB | Refresh only (until catches up), then Incremental | Much cheaper than streaming a week of historical data through the live ingest path |
| Metric with very low query rate, reconfigure | Incremental only, fall back to exact DB for historical queries | Refresh cost > fallback cost at low QPS |

The controller decides by comparing estimated costs:

```
cost_of_refresh = exact_db_bytes_to_scan * read_$_per_byte
                + cpu_seconds * cpu_$
cost_of_fallback = expected_queries_to_exact_DB_during_retention
                 * query_latency * query_$
if cost_of_fallback > cost_of_refresh: trigger refresh
```

The `/api/v1/db/cost_estimate` endpoint (§15) is what the controller
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

## 11. Admission control and isolation

The sections above treat the sketch DB as if memory, CPU, and I/O were
unbounded. Production deployments are the opposite — a noisy agg_id
must not be able to starve others. This section enumerates what has to
be in place for tier storage and backfill to be production-safe.

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

Policy is per-agg, set by the controller:

| Policy | Semantics | When to use |
|---|---|---|
| `Reject` | New group writes return error; existing groups continue | Exact-correctness metrics; controller can react by upgrading quota or retiring the agg |
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

§10.3 already covers separate worker pools. Additional requirements:

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

These are hard caps. The controller treats them as signals for
planning (e.g. don't plan a backfill if the exact-DB read budget is
saturated by other ongoing jobs).

---

## 12. Observability and debugging

A storage engine without visibility is unusable in production. This
section enumerates what must be observable and through what surface.

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
controller's `/api/v1/db/stats/*` endpoints (the controller reads
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
       └─ Span: tier2_query (SimpleMapStore)
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
{"event": "config_swap",        "added": [17], "removed": [1], "by": "controller-a"}
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

Operators and the controller use this to understand why a query
went where it went.

---

## 13. Rollout and migration

Every phase in §16 (implementation phases) crosses a live deployment.
This section lists the invariants that let each phase roll out
incrementally without breaking existing users.

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

## 14. The full reconfigure workflow

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

## 15. API surface

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

## 16. Implementation phases

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

## 17. Relationship to the hot-reload PR (PR #16)

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
  backfill from the exact DB is the right answer is captured here as the
  eventual design target but is not implemented yet.
- The storage layer still treats everything by `(agg_id, window,
  group_key)` — no sketch-aware compaction, no typed aux columns, no
  label postings.

So PR #16 is the **foundation** — hot-reload is a hard prerequisite
for everything above — but the sketch DB design is a much larger
program of work.

---

## 18. Open questions

1. **Where do backfill-source hash seeds live?** To make Backfilled
   sketches byte-identical to what live ingest would have produced,
   the hash function seeds need to be reproducible. Proposal: include
   them in `AggregationConfig.parameters`. Requires a small
   sketchlib-go / sketchlib-rust change to accept an external seed.

2. **What does the controller do when a backfill fails partway
   through?** Proposal: job_id is idempotent; controller retries
   with exponential backoff; if persistent failure, proceed without
   the backfilled range (query engine falls back for that subrange).

3. **How much exact-DB retention is needed?** Exact-DB retention
   must be ≥ the longest `backfill horizon` the controller ever
   requests, which is the longest query range users will issue that
   spans a reconfigure. If exact-DB retention is 7 days and queries
   never look back more than 24h, that's fine. Needs to be tracked
   as a deployment-level configuration — and the controller should
   reject any upgrade plan whose backfill horizon exceeds current
   exact-DB retention.

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

6. **Relationship to PromSketch?** *(Resolved — see §4.1.)*
   PromSketch is **Tier 1** of the sketch DB, not a parallel system.
   It's an in-memory EH-backed storage backend specialized for
   short-retention sub-millisecond queries. Tier 2 is the
   precompute + LSM parts store for longer retention. The two share
   one schema lifecycle, one controller API surface, one refresh
   path, and one query-engine routing layer. The controller picks
   per-`agg_id` whether to materialize into Tier 1, Tier 2, or both.

7. **Agent clock skew.** `time_unix_nano` on every sample is
   agent-local. Agents with skewed clocks produce windows that don't
   align across agents; cross-agent merge silently blends samples
   that fall into different "real" time buckets. Worse, skew is
   frozen into both live sketches and exact-DB data, so even refresh
   doesn't fix it. Open questions: should the backend reject or
   correct samples with timestamps too far from wall clock? Should
   skew be surfaced as per-agent metadata the controller can see?
   Probably a hard NTP-sync requirement on agents with metrics
   exposing skew per agent.

8. **Controller state and HA.** The monotonic `aggregation_id`
   counter has to live somewhere that survives controller restarts.
   Options: controller persists its own state (requires a DB for
   the controller); controller reads `max(agg_id)` from backend's
   `/api/v1/db/schemas` at startup (simple but needs CAS for
   multi-controller-replica HA to avoid two controllers allocating
   the same id). Also: who wins if two controllers disagree on
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
