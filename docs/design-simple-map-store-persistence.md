# Design: SimpleMapStore Persistence (Memory Limit + Disk Flush)

## Problem

`SimpleMapStore` (`asap-query-engine/src/stores/simple_map_store/`) is currently an
in-memory-only store. Under long-running ingest it grows unboundedly: every sealed
window for every `(aggregation_id, group_key)` is held in `DashMap<u64, RwLock<StoreKeyData>>`
until one of the three existing `CleanupPolicy` variants (`CircularBuffer`,
`ReadBased`, `NoCleanup`) either rotates it out and drops it on the floor or does
nothing at all.

All three of those variants are **destructive** — they delete data, they do not
persist it. That creates two problems:

1. **No memory bound that preserves data.** A deployment has to either
   overprovision RAM (`NoCleanup`), throw away potentially query-relevant data
   (`CircularBuffer` / `ReadBased`), or tune per-agg `num_aggregates_to_retain`
   values that don't correspond to any operator-meaningful quantity.
2. **No durability.** Cold data (older than the query working set) still occupies
   RAM even though most queries hit only the last few minutes.

We want `SimpleMapStore` to replace the existing cleanup-policy knob with a
single persistence policy driven by **two** knobs, in priority order:

1. **Primary — memory budget.** A configurable hard ceiling on in-memory sketch
   bytes. When exceeded, the oldest sealed epochs flush to disk until usage is
   back under a low-water mark. This is what actually bounds RAM in production.
2. **Secondary — time watermark T.** A configurable "hot window." Any sealed
   epoch whose end is older than `now - T` flushes to disk even if the store
   is nowhere near the memory budget. This guarantees predictable durability
   and a stable hot-set size under light load.

Flushed sketches are read back transparently at query time.

Scope is **single-node, single-process**. Replication, sharding, compression,
and query pushdown into segments are explicitly out of scope for v1. The three
existing destructive `CleanupPolicy` variants are removed from `SimpleMapStore`
(the enum stays in `asap_types` for any other store that still uses it).

---

## Current shape (relevant facts)

- `SimpleMapStorePerKey` (`per_key.rs:160`) keeps per-agg-id state in
  `DashMap<u64, Arc<RwLock<StoreKeyData>>>`.
- Each `StoreKeyData` has a `current_epoch` (actively being written) and
  `sealed_epochs: BTreeMap<epoch_id, Epoch>` (`per_key.rs`).
- Values are `Arc<dyn AggregateCore>`. All concrete accumulators already implement
  `SerializableToSink` (`serialize_to_bytes`, `merge_with`) — so we already have
  a serialization primitive and a merge primitive.
- Insert hot path: `insert_precomputed_output_batch` → `insert_for_store_key`
  (`per_key.rs:261`), holding only the per-agg-id `RwLock::write`.
- Query hot path: `query_precomputed_output{,_exact}` iterates
  `current_epoch` + `sealed_epochs` under `RwLock::read`.
- `CleanupPolicy` (`asap_types::enums`) currently has three destructive variants
  (`CircularBuffer`, `ReadBased`, `NoCleanup`); `SimpleMapStore` will stop
  taking a `CleanupPolicy` at all and use the new persistence config instead.
  The enum itself stays in `asap_types` for other stores.

Key observation: **`current_epoch` is the only mutable region**. Sealed epochs are
append-only until cleanup. That is exactly the right unit to flush.

---

## Design

### Unit of flush: the sealed epoch

A `SimpleMapStore` segment on disk corresponds to **one sealed epoch of one
aggregation id**. Rationale:

- Sealed epochs are immutable — safe to serialize without coordinating with writers.
- The epoch already has a well-defined time range, which is exactly what range
  queries want to filter on.
- Flushing an epoch only requires the per-agg-id `RwLock::write` briefly — same
  lock the insert path already uses, so no new contention class.
- Recovery and query planning only need epoch-level metadata, not per-window.

The `current_epoch` is never flushed while hot. It becomes flushable the moment
the rotator seals it.

### Disk layout

```
<disk_path>/
├── manifest.json                 # authoritative index of all segments
├── agg_00000042/
│   ├── seg_0000000001.bin        # one sealed epoch, serialized
│   ├── seg_0000000002.bin
│   └── ...
└── agg_00000043/
    └── seg_0000000001.bin
```

**Segment file format** (`seg_*.bin`):

```
[u32 magic][u16 version][u16 flags]
[u64 epoch_id][u64 window_start_ms][u64 window_end_ms]
[u32 num_entries]
repeated num_entries times:
  [u64 start_ts][u64 end_ts][u32 label_id][u8 agg_type]
  [u32 payload_len][payload_len bytes: serialize_to_bytes()]
[u32 crc32 of body]
```

Fixed-size header lets us mmap and binary-search by timestamp without parsing
payloads. Body is a linear scan — v1 does not build an in-segment index because
sealed epochs are small (bounded by window size × group count for a single agg).

**Manifest** (`manifest.json`):

```json
{
  "version": 1,
  "segments": [
    {"agg_id": 42, "epoch_id": 1, "path": "agg_00000042/seg_0000000001.bin",
     "start_ms": 1_700_000_000_000, "end_ms": 1_700_000_060_000,
     "num_entries": 120, "size_bytes": 48192}
  ]
}
```

The manifest is the single source of truth for which segments exist and what
ranges they cover. It is rewritten atomically (`write → fsync → rename`) after
every flush batch. Individual segment files are written+fsynced before the
manifest ever references them, so a crash mid-flush leaves orphan files that
startup sweeps away — never a dangling manifest entry.

### Memory accounting

Add a new trait method:

```rust
pub trait AggregateCore: ... {
    fn approx_memory_bytes(&self) -> usize;
}
```

Implementations are cheap per-type estimates (e.g. KLL: `k * 8 + overhead`;
SumAccumulator: `size_of::<Self>()`; SetAggregator: `len * avg_entry_bytes`).
They do not call `serialize_to_bytes` — that would be too expensive on the
insert path.

The store tracks a single `AtomicUsize` `mem_bytes_in_use`. On insert it adds
`approx_memory_bytes()` per entry; on flush it subtracts the same. This is an
estimate, not a hard guarantee — good enough to drive policy, and the alternative
(exact heap accounting) is not worth the allocator coupling.

### Configuration

New struct, threaded through `PrecomputeEngineConfig` and loaded from the
same YAML / controller channel as the existing streaming config. The two
knobs match the priority order in the problem statement: **memory budget
first, time watermark second**.

```rust
pub struct SimpleMapStorePersistenceConfig {
    // ---- Primary: memory budget ----
    //
    // High-water mark. When the store's tracked in-memory sketch bytes
    // exceed this, the background flusher evicts sealed epochs
    // oldest-first (globally, by epoch end_ms) until usage drops below
    // `memory_low_watermark_bytes`. This is the knob that bounds RAM
    // in production.
    pub memory_limit_bytes: usize,
    pub memory_low_watermark_bytes: usize,

    // Hard ceiling. If memory usage reaches this *during* an insert
    // (flusher is falling behind), the insert path blocks on a condvar
    // until the flusher catches up. Set to memory_limit_bytes * 1.25
    // as a sensible default.
    pub hard_cap_bytes: usize,

    // ---- Secondary: time watermark T ----
    //
    // Hot-window length. Any sealed epoch whose end_ms is older than
    // `now - hot_window_ms` is flushed on the next flusher tick, even
    // if the store is well under `memory_limit_bytes`. This guarantees
    // durability and a predictable hot-set size under light ingest.
    // None disables time-based flushing (not recommended — memory
    // pressure alone will still work, but cold data will linger in RAM
    // until something pushes it out).
    pub hot_window_ms: Option<u64>,

    // ---- Misc ----
    pub flush_interval_ms: u64,   // cadence of the background flusher
    pub disk_path: PathBuf,       // root dir for segments + manifest
}
```

`SimpleMapStorePerKey::new` now takes a `SimpleMapStorePersistenceConfig`
instead of a `CleanupPolicy`. There is no "persistence disabled" escape
hatch — this is now the only cleanup mechanism this store has. If someone
wants the old in-memory-only behavior, they can set `hot_window_ms = None`
and `memory_limit_bytes = usize::MAX`, which degenerates to "never flush."

### Eviction order

There is only one ordering — **oldest-sealed-epoch-first, globally by epoch
`end_ms`**. Both triggers (memory pressure and time watermark) pull from the
same ordered view, so the flusher never has two disagreeing notions of "oldest."

Rationale:

- Matches the time-window access pattern: queries overwhelmingly target recent
  windows, so evicting oldest is the lowest-regret choice.
- Makes the two triggers composable: the memory-pressure pass and the
  time-watermark pass are just two different stopping conditions on the same
  iterator over `(agg_id, epoch) sorted by epoch.end_ms`.
- Avoids cross-agg fairness debates (e.g., `LargestAggFirst`) that would
  otherwise complicate v1; we can add more orderings later behind an enum if
  it becomes necessary.

### Background flusher

A dedicated Tokio task owned by the store, started in
`SimpleMapStorePerKey::new`. Each tick, it checks the primary trigger
(memory) first, then the secondary trigger (time watermark):

```
loop {
    sleep(flush_interval_ms).await;

    let now = now_ms();
    let mut candidates = Vec::new();

    // Phase 1 (PRIMARY): memory budget.
    // If we're over the high-water mark, pull oldest sealed epochs
    // (by epoch.end_ms) until projected memory drops below the
    // low-water mark. This is the knob that actually bounds RAM.
    if mem_bytes_in_use.load() > cfg.memory_limit_bytes {
        candidates.extend(collect_oldest_until_under_low_water(
            cfg.memory_low_watermark_bytes,
        ));
    }

    // Phase 2 (SECONDARY): time watermark T.
    // Any sealed epoch older than `now - hot_window_ms` that wasn't
    // already picked up in phase 1 is flushed here. Under light ingest,
    // this is the only phase that runs and it keeps the hot set bounded
    // by T × ingest rate regardless of the memory budget.
    if let Some(hot_window) = cfg.hot_window_ms {
        candidates.extend(collect_epochs_older_than(now - hot_window));
    }

    // Dedup (phase 1 and phase 2 can pick the same epoch) and sort by
    // epoch.end_ms ascending so we flush oldest first within the batch.
    candidates.sort_unstable_by_key(|c| c.end_ms);
    candidates.dedup();

    for (agg_id, epoch_id) in candidates {
        flush_and_evict(agg_id, epoch_id).await?;
    }

    manifest.commit().await?;  // atomic rewrite after the batch
}
```

Under memory pressure, phase 1 dominates and phase 2 usually finds nothing
left to do (the oldest epochs are already gone). Under light ingest, phase 1
is a no-op and phase 2 does all the work. The two phases never fight because
they pull from the same oldest-first ordering.

`flush_and_evict` takes advantage of a property that matters a lot for the
flusher design: **sealed epochs are append-only and frozen.** Once the
rotator seals an epoch, no writer will ever touch its contents again — it
is only read (by queries) or removed wholesale (by the flusher). That
immutability is what lets the flusher stay completely off the critical
path:

1. Take the per-agg `RwLock::read` briefly, clone the `Arc<Epoch>` for the
   target epoch out of `sealed_epochs`, drop the lock.
2. Serialize the epoch, write the segment file, and fsync — **entirely
   outside any store lock**, on the flusher's own thread. Nothing in the
   system is waiting on this I/O. Inserts continue to land in
   `current_epoch`; queries continue to read from the still-in-place
   `sealed_epochs` entry (and the cloned `Arc` keeps the bytes alive for
   any query that happens to hold a reference already); the rotator
   continues to seal new epochs behind us.
3. Once the segment is durable and the manifest is updated, take the
   per-agg `RwLock::write` briefly to splice the epoch out of
   `sealed_epochs` and decrement `mem_bytes_in_use`. This is O(1) — a
   `BTreeMap::remove` plus an atomic subtraction — and is the only lock
   the flusher holds for more than a read snapshot.

Because steps 2 and 3 are decoupled by the `Arc<Epoch>` clone, **no lock
is ever held across disk I/O**, and the flusher never blocks anything on
the insert or query path beyond the two brief lock acquisitions at the
start and end.

#### Sync vs. async flush I/O — resolved

The previous revision left this as an open question. With the append-only
property made explicit, the answer is clear: **plain `std::fs` on a
dedicated `std::thread` is what we ship.** No `tokio::fs`, no Tokio
runtime for the flusher.

The only argument for async I/O would be "we need to yield the thread
while `fsync` is in flight so some other task on the same runtime can
make progress" — and there is no such other task. The flusher thread has
exactly one job — flush — and blocking it on `write` + `fsync` is fine
because:

- **Inserts never wait on the flusher.** Inserts land in `current_epoch`
  with no coordination with flush state; memory accounting is an atomic,
  not a lock. The flusher and the insert path only share the per-agg
  `RwLock`, and the flusher only holds it during the two brief windows
  above.
- **Queries never wait on the flusher.** In-memory reads take the per-agg
  `RwLock::read`, which contends with the flusher only during those same
  brief windows; disk reads go through the manifest lock, which is
  independent.
- **Back-pressure is the right answer to a slow disk.** If the flusher
  genuinely cannot keep up and memory hits `hard_cap_bytes`, the insert
  path blocks on a condvar until the flusher catches up. That is the
  correct behavior regardless of whether the I/O underneath is sync or
  async — making it async would not let more inserts through, it would
  just change which thread was parked.

Sync I/O keeps the store out of Tokio's executor entirely, keeps stack
traces readable, and eliminates a class of "why is my future not making
progress" failure modes. The flusher thread is `std::thread::spawn`'d in
`SimpleMapStorePerKey::new` and joined in `close`, with a `shutdown`
flag checked on each loop iteration.

### Query path

`query_precomputed_output` becomes a three-way merge:

1. Read from `current_epoch + sealed_epochs` as today.
2. Look up segments in the manifest whose `[start_ms, end_ms]` overlaps the
   query range for this `agg_id`.
3. For each matching segment, read the file (via the read-side segment cache
   described below), decode entries whose window overlaps, and merge into the
   result.

Segment reads happen under a read lock on the manifest; they do **not** take any
per-agg store lock, so they can run fully in parallel with inserts. Merging reuses
the existing `TimestampedBucketsMap` + `AggregateCore::merge_with` that the
in-memory query path already uses — no new merge logic.

### Read-side segment cache (two-tier memory model)

So far the flusher treats "is this sketch in RAM?" as a pure function of
*write* state — time of ingest and write-side memory pressure. That is the
right default for a TSDB, because recency dominates query patterns, but it
leaves one real gap: **cold-but-repeatedly-queried** segments. Think of a
dashboard that scans "last Tuesday's incident" every time the on-call opens
it, or a recording rule that re-reads a fixed 24h historical range every
minute. Those queries touch segments that the time watermark has correctly
decided are cold, and under the design so far they pay full disk I/O on
every hit.

The answer is **not** to let query frequency feed back into the flusher
policy. Doing that would couple write-path retention to read load, break
the monotonic "once cold, stays cold" invariant the flusher relies on, and
introduce unbounded-memory failure modes when a query sweeps everything.
The answer is a **second, separate memory tier** that exists purely as a
read-side cache on top of the disk layer.

**Tier 1 — authoritative hot (write-driven).** Bounded by
`memory_limit_bytes` + `hot_window_ms`. Contains `current_epoch` and any
sealed epoch that has not yet been flushed. Source of truth for recent
data. Managed by the flusher described above.

**Tier 2 — read-side segment cache (query-driven).** Bounded by a
separate `segment_cache_bytes` budget. Contains decoded copies of segments
pulled back from disk by the query path. Source of truth is always the
segment file — the cache is a pure optimization, drop-anytime, never
dirty. Managed by the query path, not the flusher.

```rust
pub struct SimpleMapStorePersistenceConfig {
    // ... existing fields ...

    // Read-side segment cache. Bounded independently of
    // `memory_limit_bytes`; this budget is for decoded segments the query
    // path pulls back from disk, not for the authoritative hot set.
    // Set to 0 to disable. Default: small (e.g. 64 MiB), opt-in for
    // workloads that don't need it.
    pub segment_cache_bytes: usize,
}
```

**Why a TSDB specifically benefits from this shape:**

1. **Recency and query frequency overlap ~90%.** Tier 1 already catches
   everything a "rate over last 5m" workload wants pinned. The cache
   only earns its budget on the residual workload — dashboards on fixed
   old ranges, recording rules over long horizons. Making it a separate,
   sized-independently tier means we can ship a small default (or zero)
   and only budget it up for workloads that measurably need it.

2. **Monotonic tiering.** Once a sealed epoch is flushed, it stays on
   disk. A query may cache a decoded copy in Tier 2, but the flusher
   never "un-flushes" it back into Tier 1. This preserves the property
   that Tier 1 is purely a function of write state — which is what makes
   the flusher simple enough to implement correctly.

3. **Segment granularity, not sketch granularity.** The unit of disk I/O
   is the segment file, so the cache must match that granularity.
   Caching individual sketches inside a segment would mean partial reads
   and complex invalidation; caching whole segments is a trivial
   `LruCache<SegmentId, Arc<DecodedSegment>>`.

4. **Two independent budgets are easier to tune than one unified priority
   score.** Operators reason about "how much RAM does write buffering
   need?" and "how much RAM does read caching need?" separately. A
   unified `priority = α * recency + β * frequency` score is harder to
   explain and harder to debug when it misbehaves.

**Eviction policy for Tier 2.** A plain LRU is the v1 default. If we see
recurring cold queries that get evicted by unrelated one-shot scans, we
can move to SLRU or TinyLFU later — both are drop-in replacements because
the cache has no consistency obligations. The cache should expose
hit/miss counters in `StoreDiagnostics` so we have data for that call.

**Interaction with the flusher.** None. The flusher only sees Tier 1.
The read cache has no feedback into retention decisions. This is the
whole point of splitting the tiers.

**What this deliberately does not do:**

- No pinning of individual sketches in Tier 1 based on read counts. The
  existing `read_counts` field on `StoreKeyData` becomes purely diagnostic
  for this store — it does not veto flushes.
- No promotion from Tier 2 back into Tier 1. Once cold, stays cold.
- No partial-segment loading. Segments are cached whole or not at all.

### Recovery on startup

1. Open `disk_path`, read `manifest.json`.
2. For every referenced segment, stat the file and validate magic + CRC header.
   Missing / corrupt segments are logged and removed from the manifest.
3. Sweep `disk_path` for segment files not referenced in the manifest (orphans
   from a mid-flush crash) and delete them.
4. Build the in-memory segment index; do **not** load any sketches into RAM.
   Cold data stays cold until a query asks for it.

### Concurrency summary

| Path              | Lock taken                                  |
|-------------------|---------------------------------------------|
| Insert            | per-agg `RwLock::write` (unchanged)         |
| Query in-memory   | per-agg `RwLock::read` (unchanged)          |
| Query disk        | manifest `RwLock::read` + segment-cache mutex |
| Flush: serialize  | none (reads immutable sealed `Arc`)         |
| Flush: evict      | per-agg `RwLock::write` (short, O(1) splice)|
| Flush: commit     | manifest `RwLock::write` (short)            |

No new lock held across a fsync or disk I/O.

---

## What this does **and does not** change

Changes:

- `SimpleMapStorePerKey::new` no longer takes a `CleanupPolicy`; it takes a
  `SimpleMapStorePersistenceConfig`. The three destructive cleanup variants
  (`CircularBuffer`, `ReadBased`, `NoCleanup`) are no longer wired into this
  store at all. The code paths in `per_key.rs` that branch on
  `CleanupPolicy` (`cleanup_old_aggregates`, `maybe_rotate_epoch`'s retention
  logic) are deleted in favor of the flusher.
- Call sites that construct `SimpleMapStore::new_with_strategy(..., cleanup_policy, ...)`
  update to pass a `SimpleMapStorePersistenceConfig` instead. Main.rs and any
  tests that construct the store directly will need to change.

Does not change:

- The `CleanupPolicy` enum itself stays in `asap_types` — other stores
  (`promsketch_store`, legacy paths) may still reference it. This PR only
  severs `SimpleMapStore`'s dependency on it.
- `SimpleMapStoreGlobal` is intentionally left in-memory-only. Persistence
  targets `PerKey`, which is the production path. Adding it to `Global` is a
  small follow-up if anyone needs it.
- Query planning, the controller client, and the precompute engine's output
  sink are untouched. The store's `Store` trait signature does not change.

---

## Phasing

The PR this design doc accompanies will land in three commits on one branch so
the pieces can be reviewed independently:

1. **Sizing + config plumbing + cleanup-policy removal.** Add
   `approx_memory_bytes` to `AggregateCore` and all concrete accumulators.
   Add `SimpleMapStorePersistenceConfig`. Track `mem_bytes_in_use`. Rip the
   `CleanupPolicy` branches out of `per_key.rs` and update call sites. No
   disk I/O yet — expose the memory counter in `StoreDiagnostics` so we can
   validate sizing in isolation and be confident nothing else regressed.

2. **Segment format, manifest, flusher.** Add `persistence/` submodule under
   `simple_map_store/` with segment encode/decode, manifest read/write,
   background flusher task (memory-first, then time-watermark). Wire
   `flush_and_evict` into the per-key store. Unit tests for round-trip,
   crash-after-segment-before-manifest, and orphan sweep.

3. **Query path read-through + recovery.** Extend `query_precomputed_output`
   to consult the manifest and merge segment hits with in-memory hits. Add
   startup recovery. Integration test: ingest → flush → restart → query →
   same result as no-restart. The read path uses a **trivial bounded LRU**
   for the Tier-2 segment cache in this phase — just enough to avoid
   re-reading the same segment on back-to-back queries. No SLRU/TinyLFU,
   no hit/miss exporter, no tuning knobs beyond `segment_cache_bytes`.

Because phase 1 removes `CleanupPolicy` from this store, phase 1 is **not**
independently mergeable without at least the memory-pressure path from
phase 2 — otherwise the store has no bound on RAM. In practice phases 1 and
2 land together; phase 3 can land separately once the write path is stable.

A later phase 4 (not part of this PR) would upgrade the Tier-2 cache to
SLRU/TinyLFU and export hit/miss metrics, once we have real query traces
to justify the algorithm choice.

---

## Open questions (for review before implementation)

1. **Segment file format: custom binary vs. something off-the-shelf (Parquet,
   Arrow IPC)?** Custom binary is simpler and avoids a dependency, but loses us
   tooling. I lean custom for v1 given that we never read segments outside
   this process, but happy to switch if there's an appetite.

2. **Cold data retention on disk.** Once a sketch is on disk, it lives there
   until the operator removes the directory. Do we want a third knob
   `delete_older_than_ms = T2` (with `T2 >> hot_window_ms`) so disk is also
   bounded? My lean: not in v1 — cold data is cheap and operators can manage
   the directory, but add the knob as soon as anyone asks.

3. **Per-agg-id flush fairness.** Oldest-global-first could starve small,
   slow-moving aggs during a burst on a hot agg. Acceptable for v1 since
   "oldest window first" is well-defined globally; revisit if it bites.

4. **Default `segment_cache_bytes`.** Should v1 default the Tier-2 cache to
   a small nonzero value (e.g. 64 MiB) so the typical read path gets a
   trivial hit-rate win for free, or default it to 0 (opt-in) so no
   workload pays RAM it doesn't measurably benefit from? My lean: default
   to a small nonzero value — a fresh install shouldn't have to know about
   this knob to get reasonable repeat-query performance.

5. **Tier-2 algorithm beyond LRU.** Plain LRU ships in phase 3. Do we
   commit up-front to an upgrade path (SLRU, TinyLFU) or only revisit if
   real traces show scan-resistant patterns are a problem? My lean: defer
   — LRU is fine for the 90% case and the cache has no consistency
   obligations, so swapping the algorithm is a purely local change.

*(The earlier open question about sync vs. async flush I/O is resolved
in-line above — the append-only property of sealed epochs makes sync
`std::fs` on a dedicated thread the clear winner.)*
