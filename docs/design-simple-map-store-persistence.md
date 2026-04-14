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

**Segment file format** (`seg_*.bin`). Every field is laid out so the whole
file can be `mmap`ed and sketch payloads handed directly to
`AggregateCore::deserialize_from_bytes` with zero copies:

```
[u32 magic][u16 version][u16 flags]
[u64 epoch_id][u64 window_start_ms][u64 window_end_ms]
[u32 num_entries][u32 _pad]                          // align to 8
repeated num_entries times:
  [u64 start_ts][u64 end_ts][u32 label_id][u8 agg_type][u24 _pad]
  [u32 payload_len][u32 _pad]                        // align payload to 8
  [payload_len bytes: serialize_to_bytes()]
  [0..7 bytes: tail padding to 8-byte boundary]
[u32 crc32 of body][u32 _pad]                        // trailer aligned
```

Fixed-size header lets us mmap and binary-search by timestamp without parsing
payloads. Body is a linear scan — v1 does not build an in-segment index because
sealed epochs are small (bounded by window size × group count for a single agg).

**Write-side perf details:**

- Before writing, call `fallocate(fd, 0, 0, estimated_size)` to reserve
  contiguous space and avoid ext4/xfs metadata churn under many-small-segments
  workloads. `estimated_size` is `sum of approx_memory_bytes * 1.3` for a safe
  upper bound; any slack is released via `ftruncate` at the end.
- 8-byte alignment for every payload means a single `mmap` + pointer cast is
  safe on every architecture Rust targets. Without alignment, ARM and
  `MIRI`-style UB checks require a copy-to-aligned-buffer step.
- The trailing CRC is computed streaming while we write the body, so we do not
  re-read the file to compute it.

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

    // ---- Disk retention ----
    //
    // Cold-tier TTL. Any segment whose end_ms is older than
    // `now - delete_older_than_ms` is deleted from disk on the next
    // flusher tick (after its references are removed from the manifest
    // and no in-flight query is reading it). Bounds disk usage and keeps
    // the manifest small enough to stay in L2/L3 cache on long-running
    // deployments. Must be strictly greater than hot_window_ms; expected
    // to be much greater (hours vs. days or weeks).
    // None disables cold deletion entirely — disk grows unboundedly.
    pub delete_older_than_ms: Option<u64>,

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

**Round-robin across `agg_id`, oldest-first within each agg.** Every tick,
the flusher walks agg-ids in order, pops the oldest sealed epoch from each,
and repeats until the stopping condition (memory low-water or end of time
threshold) is met. Both triggers share the same walk order so the flusher
never has two disagreeing notions of "what to flush next."

Rationale:

- **Lock-spread under burst.** A strict global oldest-first ordering would
  flush many epochs from the *same* hot agg-id back-to-back, hammering the
  same per-agg `RwLock` repeatedly and creating brief query-latency spikes
  on that one agg. Round-robin spreads the flusher's lock acquisitions
  across different `RwLock`s, which is cheap for DashMap (lock-free outer)
  and gives query latency a smoother profile.
- **Same total work, no complexity cost.** Round-robin does not evaluate
  more epochs than strict-global would; it just reorders which epoch is
  flushed next. Implementation cost is one `BTreeMap<(agg_id, end_ms),
  EpochRef>` populated by walking `store` once per tick, or equivalently a
  per-agg min-heap of sealed epochs with a round-robin cursor.
- **Freshness.** Small, slow-moving aggs are never starved by a burst on
  a hot agg — they always get a turn in each round.
- **Still matches the time-window access pattern.** Within each agg,
  oldest-first is preserved, so queries against recent windows on any agg
  remain unaffected.

### Background flusher

A dedicated `std::thread` owned by the store, started in
`SimpleMapStorePerKey::new`. Each tick, it checks the primary trigger
(memory) first, the secondary trigger (time watermark), then the
disk-retention sweep:

```
loop {
    thread::sleep(flush_interval_ms);
    if shutdown.load() { break; }

    let now = now_ms();
    let mut candidates: Vec<EpochRef> = Vec::new();

    // Phase 1 (PRIMARY): memory budget.
    // Walk agg-ids round-robin, pulling the oldest sealed epoch from
    // each on every pass, until projected memory drops below the
    // low-water mark. This is the knob that actually bounds RAM.
    if mem_bytes_in_use.load() > cfg.memory_limit_bytes {
        candidates.extend(collect_round_robin_until_under_low_water(
            cfg.memory_low_watermark_bytes,
        ));
    }

    // Phase 2 (SECONDARY): time watermark T.
    // Any sealed epoch older than `now - hot_window_ms` that wasn't
    // already picked up in phase 1 is flushed here. Also walked
    // round-robin across aggs so a burst on one hot agg does not
    // monopolize the tick.
    if let Some(hot_window) = cfg.hot_window_ms {
        candidates.extend(collect_older_than_round_robin(
            now - hot_window,
        ));
    }

    // Dedup (phase 1 and phase 2 can pick the same epoch). Order is
    // already interleaved across aggs; no re-sort.
    candidates.dedup_by_key(|c| (c.agg_id, c.epoch_id));

    // ---- Group-commit the whole tick ----
    let mut written: Vec<WrittenSegment> = Vec::new();
    for epoch_ref in candidates {
        let bytes = serialize_epoch(epoch_ref.arc.clone());
        let path  = write_segment_no_fsync(&bytes, epoch_ref)?;
        written.push(WrittenSegment { path, meta: epoch_ref.meta });
    }
    fdatasync_all(&written)?;            // one batched fsync pass
    manifest.rewrite_and_fsync(&written)?;
    fsync_parent_dir(&cfg.disk_path)?;   // one dir fsync for the whole batch

    // Now that segments are durable AND referenced by the manifest,
    // evict them from memory.
    for seg in &written {
        splice_out_of_sealed_epochs(seg.meta);
        mem_bytes_in_use.fetch_sub(seg.meta.approx_bytes);
    }

    // Phase 3 (disk retention sweep): delete segments older than T2.
    if let Some(ttl) = cfg.delete_older_than_ms {
        let cutoff = now.saturating_sub(ttl);
        let expired = manifest.segments_older_than(cutoff);
        for seg in expired {
            manifest.remove(seg.id);
            cache_tier2.invalidate(seg.id);  // drop any decoded copy
            fs::remove_file(seg.path).ok();  // best-effort; orphan sweep on restart
        }
        if !expired.is_empty() {
            manifest.rewrite_and_fsync(&[])?;
        }
    }
}
```

Under memory pressure, phase 1 dominates and phase 2 usually finds nothing
left to do (the oldest epochs are already gone). Under light ingest, phase 1
is a no-op and phase 2 does all the work. Phase 3 is independent and runs
every tick regardless; it costs one manifest scan plus one `unlink` per
expired segment.

#### Group-commit fsync

The pseudocode above batches all `fsync`/`fdatasync` calls for a tick into a
single pass at the end, rather than `fsync`ing each segment inline. On
spinning disks this is ~10× fewer head seeks per tick; on SSDs it is ~3–4×
fewer syscalls. The cost is one temporarily-larger `written` vector and one
extra `fsync_parent_dir` at the end — trivial relative to the saved I/O.

Durability invariant remains the same: **no segment is referenced by the
manifest until its bytes and the manifest itself are both `fsync`'d.** The
group-commit ordering is (1) write all segment bodies, (2) `fdatasync` all
of them, (3) rewrite + `fsync` manifest via the atomic `write → rename`
dance, (4) `fsync` parent directory. A crash at any point leaves orphan
segment files (cleaned by the startup sweep) but never a dangling manifest
entry.

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
    //
    // Default: min(10% * memory_limit_bytes, 512 MiB).
    //
    // A fresh install should not need to know about this knob to get
    // reasonable repeat-query performance. Setting to 0 disables Tier 2
    // entirely (every cold query pays disk I/O); a fixed absolute
    // default would be too small on big boxes and too large on small
    // ones, so the default scales with the write budget.
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
   `Cache<SegmentId, Arc<DecodedSegment>>` keyed on manifest metadata.

4. **Two independent budgets are easier to tune than one unified priority
   score.** Operators reason about "how much RAM does write buffering
   need?" and "how much RAM does read caching need?" separately. A
   unified `priority = α * recency + β * frequency` score is harder to
   explain and harder to debug when it misbehaves.

**Eviction policy for Tier 2: W-TinyLFU via `moka` (or `mini-moka`).**
Plain LRU is the obvious choice but is catastrophically scan-vulnerable —
a single long-range query sweeps the cache and evicts everything genuinely
hot, which is exactly the access pattern TSDB dashboards and recording
rules produce (hour/day/week range scans). W-TinyLFU's admission filter
rejects scan traffic from displacing hot entries and typically delivers
10–30% better hit rate than LRU at the same byte budget on skewed /
Zipfian workloads.

The `moka` crate is the standard Rust implementation (sync and async
variants, weight-based eviction keyed on byte size, well-maintained, used
widely in the Rust ecosystem). The API is effectively a drop-in for LRU
(`get`, `insert`, `invalidate`), so we incur no additional complexity vs.
a hand-rolled LRU — just better hit rate. W-TinyLFU's per-access overhead
is a handful of CAS ops on a small count-min sketch, cheaper than LRU's
mutex-protected list reordering.

The cache exposes hit/miss counters in `StoreDiagnostics` from day one so
we have signal for future tuning.

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
   `simple_map_store/` with segment encode/decode (8-byte aligned,
   `fallocate`d, mmap-friendly), manifest read/write, background flusher
   thread (memory-first, then time-watermark, then T2 retention sweep) with
   group-commit fsync. Wire `flush_and_evict` into the per-key store. Unit
   tests for round-trip, crash-after-segment-before-manifest, orphan sweep,
   and T2 deletion.

3. **Query path read-through + recovery + Tier-2 cache.** Extend
   `query_precomputed_output` to consult the manifest and merge segment hits
   with in-memory hits. Wire a `moka` (or `mini-moka`) weight-bounded
   segment cache sized to `min(10% * memory_limit_bytes, 512 MiB)` by
   default, with hit/miss counters exported via `StoreDiagnostics`. Add
   startup recovery. Integration test: ingest → flush → restart → query →
   same result as no-restart, plus a scan-resistance test that confirms a
   long-range query does not evict a separately-hot segment.

Because phase 1 removes `CleanupPolicy` from this store, phase 1 is **not**
independently mergeable without at least the memory-pressure path from
phase 2 — otherwise the store has no bound on RAM. In practice phases 1 and
2 land together; phase 3 can land separately once the write path is stable.

---

## Resolved decisions

Every question previously flagged as open has been resolved in favor of the
performance-optimal choice. The table below is a summary; the reasoning for
each lives in the section it points to.

| # | Question | Decision | Why |
|---|---|---|---|
| 1 | Segment file format | Custom binary, 8-byte aligned, mmap-friendly, `fallocate`d | Zero-copy deserialize into `AggregateCore`; no Parquet/Arrow overhead for data we never column-prune; smaller binary size and compile time. See **Disk layout**. |
| 2 | Sync vs. async flush I/O | Sync `std::fs` on a dedicated `std::thread`, with **group-commit fsync** batching across a tick | `tokio::fs` just routes to a blocking threadpool on Linux, so async is a wash at the syscall level; the real win is batching `fdatasync`. No task on the flusher's runtime is waiting on it to yield. See **Background flusher / Group-commit fsync**. |
| 3 | Cold data retention on disk | Add `delete_older_than_ms = T2`, run as phase 3 of the flusher tick | Unbounded segment count bloats the manifest (falls out of L2/L3), slows startup directory sweeps, and pressures Tier-2 eviction. Cheap to add now, painful to retrofit once a deployment has millions of orphans. See **Configuration** and **Background flusher phase 3**. |
| 4 | Per-agg-id flush fairness | **Round-robin across agg-ids, oldest-first within each agg** | Strict-global-oldest hammers one `RwLock` during a hot-agg burst and creates query-latency spikes on that one agg. Round-robin spreads lock acquisitions across different `RwLock`s for the same total work. See **Eviction order**. |
| 5 | Default `segment_cache_bytes` | `min(10% * memory_limit_bytes, 512 MiB)` | A fixed 64 MiB default is too small on big boxes and too large on small ones; scaling with the write budget keeps the tier sensibly sized without requiring operator tuning on a fresh install. See **Configuration**. |
| 6 | Tier-2 algorithm | **W-TinyLFU via `moka`** from day one, not plain LRU | LRU is scan-vulnerable — one long-range query evicts everything genuinely hot, which is exactly the TSDB dashboard access pattern. W-TinyLFU's admission filter rejects scan traffic and typically delivers 10–30% better hit rate at the same byte budget on skewed workloads, with a drop-in API and lower per-access CPU than LRU. See **Read-side segment cache**. |

If any of these decisions turn out to be wrong under real traces, the
affected sections are the natural point of revisiting — but none of them
are "temporary v1 shortcuts we'll upgrade later." This is the target
design.
