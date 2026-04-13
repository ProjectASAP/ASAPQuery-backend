# Design: SimpleMapStore Persistence (Memory Limit + Disk Flush)

## Problem

`SimpleMapStore` (`asap-query-engine/src/stores/simple_map_store/`) is currently an
in-memory-only store. Under long-running ingest it grows unboundedly: every sealed
window for every `(aggregation_id, group_key)` is held in `DashMap<u64, RwLock<StoreKeyData>>`
until `CleanupPolicy::CircularBuffer` rotates it out and drops it on the floor.

This creates two problems:

1. **No memory bound.** A deployment has to either overprovision RAM or rely on
   `CircularBuffer` to throw away data that may still be query-relevant.
2. **No durability.** Cold data (older than the query working set) still occupies
   RAM even though most queries hit only the last few minutes.

We want a persistence layer that lets the store:

- Honor a configurable memory budget for sketches.
- Flush sealed windows older than a configurable timestamp threshold to disk.
- Evict those flushed windows from memory when the budget is exceeded.
- Serve queries transparently from memory + disk.

Goals are scoped to a **single-node, single-process** store. Replication, sharding,
compression, and query pushdown into segments are explicitly out of scope for v1.

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
- `CleanupPolicy` (`data_model/enums.rs`) already has a concept of dropping
  old entries; persistence will become a fourth, non-destructive option.

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
same YAML / controller channel as the existing streaming config:

```rust
pub struct SimpleMapStorePersistenceConfig {
    pub enabled: bool,

    // Memory budget (high watermark). When exceeded, background flusher
    // evicts sealed epochs oldest-first until usage drops below
    // `memory_low_watermark_bytes`.
    pub memory_limit_bytes: usize,
    pub memory_low_watermark_bytes: usize,

    // Time-based flush. Any sealed epoch whose end_ms is older than
    // `now - flush_older_than_ms` is eligible to flush on the next tick,
    // regardless of memory pressure. None disables time-based flushing.
    pub flush_older_than_ms: Option<u64>,

    // Cadence of the background flusher loop.
    pub flush_interval_ms: u64,

    // Root directory for segment files and manifest.
    pub disk_path: PathBuf,

    // Hard ceiling. If memory usage reaches this *during* an insert
    // (flusher is falling behind), the insert path blocks on a condvar
    // until the flusher catches up. Set to memory_limit_bytes * 1.25
    // as a sensible default.
    pub hard_cap_bytes: usize,
}
```

Defaults keep `enabled = false` so existing deployments are unaffected until
they opt in.

### Eviction policy

v1 supports one policy — **oldest-sealed-epoch-first, globally ordered by
epoch `end_ms`**. Rationale:

- Matches the time-window access pattern: queries overwhelmingly target recent
  windows.
- Aligns with `flush_older_than_ms`: the same ordering drives both memory-pressure
  flush and time-based flush.
- Avoids cross-agg fairness debates that a `LargestAggFirst` policy would
  invite; we can add more policies later behind an enum if needed.

### Background flusher

A dedicated Tokio task owned by the store, started in
`SimpleMapStorePerKey::new` when persistence is enabled:

```
loop {
    sleep(flush_interval_ms).await;

    let now = now_ms();
    let mut candidates = Vec::new();

    // Phase 1: time-based — any sealed epoch older than watermark.
    if let Some(max_age) = cfg.flush_older_than_ms {
        candidates.extend(collect_epochs_older_than(now - max_age));
    }

    // Phase 2: memory-pressure — if still over high-water after phase 1,
    // keep pulling oldest sealed epochs until we would drop below
    // memory_low_watermark_bytes.
    if mem_bytes_in_use.load() > cfg.memory_limit_bytes {
        candidates.extend(collect_oldest_until_under_low_water());
    }

    for (agg_id, epoch_id) in candidates {
        flush_and_evict(agg_id, epoch_id).await?;
    }

    manifest.commit().await?;  // atomic rewrite after the batch
}
```

`flush_and_evict` serializes the epoch *outside* the per-agg lock (the epoch is
immutable once sealed, we can read the `Arc` without holding the write lock),
fsyncs the segment file, then takes the per-agg `RwLock::write` only to splice
the epoch out of `sealed_epochs` and decrement `mem_bytes_in_use`. This keeps
flush off the insert/query critical path.

### Query path

`query_precomputed_output` becomes a three-way merge:

1. Read from `current_epoch + sealed_epochs` as today.
2. Look up segments in the manifest whose `[start_ms, end_ms]` overlaps the
   query range for this `agg_id`.
3. For each matching segment, read the file (cached via an `lru::LruCache<SegmentId, Arc<SegmentBuf>>`),
   decode entries whose window overlaps, and merge into the result.

Segment reads happen under a read lock on the manifest; they do **not** take any
per-agg store lock, so they can run fully in parallel with inserts. Merging reuses
the existing `TimestampedBucketsMap` + `AggregateCore::merge_with` that the
in-memory query path already uses — no new merge logic.

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

## What this does **not** change

- `CleanupPolicy::CircularBuffer` and `ReadBased` continue to exist and run. A
  deployment can opt into persistence in addition to a cleanup policy; the
  persistence flusher runs *before* `CircularBuffer` would drop data, so epochs
  get a chance to survive on disk. If both policies fire on the same epoch,
  `CircularBuffer` wins (cleanup is destructive, but that's the user's stated
  intent when they configure it).
- `SimpleMapStoreGlobal` is intentionally left in-memory-only. Persistence
  targets `PerKey`, which is the production path. Adding it to `Global` is a
  small follow-up if anyone needs it.
- Query planning, the controller client, and the precompute engine's output
  sink are untouched. The store's `Store` trait signature does not change.

---

## Phasing

The PR this design doc accompanies will land in three commits on one branch so
the pieces can be reviewed independently:

1. **Sizing + config plumbing.** Add `approx_memory_bytes` to `AggregateCore`
   and all concrete accumulators. Add `SimpleMapStorePersistenceConfig`. Track
   `mem_bytes_in_use`. No disk I/O yet; expose the counter in
   `StoreDiagnostics` so we can validate sizing in isolation.

2. **Segment format, manifest, flusher.** Add `persistence/` submodule under
   `simple_map_store/` with segment encode/decode, manifest read/write,
   background flusher task. Wire `flush_and_evict` into the per-key store.
   Unit tests for round-trip, crash-after-segment-before-manifest, and orphan
   sweep.

3. **Query path read-through + recovery.** Extend `query_precomputed_output`
   to consult the manifest and merge segment hits with in-memory hits. Add
   startup recovery. Integration test: ingest → flush → restart → query →
   same result as no-restart.

Phases 1 and 2 are safe to merge independently because phase 2 is gated on
`enabled = false` by default. Phase 3 is when persistence becomes observable
to query results.

---

## Open questions (for review before implementation)

1. **Segment file format: custom binary vs. something off-the-shelf (Parquet,
   Arrow IPC)?** Custom binary is simpler and avoids a dependency, but loses us
   tooling. I lean custom for v1 given that we never read segments outside
   this process, but happy to switch if there's an appetite.

2. **Async vs. sync flush I/O.** The rest of the store is sync (`RwLock`,
   `DashMap`), but the flusher task is naturally async. Proposal: `tokio::fs`
   for segment writes, sync locks everywhere else. Flusher runs on a dedicated
   task, not a shared runtime, to avoid starving it under query load.

3. **Should `flush_older_than_ms` live here or in the existing
   `CleanupPolicy` enum?** It overlaps conceptually with `CircularBuffer`.
   Proposal: keep it separate — `CleanupPolicy` is destructive, persistence
   config is non-destructive. Confusing them in one knob would be worse.

4. **Per-agg-id flush fairness.** Oldest-global-first could starve small,
   slow-moving aggs during a burst on a hot agg. Acceptable for v1 since
   "oldest window first" is well-defined globally; revisit if it bites.
