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

### Unit of flush vs. unit of file: the *part*

There are two granularities to separate cleanly:

- **Unit of flush = sealed epoch.** Same as before. Sealed epochs are
  immutable, have a well-defined time range, and can be spliced out of
  `sealed_epochs` under a brief per-agg `RwLock::write`.
- **Unit of file = *part*.** A part is **one flush tick's worth of sealed
  epochs bundled into a single on-disk directory**, regardless of which
  agg-id they came from. The flusher already assembles all the candidate
  epochs for a tick before it touches the disk; instead of writing N
  separate segment files and fsyncing each, it writes one part.

This is the same pattern every mainstream TSDB converges on — Prometheus
blocks, InfluxDB TSM, VictoriaMetrics parts — for the same reasons:
file count is bounded by flush ticks (not by individual epochs), metadata
overhead is amortized across many entries, and compaction becomes a pure
directory-level merge.

The `current_epoch` is never flushed while hot. It becomes flushable the
moment the rotator seals it, at which point it becomes a candidate for the
next flush tick's part.

### Disk layout

```
<disk_path>/
├── parts_manifest.log          # append-only log of part additions + deletions
├── parts_manifest.snapshot     # periodic binary snapshot (compaction of the log)
└── parts/
    ├── 0000000001/             # part directory, name = monotonic part_id
    │   ├── meta.bin            # fixed-size header: min_ts, max_ts, counts, crc
    │   ├── data.bin            # all epoch payloads concatenated, 8-byte aligned
    │   └── index.bin           # sorted array of entries, mmap-binary-search target
    ├── 0000000002/
    │   ├── meta.bin
    │   ├── data.bin
    │   └── index.bin
    └── ...
```

**Why parts instead of dir-per-agg with file-per-epoch:**

- **File count scales with flush ticks, not with epochs.** On a 1-second
  flush interval with 200 agg-ids and 1-minute windows, the old layout
  produced ~288K files/day; the part layout produces ~86K files total
  (one tick = three files: `meta.bin`, `data.bin`, `index.bin`). That is
  the difference between "inode pressure is a real concern" and "we are
  well within every filesystem's comfort zone."
- **Metadata is amortized.** One `fallocate` + one `fdatasync` per
  `data.bin` covers all epochs in the tick, rather than N separate
  allocations and N separate fsyncs. Group-commit is now an intrinsic
  property of the layout, not something the flusher has to arrange.
- **Per-part in-file index.** Queries binary-search the part's `index.bin`
  rather than linear-scanning a segment body. O(log N) per part instead
  of O(N), and `index.bin` is mmap-friendly so the search is pure
  pointer arithmetic with no syscalls.
- **Aggs are interleaved inside a part, not segregated by directory.**
  No wasted directories for low-traffic aggs; the in-part index handles
  agg lookup cheaply.
- **T2 retention is whole-directory.** Each part covers a tight time
  range (roughly `flush_interval_ms`), so `delete_older_than_ms`
  operates at part granularity — `rm -rf parts/000001234/` — instead of
  touching shared files.
- **Compaction fits naturally.** A background compactor can merge N
  adjacent old parts into one larger part with the same on-disk shape.
  Readers don't care because the parts_manifest gets updated atomically
  and old part dirs get removed only after all in-flight readers are done.

**Part file formats** (all little-endian, fixed layouts, mmap-friendly,
8-byte aligned, written via `fallocate` + streaming CRC — same perf
details that applied to segments, now applied to `data.bin`):

```
meta.bin        (128 bytes, fixed)
  [u32 magic][u16 version][u16 flags]
  [u64 part_id][u64 min_ts][u64 max_ts]
  [u32 num_entries][u32 num_aggs]
  [u64 data_len][u64 index_len]
  [u64 created_unix_ns]
  [u32 _reserved; 6]
  [u32 crc32 of the above]

data.bin        (sum of padded payloads)
  repeated num_entries times, in the order the index lists them:
    [payload_len bytes: serialize_to_bytes()]
    [0..7 bytes: tail padding to 8-byte boundary]

index.bin       (32 bytes per entry, sorted by (agg_id, start_ms))
  repeated num_entries times:
    [u64 agg_id][u64 start_ms][u64 end_ms]
    [u32 data_offset][u32 payload_len]
  [u32 crc32 of the above][u32 _pad]
```

`index.bin` is the only file a query needs to traverse to locate entries
inside a part. It is small (32 B × num_entries, typically tens of KB), is
mmap'd on first access, and a binary search by `(agg_id, start_ms)` lands
on the byte range inside `data.bin` with one pointer-arithmetic step and
zero decode work.

**Parts manifest: append-only log + periodic snapshot.**

The manifest is the one piece of global state on disk. The previous
design had it as a JSON file rewritten on every flush tick — quadratic
over the lifetime of the deployment. We replace it with the standard
LSM-style pattern:

- **`parts_manifest.log`** is an append-only binary file. Each flush
  tick appends one record (add-part or delete-part, both fixed-size).
  Appending is a single `write + fdatasync` on a file whose size is
  proportional to the number of *ticks*, not the number of parts that
  have ever existed. Cheap and O(1) per tick.
- **`parts_manifest.snapshot`** is a periodic binary snapshot of the
  live set of parts, produced by replaying the log and emitting a flat
  sorted array of `(part_id: u64, min_ts: u64, max_ts: u64, size: u64)`
  = 32 bytes per live part. The snapshot is rewritten atomically (write
  tmp → fsync → rename) whenever the log gets large relative to the
  snapshot, and the log is truncated after. Snapshot + remaining log
  is always the authoritative live state.
- **On startup**, the store loads the snapshot (mmap + direct cast, no
  parse), then replays any tail of the log added since the snapshot was
  taken, then verifies every live part directory's `meta.bin` CRC.
  Sweep orphan part dirs (present on disk but not in the replayed
  state) and treat them as a mid-flush crash — delete them.

Binary formats throughout mean parse time is effectively zero; the
snapshot is "cast a byte slice to `&[PartEntry]`" — which works because
we declared the layout 8-byte aligned and fixed-size.

**Durability ordering per flush tick** (the invariant a crash must not
violate: no part is referenced in the manifest until its bytes are on
disk):

1. Assemble the tick's candidate epochs (in-memory, no I/O).
2. `fallocate` the three files in `parts/<part_id>/`, stream payloads
   into `data.bin`, stream index into `index.bin`, write `meta.bin`.
3. `fdatasync` `data.bin`, `index.bin`, `meta.bin` (batched — one
   syscall per file, not per entry).
4. `fsync` the part directory itself.
5. Append the add-part record to `parts_manifest.log` and `fdatasync`
   the log.
6. `fsync` `<disk_path>` (the root) so the log's size update is durable.

A crash at any step before (5) leaves an orphan part directory that
startup sweep deletes. A crash between (5) and (6) is fine — the log
record is already durable via (5). After (6), the part is officially
live and the flusher may evict the corresponding epochs from memory.

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
same YAML / control plane channel as the existing streaming config. The two
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
    if candidates.is_empty() { /* go straight to phase 3 below */ }

    // ---- Build and persist one part for the whole tick ----
    //
    // All candidate epochs from this tick land in a single part directory
    // under `parts/<part_id>/`. File count per tick is O(1) (three
    // files) instead of O(num_candidates). This is where group-commit
    // stops being something the flusher explicitly arranges and starts
    // being an intrinsic property of the layout.
    if !candidates.is_empty() {
        let part_id = next_part_id();
        let part_dir = cfg.disk_path.join("parts").join(fmt_part_id(part_id));

        // (a) Clone each candidate's Arc<Epoch> under a brief read lock.
        //     No serialization under any lock.
        let snapshots: Vec<EpochSnapshot> = candidates
            .iter()
            .map(|c| snapshot_under_read_lock(c))
            .collect();

        // (b) Serialize to the three part files. data.bin is fallocate'd
        //     to `sum(approx_memory_bytes) * 1.3`; index.bin is sized
        //     exactly (32 B per entry). Both are 8-byte aligned and CRCs
        //     are computed streaming.
        let (data_len, index_len) = write_part_files(&part_dir, &snapshots)?;

        // (c) Batched fdatasync of the three files + the part directory.
        //     One syscall per file; no per-epoch fsync.
        fdatasync_file(&part_dir.join("data.bin"))?;
        fdatasync_file(&part_dir.join("index.bin"))?;
        fdatasync_file(&part_dir.join("meta.bin"))?;
        fsync_dir(&part_dir)?;

        // (d) Append the add-part record to the manifest log and fsync it.
        //     Single fixed-size append — no rewrite of existing state.
        manifest.append_add_part(AddPartRecord {
            part_id,
            min_ts: snapshots.iter().map(|s| s.min_ts).min().unwrap(),
            max_ts: snapshots.iter().map(|s| s.max_ts).max().unwrap(),
            size_bytes: (data_len + index_len) as u64,
        })?;
        fsync_dir(&cfg.disk_path)?;  // log's size update is now durable

        // (e) Now that the part is officially live, evict the source
        //     epochs from memory. This is the only place we take the
        //     per-agg write lock, and we take it O(1) times per epoch.
        for snapshot in &snapshots {
            splice_out_of_sealed_epochs(snapshot.agg_id, snapshot.epoch_id);
            mem_bytes_in_use.fetch_sub(snapshot.approx_bytes);
        }

        // Maybe compact the manifest log into a fresh snapshot if the
        // log has grown large relative to the current snapshot.
        manifest.maybe_compact()?;
    }

    // Phase 3 (disk retention sweep): delete whole parts older than T2.
    //
    // Because parts cover a tight time range (~flush_interval_ms), T2
    // deletion operates at part-directory granularity — we rm -rf the
    // whole thing rather than touching shared files.
    if let Some(ttl) = cfg.delete_older_than_ms {
        let cutoff = now.saturating_sub(ttl);
        let expired: Vec<PartId> = manifest
            .live_parts()
            .filter(|p| p.max_ts < cutoff)
            .map(|p| p.part_id)
            .collect();

        for part_id in expired {
            // Invalidate Tier-2 cache entries that reference this part,
            // append a delete-part record to the log, then remove the dir.
            cache_tier2.invalidate_part(part_id);
            manifest.append_delete_part(part_id)?;
            let part_dir = cfg.disk_path
                .join("parts")
                .join(fmt_part_id(part_id));
            fs::remove_dir_all(part_dir).ok();  // orphan sweep on restart catches failures
        }
        if !expired.is_empty() {
            fdatasync_file(&manifest.log_path())?;
            fsync_dir(&cfg.disk_path)?;
        }
    }
}
```

Under memory pressure, phase 1 dominates and phase 2 usually finds nothing
left to do (the oldest epochs are already gone). Under light ingest, phase 1
is a no-op and phase 2 does all the work. Phase 3 is independent and runs
every tick regardless; it costs one manifest scan plus one `rm -rf` per
expired part directory (usually zero).

Group-commit is now **intrinsic to the layout**, not something the flusher
has to explicitly arrange: one tick = one part = three `fdatasync`s + one
dir fsync + one log append, independent of how many epochs the tick is
flushing. The durability ordering is spelled out in the previous section
("Durability ordering per flush tick").

The part-building loop above takes advantage of a property that matters a
lot for the flusher design: **sealed epochs are append-only and frozen.**
Once the rotator seals an epoch, no writer will ever touch its contents
again — it is only read (by queries) or removed wholesale (by the
flusher). That immutability is what lets the flusher stay completely off
the critical path:

1. Take the per-agg `RwLock::read` briefly, clone the `Arc<Epoch>` for
   each candidate out of `sealed_epochs`, drop the lock. This is the
   `snapshot_under_read_lock` step.
2. Serialize all candidates into `data.bin` / `index.bin` / `meta.bin`,
   fsync the three files and the part directory, and append to the
   manifest log — **entirely outside any per-agg store lock**, on the
   flusher's own thread. Nothing in the system is waiting on this I/O.
   Inserts continue to land in `current_epoch`; queries continue to
   read from the still-in-place `sealed_epochs` entries (and the cloned
   `Arc`s keep bytes alive for any query that happens to hold a
   reference already); the rotator continues to seal new epochs behind
   us.
3. Once the part is officially live in the manifest log, take each
   per-agg `RwLock::write` briefly to splice the corresponding epoch
   out of `sealed_epochs` and decrement `mem_bytes_in_use`. This is
   O(1) per epoch — a `BTreeMap::remove` plus an atomic subtraction —
   and is the only write lock the flusher holds per epoch.

Because steps 2 and 3 are decoupled by the `Arc<Epoch>` clone from step
1, **no per-agg lock is ever held across disk I/O**, and the flusher
never blocks anything on the insert or query path beyond the two brief
lock acquisitions at the start and end.

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

1. Read from `current_epoch + sealed_epochs` as today (in-memory hits).
2. Walk the parts_manifest's live-parts list for any `part.[min_ts,
   max_ts]` that overlaps the query's time range. The manifest lives in
   memory as a `Vec<PartEntry>` built from the snapshot + log replay at
   startup, so this is a linear scan over a short list (tens of
   thousands of entries in the worst case, all 32-byte records) — fast
   and trivially parallel with inserts.
3. For each overlapping part: fetch its `DecodedPart` from the Tier-2
   segment cache (`moka::Cache<PartId, Arc<DecodedPart>>`). On miss,
   `mmap` the part's `data.bin` and `index.bin`, wrap them in an
   `Arc<DecodedPart>` (holding the mmap handles), and insert into the
   cache. Then binary-search `index.bin` for `(agg_id, start_ms)`, walk
   the matching entries forward while `start_ms <= query_end`, and for
   each one hand the `&[u8]` slice of `data.bin` directly to
   `AggregateCore::deserialize_from_bytes` with zero copies.

Part reads happen under a read lock on the parts_manifest vector; they
do **not** take any per-agg store lock, so they run fully in parallel
with inserts. Merging reuses the existing `TimestampedBucketsMap` +
`AggregateCore::merge_with` that the in-memory query path already uses
— no new merge logic.

**Why this is fast:**

- **One file open per part hit, not per entry.** Queries that span many
  aggs inside a part still only pay one `mmap`'s worth of setup cost.
- **Zero-copy deserialize.** The 8-byte alignment guarantee means
  `&data.bin[offset..offset+len]` can be fed straight to the
  sketch-specific decoder without a staging buffer.
- **Binary search, not linear scan.** `index.bin` is sorted by
  `(agg_id, start_ms)` and is a flat mmap'd array; `partition_point` is
  a few cache lines of work.
- **Page cache locality.** Adjacent entries for the same agg inside a
  part are physically adjacent on disk, so a query over a time range
  touches contiguous pages.

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

**Tier 2 — read-side part cache (query-driven).** Bounded by a separate
`part_cache_bytes` budget. Contains mmap handles and decoded index views
of parts pulled back from disk by the query path. Source of truth is
always the part directory on disk — the cache is a pure optimization,
drop-anytime, never dirty. Managed by the query path, not the flusher.

```rust
pub struct SimpleMapStorePersistenceConfig {
    // ... existing fields ...

    // Read-side part cache. Bounded independently of
    // `memory_limit_bytes`; this budget is for decoded parts the query
    // path pulls back from disk, not for the authoritative hot set.
    //
    // Default: min(10% * memory_limit_bytes, 512 MiB).
    //
    // A fresh install should not need to know about this knob to get
    // reasonable repeat-query performance. Setting to 0 disables Tier 2
    // entirely (every cold query pays disk I/O); a fixed absolute
    // default would be too small on big boxes and too large on small
    // ones, so the default scales with the write budget.
    pub part_cache_bytes: usize,
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

1. Open `disk_path`. If `parts_manifest.snapshot` exists, mmap it and
   cast the bytes to `&[PartEntry]` (no parse — the layout is 8-byte
   aligned and versioned in a small header). Otherwise, start with an
   empty live set.
2. Replay `parts_manifest.log` from the offset recorded in the
   snapshot's footer, applying add-part and delete-part records to the
   live set.
3. For every live part, stat its directory, verify `meta.bin`'s magic,
   version, and CRC. Parts whose directory is missing or whose
   `meta.bin` fails verification are logged and removed from the live
   set (and a delete-part record is appended to the log to make the
   removal durable).
4. Sweep `parts/` for directories not referenced in the live set
   (orphans from a mid-flush crash before step 5 of the durability
   ordering) and `rm -rf` them.
5. Build the in-memory `Vec<PartEntry>` that the query path reads. Do
   **not** mmap `data.bin` or `index.bin` eagerly — parts are mapped
   lazily on first query hit and cached in Tier 2. Cold data stays
   cold until a query asks for it.

### Concurrency summary

| Path              | Lock taken                                  |
|-------------------|---------------------------------------------|
| Insert            | per-agg `RwLock::write` (unchanged)         |
| Query in-memory   | per-agg `RwLock::read` (unchanged)          |
| Query disk        | parts_manifest `RwLock::read` + moka cache internal |
| Flush: snapshot   | per-agg `RwLock::read` (brief, per candidate) |
| Flush: serialize  | none (operates on cloned `Arc<Epoch>`s)     |
| Flush: commit log | parts_manifest `RwLock::write` (brief, append) |
| Flush: evict      | per-agg `RwLock::write` (short, O(1) splice per epoch) |
| T2 sweep          | parts_manifest `RwLock::write` (brief, delete records) |

No lock of any kind is held across a `fsync` or disk I/O.

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
- Query planning, the control plane client, and the precompute engine's output
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

2. **Parts layout, manifest log, flusher.** Add `persistence/` submodule
   under `simple_map_store/` with part encode/decode (`meta.bin` +
   `data.bin` + `index.bin`, 8-byte aligned, `fallocate`'d, mmap-friendly),
   parts_manifest log + snapshot read/write, background flusher thread
   (memory-first, then time-watermark, then T2 retention sweep, one part
   per tick, group-commit implicit in the layout). Wire part-building and
   eviction into the per-key store. Unit tests for round-trip,
   crash-before-log-append, crash-between-log-append-and-dir-fsync,
   orphan-part sweep, and T2 whole-part deletion.

3. **Query path read-through + recovery + Tier-2 part cache.** Extend
   `query_precomputed_output` to walk the parts_manifest, binary-search
   `index.bin` of overlapping parts, and zero-copy deserialize payloads
   out of mmap'd `data.bin`. Wire a `moka` (or `mini-moka`)
   weight-bounded part cache sized to
   `min(10% * memory_limit_bytes, 512 MiB)` by default, keyed on
   `PartId`, with hit/miss counters exported via `StoreDiagnostics`.
   Add startup recovery (snapshot mmap + log replay + CRC verify +
   orphan sweep). Integration test: ingest → flush → restart → query →
   same result as no-restart, plus a scan-resistance test that confirms
   a long-range query does not evict a separately-hot part.

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
| 7 | Disk layout | **Parts** (one directory per flush tick containing `meta.bin` + `data.bin` + `index.bin`) with an **append-only `parts_manifest.log` + periodic binary snapshot**, not one file per sealed epoch with a JSON manifest | One-file-per-epoch produces hundreds of thousands of tiny files on a real deployment (inode pressure, readdir slowdown, per-file fsync floor). A JSON manifest rewritten each tick is quadratic over the deployment's lifetime. Parts bound file count to O(flush ticks), bundle all of a tick's epochs behind one fallocate + three fdatasyncs, push per-part indexing into a binary-searchable `index.bin`, and reduce the global manifest to an append-only log whose size is proportional to ticks, not epochs. This is the layout every mainstream TSDB converges on. See **Unit of flush vs. unit of file: the *part*** and **Disk layout**. |

If any of these decisions turn out to be wrong under real traces, the
affected sections are the natural point of revisiting — but none of them
are "temporary v1 shortcuts we'll upgrade later." This is the target
design.

**Not resolved here, deliberately punted to v2:** background compaction
of adjacent small parts into larger ones. The parts layout accommodates
compaction cleanly (it's a pure directory-level merge with the same
on-disk shape as a regular flush tick), but v1 ships without it. With
T2 retention in place and a reasonable flush interval (seconds, not
milliseconds), the number of live parts in v1 stays bounded at
`T2 / flush_interval_ms` — a few tens of thousands at most, well within
what a binary-searched `Vec<PartEntry>` handles with room to spare.
Compaction becomes necessary only if we lower the flush interval
significantly or extend T2 to very long horizons.
