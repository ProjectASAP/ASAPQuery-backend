# Design: tiered view storage (hot windows in the heap, cold windows in files)

## Audience and context

This document is for developers and architects of the backend data plane.
It extends [the data plane demo design](data-plane-demo.md) ("the demo
design") and is independent of
[parallel precompute](parallel-precompute.md): that one changes how a
window's views are computed, and this one changes where a committed window
is kept and how a query reads it.

In the demo design:

- the precompute committer appends each refreshed window's rows to the
  materialized view store (`ViewStore`), as decoded Arrow batches whose
  summary columns hold one `Arc<dyn AggregateCore>` per row, and keeps
  them for the view's `retention_ms`;
- persist threads write the same rows, encoded, to one append-only file
  per refresh group (`persist.rs`), for recovery only: no query ever reads
  the files.

Terms used here:

- **Window**: one view's rows for one `(start, end]`; the unit
  `ViewStore` appends, reads and drops. (The demo design's raw *panes*
  are unrelated and are not tiered here.)
- **Hot window**: decoded batches in the heap, read without copying.
- **Cold window**: only its encoded bytes in a file, read through the OS
  page cache and decoded on read.
- **Window cache**: the bounded set of decoded windows shared by every
  query (the hot tier).

## Problem and why now

At 10k series and 1 sample/s, the data plane's RSS is larger than
VictoriaMetrics':

| Workload (10k series), comparison runs | ASAP steady RSS | VictoriaMetrics steady RSS |
| --- | --- | --- |
| dashboard-heavy | 6.7 GB | 3.7 GB |
| typical-dashboard | 4.1 GB | 0.5 GB |

An in-process run of the dashboard plan alone (see
[Measured expectations](#measured-expectations)) holds 3.9 GB, about
3.1 GB of it view windows, while the same windows take 1.46 GB in its view
files.

The two engines hold different things:

- **VictoriaMetrics** keeps compressed raw samples (about 1–2 B each) in
  files and reads them through the page cache. Its caches shrink under
  memory pressure, and the kernel reclaims clean file pages.
- **The data plane** keeps every view window decoded in the heap until
  retention drops it. A 15 s window at 1 sample/s summarizes 15 samples,
  yet a decoded KLL state takes about 0.96 KB and a DDSketch state
  1.2–1.5 KB (from the memory investigation of the dashboard runs). The
  view files hold the same windows in compact form, but nothing reads
  them while running.

Experiments now run both engines in cgroups with the same `memory.max`,
which counts page cache as well as anonymous memory. Under that limit
VictoriaMetrics slows down (its page cache is reclaimed) while the data
plane is killed (its heap cannot be reclaimed). The data plane needs a
way to hold windows that it can give back under pressure.

## Goals and non-goals

Goals:

- Windows that recent evaluations did not read live only in files and are
  read through the page cache; the heap holds a bounded set of decoded
  windows.
- Under a cgroup `memory.max`, the data plane is never OOM-killed because
  of view windows: their memory is either a bounded cache or reclaimable
  page cache.
- The persistence files and the cold tier are the same files: each window
  is encoded and written once.
- Answers are identical with tiering on and off.
- A restart reads the same files and does not decode them all up front.
- Disk use is bounded by retention (today the files grow without
  trimming).

Non-goals (v1):

- Tiering raw panes (only open windows and query-time `Scan` lookbacks
  hold raw samples).
- Tiering query-time results or the result cache (both are already
  bounded by bytes).
- Compressing the encoded states further (general-purpose compression,
  columnar re-encoding); measured below as a follow-up.
- The Planner choosing placement per view. The requirements are listed in
  [What the Planner cost model would need](#what-the-planner-cost-model-would-need).
- Shrinking the decoded states themselves (compact sealed KLL, shared
  identity columns, smaller exact-state records). That work is
  complementary and is discussed under
  [Alternatives](#alternatives-and-decisions).

## Inputs, outputs, and end-to-end behavior

- Inputs: unchanged (Remote Write, the `InstallablePlan`), plus two
  options: a byte budget for the window cache and an idle time after which
  a window leaves it.
- Outputs: unchanged answers. New `/metrics` series for the cache and the
  cold reads. View files are now per group *directory* of time segments.

Example (dashboard plan, demo design example 4): four 15 s views over two
metrics, `quantile_over_time(0.99, lat[1h])` and three others, every 15 s.

| | Today | Tiered, no rollups | Tiered, with rollup levels |
| --- | --- | --- | --- |
| Windows kept per view | 240 (1 h) | 240, in files | 240 fine + 12 coarse, in files |
| Decoded in the heap | all 240 | those read in the last idle period, up to the budget: all 240 if the budget fits them | about 12 coarse + 20–40 fine |
| A query at T reads | 240 hot windows | hot windows, plus cold ones decoded | about 12 coarse + 20–40 fine, all hot |
| Under a small `memory.max` | OOM | slower queries (cold decode), no OOM | fits |

Today's dashboard reads every window of the hour at every evaluation, so
recency alone keeps every window hot. Tiering then lowers RSS only when a
budget forces it, and pays with decode CPU per query. Hierarchical rollup
levels (an upcoming Planner change, demo design "Hierarchical rollups")
make most fine windows unread, so they leave the heap at no query cost.

## Proposed design

```
precompute committer ──append──► ViewStore snapshot (immutable, Arc)
   │                               per (view, window): location + state
   │                               Pending(batches) │ Stored(segment, offset, len)
   │ insert decoded windows        ▲ ack (window → offset)
   ▼                               │
window cache (hot tier) ◄───── persist-N threads: encode once, append the
  sharded map, byte budget,        record to group-g/<segment>.seg, sync
  CLOCK eviction, single-flight    every interval, roll and delete segments
  decode on miss                   past retention
   ▲
   │ hit: Arc<Vec<Batch>>; miss: decode the fragment from the mapped segment → insert
query workers (RunStore::read_materialized_view)
```

### Window states

Each `ViewWindow` in the store keeps its `(start, end]` and one of:

- **`Pending(Arc<Vec<Batch>>)`**: committed but not yet written to a
  file. Always in the heap. Every window starts here.
- **`Stored(WindowLocation)`**: written (not necessarily synced); its
  segment, byte offset and length. The decoded batches may or may not be
  in the window cache.
- **`Pinned(Arc<Vec<Batch>>)`**: cannot be written (a UnivMon whose heaps
  mix key types, an encoding error, or a write error), or persistence is
  off. Always in the heap and counted in `asap_view_pinned_bytes`.

The committer is still the only writer of `ViewStore`. Each window is a
`WindowSlot` shared by every snapshot holding it, and its state is the one
thing that changes after commit: once a persist thread's write returns, it
moves the window's slot from `Pending` to `Stored` with its location (and
inserts the decoded batches into the cache), or to `Pinned` when the view
was not written. No snapshot needs republishing, and an event-time barrier
answered after the write sees the window `Stored`.

### Tiering policy

The window cache decides which `Stored` windows stay decoded:

1. **Insert on commit.** The committer inserts each committed window
   into the cache, so the newest windows, which every reader of a
   periodic query reads at its next evaluation, start hot. The entry and
   the window's `Pending` state share one `Arc`; its bytes count once.
2. **Recency.** Each read sets the entry's reference bit. A CLOCK hand
   (second chance) evicts entries whose bit is clear. An entry not read
   for `--view-cache-idle-ms` is evicted even when the budget has room;
   the default is 60 s, four 15 s dashboard refreshes (the installed plan
   does not carry its queries' intervals), so a window read by every
   evaluation never goes idle.
3. **Budget.** When the cache holds more than `--view-cache-max-bytes`
   (counted with the bytes `ViewStore::append` already records per window,
   independent of the states' held-outside-run flag), the CLOCK hand runs
   until it fits.
4. **Promotion.** A read of a cold window decodes it and inserts it, so a
   window read again soon is hot.

This approximates "hot = read by the next evaluation" without the store
knowing the executor's tiling: periodic readers mark the windows their
next evaluation needs, while windows only long ranges or edges read
(for example the middle fine windows under rollup levels, or the tail of
a long range request) age out. A plan-derived next-read time (Belady
order, possible because the readers' ranges and intervals are known) is a
follow-up if recency evicts badly in B2.

`Pending` and `Pinned` windows are never evicted: their bytes count
against the budget first, and the cache gets the rest.

### On-disk format

Per refresh group, a directory `group-<g>/` of time segments
`<first window end ms>.seg`. Each segment is append-only. It is
**active** while its persist thread appends to it and **sealed** once the
thread rolls to the next one; a sealed segment never changes again until
it is deleted whole. A segment holds the existing framing (a little-endian
`u32` length and a payload per frame), with every frame starting on an
8-byte boundary (zero padding) so fixed-width columns can be borrowed in
place:

- a header frame (MessagePack): plan hash, group, **format version 2**,
  and the segment start;
- one record frame per refreshed window. Its head is a small MessagePack
  map, followed by the parts it points at:

```
Record v2 head = { start_ms, end_ms,
                   texts: (offset, len),   // texts new to this segment
                   grids: (offset, len),   // Hydra-Count-Min grids, ASAPv1, once per record
                   views: [(view id, offset, len)] }   // one columnar fragment per view
```

**Columnar fragments.** Each view's rows of the window are one fragment,
laid out by column (all offsets relative to the fragment, every section
8-byte aligned):

```
Fragment = { rows: u32, columns: [(kind, offset, len)] }
  text column       u32 keys into the segment's dictionary (+ validity bitmap)
  timestamp column  i64 per row, or one i64 when every row has the same value
                    (the window end, today stored once per row)
  plain column      f64 / i64 / u64 / bool per row (+ validity bitmap)
  state column      family tag, then u32 offsets[rows + 1] and the states'
                    bytes back to back: ASAPv1 through sketch() for sketches,
                    the existing serde form for exact and increase states,
                    a grid index plus group name for Hydra-Count-Min groups
```

What each state's bytes are does not change; only the layout around them
does (columns instead of row-wise `Vec<Vec<StoredValue>>`). That removes
the per-value enum tags and the per-row identity indices interleaved with
states (exact states take about 140 B per state on disk today for a 48 B
counter), and it lets fixed-width columns be read without decoding.

**Segment dictionary.** Texts (series identities and label values) are
stored once per segment: a record's `texts` part is a string column
(`u32` offsets and UTF-8 bytes, the Arrow layout) of texts new to the
segment, and a segment's first use of a text writes it again. Deleting an
old segment then never strands a later one's keys. At 10k series and about
96 B per identity this costs about 1–2 MB per segment.

Index: `WindowLocation { segment: Arc<Segment>, offset: u64, len: u32 }`
points at a view's fragment. It lives in the `ViewWindow` in memory (about
40 B per (view, window)); there is no index file. At startup the index is
rebuilt by reading only record heads, never fragments.

The index is per (view, window), not per (series, window). A view read
(`MaterializedViewRequest { time_range_ms, schema }`) always returns whole
windows; nothing reads one series of a view window. A per-series index
would add bytes to every row for no reader.

Segments roll every `--view-segment-ms` (default 15 min, so a 1 h view
spans about five). The persist thread that owns a group deletes (unlinks)
a sealed segment once every record in it ends at or before every view's
oldest kept window end. Readers holding an `Arc<Segment>` keep its mapping
and file descriptor, so an unlinked segment stays readable until they
finish. Disk use is then retention plus one segment, not unbounded.

A file in the old format (`group-<g>.log`, version 1) is renamed
`*.stale` and not loaded, as a file of another plan is today.

### Read path

`RunStore::read_materialized_view` returns a stream of the windows
tiling the range, as today. Each window yields:

- `Pending`/`Pinned`: its batches;
- `Stored` and in the cache: the cached `Arc<Vec<Batch>>` (reference bit
  set);
- `Stored` and not cached: the cache's single-flight `get_or_load`, which
  decodes the fragment from its segment's bytes, inserts the batches and
  returns them. Other runs needing the same window wait for that decode.

Decoding happens lazily as the stream is pulled, so a `SummaryMerge` over
240 windows holds at most the window it is merging plus its accumulator,
not 240 decoded windows.

**`mmap` of sealed segments.** A sealed segment is mapped once, read-only
(`MAP_SHARED`, `PROT_READ`), when its first window is read cold, and
unmapped when its last `Arc<Segment>` drops. Decoding reads directly from
the mapped bytes:

- text keys, timestamps and plain columns become Arrow buffers over the
  mapping itself (`Buffer::from_custom_allocation` with the
  `Arc<Segment>` as owner): no copy;
- the segment dictionary is a `StringArray` over the mapping, shared by
  every fragment of the segment, and a text column is a
  `DictionaryArray<UInt32>` of borrowed keys over it: no copy and no
  per-row allocation;
- states are rebuilt from their bytes in place (`from_asapv1(&[u8])` on
  the mapped slice) into owned sketch structures; this is the remaining
  copy, inherent to the executor's in-memory state types.

`SIGBUS` comes only from accessing a mapped page past the end of the file.
That cannot happen here: only sealed segments are mapped, a sealed
segment's length never changes, a torn tail is cut off only at startup
before any mapping, and deletion is `unlink`, which keeps the mapped pages
valid. A disk read error under a mapping would also raise `SIGBUS`
instead of an error; `--view-read=pread` reads every fragment with
`pread` into a buffer and decodes the same way (with copies), for hosts
where that matters or a measured case prefers it. B3 compares the two.

**The active segment** holds the newest windows, which are mostly still
in the cache. A cold read of one of them uses `pread` of the fragment
into an 8-byte-aligned buffer, then the same decoder; mapping a file that
is still growing would need re-mapping as it grows and is not worth it for
the few windows involved. When the segment is sealed, later reads map it.

**Decode cost** (measured below, today's row-wise format) is about 3 µs
per state, dominated by rebuilding sketch structures and the row-to-batch
conversion. The columnar fragment removes the conversion and the per-row
text decode; B3 reports the new cost per kernel. It is paid once per cache
miss, not per read.

**Range requests.** A range request takes one snapshot for all its steps
(the demo design's read-once snapshot). Its steps share cold windows
through the window cache, not through a per-request map like `RawReads`:
a per-request map of decoded windows would hold every window of the span
plus the range (480 windows for a 1 h range of 1 h queries) outside any
budget. Steps are dispatched in time order, so the windows the next step
needs are the ones the previous steps just inserted.

**Result cache.** Unchanged. A hit reads no view; a refreshed range
computes only its new steps, which read the newest windows, which are hot.
Final answers stay bounded by `--result-cache-bytes`.

**Query-time results** (`OneEvaluation`) are outputs of runs, not view
windows; unchanged and bounded by `--query-time-results-max-bytes`.

**Hierarchical rollup levels.** With levels, a step of `quantile_over_time
(0.99, lat[1h])` reads about 12 coarse windows and its unaligned fine
edges (demo design: about 10–15 windows with four levels, at most 49 with
15 s + 5 min) instead of 240. The fine windows in the middle of the hour
are read only when a coarse window is built (once, when it closes, while
they are still hot) and by queries whose edges fall on them. They go idle
and leave the cache, so the hot set shrinks by about the same factor as
the merges, and cold reads stay rare. Nothing in this design depends on
levels: each level is its own view, tiered the same way.

### Memory accounting under a cgroup

The cgroup charges the data plane for:

| Memory | Kind | Bounded by |
| --- | --- | --- |
| Window cache (hot windows) | anonymous | `--view-cache-max-bytes` |
| `Pending` and `Pinned` windows | anonymous | the persist backlog (`--max-persist-backlog-bytes`) and pinned views |
| Raw panes | anonymous | open windows and `Scan` lookbacks (unchanged) |
| Query runs | anonymous | query workers × `--run-memory-limit-bytes` |
| Query-time results, result cache | anonymous | their byte options (1 GiB and 256 MiB) |
| Persist backlog | anonymous | `--max-persist-backlog-bytes` (256 MiB) |
| Allocator dirty pages and metadata | anonymous | jemalloc decay settings |
| Cold segments read | file (clean) | reclaimable by the kernel |
| Segments being written | file (dirty, then clean after sync) | the sync interval (5 s) |

Only anonymous memory can trigger the OOM killer; clean file pages are
reclaimed first. So "never OOM because of cold windows" holds when the
anonymous sum stays under `memory.max`:

- **Default budget.** When the data plane runs in a cgroup with a
  `memory.max`, `--view-cache-max-bytes` defaults to `memory.max` minus
  the other bounded pools above minus a headroom of 10 % (for page cache
  of the segments being written and the newest cold reads). Without a
  limit it defaults to unbounded, which keeps today's behavior except
  that idle windows leave the heap.
- **Backstop.** A memory monitor thread reads `memory.current` and
  `memory.stat` (`anon`, `file`) once per second. When `anon` passes the
  budget's assumption (other pools larger than estimated: a long range's
  steps, allocator slack), it lowers the cache's target for the next
  second, down to zero cached `Stored` windows. Every view read then
  decodes from the page cache: slower, still answered.
- **Allocator.** Freed decoded windows must leave RSS. The jemalloc
  settings from the memory investigation
  (`background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0`) become
  the default in `main.rs`; otherwise evicted windows stay resident as
  dirty pages.
- **Hints.** Each mapping gets `MADV_RANDOM` (a read takes exactly one
  fragment; readahead of neighbors wastes page cache under a limit); a
  fragment about to be decoded gets `MADV_WILLNEED` over its range; when
  a window leaves the cache its fragment's range gets `MADV_COLD`, so the
  kernel reclaims those pages first. The startup scan of record heads
  uses `POSIX_FADV_SEQUENTIAL`. No `POSIX_FADV_DONTNEED` after writing:
  whether a just-written window will be read cold is not known, and the
  kernel's LRU already evicts unread pages first.
- **RSS reporting.** Mapped pages that were touched count in the
  process's RSS as `RssFile`, although they are clean and reclaimable.
  Reports split RSS into `RssAnon` (what can OOM) and `RssFile`, next to
  the cgroup's `anon` and `file`, so the comparison with VictoriaMetrics
  (which also maps its files) stays fair. A cached decoded window whose
  fixed-width columns borrow the mapping holds those columns in page
  cache, not in the heap.

`asap_view_bytes` is split: `asap_view_cache_bytes` (hot, decoded),
`asap_view_pending_bytes`, `asap_view_pinned_bytes`, and
`asap_view_file_bytes` (on disk). The memory investigation showed the old
gauge undercounts once queries mark states held outside a run; the new
gauges count bytes recorded at append, which the budget also uses.

### Eviction under `memory.max`

- Eviction drops the cache's `Arc`; the memory is freed once runs that
  hold the batches finish. Overshoot is bounded by the windows in flight:
  query workers × windows one run holds at once (one per streaming view
  read).
- A cold read inserts into the cache only if, after evicting, the window
  fits the target. Otherwise the decoded batches go to the run alone and
  are freed when it finishes, so a budget of zero still answers.
- Page cache of cold segments is reclaimed by the kernel; a read after
  that goes to disk. That is the "slows down rather than OOM" behavior
  VictoriaMetrics has under the same limit.
- `Pending` windows are bounded by the persist backlog: when the disk
  cannot keep up, the committer waits (`asap_view_persist_wait_seconds_total`)
  and Remote Write gets 503, as today.

### Recovery

- At startup, for each group directory: segments are listed by start;
  each is opened, its header checked (another plan, group or version →
  renamed `*.stale`), its frames scanned for record ranges, texts and the
  views' slices; a torn tail of the last segment is cut off, as today.
- Windows within each view's retention of its newest window become
  `Stored` entries with their locations. Nothing is decoded at startup;
  the first evaluations decode what they read and fill the cache.
- Startup reads each segment once (sequentially) and decodes only the
  texts, so it takes about the time to read the files, not to decode
  them (today it decodes every window; measured below).
- Durability is unchanged: a window is durable within the sync interval
  (5 s); after a crash the windows of the last interval may be missing.
  A window is evicted from the heap only after its record is written, so
  a running data plane never loses a window it has answered from: reads
  of written-but-unsynced bytes come from the page cache.

### Concurrency

- **Ingest shards** (and push execution from parallel precompute): do not
  touch the store or the files. Per-shard partial states are never
  persisted.
- **Committer**: still the only writer of `ViewStore`; it appends
  `Pending` windows, drops windows past retention,
  and publishes snapshots, as today. It also inserts committed windows into
  the cache.
- **Persist threads**: one writer per group directory (group `g` on
  thread `g % n`, as today). They encode, append, sync, roll and delete
  segments, and move written windows' slots to `Stored`. Encoding is done once; the bytes written are
  the bytes later read.
- **Query workers**: read snapshots without locks. The cache is a sharded
  map (64 shards, as `RawReads`) with one `OnceLock` per entry for single
  flight; eviction takes one shard lock at a time.
- **Ordering**: a window's location exists only after its bytes are in the
  file, and a segment is deleted only after the store dropped all its
  windows and readers holding it finish (by `Arc`), so a read never sees a
  location without bytes.

### Measured expectations

Measured on a 56-core x86 server (8 cores pinned, rotating disk) with a
scratch in-process probe (not committed): the dashboard plan (demo design
example 4) planned with raw data not retained (KLL quantile chain, Max,
Rate and Increase chains, 15 s windows, 1 h retention, 30 min for Max), 10k
series per metric at 1 sample/s, 70 min of samples, views persisted. One
run of each step.

**Today (everything hot):**

| | Value |
| --- | --- |
| Steady RSS (after 65 min) | 3.94–3.96 GB |
| `asap_view_bytes` before any query | 3.08 GB |
| View files written per hour | 1.46 GB (about 6 MB per 15 s window, four views) |
| of which the three exact views | 0.85 GB/h (about 140 B per state on disk; about 220 B in the heap) |
| of which the KLL view | about 0.6 GB/h (about 250 B per state on disk; about 960 B in the heap) |
| Query latency, warm heap | q01 quantile 4.6–7.2 s; q02 topk-rate and q04 increase 1.0–1.2 s; q03 max 0.45–0.56 s |
| Load at restart (decodes every window in retention: about 856 (view, window) pairs, 8.5M states, page cache warm) | 26.2–26.6 s on one thread: about 31 ms per (view, window) of 10k rows, about 3.1 µs per state |
| Reading the 1.03 GB file, warm page cache / cold (rotating disk) | 0.23 s / 15.8 s; a random 3.5 MB `pread` cold: 78 ms |

So the encoded windows are about 2.7x smaller than the decoded ones (3.8x
for KLL, 1.6x for the exact states), and decoding costs about 3 µs per
state. The row-wise MessagePack encoding is bloated for exact states
(140 B for a 48 B counter state plus a 4 B identity index), which is why
version 2 is columnar.

**Expected after tiering** (derived from the numbers above, to be measured
by B1 and B2):

| Setting | Heap for views | Page cache (reclaimable) | Query cost |
| --- | --- | --- | --- |
| Default outside a cgroup (unbounded budget) | about 3.1 GB, as today: every window is read every 15 s | up to 1.46 GB | as today |
| Budget 1.0 GB (for example `memory.max` 1.5 GiB), no rollups | 1.0 GB; RSS about 1.4 GB | 1.46 GB, reclaimed as needed | about 170 of 244 windows per view cold per evaluation: +5 s CPU per 1 h query with the page cache warm; with it reclaimed, + about 1 GB read from disk per evaluation round (about 15 s on this disk) |
| Budget 0 (fallback), no rollups | `Pending` windows only (tens of MB) | 1.46 GB | +7.5 s CPU per 1 h query (244 windows × 31 ms) |
| Rollup levels 15 s + 5 min, default budget | about 0.6–0.9 GB: 12 coarse + 20–40 fine windows per 1 h view stay read; the middle fine windows go idle | 1.5 GB plus the coarse level | about as today's hot path, with 5–8x fewer merges |

Without rollup levels, tiering trades memory for decode CPU on this
workload, because every evaluation reads the whole hour. The cgroup case
is still the point: the data plane answers, slower, instead of being
killed. Rollup levels are what make the cold tier nearly free.

### What the Planner cost model would need

This is a requirements list for a separate ASAPPlanner change; nothing
here changes the Planner.

1. **A placement per view**: heap (today) or file-backed (tiered), or a
   number of hot windows per view, as a deployment capability
   (`view_file_tier`, with a memory budget and disk read bandwidth).
2. **Heap bytes per state, measured**: the decoded size including fixed
   overhead (KLL about 0.96 KB, DDSketch 1.2–1.5 KB at 15 samples), not
   the encoded size.
3. **Encoded bytes per state**: the file and page-cache size per state as
   a function of the kernel's parameters and samples per window.
4. **Decode cost per state** per kernel, plus a per-row cost for the
   row-to-batch conversion, from calibration.
5. **Cold reads per evaluation**: for each reader, the windows it reads
   per evaluation that are not hot (from its range, interval, tiling and
   rollup levels, and the hot set), times its evaluation rate, giving
   decode CPU per second and read bytes per second.
6. **Memory objective**: hot windows charged fully; file pages not charged
   as memory (reclaimable), or charged at a discount; disk bytes charged as
   storage.
7. **Write cost**: encoding and writing each window once (already paid by
   persistence; it should be charged whether or not the view is tiered).
8. **Rollup levels and tiering together**: levels lower both merges and
   cold reads, so their cost should be priced with the tier.

### Alternatives and decisions

| Question | Decision | Rejected and why |
| --- | --- | --- |
| Where cold windows live | the persistence files | a second cold file (double write); encoded bytes in the heap (compact, but not reclaimable, so the cgroup OOM stays) |
| Read primitive | `mmap` of sealed segments, decoding in place (fixed-width columns and the dictionary borrowed, states rebuilt from the mapped bytes); `pread` for the active segment and as `--view-read=pread` | `pread` everywhere: copies every fragment into a buffer, and the row data again into Arrow arrays; kept as the fallback. `mmap` of the active segment: needs re-mapping as it grows, for windows that are mostly still cached |
| Fragment encoding | columnar per (view, window), state bytes unchanged | row-wise `Vec<Vec<StoredValue>>` as today: per-value tags and interleaved identity indices (140 B per exact state), a row-to-batch conversion on every decode, nothing to borrow |
| Index granularity | (view, window) → slice, in memory | (series, window) → offset: no reader reads one series of a view window; an index file: rebuilt from the frames at startup instead |
| Hot policy | CLOCK recency + idle time + byte budget, insert on commit | plan-derived next-read (couples the store to the executor's tiling; follow-up if B2 shows bad evictions); LRU list (a lock on every read) |
| Text dictionary | per segment | per file (deleting old segments strands indices); none (every row repeats a 96 B identity) |
| Disk growth | time segments deleted past retention | compaction of one file per group |
| Old files | renamed `*.stale` | reading version 1 (all windows decoded at load, as today): a second code path for a draft format |
| Shrink decoded states instead | complementary, not instead | compact sealed KLL, shared identity columns and smaller exact states cut the hot bytes about 4x (memory investigation), but they stay unreclaimable and still grow with retention; tiering bounds them. Both are wanted |
| Budget source | cgroup `memory.max` minus bounded pools, plus a 1 s monitor | a fixed default (wrong on every host); `memory.high` throttling alone (slows the whole process, including ingest) |

### Decided at review

- **Read path:** `mmap` of sealed segments with in-place decoding;
  `pread` only for the active segment and as a fallback option. B3 still
  compares the two.
- **Hot policy:** recency (CLOCK, idle time, byte budget).
- **Fragment encoding:** columnar, with format version 2.
- **No cgroup limit:** no budget, plus idle eviction.
- **Old view files:** renamed `*.stale`.

## Acceptance and test design (for approval)

Each test states what it verifies, its inputs, the expected result, and
whether it fails today ("fails today" means it fails or cannot be built).
The tests were written after this plan was approved and before the
implementation; they fail until it lands. Unless a row says otherwise, tiering is compared with the
same build with tiering off (`--view-cache-max-bytes` unbounded and
`--view-cache-idle-ms 0`, which keeps every window hot).

| # | Behavior verified | Inputs | Expected | Today |
| --- | --- | --- | --- | --- |
| V1 | Answers are identical with tiering on and off. | All parity workloads (`tests/workloads/`), raw data kept and not kept, ingest shards 1, 4 and 8 (`PARITY_INGEST_THREADS`), instant and range queries; tiering with a budget of zero (every read cold). | Equal to the Planner executor and to tiering off. KLL plans within the existing 5 %, as today. | Fails: no tiering options. |
| V2 | A cold window decodes, from its columnar fragment, to the rows that were committed. | One window per state family a view column can hold (DDSketch, KLL, Count-Min, KMV, HLL, Count-Min and Count Sketch with heap, UnivMon, Hydra-Count-Min groups, exact Sum, Max and Counter), with a text column, a timestamp column and nulls; written, then read back through its location from a sealed (mapped) and from the active segment. | Same rows: texts equal, states equal by `sketch()` bytes or exact value. (Hydra-KLL and heapless weighted frequency cannot be a view column today.) | Fails: no fragment format, no locations. |
| V3 | A view's fragment decodes without the record's other views. | A record with four views; the bytes of view 2's fragment overwritten. | Views 1, 3 and 4 read correctly; view 2's read returns an error naming the view and window. | Fails: as V2. |
| V4 | The cache keeps windows recent reads touched and evicts idle ones. | Unit test on the cache: idle time 30 s, budget unbounded; windows read every 15 s and windows never read. | Read windows stay; unread ones leave after 30 s; eviction never touches `Pending` or `Pinned`. | Fails: no cache. |
| V5 | The byte budget holds. | Budget = 3 windows' bytes; 10 windows committed; reads of every window in turn. | Cached bytes ≤ budget after every insert; reads all answered; misses counted. | Fails: no cache. |
| V6 | Concurrent reads of one cold window decode it once. | 8 query workers reading the same cold window at once (a hook slows the decode). | One decode (`asap_view_cold_reads_total` +1), 8 equal answers. | Fails: no cache. |
| V7 | A window leaves the heap only after its record is written. | Persist delay hook (`persist_delay_ms`); budget zero; result cache off. | Until the write, the window is `Pending` and read from the heap; after the write it is `Stored`; answers unchanged throughout. | Fails: no window states. |
| V8 | Windows that cannot be persisted stay hot. | A UnivMon view whose heaps mix key types; persistence on, budget zero. | Answered from the heap; `asap_view_pinned_bytes` > 0; never evicted. | Fails: no pinned state. |
| V9 | Segments roll and are deleted past retention; disk is bounded. | 1 min retention, `--view-segment-ms` 15 s, 5 min of samples. | At most ⌈(retention + margin) / segment⌉ + 1 segments per group; `asap_view_file_bytes` flat after the first minute; answers unchanged. | Fails: one file per group grows without bound. |
| V10 | A segment deleted while a run reads it stays readable. | A hook holds a run after it took its snapshot; the segment it reads passes retention and is unlinked. | The run's answer is complete; the file is gone after it finishes. | Fails: no segments. |
| V11 | Restart reads the same files without decoding them up front. | Write 10 min at 1k series; stop; restart with no samples written. | Answers equal before and after; startup decodes no window (`asap_view_cold_reads_total` = 0 until the first query); `asap_view_windows_loaded_total` counts the windows indexed. | Fails: load decodes every window. |
| V12 | Crash recovery: a torn record at the end of the last segment is cut off. | Kill (`SIGKILL`) a child data plane mid-write (`persist_delay_ms` plus a write split by a hook), or truncate the last segment by a few bytes. | Restart loads every whole record; later appends stay aligned; answers for whole windows equal. | Guard: passes today for one file; must keep passing per segment. |
| V13 | Old-format and other-plan files are not loaded. | A version 1 `group-0.log`; a segment with another plan hash. | Renamed `*.stale`; nothing loaded; no panic. | Fails for version 1 (loaded today). |
| V14 | Budget defaults from the cgroup. | Unit test of the budget function over a fake cgroup directory (`memory.max`, `memory.stat`). | Budget = `memory.max` − pools − 10 %; `max` (no limit) gives unbounded; the monitor lowers the target when `anon` passes it. | Fails: no monitor. |
| V15 | No OOM under a small `memory.max`. | The dashboard plan at 10k series, 1 sample/s, 1 h retention, run in a cgroup with `memory.max` = 1.5 GiB (below today's view heap), swap off; 70 min; instant queries every 15 s. Script under `scripts/`, not CI. | No OOM kill (`memory.events` `oom_kill` 0); every query answered; latency reported. With tiering off the same run is OOM-killed (the control). | Fails: OOM-killed today. |
| V16 | Concurrency: commits, persist acks, segment deletion and queries race safely. | Loom-free stress test: 8 ingest shards, 4 precompute threads, 2 persist threads, 8 query workers, budget = 2 windows, segment 5 s, retention 30 s, 3 min. | Every answer equals the tiering-off run at the same T; no panic. | Fails: no tiering. |
| V17 | Metrics. | Any plan, a few windows, budget zero. | `asap_view_cache_{bytes,entries}`, `asap_view_{pending,pinned,file}_bytes`, `asap_view_cache_{hits,misses,evictions}_total`, `asap_view_cold_reads_total`, `asap_view_cold_read_bytes_total`, `phase="view_decode"` CPU. | Fails: new series. |
| B1 | Bench at 10k (reported, not CI). | Dashboard-heavy and typical-dashboard at 10k series, 1 sample/s, in the harness's cgroups with the same `memory.max` for both engines; tiering off, on with the default budget, on with a budget of zero. | RSS, `memory.current` split into anon and file, peak and steady; query latency per query; CPU per phase incl. `view_decode`; disk bytes. | Tiering-off numbers come from today's code. |
| B2 | Cold versus warm page cache (reported). | As B1, budget zero; before a round of queries, drop the segments' pages (`POSIX_FADV_DONTNEED` on each file) for the cold case. | Query latency and read bytes cold vs warm; cache hit ratio over a run (checks the recency policy). | — |
| V18 | Fixed-width columns and texts are borrowed from the mapping, not copied. | A window of 1,000 rows read cold from a sealed segment. | The text keys' and timestamps' buffer addresses lie inside the segment's mapping; the dictionary values array is shared by every window of the segment. With `--view-read=pread` they lie outside it, and the rows are equal. | Fails: as V2. |
| V19 | No `SIGBUS`: only sealed segments are mapped, and a sealed segment is never shortened. | Read a window of the active segment (by `pread`), seal it, read again (mapped); unlink a mapped segment and read through a held location; restart over a segment with a torn tail. | All reads equal; the torn tail is cut before any mapping; no crash. | Fails: as V2. |
| B3 | `pread` versus `mmap` (reported). | `examples/view_read_bench.rs` over one 10k-series segment of the dashboard plan: decode each window with `--view-read=mmap` and `pread`, page cache warm and cold. | ns per window and per state by family, bytes copied, `RssAnon`/`RssFile`. | — |

Parity, crash recovery and eviction are V1, V12 and V15. The test design is
by the author of this document; an independent review is requested in the
PR.

## Implementation plan

| Step | Work |
| --- | --- |
| 0 | V1–V17 and B1–B3, failing (after approval) |
| 1 | Record v2 and segments: writer, scanner, per-segment dictionary, roll and delete (V2, V3, V9, V12, V13) |
| 2 | Window states and persist acks; startup index without decode (V7, V8, V11) |
| 3 | Window cache: CLOCK, idle time, budget, single flight; lazy cold reads in `read_materialized_view` (V4–V6, V10) |
| 4 | cgroup budget and monitor, jemalloc defaults, metrics (V14, V17) |
| 5 | Parity and stress (V1, V16); B1–B3 and the cgroup run (V15) |

## Risks, rollout, and exit criteria

- **Decode CPU.** Without rollup levels the dashboard reads every window
  at every evaluation; a budget below the hot set turns memory into decode
  CPU per query. B1 reports it; the default budget is unbounded outside a
  cgroup.
- **Recency is not the plan's schedule.** A reader with a long interval
  (a 1 h range every 5 min) may find its windows evicted between
  evaluations. The idle time defaults to twice the longest reader interval
  for that reason; B2 reports the hit ratio.
- **Budget estimate.** Bytes per window come from `Batch` accounting,
  which is about 10 % below the allocator's (KLL: 872 B counted vs 964 B
  allocated); the monitor is the backstop.
- **Format change.** Old view files are not loaded after the upgrade.
- **Rollout.** Tiering is on whenever persistence is on; the cache budget
  is unbounded outside a cgroup, so behavior there changes only by idle
  eviction. `--view-cache-idle-ms 0` turns that off.
- **Exit.** If B1 shows no RSS drop for typical-dashboard at the default
  budget, or V15 still OOMs, the design is revisited before more work.
