# Data plane and demo plan

> **Paths.** This document predates the folder moves of
> `docs/design/repo-layout.md` and keeps its original paths: `data_plane/` is
> now `crates/data-plane/`, `control_plane/` is `crates/control-plane/`,
> `examples/*_bench.rs` are in `benchmarks/micro/` (`remote_write_load.rs`
> stays in `crates/data-plane/examples/`), the workloads in `examples/` are in
> `docs/examples/`, `scripts/profile.sh` and `scripts/tiered_views_cgroup.sh`
> are in `benchmarks/scripts/`, and `bench/demo/` is split into `eval/`,
> `eval/demo/` and `deployment/monitoring/`. The data plane's sources are
> split into crates: `src/{ingestion,push}.rs` and the ingest and
> precompute threads of `src/engine.rs` are in `crates/precompute/`,
> `src/{query,result_cache}.rs` and the query workers in `crates/query/`,
> and the stores, persistence and the persist threads in `crates/storage/`.

This document describes the data plane as it stands on `main` and the
plan for the single-server demo against VictoriaMetrics. The component
designs it summarises are in `data-plane-demo.md` (ingest, precompute,
queries), `parallel-precompute.md` (push pipelines) and
`tiered-view-storage.md` (view storage); this document ties them together
and records the demo's workload, method and decisions.

## 1. Goal

On one machine, under the same CPU, memory and disk limits, the ASAP data
plane executes the queries ASAPPlanner planned ahead of time faster, with
less CPU, memory and disk, and fresher data than VictoriaMetrics single-node,
within each query's accuracy target.

The demo shows six things:

1. ASAPPlanner plans for a given resource limit and minimises the queries'
   mean latency; its planning time and memory are reported separately
   (planning is offline).
2. Under query load, VictoriaMetrics' ingestion falls behind or it rejects
   queries, so its windows miss samples; the ASAP data plane is unaffected.
3. ASAP answers with lower latency and fresher data.
4. ASAP uses less CPU, memory (working set) and disk.
5. All of it is live on a Grafana dashboard.
6. Every answer is checked against ground truth and stays within its target.

## 2. Architecture

```
Remote Write -> ingest shards (xN) -> coordinator -> precompute task pool
                                                          |
                                                    window close
                                                          v
HTTP query API <- query workers <------------------- view store
                       ^                     (segments + mmap, hot cache)
                       |
                control plane: plans with ASAPPlanner, installs the plan
```

The control plane splits ASAPPlanner's plan (`PhysicalASAPDAG`) into
ingestion-time views and query-time roots. Ingest, precompute and queries
run on separate thread pools; persistence runs in the background. Queries
read views (and, for query-time scans, the raw panes), never the raw stream.

## 3. Ingest

- Remote Write is decoded in place, into a per-thread decompression
  buffer, with a per-thread series cache that predicts each series from
  the sender's order (about 0.15 µs per sample on captured demo
  requests, 0.21 µs in the server).
- Samples are sharded by series hash across N ingest threads, each
  appending to columnar raw panes.
- A single coordinator collects shard reports, sends flushes and advances
  the watermark. The watermark moves only behind a flush, capped at the
  time the flush was sent, so it never passes samples still in a shard.
- A window closes once event time is past its end plus the allowed
  lateness (2 s); later samples are counted and dropped.
- Backpressure: a shard's queue is drained only when the coordinator
  applies its report, so a precompute that falls behind fills the queues
  and Remote Write returns 503.
- Known limit: the coordinator is single-threaded and its cost grows with
  the request count; it is the scaling limit beyond about 28 cores.

## 4. Precompute

- Views with the same window width and phase form a refresh group; each
  closed window runs one `execute_ingestion`, reading shared scans once.
- A task pool (`--precompute-threads`) runs windows of different groups,
  and consecutive windows of one group, in parallel. A per-group reorder
  buffer commits windows in order; a group that reads another group's
  views waits for those windows; groups that carry state across windows
  run serially.
- A group whose nodes compute each series independently splits a window
  by series across threads and merges the parts byte-identically. At 10k
  series and 10 Hz a 60 s window is ready 3.5 s after it ends on 12
  threads (24.7 s on one).
- Multi-level window chains: long ranges use wider levels (for example
  15 s, 1 min, 5 min, 15 min). A wider level's window is merged from the
  narrower level's committed windows when it closes
  (`SummaryMergeWindows`); each level has its own retention, and a
  narrower level keeps the windows a wider level has yet to read.
- The demo runs push precompute (`--precompute-mode push`, the default):
  each ingest shard folds its series' samples, batched (32k samples or
  1 s), into the open windows' pipelines as they arrive, and at close the
  shards' partials are concatenated and committed. Groups that cannot be
  pushed, such as the wider levels of a window chain, keep the window path
  above; `--precompute-mode window` runs every group that way. On the demo
  workload push used 2% less CPU, 10–20% less working set and no raw
  samples, with the same window latency, query latency and answers; under
  catch-up load it accepted 1.6× the samples at 41% of the CPU
  (`parallel-precompute.md`, "Demo workload A/B").

## 5. View storage

- A window is pending (in the heap, waiting to be written) until its
  persist thread writes it; after that it is stored (segment, offset,
  length) and read from the file. A window that cannot be written stays
  in the heap.
- Each refresh group has a directory of 15-minute segments; a window's
  record holds one columnar fragment per view. The persisted file is the
  cold tier, written once; a segment is deleted when all its windows have
  expired.
- Zero-copy reads: states are stored as aligned little-endian fixed-width
  arrays. Read-only views of DDSketch, KLL and Count-Min estimate and merge
  directly from the mapped bytes; `SummaryMerge` folds batch by batch, so a
  query holds only its accumulators on the heap. Heap top-k, UnivMon and
  Hydra are still decoded.
- DDSketch stores only the occupied bucket range, at the narrowest count
  width (u8/u16/u32/u64), dense or sparse, whichever is smaller; merges add
  in place with width-specialised loops.
- A shared hot cache (CLOCK eviction, byte budget, 60 s idle eviction).
  The budget defaults to the cgroup's `memory.max` less the other pools;
  the demo runs with `--view-cache-max-bytes 0`.
- Persistence runs in the background: `sync_file_range` after each write,
  batched fsync every 5 s, and the persist queue never blocks ingest or
  precompute.
- Restart indexes the record headers without decoding, cuts a torn tail
  and renames old-format files to `*.stale`.

## 6. Queries

- Pull execution: each query is compiled once at install into a root, with
  views and shared nodes as input slots; one evaluation runs on one query
  worker.
- A `OneEvaluation` node shared by several queries is computed once per
  evaluation time (single flight).
- Result cache: final answers per (root, evaluation time); several users
  asking the same query at the same time cost one evaluation, and a range
  refresh computes only its new steps. The cache is not persisted.
- A range request reads one snapshot for all its steps; a failing step
  does not stop the others.
- Covers (`TimeRangeCover`) read the widest whole windows inside
  (T − W, T] and fill the edges from narrower levels; a 1 h range merges
  about 12 windows per step instead of 240. An evaluation time off the
  narrowest grid is an error, not a silent loss.
- A range that reaches back before a view's first window answers from the
  rows that exist, as PromQL does over a range that starts before the first
  sample (a 6 h query after 3 h of data answers over those 3 h). A gap after
  the first window is missing data and is refused.

## 7. Resources and memory accounting

Each engine runs in its own cgroup v2 from start-up, under identical limits.

| Resource | Limit | Recorded |
| --- | --- | --- |
| CPU | cpuset + `cpu.max` (average cores) | CPU seconds, average cores |
| Memory | `memory.max`, page cache included | working set (`memory.current` − inactive_file), anon, active/inactive file, dirty/writeback, kernel, peak, OOM events, pressure (PSI) |
| Disk | a fixed-size filesystem per engine | usage, bytes written, ENOSPC |

Working set and anon are the headline memory figures: VictoriaMetrics keeps
compressed data in files read through the page cache, which is mostly
reclaimable, so RSS (which counts mapped files) and the cgroup total
overstate it. An OOM kill or a full disk under the limit is a result and is
recorded; limits are never raised to make a run pass. Feeders, the harness
and the reference instance run on another machine.

## 8. Demo configuration

Hardware: one machine with 2 × 14-core CPUs (56 threads) and 251 GB of
memory. The engine under test gets 48 vCPU (24 physical cores and their
hyperthreads) and 100 GB, identical for ASAP and VictoriaMetrics
single-node. The data and query scale are raised until one VictoriaMetrics
server is close to its CPU limit (ingestion still keeping up, queries slow),
then ASAP runs at that scale. Feeders send over a 10 GbE link from a second
machine.

### Data

Every metric is sampled at 10 Hz with integer Zipf values (s = 1.1,
N = 1e6) and declared value ranges; about 2.5M samples/s from 3–4 feeders.
This is the starting point of the scale sweep.

| Metric | Type | Series | Samples/s |
| --- | --- | --- | --- |
| `data` | gauge (latency-like) | 40k | 400k |
| `data_total` | counter | 40k | 400k |
| `http_requests_total` / `http_errors_total` | counter | 10k / 10k | 200k |
| `cache_hits_total` / `cache_lookups_total` | counter | 10k / 10k | 200k |
| `queue_depth` / `capacity_rps` / `request_latency_seconds` | gauge | 10k each | 300k |
| `http_request_duration_seconds_bucket` | histogram | 10k × 8 `le` | 800k |
| `power` | gauge | 10k | 100k |
| `delivery_latency_bucket`, `calc_duration_bucket`, `data_latency_bucket` | histogram | 10k × 8 `le` each | 800k each |

The data is semantically valid: errors ≤ requests, hits ≤ lookups,
histogram buckets cumulative in `le`; 5% of counters reset every
30–120 min. Metrics joined `on(job)` have one series per job.

### Queries

| # | Query | Interval |
| --- | --- | --- |
| q01 | `quantile_over_time(0.99, data[1h])` | 10 s |
| q02 | `topk(10, sum by (region, service, job) (rate(data_total[1h])))` | 15 s |
| q03 | `max_over_time(data[30m])` | 10 s |
| q04 | `sum by (region, service) (increase(data_total[1h]))` | 15 s |
| q05 | q01 as a range panel (1 h span, 15 s step) | 15 s |
| A | `(increase(http_errors_total[6h]) / on(job) group_left() increase(http_requests_total[6h])) > 0.01` | 15 s |
| B | `(rate(http_requests_total[30s]) * 100 / on(job) group_left() max_over_time(capacity_rps[6h])) >= 80` | 15 s |
| C | `(avg by (region, service, job) (rate(cache_hits_total[60s]) / rate(cache_lookups_total[60s])) * 100) >= 90` | 15 s |
| D | `count by (region) (sum by (region, service) (increase(http_requests_total[30m])) > 1000)` | 15 s |
| E | `quantile_over_time(0.99, request_latency_seconds[6h])` | 15 s |
| G1 | `topk(10, increase(http_requests_total[1h]))` | 15 s |
| H | `histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[1h])))` | 15 s |
| I | `sum by (region) (max by (region, service) (max_over_time(queue_depth[1h])))` | 15 s |
| J | `max by (job) (max_over_time(queue_depth[1h])) > max by (job) (max_over_time(queue_depth[1h] offset 1h))` | 15 s |
| K | `sum(increase(http_errors_total[1h])) / sum(increase(http_requests_total[1h]))` | 15 s |
| L1 | `stddev_over_time(power{host=~".*"}[6h])` | 15 s |
| L2 | `quantile_over_time(1., power{host=~"<pattern>"}[6h])` | 15 s |
| L3 | `quantile_over_time(0.99, power{host=~".*"}[6h])` | 15 s |
| L4 | `sum by (le) (rate(delivery_latency_bucket{region=~"<pattern>", host=~"<200+ hosts>", kind="written"}[40s]))` | 15 s |
| L5 | `histogram_quantile(0.95, sum by (le, svc) (rate(label_replace(calc_duration_bucket{env="prod"}, "svc", "$1", "service", "<regex>")[1m]))) > 2` | 15 s |
| L6 | `histogram_quantile(0.95, sum by (le, app, instance) (rate(data_latency_bucket{app=~"<pattern>", instance=~"<pattern>", env="prod"}[30s]))) offset 30s` | 15 s |

L1–L6 are common slow-query shapes (long lookbacks, quantiles over long
windows, high-cardinality series with broad regex matchers, histogram
quantiles with `label_replace` and `offset`) with their windows capped at
6 h. Queries that read each series' latest sample (instant selectors joined
with `and on`, `unless on`, several `group_left` levels) cannot be planned
without raw data; they are shown only in forward mode (section 10), answered
by VictoriaMetrics and labelled as such.

Users: 1 → 4 → 8; every user sends every query, spread over its interval.
Every query is sent 6 s after its evaluation time, by both engines alike.

### Run

Both engines start from empty and ingest at real time for about 6 h while
every query runs from the start. There is no backfill. Each engine's total
cost over the whole run is counted from second 0: ASAP's ingest, precompute,
persistence and queries; VictoriaMetrics' ingest, background merges and
queries. The arms run one after another on the same machine and cpuset, so
the hardware is identical (two hosts of the same model differed 1.36× in
clock speed).

Grafana shows, live and side by side, the totals from the start to now:
CPU core-seconds and current cores; working set and anon now and their
peak; disk bytes written and in use; requests by outcome and goodput;
each query's latency (p50/p99) and error; and whether ingestion keeps up
(ingest lag, visible fraction of the window's samples).

## 9. Method and fairness

- Both engines get the same data, queries and users. Every request is
  classified: on time and accurate (within its interval and within 5%),
  inaccurate (missing samples vs approximation), slow, rejected, timed
  out, partial, unverified. The headline is goodput: accurate on-time
  answers per second.
- Every query reports mean, p50, p99 and max latency, timeouts and
  rejections.
- VictoriaMetrics v1.156.0 keeps its default concurrency protection, so its
  self-protection shows as rejections. `-search.latencyOffset` is 6 s,
  matching the query delay. `-search.maxQueryDuration` and the client
  timeout are 600 s, so latencies above 100 s are measured rather than cut
  at 30 s.
- Ground truth: rate and increase use Prometheus' extrapolated-rate
  semantics over the complete raw data; other queries use a reference
  VictoriaMetrics instance that receives no query load. VictoriaMetrics'
  own answers are scored against MetricsQL semantics. While a window is
  filling, the truth uses the same partial-window semantics as both
  engines.
- A visibility probe measures the fraction of a window's samples visible at
  each evaluation, separating ingestion lag from approximation error.
- Self-protection counters are sampled every 5 s: VictoriaMetrics'
  concurrency-limit hits, queue timeouts and slow queries; ASAP's
  refusals, 503s and late samples.
- Per-query cost: (1) sampled VictoriaMetrics query traces (`trace=1`:
  blocks, rows and series read, time per step); (2) `/metrics` scraped every
  second (CPU, memory, rows read per query), apportioned to the requests in
  flight; (3) a pass running each query alone to check (2). ASAP reports its
  own per-evaluation time and windows merged.

## 10. Results so far and next steps

Candidate backend: the pre-merge branch at 60ae5ada (ASAPPlanner 846a70a,
asap_sketchlib 355093f). All data plane tests and the parity test pass.

Results at 10k series and 10 Hz, 12 cores, each engine alone:

| | ASAP | VictoriaMetrics |
| --- | --- | --- |
| Working set at `memory.max` 3 GiB | 356 MiB | 1361–1437 MiB |
| q01 mean latency | 0.26 s | 15.1–15.7 s |
| q02–q04 mean latency | 0.09–0.13 s | 4.0–7.7 s |
| Requests answered within target (1 user) | 204/204 | 148–151/204 |
| Goodput, 8 users, range load | 3.72/s (87.5%) | 0.59/s (13.9%) |
| CPU under that load | 3.6 cores | 11.7 cores |

- Accuracy: every verified point is within 5%; rate and increase match the
  Prometheus truth exactly; counter resets are handled; ten complex queries
  are all exact and 4–15× faster than VictoriaMetrics. The ASAP-alone run on
  the pre-merge branch at 60ae5ada still needs a run with a VictoriaMetrics arm to verify accuracy.
- VictoriaMetrics fails in two ways: under instant-query load from two or
  three users its ingestion falls minutes behind and its answers go stale;
  under range load it rejects most queries with 429.

Next, after the 48-core demo:

1. Forward mode: designed and implemented, see §10.1.
2. A compressed raw store, so the data plane answers any query itself:
   compressed blocks written by the ingest shards, a label inverted index
   and time-range reads. Existing options (Parquet/Arrow with DataFusion,
   embedded time-series libraries, roaring-bitmap indexes) are evaluated
   before a design.
3. Multi-level windows under cost model 3: start from the plan solved
   without levels and cap the search by a node limit, reporting the gap.

### 10.1 Forward mode

Forward mode is an optional switch, off by default. With it on, the data plane
also sends every raw sample to a VictoriaMetrics single-node used as the raw
store. That lets the demo answer any query: planned queries are answered by
ASAP, and everything else by VictoriaMetrics.

**Switch.** `--raw-store-url URL`, the base URL of the VictoriaMetrics instance.

- Off (no flag), the data plane behaves exactly as before. The forwarder does
  not exist, no extra route is served, and no metric is added.
- It cannot be combined with `--no-forward`, which refuses unplanned queries.
- Two settings tune it:
  - `--raw-forward-queue-bytes` (default 256 MiB): the byte bound described under
    Write path.
  - `--raw-forward-concurrency` (default 4): how many deliveries run at once.

**Write path.** For each Remote Write request:

1. The data plane decodes it. A bad request is answered with 400 and is not
   forwarded.
2. It reserves room for the compressed body in a forward queue, bounded in
   bytes (bodies being delivered count too). If the queue is full, it answers
   503 and goes no further.
3. It hands the samples to the engine. If the engine refuses them (its own
   backpressure), it releases the reserved room and answers 503.
4. It puts the body, unchanged, in the forward queue and answers 204.

So a 204 means both stores have the request: the engine has admitted the
samples, and the raw store's copy is queued. A 503 means neither has it, so a
sender's retry never duplicates samples in either store.

All samples go to VictoriaMetrics, including metrics no plan reads, because it
is the store for every query the plan does not cover.

**Delivery.** A few workers on their own thread send queued bodies to
`URL/api/v1/write`:

- They send the snappy-compressed bodies with the Remote Write headers.
- After a 429, a 5xx, a timeout or a connection error they retry the same body,
  with an exponential backoff from 100 ms to 5 s.
- Any other 4xx means VictoriaMetrics will never accept that body. It is dropped
  and counted.
- The order between concurrent deliveries is not kept. VictoriaMetrics accepts
  samples out of order.
- The queue is in memory, and there is no disk spool, by decision. This is a
  known limit: a crash of the data plane loses up to `--raw-forward-queue-bytes`
  of samples it has already acknowledged, on the raw-store side only. ASAP
  still holds those samples; VictoriaMetrics, which answers the unplanned
  queries, does not. A smaller bound limits the loss, at the cost of more 503s
  when the raw store slows down.

**Queries.** Each query is answered by whichever store can answer it:

- A planned query the plan answers gets `x-asap-execution: plan`.
- Every other query (unplanned, or a planned query the plan cannot answer) goes
  to VictoriaMetrics, and so does every other path: `labels`, `series`, label
  values, metadata and the VictoriaMetrics UI.
- These forwarded answers get `x-asap-execution: forwarded`. A planned query that
  falls back also carries `x-asap-forward-reason`.
- This reuses the existing query forwarding: forward mode points it at the raw
  store instead of `--prometheus-url`.

**Resources.** The forward-mode arm runs the data plane and VictoriaMetrics in
one cgroup, under the same limits as the VictoriaMetrics-alone arm. The
cgroup's memory, disk and network are therefore their sum, ASAP +
VictoriaMetrics. CPU and RSS are also reported per process. All of this is
compared against VictoriaMetrics alone.

The data plane also exports these metrics:

- `asap_raw_forward_queued_bytes` and `asap_raw_forward_queued_requests`;
- `asap_raw_forward_requests_total{outcome="delivered|dropped"}`;
- `asap_raw_forward_retries_total`;
- `asap_raw_forward_delivered_bytes_total`;
- `asap_raw_forward_refused_writes_total`.

The same numbers appear under `raw_forward` in `/api/v1/status/asap`.

**Test plan** (`data_plane/tests/forward_mode.rs`). The raw store is a
recording mock that can accept, fail N times or hold requests.

| # | Behaviour | Expected |
| --- | --- | --- |
| F1 | Forward mode off | Unknown paths answer 404. Writes and planned queries behave as before. No `asap_raw_forward` metric exists. |
| F2 | Accepted writes | They reach the raw store byte for byte, snappy-encoded. ASAP's answers equal those with forwarding off. |
| F3 | Planned query | Answered by ASAP, marked `plan`. |
| F3 | Unplanned `query` and `query_range`, `labels`, `series`, label values | Proxied with their query string, marked `forwarded`. |
| F4 | Raw store answers 503 or 429 twice | The body is retried and delivered once, with 2 retries counted. |
| F5 | Raw store down, then up | Queued bodies wait, and are delivered once it is up. |
| F6 | Queue full (raw store holds requests) | The write gets 503 and the engine never sees it. After the store recovers, the retried write is accepted, and each store has every sample exactly once. |
| F7 | Raw store answers 400 | The body is dropped and counted. Later bodies are still delivered. |
| F8 | Forward mode with `--no-forward` | Refused at start. |

All existing data plane tests run with forward mode off and must pass
unchanged.
