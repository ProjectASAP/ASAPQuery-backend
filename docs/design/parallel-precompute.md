# Design: parallel precompute (push pipelines)

## Audience and context

This document is for developers and architects of the backend data plane.
It extends [the data plane demo design](data-plane-demo.md). In that design:

- Remote Write is sharded by series across `--ingest-threads` ingest shards
  (`ingest-N`).
- An `ingest` coordinator moves the watermark, but only behind a flush of
  every shard.
- The precompute side runs `execute_ingestion` once per closed refresh
  window, over every shard's raw data, and stores the result in the
  materialized view store.
- Queries run on worker threads. Shared query-time results use single
  flight, and final answers go to a result cache.
- Views are persisted asynchronously.

That design computes windows in parallel at the granularity of a refresh
group's window: one task per (group, window) on a pool. This document goes
below that granularity. It changes only how ingestion-time views are
computed. What is computed stays the same, and so do the view store, the
queries, the result cache and the file format.

Terms, as databases use them:

- **Pipeline**: a maximal chain of operators that pass each batch on as it
  arrives, from a source (a `Scan`, or an earlier pipeline's output) to
  one sink (a pipeline breaker or a view).
- **Morsel**: one batch pushed through a pipeline. Here it is the samples
  of one Remote Write request that one shard owns, for one metric.
- **Pipeline breaker**: an operator that must see all of its input before
  it outputs anything. It is its pipeline's sink.
- **Instance**: one running copy of a pipeline for one (shard, refresh
  window).
- **Seal**: at window close, a shard ends an instance's input and the
  breaker emits its partial output.
- **Combine**: turns the K partial outputs of a window into the window's
  one output.
- **Commit**: appends a window's rows to the view store, publishes the
  snapshot and queues the rows for persistence.

The decisions are recorded in "Decisions D1–D10" below. Each one lists its
options, precedents, evidence and conclusion. Several decisions are made
by experiments E1–E5. Their result slots are left open until the runs
finish. The evidence comes from two inputs:

- a survey of push and streaming engines: DuckDB, HyPer/Umbra, Velox,
  DataFusion, ClickHouse, Flink, Arroyo, Kafka Streams, Timely and
  Differential Dataflow, RisingWave, Materialize, VictoriaMetrics, vmagent
  and Prometheus;
- a classification of what the Planner puts at ingestion time, over 3,731
  public corpus queries (real-world PromQL, Prometheus engine tests, SQL
  data-quality, TPC-H and BGP), both raw-not-kept (mvp) and raw-kept
  deployments, plus 49 multi-query workloads (8,002 planner runs).

## Problem and why now

- **Close-time work.** All of a window's view computation happens when the
  window closes: one batch run over buffered raw samples.
  - One precompute thread took 0.75–1.3 s per window at 10k series.
  - With the window-task pool, different windows run on different
    threads, but one window of one group is still one core.
  - The dashboard plan has a single refresh group, so the pool helps it
    only when consecutive windows overlap.
- **Memory.** Raw samples are kept until the windows that read them close,
  even when the plan does not keep raw data. A sample is 20 bytes. At 100k
  samples/s that is 60 MB for a 30 s window and 600 MB for the 5-minute
  chain of `sketch-plan-chains-10m-5m-3m-10s`. The summary built from them
  is far smaller.
- **Per-core rate.** The data plane ingests about 132k samples/s per core.
  VictoriaMetrics ingests 385–443k samples/s per core. Parallelism does not
  close that gap by itself. The per-sample path must also become a cheap
  append plus per-batch work. Folding each batch into the open window's
  summaries, instead of keeping raw rows and recomputing at close, is part
  of that.

Ingestion is already sharded by series, and each shard owns a disjoint set
of series. The same shards can run the view pipelines without
repartitioning.

## Inputs, outputs, and end-to-end behavior

- Inputs and installed plans are the same as before.
- Outputs are the same views, rows and view files, and queries get the
  same answers.
  - Concatenated per-series states and integer-count merges are
    identical.
  - Float sums and KLL merges stay within the plan's accuracy target and
    use a fixed merge order (by shard index).
- Changed behavior:
  - Each shard pushes its morsels into the open windows' instances as they
    arrive, inline on its own thread.
  - At window close, each shard seals its instances, a combine merges the
    partials, and a committer commits each group's windows in window
    order.
  - When raw data is not kept, it is no longer buffered for views.
  - `/metrics` breaks precompute down by group, shard and step.
- `--precompute-mode=window` keeps the window-task pool path, for
  comparison and for plans that cannot be pushed (D4, D7).
  `--precompute-mode=push` is the new path and the `data_plane` default
  (D9; "Demo workload A/B").

## Goals and non-goals

Goals:

- Move the view work from window close to arrival time (D5). Throughput
  should scale with the shard count for plans whose ingestion-time
  breakers are per-arrival folds.
- When raw data is not kept, raw memory should no longer grow with window
  width.
- Answers and stored rows should match the window path, on every parity
  workload, at 1, 2, 4 and 8 shards.
- No Planner executor change for the plans in today's workloads.

Non-goals:

- Parallel query execution.
- Changing the shard count at run time.
- A generic push-operator interface in the executor (D1).
- A parallel combine for sort, exact quantile and exact distinct
  (ordered-merge). It is listed as future work in D4.
- Persisting open windows or raw samples.

## Heilmeier questions

- **What are we trying to do?** Compute the planned views as samples
  arrive, on every ingest shard, instead of in one batch run per window at
  close.
- **How is it done today?** One task per closed (group, window) runs over
  buffered raw samples. Work comes in bursts at close, a single group
  cannot use more than one core, and raw memory grows with window width.
- **What is new?** The DuckDB-style contract, driven by the watermark
  instead of end-of-input:
  - Sink: push a morsel into the thread-local state of the shard that
    owns its series.
  - Combine: seal each shard's state at window close and merge the
    partials.
  - Finalize: commit.

  Today's executor runs this unchanged, fed by a channel.
- **Who cares?** Anyone ingesting at high series counts. The window path
  is the first limit reached.
- **Risks and costs?** Shard threads now do the view work. A hot series is
  bounded by one core. Grouped and global partials multiply open-window
  state by the shard count.
- **How long?** About 5 days after the test plan is approved.
- **Checks?** Midterm: parity at 1, 2, 4 and 8 shards. Final: B1 and B2
  against the window path at 1k, 10k and 100k series.

## What the Planner puts at ingestion time (corpus evidence)

These are the breaker classes in plans with ingestion work, in the mvp
deployment. Plans are counted when they contain the class.

| Breaker class | Mergeable | Real-world PromQL (355 plans) | Engine tests (250) | SQL (514) |
| --- | --- | --- | --- | --- |
| per-series exact chain: Rate / Increase | yes | 254 / 85 | 79 / 15 | 0 |
| per-series exact chain: Max, Min, Sum, Count | yes | 15 | 86 | 0 |
| per-series sketch chain (KLL) | yes | 0 | 7 | 0 |
| raw window buffer, PromQL subquery at ingestion | no | 2 | 88 | 0 |
| grouped or global exact chain (Count, Sum, Min, Max) | yes | 0 | 0 | 51 |
| grouped sketch chain (KLL) | yes | 0 | 0 | 2 |
| global running exact Aggregate (count, sum, avg, …) | yes | 0 | 0 | 397 |
| global exact quantile / exact distinct | no | 0 | 0 | 39 / 17 |
| join or SQL window function at ingestion | no | 0 | 4 | 25 |

Roll-ups:

- Every real-world PromQL ingestion plan has only per-series breakers, so
  series-hash partitioning needs no combine beyond concatenation.
- Every SQL plan needs a cross-partition combine. In 15.6% of SQL plans
  (all whole-table data-quality checks) the breaker is non-mergeable.
- No plan has a view that reads another view.
- 36% of real-world plans have two or more views. Dashboards put 5–6
  views (seed-862 workloads) up to 13 views (an o11y dashboard) on one
  source, at one to four window widths.
- In 9% of real-world plans at 60 s refresh, and in every dashboard
  workload, a query merges more than 20 panes per evaluation.

## Proposed design

```
Remote Write ─► HTTP threads: decode, intern, send each series' samples
                to its ingest shard (whole request between two flushes)
        ┌────────────┬────────────┬────────────┐
        ▼            ▼            ▼            ▼
     ingest-0     ingest-1       ...       ingest-(K-1)
     append raw (only what query-time Scans read)
     push the morsel, inline, into every open instance of every refresh
     group that scans its metric: Scan → Filter/Project/TimeRange →
     {breaker, breaker, …} (one shared batch, no queue between views)
     on Seal(g, e): end the instance's input; the breakers emit; send the
     partial (Send batches) onward; chunked so ingest keeps running
        └────────────┴─────┬──────┴────────────┘
                           │       ▲ Flush / Seal(g, e) to every shard
                           │       │ (watermark moves only behind a flush)
                           │   ingest coordinator
                           ▼
       combine (window-task pool threads): when all K partials of (g, e)
       are in: concatenate / merge / single partition; then the
       pipelines above the breakers
                           ▼
       committer (`precompute`): per group in window order; publish
       snapshot; queue persistence; schedule groups that read this one
                           ▼
                 materialized view store  ◄── query workers (unchanged)
```

### Pipelines from the refresh group's DAG

Each refresh group's ingestion DAG is compiled once at install, as today.
It is split at its breakers:

| Operator (physical) | Class |
| --- | --- |
| `Scan` (raw data slot) | source; each shard feeds its own copy |
| `Filter`, `Project`, `TimeRange` (ingestion window), `TimeShift`, label and scalar row functions | non-blocking |
| `SummaryTumblingWindowChain`, `SummaryAgg` (per group, keyed, all-groups) | breaker; folds each morsel into per-group state and emits at close |
| exact `Aggregate` | breaker; decomposable functions fold, holistic ones (quantile, distinct) hold rows |
| `Sort`, `Sort` + `Limit`, join build side, SQL window function, ingestion-time `TimeRange` kept as a raw window buffer, PromQL subquery | breaker; holds rows (not a per-arrival fold) |
| `SummaryMerge`, `SummaryEstimate`, `FinalizeExactAccumulator` above a breaker | non-blocking, in the pipeline after combine |
| a view slot of another group | source of a post-commit pipeline (D7) |

All of a group's pipelines over one source share the scan node inside
the instance. The executor's ingestion runtime hands each batch to every
consumer, so one morsel reaches all views of its source on the shard
thread, without a copy and without a queue (D2).

### Example 1: the dashboard plan

`dashboard.json` (example 4 of the demo design): 10k series per metric,
1 sample/s, every query every 15 s.

```
n0   Ingestion None               Scan lat
n1   Ingestion AcrossEvaluations  SummaryTumblingWindowChain KLL W=15s       <- n0
n6   Ingestion AcrossEvaluations  SummaryTumblingWindowChain Max W=15s       <- n0
n10  Ingestion None               Scan reqs
n11  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Rate W=15s      <- n10
n17  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Increase W=15s  <- n10
```

(Newer Planner revisions may choose DDSketch, or compute the quantile at
query time. The shape stays the same.)

- **Pipelines.** One refresh group, two pipelines: `Scan lat → {KLL n1,
  Max n6}` and `Scan reqs → {Rate n11, Increase n17}`. All four breakers
  are `PerEntity`.
- **Push with K = 4.** Each shard owns about 2.5k series per metric. Each
  request's `lat` morsel updates that shard's KLL and Max states, and its
  `reqs` morsel updates the Rate and Increase states. Nothing else is
  kept, because the plan keeps no raw data.
- **Close at e.** Each shard seals its instances and emits its 2.5k series
  per view as `(window_end, series keys, state)`. The combine concatenates
  the partials, and the committer appends the four views for
  (e − 15 s, e].
- **Query.** Unchanged. For example, `topk(10, sum by (job)
  (rate(reqs[1h])))` merges 240 windows of n11 per series, then
  aggregates by `job` at query time.

### Example 2: a `by` grouping applied at query time

`avg by (region) (quantile_over_time(0.9, data[5m]))`
(`sketch-plan-quantile-grouped`):

```
n0  Ingestion None               Scan
n1  Ingestion AcrossEvaluations  SummaryTumblingWindowChain DDSketch W=10s α=0.05  <- n0
n2  Query     None               TimeRange 300s                                     <- n1
n3  Query     OneEvaluation      SummaryMerge                                       <- n2
n4  Query     None               SummaryEstimate q=0.9                              <- n3
n5  Query     None               Project                                            <- n4
n6  Query     OneEvaluation      Aggregate avg() by (region)                        <- n5   (root)
```

The ingestion-time breaker is per series, so the combine concatenates. As
the corpus shows, this is the shape of every real-world PromQL ingestion
plan.

### Example 3: an ingestion-time view grouped by coarser labels

The IR allows a chain grouped by labels (`Reduction::Reduce`). The SQL
corpus has 51 plans of this shape, for example a rolling
`SELECT operation, count(*) … GROUP BY operation` over 1-day windows. In
PromQL terms:

```
n0  Ingestion None               Scan m
n1  Ingestion AcrossEvaluations  SummaryTumblingWindowChain ExactAggregate(Sum) W=60s by (job)  <- n0
```

- Each shard holds a partial sum for every `job` it has seen.
- At close, `Union(K) → SummaryMerge`, grouped by `(window_end, job)`,
  gives one state per job.
- The current Planner selects no PromQL plan of this shape. The tests use
  a hand-written plan.

### Combine rules

The rule is chosen once at install, per breaker, from the plan node
alone. A breaker that matches no row makes install fail; no rule is
guessed.

| Class | Breaker and condition | Combine | Equality with the window path |
| --- | --- | --- | --- |
| **Concatenate** | Summary build with `Reduction::PerEntity` (one state per series; each series lives on one shard) | Concatenate the K outputs in shard order | identical |
| **Merge** | Summary build with `Reduce(keys)` whose `SummaryTypeCapability` merges groups (`Disjoint`/`MayOverlap`); a Hydra grid; exact `Aggregate` sum, min, max, count, topk, bottomk (re-aggregate) | `Union(K) → SummaryMerge` grouped by the output's group columns (and `window_end`), or the same aggregate again over the partials | integer counts, min and max: identical. Float sums: within the plan target. KLL and heap sketches: within the plan target. Merge order fixed by shard index |
| **Ordered-merge** (not in v1) | `Sort`, exact quantile (sorted runs), exact distinct (set union, radix-partitioned by value) | A parallel merge (Merge Path, radix partitions) | identical |
| **Single partition** (v1 for the row above, plus joins, window functions, counters `Reduce`d across series, exact avg until it is lowered to sum + count) | Shard 0 owns the breaker. The other shards run the non-blocking prefix and forward its output batches to shard 0 | none needed | identical |

- The rule is derived from the plan: from `Reduction` and from the
  Planner's `SummaryTypeCapability`, which is the authoritative merge
  rule. The plan format does not change.
- The rule is recorded at install. It appears in the log line
  `group, node, operator, rule` and in `/api/v1/status/asap` under
  `ingestion.pipelines`.
- **Empty windows.** Every shard opens every window and seals it even
  when it saw no sample. Its empty partial costs nothing (no allocation).
  A global build with no input emits its one empty row
  (`emit_empty_global`), and K of those merge into one. That is what the
  window path stores for an empty window.

### Scheduling

- **Push.** Each shard thread owns its instances, which are not `Send`
  because the executor's runs use `Rc`.
  - An instance is the group's `CompiledPhysicalDAG`, instantiated over a
    per-instance store whose `read_raw_data` returns a channel's receiving
    end.
  - Its `execute_ingestion` future lives in a `LocalPool` on the shard
    thread.
  - After appending a request (if raw is kept), the shard sends the
    morsel to each instance of a group that scans its metric. Then it
    runs the pool until it stalls, so the morsel reaches every breaker
    before the shard takes the next request. The channel never holds more
    than one morsel.
- **Opening windows.**
  - An instance is created when its window opens on the shard, for every
    group on every shard.
  - With allowed lateness L, a group has at most ⌈L / W⌉ + 1 windows open
    per shard.
  - A window that every view of the group already stores (loaded from the
    view files) is not opened.
- **Seal.**
  - The coordinator closes windows as it does today: only behind a
    flush, after every shard has run the requests admitted before the
    flush.
  - In push mode, the flush carries `Seal(g, e)` for every window the
    pass closes.
  - A shard seals after the samples it ran for the flush and before any
    later request. So each instance has seen exactly the samples the
    window path reads from the shards' raw data.
  - Idle shards answer the flush at once and seal empty, so they never
    hold the watermark or a window back.
- **Chunked seal (D5).** Emitting a shard's states for a large group is
  split into chunks of a bounded time (target about 2 ms; E5 sets it).
  Ingest requests run between the chunks, so close work does not stall
  ingestion.
- **Combine.**
  - When all K partials of (g, e) have arrived, one task is queued on the
    window-task pool.
  - The task combines each breaker, then runs the pipelines above the
    breakers. Those pipelines are compiled with each breaker bound as a
    view slot over the combined rows, the same trick the query engine
    uses for shared `OneEvaluation` nodes.
- **Commit.**
  - The `precompute` thread commits each group's windows in window order
    through a reorder buffer.
  - The buffer is bounded by `--max-precompute-backlog`, so it doubles as
    backpressure.
  - The committer publishes the snapshot, queues persistence, and
    schedules post-commit groups.
- **Barrier.** The event-time barrier answers once every window it closed
  is committed and queued for persistence, as today.

### Watermark and late data

The rules from the demo design stay the same:

- A window (s, e] closes once event time is past e.
- The watermark moves only behind a flush and is capped at the flush
  time.

In push mode:

- A sample for a window that is still open on its shard (within the
  allowed lateness) is pushed into that window's instance.
- A sample for a window already sealed does not change the stored window,
  as today. It is still appended to the raw data if a query-time `Scan`
  reads its metric. `asap_samples_total{outcome="late_for_views"}` counts
  it.
- Counter states see each series' samples in arrival order. A series is
  on one shard, and each shard's queue is FIFO. This is the same order as
  the chunks the window path reads.

### Backpressure

- Shard queues are bounded (`--max-queued-samples`), and a request is
  admitted whole or refused with 503, as today. A slow pipeline slows only
  its own shard's queue.
- Windows that are closed but not yet committed are bounded by
  `--max-precompute-backlog`. When the backlog is full, the coordinator
  stops issuing flushes, so the shard queues fill up and Remote Write gets
  503.
- Memory reservations (D8) wait instead of failing while older windows
  are still open on the shard.

### Persistence, metrics, failure handling

- **Persistence.**
  - The committer persists the combined rows. Per-shard partials are never
    persisted.
  - The file format and the per-group writer are unchanged.
  - The shard count is not in the files, so a restart with any shard
    count or mode loads them.
  - Series-to-shard routing (series id modulo K) is not persisted either,
    because no per-shard state outlives a window.
- **Metrics.** Each of these goes into `asap_phase_cpu_seconds_total`,
  `_wall_seconds_total` and `_runs_total`:
  - `phase="precompute_push"` with `group` and `shard`, measured once per
    morsel and group;
  - `precompute_seal` with `group` and `shard`;
  - `precompute_combine` with `group`;
  - `precompute_commit`.

  New gauges and counters: `asap_precompute_state_bytes{group,shard}`,
  `asap_precompute_open_instances{shard}`,
  `asap_window_commit_lag_seconds` (from window end to commit: the newest
  window's lag), and `asap_samples_total{outcome="late_for_views"}`.
- **Failures.**
  - When an instance fails (an operator error, or a memory reservation
    that cannot be met, see D8), its shard sends `Err`. The window is
    failed and not committed: `windows_failed` goes up and queries that
    read the window are not answered, as today. Later windows still
    commit.
  - A panic in a combine task is caught and fails the window. A panic in
    a shard thread stops the engine, as today.

### Executor reuse and ASAPPlanner changes

Reused unchanged at the pinned revision:

- `compile` and `instantiate`, once per (shard, window);
- `execute_ingestion` over a channel-fed `read_raw_data`. Summary builds
  already fold their input stream batch by batch, which makes this push
  execution without a new operator interface. Arroyo reuses DataFusion
  operators inside its own streaming runtime the same way, and so does
  DataFusion's "Inbox" push leaf.
- `Operator::source`, `union` and `summary_merge` for the merge combine.

These would need an ASAPPlanner PR, and only when a plan needs them:

1. Lowering ingestion-time exact `avg` and other decomposable aggregates
   to `SummaryAgg ExactAggregate` (sum and count) + `SummaryMerge` +
   `FinalizeExactAccumulator`, so that they merge instead of running on a
   single partition. This is a physical-compiler change only.
2. Ordered-merge combines for sort, exact quantile and exact distinct
   (D4).
3. A "latest sample per series" chain state, which the Planner does not
   offer yet. 619 of 1,025 real-world PromQL queries (60%) need raw data
   only for it. It is a per-arrival overwrite (concatenate class), and it
   will be the most frequent view once offered, so push mode must make it
   cheap. It is listed here because it changes the mix, not because v1
   implements it.
4. Optionally, a seeded KLL compaction coin (`asap_sketchlib`), if
   bit-identical KLL parity is wanted.

## Decisions D1–D10

Each decision lists its options, precedents, the corpus and local
evidence, its experiment if one decides it, and the conclusion. An
experiment result slot reads *pending* until the run is reported. A
conclusion marked *provisional* holds until its experiment confirms it or
overturns it.

### D1. Execution model

- **Options:**
  - (a) Keep the window-task pool: batch at close, one task per
    (group, window).
  - (b) Per-source push pipelines on owned shards, with breakers as sinks
    sealed by the watermark.
  - (c) Morsel-driven execution on a shared pool (DuckDB, HyPer): any
    thread takes any morsel, and every breaker keeps thread-local plus
    global state.
  - (d) DataFusion's runtime (partitioned pull streams, `RepartitionExec`).
- **Precedents:**
  - (b) is DuckDB's Sink/Combine/Finalize (push since 2021), with Combine
    triggered per window by the watermark instead of at end of input.
  - Kafka Streams tasks, Timely workers and RisingWave actors run the
    same shape on owned partitions.
  - Shaikhha et al. found no general advantage of push over pull; what
    matters is where the thread boundaries are.
  - DuckDB criticizes (d)-style plan-time exchange for plan explosion and
    load imbalance.
- **Evidence:**
  - Per-arrival folds into mergeable window states cover 100% of
    real-world PromQL ingestion plans and 84% of SQL plans.
  - No plan has a view that reads a view, so no general dataflow graph is
    needed.
  - With (c), the executor's runs (not `Send`) would have to move between
    threads, and order-dependent per-series counters (Rate, Increase)
    could not be updated from any thread.
  - With (a), one window of the single-group dashboard plan is still one
    core.
- **Experiment:** E1 (fold throughput per core and scaling, push against
  the window path and against DuckDB, ClickHouse and DataFusion arms).
  Result (100k series, W = 60 s, 10 samples/s, 1 to 16 cores):
  - **Single-thread fold rate.** It is on par across engines: the data
    plane 11–15M samples/s per core for sum, max and rate, and about 4M
    for a quantile sketch. DuckDB, ClickHouse and DataFusion fold sum at
    10–17M samples/s per core and a quantile at 3–6M.
  - **Scaling.** Series-hash push into per-thread view states with no
    combine scaled best: 13–17× on 16 cores (ClickHouse insert-time views
    fed by series-sharded senders). Whole-window GROUP BY with partial
    states and a combine scaled worse, and worst for sketches:
    ClickHouse 1.9–3.8× for a quantile, DuckDB 10–11×, DataFusion about 6×.
  - **The window path on one thread.** It needs 27–29 s of each 60 s
    window for four views.
- **Conclusion:** (b). The window-task pool stays, both as the slow path
  for groups that cannot be pushed (D4, D7) and as the pool that runs
  combine tasks.

### D2. Fan-out: one morsel to many views

- **Options:**
  - (a) Same-thread fan-out: the shard thread runs every view of the
    source on the same batch.
  - (b) Cross-thread fan-out: views of one source on different threads,
    connected by queues.
  - (c) Materialize the morsel once, and let the views scan it.
- **Precedents:**
  - DuckDB's 2026 pipeline broadcast exchange pushes chunks directly into
    each consumer pipeline, inline on the producer thread. DuckDB also
    found buffered multi-consumer fan-out costlier than materializing.
  - HyPer measured that independent pipelines on different threads hurt
    cache locality.
  - Snowflake's push execution handles DAG-shaped plans by having the
    producer call every consumer.
  - ClickHouse runs insert-time materialized views sequentially by
    default. Its `parallel_view_processing` cuts one insert's latency at
    the cost of CPU and memory.
  - vmagent pushes N aggregators serially per batch.
- **Evidence:**
  - 36% of real-world plans have two or more views.
  - The seed-862 workloads put 5–6 views on one source at up to 4
    widths, and the o11y dashboard puts 13 views on one source.
  - The executor's ingestion runtime already gives each batch to every
    consumer with no copy.
- **Experiment:** E2 (one insert feeding 1 to 13 views; same-thread
  against cross-thread). Result (K = 1, 4, 16 views over one 15 s batch,
  10k and 100k series):
  - **CPU per view.** CPU grows close to linearly with views in every
    engine. Reading the batch once instead of K times saves nothing
    measurable. DuckDB's broadcast CTE uses 15–35% more CPU and 7–9× more
    memory than K separate scans at 16 threads.
  - **Cross-thread fan-out.** ClickHouse's parallel view processing cuts
    one stream's wall time by 2.5–4.3×, at 27–60% more CPU, and the
    slowest view bounds it. With 16 series-hash senders, it makes wall
    time and CPU worse.
- **Conclusion:** (a). The earlier draft said pipelines of different views
  would run on different threads. That is withdrawn: the pipeline DAG
  orders dependent work (seal, combine, dependent group, commit) and does
  not spread views over threads. Cross-thread fan-out would be considered
  only if E2 shows one view's cost dominating a shard. An example is a
  1 s DDSketch next to a 60 s Sum on the same source.

### D3. Partitioning

- **Options:**
  - (a) Series hash, using the ingest shards that exist today: series id
    modulo `--ingest-threads`.
  - (b) A fixed virtual space (`hash(series) % V`, V = 256) with vnode
    ranges mapped to shards.
  - (c) Round-robin plus per-partition partials and a merge.
  - (d) Hash by group key.
- **Precedents:**
  - Prometheus keeps mutable per-series heads under 16,384 lock stripes.
  - Kafka Streams, Timely and Seastar give each thread exclusive
    ownership.
  - VictoriaMetrics appends round-robin because nothing it computes at
    ingest depends on series order.
  - Flink key groups and RisingWave vnodes keep the key-to-partition map
    fixed under rescaling.
  - RisingWave puts the vnode in the local group key and drops it in the
    global phase.
  - DataFusion names the per-key case `SinglePartitioned`: no merge
    needed.
- **Evidence:**
  - Every real-world PromQL ingestion breaker is per series, and
    `sum by`, `topk` and `histogram_quantile` run at query time.
  - SQL breakers are grouped or global and have no series identity. Key
    counts range from 2 (BGP `operation`) to 10k (netflow `srcip`).
- **Experiment:** E4 (series hash with no combine; round-robin plus merge
  against key-hash for 2 and 10k keys; single owner). Result: *pending*.
- **Conclusion:**
  - Time series: (a). This adds no partitioning of its own.
  - (b) is not needed. No per-shard state is persisted or outlives a
    window, so changing the shard count needs no state movement. Revisit
    if state is ever kept per shard across restarts.
  - Table sources (SQL), when the data plane takes them: (c) with a merge
    at close. Use (d) only when keys are many and E4 shows the merge
    dominating. Never use (d) on a 2-key group-by (skew).
  - Known limit: a hot series is bounded by one core, because
    order-dependent per-series state cannot be split. No surveyed engine
    splits it either. Prometheus series rarely exceed a few samples per
    second.

### D4. Non-mergeable breakers

- **Options:**
  - (a) Single partition: shard 0 owns the breaker and the others forward.
  - (b) Exchange by group-key hash.
  - (c) Ordered-merge combines: per-shard sorted runs merged with Merge
    Path, and exact distinct as a radix-partitioned union.
  - (d) Have the Planner replace them with sketches where accuracy
    allows.
- **Precedents:**
  - Velox runs LocalMerge single-threaded, and Flink runs non-keyed
    windows at parallelism 1. Both match (a).
  - DuckDB and HyPer use (c) for sort, and DuckDB uses radix partitions
    for distinct.
  - DuckDB excludes holistic aggregates only from its partitioned
    pre-aggregation path, not from parallel execution.
- **Evidence:**
  - Non-mergeable breakers appear in 0.6% of real-world PromQL plans and
    15.6% of SQL plans. All of the SQL ones are whole-table data-quality
    checks: 39 exact quantile, 17 exact distinct, 13 joins and 12 window
    functions.
  - Rolling-window SQL has none, and windowed non-mergeable queries do
    not plan without raw data.
- **Experiment:** E4 (single owner), E5 (per-arrival set insert against
  close-time recompute for exact distinct). Result: *pending*.
- **Conclusion:** (a) in v1, as a list of breakers awaiting (c) rather
  than a permanent class. A group that contains any breaker which is not
  a per-arrival fold (raw window buffer, subquery, join, window function)
  runs on the window-task pool, as today, until E5 says otherwise. (d) is
  a Planner preference, not engine work.

### D5. Per-arrival folding versus close-time computation

- **Options:**
  - (a) Fold each morsel into the open window's state on arrival; at
    close, only seal and emit (O(series) per window).
  - (b) Buffer raw rows and compute at close (today).
  - (c) Per class: fold where the state is a fold, and compute at close
    for holistic states.
- **Precedents:**
  - RisingWave's emit-on-window-close and Kafka Streams'
    `emitStrategy(ON_WINDOW_CLOSE)` emit the final result once, at close,
    from incrementally maintained state. RisingWave recommends it for
    percentile-like aggregates that update poorly.
  - Umbra schedules about 2 ms tasks, because fixed-size morsels varied
    more than 30× in duration.
  - Flink notes that barriers wait behind timers firing at once.
- **Evidence:**
  - Folds cover 87% of all ingestion plans.
  - Close-time emission is series ÷ width states per second: 10k/s at
    1 s widths (seed-862) and 17/s at 60 s with 1k series.
  - The window path took 0.75–1.3 s per window at 10k series.
- **Experiment:** E5 (W = 1 s and 60 s for the Rate chain, 1 s DDSketch
  windows, exact distinct; close latency and ingest stalls). Result:
  *pending*. E5 also sets the seal chunk size.
- **Conclusion:** (c). Fold every per-arrival breaker. Cut seal work into
  time-bounded chunks interleaved with ingest. Keep close-time
  computation for holistic states (D4).

### D6. Idle shards and watermarks

- **Options:**
  - (a) Global watermark = min over shards, with idleness timeouts
    (Flink's `withIdleness`).
  - (b) The router broadcasts watermark ticks to every shard.
  - (c) The coordinator moves the watermark only behind a flush that every
    shard answers. An idle shard answers at once (the demo design's
    rule). Every shard opens and seals every window.
- **Precedents:**
  - Flink inherits the stalled-min failure and adds idleness handling and
    watermark alignment.
  - Arroyo returns watermarks to broadcast.
  - RisingWave aligns barriers at fan-in.
  - Timely advances time per batch, never per record.
- **Evidence:**
  - 58% of ingestion plans are windowed, with widths down to 1 s.
  - 17% of real-world plans read two or more sources.
  - Alert matchers are selective, so a few series may land on one shard.
  - The sharded ingestion found and fixed two races: a min over queued
    shards, and reports from a shard running ahead. Both are now covered
    by tests.
- **Experiment:** none needed. The rule is already in the data plane and
  tested (`ingest_shards.rs`).
- **Conclusion:** (c). It is (b) in its in-process form. The commit
  barrier is a fixed count of K partials per (group, window). Seal travels
  with the flush, so no separate tick is needed.

### D7. Shared intermediate state and views over views

- **Options:**
  - (a) A group that reads another group's views runs as a post-commit
    task on the window-task pool, scheduled once the windows it reads are
    committed.
  - (b) Push sealed partials of the fine view directly into the coarse
    view's per-shard instance.
- **Precedents:**
  - ClickHouse chains materialized views (insert into `q_15s` fires
    `q_5m`).
  - DuckDB's event DAG and HyPer's QEPobject order dependent work.
  - Differential Dataflow's shared arrangements cut latency 1.3–3× and
    memory 2–4×.
- **Evidence:**
  - No current plan reads a view from a view.
  - In 9% of real-world plans at 60 s, and in every dashboard workload at
    15 s, a query merges more than 20 panes per evaluation. That is the
    hierarchical-rollup case of the demo design.
  - Shared chains are common: 4–5 of 7–8 views in seed-862 and 15 of 23
    in the o11y dashboard are read by two or more queries.
- **Experiment:** E3 (shared chains and chained rollups 15 s → 5 min →
  1 h). Result: *pending*.
- **Conclusion:** (a) now. It is the next event in the per-window DAG,
  not a second scheduler. (b) would be considered only if E3 shows the
  post-commit task on the critical path, since a coarse window over
  combined fine windows is small.

### D8. Memory

- **Options for the per-instance limit:**
  - (a) Each instance has its own `--run-memory-limit-bytes`.
  - (b) That limit divided by K.
  - (c) Reserve before applying a morsel, in coarse quanta, per (group,
    shard, open window), against one engine-wide precompute budget. When a
    reservation fails while older windows are open on the shard, wait for
    them to seal (backpressure). Fail the window only when nothing else
    is open.
- **Precedents:**
  - Photon reserves before it allocates, so a half-applied batch is
    impossible.
  - Velox checks only at the root, in 1, 4 or 8 MB quanta, and an
    arbitrator pauses or aborts tasks.
  - vmagent's event-time mode doubles its state memory and caps lateness
    at 90% of an interval.
- **Evidence:**
  - Retained state is about series × (window ÷ width + 1) × state size:
    about 10 MB per real-world plan at 1k series, and 3.5–5.2 GB for
    the seed-862 dashboards with 1 s sketch windows.
  - Holistic whole-table states reach 6.4 GB.
  - Per-shard open state for `PerEntity` breakers adds up to the same
    total as one thread. For `Reduce` breakers it is up to
    min(K, series per group) times larger.
- **Experiment:** E2 records state bytes per view as fan-out grows.
  Result:
  - Views run one after another add state linearly (ClickHouse 128 MB
    at K = 16 on one stream).
  - Views run concurrently multiply it (DuckDB's broadcast CTE 12.5 GB
    against 1.4 GB for separate scans; ClickHouse parallel views 8.4 GB).
- **Conclusion:** (c), with no spill. Shrinking allowed lateness to close
  windows sooner is an opt-in last resort, not automatic.
  `asap_precompute_state_bytes` reports what is held.

### D9. Thread defaults

- **Options:**
  - (a) Keep `--ingest-threads` at a quarter of the cores and add a
    separate combine pool.
  - (b) Shards = available cores minus the decode threads, with combine on
    the existing window-task pool (2–4 threads).
  - (c) Size shards per source by measured fold rate (source rows/s
    divided by per-thread fold rate with all of its views), capped at the
    core count.
- **Precedents:**
  - HyPer pins fixed threads.
  - VictoriaMetrics sizes pools to the available CPUs and admits inserts
    at 2× CPU.
- **Evidence:**
  - Corpus sources run 1k–10k series at 1–10 samples/s (1k–100k rows/s),
    with 1–13 views each.
  - Shards now do the view work, so a quarter of the cores leaves
    precompute capacity unused.
- **Experiment:** E1 (fold rate per thread for each view class; scaling
  at 1, 2, 4 and 8 shards). Result:
  - **Fold rate per thread.** About 4.5M samples/s with sum, max, rate
    and a sketch on one source, and 11–15M with cheap folds only.
  - **Scaling.** Per-thread rates held to 16 cores in the series-hash arm.
  - **Short windows.** A source with 1 s windows needs more than one
    thread at 100k series: 5 views take 1.34 s per 1 s window.
- **Conclusion:** *provisional* (b). Combine runs on the window-task pool,
  so no new pool and no `--precompute-threads` flag. (c) only if E1 shows
  per-source rates differ widely within one deployment. Push becomes the
  default mode when:
  - parity passes on every workload, and
  - E1 and B1 show higher sustained samples/s and lower newest-window
    commit lag than the window path at 1k and 10k series.

  The window mode stays behind `--precompute-mode=window`.

### D10. Planner memory term

- **Options:**
  - (a) Add a shard count to the Planner's cost inputs.
  - (b) A deployment calibration input: an open-state multiplier per
    breaker class, plus whether the deployment buffers raw rows for
    views. The Planner never sees shards.
  - (c) No change.
- **Precedents:**
  - DuckDB sizes partitions by memory.
  - vmagent documents its doubled state.
- **Evidence:**
  - The Planner already charges retained bytes per view.
  - It does not see that `Reduce` partials are multiplied by the shard
    count while open.
  - It does not see that push removes the raw buffer, which today
    dominates for long windows.
- **Experiment:** E3 and E4 measure the open-state multipliers. Result:
  *pending*.
- **Conclusion:** (b). The term is: `PerEntity` × 1, `Reduce` and Hydra
  partials × shards × open windows, and single-partition breakers × 1
  plus their buffered rows. This keeps the Planner deployment-agnostic
  and is separate Planner work, after E3 and E4.

### B1/B2 measurements (typical 60 s / 10 Hz dashboard, 12 views)

**Setup.** Push compared with the window path on the precompute task pool, with K
ingest shards and K precompute threads, one Remote Write request per scrape, paced at
10 scrapes/s. Window ready is the time from when the scrape at a window's end was sent
to that window's commit, including the 15 s allowed lateness. Push coalesces each
shard's samples into morsels of up to 32,768 samples or 1 s before folding (the
default).

| series, K | window ready, window path / push (mean s) | M samples/s, window path / push | fold CPU, window path / push (s) | total CPU, window path / push (s) |
| --- | --- | --- | --- | --- |
| 10k, 4 | 18.5 / 12.3 | 0.20 / 0.20 (offered) | 121 / 135 | 234 / 235 |
| 10k, 12 | 14.7 / 12.3 | 0.20 / 0.20 (offered) | 126 / 140 | 254 / 259 |
| 100k, 4 (2M/s offered) | 224 / 193 | 0.78 / 0.86 | 1,436 / 1,523 | 1,870 / 2,028 |
| 100k, 12 (2M/s offered) | 88.6 / 44.7 | 1.33 / 1.59 | 1,422 / 1,312 | 1,926 / 1,693 |

**Findings.**

- **Latency.** Push commits each window at the lateness bound at 10k series,
  whatever K is. At 100k series and K = 12 it halves the window path's latency.
- **CPU, total.** With morsel batching, push's total CPU matches the window path's
  at 10k series and is 12% lower at 100k series with K = 12.
- **CPU, fold only.** Push's folding costs 6–12% more than the window path's,
  except at 100k series with K = 12, where it costs 8% less.
- **Without batching.** When every 10 Hz scrape was folded at once, push used about
  1.75× the window path's fold CPU at K ≥ 4. With morsels of 8,192 samples it still
  used 1.6×.

### Core scaling (engine and senders on separate hosts)

**Setup.**

- **Hosts.** The engine host has 2 sockets × 14 cores × 2 hardware threads: 28
  physical cores, 56 logical CPUs. CPUs 0–27 are the physical cores, and 28–55
  their SMT siblings. The engine (the `data_plane` server) is pinned to CPUs
  `0..K`, so K ≤ 28 uses physical cores only and K = 32, 48 and 56 add SMT
  siblings.
- **Engine settings.** K ingest shards, and K precompute threads for the window
  path.
- **Senders.** The Remote Write senders (`examples/remote_write_load.rs`) run on a
  second host and send over a 10 GbE link. iperf3 measured 9.42 Gbit/s (4 streams)
  with a 0.06 ms RTT. The highest load sent, 8.7M samples/s, is below 3 Gbit/s
  snappy-compressed, so the network is not the limit.
- **Workload.** The typical 60 s / 10 Hz dashboard (12 views), with 100k series
  per metric (200k series).
- **Saturation runs.** Samples are stamped in the past and sent as fast as the
  engine takes them, for 180 s, so windows close as event time passes.
- **Senders' behaviour.** 48 sender threads, about 4.2k samples per request, each
  with one request in flight. A 503 is retried with exponential backoff (10 ms to
  1 s), as Prometheus does.
- **Throughput.** Samples the engine accepted over a 145 s interval. Fold CPU is
  the `precompute*` phases.

| K (CPUs) | push M samples/s | push efficiency vs K = 12 | push fold / engine CPU (cores) | window path M samples/s | window engine CPU (cores) |
| --- | --- | --- | --- | --- | --- |
| 4 | 0.89 | 0.82 | 2.3 / 3.9 | 0.46 | 3.3 |
| 12 | 3.25 | 1.00 | 6.6 / 11.8 | 0.21 | 3.8 |
| 16 | 4.41 | 1.02 | 8.8 / 15.7 | 0.17 | 3.7 |
| 28 (all physical) | 6.59 | 0.87 | 16.6 / 27.4 | 0.13 | 4.0 |
| 32 (+4 SMT) | 6.56 | 0.76 | 19.1 / 31.0 | 0.12 | 4.4 |
| 48 (+20 SMT) | 8.00 | 0.62 | 30.9 / 46.8 | 0.10 | 6.1 |
| 56 (+28 SMT) | 8.59 | 0.57 | 30.6 / 48.6 | 0.09 | 6.7 |

**Window-ready latency.** Measured at 1M samples/s offered (50k series per metric,
stamped at the wall clock):

- Push commits windows 16.8 s after their end at K = 12, 28 and 56 (max 17.0 s):
  the 15 s lateness plus under 2 s.
- The window path does not sustain 1M samples/s from 48 senders at any K
  (0.07–0.12M accepted), so no window closed.

**Request size.** With 12 sender threads (about 17k samples per request) at
K = 28:

- push takes 8.69M samples/s with no 503, using 22.6 of 28 cores, so the senders
  are the limit;
- the window path takes 0.51M samples/s.

With 96 sender threads (about 2.1k samples per request), push takes 7.4M at K = 48
and 7.1M at K = 56. That is less than with 48 senders, at 32–35 of the engine's
cores.

**Where it stops scaling, and why.**

- **Push, physical cores.** Push scales close to linearly to the 28 physical cores:
  0.87–1.02 of the K = 12 rate per core.
- **Push, SMT siblings.** The 28 SMT siblings add 30% (6.6M → 8.6M samples/s).
  Per-CPU efficiency drops to 0.57, as expected from hyperthreads sharing a core.
- **Push, the knee is the coordinator.** Beyond the physical cores the limit is
  the ingest coordinator, a single thread. It applies every shard's per-request
  report and runs the flush passes. Shard queues are counted out only when the
  coordinator applies a report. So when it falls behind, Remote Write gets 503s
  while the engine still has idle CPU: at K = 56 the engine used 35–49 of 56
  cores.
  - Its CPU grows with the request rate, not the sample rate. At K = 28 it used
    56 s with 48 senders, against 13 s with 12 senders at a higher sample rate.
  - Halving the request size at K = 56 raised it from 130 s to 187 s and lowered
    throughput.
- **Push, where the CPU goes.** The perf profile at K = 28 and K = 56 puts most
  engine CPU in Remote Write decode on the HTTP threads: series interning
  (per-thread `cached_series` hash lookups and memcmp, about 10–17% of samples),
  protobuf field and varint parsing, and snappy. The fold runs on the shards
  (30–58% of engine CPU).
- **Push, ruled out.** The committer (1–2 s of CPU per window) and persistence
  (views in memory) are not limits.
- **Window path.** It does not scale at all with many small requests. Throughput
  falls from 0.46M samples/s at K = 4 to 0.09M at K = 56, using only 3–7 cores.
  - Its coordinator is saturated: 250 s of CPU at K = 12 and 757 s at K = 56.
    perf shows it dropping and rebuilding raw-data pane maps, 15% of all samples
    at K = 56.
  - Its shards clone those `BTreeMap`s on every append, because the coordinator's
    published snapshot shares them (copy-on-write).
  - Push keeps no raw data for these views, so none of this cost exists there.
  - With fewer, larger requests the window path does better (0.51M at K = 28; the
    in-process bench reaches 1.4M with one request per scrape). It stays
    coordinator-bound.

### Demo workload A/B (push against the window path)

**Setup.**

- **Build.** The demo branch with push merged in, one binary for both arms;
  only `--precompute-mode` differs.
- **Workload.** The demo's own workload (`demo48`, docs/design/demo-plan.md
  §8): 19 queries over 14 metrics, 2,800 series per base label set at
  10 samples/s (about 342k samples/s routed to views, 4 paced feeders, Zipf
  values), every query from the stream start, 4 query users, 2 s allowed
  lateness, view files on.
- **Hosts.** Two hosts of the same model (2 × 10-core E5-2660 v2, 40
  threads), one arm each, started at the same aligned stream start. The
  engine is pinned to 20 CPUs (10 cores and their hyperthreads) in a cgroup
  of 20 cores and 100 GiB; feeders and the query client use the other 20.
- **Length.** 45 min of queries per arm, then a VictoriaMetrics arm on the
  same host as the accuracy reference (its post-run answers and PromQL over
  its kept raw samples).
- **Groups.** Push builds the three base refresh groups; the four wider
  window-chain levels (`SummaryMergeWindows`) keep the window path.

| | window path | push |
| --- | --- | --- |
| ASAP CPU, cgroup (core-seconds) | 1,620 | 1,589 |
| ingest shard threads / precompute threads (core-seconds) | 283 / 423 | 677 / 10 |
| working set, peak / steady (MB) | 941 / 730 | 750 / 657 |
| anon, peak / steady (MB) | 891 / 684 | 707 / 611 |
| `memory.current`, peak / steady (MB) | 2,279 / 1,902 | 2,066 / 1,832 |
| peak RSS (MB) | 920 | 744 |
| raw samples held at the end (MB) | 39.1 | 0 |
| view files on disk / view bytes in the heap (MB) | 793 / 73.9 | 793 / 70.7 |
| windows committed / failed | 1,057 / 0 | 1,057 / 0 |
| window ready after its end, mean / max (s) | 2.07 / 2.55 | 2.03 / 2.45 |
| samples accepted / dropped late | 778.2M / 0 | 778.2M / 0 |
| Remote Write requests failed (503 included) | 0 | 0 |
| queries answered | 14,400 / 14,400 | 14,400 / 14,400 |
| query latency p50 / p99 (ms) | 10.9 / 147.2 | 10.9 / 145.7 |
| within 5% of the reference (verified points) | 306 / 306 | 306 / 306 |

Steady is the median over the last 10 minutes. Window ready includes the
2 s lateness.

- **Answers.** Every recorded answer (360 per arm) is identical between the
  arms, and the per-query error against the reference is the same: 0–0.81%
  for exact and histogram queries and at most 5.00% for the quantile
  sketches. Three queries (54 points per arm) have no verifiable reference
  in the harness, in both arms.
- **Per-query latency.** p50 and p99 of each query are within the run-to-run
  noise of each other (largest p99 gap: the 1 h range query, 236 against
  250 ms).
- **Saturation point.** On one of the hosts, the same plan with 2,800
  series per metric of all 14 metrics stamped 15 min in the past and sent
  as fast as accepted (16 sender threads, 503 retried with backoff):

  | | window path | push |
  | --- | --- | --- |
  | samples accepted per second | 2.22M (88M more dropped late) | 3.59M (none dropped) |
  | engine CPU (core-seconds) | 2,111 | 869 |
  | peak RSS (MB) | 7,108 | 857 |

  The window path keeps raw samples until the windows reading them close,
  so under catch-up load it holds gigabytes of raw panes, and senders that
  run ahead close windows other senders' samples are still headed for;
  push holds no raw samples for its groups.

**Conclusion.** On the demo workload push uses 2% less CPU and 10–20% less
working set and anon memory, holds no raw samples, and has the same window
latency, query latency and answers. Under catch-up load it accepts 1.6× the
samples at 41% of the CPU and 12% of the memory. Push is the `data_plane`
default (D9); `--precompute-mode window` keeps the window path.

## Minimal complexity

- **No new partitioning, pool or flag for combine.** The ingest shards, the
  coordinator's flush, the window-task pool and the `precompute` thread
  already exist. This design adds instances on the shards, a seal message
  in the flush, and a reorder buffer in the committer.
- **No new operator layer.** Push is a channel-fed `DataStore` read and a
  local pool. Merge uses the executor's `SummaryMerge`, and the rule table
  reads the Planner's capability table without restating it.
- **One writer.** The committer keeps the view store single-writer, which
  copy-on-write snapshots, persistence order and barriers rely on.
- **Ordered-merge and exchange are specified, not built.**

## Alternatives and decisions

| Question | Decision | Rejected |
| --- | --- | --- |
| Execution model | per-source push on owned shards (D1) | window tasks only; morsel-driven shared pool; DataFusion runtime |
| Views of one source | inline on the shard thread (D2) | a thread per view or per pipeline (cache locality; buffered fan-out cost) |
| Partition key | today's series routing (D3) | vnodes (no per-shard state persists); round-robin for series-affine state |
| Non-mergeable | single partition, listed for ordered-merge (D4) | exchange now |
| Close work | fold on arrival, chunked seal (D5) | compute at close |
| Idle shards | flush-gated watermark, every shard seals every window (D6) | min over shards with idleness timeouts |
| Views over views | post-commit task (D7) | push sealed partials into coarse instances |
| Memory | reserve, then backpressure, no spill (D8) | a separate limit per instance |
| Threads | shards do the folding; combine on the window-task pool (D9) | a separate combine pool |
| Planner term | deployment calibration multipliers (D10) | shard count in the Planner |
| Where an instance seals | on its shard thread (its future is not `Send`) | moving states to a pool |
| Rule source | derived at install; install fails on no rule | a plan field; guessing |

## Quality attributes and evidence

- **Throughput:** sustained samples/s with a bounded backlog; samples/s
  per core against the window path (and the per-core context of 132k for
  the data plane against 385–443k for VictoriaMetrics).
- **Latency:** newest-window commit lag (`asap_window_commit_lag_seconds`)
  and ingest stalls during seal.
- **CPU:** push, seal, combine and commit, by group and shard.
- **Memory:** `asap_raw_data_bytes`, `asap_precompute_state_bytes`, RSS.
- **Debuggability:** the install log and status show each breaker's rule.
  A failed window names its group, window and shard.

## Acceptance and test design (for approval)

Each test states the behavior it verifies, its inputs, the expected
result, and whether it fails today. Tests are written after this plan is
approved and before the implementation. "Fails today" means the test fails
or cannot be built against the current data plane. A guard ("passes
today") protects a property the window path has by construction and the
push path could break. K is the ingest shard count (`--ingest-threads`).
Unless a row says otherwise, the push path is compared with the window
path of the same build.

| # | Behavior verified | Inputs | Expected | Today |
| --- | --- | --- | --- | --- |
| T1 | Push answers what the window path and the Planner executor answer on every parity workload. | All parity workloads (`tests/workloads/`, `examples/`), raw data kept and not kept, K = 1, 2, 4, 8 (`PARITY_INGEST_THREADS`), instant and range queries. | Equal to the reference. Plans with KLL are within the existing 5%. | Fails: no push mode. |
| T2 | Stored view rows, not only answers, equal the window path's. | The same workloads, K = 4. Each (view, window) is compared by sorted keys, with states compared as `sketch()` bytes or exact values. | Equal, except KLL states and float sums from a `Reduce` merge, which are within the plan target. | Fails: no push mode. |
| T3 | One morsel reaches every view of its source, inline, read once. | seed-862-01 (5–6 views on `data`), plus an o11y-shaped plan with 13 chains on one source (hand-written if the Planner does not produce it), K = 4. | Equal to the window path. `precompute_push` runs once per morsel and group, not per view. No queue between views. | Fails: no push mode. |
| T4 | Per-series breakers are concatenated, with each series exactly once. | A DDSketch chain per series, 1,000 series, K = 8, 3 windows. | One row per (window, series), equal to the window path. The rule is `Concatenate`. | Fails: no rule exists. |
| T5 | `Reduce` breakers merge to one state per group, including empty and global cases. | A hand-written plan with chains grouped by `job` for ExactAggregate(Sum, Count, Max), DDSketch, HLL and Count-Min; 4 jobs spread over K = 4; one window with no sample; a global `sum()` variant; a 2-key group-by. | One row per (window, job), equal to the window path (float sums within target). Global: one row per window, and the empty window has one empty row. The rule is `Merge`. | Fails: as T4. |
| T6 | A non-mergeable breaker runs on a single partition. A group with a breaker that is not a fold runs on the window-task pool. | A hand-written plan with an ingestion-time exact grouped quantile (single partition), and one with a raw window buffer (window path), K = 4. If the executor's compiler refuses either plan, a combine-level test is used instead. | Equal to the window path. The rules are `SinglePartition` and `WindowPath`. | Fails: as T4. |
| T7 | The combine rule follows the table, and install fails for a breaker with no rule. | Unit test over plan nodes: `PerEntity` and `Reduce` chains of every supported family, Hydra, counters, each exact `Aggregate` function, `Sort`, join, window function, and an unknown operator. | Each gets its rule. The unknown operator refuses install with its node named. | Fails: as T4. |
| T8 | Windows commit in window order when combines finish out of order. | A hook delays window 2's combine until window 3's is done, K = 2. | Stored and persisted order is 1, 2, 3. A barrier past window 3 answers only after window 2 commits. | Guard: passes today with one precompute thread. Needs the window-task pool once it lands. |
| T9 | A group that reads another group's views runs after the windows it reads commit. | A hand-written plan with group B (60 s) reading group A (15 s), and a hook delaying A's last window. | B runs only after A's four windows commit, and B's rows equal the window path. | Guard, if the compiler accepts the plan. |
| T10 | Seal is exact under shard skew: each instance gets exactly the samples admitted before the flush that seals it. | `shard_delay_ms` slowing all shards but the first, 8 shards; samples stamped up to 1 s ahead of the clock (the demo design's two race tests, run in push mode). | No late drops, and every window equals the 1-shard result. | Fails: no push mode. The window-path versions pass. |
| T11 | Idle shards seal empty and hold nothing back. | One series, K = 8. A selective plan whose series all hash to two shards. | Every window commits when the barrier passes it. Each shard sends one partial per window; idle shards send empty ones. | Fails: no push mode. |
| T12 | Pushed pipelines do not buffer raw samples. | The dashboard plan, raw data not kept, 10k series at 1 sample/s, W = 15 s, K = 4. `/metrics` is sampled before each close. With a query-time `Scan`, raw data is kept. | `asap_raw_data_bytes` stays below one request's samples, not rate × W × 20 B (3 MB per metric here), and `asap_precompute_state_bytes` is reported. With a `Scan`, raw bytes follow its lookback only. | Fails: raw panes grow until the window closes. |
| T13 | Late and out-of-order samples count in the same windows as the window path. | 5 s allowed lateness; samples 4 s late (window still open) and 20 s late (window sealed); counters included; K = 4; raw kept and not kept. | Stored windows equal the window path. The 20 s-late samples are counted as `late_for_views` and are read by query-time `Scan`s when raw is kept. | Fails: no push mode; the counter is new. |
| T14 | Backpressure goes from the commit backlog to the shard queues to 503, and loses nothing. | A gated combine, `--max-precompute-backlog 2`, a small `--max-queued-samples`, K = 4. | 503 once full. After the gate opens, every window commits and the answers equal an ungated run. | Fails: no push mode. |
| T15 | A memory reservation waits while older windows are open, and fails the window only when nothing else is open. | A precompute budget that fits one open window; lateness so that two windows overlap; then a single window over budget. | The first case delays ingest (queue then 503) with no failed window. The second fails that window only, and later windows commit. | Fails: no reservation exists. |
| T16 | Seal work is chunked, so ingest keeps running during a large seal. | 100k series, one 1 s DDSketch view, K = 2; requests sent continuously across window ends. | The longest gap between two ingested requests on a shard during a seal stays below the chunk bound times a small factor (the bound is set by E5). | Fails: no push mode. |
| T17 | A restart loads the view files whatever the shard count or mode. | Push with K = 4, then restart with K = 2, and again with `--precompute-mode=window`, writing no sample. | Answers are equal, and no window is appended twice. | Fails: no push mode. |
| T18 | A failing instance or combine fails only its window. | One shard exceeds an operator limit in window 2; separately, a combine forced to panic. | Window 2 is counted in `windows_failed` and its readers are not answered. Windows 1 and 3 commit, and the engine keeps running. | Fails: no push mode. |
| T19 | Precompute CPU and state are exported per group and shard. | Any plan, K = 4, a few windows. | `/metrics` has `precompute_push`, `precompute_seal`, `precompute_combine`, `precompute_commit`, `asap_precompute_state_bytes`, `asap_window_commit_lag_seconds`, with run counts matching windows and morsels. | Fails: the series are new. |
| B1 | Throughput benchmark (reported, not a CI gate). | `examples/ingest_bench.rs`: dashboard and seed-862-01, raw data not kept, 1k, 10k and 100k series; window path and push at K = 1, 2, 4, 8; paced, then unpaced to saturation. | The highest samples/s with a bounded backlog, samples/s per core, CPU per phase, RSS and raw bytes for each setting. | The window-path numbers come from today's code. |
| B2 | Close latency benchmark (reported). | As B1 at 10k series. | Newest-window commit lag (median and max) and ingest stall during seal, for the window path against push. | As B1. |

Experiments E1–E5 (run separately, results recorded in D1–D10) decide the
design. T1–T19 accept the implementation, and B1–B2 report it. The test
design is by the author of this document. An independent review of this
plan is requested in the PR, not claimed.

## Implementation plan

| Day | Work |
| --- | --- |
| 0 | T1–T19 and B1–B2, failing (after approval) |
| 1 | Rule table and install refusal (T7); per-instance channel store; instances and push on the shards |
| 2 | Seal in the flush; chunked seal; combine (concatenate, merge, single partition) on the window-task pool |
| 3 | Committer reorder buffer; pipelines above breakers; post-commit groups; window-path routing for groups that cannot be pushed |
| 4 | Reservations and backpressure; late-data counter; metrics; failure paths |
| 5 | Parity at K = 1–8; B1, B2; fixes |

## Risks, rollout, and exit criteria

- **Rollout.** Push becomes the default by the D9 rule.
  `--precompute-mode=window` stays for one release.
- **Exit.** If B1 shows no gain at 10k series, the default stays window
  mode and the profile is reported. The per-sample path (append plus
  per-batch fold) is then the next target.
- **Shard threads now do view work.** Heavy views slow ingestion on every
  shard. `precompute_push` shows it.
- **Hot series.** One series is bounded by one core (D3).
- **`Reduce` partials are multiplied by K while open.** The Planner term
  waits for D10.
- **KLL and float-sum merges** match within the plan target only.
- **Coupling to the coordinator's flush protocol.** Seal travels with the
  flush, so a change to that protocol changes this design.
