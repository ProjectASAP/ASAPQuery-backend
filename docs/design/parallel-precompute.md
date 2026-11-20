# Design: parallel precompute (push pipelines)

## Audience and context

Developers and architects of the backend data plane. This design extends
[the data plane demo design](data-plane-demo.md): ingestion shards samples
by series hash, one `precompute` thread runs `execute_ingestion` once per
closed refresh window, and the results go to the materialized view store,
which query workers read. Persistence runs in the background. A query-time
`OneEvaluation` node is computed once per evaluation time (single flight).
This document changes only how views are computed at ingestion time. What
is computed, the view store, queries and the file format stay the same.

Terms used below, as databases use them:

- **Pipeline**: a maximal chain of operators that pass each batch on as it
  arrives. It starts at a source (a `Scan`, or the output of an earlier
  pipeline) and ends at a sink (a pipeline breaker or a view). Example:
  `Scan lat → TimeRange → SummaryTumblingWindowChain`.
- **Morsel**: one batch pushed through a pipeline, here the samples of one
  Remote Write request that one shard receives for one metric.
- **Pipeline breaker**: an operator that must see all of its input before
  it can output anything. It is always its pipeline's sink.
- **Instance**: one running copy of a pipeline for one (shard, refresh
  window).
- **Combine**: the step at window close that turns the instances' partial
  outputs into the window's single output.
- **Commit**: appending a window's rows to the view store, publishing the
  new snapshot, and queueing the rows for persistence.

## Problem and why now

All ingestion-time work runs on one thread. Each closed window of each
refresh group is one `execute_ingestion` run over the raw samples of that
window. Three limits follow:

- **CPU.** Precompute cannot use more than one core. The dashboard plan
  (example 4 of the demo design) has a single refresh group, with all four
  views at W = 15 s. Every 15 s one run builds 10k KLL sketches, 10k Max
  states, 10k Rate states and 10k Increase states from 150k samples per
  metric. If the series count goes up 10x, the run takes 10x as long on
  the same core. Once it takes longer than 15 s, the backlog grows, the
  ingest thread waits and Remote Write gets 503.
- **Memory.** Raw samples are kept until the windows that read them close.
  A view whose plan does not keep raw data still holds about input rate ×
  window width × bytes per sample. A sample is 20 bytes (time, value,
  series code). At 100k samples/s that is 60 MB for a 30 s window and
  600 MB for a 300 s window (the 5-minute KLL chain of
  `sketch-plan-chains-10m-5m-3m-10s`). The summary built from those samples
  is far smaller.
- **Latency peaks.** All of a window's work happens at once when it
  closes, so precompute CPU is idle most of the time and then spikes.

Ingestion is now sharded by series hash. Each shard already holds a
disjoint set of series, so the same shards can run the view pipelines. No
new partitioning is needed.

## Inputs, outputs, and end-to-end behavior

- Inputs: the same as before. Remote Write samples go to the ingest shards,
  and the plan is installed in the same way.
- Outputs: the same views, rows and view files. Queries get the same
  answers. A sketch whose build is randomized (KLL), or whose merge breaks
  ties among heap candidates (heap-carrying sketches, UnivMon), answers
  within the tolerance the parity tests already allow.
- Changed behavior:
  - Each shard pushes its samples into the open windows' pipeline
    instances as they arrive.
  - At window close, each shard finalizes its instances, a pool combines
    the partial outputs, and one committer commits the windows in window
    order.
  - When a plan does not keep raw samples, they are no longer buffered for
    views. Each sample is folded into the summaries as soon as it arrives.
  - `/metrics` breaks precompute CPU down by refresh group, shard and step
    (push, finalize, combine, commit).
- `--no-parallel-precompute` keeps the single-thread precompute described
  in the demo design, for comparison and for the parity tests.

## Goals and non-goals

Goals:

- Precompute throughput grows with the shard count on plans whose
  ingestion-time breakers are per-series summaries. That covers every
  ingestion-time view in the current parity workloads.
- When raw data is not kept, raw memory no longer depends on window width.
  Only the open instances' summary state and the in-flight morsels are
  held.
- Answers and stored view rows are the same as with single-thread
  precompute, on every parity workload, for shard counts 1, 2, 4 and 8.
- No change to the Planner executor is needed for the plans in the parity
  workloads. The changes some other operators would need are listed, as
  separate ASAPPlanner work.

Non-goals:

- Parallel query execution. Queries already run on parallel workers, one
  run per worker.
- Changing the shard count while the data plane runs, or moving series
  between shards.
- A generic push-operator interface in the executor. Push is built from
  today's pull operators fed by a channel (see "Executor reuse").
- Exchange (repartitioning by group key) for non-mergeable breakers. It is
  specified here, but v1 implements only the single-partition route. No
  plan in the workloads has a non-mergeable ingestion-time breaker (see
  "Combine").
- Persisting open windows or raw samples. A restart loses open windows, as
  it does today.

## Heilmeier questions

- **What are we trying to do?** Compute the views the plan precomputes on
  all cores, as samples arrive, instead of on one core when each window
  closes.
- **How is it done today, and what are the limits?** One thread runs one
  batch execution per closed window, over buffered raw samples. Throughput
  is capped at one core and raw memory grows with window width.
- **What is new?** The refresh group's ingestion DAG is split into push
  pipelines at its pipeline breakers. Each ingest shard runs its own
  instance of each pipeline. At window close, the breakers' partial states
  are combined by a rule taken from the plan: concatenate per-series
  states, merge mergeable states, or route non-mergeable ones to a single
  partition. The executor's existing operators and `SummaryMerge` do the
  work.
- **Who cares?** Anyone running the demo or a deployment at high series
  counts. Today the precompute backlog is the first limit reached.
- **Risks and costs?**
  - Partial states are multiplied by the shard count for views grouped by
    coarse labels.
  - Combine and commit add a step at every window close.
  - The executor's runs are not `Send`, so an instance stays on its shard
    thread.
- **How long?** About 4 days after the test plan is approved (see
  "Implementation plan").
- **Checks?** Midterm: the parity test passes at shard counts 1, 2, 4 and
  8 with raw data kept and not kept. Final: the throughput benchmark shows
  samples/s and CPU against single-thread precompute at 10k and 100k
  series.

## Proposed design

```
Remote Write ─► HTTP threads: decode, intern, route by series hash
                     │ (per-shard FIFO queues: samples and Close events,
                     │  in the order the router decided them)
        ┌────────────┼────────────┬────────────┐
        ▼            ▼            ▼            ▼
     shard 0      shard 1       ...         shard K-1
     append raw (only what query-time Scans read)
     push each morsel into the open instances of every refresh
     group that scans its metric:
        instance(g, window e): Scan → Filter/Project/TimeRange → breaker
     on Close(g, e): end the instance's input, run it to completion,
     send its partial output (Send batches) to the combine stage
        └────────────┴─────┬──────┴────────────┘
                           ▼
       combine pool (`--precompute-threads`): once all K partials of
       (g, e) are in, run the breaker's combine (concatenate / SummaryMerge
       / single partition), then the pipelines above the breaker
                           ▼
       committer (the existing `precompute` thread): commits each group's
       windows in window order; publishes the view snapshot; queues
       persistence; schedules groups that read this group's views
                           ▼
                 materialized view store  ◄── query workers (unchanged)
```

### Pipelines from the refresh group's DAG

Each refresh group's ingestion run is compiled once at install, as it is
today (`compile` over the group's views). Then it is split into pipelines
by walking down from each view to its sources and classifying each
operator:

| Operator (physical) | Class | Note |
| --- | --- | --- |
| `Scan` (raw data slot) | source | One per scanned metric. Each shard feeds its own copy. |
| `Filter`, `Project`, `TimeRange` (ingestion window), `TimeShift`, label operators, scalar functions per row | non-blocking | Each row is kept, dropped or changed on its own. |
| `SummaryTumblingWindowChain`, `SummaryAgg` (every summary build: per group, keyed, all-groups) | breaker | Emits states at window close. Keeps one state per group while the window is open. |
| `Aggregate` (exact; sum, count, min, max, avg, quantile, count_values, topk, …) | breaker | Exact quantile and exact distinct hold their rows. |
| `Sort`, `Sort` + `Limit` | breaker | |
| Hash join: build side | breaker | The probe side is non-blocking once the build side has finished. |
| `SummaryMerge`, `SummaryEstimate`, `FinalizeExactAccumulator` above a breaker | non-blocking on the breaker's output | These run in the pipeline that starts at the breaker's output. |
| Materialized view slot of another group | source of a post-commit pipeline | See "Pipelines across refresh groups". |

A pipeline runs from a source through non-blocking operators to a breaker
or a view. A node that feeds several breakers (one `Scan` under two chains)
stays a single node in the instance. The executor's ingestion runtime
already sends each batch to every consumer, so a morsel is read once and
reaches every chain.

The output of a breaker starts the next pipeline. That pipeline runs once
per window, after combine, on the combined rows. For example, if a view is
`topk` over an exact `sum by (job)` built at ingestion, the second
pipeline is `combined sum → topk → view`. The stage between the two
pipelines is reached only at window close.

### Example 1: the dashboard plan (one refresh group, four views)

This is the plan of example 4 in the demo design (`dashboard.json`,
10k series per metric, 1 sample/s, every query every 15 s):

```
n0   Ingestion None               Scan lat
n1   Ingestion AcrossEvaluations  SummaryTumblingWindowChain KLL W=15s       <- n0
n6   Ingestion AcrossEvaluations  SummaryTumblingWindowChain Max W=15s       <- n0
n10  Ingestion None               Scan reqs
n11  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Rate W=15s      <- n10
n17  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Increase W=15s  <- n10
(query-time nodes as in the demo design)
```

(Plans from newer Planner revisions may pick DDSketch instead of KLL, or
compute the quantile at query time. The pipeline shapes do not change.)

- Pipelines: there is one refresh group {n1, n6, n11, n17} with
  W = 15 s and phase 0, and two pipelines.
  - P1 is `Scan lat → {KLL chain n1, Max chain n6}`.
  - P2 is `Scan reqs → {Rate chain n11, Increase chain n17}`.
  - All four sinks are breakers. Each is `PerEntity`: one state per series.
- Execution with K = 4 shards:
  - Each shard owns about 2.5k `lat` series and 2.5k `reqs` series.
  - When a Remote Write request reaches shard s, its `lat` samples become
    one morsel. The morsel is pushed into instance (s, window e) of P1,
    where it updates s's KLL and Max states for those series. The `reqs`
    morsel updates s's Rate and Increase states in P2 the same way.
  - Nothing is kept for these views after the push. This plan does not
    keep raw data, so the shard keeps nothing at all.
- Window close at e:
  - The router sends Close(g, e) to all four shards.
  - Each shard ends its instances' input and runs them to the end. Each
    chain emits the rows for its 2.5k series as `(window_end, series
    keys, state)`.
  - The combine concatenates the four partial outputs per view. The
    series sets are disjoint, so no state is merged.
  - The committer appends the four views' rows for (e − 15 s, e].
- Query: unchanged. For example, `topk(10, sum by (job) (rate(reqs[1h])))`
  still merges 240 windows of n11 per series, then aggregates by `job` at
  query time.

### Example 2: a grouped query, aggregated at query time

`avg by (region) (quantile_over_time(0.9, data[5m]))`
(`sketch-plan-quantile-grouped`), raw data not kept:

```
n0  Ingestion None               Scan
n1  Ingestion AcrossEvaluations  SummaryTumblingWindowChain DDSketch W=10s α=0.05  <- n0
n2  Query     None               TimeRange 300s                                     <- n1
n3  Query     OneEvaluation      SummaryMerge                                       <- n2
n4  Query     None               SummaryEstimate q=0.9                              <- n3
n5  Query     None               Project                                            <- n4
n6  Query     OneEvaluation      Aggregate avg() by (region)                        <- n5   (root)
```

The `by (region)` grouping is applied at query time. The ingestion-time
breaker n1 is per series (`PerEntity`), so the combine concatenates the
partial outputs, as in example 1. Every ingestion-time view in the current
parity workloads has this form: a per-series chain whose grouping happens
at query time. This is why v1 is expected to need no merge for them.

### Example 3: an ingestion-time view grouped by coarser labels

The plan IR allows a chain or `SummaryAgg` grouped by a label set
(`Reduction::Reduce(GroupKeys)`), for example
`sum by (job) (sum_over_time(m[1m]))` built at ingestion as a single
`ExactAggregate(Sum)` chain grouped by `job`:

```
n0  Ingestion None               Scan m
n1  Ingestion AcrossEvaluations  SummaryTumblingWindowChain ExactAggregate(Sum) W=60s by (job)  <- n0
```

- Series of `job="api"` are spread over all K shards, so each shard's
  instance holds a partial sum for `job="api"`.
- At close, the combine runs the executor's `SummaryMerge` grouped by
  `(window_end, job)` over the union of the K partial outputs.
  `ExactAggregate(Sum)` merges are exact, so the merged state equals the
  single-thread state.
- The same applies to Max, Min, Count, DDSketch, HLL, Count-Min and KMV:
  their merges do not depend on order. KLL merges are randomized, and
  heap-carrying sketches (Count-Min or Count Sketch with heap, UnivMon)
  re-rank their candidate heaps when they merge. For those, the merged
  state equals the single-thread state only within the sketch's error.
- The current Planner does not select this shape on the parity workloads.
  The tests use a hand-written plan for it (see the test plan).

### Combine: which rule, chosen how, recorded where

The rule is chosen once at install, per breaker, from the plan node alone:

| Breaker | Condition | Combine | Result vs single thread |
| --- | --- | --- | --- |
| Summary build (chain or `SummaryAgg`) | `Reduction::PerEntity`: one state per series, and series are partitioned by shard | **Concatenate** the K outputs. Rows are disjoint. | identical states |
| Summary build | `Reduction::Reduce(keys)`, and `SummaryTypeCapability::of(summary_type).merge_inputs` is `Disjoint` or `MayOverlap` (`merges_groups`) | **Merge**: `Union(K) → SummaryMerge` grouped by the output's group columns (and `window_end` for a chain) | identical for exact accumulators, DDSketch, HLL, Count-Min, KMV; within the sketch's error for KLL and heap-carrying sketches |
| Hydra summary build (one grid for all groups, `shares_one_state_across_groups`) | any | **Merge** the K grids (`SummaryMerge` merges Hydra grids pairwise) | identical counters |
| Summary build | `Reduce(keys)`, and the family has no group merge (counters Rate, Increase, irate: `DisjointInTime`; or `merge_inputs: None`) | **Single partition** | identical |
| Exact `Aggregate` sum, min, max, count, topk, bottomk | grouped or global | **Re-aggregate**: run the same function over the K partial results (count becomes a sum of counts) | identical, except float sums, which may differ in the last bits because addition order changes |
| Exact `Aggregate` avg, quantile, count distinct, stddev and other non-decomposable functions; `Sort` without `Limit`; join build side | any | **Single partition** (v1); **exchange by group-key hash** when profiling shows the single partition is the bottleneck | identical |

- **Concatenate.** No executor call is needed. The combined output is the
  K partial batch lists in shard order. Row order inside a view is not
  part of the store's contract: reads select windows and keys, and the
  parity test compares answers by series.
- **Merge.** At install, a combine DAG is built for each breaker:
  `Operator::source` × K → `Operator::union(K)` →
  `Operator::summary_merge(state, groups)`. Each shard emits the empty
  global row when its build has no group keys and no input
  (`emit_empty_global`), so merging K such rows gives the one empty row
  the single-thread run stores for an empty window.
- **Single partition.** The breaker's pipeline instance exists only on
  shard 0. The other shards run the non-blocking prefix of the pipeline
  (`Scan → Filter → Project`) and forward its output batches to shard 0's
  instance through a channel. This is an exchange with one target. The
  forwarded rows are filtered and projected, but they are still rows
  until the window closes.
- **Exchange (specified, not in v1).** There is one breaker instance per
  shard. Each shard routes a prefix-output row to shard
  `hash(group key) mod K`. The combine is then a concatenation. It is
  needed only when a non-mergeable breaker's single partition limits
  throughput. No plan in the workloads has such a breaker.
- **Where the choice is recorded.**
  - The data plane's compiled refresh group keeps the rule for each
    breaker node.
  - The install log line lists `group, node, operator, rule`.
  - `/api/v1/status/asap` reports the rule per breaker under
    `ingestion.pipelines`.
  - The rule is derived from the plan (`Reduction` and the Planner's
    `SummaryTypeCapability`, which is the authoritative merge rule). The
    plan format does not change.

### Inter-operator parallelism

- **Pipelines in one window run at the same time.** On one shard, P1 and
  P2 of the dashboard plan are separate instances. A morsel of `lat` goes
  only to P1. Different shards run their instances in parallel.
- **Windows and refresh groups run at the same time.** Windows of
  different groups close independently, and their combines run on
  different pool threads. Consecutive windows of one group may also be
  combined at the same time. The committer restores order (see
  "Scheduling").
- **Pipeline DAG.** At install, the refresh groups and their pipelines
  form a DAG:
  - A pipeline whose source is another pipeline's breaker output depends
    on that breaker's combine.
  - A group that reads another group's views (`inputs.materialized_views`,
    already ordered by `ingestion_refresh_groups`) depends on that group's
    commits.

### Pipelines across refresh groups

A group whose ingestion run reads another group's view, such as a coarse
chain over a fine chain (the "Hierarchical rollups" target design), reads
no raw samples through that view. It runs as a post-commit pipeline: one
task in the combine pool per closed window (e − W_B, e] of group B, over
the view snapshot. The task is scheduled once group A has committed every
window inside (e − W_B, e]. This is today's batch `refresh`, triggered by
commits instead of by the watermark.

A group that reads both raw samples and another group's views runs, in v1,
as a single-thread batch run at window close over the raw panes and
views, as it does today. Its scanned metrics keep their raw panes until
that run.

### Scheduling

- **Shard threads (push).**
  - Each shard thread owns its instances. An instance is the group's
    `CompiledPhysicalDAG` instantiated over a per-instance store, whose
    `read_raw_data` returns the receiving end of a channel.
  - `execute_ingestion` gives a future. The shard keeps it in a local
    executor (`futures::executor::LocalPool`).
  - After pushing a morsel into an instance's channel, the shard runs the
    pool until it stalls. The morsel then passes through the pipeline to
    the breaker's state before the next morsel is read.
  - Instances are created when a window opens. A window opens when the
    group starts or the previous window's end passes, for every group on
    every shard. Every shard opens every window, so an idle shard still
    produces its part, and the empty-window rule stays the same.
  - With allowed lateness L, a group has at most ⌈L / W⌉ + 1 windows open
    on a shard at a time.
- **Close (event).**
  - The router decides when the watermark passes a window end e, exactly
    as `close_windows` does today (the global watermark is the minimum
    over shards, and idle shards are sent the time).
  - It then puts Close(g, e) in every shard's FIFO, after every sample it
    routed before that decision. A shard therefore closes the window
    after exactly the samples the single-thread engine would have read.
  - On Close, the shard ends the instance's input channel, runs the
    instance to completion (the breaker finalizes and emits), and sends
    `(g, e, shard, Result<batches>)` to the combine stage.
  - Finalizing on the shard thread is required: the instance's future is
    not `Send` (the executor's runtime uses `Rc`). Only its output
    batches, which are `Send`, leave the thread.
- **Combine (task).**
  - The combine stage counts the partials of each (g, e). When all K have
    arrived, it schedules one task on the combine pool
    (`--precompute-threads`).
  - The task runs each breaker's combine, then the pipelines above the
    breakers, and returns the group's view rows for the window.
- **Commit (ordered).**
  - The committer is the existing `precompute` thread. It keeps a
    reorder buffer per group and commits window e only after every
    earlier closed window of that group has been committed or has failed.
  - Each commit appends the rows, publishes the view snapshot and queues
    persistence, as today. Then the committer schedules the post-commit
    pipelines that were waiting on the commit.
  - A view's windows therefore always appear in window order, and a
    persist thread receives each group's windows in order.
- **Barrier.** The event-time barrier answers once every window it closed
  has been committed and queued for persistence, as today.

### Watermark and late data

The rules stay the same. Only where they apply changes:

- A sample whose window on its shard is still open (it is within the
  allowed lateness) is pushed into that instance and counted, as the
  single-thread run would count it from the panes.
- A sample whose window has already closed on its shard does not change
  the stored window, as today. It is still appended to the raw panes if a
  query-time `Scan` reads its metric. A new counter,
  `asap_samples_total{outcome="late_for_views"}`, counts such samples, so
  the loss is visible.
- A sample older than every open window and older than every query-time
  `Scan`'s lookback is dropped (`dropped_late`), as today.
- Counter states (Rate, Increase) see each series' samples in the order
  they arrived. A series lives on one shard, and that shard's queue is
  FIFO, so the order is the same as in the single-thread run, which reads
  chunks in arrival order.

### Backpressure

- The shard input queues are bounded as before (`--max-queued-samples`,
  503 when full). A shard does its push work inline, so a slow pipeline
  slows its own shard and fills its own queue.
- Windows that have closed but are not yet committed (waiting for
  partials, combining, or waiting for an earlier window) are bounded by
  `--max-precompute-backlog`, with the same meaning and metric
  (`asap_precompute_backlog_windows`). When the backlog is full, the
  router stops issuing Close events and stops taking from its queue.
  Remote Write then gets 503 as before.
- The channel between a shard and an instance never holds more than one
  morsel, because the shard drains it right away.
- In single-partition mode, the channel from the other shards to shard 0
  is bounded in bytes. A full channel makes those shards wait.

### Memory accounting

- **Per-series breaker (`PerEntity`).** The states add up to the same
  total as with a single thread, because each series' state lives on
  exactly one shard. Each instance adds a fixed overhead (group table,
  operator buffers), multiplied by K × groups × open windows.
- **Breaker grouped by coarser labels (`Reduce`).** Every shard can hold a
  state for every group. With G groups the cost is up to min(K, series per
  group) × G states instead of G. A Hydra grid is multiplied by K. A
  low-cardinality view, such as `sum by (job)` with 10 jobs, therefore
  costs up to K times as much while its window is open. The stored
  (combined) view does not change.
- **Raw panes.** Only query-time `Scan` lookback, and groups that run as a
  batch over raw data, keep raw panes. For a plan with raw data not kept,
  `asap_raw_data_bytes` stays near zero instead of growing to rate × W ×
  20 bytes.
- **Limits.** Each instance has its own `RunContext` with
  `--run-memory-limit-bytes`, so open instances can hold up to
  K × groups × open windows × the limit in total. A new gauge,
  `asap_precompute_state_bytes{group,shard}`, reports what the open
  instances hold.
- **Cost model.** The Planner's ingestion memory term needs:
  - the K multiplier for `Reduce`-grouped and Hydra views;
  - zero raw buffer for streamed pipelines, in place of input rate ×
    longest window × bytes per sample.

  This is the cost-model note of the demo design's sharded-ingestion
  target. It belongs in Planner cost inputs (a shard count, and whether
  the backend streams ingestion-time pipelines), as separate Planner work.

### Persistence

- The committer queues the combined rows of a window: the rows that went
  into the view store, as today. Partial per-shard states are never
  persisted.
- The file format, one file per refresh group, the per-group writer and
  syncing are unchanged. The shard count is not stored in the files, so a
  restart with a different shard count, or with
  `--no-parallel-precompute`, loads the same files.
- At startup, a window that every view of a group already stores is not
  opened on any shard. This is today's `AlreadyStored`, checked when a
  window opens instead of when it closes.

### Metrics

Each of these is added to `asap_phase_cpu_seconds_total`,
`asap_phase_wall_seconds_total` and `asap_phase_runs_total`, with thread
CPU from `CLOCK_THREAD_CPUTIME_ID` as before:

- `phase="precompute_push"` with `group` and `shard`: the time to push one
  morsel through one group's pipelines, measured once per morsel and
  group, not per row.
- `phase="precompute_finalize"` with `group` and `shard`.
- `phase="precompute_combine"` with `group`, and
  `phase="precompute_commit"`.
- `asap_precompute_state_bytes{group,shard}`,
  `asap_precompute_open_instances{shard}`,
  `asap_samples_total{outcome="late_for_views"}`.
- The single-thread `phase="precompute"` series is kept for
  `--no-parallel-precompute`.

### Failure handling

- **An instance fails** (an operator error, such as a counter sample out
  of order beyond the declared bound, or the memory limit). The shard
  stops feeding it and later sends `Err` as its partial. The combine marks
  (g, e) failed and the committer skips it. `windows_failed` is
  incremented, and queries that read the window are not answered (missing
  rows), as with a failed run today. Later windows commit normally.
- **A combine task panics.** The panic is caught. The window is failed
  and logged, and the pool thread keeps serving, as query workers do
  today.
- **A shard thread panics.** The engine stops (`RunningGuard`), as an
  ingest thread panic does today.
- **The committer or a persist thread fails.** Unchanged.

### Executor reuse and what would need an ASAPPlanner change

Reused unchanged (all public at the pinned revision):

- `compile` and `CompiledPhysicalDAG::instantiate`, once per (shard,
  window) instance, with a per-instance `DataStore` whose
  `read_raw_data` returns a channel stream.
- `execute_ingestion`, which polls the roots together and sends each batch
  to every consumer. Summary builds already update their state batch by
  batch from a stream (`build_per_group_summary` loops over
  `input.next().await`), so a channel-fed run is push execution without a
  new operator interface.
- `Operator::source`, `Operator::union`, `Operator::summary_merge` and
  `PhysicalDAG::execute`, for the merge combine.
- The pipelines above a breaker reuse `compile` with the breaker bound as
  a materialized view slot, read from an in-memory store holding the
  combined rows. This is the same trick the query engine uses to cut out a
  shared `OneEvaluation` node.

Would need an ASAPPlanner PR, only when a plan needs it:

1. **Partial exact aggregation.** An ingestion-time exact `Aggregate` avg
   (or any decomposable function whose partial is not its final value)
   needs either a partial-state output (sum and count as exact
   accumulator states) or a lowering to `SummaryAgg ExactAggregate` +
   `SummaryMerge` + `FinalizeExactAccumulator`. The second option needs no
   new runtime interface, only a physical-compiler change.
2. **Thread-local state with a combine, for exact quantile, distinct,
   Sort and join build.** There is no partial state to merge. Exchange
   needs no executor change, because rows are routed before the operator.
   An executor interface (`PhysicalOperator` with
   `partial_state()`/`combine()`) would be needed only to merge sorted
   runs or hash tables without moving rows. That is not proposed.
3. **Deterministic KLL merge** (a seeded compaction coin). This is needed
   only if bit-identical parity is wanted for KLL; today's tolerance
   already covers it. It is an `asap_sketchlib` change.

## Minimal complexity

- **No new partitioning.** The ingest shards' series hash is the partition
  key. Per-series views, which are all ingestion-time views in today's
  plans, need no merge.
- **No new operator layer.** Push is a channel-fed `DataStore` read
  together with a local executor on each shard. The merge combine is the
  executor's `SummaryMerge`. The rule table reads the Planner's
  capability table and does not restate it.
- **One committer.** The existing precompute thread becomes the committer,
  so the view store keeps a single writer and the copy-on-write snapshots
  of the demo design.
- **Exchange is specified but not built.** Single partition covers the
  non-mergeable case until a plan and a profile need more.

## Alternatives and decisions

| Question | Decision | Rejected, and why |
| --- | --- | --- |
| Unit of parallelism | (shard, window) instances of push pipelines, partitioned by series hash | **Window-level tasks only** (today's batch run, several (group, window) runs on a pool). This needs no executor work, but one window of one group is still one core. The dashboard plan has a single group, so it would gain only the overlap of consecutive windows. Raw samples would still be buffered for the whole window. |
| Scheduling model | push per morsel on the shard threads; window close as a scheduled combine task | **Morsel-driven, DuckDB-style** (a pool pulls morsels from any partition, with work stealing). The executor's runs are not `Send`, so an instance cannot move between threads. Every breaker would need thread-local and global state for any row from any thread, which mergeable summaries have but exact operators do not. The ingest shards already balance by series hash. |
| Runtime | the Planner executor | **DataFusion's runtime** (partitioned `ExecutionPlan`, `RepartitionExec`). It replaces the executor's operator layer, which ASAPPlanner#1421 tracks as separate multi-week work. Its streams are pull-based per partition, so ingestion would still need a push adapter. |
| Combine rule source | derived at install from `Reduction` and `SummaryTypeCapability` | A new field in the plan. The Planner would then decide a deployment detail (the shard count) that it does not know. |
| Non-mergeable breakers | single partition in v1; exchange specified | Exchange now: no plan needs it yet. |
| Where breakers finalize | on the shard thread that owns the instance | Moving states to the pool: the instance is not `Send`, and its outputs (states) already are. |
| Commit | one committer, in window order per group | Lock-free commits from pool threads: the view store's single-writer snapshots, persistence order and barriers all assume a single writer. |
| Opening windows | every shard opens every window | Lazy opening, at the first sample: an idle shard would have no part to send, and empty global rows would need a special case. |

## Quality attributes and evidence

- **Throughput.** Sustained samples/s at a bounded backlog, against
  single-thread precompute, at K = 1, 2, 4, 8 (benchmark below).
- **CPU.** Per-phase CPU (push, finalize, combine, commit) by group and
  shard. The total should be within the cost of single-thread precompute
  plus the combine.
- **Memory.** `asap_raw_data_bytes`, `asap_precompute_state_bytes` and RSS,
  against single-thread precompute.
- **Debuggability.** The install log and `/api/v1/status/asap` show each
  breaker's rule. A failed window names its group, window and shard.

## Acceptance and test design (for approval)

Each test states the behavior it verifies, its inputs, the expected
result, and whether it fails today. The tests are written after this plan
is approved and before the implementation. "Fails today" means the test
fails, or cannot be built, against the current data plane. A guard (marked
"passes today") keeps a property that the single-thread engine has by
construction and that the parallel engine could break.

| # | Behavior verified | Inputs | Expected | Today |
| --- | --- | --- | --- | --- |
| T1 | Parallel precompute answers what single-thread precompute and the Planner executor answer, on every parity workload. | Every workload of the parity test (`tests/workloads/`, `examples/`), planned with raw data kept and not kept; K = 1, 2, 4, 8; the parity test's samples and queries; instant and range queries. | Each answer equals the Planner executor's reference and the `--no-parallel-precompute` data plane's answer. They are exact except for plans with KLL, which stay within the existing 5% tolerance. | Fails: there is no parallel precompute to select. |
| T2 | Stored view rows, not only answers, are the same as with a single thread. | The same workloads, K = 4. For each (view, window), the rows are compared after sorting by key, with states compared by their `sketch()` bytes or exact values. | Equal for every non-KLL state. KLL states answer within tolerance. | Fails: as T1. |
| T3 | A per-series breaker's outputs are concatenated, with each series exactly once. | A DDSketch chain per series (W = 10 s), 1000 series spread over K = 8 shards, 3 windows. | One row per (window, series), equal to the single-thread row; the combine is `Concatenate` in the status. | Fails: as T1. |
| T4 | A grouped mergeable breaker's partials are merged into one state per group. | A hand-written plan with an ingestion-time chain grouped by `job` for each of ExactAggregate(Sum), Max, DDSketch, HLL and Count-Min; 4 jobs whose series are spread over K = 4 shards; one window with no sample; a global (`sum()`) variant. | One row per (window, job), equal to the single-thread row. The global variant has one row per window, and the empty window has one empty row. The rule is `Merge`. | Fails: as T1. |
| T5 | A non-mergeable breaker runs as a single partition and answers as with a single thread. | A hand-written plan with an ingestion-time breaker the rule table routes to single partition (an exact grouped quantile `Aggregate`, or a Rate chain grouped by `job`), K = 4. If the executor's compiler refuses such a view, this is replaced by the unit test of the rule (T6) plus a combine-level test that feeds the forwarded batches to shard 0's instance. | Equal to single-thread; the rule is `SinglePartition`. | Fails: as T1. |
| T6 | The combine rule is derived from the plan node as the table says. | Unit test over plan nodes: PerEntity and Reduce chains for each supported summary family, Hydra, the counters, exact `Aggregate` functions, `Sort`, `Sort` + `Limit`. | Each gets the rule in the table. | Fails: no rule exists. |
| T7 | Windows commit in window order even when combines finish out of order. | A test hook delays the combine of window 2 of a group until window 3's combine has finished; K = 2. | The view's stored windows and the persisted records appear in the order 1, 2, 3. Before window 2 commits, a query reading windows 2–3 is not answered with window 2 missing (the barrier waits). | Passes today (guard): there is one thread and no hook. |
| T8 | A group that reads another group's views runs after the windows it reads are committed. | A hand-written plan in which group B (W = 60 s) reads group A's view (W = 15 s); a hook delays A's combine of its last 15 s window inside B's window. | B's window runs only after A commits all four windows. B's rows equal the single-thread rows. | Passes today (guard), if the compiler accepts the plan. |
| T9 | Backpressure: a slow combine first fills the backlog, then the shard queues, then Remote Write gets 503. No window is lost. | The combine is gated (as `hold_precompute` does), `--max-precompute-backlog 2`, small `--max-queued-samples`, K = 4. | 503 once the queues are full. After the gate opens, every window commits and the answers equal those of an ungated run. | Fails: the parallel path does not exist. A single-thread backpressure test already exists. |
| T10 | Mergeable-summary pipelines do not buffer raw samples (bounded memory). | The dashboard plan with raw data not kept, 10k series at 1 sample/s, W = 15 s, K = 4. `/metrics` is sampled before every window close. | `asap_raw_data_bytes` stays below one request's samples, not rate × W × 20 bytes (3 MB per metric here). `asap_precompute_state_bytes` is reported. With a query-time `Scan` (raw kept), raw bytes match its lookback only. | Fails: today the raw panes grow until the window closes. |
| T11 | Late and out-of-order samples count in the same windows as with a single thread. | An allowed lateness of 5 s; series whose samples arrive up to 4 s late (still in the window) and 20 s late (after close); counters among them; K = 4; raw kept and not kept. | Stored windows equal single-thread. The 20 s-late samples are not in the views, are counted as `late_for_views`, and are read by query-time `Scan`s when raw is kept. | Fails: as T1 (and the counter is new). |
| T12 | An idle shard does not hold windows open. | One series (so K − 1 shards get nothing), K = 8; barriers close windows. | Every window commits when the barrier passes it, as with a single thread. | Fails: as T1. |
| T13 | A restart over the same view files loads them whatever the shard count. | Run with K = 4 and persistence on, stop, restart with K = 2 and again with `--no-parallel-precompute`, writing no sample. | Answers equal before and after. No window is appended twice (`AlreadyStored`). | Fails: as T1. |
| T14 | A failing instance fails only its window. | A memory limit that one shard's instance exceeds in window 2 (skewed series); separately, a combine task forced to panic. | Window 2 is counted in `windows_failed` and queries reading it are not answered. Windows 1 and 3 commit. The engine keeps running. | Fails: as T1. |
| T15 | Per-shard, per-group precompute CPU is exported. | Any plan, K = 4, after a few windows. | `/metrics` has `precompute_push`, `precompute_finalize`, `precompute_combine` and `precompute_commit`, with `group` and `shard` labels where applicable, and the run counts match the windows and morsels. | Fails: the series are new. |
| B1 | Throughput benchmark (reported, not a CI gate). | `examples/ingest_bench.rs` with the dashboard workload, raw data not kept, at 10k and 100k series. Single-thread precompute and K = 1, 2, 4, 8. Paced, then unpaced to saturation. | Report the highest samples/s at which the backlog stays bounded, CPU per phase, RSS and raw bytes for each setting. | Single-thread numbers come from today's code. |

The existing tests (parity, range parity, concurrency, persistence,
shared results) keep running with the default setting. Test design is by
the author of this document. An independent review of the test plan is
requested in the PR, not claimed.

## Implementation plan

| Day | Work |
| --- | --- |
| 0 | Tests T1–T15 and B1, failing (after approval) |
| 1 | Rule table (T6); per-instance channel store; shard-side instances and push; Close events in the shard FIFOs |
| 2 | Combine stage and pool (concatenate, merge, single partition); committer reorder buffer; pipelines above breakers |
| 3 | Post-commit pipelines across groups; backpressure; late-data counter; metrics; failure paths |
| 4 | Parity at K = 1–8; benchmark; fixes |

## Risks, rollout, and exit criteria

- **Rollout.** Parallel precompute is on by default once T1–T15 pass;
  `--no-parallel-precompute` restores the single-thread path. Exit
  criteria: B1 shows higher sustained samples/s than single-thread
  precompute at 100k series without higher total CPU per sample beyond the
  combine and commit cost. If it does not, the default stays single-thread
  and the profile is reported.
- **Shard threads do the pipeline work.** Heavy views slow ingestion on
  every shard, and appends and pushes share a core. `precompute_push` CPU
  makes this visible.
- **Memory for coarse groupings is multiplied by K** (see "Memory
  accounting"). The Planner's cost model does not include this yet.
- **Finalize on shard threads.** Emitting 10k states is expected to take
  milliseconds. If profiling shows otherwise, emission can be split
  across shards in the same way.
- **KLL and heap-carrying sketches** stay within tolerance only.
- **This depends on the sharded ingestion design.** Its router, its global
  watermark and its per-shard queues must deliver Close events in the
  same order as samples. If that design changes, this one changes with it.

Open questions for a human decision:

1. The default for `--precompute-threads` (the combine pool). Shard
   threads, query workers and persist threads already use the cores.
   Proposed: a quarter of the cores, 1 to 4, as for persistence.
2. The per-instance memory limit: the full `--run-memory-limit-bytes`
   (proposed), or that limit divided by K so that the total matches the
   single-thread run.
3. Whether v1 should implement exchange as well as single partition. No
   current plan needs either.
4. The B1 target that makes parallel precompute the default (proposed:
   higher sustained samples/s at 100k series with K = 4 than single-thread
   precompute).
5. Whether the Planner's cost inputs should get the shard count and a
   "streams ingestion pipelines" flag now (a Planner PR), or after B1.
