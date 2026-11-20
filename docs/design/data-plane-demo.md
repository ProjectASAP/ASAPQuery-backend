# Design: data plane demo against VictoriaMetrics

## Audience and context

Developers and architects of the backend data plane, and whoever runs or
reviews the comparison. The data plane executes the physical ASAP DAG that
ASAPPlanner selected (`InstallablePlan`) with the Planner's executor
(`asap-executor`). Planning is offline: the plan is computed once before the
data plane starts; its time and memory are reported on their own line and
are never part of the data plane's resource usage.

## Problem and why now

We want to show, on the same input and the same queries, that the data plane
executing an ASAPPlanner plan answers planned PromQL queries with lower query
latency and lower total CPU and memory than a time-series database that
computes every answer from raw data at query time: VictoriaMetrics
single-node without stream aggregation.

The data plane before this change cannot show that:

- One engine thread ran everything in turn: Remote Write appends, the
  ingestion run of every closed refresh window, and every query. While a
  window's views were computed, Remote Write queued (and was refused when
  the queue was full) and queries waited.
- Raw samples were kept row by row (`Vec<RawSample>` per metric). Every read
  scanned all retained samples of the metric and rebuilt its Arrow columns,
  serializing each series' identity again.
- Its CPU could not be broken down by phase.

## Inputs, outputs, and end-to-end behavior

- Input: Prometheus Remote Write samples; the installed `InstallablePlan`.
- Output: answers to the plan's instant and range queries on the Prometheus
  query API; `GET /metrics` with the data plane's own resource and timing
  measurements; the materialized views on disk.
- Answers do not change: the executor reads the same rows as before.

Example: a workload with `quantile_over_time(0.99, m[1h])` and
`topk(10, sum by (job) (rate(m[1h])))`, each evaluated every 15 s over 10k
series. VictoriaMetrics reads one hour of raw samples per series for each
evaluation. The data plane reads the per-window views computed at ingestion
time and merges them, so a query's work does not grow with the window's
sample count.

## Goals and non-goals

Goals (demo, v1):

- Ingestion throughput is not limited by view computation.
- Queries run concurrently with ingestion and with each other.
- What is shared is the plan's decision, and the data plane executes it as
  given: a query-time `OneEvaluation` node is computed once per evaluation
  time, never once per query.
- Materialized views are computed only at ingestion time, when their window
  closes, and kept in the materialized view store for their retention.
- Operators run batch at a time on Arrow columns.
- Per-phase CPU is measurable (decode, append, precompute per refresh group,
  query per query) for profiling and benchmarks.
- Planned range queries (`/api/v1/query_range`) are answered with one
  evaluation per step, equal to the instant answers at the step times.
- Materialized views survive a restart: each refreshed window is appended
  to its view's file and synced; at startup the windows within retention
  are loaded back.
- A repeatable comparison with VictoriaMetrics, shown in Grafana: query
  latency, total CPU, memory, and a CPU breakdown, per component.

Non-goals (v1):

- Materializing at query time across evaluations (views refreshed on first
  read, query-time `AcrossEvaluations`): Stage 3 does not select it on the
  demo workloads, with or without raw data kept and whether or not the
  backend declares `query_time_results_retained`; revisit with
  ProjectASAP/ASAPPlanner#754. A plan that needs it is refused.
- Computing views in the background after ingestion.
- Streaming (per-sample, incremental) operators: a window's views are
  computed by one batch run when the window closes. Streaming means the
  generic push operators of ProjectASAP/ASAPPlanner#1421; the raw buffer
  bytes and the precompute CPU peaks measured here decide whether it is
  needed.
- Reusing DataFusion's operator implementations (ProjectASAP/ASAPPlanner#1421):
  it replaces the executor's whole operator layer and changes edge-case
  semantics, so it is separate, multi-week work.
- Persisting raw samples or open windows, checkpointing, and changing the
  shard count at run time. Late samples are handled as before: a sample
  older than every window still to be closed and than what a query-time
  `Scan` reads back is dropped (`asap_samples_total{outcome="dropped_late"}`);
  a later sample for a window already refreshed is kept for query-time
  `Scan`s but does not change the stored window.

## Framing questions

- **What are we trying to do?** Answer a fixed set of monitoring queries
  faster and with less CPU and memory by computing, at ingestion time, the
  small summaries the Planner chose, instead of rescanning raw data per query.
- **How is it done today?** Time-series databases keep raw samples and
  compute each query from them at query time; cost grows with the window's
  sample count and with the query rate. Stream aggregation and recording
  rules precompute, but each rule is written by hand, per query.
- **What is new?** The Planner chooses what to precompute, sketch or exact,
  and what to share across queries, under accuracy targets and a cost model;
  the data plane executes that plan as given.
- **Who cares?** Operators running many dashboards and alerts over
  high-cardinality metrics.
- **Risks and costs?** VictoriaMetrics ingests very efficiently, so total CPU
  can favor it when queries are few and cheap; sketches answer within an
  error target, not exactly; only planned queries are answered.
- **How long?** Three days (see the implementation plan).
- **Checks?** Midterm: parity tests pass and a smoke run on one workload
  produces every number in the report. Final: the full report and dashboard.

## Proposed design

```
Remote Write ─► HTTP threads: decode, intern series
                     ▼
                ingest shards (ingest-N, by series hash): append each
                request's samples of a pane as one immutable columnar chunk
                (time, value, series code), into the shard's own raw data
                     │ watermark passes the pane ⇒ chunks compacted into one
                     ▼
                ingest coordinator: watermark over the shards, closes
                windows over the union of the shards' raw data
                     ▼
                precompute thread: one ingestion run per closed refresh
                window over the panes it covers; publishes a new snapshot of
                the ingestion-time views (Arc swap)
                     ▼
        materialized view store
          ├─ ingestion-time views: immutable snapshots, kept for each view's
          │  retention; each window appended to its group's file and synced
          └─ query-time results: (OneEvaluation node, T) → rows, written by
             query workers, single flight, kept while T is within a margin
             of the watermark
                     ▲
HTTP queries ─► query workers: one query run per worker thread over
                (view snapshot + query-time results); a range query is one
                evaluation per step
```

### Ingestion

- Remote Write is decoded and its series interned on the HTTP threads,
  which send each series' samples to one of `--ingest-threads` ingest
  shards (`ingest-N`, by series id; default a quarter of the cores, at
  least 1), as VictoriaMetrics spreads ingestion over its cores. Each shard
  owns its raw data: it routes and appends its samples with no lock shared
  on the hot path. Each shard's queue is bounded (`--max-queued-samples`);
  a request is admitted whole or, if any shard it reaches is full, refused
  with 503. A series' identity and label text
  are computed once, when the series is interned; rows hold an index into
  their chunk's series.
- Raw samples are held per pane, whose width is the gcd of the refresh
  groups' window widths and phases and of 60 s (so eviction frees memory at
  least every minute). Each Remote Write request's samples of a pane become
  one immutable chunk, shared by `Arc`; once the watermark passes the pane,
  its chunks are compacted into one (a late sample adds a chunk, compacted
  again). Eviction drops whole panes once no window still to be closed and
  no query-time `Scan` (back to its `retention_ms`) reads them, and a read
  never returns a sample at or before the eviction cutoff, so a pane the
  cutoff falls in answers as the row store did.
- A read returns one batch per chunk of the panes overlapping its range;
  only a chunk straddling the range is filtered. A query-time `Scan` that
  reads the open pane reads the chunks published so far, the same way:
  every shard reports its raw data after every request (copy on write: the
  per-pane chunk lists are copied, never the chunks), and a read takes the
  union over the shards.
- An `ingest` coordinator thread keeps the watermark over the shards: it
  follows the newest sample any shard holds, and before it moves the
  coordinator flushes every shard (each runs the samples queued to it and
  reports), so a window closes over every sample admitted before it, from
  whichever shard. An idle shard answers the flush at once, so it never
  holds the watermark back; the coordinator also wakes every 100 ms. The
  allowed lateness is unchanged. (Taking the minimum over the shards with
  samples queued, without a flush, raced with requests spread over
  several shards and lost samples in the parity test.) Two rules make the
  flush exact. A request is sent to its shards whole between two flushes,
  never split by one (a read-write lock: requests share it, a flush takes
  it alone). And a shard that has answered the flush may already run later
  requests while slower shards still run earlier ones; the reports it
  sends after its answer are held for the next pass, so its newer samples
  move neither this pass's watermark nor the raw data its windows read.
  (Applying them at once let the watermark pass samples the slower shards
  had yet to append: windows closed without them, and sums and top-k came
  out low.) A window `(s, e]` closes only once event time is past `e`:
  the watermark from samples is one millisecond before the newest sample
  less the allowed lateness, since a scraper sends one timestamp over
  several requests and samples at `e` may still come (a barrier counts
  whole). Every pass that took a report or a barrier flushes before the
  watermark moves, and the wall clock the watermark is capped at (samples
  may be stamped ahead of it) is the time that flush was sent, not a
  shard's report time: a shard working through a backlog reports the
  current time while it still runs old requests, and taking that time let
  the watermark pass samples of requests admitted after the flush. A
  barrier that comes while a flush is waited for takes the next pass (a
  queue), behind its own flush. It closes each
  refresh window the watermark passes and hands it, with every shard's raw
  data as it was then (shared `Arc` chunk lists, no copy), to the
  precompute thread,
  which runs `execute_ingestion` and publishes the views' rows as a new
  immutable snapshot. A refresh therefore reads exactly the samples the
  serial engine read. Inside a run, a node feeding several views runs once
  and each batch reaches every view (one producer, many consumers).
- Borrowed from the ASAPQuery precompute engine
  (`asap-query-engine/src/precompute_engine/`): routing by series hash to
  one worker per shard; a watermark that is the newest applied sample less
  the allowed lateness, combined across workers so that time moves only
  as far as every worker with samples has applied, and workers with no
  samples do not hold it back; a periodic tick so windows close while
  input is idle; closing `(s, e]` only once event time is past `e`
  (`window_manager.rs`); dropping and counting late samples. Its tests for
  the cross-worker watermark, workers not yet started, window-end
  boundaries and late data are ported, at the engine level over one and
  eight shards, to `data_plane/tests/ingest_shards.rs`. ASAPQuery closes
  each worker's own partial windows from the shared minimum; here one
  coordinator closes a window over every shard's raw data, so the
  minimum is reached by a flush: every shard runs what was admitted
  before it, and an idle shard answers at once.
- One precompute thread refreshes the windows in this design; computing
  windows in parallel is the next step (a separate, stacked change).
- At most `--max-precompute-backlog` (default 256) windows wait for their
  refresh; when the backlog is full the coordinator waits, the shards'
  queues fill up to `--max-queued-samples`, and Remote Write gets 503 so
  senders retry. The backlog is exported: a growing backlog means the data plane
  cannot keep up with the input rate.

### Queries

- Pull model. A fixed set of worker threads (`query-N`, `--query-threads`,
  default one per core); each runs one query run to completion
  (`block_on`). Runs are not `Send` (the executor's runtime uses
  `Rc`), and that is not needed: what crosses threads (compiled plans, store
  snapshots, output batches, sketch states) is `Send + Sync`, checked at
  compile time.
- Concurrency is bounded by the worker count; further requests queue. Each
  run has its own memory limit, so runs in progress hold at most workers ×
  run limit. On top of that come the queued requests (unbounded), the
  query-time results (up to `--query-time-results-max-bytes`), and a range
  query's step answers until its matrix is sent; peak query memory is the
  sum, not the run limit alone.
- A panic in a run answers that query with an error and the worker keeps
  serving; it is logged.
- Each query takes the latest view snapshot when it starts, so two queries at
  the same evaluation time may read different snapshots.

### What is shared: the plan decides

Each query-time node's `Lifetime` in the plan says how it runs, and Stage 3
charges it accordingly:

| Planned `Lifetime` | Runs | Data plane |
| --- | --- | --- |
| `None` | in each run that reads it; one consumer | computed per run |
| `OneEvaluation` | once per evaluation time; its consumers share it | query-time results partition, keyed by (node, T) |
| `AcrossEvaluations` | kept across evaluations (materialized view) | ingestion-time views; query-time ones are refused in v1 |

Within one query run the executor already computes a `OneEvaluation` node
once. Across runs, a `OneEvaluation` node reached from more than one
query's run is cut out of them as at a view: its sub-DAG is compiled on its
own, and each query reading it binds it as a view slot (an on-first-read
`MaterializedView` descriptor, so the executor is unchanged) read through
`read_materialized_view`. The first query needing (node, T) computes it;
queries needing it meanwhile wait for that result (single flight); later
ones read it. A failed computation reaches the queries that waited for it
and is not kept. A run holds the results it reads, so dropping one cannot
fail a run. A result is kept only for a time T the watermark has passed,
whose inputs are complete; at a later T (for example a client's
far-future `time=`) each run computes the node itself. Results are dropped
once T is more than `--query-time-results-retention-ms` (default 60 s)
behind the watermark, or, oldest first, while more than
`--query-time-results-max-bytes` (default 1 GiB) are held; an entry being
computed is never dropped. Install fails if
the plan and the runtime disagree: any other node (for example a computed
`None` node) that several queries' runs would compute. `--no-shared-results`
computes the node in every query's run instead, for comparison. Stage 3
charges a `OneEvaluation` node once per evaluation; this is what makes the
runtime match it.

The query-time results partition is also where views refreshed on first read
will live, with a longer retention, when they are supported.

### Examples: executing PromQL plans of any shape

The engines never match plan shapes. They execute whatever DAG the plan
contains, by these rules:

| Plan element | Precompute engine | Query engine |
| --- | --- | --- |
| Ingestion-time view (`AcrossEvaluations` at ingestion) | grouped by the plan with the views of the same window width W and phase p (a refresh group, `ingestion_refresh_groups`); one `execute_ingestion` run per closed window `(e − W, e]`; when one watermark advance closes windows of several groups, they run in plan order, so a group runs after the groups whose views it reads | a slot: its stored windows that tile the run's `TimeRange` |
| Ingestion-time `None` node (e.g. the `Scan` under a view) | computed inside each refresh group's run that reaches it (a `Scan` under two groups is read by each group's run); within one run, once for all its consumers | — |
| Query-time `OneEvaluation` node read by several queries' runs | — | cut out like a view: computed once per evaluation time T, single flight, then a slot of each run that reads it |
| Query-time `OneEvaluation` node with several consumers inside one run | — | computed once inside that run, held until its consumers finish (the executor's rule) |
| Query-time `None` node | — | computed inside the run that reads it (one consumer) |
| Query-time `Scan` (raw data retained) | keeps the raw panes back to the scan's retention | a raw-data slot over the panes |

Each query root is compiled once at install, with views and shared nodes as
slots. An instant query at T runs its compiled root once; a range query runs
it once per step, at most one step per query worker at a time. The plans
below come from `asap_planner --run-until stage3` at ASAPPlanner `1b18ad92`
(workload files are ASAPPlanner's sketch plan snapshot workloads, ids in
parentheses). Each listing shows plan node
`n<k>`, its timing and lifetime, the operator, and the nodes it reads.

#### 1. One query, a diamond inside it (`ratio-of-quantiles-1h-mvp`)

`quantile_over_time(0.9, data[1h]) / quantile_over_time(0.5, data[1h])`,
every 60 s, raw data not retained.

```
n0  Ingestion None                Scan
n1  Ingestion AcrossEvaluations   SummaryTumblingWindowChain DDSketch W=60s α=0.02  <- n0
n2  Query     None                TimeRange 1h                                       <- n1
n3  Query     OneEvaluation       SummaryMerge                                       <- n2
n4  Query     None                SummaryEstimate q=0.5                              <- n3
n5  Query     None                Project                                            <- n4
n6  Query     None                SummaryEstimate q=0.9                              <- n3
n7  Query     None                Project                                            <- n6
n8  Query     None                BinaryOp                                           <- n7, n5   (root)
```

- Precompute: one refresh group {n1} (W = 60 s). Each closed minute runs
  n0 → n1 once and appends one window of DDSketches per series.
- Query: one compiled root (n8) with n1 as a view slot. In each run, n3
  merges the 60 windows once and both estimates read it (two consumers in one
  run), then the division joins the two sides.

#### 2. Two queries share a query-time node (`shared-quantiles-1h-mvp`)

`quantile_over_time(0.5, data[1h])` and `quantile_over_time(0.9, data[1h])`,
both every 60 s.

```
n0  Ingestion None                Scan
n1  Ingestion AcrossEvaluations   SummaryTumblingWindowChain DDSketch W=60s α=0.05  <- n0
n2  Query     None                TimeRange 1h                                       <- n1
n3  Query     OneEvaluation       SummaryMerge                                       <- n2
n4  Query     None                SummaryEstimate q=0.9                              <- n3
n5  Query     None                Project                                            <- n4   (root, q2)
n6  Query     None                SummaryEstimate q=0.5                              <- n3
n7  Query     None                Project                                            <- n6   (root, q1)
```

- Precompute: as in example 1, one view feeds both queries.
- Query: n3 is `OneEvaluation` and is read by two queries' runs, so it is cut
  out. At T, whichever query arrives first runs n2 → n3 and stores the merged
  sketches under (n3, T); the other waits for that result or reads it. Then
  q1 runs slot(n3) → n6 → n7 and q2 runs slot(n3) → n4 → n5, on two worker
  threads. The 60-window merge happens once per T, as Stage 3 charged it.

#### 3. Different window widths and intervals (`chains-10m-5m-3m-10s-mvp`)

`quantile_over_time(0.5, data[10m])` and `quantile_over_time(0.9, data[5m])`
every 5 min; `quantile_over_time(0.99, data[3m])` every 10 s.

```
n0   Ingestion None               Scan
n1   Ingestion AcrossEvaluations  SummaryTumblingWindowChain KLL W=300s k=1404  <- n0
n2   Query     None               TimeRange 5m                                   <- n1
n3   Query     OneEvaluation      SummaryMerge                                   <- n2
n4   Query     None               SummaryEstimate q=0.9                          <- n3
n5   Query     None               Project                                        <- n4   (root)
n6   Query     None               TimeRange 10m                                  <- n1
n7   Query     OneEvaluation      SummaryMerge                                   <- n6
n8   Query     None               SummaryEstimate q=0.5                          <- n7
n9   Query     None               Project                                        <- n8   (root)
n10  Ingestion AcrossEvaluations  SummaryTumblingWindowChain KLL W=10s k=1404   <- n0
n11  Query     None               TimeRange 3m                                   <- n10
n12  Query     OneEvaluation      SummaryMerge                                   <- n11
n13  Query     None               SummaryEstimate q=0.99                         <- n12
n14  Query     None               Project                                        <- n13  (root)
```

- Precompute: two refresh groups, {n1} with W = 300 s and {n10} with
  W = 10 s. Each runs on its own windows; the `Scan` n0 under both runs in
  each group's run, so the raw rows of a 10 s window are read again by the
  300 s window that contains it. (Stage 3 charges n0 once; the second read
  is a known cost-model mismatch, small because a scan is cheap.)
- Query: view n1 is read by two roots with different ranges (5 m reads one
  window, 10 m reads two); n3 and n7 each have one reader, so they are not
  cut. The 10 s query reads 18 windows of n10.

#### 4. Four dashboard queries over two metrics (dashboard workload)

`quantile_over_time(0.99, lat[1h])`, `max_over_time(lat[30m])`,
`topk(10, sum by (job) (rate(reqs[1h])))`,
`sum by (job) (increase(reqs[1h]))`, every 15 s; 10k series per metric,
1 sample/s; raw data not retained.

```
n0   Ingestion None               Scan lat
n1   Ingestion AcrossEvaluations  SummaryTumblingWindowChain KLL W=15s  <- n0
n2   Query     None               TimeRange 1h                    <- n1
n3   Query     OneEvaluation      SummaryMerge                    <- n2
n4   Query     None               SummaryEstimate q=0.99          <- n3
n5   Query     None               Project                         <- n4   (root)
n6   Ingestion AcrossEvaluations  SummaryTumblingWindowChain Max W=15s       <- n0
n7   Query     None               TimeRange 30m                   <- n6
n8   Query     OneEvaluation      SummaryMerge                    <- n7
n9   Query     None               FinalizeExactAccumulator        <- n8   (root)
n10  Ingestion None               Scan reqs
n11  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Rate W=15s      <- n10
n12  Query     None               TimeRange 1h                    <- n11
n13  Query     OneEvaluation      SummaryMerge                    <- n12
n14  Query     None               FinalizeExactAccumulator        <- n13
n15  Query     OneEvaluation      Aggregate sum by (job)          <- n14
n16  Query     OneEvaluation      Aggregate topk(10)              <- n15  (root)
n17  Ingestion AcrossEvaluations  SummaryTumblingWindowChain Increase W=15s  <- n10
n18  Query     None               TimeRange 1h                    <- n17
n19  Query     OneEvaluation      SummaryMerge                    <- n18
n20  Query     None               FinalizeExactAccumulator        <- n19
n21  Query     OneEvaluation      Aggregate sum by (job)          <- n20  (root)
```

- Precompute: all four views have W = 15 s and phase 0, so they form one
  refresh group. Each closed 15 s window runs one `execute_ingestion`: n0
  feeds both the DDSketch and the Max chain, n10 feeds both the Rate and the
  Increase chain (one producer, several consumers, each scan read once).
- Query: four roots, each over its own view; no node is read by two
  queries, so nothing is cut. (At ASAPPlanner `1b18ad92` with default cost
  inputs the plan's quantile chain is a KLL sketch; KLL seeds its
  compaction coin at random, so the parity test compares this plan's
  answers within 5%.) The quantile root merges 240 windows per
  series per evaluation (see "Hierarchical rollups").

#### 5. Query-time only, raw data retained (`topk-by-job-and-global-raw-retained`)

`topk by (job) (5, data)` and `topk(10, data)`, every 60 s.

```
n0  Query None           Scan
n1  Query None           TimeRange 15s  <- n0
n2  Query None           Sort           <- n1
n3  Query None           Limit          <- n2   (root)
n4  Query None           Scan
n5  Query None           TimeRange 15s  <- n4
n6  Query OneEvaluation  Sort           <- n5
n7  Query None           Limit          <- n6   (root)
```

- Precompute: no views. The ingest thread only keeps the raw panes that the
  query-time scans read (their retention).
- Query: each root reads raw data through its own `Scan` slot, sorts and
  limits; the plan gives each query its own scan, so each run reads the
  panes itself.

#### 6. Two label granularities, raw data retained (`rollup-quantile-nested-labels-raw-retained`)

`quantile by (job) (0.9, data)` and `quantile by (job, host) (0.9, data)`,
every 60 s.

```
n0  Query None           Scan
n1  Query None           TimeRange 15s               <- n0
n2  Query OneEvaluation  Aggregate quantile(q=0.9)   <- n1   (root)
n3  Query None           Scan
n4  Query None           TimeRange 15s               <- n3
n5  Query OneEvaluation  Aggregate quantile(q=0.9)   <- n4   (root)
```

- Precompute: no views; raw panes kept for the scans.
- Query: two independent runs, each an exact grouped quantile over the last
  15 s of raw samples (Stage 3 chose exact over a shared sketch here).

#### Range queries in these examples

A planned range query with `range_evaluation {step, span}` runs its compiled
root once per step time T in `[start, end]` (`start`, `start + step`, … up
to `end`), on the worker threads; at each T, the cut-out `OneEvaluation`
nodes of example 2 are shared with every other run at the same T (once the
watermark has passed T; before that each run computes them).

Every example is executed by the parity test against the Planner executor on
the same plan and samples, with raw data kept and not kept
(`data_plane/tests/workloads/`: `sketch-plan-ratio-of-quantiles-1h.json`,
`rangeheavy-t5-q05-q09-1h.json`, `sketch-plan-chains-10m-5m-3m-10s.json`,
`dashboard.json`, `sketch-plan-topk-by-job-and-global.json`,
`sketch-plan-rollup-quantile-nested-labels.json`).

### Range queries

A range request for a planned query (`start`, `end`, `step` as Prometheus
takes them; refused, as Prometheus does, when `(end − start) / step`
exceeds 11,000) is evaluated at every step time, at most one step per
query worker in flight, so a long range neither floods the queue nor
starves instant queries, and answered as a Prometheus matrix. The first
step that is not answered, or the client going away, drops the remaining
steps unrun; the query is then not answered (forwarded with the reason, or
refused under `--no-forward`). Steps are measured as
`phase="query_range_step"` and the request as
`asap_range_query_latency_seconds`, apart from instant queries. Each step is an ordinary
evaluation at its time, so `OneEvaluation` sharing applies per step. The
workload's `range_evaluation` (`step_ms`, `span_ms`) tells the Planner the
requests' shape, so the plan delivers every step time.

### Result cache

A Grafana range panel refreshed every 15 s asks for the same range shifted
by one step; without a cache every step is computed again. VictoriaMetrics
keeps a rollup result cache for this; the data plane keeps one too
(`data_plane/src/result_cache.rs`).

- Key and value: (planned query root, evaluation time T) → the answer
  vector (`Arc`). An instant query at T and a range step at T share the
  entry. Not aligned start/end are still correct, with fewer hits.
- Only final answers are kept (`QueryEngine::evaluate`): the run succeeded,
  so every view window it read was stored (a window is appended once, when
  the watermark closes it, and never changes; a missing one fails the run);
  and, when the root reaches a `Scan` without passing a view (it reads raw
  data at query time), T ≤ W, where W is the watermark the run's snapshot
  was published with: max(min(newest sample, wall clock) − allowed
  lateness, newest barrier). An answer at T > W is computed and returned,
  not kept, so the next request computes it again. W is read before the
  snapshot (raw data is published before the watermark that covers it), so
  the snapshot holds every sample at or before W; this also fixes the same
  race for the shared query-time results. A sample later than the allowed
  lateness may still be appended to raw data and is not reflected in a kept
  answer, as it is not in a closed view window. This rule replaces
  VictoriaMetrics' fixed 5-minute `-search.cacheTimestampOffset` with what
  the data plane knows.
- A range request looks up every step and sends only the missing ones
  through the bounded worker fan-out (and `OneEvaluation` single flight).
- Eviction: an answer whose T is more than the plan's longest retention
  (the largest `retention_ms` of any view or `Scan`, which counts back from
  the newest evaluation time) plus `--view-retention-margin-ms` behind the
  watermark is dropped (no planned evaluation reaches it; none when a `Scan`
  is unbounded); beyond `--result-cache-bytes` (default 256 MiB, estimated
  from labels and samples) the least recently used go. `--no-result-cache`
  turns it off.
- Metrics: `asap_result_cache_{hits,misses,evictions}_total`,
  `asap_result_cache_{entries,bytes}`, and per query
  `asap_range_query_steps_total` and `asap_range_query_steps_cached_total`.
- The Planner's cost model does not know about the cache: it charges every
  evaluation of a range. Follow-up: charge a refreshed range only its new
  steps.

Every step of one range request also reads the same snapshot (raw shards,
views, watermark), taken when the request starts. A raw chunk a step reads
whole is converted to a batch once per request and shared by the other
steps (`RawReads`, sharded by chunk); a view read finds the windows tiling
its range by binary search. View windows were already `Arc`ed batches read
without copying, so this saves little: the cold cost is the per-step merge
of window states, which incremental cross-step aggregation would address
(not done).

Evidence (`examples/range_bench`, dashboard workload, 1,000 series per
metric, a sample every 15 s, four panels each a 1 h range at 15 s steps,
964 steps; 56-core x86 server, three runs each):

| Request | Result cache on | off |
| --- | --- | --- |
| first (cold), views plan | 5.0–5.1 s wall, 176–187 s CPU | 5.1 s, 177–180 s CPU |
| 15 s later, views plan | 1.2–1.6 s wall, 1.2–1.6 s CPU (960/964 steps cached) | 4.8–5.1 s, 172–175 s CPU |
| first (cold), raw plan | 4.7–4.9 s wall, 163–176 s CPU | 4.9–5.1 s, 165–180 s CPU |
| 15 s later, raw plan | 1.1–1.3 s wall, 1.1–1.3 s CPU (960/964 steps cached) | 4.5–4.9 s, 152–172 s CPU |

- Of the refresh, the four new steps of `quantile_over_time(0.99,
  lat[1h])` take about 0.3 s; most of the rest is encoding the two
  1,000-series matrices as JSON (a fully cached request still takes
  0.8–1.1 s, 0.4–0.5 s per 1,000-series panel, 8–10 ms per small panel).
  Follow-up: encode the matrix without building `serde_json::Value`s.
- Cold profile (views plan): `DdSketch::merge_store` 20 %, `memmove` 18 %,
  the executor's summary merge 5.5 %; the store's reads are not among the
  top symbols. Reading one snapshot per request: cold CPU 186–195 s →
  174–195 s (views plan) and 170–181 s → 162–176 s (raw plan), four runs
  each, within run-to-run noise on wall time. A first version guarding the
  converted chunks with one mutex took 7.0 s and 230–262 s CPU (39 % in
  the kernel's spin lock), so they are sharded.

### Persisting views

Each refresh group has one append-only file in `--view-dir` (default
`asap-views`; `--no-view-persistence` keeps views in memory only): a header
naming the plan (a hash of the plan file) and the group, then one record
per refreshed window, a length and a MessagePack payload holding
`(start, end]` and the rows of each view appended. A text (a label value or
a series identity) is stored once per file: a record carries the texts it
adds to the file's dictionary, and rows hold indices. Persisting runs in
the background: the precompute thread publishes a refreshed window first
and then queues its rows for a pool of `--persist-threads` threads
(`persist-N`; default a quarter of the cores, 1 to 4), which encode, write
and sync. Refresh group `g` goes to thread `g % n`, so each file has
exactly one writer and its records stay in window order. The queue is
bounded by bytes (`--max-persist-backlog-bytes`, default 256 MiB;
`asap_persist_backlog_bytes`, `asap_persist_backlog_windows`); only when
it is full does the precompute thread wait, and the wait is exported
(`asap_view_persist_wait_seconds_total`). An event-time barrier answers
once the windows before it are written, without the precompute thread
waiting. Each persist thread syncs its files (`fdatasync`) every
`--view-sync-interval-ms` on its own timer, also when no window arrives,
and writes and syncs everything queued when the engine stops;
the default, 5 s, is VictoriaMetrics' own default
`-inmemoryDataFlushInterval` (v1.155.0: "The interval for guaranteed saving
of in-memory data to disk ... (default 5s)"). A refreshed window is
therefore durable within that interval, the same class of guarantee
VictoriaMetrics gives: after a crash, the windows of the last interval may
be lost and are left missing (their raw samples are not persisted). 0
syncs after every window. A file of another plan or
group is renamed `*.stale` and never loaded. At startup the records within
each view's retention of its newest one are loaded back, oldest first; a
torn or oversized record at a file's end (a crash while writing) is cut
off, so later records stay aligned. A view that already stores a window is
not appended to again, while the group's other views are. Raw samples are
not persisted, so after a restart that loaded views, windows whose samples
arrived before the restart stay missing: a declared stream start no longer
makes the first window complete.

Plain values and exact states (sum, count, min, max, rate, increase, which
the executor serializes) are stored by their serde form. Sketch states are
stored as `asap_sketchlib`'s ASAPv1 bytes with the kernel they rebuild,
read and rebuilt through every executor sketch kernel's one interface,
`sketch()` and `restore_from_disk(...)`: DDSketch, KLL, Count-Min, KMV,
HyperLogLog (with its precision), weighted frequency (Count-Min or Count
Sketch, with or without a heap), UnivMon (with its key-byte counters),
Hydra-KLL and Hydra-Count-Min, whose grid is shared by every group of a
build and so is stored once per record and rebuilt once, shared again by
the rows. A UnivMon whose heaps mix key types has no ASAPv1 encoding
(`asap_sketchlib` #265); its (view, window) pairs are counted in
`asap_view_windows_not_persisted_total` and the refresh is unaffected.
UnivMon merges break heap ties in a nondeterministic order (#266), so a
merge after reload may differ slightly from one before. Files are not compacted while running; records past
retention are skipped at load.

### Measurements

- Threads are named by role: `ingest-N` (shards), `ingest` (coordinator),
  `precompute`, `persist-N`,
  `query-N`; Remote Write is decoded on the HTTP runtime's threads.
- `GET /metrics` exports, in the Prometheus text format:
  - `asap_phase_cpu_seconds_total`, `asap_phase_wall_seconds_total` and
    `asap_phase_runs_total` by `phase`: `remote_write_decode`, `append`,
    `precompute` (per refresh group: `group`, `window_ms`, `views`) and
    `query` (per `query` text). Thread CPU time comes from
    `CLOCK_THREAD_CPUTIME_ID`. A shared query-time result is counted in the
    `query` phase of the query that computed it.
  - `asap_query_latency_seconds` (histogram per query, queueing for a
    worker included); `asap_samples_total` and `asap_windows_total` by
    outcome; `asap_samples_queued`, `asap_precompute_backlog_windows`,
    `asap_watermark_lag_seconds`.
  - Bytes held: `asap_raw_data_bytes` (with `asap_raw_data_samples`,
    `asap_raw_data_panes`), `asap_view_bytes`,
    `asap_query_time_result_bytes`; `process_resident_memory_bytes`.
  - Query-time results: `asap_query_time_results`,
    `asap_query_time_result_computes_total` (misses),
    `asap_query_time_result_hits_total`.
  - Result cache ([Result cache](#result-cache)):
    `asap_result_cache_{hits,misses,evictions}_total`,
    `asap_result_cache_{entries,bytes}`, and per query
    `asap_range_query_steps_total` and
    `asap_range_query_steps_cached_total`.
  - Persistence: `asap_view_persisted_bytes_total`,
    `asap_view_sync_seconds_total`, and (view, window) pairs not persisted
    (`asap_view_windows_not_persisted_total`) or loaded at startup
    (`asap_view_windows_loaded_total`).
  - `asap_thread_cpu_seconds_total` by thread name (trailing digits
    removed), from `/proc`, for the threads the phases do not cover.
- `scripts/profile.sh` records a CPU profile (`perf record`) of a running
  data plane and writes a flame graph.

## Evaluation

### Arms

| Arm | What runs | Notes |
| --- | --- | --- |
| ASAP | `data_plane --no-forward` executing the Planner's plan | Every query is planned; nothing is forwarded, so no Prometheus runs beside it. Planning time and memory are reported once, separately |
| VictoriaMetrics | single-node, pinned release, no `-streamAggr.config`, default caches on | Flags recorded in the report |
| Prometheus 3.5 | exact reference | Accuracy only. MetricsQL `rate`/`increase` do not extrapolate, so VictoriaMetrics' difference from Prometheus is recorded but not counted as error |

### Input and queries

- The same paced feeder sends the same samples by Remote Write to each arm,
  in real time.
- Workloads: the mixed PromQL workloads `random-mixed-seed-862` 01–03, and a
  dashboard-heavy workload with long windows and frequent evaluation
  (`quantile_over_time(0.99, m[1h])`, `topk(10, sum by (l) (rate(m[1h])))`,
  `max_over_time(m[30m])`, `sum by (l) (increase(m[1h]))`). Each query must be
  plannable with raw data not retained.
- Scale: 1k and 10k series, then larger while the ASAP backlog stays bounded
  (that limit is itself a result).
- Each query is sent at every evaluation time of its interval, to every arm,
  at the same offset after the evaluation time (the allowed lateness).
- Range queries, a separate group: dashboard panels over the last hour at
  1-minute steps (`range_evaluation` `{"step_ms": 60000, "span_ms":
  3600000}`), sent to both arms' `query_range` at each refresh; latency,
  CPU and accuracy reported apart from the instant queries.

### Measurements

- Query latency: per query, over time, from the client.
- CPU: core-seconds per component, split into ingestion and query phases.
- Memory: peak and steady RSS per component.
- CPU breakdown: ASAP per phase from `/metrics` plus a flame graph;
  VictoriaMetrics from its Go CPU profile (`/debug/pprof/profile`) and its
  own metrics.
- Accuracy: per query, against the Prometheus answer, with the plan's target.
- All numbers are absolute, one row per component; no ratios only.

### Placement

One host. Each arm gets its own cores (cpusets) and the same core count; the
feeder, client and monitoring run on other cores. Arms run one after another
on the same feed, so they never compete for memory bandwidth.

### Dashboard (Grafana)

A monitoring Prometheus scrapes node and per-process metrics, both arms'
`/metrics` and the client's latencies. One dashboard, top to bottom: a
diagram of the arms and workload; CPU per component as bars (ingestion,
query); memory per component as bars (peak, steady); query latency over time
per query (log scale); input rate; CPU breakdown per phase; accuracy and
answers table. No percentile panels and no CPU-over-time lines.

## Implementation plan

| Day | Data plane | Evaluation |
| --- | --- | --- |
| 1 | Re-pin to Planner main; query workers; ingest/precompute split with columnar panes; `/metrics` CPU breakdown; parity tests | VictoriaMetrics arm in the harness; monitoring and dashboard; deploy; smoke run on one workload at 1k series |
| 2 | Shared `OneEvaluation` nodes once per evaluation (query-time results partition); profile and fix the largest hot spots | Dashboard-heavy workload; runs at 1k and 10k series; find the ingest limit |
| 3 | Range queries (one evaluation per step); view persistence; parity for both | Range-query group in the harness; restart run to check persisted views |
| 4–4.5 | Fixes from the runs only | Full runs, three rounds each; report table; dashboard screenshots |

## Minimal complexity

- Two threads and a worker pool replace one thread. The new public
  surface is what the demo needs: `/metrics`, planned `query_range`, the
  view files, and the flags that bound queues, workers and caches.
- Panes are the smallest unit every refresh window tiles, so one buffer
  serves all refresh groups.
- Query-time results reuse the materialized view store and its read path
  instead of a separate cache; the only new mechanism is single flight.
- Snapshots are immutable `Arc`s; no lock is held during a run.

## Alternatives and decisions

| Question | Decision | Rejected |
| --- | --- | --- |
| Parallelism in v1 | ingest, precompute and query workers on separate threads | Morsel-driven parallelism (DuckDB): every stateful operator needs thread-local and global state and a combine step |
| Query workers | fixed threads, one run each | `tokio::spawn`: needs `Send` runs (executor runtime rewrite); runs are CPU-bound over an in-memory store, so async gains nothing, work stealing migrates runs across cores, and long runs starve the HTTP runtime. Revisit when the store does async remote I/O or the query engine moves to DataFusion's runtime |
| Shared query-time nodes | computed once per evaluation, on demand, single flight, in the view store's query-time partition (the plan's `OneEvaluation`) | (A) collect same-time queries into one multi-root run: adds a wait, one thread; (C) evaluate planned queries on their schedule and serve stored answers: request latency is then not comparable with VictoriaMetrics |
| Query engine model | pull | Push per evaluation time: sharing by fan-out, but needs the due queries known in advance (they are: planned queries and their intervals), runs one thread per tick unless the runtime gets morsel scheduling, and answers only grid times. It is the long-term model for scheduled dashboard and alert workloads (a per-tick scheduler with a cache) |
| Who decides sharing | the plan (`Lifetime`) | the data plane discovering nodes reached from several queries |
| Raw sample layout | columnar per pane | rows converted to columns on every read |
| Open pane for query-time `Scan`s | each request's samples are an immutable chunk, published after every request; reads take the chunks so far | one mutable Arrow builder per pane copied into a batch on every read |
| View persistence | append-only file per refresh group, one record per window, texts in a per-file dictionary, synced on an interval (5 s, as VictoriaMetrics), tied to the plan, loaded within retention | a sync per window (on the test host, 0.2–0.5 s per sync capped refreshes at a few windows per second); a file per view; label text per row; a checkpoint of the whole store; persisting raw samples |
| When views are computed | when their window closes | query time; background |
| Operators | the Planner executor's | DataFusion's now (ASAPPlanner#1421) |
| Should a view be bounded or unbounded? | stored views are bounded: one immutable instance per (definition, window). How a window is computed (one batch run when it closes, or incremental streaming updates) is a separate choice, and freshness for a query at "now" is a third one (reading the open window's partial state, flagged as partial), not in the demo | one unbounded, continuously updated view per definition |

### Target design beyond the demo

- **Sharded ingestion.** Route samples by series hash to one ingest shard per
  core, sharing nothing. Mergeable views (sum, count, sketches) keep one
  partial state per (view, window, shard), merged at query time, so
  ingestion needs no cross-thread barrier. Views that are not mergeable
  (exact distinct, exact quantile, joins) need all rows of a group on one
  shard: compute them on one shard first and add a repartition step only if
  it becomes the bottleneck. A window is readable once every shard wrote it;
  the global watermark is the minimum over shards, and the router broadcasts
  time so an idle shard still closes its windows.
- **Cost model.** Per-shard partial states multiply the memory and writes of
  low-cardinality views by up to the shard count, and buffering raw samples
  until a window closes costs about input rate × longest window × bytes per
  sample. The Planner's cost inputs must include both.
- **Vectorized sketch updates.** Batch updates per group
  (`update_batch(values, group_ids)`), the shape of DataFusion's
  `GroupsAccumulator`, whether or not DataFusion's code is reused.

### Hierarchical rollups (after the demo)

Today a chain has one width, the gcd of the evaluation interval, the
window and the offsets (Pass 2 `add_chain_groups`,
`stage1-pass2-reuse-candidates/src/rules/chains.rs`). So
`quantile_over_time(0.99, lat[1h])` every 15 s over 10k series merges
3600 / 15 = 240 DDSketches per series per evaluation (2.4M). At 1 Hz each
holds 15 samples, so merging is no cheaper than scanning the raw rows
(ProjectASAP/ASAPPlanner#1315 measured 3.5x at 1 Hz and 10 s windows, against
145x at 10 Hz and 60 s windows).

Design:

- The plan decides the levels, and Stage 3 costs them.
- A coarse chain is a view whose input is the fine chain's view, not raw
  data. It is computed at ingestion time on the precompute thread when its
  coarse window closes, in a refresh group ordered after the fine one, so
  it needs no raw data and no background recompute.
- A query's `TimeRange` tiles `(t − 1h, t]` from several chains: fine
  windows at the unaligned edges, coarse windows in the middle.
- Each level is its own view definition, keyed by width, so the store
  needs nothing new.

Numbers for the example: 15 s alone, 240 merges per series; 15 s + 5 min,
at most about 49 (about 31 on average, 5–8x fewer); 15 s / 1 min / 5 min /
15 min, about 10–15. The 5-minute level costs about 10k × 20 merges per
5 minutes at ingestion, about 670 merges/s, and about 1/20 more storage.
The fine level is still kept for the whole window (for the unaligned head).
Mergeable sketches and exact accumulators keep their guarantees.

Work: Planner Pass 2 candidates 1–2 days; plan representation and Stage 3
cost 1–2 days; executor tiling from several slots about 1 day; data plane
refresh ordering about 0.5 day. The demo reports the 1 Hz / 15 s case as
this known limit and also runs workloads with more samples per window.

## Quality attributes and evidence

- Ingest throughput (samples/s) and per-phase CPU before and after this
  change on the same input (PR body).
- Query latency while windows are being computed.
- Peak and steady RSS; bytes held by raw panes, views and query-time results.

## Acceptance and test design

- The planner-executor parity test answers every query as before, with
  `OneEvaluation` sharing on.
- A query is answered while a precompute run is in progress.
- Two queries sharing a `OneEvaluation` node, run concurrently: the node is
  computed once per evaluation time and both answers equal those of runs
  that compute it themselves.
- A plan whose node two queries' runs would compute is refused unless the
  plan marks it query-time `OneEvaluation`; a plan with a view refreshed on
  first read is refused at install.
- Panes are evicted once nothing reads them.
- A columnar read returns the same rows as the row read on a fixture.
- A planned range query's value at each step equals the instant answer at
  that time; in the parity harness, mixed workloads with `range_evaluation`
  equal the Planner executor's query run at every step.
- After a restart over the same view files, a query is answered as before
  without any sample written again.

## Risks, rollout, and exit criteria

- VictoriaMetrics may use less ingestion CPU; the report shows every
  component's numbers, whichever arm they favor.
- If precompute cannot keep up, the backlog grows and Remote Write is pushed
  back; the backlog metric shows where the limit is.
- Query-time `Scan`s that read the open pane read every chunk published so
  far; an open pane holds one chunk per Remote Write request until the
  watermark passes it, so many small requests mean many small batches.
- A UnivMon view whose heaps mix key types is not persisted.
- Follow-ups in the Planner executor (a few percent of query CPU in the
  profile): DDSketch merge clones then merges (`summary::execute_merge`);
  group tables are not pre-sized.
- Sketch answers are within the plan's error target, not exact; only planned
  queries are answered.
- Open: sharded ingestion and its cost-model terms; batch sketch updates.
- Follow-ups: the backend builds `MaterializedView` values through their
  serde JSON form (the retention margin, and the on-first-read slot of a
  shared query-time node); both belong in executor APIs. The ingest thread's
  per-request bookkeeping (publishing the raw data, compaction, eviction)
  is linear in the panes retained; it changes only if profiling shows it.
