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
  shard count at run time. Late samples are dropped, as before.

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
                ingest thread: append each request's samples of a pane as
                one immutable columnar chunk (time, value, series code)
                     │ watermark passes the pane ⇒ chunks compacted into one
                     ▼
                precompute thread: one ingestion run per closed refresh
                window over the panes it covers; publishes a new snapshot of
                the ingestion-time views (Arc swap)
                     ▼
        materialized view store
          ├─ ingestion-time views: immutable snapshots, retention from the plan,
          │  each window appended to its view's file and synced
          └─ query-time results: (OneEvaluation node, T) → rows, written by
             query workers, single flight, dropped once T is past
                     ▲
HTTP queries ─► query workers: one query run per worker thread over
                (view snapshot + query-time results); a range query is one
                evaluation per step
```

### Ingestion

- Remote Write is decoded and its series interned on the HTTP threads; the
  ingest thread only routes and appends. A series' identity and label text
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
  reads the open pane reads the chunks published so far, the same way: the
  ingest thread publishes the raw data after every request (copy on write:
  the per-pane chunk lists are copied, never the chunks).
- The ingest thread closes each refresh window the watermark passes and
  hands it, with the raw data as it was then, to the precompute thread,
  which runs `execute_ingestion` and publishes the views' rows as a new
  immutable snapshot. A refresh therefore reads exactly the samples the
  serial engine read. Inside a run, a node feeding several views runs once
  and each batch reaches every view (one producer, many consumers).
- At most `--max-precompute-backlog` (default 256) windows wait for their
  refresh; when the backlog is full the ingest thread waits, its queue
  fills up to `--max-queued-samples`, and Remote Write gets 503 so senders
  retry. The backlog is exported: a growing backlog means the data plane
  cannot keep up with the input rate.

### Queries

- Pull model. A fixed set of worker threads (`query-N`, `--query-threads`,
  default one per core); each runs one query run to completion
  (`block_on`). Runs are not `Send` (the executor's runtime uses
  `Rc`), and that is not needed: what crosses threads (compiled plans, store
  snapshots, output batches, sketch states) is `Send + Sync`, checked at
  compile time.
- Concurrency is bounded by the worker count; further requests queue. Each
  run has its own memory limit, so peak query memory ≤ workers × run limit.
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
ones read it. A run holds the results it reads, so dropping one cannot
fail a run. A failed computation is not kept. Results are dropped once T
is more than `--query-time-results-retention-ms` (default 60 s) behind the
newest evaluation time computed, or, oldest first, while more than
`--query-time-results-max-bytes` (default 1 GiB) are held. Install fails if
the plan and the runtime disagree: any other node (for example a computed
`None` node) that several queries' runs would compute. `--no-shared-results`
computes the node in every query's run instead, for comparison. Stage 3
charges a `OneEvaluation` node once per evaluation; this is what makes the
runtime match it.

The query-time results partition is also where views refreshed on first read
will live, with a longer retention, when they are supported.

### Range queries

A range request for a planned query (`start`, `end`, `step` as Prometheus
takes them, at most 11,000 steps) is evaluated at every step time, the
steps in parallel on the query workers, and answered as a Prometheus
matrix; if any step is not answered, the query is not (forwarded with the
reason, or refused under `--no-forward`). Each step is an ordinary
evaluation at its time, so `OneEvaluation` sharing applies per step. The
workload's `range_evaluation` (`step_ms`, `span_ms`) tells the Planner the
requests' shape, so the plan delivers every step time.

### Persisting views

Each view has one append-only file in `--view-dir` (default `asap-views`;
`--no-view-persistence` keeps views in memory only): one record per
refreshed window, a length and a MessagePack payload holding `(start, end]`
and the rows, synced (`fdatasync`) before the next window is refreshed. At
startup the records within each view's retention of its newest one are
loaded back, oldest first, and a window every view of a group already
stores is not refreshed again. A partial record at a file's end (a crash
while writing) is ignored. Plain values and exact states (sum, count, min,
max, rate, increase, which the executor serializes) are stored; sketch
states have no byte encoding yet, so their windows are counted in
`asap_view_windows_not_persisted_total` and not written. Persisting them
needs byte encodings in `asap_sketchlib` and an encode hook in the
executor. Files are not compacted while running; records past retention
are skipped at load.

### Measurements

- Threads are named by role: `ingest`, `precompute`, `query-N`; Remote
  Write is decoded on the HTTP runtime's threads.
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
  - Persistence: `asap_view_persisted_bytes_total`,
    `asap_view_sync_seconds_total`, `asap_view_windows_not_persisted_total`,
    `asap_view_windows_loaded_total`.
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

- Two threads and a worker pool replace one thread; no new public interface.
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
| View persistence | append-only file per view, synced per window, loaded within retention | a checkpoint of the whole store; persisting raw samples |
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
- Views holding sketch states are not persisted yet.
- Sketch answers are within the plan's error target, not exact; only planned
  queries are answered.
- Open: sharded ingestion and its cost-model terms; batch sketch updates.
