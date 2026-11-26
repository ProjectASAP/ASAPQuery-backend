[![MVP CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml)

# ASAPQuery-backend

ASAPQuery-backend is the deployment that executes
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner)'s plans for PromQL
workloads over Prometheus Remote Write data.

Following the Planner's
[planning stages](https://github.com/ProjectASAP/ASAPPlanner/blob/main/docs/design_docs/proposals/planner-layering.md),
the Planner decides everything about a plan: which summaries to use, how to
share them, what to materialize and which plan is cheapest. The backend
supplies its deployment inputs (`DeploymentCapabilities` and Stage 3's
`CostInputs`), then executes the one selected `PhysicalASAPDAG` as given,
with the Planner's executor (`asap-executor`).

```text
PlanningWorkload (JSON)       DeploymentCapabilities + CostInputs
          \                             /
           control_plane: asap_planner::plan_installable
                         |
           plan.json: InstallablePlan (selected PhysicalASAPDAG with each
                      view's and query-time Scan's retention_ms, query texts)
           report.json: SelectedPlanReport (cost, predicted usage)
                         |
           data_plane: Remote Write in, Prometheus query API out
```

## How the data plane executes a plan

The data plane keeps one time-indexed store, which the executor reads
through its `DataStore` interface:

- **Raw data:** every Remote Write sample of a metric some `Scan` of the
  plan reads, as a time series table `(ts, value, $promql_series_identity,
  labels...)`, held columnar in panes whose width divides every refresh
  window. The DAG's own `TimeRange`s keep each window, an instant
  selector's latest sample per series, and drop stale markers.
- **Materialized views:** each view's rows, appended by the ingestion run
  that refreshed them with the `(start, end]` they cover, and kept for the
  view's `retention_ms`.

**Threads.** `--ingest-threads` `ingest-N` shards (default a quarter of the
cores, at least 1) each store the samples of their series, an `ingest`
coordinator keeps the watermark over them and closes windows,
`--precompute-threads` `precompute-N` workers (default a quarter of the
cores, 1 to 4) compute the closed windows' views in parallel and a
`precompute` committer stores each refresh group's windows in window order,
and `query-N` worker threads (`--query-threads`, default one per core)
answer queries, each over the newest published snapshot of the store. See
[docs/design/data-plane-demo.md](docs/design/data-plane-demo.md).

**Ingestion.** The plan lists its views refreshed at ingestion by refresh
group (`InstallablePlan::ingestion_refresh_groups`): views with one window
width `W` and phase `p`. A group's window `(e - W, e]`, `e = p (mod W)`,
closes when the watermark reaches `e`: the newest sample of a scanned metric,
capped at the wall clock, minus `--allowed-lateness-ms` (default 15 s). By
default (`--precompute-mode push`) the ingest shards fold samples into the
open windows' pipelines as they arrive, in batches of up to
`--push-batch-samples` (default 32,768) or `--push-batch-max-ms` (default
1 s), with open-window state under `--precompute-memory-budget-bytes`
(default: no limit); at the window's close the shards' partials are combined
and each view's rows appended. Groups that cannot be pushed, and all groups
under `--precompute-mode window`, run one ingestion run per closed window
over the buffered raw samples. A window without samples is stored empty, not
missing. The first window of each group may have started before the data
plane did, so it is skipped, unless `--stream-start-ms` says where the
producers' stream starts. Windows that would already be dropped when the
watermark jumps are skipped. Raw samples are kept while a window still to be
refreshed or a query-time `Scan` (back to its `retention_ms`) reads them.
The plan's retention counts back from the newest evaluation, but ingestion
refreshes windows ahead of the queries it answers, so views and query-time
raw samples are kept `--view-retention-margin-ms` (default 60 s) longer: a
query up to that far behind the newest refreshed window is still answered.

Samples more than `--max-future-ms` (default 5 min) ahead of the wall clock
are dropped. Remote Write gets 503 when admitting it would leave more than
`--max-queued-samples` (default 2,000,000) samples waiting for an ingest
shard it reaches (a request into an empty queue is always admitted), and
500 if the engine has stopped, which also makes `/-/ready` fail. The
ingest coordinator waits while `--max-precompute-backlog` (default 256) closed windows await
their refresh, so a precompute that cannot keep up pushes back on Remote
Write. `/api/v1/status/asap` reports the engine's `ingestion` counters
(samples accepted, late, future, unrouted; windows refreshed, failed,
skipped) and the precompute backlog as JSON.

A producer can close windows without waiting for later samples:
`POST /api/v1/precompute/watermark?time_ms=T` states that every sample at or
before `T` has been written. It answers once the writes acknowledged before it
have been ingested and every window ending at or before `T` (capped at the
wall clock) is refreshed. A repeated or lower `T` does nothing. The response is
`{"barrier_ms": B}`, the barrier in force. With several producers, send the
barrier only after all of them have written through `T`.

**Queries.** An instant query whose text is one of the plan's queries is
answered by one query run of its root at its evaluation time `T`
(`x-asap-execution: plan`). The run reads the views the root reaches,
through the windows its `TimeRange`s select, and the raw samples its
query-time `Scan`s read; every view read must find rows tiling its window,
or the query is not answered. Each run is limited to
`--run-memory-limit-bytes` (default 4 GiB). Anything else is forwarded to
Prometheus with its end-to-end headers, such as `Authorization` and
`X-Scope-OrgID` (`x-asap-execution: forwarded`); a planned query that could
not be answered also says why in `x-asap-forward-reason`. A query reads the
newest published snapshot when its run starts; it does not wait for writes
still queued (a barrier does). What runs once is the plan's decision: a
query-time `OneEvaluation` node that several queries read is computed once
per evaluation time, by the first query that needs it while the others
wait, and kept in the view store's query-time partition for
`--query-time-results-retention-ms` (default 60 s) behind the watermark;
only at a time the watermark has passed, whose inputs are complete
(`--no-shared-results` computes it in every query's run, for comparison).
A plan that would compute any other node in several queries' runs is
refused. Runs in progress hold up to `--query-threads` times the run
memory limit, plus the query-time results (`--query-time-results-max-bytes`,
default 1 GiB) and queued requests.

**Measurements.** `GET /metrics` exports, in the Prometheus text format,
the thread CPU and wall time of each phase
(`asap_phase_cpu_seconds_total{phase=...}`: `remote_write_decode`,
`append`, `precompute` per refresh group, `query` per query text), query
latency, sample and window counters, the backlogs, the watermark lag, the
bytes held by raw panes and views, RSS, and each thread's CPU time by
name; the view tier and push precompute add `asap_view_cache_*`,
`asap_view_{pending,pinned,file}_bytes`, `asap_view_cold_reads_total`,
`asap_view_cold_read_bytes_total`, `asap_view_segments_marked_cold_total`,
`phase="view_decode"`, `asap_precompute_state_bytes`,
`asap_precompute_open_instances`, `asap_window_commit_lag_seconds` and
`asap_samples_total{outcome="late_for_views"}`. `benchmarks/scripts/profile.sh` records a flamegraph of a running data plane;
`cargo run --release -p data_plane --example ingest_bench -- 10000 300`
measures ingest throughput in process (the micro benchmarks are in
`benchmarks/micro/`).

A range query (`/api/v1/query_range`) whose text is one of the plan's
queries is answered with one evaluation per step, at most one step per
query worker at a time, as a Prometheus matrix; it is not answered if any
step is not, and the remaining steps are then not run. Plan a dashboard's range requests with the workload's
`range_evaluation` (`{"step_ms": 60000, "span_ms": 3600000}`), so the plan
delivers every step time.

Final answers are kept in a result cache (`--result-cache-bytes`, default
256 MiB; `--no-result-cache` turns it off): an answer at a time the
watermark has passed, or of a query that reads no raw samples, is reused by
later instant queries and range steps at that time.

**Persistence and tiered views.** Views are written to `--view-dir`
(default `asap-views`; `--no-view-persistence` keeps views in memory only):
per refresh group and per retention of its views, a directory
`group-<index>/retention-<ms>/` of time segments, each holding
`--view-segment-ms` (default 900,000) of windows, one record per refreshed
window with one columnar fragment per view. A segment is deleted once
every window in it is past its views' retention. The files are both the
views' recovery and their cold tier: decoded windows stay in memory up to
`--view-cache-max-bytes` (default: `memory.max` less the other bounded
pools and 10 % headroom; no limit outside a cgroup) and leave it after `--view-cache-idle-ms` (default 60 s) without a
read; an evicted window is read back from its fragment, by `mmap` of sealed
segments or `pread` (`--view-read`), with states read in place. Encoding,
writing and syncing run on `--persist-threads` `persist-N` threads (default
a quarter of the cores, 1 to 4; refresh group `g` goes to thread `g % n`,
so each file has one writer and keeps window order) after the window is
published, so disk I/O does not slow refreshes; windows queued past
`--max-persist-backlog-bytes` (default 256 MiB) are counted
(`asap_view_persist_over_bound_total`, `asap_persist_backlog_bytes`).
Design: [tiered view storage](docs/design/tiered-view-storage.md).
Files are synced every
`--view-sync-interval-ms` (default 5,000, VictoriaMetrics' default
`-inmemoryDataFlushInterval` as of v1.155.0), so a refreshed window is
durable within that interval, the same guarantee VictoriaMetrics gives its
in-memory data; 0 syncs after every window. A segment names the plan,
group and format version it was written for; another plan's, group's or
version's segment is set aside (`*.stale`), never read. At startup the
segments are indexed from their record heads without decoding fragments; a
torn last record, or the first record that does not decode and everything
after it, is cut off; and a window a view already stores is not refreshed
for it again. Raw samples and open
windows are not persisted, so after a restart the windows whose samples
arrived before it stay missing (`--stream-start-ms` no longer makes the
first window complete). Sketch states are stored as `asap_sketchlib`'s
ASAPv1 bytes, read and rebuilt through each executor kernel's `sketch()`
and `restore_from_disk`: DDSketch, KLL, Count-Min, KMV, HyperLogLog,
weighted frequency (Count-Min or Count Sketch, with or without a heap),
UnivMon, Hydra-KLL and Hydra-Count-Min (one grid per window, shared by its
groups). A UnivMon whose heaps mix key types has no ASAPv1 encoding; its
windows are counted in `asap_view_windows_not_persisted_total`.

Not supported yet: views refreshed on first read (so the backend declares
`query_time_results_retained: false`), SQL.

## Running

```sh
# Plan the planner-layering Example 1 with this backend's capabilities.
cargo run -p control_plane -- \
  --workload docs/examples/planner-layering-example1.json --out plan.json --report report.json

# Serve Remote Write on /api/v1/write and the query API on /api/v1/query[_range].
cargo run -p data_plane -- --plan plan.json --prometheus-url http://localhost:9090
```

Besides Prometheus Remote Write, the data plane ingests VictoriaMetrics
Remote Write (the same protobuf, zstd-compressed) on `/api/v1/import` and
OTLP/HTTP metrics (protobuf, optionally gzip) on `/v1/metrics`, the routes
ASAPQuery serves them on. An OTLP data point becomes a sample of the series
named by its metric (`<name>_sum` and `<name>_count` for histograms and
summaries) and labeled by its scope's, resource's and own attributes. Both
routes answer as Remote Write does (400, 503, 500), and are not served in
forward mode, which delivers only Remote Write bodies to the raw store.

`--deployment-capabilities caps.json` replaces the default
`DeploymentCapabilities`: every summary the Planner's executor builds, raw
data kept by the data plane, and no query-time result kept across
evaluations. For example `{"raw_data_retained": false}` makes the Planner
build every input at ingestion time.

`--cost-inputs costs.json` replaces the Planner's default `CostInputs`
(Stage 3's cost model, resource speed and measured operation costs); an
omitted field keeps its default, cost model 1 with illustrative unit costs.
`--measured-operation-costs table.json` gives Stage 3 the deployment's
measured operation costs (`MeasuredOperationCosts`).
`--ignore-accuracy-guarantees` accepts every query output whatever its
accuracy guarantee, so candidates are compared on cost alone.
`--report report.json` writes Stage 3's `SelectedPlanReport`: the plan's
cost and predicted usage per node and per query. The data facts the plan
assumed are printed. The Planner returns a plan only when its executor
compiles it; otherwise the control plane fails.

`--forward-timeout-seconds` bounds each forwarded query (default 120, matching
Prometheus's default query timeout); a query that runs longer gets 502.

`--no-forward` makes every answer ASAP's: the data plane never sends a query
to Prometheus. It refuses to start with a plan that has a query it cannot
run, and answers any query the plan does not answer (not planned, a window
not stored) with a Prometheus `execution` error (422,
`x-asap-execution: refused`) whose `error` is the reason a forwarded query's
`x-asap-forward-reason` would give.

## Repository layout

| Path | Holds |
|---|---|
| `crates/control-plane/` | package `control_plane`: workload → `asap-planner` → plan file |
| `crates/common/` | package `common`: the engine boundary's types and traits |
| `crates/data-plane/` | package `data_plane`: the binary, its CLI and HTTP routes, and the engine that wires the crates below |
| `crates/precompute/` | package `precompute`: ingest shards, the ingest coordinator, push pipelines and the precompute pool |
| `crates/query/` | package `query`: planned queries on the query workers, and the result cache |
| `crates/storage/` | package `storage`: raw samples and views in memory, the view cache, segments, persistence and the persist threads |
| `crates/internal-metrics/` | package `internal_metrics`: the backend's measurements of itself, exported on `/metrics` |
| `crates/adapters/ingest/prometheus-remote-write/` | package `prometheus_remote_write`: Remote Write v1 decoding and validation |
| `crates/adapters/ingest/victoriametrics-remote-write/` | package `victoriametrics_remote_write`: VictoriaMetrics Remote Write (the Remote Write protobuf, zstd-compressed), `POST /api/v1/import` |
| `crates/adapters/ingest/otlp/` | package `otlp`: OTLP/HTTP metrics decoding into samples, `POST /v1/metrics` |
| `crates/adapters/query/prometheus-http/` | package `prometheus_http`: query parameters, response bodies and the fallback client of a Prometheus-compatible server |
| `benchmarks/` | the backend alone: `micro/` benchmarks (examples of `data_plane`) and `scripts/` |
| `eval/` | comparisons with other systems and the live demo; see [eval/README.md](eval/README.md) |
| `deployment/monitoring/` | Prometheus, exporters and Grafana for the demo and the comparisons |
| `docs/` | design documents and `examples/` workloads |
| `scripts/` | CI and the Planner dependency updater |

The target layout and its migration steps are in `docs/design/repo-layout.md`
(#874).

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The Planner revision is pinned in `Cargo.toml` (ASAPPlanner `737bfb1`; `asap_sketchlib` patched
to `355093f`); see
[scripts/ci/README.md](scripts/ci/README.md).
