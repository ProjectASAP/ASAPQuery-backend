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

**Threads.** An `ingest` thread stores samples and closes windows, a
`precompute` thread refreshes the closed windows' views, and `query-N`
worker threads (`--query-threads`, default one per core) answer queries,
each over the newest published snapshot of the store. See
[docs/design/data-plane-runtime.md](docs/design/data-plane-runtime.md).

**Ingestion.** The plan lists its views refreshed at ingestion by refresh
group (`InstallablePlan::ingestion_refresh_groups`): views with one window
width `W` and phase `p`. A group's window `(e - W, e]`, `e = p (mod W)`,
closes when the watermark reaches `e`: the newest sample of a scanned metric,
capped at the wall clock, minus `--allowed-lateness-ms` (default 15 s). The
data plane then runs the group's views in one ingestion run over that window
and appends each view's rows. A window without samples is stored empty, not
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
`--max-queued-samples` (default 2,000,000) samples waiting for the ingest
thread (a request into an empty queue is always admitted), and 500 if the
engine has stopped, which also makes `/-/ready` fail. The ingest thread
waits while `--max-precompute-backlog` (default 256) closed windows await
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
`--query-time-results-retention-ms` (default 60 s) behind the newest
evaluation time (`--no-shared-results` computes it in every query's run,
for comparison). A plan that would compute any other node in several
queries' runs is refused. Queries may hold up to `--query-threads`
times the run memory limit.

**Measurements.** `GET /metrics` exports, in the Prometheus text format,
the thread CPU and wall time of each phase
(`asap_phase_cpu_seconds_total{phase=...}`: `remote_write_decode`,
`append`, `precompute` per refresh group, `query` per query text), query
latency, sample and window counters, the backlogs, the watermark lag, the
bytes held by raw panes and views, RSS, and each thread's CPU time by
name. `scripts/profile.sh` records a flamegraph of a running data plane;
`cargo run --release -p data_plane --example ingest_bench -- 10000 300`
measures ingest throughput in process.

Not supported yet: views refreshed on first read (so the backend declares
`query_time_results_retained: false`), range queries, SQL.

## Running

```sh
# Plan #509 Example 1 with this backend's capabilities.
cargo run -p control_plane -- \
  --workload examples/planner-layering-example1.json --out plan.json --report report.json

# Serve Remote Write on /api/v1/write and the query API on /api/v1/query[_range].
cargo run -p data_plane -- --plan plan.json --prometheus-url http://localhost:9090
```

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
not stored, a range query) with a Prometheus `execution` error (422,
`x-asap-execution: refused`) whose `error` is the reason a forwarded query's
`x-asap-forward-reason` would give.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The Planner revision is pinned in `Cargo.toml` (ASAPPlanner `1b18ad92`); see
[scripts/ci/README.md](scripts/ci/README.md).
