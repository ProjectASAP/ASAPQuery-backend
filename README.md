[![MVP CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/mvp-ci.yml)

# ASAPQuery-backend

ASAPQuery-backend is the deployment that executes
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner)'s plans for PromQL
workloads over Prometheus Remote Write data.

Following the Planner's
[planning stages](https://github.com/ProjectASAP/ASAPPlanner/blob/main/docs/design_docs/proposals/planner-layering.md),
the Planner decides everything about a plan: which summaries to use, how to
share them, what to materialize and which plan is cheapest. The backend
supplies its deployment inputs (`DeploymentCapabilities`, and Stage 3's
`PlanSelectionSettings`), then executes the one selected `PhysicalASAPDAG` as
given.

```text
PlanningWorkload (JSON)       DeploymentCapabilities + PlanSelectionSettings
          \                             /
           control_plane: asap_planner::plan_installable
                         |
           plan.json: InstallablePlan (selected PhysicalASAPDAG, query per
                      root, stored outputs with retention, raw windows)
                         |
           data_plane: Remote Write in, Prometheus query API out
```

## Status

The backend is being rebuilt on the Planner's physical DAG:

| Part | State |
|---|---|
| Control plane: plan a PromQL workload, write `plan.json` | done |
| Data plane: load and validate `plan.json`, accept Remote Write | done |
| Stored summaries (SDS): one definition per computation, panes by end time | done |
| Precompute engine: build ingestion-time outputs once per tumbling pane | done |
| Query engine: answer planned instant queries from stored panes and raw samples | done |

The data plane stores the plan's `stored_outputs`. Outputs that differ only
in which pane they read share one stored summary, kept for the largest
`retention_ms` the plan states among them (which covers range-evaluation
spans), plus one pane width so an evaluation one pane behind the newest
closed pane is still served; older panes are evicted.

Panes close `--allowed-lateness-ms` (default 15 s) after the newest sample
of a stored summary's metric, capped at the wall clock, passes their end; later samples for them are
dropped. The first pane after startup is skipped because it may be partial.
Samples more than `--max-future-ms` (default 5 min) ahead of the wall clock
are dropped. Remote Write gets 503 when admitting it would leave more than
`--max-queued-samples` (default 2,000,000) samples waiting for the engine
(a request into an empty queue is always admitted), and 500 if the engine has
stopped, which also makes `/-/ready` fail. `/api/v1/status/asap` reports the
engine's sample and pane counters as JSON.

A producer can close panes without waiting for later samples:
`POST /api/v1/precompute/watermark?time_ms=T` states that every sample at or
before `T` has been written. After the writes acknowledged before it have
been ingested, every pane ending at or before `T` (capped at the wall clock)
is closed, so the last pane of a series that stopped sending is built and
served. A repeated or lower `T` does nothing and input stays open. A sample
that arrives later for a pane the barrier closed (one ending at or before
`T`) counts as late; a later sample in a pane still open is accepted, even if
it is at or before `T`. The response is `{"barrier_ms": B}`, the barrier in
force: `T` capped at the wall clock, or an earlier higher barrier. With several producers,
send the barrier only after all of them have written through `T`.

An instant query whose text is one of the plan's queries is answered by the
plan (`x-asap-execution: plan`) at its evaluation time `T`. Stored outputs
built in an `IngestionRun` are read from the SDS as the panes ending at
`T - offset`, so `T` must be on the pane grid and those panes stored. The
plan's query-time `RawSampleWindow`s are read from Prometheus. A stored output
built in a `QueryRun` (`MatAtQueryKeptAcrossEvaluations`) is built from its
pane's raw samples the first time an evaluation needs it. It is stored in the
SDS under the same key once its pane is closed, that is, once its end is at or
before the watermark at the time the evaluation chose its fetch. Later
evaluations reuse it. It is evicted like an `IngestionRun` output: the plan's
`retention_ms` plus one pane, measured from the newest closed pane at or
before the watermark. `/api/v1/status/asap` reports `query_run` counters,
including summary states stored while their own metric lagged behind the
global watermark. Anything else is forwarded
to Prometheus with its end-to-end headers, such as `Authorization` and
`X-Scope-OrgID` (`x-asap-execution: forwarded`); a planned query that could not
be answered also says why in `x-asap-forward-reason`. Range queries, SQL and
ClickHouse are not supported yet.

## Running

```sh
# Plan #509 Example 1 with this backend's capabilities.
cargo run -p control_plane -- \
  --workload examples/planner-layering-example1.json --out plan.json

# Serve Remote Write on /api/v1/write and the query API on /api/v1/query[_range].
cargo run -p data_plane -- --plan plan.json --prometheus-url http://localhost:9090
```

`--deployment-capabilities caps.json` replaces the default
`DeploymentCapabilities`: every summary the Planner can build, raw data kept
in Prometheus, and stored outputs built in a `QueryRun` (`MatAtQueryKeptAcrossEvaluations`). For example
`{"raw_data_retained": false}` makes the Planner build every input at
ingestion time.

`--plan-selection-settings settings.json` replaces the Planner's default
`PlanSelectionSettings` (Stage 3's cost model, resource speed, measured
operation costs and `ignore_accuracy_guarantees`); an omitted field keeps its
default, cost model 1 with illustrative unit costs.
`--measured-operation-costs costs.json` gives Stage 3 the deployment's
measured operation costs (`MeasuredOperationCosts`).
`--ignore-accuracy-guarantees` accepts every query output whatever its
accuracy guarantee, so candidates are compared on cost alone; the plan's
`selection.queries_accepted_without_guarantee` lists the queries accepted
only because of it.

`--stream-start-ms T` declares that the producers write every raw sample at
or after `T`: the first pane of each stored output that starts at or after
`T` is complete and is built. Without it, the first pane each stored output
sees may have started before the data plane did, so it is skipped and a
query whose window needs it is not answered from the plan.

`--forward-timeout-seconds` bounds each forwarded query (default 120, matching
Prometheus's default query timeout); a query that runs longer gets 502.

`--no-forward` makes every answer ASAP's: the data plane never sends a query
to Prometheus for computation. It refuses to start with a plan that has a
query it cannot run, and answers any query the plan does not answer (not
planned, a pane not stored, an evaluation time off the pane grid, a range
query) with a Prometheus `execution` error (422, `x-asap-execution: refused`)
whose `error` is the reason a forwarded query's `x-asap-forward-reason` would
give. Raw samples for query-time windows are still read from Prometheus. The
control plane writes a plan only when every workload query has one the data
plane can execute; otherwise it fails.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The Planner revision is pinned in `Cargo.toml`; see
[scripts/ci/README.md](scripts/ci/README.md).
