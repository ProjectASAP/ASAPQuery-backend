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
| Query engine: run query-time nodes over stored panes and raw data | planned |

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

Until the query engine lands, the data plane forwards every query to
Prometheus, with its end-to-end headers such as `Authorization` and
`X-Scope-OrgID`, and marks the response `x-asap-execution: forwarded`. SQL and
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
in Prometheus, and no query-time result kept across evaluations
(`MatAtQueryKeptAcrossEvaluations` is not implemented). For example
`{"raw_data_retained": false}` makes the Planner build every input at
ingestion time. `--measured-operation-costs costs.json` gives Stage 3 the
deployment's measured sketch operation costs; otherwise it prices every node
with cost model 1 and illustrative unit costs.

`--forward-timeout-seconds` bounds each forwarded query (default 120, matching
Prometheus's default query timeout); a query that runs longer gets 502.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The Planner revision is pinned in `Cargo.toml`; see
[scripts/ci/README.md](scripts/ci/README.md).
