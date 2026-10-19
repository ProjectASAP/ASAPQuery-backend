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
`PlanSelectionSettings`), calls `asap_planner::plan_installable` once, and
executes the returned `InstallablePlan` as given. Among the Planner's crates
it depends only on `asap-planner` (to plan) and `asap-executor` (to execute).

```text
PlanningWorkload (JSON)       DeploymentCapabilities + PlanSelectionSettings
          \                             /
           control_plane: asap_planner::plan_installable
                         |
           plan.json: InstallablePlan (selected DAG, root per query,
                      stored outputs with pane and retention, raw windows)
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
| Query engine: keep query-time panes across evaluations (`MatAtQueryKeptAcrossEvaluations`) | done |

Ingestion runs on `--ingestion-workers` threads and queries on
`--query-threads` threads (each default one per core). Series are sharded
across the ingestion workers by a hash of their label set; a pane closes
`--allowed-lateness-ms` (default 15 s) after every worker's newest sample
passes its end, and later samples for it are dropped. The first pane after
startup is skipped because it may be partial. Each query, including every
step of a range query, runs on one query thread; queries read the stored
panes concurrently, by shared reference. The data plane uses jemalloc as its
allocator.

An instant query whose text is one of the plan's queries is answered by the
plan (`x-asap-execution: plan`). Its stored inputs read the panes ending at
`T - offset`, so `T` must be on the pane grid and those panes stored; its
query-time raw ranges are read from Prometheus. An input the plan keeps
across evaluations (`MatAtQueryKeptAcrossEvaluations`) is computed from its
pane's raw samples when an evaluation reads it and no evaluation has kept
it. It is kept in memory, by definition and pane end, once the pane is
closed by the precompute engine's rule (its end at or before the newest
sample minus `--allowed-lateness-ms`), and later evaluations reuse it; a pane
read before it closes is computed again by later evaluations. Kept panes are
evicted like stored ones, measured from the newest evaluation.

Each stored definition is kept for the largest `retention_ms` the plan
states among its outputs: a pane that starts more than that before the
newest evaluation is evicted. The Planner's retention covers the range
evaluations the workload declares (`range_evaluation`), so a range query
reaching further back than declared reads an evicted pane and is
forwarded. Anything else is forwarded
to Prometheus (`x-asap-execution: forwarded`); a planned query that could not
be answered also says why in `x-asap-forward-reason`.

A range query (`/api/v1/query_range`) whose text is one of the plan's queries
is answered as one evaluation per step, `start + k * step` through `end`,
and returns a Prometheus matrix. Each query-time raw range is read from
Prometheus once for all steps and sliced per step; stored and kept panes are
read per step, so a kept pane built at one step is reused by the later ones.
If any step cannot be answered, for example because a step is off the pane
grid or reads an evicted pane, the whole range query is forwarded with the
reason. SQL and ClickHouse are not supported yet.

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
in Prometheus, and query-time results kept across evaluations. For example
`{"raw_data_retained": false}` makes the Planner build every input at
ingestion time. `--plan-selection-settings settings.json` replaces Stage 3's
`PlanSelectionSettings` (cost model and unit prices, resource speed, measured
operation costs). `--measured-operation-costs costs.json` gives Stage 3 the
deployment's measured sketch operation costs; otherwise it prices every node
with cost model 1 and illustrative unit costs.

## Examples

`examples/` holds #509's PromQL example workloads (1, 3a, 3b, 4a, 4b, and
`4-b3`: Example 3b's panel over ten series sampled every millisecond, for
which the Planner keeps query-time panes across evaluations) as Planner
`PlanningWorkload`s. `data_plane/tests/e2e_examples.rs` plans each
one, runs the `data_plane` binary against a Prometheus holding generated
samples, Remote Writes the same samples, and checks every planned query
against the Planner executor running the plan's query-time reference
(`InstallablePlan::query_time_reference`: the same plan with every node at
query time).

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

The Planner revision is pinned in `Cargo.toml`; see
[scripts/ci/README.md](scripts/ci/README.md).
