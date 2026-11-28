# Design: repository layout and engine boundaries

Status: accepted; the layout below is in place. Decisions are recorded at
the end.

## Audience and context

This document is for developers of the backend. It decides two things:

1. **Layout**: which crates and top-level folders the repository has, and
   where every current file goes.
2. **Boundaries**: the interfaces between the control plane and the data
   plane, and between the data plane's parts. They follow ASAPQuery's
   [engine boundaries design](https://github.com/ProjectASAP/ASAPQuery/blob/main/.design_docs/asapplanner-engine-boundaries.md)
   (ProjectASAP/ASAPQuery#935), so that the protocol adapters ASAPQuery has
   today (Prometheus, VictoriaMetrics and OTLP ingest; Prometheus, ClickHouse
   and Elastic query endpoints and fallback clients) can move here against
   the same types.

The starting point: before this work the workspace had two crates.

| Crate | Lines | Holds |
|---|---|---|
| `control_plane` | about 200 | `plan_installable` on a `PlanningWorkload`, writes the plan file |
| `data_plane` | about 17,000 | everything else: Remote Write decoding, ingest shards, push precompute, the engine around `asap-executor`, the view store (hot tier, segments, persistence, codecs), queries, result cache, raw forward, the Prometheus HTTP API, `/metrics`, the binary |

Measurement code was in three places: `bench/demo` (the comparison against
VictoriaMetrics, the live demo, Grafana monitoring), `data_plane/examples/`
(eight micro benchmarks and the `remote_write_load` load generator) and
`scripts/`.

One crate for the whole data plane meant nothing enforced which part may use
which. For example, the query code could reach into the store's internals. A
crate per part makes the dependency direction a compile-time rule, and it
speeds up incremental builds.

## Layout

```text
ASAPQuery-backend/
├── Cargo.toml                    # workspace
├── crates/
│   ├── common/                   # boundary types and the FallbackClient trait (see Boundaries)
│   ├── control-plane/            # workload → asap-planner → InstallablePlan
│   ├── precompute/               # the ingest engine: ingest shards, push pipelines, asap-executor ingestion
│   ├── storage/                  # view and raw-data stores: hot tier, segments, persistence, codecs
│   ├── query/                    # the query engine: planned queries over the stores, result cache
│   ├── internal-metrics/         # the backend's own measurements (data-plane renders /metrics)
│   ├── data-plane/               # the binary: CLI, PlanInstaller, QueryDispatcher, HTTP routes, /metrics, wiring
│   └── adapters/
│       ├── ingest/               # prometheus-remote-write, victoriametrics-remote-write, otlp
│       └── query/                # prometheus-http, clickhouse-http, elastic-http, fallback clients
├── benchmarks/                   # the backend alone: micro/ (benchmark binaries), scripts/ (profiling)
├── eval/                         # cross-system experiments for the paper
│   ├── baselines/<system>/       # compared systems that run as their own service (compose files)
│   ├── workloads/
│   ├── harness/                  # prepare, run the arms, report
│   ├── scripts/                  # entry points: fetch pinned binaries, build, run, profile
│   ├── demo/                     # the live Grafana demo
│   └── campaign/                 # records of past multi-host runs
├── deployment/                   # Dockerfile, compose file, monitoring/ (Grafana, Prometheus)
├── docs/
└── README.md
```

### Names

Folders use hyphens (`crates/control-plane`); package names use underscores
(`control_plane`), as Rust identifiers need.

- **`control-plane`, not `planner`.** Planning is ASAPPlanner's
  (`asap-planner`, a pinned git dependency). This crate only builds the
  workload and deployment inputs, calls `plan_installable` and hands the
  plan on. A folder called `planner` would suggest a planner copy here and
  collide with the `asap-planner` crate.
- **`internal-metrics`, not `metrics` or `telemetry`.** In a time-series
  database, "metrics" are the data users write. "Telemetry" is also user
  data here (OTLP is OpenTelemetry). The crate holds the backend's
  measurements of itself.
- **`data-plane`** is the crate that assembles precompute, storage and query
  into one process. Its binary keeps the crate's name until a product name is
  decided (`[[bin]] name` can change it without moving code).
- **No `utils` crate.** A helper goes in the crate that uses it. A helper two
  crates use goes in the lower of the two in the dependency graph, or in
  `common` if it is part of the boundary.

### `benchmarks/` versus `eval/`

- `benchmarks/` measures the backend alone and guards against regressions.
  It runs often and needs only this repository.
- `eval/` compares the backend with other systems for the paper. It installs
  other systems, runs long and writes large results, which stay out of git
  (`eval/.gitignore`).
  Private workloads and anything derived from them never go into `eval/`.

## Boundaries

### Dependency graph

```text
crate          depends on
-------------  --------------------------------------------------
control-plane  asap-planner
adapters/*     common, nothing, or prometheus-remote-write (whose wire format victoriametrics-remote-write extends, and whose protobuf reader and series cache otlp reuses)
precompute     common, storage, internal-metrics, asap-executor
storage        internal-metrics, asap-executor, asap_sketchlib
query          common, storage, internal-metrics, asap-executor
common         asap-planner and asap-types (plan types; asap-planner itself builds on the executor)
internal-metrics  nothing in this workspace (a leaf)
data-plane     common, precompute, storage, query, internal-metrics, every adapter (wiring only)
```

- `common` depends on ASAPPlanner's plan types only (`InstallablePlan`,
  `PhysicalASAPDAG`, `QueryLanguage`, …), never on the executor's runtime.
  This is #935's decision 1.
- Adapters never see an engine or a store. In ASAPQuery, adapters depend
  on the whole engine crate (#935, "Not solved"); a separate `common` crate
  closes that here. The ingest adapters decode samples into the caller's
  types (below), so `prometheus-remote-write` depends on nothing in the
  workspace, `victoriametrics-remote-write` only on
  `prometheus-remote-write`, whose protobuf it reads (zstd-compressed
  instead of snappy), and `otlp` on `prometheus-remote-write`, whose
  `SampleBuilder`, in-place protobuf reader and series cache its decoder
  uses, and on `common`, for the collector view states it decodes
  (`ViewStateBatch`, docs/design/collector-sketch-ingest.md).
- `control-plane` is a dev-dependency of `data-plane`, `precompute` and
  `query` only: their tests plan workloads with it.
- `precompute` and `query` do not depend on each other. Both depend on
  `storage` directly: they read its copy-on-write snapshot and the
  executor's record batches. There is no store trait, and no engine trait:
  each has one implementation, and a trait is added with its second
  implementation (decision 4).
- Only `data-plane` knows every crate. It builds the stores, hands them to
  both engines, and owns `PlanInstaller` and `QueryDispatcher`.

### Control plane → data plane

The contract is the `InstallablePlan`, which is a `PhysicalASAPDAG` plus one
query text per DAG root. Today the control plane writes it as a JSON plan
file, and the data plane installs it at startup with `--plan`. This stays:

```rust
// crates/data-plane
struct PlanInstaller { /* the two engines and the dispatcher */ }
impl PlanInstaller {
    /// Validates the plan, then installs it into the query engine, the
    /// ingest engine and the dispatcher, in that order. Queries see the plan
    /// only after every engine accepts it; on failure the current plan stays.
    fn install(&self, plan: InstallablePlan) -> Result<PlanId, InstallError>;
}
```

The control plane has two callers of `install`:
- the plan file at startup, as today;
- later, the workload tracker (#935's `QueryWorkloadTracker`). It records the
  queries the HTTP endpoints receive as a `PlanningWorkload`, decides when to
  replan, calls `plan_installable` and passes the plan to `install`. Like
  #935's I14, v1 installs once.

`DeploymentCapabilities` and `CostInputs` are control-plane inputs. They
describe what this deployment's engines support and what its resources cost.
They are not part of the data plane's interface.

### Data plane parts (`crates/common`)

These are #935's types, with the same names, so adapters move unchanged:

```rust
// Plans (installer → engines)
struct PlanId(u64);
struct InstalledPlan { id: PlanId, plan: InstallablePlan }
struct InstallError { message: String }

// Requests and results (adapters ↔ dispatcher)
struct QueryRequest { language: QueryLanguage, query: String, eval: QueryEval }
enum QueryEval { Instant { time_ms: i64 }, Range { start_ms: i64, end_ms: i64, step_ms: i64 } }
enum QueryResult { Vector(InstantVector), Matrix(RangeVector), Scalar(Sample), Table(Table) }
type Answer = Arc<Vec<ResultSample>>;   // an instant answer as the query engine returns it natively
struct ResultSample { labels: Arc<LabelSet>, value: f64 }   // labels shared with the stored series: no copy

// Query (dispatcher → query engine)
enum AnswerSource { Planned { plan: PlanId, root: PhysicalASAPNodeId } }
enum EngineQueryResult { Ready { result: QueryResult }, NotReady }
struct QueryEngineError { message: String }

// Fallback (data-plane → query adapters)
trait FallbackClient {
    async fn execute(&self, req: &QueryRequest, headers: &[(String, String)])
        -> Result<FallbackResponse, FallbackError>;   // status, content type, body: no HTTP crates
}

// Re-exported, so callers need no direct Planner dependency
type LabelSet = BTreeMap<String, String>;
const MAX_TIME_MS: i64;   // the largest time, either side of the epoch, the data plane takes
use asap_planner::{InstallablePlan, PhysicalASAPNodeId};
use asap_types::{QueryLanguage, SqlDialect};
```

The engines are concrete types with inherent methods, called by
`data-plane`:

```rust
// crates/precompute
impl PrecomputeEngine { fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>; }
// crates/query
impl ExecutorQueryEngine {
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>;
    async fn execute(&self, source: &AnswerSource, request: &QueryRequest)
        -> Result<EngineQueryResult, QueryEngineError>;
}
```

#935 also has `IngestEngine`, `QueryEngine`, `ViewStore` and
`RawDataStore` traits. The backend has one implementation of each, so it
has no trait; each is added with its second implementation (decision 4).
Ingest routes hand samples to the precompute engine directly, and the
engines read and write `storage`'s stores directly.

`QueryDispatcher` (in `data-plane`) resolves each request against the
installed plan's query texts, runs the query engine, and falls back on
`Unmatched`, `NotReady` or an engine error, as #935 specifies. On the hot
path it calls the query engine's native answer type (`Answer`) instead of
`ExecutorQueryEngine::execute`, so a query does not copy every series'
labels into a `QueryResult`.

The same holds on the ingest side. An ingest adapter builds no boundary
batch type: its decoder takes the caller's series and sample types
(the Remote Write decoder's `SampleBuilder`, which
`victoriametrics-remote-write` and `otlp` reuse), so `data-plane` interns each new series into the store's series
table and a Remote Write or OTLP request decodes straight into the ingest
engine's samples, with no label copied per sample.

The Prometheus HTTP routes forward what the plan does not answer verbatim:
`PrometheusHttpFallback::forward` sends the request's method, path, query
string, headers and body unchanged, so the response is byte-identical to the
server's. `PrometheusHttpFallback` also implements `FallbackClient`.

The ClickHouse and Elasticsearch routes have no request to forward verbatim,
so they use the seam: each parses its request into a `QueryRequest`, calls
`QueryDispatcher::dispatch` (through `ExecutorQueryEngine::execute`, with
`QueryResult`), and
sends what the plan does not answer to its protocol's `FallbackClient`
(`ClickHouseHttpFallback`, `ElasticHttpFallback`). Only a PromQL request can
be the plan's: the control plane plans PromQL workloads, and `storage` holds
time series, while ASAPPlanner lowers SQL to `Table` scans and has no
Elasticsearch frontend. So today every ClickHouse and Elasticsearch query is
forwarded, even one whose text equals a planned PromQL query.

`PlanInstaller` installs once, as #935's I14 does: the engines refuse a
second plan. A retryable install comes with tracker-delivered plans.

### Routes

`data-plane` serves:

- always: `POST /api/v1/write` (Remote Write), `/api/v1/query` and
  `/api/v1/query_range`, `/api/v1/status/asap` (the installed plan and
  ingestion counters), `/metrics`, `POST /api/v1/precompute/watermark` (an
  event-time barrier) and `/-/ready`;
- with forward mode off: `POST /api/v1/import` (VictoriaMetrics Remote
  Write) and `POST /v1/metrics` (OTLP over HTTP). Forward mode delivers
  only Remote Write bodies to the raw store, which would miss their samples;
- with forward mode on: every other path, proxied to the raw store;
- with `--clickhouse-url`: `/clickhouse/query`; with `--elastic-url`:
  `POST /_sql` and `/_search`.

`/metrics` is rendered in `data-plane` (`metrics_endpoint.rs`) from the
measurements `internal-metrics` keeps, so `internal-metrics` stays a leaf.

### Where the backend differs from #935

| Topic | #935 (ASAPQuery) | Backend | Proposal |
|---|---|---|---|
| Query engine | ASAPPlanner's DataFusion query-time engine (ProjectASAP/ASAPPlanner#1468, a merged proposal, not yet code) | `asap-executor` | Keep `asap-executor` inside `crates/query`'s `ExecutorQueryEngine`. Switching later changes only `crates/query`; a `QueryEngine` trait comes if both engines are kept |
| Ingest engine | a new push engine modeled on ASAPQuery's precompute engine | ingest shards + push precompute + the executor's ingestion (docs/design/parallel-precompute.md) | Keep ours, `crates/precompute`'s `PrecomputeEngine` |
| View state | opaque `Arc<dyn Any>` behind a `ViewStore` trait, in memory only, durability deferred | hot tier of decoded windows plus compact on-disk segments | `storage` keeps its own state codecs; no `ViewStore` trait until a second store exists |
| View id | `(PlanId, PhysicalASAPNodeId)` | the plan file's hash, the refresh group and the node id | Keep it; a `PlanId` in the key comes with a second installed plan. On-disk recovery across plans needs ProjectASAP/ASAPPlanner#1429, as in #935 |
| Raw data | `RawDataStore` in memory | raw samples in `storage`'s store, plus optional forward to a raw store | No `RawDataStore` trait; the forward is `PrometheusHttpFallback::forward` plus an ingest tee in `data-plane` |
| Sketch kernels | move out of `asap-executor` into a shared ASAPPlanner crate (#935's decision 48) | through `asap-executor` and `asap_sketchlib` | No change until that crate exists |
| Adapters' dependency | the whole engine crate | `common` only | as in the dependency graph above |
| SQL and Query DSL queries | planned and answered from views | the control plane plans PromQL only; `storage` holds no tables | The ClickHouse and Elasticsearch routes forward every query; planned answers need table sources (ingest, a raw table store, a SQL catalog for the control plane) |

## Where the current files go

| Today | New home |
|---|---|
| `control_plane/` | `crates/control-plane/` |
| `data_plane/src/{ingestion,push}.rs` | `crates/precompute/` |
| `data_plane/src/series_ids.rs` | `crates/storage/` (the store and segments use it; `storage` must not depend on `precompute`) |
| `data_plane/src/engine.rs` | split, see below |
| `data_plane/src/{store,window_cache,memory_budget,segment,persist,state_codec}.rs` | `crates/storage/` |
| `data_plane/src/{query,result_cache}.rs` | `crates/query/` |
| `data_plane/src/raw_forward.rs` | split: the tee into `crates/data-plane/`, the proxy into `crates/adapters/query/` |
| `data_plane/src/remote_write.rs` | `crates/adapters/ingest/prometheus-remote-write/` (the series interning stays in `crates/data-plane/`) |
| ASAPQuery's VictoriaMetrics Remote Write and OTLP decoders | `crates/adapters/ingest/{victoriametrics-remote-write,otlp}/`, served on ASAPQuery's routes (`/api/v1/import`, `/v1/metrics`) when forward mode is off |
| `data_plane/src/prometheus.rs` | `crates/adapters/query/prometheus-http/` |
| ASAPQuery's ClickHouse and Elastic query adapters and fallback clients | `crates/adapters/query/{clickhouse-http,elastic-http}/`, served on ASAPQuery's routes (`/clickhouse/query`, `/_sql`, `/_search`) when their server is given |
| `data_plane/src/metrics.rs` | `crates/internal-metrics/` (the `/metrics` renderer, `metrics_endpoint.rs`, stays in `crates/data-plane/`) |
| `data_plane/src/{lib,main}.rs` | `crates/data-plane/` |
| `data_plane/tests/` | `crates/data-plane/tests/`: each starts the engine or the HTTP routes. Unit tests move with their code, and tests of one crate alone go to that crate's `tests/` (`crates/storage/tests/`) |
| `data_plane/examples/*_bench.rs` | `benchmarks/micro/`; the load generator `remote_write_load.rs` stays in `crates/data-plane/examples/` |
| `bench/demo/{campaign,harness,workloads}` | `eval/` |
| `bench/demo/{live,run_live_demo.sh}` | `eval/demo/` |
| `bench/demo/monitoring/` | `deployment/monitoring/` |
| `scripts/ci/`, `scripts/sync-asapplanner-main.sh` | stay in `scripts/` |
| `scripts/profile.sh`, `scripts/tiered_views_cgroup.sh` | `benchmarks/scripts/` |
| `bench/demo/{run_vm_demo,fetch_binaries,build}.sh`, `bench/demo/profile/profile_query_phase.sh` | `eval/scripts/` |
| ASAPQuery's ClickHouse experiment service | `eval/baselines/clickhouse/docker-compose.yml` (image pinned) |
| (new) a single-node Elasticsearch | `eval/baselines/elasticsearch/docker-compose.yml` (image pinned) |
| `examples/*.json` | `docs/examples/` |

`engine.rs` (about 2,700 lines) is the one file that splits rather than
moves. It holds the installed plan's compiled form and every thread that
executes it:

| Part of `engine.rs` | New home |
|---|---|
| ingest shards, the `ingest` coordinator, the precompute pool and committer | `crates/precompute/` |
| `persist-N` threads and the view-cache monitor | `crates/storage/` (the code; the precompute engine's install starts them) |
| `query-N` workers | `crates/query/` |
| `EngineHandle`, `EngineOptions`, install | `crates/data-plane/` |

## Migration order

Steps 2–6 keep behavior and all tests passing; steps 7–9 only add (routes,
flags, an image) and keep every existing behavior. The steps are separate
PRs, each reviewed on its own:

1. This document.
2. Folder moves only: `control_plane` → `crates/control-plane`, `data_plane` →
   `crates/data-plane` (still one crate), `bench/demo` → `eval/` and
   `deployment/monitoring`, micro benchmarks → `benchmarks/micro`. Paths in
   scripts, CI and docs are updated.
3. Split leaf crates out of `data-plane`: `internal-metrics`, then `storage`.
4. `common` with the boundary types and the `FallbackClient` trait, no
   implementations.
5. Split `precompute` and `query` out of `data-plane`, with
   `PrecomputeEngine` and `ExecutorQueryEngine` taking `InstalledPlan`.
   `PlanInstaller` and `QueryDispatcher` are added in `data-plane`.
6. Move the existing Remote Write decoder and Prometheus HTTP API into
   `crates/adapters/`.
7. Bring over ASAPQuery's ingest adapters (VictoriaMetrics Remote Write,
   OTLP over HTTP) from its main branch, with names that follow its adapter
   extraction (ProjectASAP/ASAPQuery#936); neither needs its `QueryRequest`
   change (ProjectASAP/ASAPQuery#937). They feed the ingest engine as Remote
   Write does.
8. Bring over ASAPQuery's query adapters and fallback clients (ClickHouse and
   Elastic HTTP), named as in #936 (`ClickHouseHttpFallback`,
   `ElasticHttpFallback`) and taking #937's `QueryRequest` (already in
   `common`). Their routes go through `QueryDispatcher::dispatch` and reach
   their fallback through `FallbackClient`.
9. `eval/` baselines and `deployment/` Dockerfiles. The entry-point scripts
   move to `eval/scripts/`. Systems the harness starts itself (Prometheus,
   VictoriaMetrics) stay pinned in `eval/scripts/fetch_binaries.sh` and
   configured in `eval/harness/run.py`; ClickHouse and Elasticsearch get a
   pinned compose file each under `eval/baselines/`, as the servers the
   backend's query routes forward to. `eval/README.md` lists every baseline
   and what is not ported yet (harness arms for ClickHouse and Elasticsearch,
   ASAPQuery's data loaders, sampling baselines). `deployment/Dockerfile`
   builds `control_plane` and `data_plane` into one image (multi-stage,
   pinned bases), and `deployment/docker-compose.yml` plans a workload and
   serves the plan.

## Decisions

1. **`common` lives in this repository**, in `crates/common`. This work
   changes neither ASAPPlanner nor ASAPQuery. The types keep #935's names and
   shapes, so ASAPQuery's adapters can move here without changes.
2. **The workload tracker lives in `control-plane`.** It produces a planning
   input. The query adapters only report each query they receive to it.
3. **No rename for now.** The binary keeps the `data-plane` name; a product
   name can be set later with `[[bin]] name`.
4. **A trait is added with its second implementation.** `FallbackClient` has
   three (Prometheus, ClickHouse, Elasticsearch), called through `dyn` per
   forwarded request. The engines and stores have one each, so they are
   concrete types; #935's `IngestEngine`, `QueryEngine`, `ViewStore` and
   `RawDataStore` come back when a second engine or store does.
