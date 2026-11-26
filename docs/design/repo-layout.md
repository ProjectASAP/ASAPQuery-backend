# Design: repository layout and engine boundaries

Status: proposal, for review. Decisions are recorded at the end.

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
   the same traits.

Today the workspace has two crates:

| Crate | Lines | Holds |
|---|---|---|
| `control_plane` | about 200 | `plan_installable` on a `PlanningWorkload`, writes the plan file |
| `data_plane` | about 17,000 | everything else: Remote Write decoding, ingest shards, push precompute, the engine around `asap-executor`, the view store (hot tier, segments, persistence, codecs), queries, result cache, raw forward, the Prometheus HTTP API, `/metrics`, the binary |

Measurement code is in three places: `bench/demo` (the comparison against
VictoriaMetrics, the live demo, Grafana monitoring), `data_plane/examples/*_bench.rs`
(nine micro benchmarks) and `scripts/`.

One crate for the whole data plane means nothing enforces which part may use
which. For example, the query code can reach into the store's internals. A
crate per part makes the dependency direction a compile-time rule, and it
speeds up incremental builds.

## Layout

```text
ASAPQuery-backend/
├── Cargo.toml                    # workspace
├── crates/
│   ├── common/                   # engine boundary types and traits (see Boundaries)
│   ├── control-plane/            # workload → asap-planner → InstallablePlan
│   ├── precompute/               # IngestEngine: ingest shards, push pipelines, asap-executor ingestion
│   ├── storage/                  # ViewStore + RawDataStore: hot tier, segments, persistence, codecs
│   ├── query/                    # QueryEngine: planned queries over the stores, result cache
│   ├── internal-metrics/         # the backend's own measurements, exported on /metrics
│   ├── data-plane/               # the binary: CLI, PlanInstaller, QueryDispatcher, HTTP routes, wiring
│   └── adapters/
│       ├── ingest/               # prometheus-remote-write, victoriametrics-remote-write, otlp
│       └── query/                # prometheus-http, clickhouse-http, elastic-http, fallback clients
├── benchmarks/                   # the backend alone: micro (criterion) and system ingest/query
├── eval/                         # cross-system experiments for the paper
│   ├── baselines/<system>/       # install and configure each compared system
│   ├── workloads/
│   ├── harness/
│   ├── scripts/
│   └── results/                  # .gitignore'd; never committed
├── deployment/                   # Dockerfiles, compose files, monitoring (Grafana, Prometheus)
├── docs/
└── README.md
```

### Names

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
  other systems, runs long and writes large results, which stay out of git.
  Private workloads and anything derived from them never go into `eval/`.

## Boundaries

### Dependency graph

```text
crate          depends on
-------------  --------------------------------------------------
control-plane  common, asap-planner
adapters/*     common, nothing, or the adapter whose wire format they extend
precompute     common, storage, internal-metrics, asap-executor
storage        common, internal-metrics, asap-executor
query          common, storage, internal-metrics, asap-executor
common         asap-planner and asap-types (plan types; asap-planner itself builds on the executor)
internal-metrics  nothing in this workspace (a leaf)
data-plane     every crate above (wiring only)
```

- `common` depends on ASAPPlanner's plan types only (`InstallablePlan`,
  `PhysicalASAPDAG`, `QueryLanguage`, …), never on the executor's runtime.
  This is #935's decision 1.
- Adapters never see an engine or a store. In ASAPQuery, adapters depend
  on the whole engine crate (#935, "Not solved"); a separate `common` crate
  closes that here. The ingest adapters need not even `common`: each
  decodes into the caller's types (below), so `prometheus-remote-write` and
  `otlp` depend on nothing in the workspace, and
  `victoriametrics-remote-write` only on `prometheus-remote-write`, whose
  protobuf it reads (zstd-compressed instead of snappy).
- `precompute` and `query` do not depend on each other. Both depend on
  `storage`: they read its copy-on-write snapshot and the executor's record
  batches, which the `ViewStore` and `RawDataStore` traits cannot express
  without copying. The traits remain the boundary for other implementations;
  `storage` provides adapters for them.
- Only `data-plane` knows every implementation. It builds the stores, injects
  them into both engines, and owns `PlanInstaller` and `QueryDispatcher`.

### Control plane → data plane

The contract is the `InstallablePlan`, which is a `PhysicalASAPDAG` plus one
query text per DAG root. Today the control plane writes it as a JSON plan
file, and the data plane installs it at startup with `--plan`. This stays:

```rust
// crates/data-plane
struct PlanInstaller { /* the two engines and the dispatcher */ }
impl PlanInstaller {
    /// Validates the plan (supported operators, sources), then installs it
    /// into the ingest engine, the query engine and the dispatcher, all or
    /// nothing. On failure the current plan stays.
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

These are #935's interfaces, with the same names, so adapters move unchanged:

```rust
// Requests and results (adapters ↔ dispatcher)
struct QueryRequest { language: QueryLanguage, query: String, eval: QueryEval }
enum QueryEval { Instant { time_ms: i64 }, Range { start_ms: i64, end_ms: i64, step_ms: i64 } }
enum QueryResult { Vector(..), Matrix(..), Scalar(..), Table { columns, rows } }
type Answer = Arc<Vec<ResultSample>>;   // an instant answer as the query engine returns it natively
struct ResultSample { labels: Arc<LabelSet>, value: f64 }   // labels shared with the stored series: no copy

// Ingest (data-plane → precompute)
enum RawDataBatch { TimeSeries { series: Vec<(LabelSet, Vec<(i64, f64)>)> } }
trait IngestEngine {
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>;
    async fn ingest(&self, batch: RawDataBatch) -> Result<(), IngestError>;  // waits under backpressure
    fn shutdown(&self);
}

// Query (dispatcher → query)
enum AnswerSource { Planned { plan: PlanId, root: PhysicalASAPNodeId } }
enum EngineQueryResult { Ready { result: QueryResult }, NotReady }
trait QueryEngine {
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>;
    async fn execute_batch(&self, queries: &[(AnswerSource, QueryRequest)])
        -> Vec<Result<EngineQueryResult, QueryEngineError>>;
}

// Stores (precompute writes, query reads)
struct ViewId { plan: PlanId, node: PhysicalASAPNodeId }
struct ViewInstanceKey { view: ViewId, window_ms: (i64, i64) }   // (start, end]
trait ViewStore { put, get, instances, evict_before, timestamp_range_per_view }
trait RawDataStore { append, read /* None if the range reaches evicted data */, evict }

// Fallback (data-plane → query adapters)
trait FallbackClient {
    async fn execute(&self, req: &QueryRequest, headers: &[(String, String)])
        -> Result<FallbackResponse, FallbackError>;   // status, content type, body: no HTTP crates
}
```

`QueryDispatcher` (in `data-plane`) resolves each request against the
installed plan's query texts, runs the query engine, and falls back on
`Unmatched`, `NotReady` or an engine error, as #935 specifies. On the hot
path it calls the query engine's native answer type (`Answer`) instead of
`QueryEngine::execute_batch`, so a query does not copy every series'
labels into a `QueryResult`.

The same holds on the ingest side. An ingest adapter does not build a
`RawDataBatch`: its decoder takes the caller's series and sample types
(the Remote Write decoder's `SampleBuilder`, which
`victoriametrics-remote-write` reuses), so `data-plane` interns each new series into the store's series
table and a Remote Write request decodes straight into the ingest engine's
samples, with no label copied per sample. `otlp` returns each point's
labels as strings, which `data-plane` interns the same way; OTLP ingest is
not on the measured path, so it copies them.

The Prometheus HTTP routes forward what the plan does not answer verbatim:
`PrometheusHttpFallback::forward` sends the request's method, path, query
string, headers and body unchanged, so the response is byte-identical to the
server's. `PrometheusHttpFallback` also implements `FallbackClient`, the seam
the ClickHouse and Elastic fallback clients (step 8) implement.

`PlanInstaller` installs once, as #935's I14 does; a retryable install
comes with tracker-delivered plans.

### Where the backend differs from #935

| Topic | #935 (ASAPQuery) | Backend | Proposal |
|---|---|---|---|
| Query engine | ASAPPlanner's DataFusion query-time engine (ProjectASAP/ASAPPlanner#1468, a merged proposal, not yet code) | `asap-executor` | Keep `asap-executor` behind `QueryEngine`. Switching later changes only `crates/query` |
| Ingest engine | a new push engine modeled on ASAPQuery's precompute engine | ingest shards + push precompute + the executor's ingestion (docs/design/parallel-precompute.md) | Keep ours behind `IngestEngine` |
| View state | opaque `Arc<dyn Any>`, in memory only, durability deferred | hot tier of decoded windows plus compact on-disk segments | `storage` keeps its own state codecs; an encode/decode hook joins `ViewStore` only when a second implementer needs it |
| View id | `(PlanId, PhysicalASAPNodeId)` | directories per refresh group and retention | Key the store by `ViewId`. On-disk recovery across plans needs ProjectASAP/ASAPPlanner#1429, as in #935 |
| Raw data | `RawDataStore` in memory | raw samples in `store.rs`, plus optional forward to a raw store | `RawDataStore` trait over both; the forward becomes a `FallbackClient` plus an ingest tee in `data-plane` |
| Sketch kernels | move out of `asap-executor` into a shared ASAPPlanner crate (#935's decision 48) | through `asap-executor` and `asap_sketchlib` | No change until that crate exists |
| Adapters' dependency | the whole engine crate | `common` only | as in the dependency graph above |

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
| ASAPQuery's VictoriaMetrics Remote Write and OTLP decoders | `crates/adapters/ingest/{victoriametrics-remote-write,otlp}/`, served on ASAPQuery's routes (`/api/v1/import`, `/v1/metrics`) |
| `data_plane/src/prometheus.rs` | `crates/adapters/query/prometheus-http/` |
| `data_plane/src/metrics.rs` | `crates/internal-metrics/` |
| `data_plane/src/{lib,main}.rs` | `crates/data-plane/` |
| `data_plane/tests/` | the crate whose behavior each test checks; end-to-end tests in `crates/data-plane/tests/` |
| `data_plane/examples/*_bench.rs` | `benchmarks/micro/` |
| `bench/demo/{campaign,harness,workloads,profile}` | `eval/` |
| `bench/demo/{live,run_live_demo.sh}` | `eval/demo/` |
| `bench/demo/monitoring/` | `deployment/monitoring/` |
| `scripts/ci/`, `scripts/sync-asapplanner-main.sh` | stay in `scripts/` |
| `scripts/profile.sh`, `scripts/tiered_views_cgroup.sh` | `benchmarks/scripts/` |
| `examples/*.json` | `docs/examples/` |

`engine.rs` (about 2,700 lines) is the one file that splits rather than
moves. It holds the installed plan's compiled form and every thread that
executes it:

| Part of `engine.rs` | New home |
|---|---|
| ingest shards, the `ingest` coordinator, the precompute pool and committer | `crates/precompute/` |
| `persist-N` threads and the view-cache monitor | `crates/storage/` |
| `query-N` workers | `crates/query/` |
| `EngineHandle`, `EngineOptions`, install | `crates/data-plane/` |

## Migration order

Every step keeps behavior and all tests passing. The steps are separate PRs,
each reviewed on its own:

1. This document.
2. Folder moves only: `control_plane` → `crates/control-plane`, `data_plane` →
   `crates/data-plane` (still one crate), `bench/demo` → `eval/` and
   `deployment/monitoring`, micro benchmarks → `benchmarks/micro`. Paths in
   scripts, CI and docs are updated.
3. Split leaf crates out of `data-plane`: `internal-metrics`, then `storage`.
4. `common` with the boundary types and traits, no implementations.
5. Split `precompute` and `query` out of `data-plane`. `storage` implements
   `ViewStore` and `RawDataStore`, `precompute` implements `IngestEngine`,
   and `query` implements `QueryEngine`. `PlanInstaller` and `QueryDispatcher`
   are added in `data-plane`.
6. Move the existing Remote Write decoder and Prometheus HTTP API into
   `crates/adapters/`.
7. Bring over ASAPQuery's ingest adapters (VictoriaMetrics Remote Write,
   OTLP over HTTP) from its main branch, with names that follow its adapter
   extraction (ProjectASAP/ASAPQuery#936); neither needs its `QueryRequest`
   change (ProjectASAP/ASAPQuery#937). They feed the ingest engine as Remote
   Write does.
8. Bring over ASAPQuery's query adapters and fallback clients (ClickHouse and
   Elastic HTTP).
9. `eval/` baselines (ClickHouse and others) and `deployment/` Dockerfiles.

## Decisions

1. **`common` lives in this repository**, in `crates/common`. This work
   changes neither ASAPPlanner nor ASAPQuery. The traits keep #935's names and
   shapes, so ASAPQuery's adapters can move here without changes.
2. **The workload tracker lives in `control-plane`.** It produces a planning
   input. The query adapters only report each query they receive to it.
3. **No rename for now.** The binary keeps the `data-plane` name; a product
   name can be set later with `[[bin]] name`.
