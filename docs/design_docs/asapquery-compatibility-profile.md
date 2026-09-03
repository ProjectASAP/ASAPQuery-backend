# ASAPQuery compatibility profile

> Status: proposed MVP architecture and implementation contract
>
> Reference: [ProjectASAP/ASAPQuery at `9fb051a`](https://github.com/ProjectASAP/ASAPQuery/tree/9fb051aa798361fca8e3012835412cb6fa338a0c)
>
> Scope: a deliberately smaller target operating profile of ASAPQuery-backend that
> accepts raw Prometheus Remote Write samples and does not depend on
> ASAPCollector.

## Goal

ASAPQuery-backend has a broader architecture than ASAPQuery: it can coordinate
external collectors, accept materialized summaries over modified OTLP, use
multiple storage tiers, and compile distributed physical plans. This profile
does not remove those capabilities. It defines the target configuration required
for ASAPQuery-compatible behavior and gives that configuration an independent
end-to-end acceptance target. It is not yet a strict subset of the implemented
runtime: Remote Write ingestion and in-backend workload observation must first
be restored as optional backend components. After that work lands, selecting
this profile is a strict configuration subset of the broader product.

The user-visible goal is the same drop-in shape as ASAPQuery:

```text
Prometheus ── remote_write raw samples ──► ASAPQuery-backend
     ▲                                      │
     │ exact fallback                       │ in-process summaries
     │                                      ▼
Grafana / PromQL client ── query ──► accelerated query endpoint
```

Prometheus remains the exact raw-data system. It sends a copy of ingested
samples to ASAPQuery-backend through Prometheus Remote Write. The backend builds
planned summaries from those raw samples and serves supported PromQL from the
summaries. Unsupported, not-yet-ready, stale, or unsafe queries are forwarded
to Prometheus.

The reference behavior is anchored by ASAPQuery's
[top-level architecture](https://github.com/ProjectASAP/ASAPQuery/blob/9fb051aa798361fca8e3012835412cb6fa338a0c/README.md),
[Remote Write decoder](https://github.com/ProjectASAP/ASAPQuery/blob/9fb051aa798361fca8e3012835412cb6fa338a0c/asap-query-engine/src/drivers/ingest/prometheus_remote_write.rs),
[query tracker](https://github.com/ProjectASAP/ASAPQuery/blob/9fb051aa798361fca8e3012835412cb6fa338a0c/asap-query-engine/src/query_tracker/tracker.rs),
and
[precompute design](https://github.com/ProjectASAP/ASAPQuery/blob/9fb051aa798361fca8e3012835412cb6fa338a0c/asap-query-engine/src/precompute_engine/precompute_engine_design_doc.md).
The goal is behavioral compatibility, not copying its historical planner or
internal types.

## Profile boundary

### Required in this profile

- Prometheus Remote Write v1 ingestion at `POST /api/v1/write`;
- Snappy decompression and protobuf `WriteRequest` decoding;
- raw scalar sample, stale-marker, and label canonicalization;
- in-process streaming precompute with windowing and lateness handling;
- an in-process summary store sufficient for the accelerated query path;
- Prometheus-compatible instant and range query endpoints;
- query-workload observation and canonical ASAPPlanner invocation;
- backend-only physical compilation and atomic plan activation;
- summary-backed execution with explicit Prometheus fallback; and
- one self-contained compatibility demo and end-to-end test.

### Excluded from this profile

- ASAPCollector discovery, configuration, or plan publication;
- `CollectorPlan`, `SDKPlan`, OpAMP, or collector activation evidence;
- OTLP or modified-OTLP ingestion;
- prebuilt summary ingestion from external producers;
- source sampling, GOS, sparse delta transmission, and frame ACK protocols;
- VictoriaMetrics Remote Write, Kafka, CSV, JSON, or other ingest connectors;
- SQL, ClickHouse, and Elasticsearch query protocols;
- S3, Thanos, Gorilla, or other durable/cold tiers; and
- distributed placement or multi-producer summary merging.

Excluded features may continue to exist in the larger product. They must be
disabled and must not be startup dependencies when this profile is selected.

## Target architecture

```text
Runtime data path

Prometheus ── remote_write ──► raw receiver ──► precompute ──► SummaryStore

PromQL client ──► Prometheus API ──► query router
                                      ├──► summary readout ◄── SummaryStore
                                      └──► exact fallback ──► Prometheus

Planning path

query observations + Remote Write evidence
                    │
                    ▼
         QueryWorkload + DataWorkload
                    │
                    ▼
               ASAPPlanner
     Post-ASAP candidates + selection
                    │
                    ▼
      backend-only physical compiler
             │              │
             ▼              ▼
       PrecomputePlan   BackendPlan
             │              │
             ▼              ▼
        precompute     store + query router
```

The plan has no collector projection. One compile produces a backend-local
`PrecomputePlan` and `BackendPlan` from the same selected Post-ASAP candidate.
`PrecomputePlan` is an internal typed projection/section of `BackendPlan`, not a
separately published protocol. They share plan and materialization identities.
One immutable version installs the precompute configuration, store catalog, and
inactive query routes atomically. Materialization readiness is runtime state:
each route becomes eligible for summary serving only after its required windows
have complete and fresh coverage.

## Component responsibilities

### ASAPPlanner

ASAPPlanner consumes the complete query workload and associated data workload.
It owns query semantics, workload-wide sharing, abstract summary candidates,
accuracy reasoning, logical window/lifecycle choices, selection, and exact
fallback decisions.

The compatibility profile consumes canonical types and behavior from the pinned
ASAPPlanner revision. It must not restore ASAPQuery's historical planner as a
second planner or copy Planner optimizer, intent-algebra, or sketch-capability
rules into ASAPQuery-backend.

### ASAPQuery-backend control plane

The data-plane query endpoint emits bounded, canonical query observations
without performing planning. The control plane:

1. aggregates those observations as one workload rather than planning each
   request;
2. derives data-workload evidence from Remote Write and runtime measurements;
3. invokes ASAPPlanner with `QueryWorkload` and its `DataWorkload`;
4. enumerates only backend-local implementations for Planner candidates;
5. returns implementation-cost evidence needed for selection;
6. compiles the selected candidate into matching PrecomputePlan and BackendPlan
   views; and
7. stages and atomically activates those views.

It does not enumerate SDK or Collector placements in this profile. A Planner
candidate that has no backend-local raw-sample implementation is unavailable;
the control plane must not assign it an optimistic zero cost.

### Raw ingest and precompute

The Remote Write adapter owns only wire decoding and validation:

```text
HTTP body
  -> verify request limits and content encoding
  -> Snappy decode
  -> protobuf WriteRequest decode
  -> validate __name__, labels, timestamps, and sample encodings
  -> recognize the Prometheus stale-NaN marker before ordinary numeric checks
  -> canonical series key
  -> route raw samples to the active PrecomputePlan
```

The precompute engine owns series routing, bounded buffering, watermark and
lateness behavior, window assignment, selected accumulator updates, and writes
to SummaryStore. The decoder must not choose an aggregation family.

The exact Prometheus stale-NaN bit pattern is a series-staleness event, not a
numeric accumulator input. Other unsupported NaN encodings are rejected by the
profile's documented invalid-sample policy.

Prometheus Remote Write retries can repeat a whole request. Accepted samples
must not be double-counted. The MVP uses canonical series identity plus
timestamp as its idempotency key. An identical value is a duplicate and a
no-op; a conflicting value at the same timestamp is rejected deterministically
and is never aggregated twice. Deduplication state and accumulator mutation
share one warm-state failure domain and are committed before success is
returned. The bounded deduplication horizon is an explicit receiver setting and
must cover the configured maximum lateness plus the deployment's expected
Remote Write retry interval; the receiver cannot infer that interval from the
v1 request.

Batch handling is retry-safe. A response that may cause the sender to retry the
whole request must not leave untracked partial mutations. The implementation
either validates and applies the batch atomically or records enough per-sample
idempotency state for a replay to converge to the same result. Invalid requests
return a non-retryable response; overload and internal failures return the
documented retryable response.

Backpressure is visible. If a bounded queue or memory limit prevents durable
acceptance, `/api/v1/write` returns a retryable non-success response; it must not
return success after silently dropping input.

### SummaryStore

The MVP may use the existing in-process summary store. It stores only state
produced by the active backend-local PrecomputePlan and indexes it by
materialization, canonical label values, and logical window. It tracks enough
coverage and watermark state to distinguish ready, missing, incomplete, and
stale ranges.

Persistent/cold storage is an extension of ASAPQuery-backend, not a dependency
of this compatibility profile. A process restart may lose the warm summary
cache only if query routing falls back to Prometheus until the new plan has
rebuilt complete coverage. Recovery creates a new backend-local producer/runtime
epoch. Pre-crash partial windows remain incomplete and must never be combined
with post-restart samples; they can be served only after explicit Prometheus
backfill, otherwise the first eligible result is a complete post-restart window.

### Query path

The public MVP query surface is:

```text
GET or POST /api/v1/query
GET or POST /api/v1/query_range
```

For each request, the Prometheus adapter preserves query semantics and response
shape. Semantic preservation includes `query`, `time`, `start`, `end`, `step`,
and `timeout`, plus configured tenant and authorization context; it does not
require byte-for-byte reproduction of the incoming HTTP request. Routing has two
successful outcomes:

1. execute the active BackendPlan readout when compatible summary state has
   complete and fresh coverage; or
2. forward a semantically equivalent request to the configured Prometheus
   endpoint.

A parse failure, store miss, unsupported expression, inactive plan, incomplete
window, stale coverage, or insufficient accuracy is not an empty successful
summary result. It follows the explicit fallback route or returns an error if
fallback is unavailable.

The first compatibility level is intentionally explicit rather than claiming
all PromQL. It supports raw scalar samples, canonical label grouping, tumbling
window sum, one sketch-backed quantile operation, and instant and range
evaluation of those planned summaries. Selectors or expressions outside that
set exercise the exact Prometheus fallback path. Expanding the accelerated
surface requires a versioned compatibility-level change and conformance tests.

## Planning and activation lifecycle

Startup is fallback-safe:

```text
start backend
  -> verify Prometheus fallback health
  -> accept Remote Write and observe queries
  -> forward every query to Prometheus
  -> build QueryWorkload and DataWorkload snapshot
  -> run Planner candidate search and selection
  -> compile PrecomputePlan + BackendPlan
  -> atomically install precompute + catalog + inactive routes under one version
  -> enter Materializing state
  -> wait for complete summary coverage
  -> mark each ready materialization Serving
```

The lifecycle is `Compiled -> Installed -> Materializing -> Ready -> Serving`.
Installation is the atomic configuration boundary; readiness is evidence about
runtime data coverage. Plan replacement builds a new immutable snapshot.
Precompute, store metadata, and query routing must never observe a mixture of
old and new aggregation parameters. Until the new plan is installed and warm,
the previous compatible route or Prometheus remains authoritative.

The first MVP may plan once after a fixed observation window. Repeated
replanning is optional, but any later implementation must preserve the same
atomic cutover and warmup rules.

## Relationship to the broader backend

| Concern | ASAPQuery compatibility profile | Broader ASAPQuery-backend |
| --- | --- | --- |
| Ingest source | Prometheus Remote Write raw samples | Collector materializations over modified OTLP and other explicit profiles |
| Summary construction | Backend-local only | Collector or backend placement |
| Physical outputs | PrecomputePlan + BackendPlan | SDKPlan/CollectorPlan/TransmissionPlan/BackendPlan views as applicable |
| Query protocol | Prometheus HTTP / PromQL | Additional protocols may be supported |
| Exact fallback | Upstream Prometheus | Prometheus or another compiled storage/query route |
| Storage required for MVP | In-process warm summary state | Warm, durable, archive, and remote tiers |
| Sampling and delta | Disabled | Optional physical mechanisms |

This table describes the intended product boundary, not the current
implementation state. The compatibility profile is a restricted target profile,
but it is not a strict subset of the backend executable today because some of
its required adapters were removed. Once those adapters are restored behind the
explicit profile, every enabled component belongs to ASAPQuery-backend and the
selected runtime configuration is a strict subset of the broader product.

The profile is also not a literal subset of historical ASAPQuery internals. It
preserves the relevant external behavior while adding the current canonical
ASAPPlanner types, versioned physical compilation, readiness evidence, and
stronger activation and retry contracts.

## Current implementation gap

Against ASAPQuery-backend
[`d1498fd`](https://github.com/ProjectASAP/ASAPQuery-backend/tree/d1498fd191b5f782e743c2d0f27a382b5c69f432):

| Area | Reusable today | Required change |
| --- | --- | --- |
| Prometheus query adapter and fallback client | Present | Bind them to the compatibility profile and its BackendPlan readiness checks. |
| Streaming precompute workers and accumulators | Present | Admit raw Remote Write samples through a dedicated adapter. |
| Hot-reload plan/store/query snapshots | Partial | Install PrecomputePlan and BackendPlan as one atomic version. |
| Prometheus Remote Write decoder/listener | Removed from the current backend path | Restore the narrow v1 adapter from the reference behavior without restoring other legacy connectors. Preserve stale-marker semantics and retry-safe batch application. |
| Query-workload observation | Removed from the data-plane request path | Add a bounded data-plane observer; aggregate its canonical Planner workload input in the control plane without moving planning logic into the request path. |
| Collector/OTLP path | Present in the broader product | Disable it in this profile; do not make it a test or startup dependency. |
| Compatibility E2E | Missing | Add a Prometheus + backend + synthetic writer/query test and demo. |

## Phased implementation

### Phase A: profile and startup contract

Add an explicit `asapquery` profile with startup validation. It permits only
Prometheus fallback, Remote Write ingestion, PromQL HTTP serving, backend-local
precompute, and the warm summary store. An excluded connector or required
Collector endpoint is a configuration error.

Acceptance: the backend starts with no ASAPCollector or OTLP endpoint and all
queries initially reach Prometheus.

### Phase B: Remote Write ingestion

Implement the v1 receiver, strict resource limits, canonical label and stale
marker handling, bounded deduplication/retry behavior, visible backpressure, and
routing into the existing raw-sample precompute input.

Acceptance: valid Snappy/protobuf batches produce the same canonical samples and
staleness events as a reference decoder; replayed whole or partial batches
converge without double-counting; corrupt, oversized, conflicting, and
overloaded requests cannot leave untracked mutations while returning success.

### Phase C: backend-only planning

Collect query and data workload evidence, call the pinned ASAPPlanner, enumerate
backend-local implementations, and compile one PrecomputePlan plus BackendPlan.
Do not create or wait for CollectorPlan.

Acceptance: captured Planner input, selected Post-ASAP candidate,
PrecomputePlan, and BackendPlan are deterministic golden artifacts with matching
plan/materialization/window/family/parameter identities.

### Phase D: atomic activation and warmup

Install the precompute configuration, store catalog, and inactive query routes
atomically under one plan version. Track readiness separately for each
materialization and keep its queries on Prometheus until all required windows
are complete and fresh.

Acceptance: injected failure at every installation boundary exposes either the
old complete configuration or the new complete configuration, never a mixed
one. An installed-but-materializing plan remains on fallback and cannot be
mistaken for a serving route.

### Phase E: PromQL serving and fallback

Serve the declared compatibility-level instant and range queries from planned
summaries. Forward every unsupported or unsafe request to Prometheus with
semantically equivalent parameters and configured request context, and preserve
Prometheus response types, labels, timestamps, warnings, and errors.

Acceptance: accelerated results satisfy their declared error bound against
Prometheus, and fallback responses are equivalent to direct Prometheus calls.

### Phase F: compatibility demo

Run Prometheus with `remote_write` configured to the backend, send a repeating
query workload through the backend, wait for planning and warmup, and capture
route decisions and resource measurements.

Acceptance evidence includes:

- Remote Write requests, samples, rejected requests, duplicates, and bytes;
- observed queries and the exact Planner input/output artifacts;
- active plan/materialization identities and activation time;
- summary versus fallback query counts;
- window coverage and end-to-end freshness;
- result error against direct Prometheus; and
- query latency, backend CPU, and backend memory before and after activation.

## MVP completion criterion

The profile is complete when a clean checkout can run one documented command
that starts Prometheus and ASAPQuery-backend without ASAPCollector, ingests only
through Prometheus Remote Write, plans from the observed workload, activates a
backend-local summary, serves both the declared sum and sketch-backed quantile
compatibility cases from complete summary windows, and transparently falls back
for an unsupported query. The run
must fail if ingestion, planning, activation, coverage, accuracy, or fallback
evidence is missing.

Starting the components or exposing `/api/v1/write` alone is not completion.
