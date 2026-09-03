# ASAPQuery compatibility profile

> Status: proposed MVP architecture and implementation contract
>
> Reference: [ProjectASAP/ASAPQuery at `9fb051a`](https://github.com/ProjectASAP/ASAPQuery/tree/9fb051aa798361fca8e3012835412cb6fa338a0c)
>
> Scope: a deliberately smaller operating profile of ASAPQuery-backend that
> accepts raw Prometheus Remote Write samples and does not depend on
> ASAPCollector.

## Goal

ASAPQuery-backend has a broader architecture than ASAPQuery: it can coordinate
external collectors, accept materialized summaries over modified OTLP, use
multiple storage tiers, and compile distributed physical plans. This profile
does not remove those capabilities. It defines the subset required for
ASAPQuery-compatible behavior and gives that subset an independent end-to-end
acceptance target.

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
- raw scalar sample and label canonicalization;
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
separately published protocol. They share plan and materialization identities
and become visible atomically.

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

The control plane:

1. observes PromQL requests as one workload rather than planning each request;
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
  -> validate __name__, labels, timestamps, and finite values
  -> canonical series key
  -> route raw samples to the active PrecomputePlan
```

The precompute engine owns series routing, bounded buffering, watermark and
lateness behavior, window assignment, selected accumulator updates, and writes
to SummaryStore. The decoder must not choose an aggregation family.

Prometheus Remote Write retries can repeat a request. Accepted samples must not
be double-counted. Before returning success, the MVP atomically records recent
deduplication state and applies the sample in the same warm-state failure
domain; the deduplication horizon covers the configured retry horizon. For one
canonical series and timestamp, an identical replay is a duplicate; a
conflicting value is surfaced according to an explicit Prometheus-compatible
policy rather than aggregated twice. After a process crash loses warm state,
query routing remains on Prometheus until complete new summary windows exist.

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
rebuilt complete coverage.

### Query path

The public MVP query surface is:

```text
GET or POST /api/v1/query
GET or POST /api/v1/query_range
```

For each request, the Prometheus adapter preserves query parameters and response
shape. Routing has two successful outcomes:

1. execute the active BackendPlan readout when compatible summary state has
   complete and fresh coverage; or
2. forward the unchanged request to the configured Prometheus endpoint.

A parse failure, store miss, unsupported expression, inactive plan, incomplete
window, stale coverage, or insufficient accuracy is not an empty successful
summary result. It follows the explicit fallback route or returns an error if
fallback is unavailable.

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
  -> stage both against one plan version
  -> activate precompute
  -> wait for complete summary coverage
  -> enable summary query routes
```

Plan replacement builds a new immutable snapshot. Precompute, store metadata,
and query routing must never observe a mixture of old and new aggregation
parameters. Until the new plan is installed and warm, the previous compatible
route or Prometheus remains authoritative.

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

This is a subset relationship: every component enabled in the compatibility
profile belongs to ASAPQuery-backend, but the profile does not exercise every
ASAPQuery-backend component.

## Current implementation gap

Against ASAPQuery-backend
[`d1498fd`](https://github.com/ProjectASAP/ASAPQuery-backend/tree/d1498fd191b5f782e743c2d0f27a382b5c69f432):

| Area | Reusable today | Required change |
| --- | --- | --- |
| Prometheus query adapter and fallback client | Present | Bind them to the compatibility profile and its BackendPlan readiness checks. |
| Streaming precompute workers and accumulators | Present | Admit raw Remote Write samples through a dedicated adapter. |
| Hot-reload plan/store/query snapshots | Partial | Install PrecomputePlan and BackendPlan as one atomic version. |
| Prometheus Remote Write decoder/listener | Removed from the current backend path | Restore the narrow v1 adapter from the reference behavior without restoring other legacy connectors. |
| Query-workload observation | Removed from the data-plane request path | Add a bounded observer that produces canonical Planner workload input without owning planning logic. |
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

Implement the v1 receiver, strict resource limits, canonical label handling,
deduplication/retry behavior, visible backpressure, and routing into the existing
raw-sample precompute input.

Acceptance: valid Snappy/protobuf batches produce the same canonical samples as
a reference decoder; corrupt, oversized, conflicting, and overloaded requests
cannot mutate state while returning success.

### Phase C: backend-only planning

Collect query and data workload evidence, call the pinned ASAPPlanner, enumerate
backend-local implementations, and compile one PrecomputePlan plus BackendPlan.
Do not create or wait for CollectorPlan.

Acceptance: captured Planner input, selected Post-ASAP candidate,
PrecomputePlan, and BackendPlan are deterministic golden artifacts with matching
plan/materialization/window/family/parameter identities.

### Phase D: atomic activation and warmup

Install the precompute configuration, store catalog, and query routes under one
plan version. Keep queries on Prometheus until all required windows are complete
and fresh.

Acceptance: injected failure at every installation boundary exposes either the
old complete snapshot or the new complete snapshot, never a mixed one.

### Phase E: PromQL serving and fallback

Serve supported instant and range queries from planned summaries. Forward every
unsupported or unsafe request to Prometheus with the original semantic
parameters and preserve Prometheus response types, labels, timestamps, warnings,
and errors.

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
backend-local summary, serves at least one supported instant or range query from
that summary, and transparently falls back for an unsupported query. The run
must fail if ingestion, planning, activation, coverage, accuracy, or fallback
evidence is missing.

Starting the components or exposing `/api/v1/write` alone is not completion.
