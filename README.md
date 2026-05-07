[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/rust.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/rust.yml)

# ASAPQuery-backend

The query backend of the **ASAP** observability system.

ASAPQuery-backend exposes a **single PromQL HTTP surface** that
internally dispatches to one of two engines based on the controller's
plan and the query's shape:

- **Warm tier** — in-process `SimpleEngine` over a sketch precompute
  store (DDSketch / KLL / HLL / CountSketch / Count-Min Sketch).
  Sub-millisecond responses with bounded `(ε, δ)` accuracy for
  controller-planned query shapes.
- **Archive tier** — Prometheus `promql.Engine` (via Thanos
  store-gateway and thanos-query) over Gorilla-XOR-compressed raw
  chunks on object storage. Exact PromQL surface for ad-hoc and
  post-hoc queries the warm tier cannot answer.

```
                    PromQL HTTP request
                            │
                            ▼
                    ASAPQuery-backend
                  ┌───────────────────────┐
                  │  EngineRouter         │
                  │  (BackendStorage-     │
                  │   Routing + query     │
                  │   shape dispatch)     │
                  └─────────┬─────────────┘
                            │
            ┌───────────────┴───────────────┐
            │                               │
            ▼                               ▼
   ┌─────────────────┐             ┌────────────────────┐
   │   warm tier     │             │   archive tier     │
   │   SimpleEngine  │             │   thanos-query     │
   │   (sketch state │             │   (Prometheus      │
   │   in RAM)       │             │    promql.Engine   │
   │                 │             │    over Gorilla    │
   │   ε > 0,        │             │    chunks on S3)   │
   │   sub-ms        │             │                    │
   │                 │             │   ε = 0, exact     │
   └─────────────────┘             └────────────────────┘
```

## Why two engines?

| | Warm sketch tier | Archive tier (Thanos + Gorilla on S3) |
|---|---|---|
| What it serves | Controller-planned shapes (planned `(metric, W, L, agg_type)` triples) | Anything PromQL — ad-hoc, post-hoc, un-planned shapes |
| Accuracy | bounded ε per sketch family | exact (`ε=0, δ=0, kind=exact`) |
| Latency | µs–ms (RAM lookup) | 10s–100s of ms (object-store reads) |
| Cost | sketch state RAM at the backend | object-store storage + per-query S3 GETs |
| Implementation | this repo | this repo (HTTP forwarder) + [Thanos](https://thanos.io/) |

The split is explicit — every PromQL response carries a
`data_source: <warm|archive>` annotation plus the answer's accuracy
envelope, so callers can decide whether to act on the value or
escalate.

## Repository layout

```
ASAPQuery-backend/
├── asap-common/              # Shared types + utilities
│   └── dependencies/rs/
│       ├── asap_types/         # StorageBackend enum, accuracy envelopes
│       ├── promql_utilities/   # PromQL AST helpers
│       └── ...
├── asap-query-engine/        # The backend (this is the binary)
│   └── src/
│       ├── main.rs              # Entrypoint; wires engines and routing
│       ├── bin/
│       │   └── precompute_engine.rs   # Production binary (Docker entry)
│       ├── engines/
│       │   ├── simple/                # warm tier — SimpleEngine
│       │   │                          # (33 PromQL pattern matchers)
│       │   └── gorilla/               # archive tier
│       │       ├── engine.rs            # in-process curated-subset
│       │       │                        # (fallback when Thanos unset)
│       │       ├── thanos_forward.rs    # ThanosForwardEngine — HTTP
│       │       │                        # forwards to thanos-query
│       │       ├── store.rs             # GorillaS3 chunk fetcher
│       │       ├── postings.rs          # Postings index cache
│       │       └── s3_cost.rs           # Per-engine S3 op counters
│       ├── routing/
│       │   ├── backend_storage_routing.rs  # Per-metric multi-target
│       │   │                                # routing; hot-loaded
│       │   └── engine_router.rs              # Query-shape dispatcher
│       ├── stores/                # SimpleMapStore (warm sketch state)
│       ├── precompute_operators/  # Per-sketch AggregateCore (query)
│       ├── precompute_engine/     # Streaming pipeline (pane folding)
│       └── drivers/
│           └── query/servers/http.rs   # PromQL HTTP API surface
├── asap-quickstart/          # Self-contained 5-min demo
├── asap-summary-ingest/      # Optional alternative ingest path
│                              #   (Arroyo pipelines for users who
│                              #    prefer it over canonical OTLP
│                              #    ingest from ASAPCollector)
├── asap-planner-rs/          # CLI tool — `bin/asap-planner` only.
│                              # Library code that previously planned
│                              # sketch placement was migrated into
│                              # the ASAPCollector controller (see
│                              # "Where the planner lives" below).
└── docs/                      # Design docs
```

## Where the planner lives

**The planner is in [`ASAPCollector/controller/`](https://github.com/ProjectASAP/ASAPCollector/tree/main/controller),
not in this repo.**

This is a deliberate consolidation — the controller is the single
authority for:

1. Which sketches to compute, and where (SDK / agent / gateway / backend)
2. Per-metric `BackendStorageRouting` (which engine answers which query shape)
3. Per-runtime configuration (agent YAML, gateway YAML, backend
   StreamingConfig + StorageRouting JSON)

The controller pushes its plan to all runtimes via OpAMP. The backend
hot-loads the new `BackendStorageRouting` and `StreamingConfig` on
each push without restart. ASAPQuery-backend is therefore mostly an
**executor** — it ingests sketches, evaluates queries, and dispatches
based on the controller-emitted routing table.

The legacy `asap-planner-rs` library (which had its own pattern
matchers) was migrated into the controller. The CLI binary
(`bin/asap-planner`) is retained for external users who script
against the planner from the command line.

## Quick start

The simplest way to see ASAPQuery-backend in action is the
quickstart in [`asap-quickstart/`](asap-quickstart/), which spins up
ASAPCollector + ASAPQuery-backend + Grafana side-by-side with a
minimal workload:

```bash
cd asap-quickstart
docker compose up -d
```

Open `http://localhost:3000` for the Grafana dashboard. See
[`asap-quickstart/README.md`](asap-quickstart/README.md) for the
walkthrough.

For the full multi-stage MVP demo (10 producers / 2 agents / 1
gateway / 1 backend / Thanos store-gateway / MinIO), see
`docs/mvp-demo-runbook.md` in
[ASAPCollector](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/mvp-demo-runbook.md).

## Building from source

This repo path-deps `asap-precompute-rs` and `asap-gorilla` from
[ASAPCollector](https://github.com/ProjectASAP/ASAPCollector), and
`asap_sketchlib` from
[asap_sketchlib](https://github.com/ProjectASAP/asap_sketchlib).
Clone all three repos as siblings under `~/repos/`:

```
~/repos/
├── ASAPCollector/         # path-dep'd by this repo
├── ASAPQuery-backend/     # this repo
└── asap_sketchlib/        # path-dep'd by both
```

Then:

```bash
cd ~/repos/ASAPQuery-backend
cargo build --release
cargo test --release --lib
```

The production binary is at
`target/release/precompute_engine`. For the Docker image, see
ASAPCollector's `deploy/docker/Dockerfile.backend`.

## Configuration

All runtime configuration is **controller-emitted via OpAMP push**;
manual YAML is only the bootstrap when no controller has connected
yet (or in dev / standalone deployments).

The backend reads three environment variables to gate its operating
mode:

| Env var | Purpose | Default |
|---|---|---|
| `ASAP_THANOS_QUERY_URL` | Path A2 — when set, archive-tier queries forward to thanos-query | unset (legacy in-process engine) |
| `ASAP_GORILLA_S3_*` | Legacy in-process `GorillaQueryEngine` config (endpoint, bucket, credentials) | unset (warm-only mode) |
| `CONTROLLER_BACKEND_ENDPOINT` | URL the controller pushes plans to | unset (static-config mode) |

When `ASAP_THANOS_QUERY_URL` is set, the backend registers the
`ThanosForwardEngine` and `BackendStorageRouting` can dispatch
archive queries to it. When unset, the backend falls back to the
in-process curated-subset `GorillaQueryEngine`.

## Wire format

Ingest is **modified-OTLP** with five new typed `Metric.data` variants
(tags 13–17): `DDSketch`, `KLLSketch`, `HLLSketch`, `CountSketch`,
`CountMinSketch`. Each carries a `SketchEnvelope.Payload` with
sketch parameters and an accuracy envelope `(ε, δ, kind)` that the
backend surfaces in every response's `infos` field.

The wire format is documented in
[`asap_otel_proto`](asap-common/dependencies/rs/asap_otel_proto/) and
the cross-language byte-parity gate is described in
[ASAPCollector's edge-framework design](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design-asap-edge-framework.md).

## Query response shape

Every PromQL response carries `infos[]` annotations that tell the
caller how to interpret the answer:

```json
{
  "status": "success",
  "data": { "resultType": "vector", "result": [...] },
  "infos": [
    "data_source: warm",
    "accuracy: ε=0.01, δ=0, kind=relative_quantile",
    "query_latency_ms: 2"
  ]
}
```

| Annotation | What it says |
|---|---|
| `data_source: warm` | answered by the in-process warm sketch tier |
| `data_source: gorilla_archive` | answered by the archive tier (legacy in-process engine) |
| `data_source: thanos_archive` | answered by the archive tier via thanos-query |
| `accuracy: ε=…, δ=…, kind=…` | the sketch's theoretical bound on this answer |
| `query_latency_ms: …` | wall time the backend spent answering |

A request can force a specific engine via the `X-ASAP-Engine` header
or `?engine=` query parameter (used for ground-truth queries during
accuracy verification).

## What's NOT in this repo

These were intentionally moved or deleted as part of the consolidation
that produced the current architecture:

- **Sketch placement planner** — moved into [`ASAPCollector/controller/`](https://github.com/ProjectASAP/ASAPCollector/tree/main/controller)
- **PromQL pattern matchers for the planner** — migrated into the
  controller's L3 `intent_algebra` + L4 `sketch_algebra`
- **JSONL cold-fallback path** — deleted; the archive tier replaces
  it. See ASAPCollector's
  [`docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md)
- **`StorageBackend::ColdJsonlFallback`** enum variant — removed
- **Backend-local cost-model line item for cold-tier scan bytes** —
  removed (controller's tier-spanning cost model is the source of
  truth)

## What's still planned

- **Phase δ — backend pure executor**: drop the in-process
  curated-subset `GorillaQueryEngine` once Path A2 (Thanos) is
  verified at scale. The backend then becomes a thin warm-tier
  evaluator + HTTP forwarder.
- **Per-tenant routing**: `BackendStorageRouting` is global today;
  multi-tenant deployments will need per-tenant routing tables.
- **Hot-reload signal-driven** (not just controller-pushed): so
  operators can rotate the static-YAML bootstrap without restart.

Tracked in [issues](https://github.com/ProjectASAP/ASAPQuery-backend/issues).

## Related repos

- [ASAPCollector](https://github.com/ProjectASAP/ASAPCollector) —
  edge runtimes, controller, gorillas3processor, MVP demo
- [asap_sketchlib](https://github.com/ProjectASAP/asap_sketchlib) —
  cross-language byte-identical sketch library (Rust + Go)

## License

MIT — see [LICENSE](LICENSE).
