[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/rust.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery-backend/actions/workflows/rust.yml)

# ASAPQuery-backend

The query backend of the **ASAP** observability system.

ASAPQuery-backend exposes a **single PromQL HTTP surface** that
internally dispatches to one of two engines based on the control plane's
plan and the query's shape:

- **Warm tier** — in-process `ASAPQueryEngine` over a sketch precompute
  store (DDSketch / KLL / HLL / CountSketch / Count-Min Sketch).
  Sub-millisecond responses with bounded `(ε, δ)` accuracy for
  control-plane-planned query shapes.
- **Archive tier** — Prometheus `promql.Engine` (via Thanos
  store-gateway and thanos-query) over Gorilla-XOR-compressed raw
  chunks on object storage. Exact PromQL surface for ad-hoc and
  post-hoc queries the ASAP tier cannot answer.

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
   │   ASAP tier     │             │   archive tier     │
   │ ASAPQueryEngine │             │   thanos-query     │
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
| What it serves | Control-plane-planned shapes (planned `(metric, W, L, agg_type)` triples) | Anything PromQL — ad-hoc, post-hoc, un-planned shapes |
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
ASAPQuery-backend/                 # Cargo workspace
├── crates/                        # Shared workspace libraries
│   ├── asap_types/                  # StorageBackend enum, accuracy envelopes
│   ├── promql_utilities/            # PromQL AST helpers
│   └── asap_otel_proto/             # OTLP protobuf bindings
├── data_plane/                    # The query backend (binary; src/main.rs)
│   └── src/
│       ├── main.rs                  # Entrypoint; wires engines and routing
│       ├── query_engines/
│       │   ├── asap_query_engine/     # warm/ASAP tier — ASAPQueryEngine
│       │   ├── thanos_query_engine/   # archive tier — ThanosQueryEngine
│       │   │                          #   (forward.rs HTTP-forwards to thanos-query)
│       │   └── routing/               # EngineRouter + BackendStorageRouting
│       ├── storage_engines/
│       │   ├── sketch_db/             # SketchStore — warm sketch state
│       │   │                          #   (index/ data/ query/ lifecycle/
│       │   │                          #    persistence/ backfill/)
│       │   ├── gorilla_object_store/  # GorillaS3Store + GorillaQueryEngine
│       │   │                          #   (in-process archive fallback)
│       │   └── types/                 # shared storage types
│       ├── precompute_engine/         # Streaming pipeline (+ operators/)
│       └── drivers/                   # ingest/, query/ (PromQL HTTP API),
│                                      #   control_plane_client/
├── control_plane/                 # In-repo control plane / planner (binary)
│   └── src/
│       ├── query_parser/            # PromQL/SQL parsing → intent algebra
│       ├── intent_algebra/          # shared intent representation
│       ├── sketch_algebra/          # sketch planning (+ rules/)
│       ├── optimizer/               # plan optimization (cost/ + rules/)
│       ├── physical/                # physical plan (colored_dag/)
│       ├── emit/                    # per-runtime config emission
│       └── opamp/                   # OpAMP server — pushes plans to runtimes
└── docs/                          # Design docs
```

## Where the planner lives

**The planner — the "control plane" — now lives in this repo at
[`control_plane/`](control_plane/).** It was moved in-tree (Phase 9)
from its former `ASAPCollector/controller/` location; `data_plane/`
(the query backend) and `control_plane/` (the planner) are now
workspace siblings.

The control plane is the single authority for:

1. Which sketches to compute, and where (SDK / agent / gateway / backend)
2. Per-metric `BackendStorageRouting` (which engine answers which query shape)
3. Per-runtime configuration (agent YAML, gateway YAML, backend
   StreamingConfig + StorageRouting JSON)

It pushes its plan to all runtimes via OpAMP. The data plane hot-loads
the new `BackendStorageRouting` and `StreamingConfig` on each push
without restart, so it is mostly an **executor** — it ingests
sketches, evaluates queries, and dispatches based on the
control-plane-emitted routing table.

The legacy `asap-planner-rs` workspace member (library + CLI) was
deleted in Phase γ; its PromQL pattern-matching and archive-only
intents now live in `control_plane/`'s query-lowering stages.

## Quick start

ASAPQuery-backend is the query backend; the runnable demos — which
spin up ASAPCollector + ASAPQuery-backend + Grafana together — live in
[ASAPCollector](https://github.com/ProjectASAP/ASAPCollector). The
full multi-stage MVP demo (10 producers / 2 agents / 1 gateway / 1
backend / Thanos store-gateway / MinIO) is documented in its
[`mvp-demo-runbook.md`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/user_guide/mvp-demo-runbook.md).

To build and run just this backend, see **Building from source** below.

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
`ThanosQueryEngine` and `BackendStorageRouting` can dispatch
archive queries to it. When unset, the backend falls back to the
in-process curated-subset `GorillaQueryEngine`.

## Wire format

Ingest is **modified-OTLP** with five new typed `Metric.data` variants
(tags 13–17): `DDSketch`, `KLLSketch`, `HLLSketch`, `CountSketch`,
`CountMinSketch`. Each carries a `SketchEnvelope.Payload` with
sketch parameters and an accuracy envelope `(ε, δ, kind)` that the
backend surfaces in every response's `infos` field.

The wire format is documented in
[`asap_otel_proto`](crates/asap_otel_proto/) and
the cross-language byte-parity gate is described in
[ASAPCollector's system design](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/system-overview.md).

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
- **JSONL cold-fallback path** — deleted; the configured exact backend is the
  explicit fallback described by
  [`query-execution.md`](data_plane/docs/query-execution.md).
- **`StorageBackend::ColdJsonlFallback`** enum variant — removed
- **Backend-local cost-model line item for cold-tier scan bytes** —
  removed (controller's tier-spanning cost model is the source of
  truth)

## What's still planned

- **Phase δ — backend pure executor**: drop the in-process
  curated-subset `GorillaQueryEngine` once Path A2 (Thanos) is
  verified at scale. The backend then becomes a thin ASAP-tier
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
