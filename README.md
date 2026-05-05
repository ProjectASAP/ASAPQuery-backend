[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Build](https://github.com/ProjectASAP/ASAPQuery/actions/workflows/rust.yml/badge.svg)](https://github.com/ProjectASAP/ASAPQuery/actions/workflows/rust.yml)

# ASAPQuery

**ASAPQuery** is a drop-in query accelerator for queries in various languages like PromQL and SQL. ASAPQuery delivers:
- **100x latency reduction** for various complex aggregate queries such as quantiles
- **Configurable query accuracy**
- **Ease-of-use with your existing tech stack**

![ASAPQuery v0.1.0 intercepts queries between Grafana and Prometheus, and accelerates them. Currently, it ingests data using Prometheus' remote_write interface](assets/img/asapquery_intro_figure.jpg)

ASAPQuery v0.1.0 sits between Prometheus and Grafana. It intercepts queries from Grafana and answers them using streaming sketches, instead of scanning large volumes of raw data in Prometheus.
Currently, it ingests data using Prometheus' remote_write interface.
Future versions of ASAPQuery will accelerate queries against other observability systems (e.g. VictoriaMetrics) and time-series databases (e.g. Clickhouse, Elastic).

## Quick Start

**Try ASAPQuery in 5 minutes** with our self-contained demo:

```bash
cd asap-quickstart
docker compose up -d
```

Open http://localhost:3000 and see ASAPQuery vs Prometheus side-by-side!

Full quickstart instructions at [**Quickstart Guide**](asap-quickstart/README.md)

## Why ASAPQuery?

### The Problem

Prometheus (and most time-series analytics) struggle with:
- **High-cardinality metrics** that slow down queries
- **Complex aggregations** such as percentiles
- **Long time windows** - `quantile_over_time(...[1h])` can take seconds or fail
- **Memory pressure** - Loading all raw timeseries required for query computation

### The Solution

ASAPQuery uses **streaming sketches** to:
1. **Pre-compute approximate summaries** as data arrives
2. **Answer queries in milliseconds** using compact sketches instead of raw data
3. **Bound memory usage** - sketches are fixed-size regardless of data volume
4. **Maintain high accuracy** - configurable error bounds (typically <1% error)

## Architecture

ASAPQuery has four main components: the **asap-planner-rs** generates sketch configurations from your query workload, **asap-summary-ingest** deploys streaming pipelines in **Arroyo** that continuously build sketches from live Prometheus metrics, and **asap-query-engine** intercepts PromQL queries and serves them from those pre-computed sketches.

### Components

- **[asap-planner-rs](asap-planner-rs/)** - Analyzes a PromQL query workload and auto-generates sketch configurations for asap-summary-ingest and asap-query-engine
- **[asap-summary-ingest](asap-summary-ingest/)** - Deploys Arroyo streaming pipelines that continuously compute and publish sketches from live metrics
- **[arroyo](https://github.com/ProjectASAP/arroyo)** - Fork of the [Arroyo](https://github.com/ArroyoSystems/arroyo) stream processing engine that runs the sketch-building SQL pipelines
- **[asap-query-engine](asap-query-engine/)** - Intercepts incoming PromQL queries and serves them from pre-computed sketches, falling back to Prometheus for unsupported queries

### Repository Structure

```
├── asap-quickstart/         # Self-contained demo (start here!)
├── asap-planner-rs/         # Auto-configuration service
├── asap-summary-ingest/      # Arroyo pipeline deployer
└── asap-query-engine/       # Query serving engine
# Note: Arroyo fork lives at https://github.com/ProjectASAP/arroyo
```

### Ingest path consumes asap-precompute-rs

Phase 3 step 3 of the ASAP edge-framework migration (see
`docs/design-asap-edge-framework.md` and ADR-0002). The backend's
ingest path now delegates the **shared** envelope-parsing,
sketch-reconstruction, and sketch-merge logic to
[`asap-precompute-rs`](https://github.com/ProjectASAP/ASAPCollector/tree/main/asap-precompute-rs)
— the host-neutral Rust edge runtime — so the same code runs in
agents (Rust shims) and the backend.

**What moved out of this repo (to asap-precompute-rs):**

- Envelope wire-format parsing (`SketchEnvelope` runtime view; was
  inlined in every backend `*Accumulator::from_sketchlib_proto_bytes`).
- Per-sketch state extraction from the
  `asap_sketchlib::proto::sketchlib::SketchEnvelope.sketch_state`
  oneof.
- Sketch reconstruction (DDSketch + KLL today;
  HLL / CountSketch / CountMinSketch are tracked under
  [ProjectASAP/ASAPCollector#243](https://github.com/ProjectASAP/ASAPCollector/issues/243)).
- Cross-runtime sketch merge (asap-precompute-rs's `Sketch::merge`).

**What stays in this repo:**

- Query-side engine — PromQL aggregation, storage, query planning.
- Backend's per-accumulator query-side surface (`AggregateCore`,
  `query_statistic`, `MergeableAccumulator`, ...).
- Sparse-delta application
  (`apply_modified_otlp_delta_bytes` /
  `*Accumulator::apply_proto_delta_bytes`) — `asap_sketchlib`
  doesn't yet expose the `compute_delta` family upstream
  (Go's `sketchlib-go` has it; tracked upstream), so
  asap-precompute-rs's wrappers fall back to "always full" delta
  encoding. Backend's typed-delta apply is independent and stays.

**Bridge layer:**
[`asap-query-engine/src/precompute_operators/edge_runtime_adapter.rs`](asap-query-engine/src/precompute_operators/edge_runtime_adapter.rs)
re-exports the asap-precompute-rs runtime view types
(`SketchEnvelope`, `Encoding`, `SketchType`, the `Sketch` trait
family) and provides the
`reconstruct_via_runtime` / `unwrap_envelope_state` /
`encode_ddsketch_envelope` / `merge_ddsketches_via_runtime`
helpers used by the backend's `decode_modified_otlp_sketch_bytes`
hot path.

**Acceptance tests:**
[`asap-query-engine/tests/edge_runtime_consumes_precompute_rs.rs`](asap-query-engine/tests/edge_runtime_consumes_precompute_rs.rs)
contains round-trip + structural tests proving asap-precompute-rs
sits in the backend's ingest path. HLL / CountSketch / CountMinSketch
tests are present and `#[ignore]`'d with a comment pointing at issue
#243.

## Coming soon

1. Drop-in ASAPQuery artifact that works with your existing pre-configured Prometheus-Grafana stack
2. Drop-in ASAPQuery artifact that accelerates Clickhouse queries

## Current state

ASAPQuery is currently alpha. There are missing features, known bugs, and possible performance issues. We will continue to work on these and create a more mature artifact.

## Research

ASAPQuery is part of [ProjectASAP](https://projectasap.github.io/), a joint effort by researchers at Carnegie Mellon University and University of Maryland.
ASAPQuery is based on academic research on query processing and sketching algorithms.
If you are a researcher interested in using or contributing to ASAPQuery, please [contact us](README.md#contact-us). We are happy to help you.

## Development

<instructions coming soon>

## Contributing

<instructions coming soon>

## License

ASAPQuery is licensed under the MIT License.

## Acknowledgments

We are extremely grateful to the following sources of funding support for ASAPQuery and the academic research that underpins it:
- Laude Institute's Slingshot grant
- Juniper Networks
- U.S. NSF grants CNS-2431093, CNS-2415758, CNS-2132639, CNS-2111751, and CNS-2106214
- U.S. Army Research Office and U.S. Army Research Laboratory Grant W911NF-25-2-0028

## Contact us

Open a Github issue or email us at [contact@projectasap.dev](mailto:contact@projectasap.dev)
