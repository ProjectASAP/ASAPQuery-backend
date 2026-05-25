# Overview & Key Concepts

Welcome to ASAP! This guide will help you understand what ASAP is, why it exists, and the key concepts you need to know.

## What is ASAP?

**ASAP** is a **drop-in query accelerator** for Prometheus that delivers:

- ⚡ **Sub-second latency** for complex quantile and aggregate queries
- 💾 **100x memory reduction** compared to raw time-series data
- 🎯 **Configurable accuracy**
- 🔌 **Full Prometheus compatibility** - works with existing Grafana dashboards and PromQL queries

ASAP sits between Prometheus and Grafana, intercepting queries and answering them using pre-computed streaming sketches instead of scanning raw data.

## Why ASAP?

### The Problem

Prometheus struggles with:
- **High-cardinality metrics** - Queries slow down as cardinality increases
- **Quantile queries** - Computing percentiles requires scanning massive amounts of data
- **Long time windows** - `quantile_over_time(...[1h])` can take seconds or fail
- **Memory pressure** - Storing raw samples for all time series

### The Solution

ASAP uses **streaming sketches** to:
1. **Pre-compute approximate summaries** as data arrives
2. **Answer queries in milliseconds** using compact sketches instead of raw data
3. **Bound memory usage** - sketches are fixed-size regardless of data volume
4. **Maintain accuracy** - configurable error bounds (typically <1% error)

## Key Concepts

### Sketches

**Sketches** are probabilistic data structures that provide approximate answers with bounded error. Think of them as compact summaries that capture the essential characteristics of data distributions.

Examples:
- **DDSketch** and **KLL Sketch** - For quantiles (P50, P95, P99)
- **Count-Min Sketch** and **CountSketch** - For frequency/sum estimation

Key properties:
- **Fixed size** - Memory usage doesn't grow with data volume or grows very slowly
- **Mergeable** - Can merge multiple sketches into a single sketch
- **Bounded error** - Guarantees on approximation quality (e.g., ±1% relative error)

### Streaming Pipelines

ASAP builds sketches in **real-time at the edge**, before data reaches the backend:

1. **Exporters / SDKs** emit metrics, scraped or pushed as **OTLP**
2. **asap-otel agents** run sketch processors that encode each metric as a sketch payload (DDSketch / KLL / HLL / CountSketch / Count-Min) — the metric name is **preserved**, not rewritten
3. A **gateway** merges sketches across agents and forwards them (OTLP) to the backend
4. **ASAPQuery-backend** ingests the sketch payloads into `SketchStore` and answers queries from them via `ASAPQueryEngine`

A **control plane** (the in-repo `control_plane/` planner) decides which sketches to compute and where, then pushes per-runtime config to the agents over **OpAMP**.

### Query Protocol

ASAP implements the **Prometheus HTTP API**, making it a drop-in replacement:

```
# Point Grafana to ASAP instead of Prometheus
Prometheus URL: http://asap:8088

# Use the same PromQL queries
quantile by (job) (0.99, http_request_duration_seconds)
```

Your existing dashboards work without modification!

### Fallback

Not all queries can be answered from sketches. ASAP automatically:

1. **Detects queries the warm sketch tier can't serve** (un-planned shapes, ad-hoc queries)
2. **Forwards them to the archive tier** — exact PromQL over Gorilla-compressed raw samples on object storage (via Thanos)
3. **Returns results transparently** to the user

This ensures full PromQL compatibility while accelerating what sketches can answer.

## High-Level Architecture

```
              Applications / Exporters / SDKs
                          │ OTLP
                          ▼
   ┌─────────────────────────────────────────────┐
   │  Edge (ASAPCollector)                        │
   │    asap-otel agents → gateway (sketch-merge) │
   │    sketch processors encode DDSketch / KLL / │
   │    HLL / CMS, preserving metric names        │
   └─────────────────────────────────────────────┘
                          │ OTLP (sketch payloads, raw metric names)
                          ▼
   ┌─────────────────────────────────────────────┐
   │  ASAPQuery-backend                           │
   │    control_plane: plans sketches, pushes     │
   │                   config to agents via OpAMP │
   │    data_plane (warm):    ASAPQueryEngine     │
   │                          over SketchStore    │
   │    data_plane (archive): ThanosQueryEngine ──┼──▶ Thanos + MinIO/S3
   │                          (exact raw PromQL)  │    (Gorilla-XOR)
   └─────────────────────────────────────────────┘
                          ▲ PromQL (Prometheus HTTP API)
                          │
                       Grafana
```
