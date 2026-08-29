# Ingest and precompute engine

> Status: active

## Responsibility

The ingest engine accepts raw or already-materialized telemetry, validates its
runtime contract, and produces windowed state for the summary store. It owns
protocol decoding, series resolution, buffering, window assignment,
accumulator execution, and storage writes. It does not choose the logical
summary or route PromQL queries.

## Inputs and outputs

```text
Collector/raw producer
  -> OTLP gRPC :4317 or OTLP HTTP :4318/v1/metrics
  -> decode + series-ID resolution
  -> match installed aggregation configuration
  -> buffer/order by stored-series SID
  -> accumulate raw input or accept materialized state
  -> emit windowed payload to SummaryStore
```

The input contract is the installed BackendPlan/streaming configuration plus
modified OTLP. A materialized input contains its summary family, parameters,
encoding, stored labels, window, SID evidence, and payload. Raw input contains
the observations and labels needed by a backend-side precompute job.

Output is one or more `(sid, logical window, retained label values, payload)`
records. Sketch payloads retain full/delta encoding; exact aggregations retain
their accumulator type.

## Two ingest paths

### Materialized-at-Collector

ASAPCollector applies the CollectorPlan, builds a summary, and sends modified
OTLP. Backend validates family/config and resolves the stored-series SID before
appending the opaque state. It must not run the same aggregation over an
already-materialized payload.

### Precompute-at-Backend

Raw samples match an installed aggregation configuration. `SeriesRouter`
partitions them by SID, workers maintain per-series buffers and window managers,
and the accumulator factory creates the configured sketch or exact aggregate.
Completed/flushable outputs are written through `SketchStoreSink`.

## Correctness rules

- The physical plan decides whether aggregation occurs at Collector or Backend.
- One input is processed exactly once by the selected path.
- Window boundaries, lateness, grouping, family, parameters, and filter match
  the installed configuration.
- Writes to retired SIDs are rejected.
- Unknown ID-only frames request SID re-registration instead of guessing.
- Buffer or channel exhaustion is visible; it cannot silently drop accepted
  data while reporting complete coverage.

## Configuration and lifecycle

The data-plane binary configures worker count, channel capacity, maximum
per-series buffer, allowed lateness, flush interval, OTLP listeners, and storage
sink. Configuration replacement reconciles active policies and SIDs. A worker
finishes or rejects state according to the declared cutover rather than mixing
old and new parameters in one window.

## Acceptance behavior

Tests cover raw and materialized input, every supported family, grouping,
out-of-order/late points, boundary timestamps, full/delta frames, unknown SID,
retired SID, backpressure, and configuration cutover. An end-to-end test must
start from Collector-produced OTLP and prove the stored windows and query result
match the physical plan.

Developer implementation details are in
[Developing the ingest engine](README.md).
