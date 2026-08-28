# ASAP overview

## TL;DR

ASAP is a summary-based metrics pipeline. ASAPCollector maintains selected
summaries near the source, and ASAPQuery-backend answers planned PromQL queries
from those summaries. An exact backend remains available for query shapes or
time ranges that cannot be served correctly from maintained state.

## Why summaries

Exact metrics systems retain raw samples and repeatedly scan them for queries.
For selected recurring workloads, ASAP maintains compact reusable state as
samples arrive. This can reduce query work, transmission, and storage while
meeting a declared accuracy and freshness contract.

Summaries may be exact accumulators or approximate structures such as DDSketch,
KLL, HLL, Count-Min Sketch, or CountSketch. The term “summary” is broader than
“sketch” and includes both exact and approximate maintained state.

## Components

- **ASAPPlanner** chooses a valid logical plan for the complete query workload.
- **ASAPQuery control plane** compiles that selection into matching collector
  and backend plans and coordinates activation.
- **ASAPCollector** observes metrics, maintains the requested summaries, and
  transmits raw, full-summary, or delta-summary payloads.
- **ASAPQuery data plane** validates and stores those payloads, executes planned
  readouts, and exposes Prometheus-compatible query responses.
- **Exact backend** serves the explicit fallback path and provides the MVP
  correctness baseline.

## Correctness contract

A summary-backed answer is valid only when:

- it comes from the active plan and matching materialization;
- its labels and logical time range match the request;
- required windows are complete and fresh;
- its summary semantics satisfy the requested accuracy; and
- every required producer and delta sequence is compatible.

Otherwise the request fails or follows the configured exact route. Missing or
stale state is never treated as a zero-valued answer.

## MVP claim

The MVP is not merely that the components start. One reproducible paired run
must show functional correctness, bounded query error, fresh results, lower
query latency for claimed accelerated classes, and lower collector and total
resource cost than the raw exact baseline.
