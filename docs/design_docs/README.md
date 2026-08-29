# ASAPQuery-backend design documents

> Status: active

## TL;DR

All backend design documentation lives below `docs/design_docs`; the old
component-local documentation trees are intentionally consolidated here.

| Document | Scope | Status |
| --- | --- | --- |
| [Summary store engine](summary-store-engine.md) | Materialization state, indexing, metadata, windows, persistence, and lifecycle. | Active MVP design |
| [Query engine](query-engine.md) | Query execution, Planner boundary, physical configuration, and readout. | Active MVP design |
| [Ingest engine](ingest-engine.md) | Collector-produced ingest and backend-side precomputation. | Active MVP design |
| [Summary series ID](summary-series-id.md) | Materialization identity across collection, transmission, storage, and analytics. | Active MVP design |
| [Physical planning](control-plane-physical-planning.md) | Planner-to-physical-plan compilation and separate Collector/Backend plans. | Active MVP design |
| [Planner workload inputs](control-plane-workload-inputs.md) | Query workload, data workload, horizon, and deployment inventory. | Active contract |
| [Runtime accuracy feedback](control-plane-runtime-accuracy-feedback.md) | Empirical evidence collection and replanning loop. | Proposed extension |
| [Future storage and compression](future-storage-and-compression.md) | Current local persistence baseline plus compaction, remote tiers, backfill, service mode, and compression roadmap. | Mixed: local persistence active; remaining scopes future |
| [Backend service runtime](service-runtime.md) | End-to-end control/data-plane flow, ownership, failure behavior, and acceptance evidence. | Active MVP design |

Logical planning and summary accuracy algebra are owned by
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) and the relevant
summary libraries. Benchmark measurements belong in reproducible run artifacts,
not in these design documents.
