# ASAPQuery-backend documentation

## Getting started

- [Overview](01-getting-started/overview.md)
- [System architecture](01-getting-started/architecture.md)
- [Local setup](01-getting-started/local-setup.md)

## Component design

- [Design document index](design_docs/README.md)
- [Summary store engine](design_docs/summary-store-engine.md)
- [Query engine](design_docs/query-engine.md)
- [Ingest engine](design_docs/ingest-engine.md)
- [Summary series ID](design_docs/summary-series-id.md)
- [Control-plane physical planning](design_docs/control-plane-physical-planning.md)
- [Planner workload inputs](design_docs/control-plane-workload-inputs.md)
- [Runtime accuracy feedback](design_docs/control-plane-runtime-accuracy-feedback.md)
- [Future storage and compression](design_docs/future-storage-and-compression.md)
- [Backend service runtime](design_docs/service-runtime.md)

## Developer guides

- [Developer guide](developer_docs/README.md)
- [Adding a summary family](developer_docs/adding-summary-family.md)
- [Summary store engine](developer_docs/summary-store-engine.md)
- [Query engine](developer_docs/query-engine.md)
- [Ingest engine](developer_docs/ingest-engine.md)
- [Summary series ID](developer_docs/summary-series-id.md)
- [Control-plane physical compiler](developer_docs/control-plane-physical-compiler.md)
- [Planner workload inputs](developer_docs/control-plane-workload-inputs.md)
- [Runtime accuracy feedback](developer_docs/control-plane-runtime-accuracy-feedback.md)

## User guide

- [User guide index](user_guide/README.md)
- [Run and verify the backend](user_guide/running-and-verifying.md)
- [Querying ASAP](user_guide/querying-asap.md)

## Documentation ownership

This repository documents ASAPQuery-backend-specific physical planning,
runtime query execution, and storage contracts.

- Logical query planning and query-to-summary mapping belong to
  [ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner).
- Collector processing and CollectorPlan application belong to
  [ASAPCollector](https://github.com/ProjectASAP/ASAPCollector).
- Summary algorithm implementation and mathematical guarantees belong to the
  corresponding summary library.

Design documents link to those owners instead of maintaining parallel copies.
Benchmark outputs, migration histories, file-by-file implementation plans, and
future roadmaps are not active design specifications.
