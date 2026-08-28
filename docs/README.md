# ASAPQuery-backend documentation

## Getting started

- [Overview](01-getting-started/overview.md)
- [System architecture](01-getting-started/architecture.md)
- [Local setup](01-getting-started/local-setup.md)

## Component design

- [Control plane](../control_plane/docs/README.md)
- [Data plane](../data_plane/docs/README.md)
- [Summary storage](design_docs/summary-storage.md)
- [Series identity](design_docs/series-identity.md)
- [Future storage and compression](design_docs/future-storage-and-compression.md)

## Developer guides

- [Adding a summary family](developer_docs/adding-summary-family.md)
- [Data-plane extension boundaries](../data_plane/docs/developer_docs/extension-points.md)
- [Querying ASAP](../data_plane/docs/user_guide/querying-asap.md)

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
