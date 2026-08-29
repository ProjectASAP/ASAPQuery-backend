# ASAPQuery-backend developer documents

These pages are grouped by code component. Canonical system behavior and
cross-repository interfaces live in
[ASAPCollector/docs](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs).

## Summary store engine

- [Developer entry](summary-store-engine/README.md)
- [Implementation design](summary-store-engine/design.md)
- [Storage and SID internals](summary-store-engine/storage-and-series-id.md)
- [Future storage and compression](summary-store-engine/future-storage-and-compression.md)

## Query engine

- [Developer entry](query-engine/README.md)
- [Implementation design](query-engine/design.md)
- [BackendPlan runtime](query-engine/backend-plan-runtime.md)
- [Routing and readout](query-engine/routing-and-readout.md)
- [Extension points](query-engine/extension-points.md)

## Ingest engine

- [Developer entry](ingest-engine/README.md)
- [Implementation design](ingest-engine/design.md)
- [OTLP summary ingestion](ingest-engine/otlp-summary-ingestion.md)

## Summary series ID

- [Resolver guide](summary-series-id/README.md)
- [Implementation design](summary-series-id/design.md)

## Control plane

- [Physical compiler](control-plane/physical-compiler.md)
- [Plan publication](control-plane/plan-publication.md)
- [BackendPlan design](control-plane/backend-plan-design.md)
- [Physical-planning design](control-plane/physical-planning-design.md)
- [Planner integration](control-plane/planner-integration-design.md)
- [Workload inputs](control-plane/workload-inputs.md)
- [Runtime accuracy feedback](control-plane/runtime-accuracy-feedback.md)

## Cross-component

- [Adding a summary family](cross-component-adding-summary-family.md)
- [Service runtime](runtime/service-runtime-design.md)
