# ASAPQuery-backend developer documents

These pages are grouped by code component. Canonical system behavior and
cross-repository interfaces live in
[ASAPCollector/docs](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs).

## Summary store engine

- [Storage implementation and SID internals](summary-store-engine/implementation.md)

## Query engine

- [Routing, readout, and query-engine implementation](query-engine/query-engine.md)
- [Catalog-backed physical-plan runtime](query-engine/catalog-physical-plan-runtime.md)
- [Extension points](query-engine/extension-points.md)
- [VictoriaMetrics and MetricsQL support](query-engine/victoriametrics-metricsql-support.md)

## Ingest engine

- [OTLP ingest-engine implementation](ingest-engine/ingest-engine.md)

## Summary series ID

- [Resolver guide](summary-series-id/resolver.md)

## Control plane

- [Physical compiler](control-plane/physical-compiler.md)
- [Plan publication](control-plane/plan-publication.md)
- [Workload inputs](control-plane/workload-inputs.md)
- [Runtime accuracy feedback](control-plane/runtime-accuracy-feedback.md)

## Cross-component

- [Adding a summary family](cross-component-adding-summary-family.md)
