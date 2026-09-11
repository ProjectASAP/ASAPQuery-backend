# ASAPQuery-backend documentation

## Testing

- [E2E physical-DAG walkthrough for Milind](evaluation/e2e-physical-dag.md)

- [Manual component and whole-system E2E tests](03-how-to-guides/manual-e2e-tests.md)
- [ASAPQuery vs. AutoSketch evaluation plan](evaluation/autosketch-comparison.md)

## Canonical system design

The end-to-end architecture and shared contracts are maintained centrally in
[ASAPCollector design docs](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs).
That documentation owns collection, transmission, backend ingest/storage/query,
SID, physical plans, workload inputs, and runtime feedback.

This repository keeps code-owned implementation guidance, user-facing backend
operation material, and a scoped
[ASAPPlanner integration proposal](design_docs/asapplanner-integration.md)
covering architecture boundaries, workflow, and high-level migration without
duplicating the canonical cross-component contracts.

## Getting started

- [Overview](01-getting-started/overview.md)
- [Local setup](01-getting-started/local-setup.md)
- [Architecture orientation](01-getting-started/architecture.md)

## Backend implementation

- [ASAPQuery compatibility profile](design_docs/asapquery-compatibility-profile.md)
  - [Run the compatibility profile](user_guide/asapquery-profile.md)
- [Developer guide](developer_docs/README.md)
- [Summary store engine](developer_docs/summary-store-engine/implementation.md)
- [Query engine](developer_docs/query-engine/query-engine.md)
- [Ingest engine](developer_docs/ingest-engine/ingest-engine.md)
- [Summary series ID resolver](developer_docs/summary-series-id/resolver.md)
- [Control plane](developer_docs/control-plane/physical-compiler.md)

## User guide

- [User guide index](user_guide/README.md)
- [Run and verify the backend](user_guide/running-and-verifying.md)
- [Querying ASAP](user_guide/querying-asap.md)
