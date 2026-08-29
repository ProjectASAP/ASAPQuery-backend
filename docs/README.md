# ASAPQuery-backend documentation

## Canonical system design

The end-to-end architecture and shared contracts are maintained centrally in
[ASAPCollector design docs](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs).
That documentation owns collection, transmission, backend ingest/storage/query,
SID, physical plans, workload inputs, and runtime feedback.

This repository keeps only code-owned implementation guidance and user-facing
backend operation material.

## Getting started

- [Overview](01-getting-started/overview.md)
- [Local setup](01-getting-started/local-setup.md)
- [Architecture orientation](01-getting-started/architecture.md)

## Backend implementation

- [Developer guide](developer_docs/README.md)
- [Summary store engine](developer_docs/summary-store-engine/README.md)
- [Query engine](developer_docs/query-engine/README.md)
- [Ingest engine](developer_docs/ingest-engine/README.md)
- [Summary series ID resolver](developer_docs/summary-series-id/README.md)
- [Control plane](developer_docs/control-plane/physical-compiler.md)
- [Runtime](developer_docs/runtime/service-runtime-design.md)

## User guide

- [User guide index](user_guide/README.md)
- [Run and verify the backend](user_guide/running-and-verifying.md)
- [Querying ASAP](user_guide/querying-asap.md)
