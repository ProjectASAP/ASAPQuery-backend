# ASAPQuery-backend design documents

> Status: active

## TL;DR

These documents cover backend-owned storage and identity decisions. Planning
design lives in [control-plane docs](../../control_plane/docs/README.md), and
query serving lives in [data-plane docs](../../data_plane/docs/README.md).

| Document | Scope | Status |
| --- | --- | --- |
| [Summary storage](summary-storage.md) | Materialization state, windows, ingest/query consistency, and lifecycle. | Active MVP design |
| [Series identity](series-identity.md) | Canonical metric-series identity and recovery behavior. | Active MVP design |
| [Future storage and compression](future-storage-and-compression.md) | Current local persistence baseline plus compaction, remote tiers, backfill, service mode, and compression roadmap. | Mixed: local persistence active; remaining scopes future |
| [Backend service runtime](service-runtime.md) | End-to-end control/data-plane flow, ownership, failure behavior, and acceptance evidence. | Active MVP design |

Logical planning and summary accuracy algebra are owned by
[ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) and the relevant
summary libraries. Benchmark measurements belong in reproducible run artifacts,
not in these design documents.
