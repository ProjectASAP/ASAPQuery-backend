# ASAPQuery data-plane documentation

> Status: active

## TL;DR

The ASAPQuery data plane ingests state produced under an active BackendPlan,
stores that state, answers supported PromQL queries, and uses an explicit exact
fallback for unsupported queries. It executes plans; it does not choose summary
families or re-plan queries.

## Documents

| Document | Scope |
| --- | --- |
| [Query execution](query-execution.md) | Ingestion, plan-aware routing, summary readout, freshness, and exact fallback. |
| [Extension points](extension-points.md) | Stable responsibilities of protocol adapters, protocol servers, and fallback clients. |

Storage and identity designs live in the repository-wide
[design documents](../../docs/design_docs/README.md). The control-plane to
data-plane contract is [BackendPlan](../../control_plane/docs/backend-plan.md).

## Ownership rule

- [ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) owns logical query
  planning, query-to-summary mapping, and accuracy reasoning.
- The ASAPQuery control plane owns physical compilation and BackendPlan.
- The data plane owns ingestion, storage, readout, query execution, and exact
  fallback under that installed plan.
- [ASAPCollector](https://github.com/ProjectASAP/ASAPCollector) owns summary
  construction and transmission at the edge.

Documents in this directory describe only the data-plane boundary. Historical
ingestion paths, file-by-file migrations, and configuration walkthroughs are
not design contracts.
