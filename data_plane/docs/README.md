# ASAPQuery data-plane documentation

The data plane ingests state produced under an active BackendPlan, stores that
state, answers supported PromQL queries, and uses an explicit exact fallback
for unsupported queries. It executes plans; it does not choose summary families
or re-plan queries.

## Design documents

- [Plan-aware query execution](design_docs/query-execution.md) — ingestion,
  routing, readiness, summary readout, and exact fallback contracts.
- [Repository-wide summary storage](../../docs/design_docs/summary-storage.md) —
  materialization state, lifecycle, and query consistency.
- [BackendPlan](../../control_plane/docs/backend-plan.md) — the control-plane
  contract installed by the data plane.

## Developer documentation

- [Extension boundaries](developer_docs/extension-points.md) — responsibilities
  of protocol servers, adapters, and fallback clients.
- [BackendPlan runtime](developer_docs/backend-plan-runtime.md) — validation,
  staging, atomic installation, and snapshots.
- [OTLP summary ingestion](developer_docs/otlp-summary-ingestion.md) — decoding,
  plan validation, SID resolution, and full/delta handling.
- [Query routing and readout](developer_docs/query-routing-and-readout.md) —
  readiness, summary execution, and exact fallback.
- [Summary storage and series identity](developer_docs/summary-storage-and-series-identity.md)
  — store boundaries, identity hierarchy, lifecycle, and concurrency.
- [Adding a summary family](../../docs/developer_docs/adding-summary-family.md) —
  cross-repository prerequisites and backend validation.

## User guide

- [Querying ASAP](user_guide/querying-asap.md) — PromQL behavior, planned
  summary execution, exact fallback, freshness, and errors.

## Ownership

- [ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) owns logical query
  planning, query-to-summary mapping, and accuracy reasoning.
- The ASAPQuery control plane owns physical compilation and BackendPlan.
- The data plane owns ingestion, storage, readout, query execution, and exact
  fallback under the installed plan.
- [ASAPCollector](https://github.com/ProjectASAP/ASAPCollector) owns summary
  construction and transmission at the edge.

Historical ingestion paths, file-by-file migrations, and configuration
walkthroughs are not data-plane design contracts.
