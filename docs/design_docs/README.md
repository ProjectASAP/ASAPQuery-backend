# System design location

ASAPQuery-backend does not maintain a second copy of the system design.
The canonical component design is in
[ASAPCollector/docs/design_docs](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs).

Backend-specific implementation design notes are organized by component under
[`../developer_docs`](../developer_docs/README.md). They explain current Rust
internals and are subordinate to the shared system contracts.

Proposals for shared-contract review:

- [ASAPPlanner integration architecture](asapplanner-integration.md) proposes
  the Planner/backend responsibility boundary, shared semantic DAG workflow,
  and high-level consolidation milestones.
- [Summary Catalog and SDS Architecture](summary-catalog-sds-architecture.md) defines the proposed
  Summary Descriptor, Data Descriptor and Summary Instance layers.

These proposals complement the canonical cross-component contracts above.

Backend planning decisions:

- [Evidence-dependent candidate selection](evidence-dependent-candidates.md)
  defines evidence ownership, logical selection, physical admission, exact
  fallback and the current HLL/ERP proof limits.

Backend-specific operating profiles:

- [ASAPQuery compatibility profile](asapquery-compatibility-profile.md) defines
  the smaller target configuration for Prometheus Remote Write, backend-local
  precompute, and PromQL serving without ASAPCollector. It becomes a strict
  configuration subset after its currently missing Remote Write adapter lands.
