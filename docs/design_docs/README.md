# Design documents

These documents are for architects and developers. The integration proposal and
SDS model below define the target Planner-to-runtime boundary; their current-code
notes and migration gates distinguish implemented behavior from proposed changes.

- [Planner/backend glossary](planner-backend-glossary.md) defines the terms used
  by the following three designs.
- [Planner output to backend physical plans](asapplanner-integration.md) defines
  how one selected post-ASAP DAG becomes executable PrecomputePlan and QueryPlan
  subgraphs joined at materialization boundaries.
- [Summary definitions table and SDS](summary-catalog-sds-architecture.md) owns definition
  and instance identity, version-scoped state references, readiness and state
  lifecycle semantics.
- [QueryPlan DAG execution](query-dag-execution.md) explains how the query
  engine evaluates installed query sub-DAGs and reads bound stored summaries.
- [Architecture migration delivery plan](asapplanner-migration-plan.md) defines
  common-library extraction, removal of ASAPCollector dependencies, the two-plan
  rollout, and backend acceptance/retirement gates.
- [Accepted-input completeness](continuous-summary-completeness.md) describes
  the backend's bounded admission, publication and recovery behavior.

- [Planner selection contract](planner-selection-contract.md) defines the current
  candidate API, accuracy admission and removal of obsolete selection adapters.

Existing [Collector system contracts](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs)
remain the cross-component baseline. Current backend implementation guides live
under [developer docs](../developer_docs/README.md).

Other designs and profiles:

- [Evidence-dependent candidate selection](evidence-dependent-candidates.md) defines evidence ownership, logical selection, physical admission, exact fallback, and current proof limits.
- [ASAPQuery compatibility profile](asapquery-compatibility-profile.md)
- [Shape-aware ERP](shape-aware-erp-v1.md)
- [Empirical observability execution plan](empirical-o11y-execution-plan.md)
