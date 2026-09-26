# Design documents

These documents are for architects and developers. The integration proposal and
SDS model below define the target Planner-to-runtime boundary. Acceptance
requirements and migration gates distinguish target behavior from completed
integration.

- [Planner/backend glossary](planner-backend-glossary.md) defines the terms used
  by the following three designs.
- [Binding Planner Physical DAGs to deployment plans](asapplanner-integration.md)
  defines how backend source/state bindings and operational policy instantiate
  Planner-provided maintenance and query computation.
- [Summary definitions table and SDS](summary-catalog-sds-architecture.md) owns definition
  and instance identity, version-scoped state references, read eligibility and
  committed-state metadata.
- [QueryPlan DAG execution](query-dag-execution.md) explains how the query
  engine evaluates installed query sub-DAGs and reads bound stored summaries.
- [Architecture migration delivery plan](asapplanner-migration-plan.md) defines
  common-library extraction, removal of ASAPCollector dependencies, the two-plan
  rollout, and backend acceptance/retirement gates.
- [Accepted-input completeness](continuous-summary-completeness.md) describes
  the backend's bounded admission, publication and recovery behavior.

Existing [Collector system contracts](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs)
remain the cross-component baseline. Current backend implementation guides live
under [developer docs](../developer_docs/README.md).

Other designs and profiles:

- [Evidence-dependent candidate selection](evidence-dependent-candidates.md) defines evidence ownership, logical selection, physical admission, exact fallback, and current proof limits.
- [ASAPQuery compatibility profile](asapquery-compatibility-profile.md)
- [Shape-aware ERP](shape-aware-erp-v1.md)
- [Empirical observability execution plan](empirical-o11y-execution-plan.md)
