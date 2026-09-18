# Design documents

These documents are for architects and developers. The integration proposal and
SDS model below define the target Planner-to-runtime boundary; their current-code
notes and migration gates distinguish implemented behavior from proposed changes.

- [Planner, physical plans, SDS, and runtime architecture](asapplanner-integration.md)
  owns semantic/physical compilation, common bindings, the four plan projections,
  policy ownership, codec boundaries, and publication/activation requirements.
- [Summary Catalog and SDS](summary-catalog-sds-architecture.md) owns descriptors,
  definition/instance identity, state references, inventory and lifecycle semantics.
- [Architecture migration delivery plan](asapplanner-migration-plan.md) defines
  the current PrecomputePlan/QueryPlan scope, common-library extraction, removal
  of ASAPCollector dependencies, and backend acceptance/retirement gates. Collector
  and transmission plan changes are deferred.
- [Accepted-input completeness](continuous-summary-completeness.md) describes
  the backend's bounded admission, publication and recovery behavior.

Existing [Collector system contracts](https://github.com/ProjectASAP/ASAPCollector/tree/main/docs/design_docs)
remain the cross-component compatibility baseline until coordinated migrations
land. These proposals do not silently change those interfaces. Current backend
implementation guides live under [developer docs](../developer_docs/README.md).

Other designs and profiles:

- [ASAPQuery compatibility profile](asapquery-compatibility-profile.md)
- [Shape-aware ERP](shape-aware-erp-v1.md)
- [Empirical observability execution plan](empirical-o11y-execution-plan.md)
