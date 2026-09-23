# Planner selection contract

The backend pins ASAPPlanner `2ec3fc80` and uses its candidate inventory,
global selection and selected-DAG assembly for both individual queries and
workloads. Enumeration order does not authorize deployment. Unknown accuracy
remains visible in explain output and cannot certify a summary; physical
compilation independently rejects directly supplied uncertified readouts.

`ControlPlaneCostModel::candidate_cost` supplies a numerical analytical estimate
of retained state bytes per partition. Shared producer nodes are counted once.
Unsupported shapes remain uncosted. This estimate is a local selection input,
not a complete workload quote. This baseline does not enable uncosted legacy
selection. ERP resource matching and backend-computed workload costs belong to
the subsequent evidence/costing change (#761).

The public report uses `candidates`, `candidate_id` and
`physical_candidate_id`. Callers use the domain types (`CompiledPhysicalPlan`,
`PhysicalCompilationRequest`, `CandidatePlanEvaluation`) and explicit frontend
methods (`compile_promql` / `compile_metricsql`). Deprecated aliases, the
first-candidate selection helpers and the `query_plan::logical` re-export are
removed; callers use `query_plan::residual` directly.

The snapshot has one serialized contract: `snapshot_version`, `implementation`
and `environment.collector_ids`; removed alternate spellings are rejected.
An absent memory limit uses the documented default and is not a schema-version
compatibility path. Exact execution remains a normal planning outcome when no
certified, costed summary is selected.
