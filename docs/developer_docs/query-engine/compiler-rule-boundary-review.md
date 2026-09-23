# Backend compiler rule-boundary review

Scope: ASAPQuery-backend main `877ea128`, plus PR #700 (`1dcf299f`) and its current correction. Reviewed implementation and callers, not documentation claims. Findings below are code-level boundary violations or coupling; no untested claim of production wrong answers.

Rechecked after merging main `674b9573` (#699): the four implementation files containing these five findings are unchanged; the legacy callers remain.

## Findings

1. **[P1] Legacy binding rewrites Rate into Increase before Planner selection.** `control_plane/src/physical/post_asap/lower.rs:171-214` calls `rewrite_rate_to_increase` before `select_summary`. This erases Rate from the selected logical IR and relies on downstream knowledge to recover division by the window. The caller in `control_plane/src/main.rs:1135-1162` remains reachable through the legacy algebra/typed-stage path. Share a physical counter implementation while preserving the logical Rate node/readout; if a logical rate-to-increase/divide transformation is needed, Planner must emit both operations. This is distinct from the main physical compiler's `physical_materialization_family`, which only maps the stored family and retains the selected readout.

2. **[P1] Legacy metric-name policy invents aggregation semantics.** `control_plane/src/physical/workload_planner.rs:23-38,126-143,180-282` maps `top_endpoint_qps` to TopK and constructs a synthetic per-endpoint Count aggregate. It also chooses Quantile/Cardinality/Frequency from names rather than solely from the canonical query. `main.rs:1161` and `main.rs:1958` still call this adapter. Planner checks the fabricated intent's legality, not whether it describes the submitted query. Restrict such examples to fixtures, or require an explicit typed workload intent; never choose a statistic from a metric name.

3. **[P2] Native residual lowering reruns Planner to rediscover the selected operator from text.** `control_plane/src/query_plan/logical.rs:537-620` parses original subexpressions and calls `select_summary_default` to construct an equality witness; `selected_aggregate_operator` then extracts the operation from that text. It fails closed on missing/ambiguous witnesses, which protects against blindly choosing an unrelated expression, but a valid selected DAG can become unlowerable when selection policy or representation changes. Carry sufficient explicit readout/operator information in Planner IR and lower that directly. The observed cross-series quantile failure in the old implementation was “native residual substitution requires an exact selected value”.


5. **[P2] Legacy binding owns an unconditional TimeRange/Aggregate swap.** `control_plane/src/physical/post_asap/lower.rs:120-141` turns `TimeRange(Aggregate(X))` into `Aggregate(TimeRange(X))` for any aggregate at that position. This is a logical tree rewrite, with no local legality condition for the measure or grouping. Move any required canonicalization and its legality proof to Planner/frontend; the backend should consume the canonical shape. Reachable through the same legacy binding entry as finding 1; no production failure was reproduced in this audit.

## PR #700 correction

The original backend `current_series.rs` parsed PromQL and independently assembled a maintained-state alternative. The correction moves recognition and workload sharing into Planner's `MaintainedPopulationStrategy`, adds typed maintenance/readout operators, removes `PlanningRequest.current_series`, and lowers the selected operators directly. Regression tests check that the installed Planner DAG contains the producer and that changing catalog text does not change physical operator binding.

## Within the backend's responsibility

- Complete workload quote comparison, placement, memory limits, input-lag bounds and runtime capability filtering.
- Choosing supported physical window implementations and subsets of retained materializations while preserving exact residual execution.
- Mapping Count/Sum or Rate/Increase to compatible physical storage while retaining the original logical readout.
- Explicit exact fallback for unsupported operators/accuracy/ERP evidence. A fallback may reduce acceleration coverage but is not itself a new logical rewrite rule.

The other findings are review follow-ups, not silently bundled fixes. Retiring the legacy binding path and removing text-based residual reconstruction should be separate, focused changes with compatibility tests.
