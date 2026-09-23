# ASAP physical operators

Shared Rust kernels for ingestion time and query time execution. Used by
ASAPQuery-backend; other deployments can depend on the `asap-physical-operators`
package in this Git repository. No backend server, storage implementation,
background worker, Arrow or DataFusion version is required.

The public `planner` export names the exact Planner types used by the library.
`capability::validate_summary_kernel` checks family, parameters, update layout
and grouping without allocating state. `factory::create_planner_accumulator`
constructs that same kernel. A compiler should validate before accepting the
operator; a caller then supplies evaluated updates, merges compatible states,
and invokes the state's typed readout. Neither operation requires persistence.

```rust
use asap_physical_operators::{factory::create_planner_accumulator, Statistic};
use asap_physical_operators::planner::{
    post_asap::{ExactKind, ExactParams, SummaryFamilyType, SummaryUpdate},
    pre_asap::ColumnRef,
};

let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
let mut operator = create_planner_accumulator(
    &family,
    &SummaryUpdate::column(ColumnRef::SampleValue),
    &Default::default(),
).unwrap();
operator.validate_single_input(3.0).unwrap();
operator.update_single(3.0, 1000);
let state = operator.into_accumulator();
assert_eq!(state.query_statistic(Statistic::Sum, &None, &Default::default()).unwrap(), 3.0);
```

The crate contains exact and sketch accumulators, common state traits, the
Planner-family factory, Float64 arithmetic and synchronous/asynchronous
QueryPlan DAG traversal. Unsupported kernel families are errors. It does not
infer new plans, choose a fallback engine, promise arbitrary SQL/PromQL support,
or change the execution phase encoded by Planner. Kernel availability does not
certify an accuracy guarantee; Planner and the deployment must still validate
the requested accuracy and evidence scope.

Deployment adapters supply storage, source rows, time/population scope, expression
evaluation, I/O and output representation. Planner's current maintenance-only
summary placement and the backend's missing local raw Scan remain separate
integration limitations; exporting these kernels does not silently bypass them.

## Composable row operators

`rows::membership_filter` performs a value-preserving semijoin and reports
missing membership keys. `rows::grouped_topk` independently ranks rows within
groups, retaining input order for ties and placing NaN after numeric values.
Neither kernel knows about sketches, storage, execution phase or external
queries. The deployment enforces the pruning certificate; filtering and ranking
remain separate operations in the installed graph.

## Shared DAG execution design

The target is an independently implemented ASAP DAG runtime and physical
operator library used by both the precompute engine and the query engine.
DataFusion is a design reference, not the execution framework. The current kernels and backend-driven traversal are not yet
that implementation. See the [design document](../../docs/design_docs/query-dag-execution.md)
for operator responsibilities, engine integration, and shared-dependency semantics.
