# Planner/backend design glossary

For developers reading the [integration](asapplanner-integration.md),
[SDS](summary-catalog-sds-architecture.md), and
[migration](asapplanner-migration-plan.md) designs. Definitions describe the
proposed boundary; they do not imply that every proposed field already exists
in the serialized API.

## Computation and execution

| Term | Meaning |
| --- | --- |
| Selected post-ASAP DAG | Planner-selected computation graph, including summary producers, shared dependencies and query readouts. |
| Summary producer | An operation or subgraph that builds summary state. Multiple queries may share its stored output. |
| `SummaryMaintenanceLifecyclePlan` | Planner result associating a post-ASAP root with deployment decisions for its unique reachable summary producers, plus workload and costing context. |
| Selected deployment guarantee and schedule/retention | The selected `SummaryMaintenanceLifecycleGuarantee` for one producer, together with its concrete scheduling and retention binding. This is part of a deployment in `SummaryMaintenanceLifecyclePlan`, not a separate model. |
| Maintenance | Work that constructs, refreshes or derives stored summary state, including batch rebuilds and incremental updates. |
| `PrecomputePlan` | Backend executable plan for maintenance and state writes. |
| `QueryPlan` | Backend executable plan for state reads, query readouts and remaining query operations. |
| Readout / `SummaryEstimate` | Operation that obtains a query value from summary state, such as p99 from KLL. |
| Derived summary state | Stored summary state computed from existing summary states. Earlier discussion calls this a “derived materialization”; it does not require a separate catalog object. |
| Exact residual | Part of the selected query computed exactly around summary operations, such as supported filtering or arithmetic after readout. It does not make the whole approximate result exact. |
| Exact fallback | Configured execution of the original query through an exact route when the summary plan cannot serve it. |

For example, merging five compatible one-minute KLL summaries and storing the
five-minute result produces derived summary state in a separate destination
stored output. Merging them only to answer a query is a query-time operation. Both require
compatible grouping, coverage and accuracy.

## State and identity

| Term | Meaning |
| --- | --- |
| Summary Catalog | Definition snapshot validated with the installed plan; it does not track runtime instances or hold payload bytes. |
| SDS (Self-Describing Summary) | The description and metadata needed to interpret and validate stored summary state. It is not a separate execution engine or payload store. |
| `SummaryDefinition` | What a summary represents: source/filter, input value, grouping, time semantics, algorithm and parameters. |
| `stored_output_id` | Compiler-assigned binding ID for a persisted PrecomputePlan DAG output within one plan version. Writers and shared readers use it to name the same output; it is not a memory slot or independent catalog object. |
| `StateReference` | Plan reference identifying a stored output and summary definition within the enclosing plan version. Reader configuration selects the required state instances and constrains format and coverage. |
| `SummaryStateInstance` | A concrete stored state, such as one service's completed five-minute KLL snapshot, with partition, coverage, format and location metadata. |
| `SummaryStore` | One runtime store for summary instance metadata and payload bytes. Its metadata indexes instances and records coverage, format, readiness and payload location. The current implementation is `SketchStore`; no separate metadata or payload service is required. |
| `plan_version` | Version shared by an installed plan bundle and its catalog bindings. Creating or updating state instances does not itself change this version. |
| Schema / encoding | Schema describes the state structure; encoding describes how that structure is represented as bytes. |
| Provenance | Mapping from physical plan operations back to the selected Planner computation. |

The existing `BackendNodeBinding::Materialization` marks a node whose output is
stored. It remains a node binding; this design has no standalone catalog
`Materialization` object. A materialization boundary is simply where a producer
writes stored state and a consumer reads it.

## Time, selection and validation

| Term | Meaning |
| --- | --- |
| Logical range | Input interval required by the computation. In the example, `range: 5m` means `(T - 5m, T]` at evaluation time `T`. |
| Pane | Physical time partition of stored state. Several compatible panes may serve one logical range; pane size need not equal that range. |
| Refresh cadence | How often the producer is scheduled to build or refresh state. |
| Retention | How long state remains available; distinct from its input range and refresh cadence. |
| Readiness | Whether the required state is available with valid format and sufficient coverage/completeness for a read. Plan installation alone does not establish readiness. |
| Backend capability | Declaration of supported implementation combinations: algorithm/parameters, maintenance mode, input kind, window behavior and format. |
| Physical cost evidence | Scoped measurements or estimates used to compare executable alternatives; includes workload and implementation context. |
| Compiler contract | Required inputs, outputs, validation rules and guarantees, including matching writer/reader definitions, formats, partitions and plan versions. |
