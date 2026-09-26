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
| Physical DAG | Planner-owned concrete operators, typed input boundaries, dependencies and roots; no storage identities or placement. |
| Deployment plan | System instantiation of physical computation with concrete source/state bindings and operational policy. |
| Summary producer | An operation or subgraph that builds summary state. Multiple queries may share its stored output. |
| `SummaryMaintenanceLifecyclePlan` | Planner result associating a post-ASAP root with selected maintenance requirements for its unique reachable summary producers, plus workload and costing context. |
| Selected deployment guarantee and schedule/retention | The selected `SummaryMaintenanceLifecycleGuarantee` for one producer, together with its concrete scheduling and retention binding. Planner supplies the selected lifecycle requirements; backend scheduling and retention must realize them without changing their semantics. |
| Maintenance | Work that constructs, refreshes or derives stored summary state, including batch rebuilds and incremental updates. |
| `PrecomputePlan` | Planner maintenance Physical DAGs plus backend input/output bindings, scheduling and publication policy. |
| `QueryPlan` | Planner query Physical DAGs plus backend input bindings, query/result associations and fallback policy. |
| Readout / `SummaryEstimate` | Operation that obtains a query value from summary state, such as p99 from KLL. |
| Derived summary state | Stored summary state computed from existing summary states. Earlier discussion calls this a “derived materialization”; it does not require a separate catalog object. |
| Exact residual | Part of the selected query computed exactly around summary operations, such as supported filtering or arithmetic after readout. It does not make the whole approximate result exact. |
| Exact fallback | Configured execution of the original query through an exact route when the summary plan cannot serve it. |

For example, merging five compatible one-minute KLL summaries and storing the
five-minute result produces derived summary state in a separate destination
stored output. Merging them only to answer a query is a query-time operation. Both require
compatible grouping, coverage and accuracy.

## State and identity

The storage contract has two stored objects: `SummaryDefinition` and
`StoredSummary`. `StoredOutputReference` belongs to installed boundary bindings,
not a third storage table. Metadata and payload form one logical stored record.
Migration of existing types and fields is covered in the
[migration plan](asapplanner-migration-plan.md).

| Term | Meaning |
| --- | --- |
| `summary_definitions` | Logical table inside `SummaryStore`: definition ID → `SummaryDefinition`. The compiler supplies a snapshot for validation and registration during installation. |
| `stored_summaries` | Logical table inside the same store: `(plan_version, stored_output_id, population_key, window)` → `StoredSummary`. |
| SDS (Self-Describing Summary) | The description and metadata needed to interpret and validate stored summary state. It is not a separate execution engine or payload store. |
| `SummaryDefinition` | What a summary represents: source/filter, input value, grouping, time semantics, algorithm and parameters. |
| `stored_output_id` | Compiler-assigned binding ID for a persisted PrecomputePlan DAG output within one plan version. Writers and shared readers use it to name the same output; it is not a memory slot or independent catalog object. |
| `StoredOutputReference` | Plan reference identifying a stored output and summary definition within the enclosing plan version. Reader configuration selects the required state instances and constrains format and coverage. |
| `StoredSummary` | One committed record containing instance metadata and payload, such as one service's completed five-minute KLL snapshot. |
| `SummaryStore` | One storage engine owning `summary_definitions` and `stored_summaries`, including definition rows, instance metadata and payload bytes. The current implementation is `SketchStore`; no separate metadata or payload service is required. |
| `plan_version` | Version shared by an installed plan bundle and its catalog bindings. Creating or updating state instances does not itself change this version. |
| Schema / encoding | Schema describes the state structure; encoding describes how that structure is represented as bytes. |
| Provenance | Mapping from physical plan operations back to the selected Planner computation. |

A materialization boundary is a Planner-selected physical output consumed
through typed inputs. Backend binding assigns its storage identity without
reclassifying logical nodes or changing that boundary.

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

## Compilation ownership

The [canonical Planner design at e9390031](https://github.com/ProjectASAP/ASAPPlanner/blob/e9390031fcecd7bc0d611127eddc5c6603a281e5/docs/design_docs/physical-planning-and-deployment.md)
defines physical lowering and frontier selection as Planner responsibilities.
The backend Deployment Plan Compiler binds declared physical inputs and outputs;
it does not reinterpret the logical DAG. Operator kind alone does not determine
maintenance versus query placement.
