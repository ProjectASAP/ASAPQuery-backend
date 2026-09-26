# Binding Planner Physical DAGs to Backend Deployment Plans

Status: target design. Audience: developers implementing deployment compilation
and the precompute/query engines. This document defines required behavior, not
completed backend integration.

## 1. Problem and goals

Precompute and query execution must agree on what state is produced, where it
is stored and which query inputs may consume it. A full logical DAG embedded in
PrecomputePlan obscures these boundaries. Reconstructing computation independently
in the backend also duplicates Planner's lowering and permits operator or
materialization decisions to diverge.

The backend will consume Planner-compiled Physical DAGs and bind their typed
boundaries into one coherent deployment plan. PrecomputePlan and QueryPlan reuse
those DAGs and the shared executor; they do not define another computation IR.

Goals:

- Preserve Planner's selected operators, sharing and materialization boundaries.
- Bind every physical input/output to an explicit source, stored output or result.
- Install matching producer and consumer contracts atomically.
- Execute through the shared physical library while keeping storage, scheduling,
  readiness and serving backend-owned.

Non-goals are backend operator lowering, a second maintenance-selection model,
CollectorPlan/TransmissionPlan compilation, distributed activation and new
transport protocols. Backend integration must not depend on ASAPCollector.

## 2. Architecture and ownership

The authoritative boundary is [Physical Planning, Summary Maintenance, and
Deployment at e9390031](https://github.com/ProjectASAP/ASAPPlanner/blob/e9390031fcecd7bc0d611127eddc5c6603a281e5/docs/design_docs/physical-planning-and-deployment.md).

```text
ASAPPlanner
  Logical Post-ASAP DAG + selected Summary Maintenance Lifecycle
      ↓ Physical Plan Compiler
  Physical DAGs + typed input/output boundaries
      ↓
Backend
  Deployment Plan Compiler + sources/store + operational policy
      ↓
  PrecomputePlan + QueryPlan + summary definitions
      ↓ atomic installation
  Deployment engines → shared physical executor
```

| Owner | Decisions |
| --- | --- |
| Planner logical and maintenance selection | Computation semantics, guarantees, window/retention/reuse requirements |
| Planner Physical Plan Compiler | Concrete operators, schemas, dependencies, roots, sharing and materialization frontiers |
| Backend Deployment Plan Compiler | Concrete source/state bindings, stored-output identities, placement, scheduling and installation version |
| Backend engines | Resolve inputs, drive execution, publish results, check actual readiness and apply installed fallback policy |
| Shared physical library | Operator execution, per-run sharing, backpressure, cancellation and resource contracts |
| SummaryStore | Committed definitions and stored records, lookup, recovery and reclamation |

Capabilities and scoped cost evidence flow from the backend to Planner selection.
Missing support makes a candidate unavailable. Deployment compilation validates
the selected realization; it does not repair an unsupported candidate by changing
operators, windows or boundaries. Such changes require replanning.

A maintenance lifecycle is a contract associated with computation, not another
operator IR. A deployment plan is an operational wrapper around Physical DAGs,
not another lowering stage.

## 3. Deployment plan structure

One installed version contains:

| Part | Content |
| --- | --- |
| Summary definitions | Semantic descriptions referenced by stored outputs |
| PrecomputePlan | Planner-provided maintenance Physical DAGs, input/output bindings, schedules and retention/publication policy |
| QueryPlan | Planner-provided query Physical DAGs, input bindings, query associations and explicit fallback policy |

A physical graph may be embedded or referenced within the bundle; either way,
its operator vocabulary and computation remain Planner-owned. The backend does
not copy it into a second set of Build/Merge/Estimate node variants.

Bindings attach only to declared physical boundaries:

```text
physical input slot → concrete raw source or stored-output reference
physical output     → persisted output or query result
```

The compiler assigns each persisted output a `stored_output_id` within the plan
version. Its writer and all readers refer to the same definition and compatible
format. A `StoredOutputReference` is a binding, not a separately managed catalog
object. [SDS](summary-catalog-sds-architecture.md) defines the storage contract.

Logical-to-physical provenance comes from Planner and remains available for
inspection. It does not drive backend semantic-node classification or re-lowering.
There is no backend `MaintenanceInput`/`QueryInput` decision in this target model.

## 4. Worked example: shared KLL state

Suppose p50 and p99 use KLL with `k=200` over aligned five-minute windows. Planner
selects one-minute panes and compiles:

```text
Maintenance Physical DAG             Query Physical DAG

raw-pane input                       compatible-pane input
      ↓                                      ↓
NativeKllBuild(k=200)                 NativeKllMerge(k=200)
      ↓                                  ┌───┴───┐
kll-state output                         ↓       ↓
                                     p50 readout p99 readout
```

The raw input must contain the complete one-minute population. The query input
requires compatible panes covering the requested aligned five-minute interval.
These are Planner contracts, not a backend decision to cut the logical graph.

The backend adds operational bindings. This YAML illustrates ownership and is
not a proposed Rust or wire schema:

```yaml
plan_version: 42
summary_definitions:
  - id: latency-kll-1m
    input: request_latency_seconds
    group_by: [service]
    population_interval: 1m
    algorithm: {kind: kll, k: 200}

precompute_plan:
  physical_dag: planner.maintenance_dag
  inputs:
    raw-pane: {source: latency_source, scope: scheduled_complete_pane}
  outputs:
    kll-state:
      reference: {stored_output_id: latency-panes, definition_id: latency-kll-1m}
      format: {schema: kll-v1, encoding: kll-binary-v1}
  schedule: {every: 1m, anchor: unix_epoch, require: complete_input}
  retention: {minimum: selected_lifecycle_requirement}

query_plan:
  physical_dag: planner.query_dag
  inputs:
    compatible-pane:
      reference: {stored_output_id: latency-panes, definition_id: latency-kll-1m}
      selection: complete_nonoverlapping_panes_for_requested_range
      expected_format: {schema: kll-v1, encoding: kll-binary-v1}
  outputs: {p50: query_p50, p99: query_p99}
  on_unready: unavailable
```

For `(12:00, 12:05]`, the query engine resolves five one-minute records for the
requested population, validates their format, coverage and revision compatibility,
and supplies them to the query DAG. Merge runs once for its two consumers within
that run. Separate query runs do not implicitly share mutable execution state.

Each maintained pane contributes once. Replacing a pane snapshot does not add
another population to a query merge. Missing, overlapping or incomplete panes
cannot be treated as the requested complete range.

A delayed build keeps its original coverage interval; publication time does not
change query semantics. If required state is missing, the installed fallback or
unavailability policy applies. The backend must not substitute older state or
change the maintained window to make a read succeed.

## 5. Deployment compilation contract

Inputs are selected Physical DAGs and boundaries, the selected lifecycle,
query associations, source/store capabilities and installation context.
Compilation opens no readers and does not establish future state readiness.

For each selected physical candidate, the compiler:

1. Verifies that the backend can supply every input and operate the selected
   maintenance requirements without changing their semantics.
2. Binds raw inputs and assigns identities to persisted physical outputs.
3. Connects stored-state inputs to those outputs, with matching definitions,
   grouping, coverage rules, revision scope and supported format.
4. Binds schedules and retention that satisfy the selected lifecycle, then
   packages the provided DAGs and bindings into precompute/query plans.
5. Validates the complete bundle before it can be staged.

Backend feasibility includes persisting the selected output type. Planner may
produce scalar/result frontiers as well as sketches; this does not imply the
backend supports all of them. An unsupported output is rejected or excluded
through Planner feasibility selection, never silently replaced with another
frontier.

Build, merge and readout are reusable operators, not deployment-phase classes.
A query-only candidate can build state during a query; a precompute candidate
can finalize values before persisting them. The backend follows the selected
Physical DAGs rather than enforcing build-only/estimate-only phase rules.

## 6. Installation and execution contracts

| Contract | Requirement |
| --- | --- |
| Preserve computation | Binding does not change physical operators, ordered edges, roots or sharing. |
| Bind completely | Every required boundary resolves to one compatible input/output contract. |
| Install atomically | Definitions and both plans become active as one version; failed staging leaves the previous version active. |
| Distinguish readiness | Installation authorizes a plan; actual state coverage and readiness are checked when resolving inputs. |
| Execute once per run | Shared physical producers are driven by the shared runtime, not duplicated by separate backend traversals. |
| Publish consistently | Stored metadata and payload become visible together under the authorized output binding. |
| Fail explicitly | Unsupported bindings or unreadable state follow rejection, fallback or unavailability policy without changing computation. |

The precompute engine schedules work, resolves bounded inputs, invokes the shared
executor and commits output. The query engine resolves request-specific inputs,
invokes the same executor and adapts results. Both propagate cancellation and
resource limits. Neither interprets logical Post-ASAP nodes at runtime.

Cleanup respects retention and active readers/dependent producers. Storage lookup
uses installed references; it does not search for an alternative summary at
serving time. See SDS for record eligibility and recovery requirements.

## 7. Alternatives and tradeoffs

Re-lowering logical nodes in the backend would duplicate physical selection and
allow deployment and Planner graphs to drift. Consuming Physical DAGs avoids that
second compiler, at the cost of requiring an explicit capability/replanning
boundary when the backend cannot realize a candidate.

Keeping one full logical DAG under PrecomputePlan would require runtime phase
filtering and obscure which inputs are stored. Separate Planner-provided physical
subgraphs make execution ownership explicit without inventing separate operator
systems for precompute and queries.

A separate catalog Materialization object would repeat fields already owned by
definitions, boundary bindings and stored records. Two stored object types and
plan-local references are sufficient for the selected scope.

## 8. Validation and acceptance

Tests must establish:

1. Deployment binding preserves Planner's operators, boundaries and shared
   dependencies; unsupported bindings fail before activation.
2. The KLL example writes one pane population once and serves both readouts with
   one merge per shared run. Missing/overlapping panes and incompatible revisions
   fail read eligibility.
3. A supported query-only build and precomputed readout/result follow their
   selected phases. Unsupported persisted types are rejected explicitly.
4. Multiple queries can reference one producer, and one query can consume multiple
   compatible outputs. Derived maintenance checks its source completeness.
5. Compilation, installation and runtime agree on identity, schema and version.
   Staging failure, restart and version switching preserve consistency.
6. The complete path runs without an ASAPCollector checkout or process.

These are acceptance requirements, not claims of completed deployment tests.
The [migration plan](asapplanner-migration-plan.md) defines delivery gates.

## 9. Scope and follow-up work

Backend work binds and operates Planner computation. It does not add an execution
IR, alter the Planner API's ownership, or introduce another maintenance model.
Distributed activation, Collector and transmission plans, and new checkpoint
protocols remain separate work. Changes to physical algorithms or materialization
frontiers belong in Planner and its shared physical library.
