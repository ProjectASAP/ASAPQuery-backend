# Bind Planner physical candidates without Backend operator lowering

Status: target design. The current Backend still performs the lowering described below.

## Problem

Backend shares Planner's operators and runtime, but it still reconstructs parts
of the computation. `QueryPlanNode` includes logical and relational operations;
control-plane query lowering creates those nodes; SQL and PromQL serving paths
compile some of them into physical fragments. This duplicates responsibility even
though execution eventually uses the shared library.

## Required boundary

```text
Logical Post-ASAP DAG + Summary Maintenance Lifecycle
    → Planner Physical Plan Compiler
    → supported Physical DAG candidates
    → Backend feasibility, workload costs and selection
    → Deployment Plan / DAG: PrecomputePlan + QueryPlan
    → bind concrete inputs and execute the selected Physical DAGs
```

`CompiledPhysicalDag` is an implementation type, not another architectural layer.
Backend must preserve candidate operators, dependencies, sharing, roots and
materialization boundaries. Runtime input binding may select eligible records;
it must not select algorithms, rebuild operators, or move work between phases.

## Implementation changes

| Component | Change |
| --- | --- |
| Planner handoff | Consume complete executable physical candidates and their typed boundaries; report missing Planner implementations explicitly |
| Deployment compiler | Price feasible candidates, select one, and install source/SDS bindings without logical lowering |
| Installed query | Store the selected physical computation and deployment metadata, removing the parallel Backend computation node representation |
| SQL execution | Remove request-time `ExecutableDagNode`, `compile_node` and physical graph reconstruction |
| PromQL execution | Move computation currently lowered by `residual.rs` and `logical_dag/native_values.rs` to Planner; retain protocol and input conversion |
| Precomputation | Bind the candidate's precompute graph and stored outputs without reclassifying operators by kind |
| Persistence | Update the installed-plan schema; reject incompatible stored plans explicitly rather than retaining a legacy execution path |

SDS semantic identity, deployed-output identity, same-version recovery, query-wide
revision pinning, freshness, correction horizons, request resource budgets and
terminal cancellation/resource errors remain deployment contracts.

A gap in Planner support must be fixed and tested in Planner before deleting the
corresponding Backend path. A missing connector is deployment infeasibility;
losing an operator's semantics in an adapter is not.

## Acceptance

1. Structural tests compare each selected candidate with its installed graph:
   operators, parameters, typed edges, shared producers, roots and frontiers remain
   unchanged. Include a shared-producer query ensemble.
2. Synthetic-cost tests reverse candidate selection without changing a candidate's
   computation or introducing unpriced work.
3. Deployment E2E tests compile, install, ingest, publish and query each supported
   candidate, checking exact results or admitted approximation guarantees.
4. Recovery tests preserve SDS identity and common-revision reads; a new plan
   version requires its own warm-up.
5. Request tests retain cancellation and memory exhaustion as terminal errors,
   including nested DAGs, range evaluation and concurrent revision publication.
6. Production serving modules do not import semantic lowering or compile nodes.
   Decoder validation and binding to typed sources remain permitted.

This work is complete only when the installed production path uses the handoff,
not when a new API or isolated test executes a physical fragment successfully.
