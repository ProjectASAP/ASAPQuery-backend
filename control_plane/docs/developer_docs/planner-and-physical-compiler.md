# Planner adapter and physical compiler

> Implementation status: target integration; current modules are migration
> substrate and do not replace ASAPPlanner ownership.

## Purpose

This component consumes one ASAPPlanner-selected workload plan and compiles it
into matching collector and backend runtime plans. It must preserve Planner's
logical semantics while adding only ASAPQuery-backend-owned deployment choices.

Design sources:

- [ASAPPlanner integration](../asapplanner-integration.md)
- [Physical planning](../physical-planning.md)

## Component boundary

```text
workload + schema + logical constraints
                  |
                  v
          ASAPPlanner adapter
                  |
        selected workload DAG
                  |
                  v
          physical compiler
             /          \
            v            v
     CollectorPlan    BackendPlan
```

The adapter must use the pinned ASAPPlanner API directly. It must not recreate
Planner IR, query-to-summary rules, accuracy algebra, or candidate ranking in
backend-local types.

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| Workload pipeline coordination | [`pipeline.rs`](../../src/pipeline.rs) |
| Planner-facing workload/types during migration | [`workload.rs`](../../src/workload.rs), [`types_v2.rs`](../../src/types_v2.rs) |
| Physical allocation | [`physical/allocator.rs`](../../src/physical/allocator.rs) |
| Stage allocation and topology | [`physical/colored_dag/`](../../src/physical/colored_dag/) |
| Window realization | [`physical/window_fusion.rs`](../../src/physical/window_fusion.rs) |
| Runtime stage model and emitter | [`physical/colored_dag/emitter.rs`](../../src/physical/colored_dag/emitter.rs) |
| BackendPlan compilation | [`backend_plan/from_stage_config.rs`](../../src/backend_plan/from_stage_config.rs) |

The legacy `intent_algebra`, `sketch_algebra`, and optimizer modules are
migration inputs, not a second canonical Planner implementation. New logical
semantics belong in ASAPPlanner.

## Inputs

The compile operation requires:

- the complete selected Planner workload DAG, with shared nodes intact;
- the immutable Planner revision;
- collector and backend capability snapshots;
- deployment topology and tenant boundaries;
- physical window, freshness, retention, and transmission policy; and
- runtime statistics and resource constraints used for physical placement.

Missing capabilities or statistics stay unknown. Do not replace unknown values
with zero cost, exact accuracy, or universal support.

## Outputs

One compile produces one shared plan envelope plus:

- one CollectorPlan for each targeted collector; and
- one BackendPlan for the data plane.

Both sides must agree on materialization identity, family, parameters,
grouping, windows, representation, and transmission semantics. Generate them
from one in-memory compiled decision, never with independent reinterpretation.

## Implementation invariants

- Preserve workload-wide sharing; do not flatten to independent metric rows.
- Treat Planner result guarantees and exact fallback as authoritative.
- Reject an operation if its assigned executor lacks any required capability.
- Keep logical range semantics separate from physical pane size.
- Keep aggregation placement separate from logical grouping.
- Permit delta only when both endpoints share sequencing/checkpoint semantics.
- Make plan/materialization identity deterministic from semantic content.

## Adding a physical decision

When adding placement, window, representation, or transport behavior:

1. Confirm it does not change the selected logical result.
2. Add the capability input needed to make the decision safely.
3. Include every semantic effect in materialization or compatibility identity.
4. Emit the choice to both runtime plans where applicable.
5. Add a mismatch test showing incompatible plans fail before activation.

If the change selects a different summary or changes its guarantee, implement
it in ASAPPlanner instead.

## Required tests

- shared Planner nodes remain one logical materialization;
- collector and backend outputs carry identical semantic fields;
- unsupported capabilities fail closed;
- incompatible windows or delta semantics are rejected;
- deterministic input produces deterministic identities; and
- a three-query PromQL workload covers within-series, across-label, and combined
  time-plus-label aggregation shapes.
