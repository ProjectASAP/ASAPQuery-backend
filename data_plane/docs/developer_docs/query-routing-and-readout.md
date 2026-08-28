# Query routing and summary readout

> Implementation status: partial; summary execution and fallback exist while
> BackendPlan replaces legacy routing and local query-shape logic.

## Purpose

This component converts a Prometheus-compatible request into a BackendPlan route,
checks readiness, executes the declared summary readout and remaining operators,
or invokes the explicit exact fallback.

Design source: [Plan-aware query execution](../design_docs/query-execution.md).

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| ASAP query engine | [`asap_query_engine/engine.rs`](../../src/query_engines/asap_query_engine/engine.rs) |
| Live summary-serving gate | [`asap_query_engine/live_serve.rs`](../../src/query_engines/asap_query_engine/live_serve.rs) |
| Planner-node readout | [`asap_query_engine/l4_readout.rs`](../../src/query_engines/asap_query_engine/l4_readout.rs) |
| Summary executor contract | [`summary_exec.rs`](../../src/query_engines/asap_query_engine/summary_exec.rs), [`summary_executor.rs`](../../src/query_engines/asap_query_engine/summary_executor.rs) |
| Legacy routing during migration | [`routing/backend_storage_routing.rs`](../../src/query_engines/routing/backend_storage_routing.rs) |
| Engine/fallback dispatch | [`routing/query_engine_routing.rs`](../../src/query_engines/routing/query_engine_routing.rs) |
| Freshness probes | [`routing/freshness_probe_cache.rs`](../../src/query_engines/routing/freshness_probe_cache.rs) |

## Request flow

1. Adapter produces canonical query, evaluation time/range, tenant, and accuracy
   requirement.
2. Snapshot one active BackendPlan.
3. Match a route by canonical query capability and bound source—not by metric
   name alone.
4. Resolve referenced materializations and required logical windows.
5. Check plan identity, coverage, watermark, delta continuity, and producer
   completeness.
6. Execute the declared readout and remaining backend operators.
7. Align labels/timestamps and encode the Prometheus response.
8. If the plan selects exact fallback, forward the same logical request.

## Result contract

- Missing or extra series are not treated as zero or dropped.
- One response does not combine incompatible materializations/plan versions.
- Approximation metadata reflects the selected result guarantee.
- A partial interval is not returned as a complete answer.
- Unsupported/missing/stale state returns a typed miss/error or explicit exact
  route; it never returns a plausible summary value.

## Current migration boundary

`BackendStorageRouting`, backend capability matching, and some lowering helpers
still contain local query-shape logic. BackendPlan is the target authority.
Do not add a new query-to-summary rule to these legacy paths; add logical
support to ASAPPlanner and consume its selected readout.

## Adding a readout/operator

1. Confirm ASAPPlanner represents its exact/approximate semantics and guarantee.
2. Add backend capability advertisement.
3. Map the canonical Planner node to one executor operation without replanning.
4. Define label, timestamp, scalar/vector, and partial-coverage behavior.
5. Add exact-baseline comparison for the same PromQL and logical interval.

## Required tests

- the three MVP aggregation shapes;
- matching and deliberately mismatched labels/timestamps;
- incomplete/stale windows and delta gaps;
- exact versus approximate accuracy handling;
- unsupported query fallback and fallback failure; and
- concurrent plan swap does not mix versions in one response.
