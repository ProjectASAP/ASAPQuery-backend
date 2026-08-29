# BackendPlan: control-plane to data-plane contract

> Status: proposed
>
> MVP relation: required for the ASAPQuery data plane to ingest and query the
> state selected by the control plane.
>
> Scope: the typed runtime contract by which ASAPQuery-backend's control
> plane tells its data plane what summary materializations exist, how they
> are ingested, and which query capabilities they serve.

Developer guides:
[runtime plan publication](plan-publication.md) and
[BackendPlan installation](../query-engine/backend-plan-runtime.md).

## TL;DR

Planning chooses once; serving reuses that exact decision.

`BackendPlan` is the backend half of a `CompiledPlan`. It describes the state
that matching CollectorPlans produce and the readout/routing decisions the
data plane must apply. It prevents the data plane from independently guessing
summary family, parameters, grouping, windows, accuracy, or storage.

```text
selected Planner DAG
        |
        v
physical compile
   /           \
CollectorPlan  BackendPlan
   |           |
summary state  ingest + route + readout
```

## 1. Why BackendPlan exists

Control-plane planning and data-plane serving happen in different processes
and at different times. They must nevertheless agree on:

- which materializations should exist;
- their exact summary state types;
- where their payloads come from;
- which windows and groups they represent;
- which queries/readouts they can answer;
- which guarantees apply; and
- which plan version is active.

Without a typed plan, serving tends to reconstruct decisions from query text,
hard-coded defaults, or observed storage metadata. That can silently select a
different algorithm or parameters, miss valid state, merge incompatible
state, or serve under the wrong accuracy contract.

BackendPlan makes the control-plane decision authoritative.

## 2. Relationship to other plans

ASAPPlanner supplies the selected logical post-ASAP DAG. ASAPQuery's physical
compiler creates:

- CollectorPlan, which constructs and transmits materializations; and
- BackendPlan, which validates, stores, routes, merges, and reads them.

The two are defined together in
[`physical-planning.md`](physical-planning-design.md).

BackendPlan is not:

- a copy of ASAPPlanner's internal DAG;
- a query-language AST;
- a collector configuration;
- a storage inventory discovered after planning; or
- an explain/viewer artifact.

## 3. Plan envelope

Every BackendPlan contains:

| Field | Meaning |
| --- | --- |
| `plan_id` | Identity shared with every CollectorPlan compiled from the same decision. |
| `plan_version` | Ordered version within that plan identity. |
| `activation` | Earliest time this plan may serve queries. |
| `expiry` | Optional time after which this plan is invalid. |
| `backend_compat` | Backend-plan and summary-state schema compatibility identity. |
| `generated_at` | Time the control plane emitted the plan. |
| `planner_revision` | Immutable Planner revision that produced the logical selection. |

The data plane rejects stale, conflicting, premature, expired, or
incompatible plans. Reapplying the same plan/version/content is idempotent.

## 4. Materializations

A materialization describes one logical summary state expected by the
backend. It contains:

- content-addressed materialization identity;
- bound metric/source and canonical filters;
- summarized value or item label;
- physical/logical window contract;
- reduction and grouping layout;
- exact `SummaryFamilyType`, including concrete algorithm and parameters;
- collector producer references;
- state encoding/schema compatibility;
- storage destination and retention; and
- selected result guarantee when supplied by Planner.

Exact accumulators and approximate summaries use the same materialization
concept. The family discriminator determines which state and parameters are
valid. An algorithm/parameter mismatch is a decoding or validation failure,
not an untyped configuration bag.

The materialization identity includes every semantic property required for
safe reuse and merge. Two states with the same metric name but different
filters, reductions, grouping, windows, families, parameters, or encoding are
different materializations.

## 5. Routing and readout

Materializations describe what state exists. Routing entries describe what
queries that state can answer. These are separate because one materialization
may serve several readouts or queries.

For example, one sufficiently accurate quantile materialization may support
p50, p95, and p99 readouts. It remains one maintained state with several
routing entries.

A routing entry identifies:

- the query capability/readout it satisfies;
- the materialization used;
- any remaining backend-side operation;
- required grouping/window compatibility;
- the storage tier; and
- exact fallback behavior on a miss.

Routing never changes the materialization's summary semantics. A capability
match can choose among already valid planned routes; it cannot reinterpret
stored state as another family or parameterization.

## 6. Two levels of matching

Serving needs two different matching strengths.

### Capability matching

Capability matching answers:

> Is there a planned materialization that can answer this logical query shape?

It may allow a compatible family-level or readout-level relationship, such as
using one quantile summary for several ranks or rolling up a mergeable finer
grouping when the plan explicitly permits it.

### State compatibility matching

State compatibility answers:

> Can these exact payloads be decoded, merged, and read under this
> materialization contract?

This comparison is strict. Family, algorithm, parameters, grouping layout,
window, encoding, plan identity, and materialization identity must agree.

Capability compatibility never implies state compatibility. The backend may
route a query to a compatible planned materialization, but it may merge only
strictly compatible states.

## 7. Accuracy guarantees

`AccuracyTarget` is the requested constraint used during planning. When the
pinned Planner revision provides `ResultGuarantee`, BackendPlan preserves the
selected result's:

- error metric;
- bound expression;
- failure probability;
- provenance and budget allocation; and
- any unavailable statistic that kept the guarantee unknown.

The data plane does not recompute or weaken that guarantee. Unknown is not
zero and not exact. A route that requires a guarantee fails when the stored
plan lacks a sufficient one.

For an explicitly permitted approximate realization of an Exact-requested
TopK query, BackendPlan records the effective approximation target and
guarantee. It must never label that realization exact.

## 8. Source and payload validation

Each ingested summary payload identifies:

- plan ID and version;
- backend compatibility identity;
- materialization ID;
- collector/producer ID;
- window identity;
- state schema version; and
- full/delta sequencing and checkpoint identity when applicable.

The backend rejects payloads that are unknown, stale, incompatible, out of
sequence, or addressed to another active plan. It does not place them into a
best-effort metric-name bucket.

A plan may reference several collector producers for one sharded
materialization. The backend merges them only if the materialization contract
and family algebra allow it.

## 9. Installation and lifecycle

BackendPlan installation is staged and atomic:

1. Decode and validate the complete plan.
2. Validate every family, readout, route, source, and storage capability.
3. Build a new routing/index view without mutating the active view.
4. Confirm compatibility with the matching collector subplan.
5. Mark the plan staged until its activation time and collector application
   evidence are available.
6. Atomically switch query routing to the new view.
7. Retain the previous view until in-flight readers and the configured drain
   horizon finish.

An invalid update never partially changes routing. The previous unexpired
plan remains available for rollback.

## 10. Query-time behavior

At query time, the data plane:

1. parses/canonicalizes the query only as needed to identify its planned
   logical shape;
2. looks up routes installed from BackendPlan;
3. verifies readiness, freshness, grouping, window, and guarantee;
4. fetches strictly compatible states;
5. merges and reads them according to the planned operation; and
6. returns the result or takes the explicit exact fallback.

It does not invoke Planner candidate search, run a planning cost model, resize
a sketch, or infer a missing parameter.

## 11. Example

Suppose the workload contains:

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

The selected and compiled plan may contain one DDSketch materialization with
one-minute panes and a p95 routing/readout entry. BackendPlan records the
DDSketch parameter, per-entity reduction, independent grouping layout,
producer collectors, storage route, window composition, and selected
guarantee.

When the query arrives, the data plane finds that route, fetches five
compatible panes, merges DDSketch state, and reads p95. It does not run a cost
model to reconsider KLL or choose a new DDSketch parameter.

## 12. Wire-format principles

BackendPlan is a typed protobuf contract.

The schema follows these principles:

- family-specific values use typed discriminated variants;
- invalid family/parameter combinations are unrepresentable or rejected;
- additive optional fields support controlled rollout;
- unknown required variants fail closed;
- compatibility is explicit through `backend_compat`;
- materialization identity is stable and content-addressed; and
- debug/explain fields are not runtime identity.

Exact protobuf field numbers and generated-language types belong in the
protocol definition and implementation review, not this design document.

## 13. Fail-closed behavior

The plan or query fails when:

- the plan envelope is stale or incompatible;
- a materialization family/parameter/grouping/window is unsupported;
- a collector source does not match the expected contract;
- state payload identities or sequences are invalid;
- a requested route has no valid materialization;
- a required guarantee is absent or insufficient;
- required state is empty, stale, or incomplete; or
- exact fallback is required but unavailable.

The backend must not return a plausible summary result from mismatched state,
silently drop missing series, treat missing data as zero, or hide a failure as
a routing miss.

## 14. Non-goals

This document does not define:

- Planner candidate generation or ranking;
- CollectorPlan;
- summary-state byte encoding;
- storage-engine implementation;
- query parser implementation;
- routing-index data structures or performance optimizations; or
- protobuf field numbering.

## 15. Definition of done

The BackendPlan design is satisfied when:

- control plane and data plane share one typed contract;
- every expected collector materialization has an exact backend declaration;
- one materialization can serve several explicit routes/readouts;
- state merge uses strict compatibility;
- guarantees remain Planner-derived and fail closed;
- plan installation and routing switch atomically;
- query serving performs no independent summary planning;
- stale or mismatched payloads are rejected; and
- end-to-end tests prove matching plans serve and mismatched plans fail.
