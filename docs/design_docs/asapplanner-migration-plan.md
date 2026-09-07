# ASAPPlanner integration: migration delivery plan

Status: implementation sequence for the
[system architecture proposal](asapplanner-integration.md). A checked milestone
requires executable evidence; publishing this plan or opening a PR does not
complete migration.

## Baseline and completion definition

The inspected baseline is backend `95131d83972bb7a07d338e2a5af925a20c15ddce`.
The compiler already deduplicates backend PrecomputePlan state by physical
fingerprint and binds QueryPlan leaves explicitly. It still builds Collector
materialization declarations per query, and lifecycle selection builds a
single-query demand. Therefore, do not describe all sharing as absent, or
treat existing fingerprint deduplication as workload-wide optimization.

Migration is complete for a declared supported workload/profile when:

- one Planner-authorized semantic decision governs all result roots;
- compatible shared producers have one physical maintenance path per source
  partition and generation;
- unsupported sharing or operators are rejected or explicitly fall back;
- activation, readiness, query execution and feedback refer to matching
  bindings and generations;
- supported entry points no longer independently select a different summary;
- parity and producer-update tests pass for the promised deployment profile.

Backend-local and distributed profiles have separate acceptance evidence.
Neither arbitrary PromQL coverage nor ProjectASAP-wide engine coverage is a
completion prerequisite.

## Delivery sequence and dependencies

Implementation tracking:

- Slice A: [PR #513](https://github.com/ProjectASAP/ASAPQuery-backend/pull/513)
  implements compatible Collector producer deduplication and deployment-contract
  conflict checks. All 638 control-plane library tests pass. It preserves
  consumer-sensitive plan identity and rate/increase state compatibility.
  Runtime update-count and actual Collector-consumption acceptance remain
  slice E work; declaration-count tests do not complete those gates.
- Slices B–F remain unimplemented by this migration series. Existing code and
  overlapping PRs must be reused rather than re-created.

| Slice | Repository | Depends on | Deliverable and acceptance |
| --- | --- | --- | --- |
| A. Safe physical state sharing | ASAPQuery-backend | Existing compiler | Deduplicate Collector declarations for compatible state; reject conflicting implementation/layout/lifecycle contracts; keep both query roots bound to one backend state |
| B. Workload semantic planning adapter | ASAPQuery-backend, with Planner changes only for demonstrated gaps | A and Planner API audit | Batch registered canonical roots through reusable Planner search; preserve root mapping and producer identity; do not implement backend-local semantic CSE |
| C. Aggregate-state fusion and readouts | ASAPPlanner for rules; backend for execution | B | SUM/COUNT example with per-consumer projections, label/time equivalence and fully executable division; reuse existing decomposition/rollup rules |
| D. Workload-wide implementation evidence | ASAPQuery-backend and Planner evidence boundary | B; C for fused states | Compare complete alternatives with shared build/update cost once and per-consumer read costs; joint state lifecycle/implementation agreement |
| E. Bound execution and lifecycle acceptance | ASAPQuery-backend; Collector only where public runtime gaps require it | A–D | Producer update counts, readiness/fallback, generation isolation, failed rollout, and distributed projection tests |
| F. Compatibility-path retirement | ASAPQuery-backend | E for each affected profile | Route supported entry points through the validated path; remove duplicate selection only after call-site and parity audit |

Slices are reviewable PR units, not an instruction to open empty placeholder
PRs. If a slice spans semantic changes and physical execution, split by
repository and stack the dependent PR explicitly. Do not merge automatically
or make one unverified pin bump cover unrelated Planner changes.

## A. Safe physical state sharing

The immediate regression fixture is two different quantile readouts over the
same source, parameters and window. It exercises existing supported operations
without depending on future SUM/COUNT fusion.

Implementation scope:

1. Compare concrete contracts when multiple selected leaves resolve to the
   same physical fingerprint. Include algorithm/parameters, grouping, window
   framework, implementation, pane layout and lifecycle. Runtime transmission
   policies must also agree.
2. Emit one Collector producer declaration for a compatible shared state while
   preserving every query's binding and readout.
3. Keep evidence conservative: differing evidence cannot silently disappear
   during deduplication. A future certificate-union design is a separate step.
4. Reject conflicting contracts before any plan is published. Do not pick
   whichever query happened to be visited first.

Acceptance: both query roots exist; one BackendPlan state and one PrecomputePlan
state exist; each Collector has one producer declaration; both bindings point
to that state. A different implementation/layout for the same fingerprint
fails compilation. Distinct source/window/parameters must remain distinct.

This slice establishes deployment consistency, not workload search or a claim
that all query-time computations execute once across separate HTTP requests.

## B. Workload semantic planning adapter

Audit the pinned Planner workload/search APIs before defining another backend
plan representation. Inputs must preserve canonical query identity, source
selection, requirements, recurrence and time scope.

The result must retain all original roots and shared logical producers.
Backend bindings must be keyed by workload-scoped producer identity, not only
a per-query pointer. Physical IDs stay downstream. Preserve explicit mappings
from each query root to its required materializations and fallback.

Acceptance fixtures:

- identical producers used by two different roots;
- a diamond within one query and sharing across queries;
- incompatible filters, grouping, windows or accuracy do not share;
- round-trip compilation retains roots and sharing;
- unsupported alternatives cannot become partially executable warm routes.

Pointer sharing in memory alone is not persistent identity. A serialized
execution projection must preserve the relationship explicitly.

## C. Aggregate-state fusion and complete readouts

Use the system document's sample-weighted mean example as the target.
Planner owns the equivalence rule: union compatible SUM/COUNT states and
project the needed results to consumers. The backend owns physical state
implementations and exact output operators.

First inspect existing AVG decomposition, CSE and rollup rules. Add only missing
semantics upstream; do not copy DQC transformation objects or hard-code metric
names in Planner.

Acceptance includes uneven per-instance sample counts, missing/stale series,
multiple services, exact interval endpoints, range evaluation steps and
denominator edge cases. Query results must match Prometheus labels, timestamps
and numeric semantics. Until the whole expression is supported, preserve
explicit fallback rather than claiming partial integration.

## D. Workload-wide evidence and selection

Today per-query lifecycle inputs are not proof of joint workload costing.
Aggregate demand for each shared producer while retaining consumer-specific
requirements. Compare alternatives over one horizon and data scope.

Charge shared initialization and maintenance once, account for all consumer
readouts and live/retained state, and include applicable placement and
transmission costs. Feasibility checks cover the entire selected DAG, not
only a summary family. The winning evidence must resolve to the same concrete
implementation that compilation installs.

Acceptance: a shared alternative wins when its complete cost is lower, loses
when retention/materialization overhead dominates, and is unavailable when
any required capability/evidence is absent or stale. Adding another consumer
must not double-count the producer's update stream.

## E. Runtime and deployment acceptance

Start backend-local, then validate the distributed profile independently.

- Replay deterministic raw samples through production ingestion.
- Count state creation and updates: one compatible producer per generation,
  with no duplicated updates when a second query subscribes.
- Query both roots through HTTP and compare with an exact reference.
- Test incomplete coverage, stale state, absent routes and unavailable fallback.
- Stage a successor while requests run; each request observes one generation.
- Fail staging or producer acknowledgement and verify the active generation
  remains unchanged.
- For distributed collection, decode emitted plans through the actual Collector
  validator and assert one producer per source partition, not one producer
  globally across independent sources.

Unit-level declaration counts do not replace runtime update-count tests.

## F. Retire duplicate selection safely

Inventory canonical startup compilation, explicit compile-and-publish,
legacy workload adapters and serving-time binding helpers. Distinguish dead
code from intentionally supported profiles using call-site inspection.

For each path, either route it through the selected workload contract, retain
it as an explicitly unsupported/fallback adapter, or remove it after parity.
Parsing and canonicalization at serving time are fine; family/parameter,
grouping or lifecycle reselection is not.

Do not remove QueryPlan, PrecomputePlan, physical deployment selection,
exact fallback, or profile-specific adapters merely because their types are
different from post-ASAP IR.

## Existing PR coordination

At the baseline inspection, open PRs
[#505](https://github.com/ProjectASAP/ASAPQuery-backend/pull/505),
[#506](https://github.com/ProjectASAP/ASAPQuery-backend/pull/506),
[#509](https://github.com/ProjectASAP/ASAPQuery-backend/pull/509) and
[#511](https://github.com/ProjectASAP/ASAPQuery-backend/pull/511) cover PromQL,
process-E2E and TopK-related work. Re-check their status and changed files
before touching overlapping paths. Their presence is not evidence that the
workload-sharing migration is complete.

The [architecture PR #512](https://github.com/ProjectASAP/ASAPQuery-backend/pull/512)
tracks the design and this delivery plan. Implementation PRs should report the
slice they complete, tests actually run, and remaining acceptance gaps.
