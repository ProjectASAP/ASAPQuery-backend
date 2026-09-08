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

Implementation tracking (PRs are not merged automatically):

| PR | Implemented scope |
| --- | --- |
| [Backend #513](https://github.com/ProjectASAP/ASAPQuery-backend/pull/513) | A: compatible physical producer deduplication and conflicting deployment-contract rejection |
| [Backend #514](https://github.com/ProjectASAP/ASAPQuery-backend/pull/514) | B prerequisite: port the backend from its divergent historical pin to merged Planner APIs, including typed summary inputs |
| [Planner #356](https://github.com/ProjectASAP/ASAPPlanner/pull/356) | B: reusable, scope-local typed post-ASAP subtree interning; includes schemas and guarantees in equivalence |
| [Backend #515](https://github.com/ProjectASAP/ASAPQuery-backend/pull/515) | B: workload search, shared producer bindings and persistent query-root mapping |
| [Backend #516](https://github.com/ProjectASAP/ASAPQuery-backend/pull/516) | C: backend-local packed SUM/observation-count state, exact readouts, additive reductions and constrained arithmetic; production HTTP acceptance |
| [Backend #517](https://github.com/ProjectASAP/ASAPQuery-backend/pull/517) | E: current distributed publication/frame protocol, actual Collector validator, two shared readouts, failed staging and inactive-generation rejection |
| [Backend #518](https://github.com/ProjectASAP/ASAPQuery-backend/pull/518) | B/F: one workload-selection adapter for canonical startup and compile-and-publish; query-scoped accuracy certificates |
| [Backend #519](https://github.com/ProjectASAP/ASAPQuery-backend/pull/519) | D component: joint producer lifecycle demand, incompatible-evidence rejection and identity-keyed lifecycle estimates |
| [Backend #520](https://github.com/ProjectASAP/ASAPQuery-backend/pull/520) | E: published config drives the actual Collector Rust update/window/emission loop; N raw observations yield N updates and one shared output |
| [Backend #521](https://github.com/ProjectASAP/ASAPQuery-backend/pull/521) | E: failed staging cleanup permits retry; concurrent readers survive successful same-semantic generation cutover; retired frames are rejected |
| [Backend #522](https://github.com/ProjectASAP/ASAPQuery-backend/pull/522) | D: provider-priced complete bound-workload selection, strict v2 startup evidence, read-only quote preparation, live publication/reporting and process acceptance |

The backend PRs form a sequential review stack from #513 through #522;
#515 uses merged Planner #356 at revision
`378a7547ede629a64e84c9f7c810226ce196cce9`. #516 includes the fail-closed
arithmetic regression fix, propagated through its dependent branches.
The backend-local dashboard and distributed single-partition quantile examples
have executable acceptance evidence, including complete cost-based selection
and same-semantic generation cutover. The supported-profile implementation
is in the review stack, not yet merged or deployed. Production calibration,
platform-specific rollout and broader semantic workload replacement are not
claimed complete by these fixtures.

Local verification of the original combined migration stack: 654 control-plane
library tests, 28 control-plane binary tests, one control-plane integration
test, 977 data-plane library tests and three production-process tests passed. Planner
#356 passed its 156 type-library tests and GitHub formatting/lint/test checks.
The backend process tests cover the actual binaries and Collector Rust library,
not production traffic or every Collector platform adapter. Local passes do
not replace PR CI, review or the remaining migration gates.

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

The implemented backend-local example uses one raw accumulator that retains
both sum and observation count. This is native physical packing of selected
Planner operations, not a new backend semantic rewrite. The process test has
two services: observations `[10]` and `[2, 4, 8]` across two API instances give
SUM = 24, COUNT = 4 and weighted mean = 6; worker observations `[9, 15]` give
SUM = 24, COUNT = 2 and mean = 12. Three registered consumers still configure
one producer; a Remote Write retry does not double the counts. Range steps,
output labels/timestamps and unaligned-window fallback are checked.

Do not generalize that execution contract to `sum(sum_over_time(m) /
count_over_time(m))`: summing per-instance means cannot pool samples first.
Non-additive entity reduction, mismatched operand grouping/windows, shifted
selectors and unverified instantaneous/temporal combinations remain explicit
fallbacks. Unknown legacy observation counts also fail closed. Distributed
observation-count readout is not advertised by this implementation.

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

Implemented component: #519 gives each unique physical producer a
`WorkloadDemand` containing all its consuming query entries. For a 300-second
horizon, 100 updates/second and two consumers reading every 10 and 20 seconds,
the demand is 30,000 updates and 45 reads. With build = 10, update = 0.001,
read = 0.1, retention/second = 0.001 and retirement = 1, the lifecycle cost is
45.8. Adding the second consumer increases cost by 1.5, not another build and
update stream. Publication reports this component against the materialization
and implementation identities; it is not a complete-plan total.

Implemented selection: #522 compares complete bound alternatives before
commitment. A provider prices source upkeep, each shared state's build/update/
residency/retirement per location, transport, every reachable query operator,
and results over one common horizon. Query work is multiplied by recurrence;
shared maintenance is not multiplied by consumer count. Native exact fallback
includes its service's input upkeep as well as full native query execution.

The default inventory is the Planner-selected continuously maintained workload
and its whole-workload exact alternative. The comparison interface also accepts
additional Planner-authorized, bindable forests; this is not exhaustive search
over all engines or lifecycle variants. Tests prove both the sharing win and
high-retention loss, and reject missing, stale, mismatched or infeasible quotes.

Implementation refinement: pricing uses a flat coverage manifest over the
existing bound physical projection, not another semantic DAG. It does not
populate `PlannerPhysicalPlanProvider` with guessed source statistics or split
the older opaque per-query window scalar into fabricated components. Providers
must quote the actual source scope, state layout, implementation and capability
generation. The selected plan and report retain those identities.

Version-2 canonical snapshots require complete evidence. Live requests can
obtain requirements from the read-only `cost-manifests` endpoint before
publication. Version 1 and live requests without quotes remain explicitly
uncosted compatibility paths. See the [provider workflow in #522](https://github.com/ProjectASAP/ASAPQuery-backend/blob/feat/complete-workload-cost-selection/docs/examples/workload-cost-evidence.md).

Production calibration still requires evidence from the intended deployment;
the deterministic fixture costs are not production measurements. The provider
attests exact-backend access and resource feasibility; a low cost alone does
not establish either.

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

Current evidence combines real backend executables with the actual Collector
Rust runtime library. The test's host adapter supplies OpAMP acknowledgements
and frame metadata; it does not launch a platform-specific Collector binary.
In #521, failed Collector staging is discarded without touching the active
snapshot; the same successor version can then be retried successfully while
queries run. Old-generation frames are rejected after cutover and successor
frames become queryable. #522 exercises this flow with costed publication.
This verifies same-semantic runtime generation replacement, not arbitrary
semantic workload replacement or a platform-specific production rollout.
Platform adapter rollout remains a deployment acceptance step.

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

Call-site audit: production instant/range serving already requires an active
physical QueryPlan and declines absent or unregistered routes. The old
summary-selection serving branches in `engine.rs` are `cfg(test)` fixtures.
#518 unifies the two first-class compilation entry points. Legacy flat-workload
demo/configuration adapters remain separate compatibility paths; they must not
be presented as migrated canonical-workload entry points or removed without
their own parity/retirement decision.

## Existing PR coordination

At the baseline inspection, open PRs
[#505](https://github.com/ProjectASAP/ASAPQuery-backend/pull/505),
[#506](https://github.com/ProjectASAP/ASAPQuery-backend/pull/506),
[#509](https://github.com/ProjectASAP/ASAPQuery-backend/pull/509) and
[#511](https://github.com/ProjectASAP/ASAPQuery-backend/pull/511) cover PromQL,
process-E2E and TopK-related work. Re-check their status and changed files
before touching overlapping paths. Their presence is not evidence that the
workload-sharing migration is complete.

Review follow-up (2026-09-08): #505 is now stacked on #522 and uses the merged
Planner revision above. Typed TopK update weights belong to the selected
producer, not its readout. Its multi-series fixture distinguishes count ranking
(`api=4`) from value ranking (`worker=200`). #509 compares complete vectors at
each range step, including changing winners. #506 tests unregistered-query
fallback; it is not evidence that registered arithmetic is unsupported.

#515 preserves duplicate algorithm candidates during cost ranking; removing
them violates Planner's candidate-multiset contract and can panic. #522 quote
preparation enumerates bindable alternatives without requiring the default
warm alternative to compile, so missing warm implementations do not hide an
available exact quote. Publication still requires a selected, validated plan.

#511 retains evidence-aware legacy binding and preserves count update semantics
in emitted heap configuration. Its two heap TopK acceptance tests now use
registered `topk(3, count_over_time(top_endpoint_qps[5s]))`, a compiled physical
QueryPlan, and the production backend-local Remote Write path. Both CMS-with-heap
and CountSketch-with-heap return gamma=200, zeta=150 and alpha=100 over two
windows, with exact item identities, timestamps and retry deduplication checked.
Unregistered instantaneous TopK still follows the explicit exact fallback.
This replaces the two obsolete no-QueryPlan tests; it does not restore that
serving contract or claim migration of other legacy OTLP fixtures.

The [architecture PR #512](https://github.com/ProjectASAP/ASAPQuery-backend/pull/512)
tracks the design and this delivery plan. Implementation PRs should report the
slice they complete, tests actually run, and remaining acceptance gaps.
