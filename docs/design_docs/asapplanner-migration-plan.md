# Physical-plan architecture: migration delivery plan

Audience: developers implementing the
[integration architecture](asapplanner-integration.md). Status: proposed delivery
sequence, not a record of completed implementation. This replaces historical PR
stack tracking with behavior-based gates. Existing merged behavior is the baseline;
old test totals and PR status are not evidence for this migration.

## Completion definition

For each declared supported deployment profile, one Planner decision is bound
once and projected into catalog, QueryPlan, PrecomputePlan, CollectorPlan and
TransmissionPlan. Publication, runtime state, and query readout agree on identity,
schema, window, guarantees and generation. Backend production code no longer
imports the Collector execution runtime for reconstruction.

Backend-local and distributed profiles need separate acceptance. Arbitrary
PromQL, all sketch-family delta modes, general multi-hop execution, and a new
repository are outside the completion gate. Preserve supported existing behavior;
record unsupported combinations as capabilities rather than broadening claims.

## Sequence and dependencies

| Stage | Owner | Deliverable | Exit gate |
| --- | --- | --- | --- |
| 1. Contract and behavior inventory | Backend, Collector, Planner maintainers | Authority map, supported capability matrix, cross-language fixtures | Every existing production wire path and plan entry point has an explicit compatibility expectation |
| 2. Common physical bindings | Backend control plane | Internal binding stage, catalog construction, four projections | Existing supported inputs produce semantically equivalent publications; no repeated selection through PrecomputePlan |
| 3. Shared contracts and validation | Backend/Collector; Planner for IR export | Lightweight contracts, typed semantic export, shared publication validation | Actual Go/Rust consumers accept matching artifacts and reject incompatible ones |
| 4. Policy and deployment boundaries | Compiler and runtimes | Production/transport policy split; explicit application and activation rules | Guarantee, checkpoint, readiness and partial-rollout fixtures pass for enabled modes |
| 5. Codec extraction | Sketch libraries, Collector, backend | Typed reconstruction APIs and consumer migration | Backend excludes `asap-precompute-rs`; supported decoding and query results remain compatible |
| 6. Retirement and release | Participating repositories | Remove superseded copies/adapters, pin compatible versions | Both profiles pass end-to-end gates without retired paths |

Stages 2 and 3 preserve existing wire formats through boundary adapters. Stage 4
changes public contracts only with negotiated/versioned compatibility. Codec work
can proceed after stage 1, but its removal gate depends on stable contracts and
consumer coverage. Do not combine an unrelated Planner upgrade with extraction.

## 1. Establish authority and fixtures

Inventory Planner exports, backend installed contracts, Collector Go/Rust DTOs,
OTel carriers, sketch state/delta schemas and legacy bare-state decoders. Record
one owner for each concept and the current supported producer/consumer versions.
Compare actual field shapes, defaults, enum meanings, units and rejection behavior;
a similarly named struct is not compatibility evidence.

Capture supported backend-local, distributed full-state, distributed delta, and
generation-transition examples. Use distinct evidence for wire equivalence and
semantic state/readout equivalence; randomized state may require persisted fixtures
and semantic assertions rather than comparing unrelated fresh encodings.

Protocol cases include duplicate/conflicting sequences, unknown delta base, gaps,
producer restart, malformed framed payload, and legacy unframed state. Label any
currently failing target invariant as migration work, not passing baseline behavior.
Have a separate reviewer review expected outcomes before protocol changes.

### Planner caller contract gate (#438)

Resolve [Planner #438](https://github.com/ProjectASAP/ASAPPlanner/issues/438)
at the public workflow boundary, not only in the backend compiler. First deliver
an ASAPPlanner user guide for supported entry/exit points, including workflows
that intentionally stop at pre-ASAP IR, candidates, or a selected semantic DAG.
Each recipe must document exact APIs, controls, defaults, performed checks and
output limitations and run against the documented revision. This guide does not
depend on implementing a unified facade. Document strategy selection and automatic
passes separately from model providers, runtime capabilities and requirements. Audit actual
API defaults and low-level output guarantees, including the current all-enabled
lifecycle capability default, unknown lifecycle cost inputs, and per-root accuracy
propagation. Distinguish Rust defaults from serialized-field omission. Then specify
one application-facing
request/result contract with explicit incomplete/infeasible outcomes. Use the
[caller contract](asapplanner-integration.md#caller-contract-and-lifecycle-completeness)
as the target; its omission rules are proposed behavior, not current API facts.

The complete output must associate each materialized state with a selected or
capability-constrained, validated lifecycle. Planner models the available
lifecycle vocabulary; runtime support and workload/policy constraints determine
which modes may enter candidate selection. A singleton legal set is a complete
selection, not a skipped lifecycle decision. Lifecycle feasibility and applicable
costs must participate in candidate selection. Keep diagnostic DAG exports accessible, but
do not allow them to masquerade as deployment-complete results. Document which
inputs callers control and which evidence/capabilities come from providers.

Gate: executable public-API examples cover one-shot, recurring, unknown-demand,
and missing-evidence inputs; diagnostics expose defaults and their consequences.
Include a backend that can build summaries only from data at rest: no incremental
mode may enter ranking, and a singleton legal lifecycle must produce a complete
commitment. Recurring demand must not imply incremental support or permission for
retained reuse. Verify that an empty legal set is reported explicitly.
The physical compiler rejects incomplete stateful commitments. Stages 2 and 3
must preserve this distinction while existing lower-level APIs remain compatible.

## 2. Refactor compilation without changing semantics

Retain candidate selection and cost/capability evaluation. Introduce only a
compiler-local structure for selected tasks, definitions, state bindings and
producer/consumer edges. Construct the catalog from the selected definitions,
then project all four plans from those bindings.

Remove the dependency of transmission compilation on PrecomputePlan. Preserve
shared producer identity across roots and reject incompatible physical bindings.
Target artifacts may embed catalog/rule subsets but must be derived from the
same publication. Compare old/new outputs with normalization only for explicitly
nondeterministic metadata; do not normalize away semantic or identity differences.

Gate: supported profiles retain query results, window/label semantics, producer
update counts, configured fallback and publication compatibility. New binding
provenance makes every runtime task traceable to the selected decision.

## 3. Extract contracts and unify validation

Separate lightweight semantic IR export from Planner search internals. Preserve
node/operator/schema/guarantee meaning while migrating `OwnedPostAsapDag`; do not
replace typed semantic validation with arbitrary JSON acceptance.

Extract SDS, installed plan, publication and frame contracts into packages that
import neither execution runtime nor optimizer. Select a schema authority and
binding-generation approach before removing manual Go/Rust copies. Keep sketch
payload schemas in their sketch-library authority.

Use shared cross-plan validation at compile and install boundaries, followed by
local resource checks. Versioned legacy adapters normalize once at the boundary.
Gate: fixtures run against real consumers, including Collector Go and Rust;
missing/unknown versions, catalog mismatches and unsupported capabilities fail
before activation. Package boundaries are checked through dependency inspection.

## 4. Make production, delivery and activation explicit

Split sampling/estimator policy from transmission suppression/cadence/checkpoint
policy. Allocate and validate them together against the selected query guarantee.
Preserve the rule that adaptive changes produce an authorized successor rather
than mutate an immutable generation.

For each enabled state family, specify full-state replacement versus independent
contribution semantics, delta base/application rules, replay persistence, and
resynchronization. Retain current encoding until the required endpoint migration
lands. Never assume merge supports subtraction or replacement.

Specify publication content identity and recoverable rollout coordination. Test
receiver preparation, exact target acknowledgements, failed stage cleanup, partial
activation, restart and delayed old-generation frames. Distinguish local atomic
snapshot installation from distributed convergence and state readiness.

Gate: no duplicate application or cross-generation query mixing; insufficient
coverage uses fallback/unavailability; unsupported recovery modes remain disabled.

## 5. Move codecs below runtimes

Move reusable Collector wrapper reconstruction to typed sketch-library APIs.
Switch both Collector and backend to these APIs. Preserve backend-specific
accumulator/readout adaptation while removing the KLL re-encode/decode detour.
Migrate DDSketch/KLL first; retain supported local paths for other families until
their replacements have parity evidence. Remove vendored delta definitions only
when their authoritative replacement is consumed by both endpoints.

Gate: full/delta/legacy fixtures and query results pass; dependency inspection
shows no backend production import of Collector runtime. Also remove the obsolete
Collector-specific dependency patch when no longer needed. Test-only end-to-end
fixtures may still build the actual Collector separately.

## 6. Roll out and retire

Roll out per supported profile with compatible pinned releases and preserved
rollback artifacts. Keep legacy readers for the agreed producer upgrade window;
remove them only after consumer inventory and replay/recovery retention permit it.
Do not reuse a codec version or descriptor identity for changed semantics.

Before activation, failure leaves the previous plan intact and staged resources
can be discarded. After partial activation, use the specified recovery protocol
or an explicit successor; a backend-only rollback is not sufficient. State reuse
across generations must pass compatibility checks independently of binary rollback.

Delete superseded DTO/schema copies, reconstruction paths, and stale documentation
after the replacement passes its gate. Independent query/maintenance projections,
profile adapters, and required legacy readers are not duplication to remove blindly.
Repository relocation and release automation follow stable package boundaries;
they are not prerequisites for runtime correctness.

## Final evidence

Record tested revisions, supported families/profiles, fixture results, dependency
graph checks, and compile/install/ingest measurements. Trace one query through its
semantic root, state definition, producer, flow and installed publication. Report
remaining capability gaps explicitly. Completion requires executable evidence,
not document publication, an open PR, or prior migration test counts.
