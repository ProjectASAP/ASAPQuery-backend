# ASAPQuery-backend integration with ASAPPlanner

> Status: proposed
>
> Scope: migrate ASAPQuery-backend to use ASAPPlanner as its only logical
> query and summary planner, while keeping deployment planning and execution
> inside ASAPQuery-backend.

## TL;DR

ASAPPlanner answers:

> What exact or summary-based logical plans can answer this query workload,
> and which candidate should be selected under the supplied correctness and
> cost constraints?

ASAPQuery-backend answers:

> Where should the selected logical operators run, how are their states
> transmitted and stored, and how are queries routed to the active plan?

The integration boundary is the selected post-ASAP workload DAG. ASAPQuery
must not duplicate Planner parsing, canonicalization, summary selection,
accuracy algebra, common-subexpression search, or candidate ranking.
ASAPPlanner must not own collector placement, runtime windows, OpAMP,
transmission mode, storage routing, deployment rollout, or query serving.

One downstream physical compile converts the selected DAG into two matching
runtime plans:

```text
Query workload
      |
      v
ASAPPlanner
  selected post-ASAP workload DAG
      |
      v
ASAPQuery-backend physical compiler
      |
      +-----------------------------+
      |                             |
      v                             v
CollectorPlan                   BackendPlan
ASAPCollector                   ASAPQuery data plane
build/transmit state            ingest/store/read state
```

Both plans carry the same plan and materialization identities. Neither plan
is activated unless both sides validate the same compiled decision.

## 1. Goals

The migration must produce one planning path that:

- accepts one-shot and repeating PromQL workloads, with SQL enabled only when
  a real schema catalog is available;
- preserves workload-wide sharing instead of flattening queries into
  independent metric/sketch requests;
- uses ASAPPlanner's selected summary family, algorithm, parameters,
  reduction, grouping strategy, readout, and accuracy contract exactly;
- compiles that selection into compatible collector and backend plans;
- lets the data plane execute the selected plan without planning again;
- rejects unsupported or incompatible plans before activation;
- supports shadow comparison and rollback during migration; and
- removes the legacy planner only after the new path passes end-to-end gates.

## 2. Non-goals

This migration does not:

- move physical placement or scheduling into ASAPPlanner;
- serialize ASAPPlanner's internal Rust DAG as a runtime wire format;
- use DAG-viewer JSON, node colors, explain IDs, or rationale strings as
  execution input;
- require every summary family or every open Planner proposal to be supported
  by the MVP runtime;
- remove exact raw/archive fallback;
- reimplement Prometheus rule scheduling or alert state in ASAPPlanner; or
- delete the legacy path before shadow validation and rollback exist.

## 3. Ownership

### ASAPPlanner owns logical planning

ASAPPlanner is the canonical owner of:

- query parsing and semantic lowering;
- pre-ASAP IR and canonicalization;
- schema binding;
- workload-level common-subexpression discovery;
- exact and summary-based candidate generation;
- summary family, algorithm, parameters, and grouping alternatives;
- logical rewrites and reuse opportunities;
- accuracy requirements and, when supported by the pinned revision, result
  guarantees;
- cost-aware candidate search and global selection; and
- logical explain and rejection information.

### ASAPQuery-backend owns physical planning and rollout

The ASAPQuery control plane owns:

- source/protocol adapters and caller identity;
- runtime statistics, budgets, and its ASAPPlanner cost-model implementation;
- executor capability discovery;
- collector/backend/archive placement;
- physical streaming windows and lateness policy;
- raw, full-summary, and delta-summary transmission decisions;
- materialization identity and storage routing;
- creation of matching `CollectorPlan` and `BackendPlan` artifacts;
- deployment diff, warm-up, activation, retirement, and rollback; and
- planning telemetry and operator-facing explain endpoints.

### ASAPQuery data plane owns execution

The data plane owns:

- backend-plan installation and atomic hot reload;
- summary-state ingestion, validation, storage, and merge;
- query-time readout and remaining backend-side logical operators;
- readiness, freshness, and watermark checks;
- exact archive fallback; and
- Prometheus-compatible request and response behavior.

### ASAPCollector owns collector-plan execution

ASAPCollector owns:

- validating and atomically applying its `CollectorPlan`;
- computing the specified materializations;
- emitting raw, full, or delta payloads as directed;
- rejecting unsupported families, parameters, grouping layouts, or
  transmission modes; and
- reporting semantic plan activation and emitted-state evidence.

## 4. Stable integration contracts

### 4.1 Workload input

Protocol adapters convert external requests into ASAPPlanner's canonical
workload model. They may attach caller IDs outside the Planner value, but they
must not copy Planner domain types into a second backend schema.

For each query, the planning input contains at least:

- query language and expression;
- one-shot or repeating evaluation shape;
- requested accuracy;
- optional latency requirement;
- schema/catalog reference when required; and
- protocol-neutral recurrence information.

Prometheus-only behavior remains adapter/runtime metadata, including rule
group ordering, query offset, missed iterations, `for`, `keep_firing_for`,
labels, annotations, and Alertmanager state.

### 4.2 Planner output

The output consumed by ASAPQuery is one selected post-ASAP DAG for the whole
workload, with shared node identity intact. It may contain:

- `SummaryAgg` producers;
- exact accumulators and approximate summaries;
- `SummaryEstimate` readouts;
- reductions and grouping strategies;
- summary merge/subtract/delete/join operations;
- logical rewrites and shared sub-DAGs; and
- `KeepPreAsap` exact fallback subtrees.

ASAPQuery does not flatten this DAG into `(metric, query type, sketch)` rows
before placement. Doing so would lose sharing, composition, grouping, rollup,
and provenance.

### 4.3 Physical compile

The physical compiler consumes exactly one selected DAG plus deployment
topology, capabilities, statistics, and resource constraints. It produces one
`CompiledPlan` with:

- a `CollectorSubplan` containing one `CollectorPlan` per targeted collector;
- a `BackendSubplan` containing the matching `BackendPlan`;
- shared `plan_id`, `plan_version`, `activation`, `expiry`, and
  `backend_compat` values; and
- shared content-addressed materialization identities.

The collector and backend portions are emitted by the same compile operation.
Two independent compilers must not reinterpret the selected DAG separately.

The detailed split is defined in
[`design-compiled-plan-collector-backend-split.md`](design-compiled-plan-collector-backend-split.md).

### 4.4 Collector interface

The collector half follows ASAPCollector's
[collection-plan interface](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md):

- OpAMP protobuf is the delivery envelope;
- `asap-collector-plan.yaml` is the exact config-map entry;
- the entry contains a versioned `CollectorPlan`, not a complete OTel
  bootstrap configuration;
- summary family, algorithm, parameters, accuracy, reduction, and grouping
  retain Planner semantics;
- source binding, windows, placement, and transmission are added by the
  physical compiler; and
- semantic activation requires the collector application report, not only
  `RemoteConfigStatus.APPLIED`.

### 4.5 Backend interface

`BackendPlan` carries:

- the shared compiled-plan envelope;
- every planned materialization and its exact `SummaryFamilyType`;
- source, filter, window, reduction, grouping, and storage route;
- collector source references;
- query capabilities and readouts satisfied by each materialization; and
- the selected result guarantee when the pinned Planner revision supplies
  one.

The data plane builds its routing index from this plan. It must not run a cost
model or infer summary parameters from stored state at query time.

The detailed backend contract is defined in
[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md).

## 5. Planner version and open-PR policy

ASAPQuery pins every ASAPPlanner crate to one immutable revision of Planner
`main`. It never combines several open PR branches in production. A pin update
changes all Planner crates and the lockfile together.

Open Planner work is handled according to its integration effect:

| Planner PR | Integration effect | Adoption rule |
| --- | --- | --- |
| [#300](https://github.com/ProjectASAP/ASAPPlanner/pull/300) | Adds explicit update/readout phases and exact-summary composition | After merge, use validated execution availability to partition collector and backend operators. Enable only phases supported by both executors. |
| [#299](https://github.com/ProjectASAP/ASAPPlanner/pull/299) | Adds typed result guarantees, error propagation, budgets, and accuracy rejections | After merge, supply root accuracy targets, reject invalid guarantees before cost ranking, and preserve the selected guarantee in `BackendPlan`. |
| [#295](https://github.com/ProjectASAP/ASAPPlanner/pull/295) | Adds recurrence-aware CSE and global selection | After merge, provide evaluation/update rates and an explicit horizon for mixed one-shot/repeating workloads. Scheduling remains outside Planner. |
| [#293](https://github.com/ProjectASAP/ASAPPlanner/pull/293) | Allows an explicit approximate TopK candidate for an Exact-requested TopK | Opt in only through the upstream hook with an approved non-Exact sizing target. Record the effective approximation; retain exact pass-through. |
| [#291](https://github.com/ProjectASAP/ASAPPlanner/pull/291) | Adds optional `Concat` discriminator unique-key metadata | Preserve it in exhaustive IR visitors. Do not invent keys downstream. It creates no collector primitive by itself. |
| [#296](https://github.com/ProjectASAP/ASAPPlanner/pull/296) | Adds explain/viewer cost annotations | Consume only in explain output. Never use it for execution, identity, or placement. |
| [#292](https://github.com/ProjectASAP/ASAPPlanner/pull/292) | Corrects viewer node categories | No runtime integration effect. |

This table is an audit, not a merge dependency list. The migration proceeds
against the pinned baseline. When a contract-affecting PR merges, the pin
update must include its adapter/compiler/capability changes and golden tests
in the same backend PR.

ASAPQuery must not pre-copy proposed Planner types such as phase assignments,
result guarantees, recurrence profiles, or discriminator keys. Until a type
exists at the pin, it is absent. When a new upstream variant appears, physical
compilation fails with an unsupported-shape diagnostic until it is mapped
deliberately.

## 6. Accuracy and cost

### Accuracy

`AccuracyTarget` is the requested correctness constraint. If the pinned
Planner revision supplies `ResultGuarantee`, that is the computed guarantee
of the selected result. They are different values and both remain owned by
Planner.

The required order is:

```text
candidate generation
  -> guarantee propagation
  -> reject candidates outside the requested target
  -> cost ranking among valid candidates
  -> global selection
  -> physical placement
```

Cost and placement cannot resurrect an accuracy-invalid candidate. Unknown
error bounds, probabilities, or required statistics remain unknown; they are
never converted to zero or exact.

`AccuracyTarget::Exact` normally selects an exact summary or `KeepPreAsap`.
The only planned exception is the explicit TopK policy exposed by Planner
#293. If enabled, the compiled plan states that the chosen realization is
approximate and records its effective target and guarantee.

### Cost

ASAPQuery supplies deployment-specific cost information through Planner's
cost-model interface. Inputs may include:

- ingest/update rate;
- query evaluation rate;
- summary maintenance and read cost;
- exact recomputation cost;
- initial materialization cost;
- expected cardinality and subpopulation count;
- memory, network, and storage budgets; and
- the comparison horizon for mixed recurring and one-shot work.

Missing statistics remain unknown. Structural fallback is allowed only when
Planner defines it explicitly. Backend code must not treat missing data as a
free operation.

Physical placement applies deployment constraints after logical selection. It
does not choose a different logical summary because a selected placement is
inconvenient; it either finds a valid placement, asks Planner to select among
capability-constrained candidates, or fails planning.

## 7. Repeating workloads

ASAPPlanner receives protocol-neutral repeating-query intervals. These
intervals may affect workload CSE and cost selection, but they do not make
Planner a scheduler.

Keep these concepts separate:

- evaluation interval: how often the query runs;
- query lookback: the data range in the query, such as `[5m]`;
- physical pane size: chosen by the ASAPQuery physical compiler;
- query offset: changes the logical evaluation timestamp;
- allowed lateness and watermark: runtime readiness policy; and
- alert `for`/`keep_firing_for`: Prometheus alert-state behavior.

For the MVP, physical windows use anchored tumbling panes. A chosen pane must
compose into every claimed query range, and the backend must query only panes
whose watermark covers the logical evaluation time. Incompatible window
alignment is a planning failure, not an approximate answer.

When multiple recurring queries can reuse one materialization, recurrence-
aware selection uses their combined evaluation rate. Prometheus schedules
remain authoritative; inferred dashboard frequency must not override a
declared rule interval.

## 8. Capability and failure contract

Before selection and again before activation, ASAPQuery validates the chosen
plan against collector and backend capability snapshots.

The following conditions fail closed:

- Planner selects a family, algorithm, parameter set, grouping strategy,
  execution phase, or readout unsupported by an assigned executor;
- the collector and backend disagree on plan or materialization identity;
- the backend expects a different summary family, parameters, grouping,
  window, or state encoding from the collector;
- an accuracy guarantee is missing or insufficient where one is required;
- a delta mode lacks compatible sequencing/checkpoint semantics;
- a plan is stale, expired, or not yet active;
- application evidence is missing; or
- a query result would use state from an earlier plan/run.

No failure may silently substitute another summary, loosen accuracy, erase a
grouping strategy, treat a missing series as zero, or return a plausible
result from incompatible state.

## 9. Migration phases

### Phase 0: pin and baseline

- Pin all Planner crates to one immutable revision.
- Record that revision in build and planning artifacts.
- Capture legacy plans and query results for the golden workload.
- Add deadlines, size limits, and planning telemetry before enabling shadow
  workloads.

Exit gate: the workspace builds and existing behavior is unchanged.

### Phase 1: canonical workload ingestion

- Route single and multi-query inputs through one workload adapter.
- Add repeating-query input without moving scheduler metadata into Planner.
- Preserve caller IDs and per-query diagnostics outside canonical Planner
  values.

Exit gate: one-shot and repeating entries lower deterministically, and shared
canonical sub-DAGs remain shared.

### Phase 2: workload selection in shadow mode

- Run Planner candidate search and global selection for the full workload.
- Supply the ASAPQuery cost model, accuracy inputs, recurrence data, and
  capability constraints supported at the pin.
- Export logical explain and typed rejection information.
- Compare with legacy decisions without changing production plans.

Exit gate: every difference is explained by a documented semantic, accuracy,
cost, or sharing improvement; unexplained differences block rollout.

### Phase 3: compile both physical plans

- Convert one selected workload DAG into matching collector and backend
  subplans.
- Preserve shared materializations and readout consumers.
- Produce stable materialization fingerprints and the shared plan envelope.
- Validate both subplans before either is pushed.

Exit gate: golden tests prove that a deliberately mismatched collector/backend
pair is rejected and that matching plans round-trip through both wire formats.

### Phase 4: executor coverage and end-to-end shadowing

- Install BackendPlan without query-time replanning.
- Push CollectorPlan and require semantic application reports.
- Exercise exact accumulators, supported summaries, reductions, grouping,
  readouts, merge, raw/full/delta modes, and archive fallback.
- Compare aligned ASAP and exact results, freshness, and plan identities.

Exit gate: every selectable runtime shape is executed correctly or rejected
before selection; missing evidence fails the run.

### Phase 5: selected rollout

Use three rollout modes:

| Mode | Planner executed | Production plans emitted |
| --- | --- | --- |
| `legacy` | legacy only | legacy |
| `shadow` | legacy and Planner | legacy |
| `selected` | Planner | compiled Planner-derived plans |

Roll out `selected` by workload/tenant. Keep the previous valid compiled plan
available for immediate rollback. A failed new plan never replaces it.

Exit gate: the agreed observation period has no unexplained correctness,
freshness, capability, or plan-identity failures.

### Phase 6: delete legacy planning

Remove legacy paths that:

- union per-query capabilities into sketch choices;
- implement aggregate roots independently;
- choose summary families outside Planner;
- infer planned parameters from stored state at serving time; or
- generate collector and backend plans from separate logical decisions.

Retain protocol adapters, physical placement, runtime cost inputs,
CollectorPlan/BackendPlan compilation, routing, execution, and archive
fallback.

Exit gate: production has one logical planning path and one physical compile
path, with rollback based on previous compiled plans rather than legacy
planning.

## 10. Validation

### Cross-repository golden workload

Maintain one versioned workload covering:

- a repeated subexpression shared by multiple queries;
- compatible and incompatible filters;
- per-entity and grouped reductions, including empty global reduction;
- independent grouping and a capability-rejected shared grouping case;
- exact aggregation and each claimed MVP summary family;
- one-shot and repeating queries with different intervals;
- raw, full, and delta transmission;
- an unsupported query that takes exact fallback;
- an accuracy-invalid candidate;
- a stale or mismatched plan; and
- cold/archive fallback.

For each pinned Planner revision, retain:

- input workload and schemas;
- Planner revision and configuration;
- canonical and selected logical explain artifacts;
- rejected candidates and reasons;
- compiled collector and backend plans;
- capability snapshots;
- application reports;
- emitted materialization identities;
- query results and aligned exact results; and
- derived correctness, accuracy, freshness, latency, and cost measurements.

### Required test boundaries

1. External workload input to canonical Planner workload.
2. Canonical workload to selected post-ASAP DAG.
3. Selected DAG to matching CollectorPlan and BackendPlan.
4. Both wire formats to semantic activation.
5. Observations to stored summary state.
6. Query to summary readout or explicit exact fallback.
7. Replan to warm cutover, retirement, and rollback.

### Pin-update tests

Every Planner pin update must:

- build all Planner-dependent crates at one revision;
- rerun the golden workload;
- update exhaustive mappings for new IR variants;
- prove unsupported new shapes fail before activation;
- compare selected materializations and guarantees against the previous pin;
  and
- explain every intentional difference.

## 11. Operational limits

The control plane enforces checked-in limits for:

- queries and schemas per workload;
- reachable IR nodes;
- candidate groups and candidates per group;
- planning iterations and wall time;
- explain artifact size; and
- materializations per compiled plan.

Planning supports cancellation and reports phase timings, candidate counts,
rejections, selected cost, fallback, and timeout. A timeout or exceeded limit
returns a typed planning failure or configured exact fallback; it never emits
a partial compiled plan.

## 12. Definition of done

The migration is complete when all of the following are true in the same
supported release:

- all production queries enter one workload-aware ASAPPlanner path;
- every Planner crate is pinned to the same recorded revision;
- the selected post-ASAP DAG is the only logical source for runtime plans;
- one compiler emits matching CollectorPlan and BackendPlan artifacts;
- shared workload sub-DAGs remain shared through materialization and serving;
- the data plane never selects or sizes summaries independently;
- accuracy targets and guarantees are preserved without backend-local
  reinterpretation;
- every selected runtime shape is supported by both executors or rejected
  before activation;
- semantic collector activation and backend activation agree on plan and
  materialization identities;
- exact fallback remains available for unsupported queries;
- cross-repository golden and end-to-end tests pass;
- shadow rollout and rollback have been exercised; and
- legacy logical planning and serving-time reconstruction have been removed.
