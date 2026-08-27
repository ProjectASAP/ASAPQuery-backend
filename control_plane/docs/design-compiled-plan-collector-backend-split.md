# Compiling a selected post-ASAP DAG into two physical subplans

> Status: proposed, 2026-08-27
>
> Scope: the interface between ASAPQuery-backend's control plane and its two
> executors — ASAPCollector (via OpAMP) and the ASAPQuery-backend data plane
> (via `BackendPlan`). This document replaces the implicit assumption, in
> earlier design notes, that a selected post-ASAP node can be serialized
> more or less directly into collector YAML. It complements
> [`design-asapplanner-workload-planner-migration.md`](design-asapplanner-workload-planner-migration.md)
> (the planner-migration boundary) and
> [`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
> (the backend half of the wire contract this document extends).

## 0. The boundary this document sits on

ASAPPlanner's own scope statement (`README.md`, "Scope") is explicit:

> not caring about CTSA stages i.e. whether a part of a plan is executed at
> the collector or at the analytics stage
> not caring about assignment of physical resources, like CPU threads and
> memory, to nodes in the ASAP plan

and `docs/design_docs/asap-aware-mapping/README.md`'s non-goals repeat this
for the mapping layer specifically: no CPU/memory assignment, no machine
placement, no scheduling, no admission control, no low-level execution
tuning. ASAPPlanner's own README names the resulting open question directly
("Open questions", #1): its output "has semantics of batch query execution
over data at rest" and "needs to be converted into two plans: (1) streaming
dataflow graph that computes summaries on raw data, and (2) batch query
execution plan that uses summaries to answer queries" — collector and
backend, in this deployment's vocabulary.

So: ASAPPlanner selects **what** replaces a query sub-DAG (which summary
family, algorithm, parameters, grouping layout, and how per-query readouts
compose over shared summary state). ASAPQuery-backend's control plane
decides **where** each piece of that selected DAG runs, **how** it is
represented on each wire, and **that** both sides agree they are running the
same decision. This document is the second half — the "where/how/that" —
concretely.

## 1. What ASAPPlanner hands us today

Grounded in `crates/types/src/post_asap/{mod,expr,schema,sketch}.rs` on
ASAPPlanner `main`, not carried over from older docs. The selected output of
`asap_aware_mapping::replacement::search_workload_with(...).global_selection(...)`
is a DAG of `Rc<SummaryNode>` (shared `Rc` = shared physical state — see
[migration doc](design-asapplanner-workload-planner-migration.md) §3), each
node one of:

- **`SummaryExpr::SummaryAgg { child, family, col, reduction, grouping }`**
  — the *update* side: consumes raw/plain input and produces summary state.
  `family: SummaryFamilyType` is one of `ExactAggregate(ExactKind,
  ExactParams)`, `Sketch(SketchKind, GroupingStrategy)` (`SketchKind` itself
  nests `category`/`algorithm`/`params` — `Kll`/`DDSketch`/`Hll`/`Cms`/
  `CmsWithHeap`/`Kmv`/`Theta`/`CountSketch`/`CountSketchWithHeap`, each with
  its own concrete `SketchParams`), `Sample(SamplingKind, SamplingParams)`,
  `Wavelet(WaveletKind, WaveletParams)`, or `StatModel(StatModelKind,
  StatModelParams)`. `reduction: Reduction` is `Reduce(GroupKeys)` or
  `PerEntity` (`crates/types/src/pre_asap/query_expr.rs`). `grouping:
  GroupingStrategy` is `PerSubpopulationInstance` (default) or
  `SharedMultiSubpopulation { kind: HydraKind, params: HydraParams }`.
- **`SummaryExpr::SummaryEstimate { summary_input, query }`** — the
  *readout* side: reads a `SketchQuery` (`Quantile`/`PointCount`/
  `Cardinality`/`TopK`) out of already-built summary state, producing a
  plain value. Summary-state typing does not propagate past this node.
- **`SummaryExpr::SummaryMerge { children }`** / **`SummarySubtract`** /
  **`SummaryDelete`** / **`SummaryJoin`** — combine or transform summary
  state; still summary-typed in/out, still on the readout side of any
  `SummaryEstimate` that eventually consumes them.
- **`SummaryExpr::KeepPreAsap(Rc<QueryExpr>)`** — no replacement chosen;
  executed against raw/archive data, never against collector-maintained
  state.

**Forward note on an open upstream PR.** ASAPPlanner PR
[#300](https://github.com/ProjectASAP/ASAPPlanner/pull/300) (open, not yet
merged) proposes to make exactly this update/readout distinction an
explicit, validated field: `post_asap::phase::ExecutionAvailability {
UpdateValue, SummaryState, ReadoutValue }`, with `SummaryAgg.child` typed to
accept only `UpdateValue` (or nested exact-accumulator state), and
`SummaryEstimate` typed `SummaryState -> ReadoutValue`. The split this
document defines (§2) is derived from the same structural fact —
`SummaryAgg` is the only node that *consumes* plain/update values and
*produces* summary state — so it does not depend on #300 landing, but it is
literally the same boundary #300 gives a name to. If/when #300 merges, §2's
partition rule should be re-expressed as "everything upstream of and
including a `PhaseAssignment` boundary at `SummaryState`" rather than
re-derived structurally; no other part of this design changes.

## 2. Why not compile 1:1, node-by-node, straight to collector YAML

A naive compiler would walk the selected DAG and, for each `SummaryAgg`,
emit one `asap_edge.metrics[]` entry with the same `family`/`col`/
`reduction` fields, then hand the whole thing to whichever process asks for
it. This does not work, for reasons that are all direct consequences of §0's
non-goals:

1. **No stage/edge/shard is chosen.** ASAPPlanner has no concept of "which
   collector process" or "how many shards" — `SummaryAgg` names a logical
   aggregation, not a physical instance of one. Something has to decide
   fan-out: one `SummaryAgg` shared by two queries might still be one
   physical summary; one `SummaryAgg` under high cardinality might be
   sharded across `shard_count` collector processes and merged with
   `SummaryMerge` before it ever reaches a `SummaryEstimate`. That decision
   is deployment placement, owned here, not upstream.
2. **No transport/physical parameters exist upstream.** `edge_id`,
   `window_duration`, `warm_allowed_lateness`, `drop_original`,
   `delta_transmission`, `delta_threshold` (see
   [ASAPCollector's OpAMP interface doc](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md))
   are bandwidth/latency/resource trade-offs a deployment makes; ASAPPlanner
   has no field for any of them and should not grow one (they are the
   physical-resource assignment its own non-goals name explicitly).
3. **Sharing and sharding both break 1-selected-node = 1-wire-fragment.** A
   shared `Rc<SummaryNode>` reached by two query roots must still be *one*
   collector-side summary and *one* backend-side `Materialization` with two
   `RoutingEntry` rows (see
   [`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
   §3 on why routing is a separate table). A single logical node sharded for
   cardinality must become *several* collector-side instances merged back
   into *one* backend-side materialization. Neither direction is a
   serialization concern; both require an explicit compile/allocate pass.
4. **Two independent readings of the same DAG can silently disagree.**
   The [migration plan](design-asapplanner-workload-planner-migration.md)
   requires one physical compile path for exactly this reason. If the collector
   subplan and the backend subplan are derived independently — even from
   the same selected DAG, by two different code paths, at two different
   times — nothing stops them drifting. A single compile step that emits
   both subplans from one pass over one selected DAG, stamped with one
   shared identity (§4), is what removes that possibility structurally
   instead of by convention.
5. **The two wires evolve independently and are consumed by different
   processes at different times.** OpAMP YAML is read by ASAPCollector;
   `BackendPlan` protobuf is read by `data_plane`. Neither should decode
   the other's format, and neither should decode ASAPPlanner's internal
   Rust IR — that IR is not a stable cross-process wire contract and was
   never meant to be one (migration plan §§2 and 4.2).

## 3. `CompiledPlan`: one compile step, two subplans, one identity

```rust
/// The output of compiling one `GlobalSelection` for one deployment
/// topology. This is control_plane's L5 (see
/// design-target-architecture.md §2, "L5 — physical plan": the one layer
/// this deployment owns in full because no upstream `asap-physical` crate
/// exists) — and it is the *only* thing that leaves the control plane's
/// planning boundary. Neither subplan is ever emitted independently of the
/// other; they are two views produced by the same compile call.
pub struct CompiledPlan {
    /// Shared identity across BOTH subplans. Content addressed from the
    /// selected DAG, topology identity, and semantic constraints. Mutable
    /// sizing and lifecycle values are deliberately excluded and ordered by
    /// `plan_version` instead.
    pub plan_id: PlanId,
    /// Monotonic per-`plan_id` counter — bumped on re-compile against an
    /// unchanged selection (e.g. a resize), not on every replan.
    pub plan_version: u64,
    /// Not-before: neither subplan should be treated as authoritative
    /// before this time. Lets a warm cutover (see migration plan §9,
    /// `DeploymentPlanDiff`) land both subplans ahead of the switch.
    pub activation: DateTime<Utc>,
    /// Not-after / supersede horizon. `None` for "until superseded."
    pub expiry: Option<DateTime<Utc>>,
    /// Identifies the *wire schema version* the backend subplan requires,
    /// so a collector/backend pair that somehow ends up on mismatched
    /// deploys fails a compatibility check instead of silently serving
    /// under the wrong contract. Distinct from `plan_id`: this changes on
    /// a schema/deploy version bump, not on every replan.
    pub backend_compat: BackendCompatId,

    pub collector: CollectorSubplan,
    pub backend: BackendSubplan,
}

pub struct CollectorSubplan {
    pub plan_id: PlanId,           // == CompiledPlan::plan_id
    pub plan_version: u64,         // == CompiledPlan::plan_version
    pub activation: DateTime<Utc>,
    pub expiry: Option<DateTime<Utc>>,
    pub backend_compat: BackendCompatId,
    /// One entry per collector fleet member this plan touches.
    pub edges: Vec<EdgeAssignment>,
}

pub struct EdgeAssignment {
    pub edge_id: String,
    /// The versioned CollectorPlan fields (§5), produced by compiling the
    /// `SummaryAgg` nodes assigned to this edge, not authored ad hoc.
    pub config: AsapEdgeConfig,
    /// Opaque identity of *this edge's* exact YAML body — unchanged
    /// semantics from ASAPCollector's existing `config_hash` (it still
    /// identifies collector-config bytes, nothing more); `plan_id` is the
    /// new, separate field that identifies the plan those bytes were
    /// compiled from.
    pub config_hash: ConfigHash,
}

pub struct BackendSubplan {
    pub plan_id: PlanId,           // == CompiledPlan::plan_id
    pub plan_version: u64,         // == CompiledPlan::plan_version
    pub activation: DateTime<Utc>,
    pub expiry: Option<DateTime<Utc>>,
    pub backend_compat: BackendCompatId,
    /// `BackendPlan` from design-backend-plan-wire-format.md §3. Its
    /// envelope fields equal this `BackendSubplan` and the matching
    /// `CollectorSubplan`.
    pub backend_plan: BackendPlan,
}
```

## 4. Compile algorithm

Input: the materialized selection (`GlobalSelection::materialize()`'s
`Rc<SummaryNode>` roots, with the shared identity required by migration plan
§4.2 intact) plus this deployment's topology and
constraints (collector fleet membership, per-edge shard/memory budgets,
transport cost model — the same inputs `physical::colored_dag` already
takes today, see §8).

1. **Partition by node kind**, not by heuristic: every `SummaryAgg`
   reached anywhere in the selection is an *update-side* node; every
   `SummaryEstimate`/`SummaryMerge`/`SummarySubtract`/`SummaryDelete`/
   `SummaryJoin` is a *readout-side* node (it consumes summary state and
   either produces more summary state for further readout-side composition,
   or a plain value). `KeepPreAsap` subtrees are neither — they stay the
   backend/archive fallback path already described in
   [`design-target-architecture.md`](design-target-architecture.md) §3.
2. **Allocate each distinct `SummaryAgg` (by `Rc` identity) to one or more
   collector edges.** A single logical `SummaryAgg` may become several
   `EdgeAssignment` entries (sharding by cardinality/volume budget) or share
   one existing edge with another `SummaryAgg` from a different query root
   (the shared-`Rc` case). This is where `shard_count`, `edge_id` selection,
   and per-edge resource budgeting happen — genuinely new information, not
   copied from the selection.
3. **Insert `SummaryMerge` at the shard boundary** when step 2 sharded a
   node: the collector side ships `shard_count` partial states: the backend
   side's `Materialization` reflects one logical summary, reconciled via
   merge before any `SummaryEstimate` reads it (`SummaryMerge`'s own
   catalog-`mergeable` requirement, already enforced upstream, is what makes
   this legal at all).
4. **Compile every readout-side subtree into `Materialization` +
   `RoutingEntry` rows** in the backend subplan (§7), each one recording
   which `EdgeAssignment`(s) supply its input summary state.
5. **Decide transport parameters** (`delta_transmission`, `delta_threshold`,
   `drop_original`, `warm_allowed_lateness`) per `EdgeAssignment` from the
   deployment cost model — never from the selection, which has no opinion
   on transport (§2.2).
6. **Compute `plan_id`** from the compiled structure (§3), stamp it plus
   `plan_version`/`activation`/`expiry`/`backend_compat` identically onto
   both subplans, and return the `CompiledPlan`.

Steps 2–3 are exactly the job `physical::allocator::SketchAllocator` and
`physical::stage_split` already do today against the *legacy* locally-typed
`QueryExpr`/`PipelineStage` tree (`physical/plan.rs`); this document asks
for the same allocation job, retargeted to consume ASAPPlanner's own
`SummaryNode` selection instead of a parallel local IR — see §9.

## 5. Collector subplan wire contract

The authoritative collector-side schema is ASAPCollector's
[`ASAPQuery-to-ASAPCollector collection-plan interface`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).
This document does not define a second flat `asap_edge.metrics[]` schema.

For each `EdgeAssignment`, the compiler emits one versioned
`CollectorPlan` YAML document in the exact OpAMP `AgentConfigMap` entry
`asap-collector-plan.yaml`, with content type `application/yaml`. The OpAMP
protobuf is the transport envelope; the YAML document is the typed physical
execution contract. It is not a serialized ASAPPlanner Rust DAG and it is
not a complete OTel Collector configuration.

The compiler maps the selected DAG into that schema as follows:

| Selected post-ASAP field | `CollectorPlan` field |
| --- | --- |
| `SummaryAgg` identity | `materializations[].logical_node_ref` plus a content-addressed `materializations[].id` |
| bound `Source` and predicates | `materializations[].input.metric` and canonical `input.matchers` |
| `SummaryAgg.col` | `materializations[].input.value` |
| `SummaryFamilyType` | `materializations[].summary.family` |
| sketch algorithm and parameters | `summary.algorithm` and typed `summary.parameters` |
| Planner accuracy constraint | `summary.accuracy` |
| `Reduction::PerEntity` | `reduction.kind: per_entity` |
| `Reduction::Reduce(GroupKeys)` | `reduction.kind: reduce`, explicit `by`, and `without` |
| `GroupingStrategy` | `grouping.kind`, plus Hydra kind/parameters for shared grouping |

The physical compiler adds fields ASAPPlanner intentionally does not own:
target agent/edge, capability snapshot, concrete streaming windows, local
shards, exporter reference, and raw/full/delta transmission policy. These
fields must never be inferred by ASAPCollector from missing values.

The complete plan envelope carries `plan_id`, `plan_version`, `activation`,
`expiry`, and `backend_compat` verbatim from `CompiledPlan`. Each emitted
summary or delta also carries those compatibility identities plus its
materialization, window, producer, and sequence/checkpoint identity.

Unsupported Planner alternatives remain visible in the logical candidate
space but cannot be emitted unless the targeted collector capability snapshot
and backend compatibility ID both support them. The compiler chooses another
valid candidate or exact fallback; it never renames an unsupported algorithm
to a similar supported one. In particular, shared Hydra grouping must not be
silently flattened to independent per-group state.

## 6. Gaps this closes vs. what it still leaves open

**Closes in the target design**, on the ASAPCollector side: the
`CollectorPlan` envelope now has explicit `plan_id`, `plan_version`,
`activation`, `expiry`, and `backend_compat` fields, carried identically on
the matching backend plan. It is delivered as the
`asap-collector-plan.yaml` OpAMP config-map entry. The collector returns the
semantic result through the `io.asap.collector.plan.v1` /
`application_report` custom message. The MVP harness compares the active
plan and materialization identities on both sides instead of treating
OpAMP's `config_hash` or `RemoteConfigStatus.APPLIED` as proof of semantic
activation.

This remains a target contract rather than a claim about current runtime
behavior. ASAPCollector currently writes a complete OTel YAML file, restarts,
and reports only the OpAMP config hash after a syntax check. Implementing the
new parser, atomic activation, and application report is a separate code
change.

**Opens**, in ASAPCollector's execution layer: the target schema can name all
Planner families and grouping layouts, but `cms_with_heap`,
`count_sketch_with_heap`, KMV, Theta, sampling, wavelets, statistical models,
and shared Hydra grouping still require actual collector and backend support.
Naming an algorithm in the schema does not advertise that runtime support.

**Stays open**, and is explicitly out of scope here: the rollup algebra
question already on record in
[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
§7 ("which summary families roll up safely"), and the composed exact/summary
execution gaps tracked against ASAPPlanner PR #300 / issue #171.
`CompiledPlan` treats a `RollupStrategy` selection the same as any other
readout-side subtree (§4 step 4) — it does not independently re-derive
rollup legality, which remains ASAPPlanner's decision to have made during
selection.

## 7. Backend subplan materialization shape

[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
§3 carries ASAPPlanner's current `SummaryFamilyType` directly. That type
nests algorithm and parameters for sketches and retains the orthogonal
`GroupingStrategy` axis; the backend wire must not re-flatten it into a local
`SummaryKind`/`SummaryParams` vocabulary:

```rust
pub struct Materialization {
    pub fingerprint: PolicyFingerprint,
    pub source: Source,
    pub window: WindowSpec,
    pub group_by: Vec<GroupKey>,
    pub rollup: Vec<GroupKey>,

    /// Current upstream type directly, including sketch grouping layout.
    pub family: SummaryFamilyType,
    pub col: ColumnRef,

    /// New: which `EdgeAssignment`(s) this materialization's input summary
    /// state comes from. Lets the backend validate, at plan-apply time,
    /// that the collector subplan sharing this `CompiledPlan::plan_id`
    /// actually produces a `family`-compatible input — the concrete
    /// mechanism behind the migration doc's completion criterion
    /// ("Backend applies an incompatible plan -> reject the emitted state
    /// or fail the run").
    pub sources: Vec<EdgeSourceRef>,

    pub retention: Option<RetentionPolicy>,
}

pub struct EdgeSourceRef {
    pub edge_id: String,
    pub materialization_id: PolicyFingerprint,
}
```

`BackendPlan.plan_id` is the field joining the two subplans. Content-addressed
`PolicyFingerprint` remains the identity of one `Materialization`
(reuse/diff/resize within a single backend subplan, per the migration doc's
`DeploymentPlanDiff`); `plan_id` answers a different question — "were these
two subplans compiled together" — that a per-materialization fingerprint
cannot answer.

## 8. What does not change

- `PolicyFingerprint`, `RoutingIndex`, hot reload, `DeploymentPlanDiff`,
  warm cutover, and archive fallback — all as designed in
  `design-backend-plan-wire-format.md` and
  `design-asapplanner-workload-planner-migration.md` §5/§6.
- OpAMP's `AgentRemoteConfig`/`AgentConfigMap`/`config_hash` delivery
  mechanics. `config_hash` still identifies exact remote-config bytes; it
  does not replace `plan_id` or the semantic application report.
- Existing `asap_edge` runtime behavior until the versioned `CollectorPlan`
  parser and apply path are implemented.

## 9. Migration notes

- `physical::plan::PlanNode` / `PipelineStage` / `physical::allocator::
  SketchAllocator` / `physical::stage_split` operate on this repo's own
  locally-typed `QueryExpr` (`crate::intent_algebra`), annotated with a
  per-node `PipelineStage` tag on one combined tree. `CompiledPlan` replaces
  that shape with two explicit typed subplans compiled from ASAPPlanner's
  own selected `SummaryNode` DAG. This module belongs on the
  migration plan §9 Phase 6 removal list — remove it only after the compiled
  plan path is selected and rollback no longer depends on legacy planning.
- `emit::agent::generate_agent_collector_config` currently builds a complete
  collector YAML. It should become the `EdgeAssignment -> CollectorPlan`
  serializer defined in §5. Bootstrap OTel receivers/exporters and credentials
  remain deployment configuration; a workload replan must not replace them.
- Both subplans should land behind the same workload-planner rollout mode
  the migration plan defines (§9 Phase 5): in
  `shadow` mode, compile `CompiledPlan` and record `plan_id` agreement and
  field-level diffs against the legacy allocator's output without pushing
  either subplan, exactly mirroring that section's existing comparison
  list.

## 10. Open questions
- **`backend_compat` granularity.** One id per `BackendPlan` proto schema
  version, or one per `(schema version, family vocabulary version)` so an
  `asap_edge` schema gap closing (§6) doesn't force every unrelated plan to
  recompute compatibility — needs a decision before the field ships, not
  after.
- **Cross-shard `SummaryMerge` placement.** Step 3 (§4) inserts
  `SummaryMerge` "at the shard boundary" without saying which physical
  stage performs it — collector-side gateway merge vs. backend-side merge
  on ingest are both live options already implied by `PipelineStage`'s
  existing `Backend`/`Precompute` distinction, and the choice affects
  `EdgeAssignment` fan-in bandwidth materially. Needs its own short design
  pass, not resolved here.
