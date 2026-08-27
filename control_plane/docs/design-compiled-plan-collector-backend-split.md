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
   [`design-asapplanner-workload-planner-migration.md`](design-asapplanner-workload-planner-migration.md)
   §2 already names this failure mode for the legacy planner ("the control
   plane, data plane, and ASAPPlanner silently select different physical
   summary families or parameters for the same query"). If the collector
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
   never meant to be one (`design-asapplanner-workload-planner-migration.md`
   §5/PR4: *"Do not copy `dag_export`'s JSON into the runtime contract and
   do not infer mappings from labels, hashes, strategy rationale, or viewer
   node signatures."*).

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
    /// Shared identity across BOTH subplans — the field this document adds
    /// to close ASAPCollector's own documented gap (§6 below). Content
    /// addressed: a hash of the selected DAG's structure plus the
    /// deployment topology/constraints the compiler ran against, so two
    /// compiles of the same selection against the same topology produce
    /// the same `plan_id` and two different selections never collide.
    pub plan_id: PlanId,
    /// Monotonic per-`plan_id` counter — bumped on re-compile against an
    /// unchanged selection (e.g. a resize), not on every replan.
    pub plan_version: u64,
    /// Not-before: neither subplan should be treated as authoritative
    /// before this time. Lets a warm cutover (see migration doc §6.5,
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
    pub backend_compat: BackendCompatId,
    /// One entry per collector fleet member this plan touches.
    pub edges: Vec<EdgeAssignment>,
}

pub struct EdgeAssignment {
    pub edge_id: String,
    /// Today's `asap_edge` processor fields (§5) — produced by compiling
    /// the `SummaryAgg` nodes assigned to this edge, not authored ad hoc.
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
    /// Today's `BackendPlan` (design-backend-plan-wire-format.md §3), with
    /// one change: `BackendPlan::plan_id` stops being "observability only"
    /// (its current doc comment) and becomes literally
    /// `CompiledPlan::plan_id` — see §6.
    pub backend_plan: BackendPlan,
}
```

## 4. Compile algorithm

Input: the materialized selection (`GlobalSelection::materialize()`'s
`Rc<SummaryNode>` roots, per
[migration doc](design-asapplanner-workload-planner-migration.md) §5/PR4 —
shared node identity intact) plus this deployment's topology and
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

## 5. Redesigning `asap_edge.metrics[]` to match the DAG's own shape

The compile step's output should not reinvent a second vocabulary for what
the selected DAG already names. Checked against the real processor, not a
doc summary of it —
[`opentelemetry-collector-contrib-patch/processor/asapedgeprocessor/config.go`](https://github.com/ProjectASAP/ASAPCollector/blob/main/opentelemetry-collector-contrib-patch/processor/asapedgeprocessor/config.go) —
today's `MetricFamily` struct does exactly that in two places: `Mode` is a
bare string validated to exactly two values (`precompute.ParseAggMode`
accepts only `per_series`/`whole_stream`), and `AggregateBy []string` has no
way to express `GroupKeys.without`. Neither can distinguish
`Reduction::PerEntity` from a genuine zero-key `Reduce` — the exact
ambiguity ASAPPlanner's own `Reduction` type was introduced to remove
(issue #163, per `crates/types/src/post_asap/expr.rs`'s own doc comment on
`SummaryAgg.reduction`). `Family` is a flat string covering only `sum`/
`ddsketch`/`kll`/`hll`/`countsketch`/`countminsketch`, with no discriminator
for the other four `ExactKind` variants and no field at all for
`GroupingStrategy`.

Go's `mapstructure` decoding has no polymorphic nested-union support the way
`serde` does, and this file's own existing pattern (`FamilyKind`/`Tier`/
`ColdFormat`) is already flat discriminator-plus-sibling-fields, not
nesting. So "aligned with the DAG" here means *the same flat shape*, with
every field name and enum spelling drawn directly from `post_asap` — not a
Rust-style nested tagged union grafted onto a struct that was never built to
decode one:

```go
// today                              // redesigned
type MetricFamily struct {            type MetricFamily struct {
    Metric      string                    Source    string     // SummaryAgg.col
    Family      FamilyKind                Family    FamilyKind // exact|ddsketch|kll|hll|
                // sum|ddsketch|kll|                // cms|count_sketch|
                // hll|countsketch|                 // cms_with_heap|
                // countminsketch                   // count_sketch_with_heap|
                                                      // kmv|theta
    AggregateBy []string                  ExactKind ExactKind  // sum|count|min_max|
    // Mode: "" | "per_series" |                     // increase|rate — read only
    // "whole_stream" — free string,                 // when family=exact
    // no per_entity / without concept
    Mode        string                    // Reduction, named directly.
                                           // PerEntity excludes ReduceBy/
                                           // ReduceWithout — Validate() enforces it.
                                           ReduceBy      []string
                                           ReduceWithout bool
                                           PerEntity     bool

                                           // GroupingStrategy — no field
                                           // existed before.
                                           Grouping      GroupingKind // per_subpopulation_instance
                                                                       // (default) |
                                                                       // shared_multi_subpopulation
                                           HydraKind     string
                                           SharedRows    uint32
                                           SharedColumns uint32

    RelativeAccuracy, K, Rows, Cols        RelativeAccuracy, K, Rows, Cols
    // ...ItemLabel, SampleP,              // ...unchanged, see below
    // MaxSeries, Tier, SpatialFilter,
    // GosDeltaEpsilon, GosSites,
    // EmitHeap, HeapSize, WeightMode,
    // Threshold, HLLSparse
}                                      }
```

Worked example — p99 latency by `(service, region)`, an exact per-zone
sum, and a Hydra-CMS unique-IP count by zone:

```yaml
metrics:
  - source: request_duration_seconds
    family: ddsketch
    reduce_by: [service, region]
    reduce_without: false
    relative_accuracy: 0.01
    delta_transmission: true

  - source: page_views
    family: sum
    exact_kind: sum
    reduce_by: [zone]

  - source: unique_ips
    family: cms
    grouping: shared_multi_subpopulation
    hydra_kind: cms
    shared_rows: 4
    shared_columns: 2048
    reduce_by: [zone]
    rows: 4
    cols: 2048
```

Two consequences fall out of the realignment itself, not as separate
follow-ups: an `exact_kind` slot and a `grouping`/`hydra_kind` slot exist
because every `post_asap` variant now has somewhere to go — they were
"gaps" in the old flat vocabulary specifically because that vocabulary was
invented independently of `SummaryFamilyType`/`GroupingStrategy` rather than
read off them. `cms_with_heap`/`count_sketch_with_heap`/`kmv`/`theta` are
named for completeness; the processor doesn't implement them yet (today's
`EmitHeap: true` on `count_sketch` approximates `CountSketchWithHeap` as a
special case) — naming the slot doesn't imply the runtime behind it exists.

`edge_id`/`shard_count`/`window_duration`/`warm_allowed_lateness`/
`drop_original` stay top-level `Config` fields, and `tier`/`spatial_filter`/
`sample_p`/`max_series`/`item_label`/`delta_transmission`/`delta_threshold`/
`gos_delta_epsilon`/`gos_sites`/`emit_heap`/`heap_size`/`weight_mode`/
`hll_sparse`/`threshold{…}`/`cold{…}`/`control_channel{…}` all stay exactly
as they are on `MetricFamily` — collector-implementation and deployment
knobs with no `post_asap` counterpart to align to. Realigning them would
mean inventing DAG concepts that don't exist, the same mistake in reverse.
`item_label` in particular still comes from the deployment's `Frequency`
extension realization (`design-target-architecture.md`'s
`CostModel::realize_extension`), the same place it's produced today.

This is a schema proposal, not a claim that ASAPCollector has implemented
it. `Source`/`ReduceBy`/`ReduceWithout`/`PerEntity`/`Grouping`/`HydraKind`/
`SharedRows`/`SharedColumns`/`ExactKind` do not exist on `MetricFamily`
today; `Metric`/`AggregateBy`/`Mode` remain the only way to express this
today. A migration should decode both old and new field names for one
release (`Metric`→`Source`, `AggregateBy`→`ReduceBy`, `Mode` derived from
`PerEntity`/`ReduceWithout`) rather than break existing deployed configs on
cutover.

## 6. Gaps this closes vs. what it still leaves open

**Closes**, on the ASAPCollector side (its own documented gap, verbatim from
[`opamp-config-push.md`](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md)'s
"Current contract gap" section): *"the implemented OpAMP YAML schema
currently has no explicit `plan_id`, `plan_version`, activation time, expiry
time, or backend compatibility identifier. `config_hash` identifies the
remote collector configuration; it is not a complete versioned end-to-end
plan contract."* `CompiledPlan`'s envelope (§3) is exactly those five
fields, carried on both `CollectorSubplan` and `BackendSubplan`. The MVP
harness (per that same doc) can now compare `plan_id`/`plan_version`
reported by a collector's `AgentToServer` health/status against the
`plan_id`/`plan_version` the backend reports as active, instead of only
having `config_hash` (which proves the collector loaded *some* YAML, not
that it's the YAML compiled alongside the currently-active `BackendPlan`).
This is additive to — not a replacement for — ASAPCollector's own
`AgentRemoteConfig`/`config_hash` mechanics; see that document for exactly
where in the OpAMP message envelope these fields should be encoded (an
ASAPCollector-side decision this document does not make unilaterally).

**Opens**, in `asap_edge`'s own schema (ASAPCollector-owned — §5's redesign
proposes the shape, but implementing it there is a separate, tracked change,
not something this document does unilaterally): the `exact_kind` and
`grouping`/`hydra_kind` slots §5 proposes cover every `post_asap` variant
that exists today, but `cms_with_heap`/`count_sketch_with_heap`/`kmv`/
`theta` name families the processor doesn't build yet — exercising them
still needs real runtime support, not just a schema slot. None of that is
exercised by this deployment's current MVP metric set, so implementing the
unbuilt families is out of scope for the first `CompiledPlan`
implementation; §5's redesign should not be read as "these are all the
families `asap_edge` will ever need" — only as "every family that exists
today has somewhere to go."

**Stays open**, and is explicitly out of scope here: the rollup algebra
question already on record in
[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
§7 ("which `SummaryKind`s roll up safely"), and the composed exact/summary
execution gaps tracked against ASAPPlanner PR #300 / issue #171.
`CompiledPlan` treats a `RollupStrategy` selection the same as any other
readout-side subtree (§4 step 4) — it does not independently re-derive
rollup legality, which remains ASAPPlanner's decision to have made during
selection.

## 7. Backend subplan: one correction to the existing `Materialization` shape

[`design-backend-plan-wire-format.md`](design-backend-plan-wire-format.md)
§3 defines `Materialization.kind: SummaryKind` / `params: SummaryParams` as
a flat pair, citing `asap_sketch::SummaryKind`/`SummaryParams`. Those exact
type names do not exist in ASAPPlanner's current `crates/types::post_asap`
(§1) — the flat-pair shape predates the current IR, which nests kind+params
*per family* inside `SummaryFamilyType` (and nests a further
algorithm+params level specifically for `Sketch`, plus the orthogonal
`GroupingStrategy` axis). `Materialization` should be updated to carry the
current type directly, the same "reuse the canonical vocabulary, don't
re-flatten it" principle §3 of that document already states as its own
goal:

```rust
pub struct Materialization {
    pub fingerprint: PolicyFingerprint,
    pub source: Source,
    pub window: WindowSpec,
    pub group_by: Vec<GroupKey>,
    pub rollup: Vec<GroupKey>,

    /// Was `kind: SummaryKind, params: SummaryParams`. Now the current
    /// upstream type directly — carries grouping layout for `Sketch` too,
    /// which the old flat pair had no field for at all.
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
    pub metric: String, // matches an EdgeAssignment's asap_edge.metrics[].metric
}
```

`BackendPlan.plan_id`'s doc comment ("observability only, not identity") no
longer holds under this design — see §3: it becomes the field two subplans
are joined on. Content-addressed `PolicyFingerprint` remains correct as the
identity of one `Materialization` (reuse/diff/resize within a single
backend subplan, per the migration doc's `DeploymentPlanDiff`); `plan_id`
now answers a different question — "were these two subplans compiled
together" — that `PolicyFingerprint` was never meant to answer.

## 8. What does not change

- `PolicyFingerprint`, `RoutingIndex`, hot reload, `DeploymentPlanDiff`,
  warm cutover, and archive fallback — all as designed in
  `design-backend-plan-wire-format.md` and
  `design-asapplanner-workload-planner-migration.md` §5/§6.
- ASAPCollector's `AgentRemoteConfig`/`AgentConfigMap`/`config_hash`
  mechanics and apply/restart semantics — unchanged; this document adds
  envelope fields alongside them, per §6.
- The `asap_edge` processor's already-documented fields (§5's left two
  columns) — extended, not replaced.

## 9. Migration notes

- `physical::plan::PlanNode` / `PipelineStage` / `physical::allocator::
  SketchAllocator` / `physical::stage_split` operate on this repo's own
  locally-typed `QueryExpr` (`crate::intent_algebra`), annotated with a
  per-node `PipelineStage` tag on one combined tree. `CompiledPlan` replaces
  that shape with two explicit typed subplans compiled from ASAPPlanner's
  own selected `SummaryNode` DAG. This module belongs on the
  [migration doc](design-asapplanner-workload-planner-migration.md) §4.1
  removal list (it is not currently listed there) — add it once PR4/PR5 of
  that stack lands, not before, since it is still the live path until then.
- `emit::agent::generate_agent_collector_config` currently builds one
  processor keyed by `cfg.sketch_type` per collector — a shape that
  predates the unified `asap_edge` processor with a `metrics[]` list that
  ASAPCollector's own OpAMP doc now documents as canonical. It should become
  the `EdgeAssignment -> asap_edge YAML` serializer described here, which is
  a strictly larger rewrite than a field-mapping change — flagging it here
  so it isn't mistaken for a small follow-up.
- Both subplans should land behind the same `ASAP_WORKLOAD_PLANNER_V2`
  shadow-rollout flag the migration doc already proposes (§5/PR8): in
  `shadow` mode, compile `CompiledPlan` and record `plan_id` agreement and
  field-level diffs against the legacy allocator's output without pushing
  either subplan, exactly mirroring that section's existing comparison
  list.

## 10. Open questions

- **Where in the OpAMP envelope do `plan_id`/`plan_version`/`activation`/
  `expiry`/`backend_compat` live?** A sibling top-level YAML key next to
  `processors.asap_edge`, a field inside `asap_edge` itself, or a separate
  `AgentConfigFile` entry — this is ASAPCollector's schema to own; this
  document only establishes that the fields must exist and must be
  identical to the backend subplan's copy.
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
