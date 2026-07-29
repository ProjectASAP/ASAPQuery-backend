# `control_plane` target architecture — planning-time and serving-time

> **Status.** Target-state design, written independent of what's currently
> implemented in `control_plane`/`data_plane`. Grounded entirely in
> ASAPController's own current interfaces — specifically
> [`docs/l1-query-language.md`](https://github.com/ProjectASAP/ASAPController/blob/main/docs/l1-query-language.md)
> through
> [`l5-physical-plan.md`](https://github.com/ProjectASAP/ASAPController/blob/main/docs/l5-physical-plan.md)'s
> `## Interface` sections, added in
> [ASAPController#169](https://github.com/ProjectASAP/ASAPController/pull/169)
> (every signature there verified against ASAPController `main` at
> `cc18c98`, 2026-07-28) — not carried over from this repo's own history or
> assumed from memory. Where this doc says "should," it is describing a
> target, not certifying that the current code already matches it; see §4
> for the gap.

## 0. The one-sentence version

`control_plane` should be a **thin planning-time shell** around
ASAPController's `asap-ir`/`asap-l2`/`asap-plan`/`asap-sketch` crates,
contributing exactly two things ASAPController doesn't ship: an L5
physical-planner implementation (ASAPController has no `asap-physical`
crate — L5 is speculative there, real here) and deployment-specific L4
extension points (`CostModel`, `Matcher`). `data_plane` should be a thin
**serving-time shell**, contributing exactly one thing: a `SummaryExecutor`
implementation. Planning and serving are two separate interfaces per
ASAPController's own `design.md` §"Serving-time execution" — this doc
treats them as two separate sections for the same reason.

## 1. Planning vs. serving — the split ASAPController's own design draws

```mermaid
flowchart LR
    Q["query string"] --> P["PLANNING\nL1 → L2 → L3 → L4"]
    P --> L4N["L4Node\n(symbolic plan)"]
    L4N --> E["SERVING\nSummaryExecutor::execute"]
    E --> V["answer"]
```

- **Planning** turns a query string into a plan. It has, by design, no
  reference to what's actually materialized anywhere — it symbolically
  picks a summary family and parameters from the query shape and an
  accuracy target alone.
- **Serving** walks an already-decided plan against whatever is actually
  materialized *right now* and produces an answer. Reality can diverge
  from the plan in ways planning never sees — missing data, multiple
  instances needing a merge, instances that disagree on parameters —
  which is why serving needs its own error vocabulary, distinct from
  planning's.
- In this deployment, `control_plane` runs **in-process** with
  `data_plane` (same binary), so calling `control_plane`'s planning entry
  point from `data_plane` at request time is a same-binary library call,
  not a network hop or a second planning implementation living in
  `data_plane`. That's a deployment-specific convenience this topology
  affords, not something ASAPController assumes of every deployment.

## 2. Planning-time — `control_plane`'s job

### L1 — query language

ASAPController's L1 interface (no shared trait; converging free functions):

```rust
pub fn lower_promql(query: &str, accuracy: AccuracyTarget) -> Result<QueryExpr, PromqlError>;
pub async fn lower_sql(query: &str, catalog: &SqlCatalog, accuracy: AccuracyTarget) -> Result<QueryExpr, SqlError>;
```

**Target: `control_plane` calls `asap_frontend_promql::lower_promql` directly.**
No local PromQL parser. SQL support (currently 0% adopted in this repo) is
`asap_frontend_sql::lower_sql` the same way, gated behind whatever schema
catalog this deployment can supply (`SqlCatalog` — see L2 below; PromQL has
none, SQL needs a real one).

### L2 — logical plan / schema binding

```rust
pub trait SchemaCatalog { fn columns_for(&self, source: &str) -> Option<Vec<Column>>; }
pub struct Binder<C: SchemaCatalog = UsageDerivedCatalog>;
pub fn convert_root(legacy: &QueryExpr, accuracy: &AccuracyTarget) -> Result<QueryExpr, ConvertError>;
```

**Target: no `control_plane`-local `SchemaCatalog` for PromQL** — this
deployment's metrics have no real catalog (a metric's label set is only
knowable from what a query references), so the default `Binder::default()`
(`UsageDerivedCatalog`) is correct as-is, same as ASAPController's own
PromQL front end uses. `convert_root` is called *inside*
`lower_promql`/`lower_sql` already — `control_plane` never calls it
directly.

### L3 — intent algebra

The canonical `QueryExpr`/`AggIntent`/`Schema` vocabulary (see PR #169's
`l3-intent-algebra.md` diff for the full ~30-variant `AggIntent` list this
deployment should treat as canonical, not its own subset).

**Target: `control_plane` has zero local L3 type definitions.** Every
`QueryExpr`/`AggIntent`/`Schema` reference is `asap_ir::intent_algebra::*`,
used directly — no wrapper types, no local re-derivation of accuracy-math
helpers (`hll_accuracy`, `default_cardinality`, etc. — those already live
in `asap_ir` and should be called there, not duplicated locally).

**One acknowledged, deliberate exception**, matching ASAPController's own
design rule ("core treats `Extension` opaquely; the owning deployment
defines and interprets `payload` itself"): this deployment's `Frequency`
capability (`AggIntent::Extension{ext_kind: "frequency", ..}` — per-series
sample-count estimation via CMS/CountSketch, with no upstream equivalent)
is genuinely deployment-specific vocabulary layered *on top of* the
canonical `AggIntent`, not a competing definition of it. It's implemented
as free functions (`frequency()`/`as_frequency()`) wrapping the foreign
`AggIntent` type (Rust's orphan rules require this), not a fork of the
type itself.

### L4 — cost-aware binding + summary-bound IR

```rust
pub fn implement_tree_with(expr: &QueryExpr, cost_model: &dyn CostModel) -> Result<Rc<L4Node>, ImplementError>;
pub trait CostModel {
    fn rank_candidates(&self, intent: &AggIntent, candidates: &[SummaryKind]) -> Vec<SummaryKind>;
    fn size_params(&self, kind: SummaryKind, intent: &AggIntent, eps: f64, delta: f64) -> SummaryParams { .. }
    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Implementation { .. }
    fn readout_extension(&self, ext_kind: &str, payload: &serde_json::Value, col: &ColumnRef) -> SketchQuery { .. }
}
```

**Target: `control_plane` owns exactly one `CostModel` impl and nothing
else at this layer.** `implement_tree_with` + the `L4Node`/`SummaryExpr`
IR are pure ASAPController types, used directly, not wrapped. The
`CostModel` is the single deployment-specific extension point this layer
grants:

- `rank_candidates` — this deployment's accuracy-bound-driven family
  preference (KLL vs. DDSketch for quantiles, etc.).
- `size_params` — real parameter sizing from an accuracy target, not the
  crate's built-in default formulas (this deployment has its own
  calibrated cost table).
- `realize_extension`/`readout_extension` — the two hooks that make the
  `Frequency` extension (§ above) actually work: turning
  `Extension{"frequency", ..}` into a real `Implementation::Summary{Cms/CountSketch,..}`
  and answering a readout against it. This is *the* mechanism, not a
  workaround — ASAPController's `CostModel` trait exists specifically so a
  deployment can plug in exactly this kind of opaque-extension realization
  without touching `asap-plan`'s binding pass.

No local `PhysicalExpr`/`L4Plan` wrapper type is needed for the IR itself
— `Rc<L4Node>` is the L4 output, used directly by L5 below. (A thin L5
*placement* wrapper is still legitimate — see next section — but it should
wrap `Rc<L4Node>`, not reinvent the L4 algebra inside it.)

### L5 — physical plan (control_plane's real, load-bearing contribution)

ASAPController has **no `asap-physical` crate** — confirmed: `crates/` on
`main` is `{ir, l2, sketch, plan, frontend-promql, frontend-sql, lower,
e2e}`, no fifth entry. L5's interface section in `l5-physical-plan.md` is
explicitly marked speculative — "a concrete target to design against, not
an API to depend on." **This is the one layer `control_plane` should own
in full**, implemented *against* that speculative shape so a future real
`asap-physical` crate absorbs it with minimal churn, not built to some
other, incompatible pattern:

```rust
pub trait PhysicalPlanner {
    type Topology: TopologyDescriptor;
    type Output;
    fn lower(&self, l4: Rc<L4Node>, t: &Self::Topology) -> Result<Self::Output, PlanError>;
}
pub trait TopologyDescriptor {
    fn stages(&self) -> &[StageDescriptor];
    fn edges(&self) -> &[StageEdge];
}
pub struct StageAllocator;
impl StageAllocator {
    pub fn allocate<T: TopologyDescriptor>(&self, l4: Rc<L4Node>, topology: &T, c: &DeploymentConstraints)
        -> Result<Vec<StageAssignment>, PlanError>;
}
```

Concretely, for this deployment:

- **`Topology`**: a 3-stage topology (`edge` / `gateway` / `backend`) —
  this repo's existing `physical::colored_dag` machinery is structurally
  the right shape (`StageId`, per-stage allocator, per-stage emitter);
  it should be read as *this deployment's* `TopologyDescriptor` +
  `StageAllocator` + `PhysicalPlanner` impl, not a bespoke thing outside
  ASAPController's pattern.
- **`Executor`**: an OpAMP-addressed agent/gateway process or an HTTP-addressed
  backend — `address: ExecutorAddr` maps onto this repo's existing
  OpAMP-connection / backend-HTTP-endpoint machinery.
- **Output** (`PhysicalPlanner::Output`): per-executor configuration —
  OpAMP `RemoteConfig` (OTel-collector YAML) for edge/gateway executors,
  `StreamingConfig` JSON POST for the backend executor. This is exactly
  what `emit::{emit_edge_yaml, emit_gateway_yaml, emit_backend_streaming_config_json}`
  already produce — L5's job is *stage allocation + emission format*, and
  that's genuinely this repo's own concern, not something to strip out or
  route through ASAPController.

**This is the load-bearing implication of "no `asap-physical` crate
exists": `control_plane`'s physical-planning code is not legacy debt to
retire — it's the actual product.** Everything upstream of it (L1-L4)
should shrink toward zero local code; L5 should not.

## 3. Serving-time — `data_plane`'s job

```rust
pub trait SummaryExecutor {
    type Handle: Clone; type State; type Value; type Error;
    type GroupKey: Clone + Ord + Default;
    fn find_candidates(&self, summary: &SummaryKind, params: &SummaryParams, col: &ColumnRef,
        reduction: &Reduction, child: &L4Node) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error>;
    fn fetch_state(&self, handle: &Self::Handle) -> Result<Self::State, Self::Error>;
    fn merge_states(&self, states: Vec<Self::State>) -> Result<Self::State, Self::Error>;
    fn readout(&self, state: &Self::State, query: &SketchQuery) -> Result<Self::Value, Self::Error>;
    fn logical(&self, expr: &QueryExpr) -> Result<Self::Value, Self::Error>;
}
pub fn execute<E: SummaryExecutor>(node: &L4Node, exec: &E) -> Result<ExecOutcome<E>, ExecError<E::Error>>;
```

**Target: `data_plane` implements `SummaryExecutor` once, and that
implementation is the sole live serving path.** `execute()` (upstream,
not deployment code) owns every *structural* rule generically — which
nestings are valid, what must agree for a merge to be legal, how deep
recursion composes. The deployment supplies only storage, summary math,
and readout, through the five trait methods:

- `find_candidates` — resolve `(SummaryKind, SummaryParams, group_by)`
  against this deployment's `SketchStore` index, using `reduction`
  (`Reduce(GroupKeys)` vs. `PerEntity`) as the actual, structural signal
  for grouping — **not** inferred from an empty key list (this is where
  the "empty `by` is ambiguous" problem this repo's own
  `data_plane/docs/l4node-plan-executor-design.md` documents gets solved
  for real: `reduction` carries the distinction `L4Node` alone doesn't).
  Family-compatibility inside this method's own matching logic is exactly
  what `sketch_algebra::matcher::SummaryFamilyMatcher` (the `asap_plan::Matcher`
  impl) exists for.
- `fetch_state`/`merge_states` — decode + merge sketch/exact-accumulator
  state from storage; this repo's existing per-family merge logic
  (`AggregateCore::merge_with`, the sketch-family merge helpers) is the
  right home, generalized to run through this one trait method instead of
  as one-off special cases.
- `readout` — evaluate a `SketchQuery` against merged state; this repo's
  existing per-sketch-family readout code is the right home.
- `logical` — evaluate a raw, unrewritten subtree directly (the
  `SummaryExpr::Logical` escape hatch) — this deployment's fallback path
  for anything that reaches serving time without a summary decision.

**Target: `storage_engines/sketch_db/query/sketch_reducer.rs` is retired
once `SummaryExecutor` reaches parity.** Not before — per this repo's own
rollout doc, cutover requires confidence data from a shadow-mode
comparison period (compute both, log discrepancies, serve the legacy
answer) before the legacy path can be deleted; the doc's own
"Rollout design" section already specifies this precisely. This doc
doesn't relitigate that plan — it just confirms the plan's target state
(a single `SummaryExecutor`-driven serving path) is exactly what
ASAPController's own interface is designed to make possible, not a
deployment-specific detour from it.

## 4. What this means for current code — gap against this target

| Layer | Target | Current gap |
|---|---|---|
| L1 | `asap_frontend_promql::lower_promql` called directly; no local parser | `control_plane/src/query_parser/{mod,promql}.rs` (1793 lines) is still fully local — zero `asap-frontend-promql` dependency. **Full gap.** |
| L2 | `Binder::default()` / `convert_root` used via L1, no local schema logic | Already true in substance — `intent_algebra/{binder,column_resolution}.rs` are thin re-export shims. **Effectively closed.** |
| L3 | Zero local `QueryExpr`/`AggIntent`/`Schema` definitions | Already true — `intent_algebra/{agg_intent,query_expr,relational,schema,expr_ir}.rs` are thin re-export shims with only genuinely-local residues (`Frequency` extension helpers, `PerPartitionWrap`, PromQL-ergonomic `LabelFilter`). `intent_algebra/lower.rs` (~1000 lines) remains real local code — deliberately, for two documented reasons with no ASAPController equivalent (multi-agg fusion, the windowed-Count-as-Frequency heuristic). **Effectively closed modulo `lower.rs`'s two documented exceptions.** |
| L4 | One `CostModel` impl; `Rc<L4Node>` used directly | `sketch_algebra::cost_model::ControlPlaneCostModel` + `sketch_algebra::lower::bind_query_expr` (delegating to `implement_tree_in_with`) already match this shape. `sketch_algebra::matcher::SummaryFamilyMatcher` is the `Matcher` impl this section's serving-time §3 depends on. **Effectively closed** — `PhysicalExpr`/`L4Plan` is a thin, acceptable L5-placement wrapper around `Rc<L4Node>`, not a competing L4 algebra. |
| L5 | Full local `PhysicalPlanner`/`TopologyDescriptor`/`StageAllocator` impl | `physical/colored_dag/*` + `emit/*` already implement this shape structurally, just not against the trait names above (no literal `PhysicalPlanner` trait exists in this repo — the free functions/structs are the de facto impl). Low-priority gap: naming/trait-alignment, not missing functionality. |
| Serving | Single `SummaryExecutor` impl is the live path | `data_plane`'s `summary_executor.rs` implements the trait fully, but is not yet the live path — `engine.rs`'s query-serving entry point still calls the legacy flat `SketchReducer`/`capability_for`-based dispatch. `live_serve.rs`/`shadow_compare.rs` exist as the rollout mechanism (both env-flag-gated, off by default). **Rollout in progress, not complete.** |

**Net reading**: L2–L4 are substantially already at target — the earlier
instinct that "`intent_algebra`/`sketch_algebra` should be unnecessary
once connected to ASAPController" is correct and largely *already true*
for L2–L4, not a still-open gap. The two real, still-open items are L1
(adopt `asap-frontend-promql`, retiring `query_parser/` outright) and the
serving-time cutover (finish the `SummaryExecutor` rollout, then retire
`sketch_reducer.rs`). L5 should **not** shrink — it's this deployment's
own, permanent responsibility per ASAPController's own "no `asap-physical`
crate" status.

## 5. Open questions (carried from `data_plane/docs/l4node-plan-executor-design.md`, still unresolved)

These block the serving-time cutover in §3, not the L1 adoption in §4 —
listed here because both docs describe the same target and shouldn't
drift into two different pictures of what's still open:

1. **Grouping ambiguity for empty, sketch-family `by`.** PR #169's
   `l3-intent-algebra.md` interface section resolves this at the type
   level — `Reduction::{Reduce(GroupKeys), PerEntity}` is exactly the
   upstream IR signal this repo's design doc flagged as missing (see
   `Aggregate.reduction`'s doc: "an implementer must branch on `Reduce`
   vs. `PerEntity` there, not guess from an empty key list"). **This
   should now be resolvable** — `find_candidates`'s `reduction` parameter
   already carries the distinction; the open work is wiring
   `data_plane`'s `find_candidates` impl to actually branch on it instead
   of the empty-key heuristic the current draft implementation uses.
2. **Outer-fold family of gaps** (`topk(K, sum by (...) (rate(m[r])))`,
   stacking an outer exact statistic on a sketch/exact-agg readout) —
   still open, still a cross-repo IR design question per the original doc.
3. **Sizing drift** — how often a freshly-planned `SummaryParams` fails to
   match what's actually registered — still only measurable empirically
   once serving-time re-planning runs against real traffic.
