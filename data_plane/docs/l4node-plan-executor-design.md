# Step C: a recursive `L4Node` plan-executor for `ASAPQueryEngine`

Planning doc, not an implementation — same spirit as
[`sketchindex-sid-unification-plan.md`](./sketchindex-sid-unification-plan.md).

> **Status (2026-07-27).** `impl SummaryExecutor for QueryExecutionContext`
> (`summary_executor.rs`) now covers quantile/cardinality (DDSketch/Kll/Hll),
> the Frequency family (CMS/CountSketch/CmsWithHeap/CountSketchWithHeap —
> bare total, per-item point lookup, top-k), both cumulative (instant) and
> per-window (matrix/range) queries, real cross-sid merging for all of the
> above, per-group coverage tracking (`SummaryValue::coverage`), and
> `AggKind::ExactAgg` candidate matching for `Sum`/`Increase` (#411, #412,
> #414, #415, #417 — all merged). `ExactAgg` readout still never reaches
> `SummaryExecutor::readout`/`SketchQuery` at all (unchanged from the
> original design below) — `GroupState::exact_value` is the accessor a
> caller uses instead. `MinMax`/`Count`/`Rate` are deliberately NOT matched
> for `ExactAgg` (no direction info survives to `AggKind::ExactAgg`'s
> metadata for `MinMax`; no `AggregationType` resolves to `Count`/`Rate`
> today) — see `summary_executor.rs`'s own module doc and
> `exact_agg_kind_match`.
>
> `execute()` is **still not wired into `ASAPQueryEngine::execute()`'s live
> serving path** — see "Rollout" below, which now has a concrete plan
> (shadow mode) rather than being an open question. The sections below
> describe the target design; some turned out to differ from what actually
> got built — see the inline corrections.

## Architecture reference: planning vs. serving-time execution

This doc is the `data_plane`-specific instance of what ASAPController's own
design doc calls out as a first-class split — see
[ASAPController `docs/design.md` § "Serving-time execution"](https://github.com/ProjectASAP/ASAPController/blob/097f440079f851a560cc6927f50eb7f56a49e6c/docs/design.md#serving-time-execution):

> Everything above (L1-L5) is the **planning** pipeline: turning a query
> string into a plan. This section is different in kind — it's what
> actually **answers** a query at request time, using whatever plan L1-L5
> already decided... From a code standpoint, planning and serving are two
> separate interfaces a downstream deployment implements against:
> `asap_plan` (planning-time binder) and `SummaryExecutor` (serving-time
> executor).

Concretely, for this deployment:

- **Planning** (`L3 QueryExpr → L4 L4Node`) is `control_plane`'s job —
  `asap_plan::bind`/`implement_tree_in_with`, plugged with a `CostModel`.
  `control_plane` runs **in-process** with `data_plane` in this deployment
  (the "Phase 9" comment in `data_plane/Cargo.toml`), so this is a
  same-binary library call, not a network hop or a second planning
  implementation living in `data_plane`.
- **Serving** (`L4Node → Value`, "walk the already-decided tree against
  whatever is actually materialized right now") is `data_plane`'s job —
  `impl SummaryExecutor for QueryExecutionContext` (`summary_executor.rs`),
  covered by the rest of this doc.

The one thing this deployment's topology adds that the upstream doc doesn't
need to say: because both halves run in the same process, `data_plane` CAN
call `control_plane`'s planning function directly at request time (see
"Rollout" below) rather than needing a wire protocol to receive an
already-built plan from a separately-deployed control plane. This is a
deployment-specific convenience, not something `asap-plan`/`asap-sketch`
assume.

## Today

`ASAPQueryEngine::execute()` (`asap_query_engine/engine.rs:1264-1923`) has no
tree. It gets a flat `Vec<ASAPTierCandidate>` from
`analyze_promql_for_asap_tier` and loops:

```
for candidate in candidates {
    sids = instances_matching(metric, group_by).filter(|sid| required.is_satisfied_by(sid.capability));
    result = evaluate_exact_agg / evaluate_for_capability(sids, ...);
    combined_result = Some(result);   // ← overwrites, never folds
}
```

Four real gaps this left (as of the original version of this doc):

1. **No fold.** The loop overwrites instead of combining — fine today because
   real queries only ever produce ≤1 candidate, but there's no existing
   multi-node behavior to be "faithful to" once that's no longer true.
   Still true of the legacy `SketchReducer` path — unaffected by anything
   below, since none of it has been wired into `engine.rs` yet.
2. **Merge only existed for `ExactAgg`** on the legacy path. ~~Every other
   grouped sketch case silently emits duplicate un-merged series today.~~
   **Resolved on the new path**: `summary_executor.rs`'s `readout_cumulative`/
   `readout_per_window` do real cross-sid merging generically for every
   sketch family via `SummaryState::merge_same_family`, not an `ExactAgg`-only
   special case. Still an open bug on the legacy `SketchReducer` path itself,
   which this doc's plan doesn't touch.
3. **Sketch merge needs exact param match, and nothing checked that on the
   legacy path.** ~~`AccumulatorSpec` now carries the params `data_plane`
   needs for this check; nothing wires it into merge-candidate selection
   yet.~~ **Resolved on the new path** — see "resolved by construction"
   below.
4. **Two raw-AST fallbacks exist** (`try_topk_over_rate_fallback`,
   `try_rate_over_frequency_fallback`) for compositions the flat `Capability`
   vocabulary can't express. Still true, still unaddressed by anything on
   this doc's plan — see "Remaining open questions" #3.

The canonical tree (`asap_sketch::{SummaryExpr, L4Node}`) already exists and
can be built control-plane-side — see "Remaining open questions" #1 below
for exactly which function does this correctly (it turned out to matter
which one).

## The executor is now a common interface, not a bespoke walk

ASAPController#155 (`crates/sketch/src/exec.rs`) adds `SummaryExecutor` +
`execute()` upstream: a deployment implements the trait, `asap-sketch`
does the recursive walk and enforces the structural rules generically
(which nestings are valid, that `SummaryMerge` children must agree on
`(SummaryKind, SummaryParams)`, propagated correctly through arbitrary
nesting depth). See `docs/l4-summary-bound-ir.md` in ASAPController for
the full design; this doc only covers what's specific to `data_plane`.

`data_plane`'s implementation (`summary_executor.rs`) is
`impl SummaryExecutor for QueryExecutionContext`, a small per-query
context — not `ASAPQueryEngine` directly, since the trait carries no
time-range parameter and `ASAPQueryEngine` is called concurrently;
`QueryExecutionContext` is constructed fresh per query with `t0_ms`/
`t1_ms`/`is_cumulative` as plain fields:

| `SummaryExecutor` method | `data_plane` implementation |
|---|---|
| `Handle` | `SidHandle` — an **enum**: `Sketch { series: Rc<SketchTimeSeries>, kind: DeltaSketchKind }` or `ExactAgg { windows: Rc<BTreeMap<i64, Arc<dyn AggregateCore>>>, agg_type: AggregationType }`. `find_candidates` needs to fetch the series/windows anyway (to read label values for the group key), so the handle carries it forward instead of `fetch_state`/`readout` re-fetching the same `(sid, t0, t1)` range a second time. |
| `GroupKey` | `BTreeMap<String, String>` — the sid's own label values projected onto the query's `by` columns. |
| `State` | `GroupState` — also an enum, mirroring `SidHandle`'s two variants. Decode/merge is deliberately lazy for the `Sketch` variant: `fetch_state`/`merge_states` just assemble the candidate list; the actual `SummaryState` reconstruction and merge happens in `readout`, which is where cumulative-vs-per-window mode is known. `ExactAgg`'s value extraction (`GroupState::exact_value`) is eager-ish but still only runs when a caller asks. |
| `find_candidates(sketch, params, col, by, child)` | walks `child` down to a `Scan{source: Source::TimeSeries{metric}, ..}` to recover the metric, then `instances_matching(metric, by)` filtered to sids whose `AggKind` is either `Sketch{kind, config, ..}` with `(SketchKindHandle, SketchConfig)` exactly `(sketch, params)`, or `ExactAgg{agg_type, ..}` matching `exact_agg_kind_match(sketch, params, agg_type)` — not the looser family-only `Capability::is_satisfied_by` check the legacy analyzer path uses. |
| `merge_states` | Lazy for `Sketch` — see `State` above. Errors (`UnsupportedFamily`) if a group somehow mixes `Sketch` and `ExactAgg` entries (shouldn't happen; `find_candidates`'s per-`(kind,params)` exactness makes this defensive, not a real path). |
| `readout` | Real cross-sid merge via `delta_apply::cumulative_summary_state`/`per_window_summary_states` + `SummaryState::merge_same_family`, covering DDSketch/Kll/Hll/Cms/CmsWithHeap/CountSketch/CountSketchWithHeap. Returns `SummaryExecutorError::UnsupportedFamily` for an `ExactAgg` state (defensive — see below, `readout` is never actually called for one in practice). |
| `logical` | `Err(SummaryExecutorError::Logical)` — same meaning as "no candidate bound" today, lets `EngineRouter` fail over to archive. |

`AggregateCore::merge_with`/per-family `asap_sketchlib` merge turned out to
be two genuinely separate things, not one shared mechanism: `ExactAgg`
readout never reaches `SummaryExecutor::readout` at all (`asap_plan::bind`
never wraps an `ExactAccumulator` implementation in a `SummaryEstimate`,
so `execute()` on such a tree returns `ExecOutcome::State` at the root —
a caller-side concern, not something this trait implementation handles),
while the sketch-family merge is what's actually implemented in `readout`,
via `SummaryState::merge_same_family` rather than `asap_sketchlib` calls
made directly in this module. `GroupState::exact_value(&self, key)` is the
`ExactAgg` analog of `readout_cumulative` — folds every window/sid in the
group via `AggregateCore::merge_with`, then reads out the statistic
`agg_type` implies — called directly by whatever future caller reads an
`ExecOutcome::State`'s contents (see "Rollout" below for the first such
caller).

Because `find_candidates` is now contractually required to return only
exact-`(kind, params)` matches (`asap-sketch`'s trait doc), gap 3 above
(nothing checked param agreement on the legacy path) is resolved by
construction for anything routed through `execute()` — `data_plane` no
longer needs its own merge-precondition check, `asap-sketch`'s does it for
`SummaryMerge` children, and `find_candidates`'s contract does it for a
single `SummaryAgg`'s candidate set.

## Nested queries in this deployment's topology

`execute()`'s recursion handles arbitrary nesting depth already (see the
ASAPController doc) — the deployment-specific question is what nesting
actually *occurs* in a three-stage (edge/gateway/backend) topology:

- **`SummaryAgg`-of-`SummaryAgg`** (`quantile(0.9, sum by (job) (m))`):
  edge builds per-job sums; backend's KLL is built over that sum stream.
  Already a valid, tested shape upstream — no new work here beyond
  `find_candidates` correctly walking past the inner `SummaryAgg` to find
  the metric.
- **`SummaryMerge`-of-`SummaryMerge`**: a plausible real shape once
  merging isn't `ExactAgg`-only — e.g. per-zone edge sketches merge at a
  regional gateway, regional merges merge again at the backend. `execute()`
  already handles this (the `(kind, params)` check is transitive), so this
  is a rollout/topology question, not a design gap: does this deployment's
  stage allocator ever actually *produce* a two-level merge cascade today,
  or does everything currently collapse to one gateway hop? Worth checking
  against `physical/colored_dag`'s stage-allocation output before assuming
  the two-level case needs dedicated testing. Still unresolved — no new
  information this round.
- **`SummaryAgg`-over-`SummaryEstimate`**: flagged upstream as an open,
  unresolved question (building a new summary from another summary's
  query-time readout). `data_plane`'s `SketchStore` only ever serves
  summaries built at ingest time from raw samples — if `find_candidates`
  ever receives a `child` whose subtree bottoms out in a `SummaryEstimate`
  rather than raw `Logical`/`SummaryAgg`, that's this shape, and the
  right answer today is `Err` (unsupported), not a guess. Still unresolved.

## Rollout

`execute()` isn't wired into `ASAPQueryEngine`'s live serving path yet at
all. This round's plan (see the tracked plan file / PR for the actual
implementation) is a **shadow-mode** first phase, not a cutover:

1. **Tree source, corrected.** The previous version of this doc said
   `implement_promql_for_asap_tier` (`control_plane::asap_tier_implement`,
   Step A) "is the seam for planning-time tree construction." That's
   *incomplete* — that function uses the naive `asap_plan::DefaultCostModel`
   (no real accuracy-driven parameter sizing) and has its own documented,
   tracked gap: it cannot realize `AggIntent::Extension`/`Frequency`
   (CMS/CountSketch) at all — falls back to `SummaryExpr::Logical` for the
   entire Frequency family (see that module's own doc, "Known gap:
   Extension/Frequency under-realizes"). The correct seam is
   `control_plane::sketch_algebra::lower::bind_query_expr` — the function
   `main.rs`'s real production planning pipeline calls, using
   `ControlPlaneCostModel` (real accuracy-bound sizing, and, via
   `boundary::implementation_for_with` + `realize_extension`/
   `readout_extension`, ASAPController#150, correct Frequency realization
   too). `ControlPlaneCostModel::new(accuracy)` takes only an
   `AccuracyTarget` — no live server/catalog state — so it's constructible
   standalone from `data_plane`, matching the "same-binary library call"
   framing in "Architecture reference" above. `bind_query_expr` always
   returns `PhysicalExpr::Committed(L4Plan::Summary(Rc<L4Node>))` per its
   own doc (never picks a Phase ε.1 edge/backend placement), so extracting
   the tree is a simple pattern match.
2. **Sizing drift is expected, not a bug to fix first.** A freshly-computed
   `SummaryParams` (width/depth/k/precision) from step 1 is NOT guaranteed
   to exactly match what's actually registered in `SketchStore` right now
   — `find_candidates`'s contract is an exact match, so a mismatch just
   means `NoCandidates`/an empty group, not a wrong answer. This is exactly
   what shadow mode is for: surface how often/how badly this happens before
   ever trying to close the gap.
3. **`rate()`/topk-over-rate/outer-agg-fold are excluded from the shadow
   comparison entirely**, not just deprioritized. `lower.rs`'s
   `bind_recursive` rewrites `AggIntent::Rate → Increase` before binding
   (so a `rate()` query DOES bind to a valid `SummaryAgg{Increase}` tree),
   but `summary_executor.rs` has no rate-division logic (dividing by a
   coverage-clamped range is `sketch_reducer.rs::evaluate_exact_agg_rate`/
   `evaluate_frequency_rate`'s job) — so the new path would produce a
   semantically wrong (un-divided) answer for `rate()` if compared naively.
   Detected and skipped before ever calling into `control_plane` for this
   phase, using the same raw-AST inspection `engine.rs`'s existing
   fallbacks already do.
4. **Shadow, not cutover.** Compute the new answer alongside the old
   (`SketchReducer`), diff, log discrepancies, always return the old
   answer — see `docs/design-sketch-db-roadmap.md` § 13.2 "Shadow mode"
   for the pattern this follows (already documented there, unimplemented
   until now). `ASAP_LEGACY_DUAL_WRITE`
   (`data_plane/src/drivers/ingest/otel.rs:891-895`) is the closest
   actually-shipped env-var mechanics to mirror for the flag itself;
   `control_plane`'s `USE_TYPED_STAGE_SPLIT` is a single-path selector, not
   a shadow/diff pattern, so it's the wrong template despite being more
   prominent in this codebase.

Actually switching what's served, and retiring `sketch_reducer.rs`, both
require confidence data this phase doesn't yet produce, plus a resolution
for the rate/outer-fold gap (which needs its own cross-repo design
conversation with ASAPController, not a unilateral local decision) —
neither is in scope for the shadow-mode phase.

## Remaining open questions

1. **Tree source — see "Rollout" above**, now resolved with a correction
   to the original answer (`bind_query_expr`, not
   `implement_promql_for_asap_tier`). `data_plane` still needs a *direct*
   `asap-sketch` (and `asap-ir`) dependency, pinned to match `control_plane`'s
   exactly — implementing `SummaryExecutor` and matching `L4Node`/
   `SketchQuery` variants requires importing their defining crate directly;
   consuming a function that merely returns those types isn't enough for
   Rust's trait/pattern-matching rules.
2. **`SummaryMerge` for sketch families — resolved.** KLL/DDSketch/HLL/
   CMS/CountSketch (bare and heap-bearing) merge are all implemented via
   `SummaryState::merge_same_family`. Partial coverage (a group missing a
   sid for one part) is resolved as "fold whatever's present," not "miss
   the whole group" (mirrors `SummaryMerge`'s own semantics,
   ASAPController#159/#161). Accuracy-math and resize/downsample questions
   remain genuinely open, not yet investigated.
3. **`topk`/`rate` post-processing**: still open. `try_topk_over_rate_fallback`/
   `try_rate_over_frequency_fallback` in `engine.rs` remain
   `SketchReducer`-only; the shadow-mode rollout explicitly excludes these
   shapes rather than attempting them (see "Rollout" #3) pending the
   cross-repo design conversation on outer-agg-fold.
4. **Rollout — no longer just an open question**, see "Rollout" above for
   the concrete shadow-mode plan.
