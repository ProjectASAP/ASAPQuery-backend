# Step C: a recursive `L4Node` plan-executor for `ASAPQueryEngine`

Planning doc, not an implementation — same spirit as
[`sketchindex-sid-unification-plan.md`](./sketchindex-sid-unification-plan.md).

> **Status (2026-07-25).** `impl SummaryExecutor for QueryExecutionContext`
> is implemented and tested (#411, unmerged pending review) for
> quantile/cardinality over the DDSketch/Kll/Hll families, both cumulative
> (instant) and per-window (matrix/range) queries. Real cross-sid merging
> is wired in for both modes. Not yet covered: the Frequency family
> (`TopK`/`PointCount`, i.e. CMS/CountSketch — in progress) and `ExactAgg`
> readout (`Sum`/`Rate`/`Increase`/`MinMax`/exact `Count`, which never
> reaches `SummaryExecutor::readout` at all — see below). `execute()`
> isn't wired into `ASAPQueryEngine::execute()`'s live serving path yet;
> the sections below describe the target design, some of which turned out
> to differ from what actually got built — see the inline corrections.

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

Four real gaps this leaves:

1. **No fold.** The loop overwrites instead of combining — fine today because
   real queries only ever produce ≤1 candidate, but there's no existing
   multi-node behavior to be "faithful to" once that's no longer true.
2. **Merge only exists for `ExactAgg`.** `evaluate_exact_agg` folds sids by
   `(group, window)` via `AggregateCore::merge_with` — the one real
   precedent. HLL has a global-only special case. Every other grouped sketch
   case (`quantile by (zone) (...)`, per-group `topk`/`frequency`) silently
   emits duplicate un-merged series today when two sids project to the same
   group — a live bug, not a gap in a working feature.
3. **Sketch merge needs exact param match, and nothing checks that today.**
   `SummaryMerge` requires every child to agree on `(SummaryKind,
   SummaryParams)`, not just family — but `Capability` (what
   `is_satisfied_by` matches on) is family-level only. `AccumulatorSpec`
   (`asap_types::accumulator_spec`, landed) now carries the params
   `data_plane` needs for this check; nothing wires it into merge-candidate
   selection yet.
4. **Two raw-AST fallbacks exist** (`try_topk_over_rate_fallback`,
   `try_rate_over_frequency_fallback`) for compositions the flat `Capability`
   vocabulary can't express. The first is `topk(K, sum by(gbk)(rate(m[r])))`
   — PromQL classifies this as `RankingMeasure::NonAdditive` (only
   `topk(K, count_over_time(...))` maps to the real `AggIntent::TopK`
   sketch), which lowers to `QueryExpr::Sort{Limit{Aggregate}}`.
   `implement_tree_in_with` only recurses through `Aggregate`, so this whole
   shape stays one opaque `Logical` blob today even though the inner
   aggregate would bind fine on its own.

The canonical tree (`asap_sketch::{SummaryExpr, L4Node}`) already exists and
is built control-plane-side (`implement_promql_for_asap_tier`, Step A/B) —
`data_plane` has never consumed it.

## The executor is now a common interface, not a bespoke walk

ASAPController#155 (`crates/sketch/src/exec.rs`) adds `SummaryExecutor` +
`execute()` upstream: a deployment implements the trait, `asap-sketch`
does the recursive walk and enforces the structural rules generically
(which nestings are valid, that `SummaryMerge` children must agree on
`(SummaryKind, SummaryParams)`, propagated correctly through arbitrary
nesting depth). See `docs/l4node-execution-model.md` in ASAPController for
the full design; this doc only covers what's specific to `data_plane`.

`data_plane`'s implementation (`summary_executor.rs`) is
`impl SummaryExecutor for QueryExecutionContext`, a small per-query
context — not `ASAPQueryEngine` directly, since the trait carries no
time-range parameter and `ASAPQueryEngine` is called concurrently;
`QueryExecutionContext` is constructed fresh per query with `t0_ms`/
`t1_ms`/`is_cumulative` as plain fields:

| `SummaryExecutor` method | `data_plane` implementation |
|---|---|
| `Handle` | `SidHandle` — a sid plus its already-fetched `Rc<SketchTimeSeries>` and decode params, not a bare `u64`. `find_candidates` needs to fetch the series anyway (to read label values for the group key), so the handle carries it forward instead of `fetch_state`/`readout` re-fetching the same `(sid, t0, t1)` range a second time. |
| `GroupKey` | `BTreeMap<String, String>` — the sid's own label values projected onto the query's `by` columns. |
| `State` | `GroupState` — the group's accumulated `SidHandle`s plus the shared decode kind. Decode/merge is deliberately lazy: `fetch_state`/`merge_states` just assemble the candidate list; the actual `RollingState` reconstruction and merge happens in `readout`, which is where cumulative-vs-per-window mode is known. |
| `find_candidates(sketch, params, col, by, child)` | walks `child` down to a `Scan{source: Source::TimeSeries{metric}, ..}` to recover the metric, then `instances_matching(metric, by)` filtered to sids whose `(SketchKindHandle, SketchConfig)` is exactly `(sketch, params)` — not the looser family-only `Capability::is_satisfied_by` check the legacy analyzer path uses. |
| `merge_states` | Lazy — see `State` above. |
| `readout` | Real cross-sid merge via `delta_apply::cumulative_rolling_state`/`per_window_rolling_states` + `RollingState::merge_same_family`, covering the DDSketch/Kll/Hll families. CMS/CountSketch (Frequency family) and `ExactAgg` are not covered — see the status note above. |
| `logical` | `Err(SummaryExecutorError::Logical)` — same meaning as "no candidate bound" today, lets `EngineRouter` fail over to archive. |

`AggregateCore::merge_with`/per-family `asap_sketchlib` merge turned out to
be two genuinely separate things, not one shared mechanism: `ExactAgg`
readout never reaches `SummaryExecutor::readout` at all (`asap_plan::bind`
never wraps an `ExactAccumulator` implementation in a `SummaryEstimate`,
so `execute()` on such a tree returns `ExecOutcome::State` at the root —
a caller-side concern, not something this trait implementation handles),
while the sketch-family merge (gap 2) is what's actually implemented here,
via `RollingState::merge_same_family` rather than `asap_sketchlib` calls
made directly in this module.

Because `find_candidates` is now contractually required to return only
exact-`(kind, params)` matches (`asap-sketch`'s trait doc), gap 3 (nothing
checks param agreement today) is resolved by construction for anything
routed through `execute()` — `data_plane` no longer needs its own
merge-precondition check, `asap-sketch`'s does it for `SummaryMerge`
children, and `find_candidates`'s contract does it for a single
`SummaryAgg`'s candidate set.

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
  the two-level case needs dedicated testing.
- **`SummaryAgg`-over-`SummaryEstimate`**: flagged upstream as an open,
  unresolved question (building a new summary from another summary's
  query-time readout). `data_plane`'s `SketchStore` only ever serves
  summaries built at ingest time from raw samples — if `find_candidates`
  ever receives a `child` whose subtree bottoms out in a `SummaryEstimate`
  rather than raw `Logical`/`SummaryAgg`, that's this shape, and the
  right answer today is `Err` (unsupported), not a guess.

## Remaining open questions

1. **Tree source — resolved, and it's both.** `implement_promql_for_asap_tier`
   (Step A) is the seam for planning-time tree construction, as leaned
   toward. But `data_plane` turned out to need a *direct* `asap-sketch`
   (and `asap-ir`) dependency too, pinned to match `control_plane`'s exactly
   — implementing `SummaryExecutor` and matching `L4Node`/`SketchQuery`
   variants requires importing their defining crate directly; consuming a
   function that merely returns those types isn't enough for Rust's trait/
   pattern-matching rules. So this isn't purely "through the seam" as
   originally envisioned — it's the seam for tree construction, plus a
   direct dependency for everything serving-time.
2. **`SummaryMerge` for sketch families — partially resolved.**
   KLL/DDSketch/HLL merge is implemented via `RollingState::merge_same_family`
   (generalized from an existing HLL-only global-cardinality special case,
   not built from scratch against raw `asap_sketchlib` calls). CMS/CountSketch
   still open (Frequency family, in progress as a follow-up). Partial
   coverage — a group missing a sid for one part — is resolved as "fold
   whatever's present," not "miss the whole group" (mirrors
   `SummaryMerge`'s own semantics, ASAPController#159/#161). Accuracy-math
   and resize/downsample questions remain genuinely open, not yet
   investigated.
3. **`topk`/`rate` post-processing**: still open, unrelated to what's been
   built so far — the Frequency-family follow-up doesn't address the
   `try_topk_over_rate_fallback`/`try_rate_over_frequency_fallback` raw-AST
   paths in `engine.rs`.
4. **Rollout**: still open. `execute()` isn't wired into `ASAPQueryEngine`'s
   live serving path yet at all — this question doesn't arise until that
   wiring is attempted.
