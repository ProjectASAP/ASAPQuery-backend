# Step C: a recursive `L4Node` plan-executor for `ASAPQueryEngine`

Planning doc, not an implementation — same spirit as
[`sketchindex-sid-unification-plan.md`](./sketchindex-sid-unification-plan.md).
Status: not started.

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

`data_plane`'s job is one `impl SummaryExecutor for ASAPQueryEngine` (or a
small wrapper around it):

| `SummaryExecutor` method | `data_plane` implementation |
|---|---|
| `Handle` | `u64` (sid) |
| `State` | decoded sketch bytes / `Box<dyn AggregateCore>`, per family |
| `find_candidates(sketch, params, col, by, child)` | walk `child` to recover the metric (same shape as today's `extract_edge_facts`), then `instances_matching(metric, by)` filtered to sids whose `AccumulatorSpec` is exactly `(sketch, params)` — **not** the looser family-only `Capability::is_satisfied_by` check `data_plane` uses today; `AccumulatorSpec` (landed) is what makes the exact check possible |
| `merge_states` | `AggregateCore::merge_with` for `ExactAgg` kinds; per-family `asap_sketchlib` merge for sketch kinds (KLL/DDSketch/CMS/HLL) — the part that's genuinely new (gap 2) |
| `readout` | `evaluate_core`'s existing per-family decode blocks, re-homed |
| `logical` | `Err(CapabilityMiss)` — same meaning as "no candidate bound" today, lets `EngineRouter` fail over to archive |

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

1. **Tree source**: does `data_plane` call `asap_plan::bind::implement_tree_in_with`
   directly, or through a `control_plane`-side seam (matching how it already
   depends on `control_plane::sketch_algebra::Capability` rather than
   `asap_sketch` directly)? Leaning toward the seam.
2. **`SummaryMerge` for sketch families**: per-family merge (KLL/DDSketch/
   CMS/HLL) via `asap_sketchlib` is genuinely new work, not a port. Does
   merging change a `SummaryEstimate`'s accuracy math (a merged multi-sid
   sketch can have different error bounds than a single-sid one)? What
   happens when `find_candidates` can't find *any* sid with exactly
   matching params for part of a group — drop it (partial coverage) or
   miss the whole group? No resize/downsample path exists for any sketch
   family here.
3. **`topk`/`rate` post-processing**: keep the raw-AST fallbacks as-is, or
   add a `control_plane`-side pre-pass that strips `Sort{Limit{...}}`,
   binds the inner aggregate for real, and carries rank/limit as metadata
   alongside the `L4Node` for the executor to apply after evaluation?
4. **Rollout**: parallel path behind an env-var gate (matching
   `USE_TYPED_STAGE_SPLIT`), or replace `execute()`'s loop directly?
