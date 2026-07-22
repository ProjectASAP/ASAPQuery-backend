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

## Proposed executor

```rust
fn execute_node(node: &L4Node, window: TimeRange, ctx: &ExecCtx) -> Result<ExecResult, EngineError> {
    match &node.expr {
        SummaryAgg { sketch, params, by, .. } => {
            // one consolidated sid lookup, replacing ~4 duplicated call sites
            let sids = ctx.index.instances_matching(&ctx.metric, by)
                .filter(|s| required_capability(sketch, params).is_satisfied_by(s.capability));
            ExecResult::SketchState { sids, by: by.clone() }
        }
        SummaryEstimate { sketch_input, query } => {
            readout(execute_node(sketch_input, window, ctx)?, query)
        }
        SummaryMerge { children } => {
            let states = children.iter().map(|c| execute_node(c, window, ctx)).collect::<Result<Vec<_>,_>>()?;
            merge_by_group(states)   // must filter to exact (kind, params) match per group first
        }
        Logical(_) => Err(EngineError::CapabilityMiss(..)),   // same meaning as "no candidate bound" today
        SummaryJoin | SummarySubtract | SummaryDelete => unimplemented!(),  // no Bind* produces these yet
    }
}
```

`execute()` calls this once at the root, then applies the existing
coverage-aware archive stitch once on the result (not per-node).

| Piece | New or porting |
|---|---|
| `SummaryAgg` → sid lookup | Porting (consolidation) |
| `SummaryEstimate` → readout | Porting (`evaluate_core`'s decode blocks already exist) |
| `SummaryMerge` for `ExactAgg` | Porting (`evaluate_exact_agg`'s fold) |
| `SummaryMerge` for sketch families | **New** — plus the param-match filter (gap 3) |
| `topk`-over-non-frequency-measure | Not tree-shaped today; a control-plane pre-pass could make the inner aggregate bindable (see open questions) |

## Open questions

1. **Tree source**: does `data_plane` call `asap_plan::bind::implement_tree_in_with`
   directly, or through a `control_plane`-side seam (matching how it already
   depends on `control_plane::sketch_algebra::Capability` rather than
   `asap_sketch` directly)? Leaning toward the seam.
2. **`SummaryMerge` param-match**: use `AccumulatorSpec` to filter each
   group's candidate sids to exact `(kind, params)` agreement before
   folding. What happens to a sid that satisfies `Capability` but not
   params — drop it (partial coverage) or miss the whole group? No
   resize/downsample path exists for any sketch family here.
3. **`topk`/`rate` post-processing**: keep the raw-AST fallbacks as-is, or
   add a `control_plane`-side pre-pass that strips `Sort{Limit{...}}`,
   binds the inner aggregate for real, and carries rank/limit as metadata
   alongside the `L4Node` for the executor to apply after evaluation?
4. **Rollout**: parallel path behind an env-var gate (matching
   `USE_TYPED_STAGE_SPLIT`), or replace `execute()`'s loop directly?
