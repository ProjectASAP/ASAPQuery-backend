# Step C: a recursive `L4Node` plan-executor for `ASAPQueryEngine`

This is a planning doc, not an implementation — same spirit as
[`sketchindex-sid-unification-plan.md`](./sketchindex-sid-unification-plan.md).
It records what today's query-execution path actually does (verified by
reading the live code, not by memory of what it's supposed to do),
proposes a design, and flags the places where the design isn't settled
yet.

**Status as of Jul 2026:** not started. Step A and Step B (see
`control_plane/docs/design.md` §3's terminology table and
`control_plane/src/sketch_algebra/physical_expr.rs`'s module docs) landed
the canonical L4 IR — `asap_sketch::{SummaryExpr, L4Node}` — and a real
L3→L4 binder (`asap_plan::bind::implement_tree_in_with`,
`control_plane::asap_tier_implement::implement_promql_for_asap_tier`) on
the **control-plane** side only. `data_plane` has never consumed either;
grepping `data_plane/src` for `asap_sketch`, `L4Node`, `SummaryExpr`,
`SummaryAgg`, `SummaryMerge`, `SummaryEstimate` returns zero hits (the
`asap_sketch*`-prefixed names it does use, e.g. `DdSketch`/`KllSketch`,
are the unrelated `asap_sketchlib` sketch-math crate). Wiring the query
path onto the tree is new integration work, not finishing something
half-connected.

## 1. What today's engine actually does

The live entry point is `ASAPQueryEngine::execute()`
(`data_plane/src/query_engines/asap_query_engine/engine.rs:1264-1923`).
There is no tree anywhere in this path — the older `find_compatible_aggregation`/
`handle_query` dispatcher is fully retired (comments only, e.g.
`engine.rs:1904-1912`) and `query_precompute_for_statistic`
(`engine.rs:602-631`) is `#[cfg(test)]`-only, unreachable from production.

```
PromQL string
  → control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(query)
      → query_parser::parse_query_expr_canonical      // PromQL → L3 QueryExpr
      → capability_for(&AggIntent)                    // AggIntent → Capability, per node
  → ASAPTierAnalysis { candidates: Vec<ASAPTierCandidate>, unsupported }   // FLAT LIST
  → for candidate in &analysis.candidates {            // engine.rs:1394
        sids = idx.instances_matching(metric, group_by_keys)
        sids = sids.filter(|sid| candidate.required_capability.is_satisfied_by(sid.capability))
        result = SketchReducer::evaluate_exact_agg / evaluate_for_capability(...)
        combined_result = Some(result)                 // ← overwrites every iteration
    }
  → asap_tier_result_to_query_result(combined_result, ...)
```

`ASAPTierAnalysis`/`ASAPTierCandidate` (`control_plane/src/asap_tier_analysis.rs:67-131`)
are a flat `Vec`, each candidate a leaf-shaped unit: `metric_name`,
`group_by_keys`, `required_capability: Capability`, plus
`outer_fn`/`outer_agg` flags used to encode *some* composition (e.g. "rate
over sum") without any actual nesting.

**This loop doesn't fold multiple candidates — it overwrites.** The
comment at `engine.rs:1367-1372` says so directly: *"Multi-candidate
aggregation is deferred (single-result shapes today)... When more than
one candidate is supported, a follow-up will fold per-candidate
ASAPTierResults."* Today's queries happen to produce ≤1 real candidate,
so this has never been exercised. **This means a real tree-walk executor
is new behavior, not a refactor of an existing fold** — there's no
existing multi-node composition semantics to be faithful to.

### 1a. The typed capability matcher is real, and already control-plane-coupled

`data_plane` is not independent of `control_plane` today — despite the
crate boundary, `data_plane::storage_engines::sketch_db::data::Capability`
is a direct re-export:

```rust
// data_plane/src/storage_engines/sketch_db/data/mod.rs:59
pub use control_plane::sketch_algebra::{Capability, SketchKindHandle};
```

and the matcher call is real and load-bearing (`engine.rs:1480-1487`):

```rust
let satisfied = idx
    .with_instance(*sid, |m| {
        m.capability
            .as_ref()
            .map(|cap| required.is_satisfied_by(cap))
            .unwrap_or(false)
    })
    .unwrap_or(false);
```

`Capability::is_satisfied_by` (`control_plane/src/sketch_algebra/capability.rs:330-379`)
is asymmetric and encodes real cross-family equivalences: a
`MultipleSum`-indexed sid satisfies a required `Sum`; a `Sum`-indexed sid
satisfies a required `Increase`/`Rate` (via `sum_satisfies_increase`,
added this session for PR #395) but never the reverse. **Whatever "sid
catalog lookup" primitive `SummaryAgg` leaves resolve into in the new
design must call this exact function, unchanged** — it would be very easy
to accidentally re-derive a subtly different, wrong matcher while
re-plumbing this into a tree-executor's leaf-resolution step.

### 1b. `StreamingConfig` carries no tree either

The only wire payload `data_plane` consumes from `control_plane` is
`StreamingConfig` (`crates/asap_types/src/streaming_config.rs:58-73`):

```rust
pub struct StreamingConfig {
    pub aggregation_configs: HashMap<u64, AggregationConfig>,  // keyed by PolicyFingerprint
    pub storage_backend: StorageBackend,
    pub monitors: Vec<MonitorSpec>,
}
```

`AggregationConfig` is one flat struct per policy (family, params,
grouping, window, spatial filter) — used at *ingest* time to decide what
to build, and secondarily as a `policy_fp → {sids}` index optimization at
query time (`find_matching_policies`, `engine.rs:716-724`, itself a
fallback because `policy_fp` often comes back `UNSET` for real wire
sketches — issue #271/#272). No readout/estimate/merge structure rides on
the wire at all. **This confirms the design question below is real, not
hypothetical**: today, `data_plane` re-derives everything about "what to
compute" independently, per query, from the PromQL text — it has never
received a plan from `control_plane`.

### 1c. Merge exists for exactly one family today

Two real merge paths, not unified:

- **ExactAgg (general, keyed, works today).** `SketchReducer::evaluate_exact_agg`
  (`sketch_reducer.rs:903-1084`) groups all matching sids' accumulators by
  `(projected_group_by_keys, window_end)`, folds each bucket via
  `AggregateCore::merge_with` (line 1024), reads out via `query_statistic`.
  Comment at `sketch_reducer.rs:1009-1015`: *"multiple entries come from
  multiple sids that share the projected group (e.g. several (zone=z0,
  rack=*) sids collapsing to a single zone=z0 group)."* This is the one
  real, general precedent for what `SummaryMerge` needs to become.
- **HLL (special-cased, global only).** `evaluate_cardinality_global`
  (`sketch_reducer.rs:428-527`) merges HLL registers register-wise across
  every candidate sid, but only fires when `group_by_keys.is_empty()`
  (`engine.rs:1744-1752`) — the comment at `engine.rs:1734-1743` explains
  why: the per-series path would otherwise double-count or never produce
  one global number.

**For every other grouped sketch case — `quantile by (zone) (...)`,
`count by (zone) (unique_metric)`, per-group `topk`/`frequency` — there is
no cross-sid merge at all today.** `evaluate_core`
(`sketch_reducer.rs:534-857`) just pushes each sid's series into the
output with no de-dup on `(series_label_values)` — two sids projecting to
the same group silently produce two output series with identical labels,
not one merged series. **This is a real, currently-shipping bug, and
per-group sketch merge for `SummaryMerge` is new work, not a
generalization of something that already generalizes.**

**Sketch merge is conditional on exact param match, and today's
`Capability` matcher can't see that.** `asap_sketch::SummaryExpr::SummaryMerge`'s
own doc (`crates/sketch/src/expr.rs:87-90`) is explicit: *"all inputs must
agree on `(sketch, params)` and the catalog flag `mergeable` must be
true"* — two KLL sketches only merge cleanly if they were built with the
same `k` (compaction schedule); two CMS matrices only merge if `width`/`depth`
match. `control_plane::sketch_algebra::Capability` (the type
`is_satisfied_by`-matching already runs on, §1a) is **family-level only**
— `Capability::QuantileApprox(SketchKindHandle)` etc. carry a family
selector, never a concrete `SummaryParams` value
(`control_plane/src/sketch_algebra/capability.rs:42-73`). So the existing
matcher answers "does this sid's family satisfy the requirement," not
"do these two sids' params match each other" — a real gap for
`SummaryMerge`'s child-sid selection, which needs the stricter check.
Concretely: if two sids covering the same group were registered with
different `k`/`width`/`depth` (plausible after a re-plan bumps accuracy
targets, or a rolling deploy changes catalog defaults mid-flight),
`instances_matching` + `is_satisfied_by` would happily return both as
individually-satisfying candidates, but folding them via `SummaryMerge`
would be invalid. This needs an explicit param-equality filter *within*
each `SummaryMerge`'s child set (not just per-child `Capability`
satisfaction), plus a decision for what happens to a mismatched minority
(exclude and note partial coverage? `CapabilityMiss` the whole group?
no downsampling/resize path exists in any sketch library used here today,
so "resize to match" is not on the table) — see open question 2.

### 1d. Two structural escape hatches exist today — but the reason is narrower than it first looks

- `try_topk_over_rate_fallback` (`engine.rs:280-537`) hand-parses the raw
  `promql_parser` AST for `topk(K, sum by (gbk) (rate(metric[r])))` (or
  `bottomk`/`irate`), synthesizes an `ExactAgg(Sum)` candidate for the
  *inner* `sum(rate(...))`, evaluates it exactly, then **sorts and slices
  the result to the top/bottom K groups in-engine, after readout**. This
  runs *before* the typed candidate path and bypasses it.
- `try_rate_over_frequency_fallback` (`engine.rs:557-600`) similarly
  special-cases `rate(cms_metric[r])` against a `FrequencyEstimate` sid.

**Correction from an earlier draft of this doc:** `topk(K, expr)` does
*not* categorically fail to decompose into the L4 tree — `AggIntent::TopK`
is explicitly documented as ranking "by the *aggregate output*"
(`crates/ir/src/intent_algebra/agg_intent.rs:79-87`), i.e. it's designed
to compose with an inner aggregate, not just rank raw per-key frequency.
The real story, grounded in `control_plane/src/query_parser/promql.rs:309-345`:

```rust
let measure = if is_count_over_time(agg.expr.as_ref()) {
    RankingMeasure::Frequency
} else {
    RankingMeasure::NonAdditive
};
if is_frequency_heavy_hitter(descending, measure) {
    return Ok(QueryExpr::TopK { k, by, input: Box::new(inner) });  // → AggIntent::TopK
}
// otherwise: Sort { Limit { inner } } — the generic order-by-value path
```

`AggIntent::TopK` only fires for `topk` (never `bottomk`) ranking by
`count_over_time(...)` specifically (`RankingMeasure::Frequency`) — that
shape *does* decompose today, straight into a single bindable
`SummaryAgg{CmsWithHeap/CountSketchWithHeap}` node, no fallback needed.
Every other ranking measure — including `sum by (gbk) (rate(...))`, the
shape the fallback actually handles — is `RankingMeasure::NonAdditive`
and lowers to `QueryExpr::Sort { QueryExpr::Limit { <inner Aggregate> } }`
instead. The inner `Aggregate` (the `sum(rate(...))`) would itself bind
fine on its own (Step B already proved `Aggregate{Sum, child: Aggregate{Rate,...}}`
binds to a nested `SummaryAgg{Sum, child: SummaryAgg{Increase,...}}`) —
the actual blocker is that `implement_tree_in_with` only recurses through
the `Aggregate` spine (its own "conservative fallbacks" doc: a logical
parent above a bindable aggregate subsumes the whole subtree unbound), and
`Sort`/`Limit` aren't `Aggregate` — so `Sort{Limit{Aggregate{...}}}` stops
at the outer `Sort` and the *whole thing* falls to one opaque `Logical`
node, never exposing the inner `Aggregate` to be bound at all.

**This means the fallback isn't necessarily permanent, forced-external
special-casing** — it's the same shape of gap Step B's `lower.rs`
pre-passes already close for other cases (Count{Exact}, Rate). A
control-plane-side pre-pass recognizing `Sort{Limit{Aggregate{...}}}`
could strip the `Sort`/`Limit` wrapper, recurse `implement_tree_in_with`
into the inner `Aggregate` to get a real `SummaryAgg`/`SummaryEstimate`
node, and carry the stripped rank/limit info *alongside* the tree (not as
a new `SummaryExpr` variant — ranking-and-slicing a readout is an
execution-time/L5 concern, not an L4 "what sketch to build" concern) for
`data_plane`'s executor to apply after evaluating that subtree. Whether
that pre-pass is worth building now or the raw-AST fallback should stay
as-is a while longer is an open call — see open question 3 — but the
reasoning for *why* is now precise instead of "these are fundamentally
incompatible," which was wrong.

### 1e. Other things any executor rewrite must reproduce exactly

- **Keyed-CMS safe-miss gate** (`engine.rs:1549-1626`): a
  `FrequencyEstimate` sid answers only bucket totals; a keyed selector
  like `cms_metric{item="X"}` must `CapabilityMiss` to archive *unless*
  the sid is registered in `item_label` mode and the filter resolves to
  that label. Skipping this returns a *wrong answer*, not just a miss —
  correctness-critical, must be preserved verbatim.
- **Coverage-aware archive stitching** (`stitch_warm_and_archive`,
  `engine.rs:1136-1190`): compares `ASAPTierResult.coverage` against the
  requested window and merges with an archive-engine answer by
  `(labels, timestamp)` when the warm tier only partially covers it. This
  is a cross-cutting concern layered *around* whatever answers the query,
  not encodable in the L4 IR — it belongs at the tree root only, once,
  not per node (an N-way per-node stitch is not what this needs to
  become).
- **`instances_matching` + capability-check is duplicated at ~4 call
  sites** in `engine.rs` today rather than being one function — worth
  consolidating into the single reusable primitive `SummaryAgg` leaves
  would call, rather than duplicating it a 5th time.
- `EngineRouter`/`BackendStorageRouting`
  (`data_plane/src/query_engines/routing/`) sit one layer above all of
  this and are orthogonal — they decide *which whole engine* (warm sketch
  tier vs. archive tier) answers a query, never *which node inside* it.
  The only contract the new executor must keep is `execute()`'s existing
  `Err(EngineError::CapabilityMiss(...))` signaling (used at ~7 sites
  today, each firing `spawn_capability_miss_notify`), so archive failover
  keeps working unchanged.

## 2. Proposed design

### 2a. Where the tree comes from

`data_plane` builds its own `Rc<L4Node>` tree at query time, mirroring
Step A's `implement_promql_for_asap_tier` rather than waiting for
`control_plane` to ship one over the wire. Rationale: `data_plane`
already independently re-parses PromQL and independently derives
`AggIntent`s today (§1); it is *already* the pattern in this codebase
that the query-time IR is re-derived locally rather than carried on
`StreamingConfig` (which is, and stays, ingest-time-only — see §1b). Two
concrete integration options, need a decision before implementation:

1. `data_plane` takes a direct dependency on `asap_plan`/`asap_sketch`
   (already a `control_plane` git dependency; would become a `data_plane`
   one too) and calls `asap_plan::bind::implement_tree_in_with` itself,
   passing its own `CostModel` (mirroring
   `control_plane::sketch_algebra::cost_model::ControlPlaneCostModel`, or
   literally reusing it if `data_plane` is willing to depend on
   `control_plane` for it the way it already does for `Capability`).
2. `data_plane` calls into a new `control_plane`-side function (e.g.
   exposing `implement_promql_for_asap_tier` or an equivalent) rather
   than depending on `asap_plan` directly, keeping `asap_plan`/`asap_sketch`
   coupling confined to `control_plane`, consistent with `data_plane`'s
   existing pattern of depending on `control_plane::sketch_algebra`
   types (`Capability`) rather than reaching past it to `asap_sketch`
   itself.

Leaning toward (2) — it matches the existing `Capability` re-export
pattern (§1a) and keeps `asap_plan`/`asap_sketch` as a `control_plane`-only
upstream coupling, one seam instead of two — but this needs to be decided
before writing code, not discovered mid-implementation.

### 2b. The recursive walk

```rust
fn execute_node(node: &L4Node, window: TimeRange, ctx: &ExecCtx)
    -> Result<ExecResult, EngineError>
{
    match &node.expr {
        SummaryExpr::SummaryAgg { sketch, params, col, by, .. } => {
            // LEAF: one consolidated sid-catalog-lookup primitive,
            // replacing the ~4 duplicated call sites in engine.rs today.
            let required = capability_from_summary_kind(sketch, params);
            let sids = ctx.index
                .instances_matching(&ctx.metric, by)
                .filter(|sid| ctx.index.with_instance(*sid, |m|
                    m.capability.as_ref().is_some_and(|cap| required.is_satisfied_by(cap))
                ).unwrap_or(false));
            ExecResult::SketchState { sids: sids.collect(), by: by.clone() }
        }
        SummaryExpr::SummaryEstimate { sketch_input, query } => {
            let state = execute_node(sketch_input, window, ctx)?;   // recurse
            readout(state, query)   // quantile / cardinality / point_count / topk
        }
        SummaryExpr::SummaryMerge { children } => {
            let states: Vec<_> = children.iter()
                .map(|c| execute_node(c, window, ctx))
                .collect::<Result<_, _>>()?;
            // generalizes evaluate_exact_agg's (group, window) fold (§1c)
            // to every SummaryKind, not just ExactAgg — but unlike ExactAgg
            // accumulators, sketch families require exact (kind, params)
            // agreement to merge at all (§1c's mergeable-params landmine);
            // this must filter/partition each group's candidate sids on
            // params equality *before* folding, not just Capability
            // satisfaction.
            merge_by_group(states)
        }
        SummaryExpr::Logical(qe) => {
            // No summary committed for this subtree — same meaning as
            // today's "no candidate bound": CapabilityMiss, let
            // EngineRouter fail over to archive.
            Err(EngineError::CapabilityMiss(/* ... */))
        }
        SummaryExpr::SummaryJoin { .. }
        | SummaryExpr::SummarySubtract { .. }
        | SummaryExpr::SummaryDelete { .. } => {
            // No Bind* path produces these yet (see physical_expr.rs's
            // module docs) — unreachable in practice today.
            unimplemented!("not yet surfaced by any binder")
        }
    }
}
```

`execute()` calls `execute_node` once at the tree root, then applies the
coverage-aware archive stitch (§1e) once on the final result — not
per-node.

### 2c. What's genuinely new work vs. what's porting

| Piece | Status |
|---|---|
| `SummaryAgg` leaf → sid lookup + capability check | Porting — consolidates 4 duplicated call sites, must call `is_satisfied_by` unchanged (§1a) |
| `SummaryEstimate` → readout | Porting — `evaluate_core`'s per-family decode blocks (quantile/cardinality/frequency/topk) already exist, just need re-homing under the new dispatch |
| `SummaryMerge` for `ExactAgg`-family kinds | Porting — `evaluate_exact_agg`'s fold-by-`(group,window)` already does this |
| `SummaryMerge` for sketch families (KLL/DDSketch/CMS/CountSketch by group) | **New** — doesn't exist today outside the global-only HLL special case (§1c), and needs a param-equality filter `Capability` matching alone doesn't provide (§1c) |
| Keyed-CMS safe-miss gate | Porting — must be preserved verbatim inside the `SummaryEstimate`/`FrequencyEstimate` readout path |
| Coverage-aware archive stitch | Porting — moves from per-query to tree-root-only, same logic |
| `topk(K, sum(...))` (`RankingMeasure::NonAdditive`) / `rate(cms(...))` post-processing | Not tree-shaped under today's `implement_tree_in_with` (stops at the outer `Sort`/`Limit`), but potentially *becomes* tree-shaped down to a thin rank/limit wrapper with a control-plane-side pre-pass — see the corrected §1d and open question 3 |

## 3. Open questions (need a decision before/while implementing)

1. **Tree-source integration (§2a)**: does `data_plane` depend on
   `asap_plan`/`asap_sketch` directly, or call through a `control_plane`
   seam? Leaning (2) above but unconfirmed.
2. **`SummaryMerge` semantics for sketch families**: `evaluate_exact_agg`'s
   `(group, window)` bucketing is the closest precedent, but it needs a
   param-equality filter on top that `Capability` alone can't provide
   (§1c) — `control_plane::sketch_algebra::Capability` is family-level
   only (`SketchKindHandle`, no `SummaryParams`), so `SummaryMerge`'s
   child-sid selection needs a *stricter* check than
   `instances_matching` + `is_satisfied_by` already gives the plain
   `SummaryAgg` leaf case: exact `(SummaryKind, SummaryParams)` agreement
   across every sid folded into one merge. Open sub-questions: what
   happens to a sid that matches on `Capability` but not on params (drop
   it and report partial coverage? `CapabilityMiss` the whole group? no
   resize/downsample path exists in any sketch library used here today);
   does merging change the `SummaryEstimate`'s accuracy math (a merged
   multi-sid sketch can have different error bounds than a single-sid
   one — is that tracked anywhere, or a new gap); and does the same
   `(group, window)` bucketing generalize cleanly to every sketch family
   `data_plane` needs to merge here, or do some (e.g. CMS-with-heap, where
   merging matrices is well-defined but merging *heaps* isn't the same
   operation) need family-specific merge logic beyond a generic fold?
3. **Where does "rank-and-slice a readout" live?** Corrected in §1d:
   `topk`/`bottomk` ranking by a non-`count_over_time` measure (the shape
   `try_topk_over_rate_fallback` handles) lowers to `QueryExpr::Sort {
   QueryExpr::Limit { <Aggregate> } }`, which `implement_tree_in_with`
   doesn't decompose today (stops at the outer `Sort`, whole subtree
   stays `Logical`) — but the inner `Aggregate` would bind fine on its
   own if reached. Options: (a) leave the raw-AST fallback exactly as-is,
   `data_plane`-side, unchanged; (b) add a `control_plane`-side pre-pass
   (same shape as Step B's `lower.rs` pre-passes) that strips `Sort`/
   `Limit`, binds the inner `Aggregate` for real, and carries the
   rank/limit as metadata *alongside* the resulting `L4Node` (not a new
   `SummaryExpr` variant — this is an L5/execution-time concern, not an
   L4 "what to build" concern) for `data_plane`'s executor to apply after
   evaluating that subtree; (c) something upstream in `asap_plan`
   (unlikely — this is deployment-specific PromQL-surface shape
   recognition, the same category of thing Step B's `lower.rs`
   pre-passes handle locally rather than pushing upstream). (b) is more
   work than (a) but turns a permanent raw-AST special case into a real,
   general "any `Sort{Limit{...}}}`-wrapped bindable aggregate" capability
   — worth deciding deliberately rather than defaulting to (a) just
   because it's less work right now.
4. **Rollout**: land behind a parallel path (mirroring
   `USE_TYPED_STAGE_SPLIT`/`USE_TYPED_SKETCH_ALGEBRA`'s env-var-gated
   parallel-path pattern from Step A/B) so the new executor can be
   compared against the existing flat dispatcher before it becomes the
   only path, or replace `execute()`'s loop directly once confident?
   Given §1c/§1d's gaps are real, currently-shipping-behavior questions
   (not just refactor risk), a parallel/comparable rollout seems safer
   than a hard cutover, but this changes the shape of the work
   noticeably (need to keep both paths correct simultaneously for a
   while) and should be confirmed before starting.
