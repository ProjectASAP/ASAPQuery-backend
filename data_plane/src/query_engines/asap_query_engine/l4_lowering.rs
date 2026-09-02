//! PromQL string → `planner_types::post_asap::SummaryNode` bridge, shared by the actual
//! serving cutover (`live_serve.rs`, via `l4_readout.rs`). See
//! `data_plane/docs/l4node-plan-executor-design.md`'s "Rollout" section
//! for the general design.
//!
//! `control_plane` runs in-process with `data_plane` in this deployment
//! (see `data_plane/Cargo.toml`'s "Phase 9" comment), so this is a
//! same-binary library call, not a new planning implementation living
//! here.
//!
//! ## Serving time must not re-plan
//!
//! `parse_query_expr_canonical` (L1→L2→L3) is safe to re-run at serving
//! time — it's a pure, deterministic canonicalization of the query text,
//! not a decision. Binding L3→L4 (which sketch family, what parameters)
//! is a genuine PLANNING decision, and planning already made it once, for
//! real, when this metric's workload was planned — that decision is what
//! `data_plane`'s ingest path actually registered in the `SketchStore`
//! (`AggKind::Sketch { kind, config, .. }`). Serving time must reproduce
//! THAT decision, not independently re-derive a fresh one from a
//! hardcoded accuracy target: doing so picks whatever family/params an
//! accuracy-driven cost model prefers in the abstract (e.g. DDSketch
//! over Kll for quantiles, unconditionally), with no guarantee it matches
//! what's actually registered — and `SummaryExecutor::find_candidates`
//! requires an exact `(SketchAlgorithm, SketchParams)` match, by design (see
//! `summary_executor.rs::summary_params_match`'s doc: this deployment
//! chose strict equality over silently serving an answer under a looser
//! guarantee than what was planned).
//!
//! So before binding, [`lower_promql_to_l4node`] looks up what's actually
//! registered for the query's target metric and constructs an
//! [`ObservedFamilyCostModel`] that echoes that back — the resulting
//! `SummaryNode` matches reality by construction, not by a coincidental
//! accuracy-target match. When nothing is registered for the metric (or
//! this deployment's family/param mapping doesn't recognize the
//! registered shape), `observed` is `None` and binding falls back to the
//! same accuracy-driven `ControlPlaneCostModel` behavior as before — it
//! won't find a match either way, so the outcome (`find_candidates` finds
//! nothing) is unchanged, just for a more honest reason.

use std::rc::Rc;

use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryExpr, SummaryNode};

use control_plane::physical::runtime_capability::{OuterFn, SketchKindHandle};
use control_plane::sketch_algebra::cost_model::ObservedFamilyCostModel;
use control_plane::sketch_algebra::{
    bind_query_expr_with_cost_model, BindingError, L4Plan, PhysicalExpr,
};
use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::summary_executor::find_metric_in_query_expr;
use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
use crate::storage_engines::sketch_db::index::SketchStore;

/// Why a query couldn't be answered through the `SummaryNode`/`SummaryExecutor`
/// path — covers both `lower_promql_to_l4node`'s own failure to produce a
/// tree, AND (via `l4_readout.rs`'s `execute_l4_readout`) a failure of
/// `crate::query_engines::asap_query_engine::summary_exec::execute()` on a tree that DID lower successfully.
/// None of these are errors in the alarming sense — every variant is an
/// expected, frequent outcome for *some* fraction of live traffic; the
/// caller's only obligation is "fall back to the legacy path," never
/// "log this as a problem."
#[derive(Debug)]
pub enum LoweringSkip {
    /// `parse_query_expr_canonical` failed — same failure mode the legacy
    /// `analyze_promql_for_asap_tier` path already tolerates.
    ParseFailed(String),
    /// The query contains `rate(...)`/`irate(...)` (`OuterFn::Rate`, per
    /// `control_plane::asap_tier_analysis`'s own candidate analysis).
    /// `lower.rs`'s `bind_recursive` rewrites `AggIntent::Rate` →
    /// `Increase` before binding, so this WOULD otherwise bind
    /// successfully to a valid `SummaryAgg{Increase}` tree — but
    /// `summary_executor.rs` has no rate-division logic (dividing by a
    /// coverage-clamped range), so comparing against it would produce a
    /// spurious mismatch, not a real one. Must be excluded before ever
    /// calling into `control_plane`'s binder, not just deprioritized.
    RateShape,
    /// `bind_query_expr` itself failed (a genuine `BindingError`, e.g.
    /// L3→L4 schema-derivation failure).
    Implement(String),
    /// `bind_query_expr` returned a `PhysicalExpr` variant other than
    /// `Committed(L4Plan::Summary(_))`. Per `bind_query_expr`'s own doc
    /// this shouldn't happen in practice (it never picks a Phase ε.1
    /// placement), but the match is kept exhaustive and defensive rather
    /// than assuming.
    UnsupportedPhysicalShape,
    /// The root node is `SummaryExpr::Logical(_)` — the query didn't
    /// realize to any sketch/exact-agg binding at all (e.g.
    /// `topk(K, sum by(...)(rate(m[r])))`: `implement_tree_in_with` only
    /// recurses through `Aggregate` nodes, so hitting the outer
    /// `Sort`/`Limit` wraps the WHOLE tree as one opaque `Logical` blob
    /// even though the inner aggregate would bind fine on its own — see
    /// this crate's design doc). Not an error: this is exactly the
    /// existing `SummaryExecutorError::Logical`/"no candidate bound"
    /// outcome, just detected one step earlier so the caller can skip
    /// without even constructing a `QueryExecutionContext`.
    NotRealized,
    /// The tree lowered successfully, but `crate::query_engines::asap_query_engine::summary_exec::execute()`
    /// itself returned `Err` (`NoCandidates`, `MergeKindParamsMismatch`,
    /// a decode/merge failure surfaced from `summary_executor.rs`, ...).
    /// Always safe to just fall back — this means "can't answer this way
    /// right now" (e.g. the sid catalog doesn't have an exact
    /// `(SketchAlgorithm, SketchParams)` match), never "answered wrong."
    ExecuteFailed(String),
}

/// Map a registered sid's `(SketchKindHandle, SketchConfig)` — the
/// durable record of what planning actually decided for this metric — to
/// the `(SketchAlgorithm, SketchParams)` pair `ObservedFamilyCostModel`
/// needs to reproduce that decision exactly. `None` for shapes this
/// deployment doesn't map (e.g. `SketchKindHandle::Any`, which is an
/// analysis-time wildcard that's never actually registered on a sid).
///
/// Heap-bearing kinds (`CmsWithHeap`/`CountSketchWithHeap`) reuse their
/// heap-less base's `SketchConfig` shape for identity (no `heap_size`
/// field exists on `SketchConfig` at all — mirrors `to_delta_kind`'s same
/// note), so `heap_size` here is a placeholder; `summary_params_match`
/// only compares `width`/`depth` for these kinds, so it doesn't affect
/// matching.
fn observed_summary_params(
    kind: SketchKindHandle,
    config: &SketchConfig,
) -> Option<(SketchAlgorithm, SketchParams)> {
    const PLACEHOLDER_HEAP_SIZE: u32 = 100;
    match (kind, config) {
        (SketchKindHandle::DDSketch, SketchConfig::DDSketch { relative_accuracy }) => Some((
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch {
                alpha: *relative_accuracy,
            },
        )),
        (SketchKindHandle::Kll, SketchConfig::Kll { k }) => {
            Some((SketchAlgorithm::Kll, SketchParams::Kll { k: *k }))
        }
        (SketchKindHandle::Hll, SketchConfig::Hll { precision }) => Some((
            SketchAlgorithm::Hll,
            SketchParams::Hll {
                precision: *precision as u8,
            },
        )),
        (SketchKindHandle::CountMin, SketchConfig::CountMin { rows, cols }) => Some((
            SketchAlgorithm::Cms,
            SketchParams::Cms {
                width: *cols as u32,
                depth: *rows as u32,
            },
        )),
        (SketchKindHandle::CmsWithHeap, SketchConfig::CountMin { rows, cols }) => Some((
            SketchAlgorithm::CmsWithHeap,
            SketchParams::CmsWithHeap {
                width: *cols as u32,
                depth: *rows as u32,
                heap_size: PLACEHOLDER_HEAP_SIZE,
            },
        )),
        (SketchKindHandle::CountSketch, SketchConfig::CountSketch { rows, cols }) => Some((
            SketchAlgorithm::CountSketch,
            SketchParams::CountSketch {
                width: *cols as u32,
                depth: *rows as u32,
            },
        )),
        (SketchKindHandle::CountSketchWithHeap, SketchConfig::CountSketch { rows, cols }) => {
            Some((
                SketchAlgorithm::CountSketchWithHeap,
                SketchParams::CountSketchWithHeap {
                    width: *cols as u32,
                    depth: *rows as u32,
                    heap_size: PLACEHOLDER_HEAP_SIZE,
                },
            ))
        }
        _ => None,
    }
}

/// Look up what family/params is ACTUALLY registered for `metric` in
/// `index` — the durable record of planning's real decision (see this
/// module's docs). Checks every sid registered for the metric (no
/// group-by filter — an empty `required_keys` set matches any
/// registration, since we only need to know the FAMILY here, not resolve
/// a specific series) and returns the first sketch-typed one found.
/// `None` when nothing is registered (or only `ExactAgg` sids are —
/// those never consult `CostModel` at all, so there's nothing to
/// observe for them).
fn observed_family_for_metric(
    index: &SketchStore,
    metric: &str,
) -> Option<(SketchAlgorithm, SketchParams)> {
    for sid in index.instances_matching(metric, &Default::default()) {
        let found = index.with_instance(sid, |m| match &m.agg_kind {
            AggKind::Sketch { kind, config, .. } => observed_summary_params(*kind, config),
            AggKind::ExactAgg { .. } => None,
        });
        if let Some(Some(observed)) = found {
            return Some(observed);
        }
    }
    None
}

/// Look up what family/params `plan` says is materialized for `metric` —
/// the `BackendPlan`-sourced sibling of [`observed_family_for_metric`].
/// Unlike that function, no reconstruction is needed:
/// `Materialization.kind`/`.params` already ARE the pair this needs,
/// straight off the wire the control plane pushed. Returns the first
/// matching materialization found (mirrors
/// `observed_family_for_metric`'s "first sketch-typed one found"
/// semantics); `None` when the plan has no materialization for this
/// metric.
fn observed_family_for_metric_from_plan(
    plan: &control_plane::backend_plan::BackendPlan,
    metric: &str,
) -> Option<(SketchAlgorithm, SketchParams)> {
    plan.materializations.values().find_map(|m| {
        if !matches!(&m.source, planner_types::pre_asap::Source::TimeSeries { metric: mm } if mm == metric)
        {
            return None;
        }
        // `Materialization.kind`/`.params` are the flat type (spans
        // exact accumulators too) -- narrow to the sketch-only pair
        // this function returns, skipping exact-accumulator
        // materializations (mirrors `observed_family_for_metric`'s
        // "first sketch-typed one found" semantics).
        Some((m.kind.as_sketch_kind()?, m.params.as_sketch_params()?))
    })
}

/// Lower a raw PromQL query string to the `SummaryNode` tree
/// `crate::query_engines::asap_query_engine::summary_exec::execute`/`SummaryExecutor` needs — the actual
/// serving cutover (`live_serve.rs`). Returns `Err` for any shape serving
/// shouldn't attempt (parse failure, `rate()`, or anything that doesn't
/// realize to a concrete sketch/exact-agg binding) — see
/// `LoweringSkip`'s variants.
pub fn lower_promql_to_l4node(
    index: &SketchStore,
    query: &str,
    accuracy: AccuracyTarget,
    backend_plan: Option<&control_plane::backend_plan::BackendPlan>,
) -> Result<Rc<SummaryNode>, LoweringSkip> {
    // Reuse the SAME candidate analysis `engine.rs` already runs for the
    // legacy dispatch, rather than re-deriving rate detection via a
    // second raw-AST walk. `OuterFn::Rate` is the one shape that binds
    // SUCCESSFULLY today (via the Rate->Increase rewrite) but would
    // produce a semantically wrong comparison -- see `LoweringSkip::RateShape`.
    let analysis = control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(query);
    if analysis
        .candidates
        .iter()
        .any(|c| c.outer_fn == OuterFn::Rate)
    {
        return Err(LoweringSkip::RateShape);
    }

    let qe = control_plane::query_parser::parse_query_expr_canonical(query, accuracy.clone())
        .map_err(|e| LoweringSkip::ParseFailed(e.to_string()))?;
    if analysis.candidates.is_empty() {
        return Err(LoweringSkip::NotRealized);
    }

    // Serving time must reproduce the REAL planning decision, not
    // independently re-derive one -- see this module's docs. Prefer
    // reading it straight off an installed `BackendPlan`'s
    // materializations when one covers this metric --
    // `Materialization.kind`/`.params` already ARE the
    // `(SketchAlgorithm, SketchParams)` pair this needs, no
    // `AggregationConfig` reconstruction required (design-backend-plan-wire-format.md
    // §5). Otherwise fall back to the `SketchStore`-reconstruction path
    // (`observed_family_for_metric`), which is `None` when this metric
    // has nothing registered (or only an `ExactAgg` sid, which bypasses
    // `CostModel` entirely) -- `ObservedFamilyCostModel` then falls back
    // further to the accuracy-driven default.
    let observed = find_metric_in_query_expr(&qe).and_then(|metric| {
        backend_plan
            .and_then(|plan| observed_family_for_metric_from_plan(plan, &metric))
            .or_else(|| observed_family_for_metric(index, &metric))
    });
    let cost_model = ObservedFamilyCostModel::new(accuracy, observed);

    let physical = bind_query_expr_with_cost_model(&qe, &cost_model)
        .map_err(|e: BindingError| LoweringSkip::Implement(e.to_string()))?;

    match physical {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => {
            if matches!(node.expr, SummaryExpr::KeepPreAsap(_)) {
                Err(LoweringSkip::NotRealized)
            } else {
                Ok(node)
            }
        }
        _ => Err(LoweringSkip::UnsupportedPhysicalShape),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accuracy() -> AccuracyTarget {
        AccuracyTarget::Epsilon(0.01)
    }

    /// Empty index -- every test below exercises a shape that either
    /// self-excludes before ever consulting the `SketchStore`, or (for
    /// `frequency_intent_realizes_via_bind_query_expr`) relies on
    /// `ObservedFamilyCostModel` falling back to the accuracy-driven
    /// default when nothing is registered.
    fn empty_index() -> SketchStore {
        SketchStore::new()
    }

    #[test]
    fn rate_query_is_skipped_before_binding() {
        let idx = empty_index();
        let result =
            lower_promql_to_l4node(&idx, "rate(http_requests_total[5m])", accuracy(), None);
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn irate_query_is_skipped_before_binding() {
        let idx = empty_index();
        let result =
            lower_promql_to_l4node(&idx, "irate(http_requests_total[5m])", accuracy(), None);
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn unparseable_query_is_skipped() {
        let idx = empty_index();
        let result = lower_promql_to_l4node(&idx, "this is not promql (((", accuracy(), None);
        assert!(
            matches!(result, Err(LoweringSkip::ParseFailed(_))),
            "expected ParseFailed, got {result:?}"
        );
    }

    #[test]
    fn bare_selector_is_not_realized() {
        // L1 adoption (design-target-architecture.md Part B), accepted
        // behavior change: `asap_frontend_promql::lower_promql` no longer
        // wraps a bare selector in an implicit `Aggregate { Sum }` (see
        // control_plane's
        // `asap_tier_implement::bare_selector_has_no_aggregate_root_to_implement`
        // and `asap_tier_analysis::bare_selector_is_no_longer_asap_tier_answerable`
        // for the sibling fixes). With no `Aggregate` node anywhere in the
        // tree, `implement_tree_in_with` has nothing to bind and the whole
        // expression stays one opaque `Logical` blob, which this module
        // surfaces as `NotRealized`.
        let idx = empty_index();
        let result = lower_promql_to_l4node(&idx, "http_requests_total", accuracy(), None);
        assert!(
            matches!(result, Err(LoweringSkip::NotRealized)),
            "expected NotRealized, got {result:?}"
        );
    }

    #[test]
    fn frequency_intent_realizes_via_bind_query_expr() {
        // The exact shape `asap_tier_implement.rs`'s own
        // `implement_frequency_as_agg_test` pins as a KNOWN, documented gap
        // for `implement_promql_for_asap_tier`/`DefaultCostModel` (asserts
        // it stays `Logical` "for now"). `bind_query_expr`/
        // `ControlPlaneCostModel` is exactly the fix -- via
        // `realize_extension`/`readout_extension` (ASAPController#150) --
        // so this must realize to a real binding here, confirming this
        // module picked the seam that actually handles Frequency. No sid
        // is registered for this metric, so `ObservedFamilyCostModel`
        // falls back to the accuracy-driven default -- same outcome as
        // before this module started consulting the `SketchStore`.
        let idx = empty_index();
        let node = lower_promql_to_l4node(
            &idx,
            "count_over_time(http_requests_total[5m])",
            accuracy(),
            None,
        )
        .expect("Frequency intent must realize via bind_query_expr/ControlPlaneCostModel");
        assert!(
            !matches!(node.expr, SummaryExpr::KeepPreAsap(_)),
            "expected a real SummaryAgg/SummaryEstimate binding, got Logical (the gap \
             this module exists to avoid): {:?}",
            node.expr
        );
    }

    #[test]
    fn topk_over_rate_is_not_realized() {
        // The outer `Sort{Limit{Aggregate}}` shape: `implement_tree_in_with`
        // only recurses through `Aggregate`, so the whole tree wraps as
        // one opaque `Logical` blob -- self-excludes via `NotRealized`,
        // no special-case detection needed for this shape specifically.
        let idx = empty_index();
        let result = lower_promql_to_l4node(
            &idx,
            "topk(5, sum by (host) (rate(http_requests_total[5m])))",
            accuracy(),
            None,
        );
        assert!(
            matches!(result, Err(LoweringSkip::NotRealized) | Err(LoweringSkip::RateShape)),
            "expected NotRealized or RateShape (both are valid skips for this shape), got {result:?}"
        );
    }

    // ── BackendPlan-sourced family lookup (design-backend-plan-wire-format.md §5) ────

    mod backend_plan_cutover {
        use super::*;
        use crate::storage_engines::sketch_db::index::{
            AccuracyBound, Capability, SketchInstanceMetadata, SketchKindHandle,
        };
        use asap_types::enums::WindowKind;
        use control_plane::backend_plan::{BackendPlan, Materialization, WindowSpec};
        use planner_types::pre_asap::{ColumnRef, Source};
        use std::collections::HashMap;

        fn register_kll(idx: &SketchStore, metric: &str) {
            let cfg = SketchConfig::Kll { k: 269 };
            idx.register(SketchInstanceMetadata {
                sid: 1,
                metric_name: metric.to_string(),
                group_by_keys: Default::default(),
                capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
                agg_kind: AggKind::Sketch {
                    kind: SketchKindHandle::Kll,
                    config: cfg.clone(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: Some(AccuracyBound::from_config(&cfg)),
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
        }

        fn plan_with_ddsketch_materialization(metric: &str) -> BackendPlan {
            let fingerprint = asap_types::PolicyFingerprint(42);
            let mut materializations = HashMap::new();
            materializations.insert(
                fingerprint,
                Materialization {
                    fingerprint,
                    source: Source::TimeSeries {
                        metric: metric.to_string(),
                    },
                    window: WindowSpec {
                        kind: WindowKind::Tumbling,
                        size_ms: 60_000,
                        slide_ms: None,
                    },
                    group_by: Vec::new(),
                    rollup: Vec::new(),
                    // `Materialization.kind`/`.params` span both exact
                    // accumulators and sketches -- the flat
                    // `asap_types::SummaryKind`, not this file's own
                    // `planner_types::post_asap::SketchAlgorithm` import (see
                    // `physical::colored_dag::emitter`'s `use
                    // asap_types::{...}` note in control_plane).
                    kind: asap_types::SummaryKind::DDSketch,
                    params: asap_types::SummaryParams::DDSketch { alpha: 0.01 },
                    col: ColumnRef::SampleValue,
                    retention: None,
                    lifecycle: None,
                },
            );
            BackendPlan {
                plan_id: 1,
                generated_at_unix_ms: 0,
                materializations,
                routing: Vec::new(),
                monitors: Vec::new(),
            }
        }

        /// Extract the bound `(SketchAlgorithm, SketchParams)` from the
        /// `SummaryEstimate { summary_input: SummaryNode { expr: SummaryAgg {
        /// summary, params, .. }, .. }, .. }` shape a bare
        /// `quantile_over_time` query lowers to (confirmed by inspecting
        /// the tree directly).
        fn bound_family(node: &SummaryNode) -> (SketchAlgorithm, SketchParams) {
            match &node.expr {
                SummaryExpr::SummaryEstimate { summary_input, .. } => match &summary_input.expr {
                    SummaryExpr::SummaryAgg {
                        family: planner_types::post_asap::SummaryFamilyType::Sketch(kind, _),
                        ..
                    } => (kind.algorithm().clone(), kind.params().clone()),
                    other => panic!("expected a Sketch SummaryAgg, got {other:?}"),
                },
                other => panic!("expected SummaryEstimate, got {other:?}"),
            }
        }

        #[test]
        fn without_a_plan_sketchstore_reconstruction_wins() {
            // Baseline: no `BackendPlan` -- `observed_family_for_metric`'s
            // SketchStore reconstruction is the only source.
            let idx = SketchStore::new();
            register_kll(&idx, "m");
            let node =
                lower_promql_to_l4node(&idx, "quantile_over_time(0.99, m[1m])", accuracy(), None)
                    .expect("should lower");
            assert_eq!(bound_family(&node).0, SketchAlgorithm::Kll);
        }

        #[test]
        fn a_plan_materialization_wins_over_sketchstore_reconstruction() {
            // `SketchStore` has Kll registered for `m` (what
            // reconstruction alone would find), but the installed
            // `BackendPlan` says DDSketch for
            // the SAME metric. The plan must win -- serving time reads
            // planning's real (plan-sourced) decision, not whatever
            // `SketchStore` metadata happens to reconstruct to.
            let idx = SketchStore::new();
            register_kll(&idx, "m");
            let plan = plan_with_ddsketch_materialization("m");
            let node = lower_promql_to_l4node(
                &idx,
                "quantile_over_time(0.99, m[1m])",
                accuracy(),
                Some(&plan),
            )
            .expect("should lower");
            assert_eq!(
                bound_family(&node).0,
                SketchAlgorithm::DDSketch,
                "BackendPlan's materialization must take priority over SketchStore reconstruction"
            );
        }

        #[test]
        fn plan_present_but_no_materialization_for_metric_falls_back_to_sketchstore() {
            // The plan is installed but doesn't cover THIS metric --
            // `observed_family_for_metric_from_plan` returns `None` for
            // it, so the lookup must fall through to SketchStore
            // reconstruction, not silently fail to observe anything.
            let idx = SketchStore::new();
            register_kll(&idx, "m");
            let plan = plan_with_ddsketch_materialization("some_other_metric");
            let node = lower_promql_to_l4node(
                &idx,
                "quantile_over_time(0.99, m[1m])",
                accuracy(),
                Some(&plan),
            )
            .expect("should lower");
            assert_eq!(bound_family(&node).0, SketchAlgorithm::Kll);
        }
    }
}
