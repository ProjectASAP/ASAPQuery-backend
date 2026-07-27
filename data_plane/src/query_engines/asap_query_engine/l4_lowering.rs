//! PromQL string → `asap_sketch::L4Node` bridge for shadow-mode
//! `SummaryExecutor` comparison. See
//! `data_plane/docs/l4node-plan-executor-design.md`'s "Rollout" section
//! for the full design and why this calls
//! `control_plane::sketch_algebra::lower::bind_query_expr`
//! (`ControlPlaneCostModel`) rather than
//! `control_plane::asap_tier_implement::implement_promql_for_asap_tier`
//! (`DefaultCostModel`, which can't realize the Frequency intent at all).
//!
//! `control_plane` runs in-process with `data_plane` in this deployment
//! (see `data_plane/Cargo.toml`'s "Phase 9" comment), so this is a
//! same-binary library call, not a new planning implementation living
//! here.

use std::rc::Rc;

use asap_sketch::{L4Node, SummaryExpr};

use control_plane::sketch_algebra::capability::OuterFn;
use control_plane::sketch_algebra::{BindingError, L4Plan, PhysicalExpr};
use control_plane::types_v2::AccuracyTarget;

/// Why `lower_promql_to_l4node` didn't produce a comparable `L4Node`.
/// None of these are errors in the alarming sense — every variant is an
/// expected, frequent outcome for *some* fraction of live traffic; the
/// caller's only obligation is "don't attempt a shadow comparison," never
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
}

/// Lower a raw PromQL query string to the `L4Node` tree
/// `asap_sketch::exec::execute`/`SummaryExecutor` needs, for shadow-mode
/// comparison against the legacy `SketchReducer` path. Returns `Err` for
/// any shape shadow-mode shouldn't attempt (parse failure, `rate()`,
/// or anything that doesn't realize to a concrete sketch/exact-agg
/// binding) — see `LoweringSkip`'s variants.
pub fn lower_promql_to_l4node(
    query: &str,
    accuracy: AccuracyTarget,
) -> Result<Rc<L4Node>, LoweringSkip> {
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

    let qe = control_plane::query_parser::parse_query_expr_canonical(query)
        .map_err(|e| LoweringSkip::ParseFailed(e.to_string()))?;

    let physical = control_plane::sketch_algebra::bind_query_expr(&qe, accuracy)
        .map_err(|e: BindingError| LoweringSkip::Implement(e.to_string()))?;

    match physical {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => {
            if matches!(node.expr, SummaryExpr::Logical(_)) {
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

    #[test]
    fn rate_query_is_skipped_before_binding() {
        let result = lower_promql_to_l4node("rate(http_requests_total[5m])", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn irate_query_is_skipped_before_binding() {
        let result = lower_promql_to_l4node("irate(http_requests_total[5m])", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::RateShape)),
            "expected RateShape, got {result:?}"
        );
    }

    #[test]
    fn unparseable_query_is_skipped() {
        let result = lower_promql_to_l4node("this is not promql (((", accuracy());
        assert!(
            matches!(result, Err(LoweringSkip::ParseFailed(_))),
            "expected ParseFailed, got {result:?}"
        );
    }

    #[test]
    fn bare_selector_realizes_to_a_summary_agg() {
        // Mirrors `implement_promql_for_asap_tier`'s own
        // `bare_selector_implements_to_an_exact_sum_agg` test -- a bare
        // selector is `Aggregate { Sum }` over the sample value.
        let node = lower_promql_to_l4node("http_requests_total", accuracy())
            .expect("bare selector should realize");
        assert!(
            matches!(node.expr, SummaryExpr::SummaryAgg { .. }),
            "expected SummaryAgg, got {:?}",
            node.expr
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
        // module picked the seam that actually handles Frequency.
        let node = lower_promql_to_l4node("count_over_time(http_requests_total[5m])", accuracy())
            .expect("Frequency intent must realize via bind_query_expr/ControlPlaneCostModel");
        assert!(
            !matches!(node.expr, SummaryExpr::Logical(_)),
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
        let result = lower_promql_to_l4node(
            "topk(5, sum by (host) (rate(http_requests_total[5m])))",
            accuracy(),
        );
        assert!(
            matches!(result, Err(LoweringSkip::NotRealized) | Err(LoweringSkip::RateShape)),
            "expected NotRealized or RateShape (both are valid skips for this shape), got {result:?}"
        );
    }
}
