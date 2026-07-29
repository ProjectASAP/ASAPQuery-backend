//! PromQL → compositional L4 plan (`asap_plan::bind::implement_tree`
//! adoption, Step A of the plan-shaped-serving scoping).
//!
//! ## Terminology: "implementation", not "bind"
//!
//! This module (and the rest of `asap_tier_analysis`) has historically
//! used "bind"/"binding" loosely for several different things. `asap-plan`
//! itself is explicit about *not* doing that — its crate-level doc
//! (`crates/plan/src/lib.rs` in `ASAPController`) lays out a table of
//! four already-distinct senses of "bind" in this stack (L2 name
//! resolution `ColumnRef → ColumnId`; the L3→L4 per-node physical
//! choice; a deployment's own L4→L5 *placement* decision, e.g.
//! `control_plane::sketch_algebra::rules::bind_*`; and this module's own
//! prior usage) and deliberately names its L3→L4 step **"implementation"**
//! instead (Cascades/Volcano terminology: an *implementation rule* is
//! logical → physical, distinct from a *transformation rule*, logical →
//! logical) so it doesn't become a fifth colliding sense. Following that
//! lead here: [`collect_aggregate_roots`] *finds* realizable `Aggregate`
//! subtrees, [`implement_promql_for_asap_tier`] *implements* (realizes)
//! them via `asap_plan::bind::implement_tree_in_with` — "bind" is used
//! below only where citing `asap-plan`'s own module/function names
//! (`asap_plan::bind`, `implement_tree_in_with`) or the specific
//! deployment-side "Bind #2" placement decision that table names.
//!
//! ## The gap this closes
//!
//! `asap_tier_analysis::analyze_promql_for_asap_tier` collects every
//! `AggIntent` anywhere in the query tree into one **flat** list, then
//! checks each independently against `capability_for` — "does *some*
//! pre-registered accumulator answer *this one* intent." It never
//! composes multiple accumulators into a multi-stage plan (`Avg = Sum /
//! Count` stays archive-only for exactly this reason).
//!
//! `asap_plan::bind::implement_tree_in_with` already builds a genuinely
//! compositional plan (`Rc<L4Node>` / `SummaryExpr` — `SummaryAgg`,
//! `SummaryEstimate`, `SummaryMerge`, ...) from an L3 `QueryExpr` — but
//! it is deliberately conservative about *where* it looks for a
//! realizable `Aggregate`: hitting any non-`Aggregate` node (`Filter`,
//! `Window`, `Project`, ...) immediately wraps the **whole** subtree as
//! `SummaryExpr::Logical`, with no attempt to recurse past it looking
//! for a realizable `Aggregate` further down. Per `asap-plan`'s own
//! module doc: "rewriting through logical parents is the L4 rule
//! engine's job" — the deployment-specific "Bind #2" placement decision
//! that crate explicitly does not model. Finding realizable `Aggregate`
//! subtrees wherever they occur in the tree is squarely this crate's
//! job.
//!
//! [`collect_aggregate_roots`] does that: it mirrors
//! `asap_tier_analysis::collect_agg_intents`'s full recursion through
//! every `QueryExpr` variant, but instead of flattening `AggIntent`s it
//! collects the `Aggregate` subtree **roots** `implement_tree_in_with`
//! can realize. [`implement_promql_for_asap_tier`] parses PromQL, finds
//! those roots, and implements each independently.
//!
//! ## Known gap: `Extension`/`Frequency` under-realizes
//!
//! `asap-plan` has no opinion on `AggIntent::Extension` (control_plane's
//! deployment-specific `Frequency` intent rides in one) — it always
//! returns `Implementation::PassThrough`, by design (the crate
//! genuinely cannot see into an opaque extension payload). So today, an
//! `Aggregate` root whose one intent is a `Frequency` extension
//! implements to `SummaryExpr::Logical` here — i.e. this walker
//! currently *under-realizes* exactly the query shape `capability_for`'s
//! `as_frequency` special case already handles correctly on the flat
//! path. `implement_frequency_as_agg_test` below pins this as a known,
//! tracked gap (not a silent trap) — closing it needs either an
//! `asap-plan` extension hook or a hand-rolled `SummaryAgg`/
//! `SummaryEstimate` construction here (the L4Node-building helpers in
//! `asap_plan::bind` are private, so today there's no way to do the
//! latter without reimplementing them). Not yet wired into the live
//! serving path for this reason — see module doc for the broader
//! plan-shaped-serving scope this is Step A of.

use std::rc::Rc;

use asap_plan::{implement_tree_in_with, DefaultCostModel, ImplementError};
use asap_sketch::L4Node;

use crate::intent_algebra::query_expr::{BindingScope, QueryExpr};
use crate::query_parser::parse_query_expr_canonical;
use crate::types_v2::AccuracyTarget;

/// Fixed accuracy target for this L1 call site (L1 adoption,
/// design-target-architecture.md Part B) -- matches
/// `asap_tier_analysis::WARM_TIER_ANALYSIS_ACCURACY`; this module has no
/// per-query accuracy bound available either (see its own module doc:
/// "not yet a drop-in replacement for `analyze_promql_for_asap_tier`").
const IMPLEMENT_PROMQL_ACCURACY: AccuracyTarget = AccuracyTarget::Epsilon(0.01);

/// Find every independently-realizable `Aggregate` subtree in `expr`.
///
/// Mirrors `asap_tier_analysis::collect_agg_intents`'s exact recursion
/// through every `QueryExpr` variant, but collects `Aggregate` node
/// references instead of flattening their `AggIntent`s.
///
/// A **realizable-shaped** `Aggregate` (exactly one intent, no `HAVING`
/// — what `implement_tree_in_with`'s own internal comment calls its
/// "bindable shape") is pushed as a root and NOT recursed into further:
/// `implement_tree_in_with` already recurses into its own `child` via
/// `bind_summary_agg`, so walking past it here would find the same
/// nested aggregates twice.
///
/// A **non-realizable** `Aggregate` (multiple intents, or a `HAVING`
/// predicate) is different: `implement_tree_in_with` gives up on it
/// entirely (falls straight to `Logical`, without recursing into
/// `child` at all — see its own shape check). So a realizable
/// `Aggregate` nested inside a non-realizable one's `child` would
/// otherwise never be found. Recurse into `child` by hand for exactly
/// this case.
fn collect_aggregate_roots<'a>(expr: &'a QueryExpr, out: &mut Vec<&'a QueryExpr>) {
    match expr {
        QueryExpr::Aggregate {
            aggs,
            having,
            child,
            ..
        } => {
            if let ([_], None) = (aggs.as_slice(), having) {
                out.push(expr);
            } else {
                collect_aggregate_roots(child, out);
            }
        }
        QueryExpr::Window { child, .. } => collect_aggregate_roots(child, out),
        QueryExpr::LetBinding { expr, child, .. } => {
            collect_aggregate_roots(expr, out);
            collect_aggregate_roots(child, out);
        }
        QueryExpr::Scan { .. } | QueryExpr::Ref { .. } => {}
        // A-variants lifted in Batch 2 of the relational migration. They
        // carry no AggIntent themselves — recurse into their children to
        // find Aggregates further down the tree. `Partition` no longer
        // exists in the canonical IR — its keys fold into `Aggregate.by`
        // at construction time (`intent_algebra::lower`).
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Subquery { child, .. } => collect_aggregate_roots(child, out),
        QueryExpr::Merge { children } => {
            for c in children {
                collect_aggregate_roots(c, out);
            }
        }
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => {
            collect_aggregate_roots(left, out);
            collect_aggregate_roots(right, out);
        }
        // The PromQL-surface superset (Scalar/EvalTime/VectorFromScalar/
        // ScalarFromVector/Relabel/InfoJoin/Sample/TimeRange/TimeShift/
        // WindowFunc) isn't constructed by this parser today; the
        // single-child wrappers among them carry no `AggIntent` either
        // way, so a no-op default is safe (mirrors `collect_agg_intents`).
        _ => {}
    }
}

/// Errors from [`implement_promql_for_asap_tier`].
#[derive(Debug)]
pub enum ImplementPromqlError {
    /// PromQL failed to parse / lower to canonical `QueryExpr`.
    UnparseableMetricsql(String),
    /// L3→L4 implementation failed for a found `Aggregate` root (schema
    /// derivation error — see `asap_plan::bind::ImplementError`).
    Implement(ImplementError),
}

/// Parse `metricsql`, find every independently-realizable `Aggregate`
/// subtree ([`collect_aggregate_roots`]), and implement each into a
/// compositional L4 plan via `asap_plan::bind::implement_tree_in_with`.
///
/// Returns one `Rc<L4Node>` per root found, in tree order. Empty (not an
/// error) when the query has no realizable `Aggregate` at all (a bare
/// selector, or a window-bound exact-aggregation the lowerer didn't
/// emit an `Aggregate` for) — same "no candidates" contract
/// `analyze_promql_for_asap_tier` uses.
///
/// See the module doc for the current `Extension`/`Frequency`
/// under-realization gap — not yet a drop-in replacement for
/// `analyze_promql_for_asap_tier`, and not wired into the live serving
/// path.
pub fn implement_promql_for_asap_tier(
    metricsql: &str,
) -> Result<Vec<Rc<L4Node>>, ImplementPromqlError> {
    let expr = parse_query_expr_canonical(metricsql, IMPLEMENT_PROMQL_ACCURACY)
        .map_err(|e| ImplementPromqlError::UnparseableMetricsql(e.to_string()))?;

    let mut roots: Vec<&QueryExpr> = Vec::new();
    collect_aggregate_roots(&expr, &mut roots);

    roots
        .into_iter()
        .map(|root| {
            implement_tree_in_with(root, &BindingScope::default(), &DefaultCostModel)
                .map_err(ImplementPromqlError::Implement)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_sketch::SummaryExpr;

    fn root_count(metricsql: &str) -> usize {
        implement_promql_for_asap_tier(metricsql)
            .expect("parses and implements")
            .len()
    }

    #[test]
    fn bare_selector_has_no_aggregate_root_to_implement() {
        // L1 adoption (design-target-architecture.md Part B), accepted
        // behavior change -- see
        // asap_tier_analysis::bare_selector_is_no_longer_asap_tier_answerable's
        // comment: `lower_promql` doesn't implicitly wrap a bare selector
        // in `Aggregate { Sum }` the way the retired local parser did, so
        // there's no `Aggregate` node here at all to find a root at.
        let roots =
            implement_promql_for_asap_tier("http_requests_total").expect("parses and implements");
        assert!(roots.is_empty(), "{roots:?}");
    }

    #[test]
    fn single_sum_finds_one_root() {
        assert_eq!(root_count("sum(http_requests_total)"), 1);
    }

    #[test]
    fn quantile_implements_to_a_sketch_agg_estimate() {
        let roots = implement_promql_for_asap_tier(
            "quantile_over_time(0.99, http_requests_total_latency_ms[5m])",
        )
        .expect("parses and implements");
        assert_eq!(roots.len(), 1);
        assert!(
            matches!(roots[0].expr, SummaryExpr::SummaryEstimate { .. }),
            "quantile is sketch-realized (SummaryAgg wrapped in a SummaryEstimate \
             readout), not archive-only Logical: {:?}",
            roots[0].expr,
        );
    }

    #[test]
    fn sum_implements_to_an_exact_accumulator_agg_with_no_estimate() {
        let roots = implement_promql_for_asap_tier("sum(http_requests_total)")
            .expect("parses and implements");
        assert_eq!(roots.len(), 1);
        assert!(
            matches!(roots[0].expr, SummaryExpr::SummaryAgg { .. }),
            "Sum is an exact mergeable accumulator -- SummaryAgg with no \
             wrapping SummaryEstimate (the partial state IS the value): {:?}",
            roots[0].expr,
        );
    }

    #[test]
    fn avg_over_time_is_not_yet_realizable_matching_capability_for_today() {
        // Avg = Sum / Count needs a cross-policy join implement_tree_in_with
        // doesn't build (matches capability_for(&AggIntent::Avg) => None
        // on the flat path -- see asap_tier_analysis.rs and lower.rs's
        // AggFunc::Avg comment). Use avg_over_time (a range-vector
        // function), not bare instant avg(...) -- only the former is
        // guaranteed to lower through AggFunc::Avg in this frontend.
        let roots = implement_promql_for_asap_tier("avg_over_time(http_requests_total[5m])")
            .expect("parses and implements");
        assert_eq!(roots.len(), 1);
        assert!(
            matches!(roots[0].expr, SummaryExpr::Logical(_)),
            "Avg has no ASAP-tier realization yet on either path: {:?}",
            roots[0].expr,
        );
    }

    #[test]
    fn nested_aggregate_under_aggregate_finds_both_roots_independently() {
        // topk(5, quantile_over_time(0.9, m[5m])) -- outer TopK Aggregate
        // wraps an inner Quantile Aggregate. implement_tree_in_with's own
        // recursion into `child` handles this once we hand it the OUTER
        // root; collect_aggregate_roots must not ALSO independently
        // re-find the inner one (that would double-implement it).
        let roots = implement_promql_for_asap_tier(
            "topk(5, quantile_over_time(0.9, http_requests_total_latency_ms[5m]))",
        )
        .expect("parses and implements");
        assert_eq!(
            roots.len(),
            1,
            "the outer TopK root's own recursion already covers the nested \
             Quantile -- collect_aggregate_roots must find exactly one \
             independent root here, not two"
        );
    }

    #[test]
    fn filter_wrapping_a_realizable_aggregate_is_still_found() {
        // Filter { child: Aggregate{ Sum } } -- implement_tree_in_with
        // alone would give up at the Filter and wrap the whole subtree
        // as Logical (archive-only). collect_aggregate_roots must recurse
        // past the Filter to find the Sum root underneath -- this is the
        // exact gap Step A closes.
        let roots = implement_promql_for_asap_tier("sum(http_requests_total{zone=\"us-east\"})")
            .expect("parses and implements");
        assert_eq!(roots.len(), 1);
        assert!(
            matches!(roots[0].expr, SummaryExpr::SummaryAgg { .. }),
            "a label filter above the aggregate must not prevent implementation: {:?}",
            roots[0].expr,
        );
    }

    /// KNOWN GAP (see module doc): a bare frequency point-query
    /// (`count_over_time(m[5m])` -- a *windowed* Count is one of
    /// control_plane's two triggers for the `Extension`/`Frequency`
    /// intent, per `lower.rs`'s `frequency_trigger = !by.is_empty() ||
    /// windowed`, pinned by its own `windowed_count_is_frequency` unit
    /// test) binds to `Logical` here today, even though
    /// `capability_for`'s `as_frequency` special case on the flat path
    /// correctly realizes it as `FrequencyEstimate`. Pinned as a test so
    /// closing this gap is a deliberate, visible change, not a silent
    /// behavior shift.
    #[test]
    fn implement_frequency_as_agg_test() {
        // Per this test's own prior instructions: the gap it used to
        // document (under-realizing to `Logical` because `asap-plan` had
        // no `Extension`/`Frequency` opinion) is now closed -- not via an
        // `Extension` hook, but because L1 adoption
        // (design-target-architecture.md Part B) makes `count_over_time`
        // lower directly to `AggIntent::Count { accuracy: Epsilon(...) }`
        // (a real, first-class, non-exact intent) rather than needing
        // this deployment's `Frequency` extension wrapper at all --
        // `asap-plan` realizes a non-exact `Count` as a real CMS-backed
        // `SummaryAgg` + `SummaryEstimate` on its own.
        let roots = implement_promql_for_asap_tier("count_over_time(http_requests_total[5m])")
            .expect("parses and implements");
        assert_eq!(roots.len(), 1);
        assert!(
            matches!(roots[0].expr, SummaryExpr::SummaryEstimate { .. }),
            "expected a realized SummaryEstimate, got: {:?}",
            roots[0].expr,
        );
    }
}
