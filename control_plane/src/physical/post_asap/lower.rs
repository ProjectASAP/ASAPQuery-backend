//! L3 → L4/L5 lowering — `QueryExpr` walk that adopts
//! `asap_aware_mapping::bind::implement_tree_in_with` for the sketch algebra
//! itself (Step B of the plan-shaped-serving migration), with
//! `crate::physical::post_asap::cost_model::ControlPlaneCostModel` plugged in
//! for family selection + parameter sizing.
//!
//! Per `control_plane/docs/design.md` §6: "the optimizer's job is to
//! selectively replace logical aggregates / joins with their sketch-bound
//! variants when a binding rule fires; everything else stays inside
//! `Logical(…)`."
//!
//! Two node shapes are rewritten *before* delegating to
//! `implement_tree_in_with`, because `asap_aware_mapping::boundary::implementation_for`
//! actively binds them to an `Implementation` this deployment's data plane
//! doesn't (or, deliberately, shouldn't) serve — not something the
//! `CostModel` hook can reach, since the decision of *whether* to call
//! into `rank_candidates`/`size_params` at all is made before the
//! `CostModel` is ever consulted. See each helper's docs for the specific
//! reason.
//!
//! `AggIntent::Extension` (the `Frequency` point-query) needs no such
//! pre-pass anymore: `ControlPlaneCostModel::realize_extension`/
//! `readout_extension` (ASAPController#150) now realize it as a real
//! `CountSketch`, so the catch-all arm below commits it via
//! `implement_tree_in_with` like any other intent.
//! `AggIntent::TopK { accuracy: Exact }` is the one remaining case left to
//! fall through to `implement_tree_in_with`'s own `Logical` fallback
//! unchanged — a genuine, still-open `asap-plan` coverage gap (filed
//! upstream — see ASAPController#151), not something this deployment
//! should route around locally.

#![allow(dead_code)]

use std::rc::Rc;

use asap_aware_mapping::cost_model::CostModel;
use thiserror::Error;

use crate::physical::post_asap::cost_model::ControlPlaneCostModel;
use crate::physical::post_asap::deployment_expr::{PhysicalExpr, PostAsapPlan};
use crate::types_v2::AccuracyTarget;
use planner_types::pre_asap::{AggIntent, QueryExpr};

/// Errors surfaced by the `bind_query_expr` lowering.
#[derive(Debug, Error)]
pub enum BindingError {
    /// L3 schema derivation failed while lifting an edge to `SummarySchema` —
    /// forwarded from `asap_aware_mapping::bind`.
    #[error("L3->L4 implementation failed: {0}")]
    Implement(#[from] crate::planner_selection::SelectionError),
}

/// Lower an L3 `QueryExpr` to L4/L5 under the supplied workload-level
/// accuracy target, via [`ControlPlaneCostModel`] (this deployment's
/// planning-time family/sizing preferences). The result is always
/// [`PhysicalExpr::Committed`] — this walk never picks a Phase ε.1
/// backend/archive placement; that's a separate, later L5 decision
/// (`physical::deployment_cost::wire`).
pub fn bind_query_expr(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
) -> Result<PhysicalExpr, BindingError> {
    let cost_model = ControlPlaneCostModel::new(accuracy);
    bind_query_expr_with_cost_model(expr, &cost_model)
}

/// Like [`bind_query_expr`], but with an explicitly supplied [`CostModel`]
/// instead of the default planning-time [`ControlPlaneCostModel`].
///
/// This is the seam serving-time re-binding needs: `data_plane`'s
/// live-serving path (`post_asap_planner.rs`) must NOT re-derive family/params
/// choice independently of what was actually planned — it looks up what's
/// really registered in the `SketchStore` and hands in a cost model that
/// echoes that back, so the resulting `SummaryNode` matches reality by
/// construction rather than by a coincidental accuracy-target match. See
/// `control_plane/docs/design-target-architecture.md`'s "planning vs
/// serving" split.
pub fn bind_query_expr_with_cost_model(
    expr: &QueryExpr,
    cost_model: &dyn CostModel,
) -> Result<PhysicalExpr, BindingError> {
    Ok(PhysicalExpr::Committed(bind_recursive(expr, cost_model)?))
}

fn bind_recursive(
    expr: &QueryExpr,
    cost_model: &dyn CostModel,
) -> Result<PostAsapPlan, BindingError> {
    match expr {
        // `QueryExpr::LetBinding`/`::Ref` don't exist in the canonical IR
        // anymore (ASAPPlanner#181/#192 -- see
        // control_plane/docs/design-asapplanner-pin-migration.md), so
        // this walk can never actually receive that shape; the arms that
        // used to produce `PostAsapPlan::LetBinding`/`PostAsapPlan::Ref` here are
        // gone with it.

        // The canonical L3 IR places `TimeRange` *above* a single-statistic
        // sketchable `Aggregate` (`lower_promql`'s window-swap; was
        // `Window` before the ASAPPlanner pin migration). `implement_tree_with`
        // only recurses through the `Aggregate` spine (see its module
        // docs' "conservative fallbacks" — a logical parent above a
        // bindable aggregate subsumes it unbound), so push the range
        // down under the aggregate and re-dispatch, exactly as the old
        // hand-written walk did — the range then rides along inside the
        // bound node's `Logical(...)` child.
        QueryExpr::TimeRange { range, child }
            if matches!(child.as_ref(), QueryExpr::Aggregate { .. }) =>
        {
            let QueryExpr::Aggregate {
                reduction,
                measures: aggs,
                output_names,
                having,
                child: agg_child,
            } = child.as_ref()
            else {
                unreachable!("guarded by the `matches!` above")
            };
            let pushed = QueryExpr::Aggregate {
                reduction: reduction.clone(),
                measures: aggs.clone(),
                output_names: output_names.clone(),
                having: having.clone(),
                child: Rc::new(QueryExpr::TimeRange {
                    range: *range,
                    child: agg_child.clone(),
                }),
            };
            bind_recursive(&pushed, cost_model)
        }

        // `AggIntent::Count { accuracy: Exact }` — `boundary::implementation_for`'s
        // `exact_realization` actively binds this to `ExactKind::Count`,
        // but this deployment's data plane has no count accumulator: its
        // `SumAccumulator` only tracks `sum: f64`, so a `Count`
        // accumulator would silently return the sum of sample VALUES, not
        // the sample count (PR #200/#201, reverted — see the retired
        // `bind_exact_agg.rs`). Force the same fallback `implement_tree_with`
        // uses for unbound shapes, via the public `bind::logical`
        // ASAPController exposes for exactly this "deployment knows
        // better" case — no local schema-lift duplication needed. Stays
        // on archive, matching today's behavior.
        QueryExpr::Aggregate {
            measures: aggs,
            having: None,
            ..
        } if matches!(
            aggs.as_slice(),
            [AggIntent::Count {
                accuracy: AccuracyTarget::Exact
            }]
        ) =>
        {
            Ok(PostAsapPlan::Summary(
                crate::planner_selection::select_summary(expr, cost_model)?,
            ))
        }

        _ => {
            let rewritten = rewrite_rate_to_increase(expr);
            let node = crate::planner_selection::select_summary(&rewritten, cost_model)?;
            Ok(PostAsapPlan::Summary(node))
        }
    }
}

/// Rewrite every `AggIntent::Rate` reachable via the `Aggregate` spine
/// (nested `Aggregate.child` chains — the only shape `implement_tree_in_with`
/// itself recurses through; see its "conservative fallbacks" docs) to
/// `AggIntent::Increase`.
///
/// `boundary::implementation_for` gives `Rate` its own `SummaryKind::Rate`;
/// this deployment's data plane has no accumulator family for it — rate is
/// computed as `increase / window_seconds`, a scalar division on the
/// `Increase` accumulator's output applied at readout, not a separate
/// accumulator (see the retired `bind_exact_agg.rs`, which bound both to
/// the same accumulator for the same reason). Representing `Rate` as
/// `Increase` up through L4 preserves that — the L5 emitter is still the
/// one that knows to apply the division.
fn rewrite_rate_to_increase(expr: &QueryExpr) -> QueryExpr {
    match expr {
        QueryExpr::Aggregate {
            reduction,
            measures: aggs,
            output_names,
            having,
            child,
        } => QueryExpr::Aggregate {
            reduction: reduction.clone(),
            measures: aggs
                .iter()
                .map(|intent| {
                    if matches!(intent, AggIntent::Rate) {
                        AggIntent::Increase
                    } else {
                        intent.clone()
                    }
                })
                .collect(),
            output_names: output_names.clone(),
            having: having.clone(),
            child: Rc::new(rewrite_rate_to_increase(child)),
        },
        other => other.clone(),
    }
}
