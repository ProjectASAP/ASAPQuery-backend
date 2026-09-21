//! L3 → L4/L5 lowering — `QueryExpr` walk that adopts
//! `asap_aware_mapping::replacement::realizations_for_intent` for the sketch algebra
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
//! `realizations_for_intent`, because that upstream pass actively realizes
//! them as a `Realization` this deployment's data plane
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
//! `realizations_for_intent` like any other intent.
//! `AggIntent::TopK { accuracy: Exact }` is the one remaining case left to
//! fall through to `realizations_for_intent`'s own `Logical` fallback
//! unchanged — a genuine, still-open `asap-plan` coverage gap (filed
//! upstream — see ASAPController#151), not something this deployment
//! should route around locally.

#![allow(dead_code)]

use std::rc::Rc;

use asap_aware_mapping::cost_model::CostModel;
use thiserror::Error;

use crate::physical::post_asap::cost_model::ControlPlaneCostModel;
use crate::physical::post_asap::deployment_expr::{PhysicalExpr, PostAsapPlan};
use crate::types::AccuracyTarget;
use planner_types::pre_asap::{AggIntent, QueryExpr};

/// Errors surfaced by the `bind_query_expr` lowering.
#[derive(Debug, Error)]
pub enum BindingError {
    /// L3 schema derivation failed while lifting an edge to `SummarySchema` —
    /// forwarded from `asap_aware_mapping::replacement`.
    #[error("L3->L4 implementation failed: {0}")]
    Implement(#[from] crate::planner_selection::SelectionError),
}

/// Lower an L3 query under the supplied accuracy target using
/// [`ControlPlaneCostModel`]. Returns [`PhysicalExpr::Committed`]; placement
/// is separate from summary selection.
pub fn bind_query_expr(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
) -> Result<PhysicalExpr, BindingError> {
    let cost_model = ControlPlaneCostModel::new(accuracy);
    bind_query_expr_with_cost_model(expr, &cost_model)
}

/// Bind a query using the explicitly supplied planning cost model.
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
        // These roots describe exact query semantics, not summary candidate
        // sites. Preserve the complete expression so archive execution retains
        // selector labels, ordering, and comparison filtering.
        QueryExpr::Scan { .. }
        | QueryExpr::Sort { .. }
        | QueryExpr::Filter { .. }
        | QueryExpr::BinaryOp {
            op: planner_types::pre_asap::BinaryOpKind::Compare(_),
            ..
        } => Ok(PostAsapPlan::Summary(
            crate::planner_selection::keep_pre_asap(expr)?,
        )),

        // The canonical L3 IR places `TimeRange` *above* a single-statistic
        // sketchable `Aggregate` (`lower_promql`'s window-swap; was
        // `Window` before the ASAPPlanner pin migration). `realizations_for_intent`
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

        // Workload-aware instant selectors retain their source horizon even
        // when there is no aggregate to bind.
        QueryExpr::TimeRange { .. } => Ok(PostAsapPlan::Summary(
            crate::planner_selection::keep_pre_asap(expr)?,
        )),

        // Exact Count cannot use this deployment's Sum accumulator: it counts
        // values rather than samples. Keep it logical for archive execution.
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

/// Rewrite Rate to Increase along the aggregate spine traversed by Planner.
/// This deployment computes rate by dividing the Increase readout by window
/// seconds, rather than storing a separate Rate accumulator.
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
