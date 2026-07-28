//! L3 → L4/L5 lowering — `QueryExpr` walk that adopts
//! `asap_plan::bind::implement_tree_in_with` for the sketch algebra
//! itself (Step B of the plan-shaped-serving migration), with
//! `crate::sketch_algebra::cost_model::ControlPlaneCostModel` plugged in
//! for family selection + parameter sizing.
//!
//! Per `control_plane/docs/design.md` §6: "the optimizer's job is to
//! selectively replace logical aggregates / joins with their sketch-bound
//! variants when a binding rule fires; everything else stays inside
//! `Logical(…)`."
//!
//! Two node shapes are rewritten *before* delegating to
//! `implement_tree_in_with`, because `asap_plan::boundary::implementation_for`
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

use asap_plan::bind::implement_tree_in_with;
use thiserror::Error;

use crate::intent_algebra::{AggIntent, BindingScope, QueryExpr};
use crate::sketch_algebra::cost_model::ControlPlaneCostModel;
use crate::sketch_algebra::physical_expr::{L4Plan, PhysicalExpr};
use crate::types_v2::{AccuracyTarget, BindingName};

/// Errors surfaced by the `bind_query_expr` lowering.
#[derive(Debug, Error)]
pub enum BindingError {
    /// L3 schema derivation failed while lifting an edge to `L4Schema` —
    /// forwarded from `asap_plan::bind`.
    #[error("L3->L4 implementation failed: {0}")]
    Implement(#[from] asap_plan::ImplementError),
}

/// Lower an L3 `QueryExpr` to L4/L5 under the supplied workload-level
/// accuracy target. The result is always [`PhysicalExpr::Committed`] —
/// this walk never picks a Phase ε.1 backend/archive placement; that's a
/// separate, later L5 decision (`optimizer::cost::wire`).
pub fn bind_query_expr(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
) -> Result<PhysicalExpr, BindingError> {
    Ok(PhysicalExpr::Committed(bind_recursive(expr, &accuracy)?))
}

fn bind_recursive(expr: &QueryExpr, accuracy: &AccuracyTarget) -> Result<L4Plan, BindingError> {
    match expr {
        QueryExpr::LetBinding { name, expr, child } => Ok(L4Plan::LetBinding {
            name: BindingName::new(name.as_str()),
            expr: Rc::new(bind_recursive(expr, accuracy)?),
            child: Rc::new(bind_recursive(child, accuracy)?),
        }),
        QueryExpr::Ref { name } => Ok(L4Plan::Ref {
            name: BindingName::new(name.as_str()),
        }),

        // The canonical L3 IR places `Window` *above* a single-statistic
        // sketchable `Aggregate` (`lower`'s window-swap). `implement_tree_in_with`
        // only recurses through the `Aggregate` spine (see its module
        // docs' "conservative fallbacks" — a logical parent above a
        // bindable aggregate subsumes it unbound), so push the window
        // down under the aggregate and re-dispatch, exactly as the old
        // hand-written walk did — the window then rides along inside the
        // bound node's `Logical(...)` child.
        QueryExpr::Window {
            kind,
            size,
            slide,
            child,
        } if matches!(child.as_ref(), QueryExpr::Aggregate { .. }) => {
            let QueryExpr::Aggregate {
                reduction,
                aggs,
                output_names,
                having,
                child: agg_child,
            } = child.as_ref()
            else {
                unreachable!("guarded by the `matches!` above")
            };
            let pushed = QueryExpr::Aggregate {
                reduction: reduction.clone(),
                aggs: aggs.clone(),
                output_names: output_names.clone(),
                having: having.clone(),
                child: Box::new(QueryExpr::Window {
                    kind: kind.clone(),
                    size: *size,
                    slide: *slide,
                    child: agg_child.clone(),
                }),
            };
            bind_recursive(&pushed, accuracy)
        }

        // `AggIntent::Count { accuracy: Exact }` — `boundary::implementation_for`'s
        // `exact_realization` actively binds this to `SummaryKind::Count`,
        // but this deployment's data plane has no count accumulator: its
        // `SumAccumulator` only tracks `sum: f64`, so a `Count`
        // accumulator would silently return the sum of sample VALUES, not
        // the sample count (PR #200/#201, reverted — see the retired
        // `bind_exact_agg.rs`). Force the same fallback `implement_tree_in_with`
        // uses for unbound shapes, via the public `bind::logical`
        // ASAPController exposes for exactly this "deployment knows
        // better" case — no local schema-lift duplication needed. Stays
        // on archive, matching today's behavior.
        QueryExpr::Aggregate {
            aggs, having: None, ..
        } if matches!(
            aggs.as_slice(),
            [AggIntent::Count {
                accuracy: AccuracyTarget::Exact
            }]
        ) =>
        {
            Ok(L4Plan::Summary(asap_plan::bind::logical(
                expr,
                &BindingScope::default(),
            )?))
        }

        _ => {
            let rewritten = rewrite_rate_to_increase(expr);
            let cost_model = ControlPlaneCostModel::new(accuracy.clone());
            let node = implement_tree_in_with(&rewritten, &BindingScope::default(), &cost_model)?;
            Ok(L4Plan::Summary(node))
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
            aggs,
            output_names,
            having,
            child,
        } => QueryExpr::Aggregate {
            reduction: reduction.clone(),
            aggs: aggs
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
            child: Box::new(rewrite_rate_to_increase(child)),
        },
        other => other.clone(),
    }
}
