//! L3 → L4 lowering — `QueryExpr` walk that fires `Bind*` rules.
//!
//! Per `controller/docs/design.md` §6 sketch_algebra (line ~616): "the
//! optimizer's job is to selectively replace logical aggregates / joins
//! with their sketch-bound variants when a binding rule fires; everything
//! else stays inside `SketchExpr::Logical(…)`."
//!
//! Phase C ships the bottom-up walk — every `QueryExpr` sub-tree is
//! offered to the rule dispatcher; if a rule fires, its output replaces
//! the sub-tree; otherwise we recurse into the children and wrap the
//! result in `SketchExpr::Logical`.
//!
//! `LetBinding` / `Ref` survive the lowering: the bound expression is
//! lowered to L4, the child is lowered against the same workload-level
//! accuracy target, and the result is a `SketchExpr::LetBinding` /
//! `SketchExpr::Ref` with the L4-bound payload.

#![allow(dead_code)]

use thiserror::Error;

use crate::intent_algebra::QueryExpr;
use crate::sketch_algebra::rules::dispatch;
use crate::sketch_algebra::sketch_expr::SketchExpr;
use crate::types_v2::AccuracyTarget;

/// Errors surfaced by the `bind_query_expr` lowering. Reserved — Phase C
/// has no bind-time errors that aren't expressible as "no rule fires"
/// (the dispatcher returns `None` and the caller wraps the input in
/// `SketchExpr::Logical`). Defined now so future rules that *can* fail
/// at bind time (catalog mismatch, parameter overflow) plug in without
/// an API break.
#[derive(Debug, Error)]
pub enum BindingError {
    /// Carried for downstream consumers — Phase C has no producers yet.
    #[error("binding failed: {0}")]
    Other(String),
}

/// Lower an L3 `QueryExpr` to an L4 [`SketchExpr`] under the supplied
/// workload-level accuracy target. Bottom-up walk; `Bind*` rules consult
/// the `accuracy` param + the per-intent `accuracy` field on each
/// `Aggregate` and pick the tighter of the two.
///
/// Return value: `Ok(SketchExpr)` always — Phase C never errors. The
/// caller observes "no binding" via the returned `SketchExpr::Logical`
/// at the matched sub-tree position.
pub fn bind_query_expr(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
) -> Result<SketchExpr, BindingError> {
    Ok(bind_recursive(expr, &accuracy))
}

fn bind_recursive(expr: &QueryExpr, accuracy: &AccuracyTarget) -> SketchExpr {
    // Try the rule dispatcher first — if a `Bind*` rule fires, its
    // output replaces the matched sub-tree wholesale. The rule's output
    // already wraps the L3 child in `SketchExpr::Logical(...)` per the
    // `estimate_over_agg` constructor.
    if let Some(bound) = dispatch(expr, accuracy) {
        return bound;
    }

    // No rule matched — recurse into the children to find sub-trees that
    // bind. For pass-through nodes (`Scan`, `Window`, `LetBinding`,
    // `Ref`) we surface the recursive structure in `SketchExpr` directly
    // when relevant, otherwise we wrap the L3 sub-tree in `Logical`.
    match expr {
        QueryExpr::LetBinding { name, expr, child } => SketchExpr::LetBinding {
            name: name.clone(),
            expr: Box::new(bind_recursive(expr, accuracy)),
            child: Box::new(bind_recursive(child, accuracy)),
        },
        QueryExpr::Ref { name } => SketchExpr::Ref { name: name.clone() },
        // For `Aggregate`, the rule dispatcher already had a chance and
        // declined. For `Scan` and `Window`, the recursive walk is a
        // no-op (no binding rule applies to these shapes today). In all
        // cases, wrap the L3 sub-tree as a logical pass-through.
        QueryExpr::Aggregate { .. } | QueryExpr::Scan { .. } | QueryExpr::Window { .. } => {
            SketchExpr::Logical(expr.clone())
        }
        // A-variants lifted in Batch 2 of the legacy_expr migration. No
        // sketch binding rule applies to these shapes today — wrap as a
        // logical pass-through, matching the policy for `Aggregate` /
        // `Scan` / `Window`. Rule extensions can specialise individual
        // variants as the catalog grows.
        _ => SketchExpr::Logical(expr.clone()),
    }
}
