//! L3 → L4 lowering — `QueryExpr` walk that fires `Bind*` rules.
//!
//! Per `control_plane/docs/design.md` §6 sketch_algebra (line ~616): "the
//! optimizer's job is to selectively replace logical aggregates / joins
//! with their sketch-bound variants when a binding rule fires; everything
//! else stays inside `PhysicalExpr::Logical(…)`."
//!
//! Phase C ships the bottom-up walk — every `QueryExpr` sub-tree is
//! offered to the rule dispatcher; if a rule fires, its output replaces
//! the sub-tree; otherwise we recurse into the children and wrap the
//! result in `PhysicalExpr::Logical`.
//!
//! `LetBinding` / `Ref` survive the lowering: the bound expression is
//! lowered to L4, the child is lowered against the same workload-level
//! accuracy target, and the result is a `PhysicalExpr::LetBinding` /
//! `PhysicalExpr::Ref` with the L4-bound payload.

#![allow(dead_code)]

use thiserror::Error;

use crate::intent_algebra::QueryExpr;
use crate::sketch_algebra::physical_expr::PhysicalExpr;
use crate::sketch_algebra::rules::dispatch;
use crate::types_v2::AccuracyTarget;

/// Errors surfaced by the `bind_query_expr` lowering. Reserved — Phase C
/// has no bind-time errors that aren't expressible as "no rule fires"
/// (the dispatcher returns `None` and the caller wraps the input in
/// `PhysicalExpr::Logical`). Defined now so future rules that *can* fail
/// at bind time (catalog mismatch, parameter overflow) plug in without
/// an API break.
#[derive(Debug, Error)]
pub enum BindingError {
    /// Carried for downstream consumers — Phase C has no producers yet.
    #[error("binding failed: {0}")]
    Other(String),
}

/// Lower an L3 `QueryExpr` to an L4 [`PhysicalExpr`] under the supplied
/// workload-level accuracy target. Bottom-up walk; `Bind*` rules consult
/// the `accuracy` param + the per-intent `accuracy` field on each
/// `Aggregate` and pick the tighter of the two.
///
/// Return value: `Ok(PhysicalExpr)` always — Phase C never errors. The
/// caller observes "no binding" via the returned `PhysicalExpr::Logical`
/// at the matched sub-tree position.
pub fn bind_query_expr(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
) -> Result<PhysicalExpr, BindingError> {
    Ok(bind_recursive(expr, &accuracy))
}

fn bind_recursive(expr: &QueryExpr, accuracy: &AccuracyTarget) -> PhysicalExpr {
    // Try the rule dispatcher first — if a `Bind*` rule fires, its
    // output replaces the matched sub-tree wholesale. The rule's output
    // already wraps the L3 child in `PhysicalExpr::Logical(...)` per the
    // `estimate_over_agg` constructor.
    if let Some(bound) = dispatch(expr, accuracy) {
        return bound;
    }

    // No rule matched — recurse into the children to find sub-trees that
    // bind. For pass-through nodes (`Scan`, `Window`, `LetBinding`,
    // `Ref`) we surface the recursive structure in `PhysicalExpr` directly
    // when relevant, otherwise we wrap the L3 sub-tree in `Logical`.
    match expr {
        QueryExpr::LetBinding { name, expr, child } => PhysicalExpr::LetBinding {
            name: crate::types_v2::BindingName::new(name.as_str()),
            expr: Box::new(bind_recursive(expr, accuracy)),
            child: Box::new(bind_recursive(child, accuracy)),
        },
        QueryExpr::Ref { name } => PhysicalExpr::Ref {
            name: crate::types_v2::BindingName::new(name.as_str()),
        },
        // The canonical L3 IR places `Window` *above* a single-statistic
        // sketchable `Aggregate` (`lower`'s window-swap).
        // The `Bind*` rules match `Aggregate` with the window as its
        // *child*, so push the window down under the aggregate and
        // re-dispatch — the window then rides along inside the bound
        // node's `Logical(...)` child, exactly as it did when the
        // aggregate sat on top. A `Window` over anything else stays a
        // logical pass-through.
        QueryExpr::Window {
            kind,
            size,
            slide,
            child,
        } if matches!(child.as_ref(), QueryExpr::Aggregate { .. }) => {
            let QueryExpr::Aggregate {
                by,
                aggs,
                output_names,
                having,
                child: agg_child,
            } = child.as_ref()
            else {
                unreachable!("guarded by the `matches!` above")
            };
            let pushed = QueryExpr::Aggregate {
                by: by.clone(),
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
        // For `Aggregate`, the rule dispatcher already had a chance and
        // declined. For `Scan` and a `Window` over a non-`Aggregate`
        // child, no binding rule applies — wrap the L3 sub-tree as a
        // logical pass-through.
        QueryExpr::Aggregate { .. } | QueryExpr::Scan { .. } | QueryExpr::Window { .. } => {
            PhysicalExpr::Logical(expr.clone())
        }
        // A-variants lifted in Batch 2 of the relational migration. No
        // sketch binding rule applies to these shapes today — wrap as a
        // logical pass-through, matching the policy for `Aggregate` /
        // `Scan` / `Window`. Rule extensions can specialise individual
        // variants as the catalog grows.
        _ => PhysicalExpr::Logical(expr.clone()),
    }
}
