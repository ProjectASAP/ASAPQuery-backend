//! `BindDDSketchOnQuantile` — `Aggregate{Quantile{q, accuracy}}` → DDSketch.
//!
//! Reference: `control_plane/docs/design.md` §6 sketch_algebra (line ~565)
//! lists DDSketch alongside KLL as a quantile family. DDSketch has a
//! tail-relative-error guarantee — `|estimate − true| ≤ alpha · true` —
//! which makes it the preferred choice when the user asks for relative
//! tail-error rather than rank-error.
//!
//! Accuracy mapping: `AccuracyTarget::Epsilon(eps)` → `alpha = eps`.
//! DDSketch's parameter *is* the relative-error bound, so the mapping is
//! the identity. See `accuracy_profile.rs` (ASAPQuery-backend) for the
//! formal proof.
//!
//! Rule selection: this rule has priority 6 (just above
//! `BindKllOnQuantile`'s priority 5). The dispatcher's tie-break gives
//! DDSketch precedence whenever both rules fire on the same intent —
//! that matches the legacy `algebra::directory::sketch_type_for_agg`
//! behaviour, which already picks DDSketch as the default Quantile
//! sketch (see `controller/src/algebra/directory.rs` line ~59).

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::params::{DDSketchParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::Rule;
use crate::sketch_algebra::sketch_expr::{EstimateOp, SketchExpr};
use crate::types_v2::AccuracyTarget;

/// Bind a single-intent `Aggregate{Quantile{q, accuracy}}` to DDSketch.
pub struct BindDDSketchOnQuantile;

impl Rule for BindDDSketchOnQuantile {
    fn name(&self) -> &'static str {
        "bind_ddsketch_quantile"
    }

    fn priority(&self) -> u16 {
        // DDSketch wins the tie-break when both rules fire — see module
        // docstring for the rationale (matches the legacy directory
        // default for SP-2/SP-4).
        6
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<SketchExpr> {
        let (q, intent_accuracy, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::Quantile { q, accuracy } => (*q, accuracy.clone(), child),
                _ => return None,
            },
            _ => return None,
        };

        if !(0.0..=1.0).contains(&q) {
            return None;
        }

        // DDSketch needs an explicit relative-error budget. `Exact`
        // disables the rule; the dispatcher then picks KLL (or falls
        // through to logical pass-through).
        let alpha = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => return None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => a.min(*b),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { eps, .. })
            | (AccuracyTarget::EpsilonDelta { eps, .. }, AccuracyTarget::Epsilon(a)) => a.min(*eps),
            (
                AccuracyTarget::EpsilonDelta { eps: a, .. },
                AccuracyTarget::EpsilonDelta { eps: b, .. },
            ) => a.min(*b),
        };

        if alpha <= 0.0 || alpha >= 1.0 {
            return None;
        }

        Some(SketchExpr::estimate_over_agg(
            EstimateOp::Quantile { q },
            SketchKind::DDSketch,
            SketchParams::DDSketch(DDSketchParams { alpha }),
            (**child).clone(),
        ))
    }
}
