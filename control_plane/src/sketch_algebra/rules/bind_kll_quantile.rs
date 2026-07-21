//! `BindKllOnQuantile` — `Aggregate{Quantile{q, accuracy}}` → `SketchEstimate{Quantile{q}}` over `SketchAgg{KLL}`.
//!
//! Reference: `control_plane/docs/design.md` §6 line ~1157 — "the shared rule
//! `core::optimizer::rules::BindKllOnQuantile` matches `Aggregate { aggs:
//! [Quantile{q, accuracy}] }`, consults the sketch catalog (KLL has
//! `supported_intents: [Quantile]`, is mergeable, satisfies `ε=0.01` at
//! `k=200`), and rewrites the matched sub-DAG into a `SketchAgg` wrapped
//! in a `SketchEstimate`".
//!
//! Accuracy mapping: `AccuracyTarget::Epsilon(eps)` → KLL stream-size `k`.
//! KLL's rank-error bound is `≈ 2.6 / (eps · √π)` per Karnin-Lang-Liberty;
//! see `accuracy_profile.rs` (ASAPQuery-backend) for the formal mapping
//! table — `eps=0.01 → k=200`, `eps=0.005 → k=400`, `eps=0.001 → k=2048`,
//! `eps=0.0001 → k=8192`. `Exact` does not bind.

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};
use crate::sketch_algebra::rules::Rule;
use crate::types_v2::AccuracyTarget;
use asap_sketch::{SummaryKind, SummaryParams};

/// Bind a single-intent `Aggregate{Quantile{q, accuracy}}` to KLL.
pub struct BindKllOnQuantile;

impl Rule for BindKllOnQuantile {
    fn name(&self) -> &'static str {
        "bind_kll_quantile"
    }

    fn priority(&self) -> u16 {
        // KLL is the planner's preferred quantile sketch when the
        // accuracy budget is unspecified or when an `Exact`-by-default
        // SLA is in play (see `algebra::directory::sketch_type_for_agg`,
        // which already picks KLL as the in-tree default for SP-2/SP-4).
        // It edges out DDSketch on rank-error tightness when the budget
        // is rank-driven; DDSketch (priority 6) wins when the user
        // supplied a tail-relative epsilon.
        5
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
        // Only the single-intent Aggregate{Quantile{...}} shape binds —
        // multi-intent Aggregates and TopK-shaped quantiles take other
        // rules.
        let (q, intent_accuracy, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::Quantile { q, accuracy, .. } => (*q, accuracy.clone(), child),
                _ => return None,
            },
            _ => return None,
        };

        // Quantile φ must be in [0, 1].
        if !(0.0..=1.0).contains(&q) {
            return None;
        }

        // Pick the binding budget. Per the orchestrator-level convention,
        // the per-intent `accuracy` field overrides the workload-level
        // `AccuracyTarget` when both are present; here we use the more
        // restrictive (lower-eps) of the two when both are `Epsilon`. If
        // either side is `Exact`, the rule does not bind.
        let eps = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => return None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => a.min(*b),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { epsilon: eps, .. })
            | (AccuracyTarget::EpsilonDelta { epsilon: eps, .. }, AccuracyTarget::Epsilon(a)) => {
                a.min(*eps)
            }
            (
                AccuracyTarget::EpsilonDelta { epsilon: a, .. },
                AccuracyTarget::EpsilonDelta { epsilon: b, .. },
            ) => a.min(*b),
        };

        let k = kll_k_for_eps(eps);

        Some(PhysicalExpr::estimate_over_agg(
            EstimateOp::Quantile { q },
            SummaryKind::Kll,
            SummaryParams::Kll { k },
            (**child).clone(),
        ))
    }
}

/// Map an ε rank-error budget to a KLL stream-size `k`. Mirrors the
/// `accuracy_profile.rs` table referenced in the module docstring.
/// Bumped to power-of-two rungs (200, 400, 800, 2048, 8192) so the
/// in-tree `algebra::directory` continues to recognise the parameter.
fn kll_k_for_eps(eps: f64) -> u32 {
    if eps <= 0.0 {
        return 8192;
    }
    if eps >= 0.01 {
        200
    } else if eps >= 0.005 {
        400
    } else if eps >= 0.0025 {
        800
    } else if eps >= 0.001 {
        2048
    } else {
        8192
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k_at_canonical_rungs() {
        assert_eq!(kll_k_for_eps(0.01), 200);
        assert_eq!(kll_k_for_eps(0.005), 400);
        assert_eq!(kll_k_for_eps(0.001), 2048);
        assert_eq!(kll_k_for_eps(0.0001), 8192);
    }
}
