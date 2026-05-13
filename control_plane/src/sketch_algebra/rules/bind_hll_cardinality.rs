//! `BindHllOnCardinality` — `Aggregate{Cardinality{accuracy}}` → HLL.
//!
//! Reference: `control_plane/docs/design.md` §6 line ~714 lists
//! `BindHllOnCardinality` in the shared rule library. HLL is the
//! catalog-default sketch family for COUNT DISTINCT — see
//! `algebra::directory::sketch_type_for_agg` line ~59 for the existing
//! in-tree binding.
//!
//! Accuracy mapping: HLL standard error is `≈ 1.04 / √m` where
//! `m = 2^precision`. Inverting: `precision ≈ 2 · log2(1.04 / eps)`.
//! See `accuracy_profile.rs` (ASAPQuery-backend) for the formal bound
//! and the in-tree default rungs (precision 10 / 12 / 14 / 16 covering
//! ε ≈ 3% / 1.5% / 0.8% / 0.4%).

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::params::{HllParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::Rule;
use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};
use crate::types_v2::AccuracyTarget;

pub struct BindHllOnCardinality;

impl Rule for BindHllOnCardinality {
    fn name(&self) -> &'static str {
        "bind_hll_cardinality"
    }

    fn priority(&self) -> u16 {
        5
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
        let (intent_accuracy, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::Cardinality { accuracy } => (accuracy.clone(), child),
                _ => return None,
            },
            _ => return None,
        };

        // Read the tighter of the workload-level and per-intent budgets.
        let eps = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => return None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => a.min(*b),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { eps, .. })
            | (AccuracyTarget::EpsilonDelta { eps, .. }, AccuracyTarget::Epsilon(a)) => a.min(*eps),
            (
                AccuracyTarget::EpsilonDelta { eps: a, .. },
                AccuracyTarget::EpsilonDelta { eps: b, .. },
            ) => a.min(*b),
        };

        if eps <= 0.0 {
            return None;
        }

        let precision = hll_precision_for_eps(eps);

        Some(PhysicalExpr::estimate_over_agg(
            EstimateOp::Cardinality,
            SketchKind::Hll,
            SketchParams::Hll(HllParams { precision }),
            (**child).clone(),
        ))
    }
}

/// Map an ε standard-error budget to the HLL `precision` (log2 register
/// count). Mirrors the in-tree default rungs in `algebra::directory` —
/// precision 10 (ε≈3.25%) / 12 (ε≈1.6%) / 14 (ε≈0.81%) / 16 (ε≈0.41%).
fn hll_precision_for_eps(eps: f64) -> u32 {
    if eps <= 0.0 {
        return 16;
    }
    if eps >= 0.03 {
        10
    } else if eps >= 0.015 {
        12
    } else if eps >= 0.008 {
        14
    } else {
        16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precision_at_canonical_rungs() {
        assert_eq!(hll_precision_for_eps(0.03), 10);
        assert_eq!(hll_precision_for_eps(0.015), 12);
        assert_eq!(hll_precision_for_eps(0.008), 14);
        assert_eq!(hll_precision_for_eps(0.001), 16);
    }
}
