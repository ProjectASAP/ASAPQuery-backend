//! `BindCmsOnCount` — `Aggregate{Count{accuracy}}` and `Aggregate{Frequency{accuracy}}` → CountMin.
//!
//! Reference: `controller/docs/design.md` §6 line ~713 lists `BindCmsOnCount`
//! in the shared rule library. CMS is the canonical sketch for both:
//!
//! - `AggIntent::Count{accuracy}` — when the user wants COUNT(*) per
//!   group with relaxed accuracy. The L4 readout is `EstimateOp::PointCount`
//!   over each group key (the L5 emitter materialises the per-key reads).
//! - `AggIntent::Frequency{accuracy}` — `count(*) WHERE key = k`. CMS is
//!   the textbook fit (Cormode-Muthukrishnan).
//!
//! Accuracy mapping: `AccuracyTarget::EpsilonDelta { eps, delta }` →
//! `(w, d) = (⌈e/eps⌉, ⌈ln(1/delta)⌉)`. CMS guarantees additive error
//! `≤ eps · ‖f‖₁` with probability `≥ 1 − delta` (see
//! `accuracy_profile.rs` in ASAPQuery-backend for the formal bound).
//!
//! `AccuracyTarget::Epsilon(eps)` (no delta) defaults to `delta = 0.01`
//! per the in-tree default (`SketchDefaults::count_min_sketch.delta`).
//! `Exact` does not bind.

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::params::{CmsParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::Rule;
use crate::sketch_algebra::sketch_expr::{EstimateOp, SketchExpr};
use crate::types_v2::AccuracyTarget;

/// Bind `Aggregate{Count}` / `Aggregate{Frequency}` to CMS.
pub struct BindCmsOnCount;

impl Rule for BindCmsOnCount {
    fn name(&self) -> &'static str {
        "bind_cms_count"
    }

    fn priority(&self) -> u16 {
        5
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<SketchExpr> {
        let (intent_accuracy, readout, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::Count { accuracy } => {
                    // Generic Count without a per-key readout falls back
                    // to the legacy logical aggregate; the only sketch-
                    // bound shape we emit here is a per-key PointCount.
                    // Use a sentinel "*" key meaning "all rows"; the L5
                    // emitter resolves it against the stage allocator's
                    // per-group output.
                    (
                        accuracy.clone(),
                        EstimateOp::PointCount { key: "*".into() },
                        child,
                    )
                }
                AggIntent::Frequency { accuracy } => (
                    accuracy.clone(),
                    EstimateOp::PointCount { key: "*".into() },
                    child,
                ),
                _ => return None,
            },
            _ => return None,
        };

        // Read `(eps, delta)` from the tighter of the workload-level
        // and per-intent accuracy targets.
        let (eps, delta) = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => return None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => (a.min(*b), 0.01),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { eps, delta })
            | (AccuracyTarget::EpsilonDelta { eps, delta }, AccuracyTarget::Epsilon(a)) => {
                (a.min(*eps), *delta)
            }
            (
                AccuracyTarget::EpsilonDelta {
                    eps: a,
                    delta: da,
                },
                AccuracyTarget::EpsilonDelta {
                    eps: b,
                    delta: db,
                },
            ) => (a.min(*b), da.min(*db)),
        };

        if eps <= 0.0 || delta <= 0.0 || delta >= 1.0 {
            return None;
        }

        let w = (std::f64::consts::E / eps).ceil() as u32;
        let d = (1.0 / delta).ln().ceil() as u32;
        let w = w.max(2);
        let d = d.max(1);

        Some(SketchExpr::estimate_over_agg(
            readout,
            SketchKind::Cms,
            SketchParams::Cms(CmsParams { w, d }),
            (**child).clone(),
        ))
    }
}
