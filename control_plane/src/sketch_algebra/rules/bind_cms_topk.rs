//! `BindCountSketchOnTopK` — `Aggregate{TopK{k, accuracy}}` → CountSketch-with-heap.
//!
//! Reference: `control_plane/docs/design.md` §6 line ~419 — "`SketchAgg
//! { intent, col }` … L4 emits `PhysicalExpr::SketchAgg`" — and
//! `intent_algebra::AggIntent::TopK` (heavy-hitter intent) maps directly
//! to a heavy-hitter sketch primitive. CountSketch with a heap of size
//! `k` is the textbook fit (Charikar-Chen-Farach-Colton); CMS with a
//! heap is an alternative the cost model can pick instead.
//!
//! Phase C ships the CountSketch-with-heap variant only — it pairs
//! cleanly with the existing `algebra::directory` defaults
//! (CountSketch is what the legacy in-tree planner already emits for
//! Frequency-shaped workloads) and the heap is the part that turns it
//! into a TopK primitive.
//!
//! Accuracy mapping: `AccuracyTarget::EpsilonDelta { eps, delta }` →
//! `(w, d) = (⌈e/eps⌉, ⌈ln(1/delta)⌉)`, identical to CMS. The heap size
//! is fixed at the requested `k`. See `accuracy_profile.rs`
//! (ASAPQuery-backend) for the formal heavy-hitter recall guarantee
//! (CountSketch + size-k heap recovers all heavy hitters with
//! frequency `≥ ‖f‖₁ / k` w.h.p.).

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::params::{CountSketchParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::Rule;
use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};
use crate::types_v2::AccuracyTarget;

pub struct BindCountSketchOnTopK;

impl Rule for BindCountSketchOnTopK {
    fn name(&self) -> &'static str {
        "bind_cms_topk"
    }

    fn priority(&self) -> u16 {
        5
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
        let (k_topk, intent_accuracy, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::TopK { k, accuracy } => (*k, accuracy.clone(), child),
                _ => return None,
            },
            _ => return None,
        };

        if k_topk == 0 {
            return None;
        }

        let (eps, delta) = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => return None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => (a.min(*b), 0.01),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { eps, delta })
            | (AccuracyTarget::EpsilonDelta { eps, delta }, AccuracyTarget::Epsilon(a)) => {
                (a.min(*eps), *delta)
            }
            (
                AccuracyTarget::EpsilonDelta { eps: a, delta: da },
                AccuracyTarget::EpsilonDelta { eps: b, delta: db },
            ) => (a.min(*b), da.min(*db)),
        };

        if eps <= 0.0 || delta <= 0.0 || delta >= 1.0 {
            return None;
        }

        let w = (std::f64::consts::E / eps).ceil() as u32;
        let d = (1.0 / delta).ln().ceil() as u32;
        // CountSketch columns MUST be a power of two: the agent
        // (asapedgeprocessor config_validate) rejects non-pow2 cols because
        // sketchlib bit-slices the hash with a pow2 column mask. Round the
        // ε-derived width UP to the next power of two — this only tightens
        // the additive bound (ε ≤ e/w) and prevents an agent-side
        // "cols must be a power of two" crash on config apply.
        let w = w.max(2).next_power_of_two();
        let d = d.max(1);

        Some(PhysicalExpr::estimate_over_agg(
            EstimateOp::TopK { k: k_topk },
            SketchKind::CountSketch,
            SketchParams::CountSketch(CountSketchParams {
                w,
                d,
                with_heap: true,
            }),
            (**child).clone(),
        ))
    }
}
