//! Theoretical accuracy profile of a chosen `SketchParams`.
//!
//! Mirrors `ASAPQuery-backend/src/stores/sketch_db/accuracy.rs` —
//! both sides compute the same (ε, δ) from the same sketch type
//! + parameters, so a plan the control plane validates here meets
//! the same bound the backend will later surface on query
//! responses. The two files must be kept in lockstep; a golden
//! test at the bottom of this module pins the numeric parity.
//!
//! ## Why the control plane needs this
//!
//! The planner picks `SketchParams` (width/depth/K/α/precision)
//! to meet a user-supplied `accuracy_sla`. Today the cost model
//! approximates the bound inline in a few places
//! (`cost_model.rs:156-158`). Centralising the derivation here:
//!
//! * lets `DeploymentPlanCompiler` / `DeploymentCostPlanner` / any future
//!   planner compute the post-hoc ε of the chosen plan and
//!   verify it actually meets the SLA.
//! * surfaces the bound to downstream systems (backend via
//!   `/api/v1/plan` response; dashboards via
//!   `asap_otel_processor_accuracy_epsilon` gauge) without the
//!   caller re-deriving from scratch.
//!
//! ## Bounds we encode
//!
//! | Sketch            | `kind`                | ε formula            | δ formula          |
//! |-------------------|-----------------------|----------------------|--------------------|
//! | CountMinSketch    | `AdditiveFrequency`   | e / w                | 1 / 2^d            |
//! | CountSketch(ε, δ) | `AdditiveFrequency`   | ε (user-supplied)    | δ (user-supplied)  |
//! | HLL(p)            | `RelativeCardinality` | 1.04 / √(2^p)        | — (Gaussian σ)     |
//! | KLL(k)            | `RankQuantile`        | 2.296 / √k           | 0.01 (fixed)       |
//! | DDSketch(α)       | `RelativeQuantile`    | α                    | 0 (deterministic)  |
//!
//! Citations (verbatim from the backend):
//! * CMS — Cormode & Muthukrishnan, *J. Algorithms* 55(1) 2005
//! * CountSketch — Charikar, Chen, Farach-Colton, ICALP 2002
//! * HLL — Flajolet et al., DMTCS 2007
//! * KLL — Karnin, Lang, Liberty, FOCS 2016
//! * DDSketch — Masson, Rim, Lee, VLDB 2019

use crate::types::SketchParams;
pub use asap_types::{AccuracyKind, AccuracyProfile};

/// Planner-specific derivation over Planner-owned sketch parameters.
pub trait PlannerAccuracyProfile {
    fn derive(params: &SketchParams) -> Self;
}

impl PlannerAccuracyProfile for AccuracyProfile {
    fn derive(params: &SketchParams) -> Self {
        match params {
            SketchParams::CountMinSketch { rows, cols, .. } => {
                let cols = (*cols).max(1) as f64;
                let rows = (*rows).max(1) as i32;
                Self {
                    epsilon: std::f64::consts::E / cols,
                    delta: 0.5_f64.powi(rows),
                    kind: AccuracyKind::AdditiveFrequency,
                }
            }
            SketchParams::CountSketch { epsilon, delta } => Self {
                epsilon: *epsilon,
                delta: *delta,
                kind: AccuracyKind::AdditiveFrequency,
            },
            SketchParams::HLL { precision } => {
                let m = (1u64 << precision) as f64;
                Self {
                    epsilon: 1.04 / m.sqrt(),
                    delta: 0.0,
                    kind: AccuracyKind::RelativeCardinality,
                }
            }
            SketchParams::KLL { k, .. } => {
                let k = (*k).max(1) as f64;
                Self {
                    epsilon: 2.296 / k.sqrt(),
                    delta: 0.01,
                    kind: AccuracyKind::RankQuantile,
                }
            }
            SketchParams::DDSketch {
                relative_accuracy, ..
            } => Self {
                epsilon: *relative_accuracy,
                delta: 0.0,
                kind: AccuracyKind::RelativeQuantile,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cms_bound_is_e_over_w() {
        let p = AccuracyProfile::derive(&SketchParams::CountMinSketch {
            rows: 3,
            cols: 1000,
            metric_name: "m".into(),
        });
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert!((p.epsilon - std::f64::consts::E / 1000.0).abs() < 1e-12);
        assert!((p.delta - 0.125).abs() < 1e-12);
    }

    #[test]
    fn countsketch_passes_through_user_supplied_bounds() {
        let p = AccuracyProfile::derive(&SketchParams::CountSketch {
            epsilon: 0.01,
            delta: 0.001,
        });
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert_eq!(p.epsilon, 0.01);
        assert_eq!(p.delta, 0.001);
    }

    #[test]
    fn hll_p14_matches_flajolet_bound() {
        let p = AccuracyProfile::derive(&SketchParams::HLL { precision: 14 });
        assert_eq!(p.kind, AccuracyKind::RelativeCardinality);
        // 1.04 / √16384 = 0.008125
        assert!((p.epsilon - 0.008125).abs() < 1e-9);
    }

    #[test]
    fn kll_k200_matches_karnin_lang_liberty_bound() {
        let p = AccuracyProfile::derive(&SketchParams::KLL {
            k: 200,
            quantiles: vec![0.5, 0.95, 0.99],
        });
        assert_eq!(p.kind, AccuracyKind::RankQuantile);
        assert!((p.epsilon - 2.296 / 200.0_f64.sqrt()).abs() < 1e-12);
        assert!((p.delta - 0.01).abs() < 1e-12);
    }

    #[test]
    fn ddsketch_alpha_passes_through_verbatim() {
        for alpha in [0.005, 0.01, 0.02, 0.05] {
            let p = AccuracyProfile::derive(&SketchParams::DDSketch {
                relative_accuracy: alpha,
                quantiles: vec![0.5, 0.99],
            });
            assert_eq!(p.kind, AccuracyKind::RelativeQuantile);
            assert_eq!(p.epsilon, alpha);
            assert_eq!(p.delta, 0.0);
        }
    }

    /// **Parity contract** with the backend. Pinned numeric
    /// values are identical to what `ASAPQuery-backend`'s
    /// `AccuracyProfile::derive` produces for the matching
    /// `AggregationConfig`. Any change that drifts these tests
    /// likely needs a matching change on the backend side —
    /// and vice versa.
    #[test]
    fn golden_parity_with_backend() {
        // HLL precision=14: ε = 0.008125, kind = relative_cardinality
        let p = AccuracyProfile::derive(&SketchParams::HLL { precision: 14 });
        assert_eq!(
            p.summary(),
            "accuracy: ε=0.008125, δ=0, kind=relative_cardinality"
        );

        // KLL k=200: ε = 2.296 / √200, kind = rank_quantile, δ = 0.01
        let p = AccuracyProfile::derive(&SketchParams::KLL {
            k: 200,
            quantiles: vec![],
        });
        let expected_eps = 2.296_f64 / 200.0_f64.sqrt();
        assert_eq!(
            p.summary(),
            format!("accuracy: ε={}, δ=0.01, kind=rank_quantile", expected_eps)
        );

        // DDSketch α=0.02: passes verbatim, δ=0, kind=relative_quantile
        let p = AccuracyProfile::derive(&SketchParams::DDSketch {
            relative_accuracy: 0.02,
            quantiles: vec![],
        });
        assert_eq!(p.summary(), "accuracy: ε=0.02, δ=0, kind=relative_quantile");
    }

    #[test]
    fn kind_serialises_to_snake_case() {
        let p = AccuracyProfile::derive(&SketchParams::HLL { precision: 14 });
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"kind\":\"relative_cardinality\""));
    }

    #[test]
    fn round_trip_through_serde() {
        let src = AccuracyProfile {
            epsilon: 0.008125,
            delta: 0.0,
            kind: AccuracyKind::RelativeCardinality,
        };
        let json = serde_json::to_string(&src).unwrap();
        let back: AccuracyProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, src);
    }
}
