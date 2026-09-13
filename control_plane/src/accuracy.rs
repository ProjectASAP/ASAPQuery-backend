//! Adapt planner sketch parameters to ASAPPlanner accuracy guarantees.

use crate::types::SketchParams;
pub use asap_types::accuracy::{AccuracyKind, AccuracyProfile};
use planner_types::post_asap::SketchParams as PlannerParams;

pub fn derive(params: &SketchParams) -> AccuracyProfile {
    let normalized = match params {
        SketchParams::CountMinSketch { rows, cols, .. } => PlannerParams::Cms {
            depth: (*rows).max(1),
            width: (*cols).max(1),
        },
        SketchParams::CountSketch { epsilon, delta } => {
            return AccuracyProfile {
                epsilon: *epsilon,
                delta: Some(*delta),
                kind: AccuracyKind::AdditiveFrequency,
            }
        }
        SketchParams::HLL { precision } => PlannerParams::Hll {
            precision: u8::try_from(*precision).unwrap_or(14),
        },
        SketchParams::KLL { k, .. } => PlannerParams::Kll { k: (*k).max(1) },
        SketchParams::DDSketch {
            relative_accuracy, ..
        } => PlannerParams::DDSketch {
            alpha: *relative_accuracy,
        },
    };
    AccuracyProfile::from_sketch_params(&normalized)
        .expect("shared family has a numeric planner bound")
}

/// Source adapter for planner configuration parameters.
pub trait PlannerAccuracyProfile {
    fn derive(params: &SketchParams) -> Self;
}
impl PlannerAccuracyProfile for AccuracyProfile {
    fn derive(params: &SketchParams) -> Self {
        derive(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cms_bound_is_e_over_w() {
        let p = derive(&SketchParams::CountMinSketch {
            rows: 3,
            cols: 1000,
            metric_name: "m".into(),
        });
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert!((p.epsilon - std::f64::consts::E / 1000.0).abs() < 1e-12);
        assert!((p.delta.unwrap() - (-3.0_f64).exp()).abs() < 1e-12);
    }

    #[test]
    fn countsketch_passes_through_user_supplied_bounds() {
        let p = derive(&SketchParams::CountSketch {
            epsilon: 0.01,
            delta: 0.001,
        });
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert_eq!(p.epsilon, 0.01);
        assert_eq!(p.delta, Some(0.001));
    }

    #[test]
    fn hll_p14_matches_flajolet_bound() {
        let p = derive(&SketchParams::HLL { precision: 14 });
        assert_eq!(p.kind, AccuracyKind::RelativeCardinality);
        // 1.04 / √16384 = 0.008125
        assert!((p.epsilon - 0.008125).abs() < 1e-9);
    }

    #[test]
    fn kll_k200_matches_karnin_lang_liberty_bound() {
        let p = derive(&SketchParams::KLL {
            k: 200,
            quantiles: vec![0.5, 0.95, 0.99],
        });
        assert_eq!(p.kind, AccuracyKind::RankQuantile);
        assert!((p.epsilon - 2.296 / 200.0_f64.powf(0.9723)).abs() < 1e-12);
        assert!((p.delta.unwrap() - 0.01).abs() < 1e-12);
    }

    #[test]
    fn ddsketch_alpha_passes_through_verbatim() {
        for alpha in [0.005, 0.01, 0.02, 0.05] {
            let p = derive(&SketchParams::DDSketch {
                relative_accuracy: alpha,
                quantiles: vec![0.5, 0.99],
            });
            assert_eq!(p.kind, AccuracyKind::RelativeQuantile);
            assert_eq!(p.epsilon, alpha);
            assert_eq!(p.delta, Some(0.0));
        }
    }

    /// Keep the user-facing summary explicit about unknown HLL confidence.
    #[test]
    fn golden_parity_with_backend() {
        // HLL precision=14: ε = 0.008125, kind = relative_cardinality
        let p = derive(&SketchParams::HLL { precision: 14 });
        assert_eq!(
            p.summary(),
            "accuracy: ε=0.008125, δ=unknown, kind=relative_cardinality"
        );

        // KLL k=200: ε = 2.296 / 200^0.9723, kind = rank_quantile, δ = 0.01
        let p = derive(&SketchParams::KLL {
            k: 200,
            quantiles: vec![],
        });
        let expected_eps = 2.296_f64 / 200.0_f64.powf(0.9723);
        assert_eq!(
            p.summary(),
            format!("accuracy: ε={}, δ=0.01, kind=rank_quantile", expected_eps)
        );

        // DDSketch α=0.02: passes verbatim, δ=0, kind=relative_quantile
        let p = derive(&SketchParams::DDSketch {
            relative_accuracy: 0.02,
            quantiles: vec![],
        });
        assert_eq!(p.summary(), "accuracy: ε=0.02, δ=0, kind=relative_quantile");
    }

    #[test]
    fn kind_serialises_to_snake_case() {
        let p = derive(&SketchParams::HLL { precision: 14 });
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"kind\":\"relative_cardinality\""));
    }

    #[test]
    fn round_trip_through_serde() {
        let src = AccuracyProfile {
            epsilon: 0.008125,
            delta: None,
            kind: AccuracyKind::RelativeCardinality,
        };
        let json = serde_json::to_string(&src).unwrap();
        let back: AccuracyProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, src);
    }
}
