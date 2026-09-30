//! `AccuracyProfile` — derived error / confidence bound for each
//! `PrecomputeMaterialization`.
//!
//! Implements backend accuracy metadata consumed through SummaryCatalog and QueryPlan. Logical
//! guarantees are owned by ASAPPlanner and family bounds by summary libraries.
//! Given the Planner state family of a stored output, this exposes the
//! theoretical accuracy bound of every query answer computed from it — so
//! users and control planes see "this quantile is within ε relative error
//! with probability 1 - δ" as a first-class part of the schema, not a number
//! they have to rederive from the sketch literature.
//!
//! ## Scope of this module
//!
//! Pure derivation: [`derive`] maps a `SummaryFamilyType` to an
//! `AccuracyProfile`. No runtime measurement, no sampling — just the
//! textbook bound.
//!
//! These bounds are asymptotic / probabilistic worst-case
//! guarantees from the original sketch papers. Real error
//! distributions are often tighter; see §19 of the design doc
//! for empirical vs theoretical. For user-facing renderings
//! ("how far off might this answer be?") the theoretical bound
//! is the honest upper envelope.
//!
//! CMS, HLL, KLL, and DDSketch profiles come from ASAPPlanner's
//! `DefaultAccuracyModel`. HLL reports relative standard error with unknown
//! failure probability (`delta: null`), rather than a deterministic guarantee.
//! CountSketch and heap-retention extensions remain local.

use serde::{Deserialize, Serialize};

pub use asap_types::accuracy::{AccuracyKind, AccuracyProfile};
use planner_types::post_asap::{SketchParams as PlannerParams, SummaryFamilyType};

/// Derive the [`AccuracyProfile`] of state with the Planner `family`.
/// Exact families have zero error.
pub fn derive(family: &SummaryFamilyType) -> AccuracyProfile {
    let SummaryFamilyType::Sketch(kind, _) = family else {
        return AccuracyProfile::exact();
    };
    match kind.params() {
        PlannerParams::UnivMon { .. } => AccuracyProfile {
            epsilon: f64::MAX,
            delta: Some(1.0),
            kind: AccuracyKind::Uncalibrated,
        },
        params @ (PlannerParams::Cms { .. }
        | PlannerParams::Hll { .. }
        | PlannerParams::Kll { .. }
        | PlannerParams::DDSketch { .. }) => shared_profile(params.clone()),

        // CountMinSketchWithHeap: CMS frequency estimator
        // coupled with a heap of the top-`k` heaviest items
        // (Metwally et al.'s SpaceSaving-style retention).
        // Two bounds apply:
        //   * per-item point-lookup: ε_point = e/w
        //     (inherited from the CMS part)
        //   * top-K retention: any item with true frequency
        //     ≥ N/heap_size is guaranteed to be in the top-K
        //     output; each retained count is within N/heap_size
        //     of the true value (SpaceSaving guarantee).
        // We report the worse of the two as the user-facing ε.
        // `kind = TopK` signals that ε is the combined frequency +
        // retention guarantee. δ stays `exp(-d)` from the CMS half.
        // Sources:
        //   - Cormode & Muthukrishnan 2005 (CMS bound)
        //   - Metwally, Agrawal, El Abbadi. "Efficient
        //     computation of frequent and top-k elements in
        //     data streams." ICDT 2005. (top-K retention)
        PlannerParams::CmsWithHeap {
            width,
            depth,
            heap_size,
        } => {
            let cms = shared_profile(PlannerParams::Cms {
                depth: (*depth).max(1),
                width: (*width).max(1),
            });
            AccuracyProfile {
                epsilon: cms.epsilon.max(1.0 / f64::from(*heap_size).max(1.0)),
                delta: cms.delta,
                kind: AccuracyKind::TopK,
            }
        }

        // CountSketch: ε = 1/√w, δ = 1/2^d (Charikar-Chen-
        // Farach-Colton). Signed counters → tighter epsilon
        // than CMS but same confidence ramp with depth.
        PlannerParams::CountSketch { width, depth } => AccuracyProfile {
            epsilon: 1.0 / f64::from(*width).max(1.0).sqrt(),
            delta: Some(0.5_f64.powi(*depth as i32)),
            kind: AccuracyKind::AdditiveFrequency,
        },

        // CountSketchWithHeap: the CountSketch half gives ε_point = 1/√w;
        // the heap half gives ε_heap = 1/heap_size for retention.
        PlannerParams::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => AccuracyProfile {
            epsilon: (1.0 / f64::from(*width).max(1.0).sqrt())
                .max(1.0 / f64::from(*heap_size).max(1.0)),
            delta: Some(0.5_f64.powi(*depth as i32)),
            kind: AccuracyKind::TopK,
        },
        _ => AccuracyProfile {
            epsilon: f64::MAX,
            delta: Some(1.0),
            kind: AccuracyKind::Uncalibrated,
        },
    }
}

fn shared_profile(params: PlannerParams) -> AccuracyProfile {
    AccuracyProfile::from_sketch_params(&params).expect("shared family has a numeric planner bound")
}

/// Per-segment accuracy record. Attached to a multi-segment
/// [`AccuracyEnvelope`] so clients can see the error bound for
/// each piece of the schema-timeline-crossing query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerSegmentAccuracy {
    pub agg_id: u64,
    /// Half-open millisecond range `[start_ms, end_ms)` this
    /// segment covered.
    pub range_ms: [i64; 2],
    #[serde(flatten)]
    pub profile: AccuracyProfile,
}

/// Wire-side envelope emitted on PromQL responses as the
/// top-level `accuracy` field. Single-schema queries fill
/// `profile`; queries that span a schema-timeline boundary also
/// populate `per_segment` so the caller can see each piece's
/// bound. The top-level `profile` is the worst-case (max ε,
/// max δ) across segments — a conservative upper envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccuracyEnvelope {
    #[serde(flatten)]
    pub profile: AccuracyProfile,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub per_segment: Vec<PerSegmentAccuracy>,
}

impl AccuracyEnvelope {
    /// Envelope for a single resolved aggregation.
    pub fn single(profile: AccuracyProfile) -> Self {
        Self {
            profile,
            per_segment: Vec::new(),
        }
    }

    /// Build an envelope from a slice of per-segment tuples.
    /// Top-level `profile.epsilon` is `max(segment.epsilon)` and
    /// same for δ — the conservative envelope across segments.
    /// Returns `None` when the slice is empty.
    pub fn from_segments(segs: Vec<PerSegmentAccuracy>) -> Option<Self> {
        if segs.is_empty() {
            return None;
        }
        let mut epsilon = 0.0_f64;
        let mut delta = Some(0.0_f64);
        // Pick the "most lossy" kind: any non-Exact wins over
        // Exact; if mixed non-Exact kinds span segments we pick
        // the first non-Exact and trust the per-segment data for
        // the caller's finer needs.
        let mut kind = AccuracyKind::Exact;
        for s in &segs {
            if s.profile.epsilon > epsilon {
                epsilon = s.profile.epsilon;
            }
            // A known bound from another segment cannot fill unknown confidence.
            delta = match (delta, s.profile.delta) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            };
            if matches!(kind, AccuracyKind::Exact) && !matches!(s.profile.kind, AccuracyKind::Exact)
            {
                kind = s.profile.kind;
            }
        }
        Some(Self {
            profile: AccuracyProfile {
                epsilon,
                delta,
                kind,
            },
            per_segment: segs,
        })
    }

    /// Summary line suitable for Prometheus `infos`.
    pub fn summary(&self) -> String {
        if self.per_segment.is_empty() {
            self.profile.summary()
        } else {
            format!(
                "{} (worst-case over {} schema-timeline segments)",
                self.profile.summary(),
                self.per_segment.len()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::post_asap::{
        ExactKind, ExactParams, GroupingStrategy, SketchAlgorithm, SketchKind,
    };

    fn sketch(algorithm: SketchAlgorithm, params: PlannerParams) -> SummaryFamilyType {
        SummaryFamilyType::Sketch(
            SketchKind::new(algorithm, params),
            GroupingStrategy::PerSubpopulationInstance,
        )
    }

    fn cms(depth: u32, width: u32) -> SummaryFamilyType {
        sketch(SketchAlgorithm::Cms, PlannerParams::Cms { width, depth })
    }

    fn hll(precision: u8) -> SummaryFamilyType {
        sketch(SketchAlgorithm::Hll, PlannerParams::Hll { precision })
    }

    /// Planner and backend derivations agree for the same shared family parameters.
    #[test]
    fn planner_and_backend_shared_family_parity() {
        use control_plane::types::SketchParams;
        let cases = vec![
            (
                SketchParams::CountMinSketch {
                    rows: 7,
                    cols: 4096,
                    metric_name: "m".into(),
                },
                cms(7, 4096),
            ),
            (SketchParams::HLL { precision: 10 }, hll(10)),
            (
                SketchParams::KLL {
                    k: 512,
                    quantiles: vec![],
                },
                sketch(SketchAlgorithm::Kll, PlannerParams::Kll { k: 512 }),
            ),
            (
                SketchParams::DDSketch {
                    relative_accuracy: 0.05,
                    quantiles: vec![],
                },
                sketch(
                    SketchAlgorithm::DDSketch,
                    PlannerParams::DDSketch { alpha: 0.05 },
                ),
            ),
        ];
        for (params, family) in cases {
            let planner = control_plane::accuracy::derive(&params);
            let backend = derive(&family);
            assert_eq!(planner, backend, "parameters: {params:?}");
            assert_eq!(
                serde_json::to_value(planner).unwrap(),
                serde_json::to_value(backend).unwrap()
            );
        }
    }

    /// Unknown confidence stays unknown regardless of segment order.
    #[test]
    fn envelope_preserves_unknown_failure_probability() {
        let hll = derive(&hll(14));
        assert_eq!(hll.delta, None);
        for profiles in [
            [hll, AccuracyProfile::exact()],
            [AccuracyProfile::exact(), hll],
        ] {
            let segments = profiles
                .into_iter()
                .enumerate()
                .map(|(i, profile)| PerSegmentAccuracy {
                    agg_id: i as u64,
                    range_ms: [i as i64, i as i64 + 1],
                    profile,
                })
                .collect();
            let envelope = AccuracyEnvelope::from_segments(segments).unwrap();
            assert_eq!(envelope.profile.delta, None);
            assert!(serde_json::to_value(envelope).unwrap()["delta"].is_null());
        }
    }

    /// Backend-only UnivMon must not acquire a calibrated shared-family bound.
    #[test]
    fn univmon_remains_uncalibrated() {
        let p = derive(&sketch(
            SketchAlgorithm::UnivMon,
            PlannerParams::UnivMon {
                heap_size: 32,
                sketch_rows: 5,
                sketch_cols: 1024,
                layers: 4,
            },
        ));
        assert_eq!(p.kind, AccuracyKind::Uncalibrated);
    }

    /// Every exact family has zero error.
    #[test]
    fn exact_families_are_exact() {
        for (kind, params) in [
            (ExactKind::Sum, ExactParams::Sum),
            (ExactKind::Min, ExactParams::Min),
            (ExactKind::Max, ExactParams::Max),
            (ExactKind::Increase, ExactParams::Increase),
        ] {
            let p = derive(&SummaryFamilyType::ExactAggregate(kind, params));
            assert_eq!(p.epsilon, 0.0);
            assert_eq!(p.kind, AccuracyKind::Exact);
        }
    }

    /// CMS reports ε = e/w and δ = e^-d.
    #[test]
    fn cms_epsilon_is_e_over_w() {
        let p = derive(&cms(5, 2718));
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert!((p.epsilon - std::f64::consts::E / 2718.0).abs() < 1e-12);
        assert!((p.delta.unwrap() - (-5.0_f64).exp()).abs() < 1e-12);
    }

    /// A heap reports the worse of the CMS point bound and the retention bound.
    #[test]
    fn cms_with_heap_reports_the_worse_of_point_and_retention_bounds() {
        let heap = |depth, width, heap_size| {
            derive(&sketch(
                SketchAlgorithm::CmsWithHeap,
                PlannerParams::CmsWithHeap {
                    width,
                    depth,
                    heap_size,
                },
            ))
        };
        let retention = heap(5, 1_000_000, 10_000);
        assert_eq!(retention.kind, AccuracyKind::TopK);
        assert!((retention.epsilon - 1.0 / 10_000.0).abs() < 1e-12);
        assert!((retention.delta.unwrap() - (-5.0_f64).exp()).abs() < 1e-12);
        let point = heap(4, 100, 1_000_000);
        assert!((point.epsilon - std::f64::consts::E / 100.0).abs() < 1e-12);
    }

    /// CountSketch reports ε = 1/√w, which differs from the CMS bound.
    #[test]
    fn countsketch_epsilon_is_one_over_sqrt_w() {
        let cs = derive(&sketch(
            SketchAlgorithm::CountSketch,
            PlannerParams::CountSketch {
                width: 10_000,
                depth: 4,
            },
        ));
        assert!((cs.epsilon - 0.01).abs() < 1e-12);
        assert!(derive(&cms(4, 10_000)).epsilon < cs.epsilon);
    }

    /// Profiles round-trip through their wire form.
    #[test]
    fn accuracy_profile_roundtrips_through_serde() {
        let input = AccuracyProfile {
            epsilon: 0.008125,
            delta: None,
            kind: AccuracyKind::RelativeCardinality,
        };
        let json = serde_json::to_string(&input).unwrap();
        assert!(json.contains("\"relative_cardinality\""));
        let back: AccuracyProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, input);
    }
}
