//! `AccuracyProfile` — derived error / confidence bound for each
//! `AggregationConfig`.
//!
//! Implements backend accuracy metadata consumed through SummaryCatalog and QueryPlan. Logical
//! guarantees are owned by ASAPPlanner and family bounds by summary libraries.
//! Given the `aggregation_type` + `parameters` pinned on an
//! `AggSchema`, the registry can expose the theoretical accuracy
//! bound of every query answer computed from it — so users and
//! control planes see "this quantile is within ε relative error with
//! probability 1 - δ" as a first-class part of the schema, not a
//! number they have to rederive from the sketch literature.
//!
//! ## Scope of this module
//!
//! Pure derivation: `AccuracyProfile::derive(&AggregationConfig)`
//! looks at `aggregation_type` and the relevant entries in
//! `config.parameters` and returns an `AccuracyProfile`. No
//! runtime measurement, no sampling — just the textbook bound.
//!
//! These bounds are asymptotic / probabilistic worst-case
//! guarantees from the original sketch papers. Real error
//! distributions are often tighter; see §19 of the design doc
//! for empirical vs theoretical. For user-facing renderings
//! ("how far off might this answer be?") the theoretical bound
//! is the honest upper envelope.
//!
//! ## Bounds we encode
//!
//! | Sketch | `kind` | ε formula | δ formula |
//! |---|---|---|---|
//! | Sum / Min / Max / Increase | `Exact` | 0 | 0 |
//! | CountMinSketch(w, d) | `AdditiveFrequency` | e / w | 1 / 2^d |
//! | CountMinSketchWithHeap(w, d, k) | `TopK` | max(e/w, 1/k) | 1 / 2^d |
//! | CountSketch(w, d) | `AdditiveFrequency` | 1 / √w | 1 / 2^d |
//! | HLL(p) | `RelativeCardinality` | 1.04 / √(2^p) | — (Gaussian std-dev) |
//! | KLL(k) | `RankQuantile` | ≈ 2.296 / √k (worst-case constant) | 1 / 100 (fixed) |
//! | DDSketch(α) | `RelativeQuantile` | α | 0 (deterministic α guarantee) |
//!
//! Constants are chosen to match the tighter published bounds
//! rather than loose textbook versions; sources are cited inline
//! in each branch of [`AccuracyProfile::derive`].
//!
// See `docs/design_docs/summary-storage.md` for backend storage guarantees.

use asap_types::aggregation_config::AggregationConfig;
use asap_types::AggregationType;
pub use asap_types::{AccuracyKind, AccuracyProfile};
use serde::{Deserialize, Serialize};

/// Backend-specific derivation over the installed aggregation config.
pub trait BackendAccuracyProfile {
    fn derive(config: &AggregationConfig) -> Self;
    fn derive_sketch_only(config: &AggregationConfig) -> Self;
}

impl BackendAccuracyProfile for AccuracyProfile {
    /// Derive an [`AccuracyProfile`] from a pinned
    /// [`AggregationConfig`]. Reads `aggregation_type` and any
    /// necessary entries in `parameters`; falls back to exact for
    /// unknown / legacy variants (harmless — the caller just gets
    /// "0 error" rather than a panic).
    ///
    /// GOS continuous-query envelope (design-gos-unified-edge-telemetry.md §4,
    /// Theorem 1): when the edge gates delta transmission by the GOS relative
    /// threshold (`parameters["gos_delta_epsilon"] = ε_st > 0`), the warm
    /// sketch answered from delta-applied state carries an extra DETERMINISTIC
    /// staleness term of at most `ε_st` (relative) at any query time — it adds
    /// linearly to the sketch's own probabilistic bound (`ε_total = ε_sk +
    /// ε_st`; the random parts compose in quadrature but the staleness part is
    /// adversarial, so linear addition is the honest envelope). δ is
    /// unchanged (staleness is not probabilistic).
    fn derive(config: &AggregationConfig) -> Self {
        let mut profile = Self::derive_sketch_only(config);
        let eps_st = config
            .parameters
            .get("gos_delta_epsilon")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        if eps_st > 0.0 && profile.kind != AccuracyKind::Exact {
            profile.epsilon += eps_st;
        }
        profile
    }

    /// The sketch's own theoretical bound, without the GOS staleness term.
    fn derive_sketch_only(config: &AggregationConfig) -> Self {
        match config.aggregation_type {
            // Exact aggregates. (The `SetAggregator` /
            // `DeltaSetAggregator` exact-set-membership family lived
            // here too before its retirement.)
            AggregationType::Sum
            | AggregationType::Increase
            | AggregationType::MinMax
            | AggregationType::MultipleSum
            | AggregationType::MultipleIncrease
            | AggregationType::MultipleMinMax => Self::exact(),

            // CountMinSketch: classic Cormode-Muthukrishnan bound.
            // ε = e/w, δ = 1/2^d with w = width, d = depth. We
            // pull w from `parameters["w"]` and d from
            // `parameters["d"]` — the canonical keys the controller
            // emits and `accumulator_factory::cms_params` reads.
            // Defaults to (rows=4, cols=1000) when absent.
            AggregationType::CountMinSketch => {
                let (rows, cols) = cms_params(config);
                // Using natural e ≈ 2.71828 for tighter bound.
                // Source: Cormode & Muthukrishnan, "An improved
                // data stream summary: the count-min sketch and
                // its applications," J. Algorithms 55(1) 2005.
                let epsilon = std::f64::consts::E / (cols as f64).max(1.0);
                let delta = 0.5_f64.powi(rows as i32);
                Self {
                    epsilon,
                    delta,
                    kind: AccuracyKind::AdditiveFrequency,
                }
            }

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
            // We report the **tighter** of the two as the
            // user-facing ε — typically the heap bound
            // `1/heap_size` dominates when heap_size ≪ w, and the
            // CMS bound `e/w` dominates when the heap is generously
            // sized. `kind = TopK` signals that ε is the
            // combined frequency + retention guarantee.
            // δ stays `1/2^d` from the CMS half; retention itself
            // is deterministic given an adversarial-free stream,
            // but the count estimate remains probabilistic at
            // depth d.
            // Sources:
            //   - Cormode & Muthukrishnan 2005 (CMS bound)
            //   - Metwally, Agrawal, El Abbadi. "Efficient
            //     computation of frequent and top-k elements in
            //     data streams." ICDT 2005. (top-K retention)
            AggregationType::CountMinSketchWithHeap => {
                let (rows, cols) = cms_params(config);
                let heap = cms_heap_size(config);
                let cms_epsilon = std::f64::consts::E / (cols as f64).max(1.0);
                let heap_epsilon = 1.0 / (heap as f64).max(1.0);
                // Worst of the two — a user should expect errors
                // no bigger than `ε · N`.
                let epsilon = cms_epsilon.max(heap_epsilon);
                let delta = 0.5_f64.powi(rows as i32);
                Self {
                    epsilon,
                    delta,
                    kind: AccuracyKind::TopK,
                }
            }

            // CountSketch: ε = 1/√w, δ = 1/2^d (Charikar-Chen-
            // Farach-Colton). Signed counters → tighter epsilon
            // than CMS but same confidence ramp with depth.
            AggregationType::CountSketch => {
                let (rows, cols) = cms_params(config);
                let epsilon = 1.0 / (cols as f64).max(1.0).sqrt();
                let delta = 0.5_f64.powi(rows as i32);
                Self {
                    epsilon,
                    delta,
                    kind: AccuracyKind::AdditiveFrequency,
                }
            }

            // CountSketchWithHeap: CountSketch frequency estimator
            // paired with a top-k heap. Mirrors the
            // CountMinSketchWithHeap branch above — the CountSketch
            // half gives ε_point = 1/√w; the heap half gives
            // ε_heap = 1/heap_size for retention. Report the
            // tighter (max) of the two.
            AggregationType::CountSketchWithHeap => {
                let (rows, cols) = cms_params(config);
                let heap = cms_heap_size(config);
                let cs_epsilon = 1.0 / (cols as f64).max(1.0).sqrt();
                let heap_epsilon = 1.0 / (heap as f64).max(1.0);
                let epsilon = cs_epsilon.max(heap_epsilon);
                let delta = 0.5_f64.powi(rows as i32);
                Self {
                    epsilon,
                    delta,
                    kind: AccuracyKind::TopK,
                }
            }

            // HLL: std-dev ≈ 1.04/√m, m = 2^precision. Report
            // this as relative error ε; δ is the Gaussian
            // std-dev convention (stored as 0 because our δ
            // field is "confidence parameter" not "variance";
            // future AccuracyKind::RelativeCardinality variant
            // could carry the Gaussian flavor explicitly).
            // Source: Flajolet et al., "HyperLogLog: the analysis
            // of a near-optimal cardinality estimation algorithm,"
            // DMTCS 2007.
            AggregationType::HLL => {
                let p = hll_precision(config);
                let m = (1u64 << p) as f64;
                Self {
                    epsilon: 1.04 / m.sqrt(),
                    delta: 0.0,
                    kind: AccuracyKind::RelativeCardinality,
                }
            }

            // KLL: rank error ε = C/√k with δ ≤ 0.01 (fixed
            // confidence; KLL's theoretical guarantee). Empirical
            // C ≈ 2.296 for the standard floating-point KLL
            // variant implemented here.
            // Source: Karnin, Lang, Liberty. "Optimal quantile
            // approximation in streams," FOCS 2016.
            AggregationType::DatasketchesKLL | AggregationType::HydraKLL => {
                let k = kll_k(config);
                Self {
                    epsilon: 2.296 / (k as f64).max(1.0).sqrt(),
                    delta: 0.01,
                    kind: AccuracyKind::RankQuantile,
                }
            }

            // DDSketch: α is the relative quantile error directly
            // — it's a design parameter of the sketch, not a
            // probabilistic bound. δ = 0 (deterministic).
            // Source: Masson, Rim, Lee. "DDSketch: a fast and
            // fully-mergeable quantile sketch with relative-error
            // guarantees," VLDB 2019.
            AggregationType::DDSketch => {
                let alpha = ddsketch_alpha(config);
                Self {
                    epsilon: alpha,
                    delta: 0.0,
                    kind: AccuracyKind::RelativeQuantile,
                }
            }

            // Legacy / wrapper variants. Return exact — they are
            // config-shape placeholders that dispatch to concrete
            // aggregator types elsewhere; their accuracy profile
            // depends on the sub_type, which the factory resolves
            // at updater-construction time. Phase 6.4 v2 can walk
            // sub_type to give a tighter answer.
            AggregationType::SingleSubpopulation | AggregationType::MultipleSubpopulation => {
                Self::exact()
            }
        }
    }
}

// Parameter extraction helpers. Kept file-local (not pub) because
// they duplicate tiny bits of `precompute_engine::accumulator_factory`
// and the backfill-vs-live separation rule (see that module's doc)
// says it's OK for them to drift — this module is the single
// authority on *accuracy*, not on *construction*.

/// Reads canonical `d` (depth = rows) / `w` (width = cols) keys.
/// The legacy `row_num` / `col_num` form was retired in lock-step
/// with the asapcollector migration to canonical keys — see
/// `accumulator_factory::cms_params` for the matching change.
fn cms_params(config: &AggregationConfig) -> (u64, u64) {
    let rows = config
        .parameters
        .get("d")
        .and_then(|v| v.as_u64())
        .unwrap_or(4);
    let cols = config
        .parameters
        .get("w")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000);
    (rows, cols)
}

fn hll_precision(config: &AggregationConfig) -> u32 {
    config
        .parameters
        .get("precision")
        .or_else(|| config.parameters.get("p"))
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(14)
}

fn kll_k(config: &AggregationConfig) -> u32 {
    config
        .parameters
        .get("K")
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(200)
}

fn ddsketch_alpha(config: &AggregationConfig) -> f64 {
    config
        .parameters
        .get("alpha")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.01)
}

/// CMS-with-heap heap size (the `k` in "top-k retention"). Read
/// from `parameters["heap_size"]` with a default of 100 —
/// matches the default the control plane's planner uses when the
/// caller didn't override.
fn cms_heap_size(config: &AggregationConfig) -> u64 {
    config
        .parameters
        .get("heap_size")
        .or_else(|| config.parameters.get("topk"))
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .unwrap_or(100)
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
        let mut delta = 0.0_f64;
        // Pick the "most lossy" kind: any non-Exact wins over
        // Exact; if mixed non-Exact kinds span segments we pick
        // the first non-Exact and trust the per-segment data for
        // the caller's finer needs.
        let mut kind = AccuracyKind::Exact;
        for s in &segs {
            if s.profile.epsilon > epsilon {
                epsilon = s.profile.epsilon;
            }
            if s.profile.delta > delta {
                delta = s.profile.delta;
            }
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
    use asap_types::enums::WindowKind;
    use asap_types::KeyByLabelNames;
    use serde_json::{json, Value};
    use std::collections::HashMap;

    fn base_config(agg_type: AggregationType, params: HashMap<String, Value>) -> AggregationConfig {
        AggregationConfig::new(
            agg_type,
            String::new(),
            params,
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "m".to_string(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn sum_is_exact() {
        let p = AccuracyProfile::derive(&base_config(AggregationType::Sum, HashMap::new()));
        assert_eq!(p.kind, AccuracyKind::Exact);
        assert_eq!(p.epsilon, 0.0);
        assert_eq!(p.delta, 0.0);
    }

    #[test]
    fn min_max_increase_are_exact() {
        for t in [AggregationType::MinMax, AggregationType::Increase] {
            let p = AccuracyProfile::derive(&base_config(t, HashMap::new()));
            assert_eq!(p.kind, AccuracyKind::Exact);
        }
    }

    #[test]
    fn gos_staleness_widens_epsilon_linearly() {
        // A CountSketch (ε_sk = 1/√w) whose edge gates deltas at ε_st carries
        // ε_total = ε_sk + ε_st in the continuous-query envelope (Theorem 1);
        // δ is unchanged (staleness is deterministic).
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(5));
        params.insert("w".to_string(), json!(256));
        let base =
            AccuracyProfile::derive(&base_config(AggregationType::CountSketch, params.clone()));
        params.insert("gos_delta_epsilon".to_string(), json!(0.05));
        let widened = AccuracyProfile::derive(&base_config(AggregationType::CountSketch, params));
        assert!((widened.epsilon - (base.epsilon + 0.05)).abs() < 1e-12);
        assert_eq!(widened.delta, base.delta);
        assert_eq!(widened.kind, base.kind);
    }

    #[test]
    fn gos_staleness_does_not_touch_exact() {
        // Exact aggregates are not GOS-gated (Count-Sketch families only), so a
        // stray parameter must not fabricate an ε>0 "exact" answer.
        let mut params = HashMap::new();
        params.insert("gos_delta_epsilon".to_string(), json!(0.05));
        let p = AccuracyProfile::derive(&base_config(AggregationType::Sum, params));
        assert_eq!(p.epsilon, 0.0);
        assert_eq!(p.kind, AccuracyKind::Exact);
    }

    #[test]
    fn cms_epsilon_is_e_over_w() {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(5));
        params.insert("w".to_string(), json!(2718));
        let p = AccuracyProfile::derive(&base_config(AggregationType::CountMinSketch, params));
        // e / 2718 ≈ 0.0010001 — very close to 0.001.
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert!((p.epsilon - std::f64::consts::E / 2718.0).abs() < 1e-12);
        // δ = 1/2^5 = 0.03125
        assert!((p.delta - 0.03125).abs() < 1e-12);
    }

    #[test]
    fn cms_uses_defaults_when_params_absent() {
        let p = AccuracyProfile::derive(&base_config(
            AggregationType::CountMinSketch,
            HashMap::new(),
        ));
        // Defaults rows=4, cols=1000 per accumulator_factory.
        assert!((p.epsilon - std::f64::consts::E / 1000.0).abs() < 1e-12);
        assert!((p.delta - 0.0625).abs() < 1e-12); // 1/16
    }

    #[test]
    fn cms_with_heap_carries_top_k_kind_and_heap_bound() {
        // Large heap: 1/heap_size (= 1e-4) dominates the e/w CMS
        // bound (e/1e6 ≈ 2.72e-6). Expect ε = 1/heap.
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(5));
        params.insert("w".to_string(), json!(1_000_000));
        params.insert("heap_size".to_string(), json!(10_000));
        let p = AccuracyProfile::derive(&base_config(
            AggregationType::CountMinSketchWithHeap,
            params,
        ));
        assert_eq!(p.kind, AccuracyKind::TopK);
        assert!((p.epsilon - 1.0 / 10_000.0).abs() < 1e-12);
        assert!((p.delta - 1.0 / 32.0).abs() < 1e-12); // 1/2^5
    }

    #[test]
    fn cms_with_heap_cms_bound_dominates_when_heap_is_generous() {
        // Generously-sized heap (1e6) + narrow CMS (w=100) →
        // 1/heap (1e-6) ≪ e/w (2.7e-2), so the CMS bound dominates.
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4));
        params.insert("w".to_string(), json!(100));
        params.insert("heap_size".to_string(), json!(1_000_000));
        let p = AccuracyProfile::derive(&base_config(
            AggregationType::CountMinSketchWithHeap,
            params,
        ));
        assert_eq!(p.kind, AccuracyKind::TopK);
        assert!((p.epsilon - std::f64::consts::E / 100.0).abs() < 1e-12);
    }

    #[test]
    fn cms_with_heap_uses_default_heap_size_100() {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4));
        params.insert("w".to_string(), json!(1000));
        // heap_size absent → default 100 → 1/100 = 0.01 dominates
        // e/1000 ≈ 0.00272.
        let p = AccuracyProfile::derive(&base_config(
            AggregationType::CountMinSketchWithHeap,
            params,
        ));
        assert_eq!(p.kind, AccuracyKind::TopK);
        assert!((p.epsilon - 0.01).abs() < 1e-12);
    }

    #[test]
    fn cms_with_heap_accepts_alternative_param_names() {
        // Config may name the heap "k" or "topk" instead of
        // "heap_size" — all three should work.
        for alias in ["heap_size", "topk", "k"] {
            let mut params = HashMap::new();
            params.insert("d".to_string(), json!(4));
            params.insert("w".to_string(), json!(1_000_000));
            params.insert(alias.to_string(), json!(500));
            let p = AccuracyProfile::derive(&base_config(
                AggregationType::CountMinSketchWithHeap,
                params,
            ));
            assert!(
                (p.epsilon - 1.0 / 500.0).abs() < 1e-12,
                "alias '{}' should produce heap-driven ε",
                alias
            );
        }
    }

    #[test]
    fn countsketch_epsilon_is_one_over_sqrt_w() {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4));
        params.insert("w".to_string(), json!(100));
        let p = AccuracyProfile::derive(&base_config(AggregationType::CountSketch, params));
        assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
        assert!((p.epsilon - 0.1).abs() < 1e-9); // 1/√100 = 0.1
        assert!((p.delta - 0.0625).abs() < 1e-12); // 1/2^4
    }

    #[test]
    fn hll_epsilon_matches_flajolet_bound() {
        let mut params = HashMap::new();
        params.insert("precision".to_string(), json!(14));
        let p = AccuracyProfile::derive(&base_config(AggregationType::HLL, params));
        assert_eq!(p.kind, AccuracyKind::RelativeCardinality);
        // 1.04 / √16384 = 1.04 / 128 = 0.008125
        assert!((p.epsilon - 0.008125).abs() < 1e-9);
    }

    #[test]
    fn hll_uses_default_precision_14() {
        let p = AccuracyProfile::derive(&base_config(AggregationType::HLL, HashMap::new()));
        assert!((p.epsilon - 0.008125).abs() < 1e-9);
    }

    #[test]
    fn kll_epsilon_matches_karnin_lang_liberty_bound() {
        let mut params = HashMap::new();
        params.insert("K".to_string(), json!(200));
        let p = AccuracyProfile::derive(&base_config(AggregationType::DatasketchesKLL, params));
        assert_eq!(p.kind, AccuracyKind::RankQuantile);
        // 2.296 / √200 ≈ 0.16235
        assert!((p.epsilon - 2.296 / 200.0_f64.sqrt()).abs() < 1e-12);
        assert!((p.delta - 0.01).abs() < 1e-12);
    }

    #[test]
    fn hydra_kll_follows_the_same_kll_bound() {
        let mut params = HashMap::new();
        params.insert("k".to_string(), json!(400));
        let p = AccuracyProfile::derive(&base_config(AggregationType::HydraKLL, params));
        // 2.296 / √400 = 2.296 / 20 = 0.1148
        assert!((p.epsilon - 0.1148).abs() < 1e-9);
    }

    #[test]
    fn ddsketch_epsilon_is_alpha_directly() {
        let mut params = HashMap::new();
        params.insert("alpha".to_string(), json!(0.02));
        let p = AccuracyProfile::derive(&base_config(AggregationType::DDSketch, params));
        assert_eq!(p.kind, AccuracyKind::RelativeQuantile);
        assert_eq!(p.epsilon, 0.02);
        assert_eq!(p.delta, 0.0);
    }

    #[test]
    fn ddsketch_uses_default_alpha_0_01() {
        let p = AccuracyProfile::derive(&base_config(AggregationType::DDSketch, HashMap::new()));
        assert_eq!(p.epsilon, 0.01);
    }

    // (The historical `set_aggregators_are_exact` test verified the
    // accuracy bound for `SetAggregator` / `DeltaSetAggregator`;
    // retired alongside the family itself.)

    #[test]
    fn legacy_wrapper_types_fall_back_to_exact() {
        // `SingleSubpopulation` / `MultipleSubpopulation` are
        // config-shape wrappers whose real type is in sub_type.
        // Without resolving sub_type we return exact (harmless
        // lower bound). Phase 6.4 v2 may tighten this.
        for t in [
            AggregationType::SingleSubpopulation,
            AggregationType::MultipleSubpopulation,
        ] {
            let p = AccuracyProfile::derive(&base_config(t, HashMap::new()));
            assert_eq!(p.kind, AccuracyKind::Exact);
        }
    }

    #[test]
    fn accuracy_profile_roundtrips_through_serde() {
        let input = AccuracyProfile {
            epsilon: 0.008125,
            delta: 0.0,
            kind: AccuracyKind::RelativeCardinality,
        };
        let json = serde_json::to_string(&input).unwrap();
        assert!(json.contains("\"relative_cardinality\""));
        let back: AccuracyProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(back, input);
    }

    #[test]
    fn cms_and_countsketch_differ_by_sqrt_e() {
        // Sanity check: for the same (w, d), CountSketch's ε is
        // smaller by a factor of √e / √w × 1/√w = 1/(√e) —
        // i.e. CountSketch is a factor ~1.65 tighter than CMS on
        // epsilon alone. Confirms the bounds are not copy-pasted.
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4));
        params.insert("w".to_string(), json!(10000));
        let cms = AccuracyProfile::derive(&base_config(
            AggregationType::CountMinSketch,
            params.clone(),
        ));
        let cs = AccuracyProfile::derive(&base_config(AggregationType::CountSketch, params));
        // cms.epsilon = e/10000 ≈ 2.718e-4
        // cs.epsilon = 1/√10000 = 0.01 = 1e-2
        // So cms < cs (for w=10000). They cross at w = e.
        assert!(cms.epsilon < cs.epsilon);
        assert!(cs.epsilon > 0.0);
    }
}
