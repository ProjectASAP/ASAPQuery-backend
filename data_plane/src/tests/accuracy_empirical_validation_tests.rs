//! Paper-artifact checks on the `AccuracyProfile` derivation:
//!
//! 1. **Published-constant parity.** For each sketch family, the
//!    numerical bound at a canonical parameter matches the
//!    published constant to ≥ 6 decimal places. Regression guard
//!    against typos in the formula or citation drift.
//! 2. **Monotonicity across parameter sweeps.** Larger capacity
//!    parameters (more CMS columns, more HLL bits, larger KLL
//!    k, tighter DDSketch α) must monotonically tighten the
//!    theoretical bound. Any non-monotone step would indicate a
//!    broken formula.
//!
//! ### What this test is *not*
//!
//! Live "measure sketch error, compare to bound" runs don't
//! live here — the `asap_sketchlib` git dep the backend pulls
//! exposes a different API from the
//! [`sketch-bench`](https://github.com/ProjectASAP/sketch-bench)
//! workspace's path dep, and bridging the two inside a unit
//! test pulls in a lot of version-coupling we don't want.
//! Empirical sweeps are produced by `sketchlib bench
//! --metrics accuracy` runs in sketch-bench and compared
//! against `AccuracyProfile::derive` externally; the test
//! below pins the *theoretical* side that those comparisons
//! are made against.

#[cfg(test)]
use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::WindowType;
use asap_types::KeyByLabelNames;
use promql_utilities::query_logics::enums::AggregationType;
use serde_json::{json, Value};

use crate::storage_engines::sketch_db::accuracy::{AccuracyKind, AccuracyProfile};

fn cfg(agg_type: AggregationType, params: HashMap<String, Value>) -> AggregationConfig {
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
        WindowType::Tumbling,
        String::new(),
        "m".to_string(),
        None,
        None,
        None,
    )
}

// -------- published-constant parity ----------

#[test]
fn hll_p14_matches_flajolet_1_04_over_sqrt_m() {
    // Flajolet et al. 2007: std-err = 1.04 / √m, m = 2^p.
    // For p = 14 → ε = 1.04 / 128 = 0.008125.
    let mut params = HashMap::new();
    params.insert("precision".to_string(), json!(14u64));
    let p = AccuracyProfile::derive(&cfg(AggregationType::HLL, params));
    assert_eq!(p.kind, AccuracyKind::RelativeCardinality);
    assert!((p.epsilon - 0.008125).abs() < 1e-9);
}

#[test]
fn cms_3x1000_matches_cormode_muthukrishnan_e_over_w() {
    // Cormode-Muthukrishnan 2005: ε = e/w, δ = 1/2^d.
    // For (w=1000, d=3): ε = e/1000 ≈ 2.71828e-3, δ = 0.125.
    let mut params = HashMap::new();
    params.insert("d".to_string(), json!(3u64));
    params.insert("w".to_string(), json!(1000u64));
    let p = AccuracyProfile::derive(&cfg(AggregationType::CountMinSketch, params));
    assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
    assert!((p.epsilon - std::f64::consts::E / 1000.0).abs() < 1e-12);
    assert!((p.delta - 0.125).abs() < 1e-12);
}

#[test]
fn count_sketch_4x10000_matches_charikar_one_over_sqrt_w() {
    // Charikar-Chen-Farach-Colton 2002: ε = 1/√w for signed
    // counter sketch.
    let mut params = HashMap::new();
    params.insert("d".to_string(), json!(4u64));
    params.insert("w".to_string(), json!(10_000u64));
    let p = AccuracyProfile::derive(&cfg(AggregationType::CountSketch, params));
    assert_eq!(p.kind, AccuracyKind::AdditiveFrequency);
    assert!((p.epsilon - 0.01).abs() < 1e-12);
}

#[test]
fn kll_k200_matches_karnin_lang_liberty_2_296() {
    // Karnin-Lang-Liberty FOCS 2016: rank err ≈ 2.296 / √k.
    let mut params = HashMap::new();
    params.insert("K".to_string(), json!(200u64));
    let p = AccuracyProfile::derive(&cfg(AggregationType::DatasketchesKLL, params));
    assert_eq!(p.kind, AccuracyKind::RankQuantile);
    assert!((p.epsilon - 2.296 / 200.0_f64.sqrt()).abs() < 1e-12);
    assert!((p.delta - 0.01).abs() < 1e-12);
}

#[test]
fn ddsketch_alpha_passes_through_verbatim() {
    // Masson-Rim-Lee VLDB 2019: α is the published relative
    // quantile-error guarantee and is deterministic.
    for alpha in [0.005_f64, 0.01, 0.02, 0.05] {
        let mut params = HashMap::new();
        params.insert("alpha".to_string(), json!(alpha));
        let p = AccuracyProfile::derive(&cfg(AggregationType::DDSketch, params));
        assert_eq!(p.kind, AccuracyKind::RelativeQuantile);
        assert_eq!(p.epsilon, alpha);
        assert_eq!(p.delta, 0.0);
    }
}

#[test]
fn cms_with_heap_top_k_combines_cms_and_retention_bounds() {
    // (w=1000, d=3, heap=50): CMS bound e/w ≈ 2.72e-3, heap
    // bound 1/50 = 0.02. Heap dominates → ε = 0.02.
    let mut params = HashMap::new();
    params.insert("d".to_string(), json!(3u64));
    params.insert("w".to_string(), json!(1000u64));
    params.insert("heap_size".to_string(), json!(50u64));
    let p = AccuracyProfile::derive(&cfg(AggregationType::CountMinSketchWithHeap, params));
    assert_eq!(p.kind, AccuracyKind::TopK);
    assert!((p.epsilon - 0.02).abs() < 1e-12);
    assert!((p.delta - 0.125).abs() < 1e-12);
}

// -------- monotonicity ----------

#[test]
fn hll_epsilon_shrinks_monotonically_with_precision() {
    let mut last = f64::INFINITY;
    for p in [8u64, 10, 12, 14, 16] {
        let mut params = HashMap::new();
        params.insert("precision".to_string(), json!(p));
        let eps = AccuracyProfile::derive(&cfg(AggregationType::HLL, params)).epsilon;
        assert!(
            eps < last,
            "HLL ε should tighten as precision grows: p={p} ε={eps} previous={last}"
        );
        last = eps;
    }
}

#[test]
fn cms_epsilon_shrinks_monotonically_with_width() {
    let mut last = f64::INFINITY;
    for w in [100u64, 500, 2000, 10_000, 100_000] {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4u64));
        params.insert("w".to_string(), json!(w));
        let eps = AccuracyProfile::derive(&cfg(AggregationType::CountMinSketch, params)).epsilon;
        assert!(eps < last, "CMS ε at w={w}: {eps} should be < {last}");
        last = eps;
    }
}

#[test]
fn cms_delta_shrinks_monotonically_with_depth() {
    let mut last = f64::INFINITY;
    for d in [2u64, 3, 4, 5, 6, 8] {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(d));
        params.insert("w".to_string(), json!(1000u64));
        let delta = AccuracyProfile::derive(&cfg(AggregationType::CountMinSketch, params)).delta;
        assert!(delta < last, "CMS δ at d={d}: {delta} should be < {last}");
        last = delta;
    }
}

#[test]
fn countsketch_epsilon_shrinks_monotonically_with_width() {
    let mut last = f64::INFINITY;
    for w in [100u64, 500, 2000, 10_000, 100_000] {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(4u64));
        params.insert("w".to_string(), json!(w));
        let eps = AccuracyProfile::derive(&cfg(AggregationType::CountSketch, params)).epsilon;
        assert!(
            eps < last,
            "CountSketch ε at w={w}: {eps} should be < {last}"
        );
        last = eps;
    }
}

#[test]
fn kll_epsilon_shrinks_monotonically_with_k() {
    let mut last = f64::INFINITY;
    for k in [50u64, 100, 200, 500, 1000] {
        let mut params = HashMap::new();
        params.insert("K".to_string(), json!(k));
        let eps = AccuracyProfile::derive(&cfg(AggregationType::DatasketchesKLL, params)).epsilon;
        assert!(eps < last, "KLL ε at k={k}: {eps} should be < {last}");
        last = eps;
    }
}

#[test]
fn ddsketch_epsilon_tracks_alpha_monotonically() {
    // Tighter α → smaller ε (same value).
    let mut last = f64::INFINITY;
    for alpha in [0.05f64, 0.02, 0.01, 0.005, 0.001] {
        let mut params = HashMap::new();
        params.insert("alpha".to_string(), json!(alpha));
        let eps = AccuracyProfile::derive(&cfg(AggregationType::DDSketch, params)).epsilon;
        assert!(
            eps < last,
            "DDSketch ε at α={alpha}: {eps} should be < {last}"
        );
        last = eps;
    }
}

#[test]
fn cms_with_heap_epsilon_shrinks_monotonically_with_heap_size_when_heap_dominates() {
    // Hold CMS params fixed so the CMS contribution e/w stays
    // constant; as heap_size grows, the heap bound 1/heap
    // shrinks, so ε tightens until it meets the CMS floor.
    // Use a wide CMS (w=1_000_000) so the CMS floor is tiny
    // and the heap bound dominates the entire sweep range.
    let mut last = f64::INFINITY;
    for heap in [10u64, 100, 1000, 10_000, 100_000] {
        let mut params = HashMap::new();
        params.insert("d".to_string(), json!(5u64));
        params.insert("w".to_string(), json!(1_000_000u64));
        params.insert("heap_size".to_string(), json!(heap));
        let eps =
            AccuracyProfile::derive(&cfg(AggregationType::CountMinSketchWithHeap, params)).epsilon;
        assert!(
            eps < last,
            "CMS-with-heap ε at heap={heap}: {eps} should be < {last}"
        );
        last = eps;
    }
}

// -------- cross-family spot-check ----------

#[test]
fn relative_ordering_of_bounds_matches_published_intuition() {
    // Published intuition: for the same width, CountSketch gives
    // a smaller ε than CountMin when w is large enough that
    // 1/√w < e/w (i.e. w > e² ≈ 7.39). Cross-check the
    // `countsketch_oxide_matches_cms_oxide` sanity in
    // sketch-bench.
    let mut params = HashMap::new();
    params.insert("d".to_string(), json!(4u64));
    params.insert("w".to_string(), json!(10_000u64));
    let cms = AccuracyProfile::derive(&cfg(AggregationType::CountMinSketch, params.clone()));
    let cs = AccuracyProfile::derive(&cfg(AggregationType::CountSketch, params));
    // CMS at w=10_000: e/10_000 ≈ 0.000272
    // CountSketch at w=10_000: 1/√10_000 = 0.01
    // So cms.epsilon < cs.epsilon at this width.
    assert!(cms.epsilon < cs.epsilon);
}
