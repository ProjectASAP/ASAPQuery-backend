//! Capability matrix — which (sketch, statistic) pairs are valid.
//!
//! The shared MVP demo contract pins a per-metric family-per-statistic
//! mapping (issue #46):
//!
//! | metric                  | family       | query class           |
//! |-------------------------|--------------|-----------------------|
//! | `http_requests_total`   | raw          | sum / rate / count    |
//! | `http_latency_ms`       | DDSketch     | quantile (rel-err)    |
//! | `request_size_bytes`    | KLL          | quantile (rank-err)   |
//! | `unique_users_per_min`  | HLL          | cardinality           |
//! | `top_endpoint_qps`      | CountSketch  | top-K                 |
//! | `endpoint_request_freq` | CountMinSketch (CMS) | frequency     |
//!
//! Note that CountMinSketch ALSO supports top-K via the CMS-Heap pattern
//! (Cormode & Muthukrishnan, 2005 — "An Improved Data Stream Summary: The
//! Count-Min Sketch and its Applications"). The capability matrix below
//! reflects this: CMS validly answers Frequency *and* TopK. CountSketch
//! remains the canonical (unbiased) TopK pick — `pick_family` prefers it
//! when no override is supplied — but a workload's
//! `sketch_family_override` may still pin CountMin for a TopK metric.
//!
//! Backend gap: declaring CMS-supports-TopK here is a planner-side concern.
//! The backend's actual "top-K from CountMin state" query path (the heap
//! readout) is a separate workstream and is not yet implemented in
//! `ASAPQuery-backend`. Until that lands, a planner-pinned CMS-for-TopK
//! binding will produce a sketch the backend cannot extract heavy hitters
//! from. Keep this caveat in mind when reviewing override-driven plans.
//!
//! The asap-common docstring (`asap-common/dependencies/rs/asap_types/src/
//! capability_matching.rs` per the orchestrator spec) keeps this matrix
//! as the single source of truth so the planner's `bind_workload_typed`
//! and the L4 `Bind*` rule dispatcher agree on which family to pick.
//!
//! There are two perspectives the matrix has to satisfy:
//!
//! 1. **Static.** `(sketch, statistic)` is a *valid* binding — a CMS can
//!    answer Frequency, but an HLL can not. Used by the Phase β catalog
//!    check (`is_valid_pair`) and the `Bind*` rule guards.
//! 2. **Dynamic.** Given a `StatisticClass` + `AccuracyPreference`, pick
//!    the *preferred* sketch family. The MVP picks DDSketch when the
//!    preference is relative-error and KLL when it's rank-error; the rest
//!    of the catalog is one-to-one.
//!
//! The asap-common path referenced in the orchestrator spec doesn't exist
//! in this monorepo today (the controller crate is the only Rust consumer
//! of the catalog), so the canonical home is `controller/src/sketch_algebra/
//! capability_matching.rs`. When asap-common ships as a separate Cargo
//! crate, this module lifts there verbatim.

#![allow(dead_code)]

use crate::sketch_algebra::params::SketchKind;

/// Query intent the user is expressing — abstracted away from the L1
/// language (PromQL `quantile_over_time`, SQL `PERCENTILE_CONT`, etc.) and
/// from the L4 sketch family. Each variant enumerates one entry in the
/// MVP-contract row.
///
/// Phase β scope: the variants below are the six classes the demo
/// workload exercises. Adding a class (e.g. `Histogram`) is purely
/// additive: extend [`StatisticClass`], extend [`valid_pair`] /
/// [`pick_family`], add a row to the matrix tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatisticClass {
    /// `quantile(φ, x)` — DDSketch (relative-error) and KLL (rank-error)
    /// are both valid; the [`AccuracyPreference`] picks which one.
    Quantile,
    /// `count_distinct(x)` — only HLL is valid.
    Cardinality,
    /// `topk(k, x)` — CountSketch (with-heap) is the canonical pick, but
    /// CountMinSketch (with-heap) is *also* valid via the CMS-Heap pattern
    /// (Cormode & Muthukrishnan 2005). Misra-Gries / SpaceSaving would
    /// extend the matrix but are not part of Phase β.
    TopK,
    /// `freq(x = k)` — only CMS (CountMinSketch) is valid in the MVP
    /// catalog. CountSketch could in principle answer it but the
    /// MVP-contract row pins CMS.
    Frequency,
    /// `sum(x) / rate(x[w]) / count(x)` — no sketch needed; the agent
    /// emits raw OTLP and the backend / Prometheus computes the answer
    /// directly. Maps to "raw passthrough" in the contract.
    SumRateCount,
}

impl StatisticClass {
    /// Stable, human-readable identifier (`"quantile"`, …) — used for
    /// diagnostics + the `cargo test` matrix.
    pub fn as_str(&self) -> &'static str {
        match self {
            StatisticClass::Quantile => "quantile",
            StatisticClass::Cardinality => "cardinality",
            StatisticClass::TopK => "topk",
            StatisticClass::Frequency => "frequency",
            StatisticClass::SumRateCount => "sum_rate_count",
        }
    }
}

/// Tie-breaker preference for [`StatisticClass::Quantile`] — relative-error
/// (DDSketch) vs rank-error (KLL). Other statistic classes ignore this
/// field today (each maps to exactly one family).
///
/// Default is [`AccuracyPreference::RelativeError`] — matches the legacy
/// `algebra::directory::sketch_type_for_agg` default of DDSketch for
/// Quantile workloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AccuracyPreference {
    /// DDSketch — `|estimate − true| ≤ alpha · true`. The right pick when
    /// the user cares about tail-relative error (e.g. p99 latency in
    /// milliseconds, where 1ms vs 1s should be 1% off, not "rank 99%").
    #[default]
    RelativeError,
    /// KLL — rank-error bound (φ̂ within ε of φ in CDF space). The right
    /// pick when the user cares about rank-stability (e.g. byte-size
    /// distribution where the absolute value range is many orders of
    /// magnitude and rank-distance is the meaningful metric).
    RankError,
}

/// Whether `(sketch, statistic)` is a valid pair in the MVP catalog.
///
/// The capability matrix:
///
/// | sketch       | Quantile | Cardinality | TopK | Frequency | SumRateCount |
/// |--------------|----------|-------------|------|-----------|--------------|
/// | DDSketch     | yes      | no          | no   | no        | no           |
/// | KLL          | yes      | no          | no   | no        | no           |
/// | HLL          | no       | yes         | no   | no        | no           |
/// | CountSketch  | no       | no          | yes  | no        | no           |
/// | CMS          | no       | no          | yes  | yes       | no           |
///
/// `SumRateCount` has no valid sketch — the agent emits raw OTLP for
/// those statistic classes (see [`pick_family`]).
///
/// CMS gains TopK validity via the CMS-Heap pattern (Cormode &
/// Muthukrishnan 2005). NB: this is the planner-side capability
/// declaration; the backend's "top-K from CountMin state" readout path
/// is a separate workstream — see the module-level docs.
pub fn is_valid_pair(sketch: SketchKind, statistic: StatisticClass) -> bool {
    use SketchKind::*;
    use StatisticClass::*;
    match (sketch, statistic) {
        (DDSketch, Quantile)
        | (Kll, Quantile)
        | (Hll, Cardinality)
        | (CountSketch, TopK)
        | (Cms, TopK)
        | (Cms, Frequency) => true,
        _ => false,
    }
}

/// Pick the preferred sketch family for a `(StatisticClass, AccuracyPreference)`
/// pair. Returns `None` for [`StatisticClass::SumRateCount`] — that class
/// uses raw passthrough, no sketch needed.
///
/// The mapping:
///
/// - Quantile + RelativeError → DDSketch
/// - Quantile + RankError → KLL
/// - Cardinality → HLL
/// - TopK → CountSketch
/// - Frequency → CMS (CountMinSketch)
/// - SumRateCount → None (raw passthrough)
///
/// `sketch_family_override` (treated as `QueryWorkload::sketch_type_override`
/// at the planner-rules layer) wins over the capability-matched default —
/// see `planner::rules::bind_workload_typed`.
pub fn pick_family(statistic: StatisticClass, accuracy: AccuracyPreference) -> Option<SketchKind> {
    use AccuracyPreference::*;
    use SketchKind::*;
    use StatisticClass::*;
    let kind = match (statistic, accuracy) {
        (Quantile, RelativeError) => DDSketch,
        (Quantile, RankError) => Kll,
        (Cardinality, _) => Hll,
        (TopK, _) => CountSketch,
        (Frequency, _) => Cms,
        (SumRateCount, _) => return None,
    };
    debug_assert!(
        is_valid_pair(kind.clone(), statistic),
        "pick_family produced an invalid (sketch={kind:?}, stat={statistic:?}) pair"
    );
    Some(kind)
}

/// Heuristic mapping from the MVP-demo metric names in the shared contract
/// (issue #46) to a `(StatisticClass, AccuracyPreference)` pair. When the
/// metric name matches one of the six contract rows, this is the canonical
/// classification; otherwise [`None`] is returned and the caller falls
/// back to its `AggType`-driven default.
///
/// The name-matching is exact (case-sensitive) to keep the contract row
/// the single source of truth — a typo'd metric name should fall through
/// to the AggType default rather than silently bind to the wrong family.
pub fn classify_demo_metric(metric_name: &str) -> Option<(StatisticClass, AccuracyPreference)> {
    use AccuracyPreference::*;
    use StatisticClass::*;
    Some(match metric_name {
        "http_requests_total" => (SumRateCount, RelativeError),
        "http_latency_ms" => (Quantile, RelativeError),
        "request_size_bytes" => (Quantile, RankError),
        "unique_users_per_min" => (Cardinality, RelativeError),
        "top_endpoint_qps" => (TopK, RelativeError),
        "endpoint_request_freq" => (Frequency, RelativeError),
        _ => return None,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── (sketch, statistic) capability matrix ─────────────────────────────────

    #[test]
    fn ddsketch_is_quantile_only() {
        assert!(is_valid_pair(
            SketchKind::DDSketch,
            StatisticClass::Quantile
        ));
        assert!(!is_valid_pair(
            SketchKind::DDSketch,
            StatisticClass::Cardinality
        ));
        assert!(!is_valid_pair(SketchKind::DDSketch, StatisticClass::TopK));
        assert!(!is_valid_pair(
            SketchKind::DDSketch,
            StatisticClass::Frequency
        ));
        assert!(!is_valid_pair(
            SketchKind::DDSketch,
            StatisticClass::SumRateCount
        ));
    }

    #[test]
    fn kll_is_quantile_only() {
        assert!(is_valid_pair(SketchKind::Kll, StatisticClass::Quantile));
        assert!(!is_valid_pair(SketchKind::Kll, StatisticClass::Cardinality));
        assert!(!is_valid_pair(SketchKind::Kll, StatisticClass::TopK));
        assert!(!is_valid_pair(SketchKind::Kll, StatisticClass::Frequency));
        assert!(!is_valid_pair(
            SketchKind::Kll,
            StatisticClass::SumRateCount
        ));
    }

    #[test]
    fn hll_is_cardinality_only() {
        assert!(is_valid_pair(SketchKind::Hll, StatisticClass::Cardinality));
        assert!(!is_valid_pair(SketchKind::Hll, StatisticClass::Quantile));
        assert!(!is_valid_pair(SketchKind::Hll, StatisticClass::TopK));
        assert!(!is_valid_pair(SketchKind::Hll, StatisticClass::Frequency));
        assert!(!is_valid_pair(
            SketchKind::Hll,
            StatisticClass::SumRateCount
        ));
    }

    #[test]
    fn countsketch_is_topk_only() {
        assert!(is_valid_pair(SketchKind::CountSketch, StatisticClass::TopK));
        assert!(!is_valid_pair(
            SketchKind::CountSketch,
            StatisticClass::Quantile
        ));
        assert!(!is_valid_pair(
            SketchKind::CountSketch,
            StatisticClass::Cardinality
        ));
        assert!(!is_valid_pair(
            SketchKind::CountSketch,
            StatisticClass::Frequency
        ));
        assert!(!is_valid_pair(
            SketchKind::CountSketch,
            StatisticClass::SumRateCount
        ));
    }

    #[test]
    fn cms_supports_frequency_and_topk() {
        // CMS validly answers Frequency (point-frequency, additive bound)
        // AND TopK via the CMS-Heap pattern (Cormode & Muthukrishnan 2005).
        assert!(is_valid_pair(SketchKind::Cms, StatisticClass::Frequency));
        assert!(is_valid_pair(SketchKind::Cms, StatisticClass::TopK));
        assert!(!is_valid_pair(SketchKind::Cms, StatisticClass::Quantile));
        assert!(!is_valid_pair(SketchKind::Cms, StatisticClass::Cardinality));
        assert!(!is_valid_pair(
            SketchKind::Cms,
            StatisticClass::SumRateCount
        ));
    }

    #[test]
    fn countmin_supports_topk_capability() {
        // Pin the new matrix entry: CMS validly answers TopK. The
        // canonical pick remains CountSketch — see
        // `pick_family_topk_picks_countsketch` — but the catalog now
        // accepts a `sketch_family_override: CountMinSketch` for a
        // TopK-shaped workload (CMS-Heap pattern, Cormode &
        // Muthukrishnan 2005).
        assert!(
            is_valid_pair(SketchKind::Cms, StatisticClass::TopK),
            "CMS should support TopK via the CMS-Heap pattern",
        );
    }

    // ── pick_family — capability-matched defaults ─────────────────────────────

    #[test]
    fn pick_family_quantile_relative_picks_ddsketch() {
        assert_eq!(
            pick_family(StatisticClass::Quantile, AccuracyPreference::RelativeError),
            Some(SketchKind::DDSketch),
        );
    }

    #[test]
    fn pick_family_quantile_rank_picks_kll() {
        assert_eq!(
            pick_family(StatisticClass::Quantile, AccuracyPreference::RankError),
            Some(SketchKind::Kll),
        );
    }

    #[test]
    fn pick_family_cardinality_picks_hll() {
        for pref in [
            AccuracyPreference::RelativeError,
            AccuracyPreference::RankError,
        ] {
            assert_eq!(
                pick_family(StatisticClass::Cardinality, pref),
                Some(SketchKind::Hll),
            );
        }
    }

    #[test]
    fn pick_family_topk_picks_countsketch() {
        assert_eq!(
            pick_family(StatisticClass::TopK, AccuracyPreference::default()),
            Some(SketchKind::CountSketch),
        );
    }

    #[test]
    fn pick_family_frequency_picks_cms() {
        assert_eq!(
            pick_family(StatisticClass::Frequency, AccuracyPreference::default()),
            Some(SketchKind::Cms),
        );
    }

    #[test]
    fn pick_family_sum_rate_count_is_raw_passthrough() {
        assert_eq!(
            pick_family(StatisticClass::SumRateCount, AccuracyPreference::default()),
            None,
            "SumRateCount must produce no sketch (raw passthrough)",
        );
    }

    // ── classify_demo_metric — every contract row ─────────────────────────────

    #[test]
    fn classify_demo_metric_six_contract_rows() {
        let cases = [
            (
                "http_requests_total",
                StatisticClass::SumRateCount,
                AccuracyPreference::RelativeError,
            ),
            (
                "http_latency_ms",
                StatisticClass::Quantile,
                AccuracyPreference::RelativeError,
            ),
            (
                "request_size_bytes",
                StatisticClass::Quantile,
                AccuracyPreference::RankError,
            ),
            (
                "unique_users_per_min",
                StatisticClass::Cardinality,
                AccuracyPreference::RelativeError,
            ),
            (
                "top_endpoint_qps",
                StatisticClass::TopK,
                AccuracyPreference::RelativeError,
            ),
            (
                "endpoint_request_freq",
                StatisticClass::Frequency,
                AccuracyPreference::RelativeError,
            ),
        ];
        for (metric, want_class, want_pref) in cases {
            assert_eq!(
                classify_demo_metric(metric),
                Some((want_class, want_pref)),
                "metric {metric} should classify to ({want_class:?}, {want_pref:?})",
            );
        }
    }

    #[test]
    fn classify_demo_metric_unknown_returns_none() {
        assert_eq!(classify_demo_metric("foo_bar_baz"), None);
        // Case-sensitive — typo'd capitalisation must NOT silently match.
        assert_eq!(classify_demo_metric("HTTP_LATENCY_MS"), None);
        assert_eq!(classify_demo_metric(""), None);
    }

    // ── End-to-end: every contract row maps to its expected SketchKind ────────

    #[test]
    fn every_contract_metric_picks_its_contract_family() {
        let cases = [
            ("http_requests_total", None),
            ("http_latency_ms", Some(SketchKind::DDSketch)),
            ("request_size_bytes", Some(SketchKind::Kll)),
            ("unique_users_per_min", Some(SketchKind::Hll)),
            ("top_endpoint_qps", Some(SketchKind::CountSketch)),
            ("endpoint_request_freq", Some(SketchKind::Cms)),
        ];
        for (metric, want_kind) in cases {
            let (stat, pref) = classify_demo_metric(metric)
                .unwrap_or_else(|| panic!("contract metric {metric} must classify"));
            let got = pick_family(stat, pref);
            assert_eq!(
                got, want_kind,
                "metric {metric}: expected family {want_kind:?}, got {got:?}",
            );
        }
    }
}
