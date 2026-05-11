//! Single source of truth for capability state.
//!
//! Step 2a of the architectural refactor consolidates the four overlapping
//! capability tables that previously existed in the controller — the YAML
//! at `controller/sketch_capabilities.yml`, the compiled-in defaults in
//! `algebra/optimizer.rs::sketch_capability`, the `SketchKind` enum in
//! `sketch_algebra/params.rs`, and the per-query `Capability` /
//! `SketchKindHandle` invented inside `warm_tier_analysis.rs` (PR #128).
//! All four collapse into this module:
//!
//! - [`SketchCapability`] / [`SupportedIntent`] — per-sketch performance
//!   profile (insert / memory / CPU / transmission costs + the logical
//!   intents the sketch can serve). Read by `algebra/optimizer.rs` for
//!   cost-based plan rewriting and by `algebra/physical.rs` for stage
//!   placement.
//! - [`Capability`] / [`SketchKindHandle`] — query-side capability tag,
//!   used by the warm-tier reducer in `asap-query-engine` to dispatch
//!   PromQL → per-Capability sketch evaluation.
//! - [`capability_for`] — the **semantic** intent → warm-tier dispatch
//!   bridge. PromQL → intent_algebra::lower → `AggIntent` → (this fn) →
//!   `Capability`. The warm-tier analyzer is now a thin facade around
//!   this single function; PromQL function-name string matching lives
//!   only inside the lowerer.
//! - [`default_capability_table`] / [`load_capability_overrides`] —
//!   compiled-in defaults + YAML override loader. Replaces the
//!   `sketch_capability()` and `load_sketch_capabilities()` functions
//!   that previously lived in `algebra/optimizer.rs`.
//!
//! ## Why one module
//!
//! Before Step 2a, "what can a sketch do" was duplicated four times.
//! Adding a new sketch family meant touching `params.rs`,
//! `optimizer.rs`, the YAML, and `warm_tier_analysis.rs`. After Step 2a
//! every capability fact has exactly one home — `params.rs` declares
//! the sketch families, this module declares everything else.

#![allow(dead_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::intent_algebra::agg_intent::AggIntent;
use crate::sketch_algebra::params::SketchKind;
use crate::types_v2::AccuracyTarget;

// ── Query-side capability tag ────────────────────────────────────────────────

/// Warm-tier capability tag. One variant per logical query family the
/// warm tier can answer. The inner [`SketchKindHandle`] is the
/// implementation choice (e.g. DDSketch vs KLL for `QuantileApprox`).
/// Query routing keys on the variant, not the implementation, so two
/// CMS instances and one CountSketch instance for the same metric-and-
/// group-by all map to `FrequencyTopk` and the query path picks any
/// of them.
///
/// Used by both the controller (via [`capability_for`] in the warm-tier
/// analyzer) and the `asap-query-engine` backend (re-exported as the
/// `sketch_index::Capability` it indexes sketch instances under). One
/// canonical definition; the backend re-exports rather than duplicating.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Approximate quantile via DDSketch / KLL / t-digest. The handle's
    /// `Any` variant means "any quantile-family sketch satisfies"; a
    /// concrete handle means "must be exactly this family".
    QuantileApprox(SketchKindHandle),
    /// Approximate cardinality via HLL / theta-sketch / linear-counting.
    /// No inner handle — cardinality has a single canonical family
    /// today (HLL).
    CardinalityApprox,
    /// Heavy-hitter top-k via CMS-with-heap (or CountSketch + heap).
    /// `CmsWithHeap` is the canonical handle today; the
    /// `Any` variant is unused for top-k because the wire format
    /// distinguishes the heap-bearing variant from raw CMS at ingest
    /// time.
    FrequencyTopk(SketchKindHandle),
}

/// Compact, hashable handle for sketch implementation choice. Mirrors
/// [`SketchKind`] but adds the `CmsWithHeap` and `Any` query-side
/// concepts (which aren't sketch families, they're dispatch hints).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchKindHandle {
    DDSketch,
    Kll,
    Hll,
    CountSketch,
    CountMin,
    /// CMS paired with a Misra-Gries / heavy-hitter heap. Distinct from
    /// `CountMin` because vanilla CMS carries no item universe — the
    /// heap is what lets the warm-tier reducer enumerate top-k items
    /// without an external item list.
    CmsWithHeap,
    /// "Any implementation that satisfies the family". Analysis-time
    /// wildcard, never indexed against a concrete sketch instance.
    /// Consumed by [`Capability::is_satisfied_by`].
    Any,
}

impl Capability {
    /// True when an indexed sketch instance's capability satisfies the
    /// query's required capability. `SketchKindHandle::Any` on the
    /// query side is a wildcard that matches any concrete handle in
    /// the same family.
    ///
    /// `self` is the **required** capability (from the analyzer);
    /// `indexed` is the **available** capability (from the sketch
    /// index). The backend's warm-tier hook reads both and routes the
    /// query to whichever sids satisfy.
    pub fn is_satisfied_by(&self, indexed: &Capability) -> bool {
        match (self, indexed) {
            // Quantile family: Any matches any concrete handle; concrete
            // handles must match exactly.
            (Capability::QuantileApprox(req), Capability::QuantileApprox(have)) => {
                handles_compatible(*req, *have)
            }
            // Cardinality has no inner handle; family match is total.
            (Capability::CardinalityApprox, Capability::CardinalityApprox) => true,
            // Top-k family: same Any / concrete-match semantics as
            // quantile.
            (Capability::FrequencyTopk(req), Capability::FrequencyTopk(have)) => {
                handles_compatible(*req, *have)
            }
            _ => false,
        }
    }
}

/// True when the required handle is `Any` (wildcard) or matches the
/// available handle exactly. Used by [`Capability::is_satisfied_by`].
fn handles_compatible(required: SketchKindHandle, available: SketchKindHandle) -> bool {
    matches!(required, SketchKindHandle::Any) || required == available
}

// ── AggIntent → Capability bridge ────────────────────────────────────────────

/// Map a semantic [`AggIntent`] to the warm-tier [`Capability`] that can
/// answer it. Returns `None` for intents that have no warm-tier sketch
/// (Sum / Min / Max / Avg / Rate / Increase / every archive-only intent
/// — see [`AggIntent::archive_only`]).
///
/// This is the **single bridge** between the L3 intent vocabulary and
/// the L4/Q1 sketch-capability vocabulary. Both the warm-tier analyzer
/// and the optimizer's binding rules read it. PromQL function-name
/// string matching does NOT happen here — it happens in the lowerer
/// (`intent_algebra::lower::lower_parsed_query`), which is the single
/// owner of "what does this PromQL function mean".
///
/// ## Mapping table
///
/// | `AggIntent` variant | Returns |
/// |---|---|
/// | `Quantile { q, accuracy }` (accuracy not `Exact`) | `Some(QuantileApprox(Any))` |
/// | `Quantile { q, accuracy: Exact }` | `None` (exact must use HashAgg/SortAgg) |
/// | `Cardinality { accuracy }` (accuracy not `Exact`) | `Some(CardinalityApprox)` |
/// | `Cardinality { accuracy: Exact }` | `None` |
/// | `Count { accuracy }` (same logic as Cardinality) | `Some(CardinalityApprox)` / `None` |
/// | `TopK { k, accuracy }` | `Some(FrequencyTopk(CmsWithHeap))` |
/// | `Frequency { accuracy }` (accuracy not `Exact`) | `Some(FrequencyTopk(CmsWithHeap))` |
/// | `Sum` / `Min` / `Max` / `Avg` / `Rate` / `Increase` | `None` |
/// | Every archive-only intent | `None` |
pub fn capability_for(intent: &AggIntent) -> Option<Capability> {
    match intent {
        AggIntent::Quantile { accuracy, .. } => {
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::QuantileApprox(SketchKindHandle::Any))
            }
        }
        AggIntent::Cardinality { accuracy } => {
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::CardinalityApprox)
            }
        }
        AggIntent::Count { accuracy } => {
            // Count is the legacy bridge — `count_over_time` lowers to
            // `Count{accuracy:Exact}` (exact counter, no sketch). When
            // the lowerer or callers ask for an approximate count
            // (`distinct_over_time` / SQL `COUNT(DISTINCT)`), the
            // accuracy is non-Exact and we hand it to the cardinality
            // sketch path.
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::CardinalityApprox)
            }
        }
        AggIntent::TopK { .. } => {
            // Top-k is intrinsically heavy-hitter — only the
            // CMS-with-heap variant can enumerate the items. CountMin /
            // CountSketch without a heap can answer point-frequency but
            // not top-k.
            Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
        }
        AggIntent::Frequency { accuracy } => {
            if is_exact(accuracy) {
                None
            } else {
                // Frequency point-queries use CMS-with-heap as the
                // canonical family (lets a single sketch family answer
                // both Frequency and TopK on the same metric).
                Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
            }
        }
        // ── No warm-tier sketch ──────────────────────────────────────
        AggIntent::Sum
        | AggIntent::Min
        | AggIntent::Max
        | AggIntent::Avg
        | AggIntent::Rate { .. }
        | AggIntent::Increase { .. } => None,
        // Archive-only intents — never bind to a warm-tier capability;
        // routed to the cold tier (Gorilla / Thanos).
        AggIntent::HistogramQuantile { .. }
        | AggIntent::Absent
        | AggIntent::Present
        | AggIntent::Delta { .. }
        | AggIntent::Deriv { .. }
        | AggIntent::PredictLinear { .. }
        | AggIntent::HoltWinters { .. }
        | AggIntent::Idelta { .. }
        | AggIntent::Irate { .. }
        | AggIntent::Resets { .. }
        | AggIntent::Changes { .. } => None,
    }
}

/// True iff the accuracy target forbids approximation. Wraps the match
/// so the call sites read as `if is_exact(accuracy) { ... }`.
fn is_exact(accuracy: &AccuracyTarget) -> bool {
    matches!(accuracy, AccuracyTarget::Exact)
}

// ── Per-sketch performance / capability profile ──────────────────────────────

/// Performance and capability profile for a single sketch family.
///
/// Used by the optimizer to compare candidates and by the physical
/// planner to check whether a sketch fits within a stage's budget.
/// Populated from compiled-in defaults via [`default_capability_table`]
/// or overridden at runtime via [`load_capability_overrides`].
#[derive(Debug, Clone)]
pub struct SketchCapability {
    /// Insertion throughput (samples/sec at 1 core).
    pub insert_throughput: f64,
    /// Query throughput (queries/sec at 1 core).
    pub query_throughput: f64,
    /// Memory footprint per series (bytes).
    pub memory_bytes_per_series: u64,
    /// CPU cost per insert (µs/sample).
    pub cpu_micros_per_insert: f64,
    /// Transmission size per flush (bytes).
    pub transmission_bytes: u64,
    /// Which logical aggregation intents this sketch supports.
    pub supported_intents: Vec<SupportedIntent>,
    /// Whether the sketch supports merge (`sketch(A∪B) = merge(sketch(A), sketch(B))`).
    pub mergeable: bool,
    /// Whether the sketch supports delta encoding.
    pub supports_delta: bool,
    /// Whether the sketch supports sliding windows natively.
    pub supports_sliding_window: bool,
}

/// A logical aggregation intent that a sketch can serve. Used in
/// [`SketchCapability::supported_intents`] to declare per-sketch
/// coverage; the optimizer reads this when deciding which family to
/// bind to an [`AggIntent`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupportedIntent {
    Quantile,
    Cardinality,
    Frequency,
    Extrema,
}

// ── YAML override loader ─────────────────────────────────────────────────────

/// YAML-serialisable capability profile (matches `sketch_capabilities.yml`).
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SketchCapabilityYaml {
    insert_throughput: f64,
    query_throughput: f64,
    memory_bytes_per_series: u64,
    cpu_micros_per_insert: f64,
    transmission_bytes: u64,
    supported_intents: Vec<String>,
    mergeable: bool,
    supports_delta: bool,
    supports_sliding_window: bool,
}

impl SketchCapabilityYaml {
    fn to_capability(&self) -> SketchCapability {
        let intents = self
            .supported_intents
            .iter()
            .filter_map(|s| match s.as_str() {
                "quantile" => Some(SupportedIntent::Quantile),
                "cardinality" => Some(SupportedIntent::Cardinality),
                "frequency" => Some(SupportedIntent::Frequency),
                "extrema" => Some(SupportedIntent::Extrema),
                _ => None,
            })
            .collect();
        SketchCapability {
            insert_throughput: self.insert_throughput,
            query_throughput: self.query_throughput,
            memory_bytes_per_series: self.memory_bytes_per_series,
            cpu_micros_per_insert: self.cpu_micros_per_insert,
            transmission_bytes: self.transmission_bytes,
            supported_intents: intents,
            mergeable: self.mergeable,
            supports_delta: self.supports_delta,
            supports_sliding_window: self.supports_sliding_window,
        }
    }
}

/// YAML file structure for all sketch capabilities. Mirrors
/// `controller/sketch_capabilities.yml` 1:1.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct SketchCapabilitiesFile {
    ddsketch: SketchCapabilityYaml,
    kll: SketchCapabilityYaml,
    hll: SketchCapabilityYaml,
    count_sketch: SketchCapabilityYaml,
    count_min_sketch: SketchCapabilityYaml,
}

/// Compiled-in capability defaults — one entry per [`SketchKind`].
/// Replaces the per-variant `sketch_capability(SketchType)` function
/// that previously lived in `algebra/optimizer.rs`. Numerical values
/// are mirrored from the YAML so the in-process defaults match the
/// reference deployment file.
pub fn default_capability_table() -> HashMap<SketchKind, SketchCapability> {
    let mut map = HashMap::new();
    map.insert(
        SketchKind::DDSketch,
        SketchCapability {
            insert_throughput: 10_000_000.0,
            query_throughput: 50_000_000.0,
            memory_bytes_per_series: 4_096,
            cpu_micros_per_insert: 0.1,
            transmission_bytes: 4_096,
            supported_intents: vec![SupportedIntent::Quantile, SupportedIntent::Extrema],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchKind::Kll,
        SketchCapability {
            insert_throughput: 5_000_000.0,
            query_throughput: 20_000_000.0,
            memory_bytes_per_series: 8_192,
            cpu_micros_per_insert: 0.2,
            transmission_bytes: 8_192,
            supported_intents: vec![SupportedIntent::Quantile, SupportedIntent::Extrema],
            mergeable: true,
            supports_delta: false,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchKind::Hll,
        SketchCapability {
            insert_throughput: 20_000_000.0,
            query_throughput: 100_000_000.0,
            memory_bytes_per_series: 16_384,
            cpu_micros_per_insert: 0.05,
            transmission_bytes: 16_384,
            supported_intents: vec![SupportedIntent::Cardinality],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchKind::CountSketch,
        SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            supported_intents: vec![SupportedIntent::Frequency],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map.insert(
        SketchKind::Cms,
        SketchCapability {
            insert_throughput: 8_000_000.0,
            query_throughput: 10_000_000.0,
            memory_bytes_per_series: 80_000,
            cpu_micros_per_insert: 0.5,
            transmission_bytes: 80_000,
            supported_intents: vec![SupportedIntent::Frequency],
            mergeable: true,
            supports_delta: true,
            supports_sliding_window: false,
        },
    );
    map
}

/// Load sketch capability overrides from a YAML file. Falls back to
/// [`default_capability_table`] if the file is missing or malformed.
/// Replaces `algebra::optimizer::load_sketch_capabilities`.
///
/// Env var: `CONTROLLER_SKETCH_CAPABILITIES=path/to/this/file.yml`.
pub fn load_capability_overrides(path: &str) -> HashMap<SketchKind, SketchCapability> {
    if let Ok(contents) = std::fs::read_to_string(path) {
        if let Ok(file) = serde_yaml::from_str::<SketchCapabilitiesFile>(&contents) {
            let mut map = HashMap::new();
            map.insert(SketchKind::DDSketch, file.ddsketch.to_capability());
            map.insert(SketchKind::Kll, file.kll.to_capability());
            map.insert(SketchKind::Hll, file.hll.to_capability());
            map.insert(SketchKind::CountSketch, file.count_sketch.to_capability());
            map.insert(SketchKind::Cms, file.count_min_sketch.to_capability());
            return map;
        }
    }
    default_capability_table()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ── capability_for: AggIntent → Capability bridge ────────────────────

    #[test]
    fn capability_for_quantile_returns_quantile_approx() {
        let intent = AggIntent::Quantile {
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::QuantileApprox(SketchKindHandle::Any))
        );
    }

    #[test]
    fn capability_for_quantile_exact_returns_none() {
        let intent = AggIntent::Quantile {
            q: 0.99,
            accuracy: AccuracyTarget::Exact,
        };
        // Exact quantiles must be answered by HashAgg/SortAgg — no
        // warm-tier sketch in this case.
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_cardinality_with_epsilon_returns_cardinality_approx() {
        let intent = AggIntent::Cardinality {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(capability_for(&intent), Some(Capability::CardinalityApprox));
    }

    #[test]
    fn capability_for_cardinality_with_epsilon_delta_returns_cardinality_approx() {
        let intent = AggIntent::Cardinality {
            accuracy: AccuracyTarget::EpsilonDelta {
                eps: 0.01,
                delta: 0.001,
            },
        };
        assert_eq!(capability_for(&intent), Some(Capability::CardinalityApprox));
    }

    #[test]
    fn capability_for_cardinality_with_exact_returns_none() {
        let intent = AggIntent::Cardinality {
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_count_approximate_returns_cardinality_approx() {
        let intent = AggIntent::Count {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(capability_for(&intent), Some(Capability::CardinalityApprox));
    }

    #[test]
    fn capability_for_count_exact_returns_none() {
        // `count_over_time` lowers to `Count{accuracy:Exact}` per
        // intent_algebra::lower. `capability_for` returning `None`
        // here is the contract that drives the analyzer to mark the
        // query as warm-tier-unsupported (it'll route to archive).
        let intent = AggIntent::Count {
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_sum_returns_none() {
        assert_eq!(capability_for(&AggIntent::Sum), None);
    }

    #[test]
    fn capability_for_min_max_avg_return_none() {
        assert_eq!(capability_for(&AggIntent::Min), None);
        assert_eq!(capability_for(&AggIntent::Max), None);
        assert_eq!(capability_for(&AggIntent::Avg), None);
    }

    #[test]
    fn capability_for_rate_increase_return_none() {
        assert_eq!(
            capability_for(&AggIntent::Rate {
                window: Duration::from_secs(60)
            }),
            None
        );
        assert_eq!(
            capability_for(&AggIntent::Increase {
                window: Duration::from_secs(60)
            }),
            None
        );
    }

    #[test]
    fn capability_for_topk_returns_frequency_topk_cms_with_heap() {
        let intent = AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::Epsilon(0.05),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
        );
    }

    #[test]
    fn capability_for_frequency_approximate_returns_topk_cms_with_heap() {
        let intent = AggIntent::Frequency {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
        );
    }

    #[test]
    fn capability_for_archive_only_intents_return_none() {
        // Spot-check each archive-only variant.
        assert_eq!(
            capability_for(&AggIntent::HistogramQuantile { q: 0.99 }),
            None,
        );
        assert_eq!(capability_for(&AggIntent::Absent), None);
        assert_eq!(capability_for(&AggIntent::Present), None);
        assert_eq!(
            capability_for(&AggIntent::Delta {
                window: Duration::from_secs(60)
            }),
            None
        );
        assert_eq!(
            capability_for(&AggIntent::Idelta {
                window: Duration::from_secs(60)
            }),
            None
        );
        assert_eq!(
            capability_for(&AggIntent::Irate {
                window: Duration::from_secs(60)
            }),
            None
        );
    }

    // ── Capability::is_satisfied_by ──────────────────────────────────────

    #[test]
    fn is_satisfied_by_any_wildcard_matches_concrete() {
        let required = Capability::QuantileApprox(SketchKindHandle::Any);
        let indexed_dd = Capability::QuantileApprox(SketchKindHandle::DDSketch);
        let indexed_kll = Capability::QuantileApprox(SketchKindHandle::Kll);
        assert!(required.is_satisfied_by(&indexed_dd));
        assert!(required.is_satisfied_by(&indexed_kll));
    }

    #[test]
    fn is_satisfied_by_concrete_kind_must_match_exact() {
        let required = Capability::QuantileApprox(SketchKindHandle::DDSketch);
        let indexed_dd = Capability::QuantileApprox(SketchKindHandle::DDSketch);
        let indexed_kll = Capability::QuantileApprox(SketchKindHandle::Kll);
        assert!(required.is_satisfied_by(&indexed_dd));
        assert!(!required.is_satisfied_by(&indexed_kll));
    }

    #[test]
    fn is_satisfied_by_cardinality_is_total() {
        let required = Capability::CardinalityApprox;
        let indexed = Capability::CardinalityApprox;
        assert!(required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_different_families_are_incompatible() {
        let required = Capability::QuantileApprox(SketchKindHandle::Any);
        let indexed = Capability::CardinalityApprox;
        assert!(!required.is_satisfied_by(&indexed));

        let required = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let indexed = Capability::QuantileApprox(SketchKindHandle::DDSketch);
        assert!(!required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_topk_handles_must_match() {
        let required = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let indexed_with_heap =
            Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let indexed_no_heap =
            Capability::FrequencyTopk(SketchKindHandle::CountMin);
        assert!(required.is_satisfied_by(&indexed_with_heap));
        assert!(!required.is_satisfied_by(&indexed_no_heap));
    }

    // ── default_capability_table ─────────────────────────────────────────

    #[test]
    fn default_table_carries_all_five_sketch_kinds() {
        let t = default_capability_table();
        assert!(t.contains_key(&SketchKind::DDSketch));
        assert!(t.contains_key(&SketchKind::Kll));
        assert!(t.contains_key(&SketchKind::Hll));
        assert!(t.contains_key(&SketchKind::Cms));
        assert!(t.contains_key(&SketchKind::CountSketch));
    }

    #[test]
    fn default_table_ddsketch_serves_quantile_intent() {
        let t = default_capability_table();
        let cap = t.get(&SketchKind::DDSketch).unwrap();
        assert!(cap.supported_intents.contains(&SupportedIntent::Quantile));
        assert!(cap.mergeable);
    }

    #[test]
    fn default_table_hll_serves_cardinality_intent() {
        let t = default_capability_table();
        let cap = t.get(&SketchKind::Hll).unwrap();
        assert!(cap.supported_intents.contains(&SupportedIntent::Cardinality));
    }

    // ── load_capability_overrides ────────────────────────────────────────

    #[test]
    fn load_overrides_missing_path_returns_defaults() {
        let loaded = load_capability_overrides("/nonexistent/path/sketch_capabilities.yml");
        let defaults = default_capability_table();
        // Same set of keys, same defaults — we don't assert byte
        // equality on the SketchCapability values because they don't
        // impl PartialEq, but they share the same supported_intents
        // set per kind.
        assert_eq!(loaded.len(), defaults.len());
        for k in defaults.keys() {
            assert!(loaded.contains_key(k));
        }
    }
}
