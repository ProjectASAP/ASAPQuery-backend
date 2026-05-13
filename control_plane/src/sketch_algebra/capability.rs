//! Single source of truth for capability state.
//!
//! Step 2a of the architectural refactor consolidates the four overlapping
//! capability tables that previously existed in the controller — the YAML
//! at `control_plane/sketch_capabilities.yml`, the compiled-in defaults in
//! `algebra/optimizer.rs::sketch_capability`, the `SketchKind` enum in
//! `sketch_algebra/params.rs`, and the per-query `Capability` /
//! `SketchKindHandle` invented inside `warm_tier_analysis.rs` (PR #128).
//! All four collapse into this module:
//!
//! - [`SketchCapability`] / [`SupportedIntent`] — per-sketch performance
//!   profile (insert / memory / CPU / transmission costs + the logical
//!   intents the sketch can serve). Read by `algebra/optimizer.rs` for
//!   cost-based plan rewriting and by `algebra/physical.rs` for stage
//!   placement. Disambiguation: distinct from `schema.rs::SketchStateMetadata`
//!   (which carries L4 type-system flags `mergeable` / `subtractable` /
//!   `deletable`) — `SketchCapability` here is the perf / cost-model surface,
//!   `SketchStateMetadata` is the L4 catalog-flag surface.
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
use promql_utilities::query_logics::enums::AggregationType;

// ── Query-side capability tag ────────────────────────────────────────────────

/// Warm-tier capability tag. One variant per logical query family the
/// warm tier can answer. The inner [`SketchKindHandle`] is the
/// implementation choice (e.g. DDSketch vs KLL for `QuantileApprox`).
/// Query routing keys on the variant, not the implementation, so two
/// CMS instances and one CountSketch instance for the same metric-and-
/// group-by all map to `FrequencyTopk` and the query path picks any
/// of them.
///
/// Used by both the control plane (via [`capability_for`] in the warm-tier
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
    /// Bare per-item frequency estimate (CMS / CountSketch point query,
    /// no top-k extraction). Heap-LESS — answers `sum by (item) (rate(m[r]))`
    /// with epsilon accuracy. Distinct from [`Capability::FrequencyTopk`]:
    /// any heap-bearing variant ALSO satisfies bare frequency (the heap is
    /// additional info layered on top of the sketch matrix), so
    /// `is_satisfied_by` allows {CountMin, CountSketch, CmsWithHeap,
    /// CountSketchWithHeap} on the available side.
    FrequencyEstimate(SketchKindHandle),
    /// Heavy-hitter top-k via CMS-with-heap or CountSketch-with-heap.
    /// Heap-BEARING — only handles that carry an item universe in their
    /// wire format can answer this. `Any` required matches either
    /// `CmsWithHeap` or `CountSketchWithHeap`.
    FrequencyTopk(SketchKindHandle),
    /// Exact-aggregation warm-tier state — Sum / Count / MinMax / Avg /
    /// Rate / Increase / SetAggregator etc. Backed by a per-accumulator
    /// payload (`AggPayload::ExactAgg` in the data plane). One variant
    /// per [`AggregationType`] — the inner enum names the concrete
    /// accumulator family.
    ///
    /// Distinct from the `*Approx` variants above: the `*Approx`
    /// capabilities serve approximate sketch-bound intents; `ExactAgg`
    /// serves the warm-tier exact-aggregation path (the data plane's
    /// `AggKind::ExactAgg`-backed sids). Routing an analyzer candidate
    /// at `Capability::ExactAgg(Sum)` to a sid whose `agg_kind` is
    /// `AggKind::ExactAgg { agg_type: Sum, .. }` is what closes the gap
    /// between the control plane's vocabulary and the data plane's
    /// exact-aggregation state.
    ///
    /// PR 6 introduces this variant + the matching machinery. The
    /// `capability_for(&AggIntent)` lookup deliberately does NOT route
    /// `Sum` / `Min` / `Max` / `Rate` / `Increase` / exact-accuracy
    /// intents to `ExactAgg` yet — that re-routing is a behavior
    /// change deferred to a follow-up. The variant is dormant on the
    /// analyzer side until then; the matching half (`is_satisfied_by`)
    /// is wired so that sids whose stored `Capability` is
    /// `ExactAgg(...)` can be filtered against an `ExactAgg(...)`
    /// required capability once callers start populating it.
    ExactAgg(AggregationType),
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
    /// CountSketch paired with a heavy-hitter heap. Same role as
    /// `CmsWithHeap` but on the CountSketch substrate (balanced /
    /// zero-mean error instead of CMS's one-sided bias).
    CountSketchWithHeap,
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
            // Top-k family: only heap-bearing handles (CmsWithHeap or
            // CountSketchWithHeap) qualify on the available side. `Any`
            // required matches either; a concrete required handle must
            // match exactly.
            (Capability::FrequencyTopk(req), Capability::FrequencyTopk(have)) => {
                is_heap_bearing(*have) && handles_compatible_for_topk(*req, *have)
            }
            // Bare frequency: any frequency-family handle works on the
            // available side — heap-LESS (CountMin / CountSketch) AND
            // heap-bearing (CmsWithHeap / CountSketchWithHeap) all answer
            // a point-frequency query (heap is additional info layered on
            // the sketch matrix). A heap-bearing `FrequencyTopk` indexed
            // capability ALSO satisfies a bare-frequency required capability.
            (Capability::FrequencyEstimate(req), Capability::FrequencyEstimate(have)) => {
                is_frequency_family(*have) && handles_compatible(*req, *have)
            }
            (Capability::FrequencyEstimate(req), Capability::FrequencyTopk(have)) => {
                is_heap_bearing(*have) && handles_compatible(*req, *have)
            }
            // Exact-aggregation family: the agg_type must match exactly.
            // There is no `Any` wildcard for ExactAgg — a Sum sid does
            // not satisfy a MinMax requirement and vice versa. If a
            // future PR introduces a wildcard semantic (e.g. "any
            // single-population accumulator"), extend the match here.
            (Capability::ExactAgg(req), Capability::ExactAgg(have)) => req == have,
            _ => false,
        }
    }
}

/// True when the required handle is `Any` (wildcard) or matches the
/// available handle exactly. Used by [`Capability::is_satisfied_by`].
fn handles_compatible(required: SketchKindHandle, available: SketchKindHandle) -> bool {
    matches!(required, SketchKindHandle::Any) || required == available
}

/// `Any` required for top-k means "any heap-bearing handle"; concrete
/// required must match exactly.
fn handles_compatible_for_topk(
    required: SketchKindHandle,
    available: SketchKindHandle,
) -> bool {
    matches!(required, SketchKindHandle::Any) || required == available
}

/// True when the handle carries a heavy-hitter heap (i.e. it can
/// enumerate top-k items without an external item list).
fn is_heap_bearing(h: SketchKindHandle) -> bool {
    matches!(
        h,
        SketchKindHandle::CmsWithHeap | SketchKindHandle::CountSketchWithHeap
    )
}

/// True when the handle belongs to the frequency family — any of
/// `CountMin` / `CountSketch` (heap-less) or `CmsWithHeap` /
/// `CountSketchWithHeap` (heap-bearing).
fn is_frequency_family(h: SketchKindHandle) -> bool {
    matches!(
        h,
        SketchKindHandle::CountMin
            | SketchKindHandle::CountSketch
            | SketchKindHandle::CmsWithHeap
            | SketchKindHandle::CountSketchWithHeap
    )
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
/// | `Min` / `Max` | `Some(QuantileApprox(Any))` — quantile sketches answer min = q(0), max = q(1) |
/// | `Cardinality { accuracy }` (accuracy not `Exact`) | `Some(CardinalityApprox)` |
/// | `Cardinality { accuracy: Exact }` | `None` |
/// | `Count { accuracy }` (same logic as Cardinality) | `Some(CardinalityApprox)` / `None` |
/// | `TopK { k, accuracy }` (accuracy not `Exact`) | `Some(FrequencyTopk(CmsWithHeap))` |
/// | `Frequency { accuracy }` (accuracy not `Exact`) | `Some(FrequencyEstimate(Any))` |
/// | `Frequency { accuracy: Exact }` | `None` (exact aggregation; route to archive) |
/// | `Sum` / `Avg` / `Rate` / `Increase` | `None` |
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
        AggIntent::TopK { accuracy, .. } => {
            if is_exact(accuracy) {
                // Exact top-k must use HashAgg+Heap; no warm-tier sketch.
                None
            } else {
                // Top-k is intrinsically heavy-hitter — only heap-bearing
                // handles can enumerate the items. `CmsWithHeap` is the
                // canonical handle today; `is_satisfied_by` accepts
                // either heap-bearing variant against an `Any` required.
                Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
            }
        }
        AggIntent::Frequency { accuracy } => {
            if is_exact(accuracy) {
                // Exact aggregation — sketch fallback is only meaningful
                // when raw counters aren't kept at the ingest tier; with
                // accuracy=Exact the caller wants exact `sum by (label)
                // (rate(...))`, which routes to archive.
                None
            } else {
                // Bare frequency point-query uses a frequency-family
                // sketch — any of CMS / CountSketch / CmsWithHeap /
                // CountSketchWithHeap works (the heap is additional
                // info that the FrequencyTopk path uses). `Any` here
                // means the optimizer picks the cheapest indexed sid.
                Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
            }
        }
        // ── Min / Max via quantile sketches ──────────────────────────
        // DDSketch / KLL answer min = quantile(0) and max = quantile(1)
        // out of the box. No dedicated extrema sketch is needed; route
        // these through the quantile-family handler.
        AggIntent::Min | AggIntent::Max => {
            Some(Capability::QuantileApprox(SketchKindHandle::Any))
        }
        // ── No warm-tier sketch ──────────────────────────────────────
        AggIntent::Sum
        | AggIntent::Avg
        | AggIntent::Rate { .. }
        | AggIntent::Increase { .. } => None,
        // Archive-only intents — never bind to a warm-tier capability;
        // routed to the cold tier (Gorilla / Thanos).
        AggIntent::Absent
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
///
/// Distinct from
/// [`crate::sketch_algebra::schema::SketchStateMetadata`] — this struct
/// is the **perf / feasibility / intent-routing** profile consumed by
/// the cost model and the optimizer's binding rules. The schema-side
/// `SketchStateMetadata` carries the **L4 type-system flags**
/// (`mergeable` / `subtractable` / `deletable`) that gate `SketchMerge` /
/// `SketchSubtract` / `SketchDelete` at plan-time. The two have
/// different consumers and different lifecycles — `SketchCapability`
/// is read at every plan-rewrite call site; `SketchStateMetadata`
/// is sealed onto each `SketchExpr` edge once the binding rule fires.
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
/// `control_plane/sketch_capabilities.yml` 1:1.
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

// ── Sketch-family error bounds ───────────────────────────────────────────────
//
// These two helpers were originally defined in `controller/src/algebra/expr.rs`
// (now `controller/src/intent_algebra/legacy_expr.rs`). The 2026-05
// layered-cleanup refactor moves them here — they are sketch-family
// error bounds, so the capability module is their structural home.
//
// The legacy module re-exports both via [`hll_accuracy`] /
// [`countmin_accuracy`] aliases so callers like `AggIntent::default_cardinality`
// keep compiling.

/// HLL accuracy from register count: `1.04 / sqrt(2^registers)`.
///
/// Source: Flajolet et al., "HyperLogLog: the analysis of a
/// near-optimal cardinality estimation algorithm" (2007).
pub fn hll_accuracy(registers: u8) -> f64 {
    1.04 / (2.0f64.powi(registers as i32)).sqrt()
}

/// Count-Min Sketch accuracy from width: `e / width`.
///
/// Source: Cormode & Muthukrishnan, "An improved data stream summary:
/// the count-min sketch and its applications" (2005).
pub fn countmin_accuracy(width: u32) -> f64 {
    std::f64::consts::E / width as f64
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
    fn capability_for_avg_returns_none() {
        // Avg is exact at L3 — no warm-tier sketch substitutes for it
        // today (a sketch-bound `Avg` would fold onto `Quantile{q=0.5}`
        // only when the cost model allows the relaxation, which is a
        // follow-up).
        assert_eq!(capability_for(&AggIntent::Avg), None);
    }

    #[test]
    fn capability_for_min_returns_quantile_approx() {
        // Min = quantile(0); DDSketch / KLL answer it directly.
        assert_eq!(
            capability_for(&AggIntent::Min),
            Some(Capability::QuantileApprox(SketchKindHandle::Any))
        );
    }

    #[test]
    fn capability_for_max_returns_quantile_approx() {
        // Max = quantile(1); DDSketch / KLL answer it directly.
        assert_eq!(
            capability_for(&AggIntent::Max),
            Some(Capability::QuantileApprox(SketchKindHandle::Any))
        );
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
    fn capability_for_topk_exact_returns_none() {
        // Exact top-k must use HashAgg+Heap; no warm-tier sketch.
        let intent = AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn frequency_estimate_with_epsilon_returns_frequency_estimate_approx() {
        let intent = AggIntent::Frequency {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
        );
    }

    #[test]
    fn frequency_estimate_with_epsilon_delta_returns_frequency_estimate_approx() {
        let intent = AggIntent::Frequency {
            accuracy: AccuracyTarget::EpsilonDelta {
                eps: 0.01,
                delta: 0.001,
            },
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
        );
    }

    #[test]
    fn frequency_estimate_with_exact_returns_none() {
        // Exact aggregation routes to archive (sketch fallback only
        // meaningful when raw counters aren't kept).
        let intent = AggIntent::Frequency {
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_archive_only_intents_return_none() {
        // Spot-check each archive-only variant.
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
        let indexed_with_heap = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let indexed_no_heap = Capability::FrequencyTopk(SketchKindHandle::CountMin);
        assert!(required.is_satisfied_by(&indexed_with_heap));
        assert!(!required.is_satisfied_by(&indexed_no_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_topk_rejects_heapless() {
        // Top-k REQUIRES a heap-bearing handle. Even when the available
        // capability declares itself as `FrequencyTopk(CountMin)` (an
        // ill-formed catalog entry), the satisfaction check must reject
        // it — top-k cannot enumerate items off a heap-less sketch.
        let required_any = Capability::FrequencyTopk(SketchKindHandle::Any);
        let required_concrete = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let indexed_heapless = Capability::FrequencyTopk(SketchKindHandle::CountMin);
        let indexed_heapless_cs = Capability::FrequencyTopk(SketchKindHandle::CountSketch);
        assert!(!required_any.is_satisfied_by(&indexed_heapless));
        assert!(!required_any.is_satisfied_by(&indexed_heapless_cs));
        assert!(!required_concrete.is_satisfied_by(&indexed_heapless));
    }

    #[test]
    fn is_satisfied_by_frequency_topk_any_matches_either_heap() {
        // `Any` required for top-k accepts either heap-bearing handle.
        let required = Capability::FrequencyTopk(SketchKindHandle::Any);
        let cms_heap = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let cs_heap = Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        assert!(required.is_satisfied_by(&cms_heap));
        assert!(required.is_satisfied_by(&cs_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_estimate_accepts_heap_bearing() {
        // Bare frequency point queries can be answered by ANY
        // frequency-family sketch — heap-less AND heap-bearing both work
        // (the heap is additional metadata; the underlying CMS / CS
        // matrix answers the point query either way).
        let required = Capability::FrequencyEstimate(SketchKindHandle::Any);
        let cms = Capability::FrequencyEstimate(SketchKindHandle::CountMin);
        let cs = Capability::FrequencyEstimate(SketchKindHandle::CountSketch);
        let cms_heap = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        let cs_heap = Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        assert!(required.is_satisfied_by(&cms));
        assert!(required.is_satisfied_by(&cs));
        assert!(required.is_satisfied_by(&cms_heap));
        assert!(required.is_satisfied_by(&cs_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_estimate_rejects_non_frequency_family() {
        let required = Capability::FrequencyEstimate(SketchKindHandle::Any);
        // QuantileApprox / CardinalityApprox don't answer frequency.
        let q = Capability::QuantileApprox(SketchKindHandle::DDSketch);
        let c = Capability::CardinalityApprox;
        // FrequencyEstimate with a non-frequency-family handle on the
        // available side is also rejected (defensive).
        let bad = Capability::FrequencyEstimate(SketchKindHandle::Hll);
        assert!(!required.is_satisfied_by(&q));
        assert!(!required.is_satisfied_by(&c));
        assert!(!required.is_satisfied_by(&bad));
    }

    // ── Capability::ExactAgg — matching ──────────────────────────────────

    #[test]
    fn is_satisfied_by_exact_agg_same_type_matches() {
        // Sum required, Sum indexed → match. Same for every concrete
        // AggregationType — the equality check is structural.
        let required = Capability::ExactAgg(AggregationType::Sum);
        let indexed = Capability::ExactAgg(AggregationType::Sum);
        assert!(required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_exact_agg_different_types_do_not_match() {
        // Sum required, MinMax indexed → no match. No wildcard for
        // ExactAgg — every agg_type stands on its own.
        let required = Capability::ExactAgg(AggregationType::Sum);
        let indexed = Capability::ExactAgg(AggregationType::MinMax);
        assert!(!required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_exact_agg_does_not_match_other_families() {
        // ExactAgg is its own family — no cross-family satisfaction
        // with QuantileApprox / CardinalityApprox / FrequencyEstimate /
        // FrequencyTopk.
        let required = Capability::ExactAgg(AggregationType::Sum);
        assert!(!required
            .is_satisfied_by(&Capability::QuantileApprox(SketchKindHandle::DDSketch)));
        assert!(!required.is_satisfied_by(&Capability::CardinalityApprox));
        assert!(!required
            .is_satisfied_by(&Capability::FrequencyEstimate(SketchKindHandle::CountMin)));
        assert!(!required
            .is_satisfied_by(&Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap)));

        // And the reverse — a sketch-family required capability must
        // not match an ExactAgg-backed sid.
        let sketch_required = Capability::QuantileApprox(SketchKindHandle::Any);
        let exact_indexed = Capability::ExactAgg(AggregationType::DatasketchesKLL);
        assert!(!sketch_required.is_satisfied_by(&exact_indexed));
    }

    #[test]
    fn exact_agg_covers_each_canonical_agg_type() {
        // Spot-check the full AggregationType surface — each variant
        // round-trips through Capability::ExactAgg without losing
        // information. Documents the intended coverage of the new
        // variant. If a future PR adds an AggregationType variant, this
        // test (combined with the exhaustive match in `is_satisfied_by`'s
        // `req == have` form) will not require code changes — equality
        // is structural.
        let cases = [
            AggregationType::Sum,
            AggregationType::Increase,
            AggregationType::MinMax,
            AggregationType::DatasketchesKLL,
            AggregationType::MultipleSum,
            AggregationType::MultipleIncrease,
            AggregationType::MultipleMinMax,
            AggregationType::HydraKLL,
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
            AggregationType::CountSketch,
            AggregationType::SetAggregator,
            AggregationType::DeltaSetAggregator,
            AggregationType::HLL,
            AggregationType::DDSketch,
        ];
        for t in cases {
            let cap = Capability::ExactAgg(t);
            assert!(
                cap.is_satisfied_by(&Capability::ExactAgg(t)),
                "ExactAgg({t:?}) should satisfy itself"
            );
        }
    }

    // ── capability_for: ExactAgg dormancy ────────────────────────────────

    #[test]
    fn capability_for_sum_still_returns_none_after_exact_agg_landing() {
        // PR 6 explicitly does NOT change `capability_for` for the
        // intents that today return `None` (Sum / Min / Max / Avg /
        // Rate / Increase / archive-only). The `Capability::ExactAgg`
        // variant is wired into `is_satisfied_by` but the analyzer's
        // intent → capability bridge stays as it was — re-routing
        // those intents to warm-tier ExactAgg is a follow-up that
        // requires populating `SketchInstanceMetadata.capability` with
        // `Some(Capability::ExactAgg(_))` for the ExactAgg-backed sids
        // first.
        assert_eq!(capability_for(&AggIntent::Sum), None);
        // Min / Max are intentionally NOT in this dormancy list — they
        // already route to QuantileApprox (DDSketch / KLL answer them
        // via quantile(0) / quantile(1)) and that path is unchanged.
    }

    #[test]
    fn count_sketch_with_heap_handle_round_trips() {
        // `CountSketchWithHeap` is the CountSketch counterpart to
        // `CmsWithHeap`. Construct a `FrequencyTopk` capability around
        // it and verify it satisfies an `Any`-required top-k.
        let cap = Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        let required = Capability::FrequencyTopk(SketchKindHandle::Any);
        assert!(required.is_satisfied_by(&cap));
        // And the concrete-against-concrete must match exactly.
        let required_concrete =
            Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        assert!(required_concrete.is_satisfied_by(&cap));
        // A different concrete heap-bearing handle must NOT match.
        let required_cms = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        assert!(!required_cms.is_satisfied_by(&cap));
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
        assert!(cap
            .supported_intents
            .contains(&SupportedIntent::Cardinality));
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
