//! Runtime SID capability tag and query-to-deployed-state routing adapter.
//!
//! Step 2a of the architectural refactor originally consolidated four
//! overlapping capability tables into this module. The performance /
//! cost-model half of that consolidation — [`SketchCapability`] /
//! `default_capability_table` / `load_capability_overrides`
//! — moved to `crate::physical::deployment_cost::sketch_capability` (Stage 4 of the
//! `physical::post_asap` re-layering): it's a cost-model concern read by the
//! optimizer and physical planner, not L4 IR. What's left here:
//!
//! - [`Capability`] / [`SketchAlgorithm`] — query-side capability tag,
//!   used by the ASAP-tier reducer in `asap-query-engine` to dispatch
//!   PromQL → per-Capability sketch evaluation.
//! - [`capability_for`] — the **semantic** intent → ASAP-tier dispatch
//!   bridge. PromQL → intent_algebra::lower → `AggIntent` → (this fn) →
//!   `Capability`. The ASAP-tier analyzer is now a thin facade around
//!   this single function; PromQL function-name string matching lives
//!   only inside the lowerer.

#![allow(dead_code)]

use crate::physical::post_asap::matcher::sketch_family_satisfied;
use crate::types_v2::AccuracyTarget;
use asap_types::AggregationType;
pub use planner_types::post_asap::SketchAlgorithm;
use planner_types::pre_asap::AggIntent;

// ── Query-side capability tag ────────────────────────────────────────────────

/// Warm-tier capability tag. One variant per logical query family the
/// ASAP tier can answer. The inner [`SketchAlgorithm`] is the
/// implementation choice (e.g. DDSketch vs KLL for `QuantileApprox`).
/// Query routing keys on the variant, not the implementation, so two
/// CMS instances and one CountSketch instance for the same metric-and-
/// group-by all map to `FrequencyTopk` and the query path picks any
/// of them.
///
/// Used by both the control plane (via [`capability_for`] in the ASAP-tier
/// analyzer) and the `asap-query-engine` backend (re-exported as the
/// `sketch_index::Capability` it indexes sketch instances under). One
/// canonical definition; the backend re-exports rather than duplicating.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Approximate quantile via DDSketch / KLL / t-digest. The handle's
    /// `Any` variant means "any quantile-family sketch satisfies"; a
    /// concrete handle means "must be exactly this family".
    QuantileApprox(Option<SketchAlgorithm>),
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
    FrequencyEstimate(Option<SketchAlgorithm>),
    /// Heavy-hitter top-k via CMS-with-heap or CountSketch-with-heap.
    /// Heap-BEARING — only handles that carry an item universe in their
    /// wire format can answer this. `Any` required matches either
    /// `CmsWithHeap` or `CountSketchWithHeap`.
    FrequencyTopk(Option<SketchAlgorithm>),
    /// Exact-aggregation ASAP-tier state — Sum / Count / MinMax / Avg /
    /// Rate / Increase / SetAggregator etc. Backed by a per-accumulator
    /// payload (`AggPayload::ExactAgg` in the data plane). One variant
    /// per [`AggregationType`] — the inner enum names the concrete
    /// accumulator family.
    ///
    /// Distinct from the `*Approx` variants above: the `*Approx`
    /// capabilities serve approximate sketch-bound intents; `ExactAgg`
    /// serves the ASAP-tier exact-aggregation path (the data plane's
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

/// PromQL outer-function flavour carried on each `ASAPTierCandidate` so
/// the engine can distinguish `rate(metric[r])` / `irate(...)` from
/// `sum_over_time(metric[r])` / `sum(metric)` / bare selector WITHOUT
/// re-parsing the raw PromQL string.
///
/// Background: until the Phase 2 semantic retarget (ASAPController
/// alignment), the lowerer collapsed every `AggFunc` in
/// `{Sum, Rate, Increase, Delta}` onto a single `AggIntent::Sum`, which
/// `capability_for` then mapped to `Capability::ExactAgg(Sum)` for all
/// of them — erasing the rate-vs-plain distinction the engine needs to
/// decide between the plain ExactAgg reducer and the rate-divisor
/// reducer (`evaluate_exact_agg_rate`). `OuterFn` was introduced to
/// carry that distinction back (replacing an earlier raw-string
/// `query_contains_rate_call` re-parse). `rate`/`increase` now bind
/// their own `AggIntent::Rate`/`AggIntent::Increase` (matching
/// `asap_aware_mapping::boundary::implementation_for`'s `SummaryKind::Rate`/
/// `Increase` — ASAPController models Rate as a distinct summary
/// family), both mapping to `Capability::ExactAgg(AggregationType::Increase)`;
/// only `sum`/`sum by (...)`/bare selectors and `sum_over_time` still
/// bind `AggIntent::Sum` → `ExactAgg(Sum)`. `OuterFn` still carries the
/// PromQL-function-shape distinction `Capability` doesn't encode (e.g.
/// which divisor/accumulation strategy `evaluate_exact_agg_rate` uses),
/// but the sid-matching predicate is no longer purely `ExactAgg(Sum)`
/// -- see `Capability::is_satisfied_by`'s `sum_satisfies_increase` for
/// how a Sum-registered sid still answers an `ExactAgg(Increase)`
/// required capability.
///
/// The walker that populates this lives in `asap_tier_analysis.rs`
/// (`trace_from_promql`) — it picks the most-specific counter-function
/// flavour found anywhere in the expression tree (inner-function wins
/// for composed shapes like `sum by (...) (rate(...))`).
///
/// ## Counter-function taxonomy (issue #301)
///
/// Post-#299 the agent streams per-window DELTAS for counters. The four
/// PromQL counter idioms have genuinely different semantics over those
/// deltas. Before #301 the engine only distinguished `Rate` from
/// everything else, so `sum`, `sum_over_time`, `increase`, and
/// instant-sum all hit the same reducer path and returned the same
/// (wrong) number. This enum carries the function distinction the
/// engine needs to dispatch correctly:
///
/// | Variant       | PromQL                       | `required_capability`  | Engine dispatch                                   |
/// |---------------|------------------------------|-------------------------|----------------------------------------------------|
/// | `Plain`       | `sum(c)` / `sum by (..) (c)` | `ExactAgg(Sum)`         | accumulate ALL windows → cumulative-since-storage |
/// | `Rate`        | `rate(c[r])` / `irate(c[r])` | `ExactAgg(Increase)`    | Σ deltas in `[t-r,t]` ÷ min(r, coverage)          |
/// | `Increase`    | `increase(c[r])`             | `ExactAgg(Increase)`    | Σ deltas in `[t-r,t]` (one cumulative number)     |
/// | `SumOverTime` | `sum_over_time(c[r])`        | `ExactAgg(Sum)`         | capability-miss → archive (can't reconstruct)     |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OuterFn {
    /// No range-style counter function in the expression — bare selector,
    /// `sum(metric)`, `sum by (...) (metric)`. PromQL semantics for an
    /// instant `sum` over a counter is "current cumulative counter value,
    /// summed per group". Over per-window deltas the engine accumulates
    /// EVERY window in storage up to `now` into one cumulative number per
    /// group. This is the default — `Default::default()` returns `Plain`
    /// so candidates built without an explicit outer-fn (test fixtures,
    /// fallback paths) get the safe accumulate-all dispatch.
    #[default]
    Plain,
    /// `rate(metric[r])` or `irate(metric[r])` appears in the expression
    /// (possibly nested inside an outer `sum by (...) (...)`). The engine
    /// dispatches to `evaluate_exact_agg_rate`, which sums the deltas in
    /// `[t-r, t]` and divides by `min(r, actual_coverage_seconds)` to
    /// produce events-per-second.
    Rate,
    /// `increase(metric[r])` appears in the expression. PromQL semantics:
    /// `counter(t) − counter(t−r)`. Over per-window deltas that is exactly
    /// the sum of deltas in `[t-r, t]`. The engine dispatches to the
    /// accumulate-across-windows path scoped to the `[t-r, t]` clip,
    /// yielding ONE cumulative number per group (no `÷ r`).
    Increase,
    /// `sum_over_time(metric[r])` appears in the expression. PromQL
    /// semantics: Σ of the (cumulative) SAMPLE values in `[r]` — a
    /// quadratic over the storage horizon that asap CANNOT reconstruct
    /// from stored deltas. The engine returns a capability-miss so the
    /// query routes to the archive tier (which has raw samples) rather
    /// than fabricating a wrong number. See issue #301 decision (a).
    SumOverTime,
}

/// PromQL outer-aggregation operator carried on each `ASAPTierCandidate`
/// for the shape `<agg-op> by (labels) (<inner>)` where `<inner>` is a
/// function the analyzer ALREADY routes to a per-row ASAP-tier
/// candidate (e.g. `quantile_over_time`, `sum_over_time`, `rate`).
///
/// Background: the analyzer's lowerer captures the INNER intent
/// (`AggIntent::Quantile`, `AggIntent::Sum`, etc.) — that's what
/// `capability_for` maps to a `Capability`. The OUTER aggregation
/// operator (`max`, `min`, `avg`, `count`, etc.) wrapping the inner
/// function is dropped on the floor: the lowerer either folds it into a
/// dedicated `AggIntent` (`Sum` → `ExactAgg(Sum)`, handled separately)
/// or returns no extra intent for the wrapper (`max`, `min`, `avg`,
/// `count` — which are scalar folds over the inner's per-row result).
///
/// `OuterAgg` carries that wrapper so the engine's evaluator can
/// fold per-row results into one row per `by`-group AFTER the inner
/// function returns its rows. The taxonomy intentionally splits the
/// fold operators that ARE composable on top of a per-row inner result
/// — the inner sketch / accumulator computes per-row values, and the
/// outer aggregation reduces across rows in each `by`-group.
///
/// Identity case: when the inner result already has exactly one row
/// per `by`-group (e.g. asap's per-zone DDSketch quantile), the fold
/// is the identity — `max(x) = min(x) = avg(x) = x`. The general fold
/// machinery handles this naturally without a special case.
///
/// `None` is the default — `Default::default()` returns `None` so
/// candidates built without an explicit outer aggregation (test
/// fixtures, plain inner-only queries) keep the prior behavior.
///
/// Out of scope (separate follow-up):
/// - PromQL `quantile(phi, vec)` (instant) over function results —
///   needs per-group sketch merging, not a scalar fold.
/// - `sum` is NOT included here: `sum by (...) (...)` already routes
///   through `Capability::ExactAgg(Sum)` via the analyzer's lowerer +
///   the `Sum` intent collapse; adding it here would double-dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OuterAgg {
    /// No outer aggregation operator wraps the inner function — the
    /// engine emits the inner result directly. This is the default.
    #[default]
    None,
    /// `max by (labels) (<inner>)` — fold each `by`-group's values by
    /// taking the maximum.
    Max(Vec<String>),
    /// `min by (labels) (<inner>)` — fold each `by`-group's values by
    /// taking the minimum.
    Min(Vec<String>),
    /// `avg by (labels) (<inner>)` — fold each `by`-group's values by
    /// taking the arithmetic mean.
    Avg(Vec<String>),
    /// `count by (labels) (<inner>)` — fold each `by`-group's values
    /// by counting the contributing rows (cardinality of the group).
    Count(Vec<String>),
    /// `group by (labels) (<inner>)` — PromQL `group` operator returns
    /// 1.0 per `by`-group (label preservation, value-erasing fold).
    Group(Vec<String>),
    /// `stddev by (labels) (<inner>)` — fold each `by`-group's values
    /// by taking the population standard deviation.
    Stddev(Vec<String>),
    /// `stdvar by (labels) (<inner>)` — fold each `by`-group's values
    /// by taking the population variance.
    Stdvar(Vec<String>),
}

impl OuterAgg {
    /// True when an outer aggregation operator is set. False for the
    /// `None` default. Used by the engine's evaluator to skip the
    /// fold pass when no outer aggregation applies.
    pub fn is_some(&self) -> bool {
        !matches!(self, OuterAgg::None)
    }

    /// The `by`-labels carried by every operator variant. `None` returns
    /// an empty slice. Caller projects each result row's label map onto
    /// these keys to form the group identity.
    pub fn by_labels(&self) -> &[String] {
        match self {
            OuterAgg::None => &[],
            OuterAgg::Max(l)
            | OuterAgg::Min(l)
            | OuterAgg::Avg(l)
            | OuterAgg::Count(l)
            | OuterAgg::Group(l)
            | OuterAgg::Stddev(l)
            | OuterAgg::Stdvar(l) => l.as_slice(),
        }
    }

    /// Fold a slice of f64 values into a single scalar per the operator.
    /// Returns `None` only for an empty input slice (caller drops empty
    /// groups). All operators are defined on at least one value.
    pub fn fold(&self, values: &[f64]) -> Option<f64> {
        if values.is_empty() {
            return None;
        }
        Some(match self {
            // Identity / no-op — engine shouldn't call this when None,
            // but defensively return the first value.
            OuterAgg::None => values[0],
            OuterAgg::Max(_) => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            OuterAgg::Min(_) => values.iter().copied().fold(f64::INFINITY, f64::min),
            OuterAgg::Avg(_) => {
                let sum: f64 = values.iter().sum();
                sum / values.len() as f64
            }
            OuterAgg::Count(_) => values.len() as f64,
            OuterAgg::Group(_) => 1.0,
            OuterAgg::Stddev(_) => {
                let n = values.len() as f64;
                let mean: f64 = values.iter().sum::<f64>() / n;
                let var: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
                var.sqrt()
            }
            OuterAgg::Stdvar(_) => {
                let n = values.len() as f64;
                let mean: f64 = values.iter().sum::<f64>() / n;
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n
            }
        })
    }
}

impl Capability {
    /// True when an indexed sketch instance's capability satisfies the
    /// query's required capability. `None` on the query side means any
    /// algorithm in that capability family.
    ///
    /// `self` is the **required** capability (from the analyzer);
    /// `indexed` is the **available** capability (from the sketch
    /// index). The backend's ASAP-tier hook reads both and routes the
    /// query to whichever sids satisfy.
    ///
    /// The four sketch-family variants (`QuantileApprox`,
    /// `CardinalityApprox`, `FrequencyEstimate`, `FrequencyTopk`)
    /// delegate their family-compatibility logic to
    /// [`sketch_family_satisfied`] (`enum-unification-plan.md`
    /// §5/§8 Step 4) — via [`resolve_handle`], which picks a concrete
    /// per-family stand-in for the `Any` wildcard since `SketchAlgorithm`
    /// has no wildcard concept of its own; family-matching subsumes it.
    /// `ExactAgg` is intentionally NOT routed through this path — see
    /// [`multi_pop_satisfies_single`]'s doc for why.
    pub fn is_satisfied_by(&self, indexed: &Capability) -> bool {
        match (self, indexed) {
            (Capability::QuantileApprox(req), Capability::QuantileApprox(have)) => {
                sketch_algorithms_compatible(req, SketchAlgorithm::Kll, have, SketchAlgorithm::Kll)
            }
            // Cardinality has no inner handle; family match is total.
            (Capability::CardinalityApprox, Capability::CardinalityApprox) => true,
            // Top-k: the `Any` stand-in on EITHER side must be the
            // heap-bearing `CmsWithHeap`, not bare `Cms` — a bare
            // stand-in would let a heap-less available sketch wrongly
            // satisfy a top-k requirement (see `resolve_handle`'s doc).
            (Capability::FrequencyTopk(req), Capability::FrequencyTopk(have)) => {
                sketch_algorithms_compatible(
                    req,
                    SketchAlgorithm::CmsWithHeap,
                    have,
                    SketchAlgorithm::CmsWithHeap,
                )
            }
            // Bare frequency: any frequency-family handle works on the
            // available side — heap-LESS (CountMin / CountSketch) AND
            // heap-bearing (CmsWithHeap / CountSketchWithHeap) all answer
            // a point-frequency query (heap is additional info layered on
            // the sketch matrix). A heap-bearing `FrequencyTopk` indexed
            // capability ALSO satisfies a bare-frequency required capability.
            (Capability::FrequencyEstimate(req), Capability::FrequencyEstimate(have)) => {
                sketch_algorithms_compatible(req, SketchAlgorithm::Cms, have, SketchAlgorithm::Cms)
            }
            (Capability::FrequencyEstimate(req), Capability::FrequencyTopk(have)) => {
                sketch_algorithms_compatible(
                    req,
                    SketchAlgorithm::Cms,
                    have,
                    SketchAlgorithm::CmsWithHeap,
                )
            }
            // Exact-aggregation family: the agg_type must match
            // exactly OR be the single-pop ⇆ multi-pop equivalent. A
            // `MultipleSum` policy can serve a `Sum` query by
            // re-aggregating across keys; the `find_matching_policies`
            // group_by ⊆ policy_grouping_labels check is what
            // ultimately decides whether the re-aggregation is
            // semantically valid. The reverse direction (single-pop
            // serving multi-pop) is NOT allowed — the single-pop
            // policy has lost the key dimension and can't recover it.
            //
            // Cross-family ExactAgg combos are mostly non-satisfiable
            // (Sum vs MinMax, etc. are different operations) — except
            // Sum-family serving a required Increase/Rate capability,
            // which IS sound; see `sum_satisfies_increase`.
            (Capability::ExactAgg(req), Capability::ExactAgg(have)) => {
                req == have
                    || multi_pop_satisfies_single(*req, *have)
                    || sum_satisfies_increase(*req, *have)
            }
            _ => false,
        }
    }
}

/// Resolve optional query-side algorithm constraints and delegate family
/// compatibility to the ASAPPlanner algorithm taxonomy. `None` means any
/// algorithm in the capability's category; it is not a fake algorithm.
fn sketch_algorithms_compatible(
    required: &Option<SketchAlgorithm>,
    required_any_stand_in: SketchAlgorithm,
    available: &Option<SketchAlgorithm>,
    available_any_stand_in: SketchAlgorithm,
) -> bool {
    let required = required.as_ref().unwrap_or(&required_any_stand_in);
    let available = available.as_ref().unwrap_or(&available_any_stand_in);
    sketch_family_satisfied(required, available)
}

/// True when `available` is the multi-population equivalent of
/// `required`'s single-population variant — i.e. a `MultipleSum`
/// policy can serve a `Sum` query (via re-aggregation across keys),
/// `MultipleIncrease` can serve `Increase`, `MultipleMinMax` can
/// serve `MinMax`. Asymmetric: this returns `false` for the reverse
/// direction (single-pop can't recover keys that have been collapsed
/// away).
fn multi_pop_satisfies_single(required: AggregationType, available: AggregationType) -> bool {
    matches!(
        (required, available),
        (AggregationType::Sum, AggregationType::MultipleSum)
            | (AggregationType::Increase, AggregationType::MultipleIncrease)
            | (AggregationType::MinMax, AggregationType::MultipleMinMax)
    )
}

/// True when an available Sum-family accumulator can answer a required
/// Increase/Rate capability.
///
/// `rate()`/`increase()` PromQL both lower to a required
/// `Capability::ExactAgg(AggregationType::Increase)` (`capability_for`'s
/// `AggIntent::Rate | AggIntent::Increase` case, matching
/// `asap_aware_mapping::boundary::implementation_for`'s `SummaryKind::Increase |
/// SummaryKind::Rate` — ASAPController models Rate as its own summary
/// family). But this workspace's data plane has no storage kind distinct
/// from Sum for it: `evaluate_exact_agg_rate` (`sketch_reducer.rs`)
/// already reduces `Sum`/`MultipleSum`/`Increase`/`MultipleIncrease`
/// identically — all read as `Statistic::Sum` per window, then divided
/// by the coverage-aware elapsed range — so a Sum-registered sid's raw
/// per-window deltas answer a rate query exactly as well as an
/// Increase-registered one's. This is what lets counter metrics ingested
/// as plain `Sum` (not every ingest path distinguishes Increase from
/// Sum at registration time) still answer `rate(...)`/`increase(...)`.
///
/// Respects the same single/multi-population direction as
/// [`multi_pop_satisfies_single`]: a multi-pop available (`MultipleSum`)
/// can serve a single- or multi-pop required capability; a single-pop
/// available (`Sum`) can only serve a single-pop required one — it has
/// already lost the per-key breakdown a multi-pop required capability
/// would need.
///
/// Deliberately does NOT apply to a required `Capability::ExactAgg(Sum)`
/// (plain `sum_over_time`) — reconstructing Σ-of-cumulative-counter-
/// samples from per-window deltas is unsound (issue #301); that shape is
/// refused explicitly at the query-dispatch layer, not routed here.
fn sum_satisfies_increase(required: AggregationType, available: AggregationType) -> bool {
    matches!(
        (required, available),
        (AggregationType::Increase, AggregationType::Sum)
            | (AggregationType::Increase, AggregationType::MultipleSum)
            | (
                AggregationType::MultipleIncrease,
                AggregationType::MultipleSum
            )
    )
}

// ── AggIntent → Capability bridge ────────────────────────────────────────────

/// Map a semantic [`AggIntent`] to the ASAP-tier [`Capability`] that can
/// answer it. Returns `None` for intents that have no ASAP-tier sketch
/// (Sum / Min / Max / Avg / Rate / Increase / every archive-only intent
/// — see [`AggIntent::archive_only`]).
///
/// This is a runtime routing requirement, not a summary-selection rule.
/// ASAPPlanner owns legal implementations and candidate enumeration; this
/// adapter describes which already-deployed SID the data plane may read.
pub fn capability_for(intent: &AggIntent) -> Option<Capability> {
    if let Some(accuracy) = crate::planner_selection::as_frequency(intent) {
        return if is_exact(&accuracy) {
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
            Some(Capability::FrequencyEstimate(None))
        };
    }
    match intent {
        AggIntent::Sum { .. } => Some(Capability::ExactAgg(AggregationType::Sum)),
        AggIntent::Min { .. } | AggIntent::Max { .. } => {
            Some(Capability::ExactAgg(AggregationType::MinMax))
        }
        AggIntent::Increase | AggIntent::Rate => {
            Some(Capability::ExactAgg(AggregationType::Increase))
        }
        AggIntent::Quantile { accuracy, .. } if !is_exact(accuracy) => {
            Some(Capability::QuantileApprox(None))
        }
        AggIntent::Cardinality { accuracy, .. } if !is_exact(accuracy) => {
            Some(Capability::CardinalityApprox)
        }
        AggIntent::TopK { accuracy, .. } if !is_exact(accuracy) => {
            Some(Capability::FrequencyTopk(None))
        }
        AggIntent::Count { accuracy } if !is_exact(accuracy) => {
            Some(Capability::FrequencyEstimate(None))
        }
        _ => None,
    }
}

/// True iff the accuracy target forbids approximation. Wraps the match
/// so the call sites read as `if is_exact(accuracy) { ... }`.
fn is_exact(accuracy: &AccuracyTarget) -> bool {
    matches!(accuracy, AccuracyTarget::Exact)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── capability_for: AggIntent → Capability bridge ────────────────────

    #[test]
    fn capability_for_quantile_returns_quantile_approx() {
        let intent = AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::QuantileApprox(None))
        );
    }

    #[test]
    fn capability_for_quantile_exact_returns_none() {
        let intent = AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: AccuracyTarget::Exact,
        };
        // Exact quantiles must be answered by HashAgg/SortAgg — no
        // ASAP-tier sketch in this case.
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_cardinality_with_epsilon_returns_cardinality_approx() {
        let intent = AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(capability_for(&intent), Some(Capability::CardinalityApprox));
    }

    #[test]
    fn capability_for_cardinality_with_epsilon_delta_returns_cardinality_approx() {
        let intent = AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.001,
            },
        };
        assert_eq!(capability_for(&intent), Some(Capability::CardinalityApprox));
    }

    #[test]
    fn capability_for_cardinality_with_exact_returns_none() {
        let intent = AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_count_approximate_returns_frequency_estimate() {
        // Non-exact `Count` is a relaxed-accuracy `COUNT(*)` point query
        // (CMS), matching ASAPController's own `bind.rs::readout`
        // (`Count => SketchQuery::PointCount`) and this repo's
        // `BindCmsOnCount` rule — not `CardinalityApprox`/HLL, which is
        // `AggIntent::Cardinality`'s job (`distinct_over_time`/
        // `COUNT(DISTINCT)` never lower to `Count`).
        let intent = AggIntent::Count {
            accuracy: AccuracyTarget::Epsilon(0.01),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(None))
        );
    }

    #[test]
    fn capability_for_count_exact_routes_to_archive() {
        // `count_over_time` lowers to `Count{accuracy:Exact}`. The
        // PR #200/#201 follow-up briefly routed this to
        // `ExactAgg(Sum)`, but the data plane has no count
        // accumulator — `SumAccumulator` returns its `sum` for both
        // `Statistic::Sum` and `Statistic::Count`, so the result was
        // sum-of-values, not sample-count. Reverted to `None` (archive
        // routing) until a real `SumCountAccumulator` lands.
        let intent = AggIntent::Count {
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_sum_routes_to_exact_agg_sum() {
        // PR-6 follow-up: Sum routes to ASAP-tier ExactAgg(Sum) state.
        // Pre-follow-up this returned `None`.
        assert_eq!(
            capability_for(&AggIntent::Sum { col: None }),
            Some(Capability::ExactAgg(AggregationType::Sum))
        );
    }

    #[test]
    fn capability_for_avg_returns_none() {
        // Avg is exact at L3 — no ASAP-tier sketch substitutes for it
        // today (a sketch-bound `Avg` would fold onto `Quantile{q=0.5}`
        // only when the cost model allows the relaxation, which is a
        // follow-up).
        assert_eq!(capability_for(&AggIntent::Avg { col: None }), None);
    }

    #[test]
    fn capability_for_min_returns_exact_agg_minmax() {
        // Min/Max are exact, mergeable accumulators -- no approximation
        // needed at all -- matching ASAPController's own
        // `crates/plan/src/boundary.rs` treatment.
        assert_eq!(
            capability_for(&AggIntent::Min { col: None }),
            Some(Capability::ExactAgg(AggregationType::MinMax))
        );
    }

    #[test]
    fn capability_for_max_returns_exact_agg_minmax() {
        assert_eq!(
            capability_for(&AggIntent::Max { col: None }),
            Some(Capability::ExactAgg(AggregationType::MinMax))
        );
    }

    #[test]
    fn capability_for_rate_increase_route_to_exact_agg_increase() {
        // PR-6 follow-up: Rate and Increase route to ASAP-tier
        // ExactAgg(Increase) — the counter-reset-aware exact precompute.
        // Pre-follow-up this returned `None`.
        let exact_inc = Some(Capability::ExactAgg(AggregationType::Increase));
        assert_eq!(capability_for(&AggIntent::Rate), exact_inc);
        assert_eq!(capability_for(&AggIntent::Increase), exact_inc);
    }

    #[test]
    fn capability_for_topk_returns_frequency_topk_any() {
        // The analyzer no longer pins a concrete heap-bearing variant
        // for top-k — `Any` lets `handles_compatible_for_topk` accept
        // either `CmsWithHeap` or `CountSketchWithHeap` registered at
        // ingest time. Both variants share the heap envelope and the
        // reducer dispatches them identically.
        let intent = AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::Epsilon(0.05),
        };
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyTopk(None))
        );
    }

    #[test]
    fn capability_for_topk_exact_returns_none() {
        // Exact top-k must use HashAgg+Heap; no ASAP-tier sketch.
        let intent = AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::Exact,
        };
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn frequency_estimate_with_epsilon_returns_frequency_estimate_approx() {
        let intent = crate::planner_selection::frequency(AccuracyTarget::Epsilon(0.01), None);
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(None))
        );
    }

    #[test]
    fn frequency_estimate_with_epsilon_delta_returns_frequency_estimate_approx() {
        let intent = crate::planner_selection::frequency(
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.01,
                delta: 0.001,
            },
            None,
        );
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(None))
        );
    }

    #[test]
    fn frequency_estimate_with_exact_returns_none() {
        // Exact aggregation routes to archive (sketch fallback only
        // meaningful when raw counters aren't kept).
        let intent = crate::planner_selection::frequency(AccuracyTarget::Exact, None);
        assert_eq!(capability_for(&intent), None);
    }

    #[test]
    fn capability_for_archive_only_intents_return_none() {
        // Spot-check each archive-only variant, including a few added by
        // the Phase 1 IR merge. `Irate` is intentionally absent — folded
        // into `Rate` (see `agg_intent.rs` module docs); `rate`/`irate`
        // now share `capability_for`'s `Rate` arm.
        assert_eq!(capability_for(&AggIntent::Absent), None);
        assert_eq!(capability_for(&AggIntent::AbsentOverTime), None);
        assert_eq!(capability_for(&AggIntent::PresentOverTime), None);
        assert_eq!(capability_for(&AggIntent::Delta), None);
        assert_eq!(capability_for(&AggIntent::IDelta), None);
        assert_eq!(capability_for(&AggIntent::HistogramCount), None);
        assert_eq!(capability_for(&AggIntent::Group), None);
    }

    // ── Capability::is_satisfied_by ──────────────────────────────────────

    #[test]
    fn is_satisfied_by_any_wildcard_matches_concrete() {
        let required = Capability::QuantileApprox(None);
        let indexed_dd = Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch));
        let indexed_kll = Capability::QuantileApprox(Some(SketchAlgorithm::Kll));
        assert!(required.is_satisfied_by(&indexed_dd));
        assert!(required.is_satisfied_by(&indexed_kll));
    }

    #[test]
    fn is_satisfied_by_concrete_kind_matches_same_family() {
        // Intentional broadening vs. this module's pre-`sketch_family_satisfied`
        // behavior: a concrete required handle used to need an EXACT
        // available match (only `Any` unlocked family-level matching).
        // Delegating to `sketch_family_satisfied` makes family membership
        // the only thing that matters, matching enum-unification-plan.md §5's
        // table ("Kll or DDSketch | the other one | yes ... either
        // answers a quantile requirement not pinned to a concrete kind") —
        // that rule isn't conditioned on whether the requirement happened
        // to spell out `Any` or a concrete kind. Harmless in practice:
        // `capability_for` never emits a concrete `QuantileApprox` handle
        // (always `Any`), so this path is exercised only defensively.
        let required = Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch));
        let indexed_dd = Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch));
        let indexed_kll = Capability::QuantileApprox(Some(SketchAlgorithm::Kll));
        assert!(required.is_satisfied_by(&indexed_dd));
        assert!(required.is_satisfied_by(&indexed_kll));

        // Still cross-family-incompatible: a concrete quantile requirement
        // is never satisfied by a cardinality-family available handle.
        let indexed_hll = Capability::QuantileApprox(Some(SketchAlgorithm::Hll));
        assert!(!required.is_satisfied_by(&indexed_hll));
    }

    #[test]
    fn is_satisfied_by_cardinality_is_total() {
        let required = Capability::CardinalityApprox;
        let indexed = Capability::CardinalityApprox;
        assert!(required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_different_families_are_incompatible() {
        let required = Capability::QuantileApprox(None);
        let indexed = Capability::CardinalityApprox;
        assert!(!required.is_satisfied_by(&indexed));

        let required = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let indexed = Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch));
        assert!(!required.is_satisfied_by(&indexed));
    }

    #[test]
    fn is_satisfied_by_topk_handles_must_match() {
        let required = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let indexed_with_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let indexed_no_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::Cms));
        assert!(required.is_satisfied_by(&indexed_with_heap));
        assert!(!required.is_satisfied_by(&indexed_no_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_topk_rejects_heapless() {
        // Top-k REQUIRES a heap-bearing handle. Even when the available
        // capability declares itself as `FrequencyTopk(CountMin)` (an
        // ill-formed catalog entry), the satisfaction check must reject
        // it — top-k cannot enumerate items off a heap-less sketch.
        let required_any = Capability::FrequencyTopk(None);
        let required_concrete = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let indexed_heapless = Capability::FrequencyTopk(Some(SketchAlgorithm::Cms));
        let indexed_heapless_cs = Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketch));
        assert!(!required_any.is_satisfied_by(&indexed_heapless));
        assert!(!required_any.is_satisfied_by(&indexed_heapless_cs));
        assert!(!required_concrete.is_satisfied_by(&indexed_heapless));
    }

    #[test]
    fn is_satisfied_by_frequency_topk_any_matches_either_heap() {
        // `Any` required for top-k accepts either heap-bearing handle.
        let required = Capability::FrequencyTopk(None);
        let cms_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let cs_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketchWithHeap));
        assert!(required.is_satisfied_by(&cms_heap));
        assert!(required.is_satisfied_by(&cs_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_estimate_accepts_heap_bearing() {
        // Bare frequency point queries can be answered by ANY
        // frequency-family sketch — heap-less AND heap-bearing both work
        // (the heap is additional metadata; the underlying CMS / CS
        // matrix answers the point query either way).
        let required = Capability::FrequencyEstimate(None);
        let cms = Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms));
        let cs = Capability::FrequencyEstimate(Some(SketchAlgorithm::CountSketch));
        let cms_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        let cs_heap = Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketchWithHeap));
        assert!(required.is_satisfied_by(&cms));
        assert!(required.is_satisfied_by(&cs));
        assert!(required.is_satisfied_by(&cms_heap));
        assert!(required.is_satisfied_by(&cs_heap));
    }

    #[test]
    fn is_satisfied_by_frequency_estimate_rejects_non_frequency_family() {
        let required = Capability::FrequencyEstimate(None);
        // QuantileApprox / CardinalityApprox don't answer frequency.
        let q = Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch));
        let c = Capability::CardinalityApprox;
        // FrequencyEstimate with a non-frequency-family handle on the
        // available side is also rejected (defensive).
        let bad = Capability::FrequencyEstimate(Some(SketchAlgorithm::Hll));
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
        assert!(
            !required.is_satisfied_by(&Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch)))
        );
        assert!(!required.is_satisfied_by(&Capability::CardinalityApprox));
        assert!(
            !required.is_satisfied_by(&Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms)))
        );
        assert!(!required.is_satisfied_by(&Capability::FrequencyTopk(Some(
            SketchAlgorithm::CmsWithHeap
        ))));

        // And the reverse — a sketch-family required capability must
        // not match an ExactAgg-backed sid.
        let sketch_required = Capability::QuantileApprox(None);
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

    #[test]
    fn is_satisfied_by_sum_family_answers_required_increase() {
        // A Sum-registered sid's raw per-window deltas answer a
        // rate()/increase() query exactly as well as an Increase-
        // registered one's -- evaluate_exact_agg_rate (sketch_reducer.rs)
        // reduces both identically. See sum_satisfies_increase's doc.
        let required = Capability::ExactAgg(AggregationType::Increase);
        assert!(required.is_satisfied_by(&Capability::ExactAgg(AggregationType::Sum)));
        assert!(required.is_satisfied_by(&Capability::ExactAgg(AggregationType::MultipleSum)));

        let required_multi = Capability::ExactAgg(AggregationType::MultipleIncrease);
        assert!(required_multi.is_satisfied_by(&Capability::ExactAgg(AggregationType::MultipleSum)));
    }

    #[test]
    fn is_satisfied_by_sum_family_does_not_answer_required_multi_increase_from_single_sum() {
        // Same single/multi-population direction as multi_pop_satisfies_single:
        // a single-pop available (Sum) can't serve a multi-pop required
        // capability (MultipleIncrease) -- it already lost the per-key
        // breakdown a multi-pop caller needs.
        let required = Capability::ExactAgg(AggregationType::MultipleIncrease);
        assert!(!required.is_satisfied_by(&Capability::ExactAgg(AggregationType::Sum)));
    }

    #[test]
    fn is_satisfied_by_increase_does_not_answer_required_sum() {
        // The reverse direction is NOT sound: required ExactAgg(Sum) is
        // sum_over_time semantics (Σ of raw cumulative-counter samples),
        // which an Increase-registered sid's per-window deltas cannot
        // reconstruct (issue #301) -- refused explicitly at the
        // query-dispatch layer, not routed through here.
        let required = Capability::ExactAgg(AggregationType::Sum);
        assert!(!required.is_satisfied_by(&Capability::ExactAgg(AggregationType::Increase)));
    }

    // ── capability_for: ExactAgg dormancy ────────────────────────────────

    #[test]
    fn exact_agg_routing_covers_sum_rate_increase_only() {
        // `Capability::ExactAgg` routing covers the three intents the
        // data plane has a real accumulator for: `Sum` (SumAccumulator)
        // and `Rate` / `Increase` (IncreaseAccumulator).
        assert_eq!(
            capability_for(&AggIntent::Sum { col: None }),
            Some(Capability::ExactAgg(AggregationType::Sum))
        );
        assert_eq!(
            capability_for(&AggIntent::Rate),
            Some(Capability::ExactAgg(AggregationType::Increase))
        );
        assert_eq!(
            capability_for(&AggIntent::Increase),
            Some(Capability::ExactAgg(AggregationType::Increase))
        );
        // `Count{Exact}` (count_over_time) and `Avg` both need a real
        // count accumulator that doesn't exist yet — they route to
        // archive until `SumCountAccumulator` lands.
        assert_eq!(
            capability_for(&AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            }),
            None
        );
        assert_eq!(capability_for(&AggIntent::Avg { col: None }), None);
    }

    #[test]
    fn count_sketch_with_heap_handle_round_trips() {
        // `CountSketchWithHeap` is the CountSketch counterpart to
        // `CmsWithHeap`. Construct a `FrequencyTopk` capability around
        // it and verify it satisfies an `Any`-required top-k.
        let cap = Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketchWithHeap));
        let required = Capability::FrequencyTopk(None);
        assert!(required.is_satisfied_by(&cap));
        // And the concrete-against-concrete (same handle) case matches.
        let required_concrete =
            Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketchWithHeap));
        assert!(required_concrete.is_satisfied_by(&cap));
        // Intentional broadening vs. this module's pre-`sketch_family_satisfied`
        // behavior: CMS and CountSketch are the SAME frequency family in
        // `physical::post_asap::matcher`'s reference rule table (both bare and
        // heap-bearing — see its
        // `cms_and_count_sketch_are_the_same_frequency_family` test), so a
        // `CmsWithHeap` requirement IS now satisfied by a
        // `CountSketchWithHeap` available (both heap-bearing, same
        // family), not just an exact-handle match. Harmless in practice:
        // `capability_for` always emits `Any` for `FrequencyTopk`, never a
        // concrete handle, so this exact combination never arises from a
        // real query.
        let required_cms = Capability::FrequencyTopk(Some(SketchAlgorithm::CmsWithHeap));
        assert!(required_cms.is_satisfied_by(&cap));
        // Still cross-family-incompatible: a heap-bearing requirement is
        // never satisfied by a bare (heap-less) available handle.
        let bare = Capability::FrequencyTopk(Some(SketchAlgorithm::Cms));
        assert!(!required_cms.is_satisfied_by(&bare));
    }

    // ── OuterAgg fold semantics ──────────────────────────────────────────
    //
    // Pin the fold-by-operator dispatch shape the engine reads off the
    // typed `ASAPTierCandidate.outer_agg` field. The identity case
    // (single-value group) is the load-bearing assertion for asap's
    // per-zone-DDSketch shape: `max by (zone) (quantile_over_time(...))`
    // produces one row per zone, and `OuterAgg::Max.fold([x]) == x` —
    // the wrapper is a no-op for already-grouped inner results.

    #[test]
    fn outer_agg_default_is_none() {
        assert_eq!(OuterAgg::default(), OuterAgg::None);
        assert!(!OuterAgg::default().is_some());
    }

    #[test]
    fn outer_agg_none_carries_no_by_labels() {
        assert!(OuterAgg::None.by_labels().is_empty());
    }

    #[test]
    fn outer_agg_max_fold_single_value_is_identity() {
        // Issue #296 identity case: inner already emits one row per
        // by-group; the outer max fold must return that row unchanged.
        let v = OuterAgg::Max(vec!["zone".to_string()])
            .fold(&[42.5])
            .unwrap();
        assert_eq!(v, 42.5);
    }

    #[test]
    fn outer_agg_min_fold_single_value_is_identity() {
        let v = OuterAgg::Min(vec!["zone".to_string()])
            .fold(&[42.5])
            .unwrap();
        assert_eq!(v, 42.5);
    }

    #[test]
    fn outer_agg_avg_fold_single_value_is_identity() {
        let v = OuterAgg::Avg(vec!["zone".to_string()])
            .fold(&[42.5])
            .unwrap();
        assert_eq!(v, 42.5);
    }

    #[test]
    fn outer_agg_max_fold_multi_picks_largest() {
        let v = OuterAgg::Max(vec![]).fold(&[1.0, 5.0, 3.0]).unwrap();
        assert_eq!(v, 5.0);
    }

    #[test]
    fn outer_agg_min_fold_multi_picks_smallest() {
        let v = OuterAgg::Min(vec![]).fold(&[1.0, 5.0, 3.0]).unwrap();
        assert_eq!(v, 1.0);
    }

    #[test]
    fn outer_agg_avg_fold_multi_is_mean() {
        let v = OuterAgg::Avg(vec![]).fold(&[1.0, 5.0, 3.0]).unwrap();
        assert!((v - 3.0).abs() < 1e-9);
    }

    #[test]
    fn outer_agg_count_fold_returns_cardinality() {
        let v = OuterAgg::Count(vec![]).fold(&[1.0, 5.0, 3.0]).unwrap();
        assert_eq!(v, 3.0);
    }

    #[test]
    fn outer_agg_group_fold_returns_one() {
        let v = OuterAgg::Group(vec![]).fold(&[1.0, 5.0, 3.0]).unwrap();
        assert_eq!(v, 1.0);
    }

    #[test]
    fn outer_agg_stddev_fold_multi_is_population_stddev() {
        // population stddev of [1,2,3,4,5] is sqrt(2) ≈ 1.4142
        let v = OuterAgg::Stddev(vec![])
            .fold(&[1.0, 2.0, 3.0, 4.0, 5.0])
            .unwrap();
        assert!((v - 2.0_f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn outer_agg_stdvar_fold_multi_is_population_variance() {
        let v = OuterAgg::Stdvar(vec![])
            .fold(&[1.0, 2.0, 3.0, 4.0, 5.0])
            .unwrap();
        assert!((v - 2.0).abs() < 1e-9);
    }

    #[test]
    fn outer_agg_empty_input_returns_none() {
        assert_eq!(OuterAgg::Max(vec![]).fold(&[]), None);
        assert_eq!(OuterAgg::Avg(vec![]).fold(&[]), None);
        assert_eq!(OuterAgg::Count(vec![]).fold(&[]), None);
    }
}
