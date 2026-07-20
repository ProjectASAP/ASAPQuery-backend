//! Query-side capability tag + the semantic `AggIntent` → `Capability`
//! dispatch bridge.
//!
//! Step 2a of the architectural refactor originally consolidated four
//! overlapping capability tables into this module. The performance /
//! cost-model half of that consolidation — [`SketchCapability`] /
//! `SupportedIntent` / `default_capability_table` / `load_capability_overrides`
//! — moved to `crate::optimizer::cost::sketch_capability` (Stage 4 of the
//! `sketch_algebra` re-layering): it's a cost-model concern read by the
//! optimizer and physical planner, not L4 IR. What's left here:
//!
//! - [`Capability`] / [`SketchKindHandle`] — query-side capability tag,
//!   used by the ASAP-tier reducer in `asap-query-engine` to dispatch
//!   PromQL → per-Capability sketch evaluation.
//! - [`capability_for`] — the **semantic** intent → ASAP-tier dispatch
//!   bridge. PromQL → intent_algebra::lower → `AggIntent` → (this fn) →
//!   `Capability`. The ASAP-tier analyzer is now a thin facade around
//!   this single function; PromQL function-name string matching lives
//!   only inside the lowerer.

#![allow(dead_code)]

use crate::intent_algebra::agg_intent::AggIntent;
use crate::types_v2::AccuracyTarget;
use asap_types::AggregationType;

// ── Query-side capability tag ────────────────────────────────────────────────

/// Warm-tier capability tag. One variant per logical query family the
/// ASAP tier can answer. The inner [`SketchKindHandle`] is the
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
/// Background: the lowerer collapses every `AggFunc` in
/// `{Sum, Rate, Increase, Delta}` onto a single `AggIntent::Sum`, which
/// `capability_for` then maps to `Capability::ExactAgg(Sum)`. That
/// collapse erases the rate-vs-plain distinction the engine needs to
/// decide between the plain ExactAgg reducer and the rate-divisor
/// reducer (`evaluate_exact_agg_rate`). Before this enum landed the
/// engine re-walked the raw PromQL via a `query_contains_rate_call`
/// helper to recover the distinction; that was a lossy-lowering smell.
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
/// deltas, but they ALL lower to a single `Capability::ExactAgg(Sum)`
/// (the `AggIntent::Sum` collapse erases the function name). Before
/// #301 the engine only distinguished `Rate` from everything else, so
/// `sum`, `sum_over_time`, `increase`, and instant-sum all hit the same
/// reducer path and returned the same (wrong) number. This enum carries
/// the function distinction the engine needs to dispatch correctly:
///
/// | Variant       | PromQL                       | Engine dispatch                                   |
/// |---------------|------------------------------|---------------------------------------------------|
/// | `Plain`       | `sum(c)` / `sum by (..) (c)` | accumulate ALL windows → cumulative-since-storage |
/// | `Rate`        | `rate(c[r])` / `irate(c[r])` | Σ deltas in `[t-r,t]` ÷ min(r, coverage)          |
/// | `Increase`    | `increase(c[r])`             | Σ deltas in `[t-r,t]` (one cumulative number)     |
/// | `SumOverTime` | `sum_over_time(c[r])`        | capability-miss → archive (can't reconstruct)     |
///
/// The taxonomy lives on `OuterFn` (not the `Capability` algebra) so the
/// sid-matching half stays a pure `ExactAgg(Sum)` predicate — the
/// function distinction is a query-evaluation concern, not a stored-state
/// one.
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

/// Compact, hashable handle for sketch implementation choice. Mirrors
/// `asap_sketch::SummaryKind` but adds the `Any` query-side wildcard
/// (not a sketch family — a dispatch hint).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchKindHandle {
    DDSketch,
    Kll,
    Hll,
    CountSketch,
    CountMin,
    /// CMS paired with a Misra-Gries / heavy-hitter heap. Distinct from
    /// `CountMin` because vanilla CMS carries no item universe — the
    /// heap is what lets the ASAP-tier reducer enumerate top-k items
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
    /// index). The backend's ASAP-tier hook reads both and routes the
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
            // Cross-family ExactAgg combos (Sum vs MinMax, etc.)
            // remain non-satisfiable: they're different operations.
            (Capability::ExactAgg(req), Capability::ExactAgg(have)) => {
                req == have || multi_pop_satisfies_single(*req, *have)
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

/// `Any` required for top-k means "any heap-bearing handle"; concrete
/// required must match exactly.
fn handles_compatible_for_topk(required: SketchKindHandle, available: SketchKindHandle) -> bool {
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

// ── AggIntent → Capability bridge ────────────────────────────────────────────

/// Map a semantic [`AggIntent`] to the ASAP-tier [`Capability`] that can
/// answer it. Returns `None` for intents that have no ASAP-tier sketch
/// (Sum / Min / Max / Avg / Rate / Increase / every archive-only intent
/// — see [`AggIntent::archive_only`]).
///
/// This is the **single bridge** between the L3 intent vocabulary and
/// the L4/Q1 sketch-capability vocabulary. Both the ASAP-tier analyzer
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
/// | `Min` / `Max` | `Some(ExactAgg(MinMax))` — exact mergeable accumulator, no approximation needed |
/// | `Cardinality { accuracy }` (accuracy not `Exact`) | `Some(CardinalityApprox)` |
/// | `Cardinality { accuracy: Exact }` | `None` |
/// | `Count { accuracy: Exact }` | `None` — no count accumulator exists yet; routes to archive |
/// | `Count { accuracy }` (accuracy not `Exact`) | `Some(FrequencyEstimate(Any))` — bare per-item frequency point-query (CMS) |
/// | `TopK { k, accuracy }` (accuracy not `Exact`) | `Some(FrequencyTopk(CmsWithHeap))` |
/// | `Frequency { accuracy }` (accuracy not `Exact`) | `Some(FrequencyEstimate(Any))` |
/// | `Frequency { accuracy: Exact }` | `None` (exact aggregation; route to archive) |
/// | `Sum` | `Some(ExactAgg(Sum))` — ASAP-tier exact precompute (PR-6 follow-up) |
/// | `Rate` / `Increase` | `Some(ExactAgg(Increase))` — counter-reset-aware precompute (PR-6 follow-up) |
/// | `Avg` | `None` — needs cross-policy join (Sum + Count); follow-up |
/// | Every archive-only intent | `None` |
pub fn capability_for(intent: &AggIntent) -> Option<Capability> {
    if let Some(accuracy) = crate::intent_algebra::as_frequency(intent) {
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
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
        };
    }
    match intent {
        AggIntent::Quantile { accuracy, .. } => {
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::QuantileApprox(SketchKindHandle::Any))
            }
        }
        AggIntent::Cardinality { accuracy, .. } => {
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::CardinalityApprox)
            }
        }
        AggIntent::Count { accuracy } => {
            // Count is the legacy bridge — `count_over_time` lowers to
            // `Count{accuracy:Exact}` (exact counter, no sketch).
            //
            // Exact count routes to archive (`None`). The PR #200/#201
            // follow-up flipped this to `ExactAgg(Sum)` on the theory
            // "count = sum-of-1s" — but the data plane has no count
            // accumulator. `SumAccumulator` only tracks `sum: f64` and
            // its `query` returns `self.sum` for BOTH `Statistic::Sum`
            // and `Statistic::Count`, so a `count_over_time` query
            // matched against a `Sum` policy returns the sum of the
            // sample VALUES, not the count of samples. Reverted here
            // until a real `SumCountAccumulator` lands (the
            // temporal/spatial-split work) — archive counts correctly
            // in the meantime.
            //
            // Non-exact `Count` is a relaxed-accuracy `COUNT(*) per
            // group` — a bare per-item frequency point-query, matching
            // ASAPController's own `crates/plan/src/bind.rs::readout`
            // (`AggIntent::Count => SketchQuery::PointCount`) and this
            // repo's own `BindCmsOnCount` rule (CMS, no top-k heap).
            // Previously mapped to `CardinalityApprox`/HLL under the
            // theory that non-exact `Count` meant "distinct count" —
            // but `distinct_over_time`/`COUNT(DISTINCT)` always lower to
            // `AggIntent::Cardinality`, never to `Count`, so that
            // premise never had a real caller; `AggIntent::Count`
            // itself is unreachable via this repo's own PromQL frontend
            // today regardless of accuracy (`lower.rs` only ever
            // constructs `Count{Exact}` or the `Extension`-based
            // `Frequency` intent), so this only affects future callers
            // (e.g. a SQL frontend) — fixed here for consistency with
            // `rules::dispatch` rather than because it changes any
            // query routing today.
            if is_exact(accuracy) {
                None
            } else {
                Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
            }
        }
        AggIntent::TopK { accuracy, .. } => {
            if is_exact(accuracy) {
                // Exact top-k must use HashAgg+Heap; no ASAP-tier sketch.
                None
            } else {
                // Top-k is intrinsically heavy-hitter — only heap-bearing
                // handles can enumerate the items. The analyzer doesn't
                // care which heap-bearing variant answers (CmsWithHeap or
                // CountSketchWithHeap both work — the reducer dispatches
                // both through `decode_cms_with_heap_from_msgpack` and
                // produces top-k items either way). Return `Any` so
                // `is_satisfied_by`'s `handles_compatible_for_topk`
                // wildcard accepts whichever variant the ingest tier
                // chose to register.
                Some(Capability::FrequencyTopk(SketchKindHandle::Any))
            }
        }
        // ── ExactAgg (PR-6 follow-up) ────────────────────────────────
        // These intents previously returned `None` and routed to the
        // archive engine. Now that the data plane carries
        // `Capability::ExactAgg(agg_type)` on ExactAgg-backed sids,
        // the analyzer can match them to ASAP-tier exact-precompute
        // state instead. `is_satisfied_by` checks `agg_type` equality
        // structurally — a sid registered as `ExactAgg(Sum)` only
        // satisfies a required `ExactAgg(Sum)`.
        AggIntent::Sum { .. } => Some(Capability::ExactAgg(AggregationType::Sum)),
        AggIntent::Rate | AggIntent::Increase => {
            Some(Capability::ExactAgg(AggregationType::Increase))
        }
        // Min / Max are exact, mergeable accumulators — comparing two
        // partial min/maxes is exact by construction, no approximation
        // needed at all. Previously routed through `QuantileApprox`
        // (DDSketch/KLL answer min = quantile(0), max = quantile(1) —
        // a strictly worse, lossy answer when an exact accumulator is
        // just as cheap) on the theory that a dedicated `MinMax` bind
        // rule wasn't worth the cost-model churn versus the
        // already-existing quantile-sketch path. That premise didn't
        // hold: `bind_kll_quantile`/`bind_ddsketch_quantile` only ever
        // matched `AggIntent::Quantile`, never `Min`/`Max`, so no rule
        // actually implemented the promised quantile-sketch coverage —
        // and matches ASAPController's own `crates/plan/src/boundary.rs`
        // (`Min`/`Max` are exact mergeable accumulators, same tier as
        // `Sum`/`Rate`/`Increase`). The data plane already has a fully
        // wired `MinMaxAccumulator`/`AggregationType::MinMax`, so this
        // isn't new infrastructure — see `bind_exact_agg.rs`'s matching
        // `AggIntent::Min | AggIntent::Max` arm.
        AggIntent::Min { .. } | AggIntent::Max { .. } => {
            Some(Capability::ExactAgg(AggregationType::MinMax))
        }
        // ── Avg / StdDev / Variance: still no ASAP-tier substitute ────
        // Avg = Sum / Count, which needs two separate ExactAgg policies
        // (one for Sum, one for Count) joined at query time. The L4
        // binder doesn't yet emit that pattern, so capability_for keeps
        // these on the archive path for now. Follow-up.
        AggIntent::Avg { .. } | AggIntent::StdDev { .. } | AggIntent::Variance { .. } => None,
        // Archive-only intents — never bind to a ASAP-tier capability;
        // routed to the cold tier (Gorilla / Thanos). Includes every
        // intent added by the Phase 1 IR merge (none has a `Bind*` rule
        // yet) plus the pre-existing archive-only set. `Irate` is
        // intentionally absent — folded into `Rate` above (see
        // `agg_intent.rs` module docs).
        AggIntent::Absent
        | AggIntent::AbsentOverTime
        | AggIntent::PresentOverTime
        | AggIntent::Delta
        | AggIntent::Deriv
        | AggIntent::PredictLinear { .. }
        | AggIntent::DoubleExpSmoothing { .. }
        | AggIntent::IDelta
        | AggIntent::Resets
        | AggIntent::Changes
        | AggIntent::HistogramCount
        | AggIntent::HistogramSum
        | AggIntent::HistogramAvg
        | AggIntent::HistogramStdDev
        | AggIntent::HistogramStdVar
        | AggIntent::HistogramFraction { .. }
        | AggIntent::HistogramQuantile { .. }
        | AggIntent::Math(_)
        | AggIntent::TimeFn(_)
        | AggIntent::Group
        | AggIntent::CountValues { .. }
        | AggIntent::LastOverTime
        | AggIntent::FirstOverTime
        | AggIntent::MadOverTime
        | AggIntent::TsOfMinOverTime
        | AggIntent::TsOfMaxOverTime
        | AggIntent::TsOfFirstOverTime
        | AggIntent::TsOfLastOverTime => None,
        // Unrecognized Extension (not the Frequency one, guarded above) -- no
        // binding exists for a shape core cannot even see into.
        AggIntent::Extension { .. } => None,
    }
}

/// True iff the accuracy target forbids approximation. Wraps the match
/// so the call sites read as `if is_exact(accuracy) { ... }`.
fn is_exact(accuracy: &AccuracyTarget) -> bool {
    matches!(accuracy, AccuracyTarget::Exact)
}

// ── Sketch-family error bounds ───────────────────────────────────────────────
//
// These two helpers were originally defined in `controller/src/algebra/expr.rs`
// (now `controller/src/intent_algebra/relational.rs`). The 2026-05
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
            col: None,
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
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
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
            Some(Capability::FrequencyTopk(SketchKindHandle::Any))
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
        let intent = crate::intent_algebra::frequency(AccuracyTarget::Epsilon(0.01));
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
        );
    }

    #[test]
    fn frequency_estimate_with_epsilon_delta_returns_frequency_estimate_approx() {
        let intent = crate::intent_algebra::frequency(AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.001,
        });
        assert_eq!(
            capability_for(&intent),
            Some(Capability::FrequencyEstimate(SketchKindHandle::Any))
        );
    }

    #[test]
    fn frequency_estimate_with_exact_returns_none() {
        // Exact aggregation routes to archive (sketch fallback only
        // meaningful when raw counters aren't kept).
        let intent = crate::intent_algebra::frequency(AccuracyTarget::Exact);
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
        assert!(!required.is_satisfied_by(&Capability::QuantileApprox(SketchKindHandle::DDSketch)));
        assert!(!required.is_satisfied_by(&Capability::CardinalityApprox));
        assert!(
            !required.is_satisfied_by(&Capability::FrequencyEstimate(SketchKindHandle::CountMin))
        );
        assert!(
            !required.is_satisfied_by(&Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
        );

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
        let cap = Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        let required = Capability::FrequencyTopk(SketchKindHandle::Any);
        assert!(required.is_satisfied_by(&cap));
        // And the concrete-against-concrete must match exactly.
        let required_concrete = Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap);
        assert!(required_concrete.is_satisfied_by(&cap));
        // A different concrete heap-bearing handle must NOT match.
        let required_cms = Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap);
        assert!(!required_cms.is_satisfied_by(&cap));
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
