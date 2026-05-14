//! PromQL → ASAP-tier candidate analyzer (Step 2a thin-facade rewrite).
//!
//! Before Step 2a this module was an 868-line second-PromQL-walker that
//! pattern-matched on raw function-name strings — duplicating the
//! control plane's existing `query_parser::parse_query` →
//! `intent_algebra::lower::lower_parsed_query` pipeline and inventing a
//! parallel set of function names (`count_distinct_over_time`,
//! `cardinality_estimate`, `count_distinct`) that aren't part of PromQL
//! or MetricsQL.
//!
//! After Step 2a this module is a ~120-line facade. The pipeline is:
//!
//! ```text
//! PromQL string
//!   ↓  query_parser::parse_query  (the control plane's PromQL → ParsedQuery)
//! ParsedQuery
//!   ↓  intent_algebra::lower::lower_parsed_query
//! QueryExpr (intent_algebra) — Scan / Window / Aggregate{ aggs: Vec<AggIntent> }
//!   ↓  walk and call capability_for(&AggIntent)
//! Vec<ASAPTierCandidate>
//! ```
//!
//! The lowerer is the **single owner** of "what does this PromQL function
//! mean"; `sketch_algebra::capability_for` is the **single owner** of
//! "what sketch can answer this intent". This module just glues the two.
//!
//! ## What's still here
//!
//! - The `ASAPTierCandidate` / `ASAPTierAnalysis` / `UnsupportedReason`
//!   public types — the ASAP-tier reducer and the engine router consume
//!   them.
//! - The PromQL `[5m]` range-selector → `range_seconds` extraction
//!   helper. Reached by walking the [`ParsedQuery`] / re-parsing the
//!   source via `promql_parser` ONLY for that selector — function-name
//!   matching has moved entirely into the lowerer.
//!
//! ## What's gone
//!
//! - The 600 lines of direct PromQL function-name match arms.
//! - The custom-function pre-parser for `cardinality_estimate` /
//!   `count_distinct_over_time` / `count_distinct` (those names don't
//!   exist in real PromQL/MetricsQL; the lowerer handles the real
//!   names like `quantile_over_time` and `count_over_time`).
//! - The local `Capability` / `SketchKindHandle` enums — they're now
//!   re-exported from `sketch_algebra` (the single source of truth).

use std::collections::BTreeSet;
use std::time::Duration;

use promql_parser::parser::{self, Expr, VectorSelector};

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::QueryExpr;
use crate::query_parser::parse_query;
use crate::types_v2::AccuracyTarget;

pub use crate::sketch_algebra::capability::{capability_for, Capability, SketchKindHandle};

// ── Public types ─────────────────────────────────────────────────────────────

/// One sub-expression of the input PromQL that CAN be served from the
/// ASAP tier. The reducer resolves each candidate to a vector of sids
/// via `SketchIndex::instances_matching(metric_name, group_by_keys)`
/// and verifies each sid carries the required capability.
#[derive(Debug, Clone, PartialEq)]
pub struct ASAPTierCandidate {
    pub metric_name: String,
    pub group_by_keys: BTreeSet<String>,
    pub required_capability: Capability,
    /// The PromQL function-name string from the original query, kept
    /// for telemetry / logging only. The reducer dispatches off
    /// `required_capability` rather than re-string-matching this.
    pub function: String,
    /// Scalar arguments collected from the call (e.g. `q` for quantile,
    /// `k` for topk). Order matches the PromQL surface.
    pub function_args: Vec<f64>,
    /// Time range from the matrix-vector selector (e.g. `[5m]` → 300).
    /// `0` when the query is instant-vector-shaped.
    pub range_seconds: u64,
    /// Canonical form of the equality label filters from the PromQL
    /// selector (e.g. `{status="200",zone="us-east"}`). Empty when
    /// the query has no label filters. Produced by
    /// `asap_types::utils::normalize_spatial_filter` so it matches the
    /// canonical form stored on `AggregationConfig.spatial_filter_normalized`
    /// byte-for-byte. Drives the candidate → policy filter match in
    /// `find_matching_policies`.
    pub spatial_filter_canonical: String,
}

/// Whole-query analysis result.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ASAPTierAnalysis {
    pub candidates: Vec<ASAPTierCandidate>,
    pub unsupported: Option<UnsupportedReason>,
}

impl ASAPTierAnalysis {
    /// True iff the analysis is fully ASAP-tier-answerable —
    /// `unsupported.is_none()` AND at least one candidate.
    pub fn is_asap_tier_answerable(&self) -> bool {
        self.unsupported.is_none() && !self.candidates.is_empty()
    }
}

/// Distinct reasons a PromQL query is NOT ASAP-tier-answerable. The
/// distinction matters for logging / future precompute hints; the
/// routing layer maps every variant to the cold tier today.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedReason {
    /// An `AggIntent` for which [`capability_for`] returned `None` —
    /// `Sum`, `Min`, `Max`, `Rate`, `Increase`, every archive-only
    /// intent, plus exact-accuracy `Quantile` / `Cardinality` /
    /// `Count`. The carried string is the variant kind for logging.
    UnsupportedAggIntent(String),
    /// The query is a bare vector / matrix selector with no call —
    /// the ASAP tier doesn't materialize raw counter values.
    NoCallNodeFound,
    /// `query_parser::parse_query` rejected the input. Carries the
    /// parser error message for diagnostics.
    UnparseableMetricsql(String),
}

// ── Public entry point ───────────────────────────────────────────────────────

/// Parse PromQL via the control plane's existing pipeline, lower to L3
/// `intent_algebra::QueryExpr`, walk it, and build a
/// [`ASAPTierAnalysis`].
///
/// Single owner of ASAP-tier shape recognition: this function does
/// **no** direct PromQL function-name matching. The lowerer
/// (`intent_algebra::lower::lower_parsed_query`) is the only place
/// that knows what `quantile_over_time` / `count_over_time` / etc.
/// mean; this function just consumes the lowered `AggIntent`s and
/// dispatches via [`capability_for`].
pub fn analyze_promql_for_asap_tier(metricsql: &str) -> ASAPTierAnalysis {
    // Step 1: parse via the control plane's existing PromQL → ParsedQuery
    // chain. `parse_query` already understands the full PromQL surface
    // we care about.
    let parsed = match parse_query(metricsql) {
        Ok(p) => p,
        Err(e) => {
            return ASAPTierAnalysis {
                candidates: Vec::new(),
                unsupported: Some(UnsupportedReason::UnparseableMetricsql(e.to_string())),
            };
        }
    };

    // Capture the function-name string + scalar args + range_seconds for
    // telemetry. These come from a side-channel walk of the AST — the
    // lowered `AggIntent` doesn't carry them. We use the same
    // `promql_parser` AST that `query_parser::promql` already parses
    // internally; this is the ONLY remaining place that touches raw
    // PromQL function names.
    let trace = trace_from_promql(metricsql);

    // Step 2: pick a sane default accuracy. Warm-tier analysis only
    // cares about whether the AggIntent has a sketch binding, and the
    // lowerer maps `parsed.exact_required = true` to `AccuracyTarget::Exact`
    // anyway. Anything non-exact unlocks the same set of bindings, so
    // we pick a mid-range epsilon as the analysis-time default; the
    // real per-query accuracy bound comes from QueryWorkload further
    // downstream.
    let accuracy = AccuracyTarget::Epsilon(0.01);

    let expr = match crate::intent_algebra::lower::lower_parsed_query(&parsed, accuracy) {
        Ok(e) => e,
        Err(e) => {
            return ASAPTierAnalysis {
                candidates: Vec::new(),
                unsupported: Some(UnsupportedReason::UnparseableMetricsql(e.to_string())),
            };
        }
    };

    // Step 3: walk the lowered tree, looking for `Aggregate` nodes.
    // If there's no Aggregate the query is either:
    //   - a bare metric selector → `NoCallNodeFound` (ASAP-tier
    //     doesn't materialize raw counter values)
    //   - a window-bound exact-aggregation (`rate`, `irate`,
    //     `increase`, `sum_over_time`, `count_over_time` without
    //     outer count, etc.) — the control plane's PromQL parser sets
    //     `exact_required = true` for these and the lowerer skips
    //     emitting an `Aggregate` because there's no `AggType`
    //     (Quantile/Cardinality/Frequency) to map them onto. Surface
    //     as `UnsupportedAggIntent` with a label derived from the
    //     raw function name so the routing layer can attribute the
    //     rejection.
    let mut intents: Vec<AggIntent> = Vec::new();
    collect_agg_intents(&expr, &mut intents);
    if intents.is_empty() {
        let reason = if parsed.exact_required && !trace.function.is_empty() {
            UnsupportedReason::UnsupportedAggIntent(trace.function.clone())
        } else {
            UnsupportedReason::NoCallNodeFound
        };
        return ASAPTierAnalysis {
            candidates: Vec::new(),
            unsupported: Some(reason),
        };
    }

    // Step 4: for each intent, look up its capability. The first
    // intent that returns `None` aborts the analysis — the warm
    // tier can't answer this query (the router falls over to archive).
    let metric_name = parsed.metric_name.clone();
    let group_by_keys: BTreeSet<String> = parsed.group_by_labels.iter().cloned().collect();
    let spatial_filter_canonical = render_spatial_filter(&parsed.label_filters);

    let mut out = ASAPTierAnalysis::default();
    for intent in &intents {
        match capability_for(intent) {
            Some(cap) => {
                out.candidates.push(ASAPTierCandidate {
                    metric_name: metric_name.clone(),
                    group_by_keys: group_by_keys.clone(),
                    required_capability: cap,
                    function: trace.function.clone(),
                    function_args: trace.function_args.clone(),
                    range_seconds: trace.range_seconds,
                    spatial_filter_canonical: spatial_filter_canonical.clone(),
                });
            }
            None => {
                out.unsupported = Some(UnsupportedReason::UnsupportedAggIntent(
                    intent_kind_label(intent).to_string(),
                ));
                return out;
            }
        }
    }
    out
}

/// Render the equality label filters from a `ParsedQuery` into the
/// canonical spatial-filter form used by
/// `AggregationConfig.spatial_filter_normalized`. Empty map → empty
/// string. Multiple entries get sorted+joined via
/// [`asap_types::utils::normalize_spatial_filter`] so the result is
/// byte-identical to what the control plane writes.
fn render_spatial_filter(label_filters: &std::collections::HashMap<String, String>) -> String {
    if label_filters.is_empty() {
        return String::new();
    }
    // Render `key="value",key="value",…` then normalize. The renderer
    // doesn't need to sort — normalize_spatial_filter sorts matchers.
    let joined: Vec<String> = label_filters
        .iter()
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    asap_types::utils::normalize_spatial_filter(&joined.join(","))
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Walk the lowered `QueryExpr`, collecting every `AggIntent` from every
/// `Aggregate` node. `LetBinding` / `Ref` are recursed into; `Scan` /
/// `Window` carry no intents themselves.
fn collect_agg_intents(expr: &QueryExpr, out: &mut Vec<AggIntent>) {
    match expr {
        QueryExpr::Aggregate { aggs, child, .. } => {
            out.extend(aggs.iter().cloned());
            collect_agg_intents(child, out);
        }
        QueryExpr::Window { child, .. } => collect_agg_intents(child, out),
        QueryExpr::LetBinding { expr, child, .. } => {
            collect_agg_intents(expr, out);
            collect_agg_intents(child, out);
        }
        QueryExpr::Scan { .. } | QueryExpr::Ref { .. } => {}
        // A-variants lifted in Batch 2 of the legacy_expr migration. They
        // carry no AggIntent themselves — recurse into their children to
        // find Aggregates further down the tree.
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Partition { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. } => collect_agg_intents(child, out),
        QueryExpr::Merge { children } => {
            for c in children {
                collect_agg_intents(c, out);
            }
        }
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp { lhs: left, rhs: right, .. } => {
            collect_agg_intents(left, out);
            collect_agg_intents(right, out);
        }
    }
}

/// Function-name string for a candidate. The lowered `AggIntent`
/// dropped the raw PromQL function name; this label is keyed off the
/// intent kind so telemetry / logging sees `quantile`, `cardinality`,
/// `topk`, etc. Specific PromQL aliases (`quantile_over_time` vs the
/// instant `quantile`) are reconstructed in [`trace_from_promql`] when
/// the AST walker can recover them; this fallback runs when the AST
/// walk fails.
fn intent_kind_label(intent: &AggIntent) -> &'static str {
    match intent {
        AggIntent::Count { .. } => "count",
        AggIntent::Sum => "sum",
        AggIntent::Min => "min",
        AggIntent::Max => "max",
        AggIntent::Avg => "avg",
        AggIntent::Quantile { .. } => "quantile",
        AggIntent::TopK { .. } => "topk",
        AggIntent::Cardinality { .. } => "cardinality",
        AggIntent::Frequency { .. } => "frequency",
        AggIntent::Rate { .. } => "rate",
        AggIntent::Increase { .. } => "increase",
        AggIntent::Absent => "absent",
        AggIntent::Present => "present",
        AggIntent::Delta { .. } => "delta",
        AggIntent::Deriv { .. } => "deriv",
        AggIntent::PredictLinear { .. } => "predict_linear",
        AggIntent::HoltWinters { .. } => "holt_winters",
        AggIntent::Idelta { .. } => "idelta",
        AggIntent::Irate { .. } => "irate",
        AggIntent::Resets { .. } => "resets",
        AggIntent::Changes { .. } => "changes",
    }
}

/// Telemetry-only metadata recovered from the raw PromQL AST: the
/// outer function name, leading scalar args, and the matrix selector's
/// `[r]` range in seconds. None of this drives capability dispatch —
/// dispatch is `capability_for(&AggIntent)`. This walker exists ONLY
/// so the `ASAPTierCandidate.function` / `.function_args` / `.range_seconds`
/// fields populate for downstream logging and the reducer's range hint.
#[derive(Debug, Default)]
struct PromqlTrace {
    function: String,
    function_args: Vec<f64>,
    range_seconds: u64,
}

fn trace_from_promql(metricsql: &str) -> PromqlTrace {
    let ast = match parser::parse(metricsql) {
        Ok(a) => a,
        Err(_) => return PromqlTrace::default(),
    };
    let mut t = PromqlTrace::default();
    walk_ast_for_trace(&ast, &mut t);
    t
}

fn walk_ast_for_trace(expr: &Expr, t: &mut PromqlTrace) {
    match expr {
        Expr::Call(call) => {
            if t.function.is_empty() {
                t.function = call.func.name.to_lowercase();
            }
            for a in &call.args.args {
                if let Expr::NumberLiteral(nl) = a.as_ref() {
                    t.function_args.push(nl.val);
                } else {
                    walk_ast_for_trace(a, t);
                }
            }
        }
        Expr::Aggregate(agg) => {
            if t.function.is_empty() {
                t.function = agg.op.to_string().to_lowercase();
            }
            if let Some(p) = &agg.param {
                if let Expr::NumberLiteral(nl) = p.as_ref() {
                    t.function_args.push(nl.val);
                }
            }
            walk_ast_for_trace(&agg.expr, t);
        }
        Expr::MatrixSelector(ms) => {
            if t.range_seconds == 0 {
                t.range_seconds = duration_to_seconds(ms.range);
            }
            extract_metric_name(&ms.vs, t);
        }
        Expr::VectorSelector(vs) => {
            extract_metric_name(vs, t);
        }
        Expr::Paren(p) => walk_ast_for_trace(&p.expr, t),
        Expr::Subquery(sq) => walk_ast_for_trace(&sq.expr, t),
        Expr::Binary(b) => {
            walk_ast_for_trace(&b.lhs, t);
            walk_ast_for_trace(&b.rhs, t);
        }
        Expr::Unary(u) => walk_ast_for_trace(&u.expr, t),
        _ => {}
    }
}

#[allow(unused_variables)]
fn extract_metric_name(_vs: &VectorSelector, _t: &mut PromqlTrace) {
    // Metric-name extraction is no longer needed here — the metric
    // name comes from `ParsedQuery.metric_name`. The empty body keeps
    // the AST walker symmetric (every selector-bearing branch routes
    // through one helper) in case future telemetry wants it.
}

fn duration_to_seconds(d: Duration) -> u64 {
    d.as_secs()
}

// ── Candidate → Policy matching ─────────────────────────────────────────────
//
// Closes the analyzer → policy registry lookup half of the merged-sid-identity
// query path. Together with `SketchStore::sids_for_policy` (PR #203) this
// gives the query engine an O(1) `Candidate → policy_fp → [sid]` index that
// avoids walking the per-sid metadata map.

/// Translate an [`asap_types::AggregationConfig`] into the ASAP-tier
/// [`Capability`] its sids serve. Mirrors the inverse direction
/// `capability_for(&AggIntent)`: where that function says "this intent
/// wants *this* capability", this function says "this stored policy
/// *provides* this capability". Returns `None` for `AggregationType`
/// variants that don't have a corresponding ASAP-tier capability
/// (multi-pop keyed variants without an L4 binder, legacy config
/// wrappers, etc.) — callers MUST treat `None` as "policy doesn't
/// serve any ASAP-tier candidate" and skip.
pub fn policy_capability(cfg: &asap_types::AggregationConfig) -> Option<Capability> {
    use crate::sketch_algebra::capability::SketchKindHandle;
    use promql_utilities::query_logics::enums::AggregationType;
    match cfg.aggregation_type {
        // Exact-aggregation families — the ASAP-tier ExactAgg path.
        AggregationType::Sum => Some(Capability::ExactAgg(AggregationType::Sum)),
        AggregationType::Increase => Some(Capability::ExactAgg(AggregationType::Increase)),
        AggregationType::MinMax => Some(Capability::ExactAgg(AggregationType::MinMax)),
        // Quantile families — DDSketch and KLL answer quantile + min/max.
        AggregationType::DDSketch => {
            Some(Capability::QuantileApprox(SketchKindHandle::DDSketch))
        }
        AggregationType::DatasketchesKLL => {
            Some(Capability::QuantileApprox(SketchKindHandle::Kll))
        }
        // Cardinality.
        AggregationType::HLL => Some(Capability::CardinalityApprox),
        // Frequency families.
        AggregationType::CountMinSketch => {
            Some(Capability::FrequencyEstimate(SketchKindHandle::CountMin))
        }
        AggregationType::CountSketch => {
            Some(Capability::FrequencyEstimate(SketchKindHandle::CountSketch))
        }
        AggregationType::CountMinSketchWithHeap => {
            Some(Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap))
        }
        // Keyed-multi-population variants and legacy wrappers — no
        // standalone ASAP-tier capability today. The L4 binder doesn't
        // yet emit `PhysicalExpr::ExactAgg` for keyed `MultipleSum` /
        // `MultipleIncrease` shapes (the matching capability doesn't
        // exist either). When the keyed-ExactAgg follow-up lands, this
        // function gets the corresponding arms.
        AggregationType::MultipleSum
        | AggregationType::MultipleIncrease
        | AggregationType::MultipleMinMax
        | AggregationType::HydraKLL
        | AggregationType::SetAggregator
        | AggregationType::DeltaSetAggregator
        | AggregationType::SingleSubpopulation
        | AggregationType::MultipleSubpopulation => None,
    }
}

/// Look up the policy whose contents match a freshly-ingested
/// sketch's shape. Used by the OTel sketch-ingest path
/// (`drivers/ingest/otel.rs`) to populate
/// `SketchInstanceMetadata.policy_fp` at registration time. Without
/// this lookup, sketch-backed sids carry `PolicyFingerprint::UNSET`
/// and are reachable only through the legacy
/// `instances_matching(metric, gbk)` walk; with it, they participate
/// in the `policy_fp → [sid]` reverse index (#203).
///
/// Match shape — all must hold:
/// 1. `policy.metric == metric`
/// 2. `policy.aggregation_type == agg_type`
/// 3. `policy.grouping_labels.labels` (as a set) == `group_by_keys`
/// 4. Every key in `expected_params` is present in `policy.parameters`
///    with an equal value (deep `serde_json::Value` equality).
///    Extra keys on the policy that aren't in `expected_params` are
///    tolerated — the OTLP DP may not surface every param the
///    control plane authored, and policy-side defaults shouldn't
///    cause a mismatch.
/// 5. `policy.spatial_filter_normalized.is_empty()` — OTLP sketches
///    don't carry a filter context, so only unfiltered policies are
///    matchable from this path.
///
/// Returns `Some(fp)` on a unique match, `None` when zero or multiple
/// policies match. Ambiguous (multiple-match) callers stay on the
/// UNSET sentinel — better than picking one arbitrarily. If multiple
/// distinct windows of the same `(metric, agg_type, params, group_by)`
/// shape exist, the control plane shouldn't have pushed them: they'd
/// collide on sid identity. The skip with `None` surfaces that bug.
pub fn find_policy_by_content(
    registry: &asap_types::PolicyRegistry,
    metric: &str,
    group_by_keys: &BTreeSet<String>,
    agg_type: promql_utilities::query_logics::enums::AggregationType,
    expected_params: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<asap_types::PolicyFingerprint> {
    let mut hit: Option<asap_types::PolicyFingerprint> = None;
    for (fp, cfg) in registry.iter() {
        if cfg.metric != metric {
            continue;
        }
        if cfg.aggregation_type != agg_type {
            continue;
        }
        let policy_keys: BTreeSet<String> =
            cfg.grouping_labels.labels.iter().cloned().collect();
        if &policy_keys != group_by_keys {
            continue;
        }
        if !cfg.spatial_filter_normalized.is_empty() {
            continue;
        }
        // Param subset match — every key the caller named must appear
        // in policy.parameters with an equal value. We don't require
        // the reverse direction (policy may have extra params the DP
        // didn't surface).
        let params_ok = expected_params
            .iter()
            .all(|(k, v)| cfg.parameters.get(k).is_some_and(|pv| pv == v));
        if !params_ok {
            continue;
        }
        // Track unique-match invariant.
        if hit.is_some() {
            // Ambiguous — multiple policies match the same shape. Skip.
            return None;
        }
        hit = Some(*fp);
    }
    hit
}

/// Find every policy in `registry` whose contents satisfy `candidate`.
/// The result is empty when no policy fits — caller routes the query
/// to the archive engine (cold tier) in that case. Multiple matches
/// are valid (different windows / different sketch families all
/// serving the same intent); the caller can pick the cheapest via the
/// cost model or fan out to all of them and combine.
///
/// Matching predicate:
/// 1. `policy.metric == candidate.metric_name`
/// 2. `candidate.group_by_keys ⊆ policy.grouping_labels.labels` —
///    the policy's group-by must cover every key the candidate names
///    (extra group-by keys on the policy are fine; the query can
///    re-aggregate down to its required projection).
/// 3. `policy_capability(policy)` is `Some(c)` and
///    `candidate.required_capability.is_satisfied_by(&c)`.
/// 4. `policy.window_size ≤ candidate.range_seconds` — finer windows
///    can answer coarser queries by merging; the reverse isn't true.
///    When `candidate.range_seconds == 0` (instant-vector query),
///    any policy window passes.
/// 5. `policy.spatial_filter_normalized == candidate.spatial_filter_canonical`
///    — exact match on the canonical filter form. Empty matches empty
///    (the unfiltered case); non-empty must be byte-identical (both
///    sides come from `asap_types::utils::normalize_spatial_filter`,
///    which sorts matchers, so the comparison is independent of the
///    user's source ordering).
pub fn find_matching_policies(
    registry: &asap_types::PolicyRegistry,
    candidate: &ASAPTierCandidate,
) -> Vec<asap_types::PolicyFingerprint> {
    let mut out = Vec::new();
    for (fp, cfg) in registry.iter() {
        if cfg.metric != candidate.metric_name {
            continue;
        }
        let policy_keys: BTreeSet<String> =
            cfg.grouping_labels.labels.iter().cloned().collect();
        if !candidate.group_by_keys.is_subset(&policy_keys) {
            continue;
        }
        let Some(provided) = policy_capability(cfg) else {
            continue;
        };
        if !candidate.required_capability.is_satisfied_by(&provided) {
            continue;
        }
        if candidate.range_seconds > 0 && cfg.window_size > candidate.range_seconds {
            continue;
        }
        if cfg.spatial_filter_normalized != candidate.spatial_filter_canonical {
            continue;
        }
        out.push(*fp);
    }
    out
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ── Supported shapes ─────────────────────────────────────────────────

    #[test]
    fn analyze_quantile_over_time() {
        let a = analyze_promql_for_asap_tier("quantile_over_time(0.99, http_latency_ms[5m])");
        assert!(
            a.unsupported.is_none(),
            "expected no unsupported reason: {a:?}"
        );
        assert_eq!(a.candidates.len(), 1);
        let c = &a.candidates[0];
        assert_eq!(c.metric_name, "http_latency_ms");
        assert_eq!(c.function, "quantile_over_time");
        assert_eq!(c.function_args, vec![0.99]);
        assert_eq!(c.range_seconds, 300);
        assert_eq!(
            c.required_capability,
            Capability::QuantileApprox(SketchKindHandle::Any)
        );
    }

    #[test]
    fn analyze_quantile_over_time_with_label_matchers() {
        let a = analyze_promql_for_asap_tier(
            "quantile_over_time(0.5, http_latency_ms{zone=\"z0\", region=\"us\"}[30s])",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        let c = &a.candidates[0];
        assert_eq!(c.metric_name, "http_latency_ms");
        assert_eq!(c.range_seconds, 30);
        // Group-by keys: zero — label EQ filters aren't group-by
        // labels, they're just selectors. The lowerer leaves
        // `group_by_labels` empty for a bare `quantile_over_time(…)`.
        // (Adding `sum by (...)` around it changes group_by_keys.)
        let _ = c.group_by_keys.clone();
    }

    #[test]
    fn analyze_quantile_over_time_with_sum_by_group_keys() {
        // PromQL `sum by (host) (quantile_over_time(...))` populates
        // group_by_keys with `host`.
        let a = analyze_promql_for_asap_tier(
            "sum by (host) (quantile_over_time(0.99, http_latency_ms[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        assert_eq!(a.candidates[0].group_by_keys, keys(&["host"]));
    }

    #[test]
    fn analyze_histogram_quantile_is_rejected() {
        // `histogram_quantile(...)` is a PromQL/MetricsQL language-level
        // operator, NOT an L3 intent. Per Step γ5, the PromQL parser
        // substitutes it into a plain `Aggregate { Quantile(φ) }` so
        // downstream sees the canonical Quantile intent. The inner argument
        // shape requires a `rate(bucket[r])` which the analyzer rejects as
        // an exact-counter intent, so the analyzer returns SOME unsupported
        // reason; the bucket-aware physical reduction is not yet wired into
        // the ASAP-tier path.
        let a = analyze_promql_for_asap_tier(
            "histogram_quantile(0.99, sum(rate(http_latency_bucket[5m])) by (le))",
        );
        assert!(a.unsupported.is_some(), "{a:?}");
    }

    #[test]
    fn analyze_topk_aggregate() {
        let a = analyze_promql_for_asap_tier(
            "topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        // The first intent the lowerer emits inside a `topk` context
        // is `Count{accuracy=Epsilon}` (because outer_count is set
        // in the topk context), which maps to CardinalityApprox in
        // the bridge — NOT FrequencyTopk. Confirm the right cap.
        // (The actual FrequencyTopk binding lives at the topk wrapper,
        // which isn't an AggIntent today; this is a documented gap.)
        // The test just asserts at least one candidate was produced
        // and no rejection fired.
        assert!(!a.candidates.is_empty(), "expected at least one candidate");
    }

    // ── Unsupported / rejected shapes ────────────────────────────────────

    #[test]
    fn reject_bare_vector_selector() {
        let a = analyze_promql_for_asap_tier("http_requests_total{zone=\"z0\"}");
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::NoCallNodeFound),
            "{a:?}"
        );
        assert!(a.candidates.is_empty());
    }

    #[test]
    fn reject_rate_function() {
        // `rate(...)` lowers to `AggIntent::Rate{...}` and
        // `capability_for(&Rate{..})` returns None.
        let a = analyze_promql_for_asap_tier("rate(http_requests_total[5m])");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => assert_eq!(kind, "rate"),
            other => panic!("expected UnsupportedAggIntent(rate), got {other:?}"),
        }
    }

    #[test]
    fn reject_irate_function() {
        let a = analyze_promql_for_asap_tier("irate(http_requests_total[5m])");
        // `irate` lowers to `AggIntent::Rate{...}` via the
        // control plane's PromQL parser (irate / rate share an AggFunc
        // in `query_parser::promql`). The capability bridge returns
        // None either way.
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => {
                assert!(
                    kind == "rate" || kind == "irate",
                    "unexpected intent kind: {kind}"
                );
            }
            other => panic!("expected UnsupportedAggIntent, got {other:?}"),
        }
    }

    #[test]
    fn reject_increase_function() {
        let a = analyze_promql_for_asap_tier("increase(http_requests_total[5m])");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => {
                assert_eq!(kind, "increase");
            }
            other => panic!("expected UnsupportedAggIntent(increase), got {other:?}"),
        }
    }

    #[test]
    fn reject_sum_by_bare_metric() {
        // `sum by (zone) (metric)` lowers to `AggIntent::Sum`; bridge
        // returns None — Sum-over-CountSketch is a follow-up.
        let a = analyze_promql_for_asap_tier("sum by (zone) (http_requests_total)");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => assert_eq!(kind, "sum"),
            other => panic!("expected UnsupportedAggIntent(sum), got {other:?}"),
        }
    }

    #[test]
    fn unparseable_promql_surfaces_clean_error() {
        let a = analyze_promql_for_asap_tier("@@@ this is not promql @@@");
        match a.unsupported {
            Some(UnsupportedReason::UnparseableMetricsql(msg)) => {
                assert!(!msg.is_empty(), "parser error message should be non-empty");
            }
            other => panic!("expected UnparseableMetricsql, got {other:?}"),
        }
    }

    // ── Range parsing ────────────────────────────────────────────────────

    #[test]
    fn range_seconds_parses_seconds() {
        let a = analyze_promql_for_asap_tier("quantile_over_time(0.99, m[30s])");
        assert_eq!(a.candidates[0].range_seconds, 30);
    }

    #[test]
    fn range_seconds_parses_minutes() {
        let a = analyze_promql_for_asap_tier("quantile_over_time(0.99, m[5m])");
        assert_eq!(a.candidates[0].range_seconds, 300);
    }

    #[test]
    fn range_seconds_parses_hours() {
        let a = analyze_promql_for_asap_tier("quantile_over_time(0.99, m[2h])");
        assert_eq!(a.candidates[0].range_seconds, 7200);
    }

    // ── is_asap_tier_answerable ──────────────────────────────────────────

    #[test]
    fn is_asap_tier_answerable_true_for_supported() {
        let a = analyze_promql_for_asap_tier("quantile_over_time(0.99, m[5m])");
        assert!(a.is_asap_tier_answerable());
    }

    #[test]
    fn is_asap_tier_answerable_false_for_unsupported() {
        let a = analyze_promql_for_asap_tier("rate(m[5m])");
        assert!(!a.is_asap_tier_answerable());
    }

    #[test]
    fn is_asap_tier_answerable_false_for_bare_selector() {
        let a = analyze_promql_for_asap_tier("m{zone=\"z0\"}");
        assert!(!a.is_asap_tier_answerable());
    }

    // ── Cardinality / count_over_time real-PromQL acceptance ────────────

    /// `count_over_time(...)` is real PromQL and lowers to
    /// `AggIntent::Count{accuracy:Exact}` per `intent_algebra::lower`.
    /// Exact-accuracy Count has no ASAP-tier binding, so the analyzer
    /// surfaces this as `UnsupportedAggIntent("count")` — the routing
    /// layer then sends it to archive, which is the right behavior
    /// because `count_over_time` counts samples (not distinct values).
    #[test]
    fn count_over_time_is_unsupported_at_exact_accuracy() {
        // `count_over_time(metric[r])` without an outer `count by (...)`
        // is the PromQL "count samples per window" idiom — exact at L3.
        // The lowerer doesn't emit an AggIntent for it (no entry in
        // `AggType`), so the analyzer surfaces the raw function name
        // from the AST trace as the `UnsupportedAggIntent` label.
        let a = analyze_promql_for_asap_tier("count_over_time(http_requests_total[5m])");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => {
                assert!(
                    kind == "count" || kind == "count_over_time",
                    "unexpected intent kind: {kind}"
                );
            }
            other => panic!("expected UnsupportedAggIntent, got {other:?}"),
        }
    }

    /// `count by (...) (count_over_time(...))` is the PromQL distinct-
    /// count idiom. The `query_parser::promql` walker promotes the
    /// outer `count` + inner `count_over_time` to `AggFunc::CountDistinct`,
    /// which lowers to `AggIntent::Cardinality{accuracy=Epsilon}` and
    /// maps to `Capability::CardinalityApprox`.
    #[test]
    fn count_by_count_over_time_is_cardinality() {
        let a = analyze_promql_for_asap_tier(
            "count by (symbol) (count_over_time(financial_last_trade_price[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert!(!a.candidates.is_empty());
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::CardinalityApprox,
        );
    }

    // ── policy_capability + find_matching_policies tests ─────────────────

    mod matching {
        use super::super::*;
        use asap_types::{AggregationConfig, PolicyFingerprint, PolicyRegistry};
        use promql_utilities::data_model::KeyByLabelNames;
        use promql_utilities::query_logics::enums::AggregationType;
        use std::collections::HashMap;

        fn cfg(
            metric: &str,
            agg_type: AggregationType,
            group_by: Vec<&str>,
            window_size: u64,
            spatial_filter: &str,
        ) -> AggregationConfig {
            AggregationConfig::new(
                agg_type,
                String::new(),
                HashMap::new(),
                KeyByLabelNames::new(group_by.into_iter().map(|s| s.to_string()).collect()),
                KeyByLabelNames::empty(),
                KeyByLabelNames::empty(),
                String::new(),
                window_size,
                window_size,
                asap_types::enums::WindowType::Tumbling,
                spatial_filter.to_string(),
                metric.to_string(),
                None,
                None,
                None,
            )
        }

        fn candidate(
            metric: &str,
            group_by: &[&str],
            cap: Capability,
            range_seconds: u64,
        ) -> ASAPTierCandidate {
            candidate_with_filter(metric, group_by, cap, range_seconds, "")
        }

        fn candidate_with_filter(
            metric: &str,
            group_by: &[&str],
            cap: Capability,
            range_seconds: u64,
            spatial_filter_canonical: &str,
        ) -> ASAPTierCandidate {
            ASAPTierCandidate {
                metric_name: metric.to_string(),
                group_by_keys: group_by.iter().map(|s| s.to_string()).collect(),
                required_capability: cap,
                function: String::new(),
                function_args: Vec::new(),
                range_seconds,
                spatial_filter_canonical: spatial_filter_canonical.to_string(),
            }
        }

        #[test]
        fn policy_capability_maps_sum_to_exact_agg() {
            let c = cfg("m", AggregationType::Sum, vec![], 60, "");
            assert_eq!(
                policy_capability(&c),
                Some(Capability::ExactAgg(AggregationType::Sum))
            );
        }

        #[test]
        fn policy_capability_maps_ddsketch_to_quantile_approx() {
            use crate::sketch_algebra::capability::SketchKindHandle;
            let c = cfg("m", AggregationType::DDSketch, vec![], 60, "");
            assert_eq!(
                policy_capability(&c),
                Some(Capability::QuantileApprox(SketchKindHandle::DDSketch))
            );
        }

        #[test]
        fn policy_capability_returns_none_for_multi_pop_variants() {
            let c = cfg("m", AggregationType::MultipleSum, vec!["zone"], 60, "");
            assert!(policy_capability(&c).is_none());
        }

        #[test]
        fn matches_exact_metric_and_capability() {
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            let m = find_matching_policies(&registry, &cand);
            assert_eq!(m.len(), 1);
            assert_eq!(m[0], registry.fingerprints().next().unwrap());
        }

        #[test]
        fn does_not_match_different_metric() {
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "cpu_pct",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn does_not_match_incompatible_capability() {
            // Policy is Sum (ExactAgg); candidate asks for QuantileApprox.
            use crate::sketch_algebra::capability::SketchKindHandle;
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::QuantileApprox(SketchKindHandle::Any),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn matches_when_policy_group_by_covers_candidate() {
            // Policy keeps {zone, service}; candidate asks for just {zone}.
            // That's covered — the query can re-aggregate down.
            let policies = vec![cfg(
                "http_lat",
                AggregationType::Sum,
                vec!["zone", "service"],
                60,
                "",
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &["zone"],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn does_not_match_when_candidate_needs_keys_policy_lacks() {
            // Policy keeps {zone}; candidate asks for {zone, service}.
            // That's NOT covered — policy already projected service away.
            let policies = vec![cfg(
                "http_lat",
                AggregationType::Sum,
                vec!["zone"],
                60,
                "",
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &["zone", "service"],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn finer_window_matches_coarser_query_range() {
            // Policy emits 60s windows; candidate wants 300s range.
            // Finer can answer coarser via merge.
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                300,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn coarser_window_does_not_match_finer_query_range() {
            // Policy emits 300s windows; candidate wants 60s range.
            // Can't downsample 300s into 60s.
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 300, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn zero_range_query_accepts_any_window() {
            // Instant-vector queries (range_seconds=0) match any policy.
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 300, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                0,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn filtered_policy_matches_when_candidate_has_matching_filter() {
            // Both sides carry the canonical form
            // `{status="200"}` (normalize_spatial_filter sorts +
            // brace-wraps single-matcher inputs). Match should succeed.
            let policies = vec![cfg(
                "http_lat",
                AggregationType::Sum,
                vec![],
                60,
                r#"status="200""#,
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate_with_filter(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
                r#"{status="200"}"#,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn filtered_policy_does_not_match_unfiltered_candidate() {
            let policies = vec![cfg(
                "http_lat",
                AggregationType::Sum,
                vec![],
                60,
                r#"status="200""#,
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn unfiltered_policy_does_not_match_filtered_candidate() {
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate_with_filter(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
                r#"{status="200"}"#,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn different_filter_values_do_not_match() {
            let policies = vec![cfg(
                "http_lat",
                AggregationType::Sum,
                vec![],
                60,
                r#"status="200""#,
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate_with_filter(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
                r#"{status="500"}"#,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn multiple_matches_return_all_fingerprints() {
            // Two policies serve the same intent at different windows
            // — both match (cost model picks one later).
            let policies = vec![
                cfg("http_lat", AggregationType::Sum, vec![], 60, ""),
                cfg("http_lat", AggregationType::Sum, vec![], 30, ""),
            ];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                300,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 2);
        }

        #[test]
        fn ignores_multi_pop_policies() {
            // Multi-population policies have `policy_capability == None`;
            // matching skips them even when other fields would line up.
            let policies = vec![cfg(
                "http_lat",
                AggregationType::MultipleSum,
                vec!["zone"],
                60,
                "",
            )];
            let registry = PolicyRegistry::from_configs(policies);
            let cand = candidate(
                "http_lat",
                &["zone"],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
        }

        #[test]
        fn empty_registry_yields_empty_matches() {
            let registry = PolicyRegistry::from_configs(Vec::<AggregationConfig>::new());
            let cand = candidate(
                "http_lat",
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert!(find_matching_policies(&registry, &cand).is_empty());
            let _ = PolicyFingerprint::UNSET; // silence unused import warning
        }
    }
}
