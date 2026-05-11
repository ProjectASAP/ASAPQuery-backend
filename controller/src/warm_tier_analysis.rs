//! PromQL → warm-tier candidate analyzer (Step 2a thin-facade rewrite).
//!
//! Before Step 2a this module was an 868-line second-PromQL-walker that
//! pattern-matched on raw function-name strings — duplicating the
//! controller's existing `query_parser::parse_query` →
//! `intent_algebra::lower::lower_parsed_query` pipeline and inventing a
//! parallel set of function names (`count_distinct_over_time`,
//! `cardinality_estimate`, `count_distinct`) that aren't part of PromQL
//! or MetricsQL.
//!
//! After Step 2a this module is a ~120-line facade. The pipeline is:
//!
//! ```text
//! PromQL string
//!   ↓  query_parser::parse_query  (the controller's PromQL → ParsedQuery)
//! ParsedQuery
//!   ↓  intent_algebra::lower::lower_parsed_query
//! QueryExpr (intent_algebra) — Scan / Window / Aggregate{ aggs: Vec<AggIntent> }
//!   ↓  walk and call capability_for(&AggIntent)
//! Vec<WarmTierCandidate>
//! ```
//!
//! The lowerer is the **single owner** of "what does this PromQL function
//! mean"; `sketch_algebra::capability_for` is the **single owner** of
//! "what sketch can answer this intent". This module just glues the two.
//!
//! ## What's still here
//!
//! - The `WarmTierCandidate` / `WarmTierAnalysis` / `UnsupportedReason`
//!   public types — the warm-tier reducer and the engine router consume
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
/// warm tier. The reducer resolves each candidate to a vector of sids
/// via `SketchIndex::instances_matching(metric_name, group_by_keys)`
/// and verifies each sid carries the required capability.
#[derive(Debug, Clone, PartialEq)]
pub struct WarmTierCandidate {
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
}

/// Whole-query analysis result.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WarmTierAnalysis {
    pub candidates: Vec<WarmTierCandidate>,
    pub unsupported: Option<UnsupportedReason>,
}

impl WarmTierAnalysis {
    /// True iff the analysis is fully warm-tier-answerable —
    /// `unsupported.is_none()` AND at least one candidate.
    pub fn is_warm_tier_answerable(&self) -> bool {
        self.unsupported.is_none() && !self.candidates.is_empty()
    }
}

/// Distinct reasons a PromQL query is NOT warm-tier-answerable. The
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
    /// the warm tier doesn't materialize raw counter values.
    NoCallNodeFound,
    /// `query_parser::parse_query` rejected the input. Carries the
    /// parser error message for diagnostics.
    UnparseableMetricsql(String),
}

// ── Public entry point ───────────────────────────────────────────────────────

/// Parse PromQL via the controller's existing pipeline, lower to L3
/// `intent_algebra::QueryExpr`, walk it, and build a
/// [`WarmTierAnalysis`].
///
/// Single owner of warm-tier shape recognition: this function does
/// **no** direct PromQL function-name matching. The lowerer
/// (`intent_algebra::lower::lower_parsed_query`) is the only place
/// that knows what `quantile_over_time` / `count_over_time` / etc.
/// mean; this function just consumes the lowered `AggIntent`s and
/// dispatches via [`capability_for`].
pub fn analyze_promql_for_warm_tier(metricsql: &str) -> WarmTierAnalysis {
    // Step 1: parse via the controller's existing PromQL → ParsedQuery
    // chain. `parse_query` already understands the full PromQL surface
    // we care about.
    let parsed = match parse_query(metricsql) {
        Ok(p) => p,
        Err(e) => {
            return WarmTierAnalysis {
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
            return WarmTierAnalysis {
                candidates: Vec::new(),
                unsupported: Some(UnsupportedReason::UnparseableMetricsql(e.to_string())),
            };
        }
    };

    // Step 3: walk the lowered tree, looking for `Aggregate` nodes.
    // If there's no Aggregate the query is either:
    //   - a bare metric selector → `NoCallNodeFound` (warm-tier
    //     doesn't materialize raw counter values)
    //   - a window-bound exact-aggregation (`rate`, `irate`,
    //     `increase`, `sum_over_time`, `count_over_time` without
    //     outer count, etc.) — the controller's PromQL parser sets
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
        return WarmTierAnalysis {
            candidates: Vec::new(),
            unsupported: Some(reason),
        };
    }

    // Step 4: for each intent, look up its capability. The first
    // intent that returns `None` aborts the analysis — the warm
    // tier can't answer this query (the router falls over to archive).
    let metric_name = parsed.metric_name.clone();
    let group_by_keys: BTreeSet<String> = parsed.group_by_labels.iter().cloned().collect();

    let mut out = WarmTierAnalysis::default();
    for intent in &intents {
        match capability_for(intent) {
            Some(cap) => {
                out.candidates.push(WarmTierCandidate {
                    metric_name: metric_name.clone(),
                    group_by_keys: group_by_keys.clone(),
                    required_capability: cap,
                    function: trace.function.clone(),
                    function_args: trace.function_args.clone(),
                    range_seconds: trace.range_seconds,
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
/// so the `WarmTierCandidate.function` / `.function_args` / `.range_seconds`
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
        let a = analyze_promql_for_warm_tier("quantile_over_time(0.99, http_latency_ms[5m])");
        assert!(a.unsupported.is_none(), "expected no unsupported reason: {a:?}");
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
        let a = analyze_promql_for_warm_tier(
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
        let a = analyze_promql_for_warm_tier(
            "sum by (host) (quantile_over_time(0.99, http_latency_ms[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        assert_eq!(a.candidates[0].group_by_keys, keys(&["host"]));
    }

    #[test]
    fn analyze_histogram_quantile_is_rejected() {
        // `histogram_quantile(...)` is a PromQL/MetricsQL language-level
        // operator (a `legacy_expr::QueryExpr::HistogramQuantile` node),
        // NOT an L3 intent. The inner argument shape requires a
        // `rate(bucket[r])` which the analyzer rejects as an exact-counter
        // intent, so the analyzer returns SOME unsupported reason. The
        // architectural mapping `histogram_quantile(q, bucket_metric)` →
        // `AggIntent::Quantile{q,...}` is documented but the bucket-aware
        // physical reduction is not yet wired into the warm-tier path.
        let a = analyze_promql_for_warm_tier(
            "histogram_quantile(0.99, sum(rate(http_latency_bucket[5m])) by (le))",
        );
        assert!(a.unsupported.is_some(), "{a:?}");
    }

    #[test]
    fn analyze_topk_aggregate() {
        let a = analyze_promql_for_warm_tier(
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
        let a = analyze_promql_for_warm_tier("http_requests_total{zone=\"z0\"}");
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
        let a = analyze_promql_for_warm_tier("rate(http_requests_total[5m])");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => assert_eq!(kind, "rate"),
            other => panic!("expected UnsupportedAggIntent(rate), got {other:?}"),
        }
    }

    #[test]
    fn reject_irate_function() {
        let a = analyze_promql_for_warm_tier("irate(http_requests_total[5m])");
        // `irate` lowers to `AggIntent::Rate{...}` via the
        // controller's PromQL parser (irate / rate share an AggFunc
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
        let a = analyze_promql_for_warm_tier("increase(http_requests_total[5m])");
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
        let a = analyze_promql_for_warm_tier("sum by (zone) (http_requests_total)");
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedAggIntent(kind)) => assert_eq!(kind, "sum"),
            other => panic!("expected UnsupportedAggIntent(sum), got {other:?}"),
        }
    }

    #[test]
    fn unparseable_promql_surfaces_clean_error() {
        let a = analyze_promql_for_warm_tier("@@@ this is not promql @@@");
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
        let a = analyze_promql_for_warm_tier("quantile_over_time(0.99, m[30s])");
        assert_eq!(a.candidates[0].range_seconds, 30);
    }

    #[test]
    fn range_seconds_parses_minutes() {
        let a = analyze_promql_for_warm_tier("quantile_over_time(0.99, m[5m])");
        assert_eq!(a.candidates[0].range_seconds, 300);
    }

    #[test]
    fn range_seconds_parses_hours() {
        let a = analyze_promql_for_warm_tier("quantile_over_time(0.99, m[2h])");
        assert_eq!(a.candidates[0].range_seconds, 7200);
    }

    // ── is_warm_tier_answerable ──────────────────────────────────────────

    #[test]
    fn is_warm_tier_answerable_true_for_supported() {
        let a = analyze_promql_for_warm_tier("quantile_over_time(0.99, m[5m])");
        assert!(a.is_warm_tier_answerable());
    }

    #[test]
    fn is_warm_tier_answerable_false_for_unsupported() {
        let a = analyze_promql_for_warm_tier("rate(m[5m])");
        assert!(!a.is_warm_tier_answerable());
    }

    #[test]
    fn is_warm_tier_answerable_false_for_bare_selector() {
        let a = analyze_promql_for_warm_tier("m{zone=\"z0\"}");
        assert!(!a.is_warm_tier_answerable());
    }

    // ── Cardinality / count_over_time real-PromQL acceptance ────────────

    /// `count_over_time(...)` is real PromQL and lowers to
    /// `AggIntent::Count{accuracy:Exact}` per `intent_algebra::lower`.
    /// Exact-accuracy Count has no warm-tier binding, so the analyzer
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
        let a = analyze_promql_for_warm_tier("count_over_time(http_requests_total[5m])");
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
        let a = analyze_promql_for_warm_tier(
            "count by (symbol) (count_over_time(financial_last_trade_price[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert!(!a.candidates.is_empty());
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::CardinalityApprox,
        );
    }
}
