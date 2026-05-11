//! PromQL → warm-tier candidate analyzer.
//!
//! Single owner of "is this PromQL warm-tier-answerable" knowledge,
//! pulled out of `asap-query-engine/src/engines/warm_tier/promql_extract.rs`
//! (deleted in the same change). The controller already encodes the
//! PromQL → Intent → Capability pipeline via `query_parser`,
//! `intent_algebra`, `sketch_algebra`, and `algebra::lower`; this module
//! is the **query-time** analog: rather than emitting a full plan, it
//! decides which sub-expressions of a PromQL query CAN be answered from
//! the warm tier and what [`Capability`] each requires.
//!
//! The output is consumed by the warm-tier reducer in
//! `asap-query-engine/src/engines/warm_tier/sketch_reducer.rs` and by
//! the `SimpleEngine::execute` warm-tier hook, replacing the previous
//! per-PromQL string-matched dispatch.
//!
//! # API
//!
//! - [`analyze_promql_for_warm_tier`] — pure function; parses + walks
//!   the PromQL AST and returns either a populated [`WarmTierAnalysis`]
//!   or an [`UnsupportedReason`].
//! - [`WarmTierAnalysis`] — a vector of [`WarmTierCandidate`]s (the
//!   sub-expressions the warm tier CAN serve) plus an
//!   [`UnsupportedReason`] when the query has parts that cannot be
//!   served (or cannot be parsed).
//! - [`Capability`] — warm-tier capability tag. Mirrors the
//!   `sketch_index::Capability` enum in `asap-query-engine`; this is
//!   the controller-side authority for the type. The
//!   `asap-query-engine` side type-aliases / converts via small From
//!   adapters at the call site.
//!
//! # Supported PromQL shapes (and the Capability each maps to)
//!
//! | Shape | Capability |
//! |---|---|
//! | `quantile_over_time(q, m[r])` | `QuantileApprox(Any)` |
//! | `quantile_over_time(q, m)` | `QuantileApprox(Any)` (instant — no range) |
//! | `histogram_quantile(q, m)` | `QuantileApprox(Any)` |
//! | `count_distinct_over_time(m[r])` | `CardinalityApprox` |
//! | `cardinality_estimate(m)` | `CardinalityApprox` |
//! | `topk(k, m)` | `FrequencyTopk(CmsWithHeap)` |
//! | `topk_over_time(k, m[r])` | `FrequencyTopk(CmsWithHeap)` |
//! | bare `m{filters}` | `UnsupportedReason::NoCallNodeFound` |
//!
//! # Explicitly rejected PromQL shapes
//!
//! The demo's compound queries that today silently route through the
//! archive engine are surfaced explicitly:
//!
//! - `sum by (label_set) (rate(metric[range]))` — `rate` is raw
//!   counter math, not a sketch op. Surface as
//!   `UnsupportedFunction("rate")`.
//! - `histogram_quantile(q, sum(rate(bucket[r])) by (le))` — same
//!   reason: nested `rate`.
//! - `sum by (label_set) (metric)` — `Sum-over-CountSketch` reducer
//!   is a future follow-up. Surface as `UnsupportedComposition(...)`.
//! - `increase` / `irate` — raw counter math. Surface as
//!   `UnsupportedFunction(...)`.
//! - `topk(k, rate(metric[r]))` — `topk` is only meaningful over an
//!   instant vector of items. Surface as `UnsupportedComposition(...)`.
//!
//! # Time range extraction
//!
//! Each candidate carries `range_seconds: u64` — the matrix-vector
//! selector's `[r]` parsed into seconds. A `0` value means "no matrix
//! selector" (instant-vector query). The reducer uses this when
//! deciding the per-window vs cumulative dispatch.

use std::collections::BTreeSet;
use std::time::Duration;

use promql_parser::parser::{self, AggregateExpr, Call, Expr, MatrixSelector, VectorSelector};

// ── Public types ─────────────────────────────────────────────────────────────

/// Controller-side warm-tier capability tag. Mirrors the
/// `asap-query-engine`-side `sketch_index::Capability` enum so the
/// controller can emit capability requirements without depending on
/// the backend's sketch_index module. The two enums are kept
/// structurally identical and adapted via a small `From` impl at the
/// call site.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    QuantileApprox(SketchKindHandle),
    CardinalityApprox,
    FrequencyTopk(SketchKindHandle),
}

/// Compact handle for sketch family choice. Mirrors
/// `sketch_index::SketchKindHandle`. The `Any` variant is the
/// controller's "any implementation that satisfies the family works"
/// signal — e.g. for QuantileApprox the controller doesn't pick
/// DDSketch vs KLL at analysis time; the resolver picks whichever
/// instance the index already carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SketchKindHandle {
    DDSketch,
    Kll,
    Hll,
    CountSketch,
    CountMin,
    CmsWithHeap,
    /// "Any implementation that satisfies the family". Used at analysis
    /// time when the capability is family-bound but not
    /// implementation-bound.
    Any,
}

/// One sub-expression of the input PromQL that CAN be served from
/// the warm tier. The reducer resolves each candidate to a vector of
/// sids via `SketchIndex::instances_matching(metric_name, group_by_keys)`
/// and verifies each sid carries the required capability.
#[derive(Debug, Clone, PartialEq)]
pub struct WarmTierCandidate {
    pub metric_name: String,
    pub group_by_keys: BTreeSet<String>,
    pub required_capability: Capability,
    pub function: String,
    /// Already-evaluated leading scalar args. Order matches the
    /// PromQL surface (`quantile_over_time(q, foo[r])` → `args[0] = q`).
    pub function_args: Vec<f64>,
    /// Time range from the matrix-vector selector (e.g. `[5m]` → 300).
    /// `0` when the query is instant-vector-shaped (`histogram_quantile`
    /// over an already-bucketed metric, bare cardinality_estimate, etc.).
    pub range_seconds: u64,
}

/// Whole-query analysis result. The vector of [`WarmTierCandidate`]s
/// covers every sub-expression the warm tier CAN serve. `unsupported`
/// is `Some` when ANY sub-expression cannot be served (or when the
/// query parse failed); in that case `candidates` may be partially
/// populated (sub-expressions BEFORE the rejected one) but the
/// reducer treats the analysis as warm-tier-miss and routes to cold.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WarmTierAnalysis {
    pub candidates: Vec<WarmTierCandidate>,
    pub unsupported: Option<UnsupportedReason>,
}

impl WarmTierAnalysis {
    /// True when the analysis is fully warm-tier-answerable —
    /// `unsupported.is_none()` AND at least one candidate.
    pub fn is_warm_tier_answerable(&self) -> bool {
        self.unsupported.is_none() && !self.candidates.is_empty()
    }
}

/// Distinct reasons a PromQL query is NOT warm-tier-answerable.
/// Each variant maps onto a different routing decision the caller
/// makes (typically all → cold tier / archive, but the variant
/// distinction matters for logging + future precompute hints).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsupportedReason {
    /// PromQL function the warm tier has no reducer for —
    /// `rate`, `irate`, `increase`, raw arithmetic, etc.
    UnsupportedFunction(String),
    /// PromQL composition shape the warm tier can't unfold —
    /// `topk(k, rate(...))`, `sum by (...) (metric)` (pending
    /// the Sum-over-CountSketch reducer), histogram_quantile over
    /// a nested rate, etc. The string carries a short description.
    UnsupportedComposition(String),
    /// The query is a bare vector / matrix selector with no call —
    /// the warm tier doesn't materialize raw counter values; the
    /// archive answers these directly.
    NoCallNodeFound,
    /// `promql_parser` failed to parse the input. Carries the parser
    /// error message for diagnostics.
    UnparseablePromql(String),
}

// ── Public entry point ───────────────────────────────────────────────────────

/// Parse PromQL + walk the AST and produce a [`WarmTierAnalysis`].
///
/// This is the single owner of warm-tier shape recognition. All
/// downstream code (the warm-tier reducer, the engine router) keys
/// off the returned `WarmTierAnalysis` and never re-parses the
/// PromQL string.
pub fn analyze_promql_for_warm_tier(promql: &str) -> WarmTierAnalysis {
    // ── Custom warm-tier function names ─────────────────────────────────
    //
    // `cardinality_estimate(metric)`, `count_distinct_over_time(metric[r])`,
    // `count_distinct(metric)`, and `topk_over_time(k, metric[r])` are
    // not in `promql_parser`'s built-in function table, so the AST
    // parser rejects them outright. We pre-detect those shapes via a
    // narrow regex on the OUTER call, then re-parse the inner
    // selector / matrix-selector expression as standalone PromQL.
    if let Some(analysis) = try_parse_custom_function(promql) {
        return analysis;
    }

    let ast = match parser::parse(promql) {
        Ok(ast) => ast,
        Err(e) => {
            return WarmTierAnalysis {
                candidates: Vec::new(),
                unsupported: Some(UnsupportedReason::UnparseablePromql(e.to_string())),
            };
        }
    };
    let mut out = WarmTierAnalysis::default();
    analyze_expr(&ast, &mut out);
    out
}

/// Match a small set of custom warm-tier function names that
/// `promql_parser` doesn't recognize, and parse the inner argument
/// as a standalone selector / matrix-selector to extract
/// `(metric, group_by, range)`. Returns `None` if the input doesn't
/// look like one of those custom shapes.
fn try_parse_custom_function(promql: &str) -> Option<WarmTierAnalysis> {
    let trimmed = promql.trim();
    // Shape: NAME ( [scalar_args... , ] inner_expr )
    let open = trimmed.find('(')?;
    if !trimmed.ends_with(')') {
        return None;
    }
    let name = trimmed[..open].trim().to_lowercase();
    let inner = &trimmed[open + 1..trimmed.len() - 1];

    let (capability, has_scalar) = match name.as_str() {
        "cardinality_estimate" => (Capability::CardinalityApprox, false),
        "count_distinct" => (Capability::CardinalityApprox, false),
        "count_distinct_over_time" => (Capability::CardinalityApprox, false),
        "topk_over_time" => {
            (Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap), true)
        }
        _ => return None,
    };

    // Split off leading scalar arg (the `k` for topk_over_time, the
    // `q` for any future quantile-shaped custom function).
    let (scalar_args, body) = if has_scalar {
        let (head, tail) = split_first_top_level_comma(inner)?;
        let v = head.trim().parse::<f64>().ok()?;
        (vec![v], tail.trim())
    } else {
        (Vec::new(), inner.trim())
    };

    // Parse the body as standalone PromQL. We accept either a bare
    // vector selector or a matrix selector.
    let ast = parser::parse(body).ok()?;
    let (metric, gb, range_s) = extract_metric_keys_range(&ast)?;
    Some(WarmTierAnalysis {
        candidates: vec![WarmTierCandidate {
            metric_name: metric,
            group_by_keys: gb,
            required_capability: capability,
            function: name,
            function_args: scalar_args,
            range_seconds: range_s,
        }],
        unsupported: None,
    })
}

/// Split a comma-separated argument list at the FIRST top-level comma
/// (one outside any nested parentheses / brackets). Used by
/// [`try_parse_custom_function`] to peel off a leading scalar argument
/// from `topk_over_time(k, metric[r])` without mistakenly splitting on
/// a comma inside a label-matcher.
fn split_first_top_level_comma(s: &str) -> Option<(&str, &str)> {
    let bytes = s.as_bytes();
    let mut depth: i32 = 0;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                return Some((&s[..i], &s[i + 1..]));
            }
            _ => {}
        }
    }
    None
}

// ── AST walker ───────────────────────────────────────────────────────────────

fn analyze_expr(expr: &Expr, out: &mut WarmTierAnalysis) {
    match expr {
        Expr::Call(call) => analyze_call(call, out),
        Expr::Aggregate(agg) => analyze_aggregate(agg, out),
        Expr::Paren(p) => analyze_expr(&p.expr, out),
        Expr::Subquery(sq) => analyze_expr(&sq.expr, out),
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => {
            // Bare selector — no call to dispatch on. The archive
            // engine answers raw selectors; warm tier doesn't
            // materialize raw counter values.
            out.unsupported = Some(UnsupportedReason::NoCallNodeFound);
        }
        Expr::Binary(_) => {
            // Binary ops (e.g. `rate(...) > 0.5`) aren't a single
            // warm-tier candidate. We don't try to decompose them.
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
                "binary expression — warm tier does not stitch lhs/rhs".to_string(),
            ));
        }
        Expr::Unary(_) => {
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
                "unary expression — warm tier does not stitch unary over sketch output"
                    .to_string(),
            ));
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
            // Literals as top-level expressions aren't queries that
            // hit the warm tier.
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
                "literal at query root — no metric selector".to_string(),
            ));
        }
        // Promql_parser exposes additional variants for future shapes
        // (Extension, etc.); treat everything else as unsupported.
        _ => {
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
                "unrecognized PromQL expression shape".to_string(),
            ));
        }
    }
}

/// Handle `Call` nodes: the canonical warm-tier shapes
/// (`quantile_over_time`, `histogram_quantile`,
/// `count_distinct_over_time`, `cardinality_estimate`,
/// `topk_over_time`) plus the demo's explicit rejections
/// (`rate`, `irate`, `increase`).
fn analyze_call(call: &Call, out: &mut WarmTierAnalysis) {
    let func_name = call.func.name.to_lowercase();

    // ── Reject raw-counter math up-front ────────────────────────────────
    if matches!(
        func_name.as_str(),
        "rate" | "irate" | "increase" | "deriv" | "predict_linear" | "delta" | "idelta"
    ) {
        out.unsupported = Some(UnsupportedReason::UnsupportedFunction(func_name));
        return;
    }

    // Leading scalar args (e.g. `q` in `quantile_over_time(q, m[r])`).
    let mut scalar_args = Vec::new();
    for a in &call.args.args {
        match a.as_ref() {
            Expr::NumberLiteral(nl) => scalar_args.push(nl.val),
            _ => break,
        }
    }

    // Extract the inner selector (or detect nested forbidden calls
    // like `histogram_quantile(q, sum(rate(...)) by (le))`).
    let body_arg = match call.args.args.iter().find(|a| {
        !matches!(a.as_ref(), Expr::NumberLiteral(_) | Expr::StringLiteral(_))
    }) {
        Some(a) => a.as_ref(),
        None => {
            // No selector arg — `quantile_over_time(0.99)` with no
            // metric. Malformed but we surface as unsupported.
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(format!(
                "{func_name}: no metric selector argument"
            )));
            return;
        }
    };

    // For histogram_quantile, the body may be a bare bucket selector
    // (our supported case) OR a nested aggregate over rate (the
    // explicit rejection). Walk in.
    if func_name == "histogram_quantile" {
        if has_nested_rate(body_arg) {
            out.unsupported = Some(UnsupportedReason::UnsupportedFunction("rate".to_string()));
            return;
        }
        if let Some((metric, gb, range_s)) = extract_metric_keys_range(body_arg) {
            out.candidates.push(WarmTierCandidate {
                metric_name: metric,
                group_by_keys: gb,
                required_capability: Capability::QuantileApprox(SketchKindHandle::Any),
                function: "histogram_quantile".to_string(),
                function_args: scalar_args,
                range_seconds: range_s,
            });
            return;
        }
        out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
            "histogram_quantile: body is not a recognizable metric/aggregate".to_string(),
        ));
        return;
    }

    // Reject `*_over_time` wrappers around rate / irate / increase
    // even when not under histogram_quantile.
    if has_nested_rate(body_arg) {
        out.unsupported = Some(UnsupportedReason::UnsupportedFunction("rate".to_string()));
        return;
    }

    let (metric, gb, range_s) = match extract_metric_keys_range(body_arg) {
        Some(x) => x,
        None => {
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(format!(
                "{func_name}: cannot extract metric selector from body"
            )));
            return;
        }
    };

    let cap = match func_name.as_str() {
        "quantile_over_time" => Capability::QuantileApprox(SketchKindHandle::Any),
        "count_distinct_over_time" | "cardinality_estimate" => Capability::CardinalityApprox,
        "topk_over_time" => Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap),
        other => {
            out.unsupported = Some(UnsupportedReason::UnsupportedFunction(other.to_string()));
            return;
        }
    };
    out.candidates.push(WarmTierCandidate {
        metric_name: metric,
        group_by_keys: gb,
        required_capability: cap,
        function: func_name,
        function_args: scalar_args,
        range_seconds: range_s,
    });
}

/// Handle `Aggregate` nodes: `topk(k, m)` is the only supported
/// shape today. `sum by (label_set) (metric)` is the documented
/// follow-up — surface as `UnsupportedComposition`.
fn analyze_aggregate(agg: &AggregateExpr, out: &mut WarmTierAnalysis) {
    let op_name = agg.op.to_string().to_lowercase();

    // Nested rate / irate / increase anywhere in the aggregate's body
    // disqualifies the whole expression regardless of the outer
    // aggregate op. Detect this first so the
    // `sum by (zone) (rate(http_requests_total[5m]))` shape surfaces
    // the canonical "rate" error message rather than the outer-op
    // composition error.
    if has_nested_rate(&agg.expr) {
        out.unsupported = Some(UnsupportedReason::UnsupportedFunction("rate".to_string()));
        return;
    }

    // Pull `k` from the aggregate's `param` (for `topk` / `bottomk` /
    // `quantile`).
    let mut scalar_args: Vec<f64> = Vec::new();
    if let Some(p) = &agg.param {
        if let Expr::NumberLiteral(nl) = p.as_ref() {
            scalar_args.push(nl.val);
        }
    }

    match op_name.as_str() {
        "topk" | "bottomk" => {
            // Nested rate is already filtered out above (top-of-fn
            // `has_nested_rate` check); here we just need to extract
            // the metric from the body.
            let (metric, gb, range_s) = match extract_metric_keys_range(&agg.expr) {
                Some(x) => x,
                None => {
                    out.unsupported = Some(UnsupportedReason::UnsupportedComposition(format!(
                        "{op_name}: cannot extract metric selector from body"
                    )));
                    return;
                }
            };
            out.candidates.push(WarmTierCandidate {
                metric_name: metric,
                group_by_keys: gb,
                required_capability: Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap),
                function: op_name,
                function_args: scalar_args,
                range_seconds: range_s,
            });
        }
        "sum" | "avg" | "count" | "min" | "max" | "group" | "stddev" | "stdvar" => {
            // The clean Sum-over-CountSketch reducer is a future
            // follow-up. Surface as UnsupportedComposition so the
            // routing decision is explicit and the follow-up has a
            // clear hook.
            out.unsupported = Some(UnsupportedReason::UnsupportedComposition(format!(
                "{op_name} by (...) (metric) — pending Sum-over-CountSketch reducer; \
                 falling over to archive"
            )));
        }
        "quantile" => {
            // `quantile(q, m)` is the instant-vector aggregate (no
            // matrix selector). Map to QuantileApprox.
            let (metric, gb, range_s) = match extract_metric_keys_range(&agg.expr) {
                Some(x) => x,
                None => {
                    out.unsupported = Some(UnsupportedReason::UnsupportedComposition(
                        "quantile: cannot extract metric selector from body".to_string(),
                    ));
                    return;
                }
            };
            out.candidates.push(WarmTierCandidate {
                metric_name: metric,
                group_by_keys: gb,
                required_capability: Capability::QuantileApprox(SketchKindHandle::Any),
                function: op_name,
                function_args: scalar_args,
                range_seconds: range_s,
            });
        }
        other => {
            out.unsupported = Some(UnsupportedReason::UnsupportedFunction(other.to_string()));
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Walk into `expr` to find a Vector / Matrix selector and return
/// `(metric_name, group_by_keys, range_seconds)`. `range_seconds`
/// is `0` for an instant-vector selector.
fn extract_metric_keys_range(expr: &Expr) -> Option<(String, BTreeSet<String>, u64)> {
    match expr {
        Expr::VectorSelector(vs) => {
            let (m, keys) = extract_vs_metric_and_keys(vs)?;
            Some((m, keys, 0))
        }
        Expr::MatrixSelector(ms) => {
            let (m, keys) = extract_vs_metric_and_keys(&ms.vs)?;
            Some((m, keys, duration_to_seconds(ms.range)))
        }
        Expr::Paren(p) => extract_metric_keys_range(&p.expr),
        Expr::Subquery(sq) => extract_metric_keys_range(&sq.expr),
        Expr::Call(c) => {
            // Walk into single-arg call wrappers (e.g. an inner aggregate).
            c.args.args.iter().find_map(|a| extract_metric_keys_range(a))
        }
        Expr::Aggregate(a) => extract_metric_keys_range(&a.expr),
        _ => None,
    }
}

fn extract_vs_metric_and_keys(vs: &VectorSelector) -> Option<(String, BTreeSet<String>)> {
    let mut keys = BTreeSet::new();
    let mut metric = vs.name.clone().unwrap_or_default();
    for m in &vs.matchers.matchers {
        if m.name == "__name__" {
            if metric.is_empty() {
                metric = m.value.clone();
            }
            continue;
        }
        keys.insert(m.name.clone());
    }
    if metric.is_empty() {
        None
    } else {
        Some((metric, keys))
    }
}

fn duration_to_seconds(d: Duration) -> u64 {
    d.as_secs()
}

/// Walk into `expr` looking for a `rate` / `irate` / `increase` /
/// `deriv` / `delta` / `idelta` / `predict_linear` call anywhere in
/// the subtree. Used to reject the demo's
/// `sum by (zone) (rate(http_requests_total[5m]))` and
/// `histogram_quantile(0.99, sum(rate(bucket[5m])) by (le))` shapes
/// up-front.
fn has_nested_rate(expr: &Expr) -> bool {
    match expr {
        Expr::Call(call) => {
            let name = call.func.name.to_lowercase();
            if matches!(
                name.as_str(),
                "rate" | "irate" | "increase" | "deriv" | "delta" | "idelta" | "predict_linear"
            ) {
                return true;
            }
            call.args.args.iter().any(|a| has_nested_rate(a))
        }
        Expr::Aggregate(agg) => has_nested_rate(&agg.expr),
        Expr::Paren(p) => has_nested_rate(&p.expr),
        Expr::Subquery(sq) => has_nested_rate(&sq.expr),
        Expr::Binary(b) => has_nested_rate(&b.lhs) || has_nested_rate(&b.rhs),
        Expr::Unary(u) => has_nested_rate(&u.expr),
        _ => false,
    }
}

// ── Bidirectional adapters with the backend's sketch_index::Capability ───────
//
// `asap-query-engine` carries its own `Capability` / `SketchKindHandle`
// enums (in `stores::sketch_db::sketch_index`) which the backend's
// ingest + storage paths reference everywhere. Rather than relocate
// those types and churn 18 backend files, we own the canonical
// definition here and adapt at the controller↔backend boundary.
//
// The adapters live as `From` impls on the BACKEND side because that's
// where the source-of-truth `sketch_index::Capability` lives; this
// module just defines the controller-local mirror. See
// `asap-query-engine/src/stores/sketch_db/sketch_index.rs` for the
// `From<controller::warm_tier_analysis::Capability>` impl.

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
        assert_eq!(c.required_capability, Capability::QuantileApprox(SketchKindHandle::Any));
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
        assert_eq!(c.group_by_keys, keys(&["zone", "region"]));
        assert_eq!(c.range_seconds, 30);
    }

    #[test]
    fn analyze_histogram_quantile_over_bare_metric() {
        let a = analyze_promql_for_warm_tier("histogram_quantile(0.99, http_latency_ms)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        let c = &a.candidates[0];
        assert_eq!(c.function, "histogram_quantile");
        assert_eq!(c.function_args, vec![0.99]);
        assert_eq!(c.metric_name, "http_latency_ms");
        assert_eq!(c.range_seconds, 0);
        assert_eq!(c.required_capability, Capability::QuantileApprox(SketchKindHandle::Any));
    }

    #[test]
    fn analyze_cardinality_estimate() {
        let a = analyze_promql_for_warm_tier("cardinality_estimate(uniq_users)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::CardinalityApprox,
        );
    }

    #[test]
    fn analyze_topk_aggregate() {
        let a = analyze_promql_for_warm_tier("topk(5, endpoint_hits)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        let c = &a.candidates[0];
        assert_eq!(c.function, "topk");
        assert_eq!(c.function_args, vec![5.0]);
        assert_eq!(
            c.required_capability,
            Capability::FrequencyTopk(SketchKindHandle::CmsWithHeap),
        );
    }

    #[test]
    fn analyze_topk_over_time() {
        let a = analyze_promql_for_warm_tier("topk_over_time(10, endpoint_hits[1h])");
        assert!(a.unsupported.is_none(), "{a:?}");
        let c = &a.candidates[0];
        assert_eq!(c.function, "topk_over_time");
        assert_eq!(c.function_args, vec![10.0]);
        assert_eq!(c.range_seconds, 3600);
    }

    #[test]
    fn analyze_quantile_instant_aggregate() {
        let a = analyze_promql_for_warm_tier("quantile(0.5, http_latency_ms)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::QuantileApprox(SketchKindHandle::Any),
        );
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
        let a = analyze_promql_for_warm_tier("rate(http_requests_total[5m])");
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("rate".to_string())),
        );
    }

    #[test]
    fn reject_irate_function() {
        let a = analyze_promql_for_warm_tier("irate(http_requests_total[5m])");
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("irate".to_string())),
        );
    }

    #[test]
    fn reject_increase_function() {
        let a = analyze_promql_for_warm_tier("increase(http_requests_total[5m])");
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("increase".to_string())),
        );
    }

    #[test]
    fn reject_sum_by_rate_compound() {
        // The demo's `sum by (zone) (rate(http_requests_total[5m]))`.
        let a = analyze_promql_for_warm_tier(
            "sum by (zone) (rate(http_requests_total[5m]))",
        );
        // Nested `rate` is detected first and surfaced as UnsupportedFunction.
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("rate".to_string())),
            "{a:?}"
        );
    }

    #[test]
    fn reject_histogram_quantile_over_sum_rate() {
        // `histogram_quantile(0.99, sum(rate(bucket[5m])) by (le))`.
        let a = analyze_promql_for_warm_tier(
            "histogram_quantile(0.99, sum(rate(http_latency_bucket[5m])) by (le))",
        );
        // Inner rate is detected, surfaced as UnsupportedFunction.
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("rate".to_string())),
            "{a:?}"
        );
    }

    #[test]
    fn reject_sum_by_bare_metric_pending_sum_reducer() {
        // `sum by (zone) (http_requests_total)` — supported in a future
        // Sum-over-CountSketch follow-up; for now surface as
        // UnsupportedComposition.
        let a = analyze_promql_for_warm_tier(
            "sum by (zone) (http_requests_total)",
        );
        match a.unsupported {
            Some(UnsupportedReason::UnsupportedComposition(msg)) => {
                assert!(
                    msg.contains("sum") && msg.contains("CountSketch"),
                    "expected msg to mention sum + CountSketch, got `{msg}`"
                );
            }
            other => panic!("expected UnsupportedComposition for sum-by, got {other:?}"),
        }
    }

    #[test]
    fn reject_topk_over_rate() {
        // `topk(5, rate(http_requests_total[5m]))` — topk only over
        // instant vectors. Nested rate is detected by the
        // top-of-aggregate-handler `has_nested_rate` check and
        // surfaces as the canonical UnsupportedFunction("rate") so
        // log telemetry attributes the rejection to the
        // root-cause function regardless of which outer op wrapped it.
        let a = analyze_promql_for_warm_tier(
            "topk(5, rate(http_requests_total[5m]))",
        );
        assert_eq!(
            a.unsupported,
            Some(UnsupportedReason::UnsupportedFunction("rate".to_string())),
            "{a:?}"
        );
    }

    #[test]
    fn reject_binary_op() {
        let a = analyze_promql_for_warm_tier("rate(foo[5m]) > 0.5");
        // The binary op contains a rate call — rate is detected first
        // at the AST root or as an UnsupportedComposition; either way
        // we surface an unsupported reason.
        assert!(a.unsupported.is_some(), "{a:?}");
    }

    #[test]
    fn unparseable_promql_surfaces_clean_error() {
        let a = analyze_promql_for_warm_tier("@@@ this is not promql @@@");
        match a.unsupported {
            Some(UnsupportedReason::UnparseablePromql(msg)) => {
                assert!(!msg.is_empty(), "parser error message should be non-empty");
            }
            other => panic!("expected UnparseablePromql, got {other:?}"),
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

    #[test]
    fn instant_vector_has_zero_range() {
        let a = analyze_promql_for_warm_tier("histogram_quantile(0.5, m)");
        assert_eq!(a.candidates[0].range_seconds, 0);
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
}
