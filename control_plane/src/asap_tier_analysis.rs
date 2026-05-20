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
use crate::query_parser::{parse_query, parse_query_expr_canonical};

pub use crate::sketch_algebra::capability::{
    capability_for, Capability, OuterAgg, OuterFn, SketchKindHandle,
};

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
    /// PromQL outer-function flavour — `Rate` if the expression
    /// contains `rate(...)` / `irate(...)` anywhere in the tree,
    /// `Plain` otherwise. Preserves the rate-vs-plain distinction the
    /// `AggIntent::Sum` collapse erases, so the engine's reducer
    /// dispatch can branch on the typed candidate instead of re-parsing
    /// the raw PromQL string. See [`OuterFn`] for the taxonomy.
    pub outer_fn: OuterFn,
    /// PromQL outer-AGGREGATION operator — `Max(...)` / `Min(...)` /
    /// `Avg(...)` / `Count(...)` / `Group(...)` / `Stddev(...)` /
    /// `Stdvar(...)` when the original query is shaped
    /// `<agg-op> by (labels) (<inner>)` and the inner is a function the
    /// analyzer already binds to a candidate (e.g.
    /// `max by (zone) (quantile_over_time(0.99, m[5m]))`). `None`
    /// otherwise.
    ///
    /// The engine's evaluator applies the fold AFTER the inner function
    /// produces its per-row result — grouping rows by the projected
    /// by-labels and folding each group's values. For the identity case
    /// (inner already emits one row per by-group, e.g. asap's per-zone
    /// DDSketch sketch), the fold returns the single value unchanged.
    /// Closes [#296](https://github.com/ProjectASAP/ASAPQuery-backend/issues/296).
    ///
    /// `sum` is intentionally NOT a variant of `OuterAgg`: the lowerer
    /// collapses `sum`-shaped outers into `AggIntent::Sum` →
    /// `Capability::ExactAgg(Sum)`, which has its own engine dispatch
    /// (see `evaluate_exact_agg`); adding it here would double-dispatch.
    pub outer_agg: OuterAgg,
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

    // Step 2: lower to the canonical L3 `QueryExpr` via the real parse
    // path. `parse_query` (above) already ran this conversion internally
    // to build its flat summary; we re-run it here to get the *tree*
    // itself, which the flat `ParsedQuery` doesn't carry. The walk below
    // only inspects `AggIntent` kinds + accuracy; the converter pins
    // sketch-eligible intents at a non-exact epsilon, which is all
    // warm-tier analysis needs (the real per-query accuracy bound comes
    // from QueryWorkload further downstream).
    let expr = match parse_query_expr_canonical(metricsql) {
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
                    outer_fn: trace.outer_fn,
                    outer_agg: trace.outer_agg.clone(),
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
        // A-variants lifted in Batch 2 of the relational migration. They
        // carry no AggIntent themselves — recurse into their children to
        // find Aggregates further down the tree.
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Partition { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Subquery { child, .. } => collect_agg_intents(child, out),
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

/// Metadata recovered from the raw PromQL AST that the lowered
/// `AggIntent` doesn't carry: the outer function name, leading scalar
/// args, matrix selector's `[r]` range in seconds, and the rate-vs-plain
/// outer-function flavour ([`OuterFn`]) used by engine reducer
/// dispatch.
///
/// The `function` / `function_args` / `range_seconds` fields are for
/// telemetry + the reducer's range hint. The `outer_fn` field is the
/// load-bearing signal that lets the engine pick `evaluate_exact_agg`
/// vs `evaluate_exact_agg_rate` for `Capability::ExactAgg(Sum)`
/// candidates — preserving the rate-vs-plain distinction that the
/// `AggIntent::Sum` collapse erases.
#[derive(Debug, Default)]
struct PromqlTrace {
    function: String,
    function_args: Vec<f64>,
    range_seconds: u64,
    /// Counter-function flavour recovered from the expression tree
    /// (issue #301): `Rate` for `rate`/`irate`, `Increase` for
    /// `increase`, `SumOverTime` for `sum_over_time`, else `Plain`
    /// (bare selector / instant `sum`). The most-specific counter idiom
    /// found anywhere in the tree wins (see [`set_counter_fn`]) so
    /// composed shapes like `sum by (..) (rate(..))` report `Rate`.
    /// Done here once so the engine reads it off the typed candidate
    /// instead of re-parsing the raw query string.
    outer_fn: OuterFn,
    /// PromQL outer-aggregation operator wrapping the inner function —
    /// `max`/`min`/`avg`/`count`/`group`/`stddev`/`stdvar` only. `sum`
    /// is intentionally excluded; it has its own ExactAgg dispatch.
    /// `OuterAgg::None` for queries with no such wrapper.
    ///
    /// Captured ONLY for the OUTERMOST aggregation node — composed
    /// shapes like `max by (a) (avg by (b) (q...))` capture only `max`
    /// because the engine's fold is one-pass over the inner result.
    /// Deeper nesting is a documented follow-up.
    outer_agg: OuterAgg,
}

fn trace_from_promql(metricsql: &str) -> PromqlTrace {
    let ast = match parser::parse(metricsql) {
        Ok(a) => a,
        Err(_) => return PromqlTrace::default(),
    };
    let mut t = PromqlTrace::default();
    // Lift the OUTERMOST aggregation operator into `outer_agg` before
    // the recursive walker descends into the inner expression — the
    // walker captures inner-most function-name / range / rate-flag
    // semantics, while `outer_agg` is a property of the root node only.
    // See `extract_outer_agg` for the operator → `OuterAgg` mapping
    // and the explicit-exclusion of `sum` (which has its own
    // ExactAgg dispatch).
    t.outer_agg = extract_outer_agg(&ast);
    // When outer_agg lifts the outermost Aggregate (e.g. `max by (zone) (
    // quantile_over_time(...))`), the walker must descend INTO the
    // aggregate's inner expression — otherwise the Aggregate branch in
    // `walk_ast_for_trace` would set `t.function = "max"` and shadow
    // the inner function name (`quantile_over_time`) that the engine's
    // reducer actually dispatches on. The engine then sees the outer
    // operator name in `candidate.function`, treats it as an unknown
    // function, and CapabilityMisses to archive. Issue #296.
    let walk_root = if t.outer_agg.is_some() {
        unwrap_outermost_aggregate(&ast)
    } else {
        &ast
    };
    walk_ast_for_trace(walk_root, &mut t);
    t
}

/// Companion to [`extract_outer_agg`] — peels a leading `Paren` once,
/// then descends one level into an `Aggregate.expr`. Returns the
/// original `expr` unchanged if neither pattern matches (caller
/// should only call this when `outer_agg.is_some()`, in which case the
/// shape is guaranteed to be `[Paren?]Aggregate{..}`).
fn unwrap_outermost_aggregate(expr: &Expr) -> &Expr {
    let root = match expr {
        Expr::Paren(p) => p.expr.as_ref(),
        other => other,
    };
    match root {
        Expr::Aggregate(a) => &a.expr,
        _ => expr,
    }
}

/// Lift the OUTERMOST PromQL aggregation operator into `OuterAgg`.
///
/// Returns `OuterAgg::None` for any non-aggregation root (bare
/// selector, `Call(...)` with no outer agg, etc.), for `sum`
/// (already handled via the ExactAgg pipeline), and for `topk` /
/// `bottomk` / `quantile` (which have their own dispatch paths or
/// are out-of-scope for the per-row fold).
///
/// The `by`-labels are pulled from the `LabelModifier::Include`
/// list. `without (labels)` is NOT supported today — the engine's
/// fold currently keys on the explicit `by`-labels set, and
/// translating `without` to `by` needs knowledge of the inner
/// result's label universe; deferred to a follow-up.
///
/// A leading `Paren` (e.g. `(max by (zone) (...))`) is unwrapped
/// once so users who put the root in parens get the same shape.
fn extract_outer_agg(expr: &Expr) -> OuterAgg {
    use promql_parser::parser::LabelModifier;
    let root = match expr {
        Expr::Paren(p) => p.expr.as_ref(),
        other => other,
    };
    let agg = match root {
        Expr::Aggregate(a) => a,
        _ => return OuterAgg::None,
    };
    // `by (labels)` → Vec<String>. `without (...)` → no support yet.
    let by_labels: Vec<String> = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.iter().cloned().collect(),
        // `without (...)` — not modeled here. Return None so the engine
        // emits the inner result unchanged and the query falls over to
        // archive if the consumer expected the fold. Documented gap.
        Some(LabelModifier::Exclude(_)) => return OuterAgg::None,
        None => Vec::new(),
    };
    let op = agg.op.to_string().to_lowercase();
    match op.as_str() {
        "max" => OuterAgg::Max(by_labels),
        "min" => OuterAgg::Min(by_labels),
        "avg" => OuterAgg::Avg(by_labels),
        "count" => OuterAgg::Count(by_labels),
        "group" => OuterAgg::Group(by_labels),
        "stddev" => OuterAgg::Stddev(by_labels),
        "stdvar" => OuterAgg::Stdvar(by_labels),
        // `sum` → ExactAgg(Sum) pipeline; `topk`/`bottomk` → engine-side
        // fallback path; `quantile` → out of scope (instant quantile
        // over function results needs per-group sketch merging).
        _ => OuterAgg::None,
    }
}

/// Set `t.outer_fn` honoring counter-idiom precedence (issue #301):
/// `Rate` > `Increase` > `SumOverTime` > `Plain`. The walker may visit
/// nested calls in any order, so a more-specific flavour already set
/// must not be downgraded by a less-specific one seen later. (In
/// practice a single counter query has exactly one of these, but
/// pathological compositions like `increase(sum_over_time(...))` resolve
/// deterministically.)
fn set_counter_fn(t: &mut PromqlTrace, candidate: OuterFn) {
    fn rank(f: OuterFn) -> u8 {
        match f {
            OuterFn::Rate => 3,
            OuterFn::Increase => 2,
            OuterFn::SumOverTime => 1,
            OuterFn::Plain => 0,
        }
    }
    if rank(candidate) > rank(t.outer_fn) {
        t.outer_fn = candidate;
    }
}

fn walk_ast_for_trace(expr: &Expr, t: &mut PromqlTrace) {
    match expr {
        Expr::Call(call) => {
            let name = call.func.name.to_lowercase();
            if t.function.is_empty() {
                t.function = name.clone();
            }
            // Flag the counter-function flavour ANYWHERE in the tree
            // (issue #301) — mirrors the retired `query_contains_rate_call`
            // walker but with the full taxonomy. For composed shapes like
            // `sum by (zone) (rate(metric[r]))` the FIRST function set
            // above is `"sum"` (the outer Aggregate), but `outer_fn` must
            // report the INNER counter function so the engine dispatches
            // correctly. `rate`/`irate` win over `increase`, which wins
            // over `sum_over_time` (most-specific-counter-idiom wins);
            // `set_counter_fn` enforces that precedence so the order in
            // which the walker encounters nested calls doesn't matter.
            match name.as_str() {
                "rate" | "irate" => set_counter_fn(t, OuterFn::Rate),
                "increase" => set_counter_fn(t, OuterFn::Increase),
                "sum_over_time" => set_counter_fn(t, OuterFn::SumOverTime),
                _ => {}
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
        AggregationType::CountSketchWithHeap => {
            Some(Capability::FrequencyTopk(SketchKindHandle::CountSketchWithHeap))
        }
        // Keyed-multi-population variants. The capability the policy
        // *provides* is the multi-pop variant itself; the matching
        // predicate (`Capability::is_satisfied_by`) recognises that
        // a multi-pop indexed capability satisfies a single-pop
        // required capability through `multi_pop_satisfies_single`.
        // So a candidate's `ExactAgg(Sum)` matches a policy whose
        // `policy_capability` returns `ExactAgg(MultipleSum)`.
        AggregationType::MultipleSum => Some(Capability::ExactAgg(AggregationType::MultipleSum)),
        AggregationType::MultipleIncrease => {
            Some(Capability::ExactAgg(AggregationType::MultipleIncrease))
        }
        AggregationType::MultipleMinMax => {
            Some(Capability::ExactAgg(AggregationType::MultipleMinMax))
        }
        // No ASAP-tier capability today. HydraKLL is a keyed-quantile
        // family that needs its own QuantileApprox arm (with a
        // multi-pop equivalent rule) — separate follow-up.
        // `Single/MultipleSubpopulation` are legacy enum wrappers from
        // the pre-refactor config schema and have no semantic shape.
        // (The retired `SetAggregator` / `DeltaSetAggregator` family
        // used to live here too.)
        AggregationType::HydraKLL
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
    use promql_utilities::query_logics::enums::AggregationType;

    fn keys(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    // ── Supported shapes ─────────────────────────────────────────────────

    /// PromQL `count(metric)` is the spec's distinct-counting idiom
    /// (count of label sets in the result vector). The analyzer must
    /// collect EXACTLY ONE candidate — Cardinality — for the outer
    /// count; the bare-metric inner selector must NOT synthesize an
    /// `ExactAgg(Sum)` candidate that would force the engine's
    /// "all candidates must succeed" loop to fail when no Sum policy
    /// is registered. (The fix lives in
    /// `query_parser::promql::walk_qe::Expr::VectorSelector` — gates
    /// the implicit `Aggregate(Sum)` wrapper on `!ctx.outer_count`.)
    #[test]
    fn analyze_count_bare_metric_yields_only_cardinality_candidate() {
        let a = analyze_promql_for_asap_tier("count(unique_users_per_min)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates.len(),
            1,
            "count(metric) must yield exactly one Cardinality candidate \
             (no implicit Sum from the bare-selector inner): {a:?}"
        );
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::CardinalityApprox,
            "{a:?}"
        );
    }

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

    // ── ExactAgg-routed shapes ───────────────────────────────────────────
    //
    // `rate` / `irate` / `increase` / `sum` and the bare selector all
    // lower (via `lower`) to `AggIntent::Sum`, and
    // `capability_for(&Sum)` returns `Capability::ExactAgg(Sum)` — so
    // they are ASAP-tier-answerable from exact-precompute state. (The
    // older `lower_parsed_query` path *dropped* these intents, masking
    // the `ExactAgg` capability and routing everything to archive.)

    #[test]
    fn bare_vector_selector_binds_to_exact_agg() {
        // The PromQL parser models a bare selector as `Aggregate { Sum }`
        // over the sample value; `Sum` carries an `ExactAgg` capability.
        let a = analyze_promql_for_asap_tier("http_requests_total{zone=\"z0\"}");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum)
        );
    }

    #[test]
    fn rate_binds_to_exact_agg() {
        let a = analyze_promql_for_asap_tier("rate(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum)
        );
    }

    #[test]
    fn irate_binds_to_exact_agg() {
        // `irate` shares `AggFunc::Rate` with `rate` in
        // `query_parser::promql`; both lower to `AggIntent::Sum`.
        let a = analyze_promql_for_asap_tier("irate(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum)
        );
    }

    #[test]
    fn increase_binds_to_exact_agg() {
        let a = analyze_promql_for_asap_tier("increase(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum)
        );
    }

    #[test]
    fn sum_by_binds_to_exact_agg() {
        let a = analyze_promql_for_asap_tier("sum by (zone) (http_requests_total)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum)
        );
        assert_eq!(a.candidates[0].group_by_keys, keys(&["zone"]));
    }

    // ── outer_fn — rate vs plain disambiguation ──────────────────────────
    //
    // Regression coverage for the PR that retired the engine's
    // `query_contains_rate_call` raw-PromQL re-parser. The analyzer's
    // lowerer collapses `rate(metric[r])`, `sum_over_time(metric[r])`,
    // `sum(metric)`, and the bare selector all onto `AggIntent::Sum` /
    // `Capability::ExactAgg(Sum)` — so the engine can't tell from the
    // capability alone which the user wrote. The `outer_fn` field on
    // `ASAPTierCandidate` carries the rate-vs-plain distinction so the
    // engine's reducer dispatch is a typed branch instead of a raw-PromQL
    // re-parse.

    #[test]
    fn rate_candidate_carries_outer_fn_rate() {
        let a = analyze_promql_for_asap_tier("rate(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Rate, "{a:?}");
    }

    #[test]
    fn irate_candidate_carries_outer_fn_rate() {
        let a = analyze_promql_for_asap_tier("irate(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Rate, "{a:?}");
    }

    #[test]
    fn sum_over_time_candidate_carries_outer_fn_sum_over_time() {
        // `sum_over_time(metric[r])` shares `Capability::ExactAgg(Sum)`
        // with `rate(metric[r])` — the capability alone can't
        // disambiguate. Post-#301 the `outer_fn` field reports
        // `SumOverTime` so the engine can capability-miss → archive
        // (asap can't reconstruct Σ-of-cumulative-samples from deltas).
        let a = analyze_promql_for_asap_tier("sum_over_time(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum),
            "{a:?}"
        );
        assert_eq!(a.candidates[0].outer_fn, OuterFn::SumOverTime, "{a:?}");
    }

    #[test]
    fn increase_candidate_carries_outer_fn_increase() {
        // `increase(metric[r])` shares `Capability::ExactAgg(Sum)` with
        // `rate`/`sum_over_time`; the `outer_fn` field carries the
        // distinction so the engine sums deltas in `[t-r,t]` WITHOUT the
        // rate divisor (issue #301).
        let a = analyze_promql_for_asap_tier("increase(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum),
            "{a:?}"
        );
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Increase, "{a:?}");
    }

    #[test]
    fn sum_by_over_increase_candidate_carries_outer_fn_increase() {
        // Composed `sum by (zone) (increase(metric[r]))` — inner counter
        // function wins over the outer `sum` (same precedence as the
        // rate case).
        let a = analyze_promql_for_asap_tier(
            "sum by (zone) (increase(http_requests_total[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Increase, "{a:?}");
        assert_eq!(a.candidates[0].range_seconds, 300, "{a:?}");
    }

    #[test]
    fn sum_by_candidate_carries_outer_fn_plain() {
        let a = analyze_promql_for_asap_tier("sum by (zone) (http_requests_total)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Plain, "{a:?}");
    }

    #[test]
    fn bare_selector_candidate_carries_outer_fn_plain() {
        let a = analyze_promql_for_asap_tier("http_requests_total{zone=\"z0\"}");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Plain, "{a:?}");
    }

    #[test]
    fn sum_by_over_rate_candidate_carries_outer_fn_rate() {
        // Composed shape `sum by (zone) (rate(metric[5m]))` — the
        // outer function NAME is `"sum"` (the trace's `.function`
        // field) but `outer_fn` MUST be `Rate` because the inner
        // `rate(...)` call needs the rate-divisor reducer. This is
        // the case that motivated the original `query_contains_rate_call`
        // walker — now satisfied by walking the AST once in the
        // analyzer and emitting the typed `OuterFn::Rate` flag.
        let a = analyze_promql_for_asap_tier(
            "sum by (zone) (rate(http_requests_total[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Sum),
            "{a:?}"
        );
        assert_eq!(a.candidates[0].outer_fn, OuterFn::Rate, "{a:?}");
        // Range is lifted from the inner rate's matrix selector.
        assert_eq!(a.candidates[0].range_seconds, 300, "{a:?}");
    }

    #[test]
    fn rate_and_sum_over_time_share_capability_but_differ_on_outer_fn() {
        // Both collapse to `Capability::ExactAgg(Sum)`; the engine MUST
        // disambiguate via the typed `outer_fn` field, not by string-
        // parsing the raw PromQL. This test pins the asymmetry the
        // engine's dispatch reads off.
        let rate = analyze_promql_for_asap_tier("rate(http_requests_total[5m])");
        let sot = analyze_promql_for_asap_tier(
            "sum_over_time(http_requests_total[5m])",
        );
        assert_eq!(
            rate.candidates[0].required_capability,
            sot.candidates[0].required_capability,
            "rate and sum_over_time should produce the same Capability"
        );
        assert_ne!(
            rate.candidates[0].outer_fn,
            sot.candidates[0].outer_fn,
            "rate and sum_over_time MUST differ on outer_fn so the engine \
             can dispatch correctly without re-parsing the raw PromQL"
        );
        assert_eq!(rate.candidates[0].outer_fn, OuterFn::Rate);
        assert_eq!(sot.candidates[0].outer_fn, OuterFn::SumOverTime);
    }

    // ── outer_agg — outer aggregation operator on function results ──────
    //
    // Regression coverage for issue #296: the asap engine was rejecting
    // `max by (zone) (quantile_over_time(0.99, m[5m]))` because no
    // generic "aggregation operator wraps a function result" path
    // existed. The analyzer now captures the outer agg operator on a
    // typed `OuterAgg` field; the engine's fold pass consumes it
    // after the inner function returns its per-row result.

    #[test]
    fn max_by_quantile_over_time_carries_outer_agg_max() {
        let a = analyze_promql_for_asap_tier(
            "max by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1);
        let c = &a.candidates[0];
        // Inner function still binds to QuantileApprox — outer_agg
        // doesn't alter the candidate's required_capability (the
        // engine's fold runs over the inner result).
        assert_eq!(
            c.required_capability,
            Capability::QuantileApprox(SketchKindHandle::Any),
            "{c:?}"
        );
        // OuterAgg captured.
        match &c.outer_agg {
            OuterAgg::Max(labels) => {
                assert_eq!(labels, &vec!["zone".to_string()]);
            }
            other => panic!("expected OuterAgg::Max([zone]), got {other:?}"),
        }
    }

    #[test]
    fn avg_by_quantile_over_time_carries_outer_agg_avg() {
        let a = analyze_promql_for_asap_tier(
            "avg by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        match &a.candidates[0].outer_agg {
            OuterAgg::Avg(labels) => assert_eq!(labels, &vec!["zone".to_string()]),
            other => panic!("expected OuterAgg::Avg([zone]), got {other:?}"),
        }
    }

    #[test]
    fn min_by_quantile_over_time_carries_outer_agg_min() {
        let a = analyze_promql_for_asap_tier(
            "min by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        match &a.candidates[0].outer_agg {
            OuterAgg::Min(labels) => assert_eq!(labels, &vec!["zone".to_string()]),
            other => panic!("expected OuterAgg::Min([zone]), got {other:?}"),
        }
    }

    #[test]
    fn count_by_rate_carries_outer_agg_count() {
        let a = analyze_promql_for_asap_tier(
            "count by (zone) (rate(http_requests_total[5m]))",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        match &a.candidates[0].outer_agg {
            OuterAgg::Count(labels) => assert_eq!(labels, &vec!["zone".to_string()]),
            other => panic!("expected OuterAgg::Count([zone]), got {other:?}"),
        }
    }

    #[test]
    fn sum_over_time_carries_outer_agg_none() {
        // No outer aggregation wrapper → OuterAgg::None.
        let a = analyze_promql_for_asap_tier("sum_over_time(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_agg, OuterAgg::None);
    }

    #[test]
    fn sum_by_zone_does_not_set_outer_agg_sum() {
        // `sum` MUST NOT populate OuterAgg — that operator routes through
        // the ExactAgg(Sum) pipeline; double-dispatching would re-fold
        // values that the per-window reducer has already accumulated.
        let a = analyze_promql_for_asap_tier("sum by (zone) (http_requests_total)");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(
            a.candidates[0].outer_agg,
            OuterAgg::None,
            "sum must not populate OuterAgg — handled by ExactAgg(Sum) pipeline"
        );
    }

    #[test]
    fn bare_quantile_over_time_carries_outer_agg_none() {
        let a = analyze_promql_for_asap_tier(
            "quantile_over_time(0.99, http_latency_ms[5m])",
        );
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates[0].outer_agg, OuterAgg::None);
    }

    // ── Unsupported / rejected shapes ────────────────────────────────────

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
    fn is_asap_tier_answerable_true_for_count_over_time() {
        // `count_over_time(...)` now lowers to
        // `AggIntent::Frequency{Epsilon}` (per-series sample count
        // over the window), which `capability_for` maps to
        // `Capability::FrequencyEstimate(Any)` — answerable by any
        // frequency-family sketch (CMS / CountSketch, heap-less).
        let a = analyze_promql_for_asap_tier("count_over_time(m[5m])");
        assert!(a.is_asap_tier_answerable(), "{a:?}");
    }

    #[test]
    fn is_asap_tier_answerable_true_for_bare_selector() {
        // A bare selector lowers to `Aggregate { Sum }`, which carries an
        // `ExactAgg` capability — so it is ASAP-tier-answerable.
        let a = analyze_promql_for_asap_tier("m{zone=\"z0\"}");
        assert!(a.is_asap_tier_answerable());
    }

    // ── Cardinality / count_over_time real-PromQL acceptance ────────────

    /// `count_over_time(metric[range])` is the PromQL per-series
    /// sample-count idiom — exactly what a heap-less CMS / CountSketch
    /// estimates. The parser maps it to `AggFunc::Frequency` which
    /// lowers to `AggIntent::Frequency{Epsilon}`; `capability_for`
    /// returns `Capability::FrequencyEstimate(Any)`. The warm engine
    /// binds the query to any frequency-family policy registered for
    /// the metric.
    #[test]
    fn count_over_time_binds_to_frequency_estimate() {
        let a = analyze_promql_for_asap_tier("count_over_time(http_requests_total[5m])");
        assert!(a.unsupported.is_none(), "{a:?}");
        assert_eq!(a.candidates.len(), 1, "{a:?}");
        assert_eq!(
            a.candidates[0].required_capability,
            Capability::FrequencyEstimate(SketchKindHandle::Any),
            "{a:?}"
        );
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
                outer_fn: OuterFn::default(),
                outer_agg: OuterAgg::default(),
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
        fn policy_capability_maps_multiple_sum_to_exact_agg_multiple_sum() {
            let c = cfg("m", AggregationType::MultipleSum, vec!["zone"], 60, "");
            assert_eq!(
                policy_capability(&c),
                Some(Capability::ExactAgg(AggregationType::MultipleSum))
            );
        }

        #[test]
        fn multiple_sum_policy_satisfies_unkeyed_sum_query() {
            // MultipleSum policy keeps per-zone state; an unkeyed Sum
            // query can re-aggregate across zones. The is_satisfied_by
            // multi-pop-satisfies-single rule + the group_by ⊆ policy
            // grouping check let it through.
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
                &[],
                Capability::ExactAgg(AggregationType::Sum),
                60,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn multiple_increase_policy_satisfies_keyed_increase_query() {
            let policies = vec![cfg(
                "http_requests_total",
                AggregationType::MultipleIncrease,
                vec!["zone", "service"],
                60,
                "",
            )];
            let registry = PolicyRegistry::from_configs(policies);
            // Query asks for per-zone increase; policy keeps {zone,
            // service} (superset).
            let cand = candidate(
                "http_requests_total",
                &["zone"],
                Capability::ExactAgg(AggregationType::Increase),
                60,
            );
            assert_eq!(find_matching_policies(&registry, &cand).len(), 1);
        }

        #[test]
        fn single_pop_policy_does_not_satisfy_keyed_query() {
            // Unkeyed Sum policy can't answer per-zone Sum — keys
            // already collapsed. Group_by ⊆ policy_grouping_labels
            // check rejects this even though capabilities would
            // structurally satisfy.
            let policies = vec![cfg("http_lat", AggregationType::Sum, vec![], 60, "")];
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
        fn ignores_unsupported_multi_pop_variants() {
            // `HydraKLL` has `policy_capability == None` because no
            // Capability variant covers its shape today. Matching
            // skips it. (Historically `SetAggregator` /
            // `DeltaSetAggregator` were also in this bucket; they've
            // been retired.)
            let policies = vec![cfg(
                "http_lat",
                AggregationType::HydraKLL,
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
