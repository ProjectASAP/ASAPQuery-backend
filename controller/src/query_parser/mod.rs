//! SP-1 query workload extraction — PromQL and SQL parsers.
//!
//! # Entry points
//!
//! | Function | Returns | Use |
//! |---|---|---|
//! | [`parse_query_expr`] | `QueryExpr` | Full algebra IR |
//! | [`parse_query`] | `ParsedQuery` | Backward compat with existing analyzer |
//!
//! # Supported PromQL patterns (via `promql-parser` AST)
//! - `quantile_over_time(φ, m{f}[w]) by (dims)`
//! - `histogram_quantile(φ, rate(m{f}[w])) by (le)`
//! - `avg/min/max/stddev/stdvar_over_time(m{f}[w]) by (dims)`
//! - `sum/count_over_time(m{f}[w]) by (dims)`
//! - `topk(k, *_over_time(…) by (dims))`
//! - `count(*_over_time(…) by (dims))` — cardinality
//! - `changes/resets(m{f}[w])`
//! - Bare metric selector / binary op → `exact_required`
//!
//! # Supported SQL patterns (doc §SQL Operators)
//! - `COUNT(*)` with/without GROUP BY → frequency / exact
//! - `COUNT(DISTINCT col)` ± GROUP BY → cardinality / Hydra
//! - `AVG/MIN/MAX(col)` ± GROUP BY → quantile / exact extrema
//! - `SUM(col)` → exact
//! - ORDER BY … DESC LIMIT k → heavy-hitter CountSketch
//! - Multiple aggs in one SELECT → all ops collected (Merge)
//! - JOIN … ON key → backend-side Join (sketch-aware push-down: see physical planner)
//! - UNION ALL → Merge (sketch linearity)

pub mod promql;
pub mod sql;
pub mod language;

// Re-export the L1 language façade at this module's top so call sites
// that historically used `crate::query_language::*` (folded in by the
// 2026-05 refactor) keep a one-level import.
pub use language::{Language, LanguageAst, ParseError, PromQLLanguage, SqlLanguage, ElasticDslLanguage};

use std::collections::HashMap;
use std::time::Duration;

use crate::intent_algebra::legacy_expr::{AggIntent, QueryExpr};
use crate::types::AggType;

// ── Output types (legacy — consumed by analyzer and planner) ──────────────────

/// Flat intermediate representation consumed by [`crate::analyzer::Analyzer`].
///
/// Produced by [`parse_query`] via [`QueryExpr`] tree walking.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Metric name (PromQL: from selector; SQL: FROM clause table).
    pub metric_name: String,
    /// Aggregation types inferred from the query.
    pub aggregations: Vec<AggType>,
    /// Dimensions that must be preserved for GROUP BY / `by (dims)`.
    pub group_by_labels: Vec<String>,
    /// Equality label filters extracted from the query.
    pub label_filters: HashMap<String, String>,
    /// Time window extracted from the range vector or query context.
    pub time_window: Duration,
    /// True when the query requires per-sample exact values.
    pub exact_required: bool,
    /// Quantile φ values implied by the query.
    pub quantiles: Vec<f64>,
    /// Named pattern hint for domain-specific planner defaults.
    pub hint: Option<QueryHint>,
}

/// Named query pattern recognised by the DEBS-aware planner.
#[derive(Debug, Clone)]
pub enum QueryHint {
    // ── DEBS 2022 financial queries ───────────────────────────────────────────
    /// Q1 – per-symbol EMA via quantile proxy (DDSketch / KLL).
    DebsEma,
    /// Q3 – top-K symbols by event count or price move (CountSketch).
    DebsTopK { k: u64 },
    /// Q4 – per-symbol high / low / last / range (extreme-quantile DDSketch).
    DebsPriceStats,
    /// Q5 / Q9 – realized volatility / Bollinger bands via IQR proxy.
    DebsVolatility,
    /// Q6 – distinct active symbols per window (HLL).
    DebsCardinality,
    /// Q7 – TWAP as median / p50 (DDSketch).
    DebsTwap,
    /// Q8 – price anomaly detection via IQR (DDSketch).
    DebsAnomaly,
    // ── Exact-only patterns ───────────────────────────────────────────────────
    /// Query requires stateful per-sample computation; no sketch benefit.
    ExactRequired { reason: String },
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Parse a raw query string (PromQL or SQL) into the general [`QueryExpr`] IR.
///
/// Both parsers emit Layer 2 relational operators (`Aggregate { AggFunc }`).
/// The shared lowering pass converts `Aggregate` → `SketchAgg { AggIntent }`
/// where applicable.
pub fn parse_query_expr(query: &str) -> anyhow::Result<QueryExpr> {
    let q = query.trim();
    let upper = q.to_ascii_uppercase();
    let layer2 = if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        sql::parse_sql_expr(q)?
    } else {
        promql::parse_promql_expr(q)?
    };
    // Layer 2 → Layer 3 lowering (shared by both languages).
    Ok(crate::intent_algebra::legacy_lower::lower_to_sketch_algebra(layer2))
}

/// Parse a raw query string (PromQL or SQL) into a [`ParsedQuery`].
///
/// This is the backward-compatible entry point for the existing
/// [`crate::analyzer::Analyzer`].  Internally it parses via [`parse_query_expr`]
/// and extracts the flat summary by walking the [`QueryExpr`] tree.
pub fn parse_query(query: &str) -> anyhow::Result<ParsedQuery> {
    let qe = parse_query_expr(query)?;
    Ok(qe_to_parsed_query(&qe))
}

/// Extract a flat [`ParsedQuery`] by walking a [`QueryExpr`] tree.
fn qe_to_parsed_query(qe: &QueryExpr) -> ParsedQuery {
    let mut c = QeCollector::default();
    c.visit(qe);
    c.build()
}

#[derive(Default)]
struct QeCollector {
    metric_name:     Option<String>,
    agg_types:       Vec<AggType>,
    group_by_labels: Vec<String>,
    label_filters:   HashMap<String, String>,
    time_window:     Option<Duration>,
    exact_required:  bool,
    quantiles:       Vec<f64>,
    topk:            Option<u64>,
    /// True when currently visiting inside a TopK node (affects Count handling).
    inside_topk:     bool,
}

impl QeCollector {
    fn visit(&mut self, expr: &QueryExpr) {
        use crate::intent_algebra::legacy_expr::{FilterOp, FilterVal, LiteralValue, ScalarExpr};
        match expr {
            QueryExpr::Source(s) => {
                if self.metric_name.is_none() {
                    self.metric_name = Some(s.name.clone());
                }
            }
            QueryExpr::Filter { pred, input } => {
                // Extract equality label filters from the predicate tree.
                collect_filters_from_scalar(pred, &mut self.label_filters);
                self.visit(input);
            }
            QueryExpr::Window { duration, input, .. } => {
                if self.time_window.is_none() {
                    self.time_window = Some(*duration);
                }
                self.visit(input);
            }
            QueryExpr::Partition { keys, input } => {
                for k in keys.keys() {
                    if !self.group_by_labels.contains(k) {
                        self.group_by_labels.push(k.clone());
                    }
                }
                self.visit(input);
            }
            QueryExpr::SketchAgg { op, input, .. } => {
                self.collect_op(op);
                self.visit(input);
            }
            QueryExpr::WindowedAgg { agg, window, input, .. } => {
                if self.time_window.is_none() {
                    if let crate::intent_algebra::legacy_expr::WindowKind::Tumbling { size } = &window.kind {
                        self.time_window = Some(*size);
                    }
                }
                self.collect_op(agg);
                self.visit(input);
            }
            QueryExpr::TopK { k, input, .. } => {
                self.topk = Some(*k);
                let prev = self.inside_topk;
                self.inside_topk = true;
                self.visit(input);
                self.inside_topk = prev;
            }
            QueryExpr::Distinct { input, .. } => self.visit(input),
            QueryExpr::Merge { inputs } => {
                for i in inputs { self.visit(i); }
            }
            QueryExpr::Aggregate { keys, aggs, input, .. } => {
                for k in keys {
                    if !self.group_by_labels.contains(k) {
                        self.group_by_labels.push(k.clone());
                    }
                }
                let has_group_by = !keys.is_empty();
                for agg in aggs {
                    self.collect_agg_func_with_group(&agg.func, has_group_by);
                }
                self.visit(input);
            }
            QueryExpr::Project { input, .. }
            | QueryExpr::Sort { input, .. }
            | QueryExpr::Limit { input, .. }
            | QueryExpr::HistogramQuantile { input, .. }
            | QueryExpr::PromQLSubquery { input, .. } => self.visit(input),
            QueryExpr::Join { left, right, .. }
            | QueryExpr::SetOp { left, right, .. }
            | QueryExpr::BinaryOp { lhs: left, rhs: right, .. } => {
                self.visit(left);
                self.visit(right);
            }
            QueryExpr::LetBinding { expr, body, .. } => {
                self.visit(expr);
                self.visit(body);
            }
            QueryExpr::Ref(_) => {}
        }
    }

    fn collect_agg_func_with_group(&mut self, func: &crate::intent_algebra::legacy_expr::AggFunc, has_group_by: bool) {
        use crate::intent_algebra::legacy_expr::AggFunc;
        // COUNT(*) without GROUP BY → exact (no sketch benefit), unless inside topk
        // where Count means frequency counting.
        if matches!(func, AggFunc::Count) && !has_group_by && !self.inside_topk {
            self.exact_required = true;
            return;
        }
        self.collect_agg_func(func);
    }

    fn collect_agg_func(&mut self, func: &crate::intent_algebra::legacy_expr::AggFunc) {
        use crate::intent_algebra::legacy_expr::AggFunc;
        match func {
            AggFunc::CountDistinct => {
                if !self.agg_types.contains(&AggType::Cardinality) {
                    self.agg_types.push(AggType::Cardinality);
                }
            }
            AggFunc::Count => {
                if !self.agg_types.contains(&AggType::Frequency) {
                    self.agg_types.push(AggType::Frequency);
                }
            }
            AggFunc::HeavyHitters { .. } => {
                if !self.agg_types.contains(&AggType::Frequency) {
                    self.agg_types.push(AggType::Frequency);
                }
            }
            AggFunc::Quantile(phi) => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(phi) {
                    self.quantiles.push(*phi);
                }
            }
            AggFunc::Avg => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                // AVG maps to p50 (median) sketch
                if !self.quantiles.contains(&0.5) { self.quantiles.push(0.5); }
            }
            AggFunc::Min => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&0.0) { self.quantiles.push(0.0); }
            }
            AggFunc::Max => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&1.0) { self.quantiles.push(1.0); }
            }
            AggFunc::StdDev { .. } | AggFunc::Variance { .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                // StdDev/Variance use IQR proxy via p25/p75.
                if !self.quantiles.contains(&0.25) { self.quantiles.push(0.25); }
                if !self.quantiles.contains(&0.75) { self.quantiles.push(0.75); }
            }
            AggFunc::Sum | AggFunc::Rate | AggFunc::Increase | AggFunc::Delta
            | AggFunc::Custom(_) => {
                self.exact_required = true;
            }
        }
    }

    fn collect_op(&mut self, op: &AggIntent) {
        // Canonical Quantile is single-φ post Step α (multi-φ legacy
        // intents fan out into sibling SketchAggs at construction time —
        // each one routes through this collector independently). The
        // legacy `Extrema { min, max }` enum split into `Min` / `Max`
        // canonical variants; map each to the corresponding boundary
        // quantile for legacy compat.
        match op {
            AggIntent::Cardinality { .. } => {
                if !self.agg_types.contains(&AggType::Cardinality) {
                    self.agg_types.push(AggType::Cardinality);
                }
            }
            AggIntent::Frequency { .. } => {
                if !self.agg_types.contains(&AggType::Frequency) {
                    self.agg_types.push(AggType::Frequency);
                }
            }
            AggIntent::Quantile { q, .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(q) { self.quantiles.push(*q); }
            }
            AggIntent::Min => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&0.0) { self.quantiles.push(0.0); }
            }
            AggIntent::Max => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&1.0) { self.quantiles.push(1.0); }
            }
            // Sum / Count / Avg / TopK / Rate / Increase / archive-only —
            // all flip the exact_required flag (no sketch benefit at the
            // legacy planner's level).
            _ => { self.exact_required = true; }
        }
    }

    fn build(self) -> ParsedQuery {
        let metric_name = self.metric_name.unwrap_or_default();
        let mut qs = self.quantiles;
        qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        qs.dedup();
        let hint = debs_hint(
            &metric_name,
            &self.agg_types,
            &qs,
            self.exact_required,
            self.topk,
        );
        ParsedQuery {
            metric_name,
            aggregations:    self.agg_types,
            group_by_labels: self.group_by_labels,
            label_filters:   self.label_filters,
            time_window:     self.time_window.unwrap_or(Duration::from_secs(300)),
            exact_required:  self.exact_required,
            quantiles:       qs,
            hint,
        }
    }
}

fn collect_filters_from_scalar(
    pred: &crate::intent_algebra::legacy_expr::ScalarExpr,
    out:  &mut HashMap<String, String>,
) {
    use crate::intent_algebra::legacy_expr::{BinaryOpKind, LiteralValue, ScalarExpr};
    match pred {
        ScalarExpr::BinaryOp { op: BinaryOpKind::Eq, lhs, rhs } => {
            if let (ScalarExpr::Column(col), ScalarExpr::Literal(LiteralValue::Str(v))) =
                (lhs.as_ref(), rhs.as_ref())
            {
                out.insert(col.clone(), v.clone());
            }
        }
        ScalarExpr::BinaryOp { op: BinaryOpKind::And, lhs, rhs } => {
            collect_filters_from_scalar(lhs, out);
            collect_filters_from_scalar(rhs, out);
        }
        _ => {}
    }
}

// ── DEBS hint classifier (shared by both parsers via to_parsed_query) ─────────

/// Returns the DEBS-specific hint for `financial.last_trade_price` queries.
pub(super) fn debs_hint(
    metric:         &str,
    aggs:           &[AggType],
    quantiles:      &[f64],
    exact_required: bool,
    topk:           Option<u64>,
) -> Option<QueryHint> {
    let is_debs = metric == "financial.last_trade_price"
        || metric == "financial_last_trade_price";
    if !is_debs { return None; }

    if exact_required {
        return Some(QueryHint::ExactRequired {
            reason: "query requires per-sample stateful computation".into(),
        });
    }
    if let Some(k) = topk {
        return Some(QueryHint::DebsTopK { k });
    }
    let primary = aggs.first()?;
    match primary {
        AggType::Cardinality => Some(QueryHint::DebsCardinality),
        AggType::Frequency   => Some(QueryHint::DebsTopK { k: 10 }),
        AggType::Quantile    => {
            let qs: std::collections::HashSet<i32> = quantiles
                .iter()
                .map(|&q| (q * 100.0).round() as i32)
                .collect();
            if qs.contains(&50) && qs.len() == 1 {
                Some(QueryHint::DebsTwap)
            } else if qs.contains(&0) || qs.contains(&100) {
                Some(QueryHint::DebsPriceStats)
            } else if qs.contains(&25) && qs.contains(&75) && qs.contains(&50) {
                Some(QueryHint::DebsAnomaly)
            } else if qs.contains(&25) && qs.contains(&75) {
                Some(QueryHint::DebsVolatility)
            } else {
                Some(QueryHint::DebsEma)
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke tests for the unified entry point.

    #[test]
    fn sql_dispatched_correctly() {
        let pq = parse_query("SELECT COUNT(*) FROM hits GROUP BY AdvEngineID").unwrap();
        assert!(pq.aggregations.contains(&AggType::Frequency));
    }

    #[test]
    fn promql_dispatched_correctly() {
        // `by` belongs to the aggregate operator, not the function call.
        let pq = parse_query(
            "sum by (host) (quantile_over_time(0.99, latency[5m]))"
        ).unwrap();
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert_eq!(pq.quantiles, vec![0.99]);
    }

    #[test]
    fn parse_query_expr_returns_expr() {
        let pq = parse_query(
            "topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))"
        ).unwrap();
        // Should parse without error and extract the metric name.
        assert_eq!(pq.metric_name, "financial_last_trade_price");
    }
}

#[cfg(test)]
mod doc_verify_all {
    use super::*;
    use crate::intent_algebra::legacy_expr::*;

    #[test]
    fn example4_promql_quantile() {
        let expr = parse_query_expr(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])"
        ).unwrap();
        // Doc: WindowedAgg { Quantile([0.99]), Tumbling(5m), Filter(Source) }
        assert!(matches!(&expr, QueryExpr::WindowedAgg { agg: AggIntent::Quantile { .. }, .. }));
    }

    #[test]
    fn example5_promql_topk() {
        let expr = parse_query_expr(
            "topk by (service) (10, count_over_time(requests{env=\"prod\"}[1m]))"
        ).unwrap();
        // Doc: TopK { 10, Partition { ["service"], WindowedAgg { Frequency } } }
        match &expr {
            QueryExpr::TopK { k: 10, input, .. } => {
                match input.as_ref() {
                    QueryExpr::Partition { keys, input: inner } => {
                        assert_eq!(keys.keys(), &["service".to_string()]);
                        assert!(matches!(inner.as_ref(), QueryExpr::WindowedAgg { agg: AggIntent::Frequency { .. }, .. }));
                    }
                    other => panic!("expected Partition, got {other:?}"),
                }
            }
            other => panic!("expected TopK, got {other:?}"),
        }
    }

    #[test]
    fn example6_sql_avg() {
        let expr = parse_query_expr(
            "SELECT symbol, AVG(price) FROM trades GROUP BY symbol"
        ).unwrap();
        // Doc: Partition { ["symbol"], SketchAgg { Quantile([0.5]), Source } }
        match &expr {
            QueryExpr::Partition { keys, input } => {
                assert_eq!(keys.keys(), &["symbol".to_string()]);
                assert!(matches!(input.as_ref(), QueryExpr::SketchAgg { op: AggIntent::Quantile { .. }, .. }));
            }
            other => panic!("expected Partition, got {other:?}"),
        }
    }

    #[test]
    fn example7_sql_tumble() {
        let expr = parse_query_expr(
            "SELECT region, COUNT(DISTINCT user_id) AS cnt FROM sessions GROUP BY region, TUMBLE(ts, INTERVAL '5' MINUTE) ORDER BY cnt DESC LIMIT 10"
        ).unwrap();
        // Doc: Limit { 10, Sort { Partition { ["region"], WindowedAgg { Cardinality } } } }
        match &expr {
            QueryExpr::Limit { n: 10, input, .. } => {
                match input.as_ref() {
                    QueryExpr::Sort { input: sort_inner, .. } => {
                        match sort_inner.as_ref() {
                            QueryExpr::Partition { keys, input: part_inner } => {
                                assert_eq!(keys.keys(), &["region".to_string()]);
                                assert!(matches!(part_inner.as_ref(), QueryExpr::WindowedAgg { agg: AggIntent::Cardinality { .. }, .. }));
                            }
                            other => panic!("expected Partition, got {other:?}"),
                        }
                    }
                    other => panic!("expected Sort, got {other:?}"),
                }
            }
            other => panic!("expected Limit, got {other:?}"),
        }
    }
}
