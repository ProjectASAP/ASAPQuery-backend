//! SP-1 query workload extraction — PromQL and SQL parsers.
//!
//! # Entry points
//!
//! | Function | Returns | Use |
//! |---|---|---|
//! | [`parse_query_expr_canonical`] | canonical `query_expr::QueryExpr` | Full algebra IR |
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

use std::collections::HashMap;
use std::time::Duration;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::{
    BinaryOpKind, ColumnRef, LiteralValue, Predicate, QueryExpr, Source,
};
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

/// Parse a raw query string (PromQL or SQL) into the **legacy Layer-2**
/// [`legacy_expr::QueryExpr`](crate::intent_algebra::legacy_expr::QueryExpr) IR.
///
/// Both parsers emit Layer-2 relational operators (`Aggregate { AggFunc }`,
/// `Window`, `Filter`, `Join`, …). The Layer-2 → Layer-3 sketch lowering
/// and the conversion to the canonical IR both live inside
/// [`intent_algebra::convert_root`](crate::intent_algebra::convert_root) —
/// this function is just the language-dispatch front door.
///
/// Internal to the crate: the only caller is
/// [`parse_query_expr_canonical`], which is the public canonical-IR entry.
pub(crate) fn parse_query_expr(
    query: &str,
) -> anyhow::Result<crate::intent_algebra::legacy_expr::QueryExpr> {
    let q = query.trim();
    let upper = q.to_ascii_uppercase();
    if upper.starts_with("SELECT") || upper.starts_with("WITH") {
        sql::parse_sql_expr(q)
    } else {
        promql::parse_promql_expr(q)
    }
}

/// Parse a raw query string (PromQL or SQL) into the **canonical** L3
/// [`query_expr::QueryExpr`](crate::intent_algebra::query_expr::QueryExpr) IR.
///
/// This is the single public algebra-IR entry point. It parses the query
/// into the crate-internal legacy Layer-2 tree via [`parse_query_expr`],
/// then runs that through
/// [`intent_algebra::convert_root`](crate::intent_algebra::convert_root),
/// which folds the Layer-2 → Layer-3 sketch lowering and the
/// legacy → canonical conversion into one entry. The legacy IR is never
/// observable to callers.
pub fn parse_query_expr_canonical(
    query: &str,
) -> anyhow::Result<crate::intent_algebra::query_expr::QueryExpr> {
    let legacy = parse_query_expr(query)?;
    // `ConvertError` derives `thiserror::Error`, so `?` lifts it straight
    // into `anyhow::Error`.
    let canonical = crate::intent_algebra::convert_root(&legacy)?;
    Ok(canonical)
}

/// Parse a raw query string (PromQL or SQL) into a [`ParsedQuery`].
///
/// This is the backward-compatible entry point for the existing
/// [`crate::analyzer::Analyzer`].  Internally it parses via
/// [`parse_query_expr_canonical`] and extracts the flat summary by walking
/// the canonical [`QueryExpr`] tree.
pub fn parse_query(query: &str) -> anyhow::Result<ParsedQuery> {
    let qe = parse_query_expr_canonical(query)?;
    Ok(qe_to_parsed_query(&qe))
}

/// Extract a flat [`ParsedQuery`] by walking a canonical [`QueryExpr`] tree.
///
/// Step γ7: the collector walks the canonical IR. `Aggregate` carries
/// `Vec<AggIntent>` directly (the legacy `AggFunc` is gone), so the
/// `collect_agg_func*` helpers are replaced by per-intent [`collect_op`]
/// calls. Canonical `Scan` also carries `label_filters` inline, so they
/// are picked up at the scan leaf as well as from any `Filter` predicate.
fn qe_to_parsed_query(qe: &QueryExpr) -> ParsedQuery {
    // The Binder-built `Scan.schema` is the complete, self-contained
    // column universe every `ColumnId` in the tree indexes into. We grab
    // it up-front so the `Aggregate` walk can recover group-by *names*
    // from positional `by` ids.
    let schema = root_scan_schema(qe);
    let mut c = QeCollector::default();
    c.visit(qe, schema);
    c.build()
}

/// Find the schema carried by the tree's first `Scan` leaf. Every `Scan`
/// in a converted tree carries the same Binder schema, so the first one
/// found is representative.
fn root_scan_schema(qe: &QueryExpr) -> Option<&crate::intent_algebra::Schema> {
    match qe {
        QueryExpr::Scan { schema, .. } => Some(schema),
        QueryExpr::Filter { child, .. }
        | QueryExpr::Window { child, .. }
        | QueryExpr::Aggregate { child, .. }
        | QueryExpr::Partition { child, .. }
        | QueryExpr::Distinct { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::Subquery { child, .. } => root_scan_schema(child),
        QueryExpr::Merge { children } => children.iter().find_map(root_scan_schema),
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => root_scan_schema(left).or_else(|| root_scan_schema(right)),
        QueryExpr::LetBinding { expr, child, .. } => {
            root_scan_schema(expr).or_else(|| root_scan_schema(child))
        }
        QueryExpr::Ref { .. } => None,
    }
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
}

impl QeCollector {
    /// `schema` is the Binder-built `Scan.schema` — the complete column
    /// universe `Aggregate.by` positional ids index into. Threaded
    /// unchanged through the walk; only the `Aggregate` arm reads it.
    fn visit(&mut self, expr: &QueryExpr, schema: Option<&crate::intent_algebra::Schema>) {
        match expr {
            QueryExpr::Scan {
                source,
                label_filters,
                ..
            } => {
                if self.metric_name.is_none() {
                    self.metric_name = Some(match source {
                        Source::TimeSeries { metric } => metric.clone(),
                        Source::Table { table_ref } => table_ref.clone(),
                    });
                }
                // Canonical `Scan` carries equality label filters inline.
                for lf in label_filters {
                    self.label_filters
                        .entry(lf.label.clone())
                        .or_insert_with(|| lf.equals.clone());
                }
            }
            QueryExpr::Filter { pred, child } => {
                // Extract equality label filters from the predicate tree.
                collect_filters_from_scalar(pred, &mut self.label_filters);
                self.visit(child, schema);
            }
            QueryExpr::Window { size, child, .. } => {
                if self.time_window.is_none() {
                    self.time_window = Some(*size);
                }
                self.visit(child, schema);
            }
            QueryExpr::Partition { keys, child } => {
                for k in keys.keys() {
                    if !self.group_by_labels.contains(k) {
                        self.group_by_labels.push(k.clone());
                    }
                }
                self.visit(child, schema);
            }
            QueryExpr::Aggregate { by, aggs, child, .. } => {
                // The canonical IR folds legacy SketchAgg / WindowedAgg-inner
                // / TopK / Aggregate into one variant carrying `AggIntent`s.
                // `by` is positional — recover the group-by label *names*
                // from the Binder schema (the legacy `Aggregate.keys` were
                // names; multi-agg aggregates reach here with a non-empty
                // `by` after the Binder resolves them).
                if let Some(s) = schema {
                    for &id in by {
                        if let Some(col) = s.columns.get(id) {
                            if !self.group_by_labels.contains(&col.name) {
                                self.group_by_labels.push(col.name.clone());
                            }
                        }
                    }
                }
                for intent in aggs {
                    if let AggIntent::TopK { k, .. } = intent {
                        self.topk = Some(*k as u64);
                    } else {
                        self.collect_op(intent);
                    }
                }
                self.visit(child, schema);
            }
            QueryExpr::Distinct { child, .. } => self.visit(child, schema),
            QueryExpr::Merge { children } => {
                for c in children {
                    self.visit(c, schema);
                }
            }
            QueryExpr::Project { child, .. }
            | QueryExpr::Sort { child, .. }
            | QueryExpr::Limit { child, .. }
            | QueryExpr::Subquery { child, .. } => self.visit(child, schema),
            QueryExpr::Join { left, right, .. }
            | QueryExpr::SetOp { left, right, .. }
            | QueryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            } => {
                self.visit(left, schema);
                self.visit(right, schema);
            }
            QueryExpr::LetBinding { expr, child, .. } => {
                self.visit(expr, schema);
                self.visit(child, schema);
            }
            QueryExpr::Ref { .. } => {}
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

fn collect_filters_from_scalar(pred: &Predicate, out: &mut HashMap<String, String>) {
    match pred {
        Predicate::BinaryOp {
            op: BinaryOpKind::Eq,
            lhs,
            rhs,
        } => {
            if let (
                Predicate::Column(ColumnRef::Named(col)),
                Predicate::Literal(LiteralValue::Str(v)),
            ) = (lhs.as_ref(), rhs.as_ref())
            {
                out.insert(col.clone(), v.clone());
            }
        }
        Predicate::BinaryOp {
            op: BinaryOpKind::And,
            lhs,
            rhs,
        } => {
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

    // ── Step γ7: canonical-IR entry point ────────────────────────────────────

    #[test]
    fn canonical_promql_quantile_yields_window_over_aggregate() {
        use crate::intent_algebra::query_expr::QueryExpr as CQueryExpr;
        // `quantile_over_time` lowers to a legacy `WindowedAgg`, which
        // `convert_root` maps to canonical `Window { child: Aggregate }`.
        let expr = parse_query_expr_canonical(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])"
        ).unwrap();
        match expr {
            CQueryExpr::Window { child, .. } => {
                assert!(matches!(*child, CQueryExpr::Aggregate { .. }));
            }
            other => panic!("expected canonical Window, got {other:?}"),
        }
    }

    #[test]
    fn canonical_promql_avg_over_time_yields_window_over_aggregate() {
        use crate::intent_algebra::query_expr::QueryExpr as CQueryExpr;
        // A bare `avg_over_time(m[w])` (no `by`) lowers to a legacy
        // `WindowedAgg` over the implicit sample-value column, which
        // `convert_root` maps to canonical `Window { child: Aggregate }`.
        let expr = parse_query_expr_canonical(
            "avg_over_time(cpu_seconds_total[10m])"
        ).unwrap();
        match expr {
            CQueryExpr::Window { child, .. } => match *child {
                CQueryExpr::Aggregate { child, .. } => {
                    assert!(matches!(*child, CQueryExpr::Scan { .. }));
                }
                other => panic!("expected canonical Aggregate, got {other:?}"),
            },
            other => panic!("expected canonical Window, got {other:?}"),
        }
    }

    #[test]
    fn legacy_entry_point_returns_raw_layer2() {
        use crate::intent_algebra::legacy_expr::{AggFunc, QueryExpr as LQueryExpr};
        // `parse_query_expr` is the crate-internal language-dispatch front
        // door: it returns the raw legacy Layer-2 relational tree with no
        // sketch lowering applied — the L2→L3 fusion now lives inside
        // `convert_root`.
        let layer2 = parse_query_expr(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])"
        ).unwrap();
        // Raw Layer 2: an `Aggregate { AggFunc::Quantile }` sitting
        // *directly* over a `Window` — un-fused, un-lowered.
        match layer2 {
            LQueryExpr::Aggregate { aggs, input, .. } => {
                assert!(matches!(
                    aggs.as_slice(),
                    [item] if matches!(item.func, AggFunc::Quantile(_))
                ));
                assert!(matches!(*input, LQueryExpr::Window { .. }));
            }
            other => panic!("expected raw Layer-2 Aggregate, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod doc_verify_all {
    // These tests pin the design.md §6 worked examples against the
    // canonical IR that `parse_query_expr_canonical` produces — the only
    // algebra IR the parse path now emits. The legacy `WindowedAgg` /
    // `SketchAgg` fusion the doc text once showed is folded by
    // `convert_root` into the canonical `Window { Aggregate }` /
    // `Aggregate { by: [], .. }` stacked forms.
    use super::parse_query_expr_canonical;
    use crate::intent_algebra::query_expr::QueryExpr;
    use crate::intent_algebra::AggIntent;

    #[test]
    fn example4_promql_quantile() {
        let expr = parse_query_expr_canonical(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])"
        ).unwrap();
        // Canonical fold of the legacy `WindowedAgg { Quantile }`:
        // `Window { Aggregate { by: [], [Quantile] } }`.
        match &expr {
            QueryExpr::Window { child, .. } => match child.as_ref() {
                QueryExpr::Aggregate { by, aggs, .. } => {
                    assert!(by.is_empty());
                    assert!(matches!(aggs.as_slice(), [AggIntent::Quantile { .. }]));
                }
                other => panic!("expected Aggregate under Window, got {other:?}"),
            },
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn example5_promql_topk() {
        let expr = parse_query_expr_canonical(
            "topk by (service) (10, count_over_time(requests{env=\"prod\"}[1m]))"
        ).unwrap();
        // The legacy `TopK` folds to a canonical `Aggregate` carrying an
        // `AggIntent::TopK`, over the `Partition { Window { Aggregate } }`
        // the grouped windowed frequency sketch lowers to.
        match &expr {
            QueryExpr::Aggregate { aggs, child, .. } => {
                assert!(matches!(aggs.as_slice(), [AggIntent::TopK { k: 10, .. }]));
                assert!(matches!(child.as_ref(), QueryExpr::Partition { .. }));
            }
            other => panic!("expected Aggregate with TopK intent, got {other:?}"),
        }
    }

    #[test]
    fn example6_sql_avg() {
        let expr = parse_query_expr_canonical(
            "SELECT symbol, AVG(price) FROM trades GROUP BY symbol"
        ).unwrap();
        // Canonical fold of `Partition { ["symbol"], SketchAgg { Quantile } }`:
        // `Partition { ["symbol"], Aggregate { by: [], [Quantile] } }`.
        match &expr {
            QueryExpr::Partition { keys, child } => {
                assert_eq!(keys.keys(), &["symbol".to_string()]);
                match child.as_ref() {
                    QueryExpr::Aggregate { by, aggs, .. } => {
                        assert!(by.is_empty());
                        assert!(matches!(aggs.as_slice(), [AggIntent::Quantile { .. }]));
                    }
                    other => panic!("expected Aggregate under Partition, got {other:?}"),
                }
            }
            other => panic!("expected Partition, got {other:?}"),
        }
    }

    #[test]
    fn example7_sql_tumble() {
        let expr = parse_query_expr_canonical(
            "SELECT region, COUNT(DISTINCT user_id) AS cnt FROM sessions GROUP BY region, TUMBLE(ts, INTERVAL '5' MINUTE) ORDER BY cnt DESC LIMIT 10"
        ).unwrap();
        // Canonical fold of
        // `Limit { Sort { Partition { ["region"], WindowedAgg { Cardinality } } } }`.
        let QueryExpr::Limit { n: 10, child, .. } = &expr else {
            panic!("expected Limit, got {expr:?}")
        };
        let QueryExpr::Sort { child: sort_child, .. } = child.as_ref() else {
            panic!("expected Sort, got {child:?}")
        };
        let QueryExpr::Partition { keys, child: part_child } = sort_child.as_ref() else {
            panic!("expected Partition, got {sort_child:?}")
        };
        assert_eq!(keys.keys(), &["region".to_string()]);
        let QueryExpr::Window { child: win_child, .. } = part_child.as_ref() else {
            panic!("expected Window, got {part_child:?}")
        };
        assert!(matches!(
            win_child.as_ref(),
            QueryExpr::Aggregate { aggs, .. }
                if matches!(aggs.as_slice(), [AggIntent::Cardinality { .. }])
        ));
    }
}
