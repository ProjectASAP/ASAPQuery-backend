//! SP-1 query workload extraction — PromQL parser.
//!
//! L1 adoption (`control_plane/docs/design-target-architecture.md` Part
//! B): the PromQL parsing + L1→L2→L3 lowering previously done by this
//! crate's own `promql.rs` (retired) is now `asap_frontend_promql::lower_promql`
//! directly — no local parser, no local L2 relational tree. This crate's
//! own `intent_algebra::lower.rs` two heuristics (multi-agg fusion, the
//! windowed-Count-as-Frequency trigger) do **not** run anymore; per
//! explicit direction, this adopts whatever `AggIntent` classification
//! ASAPController's `asap_l2::lower` produces as-is (e.g. a classic
//! `by (le)` `histogram_quantile(...)` now correctly classifies as the
//! exact `AggIntent::HistogramQuantile`, not the sketchable `Quantile`
//! this deployment previously forced; grouped/windowed `count_over_time`
//! becomes plain exact `Count`, not the `Frequency` extension) rather
//! than reconciling it back to the old local behavior.
//!
//! # Entry points
//!
//! | Function | Returns | Use |
//! |---|---|---|
//! | [`parse_query_expr_canonical`] | canonical `query_expr::QueryExpr` | Full algebra IR |
//! | [`parse_query`] | `ParsedQuery` | Backward compat with existing analyzer |
//!
//! Both now take an explicit [`AccuracyTarget`] — `lower_promql` requires
//! one (accuracy-driven parameter sizing happens as early as L1/L2 for
//! some intents), where the old local pipeline took none.

use std::collections::HashMap;
use std::time::Duration;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::{Predicate, QueryExpr, Source};
use crate::intent_algebra::{ColumnId, CompareOp, L3Expr, L3Scalar};
use crate::types::AggType;
use crate::types_v2::AccuracyTarget;

// ── Output types (legacy — consumed by analyzer and planner) ──────────────────

/// Flat intermediate representation consumed by [`crate::analyzer::Analyzer`].
///
/// Produced by [`parse_query`] via [`QueryExpr`] tree walking.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Metric name (from the PromQL selector).
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

/// Parse a PromQL query string into the **canonical** L3
/// [`query_expr::QueryExpr`](crate::intent_algebra::query_expr::QueryExpr) IR.
///
/// This is the single public algebra-IR entry point — a direct call into
/// `asap_frontend_promql::lower_promql`, which does the full L1 parse →
/// L2 relational tree → L3 canonical conversion in one call. No local
/// parser, no local L2 tree; `accuracy` is threaded onto every
/// accuracy-bearing intent the same way ASAPController's own PromQL
/// front end threads it.
pub fn parse_query_expr_canonical(
    query: &str,
    accuracy: AccuracyTarget,
) -> anyhow::Result<crate::intent_algebra::query_expr::QueryExpr> {
    let canonical = asap_frontend_promql::lower_promql(query.trim(), accuracy)?;
    Ok(canonical)
}

/// Parse a PromQL query string into a [`ParsedQuery`].
///
/// This is the backward-compatible entry point for the existing
/// [`crate::analyzer::Analyzer`].  Internally it parses via
/// [`parse_query_expr_canonical`] and extracts the flat summary by walking
/// the canonical [`QueryExpr`] tree.
pub fn parse_query(query: &str, accuracy: AccuracyTarget) -> anyhow::Result<ParsedQuery> {
    let qe = parse_query_expr_canonical(query, accuracy)?;
    Ok(qe_to_parsed_query(&qe))
}

/// Extract a flat [`ParsedQuery`] from an ALREADY-parsed canonical
/// [`QueryExpr`].
///
/// This is the parse-once seam (P2-1): callers that already hold a
/// canonical tree (e.g. `asap_tier_analysis::analyze_promql_for_asap_tier`,
/// which needs both the tree AND the flat summary) derive the
/// [`ParsedQuery`] from it directly instead of re-running the full
/// PromQL → legacy → canonical pipeline a second time. The output is
/// byte-for-byte identical to `parse_query(src)` for the `src` that
/// produced `qe`.
pub(crate) fn parsed_query_from_canonical(qe: &QueryExpr) -> ParsedQuery {
    qe_to_parsed_query(qe)
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
        // `TimeRange`/`TimeShift` are `asap_l2::lower`'s range-vector-selector
        // and offset/@ markers (L1 adoption, design-target-architecture.md
        // Part B) -- pass-through wrappers over the same `Scan`.
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::Aggregate { child, .. }
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
        // `Ref` has no reachable `Scan` without a `LetBinding` scope, and
        // the remaining PromQL-surface superset (Scalar/EvalTime/
        // VectorFromScalar/ScalarFromVector/Relabel/InfoJoin/Sample/
        // WindowFunc) is real, new capability `lower_promql` adds but this
        // crate's flat `ParsedQuery` extraction doesn't attempt to unpack
        // yet -- accepted gap (design-target-architecture.md Part B): these
        // shapes weren't reachable at all before this swap, so nothing
        // regresses; `ParsedQuery` may come back incomplete for them.
        _ => None,
    }
}

#[derive(Default)]
struct QeCollector {
    metric_name: Option<String>,
    agg_types: Vec<AggType>,
    group_by_labels: Vec<String>,
    label_filters: HashMap<String, String>,
    time_window: Option<Duration>,
    exact_required: bool,
    quantiles: Vec<f64>,
    topk: Option<u64>,
}

impl QeCollector {
    /// `schema` is the Binder-built `Scan.schema` — the complete column
    /// universe `Aggregate.by` positional ids index into. Threaded
    /// unchanged through the walk; only the `Aggregate` arm reads it.
    fn visit(&mut self, expr: &QueryExpr, schema: Option<&crate::intent_algebra::Schema>) {
        match expr {
            QueryExpr::Scan {
                source,
                predicates,
                schema: scan_schema,
            } => {
                if self.metric_name.is_none() {
                    self.metric_name = Some(match source {
                        Source::TimeSeries { metric } => metric.clone(),
                        Source::Table { table_ref } => table_ref.clone(),
                    });
                }
                // Canonical `Scan.predicates` carries equality label
                // filters as typed `Predicate(L3Expr::Compare{Column, Eq,
                // Literal(Utf8)})` trees (the shape
                // `query_expr::label_filter_to_predicate` builds) — resolve
                // each `Column` id back to its name via the Scan's own
                // schema to recover the flat name/value map this legacy
                // `ParsedQuery` output still wants.
                for p in predicates {
                    collect_filters_from_expr(&p.0, Some(scan_schema), &mut self.label_filters);
                }
            }
            QueryExpr::Filter { pred, child } => {
                // Extract equality label filters from the predicate tree.
                collect_filters_from_scalar(pred, schema, &mut self.label_filters);
                self.visit(child, schema);
            }
            QueryExpr::Window { size, child, .. } => {
                if self.time_window.is_none() {
                    self.time_window = Some(*size);
                }
                self.visit(child, schema);
            }
            // `TimeRange` is `asap_l2::lower`'s range-vector-selector marker
            // (`m[5m]` in `quantile_over_time(φ, m[5m])`) -- the range-window
            // duration this collector's `time_window` field wants, same as
            // `Window::size` above. `TimeShift` (`offset`/`@`) is a pure
            // pass-through, no window/label information of its own.
            QueryExpr::TimeRange { range, child } => {
                if self.time_window.is_none() {
                    self.time_window = Some(*range);
                }
                self.visit(child, schema);
            }
            QueryExpr::TimeShift { child, .. } => self.visit(child, schema),
            QueryExpr::Aggregate {
                reduction,
                aggs,
                child,
                ..
            } => {
                // The canonical IR folds legacy SketchAgg / WindowedAgg-inner
                // / TopK / Aggregate into one variant carrying `AggIntent`s.
                // `by` is positional — recover the group-by label *names*
                // from the Binder schema (the legacy `Aggregate.keys` were
                // names; multi-agg aggregates reach here with a non-empty
                // `by` after the Binder resolves them). A per-entity
                // reduction (ASAPController#163/#165) has no `by` at all —
                // same as an empty one here, no label names to recover.
                let by: &[ColumnId] = reduction.group_keys().map(|k| k.keys()).unwrap_or(&[]);
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
            // `Ref` has no reachable `Scan` without a `LetBinding` scope
            // to resolve it against, and the PromQL-surface superset
            // (Scalar/EvalTime/VectorFromScalar/ScalarFromVector/Relabel/
            // InfoJoin/Sample/TimeRange/TimeShift/WindowFunc) isn't
            // constructed by this parser today.
            QueryExpr::Ref { .. } => {}
            _ => {}
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
            op if crate::intent_algebra::as_frequency(op).is_some() => {
                if !self.agg_types.contains(&AggType::Frequency) {
                    self.agg_types.push(AggType::Frequency);
                }
            }
            AggIntent::Quantile { q, .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(q) {
                    self.quantiles.push(*q);
                }
            }
            AggIntent::Min { .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&0.0) {
                    self.quantiles.push(0.0);
                }
            }
            AggIntent::Max { .. } => {
                if !self.agg_types.contains(&AggType::Quantile) {
                    self.agg_types.push(AggType::Quantile);
                }
                if !self.quantiles.contains(&1.0) {
                    self.quantiles.push(1.0);
                }
            }
            // Sum / Count / Avg / TopK / Rate / Increase / archive-only —
            // all flip the exact_required flag (no sketch benefit at the
            // legacy planner's level).
            _ => {
                self.exact_required = true;
            }
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
            aggregations: self.agg_types,
            group_by_labels: self.group_by_labels,
            label_filters: self.label_filters,
            time_window: self.time_window.unwrap_or(Duration::from_secs(300)),
            exact_required: self.exact_required,
            quantiles: qs,
            hint,
        }
    }
}

fn collect_filters_from_scalar(
    pred: &Predicate,
    schema: Option<&crate::intent_algebra::Schema>,
    out: &mut HashMap<String, String>,
) {
    collect_filters_from_expr(&pred.0, schema, out);
}

/// Recover a flat name/value equality map from a canonical `L3Expr`
/// predicate tree — `Column(id) == Literal(Utf8(v))` conjuncts, `id`
/// resolved back to a name via `schema` (positional `Column` carries no
/// name of its own, unlike the pre-merge name-based `Predicate::Column`).
fn collect_filters_from_expr(
    expr: &L3Expr,
    schema: Option<&crate::intent_algebra::Schema>,
    out: &mut HashMap<String, String>,
) {
    match expr {
        L3Expr::Compare {
            left,
            op: CompareOp::Eq,
            right,
        } => {
            if let (L3Expr::Column(id), L3Expr::Literal(L3Scalar::Utf8(v))) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(col) = schema.and_then(|s| s.columns.get(*id)) {
                    out.entry(col.name.clone()).or_insert_with(|| v.clone());
                }
            }
        }
        L3Expr::BoolAnd(parts) => {
            for p in parts {
                collect_filters_from_expr(p, schema, out);
            }
        }
        _ => {}
    }
}

// ── DEBS hint classifier (shared by both parsers via to_parsed_query) ─────────

/// Returns the DEBS-specific hint for `financial.last_trade_price` queries.
pub(super) fn debs_hint(
    metric: &str,
    aggs: &[AggType],
    quantiles: &[f64],
    exact_required: bool,
    topk: Option<u64>,
) -> Option<QueryHint> {
    let is_debs = metric == "financial.last_trade_price" || metric == "financial_last_trade_price";
    if !is_debs {
        return None;
    }

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
        AggType::Frequency => Some(QueryHint::DebsTopK { k: 10 }),
        AggType::Quantile => {
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

    const ACC: AccuracyTarget = AccuracyTarget::Epsilon(0.01);

    // Smoke tests for the parse entry point.

    #[test]
    fn promql_dispatched_correctly() {
        // `by` belongs to the aggregate operator, not the function call.
        let pq = parse_query("sum by (host) (quantile_over_time(0.99, latency[5m]))", ACC).unwrap();
        assert!(pq.aggregations.contains(&AggType::Quantile));
        assert_eq!(pq.quantiles, vec![0.99]);
    }

    #[test]
    fn parse_query_expr_returns_expr() {
        let pq = parse_query(
            "topk by (symbol) (10, count_over_time(financial_last_trade_price[5m]))",
            ACC,
        )
        .unwrap();
        // Should parse without error and extract the metric name.
        assert_eq!(pq.metric_name, "financial_last_trade_price");
    }

    // ── canonical-IR entry point ─────────────────────────────────────────────

    #[test]
    fn canonical_promql_quantile_yields_aggregate_over_time_range() {
        use crate::intent_algebra::query_expr::QueryExpr as CQueryExpr;
        // `asap_l2::lower` models a range-vector selector (`m[5m]`) as
        // `TimeRange`, not `Window` -- `Window` is reserved for real
        // streaming/tumbling windows. `Aggregate` sits directly on top,
        // no `Window` wrapper (see this module's own doc for why this
        // differs from the retired local parser's shape).
        let expr = parse_query_expr_canonical(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])",
            ACC,
        )
        .unwrap();
        match expr {
            CQueryExpr::Aggregate { child, .. } => {
                assert!(matches!(*child, CQueryExpr::TimeRange { .. }));
            }
            other => panic!("expected canonical Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn canonical_promql_avg_over_time_yields_aggregate_over_time_range() {
        use crate::intent_algebra::query_expr::QueryExpr as CQueryExpr;
        // A bare `avg_over_time(m[w])` (no `by`) lowers to canonical
        // `Aggregate { child: TimeRange { child: Scan } }`.
        let expr = parse_query_expr_canonical("avg_over_time(cpu_seconds_total[10m])", ACC).unwrap();
        match expr {
            CQueryExpr::Aggregate { child, .. } => match *child {
                CQueryExpr::TimeRange { child, .. } => {
                    assert!(matches!(*child, CQueryExpr::Scan { .. }));
                }
                other => panic!("expected canonical TimeRange, got {other:?}"),
            },
            other => panic!("expected canonical Aggregate, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod doc_verify_all {
    // Pins the design.md §6 worked examples against the canonical IR
    // `asap_frontend_promql::lower_promql` produces — the only algebra IR
    // the parse path now emits.
    use super::parse_query_expr_canonical;
    use crate::intent_algebra::query_expr::QueryExpr;
    use crate::intent_algebra::{AggIntent, Reduction};
    use crate::types_v2::AccuracyTarget;

    const ACC: AccuracyTarget = AccuracyTarget::Epsilon(0.01);

    #[test]
    fn example4_promql_quantile() {
        let expr = parse_query_expr_canonical(
            "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])",
            ACC,
        )
        .unwrap();
        // `Aggregate { reduction: PerEntity, [Quantile], child: TimeRange }`
        // — no explicit `by()`, and ranged over a single non-per-series
        // intent, so there's no grouping concept at all (see #165's
        // `Reduction`). No `Window` wrapper -- `asap_l2::lower` models the
        // range-vector selector itself as `TimeRange`, not `Window`.
        match &expr {
            QueryExpr::Aggregate {
                reduction,
                aggs,
                child,
                ..
            } => {
                assert!(matches!(reduction, Reduction::PerEntity));
                assert!(matches!(aggs.as_slice(), [AggIntent::Quantile { .. }]));
                assert!(matches!(child.as_ref(), QueryExpr::TimeRange { .. }));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn example5_promql_topk() {
        let expr = parse_query_expr_canonical(
            "topk by (service) (10, count_over_time(requests{env=\"prod\"}[1m]))",
            ACC,
        )
        .unwrap();
        // `topk(...)` ranking by `count_over_time(...)` is the heavy-hitter
        // shape both this deployment and `asap_frontend_promql` route to a
        // canonical `Aggregate` carrying `AggIntent::TopK`, over the
        // `Window { Aggregate { by } } }` the grouped windowed count lowers
        // to — regardless of what intent that INNER aggregate now carries
        // (see this module's doc: no longer necessarily the `Frequency`
        // extension), the outer `TopK` shape itself is unaffected.
        match &expr {
            QueryExpr::Aggregate { aggs, child, .. } => {
                assert!(matches!(aggs.as_slice(), [AggIntent::TopK { k: 10, .. }]));
                // Inner reduction the TopK ranks by: `Aggregate { Count,
                // child: TimeRange { child: Scan } }` -- same `TimeRange`
                // shape as the other tests above, one level down. `Count`
                // carries its own `AccuracyTarget` field (here `Epsilon(0.01)`,
                // threaded from this call's `accuracy` argument) rather than
                // being forced exact -- adopted as-is per this module's doc.
                match child.as_ref() {
                    QueryExpr::Aggregate {
                        aggs: inner_aggs,
                        child: inner_child,
                        ..
                    } => {
                        assert!(matches!(inner_aggs.as_slice(), [AggIntent::Count { .. }]));
                        assert!(matches!(inner_child.as_ref(), QueryExpr::TimeRange { .. }));
                    }
                    other => panic!("expected inner Aggregate, got {other:?}"),
                }
            }
            other => panic!("expected Aggregate with TopK intent, got {other:?}"),
        }
    }
}
