//! Lowering from `query_parser::ParsedQuery` → L3 [`QueryExpr`].
//!
//! Phase B exposes this as a standalone function so callers can opt into
//! the L3 IR without touching the existing `Analyzer::analyze` →
//! `QueryWorkload` pipeline. Wiring the analyzer to *also* emit a
//! `QueryExpr` is the follow-up phase's job.
//!
//! Per-language scope. The `parse_query` entry point in
//! `query_parser::mod` already dispatches PromQL vs SQL; by the time we
//! see a `ParsedQuery` the language distinction has been collapsed onto
//! the flat summary. For PromQL (the only language this crate consumes
//! per the orchestrator spec), the lowering is:
//!
//! ```text
//! Scan{TimeSeries} → [Window?] → Aggregate{ by, [intent_per_aggregation] }
//! ```
//!
//! The window is emitted only when `ParsedQuery.time_window` is non-zero
//! and the query has at least one aggregation (a bare metric selector
//! lowers to a `Scan` alone). Workload-level CSE that produces fan-in
//! (multi-root with `LetBinding` / `Ref`) is a follow-up; this lowers
//! each query independently.

#![allow(dead_code)]

use std::collections::HashMap;
use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::query_expr::{
    LabelFilter, QueryExpr, QueryExprError, Source, WindowKind,
};
use crate::intent_algebra::schema::{Column, ColumnId, DataType, Schema};
use crate::query_parser::ParsedQuery;
use crate::types::AggType;
use crate::types_v2::AccuracyTarget;

/// Errors returned by [`lower_parsed_query`].
#[derive(Debug, Error)]
pub enum LoweringError {
    /// `ParsedQuery.metric_name` was empty — nothing to scan.
    #[error("empty metric name in parsed query")]
    EmptyMetricName,
    /// `Aggregate` schema-derivation invariant failed inside the lowered
    /// tree. Indicates the lowering logic produced an inconsistent shape;
    /// surfaced rather than panicked.
    #[error("post-lower schema check failed: {0}")]
    PostLowerSchema(#[from] QueryExprError),
}

/// Lower a `ParsedQuery` into the L3 `QueryExpr` DAG. Single-rooted
/// (one query in, one root out). Workload-level CSE is a follow-up.
///
/// The accuracy target is supplied separately because `ParsedQuery`
/// carries the legacy `accuracy_sla: f64` only on the `QueryWorkload`
/// downstream of analyzer; passing it explicitly keeps this function
/// orthogonal to the analyzer wiring.
pub fn lower_parsed_query(
    parsed: &ParsedQuery,
    accuracy: AccuracyTarget,
) -> Result<QueryExpr, LoweringError> {
    if parsed.metric_name.trim().is_empty() {
        return Err(LoweringError::EmptyMetricName);
    }

    // ── Scan ─────────────────────────────────────────────────────────────
    let scan_schema = scan_schema_for(parsed);
    let label_filters = parsed
        .label_filters
        .iter()
        .map(|(k, v)| LabelFilter {
            label: k.clone(),
            equals: v.clone(),
        })
        .collect::<Vec<_>>();
    let mut node = QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: parsed.metric_name.clone(),
        },
        label_filters,
        schema: scan_schema.clone(),
    };

    // Bare metric selector (no aggregations, no window) — return the Scan
    // directly. PromQL `up{job="api"}` lowers here.
    if parsed.aggregations.is_empty() && parsed.time_window.is_zero() {
        return Ok(node);
    }

    // ── Window (optional) ───────────────────────────────────────────────
    if !parsed.time_window.is_zero() {
        node = QueryExpr::Window {
            // PromQL `[5m]` is canonically Sliding (per design.md §6
            // line ~308: "PromQL `[5m]` and streaming windows lower
            // here"). SQL `TUMBLE` would lower to `Tumbling` — out of
            // scope for the PromQL-only Phase B.
            kind: WindowKind::Sliding,
            size: parsed.time_window,
            slide: None,
            child: Box::new(node),
        };
    }

    // ── Aggregate (optional, when intents present) ─────────────────────
    if !parsed.aggregations.is_empty() {
        let aggs = build_intents(parsed, accuracy);
        let by = group_by_column_ids(&scan_schema, &parsed.group_by_labels);
        node = QueryExpr::Aggregate {
            by,
            aggs,
            having: None,
            child: Box::new(node),
        };
    }

    // Validate the lowered tree has a derivable output schema. Catches
    // `Window` without time_index, out-of-range `by` ids, etc. The
    // discard is intentional — we only want the validation side-effect.
    let _ = node.output_schema()?;

    Ok(node)
}

/// Synthesise the output schema of the `Scan` for this `ParsedQuery`.
///
/// PromQL convention: a leaf produces `(ts, value, *labels)` with `ts` at
/// position 0 (the time index) and `value` at position 1. Group-by
/// labels and equality-filter labels are projected into positions 2..N
/// in alphabetical order (deterministic). The unique-key set is
/// `[ts, *labels]` — one sample per (timestamp, label-tuple) pair.
fn scan_schema_for(parsed: &ParsedQuery) -> Schema {
    let mut columns = vec![
        Column {
            name: "ts".into(),
            dtype: DataType::Timestamp,
            nullable: false,
        },
        Column {
            name: "value".into(),
            dtype: DataType::Float64,
            nullable: false,
        },
    ];
    // Collect label names from group_by + filters, dedup + sort for
    // determinism.
    let mut labels: Vec<String> = parsed.group_by_labels.to_vec();
    for k in parsed.label_filters.keys() {
        if !labels.contains(k) {
            labels.push(k.clone());
        }
    }
    labels.sort();
    for label in &labels {
        columns.push(Column {
            name: label.clone(),
            dtype: DataType::Utf8,
            nullable: false,
        });
    }
    // unique_keys = [ts, *labels] — one sample per (ts, label-tuple).
    let mut uk: Vec<ColumnId> = vec![0]; // ts
    for i in 2..columns.len() {
        uk.push(i);
    }
    Schema::with_time_index(columns, 0, vec![uk])
}

/// Resolve `group_by_labels` (names) to column ids in the scan schema.
/// Labels missing from the schema are silently dropped — they wouldn't
/// have been preserved by the scan anyway.
fn group_by_column_ids(schema: &Schema, group_by_labels: &[String]) -> Vec<ColumnId> {
    group_by_labels
        .iter()
        .filter_map(|name| schema.column_id(name))
        .collect()
}

/// Translate the legacy `(AggType, Vec<f64>, exact_required)` triple
/// from `ParsedQuery` into typed `AggIntent` entries.
///
/// Each `AggType::Quantile` fans out to one `AggIntent::Quantile{q}` per
/// `parsed.quantiles` entry — `quantile_over_time(0.95)` and
/// `quantile_over_time(0.99)` co-occurring on the same metric produce
/// two intents on the same `Aggregate`.
fn build_intents(parsed: &ParsedQuery, accuracy: AccuracyTarget) -> Vec<AggIntent> {
    let effective_accuracy = if parsed.exact_required {
        AccuracyTarget::Exact
    } else {
        accuracy.clone()
    };

    // Track which quantiles we've already emitted to avoid duplicates
    // when multiple legacy `AggType` rows imply the same φ (e.g. Min →
    // q=0.0, Max → q=1.0 historically lived under `AggType::Quantile`).
    let mut emitted_q = HashMap::<u64, bool>::new();
    let mut out = Vec::new();
    for agg in &parsed.aggregations {
        match agg {
            AggType::Quantile => {
                if parsed.quantiles.is_empty() {
                    out.push(AggIntent::Quantile {
                        q: 0.5,
                        accuracy: effective_accuracy.clone(),
                    });
                } else {
                    for &q in &parsed.quantiles {
                        let key = (q * 1e9) as u64;
                        if emitted_q.insert(key, true).is_none() {
                            out.push(AggIntent::Quantile {
                                q,
                                accuracy: effective_accuracy.clone(),
                            });
                        }
                    }
                }
            }
            AggType::Cardinality => out.push(AggIntent::Cardinality {
                accuracy: effective_accuracy.clone(),
            }),
            AggType::Frequency => out.push(AggIntent::Frequency {
                accuracy: effective_accuracy.clone(),
            }),
        }
    }
    if out.is_empty() && parsed.exact_required {
        // PromQL `sum`, `rate`, … — no sketch intent fires; the lowering
        // surfaces this as `AggIntent::Sum` so the L3 schema is
        // well-formed (an `Aggregate` with empty aggs is meaningless).
        out.push(AggIntent::Sum);
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::query_expr::QueryExpr;
    use crate::query_parser::parse_query;

    #[test]
    fn lower_promql_basic() {
        let parsed = parse_query(
            "quantile_over_time(0.99, http_request_duration_seconds{service=\"api\"}[5m])",
        )
        .expect("parse");
        let expr = lower_parsed_query(&parsed, AccuracyTarget::Epsilon(0.01)).expect("lower");

        // Root: Aggregate { aggs: [Quantile{0.99, ε=0.01}] }
        match &expr {
            QueryExpr::Aggregate { aggs, child, .. } => {
                assert_eq!(aggs.len(), 1);
                match &aggs[0] {
                    AggIntent::Quantile { q, accuracy } => {
                        assert!((*q - 0.99).abs() < 1e-9);
                        assert_eq!(*accuracy, AccuracyTarget::Epsilon(0.01));
                    }
                    other => panic!("expected Quantile intent, got {other:?}"),
                }
                // Mid: Window
                match child.as_ref() {
                    QueryExpr::Window { kind, size, child, .. } => {
                        assert_eq!(*kind, WindowKind::Sliding);
                        assert_eq!(*size, std::time::Duration::from_secs(300));
                        // Leaf: Scan
                        match child.as_ref() {
                            QueryExpr::Scan {
                                source,
                                label_filters,
                                schema,
                            } => {
                                match source {
                                    Source::TimeSeries { metric } => {
                                        assert_eq!(metric, "http_request_duration_seconds")
                                    }
                                    _ => panic!("expected TimeSeries source"),
                                }
                                assert_eq!(label_filters.len(), 1);
                                assert_eq!(label_filters[0].label, "service");
                                assert_eq!(label_filters[0].equals, "api");
                                // Schema: ts (time_index), value, service
                                assert_eq!(schema.time_index, Some(0));
                                assert_eq!(schema.columns[0].name, "ts");
                                assert_eq!(schema.columns[1].name, "value");
                                assert!(schema.has_unique_key());
                            }
                            other => panic!("expected Scan, got {other:?}"),
                        }
                    }
                    other => panic!("expected Window, got {other:?}"),
                }
            }
            other => panic!("expected Aggregate at root, got {other:?}"),
        }

        // The lowered tree's output schema is derivable end-to-end.
        let schema = expr.output_schema().expect("output_schema");
        assert!(
            schema.columns.iter().any(|c| c.name == "quantile_0_99"),
            "output schema should contain quantile_0_99 column, got {:?}",
            schema.columns
        );
    }

    #[test]
    fn lower_promql_with_group_by() {
        let parsed = parse_query(
            "sum by (host) (quantile_over_time(0.95, latency{env=\"prod\"}[1m]))",
        )
        .expect("parse");
        let expr = lower_parsed_query(&parsed, AccuracyTarget::Epsilon(0.05)).expect("lower");
        let schema = expr.output_schema().expect("output_schema");
        // `host` is a group-by → it lands in unique_keys at position 0
        // in the output (positional projection of `by`).
        assert!(schema.has_unique_key());
        assert!(schema.columns.iter().any(|c| c.name == "host"));
    }

    #[test]
    fn lower_promql_cardinality() {
        let parsed = parse_query(
            "count by (user_id) (count_over_time(active_users{env=\"prod\"}[5m]))",
        )
        .expect("parse");
        let expr = lower_parsed_query(&parsed, AccuracyTarget::Epsilon(0.01)).expect("lower");
        let schema = expr.output_schema().expect("output_schema");
        // Cardinality intent → output column named `cardinality` of dtype Int64.
        let card = schema
            .columns
            .iter()
            .find(|c| c.name == "cardinality")
            .expect("cardinality column should be present");
        assert!(matches!(card.dtype, DataType::Int64));
    }

    #[test]
    fn lower_empty_metric_errors() {
        let parsed = ParsedQuery {
            metric_name: String::new(),
            aggregations: vec![],
            group_by_labels: vec![],
            label_filters: HashMap::new(),
            time_window: std::time::Duration::ZERO,
            exact_required: false,
            quantiles: vec![],
            hint: None,
        };
        let err = lower_parsed_query(&parsed, AccuracyTarget::Exact).unwrap_err();
        assert!(matches!(err, LoweringError::EmptyMetricName));
    }
}
