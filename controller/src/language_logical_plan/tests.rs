//! Tests for L2 lowering (`language_logical_plan`).

use super::*;
use crate::algebra::expr::{AggIntent, QueryExpr};
use crate::query_language::{Language, LanguageAst, PromQLLanguage};
use crate::types::AggType;
use crate::types_v2::QueryLanguage;

fn parse_promql(src: &str) -> LanguageAst {
    PromQLLanguage.parse(src).expect("PromQL parse should succeed")
}

#[test]
fn lower_promql_to_logical_plan_basic() {
    let ast = parse_promql("up");
    let plan = lower_to_logical_plan(&ast).expect("lowering should succeed");
    assert_eq!(plan.language(), QueryLanguage::PromQL);
    assert_eq!(plan.source(), "up");
    let s = plan.summary();
    assert_eq!(s.metric_name, "up");
}

#[test]
fn lower_promql_quantile_preserves_summary() {
    let ast = parse_promql(
        "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])",
    );
    let plan = lower_to_logical_plan(&ast).unwrap();
    let s = plan.summary();
    assert_eq!(s.metric_name, "http_request_duration");
    assert!(s.aggregations.contains(&AggType::Quantile));
    assert_eq!(s.quantiles, vec![0.99]);
    assert_eq!(s.label_filters.get("env").map(|v| v.as_str()), Some("prod"));
    assert_eq!(s.time_window, std::time::Duration::from_secs(5 * 60));
}

#[test]
fn lower_promql_keeps_algebra_tree_for_l3() {
    // The PromQL L2 tree IS the existing `QueryExpr`; assert that the
    // tree shape matches what `parse_query_expr` would have produced.
    let ast = parse_promql(
        "quantile_over_time(0.99, http_request_duration{env=\"prod\"}[5m])",
    );
    let plan = lower_to_logical_plan(&ast).unwrap();
    let tree = plan.as_promql_tree().expect("PromQL plan");
    assert!(matches!(
        tree,
        QueryExpr::WindowedAgg { agg: AggIntent::Quantile { .. }, .. }
    ));
}

#[test]
fn lower_promql_topk_extracts_groupby() {
    let ast = parse_promql(
        "topk by (service) (10, count_over_time(requests{env=\"prod\"}[1m]))",
    );
    let plan = lower_to_logical_plan(&ast).unwrap();
    let s = plan.summary();
    assert_eq!(s.metric_name, "requests");
    assert!(s.group_by_labels.contains(&"service".to_string()));
}

#[test]
fn lower_unsupported_language_errors_cleanly() {
    // Build a stub LanguageAst variant by hand — we cannot construct
    // SQL/DataFusion/ElasticDsl variants because LanguageAst doesn't
    // expose those yet (PromQL is the only variant). So the equivalent
    // smoke test is: stub backends fail at L1 with `Unimplemented`, and
    // the type system rules out passing them to L2 lowering. We assert
    // that contract here.
    let err = crate::query_language::SqlLanguage.parse("SELECT 1").unwrap_err();
    assert!(matches!(
        err,
        crate::query_language::ParseError::Unimplemented(_)
    ));
}

#[test]
fn lowering_error_for_unsupported_language_is_displayable() {
    // Direct construction of the error variant — verifies the Display
    // impl renders something useful. Future variants (Sql/etc.) will
    // exercise this path organically once their L1 backends ship.
    let err = LoweringError::UnsupportedLanguage(QueryLanguage::Sql);
    let msg = format!("{err}");
    assert!(msg.contains("Sql"), "got: {msg}");
}
