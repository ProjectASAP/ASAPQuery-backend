//! Tests for the L1 `query_language` façade.

use super::*;
use crate::types_v2::QueryLanguage;

#[test]
fn promql_language_id_is_promql() {
    let l = PromQLLanguage;
    assert_eq!(l.id(), QueryLanguage::PromQL);
}

#[test]
fn promql_language_parses_basic() {
    let ast = PromQLLanguage
        .parse("up")
        .expect("PromQL parse should succeed");
    assert!(ast.is_promql(), "expected PromQL variant");
    let promql = ast.as_promql().unwrap();
    assert_eq!(promql.summary().metric_name, "up");
    assert_eq!(promql.source, "up");
}

#[test]
fn promql_language_parses_quantile_over_time() {
    let ast = PromQLLanguage
        .parse("sum by (host) (quantile_over_time(0.99, latency[5m]))")
        .expect("PromQL quantile parse should succeed");
    let promql = ast.as_promql().unwrap();
    assert!(promql.summary().quantiles.contains(&0.99));
    assert_eq!(promql.summary().metric_name, "latency");
}

#[test]
fn promql_language_surfaces_parse_error_as_backend_error() {
    let err = PromQLLanguage.parse("@!not promql@!").unwrap_err();
    match err {
        ParseError::Backend { language, .. } => {
            assert_eq!(language, QueryLanguage::PromQL);
        }
        other => panic!("expected ParseError::Backend, got {other:?}"),
    }
}

#[test]
fn sql_language_returns_unimplemented() {
    let err = SqlLanguage.parse("SELECT 1").unwrap_err();
    assert!(matches!(err, ParseError::Unimplemented(_)));
    assert_eq!(SqlLanguage.id(), QueryLanguage::Sql);
}

#[test]
fn elastic_dsl_language_returns_unimplemented() {
    let err = ElasticDslLanguage.parse("{}").unwrap_err();
    assert!(matches!(err, ParseError::Unimplemented(_)));
    assert_eq!(ElasticDslLanguage.id(), QueryLanguage::ElasticDsl);
}

#[test]
fn language_trait_object_is_object_safe() {
    // Compile-time check: the trait can be used as `dyn Language` so
    // future code can store a `Vec<Box<dyn Language>>` registry.
    let l: Box<dyn Language> = Box::new(PromQLLanguage);
    assert_eq!(l.id(), QueryLanguage::PromQL);
}
