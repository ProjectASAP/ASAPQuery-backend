//! Elasticsearch's HTTP query APIs as the data plane speaks them, on
//! ASAPQuery's routes: `POST /_sql` (Elasticsearch SQL) and `POST /_search`
//! (Query DSL) request bodies, `_search` response and error bodies, and the
//! [`fallback`] client that sends a query the data plane does not answer to
//! an Elasticsearch server. An adapter: it depends on `common` only.

use common::{QueryEval, QueryLanguage, QueryRequest, QueryResult, SqlDialect};
use serde_json::{json, Map, Value};

pub mod fallback;

/// A request body that is not the query its endpoint takes: the reason.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BadRequestBody(String);

impl BadRequestBody {
    /// The 400 body: a `parsing_exception` with the reason.
    pub fn error_body(&self) -> Value {
        error_body("parsing_exception", &self.0)
    }
}

/// The 400 body refusing a `GET`, ASAPQuery's reason: Elasticsearch
/// queries are posted as JSON.
pub fn get_refused_body() -> Value {
    let reason = "GET requests not supported for Elasticsearch queries. Use POST with Query DSL.";
    error_body("illegal_argument_exception", reason)
}

/// The `query` string of a `_sql` body (`{"query": "SELECT …", …}`; other
/// fields such as `time_zone` are not kept), evaluated at `now_ms`:
/// Elasticsearch queries name no evaluation time.
pub fn parse_sql(body: &[u8], now_ms: i64) -> Result<QueryRequest, BadRequestBody> {
    let body: Value = serde_json::from_slice(body)
        .map_err(|error| BadRequestBody(format!("Invalid JSON: {error}")))?;
    let query = body
        .get("query")
        .and_then(Value::as_str)
        .ok_or_else(|| BadRequestBody("missing parameter \"query\"".into()))?;
    Ok(QueryRequest {
        language: QueryLanguage::SQL(SqlDialect::ElasticSQL),
        query: query.to_string(),
        eval: QueryEval::Instant { time_ms: now_ms },
    })
}

/// A `_search` body, a Query DSL JSON object, kept as sent (the fallback
/// sends these bytes), evaluated at `now_ms`.
pub fn parse_search(body: &[u8], now_ms: i64) -> Result<QueryRequest, BadRequestBody> {
    let query = std::str::from_utf8(body)
        .map_err(|error| BadRequestBody(format!("Invalid JSON: {error}")))?;
    serde_json::from_str::<Map<String, Value>>(query)
        .map_err(|error| BadRequestBody(format!("Invalid JSON: {error}")))?;
    Ok(QueryRequest {
        language: QueryLanguage::ElasticDSL,
        query: query.to_string(),
        eval: QueryEval::Instant { time_ms: now_ms },
    })
}

/// Elasticsearch's error body for a 400: `{"error": {"type", "reason"},
/// "status": 400}`. Sent with HTTP 400, as Elasticsearch does (ASAPQuery
/// sent it with 200).
fn error_body(error_type: &str, reason: &str) -> Value {
    json!({"error": {"type": error_type, "reason": reason}, "status": 400})
}

/// A result a `_search` response cannot hold (anything but an instant
/// vector).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("an Elasticsearch response cannot hold a {0} result")]
pub struct UnsupportedResult(&'static str);

/// An instant vector as a `_search` response, as ASAPQuery writes it: one
/// hit per series, whose `_source` holds its labels and its `value`.
pub fn search_response(result: &QueryResult) -> Result<Value, UnsupportedResult> {
    let vector = match result {
        QueryResult::Vector(vector) => vector,
        QueryResult::Matrix(_) => return Err(UnsupportedResult("range")),
        QueryResult::Scalar(_) => return Err(UnsupportedResult("scalar")),
        QueryResult::Table(_) => return Err(UnsupportedResult("table")),
    };
    let hits: Vec<Value> = vector
        .series
        .iter()
        .map(|series| {
            let mut source: Map<String, Value> = series
                .labels
                .iter()
                .map(|(name, value)| (name.clone(), json!(value)))
                .collect();
            source.insert("value".into(), json!(series.value));
            json!({ "_source": source })
        })
        .collect();
    Ok(json!({
        "took": 0,
        "timed_out": false,
        "hits": {"total": {"value": hits.len(), "relation": "eq"}, "hits": hits},
    }))
}

#[cfg(test)]
mod tests {
    use common::{InstantVector, InstantVectorElement, LabelSet, RangeVector};

    use super::*;

    /// From ASAPQuery's `test_parse_sql_query` and
    /// `test_parse_sql_query_with_params`: only the `query` string is kept.
    #[test]
    fn parses_a_sql_body() {
        let body = json!({
            "query": "SELECT status, COUNT(*) FROM logs GROUP BY status",
            "time_zone": "UTC",
        });
        assert_eq!(
            parse_sql(body.to_string().as_bytes(), 7),
            Ok(QueryRequest {
                language: QueryLanguage::SQL(SqlDialect::ElasticSQL),
                query: "SELECT status, COUNT(*) FROM logs GROUP BY status".into(),
                eval: QueryEval::Instant { time_ms: 7 },
            })
        );
        assert_eq!(
            parse_sql(br#"{"fetch_size": 10}"#, 7),
            Err(BadRequestBody("missing parameter \"query\"".into()))
        );
    }

    /// From ASAPQuery's `test_parse_json_post_request`,
    /// `test_parse_query_with_timestamp` and `test_invalid_json`: a Query
    /// DSL body is kept byte for byte; a body that is not a JSON object is
    /// refused.
    #[test]
    fn parses_a_search_body() {
        let body = r#"{"query": {"bool": {"filter": [{"range": {"@timestamp": {"gte": 1640000000.0, "lte": 1640086400.0}}}]}}}"#;
        let request = parse_search(body.as_bytes(), 7).unwrap();
        assert_eq!(request.language, QueryLanguage::ElasticDSL);
        assert_eq!(request.query, body);
        assert_eq!(request.eval, QueryEval::Instant { time_ms: 7 });
        for bad in [&b"not valid json"[..], b"[1]", b"\xff"] {
            let error = parse_search(bad, 7).unwrap_err();
            assert!(error.to_string().starts_with("Invalid JSON: "), "{error}");
        }
    }

    /// ASAPQuery's `test_format_error_response`: a body that does not
    /// parse is a `parsing_exception`; a `GET` an
    /// `illegal_argument_exception`.
    #[test]
    fn writes_error_bodies() {
        assert_eq!(
            BadRequestBody("Invalid query syntax".into()).error_body(),
            json!({"error": {"type": "parsing_exception", "reason": "Invalid query syntax"}, "status": 400})
        );
        assert_eq!(
            get_refused_body()["error"]["type"],
            "illegal_argument_exception"
        );
    }

    /// From ASAPQuery's `test_format_success_response`: an empty vector has
    /// no hits; each series is a hit of its labels and value. Other
    /// results have no `_search` form (ASAPQuery answered 501).
    #[test]
    fn writes_an_instant_vector_as_hits() {
        let empty = QueryResult::Vector(InstantVector {
            timestamp_ms: 0,
            series: vec![],
        });
        assert_eq!(
            search_response(&empty).unwrap(),
            json!({"took": 0, "timed_out": false, "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}})
        );
        let one = QueryResult::Vector(InstantVector {
            timestamp_ms: 0,
            series: vec![InstantVectorElement {
                labels: LabelSet::from([("status".to_string(), "500".to_string())]),
                value: 3.0,
            }],
        });
        assert_eq!(
            search_response(&one).unwrap()["hits"]["hits"],
            json!([{"_source": {"status": "500", "value": 3.0}}])
        );
        let range = QueryResult::Matrix(RangeVector { series: vec![] });
        assert_eq!(
            search_response(&range).unwrap_err().to_string(),
            "an Elasticsearch response cannot hold a range result"
        );
    }
}
