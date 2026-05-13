use super::config::AdapterConfig;
use super::traits::*;
use crate::query_engines::QueryResult;
use crate::utils::http::{convert_query_result_to_prometheus, convert_range_result_to_prometheus};
use async_trait::async_trait;
use axum::{
    extract::{Form, Query},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use promql_utilities::data_model::KeyByLabelNames;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};

/// Prometheus-compatible response structure, with two ASAP
/// extensions:
///
/// * `infos` — Prometheus 3.0-style informational annotations
///   (Grafana 11+ renders these inline). Mirrors a one-liner
///   summary of `accuracy` so older / non-JSON-aware UIs still
///   see the ε/δ bound.
/// * `accuracy` — structured [`AccuracyEnvelope`] carrying ε, δ,
///   kind, and optional `per_segment` for schema-timeline-
///   crossing queries. Standard Prometheus clients ignore
///   unknown top-level fields, so this is a zero-risk extension.
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResponse {
    pub status: String,
    pub data: Option<Value>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Non-error advisories — maps to Prometheus's top-level
    /// `warnings: []` field. The schema-timeline dispatcher uses
    /// this to surface partial results when a query spans a
    /// reconfigure boundary with a non-combinable statistic or a
    /// Purged segment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Prometheus 3.0 `infos: []`. Grafana 11+ renders each
    /// string inline. Used to mirror a human-readable
    /// `accuracy: ε=..., δ=..., kind=...` line when the
    /// structured `accuracy` field is present.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub infos: Vec<String>,
    /// ASAP extension: theoretical accuracy envelope for the
    /// answer (§6.4 of docs/design-sketch-db.md). Unknown to
    /// standard Prometheus clients (they ignore unknown fields),
    /// consumed by Grafana panels / paper artifacts that want
    /// the machine-readable (ε, δ) bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<crate::stores::sketch_db::AccuracyEnvelope>,
}

impl PrometheusResponse {
    pub fn success(data: Value) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
            warnings: Vec::new(),
            infos: Vec::new(),
            accuracy: None,
        }
    }

    /// `success` + a non-empty `warnings` list attached. Used by the
    /// query adapters when the engine returned a `CombinedResult::Partial`.
    pub fn success_with_warnings(data: Value, warnings: Vec<String>) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
            warnings,
            infos: Vec::new(),
            accuracy: None,
        }
    }

    /// Attach an accuracy envelope: sets the structured `accuracy`
    /// field and mirrors a human-readable one-liner to `infos`.
    /// Chainable so both warning + accuracy paths can decorate
    /// the same `success(...)` construction.
    pub fn with_accuracy(mut self, envelope: crate::stores::sketch_db::AccuracyEnvelope) -> Self {
        self.infos.push(envelope.summary());
        self.accuracy = Some(envelope);
        self
    }

    /// Attach the actual `[start_ms, end_ms)` precompute window
    /// the engine consulted to answer this query. Surfaced as a
    /// `precompute_window: ...` line in the response's `infos`
    /// array so the caller can see which pane produced the
    /// answer — important for window queries where the request
    /// range and the answered range may differ (the engine picks
    /// the latest closest pane that overlaps the request).
    pub fn with_precompute_window(mut self, window: (u64, u64)) -> Self {
        self.infos.push(format!(
            "precompute_window: [{}, {}) ms (width {} ms)",
            window.0,
            window.1,
            window.1.saturating_sub(window.0),
        ));
        self
    }

    pub fn error(error_type: &str, error: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.to_string()),
            error: Some(error.to_string()),
            warnings: Vec::new(),
            infos: Vec::new(),
            accuracy: None,
        }
    }
}

/// Prometheus HTTP protocol adapter
pub struct PrometheusHttpAdapter {
    config: AdapterConfig,
}

impl PrometheusHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self { config }
    }

    /// Helper to parse query parameters (used by both GET and POST)
    fn parse_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        let time = if let Some(time_str) = params.get("time") {
            time_str.parse::<f64>().map_err(|e| {
                AdapterError::InvalidParameter(format!("Invalid time parameter: {}", e))
            })?
        } else {
            // Use current time as default
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        };

        Ok(ParsedQueryRequest { query, time })
    }

    /// Helper to parse range query parameters
    fn parse_range_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        let start = params
            .get("start")
            .ok_or_else(|| AdapterError::MissingParameter("start".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid start: {}", e)))?;

        let end = params
            .get("end")
            .ok_or_else(|| AdapterError::MissingParameter("end".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid end: {}", e)))?;

        let step = params
            .get("step")
            .ok_or_else(|| AdapterError::MissingParameter("step".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid step: {}", e)))?;

        // Basic validation
        if start >= end {
            return Err(AdapterError::InvalidParameter(
                "start must be before end".to_string(),
            ));
        }
        if step <= 0.0 {
            return Err(AdapterError::InvalidParameter(
                "step must be positive".to_string(),
            ));
        }

        Ok(ParsedRangeQueryRequest {
            query,
            start,
            end,
            step,
        })
    }
}

#[async_trait]
impl QueryRequestAdapter for PrometheusHttpAdapter {
    async fn parse_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing GET request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    async fn parse_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing POST request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    fn get_query_endpoint(&self) -> &'static str {
        "/api/v1/query"
    }

    async fn parse_range_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing range GET request with params: {:?}",
            params
        );
        self.parse_range_params(&params)
    }

    async fn parse_range_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing range POST request with params: {:?}",
            params
        );
        self.parse_range_params(&params)
    }

    fn get_range_query_endpoint(&self) -> &'static str {
        "/api/v1/query_range"
    }
}

#[async_trait]
impl QueryResponseAdapter for PrometheusHttpAdapter {
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting success response");

        let prometheus_data =
            convert_query_result_to_prometheus(&result.query_result, &result.query_output_labels)
                .map_err(|e| {
                error!("Failed to convert query result: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        // Thread through any schema-timeline dispatcher warnings
        // so they land on the top-level `warnings` field, matching
        // Prometheus's native API.
        let warnings = result.query_result.warnings().to_vec();
        let accuracy = result.query_result.accuracy().cloned();
        let window_used = result.query_result.window_used();
        let mut response = if warnings.is_empty() {
            PrometheusResponse::success(prometheus_data)
        } else {
            PrometheusResponse::success_with_warnings(prometheus_data, warnings)
        };
        if let Some(envelope) = accuracy {
            response = response.with_accuracy(envelope);
        }
        if let Some(window) = window_used {
            response = response.with_precompute_window(window);
        }
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_range_success_response(
        &self,
        result: &QueryResult,
        labels: &KeyByLabelNames,
    ) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting range success response");

        let prometheus_data = convert_range_result_to_prometheus(result, labels).map_err(|e| {
            error!("Failed to convert range result: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let warnings = result.warnings().to_vec();
        let accuracy = result.accuracy().cloned();
        let window_used = result.window_used();
        let mut response = if warnings.is_empty() {
            PrometheusResponse::success(prometheus_data)
        } else {
            PrometheusResponse::success_with_warnings(prometheus_data, warnings)
        };
        if let Some(envelope) = accuracy {
            response = response.with_accuracy(envelope);
        }
        if let Some(window) = window_used {
            response = response.with_precompute_window(window);
        }
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting error response: {:?}", error);

        let (error_type, error_msg) = match error {
            AdapterError::MissingParameter(p) => ("bad_data", format!("Missing parameter: {}", p)),
            AdapterError::InvalidParameter(p) => ("bad_data", format!("Invalid parameter: {}", p)),
            AdapterError::ParseError(e) => ("bad_data", format!("Parse error: {}", e)),
            AdapterError::NetworkError(e) => ("internal", format!("Network error: {}", e)),
            AdapterError::ProtocolError(e) => ("internal", format!("Protocol error: {}", e)),
        };

        let response = PrometheusResponse::error(error_type, &error_msg);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting unsupported query response");

        let response = PrometheusResponse::error("bad_data", "No result for query");
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }
}

#[async_trait]
impl HttpProtocolAdapter for PrometheusHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "PrometheusHTTP"
    }

    fn get_runtime_info_path(&self) -> &'static str {
        "/api/v1/status/runtimeinfo"
    }

    async fn handle_runtime_info(
        &self,
        sketch_index: Arc<crate::stores::sketch_db::index::SketchStore>,
    ) -> Result<Json<Value>, StatusCode> {
        debug!("Handling runtime info request in Prometheus adapter");

        // M2.3.6g — earliest timestamps now come from SketchStore's
        // per-sid `first_seen_unix_ms` metadata. Wire field renamed
        // accordingly below.
        let earliest_timestamps = sketch_index.earliest_timestamps_per_sid();

        // Get runtime info from fallback if available
        let mut runtime_data = if let Some(fallback) = &self.config.fallback {
            debug!("Fetching runtime info from fallback");
            match fallback.get_runtime_info().await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to get runtime info from fallback: {:?}", e);
                    json!({})
                }
            }
        } else {
            json!({})
        };

        // Merge local data with fallback data
        if let Some(data_obj) = runtime_data.as_object_mut() {
            data_obj.insert(
                "earliest_timestamp_per_sid".to_string(),
                serde_json::to_value(earliest_timestamps).unwrap_or(json!({})),
            );
        } else {
            runtime_data = json!({
                "earliest_timestamp_per_sid": earliest_timestamps
            });
        }

        debug!("Successfully merged runtime info with local data");

        // Wrap in Prometheus response format
        let response = PrometheusResponse::success(runtime_data);
        Ok(Json(serde_json::to_value(response).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::types::enums::{QueryLanguage, QueryProtocol};

    fn create_test_adapter() -> PrometheusHttpAdapter {
        let config = AdapterConfig::new(QueryProtocol::PrometheusHttp, QueryLanguage::promql, None);
        PrometheusHttpAdapter::new(config)
    }

    // Tests for parse_range_params

    #[test]
    fn test_parse_range_params_valid() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
        assert_eq!(parsed.start, 1700000000.0);
        assert_eq!(parsed.end, 1700001000.0);
        assert_eq!(parsed.step, 60.0);
    }

    #[test]
    fn test_parse_range_params_missing_query() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "query"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_start() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "start"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_end() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "end"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "step"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_invalid_start() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "not_a_number".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("start")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_start_after_end() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700001000".to_string());
        params.insert("end".to_string(), "1700000000".to_string()); // end before start
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => {
                assert!(msg.contains("start must be before end"))
            }
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_zero_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "0".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("step must be positive")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_negative_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "-60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("step must be positive")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_get_range_query_endpoint() {
        let adapter = create_test_adapter();
        assert_eq!(adapter.get_range_query_endpoint(), "/api/v1/query_range");
    }

    #[tokio::test]
    async fn test_parse_range_get_request() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_get_request(Query(params)).await;

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
    }

    #[tokio::test]
    async fn test_parse_range_post_request() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_post_request(Form(params)).await;

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
    }

    #[test]
    fn success_response_without_warnings_omits_field() {
        let r = PrometheusResponse::success(json!({"resultType": "vector", "result": []}));
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"status\":\"success\""));
        assert!(
            !s.contains("\"warnings\""),
            "empty warnings must be skip-serialised for wire compatibility"
        );
    }

    #[test]
    fn success_response_with_warnings_serialises_the_top_level_field() {
        // Contract: a Partial result coming out of the §7
        // schema-timeline dispatcher lands on Prometheus's native
        // `warnings: []` field at the top of the response,
        // matching upstream behaviour for warning-carrying queries.
        let r = PrometheusResponse::success_with_warnings(
            json!({"resultType": "vector", "result": []}),
            vec![
                "partial result: query spans 2 schemas".to_string(),
                "1 group(s) dropped".to_string(),
            ],
        );
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"warnings\":["));
        assert!(s.contains("partial result: query spans 2 schemas"));
        assert!(s.contains("1 group(s) dropped"));
    }
}
