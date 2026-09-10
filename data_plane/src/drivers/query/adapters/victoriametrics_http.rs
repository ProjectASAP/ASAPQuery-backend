use super::{
    AdapterConfig, AdapterError, HttpProtocolAdapter, ParsedQueryRequest, ParsedRangeQueryRequest,
    PrometheusHttpAdapter, QueryExecutionResult, QueryRequestAdapter, QueryResponseAdapter,
};
use asap_types::KeyByLabelNames;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{Form, Query},
    http::StatusCode,
    response::{Json, Response},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

/// VictoriaMetrics HTTP adapter. VictoriaMetrics intentionally uses the
/// Prometheus-compatible wire envelope; language classification remains in the
/// MetricsQL binder and engine fallback path.
pub struct VictoriaMetricsHttpAdapter {
    wire: PrometheusHttpAdapter,
}

impl VictoriaMetricsHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self {
            wire: PrometheusHttpAdapter::new(config),
        }
    }
}

#[async_trait]
impl QueryRequestAdapter for VictoriaMetricsHttpAdapter {
    async fn parse_get_request(
        &self,
        params: Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        self.wire.parse_get_request(params).await
    }
    async fn parse_post_request(
        &self,
        params: Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        self.wire.parse_post_request(params).await
    }
    async fn parse_json_post_request(
        &self,
        body: Bytes,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        self.wire.parse_json_post_request(body).await
    }
    fn get_query_endpoint(&self) -> &'static str {
        "/api/v1/query"
    }
    async fn parse_range_get_request(
        &self,
        params: Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        self.wire.parse_range_get_request(params).await
    }
    async fn parse_range_post_request(
        &self,
        params: Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        self.wire.parse_range_post_request(params).await
    }
    fn get_range_query_endpoint(&self) -> &'static str {
        "/api/v1/query_range"
    }
}

#[async_trait]
impl QueryResponseAdapter for VictoriaMetricsHttpAdapter {
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode> {
        self.wire.format_success_response(result).await
    }
    async fn format_range_success_response(
        &self,
        result: &crate::query_engines::QueryResult,
        labels: &KeyByLabelNames,
    ) -> Result<Response, StatusCode> {
        self.wire
            .format_range_success_response(result, labels)
            .await
    }
    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        self.wire.format_error_response(error).await
    }
    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        self.wire.format_unsupported_query_response().await
    }
}

#[async_trait]
impl HttpProtocolAdapter for VictoriaMetricsHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "VictoriaMetrics HTTP / MetricsQL"
    }
    fn canonical_plan_identity(&self, query: &str) -> Result<Option<String>, AdapterError> {
        asap_frontend_metricsql::canonical_metricsql(query)
            .map(Some)
            .map_err(|error| AdapterError::ParseError(format!("frontend.metricsql: {error}")))
    }
    fn get_runtime_info_path(&self) -> &'static str {
        "/api/v1/status/runtimeinfo"
    }
    async fn handle_runtime_info(
        &self,
        index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Result<Json<Value>, StatusCode> {
        self.wire.handle_runtime_info(index).await
    }
    async fn handle_runtime_info_with_headers(
        &self,
        index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
        headers: HashMap<String, String>,
    ) -> Result<Json<Value>, StatusCode> {
        self.wire
            .handle_runtime_info_with_headers(index, headers)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::types::enums::{QueryLanguage, QueryProtocol};

    fn adapter() -> VictoriaMetricsHttpAdapter {
        VictoriaMetricsHttpAdapter::new(AdapterConfig::new(
            QueryProtocol::PrometheusHttp,
            QueryLanguage::PromQl,
            None,
        ))
    }

    #[tokio::test]
    async fn preserves_metricsql_expression_for_binding_or_fallback() {
        let mut params = HashMap::new();
        params.insert(
            "query".into(),
            "rate(requests[5m]) keep_metric_names".into(),
        );
        params.insert("time".into(), "1700000000".into());
        let parsed = adapter().parse_get_request(Query(params)).await.unwrap();
        assert_eq!(parsed.query, "rate(requests[5m]) keep_metric_names");
    }

    #[test]
    fn exposes_victoriametrics_query_endpoints() {
        assert_eq!(adapter().get_query_endpoint(), "/api/v1/query");
        assert_eq!(adapter().get_range_query_endpoint(), "/api/v1/query_range");
    }
}
