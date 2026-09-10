use super::{FallbackClient, FallbackResponse, PrometheusHttpFallback};
use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use async_trait::async_trait;
use axum::http::StatusCode;
use std::collections::HashMap;

/// Exact MetricsQL backend. VictoriaMetrics' query API is wire-compatible with
/// Prometheus, so transport is shared while this type keeps backend selection
/// independent from the Prometheus listener.
pub struct VictoriaMetricsHttpFallback(PrometheusHttpFallback);

impl VictoriaMetricsHttpFallback {
    pub fn new(base_url: String) -> Self {
        Self(PrometheusHttpFallback::new(base_url))
    }
}

#[async_trait]
impl FallbackClient for VictoriaMetricsHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        self.0.execute_query(request).await
    }
    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        self.0.execute_query_with_headers(request, headers).await
    }
    async fn execute_range_query(
        &self,
        request: &ParsedRangeQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        self.0.execute_range_query(request).await
    }
    async fn execute_range_query_with_headers(
        &self,
        request: &ParsedRangeQueryRequest,
        headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        self.0
            .execute_range_query_with_headers(request, headers)
            .await
    }
    async fn get_runtime_info(&self) -> Result<serde_json::Value, StatusCode> {
        Ok(serde_json::json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Query, routing::get, Json, Router};

    #[tokio::test]
    async fn exact_fallback_preserves_metricsql_expression() {
        let app = Router::new().route(
            "/api/v1/query",
            get(|Query(params): Query<HashMap<String, String>>| async move {
                Json(serde_json::json!({
                    "status": "success",
                    "data": {"received": params["query"]}
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = VictoriaMetricsHttpFallback::new(format!("http://{address}"));
        let response = client
            .execute_query(&ParsedQueryRequest {
                query: "rate(requests[5m]) keep_metric_names".into(),
                time: 1_700_000_000.0,
                timeout: Some("5s".into()),
            })
            .await
            .unwrap();
        let FallbackResponse::Json(payload) = response else {
            panic!("VictoriaMetrics fallback must return JSON")
        };
        assert_eq!(
            payload["data"]["received"],
            "rate(requests[5m]) keep_metric_names"
        );
    }
}
