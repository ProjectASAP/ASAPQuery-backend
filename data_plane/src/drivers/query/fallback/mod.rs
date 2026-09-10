use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde_json::Value;
use std::collections::HashMap;

use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};

/// Response format from fallback backend
#[derive(Debug, Clone)]
pub enum FallbackResponse {
    /// JSON response (used by Prometheus, etc.)
    Json(Value),
    /// Plain text response.
    Text(String),
}

impl IntoResponse for FallbackResponse {
    fn into_response(self) -> Response {
        let mut response = match self {
            FallbackResponse::Json(value) => Json(value).into_response(),
            FallbackResponse::Text(text) => {
                // Return plain text with appropriate content type
                (
                    [(
                        axum::http::header::CONTENT_TYPE,
                        "text/tab-separated-values",
                    )],
                    text,
                )
                    .into_response()
            }
        };
        response.headers_mut().insert(
            "x-asap-execution",
            axum::http::HeaderValue::from_static("exact_fallback"),
        );
        response
    }
}

#[cfg(test)]
mod execution_attribution_tests {
    use super::*;

    // An HTTP fallback must be distinguishable from warm execution, even if
    // its upstream response carries misleading or absent source annotations.
    #[test]
    fn forwarded_response_has_backend_owned_execution_marker() {
        let response = FallbackResponse::Json(serde_json::json!({
            "status": "success", "infos": ["data_source: asap_query"]
        }))
        .into_response();
        assert_eq!(response.headers()["x-asap-execution"], "exact_fallback");
    }
}

/// Client for forwarding unsupported queries to a fallback backend
#[async_trait]
pub trait FallbackClient: Send + Sync {
    /// Execute a query against the fallback backend
    ///
    /// # Arguments
    /// * `request` - The parsed query request (query string, time, etc.)
    ///
    /// # Returns
    /// Protocol-specific response from the fallback backend (JSON or Text)
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode>;

    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        _headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        // Default implementation delegates to execute_query
        self.execute_query(request).await
    }

    /// Execute a Prometheus range query against the fallback backend.
    async fn execute_range_query(
        &self,
        _request: &ParsedRangeQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        Err(StatusCode::NOT_IMPLEMENTED)
    }

    async fn execute_range_query_with_headers(
        &self,
        request: &ParsedRangeQueryRequest,
        _headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        self.execute_range_query(request).await
    }

    /// Get runtime info from the fallback backend (optional)
    ///
    /// # Returns
    /// Runtime info as JSON, or empty object if not supported
    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        // Default implementation: return empty object
        Ok(serde_json::json!({}))
    }

    async fn get_runtime_info_with_headers(
        &self,
        headers: HashMap<String, String>,
    ) -> Result<Value, StatusCode> {
        // Default implementation delegates to get_runtime_info
        let _ = headers;
        self.get_runtime_info().await
    }
}

mod prometheus;
mod victoriametrics;

pub mod metrics;

pub use prometheus::PrometheusHttpFallback;
pub use victoriametrics::VictoriaMetricsHttpFallback;
