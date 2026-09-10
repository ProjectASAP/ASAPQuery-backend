use super::{FallbackClient, FallbackResponse};
use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use async_trait::async_trait;
use axum::http::StatusCode;
use std::collections::HashMap;

/// Exact MetricsQL backend. VictoriaMetrics' query API is wire-compatible with
/// Prometheus, so transport is shared while this type keeps backend selection
/// independent from the Prometheus listener.
pub struct VictoriaMetricsHttpFallback {
    client: reqwest::Client,
    base_url: String,
}

impl VictoriaMetricsHttpFallback {
    pub fn new(base_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
        }
    }

    fn endpoint(&self, tenant: Option<&str>, range: bool) -> String {
        let suffix = if range { "query_range" } else { "query" };
        match tenant {
            Some(tenant) => format!(
                "{}/select/{tenant}/prometheus/api/v1/{suffix}",
                self.base_url.trim_end_matches('/')
            ),
            None => format!("{}/api/v1/{suffix}", self.base_url.trim_end_matches('/')),
        }
    }

    async fn send(
        &self,
        endpoint: String,
        params: Vec<(&str, String)>,
        mut headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        headers.remove("x-asap-tenant");
        let mut request = self.client.get(endpoint).query(&params);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        let status = StatusCode::from_u16(response.status().as_u16())
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        let mut response_headers = axum::http::HeaderMap::new();
        for name in ["content-type", "cache-control", "warning", "retry-after"] {
            if let Some(value) = response.headers().get(name) {
                if let Ok(value) = axum::http::HeaderValue::from_bytes(value.as_bytes()) {
                    response_headers.insert(
                        axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        value,
                    );
                }
            }
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        Ok(FallbackResponse::Forwarded {
            status,
            headers: response_headers,
            body,
        })
    }
}

#[async_trait]
impl FallbackClient for VictoriaMetricsHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        self.execute_query_with_headers(request, HashMap::new())
            .await
    }
    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        let tenant = headers.get("x-asap-tenant").cloned();
        let mut params = vec![
            ("query", request.query.clone()),
            ("time", request.time.to_string()),
        ];
        if let Some(timeout) = &request.timeout {
            params.push(("timeout", timeout.clone()));
        }
        self.send(self.endpoint(tenant.as_deref(), false), params, headers)
            .await
    }
    async fn execute_range_query(
        &self,
        request: &ParsedRangeQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        self.execute_range_query_with_headers(request, HashMap::new())
            .await
    }
    async fn execute_range_query_with_headers(
        &self,
        request: &ParsedRangeQueryRequest,
        headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        let tenant = headers.get("x-asap-tenant").cloned();
        let mut params = vec![
            ("query", request.query.clone()),
            ("start", request.start.to_string()),
            ("end", request.end.to_string()),
            ("step", request.step.to_string()),
        ];
        if let Some(timeout) = &request.timeout {
            params.push(("timeout", timeout.clone()));
        }
        self.send(self.endpoint(tenant.as_deref(), true), params, headers)
            .await
    }
    async fn get_runtime_info(&self) -> Result<serde_json::Value, StatusCode> {
        Ok(serde_json::json!({}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Query, response::IntoResponse, routing::get, Json, Router};

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
        let FallbackResponse::Forwarded { status, body, .. } = response else {
            panic!("VictoriaMetrics fallback must forward the upstream response")
        };
        assert_eq!(status, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["data"]["received"],
            "rate(requests[5m]) keep_metric_names"
        );
    }

    #[tokio::test]
    async fn cluster_fallback_uses_tenant_prefixed_path() {
        let app = Router::new().route(
            "/select/42/prometheus/api/v1/query_range",
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
            .execute_range_query_with_headers(
                &ParsedRangeQueryRequest {
                    query: "sum(rate(requests[5m]))".into(),
                    start: 1.0,
                    end: 2.0,
                    step: 1.0,
                    timeout: None,
                },
                HashMap::from([("x-asap-tenant".into(), "42".into())]),
            )
            .await
            .unwrap();
        let FallbackResponse::Forwarded { status, body, .. } = response else {
            panic!("VictoriaMetrics cluster fallback must forward the upstream response")
        };
        assert_eq!(status, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["data"]["received"], "sum(rate(requests[5m]))");
    }

    #[tokio::test]
    async fn cluster_fallback_preserves_upstream_status_and_protocol_headers() {
        let app = Router::new().route(
            "/select/42/prometheus/api/v1/query",
            get(|| async {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    [("content-type", "application/json"), ("retry-after", "7")],
                    r#"{"status":"error","error":"bad MetricsQL"}"#,
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = VictoriaMetricsHttpFallback::new(format!("http://{address}"))
            .execute_query_with_headers(
                &ParsedQueryRequest {
                    query: "invalid".into(),
                    time: 1.0,
                    timeout: None,
                },
                HashMap::from([("x-asap-tenant".into(), "42".into())]),
            )
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(response.headers()["retry-after"], "7");
        assert_eq!(response.headers()["x-asap-execution"], "exact_fallback");
    }
}
