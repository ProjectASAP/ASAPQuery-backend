use super::{FallbackClient, FallbackResponse};
use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use async_trait::async_trait;
use axum::http::StatusCode;
use reqwest::Client;
use serde_json::Value;
use tracing::{debug, error};

/// Fallback client for Prometheus HTTP API
pub struct PrometheusHttpFallback {
    client: Client,
    base_url: String,
}

impl PrometheusHttpFallback {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    fn apply_headers(
        mut request: reqwest::RequestBuilder,
        headers: std::collections::HashMap<String, String>,
    ) -> reqwest::RequestBuilder {
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request
    }
}

#[async_trait]
impl FallbackClient for PrometheusHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO PROMETHEUS ===");
        debug!(
            "Forwarding query: '{}', time: {}",
            request.query, request.time
        );

        // Build the full URL for the Prometheus endpoint
        let full_url = format!("{}/api/v1/query", self.base_url.trim_end_matches('/'));

        debug!("Full forwarding URL: {}", full_url);

        // Prepare query parameters for forwarding
        let mut query_params = vec![
            ("query", request.query.clone()),
            ("time", request.time.to_string()),
        ];
        if let Some(timeout) = &request.timeout {
            query_params.push(("timeout", timeout.clone()));
        }

        debug!("Final query parameters for forwarding: {:?}", query_params);

        // Forward the request to Prometheus
        debug!("Sending request to Prometheus...");
        match self
            .client
            .get(&full_url)
            .query(&query_params)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!("Received response from Prometheus, status: {}", status);
                match response.json::<Value>().await {
                    Ok(prometheus_response) => {
                        debug!(
                            "Successfully parsed Prometheus response: {:?}",
                            prometheus_response
                        );
                        debug!("=== PROMETHEUS FORWARD SUCCESS ===");
                        Ok(FallbackResponse::Json(prometheus_response))
                    }
                    Err(parse_err) => {
                        error!("Failed to parse Prometheus response: {}", parse_err);
                        debug!("=== PROMETHEUS FORWARD PARSE ERROR ===");

                        use crate::drivers::query::adapters::PrometheusResponse;
                        let error = PrometheusResponse::error(
                            "internal",
                            "Failed to parse Prometheus response",
                        );
                        Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
                    }
                }
            }
            Err(req_err) => {
                error!("Failed to forward query to Prometheus: {}", req_err);
                debug!("=== PROMETHEUS FORWARD REQUEST ERROR ===");

                use crate::drivers::query::adapters::PrometheusResponse;
                let error = PrometheusResponse::error(
                    "internal",
                    &format!("Failed to forward query to Prometheus: {}", req_err),
                );
                Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
            }
        }
    }

    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        headers: std::collections::HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        let full_url = format!("{}/api/v1/query", self.base_url.trim_end_matches('/'));
        let mut query_params = vec![
            ("query", request.query.clone()),
            ("time", request.time.to_string()),
        ];
        if let Some(timeout) = &request.timeout {
            query_params.push(("timeout", timeout.clone()));
        }
        let request = Self::apply_headers(self.client.get(full_url).query(&query_params), headers);
        let response = match request
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                use crate::drivers::query::adapters::PrometheusResponse;
                return Ok(FallbackResponse::Json(
                    serde_json::to_value(PrometheusResponse::error(
                        "internal",
                        &format!("Failed to forward query to Prometheus: {error}"),
                    ))
                    .expect("Prometheus error response serializes"),
                ));
            }
        };
        let status = response.status();
        let payload = match response.json::<Value>().await {
            Ok(payload) => payload,
            Err(_) => {
                use crate::drivers::query::adapters::PrometheusResponse;
                serde_json::to_value(PrometheusResponse::error(
                    "internal",
                    "Failed to parse Prometheus response",
                ))
                .expect("Prometheus error response serializes")
            }
        };
        if !status.is_success() {
            debug!(status = %status, "Prometheus instant fallback returned an error response");
        }
        Ok(FallbackResponse::Json(payload))
    }

    async fn execute_range_query(
        &self,
        request: &ParsedRangeQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        self.execute_range_query_with_headers(request, std::collections::HashMap::new())
            .await
    }

    async fn execute_range_query_with_headers(
        &self,
        request: &ParsedRangeQueryRequest,
        headers: std::collections::HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        let full_url = format!("{}/api/v1/query_range", self.base_url.trim_end_matches('/'));
        let mut query_params = vec![
            ("query", request.query.clone()),
            ("start", request.start.to_string()),
            ("end", request.end.to_string()),
            ("step", request.step.to_string()),
        ];
        if let Some(timeout) = &request.timeout {
            query_params.push(("timeout", timeout.clone()));
        }
        let request = Self::apply_headers(self.client.get(full_url).query(&query_params), headers);
        let response = request
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        let status = response.status();
        let payload = response
            .json::<Value>()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        if !status.is_success() {
            debug!(status = %status, "Prometheus range fallback returned an error response");
        }
        Ok(FallbackResponse::Json(payload))
    }

    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        debug!("Fetching runtime info from Prometheus fallback");

        // Build the runtime info URL
        let url = format!(
            "{}/api/v1/status/runtimeinfo",
            self.base_url.trim_end_matches('/')
        );

        debug!("Runtime info URL: {}", url);

        // Send request to Prometheus
        match self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                match response.text().await {
                    Ok(text) => {
                        debug!("Prometheus runtime info response: {}", text);

                        // Check for VictoriaMetrics unsupported path error
                        if text.contains("unsupported path requested") {
                            debug!("VictoriaMetrics detected - returning empty runtime info");
                            return Ok(serde_json::json!({}));
                        }

                        // Try to parse as JSON
                        match serde_json::from_str::<Value>(&text) {
                            Ok(json) => {
                                // Extract the data field if it exists (Prometheus format)
                                if let Some(data) = json.get("data") {
                                    Ok(data.clone())
                                } else {
                                    Ok(json)
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse runtime info response: {}", e);
                                Ok(serde_json::json!({}))
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read runtime info response: {}", e);
                        Ok(serde_json::json!({}))
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch runtime info from Prometheus: {}", e);
                Ok(serde_json::json!({}))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Query, http::HeaderMap, routing::get, Json, Router};
    use std::{collections::HashMap, sync::Arc};
    use tokio::{net::TcpListener, sync::Mutex};

    #[tokio::test]
    async fn range_fallback_preserves_parameters_and_request_context() {
        let captured = Arc::new(Mutex::new(None));
        let handler_capture = Arc::clone(&captured);
        let app = Router::new().route(
            "/api/v1/query_range",
            get(
                move |Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                    let captured = Arc::clone(&handler_capture);
                    async move {
                        *captured.lock().await = Some((params, headers));
                        Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "matrix", "result": []}
                        }))
                    }
                },
            ),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let fallback = PrometheusHttpFallback::new(format!("http://{address}"));
        let request = ParsedRangeQueryRequest {
            query: "rate(requests_total[1m])".into(),
            start: 100.25,
            end: 200.75,
            step: 15.5,
            timeout: Some("7s".into()),
        };
        fallback
            .execute_range_query_with_headers(
                &request,
                HashMap::from([
                    ("authorization".into(), "Bearer secret".into()),
                    ("x-scope-orgid".into(), "tenant-a".into()),
                ]),
            )
            .await
            .unwrap();

        let (params, headers) = captured.lock().await.take().unwrap();
        assert_eq!(params.get("query"), Some(&request.query));
        assert_eq!(params.get("start").map(String::as_str), Some("100.25"));
        assert_eq!(params.get("end").map(String::as_str), Some("200.75"));
        assert_eq!(params.get("step").map(String::as_str), Some("15.5"));
        assert_eq!(params.get("timeout").map(String::as_str), Some("7s"));
        assert_eq!(headers["authorization"], "Bearer secret");
        assert_eq!(headers["x-scope-orgid"], "tenant-a");
    }
}
