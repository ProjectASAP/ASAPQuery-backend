use super::request::ClickHouseQueryRequest;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
};
use std::str::FromStr;

#[derive(Debug)]
pub struct ClickHouseRawResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseFallbackError {
    #[error("ClickHouse request failed: {0}")]
    Request(String),
    #[error("ClickHouse returned an invalid response: {0}")]
    Response(String),
}

#[async_trait]
pub trait ClickHouseExactBackend: Send + Sync {
    async fn execute(
        &self,
        request: &ClickHouseQueryRequest,
    ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError>;
    async fn ping(&self) -> Result<ClickHouseRawResponse, ClickHouseFallbackError>;
}

pub struct ClickHouseHttpFallback {
    client: reqwest::Client,
    base_url: String,
    default_database: String,
}

impl ClickHouseHttpFallback {
    pub fn new(base_url: String, default_database: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            default_database,
        }
    }

    async fn copy_response(
        response: reqwest::Response,
    ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
        let status = StatusCode::from_u16(response.status().as_u16())
            .map_err(|e| ClickHouseFallbackError::Response(e.to_string()))?;
        let mut headers = HeaderMap::new();
        for (name, value) in response.headers() {
            if let (Ok(name), Ok(value)) = (
                HeaderName::from_str(name.as_str()),
                HeaderValue::from_bytes(value.as_bytes()),
            ) {
                headers.append(name, value);
            }
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| ClickHouseFallbackError::Response(e.to_string()))?;
        Ok(ClickHouseRawResponse {
            status,
            headers,
            body,
        })
    }
}

#[async_trait]
impl ClickHouseExactBackend for ClickHouseHttpFallback {
    async fn execute(
        &self,
        request: &ClickHouseQueryRequest,
    ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
        let mut parameters = request.parameters.clone();
        parameters
            .entry("database".into())
            .or_insert_with(|| self.default_database.clone());
        let mut upstream = self
            .client
            .request(
                reqwest::Method::from_bytes(request.method.as_str().as_bytes())
                    .map_err(|e| ClickHouseFallbackError::Request(e.to_string()))?,
                format!("{}/", self.base_url.trim_end_matches('/')),
            )
            .query(&parameters)
            .body(request.body.clone());
        for name in [
            "authorization",
            "x-clickhouse-user",
            "x-clickhouse-key",
            "user-agent",
        ] {
            if let Some(value) = request.headers.get(name).and_then(|v| v.to_str().ok()) {
                upstream = upstream.header(name, value);
            }
        }
        let response = upstream
            .send()
            .await
            .map_err(|e| ClickHouseFallbackError::Request(e.to_string()))?;
        Self::copy_response(response).await
    }

    async fn ping(&self) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
        let response = self
            .client
            .get(format!("{}/ping", self.base_url.trim_end_matches('/')))
            .send()
            .await
            .map_err(|e| ClickHouseFallbackError::Request(e.to_string()))?;
        Self::copy_response(response).await
    }
}
