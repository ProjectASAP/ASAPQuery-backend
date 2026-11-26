//! The client of a Prometheus-compatible server (Prometheus, or the raw
//! store in forward mode) that answers what the data plane does not.
//!
//! [`PrometheusHttpFallback::forward`] proxies a client's request as it
//! arrived: method, path, URL query, body and end-to-end headers. The data
//! plane's routes use it, so the server sees exactly the client's request.
//! [`FallbackClient::execute`] sends a parsed [`QueryRequest`] instead, for
//! callers that hold no HTTP request.

use std::time::Duration;

use async_trait::async_trait;
use common::{
    FallbackClient, FallbackError, FallbackResponse, QueryEval, QueryLanguage, QueryRequest,
};
use reqwest::header::{self, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Body, Method};

/// The `Content-Type` of a response whose server sent none: bytes of
/// unknown type (RFC 9110 section 8.3).
const UNKNOWN_CONTENT_TYPE: &str = "application/octet-stream";

/// Sends requests to one Prometheus-compatible server.
pub struct PrometheusHttpFallback {
    /// Without a trailing `/`.
    base_url: String,
    client: reqwest::Client,
}

impl PrometheusHttpFallback {
    /// A client of the server at `base_url`, each request bounded by
    /// `timeout`, from sending it to reading the whole response.
    pub fn new(base_url: &str, timeout: Duration) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("reqwest client with only a timeout set"),
        }
    }

    /// The same client, sending to `base_url` instead.
    pub fn with_base_url(self, base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            ..self
        }
    }

    /// The server's base URL, without a trailing `/`.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Sends `method` `path` (with `query`, the URL query as received, and
    /// `body`) to the server with the end-to-end request `headers`
    /// (including `Authorization` and tenancy headers such as
    /// `X-Scope-OrgID`), and returns the server's status, body and
    /// `Content-Type`. Errors: the server cannot be reached or its response
    /// cannot be read.
    pub async fn forward(
        &self,
        method: Method,
        path: &str,
        query: Option<&str>,
        headers: &HeaderMap,
        body: impl Into<Body>,
    ) -> Result<FallbackResponse, FallbackError> {
        let mut url = format!("{}{path}", self.base_url);
        if let Some(query) = query {
            url = format!("{url}?{query}");
        }
        let request = self
            .client
            .request(method, url)
            .headers(forwarded_headers(headers))
            .body(body);
        let upstream = request
            .send()
            .await
            .map_err(|error| FallbackError::new(format!("forwarding to Prometheus: {error}")))?;
        let status_code = upstream.status().as_u16();
        let content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or(UNKNOWN_CONTENT_TYPE)
            .to_string();
        let body = upstream
            .bytes()
            .await
            .map_err(|error| FallbackError::new(format!("reading Prometheus response: {error}")))?;
        Ok(FallbackResponse {
            status_code,
            content_type,
            body: body.into(),
        })
    }
}

#[async_trait]
impl FallbackClient for PrometheusHttpFallback {
    /// `GET /api/v1/query` (`query`, `time`) or `/api/v1/query_range`
    /// (`query`, `start`, `end`, `step`), times in seconds. A PromQL or
    /// MetricsQL query only; header pairs that are not valid HTTP headers
    /// are skipped.
    async fn execute(
        &self,
        request: &QueryRequest,
        headers: &[(String, String)],
    ) -> Result<FallbackResponse, FallbackError> {
        if !matches!(
            request.language,
            QueryLanguage::PromQL | QueryLanguage::MetricsQL
        ) {
            return Err(FallbackError::new(format!(
                "a Prometheus server answers PromQL, not {:?}",
                request.language
            )));
        }
        // In a block: the serializer is not `Send`, so it must not live
        // across the await.
        let (path, query) = {
            let mut params = form_urlencoded::Serializer::new(String::new());
            params.append_pair("query", &request.query);
            match request.eval {
                QueryEval::Instant { time_ms } => {
                    params.append_pair("time", &seconds(time_ms));
                    ("/api/v1/query", params.finish())
                }
                QueryEval::Range {
                    start_ms,
                    end_ms,
                    step_ms,
                } => {
                    params.append_pair("start", &seconds(start_ms));
                    params.append_pair("end", &seconds(end_ms));
                    params.append_pair("step", &seconds(step_ms));
                    ("/api/v1/query_range", params.finish())
                }
            }
        };
        let headers: HeaderMap = headers
            .iter()
            .filter_map(|(name, value)| {
                Some((
                    HeaderName::try_from(name.as_str()).ok()?,
                    HeaderValue::try_from(value.as_str()).ok()?,
                ))
            })
            .collect();
        self.forward(Method::GET, path, Some(&query), &headers, Vec::new())
            .await
    }
}

/// `ms` as Prometheus reads a time or step: seconds, exact to the
/// millisecond.
fn seconds(ms: i64) -> String {
    let sign = if ms < 0 { "-" } else { "" };
    let ms = ms.unsigned_abs();
    format!("{sign}{}.{:03}", ms / 1000, ms % 1000)
}

/// Request headers not forwarded: hop-by-hop headers (RFC 9110 section 7.6.1),
/// `Host` and `Content-Length`, which the client sets for the new request,
/// and `Accept-Encoding`, because only the response's `Content-Type` is
/// copied back, so the response must not be compressed.
const NOT_FORWARDED: [HeaderName; 11] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
    header::HOST,
    header::CONTENT_LENGTH,
    header::ACCEPT_ENCODING,
];

/// Copies every request header except those in `NOT_FORWARDED` and any
/// named by `Connection`.
fn forwarded_headers(headers: &HeaderMap) -> HeaderMap {
    let named_by_connection: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        if NOT_FORWARDED.contains(name) || named_by_connection.iter().any(|n| n == name.as_str()) {
            continue;
        }
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{OriginalUri, State};
    use axum::http::HeaderMap as AxumHeaders;
    use axum::routing::any;
    use axum::Router;

    use super::*;

    /// What the mock server received: method, path and query, headers.
    type Received = Arc<Mutex<Vec<(String, String, Vec<(String, String)>)>>>;

    /// A server that records each request and answers 200 with a JSON
    /// body naming its path; its base URL.
    async fn server(received: Received) -> String {
        async fn record(
            State(received): State<Received>,
            method: axum::http::Method,
            OriginalUri(uri): OriginalUri,
            headers: AxumHeaders,
        ) -> ([(header::HeaderName, &'static str); 1], String) {
            let headers = headers
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
                .collect();
            received
                .lock()
                .unwrap()
                .push((method.to_string(), uri.to_string(), headers));
            (
                [(header::CONTENT_TYPE, "application/json")],
                format!("{{\"path\":\"{}\"}}", uri.path()),
            )
        }
        let app = Router::new().fallback(any(record)).with_state(received);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/")
    }

    /// A parsed instant and range query reach the server as Prometheus
    /// API requests with their times in seconds and the client's headers;
    /// the server's status, `Content-Type` and body come back unchanged.
    #[tokio::test]
    async fn sends_a_parsed_query_as_a_prometheus_request() {
        let received = Received::default();
        let fallback =
            PrometheusHttpFallback::new(&server(received.clone()).await, Duration::from_secs(5));
        let instant = QueryRequest {
            language: QueryLanguage::PromQL,
            query: "sum(rate(x[1m]))".into(),
            eval: QueryEval::Instant { time_ms: 60_250 },
        };
        let headers = [("X-Scope-OrgID".to_string(), "tenant".to_string())];
        let response = fallback.execute(&instant, &headers).await.unwrap();
        assert_eq!(
            response,
            FallbackResponse {
                status_code: 200,
                content_type: "application/json".into(),
                body: br#"{"path":"/api/v1/query"}"#.to_vec(),
            }
        );
        let range = QueryRequest {
            eval: QueryEval::Range {
                start_ms: -1_500,
                end_ms: 30_000,
                step_ms: 15_000,
            },
            ..instant
        };
        fallback.execute(&range, &[]).await.unwrap();
        let received = received.lock().unwrap();
        assert_eq!(received[0].0, "GET");
        assert_eq!(
            received[0].1,
            "/api/v1/query?query=sum%28rate%28x%5B1m%5D%29%29&time=60.250"
        );
        assert!(received[0]
            .2
            .contains(&("x-scope-orgid".to_string(), "tenant".to_string())));
        assert_eq!(
            received[1].1,
            "/api/v1/query_range?query=sum%28rate%28x%5B1m%5D%29%29&start=-1.500&end=30.000&step=15.000"
        );
    }

    /// A SQL query is not sent; an unreachable server is an error.
    #[tokio::test]
    async fn refuses_sql_and_reports_an_unreachable_server() {
        let fallback = PrometheusHttpFallback::new("http://127.0.0.1:1", Duration::from_secs(5));
        let mut request = QueryRequest {
            language: QueryLanguage::SQL(asap_types::workload::SqlDialect::ClickhouseSQL),
            query: "SELECT 1".into(),
            eval: QueryEval::Instant { time_ms: 0 },
        };
        assert!(fallback.execute(&request, &[]).await.is_err());
        request.language = QueryLanguage::PromQL;
        let error = fallback.execute(&request, &[]).await.unwrap_err();
        assert!(
            error.message.starts_with("forwarding to Prometheus: "),
            "{error}"
        );
    }

    /// Hop-by-hop headers, those `Connection` names, `Host`,
    /// `Content-Length` and `Accept-Encoding` are dropped; the rest kept.
    #[test]
    fn forwards_end_to_end_headers_only() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, "keep-alive, x-hop".parse().unwrap());
        headers.insert("x-hop", "1".parse().unwrap());
        headers.insert(header::HOST, "a".parse().unwrap());
        headers.insert(header::ACCEPT_ENCODING, "gzip".parse().unwrap());
        headers.insert(header::AUTHORIZATION, "Bearer t".parse().unwrap());
        headers.insert("x-scope-orgid", "tenant".parse().unwrap());
        let forwarded = forwarded_headers(&headers);
        let mut names: Vec<&str> = forwarded.keys().map(HeaderName::as_str).collect();
        names.sort_unstable();
        assert_eq!(names, ["authorization", "x-scope-orgid"]);
    }
}
