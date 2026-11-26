//! The client of a ClickHouse server that answers the queries the data
//! plane does not, as ASAPQuery's `ClickHouseHttpFallback` sends them.

use std::time::Duration;

use async_trait::async_trait;
use common::{
    FallbackClient, FallbackError, FallbackResponse, QueryEval, QueryLanguage, QueryRequest,
    SqlDialect,
};
use reqwest::header::{self, HeaderName, HeaderValue};

/// Sends queries to one ClickHouse server's HTTP interface.
pub struct ClickHouseHttpFallback {
    /// `<base URL>/?database=<database>`, where every query is posted.
    query_url: String,
    client: reqwest::Client,
}

impl ClickHouseHttpFallback {
    /// A client of the server at `base_url` that runs queries in
    /// `database`, each request bounded by `timeout`, from sending it to
    /// reading the whole response.
    pub fn new(base_url: &str, database: &str, timeout: Duration) -> Self {
        let database: String = form_urlencoded::byte_serialize(database.as_bytes()).collect();
        Self {
            query_url: format!("{}/?database={database}", base_url.trim_end_matches('/')),
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("reqwest client with only a timeout set"),
        }
    }
}

#[async_trait]
impl FallbackClient for ClickHouseHttpFallback {
    /// `POST /?database=<database>` with the query as the body, so the
    /// server answers in its default format (TabSeparated) unless the
    /// query names one. A ClickHouse SQL instant query only: ClickHouse has
    /// no range queries. Header pairs that are not valid HTTP headers are
    /// skipped.
    async fn execute(
        &self,
        request: &QueryRequest,
        headers: &[(String, String)],
    ) -> Result<FallbackResponse, FallbackError> {
        if request.language != QueryLanguage::SQL(SqlDialect::ClickhouseSQL) {
            return Err(FallbackError::new(format!(
                "a ClickHouse server answers ClickHouse SQL, not {:?}",
                request.language
            )));
        }
        if let QueryEval::Range { .. } = request.eval {
            return Err(FallbackError::new("ClickHouse has no range queries"));
        }
        let mut upstream = self.client.post(&self.query_url);
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::try_from(value.as_str()),
            ) {
                upstream = upstream.header(name, value);
            }
        }
        let upstream = upstream
            .body(request.query.clone())
            .send()
            .await
            .map_err(|error| FallbackError::new(format!("forwarding to ClickHouse: {error}")))?;
        let status_code = upstream.status().as_u16();
        // A server that names no type sent bytes of unknown type (RFC 9110
        // section 8.3).
        let content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let body = upstream
            .bytes()
            .await
            .map_err(|error| FallbackError::new(format!("reading ClickHouse response: {error}")))?;
        Ok(FallbackResponse {
            status_code,
            content_type,
            body: body.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::extract::{OriginalUri, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::Router;

    use super::*;

    /// What the mock server received: path and query, `Authorization`, body.
    type Received = Arc<Mutex<Vec<(String, Option<String>, String)>>>;

    /// A ClickHouse server that records each request and answers 200 with
    /// a TabSeparated row, or, for a query on `missing`, 404 with
    /// ClickHouse's plain-text exception; its base URL.
    async fn server(received: Received) -> String {
        async fn record(
            State(received): State<Received>,
            OriginalUri(uri): OriginalUri,
            headers: HeaderMap,
            body: String,
        ) -> (StatusCode, [(header::HeaderName, &'static str); 1], String) {
            let authorization = headers
                .get(header::AUTHORIZATION)
                .map(|v| v.to_str().unwrap().to_string());
            let missing = body.contains("missing");
            received
                .lock()
                .unwrap()
                .push((uri.to_string(), authorization, body));
            if missing {
                return (
                    StatusCode::NOT_FOUND,
                    [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")],
                    "Code: 60. DB::Exception: Table default.missing does not exist.".into(),
                );
            }
            (
                StatusCode::OK,
                [(
                    header::CONTENT_TYPE,
                    "text/tab-separated-values; charset=UTF-8",
                )],
                "1\n".into(),
            )
        }
        let app = Router::new().route("/", post(record)).with_state(received);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/")
    }

    fn request(query: &str) -> QueryRequest {
        QueryRequest {
            language: QueryLanguage::SQL(SqlDialect::ClickhouseSQL),
            query: query.into(),
            eval: QueryEval::Instant { time_ms: 0 },
        }
    }

    /// From ASAPQuery's `test_execute_query_success` and
    /// `test_url_with_database_param`: the query is posted as the body to
    /// `/?database=<database>` with the client's headers, and the server's
    /// status, `Content-Type` and body come back unchanged, an exception
    /// included (`test_execute_query_error_response`).
    #[tokio::test]
    async fn posts_the_query_and_returns_the_response_unchanged() {
        let received = Received::default();
        let fallback = ClickHouseHttpFallback::new(
            &server(received.clone()).await,
            "test db",
            Duration::from_secs(5),
        );
        let headers = [("Authorization".to_string(), "Basic dTpw".to_string())];
        let response = fallback
            .execute(&request("SELECT 1"), &headers)
            .await
            .unwrap();
        assert_eq!(
            response,
            FallbackResponse {
                status_code: 200,
                content_type: "text/tab-separated-values; charset=UTF-8".into(),
                body: b"1\n".to_vec(),
            }
        );
        let error = fallback
            .execute(&request("SELECT * FROM missing"), &[])
            .await
            .unwrap();
        assert_eq!(error.status_code, 404);
        assert_eq!(
            error.body,
            b"Code: 60. DB::Exception: Table default.missing does not exist."
        );
        let received = received.lock().unwrap();
        assert_eq!(
            received[0],
            (
                "/?database=test+db".to_string(),
                Some("Basic dTpw".to_string()),
                "SELECT 1".to_string()
            )
        );
        assert_eq!(received[1].1, None);
    }

    /// A range query (ASAPQuery's `test_range_query_not_implemented`) or a
    /// query in another language is not sent; an unreachable server
    /// (`test_clickhouse_server_unreachable`) is an error.
    #[tokio::test]
    async fn refuses_what_clickhouse_cannot_answer_and_reports_an_unreachable_server() {
        let fallback =
            ClickHouseHttpFallback::new("http://127.0.0.1:1", "default", Duration::from_secs(5));
        let range = QueryRequest {
            eval: QueryEval::Range {
                start_ms: 0,
                end_ms: 60_000,
                step_ms: 15_000,
            },
            ..request("SELECT 1")
        };
        assert_eq!(
            fallback.execute(&range, &[]).await,
            Err(FallbackError::new("ClickHouse has no range queries"))
        );
        let promql = QueryRequest {
            language: QueryLanguage::PromQL,
            ..request("up")
        };
        assert!(fallback.execute(&promql, &[]).await.is_err());
        let error = fallback
            .execute(&request("SELECT 1"), &[])
            .await
            .unwrap_err();
        assert!(
            error.message.starts_with("forwarding to ClickHouse: "),
            "{error}"
        );
    }
}
