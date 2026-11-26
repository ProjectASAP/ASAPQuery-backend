//! The client of an Elasticsearch server that answers the queries the data
//! plane does not, as ASAPQuery's `ElasticHttpFallback` sends them.

use std::time::Duration;

use async_trait::async_trait;
use common::{
    FallbackClient, FallbackError, FallbackResponse, QueryEval, QueryLanguage, QueryRequest,
    SqlDialect,
};
use reqwest::header::{self, HeaderName, HeaderValue};

/// Rows per page an Elasticsearch SQL query asks for, as ASAPQuery asks.
const SQL_FETCH_SIZE: u64 = 1000;

/// Sends queries to one Elasticsearch server.
pub struct ElasticHttpFallback {
    /// Without a trailing `/`.
    base_url: String,
    /// The index (or index pattern) a Query DSL search reads; empty: every
    /// index (`/_search`).
    index: String,
    client: reqwest::Client,
}

impl ElasticHttpFallback {
    /// A client of the server at `base_url` whose Query DSL searches read
    /// `index`, each request bounded by `timeout`, from sending it to
    /// reading the whole response.
    pub fn new(base_url: &str, index: &str, timeout: Duration) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            index: index.to_string(),
            client: reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .expect("reqwest client with only a timeout set"),
        }
    }
}

#[async_trait]
impl FallbackClient for ElasticHttpFallback {
    /// Elasticsearch SQL: `POST /_sql` with `{"query", "fetch_size"}`, the
    /// query without a trailing `;`. Query DSL: `POST /<index>/_search`
    /// with the query as the body. Instant queries only: Elasticsearch has
    /// no range queries. Header pairs that are not valid HTTP headers are
    /// skipped.
    async fn execute(
        &self,
        request: &QueryRequest,
        headers: &[(String, String)],
    ) -> Result<FallbackResponse, FallbackError> {
        if let QueryEval::Range { .. } = request.eval {
            return Err(FallbackError::new("Elasticsearch has no range queries"));
        }
        let (url, body) = match &request.language {
            QueryLanguage::SQL(SqlDialect::ElasticSQL) => {
                let body = serde_json::json!({
                    "query": request.query.trim().trim_end_matches(';'),
                    "fetch_size": SQL_FETCH_SIZE,
                });
                (format!("{}/_sql", self.base_url), body.to_string())
            }
            QueryLanguage::ElasticDSL if self.index.is_empty() => {
                (format!("{}/_search", self.base_url), request.query.clone())
            }
            QueryLanguage::ElasticDSL => (
                format!("{}/{}/_search", self.base_url, self.index),
                request.query.clone(),
            ),
            language => {
                let message =
                    format!("Elasticsearch answers its SQL and Query DSL, not {language:?}");
                return Err(FallbackError::new(message));
            }
        };
        let mut upstream = self
            .client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json");
        for (name, value) in headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::try_from(value.as_str()),
            ) {
                upstream = upstream.header(name, value);
            }
        }
        let upstream =
            upstream.body(body).send().await.map_err(|error| {
                FallbackError::new(format!("forwarding to Elasticsearch: {error}"))
            })?;
        let status_code = upstream.status().as_u16();
        // A server that names no type sent bytes of unknown type (RFC 9110
        // section 8.3).
        let content_type = upstream
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let body = upstream.bytes().await.map_err(|error| {
            FallbackError::new(format!("reading Elasticsearch response: {error}"))
        })?;
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
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::Router;

    use super::*;

    /// What the mock server received: path, `Content-Type`,
    /// `Authorization`, body.
    type Received = Arc<Mutex<Vec<(String, String, Option<String>, String)>>>;

    /// An Elasticsearch server that records each request and answers 200
    /// with a JSON body naming its path; its base URL.
    async fn server(received: Received) -> String {
        async fn record(
            State(received): State<Received>,
            OriginalUri(uri): OriginalUri,
            headers: HeaderMap,
            body: String,
        ) -> ([(header::HeaderName, &'static str); 1], String) {
            let header = |name| {
                headers
                    .get(name)
                    .map(|v: &HeaderValue| v.to_str().unwrap().to_string())
            };
            received.lock().unwrap().push((
                uri.to_string(),
                header(header::CONTENT_TYPE).unwrap_or_default(),
                header(header::AUTHORIZATION),
                body,
            ));
            (
                [(header::CONTENT_TYPE, "application/json")],
                format!("{{\"path\":\"{}\"}}", uri.path()),
            )
        }
        let app = Router::new()
            .route("/_sql", post(record))
            .route("/_search", post(record))
            .route("/:index/_search", post(record))
            .with_state(received);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/")
    }

    fn request(language: QueryLanguage, query: &str) -> QueryRequest {
        QueryRequest {
            language,
            query: query.into(),
            eval: QueryEval::Instant { time_ms: 0 },
        }
    }

    /// An SQL query is posted to `/_sql` wrapped with ASAPQuery's fetch
    /// size and without its `;`; a Query DSL query to the index's
    /// `_search` (or `/_search` with no index) as sent; both as JSON with
    /// the client's `Authorization`. The server's response comes back
    /// unchanged.
    #[tokio::test]
    async fn posts_sql_and_query_dsl_and_returns_the_response_unchanged() {
        let received = Received::default();
        let url = server(received.clone()).await;
        let fallback = ElasticHttpFallback::new(&url, "logs-*", Duration::from_secs(5));
        let headers = [("Authorization".to_string(), "ApiKey k".to_string())];
        let sql = request(
            QueryLanguage::SQL(SqlDialect::ElasticSQL),
            " SELECT COUNT(*) FROM logs; ",
        );
        let response = fallback.execute(&sql, &headers).await.unwrap();
        assert_eq!(
            response,
            FallbackResponse {
                status_code: 200,
                content_type: "application/json".into(),
                body: br#"{"path":"/_sql"}"#.to_vec(),
            }
        );
        let search = r#"{"query": {"match_all": {}}}"#;
        fallback
            .execute(&request(QueryLanguage::ElasticDSL, search), &[])
            .await
            .unwrap();
        ElasticHttpFallback::new(&url, "", Duration::from_secs(5))
            .execute(&request(QueryLanguage::ElasticDSL, search), &[])
            .await
            .unwrap();
        let received = received.lock().unwrap();
        let json = "application/json".to_string();
        let (path, content_type, authorization, body) = &received[0];
        assert_eq!(
            (path.as_str(), content_type, authorization.as_deref()),
            ("/_sql", &json, Some("ApiKey k"))
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({"query": "SELECT COUNT(*) FROM logs", "fetch_size": 1000})
        );
        assert_eq!(
            received[1],
            (
                "/logs-*/_search".to_string(),
                json.clone(),
                None,
                search.to_string()
            )
        );
        assert_eq!(received[2].0, "/_search");
    }

    /// A range query or a query in another language is not sent; an
    /// unreachable server is an error.
    #[tokio::test]
    async fn refuses_what_elasticsearch_cannot_answer_and_reports_an_unreachable_server() {
        let fallback = ElasticHttpFallback::new("http://127.0.0.1:1", "", Duration::from_secs(5));
        let range = QueryRequest {
            eval: QueryEval::Range {
                start_ms: 0,
                end_ms: 60_000,
                step_ms: 15_000,
            },
            ..request(QueryLanguage::ElasticDSL, "{}")
        };
        assert_eq!(
            fallback.execute(&range, &[]).await,
            Err(FallbackError::new("Elasticsearch has no range queries"))
        );
        let clickhouse = request(QueryLanguage::SQL(SqlDialect::ClickhouseSQL), "SELECT 1");
        assert!(fallback.execute(&clickhouse, &[]).await.is_err());
        let error = fallback
            .execute(&request(QueryLanguage::ElasticDSL, "{}"), &[])
            .await
            .unwrap_err();
        assert!(
            error.message.starts_with("forwarding to Elasticsearch: "),
            "{error}"
        );
    }
}
