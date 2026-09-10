use super::{
    clickhouse_result_adapter::raw_response, fallback::ClickHouseExactBackend,
    request::ClickHouseQueryRequest,
};
use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::net::TcpListener;

#[derive(Clone)]
struct ServerState {
    fallback: Arc<dyn ClickHouseExactBackend>,
}

pub struct ClickHouseHttpServer {
    pub listen_address: String,
    pub fallback: Arc<dyn ClickHouseExactBackend>,
}

impl ClickHouseHttpServer {
    pub fn router(fallback: Arc<dyn ClickHouseExactBackend>) -> Router {
        let state = ServerState { fallback };
        Router::new()
            .route("/", get(query_get).post(query_post))
            .route("/ping", get(ping))
            .with_state(state)
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        let app = Self::router(self.fallback);
        let listener = TcpListener::bind(&self.listen_address).await?;
        axum::serve(listener, app).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engines::asap_clickhouse_query_engine::fallback::{
        ClickHouseFallbackError, ClickHouseRawResponse,
    };
    use async_trait::async_trait;
    use http_body_util::BodyExt;
    use std::sync::Mutex;
    use tower::ServiceExt;

    #[derive(Default)]
    struct RecordingFallback {
        sql: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ClickHouseExactBackend for RecordingFallback {
        async fn execute(
            &self,
            request: &ClickHouseQueryRequest,
        ) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
            self.sql.lock().unwrap().push(request.sql.clone());
            Ok(ClickHouseRawResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: Bytes::from_static(b"1\n"),
            })
        }
        async fn ping(&self) -> Result<ClickHouseRawResponse, ClickHouseFallbackError> {
            Ok(ClickHouseRawResponse {
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: Bytes::from_static(b"Ok.\n"),
            })
        }
    }

    #[tokio::test]
    async fn forwards_get_raw_post_and_ping_without_promql_types() {
        let fallback = Arc::new(RecordingFallback::default());
        let app = ClickHouseHttpServer::router(fallback.clone());
        let get_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/?query=SELECT%201")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let post_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/")
                    .body(axum::body::Body::from("SELECT 2"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            post_response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
            "1\n"
        );
        let ping = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/ping")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            ping.into_body().collect().await.unwrap().to_bytes(),
            "Ok.\n"
        );
        assert_eq!(*fallback.sql.lock().unwrap(), vec!["SELECT 1", "SELECT 2"]);
    }
}

fn request(
    method: Method,
    params: HashMap<String, String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<ClickHouseQueryRequest, Response> {
    let parameters: BTreeMap<_, _> = params.into_iter().collect();
    let sql = parameters
        .get("query")
        .cloned()
        .or_else(|| String::from_utf8(body.to_vec()).ok())
        .unwrap_or_default();
    if sql.trim().is_empty() {
        return Err((StatusCode::BAD_REQUEST, "missing query").into_response());
    }
    Ok(ClickHouseQueryRequest {
        method,
        sql,
        body,
        parameters,
        headers,
    })
}

async fn query_get(
    State(state): State<ServerState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let request = match request(Method::GET, params, headers, Bytes::new()) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match state.fallback.execute(&request).await {
        Ok(v) => raw_response(v),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn query_post(
    State(state): State<ServerState>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match request(Method::POST, params, headers, body) {
        Ok(v) => v,
        Err(e) => return e,
    };
    match state.fallback.execute(&request).await {
        Ok(v) => raw_response(v),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn ping(State(state): State<ServerState>) -> Response {
    match state.fallback.ping().await {
        Ok(v) => raw_response(v),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}
