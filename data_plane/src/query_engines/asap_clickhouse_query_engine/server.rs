use super::{
    clickhouse_result_adapter::raw_response,
    fallback::{ClickHouseExactBackend, ClickHouseRawResponse},
    request::ClickHouseQueryRequest,
};
use async_trait::async_trait;
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
    accelerator: Arc<dyn ClickHouseAccelerator>,
}

/// The SQL-language boundary for accelerated execution.
///
/// Implementations own plan-catalog lookup, planning/binding, DAG execution,
/// coverage validation, and ClickHouse result encoding. Every fail-closed
/// outcome is routed to the exact ClickHouse backend by this HTTP adapter.
#[async_trait]
pub trait ClickHouseAccelerator: Send + Sync {
    async fn execute(&self, request: &ClickHouseQueryRequest) -> ClickHouseAccelerationOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseAccelerationFallback {
    CatalogMiss,
    Planning(String),
    Execution(String),
    IncompleteCoverage,
    UnsupportedFormat(String),
}

impl ClickHouseAccelerationFallback {
    pub fn stage(&self) -> &'static str {
        match self {
            Self::CatalogMiss => "publication",
            Self::Planning(_) => "planner",
            Self::Execution(_) => "executor",
            Self::IncompleteCoverage => "validator",
            Self::UnsupportedFormat(_) => "adapter",
        }
    }

    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::CatalogMiss => "catalog_miss",
            Self::Planning(_) => "planning_failed",
            Self::Execution(_) => "execution_failed",
            Self::IncompleteCoverage => "incomplete_coverage",
            Self::UnsupportedFormat(_) => "unsupported_format",
        }
    }
}

#[derive(Debug)]
pub enum ClickHouseAccelerationOutcome {
    Accelerated(ClickHouseRawResponse),
    Fallback(ClickHouseAccelerationFallback),
}

struct DisabledAccelerator;

#[async_trait]
impl ClickHouseAccelerator for DisabledAccelerator {
    async fn execute(&self, _request: &ClickHouseQueryRequest) -> ClickHouseAccelerationOutcome {
        ClickHouseAccelerationOutcome::Fallback(ClickHouseAccelerationFallback::CatalogMiss)
    }
}

pub struct ClickHouseHttpServer {
    pub listen_address: String,
    pub fallback: Arc<dyn ClickHouseExactBackend>,
}

impl ClickHouseHttpServer {
    /// Builds the original exact-only router.
    pub fn router(fallback: Arc<dyn ClickHouseExactBackend>) -> Router {
        Self::router_with_accelerator(fallback, Arc::new(DisabledAccelerator))
    }

    /// Builds a router that attempts accelerated SQL execution before exact
    /// fallback. The accelerator must return a fully encoded ClickHouse HTTP
    /// response so format conversion remains at the SQL boundary.
    pub fn router_with_accelerator(
        fallback: Arc<dyn ClickHouseExactBackend>,
        accelerator: Arc<dyn ClickHouseAccelerator>,
    ) -> Router {
        let state = ServerState {
            fallback,
            accelerator,
        };
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

    pub async fn run_with_accelerator(
        self,
        accelerator: Arc<dyn ClickHouseAccelerator>,
    ) -> Result<(), std::io::Error> {
        let app = Self::router_with_accelerator(self.fallback, accelerator);
        let listener = TcpListener::bind(&self.listen_address).await?;
        axum::serve(listener, app).await
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
    execute_or_fallback(&state, &request).await
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
    execute_or_fallback(&state, &request).await
}

async fn execute_or_fallback(state: &ServerState, request: &ClickHouseQueryRequest) -> Response {
    match state.accelerator.execute(request).await {
        ClickHouseAccelerationOutcome::Accelerated(response) => {
            let mut response = raw_response(response);
            response
                .headers_mut()
                .entry("x-asap-execution")
                .or_insert(axum::http::HeaderValue::from_static("warm"));
            response
                .headers_mut()
                .entry("x-asap-execution-detail")
                .or_insert(axum::http::HeaderValue::from_static("asap"));
            response
        }
        ClickHouseAccelerationOutcome::Fallback(reason) => {
            tracing::info!(
                failure_stage = reason.stage(),
                failure_reason = reason.reason_code(),
                failure_detail = ?reason,
                "ClickHouse acceleration routed to exact fallback"
            );
            let stage = reason.stage();
            let reason = reason.reason_code();
            return match state.fallback.execute(request).await {
                Ok(v) => {
                    let mut response = raw_response(v);
                    for (name, value) in [
                        ("x-asap-execution", "exact_fallback"),
                        ("x-asap-execution-detail", reason),
                        ("x-asap-failure-stage", stage),
                        ("x-asap-failure-reason", reason),
                    ] {
                        response.headers_mut().insert(
                            axum::http::HeaderName::from_static(name),
                            axum::http::HeaderValue::from_static(value),
                        );
                    }
                    response
                }
                Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
            };
        }
    }
}

async fn ping(State(state): State<ServerState>) -> Response {
    match state.fallback.ping().await {
        Ok(v) => raw_response(v),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
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

    struct FixedAccelerator {
        outcome: Mutex<Option<ClickHouseAccelerationOutcome>>,
    }

    #[async_trait]
    impl ClickHouseAccelerator for FixedAccelerator {
        async fn execute(
            &self,
            _request: &ClickHouseQueryRequest,
        ) -> ClickHouseAccelerationOutcome {
            self.outcome.lock().unwrap().take().unwrap()
        }
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

    #[tokio::test]
    async fn returns_accelerated_response_without_calling_exact_backend() {
        let fallback = Arc::new(RecordingFallback::default());
        let accelerator = Arc::new(FixedAccelerator {
            outcome: Mutex::new(Some(ClickHouseAccelerationOutcome::Accelerated(
                ClickHouseRawResponse {
                    status: StatusCode::OK,
                    headers: HeaderMap::new(),
                    body: Bytes::from_static(b"accelerated\n"),
                },
            ))),
        });
        let response = ClickHouseHttpServer::router_with_accelerator(fallback.clone(), accelerator)
            .oneshot(
                axum::http::Request::builder()
                    .uri("/?query=SELECT%201")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers()["x-asap-execution"], "warm");
        assert_eq!(response.headers()["x-asap-execution-detail"], "asap");

        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "accelerated\n"
        );
        assert!(fallback.sql.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn every_acceleration_failure_routes_to_exact_backend() {
        let cases = [
            (ClickHouseAccelerationFallback::CatalogMiss, "catalog_miss"),
            (
                ClickHouseAccelerationFallback::Planning("cannot bind".into()),
                "planning_failed",
            ),
            (
                ClickHouseAccelerationFallback::Execution("unsupported operator".into()),
                "execution_failed",
            ),
            (
                ClickHouseAccelerationFallback::IncompleteCoverage,
                "incomplete_coverage",
            ),
            (
                ClickHouseAccelerationFallback::UnsupportedFormat("Native".into()),
                "unsupported_format",
            ),
        ];

        for (reason, expected_detail) in cases {
            let fallback = Arc::new(RecordingFallback::default());
            let accelerator = Arc::new(FixedAccelerator {
                outcome: Mutex::new(Some(ClickHouseAccelerationOutcome::Fallback(reason))),
            });
            let response =
                ClickHouseHttpServer::router_with_accelerator(fallback.clone(), accelerator)
                    .oneshot(
                        axum::http::Request::builder()
                            .uri("/?query=SELECT%201")
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();

            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-asap-execution"], "exact_fallback");
            assert_eq!(
                response.headers()["x-asap-execution-detail"],
                expected_detail
            );
            assert_eq!(response.headers()["x-asap-failure-reason"], expected_detail);
            assert_eq!(*fallback.sql.lock().unwrap(), vec!["SELECT 1"]);
        }
    }
}
