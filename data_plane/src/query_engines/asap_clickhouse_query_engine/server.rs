use super::{
    accelerator::{CatalogClickHouseAccelerator, ClickHousePlanBundle},
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
    Json, Router,
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
    publisher: Option<Arc<CatalogClickHouseAccelerator>>,
    publication_token: Option<Arc<str>>,
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
            publisher: None,
            publication_token: None,
        };
        Router::new()
            .route("/", get(query_get).post(query_post))
            .route("/ping", get(ping))
            .with_state(state)
    }

    /// Adds the SQL plan publication surface to the independent ClickHouse
    /// listener. The PromQL and physical-plan routes are not involved.
    pub fn router_with_catalog(
        fallback: Arc<dyn ClickHouseExactBackend>,
        accelerator: Arc<CatalogClickHouseAccelerator>,
    ) -> Router {
        Self::router_with_catalog_token(fallback, accelerator, None)
    }

    pub fn router_with_catalog_token(
        fallback: Arc<dyn ClickHouseExactBackend>,
        accelerator: Arc<CatalogClickHouseAccelerator>,
        publication_token: Option<String>,
    ) -> Router {
        let state = ServerState {
            fallback,
            accelerator: accelerator.clone(),
            publisher: Some(accelerator),
            publication_token: publication_token.map(Arc::from),
        };
        Router::new()
            .route("/", get(query_get).post(query_post))
            .route("/ping", get(ping))
            .route(
                "/api/v1/clickhouse-plan/stage",
                axum::routing::post(stage_plan),
            )
            .route(
                "/api/v1/clickhouse-plan/activate",
                axum::routing::post(activate_plan),
            )
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

    pub async fn run_with_catalog(
        self,
        accelerator: Arc<CatalogClickHouseAccelerator>,
        publication_token: Option<String>,
    ) -> Result<(), std::io::Error> {
        let app = Self::router_with_catalog_token(self.fallback, accelerator, publication_token);
        let listener = TcpListener::bind(&self.listen_address).await?;
        axum::serve(listener, app).await
    }
}

#[derive(serde::Deserialize)]
struct ActivatePlanRequest {
    plan_id: u64,
    plan_version: u64,
}

async fn stage_plan(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(bundle): Json<ClickHousePlanBundle>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(publisher) = state.publisher else {
        return (
            StatusCode::NOT_FOUND,
            "ClickHouse plan publication is disabled",
        )
            .into_response();
    };
    match publisher.stage_bundle(bundle) {
        Ok(ack) => Json(ack).into_response(),
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

async fn activate_plan(
    State(state): State<ServerState>,
    headers: HeaderMap,
    Json(request): Json<ActivatePlanRequest>,
) -> Response {
    if !authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(publisher) = state.publisher else {
        return (
            StatusCode::NOT_FOUND,
            "ClickHouse plan publication is disabled",
        )
            .into_response();
    };
    match publisher.activate(request.plan_id, request.plan_version) {
        Ok(ack) => Json(ack).into_response(),
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

fn authorized(state: &ServerState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.publication_token.as_deref() else {
        return true;
    };
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| provided.as_bytes() == expected.as_bytes())
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

        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "accelerated\n"
        );
        assert!(fallback.sql.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn every_acceleration_failure_routes_to_exact_backend() {
        let cases = [
            ClickHouseAccelerationFallback::CatalogMiss,
            ClickHouseAccelerationFallback::Planning("cannot bind".into()),
            ClickHouseAccelerationFallback::Execution("unsupported operator".into()),
            ClickHouseAccelerationFallback::IncompleteCoverage,
            ClickHouseAccelerationFallback::UnsupportedFormat("Native".into()),
        ];

        for reason in cases {
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
            assert_eq!(*fallback.sql.lock().unwrap(), vec!["SELECT 1"]);
        }
    }

    #[tokio::test]
    async fn remotely_stages_and_activates_an_atomic_sql_generation() {
        use asap_types::{
            summary_catalog::SummaryCatalog, AggregationType, KeyByLabelNames,
            PrecomputeMaterialization, WindowKind,
        };
        use planner_types::types::AccuracyTarget;

        let materialization = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "requests".into(),
            None,
            None,
            None,
        );
        let bundle = ClickHousePlanBundle {
            sds: SummaryCatalog::from_materializations(19, 4, &[materialization]).unwrap(),
            tables: HashMap::new(),
            accuracy: AccuracyTarget::Exact,
            plans: Vec::new(),
        };
        let fallback = Arc::new(RecordingFallback::default());
        let accelerator = Arc::new(CatalogClickHouseAccelerator::empty(Arc::new(
            crate::storage_engines::sketch_db::index::SketchStore::new(),
        )));
        let app = ClickHouseHttpServer::router_with_catalog_token(
            fallback,
            accelerator,
            Some("publish-secret".into()),
        );

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/clickhouse-plan/stage")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(&bundle).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let staged = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/clickhouse-plan/stage")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer publish-secret")
                    .body(axum::body::Body::from(serde_json::to_vec(&bundle).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(staged.status(), StatusCode::OK);
        let staged: serde_json::Value =
            serde_json::from_slice(&staged.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(staged["phase"], "staged");
        assert_eq!(staged["plan_version"], 4);

        let activated = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/clickhouse-plan/activate")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer publish-secret")
                    .body(axum::body::Body::from(r#"{"plan_id":19,"plan_version":4}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(activated.status(), StatusCode::OK);
        let activated: serde_json::Value =
            serde_json::from_slice(&activated.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(activated["phase"], "active");
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
    if let ClickHouseAccelerationOutcome::Accelerated(response) =
        state.accelerator.execute(request).await
    {
        return raw_response(response);
    }
    match state.fallback.execute(request).await {
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
