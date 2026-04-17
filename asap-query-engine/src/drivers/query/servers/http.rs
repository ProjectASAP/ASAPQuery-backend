use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use axum::{
    body::Bytes,
    extract::{Form, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::drivers::query::adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
use crate::engines::SimpleEngine;
use crate::query_tracker::QueryTracker;
use crate::stores::Store;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub adapter_config: AdapterConfig,
}

#[derive(Clone)]
pub struct HttpServer {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
    query_tracker: Option<Arc<QueryTracker>>,
    /// Hot-reloadable `StreamingConfig` source. `None` when hot-reload
    /// is not wired up by the caller (unit tests, legacy binaries).
    hot_reload_config: Option<crate::data_model::HotReloadStreamingConfig>,
    /// Per-`agg_id` schema registry (sketch DB §6). `None` when the
    /// caller hasn't wired the precompute engine into the HTTP
    /// server — in that case the `POST /api/v1/streaming-config`
    /// handler still swaps the config but doesn't drive schema
    /// lifecycle transitions.
    schemas: Option<Arc<crate::stores::sketch_db::SchemaRegistry>>,
}

#[derive(Clone)]
struct AppState {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
    query_tracker: Option<Arc<QueryTracker>>,
    adapter: Arc<dyn HttpProtocolAdapter>,
    fallback: Option<Arc<dyn crate::drivers::query::fallback::FallbackClient>>,
    hot_reload_config: Option<crate::data_model::HotReloadStreamingConfig>,
    /// Per-`agg_id` schema registry (sketch DB §6). Phase 2b wires
    /// `POST /api/v1/streaming-config` to call `schemas.reconcile()`
    /// on every swap so schema lifecycle transitions happen
    /// event-driven instead of on every ingest batch. When absent,
    /// the swap handler leaves the registry alone (legacy
    /// per-batch reconcile still works).
    schemas: Option<Arc<crate::stores::sketch_db::SchemaRegistry>>,
}

impl HttpServer {
    pub fn new(
        config: HttpServerConfig,
        query_engine: Arc<SimpleEngine>,
        store: Arc<dyn Store>,
        query_tracker: Option<Arc<QueryTracker>>,
    ) -> Self {
        Self {
            config,
            query_engine,
            store,
            query_tracker,
            hot_reload_config: None,
            schemas: None,
        }
    }

    /// Attach a `HotReloadStreamingConfig` handle so the
    /// `GET/POST /api/v1/streaming-config` endpoints can read and
    /// swap the currently active config. Without this handle the
    /// endpoints return `503 Service Unavailable`.
    pub fn with_hot_reload_config(
        mut self,
        handle: crate::data_model::HotReloadStreamingConfig,
    ) -> Self {
        self.hot_reload_config = Some(handle);
        self
    }

    /// Attach the `SchemaRegistry` that the precompute engine's
    /// `IngestState` also holds. When attached, the
    /// `POST /api/v1/streaming-config` handler calls
    /// `schemas.reconcile(new_config)` after the ArcSwap store, so
    /// schema lifecycle transitions (§6 of the sketch DB design) are
    /// event-driven rather than per-ingest-batch. Without the handle
    /// the registry still gets reconciled on the next ingest batch,
    /// just less promptly.
    pub fn with_schemas(mut self, schemas: Arc<crate::stores::sketch_db::SchemaRegistry>) -> Self {
        self.schemas = Some(schemas);
        self
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();
        info!(
            "Adapter '{}' configured for endpoint: {}",
            adapter.adapter_name(),
            query_endpoint
        );
        info!("Runtime info endpoint: {}", runtime_info_path);

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            store: self.store,
            query_tracker: self.query_tracker,
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            schemas: self.schemas.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route(runtime_info_path, post(handle_runtime_info))
            .route("/metrics", get(handle_metrics))
            // Controller integration endpoints
            .route("/api/v1/precompute", post(handle_precompute_job))
            .route("/api/v1/health", get(handle_health))
            .route("/api/v1/store/metrics", get(handle_store_metrics))
            .route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
            .with_state(app_state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        info!("HTTP server listening on port {}", self.config.port);

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start server for testing on a random available port
    /// Returns the actual port number used
    #[cfg(test)]
    pub async fn start_test_server(&self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            store: self.store.clone(),
            query_tracker: self.query_tracker.clone(),
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
            hot_reload_config: self.hot_reload_config.clone(),
            schemas: self.schemas.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route(
                "/api/v1/streaming-config",
                get(handle_get_streaming_config).post(handle_post_streaming_config),
            )
            .with_state(app_state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let actual_port = listener.local_addr()?.port();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(actual_port)
    }
}

/// Core query execution logic shared between GET and POST handlers
async fn process_query_request(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled");
        if let Some(fallback) = &state.fallback {
            debug!("Forwarding to fallback due to disabled handling");
            return match fallback
                .execute_query_with_headers(parsed_request, headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        } else {
            debug!("Returning error - both handling and forwarding disabled");
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ProtocolError(
                    "Query handling is disabled".to_string(),
                ))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // Record query for passive auto-discovery (if tracker is enabled)
    if let Some(tracker) = &state.query_tracker {
        tracker.record_instant(&parsed_request.query, parsed_request.time);
    }

    // Step 2: Execute query with engine (using parsed request)
    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.handle_query with query='{}' and time={}",
        parsed_request.query, parsed_request.time
    );
    match state
        .query_engine
        .handle_query(parsed_request.query.clone(), parsed_request.time)
    {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!("=== QUERY ENGINE SUCCESS ===");
            debug!(
                "Query engine execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            debug!("Query output labels: {:?}", query_output_labels);
            debug!("Query result: {:?}", query_result);

            // Step 3: Format success response using adapter
            // (Adapter handles protocol-specific formatting, e.g., convert_query_result_to_prometheus)
            use crate::drivers::query::adapters::QueryExecutionResult;
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING SUCCESS RESPONSE ===");

            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            let total_duration = start_time.elapsed();
            debug!("=== QUERY ENGINE RETURNED NONE ===");
            debug!(
                "Request failed after: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Step 4: Handle unsupported query using fallback client
            if let Some(fallback) = &state.fallback {
                debug!("Query not supported locally, forwarding to fallback");
                // Fallback client handles the HTTP call and returns formatted response
                match fallback
                    .execute_query_with_headers(parsed_request, headers)
                    .await
                {
                    Ok(response) => response.into_response(),
                    Err(status) => status.into_response(),
                }
            } else {
                debug!("Query not supported and forwarding disabled, returning error");
                // Adapter formats the unsupported query error for its protocol
                match state.adapter.format_unsupported_query_response().await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                }
            }
        }
    }
}

async fn handle_instant_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    let parsed_request = match state.adapter.parse_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed - query: '{}', time: {}",
                req.query, req.time
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_query_request(&state, &parsed_request, start_time, HashMap::new()).await
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    }

    // Check content type to determine how to parse the body
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    debug!("Content-Type: {}", content_type);

    let parsed_request = if content_type.contains("application/json") {
        // Handle JSON POST (Elasticsearch)
        debug!("Parsing as JSON POST request");
        match state.adapter.parse_json_post_request(body).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed JSON POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse JSON POST request: {:?}", parse_error);
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    } else {
        // Handle form-encoded POST
        debug!("Parsing as form-encoded POST request");

        // Parse the body as form data
        let body_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(e) => {
                debug!("Failed to parse body as UTF-8: {}", e);
                use crate::drivers::query::adapters::AdapterError;
                return match state
                    .adapter
                    .format_error_response(&AdapterError::ParseError(format!(
                        "Invalid UTF-8 in request body: {}",
                        e
                    )))
                    .await
                {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        };

        // Parse form parameters
        let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
            .into_owned()
            .collect();
        debug!("Form params extracted: {:?}", params);

        // Use adapter to parse POST request (handles form-encoded parameters)
        match state.adapter.parse_post_request(Form(params)).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse POST request: {:?}", parse_error);
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    };

    let result =
        process_query_request(&state, &parsed_request, start_time, forwarding_headers).await;

    let total_duration = start_time.elapsed();
    debug!(
        "Total POST request processing took: {:.2}ms",
        total_duration.as_secs_f64() * 1000.0
    );

    result
}

async fn handle_runtime_info(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    debug!("Delegating runtime info request to adapter");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            debug!("Found Authorization header for runtime info: {}", auth_str);
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    } else {
        debug!("No Authorization header found in runtime info request");
    }

    // Delegate to adapter for protocol-specific handling
    state
        .adapter
        .handle_runtime_info_with_headers(state.store.clone(), forwarding_headers)
        .await
}

// ============================================================
// Metrics Handler
// ============================================================

async fn handle_metrics() -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    prometheus::Encoder::encode(&encoder, &metric_families, &mut buffer)
        .unwrap_or_else(|e| tracing::error!("Failed to encode metrics: {}", e));
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        buffer,
    )
}

// ============================================================
// Range Query Handlers
// ============================================================

/// Core range query execution logic shared between GET and POST handlers
async fn process_range_query_request(
    state: &AppState,
    parsed_request: &ParsedRangeQueryRequest,
    start_time: Instant,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled for range query");
        // For now, return error - fallback for range queries can be added later
        use crate::drivers::query::adapters::AdapterError;
        return match state
            .adapter
            .format_error_response(&AdapterError::ProtocolError(
                "Range query handling is disabled".to_string(),
            ))
            .await
        {
            Ok(json) => json.into_response(),
            Err(status) => status.into_response(),
        };
    }

    // Record query for passive auto-discovery (if tracker is enabled)
    if let Some(tracker) = &state.query_tracker {
        tracker.record_range(
            &parsed_request.query,
            parsed_request.start,
            parsed_request.end,
            parsed_request.step,
        );
    }

    // Execute range query with engine
    let query_start_time = Instant::now();
    debug!(
        "Executing range query: '{}' from {} to {} step {}",
        parsed_request.query, parsed_request.start, parsed_request.end, parsed_request.step
    );

    match state.query_engine.handle_range_query_promql(
        parsed_request.query.clone(),
        parsed_request.start,
        parsed_request.end,
        parsed_request.step,
    ) {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "Range query execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );

            let total_duration = start_time.elapsed();
            debug!(
                "Total range query processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Format range success response
            match state
                .adapter
                .format_range_success_response(&query_result, &query_output_labels)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            debug!("Range query returned None - query not supported");
            match state.adapter.format_unsupported_query_response().await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
    }
}

async fn handle_range_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    let parsed_request = match state.adapter.parse_range_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range query - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range query request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_range_query_request(&state, &parsed_request, start_time).await
}

async fn handle_range_query_post(State(state): State<AppState>, body: Bytes) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY POST REQUEST ===");

    // Parse the body as form data
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            debug!("Failed to parse body as UTF-8: {}", e);
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ParseError(format!(
                    "Invalid UTF-8 in request body: {}",
                    e
                )))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    // Parse form parameters
    let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
        .into_owned()
        .collect();
    debug!("Form params extracted: {:?}", params);

    let parsed_request = match state.adapter.parse_range_post_request(Form(params)).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range POST - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range POST request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_range_query_request(&state, &parsed_request, start_time).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{HotReloadStreamingConfig, InferenceConfig, StreamingConfig};
    use crate::engines::SimpleEngine;
    use crate::stores::simple_map_store::SimpleMapStore;
    use reqwest::Client;
    use std::sync::Arc;

    async fn setup_test_server() -> u16 {
        setup_test_server_with_hot_reload(None).await
    }

    async fn setup_test_server_with_hot_reload(
        hot_reload: Option<HotReloadStreamingConfig>,
    ) -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(), // Unused for this test
            false,                               // forward_unsupported_queries
        );

        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };

        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            // None,
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));

        let mut server = HttpServer::new(config, query_engine, store, None);
        if let Some(handle) = hot_reload {
            server = server.with_hot_reload_config(handle);
        }
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_get_endpoint_plus_symbol_decoding() {
        // Enable debug logging for this test
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test query with + symbols that should become spaces
        let test_query = "quantile by (instance, job) (0.95, fake_metric_total)";

        println!("Sending query: {test_query}");

        let response = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", test_query)])
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        // The debug logs should show what query was actually parsed
        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_endpoint_form_decoding() {
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test the same query via POST with form encoding
        let test_query = "quantile+by+(instance,+job)+(0.95,+fake_metric_total)";

        println!("Sending POST with form data: {test_query}");

        let response = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("query={test_query}&time=1758161478.205"))
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    // ── StreamingConfig hot-reload (PR E) ────────────────────────────────

    /// POST a YAML streaming-config and verify the active state via
    /// GET reflects the swap. Covers the full round-trip through
    /// `HttpServer::with_hot_reload_config`, the POST parse+swap, and
    /// the GET snapshot emission.
    #[tokio::test]
    async fn test_streaming_config_hot_reload_round_trip() {
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload.clone())).await;
        let client = Client::new();

        // Initial GET: empty config, 0 entries.
        let initial = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .expect("GET failed");
        assert!(initial.status().is_success());
        let initial_body: serde_json::Value = initial.json().await.unwrap();
        assert_eq!(initial_body["aggregation_count"], 0);

        // POST a new config with two aggregation_ids. The YAML shape
        // matches what `StreamingConfig::from_yaml_data` parses — see
        // `asap-common/dependencies/rs/asap_types/src/streaming_config.rs`
        // and the sample files in `asap-tools/execution-utilities/`.
        let new_config_yaml = r#"
aggregations:
  - aggregationId: 101
    aggregationType: Sum
    aggregationSubType: ''
    metric: cpu_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
  - aggregationId: 102
    aggregationType: Sum
    aggregationSubType: ''
    metric: mem_usage
    labels:
      grouping: [host, region]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 120
    windowType: tumbling
    spatialFilter: ''
"#;
        let post_resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(new_config_yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        let post_status = post_resp.status();
        let post_body: serde_json::Value = post_resp.json().await.unwrap();
        assert!(
            post_status.is_success(),
            "POST returned {post_status}: {post_body}"
        );
        assert_eq!(post_body["status"], "success");
        assert_eq!(post_body["new_aggregation_count"], 2);
        let added = post_body["agg_ids_added"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            added,
            std::collections::HashSet::from([101u64, 102u64]),
            "expected both ids in added set"
        );

        // GET again: should reflect the two new ids.
        let after = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .expect("GET after swap failed");
        assert!(after.status().is_success());
        let after_body: serde_json::Value = after.json().await.unwrap();
        assert_eq!(after_body["aggregation_count"], 2);

        // The underlying HotReloadStreamingConfig handle (cloned into
        // the server at setup) also reflects the swap — proving that
        // downstream consumers that re-snapshot would see the new
        // state.
        let direct_snap = hot_reload.snapshot();
        assert_eq!(direct_snap.aggregation_configs.len(), 2);
        assert!(direct_snap.aggregation_configs.contains_key(&101));
        assert!(direct_snap.aggregation_configs.contains_key(&102));
    }

    #[tokio::test]
    async fn test_streaming_config_hot_reload_missing_handle_503() {
        // setup_test_server() passes `None` for hot_reload → both
        // endpoints should return 503 with a clear error message.
        let server_port = setup_test_server().await;
        let client = Client::new();

        let get_resp = client
            .get(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(get_resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

        let post_resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .body("anything")
            .send()
            .await
            .unwrap();
        assert_eq!(post_resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_streaming_config_hot_reload_rejects_bad_yaml() {
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .body("not: : : valid: yaml: :")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "error");
    }

    /// Set up a test server with both a hot-reload handle AND a schema
    /// registry attached. Proves the Phase 2b wiring: a swap through
    /// the HTTP handler drives schema lifecycle transitions
    /// event-driven (sketch DB design §6).
    async fn setup_test_server_with_hot_reload_and_schemas(
        hot_reload: HotReloadStreamingConfig,
        schemas: Arc<crate::stores::sketch_db::SchemaRegistry>,
    ) -> u16 {
        let adapter_config =
            AdapterConfig::prometheus_promql("http://127.0.0.1:9999".to_string(), false);
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };
        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));
        let server = HttpServer::new(config, query_engine, store, None)
            .with_hot_reload_config(hot_reload)
            .with_schemas(schemas);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_streaming_config_swap_drives_schema_reconcile() {
        use crate::stores::sketch_db::{AggStatus, SchemaRegistry};

        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let schemas = Arc::new(SchemaRegistry::empty());
        let server_port =
            setup_test_server_with_hot_reload_and_schemas(hot_reload.clone(), schemas.clone())
                .await;
        let client = Client::new();

        // Empty registry at start.
        assert!(!schemas.is_writable(101));
        assert!(!schemas.is_writable(202));

        // POST a config with two agg_ids — the handler should swap
        // the config AND reconcile the registry.
        let yaml = r#"
aggregations:
  - aggregationId: 101
    aggregationType: Sum
    aggregationSubType: ''
    metric: cpu_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
  - aggregationId: 202
    aggregationType: Sum
    aggregationSubType: ''
    metric: mem_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        // The new field from Phase 2b.
        let created = body["schemas_created"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            created,
            std::collections::HashSet::from([101u64, 202u64]),
            "expected both agg_ids in schemas_created"
        );

        // Registry now has Active schemas for both ids.
        assert!(schemas.is_writable(101));
        assert!(schemas.is_writable(202));
        assert_eq!(schemas.get(101).unwrap().status(), AggStatus::Active);
        assert_eq!(schemas.get(202).unwrap().status(), AggStatus::Active);

        // Swap to a config that removes 101. Schema 101 should be
        // Retired (§6.3 barrier: is_writable(101) now false).
        let yaml2 = r#"
aggregations:
  - aggregationId: 202
    aggregationType: Sum
    aggregationSubType: ''
    metric: mem_usage
    labels:
      grouping: [host]
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp2 = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml2.to_string())
            .send()
            .await
            .expect("POST failed");
        let body2: serde_json::Value = resp2.json().await.unwrap();
        let retired = body2["schemas_retired"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(retired, vec![101u64]);

        assert!(
            !schemas.is_writable(101),
            "101 retired, should be unwritable"
        );
        assert!(schemas.is_writable(202), "202 still active");
        assert_eq!(schemas.get(101).unwrap().status(), AggStatus::Retired);
    }

    #[tokio::test]
    async fn test_streaming_config_swap_without_schemas_still_succeeds() {
        // If the HttpServer isn't wired with a schema registry, the
        // swap handler still works — it just omits schemas_created
        // and schemas_retired from the response.
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let server_port = setup_test_server_with_hot_reload(Some(hot_reload)).await;
        let client = Client::new();

        let yaml = r#"
aggregations:
  - aggregationId: 42
    aggregationType: Sum
    aggregationSubType: ''
    metric: m
    labels:
      grouping: []
      rollup: []
      aggregated: []
    parameters: {}
    windowSize: 60
    windowType: tumbling
    spatialFilter: ''
"#;
        let resp = client
            .post(format!(
                "http://127.0.0.1:{server_port}/api/v1/streaming-config"
            ))
            .header("content-type", "application/x-yaml")
            .body(yaml.to_string())
            .send()
            .await
            .expect("POST failed");
        assert!(resp.status().is_success());
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "success");
        // Without a registry, the arrays are empty (not missing).
        assert_eq!(body["schemas_created"].as_array().unwrap().len(), 0);
        assert_eq!(body["schemas_retired"].as_array().unwrap().len(), 0);
    }
}

// ── Controller integration: PrecomputeJob execution ──────────────────────────

/// Request body from DataCollector controller's PrecomputeJob.
#[derive(serde::Deserialize)]
struct PrecomputeJobRequest {
    /// PromQL expression to evaluate against stored sketches.
    query_expr: String,
    /// Window granularity in seconds.
    #[serde(default)]
    granularity_secs: u64,
    /// Start timestamp (unix seconds). 0 = use earliest available.
    #[serde(default)]
    start: f64,
    /// End timestamp (unix seconds). 0 = use latest available.
    #[serde(default)]
    end: f64,
}

/// Execute a precompute job from the DataCollector controller.
///
/// POST /api/v1/precompute
///
/// The controller creates PrecomputeJobs when a query's upper sub-tree
/// (e.g., TopK, HistogramQuantile) requires evaluation on merged sketches.
/// This endpoint receives that job and runs it against the SimpleMapStore.
async fn handle_precompute_job(
    State(state): State<AppState>,
    axum::Json(req): axum::Json<PrecomputeJobRequest>,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let time = if req.end > 0.0 {
        req.end
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    };

    info!(
        query = %req.query_expr,
        start = %req.start,
        end = %req.end,
        granularity_secs = %req.granularity_secs,
        time = %time,
        "Executing precompute job from controller"
    );

    match state.query_engine.handle_query_promql(req.query_expr, time) {
        Some((key_by, result)) => {
            let body = serde_json::json!({
                "status": "success",
                "data": {
                    "result_type": "precompute",
                    "key_by": format!("{:?}", key_by),
                    "result": format!("{:?}", result),
                }
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        None => {
            // Query not answerable by sketches — return 404 with hint
            let body = serde_json::json!({
                "status": "error",
                "error": "query not answerable by stored sketches",
                "hint": "ensure the metric has been ingested via OTLP/Kafka and a matching query_config exists"
            });
            (StatusCode::NOT_FOUND, axum::Json(body)).into_response()
        }
    }
}

/// Health check endpoint for DataCollector controller to verify backend is alive.
async fn handle_health() -> &'static str {
    "ok"
}

/// Return list of metrics currently in the store.
async fn handle_store_metrics(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    match state.store.get_earliest_timestamp_per_aggregation_id() {
        Ok(timestamps) => {
            let body = serde_json::json!({
                "status": "success",
                "aggregation_count": timestamps.len(),
                "earliest_timestamps": timestamps,
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("{}", e),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
        }
    }
}

// ─── StreamingConfig hot-reload (PR E) ───────────────────────────────────
//
// `GET /api/v1/streaming-config`  — return the currently active config
//                                   as JSON (debug / verification).
// `POST /api/v1/streaming-config` — accept a YAML body, parse, and
//                                   atomically swap via ArcSwap.
//
// Phase 1 scope: the swap only takes effect for new readers that
// snapshot after the swap. `SimpleEngine`, the ingest router, and
// in-flight precompute workers all hold startup snapshots today and
// ignore the swap until they are rebuilt — see the module doc on
// `HotReloadStreamingConfig` for the full contract. Tests POST a new
// config and verify it via the GET endpoint; controller integration
// and per-query re-snapshot are phase 2.

async fn handle_get_streaming_config(State(state): State<AppState>) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    let Some(handle) = state.hot_reload_config else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };
    let snap = handle.snapshot();
    let body = serde_json::json!({
        "status": "success",
        "aggregation_count": snap.aggregation_configs.len(),
        "aggregation_ids": snap.aggregation_configs.keys().copied().collect::<Vec<_>>(),
        "streaming_config": &*snap,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn handle_post_streaming_config(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use std::collections::HashSet;

    let Some(handle) = state.hot_reload_config else {
        let body = serde_json::json!({
            "status": "error",
            "error": "hot-reload handle not attached; backend was built without HttpServer::with_hot_reload_config",
        });
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
    };

    let yaml_text = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("request body is not valid UTF-8: {e}"),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let yaml_value: serde_yaml::Value = match serde_yaml::from_str(yaml_text) {
        Ok(v) => v,
        Err(e) => {
            let body = serde_json::json!({
                "status": "error",
                "error": format!("YAML parse error: {e}"),
            });
            return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
        }
    };
    let new_config =
        match asap_types::streaming_config::StreamingConfig::from_yaml_data(&yaml_value, None) {
            Ok(c) => c,
            Err(e) => {
                let body = serde_json::json!({
                    "status": "error",
                    "error": format!("StreamingConfig build error: {e}"),
                });
                return (StatusCode::BAD_REQUEST, axum::Json(body)).into_response();
            }
        };

    let new_ids: HashSet<u64> = new_config.aggregation_configs.keys().copied().collect();
    let old_arc = handle.swap(new_config);
    let old_ids: HashSet<u64> = old_arc.aggregation_configs.keys().copied().collect();
    let added: Vec<u64> = new_ids.difference(&old_ids).copied().collect();
    let removed: Vec<u64> = old_ids.difference(&new_ids).copied().collect();

    if !removed.is_empty() {
        warn!(
            "streaming-config hot-reload removed agg_ids {:?} — any in-flight \
             precompute worker groups for these ids will continue with their \
             construction-time config until they close naturally (phase 1 \
             limitation; see HotReloadStreamingConfig module doc)",
            removed
        );
    }

    // Phase 2b of the sketch DB design (docs/design-sketch-db.md §6):
    // drive schema lifecycle transitions event-driven from the swap
    // handler instead of running on every ingest batch. When attached,
    // the SchemaRegistry's reconcile adds new agg_ids as Active
    // schemas and retires removed agg_ids (scheduling their data for
    // expiry after the retirement retention).
    //
    // If `schemas` isn't attached (tests, legacy deployments), the
    // per-batch reconcile in IngestState still handles it — just
    // with up to one batch worth of latency.
    let (schema_added, schema_retired) = if let Some(schemas) = &state.schemas {
        let snap = handle.snapshot();
        let summary = schemas.reconcile(snap.as_ref());
        (summary.added, summary.retired)
    } else {
        (Vec::new(), Vec::new())
    };

    let body = serde_json::json!({
        "status": "success",
        "agg_ids_added": added,
        "agg_ids_removed": removed,
        "new_aggregation_count": new_ids.len(),
        "schemas_created": schema_added,
        "schemas_retired": schema_retired,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}
