//! OTLP ingest driver.
//!
//! Accepts OTLP metrics via gRPC (4317) and HTTP (4318, POST /v1/metrics).
//! Parses `ExportMetricsServiceRequest`, and — when wired to a precompute
//! engine via [`OtlpReceiver::with_ingest_state`] — routes both raw metric
//! points and pre-built sketches through the precompute engine's worker
//! pool. The precompute engine then performs window-aligned aggregation
//! per `StreamingConfig` and writes results to `SimpleMapStore`.
//!
//! Architectural flow:
//! ```text
//!   DataCollector OTel collector
//!     → OTLP gRPC/HTTP (this receiver)
//!     → precompute engine ingest router
//!     → workers (per (agg_id, group_key) panes)
//!     → StoreOutputSink → SimpleMapStore
//!     → query engine
//! ```
//!
//! Labels from the OTLP wire format are preserved all the way into
//! `KeyByLabelValues` via the standard `series_key` → grouping-label
//! extraction used by the Prometheus/VictoriaMetrics ingest paths.

use std::collections::HashMap;
use std::io::Read;

use crate::data_model::AggregateCore;
use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::IngestState;
use crate::precompute_operators::sketch_envelope_accumulator::SketchEnvelopeAccumulator;
use crate::routing::FreshnessProbeCache;
use asap_otel_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use asap_otel_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use asap_otel_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope};
use axum::{body::Bytes, extract::State, routing::post, Json, Router};
use flate2::read::GzDecoder;
use prost::Message;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

/// Configuration for the OTLP receiver.
#[derive(Debug, Clone)]
pub struct OtlpReceiverConfig {
    pub grpc_port: u16,
    pub http_port: u16,
}

/// Shared state accessible by both gRPC and HTTP handlers.
#[derive(Clone)]
struct OtlpSharedState {
    /// Handle into the precompute engine's worker pool. When `Some`,
    /// OTLP metrics and sketches are routed through the engine; when
    /// `None` the receiver accepts data but only logs it (no storage).
    ingest_state: Option<Arc<IngestState>>,
    /// Freshness-probe last-value cache (issue #46 ⑥). When `Some`,
    /// every received `http_freshness_probe_*` data point updates the
    /// cache so the HTTP query handler can answer
    /// `last_over_time(<probe>[<range>])` from RAM with sub-second
    /// freshness — bypassing the 60–90 s cold-tier flush gap that
    /// would otherwise leave a 10 s lookback window empty.
    probe_cache: Option<Arc<FreshnessProbeCache>>,
}

/// OTLP receiver that accepts metrics via gRPC and HTTP.
pub struct OtlpReceiver {
    config: OtlpReceiverConfig,
    ingest_state: Option<Arc<IngestState>>,
    probe_cache: Option<Arc<FreshnessProbeCache>>,
}

impl OtlpReceiver {
    /// Construct a receiver without a backend. Metrics are parsed and
    /// logged but not stored — useful for smoke-testing the OTLP pipe.
    pub fn new(config: OtlpReceiverConfig) -> Self {
        Self {
            config,
            ingest_state: None,
            probe_cache: None,
        }
    }

    /// Construct a receiver wired to a precompute engine's ingest state.
    /// Incoming metrics and sketches are routed through the engine's
    /// worker pool, where they are merged into per-`(agg_id, group_key)`
    /// panes and eventually emitted to the store.
    pub fn with_ingest_state(config: OtlpReceiverConfig, ingest_state: Arc<IngestState>) -> Self {
        Self {
            config,
            ingest_state: Some(ingest_state),
            probe_cache: None,
        }
    }

    /// Attach a [`FreshnessProbeCache`] so the receiver captures the
    /// latest sample for every `http_freshness_probe_*` metric it
    /// sees. Builder-style; chains with [`Self::with_ingest_state`]
    /// at the binary's wiring site. Without this call, probe-shaped
    /// metrics still reach the precompute engine and the cold-tier
    /// TSDB write path — only the in-memory query short-circuit is
    /// disabled.
    pub fn with_probe_cache(mut self, cache: Arc<FreshnessProbeCache>) -> Self {
        debug!(
            "OTLP receiver attached freshness-probe cache for issue #46 \
             criterion ⑥ short-circuit"
        );
        self.probe_cache = Some(cache);
        self
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.grpc_port));
        let http_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.http_port));

        let shared = Arc::new(OtlpSharedState {
            ingest_state: self.ingest_state.clone(),
            probe_cache: self.probe_cache.clone(),
        });

        let grpc_svc = MetricsServiceImpl {
            shared: shared.clone(),
        };
        // Bump tonic's default 4 MiB receive cap. A single agent
        // window emits ~1000 series, each carrying a typed
        // DDSketch / KLLSketch / ... state — the full-state
        // payloads run 17+ MiB at the cardinalities the e2e
        // harness uses. With the default cap, the gateway's
        // OTLP exporter retries forever with
        // `decoded message length too large`. Match the
        // gateway/agent receiver caps (`max_recv_msg_size_mib: 64`
        // in their YAMLs) so all three tiers agree.
        let grpc_svc =
            asap_otel_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer::new(
                grpc_svc,
            )
            .max_decoding_message_size(64 * 1024 * 1024);

        let app = Router::new()
            .route("/v1/metrics", post(handle_otlp_http))
            .with_state(shared);

        let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await?;
        let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

        info!("OTLP gRPC listening on {}", grpc_addr);
        info!("OTLP HTTP listening on {} (POST /v1/metrics)", http_addr);

        tokio::select! {
            r = tonic::transport::Server::builder()
                .add_service(grpc_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc_listener)) => {
                    if let Err(e) = r {
                        error!("OTLP gRPC server error: {}", e);
                    }
                }
            r = axum::serve(http_listener, app) => {
                if let Err(e) = r {
                    error!("OTLP HTTP server error: {}", e);
                }
            }
        }

        Ok(())
    }
}

struct MetricsServiceImpl {
    shared: Arc<OtlpSharedState>,
}

#[tonic::async_trait]
impl MetricsService for MetricsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        debug!("OTLP received request via gRPC");
        let req = request.into_inner();
        process_otlp_request(&req, "gRPC");
        if let Some(cache) = &self.shared.probe_cache {
            capture_freshness_probe_samples(&req, cache);
        }
        let mut unknown_series_ids: Vec<u64> = Vec::new();
        if let Some(state) = &self.shared.ingest_state {
            route_otlp_to_precompute(&req, state).await;
            unknown_series_ids =
                route_modified_otlp_sketches_to_precompute(&req, state).await;
        }
        debug!("OTLP sending response via gRPC");
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
            // Modified-OTLP collector hands out stable series descriptors via
            // this field; not yet wired (PR B will populate it when the
            // backend learns to mint series_ids).
            series_assignments: Vec::new(),
            // Phase 4 — backend signals senders to evict cached sids here
            // when this Export carried a sid the resolver does not
            // recognize (sid-cache divergence — e.g. after a backend
            // restart without persistence, or when the sender's sid
            // disagrees with the resolved sid for the same attrs).
            unknown_series_ids,
        }))
    }

    async fn resolve_series_i_ds(
        &self,
        request: Request<
            asap_otel_proto::tonic::collector::metrics::v1::ResolveSeriesIDsRequest,
        >,
    ) -> Result<
        Response<asap_otel_proto::tonic::collector::metrics::v1::ResolveSeriesIDsResponse>,
        Status,
    > {
        use asap_otel_proto::tonic::collector::metrics::v1::{
            ResolveSeriesIDsResponse, SeriesAssignment,
        };
        let req = request.into_inner();
        let mut assignments = Vec::with_capacity(req.queries.len());
        if let Some(state) = &self.shared.ingest_state {
            for q in req.queries {
                // The fingerprint travels as opaque bytes on the wire, but
                // the resolver hashes it as a string (the sender's
                // canonical fingerprint algorithm matches our
                // `canonical_attrs_fingerprint`). UTF-8 is lossy here only
                // for malformed inputs — those produce a degraded but
                // deterministic key, never a panic.
                let fp = String::from_utf8_lossy(&q.attributes_fingerprint).into_owned();
                let sid = state.series_resolver.resolve(&q.metric_name, &fp);
                assignments.push(SeriesAssignment {
                    attributes_fingerprint: q.attributes_fingerprint,
                    series_id: sid,
                    ..Default::default()
                });
            }
        }
        Ok(Response::new(ResolveSeriesIDsResponse { assignments }))
    }
}

async fn handle_otlp_http(
    headers: axum::http::HeaderMap,
    State(shared): State<Arc<OtlpSharedState>>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    debug!("OTLP received request via HTTP, body_bytes={}", body.len());
    let body = if let Some(enc) = headers.get(axum::http::header::CONTENT_ENCODING) {
        let enc = enc.to_str().unwrap_or("").trim().to_ascii_lowercase();
        if enc == "gzip" {
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut out = Vec::new();
            decoder.read_to_end(&mut out).map_err(|e| {
                (
                    axum::http::StatusCode::BAD_REQUEST,
                    format!("Gzip decode error: {}", e),
                )
            })?;
            Bytes::from(out)
        } else {
            body
        }
    } else {
        body
    };

    let req = ExportMetricsServiceRequest::decode(body.as_ref()).map_err(|e| {
        (
            axum::http::StatusCode::BAD_REQUEST,
            format!("Protobuf decode error: {}", e),
        )
    })?;
    process_otlp_request(&req, "HTTP");
    if let Some(cache) = &shared.probe_cache {
        capture_freshness_probe_samples(&req, cache);
    }
    let mut unknown_series_ids: Vec<u64> = Vec::new();
    if let Some(state) = &shared.ingest_state {
        route_otlp_to_precompute(&req, state).await;
        unknown_series_ids = route_modified_otlp_sketches_to_precompute(&req, state).await;
    }
    debug!("OTLP sending response via HTTP");
    Ok(Json(serde_json::json!({
        "rejected": 0,
        "unknown_series_ids": unknown_series_ids,
    })))
}

/// A parsed metric data point: name, labels, timestamp (nanos), and numeric value.
#[derive(Debug)]
pub struct MetricPoint {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub timestamp_nanos: u64,
    pub value: f64,
}

/// A parsed sketch payload extracted from OTLP attributes. Carries the
/// metric name, attribute name (identifies the sketch kind on the wire),
/// labels (preserved from the OTLP DataPoint attributes + resource/scope),
/// wire timestamp, and the opaque `SketchEnvelope` protobuf bytes.
#[derive(Debug)]
pub struct SketchPoint {
    pub name: String,
    pub attr_name: String,
    pub labels: HashMap<String, String>,
    pub timestamp_nanos: u64,
    pub payload: Vec<u8>,
}

type OtlpParseResult = (Vec<MetricPoint>, Vec<SketchPoint>);

fn format_series_key(name: &str, labels: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    let labels_str = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", name, labels_str)
}

fn get_sketch_payload_from_attrs(
    attrs: &[asap_otel_proto::tonic::common::v1::KeyValue],
) -> Option<(String, Vec<u8>)> {
    for kv in attrs {
        match kv.key.as_str() {
            "kll.sketch_payload" | "cms.sketch_payload" | "countsketch.sketch_payload" => {
                if let Some(value) = &kv.value {
                    if let Some(AnyValueVariant::BytesValue(bytes)) = &value.value {
                        return Some((kv.key.clone(), bytes.clone()));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn log_sketch_envelope_type(attr_name: &str, payload: &[u8], metric_name: &str) {
    match SketchEnvelope::decode(payload) {
        Ok(env) => {
            let sketch_type = match env.sketch_state {
                Some(sketch_envelope::SketchState::Kll(_)) => "KLL",
                Some(sketch_envelope::SketchState::CountMin(_)) => "CountMin",
                Some(sketch_envelope::SketchState::CountSketch(_)) => "CountSketch",
                Some(_) => "Other",
                None => "Unknown",
            };
            debug!(
                "OTLP Sketches: metric='{}' attr='{}' payload_bytes={} sketch_type={}",
                metric_name,
                attr_name,
                payload.len(),
                sketch_type
            );
        }
        Err(e) => {
            debug!(
                "OTLP Sketches: metric='{}' attr='{}' payload_bytes={} decode_error='{}'",
                metric_name,
                attr_name,
                payload.len(),
                e
            );
        }
    }
}

/// Capture every `http_freshness_probe_*` data point in the request
/// into the [`FreshnessProbeCache`]. Walks the parsed `MetricPoint`s
/// and only stores those whose metric name matches the probe prefix
/// — the cache itself enforces the prefix check via
/// [`FreshnessProbeCache::record`], so non-probe points are
/// short-circuited cheaply.
///
/// Issue #46 ⑥ — without this hook, the cold-tier flush latency
/// (gorillas3 → 60 s TSDB block → Thanos sync) leaves the
/// `last_over_time(probe[10s])` query empty for the entire MVP demo
/// run. The cache lets the HTTP query handler answer the same query
/// from RAM with sub-second freshness.
fn capture_freshness_probe_samples(
    request: &ExportMetricsServiceRequest,
    cache: &FreshnessProbeCache,
) {
    let (points, _sketches) = otlp_to_metric_points_and_sketches(request);
    let mut updated = 0usize;
    for point in &points {
        // The cache filters by metric-name prefix internally; calling
        // `record` for every point is fine — non-probes are cheap
        // string-prefix rejections and do not touch the lock.
        let ts_ms = (point.timestamp_nanos / 1_000_000) as i64;
        if cache.record(&point.name, ts_ms, point.value) {
            updated += 1;
        }
    }
    if updated > 0 {
        debug!(
            updated_probes = updated,
            cache_size = cache.len(),
            "freshness-probe cache updated"
        );
    }
}

fn process_otlp_request(request: &ExportMetricsServiceRequest, transport: &str) {
    let resource_count = request.resource_metrics.len();
    let total_points = otlp_to_record_count(request);
    if resource_count > 0 || total_points > 0 {
        debug!(
            "OTLP ingest: received {} resource metrics, {} total data points (transport={})",
            resource_count, total_points, transport
        );
    }

    let (points, sketch_payloads) = otlp_to_metric_points_and_sketches(request);

    for sketch in &sketch_payloads {
        log_sketch_envelope_type(&sketch.attr_name, &sketch.payload, &sketch.name);
    }
    if !sketch_payloads.is_empty() {
        debug!(
            "OTLP Sketch Payload Flow: received {} sketch payload(s), decoded successfully",
            sketch_payloads.len()
        );
    }

    let mut by_series: HashMap<String, usize> = HashMap::new();
    for point in &points {
        let key = format_series_key(&point.name, &point.labels);
        *by_series.entry(key).or_insert(0) += 1;
    }
    if !by_series.is_empty() {
        debug!(
            "OTLP Raw Metrics Flow: received {} raw metric series",
            by_series.len()
        );
        for (series, count) in by_series {
            debug!("OTLP Raw Metrics Flow: series {} count={}", series, count);
        }
    }
    if let Some(first) = points.first() {
        debug!(
            "OTLP parse example: {} {:?} @{}ns = {}",
            first.name, first.labels, first.timestamp_nanos, first.value
        );
    }
}

/// Route parsed OTLP data through the precompute engine's worker pool.
///
/// Both raw metric points and pre-built sketch payloads are dispatched via
/// `WorkerMessage::GroupSamples` / `WorkerMessage::AccumulatorInput`, with
/// `(agg_id, group_key)` derived from the wire labels using the same logic
/// as the Prometheus/VictoriaMetrics paths. This preserves full label
/// semantics and lets the precompute engine perform config-driven window
/// aggregation over both streams.
///
/// Metrics whose name does not match any aggregation in the streaming
/// config are dropped with a debug log — the precompute engine only
/// maintains state for configured metrics.
///
/// Flush a per-driver `HashMap<agg_id, count>` of §6.3 write-barrier
/// drops into `IngestState::record_barrier_drop`, emitting a single
/// debug log summarising the batch. Called from every OTLP routing
/// function after its inner loop finishes, so a query against the
/// `/metrics` endpoint sees a unified `samples_blocked_by_schema_barrier`
/// counter regardless of which OTLP variant the DataCollector is
/// shipping.
fn flush_barrier_drops(state: &IngestState, drops: &HashMap<u64, u64>, driver_tag: &'static str) {
    if drops.is_empty() {
        return;
    }
    let total: u64 = drops.values().sum();
    for (agg_id, count) in drops {
        state.record_barrier_drop(*agg_id, *count);
    }
    debug!(
        driver = driver_tag,
        total_dropped = total,
        by_agg_id = ?drops,
        "§6.3 write barrier dropped OTLP samples (agg is retired/expired)"
    );
}

async fn route_otlp_to_precompute(
    request: &ExportMetricsServiceRequest,
    ingest_state: &Arc<IngestState>,
) {
    let ingest_received_at = Instant::now();
    let (points, sketch_payloads) = otlp_to_metric_points_and_sketches(request);

    // Snapshot the latest agg_configs from the hot-reload handle so
    // new aggregations are visible without restart.
    let snap = ingest_state.config_snapshot();
    let agg_configs = snap.get_all_aggregation_configs();
    // Reconcile schema registry against the snapshot — Phase 2a of
    // the sketch DB design (`docs/design-sketch-db.md` §6).
    let _ = ingest_state.schemas.reconcile(&snap);

    // Build (agg_id, group_key) → Vec<(series_key, ts_ms, value)> for raw points.
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();
    let mut raw_matched = 0usize;
    let mut raw_unmatched = 0usize;
    let mut raw_barrier_drops: HashMap<u64, u64> = HashMap::new();

    for point in &points {
        let series_key = format_series_key(&point.name, &point.labels);
        let ts_ms = (point.timestamp_nanos / 1_000_000) as i64;
        let mut matched = false;
        for config in agg_configs.values() {
            if config.metric != point.name
                && config.spatial_filter_normalized != point.name
                && config.spatial_filter != point.name
            {
                continue;
            }
            // §6.3 write-side schema barrier — see ingest_handler.rs.
            if !ingest_state.schemas.is_writable(config.aggregation_id) {
                *raw_barrier_drops.entry(config.aggregation_id).or_default() += 1;
                continue;
            }
            let group_key = IngestState::extract_group_key_for(&series_key, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((series_key.clone(), ts_ms, point.value));
            matched = true;
        }
        if matched {
            raw_matched += 1;
        } else {
            raw_unmatched += 1;
        }
    }
    flush_barrier_drops(ingest_state, &raw_barrier_drops, "otlp-raw");

    let raw_messages: Vec<WorkerMessage> = by_group
        .into_iter()
        .map(
            |((agg_id, group_key), samples)| WorkerMessage::GroupSamples {
                agg_id,
                group_key,
                samples,
                ingest_received_at,
            },
        )
        .collect();

    if !raw_messages.is_empty() {
        if let Err(e) = ingest_state
            .router
            .route_group_batch(raw_messages, ingest_received_at)
            .await
        {
            warn!("OTLP raw-sample routing error: {}", e);
        }
    }

    // Build AccumulatorInput messages for pre-built sketches.
    // Each sketch payload carries a metric name that must match an
    // aggregation config; labels come from the point's attributes. The
    // payload is decoded enough to identify the sketch type for logging;
    // the accumulator the worker receives is a conservative placeholder
    // until per-variant `SketchEnvelope → concrete accumulator` decoders
    // are wired up (see TODO below).
    let mut sketch_messages: Vec<WorkerMessage> = Vec::new();
    let mut sketch_matched = 0usize;
    let mut sketch_unmatched = 0usize;
    let mut sketch_barrier_drops: HashMap<u64, u64> = HashMap::new();
    for point in &sketch_payloads {
        let series_key = format_series_key(&point.name, &point.labels);
        let ts_ms = (point.timestamp_nanos / 1_000_000) as i64;
        let sketch_type = identify_sketch_type(&point.payload);
        let mut matched = false;
        for config in agg_configs.values() {
            if config.metric != point.name
                && config.spatial_filter_normalized != point.name
                && config.spatial_filter != point.name
            {
                continue;
            }
            // §6.3 write-side schema barrier — see ingest_handler.rs.
            if !ingest_state.schemas.is_writable(config.aggregation_id) {
                *sketch_barrier_drops
                    .entry(config.aggregation_id)
                    .or_default() += 1;
                continue;
            }
            let group_key = IngestState::extract_group_key_for(&series_key, config);
            // Wrap the raw SketchEnvelope bytes in a SketchEnvelopeAccumulator
            // so the precompute engine receives the opaque sketch as-is. This
            // preserves all sketch state end-to-end; per-variant decoding
            // (e.g. CountMin → CountMinSketchAccumulator) can layer on top
            // later without changing the routing contract.
            let accumulator: Box<dyn AggregateCore> =
                match SketchEnvelopeAccumulator::from_proto_bytes(point.payload.clone()) {
                    Ok(acc) => Box::new(acc),
                    Err(e) => {
                        warn!(
                            "OTLP sketch decode failed for metric='{}' attr='{}': {}",
                            point.name, point.attr_name, e
                        );
                        continue;
                    }
                };
            sketch_messages.push(WorkerMessage::AccumulatorInput {
                agg_id: config.aggregation_id,
                group_key,
                timestamp_ms: ts_ms,
                accumulator,
                ingest_received_at,
            });
            matched = true;
        }
        if matched {
            sketch_matched += 1;
            debug!(
                "OTLP sketch routed to precompute engine: metric='{}' attr='{}' type={} bytes={}",
                point.name,
                point.attr_name,
                sketch_type,
                point.payload.len()
            );
        } else {
            sketch_unmatched += 1;
        }
    }
    flush_barrier_drops(ingest_state, &sketch_barrier_drops, "otlp-sketch-envelope");

    if !sketch_messages.is_empty() {
        if let Err(e) = ingest_state
            .router
            .route_group_batch(sketch_messages, ingest_received_at)
            .await
        {
            warn!("OTLP sketch routing error: {}", e);
        }
    }

    if raw_unmatched > 0 || sketch_unmatched > 0 {
        debug!(
            "OTLP ingest: {} raw samples + {} sketches dropped (no matching aggregation config); \
             {} raw + {} sketches routed to precompute engine",
            raw_unmatched, sketch_unmatched, raw_matched, sketch_matched
        );
    }
}

/// Walk the modified-OTLP first-class sketch metric variants
/// (`DDSketch` / `KLLSketch` / `CountSketch` / `CountMinSketch` /
/// `HLLSketch` on `Metric.data` tags 13–17) and dispatch each
/// `*SketchDataPoint` through the precompute engine via
/// `WorkerMessage::AccumulatorInput`.
///
/// Per-variant decoding of the typed `sketch` bytes is delegated to
/// `decode_modified_otlp_sketch_bytes`, which in turn calls into the
/// matching concrete accumulator's `from_sketchlib_proto_bytes`
/// constructor when one exists. Variants without a concrete decoder
/// today (KLL / DDSketch / CountSketch / HLL) fall through to the
/// §5.2 fallback path so the user still gets a correct answer; PR C
/// (task #8) will close those decoder gaps.
async fn route_modified_otlp_sketches_to_precompute(
    request: &ExportMetricsServiceRequest,
    ingest_state: &Arc<IngestState>,
) -> Vec<u64> {
    use asap_otel_proto::tonic::metrics::v1::metric::Data;

    let ingest_received_at = Instant::now();
    let snap = ingest_state.config_snapshot();
    let agg_configs = snap.get_all_aggregation_configs();
    // Reconcile schema registry against the snapshot — Phase 2a of
    // the sketch DB design (`docs/design-sketch-db.md` §6).
    let _ = ingest_state.schemas.reconcile(&snap);
    let mut messages: Vec<WorkerMessage> = Vec::new();
    let mut routed = 0usize;
    let mut decoded_failed = 0usize;
    let mut unconfigured = 0usize;
    let mut barrier_drops: HashMap<u64, u64> = HashMap::new();
    // Phase 4 — sids the receiver did not recognize this Export. Returned
    // to the caller so the gRPC / HTTP handler can stamp them into
    // `ExportMetricsServiceResponse.unknown_series_ids`. Senders evict
    // these sids and re-emit with attributes; backend re-resolves and
    // returns fresh `series_assignments`.
    let mut unknown_sids: Vec<u64> = Vec::new();

    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|r| attributes_to_map(&r.attributes))
            .unwrap_or_default();

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_attrs = scope_metrics
                .scope
                .as_ref()
                .map(|s| attributes_to_map(&s.attributes))
                .unwrap_or_default();

            for metric in &scope_metrics.metrics {
                if metric.name.is_empty() {
                    continue;
                }

                let base_labels: HashMap<String, String> = scope_attrs
                    .iter()
                    .chain(resource_attrs.iter())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                // Each branch yields an iterator-like slice of (kind, attrs,
                // time_unix_nano, sketch_bytes, encoding_i32) tuples. We
                // then route each tuple through the same dispatcher.
                let dps: Vec<ModifiedOtlpSketchDp> = match &metric.data {
                    Some(Data::Ddsketch(d)) => {
                        let cfg = crate::stores::sketch_db::sketch_index::SketchConfig::DDSketch {
                            relative_accuracy: d.relative_accuracy,
                        };
                        d.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                kind: SketchKind::DdSketch,
                                attrs: merge_point_attributes(&base_labels, &dp.attributes),
                                time_unix_nano: dp.time_unix_nano,
                                sketch: dp.sketch.clone(),
                                encoding: dp.encoding,
                                series_id: dp.series_id,
                                start_time_unix_nano: dp.start_time_unix_nano,
                                container_config: cfg.clone(),
                            })
                            .collect()
                    }
                    Some(Data::Kllsketch(k)) => {
                        let cfg = crate::stores::sketch_db::sketch_index::SketchConfig::Kll { k: k.k };
                        k.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                kind: SketchKind::Kll,
                                attrs: merge_point_attributes(&base_labels, &dp.attributes),
                                time_unix_nano: dp.time_unix_nano,
                                sketch: dp.sketch.clone(),
                                encoding: dp.encoding,
                                series_id: dp.series_id,
                                start_time_unix_nano: dp.start_time_unix_nano,
                                container_config: cfg.clone(),
                            })
                            .collect()
                    }
                    Some(Data::Countsketch(c)) => {
                        let cfg = crate::stores::sketch_db::sketch_index::SketchConfig::CountSketch {
                            rows: c.rows,
                            cols: c.cols,
                        };
                        c.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                kind: SketchKind::CountSketch,
                                attrs: merge_point_attributes(&base_labels, &dp.attributes),
                                time_unix_nano: dp.time_unix_nano,
                                sketch: dp.sketch.clone(),
                                encoding: dp.encoding,
                                series_id: dp.series_id,
                                start_time_unix_nano: dp.start_time_unix_nano,
                                container_config: cfg.clone(),
                            })
                            .collect()
                    }
                    Some(Data::Countminsketch(c)) => {
                        let cfg = crate::stores::sketch_db::sketch_index::SketchConfig::CountMin {
                            rows: c.rows,
                            cols: c.cols,
                        };
                        c.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                kind: SketchKind::CountMin,
                                attrs: merge_point_attributes(&base_labels, &dp.attributes),
                                time_unix_nano: dp.time_unix_nano,
                                sketch: dp.sketch.clone(),
                                encoding: dp.encoding,
                                series_id: dp.series_id,
                                start_time_unix_nano: dp.start_time_unix_nano,
                                container_config: cfg.clone(),
                            })
                            .collect()
                    }
                    Some(Data::Hllsketch(h)) => {
                        let cfg = crate::stores::sketch_db::sketch_index::SketchConfig::Hll {
                            precision: h.precision,
                        };
                        h.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                kind: SketchKind::Hll,
                                attrs: merge_point_attributes(&base_labels, &dp.attributes),
                                time_unix_nano: dp.time_unix_nano,
                                sketch: dp.sketch.clone(),
                                encoding: dp.encoding,
                                series_id: dp.series_id,
                                start_time_unix_nano: dp.start_time_unix_nano,
                                container_config: cfg.clone(),
                            })
                            .collect()
                    }
                    _ => continue,
                };

                for dp in dps {
                    let series_key = format_series_key(&metric.name, &dp.attrs);
                    let ts_ms = (dp.time_unix_nano / 1_000_000) as i64;

                    // Phase 4 — sid resolution gate. The four cases mirror
                    // the design doc §5.4 invariant:
                    //   (sid=0, attrs)        → mint a fresh sid and use it
                    //   (sid!=0, attrs)       → trust attrs; if cached value
                    //                           disagrees, the sender's sid
                    //                           is stale → push to
                    //                           `unknown_sids` so the
                    //                           response evicts it
                    //   (sid!=0, no attrs)    → reverse-lookup; if unknown,
                    //                           push to `unknown_sids` and
                    //                           drop this DP (sender will
                    //                           re-emit with attrs next pass)
                    //   (sid=0, no attrs)     → invalid wire shape, drop
                    let attrs_pairs: Vec<(&str, &str)> =
                        dp.attrs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let fp = crate::drivers::ingest::canonical_attrs_fingerprint(&attrs_pairs);
                    let resolved_sid: Option<u64> = if attrs_pairs.is_empty() {
                        // No attrs on the wire — sid alone must be
                        // recognized, otherwise signal stale.
                        match dp.series_id {
                            0 => None,
                            sid => {
                                if ingest_state.series_resolver.is_known(sid) {
                                    Some(sid)
                                } else {
                                    unknown_sids.push(sid);
                                    None
                                }
                            }
                        }
                    } else if dp.series_id == 0 {
                        // Attrs present, no sid yet → mint or fetch.
                        Some(
                            ingest_state
                                .series_resolver
                                .resolve(&metric.name, &fp),
                        )
                    } else {
                        // Both populated: attrs are the source of truth.
                        // Backend-resolved value wins; mismatched sender
                        // sid is signalled stale.
                        let cached = ingest_state
                            .series_resolver
                            .resolve(&metric.name, &fp);
                        if cached != dp.series_id {
                            unknown_sids.push(dp.series_id);
                        }
                        Some(cached)
                    };
                    let Some(sid) = resolved_sid else {
                        continue;
                    };

                    // Phase 5 — register a `SketchInstanceMetadata` on
                    // first sight of `sid` and append this DP's sketch
                    // state to the per-sid columnar storage. The instance
                    // is keyed by sid, so subsequent DPs on the same sid
                    // skip the register step. `group_by_keys` is
                    // `dp.attrs.keys()` — after the agent's `AggregateBy`
                    // rollup, `attributes` is the group-by VALUES vector,
                    // and its key set IS the group-by KEY set.
                    {
                        use crate::stores::sketch_db::sketch_index::{
                            AccuracyBound, Capability, SketchEncoding, SketchInstanceMetadata,
                            SketchKindHandle, SketchSampleState,
                        };
                        use std::collections::{BTreeMap, BTreeSet};

                        if ingest_state.sketch_index.instance(sid).is_none() {
                            let kind = sketch_kind_handle_for(&dp);
                            let cap = match kind {
                                SketchKindHandle::DDSketch | SketchKindHandle::Kll => {
                                    Capability::QuantileApprox(kind)
                                }
                                SketchKindHandle::Hll => Capability::CardinalityApprox,
                                SketchKindHandle::CountSketch
                                | SketchKindHandle::CountMin => {
                                    Capability::FrequencyTopk(kind)
                                }
                            };
                            let group_by_keys: BTreeSet<String> =
                                dp.attrs.keys().cloned().collect();
                            let cfg = dp.container_config.clone();
                            ingest_state.sketch_index.register(SketchInstanceMetadata {
                                sid,
                                metric_name: metric.name.clone(),
                                group_by_keys,
                                capability: cap,
                                sketch_kind: kind,
                                sketch_config: cfg.clone(),
                                accuracy: AccuracyBound::from_config(&cfg),
                                first_seen_unix_ms: ts_ms,
                            });
                        }

                        let label_values: BTreeMap<String, String> = dp
                            .attrs
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        let window: crate::stores::sketch_db::epoch_columnar::TimestampRange = (
                            dp.start_time_unix_nano / 1_000_000,
                            dp.time_unix_nano / 1_000_000,
                        );
                        let encoding = encoding_to_handle(dp.encoding)
                            .unwrap_or(SketchEncoding::ProtoFull);
                        ingest_state.sketch_index.append_sample(
                            sid,
                            label_values,
                            window,
                            SketchSampleState {
                                bytes: dp.sketch.clone(),
                                encoding,
                            },
                        );
                    }

                    // Encoding dispatch: full frames (PROTO /
                    // MSGPACK) decode standalone and refresh the
                    // per-series snapshot cache; delta frames
                    // (PROTO_DELTA) look up the cached base and
                    // apply the diff in place. The cache key is the
                    // series_key — agent-side windowing guarantees
                    // one in-flight delta per (metric, labels) so
                    // the next full snapshot replaces the current
                    // cache entry cleanly.
                    let accumulator: Box<dyn AggregateCore> = if dp.encoding == ENCODING_PROTO_DELTA
                        || dp.encoding == ENCODING_MSGPACK_DELTA
                    {
                        let Some(base) = ingest_state
                            .sketch_snapshots
                            .get(&series_key)
                            .map(|e| e.clone_boxed_core())
                        else {
                            decoded_failed += 1;
                            debug!(
                                "OTLP delta-sketch arrived before any base \
                                 snapshot (metric={}, series_key={}); \
                                 dropping — agent must resend the next full \
                                 frame",
                                metric.name, series_key
                            );
                            continue;
                        };
                        let mut merged = base;
                        if let Err(e) = apply_modified_otlp_delta_bytes(
                            dp.kind,
                            dp.encoding,
                            &mut merged,
                            &dp.sketch,
                        ) {
                            decoded_failed += 1;
                            debug!(
                                "OTLP delta-sketch apply failed \
                                 (metric={}, kind={:?}, encoding={}, \
                                 bytes={}): {} — falling through to §5.2 \
                                 fallback",
                                metric.name,
                                dp.kind,
                                dp.encoding,
                                dp.sketch.len(),
                                e
                            );
                            continue;
                        }
                        ingest_state
                            .sketch_snapshots
                            .insert(series_key.clone(), merged.clone_boxed_core());
                        merged
                    } else {
                        match decode_modified_otlp_sketch_bytes(dp.kind, dp.encoding, &dp.sketch) {
                            Ok(acc) => {
                                ingest_state
                                    .sketch_snapshots
                                    .insert(series_key.clone(), acc.clone_boxed_core());
                                acc
                            }
                            Err(e) => {
                                decoded_failed += 1;
                                debug!(
                                    "OTLP modified-proto sketch decode failed \
                                     (metric={}, kind={:?}, encoding={}, \
                                     bytes={}): {} — falling through to §5.2 \
                                     fallback",
                                    metric.name,
                                    dp.kind,
                                    dp.encoding,
                                    dp.sketch.len(),
                                    e
                                );
                                continue;
                            }
                        }
                    };

                    let mut matched_any = false;
                    for config in agg_configs.values() {
                        if config.metric != metric.name
                            && config.spatial_filter_normalized != metric.name
                            && config.spatial_filter != metric.name
                        {
                            continue;
                        }
                        // §6.3 write-side schema barrier — see ingest_handler.rs.
                        if !ingest_state.schemas.is_writable(config.aggregation_id) {
                            *barrier_drops.entry(config.aggregation_id).or_default() += 1;
                            continue;
                        }
                        let group_key = IngestState::extract_group_key_for(&series_key, config);
                        // DEPRECATED: aggregation_id-keyed write — remove
                        // after warm-tier validation. The Phase 5
                        // SketchIndex above is the new write path; this
                        // legacy router push stays in tandem until the
                        // query path's warm-tier reducer is wired
                        // end-to-end and the streaming-config /
                        // SimpleMapStore call sites can be deleted.
                        messages.push(WorkerMessage::AccumulatorInput {
                            agg_id: config.aggregation_id,
                            group_key,
                            timestamp_ms: ts_ms,
                            accumulator: accumulator.clone_boxed_core(),
                            ingest_received_at,
                        });
                        matched_any = true;
                    }
                    if matched_any {
                        routed += 1;
                    } else {
                        unconfigured += 1;
                    }
                }
            }
        }
    }

    flush_barrier_drops(ingest_state, &barrier_drops, "otlp-modified-proto");

    if !messages.is_empty() {
        if let Err(e) = ingest_state
            .router
            .route_group_batch(messages, ingest_received_at)
            .await
        {
            warn!("OTLP modified-proto sketch routing error: {}", e);
        }
    }

    if routed + decoded_failed + unconfigured > 0 {
        debug!(
            "OTLP modified-proto sketch ingest: {} routed, {} decode-failed (fallback), {} unconfigured",
            routed, decoded_failed, unconfigured
        );
    }

    unknown_sids
}

/// Phase 5 helper — map a `ModifiedOtlpSketchDp` to the matching
/// `SketchKindHandle` so registration and capability classification
/// share one source of truth.
fn sketch_kind_handle_for(
    dp: &ModifiedOtlpSketchDp,
) -> crate::stores::sketch_db::sketch_index::SketchKindHandle {
    use crate::stores::sketch_db::sketch_index::SketchKindHandle;
    match dp.kind {
        SketchKind::DdSketch => SketchKindHandle::DDSketch,
        SketchKind::Kll => SketchKindHandle::Kll,
        SketchKind::Hll => SketchKindHandle::Hll,
        SketchKind::CountSketch => SketchKindHandle::CountSketch,
        SketchKind::CountMin => SketchKindHandle::CountMin,
    }
}

/// Phase 5 helper — translate the wire-format `encoding` integer to the
/// SketchIndex's `SketchEncoding` enum. Returns `None` for the unset
/// (0) encoding so callers can default to `ProtoFull` (the dominant
/// case for full-state frames).
fn encoding_to_handle(encoding: i32) -> Option<crate::stores::sketch_db::sketch_index::SketchEncoding> {
    use crate::stores::sketch_db::sketch_index::SketchEncoding;
    match encoding {
        ENCODING_PROTO => Some(SketchEncoding::ProtoFull),
        ENCODING_PROTO_DELTA => Some(SketchEncoding::ProtoDelta),
        ENCODING_MSGPACK => Some(SketchEncoding::MsgpackFull),
        ENCODING_MSGPACK_DELTA => Some(SketchEncoding::MsgpackDelta),
        _ => None,
    }
}

/// Sketch family carried by a modified-OTLP `*SketchDataPoint`. Used by
/// the encoding dispatcher in `decode_modified_otlp_sketch_bytes`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SketchKind {
    DdSketch,
    Kll,
    CountSketch,
    CountMin,
    Hll,
}

/// A single modified-OTLP sketch data point flattened across the five
/// per-variant data-point types so the routing loop can treat them
/// uniformly.
struct ModifiedOtlpSketchDp {
    kind: SketchKind,
    attrs: HashMap<String, String>,
    time_unix_nano: u64,
    sketch: Vec<u8>,
    encoding: i32,
    /// Phase 4 — sender-supplied series_id, 0 when unset / first emit.
    /// Backend's resolver mints a fresh sid when this is 0 with attrs
    /// populated; pushes the sid into `unknown_series_ids` when this is
    /// non-zero with empty attrs and the resolver doesn't recognize it.
    series_id: u64,
    /// Phase 5 — DataPoint-level start of the sketch window. Combined
    /// with `time_unix_nano` to form the `(start_ms, end_ms)` window
    /// the SketchIndex's columnar storage keys on.
    start_time_unix_nano: u64,
    /// Phase 5 — sketch-instance configuration lifted off the parent
    /// container. Drives `SketchInstanceMetadata.sketch_config` and the
    /// derived `AccuracyBound`.
    container_config: crate::stores::sketch_db::sketch_index::SketchConfig,
}

/// Decode the typed `sketch` bytes from a modified-OTLP
/// `*SketchDataPoint` into a concrete `AggregateCore`.
///
/// Dispatches on the `(SketchKind, encoding)` pair. For each
/// `(kind, _ENCODING_PROTO)` pair we call the matching accumulator's
/// `from_sketchlib_proto_bytes` constructor. Variants without a
/// constructor today return `Err`; the caller falls through to §5.2
/// fallback so the user still gets a correct answer. Per-variant
/// decoders are tracked in PR C (task #8) and PR I (task #14, for
/// `_ENCODING_MSGPACK` parity).
fn decode_modified_otlp_sketch_bytes(
    kind: SketchKind,
    encoding: i32,
    bytes: &[u8],
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>> {
    use crate::precompute_operators::{
        CountMinSketchAccumulator, CountSketchAccumulator, DDSketchAccumulator,
        DatasketchesKLLAccumulator, HllSketchAccumulator,
    };

    // The encoding value is the raw i32 from the per-sketch encoding
    // enum. All five sketch variants share the same wire tag layout:
    //
    //   1  — ENCODING_PROTO          (full sketchlib proto state)
    //   2  — ENCODING_PROTO_DELTA    (sparse diff vs caller's base
    //                                 snapshot — apply via
    //                                 `apply_modified_otlp_delta_bytes`;
    //                                 not standalone-decodable)
    //   3  — ENCODING_MSGPACK        (full sketch-core msgpack state)
    //   4  — ENCODING_MSGPACK_DELTA  (MSGPACK diff; not yet wired)

    match encoding {
        ENCODING_PROTO => match kind {
            // Phase 3 step 3: DDSketch and KLL envelope-parsing /
            // sketch reconstruction route through the shared
            // `edge_runtime_adapter`, which delegates to
            // `asap-precompute-rs`'s `Sketch` trait. Backend's
            // accumulator wraps the result. Byte parity with Go is
            // covered by `asap_sketchlib` PRs #40 (DDSketch) and #41
            // (KLL).
            //
            // HLL / CountSketch / CountMinSketch byte parity is
            // tracked under ProjectASAP/ASAPCollector#243 — until it
            // lands those three sketches keep using the backend's
            // existing per-accumulator decoder.
            SketchKind::DdSketch => {
                use crate::precompute_operators::edge_runtime_adapter::{
                    reconstruct_via_runtime, ReconstructedSketch, SketchType as RtSketchType,
                };
                // Prefer the asap-precompute-rs runtime path (envelope-
                // wrapped bytes, the canonical edge-framework wire format).
                // If the input is a bare `DdSketchState` (as some unit-test
                // / pre-envelope agent payloads still emit, mirrored by the
                // PR #14 contract on `from_sketchlib_proto_bytes`), the
                // adapter returns an error decoding the envelope — fall
                // back to the backend's native decoder which already
                // accepts both shapes.
                match reconstruct_via_runtime(RtSketchType::DDSketch, bytes) {
                    Ok(ReconstructedSketch::DdSketch(inner)) => {
                        Ok(Box::new(DDSketchAccumulator { inner }))
                    }
                    Ok(_) => Err(
                        "edge_runtime_adapter returned non-DDSketch reconstruction".into(),
                    ),
                    Err(_) => Ok(Box::new(DDSketchAccumulator::from_sketchlib_proto_bytes(
                        bytes,
                    )?)),
                }
            }
            SketchKind::Kll => {
                use crate::precompute_operators::edge_runtime_adapter::{
                    reconstruct_via_runtime, ReconstructedSketch, SketchType as RtSketchType,
                };
                // Same envelope-vs-bare-state handling as DDSketch above.
                // Backend's KLL accumulator owns the wire-format-aligned
                // `KllSketch` rather than the high-throughput `KLL<f64>`
                // that asap-precompute-rs's `KLLWrapper` wraps internally
                // — when the adapter succeeds, bridge by re-feeding the
                // wrapper's snapshot bytes through backend's existing
                // decoder. The envelope work (decode + state extraction
                // + reconstruction) has already happened in the runtime
                // adapter; this final step just reshapes into backend's
                // accumulator type. On envelope-decode failure (bare
                // state bytes) fall through to the native decoder.
                match reconstruct_via_runtime(RtSketchType::KLLSketch, bytes) {
                    Ok(ReconstructedSketch::Kll { snapshot_bytes }) => Ok(Box::new(
                        DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(&snapshot_bytes)?,
                    )),
                    Ok(_) => Err(
                        "edge_runtime_adapter returned non-KLL reconstruction".into(),
                    ),
                    Err(_) => Ok(Box::new(
                        DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(bytes)?,
                    )),
                }
            }
            SketchKind::CountMin => Ok(Box::new(
                CountMinSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchKind::CountSketch => Ok(Box::new(
                CountSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchKind::Hll => Ok(Box::new(HllSketchAccumulator::from_sketchlib_proto_bytes(
                bytes,
            )?)),
        },
        ENCODING_MSGPACK => match kind {
            SketchKind::CountMin => Ok(Box::new(CountMinSketchAccumulator::from_msgpack_bytes(
                bytes,
            )?)),
            SketchKind::CountSketch => {
                Ok(Box::new(CountSketchAccumulator::from_msgpack_bytes(bytes)?))
            }
            SketchKind::Kll => Ok(Box::new(DatasketchesKLLAccumulator::from_msgpack_bytes(
                bytes,
            )?)),
            SketchKind::DdSketch => Ok(Box::new(DDSketchAccumulator::from_msgpack_bytes(bytes)?)),
            SketchKind::Hll => Ok(Box::new(HllSketchAccumulator::from_msgpack_bytes(bytes)?)),
        },
        ENCODING_PROTO_DELTA => Err(format!(
            "sketch encoding PROTO_DELTA (2) is not standalone-decodable — \
             it carries only a diff against the caller's base snapshot. \
             Caller must route these through \
             `apply_modified_otlp_delta_bytes` with a cached accumulator; \
             this decoder is for full-state frames only."
        )
        .into()),
        ENCODING_MSGPACK_DELTA => Err(format!(
            "sketch encoding MSGPACK_DELTA (4) deferred — PR G wires \
             PROTO_DELTA only; msgpack delta is a follow-up."
        )
        .into()),
        _ => Err(format!(
            "unknown modified-OTLP sketch encoding {encoding} \
             (expected 1 / 2 / 3 / 4)"
        )
        .into()),
    }
}

// Shared constants — exposed at module scope so both the full-state
// decoder and the delta applier match on the same values.
const ENCODING_PROTO: i32 = 1;
const ENCODING_PROTO_DELTA: i32 = 2;
const ENCODING_MSGPACK: i32 = 3;
const ENCODING_MSGPACK_DELTA: i32 = 4;

/// Apply a modified-OTLP `*SketchDataPoint.sketch` delta frame onto an
/// existing accumulator.
///
/// Paper §6.2 B3 / B4 sketch delta-transmission: the agent sends a
/// sparse diff against its last-flushed snapshot. The backend keeps a
/// per-series accumulator around, and on arrival of a delta frame
/// dispatches here to merge the diff in place.
///
/// The caller owns the per-series snapshot cache — this dispatcher is
/// stateless. Today wires `PROTO_DELTA` for DDSketch + HLL (the two
/// delta-capable sketches in `sketchlib-go`); `MSGPACK_DELTA` and the
/// KLL/CountSketch/CountMinSketch deltas are deferred to follow-ups
/// as their delta codecs land.
pub(crate) fn apply_modified_otlp_delta_bytes(
    kind: SketchKind,
    encoding: i32,
    existing: &mut Box<dyn AggregateCore>,
    bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::precompute_operators::{
        CountMinSketchAccumulator, CountSketchAccumulator, DDSketchAccumulator,
        HllSketchAccumulator,
    };

    match (encoding, kind) {
        (ENCODING_PROTO_DELTA, SketchKind::DdSketch) => {
            let dd = existing
                .as_any_mut()
                .downcast_mut::<DDSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a DDSketchAccumulator",
                )?;
            dd.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchKind::Hll) => {
            let hll = existing
                .as_any_mut()
                .downcast_mut::<HllSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not an HllSketchAccumulator",
                )?;
            hll.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchKind::CountSketch) => {
            let cs = existing
                .as_any_mut()
                .downcast_mut::<CountSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a CountSketchAccumulator",
                )?;
            cs.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchKind::CountMin) => {
            let cms = existing
                .as_any_mut()
                .downcast_mut::<CountMinSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a CountMinSketchAccumulator",
                )?;
            cms.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, other) => Err(format!(
            "PROTO_DELTA for sketch kind {other:?} is not yet supported; \
             DDSketch / HLL / CountSketch / CountMin are wired"
        )
        .into()),
        (ENCODING_MSGPACK_DELTA, _) => {
            Err("MSGPACK_DELTA encoding is not yet wired; PR G covers PROTO_DELTA only".into())
        }
        (ENCODING_PROTO, _) | (ENCODING_MSGPACK, _) => Err(format!(
            "encoding {encoding} is a full-state frame — route through \
             `decode_modified_otlp_sketch_bytes` and replace the cached \
             accumulator, not through the delta applier"
        )
        .into()),
        (other, _) => Err(format!("unknown modified-OTLP sketch encoding {other}").into()),
    }
}

/// Identify the concrete sketch type inside a `SketchEnvelope` payload,
/// returning a human-readable name for logging. Returns `"Unknown"` if the
/// payload does not decode or the `sketch_state` variant is unset.
fn identify_sketch_type(payload: &[u8]) -> &'static str {
    match SketchEnvelope::decode(payload) {
        Ok(env) => match env.sketch_state {
            Some(sketch_envelope::SketchState::Kll(_)) => "KLL",
            Some(sketch_envelope::SketchState::CountMin(_)) => "CountMin",
            Some(sketch_envelope::SketchState::CountSketch(_)) => "CountSketch",
            Some(_) => "Other",
            None => "Unknown",
        },
        Err(_) => "Unknown",
    }
}

/// Count total data points by traversing resource_metrics -> scope_metrics -> metrics.
/// Reuses the same conversion traversal as asap-otel-ingest (Gauge, Sum, Histogram, etc.).
fn otlp_to_record_count(request: &ExportMetricsServiceRequest) -> usize {
    let mut count = 0;
    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name.is_empty() {
                    continue;
                }

                use asap_otel_proto::tonic::metrics::v1::metric::Data;
                match &metric.data {
                    Some(Data::Gauge(g)) => count += g.data_points.len(),
                    Some(Data::Sum(s)) => count += s.data_points.len(),
                    Some(Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            if dp.sum.is_some() {
                                count += 1;
                            }
                            count += 1; // _count
                            count += dp.bucket_counts.len(); // _bucket per le
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            if dp.sum.is_some() {
                                count += 1;
                            }
                            count += 1; // _count
                            count += 1; // _scale
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        for dp in &summary.data_points {
                            count += 1; // _sum
                            count += 1; // _count
                            count += dp.quantile_values.len();
                        }
                    }
                    Some(Data::Ddsketch(d)) => count += d.data_points.len(),
                    Some(Data::Kllsketch(k)) => count += k.data_points.len(),
                    Some(Data::Countsketch(c)) => count += c.data_points.len(),
                    Some(Data::Countminsketch(c)) => count += c.data_points.len(),
                    Some(Data::Hllsketch(h)) => count += h.data_points.len(),
                    None => {}
                }
            }
        }
    }
    count
}

/// Parse OTLP request and convert to metric data points (name, labels, timestamp, value).
/// Data points with sketch payloads in attributes are excluded from points and returned
/// separately for Sketch Payload Flow processing.
fn otlp_to_metric_points_and_sketches(request: &ExportMetricsServiceRequest) -> OtlpParseResult {
    let mut points = Vec::new();
    let mut sketch_payloads = Vec::new();
    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|r| attributes_to_map(&r.attributes))
            .unwrap_or_default();

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_attrs = scope_metrics
                .scope
                .as_ref()
                .map(|s| attributes_to_map(&s.attributes))
                .unwrap_or_default();

            for metric in &scope_metrics.metrics {
                if metric.name.is_empty() {
                    continue;
                }

                let base_labels: HashMap<String, String> = scope_attrs
                    .iter()
                    .chain(resource_attrs.iter())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                use asap_otel_proto::tonic::metrics::v1::metric::Data;
                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                sketch_payloads.push(SketchPoint {
                                    name: metric.name.clone(),
                                    attr_name,
                                    labels: merge_point_attributes(&base_labels, &dp.attributes),
                                    timestamp_nanos: dp.time_unix_nano,
                                    payload,
                                });
                                continue;
                            }
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
                            let value = number_value_to_f64(&dp.value);
                            points.push(MetricPoint {
                                name: metric.name.clone(),
                                labels,
                                timestamp_nanos: dp.time_unix_nano,
                                value,
                            });
                        }
                    }
                    Some(Data::Sum(s)) => {
                        for dp in &s.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                sketch_payloads.push(SketchPoint {
                                    name: metric.name.clone(),
                                    attr_name,
                                    labels: merge_point_attributes(&base_labels, &dp.attributes),
                                    timestamp_nanos: dp.time_unix_nano,
                                    payload,
                                });
                                continue;
                            }
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
                            let value = number_value_to_f64(&dp.value);
                            points.push(MetricPoint {
                                name: metric.name.clone(),
                                labels,
                                timestamp_nanos: dp.time_unix_nano,
                                value,
                            });
                        }
                    }
                    Some(Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                sketch_payloads.push(SketchPoint {
                                    name: metric.name.clone(),
                                    attr_name,
                                    labels: merge_point_attributes(&base_labels, &dp.attributes),
                                    timestamp_nanos: dp.time_unix_nano,
                                    payload,
                                });
                                continue;
                            }
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
                            if let Some(sum) = dp.sum {
                                points.push(MetricPoint {
                                    name: format!("{}_sum", metric.name),
                                    labels: labels.clone(),
                                    timestamp_nanos: dp.time_unix_nano,
                                    value: sum,
                                });
                            }
                            points.push(MetricPoint {
                                name: format!("{}_count", metric.name),
                                labels: labels.clone(),
                                timestamp_nanos: dp.time_unix_nano,
                                value: dp.count as f64,
                            });
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                sketch_payloads.push(SketchPoint {
                                    name: metric.name.clone(),
                                    attr_name,
                                    labels: merge_point_attributes(&base_labels, &dp.attributes),
                                    timestamp_nanos: dp.time_unix_nano,
                                    payload,
                                });
                                continue;
                            }
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
                            if let Some(sum) = dp.sum {
                                points.push(MetricPoint {
                                    name: format!("{}_sum", metric.name),
                                    labels: labels.clone(),
                                    timestamp_nanos: dp.time_unix_nano,
                                    value: sum,
                                });
                            }
                            points.push(MetricPoint {
                                name: format!("{}_count", metric.name),
                                labels: labels.clone(),
                                timestamp_nanos: dp.time_unix_nano,
                                value: dp.count as f64,
                            });
                        }
                    }
                    Some(Data::Summary(sm)) => {
                        for dp in &sm.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                sketch_payloads.push(SketchPoint {
                                    name: metric.name.clone(),
                                    attr_name,
                                    labels: merge_point_attributes(&base_labels, &dp.attributes),
                                    timestamp_nanos: dp.time_unix_nano,
                                    payload,
                                });
                                continue;
                            }
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
                            points.push(MetricPoint {
                                name: format!("{}_sum", metric.name),
                                labels: labels.clone(),
                                timestamp_nanos: dp.time_unix_nano,
                                value: dp.sum,
                            });
                            points.push(MetricPoint {
                                name: format!("{}_count", metric.name),
                                labels: labels.clone(),
                                timestamp_nanos: dp.time_unix_nano,
                                value: dp.count as f64,
                            });
                        }
                    }
                    // Modified-OTLP first-class sketch metric variants. PR A
                    // vendors the proto and surfaces the new arms; PR B will
                    // populate them with per-variant decoders that route via
                    // WorkerMessage::AccumulatorInput. For now, drop with a
                    // debug log so the metric is visible in the ingest path.
                    Some(Data::Ddsketch(d)) => {
                        debug!(
                            "OTLP modified-proto Ddsketch received (metric={}, dps={}); decoder is PR B",
                            metric.name,
                            d.data_points.len()
                        );
                    }
                    Some(Data::Kllsketch(k)) => {
                        debug!(
                            "OTLP modified-proto Kllsketch received (metric={}, dps={}); decoder is PR B",
                            metric.name,
                            k.data_points.len()
                        );
                    }
                    Some(Data::Countsketch(c)) => {
                        debug!(
                            "OTLP modified-proto Countsketch received (metric={}, dps={}); decoder is PR B",
                            metric.name,
                            c.data_points.len()
                        );
                    }
                    Some(Data::Countminsketch(c)) => {
                        debug!(
                            "OTLP modified-proto Countminsketch received (metric={}, dps={}); decoder is PR B",
                            metric.name,
                            c.data_points.len()
                        );
                    }
                    Some(Data::Hllsketch(h)) => {
                        debug!(
                            "OTLP modified-proto Hllsketch received (metric={}, dps={}); decoder is PR B",
                            metric.name,
                            h.data_points.len()
                        );
                    }
                    None => {}
                }
            }
        }
    }
    (points, sketch_payloads)
}

fn merge_point_attributes(
    base: &HashMap<String, String>,
    attrs: &[asap_otel_proto::tonic::common::v1::KeyValue],
) -> HashMap<String, String> {
    let mut m = base.clone();
    for (k, v) in attributes_to_map(attrs) {
        m.insert(k, v);
    }
    m
}

fn number_value_to_f64(v: &Option<NumberValue>) -> f64 {
    match v {
        Some(NumberValue::AsDouble(x)) => *x,
        Some(NumberValue::AsInt(x)) => *x as f64,
        None => 0.0,
    }
}

fn any_value_to_string(v: &asap_otel_proto::tonic::common::v1::AnyValue) -> String {
    use asap_otel_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    match &v.value {
        Some(AnyValueVariant::StringValue(s)) => s.clone(),
        Some(AnyValueVariant::IntValue(i)) => i.to_string(),
        Some(AnyValueVariant::DoubleValue(d)) => d.to_string(),
        Some(AnyValueVariant::BoolValue(b)) => b.to_string(),
        Some(AnyValueVariant::BytesValue(bytes)) => format!("{:?}", bytes),
        _ => String::new(),
    }
}

fn attributes_to_map(
    attrs: &[asap_otel_proto::tonic::common::v1::KeyValue],
) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for kv in attrs {
        let key = kv.key.clone();
        let value = kv
            .value
            .as_ref()
            .map(any_value_to_string)
            .unwrap_or_default();
        if !key.is_empty() {
            m.insert(key, value);
        }
    }
    m
}

#[cfg(test)]
mod dispatcher_tests {
    use super::*;
    use crate::data_model::AggregateCore;
    use crate::precompute_operators::{DDSketchAccumulator, HllSketchAccumulator};
    use asap_sketchlib::sketches::ddsketch::DdSketch;
    use asap_sketchlib::sketches::hll::HllVariant;

    #[test]
    fn apply_modified_otlp_delta_bytes_ddsketch_round_trip() {
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use prost::Message;

        // Base sketch represents the last full snapshot the agent sent.
        let mut acc: Box<dyn AggregateCore> = Box::new(DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 2, 3], 0, 6, 12.0, 1.0, 3.0),
        });

        let bytes = PbDelta {
            buckets: vec![
                DdSketchBucketDelta {
                    index: 0,
                    d_count: 10,
                },
                DdSketchBucketDelta {
                    index: 2,
                    d_count: 20,
                },
            ],
            d_count: 30,
            d_sum: 70.0,
            new_min: 0.5,
            new_max: 5.0,
            min_changed: true,
            max_changed: true,
        }
        .encode_to_vec();

        apply_modified_otlp_delta_bytes(
            SketchKind::DdSketch,
            ENCODING_PROTO_DELTA,
            &mut acc,
            &bytes,
        )
        .expect("apply ok");

        let dd = acc.as_any().downcast_ref::<DDSketchAccumulator>().unwrap();
        assert_eq!(dd.inner.store_counts, vec![11, 2, 23]);
        assert_eq!(dd.inner.count, 36);
        assert_eq!(dd.inner.min, 0.5);
        assert_eq!(dd.inner.max, 5.0);
    }

    #[test]
    fn apply_modified_otlp_delta_bytes_hll_round_trip() {
        use asap_otel_proto::sketchlib::v1::{HllDelta as PbDelta, HllRegisterUpdate};
        use prost::Message;

        let mut acc: Box<dyn AggregateCore> =
            Box::new(HllSketchAccumulator::new(HllVariant::Regular, 2));
        acc.as_any_mut()
            .downcast_mut::<HllSketchAccumulator>()
            .unwrap()
            .inner
            .registers = vec![1, 5, 3, 7];

        let bytes = PbDelta {
            updates: vec![
                HllRegisterUpdate { index: 0, value: 4 },
                HllRegisterUpdate { index: 2, value: 6 },
            ],
        }
        .encode_to_vec();

        apply_modified_otlp_delta_bytes(SketchKind::Hll, ENCODING_PROTO_DELTA, &mut acc, &bytes)
            .expect("apply ok");

        let hll = acc.as_any().downcast_ref::<HllSketchAccumulator>().unwrap();
        assert_eq!(hll.inner.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn apply_rejects_wrong_accumulator_type() {
        let mut acc: Box<dyn AggregateCore> =
            Box::new(HllSketchAccumulator::new(HllVariant::Regular, 2));
        let err = apply_modified_otlp_delta_bytes(
            SketchKind::DdSketch,
            ENCODING_PROTO_DELTA,
            &mut acc,
            &[0u8; 4],
        )
        .expect_err("expected type-mismatch error")
        .to_string();
        assert!(err.contains("not a DDSketchAccumulator"));
    }

    #[test]
    fn apply_rejects_full_state_encoding() {
        let mut acc: Box<dyn AggregateCore> = Box::new(DDSketchAccumulator::new(0.01));
        let err =
            apply_modified_otlp_delta_bytes(SketchKind::DdSketch, ENCODING_PROTO, &mut acc, &[])
                .expect_err("expected full-state-rejection error")
                .to_string();
        assert!(err.contains("full-state frame"));
    }

    #[test]
    fn decode_rejects_delta_encoding_with_helpful_message() {
        let err = match decode_modified_otlp_sketch_bytes(
            SketchKind::DdSketch,
            ENCODING_PROTO_DELTA,
            &[],
        ) {
            Ok(_) => panic!("expected PROTO_DELTA to be rejected by full-state decoder"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("apply_modified_otlp_delta_bytes"));
    }
}

/// Phase 4 — sid-resolution gate tests. Construct an OTLP DDSketch
/// Export with one DataPoint per scenario, run it through
/// `route_modified_otlp_sketches_to_precompute`, and assert on the
/// returned `unknown_series_ids` plus the SeriesIdResolver / SketchIndex
/// state on the shared IngestState.
#[cfg(test)]
mod sid_resolution_tests {
    use super::*;
    use crate::data_model::{HotReloadStreamingConfig, StreamingConfig};
    use crate::drivers::ingest::series_resolver::SeriesIdResolver;
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::stores::sketch_db::SchemaRegistry;
    use crate::stores::sketch_db::sketch_index::SketchIndex;
    use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use asap_otel_proto::tonic::common::v1::{any_value::Value as AnyVal, AnyValue, KeyValue};
    use asap_otel_proto::tonic::metrics::v1::{
        metric::Data, DdSketch as PbDDSketch, DdSketchDataPoint, Metric as PbMetric,
        ResourceMetrics, ScopeMetrics,
    };
    use std::sync::Arc;
    use tokio::sync::mpsc;

    async fn make_state() -> (Arc<IngestState>, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel(1024);
        let router = SeriesRouter::new(vec![tx]);
        let streaming = StreamingConfig::new(std::collections::HashMap::new());
        let hot_reload = HotReloadStreamingConfig::new(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            schemas,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(SeriesIdResolver::new()),
            sketch_index: Arc::new(SketchIndex::new()),
        });
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        (state, drain)
    }

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(AnyVal::StringValue(v.to_string())),
            }),
        }
    }

    fn build_request(metric_name: &str, dp: DdSketchDataPoint) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![PbMetric {
                        name: metric_name.to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(Data::Ddsketch(PbDDSketch {
                            data_points: vec![dp],
                            aggregation_temporality: 0,
                            relative_accuracy: 0.01,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn fresh_sid_minted_when_sender_supplies_zero_with_attrs() {
        let (state, drain) = make_state().await;
        let dp = DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 11_000_000,
            sketch: vec![1, 2, 3],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0,
        };
        let req = build_request("http_latency_ms", dp);

        let unknown = route_modified_otlp_sketches_to_precompute(&req, &state).await;
        assert!(unknown.is_empty(), "no unknown sids on a fresh-attrs DP");
        assert_eq!(state.series_resolver.len(), 1, "resolver minted one sid");
        assert_eq!(
            state.sketch_index.instance_count(),
            1,
            "SketchIndex registered one instance"
        );

        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn unknown_sid_with_empty_attrs_is_returned_in_response() {
        let (state, drain) = make_state().await;
        // sid != 0, no attrs — resolver doesn't know it; should land in
        // unknown_sids and the DP must be dropped (no instance registered).
        let dp = DdSketchDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 5_000_000,
            sketch: vec![9],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 7777,
        };
        let req = build_request("http_latency_ms", dp);

        let unknown = route_modified_otlp_sketches_to_precompute(&req, &state).await;
        assert_eq!(unknown, vec![7777]);
        assert_eq!(state.series_resolver.len(), 0);
        assert_eq!(state.sketch_index.instance_count(), 0);

        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn sid_attrs_disagreement_signals_stale_sid_but_uses_resolved_value() {
        let (state, drain) = make_state().await;
        // First, mint the resolver's view by sending sid=0 with attrs.
        let dp_seed = DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 11_000_000,
            sketch: vec![1],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0,
        };
        let _ = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_seed),
            &state,
        )
        .await;
        let resolved_sid = state.series_resolver.lookup(
            "http_latency_ms",
            &crate::drivers::ingest::canonical_attrs_fingerprint(&[("zone", "z0")]),
        );
        let resolved_sid = resolved_sid.expect("seed mints");

        // Now arrive with the same attrs but a STALE sid.
        let stale = resolved_sid.wrapping_add(123);
        let dp_disagree = DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 12_000_000,
            sketch: vec![2],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: stale,
        };
        let unknown = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_disagree),
            &state,
        )
        .await;
        assert_eq!(unknown, vec![stale], "stale sid should be signalled");
        // Cache stays at the originally-resolved value — the second call
        // returns the same sid via the canonical fingerprint.
        let still_resolved = state
            .series_resolver
            .lookup(
                "http_latency_ms",
                &crate::drivers::ingest::canonical_attrs_fingerprint(&[("zone", "z0")]),
            )
            .expect("still cached");
        assert_eq!(still_resolved, resolved_sid);

        drop(state);
        let _ = drain.await;
    }
}
