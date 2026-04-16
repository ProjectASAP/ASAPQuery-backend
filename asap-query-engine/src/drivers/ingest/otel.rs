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
}

/// OTLP receiver that accepts metrics via gRPC and HTTP.
pub struct OtlpReceiver {
    config: OtlpReceiverConfig,
    ingest_state: Option<Arc<IngestState>>,
}

impl OtlpReceiver {
    /// Construct a receiver without a backend. Metrics are parsed and
    /// logged but not stored — useful for smoke-testing the OTLP pipe.
    pub fn new(config: OtlpReceiverConfig) -> Self {
        Self {
            config,
            ingest_state: None,
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
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.grpc_port));
        let http_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.http_port));

        let shared = Arc::new(OtlpSharedState {
            ingest_state: self.ingest_state.clone(),
        });

        let grpc_svc = MetricsServiceImpl {
            shared: shared.clone(),
        };
        let grpc_svc =
            asap_otel_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer::new(
                grpc_svc,
            );

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
        if let Some(state) = &self.shared.ingest_state {
            route_otlp_to_precompute(&req, state).await;
            route_modified_otlp_sketches_to_precompute(&req, state).await;
        }
        debug!("OTLP sending response via gRPC");
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
            // Modified-OTLP collector hands out stable series descriptors via
            // this field; not yet wired (PR B will populate it when the
            // backend learns to mint series_ids).
            series_assignments: Vec::new(),
        }))
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
    if let Some(state) = &shared.ingest_state {
        route_otlp_to_precompute(&req, state).await;
        route_modified_otlp_sketches_to_precompute(&req, state).await;
    }
    debug!("OTLP sending response via HTTP");
    Ok(Json(serde_json::json!({"rejected": 0})))
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

    // Build (agg_id, group_key) → Vec<(series_key, ts_ms, value)> for raw points.
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();
    let mut raw_matched = 0usize;
    let mut raw_unmatched = 0usize;

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
) {
    use asap_otel_proto::tonic::metrics::v1::metric::Data;

    let ingest_received_at = Instant::now();
    let snap = ingest_state.config_snapshot();
    let agg_configs = snap.get_all_aggregation_configs();
    let mut messages: Vec<WorkerMessage> = Vec::new();
    let mut routed = 0usize;
    let mut decoded_failed = 0usize;
    let mut unconfigured = 0usize;

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
                    Some(Data::Ddsketch(d)) => d
                        .data_points
                        .iter()
                        .map(|dp| ModifiedOtlpSketchDp {
                            kind: SketchKind::DdSketch,
                            attrs: merge_point_attributes(&base_labels, &dp.attributes),
                            time_unix_nano: dp.time_unix_nano,
                            sketch: dp.sketch.clone(),
                            encoding: dp.encoding,
                        })
                        .collect(),
                    Some(Data::Kllsketch(k)) => k
                        .data_points
                        .iter()
                        .map(|dp| ModifiedOtlpSketchDp {
                            kind: SketchKind::Kll,
                            attrs: merge_point_attributes(&base_labels, &dp.attributes),
                            time_unix_nano: dp.time_unix_nano,
                            sketch: dp.sketch.clone(),
                            encoding: dp.encoding,
                        })
                        .collect(),
                    Some(Data::Countsketch(c)) => c
                        .data_points
                        .iter()
                        .map(|dp| ModifiedOtlpSketchDp {
                            kind: SketchKind::CountSketch,
                            attrs: merge_point_attributes(&base_labels, &dp.attributes),
                            time_unix_nano: dp.time_unix_nano,
                            sketch: dp.sketch.clone(),
                            encoding: dp.encoding,
                        })
                        .collect(),
                    Some(Data::Countminsketch(c)) => c
                        .data_points
                        .iter()
                        .map(|dp| ModifiedOtlpSketchDp {
                            kind: SketchKind::CountMin,
                            attrs: merge_point_attributes(&base_labels, &dp.attributes),
                            time_unix_nano: dp.time_unix_nano,
                            sketch: dp.sketch.clone(),
                            encoding: dp.encoding,
                        })
                        .collect(),
                    Some(Data::Hllsketch(h)) => h
                        .data_points
                        .iter()
                        .map(|dp| ModifiedOtlpSketchDp {
                            kind: SketchKind::Hll,
                            attrs: merge_point_attributes(&base_labels, &dp.attributes),
                            time_unix_nano: dp.time_unix_nano,
                            sketch: dp.sketch.clone(),
                            encoding: dp.encoding,
                        })
                        .collect(),
                    _ => continue,
                };

                for dp in dps {
                    let series_key = format_series_key(&metric.name, &dp.attrs);
                    let ts_ms = (dp.time_unix_nano / 1_000_000) as i64;

                    let accumulator: Box<dyn AggregateCore> =
                        match decode_modified_otlp_sketch_bytes(dp.kind, dp.encoding, &dp.sketch) {
                            Ok(acc) => acc,
                            Err(e) => {
                                decoded_failed += 1;
                                debug!(
                                "OTLP modified-proto sketch decode failed (metric={}, kind={:?}, encoding={}, bytes={}): {} — falling through to §5.2 fallback",
                                metric.name,
                                dp.kind,
                                dp.encoding,
                                dp.sketch.len(),
                                e
                            );
                                continue;
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
                        let group_key = IngestState::extract_group_key_for(&series_key, config);
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
}

/// Sketch family carried by a modified-OTLP `*SketchDataPoint`. Used by
/// the encoding dispatcher in `decode_modified_otlp_sketch_bytes`.
#[derive(Debug, Clone, Copy)]
enum SketchKind {
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
    // enum. All five sketch variants share the same wire tag layout for
    // tags 1–4, so we can match on one set of constants here:
    //
    //   1  — ENCODING_PROTO          (PR B / PR C: sketchlib proto-encoded)
    //   2  — ENCODING_PROTO_DELTA    (delta transmission; deferred — falls
    //                                 through to §5.2 fallback)
    //   3  — ENCODING_MSGPACK        (PR I: cross-language sketch-core
    //                                 msgpack wire format — this dispatcher)
    //   4  — ENCODING_MSGPACK_DELTA  (deferred same as PROTO_DELTA)
    const ENCODING_PROTO: i32 = 1;
    const ENCODING_MSGPACK: i32 = 3;

    match encoding {
        ENCODING_PROTO => match kind {
            SketchKind::CountMin => Ok(Box::new(
                CountMinSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchKind::CountSketch => Ok(Box::new(
                CountSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchKind::Kll => Ok(Box::new(
                DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchKind::DdSketch => Ok(Box::new(DDSketchAccumulator::from_sketchlib_proto_bytes(
                bytes,
            )?)),
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
        _ => Err(format!(
            "modified-OTLP sketch encoding {encoding} not yet supported \
             (PROTO = 1 and MSGPACK = 3 are wired; PROTO_DELTA = 2 and \
             MSGPACK_DELTA = 4 are deferred — caller falls through to §5.2 \
             fallback)"
        )
        .into()),
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
