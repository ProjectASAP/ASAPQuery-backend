//! OTLP ingest driver.
//!
//! Accepts OTLP metrics via gRPC (4317) and HTTP (4318, POST /v1/metrics),
//! parses ExportMetricsServiceRequest, logs counts at DEBUG, and leaves
//! handoff to precompute engine as TODO.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope};
use axum::{body::Bytes, extract::State, routing::post, Json, Router};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::data_model::{KeyByLabelValues, PrecomputedOutput, StreamingConfig};
use crate::precompute_operators::SketchEnvelopeAccumulator;
use crate::stores::Store;

/// Configuration for the OTLP receiver.
#[derive(Debug, Clone)]
pub struct OtlpReceiverConfig {
    pub grpc_port: u16,
    pub http_port: u16,
}

/// Shared state passed to both gRPC and HTTP handlers.
#[derive(Clone)]
struct OtlpSharedState {
    store: Arc<dyn Store>,
    streaming_config: Arc<StreamingConfig>,
}

/// OTLP receiver that accepts metrics via gRPC and HTTP.
pub struct OtlpReceiver {
    config: OtlpReceiverConfig,
    store: Arc<dyn Store>,
    streaming_config: Arc<StreamingConfig>,
}

impl OtlpReceiver {
    pub fn new(
        config: OtlpReceiverConfig,
        store: Arc<dyn Store>,
        streaming_config: Arc<StreamingConfig>,
    ) -> Self {
        Self {
            config,
            store,
            streaming_config,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.grpc_port));
        let http_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.http_port));

        let shared = Arc::new(OtlpSharedState {
            store: self.store.clone(),
            streaming_config: self.streaming_config.clone(),
        });

        let grpc_svc = MetricsServiceImpl {
            shared: shared.clone(),
        };
        let grpc_svc =
            opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer::new(
                grpc_svc,
            );

        let app = Router::new()
            .route("/v1/metrics", post(handle_otlp_http))
            .with_state(shared.clone());

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
        process_otlp_request(&req, "gRPC", &self.shared);
        debug!("OTLP sending response via gRPC");
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
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
    process_otlp_request(&req, "HTTP", &shared);
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

/// A parsed sketch data point: metric name, attribute key, raw payload bytes,
/// timestamp in nanoseconds, and label map from the data point.
#[derive(Debug)]
pub struct SketchPoint {
    pub metric_name: String,
    pub attr_name: String,
    pub payload: Vec<u8>,
    pub timestamp_nanos: u64,
    pub labels: HashMap<String, String>,
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
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
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

/// Build a reverse lookup from metric name -> aggregation_id using the
/// streaming config.  Called once per request (configs are small).
fn build_metric_to_aggregation_id(streaming_config: &StreamingConfig) -> HashMap<String, u64> {
    let mut map = HashMap::new();
    for (agg_id, config) in streaming_config.get_all_aggregation_configs() {
        map.entry(config.metric.clone()).or_insert(*agg_id);
    }
    map
}

/// Stable hash of a metric name to use as aggregation_id when the metric is
/// not found in the streaming config.  Uses the same FNV-1a 64-bit hash that
/// other parts of the pipeline use for deterministic IDs.
fn hash_metric_name(name: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

fn process_otlp_request(
    request: &ExportMetricsServiceRequest,
    transport: &str,
    shared: &OtlpSharedState,
) {
    let resource_count = request.resource_metrics.len();
    let total_points = otlp_to_record_count(request);
    if resource_count > 0 || total_points > 0 {
        debug!(
            "OTLP ingest: received {} resource metrics, {} total data points (transport={})",
            resource_count, total_points, transport
        );
    }

    let (points, sketch_points) = otlp_to_metric_points_and_sketches(request);

    for sp in &sketch_points {
        log_sketch_envelope_type(&sp.attr_name, &sp.payload, &sp.metric_name);
    }
    if !sketch_points.is_empty() {
        debug!(
            "OTLP Sketch Payload Flow: received {} sketch payload(s), decoded successfully",
            sketch_points.len()
        );
    }

    // --- Store sketch data into SimpleMapStore ---
    if !sketch_points.is_empty() {
        let metric_to_agg = build_metric_to_aggregation_id(&shared.streaming_config);
        let mut batch: Vec<(PrecomputedOutput, Box<dyn crate::data_model::AggregateCore>)> =
            Vec::with_capacity(sketch_points.len());

        for sp in &sketch_points {
            // Resolve aggregation_id: prefer streaming config, fall back to hash.
            let aggregation_id = metric_to_agg
                .get(&sp.metric_name)
                .copied()
                .unwrap_or_else(|| {
                    let h = hash_metric_name(&sp.metric_name);
                    warn!(
                        "OTLP ingest: metric '{}' not found in streaming config, using hash {} as aggregation_id",
                        sp.metric_name, h
                    );
                    h
                });

            // Convert nanoseconds to milliseconds for store timestamps.
            let ts_ms = sp.timestamp_nanos / 1_000_000;

            // Build KeyByLabelValues from the label map (sorted for determinism).
            let mut sorted_labels: Vec<(&String, &String)> = sp.labels.iter().collect();
            sorted_labels.sort_by_key(|(k, _)| *k);
            let label_values: Vec<String> = sorted_labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            let key = if label_values.is_empty() {
                None
            } else {
                Some(KeyByLabelValues::new_with_labels(label_values))
            };

            let output = PrecomputedOutput::new(ts_ms, ts_ms, key, aggregation_id);

            match SketchEnvelopeAccumulator::from_proto_bytes(sp.payload.clone()) {
                Ok(accumulator) => {
                    debug!(
                        "OTLP ingest: storing sketch metric='{}' agg_id={} ts_ms={} type={}",
                        sp.metric_name, aggregation_id, ts_ms, accumulator.sketch_type
                    );
                    batch.push((output, Box::new(accumulator)));
                }
                Err(e) => {
                    error!(
                        "OTLP ingest: failed to decode SketchEnvelope for metric '{}': {}",
                        sp.metric_name, e
                    );
                }
            }
        }

        if !batch.is_empty() {
            let batch_len = batch.len();
            match shared.store.insert_precomputed_output_batch(batch) {
                Ok(_) => {
                    info!(
                        "OTLP ingest: stored {} sketch precompute(s) (transport={})",
                        batch_len, transport
                    );
                }
                Err(e) => {
                    error!(
                        "OTLP ingest: failed to insert sketch batch into store: {}",
                        e
                    );
                }
            }
        }
    }

    // --- Log raw metric points (non-sketch) ---
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

                use opentelemetry_proto::tonic::metrics::v1::metric::Data;
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
    let mut sketch_points = Vec::new();
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

                use opentelemetry_proto::tonic::metrics::v1::metric::Data;
                match &metric.data {
                    Some(Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            if let Some((attr_name, payload)) =
                                get_sketch_payload_from_attrs(&dp.attributes)
                            {
                                let labels = merge_point_attributes(&base_labels, &dp.attributes);
                                sketch_points.push(SketchPoint {
                                    metric_name: metric.name.clone(),
                                    attr_name,
                                    payload,
                                    timestamp_nanos: dp.time_unix_nano,
                                    labels,
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
                                let labels = merge_point_attributes(&base_labels, &dp.attributes);
                                sketch_points.push(SketchPoint {
                                    metric_name: metric.name.clone(),
                                    attr_name,
                                    payload,
                                    timestamp_nanos: dp.time_unix_nano,
                                    labels,
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
                                let labels = merge_point_attributes(&base_labels, &dp.attributes);
                                sketch_points.push(SketchPoint {
                                    metric_name: metric.name.clone(),
                                    attr_name,
                                    payload,
                                    timestamp_nanos: dp.time_unix_nano,
                                    labels,
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
                                let labels = merge_point_attributes(&base_labels, &dp.attributes);
                                sketch_points.push(SketchPoint {
                                    metric_name: metric.name.clone(),
                                    attr_name,
                                    payload,
                                    timestamp_nanos: dp.time_unix_nano,
                                    labels,
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
                                let labels = merge_point_attributes(&base_labels, &dp.attributes);
                                sketch_points.push(SketchPoint {
                                    metric_name: metric.name.clone(),
                                    attr_name,
                                    payload,
                                    timestamp_nanos: dp.time_unix_nano,
                                    labels,
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
                    None => {}
                }
            }
        }
    }
    (points, sketch_points)
}

fn merge_point_attributes(
    base: &HashMap<String, String>,
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
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

fn any_value_to_string(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
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
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
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
