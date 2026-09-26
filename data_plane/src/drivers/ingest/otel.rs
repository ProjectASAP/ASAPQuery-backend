//! OTLP ingest driver.
//!
//! Accepts OTLP metrics via gRPC (4317) and HTTP (4318, POST /v1/metrics).
//! Parses `ExportMetricsServiceRequest`, and — when wired to a precompute
//! engine via [`OtlpReceiver::with_ingest_state`] — routes both raw metric
//! points and pre-built sketches through the precompute engine's worker
//! pool. The precompute engine then performs window-aligned aggregation
//! per `InstalledPrecomputePlan` and writes results to `SketchStore`.
//!
//! Architectural flow:
//! ```text
//!   DataCollector OTel collector
//!     → OTLP gRPC/HTTP (this receiver)
//!     → precompute engine ingest router
//!     → workers (per (agg_id, group_key) panes)
//!     → StoreOutputSink → SketchStore
//!     → query engine
//! ```
//!
//! Labels from the OTLP wire format are preserved all the way into
//! `KeyByLabelValues` via the standard `series_key` → grouping-label
//! extraction used by the Prometheus/VictoriaMetrics ingest paths.

use std::collections::HashMap;
use std::io::Read;

use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::IngestState;
use crate::query_engines::routing::FreshnessProbeCache;
use crate::storage_engines::types::AggregateCore;
use asap_otel_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use asap_otel_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use asap_otel_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use asap_physical_operators::summary_kernels::sketch_envelope::SketchEnvelopeAccumulator;
use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope};
use asap_sketchlib::MessagePackCodec;
use axum::{body::Bytes, extract::State, routing::post, Json, Router};
use flate2::read::GzDecoder;
use planner_types::post_asap::SketchAlgorithm;
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
    /// logged but not stored — useful for diagnosing the OTLP pipe.
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
            .max_decoding_message_size(64 * 1024 * 1024)
            // Accept gzip-compressed request bodies so the `asap-gzip`
            // arm's agent (`otlp/backend: compression: gzip`) can
            // shrink the per-window sketch/aggregate payload on the
            // wire. This is the matched-codec partner of the OTLP→VM
            // `b1` baseline (also gzip): comparing b1 vs asap-gzip
            // isolates the aggregation gain at a fixed codec.
            // tonic negotiates per-RPC via the grpc-encoding header, so
            // uncompressed agents (the plain `asap` arm,
            // `compression: none`, matched against the `b0` baseline)
            // are unaffected — this only enables the server to decode
            // gzip when an agent chooses to send it. The control_plane
            // RuntimeSamples server already does the same
            // (control_plane/src/runtime_samples.rs).
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

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
        // PERF-1 — parse the OTLP request ONCE here and hand the parsed
        // points/sketches to the raw-path consumers
        // (process_otlp_request / capture_freshness / route_otlp_to_precompute)
        // instead of each re-walking the protobuf. The first-class
        // sketch path (`route_modified_otlp_sketches_to_precompute`)
        // still walks the raw request because it reads the sketch
        // DataPoint variants this parse intentionally skips.
        let (points, sketch_payloads) = otlp_to_metric_points_and_sketches(&req);
        process_otlp_request(&req, &points, &sketch_payloads, "gRPC");
        if let Some(cache) = &self.shared.probe_cache {
            capture_freshness_probe_samples(&points, cache);
        }
        let mut outcome = IngestOutcome::default();
        if let Some(state) = &self.shared.ingest_state {
            route_otlp_to_precompute(&points, &sketch_payloads, state).await;
            outcome = route_modified_otlp_sketches_to_precompute(&req, state)
                .await
                .map_err(Status::invalid_argument)?;
        }
        debug!("OTLP sending response via gRPC");
        Ok(Response::new(ExportMetricsServiceResponse {
            // A successful RPC is the transport ACK: every summary frame
            // passed the active-plan gate and was applied. Contract failures
            // return a non-OK gRPC status before any frame is written.
            partial_success: None,
            // Sid bindings the sender should cache. Each entry maps an
            // `attributes_fingerprint` to the canonical sid the backend's
            // `SeriesIdResolver` minted (or returned from its cache). The
            // sender's local dictionary keys on `fingerprint`, so it can
            // refresh stale entries and pick up brand-new ones from this
            // field without a separate `ResolveSeriesIDs` round-trip.
            series_assignments: outcome.series_assignments,
            // backend signals senders to evict cached sids here
            // when this Export carried a sid the resolver does not
            // recognize (sid-cache divergence — e.g. after a backend
            // restart without persistence, or when the sender's sid
            // disagrees with the resolved sid for the same attrs).
            unknown_series_ids: outcome.unknown_series_ids,
        }))
    }

    async fn resolve_series_i_ds(
        &self,
        request: Request<asap_otel_proto::tonic::collector::metrics::v1::ResolveSeriesIDsRequest>,
    ) -> Result<
        Response<asap_otel_proto::tonic::collector::metrics::v1::ResolveSeriesIDsResponse>,
        Status,
    > {
        use asap_otel_proto::tonic::collector::metrics::v1::ResolveSeriesIDsResponse;

        // Pre-resolve handshake is structurally redundant with the
        // canonical Export path under Interpretation B (sid identity is
        // `(metric, attrs_fingerprint, agg_kind_canonical)` and
        // `agg_kind` isn't carried in `SeriesQuery`). On the first
        // Export with attrs, the backend resolves the correct sid and
        // echoes it back via `ExportMetricsServiceResponse.series_assignments`
        // — that's the canonical channel, and it also handles every
        // cache-divergence failure mode (proto comment at
        // `ExportMetricsServiceResponse.unknown_series_ids:100-104`).
        //
        // The RPC is kept as a stable surface so older patched
        // exporters that still call it don't see `Unimplemented`. The
        // returned `assignments` vec is empty; the agent's local cache
        // stays cold and the first real Export populates it.
        //
        // Follow-up: drop the RPC entirely (proto change), OR extend
        // `SeriesQuery` to carry `agg_kind_canonical` so this can
        // perform a real pre-resolve. Today's stub is the no-harm path.
        let req = request.into_inner();
        if !req.queries.is_empty() {
            warn!(
                queries = req.queries.len(),
                "ResolveSeriesIDs RPC called with non-empty batch; \
                 returning empty assignments — sid identity now \
                 includes agg_kind which this RPC doesn't carry. \
                 Agent will mint on first Export with attrs.",
            );
        }
        Ok(Response::new(ResolveSeriesIDsResponse {
            assignments: Vec::new(),
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
    // PERF-1 — parse once, share the result across the raw-path
    // consumers (see the gRPC `export` path for the rationale).
    let (points, sketch_payloads) = otlp_to_metric_points_and_sketches(&req);
    process_otlp_request(&req, &points, &sketch_payloads, "HTTP");
    if let Some(cache) = &shared.probe_cache {
        capture_freshness_probe_samples(&points, cache);
    }
    let mut outcome = IngestOutcome::default();
    if let Some(state) = &shared.ingest_state {
        route_otlp_to_precompute(&points, &sketch_payloads, state).await;
        outcome = route_modified_otlp_sketches_to_precompute(&req, state)
            .await
            .map_err(|error| (axum::http::StatusCode::UNPROCESSABLE_ENTITY, error))?;
    }
    debug!("OTLP sending response via HTTP");
    // HTTP OTLP exporters don't generally read `series_assignments`
    // back the way the gRPC path does (the OTLP/HTTP spec keeps the
    // response shape minimal), so we surface assignments only as a
    // count for observability. Senders that need the bindings should
    // use the gRPC transport. Eviction signals stay first-class — they
    // are the universal recovery primitive (see proto comment on
    // `ExportMetricsServiceResponse.unknown_series_ids`).
    Ok(Json(serde_json::json!({
        "rejected": 0,
        "unknown_series_ids": outcome.unknown_series_ids,
        "series_assignments_count": outcome.series_assignments.len(),
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

/// Render OTLP `(name, labels)` into the canonical PromQL-style series
/// key `metric{k1="v1",k2="v2"}` used everywhere in the data plane.
///
/// Values are wrapped in double quotes and embedded `"`, `\`, `\n` are
/// escaped per the PromQL lexer rules. This matches:
///
///   * `parse_labels_from_series_key` in `precompute_engine/worker.rs`
///     (the inverse — expects `key="value"`)
///   * `render_series_key` in `storage_engines/sketch_db/backfill/
///     prometheus_reader.rs` (the other producer of this shape)
///   * `RawSample.labels`' documented shape (`metric{k="v",...}`)
///   * `sample_matches` in the backfill raw reader (strips `"` when
///     parsing)
///
/// Pre-fix this helper emitted **unquoted** values (`k=v`), which the
/// parser silently rejected → `IngestState::extract_group_key_for`
/// returned `""` for every OTLP wire-format input, and the keyed
/// dispatch path inside `apply_sample` saw empty aggregated keys for
/// every OTLP sample. The bug was discovered while implementing PR
/// #284 (B7.6 ingest sid rekey); that PR side-stepped it by reading
/// `point.labels` directly, but emit-time `KeyByLabelValues`
/// content elsewhere depended on the roundtrip working — hence this
/// fix. See the regression test
/// `format_series_key_roundtrips_through_parse_labels` in
/// `precompute_engine/worker.rs`.
fn format_series_key(name: &str, labels: &HashMap<String, String>) -> String {
    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    let labels_str = pairs
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", name, labels_str)
}

/// Escape a label value for the PromQL series-key format. Mirrors
/// `storage_engines::sketch_db::backfill::prometheus_reader::
/// escape_label_value` — both producers must stay in lockstep so the
/// parser in `precompute_engine::worker::parse_labels_from_series_key`
/// sees a consistent shape regardless of which ingest path emitted
/// the series key.
fn escape_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
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
fn capture_freshness_probe_samples(points: &[MetricPoint], cache: &FreshnessProbeCache) {
    let mut updated = 0usize;
    for point in points {
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

fn process_otlp_request(
    request: &ExportMetricsServiceRequest,
    points: &[MetricPoint],
    sketch_payloads: &[SketchPoint],
    transport: &str,
) {
    let resource_count = request.resource_metrics.len();
    let total_points = otlp_to_record_count(request);
    if resource_count > 0 || total_points > 0 {
        debug!(
            "OTLP ingest: received {} resource metrics, {} total data points (transport={})",
            resource_count, total_points, transport
        );
    }

    for sketch in sketch_payloads {
        log_sketch_envelope_type(&sketch.attr_name, &sketch.payload, &sketch.name);
    }
    if !sketch_payloads.is_empty() {
        debug!(
            "OTLP Sketch Payload Flow: received {} sketch payload(s), decoded successfully",
            sketch_payloads.len()
        );
    }

    let mut by_series: HashMap<String, usize> = HashMap::new();
    for point in points {
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
/// Log a per-driver `HashMap<agg_id, count>` of §6.3 write-barrier
/// drops. Post-schema-retirement the agg_id-keyed
/// `IngestState::record_barrier_drop` counter is gone — the
/// sid-level barrier inside `SketchStore::ingest_precompute_for_agg_config`
/// silently rejects retired-sid writes without crossing this
/// observer. The function is kept (callers still hand it an empty
/// map) so the call shape doesn't churn; if the map is non-empty
/// it emits a single debug log for forensic visibility.
fn flush_barrier_drops(_state: &IngestState, drops: &HashMap<u64, u64>, driver_tag: &'static str) {
    if drops.is_empty() {
        return;
    }
    let total: u64 = drops.values().sum();
    debug!(
        driver = driver_tag,
        total_dropped = total,
        by_agg_id = ?drops,
        "§6.3 write barrier dropped OTLP samples (agg is retired/expired)"
    );
}

/// Resolve the bucket sid (and `policy_fp`) for a single data point
/// against a single matching `PrecomputeMaterialization`.
///
/// B7.6 — sid is the bucket identity in the precompute engine; this
/// helper folds `(config, grouping-label-values)` into a single u64 via
/// `SeriesIdResolver`. Same `(metric, grouping-label-values, agg_kind)`
/// always returns the same sid, so distinct data points that share the
/// same group bucket land in the same `GroupSamples` /
/// `AccumulatorInput` message — the GROUP-BY semantics the legacy
/// `(agg_id, group_key)` tuple expressed.
///
/// The "attrs" passed to the resolver are the GROUPING-LABEL projection
/// of the wire labels (NOT the full label set) — otherwise every
/// distinct `(rack, node, pod)` tuple under a `grouping_labels=[zone]`
/// policy would mint its own sid and never roll up.
///
/// Configured ingest shares the policy-aware physical identity used by the
/// live storage sink and backfill. Unbound modified-OTLP sketches retain
/// their separate wire-level identity protocol.
fn resolve_bucket_sid_for_agg_config(
    ingest_state: &Arc<IngestState>,
    config: &asap_types::aggregation_config::PrecomputeMaterialization,
    point_labels: &HashMap<String, String>,
    captured_generation: Option<&asap_types::sds::CatalogGeneration>,
) -> Result<(u64, asap_types::PolicyFingerprint), String> {
    if !config.population_key_encoding.is_legacy() {
        return Err("canonical population requires typed OTLP label propagation".into());
    }
    let grouping_pairs: Vec<(&str, &str)> = config
        .grouping_labels
        .iter()
        .map(|name| {
            let v = point_labels.get(name).map(|s| s.as_str()).unwrap_or("");
            (name.as_str(), v)
        })
        .collect();
    let fp = crate::drivers::ingest::population_attrs_fingerprint(
        config.population_key_encoding,
        &grouping_pairs,
    )?;
    let sid = ingest_state.summary_store.resolve_output_storage_handle(
        &ingest_state.series_resolver,
        config.policy_fingerprint().into(),
        &fp,
        captured_generation,
    )?;
    let policy_fp = asap_types::PolicyFingerprint(config.policy_fp_u64());
    Ok((sid, policy_fp))
}

async fn route_otlp_to_precompute(
    points: &[MetricPoint],
    sketch_payloads: &[SketchPoint],
    ingest_state: &Arc<IngestState>,
) {
    let ingest_received_at = Instant::now();

    // Snapshot the latest agg_configs from the hot-reload handle so
    // new aggregations are visible without restart.
    let active_physical_plan_snapshot = ingest_state.active_physical_plan_snapshot();
    let catalog_generation = active_physical_plan_snapshot
        .as_ref()
        .and_then(|plan| plan.precompute_plan.summary_catalog.clone())
        .map(Arc::new);
    let snap = active_physical_plan_snapshot
        .as_ref()
        .map(|plan| plan.installed_precompute_plan.clone())
        .unwrap_or_else(|| ingest_state.config_snapshot());
    let agg_configs = snap.materializations();
    // Reconcile sid lifecycle using the current streaming-config snapshot.
    // The store enforces the write barrier for retired and expired instances.
    // A config-Arc identity check skips catalog scans while the config is unchanged.
    let _ = crate::storage_engines::sketch_db::lifecycle::reconcile_if_config_changed(
        ingest_state.summary_store.as_ref(),
        &snap,
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    );

    // B7.6 — bucket by `sid` instead of `(agg_id, group_key)`. The
    // grouping label values fold into the sid via the
    // `(metric, attrs_fp, agg_kind_canonical)` identity contract on
    // `SeriesIdResolver`: same (config, grouping-label-values) → same
    // sid → same bucket. We still carry `policy_fp` and `group_key`
    // alongside the sid in the WorkerMessage so the worker can resolve
    // the source config and render emit-time labels without consulting
    // the sid → attrs reverse mapping.
    type BucketTuple = (
        u64,
        asap_types::PolicyFingerprint,
        Arc<crate::precompute_engine::group_key::GroupKey>,
    ); // (sid, policy_fp, group_key)
    type SampleTuple = (String, i64, f64);
    let mut by_bucket: HashMap<u64, (BucketTuple, Vec<SampleTuple>)> = HashMap::new();
    let mut raw_matched = 0usize;
    let mut raw_unmatched = 0usize;
    // The store enforces the sid-level write barrier. This empty map preserves
    // the barrier-log interface without duplicating that check.
    let raw_barrier_drops: HashMap<u64, u64> = HashMap::new();

    for point in points {
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
            let (sid, policy_fp) = match resolve_bucket_sid_for_agg_config(
                ingest_state,
                config,
                &point.labels,
                catalog_generation.as_deref(),
            ) {
                Ok(binding) => binding,
                Err(error) => {
                    warn!(%error, "configured ingest series reactivation rejected");
                    continue;
                }
            };
            by_bucket
                .entry(sid)
                .or_insert_with(|| ((sid, policy_fp, group_key.clone()), Vec::new()))
                .1
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

    let raw_messages: Vec<WorkerMessage> = by_bucket
        .into_iter()
        .map(
            |(_sid, ((sid, policy_fp, group_key), samples))| WorkerMessage::GroupSamples {
                sid,
                policy_fp,
                group_key,
                samples,
                ingest_received_at,
            },
        )
        .collect();

    if !raw_messages.is_empty() {
        if let Err(e) = ingest_state
            .router
            .route_group_batch(raw_messages, ingest_received_at, catalog_generation.clone())
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
    // See `raw_barrier_drops` above — schema-keyed barrier dropped;
    // sid-level barrier in `SketchStore` enforces §6.3 going
    // forward.
    let sketch_barrier_drops: HashMap<u64, u64> = HashMap::new();
    for point in sketch_payloads {
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
            // B7.6 — same sid-resolution scheme as the raw path: bucket
            // identity is (metric, grouping-label-values, agg_kind).
            // The opaque SketchEnvelope path predates per-variant
            // sketch-kind plumbing here; treat the bucket as ExactAgg
            // so the resolver key matches what
            // `reconcile_from_streaming_config` derives from the same
            // config (otherwise the bucket would be reachable but never
            // reconciled).
            let (sid, policy_fp) = match resolve_bucket_sid_for_agg_config(
                ingest_state,
                config,
                &point.labels,
                catalog_generation.as_deref(),
            ) {
                Ok(binding) => binding,
                Err(error) => {
                    warn!(%error, "configured ingest series reactivation rejected");
                    continue;
                }
            };
            sketch_messages.push(WorkerMessage::AccumulatorInput {
                sid,
                policy_fp,
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
            .route_group_batch(
                sketch_messages,
                ingest_received_at,
                catalog_generation.clone(),
            )
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
/// Per-Export outcome of the modified-OTLP sketch ingest path. Carries
/// both halves of the Phase-4/B round trip:
///
/// - `unknown_series_ids` → sids the receiver could not satisfy this
///   Export (sender's cache is stale; sender must evict + re-emit with
///   attrs).
/// - `series_assignments` → newly-minted or cache-resolved sid bindings
///   the receiver wants the sender to cache. Sent back in
///   `ExportMetricsServiceResponse.series_assignments` so the sender
///   omits attrs on subsequent emits keyed by these sids.
///
/// Empty `series_assignments` is the no-op case (every DP arrived with
/// the right sid already, or every DP was rejected to `unknown_series_ids`).
#[derive(Debug, Default)]
pub(crate) struct IngestOutcome {
    pub unknown_series_ids: Vec<u64>,
    pub series_assignments: Vec<asap_otel_proto::tonic::collector::metrics::v1::SeriesAssignment>,
}

async fn route_modified_otlp_sketches_to_precompute(
    request: &ExportMetricsServiceRequest,
    ingest_state: &Arc<IngestState>,
) -> Result<IngestOutcome, String> {
    use asap_otel_proto::tonic::metrics::v1::metric::Data;

    let ingest_received_at = Instant::now();
    // Load the generation exactly once. Deriving both the runtime config and
    // transmission contract from this Arc prevents an activation between two
    // independent ArcSwap loads from producing a torn ingest view.
    let active_physical_plan_snapshot = ingest_state.active_physical_plan_snapshot();
    let snap = active_physical_plan_snapshot
        .as_ref()
        .map(|plan| plan.installed_precompute_plan.clone())
        .unwrap_or_else(|| ingest_state.config_snapshot());
    let catalog_generation = active_physical_plan_snapshot
        .as_ref()
        .and_then(|plan| plan.precompute_plan.summary_catalog.clone())
        .map(Arc::new);
    let active_physical_plan = active_physical_plan_snapshot.filter(|plan| plan.plan_id() != 0);
    let lineage_batch_guard = active_physical_plan
        .as_ref()
        .map(|_| ingest_state.observability.frame_lineage.lock_batch());
    if let Some(active) = active_physical_plan.as_ref() {
        // Validate the complete request before mutating the SID registry,
        // sketch store, snapshot cache, or worker queues. This makes the
        // OTLP request the atomic full-frame publication unit: 2xx/OK means
        // the batch was accepted, while a contract error applies none of it.
        preflight_summary_frames(request, ingest_state, active)?;
    }
    let agg_configs = snap.materializations();
    // Reconcile sid lifecycle only when the configuration snapshot changes.
    let _ = crate::storage_engines::sketch_db::lifecycle::reconcile_if_config_changed(
        ingest_state.summary_store.as_ref(),
        &snap,
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    );
    let mut messages: Vec<WorkerMessage> = Vec::new();
    let mut routed = 0usize;
    let mut decoded_failed = 0usize;
    let mut unconfigured = 0usize;
    // CQ-2 — the legacy routing-side WorkerMessage push (the DEPRECATED
    // dual-write that clones the accumulator into the worker under a
    // bucket-sid, in tandem with the Phase-5 SketchStore append above) is
    // gated OFF by default. The SketchStore `append_sample` path is the
    // live write; the worker push only matters until the ASAP-tier query
    // reducer is validated end-to-end, so re-enable it with
    // `ASAP_LEGACY_DUAL_WRITE=1` rather than paying the clone + double
    // store on the default path. Read once per Export (cheap), not per DP.
    let legacy_dual_write = std::env::var("ASAP_LEGACY_DUAL_WRITE")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")
        })
        .unwrap_or(false);
    // Schema-keyed barrier dropped (see `raw_barrier_drops` above);
    // sid-level barrier in `SketchStore` carries §6.3 going forward.
    let barrier_drops: HashMap<u64, u64> = HashMap::new();
    // sids the receiver did not recognize this Export. Returned
    // to the caller so the gRPC / HTTP handler can stamp them into
    // `ExportMetricsServiceResponse.unknown_series_ids`. Senders evict
    // these sids and re-emit with attributes; backend re-resolves and
    // returns fresh `series_assignments`.
    let mut unknown_sids: Vec<u64> = Vec::new();
    // Sid bindings the receiver wants the sender to cache. Populated
    // every time an attrs-bearing DP is resolved by the
    // `SeriesIdResolver` (either fresh mint or cache hit). Returned
    // alongside `unknown_sids` so the gRPC / HTTP handler can stamp
    // them into `ExportMetricsServiceResponse.series_assignments`.
    let mut new_assignments: Vec<asap_otel_proto::tonic::collector::metrics::v1::SeriesAssignment> =
        Vec::new();

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
                        let cfg =
                            crate::storage_engines::sketch_db::index::SketchConfig::DDSketch {
                                relative_accuracy: d.relative_accuracy,
                            };
                        d.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                algorithm: SketchAlgorithm::DDSketch,
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
                        let cfg =
                            crate::storage_engines::sketch_db::index::SketchConfig::Kll { k: k.k };
                        k.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                algorithm: SketchAlgorithm::Kll,
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
                        let cfg =
                            crate::storage_engines::sketch_db::index::SketchConfig::CountSketch {
                                rows: c.rows,
                                cols: c.cols,
                            };
                        c.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                algorithm: SketchAlgorithm::CountSketch,
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
                        let cfg =
                            crate::storage_engines::sketch_db::index::SketchConfig::CountMin {
                                rows: c.rows,
                                cols: c.cols,
                            };
                        c.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                algorithm: SketchAlgorithm::Cms,
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
                        let cfg = crate::storage_engines::sketch_db::index::SketchConfig::Hll {
                            precision: h.precision,
                        };
                        h.data_points
                            .iter()
                            .map(|dp| ModifiedOtlpSketchDp {
                                algorithm: SketchAlgorithm::Hll,
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

                // Canonicalize the metric name once per metric: strip the
                // agent-side sketch-family suffix (`_kll`, `_hll`, …) so
                // the whole ingest pipeline — sid resolution, series-key
                // snapshot cache, `SummarySeriesMetadata.metric_name`,
                // `derive_sketch_policy_fp`, and the legacy precompute
                // router match below — keys on the RAW metric name that
                // the controller's streaming-config and the query
                // analyzer speak. Without this the warm-tier sketch
                // queries can never resolve (see
                // `canonical_sketch_metric_name` for the full rationale).
                // All datapoints in one OTLP metric share the same family
                // (the `metric.data` variant), so the first dp's `kind`
                // determines the suffix for the whole metric.
                let canonical_name: String = match dps.first() {
                    Some(first) => {
                        canonical_sketch_metric_name(&metric.name, first.algorithm.clone())
                            .to_string()
                    }
                    None => metric.name.clone(),
                };

                for mut dp in dps {
                    let frame_identity = if let Some(active) = active_physical_plan.as_ref() {
                        match take_summary_frame_identity(
                            &mut dp.attrs,
                            dp.start_time_unix_nano,
                            dp.time_unix_nano,
                        )
                        .and_then(|frame| {
                            if state_encoding_for_wire(dp.encoding) != Some(frame.encoding.clone())
                            {
                                return Err("wire encoding differs from frame identity".into());
                            }
                            active
                                .transmission_plan
                                .validate_frame(&frame)
                                .map_err(|error| error.to_string())?;
                            Ok(frame)
                        }) {
                            Ok(frame) => Some(frame),
                            Err(error) => unreachable!(
                                "summary frame changed after successful request preflight: {error}"
                            ),
                        }
                    } else {
                        None
                    };
                    let series_key = format_series_key(&canonical_name, &dp.attrs);
                    let ts_ms = (dp.time_unix_nano / 1_000_000) as i64;

                    // Sid resolution — registry-allocated, NOT content-
                    // addressed. The `SeriesIdResolver` is the single
                    // authoritative mint for sids in the pipeline: same
                    // `(metric_name, attrs_fingerprint)` always returns
                    // the same sid for the lifetime of the resolver's
                    // cache. Uniqueness is by construction
                    // (`AtomicU64::fetch_add`); two different identities
                    // CANNOT share a sid. The content-addressed
                    // `compute_sketch_sid` path was retired here because
                    // u64 xxhash gives only probabilistic uniqueness,
                    // and "unique sid per series" is a contract this
                    // wire shape needs (the agent omits attrs on
                    // subsequent emits; the receiver must be able to
                    // disambiguate `sid → (metric, attrs)`).
                    //
                    // Determinism trade-off: sids are NOT stable across
                    // independent backends, and (without persistence)
                    // not across backend restarts either. PR-2 of this
                    // chain wires a WAL-backed `SeriesResolverPersistence`
                    // trait so the mapping survives restart; without it,
                    // restart-recovery still works via the
                    // `unknown_series_ids` eviction primitive (agents
                    // observe their cached sid is unknown, evict, re-emit
                    // with attrs, get a fresh assignment).
                    //
                    // Four wire cases:
                    //   (sid=0, attrs)        → resolve (mint or cache
                    //                           hit); always emit a
                    //                           `SeriesAssignment` so the
                    //                           sender caches the binding
                    //   (sid!=0, attrs)       → resolve; if the resolver's
                    //                           sid disagrees with the
                    //                           sender's, push sender's
                    //                           sid to `unknown_sids`
                    //                           (sender's cache was stale,
                    //                           e.g. after a backend
                    //                           restart without
                    //                           persistence); always emit a
                    //                           fresh assignment
                    //   (sid!=0, no attrs)    → can't resolve without
                    //                           attrs; accept the sid iff
                    //                           the SketchStore has it
                    //                           registered. Unknown → push
                    //                           to `unknown_sids` and drop
                    //                           this DP (sender will
                    //                           re-emit with attrs next
                    //                           pass)
                    //   (sid=0, no attrs)     → invalid wire shape, drop
                    let attrs_pairs: Vec<(&str, &str)> = dp
                        .attrs
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect();
                    // `canonical_attrs_fingerprint(&[])` is `""` — a valid,
                    // stable key. The empty-attrs case is therefore NOT a
                    // reason to skip the resolver (P1-5): a globally-
                    // aggregated sketch (no resource/scope/DP attrs at all)
                    // resolves to the stable `(metric, "", agg_kind)` sid
                    // just like any other series.
                    let fp = crate::drivers::ingest::canonical_attrs_fingerprint(&attrs_pairs);
                    // Store-lookup-ONLY path is reserved for the bandwidth-
                    // saving case: the sender already holds a cached sid and
                    // deliberately omits attrs on this emit. We cannot
                    // re-derive its identity (no attrs to fingerprint AND a
                    // non-zero sid the sender expects us to honor), so we
                    // accept the sid iff the SketchStore has it registered.
                    //
                    // Every OTHER case — including (sid=0, empty attrs),
                    // i.e. global aggregation — goes through the resolver,
                    // because `fp=""` is a perfectly good mint/lookup key.
                    let resolved_sid: Option<u64> = if dp.series_id != 0 && attrs_pairs.is_empty() {
                        let sid = dp.series_id;
                        if ingest_state.summary_store.is_current_storage_handle(sid) {
                            Some(sid)
                        } else {
                            unknown_sids.push(sid);
                            None
                        }
                    } else {
                        // Build the canonical AggKind string for this DP so
                        // the resolver's cache key is `(metric, fp, agg_kind)`.
                        // Two DPs over the same (metric, attrs) but different
                        // sketch algorithms/configs (e.g. DDSketch vs Kll, or two
                        // DDSketches at different relative_accuracy) get
                        // SEPARATE sids — matching the identity model the
                        // retired `compute_sketch_sid` hashed over. For the
                        // empty-attrs global-aggregation case `fp` is `""`,
                        // so the resolver mints a single stable sid for
                        // `(metric, "", agg_kind)`.
                        //
                        // P1-4 — sid identity uses the BASE (heap-less)
                        // family so a heap-bearing frame and a heap-LESS
                        // frame for the same series share ONE sid. The heap
                        // is an additive enrichment on the same sketch
                        // substrate, not a different series; the capability
                        // UPGRADE below promotes that shared sid's metadata
                        // when a heap arrives. Without this collapse the two
                        // frames would mint distinct sids and the upgrade
                        // could never fire (the analyzer would also see two
                        // candidates for one logical series).
                        let definition = frame_identity
                            .as_ref()
                            .map(|frame| frame.materialization)
                            .unwrap_or_else(|| asap_types::PolicyFingerprint(0).into());
                        let resolved = ingest_state.summary_store.resolve_output_storage_handle(
                            &ingest_state.series_resolver,
                            definition,
                            &fp,
                            catalog_generation.as_deref(),
                        );
                        // Codec-only unit fixtures have no installed plan. Production
                        // accepts stored summaries only through an installed output.
                        #[cfg(test)]
                        let resolved = if catalog_generation.is_none()
                            && definition.fingerprint().is_unset()
                        {
                            let kind = crate::storage_engines::sketch_db::data::AggKind::Sketch {
                                algorithm: base_sketch_algorithm(sketch_algorithm_for(&dp)),
                                config: dp.container_config.clone(),
                                spatial_filter_canonical: String::new(),
                            };
                            Ok(ingest_state.series_resolver.resolve(
                                &canonical_name,
                                &fp,
                                &kind.canonical_string(),
                            ))
                        } else {
                            resolved
                        };
                        let assigned = match resolved {
                            Ok(sid) => sid,
                            Err(error) => {
                                if dp.series_id != 0 {
                                    unknown_sids.push(dp.series_id);
                                }
                                warn!(%error, "modified OTLP series reactivation rejected");
                                continue;
                            }
                        };
                        if dp.series_id != 0 && dp.series_id != assigned {
                            // Sender's cached sid disagrees with the
                            // resolver's binding — sender's cache is
                            // stale, signal eviction.
                            unknown_sids.push(dp.series_id);
                        }
                        // Always echo the canonical binding back in
                        // `series_assignments` so the sender caches it
                        // (or refreshes a stale entry). The dictionary
                        // bookkeeping fields beyond
                        // `(attributes_fingerprint, series_id)` are
                        // optional today — the patched OTel-Go exporter
                        // keys its local cache on the fingerprint, not on
                        // the dictionary metadata. The empty-attrs global
                        // series echoes an assignment with an empty
                        // `attributes_fingerprint`, which the exporter
                        // caches like any other binding.
                        new_assignments.push(
                            asap_otel_proto::tonic::collector::metrics::v1::SeriesAssignment {
                                attributes_fingerprint: fp.as_bytes().to_vec(),
                                series_id: assigned,
                                metric_name: metric.name.clone(),
                                ..Default::default()
                            },
                        );
                        Some(assigned)
                    };
                    let Some(sid) = resolved_sid else {
                        continue;
                    };

                    if let Some(frame) = frame_identity.as_ref() {
                        let observed_policy = ingest_state
                            .summary_store
                            .instance(sid)
                            .map(|metadata| metadata.policy_fp)
                            .unwrap_or_else(|| {
                                derive_sketch_policy_fp(
                                    ingest_state,
                                    &canonical_name,
                                    sketch_algorithm_for(&dp),
                                    &dp.container_config,
                                    &dp.attrs.keys().cloned().collect(),
                                )
                            });
                        if observed_policy != frame.materialization.fingerprint() {
                            unreachable!(
                                "materialization changed after successful request preflight"
                            );
                        }
                    }

                    // register a `SummarySeriesMetadata` on
                    // first sight of `sid` and append this DP's sketch
                    // state to the per-sid columnar storage. The instance
                    // is keyed by sid, so subsequent DPs on the same sid
                    // skip the register step. `group_by_keys` is
                    // `dp.attrs.keys()` — after the agent's `AggregateBy`
                    // rollup, `attributes` is the group-by VALUES vector,
                    // and its key set IS the group-by KEY set.
                    {
                        use crate::storage_engines::sketch_db::index::{
                            AccuracyBound, Capability, SketchAlgorithm, SummarySeriesMetadata,
                        };
                        use std::collections::BTreeSet;

                        if ingest_state.summary_store.instance(sid).is_none() {
                            let algorithm = sketch_algorithm_for(&dp);
                            let cap = match algorithm {
                                SketchAlgorithm::DDSketch | SketchAlgorithm::Kll => {
                                    Capability::QuantileApprox(Some(algorithm.clone()))
                                }
                                SketchAlgorithm::Hll | SketchAlgorithm::UnivMon => {
                                    Capability::CardinalityApprox
                                }
                                // Heap-LESS frequency sketches answer bare
                                // frequency point queries (no top-k); index
                                // them as FrequencyEstimate so a `topk(...)`
                                // query routes to archive (or to a different
                                // sid that carries a heap-bearing variant).
                                SketchAlgorithm::CountSketch | SketchAlgorithm::Cms => {
                                    Capability::FrequencyEstimate(Some(algorithm.clone()))
                                }
                                // Heap-BEARING frequency sketches answer
                                // both point-frequency AND top-k. We register
                                // them under FrequencyTopk (top-k is the
                                // strongest claim); the analyzer-side
                                // `is_satisfied_by` for FrequencyEstimate
                                // explicitly accepts heap-bearing variants,
                                // so bare-frequency queries still route here.
                                SketchAlgorithm::CmsWithHeap
                                | SketchAlgorithm::CountSketchWithHeap => {
                                    Capability::FrequencyTopk(Some(algorithm.clone()))
                                }
                                SketchAlgorithm::Kmv | SketchAlgorithm::Theta => {
                                    Capability::CardinalityApprox
                                }
                            };
                            let group_by_keys: BTreeSet<String> =
                                dp.attrs.keys().cloned().collect();
                            let cfg = dp.container_config.clone();
                            // Derive the policy fingerprint by content-
                            // matching the OTLP DP's shape against the
                            // streaming-config registry. Sketches arrive
                            // with `(kind, config)` embedded but no policy
                            // reference; we find the policy whose contents
                            // produce the same shape. Lookup returns
                            // `Some(fp)` on a unique match, `None`
                            // when zero policies match (sketch ingested
                            // before the streaming-config caught up) or
                            // when multiple policies match the same shape
                            // (would have been a sid-collision bug —
                            // surfaces as an UNSET registration so the
                            // legacy `instances_matching` walk still
                            // covers it).
                            let policy_fp = derive_sketch_policy_fp(
                                ingest_state,
                                &canonical_name,
                                algorithm.clone(),
                                &cfg,
                                &group_by_keys,
                            );
                            // Per-item dimension (item_label) the controller threaded
                            // into the matched policy's parameters — recorded on the sid
                            // below so the query engine can answer per-item estimate(key)
                            // (the CMS/CountSketch FrequencyEstimate gate consults it).
                            let item_label_for_sid: Option<String> = {
                                snap.get_aggregation_config(policy_fp.as_u64())
                                    .or_else(|| {
                                        snap.materializations()
                                            .values()
                                            .find(|c| c.metric == canonical_name)
                                    })
                                    .and_then(|c| c.parameters.get("item_label"))
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty())
                                    .map(|s| s.to_string())
                            };
                            ingest_state.summary_store.register(SummarySeriesMetadata {
                                storage_handle: sid,
                                metric_name: canonical_name.clone(),
                                group_by_keys,
                                capability: Some(cap),
                                agg_kind:
                                    crate::storage_engines::sketch_db::index::AggKind::Sketch {
                                        algorithm,
                                        config: cfg.clone(),
                                        spatial_filter_canonical: String::new(),
                                    },
                                accuracy: Some(AccuracyBound::from_config(&cfg)),
                                first_seen_unix_ms: ts_ms,
                                retired_at_ms: None,
                                expires_at_ms: None,
                                policy_fp,
                            });
                            if let Some(label) = &item_label_for_sid {
                                ingest_state.summary_store.set_item_label(sid, label);
                            }
                        } else if let Some(existing) = ingest_state.summary_store.instance(sid) {
                            // P1-4 (a) — one-way capability UPGRADE. The sid
                            // was first registered from a non-heap frame
                            // (PROTO, a delta, or a heap-LESS MSGPACK), so it
                            // carries `FrequencyEstimate(CountMin|CountSketch)`
                            // and can never answer `topk(...)`. If a later
                            // heap-bearing frame arrives for the SAME sid,
                            // promote it to `FrequencyTopk(*WithHeap)` and
                            // upgrade its kind handle so the analyzer routes
                            // top-k queries here. Never downgrades: we only
                            // act when the current cap is heap-LESS frequency
                            // and the incoming frame actually carries a heap.
                            let incoming_algorithm = sketch_algorithm_for(&dp);
                            let upgrade_to = match (&existing.capability, incoming_algorithm) {
                                (
                                    Some(Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms))),
                                    SketchAlgorithm::CmsWithHeap,
                                ) => Some(SketchAlgorithm::CmsWithHeap),
                                (
                                    Some(Capability::FrequencyEstimate(Some(
                                        SketchAlgorithm::CountSketch,
                                    ))),
                                    SketchAlgorithm::CountSketchWithHeap,
                                ) => Some(SketchAlgorithm::CountSketchWithHeap),
                                _ => None,
                            };
                            if let Some(new_algorithm) = upgrade_to {
                                let mut upgraded = (*existing).clone();
                                upgraded.capability =
                                    Some(Capability::FrequencyTopk(Some(new_algorithm.clone())));
                                upgraded.agg_kind =
                                    crate::storage_engines::sketch_db::index::AggKind::Sketch {
                                        algorithm: new_algorithm.clone(),
                                        config: dp.container_config.clone(),
                                        spatial_filter_canonical: String::new(),
                                    };
                                // `register` overwrites the sid-keyed entry
                                // in place (same sid → same policy/metric
                                // index slots), so this is an atomic swap to
                                // the stronger capability.
                                ingest_state.summary_store.register(upgraded);
                                debug!(
                                    "OTLP sketch sid {} upgraded {:?} -> FrequencyTopk({:?}) \
                                     on heap-bearing frame (metric={}, encoding={})",
                                    sid,
                                    SketchAlgorithm::Cms,
                                    new_algorithm,
                                    metric.name,
                                    dp.encoding
                                );
                            }
                        }
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
                    //
                    // Per-window base rotation
                    // (`docs/delta-baseline-contract.md` §3): the edge
                    // tumbling window resets per-series sketch state
                    // every window, so each window's delta is that
                    // window's marginal against an empty base. The
                    // backend therefore must NOT accumulate forever
                    // (`state(N) = state(N-1) ⊕ delta(N)`), which would
                    // over-count across windows. Instead we detect a
                    // window boundary per series — a change in the data
                    // point's window start (`start_time_unix_nano`)
                    // versus the `window_start` stored with the cached
                    // base — and reset the cached base to empty before
                    // applying the new window's delta. Within a window
                    // deltas still accumulate; at a new window the base
                    // starts fresh, so the reconstructed `state(N)` is
                    // window N only. Sketch-agnostic: the reset is the
                    // additive families' (DDSketch / CMS / CountSketch /
                    // HLL) `AggregateCore::reset_to_empty`; KLL never
                    // deltas. Full frames keep REPLACE semantics and set
                    // the stored `window_start`.
                    let accumulator: Box<dyn AggregateCore> = if dp.encoding == ENCODING_PROTO_DELTA
                        || dp.encoding == ENCODING_MSGPACK_DELTA
                    {
                        let (mut merged, base_window_start) = match ingest_state
                            .sketch_snapshots
                            .get(&series_key)
                            .map(|e| (e.core.clone_boxed_core(), e.window_start))
                        {
                            Some(pair) => pair,
                            None => {
                                // P1-1/P1-2 — no cached base. Under the
                                // per-window-reset contract each delta
                                // reconstructs onto an EMPTY base, so for the
                                // ADDITIVE families (CMS / CountSketch / HLL)
                                // we bootstrap an empty accumulator of the
                                // frame's (kind, config) and apply the delta
                                // onto it — mirroring the warm-read tier so
                                // the worker tier agrees and recovers after a
                                // backend restart (which drops this in-memory
                                // cache). DDSketch / KLL can't reconstruct
                                // from a bare delta, so they keep the
                                // unchanged "drop until the next full frame"
                                // behavior.
                                match empty_accumulator_for_delta_bootstrap(
                                    dp.algorithm.clone(),
                                    &dp.container_config,
                                    dp.encoding,
                                ) {
                                    Some(empty) => {
                                        debug!(
                                            "OTLP delta-sketch with no base \
                                             bootstrapped onto an empty \
                                             accumulator (metric={}, \
                                             series_key={}, kind={:?}, \
                                             encoding={})",
                                            metric.name, series_key, dp.algorithm, dp.encoding
                                        );
                                        // Treat the freshly-minted empty base
                                        // as belonging to THIS delta's window
                                        // so the window-boundary reset below
                                        // is a no-op (the base is already
                                        // empty for this window).
                                        (empty, dp.start_time_unix_nano)
                                    }
                                    None => {
                                        // DD/KLL — unchanged drop. Count as a
                                        // distinct "no base" drop reason.
                                        ingest_state
                                            .observability
                                            .dropped_no_base
                                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                        decoded_failed += 1;
                                        debug!(
                                            "OTLP delta-sketch arrived before any base \
                                             snapshot and is not bootstrappable \
                                             (metric={}, series_key={}, kind={:?}); \
                                             dropping — agent must resend the next full \
                                             frame",
                                            metric.name, series_key, dp.algorithm
                                        );
                                        continue;
                                    }
                                }
                            }
                        };
                        // Window boundary: the incoming delta opens a new
                        // window for this series. Rotate the base to empty
                        // so the new window starts fresh (state == this
                        // window only), keeping the sketch's shape/config
                        // intact for the additive apply below.
                        if dp.start_time_unix_nano != base_window_start {
                            debug!(
                                "OTLP delta-sketch window boundary (metric={}, \
                                 series_key={}, prev_window_start={}, \
                                 new_window_start={}); rotating per-series base",
                                metric.name, series_key, base_window_start, dp.start_time_unix_nano
                            );
                            merged.reset_to_empty();
                        }
                        if let Err(e) = apply_modified_otlp_delta_bytes(
                            dp.algorithm.clone(),
                            dp.encoding,
                            &mut merged,
                            &dp.sketch,
                        ) {
                            ingest_state
                                .observability
                                .dropped_decode_fail
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            decoded_failed += 1;
                            debug!(
                                "OTLP delta-sketch apply failed \
                                 (metric={}, kind={:?}, encoding={}, \
                                 bytes={}): {} — falling through to §5.2 \
                                 fallback",
                                metric.name,
                                dp.algorithm,
                                dp.encoding,
                                dp.sketch.len(),
                                e
                            );
                            continue;
                        }
                        merged
                    } else {
                        match decode_modified_otlp_sketch_bytes(
                            dp.algorithm.clone(),
                            dp.encoding,
                            &dp.sketch,
                        ) {
                            Ok(acc) => acc,
                            Err(e) => {
                                ingest_state
                                    .observability
                                    .dropped_decode_fail
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                decoded_failed += 1;
                                let msg = e.to_string();
                                // Defensive dim-validation rejections
                                // (malformed / degenerate CMS / CountSketch
                                // dims — see `validate_sketch_dims`) signal a
                                // misbehaving producer, so surface them at
                                // WARN; ordinary decode fallbacks stay at
                                // DEBUG to avoid log spam.
                                if msg.contains("rejecting") {
                                    warn!(
                                        "OTLP modified-proto sketch dropped on dim \
                                         validation (metric={}, kind={:?}, \
                                         encoding={}, bytes={}): {}",
                                        metric.name,
                                        dp.algorithm,
                                        dp.encoding,
                                        dp.sketch.len(),
                                        msg
                                    );
                                } else {
                                    debug!(
                                        "OTLP modified-proto sketch decode failed \
                                         (metric={}, kind={:?}, encoding={}, \
                                         bytes={}): {} — falling through to §5.2 \
                                         fallback",
                                        metric.name,
                                        dp.algorithm,
                                        dp.encoding,
                                        dp.sketch.len(),
                                        msg
                                    );
                                }
                                continue;
                            }
                        }
                    };

                    // Stateful lineage is committed only after the payload
                    // has decoded successfully. Everything after this gate
                    // (snapshot replacement and SketchStore insertion) is
                    // synchronous/infallible, so an HTTP/gRPC success cannot
                    // acknowledge a sequence whose payload was never applied.
                    if let Some(frame) = frame_identity.as_ref() {
                        match ingest_state.observability.frame_lineage.observe(frame) {
                            Ok(
                                crate::precompute_engine::frame_lineage::FrameLineageDecision::Apply,
                            ) => {
                                if frame.kind
                                    == asap_types::producer_plan::SummaryFrameKind::Full
                                {
                                    ingest_state
                                        .summary_store
                                        .clear_summary_lineage_incomplete(sid, frame);
                                }
                            }
                            Ok(
                                crate::precompute_engine::frame_lineage::FrameLineageDecision::Duplicate,
                            ) => {
                                debug!(
                                    plan_id = frame.plan_id,
                                    plan_version = frame.plan_version,
                                    materialization = frame.materialization.as_u64(),
                                    producer = %frame.producer_id,
                                    producer_epoch = %frame.producer_epoch,
                                    sequence = frame.sequence,
                                    "ignored duplicate summary frame"
                                );
                                continue;
                            }
                            Err(error) => {
                                ingest_state
                                    .summary_store
                                    .mark_summary_lineage_incomplete(sid, frame);
                                warn!(
                                    plan_id = frame.plan_id,
                                    plan_version = frame.plan_version,
                                        materialization = frame.materialization.as_u64(),
                                    producer = %frame.producer_id,
                                    producer_epoch = %frame.producer_epoch,
                                    sequence = frame.sequence,
                                    %error,
                                    "rejected summary frame lineage"
                                );
                                return Err(error.to_string());
                            }
                        }
                    }

                    use crate::storage_engines::sketch_db::index::{
                        SketchEncoding, SketchSampleState,
                    };
                    use std::collections::BTreeMap;
                    let label_values: BTreeMap<String, String> = dp
                        .attrs
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect();
                    let window: crate::storage_engines::sketch_db::index::epoch_columnar::TimestampRange = (
                        dp.start_time_unix_nano / 1_000_000,
                        dp.time_unix_nano / 1_000_000,
                    );
                    let encoding =
                        encoding_to_handle(dp.encoding).unwrap_or(SketchEncoding::ProtoFull);
                    if !ingest_state.summary_store.append_sample(
                        sid,
                        label_values,
                        window,
                        SketchSampleState {
                            bytes: dp.sketch.clone(),
                            encoding,
                        },
                    ) {
                        return Err("summary window is immutable after completion".into());
                    }

                    ingest_state.sketch_snapshots.insert(
                        series_key.clone(),
                        crate::precompute_engine::ingest_handler::SnapshotCacheEntry {
                            core: accumulator.clone_boxed_core(),
                            window_start: dp.start_time_unix_nano,
                        },
                    );
                    ingest_state.note_window_and_sweep(dp.start_time_unix_nano);

                    // Collect the configs whose metric matches this DP.
                    // Detection is independent of the legacy dual-write
                    // (it only drives the routed/unconfigured accounting),
                    // so we walk it whether or not the worker push fires.
                    let matching_configs: Vec<
                        &asap_types::aggregation_config::PrecomputeMaterialization,
                    > = agg_configs
                        .values()
                        .filter(|config| {
                            config.metric == canonical_name
                                || config.spatial_filter_normalized == canonical_name
                                || config.spatial_filter == canonical_name
                        })
                        .collect();
                    let matched_any = !matching_configs.is_empty();

                    // CQ-2 — only pay the worker push (and the per-config
                    // sid resolution + accumulator clone) when the legacy
                    // dual-write is explicitly enabled. The SketchStore
                    // `append_sample` above is the live write either way.
                    if legacy_dual_write {
                        // B7.6 — bucket key is the per-config bucket sid
                        // (folds in `(metric, grouping-label-values,
                        // ExactAgg-of-config)`), NOT the per-DP `sid`
                        // resolved above. The per-DP `sid` keys the
                        // `SketchStore::register/append_sample` lane
                        // (which uses `AggKind::Sketch` to distinguish
                        // sketch shapes); the worker's group_states are
                        // keyed per-aggregation-policy bucket, which
                        // matches what `reconcile_from_streaming_config`
                        // derives from the same config (so retirement /
                        // orphan eviction stays consistent).
                        //
                        // DEPRECATED routing-side write — kept behind the
                        // `ASAP_LEGACY_DUAL_WRITE` gate until the ASAP-tier
                        // query reducer is validated end-to-end.
                        let n = matching_configs.len();
                        // PERF-2 — carry the owned accumulator so the LAST
                        // matching config can MOVE it into its message
                        // instead of cloning. `Some` until consumed; the
                        // common single-match case pays zero clones.
                        let mut owned_acc: Option<Box<dyn AggregateCore>> = Some(accumulator);
                        for (i, config) in matching_configs.iter().enumerate() {
                            // PERF-4 — group key straight from `dp.attrs`
                            // (no format_series_key → parse round-trip).
                            let group_key =
                                IngestState::extract_group_key_from_labels(&dp.attrs, config);
                            // PERF-3 — `dp.attrs` is already a
                            // `HashMap<String, String>`; pass it directly
                            // instead of rebuilding `attrs_map` per config.
                            let (bucket_sid, policy_fp) = match resolve_bucket_sid_for_agg_config(
                                ingest_state,
                                config,
                                &dp.attrs,
                                catalog_generation.as_deref(),
                            ) {
                                Ok(binding) => binding,
                                Err(error) => {
                                    warn!(%error, "configured ingest series reactivation rejected");
                                    continue;
                                }
                            };
                            let acc_for_msg = if i + 1 == n {
                                // Last (or only) match — move the owned
                                // accumulator out, no clone.
                                owned_acc.take().expect("owned_acc present on last match")
                            } else {
                                // 2nd..Nth match — clone from the still-owned
                                // accumulator (cloning a sketch is a msgpack
                                // round-trip, so we only pay it when a DP
                                // genuinely fans out to multiple policies).
                                owned_acc
                                    .as_ref()
                                    .expect("owned_acc present before last match")
                                    .clone_boxed_core()
                            };
                            messages.push(WorkerMessage::AccumulatorInput {
                                sid: bucket_sid,
                                policy_fp,
                                group_key,
                                timestamp_ms: ts_ms,
                                accumulator: acc_for_msg,
                                ingest_received_at,
                            });
                        }
                    }

                    if matched_any {
                        routed += 1;
                    } else {
                        // CQ-6 — a decoded sketch that matched no
                        // PrecomputeMaterialization in the running streaming config.
                        ingest_state
                            .observability
                            .dropped_unconfigured
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        unconfigured += 1;
                    }
                }
            }
        }
    }

    flush_barrier_drops(ingest_state, &barrier_drops, "otlp-modified-proto");

    // No lineage-protected store mutation occurs after this point. Do not
    // carry a synchronous mutex guard across the async worker-queue flush.
    drop(lineage_batch_guard);

    if !messages.is_empty() {
        if let Err(e) = ingest_state
            .router
            .route_group_batch(messages, ingest_received_at, catalog_generation.clone())
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

    Ok(IngestOutcome {
        unknown_series_ids: unknown_sids,
        series_assignments: new_assignments,
    })
}

/// Map `SketchAlgorithm` to the corresponding wire-format
/// `AggregationType`. Inverse direction is in
/// `sketch_algorithm_for` above. Used by
/// [`derive_sketch_policy_fp`] to find the policy whose
/// `PrecomputeMaterialization.aggregation_type` matches a freshly-ingested
/// sketch.
///
/// `Any` is a control-plane analysis-time wildcard — it doesn't
/// appear on the ingest path. Returns `None` so the policy lookup
/// fails the (rare) defensive path explicitly.
fn aggregation_type_for_sketch_algorithm(
    handle: crate::storage_engines::sketch_db::index::SketchAlgorithm,
) -> Option<asap_types::AggregationType> {
    use crate::storage_engines::sketch_db::index::SketchAlgorithm;
    use asap_types::AggregationType;
    match handle {
        SketchAlgorithm::DDSketch => Some(AggregationType::DDSketch),
        SketchAlgorithm::Kll => Some(AggregationType::DatasketchesKLL),
        SketchAlgorithm::Hll => Some(AggregationType::HLL),
        SketchAlgorithm::UnivMon => Some(AggregationType::UnivMon),
        SketchAlgorithm::CountSketch => Some(AggregationType::CountSketch),
        SketchAlgorithm::CountSketchWithHeap => Some(AggregationType::CountSketchWithHeap),
        SketchAlgorithm::Cms => Some(AggregationType::CountMinSketch),
        SketchAlgorithm::CmsWithHeap => Some(AggregationType::CountMinSketchWithHeap),
        SketchAlgorithm::Kmv | SketchAlgorithm::Theta => None,
    }
}

/// Render a `SketchConfig` into the param map the streaming-config
/// stores. The control plane authors these as
/// `parameters: {<name>: <value>}` JSON; the data plane has the
/// parameters typed in `SketchConfig`. This function converts.
///
/// Keys MUST match what the control plane emits (see
/// `crates/asap_types/src/aggregation_config.rs::from_yaml_data` for
/// the canonical names). Drift here surfaces as policy lookups that
/// silently miss.
fn sketch_config_to_params(
    cfg: &crate::storage_engines::sketch_db::data::SketchConfig,
) -> std::collections::HashMap<String, serde_json::Value> {
    use crate::storage_engines::sketch_db::data::SketchConfig;
    let mut params = std::collections::HashMap::new();
    match cfg {
        SketchConfig::UnivMon {
            heap_size,
            sketch_rows,
            sketch_cols,
            layers,
        } => {
            params.insert("heap_size".into(), serde_json::json!(heap_size));
            params.insert("sketch_rows".into(), serde_json::json!(sketch_rows));
            params.insert("sketch_cols".into(), serde_json::json!(sketch_cols));
            params.insert("layers".into(), serde_json::json!(layers));
        }
        SketchConfig::DDSketch { relative_accuracy } => {
            params.insert(
                "relative_accuracy".to_string(),
                serde_json::json!(*relative_accuracy),
            );
        }
        SketchConfig::Kll { k } => {
            params.insert("k".to_string(), serde_json::json!(*k));
        }
        SketchConfig::Hll { precision } => {
            params.insert("precision".to_string(), serde_json::json!(*precision));
        }
        SketchConfig::CountSketch { rows, cols } | SketchConfig::CountMin { rows, cols } => {
            // Canonical key mapping (matches the controller's
            // `sketch_params_to_json` in
            // `control_plane::emit::stage_config`): `w` is the
            // matrix width (=cols), `d` is the depth (=rows). The
            // controller writes `{w, d}` into the streaming-config
            // `parameters`, so the policy_fp content match has to
            // probe the same keys.
            params.insert("w".to_string(), serde_json::json!(*cols));
            params.insert("d".to_string(), serde_json::json!(*rows));
        }
    }
    params
}

/// Look up the policy fingerprint for a freshly-ingested OTLP sketch
/// by content-matching against the streaming-config registry.
///
/// Sketches arrive with `(metric, attrs, sketch_kind, sketch_config)`
/// embedded in the DP but no policy reference. The matching pass:
/// snapshots the current streaming config, derives a
/// `PolicyRegistry`, and asks `find_policy_by_content` for the
/// fingerprint of a policy whose contents match. Returns
/// `PolicyFingerprint::UNSET` when:
///   1. An unsupported planner algorithm reached this path
///      (defensive — shouldn't happen).
///   2. No policy in the registry matches.
///   3. Multiple policies match (would-have-been-a-bug case;
///      `find_policy_by_content` returns `None` on ambiguity).
///
/// Callers register the sid with the returned fp regardless of
/// success — UNSET sids are simply absent from the policy_fp →
/// {sids} reverse index, and remain reachable via the legacy
/// `instances_matching(metric, gbk)` walk.
fn derive_sketch_policy_fp(
    ingest_state: &IngestState,
    metric: &str,
    kind: crate::storage_engines::sketch_db::index::SketchAlgorithm,
    cfg: &crate::storage_engines::sketch_db::data::SketchConfig,
    group_by_keys: &std::collections::BTreeSet<String>,
) -> asap_types::PolicyFingerprint {
    let Some(agg_type) = aggregation_type_for_sketch_algorithm(kind) else {
        return asap_types::PolicyFingerprint::UNSET;
    };
    let params = sketch_config_to_params(cfg);
    let snap = ingest_state.config_snapshot();
    let index = asap_types::RoutingIndex::build(snap.policy_registry());
    index
        .find_policy_by_content(metric, group_by_keys, agg_type, &params)
        .unwrap_or(asap_types::PolicyFingerprint::UNSET)
}

/// Phase 5 helper — map a `ModifiedOtlpSketchDp` to the matching
/// `SketchAlgorithm` so registration and capability classification
/// share one source of truth.
///
/// CMS-with-heap detection: the OTLP `CountMinSketch` wire struct
/// itself doesn't carry a top-k heap field (see metrics.proto
/// `CountMinSketch`/`CountMinSketchDataPoint`). The heap is embedded
/// inside the msgpack-encoded `CountMinSketchWithHeapSerialized`
/// payload (an outer `{sketch, topk_heap, heap_size}` wrapper). When
/// the encoding is MSGPACK and the bytes round-trip via
/// `CountMinSketchWithHeap::deserialize_msgpack`, we classify the sid
/// as `CmsWithHeap` so the ASAP-tier reducer can later read the heap
/// directly for `topk` / `topk_over_time` queries.
/// Strip the agent-side sketch-family name suffix from an OTLP sketch
/// metric name, returning the *raw* metric name the controller's
/// streaming-config policies and user PromQL are keyed on.
///
/// ## Why this exists
///
/// The ASAPCollector edge pipeline's fused `asapedgeprocessor`
/// (`processor/asapedgeprocessor/sketch.go`) sets
/// `MetricSuffix: "_" + family` on every sketch it emits, so a KLL
/// sketch over `request_size_bytes` arrives on the wire named
/// `request_size_bytes_kll`, an HLL over `unique_users_per_min` arrives
/// as `unique_users_per_min_hll`, and so on.
///
/// Both the controller (which plans + pushes streaming-config policies
/// keyed on the *bare* metric `request_size_bytes`) and the query
/// analyzer (`control_plane::asap_tier_analysis`, which lifts the bare
/// metric name out of the PromQL selector) speak the bare name. With
/// the suffix left on, `SummarySeriesMetadata.metric_name` is the
/// suffixed form, so `SketchIndex::instances_matching(bare, …)` and
/// `find_matching_policies` / `find_policy_by_content` (all of which
/// compare `metric_name` for equality) never match — every warm sketch
/// query (`quantile_over_time`, `count`/HLL, `topk`) capability-misses
/// and the user sees `data_source: asap_query, "No result"`.
///
/// Per `docs/design_docs/series-identity.md`, the summary *family*
/// is a wire-level attribute (carried here in `agg_kind` /
/// [`SketchAlgorithm`]), NOT a name suffix; storage + query must be
/// keyed on the raw SDK metric name. This helper applies that
/// canonicalization at the ingest seam so the backend resolves
/// correctly regardless of whether the deployed agent still suffixes.
///
/// The strip is gated on the suffix matching the datapoint's *actual*
/// sketch kind, so a metric a user legitimately named `foo_hll` that
/// arrives as a KLL sketch is left untouched, and the operation is a
/// no-op (and therefore safe / idempotent) once agents stop suffixing.
fn canonical_sketch_metric_name<'a>(name: &'a str, algorithm: SketchAlgorithm) -> &'a str {
    let suffix: &str = match algorithm {
        SketchAlgorithm::DDSketch => "_ddsketch",
        SketchAlgorithm::Kll => "_kll",
        SketchAlgorithm::Hll => "_hll",
        SketchAlgorithm::CountSketch => "_countsketch",
        SketchAlgorithm::Cms => "_countminsketch",
        // These algorithms do not currently have modified-OTLP
        // containers in this receiver, so there is no suffix to strip.
        _ => return name,
    };
    // Only strip when there's a non-empty base left over (so a metric
    // literally named `_kll` is never collapsed to the empty string).
    match name.strip_suffix(suffix) {
        Some(base) if !base.is_empty() => base,
        _ => name,
    }
}

/// P1-4 — does this frequency-sketch DataPoint carry a non-empty top-k
/// heap? Detects the heap on BOTH heap-bearing wire encodings:
///
///   * `ENCODING_MSGPACK` — full heap-bearing frame
///     (`{sketch, topk_heap, heap_size}`); decoded with
///     `CountMinSketchWithHeap::from_msgpack`.
///   * `ENCODING_MSGPACK_DELTA` — DELTA-HEAP frame (sparse matrix delta +
///     the FULL top-k heap); decoded via the heap accumulator's
///     `from_msgpack_heap_delta_bytes` (the same generic `rmp_serde`
///     reader the apply path uses). This lets a sid whose FIRST frame is
///     a delta still be detected as heap-bearing and registered/upgraded
///     to a top-k capability — part (b) of P1-4.
///
/// PROTO / PROTO_DELTA frequency frames don't carry a heap on the wire,
/// so they always read as heap-LESS here.
fn dp_carries_heap(dp: &ModifiedOtlpSketchDp) -> bool {
    match dp.encoding {
        ENCODING_MSGPACK => {
            use asap_sketchlib::CountMinSketchWithHeap;
            CountMinSketchWithHeap::from_msgpack(&dp.sketch)
                .map(|cms| !cms.topk_heap_items().is_empty())
                .unwrap_or(false)
        }
        ENCODING_MSGPACK_DELTA => {
            use asap_physical_operators::summary_kernels::CountMinSketchWithHeapAccumulator;
            CountMinSketchWithHeapAccumulator::from_msgpack_heap_delta_bytes(&dp.sketch)
                .map(|acc| !acc.inner.topk_heap_items().is_empty())
                .unwrap_or(false)
        }
        _ => false,
    }
}

/// P1-4 — collapse a heap-BEARING frequency handle to its heap-LESS base
/// family. Used for SID IDENTITY so a heap-bearing frame and a heap-less
/// frame for the same `(metric, attrs, config)` resolve to ONE sid (the
/// heap is enrichment on the same substrate, not a different series). The
/// CAPABILITY still tracks the heap via the metadata upgrade path. All
/// other handles pass through unchanged.
fn base_sketch_algorithm(
    kind: crate::storage_engines::sketch_db::index::SketchAlgorithm,
) -> crate::storage_engines::sketch_db::index::SketchAlgorithm {
    use crate::storage_engines::sketch_db::index::SketchAlgorithm;
    match kind {
        SketchAlgorithm::CmsWithHeap => SketchAlgorithm::Cms,
        SketchAlgorithm::CountSketchWithHeap => SketchAlgorithm::CountSketch,
        other => other,
    }
}

fn sketch_algorithm_for(
    dp: &ModifiedOtlpSketchDp,
) -> crate::storage_engines::sketch_db::index::SketchAlgorithm {
    use crate::storage_engines::sketch_db::index::SketchAlgorithm;
    match dp.algorithm.clone() {
        SketchAlgorithm::DDSketch => SketchAlgorithm::DDSketch,
        SketchAlgorithm::Kll => SketchAlgorithm::Kll,
        SketchAlgorithm::Hll => SketchAlgorithm::Hll,
        SketchAlgorithm::CountSketch => {
            // Mirror the CountMin branch: CountSketch-with-heap
            // payloads share the same outer msgpack envelope
            // (`CountMinSketchWithHeapSerialized` — see the comment
            // in `sketch_reducer.rs` at the dispatch site, which
            // notes both heap-bearing variants reuse this wire
            // shape since the heap is the distinguishing payload).
            // Auto-promote to `CountSketchWithHeap` when the bytes
            // decode AND the heap is non-empty; otherwise stay with
            // vanilla `CountSketch`.
            if dp_carries_heap(dp) {
                return SketchAlgorithm::CountSketchWithHeap;
            }
            SketchAlgorithm::CountSketch
        }
        SketchAlgorithm::Cms => {
            // Try a no-cost peek: msgpack-encoded CMS-with-heap payloads
            // round-trip through asap_sketchlib's
            // `CountMinSketchWithHeap::deserialize_msgpack`. If the
            // sketch bytes decode against that wrapper *and* the
            // resulting heap is non-empty, treat the sid as
            // CmsWithHeap so ASAP-tier `topk` can read the heap.
            // Otherwise stay with vanilla `CountMin`.
            if dp_carries_heap(dp) {
                return SketchAlgorithm::CmsWithHeap;
            }
            SketchAlgorithm::Cms
        }
        other => other,
    }
}

/// Phase 5 helper — translate the wire-format `encoding` integer to the
/// SketchStore's `SketchEncoding` enum. Returns `None` for the unset
/// (0) encoding so callers can default to `ProtoFull` (the dominant
/// case for full-state frames).
fn encoding_to_handle(
    encoding: i32,
) -> Option<crate::storage_engines::sketch_db::index::SketchEncoding> {
    use crate::storage_engines::sketch_db::index::SketchEncoding;
    match encoding {
        ENCODING_PROTO => Some(SketchEncoding::ProtoFull),
        ENCODING_PROTO_DELTA => Some(SketchEncoding::ProtoDelta),
        ENCODING_MSGPACK => Some(SketchEncoding::MsgpackFull),
        ENCODING_MSGPACK_DELTA => Some(SketchEncoding::MsgpackDelta),
        _ => None,
    }
}

/// A single modified-OTLP sketch data point flattened across the five
/// per-variant data-point types so the routing loop can treat them
/// uniformly.
struct ModifiedOtlpSketchDp {
    algorithm: SketchAlgorithm,
    attrs: HashMap<String, String>,
    time_unix_nano: u64,
    sketch: Vec<u8>,
    encoding: i32,
    /// sender-supplied series_id, 0 when unset / first emit.
    /// Backend's resolver mints a fresh sid when this is 0 with attrs
    /// populated; pushes the sid into `unknown_series_ids` when this is
    /// non-zero with empty attrs and the resolver doesn't recognize it.
    series_id: u64,
    /// DataPoint-level start of the sketch window. Combined
    /// with `time_unix_nano` to form the `(start_ms, end_ms)` window
    /// the SketchStore's columnar storage keys on.
    start_time_unix_nano: u64,
    /// sketch-instance configuration lifted off the parent
    /// container. Drives `SummarySeriesMetadata.sketch_config` and the
    /// derived `AccuracyBound`.
    container_config: crate::storage_engines::sketch_db::index::SketchConfig,
}

/// Validate every first-class summary frame in an OTLP request before the
/// ingest loop performs any externally visible mutation. The transport
/// response is therefore the acknowledgement boundary; there is no second
/// application-level ACK protocol.
fn preflight_summary_frames(
    request: &ExportMetricsServiceRequest,
    ingest_state: &IngestState,
    active: &crate::storage_engines::types::RuntimePhysicalPlan,
) -> Result<(), String> {
    use asap_otel_proto::tonic::metrics::v1::metric::Data;

    fn validate_one(
        metric_name: &str,
        mut dp: ModifiedOtlpSketchDp,
        ingest_state: &IngestState,
        active: &crate::storage_engines::types::RuntimePhysicalPlan,
    ) -> Result<asap_types::producer_plan::SummaryFrameIdentity, String> {
        let canonical_name = canonical_sketch_metric_name(metric_name, dp.algorithm.clone());
        let frame =
            take_summary_frame_identity(&mut dp.attrs, dp.start_time_unix_nano, dp.time_unix_nano)?;
        if state_encoding_for_wire(dp.encoding) != Some(frame.encoding.clone()) {
            return Err(format!(
                "summary frame for {metric_name} declares an encoding different from its payload"
            ));
        }
        active
            .transmission_plan
            .validate_frame(&frame)
            .map_err(|error| error.to_string())?;

        if active
            .precompute_plan
            .materializations
            .iter()
            .any(|config| {
                config.policy_fingerprint() == frame.materialization.fingerprint()
                    && !config.population_key_encoding.is_legacy()
            })
        {
            return Err(
                "canonical population is not supported by modified-OTLP summary routing".into(),
            );
        }

        let schema = active
            .precompute_plan
            .schemas
            .iter()
            .find(|schema| schema.materialization == frame.materialization)
            .ok_or_else(|| format!("summary frame for {metric_name} has no active schema"))?;
        if dp.attrs.is_empty() && !schema.group_by.is_empty() {
            return Err(format!(
                "summary frame for {metric_name} omits labels required by its grouped schema"
            ));
        }
        let observed_series = if dp.attrs.is_empty() {
            "<global>".to_string()
        } else {
            let pairs: Vec<(&str, &str)> = dp
                .attrs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            crate::drivers::ingest::canonical_attrs_fingerprint(&pairs)
        };
        if observed_series != frame.series_identity {
            return Err(format!(
                "summary frame for {metric_name} declares a series identity different from its labels"
            ));
        }

        // A malformed full snapshot must not be discovered after an earlier
        // frame in the request has already reached SketchStore.
        if frame.kind == asap_types::producer_plan::SummaryFrameKind::Full {
            decode_modified_otlp_sketch_bytes(dp.algorithm.clone(), dp.encoding, &dp.sketch)
                .map_err(|error| format!("invalid full frame for {metric_name}: {error}"))?;
        } else {
            let series_key = format_series_key(canonical_name, &dp.attrs);
            let (mut base, base_window_start) = ingest_state
                .sketch_snapshots
                .get(&series_key)
                .map(|entry| (entry.core.clone_boxed_core(), entry.window_start))
                .or_else(|| {
                    empty_accumulator_for_delta_bootstrap(
                        dp.algorithm.clone(),
                        &dp.container_config,
                        dp.encoding,
                    )
                    .map(|base| (base, dp.start_time_unix_nano))
                })
                .ok_or_else(|| {
                    format!("delta frame for {metric_name} has no reconstructable base")
                })?;
            if base_window_start != dp.start_time_unix_nano {
                base.reset_to_empty();
            }
            apply_modified_otlp_delta_bytes(
                dp.algorithm.clone(),
                dp.encoding,
                &mut base,
                &dp.sketch,
            )
            .map_err(|error| format!("invalid delta frame for {metric_name}: {error}"))?;
        }

        // Attribute-elided retries can recover the policy from their known
        // SID. Attribute-bearing frames derive it from the active physical
        // schema. Either route must agree with the declared materialization.
        let observed = if dp.series_id != 0 && dp.attrs.is_empty() {
            ingest_state
                .summary_store
                .instance(dp.series_id)
                .map(|metadata| metadata.policy_fp)
                .ok_or_else(|| {
                    format!(
                        "summary frame for {metric_name} references unknown sid {} without labels",
                        dp.series_id
                    )
                })?
        } else {
            derive_sketch_policy_fp(
                ingest_state,
                canonical_name,
                sketch_algorithm_for(&dp),
                &dp.container_config,
                &dp.attrs.keys().cloned().collect(),
            )
        };
        if observed != frame.materialization.fingerprint() {
            return Err(format!(
                "summary frame for {metric_name} declares materialization {} but active schema resolves {}",
                frame.materialization.as_u64(), observed.0
            ));
        }
        Ok(frame)
    }

    let mut frames = Vec::new();
    for resource_metrics in &request.resource_metrics {
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|resource| attributes_to_map(&resource.attributes))
            .unwrap_or_default();
        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_attrs = scope_metrics
                .scope
                .as_ref()
                .map(|scope| attributes_to_map(&scope.attributes))
                .unwrap_or_default();
            for metric in &scope_metrics.metrics {
                let base_labels: HashMap<String, String> = scope_attrs
                    .iter()
                    .chain(resource_attrs.iter())
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                macro_rules! validate_points {
                    ($points:expr, $algorithm:expr, $config:expr) => {{
                        let config = $config;
                        for point in &$points {
                            frames.push(validate_one(
                                &metric.name,
                                ModifiedOtlpSketchDp {
                                    algorithm: $algorithm,
                                    attrs: merge_point_attributes(&base_labels, &point.attributes),
                                    time_unix_nano: point.time_unix_nano,
                                    sketch: point.sketch.clone(),
                                    encoding: point.encoding,
                                    series_id: point.series_id,
                                    start_time_unix_nano: point.start_time_unix_nano,
                                    container_config: config.clone(),
                                },
                                ingest_state,
                                active,
                            )?);
                        }
                    }};
                }
                match &metric.data {
                    Some(Data::Ddsketch(data)) => validate_points!(
                        data.data_points,
                        SketchAlgorithm::DDSketch,
                        crate::storage_engines::sketch_db::index::SketchConfig::DDSketch {
                            relative_accuracy: data.relative_accuracy,
                        }
                    ),
                    Some(Data::Kllsketch(data)) => validate_points!(
                        data.data_points,
                        SketchAlgorithm::Kll,
                        crate::storage_engines::sketch_db::index::SketchConfig::Kll { k: data.k }
                    ),
                    Some(Data::Countsketch(data)) => validate_points!(
                        data.data_points,
                        SketchAlgorithm::CountSketch,
                        crate::storage_engines::sketch_db::index::SketchConfig::CountSketch {
                            rows: data.rows,
                            cols: data.cols,
                        }
                    ),
                    Some(Data::Countminsketch(data)) => validate_points!(
                        data.data_points,
                        SketchAlgorithm::Cms,
                        crate::storage_engines::sketch_db::index::SketchConfig::CountMin {
                            rows: data.rows,
                            cols: data.cols,
                        }
                    ),
                    Some(Data::Hllsketch(data)) => validate_points!(
                        data.data_points,
                        SketchAlgorithm::Hll,
                        crate::storage_engines::sketch_db::index::SketchConfig::Hll {
                            precision: data.precision,
                        }
                    ),
                    _ => {}
                }
            }
        }
    }
    ingest_state
        .observability
        .frame_lineage
        .validate_batch(frames.iter())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn take_summary_frame_identity(
    attrs: &mut HashMap<String, String>,
    window_start_unix_nano: u64,
    window_end_unix_nano: u64,
) -> Result<asap_types::producer_plan::SummaryFrameIdentity, String> {
    use asap_types::producer_plan::{SummaryFrameIdentity, SummaryFrameKind};
    use control_plane::physical::compiler::StateEncoding;

    fn required(attrs: &mut HashMap<String, String>, key: &str) -> Result<String, String> {
        attrs
            .remove(key)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("missing {key}"))
    }
    fn number(attrs: &mut HashMap<String, String>, key: &str) -> Result<u64, String> {
        required(attrs, key)?
            .parse()
            .map_err(|_| format!("invalid {key}"))
    }

    let identity_version = u32::try_from(number(attrs, "asap.frame.identity_version")?)
        .map_err(|_| "invalid asap.frame.identity_version".to_string())?;
    let plan_id = number(attrs, "asap.frame.plan_id")?;
    let plan_version = number(attrs, "asap.frame.plan_version")?;
    let backend_compat = required(attrs, "asap.frame.backend_compat")?;
    let materialization =
        asap_types::PolicyFingerprint(number(attrs, "asap.frame.materialization")?);
    let series_identity = required(attrs, "asap.frame.series_identity")?;
    let schema_id = required(attrs, "asap.frame.schema_id")?;
    let producer_id = required(attrs, "asap.frame.producer_id")?;
    let producer_epoch = required(attrs, "asap.frame.producer_epoch")?;
    let sequence = number(attrs, "asap.frame.sequence")?;
    let kind = match required(attrs, "asap.frame.kind")?.as_str() {
        "full" => SummaryFrameKind::Full,
        "delta" => SummaryFrameKind::Delta,
        _ => return Err("invalid asap.frame.kind".into()),
    };
    let encoding = match required(attrs, "asap.frame.encoding")?.as_str() {
        "sketchlib_protobuf_v1" => StateEncoding::SketchlibProtobufV1,
        "sketch_core_msgpack_v1" => StateEncoding::SketchCoreMsgpackV1,
        "exact_accumulator_v1" => StateEncoding::ExactAccumulatorV1,
        _ => return Err("invalid asap.frame.encoding".into()),
    };
    let checkpoint_id = attrs
        .remove("asap.frame.checkpoint_id")
        .filter(|value| !value.is_empty());
    let base_checkpoint_id = attrs
        .remove("asap.frame.base_checkpoint_id")
        .filter(|value| !value.is_empty());

    Ok(SummaryFrameIdentity {
        identity_version,
        plan_id,
        plan_version,
        backend_compat,
        materialization: materialization.into(),
        series_identity,
        schema_id,
        producer_id,
        producer_epoch,
        window_start_unix_nano,
        window_end_unix_nano,
        sequence,
        kind,
        encoding,
        checkpoint_id,
        base_checkpoint_id,
    })
}

fn state_encoding_for_wire(encoding: i32) -> Option<asap_types::precompute_plan::StateEncoding> {
    use asap_types::precompute_plan::StateEncoding;
    match encoding {
        ENCODING_PROTO | ENCODING_PROTO_DELTA => Some(StateEncoding::SketchlibProtobufV1),
        ENCODING_MSGPACK | ENCODING_MSGPACK_DELTA => Some(StateEncoding::SketchCoreMsgpackV1),
        _ => None,
    }
}

/// Decode the typed `sketch` bytes from a modified-OTLP
/// `*SketchDataPoint` into a concrete `AggregateCore`.
///
/// Dispatches on the `(SketchAlgorithm, encoding)` pair. For each
/// `(algorithm, _ENCODING_PROTO)` pair we call the matching accumulator's
/// `from_sketchlib_proto_bytes` constructor. Variants without a
/// constructor today return `Err`; the caller falls through to §5.2
/// fallback so the user still gets a correct answer. Per-variant
/// decoders are tracked in PR C (task #8) and PR I (task #14, for
/// `_ENCODING_MSGPACK` parity).
fn decode_modified_otlp_sketch_bytes(
    algorithm: SketchAlgorithm,
    encoding: i32,
    bytes: &[u8],
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>> {
    use asap_physical_operators::summary_kernels::{
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
        ENCODING_PROTO => match algorithm {
            // The neutral codec accepts both full envelopes and supported bare
            // states. Query accumulators retain their family-specific readouts.
            SketchAlgorithm::DDSketch => {
                let (inner, sample_p) = asap_sketch_codec::reconstruct_ddsketch(bytes)?;
                let sample_p = if sample_p.is_finite() && sample_p > 0.0 && sample_p < 1.0 {
                    sample_p
                } else {
                    1.0
                };
                Ok(Box::new(DDSketchAccumulator { inner, sample_p }))
            }
            SketchAlgorithm::Kll => Ok(Box::new(
                DatasketchesKLLAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchAlgorithm::Cms => Ok(Box::new(
                CountMinSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchAlgorithm::CountSketch => Ok(Box::new(
                CountSketchAccumulator::from_sketchlib_proto_bytes(bytes)?,
            )),
            SketchAlgorithm::Hll => Ok(Box::new(HllSketchAccumulator::from_sketchlib_proto_bytes(
                bytes,
            )?)),
            other => {
                Err(format!("modified-OTLP PROTO decoding is not implemented for {other:?}").into())
            }
        },
        ENCODING_MSGPACK => match algorithm {
            SketchAlgorithm::Cms => Ok(Box::new(CountMinSketchAccumulator::from_msgpack_bytes(
                bytes,
            )?)),
            SketchAlgorithm::CountSketch => {
                // Heap-bearing CountSketch full frame: the bytes are the
                // `{sketch,topk_heap,heap_size}` envelope (a DIFFERENT inner
                // field order than the plain CountSketch msgpack), so
                // `CountSketch::from_msgpack` can't parse it. Try the heap
                // decode FIRST when the heap is non-empty (the same promotion
                // gate `sketch_algorithm_for` uses); cache THAT heap
                // accumulator as the per-series base so a later MSGPACK_DELTA
                // frame applies its matrix delta + heap onto a heap
                // accumulator. Fall back to the plain CountSketch decode for
                // heap-less msgpack frames (byte-parity path, PR I).
                //
                // Uses the real `CountSketchWithHeap` (median-of-signed-rows),
                // NOT `CountMinSketchWithHeap` — the two share the same wire
                // envelope shape (structural peek only), but decoding a real
                // CountSketch's matrix through the CMS wrapper would silently
                // apply CMS's min-of-rows math to CountSketch data forever
                // after (the same conflation bug fixed on the write side in
                // `accumulator_factory.rs`).
                use asap_sketchlib::CountSketchWithHeap;
                if let Ok(heap) = CountSketchWithHeap::from_msgpack(bytes) {
                    if !heap.topk_heap_items().is_empty() {
                        use asap_physical_operators::summary_kernels::CountSketchWithHeapAccumulator;
                        return Ok(Box::new(
                            CountSketchWithHeapAccumulator::from_msgpack_with_heap_bytes(bytes)?,
                        ));
                    }
                }
                Ok(Box::new(CountSketchAccumulator::from_msgpack_bytes(bytes)?))
            }
            SketchAlgorithm::Kll => Ok(Box::new(DatasketchesKLLAccumulator::from_msgpack_bytes(
                bytes,
            )?)),
            SketchAlgorithm::DDSketch => {
                Ok(Box::new(DDSketchAccumulator::from_msgpack_bytes(bytes)?))
            }
            SketchAlgorithm::Hll => Ok(Box::new(HllSketchAccumulator::from_msgpack_bytes(bytes)?)),
            other => Err(format!(
                "modified-OTLP MSGPACK decoding is not implemented for {other:?}"
            )
            .into()),
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

/// P1-1/P1-2 — construct an EMPTY accumulator matching a sketch
/// DataPoint's `(kind, container_config)`, for bootstrapping a delta
/// frame that arrives with no cached base.
///
/// Under the per-window-reset contract (`docs/delta-baseline-contract.md`
/// §3) each window's delta encodes that window's own state against an
/// EMPTY base. So a leading delta (no prior full frame — e.g. the very
/// first frame for a sid, or the first frame after a backend restart
/// dropped the in-memory snapshot cache) is reconstructable for the
/// ADDITIVE families by `empty(dims) + apply(delta)`. This mirrors the
/// warm-read tier's standalone delta reconstruction so the worker tier
/// agrees with it and recovers after restart, instead of dropping the
/// delta.
///
/// Returns `None` for families that CANNOT reconstruct from a bare delta
/// (DDSketch and KLL): DDSketch deltas are bucket-index diffs whose
/// absolute store layout depends on the base's offset, and KLL never
/// deltas. Those keep the unchanged "drop until the next full frame"
/// behavior.
fn empty_accumulator_for_delta_bootstrap(
    algorithm: SketchAlgorithm,
    config: &crate::storage_engines::sketch_db::index::SketchConfig,
    encoding: i32,
) -> Option<Box<dyn AggregateCore>> {
    use crate::storage_engines::sketch_db::index::SketchConfig;
    use asap_physical_operators::summary_kernels::{
        CountMinSketchAccumulator, CountSketchAccumulator, CountSketchWithHeapAccumulator,
        HllSketchAccumulator,
    };

    match (algorithm, config) {
        (SketchAlgorithm::Hll, SketchConfig::Hll { precision }) => {
            use asap_sketchlib::HllVariant;
            // Regular is the default agent variant; HLL's additive delta
            // merge tolerates an empty same-precision base.
            Some(Box::new(HllSketchAccumulator::new(
                HllVariant::Regular,
                *precision,
            )))
        }
        (SketchAlgorithm::Cms, SketchConfig::CountMin { rows, cols }) => Some(Box::new(
            CountMinSketchAccumulator::new(*rows as usize, *cols as usize),
        )),
        (SketchAlgorithm::CountSketch, SketchConfig::CountSketch { rows, cols }) => {
            // A heap-bearing DELTA-HEAP frame must reconstruct onto a heap
            // accumulator (the apply path downcasts to
            // `CountSketchWithHeapAccumulator`); a plain matrix delta
            // reconstructs onto a vanilla CountSketch. Pick the base shape
            // from the encoding so the subsequent
            // `apply_modified_otlp_delta_bytes` downcast succeeds.
            if encoding == ENCODING_MSGPACK_DELTA {
                // heap_size 0 is fine — the DELTA-HEAP apply REPLACES the
                // heap wholesale from the frame's full heap.
                Some(Box::new(CountSketchWithHeapAccumulator::new(
                    *rows as usize,
                    *cols as usize,
                    0,
                )))
            } else {
                Some(Box::new(CountSketchAccumulator::new(
                    *rows as usize,
                    *cols as usize,
                )))
            }
        }
        // DDSketch / KLL (and any config/kind mismatch) — not
        // reconstructable from a bare delta. Caller keeps the unchanged
        // drop behavior.
        _ => None,
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
    algorithm: SketchAlgorithm,
    encoding: i32,
    existing: &mut Box<dyn AggregateCore>,
    bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    use asap_physical_operators::summary_kernels::{
        CountMinSketchAccumulator, CountSketchAccumulator, CountSketchWithHeapAccumulator,
        DDSketchAccumulator, HllSketchAccumulator,
    };

    match (encoding, algorithm) {
        (ENCODING_PROTO_DELTA, SketchAlgorithm::DDSketch) => {
            let dd = existing
                .as_any_mut()
                .downcast_mut::<DDSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a DDSketchAccumulator",
                )?;
            dd.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchAlgorithm::Hll) => {
            let hll = existing
                .as_any_mut()
                .downcast_mut::<HllSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not an HllSketchAccumulator",
                )?;
            hll.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchAlgorithm::CountSketch) => {
            let cs = existing
                .as_any_mut()
                .downcast_mut::<CountSketchAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a CountSketchAccumulator",
                )?;
            cs.apply_proto_delta_bytes(bytes)
        }
        (ENCODING_PROTO_DELTA, SketchAlgorithm::Cms) => {
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
        (ENCODING_MSGPACK_DELTA, SketchAlgorithm::CountSketch) => {
            // DELTA-HEAP frame for the heap-bearing CountSketch: a sparse
            // signed matrix delta + the full top-k heap. The cached base is
            // a heap accumulator (window-1 full frame decoded via
            // `from_msgpack_with_heap_bytes`); under the per-window-reset
            // model the ingest caller has already reset it to empty at a
            // window boundary, so applying the delta reconstructs the
            // window's own matrix and replaces the heap. Decoded generically
            // in `apply_msgpack_heap_delta_bytes` (rmp_serde, no
            // `asap_sketchlib` delta API). Real `CountSketchWithHeapAccumulator`
            // (median-of-signed-rows), not the CMS-family wrapper.
            let heap = existing
                .as_any_mut()
                .downcast_mut::<CountSketchWithHeapAccumulator>()
                .ok_or(
                    "apply_modified_otlp_delta_bytes: existing accumulator is \
                     not a CountSketchWithHeapAccumulator (heap-bearing \
                     CountSketch delta requires a heap base — the window-1 \
                     full frame must have promoted the sid)",
                )?;
            heap.apply_msgpack_heap_delta_bytes(bytes)
        }
        (ENCODING_MSGPACK_DELTA, other) => Err(format!(
            "MSGPACK_DELTA for sketch kind {other:?} is not yet wired; only \
             the heap-bearing CountSketch DELTA-HEAP frame is supported"
        )
        .into()),
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
                    Some(Data::SumAgg(sa)) => count += sa.data_points.len(),
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
                    Some(Data::SumAgg(sa)) => {
                        // First-class Sum AggregationType: each data point carries
                        // a SumState envelope ({sum,count}) in `sketch`. Decode it
                        // and feed the sum as a MetricPoint into the SAME
                        // ExactAgg(Sum) path as a plain delta Sum — the backend sums
                        // the per-window/per-shard partials for the same sid.
                        for dp in &sa.data_points {
                            let value = match asap_physical_operators::summary_kernels::sum::SumAccumulator::from_sum_bytes(&dp.sketch) {
                                Ok(acc) => acc.sum,
                                Err(e) => {
                                    debug!("asap_edge: SumAgg data point decode failed (skipping): {e}");
                                    continue;
                                }
                            };
                            let labels = merge_point_attributes(&base_labels, &dp.attributes);
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
                    // Modified-OTLP first-class sketch metric variants
                    // (Ddsketch / Kllsketch / Countsketch / Countminsketch /
                    // Hllsketch) and `None` are not handled here: this
                    // function only parses raw scalar metric points and
                    // attribute-embedded `SketchEnvelope` payloads. The
                    // first-class sketch DataPoints are decoded + routed by
                    // `route_modified_otlp_sketches_to_precompute`, so we
                    // intentionally ignore them in this parse pass.
                    _ => {}
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
mod canonical_metric_name_tests {
    //! Coverage for `canonical_sketch_metric_name` — the ingest-seam
    //! strip of the agent's `_<family>` suffix (ASAPCollector
    //! `asapedgeprocessor` sets `MetricSuffix: "_" + family`). Without
    //! it, warm-tier sketch queries against the raw metric name
    //! capability-miss because `SummarySeriesMetadata.metric_name` and
    //! the controller's streaming-config policy `metric` never line up.
    use super::*;

    #[test]
    fn strips_matching_family_suffix() {
        assert_eq!(
            canonical_sketch_metric_name("request_size_bytes_kll", SketchAlgorithm::Kll),
            "request_size_bytes"
        );
        assert_eq!(
            canonical_sketch_metric_name(
                "http_requests_total_latency_ms_kll",
                SketchAlgorithm::Kll
            ),
            "http_requests_total_latency_ms"
        );
        assert_eq!(
            canonical_sketch_metric_name("unique_users_per_min_hll", SketchAlgorithm::Hll),
            "unique_users_per_min"
        );
        assert_eq!(
            canonical_sketch_metric_name(
                "top_endpoint_qps_countsketch",
                SketchAlgorithm::CountSketch
            ),
            "top_endpoint_qps"
        );
        assert_eq!(
            canonical_sketch_metric_name(
                "endpoint_request_freq_countminsketch",
                SketchAlgorithm::Cms
            ),
            "endpoint_request_freq"
        );
        assert_eq!(
            canonical_sketch_metric_name("latency_ddsketch", SketchAlgorithm::DDSketch),
            "latency"
        );
    }

    #[test]
    fn leaves_bare_name_untouched_idempotent() {
        // Once agents stop suffixing (series-identity
        // Phase 1), the strip must be a no-op.
        assert_eq!(
            canonical_sketch_metric_name("request_size_bytes", SketchAlgorithm::Kll),
            "request_size_bytes"
        );
        assert_eq!(
            canonical_sketch_metric_name("unique_users_per_min", SketchAlgorithm::Hll),
            "unique_users_per_min"
        );
    }

    #[test]
    fn does_not_strip_suffix_of_a_different_family() {
        // A metric whose name happens to end in `_hll` but arrives as a
        // KLL sketch keeps its name — the strip is gated on the dp's
        // actual sketch kind, so we never collapse a legitimately-named
        // metric onto a different one.
        assert_eq!(
            canonical_sketch_metric_name("my_metric_hll", SketchAlgorithm::Kll),
            "my_metric_hll"
        );
    }

    #[test]
    fn never_collapses_to_empty_string() {
        // A metric literally named `_kll` (base would be empty) is left
        // intact rather than emptied.
        assert_eq!(
            canonical_sketch_metric_name("_kll", SketchAlgorithm::Kll),
            "_kll"
        );
    }
}

#[cfg(test)]
mod series_key_roundtrip_tests {
    //! Regression coverage for the `format_series_key` ↔
    //! `parse_labels_from_series_key` roundtrip bug discovered while
    //! shipping PR #284 (B7.6 ingest sid rekey). The formatter
    //! emitted unquoted `k=v` pairs but the parser required
    //! `k="v"`, which silently produced empty group keys for every
    //! OTLP wire-format input. These tests pin the canonical
    //! PromQL-style quoted format the data plane now uses
    //! throughout (see also `render_series_key` in
    //! `storage_engines/sketch_db/backfill/prometheus_reader.rs`).
    use super::*;
    use crate::precompute_engine::worker::{decode_label_value, parse_labels_from_series_key};

    fn roundtrip(name: &str, input: &[(&str, &str)]) {
        let labels: HashMap<String, String> = input
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let key = format_series_key(name, &labels);
        let parsed = parse_labels_from_series_key(&key);
        for (k, v) in input {
            let got = parsed
                .get(*k)
                .map(|raw| decode_label_value(raw).into_owned())
                .unwrap_or_else(|| panic!("label '{}' missing after roundtrip; key={}", k, key));
            assert_eq!(
                got, *v,
                "label '{}' value mismatch after roundtrip; key={}",
                k, key
            );
        }
        assert_eq!(
            parsed.len(),
            input.len(),
            "label count mismatch after roundtrip; key={} parsed={:?}",
            key,
            parsed
        );
    }

    #[test]
    fn format_series_key_emits_promql_quoted_form() {
        // The canonical shape every downstream parser
        // (`parse_labels_from_series_key`, `sample_matches`) expects.
        let mut labels = HashMap::new();
        labels.insert("svc".to_string(), "auth".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let key = format_series_key("latency", &labels);
        // Keys are sorted lexicographically so the formatted output
        // is deterministic regardless of HashMap iteration order.
        assert_eq!(key, r#"latency{env="prod",svc="auth"}"#);
    }

    #[test]
    fn roundtrip_simple_alphanumeric() {
        roundtrip("metric", &[("svc", "auth"), ("env", "prod")]);
    }

    #[test]
    fn roundtrip_value_with_comma() {
        // Comma is the pair delimiter — quoting must keep it inside
        // the value. Pre-fix the unquoted format would split mid-value.
        roundtrip("metric", &[("tag", "a,b,c"), ("svc", "auth")]);
    }

    #[test]
    fn roundtrip_value_with_equals() {
        // Equals is the key/value delimiter — quoting must protect it.
        roundtrip("metric", &[("expr", "x=y"), ("svc", "auth")]);
    }

    #[test]
    fn roundtrip_value_with_embedded_quote() {
        // `"` must be escaped as `\"` on emit and decoded back on
        // read. The parser's closing-quote scan must walk past the
        // escape; `decode_label_value` un-escapes the slice.
        roundtrip("metric", &[("msg", r#"hello "world""#), ("svc", "auth")]);
    }

    #[test]
    fn roundtrip_value_with_backslash() {
        roundtrip("metric", &[("path", r"C:\Users\app"), ("svc", "auth")]);
    }

    #[test]
    fn roundtrip_value_with_newline() {
        // `\n` round-trips through the `\n` escape; verifies the
        // decoder handles all three escape body variants.
        roundtrip("metric", &[("multi", "line1\nline2"), ("svc", "auth")]);
    }

    #[test]
    fn roundtrip_value_with_all_metacharacters() {
        // One stress case combining every escape body and every
        // pair-delimiter character in a single value.
        roundtrip("metric", &[("payload", "a,b=c\"d\\e\nf"), ("svc", "auth")]);
    }

    #[test]
    fn empty_labels_yield_bare_braces() {
        let labels: HashMap<String, String> = HashMap::new();
        let key = format_series_key("metric", &labels);
        assert_eq!(key, "metric{}");
        let parsed = parse_labels_from_series_key(&key);
        assert!(parsed.is_empty());
    }
}

#[cfg(test)]
mod policy_fp_lookup_tests {
    use super::*;
    use crate::storage_engines::sketch_db::data::SketchConfig;
    use crate::storage_engines::sketch_db::index::SketchAlgorithm;
    use asap_types::AggregationType;

    #[test]
    fn handle_to_agg_type_round_trips_canonical_kinds() {
        // Locks in the data-plane → control-plane name mapping.
        // Drift surfaces as policy lookups that silently miss because
        // the handle resolves to an `AggregationType` no policy uses.
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::DDSketch),
            Some(AggregationType::DDSketch)
        );
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::Kll),
            Some(AggregationType::DatasketchesKLL)
        );
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::Hll),
            Some(AggregationType::HLL)
        );
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::Cms),
            Some(AggregationType::CountMinSketch)
        );
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::CmsWithHeap),
            Some(AggregationType::CountMinSketchWithHeap)
        );
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::CountSketch),
            Some(AggregationType::CountSketch)
        );
        // `Any` is a control-plane wildcard, not a real DP shape.
        assert_eq!(
            aggregation_type_for_sketch_algorithm(SketchAlgorithm::Kmv),
            None
        );
    }

    #[test]
    fn sketch_config_to_params_uses_canonical_keys() {
        // The param-name vocabulary must match what the control plane
        // writes in streaming-config YAML (see
        // `asap_types::aggregation_config::PrecomputeMaterialization::from_yaml_data`).
        // Drift surfaces as `find_policy_by_content` missing matches.
        let dd = sketch_config_to_params(&SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        });
        assert_eq!(dd.get("relative_accuracy"), Some(&serde_json::json!(0.01)));

        let kll = sketch_config_to_params(&SketchConfig::Kll { k: 200 });
        assert_eq!(kll.get("k"), Some(&serde_json::json!(200)));

        let hll = sketch_config_to_params(&SketchConfig::Hll { precision: 14 });
        assert_eq!(hll.get("precision"), Some(&serde_json::json!(14)));

        let cs = sketch_config_to_params(&SketchConfig::CountSketch { rows: 4, cols: 256 });
        // Canonical keys: w (=cols, width) and d (=rows, depth) —
        // matches `control_plane::emit::stage_config::sketch_params_to_json`.
        assert_eq!(cs.get("w"), Some(&serde_json::json!(256)));
        assert_eq!(cs.get("d"), Some(&serde_json::json!(4)));

        let cm = sketch_config_to_params(&SketchConfig::CountMin { rows: 4, cols: 256 });
        assert_eq!(cm.get("w"), Some(&serde_json::json!(256)));
        assert_eq!(cm.get("d"), Some(&serde_json::json!(4)));
    }
}

#[cfg(test)]
mod dispatcher_tests {
    use super::*;
    use crate::storage_engines::types::AggregateCore;
    use asap_physical_operators::summary_kernels::{DDSketchAccumulator, HllSketchAccumulator};
    use asap_sketchlib::DdSketch;
    use asap_sketchlib::HllVariant;

    #[test]
    fn apply_modified_otlp_delta_bytes_ddsketch_round_trip() {
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use prost::Message;

        // Base sketch represents the last full snapshot the agent sent.
        let mut acc: Box<dyn AggregateCore> = Box::new(DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 2, 3], 0),
            sample_p: 1.0,
        });

        // The wire delta now carries only bucket deltas (tags 2-7
        // reserved post ProjectASAP/sketchlib-go#243 / asap_sketchlib#57).
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
        }
        .encode_to_vec();

        apply_modified_otlp_delta_bytes(
            SketchAlgorithm::DDSketch,
            ENCODING_PROTO_DELTA,
            &mut acc,
            &bytes,
        )
        .expect("apply ok");

        let dd = acc.as_any().downcast_ref::<DDSketchAccumulator>().unwrap();
        assert_eq!(dd.inner.store_counts, vec![11, 2, 23]);
        // `count` recomputed from the merged buckets: 11 + 2 + 23 = 36.
        assert_eq!(dd.inner.total_count(), 36);
    }

    #[test]
    fn apply_modified_otlp_delta_bytes_hll_round_trip() {
        use asap_otel_proto::sketchlib::v1::HllDelta as PbDelta;
        use prost::Message;

        let mut acc: Box<dyn AggregateCore> =
            Box::new(HllSketchAccumulator::new(HllVariant::Regular, 2));
        acc.as_any_mut()
            .downcast_mut::<HllSketchAccumulator>()
            .unwrap()
            .inner
            .registers = vec![1, 5, 3, 7];

        // Packed (index_delta, value) blob for updates {0:4, 2:6}.
        let bytes = PbDelta {
            packed_updates: vec![0, 4, 2, 6],
        }
        .encode_to_vec();

        apply_modified_otlp_delta_bytes(
            SketchAlgorithm::Hll,
            ENCODING_PROTO_DELTA,
            &mut acc,
            &bytes,
        )
        .expect("apply ok");

        let hll = acc.as_any().downcast_ref::<HllSketchAccumulator>().unwrap();
        assert_eq!(hll.inner.registers, vec![4, 5, 6, 7]);
    }

    #[test]
    fn apply_rejects_wrong_accumulator_type() {
        let mut acc: Box<dyn AggregateCore> =
            Box::new(HllSketchAccumulator::new(HllVariant::Regular, 2));
        let err = apply_modified_otlp_delta_bytes(
            SketchAlgorithm::DDSketch,
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
        let err = apply_modified_otlp_delta_bytes(
            SketchAlgorithm::DDSketch,
            ENCODING_PROTO,
            &mut acc,
            &[],
        )
        .expect_err("expected full-state-rejection error")
        .to_string();
        assert!(err.contains("full-state frame"));
    }

    #[test]
    fn decode_rejects_delta_encoding_with_helpful_message() {
        let err = match decode_modified_otlp_sketch_bytes(
            SketchAlgorithm::DDSketch,
            ENCODING_PROTO_DELTA,
            &[],
        ) {
            Ok(_) => panic!("expected PROTO_DELTA to be rejected by full-state decoder"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("apply_modified_otlp_delta_bytes"));
    }
}

/// sid-resolution gate tests. Construct an OTLP DDSketch
/// Export with one DataPoint per scenario, run it through
/// `route_modified_otlp_sketches_to_precompute`, and assert on the
/// returned `unknown_series_ids` plus the SeriesIdResolver / SketchStore
/// state on the shared IngestState.
#[cfg(test)]
mod sid_resolution_tests {
    use super::*;
    use crate::drivers::ingest::series_resolver::SeriesIdResolver;
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::storage_engines::sketch_db::index::SketchStore;
    use crate::storage_engines::types::{InstalledPrecomputePlan, InstalledPrecomputePlanHandle};
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
        let streaming = InstalledPrecomputePlan::new(std::collections::HashMap::new());
        let hot_reload = InstalledPrecomputePlanHandle::new(streaming.clone());
        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(SeriesIdResolver::new()),
            summary_store: Arc::new(SketchStore::new()),
            observability: crate::precompute_engine::ingest_handler::IngestObservability::default(),
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

        let outcome = route_modified_otlp_sketches_to_precompute(&req, &state)
            .await
            .expect("ingest succeeds");
        assert!(
            outcome.unknown_series_ids.is_empty(),
            "no unknown sids on a fresh-attrs DP"
        );
        // Option B — sid is resolver-allocated; every attrs-bearing DP
        // gets a SeriesAssignment echoed back so the sender caches it.
        assert_eq!(
            outcome.series_assignments.len(),
            1,
            "one series_assignment returned for one fresh DP"
        );
        let assigned = &outcome.series_assignments[0];
        assert_eq!(assigned.metric_name, "http_latency_ms");
        assert_ne!(
            assigned.series_id, 0,
            "resolver mints a non-zero sid (zero is reserved on the wire)"
        );
        assert_eq!(
            state.summary_store.instance_count(),
            1,
            "SketchStore registered one instance under the resolver-minted sid"
        );
        assert!(state.summary_store.instance(assigned.series_id).is_some());

        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn unknown_sid_with_empty_attrs_is_returned_in_response() {
        let (state, drain) = make_state().await;
        // sid != 0, no attrs — SketchStore doesn't know it; should land
        // in unknown_sids and the DP must be dropped (no instance
        // registered). The resolver can't be consulted without attrs.
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

        let outcome = route_modified_otlp_sketches_to_precompute(&req, &state)
            .await
            .expect("ingest succeeds");
        assert_eq!(outcome.unknown_series_ids, vec![7777]);
        assert!(
            outcome.series_assignments.is_empty(),
            "no assignment when attrs are missing"
        );
        assert_eq!(state.summary_store.instance_count(), 0);

        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn sid_attrs_disagreement_signals_stale_sid_but_uses_resolved_value() {
        let (state, drain) = make_state().await;
        // First, register the sid by sending sid=0 with attrs. The
        // resolver mints a fresh u64 (sequential, NOT content-addressed).
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
        let seed_outcome = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_seed),
            &state,
        )
        .await
        .expect("seed ingest succeeds");
        let assigned_sid = seed_outcome.series_assignments[0].series_id;
        assert!(
            state.summary_store.instance(assigned_sid).is_some(),
            "seed registers the resolver-minted sid"
        );

        // Now arrive with the same attrs but a STALE sid (sender's
        // local cache was wrong, e.g. survived a backend restart without
        // persistence).
        let stale = assigned_sid.wrapping_add(123);
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
        let outcome = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_disagree),
            &state,
        )
        .await
        .expect("ingest succeeds");
        assert_eq!(
            outcome.unknown_series_ids,
            vec![stale],
            "stale sid should be signalled for eviction"
        );
        assert_eq!(
            outcome.series_assignments.len(),
            1,
            "fresh assignment echoed so sender can refresh its cache"
        );
        assert_eq!(
            outcome.series_assignments[0].series_id, assigned_sid,
            "resolver returns the canonical sid for this (metric, attrs) — same as seed"
        );
        // The assigned sid stays registered — the second DP routed to
        // it via the resolver's cache hit.
        assert!(state.summary_store.instance(assigned_sid).is_some());

        drop(state);
        let _ = drain.await;
    }

    /// Per-window base rotation (`docs/delta-baseline-contract.md` §3):
    /// the backend must NOT accumulate deltas across windows. For one
    /// series, a full frame opens window 1, a delta in window 1 (same
    /// `start_time_unix_nano`) accumulates onto it, then a delta in
    /// window 2 (a NEW `start_time_unix_nano`) must reset the cached base
    /// to empty first — so the reconstructed state is window 2's delta
    /// only, NOT window1 + window2.
    ///
    /// Uses DDSketch (an additive family) so accumulation vs. reset is
    /// directly observable on the bucket counts.
    #[tokio::test]
    async fn delta_apply_rotates_per_series_base_at_window_boundary() {
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use asap_physical_operators::summary_kernels::DDSketchAccumulator;
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, DdSketchState, SketchEnvelope};
        use prost::Message;

        let (state, drain) = make_state().await;

        const WIN1_START: u64 = 1_000_000;
        const WIN2_START: u64 = 2_000_000;

        // Build a DDSketch DataPoint with explicit encoding / window-start
        // / payload so we can stage a full frame then per-window deltas.
        let make_dp = |start: u64, ts: u64, encoding: i32, sketch: Vec<u8>| DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: start,
            time_unix_nano: ts,
            sketch,
            encoding,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0,
        };

        // The cache key (series_key) is derived from the canonical metric
        // name + attrs; recompute it the same way the ingest loop does so
        // we can read the reconstructed base back out.
        let mut attrs = HashMap::new();
        attrs.insert("zone".to_string(), "z0".to_string());
        let series_key = format_series_key(
            canonical_sketch_metric_name("http_latency_ms", SketchAlgorithm::DDSketch),
            &attrs,
        );

        // ── Window 1: full frame. Base buckets [10, 0, 5]. ──
        let full_w1 = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(DdSketchState {
                alpha: 0.01,
                store_counts: vec![10, 0, 5],
                store_offset: 0,
                ..Default::default()
            })),
            ..Default::default()
        }
        .encode_to_vec();
        route_modified_otlp_sketches_to_precompute(
            &build_request(
                "http_latency_ms",
                make_dp(WIN1_START, 11_000_000, 1, full_w1),
            ),
            &state,
        )
        .await
        .expect("first ingest succeeds");

        // ── Window 1: delta (SAME window_start). Adds +3 to bucket 0,
        // +7 to bucket 1. Within the window this accumulates onto the
        // full frame → [13, 7, 5]. ──
        let delta_w1 = PbDelta {
            buckets: vec![
                DdSketchBucketDelta {
                    index: 0,
                    d_count: 3,
                },
                DdSketchBucketDelta {
                    index: 1,
                    d_count: 7,
                },
            ],
        }
        .encode_to_vec();
        route_modified_otlp_sketches_to_precompute(
            &build_request(
                "http_latency_ms",
                make_dp(WIN1_START, 12_000_000, 2, delta_w1),
            ),
            &state,
        )
        .await
        .expect("second ingest succeeds");

        {
            let entry = state
                .sketch_snapshots
                .get(&series_key)
                .expect("base cached after full + delta in window 1");
            let dd = entry
                .core
                .as_any()
                .downcast_ref::<DDSketchAccumulator>()
                .expect("DDSketch base");
            assert_eq!(
                dd.inner.store_counts,
                vec![13, 7, 5],
                "within window 1 the delta accumulates onto the full frame"
            );
            assert_eq!(
                entry.window_start, WIN1_START,
                "cached window_start tracks window 1"
            );
        }

        // ── Window 2: delta with a NEW window_start. Adds +20 to bucket
        // 2. With per-window base rotation the base is reset to empty
        // BEFORE this delta is applied, so the reconstructed state is
        // window 2 ONLY: count 20 — NOT window1 + window2 (count 45).
        let delta_w2 = PbDelta {
            buckets: vec![DdSketchBucketDelta {
                index: 2,
                d_count: 20,
            }],
        }
        .encode_to_vec();
        route_modified_otlp_sketches_to_precompute(
            &build_request(
                "http_latency_ms",
                make_dp(WIN2_START, 21_000_000, 2, delta_w2),
            ),
            &state,
        )
        .await
        .expect("ingest succeeds");

        {
            let entry = state
                .sketch_snapshots
                .get(&series_key)
                .expect("base still cached after window 2 delta");
            let dd = entry
                .core
                .as_any()
                .downcast_ref::<DDSketchAccumulator>()
                .expect("DDSketch base");
            // The base was rotated to empty before the window-2 delta, so
            // it holds window 2 ONLY: total count 20 (the +20 on bucket 2),
            // NOT window1 + window2 (which would be 13 + 7 + 5 + 20 = 45).
            // Asserting on `total_count` keeps the check independent of the
            // empty-sketch's store offset/layout (a fresh sketch re-bases
            // its store offset around the first touched bucket).
            assert_eq!(
                dd.inner.total_count(),
                20,
                "new window_start rotates the base to empty: state == window 2 only \
                 (count 20), NOT the all-time accumulation (45)"
            );
            // Only bucket 2 carries mass; the window-1 buckets (0 and 1)
            // were dropped by the rotation.
            let bucket_count = |abs_idx: i32| -> u64 {
                let i = abs_idx - dd.inner.store_offset;
                if i >= 0 && (i as usize) < dd.inner.store_counts.len() {
                    dd.inner.store_counts[i as usize]
                } else {
                    0
                }
            };
            assert_eq!(bucket_count(2), 20, "window 2's +20 lands on bucket 2");
            assert_eq!(
                bucket_count(0),
                0,
                "window 1's bucket 0 mass was rotated away"
            );
            assert_eq!(
                bucket_count(1),
                0,
                "window 1's bucket 1 mass was rotated away"
            );
            assert_eq!(
                entry.window_start, WIN2_START,
                "cached window_start advanced to window 2"
            );
        }

        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn second_emit_with_cached_sid_and_no_attrs_hits_same_instance() {
        // Round-trip: emit DP with attrs → cache the assignment →
        // re-emit with (sid, no attrs). Second emit must NOT push
        // anything to unknown_sids and MUST land in the same SketchStore
        // instance. This is the bandwidth-saving path the registry
        // approach is built for.
        let (state, drain) = make_state().await;

        let dp_first = DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 11_000_000,
            sketch: vec![1],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0,
        };
        let first = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_first),
            &state,
        )
        .await
        .expect("first ingest succeeds");
        let cached_sid = first.series_assignments[0].series_id;

        let dp_second = DdSketchDataPoint {
            attributes: Vec::new(), // sender omits attrs now that it has the sid
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 12_000_000,
            sketch: vec![2],
            encoding: 1,
            exemplars: Vec::new(),
            flags: 0,
            series_id: cached_sid,
        };
        let second = route_modified_otlp_sketches_to_precompute(
            &build_request("http_latency_ms", dp_second),
            &state,
        )
        .await
        .expect("second ingest succeeds");
        assert!(
            second.unknown_series_ids.is_empty(),
            "cached sid + no attrs hits the same SketchStore instance"
        );
        assert!(
            second.series_assignments.is_empty(),
            "no fresh assignment when sender already had a valid binding"
        );
        // Both DPs landed against the same sid — no proliferation.
        assert_eq!(state.summary_store.instance_count(), 1);

        drop(state);
        let _ = drain.await;
    }

    // ── P1-1/P1-2: leading delta (no prior full) for additive families ──

    /// Build an export request carrying one `CountMinSketch` DataPoint.
    fn build_cms_request(
        metric: &str,
        rows: i32,
        cols: i32,
        encoding: i32,
        sketch: Vec<u8>,
        start_ns: u64,
        ts_ns: u64,
    ) -> ExportMetricsServiceRequest {
        use asap_otel_proto::tonic::metrics::v1::{
            CountMinSketch as PbCms, CountMinSketchDataPoint as PbCmsDp,
        };
        let dp = PbCmsDp {
            attributes: vec![kv("svc", "auth")],
            start_time_unix_nano: start_ns,
            time_unix_nano: ts_ns,
            sketch,
            encoding,
            flags: 0,
            series_id: 0,
        };
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![PbMetric {
                        name: metric.to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(Data::Countminsketch(PbCms {
                            data_points: vec![dp],
                            aggregation_temporality: 0,
                            rows,
                            cols,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// Build an export request carrying one `HllSketch` DataPoint.
    fn build_hll_request(
        metric: &str,
        precision: u32,
        encoding: i32,
        sketch: Vec<u8>,
        start_ns: u64,
        ts_ns: u64,
    ) -> ExportMetricsServiceRequest {
        use asap_otel_proto::tonic::metrics::v1::{
            HllSketch as PbHll, HllSketchDataPoint as PbHllDp,
        };
        let dp = PbHllDp {
            attributes: vec![kv("svc", "auth")],
            start_time_unix_nano: start_ns,
            time_unix_nano: ts_ns,
            sketch,
            encoding,
            flags: 0,
            series_id: 0,
        };
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![PbMetric {
                        name: metric.to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(Data::Hllsketch(PbHll {
                            data_points: vec![dp],
                            aggregation_temporality: 0,
                            precision,
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// P1-1/P1-2 — a CMS PROTO_DELTA frame that is the FIRST frame for its
    /// series (no prior full snapshot) must NOT be dropped: the ingest
    /// path bootstraps an empty CMS of the frame's (rows, cols) and
    /// applies the delta onto it, recovering the window's state. This is
    /// what makes the worker tier agree with the warm-read tier and
    /// recover after a backend restart.
    #[tokio::test]
    async fn leading_cms_delta_bootstraps_onto_empty_base() {
        use asap_otel_proto::sketchlib::v1::CountMinDelta as PbDelta;
        use asap_physical_operators::summary_kernels::CountMinSketchAccumulator;
        use prost::Message;

        let (state, drain) = make_state().await;

        const ROWS: i32 = 4;
        const COLS: i32 = 8;
        const WIN_START: u64 = 1_000_000;

        // Sparse delta: +5 at (0,1), +9 at (2,3). All cells within 4x8.
        let delta = PbDelta {
            rows: ROWS as u32,
            cols: COLS as u32,
            cell_rows: vec![0u32, 2u32],
            cell_cols: vec![1u32, 3u32],
            d_counts: vec![5i64, 9i64],
            l1: vec![5.0, 0.0, 9.0, 0.0],
            l2: vec![25.0, 0.0, 81.0, 0.0],
        }
        .encode_to_vec();

        let req = build_cms_request(
            "frequency_metric",
            ROWS,
            COLS,
            ENCODING_PROTO_DELTA,
            delta,
            WIN_START,
            11_000_000,
        );
        // No prior full frame for this series — pre-fix this DP was
        // dropped (decoded_failed). Post-fix it bootstraps + applies.
        route_modified_otlp_sketches_to_precompute(&req, &state)
            .await
            .expect("ingest succeeds");

        // The per-series base is now cached, holding the window's
        // reconstructed matrix.
        let mut attrs = HashMap::new();
        attrs.insert("svc".to_string(), "auth".to_string());
        let series_key = format_series_key(
            canonical_sketch_metric_name("frequency_metric", SketchAlgorithm::Cms),
            &attrs,
        );
        {
            let entry = state
                .sketch_snapshots
                .get(&series_key)
                .expect("leading CMS delta bootstrapped + cached a base");
            let cms = entry
                .core
                .as_any()
                .downcast_ref::<CountMinSketchAccumulator>()
                .expect("base is a CountMinSketchAccumulator");
            let matrix = cms.inner.sketch();
            assert_eq!(
                matrix[0][1], 5.0,
                "delta cell (0,1) applied onto empty base"
            );
            assert_eq!(
                matrix[2][3], 9.0,
                "delta cell (2,3) applied onto empty base"
            );
            assert_eq!(matrix[0][0], 0.0, "untouched cell stays empty");
        }
        // The DD/KLL "no base" drop counter must NOT have ticked — CMS is
        // bootstrappable.
        assert_eq!(
            state
                .observability
                .dropped_no_base
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "CMS leading delta is bootstrapped, not dropped as no-base"
        );

        drop(state);
        let _ = drain.await;
    }

    /// P1-1/P1-2 — same as above but for HLL: a leading PROTO_DELTA frame
    /// bootstraps onto an empty HLL of the frame's precision and applies
    /// the register-max updates.
    #[tokio::test]
    async fn leading_hll_delta_bootstraps_onto_empty_base() {
        use asap_otel_proto::sketchlib::v1::HllDelta as PbDelta;
        use asap_physical_operators::summary_kernels::HllSketchAccumulator;
        use prost::Message;

        let (state, drain) = make_state().await;

        const PRECISION: u32 = 2; // 2^2 = 4 registers
        const WIN_START: u64 = 1_000_000;

        // Packed (index, value) updates: set register 0 -> 4, register 2 -> 6.
        let delta = PbDelta {
            packed_updates: vec![0, 4, 2, 6],
        }
        .encode_to_vec();

        let req = build_hll_request(
            "cardinality_metric",
            PRECISION,
            ENCODING_PROTO_DELTA,
            delta,
            WIN_START,
            11_000_000,
        );
        route_modified_otlp_sketches_to_precompute(&req, &state)
            .await
            .expect("ingest succeeds");

        let mut attrs = HashMap::new();
        attrs.insert("svc".to_string(), "auth".to_string());
        let series_key = format_series_key(
            canonical_sketch_metric_name("cardinality_metric", SketchAlgorithm::Hll),
            &attrs,
        );
        {
            let entry = state
                .sketch_snapshots
                .get(&series_key)
                .expect("leading HLL delta bootstrapped + cached a base");
            let hll = entry
                .core
                .as_any()
                .downcast_ref::<HllSketchAccumulator>()
                .expect("base is an HllSketchAccumulator");
            // Empty base registers are all 0; the delta sets max(0,4)=4 and
            // max(0,6)=6 on registers 0 and 2.
            assert_eq!(hll.inner.registers[0], 4, "register 0 set to 4");
            assert_eq!(hll.inner.registers[2], 6, "register 2 set to 6");
        }
        assert_eq!(
            state
                .observability
                .dropped_no_base
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "HLL leading delta is bootstrapped, not dropped as no-base"
        );

        drop(state);
        let _ = drain.await;
    }

    /// P1-1/P1-2 — DDSketch is NOT bootstrappable from a bare delta, so a
    /// leading DDSketch delta is still dropped and counted under
    /// `dropped_no_base`. Confirms the fix is scoped to the additive
    /// families and leaves DD/KLL behavior unchanged.
    #[tokio::test]
    async fn leading_ddsketch_delta_still_dropped_as_no_base() {
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use prost::Message;

        let (state, drain) = make_state().await;
        let delta = PbDelta {
            buckets: vec![DdSketchBucketDelta {
                index: 0,
                d_count: 10,
            }],
        }
        .encode_to_vec();
        let dp = DdSketchDataPoint {
            attributes: vec![kv("zone", "z0")],
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 11_000_000,
            sketch: delta,
            encoding: ENCODING_PROTO_DELTA,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0,
        };
        route_modified_otlp_sketches_to_precompute(&build_request("dd_latency_ms", dp), &state)
            .await
            .expect("ingest succeeds");

        let mut attrs = HashMap::new();
        attrs.insert("zone".to_string(), "z0".to_string());
        let series_key = format_series_key(
            canonical_sketch_metric_name("dd_latency_ms", SketchAlgorithm::DDSketch),
            &attrs,
        );
        assert!(
            state.sketch_snapshots.get(&series_key).is_none(),
            "DDSketch leading delta is dropped (no bootstrap), so nothing cached"
        );
        assert_eq!(
            state
                .observability
                .dropped_no_base
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "DDSketch leading delta counted as a no-base drop"
        );

        drop(state);
        let _ = drain.await;
    }

    // ── P1-4: heap capability upgrade ──

    /// P1-4 (a) — a CMS sid first registered from a heap-LESS frame
    /// (`FrequencyEstimate(CountMin)`) must UPGRADE to
    /// `FrequencyTopk(CmsWithHeap)` when a later heap-bearing MSGPACK
    /// frame arrives for the same sid. One-way; never downgrades.
    #[tokio::test]
    async fn heap_bearing_frame_upgrades_cms_sid_capability() {
        use crate::storage_engines::sketch_db::index::{Capability, SketchAlgorithm};
        use asap_sketchlib::{CountMinSketchWithHeap, MessagePackCodec};

        let (state, drain) = make_state().await;

        const ROWS: i32 = 4;
        const COLS: i32 = 8;

        // ── Frame 1: heap-LESS plain CMS msgpack. Registers the sid as
        // FrequencyEstimate(CountMin). ──
        let plain = asap_sketchlib::CountMinSketch::new(ROWS as usize, COLS as usize);
        let plain_bytes = plain.to_msgpack().expect("serialize plain CMS msgpack");
        let req1 = build_cms_request(
            "topk_metric",
            ROWS,
            COLS,
            ENCODING_MSGPACK,
            plain_bytes,
            1_000_000,
            11_000_000,
        );
        let out1 = route_modified_otlp_sketches_to_precompute(&req1, &state)
            .await
            .expect("ingest succeeds");
        let sid = out1.series_assignments[0].series_id;
        let meta1 = state.summary_store.instance(sid).expect("sid registered");
        assert_eq!(
            meta1.capability,
            Some(Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms))),
            "heap-less first frame registers FrequencyEstimate(CountMin)"
        );

        // ── Frame 2: heap-BEARING CMS-with-heap msgpack for the SAME
        // (metric, attrs) → same sid. Must upgrade the capability. ──
        let mut heap = CountMinSketchWithHeap::new(ROWS as usize, COLS as usize, 4);
        heap.update("hot_key", 100.0);
        heap.update("hot_key", 50.0);
        assert!(
            !heap.topk_heap_items().is_empty(),
            "heap frame carries a non-empty top-k heap"
        );
        let heap_bytes = heap.to_msgpack().expect("serialize CMS-with-heap msgpack");
        let req2 = build_cms_request(
            "topk_metric",
            ROWS,
            COLS,
            ENCODING_MSGPACK,
            heap_bytes,
            1_000_000,
            12_000_000,
        );
        route_modified_otlp_sketches_to_precompute(&req2, &state)
            .await
            .expect("ingest succeeds");

        let meta2 = state
            .summary_store
            .instance(sid)
            .expect("sid still registered");
        assert_eq!(
            meta2.capability,
            Some(Capability::FrequencyTopk(Some(
                SketchAlgorithm::CmsWithHeap
            ))),
            "heap-bearing frame upgrades the sid to FrequencyTopk(CmsWithHeap)"
        );
        // Still one instance — the upgrade is an in-place overwrite, not a
        // new sid.
        assert_eq!(state.summary_store.instance_count(), 1);

        // ── Frame 3: a later heap-LESS frame must NOT downgrade. ──
        let plain2 = asap_sketchlib::CountMinSketch::new(ROWS as usize, COLS as usize);
        let plain2_bytes = plain2.to_msgpack().expect("serialize plain CMS msgpack");
        let req3 = build_cms_request(
            "topk_metric",
            ROWS,
            COLS,
            ENCODING_MSGPACK,
            plain2_bytes,
            1_000_000,
            13_000_000,
        );
        route_modified_otlp_sketches_to_precompute(&req3, &state)
            .await
            .expect("ingest succeeds");
        let meta3 = state
            .summary_store
            .instance(sid)
            .expect("sid still registered");
        assert_eq!(
            meta3.capability,
            Some(Capability::FrequencyTopk(Some(
                SketchAlgorithm::CmsWithHeap
            ))),
            "a later heap-less frame never downgrades the capability"
        );

        drop(state);
        let _ = drain.await;
    }

    // ── P1-5: empty-attrs (global-aggregation) series can mint a sid ──

    /// P1-5 — a sketch DataPoint with NO attributes (global aggregation,
    /// no resource/scope/DP attrs) must still resolve to a sid via the
    /// resolver (fingerprint `""`) and register a SketchStore instance,
    /// instead of being dropped by the store-lookup-only branch.
    #[tokio::test]
    async fn attr_less_sketch_dp_mints_a_sid() {
        let (state, drain) = make_state().await;

        let dp = DdSketchDataPoint {
            attributes: Vec::new(), // global aggregation — no attrs at all
            start_time_unix_nano: 1_000_000,
            time_unix_nano: 11_000_000,
            sketch: vec![1, 2, 3],
            encoding: ENCODING_PROTO,
            exemplars: Vec::new(),
            flags: 0,
            series_id: 0, // first emit, no cached sid
        };
        let out = route_modified_otlp_sketches_to_precompute(
            &build_request("global_latency_ms", dp),
            &state,
        )
        .await
        .expect("ingest succeeds");

        // Pre-fix: dropped (sid=0 + no attrs → None). Post-fix: resolver
        // mints a stable sid for (metric, "", agg_kind) and echoes an
        // assignment with an empty fingerprint.
        assert_eq!(
            out.series_assignments.len(),
            1,
            "attr-less DP resolves and echoes one assignment"
        );
        let assigned = &out.series_assignments[0];
        assert_ne!(assigned.series_id, 0, "resolver mints a non-zero sid");
        assert!(
            assigned.attributes_fingerprint.is_empty(),
            "global series carries the empty fingerprint"
        );
        assert!(
            out.unknown_series_ids.is_empty(),
            "no unknown sids — the DP was ingestable, not dropped"
        );
        assert_eq!(
            state.summary_store.instance_count(),
            1,
            "SketchStore registered the global-aggregation instance"
        );
        assert!(state.summary_store.instance(assigned.series_id).is_some());

        drop(state);
        let _ = drain.await;
    }
}

// ── B7.6 regression: ingest bucketing is keyed by sid, not (agg_id, group_key) ──
#[cfg(test)]
mod sid_bucketing_tests {
    //! Pin the B7.6 contract: ingest dispatches to the precompute engine
    //! with `(sid, policy_fp, group_key)` on every `GroupSamples` /
    //! `AccumulatorInput`, and distinct group_key values mint distinct
    //! sids that round-trip through `SeriesIdResolver::lookup`. Tests
    //! the actual `route_otlp_to_precompute` path end-to-end so a
    //! refactor that drops the per-DP sid-resolve call (or routes by
    //! agg_id) breaks here.
    use super::*;
    use crate::drivers::ingest::series_resolver::SeriesIdResolver;
    use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
    use crate::storage_engines::sketch_db::index::SketchStore;
    use crate::storage_engines::types::{InstalledPrecomputePlan, InstalledPrecomputePlanHandle};
    use asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use asap_otel_proto::tonic::common::v1::{any_value::Value as AnyVal, AnyValue, KeyValue};
    use asap_otel_proto::tonic::metrics::v1::{
        metric::Data, number_data_point::Value as NumberValue, Gauge as PbGauge,
        Metric as PbMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use asap_types::aggregation_config::PrecomputeMaterialization;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(AnyVal::StringValue(v.to_string())),
            }),
        }
    }

    fn sum_agg_config(metric: &str, grouping: &[&str]) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
            AggregationType::SingleSubpopulation,
            "Sum".to_string(),
            HashMap::new(),
            KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            10,
            10,
            WindowKind::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    /// Build a Gauge request with one DataPoint per (zone, value) entry.
    /// Distinct `zone` values are the two grouping-label buckets the
    /// test inspects.
    fn build_gauge_request(metric: &str, points: &[(&str, f64)]) -> ExportMetricsServiceRequest {
        let data_points = points
            .iter()
            .map(|(zone, val)| NumberDataPoint {
                attributes: vec![kv("zone", zone)],
                start_time_unix_nano: 1_000_000,
                time_unix_nano: 11_000_000,
                value: Some(NumberValue::AsDouble(*val)),
                exemplars: Vec::new(),
                flags: 0,
                series_id: 0,
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![PbMetric {
                        name: metric.to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(Data::Gauge(PbGauge { data_points })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// Two distinct `zone` values under one policy → two distinct
    /// `GroupSamples` messages, each keyed by the sid the resolver
    /// minted for the corresponding `(metric, zone-only-attrs,
    /// ExactAgg-of-config)` tuple.
    ///
    /// Pre-B7.6 this path dispatched `WorkerMessage::GroupSamples {
    /// agg_id, group_key, ... }` and the worker bucketed by
    /// `(agg_id, group_key)`. The contract this test pins is:
    ///   - exactly two `WorkerMessage::GroupSamples` are emitted
    ///   - their sids are non-zero and distinct
    ///   - each sid equals what `SeriesIdResolver::lookup` records for
    ///     `(metric, "zone=<zv>;", ExactAgg-canonical)` — i.e. the
    ///     bucket identity is folded into sid via the resolver
    ///   - policy_fp = config.policy_fp_u64() on every message
    ///   - samples in each bucket are exactly the DPs whose `zone`
    ///     attribute matches that bucket (the GROUP-BY semantic)
    ///
    /// Note: the `group_key` field on the message comes from
    /// `IngestState::extract_group_key_for(series_key, config)`. Pre-
    /// PR-after-#284 a quoting mismatch between `format_series_key`
    /// (unquoted) and `parse_labels_from_series_key` (quoted) caused
    /// this to return the empty string for OTLP wire inputs; the
    /// follow-up PR fixed the formatter to emit the canonical
    /// PromQL `k="v"` form and added a roundtrip regression in
    /// `series_key_roundtrip_tests`. Bucketing was always correct
    /// here because B7.6 routes by sid (read directly from
    /// `point.labels`, not the joined series_key) — the
    /// group_key value is informational only for this test.
    #[tokio::test]
    async fn raw_otlp_buckets_by_sid_with_distinct_group_keys() {
        // Channel large enough to capture all routed messages without
        // blocking the dispatch loop.
        let (tx, mut rx) = mpsc::channel::<WorkerMessage>(64);
        let router = SeriesRouter::new(vec![tx]);

        let metric = "cpu_seconds";
        let cfg = sum_agg_config(metric, &["zone"]);
        let policy_fp = asap_types::PolicyFingerprint(cfg.policy_fp_u64());
        let mut configs = HashMap::new();
        configs.insert(cfg.policy_fp_u64(), cfg.clone());
        let streaming = InstalledPrecomputePlan::new(configs);
        let hot_reload = InstalledPrecomputePlanHandle::new(streaming);

        let resolver = Arc::new(SeriesIdResolver::new());
        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: resolver.clone(),
            summary_store: Arc::new(SketchStore::new()),
            observability: crate::precompute_engine::ingest_handler::IngestObservability::default(),
        });

        // Two zones × two DPs each. The two zones must produce two
        // separate buckets; the two DPs within a zone must accumulate
        // into the same bucket.
        let req = build_gauge_request(
            metric,
            &[("z0", 1.0), ("z0", 2.0), ("z1", 10.0), ("z1", 20.0)],
        );

        // PERF-1 — parse once and pass the slices, matching the receiver
        // path's new shape.
        let (points, sketch_payloads) = otlp_to_metric_points_and_sketches(&req);
        route_otlp_to_precompute(&points, &sketch_payloads, &state).await;

        // Drain the messages the dispatcher emitted (one per bucket).
        let mut messages: Vec<WorkerMessage> = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }

        // Filter to GroupSamples — the only variant raw OTLP emits.
        let groups: Vec<(
            u64,
            asap_types::PolicyFingerprint,
            Arc<crate::precompute_engine::group_key::GroupKey>,
            Vec<(String, i64, f64)>,
        )> = messages
            .into_iter()
            .filter_map(|m| match m {
                WorkerMessage::GroupSamples {
                    sid,
                    policy_fp,
                    group_key,
                    samples,
                    ..
                } => Some((sid, policy_fp, group_key, samples)),
                _ => None,
            })
            .collect();

        assert_eq!(
            groups.len(),
            2,
            "exactly two buckets (one per zone) — observed {} messages",
            groups.len()
        );

        // Both buckets carry the same policy_fp (one source config).
        for (_, pf, _, _) in &groups {
            assert_eq!(
                *pf, policy_fp,
                "policy_fp must equal config.policy_fp_u64()"
            );
        }

        // sids must be non-zero (zero is reserved on the wire) and distinct.
        let mut sids: Vec<u64> = groups.iter().map(|(s, _, _, _)| *s).collect();
        sids.sort();
        sids.dedup();
        assert_eq!(sids.len(), 2, "two distinct sids — one per group_key");
        assert!(sids.iter().all(|&s| s != 0), "sid 0 is reserved");

        // Each sid must match what the resolver records for its bucket
        // identity: (metric, "zone=<zv>;", ExactAgg-canonical). Use
        // the bucket's sample values to identify which zone it
        // represents (sample-value-based identification is robust
        // regardless of group_key shape — the test pin is on sid
        // assignment, not on group_key content), then verify the
        // sid matches the resolver mint for THAT zone.
        let agg_kind_canonical =
            crate::storage_engines::sketch_db::data::materialization_kind_for_config(&cfg);
        for (sid, _, _, samples) in &groups {
            let mut vals: Vec<f64> = samples.iter().map(|(_, _, v)| *v).collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let zone_for_bucket: &str = match vals.as_slice() {
                [1.0, 2.0] => "z0",
                [10.0, 20.0] => "z1",
                other => panic!("unexpected bucket sample values: {other:?}"),
            };
            let fp =
                crate::drivers::ingest::canonical_attrs_fingerprint(&[("zone", zone_for_bucket)]);
            let resolved = state
                .summary_store
                .resolve_output_storage_handle(&resolver, policy_fp.into(), &fp, None)
                .ok();
            assert_eq!(
                resolved,
                Some(*sid),
                "sid for zone={zone_for_bucket} (inferred from sample values) must equal \
                 resolver mint for (metric={metric}, fp={fp}, agg_kind={agg_kind_canonical})",
            );
        }
    }

    #[test]
    fn summary_frame_identity_is_parsed_and_removed_from_series_labels() {
        let mut attrs = HashMap::from([
            ("service".into(), "api".into()),
            ("asap.frame.identity_version".into(), "1".into()),
            ("asap.frame.plan_id".into(), "42".into()),
            ("asap.frame.plan_version".into(), "3".into()),
            (
                "asap.frame.backend_compat".into(),
                "asap-query-backend.v1".into(),
            ),
            ("asap.frame.materialization".into(), "99".into()),
            (
                "asap.frame.series_identity".into(),
                "service=checkout,zone=a".into(),
            ),
            ("asap.frame.schema_id".into(), "schema-99".into()),
            ("asap.frame.producer_id".into(), "edge-a".into()),
            ("asap.frame.producer_epoch".into(), "boot-7".into()),
            ("asap.frame.sequence".into(), "8".into()),
            ("asap.frame.kind".into(), "full".into()),
            ("asap.frame.encoding".into(), "sketchlib_protobuf_v1".into()),
            ("asap.frame.checkpoint_id".into(), "cp-8".into()),
        ]);
        let frame = take_summary_frame_identity(&mut attrs, 100, 200).expect("valid identity");
        assert_eq!(frame.plan_id, 42);
        assert_eq!(frame.plan_version, 3);
        assert_eq!(
            frame.materialization,
            asap_types::PolicyFingerprint(99).into()
        );
        assert_eq!(attrs, HashMap::from([("service".into(), "api".into())]));
    }
}
