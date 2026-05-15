//! Phase B (MVP v6) — turn a typed L5 [`StageConfig`] map (produced by
//! [`crate::physical::colored_dag::ThreeStageEmitter`]) into the **wire bytes** the
//! three executors actually consume:
//!
//! - [`emit_edge_yaml`] → OTel-collector YAML for the edge agent (OTLP
//!   receiver → per-sketch processor(s) → OTLP exporter to gateway).
//! - [`emit_gateway_yaml`] → OTel-collector YAML for the gateway
//!   aggregator (OTLP receiver → per-family `*merge` processor(s) → OTLP
//!   exporter to backend).
//! - [`emit_backend_streaming_config_json`] → JSON document matching the
//!   ASAPQuery-backend `POST /api/v1/streaming-config` API surface — same
//!   shape that [`crate::config::asapquery_backend::generate_streaming_config_yaml`]
//!   builds today, just from the typed [`BackendStageConfig`] instead of
//!   a `CollectionPlan`.
//! - [`emit_backend_storage_routing`] → JSON document matching the
//!   ASAPQuery-backend `POST /api/v1/storage_routing` API surface —
//!   per-metric query-shape → engine routing table (Phase α). Sources
//!   the per-metric sketch families from the typed [`BackendStageConfig`]
//!   inputs and turns them into `(metric, [target])` rows the backend's
//!   HTTP query handler consults via `BackendStorageRouting::lookup_with_shape`.
//!
//! These four functions are deliberately **stage-shaped**, not
//! plan-shaped: the typed L5 emitter has already split the PhysicalExpr
//! across edge / gateway / backend, so each function only sees the slice
//! that's relevant to its executor. The legacy `agent.rs` emitter still
//! operates on the flat `AgentCollectorConfig`; the legacy backend
//! emitter targeted a "backend-role" OTel merge collector tier that was
//! never deployed and has been retired — typed L5 routes `StageId::Backend`
//! directly to asapquery-backend's precompute engine over HTTP via
//! `emit_backend_streaming_config_json`.
//!
//! All three are pure transformations: no I/O, no env lookup. The
//! `opamp_endpoint` parameter is the controller's WebSocket URL the
//! emitted YAML's `extensions.opamp` block must point at; the caller
//! threads it through from `AppState::opamp_endpoint`.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;

use crate::physical::colored_dag::emitter::{
    AggregationInput, ArchiveTierMetric, BackendAggregation, BackendReadout, BackendStageConfig,
    EdgeSketchProcessor, EdgeStageConfig, ExportTarget, GatewayMergeProcessor, GatewayStageConfig,
    PrometheusArchiveMetric,
};
use crate::physical::colored_dag::stage_id::StageId;
use crate::sketch_algebra::params::{SketchKind, SketchParams};
use crate::sketch_algebra::physical_expr::EstimateOp;

// ── YAML structural types ─────────────────────────────────────────────────────
//
// These mirror the structural types in `config::agent`. We keep a
// private copy here rather than re-exporting because the L5 typed path
// has slightly different shape constraints (e.g. no `series_id_ttl` on
// the receiver block — that's a wire-layer concern Phase G+ owns).

#[derive(Serialize)]
struct CollectorYaml {
    extensions: HashMap<String, Value>,
    receivers: HashMap<String, Value>,
    processors: HashMap<String, Value>,
    /// OTel collector v0.106+ ships the `routing` component as a
    /// **connector**, not a processor (`routingprocessor` was
    /// deprecated and removed). Connectors live in their own
    /// top-level block and are referenced as both an exporter (entry
    /// pipeline) and a receiver (each downstream pipeline).
    /// Empty for legacy single-pipeline / Mode-3 / warm-passthrough
    /// emit paths — preserved by `skip_serializing_if` so the YAML
    /// shape doesn't gain an empty `connectors: {}` block.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    connectors: HashMap<String, Value>,
    exporters: HashMap<String, Value>,
    service: ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    extensions: Vec<String>,
    pipelines: HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers: Vec<String>,
    processors: Vec<String>,
    exporters: Vec<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Build the OTel-collector YAML for an edge agent from the typed L5
/// [`EdgeStageConfig`] payload.
///
/// The `opamp_endpoint` is embedded under `extensions.opamp.server.ws.endpoint`
/// so the agent can receive runtime config updates without restart.
///
/// The emitter does NOT resolve `ExportTarget::Stage(_)` to a concrete
/// network address; Phase C plumbs a `DeploymentConstraints` resolver
/// that maps the symbolic stage role to e.g. `gateway:4317`. Until
/// then, we emit a documented placeholder (`gateway:4317`) so the YAML
/// is syntactically valid and round-trips through Otel's loader for
/// integration tests.
pub fn emit_edge_yaml(cfg: &EdgeStageConfig, opamp_endpoint: &str) -> Result<String> {
    // ── MVP §46: 5-sketch routing-connector dispatch ───────────────────────
    //
    // When the planner has populated `cfg.metric_to_family` (the per-metric
    // → SketchKind table sourced from the workload spec), we switch to the
    // canonical 5-sketch routing-connector wire shape: all referenced
    // sketch processors live at the top level, the OTel `routing`
    // *connector* (NOT the deprecated routing processor) lives under
    // `connectors:`, and a fan-out of per-family pipelines (DDSketch /
    // KLL / HLL / CountSketch / CountMinSketch) plus a `raw_passthrough`
    // default each consume from the connector. This is the shape the
    // asap-otel binary's builder-config registers for OTel collector
    // v0.106+ where `routingprocessor` was removed.
    //
    // Empty `metric_to_family` ⇒ legacy single-pipeline / Mode-3 /
    // warm-passthrough emit paths kick in (preserved verbatim below).
    if !cfg.metric_to_family.is_empty() {
        return emit_edge_yaml_5sketch_routing(cfg, opamp_endpoint);
    }

    // ── Receivers ─────────────────────────────────────────────────────────────
    // Edge agents accept OTLP gRPC on 4317 + HTTP on 4318. Phase B does
    // not yet plumb an alternate port through `EdgeStageConfig`; if/when
    // that field is added, swap the literal here for a `cfg.otlp_port`
    // read.
    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    )
    .context("parse static OTLP receiver block")?;

    // ── Processors ────────────────────────────────────────────────────────────
    // One processor per `EdgeSketchProcessor`. Names come straight from
    // `EdgeSketchProcessor::processor_name` (already resolved by
    // `emitter::edge_processor_name`) and the param block is built from
    // the typed `SketchParams` payload.
    let mut processors: HashMap<String, Value> = HashMap::new();
    let mut sketch_pipeline_processors: Vec<String> = Vec::new();
    for sp in &cfg.sketch_processors {
        let block = build_edge_processor_block(
            sp,
            cfg.window_secs,
            &cfg.label_filters,
            cfg.source_metric.as_deref(),
        );
        // Use the processor_name verbatim as the YAML key — matches the
        // factory `Type` strings the patched OTel-contrib build registers
        // (see `opentelemetry-collector-contrib-patch/processor/*processor/factory.go`).
        processors.insert(sp.processor_name.clone(), block);
        sketch_pipeline_processors.push(sp.processor_name.clone());
    }

    // ── Phase 3.2.5 Bug (a): Gorilla-S3 archive processor block ──────────────
    // When the plan includes any archive-tier metric (a freshness-probe
    // archive metric, a `RawAtEdgePrometheusArchive` Mode-3 metric, or
    // any other metric the routing table claims `thanos_query`
    // for), the agent's pipeline MUST run the `gorillas3` processor so
    // the metric's samples land in MinIO. Without this, freshness probes
    // (and any other archive-bound metric) never reach the cold tier and
    // the ASAP-tier engine's `last_over_time(...)` returns empty.
    //
    // Config matches `deploy/configs/asap-otel-agent-b6-asap-single-sketch.yaml`
    // — `block_format: prometheus_tsdb` so the Thanos store-gateway can
    // read the emitted blocks; `drop_original: false` so the metric also
    // flows downstream to the ASAP-tier sketch / OTLP exporter; the
    // `window_interval` is the smallest `window_secs` declared on any
    // archive-tier metric (defaults to 60s).
    let has_archive_tier = !cfg.archive_tier_metrics.is_empty();
    if has_archive_tier {
        let window_secs: u64 = cfg
            .archive_tier_metrics
            .iter()
            .filter_map(|m| m.window_secs)
            .min()
            .unwrap_or(60);
        let gorillas3_yaml = format!(
            "window_interval: {window_secs}s\n\
drop_original: false\n\
endpoint: \"${{ASAP_MINIO_ENDPOINT:-http://minio:9000}}\"\n\
bucket: \"${{ASAP_GORILLA_BUCKET:-asap-gorilla}}\"\n\
region: us-east-1\n\
use_ssl: false\n\
access_key_id: \"${{ASAP_MINIO_ACCESS_KEY:-asap}}\"\n\
secret_access_key: \"${{ASAP_MINIO_SECRET_KEY:-asap-local-only}}\"\n\
prefix_template: \"{{tenant}}/{{metric}}/{{YYYY}}/{{MM}}/{{DD}}/{{HH}}/\"\n\
tenant: \"${{ASAP_TENANT:-default}}\"\n\
max_retries: 3\n\
retry_backoff: 1s\n\
upload_timeout: 30s\n\
block_format: prometheus_tsdb\n\
tsdb_bucket: \"${{ASAP_GORILLA_TSDB_BUCKET:-asap-gorilla-tsdb}}\"\n\
tsdb_block_duration: {window_secs}s\n",
        );
        let gorillas3: Value =
            serde_yaml::from_str(&gorillas3_yaml).context("parse gorillas3 processor block")?;
        processors.insert("gorillas3".to_string(), gorillas3);
    }

    // Pipeline-processor list for the ASAP-tier path. Order matches
    // `asap-otel-agent-b6-asap-single-sketch.yaml`: gorillas3 runs FIRST
    // so the cold-tier write happens on the raw sample BEFORE the sketch
    // processor mutates / suffix-renames the metric stream.
    let asap_tier_processors: Vec<String> = {
        let mut v = Vec::new();
        if has_archive_tier {
            v.push("gorillas3".to_string());
        }
        v.extend(sketch_pipeline_processors.iter().cloned());
        v
    };
    // Pipeline-processor list for the warm-passthrough path (Bug b):
    // gorillas3 still runs (the metric still wants to land in the
    // archive) but the sketch processor is bypassed so the metric name
    // is preserved end-to-end. Empty when no archive tier and no
    // sketches — passthrough = receiver → exporter.
    let warm_passthrough_processors: Vec<String> = {
        let mut v = Vec::new();
        if has_archive_tier {
            v.push("gorillas3".to_string());
        }
        v
    };

    // ── Exporters ─────────────────────────────────────────────────────────────
    // Edge exports directly to asapquery-backend's OTLP ingest. The
    // backend's precompute engine merges per-aggregation_id accumulators
    // server-side, so no middle-tier gateway merge processor is needed.
    // (The gateway typed L5 stage + emit_gateway_yaml machinery stays in
    // source for topologies that re-introduce a middle tier, but is not
    // exercised in the default deployment.)
    let (exporter_key, exporter_val) = build_otlp_exporter("backend", &cfg.exporter_target);

    let mut exporters: HashMap<String, Value> = [(exporter_key.clone(), exporter_val)].into();
    let mut pipelines: HashMap<String, Pipeline> = HashMap::new();

    let has_prometheus_archive = !cfg.prometheus_archive_metrics.is_empty();
    let has_warm_passthrough = !cfg.warm_passthrough_metrics.is_empty();

    // ── Phase ε.1 Mode 3 / Phase 3.2.5 Bug (b) — per-pipeline routing ─────
    // Two routing axes can fire from a single edge agent:
    //
    //   * Phase ε.1: Mode-3 metrics carry `asap.mode = prometheus_archive`
    //     as a data-point attribute and dispatch to the Prometheus OTLP
    //     receiver via a separate `otlphttp/prometheus` exporter.
    //   * Phase 3.2.5 Bug (b): warm-passthrough metrics (the freshness
    //     probes) need to bypass the family-specific sketch processor so
    //     the metric name is preserved end-to-end. They dispatch by
    //     metric name, NOT by `asap.mode` (so we don't have to teach the
    //     fake-exporter to set an extra attribute on top of the name).
    //
    // When ONLY the Phase ε.1 Mode-3 axis is active we emit the legacy
    // `from_attribute: asap.mode` form to keep the wire shape stable.
    // When the warm-passthrough axis is active (alone or together with
    // Mode 3) we emit the OTTL-statement form (`route() where ...`)
    // which lets a single routing processor dispatch by both axes from
    // a single table.
    if has_prometheus_archive {
        // Exporter: OTLP HTTP to Prometheus's native receiver. The path
        // is the canonical `/api/v1/otlp/v1/metrics`. The OTel collector's
        // `otlphttp` exporter uses a `metrics_endpoint` field for the
        // full URL (the `endpoint` field auto-appends `/v1/metrics` per
        // OTel SDK convention; we use `metrics_endpoint` to be explicit
        // and match the Prom path verbatim).
        let prom_exporter_yaml = "metrics_endpoint: \"${ASAP_PROMETHEUS_OTLP_URL:-http://prometheus:9090/api/v1/otlp/v1/metrics}\"\nencoding: proto\ntls:\n  insecure: true\n";
        let prom_exporter: Value = serde_yaml::from_str(prom_exporter_yaml)
            .context("parse otlphttp/prometheus exporter block")?;
        exporters.insert("otlphttp/prometheus".to_string(), prom_exporter);
    }

    if has_warm_passthrough {
        // ── Phase 3.2.5 Bug (b): warm-passthrough routing ───────────────────
        // The freshness probes are timestamp counters by design — the
        // wire value `unix_ts_ms_of_emission` IS the freshness signal,
        // so they MUST flow through the ASAP tier with their original
        // metric name preserved. The DDSketch processor's `_quantile`
        // suffix would rename `http_freshness_probe_warm` to
        // `http_freshness_probe_warm_quantile` and break the replay
        // client's `last_over_time(http_freshness_probe_warm[10s])`
        // query.
        //
        // The fix: a `routing` processor with OTTL `route()` statements
        // dispatches by metric name. Listed metrics route to
        // `metrics/warm_passthrough` (gorillas3 → exporter, NO sketch);
        // everything else takes the regular `metrics/asap_tier` path
        // (gorillas3 → sketches → exporter). Phase ε.1's Mode-3 entry
        // (matching `attributes["asap.mode"]`) is folded into the same
        // table when prometheus_archive is also configured.
        let mut table_entries: Vec<String> = Vec::new();
        for metric in &cfg.warm_passthrough_metrics {
            table_entries.push(format!(
                "  - statement: 'route() where metric.name == \"{metric}\"'\n    pipelines: [metrics/warm_passthrough]"
            ));
        }
        if has_prometheus_archive {
            table_entries.push(
                "  - statement: 'route() where attributes[\"asap.mode\"] == \"prometheus_archive\"'\n    pipelines: [metrics/prometheus_archive]".to_string(),
            );
        }
        let routing_yaml = format!(
            "default_pipelines: [metrics/asap_tier]\ntable:\n{}\n",
            table_entries.join("\n"),
        );
        let routing: Value = serde_yaml::from_str(&routing_yaml)
            .context("parse routing processor block (OTTL form)")?;
        processors.insert("routing".to_string(), routing);

        pipelines.insert(
            "metrics/asap_tier".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: asap_tier_processors.clone(),
                exporters: vec![exporter_key.clone()],
            },
        );
        pipelines.insert(
            "metrics/warm_passthrough".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: warm_passthrough_processors.clone(),
                exporters: vec![exporter_key.clone()],
            },
        );
        if has_prometheus_archive {
            pipelines.insert(
                "metrics/prometheus_archive".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: Vec::new(),
                    exporters: vec!["otlphttp/prometheus".to_string()],
                },
            );
        }
        let mut entry_exporters = vec![exporter_key.clone()];
        if has_prometheus_archive {
            entry_exporters.push("otlphttp/prometheus".to_string());
        }
        pipelines.insert(
            "metrics".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: vec!["routing".to_string()],
                exporters: entry_exporters,
            },
        );
    } else if has_prometheus_archive {
        // Legacy Phase ε.1 routing — `from_attribute: asap.mode`.
        // Preserved as-is so the wire shape stays stable for the
        // (warm_passthrough_metrics empty) cases that already exist.
        let routing_yaml = "from_attribute: asap.mode\ndefault_pipelines: [metrics/asap_tier]\ntable:\n  - value: prometheus_archive\n    pipelines: [metrics/prometheus_archive]\n";
        let routing: Value =
            serde_yaml::from_str(routing_yaml).context("parse routing processor block")?;
        processors.insert("routing".to_string(), routing);

        // Two named pipelines:
        //   `metrics/asap_tier`        — gorillas3 (if archive) +
        //                                 sketch processors → otlp/backend
        //   `metrics/prometheus_archive` — passthrough → otlphttp/prometheus
        pipelines.insert(
            "metrics/asap_tier".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: asap_tier_processors.clone(),
                exporters: vec![exporter_key.clone()],
            },
        );
        pipelines.insert(
            "metrics/prometheus_archive".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: Vec::new(),
                exporters: vec!["otlphttp/prometheus".to_string()],
            },
        );
        // Main `metrics` pipeline keeps the receiver + routing only —
        // this is what the OTel routing connector pattern expects (one
        // entry pipeline that fans out via the routing processor's
        // table).
        pipelines.insert(
            "metrics".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: vec!["routing".to_string()],
                exporters: vec![exporter_key.clone(), "otlphttp/prometheus".to_string()],
            },
        );
    } else {
        // No routing — single pipeline with the ASAP-tier processor
        // chain (gorillas3 if archive_tier_metrics non-empty, then
        // sketches).
        pipelines.insert(
            "metrics".to_string(),
            Pipeline {
                receivers: vec!["otlp".into()],
                processors: asap_tier_processors,
                exporters: vec![exporter_key],
            },
        );
    }

    // ── OpAMP extension ───────────────────────────────────────────────────────
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .context("parse opamp extension block")?;

    // ── Top-level YAML ────────────────────────────────────────────────────────
    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        // Legacy emit paths don't use the routing connector — see the
        // MVP §46 dispatch at the top of `emit_edge_yaml`.
        connectors: HashMap::new(),
        exporters,
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines,
        },
    };

    serde_yaml::to_string(&doc).context("serialize edge stage config")
}

/// Build the OTel-collector YAML for a gateway aggregator from the
/// typed L5 [`GatewayStageConfig`] payload.
///
/// The gateway runs one `<sketch_kind>merge` processor per
/// `GatewayMergeProcessor` entry — these are the patched merge
/// processors in `opentelemetry-collector-contrib-patch/processor/`.
pub fn emit_gateway_yaml(cfg: &GatewayStageConfig, opamp_endpoint: &str) -> Result<String> {
    // Receiver — port from cfg, both gRPC + HTTP.
    let port = cfg.otlp_receiver_port;
    let otlp_receiver: Value = serde_yaml::from_str(&format!(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:{port}\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:{}\"\n",
        port + 1,
    ))
    .context("parse gateway OTLP receiver block")?;

    // Processors — one merge processor per merge entry. Naming
    // convention matches the patched contrib build:
    //   * SketchKind::DDSketch    → `ddsketchmerge`
    //   * SketchKind::Kll         → `kllmerge`
    //   * SketchKind::Hll         → `hllmerge`
    //   * SketchKind::Cms         → `countminsketchmerge`
    //   * SketchKind::CountSketch → `countsketchmerge`
    //
    // We honour `GatewayMergeProcessor::processor_name` if non-empty
    // (the typed emitter today populates it as `"sketchmergeprocessor"`
    // — a placeholder until Phase C flips factory names per-family),
    // otherwise we derive the family-specific name from `sketch_kind`.
    let mut processors: HashMap<String, Value> = HashMap::new();
    let mut pipeline_processors: Vec<String> = Vec::new();
    for mp in &cfg.merge_processors {
        let key = gateway_merge_processor_name(mp);
        let block = build_gateway_merge_block(mp);
        processors.insert(key.clone(), block);
        pipeline_processors.push(key);
    }

    // Exporter — backend OTLP.
    let (exporter_key, exporter_val) = build_otlp_exporter("backend", &cfg.exporter_target);

    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .context("parse opamp extension block")?;

    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        // Gateway stage doesn't use the routing connector.
        connectors: HashMap::new(),
        exporters: [(exporter_key.clone(), exporter_val)].into(),
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: pipeline_processors,
                    exporters: vec![exporter_key],
                },
            )]
            .into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize gateway stage config")
}

/// Build the JSON document the ASAPQuery-backend's
/// `POST /api/v1/streaming-config` endpoint accepts, sourced from the
/// typed L5 [`BackendStageConfig`].
///
/// Output shape mirrors the YAML shape produced by
/// [`crate::config::asapquery_backend::generate_streaming_config_yaml`]:
/// a top-level `aggregations` array of
/// `{ aggregationType, aggregationSubType, metric, labels, parameters,
/// windowSize, windowType, spatialFilter, aggregationInput }` rows.
/// `aggregationId` is **not** emitted — identity is content-addressed in
/// the backend via `PolicyFingerprint(u64)`.
/// We additionally surface a parallel `readouts` array so the backend's
/// query engine can prepare per-readout dispatch entries up-front (the
/// existing YAML form has no readouts list because the legacy planner
/// materialises one aggregation per metric and infers readouts from the
/// PromQL query at execution time; Phase B's typed `BackendStageConfig`
/// carries the readouts explicitly, so we ship them too — backends that
/// don't recognise the field will ignore it without erroring).
pub fn emit_backend_streaming_config_json(cfg: &BackendStageConfig) -> Result<JsonValue> {
    let aggregations: Vec<JsonValue> = cfg
        .aggregations
        .iter()
        .map(build_backend_aggregation_json)
        .collect();

    let readouts: Vec<JsonValue> = cfg
        .readouts
        .iter()
        .map(build_backend_readout_json)
        .collect();

    Ok(json!({
        "aggregations": aggregations,
        "readouts": readouts,
    }))
}

/// Phase α (MVP): build the JSON document the ASAPQuery-backend's
/// `POST /api/v1/storage_routing` endpoint accepts, sourced from the
/// typed L5 [`BackendStageConfig`] payloads emitted by [`crate::planner::stage_split`].
///
/// `metric_plans` is the list of `(metric_name, &BackendStageConfig)`
/// pairs the controller has produced this planning cycle — one entry
/// per workload that ran through the typed L5 path. Each entry yields
/// one `metrics:` row in the emitted JSON. `default_engine` is the
/// fallback for any metric the backend's HTTP handler observes that the
/// controller did not plan for.
///
/// ## Schema
///
/// Output mirrors the existing `deploy/configs/backend-storage-routing.yaml`
/// schema (the `routes:` form), serialised as JSON:
///
/// ```json
/// {
///   "default_engine": "asap_query",
///   "metrics": [
///     { "name": "http_requests_total",
///       "targets": [
///         { "engine": "thanos_query",
///           "applies_to_query_shape": ["count", "topk", "rate_post_hoc",
///                                      "histogram_quantile", "delta", "absent"] },
///         { "engine": "asap_query" }
///       ]
///     }
///   ]
/// }
/// ```
///
/// ## Classification rules (Phase α)
///
/// For each `(metric, BackendStageConfig)` we derive a target list by
/// inspecting the L4 sketch families landed at the backend:
///
/// * **DDSketch / KLL** present → ASAP-tier serves `quantile` shape;
///   ASAP-tier is the default for everything the archive doesn't claim.
/// * **HLL** present → ASAP-tier serves `count` shape (cardinality
///   readout). NOTE: with HLL planned, `count` does NOT route to archive
///   — the ASAP-tier sketch is lossier-but-cheaper than archive scan and
///   the controller already chose to spend the bandwidth on it.
/// * **Count-Sketch** present → ASAP-tier serves `topk` shape (the
///   sketch's whole purpose).
/// * **CountMinSketch** present → ASAP-tier serves `point_count` /
///   `count` shape (the CMS's `Estimate` readout).
///
/// The `thanos_query` target is always added with the **archive-eligible
/// shape list** — those PromQL shapes that no ASAP-tier sketch can
/// answer at all (`histogram_quantile`, `delta`, `deriv`, `absent`,
/// post-hoc / un-planned ranges). When a sketch-eligible shape is also
/// in the archive's claim list (e.g. `count` when no HLL was planned)
/// it is added so the archive picks it up as a fallback.
///
/// Phase α is conservative: we always emit BOTH a ASAP-tier default
/// slot AND a thanos archive slot for every planned metric, so v7
/// dual-routing semantics are preserved by construction. Future phases
/// (β / γ) may prune the archive slot for metrics the cost model
/// prices out of cold storage.
pub fn emit_backend_storage_routing(
    metric_plans: &[(String, &BackendStageConfig)],
) -> Result<JsonValue> {
    emit_backend_storage_routing_for_tenant(DEFAULT_TENANT, metric_plans)
}

/// Tenant id used when the deploy is single-tenant. Mirrors the
/// backend's `crate::query_engines::routing::DEFAULT_TENANT` (defined in
/// `ASAPQuery-backend/asap-query-engine/src/routing/backend_storage_routing.rs`)
/// — kept as a literal here so the controller doesn't take a build-time
/// dependency on the backend crate just for one constant.
pub const DEFAULT_TENANT: &str = "default";

/// Per-tenant follow-up to PR #333 — emit a `BackendStorageRouting`
/// JSON document scoped to a specific tenant. The single-tenant
/// [`emit_backend_storage_routing`] entry point delegates to this
/// with [`DEFAULT_TENANT`], preserving the existing single-tenant
/// emit contract.
///
/// The emitted JSON adds a top-level `tenant: "<id>"` field. The
/// backend's `BackendStorageRouting::from_json_payload` parser
/// reads this field (defaulting to `"default"` when absent) and
/// the `POST /api/v1/storage_routing` swap handler routes the swap
/// to the named tenant's slot. Multi-tenant deployments emit one
/// JSON per tenant; single-tenant deployments keep emitting with
/// the default tenant and need no controller-side change.
pub fn emit_backend_storage_routing_for_tenant(
    tenant: &str,
    metric_plans: &[(String, &BackendStageConfig)],
) -> Result<JsonValue> {
    let mut metrics_json: Vec<JsonValue> = Vec::with_capacity(metric_plans.len());
    for (metric_name, backend_cfg) in metric_plans {
        metrics_json.push(build_routing_entry(metric_name, backend_cfg));
    }
    Ok(json!({
        "tenant": tenant,
        "default_engine": "asap_query",
        "metrics": metrics_json,
    }))
}

/// Phase ε.1 — same as [`emit_backend_storage_routing`] but also
/// emits `thanos_query` engine entries for Mode 3 metrics.
///
/// Mode-3 metrics have NO `BackendStageConfig` entry (the backend doesn't
/// own the storage; Prometheus does). They surface here as plain metric
/// names paired with a single `thanos_query` target. The backend's
/// HTTP query handler consults the routing table at request time and
/// HTTP-forwards Mode-3 queries to
/// `${ASAP_PROMETHEUS_QUERY_URL:-http://prometheus:9090}/api/v1/query`.
///
/// Phase ε.2 implements the `thanos_query` engine on the backend
/// (the HTTP forwarder); Phase ε.1 only commits the routing wire shape.
///
/// `mode3_metrics` is the list of metric names the planner routed to
/// Prometheus archive this cycle. Each yields a single-target row with
/// `engine: thanos_query` and no shape filter (Prom answers
/// everything for these metrics, exact ε = 0).
pub fn emit_backend_storage_routing_with_prometheus(
    metric_plans: &[(String, &BackendStageConfig)],
    mode3_metrics: &[String],
) -> Result<JsonValue> {
    emit_backend_storage_routing_with_prometheus_for_tenant(
        DEFAULT_TENANT,
        metric_plans,
        mode3_metrics,
    )
}

/// Per-tenant variant of [`emit_backend_storage_routing_with_prometheus`].
/// Mirrors [`emit_backend_storage_routing_for_tenant`] — adds a
/// top-level `tenant: "<id>"` field; defaults preserve the existing
/// single-tenant emit shape.
pub fn emit_backend_storage_routing_with_prometheus_for_tenant(
    tenant: &str,
    metric_plans: &[(String, &BackendStageConfig)],
    mode3_metrics: &[String],
) -> Result<JsonValue> {
    let mut metrics_json: Vec<JsonValue> =
        Vec::with_capacity(metric_plans.len() + mode3_metrics.len());
    for (metric_name, backend_cfg) in metric_plans {
        metrics_json.push(build_routing_entry(metric_name, backend_cfg));
    }
    for metric_name in mode3_metrics {
        // Mode 3 — Prometheus owns the storage. Single target,
        // engine=thanos_query, no shape filter (all PromQL shapes
        // route through the backend's HTTP forwarder).
        metrics_json.push(json!({
            "name": metric_name,
            "targets": [
                { "engine": "thanos_query" }
            ],
            "asap_mode": "prometheus_archive",
        }));
    }
    Ok(json!({
        "tenant": tenant,
        "default_engine": "asap_query",
        "metrics": metrics_json,
    }))
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Build the JSON `metrics:` entry for one (metric, BackendStageConfig)
/// pair — picks per-shape targets from the L4 sketch families the plan
/// landed at the backend.
///
/// Returns a JSON object of shape:
/// ```text
/// { "name": <metric>, "targets": [<target>, ...] }
/// ```
/// where each `<target>` is either `{ "engine": <engine>, "applies_to_query_shape": [...] }`
/// or `{ "engine": <engine> }` for the default slot.
fn build_routing_entry(metric_name: &str, cfg: &BackendStageConfig) -> JsonValue {
    let kinds: Vec<SketchKind> = cfg
        .aggregations
        .iter()
        .map(|a| a.sketch_kind.clone())
        .collect();

    // Sketch-eligible shapes — the ASAP tier serves these natively
    // because we planned a sketch for them.
    let mut warm_shapes: Vec<&'static str> = Vec::new();
    let has_quantile_sketch = kinds
        .iter()
        .any(|k| matches!(k, SketchKind::DDSketch | SketchKind::Kll));
    if has_quantile_sketch {
        warm_shapes.push("quantile");
        warm_shapes.push("quantile_over_time");
    }
    let has_hll = kinds.iter().any(|k| matches!(k, SketchKind::Hll));
    if has_hll {
        warm_shapes.push("count");
    }
    let has_count_sketch = kinds.iter().any(|k| matches!(k, SketchKind::CountSketch));
    if has_count_sketch {
        warm_shapes.push("topk");
    }
    let has_cms = kinds.iter().any(|k| matches!(k, SketchKind::Cms));
    if has_cms {
        // CMS's `Estimate` readout serves point-count / count queries.
        // If HLL also planned, `count` is already in the list — push
        // only when not already there (keep order stable).
        if !warm_shapes.contains(&"count") {
            warm_shapes.push("count");
        }
    }
    // Sketch-planned `rate / sum / avg / min / max` over the planned
    // ranges — every sketch family the planner emits also tracks the
    // range aggregation needed to answer these from the ASAP tier
    // (the gateway merge processor produces a windowed accumulator).
    if !kinds.is_empty() {
        warm_shapes.push("rate");
        warm_shapes.push("sum");
        warm_shapes.push("avg");
        warm_shapes.push("min");
        warm_shapes.push("max");
    }

    // Archive-eligible shapes — Thanos / cold archive answers these
    // because no ASAP-tier sketch can.
    //
    // Classification rule (surprised-me bullet for the report): `topk`
    // and `count` route to archive only when NO matching sketch was
    // planned. With Count-Sketch the ASAP tier answers `topk` via the
    // CountSketch's heap-augmented Estimate; with HLL the ASAP tier
    // answers `count` via the cardinality estimate. Pruning the
    // archive's claim list is what makes Phase α a planner-driven
    // routing table rather than a static "everything goes to archive"
    // failover.
    let mut archive_shapes: Vec<&'static str> = Vec::new();
    archive_shapes.push("histogram_quantile");
    archive_shapes.push("delta");
    archive_shapes.push("deriv");
    archive_shapes.push("absent");
    archive_shapes.push("rate_post_hoc");
    if !has_count_sketch {
        archive_shapes.push("topk");
    }
    if !has_hll && !has_cms {
        archive_shapes.push("count");
    }

    // Emit the ASAP-tier default slot first (no filter — catches every
    // shape the archive doesn't claim), then the archive slot with the
    // explicit-shape claim list. Ordering matches the existing
    // `deploy/configs/backend-storage-routing.yaml` convention. The
    // backend's `lookup_with_shape` is two-pass: explicit-shape match
    // wins (so `count` / `topk` / etc. land on archive when listed
    // there), default slot otherwise (so `quantile` / `sum` / etc.
    // land on warm).
    //
    // We do NOT attach `applies_to_query_shape` to the warm slot —
    // attaching it would turn warm into a shape-specific target and
    // any unanticipated shape (e.g. `LastOverTime` on a metric where
    // the operator added a probe after planning) would fall through
    // to the archive's first-target fallback, which is the wrong
    // failure mode. Warm = default; archive = the specific shapes
    // archive serves better.
    let mut targets: Vec<JsonValue> = Vec::new();
    targets.push(json!({
        "engine": "asap_query",
    }));
    if !archive_shapes.is_empty() {
        targets.push(json!({
            "engine": "thanos_query",
            "applies_to_query_shape": archive_shapes,
        }));
    }

    // The warm-shape list is informational — surface it on a side
    // field for operators / tests to spot-check what the controller
    // decided the ASAP tier serves natively. The backend ignores
    // unknown fields (`#[serde(default)]` on the parser side).
    let mut entry = json!({
        "name": metric_name,
        "targets": targets,
    });
    if !warm_shapes.is_empty() {
        entry["asap_tier_native_shapes"] = json!(warm_shapes);
    }
    entry
}

// ── MVP §46: 5-sketch routing-connector edge YAML emitter ─────────────────
//
// CRITICAL CORRECTNESS NOTE (call out as a real bugfix, not a refactor):
// the legacy `emit_edge_yaml` placed `routing` under `processors:`. That
// is WRONG for OTel collector v0.106+ — the routing component was
// deprecated as a processor and re-shipped as a *connector*. The
// `routingprocessor` factory was removed in collector-contrib v0.106
// and the asap-otel binary's `builder-config.yaml` registers
// `routingconnector` instead. Emitting the old shape produces a YAML
// that fails `confmap.Provider` validation on the agent at boot:
//   `error decoding 'processors': unknown type: "routing"`.
//
// This function emits the canonical connector-form layout — see the
// MVP §46 contract:
//
//   receivers:  { otlp }
//   processors: { gorillas3?, batch, ddsketch, KLL, HLL,
//                 countsketch, countmin }
//   connectors: { routing: { default_pipelines: [metrics/raw_passthrough],
//                            table: [ ... per-metric OTTL conditions ... ] } }
//   exporters:  { otlp/backend, otlphttp/prometheus? }
//
//   service.pipelines:
//     metrics:                          (entry — receivers: [otlp],
//                                        exporters: [routing])
//     metrics/raw_passthrough:          (default — receivers: [routing],
//                                        processors: [gorillas3?, batch],
//                                        exporters: [otlp/backend])
//     metrics/{ddsketch,kll,hll,countsketch,countminsketch}_path:
//                                       (per-family — receivers: [routing],
//                                        processors: [gorillas3?,
//                                                     <family>processor,
//                                                     batch],
//                                        exporters: [otlp/backend])
//
// `gorillas3` runs FIRST in every per-sketch pipeline (when an
// archive tier is declared) so the raw sample lands in the cold
// archive BEFORE the family-specific sketch processor mutates the
// stream — same invariant the legacy emit path enforces.
//
// Phase ε.1 Mode-3 metrics (`prometheus_archive_metrics`) and Bug (b)
// `warm_passthrough_metrics` (the freshness probes) are folded into
// the routing table's `table:` and route to the `metrics/raw_passthrough`
// pipeline — they intentionally bypass every sketch processor.
fn emit_edge_yaml_5sketch_routing(cfg: &EdgeStageConfig, opamp_endpoint: &str) -> Result<String> {
    use crate::sketch_algebra::params::SketchKind;

    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    )
    .context("parse static OTLP receiver block")?;

    // ── Processors ─────────────────────────────────────────────────────────
    //
    // We always load all 5 sketch processors regardless of which metrics
    // route to them — the planner agent's contract is that the agent
    // can be retargeted at runtime via OpAMP without re-building, so a
    // future plan that maps a new metric to (say) HLL must work without
    // a config push that touches `processors:`.
    let mut processors: HashMap<String, Value> = HashMap::new();

    // Build per-family processor blocks. We pull from
    // `cfg.sketch_processors` when an entry exists for that family
    // (so the params flow through), otherwise we
    // synthesise a default-param block so the YAML always carries
    // all 5 processor keys.
    let mut family_to_proc: HashMap<SketchKind, &EdgeSketchProcessor> = HashMap::new();
    for sp in &cfg.sketch_processors {
        family_to_proc.insert(sp.sketch_kind.clone(), sp);
    }

    for kind in [
        SketchKind::DDSketch,
        SketchKind::Kll,
        SketchKind::Hll,
        SketchKind::CountSketch,
        SketchKind::Cms,
    ] {
        let processor_name = sketch_kind_to_processor_name(&kind);
        let metric_name_hint = cfg
            .metric_to_family
            .iter()
            .filter_map(|(metric, mapped)| {
                if mapped == &kind {
                    Some(metric.as_str())
                } else {
                    None
                }
            })
            .min();
        let block = if let Some(sp) = family_to_proc.get(&kind) {
            build_edge_processor_block(sp, cfg.window_secs, &cfg.label_filters, metric_name_hint)
        } else {
            build_default_edge_processor_block(&kind, cfg.window_secs, metric_name_hint)
        };
        processors.insert(processor_name.to_string(), block);
    }

    // ── gorillas3 archive processor ────────────────────────────────────────
    let has_archive_tier = !cfg.archive_tier_metrics.is_empty();
    if has_archive_tier {
        let window_secs: u64 = cfg
            .archive_tier_metrics
            .iter()
            .filter_map(|m| m.window_secs)
            .min()
            .unwrap_or(60);
        let gorillas3_yaml = format!(
            "window_interval: {window_secs}s\n\
drop_original: false\n\
endpoint: \"${{ASAP_MINIO_ENDPOINT:-http://minio:9000}}\"\n\
bucket: \"${{ASAP_GORILLA_BUCKET:-asap-gorilla}}\"\n\
region: us-east-1\n\
use_ssl: false\n\
access_key_id: \"${{ASAP_MINIO_ACCESS_KEY:-asap}}\"\n\
secret_access_key: \"${{ASAP_MINIO_SECRET_KEY:-asap-local-only}}\"\n\
prefix_template: \"{{tenant}}/{{metric}}/{{YYYY}}/{{MM}}/{{DD}}/{{HH}}/\"\n\
tenant: \"${{ASAP_TENANT:-default}}\"\n\
max_retries: 3\n\
retry_backoff: 1s\n\
upload_timeout: 30s\n\
block_format: prometheus_tsdb\n\
tsdb_bucket: \"${{ASAP_GORILLA_TSDB_BUCKET:-asap-gorilla-tsdb}}\"\n\
tsdb_block_duration: {window_secs}s\n",
        );
        let gorillas3: Value = serde_yaml::from_str(&gorillas3_yaml)
            .context("parse gorillas3 processor block (5-sketch routing)")?;
        processors.insert("gorillas3".to_string(), gorillas3);
    }

    // batch processor — every per-family pipeline ends in batch so the
    // gateway sees properly framed OTLP. Defaults match
    // `deploy/configs/asap-otel-agent-b6-asap-single-sketch.yaml`.
    let batch_block: Value = serde_yaml::from_str("send_batch_size: 1024\ntimeout: 1s\n")
        .context("parse batch processor block")?;
    processors.insert("batch".to_string(), batch_block);

    // memory_limiter processor — backpressure BEFORE gorillas3 so the
    // collector refuses incoming batches when RSS crosses the soft
    // threshold instead of OOM-killing the agent. Follow-up to PR #355
    // (gorillas3 archive write fix): even with `window_interval: 5s`
    // the agent was OOM-killed (exit 137) ~3 min into sustained load
    // because six per-family in-memory windowState buffers can overshoot
    // the 1.5 GiB cgroup ceiling at peak. Threshold = 1280 MiB / 256 MiB
    // spike (≈ 80 % / 17 % of cgroup), mirrors gateway shape but scaled
    // to the agent's smaller cgroup. MUST be the first processor in
    // every per-sketch pipeline (see `make_sketch_pipeline` below) —
    // limiting AFTER gorillas3 would mean the buffer has already
    // accreted on heap by the time the limiter rejects.
    let memory_limiter_block: Value =
        serde_yaml::from_str("check_interval: 1s\nlimit_mib: 1280\nspike_limit_mib: 256\n")
            .context("parse memory_limiter processor block")?;
    processors.insert("memory_limiter".to_string(), memory_limiter_block);

    // ── Exporters ──────────────────────────────────────────────────────────
    // Edge → asapquery-backend OTLP ingest (see emit_edge_yaml for the
    // gateway-less rationale).
    let (exporter_key, exporter_val) = build_otlp_exporter("backend", &cfg.exporter_target);
    let mut exporters: HashMap<String, Value> = [(exporter_key.clone(), exporter_val)].into();

    let has_prometheus_archive = !cfg.prometheus_archive_metrics.is_empty();
    if has_prometheus_archive {
        let prom_exporter_yaml = "metrics_endpoint: \"${ASAP_PROMETHEUS_OTLP_URL:-http://prometheus:9090/api/v1/otlp/v1/metrics}\"\nencoding: proto\ntls:\n  insecure: true\n";
        let prom_exporter: Value = serde_yaml::from_str(prom_exporter_yaml)
            .context("parse otlphttp/prometheus exporter block")?;
        exporters.insert("otlphttp/prometheus".to_string(), prom_exporter);
    }

    // ── Routing connector ──────────────────────────────────────────────────
    //
    // Build the OTTL route table. Iterate the planner's
    // `metric_to_family` map in deterministic order (sorted by metric
    // name) so the YAML is stable across runs — `HashMap` iteration is
    // not order-stable.
    let mut metric_family_pairs: Vec<(&String, &SketchKind)> =
        cfg.metric_to_family.iter().collect();
    metric_family_pairs.sort_by(|a, b| a.0.cmp(b.0));

    let mut table_entries: Vec<String> = Vec::new();
    let mut referenced_pipelines: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();

    for (metric, kind) in &metric_family_pairs {
        let pipeline = sketch_kind_to_pipeline_name(kind);
        table_entries.push(format!(
            "  - context: metric\n    condition: 'name == \"{metric}\"'\n    pipelines: [{pipeline}]"
        ));
        referenced_pipelines.insert(pipeline.to_string());
    }

    // Phase 3.2.5 Bug (b) — warm-passthrough freshness probes route to
    // raw_passthrough (no sketch processor mutates the metric name).
    for metric in &cfg.warm_passthrough_metrics {
        table_entries.push(format!(
            "  - context: metric\n    condition: 'name == \"{metric}\"'\n    pipelines: [metrics/raw_passthrough]"
        ));
    }

    // Phase ε.1 — Mode 3 prometheus-archive routing folds in via the
    // `asap.mode` attribute axis. The dedicated
    // `metrics/prometheus_archive` pipeline ships the metric to
    // Prometheus's native OTLP receiver via `otlphttp/prometheus`.
    if has_prometheus_archive {
        table_entries.push(
            "  - context: datapoint\n    condition: 'attributes[\"asap.mode\"] == \"prometheus_archive\"'\n    pipelines: [metrics/prometheus_archive]"
                .to_string(),
        );
    }

    let routing_yaml = format!(
        "default_pipelines: [metrics/raw_passthrough]\ntable:\n{}\n",
        table_entries.join("\n"),
    );
    let routing_block: Value =
        serde_yaml::from_str(&routing_yaml).context("parse routing connector block (5-sketch)")?;
    let mut connectors: HashMap<String, Value> = HashMap::new();
    connectors.insert("routing".to_string(), routing_block);

    // ── Pipeline assembly ──────────────────────────────────────────────────
    //
    // Helper: per-family pipeline =
    //   `[memory_limiter, gorillas3?, <family>processor, batch]`.
    // memory_limiter runs FIRST so backpressure rejects incoming batches
    // BEFORE gorillas3 buffers them into windowState. gorillas3 then
    // does the cold-tier write on raw samples BEFORE the sketch
    // processor mutates / suffix-renames the stream.
    let make_sketch_pipeline = |family_proc: &str| -> Pipeline {
        let mut procs: Vec<String> = Vec::new();
        procs.push("memory_limiter".to_string());
        if has_archive_tier {
            procs.push("gorillas3".to_string());
        }
        procs.push(family_proc.to_string());
        procs.push("batch".to_string());
        Pipeline {
            receivers: vec!["routing".into()],
            processors: procs,
            exporters: vec![exporter_key.clone()],
        }
    };

    let mut pipelines: HashMap<String, Pipeline> = HashMap::new();

    // Entry pipeline — receivers: [otlp], exporters: [routing]
    // (`routing` here is the connector, used as exporter for the entry
    // stage). NO processors on the entry pipeline; the connector is
    // responsible for fan-out.
    pipelines.insert(
        "metrics".to_string(),
        Pipeline {
            receivers: vec!["otlp".into()],
            processors: Vec::new(),
            exporters: vec!["routing".to_string()],
        },
    );

    // Default raw_passthrough —
    // `[memory_limiter, gorillas3?, batch]`. NO sketch processor — the
    // raw counters land at the gateway verbatim. This is also the
    // destination of warm_passthrough metrics (freshness probes).
    // memory_limiter runs first so backpressure applies to the default
    // route too.
    let raw_passthrough = {
        let mut procs: Vec<String> = Vec::new();
        procs.push("memory_limiter".to_string());
        if has_archive_tier {
            procs.push("gorillas3".to_string());
        }
        procs.push("batch".to_string());
        Pipeline {
            receivers: vec!["routing".into()],
            processors: procs,
            exporters: vec![exporter_key.clone()],
        }
    };
    pipelines.insert("metrics/raw_passthrough".to_string(), raw_passthrough);

    // Always emit all 5 per-family pipelines so the agent's pipeline
    // graph is closed regardless of which families the table currently
    // references — keeps the runtime swap (planner re-emits with a
    // different `metric_to_family`) zero-touch on the pipeline graph.
    for kind in [
        SketchKind::DDSketch,
        SketchKind::Kll,
        SketchKind::Hll,
        SketchKind::CountSketch,
        SketchKind::Cms,
    ] {
        let proc_name = sketch_kind_to_processor_name(&kind);
        let pipeline_name = sketch_kind_to_pipeline_name(&kind);
        pipelines.insert(pipeline_name.to_string(), make_sketch_pipeline(proc_name));
    }

    // Phase ε.1 — Mode 3 prometheus-archive pipeline (raw passthrough
    // to the Prometheus OTLP exporter). No sketch processors; only the
    // Prometheus exporter target is referenced.
    if has_prometheus_archive {
        pipelines.insert(
            "metrics/prometheus_archive".to_string(),
            Pipeline {
                receivers: vec!["routing".into()],
                processors: Vec::new(),
                exporters: vec!["otlphttp/prometheus".to_string()],
            },
        );
    }

    // ── OpAMP extension ────────────────────────────────────────────────────
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .context("parse opamp extension block")?;

    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        connectors,
        exporters,
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines,
        },
    };

    serde_yaml::to_string(&doc).context("serialize edge stage config (5-sketch)")
}

/// Map a `SketchKind` to the OTel processor name registered by the
/// patched contrib build's factory. Keep in sync with
/// `crate::physical::colored_dag::emitter::edge_processor_name`.
fn sketch_kind_to_processor_name(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::DDSketch => "ddsketch",
        SketchKind::Kll => "KLL",
        SketchKind::Hll => "HLL",
        SketchKind::CountSketch => "countsketch",
        SketchKind::Cms => "countmin",
    }
}

/// Map a `SketchKind` to its per-family pipeline name in the routing
/// connector layout.
fn sketch_kind_to_pipeline_name(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::DDSketch => "metrics/ddsketch_path",
        SketchKind::Kll => "metrics/kll_path",
        SketchKind::Hll => "metrics/hll_path",
        SketchKind::CountSketch => "metrics/countsketch_path",
        SketchKind::Cms => "metrics/countminsketch_path",
    }
}

/// Build a default-parameter processor block for a `SketchKind` when
/// the planner's `metric_to_family` references a family that
/// `cfg.sketch_processors` didn't enumerate. Defaults match the catalog
/// values used by the planner's L4 rules so the wire shape is what the
/// rest of the system expects when a metric is later re-routed onto
/// this family.
fn build_default_edge_processor_block(
    kind: &SketchKind,
    window_secs: Option<u64>,
    metric_name_hint: Option<&str>,
) -> Value {
    use crate::sketch_algebra::params::{
        CmsParams, CountSketchParams, DDSketchParams, HllParams, KllParams,
    };
    let params = match kind {
        SketchKind::DDSketch => SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
        SketchKind::Kll => SketchParams::Kll(KllParams { k: 200 }),
        SketchKind::Hll => SketchParams::Hll(HllParams { precision: 14 }),
        SketchKind::CountSketch => SketchParams::CountSketch(CountSketchParams {
            w: 2048,
            d: 5,
            with_heap: true,
        }),
        SketchKind::Cms => SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
    };
    let synthetic = EdgeSketchProcessor {
        processor_name: sketch_kind_to_processor_name(kind).to_string(),
        sketch_kind: kind.clone(),
        sketch_params: params,
        aggregation_id: format!("agg_default_{}", sketch_kind_tag(kind)),
    };
    build_edge_processor_block(&synthetic, window_secs, &[], metric_name_hint)
}

/// Resolve an `ExportTarget` to a concrete `endpoint:port` string. Phase
/// B uses documented placeholder hostnames (`backend:4317` for the
/// edge→backend default; `gateway:4317` is reachable when a caller
/// explicitly opts in via `default_host`) for symbolic stages — Phase C
/// plumbs a real `DeploymentConstraints::executors()` resolver.
fn resolve_export_endpoint(default_host: &str, target: &ExportTarget) -> String {
    match target {
        ExportTarget::Endpoint(s) => s.clone(),
        ExportTarget::Stage(StageId::Edge) => "edge:4317".to_string(),
        ExportTarget::Stage(StageId::Gateway) => format!("{default_host}:4317"),
        ExportTarget::Stage(StageId::Backend) => format!("{default_host}:4317"),
    }
}

/// Build the `(component_id, yaml)` pair for an OTLP exporter pointed
/// at the supplied symbolic / concrete target. `default_host` is the
/// host portion used when the target is a symbolic stage role.
fn build_otlp_exporter(default_host: &str, target: &ExportTarget) -> (String, Value) {
    let endpoint = resolve_export_endpoint(default_host, target);
    let yaml = format!("endpoint: \"{endpoint}\"\ntls:\n  insecure: true\ncompression: none\n",);
    (
        "otlp/backend".to_string(),
        serde_yaml::from_str(&yaml).expect("inline OTLP exporter yaml is valid"),
    )
}

/// Build the per-edge-processor parameter block. Mirrors the param
/// surface of `crate::config::agent::build_processor_block` but reads
/// from the typed `EdgeSketchProcessor` + ambient `EdgeStageConfig`
/// fields rather than the legacy `AgentCollectorConfig`.
fn build_edge_processor_block(
    sp: &EdgeSketchProcessor,
    window_secs: Option<u64>,
    label_filters: &[(String, String)],
    metric_name_hint: Option<&str>,
) -> Value {
    let mut m = Mapping::new();

    // Mode — `window` whenever a window landed on edge, else `batch`.
    if let Some(w) = window_secs {
        m.insert("mode".into(), Value::String("window".to_string()));
        m.insert("window_duration".into(), Value::String(format!("{w}s")));
    } else {
        m.insert("mode".into(), Value::String("batch".to_string()));
    }
    m.insert("transmit_sketch".into(), Value::Bool(true));
    m.insert("enable_self_monitoring".into(), Value::Bool(true));

    // Label matchers — same `[{key, value}]` shape the legacy agent
    // emitter uses (Go processor expects `[]LabelMatcher{Key, Value}`).
    if !label_filters.is_empty() {
        let matchers: Vec<Value> = label_filters
            .iter()
            .map(|(k, v)| {
                let mut e = Mapping::new();
                e.insert("key".into(), Value::String(k.clone()));
                e.insert("value".into(), Value::String(v.clone()));
                Value::Mapping(e)
            })
            .collect();
        m.insert("label_matchers".into(), Value::Sequence(matchers));
    }

    // Family-specific params.
    //
    // `delta_transmission` is set to `true` for the four families
    // that support sparse delta encoding (DDSketch, HLL, CountSketch,
    // Count-Min). KLL deliberately does NOT get the flag — KLL uses
    // randomised compaction and is not additively mergeable, so its
    // wire payload is always full state. The KLL processor's
    // `Config.Validate` rejects `delta_transmission: true` with an
    // error rather than silently falling back; emitting the flag
    // would break agent boot. See `Implementation.tex` ("KLL has
    // no delta variant and matches its full cost") and
    // `kllprocessor/config.go::Config.Validate`.
    //
    // The flag matches the four factories' `DeltaTransmission: true`
    // defaults (see `factory.go` in each processor); we still emit
    // it explicitly so the wire YAML doesn't depend on a factory
    // default that could regress to full-state in a future build.
    match &sp.sketch_params {
        SketchParams::Kll(p) => {
            m.insert("k".into(), Value::Number((p.k as u64).into()));
            // No delta_transmission for KLL: see comment above.
        }
        SketchParams::DDSketch(p) => {
            m.insert("relative_accuracy".into(), Value::Number(p.alpha.into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::Hll(_p) => {
            // HLL takes no precision knob in its Config (the
            // patched build hard-codes p=14); nothing further to set.
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::Cms(p) => {
            m.insert(
                "metric_name".into(),
                Value::String(
                    metric_name_hint
                        .unwrap_or("endpoint_request_freq")
                        .to_string(),
                ),
            );
            m.insert("rows".into(), Value::Number((p.d as u64).into()));
            m.insert("columns".into(), Value::Number((p.w as u64).into()));
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::CountSketch(p) => {
            // Translate (w, d) to the legacy (epsilon, delta) surface
            // that the patched countsketch processor's Config accepts —
            // matches `crate::sketch_algebra::params::SketchParams::to_legacy`.
            let epsilon = std::f64::consts::E / (p.w as f64);
            let delta = 2f64.powi(-(p.d as i32));
            m.insert("epsilon".into(), Value::Number(epsilon.into()));
            m.insert("delta".into(), Value::Number(delta.into()));
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
    }

    Value::Mapping(m)
}

/// Compute the gateway-side merge processor name for a `GatewayMergeProcessor`.
///
/// Today the typed emitter populates every entry's `processor_name`
/// with the placeholder `"sketchmergeprocessor"`; the patched contrib
/// build instead has per-family merge processors:
/// `kllmerge`, `ddsketchmerge`, `hllmerge`, `countminsketchmerge`,
/// `countsketchmerge`. We map the kind to the family-specific name
/// here so the emitted YAML round-trips through the patched build.
fn gateway_merge_processor_name(mp: &GatewayMergeProcessor) -> String {
    match mp.sketch_kind {
        SketchKind::Kll => "kllmerge".to_string(),
        SketchKind::DDSketch => "ddsketchmerge".to_string(),
        SketchKind::Hll => "hllmerge".to_string(),
        SketchKind::Cms => "countminsketchmerge".to_string(),
        SketchKind::CountSketch => "countsketchmerge".to_string(),
    }
}

/// Build the per-merge-processor parameter block for the gateway YAML.
fn build_gateway_merge_block(mp: &GatewayMergeProcessor) -> Value {
    let mut m = Mapping::new();
    m.insert("mode".into(), Value::String("merge".to_string()));
    m.insert(
        "aggregation_id".into(),
        Value::String(mp.aggregation_id.clone()),
    );
    m.insert(
        "sketch_kind".into(),
        Value::String(sketch_kind_tag(&mp.sketch_kind).to_string()),
    );
    Value::Mapping(m)
}

/// Build one aggregation row in the backend streaming-config JSON.
///
/// Wire shape is aligned to what `asap_types::AggregationConfig::from_yaml_data`
/// requires:
///
/// * `aggregationType` — sketch family.
/// * `aggregationSubType` — always empty; reserved for future
///   sub-family distinctions.
/// * `metric` — source metric the aggregation runs over.
/// * `labels.{grouping,rollup,aggregated}` — three label lists the
///   backend's `KeyByLabelNames` parser keys on. Today the typed L5
///   only surfaces an empty grouping; future work threads
///   `QueryExpr::Aggregate.by` through `BackendAggregation` so grouping
///   propagates faithfully.
/// * `parameters` — sketch-family-specific params (alpha, K, precision…).
/// * `windowSize` / `windowType` — tumbling window in seconds.
/// * `spatialFilter` — comma-joined `k=v` pairs from the edge's label
///   filters.
/// * `aggregationInput` — Phase ε.1 Mode 1/2 marker (sketch_envelope vs
///   raw); preserved so Phase ε.2's raw-input ingest path stays plumbed.
///
/// `aggregation_id` is **intentionally omitted** from the wire — PR 5
/// retired the controller-allocated id; identity is content-addressed
/// in the backend via `PolicyFingerprint(u64)` derived from the fields
/// above.
fn build_backend_aggregation_json(agg: &BackendAggregation) -> JsonValue {
    let parameters = sketch_params_to_json(&agg.sketch_params);
    let aggregation_input = match agg.aggregation_input {
        AggregationInput::SketchEnvelope => "sketch_envelope",
        AggregationInput::Raw => "raw",
    };
    json!({
        "aggregationType": sketch_kind_to_backend_type(&agg.sketch_kind),
        "aggregationSubType": "",
        "metric": agg.metric_name,
        "labels": {
            "grouping": agg.grouping,
            "rollup": Vec::<String>::new(),
            "aggregated": Vec::<String>::new(),
        },
        "parameters": parameters,
        "windowSize": agg.window_secs,
        "windowType": "tumbling",
        "spatialFilter": agg.spatial_filter,
        "aggregationInput": aggregation_input,
    })
}

/// Build one readout row in the backend streaming-config JSON.
///
/// `aggregation_id` is **intentionally omitted** — PR 5's content-
/// addressing convention applies to readouts the same way it applies
/// to aggregations (the controller-allocated string IDs are not on
/// the wire). The backend's current `StreamingConfig::from_yaml_data`
/// doesn't consume the `readouts` list at all; when it eventually
/// does, the cross-reference to its source aggregation will be
/// content-shaped (metric / sketch_kind / params), derived from the
/// `aggregations` list by the same `PolicyFingerprint` recipe.
fn build_backend_readout_json(r: &BackendReadout) -> JsonValue {
    match &r.op {
        EstimateOp::Quantile { q } => json!({
            "op": "quantile",
            "q": q,
        }),
        EstimateOp::Cardinality => json!({
            "op": "cardinality",
        }),
        EstimateOp::PointCount { key } => json!({
            "op": "point_count",
            "key": key,
        }),
        EstimateOp::TopK { k } => json!({
            "op": "topk",
            "k": k,
        }),
    }
}

/// Map a `SketchKind` to the backend's `AggregationType::Display` string
/// — the same mapping
/// [`crate::config::asapquery_backend::map_sketch_type_to_agg_type`] uses
/// (the strings must match `AggregationType::FromStr` in the backend's
/// `promql_utilities::query_logics::enums`).
fn sketch_kind_to_backend_type(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::DDSketch => "DDSketch",
        SketchKind::Kll => "DatasketchesKLL",
        SketchKind::Hll => "HLL",
        SketchKind::CountSketch => "CountSketch",
        SketchKind::Cms => "CountMinSketch",
    }
}

/// Stable lowercase tag for a `SketchKind` — used as a passthrough
/// `sketch_kind` field in YAML so downstream consumers can dispatch
/// without round-tripping through serde.
fn sketch_kind_tag(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::Kll => "kll",
        SketchKind::DDSketch => "ddsketch",
        SketchKind::Hll => "hll",
        SketchKind::Cms => "cms",
        SketchKind::CountSketch => "count_sketch",
    }
}

/// Serialize a `SketchParams` payload to a flat JSON object the backend
/// can read directly without round-tripping through the controller's
/// internally-tagged enum form.
fn sketch_params_to_json(p: &SketchParams) -> JsonValue {
    match p {
        SketchParams::Kll(p) => json!({ "k": p.k }),
        SketchParams::DDSketch(p) => json!({ "alpha": p.alpha }),
        SketchParams::Hll(p) => json!({ "precision": p.precision }),
        SketchParams::Cms(p) => json!({ "w": p.w, "d": p.d }),
        SketchParams::CountSketch(p) => {
            json!({ "w": p.w, "d": p.d, "with_heap": p.with_heap })
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch_algebra::params::{
        CmsParams, CountSketchParams, DDSketchParams, HllParams, KllParams,
    };

    fn ddsketch_edge_cfg() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: vec![("service".to_string(), "api".to_string())],
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "ddsketch".to_string(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: HashMap::new(),
        }
    }

    #[test]
    fn edge_yaml_contains_processor_and_pipeline_refs() {
        let yaml = emit_edge_yaml(&ddsketch_edge_cfg(), "ws://ctrl:4320/v1/opamp")
            .expect("emit_edge_yaml ok");

        // Receiver block.
        assert!(
            yaml.contains("receivers:"),
            "missing receivers section\n{yaml}"
        );
        assert!(yaml.contains("otlp:"), "missing otlp receiver key\n{yaml}");
        assert!(yaml.contains("4317"), "missing gRPC port\n{yaml}");

        // Processor key + pipeline ref.
        assert!(yaml.contains("ddsketch:"), "missing ddsketch key\n{yaml}");
        assert!(
            yaml.contains("- ddsketch"),
            "pipeline must reference ddsketch\n{yaml}"
        );

        // Window + label filter surfaced.
        assert!(
            yaml.contains("window_duration: 60s"),
            "missing window_duration\n{yaml}"
        );
        assert!(yaml.contains("relative_accuracy"), "missing alpha\n{yaml}");
        assert!(
            yaml.contains("key: service"),
            "missing label matcher key\n{yaml}"
        );
        assert!(
            yaml.contains("value: api"),
            "missing label matcher value\n{yaml}"
        );
        assert!(
            !yaml.contains("aggregation_id:") && !yaml.contains("sketch_kind:"),
            "edge processor config must not emit planning-only fields rejected by OTel configs\n{yaml}"
        );

        // Exporter — asapquery-backend OTLP ingest.
        assert!(yaml.contains("otlp/backend:"), "missing exporter\n{yaml}");
        assert!(
            yaml.contains("backend:4317"),
            "exporter should target asapquery-backend\n{yaml}"
        );

        // OpAMP extension carries the controller endpoint.
        assert!(
            yaml.contains("ws://ctrl:4320/v1/opamp"),
            "missing opamp endpoint\n{yaml}"
        );
    }

    #[test]
    fn edge_yaml_kll_uses_k_param() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.sketch_processors[0] = EdgeSketchProcessor {
            processor_name: "KLL".to_string(),
            sketch_kind: SketchKind::Kll,
            sketch_params: SketchParams::Kll(KllParams { k: 200 }),
            aggregation_id: "agg7".to_string(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("KLL:"), "{yaml}");
        assert!(yaml.contains("k: 200"), "{yaml}");
        assert!(
            !yaml.contains("relative_accuracy"),
            "KLL must not carry alpha\n{yaml}"
        );
        assert!(
            !yaml.contains("encoding:"),
            "KLL Config does not accept encoding\n{yaml}"
        );
        // KLL has no delta variant: the KLL's `Config.Validate`
        // rejects `delta_transmission: true`. Make sure we don't emit
        // the flag (a future regression that flips it on globally would
        // break agent boot for KLL).
        assert!(
            !yaml.contains("delta_transmission"),
            "KLL emit must NOT carry delta_transmission\n{yaml}"
        );
    }

    #[test]
    fn edge_yaml_emits_delta_transmission_for_supported_families() {
        // DDSketch / HLL / CountSketch / Count-Min all support sparse
        // delta encoding — the controller emits `delta_transmission:
        // true` so the per-window wire footprint is the bucket / cell
        // diff, not the full sketch state. KLL deliberately omits the
        // flag (see `edge_yaml_kll_uses_k_param`).
        for (kind, processor_name, params) in [
            (
                SketchKind::DDSketch,
                "ddsketch",
                SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
            ),
            (
                SketchKind::Hll,
                "HLL",
                SketchParams::Hll(HllParams { precision: 14 }),
            ),
            (
                SketchKind::CountSketch,
                "countsketch",
                SketchParams::CountSketch(CountSketchParams {
                    w: 2048,
                    d: 5,
                    with_heap: true,
                }),
            ),
            (
                SketchKind::Cms,
                "countmin",
                SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
            ),
        ] {
            let mut cfg = ddsketch_edge_cfg();
            cfg.sketch_processors[0] = EdgeSketchProcessor {
                processor_name: processor_name.to_string(),
                sketch_kind: kind,
                sketch_params: params,
                aggregation_id: "agg-delta".to_string(),
            };
            let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
            assert!(
                yaml.contains("delta_transmission: true"),
                "{processor_name:?} emit must carry delta_transmission: true\n{yaml}"
            );
        }
    }

    #[test]
    fn edge_yaml_countmin_includes_required_metric_name() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.source_metric = Some("endpoint_request_freq".to_string());
        cfg.sketch_processors[0] = EdgeSketchProcessor {
            processor_name: "countmin".to_string(),
            sketch_kind: SketchKind::Cms,
            sketch_params: SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
            aggregation_id: "agg-cms".to_string(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("countmin:"), "{yaml}");
        assert!(
            yaml.contains("metric_name: endpoint_request_freq"),
            "{yaml}"
        );
    }

    #[test]
    fn edge_yaml_batch_mode_when_no_window() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.window_secs = None;
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("mode: batch"), "{yaml}");
        assert!(
            !yaml.contains("window_duration"),
            "batch mode must not have window_duration\n{yaml}"
        );
    }

    fn ddsketch_gateway_cfg() -> GatewayStageConfig {
        GatewayStageConfig {
            otlp_receiver_port: 4317,
            merge_processors: vec![GatewayMergeProcessor {
                processor_name: "sketchmergeprocessor".to_string(),
                sketch_kind: SketchKind::DDSketch,
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        }
    }

    #[test]
    fn gateway_yaml_uses_family_specific_merge_name() {
        let yaml = emit_gateway_yaml(&ddsketch_gateway_cfg(), "ws://ctrl:4320/v1/opamp")
            .expect("emit_gateway_yaml ok");

        // Family-specific merge name (NOT the placeholder).
        assert!(yaml.contains("ddsketchmerge:"), "{yaml}");
        assert!(yaml.contains("- ddsketchmerge"), "{yaml}");
        assert!(
            !yaml.contains("sketchmergeprocessor"),
            "placeholder must be replaced\n{yaml}"
        );

        // Receiver bound to declared port.
        assert!(yaml.contains("0.0.0.0:4317"), "{yaml}");

        // Aggregation id threaded through.
        assert!(yaml.contains("aggregation_id: agg0"), "{yaml}");

        // Exporter targets backend.
        assert!(yaml.contains("backend:4317"), "{yaml}");

        // OpAMP endpoint embedded.
        assert!(yaml.contains("ws://ctrl:4320/v1/opamp"), "{yaml}");
    }

    #[test]
    fn gateway_yaml_emits_one_processor_per_merge_entry() {
        let cfg = GatewayStageConfig {
            otlp_receiver_port: 4317,
            merge_processors: vec![
                GatewayMergeProcessor {
                    processor_name: "x".into(),
                    sketch_kind: SketchKind::Kll,
                    aggregation_id: "agg0".into(),
                },
                GatewayMergeProcessor {
                    processor_name: "x".into(),
                    sketch_kind: SketchKind::Hll,
                    aggregation_id: "agg1".into(),
                },
            ],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        };
        let yaml = emit_gateway_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("kllmerge:"), "{yaml}");
        assert!(yaml.contains("hllmerge:"), "{yaml}");
        assert!(
            yaml.contains("- kllmerge"),
            "pipeline missing kll merge\n{yaml}"
        );
        assert!(
            yaml.contains("- hllmerge"),
            "pipeline missing hll merge\n{yaml}"
        );
    }

    #[test]
    fn backend_json_round_trips_aggregations_and_readouts() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                BackendAggregation {
                    aggregation_id: "agg0".into(),
                    metric_name: "http_latency_ms".into(),
                    sketch_kind: SketchKind::DDSketch,
                    sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                    window_secs: 60,
                    spatial_filter: String::new(),
                    grouping: Vec::new(),
                    aggregation_input: AggregationInput::SketchEnvelope,
                },
                BackendAggregation {
                    aggregation_id: "agg1".into(),
                    metric_name: "http_requests_total".into(),
                    sketch_kind: SketchKind::Hll,
                    sketch_params: SketchParams::Hll(HllParams { precision: 14 }),
                    window_secs: 60,
                    spatial_filter: String::new(),
                    grouping: Vec::new(),
                    aggregation_input: AggregationInput::SketchEnvelope,
                },
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: EstimateOp::Quantile { q: 0.99 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: EstimateOp::Cardinality,
                },
            ],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");

        let aggs = v["aggregations"].as_array().expect("aggregations array");
        assert_eq!(aggs.len(), 2, "{v}");
        // PR 5: `aggregationId` is no longer on the wire — identity is
        // content-addressed in the backend via `PolicyFingerprint(u64)`.
        assert!(
            aggs[0].get("aggregationId").is_none(),
            "controller must not emit aggregationId\n{v}"
        );
        assert_eq!(aggs[0]["aggregationType"], "DDSketch");
        assert_eq!(aggs[0]["metric"], "http_latency_ms");
        assert_eq!(aggs[0]["windowSize"], 60);
        assert_eq!(aggs[0]["windowType"], "tumbling");
        assert_eq!(aggs[0]["parameters"]["alpha"], 0.01);
        assert_eq!(aggs[1]["aggregationType"], "HLL");
        assert_eq!(aggs[1]["metric"], "http_requests_total");
        assert_eq!(aggs[1]["parameters"]["precision"], 14);

        let reads = v["readouts"].as_array().expect("readouts array");
        assert_eq!(reads.len(), 2, "{v}");
        assert_eq!(reads[0]["op"], "quantile");
        assert_eq!(reads[0]["q"], 0.99);
        assert_eq!(reads[1]["op"], "cardinality");
    }

    #[test]
    fn backend_json_handles_topk_and_pointcount_readouts() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                BackendAggregation {
                    aggregation_id: "agg0".into(),
                    metric_name: "endpoint_count".into(),
                    sketch_kind: SketchKind::CountSketch,
                    sketch_params: SketchParams::CountSketch(CountSketchParams {
                        w: 2048,
                        d: 5,
                        with_heap: true,
                    }),
                    window_secs: 60,
                    spatial_filter: String::new(),
                    grouping: Vec::new(),
                    aggregation_input: AggregationInput::SketchEnvelope,
                },
                BackendAggregation {
                    aggregation_id: "agg1".into(),
                    metric_name: "endpoint_hits".into(),
                    sketch_kind: SketchKind::Cms,
                    sketch_params: SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
                    window_secs: 60,
                    spatial_filter: String::new(),
                    grouping: Vec::new(),
                    aggregation_input: AggregationInput::SketchEnvelope,
                },
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: EstimateOp::TopK { k: 10 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: EstimateOp::PointCount {
                        key: "user_42".into(),
                    },
                },
            ],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        let reads = v["readouts"].as_array().unwrap();
        assert_eq!(reads[0]["op"], "topk");
        assert_eq!(reads[0]["k"], 10);
        assert_eq!(reads[1]["op"], "point_count");
        assert_eq!(reads[1]["key"], "user_42");

        let aggs = v["aggregations"].as_array().unwrap();
        assert_eq!(aggs[0]["aggregationType"], "CountSketch");
        assert_eq!(aggs[0]["parameters"]["with_heap"], true);
        assert_eq!(aggs[1]["aggregationType"], "CountMinSketch");
        assert_eq!(aggs[1]["parameters"]["w"], 4096);
    }

    #[test]
    fn export_target_endpoint_is_passed_through_verbatim() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.exporter_target = ExportTarget::Endpoint("custom-gw:5317".into());
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("custom-gw:5317"), "{yaml}");
    }

    // ── Phase α: BackendStorageRouting emitter tests ──────────────────────

    /// Helper: build a single-aggregation BackendStageConfig of the
    /// requested kind. `aggregation_id` is hard-coded — the routing
    /// emitter doesn't care about it.
    fn backend_cfg_with_kind(kind: SketchKind) -> BackendStageConfig {
        let params = match kind {
            SketchKind::DDSketch => SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
            SketchKind::Kll => SketchParams::Kll(KllParams { k: 200 }),
            SketchKind::Hll => SketchParams::Hll(HllParams { precision: 14 }),
            SketchKind::Cms => SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
            SketchKind::CountSketch => SketchParams::CountSketch(CountSketchParams {
                w: 2048,
                d: 5,
                with_heap: true,
            }),
        };
        BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "agg0".into(),
                metric_name: "test_metric".into(),
                sketch_kind: kind.clone(),
                sketch_params: params,
                window_secs: 60,
                spatial_filter: String::new(),
                grouping: Vec::new(),
                aggregation_input: AggregationInput::SketchEnvelope,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".into(),
                op: match kind {
                    SketchKind::DDSketch | SketchKind::Kll => EstimateOp::Quantile { q: 0.99 },
                    SketchKind::Hll => EstimateOp::Cardinality,
                    SketchKind::CountSketch => EstimateOp::TopK { k: 10 },
                    SketchKind::Cms => EstimateOp::PointCount {
                        key: "user_42".into(),
                    },
                },
            }],
        }
    }

    #[test]
    fn storage_routing_emits_default_engine_and_metrics_array() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let plans: Vec<(String, &BackendStageConfig)> =
            vec![("http_request_duration_seconds".to_string(), &ddsketch)];
        let v = emit_backend_storage_routing(&plans).expect("emit ok");
        assert_eq!(v["default_engine"], "asap_query");
        let metrics = v["metrics"].as_array().expect("metrics array");
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["name"], "http_request_duration_seconds");
    }

    // ── Per-tenant routing emit tests (follow-up to PR #333) ──────────────

    /// Single-tenant entry point — the convenience
    /// [`emit_backend_storage_routing`] alias must keep emitting the
    /// `default` tenant id so existing single-tenant deploys are
    /// byte-compatible (modulo the new `tenant` field appearing).
    #[test]
    fn storage_routing_default_tenant_for_single_tenant_emit() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let v = emit_backend_storage_routing(&[("latency".into(), &ddsketch)]).expect("emit ok");
        assert_eq!(v["tenant"], DEFAULT_TENANT);
    }

    /// Per-tenant entry point — explicit `tenant` arg lands in the
    /// emitted JSON's top-level `tenant` field. Other fields are
    /// unchanged from the single-tenant emit, so the backend's
    /// per-tenant swap routes to the named tenant's slot via the
    /// body-tenant precedence rule.
    #[test]
    fn storage_routing_for_tenant_emits_explicit_tenant_field() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let v =
            emit_backend_storage_routing_for_tenant("tenant-a", &[("latency".into(), &ddsketch)])
                .expect("emit ok");
        assert_eq!(v["tenant"], "tenant-a");
        assert_eq!(v["default_engine"], "asap_query");
        // Single metric, single warm + archive target shape — the
        // per-tenant emit doesn't change the metric-side shape.
        let metrics = v["metrics"].as_array().expect("metrics array");
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["name"], "latency");
    }

    /// Per-tenant variant of the prometheus-aware emit — tenant
    /// scope must thread through Mode-3 metrics too.
    #[test]
    fn storage_routing_with_prometheus_for_tenant_emits_explicit_tenant_field() {
        let mode3 = vec!["http_requests_total".to_string()];
        let v = emit_backend_storage_routing_with_prometheus_for_tenant("tenant-b", &[], &mode3)
            .expect("emit ok");
        assert_eq!(v["tenant"], "tenant-b");
        assert_eq!(v["metrics"][0]["name"], "http_requests_total");
        assert_eq!(v["metrics"][0]["targets"][0]["engine"], "thanos_query");
    }

    #[test]
    fn storage_routing_ddasap_query_serves_quantile_archive_serves_others() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let v = emit_backend_storage_routing(&[("latency".into(), &ddsketch)]).expect("emit ok");
        let metric = &v["metrics"][0];
        let targets = metric["targets"].as_array().expect("targets array");

        // Default slot — ASAP tier, no filter.
        assert_eq!(targets[0]["engine"], "asap_query");
        assert!(
            targets[0].get("applies_to_query_shape").is_none(),
            "warm slot must be the default (no filter); got {targets:?}"
        );

        // Archive slot — must carry the predictable archive shapes.
        assert_eq!(targets[1]["engine"], "thanos_query");
        let archive_shapes: Vec<String> = targets[1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert!(archive_shapes.contains(&"histogram_quantile".to_string()));
        assert!(archive_shapes.contains(&"delta".to_string()));
        assert!(archive_shapes.contains(&"absent".to_string()));
        assert!(archive_shapes.contains(&"rate_post_hoc".to_string()));
        // DDSketch planned → `topk` and `count` not ASAP-tier-eligible
        // (only quantile is). Both stay in archive's claim list.
        assert!(archive_shapes.contains(&"topk".to_string()));
        assert!(archive_shapes.contains(&"count".to_string()));

        // Warm-tier native shapes surfaced for spot-check.
        let warm_native: Vec<String> = metric["asap_tier_native_shapes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert!(warm_native.contains(&"quantile".to_string()));
        assert!(warm_native.contains(&"quantile_over_time".to_string()));
    }

    #[test]
    fn storage_routing_count_sketch_pulls_topk_off_archive() {
        let cs = backend_cfg_with_kind(SketchKind::CountSketch);
        let v = emit_backend_storage_routing(&[("requests".into(), &cs)]).expect("emit ok");
        let archive_shapes: Vec<String> = v["metrics"][0]["targets"][1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        // Count-Sketch planned → ASAP tier serves `topk`, archive
        // claim list must NOT include topk.
        assert!(
            !archive_shapes.contains(&"topk".to_string()),
            "Count-Sketch planned ⇒ topk must drop off the archive list; got {archive_shapes:?}"
        );
        // `count` still routes to archive (no HLL / CMS).
        assert!(archive_shapes.contains(&"count".to_string()));
    }

    #[test]
    fn storage_routing_hll_pulls_count_off_archive() {
        let hll = backend_cfg_with_kind(SketchKind::Hll);
        let v = emit_backend_storage_routing(&[("active_users".into(), &hll)]).expect("emit ok");
        let archive_shapes: Vec<String> = v["metrics"][0]["targets"][1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        // HLL planned → ASAP tier serves `count` (cardinality);
        // archive claim list must NOT include count. `topk` still
        // routes to archive (no Count-Sketch).
        assert!(
            !archive_shapes.contains(&"count".to_string()),
            "HLL planned ⇒ count must drop off the archive list; got {archive_shapes:?}"
        );
        assert!(archive_shapes.contains(&"topk".to_string()));
    }

    #[test]
    fn storage_routing_three_metric_snapshot_stable() {
        // Snapshot test: three metrics with three different sketch
        // families. The serialized form must be deterministic across
        // runs (HashMap iteration order can drift, but our impl
        // stages everything through a Vec so order matches input
        // order).
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let hll = backend_cfg_with_kind(SketchKind::Hll);
        let cs = backend_cfg_with_kind(SketchKind::CountSketch);
        let plans: Vec<(String, &BackendStageConfig)> = vec![
            ("http_requests_total".into(), &cs),
            ("active_users".into(), &hll),
            ("request_latency_seconds".into(), &ddsketch),
        ];
        let v = emit_backend_storage_routing(&plans).expect("emit ok");
        let s = serde_json::to_string_pretty(&v).expect("ser");

        // Pretty-print the snapshot for easy regression diffing.
        // Per-tenant follow-up to PR #333: the top-level `tenant`
        // field is now emitted (defaults to `"default"` for the
        // single-tenant entry point). The `serde_json::Value` map
        // serialises keys alphabetically, so `tenant` lands at the
        // end of the document.
        let expected = r#"{
  "default_engine": "asap_query",
  "metrics": [
    {
      "asap_tier_native_shapes": [
        "topk",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ],
      "name": "http_requests_total",
      "targets": [
        {
          "engine": "asap_query"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "count"
          ],
          "engine": "thanos_query"
        }
      ]
    },
    {
      "asap_tier_native_shapes": [
        "count",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ],
      "name": "active_users",
      "targets": [
        {
          "engine": "asap_query"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "topk"
          ],
          "engine": "thanos_query"
        }
      ]
    },
    {
      "asap_tier_native_shapes": [
        "quantile",
        "quantile_over_time",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ],
      "name": "request_latency_seconds",
      "targets": [
        {
          "engine": "asap_query"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "topk",
            "count"
          ],
          "engine": "thanos_query"
        }
      ]
    }
  ],
  "tenant": "default"
}"#;
        assert_eq!(s, expected, "snapshot mismatch:\n{s}");
    }

    #[test]
    fn storage_routing_empty_input_emits_empty_metrics_array() {
        let v = emit_backend_storage_routing(&[]).expect("emit ok");
        assert_eq!(v["default_engine"], "asap_query");
        assert_eq!(v["metrics"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn storage_routing_empty_aggregations_still_emits_archive_default() {
        // A plan with no aggregations (degenerate; should not happen
        // in practice but we don't want to panic). The metric still
        // lands in the table as archive-only — no ASAP-tier-native
        // shapes, no ASAP-tier annotation field.
        let cfg = BackendStageConfig {
            aggregations: vec![],
            readouts: vec![],
        };
        let v = emit_backend_storage_routing(&[("orphan".into(), &cfg)]).expect("emit ok");
        let metric = &v["metrics"][0];
        assert_eq!(metric["name"], "orphan");
        // No asap_tier_native_shapes side field.
        assert!(metric.get("asap_tier_native_shapes").is_none());
        // Targets: ASAP-tier default + archive default-shape list.
        let targets = metric["targets"].as_array().unwrap();
        assert_eq!(targets[0]["engine"], "asap_query");
        assert_eq!(targets[1]["engine"], "thanos_query");
    }

    // ── Phase β: emit_backend_streaming_config_json snapshot for new pattern coverage ──
    //
    // The archive-only L3 intents (Absent, Present, Delta, Deriv, …)
    // bind to `PhysicalExpr::Logical` rather than producing a `BackendAggregation`,
    // so they correctly stay OUT of the ASAP-tier StreamingConfig the
    // backend's ASAPQueryEngine receives. Phase α wires the archive routing
    // entry separately. This snapshot pins that contract.

    /// Snapshot: an empty `BackendStageConfig` produces the canonical
    /// `{"aggregations": [], "readouts": []}` shape — what the backend
    /// receives when every intent in the workload is archive-only.
    #[test]
    fn phase_b_empty_asap_tier_snapshot_for_all_archive_only_workload() {
        let cfg = BackendStageConfig {
            aggregations: vec![],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"{"aggregations":[],"readouts":[]}"#);
    }

    /// Snapshot: every Phase β ASAP-tier-bound intent (KLL/DDSketch
    /// quantile, HLL cardinality, CMS frequency, CountSketch topk) maps to
    /// a stable `aggregationType` string the backend's `AggregationType::
    /// FromStr` recognises. This is the contract the L4 → L5 → backend
    /// pipeline relies on; pinning it here so a sketch-kind rename can't
    /// silently break the backend.
    #[test]
    fn phase_b_backend_agg_type_strings_for_every_sketch_kind() {
        let cases = vec![
            (SketchKind::Kll, "DatasketchesKLL"),
            (SketchKind::DDSketch, "DDSketch"),
            (SketchKind::Hll, "HLL"),
            (SketchKind::Cms, "CountMinSketch"),
            (SketchKind::CountSketch, "CountSketch"),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                sketch_kind_to_backend_type(&kind),
                expected,
                "sketch_kind_to_backend_type({kind:?}) drift — backend FromStr will reject"
            );
        }
    }

    /// Grouping labels surface under `labels.grouping` in the emitted
    /// JSON. The L5 emitter itself leaves the list empty; `handle_plan`
    /// patches it from `workload.group_by_labels` before posting, so
    /// here we simulate that by setting `grouping` on the
    /// `BackendAggregation` directly and assert the JSON round-trips.
    #[test]
    fn backend_json_emits_grouping_under_labels() {
        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "agg0".into(),
                metric_name: "http_latency_ms".into(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                window_secs: 30,
                spatial_filter: String::new(),
                grouping: vec!["zone".into(), "service".into()],
                aggregation_input: AggregationInput::SketchEnvelope,
            }],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        let grouping = v["aggregations"][0]["labels"]["grouping"]
            .as_array()
            .expect("grouping array");
        let names: Vec<&str> = grouping.iter().filter_map(|s| s.as_str()).collect();
        assert_eq!(
            names,
            vec!["zone", "service"],
            "labels.grouping must surface BackendAggregation.grouping verbatim\n{v}"
        );
        // The other label lists stay empty — the L5 controller doesn't
        // yet emit rollup / aggregated.
        assert!(v["aggregations"][0]["labels"]["rollup"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(v["aggregations"][0]["labels"]["aggregated"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    /// Snapshot: aggregations + readouts together exhibit the
    /// id-aliasing the backend uses to wire readouts back to their
    /// producing aggregation. Pins the sort order + key names. Phase β
    /// uses this as the wire-format anchor for the wider intent set —
    /// the JSON shape is intent-orthogonal, so adding new intents to L3
    /// can't drift this off so long as they bind through SketchKind /
    /// SketchParams.
    #[test]
    fn phase_b_backend_json_aggregation_readout_alias_snapshot() {
        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "phase_b_agg0".into(),
                metric_name: "phase_b_metric".into(),
                sketch_kind: SketchKind::Kll,
                sketch_params: SketchParams::Kll(KllParams { k: 200 }),
                window_secs: 60,
                spatial_filter: String::new(),
                grouping: Vec::new(),
                aggregation_input: AggregationInput::SketchEnvelope,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "phase_b_agg0".into(),
                op: EstimateOp::Quantile { q: 0.99 },
            }],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        // PR 5: `aggregationId` is no longer on the wire — neither on
        // aggregations nor readouts. Identity on the aggregation side is
        // content-derived (`PolicyFingerprint(u64)` over metric,
        // sketch_kind, params, grouping, spatial_filter); the readout-
        // to-aggregation cross-reference will be content-shaped too when
        // the backend starts consuming `readouts` (today it's silently
        // dropped by `StreamingConfig::from_yaml_data`).
        assert!(
            v["aggregations"][0].get("aggregationId").is_none(),
            "controller must not emit aggregationId on aggregations\n{v}"
        );
        assert!(
            v["readouts"][0].get("aggregationId").is_none(),
            "controller must not emit aggregationId on readouts\n{v}"
        );
        assert_eq!(v["aggregations"][0]["metric"], "phase_b_metric");
        assert_eq!(v["aggregations"][0]["aggregationType"], "DatasketchesKLL");
        assert_eq!(v["aggregations"][0]["parameters"]["k"], 200);
        assert_eq!(v["readouts"][0]["op"], "quantile");
        assert_eq!(v["readouts"][0]["q"], 0.99);
    }

    // ── Phase ε.1: three-mode wire shape tests ────────────────────────────

    /// Mode 1 (sketch at edge) keeps the existing aggregation_input
    /// default — `sketch_envelope` — so legacy plans round-trip
    /// unchanged.
    #[test]
    fn phase_eps1_mode1_aggregation_input_is_sketch_envelope() {
        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "agg0".into(),
                metric_name: "test_metric".into(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                window_secs: 60,
                spatial_filter: String::new(),
                grouping: Vec::new(),
                aggregation_input: AggregationInput::SketchEnvelope,
            }],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        assert_eq!(v["aggregations"][0]["aggregationInput"], "sketch_envelope");
    }

    /// Mode 2 (raw at edge → sketch at backend) sets
    /// `aggregation_input: raw` so the backend builds the sketch from
    /// raw OTLP samples at ingest. Phase ε.2 implements the raw-input
    /// ingest path on the backend.
    #[test]
    fn phase_eps1_mode2_aggregation_input_is_raw() {
        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "agg0".into(),
                metric_name: "test_metric".into(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                window_secs: 60,
                spatial_filter: String::new(),
                grouping: Vec::new(),
                aggregation_input: AggregationInput::Raw,
            }],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg).expect("emit ok");
        assert_eq!(v["aggregations"][0]["aggregationInput"], "raw");
    }

    /// Mode 3 (Prometheus archive) — the routing emitter adds a
    /// `thanos_query` engine target for the metric. The backend's
    /// HTTP query handler HTTP-forwards the matching PromQL queries to
    /// `${ASAP_PROMETHEUS_QUERY_URL}/api/v1/query`. Phase ε.2 registers
    /// the engine on the backend.
    #[test]
    fn phase_eps1_mode3_storage_routing_emits_thanos_query() {
        // No backend-side aggregations for mode 3 — Prometheus owns it.
        let mode3 = vec!["http_requests_total".to_string()];
        let v = emit_backend_storage_routing_with_prometheus(&[], &mode3).expect("emit ok");
        let metrics = v["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["name"], "http_requests_total");
        let targets = metrics[0]["targets"].as_array().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["engine"], "thanos_query");
        // No shape filter — Prometheus serves every PromQL shape.
        assert!(targets[0].get("applies_to_query_shape").is_none());
        // `asap_mode` annotation surfaces so operators can see why a
        // metric routes off ASAP tier.
        assert_eq!(metrics[0]["asap_mode"], "prometheus_archive");
    }

    /// Mode 1 + Mode 3 mixed in one cycle — ASAP-tier metric AND
    /// Prometheus-archive metric coexist in one routing JSON.
    #[test]
    fn phase_eps1_mixed_mode1_and_mode3_share_one_routing_table() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let plans: Vec<(String, &BackendStageConfig)> = vec![("latency_seconds".into(), &ddsketch)];
        let mode3 = vec!["http_requests_total".to_string()];
        let v = emit_backend_storage_routing_with_prometheus(&plans, &mode3).expect("emit ok");
        let metrics = v["metrics"].as_array().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0]["name"], "latency_seconds");
        // Mode-1 entry — full warm/archive routing.
        let m1_targets = metrics[0]["targets"].as_array().unwrap();
        assert_eq!(m1_targets[0]["engine"], "asap_query");
        assert_eq!(m1_targets[1]["engine"], "thanos_query");
        // Mode-3 entry — single thanos_query target.
        assert_eq!(metrics[1]["name"], "http_requests_total");
        let m3_targets = metrics[1]["targets"].as_array().unwrap();
        assert_eq!(m3_targets.len(), 1);
        assert_eq!(m3_targets[0]["engine"], "thanos_query");
    }

    /// Mode 3 emit_edge_yaml — produces a YAML with `otlphttp/prometheus`
    /// exporter pointing at `/api/v1/otlp/v1/metrics`, plus the routing
    /// processor that dispatches per-metric on `asap.mode`.
    #[test]
    fn phase_eps1_mode3_edge_yaml_has_otlphttp_prometheus_exporter() {
        let cfg = EdgeStageConfig {
            source_metric: Some("http_requests_total".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: vec![PrometheusArchiveMetric {
                metric: "http_requests_total".to_string(),
                window_secs: Some(60),
                label_proj: vec!["service.name".to_string()],
            }],
            // RawAtEdgePrometheusArchive auto-populates the archive
            // tier list as well (Phase 3.2.5): the Mode-3 metric also
            // lands in the Gorilla-S3 archive so the ASAP-tier engine
            // can serve last_over_time(...) queries.
            archive_tier_metrics: vec![ArchiveTierMetric {
                metric: "http_requests_total".to_string(),
                window_secs: Some(60),
            }],
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: HashMap::new(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // Exporter — Prometheus's native OTLP receiver, full path.
        assert!(
            yaml.contains("otlphttp/prometheus:"),
            "missing otlphttp/prometheus exporter\n{yaml}"
        );
        assert!(
            yaml.contains("/api/v1/otlp/v1/metrics"),
            "exporter should hit Prometheus's native OTLP path\n{yaml}"
        );
        assert!(
            yaml.contains("ASAP_PROMETHEUS_OTLP_URL"),
            "endpoint should be env-overridable for the deploy team\n{yaml}"
        );
        // `encoding: proto` — the Prometheus OTLP receiver expects
        // protobuf-encoded OTLP HTTP, not JSON.
        assert!(
            yaml.contains("encoding: proto"),
            "encoding should be proto\n{yaml}"
        );

        // Routing processor — dispatches by `asap.mode`.
        assert!(
            yaml.contains("routing:"),
            "missing routing processor\n{yaml}"
        );
        assert!(
            yaml.contains("from_attribute: asap.mode"),
            "routing should dispatch by asap.mode\n{yaml}"
        );
        assert!(
            yaml.contains("prometheus_archive"),
            "routing must match prometheus_archive value\n{yaml}"
        );

        // Two named pipelines + the routing entry pipeline.
        assert!(
            yaml.contains("metrics/prometheus_archive:"),
            "missing metrics/prometheus_archive pipeline\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/asap_tier:"),
            "missing metrics/asap_tier pipeline\n{yaml}"
        );
    }

    /// When no Mode 3 metrics are configured, the edge YAML stays
    /// single-pipeline (no routing processor, no otlphttp/prometheus
    /// exporter) — preserves the existing Phase β layout for backward
    /// compatibility.
    #[test]
    fn phase_eps1_no_mode3_edge_yaml_unchanged_from_phase_b() {
        let cfg = ddsketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(
            !yaml.contains("otlphttp/prometheus"),
            "no Mode 3 → no otlphttp/prometheus\n{yaml}"
        );
        assert!(
            !yaml.contains("metrics/prometheus_archive"),
            "no Mode 3 → no archive pipeline\n{yaml}"
        );
        assert!(
            !yaml.contains("metrics/asap_tier"),
            "no Mode 3 → main pipeline keeps the legacy `metrics:` name\n{yaml}"
        );
        // Phase 3.2.5 — without archive_tier_metrics no gorillas3 block.
        assert!(
            !yaml.contains("gorillas3"),
            "no archive tier → no gorillas3 processor\n{yaml}"
        );
    }

    // ── Phase 3.2.5 Bug (a) — gorillas3 in the emitted edge YAML ────────────

    /// Bug (a): when at least one archive-tier metric is configured the
    /// emitted YAML MUST include the `gorillas3` processor block + the
    /// processor MUST be in the ASAP-tier pipeline. Without this freshness
    /// probes (and any other archive-bound metric) never reach MinIO so
    /// the ASAP-tier engine's `last_over_time(...)` returns empty.
    #[test]
    fn phase_3_2_5_bug_a_archive_tier_metrics_emit_gorillas3_processor() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_archive".to_string(),
            window_secs: Some(5),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // Processor block surfaced at the top level.
        assert!(
            yaml.contains("gorillas3:"),
            "missing gorillas3 processor block\n{yaml}"
        );
        // Critical knobs the gorillas3processor's Config requires + the
        // ones the demo overlay inherits via env override.
        assert!(
            yaml.contains("block_format: prometheus_tsdb"),
            "gorillas3 must emit prometheus_tsdb blocks for the Thanos sidecar\n{yaml}"
        );
        assert!(
            yaml.contains("tsdb_bucket"),
            "gorillas3 needs a TSDBBucket so the Thanos store-gateway can read the blocks\n{yaml}"
        );
        assert!(
            yaml.contains("ASAP_MINIO_ENDPOINT"),
            "endpoint should be env-overridable for the deploy team\n{yaml}"
        );
        // `drop_original: false` so the metric ALSO flows downstream
        // through the ASAP-tier sketch / OTLP exporter (without this
        // the ASAP tier never sees the metric).
        assert!(
            yaml.contains("drop_original: false"),
            "drop_original must be false so ASAP-tier sketches still see the metric\n{yaml}"
        );
        // Processor name in the pipeline list.
        assert!(
            yaml.contains("- gorillas3"),
            "gorillas3 must appear in the ASAP-tier pipeline processors\n{yaml}"
        );
        // window_interval picked up from the smallest declared
        // window_secs — 5 here, matching the freshness-probe spec.
        assert!(
            yaml.contains("window_interval: 5s"),
            "gorillas3 window_interval must reflect the smallest archive-tier window\n{yaml}"
        );
    }

    /// Bug (a) corollary: gorillas3 runs BEFORE the sketch processor in
    /// the ASAP-tier pipeline so the cold-tier write happens on raw
    /// samples — mirrors `asap-otel-agent-b6-asap-single-sketch.yaml`'s
    /// canonical `[gorillas3, ddsketch, batch]` ordering.
    #[test]
    fn phase_3_2_5_bug_a_gorillas3_runs_before_sketch_in_pipeline() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_archive".to_string(),
            window_secs: Some(5),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // Find the pipeline processor list — should contain gorillas3
        // ahead of ddsketch in the serialized order. Robust
        // search: locate the `processors:` block under the metrics
        // pipeline and check substring positions.
        let pipeline_idx = yaml.find("metrics:\n").unwrap_or_default();
        let after_pipeline = &yaml[pipeline_idx..];
        let g_idx = after_pipeline
            .find("- gorillas3")
            .expect("- gorillas3 missing in pipeline");
        let s_idx = after_pipeline
            .find("- ddsketch")
            .expect("- ddsketch missing in pipeline");
        assert!(
            g_idx < s_idx,
            "gorillas3 must come BEFORE ddsketch in the warm pipeline\n{yaml}"
        );
    }

    // ── Phase 3.2.5 Bug (b) — ASAP-tier passthrough routing ─────────────────

    /// Bug (b): freshness probes (and other counters whose value IS
    /// the signal) must bypass the family-specific sketch processor so
    /// the metric name is preserved end-to-end. The L5 emitter adds a
    /// `routing` processor with OTTL `route()` statements that dispatch
    /// listed metrics to a `metrics/warm_passthrough` pipeline; everything
    /// else takes `metrics/asap_tier` as before.
    #[test]
    fn phase_3_2_5_bug_b_warm_passthrough_routes_around_sketch() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_warm".to_string(),
            window_secs: Some(1),
        }];
        cfg.warm_passthrough_metrics = vec!["http_freshness_probe_warm".to_string()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // Routing processor present, dispatches by metric name (OTTL form).
        assert!(
            yaml.contains("routing:"),
            "missing routing processor\n{yaml}"
        );
        assert!(
            yaml.contains("route() where metric.name == \"http_freshness_probe_warm\""),
            "routing must match on metric.name\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/warm_passthrough"),
            "warm_passthrough pipeline target must be referenced\n{yaml}"
        );

        // Both pipelines exist.
        assert!(
            yaml.contains("metrics/asap_tier:"),
            "missing metrics/asap_tier pipeline\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/warm_passthrough:"),
            "missing metrics/warm_passthrough pipeline\n{yaml}"
        );

        // Critical assertion: the warm_passthrough pipeline does NOT
        // reference the family-specific sketch processor — that's the
        // whole point of routing around DDSketch.
        let passthrough_idx = yaml
            .find("metrics/warm_passthrough:")
            .expect("warm_passthrough section not found");
        // Slice to the next pipeline (or end of file).
        let after = &yaml[passthrough_idx..];
        let next_pipeline_offset = after[1..]
            .find("metrics/")
            .map(|x| x + 1)
            .unwrap_or(after.len());
        let passthrough_section = &after[..next_pipeline_offset];
        assert!(
            !passthrough_section.contains("ddsketch"),
            "warm_passthrough pipeline must NOT include ddsketch (the bug we're fixing)\n{yaml}"
        );
        // ... but it SHOULD still include gorillas3 so the metric
        // lands in the archive (the warm engine queries it from
        // there).
        assert!(
            passthrough_section.contains("gorillas3"),
            "warm_passthrough pipeline still routes through gorillas3 for archive write\n{yaml}"
        );
    }

    /// Bug (b) corollary: warm_passthrough composes cleanly with the
    /// Phase ε.1 prometheus_archive routing — single routing processor
    /// with both an `asap.mode` and a `metric.name` table entry.
    #[test]
    fn phase_3_2_5_bug_b_warm_passthrough_composes_with_prometheus_archive() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_warm".to_string(),
            window_secs: Some(1),
        }];
        cfg.warm_passthrough_metrics = vec!["http_freshness_probe_warm".to_string()];
        cfg.prometheus_archive_metrics = vec![PrometheusArchiveMetric {
            metric: "http_requests_total".to_string(),
            window_secs: Some(60),
            label_proj: Vec::new(),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // OTTL form gives us a single routing processor that handles
        // both dispatch axes.
        assert!(
            yaml.contains("route() where metric.name"),
            "must dispatch by metric name (warm_passthrough)\n{yaml}"
        );
        assert!(
            yaml.contains("attributes[\\\"asap.mode\\\"]")
                || yaml.contains("attributes['asap.mode']")
                || yaml.contains("attributes[\"asap.mode\"]"),
            "must dispatch by asap.mode (prometheus_archive)\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/prometheus_archive:"),
            "prometheus_archive pipeline still emitted\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/warm_passthrough:"),
            "warm_passthrough pipeline still emitted\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/asap_tier:"),
            "asap_tier (default) pipeline still emitted\n{yaml}"
        );
    }

    // ── MVP §46: 5-sketch routing-connector edge YAML emit tests ──────────
    //
    // The new emit path activates when `cfg.metric_to_family` is
    // non-empty. These tests pin:
    //   * All 5 sketch processors in `processors:` regardless of which
    //     metrics route to them (runtime swap → zero pipeline graph
    //     change).
    //   * `routing` in `connectors:` (NOT `processors:`) — the real
    //     bugfix; `routingprocessor` was removed in OTel-collector
    //     v0.106 so emitting it would fail agent boot.
    //   * All 6 named pipelines: entry `metrics:` + 5 per-family
    //     paths + `metrics/raw_passthrough` default.
    //   * Each per-sketch pipeline starts with `gorillas3` when an
    //     archive tier is declared (cold-tier write happens BEFORE
    //     sketch mutation).
    //   * Freshness-probe (warm-passthrough) routing folds into
    //     `metrics/raw_passthrough` so the metric name is preserved
    //     end-to-end.

    /// Helper: build a 5-metric `EdgeStageConfig` covering every sketch
    /// family per the canonical workload-spec table in MVP §46.
    fn five_sketch_edge_cfg() -> EdgeStageConfig {
        let mut metric_to_family: HashMap<String, SketchKind> = HashMap::new();
        metric_to_family.insert(
            "http_requests_total_latency_ms".into(),
            SketchKind::DDSketch,
        );
        metric_to_family.insert("request_size_bytes".into(), SketchKind::Kll);
        metric_to_family.insert("unique_users_per_min".into(), SketchKind::Hll);
        metric_to_family.insert("top_endpoint_qps".into(), SketchKind::CountSketch);
        metric_to_family.insert("endpoint_request_freq".into(), SketchKind::Cms);
        // `http_requests_total` is intentionally NOT in this map — it
        // falls through to the `metrics/raw_passthrough` default.
        EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
        }
    }

    #[test]
    fn mvp46_emit_loads_all_5_sketch_processors() {
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        for proc in ["ddsketch", "KLL", "HLL", "countsketch", "countmin"] {
            assert!(
                yaml.contains(&format!("{proc}:")),
                "missing top-level processor key {proc}\n{yaml}"
            );
        }
    }

    #[test]
    fn mvp46_routing_lives_in_connectors_not_processors() {
        // The real bugfix: OTel collector v0.106+ removed
        // `routingprocessor`; the routing component is now a
        // `routingconnector`. We MUST emit it under `connectors:`.
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // Connectors block exists with a `routing:` entry.
        assert!(
            yaml.contains("connectors:"),
            "missing top-level connectors block\n{yaml}"
        );
        let connectors_idx = yaml.find("connectors:").expect("connectors:");
        let after_conn = &yaml[connectors_idx..];
        // Find the next top-level section (one of receivers, processors,
        // exporters, service, extensions) — `routing:` must appear before
        // it.
        let routing_idx = after_conn
            .find("routing:")
            .expect("routing: not found after connectors:");
        // Heuristically check that `routing:` appears in the connectors
        // block, not later under `service.pipelines` (where it'd appear
        // as `- routing` not `routing:`).
        let next_section = ["exporters:", "service:"]
            .iter()
            .filter_map(|s| after_conn.find(s))
            .min()
            .unwrap_or(after_conn.len());
        assert!(
            routing_idx < next_section,
            "routing: must appear inside connectors block, not later\n{yaml}"
        );

        // Critical negative assertion: `routing` is NOT under
        // `processors:`. The processors block lists only the sketch
        // processors + gorillas3? + batch.
        let processors_idx = yaml.find("processors:").expect("processors:");
        let proc_end = yaml[processors_idx..]
            .find("\nconnectors:")
            .or_else(|| yaml[processors_idx..].find("\nexporters:"))
            .map(|x| processors_idx + x)
            .unwrap_or(yaml.len());
        let processors_section = &yaml[processors_idx..proc_end];
        assert!(
            !processors_section.contains("routing:"),
            "routing must NOT live under processors: (the v0.106 bug we're fixing)\n{processors_section}"
        );
    }

    #[test]
    fn mvp46_emits_all_6_named_pipelines() {
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        for pl in [
            // Entry pipeline.
            "metrics:",
            // Default raw-passthrough.
            "metrics/raw_passthrough:",
            // 5 per-family pipelines.
            "metrics/ddsketch_path:",
            "metrics/kll_path:",
            "metrics/hll_path:",
            "metrics/countsketch_path:",
            "metrics/countminsketch_path:",
        ] {
            assert!(yaml.contains(pl), "missing pipeline entry {pl}\n{yaml}");
        }
    }

    #[test]
    fn mvp46_entry_pipeline_routes_to_connector_not_processor() {
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        // Find the entry `metrics:` pipeline section (under
        // service.pipelines) and verify it has `exporters: [routing]`
        // and no processors list (or empty).
        let pipelines_idx = yaml.find("pipelines:").expect("pipelines block");
        let after = &yaml[pipelines_idx..];
        // First `metrics:` (NOT `metrics/...`) section is the entry.
        // Look for "    metrics:\n" pattern.
        let entry_marker = "    metrics:\n";
        let entry_idx = after.find(entry_marker).expect("metrics: entry");
        let entry_section_end = after[entry_idx + entry_marker.len()..]
            .find("    metrics/")
            .map(|x| entry_idx + entry_marker.len() + x)
            .unwrap_or(after.len());
        let entry_section = &after[entry_idx..entry_section_end];
        // `exporters: [routing]` — but serde_yaml may render the list
        // long-form; tolerate both `- routing` and `[routing]`.
        assert!(
            entry_section.contains("- routing") || entry_section.contains("[routing]"),
            "entry pipeline must export to the routing connector\n{entry_section}"
        );
    }

    #[test]
    fn mvp46_per_sketch_pipelines_have_gorillas3_first_when_archive_declared() {
        let mut cfg = five_sketch_edge_cfg();
        // Declare an archive-tier metric so gorillas3 is emitted.
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total_latency_ms".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // gorillas3 processor block present.
        assert!(
            yaml.contains("gorillas3:"),
            "missing gorillas3 block\n{yaml}"
        );
        assert!(yaml.contains("block_format: prometheus_tsdb"), "{yaml}");

        // Each per-sketch pipeline starts with gorillas3 BEFORE the
        // family processor. We slice the YAML per-pipeline section and
        // check the relative order.
        for (pipeline, family_proc) in [
            ("metrics/ddsketch_path:", "ddsketch"),
            ("metrics/kll_path:", "KLL"),
            ("metrics/hll_path:", "HLL"),
            ("metrics/countsketch_path:", "countsketch"),
            ("metrics/countminsketch_path:", "countmin"),
        ] {
            let p_idx = yaml.find(pipeline).expect(pipeline);
            // Section runs to the next `metrics/` header or end.
            let after = &yaml[p_idx..];
            let next_offset = after[1..]
                .find("    metrics")
                .map(|x| x + 1)
                .unwrap_or(after.len());
            let section = &after[..next_offset];
            let g_idx = section
                .find("- gorillas3")
                .unwrap_or_else(|| panic!("gorillas3 missing in {pipeline}\n{section}"));
            let f_idx = section
                .find(&format!("- {family_proc}"))
                .unwrap_or_else(|| panic!("{family_proc} missing in {pipeline}\n{section}"));
            assert!(
                g_idx < f_idx,
                "gorillas3 must come BEFORE {family_proc} in {pipeline}\n{section}"
            );
        }
    }

    #[test]
    fn mvp46_per_sketch_pipelines_have_memory_limiter_first() {
        // Follow-up to PR #355: every per-sketch pipeline (and the
        // default raw_passthrough) MUST list `memory_limiter` as the
        // FIRST processor so backpressure refuses incoming batches
        // BEFORE gorillas3 buffers them — the previous shape OOM-killed
        // the agent at ~3 min under sustained load.
        let mut cfg = five_sketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total_latency_ms".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        // memory_limiter processor block present with the chosen
        // threshold (1280 MiB ≈ 80 % of agent's 1536 MiB cgroup).
        assert!(
            yaml.contains("memory_limiter:"),
            "missing top-level memory_limiter processor block\n{yaml}"
        );
        assert!(
            yaml.contains("limit_mib: 1280"),
            "memory_limiter must pin limit_mib: 1280 (agent cgroup is 1536 MiB)\n{yaml}"
        );
        assert!(
            yaml.contains("spike_limit_mib: 256"),
            "memory_limiter must pin spike_limit_mib: 256\n{yaml}"
        );

        // Each per-sketch pipeline (and raw_passthrough) lists
        // memory_limiter as the FIRST processor — slice each section
        // and assert relative ordering.
        for (pipeline, family_proc) in [
            ("metrics/raw_passthrough:", "gorillas3"),
            ("metrics/ddsketch_path:", "gorillas3"),
            ("metrics/kll_path:", "gorillas3"),
            ("metrics/hll_path:", "gorillas3"),
            ("metrics/countsketch_path:", "gorillas3"),
            ("metrics/countminsketch_path:", "gorillas3"),
        ] {
            let p_idx = yaml.find(pipeline).expect(pipeline);
            let after = &yaml[p_idx..];
            let next_offset = after[1..]
                .find("    metrics")
                .map(|x| x + 1)
                .unwrap_or(after.len());
            let section = &after[..next_offset];
            let m_idx = section
                .find("- memory_limiter")
                .unwrap_or_else(|| panic!("memory_limiter missing in {pipeline}\n{section}"));
            let f_idx = section
                .find(&format!("- {family_proc}"))
                .unwrap_or_else(|| panic!("{family_proc} missing in {pipeline}\n{section}"));
            assert!(
                m_idx < f_idx,
                "memory_limiter must come BEFORE {family_proc} in {pipeline}\n{section}"
            );
        }
    }

    #[test]
    fn mvp46_routing_table_dispatches_per_metric_to_correct_family() {
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        // Every metric in the contract dispatches via routingconnector OTTL
        // conditions.
        // to its family pipeline. serde_yaml may render sequences
        // either inline (`[metrics/x]`) or block-form (`- metrics/x`)
        // depending on width; tolerate both.
        for (metric, pipeline) in [
            ("http_requests_total_latency_ms", "metrics/ddsketch_path"),
            ("request_size_bytes", "metrics/kll_path"),
            ("unique_users_per_min", "metrics/hll_path"),
            ("top_endpoint_qps", "metrics/countsketch_path"),
            ("endpoint_request_freq", "metrics/countminsketch_path"),
        ] {
            let needle = format!("name == \"{metric}\"");
            let n_idx = yaml
                .find(&needle)
                .unwrap_or_else(|| panic!("missing routing condition for {metric}\n{yaml}"));
            let near = &yaml[n_idx..n_idx.saturating_add(256).min(yaml.len())];
            let inline = format!("[{pipeline}]");
            let block = format!("- {pipeline}");
            assert!(
                near.contains(&inline) || near.contains(&block),
                "{metric} should route to {pipeline}; got\n{near}"
            );
        }
    }

    #[test]
    fn mvp46_default_pipeline_is_raw_passthrough() {
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        // Tolerate inline-vs-block list rendering — serde_yaml chooses
        // based on width.
        let inline = "default_pipelines: [metrics/raw_passthrough]";
        let block = "default_pipelines:\n    - metrics/raw_passthrough";
        let block2 = "default_pipelines:\n      - metrics/raw_passthrough";
        assert!(
            yaml.contains(inline) || yaml.contains(block) || yaml.contains(block2),
            "routing must default to raw_passthrough so http_requests_total\
             (and any unrouted metric) falls through without sketching\n{yaml}"
        );
    }

    #[test]
    fn mvp46_warm_passthrough_routes_to_raw_passthrough_pipeline() {
        // Freshness probes (Phase 3.2.5 Bug b) must bypass every sketch
        // processor — they route to `metrics/raw_passthrough` so the
        // metric name is preserved end-to-end.
        let mut cfg = five_sketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_warm".into(),
            window_secs: Some(1),
        }];
        cfg.warm_passthrough_metrics = vec!["http_freshness_probe_warm".into()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        let needle = "name == \"http_freshness_probe_warm\"";
        let idx = yaml
            .find(needle)
            .unwrap_or_else(|| panic!("missing freshness-probe route\n{yaml}"));
        let near = &yaml[idx..idx.saturating_add(256).min(yaml.len())];
        // serde_yaml renders sequences inline or block-form; tolerate both.
        assert!(
            near.contains("[metrics/raw_passthrough]")
                || near.contains("- metrics/raw_passthrough"),
            "warm_passthrough metric must route to raw_passthrough\n{near}"
        );

        // raw_passthrough pipeline must NOT include any family-specific
        // sketch processor (the whole point of the bypass).
        let pl_idx = yaml
            .find("metrics/raw_passthrough:")
            .expect("raw_passthrough pipeline");
        let after = &yaml[pl_idx..];
        let next_offset = after[1..]
            .find("    metrics")
            .map(|x| x + 1)
            .unwrap_or(after.len());
        let section = &after[..next_offset];
        for forbidden in ["ddsketch", "KLL", "HLL", "countsketch", "countmin"] {
            assert!(
                !section.contains(forbidden),
                "raw_passthrough must NOT include {forbidden}\n{section}"
            );
        }
        // ... but gorillas3 still runs (the metric still wants to land
        // in the cold archive).
        assert!(
            section.contains("- gorillas3"),
            "raw_passthrough still routes through gorillas3 for archive write\n{section}"
        );
    }

    #[test]
    fn mvp46_per_sketch_pipelines_use_routing_as_receiver() {
        // The connector is referenced as both an exporter (entry
        // pipeline) and a receiver (each per-family pipeline). This
        // pins the receiver-side wiring.
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        for pipeline in [
            "metrics/ddsketch_path:",
            "metrics/kll_path:",
            "metrics/hll_path:",
            "metrics/countsketch_path:",
            "metrics/countminsketch_path:",
            "metrics/raw_passthrough:",
        ] {
            let p_idx = yaml.find(pipeline).expect(pipeline);
            let after = &yaml[p_idx..];
            let next_offset = after[1..]
                .find("    metrics")
                .map(|x| x + 1)
                .unwrap_or(after.len());
            let section = &after[..next_offset];
            assert!(
                section.contains("- routing") || section.contains("[routing]"),
                "{pipeline} must consume from the routing connector\n{section}"
            );
        }
    }

    #[test]
    fn mvp46_empty_metric_to_family_falls_back_to_legacy_emit() {
        // Backward-compat invariant: when the planner hasn't populated
        // metric_to_family, the emitter must produce the legacy
        // single-pipeline shape (no connectors block, no per-family
        // pipelines).
        let cfg = ddsketch_edge_cfg();
        assert!(
            cfg.metric_to_family.is_empty(),
            "ddsketch_edge_cfg fixture must keep metric_to_family empty"
        );
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        // No connectors block.
        assert!(
            !yaml.contains("connectors:"),
            "legacy emit must NOT add connectors block\n{yaml}"
        );
        // No 5-sketch pipelines.
        assert!(
            !yaml.contains("metrics/ddsketch_path"),
            "legacy emit keeps single-pipeline shape\n{yaml}"
        );
        assert!(
            !yaml.contains("metrics/raw_passthrough"),
            "legacy emit keeps single-pipeline shape\n{yaml}"
        );
    }

    #[test]
    fn mvp46_composes_with_prometheus_archive_mode3() {
        // Mode 3 (Prometheus archive) folds into the same routing
        // connector table — the `metrics/prometheus_archive` pipeline
        // is added as an additional fan-out target.
        let mut cfg = five_sketch_edge_cfg();
        cfg.prometheus_archive_metrics = vec![PrometheusArchiveMetric {
            metric: "http_requests_total".into(),
            window_secs: Some(60),
            label_proj: vec!["service.name".into()],
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");

        assert!(
            yaml.contains("metrics/prometheus_archive:"),
            "Mode-3 pipeline must be added\n{yaml}"
        );
        assert!(
            yaml.contains("otlphttp/prometheus:"),
            "Mode-3 exporter must be added\n{yaml}"
        );
        assert!(
            yaml.contains("attributes[\\\"asap.mode\\\"]")
                || yaml.contains("attributes['asap.mode']")
                || yaml.contains("attributes[\"asap.mode\"]"),
            "routing table must dispatch by asap.mode for Mode 3\n{yaml}"
        );
    }
}
