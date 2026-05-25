//! Phase ε.1.5 — OTAP Dataflow DAG YAML emitter (per-runtime mirror of
//! [`super::stage_config::emit_edge_yaml`]).
//!
//! `asap-otap` uses the otap-dataflow Rust runtime; its config surface is
//! a DAG YAML where `nodes.<name>.type` is a registered plugin URN
//! (e.g. `receiver:otlp`, `exporter:otlp_http`,
//! `urn:otel:exporter:otlp_http`). The OTLP HTTP exporter ships in
//! `otel-arrow/rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_http_exporter/`
//! and registers under the URN `urn:otel:exporter:otlp_http` via
//! `linkme`'s `distributed_slice(OTAP_EXPORTER_FACTORIES)`.
//!
//! Three modes (mirror the [`crate::planner::wire_cost::BindMode`] enum
//! Phase ε.1 introduced):
//!
//! 1. `SketchAtEdge` — DAG includes a sketch processor node between the
//!    OTLP receiver and the OTLP gRPC exporter to the gateway. (The
//!    `asap_sketches` plugin lives in the otap-patch tree; we wire its
//!    type URN here without depending on its source.)
//! 2. `RawAtEdgeSketchAtBackend` — passthrough DAG: receiver → exporter.
//!    No sketch processor. Egress is OTLP gRPC to the gateway, which
//!    builds sketches at backend ingest.
//! 3. `RawAtEdgePrometheusArchive` — passthrough DAG: receiver → OTLP
//!    HTTP exporter pointed at Prometheus's native OTLP receiver
//!    (`/api/v1/otlp/v1/metrics`).
//!
//! The function consumes the same [`EdgeStageConfig`] the OTel-collector
//! emitter does — Phase ε.1.5 keeps the typed L5 plan as the single
//! source of truth across all three runtimes. The emitter dispatches per
//! `EdgeStageConfig` via the same `prometheus_archive_metrics` /
//! `sketch_processors` signals the OTel-collector emitter uses (Mode 3
//! ↔ `prometheus_archive_metrics` non-empty; Mode 1 ↔ `sketch_processors`
//! non-empty; Mode 2 ↔ both empty + the bind decision lives in the
//! upstream stage_split — the edge YAML for Mode 2 is identical to a
//! plain raw passthrough at this layer).

use anyhow::{Context, Result};
use serde::Serialize;
use serde_yaml::{Mapping, Value};
use std::collections::BTreeMap;

use crate::physical::colored_dag::emitter::{EdgeSketchProcessor, EdgeStageConfig, ExportTarget};
use crate::physical::colored_dag::stage_id::StageId;
use crate::sketch_algebra::params::{SketchKind, SketchParams};

/// Default URL for Prometheus's native OTLP HTTP receiver.
/// Matches `super::stage_config::emit_edge_yaml`'s placeholder so the
/// three runtime emitters agree on the wire endpoint.
pub const DEFAULT_PROMETHEUS_OTLP_URL: &str = "http://prometheus:9090/api/v1/otlp/v1/metrics";

/// URN of the OTLP HTTP exporter registered by
/// `otel-arrow/rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_http_exporter/`.
const URN_OTLP_HTTP_EXPORTER: &str = "exporter:otlp_http";

/// URN of the OTLP gRPC exporter registered by
/// `otel-arrow/rust/otap-dataflow/crates/core-nodes/src/exporters/otlp_grpc_exporter/`.
const URN_OTLP_GRPC_EXPORTER: &str = "exporter:otlp_grpc";

/// URN of the OTLP receiver (gRPC + HTTP).
const URN_OTLP_RECEIVER: &str = "receiver:otlp";

/// URN of the asap_sketches processor registered in the otap-patch tree.
/// Phase ε.1.5 wires the URN abstractly; the binary side lands the
/// plugin in `otap-patch/plugins/asap_sketches/` per
/// [`docs/design-asap-otap-rust-integration.md`].
const URN_ASAP_SKETCHES_PROCESSOR: &str = "processor:asap_sketches";

// ── DAG YAML structural types ────────────────────────────────────────────────
//
// These mirror the otap-dataflow `engine`/`groups`/`pipelines`/`nodes`
// schema in `otel-arrow/rust/otap-dataflow/configs/*.yaml`. We keep a
// minimal set to round-trip; the full schema (channel_capacity policies,
// engine settings, etc.) is left at struct defaults — Phase ε.1.5 only
// commits the wiring shape, not policy.

#[derive(Serialize)]
struct OtapDag {
    version: String,
    engine: BTreeMap<String, Value>,
    groups: BTreeMap<String, Group>,
}

#[derive(Serialize)]
struct Group {
    pipelines: BTreeMap<String, PipelineDef>,
}

#[derive(Serialize)]
struct PipelineDef {
    nodes: BTreeMap<String, NodeDef>,
    connections: Vec<Connection>,
}

#[derive(Serialize)]
struct NodeDef {
    #[serde(rename = "type")]
    kind: String,
    config: Value,
}

#[derive(Serialize)]
struct Connection {
    from: String,
    to: String,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Build the OTAP-Dataflow DAG YAML for the `asap-otap` runtime from a
/// typed L5 [`EdgeStageConfig`].
///
/// `opamp_endpoint` is the controller's WebSocket URL; reserved for a
/// future `extension:opamp` node when the otap-dataflow runtime grows
/// OpAMP support (today the otap-dataflow `engine` block has no
/// extension model, so we accept the param for shape-parity with
/// [`super::stage_config::emit_edge_yaml`] and ignore it).
///
/// `prometheus_otlp_url` overrides the default Prometheus OTLP HTTP
/// endpoint when present (for Mode 3 metrics). `None` falls back to
/// [`DEFAULT_PROMETHEUS_OTLP_URL`].
pub fn emit_otap_dag_yaml(
    cfg: &EdgeStageConfig,
    _opamp_endpoint: &str,
    prometheus_otlp_url: Option<&str>,
) -> Result<String> {
    let mut nodes: BTreeMap<String, NodeDef> = BTreeMap::new();
    let mut connections: Vec<Connection> = Vec::new();

    // ── OTLP receiver ────────────────────────────────────────────────────────
    // Both gRPC + HTTP listeners — matches `emit_edge_yaml`'s shape so
    // the three runtimes accept the same upstream traffic.
    let receiver_cfg: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    listening_addr: \"0.0.0.0:4317\"\n  http:\n    listening_addr: \"0.0.0.0:4318\"\n",
    )
    .context("parse OTAP otlp receiver block")?;
    nodes.insert(
        "receiver".to_string(),
        NodeDef {
            kind: URN_OTLP_RECEIVER.to_string(),
            config: receiver_cfg,
        },
    );

    // ── Mode dispatch ────────────────────────────────────────────────────────
    let has_prometheus_archive = !cfg.prometheus_archive_metrics.is_empty();
    let has_sketch = !cfg.sketch_processors.is_empty();

    if has_prometheus_archive {
        // Mode 3 — Prometheus archive: passthrough → otlp_http exporter
        // pointed at Prometheus's native OTLP receiver. We do NOT also
        // emit a sketch node; Mode 3 metrics are the whole edge stream
        // for that pipeline. (When a single agent is hosting Mode 3 +
        // Mode 1/2 metrics simultaneously, the upstream typed splitter
        // produces two `EdgeStageConfig`s — one per mode bucket — and
        // we emit two pipelines side-by-side via the otap-dataflow
        // multi-pipeline `pipelines:` map. Phase ε.1.5 ships the
        // single-pipeline case; the multi-pipeline case is an upstream
        // splitter concern.)
        let prom_url = prometheus_otlp_url.unwrap_or(DEFAULT_PROMETHEUS_OTLP_URL);
        let exp_cfg = build_otlp_http_exporter_config(prom_url);
        nodes.insert(
            "exporter".to_string(),
            NodeDef {
                kind: URN_OTLP_HTTP_EXPORTER.to_string(),
                config: exp_cfg,
            },
        );
        connections.push(Connection {
            from: "receiver".to_string(),
            to: "exporter".to_string(),
        });
    } else if has_sketch {
        // Mode 1 — sketch at edge. Insert one processor per
        // `EdgeSketchProcessor`; chain them serially between receiver
        // and the gateway-bound OTLP gRPC exporter.
        let mut prev = "receiver".to_string();
        for (i, sp) in cfg.sketch_processors.iter().enumerate() {
            let name = format!("sketch_{i}");
            nodes.insert(
                name.clone(),
                NodeDef {
                    kind: URN_ASAP_SKETCHES_PROCESSOR.to_string(),
                    config: build_asap_sketches_config(sp, cfg.window_secs),
                },
            );
            connections.push(Connection {
                from: prev.clone(),
                to: name.clone(),
            });
            prev = name;
        }
        let endpoint = resolve_export_endpoint("data-plane", &cfg.exporter_target);
        nodes.insert(
            "exporter".to_string(),
            NodeDef {
                kind: URN_OTLP_GRPC_EXPORTER.to_string(),
                config: build_otlp_grpc_exporter_config(&endpoint),
            },
        );
        connections.push(Connection {
            from: prev,
            to: "exporter".to_string(),
        });
    } else {
        // Mode 2 — raw at edge → sketch at backend. Passthrough DAG.
        let endpoint = resolve_export_endpoint("data-plane", &cfg.exporter_target);
        nodes.insert(
            "exporter".to_string(),
            NodeDef {
                kind: URN_OTLP_GRPC_EXPORTER.to_string(),
                config: build_otlp_grpc_exporter_config(&endpoint),
            },
        );
        connections.push(Connection {
            from: "receiver".to_string(),
            to: "exporter".to_string(),
        });
    }

    let mut pipelines = BTreeMap::new();
    pipelines.insert("main".to_string(), PipelineDef { nodes, connections });

    let mut groups = BTreeMap::new();
    groups.insert("default".to_string(), Group { pipelines });

    let dag = OtapDag {
        version: "otel_dataflow/v1".to_string(),
        engine: BTreeMap::new(),
        groups,
    };

    serde_yaml::to_string(&dag).context("serialize OTAP DAG YAML")
}

// ── Internals ────────────────────────────────────────────────────────────────

fn resolve_export_endpoint(default_host: &str, target: &ExportTarget) -> String {
    match target {
        ExportTarget::Endpoint(s) => s.clone(),
        ExportTarget::Stage(StageId::Edge) => "edge:4317".to_string(),
        ExportTarget::Stage(StageId::Gateway) => format!("{default_host}:4317"),
        ExportTarget::Stage(StageId::Backend) => format!("{default_host}:4317"),
    }
}

/// Build the OTLP HTTP exporter `config:` block. Matches the
/// `crates/core-nodes/src/exporters/otlp_http_exporter/config.rs` schema
/// — `endpoint` (base URL) plus an explicit `metrics_endpoint` so the
/// Prometheus path `/api/v1/otlp/v1/metrics` round-trips verbatim.
fn build_otlp_http_exporter_config(metrics_url: &str) -> Value {
    // Derive the bare endpoint from the metrics URL: drop the path. For
    // typical inputs this is `http://prometheus:9090`.
    let base = match metrics_url.find("/api/") {
        Some(i) => &metrics_url[..i],
        None => metrics_url,
    };
    let yaml = format!(
        "endpoint: \"{base}\"\nmetrics_endpoint: \"{metrics_url}\"\nhttp:\n  request_timeout: \"30s\"\nclient_pool_size: 1\n",
    );
    serde_yaml::from_str(&yaml).expect("inline OTLP HTTP exporter config is valid YAML")
}

/// Build the OTLP gRPC exporter `config:` block. The otap-dataflow
/// `otlp_grpc` exporter uses `grpc_endpoint` as the field name (see
/// `configs/otlp-otlp.yaml`).
fn build_otlp_grpc_exporter_config(endpoint: &str) -> Value {
    let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };
    let yaml = format!("grpc_endpoint: \"{url}\"\ntimeout: \"15s\"\n");
    serde_yaml::from_str(&yaml).expect("inline OTLP gRPC exporter config is valid YAML")
}

/// Build the per-edge-processor `asap_sketches` config block. Mirrors
/// the same fields the OTel-collector emitter writes
/// (`super::stage_config::build_edge_processor_block`) so the binary
/// side can share a single schema across the OTel + OTAP runtimes.
fn build_asap_sketches_config(sp: &EdgeSketchProcessor, window_secs: Option<u64>) -> Value {
    let mut m = Mapping::new();
    if let Some(w) = window_secs {
        m.insert("mode".into(), Value::String("window".to_string()));
        m.insert("window_duration".into(), Value::String(format!("{w}s")));
    } else {
        m.insert("mode".into(), Value::String("batch".to_string()));
    }
    // PR 5 alignment (mirroring #244 / #246 / #250's wire cleanups):
    // `aggregation_id` was the controller-allocated string IDs the
    // patched asap-otel processors don't consume — sid identity is
    // content-addressed at the backend via `(metric, attrs_fingerprint,
    // agg_kind_canonical)`. The field stays on `EdgeSketchProcessor`
    // as internal emitter plumbing for cross-stage references during
    // the DAG walk; it just doesn't reach the wire here.
    m.insert(
        "sketch_kind".into(),
        Value::String(sketch_kind_tag(&sp.sketch_kind).into()),
    );
    match &sp.sketch_params {
        SketchParams::Kll(p) => {
            m.insert("k".into(), Value::Number((p.k as u64).into()));
        }
        SketchParams::DDSketch(p) => {
            m.insert("relative_accuracy".into(), Value::Number(p.alpha.into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::Hll(_p) => {
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::Cms(p) => {
            m.insert("rows".into(), Value::Number((p.d as u64).into()));
            m.insert("columns".into(), Value::Number((p.w as u64).into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::CountSketch(p) => {
            let epsilon = std::f64::consts::E / (p.w as f64);
            let delta = 2f64.powi(-(p.d as i32));
            m.insert("epsilon".into(), Value::Number(epsilon.into()));
            m.insert("delta".into(), Value::Number(delta.into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
    }
    Value::Mapping(m)
}

fn sketch_kind_tag(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::Kll => "kll",
        SketchKind::DDSketch => "ddsketch",
        SketchKind::Hll => "hll",
        SketchKind::Cms => "cms",
        SketchKind::CountSketch => "count_sketch",
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::colored_dag::emitter::{EdgeSketchProcessor, PrometheusArchiveMetric};
    use crate::sketch_algebra::params::DDSketchParams;

    /// Minimal struct-stub used to validate the emitted DAG parses as the
    /// otap-dataflow schema. We don't pull in the otap-df-config crate
    /// here (it would add an enormous dependency footprint to the
    /// controller); instead we verify the top-level shape (`version`,
    /// `groups`, `pipelines`, `nodes`, `connections`) round-trips.
    #[derive(Debug, serde::Deserialize)]
    struct OtapDagStub {
        version: String,
        #[allow(dead_code)]
        engine: serde_yaml::Value,
        groups: BTreeMap<String, GroupStub>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct GroupStub {
        pipelines: BTreeMap<String, PipelineStub>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct PipelineStub {
        nodes: BTreeMap<String, NodeStub>,
        connections: Vec<ConnectionStub>,
    }

    #[derive(Debug, serde::Deserialize)]
    struct NodeStub {
        #[serde(rename = "type")]
        kind: String,
        #[allow(dead_code)]
        config: serde_yaml::Value,
    }

    #[derive(Debug, serde::Deserialize)]
    struct ConnectionStub {
        from: String,
        to: String,
    }

    fn ddsketch_edge_cfg_mode1() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: Vec::new(),
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
            metric_to_family: std::collections::HashMap::new(),
            metric_to_grouping_labels: std::collections::HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: std::collections::HashMap::new(),
        }
    }

    fn raw_edge_cfg_mode2() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::new(),
            metric_to_grouping_labels: std::collections::HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: std::collections::HashMap::new(),
        }
    }

    fn prom_edge_cfg_mode3() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: vec![PrometheusArchiveMetric {
                metric: "http_request_duration_seconds".to_string(),
                window_secs: Some(60),
                label_proj: vec!["service.name".to_string()],
            }],
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::new(),
            metric_to_grouping_labels: std::collections::HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: std::collections::HashMap::new(),
        }
    }

    /// Mode 1 snapshot — sketch at edge: receiver → asap_sketches →
    /// otlp_grpc exporter to asapquery-backend.
    #[test]
    fn otap_dag_mode1_sketch_at_edge_shape() {
        let yaml = emit_otap_dag_yaml(&ddsketch_edge_cfg_mode1(), "ws://ctrl/v1/opamp", None)
            .expect("emit_otap_dag_yaml ok");
        let dag: OtapDagStub = serde_yaml::from_str(&yaml).expect("DAG parses");
        assert_eq!(dag.version, "otel_dataflow/v1");
        let pipe = dag
            .groups
            .get("default")
            .unwrap()
            .pipelines
            .get("main")
            .unwrap();
        // Receiver + sketch + exporter == 3 nodes.
        assert_eq!(pipe.nodes.len(), 3, "expected 3 nodes\n{yaml}");
        assert_eq!(pipe.nodes.get("receiver").unwrap().kind, URN_OTLP_RECEIVER);
        assert_eq!(
            pipe.nodes.get("sketch_0").unwrap().kind,
            URN_ASAP_SKETCHES_PROCESSOR
        );
        assert_eq!(
            pipe.nodes.get("exporter").unwrap().kind,
            URN_OTLP_GRPC_EXPORTER
        );
        // Connections: receiver → sketch_0 → exporter.
        assert_eq!(pipe.connections.len(), 2);
        assert_eq!(pipe.connections[0].from, "receiver");
        assert_eq!(pipe.connections[0].to, "sketch_0");
        assert_eq!(pipe.connections[1].from, "sketch_0");
        assert_eq!(pipe.connections[1].to, "exporter");
        // Endpoint contains data-plane:4317.
        assert!(
            yaml.contains("data-plane:4317"),
            "missing data-plane endpoint\n{yaml}"
        );
    }

    /// Mode 2 snapshot — raw at edge: receiver → otlp_grpc exporter.
    /// No sketch node; the gateway / backend will build sketches.
    #[test]
    fn otap_dag_mode2_raw_at_edge_shape() {
        let yaml = emit_otap_dag_yaml(&raw_edge_cfg_mode2(), "ws://ctrl/v1/opamp", None)
            .expect("emit_otap_dag_yaml ok");
        let dag: OtapDagStub = serde_yaml::from_str(&yaml).expect("DAG parses");
        let pipe = dag
            .groups
            .get("default")
            .unwrap()
            .pipelines
            .get("main")
            .unwrap();
        assert_eq!(
            pipe.nodes.len(),
            2,
            "expected receiver + exporter only\n{yaml}"
        );
        assert_eq!(pipe.nodes.get("receiver").unwrap().kind, URN_OTLP_RECEIVER);
        assert_eq!(
            pipe.nodes.get("exporter").unwrap().kind,
            URN_OTLP_GRPC_EXPORTER
        );
        // Direct connection.
        assert_eq!(pipe.connections.len(), 1);
        assert_eq!(pipe.connections[0].from, "receiver");
        assert_eq!(pipe.connections[0].to, "exporter");
        // No sketch processor in YAML.
        assert!(
            !yaml.contains(URN_ASAP_SKETCHES_PROCESSOR),
            "Mode 2 must not include a sketch processor\n{yaml}"
        );
    }

    /// Mode 3 snapshot — Prometheus archive: receiver → otlp_http
    /// exporter pointed at `/api/v1/otlp/v1/metrics`.
    #[test]
    fn otap_dag_mode3_prometheus_archive_shape() {
        let yaml = emit_otap_dag_yaml(&prom_edge_cfg_mode3(), "ws://ctrl/v1/opamp", None)
            .expect("emit_otap_dag_yaml ok");
        let dag: OtapDagStub = serde_yaml::from_str(&yaml).expect("DAG parses");
        let pipe = dag
            .groups
            .get("default")
            .unwrap()
            .pipelines
            .get("main")
            .unwrap();
        assert_eq!(pipe.nodes.len(), 2);
        assert_eq!(
            pipe.nodes.get("exporter").unwrap().kind,
            URN_OTLP_HTTP_EXPORTER
        );
        // Path round-trips verbatim.
        assert!(
            yaml.contains("/api/v1/otlp/v1/metrics"),
            "missing Prometheus OTLP path\n{yaml}"
        );
        // No sketch processor.
        assert!(
            !yaml.contains(URN_ASAP_SKETCHES_PROCESSOR),
            "Mode 3 must not include a sketch processor\n{yaml}"
        );
    }

    /// Mode 3 with override URL — caller can redirect to a non-default
    /// Prometheus instance (`https://prom-prod:9090/...`).
    #[test]
    fn otap_dag_mode3_url_override() {
        let yaml = emit_otap_dag_yaml(
            &prom_edge_cfg_mode3(),
            "ws://ctrl/v1/opamp",
            Some("https://prom-prod:9090/api/v1/otlp/v1/metrics"),
        )
        .expect("emit_otap_dag_yaml ok");
        assert!(
            yaml.contains("https://prom-prod:9090/api/v1/otlp/v1/metrics"),
            "override URL not propagated\n{yaml}"
        );
        // The base endpoint should drop the path. (serde_yaml elides
        // quotes around scalar strings that don't need them, so we
        // match the unquoted form.)
        assert!(
            yaml.contains("endpoint: https://prom-prod:9090\n"),
            "base endpoint not derived\n{yaml}"
        );
    }
}
