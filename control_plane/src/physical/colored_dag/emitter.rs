//! L5 emitter — turns a [`ColoredDag`] into per-stage configs.
//!
//! Per `control_plane/docs/design.md` §1219: "L4 chose the sketch family +
//! params. L5 colors the DAG by `StageId` and emits per-executor
//! configs. Same `PhysicalExpr` input; topology and emitter differ per
//! deployment model."
//!
//! Phase E ships [`ThreeStageEmitter`] for the DC topology (edge →
//! gateway → backend). Each per-stage [`StageConfig`] is a structured
//! description that the OpAMP push (Phase G+) and the backend client
//! (Phase G+) materialise into wire bytes:
//!
//! - [`StageConfig::Edge`] — the agent OpAMP YAML's logical content:
//!   scrape source, optional window, the chosen sketch processor, and
//!   the OTLP exporter pointer to gateway.
//! - [`StageConfig::Gateway`] — the gateway OpAMP YAML's logical
//!   content: an OTLP receiver, the sketch-merge processor list, and
//!   the OTLP exporter pointer to backend.
//! - [`StageConfig::Backend`] — the backend `StreamingConfig`'s logical
//!   content: a list of `(aggregation_id → (sketch_kind, params))`
//!   tuples plus the readout query catalog.
//!
//! Emitters do NOT push to executors here — they produce the structured
//! output. The actual push (`crate::opamp::OpampServer::push_to_role`,
//! `crate::backend_client::StreamingConfigClient::post`) is wired in
//! Phase G+.

#![allow(dead_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::physical::colored_dag::dag::ColoredDag;
use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::sketch_algebra::params::{SketchKind, SketchParams};
use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};

/// Errors surfaced by [`Emitter::emit_per_stage`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EmitError {
    /// Topology shape isn't supported by this emitter — see
    /// `control_plane/docs/design.md` §6 for the per-deployment-model
    /// emitter list.
    #[error("unsupported topology for this emitter: {0:?} (expected {1:?})")]
    UnsupportedTopology(Topology, Topology),
    /// A sketch processor name could not be derived for the supplied
    /// `SketchKind`. Should not occur with the catalog ranges shipped
    /// in Phase C — kept as a defensive error for future kinds.
    #[error("no edge processor known for sketch kind {0:?}")]
    NoEdgeProcessor(SketchKind),
    /// Backend would emit an empty StreamingConfig because no sketch
    /// state ever reaches it (e.g. a colouring with only `Logical`
    /// nodes). Surfaced as a clean error so callers can fall back to
    /// the legacy planner output rather than POST an empty payload.
    #[error("backend has no sketch consumers; nothing to wire")]
    BackendEmpty,
}

/// Generic emitter trait — Phase E ships only [`ThreeStageEmitter`]; future
/// phases add `SingleStageEmitter` (asap-query), `ZeroStageEmitter`
/// (asap-fusion), and friends. The trait keeps the dispatch surface
/// uniform so `planner::stage_split` can pick at runtime.
pub trait Emitter {
    /// Lower a colored DAG into one [`StageConfig`] per occupied stage.
    /// Returns a map keyed by `StageId` for stable consumer access; any
    /// stage not occupied in the DAG is omitted.
    fn emit_per_stage(&self, dag: &ColoredDag) -> Result<HashMap<StageId, StageConfig>, EmitError>;
}

/// Per-stage emitter output for the DC three-stage topology.
///
/// The variants are deliberately struct-shaped (named fields) so future
/// downstream consumers can pattern-match without relying on tuple-index
/// stability.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum StageConfig {
    /// Edge agent's logical config — what the OpAMP push for this
    /// agent will need to materialise into OTel collector YAML.
    Edge(EdgeStageConfig),
    /// Gateway aggregator's logical config — receivers, sketch merge,
    /// onward exporter.
    Gateway(GatewayStageConfig),
    /// Backend `StreamingConfig` logical content — the
    /// (aggregation_id, sketch_type, params) bindings the backend's
    /// `OtlpReceiver` + readout catalog need.
    Backend(BackendStageConfig),
}

impl StageConfig {
    /// Stage this config corresponds to (mirror of the variant tag).
    pub fn stage(&self) -> StageId {
        match self {
            StageConfig::Edge(_) => StageId::Edge,
            StageConfig::Gateway(_) => StageId::Gateway,
            StageConfig::Backend(_) => StageId::Backend,
        }
    }
}

/// Logical content of an edge agent's per-stage config.
///
/// Mirrors the surface of `crate::types::AgentCollectorConfig` minus the
/// wire-format details (delta encoding, series-id TTL, sink addressing)
/// — those are emitter-side decisions Phase G+ owns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeStageConfig {
    /// Source metric name (from the L3 `Scan{Source::TimeSeries}`
    /// node). `None` only for synthetic colourings used in tests.
    pub source_metric: Option<String>,
    /// Equality label filters from `Scan` (`{service="api"}` etc.).
    /// Carried as `(label, equals)` tuples — the emitter's wire layer
    /// converts them to OTel YAML `attributes/include` matchers.
    pub label_filters: Vec<(String, String)>,
    /// Window size in seconds, when a `Window` node landed on edge.
    pub window_secs: Option<u64>,
    /// Sketch processor list — one per `SketchAgg` rooted at edge.
    /// For the canonical KLL quantile DAG this is exactly one entry.
    pub sketch_processors: Vec<EdgeSketchProcessor>,
    /// OTLP exporter target — the gateway endpoint. Phase E does not
    /// resolve a concrete address (no `DeploymentConstraints` plumbed
    /// in); emitters produce the abstract `Self` and downstream code
    /// fills in `gateway:4317` / similar.
    pub exporter_target: ExportTarget,
    /// Phase ε.1 — Mode 3 routing destinations, when one or more
    /// `RawAtEdgePrometheusArchive` nodes coloured to this edge stage.
    /// Each entry produces a separate `otlphttp/prometheus` exporter +
    /// pipeline tagged `asap.mode=prometheus_archive` so the agent's
    /// routing processor dispatches per-metric.
    ///
    /// Empty list = no Mode 3 metrics → no `otlphttp/prometheus`
    /// exporter is emitted (the YAML is identical to Phase β).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prometheus_archive_metrics: Vec<PrometheusArchiveMetric>,
    /// Phase 3.2.5 — archive-tier metrics that should flow through the
    /// `gorillas3` processor at the edge agent (write a Gorilla-S3
    /// chunk + Prometheus TSDB block to MinIO so the ASAP-tier query
    /// engine and the Thanos store-gateway can both serve them).
    ///
    /// Empty list = no archive-tier metrics → no `gorillas3` processor
    /// block in the emitted YAML (matches pre-Phase 3.2.5 behaviour
    /// for plans that route nothing to the archive).
    ///
    /// Populated by [`ThreeStageEmitter`] from any `Logical(Scan)` /
    /// `RawAtEdgePrometheusArchive` node whose metric is on the
    /// archive list (e.g. the freshness probes), and from explicit
    /// out-of-DAG opt-ins by callers that don't go through stage-split
    /// (the freshness probe path is the canonical example: it doesn't
    /// drop a `PhysicalExpr` node, but the agent still has to land its
    /// counter samples in MinIO so the Gorilla-S3 / Thanos archive
    /// can answer `last_over_time(...)`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub archive_tier_metrics: Vec<ArchiveTierMetric>,
    /// Phase 3.2.5 — metrics that must be carried through the
    /// ASAP-tier pipeline WITHOUT the family-specific sketch processor
    /// renaming them. The freshness probes are timestamp counters by
    /// design (the wire value `unix_ts_ms_of_emission` IS the freshness
    /// signal); the DDSketch processor's `_quantile` suffix would
    /// rename `http_freshness_probe_warm` to
    /// `http_freshness_probe_warm_quantile` and break the replay
    /// client's `last_over_time(http_freshness_probe_warm[10s])` query.
    ///
    /// When non-empty the L5 emitter adds a `routing` processor that
    /// dispatches by `metric.name`: matching metrics route to a
    /// `metrics/warm_passthrough` pipeline (gorillas3 if archive is
    /// declared, then exporter — NO sketch processor); everything
    /// else takes the existing `metrics/asap_tier` pipeline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warm_passthrough_metrics: Vec<String>,
    /// MVP §46 — per-metric → sketch-family mapping populated by the
    /// planner from the workload spec. When non-empty, the L5 edge
    /// emitter switches to the **5-sketch routing-connector** wire
    /// shape: it loads all referenced sketch processors and uses the
    /// OTel `routing` *connector* (NOT the deprecated routing
    /// processor) to dispatch each metric to the right per-family
    /// pipeline. Metrics absent from this map fall through to the
    /// `metrics/raw_passthrough` default pipeline.
    ///
    /// The field is named `metric_to_family` and the value type is
    /// [`SketchKind`] — agreed convention with the planner agent
    /// shipping in parallel (`SketchFamily` is a control-plane-side
    /// alias for `SketchKind` per `sketch_algebra::params`). Empty
    /// map ⇒ legacy single-pipeline / Mode-3 / warm-passthrough wire
    /// shapes are emitted unchanged (backward-compat).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_family: HashMap<String, SketchKind>,
}

/// Phase 3.2.5 — one archive-tier metric the agent should land in
/// MinIO via the `gorillas3` processor (Gorilla-S3 chunks + Prometheus
/// TSDB blocks for the Thanos store-gateway). The `metric` field is
/// used both for the control-plane-side bookkeeping and (downstream) for
/// the gorillas3 processor's per-metric prefix template — but the
/// processor today flushes every series it sees, so the field is
/// informational at the YAML layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchiveTierMetric {
    /// Metric name as it appears at the edge.
    pub metric: String,
    /// Optional flush window in seconds. Mirrors the planner's
    /// `gorilla_window_secs` (see `mvp-freshness-probes.yaml`); the
    /// L5 emitter uses the smallest non-None entry to size the
    /// `gorillas3.window_interval` knob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_secs: Option<u64>,
}

/// Phase ε.1 — one Mode-3 metric the agent forwards to Prometheus's
/// native OTLP receiver. The agent's `routing` processor matches on
/// `attributes["asap.mode"] == "prometheus_archive"` and dispatches to
/// the `otlphttp/prometheus` exporter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrometheusArchiveMetric {
    /// Metric name as it appears at the edge.
    pub metric: String,
    /// Optional window — informational; Prometheus stores raw samples
    /// regardless. The L5 emitter uses this to pick a scrape interval
    /// consistent with the planner's intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_secs: Option<u64>,
    /// Resource-attribute label projection — labels Prometheus's
    /// `otlp.promote_resource_attributes` will promote. Defaults to
    /// `["service.name", "service.namespace", "service.instance.id"]`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub label_proj: Vec<String>,
}

/// One sketch processor configured at an edge agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSketchProcessor {
    /// OTel processor component id — `KLL`, `ddsketch`, `HLL`,
    /// `countmin`, etc. Maps 1:1 from `SketchKind`.
    pub processor_name: String,
    /// Sketch family (mirror of the `SketchAgg::sketch_type` field).
    pub sketch_kind: SketchKind,
    /// Sketch parameters (mirror of the `SketchAgg::params` field).
    pub sketch_params: SketchParams,
    /// Stable `aggregation_id` the backend uses to look up the
    /// `(sketch_kind, params)` pair when receiving the corresponding
    /// OTLP stream. Phase E derives a deterministic id from the
    /// processor name + a position counter; downstream callers may
    /// override.
    pub aggregation_id: String,
}

/// Logical content of a gateway aggregator's per-stage config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayStageConfig {
    /// OTLP receiver port — Phase E surfaces the abstract `Default`
    /// (`4317`); deployment-specific overrides happen at Phase G.
    pub otlp_receiver_port: u16,
    /// One merge processor per `SketchMerge` rooted at gateway.
    pub merge_processors: Vec<GatewayMergeProcessor>,
    /// OTLP exporter target — typically the backend's OTLP endpoint.
    pub exporter_target: ExportTarget,
}

/// One sketch-merge processor configured at the gateway.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayMergeProcessor {
    /// OTel processor name — `sketchmergeprocessor`.
    pub processor_name: String,
    /// Sketch family being merged. All inputs to the merge agree on
    /// this (L4 type checker enforces it; design.md §6.4).
    pub sketch_kind: SketchKind,
    /// Aggregation id — matches the upstream edge's
    /// `EdgeSketchProcessor::aggregation_id` so the gateway routes
    /// streams correctly.
    pub aggregation_id: String,
}

/// Logical content of the backend `StreamingConfig`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendStageConfig {
    /// One entry per readout query the backend must serve. The
    /// `aggregation_id` in each routing entry is the backend's
    /// `OtlpReceiver` lookup key.
    pub aggregations: Vec<BackendAggregation>,
    /// One readout per `SketchEstimate` node — what the backend
    /// returns to the inference YAML's PromQL evaluator.
    pub readouts: Vec<BackendReadout>,
}

/// One sketch source the backend must accept.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendAggregation {
    /// Stable id matching the upstream gateway's `aggregation_id`.
    pub aggregation_id: String,
    /// Sketch family.
    pub sketch_kind: SketchKind,
    /// Sketch parameters — the backend uses these to build its
    /// per-aggregation `Sketch` instance (KLL with the right `k`,
    /// DDSketch with the right `alpha`, etc.).
    pub sketch_params: SketchParams,
    /// Phase ε.1 — what shape the backend ingests for this
    /// aggregation. Mode 1 (sketch at edge) / sketch_envelope is the
    /// default (the wire payload is a sketch state already). Mode 2
    /// (raw at edge → sketch at backend) sets this to `raw` so the
    /// backend builds the sketch from raw OTLP samples at ingest. The
    /// backend's `StreamingConfig` consumer interprets the field —
    /// Phase ε.2 implements the raw-input ingest path.
    #[serde(default)]
    pub aggregation_input: AggregationInput,
}

/// Phase ε.1 — what wire shape the backend ingests for an aggregation.
/// Determines whether the backend builds the sketch from raw samples
/// (Mode 2) or accepts pre-built sketch state from upstream (Mode 1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationInput {
    /// Mode 1 — backend receives sketch state envelopes (gateway-merged
    /// or direct from edge). The default for legacy plans.
    #[default]
    SketchEnvelope,
    /// Mode 2 — backend receives raw OTLP samples and builds the sketch
    /// at ingest. New in Phase ε.1; ingest path lands in Phase ε.2.
    Raw,
}

/// One readout entry — what the backend's inference YAML asks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendReadout {
    /// Aggregation this readout reads from.
    pub aggregation_id: String,
    /// Readout op (mirror of `PhysicalExpr::SketchEstimate::op`).
    pub op: EstimateOp,
}

/// Abstract OTLP / HTTP endpoint description. Phase E does not resolve
/// to a concrete URL — the emitter ships symbolic names that the
/// downstream OpAMP / backend-client wiring (Phase G+) materialises
/// using `DeploymentConstraints::executors()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExportTarget {
    /// Symbolic stage role — "ship to whichever gateway is registered".
    Stage(StageId),
    /// Concrete endpoint, e.g. `gateway:4317`.
    Endpoint(String),
}

impl Default for ExportTarget {
    fn default() -> Self {
        ExportTarget::Stage(StageId::Backend)
    }
}

// ── ThreeStageEmitter ─────────────────────────────────────────────────────────

/// DC lifecycle emitter — colours match `Topology::ThreeStage`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ThreeStageEmitter;

impl ThreeStageEmitter {
    /// Convenience alias for [`Emitter::emit_per_stage`] when callers
    /// already hold a `ThreeStageEmitter` value.
    pub fn emit(&self, dag: &ColoredDag) -> Result<HashMap<StageId, StageConfig>, EmitError> {
        self.emit_per_stage(dag)
    }
}

impl Emitter for ThreeStageEmitter {
    fn emit_per_stage(&self, dag: &ColoredDag) -> Result<HashMap<StageId, StageConfig>, EmitError> {
        if dag.topology != Topology::ThreeStage {
            return Err(EmitError::UnsupportedTopology(
                dag.topology,
                Topology::ThreeStage,
            ));
        }

        // ── Edge config ────────────────────────────────────────────────
        let mut edge = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: None,
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: HashMap::new(),
        };
        let mut backend_aggregations: Vec<BackendAggregation> = Vec::new();
        let mut gateway_processors: Vec<GatewayMergeProcessor> = Vec::new();
        let mut readouts: Vec<BackendReadout> = Vec::new();

        // Walk the DAG once, gathering per-stage facts. We rely on the
        // node table being depth-first walk order so the SketchAgg /
        // SketchEstimate / SketchMerge chain is linkable by position.
        // Aggregation ids are deterministic: `agg{N}` per SketchAgg
        // index in the DAG.
        let mut next_agg_index: usize = 0;
        // SketchAgg node id → aggregation_id. Reused by SketchMerge
        // (gateway) and SketchEstimate (backend) to thread the same id
        // through.
        let mut sketch_agg_ids: HashMap<usize, String> = HashMap::new();

        // Pass 1 — assign deterministic aggregation_ids to every
        // SketchAgg up-front so SketchMerge / SketchEstimate emission
        // (pass 2) can resolve them regardless of node-table order.
        for node in &dag.nodes {
            if let (PhysicalExpr::SketchAgg { .. }, StageId::Edge) = (&node.expr, node.stage) {
                let aggregation_id = format!("agg{next_agg_index}");
                next_agg_index += 1;
                sketch_agg_ids.insert(node.id.0, aggregation_id);
            }
        }

        // Pass 2 — emit per-stage facts.
        for node in &dag.nodes {
            match (&node.expr, node.stage) {
                // Edge: source metric + label filters from Logical
                // — the wrapped L3 sub-tree may be Scan, Window{Scan},
                // Aggregate{Window{Scan}} etc., so descend recursively.
                (PhysicalExpr::Logical(qe), StageId::Edge) => {
                    extract_edge_facts(qe, &mut edge);
                }
                // Edge: SketchAgg becomes one EdgeSketchProcessor.
                (
                    PhysicalExpr::SketchAgg {
                        sketch_type,
                        params,
                        ..
                    },
                    StageId::Edge,
                ) => {
                    let processor_name = edge_processor_name(sketch_type)?;
                    let aggregation_id = sketch_agg_ids
                        .get(&node.id.0)
                        .cloned()
                        .unwrap_or_else(|| format!("agg{}", node.id.0));
                    edge.sketch_processors.push(EdgeSketchProcessor {
                        processor_name,
                        sketch_kind: sketch_type.clone(),
                        sketch_params: params.clone(),
                        aggregation_id: aggregation_id.clone(),
                    });
                    backend_aggregations.push(BackendAggregation {
                        aggregation_id,
                        sketch_kind: sketch_type.clone(),
                        sketch_params: params.clone(),
                        // Mode 1 — sketch built at edge, ships envelope.
                        aggregation_input: AggregationInput::SketchEnvelope,
                    });
                }
                // Gateway: SketchMerge over edge sketches → one merge
                // processor per merged-sketch family. Aggregation id
                // inherited from the merge's first SketchAgg child
                // (looked up via the DAG's edges table so identical
                // child sub-trees don't collide on a position-by-expr
                // search).
                (PhysicalExpr::SketchMerge { .. }, StageId::Gateway) => {
                    if let Some((kind, aid)) =
                        first_sketch_child_via_edges(dag, node.id, &sketch_agg_ids)
                    {
                        gateway_processors.push(GatewayMergeProcessor {
                            processor_name: "sketchmergeprocessor".into(),
                            sketch_kind: kind,
                            aggregation_id: aid,
                        });
                    }
                }
                // Backend: SketchEstimate → one readout entry. The
                // matching aggregation_id comes from the descendant
                // SketchAgg (resolved by walking the DAG edges table).
                (PhysicalExpr::SketchEstimate { op, .. }, StageId::Backend) => {
                    let aid = resolve_descendant_agg_id_via_edges(dag, node.id, &sketch_agg_ids)
                        .unwrap_or_else(|| format!("agg{}", readouts.len()));
                    readouts.push(BackendReadout {
                        aggregation_id: aid,
                        op: op.clone(),
                    });
                }
                // ── Phase ε.1 Mode 3: edge raw → Prometheus OTLP receiver.
                // Records a `PrometheusArchiveMetric` so the L5 emitter
                // adds the `otlphttp/prometheus` exporter + routing
                // pipeline. Backend gets a `prometheus_remote` storage
                // routing target (no aggregation entry).
                (
                    PhysicalExpr::RawAtEdgePrometheusArchive {
                        metric,
                        window,
                        label_proj,
                    },
                    StageId::Edge,
                ) => {
                    edge.prometheus_archive_metrics
                        .push(PrometheusArchiveMetric {
                            metric: metric.clone(),
                            window_secs: window.map(|d| d.as_secs()),
                            label_proj: label_proj.clone(),
                        });
                    // Phase 3.2.5 (Bug a): Mode-3 metrics also land in
                    // the Gorilla-S3 archive so the ASAP-tier
                    // sketch-engine and the Thanos store-gateway can
                    // both serve them. The `gorillas3` processor block
                    // is emitted by the L5 emitter when this list is
                    // non-empty.
                    edge.archive_tier_metrics.push(ArchiveTierMetric {
                        metric: metric.clone(),
                        window_secs: window.map(|d| d.as_secs()),
                    });
                }
                // ── Phase ε.1 Mode 2: edge raw → backend builds sketch.
                // Edge-side: no sketch processor. Backend-side: a
                // BackendAggregation with the family the backend will
                // build at ingest. The aggregation_input=raw flag is
                // emitted by `emit_backend_config_json`.
                (PhysicalExpr::RawAtEdgeSketchAtBackend { family, params, .. }, StageId::Edge) => {
                    let aid = format!("agg{next_agg_index}");
                    next_agg_index += 1;
                    backend_aggregations.push(BackendAggregation {
                        aggregation_id: aid,
                        sketch_kind: family.clone(),
                        sketch_params: params.clone(),
                        // Mode 2 — backend builds sketch from raw OTLP.
                        aggregation_input: AggregationInput::Raw,
                    });
                }
                _ => {}
            }
        }

        let mut out: HashMap<StageId, StageConfig> = HashMap::new();
        if dag.occupied_stages().contains(&StageId::Edge) {
            out.insert(StageId::Edge, StageConfig::Edge(edge));
        }
        if dag.occupied_stages().contains(&StageId::Gateway) {
            out.insert(
                StageId::Gateway,
                StageConfig::Gateway(GatewayStageConfig {
                    otlp_receiver_port: 4317,
                    merge_processors: gateway_processors,
                    exporter_target: ExportTarget::Stage(StageId::Backend),
                }),
            );
        }
        if dag.occupied_stages().contains(&StageId::Backend) {
            if backend_aggregations.is_empty() && readouts.is_empty() {
                return Err(EmitError::BackendEmpty);
            }
            out.insert(
                StageId::Backend,
                StageConfig::Backend(BackendStageConfig {
                    aggregations: backend_aggregations,
                    readouts,
                }),
            );
        }
        Ok(out)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Map a `SketchKind` to the OTel collector processor name. Mirrors the
/// names the existing OpAMP YAML emitter (and the per-sketch processor
/// crates in `opentelemetry-collector-contrib`) already use.
pub(crate) fn edge_processor_name(kind: &SketchKind) -> Result<String, EmitError> {
    Ok(match kind {
        SketchKind::Kll => "KLL".into(),
        SketchKind::DDSketch => "ddsketch".into(),
        SketchKind::Hll => "HLL".into(),
        SketchKind::Cms => "countmin".into(),
        SketchKind::CountSketch => "countsketch".into(),
    })
}

/// Recursively descend an L3 [`crate::intent_algebra::QueryExpr`]
/// gathering edge-stage facts (source metric name, label filters,
/// window size). The L3 sub-tree wrapped in a `PhysicalExpr::Logical`
/// can be `Scan`, `Window{Scan}`, `Aggregate{Window{Scan}}`, etc.,
/// so a recursive descent is necessary to surface the leaf metric.
fn extract_edge_facts(qe: &crate::intent_algebra::QueryExpr, edge: &mut EdgeStageConfig) {
    use crate::intent_algebra::{QueryExpr as QE, Source};
    match qe {
        QE::Scan {
            source,
            label_filters,
            ..
        } => {
            if let Source::TimeSeries { metric } = source {
                if edge.source_metric.is_none() {
                    edge.source_metric = Some(metric.clone());
                }
            }
            for f in label_filters {
                let pair = (f.label.clone(), f.equals.clone());
                if !edge.label_filters.contains(&pair) {
                    edge.label_filters.push(pair);
                }
            }
        }
        QE::Window { size, child, .. } => {
            if edge.window_secs.is_none() {
                edge.window_secs = Some(size.as_secs());
            }
            extract_edge_facts(child, edge);
        }
        QE::Aggregate { child, .. } => {
            extract_edge_facts(child, edge);
        }
        QE::LetBinding { expr, child, .. } => {
            extract_edge_facts(expr, edge);
            extract_edge_facts(child, edge);
        }
        QE::Ref { .. } => {}
        // A-variants lifted in Batch 2 of the legacy_expr migration. No
        // canonical-side consumer constructs them yet — recurse into
        // children so we still surface edge facts (source metric, label
        // filters, window size) from any leaves below.
        QE::Filter { child, .. }
        | QE::Project { child, .. }
        | QE::Partition { child, .. }
        | QE::Distinct { child, .. }
        | QE::Sort { child, .. }
        | QE::Limit { child, .. }
        | QE::Subquery { child, .. } => extract_edge_facts(child, edge),
        QE::Merge { children } => {
            for c in children {
                extract_edge_facts(c, edge);
            }
        }
        QE::Join { left, right, .. }
        | QE::SetOp { left, right, .. }
        | QE::BinaryOp { lhs: left, rhs: right, .. } => {
            extract_edge_facts(left, edge);
            extract_edge_facts(right, edge);
        }
    }
}

/// Children of `parent` per the DAG's edges table. The colouring walker
/// emits parent → child edges in visit order so this iterator is
/// deterministic.
fn children_of<'a>(
    dag: &'a ColoredDag,
    parent: crate::physical::colored_dag::dag::NodeId,
) -> impl Iterator<Item = crate::physical::colored_dag::dag::NodeId> + 'a {
    dag.edges
        .iter()
        .filter(move |(p, _)| *p == parent)
        .map(|(_, c)| *c)
}

/// First descendant `SketchAgg` reachable from `parent` via the DAG's
/// edges table — used by gateway-merge / backend-readout wiring to
/// pull the right aggregation_id from `sketch_agg_ids`.
fn first_sketch_child_via_edges(
    dag: &ColoredDag,
    parent: crate::physical::colored_dag::dag::NodeId,
    sketch_agg_ids: &HashMap<usize, String>,
) -> Option<(SketchKind, String)> {
    for cid in children_of(dag, parent) {
        let cnode = dag.nodes.get(cid.0)?;
        match &cnode.expr {
            PhysicalExpr::SketchAgg { sketch_type, .. } => {
                if let Some(aid) = sketch_agg_ids.get(&cid.0) {
                    return Some((sketch_type.clone(), aid.clone()));
                }
            }
            PhysicalExpr::LetBinding { .. } | PhysicalExpr::SketchMerge { .. } => {
                if let Some(found) = first_sketch_child_via_edges(dag, cid, sketch_agg_ids) {
                    return Some(found);
                }
            }
            PhysicalExpr::Ref { name } => {
                // Resolve the ref to its binding's expr id, then recurse.
                if let Some(bid) = dag
                    .nodes
                    .iter()
                    .enumerate()
                    .find_map(|(i, n)| match &n.expr {
                        PhysicalExpr::LetBinding { name: n2, .. } if n2 == name => Some(i),
                        _ => None,
                    })
                {
                    let bnode_id = crate::physical::colored_dag::dag::NodeId(bid);
                    if let Some(found) = first_sketch_child_via_edges(dag, bnode_id, sketch_agg_ids)
                    {
                        return Some(found);
                    }
                }
            }
            _ => {}
        }
    }
    None
}

/// Aggregation_id of the first SketchAgg reachable from `parent` —
/// shorthand around [`first_sketch_child_via_edges`] for the readout
/// path (we only need the id, not the kind).
fn resolve_descendant_agg_id_via_edges(
    dag: &ColoredDag,
    parent: crate::physical::colored_dag::dag::NodeId,
    sketch_agg_ids: &HashMap<usize, String>,
) -> Option<String> {
    first_sketch_child_via_edges(dag, parent, sketch_agg_ids).map(|(_, aid)| aid)
}
