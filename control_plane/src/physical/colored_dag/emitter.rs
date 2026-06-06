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

use std::collections::{BTreeSet, HashMap};

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
    /// MVP §46 / ASAPCollector#400 — per-metric → **set of** sketch
    /// families populated by the planner from the workload spec. When
    /// non-empty, the L5 edge emitter switches to the **5-sketch
    /// routing-connector** wire shape: it loads ONLY the sketch
    /// processors for the families that at least one metric needs and
    /// uses the OTel `routing` *connector* (NOT the deprecated routing
    /// processor) to dispatch each metric to EACH per-family pipeline in
    /// its set. Metrics absent from this map fall through to the
    /// `metrics/raw_passthrough` default pipeline.
    ///
    /// CRITICAL — a metric can legitimately need MULTIPLE families,
    /// because different planned queries on the same metric require
    /// different capabilities (e.g. `quantile_over_time` → DDSketch,
    /// `count`-distinct → HLL, `topk` → CountSketch all on one metric).
    /// The value type is therefore a `BTreeSet<SketchKind>` (the UNION
    /// of capabilities across all of that metric's workload entries),
    /// not a single family. A metric in two families produces two
    /// routing-connector OTTL conditions → its samples fan into both
    /// per-family pipelines, so every (metric, capability) the workload
    /// needs still reaches its sketch family at the backend.
    ///
    /// ASAPCollector#400 bandwidth fix: the emitter prunes pipelines and
    /// processors to the union of these sets — a workload whose metrics
    /// only need DDSketch ships ONLY the DDSketch pipeline, not all 5.
    /// This eliminates the prior multi-family fan-out (every metric
    /// shipped sketch state through all 5 families regardless of need).
    ///
    /// `SketchFamily` is a control-plane-side alias for `SketchKind` per
    /// `sketch_algebra::params`. Empty map ⇒ legacy single-pipeline /
    /// Mode-3 / warm-passthrough wire shapes are emitted unchanged
    /// (backward-compat).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_family: HashMap<String, BTreeSet<SketchKind>>,
    /// MVP blocker B3 — per-metric attribute allowlist the agent must
    /// reduce wire attrs to BEFORE the sketch processor sees them.
    /// Maps each metric to its grouping-label list; the 5-sketch routing
    /// emitter (and the legacy single-pipeline emitter when
    /// `source_metric` matches) prepends a
    /// `transform/keep_for_<sanitized_metric>` OTTL processor in front
    /// of every sketch processor that calls
    /// `keep_keys(datapoint.attributes, [...])` on the listed labels.
    /// Without this the agent sketches with the full wire-attr tuple,
    /// minting one sid per unique tuple — defeating the streaming-config's
    /// `grouping_labels` contract.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_grouping_labels: HashMap<String, Vec<String>>,
    /// Issue #298 — metrics whose OTel datapoints arrive with
    /// **cumulative** temporality (OTel SDK's default for `Counter`
    /// instruments) and need to be converted to **delta** before the
    /// backend's `SumAccumulator` folds them into per-window sums.
    ///
    /// When non-empty, the 5-sketch routing emitter declares a
    /// `cumulativetodelta` processor with `include.metrics = [...]` and
    /// inserts it as the FIRST processor in the entry (`metrics:`)
    /// pipeline so every routed copy of each listed metric goes through
    /// the conversion. The processor matches on `metric.name`
    /// (strict), so unrelated metrics flow through unchanged — quantile
    /// gauges (`http_requests_total_latency_ms`) keep their wire shape.
    ///
    /// Sourced from [`crate::emit::collect_cumulative_counter_metrics`]:
    /// any metric whose workload entry classifies as
    /// [`crate::workload::AggRole::Sum`] (bare-selector / `sum` /
    /// `rate` / `increase` / `sum_over_time` / `irate`). Without the
    /// conversion, the data plane's `SumAccumulator` re-sums each
    /// cumulative carry-value within and across windows, producing a
    /// quadratic-in-time blowup (observed: `sum by (zone)
    /// (http_requests_total)` returned ~300× baseline pre-fix).
    ///
    /// Empty list (default) ⇒ no `cumulativetodelta` processor is
    /// emitted; backward-compat for plans that never declare a counter
    /// metric (e.g. quantile-only workloads).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cumulative_counter_metrics: Vec<String>,
    /// PR #311 follow-up — the cold-tier (Gorilla archive) ingest URL the
    /// fused `asap_edge` processor ships per-emit Gorilla blocks to. This
    /// is the gorilla-merger's HTTP ingest endpoint
    /// (`http://gorilla-merger:10908/ingest/gorilla`; the gRPC side is
    /// 10907) — NOT the OTLP backend host/port. PR #311 lacked this field
    /// and derived a wrong placeholder (`http://<backend>:9098/...`) from
    /// `exporter_target`; threading the real value here fixes that.
    ///
    /// `None` ⇒ the emitter falls back to [`default_cold_ship_endpoint`]
    /// (a single named default), so legacy / test construction sites that
    /// don't populate it still emit a correct merger endpoint. The
    /// `colored_dag` L5 layer cannot resolve a real per-deploy endpoint
    /// (it is deployment-independent — no `DeploymentConstraints` is
    /// plumbed in), so it populates the named default; a future layer that
    /// holds deploy info can set a concrete value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_ship_endpoint: Option<String>,
    /// PR #311 follow-up — external labels stamped on every cold-tier
    /// Gorilla block the fused `asap_edge` processor ships (the merger
    /// uses these for cross-cluster disambiguation). Carried as
    /// `(label, value)` tuples (deterministic order at the emit site).
    /// PR #311 derived `cluster` from the `ASAP_CLUSTER` env inline; this
    /// field threads it explicitly. Empty ⇒ the emitter falls back to
    /// [`default_cold_external_labels`] (a single named default that reads
    /// `ASAP_CLUSTER`, defaulting to `asap-mvp`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cold_external_labels: Vec<(String, String)>,
    /// Per-metric sketch **sampling probability** `p` in `(0, 1]`,
    /// populated by the planner from each workload entry's
    /// [`crate::workload::WorkloadEntry::sample_p`].
    ///
    /// The L5 edge emitter reads this in `build_edge_processor_block` and
    /// writes a `sample_p: <p>` knob onto the matching metric's
    /// sketch-processor block ONLY when `p < 1.0`. A metric absent from
    /// this map (or mapped to `1.0`) emits no `sample_p` key, so the
    /// agent's processor `Config.Validate` normalises the unset field to
    /// `1.0` (sampling disabled) and the wire bytes stay byte-identical to
    /// the pre-sampling format.
    ///
    /// Activates the warm-sketch sampling layer the agent's sketch
    /// processors carry (sketchlib-go geometric / hash-threshold sampling):
    /// the encoder admits a `p` fraction of updates, stores the RAW
    /// sampled state + `p`, and the backend rescales count-like estimates
    /// by `1/p` at query time. This is a static operator-set knob; an
    /// optimizer-driven dynamic `p` is a follow-up (out of scope here).
    ///
    /// Empty map (default) ⇒ no metric carries sampling — backward-compat.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_sample_p: HashMap<String, f64>,
    /// Per-metric **known distinct-key count per window** (cardinality hint),
    /// populated by the planner from each workload entry's
    /// [`crate::workload::WorkloadEntry::distinct_keys_per_window`] via
    /// [`crate::emit::collect_metric_to_distinct_keys`].
    ///
    /// The fused `asap_edge` emitter's HLL branch reads this to refine the
    /// sparse-vs-dense base selection introduced in PR #358: a per-series HLL
    /// is sparse by default, but when the hint for the metric is `Some(n)` with
    /// `n` at or above the in-memory sparse→dense promotion crossover
    /// (`DENSE_CROSSOVER`) the HLL is emitted DENSE instead — sparse only helps
    /// low-cardinality series; a high-cardinality per-series HLL would just pay
    /// promotion churn from the sparse base.
    ///
    /// A metric absent from this map keeps the PR #358 scope-based default
    /// (per-series ⇒ sparse, whole-stream ⇒ dense), so the emitted config stays
    /// byte-identical when no cardinality hint is declared. Empty map (default)
    /// ⇒ no metric carries a hint — backward-compat.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_distinct_keys: HashMap<String, u64>,
    /// Per-metric **inner item dimension** for the item-counting sketch
    /// families (HLL / CountSketch / CountMinSketch): the data-point
    /// attribute whose VALUE is the "item" the sketch counts or ranks (e.g.
    /// `user_id` for `unique_users_per_min`, `endpoint` for
    /// `top_endpoint_qps` / `endpoint_request_freq`), populated by the
    /// planner from each workload entry's
    /// [`crate::workload::WorkloadEntry::item_label`] via
    /// [`crate::emit::collect_metric_to_item_label`].
    ///
    /// The fused `asap_edge` emitter (`emit_edge_yaml_asap_edge`) writes this
    /// onto the per-metric sketch entry as `item_label`, telling the agent
    /// to fold the named high-cardinality attribute INTO the sketch instead
    /// of leaving it in the sketch's series key. Without it the inner
    /// attribute (`user_id` / `endpoint`) lands in the series key, minting
    /// one cardinality-1 HLL per distinct value instead of one HLL per
    /// grouping (zone) — the HLL/CMS warm queries then return semantically
    /// wrong / empty results.
    ///
    /// For the CountSketch family the emitter falls back to the metric-name
    /// convention (`countsketch_item_label_for`) when a metric is absent
    /// from this map, preserving the prior behaviour. Empty map (default) ⇒
    /// no metric carries an explicit item dimension — backward-compat.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metric_to_item_label: HashMap<String, String>,
    /// Cold-archive **wire format** the agent's `asapedgeprocessor` ships
    /// its cold tier in. Two formats are merged in the agent:
    ///
    /// - [`ColdFormat::Fragment`] (default) — gorilla-XOR fragments, shipped
    ///   to [`cold_ship_endpoint`](Self::cold_ship_endpoint) (`/ingest/gorilla`).
    /// - [`ColdFormat::Intchunk`] — the lossless intchunk cold-part format,
    ///   shipped to [`cold_coldpart_endpoint`](Self::cold_coldpart_endpoint)
    ///   (`/ingest/coldpart`).
    ///
    /// The L5 edge emitter writes a `cold.format` + `cold.coldpart_endpoint`
    /// pair onto the agent `cold:` block ONLY when this is
    /// [`ColdFormat::Intchunk`]. [`ColdFormat::Fragment`] (the default)
    /// emits NEITHER key, so the agent's cold block stays byte-identical to
    /// the pre-format emit (`ship_endpoint` only) — no behavior change when
    /// unset.
    #[serde(default, skip_serializing_if = "ColdFormat::is_default")]
    pub cold_format: ColdFormat,
    /// Cold-archive intchunk ingest URL — the gorilla-merger's coldpart
    /// HTTP ingest endpoint (`http://gorilla-merger:10908/ingest/coldpart`).
    /// Only emitted (and only meaningful) when
    /// [`cold_format`](Self::cold_format) is [`ColdFormat::Intchunk`].
    ///
    /// `None` ⇒ the emitter derives it from
    /// [`cold_ship_endpoint`](Self::cold_ship_endpoint) by swapping the path
    /// to `/ingest/coldpart` (same merger host:port as the fragment
    /// endpoint), falling back to [`default_cold_coldpart_endpoint`] when
    /// neither is set. Ignored entirely for [`ColdFormat::Fragment`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_coldpart_endpoint: Option<String>,
}

/// Cold-archive wire format the agent ships its cold tier in. See
/// [`EdgeStageConfig::cold_format`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColdFormat {
    /// gorilla-XOR fragments → `cold.ship_endpoint` (`/ingest/gorilla`).
    /// The default — emits NO `format:`/`coldpart_endpoint:` keys, so the
    /// agent cold block is byte-identical to the pre-format emit.
    #[default]
    Fragment,
    /// Lossless intchunk cold-part → `cold.coldpart_endpoint`
    /// (`/ingest/coldpart`).
    Intchunk,
}

impl ColdFormat {
    /// `true` for the default ([`ColdFormat::Fragment`]). Drives the
    /// `skip_serializing_if` on [`EdgeStageConfig::cold_format`] so an unset
    /// format leaves the serialized config byte-identical to today.
    pub fn is_default(&self) -> bool {
        matches!(self, ColdFormat::Fragment)
    }
}

/// Named default for [`EdgeStageConfig::cold_ship_endpoint`].
///
/// The cold tier ships per-emit Gorilla blocks to the **gorilla-merger**
/// over HTTP ingest port **10908** (the gRPC ingest side is 10907). This
/// is the one canonical place that default lives — construction sites and
/// the `asap_edge` emitter both route through here rather than inlining
/// the host/port. PR #311's `http://<backend>:9098/ingest/gorilla` guess
/// was wrong (wrong host, wrong port); this is the correct merger target.
pub fn default_cold_ship_endpoint() -> String {
    "http://gorilla-merger:10908/ingest/gorilla".to_string()
}

/// Named default for [`EdgeStageConfig::cold_coldpart_endpoint`].
///
/// The intchunk cold-part tier ships to the SAME gorilla-merger host:port
/// as the fragment tier, but on the `/ingest/coldpart` path (the fragment
/// path is `/ingest/gorilla`). Single source of truth so the emitter and
/// any construction site agree. Only consulted when
/// [`EdgeStageConfig::cold_format`] is [`ColdFormat::Intchunk`].
pub fn default_cold_coldpart_endpoint() -> String {
    "http://gorilla-merger:10908/ingest/coldpart".to_string()
}

/// Derive a coldpart ingest URL from a fragment `ship_endpoint` by
/// swapping the trailing `/ingest/gorilla` path for `/ingest/coldpart`
/// (the merger host:port is shared between the two cold tiers). Falls back
/// to [`default_cold_coldpart_endpoint`] when the input doesn't carry the
/// expected fragment path, so a non-standard endpoint still yields a
/// well-formed coldpart target rather than a malformed one.
pub fn coldpart_endpoint_from_ship(ship_endpoint: &str) -> String {
    match ship_endpoint.strip_suffix("/ingest/gorilla") {
        Some(host) => format!("{host}/ingest/coldpart"),
        None => default_cold_coldpart_endpoint(),
    }
}

/// Named default for [`EdgeStageConfig::cold_external_labels`].
///
/// One `cluster` label, read from `ASAP_CLUSTER` (default `asap-mvp`).
/// Single source of truth for the cold external-label default so the
/// emitter and any construction site agree.
pub fn default_cold_external_labels() -> Vec<(String, String)> {
    let cluster = std::env::var("ASAP_CLUSTER").unwrap_or_else(|_| "asap-mvp".to_string());
    vec![("cluster".to_string(), cluster)]
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
    /// Internal emitter plumbing — threads `EdgeSketchProcessor` →
    /// `GatewayMergeProcessor` (which DOES surface it on the wire to
    /// route merged streams) during the DAG walk. Phase E derives a
    /// deterministic id from the processor name + a position counter;
    /// downstream callers may override.
    ///
    /// **Wire-format invariant**: the asap-otel agent's
    /// sketch-processor config does NOT consume this field — the
    /// patched processors content-address sids via
    /// `(metric, attrs_fingerprint, agg_kind_canonical)` at the
    /// backend. See `emit::otap::build_asap_sketches_config` (the
    /// `EdgeSketchProcessor` → agent YAML emitter) for the explicit
    /// omission, and `emit::asapquery_backend::tests::emitted_yaml_omits_aggregation_id`
    /// for the regression guard on the streaming-config side.
    /// The field stays on the struct because it's still load-bearing
    /// for gateway-tier merge routing (see `GatewayMergeProcessor`).
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
    ///
    /// **Wire-format**: unlike its `EdgeSketchProcessor` /
    /// `BackendAggregation` siblings, this id IS surfaced on the
    /// gateway YAML wire (see `emit::stage_config::build_gateway_merge_block`)
    /// — the gateway's sketchmerge processor uses it as its
    /// per-merge lookup key. Retiring `aggregation_id` would
    /// require co-retiring the gateway-tier merge protocol.
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
///
/// `aggregation_id` is **internal plumbing only** — used by the emitter
/// to thread `SketchAgg` → `BackendAggregation` → `BackendReadout`
/// during the DAG walk. It is **not** emitted on the wire (PR 5 retired
/// the controller-allocated id; the backend content-addresses identity
/// via `PolicyFingerprint(u64)` derived from `metric_name`,
/// `sketch_kind`, `sketch_params`, grouping labels, and `spatial_filter`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackendAggregation {
    /// Internal-only id (see struct doc). Not on the wire.
    pub aggregation_id: String,
    /// Source metric the aggregation runs over (e.g.
    /// `http_requests_total_latency_ms`). Required by the backend's
    /// `AggregationConfig` parser.
    pub metric_name: String,
    /// Sketch family.
    pub sketch_kind: SketchKind,
    /// Sketch parameters — the backend uses these to build its
    /// per-aggregation `Sketch` instance (KLL with the right `k`,
    /// DDSketch with the right `alpha`, etc.).
    pub sketch_params: SketchParams,
    /// Tumbling window size in seconds. Required by the backend; the
    /// parser rejects zero-window aggregations.
    pub window_secs: u64,
    /// Spatial filter (comma-joined `k=v` pairs from the edge's
    /// `label_filters`). Empty string when no filter applies.
    #[serde(default)]
    pub spatial_filter: String,
    /// Group-by label names — keys in `labels.grouping` on the backend
    /// side, where the precompute engine's accumulator pipeline keys
    /// its per-aggregation state by the projected attribute set.
    ///
    /// The L5 emitter populates this empty (`vec![]`); the caller
    /// (`handle_plan`) patches it from `workload.group_by_labels`
    /// before posting the streaming-config JSON. The canonical L3
    /// `QueryExpr::Aggregate.by` carries the keys as positional
    /// `ColumnId`s against a synthesized schema that has no label
    /// columns (open-set label naming is a Step γ TODO in
    /// `intent_algebra::column_resolution`), so the workload-spec
    /// strings are the only reliable source of the names today.
    #[serde(default)]
    pub grouping: Vec<String>,
    /// Per-item dimension (the data-point attribute NAME, e.g. "endpoint"
    /// or "service") for an item_label-mode frequency sketch. Like
    /// `grouping`, the L5 emitter leaves this `None`; `handle_plan` patches
    /// it from the workload's `item_label`. Emitted into the aggregation's
    /// `parameters["item_label"]` so the data-plane ingest records it on the
    /// CMS sid and can answer per-item `estimate(key)` (FrequencyEstimate).
    #[serde(default)]
    pub item_label: Option<String>,
    /// Phase ε.1 — what shape the backend ingests for this
    /// aggregation. Mode 1 (sketch at edge) / sketch_envelope is the
    /// default (the wire payload is a sketch state already). Mode 2
    /// (raw at edge → sketch at backend) sets this to `raw` so the
    /// backend builds the sketch from raw OTLP samples at ingest. The
    /// backend's `StreamingConfig` consumer interprets the field —
    /// Phase ε.2 implements the raw-input ingest path.
    #[serde(default)]
    pub aggregation_input: AggregationInput,

    /// Option B (post-PR-#287) — when `Some(s)`, the wire-side
    /// `aggregationType` is `s` (e.g. `"Sum"`, `"Increase"`,
    /// `"MinMax"`) and the `parameters` object is emitted as `{}`,
    /// bypassing the sketch-kind → backend-type mapping that runs
    /// for the regular sketched aggregations.
    ///
    /// Why: the typed `bind_workload_typed` rule chain only knows
    /// how to lower sketch-shaped statistics (Quantile / Cardinality
    /// / Frequency / TopK). Sum/Rate/Count workloads — `sum by
    /// (zone) (http_requests_total)`, `rate(metric[5m])`,
    /// `count(metric)` — currently decline binding (return None) so
    /// the typed L5 stage-split emits nothing for them. Under the
    /// Option B unification, the Replanner falls back to this
    /// override shape to emit an `ExactAgg(Sum)` (or Increase /
    /// Count) row into the cumulative streaming-config so the data
    /// plane recognises the metric and `sum by (zone) (…)` queries
    /// resolve. `sketch_kind` / `sketch_params` carry sentinel
    /// values when the override is in effect (their emitted form is
    /// suppressed in `build_backend_aggregation_json`).
    #[serde(default)]
    pub agg_type_override: Option<String>,
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
        // Default exporter target is the asapquery-backend stage: the
        // backend's precompute engine merges cross-agent sketches via
        // its accumulators, so no middle-tier gateway processor sits in
        // the default data path. Callers wanting a gateway in the path
        // can re-write `exporter_target` post-emit.
        let mut edge = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: None,
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Backend),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: HashMap::new(),
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            // The colored-DAG layer is deployment-independent (no
            // `DeploymentConstraints` is plumbed in here — see the
            // module header), so we cannot resolve a real per-deploy cold
            // endpoint at this layer. Populate the single named defaults;
            // a layer that holds deploy info can overwrite `edge.cold_*`
            // post-emit (same pattern as `exporter_target`).
            cold_ship_endpoint: Some(default_cold_ship_endpoint()),
            cold_external_labels: default_cold_external_labels(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            // Cold-archive format defaults to gorilla-XOR fragments; the
            // intchunk format (and its coldpart endpoint) is opted into by
            // a deploy-info-bearing layer post-emit (same pattern as the
            // cold endpoint above), keeping this layer deployment-agnostic.
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: ColdFormat::default(),
            cold_coldpart_endpoint: None,
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

        // Pass 0 — populate edge facts (source_metric, window_secs,
        // label_filters) from every `Logical(qe) @ Edge` node before
        // anything else reads them. Necessary because the DAG's
        // depth-first node order puts SketchAgg BEFORE its
        // `Logical(Window{Scan})` child, but the SketchAgg arm of
        // Pass 2 captures `edge.source_metric` / `edge.window_secs` /
        // `edge.label_filters` *at push time* when building
        // `BackendAggregation` (and `EdgeSketchProcessor`'s
        // `spatial_filter` etc.). Without this pre-pass, those fields
        // see `None` because the child Logical hasn't been visited
        // yet — and the resulting `BackendAggregation` ships with an
        // empty `metric_name` / `window_secs: 0` / `spatial_filter:
        // ""`, which the backend's `AggregationConfig::from_yaml_data`
        // rejects on `Missing metric` / `Missing windowSize`.
        //
        // `extract_edge_facts` is idempotent on `source_metric` and
        // `window_secs` (only sets if `None`) and dedupes
        // `label_filters` — so the Pass 2 arm that also calls it
        // remains correct (no double-counting).
        for node in &dag.nodes {
            if let (PhysicalExpr::Logical(qe), StageId::Edge) = (&node.expr, node.stage) {
                extract_edge_facts(qe, &mut edge);
            }
        }

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
            item_label: None,
                        aggregation_id,
                        metric_name: edge.source_metric.clone().unwrap_or_default(),
                        sketch_kind: sketch_type.clone(),
                        sketch_params: params.clone(),
                        window_secs: edge.window_secs.unwrap_or(0),
                        spatial_filter: spatial_filter_from_label_filters(&edge.label_filters),
                        // Populated post-emit by the caller (handle_plan)
                        // from workload.group_by_labels — see the
                        // struct doc-comment for the rationale.
                        grouping: Vec::new(),
                        // Mode 1 — sketch built at edge, ships envelope.
                        aggregation_input: AggregationInput::SketchEnvelope,
                        // Regular sketch path — no override.
                        agg_type_override: None,
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
                // emitted by `emit_backend_streaming_config_json`.
                (PhysicalExpr::RawAtEdgeSketchAtBackend { family, params, .. }, StageId::Edge) => {
                    let aid = format!("agg{next_agg_index}");
                    next_agg_index += 1;
                    backend_aggregations.push(BackendAggregation {
            item_label: None,
                        aggregation_id: aid,
                        metric_name: edge.source_metric.clone().unwrap_or_default(),
                        sketch_kind: family.clone(),
                        sketch_params: params.clone(),
                        window_secs: edge.window_secs.unwrap_or(0),
                        spatial_filter: spatial_filter_from_label_filters(&edge.label_filters),
                        grouping: Vec::new(),
                        // Mode 2 — backend builds sketch from raw OTLP.
                        aggregation_input: AggregationInput::Raw,
                        // Regular sketch path — no override.
                        agg_type_override: None,
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
        // A-variants lifted in Batch 2 of the relational migration. No
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

/// Join `label_filters` into the comma-separated `k=v` form the backend's
/// `AggregationConfig` spatial-filter parser accepts. Empty list → empty
/// string (the backend reads that as "no spatial filter").
pub(crate) fn spatial_filter_from_label_filters(filters: &[(String, String)]) -> String {
    let mut parts: Vec<String> = filters.iter().map(|(k, v)| format!("{k}={v}")).collect();
    parts.sort();
    parts.join(",")
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
