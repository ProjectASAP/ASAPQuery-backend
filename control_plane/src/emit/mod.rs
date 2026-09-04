//! `emit/` — per-deployment-model plan emitters (L5 output side).
//!
//! Per `control_plane/docs/design.md` §5 `core::emit`. The 2026-05
//! layered-cleanup refactor consolidated the former
//! `controller/src/config/` directory here. Mapping:
//!
//! | Old path | New path |
//! |---|---|
//! | `config/agent.rs` | [`agent`] |
//! | `config/backend.rs` | *retired — emitted YAML for a "backend-role" OTel merge collector tier that was never deployed; superseded by the typed L5's [`stage_config::emit_backend_streaming_config_json`] which posts to asapquery-backend's precompute engine over HTTP* |
//! | `config/asapquery_backend.rs` | *retired — `generate_streaming_config_yaml` was the legacy single-aggregation `CollectionPlan`-shaped emitter for `POST /api/v1/streaming-config`; under the data plane's atomic `handle.swap(new_config)` it would WIPE sibling `(metric, role)` aggregations on every fire. Replaced by [`backend_push::post_typed_backend_for_role`], which posts a cumulative typed `BackendStageConfig` derived from the shared per-`(metric, role)` cache* |
//! | *(new)* | [`backend_push`] |
//! | `config/stage_config.rs` | [`stage_config`] (TODO: split into `opamp` + `streaming_config` + `inference_config` per design.md §5; deferred from refactor 2026-05 because the 3,020-line monolith mixes OTel-collector YAML emit, ASAPQuery-backend JSON emit, and shared internals — clean split needs ownership reorganisation, not file renames) |
//! | `config/stage_config_otap.rs` | [`otap`] |
//! | `config/stage_config_telegraf.rs` | [`telegraf`] |
//! | `config/workloads.rs` | [`crate::workload`] (top-level — design.md §5 puts `workload` next to `emit`, not inside it) |

pub mod agent;
pub mod backend_push;
pub mod monitor;
pub mod otap;
pub mod stage_config;
pub mod telegraf;

pub use agent::generate_agent_collector_config;
pub use backend_push::{
    post_typed_backend_for_role, repost_cumulative_backend_config, BackendRoutingCache, PushOutcome,
};
pub use otap::emit_otap_dag_yaml;
pub use stage_config::{
    emit_backend_storage_routing, emit_backend_storage_routing_for_tenant,
    emit_backend_storage_routing_with_prometheus,
    emit_backend_storage_routing_with_prometheus_for_tenant, emit_backend_streaming_config_json,
    emit_edge_yaml, emit_gateway_yaml, DEFAULT_TENANT,
};
pub use telegraf::emit_telegraf_toml;

// Refactor 2026-05: design.md §5 puts `WorkloadRegistry` next to
// `emit`, not inside it. The new home is `crate::workload`; we re-export
// here so historical `crate::config::WorkloadRegistry` and
// `control_plane::config::WorkloadRegistry` references via the `config`
// back-compat alias keep working without churn.
pub use crate::workload::WorkloadRegistry;

use crate::physical::colored_dag::emitter::EdgeStageConfig;
use crate::physical::post_asap::deployment_expr::PostAsapPlan;
use crate::physical::post_asap::PhysicalExpr;
use crate::store::WorkloadStore;
use anyhow::Result;
use planner_types::post_asap::{SketchAlgorithm, SummaryExpr, SummaryNode};
use std::rc::Rc;

/// Phase ε.1.5 — which edge runtime an agent identifies as.
///
/// Today every agent the controller has built for runs the OTel-collector
/// (`AsapOtel`); Phase ε.1.5 adds the two new runtime variants the
/// per-runtime emitters target. The runtime is reported by the agent on
/// OpAMP `on_connect` (header `X-Agent-Runtime`); when absent (legacy
/// agents) the controller defaults to `AsapOtel` so the existing
/// behaviour is preserved.
///
/// Phase ε.1.5 commits the enum + emit-dispatch function. Threading the
/// runtime through OpAMP `on_connect` and into the typed L5 emit path
/// is a follow-up — the emitters can be exercised in isolation today
/// (the Phase ε.1.5 test suite does exactly that).
///
/// Naming history: the variants were originally `Sketchcollector` /
/// `Sketchotap` / `Sketchtelegraf`; the rename to `AsapOtel` /
/// `AsapOtap` / `AsapTelegraf` (PR `refactor/rename-edge-runtimes-...`)
/// drops the v0 `sketch*` prefix in favour of the symmetric `asap-*`
/// namespace. `from_header` accepts both forms during transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRuntime {
    /// Default — OTel-collector contrib build (existing behaviour).
    AsapOtel,
    /// otap-dataflow Rust runtime.
    AsapOtap,
    /// Telegraf runtime.
    AsapTelegraf,
}

impl Default for AgentRuntime {
    fn default() -> Self {
        AgentRuntime::AsapOtel
    }
}

impl AgentRuntime {
    /// Parse an `X-Agent-Runtime` header value. Recognises
    /// `asap-otel` / `asap-otap` / `asap-telegraf`
    /// (case-insensitive); any other value (including the empty string)
    /// defaults to `AsapOtel` so legacy agents keep working.
    pub fn from_header(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "asap-otap" | "otap" => AgentRuntime::AsapOtap,
            "asap-telegraf" | "telegraf" => AgentRuntime::AsapTelegraf,
            "asap-otel" => AgentRuntime::AsapOtel,
            _ => AgentRuntime::AsapOtel,
        }
    }
}

/// Phase ε.1.5 — dispatch the edge emit by agent runtime. Mirrors
/// `emit_edge_yaml`'s `(cfg, opamp_endpoint) -> String` shape; the OTAP
/// and Telegraf emitters take an additional optional Prometheus URL
/// override which we pass through `prometheus_url`.
///
/// `prometheus_url` is the Mode-3 destination override:
///   * `AsapOtel` → ignored (the OTel-collector emitter already
///     reads `${ASAP_PROMETHEUS_OTLP_URL}` at runtime);
///   * `AsapOtap`      → OTLP HTTP URL passed to `emit_otap_dag_yaml`;
///   * `AsapTelegraf`  → remote-write URL passed to `emit_telegraf_toml`.
pub fn emit_for_runtime(
    runtime: AgentRuntime,
    cfg: &EdgeStageConfig,
    opamp_endpoint: &str,
    prometheus_url: Option<&str>,
    agent_id: &str,
) -> Result<String> {
    match runtime {
        AgentRuntime::AsapOtel => emit_edge_yaml(cfg, opamp_endpoint, agent_id),
        AgentRuntime::AsapOtap => emit_otap_dag_yaml(cfg, opamp_endpoint, prometheus_url),
        AgentRuntime::AsapTelegraf => emit_telegraf_toml(cfg, prometheus_url),
    }
}

/// 10s flush window for the freshness probes — see the comment in
/// `emit_bootstrap_typed` (and the original PR #333) for the rationale.
/// Smallest window that produces well-formed Prometheus-TSDB blocks
/// while keeping criterion ⑥'s ASAP-tier p50 ≤ 30s budget.
pub const FRESHNESS_PROBE_WINDOW_SECS: u64 = 10;

/// 60s window for non-probe workload-registry metrics added to the
/// archive tier so the accuracy reducer's archive engine has ground
/// truth for every replay row.
pub const WORKLOAD_ARCHIVE_WINDOW_SECS: u64 = 60;

/// The two freshness probes — bootstrap/replan demo plumbing for
/// criterion ⑥. Not user metrics. The replay client polls them via
/// `last_over_time(http_freshness_probe_warm[10s])`; without
/// warm-passthrough routing the DDSketch processor renames them to
/// `_quantile`, and without `gorillas3` archive write the warm engine
/// has nothing to look at.
pub const FRESHNESS_PROBE_METRICS: &[&str] =
    &["http_freshness_probe_warm", "http_freshness_probe_archive"];

/// Bootstrap/replan-scope plumbing: extend an Edge stage config with
/// the freshness-probe metrics (`http_freshness_probe_warm` /
/// `http_freshness_probe_archive`) AND the workload-registry archive
/// metrics so the agent's `gorillas3` processor writes them into the
/// Gorilla-S3 / Thanos archive — required for criterion ⑥
/// (freshness probe routing) and criterion ④ (archive ground truth).
///
/// Mutates `edge_cfg` in place. Idempotent — metrics already present
/// in `archive_tier_metrics` / `warm_passthrough_metrics` are not
/// duplicated.
///
/// Originally inlined in `main::emit_bootstrap_typed`; lifted here so
/// the typed-replan path in `replan::Replanner` can apply the same
/// extension without depending on private state in `main.rs`.
///
/// ## Scope note
///
/// The live planner stays free to plan per-metric without these
/// defaults bleeding into its output — the helper is only invoked
/// from the bootstrap GET path and the OpAMP-on-connect / replan
/// push paths, both of which are demo-scope contracts.
pub fn extend_edge_with_demo_plumbing(
    edge_cfg: &mut EdgeStageConfig,
    workload_registry_metrics: impl IntoIterator<Item = String>,
) {
    use crate::physical::colored_dag::emitter::ArchiveTierMetric;

    // 1. Freshness probes → archive tier with the tight 10s window.
    for m in FRESHNESS_PROBE_METRICS.iter() {
        if !edge_cfg.archive_tier_metrics.iter().any(|a| a.metric == *m) {
            edge_cfg.archive_tier_metrics.push(ArchiveTierMetric {
                metric: (*m).to_string(),
                window_secs: Some(FRESHNESS_PROBE_WINDOW_SECS),
            });
        }
    }

    // 2. Freshness probes → warm-passthrough so the DDSketch processor
    //    doesn't rename them to `_quantile`.
    for m in FRESHNESS_PROBE_METRICS.iter() {
        if !edge_cfg.warm_passthrough_metrics.iter().any(|s| s == m) {
            edge_cfg.warm_passthrough_metrics.push((*m).to_string());
        }
    }

    // 3. All non-probe workload-registry metrics → archive tier (60s).
    let mut seen: std::collections::HashSet<String> = edge_cfg
        .archive_tier_metrics
        .iter()
        .map(|a| a.metric.clone())
        .collect();
    for metric in workload_registry_metrics {
        if seen.insert(metric.clone()) {
            edge_cfg.archive_tier_metrics.push(ArchiveTierMetric {
                metric,
                window_secs: Some(WORKLOAD_ARCHIVE_WINDOW_SECS),
            });
        }
    }

    // 4. Cold-archive format opt-in. The colored-DAG L5 layer is
    //    deployment-independent and can only populate the named default
    //    (`Fragment`); this bootstrap/replan-scope helper is the first
    //    place that holds deploy info (env), so it reads the operator's
    //    `ASAP_COLD_FORMAT` knob (mirrors how `default_cold_external_labels`
    //    reads `ASAP_CLUSTER`). `intchunk` ⇒ ship the lossless intchunk
    //    cold-part format; anything else (incl. unset / `fragment`) leaves
    //    the default gorilla-XOR fragment emit byte-identical.
    apply_cold_format_from_env(edge_cfg);
}

/// Read the `ASAP_COLD_FORMAT` env knob and, when it is `intchunk`, flip
/// `edge_cfg.cold_format` to [`ColdFormat::Intchunk`] and derive the
/// `cold_coldpart_endpoint` from the cold ship endpoint (swapping the path
/// to `/ingest/coldpart`) unless an explicit `ASAP_COLD_COLDPART_ENDPOINT`
/// is supplied.
///
/// Any value other than `intchunk` (including unset, empty, or `fragment`)
/// is a no-op — the default gorilla-XOR fragment emit stays byte-identical,
/// so there is NO behavior change unless an operator deliberately opts in.
fn apply_cold_format_from_env(edge_cfg: &mut EdgeStageConfig) {
    use crate::physical::colored_dag::emitter::{
        coldpart_endpoint_from_ship, default_cold_ship_endpoint, ColdFormat,
    };
    let fmt = std::env::var("ASAP_COLD_FORMAT").unwrap_or_default();
    if !fmt.eq_ignore_ascii_case("intchunk") {
        return;
    }
    edge_cfg.cold_format = ColdFormat::Intchunk;
    // An explicit endpoint override wins; otherwise derive from the cold
    // ship endpoint (same merger host:port, `/ingest/coldpart` path).
    if let Ok(ep) = std::env::var("ASAP_COLD_COLDPART_ENDPOINT") {
        if !ep.trim().is_empty() {
            edge_cfg.cold_coldpart_endpoint = Some(ep);
            return;
        }
    }
    let ship = edge_cfg
        .cold_ship_endpoint
        .clone()
        .unwrap_or_else(default_cold_ship_endpoint);
    edge_cfg.cold_coldpart_endpoint = Some(coldpart_endpoint_from_ship(&ship));
}

// ── MVP §46: planner ↔ 5-sketch emitter stitching ──────────────────────────────
//
// PR #339 (planner) classifies a single metric and produces a `PhysicalExpr`
// pinning a sketch family. PR #340 (emitter) gates the 5-sketch
// routing-connector wire shape on `EdgeStageConfig::metric_to_family`
// being non-empty. Until this stitch shipped, nothing populated the
// HashMap — the typed bootstrap / replan paths emitted single-pipeline
// YAML and the routing-connector path stayed dormant.
//
// `extract_root_sketch_algorithm` walks a `PhysicalExpr` tree and returns the
// committed sketch family — looking through `SketchEstimate`,
// `SketchAgg`, `SketchMerge`, `LetBinding`, and `RawAtEdgeSketchAtBackend`.
// `SketchAgg::sketch_type` is the canonical source of truth (the typed
// path's `Bind*` rules drop their family commitment here).
//
// `collect_metric_to_family` is the multi-metric loop: walk the workload
// registry, run `bind_workload_typed` per metric, and collect the
// committed family into the HashMap. Metrics that decline binding —
// `http_requests_total` (raw passthrough), exact-required workloads,
// multi-intent — are skipped, which is exactly the contract the
// `emit_edge_yaml_5sketch_routing` path expects (absent metrics
// fall through to `metrics/raw_passthrough`).

/// Walk a `PhysicalExpr` tree and return the first `SketchAgg::sketch_type`
/// (or the `RawAtEdgeSketchAtBackend::family` Mode-2 equivalent). The
/// canonical shape produced by `bind_workload_typed` is
/// `SketchEstimate { child: SketchAgg { sketch_type, … } }`, so this is
/// effectively a one-level descent — but we walk recursively to stay
/// robust against future shape changes (e.g. Bind* rules wrapping
/// in `LetBinding` for fan-in shared sketches).
///
/// Returns `None` only for trees that carry no sketch commitment
/// (`Logical`-only, unresolved `Ref`, raw Mode-3 archive). These map
/// onto the raw-passthrough default pipeline in the routing emitter,
/// which is correct.
pub fn extract_root_sketch_algorithm(expr: &PhysicalExpr) -> Option<SketchAlgorithm> {
    match expr {
        PhysicalExpr::Committed(plan) => extract_from_plan(plan),
        PhysicalExpr::RawAtEdgeSketchAtBackend { family, .. } => Some(family.clone()),
        PhysicalExpr::RawAtEdgePrometheusArchive { .. } => None,
    }
}

fn extract_from_plan(plan: &PostAsapPlan) -> Option<SketchAlgorithm> {
    match plan {
        PostAsapPlan::Summary(node) => extract_from_node(node),
        PostAsapPlan::LetBinding { expr, child, .. } => {
            extract_from_plan(expr).or_else(|| extract_from_plan(child))
        }
        PostAsapPlan::Ref { .. } => None,
    }
}

fn extract_from_node(node: &Rc<SummaryNode>) -> Option<SketchAlgorithm> {
    match &node.expr {
        // `SummaryAgg`'s `kind`/`params` collapsed into one `family:
        // SummaryFamilyType` field (ASAPPlanner#218 -- see
        // control_plane/docs/design-asapplanner-pin-migration.md); the
        // exact-vs-sketch check this used to need `is_exact_accumulator`
        // for is now which enum variant `family` is.
        SummaryExpr::SummaryAgg {
            family: planner_types::post_asap::SummaryFamilyType::Sketch(kind, _),
            ..
        } => Some(kind.algorithm().clone()),
        // An exact accumulator has no sketch family beneath it (its own
        // child is always a plain `Logical` leaf) — same as the old
        // `ExactAgg` case.
        SummaryExpr::SummaryAgg { .. } => None,
        SummaryExpr::SummaryEstimate { summary_input, .. } => extract_from_node(summary_input),
        SummaryExpr::SummaryMerge { children } => children.iter().find_map(extract_from_node),
        // Not surfaced by any `Bind*` path yet (gated on rules that
        // haven't landed — see `deployment_expr.rs`'s module docs).
        SummaryExpr::SummaryJoin { .. }
        | SummaryExpr::SummarySubtract { .. }
        | SummaryExpr::SummaryDelete { .. }
        | SummaryExpr::KeepPreAsap(_) => None,
    }
}

/// Walk every entry in `registry`, look the metric up in `workload_store`,
/// run `planner::rules::bind_workload_typed` per workload, and assemble
/// the `metric_to_family` map that drives the 5-sketch
/// routing-connector emit path in `emit_edge_yaml_5sketch_routing`.
///
/// ASAPCollector#400 — SET semantics, NOT one-family-per-metric. A
/// metric can legitimately need MULTIPLE families because different
/// planned queries on the same metric require different capabilities
/// (`quantile_over_time` → DDSketch, `count`-distinct → HLL, `topk` →
/// CountSketch, …). We therefore collect the UNION of every workload
/// entry's committed sketch family per metric into a
/// `BTreeSet<SketchAlgorithm>` (deterministic order). The emitter routes the
/// metric to EACH family in its set and prunes pipelines/processors to
/// the union of all sets — eliminating the prior all-5 fan-out that
/// shipped sketch state through every family regardless of need.
///
/// Skipped:
///   - Metrics absent from `workload_store` (registry pre-pop failed).
///   - Workload entries where `bind_workload_typed` declines (raw
///     passthrough like `http_requests_total`, exact-required,
///     multi-intent) — those entries contribute no family. A metric
///     whose every entry declines is absent from the map entirely and
///     falls through to `metrics/raw_passthrough`, which is the
///     contract for raw / unsketched metrics.
///
/// The returned map drops directly into `EdgeStageConfig::metric_to_family`.
/// Empty map ⇒ caller falls back to legacy single-pipeline emit (the
/// `is_empty()` gate in `emit_edge_yaml`).
pub fn collect_metric_to_family(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> {
    let mut out: std::collections::HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
        std::collections::HashMap::new();
    for entry in registry.entries() {
        // B2 (metric, role) restructure: walk EVERY role registered for
        // this metric and accumulate the UNION of committed families.
        // Sum-shaped roles (raw passthrough / ExactAgg) decline
        // `bind_workload_typed` and contribute nothing — they fall
        // through to the routing-connector's default
        // `metrics/raw_passthrough` pipeline. Quantile / Cardinality /
        // Topk / Frequency roles each commit a family; a metric queried
        // by several capabilities accumulates several families, so its
        // samples fan into each per-family pipeline at the agent and the
        // backend serves every (metric, capability) the workload needs.
        for (_, workload, _wc) in workload_store.get_all_for_metric(&entry.metric_name) {
            // If this metric declares an `item_label` (its inner
            // high-cardinality dimension, e.g. "endpoint") and the
            // parsed query's own label filters name a value for it (e.g.
            // `{endpoint="checkout"}`), thread that through as the
            // `Frequency` intent's actual per-item filter -- see
            // `bind_workload_typed_with_item_filter`'s doc.
            let item_filter = entry.item_label.as_deref().and_then(|label| {
                workload
                    .label_filters
                    .get(label)
                    .map(|v| (label, v.as_str()))
            });
            let Some(deployment_expr) =
                crate::physical::workload_planner::bind_workload_typed_with_item_filter(
                    &workload,
                    item_filter,
                )
            else {
                continue;
            };
            if let Some(kind) = extract_root_sketch_algorithm(&deployment_expr) {
                out.entry(entry.metric_name.clone())
                    .or_default()
                    .insert(kind);
            }
        }
    }
    out
}

/// MVP blocker B3 — sibling of [`collect_metric_to_family`]: walk every
/// registry entry, look the workload up, and assemble a map from metric
/// name → the workload-spec `group_by_labels` list. Drops directly into
/// `EdgeStageConfig::metric_to_grouping_labels`.
///
/// The 5-sketch routing emitter prepends a `transform/keep_for_<metric>`
/// OTTL processor in front of each per-family sketch pipeline that
/// calls `keep_keys(datapoint.attributes, [...])` on the listed labels.
/// Without this the agent sketches with the full wire-attr tuple
/// (e.g. `{zone, rack, node, pod, endpoint, service.name,
/// telemetry.sdk.*}`) — one sid per unique tuple, defeating the
/// streaming-config's `grouping_labels` contract.
///
/// Metrics with an empty `group_by_labels` list are included with an
/// empty `Vec<String>` — that's the planner's signal that the
/// streaming-config wants a single global sid per metric. The emitter
/// handles empty by emitting `keep_keys(datapoint.attributes, [])`.
/// Metrics absent from `workload_store` are skipped; the emitter
/// treats absent entries as "no keep processor, attrs flow through".
pub fn collect_metric_to_grouping_labels(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut out = std::collections::HashMap::new();
    for entry in registry.entries() {
        // B2 (metric, role): the FIRST registered role's grouping
        // labels win — in practice all roles for a metric share the
        // same `grouping_labels` since the YAML field lives on the
        // WorkloadEntry. Using `get_all_for_metric().first()` keeps
        // pre-B2 semantics ("the entry the controller pre-popped first
        // wins") in the common case AND lets a multi-role metric still
        // emit a single keep_keys OTTL processor per metric.
        if let Some((_, workload, _)) = workload_store
            .get_all_for_metric(&entry.metric_name)
            .into_iter()
            .next()
        {
            out.insert(entry.metric_name.clone(), workload.group_by_labels.clone());
        }
    }
    out
}

/// Sibling of [`collect_metric_to_grouping_labels`]: walk every registry
/// entry and return the per-metric **sampling probability** map the L5
/// edge emitter drops into [`crate::physical::colored_dag::emitter::EdgeStageConfig::metric_to_sample_p`].
///
/// Only metrics whose workload sets a `sample_p` in `(0, 1)` are
/// included — `1.0` (the default / sampling-disabled) and out-of-range
/// values are skipped, so the map stays empty when no metric requests
/// sampling and the emitted agent config (hence the on-wire sketch bytes)
/// is byte-identical to the pre-sampling format. The edge emitter's
/// `insert_sample_p` re-guards the range defensively.
///
/// As with the sibling collectors, an entry is only honoured when its
/// metric was successfully pre-populated into the workload store, keeping
/// the emit aligned with what the backend knows about. When a metric
/// carries multiple roles the FIRST registered entry's `sample_p` wins
/// (in practice all share it, since the field lives on the WorkloadEntry).
///
/// A `p <= 0` or `p > 1` value is logged and skipped rather than emitted,
/// so a typo degrades to "no sampling" instead of a mis-scaled sketch.
///
/// NOTE: this is a static operator-set knob. A dynamic, optimizer-driven
/// `p` (tuned online against an accuracy/bandwidth budget from runtime
/// samples) is a deliberate follow-up and is out of scope here.
pub fn collect_metric_to_sample_p(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, f64> {
    let mut out = std::collections::HashMap::new();
    for entry in registry.entries() {
        if workload_store
            .get_all_for_metric(&entry.metric_name)
            .into_iter()
            .next()
            .is_none()
        {
            continue;
        }
        let p = entry.sample_p;
        if p >= 1.0 {
            // Sampling disabled (the default) — emit nothing so the wire
            // bytes stay byte-identical.
            continue;
        }
        if p <= 0.0 || !p.is_finite() {
            tracing::warn!(
                metric = %entry.metric_name,
                sample_p = p,
                "ignoring out-of-range sample_p (must be in (0, 1]); treating metric as unsampled"
            );
            continue;
        }
        out.entry(entry.metric_name.clone()).or_insert(p);
    }
    out
}

/// Sibling of [`collect_metric_to_sample_p`]: walk every registry entry and
/// return the per-metric **known distinct-key count per window** map the L5
/// edge emitter drops into
/// [`crate::physical::colored_dag::emitter::EdgeStageConfig::metric_to_distinct_keys`].
///
/// The value is the operator's declarative cardinality hint
/// ([`crate::workload::WorkloadEntry::distinct_keys_per_window`]) — the count
/// of distinct items the cardinality / frequency sketch families see per flush
/// window. The HLL branch of the fused `asap_edge` emitter uses it to refine
/// the sparse-vs-dense base decision: a per-series HLL above the in-memory
/// sparse→dense promotion crossover is emitted dense rather than sparse
/// (completing the PR #358 follow-up).
///
/// Only metrics whose workload sets `distinct_keys_per_window = Some(n)` are
/// included; entries that omit the hint (`None`) are SKIPPED, so the map stays
/// empty when no metric declares a cardinality and the emitted agent config
/// (hence the on-wire sketch bytes) is byte-identical to the PR #358 default
/// (per-series HLL ⇒ sparse).
///
/// As with the sibling collectors, an entry is only honoured when its metric
/// was successfully pre-populated into the workload store, keeping the emit
/// aligned with what the backend knows about. When a metric carries multiple
/// roles the FIRST registered entry's hint wins (in practice all share it,
/// since the field lives on the `WorkloadEntry`).
pub fn collect_metric_to_distinct_keys(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, u64> {
    let mut out = std::collections::HashMap::new();
    for entry in registry.entries() {
        if workload_store
            .get_all_for_metric(&entry.metric_name)
            .into_iter()
            .next()
            .is_none()
        {
            continue;
        }
        let Some(n) = entry.distinct_keys_per_window else {
            continue;
        };
        out.entry(entry.metric_name.clone()).or_insert(n);
    }
    out
}

/// Sibling of [`collect_metric_to_sample_p`]: walk every registry entry and
/// return a map from metric name → its declarative **inner item dimension**
/// (`WorkloadEntry::item_label`) that the L5 edge emitter drops into
/// [`crate::physical::colored_dag::emitter::EdgeStageConfig::metric_to_item_label`].
///
/// `item_label` is the data-point attribute whose VALUE is the "item" the
/// item-counting sketch families (HLL / CountSketch / CountMinSketch) count
/// or rank — e.g. `user_id` for `unique_users_per_min` (HLL), `endpoint`
/// for `top_endpoint_qps` (CountSketch) and `endpoint_request_freq` (CMS).
/// The emitter writes it onto the per-metric sketch entry as `item_label`
/// so the agent folds that high-cardinality attribute INTO the sketch
/// instead of leaving it in the sketch's series key (one cardinality-1 HLL
/// per `user_id` rather than one HLL per zone).
///
/// Only metrics whose workload declares a non-empty `item_label` are
/// included; a metric that omits it (or sets it empty) is skipped, so the
/// map stays empty for workloads that declare no inner dimension and the
/// emitted config is byte-identical to before (the CountSketch family still
/// falls back to its metric-name convention in that case).
///
/// As with the sibling collectors, an entry is only honoured when its
/// metric was successfully pre-populated into the workload store. When a
/// metric carries multiple roles the FIRST registered entry's `item_label`
/// wins (in practice all share it, since the field lives on the
/// `WorkloadEntry`).
pub fn collect_metric_to_item_label(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for entry in registry.entries() {
        if workload_store
            .get_all_for_metric(&entry.metric_name)
            .into_iter()
            .next()
            .is_none()
        {
            continue;
        }
        let Some(label) = entry.item_label.as_deref() else {
            continue;
        };
        let label = label.trim();
        if label.is_empty() {
            continue;
        }
        out.entry(entry.metric_name.clone())
            .or_insert_with(|| label.to_string());
    }
    out
}

/// Issue #298 — sibling of [`collect_metric_to_family`] /
/// [`collect_metric_to_grouping_labels`]: walk every registry entry and
/// return the deduped list of metrics whose workload(s) classify as
/// [`crate::workload::AggRole::Sum`] — bare-selector / `sum` / `rate`
/// / `increase` / `sum_over_time` / `irate`. These are the
/// Counter-shaped metrics whose OTel SDK emission defaults to
/// **cumulative** temporality and must be converted to **delta** before
/// the backend's `SumAccumulator` folds them, otherwise the
/// per-window sum is `Σ-of-cumulatives-in-window` (quadratic-in-time
/// blowup; cubic for instant `sum by (zone) (counter)` reads).
///
/// Drops directly into `EdgeStageConfig::cumulative_counter_metrics`,
/// which the 5-sketch routing emitter consumes to declare a
/// `cumulativetodelta` processor with `include.metrics = [...]` on the
/// entry pipeline. Empty list ⇒ no processor emitted (backward-compat
/// for quantile-only / sketch-only plans).
///
/// **A metric is included iff ANY of its registered roles classifies
/// as Sum**. This is the conservative direction: a metric with even
/// one Sum-shaped query needs delta conversion for that query to be
/// correct, and the OTel processor's `match_type: strict` filter then
/// gates which metrics the processor actually rewrites (every other
/// metric on the wire is a no-op pass-through). Gauge data points
/// carry no aggregation_temporality at all (it's a Counter-only
/// concept), so the processor leaves them untouched if a metric is
/// also used as a gauge elsewhere.
pub fn collect_cumulative_counter_metrics(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> Vec<String> {
    use crate::workload::{derive_agg_role, AggRole};
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for entry in registry.entries() {
        // `derive_agg_role` reads the WorkloadEntry directly (query
        // string + family override), not the lowered QueryWorkload, so
        // we classify the registry entry. We still consult the
        // workload_store to confirm the metric was successfully
        // pre-populated (matching the contract of the sibling
        // collectors) — silent skips for entries that failed the
        // pre-pop keep the emit aligned with what the backend actually
        // knows about.
        if workload_store
            .get_all_for_metric(&entry.metric_name)
            .into_iter()
            .next()
            .is_none()
        {
            continue;
        }
        if derive_agg_role(entry) == AggRole::Sum {
            seen.insert(entry.metric_name.clone());
        }
    }
    seen.into_iter().collect()
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn agent_runtime_from_header_recognises_three_values() {
        assert_eq!(
            AgentRuntime::from_header("asap-otel"),
            AgentRuntime::AsapOtel
        );
        assert_eq!(
            AgentRuntime::from_header("asap-otap"),
            AgentRuntime::AsapOtap
        );
        assert_eq!(
            AgentRuntime::from_header("asap-telegraf"),
            AgentRuntime::AsapTelegraf
        );
    }

    #[test]
    fn agent_runtime_from_header_short_aliases() {
        assert_eq!(AgentRuntime::from_header("otap"), AgentRuntime::AsapOtap);
        assert_eq!(
            AgentRuntime::from_header("telegraf"),
            AgentRuntime::AsapTelegraf
        );
    }

    #[test]
    fn agent_runtime_from_header_default_is_asap_otel() {
        assert_eq!(AgentRuntime::from_header(""), AgentRuntime::AsapOtel);
        assert_eq!(AgentRuntime::from_header("garbage"), AgentRuntime::AsapOtel);
    }

    #[test]
    fn cold_format_env_knob_opts_into_intchunk_and_derives_endpoint() {
        // The operator-facing SET path: `ASAP_COLD_FORMAT=intchunk` flips
        // the cold format to intchunk and derives the coldpart endpoint
        // from the cold ship endpoint (same merger host:port,
        // `/ingest/coldpart` path). Unset / `fragment` is a no-op.
        use crate::physical::colored_dag::emitter::{default_cold_ship_endpoint, ColdFormat};

        fn fixture() -> EdgeStageConfig {
            EdgeStageConfig {
                source_metric: None,
                label_filters: Vec::new(),
                window_secs: None,
                sketch_processors: Vec::new(),
                exporter_target: crate::physical::colored_dag::emitter::ExportTarget::Stage(
                    crate::physical::colored_dag::stage_id::StageId::Backend,
                ),
                prometheus_archive_metrics: Vec::new(),
                archive_tier_metrics: Vec::new(),
                warm_passthrough_metrics: Vec::new(),
                metric_to_family: std::collections::HashMap::new(),
                metric_to_grouping_labels: std::collections::HashMap::new(),
                cumulative_counter_metrics: Vec::new(),
                cold_ship_endpoint: Some(default_cold_ship_endpoint()),
                cold_external_labels: Vec::new(),
                metric_to_sample_p: std::collections::HashMap::new(),
                metric_to_distinct_keys: std::collections::HashMap::new(),
                metric_to_item_label: std::collections::HashMap::new(),
                cold_format: ColdFormat::default(),
                cold_coldpart_endpoint: None,
            }
        }

        // Unset ⇒ no-op (default fragment, no derived endpoint).
        {
            let _env = crate::test_support::EnvVarGuard::unset("ASAP_COLD_FORMAT");
            let mut cfg = fixture();
            apply_cold_format_from_env(&mut cfg);
            assert_eq!(cfg.cold_format, ColdFormat::Fragment);
            assert!(cfg.cold_coldpart_endpoint.is_none());
        }

        // `fragment` ⇒ no-op too.
        {
            let _env = crate::test_support::EnvVarGuard::set("ASAP_COLD_FORMAT", "fragment");
            let mut cfg = fixture();
            apply_cold_format_from_env(&mut cfg);
            assert_eq!(cfg.cold_format, ColdFormat::Fragment);
            assert!(cfg.cold_coldpart_endpoint.is_none());
        }

        // `intchunk` ⇒ flip + derive coldpart endpoint from ship endpoint.
        {
            let _env = crate::test_support::EnvVarGuard::set("ASAP_COLD_FORMAT", "intchunk");
            let mut cfg = fixture();
            apply_cold_format_from_env(&mut cfg);
            assert_eq!(cfg.cold_format, ColdFormat::Intchunk);
            assert_eq!(
                cfg.cold_coldpart_endpoint.as_deref(),
                Some("http://gorilla-merger:10908/ingest/coldpart"),
            );
        }
    }

    #[test]
    fn emit_for_runtime_default_matches_emit_edge_yaml() {
        // Serialize against the env-mutating tests in `stage_config`:
        // `emit_edge_yaml` reads `ASAP_EDGE_FUSED` and must observe the
        // default (unset) gate to emit the routing-connector shape.
        let _env = crate::test_support::env_lock();
        use crate::physical::colored_dag::emitter::{EdgeSketchProcessor, ExportTarget};
        use crate::physical::colored_dag::stage_id::StageId;
        use planner_types::post_asap::SketchParams;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "ddsketch".to_string(),
                sketch_algorithm: SketchAlgorithm::DDSketch,
                sketch_params: SketchParams::DDSketch { alpha: 0.01 },
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
            metric_to_distinct_keys: std::collections::HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let collector = emit_for_runtime(
            AgentRuntime::AsapOtel,
            &cfg,
            "ws://ctrl/v1/opamp",
            None,
            "test-agent",
        )
        .expect("collector emit ok");
        let direct =
            emit_edge_yaml(&cfg, "ws://ctrl/v1/opamp", "test-agent").expect("direct emit ok");
        assert_eq!(
            collector, direct,
            "AsapOtel dispatch must equal emit_edge_yaml"
        );
    }

    #[test]
    fn emit_for_runtime_otap_yields_dag_yaml() {
        use crate::physical::colored_dag::emitter::ExportTarget;
        use crate::physical::colored_dag::stage_id::StageId;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
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
            metric_to_distinct_keys: std::collections::HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };
        let yaml = emit_for_runtime(
            AgentRuntime::AsapOtap,
            &cfg,
            "ws://ctrl/v1/opamp",
            None,
            "test-agent",
        )
        .expect("otap emit ok");
        // OTAP-specific token.
        assert!(
            yaml.contains("otel_dataflow/v1"),
            "expected OTAP DAG version\n{yaml}"
        );
    }

    #[test]
    fn emit_for_runtime_telegraf_yields_toml() {
        use crate::physical::colored_dag::emitter::ExportTarget;
        use crate::physical::colored_dag::stage_id::StageId;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
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
            metric_to_distinct_keys: std::collections::HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };
        let toml = emit_for_runtime(
            AgentRuntime::AsapTelegraf,
            &cfg,
            "ws://ctrl/v1/opamp",
            None,
            "test-agent",
        )
        .expect("telegraf emit ok");
        // Telegraf-specific token.
        assert!(
            toml.contains("[[inputs.opentelemetry]]"),
            "expected Telegraf TOML header\n{toml}"
        );
    }

    // ── stitching-gap regression: registry walk binds all 6 contract metrics ──
    //
    // The 6 MVP contract metrics from `deploy/configs/mvp-workload.yaml` must
    // every one bind through `collect_metric_to_family` so the routing
    // table covers the full 5-sketch (DDSketch / KLL / HLL / CountSketch /
    // CountMinSketch) shape, with `http_requests_total` declining to raw.
    //
    // Reproduces the live demo gap: 3 of 6 (HLL, CountSketch, CMS) silently
    // drop because the analyzer pre-population path doesn't propagate
    // `sketch_family_override` from the workload YAML into
    // `QueryWorkload::sketch_type_override`.

    /// Mimics the pre-population loop in `main()` — turns each
    /// `WorkloadEntry` into a `QueryWorkload` via the shared `Analyzer`.
    fn populate_store_from_registry(registry: &WorkloadRegistry, store: &WorkloadStore) {
        use crate::pipeline::{Analyzer, QuerySpec};
        use crate::types;
        use crate::types_v2;
        let analyzer = Analyzer::new();
        for entry in registry.entries() {
            // Mirrors main.rs's QuerySpec construction post-B3/B4:
            // thread grouping_labels into group_by_labels; let the
            // parser drive time_window when query_string is present.
            let spec = QuerySpec {
                query_string: entry.query_string.clone(),
                metric_name: entry.metric_name.clone(),
                label_filters: Default::default(),
                group_by_labels: entry.grouping_labels.clone(),
                aggregations: vec!["quantile".into()],
                time_window: if entry.query_string.is_some() {
                    String::new()
                } else {
                    "5m".into()
                },
                repeat_every: None,
                accuracy_sla: entry.accuracy_sla,
                latency_sla: None,
                sketch_type: entry.sketch_family_override.clone(),
                workload: types::WorkloadCharacteristics::default(),
                id: None,
                language: None,
                accuracy: None,
                dollars: None,
                deployment_model: None,
                shape: types_v2::QueryShape::default(),
                data: types_v2::DataShape::default(),
            };
            if let Ok(wl) = analyzer.analyze(spec) {
                let role = crate::workload::derive_agg_role(entry);
                store.set(
                    &entry.metric_name,
                    role,
                    wl,
                    types::WorkloadCharacteristics::default(),
                );
            }
        }
    }

    #[test]
    fn collect_metric_to_family_binds_all_six_contract_metrics_from_live_yaml() {
        use planner_types::post_asap::SketchAlgorithm;

        // The 6 contract metrics reproduced inline (mirrors
        // deploy/configs/mvp-workload.yaml entries 1, 5, 6, 7, 8 plus the
        // raw-passthrough http_requests_total). Note we use the contract
        // metric name `http_latency_ms` (the live YAML uses
        // `http_requests_total_latency_ms` which falls back via AggType
        // → DDSketch — but it's the metric-name variant that exercises
        // classify_demo_metric for the DDSketch row).
        let yaml = r#"
- metric_name: http_latency_ms
  query_string: "quantile_over_time(0.99, http_latency_ms[1m])"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: http_requests_total
  query_string: "count(http_requests_total)"
  accuracy_sla: 0.0
  assign_to_role: agent
- metric_name: request_size_bytes
  query_string: "quantile_over_time(0.99, request_size_bytes[1m])"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: KLL
- metric_name: unique_users_per_min
  query_string: "count(unique_users_per_min)"
  accuracy_sla: 0.02
  assign_to_role: agent
  sketch_family_override: HLL
- metric_name: top_endpoint_qps
  query_string: "topk(5, top_endpoint_qps)"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: CountSketch
- metric_name: endpoint_request_freq
  query_string: "rate(endpoint_request_freq[5m])"
  accuracy_sla: 0.05
  assign_to_role: agent
  sketch_family_override: CountMinSketch
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        assert_eq!(entries.len(), 6, "all 6 contract metrics must deserialize");

        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let map = collect_metric_to_family(&registry, &store);

        // 5 sketched metrics + http_requests_total (raw, declines binding).
        // ASAPCollector#400: each value is now the SET of families the
        // metric needs. For THIS workload every sketched metric is
        // queried by exactly one capability, so each set has size 1.
        use std::collections::BTreeSet;
        let expected: Vec<(&str, Option<BTreeSet<SketchAlgorithm>>)> = vec![
            (
                "http_latency_ms",
                Some(BTreeSet::from([SketchAlgorithm::DDSketch])),
            ),
            ("http_requests_total", None), // raw passthrough
            (
                "request_size_bytes",
                Some(BTreeSet::from([SketchAlgorithm::Kll])),
            ),
            (
                "unique_users_per_min",
                Some(BTreeSet::from([SketchAlgorithm::Hll])),
            ),
            ("top_endpoint_qps", None),
            // `CountMinSketch` override re-derives statistic to
            // `Frequency`, `AggIntent::Extension`-shaped — now binds via
            // `ControlPlaneCostModel::realize_extension` (ASAPController#150,
            // see `physical::workload_planner::tests::typed_binding_endpoint_request_freq_binds_cms`).
            (
                "endpoint_request_freq",
                Some(BTreeSet::from([SketchAlgorithm::Cms])),
            ),
        ];
        for (metric, want) in &expected {
            let got = map.get(*metric).cloned();
            assert_eq!(
                got, *want,
                "metric {metric}: expected {want:?} in routing table, got {got:?}\n\
                 full map: {map:?}",
            );
        }
        // TopK also declines until a fresh membership-margin certificate is
        // supplied to the physical compiler.
        assert_eq!(
            map.len(),
            4,
            "routing table should have 4 evidence-valid sketch entries; raw passthrough and uncertified TopK decline, got: {map:?}"
        );
    }

    #[test]
    fn collect_metric_to_item_label_reads_workload_inner_dimension() {
        // Mirrors deploy/configs/mvp-workload.yaml's item-counting entries:
        // the HLL/CountSketch/CMS metrics declare an `item_label` (their
        // inner high-cardinality data-point attribute); the quantile metrics
        // declare none. The collector must surface exactly the declared
        // labels and skip metrics without one (byte-identical emit otherwise).
        let yaml = r#"
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
  sketch_family_override: KLL
- metric_name: unique_users_per_min
  query_string: "count(unique_users_per_min)"
  grouping_labels: [zone]
  sketch_family_override: HLL
  item_label: user_id
- metric_name: top_endpoint_qps
  query_string: "topk(5, top_endpoint_qps)"
  grouping_labels: [zone]
  sketch_family_override: CountSketch
  item_label: endpoint
- metric_name: endpoint_request_freq
  query_string: "rate(endpoint_request_freq[5m])"
  grouping_labels: [zone]
  sketch_family_override: CountMinSketch
  item_label: endpoint
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let map = collect_metric_to_item_label(&registry, &store);

        assert_eq!(
            map.get("unique_users_per_min").map(String::as_str),
            Some("user_id")
        );
        assert_eq!(
            map.get("top_endpoint_qps").map(String::as_str),
            Some("endpoint")
        );
        assert_eq!(
            map.get("endpoint_request_freq").map(String::as_str),
            Some("endpoint")
        );
        // The quantile metric declares no inner dimension → absent.
        assert!(!map.contains_key("http_requests_total_latency_ms"));
        assert_eq!(
            map.len(),
            3,
            "only the item-counting metrics carry item_label: {map:?}"
        );
    }

    #[test]
    fn collect_metric_to_distinct_keys_reads_workload_cardinality_hint() {
        // Mirrors collect_metric_to_item_label: only metrics that DECLARE a
        // `distinct_keys_per_window` surface in the map; entries that omit the
        // hint are skipped so the emit stays byte-identical to the PR #358
        // scope-based default for them.
        let yaml = r#"
- metric_name: distinct_users_high
  query_string: "count(distinct_users_high)"
  grouping_labels: [zone]
  sketch_family_override: HLL
  distinct_keys_per_window: 1000000
- metric_name: distinct_users_low
  query_string: "count(distinct_users_low)"
  grouping_labels: [zone]
  sketch_family_override: HLL
  distinct_keys_per_window: 50
- metric_name: distinct_users_unset
  query_string: "count(distinct_users_unset)"
  grouping_labels: [zone]
  sketch_family_override: HLL
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let map = collect_metric_to_distinct_keys(&registry, &store);

        assert_eq!(map.get("distinct_users_high").copied(), Some(1_000_000));
        assert_eq!(map.get("distinct_users_low").copied(), Some(50));
        // The metric that omits the hint is absent (skipped, not zero-filled).
        assert!(!map.contains_key("distinct_users_unset"));
        assert_eq!(
            map.len(),
            2,
            "only metrics declaring distinct_keys_per_window surface: {map:?}"
        );
    }

    /// ASAPCollector#400 — SET semantics at the resolution layer: a
    /// single metric queried by THREE distinct capabilities
    /// (quantile → DDSketch, cardinality → HLL, frequency → CMS) must
    /// accumulate ALL THREE families in its set, not just the first to
    /// bind. This is the multi-family-per-metric case the emitter must
    /// fan into three pipelines.
    ///
    /// We populate the store directly with three `(metric, role)`
    /// `QueryWorkload`s — one per capability — so the test pins
    /// `collect_metric_to_family`'s union semantics independently of the
    /// analyzer's query-string → AggType parsing.
    #[test]
    fn collect_metric_to_family_unions_multiple_capabilities_per_metric() {
        use crate::types::{AggType, QueryWorkload, SketchType, WorkloadCharacteristics};
        use crate::workload::AggRole;
        use planner_types::post_asap::SketchAlgorithm;
        use std::collections::BTreeSet;
        use std::time::Duration;

        const METRIC: &str = "http_requests";

        // The registry only needs ONE entry for the metric — the
        // collector iterates registry entries and, per metric, walks
        // EVERY role registered in the store. (Duplicate registry
        // entries for the same metric would just re-walk the same store
        // rows; one entry suffices.)
        let yaml = r#"
- metric_name: http_requests
  query_string: "quantile_over_time(0.99, http_requests[1m])"
  accuracy_sla: 0.01
  assign_to_role: agent
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);

        let store = WorkloadStore::new();
        let mk = |agg: AggType,
                  override_family: Option<SketchType>,
                  quantiles: Vec<f64>|
         -> QueryWorkload {
            QueryWorkload {
                metric_name: METRIC.to_string(),
                label_filters: Default::default(),
                group_by_labels: Vec::new(),
                aggregations: vec![agg],
                time_window: Duration::from_secs(60),
                repeat_every: None,
                accuracy_sla: 0.01,
                latency_sla: None,
                sketch_type_override: override_family,
                exact_required: false,
                quantiles,
            }
        };
        // Quantile → DDSketch (explicit override valid for Quantile).
        store.set(
            METRIC,
            AggRole::Quantile,
            mk(AggType::Quantile, Some(SketchType::DDSketch), vec![0.99]),
            WorkloadCharacteristics::default(),
        );
        // Cardinality → HLL (override valid for the Cardinality class).
        store.set(
            METRIC,
            AggRole::Count,
            mk(AggType::Cardinality, Some(SketchType::HLL), Vec::new()),
            WorkloadCharacteristics::default(),
        );
        // Frequency → CMS. This deployment's capability catalog exposes
        // CountSketch only for TopK, so the incompatible override is ignored;
        // importantly it does not rewrite this workload into TopK.
        store.set(
            METRIC,
            AggRole::Other,
            mk(
                AggType::Frequency,
                Some(SketchType::CountSketch),
                Vec::new(),
            ),
            WorkloadCharacteristics::default(),
        );

        let map = collect_metric_to_family(&registry, &store);
        let got = map
            .get(METRIC)
            .cloned()
            .unwrap_or_else(|| panic!("http_requests must be in the map\nmap: {map:?}"));
        assert_eq!(
            got,
            BTreeSet::from([
                SketchAlgorithm::DDSketch,
                SketchAlgorithm::Hll,
                SketchAlgorithm::Cms
            ]),
            "a metric queried by 3 capabilities must accumulate 3 families (UNION, not first-wins)\nmap: {map:?}"
        );
    }

    // ── B3 regression: WorkloadEntry.grouping_labels populates emit ───────
    //
    // Pre-B3 the WorkloadEntry YAML had no way to declare grouping
    // labels — the analyzer pulled them only from PromQL `by (...)`
    // clauses. Bare `quantile_over_time(0.99, metric[30s])` carries no
    // `by`, so `QueryWorkload.group_by_labels` ended up empty, so
    // `collect_metric_to_grouping_labels` returned `{metric: vec![]}`,
    // so the 5-sketch routing emitter wrote
    // `keep_keys(datapoint.attributes, [])` — stripping ALL attrs
    // instead of keeping `["zone"]`. Sid catalog ended up with one sid
    // per metric instead of one per (metric × zone).
    //
    // Post-B3 a declarative `grouping_labels: [zone]` on WorkloadEntry
    // is threaded through the pre-pop QuerySpec → analyzer →
    // QueryWorkload.group_by_labels → collect_metric_to_grouping_labels
    // → the emitter's keep_keys list. Without this round-trip the
    // end-to-end test's sid catalog stays empty-per-zone.
    #[test]
    fn workload_entry_grouping_labels_round_trip_through_emit_to_keep_keys() {
        let yaml = r#"
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
  accuracy_sla: 0.01
  assign_to_role: agent
  grouping_labels: ["zone"]
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].grouping_labels,
            vec!["zone".to_string()],
            "WorkloadEntry must surface grouping_labels from YAML"
        );

        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        // The analyzer must have threaded grouping_labels into
        // QueryWorkload.group_by_labels.
        let map = collect_metric_to_grouping_labels(&registry, &store);
        assert_eq!(
            map.get("http_requests_total_latency_ms"),
            Some(&vec!["zone".to_string()]),
            "collect_metric_to_grouping_labels must surface entry.grouping_labels — \
             without this the agent strips ALL attrs and the sid catalog ends up \
             with one sid per metric instead of one per (metric, zone)\nmap: {map:?}"
        );
    }

    /// Belt-and-braces companion: the emit-side keep_keys statement
    /// must contain the per-entry grouping_labels VERBATIM. Catches a
    /// regression where the pre-pop loop populates the workload store
    /// but the round-trip through the emitter drops the labels.
    #[test]
    fn workload_entry_grouping_labels_surface_in_emit_keep_keys_list() {
        // Serialize against env-mutating tests: `emit_edge_yaml` reads
        // `ASAP_EDGE_FUSED` and must observe the default (unset) gate.
        let _env = crate::test_support::env_lock();
        use crate::physical::colored_dag::emitter::{EdgeStageConfig, ExportTarget};
        use crate::physical::colored_dag::stage_id::StageId;
        use planner_types::post_asap::SketchAlgorithm;

        let yaml = r#"
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
  accuracy_sla: 0.01
  assign_to_role: agent
  grouping_labels: ["zone"]
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let mut edge_cfg = EdgeStageConfig {
            source_metric: Some("http_requests_total_latency_ms".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(30),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::from([(
                "http_requests_total_latency_ms".to_string(),
                std::collections::BTreeSet::from([SketchAlgorithm::DDSketch]),
            )]),
            metric_to_grouping_labels: std::collections::HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: std::collections::HashMap::new(),
            metric_to_distinct_keys: std::collections::HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };
        edge_cfg.metric_to_grouping_labels = collect_metric_to_grouping_labels(&registry, &store);

        let yaml_out =
            crate::emit::emit_edge_yaml(&edge_cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml_out.contains(
                "keep_keys(datapoint.attributes, [\"zone\"]) where metric.name == \"http_requests_total_latency_ms\""
            ),
            "keep_keys must list `zone` (NOT empty) for the YAML-declared grouping_labels\n{yaml_out}",
        );
        // Belt-and-braces: the bug surface is specifically
        // `keep_keys(..., [])`. Make sure we don't accidentally emit
        // the empty-list form for this metric.
        assert!(
            !yaml_out.contains(
                "keep_keys(datapoint.attributes, []) where metric.name == \"http_requests_total_latency_ms\""
            ),
            "empty keep_keys would strip all attrs and break per-zone sid splitting\n{yaml_out}",
        );
    }

    // ── Issue #298 — collect_cumulative_counter_metrics ────────────────────

    /// Workload with mixed roles — a bare counter selector, a `sum by
    /// (...)` over a counter, and a quantile gauge. Only the first two
    /// classify as `AggRole::Sum`; the gauge query is `AggRole::Quantile`
    /// and must NOT appear in the output. The two Sum entries refer to
    /// the SAME metric (`http_requests_total`), so the helper dedupes.
    #[test]
    fn issue298_collect_cumulative_counter_metrics_picks_sum_role_dedup() {
        let yaml = r#"
- metric_name: http_requests_total
  query_string: "http_requests_total"
  accuracy_sla: 0.0
  assign_to_role: agent
- metric_name: http_requests_total
  query_string: "sum by (zone) (http_requests_total)"
  accuracy_sla: 0.0
  assign_to_role: gateway
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: endpoint_request_freq
  query_string: "rate(endpoint_request_freq[5m])"
  accuracy_sla: 0.05
  assign_to_role: agent
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let counters = collect_cumulative_counter_metrics(&registry, &store);
        assert_eq!(
            counters,
            vec![
                "endpoint_request_freq".to_string(),
                "http_requests_total".to_string(),
            ],
            "expected the Sum-role metrics deduped + sorted; the \
             quantile_over_time entry on http_requests_total_latency_ms \
             must NOT appear (it's AggRole::Quantile)"
        );
    }

    /// Workload with zero Sum-shaped entries (all quantile / cardinality)
    /// produces an empty list — the emitter then skips the
    /// `cumulativetodelta` processor entirely (backward-compat for
    /// quantile-only deployments).
    #[test]
    fn issue298_collect_cumulative_counter_metrics_empty_for_quantile_only_workload() {
        let yaml = r#"
- metric_name: http_requests_total_latency_ms
  query_string: "quantile_over_time(0.99, http_requests_total_latency_ms[30s])"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: unique_users_per_min
  query_string: "count(unique_users_per_min)"
  accuracy_sla: 0.02
  assign_to_role: agent
"#;
        let entries: Vec<crate::workload::WorkloadEntry> =
            serde_yaml::from_str(yaml).expect("parse workload yaml");
        let registry = crate::workload::WorkloadRegistry::from_entries(entries);
        let store = WorkloadStore::new();
        populate_store_from_registry(&registry, &store);

        let counters = collect_cumulative_counter_metrics(&registry, &store);
        assert!(
            counters.is_empty(),
            "quantile / cardinality entries must not be classified as \
             cumulative counters; got {counters:?}"
        );
    }
}
