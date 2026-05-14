//! `emit/` — per-deployment-model plan emitters (L5 output side).
//!
//! Per `control_plane/docs/design.md` §5 `core::emit`. The 2026-05
//! layered-cleanup refactor consolidated the former
//! `controller/src/config/` directory here. Mapping:
//!
//! | Old path | New path |
//! |---|---|
//! | `config/agent.rs` | [`agent`] |
//! | `config/backend.rs` | [`backend`] |
//! | `config/asapquery_backend.rs` | [`asapquery_backend`] |
//! | `config/precompute.rs` | [`precompute`] |
//! | `config/stage_config.rs` | [`stage_config`] (TODO: split into `opamp` + `streaming_config` + `inference_config` per design.md §5; deferred from refactor 2026-05 because the 3,020-line monolith mixes OTel-collector YAML emit, ASAPQuery-backend JSON emit, and shared internals — clean split needs ownership reorganisation, not file renames) |
//! | `config/stage_config_otap.rs` | [`otap`] |
//! | `config/stage_config_telegraf.rs` | [`telegraf`] |
//! | `config/workloads.rs` | [`crate::workload`] (top-level — design.md §5 puts `workload` next to `emit`, not inside it) |

pub mod agent;
pub mod asapquery_backend;
pub mod backend;
pub mod otap;
pub mod precompute;
pub mod stage_config;
pub mod telegraf;
pub mod trait_def;

pub use agent::generate_agent_config;
pub use asapquery_backend::generate_streaming_config_yaml;
pub use backend::{generate_backend_config, generate_backend_config_staged};
pub use otap::emit_otap_dag_yaml;
pub use precompute::{build_precompute_jobs, should_precompute, PrecomputeClient};
pub use stage_config::{
    emit_backend_config_json, emit_backend_storage_routing,
    emit_backend_storage_routing_for_tenant, emit_backend_storage_routing_with_prometheus,
    emit_backend_storage_routing_with_prometheus_for_tenant, emit_edge_yaml, emit_gateway_yaml,
    DEFAULT_TENANT,
};
pub use telegraf::emit_telegraf_toml;
pub use trait_def::{
    InferenceConfigEmitter, InferenceConfigInput, OpampEmitter, OpampGatewayEmitter,
    PlanEmitter, StreamingConfigEmitter,
};

// Refactor 2026-05: design.md §5 puts `WorkloadRegistry` next to
// `emit`, not inside it. The new home is `crate::workload`; we re-export
// here so historical `crate::config::WorkloadRegistry` and
// `control_plane::config::WorkloadRegistry` references via the `config`
// back-compat alias keep working without churn.
pub use crate::workload::WorkloadRegistry;

use crate::physical::colored_dag::emitter::EdgeStageConfig;
use crate::sketch_algebra::params::SketchKind;
use crate::sketch_algebra::PhysicalExpr;
use crate::store::WorkloadStore;
use anyhow::Result;

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
) -> Result<String> {
    match runtime {
        AgentRuntime::AsapOtel => emit_edge_yaml(cfg, opamp_endpoint),
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
// `extract_root_sketch_kind` walks a `PhysicalExpr` tree and returns the
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
pub fn extract_root_sketch_kind(expr: &PhysicalExpr) -> Option<SketchKind> {
    match expr {
        PhysicalExpr::SketchAgg { sketch_type, .. } => Some(sketch_type.clone()),
        PhysicalExpr::RawAtEdgeSketchAtBackend { family, .. } => Some(family.clone()),
        PhysicalExpr::SketchEstimate { child, .. } => extract_root_sketch_kind(child),
        PhysicalExpr::SketchMerge { children, .. } => {
            children.iter().find_map(extract_root_sketch_kind)
        }
        PhysicalExpr::LetBinding { expr, child, .. } => {
            extract_root_sketch_kind(expr).or_else(|| extract_root_sketch_kind(child))
        }
        PhysicalExpr::Logical(_)
        | PhysicalExpr::Ref { .. }
        | PhysicalExpr::RawAtEdgePrometheusArchive { .. }
        // ExactAgg has no sketch family — it produces an exact
        // aggregation accumulator, not a sketch state. The
        // routing emitter routes these to the
        // `metrics/raw_passthrough` / exact-precompute pipeline
        // alongside Logical pass-throughs.
        | PhysicalExpr::ExactAgg { .. } => None,
    }
}

/// Walk every entry in `registry`, look the metric up in `workload_store`,
/// run `planner::rules::bind_workload_typed` per workload, and assemble
/// the `metric_to_family` HashMap that drives the 5-sketch
/// routing-connector emit path in `emit_edge_yaml_5sketch_routing`.
///
/// Skipped:
///   - Metrics absent from `workload_store` (registry pre-pop failed).
///   - Metrics where `bind_workload_typed` declines (raw passthrough
///     like `http_requests_total`, exact-required, multi-intent).
///     These fall through to `metrics/raw_passthrough` in the emitter,
///     which is the contract for raw / unsketched metrics.
///
/// The returned map drops directly into `EdgeStageConfig::metric_to_family`.
/// Empty map ⇒ caller falls back to legacy single-pipeline emit (the
/// `is_empty()` gate in `emit_edge_yaml`).
pub fn collect_metric_to_family(
    registry: &WorkloadRegistry,
    workload_store: &WorkloadStore,
) -> std::collections::HashMap<String, SketchKind> {
    let mut out = std::collections::HashMap::new();
    for entry in registry.entries() {
        let Some((workload, _wc)) = workload_store.get(&entry.metric_name) else {
            continue;
        };
        let Some(physical_expr) = crate::optimizer::rules::bind_workload_typed(&workload) else {
            // `http_requests_total` and other raw-passthrough metrics
            // land here — correctly excluded so they fall through to
            // the routing connector's default `metrics/raw_passthrough`
            // pipeline in the emitter.
            continue;
        };
        if let Some(kind) = extract_root_sketch_kind(&physical_expr) {
            out.insert(entry.metric_name.clone(), kind);
        }
    }
    out
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
    fn emit_for_runtime_default_matches_emit_edge_yaml() {
        use crate::physical::colored_dag::emitter::{EdgeSketchProcessor, ExportTarget};
        use crate::physical::colored_dag::stage_id::StageId;
        use crate::sketch_algebra::params::SketchKind;
        use crate::sketch_algebra::params::{DDSketchParams, SketchParams};

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
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
        };

        let collector = emit_for_runtime(AgentRuntime::AsapOtel, &cfg, "ws://ctrl/v1/opamp", None)
            .expect("collector emit ok");
        let direct = emit_edge_yaml(&cfg, "ws://ctrl/v1/opamp").expect("direct emit ok");
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
        };
        let yaml = emit_for_runtime(AgentRuntime::AsapOtap, &cfg, "ws://ctrl/v1/opamp", None)
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
        };
        let toml = emit_for_runtime(AgentRuntime::AsapTelegraf, &cfg, "ws://ctrl/v1/opamp", None)
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
            let spec = QuerySpec {
                query_string: entry.query_string.clone(),
                metric_name: entry.metric_name.clone(),
                label_filters: Default::default(),
                group_by_labels: vec![],
                aggregations: vec!["quantile".into()],
                time_window: "5m".into(),
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
                store.set(
                    &entry.metric_name,
                    wl,
                    types::WorkloadCharacteristics::default(),
                );
            }
        }
    }

    #[test]
    fn collect_metric_to_family_binds_all_six_contract_metrics_from_live_yaml() {
        use crate::sketch_algebra::params::SketchKind;

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
        let expected: Vec<(&str, Option<SketchKind>)> = vec![
            ("http_latency_ms", Some(SketchKind::DDSketch)),
            ("http_requests_total", None), // raw passthrough
            ("request_size_bytes", Some(SketchKind::Kll)),
            ("unique_users_per_min", Some(SketchKind::Hll)),
            ("top_endpoint_qps", Some(SketchKind::CountSketch)),
            ("endpoint_request_freq", Some(SketchKind::Cms)),
        ];
        for (metric, want) in &expected {
            let got = map.get(*metric).cloned();
            assert_eq!(
                got, *want,
                "metric {metric}: expected {want:?} in routing table, got {got:?}\n\
                 full map: {map:?}",
            );
        }
        // Routing table covers all 5 sketched metrics.
        assert_eq!(
            map.len(),
            5,
            "routing table should have 5 entries (5 sketches; raw declines), got: {map:?}"
        );
    }
}
