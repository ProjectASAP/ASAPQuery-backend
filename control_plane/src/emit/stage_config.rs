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
//!   ASAPQuery-backend `POST /api/v1/streaming-config` API surface,
//!   sourced from the typed [`BackendStageConfig`]. The legacy
//!   `generate_streaming_config_yaml` `CollectionPlan`-shaped emitter
//!   was retired in the Option B unification (see
//!   [`crate::emit::backend_push`]).
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
//! All three are pure transformations: no I/O. The `opamp_endpoint`
//! parameter is the controller's WebSocket URL the emitted YAML's
//! `extensions.opamp` block must point at; the caller threads it
//! through from `AppState::opamp_endpoint`. The `agent_id` parameter
//! is the identity the agent presents in the `X-Agent-ID` WS header
//! when it reconnects after a controller-pushed restart (Issue #2 —
//! without this header the controller's OpAMP server can't re-identify
//! the agent). Broadcast callers that don't have a single agent in
//! scope pass the literal placeholder `"$AGENT_ID"` and rely on the
//! agent container's env to expand it at boot.
//!
//! NOTE: the memory_limiter soft threshold the 5-sketch routing path
//! emits can be tuned via the controller's `ASAP_AGENT_MEMORY_LIMIT_MIB`
//! env var (default 1280 MiB). Operators bumping the agent container's
//! cgroup limit raise both together. See `emit_edge_yaml_5sketch_routing`.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use serde_yaml::{Mapping, Value};
use std::collections::{BTreeMap, HashMap};

use crate::physical::colored_dag::emitter::{
    coldpart_endpoint_from_ship, default_cold_external_labels, default_cold_ship_endpoint,
    AggregationInput, BackendAggregation, BackendReadout, BackendStageConfig, ColdFormat,
    EdgeSketchProcessor, EdgeStageConfig, ExportTarget, GatewayMergeProcessor, GatewayStageConfig,
};
// `ArchiveTierMetric` / `PrometheusArchiveMetric` are referenced ONLY by the
// `#[cfg(test)]` module below (test fixtures construct edge configs with
// archive-tier metric lists). Importing them at module scope produced an
// unused-import warning on every non-test build, so they're scoped into the
// test module's `use super::*` instead (P2-5).
use crate::physical::colored_dag::stage_id::StageId;
use planner_types::post_asap::{
    ExactKind, SketchAlgorithm, SketchParams, SketchQuery, SummaryFamilyType,
};
use planner_types::pre_asap::ColumnRef;
// `BackendAggregation.sketch_algorithm`/`.sketch_params` span both exact
// accumulators and approximate sketches -- see
// `physical::colored_dag::emitter`'s `use asap_types::{...}` note.

// ── YAML structural types ─────────────────────────────────────────────────────
//
// These mirror the structural types in `config::agent`. We keep a
// private copy here rather than re-exporting because the L5 typed path
// has slightly different shape constraints (e.g. no `series_id_ttl` on
// the receiver block — that's a wire-layer concern Phase G+ owns).

#[derive(Serialize)]
struct CollectorYaml {
    // BTreeMaps (not HashMaps) so serde_yaml emits in deterministic
    // alphabetical key order. With HashMap, Rust's randomized
    // iteration produced byte-different YAML on every call to the
    // emit functions — which broke the agent's opampextension
    // byte-level no-op check (ASAPCollector#381 follow-up), causing
    // the agent to apply+restart on every push of the SAME semantic
    // config. Generating deterministic YAML at the source matches
    // the rest of the controller's content-addressed identity story
    // (PolicyFingerprint, SeriesIdResolver, etc.).
    extensions: BTreeMap<String, Value>,
    receivers: BTreeMap<String, Value>,
    processors: BTreeMap<String, Value>,
    /// OTel collector v0.106+ ships the `routing` component as a
    /// **connector**, not a processor (`routingprocessor` was
    /// deprecated and removed). Connectors live in their own
    /// top-level block and are referenced as both an exporter (entry
    /// pipeline) and a receiver (each downstream pipeline).
    /// Empty for legacy single-pipeline / Mode-3 / warm-passthrough
    /// emit paths — preserved by `skip_serializing_if` so the YAML
    /// shape doesn't gain an empty `connectors: {}` block.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    connectors: BTreeMap<String, Value>,
    exporters: BTreeMap<String, Value>,
    service: ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    extensions: Vec<String>,
    pipelines: BTreeMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers: Vec<String>,
    processors: Vec<String>,
    exporters: Vec<String>,
}

// ── Window-size clamping (MVP blocker B4) ─────────────────────────────────────
//
// The controller derives `window_secs` from the workload's matrix-selector
// range (`metric[30s]` → 30s). Two bounds keep the emitted value sane:
//
//   * Lower bound 5s — below this the sketch processor mints a new
//     window before it has enough samples for the family's quality
//     guarantees, and the per-flush cardinality on the sid catalog
//     explodes (one (sid, window) row per few seconds).
//   * Upper bound 60s — above this the user's query range no longer
//     contains a closed sketch window, and replay queries return NoData
//     while the warm tier still owns the metric. 60s is also the
//     historical default the legacy single-pipeline emitter shipped with,
//     so clamping here preserves backwards-compat for plans without an
//     explicit range.
//
// `None` means "no `Window` node in the typed L5 — agent runs in batch
// mode, no window_duration in the YAML"; we pass that straight through.
//
// Centralised here so [`build_edge_processor_block`] (sketch processor
// `window_duration`) and the [`BackendAggregation`] consumer
// ([`emit_backend_streaming_config_json`]) clamp to the same bounds. The
// downstream backend's reducer keys windows by the emitted value, so
// the two MUST agree or replay-vs-warm answers go out of sync.

/// Lower bound for [`clamp_window_secs`].
pub const MIN_WINDOW_SECS: u64 = 5;

/// Upper bound for [`clamp_window_secs`]. Matches the legacy default
/// the pre-B4 emitter shipped with.
pub const MAX_WINDOW_SECS: u64 = 60;

/// Cardinality at which a per-series HLL is emitted DENSE rather than sparse
/// (ASAPCollector#472 follow-up to PR #358).
///
/// The sketchlib-go in-memory sparse HLL base (`NewHLLWrapperSparse`)
/// auto-promotes to the dense register array once roughly this many registers
/// become non-zero (the sparse representation stops saving memory past that
/// point). A per-series HLL whose known distinct-key count
/// ([`crate::workload::WorkloadEntry::distinct_keys_per_window`]) is at or
/// above this crossover would promote almost immediately, so starting it sparse
/// only pays one-time promotion churn — we emit it dense instead.
///
/// This is a HEURISTIC: distinct *keys* map to non-zero *registers* only
/// approximately (hash collisions mean registers < keys at high cardinality),
/// so the crossover is fuzzy. Being slightly off has NO correctness or accuracy
/// impact — the sparse base is lossless and serializes byte-identically to
/// dense for the same inputs; an over- or under-estimate at worst costs (or
/// saves) a single in-memory sparse→dense promotion. The value tracks the
/// in-memory promotion threshold (~4096 non-zero registers); the wire-crossover
/// constant the agent uses elsewhere is larger (~6000).
pub const DENSE_CROSSOVER: u64 = 4096;

/// Clamp a derived window size to `[MIN_WINDOW_SECS, MAX_WINDOW_SECS]`.
/// `None` is preserved as `None` so callers can keep the
/// "no-window / batch-mode" branch distinguishable from a clamped value.
pub fn clamp_window_secs(w: Option<u64>) -> Option<u64> {
    w.map(|s| s.clamp(MIN_WINDOW_SECS, MAX_WINDOW_SECS))
}

/// Process-level gate selecting the FUSED single-pipeline `asap_edge`
/// edge wire shape (issue #46) over the legacy `routing`-connector
/// per-family fan-out.
///
/// **Default OFF** so the established 5-sketch routing emit (and its
/// large unit-test surface) is unchanged for callers who haven't
/// migrated the agent build yet. Set `ASAP_EDGE_FUSED=1` (or
/// `true` / `yes`) on the controller process to switch every
/// `metric_to_family`-populated edge config to the fused
/// `[memory_limiter, cumulativetodelta, asap_edge]` pipeline that the
/// new fused agent processor consumes.
///
/// We gate on an env var (mirroring `typed_stage_split_enabled()` /
/// `ASAP_AGENT_MEMORY_LIMIT_MIB`) rather than a new `EdgeStageConfig`
/// field so the change is additive: no struct-literal churn across the
/// ~17 construction sites, no serde wire-shape bump, and the two emit
/// paths read the IDENTICAL `cfg` fields. The flag is the canonical
/// migration switch — once the fused agent build is the default
/// deployment the gate's default flips to ON (and the routing path is
/// retired).
pub fn fused_asap_edge_enabled() -> bool {
    matches!(
        std::env::var("ASAP_EDGE_FUSED").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
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
pub fn emit_edge_yaml(
    cfg: &EdgeStageConfig,
    opamp_endpoint: &str,
    agent_id: &str,
) -> Result<String> {
    // ── MVP §46: 5-sketch routing-connector dispatch ───────────────────────
    //
    // When the planner has populated `cfg.metric_to_family` (the per-metric
    // → SketchAlgorithm table sourced from the workload spec), we switch to the
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
    //
    // Issue #46 — the agent now runs ONE fused `asap_edge` processor in a
    // single pipeline instead of the routing-connector per-family
    // fan-out. When `ASAP_EDGE_FUSED` is set we emit THAT shape; the
    // legacy routing emit stays the default until the fused agent build
    // is the default deployment (see `fused_asap_edge_enabled`).
    if !cfg.metric_to_family.is_empty() {
        if fused_asap_edge_enabled() {
            return emit_edge_yaml_asap_edge(cfg, opamp_endpoint, agent_id);
        }
        return emit_edge_yaml_5sketch_routing(cfg, opamp_endpoint, agent_id);
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
    let mut processors: BTreeMap<String, Value> = BTreeMap::new();
    let mut sketch_pipeline_processors: Vec<String> = Vec::new();
    for sp in &cfg.sketch_processors {
        // MVP blocker B4: clamp `window_secs` to [5, 60] so the agent's
        // sketch processor's `window_duration` always sits inside the
        // user's query range. Without this, `metric[5m]` lands a
        // 300s window which is larger than any sensible replay range
        // and produces NoData under `quantile_over_time`.
        let block = build_edge_processor_block(
            sp,
            clamp_window_secs(cfg.window_secs),
            &cfg.label_filters,
            cfg.source_metric.as_deref(),
            cfg.source_metric
                .as_deref()
                .and_then(|m| cfg.metric_to_sample_p.get(m).copied()),
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
        let gorillas3_yaml = build_gorillas3_yaml(window_secs);
        let gorillas3: Value =
            serde_yaml::from_str(&gorillas3_yaml).context("parse gorillas3 processor block")?;
        processors.insert("gorillas3".to_string(), gorillas3);
    }

    // ── MVP blocker B3: per-metric attribute-allowlist for legacy path ─────
    //
    // The legacy `emit_edge_yaml` (non-routing) shape carries ONE source
    // metric (`cfg.source_metric`), not the per-metric routing table the
    // 5-sketch shape uses. If the controller has populated
    // `cfg.metric_to_grouping_labels` for `source_metric`, prepend a
    // `transform/keep_for_<sanitized_metric>` OTTL processor in front
    // of the sketch processor so the agent strips wire attrs to the
    // streaming-config's `grouping_labels` BEFORE sketching.
    let legacy_keep_proc_name: Option<String> = cfg.source_metric.as_deref().and_then(|m| {
        cfg.metric_to_grouping_labels.get(m).map(|labels| {
            let name = transform_keep_processor_name(m);
            let block = build_transform_keep_processor_block(m, labels);
            processors.insert(name.clone(), block);
            name
        })
    });

    // Pipeline-processor list for the ASAP-tier path. Order matches
    // `asap-otel-agent-b6-asap-single-sketch.yaml`: gorillas3 runs FIRST
    // so the cold-tier write happens on the raw sample BEFORE the sketch
    // processor mutates / suffix-renames the metric stream. The
    // `transform/keep_for_*` allowlist sits between gorillas3 and the
    // sketch so the cold tier retains full wire attrs while the sketch
    // only ever sees the reduced label set (MVP blocker B3).
    let asap_tier_processors: Vec<String> = {
        let mut v = Vec::new();
        if has_archive_tier {
            v.push("gorillas3".to_string());
        }
        if let Some(name) = &legacy_keep_proc_name {
            v.push(name.clone());
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
    let (exporter_key, exporter_val) = build_otlp_exporter("data-plane", &cfg.exporter_target);

    let mut exporters: BTreeMap<String, Value> = [(exporter_key.clone(), exporter_val)].into();
    let mut pipelines: BTreeMap<String, Pipeline> = BTreeMap::new();

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
    //
    // Issue #2: include `X-Agent-ID` in the ws headers so the agent
    // re-presents the same identity to the controller's OpAMP server
    // after a Docker restart (the on_connect handler keys on this
    // header). Without it, `/api/v1/agents` is empty post-restart and
    // the controller can't push config to the orphaned agent.
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n    headers:\n      X-Agent-ID: \"{agent_id}\"\nremote_config_path: /etc/otel/config.yaml\n"
    ))
    .context("parse opamp extension block")?;

    // ── Top-level YAML ────────────────────────────────────────────────────────
    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        // Legacy emit paths don't use the routing connector — see the
        // MVP §46 dispatch at the top of `emit_edge_yaml`.
        connectors: BTreeMap::new(),
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
pub fn emit_gateway_yaml(
    cfg: &GatewayStageConfig,
    opamp_endpoint: &str,
    agent_id: &str,
) -> Result<String> {
    // Receiver — port from cfg, both gRPC + HTTP.
    let port = cfg.otlp_receiver_port;
    let otlp_receiver: Value = serde_yaml::from_str(&format!(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:{port}\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:{}\"\n",
        port + 1,
    ))
    .context("parse gateway OTLP receiver block")?;

    // Processors — one merge processor per merge entry. Naming
    // convention matches the patched contrib build:
    //   * SketchAlgorithm::DDSketch    → `ddsketchmerge`
    //   * SketchAlgorithm::Kll         → `kllmerge`
    //   * SketchAlgorithm::Hll         → `hllmerge`
    //   * SketchAlgorithm::Cms         → `countminsketchmerge`
    //   * SketchAlgorithm::CountSketch → `countsketchmerge`
    //
    // We honour `GatewayMergeProcessor::processor_name` if non-empty
    // (the typed emitter today populates it as `"sketchmergeprocessor"`
    // — a placeholder until Phase C flips factory names per-family),
    // otherwise we derive the family-specific name from `sketch_kind`.
    let mut processors: BTreeMap<String, Value> = BTreeMap::new();
    let mut pipeline_processors: Vec<String> = Vec::new();
    for mp in &cfg.merge_processors {
        let key = gateway_merge_processor_name(mp);
        let block = build_gateway_merge_block(mp);
        processors.insert(key.clone(), block);
        pipeline_processors.push(key);
    }

    // Exporter — backend OTLP.
    let (exporter_key, exporter_val) = build_otlp_exporter("data-plane", &cfg.exporter_target);

    // Issue #2: gateway also needs X-Agent-ID so its OpAMP-pushed
    // reconnect re-identifies to the controller.
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n    headers:\n      X-Agent-ID: \"{agent_id}\"\nremote_config_path: /etc/otel/config.yaml\n"
    ))
    .context("parse opamp extension block")?;

    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        // Gateway stage doesn't use the routing connector.
        connectors: BTreeMap::new(),
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
/// Output shape: a top-level `aggregations` array of
/// `{ aggregationType, aggregationSubType, metric, labels, parameters,
/// windowSize, windowType, spatialFilter, aggregationInput }` rows.
/// (The legacy `generate_streaming_config_yaml` YAML emitter that
/// shipped the same shape from a `CollectionPlan` was retired in the
/// Option B unification — see [`crate::emit::backend_push`].)
/// `aggregationId` is **not** emitted — identity is content-addressed in
/// the backend via `PolicyFingerprint(u64)`.
/// We additionally surface a parallel `readouts` array so the backend's
/// query engine can prepare per-readout dispatch entries up-front (the
/// existing YAML form has no readouts list because the legacy planner
/// materialises one aggregation per metric and infers readouts from the
/// PromQL query at execution time; Phase B's typed `BackendStageConfig`
/// carries the readouts explicitly, so we ship them too — backends that
/// don't recognise the field will ignore it without erroring).
pub fn emit_backend_streaming_config_json(
    cfg: &BackendStageConfig,
    monitors: &[crate::emit::monitor::MonitorIntent],
) -> Result<JsonValue> {
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

    let mut doc = json!({
        "aggregations": aggregations,
        "readouts": readouts,
    });
    // Continuous-monitoring (CDM) specs: only present the `monitors` key when
    // the workload declared at least one, so configs without monitors stay
    // byte-identical to before (the backend's MonitorSpec list defaults empty).
    if !monitors.is_empty() {
        let entries: Vec<JsonValue> = monitors
            .iter()
            .map(crate::emit::monitor::streaming_config_monitor_entry)
            .collect();
        doc.as_object_mut()
            .expect("json object")
            .insert("monitors".to_string(), JsonValue::Array(entries));
    }
    Ok(doc)
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
    let algorithms: Vec<&SketchAlgorithm> = cfg
        .aggregations
        .iter()
        .filter_map(|a| match &a.family {
            SummaryFamilyType::Sketch(kind, _) => Some(kind.algorithm()),
            _ => None,
        })
        .collect();

    // Sketch-eligible shapes — the ASAP tier serves these natively
    // because we planned a sketch for them.
    let mut warm_shapes: Vec<&'static str> = Vec::new();
    let has_quantile_sketch = algorithms
        .iter()
        .any(|k| matches!(k, SketchAlgorithm::DDSketch | SketchAlgorithm::Kll));
    if has_quantile_sketch {
        warm_shapes.push("quantile");
        warm_shapes.push("quantile_over_time");
    }
    let has_hll = algorithms.iter().any(|k| matches!(k, SketchAlgorithm::Hll));
    if has_hll {
        warm_shapes.push("count");
    }
    // Heap-bearing kinds count too — `SummaryKind` (unlike the retired
    // `physical::post_asap::SummaryKind`) promotes `with_heap` to a distinct
    // identity variant, but a topk-bound Count-Sketch/CMS aggregation
    // still needs to register here exactly as it did before the split.
    let has_count_sketch = algorithms.iter().any(|k| {
        matches!(
            k,
            SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap
        )
    });
    if has_count_sketch {
        warm_shapes.push("topk");
    }
    let has_cms = algorithms
        .iter()
        .any(|k| matches!(k, SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap));
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
    if !algorithms.is_empty() {
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
fn emit_edge_yaml_5sketch_routing(
    cfg: &EdgeStageConfig,
    opamp_endpoint: &str,
    agent_id: &str,
) -> Result<String> {
    use planner_types::post_asap::SketchAlgorithm;

    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    )
    .context("parse static OTLP receiver block")?;

    // ── Required sketch families (ASAPCollector#400) ───────────────────────
    //
    // Compute the UNION of families across every metric's set. Only
    // these families get a processor block and a per-family pipeline —
    // this is the bandwidth fix: the prior emitter loaded all 5 families
    // and routed every metric through all 5 pipelines, shipping ~5×
    // the sketch state. Now a workload whose metrics only need DDSketch
    // ships ONLY the DDSketch pipeline.
    //
    // The canonical 5-family order below is the iteration order for
    // every emit (processors, pipelines, hints) so the YAML is stable
    // across controller runs regardless of HashMap iteration order.
    const FAMILY_ORDER: [SketchAlgorithm; 5] = [
        SketchAlgorithm::DDSketch,
        SketchAlgorithm::Kll,
        SketchAlgorithm::Hll,
        SketchAlgorithm::CountSketch,
        SketchAlgorithm::Cms,
    ];
    let mut needed_families: std::collections::BTreeSet<SketchAlgorithm> =
        std::collections::BTreeSet::new();
    for families in cfg.metric_to_family.values() {
        for kind in families {
            needed_families.insert(base_family(kind));
        }
    }

    // ── Processors ─────────────────────────────────────────────────────────
    //
    // Load ONLY the sketch processors for the families some metric in
    // the current plan actually needs. A future plan that maps a new
    // metric to a family not yet present re-emits via the planner
    // (`collect_metric_to_family` → fresh `metric_to_family`), which the
    // OpAMP push delivers as a new config — so pruning here does not
    // break runtime retargeting, it just stops shipping sketch state
    // for families nothing queries.
    let mut processors: BTreeMap<String, Value> = BTreeMap::new();

    // Build per-family processor blocks. We pull from
    // `cfg.sketch_processors` when an entry exists for that family
    // (so the params flow through), otherwise we synthesise a
    // default-param block. Keyed by `base_family` — `FAMILY_ORDER` is a
    // fixed 5-bare-family list with no heap-bearing entries, exactly
    // matching pre-`SketchAlgorithm`-split behavior (heap-bearing-ness was
    // never visible to this bare-kind lookup even when it lived as a
    // `with_heap` params flag).
    let mut family_to_proc: HashMap<SketchAlgorithm, &EdgeSketchProcessor> = HashMap::new();
    for sp in &cfg.sketch_processors {
        family_to_proc.insert(base_family(&sp.sketch_algorithm), sp);
    }

    for kind in FAMILY_ORDER {
        if !needed_families.contains(&kind) {
            continue;
        }
        let processor_name = sketch_algorithm_to_processor_name(&kind);
        let metric_name_hint = cfg
            .metric_to_family
            .iter()
            .filter_map(|(metric, mapped)| {
                if mapped.iter().any(|k| base_family(k) == kind) {
                    Some(metric.as_str())
                } else {
                    None
                }
            })
            .min();
        // MVP blocker B4: clamp `window_secs` to [5, 60] on the
        // 5-sketch routing path too. Without this every per-family
        // processor in the routed YAML inherits the unclamped 300s
        // window from `[5m]` queries.
        let clamped_window = clamp_window_secs(cfg.window_secs);
        // Per-metric sampling probability for the metric routed to this
        // family (keyed by the same `metric_name_hint` used above).
        let sample_p = metric_name_hint.and_then(|m| cfg.metric_to_sample_p.get(m).copied());
        let block = if let Some(sp) = family_to_proc.get(&kind) {
            build_edge_processor_block(
                sp,
                clamped_window,
                &cfg.label_filters,
                metric_name_hint,
                sample_p,
            )
        } else {
            build_default_edge_processor_block(&kind, clamped_window, metric_name_hint, sample_p)
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
        let gorillas3_yaml = build_gorillas3_yaml(window_secs);
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
    // the 1.5 GiB cgroup ceiling at peak. Threshold default = 1280 MiB
    // / 256 MiB spike (≈ 80 % / 17 % of a 1.5 GiB cgroup); operators who
    // raise the agent container's cgroup limit can also raise the soft
    // limit at controller emit time via `ASAP_AGENT_MEMORY_LIMIT_MIB`
    // (mirrors the env-substitute pattern in `build_gorillas3_yaml`).
    // `spike_limit_mib` is fixed at 20 % of the soft limit (min 256
    // MiB) so the ratio stays sensible as operators tune the limit.
    // MUST be the first processor in every per-sketch pipeline (see
    // `make_sketch_pipeline` below) — limiting AFTER gorillas3 would
    // mean the buffer has already accreted on heap by the time the
    // limiter rejects.
    let memory_limit_mib: u64 = std::env::var("ASAP_AGENT_MEMORY_LIMIT_MIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1280);
    let spike_limit_mib: u64 = std::cmp::max(256, memory_limit_mib / 5);
    let memory_limiter_block: Value = serde_yaml::from_str(&format!(
        "check_interval: 1s\nlimit_mib: {memory_limit_mib}\nspike_limit_mib: {spike_limit_mib}\n"
    ))
    .context("parse memory_limiter processor block")?;
    processors.insert("memory_limiter".to_string(), memory_limiter_block);

    // ── cumulativetodelta processor — Issue #298 ───────────────────────────
    //
    // The OTel SDK's `Counter` instruments default to **cumulative**
    // temporality: every export carries the running lifetime value of
    // the counter, not the per-export delta. Backend's `SumAccumulator`
    // (`data_plane/src/precompute_engine/operators/sum_accumulator.rs`)
    // naïvely sums every incoming value into the per-window state — fed
    // cumulative data, it computes `Σ-of-cumulatives-in-window`, a
    // quadratic-in-time blowup. The replay path then re-sums those
    // inflated per-window values across the lookback range → cubic
    // blowup for instant `sum by (zone) (counter)` queries.
    // (Observed: ~300× the baseline pre-fix; see Issue #298.)
    //
    // Fix: register the contrib build's `cumulativetodelta` processor
    // with an `include.metrics` allowlist of the workload's
    // Counter-shaped metrics (sourced from
    // `collect_cumulative_counter_metrics` — every workload entry whose
    // query classifies as `AggRole::Sum`), and run it as the FIRST
    // processor in the entry (`metrics:`) pipeline so EVERY routed copy
    // of each listed metric reaches the routing connector with delta
    // temporality.
    //
    // `match_type: strict` keeps the processor a no-op for any other
    // metric (gauges like `http_requests_total_latency_ms` pass through
    // unchanged — quantile / histogram workloads keep their wire shape).
    //
    // Why entry pipeline (not per-family pipeline): the routing
    // connector dispatches on `metric.name`; running the conversion
    // upstream of the connector means every per-family pipeline AND the
    // `raw_passthrough` default both see deltas. Per-pipeline placement
    // would duplicate work and risk double-conversion on pipelines that
    // a future plan fans the metric into.
    //
    // Empty `cumulative_counter_metrics` ⇒ no processor declared, no
    // entry-pipeline processor list — backward-compat for
    // quantile-only / sketch-only plans that never declare a counter.
    let needs_cumulativetodelta = !cfg.cumulative_counter_metrics.is_empty();
    if needs_cumulativetodelta {
        // Deterministic order so the emitted YAML is stable across
        // controller runs — mirrors the BTreeMap-not-HashMap rationale
        // on `CollectorYaml`. The agent's opampextension byte-compares
        // pushed configs; an unsorted include list would force an
        // apply+restart on every push of the same semantic plan.
        let mut sorted_metrics: Vec<&String> = cfg.cumulative_counter_metrics.iter().collect();
        sorted_metrics.sort();
        // YAML indentation note: `metrics` and `match_type` are both
        // direct children of `include` (not of each other). The
        // `include.metrics` list entries indent two more spaces under
        // `metrics:`. Get this wrong and serde_yaml rejects the block
        // with "did not find expected key" at parse time.
        let mut metrics_yaml = String::new();
        for m in &sorted_metrics {
            metrics_yaml.push_str(&format!("    - \"{m}\"\n"));
        }
        let cumulativetodelta_yaml =
            format!("include:\n  metrics:\n{metrics_yaml}  match_type: strict\n");
        let cumulativetodelta_block: Value = serde_yaml::from_str(&cumulativetodelta_yaml)
            .context("parse cumulativetodelta processor block")?;
        processors.insert("cumulativetodelta".to_string(), cumulativetodelta_block);
    }

    // ── Exporters ──────────────────────────────────────────────────────────
    // Edge → asapquery-backend OTLP ingest (see emit_edge_yaml for the
    // gateway-less rationale).
    let (exporter_key, exporter_val) = build_otlp_exporter("data-plane", &cfg.exporter_target);
    let mut exporters: BTreeMap<String, Value> = [(exporter_key.clone(), exporter_val)].into();

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
    // not order-stable. Each value is a SET of families
    // (ASAPCollector#400): a metric needing two capabilities lists BOTH
    // per-family pipelines in its single OTTL condition, so the routing
    // connector fans its samples into both pipelines. Family order
    // within each metric's pipeline list follows the canonical
    // `FAMILY_ORDER` so the YAML is stable.
    let mut metric_family_pairs: Vec<(&String, &std::collections::BTreeSet<SketchAlgorithm>)> =
        cfg.metric_to_family.iter().collect();
    metric_family_pairs.sort_by(|a, b| a.0.cmp(b.0));

    let mut table_entries: Vec<String> = Vec::new();
    let mut referenced_pipelines: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();

    // ── MVP blocker B3: per-metric attribute-allowlist processors ──────────
    //
    // For every metric the planner pinned to a sketch family, register a
    // `transform/keep_for_<sanitized_metric>` OTTL processor that strips
    // wire attrs down to the streaming-config's `grouping_labels` BEFORE
    // the sketch processor mints sids. Without this the agent sketches
    // with the full wire-attr tuple — one sid per unique tuple,
    // defeating the streaming-config contract.
    //
    // We use the OTTL transform processor (not the attributes processor)
    // because attributesprocessor has no native "keep only these" /
    // allowlist action. The transform processor's
    // `keep_keys(datapoint.attributes, [...])` is the right primitive
    // and is registered in the asap-otel builder-config.
    //
    // Metrics absent from `metric_to_grouping_labels` are skipped
    // (preserves backward-compat for raw OTel agents bypassing the
    // typed-stage-split — no keep processor injected, attrs flow
    // through unmodified). A multi-family metric's keep-processor is
    // added to EACH of its families' pipelines (the `where metric.name
    // == "<metric>"` guard makes it a no-op on the family's other
    // metrics).
    let mut family_to_keep_processors: HashMap<SketchAlgorithm, Vec<String>> = HashMap::new();
    for (metric, families) in &metric_family_pairs {
        let Some(labels) = cfg.metric_to_grouping_labels.get(*metric) else {
            continue;
        };
        let proc_name = transform_keep_processor_name(metric);
        let proc_block = build_transform_keep_processor_block(metric, labels);
        processors.insert(proc_name.clone(), proc_block);
        for kind in *families {
            family_to_keep_processors
                .entry(base_family(kind))
                .or_default()
                .push(proc_name.clone());
        }
    }

    // ── ASAPCollector#403: edge-aggregate Sum-role counters ────────────────
    //
    // A Sum-role metric (in `cumulative_counter_metrics`) whose marquee
    // queries are `sum by (<labels>) (...)` previously fell through the
    // routing connector's `default_pipelines` into `raw_passthrough` (no
    // aggregation): every counter datapoint across the full wire-attr
    // cardinality (e.g. ~10k zone×rack×node×pod series) streamed
    // continuously to the backend, which did the Sum-by-grouping fan-in
    // centrally. That inverts the edge-aggregation value prop and was the
    // dominant driver of the asap arm's backend-ingress blowup
    // (~12 Mbps of ~12.5 Mbps measured).
    //
    // Fix (mirrors the static agent config's `metrics/sum_aggregate`
    // pipeline): for each Sum-role metric that (a) has grouping_labels
    // declared and (b) is NOT routed to any sketch family, register a
    // dedicated `metricstransform/sumby_<metric>` processor +
    // `metrics/sum_aggregate_<metric>` pipeline and route the metric
    // there instead of letting it default to raw_passthrough. The agent
    // then ships one summed series per grouping-label tuple per flush
    // window. The backend's `evaluate_exact_agg` produces the identical
    // `sum by (<labels>)` and per-group `rate` answers at reduced
    // cardinality.
    //
    // gorillas3 (cold-tier archive) still writes RAW full-cardinality
    // samples on this pipeline BEFORE the metricstransform collapses the
    // stream, preserving cold-fallback drill-down (e.g.
    // `count(metric{<label>="..."})`).
    //
    // A metric already mapped to a sketch family is left on its
    // sketch path (it isn't a plain Sum-role passthrough). A Sum-role
    // metric with NO grouping labels keeps the raw_passthrough default
    // (no grouping to aggregate by).
    let mut sum_aggregate_pipelines: Vec<(String, String)> = Vec::new();
    {
        let mut sum_metrics: Vec<&String> = cfg
            .cumulative_counter_metrics
            .iter()
            .filter(|m| {
                cfg.metric_to_grouping_labels.contains_key(*m)
                    && !cfg.metric_to_family.contains_key(*m)
            })
            .collect();
        sum_metrics.sort();
        sum_metrics.dedup();
        for metric in sum_metrics {
            let labels = cfg
                .metric_to_grouping_labels
                .get(metric)
                .cloned()
                .unwrap_or_default();
            let proc_name = metricstransform_groupby_processor_name(metric);
            let proc_block = build_metricstransform_groupby_processor_block(metric, &labels);
            processors.insert(proc_name.clone(), proc_block);
            let pipeline_name = sum_aggregate_pipeline_name(metric);
            referenced_pipelines.insert(pipeline_name.clone());
            sum_aggregate_pipelines.push((pipeline_name.clone(), proc_name));
            table_entries.push(format!(
                "  - context: metric\n    condition: 'name == \"{metric}\"'\n    pipelines: [{pipeline_name}]"
            ));
        }
    }

    for (metric, families) in &metric_family_pairs {
        // Emit one routing condition per metric listing every family
        // pipeline in its set (canonical order). A single-family metric
        // → one pipeline; a multi-capability metric → its samples fan
        // into each family pipeline so the backend serves every
        // (metric, capability) the workload needs.
        let pipelines: Vec<&str> = FAMILY_ORDER
            .iter()
            .filter(|k| families.contains(*k))
            .map(sketch_algorithm_to_pipeline_name)
            .collect();
        if pipelines.is_empty() {
            continue;
        }
        for pl in &pipelines {
            referenced_pipelines.insert((*pl).to_string());
        }
        let pipelines_yaml = pipelines.join(", ");
        table_entries.push(format!(
            "  - context: metric\n    condition: 'name == \"{metric}\"'\n    pipelines: [{pipelines_yaml}]"
        ));
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
    let mut connectors: BTreeMap<String, Value> = BTreeMap::new();
    connectors.insert("routing".to_string(), routing_block);

    // ── Pipeline assembly ──────────────────────────────────────────────────
    //
    // Helper: per-family pipeline =
    //   `[memory_limiter, gorillas3?, <family>processor, batch]`.
    // memory_limiter runs FIRST so backpressure rejects incoming batches
    // BEFORE gorillas3 buffers them into windowState. gorillas3 then
    // does the cold-tier write on raw samples BEFORE the sketch
    // processor mutates / suffix-renames the stream.
    // Per-family pipeline =
    //   `[memory_limiter, gorillas3?, transform/keep_for_<metric>*, <family>processor, batch]`.
    // gorillas3 does the cold-tier write on RAW samples (full wire attrs
    // preserved in MinIO for drill-down) BEFORE the keep-processor
    // strips attrs down to grouping-labels for the sketch processor's
    // benefit (MVP blocker B3). `keep_procs` is empty for families
    // with no metrics declared in `metric_to_grouping_labels` —
    // pipeline reduces to the pre-B3 shape, attrs flow through.
    let make_sketch_pipeline = |family_proc: &str, keep_procs: &[String]| -> Pipeline {
        let mut procs: Vec<String> = Vec::new();
        procs.push("memory_limiter".to_string());
        if has_archive_tier {
            procs.push("gorillas3".to_string());
        }
        for kp in keep_procs {
            procs.push(kp.clone());
        }
        procs.push(family_proc.to_string());
        procs.push("batch".to_string());
        Pipeline {
            receivers: vec!["routing".into()],
            processors: procs,
            exporters: vec![exporter_key.clone()],
        }
    };

    let mut pipelines: BTreeMap<String, Pipeline> = BTreeMap::new();

    // Entry pipeline — receivers: [otlp], exporters: [routing]
    // (`routing` here is the connector, used as exporter for the entry
    // stage). The processor list is normally empty (the connector owns
    // fan-out), but Issue #298 requires `cumulativetodelta` to run
    // BEFORE the connector so EVERY routed copy of a Counter-shaped
    // metric reaches the downstream pipelines with delta temporality.
    // Putting the conversion here (not per-family) avoids duplicating
    // the conversion across the per-family pipelines AND the
    // `raw_passthrough` default, and stops fan-in-from-multiple-routes
    // double-conversion.
    let mut entry_processors: Vec<String> = Vec::new();
    if needs_cumulativetodelta {
        entry_processors.push("cumulativetodelta".to_string());
    }
    pipelines.insert(
        "metrics".to_string(),
        Pipeline {
            receivers: vec!["otlp".into()],
            processors: entry_processors,
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

    // ── ASAPCollector#403: edge Sum-by-grouping pipelines ──────────────────
    //
    // One dedicated pipeline per Sum-role metric routed to edge
    // aggregation (computed above). Shape:
    //   `[memory_limiter, gorillas3?, metricstransform/sumby_<metric>, batch]`.
    // memory_limiter applies backpressure first; gorillas3 (when an
    // archive tier is declared) writes RAW full-cardinality samples to
    // the cold tier BEFORE the metricstransform collapses the stream to
    // one summed series per grouping-label tuple; batch coalesces the
    // per-window export. The exporter is the same backend OTLP target.
    for (pipeline_name, proc_name) in &sum_aggregate_pipelines {
        let mut procs: Vec<String> = Vec::new();
        procs.push("memory_limiter".to_string());
        if has_archive_tier {
            procs.push("gorillas3".to_string());
        }
        procs.push(proc_name.clone());
        procs.push("batch".to_string());
        pipelines.insert(
            pipeline_name.clone(),
            Pipeline {
                receivers: vec!["routing".into()],
                processors: procs,
                exporters: vec![exporter_key.clone()],
            },
        );
    }

    // ASAPCollector#400 — emit ONLY the per-family pipelines for
    // families some metric actually needs (`needed_families`, the union
    // of every metric's set). The prior emitter emitted all 5 pipelines
    // unconditionally and the routing connector fanned every metric
    // through all 5, shipping ~5× the sketch state — the dominant cause
    // of the asap arm's bandwidth blowup. Pruning to the needed set is
    // safe for runtime retargeting because the planner re-emits a fresh
    // `metric_to_family` (via `collect_metric_to_family`) when the
    // workload changes, which the OpAMP push delivers as a new config.
    // The pipeline graph stays closed: every pipeline referenced by the
    // routing table's `table:`/`default_pipelines:` is present, because
    // `referenced_pipelines` is a subset of `needed_families`'s pipelines
    // plus `metrics/raw_passthrough` (always emitted above).
    for kind in FAMILY_ORDER {
        if !needed_families.contains(&kind) {
            continue;
        }
        let proc_name = sketch_algorithm_to_processor_name(&kind);
        let pipeline_name = sketch_algorithm_to_pipeline_name(&kind);
        // Sort per-family keep-processor list deterministically so YAML
        // output is stable across runs (HashMap iteration is not
        // order-stable). Empty list when no metrics in the family have
        // grouping labels declared (MVP blocker B3).
        let mut keep_procs = family_to_keep_processors
            .get(&kind)
            .cloned()
            .unwrap_or_default();
        keep_procs.sort();
        keep_procs.dedup();
        pipelines.insert(
            pipeline_name.to_string(),
            make_sketch_pipeline(proc_name, &keep_procs),
        );
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
    //
    // Issue #2: X-Agent-ID header — see legacy `emit_edge_yaml` for the
    // full rationale. Without it the agent has no identity after a
    // controller-pushed config triggers a Docker restart, and the
    // controller's `/api/v1/agents` is empty post-restart.
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n    headers:\n      X-Agent-ID: \"{agent_id}\"\nremote_config_path: /etc/otel/config.yaml\n"
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

/// Map a `SketchAlgorithm` to the `family:` token the fused `asap_edge`
/// processor's `metrics[]` list expects. These differ from the OTel
/// component-id processor names (`KLL`, `countmin`, …) used by the
/// routing-connector path — the fused processor takes a lower-case
/// family discriminant per entry, matching the hand-written contract in
/// `asap-otel-agent-b6-asap-single-sketch.yaml`.
fn sketch_algorithm_to_asap_edge_family(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::DDSketch => "ddsketch",
        SketchAlgorithm::Kll => "kll",
        SketchAlgorithm::Hll => "hll",
        SketchAlgorithm::CountSketch => "countsketch",
        SketchAlgorithm::Cms => "countminsketch",
        // Every caller iterates the fixed 5-bare-family `FAMILY_ORDER`
        // list (heap-bearing kinds normalize through `base_family`
        // before reaching here), and no Bind* rule in this repo
        // produces the exact-accumulator / Kmv / Theta kinds at all.
        other => unreachable!("sketch_algorithm_to_asap_edge_family: unexpected kind {other:?}"),
    }
}

/// Derive the heap-bearing CountSketch `item_label` (the data-point
/// attribute whose VALUE is the heavy-hitter item the top-k heap ranks)
/// from a metric name.
///
/// The control plane does not (yet) thread a per-metric item dimension
/// onto [`EdgeStageConfig`], so we recover it from the metric-name
/// convention the workload uses: a top-K counter is named
/// `<verb>_<dim>_<unit>` (e.g. `top_endpoint_qps`). We strip a leading
/// `top_` / `topk_` verb and a trailing `_qps` / `_count` / `_total` /
/// `_freq` / `_per_min` unit, leaving the item dimension (`endpoint`).
/// This yields `endpoint` for the demo's `top_endpoint_qps` and
/// generalises (`top_user_qps` → `user`). When nothing strips, we default
/// to `endpoint` (the canonical top-K item dimension for this workload)
/// rather than the degenerate metric-NAME keying — the heap is useless if
/// every observation lands in one cell.
fn countsketch_item_label_for(metric: &str) -> String {
    let mut s = metric;
    for prefix in ["topk_", "top_"] {
        if let Some(rest) = s.strip_prefix(prefix) {
            s = rest;
            break;
        }
    }
    for suffix in [
        "_per_min", "_per_sec", "_qps", "_count", "_total", "_freq", "_rate",
    ] {
        if let Some(rest) = s.strip_suffix(suffix) {
            s = rest;
            break;
        }
    }
    if s.is_empty() {
        "endpoint".to_string()
    } else {
        s.to_string()
    }
}

/// Issue #46 — emit the FUSED single-pipeline `asap_edge` edge agent
/// wire shape.
///
/// This is the replacement for [`emit_edge_yaml_5sketch_routing`]: the
/// agent now runs ONE processor (`asap_edge`) that does the cold archive
/// (Gorilla), the Sum-by-grouping aggregation, and all five sketch
/// families in a single sharded decode pass, instead of a `routing`
/// connector fanning out to per-family pipelines. The emitted topology
/// is:
///
/// ```text
/// otlp → [memory_limiter, cumulativetodelta, asap_edge] → otlp/backend
/// ```
///
/// The shape is generalised over the planner's inputs from the SAME
/// [`EdgeStageConfig`] fields the routing path reads — see the per-block
/// comments for the exact mapping. Selected by `ASAP_EDGE_FUSED`
/// (see [`fused_asap_edge_enabled`]); the routing path stays the default
/// until the fused agent build is the default deployment.
fn emit_edge_yaml_asap_edge(
    cfg: &EdgeStageConfig,
    _opamp_endpoint: &str,
    _agent_id: &str,
) -> Result<String> {
    use planner_types::post_asap::SketchAlgorithm;

    // ── Receivers ──────────────────────────────────────────────────────────
    // OTLP gRPC on 4317 + HTTP on 4318 — same as every other edge emit.
    // The fused contract bumps `max_recv_msg_size_mib` to 4096 (the
    // hand-written config raises it so a window's worth of batched
    // points never trips the gRPC frame limit before asap_edge buffers
    // them).
    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n    max_recv_msg_size_mib: 4096\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    )
    .context("parse static OTLP receiver block (asap_edge)")?;

    let mut processors: BTreeMap<String, Value> = BTreeMap::new();

    // ── memory_limiter — backpressure before asap_edge buffers a window
    // in memory. Same env-tunable knob as the routing path so operators
    // tune one variable for both shapes.
    let memory_limit_mib: u64 = std::env::var("ASAP_AGENT_MEMORY_LIMIT_MIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1280);
    let spike_limit_mib: u64 = std::cmp::max(256, memory_limit_mib / 5);
    let memory_limiter_block: Value = serde_yaml::from_str(&format!(
        "check_interval: 1s\nlimit_mib: {memory_limit_mib}\nspike_limit_mib: {spike_limit_mib}\n"
    ))
    .context("parse memory_limiter processor block (asap_edge)")?;
    processors.insert("memory_limiter".to_string(), memory_limiter_block);

    // ── cumulativetodelta — Issue #298 / #46 ───────────────────────────────
    //
    // Counters → delta upstream of asap_edge so the Sum aggregator and the
    // backend SumAccumulator see deltas. The include list is the
    // Counter-shaped metrics the planner classified as `AggRole::Sum`
    // (`cfg.cumulative_counter_metrics`) — which is exactly "the sum
    // metrics PLUS the counter-shaped sketch inputs" the fused contract
    // calls for: a counter that is ALSO sketched (e.g. `top_endpoint_qps`
    // → CountSketch, `endpoint_request_freq` → Count-Min,
    // `unique_users_per_min` → HLL) still classifies Sum and so still
    // lands here, while gauges (latency) are left untouched by
    // `match_type: strict`.
    let needs_cumulativetodelta = !cfg.cumulative_counter_metrics.is_empty();
    if needs_cumulativetodelta {
        let mut sorted_metrics: Vec<&String> = cfg.cumulative_counter_metrics.iter().collect();
        sorted_metrics.sort();
        sorted_metrics.dedup();
        let mut metrics_yaml = String::new();
        for m in &sorted_metrics {
            metrics_yaml.push_str(&format!("    - \"{m}\"\n"));
        }
        let cumulativetodelta_yaml =
            format!("include:\n  metrics:\n{metrics_yaml}  match_type: strict\n");
        let cumulativetodelta_block: Value = serde_yaml::from_str(&cumulativetodelta_yaml)
            .context("parse cumulativetodelta processor block (asap_edge)")?;
        processors.insert("cumulativetodelta".to_string(), cumulativetodelta_block);
    }

    // ── asap_edge — the fused processor ─────────────────────────────────────
    //
    // `metrics[]` is the metric→family map (the job the routing connector
    // + per-family pipelines used to do). We assemble it from three
    // planner inputs, all already on `EdgeStageConfig`:
    //
    //   * sum family   — every `AggRole::Sum` metric (in
    //                    `cumulative_counter_metrics`) that has grouping
    //                    labels declared and is NOT routed to a sketch
    //                    family. `aggregate_by` = the metric's
    //                    `group_by_labels` (from `metric_to_grouping_labels`).
    //                    Mirrors the routing path's `metrics/sum_aggregate`
    //                    selection (a counter that is sketched stays on its
    //                    sketch entry; an ungrouped Sum stays raw — no
    //                    aggregate entry).
    //   * sketch family — per `metric_to_family` × `sketch_processors`
    //                    params (`relative_accuracy` / `k` / `rows` / `cols`),
    //                    mirroring `build_edge_processor_block`'s param reads.
    //
    // Entry order is deterministic (sum entries first sorted by metric,
    // then sketch entries sorted by metric then canonical family order)
    // so the emitted YAML is byte-stable for the agent's opampextension
    // no-op check.
    let window_secs = clamp_window_secs(cfg.window_secs).unwrap_or(MAX_WINDOW_SECS);

    // shard_count: key-hash sharding for multi-core decode AND flush
    // staggering — flushLoop phase-shifts one shard per (WindowDuration/
    // shard_count) tick, so a higher count spreads the per-flush CPU+memory
    // burst into more, smaller bursts (smoother under the dense raw-buffer
    // workload). Default 12; env-overridable via ASAP_SHARD_COUNT.
    let shard_count: u64 = std::env::var("ASAP_SHARD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(12);

    let mut metric_entries: Vec<Value> = Vec::new();

    // ── Per-metric storage tier (issue #46 follow-up; companion to the
    // ASAPCollector asapedgeprocessor `tier` field) ─────────────────────────
    //
    // Each emitted `metrics[]` entry carries a `tier` ∈ {warm, both, cold}
    // telling the fused agent which storage tiers to feed the metric into:
    //
    //   * `warm` — warm sketch/aggregation ONLY; the metric is NOT
    //              cold-archived by the agent's Gorilla encoder.
    //   * `both` — warm sketch/agg AND cold gorilla archive.
    //   * `cold` — cold gorilla archive ONLY; no warm sketch/agg.
    //
    // We DERIVE the tier from the SAME plan routing the rest of this emit
    // reads — no hardcoded metric→tier table — so it generalises to any
    // workload:
    //
    //   * "warm" signal — the metric produces a warm entry below (a
    //     Sum-by aggregate from `cumulative_counter_metrics` +
    //     `metric_to_grouping_labels`, or a sketch family from
    //     `metric_to_family`). This is exactly the routing that lands a
    //     metric in the warm sketch/agg tier.
    //   * "cold" signal — the metric is in `archive_tier_metrics`, the
    //     plan's archive-routing decision (an exact / archive query forces
    //     the Gorilla object-store archive; the routing that the legacy
    //     `gorillas3` processor consumed).
    //
    // tier = both when a metric has BOTH signals (e.g. `http_requests_total`
    // — warm `sum by (zone)` AND an exact `count(...)` archive query), warm
    // when only the warm signal is present (the sketch-only quantile / HLL /
    // topk / rate metrics), cold when only the archive signal is present.
    // When neither is determinable the metric defaults to `both` (safe —
    // preserves archival), matching the processor's unset-tier default.
    let cold_set: std::collections::BTreeSet<&str> = cfg
        .archive_tier_metrics
        .iter()
        .map(|a| a.metric.as_str())
        .collect();
    let tier_for = |metric: &str, warm: bool| -> &'static str {
        let cold = cold_set.contains(metric);
        match (warm, cold) {
            (true, true) => "both",
            (true, false) => "warm",
            (false, true) => "cold",
            // No routing signal at all — default to `both` so the agent
            // keeps archiving (preserves data); the processor treats an
            // unset tier the same way.
            (false, false) => "both",
        }
    };

    // Sum-family entries — same predicate as ASAPCollector#403's
    // edge-aggregate selection: Sum-role metric WITH grouping labels and
    // NOT mapped to a sketch family.
    let mut sum_metrics: Vec<&String> = cfg
        .cumulative_counter_metrics
        .iter()
        .filter(|m| {
            cfg.metric_to_grouping_labels.contains_key(*m) && !cfg.metric_to_family.contains_key(*m)
        })
        .collect();
    sum_metrics.sort();
    sum_metrics.dedup();
    for metric in sum_metrics {
        let labels = cfg
            .metric_to_grouping_labels
            .get(metric)
            .cloned()
            .unwrap_or_default();
        let mut e = Mapping::new();
        e.insert("metric".into(), Value::String((*metric).clone()));
        e.insert("family".into(), Value::String("sum".to_string()));
        let by: Vec<Value> = labels.into_iter().map(Value::String).collect();
        e.insert("aggregate_by".into(), Value::Sequence(by));
        // Sum aggregate IS a warm entry → warm signal = true.
        e.insert(
            "tier".into(),
            Value::String(tier_for(metric, true).to_string()),
        );
        metric_entries.push(Value::Mapping(e));
    }

    // Sketch-family entries. Look up params from `sketch_processors`
    // (keyed by family) so the per-metric param block mirrors the
    // routing path; fall back to catalog defaults when the planner
    // mapped a family with no enumerated processor.
    let mut family_to_proc: HashMap<SketchAlgorithm, &EdgeSketchProcessor> = HashMap::new();
    for sp in &cfg.sketch_processors {
        family_to_proc.insert(base_family(&sp.sketch_algorithm), sp);
    }
    const FAMILY_ORDER: [SketchAlgorithm; 5] = [
        SketchAlgorithm::DDSketch,
        SketchAlgorithm::Kll,
        SketchAlgorithm::Hll,
        SketchAlgorithm::CountSketch,
        SketchAlgorithm::Cms,
    ];
    let mut metric_family_pairs: Vec<(&String, &std::collections::BTreeSet<SketchAlgorithm>)> =
        cfg.metric_to_family.iter().collect();
    metric_family_pairs.sort_by(|a, b| a.0.cmp(b.0));
    for (metric, families) in &metric_family_pairs {
        // Normalize to bare families before filtering against the fixed
        // `FAMILY_ORDER` list — same reasoning as `family_to_proc` above:
        // a committed heap-bearing kind (`CmsWithHeap`/`CountSketchWithHeap`)
        // must still match its bare `FAMILY_ORDER` entry.
        let bare_families: std::collections::BTreeSet<SketchAlgorithm> =
            families.iter().map(base_family).collect();
        for kind in FAMILY_ORDER.iter().filter(|k| bare_families.contains(*k)) {
            let mut e = Mapping::new();
            e.insert("metric".into(), Value::String((*metric).clone()));
            e.insert(
                "family".into(),
                Value::String(sketch_algorithm_to_asap_edge_family(kind).to_string()),
            );
            // aggregate_by: emit this metric's workload grouping_labels so each
            // sketch is one-per-group (e.g. per zone), mirroring the Sum path
            // above. CRITICAL for the heap-bearing CountSketch (warm topk): with
            // an empty aggregate_by the edge factory falls into
            // GlobalAggregation (it collapses the series grouping to a single
            // attr-less sketch), and the backend's registry-sid ingest cannot
            // mint a sid for an attr-less series — so the sketch is never
            // registered and `topk(...)` capability-misses to archive. KLL
            // metrics carry no grouping_labels (per-series quantile) and
            // correctly receive no aggregate_by here.
            let grouping = cfg
                .metric_to_grouping_labels
                .get(*metric)
                .cloned()
                .unwrap_or_default();
            // `effective_by` is the per-group keying actually emitted as
            // `aggregate_by` (grouping_labels minus the item_label). Hoisted
            // out of the emit branch so the `mode` decision below can read
            // whether the edge factory would key per-group (non-empty) or
            // collapse to a single attr-less sketch (empty).
            let effective_by: Vec<String> = if grouping.is_empty() {
                Vec::new()
            } else {
                // Exclude the item_label (the inner heavy-hitter dimension
                // for HLL / CMS / heap-bearing CountSketch) from
                // aggregate_by: it is the sketch SUBJECT — hashed into the
                // sketch / fed to the top-k heap — NOT a series grouping
                // key. A query like `topk(10, sum by (host) (m))` lands
                // `host` in grouping_labels, but for a heap-bearing
                // CountSketch `host` is the item_label; leaving it in
                // aggregate_by keys the edge series PER host (one series +
                // heap per host — a cardinality explosion) instead of one
                // heap per group. The agent observe path already projects
                // item_label out of the series key for the item-keyed
                // families, so the two layers must agree. No-op for metrics
                // without an item_label or whose item_label isn't a
                // grouping label.
                let item_label = cfg.metric_to_item_label.get(*metric);
                grouping
                    .into_iter()
                    .filter(|k| item_label.map(|il| il != k).unwrap_or(true))
                    .collect()
            };
            if !effective_by.is_empty() {
                e.insert(
                    "aggregate_by".into(),
                    Value::Sequence(effective_by.iter().cloned().map(Value::String).collect()),
                );
            }

            // ── mode (aggregation SCOPE) — ASAPCollector#471 ────────────────
            //
            // The edge `MetricFamily.mode` (`per_series` / `whole_stream`,
            // precompute `PrecomputeConfig.Scope`) decides whether a window
            // keys ONE sketch per series-group (per_series — the default) or
            // collapses EVERY matching datapoint into a single attr-less
            // sketch (whole_stream). #471 folded the legacy `GlobalAggregation`
            // bool INTO this scope, so `whole_stream` is SEMANTICALLY IDENTICAL
            // to the empty-`aggregate_by`→GlobalAggregation behaviour the edge
            // factory has today.
            //
            // Signal: a metric whose `effective_by` is EMPTY *and* whose family
            // is a genuinely cross-series/global aggregate is whole-stream.
            // The per-series quantile families (DDSketch / KLL) are NEVER
            // whole-stream here — they reduce within a single series, and an
            // empty grouping there means "no extra keying", not "collapse the
            // stream". The item-counting / frequency families (HLL / CMS /
            // CountSketch) with an empty effective grouping ARE the global
            // case the planner emits for `count(distinct …)` (no `by`),
            // global top-k, and global frequency — exactly #471's
            // `WholeStream` examples (distinct-count / global-top-k / global
            // frequency over the whole stream).
            //
            // Back-compat & the heap-bearing-CountSketch warning above: we
            // emit `whole_stream` ONLY where the code already produces an
            // empty `aggregate_by` for one of these global families. A
            // CountSketch/HLL/CMS that DOES carry per-group keying
            // (non-empty `effective_by`) keeps per_series, so we never newly
            // collapse a metric that needs per-group sid minting. `per_series`
            // is the edge default (empty/omitted `mode` ⇒ ParseAggMode →
            // ModePerSeries), so we emit NOTHING for the per_series case: the
            // YAML for every metric that isn't a genuine whole-stream global
            // aggregate stays byte-identical to today.
            let whole_stream = effective_by.is_empty()
                && matches!(
                    kind,
                    SketchAlgorithm::Hll | SketchAlgorithm::Cms | SketchAlgorithm::CountSketch
                );
            if whole_stream {
                e.insert("mode".into(), Value::String("whole_stream".to_string()));
            }

            // ── hll_sparse (in-memory sparse HLL base) — ASAPCollector#472 ──
            //
            // `MetricFamily.hll_sparse` (default false = dense) opts the HLL
            // family into the sketchlib-go sparse base
            // (`NewHLLWrapperSparse`): low-cardinality warm series hold far
            // less than the dense ~16 KB/series register array, and the
            // serialized output is byte-identical to dense for the same inputs
            // (the sparse base auto-promotes to dense once enough registers
            // are set), so there is ZERO accuracy or wire risk. Only meaningful
            // for `family: hll`.
            //
            // Rule (scope-driven — see the design note):
            //   * whole_stream HLL → ONE high-cardinality instance per metric
            //     (distinct-count over the whole stream). It promotes to dense
            //     almost immediately, so the sparse base buys nothing and only
            //     adds promotion churn → emit `hll_sparse: false` (dense).
            //   * per_series HLL → one HLL per group; most groups are
            //     low-cardinality (e.g. distinct user_ids per zone), where the
            //     sparse base is a large memory win and auto-promotes the few
            //     hot groups → emit `hll_sparse: true`.
            //
            // CARDINALITY HINT (ASAPCollector#472 follow-up to PR #358 — now
            // plumbed): the per-metric `WorkloadEntry::distinct_keys_per_window`
            // hint rides into this emit site on
            // `EdgeStageConfig::metric_to_distinct_keys` (populated by
            // `collect_metric_to_distinct_keys` in main/replan). When a
            // per_series HLL's known cardinality is at or above the in-memory
            // sparse→dense promotion point (`DENSE_CROSSOVER`), the sparse base
            // would promote almost immediately and only pay promotion churn, so
            // we emit it DENSE instead. Below the crossover (or with NO hint at
            // all — the common case) we keep the PR #358 scope-based default of
            // sparse, so metrics without the hint stay byte-identical to #358.
            // Whole-stream HLL is always dense regardless of the hint (one
            // high-cardinality instance per metric — see the rule note above).
            //
            // The crossover is a heuristic (distinct keys ≈ non-zero registers
            // only approximately); being off only costs/saves a one-time
            // promotion, never correctness or accuracy (the sparse base is
            // lossless and serializes byte-identically to dense). We emit the
            // flag ONLY for the HLL family; non-HLL families carry no
            // `hll_sparse` key.
            if matches!(kind, SketchAlgorithm::Hll) {
                let hll_sparse = if whole_stream {
                    false
                } else {
                    match cfg.metric_to_distinct_keys.get(*metric) {
                        // High-cardinality per-series HLL → dense (promotes
                        // immediately; sparse only adds churn).
                        Some(n) if *n >= DENSE_CROSSOVER => false,
                        // Low-cardinality or no hint → sparse (PR #358 default).
                        _ => true,
                    }
                };
                e.insert("hll_sparse".into(), Value::Bool(hll_sparse));
            }
            // Family-specific params — mirror the reads in
            // `build_edge_processor_block`. The fused processor's
            // per-entry surface uses `relative_accuracy` / `k` /
            // `rows` / `cols` (cols = sketch width, rows = depth).
            //
            // `countsketch_with_heap` tracks the planner's `with_heap`
            // flag (set by `BindCountSketchOnTopK` when the family is
            // CountSketch picked for a `topk(...)` query). It drives the
            // warm-topk heap keys emitted below for the CountSketch family.
            // Heap-bearing-ness now lives on `sketch_kind`, not a params
            // flag — read it off the processor's kind before matching
            // its params.
            let mut countsketch_with_heap = family_to_proc.get(kind).is_some_and(|sp| {
                matches!(sp.sketch_algorithm, SketchAlgorithm::CountSketchWithHeap)
            });
            match family_to_proc.get(kind).map(|sp| &sp.sketch_params) {
                Some(SketchParams::DDSketch { alpha }) => {
                    e.insert("relative_accuracy".into(), Value::Number((*alpha).into()));
                }
                Some(SketchParams::Kll { k }) => {
                    e.insert("k".into(), Value::Number((*k as u64).into()));
                }
                Some(SketchParams::Hll { .. }) => { /* HLL takes no per-entry knob */ }
                Some(SketchParams::CountSketch { width, depth })
                | Some(SketchParams::CountSketchWithHeap { width, depth, .. }) => {
                    e.insert("rows".into(), Value::Number((*depth as u64).into()));
                    e.insert("cols".into(), Value::Number((*width as u64).into()));
                }
                Some(SketchParams::Cms { width, depth })
                | Some(SketchParams::CmsWithHeap { width, depth, .. }) => {
                    e.insert("rows".into(), Value::Number((*depth as u64).into()));
                    e.insert("cols".into(), Value::Number((*width as u64).into()));
                }
                Some(
                    SketchParams::UnivMon { .. }
                    | SketchParams::Kmv { .. }
                    | SketchParams::Theta { .. },
                ) => unreachable!(
                    "5-sketch routing: non-sketch or unsupported SketchParams; \
                     no Bind* rule in this repo produces one"
                ),
                None => {
                    // Family with no enumerated processor — emit catalog
                    // defaults so the entry is still well-formed.
                    match kind {
                        SketchAlgorithm::DDSketch => {
                            e.insert("relative_accuracy".into(), Value::Number(0.01.into()));
                        }
                        SketchAlgorithm::Kll => {
                            e.insert("k".into(), Value::Number(200u64.into()));
                        }
                        SketchAlgorithm::Hll => {}
                        SketchAlgorithm::CountSketch => {
                            e.insert("rows".into(), Value::Number(5u64.into()));
                            e.insert("cols".into(), Value::Number(2048u64.into()));
                            // P1-4: NO enumerated EdgeSketchProcessor for this
                            // metric, so we can't read the planner's `with_heap`
                            // from `sketch_params` here. We must NOT blanket-
                            // default `with_heap = true` (that emitted a heap +
                            // guessed item_label for a plain `FrequencyEstimate`
                            // CountSketch, registering a `FrequencyTopk` sid a
                            // frequency/count query can't satisfy). But blanket-
                            // FALSE wrongly drops the heap for an actual top-k
                            // CountSketch that simply wasn't enumerated as a
                            // processor (the backend streaming-config still
                            // registers it `with_heap`, so the agent must emit
                            // the heap or the warm `topk(...)` capability-misses
                            // to archive). The reliable signal available here is
                            // the metric's `item_label`: a CountSketch carrying a
                            // heavy-hitter dimension (item_label, set by the
                            // top-k binding / workload) IS a top-k sketch and
                            // needs the heap; a plain frequency CountSketch has
                            // none → no heap. This keeps the agent emit in lock-
                            // step with the backend `with_heap` registration.
                            countsketch_with_heap = cfg.metric_to_item_label.contains_key(*metric);
                        }
                        SketchAlgorithm::Cms => {
                            e.insert("rows".into(), Value::Number(5u64.into()));
                            e.insert("cols".into(), Value::Number(2048u64.into()));
                        }
                        // `kind` always comes from the bare 5-family
                        // `FAMILY_ORDER` list.
                        other => unreachable!(
                            "5-sketch routing catalog defaults: unexpected kind {other:?}"
                        ),
                    }
                }
            }
            // Per-metric sampling: emit `sample_p` for the sampling-aware
            // families (CMS / HLL) only when `p < 1.0`. Mirrors
            // `build_edge_processor_block`'s guarded emit so an unset /
            // 1.0 probability keeps the fused entry byte-identical.
            if matches!(kind, SketchAlgorithm::Cms | SketchAlgorithm::Hll) {
                insert_sample_p(&mut e, cfg.metric_to_sample_p.get(*metric).copied());
            }

            // ── Per-metric delta_transmission (Foundation flag) ─────────────
            //
            // Mirrors the routing path's `build_edge_processor_block`: the
            // four delta-capable families (DDSketch / HLL / CountSketch /
            // Count-Min) emit `delta_transmission: true` (sparse delta
            // frames against the prior window's snapshot — large bandwidth
            // savings on slowly-changing sketches; the first window per
            // series still ships full state). KLL is deliberately OMITTED:
            // it has no delta variant (randomised compaction is not
            // additively mergeable), and the kllprocessor / asapedge KLL
            // path forces it off (`effectiveDelta`), so the key is ignored
            // there — we never emit it for KLL. The processor's per-entry
            // default is the top-level `Config.DeltaTransmission`, so an
            // explicit per-metric value here keeps the wire shape from
            // depending on that default.
            if matches!(
                kind,
                SketchAlgorithm::DDSketch
                    | SketchAlgorithm::Hll
                    | SketchAlgorithm::CountSketch
                    | SketchAlgorithm::Cms
            ) {
                e.insert("delta_transmission".into(), Value::Bool(true));
            }

            // ── CountSketch warm-topk heap keys (cross-repo dependency) ─────
            //
            // When the CountSketch family was planned with a heavy-hitter
            // heap (`with_heap`, set by `BindCountSketchOnTopK` for a
            // `topk(...)` query), emit the heap-bearing CountSketch wire
            // variant so the agent ships the `{sketch, topk_heap, heap_size}`
            // payload the backend detects as `CountSketchWithHeap`
            // (Capability::FrequencyTopk) and a warm `topk(metric)` query
            // routes to it instead of returning "No result".
            //
            //   * emit_heap: true   — select the heap-bearing variant.
            //   * heap_size: 100    — sketchlib-go's CountSketch TOPK_SIZE.
            //   * item_label: <dim> — the data-point attribute whose VALUE
            //     is the heavy-hitter "item" the heap ranks (e.g.
            //     `endpoint` for `top_endpoint_qps`); without it every
            //     observation keys by the metric NAME (degenerate single
            //     key). Derived from the metric name (see
            //     `countsketch_item_label_for`).
            //
            // CROSS-REPO DEPENDENCY: these keys (`emit_heap` / `heap_size` /
            // `item_label`) are being added to the asapedge processor's
            // `MetricFamily` config (a parallel ASAPCollector change). They
            // are pure YAML text here, so emitting them is safe even before
            // that lands — `mapstructure` ignores unknown keys by default —
            // but the warm-topk behaviour only activates once the asapedge
            // build carries the fields. See the report's cross-repo note.
            if matches!(kind, SketchAlgorithm::CountSketch) && countsketch_with_heap {
                e.insert("emit_heap".into(), Value::Bool(true));
                e.insert("heap_size".into(), Value::Number(100u64.into()));
                // Prefer the workload-declared inner dimension
                // (`metric_to_item_label`, from `WorkloadEntry::item_label`)
                // — the same generic source the HLL/CMS families read below.
                // Fall back to the metric-name convention
                // (`countsketch_item_label_for`) when a deployment's workload
                // omits the field, preserving the prior CountSketch behaviour.
                let item_label = cfg
                    .metric_to_item_label
                    .get(*metric)
                    .cloned()
                    .unwrap_or_else(|| countsketch_item_label_for(metric));
                e.insert("item_label".into(), Value::String(item_label));
            }

            // ── HLL / Count-Min inner item dimension (runtime-validation
            // bug fix) ──────────────────────────────────────────────────────
            //
            // The HLL (`unique_users_per_min` → counts distinct `user_id`)
            // and Count-Min (`endpoint_request_freq` → frequency over
            // `endpoint`) families also have a high-cardinality INNER
            // dimension that is NOT a grouping key. Without an `item_label`
            // that attribute (`user_id` / `endpoint`) stays in the sketch's
            // series key, so the agent mints one cardinality-1 HLL per
            // distinct `user_id` instead of one HLL per zone — the warm
            // HLL/CMS queries then return semantically wrong / empty results.
            //
            // We emit `item_label` for these families from the SAME generic
            // source the CountSketch family reads (`metric_to_item_label`,
            // populated from each workload entry's `item_label`). Unlike
            // CountSketch there is no metric-name fallback: the HLL/CMS inner
            // dimension (`user_id`) is not recoverable from the metric name
            // (`unique_users_per_min`), so when a workload declares no
            // `item_label` we emit none — byte-identical to before, and the
            // agent keeps its prior keying (no regression for metrics that
            // genuinely have no inner dimension).
            //
            // CROSS-REPO DEPENDENCY: HLL/CMS consumption of `item_label` is a
            // parallel ASAPCollector asapedgeprocessor change. The key is
            // pure YAML text here (`mapstructure` ignores unknown keys), so
            // emitting it is safe even before that lands; the corrected
            // keying only activates once the asapedge build carries it.
            if matches!(kind, SketchAlgorithm::Hll | SketchAlgorithm::Cms) {
                if let Some(item_label) = cfg.metric_to_item_label.get(*metric) {
                    if !item_label.is_empty() {
                        e.insert("item_label".into(), Value::String(item_label.clone()));
                    }
                }
            }

            // Sketch family IS a warm entry → warm signal = true.
            e.insert(
                "tier".into(),
                Value::String(tier_for(metric, true).to_string()),
            );
            metric_entries.push(Value::Mapping(e));
        }
    }

    // ── cold: block ─────────────────────────────────────────────────────────
    //
    // The fused processor archives per-emit Gorilla blocks. We turn the
    // cold tier ON whenever the plan declared any archive-tier metric
    // (the same `archive_tier_metrics` signal that drove the `gorillas3`
    // processor on the routing path). `block_duration` / `reorder_grace`
    // size from the archive window.
    //
    // PR #311 follow-up: `ship_endpoint` and `external_labels` now come
    // from the threaded `EdgeStageConfig` cold fields rather than a
    // derived placeholder. PR #311 lacked these fields and guessed
    // `http://<backend>:9098/ingest/gorilla` from the OTLP exporter host
    // — WRONG host AND port. The cold tier actually ships to the
    // gorilla-merger over HTTP ingest port 10908 (gRPC 10907). When a
    // construction site leaves the fields unset (`cold_ship_endpoint:
    // None` / empty `cold_external_labels`) we fall back to the single
    // named defaults (`default_cold_ship_endpoint` /
    // `default_cold_external_labels`) so the emitted endpoint is always
    // the correct merger target, never the old backend:9098 guess.
    let cold_enabled = !cfg.archive_tier_metrics.is_empty();
    let cold_block: Value = {
        // Cold window: smallest declared archive window, else the
        // pipeline window (clamped), else 60s.
        let block_secs = cfg
            .archive_tier_metrics
            .iter()
            .filter_map(|m| m.window_secs)
            .min()
            .unwrap_or(window_secs);
        // ship_endpoint: the threaded per-deploy cold ingest URL (the
        // gorilla-merger). Falls back to the named default when the
        // plan didn't carry one.
        let ship_endpoint = cfg
            .cold_ship_endpoint
            .clone()
            .unwrap_or_else(default_cold_ship_endpoint);
        // external_labels: the threaded label tuples; named default
        // (`cluster=<ASAP_CLUSTER|asap-mvp>`) when none were supplied.
        let external_labels = if cfg.cold_external_labels.is_empty() {
            default_cold_external_labels()
        } else {
            cfg.cold_external_labels.clone()
        };
        let mut m = Mapping::new();
        m.insert("enabled".into(), Value::Bool(cold_enabled));
        m.insert("ship_endpoint".into(), Value::String(ship_endpoint.clone()));
        // Cold-archive format: when the deploy opted into the lossless
        // intchunk cold-part format, emit `format: intchunk` + the
        // `coldpart_endpoint` so the agent ships to `/ingest/coldpart`
        // rather than the default gorilla-XOR fragments. `Fragment` (the
        // default) emits NEITHER key, leaving the cold block byte-identical
        // to the pre-format emit (`ship_endpoint` only).
        if cfg.cold_format == ColdFormat::Intchunk {
            m.insert("format".into(), Value::String("intchunk".to_string()));
            // coldpart_endpoint: the threaded value, else derived from the
            // fragment ship_endpoint by swapping the path to
            // `/ingest/coldpart` (same merger host:port).
            let coldpart_endpoint = cfg
                .cold_coldpart_endpoint
                .clone()
                .unwrap_or_else(|| coldpart_endpoint_from_ship(&ship_endpoint));
            m.insert("coldpart_endpoint".into(), Value::String(coldpart_endpoint));
        }
        m.insert(
            "block_duration".into(),
            Value::String(format!("{block_secs}s")),
        );
        m.insert("reorder_grace".into(), Value::String("2s".to_string()));
        let mut ext = Mapping::new();
        for (k, v) in external_labels {
            ext.insert(Value::String(k), Value::String(v));
        }
        m.insert("external_labels".into(), Value::Mapping(ext));
        Value::Mapping(m)
    };

    let mut asap_edge_block = Mapping::new();
    asap_edge_block.insert("shard_count".into(), Value::Number(shard_count.into()));
    asap_edge_block.insert(
        "window_duration".into(),
        Value::String(format!("{window_secs}s")),
    );
    // drop_original: true — aggregated metrics' raw is dropped (their
    // sum/sketch output is emitted on the flush tick). Unconfigured
    // metrics pass through raw; that is the fused processor's default,
    // independent of this knob.
    asap_edge_block.insert("drop_original".into(), Value::Bool(true));
    asap_edge_block.insert("metrics".into(), Value::Sequence(metric_entries));
    asap_edge_block.insert("cold".into(), cold_block);
    processors.insert("asap_edge".to_string(), Value::Mapping(asap_edge_block));

    // ── Exporters ──────────────────────────────────────────────────────────
    // Edge → asapquery-backend OTLP ingest. Same resolver as every other
    // edge emit (the asap-gateway hop was removed in #400).
    let (exporter_key, exporter_val) = build_otlp_exporter("data-plane", &cfg.exporter_target);
    let exporters: BTreeMap<String, Value> = [(exporter_key.clone(), exporter_val)].into();

    // ── Pipeline ─────────────────────────────────────────────────────────────
    // Single `metrics` pipeline: receivers [otlp], processors
    // [memory_limiter, cumulativetodelta?, asap_edge], exporters
    // [otlp/backend]. cumulativetodelta is omitted when no counter
    // metric is declared (sketch-only / quantile-only plans).
    let mut pipeline_processors: Vec<String> = vec!["memory_limiter".to_string()];
    if needs_cumulativetodelta {
        pipeline_processors.push("cumulativetodelta".to_string());
    }
    pipeline_processors.push("asap_edge".to_string());

    let mut pipelines: BTreeMap<String, Pipeline> = BTreeMap::new();
    pipelines.insert(
        "metrics".to_string(),
        Pipeline {
            receivers: vec!["otlp".into()],
            processors: pipeline_processors,
            exporters: vec![exporter_key.clone()],
        },
    );

    // ── No OpAMP extension (agent runs under the opamp-supervisor) ──────────
    // The supervisor injects its OWN opamp extension (→ the supervisor's local
    // OpAMP) + health_check and merges them with this remote config, loading
    // the remote config LAST. An `opamp` block here would overwrite the
    // supervisor's and make the collector dial the controller directly,
    // breaking the supervisor's control channel. So emit no opamp extension;
    // the supervisor owns the OpAMP identity (X-Agent-ID via its own config).
    let doc = CollectorYaml {
        extensions: BTreeMap::new(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        // No routing connector in the fused shape.
        connectors: BTreeMap::new(),
        exporters,
        service: ServiceSection {
            extensions: vec![],
            pipelines,
        },
    };

    serde_yaml::to_string(&doc).context("serialize edge stage config (asap_edge)")
}

/// Emit the gorillas3 (S3 archive-tier) processor YAML with all env
/// vars resolved to literal values at controller emit time. We can't
/// use bash-style `${VAR:-default}` interpolation in the emitted
/// agent YAML because the OTel collector's confmap parser treats
/// `${...}` as a provider URI (e.g. `${env:VAR}`, `${file:path}`) —
/// bash-default syntax fails with "invalid uri" at agent boot.
///
/// Substitution happens here, in the controller's process, with the
/// controller's environment as the source of truth. Operators set
/// `ASAP_MINIO_ACCESS_KEY` etc. on the controller container; the
/// emitted agent YAML carries literal values and is portable across
/// agents that don't have those env vars set.
fn build_gorillas3_yaml(window_secs: u64) -> String {
    let env_or = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let endpoint = env_or("ASAP_MINIO_ENDPOINT", "http://minio:9000");
    let access_key = env_or("ASAP_MINIO_ACCESS_KEY", "asap");
    let secret_key = env_or("ASAP_MINIO_SECRET_KEY", "asap-local-only");
    let tenant = env_or("ASAP_TENANT", "default");
    let tsdb_bucket = env_or("ASAP_GORILLA_TSDB_BUCKET", "asap-gorilla-tsdb");
    // Note: `prefix_template` placeholders (`{tenant}`, `{metric}`,
    // `{YYYY}`, …) are resolved by the gorillas3 processor at write
    // time, not by the YAML loader — they stay as literal `{...}`
    // tokens in the emitted YAML.
    //
    // Phase 2 (post-ASAPCollector#387): the legacy `bucket:` field is
    // no longer read at runtime — only `tsdb_bucket:` (the TSDB block
    // destination) drives the gorillas3 writer. We therefore stop
    // emitting `bucket:` here. The agent's gorillas3 Config struct
    // still carries a `Bucket` field for mapstructure compatibility,
    // but it stays at its zero value, which is fine post-#387.
    format!(
        "window_interval: {window_secs}s\n\
drop_original: false\n\
endpoint: \"{endpoint}\"\n\
region: us-east-1\n\
use_ssl: false\n\
access_key_id: \"{access_key}\"\n\
secret_access_key: \"{secret_key}\"\n\
prefix_template: \"{{tenant}}/{{metric}}/{{YYYY}}/{{MM}}/{{DD}}/{{HH}}/\"\n\
tenant: \"{tenant}\"\n\
max_retries: 3\n\
retry_backoff: 1s\n\
upload_timeout: 30s\n\
block_format: prometheus_tsdb\n\
tsdb_bucket: \"{tsdb_bucket}\"\n\
tsdb_block_duration: {window_secs}s\n",
    )
}

/// Map a `SketchAlgorithm` to the OTel processor name registered by the
/// patched contrib build's factory. Keep in sync with
/// `crate::physical::colored_dag::emitter::edge_processor_name`.
fn sketch_algorithm_to_processor_name(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::DDSketch => "ddsketch",
        SketchAlgorithm::Kll => "KLL",
        SketchAlgorithm::Hll => "HLL",
        SketchAlgorithm::CountSketch => "countsketch",
        SketchAlgorithm::Cms => "countmin",
        // Callers only ever pass a bare `FAMILY_ORDER` entry.
        other => unreachable!("sketch_algorithm_to_processor_name: unexpected kind {other:?}"),
    }
}

/// Map a `SketchAlgorithm` to its per-family pipeline name in the routing
/// connector layout.
fn sketch_algorithm_to_pipeline_name(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::DDSketch => "metrics/ddsketch_path",
        SketchAlgorithm::Kll => "metrics/kll_path",
        SketchAlgorithm::Hll => "metrics/hll_path",
        SketchAlgorithm::CountSketch => "metrics/countsketch_path",
        SketchAlgorithm::Cms => "metrics/countminsketch_path",
        // Callers only ever pass a bare `FAMILY_ORDER` entry.
        other => unreachable!("sketch_algorithm_to_pipeline_name: unexpected kind {other:?}"),
    }
}

/// MVP blocker B3 — compute the OTel processor name for a per-metric
/// `transform/keep_for_*` allowlist processor. OTel component-ids reject
/// dots/dashes/slashes in the `<type>/<name>` form, so we sanitise the
/// metric name by replacing every non-`[A-Za-z0-9_]` byte with `_`.
fn transform_keep_processor_name(metric: &str) -> String {
    let sanitised: String = metric
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("transform/keep_for_{sanitised}")
}

/// ASAPCollector#403 — compute the OTel processor name for a per-metric
/// Sum-by-grouping edge-aggregation processor. Same component-id
/// sanitisation as [`transform_keep_processor_name`].
fn metricstransform_groupby_processor_name(metric: &str) -> String {
    let sanitised: String = metric
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("metricstransform/sumby_{sanitised}")
}

/// ASAPCollector#403 — compute the dedicated edge-aggregation pipeline
/// name for a Sum-role metric.
fn sum_aggregate_pipeline_name(metric: &str) -> String {
    let sanitised: String = metric
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("metrics/sum_aggregate_{sanitised}")
}

/// ASAPCollector#403 — build the `metricstransform` processor block that
/// Sum-by-grouping aggregates a Sum-role counter AT THE EDGE.
///
/// Emits a block of the form:
/// ```yaml
/// transforms:
///   - include: http_requests_total
///     match_type: strict
///     action: update
///     operations:
///       - action: aggregate_labels
///         label_set: ["zone"]
///         aggregation_type: sum
/// ```
///
/// `aggregate_labels` aggregates away every datapoint attribute EXCEPT
/// the ones in `label_set`, summing the datapoints that collapse onto
/// the same grouping-label tuple. Running on the already-delta stream
/// (cumulativetodelta is upstream on the entry pipeline), each export
/// carries one summed series per grouping-label tuple instead of one per
/// full wire-attr tuple — the bandwidth fix. The backend's
/// `evaluate_exact_agg` / `SumAccumulator` fold these per-window exactly
/// as they would the raw deltas, just at reduced cardinality, so
/// `sum by (<labels>) (metric)` and per-group `rate` produce the
/// identical answer.
///
/// `match_type: strict` keeps this a no-op for every other metric routed
/// through the pipeline.
///
/// Empty `labels` ⇒ `label_set: []` — collapses to one global series per
/// metric (the planner's signal for an ungrouped Sum).
fn build_metricstransform_groupby_processor_block(metric: &str, labels: &[String]) -> Value {
    let labels_array: String = if labels.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> = labels.iter().map(|l| format!("\"{l}\"")).collect();
        format!("[{}]", quoted.join(", "))
    };
    let yaml = format!(
        "transforms:\n  - include: {metric}\n    match_type: strict\n    action: update\n    operations:\n      - action: aggregate_labels\n        label_set: {labels_array}\n        aggregation_type: sum\n",
    );
    serde_yaml::from_str(&yaml)
        .expect("metricstransform/sumby_* yaml is well-formed by construction")
}

/// MVP blocker B3 — build the OTTL `transform` processor block that
/// reduces a metric's data-point attributes to its grouping-label set.
///
/// Emits a block of the form:
/// ```yaml
/// error_mode: ignore
/// metric_statements:
///   - keep_keys(datapoint.attributes, ["zone"]) where metric.name == "<metric>"
/// ```
///
/// The `where metric.name == "<metric>"` guard makes the statement a
/// no-op on any metric routed through this pipeline that isn't the one
/// this processor was minted for — per-family pipelines see ALL metrics
/// routed to that family by the connector, not just the controller's
/// currently-planned one.
///
/// `error_mode: ignore` mirrors the contrib examples: if a metric
/// arrives without the gating-label attrs (e.g. during early-life
/// startup before exporters have populated resource attrs), the
/// processor logs and continues rather than dropping the whole batch.
///
/// Empty `labels` is supported — `keep_keys(datapoint.attributes, [])`
/// strips every attr (planner's signal for one global sid per metric).
fn build_transform_keep_processor_block(metric: &str, labels: &[String]) -> Value {
    let labels_array: String = if labels.is_empty() {
        "[]".to_string()
    } else {
        let quoted: Vec<String> = labels.iter().map(|l| format!("\"{l}\"")).collect();
        format!("[{}]", quoted.join(", "))
    };
    let yaml = format!(
        "error_mode: ignore\nmetric_statements:\n  - keep_keys(datapoint.attributes, {labels_array}) where metric.name == \"{metric}\"\n",
    );
    serde_yaml::from_str(&yaml).expect("transform/keep_for_* yaml is well-formed by construction")
}

/// Build a default-parameter processor block for a `SketchAlgorithm` when
/// the planner's `metric_to_family` references a family that
/// `cfg.sketch_processors` didn't enumerate. Defaults match the catalog
/// values used by the planner's L4 rules so the wire shape is what the
/// rest of the system expects when a metric is later re-routed onto
/// this family.
fn build_default_edge_processor_block(
    kind: &SketchAlgorithm,
    window_secs: Option<u64>,
    metric_name_hint: Option<&str>,
    sample_p: Option<f64>,
) -> Value {
    // `kind` is always one of the 5 bare `FAMILY_ORDER` entries (every
    // caller normalizes through `base_family` first) — used as-is for
    // the tag/processor-name lookups below, which are keyed on the bare
    // family. `stored_kind`/`params` are what actually land on the
    // synthesized processor; `CountSketch`'s default stays heap-bearing
    // (matching this function's pre-`SketchAlgorithm`-split default of
    // `with_heap: true` — `Cms`'s default was `with_heap: false` and
    // stays bare).
    let (stored_kind, params) = match kind {
        SketchAlgorithm::DDSketch => (
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch { alpha: 0.01 },
        ),
        SketchAlgorithm::Kll => (SketchAlgorithm::Kll, SketchParams::Kll { k: 200 }),
        SketchAlgorithm::Hll => (SketchAlgorithm::Hll, SketchParams::Hll { precision: 14 }),
        SketchAlgorithm::CountSketch => (
            SketchAlgorithm::CountSketchWithHeap,
            SketchParams::CountSketchWithHeap {
                width: 2048,
                depth: 5,
                heap_size: 10,
            },
        ),
        SketchAlgorithm::Cms => (
            SketchAlgorithm::Cms,
            SketchParams::Cms {
                width: 4096,
                depth: 4,
            },
        ),
        other => unreachable!("build_default_edge_processor_block: unexpected kind {other:?}"),
    };
    let synthetic = EdgeSketchProcessor {
        processor_name: sketch_algorithm_to_processor_name(kind).to_string(),
        sketch_algorithm: stored_kind,
        sketch_params: params,
        aggregation_id: format!("agg_default_{}", sketch_algorithm_tag(kind)),
    };
    build_edge_processor_block(&synthetic, window_secs, &[], metric_name_hint, sample_p)
}

/// Resolve an `ExportTarget` to a concrete `endpoint:port` string. Phase
/// B uses documented placeholder hostnames (`data-plane:4317` for the
/// edge→backend default; `gateway:4317` is reachable when a caller
/// explicitly opts in via `default_host`) for symbolic stages — Phase C
/// plumbs a real `DeploymentConstraints::executors()` resolver.
fn resolve_export_endpoint(default_host: &str, target: &ExportTarget) -> String {
    // The backend/gateway OTLP ingest port is normally 4317. Single-host
    // deployments (e.g. the single-node MVP collapse) run the agent's OTLP
    // *receiver* and the data_plane's OTLP *ingest* on the same host under
    // `--network host`, where both default to :4317 and collide. Let the
    // ingest port be overridden via `ASAP_EDGE_BACKEND_OTLP_PORT` so the
    // agent exports to a non-colliding data_plane port while its receiver
    // keeps :4317. Defaults to 4317 → 4-node behavior is unchanged.
    let backend_port =
        std::env::var("ASAP_EDGE_BACKEND_OTLP_PORT").unwrap_or_else(|_| "4317".to_string());
    match target {
        ExportTarget::Endpoint(s) => s.clone(),
        ExportTarget::Stage(StageId::Edge) => "edge:4317".to_string(),
        ExportTarget::Stage(StageId::Gateway) => format!("{default_host}:{backend_port}"),
        ExportTarget::Stage(StageId::Backend) => format!("{default_host}:{backend_port}"),
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
/// Compute the `(epsilon, delta)` pair the standalone `countsketchprocessor`
/// must receive so its internal `configDimensions` re-derivation produces
/// EXACTLY `cols == w` and `rows == d` — the same dimensions the fused
/// asapedge path emits as `{rows, cols}` and the backend serialises as
/// `{w, d}` in `sketch_params_to_json`.
///
/// The processor recomputes (see `countsketchprocessor/config_translate.go`):
///   cols = nextPowerOfTwo(ceil(1 / epsilon^2))   (clamped to >= 2)
///   rows = ceil(ln(1 / delta))                    (clamped to >= 1)
///
/// Inverting (with float-robust targets — see below):
///   epsilon = 1/sqrt(w - 0.5) so 1/epsilon^2 == w - 0.5, whose ceil is `w`.
///     Targeting the half-integer `w - 0.5` (rather than exactly `w`) keeps
///     `ceil(1/epsilon^2)` pinned to `w` even after sqrt/square float error
///     nudges the value a few ULPs in either direction. For any planner
///     width `w >= 2`, `w - 0.5 > w/2`, so `nextPowerOfTwo(w) == w` whenever
///     the planner sizes `w` as a power of two (it does), and otherwise
///     rounds up to the next power of two consistently for both the agent
///     and any width-derived fingerprint.
///   delta = e^-(d - 0.5) so ln(1/delta) == d - 0.5, whose ceil is `d`. Same
///     half-integer trick guards `ceil(ln(1/delta))` against float drift.
///
/// Returns `(epsilon, delta)`. Both are strictly in `(0, 1)` for `w >= 2`
/// and `d >= 1` (the processor's `Config.Validate` requires that open
/// interval), which the planner always satisfies.
fn countsketch_epsilon_delta_for(w: u32, d: u32) -> (f64, f64) {
    // Guard against degenerate planner output: a width of 0/1 or depth of 0
    // would make the processor clamp anyway; pick the smallest legal sketch
    // (w=2, d=1) so epsilon/delta stay inside the validator's open interval.
    let w = w.max(2);
    let d = d.max(1);
    let epsilon = 1.0 / (w as f64 - 0.5).sqrt();
    let delta = (-(d as f64 - 0.5)).exp();
    (epsilon, delta)
}

fn build_edge_processor_block(
    sp: &EdgeSketchProcessor,
    window_secs: Option<u64>,
    label_filters: &[(String, String)],
    metric_name_hint: Option<&str>,
    sample_p: Option<f64>,
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
        SketchParams::Kll { k } => {
            m.insert("k".into(), Value::Number((*k as u64).into()));
            // No delta_transmission for KLL: see comment above.
        }
        SketchParams::DDSketch { alpha } => {
            m.insert("relative_accuracy".into(), Value::Number((*alpha).into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::Hll { .. } => {
            // HLL takes no precision knob in its Config (the
            // patched build hard-codes p=14); nothing further to set.
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
            // Per-metric sampling: HLL's processor honours `sample_p`
            // (hash-threshold element sampling in sketchlib-go).
            insert_sample_p(&mut m, sample_p);
        }
        SketchParams::Cms { width, depth } | SketchParams::CmsWithHeap { width, depth, .. } => {
            m.insert(
                "metric_name".into(),
                Value::String(
                    metric_name_hint
                        .unwrap_or("endpoint_request_freq")
                        .to_string(),
                ),
            );
            m.insert("rows".into(), Value::Number((*depth as u64).into()));
            m.insert("columns".into(), Value::Number((*width as u64).into()));
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
            // Per-metric sampling: the CMS processor honours `sample_p`
            // (geometric admission sampling in sketchlib-go).
            insert_sample_p(&mut m, sample_p);
        }
        SketchParams::CountSketch { width, depth }
        | SketchParams::CountSketchWithHeap { width, depth, .. } => {
            // P1-3: the standalone `countsketchprocessor` Config exposes ONLY
            // `epsilon` / `delta` (no `rows` / `cols` mapstructure keys), and
            // it RE-DERIVES the sketch dimensions internally via
            // `configDimensions`:
            //   cols = nextPowerOfTwo(ceil(1 / epsilon^2))
            //   rows = ceil(ln(1 / delta))
            // The old `epsilon = e/w`, `delta = 2^-d` translation fed that
            // formula a width of `nextPow2(ceil(w^2/e^2))` — wildly larger
            // than `w` — so the agent's CountSketch width never matched the
            // backend's `parameters["w"]` (= `p.w`, see `sketch_params_to_json`).
            // A content-addressed PolicyFingerprint keys off that width, so
            // the agent sketch never bound to the backend sid.
            //
            // We instead invert `configDimensions` so the processor's own
            // formula reproduces EXACTLY `cols == p.w` and `rows == p.d`
            // (matching the fused asapedge path's `{rows, cols}` and the
            // backend JSON `{w, d}`):
            //   epsilon = 1/sqrt(w)  ⇒ ceil(1/epsilon^2) = ceil(w) = w
            //                          ⇒ nextPow2(w) = w   (w is a power of 2)
            //   delta   = e^-d       ⇒ ceil(ln(1/delta)) = ceil(d) = d
            let (epsilon, delta) = countsketch_epsilon_delta_for(*width, *depth);
            m.insert("epsilon".into(), Value::Number(epsilon.into()));
            m.insert("delta".into(), Value::Number(delta.into()));
            m.insert("encoding".into(), Value::String("msgpack".into()));
            m.insert("delta_transmission".into(), Value::Bool(true));
        }
        SketchParams::UnivMon { .. } | SketchParams::Kmv { .. } | SketchParams::Theta { .. } => {
            unreachable!(
                "edge sketch processor config requested for a non-sketch or unsupported \
             SketchAlgorithm; no Bind* rule in this repo produces one"
            )
        }
    }

    Value::Mapping(m)
}

/// Write the per-metric `sample_p` knob onto a sketch-processor block,
/// but ONLY when sampling is actually requested (`p < 1.0`).
///
/// `None` or `p >= 1.0` (the default / disabled state) emits no key, so
/// the agent processor's `Config.Validate` normalises the unset field to
/// `1.0` (sampling disabled) and the emitted YAML — hence the on-wire
/// sketch bytes — stays byte-identical to the pre-sampling format. Values
/// outside `(0, 1]` are dropped here too (the planner validates the range
/// before populating `metric_to_sample_p`, so this is a defensive guard).
fn insert_sample_p(m: &mut Mapping, sample_p: Option<f64>) {
    if let Some(p) = sample_p {
        if p > 0.0 && p < 1.0 {
            m.insert("sample_p".into(), Value::Number(p.into()));
        }
    }
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
    match mp.sketch_algorithm {
        SketchAlgorithm::Kll => "kllmerge".to_string(),
        SketchAlgorithm::DDSketch => "ddsketchmerge".to_string(),
        SketchAlgorithm::Hll => "hllmerge".to_string(),
        SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap => "countminsketchmerge".to_string(),
        SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap => {
            "countsketchmerge".to_string()
        }
        SketchAlgorithm::UnivMon | SketchAlgorithm::Kmv | SketchAlgorithm::Theta => unreachable!(
            "gateway_merge_processor_name: non-sketch or unsupported SketchAlgorithm; \
             no Bind* rule in this repo produces one"
        ),
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
        Value::String(sketch_algorithm_tag(&mp.sketch_algorithm).to_string()),
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
pub(crate) fn build_backend_aggregation_json(agg: &BackendAggregation) -> JsonValue {
    // Option B (post-PR-#287): when `agg_type_override` is set, use
    // it as the wire `aggregationType` and emit an empty
    // `parameters` object — bypasses the sketch_kind → backend type
    // mapping for ExactAgg(Sum/Increase/Count) rows the Replanner
    // synthesizes for non-sketch (Sum-shaped) workloads. The
    // `sketch_kind` / `sketch_params` fields carry sentinel values
    // in this case and are not emitted on the wire.
    let (aggregation_type, mut parameters) = match &agg.family {
        SummaryFamilyType::ExactAggregate(kind, _) => (
            match kind {
                ExactKind::Sum => "Sum",
                ExactKind::Count => "Count",
                ExactKind::MinMax | ExactKind::Min => "MinMax",
                ExactKind::Increase => "Increase",
                ExactKind::Rate => "Rate",
                ExactKind::IRate => "IRate",
            }
            .to_string(),
            json!({}),
        ),
        SummaryFamilyType::Sketch(kind, _) => (
            sketch_algorithm_to_backend_type(kind.algorithm()).to_string(),
            sketch_params_to_json(kind.params()),
        ),
        other => panic!("backend emitter cannot encode summary family {other:?}"),
    };
    // Carry the per-item dimension (e.g. "endpoint"/"service") into the
    // policy parameters so the data-plane ingest can record it on the CMS
    // sid and answer per-item estimate(key). Only set for item_label-mode
    // frequency sketches; a subset content-match keeps policy resolution
    // working for sketches that don't carry it.
    if let Some(label) = &agg.item_label {
        if let Some(obj) = parameters.as_object_mut() {
            obj.insert("item_label".to_string(), JsonValue::String(label.clone()));
        }
    }
    if let Some(mode) = agg.heap_update_mode {
        if let Some(obj) = parameters.as_object_mut() {
            obj.insert("weight_mode".into(), JsonValue::String(mode.into()));
            if mode == "counter_delta" {
                obj.insert("weight_scale".into(), json!(1_000_000));
            }
        }
    }
    // PromQL range selectors are (start, end]. Encode the boundary convention
    // in state identity so legacy half-open panes cannot satisfy this binding.
    if matches!(agg.aggregation_input, AggregationInput::Raw) {
        parameters["promql_right_closed"] = json!(true);
    }
    let aggregation_input = match agg.aggregation_input {
        AggregationInput::SketchEnvelope => "sketch_envelope",
        AggregationInput::Raw => "raw",
    };
    // MVP blocker B4: clamp `windowSize` so the backend's reducer keys
    // windows by the SAME size the agent's sketch processor uses. The
    // backend's `streaming-config.window_size` must match the agent's
    // `window_duration` exactly — drift here de-syncs the warm tier
    // and replay queries return NoData (the backend's pre-compute
    // engine looks for closed windows at the streaming-config size).
    // `agg.window_secs` is u64 (not Option) here; passing through
    // `clamp_window_secs(Some(_))` and unwrapping keeps the contract
    // explicit.
    let window_size =
        clamp_window_secs(Some(agg.window_secs)).expect("clamp_window_secs preserves Some");
    json!({
        "aggregationType": aggregation_type,
        "aggregationSubType": if matches!(
            &agg.family,
            planner_types::post_asap::SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::MinMax,
                _
            )
        ) { "max" } else if matches!(&agg.family, planner_types::post_asap::SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Min, _)) { "min" } else { "" },
        "metric": agg.metric_name,
        "labels": {
            "grouping": agg.grouping,
            "rollup": Vec::<String>::new(),
            "aggregated": agg.item_label.iter().cloned().collect::<Vec<_>>(),
        },
        "parameters": parameters,
        "windowSize": window_size,
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
        SketchQuery::FrequencyL2 => json!({"op": "frequency_l2"}),
        SketchQuery::FrequencyEntropy => json!({"op": "frequency_entropy"}),
        SketchQuery::Quantile { q } => json!({
            "op": "quantile",
            "q": q,
        }),
        SketchQuery::Cardinality => json!({
            "op": "cardinality",
        }),
        SketchQuery::PointCount { key, value } => json!({
            "op": "point_count",
            "key": column_ref_to_wire_key(key),
            "value": value,
        }),
        SketchQuery::TopK { k } => json!({
            "op": "topk",
            "k": k,
        }),
    }
}

/// The wire-string key for a `SketchQuery::PointCount` readout.
///
/// `SampleValue` and `Wildcard` both wire to the legacy `"*"` sentinel
/// (`physical::post_asap::rules::bind_cms_count`, retired by Step B, used the
/// literal string `"*"` to mean "all rows / no specific key"; the L5
/// emitter's per-group resolution already special-cases that string) —
/// there's no real queryable column for a plain `Count`/`Frequency`
/// readout in either case, so both collapse to the same sentinel.
fn column_ref_to_wire_key(col: &ColumnRef) -> String {
    match col {
        ColumnRef::Named(name) => name.clone(),
        ColumnRef::Qualified { table, name } => format!("{table}.{name}"),
        ColumnRef::SampleValue | ColumnRef::Wildcard => "*".to_string(),
    }
}

/// Collapse a heap-bearing `SketchAlgorithm` to its bare counterpart.
/// Identity for every other kind.
///
/// The 5-sketch routing-connector edge YAML path (`emit_edge_yaml`'s
/// `USE_5SKETCH_ROUTING` branch and its `metric_to_family` sibling)
/// keys its fixed `FAMILY_ORDER` list and lookup maps on the 5 bare
/// families only — matching the retired `physical::post_asap::SketchAlgorithm`,
/// which had no heap-bearing variant at all (`with_heap` was a
/// `SketchParams` field, invisible to anything keying on kind alone).
/// A committed heap-bearing kind (`CmsWithHeap`/`CountSketchWithHeap`,
/// from a topk binding) needs to normalize through this before it's
/// used as a key or set member in that path, or it silently fails to
/// match its bare `FAMILY_ORDER` entry.
fn base_family(kind: &SketchAlgorithm) -> SketchAlgorithm {
    match kind {
        SketchAlgorithm::CmsWithHeap => SketchAlgorithm::Cms,
        SketchAlgorithm::CountSketchWithHeap => SketchAlgorithm::CountSketch,
        other => other.clone(),
    }
}

/// Map a `SketchAlgorithm` to the backend's `AggregationType::Display`
/// string — the same mapping
/// [`crate::config::asapquery_backend::map_sketch_type_to_agg_type`] uses
/// (the strings must match `AggregationType::FromStr` in the backend's
/// `promql_utilities::query_logics::enums`).
///
/// Heap-bearing is now identity, not a params flag (`SketchAlgorithm::CmsWithHeap`
/// / `CountSketchWithHeap`, set by `BindCountSketchOnTopK` — see
/// `physical::post_asap::rules::bind_cms_topk`), so this maps on `kind` alone;
/// `params` is unused but kept for call-site stability. This is what
/// lets the backend's `policy_capability` lookup return
/// `FrequencyTopk(*WithHeap)` for heap-bearing aggregations — required
/// for `topk(...)` queries to bind to the right sids.
fn sketch_algorithm_to_backend_type(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::UnivMon => "UnivMon",
        SketchAlgorithm::DDSketch => "DDSketch",
        SketchAlgorithm::Kll => "DatasketchesKLL",
        SketchAlgorithm::Hll => "HLL",
        SketchAlgorithm::CountSketchWithHeap => "CountSketchWithHeap",
        SketchAlgorithm::CountSketch => "CountSketch",
        SketchAlgorithm::CmsWithHeap => "CountMinSketchWithHeap",
        SketchAlgorithm::Cms => "CountMinSketch",
        SketchAlgorithm::Kmv | SketchAlgorithm::Theta => unreachable!(
            "sketch_algorithm_to_backend_type: unsupported SketchAlgorithm; \
             no Bind* rule in this repo produces one"
        ),
    }
}

/// Stable lowercase tag for a `SketchAlgorithm` — used as a passthrough
/// `sketch_kind` field in YAML so downstream consumers can dispatch
/// without round-tripping through serde. Heap-bearing kinds reuse their
/// bare counterpart's tag — this field never distinguished `with_heap`
/// even before `SketchAlgorithm` split it into its own variant.
fn sketch_algorithm_tag(kind: &SketchAlgorithm) -> &'static str {
    match kind {
        SketchAlgorithm::Kll => "kll",
        SketchAlgorithm::DDSketch => "ddsketch",
        SketchAlgorithm::Hll => "hll",
        SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap => "cms",
        SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap => "count_sketch",
        SketchAlgorithm::UnivMon | SketchAlgorithm::Kmv | SketchAlgorithm::Theta => unreachable!(
            "sketch_algorithm_tag: non-sketch or unsupported SketchAlgorithm; \
             no Bind* rule in this repo produces one"
        ),
    }
}

/// Serialize a `SketchParams` payload to a flat JSON object the backend
/// can read directly without round-tripping through the controller's
/// internally-tagged enum form.
fn sketch_params_to_json(p: &SketchParams) -> JsonValue {
    match p {
        SketchParams::UnivMon {
            heap_size,
            sketch_rows,
            sketch_cols,
            layers,
        } => json!({
            "heap_size": heap_size, "sketch_rows": sketch_rows, "sketch_cols": sketch_cols, "layers": layers,
        }),
        SketchParams::Kll { k } => json!({ "k": k }),
        SketchParams::DDSketch { alpha } => json!({ "alpha": alpha }),
        SketchParams::Hll { precision } => json!({ "precision": precision }),
        SketchParams::Cms { width, depth } => json!({ "w": width, "d": depth }),
        SketchParams::CmsWithHeap {
            width,
            depth,
            heap_size,
        } => json!({
            "w": width,
            "d": depth,
            "with_heap": true,
            "heap_size": heap_size,
        }),
        // CountSketch/CountSketchWithHeap: the old arm always emitted
        // `with_heap` (from `CountSketchParams.with_heap: bool`);
        // that boolean is now the kind identity itself.
        SketchParams::CountSketch { width, depth } => {
            json!({ "w": width, "d": depth, "with_heap": false })
        }
        SketchParams::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => json!({
            "w": width,
            "d": depth,
            "with_heap": true,
            "heap_size": heap_size,
        }),
        // Exact accumulators never reach here -- see
        // `sketch_kind_to_backend_type`'s doc.
        SketchParams::Kmv { .. } | SketchParams::Theta { .. } => {
            unreachable!(
                "sketch_params_to_json: non-sketch or unsupported SummaryParams; \
             no Bind* rule in this repo produces one"
            )
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    // P2-5: these two emitter types are used only by the test fixtures in this
    // module; gating them here keeps the non-test build free of the
    // unused-import warning they previously triggered at module scope.
    use crate::physical::colored_dag::emitter::{ArchiveTierMetric, PrometheusArchiveMetric};

    fn backend_sketch_aggregation(
        aggregation_id: &str,
        metric_name: &str,
        algorithm: SketchAlgorithm,
        params: SketchParams,
        aggregation_input: AggregationInput,
    ) -> BackendAggregation {
        BackendAggregation {
            aggregation_id: aggregation_id.into(),
            metric_name: metric_name.into(),
            family: SummaryFamilyType::Sketch(
                planner_types::post_asap::SketchKind::new(algorithm, params),
                planner_types::post_asap::GroupingStrategy::PerSubpopulationInstance,
            ),
            window_secs: 60,
            spatial_filter: String::new(),
            grouping: Vec::new(),
            item_label: None,
            heap_update_mode: None,
            aggregation_input,
        }
    }

    fn ddsketch_edge_cfg() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: vec![("service".to_string(), "api".to_string())],
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
            metric_to_family: HashMap::new(),
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        }
    }

    #[test]
    fn edge_yaml_contains_processor_and_pipeline_refs() {
        let _env = crate::test_support::env_lock();
        let yaml = emit_edge_yaml(
            &ddsketch_edge_cfg(),
            "ws://ctrl:4320/v1/opamp",
            "test-agent",
        )
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
            !yaml.contains("aggregation_id:") && !yaml.contains("sketch_algorithm:"),
            "edge processor config must not emit planning-only fields rejected by OTel configs\n{yaml}"
        );

        // Exporter — asapquery-backend OTLP ingest.
        assert!(yaml.contains("otlp/backend:"), "missing exporter\n{yaml}");
        assert!(
            yaml.contains("data-plane:4317"),
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
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.sketch_processors[0] = EdgeSketchProcessor {
            processor_name: "KLL".to_string(),
            sketch_algorithm: SketchAlgorithm::Kll,
            sketch_params: SketchParams::Kll { k: 200 },
            aggregation_id: "agg7".to_string(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        // DDSketch / HLL / CountSketch / Count-Min all support sparse
        // delta encoding — the controller emits `delta_transmission:
        // true` so the per-window wire footprint is the bucket / cell
        // diff, not the full sketch state. KLL deliberately omits the
        // flag (see `edge_yaml_kll_uses_k_param`).
        for (kind, processor_name, params) in [
            (
                SketchAlgorithm::DDSketch,
                "ddsketch",
                SketchParams::DDSketch { alpha: 0.01 },
            ),
            (
                SketchAlgorithm::Hll,
                "HLL",
                SketchParams::Hll { precision: 14 },
            ),
            (
                SketchAlgorithm::CountSketch,
                "countsketch",
                SketchParams::CountSketchWithHeap {
                    width: 2048,
                    depth: 5,
                    heap_size: 10,
                },
            ),
            (
                SketchAlgorithm::Cms,
                "countmin",
                SketchParams::Cms {
                    width: 4096,
                    depth: 4,
                },
            ),
        ] {
            let mut cfg = ddsketch_edge_cfg();
            cfg.sketch_processors[0] = EdgeSketchProcessor {
                processor_name: processor_name.to_string(),
                sketch_algorithm: kind,
                sketch_params: params,
                aggregation_id: "agg-delta".to_string(),
            };
            let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
            assert!(
                yaml.contains("delta_transmission: true"),
                "{processor_name:?} emit must carry delta_transmission: true\n{yaml}"
            );
        }
    }

    #[test]
    fn edge_yaml_countmin_includes_required_metric_name() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.source_metric = Some("endpoint_request_freq".to_string());
        cfg.sketch_processors[0] = EdgeSketchProcessor {
            processor_name: "countmin".to_string(),
            sketch_algorithm: SketchAlgorithm::Cms,
            sketch_params: SketchParams::Cms {
                width: 4096,
                depth: 4,
            },
            aggregation_id: "agg-cms".to_string(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(yaml.contains("countmin:"), "{yaml}");
        assert!(
            yaml.contains("metric_name: endpoint_request_freq"),
            "{yaml}"
        );
    }

    #[test]
    fn edge_yaml_batch_mode_when_no_window() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.window_secs = None;
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
                sketch_algorithm: SketchAlgorithm::DDSketch,
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        }
    }

    #[test]
    fn gateway_yaml_uses_family_specific_merge_name() {
        let yaml = emit_gateway_yaml(
            &ddsketch_gateway_cfg(),
            "ws://ctrl:4320/v1/opamp",
            "test-agent",
        )
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

        // Exporter targets data-plane.
        assert!(yaml.contains("data-plane:4317"), "{yaml}");

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
                    sketch_algorithm: SketchAlgorithm::Kll,
                    aggregation_id: "agg0".into(),
                },
                GatewayMergeProcessor {
                    processor_name: "x".into(),
                    sketch_algorithm: SketchAlgorithm::Hll,
                    aggregation_id: "agg1".into(),
                },
            ],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        };
        let yaml = emit_gateway_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
    fn backend_json_injects_monitors_when_present() {
        use crate::emit::monitor::{agg_id_for_metric, Functional, MonitorIntent};
        let cfg = BackendStageConfig {
            aggregations: Vec::new(),
            readouts: Vec::new(),
        };
        // No monitors → no `monitors` key (byte-compatible with pre-CDM emit).
        let v0 = emit_backend_streaming_config_json(&cfg, &[]).expect("emit");
        assert!(v0.get("monitors").is_none(), "absent when empty: {v0}");
        // A declared monitor → monitors[] with the cross-language agg_id.
        let intents = vec![MonitorIntent {
            metric: "bytes_sent".into(),
            functional: Functional::Sum,
            key: String::new(),
            coeffs: Vec::new(),
            coordinator_url: String::new(),
            tau: 1000.0,
            epsilon: 0.05,
            window_ms: 60_000,
        }];
        let v = emit_backend_streaming_config_json(&cfg, &intents).expect("emit");
        let mons = v["monitors"].as_array().expect("monitors array");
        assert_eq!(mons.len(), 1);
        assert_eq!(
            mons[0]["agg_id"].as_u64().unwrap(),
            agg_id_for_metric("bytes_sent")
        );
        assert_eq!(mons[0]["window_ms"].as_u64().unwrap(), 60_000);
    }

    #[test]
    fn backend_json_round_trips_aggregations_and_readouts() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                backend_sketch_aggregation(
                    "agg0",
                    "http_latency_ms",
                    SketchAlgorithm::DDSketch,
                    SketchParams::DDSketch { alpha: 0.01 },
                    AggregationInput::SketchEnvelope,
                ),
                backend_sketch_aggregation(
                    "agg1",
                    "http_requests_total",
                    SketchAlgorithm::Hll,
                    SketchParams::Hll { precision: 14 },
                    AggregationInput::SketchEnvelope,
                ),
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: SketchQuery::Quantile { q: 0.99 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: SketchQuery::Cardinality,
                },
            ],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");

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
                backend_sketch_aggregation(
                    "agg0",
                    "endpoint_count",
                    SketchAlgorithm::CountSketchWithHeap,
                    SketchParams::CountSketchWithHeap {
                        width: 2048,
                        depth: 5,
                        heap_size: 10,
                    },
                    AggregationInput::SketchEnvelope,
                ),
                backend_sketch_aggregation(
                    "agg1",
                    "endpoint_hits",
                    SketchAlgorithm::Cms,
                    SketchParams::Cms {
                        width: 4096,
                        depth: 4,
                    },
                    AggregationInput::SketchEnvelope,
                ),
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: SketchQuery::TopK { k: 10 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: SketchQuery::PointCount {
                        key: ColumnRef::Named("user_42".into()),
                        value: None,
                    },
                },
            ],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
        let reads = v["readouts"].as_array().unwrap();
        assert_eq!(reads[0]["op"], "topk");
        assert_eq!(reads[0]["k"], 10);
        assert_eq!(reads[1]["op"], "point_count");
        assert_eq!(reads[1]["key"], "user_42");

        let aggs = v["aggregations"].as_array().unwrap();
        // CountSketch with `with_heap: true` promotes to
        // `CountSketchWithHeap` — the backend's `policy_capability`
        // maps that to `FrequencyTopk(CountSketchWithHeap)`, the only
        // form the analyzer's `topk(...)` candidate binds against.
        assert_eq!(aggs[0]["aggregationType"], "CountSketchWithHeap");
        assert_eq!(aggs[0]["parameters"]["with_heap"], true);
        // CMS with `with_heap: false` stays plain `CountMinSketch`.
        assert_eq!(aggs[1]["aggregationType"], "CountMinSketch");
        assert_eq!(aggs[1]["parameters"]["w"], 4096);
    }

    #[test]
    fn export_target_endpoint_is_passed_through_verbatim() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.exporter_target = ExportTarget::Endpoint("custom-gw:5317".into());
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(yaml.contains("custom-gw:5317"), "{yaml}");
    }

    // ── Phase α: BackendStorageRouting emitter tests ──────────────────────

    /// Helper: build a single-aggregation BackendStageConfig of the
    /// requested kind. `aggregation_id` is hard-coded — the routing
    /// emitter doesn't care about it. Accepts the 5 canonical bare
    /// families callers actually pass; `CountSketch` stores as the
    /// heap-bearing variant (matching this fixture's pre-`SketchAlgorithm`-split
    /// behavior, when `with_heap: true` was a `CountSketchParams` field
    /// rather than a distinct kind).
    fn backend_cfg_with_kind(kind: SketchAlgorithm) -> BackendStageConfig {
        let (stored_kind, params) = match kind {
            SketchAlgorithm::DDSketch => (
                SketchAlgorithm::DDSketch,
                SketchParams::DDSketch { alpha: 0.01 },
            ),
            SketchAlgorithm::Kll => (SketchAlgorithm::Kll, SketchParams::Kll { k: 200 }),
            SketchAlgorithm::Hll => (SketchAlgorithm::Hll, SketchParams::Hll { precision: 14 }),
            SketchAlgorithm::Cms => (
                SketchAlgorithm::Cms,
                SketchParams::Cms {
                    width: 4096,
                    depth: 4,
                },
            ),
            SketchAlgorithm::CountSketch => (
                SketchAlgorithm::CountSketchWithHeap,
                SketchParams::CountSketchWithHeap {
                    width: 2048,
                    depth: 5,
                    heap_size: 10,
                },
            ),
            other => unreachable!("backend_cfg_with_kind: unsupported test fixture kind {other:?}"),
        };
        BackendStageConfig {
            aggregations: vec![backend_sketch_aggregation(
                "agg0",
                "test_metric",
                stored_kind,
                params,
                AggregationInput::SketchEnvelope,
            )],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".into(),
                op: match kind {
                    SketchAlgorithm::DDSketch | SketchAlgorithm::Kll => {
                        SketchQuery::Quantile { q: 0.99 }
                    }
                    SketchAlgorithm::Hll => SketchQuery::Cardinality,
                    SketchAlgorithm::CountSketch => SketchQuery::TopK { k: 10 },
                    SketchAlgorithm::Cms => SketchQuery::PointCount {
                        key: ColumnRef::Named("user_42".into()),
                        value: None,
                    },
                    other => unreachable!(
                        "backend_cfg_with_kind: unsupported test fixture kind {other:?}"
                    ),
                },
            }],
        }
    }

    #[test]
    fn storage_routing_emits_default_engine_and_metrics_array() {
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
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
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
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
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
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
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
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
        let cs = backend_cfg_with_kind(SketchAlgorithm::CountSketch);
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
        let hll = backend_cfg_with_kind(SketchAlgorithm::Hll);
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
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
        let hll = backend_cfg_with_kind(SketchAlgorithm::Hll);
        let cs = backend_cfg_with_kind(SketchAlgorithm::CountSketch);
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
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
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
        let cases: Vec<(SketchAlgorithm, SketchParams, &str)> = vec![
            (
                SketchAlgorithm::Kll,
                SketchParams::Kll { k: 200 },
                "DatasketchesKLL",
            ),
            (
                SketchAlgorithm::DDSketch,
                SketchParams::DDSketch { alpha: 0.01 },
                "DDSketch",
            ),
            (
                SketchAlgorithm::Hll,
                SketchParams::Hll { precision: 14 },
                "HLL",
            ),
            (
                SketchAlgorithm::Cms,
                SketchParams::Cms {
                    width: 4096,
                    depth: 4,
                },
                "CountMinSketch",
            ),
            (
                SketchAlgorithm::CmsWithHeap,
                SketchParams::CmsWithHeap {
                    width: 4096,
                    depth: 4,
                    heap_size: 10,
                },
                "CountMinSketchWithHeap",
            ),
            (
                SketchAlgorithm::CountSketch,
                SketchParams::CountSketch {
                    width: 2048,
                    depth: 5,
                },
                "CountSketch",
            ),
            (
                SketchAlgorithm::CountSketchWithHeap,
                SketchParams::CountSketchWithHeap {
                    width: 2048,
                    depth: 5,
                    heap_size: 10,
                },
                "CountSketchWithHeap",
            ),
        ];
        for (kind, _params, expected) in cases {
            assert_eq!(
                sketch_algorithm_to_backend_type(&kind),
                expected,
                "sketch_algorithm_to_backend_type({kind:?}) drift — backend FromStr will reject"
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
                grouping: vec!["zone".into(), "service".into()],
                window_secs: 30,
                ..backend_sketch_aggregation(
                    "agg0",
                    "http_latency_ms",
                    SketchAlgorithm::DDSketch,
                    SketchParams::DDSketch { alpha: 0.01 },
                    AggregationInput::SketchEnvelope,
                )
            }],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
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
    /// can't drift this off so long as they bind through SketchAlgorithm /
    /// SketchParams.
    #[test]
    fn phase_b_backend_json_aggregation_readout_alias_snapshot() {
        let cfg = BackendStageConfig {
            aggregations: vec![backend_sketch_aggregation(
                "phase_b_agg0",
                "phase_b_metric",
                SketchAlgorithm::Kll,
                SketchParams::Kll { k: 200 },
                AggregationInput::SketchEnvelope,
            )],
            readouts: vec![BackendReadout {
                aggregation_id: "phase_b_agg0".into(),
                op: SketchQuery::Quantile { q: 0.99 },
            }],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
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
            aggregations: vec![backend_sketch_aggregation(
                "agg0",
                "test_metric",
                SketchAlgorithm::DDSketch,
                SketchParams::DDSketch { alpha: 0.01 },
                AggregationInput::SketchEnvelope,
            )],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
        assert_eq!(v["aggregations"][0]["aggregationInput"], "sketch_envelope");
    }

    /// Mode 2 (raw at edge → sketch at backend) sets
    /// `aggregation_input: raw` so the backend builds the sketch from
    /// raw OTLP samples at ingest. Phase ε.2 implements the raw-input
    /// ingest path on the backend.
    #[test]
    fn phase_eps1_mode2_aggregation_input_is_raw() {
        let cfg = BackendStageConfig {
            aggregations: vec![backend_sketch_aggregation(
                "agg0",
                "test_metric",
                SketchAlgorithm::DDSketch,
                SketchParams::DDSketch { alpha: 0.01 },
                AggregationInput::Raw,
            )],
            readouts: vec![],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
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
        let ddsketch = backend_cfg_with_kind(SketchAlgorithm::DDSketch);
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
        let _env = crate::test_support::env_lock();
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
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
        let cfg = ddsketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_archive".to_string(),
            window_secs: Some(5),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        // The endpoint is resolved at controller emit time from the
        // controller's environment (`ASAP_MINIO_ENDPOINT`, falling
        // back to the docker-compose default `http://minio:9000`).
        // Bash-style `${VAR:-default}` placeholders aren't valid in
        // emitted YAML — OTel's confmap parser treats `${...}` as a
        // provider URI and rejects bash-default syntax. So we assert
        // on the resolved literal that the deploy default produces.
        assert!(
            yaml.contains("endpoint: http://minio:9000"),
            "endpoint should resolve to the docker-compose minio default\n{yaml}"
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
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_archive".to_string(),
            window_secs: Some(5),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_warm".to_string(),
            window_secs: Some(1),
        }];
        cfg.warm_passthrough_metrics = vec!["http_freshness_probe_warm".to_string()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
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
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
    // non-empty. The `five_sketch_edge_cfg` fixture maps each of its 5
    // metrics to a DISTINCT family, so its union-of-needed-families is
    // all 5 — these tests therefore still see all 5 processors and
    // pipelines. ASAPCollector#400 pruning is exercised by the
    // `mvp46_pruned_*` and `mvp46_multi_family_metric_*` tests below.
    // These tests pin:
    //   * One sketch processor in `processors:` per family some metric
    //     needs (for this fixture: all 5).
    //   * `routing` in `connectors:` (NOT `processors:`) — the real
    //     bugfix; `routingprocessor` was removed in OTel-collector
    //     v0.106 so emitting it would fail agent boot.
    //   * Entry `metrics:` + `metrics/raw_passthrough` default + one
    //     per-family pipeline per needed family (for this fixture: 5).
    //   * Each per-sketch pipeline starts with `gorillas3` when an
    //     archive tier is declared (cold-tier write happens BEFORE
    //     sketch mutation).
    //   * Freshness-probe (warm-passthrough) routing folds into
    //     `metrics/raw_passthrough` so the metric name is preserved
    //     end-to-end.

    /// Helper: wrap a single sketch family in the per-metric family SET
    /// (ASAPCollector#400). Most fixtures map each metric to exactly one
    /// family — this keeps them concise while exercising the SET-shaped
    /// `metric_to_family`.
    fn one(kind: SketchAlgorithm) -> std::collections::BTreeSet<SketchAlgorithm> {
        std::collections::BTreeSet::from([kind])
    }

    /// Helper: build a 5-metric `EdgeStageConfig` covering every sketch
    /// family per the canonical workload-spec table in MVP §46. Each
    /// metric maps to a single-family set (this workload's per-metric set
    /// size is 1; see `mvp46_multi_family_metric_*` for the size>1 case).
    fn five_sketch_edge_cfg() -> EdgeStageConfig {
        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert(
            "http_requests_total_latency_ms".into(),
            one(SketchAlgorithm::DDSketch),
        );
        metric_to_family.insert("request_size_bytes".into(), one(SketchAlgorithm::Kll));
        metric_to_family.insert("unique_users_per_min".into(), one(SketchAlgorithm::Hll));
        metric_to_family.insert("top_endpoint_qps".into(), one(SketchAlgorithm::CountSketch));
        metric_to_family.insert("endpoint_request_freq".into(), one(SketchAlgorithm::Cms));
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
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        }
    }

    #[test]
    fn mvp46_emit_loads_all_5_sketch_processors() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        for proc in ["ddsketch", "KLL", "HLL", "countsketch", "countmin"] {
            assert!(
                yaml.contains(&format!("{proc}:")),
                "missing top-level processor key {proc}\n{yaml}"
            );
        }
    }

    #[test]
    fn mvp46_default_emits_no_sample_p() {
        // Default fixture (metric_to_sample_p empty) must NOT emit any
        // `sample_p` knob — keeps the agent config byte-identical to the
        // pre-sampling format when no metric requests sampling.
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("sample_p"),
            "default (unset) sample_p must not appear in the emitted YAML\n{yaml}"
        );
    }

    #[test]
    fn mvp46_configured_sample_p_reaches_cms_and_hll_blocks() {
        // A configured per-metric `sample_p < 1` must be threaded into the
        // emitted agent sketch-processor config for the sampling-aware
        // families (CMS / HLL). This is the control-plane half of the
        // end-to-end path: workload `sample_p` → EdgeStageConfig.
        // metric_to_sample_p → build_edge_processor_block → agent YAML →
        // processor Config.SampleP → sketchlib-go WithSampleP.
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        // endpoint_request_freq → CMS, unique_users_per_min → HLL.
        cfg.metric_to_sample_p
            .insert("endpoint_request_freq".into(), 0.1);
        cfg.metric_to_sample_p
            .insert("unique_users_per_min".into(), 0.25);
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

        // The CMS block carries sample_p: 0.1.
        assert!(
            yaml.contains("sample_p: 0.1"),
            "CMS sample_p 0.1 did not reach the emitted YAML\n{yaml}"
        );
        // The HLL block carries sample_p: 0.25.
        assert!(
            yaml.contains("sample_p: 0.25"),
            "HLL sample_p 0.25 did not reach the emitted YAML\n{yaml}"
        );
    }

    #[test]
    fn mvp46_sample_p_of_one_emits_nothing() {
        // sample_p == 1.0 is the disabled state — even when present in the
        // map it must emit no knob (insert_sample_p guards on `< 1.0`).
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        cfg.metric_to_sample_p
            .insert("endpoint_request_freq".into(), 1.0);
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("sample_p"),
            "sample_p == 1.0 must not be emitted\n{yaml}"
        );
    }

    #[test]
    fn mvp46_routing_lives_in_connectors_not_processors() {
        let _env = crate::test_support::env_lock();
        // The real bugfix: OTel collector v0.106+ removed
        // `routingprocessor`; the routing component is now a
        // `routingconnector`. We MUST emit it under `connectors:`.
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        // Declare an archive-tier metric so gorillas3 is emitted.
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total_latency_ms".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        //
        // B1 follow-up: asserts on `limit_mib: 1280` (the env-var
        // default) — unset the var under the crate-wide env lock so the
        // companion `b1_memory_limiter_honours_*` tests can't race-set
        // `ASAP_AGENT_MEMORY_LIMIT_MIB=1600` mid-emit. The guard restores
        // the prior value on drop.
        let _env = crate::test_support::EnvVarGuard::unset("ASAP_AGENT_MEMORY_LIMIT_MIB");
        let mut cfg = five_sketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total_latency_ms".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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

    // ── ASAPCollector#400: per-metric required-family SET pruning ─────────
    //
    // Pre-fix the emitter loaded all 5 sketch processors and emitted all
    // 5 per-family pipelines, and the routing connector fanned EVERY
    // metric through all 5 pipelines — shipping ~5× the sketch state to
    // the backend. The fix prunes processors + pipelines to the UNION of
    // each metric's required-family SET, and routes each metric only to
    // the families in its set. These tests pin both the pruning (size-1
    // sets) and the multi-family-per-metric correctness (size>1 sets).

    /// Helper: build an `EdgeStageConfig` whose `metric_to_family` is the
    /// given metric→set map, with sensible defaults for the other fields.
    fn edge_cfg_with_families(
        metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>>,
    ) -> EdgeStageConfig {
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
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        }
    }

    #[test]
    fn mvp46_pruned_single_family_emits_only_that_family_pipeline() {
        let _env = crate::test_support::env_lock();
        // A workload with ONE metric needing ONLY DDSketch must emit the
        // DDSketch processor + pipeline and NOTHING for the other 4
        // families — this is the core bandwidth fix.
        let cfg = edge_cfg_with_families(HashMap::from([(
            "latency_ms".to_string(),
            one(SketchAlgorithm::DDSketch),
        )]));
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

        // DDSketch present (processor + pipeline).
        assert!(
            yaml.contains("ddsketch:"),
            "ddsketch processor must be present\n{yaml}"
        );
        assert!(
            yaml.contains("metrics/ddsketch_path:"),
            "ddsketch pipeline must be present\n{yaml}"
        );
        // The other 4 families MUST NOT appear — no processor key, no
        // pipeline. (Match on the YAML key forms to avoid false hits.)
        for (proc_key, pipeline_key) in [
            ("KLL:", "metrics/kll_path:"),
            ("HLL:", "metrics/hll_path:"),
            ("countsketch:", "metrics/countsketch_path:"),
            ("countmin:", "metrics/countminsketch_path:"),
        ] {
            assert!(
                !yaml.contains(proc_key),
                "unneeded processor `{proc_key}` must be pruned (#400)\n{yaml}"
            );
            assert!(
                !yaml.contains(pipeline_key),
                "unneeded pipeline `{pipeline_key}` must be pruned (#400)\n{yaml}"
            );
        }
        // Entry + default pipelines still present (graph stays closed).
        assert!(yaml.contains("metrics/raw_passthrough:"), "{yaml}");
    }

    #[test]
    fn mvp46_pruned_two_metrics_two_families_emits_exactly_those_two() {
        let _env = crate::test_support::env_lock();
        // Two metrics, each needing a single distinct family (DDSketch,
        // HLL). Exactly those two pipelines/processors must be emitted;
        // KLL/CountSketch/CMS pruned.
        let cfg = edge_cfg_with_families(HashMap::from([
            ("latency_ms".to_string(), one(SketchAlgorithm::DDSketch)),
            ("uniques".to_string(), one(SketchAlgorithm::Hll)),
        ]));
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

        for present in [
            "ddsketch:",
            "metrics/ddsketch_path:",
            "HLL:",
            "metrics/hll_path:",
        ] {
            assert!(yaml.contains(present), "expected `{present}`\n{yaml}");
        }
        for pruned in [
            "KLL:",
            "metrics/kll_path:",
            "countsketch:",
            "metrics/countsketch_path:",
            "countmin:",
            "metrics/countminsketch_path:",
        ] {
            assert!(
                !yaml.contains(pruned),
                "`{pruned}` must be pruned (#400)\n{yaml}"
            );
        }
    }

    #[test]
    fn mvp46_multi_family_metric_emits_both_pipelines_and_routes_to_both() {
        let _env = crate::test_support::env_lock();
        // ASAPCollector#400 SET semantics — the make-or-break case: a
        // SINGLE metric queried by TWO capabilities (DDSketch + HLL) must
        // (1) emit BOTH per-family pipelines + processors, and (2) route
        // that metric to BOTH pipelines in its routing-connector
        // condition. CountSketch/KLL/CMS stay pruned (no metric needs
        // them).
        let cfg = edge_cfg_with_families(HashMap::from([(
            "http_requests".to_string(),
            std::collections::BTreeSet::from([SketchAlgorithm::DDSketch, SketchAlgorithm::Hll]),
        )]));
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

        // Both families emitted.
        for present in [
            "ddsketch:",
            "metrics/ddsketch_path:",
            "HLL:",
            "metrics/hll_path:",
        ] {
            assert!(yaml.contains(present), "expected `{present}`\n{yaml}");
        }
        // The other 3 pruned.
        for pruned in [
            "metrics/kll_path:",
            "metrics/countsketch_path:",
            "metrics/countminsketch_path:",
        ] {
            assert!(
                !yaml.contains(pruned),
                "`{pruned}` must be pruned (#400)\n{yaml}"
            );
        }
        // The routing condition for http_requests lists BOTH pipelines.
        let needle = "name == \"http_requests\"";
        let n_idx = yaml
            .find(needle)
            .unwrap_or_else(|| panic!("missing routing condition for http_requests\n{yaml}"));
        let near = &yaml[n_idx..n_idx.saturating_add(256).min(yaml.len())];
        // serde_yaml may render the pipelines list inline or block-form;
        // tolerate both. Family order follows the canonical FAMILY_ORDER
        // (DDSketch before HLL).
        let inline = near.contains("[metrics/ddsketch_path, metrics/hll_path]");
        let block = near.contains("- metrics/ddsketch_path") && near.contains("- metrics/hll_path");
        assert!(
            inline || block,
            "http_requests must route to BOTH ddsketch_path AND hll_path (multi-family fan-in)\n{near}"
        );
    }

    #[test]
    fn mvp46_default_pipeline_is_raw_passthrough() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        // Freshness probes (Phase 3.2.5 Bug b) must bypass every sketch
        // processor — they route to `metrics/raw_passthrough` so the
        // metric name is preserved end-to-end.
        let mut cfg = five_sketch_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_freshness_probe_warm".into(),
            window_secs: Some(1),
        }];
        cfg.warm_passthrough_metrics = vec!["http_freshness_probe_warm".into()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
        let _env = crate::test_support::env_lock();
        // The connector is referenced as both an exporter (entry
        // pipeline) and a receiver (each per-family pipeline). This
        // pins the receiver-side wiring.
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        // Backward-compat invariant: when the planner hasn't populated
        // metric_to_family, the emitter must produce the legacy
        // single-pipeline shape (no connectors block, no per-family
        // pipelines).
        let cfg = ddsketch_edge_cfg();
        assert!(
            cfg.metric_to_family.is_empty(),
            "ddsketch_edge_cfg fixture must keep metric_to_family empty"
        );
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
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
        let _env = crate::test_support::env_lock();
        // Mode 3 (Prometheus archive) folds into the same routing
        // connector table — the `metrics/prometheus_archive` pipeline
        // is added as an additional fan-out target.
        let mut cfg = five_sketch_edge_cfg();
        cfg.prometheus_archive_metrics = vec![PrometheusArchiveMetric {
            metric: "http_requests_total".into(),
            window_secs: Some(60),
            label_proj: vec!["service.name".into()],
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

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
    // ── MVP blocker B3: attributes/keep allowlist tests ───────────────────
    //
    // The controller must inject a `transform/keep_for_<sanitized_metric>`
    // OTTL processor upstream of every sketch processor so the agent
    // reduces wire attrs to `streaming_config.grouping_labels` BEFORE
    // sketching. Without these, the agent sketches with the FULL
    // wire-attr tuple, minting one sid per unique tuple — defeating
    // the streaming-config contract and ballooning the schema endpoint
    // per-metric sid count (51 for `http_requests_total_latency_ms`
    // in the end-to-end acceptance test).
    //
    // We chose OTTL `transform` over `attributes/keep` because the
    // attributes processor has NO native allowlist action (only
    // insert/update/delete/hash). OTTL's `keep_keys(datapoint.attributes,
    // [...])` is the right primitive and the transform processor is
    // registered in the asap-otel builder-config alongside attributes,
    // filter, and groupbyattrs.

    /// Helper: 5-sketch edge cfg with per-metric grouping labels declared.
    fn five_sketch_edge_cfg_with_grouping_labels() -> EdgeStageConfig {
        let mut cfg = five_sketch_edge_cfg();
        cfg.metric_to_grouping_labels
            .insert("http_requests_total_latency_ms".into(), vec!["zone".into()]);
        cfg.metric_to_grouping_labels.insert(
            "request_size_bytes".into(),
            vec!["zone".into(), "region".into()],
        );
        cfg.metric_to_grouping_labels
            .insert("unique_users_per_min".into(), vec!["zone".into()]);
        cfg.metric_to_grouping_labels
            .insert("top_endpoint_qps".into(), vec!["endpoint".into()]);
        cfg.metric_to_grouping_labels
            .insert("endpoint_request_freq".into(), vec!["endpoint".into()]);
        cfg
    }

    #[test]
    fn b3_emits_transform_keep_processor_per_metric_with_grouping_labels() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg_with_grouping_labels();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        for metric in [
            "http_requests_total_latency_ms",
            "request_size_bytes",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            let key = format!("transform/keep_for_{metric}:");
            assert!(
                yaml.contains(&key),
                "missing transform processor block {key}\n{yaml}"
            );
        }
    }

    #[test]
    fn b3_transform_block_uses_keep_keys_ottl_with_correct_labels() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg_with_grouping_labels();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains(
                "keep_keys(datapoint.attributes, [\"zone\"]) where metric.name == \"http_requests_total_latency_ms\""
            ),
            "missing keep_keys statement for DDSketch metric\n{yaml}"
        );
        assert!(
            yaml.contains(
                "keep_keys(datapoint.attributes, [\"zone\", \"region\"]) where metric.name == \"request_size_bytes\""
            ),
            "missing keep_keys statement for KLL metric (multi-label)\n{yaml}"
        );
        let count = yaml.matches("error_mode: ignore").count();
        assert!(
            count >= 5,
            "expected at least 5 `error_mode: ignore` markers, got {count}\n{yaml}"
        );
    }

    #[test]
    fn b3_per_family_pipeline_prepends_keep_before_sketch() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg_with_grouping_labels();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        for (pipeline, metric, family_proc) in [
            (
                "metrics/ddsketch_path:",
                "http_requests_total_latency_ms",
                "ddsketch",
            ),
            ("metrics/kll_path:", "request_size_bytes", "KLL"),
            ("metrics/hll_path:", "unique_users_per_min", "HLL"),
            (
                "metrics/countsketch_path:",
                "top_endpoint_qps",
                "countsketch",
            ),
            (
                "metrics/countminsketch_path:",
                "endpoint_request_freq",
                "countmin",
            ),
        ] {
            let p_idx = yaml.find(pipeline).expect(pipeline);
            let after = &yaml[p_idx..];
            let next_offset = after[1..]
                .find("    metrics")
                .map(|x| x + 1)
                .unwrap_or(after.len());
            let section = &after[..next_offset];
            let keep_needle = format!("- transform/keep_for_{metric}");
            let k_idx = section
                .find(&keep_needle)
                .unwrap_or_else(|| panic!("missing {keep_needle} in {pipeline}\n{section}"));
            let f_idx = section
                .find(&format!("- {family_proc}"))
                .unwrap_or_else(|| panic!("{family_proc} missing in {pipeline}\n{section}"));
            assert!(
                k_idx < f_idx,
                "transform/keep_for_{metric} must come BEFORE {family_proc} in {pipeline}\n{section}"
            );
        }
    }

    // ── ASAPCollector#403: edge-aggregate Sum-role counters ────────────────

    /// A Sum-role counter (`http_requests_total`) with grouping labels
    /// and NO sketch family must be routed to its own
    /// `metrics/sum_aggregate_<metric>` pipeline carrying a
    /// `metricstransform/sumby_<metric>` processor instead of falling
    /// through to raw_passthrough.
    fn sum_role_edge_cfg() -> EdgeStageConfig {
        let mut cfg = five_sketch_edge_cfg();
        cfg.cumulative_counter_metrics = vec!["http_requests_total".into()];
        cfg.metric_to_grouping_labels
            .insert("http_requests_total".into(), vec!["zone".into()]);
        cfg
    }

    #[test]
    fn issue403_sum_role_metric_gets_metricstransform_processor() {
        let _env = crate::test_support::env_lock();
        let cfg = sum_role_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("metricstransform/sumby_http_requests_total:"),
            "missing metricstransform processor for Sum-role counter\n{yaml}"
        );
        assert!(
            yaml.contains("aggregation_type: sum"),
            "metricstransform must use aggregation_type: sum\n{yaml}"
        );
        assert!(
            yaml.contains("action: aggregate_labels"),
            "metricstransform must use aggregate_labels op\n{yaml}"
        );
    }

    #[test]
    fn issue403_metricstransform_keeps_only_grouping_labels() {
        let _env = crate::test_support::env_lock();
        let mut cfg = sum_role_edge_cfg();
        cfg.metric_to_grouping_labels.insert(
            "http_requests_total".into(),
            vec!["zone".into(), "region".into()],
        );
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        // serde_yaml may render the label_set inline or block; tolerate both.
        let inline = "label_set:\n        - zone\n        - region";
        let inline2 = "label_set: [zone, region]";
        assert!(
            yaml.contains(inline) || yaml.contains(inline2),
            "label_set must keep exactly the grouping labels\n{yaml}"
        );
    }

    #[test]
    fn issue403_sum_role_metric_routes_to_dedicated_pipeline() {
        let _env = crate::test_support::env_lock();
        let cfg = sum_role_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        // routing-table entry maps the metric to sum_aggregate pipeline
        let needle = "name == \"http_requests_total\"";
        let idx = yaml
            .find(needle)
            .unwrap_or_else(|| panic!("missing http_requests_total route\n{yaml}"));
        let near = &yaml[idx..idx.saturating_add(256).min(yaml.len())];
        assert!(
            near.contains("[metrics/sum_aggregate_http_requests_total]")
                || near.contains("- metrics/sum_aggregate_http_requests_total"),
            "Sum-role metric must route to its sum_aggregate pipeline, not raw_passthrough\n{near}"
        );
        // the dedicated pipeline exists and carries the metricstransform
        assert!(
            yaml.contains("metrics/sum_aggregate_http_requests_total:"),
            "missing sum_aggregate pipeline\n{yaml}"
        );
        let pl_idx = yaml
            .find("metrics/sum_aggregate_http_requests_total:")
            .expect("pipeline");
        let after = &yaml[pl_idx..];
        let next_offset = after[1..]
            .find("    metrics")
            .map(|x| x + 1)
            .unwrap_or(after.len());
        let section = &after[..next_offset];
        assert!(
            section.contains("- metricstransform/sumby_http_requests_total"),
            "sum_aggregate pipeline must include the metricstransform processor\n{section}"
        );
        // no sketch processor on this path
        for forbidden in ["ddsketch", "KLL", "HLL", "countsketch", "countmin"] {
            assert!(
                !section.contains(&format!("- {forbidden}")),
                "sum_aggregate pipeline must NOT include sketch processor {forbidden}\n{section}"
            );
        }
    }

    #[test]
    fn issue403_metricstransform_runs_after_gorillas3_so_cold_tier_keeps_full_card() {
        let _env = crate::test_support::env_lock();
        let mut cfg = sum_role_edge_cfg();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        let pl_idx = yaml
            .find("metrics/sum_aggregate_http_requests_total:")
            .expect("pipeline");
        let after = &yaml[pl_idx..];
        let next_offset = after[1..]
            .find("    metrics")
            .map(|x| x + 1)
            .unwrap_or(after.len());
        let section = &after[..next_offset];
        let g_idx = section
            .find("- gorillas3")
            .unwrap_or_else(|| panic!("gorillas3 missing on sum_aggregate path\n{section}"));
        let t_idx = section
            .find("- metricstransform/sumby_http_requests_total")
            .unwrap_or_else(|| panic!("metricstransform missing\n{section}"));
        assert!(
            g_idx < t_idx,
            "gorillas3 (RAW cold-tier write) must run BEFORE metricstransform collapses cardinality\n{section}"
        );
    }

    #[test]
    fn issue403_sum_role_without_grouping_labels_stays_raw_passthrough() {
        let _env = crate::test_support::env_lock();
        // No grouping labels declared ⇒ nothing to aggregate by ⇒ keep
        // the raw_passthrough default (no dedicated pipeline emitted).
        let mut cfg = five_sketch_edge_cfg();
        cfg.cumulative_counter_metrics = vec!["http_requests_total".into()];
        // intentionally NO metric_to_grouping_labels for it
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("metricstransform/sumby_http_requests_total"),
            "no edge-aggregation when no grouping labels are declared\n{yaml}"
        );
        assert!(
            !yaml.contains("metrics/sum_aggregate_http_requests_total"),
            "no dedicated pipeline when no grouping labels\n{yaml}"
        );
    }

    #[test]
    fn issue403_sketched_metric_not_edge_summed() {
        let _env = crate::test_support::env_lock();
        // A metric mapped to a sketch family must stay on its sketch path
        // even if it also appears in cumulative_counter_metrics.
        let mut cfg = five_sketch_edge_cfg_with_grouping_labels();
        // unique_users_per_min is mapped to HLL in five_sketch_edge_cfg
        cfg.cumulative_counter_metrics = vec!["unique_users_per_min".into()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("metricstransform/sumby_unique_users_per_min"),
            "sketched metric must NOT also get a Sum-by edge-aggregation processor\n{yaml}"
        );
    }

    #[test]
    fn b3_keep_lives_after_gorillas3_so_cold_tier_keeps_full_attrs() {
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg_with_grouping_labels();
        cfg.archive_tier_metrics = vec![ArchiveTierMetric {
            metric: "http_requests_total_latency_ms".into(),
            window_secs: Some(60),
        }];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        let p_idx = yaml.find("metrics/ddsketch_path:").expect("pipeline");
        let after = &yaml[p_idx..];
        let next_offset = after[1..]
            .find("    metrics")
            .map(|x| x + 1)
            .unwrap_or(after.len());
        let section = &after[..next_offset];
        let g_idx = section.find("- gorillas3").expect("gorillas3");
        let k_idx = section
            .find("- transform/keep_for_http_requests_total_latency_ms")
            .expect("keep");
        let s_idx = section.find("- ddsketch").expect("ddsketch");
        assert!(
            g_idx < k_idx && k_idx < s_idx,
            "ordering must be gorillas3 < keep_for_* < ddsketch, got g={g_idx} k={k_idx} s={s_idx}\n{section}"
        );
    }

    #[test]
    fn b3_processor_name_sanitises_metric_special_chars() {
        assert_eq!(
            transform_keep_processor_name("foo.bar-baz/qux"),
            "transform/keep_for_foo_bar_baz_qux"
        );
        assert_eq!(
            transform_keep_processor_name("http_requests_total_latency_ms"),
            "transform/keep_for_http_requests_total_latency_ms"
        );
    }

    #[test]
    fn b3_no_transform_processor_when_grouping_labels_absent() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("transform/keep_for_"),
            "no transform/keep_for_* processor should be emitted when grouping-labels map is empty\n{yaml}"
        );
        assert!(
            !yaml.contains("keep_keys(datapoint.attributes"),
            "no keep_keys OTTL statement should be emitted when grouping-labels map is empty\n{yaml}"
        );
    }

    #[test]
    fn b3_empty_grouping_label_list_emits_empty_keep_keys() {
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        cfg.metric_to_grouping_labels
            .insert("http_requests_total_latency_ms".into(), vec![]);
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains(
                "keep_keys(datapoint.attributes, []) where metric.name == \"http_requests_total_latency_ms\""
            ),
            "empty grouping_labels must emit empty-list keep_keys\n{yaml}"
        );
    }

    #[test]
    fn b3_legacy_emit_edge_yaml_injects_keep_for_source_metric() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.source_metric = Some("http_requests_total_latency_ms".to_string());
        cfg.metric_to_grouping_labels
            .insert("http_requests_total_latency_ms".into(), vec!["zone".into()]);
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("transform/keep_for_http_requests_total_latency_ms:"),
            "legacy emit must register the keep processor\n{yaml}"
        );
        assert!(
            yaml.contains(
                "keep_keys(datapoint.attributes, [\"zone\"]) where metric.name == \"http_requests_total_latency_ms\""
            ),
            "legacy emit must surface the keep_keys OTTL statement\n{yaml}"
        );
        let pipelines_idx = yaml.find("pipelines:").expect("pipelines");
        let after = &yaml[pipelines_idx..];
        let entry_idx = after.find("metrics:").expect("metrics pipeline");
        let section = &after[entry_idx..];
        let k_idx = section
            .find("- transform/keep_for_http_requests_total_latency_ms")
            .expect("keep ref in pipeline");
        let s_idx = section
            .find("- ddsketch")
            .expect("ddsketch ref in pipeline");
        assert!(
            k_idx < s_idx,
            "keep_for_* must come BEFORE ddsketch in legacy pipeline\n{section}"
        );
    }

    // ── B1-downstream Issue #2: X-Agent-ID header threading ────────────────

    /// Legacy (`emit_edge_yaml`, no `metric_to_family`) emit threads the
    /// supplied agent_id into the opamp `headers.X-Agent-ID` field so
    /// the agent re-identifies to the controller after a Docker
    /// restart triggered by a controller-pushed OpAMP config apply.
    #[test]
    fn b1_legacy_emit_threads_x_agent_id_header() {
        let _env = crate::test_support::env_lock();
        let cfg = ddsketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://ctrl:4320/v1/opamp", "agent-7").expect("emit ok");
        assert!(
            yaml.contains("X-Agent-ID:"),
            "legacy edge emit must include the X-Agent-ID header in the opamp block\n{yaml}"
        );
        assert!(
            yaml.contains("agent-7"),
            "legacy edge emit must surface the threaded agent_id value\n{yaml}"
        );
    }

    /// 5-sketch routing emit (`emit_edge_yaml_5sketch_routing`,
    /// activated by non-empty `metric_to_family`) also threads
    /// `X-Agent-ID`. This is the wire shape MVP §46 deployments push,
    /// so the header MUST be present in the routed YAML too.
    #[test]
    fn b1_5sketch_emit_threads_x_agent_id_header() {
        let _env = crate::test_support::env_lock();
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://ctrl:4320/v1/opamp", "agent-9").expect("emit ok");
        assert!(
            yaml.contains("X-Agent-ID:"),
            "5-sketch edge emit must include the X-Agent-ID header in the opamp block\n{yaml}"
        );
        assert!(
            yaml.contains("agent-9"),
            "5-sketch edge emit must surface the threaded agent_id value\n{yaml}"
        );
    }

    /// `emit_gateway_yaml` likewise threads the X-Agent-ID. The
    /// gateway role goes through the same OpAMP apply-then-restart
    /// dance and needs the same identity contract.
    #[test]
    fn b1_gateway_emit_threads_x_agent_id_header() {
        let yaml = emit_gateway_yaml(&ddsketch_gateway_cfg(), "ws://ctrl:4320/v1/opamp", "gw-3")
            .expect("emit ok");
        assert!(
            yaml.contains("X-Agent-ID:"),
            "gateway emit must include the X-Agent-ID header in the opamp block\n{yaml}"
        );
        assert!(
            yaml.contains("gw-3"),
            "gateway emit must surface the threaded agent_id value\n{yaml}"
        );
    }

    /// Broadcast callers (handle_plan / handle_rollback / replan_metric's
    /// pre-#PR loop) don't have a single agent_id in scope and pass the
    /// literal `$AGENT_ID` so the agent container's env can expand it
    /// at boot. The placeholder must survive the YAML serialiser without
    /// being mangled.
    #[test]
    fn b1_emit_preserves_dollar_agent_id_placeholder_for_broadcast() {
        let _env = crate::test_support::env_lock();
        let yaml = emit_edge_yaml(&ddsketch_edge_cfg(), "ws://c/", "$AGENT_ID").expect("emit ok");
        assert!(
            yaml.contains("$AGENT_ID"),
            "broadcast emit must preserve the $AGENT_ID env placeholder verbatim\n{yaml}"
        );
    }

    // ── B1-downstream Issue #3: ASAP_AGENT_MEMORY_LIMIT_MIB env knob ──────

    /// Default behaviour — without the env var set, the 5-sketch
    /// routing emit pins memory_limiter at 1280 MiB (matches the
    /// pre-PR fixed value, so existing 1.5 GiB cgroup deployments
    /// don't shift).
    #[test]
    fn b1_memory_limiter_defaults_to_1280_mib_when_env_unset() {
        // Unset under the crate-wide env lock; guard restores on drop.
        let _env = crate::test_support::EnvVarGuard::unset("ASAP_AGENT_MEMORY_LIMIT_MIB");
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("limit_mib: 1280"),
            "default memory_limiter must be 1280 MiB\n{yaml}"
        );
    }

    /// Operator bumps `ASAP_AGENT_MEMORY_LIMIT_MIB=1600` on the
    /// controller container → emitted YAML carries the bumped value
    /// (and `spike_limit_mib` follows the 20%-of-limit rule, clamped
    /// to at least 256 MiB).
    #[test]
    fn b1_memory_limiter_honours_asap_agent_memory_limit_mib_env() {
        // Set under the crate-wide env lock; guard restores the prior
        // value (typically unset) on drop, so no other test ever observes
        // the bumped value.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_AGENT_MEMORY_LIMIT_MIB", "1600");
        let cfg = five_sketch_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("limit_mib: 1600"),
            "operator-bumped ASAP_AGENT_MEMORY_LIMIT_MIB=1600 must flow through to the emit\n{yaml}"
        );
        // spike = max(256, 1600/5) = 320
        assert!(
            yaml.contains("spike_limit_mib: 320"),
            "spike_limit_mib must scale as max(256, limit/5) when limit is bumped\n{yaml}"
        );
    }

    // ── B1-downstream gorillas3 bucket Phase 2: drop `bucket:` ────────────

    /// ASAPCollector#387 retired the gorillas3 `Bucket` field's
    /// runtime use — only `TSDBBucket` drives writes. The controller
    /// no longer emits `bucket:`; we keep `tsdb_bucket:` (the actual
    /// write destination). We assert ABSENCE line-by-line so the
    /// `tsdb_bucket:` line (which contains the substring `bucket:`)
    /// doesn't falsely trigger the negative match.
    // ── MVP blocker B4: window-size clamp tests ───────────────────────────
    //
    // The controller derives `window_secs` from the workload's matrix-
    // selector range. Tests below pin the clamp contract:
    //   * `[30s]` (sensible inner range) → passes through unchanged
    //   * `[5m]` (300s) → clamps DOWN to MAX_WINDOW_SECS (60)
    //   * no `[range]` (analyzer's 5m fallback at the spec level) →
    //     also clamps DOWN to 60
    //   * `[1s]` (below floor) → clamps UP to MIN_WINDOW_SECS (5)
    //   * `None` (batch mode, no Window node) → stays None
    //
    // Both consumers must agree (sketch processor's window_duration in
    // the agent YAML AND BackendAggregation's windowSize in the
    // streaming-config JSON), otherwise the backend's reducer keys
    // windows by a size the agent never closes.

    #[test]
    fn b4_clamp_window_secs_in_range_passes_through() {
        assert_eq!(clamp_window_secs(Some(30)), Some(30));
        assert_eq!(
            clamp_window_secs(Some(MIN_WINDOW_SECS)),
            Some(MIN_WINDOW_SECS)
        );
        assert_eq!(
            clamp_window_secs(Some(MAX_WINDOW_SECS)),
            Some(MAX_WINDOW_SECS)
        );
    }

    #[test]
    fn b4_clamp_window_secs_above_max_clamps_down() {
        assert_eq!(clamp_window_secs(Some(300)), Some(MAX_WINDOW_SECS));
        assert_eq!(clamp_window_secs(Some(3600)), Some(MAX_WINDOW_SECS));
    }

    #[test]
    fn b4_clamp_window_secs_below_min_clamps_up() {
        assert_eq!(clamp_window_secs(Some(0)), Some(MIN_WINDOW_SECS));
        assert_eq!(clamp_window_secs(Some(1)), Some(MIN_WINDOW_SECS));
        assert_eq!(clamp_window_secs(Some(4)), Some(MIN_WINDOW_SECS));
    }

    #[test]
    fn b4_clamp_window_secs_none_passes_through() {
        assert_eq!(clamp_window_secs(None), None);
    }

    /// Pre-B4: a `[5m]` workload landed `window_duration: 300s` in the
    /// emitted YAML. Post-B4 the clamp brings it down to 60s so the
    /// sketch processor's window sits inside any sensible replay range.
    #[test]
    fn b4_edge_yaml_clamps_oversize_window_duration() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.window_secs = Some(300); // [5m] in the workload
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("window_duration: 60s"),
            "5m window must clamp to 60s in the sketch processor block\n{yaml}"
        );
        assert!(
            !yaml.contains("window_duration: 300s"),
            "unclamped 300s window must not be emitted\n{yaml}"
        );
    }

    #[test]
    fn b4_edge_yaml_preserves_inrange_window_duration() {
        let _env = crate::test_support::env_lock();
        let mut cfg = ddsketch_edge_cfg();
        cfg.window_secs = Some(30); // [30s] — canonical MVP query range
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("window_duration: 30s"),
            "30s window is inside [5, 60] and must pass through\n{yaml}"
        );
    }

    /// 5-sketch routing path applies the same clamp — every per-family
    /// processor inherits the clamped window.
    #[test]
    fn b4_5sketch_routing_clamps_window_duration_across_all_families() {
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        cfg.window_secs = Some(300);
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("window_duration: 300s"),
            "5-sketch routing must not leak unclamped 300s windows\n{yaml}",
        );
        // The 5-sketch path emits the same processor key 5x (one per
        // family); at least one must show the clamped value.
        let clamped_count = yaml.matches("window_duration: 60s").count();
        assert!(
            clamped_count >= 1,
            "5-sketch routing must emit clamped window_duration\n{yaml}",
        );
    }

    /// Streaming-config JSON `windowSize` clamps too, so the backend
    /// reducer keys windows by the SAME size the agent's sketch
    /// processor closes. Drift here de-syncs warm tier replay answers.
    #[test]
    fn b4_streaming_config_clamps_window_size() {
        use crate::physical::colored_dag::emitter::{
            AggregationInput, BackendAggregation, BackendReadout, BackendStageConfig,
        };
        use planner_types::post_asap::SketchQuery;

        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                window_secs: 300, // pre-clamp 5m
                grouping: vec!["zone".to_string()],
                ..backend_sketch_aggregation(
                    "agg0",
                    "http_requests_total_latency_ms",
                    SketchAlgorithm::DDSketch,
                    SketchParams::DDSketch { alpha: 0.01 },
                    AggregationInput::SketchEnvelope,
                )
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".to_string(),
                op: SketchQuery::Quantile { q: 0.99 },
            }],
        };
        let v = emit_backend_streaming_config_json(&cfg, &[]).expect("emit ok");
        let aggs = v
            .get("aggregations")
            .and_then(|a| a.as_array())
            .expect("aggregations");
        assert_eq!(
            aggs[0]["windowSize"].as_u64(),
            Some(MAX_WINDOW_SECS),
            "windowSize must clamp 300 → 60 so backend reducer matches the agent's emitted window\n{v}"
        );
    }

    #[test]
    fn b1_gorillas3_emit_drops_bucket_field_keeps_tsdb_bucket() {
        let yaml = build_gorillas3_yaml(60);
        let has_bare_bucket_line = yaml
            .lines()
            .any(|line| line.trim_start().starts_with("bucket:"));
        assert!(
            !has_bare_bucket_line,
            "gorillas3 emit must NOT contain a top-level `bucket:` line after \
             Phase 2 (ASAPCollector#387 made the Bucket field unread at runtime)\n{yaml}"
        );
        let has_tsdb_bucket_line = yaml
            .lines()
            .any(|line| line.trim_start().starts_with("tsdb_bucket:"));
        assert!(
            has_tsdb_bucket_line,
            "gorillas3 emit MUST keep `tsdb_bucket:` — that's the real \
             TSDB block write destination\n{yaml}"
        );
    }

    // ── Issue #298: cumulativetodelta on counter metrics ──────────────────
    //
    // OTel SDK `Counter` instruments default to cumulative temporality.
    // Backend's `SumAccumulator::update` is sum-of-deltas — fed
    // cumulative data it returns `Σ-of-cumulatives-in-window` (quadratic
    // in time; cubic after the reducer's outer sum across the lookback
    // range). The fix is to inject `cumulativetodelta` upstream of the
    // routing connector, scoped to the workload's Counter-shaped
    // metrics. Tests below pin:
    //   * presence of the processor declaration when the list is
    //     non-empty, with the listed metrics as the `include` filter,
    //     and the entry pipeline running it FIRST;
    //   * absence (legacy quantile-only behaviour) when the list is
    //     empty — backward-compat;
    //   * sort-stability so the emitted YAML is byte-stable across
    //     planner runs (HashMap iteration drift would otherwise trip
    //     the agent's no-op apply check).

    #[test]
    fn issue298_cumulativetodelta_emitted_when_counter_metrics_present() {
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        cfg.cumulative_counter_metrics = vec![
            "http_requests_total".to_string(),
            "endpoint_request_freq".to_string(),
        ];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            yaml.contains("cumulativetodelta:"),
            "expected cumulativetodelta processor declaration when \
             cumulative_counter_metrics is non-empty\n{yaml}"
        );
        // Strict match_type so the processor stays a no-op for metrics
        // not in the include list (gauges, quantile metrics).
        assert!(
            yaml.contains("match_type: strict"),
            "cumulativetodelta processor must use strict include matching\n{yaml}"
        );
        // Both metrics appear under the include.metrics list. The YAML
        // serializer drops the redundant quotes on simple identifiers
        // (`- endpoint_request_freq`); we assert on the bare list-item
        // form, which is what the agent's confmap parser will accept.
        for m in ["http_requests_total", "endpoint_request_freq"] {
            assert!(
                yaml.contains(&format!("- {m}\n")) || yaml.contains(&format!("- \"{m}\"\n")),
                "expected metric {m} as a list item in include.metrics\n{yaml}"
            );
        }
    }

    #[test]
    fn issue298_cumulativetodelta_runs_first_on_entry_pipeline() {
        let _env = crate::test_support::env_lock();
        let mut cfg = five_sketch_edge_cfg();
        cfg.cumulative_counter_metrics = vec!["http_requests_total".to_string()];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");

        // Locate the entry `metrics:` pipeline (NOT `metrics/...`) under
        // service.pipelines and verify its `processors:` list contains
        // `cumulativetodelta` ahead of any other processor (it's the
        // only entry-pipeline processor, so checking presence on the
        // entry block is sufficient + the section's `exporters: [routing]`
        // anchor proves we matched the entry pipeline).
        let pipelines_idx = yaml.find("pipelines:").expect("pipelines:");
        let after = &yaml[pipelines_idx..];
        let entry_marker = "\n    metrics:\n";
        let entry_idx = after.find(entry_marker).expect("entry pipeline");
        let entry_after = &after[entry_idx + entry_marker.len()..];
        let next_metric_pipeline = entry_after
            .find("\n    metrics/")
            .map(|x| x)
            .unwrap_or(entry_after.len());
        let section = &entry_after[..next_metric_pipeline];
        assert!(
            section.contains("- cumulativetodelta"),
            "entry pipeline must list cumulativetodelta as a processor\n{section}"
        );
        assert!(
            section.contains("- routing"),
            "entry pipeline must keep exporters: [routing]\n{section}"
        );
    }

    #[test]
    fn issue298_cumulativetodelta_omitted_when_no_counter_metrics() {
        let _env = crate::test_support::env_lock();
        // five_sketch_edge_cfg() leaves cumulative_counter_metrics
        // empty by default — verify the processor is NOT declared and
        // the entry pipeline's processors list stays empty (backward-
        // compat for quantile-only deployments).
        let cfg = five_sketch_edge_cfg();
        assert!(
            cfg.cumulative_counter_metrics.is_empty(),
            "test precondition: default cfg has no counter metrics"
        );
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        assert!(
            !yaml.contains("cumulativetodelta"),
            "cumulativetodelta processor must NOT be emitted when \
             cumulative_counter_metrics is empty\n{yaml}"
        );
    }

    #[test]
    fn issue298_cumulativetodelta_include_list_is_sorted() {
        let _env = crate::test_support::env_lock();
        // HashMap iteration is not order-stable — but the agent's
        // opampextension byte-level no-op check would otherwise apply
        // + restart on every push of the same semantic config. Mirrors
        // the BTreeMap-not-HashMap rationale on `CollectorYaml`.
        let mut cfg = five_sketch_edge_cfg();
        cfg.cumulative_counter_metrics = vec![
            "zzz_counter".to_string(),
            "aaa_counter".to_string(),
            "mmm_counter".to_string(),
        ];
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "test-agent").expect("emit ok");
        // Quotes get stripped by serde_yaml for simple identifiers;
        // probe both forms so the assertion survives either output.
        let find_any =
            |needle_a: &str, needle_b: &str| yaml.find(needle_a).or_else(|| yaml.find(needle_b));
        let a_idx = find_any("- aaa_counter\n", "- \"aaa_counter\"\n").expect("aaa_counter");
        let m_idx = find_any("- mmm_counter\n", "- \"mmm_counter\"\n").expect("mmm_counter");
        let z_idx = find_any("- zzz_counter\n", "- \"zzz_counter\"\n").expect("zzz_counter");
        assert!(
            a_idx < m_idx && m_idx < z_idx,
            "include.metrics list must be sorted for byte-stable YAML \
             (a={a_idx} m={m_idx} z={z_idx})\n{yaml}"
        );
    }

    // ── Issue #46: fused single-pipeline asap_edge emit ─────────────────────

    /// Build a fixture mirroring the hand-written fused contract
    /// (`asap-otel-agent-b6-asap-single-sketch.yaml`): five sketch
    /// families across five metrics, one Sum-by-zone counter, an archive
    /// tier (cold), and the counter-shaped sketch inputs in the
    /// cumulativetodelta list.
    fn fused_asap_edge_cfg() -> EdgeStageConfig {
        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert(
            "http_requests_total_latency_ms".into(),
            one(SketchAlgorithm::DDSketch),
        );
        metric_to_family.insert("request_size_bytes".into(), one(SketchAlgorithm::Kll));
        metric_to_family.insert("unique_users_per_min".into(), one(SketchAlgorithm::Hll));
        metric_to_family.insert("top_endpoint_qps".into(), one(SketchAlgorithm::CountSketch));
        metric_to_family.insert("endpoint_request_freq".into(), one(SketchAlgorithm::Cms));

        // Per-family params, mirroring the target config's per-entry knobs.
        let sketch_processors = vec![
            EdgeSketchProcessor {
                processor_name: "ddsketch".into(),
                sketch_algorithm: SketchAlgorithm::DDSketch,
                sketch_params: SketchParams::DDSketch { alpha: 0.01 },
                aggregation_id: "agg0".into(),
            },
            EdgeSketchProcessor {
                processor_name: "KLL".into(),
                sketch_algorithm: SketchAlgorithm::Kll,
                sketch_params: SketchParams::Kll { k: 200 },
                aggregation_id: "agg1".into(),
            },
            EdgeSketchProcessor {
                processor_name: "HLL".into(),
                sketch_algorithm: SketchAlgorithm::Hll,
                sketch_params: SketchParams::Hll { precision: 14 },
                aggregation_id: "agg2".into(),
            },
            EdgeSketchProcessor {
                processor_name: "countsketch".into(),
                sketch_algorithm: SketchAlgorithm::CountSketchWithHeap,
                sketch_params: SketchParams::CountSketchWithHeap {
                    width: 2048,
                    depth: 5,
                    heap_size: 10,
                },
                aggregation_id: "agg3".into(),
            },
            EdgeSketchProcessor {
                processor_name: "countmin".into(),
                sketch_algorithm: SketchAlgorithm::Cms,
                sketch_params: SketchParams::Cms {
                    width: 2048,
                    depth: 5,
                },
                aggregation_id: "agg4".into(),
            },
        ];

        // Sum-by-zone counter + the counter-shaped sketch inputs.
        let mut metric_to_grouping_labels: HashMap<String, Vec<String>> = HashMap::new();
        metric_to_grouping_labels.insert("http_requests_total".into(), vec!["zone".into()]);

        // Inner item dimensions for the item-counting families, mirroring
        // `mvp-workload.yaml`'s per-metric inner attribute (the high-
        // cardinality data-point attribute the sketch counts/ranks):
        //   * unique_users_per_min (HLL) → user_id
        //   * top_endpoint_qps     (CS)  → endpoint
        //   * endpoint_request_freq (CMS)→ endpoint
        let mut metric_to_item_label: HashMap<String, String> = HashMap::new();
        metric_to_item_label.insert("unique_users_per_min".into(), "user_id".into());
        metric_to_item_label.insert("top_endpoint_qps".into(), "endpoint".into());
        metric_to_item_label.insert("endpoint_request_freq".into(), "endpoint".into());

        EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors,
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: vec![ArchiveTierMetric {
                metric: "http_requests_total".into(),
                window_secs: Some(60),
            }],
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels,
            // Sum metric + counter-shaped sketch inputs (Sum-role).
            cumulative_counter_metrics: vec![
                "http_requests_total".into(),
                "endpoint_request_freq".into(),
                "unique_users_per_min".into(),
                "top_endpoint_qps".into(),
            ],
            // PR #311 follow-up: thread the real per-deploy cold ingest
            // (the gorilla-merger HTTP ingest on 10908, NOT backend:9098)
            // + an explicit external label so the emit test asserts the
            // threaded value flows through rather than the named default.
            cold_ship_endpoint: Some("http://gorilla-merger:10908/ingest/gorilla".into()),
            cold_external_labels: vec![("cluster".into(), "asap-mvp".into())],
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label,
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        }
    }

    #[test]
    fn fused_asap_edge_emits_single_pipeline_and_metrics_list() {
        // `ASAP_EDGE_FUSED` is process-global; set it under the crate-wide
        // env lock so a parallel thread can't observe this test's setenv
        // as its own input. The guard restores the prior value on drop.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        let cfg = fused_asap_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");

        // 1. Parses as YAML (round-trips through the loader).
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));

        // 2. NO routing connector in the fused shape.
        assert!(
            !yaml.contains("connectors:"),
            "fused shape must not emit a routing connector\n{yaml}"
        );
        assert!(
            !yaml.contains("raw_passthrough"),
            "fused shape has no per-family / passthrough pipelines\n{yaml}"
        );

        // 3. Single `metrics` pipeline with the exact processor list.
        let pipelines = doc
            .get("service")
            .and_then(|s| s.get("pipelines"))
            .and_then(|p| p.as_mapping())
            .expect("service.pipelines mapping");
        assert_eq!(
            pipelines.len(),
            1,
            "fused shape emits exactly one pipeline\n{yaml}"
        );
        let metrics_pl = pipelines
            .get(serde_yaml::Value::String("metrics".into()))
            .expect("metrics pipeline present");
        let procs: Vec<String> = metrics_pl
            .get("processors")
            .and_then(|p| p.as_sequence())
            .expect("processors seq")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            procs,
            vec![
                "memory_limiter".to_string(),
                "cumulativetodelta".to_string(),
                "asap_edge".to_string()
            ],
            "pipeline processor order must be [memory_limiter, cumulativetodelta, asap_edge]\n{yaml}"
        );

        // 4. asap_edge.metrics[] has the sum-by entry + every sketch entry.
        let asap_edge = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .expect("asap_edge processor present");
        assert_eq!(
            asap_edge.get("shard_count").and_then(|v| v.as_u64()),
            Some(12),
            "shard_count\n{yaml}"
        );
        assert_eq!(
            asap_edge.get("drop_original").and_then(|v| v.as_bool()),
            Some(true),
            "drop_original\n{yaml}"
        );
        assert_eq!(
            asap_edge.get("window_duration").and_then(|v| v.as_str()),
            Some("60s"),
            "window_duration\n{yaml}"
        );
        let metrics = asap_edge
            .get("metrics")
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        // One sum entry + five sketch entries.
        assert_eq!(metrics.len(), 6, "expected 6 metric entries\n{yaml}");

        let entry_for = |name: &str| -> &serde_yaml::Value {
            metrics
                .iter()
                .find(|e| e.get("metric").and_then(|m| m.as_str()) == Some(name))
                .unwrap_or_else(|| panic!("missing metrics[] entry for {name}\n{yaml}"))
        };

        // Sum-by-zone entry.
        let sum_e = entry_for("http_requests_total");
        assert_eq!(sum_e.get("family").and_then(|v| v.as_str()), Some("sum"));
        let by: Vec<String> = sum_e
            .get("aggregate_by")
            .and_then(|v| v.as_sequence())
            .expect("aggregate_by seq")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(by, vec!["zone".to_string()], "sum aggregate_by\n{yaml}");
        // tier=both: `http_requests_total` is in `archive_tier_metrics`
        // (the exact `count(...)` archive query) AND has a warm sum-by
        // aggregate, so the agent must feed BOTH the warm sum and the
        // cold gorilla archive.
        assert_eq!(
            sum_e.get("tier").and_then(|v| v.as_str()),
            Some("both"),
            "archive + warm metric must emit tier=both\n{yaml}"
        );

        // Sketch entries + params.
        let dd = entry_for("http_requests_total_latency_ms");
        assert_eq!(dd.get("family").and_then(|v| v.as_str()), Some("ddsketch"));
        assert_eq!(
            dd.get("relative_accuracy").and_then(|v| v.as_f64()),
            Some(0.01)
        );
        let kll = entry_for("request_size_bytes");
        assert_eq!(kll.get("family").and_then(|v| v.as_str()), Some("kll"));
        assert_eq!(kll.get("k").and_then(|v| v.as_u64()), Some(200));
        let hll = entry_for("unique_users_per_min");
        assert_eq!(hll.get("family").and_then(|v| v.as_str()), Some("hll"));
        let cs = entry_for("top_endpoint_qps");
        assert_eq!(
            cs.get("family").and_then(|v| v.as_str()),
            Some("countsketch")
        );
        assert_eq!(cs.get("rows").and_then(|v| v.as_u64()), Some(5));
        assert_eq!(cs.get("cols").and_then(|v| v.as_u64()), Some(2048));
        let cms = entry_for("endpoint_request_freq");
        assert_eq!(
            cms.get("family").and_then(|v| v.as_str()),
            Some("countminsketch")
        );
        assert_eq!(cms.get("rows").and_then(|v| v.as_u64()), Some(5));
        assert_eq!(cms.get("cols").and_then(|v| v.as_u64()), Some(2048));

        // ── Per-metric delta_transmission (Foundation flag) ─────────────
        // The four delta-capable families carry `delta_transmission: true`;
        // KLL OMITS the key (no delta variant — the agent's KLL path forces
        // it off and ignores the key, but we never emit it to keep the wire
        // shape clean and match `build_edge_processor_block`).
        for delta_family in [
            "http_requests_total_latency_ms",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            assert_eq!(
                entry_for(delta_family)
                    .get("delta_transmission")
                    .and_then(|v| v.as_bool()),
                Some(true),
                "delta-capable family {delta_family} must emit delta_transmission: true\n{yaml}"
            );
        }
        assert!(
            kll.get("delta_transmission").is_none(),
            "KLL must NOT carry delta_transmission (no delta variant)\n{yaml}"
        );
        // The sum entry is not a sketch and gets no delta_transmission.
        assert!(
            sum_e.get("delta_transmission").is_none(),
            "sum family must NOT carry delta_transmission\n{yaml}"
        );

        // ── mode (scope) + hll_sparse — ASAPCollector#471/#472 ──────────
        //
        // The fused fixture's item-counting / frequency families
        // (`unique_users_per_min`→HLL, `top_endpoint_qps`→CountSketch,
        // `endpoint_request_freq`→CMS) carry an item_label and NO grouping
        // label, so their effective aggregate_by is empty → genuine
        // whole-stream global aggregates → `mode: whole_stream`.
        for ws_family in [
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            assert_eq!(
                entry_for(ws_family).get("mode").and_then(|v| v.as_str()),
                Some("whole_stream"),
                "global item-counting family {ws_family} must emit mode: whole_stream\n{yaml}"
            );
            // whole_stream families never carry an aggregate_by (the scope
            // collapses grouping).
            assert!(
                entry_for(ws_family).get("aggregate_by").is_none(),
                "whole_stream family {ws_family} must NOT carry aggregate_by\n{yaml}"
            );
        }
        // The per-series quantile families (DDSketch / KLL) are NEVER
        // whole_stream and emit NO mode (per_series is the edge default —
        // keeps their YAML byte-stable vs. the pre-#471 emit).
        for ps_family in ["http_requests_total_latency_ms", "request_size_bytes"] {
            assert!(
                entry_for(ps_family).get("mode").is_none(),
                "per-series quantile family {ps_family} must NOT carry mode (per_series default)\n{yaml}"
            );
        }
        // The sum entry never carries a scope mode.
        assert!(
            sum_e.get("mode").is_none(),
            "sum family must NOT carry mode\n{yaml}"
        );
        // hll_sparse: emitted ONLY on the HLL family. The whole-stream HLL
        // here is a single high-cardinality instance → dense (false).
        assert_eq!(
            hll.get("hll_sparse").and_then(|v| v.as_bool()),
            Some(false),
            "whole_stream HLL must emit hll_sparse: false (dense)\n{yaml}"
        );
        // No non-HLL family carries hll_sparse.
        for non_hll in [
            "http_requests_total_latency_ms",
            "request_size_bytes",
            "top_endpoint_qps",
            "endpoint_request_freq",
            "http_requests_total",
        ] {
            assert!(
                entry_for(non_hll).get("hll_sparse").is_none(),
                "non-HLL family {non_hll} must NOT carry hll_sparse\n{yaml}"
            );
        }

        // ── CountSketch warm-topk heap keys (cross-repo dependency) ─────
        // The CountSketch family (`top_endpoint_qps`, planned with_heap)
        // carries the heap-bearing wire variant keys so a warm topk query
        // routes to the heap-bearing CountSketch once the asapedge build
        // gains these fields.
        assert_eq!(
            cs.get("emit_heap").and_then(|v| v.as_bool()),
            Some(true),
            "CountSketch family must emit emit_heap: true\n{yaml}"
        );
        assert_eq!(
            cs.get("heap_size").and_then(|v| v.as_u64()),
            Some(100),
            "CountSketch family must emit heap_size: 100\n{yaml}"
        );
        assert_eq!(
            cs.get("item_label").and_then(|v| v.as_str()),
            Some("endpoint"),
            "CountSketch family must emit item_label: endpoint (the heap item dim)\n{yaml}"
        );
        // The Count-Min family (no heap) must NOT carry the heap-only keys
        // (emit_heap / heap_size) but MUST carry item_label so its inner
        // dimension (`endpoint`) is folded into the sketch instead of the
        // series key.
        assert!(
            cms.get("emit_heap").is_none() && cms.get("heap_size").is_none(),
            "Count-Min (no heap) must NOT carry the CountSketch heap-only keys\n{yaml}"
        );
        assert_eq!(
            cms.get("item_label").and_then(|v| v.as_str()),
            Some("endpoint"),
            "Count-Min family must emit item_label: endpoint (its inner dimension)\n{yaml}"
        );
        // The HLL family must carry item_label (its distinct-count dimension,
        // `user_id`) but NONE of the CountSketch heap-only keys.
        assert_eq!(
            hll.get("item_label").and_then(|v| v.as_str()),
            Some("user_id"),
            "HLL family must emit item_label: user_id (its distinct-count dimension)\n{yaml}"
        );
        assert!(
            hll.get("emit_heap").is_none() && hll.get("heap_size").is_none(),
            "HLL must NOT carry the CountSketch heap-only keys\n{yaml}"
        );
        // DDSketch / KLL have no inner item dimension → no item_label.
        assert!(
            dd.get("item_label").is_none() && kll.get("item_label").is_none(),
            "DDSketch / KLL must NOT carry item_label (no inner item dimension)\n{yaml}"
        );

        // tier=warm: the five sketch-only metrics are NOT in
        // `archive_tier_metrics` (no exact/archive query), so the agent
        // builds their warm sketch ONLY and does NOT cold-archive them —
        // exactly the bandwidth win this contract buys.
        for sketch_only in [
            "http_requests_total_latency_ms",
            "request_size_bytes",
            "unique_users_per_min",
            "top_endpoint_qps",
            "endpoint_request_freq",
        ] {
            assert_eq!(
                entry_for(sketch_only).get("tier").and_then(|v| v.as_str()),
                Some("warm"),
                "sketch-only metric {sketch_only} must emit tier=warm\n{yaml}"
            );
        }

        // 5. cold: block present + enabled. The ship_endpoint and
        // external label come from the THREADED `EdgeStageConfig` cold
        // fields (PR #311 follow-up), NOT a derived placeholder: the cfg
        // sets `cold_ship_endpoint = http://gorilla-merger:10908/...`
        // and `cold_external_labels = [(cluster, asap-mvp)]`, and the
        // emitter must surface exactly those — proving the threading,
        // and proving we no longer emit the wrong `backend:9098` guess.
        let cold = asap_edge.get("cold").expect("cold block present");
        assert_eq!(
            cold.get("enabled").and_then(|v| v.as_bool()),
            Some(true),
            "cold.enabled\n{yaml}"
        );
        assert_eq!(
            cold.get("ship_endpoint").and_then(|v| v.as_str()),
            Some("http://gorilla-merger:10908/ingest/gorilla"),
            "cold.ship_endpoint must be the threaded gorilla-merger ingest (10908), \
             not the old backend:9098 placeholder\n{yaml}"
        );
        assert!(
            !yaml.contains(":9098"),
            "must not emit the wrong backend:9098 cold endpoint\n{yaml}"
        );
        assert_eq!(
            cold.get("block_duration").and_then(|v| v.as_str()),
            Some("60s")
        );
        assert_eq!(
            cold.get("external_labels")
                .and_then(|v| v.get("cluster"))
                .and_then(|v| v.as_str()),
            Some("asap-mvp"),
            "cold.external_labels.cluster must be the threaded value\n{yaml}"
        );

        // 6. cumulativetodelta lists the Sum-role counters (strict).
        let ctd = doc
            .get("processors")
            .and_then(|p| p.get("cumulativetodelta"))
            .and_then(|c| c.get("include"))
            .expect("cumulativetodelta.include present");
        assert_eq!(
            ctd.get("match_type").and_then(|v| v.as_str()),
            Some("strict")
        );
        let ctd_metrics: Vec<String> = ctd
            .get("metrics")
            .and_then(|v| v.as_sequence())
            .expect("ctd metrics seq")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert!(
            ctd_metrics.contains(&"http_requests_total".to_string())
                && ctd_metrics.contains(&"top_endpoint_qps".to_string()),
            "cumulativetodelta must include sum + counter-shaped sketch inputs\n{yaml}"
        );

        // 7. No OpAMP extension — the agent runs under the opamp-supervisor,
        // which injects its own opamp extension (see emit_edge_yaml_asap_edge).
        // The OTLP exporter is still wired.
        assert!(
            !yaml.contains("opamp"),
            "fused emit must NOT carry an opamp extension (supervisor-managed)\n{yaml}"
        );
        assert!(yaml.contains("otlp/backend:"), "{yaml}");
    }

    /// ASAPCollector#471/#472 — a `count by (region)(distinct user_id)` style
    /// query lands a per-GROUP HLL: grouping_labels=[region], item_label=user_id.
    /// The emit must key the sketch per region (`aggregate_by: [region]`), stay
    /// `per_series` (NO `mode`, since the scope is not whole-stream), and opt the
    /// HLL into the sparse base (`hll_sparse: true`) — most regions are
    /// low-cardinality so the sparse base is a memory win that auto-promotes.
    #[test]
    fn fused_asap_edge_per_group_hll_is_per_series_and_sparse() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert("distinct_users_by_region".into(), one(SketchAlgorithm::Hll));

        let mut metric_to_grouping_labels: HashMap<String, Vec<String>> = HashMap::new();
        metric_to_grouping_labels.insert("distinct_users_by_region".into(), vec!["region".into()]);

        let mut metric_to_item_label: HashMap<String, String> = HashMap::new();
        metric_to_item_label.insert("distinct_users_by_region".into(), "user_id".into());

        // A small / below-crossover cardinality hint must NOT flip the
        // per-series HLL to dense — it stays sparse (the PR #358 default).
        let mut metric_to_distinct_keys: HashMap<String, u64> = HashMap::new();
        metric_to_distinct_keys.insert("distinct_users_by_region".into(), DENSE_CROSSOVER - 1);

        let cfg = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "HLL".into(),
                sketch_algorithm: SketchAlgorithm::Hll,
                sketch_params: SketchParams::Hll { precision: 14 },
                aggregation_id: "agg0".into(),
            }],
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels,
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys,
            metric_to_item_label,
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("metrics"))
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        let hll = metrics
            .iter()
            .find(|e| e.get("metric").and_then(|m| m.as_str()) == Some("distinct_users_by_region"))
            .expect("HLL entry present");

        // Per-group keying: region survives, user_id (item_label) is excluded.
        let by: Vec<String> = hll
            .get("aggregate_by")
            .and_then(|v| v.as_sequence())
            .expect("aggregate_by seq")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            by,
            vec!["region".to_string()],
            "per-group aggregate_by\n{yaml}"
        );
        assert_eq!(
            hll.get("item_label").and_then(|v| v.as_str()),
            Some("user_id"),
            "HLL item_label preserved\n{yaml}"
        );
        // Per_series (non-empty effective aggregate_by) → NO mode emitted.
        assert!(
            hll.get("mode").is_none(),
            "per-group HLL must NOT carry mode (per_series default)\n{yaml}"
        );
        // Per_series HLL with a below-crossover cardinality hint opts into the
        // sparse base.
        assert_eq!(
            hll.get("hll_sparse").and_then(|v| v.as_bool()),
            Some(true),
            "per-series HLL (below-crossover hint) must emit hll_sparse: true\n{yaml}"
        );
    }

    /// ASAPCollector#472 follow-up — a per-series HLL whose declared
    /// `distinct_keys_per_window` is at or above [`DENSE_CROSSOVER`] is emitted
    /// DENSE (`hll_sparse: false`): the sparse base would promote almost
    /// immediately, so starting sparse only pays one-time promotion churn.
    /// Below-crossover / unset hints keep the PR #358 default (sparse) — proven
    /// by [`fused_asap_edge_per_group_hll_is_per_series_and_sparse`].
    #[test]
    fn fused_asap_edge_per_series_hll_high_cardinality_hint_is_dense() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert("distinct_users_by_region".into(), one(SketchAlgorithm::Hll));

        let mut metric_to_grouping_labels: HashMap<String, Vec<String>> = HashMap::new();
        metric_to_grouping_labels.insert("distinct_users_by_region".into(), vec!["region".into()]);

        let mut metric_to_item_label: HashMap<String, String> = HashMap::new();
        metric_to_item_label.insert("distinct_users_by_region".into(), "user_id".into());

        // High-cardinality hint (>= crossover) → dense.
        let mut metric_to_distinct_keys: HashMap<String, u64> = HashMap::new();
        metric_to_distinct_keys.insert("distinct_users_by_region".into(), DENSE_CROSSOVER * 4);

        let cfg = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "HLL".into(),
                sketch_algorithm: SketchAlgorithm::Hll,
                sketch_params: SketchParams::Hll { precision: 14 },
                aggregation_id: "agg0".into(),
            }],
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels,
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys,
            metric_to_item_label,
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("metrics"))
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        let hll = metrics
            .iter()
            .find(|e| e.get("metric").and_then(|m| m.as_str()) == Some("distinct_users_by_region"))
            .expect("HLL entry present");

        // Still per_series (non-empty effective aggregate_by) → no `mode`.
        assert!(
            hll.get("mode").is_none(),
            "per-group HLL must NOT carry mode (per_series default)\n{yaml}"
        );
        // High-cardinality hint flips the sparse default to dense.
        assert_eq!(
            hll.get("hll_sparse").and_then(|v| v.as_bool()),
            Some(false),
            "per-series HLL with high-cardinality hint must emit hll_sparse: false (dense)\n{yaml}"
        );
    }

    /// ASAPCollector#472 follow-up — a WHOLE-STREAM HLL is dense regardless of
    /// the cardinality hint: even a tiny declared cardinality cannot flip the
    /// single-instance global aggregate to sparse (the scope rule wins).
    #[test]
    fn fused_asap_edge_whole_stream_hll_is_dense_regardless_of_hint() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert("distinct_users_global".into(), one(SketchAlgorithm::Hll));

        // No grouping label + an item_label ⇒ effective aggregate_by empty ⇒
        // whole-stream HLL.
        let mut metric_to_item_label: HashMap<String, String> = HashMap::new();
        metric_to_item_label.insert("distinct_users_global".into(), "user_id".into());

        // A tiny (below-crossover) hint MUST be ignored for whole-stream.
        let mut metric_to_distinct_keys: HashMap<String, u64> = HashMap::new();
        metric_to_distinct_keys.insert("distinct_users_global".into(), 1);

        let cfg = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "HLL".into(),
                sketch_algorithm: SketchAlgorithm::Hll,
                sketch_params: SketchParams::Hll { precision: 14 },
                aggregation_id: "agg0".into(),
            }],
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys,
            metric_to_item_label,
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("metrics"))
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        let hll = metrics
            .iter()
            .find(|e| e.get("metric").and_then(|m| m.as_str()) == Some("distinct_users_global"))
            .expect("HLL entry present");

        assert_eq!(
            hll.get("mode").and_then(|v| v.as_str()),
            Some("whole_stream"),
            "global HLL must be whole_stream\n{yaml}"
        );
        assert_eq!(
            hll.get("hll_sparse").and_then(|v| v.as_bool()),
            Some(false),
            "whole_stream HLL must emit hll_sparse: false (dense) regardless of hint\n{yaml}"
        );
    }

    /// Byte-stability guard: a metric set whose families are ALL per-series
    /// quantile (DDSketch / KLL) with no grouping emits NEITHER `mode` nor
    /// `hll_sparse` anywhere — proving the scope/sparse mapping leaves
    /// pre-#471/#472 plans byte-identical (per_series is the edge default).
    #[test]
    fn fused_asap_edge_quantile_only_omits_mode_and_sparse() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert("latency_ms".into(), one(SketchAlgorithm::DDSketch));
        metric_to_family.insert("payload_bytes".into(), one(SketchAlgorithm::Kll));

        let cfg = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![
                EdgeSketchProcessor {
                    processor_name: "ddsketch".into(),
                    sketch_algorithm: SketchAlgorithm::DDSketch,
                    sketch_params: SketchParams::DDSketch { alpha: 0.01 },
                    aggregation_id: "agg0".into(),
                },
                EdgeSketchProcessor {
                    processor_name: "KLL".into(),
                    sketch_algorithm: SketchAlgorithm::Kll,
                    sketch_params: SketchParams::Kll { k: 200 },
                    aggregation_id: "agg1".into(),
                },
            ],
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");
        assert!(
            !yaml.contains("mode:"),
            "quantile-only plan must emit no scope `mode:`\n{yaml}"
        );
        assert!(
            !yaml.contains("hll_sparse"),
            "quantile-only plan (no HLL) must emit no hll_sparse\n{yaml}"
        );
    }

    #[test]
    fn cold_format_default_fragment_emits_no_format_keys() {
        // Default cold_format (Fragment) must NOT emit `format:` or
        // `coldpart_endpoint:` in the agent `cold:` block — the cold block
        // stays byte-identical to the pre-format emit (ship_endpoint only),
        // so there is NO behavior change when the operator leaves the knob
        // unset. The default fixture builds with ColdFormat::default().
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let cfg = fused_asap_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");

        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let cold = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("cold"))
            .expect("cold block present");
        assert!(
            cold.get("format").is_none(),
            "default (fragment) cold block must NOT carry a `format:` key\n{yaml}"
        );
        assert!(
            cold.get("coldpart_endpoint").is_none(),
            "default (fragment) cold block must NOT carry a `coldpart_endpoint:` key\n{yaml}"
        );
        // The fragment ship_endpoint is unchanged.
        assert_eq!(
            cold.get("ship_endpoint").and_then(|v| v.as_str()),
            Some("http://gorilla-merger:10908/ingest/gorilla"),
            "fragment ship_endpoint must be unchanged\n{yaml}"
        );
    }

    #[test]
    fn cold_format_intchunk_emits_format_and_derived_coldpart_endpoint() {
        // When the deploy opts into the intchunk cold-part format, the
        // emitted agent `cold:` block must carry `format: intchunk` and a
        // `coldpart_endpoint:` derived from the fragment ship_endpoint
        // (same merger host:port, `/ingest/coldpart` path). The
        // ship_endpoint (fragment target) is still emitted unchanged.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let mut cfg = fused_asap_edge_cfg();
        cfg.cold_format = ColdFormat::Intchunk;
        // cold_coldpart_endpoint left None ⇒ derive from ship_endpoint.
        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");

        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let cold = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("cold"))
            .expect("cold block present");
        assert_eq!(
            cold.get("format").and_then(|v| v.as_str()),
            Some("intchunk"),
            "intchunk cold block must carry `format: intchunk`\n{yaml}"
        );
        assert_eq!(
            cold.get("coldpart_endpoint").and_then(|v| v.as_str()),
            Some("http://gorilla-merger:10908/ingest/coldpart"),
            "coldpart_endpoint must be derived from the ship_endpoint (\
             same merger host:port, /ingest/coldpart path)\n{yaml}"
        );
        // The fragment ship_endpoint stays present (the agent still knows
        // the fragment target; only the active format flips).
        assert_eq!(
            cold.get("ship_endpoint").and_then(|v| v.as_str()),
            Some("http://gorilla-merger:10908/ingest/gorilla"),
            "ship_endpoint must remain unchanged\n{yaml}"
        );
    }

    #[test]
    fn cold_format_intchunk_honours_explicit_coldpart_endpoint() {
        // An explicit `cold_coldpart_endpoint` wins over the ship-endpoint
        // derivation — lets a deploy point the cold-part tier at a
        // different merger host if needed.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let mut cfg = fused_asap_edge_cfg();
        cfg.cold_format = ColdFormat::Intchunk;
        cfg.cold_coldpart_endpoint = Some("http://other-merger:10908/ingest/coldpart".into());
        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");

        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let cold = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("cold"))
            .expect("cold block present");
        assert_eq!(
            cold.get("coldpart_endpoint").and_then(|v| v.as_str()),
            Some("http://other-merger:10908/ingest/coldpart"),
            "explicit coldpart_endpoint must win over the derivation\n{yaml}"
        );
    }

    #[test]
    fn fused_asap_edge_tier_derives_from_archive_routing() {
        // Focused regression for the per-metric `tier` contract (companion
        // to the ASAPCollector asapedgeprocessor `tier` field). The tier is
        // DERIVED from the plan routing already on `EdgeStageConfig`:
        //   * warm signal  — the metric has a warm entry (sketch family in
        //     `metric_to_family` or Sum-by aggregate).
        //   * cold signal  — the metric is in `archive_tier_metrics` (the
        //     plan's exact/archive routing decision).
        // tier = both (warm+cold), warm (warm only), defaulting to both
        // when no signal is present.
        //
        // `ASAP_EDGE_FUSED` is process-global; set it under the crate-wide
        // env lock (the #318 shared harness) so a parallel thread can't
        // observe this test's setenv as its own input.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        // Two metrics: a sketch-only one (warm) and one that is BOTH
        // sketched AND archived (both). The archive set is the precise
        // plan signal — only `archived_metric` is in it.
        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        metric_to_family.insert("sketch_only_metric".into(), [SketchAlgorithm::Kll].into());
        metric_to_family.insert("archived_metric".into(), [SketchAlgorithm::DDSketch].into());

        let cfg = EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: vec![ArchiveTierMetric {
                metric: "archived_metric".into(),
                window_secs: Some(60),
            }],
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: std::collections::HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        };

        let yaml = emit_edge_yaml(&cfg, "ws://c/", "agent-1").expect("emit ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("parse");
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|a| a.get("metrics"))
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        let tier_of = |name: &str| -> Option<String> {
            metrics
                .iter()
                .find(|e| e.get("metric").and_then(|m| m.as_str()) == Some(name))
                .and_then(|e| e.get("tier"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };

        assert_eq!(
            tier_of("sketch_only_metric").as_deref(),
            Some("warm"),
            "sketch-only (warm signal, no archive routing) must be tier=warm\n{yaml}"
        );
        assert_eq!(
            tier_of("archived_metric").as_deref(),
            Some("both"),
            "sketched + archived metric must be tier=both\n{yaml}"
        );
    }

    #[test]
    fn fused_asap_edge_honours_workload_family_override_for_latency() {
        // ── Family-mapping canonical decision: the WORKLOAD OVERRIDE wins ──
        //
        // The static reference (asap-otel-agent-asapedge.yaml) authored
        // `http_requests_total_latency_ms → ddsketch`, but the controller's
        // workload input (mvp-workload.yaml) pins
        // `sketch_family_override: KLL` for that metric (the KLL accuracy
        // experiment). The controller is the planner — `metric_to_family`
        // is populated FROM the workload, so when the override is KLL the
        // emit MUST produce a `kll` family entry for latency (and, being
        // KLL, must NOT carry delta_transmission). The static file is the
        // side that needs reconciling to KLL, not the emit.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");

        // Start from the canonical 6-family fixture and flip ONLY the
        // latency family to KLL (the workload override), as the planner
        // would have populated `metric_to_family` from mvp-workload.yaml.
        let mut cfg = fused_asap_edge_cfg();
        cfg.metric_to_family.insert(
            "http_requests_total_latency_ms".into(),
            one(SketchAlgorithm::Kll),
        );

        let yaml = emit_edge_yaml(&cfg, "ws://c/", "agent-1").expect("emit ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("parse");
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|a| a.get("metrics"))
            .and_then(|v| v.as_sequence())
            .expect("asap_edge.metrics seq");
        let latency = metrics
            .iter()
            .find(|e| {
                e.get("metric").and_then(|m| m.as_str()) == Some("http_requests_total_latency_ms")
            })
            .expect("latency entry present");
        assert_eq!(
            latency.get("family").and_then(|v| v.as_str()),
            Some("kll"),
            "workload override (KLL) is canonical — latency must emit family: kll\n{yaml}"
        );
        // KLL has no delta variant — the override entry must omit the key.
        assert!(
            latency.get("delta_transmission").is_none(),
            "KLL-overridden latency must NOT carry delta_transmission\n{yaml}"
        );
        // It is now sketch-only (no archive routing) ⇒ tier=warm.
        assert_eq!(
            latency.get("tier").and_then(|v| v.as_str()),
            Some("warm"),
            "latency (warm sketch only) must emit tier=warm\n{yaml}"
        );
    }

    #[test]
    fn fused_asap_edge_keys_are_a_subset_of_asapedgeprocessor_config_go() {
        // Cross-check every emitted key against the asapedgeprocessor
        // `Config` / `MetricFamily` / `ColdConfig` / `ControlChannelConfig`
        // mapstructure tags from
        // `opentelemetry-collector-contrib-patch/processor/asapedgeprocessor/config.go`.
        // Hardcoded here (per the task) so the test fails loudly if the emit
        // ever grows a key the processor can't load.
        //
        // NOTE: `emit_heap` / `heap_size` / `item_label` are the parallel
        // ASAPCollector change (warm-topk heap on the CountSketch family).
        // They are listed in the allowed MetricFamily set BELOW because the
        // emit intentionally ships them ahead of that processor change
        // landing (the cross-repo dependency flagged in the report). If a
        // reviewer wants to assert the gap, drop them from the set and the
        // test will pinpoint exactly which keys depend on the merge.
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let cfg = fused_asap_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://controller:4320/v1/opamp", "agent-1")
            .expect("emit fused asap_edge ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("parse");

        let asap_edge = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|v| v.as_mapping())
            .expect("asap_edge mapping");

        // Top-level Config mapstructure tags.
        let allowed_top: std::collections::BTreeSet<&str> = [
            "shard_count",
            "window_duration",
            "metrics",
            "cold",
            "control_channel",
            "max_series",
            "delta_transmission",
            "drop_original",
        ]
        .into_iter()
        .collect();
        for k in asap_edge.keys() {
            let k = k.as_str().expect("string key");
            assert!(
                allowed_top.contains(k),
                "asap_edge top-level key `{k}` not in asapedgeprocessor Config\n{yaml}"
            );
        }

        // MetricFamily mapstructure tags (incl. the parallel heap keys).
        let allowed_metric: std::collections::BTreeSet<&str> = [
            "metric",
            "family",
            "aggregate_by",
            "tier",
            "relative_accuracy",
            "k",
            "rows",
            "cols",
            "sample_p",
            "max_series",
            "delta_transmission",
            "delta_threshold",
            // Parallel ASAPCollector warm-topk change (see note above).
            "emit_heap",
            "heap_size",
            "item_label",
            // Edge aggregation scope + sparse-HLL (ASAPCollector#471/#472).
            // `mode` is on MetricFamily (config.go:91); `hll_sparse` is the
            // documented per-HLL sparse-base knob (warm_sketch.go reads
            // `fam.HLLSparse`). Both are emitted ahead of / in lock-step with
            // those edge changes — mapstructure ignores unknown keys.
            "mode",
            "hll_sparse",
        ]
        .into_iter()
        .collect();
        let metrics = asap_edge
            .get(serde_yaml::Value::String("metrics".into()))
            .and_then(|v| v.as_sequence())
            .expect("metrics seq");
        assert_eq!(metrics.len(), 6, "expected 6 metric families\n{yaml}");
        for entry in metrics {
            let m = entry.as_mapping().expect("metric entry mapping");
            for k in m.keys() {
                let k = k.as_str().expect("string key");
                assert!(
                    allowed_metric.contains(k),
                    "metrics[] key `{k}` not in asapedgeprocessor MetricFamily\n{yaml}"
                );
            }
        }

        // ColdConfig mapstructure tags.
        let allowed_cold: std::collections::BTreeSet<&str> = [
            "enabled",
            "ship_endpoint",
            "format",
            "coldpart_endpoint",
            "block_duration",
            "reorder_grace",
            "external_labels",
            "spool_dir",
            "spool_max_bytes",
            "ship_queue_depth",
            "spool_retry_interval",
            "endpoint",
            "tsdb_bucket",
            "tenant",
            "region",
            "access_key_id",
            "secret_access_key",
            "use_ssl",
        ]
        .into_iter()
        .collect();
        let cold = asap_edge
            .get(serde_yaml::Value::String("cold".into()))
            .and_then(|v| v.as_mapping())
            .expect("cold mapping");
        for k in cold.keys() {
            let k = k.as_str().expect("string key");
            assert!(
                allowed_cold.contains(k),
                "cold.* key `{k}` not in asapedgeprocessor ColdConfig\n{yaml}"
            );
        }
        // cold.ship_endpoint is the gorilla-merger HTTP ingest, control_channel
        // stays disabled (no controller poll route) — this emit is the live
        // OpAMP push path.
        assert_eq!(
            cold.get(serde_yaml::Value::String("ship_endpoint".into()))
                .and_then(|v| v.as_str()),
            Some("http://gorilla-merger:10908/ingest/gorilla"),
            "cold.ship_endpoint must be the gorilla-merger ingest\n{yaml}"
        );
        // No control_channel is emitted (the fused emit relies on the
        // processor's zero-value default, which is disabled — the live path
        // is THIS OpAMP push, not an HTTP poll). If a control_channel block
        // is ever emitted it must keep enabled: false.
        if let Some(cc) = asap_edge
            .get(serde_yaml::Value::String("control_channel".into()))
            .and_then(|v| v.as_mapping())
        {
            assert_eq!(
                cc.get(serde_yaml::Value::String("enabled".into()))
                    .and_then(|v| v.as_bool()),
                Some(false),
                "control_channel, if present, must stay disabled\n{yaml}"
            );
        }
    }

    #[test]
    fn fused_gate_off_keeps_routing_shape() {
        // Without the env gate the canonical routing-connector shape is
        // emitted (backward-compat for un-migrated agent builds). Unset
        // under the crate-wide env lock; guard restores on drop.
        let _env = crate::test_support::EnvVarGuard::unset("ASAP_EDGE_FUSED");
        let cfg = fused_asap_edge_cfg();
        let yaml = emit_edge_yaml(&cfg, "ws://c/", "agent-1").expect("emit ok");
        assert!(
            yaml.contains("connectors:") && !yaml.contains("asap_edge:"),
            "gate-off must keep the routing-connector shape\n{yaml}"
        );
    }

    // ── P1-3: CountSketch param round-trip (routing path width == backend w) ──

    /// Faithful Rust port of the standalone `countsketchprocessor`'s
    /// `configDimensions` (config_translate.go) so the test can assert the
    /// dimensions the agent would actually build from the emitted
    /// `epsilon` / `delta`.
    ///
    ///   cols = nextPowerOfTwo(ceil(1 / epsilon^2))   (clamped to >= 2)
    ///   rows = ceil(ln(1 / delta))                    (clamped to >= 1)
    ///
    /// (We don't replicate the `clampRowsForHashBits` budget clamp — the
    /// test's representative params stay inside the 64-bit row-hash budget,
    /// and the backend `w` we compare against is the WIDTH, which the row
    /// clamp never touches.)
    fn processor_config_dimensions(epsilon: f64, delta: f64) -> (u64, u64) {
        let mut rows = (1.0 / delta).ln().ceil() as i64;
        if rows < 1 {
            rows = 1;
        }
        let mut cols = (1.0 / (epsilon * epsilon)).ceil() as i64;
        if cols < 2 {
            cols = 2;
        }
        let mut p: i64 = 1;
        while p < cols {
            p <<= 1;
        }
        (p as u64, rows as u64)
    }

    /// The routing path's emitted CountSketch `epsilon`/`delta` must make
    /// the agent processor re-derive a width EXACTLY equal to the backend's
    /// `parameters["w"]` (and depth equal to `d`). Before P1-3 the routing
    /// path emitted `epsilon = e/w`, `delta = 2^-d`, which the processor
    /// expanded to a width of `nextPow2(ceil(w^2/e^2))` — a different,
    /// off-by-orders-of-magnitude width — so the content-addressed
    /// PolicyFingerprint never matched and the agent sketch failed to bind
    /// to its backend sid.
    #[test]
    fn countsketch_routing_path_width_matches_backend_w() {
        // A range of representative widths/depths the planner emits. Widths
        // are powers of two (the planner sizes them that way); the helper's
        // half-integer targeting keeps the round-trip exact regardless.
        for &(w, d) in &[(2048u32, 5u32), (1024, 4), (4096, 6), (2, 1), (256, 3)] {
            let sp = EdgeSketchProcessor {
                processor_name: "countsketch".into(),
                sketch_algorithm: SketchAlgorithm::CountSketch,
                sketch_params: SketchParams::CountSketch { width: w, depth: d },
                aggregation_id: "agg-cs".into(),
            };
            let block =
                build_edge_processor_block(&sp, Some(60), &[], Some("top_endpoint_qps"), None);
            let map = block.as_mapping().expect("processor block is a mapping");

            // The routing path no longer emits raw rows/cols — it emits the
            // epsilon/delta the standalone processor accepts.
            let epsilon = map
                .get(Value::String("epsilon".into()))
                .and_then(Value::as_f64)
                .expect("epsilon present");
            let delta = map
                .get(Value::String("delta".into()))
                .and_then(Value::as_f64)
                .expect("delta present");

            let (agent_cols, agent_rows) = processor_config_dimensions(epsilon, delta);

            // Backend side: `sketch_params_to_json` serialises CountSketch
            // params as `{ "w", "d", "with_heap" }`. The fingerprint keys off
            // `parameters["w"]`, which must equal the agent-derived width.
            let backend_json =
                sketch_params_to_json(&SketchParams::CountSketch { width: w, depth: d });
            let backend_w = backend_json["w"].as_u64().expect("backend w present");
            let backend_d = backend_json["d"].as_u64().expect("backend d present");

            assert_eq!(
                agent_cols, backend_w,
                "agent CountSketch width (cols={agent_cols}) must equal backend parameters[\"w\"]={backend_w} for (w={w}, d={d})"
            );
            assert_eq!(
                agent_rows, backend_d,
                "agent CountSketch depth (rows={agent_rows}) must equal backend parameters[\"d\"]={backend_d} for (w={w}, d={d})"
            );
        }
    }

    // ── P1-4: unenumerated CountSketch must NOT default to a top-k heap ──────

    /// Build a fused edge config that maps a CountSketch metric in
    /// `metric_to_family` but provides NO matching `EdgeSketchProcessor`,
    /// driving the fused emit into the catalog-default (`None`) arm.
    fn fused_cfg_countsketch_no_processor() -> EdgeStageConfig {
        let mut metric_to_family: HashMap<String, std::collections::BTreeSet<SketchAlgorithm>> =
            HashMap::new();
        // CountSketch family declared, but `sketch_processors` is EMPTY for
        // it — the `family_to_proc.get(kind)` lookup returns None.
        metric_to_family.insert(
            "endpoint_request_freq".into(),
            one(SketchAlgorithm::CountSketch),
        );

        EdgeStageConfig {
            source_metric: None,
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Endpoint("data-plane:4317".into()),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family,
            metric_to_grouping_labels: HashMap::new(),
            cumulative_counter_metrics: Vec::new(),
            cold_ship_endpoint: None,
            cold_external_labels: Vec::new(),
            metric_to_sample_p: HashMap::new(),
            metric_to_distinct_keys: HashMap::new(),
            metric_to_item_label: HashMap::new(),
            cold_format: crate::physical::colored_dag::emitter::ColdFormat::default(),
            cold_coldpart_endpoint: None,
        }
    }

    /// A CountSketch family mapped without an enumerated processor (a plain
    /// `FrequencyEstimate` plan) must NOT emit the top-k heap keys. Before
    /// P1-4 the catalog-default arm hardcoded `with_heap = true`, so the
    /// fused YAML carried `emit_heap: true` + a guessed `item_label`,
    /// registering a `FrequencyTopk` sid that a frequency/count query
    /// can't match.
    #[test]
    fn fused_unenumerated_countsketch_omits_heap() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let cfg = fused_cfg_countsketch_no_processor();
        let yaml = emit_edge_yaml_asap_edge(&cfg, "ws://c/", "agent-1").expect("fused emit ok");

        // The CountSketch entry must still be present (cols/rows defaults)...
        assert!(
            yaml.contains("countsketch") || yaml.contains("count_sketch"),
            "fused YAML should still carry the CountSketch family entry:\n{yaml}"
        );
        // ...but WITHOUT the heap keys that mark a FrequencyTopk plan.
        assert!(
            !yaml.contains("emit_heap"),
            "unenumerated CountSketch (no bound heap) must not emit emit_heap:\n{yaml}"
        );
    }

    /// Companion positive case: when the planner DID bind a CountSketch
    /// processor with `with_heap = true` (an actual top-k plan), the fused
    /// emit MUST carry `emit_heap: true`. This pins the heap-decision to the
    /// planner's flag rather than a hardcoded default.
    #[test]
    fn fused_enumerated_countsketch_with_heap_emits_heap() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let mut cfg = fused_cfg_countsketch_no_processor();
        cfg.metric_to_family.clear();
        cfg.metric_to_family
            .insert("top_endpoint_qps".into(), one(SketchAlgorithm::CountSketch));
        cfg.sketch_processors = vec![EdgeSketchProcessor {
            processor_name: "countsketch".into(),
            sketch_algorithm: SketchAlgorithm::CountSketchWithHeap,
            sketch_params: SketchParams::CountSketchWithHeap {
                width: 2048,
                depth: 5,
                heap_size: 10,
            },
            aggregation_id: "agg-cs".into(),
        }];
        let yaml = emit_edge_yaml_asap_edge(&cfg, "ws://c/", "agent-1").expect("fused emit ok");
        assert!(
            yaml.contains("emit_heap"),
            "an enumerated CountSketch with with_heap=true must emit emit_heap:\n{yaml}"
        );
    }

    /// Regression for the top-k cardinality explosion (#5): the item_label
    /// (heavy-hitter dimension, e.g. `host` from `topk(.., sum by (host)(m))`)
    /// must NOT appear in `aggregate_by` — it is the sketch/heap SUBJECT, not
    /// a series grouping key. Leaving it in keyed the edge series per-host
    /// (one series + heap per host) instead of one heap per group.
    #[test]
    fn fused_emit_excludes_item_label_from_aggregate_by() {
        let _env = crate::test_support::EnvVarGuard::set("ASAP_EDGE_FUSED", "1");
        let mut cfg = fused_cfg_countsketch_no_processor();
        // grouping_labels carries BOTH the real grouping key (zone) AND the
        // item_label (host) — as a `topk(10, sum by (host)(m))` workload with
        // grouping_labels:[zone] produces after the query's `by (host)` is
        // folded in.
        cfg.metric_to_grouping_labels.insert(
            "endpoint_request_freq".into(),
            vec!["host".to_string(), "zone".to_string()],
        );
        cfg.metric_to_item_label
            .insert("endpoint_request_freq".into(), "host".to_string());
        let yaml = emit_edge_yaml_asap_edge(&cfg, "ws://c/", "agent-1").expect("fused emit ok");
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("emitted YAML must parse: {e}\n{yaml}"));
        let metrics = doc
            .get("processors")
            .and_then(|p| p.get("asap_edge"))
            .and_then(|p| p.get("metrics"))
            .and_then(|m| m.as_sequence())
            .expect("metrics list present");
        let entry = metrics
            .iter()
            .find(|e| e.get("metric").and_then(|v| v.as_str()) == Some("endpoint_request_freq"))
            .expect("endpoint_request_freq entry present");
        let by: Vec<&str> = entry
            .get("aggregate_by")
            .and_then(|v| v.as_sequence())
            .map(|s| s.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        assert_eq!(
            by,
            vec!["zone"],
            "item_label `host` must be excluded from aggregate_by (got {by:?})\n{yaml}"
        );
    }
}
