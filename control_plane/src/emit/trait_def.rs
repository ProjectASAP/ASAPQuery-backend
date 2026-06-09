//! `PlanEmitter` trait — the L5 output surface declared in
//! `control_plane/docs/design.md` §5 / §6 `core::emit::PlanEmitter`.
//!
//! Per-deployment-model plan emitters implement [`PlanEmitter`] so a
//! controller pipeline can call them polymorphically: every emitter
//! takes its typed L5 stage config (`Input`) and returns the wire-format
//! payload (`Output`) the deployment model's transport expects.
//!
//! The three concrete emitters that ship in this crate today —
//! [`OpampEmitter`], [`StreamingConfigEmitter`], [`InferenceConfigEmitter`]
//! — wrap the existing free-function emitters in
//! [`super::stage_config`]. The trait is the seam that lets a future
//! `pipeline::run_pipeline` driver discover the right emitter via
//! [`crate::deployment_model::DeploymentModelRegistry`] without
//! taking a per-emitter dependency.

use anyhow::Result;

use crate::physical::colored_dag::emitter::{
    BackendStageConfig, EdgeStageConfig, GatewayStageConfig,
};

/// `PlanEmitter` — every per-deployment-model plan emitter implements
/// this so a future controller pipeline can call them polymorphically.
///
/// The `Input` associated type is the typed L5 stage config the emitter
/// consumes (e.g. `EdgeStageConfig` for OTel YAML emitters,
/// `BackendStageConfig` for the ASAPQuery-backend `StreamingConfig`
/// emitter). `Output` is the wire-format string / JSON the deployment
/// model's transport expects.
pub trait PlanEmitter: Send + Sync {
    /// Typed L5 stage config this emitter consumes.
    type Input;
    /// Wire-format output produced for the deployment model's transport.
    type Output;

    /// Stable name for diagnostics + the emitter registry.
    fn name(&self) -> &'static str;

    /// Emit the wire-format payload for the given stage config.
    fn emit(&self, input: &Self::Input) -> Result<Self::Output>;
}

// ── Concrete emitters ────────────────────────────────────────────────────────
//
// The three emitters below wrap [`super::stage_config`]'s free-function
// surface in `PlanEmitter`-typed structs. Each carries the bits of
// ambient state the free function needs as parameters (e.g. the OpAMP
// endpoint URL for the OTel-collector YAML emit). Construction is
// per-pipeline-cycle; the structs themselves are cheap to build.

/// OpAMP-pushed OTel-collector YAML emitter — the lifecycle deployment
/// model's edge-stage output. Wraps [`super::stage_config::emit_edge_yaml`]
/// (which itself dispatches to the 5-sketch routing-connector layout
/// when `cfg.metric_to_family` is populated).
///
/// The gateway-stage emit is exposed by [`OpampGatewayEmitter`] —
/// edge and gateway have different `Input` types (`EdgeStageConfig`
/// vs `GatewayStageConfig`) so they're separate emitter structs.
pub struct OpampEmitter {
    /// OpAMP WebSocket endpoint URL the emitted YAML's
    /// `extensions.opamp.server.ws.endpoint` block must point at.
    pub opamp_endpoint: String,
    /// Identity the agent presents in the `X-Agent-ID` WS header on
    /// reconnect (Issue #2). Broadcast callers without a single agent
    /// in scope use the literal `"$AGENT_ID"` placeholder and rely on
    /// the agent container's env to expand it at boot.
    pub agent_id: String,
}

impl PlanEmitter for OpampEmitter {
    type Input = EdgeStageConfig;
    type Output = String;

    fn name(&self) -> &'static str {
        "opamp_edge_yaml"
    }

    fn emit(&self, input: &EdgeStageConfig) -> Result<String> {
        super::stage_config::emit_edge_yaml(input, &self.opamp_endpoint, &self.agent_id)
    }
}

/// OpAMP-pushed OTel-collector YAML emitter for the gateway stage —
/// wraps [`super::stage_config::emit_gateway_yaml`].
pub struct OpampGatewayEmitter {
    /// OpAMP WebSocket endpoint URL the emitted YAML's
    /// `extensions.opamp.server.ws.endpoint` block must point at.
    pub opamp_endpoint: String,
    /// Identity the gateway agent presents in the `X-Agent-ID` WS
    /// header on reconnect (Issue #2). Broadcast callers use the
    /// literal `"$AGENT_ID"` placeholder.
    pub agent_id: String,
}

impl PlanEmitter for OpampGatewayEmitter {
    type Input = GatewayStageConfig;
    type Output = String;

    fn name(&self) -> &'static str {
        "opamp_gateway_yaml"
    }

    fn emit(&self, input: &GatewayStageConfig) -> Result<String> {
        super::stage_config::emit_gateway_yaml(input, &self.opamp_endpoint, &self.agent_id)
    }
}

/// Backend `StreamingConfig` JSON emitter — wraps
/// [`super::stage_config::emit_backend_streaming_config_json`]. The output is the
/// JSON document the ASAPQuery-backend's
/// `POST /api/v1/streaming-config` endpoint accepts (aggregations +
/// readouts array).
#[derive(Default)]
pub struct StreamingConfigEmitter;

impl PlanEmitter for StreamingConfigEmitter {
    type Input = BackendStageConfig;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "streaming_config_json"
    }

    fn emit(&self, input: &BackendStageConfig) -> Result<serde_json::Value> {
        super::stage_config::emit_backend_streaming_config_json(input)
    }
}

/// Per-tenant input bundle for [`InferenceConfigEmitter`]. The
/// backend's `POST /api/v1/storage_routing` endpoint requires a
/// per-tenant document covering EVERY planned metric atomically;
/// `metric_plans` is the cumulative `(metric, &BackendStageConfig)`
/// list the controller has produced this cycle and `mode3_metrics`
/// is the list of Mode-3 (Prometheus archive) metrics. The default
/// tenant is `"default"`.
pub struct InferenceConfigInput<'a> {
    /// Tenant id — `"default"` for single-tenant deployments.
    pub tenant: String,
    /// Per-metric backend stage configs the planner produced this
    /// planning cycle.
    pub metric_plans: Vec<(String, &'a BackendStageConfig)>,
    /// Mode-3 metric names — these get a single-target row with
    /// `engine: prometheus_remote`.
    pub mode3_metrics: Vec<String>,
}

/// Backend storage-routing JSON emitter — wraps
/// [`super::stage_config::emit_backend_storage_routing_with_prometheus_for_tenant`].
/// The output is the JSON document the ASAPQuery-backend's
/// `POST /api/v1/storage_routing` endpoint accepts (per-tenant
/// metric → engine routing table).
///
/// The name "inference config" comes from design.md §5: this is the
/// query-routing table that drives backend query inference (i.e.
/// shape → engine dispatch for `histogram_quantile`, `topk`,
/// `quantile_over_time`, etc.).
#[derive(Default)]
pub struct InferenceConfigEmitter;

impl PlanEmitter for InferenceConfigEmitter {
    type Input = InferenceConfigInput<'static>;
    type Output = serde_json::Value;

    fn name(&self) -> &'static str {
        "inference_config_json"
    }

    /// Note: the `Input` lifetime is `'static` only on the trait surface
    /// — callers construct the `InferenceConfigInput` with borrowed
    /// `&BackendStageConfig` references that outlive the call. For
    /// borrowed inputs, use [`Self::emit_borrowed`] below instead of
    /// the trait method (the trait's associated type carries no
    /// lifetime parameter — design.md §6's `PlanEmitter::emit` shape
    /// is the constraint).
    fn emit(&self, input: &InferenceConfigInput<'static>) -> Result<serde_json::Value> {
        self.emit_borrowed(input)
    }
}

impl InferenceConfigEmitter {
    /// Borrowing-friendly variant of [`PlanEmitter::emit`] — accepts an
    /// `InferenceConfigInput` of any lifetime. Used by `replan` /
    /// `main` which build the input from per-call cache scans.
    pub fn emit_borrowed<'a>(&self, input: &InferenceConfigInput<'a>) -> Result<serde_json::Value> {
        super::stage_config::emit_backend_storage_routing_with_prometheus_for_tenant(
            &input.tenant,
            &input.metric_plans,
            &input.mode3_metrics,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::colored_dag::emitter::{
        BackendAggregation, BackendReadout, ExportTarget, GatewayMergeProcessor,
    };
    use crate::physical::colored_dag::stage_id::StageId;
    use crate::sketch_algebra::params::{DDSketchParams, SketchKind, SketchParams};
    use crate::sketch_algebra::physical_expr::EstimateOp;
    use std::collections::HashMap;

    fn empty_edge_cfg() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("m".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
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

    fn empty_gateway_cfg() -> GatewayStageConfig {
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

    fn empty_backend_cfg() -> BackendStageConfig {
        BackendStageConfig {
            aggregations: vec![BackendAggregation {
                item_label: None,
                aggregation_id: "agg0".to_string(),
                metric_name: "test_metric".to_string(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                window_secs: 60,
                spatial_filter: String::new(),
                grouping: Vec::new(),
                aggregation_input:
                    crate::physical::colored_dag::emitter::AggregationInput::SketchEnvelope,
                agg_type_override: None,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".to_string(),
                op: EstimateOp::Quantile { q: 0.99 },
            }],
        }
    }

    #[test]
    fn opamp_emitter_round_trips_through_free_function() {
        // `emit_edge_yaml` reads process-global env (`ASAP_EDGE_FUSED`,
        // `ASAP_AGENT_MEMORY_LIMIT_MIB`); hold the crate-wide env lock so
        // a concurrent env-mutating test can't flip the gate between the
        // two emit calls this test compares for equality.
        let _env = crate::test_support::env_lock();
        let cfg = empty_edge_cfg();
        let emitter = OpampEmitter {
            opamp_endpoint: "ws://controller/v1/opamp".to_string(),
            agent_id: "test-agent".to_string(),
        };
        let trait_out = emitter.emit(&cfg).expect("trait emit");
        let direct = super::super::stage_config::emit_edge_yaml(
            &cfg,
            "ws://controller/v1/opamp",
            "test-agent",
        )
        .expect("direct emit");
        assert_eq!(trait_out, direct);
        assert_eq!(emitter.name(), "opamp_edge_yaml");
    }

    #[test]
    fn opamp_gateway_emitter_round_trips_through_free_function() {
        let cfg = empty_gateway_cfg();
        let emitter = OpampGatewayEmitter {
            opamp_endpoint: "ws://controller/v1/opamp".to_string(),
            agent_id: "test-agent".to_string(),
        };
        let trait_out = emitter.emit(&cfg).expect("trait emit");
        let direct = super::super::stage_config::emit_gateway_yaml(
            &cfg,
            "ws://controller/v1/opamp",
            "test-agent",
        )
        .expect("direct emit");
        assert_eq!(trait_out, direct);
        assert_eq!(emitter.name(), "opamp_gateway_yaml");
    }

    #[test]
    fn streaming_config_emitter_round_trips_through_free_function() {
        let cfg = empty_backend_cfg();
        let emitter = StreamingConfigEmitter::default();
        let trait_out = emitter.emit(&cfg).expect("trait emit");
        let direct = super::super::stage_config::emit_backend_streaming_config_json(&cfg)
            .expect("direct emit");
        assert_eq!(trait_out, direct);
        assert_eq!(emitter.name(), "streaming_config_json");
    }

    #[test]
    fn inference_config_emitter_borrowed_variant_round_trips() {
        let backend_cfg = empty_backend_cfg();
        let metric_plans = vec![("http_latency_ms".to_string(), &backend_cfg)];
        let mode3 = vec!["raw_metric_X".to_string()];
        let input = InferenceConfigInput {
            tenant: "default".to_string(),
            metric_plans: metric_plans.clone(),
            mode3_metrics: mode3.clone(),
        };
        let emitter = InferenceConfigEmitter::default();
        let trait_out = emitter.emit_borrowed(&input).expect("trait emit");
        let direct =
            super::super::stage_config::emit_backend_storage_routing_with_prometheus_for_tenant(
                "default",
                &metric_plans,
                &mode3,
            )
            .expect("direct emit");
        assert_eq!(trait_out, direct);
        assert_eq!(emitter.name(), "inference_config_json");
    }

    /// `PlanEmitter` is object-safe — every emitter type can be put
    /// behind `Box<dyn PlanEmitter<Input=…, Output=…>>` so the future
    /// pipeline driver can hold a homogenous registry slot.
    #[test]
    fn plan_emitter_is_object_safe() {
        let _edge: Box<dyn PlanEmitter<Input = EdgeStageConfig, Output = String>> =
            Box::new(OpampEmitter {
                opamp_endpoint: "ws://x".into(),
                agent_id: "test-agent".into(),
            });
        let _gateway: Box<dyn PlanEmitter<Input = GatewayStageConfig, Output = String>> =
            Box::new(OpampGatewayEmitter {
                opamp_endpoint: "ws://x".into(),
                agent_id: "test-agent".into(),
            });
        let _streaming: Box<
            dyn PlanEmitter<Input = BackendStageConfig, Output = serde_json::Value>,
        > = Box::new(StreamingConfigEmitter);
    }
}
