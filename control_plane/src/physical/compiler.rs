//! Backend-owned physical compilation over ASAPPlanner's selected post-ASAP IR.
//!
//! Planner owns semantic alternatives and guarantees. This module owns the
//! deployment decision: evidence freshness, target capabilities, windows, the
//! Collector execution projection, and the matching BackendPlan.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use asap_aware_mapping::cost_model::Cost;
use asap_aware_mapping::{
    plan_summary_maintenance_lifecycles, AccuracyEvidenceProvider, CostRate, DefaultAccuracyModel,
    EqualSplitAllocator, Horizon, PropagationStats, SummaryMaintenanceCapabilities,
    SummaryMaintenanceLifecycleCapabilities, SummaryMaintenanceLifecycleCostInputs, WorkloadDemand,
};
use planner_types::post_asap::{
    CompositionOperator, EvaluationSchedule, OutputRepresentation, SketchQuery, SummaryExpr,
    SummaryFamilyType, SummaryMaintenanceLifecycle, SummaryMaintenanceLifecycleGuarantee,
    SummaryMaintenanceMode, SummaryNode, SummaryWindowFramework,
};
use planner_types::pre_asap::QueryExpr;
use planner_types::workload::{
    AccuracyRequirement, DataArrival, DataWorkload, DurationMs, Evidence, EvidenceSource,
    Predictability, Query, QueryLanguage, QueryRequirements, QueryTimeScope, QueryWorkload, Rate,
    RepeatedDemand, RepeatingEntry, RepetitionInterval, TimeSelection,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

use crate::backend_plan::{self, BackendPlan};
use crate::emit::monitor::MonitorIntent;
use crate::physical::colored_dag::emitter::{
    AggregationInput, BackendAggregation, BackendReadout, BackendStageConfig,
};
use crate::physical::post_asap::cost_model::ControlPlaneCostModel;
use crate::query_plan::{
    canonical_promql, FallbackPolicy, InstantExecution, MaterializationBinding, PhysicalGrouping,
    QueryPlan, QueryPlanEntry,
};
use crate::types_v2::AccuracyTarget;
use planner_types::pre_asap::Source;

pub const PLANNER_REVISION: &str = "739753e33e096c01faccca8e7a1e3da5ad3aab9c";

#[derive(Debug, Clone)]
pub struct PlanningQuery {
    pub query_id: String,
    /// Catalog expression used only to build the stable QueryPlan identity.
    /// The selected implementation comes from `post_asap`, never this text.
    pub query_string: String,
    /// Planner-selected post-ASAP DAG. The physical compiler must not
    /// re-select a summary family from pre-ASAP input.
    pub post_asap: Rc<SummaryNode>,
    pub source: Source,
    pub window_secs: u64,
    /// Label names are deployment metadata because Planner's canonical IR
    /// currently carries positional column IDs at this boundary.
    pub group_by: Vec<String>,
    pub accuracy: AccuracyTarget,
    pub lifecycle: LifecyclePlanningInput,
    /// Executor-feasible concrete realizations offered to Planner for its
    /// abstract window-framework decision. The compiler retains physical
    /// identities and exposes only framework + complete weighted cost to
    /// Planner. An empty or stale set fails closed.
    pub window_implementations: Vec<WindowImplementationCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImplementationCostEvidence {
    pub model_version: String,
    pub workload_fingerprint: String,
    pub observed_at_unix_ms: u64,
    pub valid_for_ms: u64,
    pub horizon_seconds: f64,
    pub cpu_cost: f64,
    pub peak_memory_bytes: u64,
    pub network_bytes: u64,
    pub storage_bytes: u64,
    pub source_scan_bytes: u64,
    /// Dimensionally calibrated scalar passed to Planner for comparison.
    pub weighted_cost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WindowImplementationCandidate {
    /// Backend-owned identity; never copied into Planner IR.
    pub implementation_id: String,
    pub framework: SummaryWindowFramework,
    pub window_secs: u64,
    pub pane_secs: u64,
    pub state_layout: String,
    pub cost: ImplementationCostEvidence,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LifecycleCostEvidence {
    pub build: f64,
    pub maintenance_per_update: f64,
    pub read: f64,
    pub retention_per_second: f64,
    pub retirement: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LifecyclePlanningInput {
    pub evaluation_interval_ms: u32,
    pub ingestion_rate_per_second: f64,
    pub evidence_observed_at_unix_ms: u64,
    pub evidence_valid_for_ms: u64,
    pub horizon_seconds: f64,
    pub costs: LifecycleCostEvidence,
}

#[derive(Debug, Clone, Default)]
pub struct PlanningRequest {
    pub queries: Vec<PlanningQuery>,
    pub evidence: HashMap<String, TopKMembershipEvidence>,
    pub planner_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopKMembershipEvidence {
    pub selected_lower_bound: f64,
    pub excluded_upper_bound: f64,
    pub interval_failure_probability: f64,
    pub observed_at_unix_ms: u64,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct DeploymentEnvironment {
    pub collector_ids: Vec<String>,
    pub capability_snapshot_id: String,
    pub observed_at_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub plan_version: u64,
    pub activation_unix_ms: u64,
    pub expiry_unix_ms: Option<u64>,
    pub backend_compat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PlanEnvelope {
    pub plan_id: u64,
    pub plan_version: u64,
    pub generated_at_unix_ms: u64,
    pub activation_unix_ms: u64,
    pub expiry_unix_ms: Option<u64>,
    pub backend_compat: String,
    pub planner_revision: String,
    pub capability_snapshot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorMaterialization {
    pub query_id: String,
    pub materialization: asap_types::PolicyFingerprint,
    pub metric: String,
    pub algorithm: String,
    pub parameters: Value,
    pub group_by: Vec<String>,
    pub window_secs: u64,
    pub abstract_window_framework: SummaryWindowFramework,
    pub window_implementation_id: String,
    pub pane_secs: u64,
    pub state_layout: String,
    pub evidence_source: Option<String>,
    pub lifecycle: CollectorLifecycle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollectorLifecycle {
    pub kind: String,
    pub maintenance_mode: String,
    pub evaluation_schedule: String,
    pub output_representation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectorPlan {
    pub collector_id: String,
    pub envelope: PlanEnvelope,
    pub materializations: Vec<CollectorMaterialization>,
}

/// Backend-side materialization projection consumed by the streaming
/// precompute engine. This is deliberately config-driven: it contains no
/// PromQL string or ad-hoc scheduler job. The aggregation definitions are
/// emitted to `/api/v1/streaming-config`, where the runtime matches incoming
/// series, maintains windows, and writes content-addressed materializations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputePlan {
    pub envelope: PlanEnvelope,
    pub ingest: IngestContract,
    pub schemas: Vec<StateSchemaContract>,
    pub producers: Vec<ProducerContract>,
    pub materializations: Vec<asap_types::PrecomputeMaterialization>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestProtocol {
    ModifiedOtlpMetricsV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    UnixNanoseconds,
}

/// Backend ingress semantics installed with the precompute projection. This
/// replaces implicit knowledge formerly hidden in the streaming-config path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IngestContract {
    pub protocol: IngestProtocol,
    pub endpoint_path: String,
    pub timestamp_unit: TimestampUnit,
    pub require_plan_identity: bool,
    pub require_materialization_identity: bool,
    pub require_registered_producer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateEncoding {
    SketchlibProtobufV1,
    SketchCoreMsgpackV1,
    ExactAccumulatorV1,
}

/// Decoder/schema contract for one content-addressed materialization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StateSchemaContract {
    pub schema_id: String,
    pub schema_version: u32,
    pub materialization: asap_types::PolicyFingerprint,
    pub family: SummaryFamilyType,
    pub source: Source,
    pub value_column: planner_types::pre_asap::ColumnRef,
    pub group_by: Vec<String>,
    pub window: StateWindowContract,
    pub encodings: Vec<StateEncoding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StateWindowContract {
    pub kind: asap_types::WindowKind,
    pub size_ms: u64,
    pub slide_ms: Option<u64>,
}

/// A collector authorized to produce state for one materialization. Runtime
/// producer epochs and frame sequences belong to TransmissionPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ProducerContract {
    pub producer_id: String,
    pub collector_id: String,
    pub materialization: asap_types::PolicyFingerprint,
    pub schema_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PrecomputePlanError {
    #[error("PrecomputePlan envelope does not match BackendPlan identity/lifecycle")]
    PlanIdentityMismatch,
    #[error("precompute ingest endpoint must be /v1/metrics")]
    UnsupportedIngestEndpoint,
    #[error("duplicate materialization {0}")]
    DuplicateMaterialization(u64),
    #[error("schema set does not exactly match the materialization set")]
    SchemaSetMismatch,
    #[error("schema {schema_id} has invalid version or no encoding")]
    InvalidSchema { schema_id: String },
    #[error("producer {producer_id} references an unknown materialization or schema")]
    InvalidProducer { producer_id: String },
    #[error("materialization {0} has no registered producer")]
    MissingProducer(u64),
    #[error("duplicate producer binding {0}")]
    DuplicateProducer(String),
}

impl PrecomputePlan {
    pub fn build(
        envelope: PlanEnvelope,
        materializations: Vec<asap_types::PrecomputeMaterialization>,
        backend_plan: &BackendPlan,
        producer_ids: &[String],
    ) -> Result<Self, PrecomputePlanError> {
        if envelope.plan_id != backend_plan.plan_id
            || envelope.plan_version != backend_plan.plan_version
            || envelope.generated_at_unix_ms != backend_plan.generated_at_unix_ms
            || envelope.activation_unix_ms != backend_plan.activation_unix_ms
            || envelope.expiry_unix_ms != backend_plan.expiry_unix_ms
            || envelope.backend_compat != backend_plan.backend_compat
        {
            return Err(PrecomputePlanError::PlanIdentityMismatch);
        }
        let schemas = backend_plan
            .materializations
            .iter()
            .map(|(fingerprint, materialization)| StateSchemaContract {
                schema_id: state_schema_id(*fingerprint),
                schema_version: 1,
                materialization: *fingerprint,
                family: materialization.family.clone(),
                source: materialization.source.clone(),
                value_column: materialization.col.clone(),
                group_by: materialization.group_by.clone(),
                window: StateWindowContract {
                    kind: materialization.window.kind,
                    size_ms: materialization.window.size_ms,
                    slide_ms: materialization.window.slide_ms,
                },
                encodings: state_encodings(&materialization.family),
            })
            .collect::<Vec<_>>();
        let producers = producer_ids
            .iter()
            .flat_map(|producer_id| {
                schemas.iter().map(move |schema| ProducerContract {
                    producer_id: producer_id.clone(),
                    collector_id: producer_id.clone(),
                    materialization: schema.materialization,
                    schema_id: schema.schema_id.clone(),
                })
            })
            .collect();
        let plan = Self {
            envelope,
            ingest: IngestContract {
                protocol: IngestProtocol::ModifiedOtlpMetricsV1,
                endpoint_path: "/v1/metrics".into(),
                timestamp_unit: TimestampUnit::UnixNanoseconds,
                require_plan_identity: true,
                require_materialization_identity: true,
                require_registered_producer: true,
            },
            schemas,
            producers,
            materializations,
        };
        plan.validate_against_backend(backend_plan)?;
        Ok(plan)
    }

    pub fn runtime_materializations(
        &self,
    ) -> Result<HashMap<u64, asap_types::PrecomputeMaterialization>, PrecomputePlanError> {
        self.validate()?;
        Ok(self
            .materializations
            .iter()
            .cloned()
            .map(|materialization| (materialization.policy_fp_u64(), materialization))
            .collect())
    }

    pub fn validate(&self) -> Result<(), PrecomputePlanError> {
        if self.ingest.endpoint_path != "/v1/metrics"
            || !self.ingest.require_plan_identity
            || !self.ingest.require_materialization_identity
            || !self.ingest.require_registered_producer
        {
            return Err(PrecomputePlanError::UnsupportedIngestEndpoint);
        }
        let mut materializations = BTreeSet::new();
        for materialization in &self.materializations {
            if !materializations.insert(materialization.policy_fingerprint()) {
                return Err(PrecomputePlanError::DuplicateMaterialization(
                    materialization.policy_fp_u64(),
                ));
            }
        }
        let schemas: BTreeSet<_> = self
            .schemas
            .iter()
            .map(|schema| schema.materialization)
            .collect();
        if schemas != materializations || schemas.len() != self.schemas.len() {
            return Err(PrecomputePlanError::SchemaSetMismatch);
        }
        for schema in &self.schemas {
            if schema.schema_version == 0 || schema.encodings.is_empty() {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
            }
        }
        let schema_by_materialization: BTreeMap<_, _> = self
            .schemas
            .iter()
            .map(|schema| (schema.materialization, schema.schema_id.as_str()))
            .collect();
        let mut producers = BTreeSet::new();
        let mut produced = BTreeSet::new();
        for producer in &self.producers {
            if !materializations.contains(&producer.materialization)
                || schema_by_materialization
                    .get(&producer.materialization)
                    .copied()
                    != Some(producer.schema_id.as_str())
            {
                return Err(PrecomputePlanError::InvalidProducer {
                    producer_id: producer.producer_id.clone(),
                });
            }
            let key = (
                producer.producer_id.as_str(),
                producer.materialization,
                producer.schema_id.as_str(),
            );
            if !producers.insert(key) {
                return Err(PrecomputePlanError::DuplicateProducer(
                    producer.producer_id.clone(),
                ));
            }
            produced.insert(producer.materialization);
        }
        if let Some(missing) = materializations.difference(&produced).next() {
            return Err(PrecomputePlanError::MissingProducer(missing.0));
        }
        Ok(())
    }

    pub fn validate_against_backend(
        &self,
        backend_plan: &BackendPlan,
    ) -> Result<(), PrecomputePlanError> {
        self.validate()?;
        for schema in &self.schemas {
            let Some(materialization) = backend_plan.materializations.get(&schema.materialization)
            else {
                return Err(PrecomputePlanError::SchemaSetMismatch);
            };
            if schema.family != materialization.family
                || schema.schema_id != state_schema_id(schema.materialization)
                || schema.source != materialization.source
                || schema.value_column != materialization.col
                || schema.group_by != materialization.group_by
                || schema.window.kind != materialization.window.kind
                || schema.window.size_ms != materialization.window.size_ms
                || schema.window.slide_ms != materialization.window.slide_ms
            {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Complete physical projection of one post-ASAP planning decision.
/// All three child plans share the same envelope and are compiled together.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub envelope: PlanEnvelope,
    pub collector_plans: Vec<CollectorPlan>,
    pub precompute_plan: PrecomputePlan,
    pub backend_plan: BackendPlan,
    pub query_plan: QueryPlan,
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("planner revision mismatch: request={request}, compiler={compiler}")]
    PlannerRevision {
        request: String,
        compiler: &'static str,
    },
    #[error("query {query_id}: {reason}")]
    Query { query_id: String, reason: String },
    #[error("query {query_id}: TopK evidence is stale or invalid: {reason}")]
    InvalidEvidence { query_id: String, reason: String },
    #[error("query {query_id}: lifecycle planning failed: {reason}")]
    Lifecycle { query_id: String, reason: String },
    #[error("failed to construct BackendPlan: {0}")]
    BackendPlan(#[from] anyhow::Error),
    #[error("failed to construct QueryPlan: {0}")]
    QueryPlan(#[from] crate::query_plan::QueryPlanError),
}

struct QueryEvidence<'a>(Option<&'a TopKMembershipEvidence>);

impl AccuracyEvidenceProvider for QueryEvidence<'_> {
    fn propagation_stats(
        &self,
        op: &CompositionOperator,
        _family: &SummaryFamilyType,
        _query: Option<&SketchQuery>,
    ) -> PropagationStats {
        match (op, self.0) {
            (CompositionOperator::TopKSelection, Some(e)) => PropagationStats {
                topk_selected_lower_bound: Some(e.selected_lower_bound),
                topk_excluded_upper_bound: Some(e.excluded_upper_bound),
                topk_interval_failure_probability: Some(e.interval_failure_probability),
                ..Default::default()
            },
            _ => PropagationStats::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct PhysicalCompiler;

impl PhysicalCompiler {
    pub fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<PhysicalPlan, CompileError> {
        if request.planner_revision != PLANNER_REVISION {
            return Err(CompileError::PlannerRevision {
                request: request.planner_revision,
                compiler: PLANNER_REVISION,
            });
        }

        let mut aggregations = Vec::with_capacity(request.queries.len());
        let mut readouts = Vec::with_capacity(request.queries.len());
        let mut collector_materializations = Vec::with_capacity(request.queries.len());

        for query in &request.queries {
            let evidence = request.evidence.get(&query.query_id);
            if let Some(e) = evidence {
                validate_evidence(&query.query_id, e, &environment)?;
            }
            validate_lifecycle_input(&query.query_id, &query.lifecycle)?;
            let lifecycle_costs = SummaryMaintenanceLifecycleCostInputs {
                build_cost: Some(Cost(query.lifecycle.costs.build)),
                maintenance_cost_per_update: Some(Cost(
                    query.lifecycle.costs.maintenance_per_update,
                )),
                summary_read_cost: Some(Cost(query.lifecycle.costs.read)),
                retention_cost_rate: Some(CostRate(query.lifecycle.costs.retention_per_second)),
                retirement_cost: Some(Cost(query.lifecycle.costs.retirement)),
            };
            let window_costs = validate_window_implementations(query, &environment)?;
            let model = ControlPlaneCostModel::new(query.accuracy.clone())
                .with_summary_maintenance(
                    lifecycle_costs,
                    SummaryMaintenanceCapabilities {
                        incremental_update: true,
                        merge: true,
                        delete: false,
                    },
                )
                .with_window_framework_costs(window_costs);
            let node = query.post_asap.clone();
            let selected = extract_selected(&node).ok_or_else(|| CompileError::Query {
                query_id: query.query_id.clone(),
                reason: "selected plan has no executable sketch materialization/readout".into(),
            })?;
            let planner_selection = select_lifecycle(query, &node, &model, &environment)?;
            let window_implementation = query
                .window_implementations
                .iter()
                .filter(|candidate| candidate.framework == planner_selection.window_framework)
                .min_by(|left, right| left.cost.weighted_cost.total_cmp(&right.cost.weighted_cost))
                .ok_or_else(|| CompileError::Lifecycle {
                    query_id: query.query_id.clone(),
                    reason: "Planner selected a window framework without a retained concrete implementation".into(),
                })?;
            let metric = match &query.source {
                Source::TimeSeries { metric } => metric.clone(),
                Source::Table { .. } => {
                    return Err(CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: "MVP physical compiler supports time-series sources only".into(),
                    })
                }
            };
            let aggregation_id = format!("{}:{}", query.query_id, metric);
            let aggregation = BackendAggregation {
                aggregation_id: aggregation_id.clone(),
                metric_name: metric.clone(),
                family: SummaryFamilyType::Sketch(
                    selected.kind.clone(),
                    planner_types::post_asap::GroupingStrategy::PerSubpopulationInstance,
                ),
                window_secs: query.window_secs,
                spatial_filter: String::new(),
                grouping: query.group_by.clone(),
                item_label: None,
                aggregation_input: AggregationInput::SketchEnvelope,
            };
            let precompute_materialization =
                backend_plan::aggregation_config_for_materialization(&aggregation)?;
            let materialization = precompute_materialization.policy_fingerprint();
            aggregations.push(aggregation);
            readouts.push(BackendReadout {
                aggregation_id,
                op: selected.readout.clone(),
            });
            collector_materializations.push(CollectorMaterialization {
                query_id: query.query_id.clone(),
                materialization,
                metric,
                algorithm: format!("{:?}", selected.kind.algorithm()).to_ascii_lowercase(),
                parameters: sketch_params_json(&selected.params),
                group_by: query.group_by.clone(),
                window_secs: query.window_secs,
                abstract_window_framework: planner_selection.window_framework,
                window_implementation_id: window_implementation.implementation_id.clone(),
                pane_secs: window_implementation.pane_secs,
                state_layout: window_implementation.state_layout.clone(),
                evidence_source: evidence.map(|e| e.source.clone()),
                lifecycle: planner_selection.lifecycle,
            });
        }

        let plan_id = stable_plan_id(&collector_materializations);
        let envelope = PlanEnvelope {
            plan_id,
            plan_version: environment.plan_version,
            generated_at_unix_ms: environment.observed_at_unix_ms,
            activation_unix_ms: environment.activation_unix_ms,
            expiry_unix_ms: environment.expiry_unix_ms,
            backend_compat: environment.backend_compat.clone(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: environment.capability_snapshot_id,
        };
        let backend_stage_config = BackendStageConfig {
            aggregations: aggregations.clone(),
            readouts,
        };
        let mut backend_plan = backend_plan::from_stage_config(
            &backend_stage_config,
            &Vec::<MonitorIntent>::new(),
            plan_id,
            environment.observed_at_unix_ms,
        )?;
        backend_plan.plan_version = envelope.plan_version;
        backend_plan.activation_unix_ms = envelope.activation_unix_ms;
        backend_plan.expiry_unix_ms = envelope.expiry_unix_ms;
        backend_plan.backend_compat = envelope.backend_compat.clone();
        backend_plan
            .validate()
            .map_err(|error| CompileError::Query {
                query_id: "physical-plan-envelope".into(),
                reason: error.to_string(),
            })?;
        for materialization in backend_plan.materializations.values_mut() {
            materialization.lifecycle = Some(SummaryMaintenanceLifecycleGuarantee {
                summary_maintenance_lifecycle: SummaryMaintenanceLifecycle::ContinuouslyMaintained,
                summary_maintenance_mode: SummaryMaintenanceMode::Incremental,
                evaluation_schedule: EvaluationSchedule::PerUpdate,
                output_representation: OutputRepresentation::SummaryState,
            });
        }
        let collector_plans = environment
            .collector_ids
            .into_iter()
            .map(|collector_id| CollectorPlan {
                collector_id,
                envelope: envelope.clone(),
                materializations: collector_materializations.clone(),
            })
            .collect::<Vec<_>>();
        // Several queries/readouts may intentionally share one maintained
        // summary. PrecomputePlan is keyed by physical identity, not query ID.
        let mut materializations_by_fingerprint = BTreeMap::new();
        for aggregation in &aggregations {
            let materialization =
                backend_plan::aggregation_config_for_materialization(aggregation)?;
            materializations_by_fingerprint
                .entry(materialization.policy_fingerprint())
                .or_insert(materialization);
        }
        let materializations = materializations_by_fingerprint.into_values().collect();
        let producer_ids = collector_plans
            .iter()
            .map(|plan| plan.collector_id.clone())
            .collect::<Vec<_>>();
        let precompute_plan = PrecomputePlan::build(
            envelope.clone(),
            materializations,
            &backend_plan,
            &producer_ids,
        )
        .map_err(|error| CompileError::Query {
            query_id: "precompute-plan".into(),
            reason: error.to_string(),
        })?;
        let materialization_fingerprints: BTreeSet<_> =
            backend_plan.materializations.keys().copied().collect();
        let mut query_entries = BTreeMap::new();
        for query in &request.queries {
            let canonical = canonical_promql(&query.query_string)?;
            let selected =
                extract_selected(&query.post_asap).ok_or_else(|| CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: "selected plan has no executable sketch materialization/readout".into(),
                })?;
            let family = SummaryFamilyType::Sketch(
                selected.kind,
                planner_types::post_asap::GroupingStrategy::PerSubpopulationInstance,
            );
            let metric = match &query.source {
                Source::TimeSeries { metric } => metric,
                Source::Table { .. } => unreachable!("table source rejected above"),
            };
            let bound: BTreeSet<_> = backend_plan
                .routing
                .iter()
                .filter_map(|route| {
                    let materialization = backend_plan.materializations.get(&route.materialization)?;
                    (route.storage_backend == backend_plan::StorageBackend::SketchStore
                        && matches!(&materialization.source, Source::TimeSeries { metric: m } if m == metric)
                        && materialization.family == family
                        && materialization.window.size_ms == query.window_secs.saturating_mul(1_000)
                        && materialization.group_by == query.group_by)
                        .then_some(route.materialization)
                })
                .collect();
            if bound.is_empty() {
                return Err(CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: "compiled BackendPlan has no exact materialization for QueryPlan"
                        .into(),
                });
            }
            let entry = QueryPlanEntry::compile_bound(
                query.query_id.clone(),
                canonical.clone(),
                &query.post_asap,
                InstantExecution {
                    lookback_ms: query.window_secs.saturating_mul(1_000),
                    full_history: false,
                    cumulative_readout: true,
                },
                FallbackPolicy::ExactBackend,
                |node, node_family| {
                    let planned_metric = summary_agg_metric(node).ok_or_else(|| {
                        crate::query_plan::QueryPlanError::Invalid(
                            "materialized node has no unique time-series source".into(),
                        )
                    })?;
                    if planned_metric != *metric {
                        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
                            "catalog metric `{metric}` disagrees with post-ASAP source `{planned_metric}`"
                        )));
                    }
                    let fingerprint = bound
                        .iter()
                        .find(|fingerprint| {
                            backend_plan
                                .materializations
                                .get(fingerprint)
                                .is_some_and(|m| &m.family == node_family)
                        })
                        .copied()
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "no exact physical binding for {node_family:?}"
                            ))
                        })?;
                    Ok(MaterializationBinding {
                        materialization: fingerprint,
                        metric: metric.clone(),
                        sid_grouping: query.group_by.clone(),
                        output_grouping: PhysicalGrouping::Reduce(query.group_by.clone()),
                        window_ms: query.window_secs.saturating_mul(1_000),
                    })
                },
            )?;
            if query_entries.insert(canonical.clone(), entry).is_some() {
                return Err(CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: format!("duplicate canonical query identity `{canonical}`"),
                });
            }
        }
        let query_plan = QueryPlan {
            plan_id,
            plan_version: envelope.plan_version,
            entries: query_entries,
        };
        query_plan.validate(&materialization_fingerprints)?;
        Ok(PhysicalPlan {
            envelope,
            collector_plans,
            precompute_plan,
            backend_plan,
            query_plan,
        })
    }
}

fn summary_agg_metric(node: &SummaryNode) -> Option<String> {
    fn walk(node: &SummaryNode, metrics: &mut BTreeSet<String>) {
        match &node.expr {
            SummaryExpr::KeepPreAsap(expr) => {
                let parsed = crate::query_parser::qe_to_parsed_query(expr);
                if !parsed.metric_name.is_empty() {
                    metrics.insert(parsed.metric_name);
                }
            }
            SummaryExpr::SummaryAgg { child, .. } => walk(child, metrics),
            SummaryExpr::SummaryEstimate { summary_input, .. } => walk(summary_input, metrics),
            SummaryExpr::SummaryMerge { children } => {
                for child in children {
                    walk(child, metrics);
                }
            }
            SummaryExpr::SummaryJoin {
                outer: left,
                inner: right,
                ..
            }
            | SummaryExpr::SummarySubtract { left, right } => {
                walk(left, metrics);
                walk(right, metrics);
            }
            SummaryExpr::SummaryDelete { summary_input, .. } => walk(summary_input, metrics),
        }
    }
    let mut metrics = BTreeSet::new();
    walk(node, &mut metrics);
    (metrics.len() == 1)
        .then(|| metrics.into_iter().next())
        .flatten()
}

/// Planner-adapter selection step used before physical compilation. Keeping
/// this separate makes the ownership boundary explicit: callers supply the
/// selected post-ASAP DAG to [`PhysicalCompiler::compile`].
pub fn select_post_asap(
    expr: &QueryExpr,
    accuracy: AccuracyTarget,
    lifecycle: &LifecyclePlanningInput,
    evidence: Option<&TopKMembershipEvidence>,
) -> Result<Rc<SummaryNode>, crate::planner_selection::SelectionError> {
    let model = ControlPlaneCostModel::new(accuracy).with_summary_maintenance(
        SummaryMaintenanceLifecycleCostInputs {
            build_cost: Some(Cost(lifecycle.costs.build)),
            maintenance_cost_per_update: Some(Cost(lifecycle.costs.maintenance_per_update)),
            summary_read_cost: Some(Cost(lifecycle.costs.read)),
            retention_cost_rate: Some(CostRate(lifecycle.costs.retention_per_second)),
            retirement_cost: Some(Cost(lifecycle.costs.retirement)),
        },
        SummaryMaintenanceCapabilities {
            incremental_update: true,
            merge: true,
            delete: false,
        },
    );
    crate::planner_selection::select_summary_with_evidence(
        expr,
        &model,
        &DefaultAccuracyModel,
        &EqualSplitAllocator,
        &QueryEvidence(evidence),
    )
}

fn state_schema_id(fingerprint: asap_types::PolicyFingerprint) -> String {
    format!(
        "{}:summary-state:v1:{}",
        backend_plan::BACKEND_COMPAT,
        fingerprint.0
    )
}

fn state_encodings(family: &SummaryFamilyType) -> Vec<StateEncoding> {
    match family {
        SummaryFamilyType::ExactAggregate(..) => vec![StateEncoding::ExactAccumulatorV1],
        SummaryFamilyType::Sketch(..) => vec![
            StateEncoding::SketchlibProtobufV1,
            StateEncoding::SketchCoreMsgpackV1,
        ],
        _ => Vec::new(),
    }
}

fn validate_evidence(
    query_id: &str,
    evidence: &TopKMembershipEvidence,
    env: &DeploymentEnvironment,
) -> Result<(), CompileError> {
    let age = env
        .observed_at_unix_ms
        .saturating_sub(evidence.observed_at_unix_ms);
    let valid = evidence.selected_lower_bound.is_finite()
        && evidence.excluded_upper_bound.is_finite()
        && evidence.selected_lower_bound > evidence.excluded_upper_bound
        && (0.0..=1.0).contains(&evidence.interval_failure_probability)
        && !evidence.source.trim().is_empty()
        && age <= env.max_evidence_age_ms;
    if valid {
        Ok(())
    } else {
        Err(CompileError::InvalidEvidence {
            query_id: query_id.into(),
            reason: format!("margin/failure/source invalid or age {age}ms exceeds policy"),
        })
    }
}

fn validate_lifecycle_input(
    query_id: &str,
    input: &LifecyclePlanningInput,
) -> Result<(), CompileError> {
    let costs = [
        input.costs.build,
        input.costs.maintenance_per_update,
        input.costs.read,
        input.costs.retention_per_second,
        input.costs.retirement,
    ];
    if input.evaluation_interval_ms == 0
        || input.evidence_valid_for_ms == 0
        || !input.ingestion_rate_per_second.is_finite()
        || input.ingestion_rate_per_second < 0.0
        || !input.horizon_seconds.is_finite()
        || input.horizon_seconds <= 0.0
        || costs.iter().any(|cost| !cost.is_finite() || *cost < 0.0)
    {
        return Err(CompileError::Lifecycle {
            query_id: query_id.into(),
            reason: "rates, horizon, validity, intervals, and costs must be finite and non-negative (interval/horizon/validity non-zero)".into(),
        });
    }
    Ok(())
}

fn validate_window_implementations(
    query: &PlanningQuery,
    environment: &DeploymentEnvironment,
) -> Result<Vec<(SummaryWindowFramework, Cost)>, CompileError> {
    let mut ids = BTreeSet::new();
    let mut cheapest = BTreeMap::<SummaryWindowFramework, f64>::new();
    for candidate in &query.window_implementations {
        let evidence = &candidate.cost;
        let age = environment
            .observed_at_unix_ms
            .saturating_sub(evidence.observed_at_unix_ms);
        let valid = !candidate.implementation_id.trim().is_empty()
            && ids.insert(candidate.implementation_id.clone())
            && !candidate.state_layout.trim().is_empty()
            && !evidence.model_version.trim().is_empty()
            && !evidence.workload_fingerprint.trim().is_empty()
            && evidence.valid_for_ms != 0
            && age <= environment.max_evidence_age_ms.min(evidence.valid_for_ms)
            && evidence.horizon_seconds.is_finite()
            && (evidence.horizon_seconds - query.lifecycle.horizon_seconds).abs() <= f64::EPSILON
            && evidence.cpu_cost.is_finite()
            && evidence.cpu_cost >= 0.0
            && evidence.weighted_cost.is_finite()
            && evidence.weighted_cost >= 0.0
            && candidate.window_secs == query.window_secs
            && candidate.pane_secs != 0
            && candidate.pane_secs <= candidate.window_secs
            && candidate.window_secs % candidate.pane_secs == 0
            // Current Collector runtime contract is the MVP's anchored,
            // tumbling implementation. Other Planner primitives become
            // candidates only when an executor advertises full semantics.
            && candidate.framework == SummaryWindowFramework::Tumbling
            && candidate.pane_secs == candidate.window_secs;
        if !valid {
            return Err(CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: format!(
                    "window implementation `{}` has incomplete, stale, incompatible, or duplicate physical evidence",
                    candidate.implementation_id
                ),
            });
        }
        cheapest
            .entry(candidate.framework.clone())
            .and_modify(|cost| *cost = cost.min(evidence.weighted_cost))
            .or_insert(evidence.weighted_cost);
    }
    if cheapest.is_empty() {
        return Err(CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "no complete executor-feasible window implementation evidence".into(),
        });
    }
    Ok(cheapest
        .into_iter()
        .map(|(framework, cost)| (framework, Cost(cost)))
        .collect())
}

struct PlannerPhysicalSelection {
    lifecycle: CollectorLifecycle,
    window_framework: SummaryWindowFramework,
}

fn select_lifecycle(
    query: &PlanningQuery,
    node: &SummaryNode,
    model: &ControlPlaneCostModel,
    environment: &DeploymentEnvironment,
) -> Result<PlannerPhysicalSelection, CompileError> {
    let workload = QueryWorkload {
        language: QueryLanguage::PromQL,
        query_batch: None,
        repeating_queries: Some(vec![RepeatingEntry {
            query: Query(query.query_id.clone()),
            demand: RepeatedDemand::FixedInterval(RepetitionInterval(
                query.lifecycle.evaluation_interval_ms,
            )),
            requirements: QueryRequirements {
                accuracy: AccuracyRequirement::Explicit(query.accuracy.clone()),
                ..QueryRequirements::default()
            },
            predictability: Predictability::Predictable { known_at: None },
            time_selection: TimeSelection {
                // Collector windows are retired as whole states. They do not
                // claim deletion support for moving-window retractions.
                scope: QueryTimeScope::Unknown,
                lookback: Some(DurationMs(query.window_secs.saturating_mul(1_000))),
                as_of: None,
            },
        }]),
        data_workload: Some(DataWorkload {
            arrival: DataArrival::ContinuouslyIngesting,
            ingestion_rate: Evidence {
                value: Some(Rate(query.lifecycle.ingestion_rate_per_second)),
                source: EvidenceSource::Observed,
                observed_at_ms: Some(query.lifecycle.evidence_observed_at_unix_ms),
                valid_for_ms: Some(query.lifecycle.evidence_valid_for_ms),
            },
            ..DataWorkload::default()
        }),
    };
    let plan = plan_summary_maintenance_lifecycles(
        Rc::new(node.clone()),
        WorkloadDemand::new(&workload, &[0]),
        environment.observed_at_unix_ms,
        Some(Horizon(query.lifecycle.horizon_seconds)),
        SummaryMaintenanceLifecycleCapabilities {
            supports_ephemeral: false,
            supports_prepared: false,
            supports_shared: false,
            supports_continuously_maintained: true,
        },
        model,
    )
    .map_err(|error| CompileError::Lifecycle {
        query_id: query.query_id.clone(),
        reason: error.to_string(),
    })?;
    let guarantee = plan
        .deployments
        .first()
        .and_then(|deployment| deployment.summary_maintenance_lifecycle_guarantee.as_ref())
        .ok_or_else(|| CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "latest ASAPPlanner selected no executable Collector lifecycle".into(),
        })?;
    let window_framework = plan
        .deployments
        .first()
        .and_then(|deployment| deployment.selected_window_framework.clone())
        .ok_or_else(|| CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "latest ASAPPlanner selected no window framework from the supplied physical evidence".into(),
        })?;
    Ok(PlannerPhysicalSelection {
        lifecycle: CollectorLifecycle {
            kind: match guarantee.summary_maintenance_lifecycle {
                SummaryMaintenanceLifecycle::Ephemeral => "ephemeral",
                SummaryMaintenanceLifecycle::Prepared { .. } => "prepared",
                SummaryMaintenanceLifecycle::Shared { .. } => "shared",
                SummaryMaintenanceLifecycle::ContinuouslyMaintained => "continuously_maintained",
            }
            .into(),
            maintenance_mode: match guarantee.summary_maintenance_mode {
                SummaryMaintenanceMode::DirectBuild => "direct_build",
                SummaryMaintenanceMode::Incremental => "incremental",
            }
            .into(),
            evaluation_schedule: match guarantee.evaluation_schedule {
                EvaluationSchedule::OneShot => "one_shot",
                EvaluationSchedule::PerUpdate => "per_update",
                EvaluationSchedule::OnRead => "on_read",
            }
            .into(),
            output_representation: match guarantee.output_representation {
                OutputRepresentation::PlainRows => "plain_rows",
                OutputRepresentation::SummaryState => "summary_state",
                OutputRepresentation::FinalizedValue => "finalized_value",
            }
            .into(),
        },
        window_framework,
    })
}

struct SelectedSketch {
    kind: planner_types::post_asap::SketchKind,
    params: planner_types::post_asap::SketchParams,
    readout: SketchQuery,
}

fn extract_selected(node: &SummaryNode) -> Option<SelectedSketch> {
    let SummaryExpr::SummaryEstimate {
        summary_input,
        query,
    } = &node.expr
    else {
        return None;
    };
    let SummaryExpr::SummaryAgg {
        family: SummaryFamilyType::Sketch(kind, _),
        ..
    } = &summary_input.expr
    else {
        return None;
    };
    Some(SelectedSketch {
        kind: kind.clone(),
        params: kind.params().clone(),
        readout: query.clone(),
    })
}

fn sketch_params_json(params: &planner_types::post_asap::SketchParams) -> Value {
    use planner_types::post_asap::SketchParams as P;
    match params {
        P::Kll { k } => json!({"k": k}),
        P::Cms { width, depth } => json!({"width": width, "depth": depth}),
        P::Hll { precision } => json!({"precision": precision}),
        P::DDSketch { alpha } => json!({"alpha": alpha}),
        P::CmsWithHeap {
            width,
            depth,
            heap_size,
        } => json!({"width": width, "depth": depth, "heap_size": heap_size}),
        P::Kmv { k } | P::Theta { k } => json!({"k": k}),
        P::CountSketch { width, depth } => json!({"width": width, "depth": depth}),
        P::CountSketchWithHeap {
            width,
            depth,
            heap_size,
        } => json!({"width": width, "depth": depth, "heap_size": heap_size}),
    }
}

fn stable_plan_id(materializations: &[CollectorMaterialization]) -> u64 {
    use std::hash::{Hash, Hasher};
    let bytes = serde_json::to_vec(materializations).unwrap_or_default();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment(now: u64) -> DeploymentEnvironment {
        DeploymentEnvironment {
            collector_ids: vec!["edge-a".into(), "edge-b".into()],
            capability_snapshot_id: "caps-7".into(),
            observed_at_unix_ms: now,
            max_evidence_age_ms: 60_000,
            plan_version: 1,
            activation_unix_ms: now,
            expiry_unix_ms: None,
            backend_compat: "asap-query-backend.v1".into(),
        }
    }

    fn request_with_evidence(
        query_id: &str,
        promql: &str,
        evidence: Option<TopKMembershipEvidence>,
    ) -> Result<PlanningRequest, crate::planner_selection::SelectionError> {
        let accuracy = AccuracyTarget::EpsilonDelta {
            epsilon: 0.01,
            delta: 0.01,
        };
        let parsed = crate::query_parser::parse_query_expr_canonical(promql, accuracy.clone())
            .expect("canonical query");
        let lifecycle = LifecyclePlanningInput {
            evaluation_interval_ms: 10_000,
            ingestion_rate_per_second: 100.0,
            evidence_observed_at_unix_ms: 9_500,
            evidence_valid_for_ms: 60_000,
            horizon_seconds: 300.0,
            costs: LifecycleCostEvidence {
                build: 10.0,
                maintenance_per_update: 0.001,
                read: 0.1,
                retention_per_second: 0.001,
                retirement: 1.0,
            },
        };
        let post_asap = select_post_asap(&parsed, accuracy.clone(), &lifecycle, evidence.as_ref())?;
        let mut evidence_by_query = HashMap::new();
        if let Some(evidence) = evidence {
            evidence_by_query.insert(query_id.to_string(), evidence);
        }
        Ok(PlanningRequest {
            queries: vec![PlanningQuery {
                query_id: query_id.into(),
                query_string: promql.into(),
                post_asap,
                source: Source::TimeSeries { metric: "m".into() },
                window_secs: 60,
                group_by: vec![],
                accuracy,
                lifecycle,
                window_implementations: vec![WindowImplementationCandidate {
                    implementation_id: "collector-tumbling-v1".into(),
                    framework: SummaryWindowFramework::Tumbling,
                    window_secs: 60,
                    pane_secs: 60,
                    state_layout: "anchored-pane-v1".into(),
                    cost: ImplementationCostEvidence {
                        model_version: "test-cost-v1".into(),
                        workload_fingerprint: "test-workload".into(),
                        observed_at_unix_ms: 9_500,
                        valid_for_ms: 60_000,
                        horizon_seconds: 300.0,
                        cpu_cost: 1.0,
                        peak_memory_bytes: 1_024,
                        network_bytes: 512,
                        storage_bytes: 512,
                        source_scan_bytes: 0,
                        weighted_cost: 1.0,
                    },
                }],
            }],
            evidence: evidence_by_query,
            planner_revision: PLANNER_REVISION.into(),
        })
    }

    fn request(query_id: &str, promql: &str) -> PlanningRequest {
        request_with_evidence(query_id, promql, None).expect("post-ASAP selection")
    }

    #[test]
    fn compiles_one_decision_into_matching_collector_and_backend_views() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");
        assert_eq!(bundle.collector_plans.len(), 2);
        assert_eq!(bundle.precompute_plan.envelope, bundle.envelope);
        assert_eq!(bundle.precompute_plan.materializations.len(), 1);
        assert_eq!(bundle.precompute_plan.schemas.len(), 1);
        assert_eq!(bundle.precompute_plan.producers.len(), 2);
        assert_eq!(bundle.precompute_plan.ingest.endpoint_path, "/v1/metrics");
        bundle.precompute_plan.validate().expect("runtime contract");
        assert_eq!(bundle.backend_plan.materializations.len(), 1);
        assert_eq!(
            bundle
                .backend_plan
                .materializations
                .values()
                .next()
                .unwrap()
                .lifecycle
                .as_ref()
                .unwrap()
                .summary_maintenance_lifecycle,
            SummaryMaintenanceLifecycle::ContinuouslyMaintained
        );
        assert_eq!(bundle.backend_plan.routing.len(), 1);
        assert_eq!(bundle.query_plan.plan_id, bundle.envelope.plan_id);
        let entry = bundle
            .query_plan
            .lookup("quantile_over_time( 0.99, m[1m] )")
            .expect("canonical QueryPlan lookup");
        assert_eq!(entry.query_id, "q-quantile");
        assert_eq!(
            entry
                .nodes
                .values()
                .filter(|node| matches!(
                    node,
                    crate::query_plan::QueryPlanNode::ReadMaterialization { .. }
                ))
                .count(),
            1
        );
        entry
            .validate(
                &bundle
                    .backend_plan
                    .materializations
                    .keys()
                    .copied()
                    .collect(),
            )
            .expect("executable physical DAG");
        let wire = serde_json::to_vec(&bundle.query_plan).expect("serialize QueryPlan");
        let decoded: QueryPlan = serde_json::from_slice(&wire).expect("deserialize QueryPlan");
        assert_eq!(decoded, bundle.query_plan);
        for plan in &bundle.collector_plans {
            assert_eq!(plan.envelope, bundle.envelope);
            assert_eq!(plan.materializations[0].metric, "m");
            assert_eq!(plan.materializations[0].window_secs, 60);
            assert_eq!(
                plan.materializations[0].abstract_window_framework,
                SummaryWindowFramework::Tumbling
            );
            assert_eq!(
                plan.materializations[0].window_implementation_id,
                "collector-tumbling-v1"
            );
            assert_eq!(plan.materializations[0].pane_secs, 60);
            assert_eq!(
                plan.materializations[0].lifecycle,
                CollectorLifecycle {
                    kind: "continuously_maintained".into(),
                    maintenance_mode: "incremental".into(),
                    evaluation_schedule: "per_update".into(),
                    output_representation: "summary_state".into(),
                }
            );
            assert!(matches!(
                plan.materializations[0].algorithm.as_str(),
                "ddsketch" | "kll"
            ));
        }
    }

    #[test]
    fn multiple_readouts_share_one_precompute_materialization() {
        let mut planning_request = request("q-p90", "quantile_over_time(0.90, m[1m])");
        let second = request("q-p99", "quantile_over_time(0.99, m[1m])")
            .queries
            .into_iter()
            .next()
            .unwrap();
        planning_request.queries.push(second);
        let bundle = PhysicalCompiler
            .compile(planning_request, environment(10_000))
            .unwrap();

        assert_eq!(bundle.query_plan.entries.len(), 2);
        assert_eq!(bundle.backend_plan.materializations.len(), 1);
        assert_eq!(bundle.precompute_plan.materializations.len(), 1);
        assert_eq!(bundle.precompute_plan.schemas.len(), 1);
        assert_eq!(bundle.precompute_plan.producers.len(), 2);
    }

    #[test]
    fn precompute_schema_must_match_backend_semantics() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .unwrap();
        let mut plan = bundle.precompute_plan.clone();
        plan.schemas[0].group_by.push("invented".into());
        assert!(matches!(
            plan.validate_against_backend(&bundle.backend_plan),
            Err(PrecomputePlanError::InvalidSchema { .. })
        ));
    }

    #[test]
    fn missing_window_implementation_evidence_fails_closed() {
        let mut request = request("q-window", "quantile_over_time(0.99, m[1m])");
        request.queries[0].window_implementations.clear();
        let error = PhysicalCompiler
            .compile(request, environment(10_000))
            .expect_err("Planner must not receive a zero-cost invented window");
        assert!(matches!(error, CompileError::Lifecycle { .. }));
    }

    #[test]
    fn precompute_plan_rejects_schema_or_producer_drift() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");

        let mut schema_drift = bundle.precompute_plan.clone();
        schema_drift.schemas[0].schema_version = 0;
        assert!(matches!(
            schema_drift.validate(),
            Err(PrecomputePlanError::InvalidSchema { .. })
        ));

        let mut producer_drift = bundle.precompute_plan;
        producer_drift.producers[0].schema_id = "wrong".into();
        assert!(matches!(
            producer_drift.validate(),
            Err(PrecomputePlanError::InvalidProducer { .. })
        ));
    }

    #[test]
    fn topk_fails_closed_without_membership_evidence() {
        assert!(request_with_evidence("q-topk", "topk(5, m)", None).is_err());
    }

    #[test]
    fn stale_topk_evidence_is_rejected_before_planner_selection() {
        let request = request_with_evidence(
            "q-topk",
            "topk(5, count_over_time(m[1m]))",
            Some(TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.005,
                observed_at_unix_ms: 1,
                source: "runtime-margin-monitor".into(),
            }),
        )
        .expect("selection accepts evidence before freshness validation");
        let error = PhysicalCompiler
            .compile(request, environment(100_000))
            .expect_err("stale certificate must fail");
        assert!(matches!(error, CompileError::InvalidEvidence { .. }));
    }

    #[test]
    fn fresh_topk_evidence_enables_physical_compilation() {
        let request = request_with_evidence(
            "q-topk",
            "topk(5, count_over_time(m[1m]))",
            Some(TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.005,
                observed_at_unix_ms: 9_500,
                source: "runtime-margin-monitor".into(),
            }),
        )
        .expect("selection accepts valid evidence");
        let bundle = PhysicalCompiler
            .compile(request, environment(10_000))
            .expect("certified TopK compiles");
        assert_eq!(bundle.backend_plan.materializations.len(), 1);
        assert_eq!(
            bundle.collector_plans[0].materializations[0]
                .evidence_source
                .as_deref(),
            Some("runtime-margin-monitor")
        );
    }

    #[test]
    fn planner_revision_is_part_of_the_compile_contract() {
        let mut request = request("q", "quantile_over_time(0.9, m[1m])");
        request.planner_revision = "different".into();
        assert!(matches!(
            PhysicalCompiler.compile(request, environment(10_000)),
            Err(CompileError::PlannerRevision { .. })
        ));
    }

    #[test]
    fn stale_lifecycle_evidence_fails_closed() {
        let mut request = request("q", "quantile_over_time(0.9, m[1m])");
        request.queries[0].lifecycle.evidence_observed_at_unix_ms = 1;
        request.queries[0].lifecycle.evidence_valid_for_ms = 10;
        assert!(matches!(
            PhysicalCompiler.compile(request, environment(10_000)),
            Err(CompileError::Lifecycle { .. })
        ));
    }
}
