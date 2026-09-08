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
    CompositionOperator, EvaluationSchedule, OutputRepresentation, SketchAlgorithm, SketchParams,
    SketchQuery, SummaryExpr, SummaryFamilyType, SummaryMaintenanceLifecycle,
    SummaryMaintenanceLifecycleGuarantee, SummaryMaintenanceMode, SummaryNode,
    SummaryWindowFramework,
};
use planner_types::pre_asap::QueryExpr;
use planner_types::workload::{
    AccuracyRequirement, DataArrival, DataWorkload, DurationMs, Evidence, EvidenceSource,
    Predictability, Query, QueryLanguage, QueryRecurrence, QueryRequirements, QueryTimeScope,
    QueryWorkload, Rate, RepeatedDemand, RepeatingEntry, RepetitionInterval, TimeSelection,
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

pub const PLANNER_REVISION: &str = "378a7547ede629a64e84c9f7c810226ce196cce9";

#[derive(Debug, Clone)]
pub struct PlanningQuery {
    pub query_id: String,
    /// Catalog expression used only to build the stable QueryPlan identity.
    /// The selected implementation comes from `post_asap`, never this text.
    pub query_string: String,
    /// Planner-selected post-ASAP DAG. The physical compiler must not
    /// re-select a summary family from pre-ASAP input.
    pub post_asap: Rc<SummaryNode>,
    /// Legacy catalog source retained for request compatibility. Physical
    /// materialization sources are derived from each post-ASAP SummaryAgg;
    /// this field is only used to reject table execution in the MVP.
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
    /// Physical runtime policy selected for this Planner materialization.
    /// It is validated against the selected summary family during physical
    /// compilation and becomes part of the immutable plan generation.
    pub runtime_policy: RuntimeRulePolicy,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeploymentEnvironment {
    pub target: PhysicalDeploymentTarget,
    pub collector_ids: Vec<String>,
    pub capability_snapshot_id: String,
    pub observed_at_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub plan_version: u64,
    pub activation_unix_ms: u64,
    pub expiry_unix_ms: Option<u64>,
    pub backend_compat: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalDeploymentTarget {
    DistributedCollectors,
    BackendLocalRemoteWrite,
}

/// Versioned startup input for the Collector-free compatibility profile.
/// Query/data semantics use ASAPPlanner's canonical workload types directly;
/// this wrapper adds only backend-owned implementation evidence and lifecycle
/// identity required to choose a concrete physical realization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackendLocalPlanningSnapshot {
    pub snapshot_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_cost_evidence: Option<super::workload_cost::WorkloadCostEvidence>,
    pub query_workload: QueryWorkload,
    pub data_workload: DataWorkload,
    pub implementation: BackendLocalImplementation,
    pub environment: DeploymentEnvironment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackendLocalImplementation {
    pub lifecycle_costs: LifecycleCostEvidence,
    pub evidence_observed_at_unix_ms: u64,
    pub evidence_valid_for_ms: u64,
    pub horizon_seconds: f64,
    pub window_implementation_id: String,
    pub state_layout: String,
    pub implementation_cost: ImplementationCostEvidence,
    /// Certificates keyed by exact registered PromQL; converted to root IDs
    /// before workload selection so one query cannot borrow another's evidence.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub topk_evidence: HashMap<String, TopKMembershipEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub transmission_rules: Vec<TransmissionRule>,
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
    PrometheusRemoteWriteV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    UnixNanoseconds,
    UnixMilliseconds,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StateEncoding {
    SketchlibProtobufV1,
    SketchCoreMsgpackV1,
    ExactAccumulatorV1,
}

/// Serializable physical state identity derived from Planner's canonical
/// summary family. This is a wire DTO, not a second planning algebra.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateFamilyContract {
    Exact {
        kind: ExactStateKind,
    },
    Sketch {
        algorithm: SketchAlgorithm,
        parameters: SketchParams,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExactStateKind {
    Sum,
    Count,
    MinMax,
    Increase,
    Rate,
}

impl TryFrom<&SummaryFamilyType> for StateFamilyContract {
    type Error = ();

    fn try_from(family: &SummaryFamilyType) -> Result<Self, Self::Error> {
        use planner_types::post_asap::ExactKind;
        Ok(match family {
            SummaryFamilyType::ExactAggregate(kind, _) => Self::Exact {
                kind: match kind {
                    ExactKind::Sum => ExactStateKind::Sum,
                    ExactKind::Count => ExactStateKind::Count,
                    ExactKind::MinMax => ExactStateKind::MinMax,
                    ExactKind::Increase => ExactStateKind::Increase,
                    ExactKind::Rate => ExactStateKind::Rate,
                },
            },
            SummaryFamilyType::Sketch(kind, _) => Self::Sketch {
                algorithm: kind.algorithm().clone(),
                parameters: kind.params().clone(),
            },
            _ => return Err(()),
        })
    }
}

/// Decoder/schema contract for one content-addressed materialization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StateSchemaContract {
    pub schema_id: String,
    pub schema_version: u32,
    pub materialization: asap_types::PolicyFingerprint,
    pub family: StateFamilyContract,
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
    #[error("unsupported precompute ingest protocol/endpoint/identity contract")]
    UnsupportedIngestEndpoint,
    #[error("duplicate materialization {0}")]
    DuplicateMaterialization(u64),
    #[error("schema set does not exactly match the materialization set")]
    SchemaSetMismatch,
    #[error("schema {schema_id} has invalid version or no encoding")]
    InvalidSchema { schema_id: String },
    #[error("materialization {0} uses a summary family unsupported by the runtime schema")]
    UnsupportedFamily(u64),
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
            .map(|(fingerprint, materialization)| {
                let family = StateFamilyContract::try_from(&materialization.family)
                    .map_err(|_| PrecomputePlanError::UnsupportedFamily(fingerprint.0))?;
                Ok(StateSchemaContract {
                    schema_id: state_schema_id(*fingerprint),
                    schema_version: 1,
                    materialization: *fingerprint,
                    family,
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
            })
            .collect::<Result<Vec<_>, PrecomputePlanError>>()?;
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

    /// Build the backend-local projection used when raw Prometheus samples
    /// are precomputed inside ASAPQuery rather than by ASAPCollector.
    pub fn build_backend_local(
        envelope: PlanEnvelope,
        materializations: Vec<asap_types::PrecomputeMaterialization>,
        backend_plan: &BackendPlan,
    ) -> Result<Self, PrecomputePlanError> {
        let mut plan = Self::build(
            envelope,
            materializations,
            backend_plan,
            &["backend-local".into()],
        )?;
        plan.ingest = IngestContract {
            protocol: IngestProtocol::PrometheusRemoteWriteV1,
            endpoint_path: "/api/v1/write".into(),
            timestamp_unit: TimestampUnit::UnixMilliseconds,
            require_plan_identity: false,
            require_materialization_identity: false,
            require_registered_producer: false,
        };
        plan.producers.clear();
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
        let valid_ingest = match self.ingest.protocol {
            IngestProtocol::ModifiedOtlpMetricsV1 => {
                self.ingest.endpoint_path == "/v1/metrics"
                    && self.ingest.timestamp_unit == TimestampUnit::UnixNanoseconds
                    && self.ingest.require_plan_identity
                    && self.ingest.require_materialization_identity
                    && self.ingest.require_registered_producer
            }
            IngestProtocol::PrometheusRemoteWriteV1 => {
                self.ingest.endpoint_path == "/api/v1/write"
                    && self.ingest.timestamp_unit == TimestampUnit::UnixMilliseconds
                    && !self.ingest.require_plan_identity
                    && !self.ingest.require_materialization_identity
                    && !self.ingest.require_registered_producer
            }
        };
        if !valid_ingest {
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
        let mut schema_ids = BTreeSet::new();
        for schema in &self.schemas {
            if schema.schema_id.trim().is_empty()
                || !schema_ids.insert(schema.schema_id.as_str())
                || schema.schema_version == 0
                || schema.encodings.is_empty()
            {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
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
        if self.ingest.require_registered_producer {
            if let Some(missing) = materializations.difference(&produced).next() {
                return Err(PrecomputePlanError::MissingProducer(missing.0));
            }
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
            let expected_family = StateFamilyContract::try_from(&materialization.family)
                .map_err(|_| PrecomputePlanError::UnsupportedFamily(schema.materialization.0))?;
            if schema.family != expected_family
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
    pub transmission_plan: TransmissionPlan,
    pub backend_plan: BackendPlan,
    pub query_plan: QueryPlan,
    /// Lifecycle component only, not a complete physical-plan comparison.
    pub lifecycle_estimates: Vec<MaterializationLifecycleEstimate>,
    pub cost_comparison: Option<super::workload_cost::WorkloadCostComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaterializationLifecycleEstimate {
    pub materialization: asap_types::PolicyFingerprint,
    pub consumer_query_ids: Vec<String>,
    pub window_implementation_id: String,
    pub horizon_seconds: f64,
    pub expected_reads: f64,
    pub expected_updates: f64,
    pub lifecycle_cost: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TransmissionMode {
    Full,
    Delta,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SequenceScope {
    MaterializationSeriesProducerEpoch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FrameIdentityContract {
    pub identity_version: u32,
    pub sequence_scope: SequenceScope,
    pub require_checkpoint_for_full: bool,
    pub require_base_checkpoint_for_delta: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransmissionRule {
    pub materialization: asap_types::PolicyFingerprint,
    pub producer_id: String,
    pub schema_id: String,
    pub mode: TransmissionMode,
    pub encoding: StateEncoding,
    pub emit_every_ms: u64,
    pub full_checkpoint_every_ms: Option<u64>,
    pub destination_ref: String,
    /// Plan-owned runtime knobs. These values are part of the immutable plan
    /// generation; live feedback may only change them by publishing a
    /// successor generation accepted by [`TransmissionPlan::authorize_successor`].
    #[serde(default)]
    pub runtime_policy: RuntimeRulePolicy,
}

/// How the collector admits updates before sketch maintenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SamplingPolicy {
    Disabled,
    Fixed {
        /// Probability in `(0, 1]`; `1` is valid but should normally be
        /// represented by `Disabled`.
        probability: f64,
        estimator: SamplingEstimator,
    },
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        Self::Disabled
    }
}

impl SamplingPolicy {
    /// Build the fixed physical knob from the controller's canonical
    /// epsilon-floor allocator. Degenerate budgets/rates disable sampling.
    pub fn from_accuracy_budget(
        epsilon_sampling: f64,
        updates_per_window: f64,
        estimator: SamplingEstimator,
    ) -> Self {
        if !epsilon_sampling.is_finite()
            || epsilon_sampling <= 0.0
            || !updates_per_window.is_finite()
            || updates_per_window <= 0.0
        {
            return Self::Disabled;
        }
        let probability =
            crate::epsilon_alloc::derive_sample_p(epsilon_sampling, updates_per_window);
        if probability >= 1.0 {
            Self::Disabled
        } else {
            Self::Fixed {
                probability,
                estimator,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SamplingEstimator {
    /// Hash-threshold element sampling, used by cardinality summaries.
    HashThreshold,
    /// Geometric admission/Nitro-style update sampling, used by frequency
    /// summaries. The sketch readout carries the corresponding correction.
    GeometricAdmission,
}

/// Norm-adaptive Group-of-Sketches delta gating. GOS is meaningful only for
/// CountSketch families and only when the transmission rule is in delta mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GosPolicy {
    pub epsilon_staleness: f64,
    pub sites: u32,
    pub threshold_mode: GosThresholdMode,
}

impl GosPolicy {
    /// Allocate the deterministic staleness share with the same linear-peel
    /// composition used by `epsilon_alloc`. `None` means the selected sketch
    /// already consumes the budget or communication has no allocated weight.
    pub fn from_accuracy_budget(
        epsilon_total: f64,
        epsilon_sketch: f64,
        sites: u32,
        edge_cpu_weight: f64,
        communication_weight: f64,
        threshold_mode: GosThresholdMode,
    ) -> Option<Self> {
        if !epsilon_total.is_finite()
            || !epsilon_sketch.is_finite()
            || !(0.0..=1.0).contains(&epsilon_total)
            || epsilon_sketch < 0.0
            || !edge_cpu_weight.is_finite()
            || edge_cpu_weight < 0.0
            || !communication_weight.is_finite()
            || communication_weight <= 0.0
        {
            return None;
        }
        let (_, epsilon_staleness) = crate::epsilon_alloc::split_budget(
            epsilon_total,
            epsilon_sketch,
            edge_cpu_weight,
            communication_weight,
        );
        (epsilon_staleness.is_finite() && epsilon_staleness > 0.0).then_some(Self {
            epsilon_staleness,
            sites: sites.max(1),
            threshold_mode,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GosThresholdMode {
    Isotropic,
    Anisotropic,
}

/// Sparse-delta semantics within a delta transmission rule. An absolute
/// threshold of zero sends every changed cell. When `gos` is present it
/// replaces the fixed threshold with the GOS norm-adaptive threshold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeltaPolicy {
    pub absolute_threshold: f64,
    pub gos: Option<GosPolicy>,
}

/// Inclusive bounds for one floating-point adaptation knob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveF64Bounds {
    pub min: f64,
    pub max: f64,
    pub max_step: f64,
}

/// Inclusive bounds for one integer adaptation knob.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveU64Bounds {
    pub min: u64,
    pub max: u64,
    pub max_step: u64,
}

/// Guardrails for telemetry-driven runtime adaptation. This is an
/// authorization contract, not an instruction to mutate the active plan.
/// Every accepted change becomes a staged successor PhysicalPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdaptationPolicy {
    pub enabled: bool,
    pub not_before_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub min_evidence_samples: u64,
    pub sample_probability: Option<AdaptiveF64Bounds>,
    pub emit_every_ms: Option<AdaptiveU64Bounds>,
    pub delta_threshold: Option<AdaptiveF64Bounds>,
    pub gos_epsilon_staleness: Option<AdaptiveF64Bounds>,
}

impl Default for RuntimeAdaptationPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            not_before_unix_ms: 0,
            max_evidence_age_ms: 0,
            min_evidence_samples: 0,
            sample_probability: None,
            emit_every_ms: None,
            delta_threshold: None,
            gos_epsilon_staleness: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRulePolicy {
    #[serde(default)]
    pub sampling: SamplingPolicy,
    pub delta: Option<DeltaPolicy>,
    #[serde(default)]
    pub adaptation: RuntimeAdaptationPolicy,
}

/// Identity and sufficiency information for evidence authorizing one rule's
/// successor knobs. Raw measurements remain in the runtime-samples store; the
/// authorization boundary needs only their exact provenance and sample count.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeAdaptationEvidence {
    pub plan_id: u64,
    pub plan_version: u64,
    pub materialization: asap_types::PolicyFingerprint,
    pub producer_id: String,
    pub schema_id: String,
    pub producer_version: String,
    pub observed_at_unix_ms: u64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransmissionPlan {
    pub envelope: PlanEnvelope,
    pub frame_identity: FrameIdentityContract,
    pub rules: Vec<TransmissionRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SummaryFrameKind {
    Full,
    Delta,
}

/// Identity attached to every summary record. Window bounds come from the
/// data point; the remaining fields are carried as reserved `asap.frame.*`
/// attributes until the modified-OTLP schema gains a dedicated message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SummaryFrameIdentity {
    pub identity_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    pub backend_compat: String,
    pub materialization: asap_types::PolicyFingerprint,
    /// Canonical producer-side identity for one concrete retained-label group.
    pub series_identity: String,
    pub schema_id: String,
    pub producer_id: String,
    pub producer_epoch: String,
    pub window_start_unix_nano: u64,
    pub window_end_unix_nano: u64,
    pub sequence: u64,
    pub kind: SummaryFrameKind,
    pub encoding: StateEncoding,
    pub checkpoint_id: Option<String>,
    pub base_checkpoint_id: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransmissionPlanError {
    #[error("TransmissionPlan envelope differs from PrecomputePlan")]
    EnvelopeMismatch,
    #[error("transmission rules do not exactly match precompute producer bindings")]
    ProducerSetMismatch,
    #[error("invalid transmission rule for producer {0}")]
    InvalidRule(String),
    #[error("frame identity is invalid: {0}")]
    InvalidFrame(String),
    #[error("frame has no matching transmission rule")]
    UnknownFrame,
    #[error("runtime policy for producer {producer_id} is invalid: {reason}")]
    InvalidRuntimePolicy { producer_id: String, reason: String },
    #[error("runtime adaptation successor is invalid: {0}")]
    InvalidSuccessor(String),
    #[error("runtime adaptation evidence for producer {0} is missing or invalid")]
    InvalidAdaptationEvidence(String),
    #[error("runtime adaptation for producer {producer_id} exceeds guardrails: {knob}")]
    AdaptationOutOfBounds {
        producer_id: String,
        knob: &'static str,
    },
}

impl TransmissionPlan {
    pub fn build(
        envelope: PlanEnvelope,
        precompute: &PrecomputePlan,
        runtime_policies: &BTreeMap<asap_types::PolicyFingerprint, RuntimeRulePolicy>,
    ) -> Result<Self, TransmissionPlanError> {
        if envelope != precompute.envelope {
            return Err(TransmissionPlanError::EnvelopeMismatch);
        }
        let schemas: BTreeMap<_, _> = precompute
            .schemas
            .iter()
            .map(|schema| (schema.materialization, schema))
            .collect();
        let rules = precompute
            .producers
            .iter()
            .map(|producer| {
                let schema = schemas
                    .get(&producer.materialization)
                    .expect("validated PrecomputePlan schema binding");
                let materialization = precompute
                    .materializations
                    .iter()
                    .find(|m| m.policy_fingerprint() == producer.materialization)
                    .expect("validated PrecomputePlan materialization binding");
                let runtime_policy = runtime_policies
                    .get(&producer.materialization)
                    .cloned()
                    .unwrap_or_default();
                let mode = if runtime_policy.delta.is_some() {
                    TransmissionMode::Delta
                } else {
                    TransmissionMode::Full
                };
                let emit_every_ms = materialization.window_size.saturating_mul(1_000);
                TransmissionRule {
                    materialization: producer.materialization,
                    producer_id: producer.producer_id.clone(),
                    schema_id: producer.schema_id.clone(),
                    mode,
                    encoding: schema.encodings[0].clone(),
                    emit_every_ms,
                    full_checkpoint_every_ms: (mode == TransmissionMode::Delta)
                        .then(|| emit_every_ms.saturating_mul(10)),
                    destination_ref: "asapquery-backend".into(),
                    runtime_policy,
                }
            })
            .collect();
        let plan = Self {
            envelope,
            frame_identity: FrameIdentityContract {
                identity_version: 1,
                sequence_scope: SequenceScope::MaterializationSeriesProducerEpoch,
                require_checkpoint_for_full: true,
                require_base_checkpoint_for_delta: true,
            },
            rules,
        };
        plan.validate(precompute)?;
        Ok(plan)
    }

    pub fn validate(&self, precompute: &PrecomputePlan) -> Result<(), TransmissionPlanError> {
        if self.envelope != precompute.envelope {
            return Err(TransmissionPlanError::EnvelopeMismatch);
        }
        let expected: BTreeSet<_> = precompute
            .producers
            .iter()
            .map(|producer| {
                (
                    producer.materialization,
                    producer.producer_id.as_str(),
                    producer.schema_id.as_str(),
                )
            })
            .collect();
        let actual: BTreeSet<_> = self
            .rules
            .iter()
            .map(|rule| {
                (
                    rule.materialization,
                    rule.producer_id.as_str(),
                    rule.schema_id.as_str(),
                )
            })
            .collect();
        if expected != actual || actual.len() != self.rules.len() {
            return Err(TransmissionPlanError::ProducerSetMismatch);
        }
        for rule in &self.rules {
            let valid_checkpoint_cadence = match (rule.mode, rule.full_checkpoint_every_ms) {
                (TransmissionMode::Full, None) => true,
                (TransmissionMode::Delta, Some(full_every)) => {
                    rule.emit_every_ms > 0
                        && full_every >= rule.emit_every_ms
                        && full_every % rule.emit_every_ms == 0
                }
                _ => false,
            };
            if rule.emit_every_ms == 0
                || rule.destination_ref.is_empty()
                || !valid_checkpoint_cadence
            {
                return Err(TransmissionPlanError::InvalidRule(rule.producer_id.clone()));
            }
            let schema = precompute
                .schemas
                .iter()
                .find(|schema| schema.materialization == rule.materialization)
                .expect("producer set validation guarantees a matching schema");
            if !schema.encodings.contains(&rule.encoding) {
                return Err(TransmissionPlanError::InvalidRule(rule.producer_id.clone()));
            }
            validate_runtime_rule_policy(rule, &schema.family)?;
        }
        Ok(())
    }

    /// Authorize a telemetry-driven successor without mutating this active
    /// plan. Semantic identity, codecs, destination, transmission mode and
    /// checkpoint cadence remain fixed. Only explicitly bounded runtime knobs
    /// may move, and each changed rule needs fresh evidence attributed to the
    /// exact active generation.
    pub fn authorize_successor(
        &self,
        successor: &TransmissionPlan,
        evidence: &[RuntimeAdaptationEvidence],
        now_unix_ms: u64,
    ) -> Result<(), TransmissionPlanError> {
        if successor.envelope.plan_id != self.envelope.plan_id
            || successor.envelope.plan_version != self.envelope.plan_version.saturating_add(1)
            || successor.envelope.backend_compat != self.envelope.backend_compat
            || successor.envelope.planner_revision != self.envelope.planner_revision
            || successor.envelope.capability_snapshot_id != self.envelope.capability_snapshot_id
            || successor.envelope.generated_at_unix_ms < self.envelope.generated_at_unix_ms
            || successor.envelope.activation_unix_ms < successor.envelope.generated_at_unix_ms
            || successor.rules.len() != self.rules.len()
            || successor.frame_identity != self.frame_identity
        {
            return Err(TransmissionPlanError::InvalidSuccessor(
                "successor must be the next version of the same semantic/capability generation"
                    .into(),
            ));
        }

        for current in &self.rules {
            let Some(next) = successor.rules.iter().find(|candidate| {
                candidate.materialization == current.materialization
                    && candidate.producer_id == current.producer_id
                    && candidate.schema_id == current.schema_id
            }) else {
                return Err(TransmissionPlanError::InvalidSuccessor(format!(
                    "missing rule for producer {}",
                    current.producer_id
                )));
            };
            if current.mode != next.mode
                || current.encoding != next.encoding
                || current.full_checkpoint_every_ms != next.full_checkpoint_every_ms
                || current.destination_ref != next.destination_ref
                || current.runtime_policy.adaptation != next.runtime_policy.adaptation
                || sampling_estimator(&current.runtime_policy.sampling)
                    != sampling_estimator(&next.runtime_policy.sampling)
                || delta_shape(&current.runtime_policy.delta)
                    != delta_shape(&next.runtime_policy.delta)
            {
                return Err(TransmissionPlanError::InvalidSuccessor(format!(
                    "rule identity/codec/mode/guardrails drifted for producer {}",
                    current.producer_id
                )));
            }
            if current.emit_every_ms == next.emit_every_ms
                && current.runtime_policy.sampling == next.runtime_policy.sampling
                && current.runtime_policy.delta == next.runtime_policy.delta
            {
                continue;
            }
            let policy = &current.runtime_policy.adaptation;
            if !policy.enabled || now_unix_ms < policy.not_before_unix_ms {
                return Err(TransmissionPlanError::AdaptationOutOfBounds {
                    producer_id: current.producer_id.clone(),
                    knob: "adaptation_disabled_or_in_cooldown",
                });
            }
            let has_evidence = evidence.iter().any(|item| {
                item.plan_id == self.envelope.plan_id
                    && item.plan_version == self.envelope.plan_version
                    && item.materialization == current.materialization
                    && item.producer_id == current.producer_id
                    && item.schema_id == current.schema_id
                    && !item.producer_version.trim().is_empty()
                    && item.sample_count >= policy.min_evidence_samples
                    && item.observed_at_unix_ms <= now_unix_ms
                    && now_unix_ms.saturating_sub(item.observed_at_unix_ms)
                        <= policy.max_evidence_age_ms
            });
            if !has_evidence {
                return Err(TransmissionPlanError::InvalidAdaptationEvidence(
                    current.producer_id.clone(),
                ));
            }
            authorize_f64_change(
                sampling_probability(&current.runtime_policy.sampling),
                sampling_probability(&next.runtime_policy.sampling),
                policy.sample_probability.as_ref(),
                &current.producer_id,
                "sample_probability",
            )?;
            authorize_u64_change(
                current.emit_every_ms,
                next.emit_every_ms,
                policy.emit_every_ms.as_ref(),
                &current.producer_id,
                "emit_every_ms",
            )?;
            authorize_f64_change(
                delta_threshold(&current.runtime_policy.delta),
                delta_threshold(&next.runtime_policy.delta),
                policy.delta_threshold.as_ref(),
                &current.producer_id,
                "delta_threshold",
            )?;
            authorize_f64_change(
                gos_epsilon(&current.runtime_policy.delta),
                gos_epsilon(&next.runtime_policy.delta),
                policy.gos_epsilon_staleness.as_ref(),
                &current.producer_id,
                "gos_epsilon_staleness",
            )?;
        }
        Ok(())
    }

    pub fn validate_frame(
        &self,
        frame: &SummaryFrameIdentity,
    ) -> Result<(), TransmissionPlanError> {
        if frame.identity_version != self.frame_identity.identity_version
            || frame.plan_id != self.envelope.plan_id
            || frame.plan_version != self.envelope.plan_version
            || frame.backend_compat != self.envelope.backend_compat
            || frame.series_identity.is_empty()
            || frame.producer_epoch.is_empty()
            || frame.sequence == 0
            || frame.window_start_unix_nano >= frame.window_end_unix_nano
            || (frame.kind == SummaryFrameKind::Full
                && self.frame_identity.require_checkpoint_for_full
                && frame.checkpoint_id.is_none())
            || (frame.kind == SummaryFrameKind::Delta
                && self.frame_identity.require_base_checkpoint_for_delta
                && frame.base_checkpoint_id.is_none())
        {
            return Err(TransmissionPlanError::InvalidFrame(
                "identity/lifecycle/window/checkpoint fields do not satisfy the active contract"
                    .into(),
            ));
        }
        if self.rules.iter().any(|rule| {
            rule.materialization == frame.materialization
                && rule.producer_id == frame.producer_id
                && rule.schema_id == frame.schema_id
                // A delta rule necessarily emits periodic full checkpoints;
                // a full-only rule must never emit deltas.
                && (frame.kind == SummaryFrameKind::Full
                    || rule.mode == TransmissionMode::Delta)
                && rule.encoding == frame.encoding
        }) {
            Ok(())
        } else {
            Err(TransmissionPlanError::UnknownFrame)
        }
    }
}

fn validate_runtime_rule_policy(
    rule: &TransmissionRule,
    family: &StateFamilyContract,
) -> Result<(), TransmissionPlanError> {
    let invalid = |reason: &str| TransmissionPlanError::InvalidRuntimePolicy {
        producer_id: rule.producer_id.clone(),
        reason: reason.into(),
    };
    if let SamplingPolicy::Fixed {
        probability,
        estimator,
    } = &rule.runtime_policy.sampling
    {
        if !probability.is_finite() || !(0.0..=1.0).contains(probability) || *probability == 0.0 {
            return Err(invalid("sample probability must be finite and in (0, 1]"));
        }
        let supported = matches!(
            (family, *estimator),
            (
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::Hll,
                    ..
                },
                SamplingEstimator::HashThreshold,
            ) | (
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap,
                    ..
                },
                SamplingEstimator::GeometricAdmission,
            )
        );
        if !supported {
            return Err(invalid(
                "sampling estimator is not implemented for the materialization family",
            ));
        }
    }

    if (rule.mode == TransmissionMode::Delta) != rule.runtime_policy.delta.is_some() {
        return Err(invalid(
            "delta policy must be present exactly when transmission mode is delta",
        ));
    }
    if let Some(delta) = &rule.runtime_policy.delta {
        if !delta.absolute_threshold.is_finite() || delta.absolute_threshold < 0.0 {
            return Err(invalid("delta threshold must be finite and non-negative"));
        }
        if !matches!(
            family,
            StateFamilyContract::Sketch {
                algorithm: SketchAlgorithm::DDSketch
                    | SketchAlgorithm::Hll
                    | SketchAlgorithm::Cms
                    | SketchAlgorithm::CmsWithHeap
                    | SketchAlgorithm::CountSketch
                    | SketchAlgorithm::CountSketchWithHeap,
                ..
            }
        ) {
            return Err(invalid(
                "delta transmission is not implemented for the materialization family",
            ));
        }
        if let Some(gos) = &delta.gos {
            if !matches!(
                family,
                StateFamilyContract::Sketch {
                    algorithm: SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap,
                    ..
                }
            ) || !gos.epsilon_staleness.is_finite()
                || !(0.0..=1.0).contains(&gos.epsilon_staleness)
                || gos.epsilon_staleness == 0.0
                || gos.sites == 0
            {
                return Err(invalid(
                    "GOS requires a CountSketch family, epsilon in (0, 1], and at least one site",
                ));
            }
        }
    }

    let adaptation = &rule.runtime_policy.adaptation;
    if adaptation.enabled
        && (adaptation.max_evidence_age_ms == 0 || adaptation.min_evidence_samples == 0)
    {
        return Err(invalid(
            "enabled adaptation requires non-zero evidence age and sample-count requirements",
        ));
    }
    validate_f64_bounds(adaptation.sample_probability.as_ref(), 0.0, 1.0)
        .map_err(|reason| invalid(reason))?;
    validate_f64_bounds(adaptation.delta_threshold.as_ref(), 0.0, f64::MAX)
        .map_err(|reason| invalid(reason))?;
    validate_f64_bounds(adaptation.gos_epsilon_staleness.as_ref(), 0.0, 1.0)
        .map_err(|reason| invalid(reason))?;
    if let Some(bounds) = &adaptation.emit_every_ms {
        if bounds.min == 0
            || bounds.min > bounds.max
            || bounds.max_step == 0
            || !(bounds.min..=bounds.max).contains(&rule.emit_every_ms)
        {
            return Err(invalid("emit interval guardrails are invalid"));
        }
    }
    if let Some(bounds) = &adaptation.sample_probability {
        let current = sampling_probability(&rule.runtime_policy.sampling);
        if current < bounds.min || current > bounds.max {
            return Err(invalid(
                "current sampling probability is outside guardrails",
            ));
        }
    }
    if let Some(bounds) = &adaptation.delta_threshold {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .map(|policy| policy.absolute_threshold)
        else {
            return Err(invalid("delta guardrails require an active delta policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current delta threshold is outside guardrails"));
        }
    }
    if let Some(bounds) = &adaptation.gos_epsilon_staleness {
        let Some(current) = rule
            .runtime_policy
            .delta
            .as_ref()
            .and_then(|policy| policy.gos.as_ref())
            .map(|gos| gos.epsilon_staleness)
        else {
            return Err(invalid("GOS guardrails require an active GOS policy"));
        };
        if current < bounds.min || current > bounds.max {
            return Err(invalid("current GOS epsilon is outside guardrails"));
        }
    }
    Ok(())
}

fn validate_f64_bounds(
    bounds: Option<&AdaptiveF64Bounds>,
    domain_min: f64,
    domain_max: f64,
) -> Result<(), &'static str> {
    let Some(bounds) = bounds else {
        return Ok(());
    };
    if !bounds.min.is_finite()
        || !bounds.max.is_finite()
        || !bounds.max_step.is_finite()
        || bounds.min < domain_min
        || bounds.max > domain_max
        || bounds.min > bounds.max
        || bounds.max_step <= 0.0
    {
        Err("floating-point adaptation guardrails are invalid")
    } else {
        Ok(())
    }
}

fn sampling_probability(policy: &SamplingPolicy) -> f64 {
    match policy {
        SamplingPolicy::Disabled => 1.0,
        SamplingPolicy::Fixed { probability, .. } => *probability,
    }
}

fn sampling_estimator(policy: &SamplingPolicy) -> Option<SamplingEstimator> {
    match policy {
        SamplingPolicy::Disabled => None,
        SamplingPolicy::Fixed { estimator, .. } => Some(*estimator),
    }
}

fn delta_threshold(policy: &Option<DeltaPolicy>) -> f64 {
    policy
        .as_ref()
        .map(|policy| policy.absolute_threshold)
        .unwrap_or(0.0)
}

fn gos_epsilon(policy: &Option<DeltaPolicy>) -> f64 {
    policy
        .as_ref()
        .and_then(|policy| policy.gos.as_ref())
        .map(|gos| gos.epsilon_staleness)
        .unwrap_or(0.0)
}

fn delta_shape(policy: &Option<DeltaPolicy>) -> Option<(Option<(u32, GosThresholdMode)>,)> {
    policy.as_ref().map(|policy| {
        (policy
            .gos
            .as_ref()
            .map(|gos| (gos.sites, gos.threshold_mode)),)
    })
}

fn authorize_f64_change(
    current: f64,
    next: f64,
    bounds: Option<&AdaptiveF64Bounds>,
    producer_id: &str,
    knob: &'static str,
) -> Result<(), TransmissionPlanError> {
    if current == next {
        return Ok(());
    }
    let allowed = bounds.is_some_and(|bounds| {
        next.is_finite()
            && (bounds.min..=bounds.max).contains(&next)
            && (next - current).abs() <= bounds.max_step
    });
    if allowed {
        Ok(())
    } else {
        Err(TransmissionPlanError::AdaptationOutOfBounds {
            producer_id: producer_id.into(),
            knob,
        })
    }
}

fn authorize_u64_change(
    current: u64,
    next: u64,
    bounds: Option<&AdaptiveU64Bounds>,
    producer_id: &str,
    knob: &'static str,
) -> Result<(), TransmissionPlanError> {
    if current == next {
        return Ok(());
    }
    let allowed = bounds.is_some_and(|bounds| {
        (bounds.min..=bounds.max).contains(&next) && current.abs_diff(next) <= bounds.max_step
    });
    if allowed {
        Ok(())
    } else {
        Err(TransmissionPlanError::AdaptationOutOfBounds {
            producer_id: producer_id.into(),
            knob,
        })
    }
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("invalid backend-local workload snapshot: {0}")]
    Snapshot(String),
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

impl BackendLocalPlanningSnapshot {
    /// Invoke the pinned Planner from canonical startup workloads and compile
    /// one backend-local PhysicalPlan. No CollectorPlan is produced and no
    /// precompiled serving artifact is accepted at this boundary.
    pub fn compile(self) -> Result<PhysicalPlan, CompileError> {
        let evidence = self.workload_cost_evidence.clone();
        if self.snapshot_version == 2 && evidence.is_none() {
            return Err(CompileError::Snapshot(
                "version 2 requires complete workload cost evidence".into(),
            ));
        }
        let (request, environment) = self.planning_request()?;
        match evidence {
            Some(evidence) => super::workload_cost::select(
                super::workload_cost::with_exact_alternative(request)?,
                environment,
                &evidence,
            ),
            None => PhysicalCompiler.compile(request, environment),
        }
    }

    /// Build Planner-authorized candidates for evidence collection without publishing.
    pub fn planning_request(
        self,
    ) -> Result<(PlanningRequest, DeploymentEnvironment), CompileError> {
        if self.snapshot_version != 1 && self.snapshot_version != 2 {
            return Err(CompileError::Snapshot(format!(
                "unsupported workload snapshot version {}",
                self.snapshot_version
            )));
        }
        if self.environment.target != PhysicalDeploymentTarget::BackendLocalRemoteWrite {
            return Err(CompileError::Snapshot(
                "compatibility workload requires backend_local_remote_write target".into(),
            ));
        }
        let mut workload = self.query_workload;
        if let Some(embedded) = &workload.data_workload {
            if embedded != &self.data_workload {
                return Err(CompileError::Snapshot(
                    "embedded and standalone DataWorkload snapshots disagree".into(),
                ));
            }
        }
        workload.data_workload = Some(self.data_workload.clone());
        workload
            .validate()
            .map_err(|error| CompileError::Snapshot(error.to_string()))?;
        if workload.language != QueryLanguage::PromQL {
            return Err(CompileError::Snapshot(
                "ASAPQuery compatibility profile accepts PromQL workloads only".into(),
            ));
        }
        let ingestion_rate = self
            .data_workload
            .ingestion_rate
            .value_at(self.environment.observed_at_unix_ms)
            .copied()
            .ok_or_else(|| {
                CompileError::Snapshot("DataWorkload requires fresh ingestion_rate evidence".into())
            })?;
        let entries = workload.entries().collect::<Vec<_>>();
        if entries.is_empty() {
            return Err(CompileError::Snapshot(
                "QueryWorkload must contain at least one query".into(),
            ));
        }
        let mut queries = Vec::with_capacity(entries.len());
        let mut canonical_roots = Vec::with_capacity(entries.len());
        let mut topk_evidence_by_id = HashMap::new();
        for (index, entry) in entries.into_iter().enumerate() {
            let evaluation_interval_ms = match entry.recurrence {
                QueryRecurrence::Repeated(RepeatedDemand::FixedInterval(interval)) => interval.0,
                _ => {
                    return Err(CompileError::Snapshot(format!(
                        "query {index} must use fixed-interval repeated demand in the MVP profile"
                    )))
                }
            };
            let lookback_ms = entry
                .time_selection
                .lookback
                .ok_or_else(|| {
                    CompileError::Snapshot(format!("query {index} requires an explicit lookback"))
                })?
                .0;
            if lookback_ms == 0 || lookback_ms % 1_000 != 0 {
                return Err(CompileError::Snapshot(format!(
                    "query {index} lookback must be a positive whole number of seconds"
                )));
            }
            let accuracy = entry.requirements.accuracy.target();
            let query_string = entry.query.0;
            let parsed =
                crate::query_parser::parse_query_expr_canonical(&query_string, accuracy.clone())
                    .map_err(|error| CompileError::Snapshot(format!("query {index}: {error}")))?;
            let metadata = crate::query_parser::qe_to_parsed_query(&parsed);
            if metadata.metric_name.is_empty() {
                return Err(CompileError::Snapshot(format!(
                    "query {index} has no unique time-series source"
                )));
            }
            if !metadata.label_filters.is_empty() {
                return Err(CompileError::Snapshot(format!(
                    "query {index} uses label filters not yet represented by the physical materialization contract"
                )));
            }
            let lifecycle = LifecyclePlanningInput {
                evaluation_interval_ms,
                ingestion_rate_per_second: ingestion_rate.0,
                evidence_observed_at_unix_ms: self.implementation.evidence_observed_at_unix_ms,
                evidence_valid_for_ms: self.implementation.evidence_valid_for_ms,
                horizon_seconds: self.implementation.horizon_seconds,
                costs: self.implementation.lifecycle_costs.clone(),
            };
            let post_asap = crate::planner_selection::keep_pre_asap(&parsed)
                .map_err(|error| CompileError::Snapshot(format!("query {index}: {error}")))?;
            canonical_roots.push(Rc::new(parsed));
            let mut cost = self.implementation.implementation_cost.clone();
            cost.workload_fingerprint =
                canonical_promql(&query_string).map_err(CompileError::QueryPlan)?;
            cost.horizon_seconds = self.implementation.horizon_seconds;
            let query_id = format!("compat-query-{index}");
            if let Some(evidence) = self.implementation.topk_evidence.get(&query_string) {
                topk_evidence_by_id.insert(query_id.clone(), evidence.clone());
            }
            queries.push(PlanningQuery {
                query_id,
                query_string,
                post_asap,
                source: Source::TimeSeries {
                    metric: metadata.metric_name,
                },
                window_secs: lookback_ms / 1_000,
                group_by: metadata.group_by_labels,
                accuracy,
                lifecycle,
                window_implementations: vec![WindowImplementationCandidate {
                    implementation_id: self.implementation.window_implementation_id.clone(),
                    framework: SummaryWindowFramework::Tumbling,
                    window_secs: lookback_ms / 1_000,
                    pane_secs: lookback_ms / 1_000,
                    state_layout: self.implementation.state_layout.clone(),
                    cost,
                }],
                runtime_policy: RuntimeRulePolicy::default(),
            });
        }
        select_workload_roots(&mut queries, canonical_roots, &topk_evidence_by_id)?;
        Ok((
            PlanningRequest {
                queries,
                evidence: topk_evidence_by_id,
                planner_revision: PLANNER_REVISION.into(),
            },
            self.environment,
        ))
    }
}

impl PhysicalCompiler {
    pub fn compile(
        &self,
        mut request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<PhysicalPlan, CompileError> {
        if request.planner_revision != PLANNER_REVISION {
            return Err(CompileError::PlannerRevision {
                request: request.planner_revision,
                compiler: PLANNER_REVISION,
            });
        }
        if environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite
            && !environment.collector_ids.is_empty()
        {
            return Err(CompileError::Query {
                query_id: "deployment-target".into(),
                reason: "backend-local target cannot declare Collector producers".into(),
            });
        }

        let roots = request
            .queries
            .iter()
            .enumerate()
            .map(|(id, query)| (id, Rc::clone(&query.post_asap)))
            .collect();
        for (id, root) in planner_types::post_asap::share_common_summary_subtrees(roots) {
            request.queries[id].post_asap = root;
        }
        let mut aggregations = Vec::with_capacity(request.queries.len());
        let mut readouts = Vec::with_capacity(request.queries.len());
        let mut collector_materializations = Vec::with_capacity(request.queries.len());
        let mut plan_materializations = Vec::with_capacity(request.queries.len());
        let mut shared_materializations = BTreeMap::new();
        let mut runtime_policies = BTreeMap::new();
        // Binding is established while compiling the physical
        // materializations, then consumed by QueryPlan lowering. The key is
        // the planner DAG node identity across the workload; serving never scans
        // BackendPlan candidates to rediscover this decision.
        let mut node_bindings = HashMap::<usize, asap_types::PolicyFingerprint>::new();
        let consumers = materialization_consumers(&request.queries, environment.target)?;
        let mut lifecycle_estimates = BTreeMap::new();

        for query in &request.queries {
            let evidence = request.evidence.get(&query.query_id);
            if let Some(e) = evidence {
                validate_evidence(&query.query_id, e, &environment)?;
            }
            let node = query.post_asap.clone();
            let selected =
                collect_selected_materializations(&node).map_err(|reason| CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                })?;
            // An exact native fallback has no maintained state and must not
            // depend on evidence for unused window/state implementations.
            if selected.is_empty() {
                continue;
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
            if environment.target == PhysicalDeploymentTarget::DistributedCollectors
                && selected.iter().any(|state| {
                    matches!(
                        state.family,
                        SummaryFamilyType::ExactAggregate(
                            planner_types::post_asap::ExactKind::Count,
                            _
                        )
                    )
                })
            {
                return Err(CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: "observation-count readout requires the backend-local raw producer"
                        .into(),
                });
            }
            match &query.source {
                Source::TimeSeries { .. } => {}
                Source::Table { .. } => {
                    return Err(CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: "MVP physical compiler supports time-series sources only".into(),
                    })
                }
            }
            for (ordinal, selected) in selected.into_iter().enumerate() {
                let metric = selected.metric.clone();
                let aggregation_id = format!("{}:{ordinal}:{}", query.query_id, metric);
                // Rate is a readout over the same reset-aware counter state
                // as Increase. Keep that semantic distinction in QueryPlan,
                // while the physical store binds both to Increase state.
                let physical_family = physical_materialization_family(&selected.family);
                let physical_algorithm = match &physical_family {
                    SummaryFamilyType::ExactAggregate(kind, _) => {
                        format!("{kind:?}").to_ascii_lowercase()
                    }
                    _ => selected.algorithm.clone(),
                };
                let aggregation = physical_aggregation(
                    query,
                    &selected,
                    aggregation_id.clone(),
                    environment.target,
                );
                let precompute_materialization =
                    backend_plan::aggregation_config_for_materialization(&aggregation)?;
                let materialization = precompute_materialization.policy_fingerprint();
                let state_consumers = consumers[&materialization]
                    .iter()
                    .map(|index| &request.queries[*index])
                    .collect::<Vec<_>>();
                let planner_selection = select_lifecycle(
                    query,
                    &selected.node,
                    &model,
                    &environment,
                    &state_consumers,
                )?;
                let window_implementation = query.window_implementations.iter()
                    .filter(|candidate| candidate.framework == planner_selection.window_framework)
                    .min_by(|left, right| left.cost.weighted_cost.total_cmp(&right.cost.weighted_cost))
                    .ok_or_else(|| CompileError::Lifecycle {
                        query_id: query.query_id.clone(),
                        reason: "Planner selected a window framework without a retained concrete implementation".into(),
                    })?;
                lifecycle_estimates
                    .entry(materialization)
                    .or_insert_with(|| MaterializationLifecycleEstimate {
                        materialization,
                        consumer_query_ids: state_consumers
                            .iter()
                            .map(|query| query.query_id.clone())
                            .collect(),
                        window_implementation_id: window_implementation.implementation_id.clone(),
                        horizon_seconds: query.lifecycle.horizon_seconds,
                        expected_reads: planner_selection.expected_reads,
                        expected_updates: planner_selection.expected_updates,
                        lifecycle_cost: planner_selection.lifecycle_cost,
                    });
                let binding_key = selected.node_identity;
                if let Some(existing) = node_bindings.insert(binding_key, materialization) {
                    if existing != materialization {
                        return Err(CompileError::Query {
                            query_id: query.query_id.clone(),
                            reason: "one post-ASAP node resolved to conflicting materializations"
                                .into(),
                        });
                    }
                }
                if let Some(existing) =
                    runtime_policies.insert(materialization, query.runtime_policy.clone())
                {
                    if existing != query.runtime_policy {
                        return Err(CompileError::Query {
                            query_id: query.query_id.clone(),
                            reason: "queries sharing one materialization specify different runtime policies"
                                .into(),
                        });
                    }
                }
                aggregations.push(aggregation);
                if let Some(readout) = selected.readout.clone() {
                    readouts.push(BackendReadout {
                        aggregation_id,
                        op: readout,
                    });
                }
                let collector_materialization = CollectorMaterialization {
                    query_id: format!("state-{}", materialization.0),
                    materialization,
                    metric: metric.clone(),
                    algorithm: physical_algorithm,
                    parameters: selected.parameters,
                    group_by: query.group_by.clone(),
                    window_secs: query.window_secs,
                    abstract_window_framework: planner_selection.window_framework.clone(),
                    window_implementation_id: window_implementation.implementation_id.clone(),
                    pane_secs: window_implementation.pane_secs,
                    state_layout: window_implementation.state_layout.clone(),
                    evidence_source: evidence.map(|e| e.source.clone()),
                    lifecycle: planner_selection.lifecycle.clone(),
                };
                // The store fingerprint identifies state, not its concrete
                // deployment. Consumers may share it only when their selected
                // implementations agree; otherwise publication would install
                // conflicting producers for the same state identity.
                let mut shared_contract = collector_materialization.clone();
                // Keep every consumer in the existing plan identity even
                // though the executable producer declaration is deduplicated.
                plan_materializations.push(collector_materialization.clone());
                shared_contract.query_id.clear();
                if let Some(existing) = shared_materializations.get(&materialization) {
                    if existing != &shared_contract {
                        return Err(CompileError::Query {
                            query_id: query.query_id.clone(),
                            reason: "queries sharing one materialization specify conflicting deployment contracts"
                                .into(),
                        });
                    }
                } else {
                    shared_materializations.insert(materialization, shared_contract);
                    collector_materializations.push(collector_materialization);
                }
            }
        }

        let plan_id = stable_workload_plan_id(&plan_materializations, &request.queries);
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
        let producer_ids = match environment.target {
            PhysicalDeploymentTarget::DistributedCollectors => environment.collector_ids.clone(),
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => Vec::new(),
        };
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
        let precompute_plan = match environment.target {
            PhysicalDeploymentTarget::DistributedCollectors => PrecomputePlan::build(
                envelope.clone(),
                materializations,
                &backend_plan,
                &producer_ids,
            ),
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => {
                PrecomputePlan::build_backend_local(
                    envelope.clone(),
                    materializations,
                    &backend_plan,
                )
            }
        }
        .map_err(|error| CompileError::Query {
            query_id: "precompute-plan".into(),
            reason: error.to_string(),
        })?;
        let transmission_plan =
            TransmissionPlan::build(envelope.clone(), &precompute_plan, &runtime_policies)
                .map_err(|error| CompileError::Query {
                    query_id: "transmission-plan".into(),
                    reason: error.to_string(),
                })?;
        let collector_plans = producer_ids
            .into_iter()
            .map(|collector_id| CollectorPlan {
                transmission_rules: transmission_plan
                    .rules
                    .iter()
                    .filter(|rule| rule.producer_id == collector_id)
                    .cloned()
                    .collect(),
                collector_id,
                envelope: envelope.clone(),
                materializations: collector_materializations.clone(),
            })
            .collect::<Vec<_>>();
        let materialization_fingerprints: BTreeSet<_> =
            backend_plan.materializations.keys().copied().collect();
        let mut query_entries = BTreeMap::new();
        for query in &request.queries {
            let canonical = canonical_promql(&query.query_string)?;
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
                    let fingerprint = node_bindings
                        .get(&(node as *const SummaryNode as usize))
                        .copied()
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "post-ASAP node has no compiled physical binding for {node_family:?}"
                            ))
                        })?;
                    let materialization = backend_plan
                        .materializations
                        .get(&fingerprint)
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "compiled binding {} is absent from BackendPlan",
                                fingerprint.0
                            ))
                        })?;
                    if materialization.family != physical_materialization_family(node_family)
                        || materialization.window.size_ms != query.window_secs.saturating_mul(1_000)
                        || materialization.group_by != query.group_by
                    {
                        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
                            "compiled binding {} disagrees with post-ASAP/deployment semantics",
                            fingerprint.0
                        )));
                    }
                    Ok(MaterializationBinding {
                        materialization: fingerprint,
                        metric: planned_metric,
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
            transmission_plan,
            backend_plan,
            query_plan,
            lifecycle_estimates: lifecycle_estimates.into_values().collect(),
            cost_comparison: None,
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
            SummaryExpr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, metrics);
                walk(rhs, metrics);
            }
        }
    }
    let mut metrics = BTreeSet::new();
    walk(node, &mut metrics);
    (metrics.len() == 1)
        .then(|| metrics.into_iter().next())
        .flatten()
}

/// Shared selection boundary for canonical startup and compile-and-publish.
/// Certificate-bearing roots stay isolated: equal certificate values do not
/// establish that the certificate's source scope covers another query.
pub fn select_workload_roots(
    queries: &mut [PlanningQuery],
    roots: Vec<Rc<QueryExpr>>,
    evidence: &HashMap<String, TopKMembershipEvidence>,
) -> Result<(), CompileError> {
    if roots.len() != queries.len() {
        return Err(CompileError::Snapshot(
            "canonical root/query mapping is incomplete".into(),
        ));
    }
    let mut cohorts: Vec<(AccuracyTarget, Option<String>, Vec<(usize, Rc<QueryExpr>)>)> =
        Vec::new();
    for (index, root) in roots.into_iter().enumerate() {
        let accuracy = &queries[index].accuracy;
        let certificate_scope = evidence
            .contains_key(&queries[index].query_id)
            .then(|| queries[index].query_id.clone());
        if let Some((_, _, roots)) = cohorts
            .iter_mut()
            .find(|(target, scope, _)| target == accuracy && scope == &certificate_scope)
        {
            roots.push((index, root));
        } else {
            cohorts.push((accuracy.clone(), certificate_scope, vec![(index, root)]));
        }
    }
    for (accuracy, scope, roots) in cohorts {
        let model = ControlPlaneCostModel::new(accuracy.clone());
        let certificate = scope.as_ref().and_then(|id| evidence.get(id));
        let selected = crate::planner_selection::select_workload_with_evidence(
            roots,
            accuracy,
            &model,
            &QueryEvidence(certificate),
        )
        .map_err(|error| CompileError::Snapshot(error.to_string()))?;
        for (index, node) in selected {
            queries[index].post_asap = node;
        }
    }
    Ok(())
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
        SummaryFamilyType::Sketch(kind, _)
            if matches!(
                kind.algorithm(),
                SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap
            ) =>
        {
            vec![StateEncoding::SketchCoreMsgpackV1]
        }
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
    let valid = evidence.observed_at_unix_ms <= env.observed_at_unix_ms
        && evidence.selected_lower_bound.is_finite()
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
        let valid = evidence.observed_at_unix_ms <= environment.observed_at_unix_ms
            && !candidate.implementation_id.trim().is_empty()
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
    expected_reads: f64,
    expected_updates: f64,
    lifecycle_cost: f64,
}

fn select_lifecycle(
    query: &PlanningQuery,
    node: &SummaryNode,
    model: &ControlPlaneCostModel,
    environment: &DeploymentEnvironment,
    consumers: &[&PlanningQuery],
) -> Result<PlannerPhysicalSelection, CompileError> {
    // Current lifecycle evidence is per producer with one unit read cost.
    // Conflicting source/rate/horizon/cost snapshots cannot be averaged into
    // invented evidence. Only recurrence may differ between consumers.
    let mut common = query.lifecycle.clone();
    common.evaluation_interval_ms = 0;
    for consumer in consumers {
        let mut input = consumer.lifecycle.clone();
        input.evaluation_interval_ms = 0;
        if input != common {
            return Err(CompileError::Lifecycle {
                query_id: consumer.query_id.clone(),
                reason: "shared producer consumers have conflicting lifecycle evidence".into(),
            });
        }
    }
    let workload = QueryWorkload {
        language: QueryLanguage::PromQL,
        query_batch: None,
        repeating_queries: Some(
            consumers
                .iter()
                .map(|query| RepeatingEntry {
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
                })
                .collect(),
        ),
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
        WorkloadDemand::new(&workload, &(0..consumers.len()).collect::<Vec<_>>()),
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
        expected_reads: plan.expected_reads.ok_or_else(|| CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "missing joint read demand".into(),
        })?,
        expected_updates: plan
            .update_rate
            .ok_or_else(|| CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: "missing source update demand".into(),
            })?
            .0
            * query.lifecycle.horizon_seconds,
        lifecycle_cost: plan.deployments[0]
            .alternatives
            .iter()
            .find(|alternative| {
                alternative.rejection.is_none()
                    && alternative.summary_maintenance_lifecycle
                        == guarantee.summary_maintenance_lifecycle
            })
            .and_then(|alternative| alternative.total_cost)
            .ok_or_else(|| CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: "missing selected lifecycle cost".into(),
            })?
            .0,
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

struct SelectedMaterialization {
    node: Rc<SummaryNode>,
    node_identity: usize,
    metric: String,
    family: SummaryFamilyType,
    readout: Option<SketchQuery>,
    algorithm: String,
    parameters: Value,
}

fn physical_aggregation(
    query: &PlanningQuery,
    selected: &SelectedMaterialization,
    aggregation_id: String,
    target: PhysicalDeploymentTarget,
) -> BackendAggregation {
    BackendAggregation {
        aggregation_id,
        metric_name: selected.metric.clone(),
        family: physical_materialization_family(&selected.family),
        window_secs: query.window_secs,
        spatial_filter: String::new(),
        grouping: query.group_by.clone(),
        item_label: None,
        heap_update_mode: selected.parameters.get("weight_mode").and_then(|mode| {
            match mode.as_str() {
                Some("count") => Some("count"),
                Some("value") => Some("value"),
                _ => None,
            }
        }),
        aggregation_input: match target {
            PhysicalDeploymentTarget::DistributedCollectors => AggregationInput::SketchEnvelope,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => AggregationInput::Raw,
        },
    }
}

fn materialization_consumers(
    queries: &[PlanningQuery],
    target: PhysicalDeploymentTarget,
) -> Result<BTreeMap<asap_types::PolicyFingerprint, BTreeSet<usize>>, CompileError> {
    let mut consumers = BTreeMap::<_, BTreeSet<_>>::new();
    for (index, query) in queries.iter().enumerate() {
        let states = collect_selected_materializations(&query.post_asap).map_err(|reason| {
            CompileError::Query {
                query_id: query.query_id.clone(),
                reason,
            }
        })?;
        for state in states {
            let config = backend_plan::aggregation_config_for_materialization(
                &physical_aggregation(query, &state, query.query_id.clone(), target),
            )?;
            consumers
                .entry(config.policy_fingerprint())
                .or_default()
                .insert(index);
        }
    }
    Ok(consumers)
}

/// Collect every executable materialization leaf in the selected post-ASAP
/// graph. Readout context flows through merge nodes, so a graph such as
/// `Estimate(Merge(Agg(a), Agg(b)))` creates two physical bindings while the
/// serialized QueryPlan retains the merge edges. Unsupported operators are
/// intentionally not traversed: QueryPlan lowers them to an explicit exact
/// fallback node and no unused warm state is provisioned.
fn collect_selected_materializations(
    node: &Rc<SummaryNode>,
) -> Result<Vec<SelectedMaterialization>, String> {
    fn walk(
        node: &Rc<SummaryNode>,
        readout: Option<&SketchQuery>,
        selected: &mut Vec<SelectedMaterialization>,
    ) -> Result<(), String> {
        match &node.expr {
            SummaryExpr::BinaryOp { lhs, rhs, .. }
                if crate::query_plan::exact_value_executable(node) =>
            {
                walk(lhs, readout, selected)?;
                walk(rhs, readout, selected)?;
            }
            SummaryExpr::SummaryAgg {
                child,
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _),
                ..
            } if !matches!(child.expr, SummaryExpr::KeepPreAsap(_))
                && crate::query_plan::exact_value_executable(node) =>
            {
                walk(child, readout, selected)?;
            }
            SummaryExpr::SummaryAgg { child, .. }
                if !matches!(child.expr, SummaryExpr::KeepPreAsap(_)) => {}
            SummaryExpr::SummaryAgg {
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Count, _),
                ..
            } if !crate::query_plan::exact_value_executable(node) => {}
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => walk(summary_input, Some(query), selected)?,
            SummaryExpr::SummaryMerge { children } => {
                for child in children {
                    walk(child, readout, selected)?;
                }
            }
            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::Sketch(kind, _),
                input,
                ..
            } => {
                if let Some(readout) = readout {
                    let mut parameters = sketch_params_json(kind.params());
                    if matches!(readout, SketchQuery::TopK { .. }) {
                        use planner_types::post_asap::SummaryInputExpr;
                        let mode = match &input.weight {
                            SummaryInputExpr::Constant(value) if *value == 1.0 => "count",
                            SummaryInputExpr::Column(
                                planner_types::pre_asap::ColumnRef::SampleValue,
                            ) => "value",
                            _ => return Err("unsupported TopK SummaryUpdate weight".into()),
                        };
                        parameters["weight_mode"] = mode.into();
                    }
                    let metric = summary_agg_metric(node).ok_or_else(|| {
                        "SummaryAgg has no unique time-series source in post-ASAP IR".to_string()
                    })?;
                    selected.push(SelectedMaterialization {
                        node: Rc::clone(node),
                        node_identity: Rc::as_ptr(node) as usize,
                        metric,
                        family: SummaryFamilyType::Sketch(
                            kind.clone(),
                            planner_types::post_asap::GroupingStrategy::PerSubpopulationInstance,
                        ),
                        readout: Some(readout.clone()),
                        algorithm: format!("{:?}", kind.algorithm()).to_ascii_lowercase(),
                        parameters,
                    });
                }
            }
            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(kind, params),
                ..
            } => {
                let metric = summary_agg_metric(node).ok_or_else(|| {
                    "SummaryAgg has no unique time-series source in post-ASAP IR".to_string()
                })?;
                selected.push(SelectedMaterialization {
                    node: Rc::clone(node),
                    node_identity: Rc::as_ptr(node) as usize,
                    metric,
                    family: SummaryFamilyType::ExactAggregate(kind.clone(), params.clone()),
                    readout: None,
                    algorithm: format!("{kind:?}").to_ascii_lowercase(),
                    parameters: Value::Object(Default::default()),
                });
            }
            SummaryExpr::BinaryOp { .. }
            | SummaryExpr::KeepPreAsap(_)
            | SummaryExpr::SummaryAgg { .. }
            | SummaryExpr::SummaryJoin { .. }
            | SummaryExpr::SummarySubtract { .. }
            | SummaryExpr::SummaryDelete { .. } => {}
        }
        Ok(())
    }

    let mut selected = Vec::new();
    walk(node, None, &mut selected)?;
    Ok(selected)
}

fn physical_materialization_family(family: &SummaryFamilyType) -> SummaryFamilyType {
    match family {
        SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Count, _) => {
            // The local raw Sum updater retains the observation count alongside
            // its sum. Both logical states can use this one concrete producer.
            SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Sum,
                planner_types::post_asap::ExactParams::Sum,
            )
        }
        SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Rate, _) => {
            SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Increase,
                planner_types::post_asap::ExactParams::Increase,
            )
        }
        _ => family.clone(),
    }
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

fn stable_workload_plan_id(
    materializations: &[CollectorMaterialization],
    queries: &[PlanningQuery],
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    stable_plan_id(materializations).hash(&mut hasher);
    for query in queries {
        query.query_id.hash(&mut hasher);
        query.query_string.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Count and value rankings must configure different state update contracts.
    #[test]
    fn temporal_topk_binds_planner_update_weight() {
        for (query, mode) in [
            ("topk(1, sum_over_time(m[1m]))", "value"),
            ("topk(1, count_over_time(m[1m]))", "count"),
        ] {
            let evidence = TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.001,
                observed_at_unix_ms: 9500,
                source: "unit-fixture".into(),
            };
            let request = request_with_evidence("topk", query, Some(evidence)).unwrap();
            let plan = PhysicalCompiler
                .compile(request, environment(10000))
                .unwrap();
            assert_eq!(plan.precompute_plan.materializations.len(), 1, "{query}");
            assert_eq!(
                plan.precompute_plan.materializations[0].parameters["weight_mode"], mode,
                "{query}"
            );
        }
    }

    fn environment(now: u64) -> DeploymentEnvironment {
        DeploymentEnvironment {
            target: PhysicalDeploymentTarget::DistributedCollectors,
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
                runtime_policy: RuntimeRulePolicy::default(),
            }],
            evidence: evidence_by_query,
            planner_revision: PLANNER_REVISION.into(),
        })
    }

    fn request(query_id: &str, promql: &str) -> PlanningRequest {
        request_with_evidence(query_id, promql, None).expect("post-ASAP selection")
    }

    // Both production adapters preserve canonical root identity and select the
    // whole evidence-free cohort, rather than independently binding roots.
    #[test]
    fn shared_selection_adapter_preserves_query_mapping() {
        let mut workload = request("q90", "quantile_over_time(0.9, m[1m])");
        workload
            .queries
            .extend(request("q99", "quantile_over_time(0.99, m[1m])").queries);
        let roots = workload
            .queries
            .iter()
            .map(|query| {
                Rc::new(
                    crate::query_parser::parse_query_expr_canonical(
                        &query.query_string,
                        query.accuracy.clone(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        select_workload_roots(&mut workload.queries, roots, &workload.evidence).unwrap();
        let bundle = PhysicalCompiler
            .compile(workload, environment(10000))
            .unwrap();
        assert_eq!(bundle.query_plan.entries.len(), 2);
        assert_eq!(bundle.collector_plans[0].materializations.len(), 1);
        assert_eq!(
            bundle
                .query_plan
                .entries
                .values()
                .map(|entry| entry.query_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["q90", "q99"])
        );
    }

    // A broken input mapping must be rejected, never silently drop a root.
    #[test]
    fn shared_selection_rejects_incomplete_root_mapping() {
        let mut workload = request("q", "quantile_over_time(0.9, m[1m])");
        assert!(select_workload_roots(&mut workload.queries, vec![], &workload.evidence).is_err());
    }

    // Adding another readout adds recurring reads, not another update stream.
    #[test]
    fn joint_lifecycle_charges_shared_updates_once() {
        let baseline = PhysicalCompiler
            .compile(
                request("q90", "quantile_over_time(0.9, m[1m])"),
                environment(10000),
            )
            .unwrap();
        let mut workload = request("q90", "quantile_over_time(0.9, m[1m])");
        let mut second = request("q99", "quantile_over_time(0.99, m[1m])")
            .queries
            .remove(0);
        second.lifecycle.evaluation_interval_ms = 20000;
        workload.queries.push(second);
        let shared = PhysicalCompiler
            .compile(workload, environment(10000))
            .unwrap();
        assert_eq!(shared.lifecycle_estimates.len(), 1);
        let estimate = &shared.lifecycle_estimates[0];
        assert_eq!(estimate.consumer_query_ids, vec!["q90", "q99"]);
        assert_eq!(estimate.expected_reads, 45.0);
        assert_eq!(estimate.expected_updates, 30000.0);
        assert!((estimate.lifecycle_cost - 45.8).abs() < 1e-9);
        assert_eq!(
            estimate.expected_updates,
            baseline.lifecycle_estimates[0].expected_updates
        );
        assert!(
            (estimate.lifecycle_cost - baseline.lifecycle_estimates[0].lifecycle_cost - 1.5).abs()
                < 1e-9
        );
    }

    // Unknown joint provenance cannot be replaced by whichever root came first.
    #[test]
    fn shared_lifecycle_rejects_conflicting_source_evidence() {
        let mut workload = request("q90", "quantile_over_time(0.9, m[1m])");
        let mut second = request("q99", "quantile_over_time(0.99, m[1m])")
            .queries
            .remove(0);
        second.lifecycle.ingestion_rate_per_second = 200.0;
        workload.queries.push(second);
        assert!(matches!(
            PhysicalCompiler.compile(workload, environment(10000)),
            Err(CompileError::Lifecycle { .. })
        ));
    }

    #[test]
    fn shared_materialization_is_emitted_once_for_every_runtime() {
        for target in [
            PhysicalDeploymentTarget::DistributedCollectors,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite,
        ] {
            let mut workload = request("q90", "quantile_over_time(0.90, m[1m])");
            workload
                .queries
                .extend(request("q99", "quantile_over_time(0.99, m[1m])").queries);
            let mut env = environment(10_000);
            env.target = target;
            if target == PhysicalDeploymentTarget::BackendLocalRemoteWrite {
                env.collector_ids.clear();
            }
            let bundle = PhysicalCompiler
                .compile(workload, env)
                .expect("shared compile");
            assert_eq!(bundle.query_plan.entries.len(), 2);
            assert_eq!(bundle.backend_plan.materializations.len(), 1);
            assert_eq!(bundle.precompute_plan.materializations.len(), 1);
            assert_eq!(bundle.precompute_plan.schemas.len(), 1);
            let bindings = bundle
                .query_plan
                .entries
                .values()
                .map(|entry| entry.materialization_bindings()[0].materialization)
                .collect::<BTreeSet<_>>();
            assert_eq!(bindings.len(), 1);
            for collector in &bundle.collector_plans {
                assert_eq!(collector.materializations.len(), 1);
                assert!(bindings.contains(&collector.materializations[0].materialization));
            }
        }
    }

    #[test]
    fn adding_shared_consumer_changes_plan_identity() {
        let workload = request("q90", "quantile_over_time(0.90, m[1m])");
        let one = PhysicalCompiler
            .compile(workload, environment(10_000))
            .unwrap();
        let mut workload = request("q90", "quantile_over_time(0.90, m[1m])");
        workload
            .queries
            .extend(request("q99", "quantile_over_time(0.99, m[1m])").queries);
        let two = PhysicalCompiler
            .compile(workload, environment(10_000))
            .unwrap();
        assert_ne!(one.envelope.plan_id, two.envelope.plan_id);
        assert_eq!(two.collector_plans[0].materializations.len(), 1);
    }

    #[test]
    fn different_sources_do_not_share_materializations() {
        let mut workload = request("qm", "quantile_over_time(0.90, m[1m])");
        let mut other = request("qn", "quantile_over_time(0.90, n[1m])");
        other.queries[0].source = Source::TimeSeries { metric: "n".into() };
        workload.queries.extend(other.queries);
        let bundle = PhysicalCompiler
            .compile(workload, environment(10_000))
            .unwrap();
        assert_eq!(bundle.backend_plan.materializations.len(), 2);
        assert_eq!(bundle.precompute_plan.materializations.len(), 2);
        for collector in &bundle.collector_plans {
            assert_eq!(collector.materializations.len(), 2);
        }
    }

    #[test]
    fn rate_and_increase_share_physical_counter_state() {
        let mut workload = request("rate", "rate(m[1m])");
        workload
            .queries
            .extend(request("increase", "increase(m[1m])").queries);
        let bundle = PhysicalCompiler
            .compile(workload, environment(10_000))
            .unwrap();
        assert_eq!(bundle.query_plan.entries.len(), 2);
        assert_eq!(bundle.precompute_plan.materializations.len(), 1);
        for collector in &bundle.collector_plans {
            assert_eq!(collector.materializations.len(), 1);
            assert_eq!(collector.materializations[0].algorithm, "increase");
        }
    }

    #[test]
    fn shared_materialization_rejects_conflicting_deployment_contracts() {
        for field in ["implementation", "layout"] {
            let mut workload = request("q90", "quantile_over_time(0.90, m[1m])");
            let mut other = request("q99", "quantile_over_time(0.99, m[1m])");
            let implementation = &mut other.queries[0].window_implementations[0];
            if field == "implementation" {
                implementation.implementation_id = "another-implementation".into();
            } else {
                implementation.state_layout = "another-layout".into();
            }
            workload.queries.extend(other.queries);
            let error = PhysicalCompiler
                .compile(workload, environment(10_000))
                .expect_err("conflicting shared state must fail before publication");
            assert!(
                error
                    .to_string()
                    .contains("conflicting deployment contracts"),
                "{error}"
            );
        }
    }

    #[test]
    fn exact_dashboard_binds_sum_and_count_to_one_local_producer() {
        // Both dashboard roots use one packed raw accumulator, with explicit readouts.
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entries = snapshot.query_workload.repeating_queries.as_mut().unwrap();
        entries[0].query = Query("sum by (service) (sum_over_time(m[1m]))".into());
        entries[0].requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let mut mean = entries[0].clone();
        mean.query = Query(
            "sum by (service) (sum_over_time(m[1m])) / sum by (service) (count_over_time(m[1m]))"
                .into(),
        );
        entries.push(mean);
        let bundle = snapshot.compile().unwrap();
        assert_eq!(bundle.precompute_plan.materializations.len(), 1);
        assert_eq!(bundle.query_plan.entries.len(), 2);
        for entry in bundle.query_plan.entries.values() {
            assert!(
                !entry.nodes.values().any(|node| matches!(
                    node,
                    crate::query_plan::QueryPlanNode::ExactFallback { .. }
                )),
                "{entry:?}"
            );
            assert_eq!(entry.materialization_bindings().len(), 1);
        }
        assert!(bundle.query_plan.entries.values().any(|entry| entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::Binary { .. }))));
    }

    // Grouping must not move through non-additive arithmetic during physical
    // packing: SUM(instance SUM / instance COUNT) is not pooled SUM / COUNT.
    #[test]
    fn non_additive_entity_reduction_does_not_bind_pooled_state() {
        for query in [
            "sum by (service) (sum_over_time(m[1m]) / count_over_time(m[1m]))",
            "sum by (service) (sum_over_time(m[1m])) / sum by (region) (count_over_time(m[1m]))",
            "sum(m) / sum_over_time(m[1m])",
            "sum_over_time(m[1m]) / count_over_time(m[5m])",
            "sum_over_time(m[1m] offset 1h) / count_over_time(m[1m] offset 1h)",
        ] {
            let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
                "../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
            let entries = snapshot.query_workload.repeating_queries.as_mut().unwrap();
            entries[0].query = Query(query.into());
            entries[0].requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
            let bundle = snapshot.compile().unwrap();
            assert!(bundle.precompute_plan.materializations.is_empty());
            assert!(bundle.query_plan.entries.values().all(|entry| matches!(
                entry.nodes[&entry.root],
                crate::query_plan::QueryPlanNode::ExactFallback { .. }
            )));
        }
    }

    #[test]
    fn canonical_snapshot_preserves_shared_bindings_after_serialization() {
        // Two different registered readouts survive publication with one state.
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entries = snapshot.query_workload.repeating_queries.as_mut().unwrap();
        let mut second = entries[0].clone();
        second.query = Query("quantile_over_time(0.90, m[1m])".into());
        entries.push(second);
        let bundle = snapshot.compile().unwrap();
        assert_eq!(bundle.query_plan.entries.len(), 2);
        assert_eq!(bundle.precompute_plan.materializations.len(), 1);
        let query_plan: QueryPlan =
            serde_json::from_slice(&serde_json::to_vec(&bundle.query_plan).unwrap()).unwrap();
        let bindings = query_plan
            .entries
            .values()
            .flat_map(|entry| entry.materialization_bindings())
            .map(|binding| binding.materialization)
            .collect::<BTreeSet<_>>();
        assert_eq!(bindings.len(), 1);
        query_plan.validate(&bindings).unwrap();
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
        bundle
            .transmission_plan
            .validate(&bundle.precompute_plan)
            .expect("transmission contract");
        assert_eq!(bundle.transmission_plan.rules.len(), 2);
        let rule = &bundle.transmission_plan.rules[0];
        let frame = SummaryFrameIdentity {
            identity_version: 1,
            plan_id: bundle.envelope.plan_id,
            plan_version: bundle.envelope.plan_version,
            backend_compat: bundle.envelope.backend_compat.clone(),
            materialization: rule.materialization,
            series_identity: "service=checkout,zone=a".into(),
            schema_id: rule.schema_id.clone(),
            producer_id: rule.producer_id.clone(),
            producer_epoch: "boot-1".into(),
            window_start_unix_nano: 1,
            window_end_unix_nano: 2,
            sequence: 1,
            kind: SummaryFrameKind::Full,
            encoding: rule.encoding.clone(),
            checkpoint_id: Some("checkpoint-1".into()),
            base_checkpoint_id: None,
        };
        bundle
            .transmission_plan
            .validate_frame(&frame)
            .expect("matching frame identity");
        let mut wrong_version = frame;
        wrong_version.plan_version += 1;
        assert!(matches!(
            bundle.transmission_plan.validate_frame(&wrong_version),
            Err(TransmissionPlanError::InvalidFrame(_))
        ));
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
            assert_eq!(plan.transmission_rules.len(), 1);
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
    fn backend_local_precompute_contract_has_no_collector_producers() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");
        let plan = PrecomputePlan::build_backend_local(
            bundle.envelope.clone(),
            bundle.precompute_plan.materializations.clone(),
            &bundle.backend_plan,
        )
        .expect("backend-local contract");

        assert_eq!(
            plan.ingest.protocol,
            IngestProtocol::PrometheusRemoteWriteV1
        );
        assert_eq!(plan.ingest.endpoint_path, "/api/v1/write");
        assert_eq!(plan.ingest.timestamp_unit, TimestampUnit::UnixMilliseconds);
        assert!(plan.producers.is_empty());
        plan.validate_against_backend(&bundle.backend_plan)
            .expect("valid backend-local projection");
        TransmissionPlan::build(bundle.envelope, &plan, &BTreeMap::new())
            .expect("empty backend-local transmission contract")
            .validate(&plan)
            .expect("valid empty transmission plan");
    }

    #[test]
    fn canonical_workload_snapshot_invokes_backend_local_planning() {
        let data_workload = DataWorkload {
            arrival: DataArrival::ContinuouslyIngesting,
            ingestion_rate: Evidence {
                value: Some(Rate(100.0)),
                source: EvidenceSource::Declared,
                observed_at_ms: None,
                valid_for_ms: None,
            },
            ..DataWorkload::default()
        };
        let query_workload = QueryWorkload {
            language: QueryLanguage::PromQL,
            query_batch: None,
            repeating_queries: Some(vec![RepeatingEntry {
                query: Query("quantile_over_time(0.99, m[1m])".into()),
                demand: RepeatedDemand::FixedInterval(RepetitionInterval(10_000)),
                requirements: QueryRequirements {
                    accuracy: AccuracyRequirement::Explicit(AccuracyTarget::EpsilonDelta {
                        epsilon: 0.01,
                        delta: 0.01,
                    }),
                    ..QueryRequirements::default()
                },
                predictability: Predictability::Predictable { known_at: None },
                time_selection: TimeSelection {
                    scope: QueryTimeScope::RealTime,
                    lookback: Some(DurationMs(60_000)),
                    as_of: None,
                },
            }]),
            data_workload: Some(data_workload.clone()),
        };
        let mut environment = environment(10_000);
        environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        environment.collector_ids.clear();
        let template = request("template", "quantile_over_time(0.99, m[1m])")
            .queries
            .remove(0);
        let snapshot = BackendLocalPlanningSnapshot {
            snapshot_version: 1,
            workload_cost_evidence: None,
            query_workload,
            data_workload,
            implementation: BackendLocalImplementation {
                lifecycle_costs: template.lifecycle.costs,
                evidence_observed_at_unix_ms: 9_500,
                evidence_valid_for_ms: 60_000,
                horizon_seconds: 300.0,
                window_implementation_id: "backend-tumbling-v1".into(),
                state_layout: "anchored-pane-v1".into(),
                implementation_cost: template.window_implementations[0].cost.clone(),
                topk_evidence: HashMap::new(),
            },
            environment,
        };
        let first = snapshot
            .clone()
            .compile()
            .expect("first deterministic plan");
        let second = snapshot
            .clone()
            .compile()
            .expect("second deterministic plan");
        assert_eq!(first.envelope, second.envelope);
        assert_eq!(first.backend_plan, second.backend_plan);
        assert_eq!(first.query_plan, second.query_plan);
        assert_eq!(first.transmission_plan, second.transmission_plan);
        assert_eq!(
            serde_json::to_value(&first.precompute_plan).unwrap(),
            serde_json::to_value(&second.precompute_plan).unwrap()
        );

        let encoded = serde_json::to_vec(&snapshot).expect("serialize startup snapshot");
        let decoded: BackendLocalPlanningSnapshot =
            serde_json::from_slice(&encoded).expect("deserialize startup snapshot");
        let bundle = decoded.compile().expect("canonical startup planning");

        assert!(bundle.collector_plans.is_empty());
        assert!(bundle.transmission_plan.rules.is_empty());
        assert_eq!(
            bundle.precompute_plan.ingest.protocol,
            IngestProtocol::PrometheusRemoteWriteV1
        );
        assert!(bundle.precompute_plan.producers.is_empty());
        assert_eq!(bundle.query_plan.entries.len(), 1);
        assert_eq!(bundle.envelope.planner_revision, PLANNER_REVISION);
    }

    #[test]
    fn backend_local_compiler_materializes_declared_exact_promql_cases() {
        for (query_id, promql, expected_readout) in [
            (
                "q-rate",
                "rate(m[1m])",
                crate::query_plan::ExactReadout::Rate,
            ),
            (
                "q-increase",
                "increase(m[1m])",
                crate::query_plan::ExactReadout::Increase,
            ),
            (
                "q-sum",
                "sum_over_time(m[1m])",
                crate::query_plan::ExactReadout::Sum,
            ),
        ] {
            let mut deployment = environment(10_000);
            deployment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            deployment.collector_ids.clear();
            let plan = PhysicalCompiler
                .compile(request(query_id, promql), deployment)
                .unwrap_or_else(|error| panic!("{promql} must compile: {error}"));
            assert_eq!(plan.backend_plan.materializations.len(), 1, "{promql}");
            assert_eq!(plan.query_plan.entries.len(), 1, "{promql}");
            assert!(plan.collector_plans.is_empty(), "{promql}");
            let entry = plan.query_plan.entries.values().next().unwrap();
            assert!(matches!(
                entry.nodes.get(&entry.root),
                Some(crate::query_plan::QueryPlanNode::ExactReadout { readout, .. })
                    if *readout == expected_readout
            ));
            if expected_readout == crate::query_plan::ExactReadout::Rate {
                let materialization = plan.backend_plan.materializations.values().next().unwrap();
                assert_eq!(
                    materialization.family,
                    SummaryFamilyType::ExactAggregate(
                        planner_types::post_asap::ExactKind::Increase,
                        planner_types::post_asap::ExactParams::Increase,
                    )
                );
            }
        }
    }

    #[test]
    fn checked_in_backend_local_snapshot_is_canonical_and_compilable() {
        let source = include_str!("../../../docs/examples/asapquery-planning-snapshot.json");
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_str(source).expect("strict canonical workload fixture");
        let encoded = serde_json::to_value(&snapshot).expect("canonical snapshot value");
        let fixture: serde_json::Value = serde_json::from_str(source).expect("fixture JSON");
        assert_eq!(encoded, fixture);

        let plan = snapshot.compile().expect("fixture compiles");
        assert!(plan.collector_plans.is_empty());
        assert!(plan.transmission_plan.rules.is_empty());
        assert_eq!(
            plan.precompute_plan.ingest.protocol,
            IngestProtocol::PrometheusRemoteWriteV1
        );
        assert_eq!(plan.query_plan.entries.len(), 1);
    }

    #[test]
    fn compatibility_demo_snapshot_compiles_the_complete_query_matrix() {
        let source =
            include_str!("../../../docs/examples/asapquery-compatibility-demo-snapshot.json");
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_str(source).expect("strict compatibility demo fixture");
        let plan = snapshot.compile().expect("compatibility demo compiles");

        assert!(plan.collector_plans.is_empty());
        assert!(plan.transmission_plan.rules.is_empty());
        assert_eq!(plan.query_plan.entries.len(), 6);
        assert_eq!(plan.precompute_plan.materializations.len(), 5);
        for query in [
            "rate(asap_demo_counter_total[5s])",
            "increase(asap_demo_counter_total[5s])",
            "sum_over_time(asap_demo_gauge[5s])",
            "quantile_over_time(0.5, asap_demo_latency_ms[5s])",
            "topk(1, sum_over_time(asap_demo_gauge[5s]))",
            "topk(1, count_over_time(asap_demo_gauge[5s]))",
        ] {
            assert!(plan.query_plan.lookup(query).is_ok(), "missing {query}");
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
    fn compiles_every_materialization_leaf_in_a_merge_dag() {
        let mut planning_request = request("q-merge", "quantile_over_time(0.90, m[1m])");
        let right_request = request("q-right", "quantile_over_time(0.90, n[1m])");
        let left_root = planning_request.queries[0].post_asap.clone();
        let right_root = right_request.queries[0].post_asap.clone();
        let (left, query) = match &left_root.expr {
            SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } => (summary_input.clone(), query.clone()),
            other => panic!("expected selected estimate, got {other:?}"),
        };
        let right = match &right_root.expr {
            SummaryExpr::SummaryEstimate { summary_input, .. } => summary_input.clone(),
            other => panic!("expected selected estimate, got {other:?}"),
        };
        let merge = Rc::new(SummaryNode {
            expr: SummaryExpr::SummaryMerge {
                children: vec![left.clone(), right.clone()],
            },
            schema: left.schema.clone(),
            guarantee: None,
        });
        planning_request.queries[0].post_asap = Rc::new(SummaryNode {
            expr: SummaryExpr::SummaryEstimate {
                summary_input: merge,
                query,
            },
            schema: left_root.schema.clone(),
            guarantee: left_root.guarantee.clone(),
        });

        let bundle = PhysicalCompiler
            .compile(planning_request, environment(10_000))
            .expect("compile merged post-ASAP DAG");
        assert_eq!(bundle.backend_plan.materializations.len(), 2);
        assert_eq!(bundle.precompute_plan.materializations.len(), 2);
        assert_eq!(bundle.backend_plan.routing.len(), 2);
        let entry = bundle.query_plan.entries.values().next().unwrap();
        assert_eq!(
            entry
                .nodes
                .values()
                .filter(|node| matches!(
                    node,
                    crate::query_plan::QueryPlanNode::ReadMaterialization { .. }
                ))
                .count(),
            2
        );
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            crate::query_plan::QueryPlanNode::SummaryMerge { inputs } if inputs.len() == 2
        )));
        let bound_metrics = entry
            .nodes
            .values()
            .filter_map(|node| match node {
                crate::query_plan::QueryPlanNode::ReadMaterialization { binding } => {
                    Some(binding.metric.as_str())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(bound_metrics, BTreeSet::from(["m", "n"]));
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
    fn precompute_plan_rejects_empty_or_duplicate_schema_ids() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile schema");

        let mut empty = bundle.precompute_plan.clone();
        empty.schemas[0].schema_id.clear();
        assert!(matches!(
            empty.validate(),
            Err(PrecomputePlanError::InvalidSchema { .. })
        ));

        let mut duplicate = bundle.precompute_plan;
        duplicate.schemas.push(duplicate.schemas[0].clone());
        assert!(matches!(
            duplicate.validate(),
            Err(PrecomputePlanError::InvalidSchema { .. })
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
    fn future_topk_and_window_evidence_are_rejected() {
        let topk = request_with_evidence(
            "q-topk",
            "topk(5, count_over_time(m[1m]))",
            Some(TopKMembershipEvidence {
                selected_lower_bound: 101.0,
                excluded_upper_bound: 100.0,
                interval_failure_probability: 0.005,
                observed_at_unix_ms: 10_001,
                source: "runtime-margin-monitor".into(),
            }),
        )
        .expect("selection occurs before deployment-time freshness validation");
        assert!(matches!(
            PhysicalCompiler.compile(topk, environment(10_000)),
            Err(CompileError::InvalidEvidence { .. })
        ));

        let mut window = request("q-window", "quantile_over_time(0.99, m[1m])");
        window.queries[0].window_implementations[0]
            .cost
            .observed_at_unix_ms = 10_001;
        assert!(matches!(
            PhysicalCompiler.compile(window, environment(10_000)),
            Err(CompileError::Lifecycle { .. })
        ));
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

    #[test]
    fn runtime_policy_uses_canonical_sampling_and_gos_allocators() {
        let sampling = SamplingPolicy::from_accuracy_budget(
            0.05,
            1_000.0,
            SamplingEstimator::GeometricAdmission,
        );
        let SamplingPolicy::Fixed { probability, .. } = sampling else {
            panic!("positive budget and rate must enable sampling");
        };
        assert!((probability - 1.0 / 3.5).abs() < 1e-9);

        let gos =
            GosPolicy::from_accuracy_budget(0.1, 0.03, 4, 1.0, 1.0, GosThresholdMode::Isotropic)
                .expect("positive communication allocation");
        assert!((gos.epsilon_staleness - 0.035).abs() < 1e-12);
        assert_eq!(gos.sites, 4);
    }

    #[test]
    fn runtime_policy_is_family_and_mode_checked() {
        let bundle = PhysicalCompiler
            .compile(
                request("q", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");
        let mut rule = bundle.transmission_plan.rules[0].clone();
        let mut invalid_encoding = bundle.transmission_plan.clone();
        invalid_encoding.rules[0].encoding = StateEncoding::ExactAccumulatorV1;
        assert!(matches!(
            invalid_encoding.validate(&bundle.precompute_plan),
            Err(TransmissionPlanError::InvalidRule(_))
        ));
        rule.runtime_policy.sampling = SamplingPolicy::Fixed {
            probability: 0.5,
            estimator: SamplingEstimator::HashThreshold,
        };
        let hll = StateFamilyContract::Sketch {
            algorithm: SketchAlgorithm::Hll,
            parameters: SketchParams::Hll { precision: 14 },
        };
        let count_sketch = StateFamilyContract::Sketch {
            algorithm: SketchAlgorithm::CountSketch,
            parameters: SketchParams::CountSketch {
                width: 128,
                depth: 4,
            },
        };
        validate_runtime_rule_policy(&rule, &hll).expect("HLL supports hash-threshold sampling");
        assert!(matches!(
            validate_runtime_rule_policy(&rule, &count_sketch),
            Err(TransmissionPlanError::InvalidRuntimePolicy { .. })
        ));

        rule.runtime_policy.sampling = SamplingPolicy::Disabled;
        rule.mode = TransmissionMode::Delta;
        rule.full_checkpoint_every_ms = Some(300_000);
        rule.runtime_policy.delta = Some(DeltaPolicy {
            absolute_threshold: 0.0,
            gos: Some(GosPolicy {
                epsilon_staleness: 0.02,
                sites: 2,
                threshold_mode: GosThresholdMode::Isotropic,
            }),
        });
        validate_runtime_rule_policy(&rule, &count_sketch).expect("CountSketch supports delta GOS");
        assert!(matches!(
            validate_runtime_rule_policy(&rule, &hll),
            Err(TransmissionPlanError::InvalidRuntimePolicy { .. })
        ));
    }

    #[test]
    fn compiler_emits_delta_rule_and_accepts_periodic_full_checkpoint() {
        let mut request = request_with_evidence(
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
        .expect("frequency selection");
        request.queries[0].runtime_policy.delta = Some(DeltaPolicy {
            absolute_threshold: 0.0,
            gos: None,
        });
        let bundle = PhysicalCompiler
            .compile(request, environment(10_000))
            .expect("delta-capable physical plan");
        let rule = &bundle.transmission_plan.rules[0];
        assert_eq!(rule.mode, TransmissionMode::Delta);
        assert!(rule.full_checkpoint_every_ms.is_some());

        for (kind, sequence) in [(SummaryFrameKind::Full, 1), (SummaryFrameKind::Delta, 2)] {
            let frame = SummaryFrameIdentity {
                identity_version: 1,
                plan_id: bundle.envelope.plan_id,
                plan_version: bundle.envelope.plan_version,
                backend_compat: bundle.envelope.backend_compat.clone(),
                materialization: rule.materialization,
                series_identity: "service=checkout,zone=a".into(),
                schema_id: rule.schema_id.clone(),
                producer_id: rule.producer_id.clone(),
                producer_epoch: "boot-1".into(),
                window_start_unix_nano: 1,
                window_end_unix_nano: 2,
                sequence,
                encoding: rule.encoding.clone(),
                checkpoint_id: (kind == SummaryFrameKind::Full).then(|| "cp-1".into()),
                base_checkpoint_id: (kind == SummaryFrameKind::Delta).then(|| "cp-1".into()),
                kind,
            };
            bundle
                .transmission_plan
                .validate_frame(&frame)
                .expect("delta rule accepts its deltas and recovery full frames");
        }
    }

    #[test]
    fn runtime_adaptation_requires_fresh_exact_evidence_and_successor_version() {
        let bundle = PhysicalCompiler
            .compile(
                request("q", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .expect("compile");
        let mut current = bundle.transmission_plan;
        let rule = &mut current.rules[0];
        rule.runtime_policy.adaptation = RuntimeAdaptationPolicy {
            enabled: true,
            not_before_unix_ms: 10_500,
            max_evidence_age_ms: 5_000,
            min_evidence_samples: 100,
            sample_probability: None,
            emit_every_ms: Some(AdaptiveU64Bounds {
                min: 30_000,
                max: 120_000,
                max_step: 10_000,
            }),
            delta_threshold: None,
            gos_epsilon_staleness: None,
        };
        let mut successor = current.clone();
        successor.envelope.plan_version += 1;
        successor.envelope.generated_at_unix_ms = 11_000;
        successor.envelope.activation_unix_ms = 11_000;
        successor.rules[0].emit_every_ms += 5_000;
        let evidence = RuntimeAdaptationEvidence {
            plan_id: current.envelope.plan_id,
            plan_version: current.envelope.plan_version,
            materialization: current.rules[0].materialization,
            producer_id: current.rules[0].producer_id.clone(),
            schema_id: current.rules[0].schema_id.clone(),
            producer_version: "collector.v1".into(),
            observed_at_unix_ms: 10_900,
            sample_count: 100,
        };
        current
            .authorize_successor(&successor, std::slice::from_ref(&evidence), 11_000)
            .expect("bounded change with exact fresh evidence");

        let mut oversized = successor.clone();
        oversized.rules[0].emit_every_ms += 20_000;
        assert!(matches!(
            current.authorize_successor(&oversized, std::slice::from_ref(&evidence), 11_000),
            Err(TransmissionPlanError::AdaptationOutOfBounds { .. })
        ));
        let stale = RuntimeAdaptationEvidence {
            observed_at_unix_ms: 1,
            ..evidence.clone()
        };
        assert!(matches!(
            current.authorize_successor(&successor, &[stale], 11_000),
            Err(TransmissionPlanError::InvalidAdaptationEvidence(_))
        ));
        let future = RuntimeAdaptationEvidence {
            observed_at_unix_ms: 11_001,
            ..evidence
        };
        assert!(matches!(
            current.authorize_successor(&successor, &[future], 11_000),
            Err(TransmissionPlanError::InvalidAdaptationEvidence(_))
        ));
        let mut different_plan_id = successor.clone();
        different_plan_id.envelope.plan_id += 1;
        assert!(matches!(
            current.authorize_successor(&different_plan_id, &[], 11_000),
            Err(TransmissionPlanError::InvalidSuccessor(_))
        ));
        let mut in_place = successor;
        in_place.envelope.plan_version = current.envelope.plan_version;
        assert!(matches!(
            current.authorize_successor(&in_place, &[], 11_000),
            Err(TransmissionPlanError::InvalidSuccessor(_))
        ));
    }
}
