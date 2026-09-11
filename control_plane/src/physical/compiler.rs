//! Backend-owned physical compilation over ASAPPlanner's selected post-ASAP IR.
//!
//! Planner owns semantic alternatives and guarantees. This module owns the
//! deployment decision: evidence freshness, target capabilities, windows, the
//! Collector execution projection, SummaryCatalog, and executable plans.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use asap_aware_mapping::cost_model::Cost;
use asap_aware_mapping::{
    plan_summary_maintenance_lifecycles, AccuracyEvidenceProvider, CostRate, DefaultAccuracyModel,
    EqualSplitAllocator, Horizon, PropagationStats, SummaryMaintenanceCapabilities,
    SummaryMaintenanceLifecycleCapabilities, SummaryMaintenanceLifecycleCostInputs, WorkloadDemand,
};
use planner_types::post_asap::{
    CompositionOperator, EvaluationSchedule, ExecutableDagCompilation, OutputRepresentation,
    PostAsapNodeId, SketchAlgorithm, SketchParams, SketchQuery, SummaryExpr, SummaryFamilyType,
    SummaryMaintenanceLifecycle, SummaryMaintenanceMode, SummaryNode, SummaryWindowFramework,
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

use crate::physical::colored_dag::emitter::{AggregationInput, BackendAggregation};
use crate::physical::post_asap::cost_model::{ControlPlaneCostModel, ExactCompositionCostEvidence};
use crate::query_plan::{
    canonical_promql, FallbackPolicy, InstantExecution, MaterializationBinding, PhysicalGrouping,
    QueryPlan, QueryPlanEntry,
};
use crate::types_v2::AccuracyTarget;
use planner_types::pre_asap::Source;

pub const PLANNER_REVISION: &str = env!("ASAPPLANNER_REVISION");
pub const BACKEND_REVISION: &str = env!("ASAPQUERY_BACKEND_REVISION");
pub const BACKEND_COMPAT: &str = "asap-query-backend.v1";
/// Matches the data plane's default persistence memory limit. A backend-local
/// summary candidate must fit its complete retained state inside this budget.
pub const DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn default_retained_summary_memory_budget_bytes() -> u64 {
    DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES
}

fn is_default_retained_summary_memory_budget_bytes(value: &u64) -> bool {
    *value == DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES
}

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
    /// Measured build plus incremental-update CPU over `horizon_seconds`.
    pub cpu_cost: f64,
    pub peak_memory_bytes: u64,
    pub network_bytes: u64,
    pub storage_bytes: u64,
    pub source_scan_bytes: u64,
    /// Dimensionally calibrated scalar passed to Planner for comparison. The
    /// evidence producer must price update CPU, query-time merges at the
    /// workload's read frequency, retained memory, storage, source scans, and
    /// network traffic. Keeping the components alongside this quote makes the
    /// selected tradeoff auditable without teaching Planner backend units.
    pub weighted_cost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WindowImplementationCandidate {
    /// Backend-owned identity; never copied into Planner IR.
    pub implementation_id: String,
    pub framework: SummaryWindowFramework,
    pub window_secs: u64,
    pub slide_secs: u64,
    pub layout: asap_types::WindowMaterializationLayout,
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
    /// Enable a composable DAG with SummaryStore materializations and Prometheus exact subtrees.
    pub hybrid_execution: bool,
    /// Allowed materialization leaf contracts; None enables every eligible leaf.
    pub materialization_policy: Option<BTreeSet<String>>,
    /// Original dashboard demand, in the same order as queries. None is legacy input.
    pub query_workload: Option<QueryWorkload>,
    pub queries: Vec<PlanningQuery>,
    pub evidence: HashMap<String, TopKMembershipEvidence>,
    /// Fresh measured costs for Planner exact/summary composition sites,
    /// scoped to query IDs just like accuracy evidence.
    pub exact_composition_costs: HashMap<String, Vec<ExactCompositionCostEvidence>>,
    /// Optional distribution-conditioned empirical sizing policy. Hybrid
    /// mode falls back to theoretical sizing and then exact execution.
    pub erp: Option<super::erp::ErpPlanningInput>,
    pub planner_revision: String,
    /// Observed cadence of source samples. Exact temporal panes must divide
    /// both this cadence and the repeated-query evaluation interval.
    pub source_sample_interval_ms: Option<u64>,
    /// How far behind the newest ingested sample an admitted query may be
    /// evaluated. This extends physical retention only; it never changes the
    /// PromQL range selector used for readout.
    pub query_staleness_margin_ms: u64,
    /// Maximum aggregate encoded footprint of every retained summary pane.
    /// `None` uses the backend's default persistence-memory limit.
    pub retained_summary_memory_budget_bytes: Option<u64>,
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
    /// Provider-priced concrete pane choices keyed by the registered PromQL text.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub window_candidates: HashMap<String, Vec<WindowImplementationCandidate>>,
    pub lifecycle_costs: LifecycleCostEvidence,
    pub evidence_observed_at_unix_ms: u64,
    pub evidence_valid_for_ms: u64,
    pub horizon_seconds: f64,
    pub window_implementation_id: String,
    pub implementation_cost: ImplementationCostEvidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sample_interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "u64_is_zero")]
    pub query_staleness_margin_ms: u64,
    /// Admission budget for all retained panes and estimated partitions.
    /// Missing legacy snapshots inherit the backend default.
    #[serde(
        default = "default_retained_summary_memory_budget_bytes",
        alias = "maxRetainedSummaryBytes",
        skip_serializing_if = "is_default_retained_summary_memory_budget_bytes"
    )]
    pub max_retained_summary_bytes: u64,
    /// Certificates keyed by exact registered PromQL; converted to root IDs
    /// before workload selection so one query cannot borrow another's evidence.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub topk_evidence: HashMap<String, TopKMembershipEvidence>,
    /// Measured exact/summary composition profiles keyed by registered
    /// PromQL. Missing rows keep the corresponding Planner site opaque.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub exact_composition_costs: HashMap<String, Vec<ExactCompositionCostEvidence>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub erp: Option<super::erp::ErpPlanningInput>,
}

fn u64_is_zero(value: &u64) -> bool {
    *value == 0
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
    pub materialization: asap_types::sds::SummaryDefinitionId,
    pub metric: String,
    pub algorithm: String,
    pub parameters: Value,
    pub group_by: Vec<String>,
    pub window_secs: u64,
    pub abstract_window_framework: SummaryWindowFramework,
    pub window_implementation_id: String,
    pub slide_secs: u64,
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,
    pub window_layout: asap_types::WindowMaterializationLayout,
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
    /// Absent only in legacy artifacts; catalog-aware validation requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<asap_types::sds::CatalogGeneration>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<asap_types::sds::CatalogGeneration>,
    pub envelope: PlanEnvelope,
    pub ingest: IngestContract,
    pub schemas: Vec<StateSchemaContract>,
    pub producers: Vec<ProducerContract>,
    pub materializations: Vec<asap_types::PrecomputeMaterialization>,
    /// Planner semantic DAGs and backend-owned placement for this generation.
    /// Empty only for legacy/config-only construction paths.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub executable_dags:
        BTreeMap<String, crate::physical::executable_binding::InstalledPostAsapDag>,
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
    #[serde(alias = "require_materialization_identity")]
    pub require_summary_definition_identity: bool,
    pub require_registered_producer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StateEncoding {
    SketchlibProtobufV1,
    SketchCoreMsgpackV1,
    ExactAccumulatorV1,
    ExactCounterAccumulatorV2,
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
    IRate,
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
                    ExactKind::IRate => ExactStateKind::IRate,
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
    pub materialization: asap_types::sds::SummaryDefinitionId,
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
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,
}

/// A collector authorized to produce state for one materialization. Runtime
/// producer epochs and frame sequences belong to TransmissionPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ProducerContract {
    pub producer_id: String,
    pub collector_id: String,
    pub materialization: asap_types::sds::SummaryDefinitionId,
    pub schema_id: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PrecomputePlanError {
    #[error("invalid precompute catalog contract: {0}")]
    CatalogContract(String),
    #[error("PrecomputePlan envelope does not match its SummaryCatalog identity/lifecycle")]
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
    #[error("materialization {materialization} has an invalid window layout: {reason}")]
    InvalidWindowLayout {
        materialization: u64,
        reason: String,
    },
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
        producer_ids: &[String],
    ) -> Result<Self, PrecomputePlanError> {
        let schemas = materializations
            .iter()
            .map(|materialization| {
                let fingerprint = materialization.policy_fingerprint();
                let accumulator = materialization
                    .accumulator_spec()
                    .map_err(|_| PrecomputePlanError::UnsupportedFamily(fingerprint.0))?;
                let family = StateFamilyContract::try_from(&accumulator.family)
                    .map_err(|_| PrecomputePlanError::UnsupportedFamily(fingerprint.0))?;
                let source = materialization.table_name.as_ref().map_or_else(
                    || Source::TimeSeries {
                        metric: materialization.metric.clone(),
                    },
                    |table_ref| Source::Table {
                        table_ref: table_ref.clone(),
                    },
                );
                let value_column = materialization
                    .value_column
                    .clone()
                    .map(planner_types::pre_asap::ColumnRef::Named)
                    .unwrap_or(planner_types::pre_asap::ColumnRef::SampleValue);
                Ok(StateSchemaContract {
                    schema_id: state_schema_id(fingerprint),
                    schema_version: 1,
                    materialization: fingerprint.into(),
                    family,
                    source,
                    value_column,
                    group_by: materialization.grouping_labels.labels.clone(),
                    window: StateWindowContract {
                        kind: materialization.window_type,
                        size_ms: materialization.window_size.saturating_mul(1_000),
                        slide_ms: match materialization.window_type {
                            asap_types::WindowKind::Tumbling => None,
                            asap_types::WindowKind::Sliding => {
                                Some(materialization.slide_interval.saturating_mul(1_000))
                            }
                            asap_types::WindowKind::Session => None,
                        },
                        pane_origin_ms: materialization.pane_origin_ms,
                    },
                    encodings: state_encodings(&accumulator.family),
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
            summary_catalog: None,
            envelope,
            ingest: IngestContract {
                protocol: IngestProtocol::ModifiedOtlpMetricsV1,
                endpoint_path: "/v1/metrics".into(),
                timestamp_unit: TimestampUnit::UnixNanoseconds,
                require_plan_identity: true,
                require_summary_definition_identity: true,
                require_registered_producer: true,
            },
            schemas,
            producers,
            materializations,
            executable_dags: BTreeMap::new(),
        };
        plan.validate()?;
        Ok(plan)
    }

    /// Build the backend-local projection used when raw Prometheus samples
    /// are precomputed inside ASAPQuery rather than by ASAPCollector.
    pub fn build_backend_local(
        envelope: PlanEnvelope,
        materializations: Vec<asap_types::PrecomputeMaterialization>,
    ) -> Result<Self, PrecomputePlanError> {
        let mut plan = Self::build(envelope, materializations, &["backend-local".into()])?;
        plan.ingest = IngestContract {
            protocol: IngestProtocol::PrometheusRemoteWriteV1,
            endpoint_path: "/api/v1/write".into(),
            timestamp_unit: TimestampUnit::UnixMilliseconds,
            require_plan_identity: false,
            require_summary_definition_identity: false,
            require_registered_producer: false,
        };
        plan.producers.clear();
        plan.validate()?;
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
                    && self.ingest.require_summary_definition_identity
                    && self.ingest.require_registered_producer
            }
            IngestProtocol::PrometheusRemoteWriteV1 => {
                self.ingest.endpoint_path == "/api/v1/write"
                    && self.ingest.timestamp_unit == TimestampUnit::UnixMilliseconds
                    && !self.ingest.require_plan_identity
                    && !self.ingest.require_summary_definition_identity
                    && !self.ingest.require_registered_producer
            }
        };
        if !valid_ingest {
            return Err(PrecomputePlanError::UnsupportedIngestEndpoint);
        }
        for (query_id, installed) in &self.executable_dags {
            if query_id != &installed.document.query_id {
                return Err(PrecomputePlanError::CatalogContract(
                    "post-ASAP DAG map key differs from document query ID".into(),
                ));
            }
            installed
                .validate()
                .map_err(PrecomputePlanError::CatalogContract)?;
        }
        let mut materializations = BTreeSet::new();
        for materialization in &self.materializations {
            materialization
                .window_layout
                .validate(materialization.window_size, materialization.slide_interval)
                .map_err(|reason| PrecomputePlanError::InvalidWindowLayout {
                    materialization: materialization.policy_fp_u64(),
                    reason,
                })?;
            let expected_kind = if materialization.slide_interval == materialization.window_size {
                asap_types::WindowKind::Tumbling
            } else {
                asap_types::WindowKind::Sliding
            };
            if materialization.window_type != expected_kind {
                return Err(PrecomputePlanError::InvalidWindowLayout {
                    materialization: materialization.policy_fp_u64(),
                    reason: "window kind disagrees with size and slide".into(),
                });
            }
            if !materializations.insert(materialization.policy_fingerprint().into()) {
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
        for schema in &self.schemas {
            let materialization = self
                .materializations
                .iter()
                .find(|candidate| {
                    candidate.policy_fingerprint() == schema.materialization.fingerprint()
                })
                .ok_or(PrecomputePlanError::SchemaSetMismatch)?;
            let accumulator = materialization.accumulator_spec().map_err(|_| {
                PrecomputePlanError::UnsupportedFamily(schema.materialization.as_u64())
            })?;
            let family = StateFamilyContract::try_from(&accumulator.family).map_err(|_| {
                PrecomputePlanError::UnsupportedFamily(schema.materialization.as_u64())
            })?;
            let source = materialization.table_name.as_ref().map_or_else(
                || Source::TimeSeries {
                    metric: materialization.metric.clone(),
                },
                |table_ref| Source::Table {
                    table_ref: table_ref.clone(),
                },
            );
            let value_column = materialization
                .value_column
                .clone()
                .map(planner_types::pre_asap::ColumnRef::Named)
                .unwrap_or(planner_types::pre_asap::ColumnRef::SampleValue);
            if schema.schema_id != state_schema_id(schema.materialization.fingerprint())
                || schema.family != family
                || schema.source != source
                || schema.value_column != value_column
                || schema.group_by != materialization.grouping_labels.labels
                || schema.window.kind != materialization.window_type
                || schema.window.size_ms != materialization.window_size.saturating_mul(1_000)
                || schema.window.slide_ms
                    != match materialization.window_type {
                        asap_types::WindowKind::Tumbling => None,
                        asap_types::WindowKind::Sliding => {
                            Some(materialization.slide_interval.saturating_mul(1_000))
                        }
                        asap_types::WindowKind::Session => None,
                    }
                || schema.window.pane_origin_ms != materialization.pane_origin_ms
                || schema.encodings != state_encodings(&accumulator.family)
            {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
            }
        }
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
                return Err(PrecomputePlanError::MissingProducer(missing.as_u64()));
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
    pub summary_catalog: super::summary_catalog::SummaryCatalog,
    pub collector_plans: Vec<CollectorPlan>,
    pub precompute_plan: PrecomputePlan,
    pub transmission_plan: TransmissionPlan,
    pub query_plan: QueryPlan,
    /// Lifecycle component only, not a complete physical-plan comparison.
    pub lifecycle_estimates: Vec<MaterializationLifecycleEstimate>,
    pub cost_comparison: Option<super::workload_cost::WorkloadCostComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaterializationLifecycleEstimate {
    pub materialization: asap_types::sds::SummaryDefinitionId,
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
    pub materialization: asap_types::sds::SummaryDefinitionId,
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
    pub materialization: asap_types::sds::SummaryDefinitionId,
    pub producer_id: String,
    pub schema_id: String,
    pub producer_version: String,
    pub observed_at_unix_ms: u64,
    pub sample_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransmissionPlan {
    /// Absent only in legacy artifacts; catalog-aware validation requires it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<asap_types::sds::CatalogGeneration>,
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
    pub materialization: asap_types::sds::SummaryDefinitionId,
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
    #[error("summary catalog mismatch: {0}")]
    Catalog(String),
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

fn validate_catalog_projection(
    reference: Option<&asap_types::sds::CatalogGeneration>,
    envelope: &PlanEnvelope,
    materializations: impl IntoIterator<Item = asap_types::sds::SummaryDefinitionId>,
    catalog: &super::summary_catalog::SummaryCatalog,
) -> Result<(), TransmissionPlanError> {
    let expected = catalog
        .reference()
        .map_err(|error| TransmissionPlanError::Catalog(error.to_string()))?;
    if reference != Some(&expected)
        || envelope.plan_id != catalog.plan_id
        || envelope.plan_version != catalog.plan_version
    {
        return Err(TransmissionPlanError::Catalog(
            "missing or different snapshot reference".into(),
        ));
    }
    for id in materializations {
        if !catalog.materializations.contains_key(&id) {
            return Err(TransmissionPlanError::Catalog(format!(
                "unknown materialization {}",
                id.as_u64()
            )));
        }
    }
    Ok(())
}

impl CollectorPlan {
    pub fn validate_against_catalog(
        &self,
        catalog: &super::summary_catalog::SummaryCatalog,
    ) -> Result<(), TransmissionPlanError> {
        validate_catalog_projection(
            self.summary_catalog.as_ref(),
            &self.envelope,
            self.materializations
                .iter()
                .map(|m| m.materialization)
                .chain(self.transmission_rules.iter().map(|r| r.materialization)),
            catalog,
        )
    }
}

impl TransmissionPlan {
    pub fn validate_against_catalog(
        &self,
        catalog: &super::summary_catalog::SummaryCatalog,
    ) -> Result<(), TransmissionPlanError> {
        validate_catalog_projection(
            self.summary_catalog.as_ref(),
            &self.envelope,
            self.rules.iter().map(|r| r.materialization),
            catalog,
        )
    }

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
                    .find(|m| m.policy_fingerprint() == producer.materialization.fingerprint())
                    .expect("validated PrecomputePlan materialization binding");
                let runtime_policy = runtime_policies
                    .get(&producer.materialization.fingerprint())
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
            summary_catalog: precompute.summary_catalog.clone(),
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
        if self.summary_catalog != precompute.summary_catalog {
            return Err(TransmissionPlanError::Catalog(
                "transmission and precompute plans reference different catalog snapshots".into(),
            ));
        }
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
    #[error("failed to construct a physical materialization: {0}")]
    Materialization(#[from] anyhow::Error),
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
            None => {
                // Unquoted v1 startup snapshots keep the established summary/native
                // compatibility policy. Local residual candidates are enumerated by
                // planning_request and admitted through measured workload selection.
                let mut request = request;
                request.hybrid_execution = false;
                PhysicalCompiler.compile(request, environment)
            }
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
        for query in self.implementation.window_candidates.keys() {
            if !entries.iter().any(|entry| &entry.query.0 == query) {
                return Err(CompileError::Snapshot(format!(
                    "window candidates reference unregistered query `{query}`"
                )));
            }
        }
        let mut queries = Vec::with_capacity(entries.len());
        let mut canonical_roots = Vec::with_capacity(entries.len());
        let mut topk_evidence_by_id = HashMap::new();
        for (index, entry) in entries.into_iter().enumerate() {
            let evaluation_interval_ms = match entry.recurrence {
                QueryRecurrence::Repeated(RepeatedDemand::FixedIntervalAt {
                    interval, ..
                }) => interval.0,
                _ => {
                    return Err(CompileError::Snapshot(format!(
                        "query {index} must use phase-bearing fixed-interval repeated demand in the MVP profile"
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
            let source_metrics = super::workload_cost::exact_source_metrics(&parsed)?;
            let source_hint = source_metrics.iter().next().cloned().ok_or_else(|| {
                CompileError::Snapshot(format!("query {index} has no named time-series source"))
            })?;
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
                query_string: query_string.clone(),
                post_asap,
                source: Source::TimeSeries {
                    metric: source_hint,
                },
                window_secs: lookback_ms / 1_000,
                group_by: metadata.group_by_labels,
                accuracy,
                lifecycle,
                window_implementations: self
                    .implementation
                    .window_candidates
                    .get(&query_string)
                    .cloned()
                    .unwrap_or_else(|| {
                        vec![WindowImplementationCandidate {
                            implementation_id: self.implementation.window_implementation_id.clone(),
                            framework: SummaryWindowFramework::Tumbling,
                            window_secs: lookback_ms / 1_000,
                            slide_secs: lookback_ms / 1_000,
                            layout: asap_types::WindowMaterializationLayout::Pane {
                                pane_secs: lookback_ms / 1_000,
                            },
                            cost,
                        }]
                    }),
                runtime_policy: RuntimeRulePolicy::default(),
            });
        }
        let mut exact_costs_by_id = HashMap::new();
        for (index, entry) in workload.entries().enumerate() {
            if let Some(rows) = self
                .implementation
                .exact_composition_costs
                .get(&entry.query.0)
            {
                for row in rows {
                    row.validate(
                        self.environment.observed_at_unix_ms,
                        self.environment.max_evidence_age_ms,
                    )
                    .map_err(|reason| {
                        CompileError::Snapshot(format!(
                            "query {index}: invalid exact-composition evidence: {reason}"
                        ))
                    })?;
                }
                exact_costs_by_id.insert(format!("compat-query-{index}"), rows.clone());
            }
        }
        select_workload_roots_with_erp(
            &mut queries,
            canonical_roots,
            &topk_evidence_by_id,
            &exact_costs_by_id,
            self.implementation.erp.as_ref(),
        )?;
        // Composable lowering residualizes unsafe leaves individually; retain Planner siblings.
        Ok((
            PlanningRequest {
                hybrid_execution: true,
                materialization_policy: None,
                query_workload: Some(workload),
                queries,
                evidence: topk_evidence_by_id,
                exact_composition_costs: exact_costs_by_id,
                erp: self.implementation.erp,
                planner_revision: PLANNER_REVISION.into(),
                source_sample_interval_ms: self.implementation.source_sample_interval_ms,
                query_staleness_margin_ms: self.implementation.query_staleness_margin_ms,
                retained_summary_memory_budget_bytes: Some(
                    self.implementation.max_retained_summary_bytes,
                ),
            },
            self.environment,
        ))
    }
}

/// Raw accumulators do not retain arbitrary source labels. Preserve native semantics
/// unless the selected DAG explicitly authorizes pooling the source entities.
fn has_unsafe_raw_entity_leaf(
    node: &Rc<SummaryNode>,
    selected: &[Rc<SummaryNode>],
    pooling: bool,
) -> bool {
    use planner_types::post_asap::ExactKind;
    use planner_types::pre_asap::Reduction;
    match &node.expr {
        SummaryExpr::SummaryAgg {
            child,
            reduction,
            family,
            ..
        } => {
            if selected.iter().any(|selected| Rc::ptr_eq(selected, node)) {
                let preserves_series_state = matches!(
                    family,
                    SummaryFamilyType::ExactAggregate(
                        ExactKind::Increase | ExactKind::Rate | ExactKind::MinMax,
                        _
                    )
                );
                return matches!(reduction, Reduction::PerEntity)
                    && !pooling
                    && !preserves_series_state;
            }
            let additive_reduction = matches!(reduction, Reduction::Reduce(_))
                && matches!(family, SummaryFamilyType::ExactAggregate(ExactKind::Sum, _))
                && matches!(
                    &child.expr,
                    SummaryExpr::SummaryAgg {
                        family: SummaryFamilyType::ExactAggregate(
                            ExactKind::Sum | ExactKind::Count,
                            _
                        ),
                        ..
                    }
                );
            has_unsafe_raw_entity_leaf(child, selected, additive_reduction)
        }
        SummaryExpr::BinaryOp { lhs, rhs, .. } => {
            has_unsafe_raw_entity_leaf(lhs, selected, false)
                || has_unsafe_raw_entity_leaf(rhs, selected, false)
        }
        SummaryExpr::SummaryEstimate { summary_input, .. } => {
            has_unsafe_raw_entity_leaf(summary_input, selected, false)
        }
        SummaryExpr::SummaryMerge { children } => children
            .iter()
            .any(|child| has_unsafe_raw_entity_leaf(child, selected, false)),
        _ => false,
    }
}

/// Preserve native execution for raw states that cannot preserve source semantics.
fn preserve_native_unsafe_raw_roots(queries: &mut [PlanningQuery]) -> Result<(), CompileError> {
    for query in queries {
        let selected =
            collect_selected_materializations(&query.post_asap, false).map_err(|reason| {
                CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                }
            })?;
        if selected.is_empty() {
            continue;
        }
        let selected_nodes = selected
            .iter()
            .map(|state| Rc::clone(&state.node))
            .collect::<Vec<_>>();
        let unsafe_entities = has_unsafe_raw_entity_leaf(&query.post_asap, &selected_nodes, false);
        if unsafe_entities {
            let parsed = crate::query_parser::parse_query_expr_canonical(
                &query.query_string,
                query.accuracy.clone(),
            )
            .map_err(|error| CompileError::Query {
                query_id: query.query_id.clone(),
                reason: error.to_string(),
            })?;
            query.post_asap =
                crate::planner_selection::keep_pre_asap(&parsed).map_err(|error| {
                    CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: error.to_string(),
                    }
                })?;
        }
    }
    Ok(())
}

/// Canonicalize Planner candidates that have no backend-maintained state at
/// the physical compiler boundary. Their selected post-ASAP shape may be
/// intentionally unsupported (and therefore invalid as an executable
/// maintenance DAG), but the original query remains a valid exact plan.
fn preserve_invalid_exact_fallback_roots(
    queries: &mut [PlanningQuery],
    composable: bool,
) -> Result<(), CompileError> {
    for query in queries {
        let selected =
            collect_selected_materializations(&query.post_asap, composable).map_err(|reason| {
                CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                }
            })?;
        let invalid_executable =
            selected.is_empty() && validate_executable_subdag(&query.post_asap).is_err();
        if invalid_executable && !matches!(query.post_asap.expr, SummaryExpr::KeepPreAsap(_)) {
            let parsed = crate::query_parser::parse_query_expr_canonical(
                &query.query_string,
                query.accuracy.clone(),
            )
            .map_err(|error| CompileError::Query {
                query_id: query.query_id.clone(),
                reason: error.to_string(),
            })?;
            query.post_asap =
                crate::planner_selection::keep_pre_asap(&parsed).map_err(|error| {
                    CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: error.to_string(),
                    }
                })?;
        }
    }
    Ok(())
}

impl PhysicalCompiler {
    pub fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<PhysicalPlan, CompileError> {
        self.compile_language(request, environment, false)
    }

    pub fn compile_metricsql(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<PhysicalPlan, CompileError> {
        self.compile_language(request, environment, true)
    }

    fn compile_language(
        &self,
        mut request: PlanningRequest,
        environment: DeploymentEnvironment,
        metricsql: bool,
    ) -> Result<PhysicalPlan, CompileError> {
        if request.hybrid_execution
            && environment.target != PhysicalDeploymentTarget::BackendLocalRemoteWrite
        {
            return Err(CompileError::Snapshot(
                "hybrid execution requires backend-local deployment".into(),
            ));
        }
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
        let query_ids = request
            .queries
            .iter()
            .map(|query| query.query_id.as_str())
            .collect::<BTreeSet<_>>();
        if query_ids.len() != request.queries.len() {
            return Err(CompileError::Snapshot(
                "planning query IDs must be unique within a plan generation".into(),
            ));
        }

        if let Some(workload) = &request.query_workload {
            let entries = workload.entries().collect::<Vec<_>>();
            if entries.len() != request.queries.len()
                || entries
                    .iter()
                    .zip(&request.queries)
                    .any(|(entry, query)| entry.query.0 != query.query_string)
            {
                return Err(CompileError::Snapshot("original workload and planning queries must have identical order and query text".into()));
            }
        }

        preserve_invalid_exact_fallback_roots(&mut request.queries, request.hybrid_execution)?;

        if environment.target == PhysicalDeploymentTarget::BackendLocalRemoteWrite
            && !request.hybrid_execution
        {
            preserve_native_unsafe_raw_roots(&mut request.queries)?;
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
        let mut compiled_materializations = Vec::with_capacity(request.queries.len());
        let mut collector_materializations = Vec::with_capacity(request.queries.len());
        let mut plan_materializations = Vec::with_capacity(request.queries.len());
        let mut shared_materializations = BTreeMap::new();
        let mut runtime_policies = BTreeMap::new();
        // Binding is established while compiling the physical
        // materializations, then consumed by QueryPlan lowering. The key is
        // the planner DAG node identity across the workload; serving never scans
        // downstream components to rediscover this decision.
        let mut executable_dags = vec![None::<ExecutableDagCompilation>; request.queries.len()];
        let mut node_bindings =
            BTreeMap::<(usize, PostAsapNodeId), asap_types::PolicyFingerprint>::new();
        let mut query_node_bindings =
            BTreeMap::<(usize, PostAsapNodeId), crate::query_plan::QueryNodeId>::new();
        let consumers = materialization_consumers(
            &request.queries,
            environment.target,
            request.hybrid_execution,
        )?;
        let mut lifecycle_estimates =
            BTreeMap::<asap_types::PolicyFingerprint, MaterializationLifecycleEstimate>::new();

        for (query_index, query) in request.queries.iter().enumerate() {
            let evidence = request.evidence.get(&query.query_id);
            if let Some(e) = evidence {
                validate_evidence(&query.query_id, e, &environment)?;
            }
            let node = query.post_asap.clone();
            let selected = collect_selected_materializations(&node, request.hybrid_execution)
                .map_err(|reason| CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                })?;
            let selected = selected
                .into_iter()
                .filter(|state| {
                    (!request.hybrid_execution
                        || state.window_secs.is_none_or(|window| {
                            query
                                .window_implementations
                                .iter()
                                .any(|candidate| candidate.window_secs == window)
                        }))
                        && (!matches!(
                            state.family,
                            SummaryFamilyType::ExactAggregate(
                                planner_types::post_asap::ExactKind::MinMax,
                                _
                            )
                        ) || crate::query_plan::logical::selected_range_max_materialization(
                            &query.query_string,
                            &state.node,
                        )
                        .ok()
                        .flatten()
                        .is_some())
                        && request.materialization_policy.as_ref().is_none_or(|policy| {
                            let key = crate::query_plan::logical::selected_counter_materialization(
                                &query.query_string,
                                &state.node,
                            )
                            .ok()
                            .flatten()
                            .or_else(|| {
                                crate::query_plan::logical::selected_range_max_materialization(
                                    &query.query_string,
                                    &state.node,
                                )
                                .ok()
                                .flatten()
                            });
                            key.is_some_and(|key| policy.contains(&key))
                        })
                })
                .collect::<Vec<_>>();
            // An exact native fallback has no maintained state and must not
            // depend on evidence for unused window/state implementations.
            if selected.is_empty() {
                continue;
            }
            let executable =
                planner_types::post_asap::compile_executable_dag_with_node_ids(&query.post_asap)
                    .map_err(|error| CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: format!("invalid executable subDAG: {error}"),
                    })?;
            executable_dags[query_index] = Some(executable);
            validate_lifecycle_input(&query.query_id, &query.lifecycle)?;
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
                    reason: "observation-count readout requires the backend precompute producer"
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
                let mut branch_query = query.clone();
                branch_query.window_secs = selected.window_secs.unwrap_or(query.window_secs);
                branch_query.group_by = selected
                    .group_by
                    .clone()
                    .unwrap_or_else(|| query.group_by.clone());
                branch_query
                    .window_implementations
                    .retain(|candidate| candidate.window_secs == branch_query.window_secs);
                let query = &branch_query;
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
                            delete: environment.target
                                == PhysicalDeploymentTarget::BackendLocalRemoteWrite,
                        },
                    )
                    .with_window_implementation_costs(window_costs);
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
                let mut aggregation = physical_aggregation(
                    query,
                    &selected,
                    aggregation_id.clone(),
                    environment.target,
                );
                let precompute_materialization =
                    aggregation_config_for_materialization(&aggregation)?;
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
                    request.query_workload.as_ref().map(|workload| {
                        (
                            workload,
                            consumers[&materialization].iter().copied().collect(),
                        )
                    }),
                )?;
                let window_implementation = query.window_implementations.iter()
                    .find(|candidate| candidate.implementation_id == planner_selection.window_implementation_id
                        && candidate.framework == planner_selection.window_framework)
                    .ok_or_else(|| CompileError::Lifecycle {
                        query_id: query.query_id.clone(),
                        reason: "Planner selected a window framework without a retained concrete implementation".into(),
                    })?;
                // The logical consumer group is identified above. The installed state
                // identity includes the actual pane width selected by Planner.
                // Preserve semantic window and evaluation cadence independently
                // from the selected storage representation.
                aggregation.window_secs = window_implementation.window_secs;
                let mut runtime_materialization =
                    aggregation_config_for_materialization(&aggregation)?;
                runtime_materialization.window_size = window_implementation.window_secs;
                runtime_materialization.slide_interval = window_implementation.slide_secs;
                runtime_materialization.window_type =
                    if window_implementation.slide_secs == window_implementation.window_secs {
                        asap_types::WindowKind::Tumbling
                    } else {
                        asap_types::WindowKind::Sliding
                    };
                runtime_materialization.window_layout = window_implementation.layout.clone();
                let pane_width_ms = match &window_implementation.layout {
                    asap_types::WindowMaterializationLayout::FullWindow => {
                        window_implementation.slide_secs
                    }
                    layout => layout.base_pane_secs(),
                }
                .saturating_mul(1_000);
                runtime_materialization.pane_origin_ms = shared_pane_origin_ms(
                    request.query_workload.as_ref(),
                    consumers[&materialization].iter().copied(),
                    pane_width_ms,
                )
                .map_err(|reason| CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                })?;
                let materialization = runtime_materialization.policy_fingerprint();
                let consumer_query_ids = state_consumers
                    .iter()
                    .map(|query| query.query_id.clone())
                    .collect::<Vec<_>>();
                // Lifecycle demand was priced before choosing pane width. Two
                // distinct logical cohorts can now collide on one physical
                // fingerprint; retaining either quote would omit consumers.
                if lifecycle_estimates
                    .get(&materialization)
                    .is_some_and(|existing| existing.consumer_query_ids != consumer_query_ids)
                {
                    return Err(CompileError::Lifecycle {
                        query_id: query.query_id.clone(),
                        reason: "selected pane coalesces distinct logical consumer cohorts; joint physical-pane lifecycle evidence is required".into(),
                    });
                }
                lifecycle_estimates
                    .entry(materialization)
                    .or_insert_with(|| MaterializationLifecycleEstimate {
                        materialization: materialization.into(),
                        consumer_query_ids,
                        window_implementation_id: window_implementation.implementation_id.clone(),
                        horizon_seconds: query.lifecycle.horizon_seconds,
                        expected_reads: planner_selection.expected_reads,
                        expected_updates: planner_selection.expected_updates,
                        lifecycle_cost: planner_selection.lifecycle_cost,
                    });
                let node_id = executable_dags[query_index]
                    .as_ref()
                    .expect("selected query has a compiled executable DAG")
                    .node_ids
                    .node_id(&selected.node)
                    .ok_or_else(|| CompileError::Query {
                        query_id: query.query_id.clone(),
                        reason: "selected materialization is absent from the compiled Planner DAG"
                            .into(),
                    })?;
                let binding_key = (query_index, node_id);
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
                let physical_group_by = aggregation.grouping.clone();
                compiled_materializations.push(runtime_materialization.clone());
                let collector_materialization = CollectorMaterialization {
                    query_id: format!("state-{}", materialization.0),
                    materialization: materialization.into(),
                    metric: metric.clone(),
                    algorithm: physical_algorithm,
                    parameters: selected.parameters,
                    group_by: physical_group_by,
                    window_secs: runtime_materialization.window_size,
                    abstract_window_framework: planner_selection.window_framework.clone(),
                    window_implementation_id: window_implementation.implementation_id.clone(),
                    slide_secs: runtime_materialization.slide_interval,
                    pane_origin_ms: runtime_materialization.pane_origin_ms,
                    window_layout: window_implementation.layout.clone(),
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
                            reason: format!(
                                "queries sharing one materialization specify conflicting deployment contracts: existing={existing:?}, requested={shared_contract:?}"
                            ),
                        });
                    }
                } else {
                    shared_materializations.insert(materialization, shared_contract);
                    collector_materializations.push(collector_materialization);
                }
            }
        }

        let plan_id = if request.hybrid_execution {
            use std::hash::{Hash, Hasher};
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            stable_workload_plan_id(&plan_materializations, &request.queries).hash(&mut hash);
            "typed-local-residual-v3-counter-index".hash(&mut hash);
            request.materialization_policy.hash(&mut hash);
            for query in &request.queries {
                format!("{:?}", query.post_asap).hash(&mut hash);
            }
            hash.finish()
        } else {
            stable_workload_plan_id(&plan_materializations, &request.queries)
        };
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
        let producer_ids = match environment.target {
            PhysicalDeploymentTarget::DistributedCollectors => environment.collector_ids.clone(),
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => Vec::new(),
        };
        // Several queries/readouts may intentionally share one maintained
        // summary. PrecomputePlan is keyed by physical identity, not query ID.
        let mut materializations_by_fingerprint = BTreeMap::new();
        for materialization in compiled_materializations {
            materializations_by_fingerprint
                .entry(materialization.policy_fingerprint())
                .or_insert(materialization);
        }
        let materializations = materializations_by_fingerprint.into_values().collect();
        let mut precompute_plan = match environment.target {
            PhysicalDeploymentTarget::DistributedCollectors => {
                PrecomputePlan::build(envelope.clone(), materializations, &producer_ids)
            }
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => {
                PrecomputePlan::build_backend_local(envelope.clone(), materializations)
            }
        }
        .map_err(|error| CompileError::Query {
            query_id: "precompute-plan".into(),
            reason: error.to_string(),
        })?;
        let mut transmission_plan =
            TransmissionPlan::build(envelope.clone(), &precompute_plan, &runtime_policies)
                .map_err(|error| CompileError::Query {
                    query_id: "transmission-plan".into(),
                    reason: error.to_string(),
                })?;
        let mut collector_plans = producer_ids
            .into_iter()
            .map(|collector_id| CollectorPlan {
                summary_catalog: transmission_plan.summary_catalog.clone(),
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
        let materialization_fingerprints: BTreeSet<_> = precompute_plan
            .materializations
            .iter()
            .map(asap_types::PrecomputeMaterialization::policy_fingerprint)
            .collect();
        let mut query_entries = BTreeMap::new();
        for (query_index, query) in request.queries.iter().enumerate() {
            // VictoriaMetrics acceleration accepts only the shared
            // PromQL-compatible parser contract. MetricsQL-only syntax fails
            // closed here and remains eligible for exact VM execution.
            let canonical =
                canonical_promql(&query.query_string).map_err(|error| CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: format!("promql-compatible identity: {error}"),
                })?;
            let binding = |node: &Rc<SummaryNode>, node_family: &SummaryFamilyType| -> Result<MaterializationBinding, crate::query_plan::QueryPlanError> {
                    summary_agg_metric(node).ok_or_else(|| {
                        crate::query_plan::QueryPlanError::Invalid(
                            "materialized node has no unique time-series source".into(),
                        )
                    })?;
                    let node_id = executable_dags[query_index]
                        .as_ref()
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(
                                "materialized query has no compiled executable DAG".into(),
                            )
                        })?
                        .node_ids
                        .node_id(node)
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(
                                "query materialization is absent from the compiled Planner DAG".into(),
                            )
                        })?;
                    let fingerprint = node_bindings
                        .get(&(query_index, node_id))
                        .copied()
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "post-ASAP node has no compiled physical binding for {node_family:?}"
                            ))
                        })?;
                    let materialization = precompute_plan
                        .materializations
                        .iter()
                        .find(|candidate| candidate.policy_fingerprint() == fingerprint)
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "compiled binding {} is absent from PrecomputePlan",
                                fingerprint.0
                            ))
                        })?;
                    let stored_interval_ms = precompute_plan
                        .materializations
                        .iter()
                        .find(|candidate| candidate.policy_fingerprint() == fingerprint)
                        .map(|candidate| match &candidate.window_layout {
                            asap_types::WindowMaterializationLayout::FullWindow => {
                                candidate.window_size
                            }
                            layout => layout.base_pane_secs(),
                        }
                        .saturating_mul(1_000))
                        .ok_or_else(|| {
                            crate::query_plan::QueryPlanError::Invalid(format!(
                                "compiled binding {} has no precompute materialization",
                                fingerprint.0
                            ))
                        })?;
                    let (_, source_window, _) = materialization_leaf_contract(node)
                        .map_err(crate::query_plan::QueryPlanError::Invalid)?;
                    let materialization_family = materialization.accumulator_spec()
                        .map_err(|error| crate::query_plan::QueryPlanError::Invalid(error.to_string()))?
                        .family;
                    let window_ms = materialization.window_size.saturating_mul(1_000);
                    if materialization_family != physical_materialization_family(node_family)
                        || window_ms == 0
                        || source_window.unwrap_or(query.window_secs).saturating_mul(1_000)
                            % window_ms != 0
                    {
                        return Err(crate::query_plan::QueryPlanError::Invalid(format!(
                            "compiled binding {} disagrees with post-ASAP/deployment semantics",
                            fingerprint.0
                        )));
                    }
                    Ok(MaterializationBinding {
                        readout_lookback_ms: source_window.map(|seconds| seconds.saturating_mul(1_000)),
                        materialization: fingerprint.into(),
                        output_grouping: PhysicalGrouping::Reduce(
                            materialization.grouping_labels.labels.clone(),
                        ),
                        item_labels: materialization.aggregated_labels.labels.clone(),
                        window_ms: stored_interval_ms,
                        pane_origin_ms: materialization.pane_origin_ms,
                    })
            };
            let instant = InstantExecution {
                lookback_ms: query.window_secs.saturating_mul(1_000),
                full_history: false,
                cumulative_readout: true,
            };
            let mut entry = if request.hybrid_execution {
                QueryPlanEntry::compile_bound_composable_mapped(
                    query.query_id.clone(),
                    canonical.clone(),
                    &query.post_asap,
                    instant,
                    FallbackPolicy::ExactBackend,
                    binding,
                    |node, query_node| {
                        if let Some(post_asap_node) = executable_dags[query_index]
                            .as_ref()
                            .and_then(|compiled| compiled.node_ids.node_id(node))
                        {
                            query_node_bindings.insert((query_index, post_asap_node), query_node);
                        }
                    },
                )
            } else {
                QueryPlanEntry::compile_bound_mapped(
                    query.query_id.clone(),
                    canonical.clone(),
                    &query.post_asap,
                    instant,
                    FallbackPolicy::ExactBackend,
                    binding,
                    |node, query_node| {
                        if let Some(post_asap_node) = executable_dags[query_index]
                            .as_ref()
                            .and_then(|compiled| compiled.node_ids.node_id(node))
                        {
                            query_node_bindings.insert((query_index, post_asap_node), query_node);
                        }
                    },
                )
            }?;
            if request.hybrid_execution {
                // Any Planner-selected leaf without a physical summary binding
                // is an exact subtree boundary. Deployed plans never retain a
                // backend-local range index leaf.
                crate::query_plan::logical::finalize_residuals(&mut entry)?;
            }
            if metricsql {
                entry.language = crate::query_plan::QueryLanguage::MetricsQl;
            }
            let catalog_key = QueryPlan::catalog_key(entry.language, &canonical);
            if query_entries.insert(catalog_key, entry).is_some() {
                return Err(CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason: format!("duplicate canonical query identity `{canonical}`"),
                });
            }
        }
        let query_plan = QueryPlan {
            plan_id,
            plan_version: envelope.plan_version,
            clickhouse_context: None,
            entries: query_entries,
        };
        for (query_index, compiled) in executable_dags.iter().enumerate() {
            let Some(compiled) = compiled else { continue };
            let query_id = request.queries[query_index].query_id.clone();
            let mut placements = BTreeMap::new();
            let mut precompute_sinks = Vec::new();
            for node in &compiled.dag.nodes {
                let placement =
                    if let Some(definition) = node_bindings.get(&(query_index, node.id)).copied() {
                        precompute_sinks.push(node.id);
                        crate::physical::executable_binding::BackendNodeBinding::Materialization {
                            summary_definition: definition.into(),
                        }
                    } else if node.output_state.timing
                        == planner_types::post_asap::ExecutionTiming::MaintenanceTime
                    {
                        super::executable_binding::BackendNodeBinding::MaintenanceInput
                    } else {
                        match query_node_bindings.get(&(query_index, node.id)).copied() {
                            Some(query_node) => {
                                super::executable_binding::BackendNodeBinding::Query { query_node }
                            }
                            None => super::executable_binding::BackendNodeBinding::QueryInput,
                        }
                    };
                placements.insert(node.id, placement);
            }
            precompute_sinks.sort();
            let installed = crate::physical::executable_binding::InstalledPostAsapDag {
                document: super::executable_binding::PostAsapDagDocument::from_executable(
                    query_id.clone(),
                    &compiled.dag,
                )
                .map_err(|reason| CompileError::Query {
                    query_id: query_id.clone(),
                    reason,
                })?,
                binding: super::executable_binding::BackendExecutableBinding {
                    nodes: placements,
                    query_sink: compiled.dag.root,
                    query_plan_sink: query_plan
                        .entries
                        .values()
                        .find(|entry| entry.query_id == query_id)
                        .expect("compiled query entry exists")
                        .root,
                    precompute_sinks,
                },
            };
            installed.validate().map_err(|reason| CompileError::Query {
                query_id: query_id.clone(),
                reason,
            })?;
            precompute_plan.executable_dags.insert(query_id, installed);
        }
        for materialization in &mut precompute_plan.materializations {
            let fingerprint = materialization.policy_fingerprint();
            let max_lookback_ms = query_plan
                .entries
                .values()
                .flat_map(QueryPlanEntry::materialization_bindings)
                .filter(|binding| binding.materialization.fingerprint() == fingerprint)
                .filter_map(|binding| binding.readout_lookback_ms)
                .max();
            if let Some(lookback_ms) = max_lookback_ms {
                materialization.num_aggregates_to_retain = Some(retained_state_count(
                    lookback_ms,
                    request.query_staleness_margin_ms,
                    materialization.slide_interval.saturating_mul(1_000),
                    &materialization.window_layout,
                ));
            }
        }
        validate_retained_summary_footprint(
            &precompute_plan.materializations,
            request
                .query_workload
                .as_ref()
                .and_then(|workload| workload.data_workload.as_ref())
                .and_then(|data| {
                    data.input_cardinality
                        .value_at(environment.observed_at_unix_ms)
                })
                .copied(),
            request
                .retained_summary_memory_budget_bytes
                .unwrap_or(DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES),
        )?;
        query_plan.validate(&materialization_fingerprints)?;
        for (query_id, installed) in &precompute_plan.executable_dags {
            let entry = query_plan
                .entries
                .values()
                .find(|entry| &entry.query_id == query_id)
                .ok_or_else(|| CompileError::Query {
                    query_id: query_id.clone(),
                    reason: "installed post-ASAP DAG has no query-plan entry".into(),
                })?;
            installed
                .validate_query_plan(entry)
                .map_err(|reason| CompileError::Query {
                    query_id: query_id.clone(),
                    reason,
                })?;
        }
        let summary_catalog = super::summary_catalog::SummaryCatalog::from_materializations(
            envelope.plan_id,
            envelope.plan_version,
            &precompute_plan.materializations,
        )
        .map_err(|error| CompileError::Query {
            query_id: "summary-catalog".into(),
            reason: error.to_string(),
        })?;
        precompute_plan
            .bind_catalog(&summary_catalog)
            .map_err(|error| CompileError::Query {
                query_id: "precompute-catalog".into(),
                reason: error.to_string(),
            })?;
        transmission_plan.summary_catalog = precompute_plan.summary_catalog.clone();
        for collector in &mut collector_plans {
            collector.summary_catalog = precompute_plan.summary_catalog.clone();
        }
        transmission_plan
            .validate_against_catalog(&summary_catalog)
            .map_err(|error| CompileError::Query {
                query_id: "summary-catalog".into(),
                reason: error.to_string(),
            })?;
        for collector in &collector_plans {
            collector
                .validate_against_catalog(&summary_catalog)
                .map_err(|error| CompileError::Query {
                    query_id: "summary-catalog".into(),
                    reason: error.to_string(),
                })?;
        }
        query_plan.validate_against_catalog(&summary_catalog)?;
        Ok(PhysicalPlan {
            envelope,
            summary_catalog,
            collector_plans,
            precompute_plan,
            transmission_plan,
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
            SummaryExpr::CandidateTopK {
                candidates, values, ..
            } => {
                walk(candidates, metrics);
                walk(values, metrics);
            }
            SummaryExpr::ValueOperation { child, .. } => walk(child, metrics),
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
            | SummaryExpr::RelationalJoin { left, right, .. }
            | SummaryExpr::SummarySubtract { left, right }
            | SummaryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            } => {
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

/// Shared selection boundary for canonical startup and compile-and-publish.
/// Certificate-bearing roots stay isolated: equal certificate values do not
/// establish that the certificate's source scope covers another query.
pub fn select_workload_roots(
    queries: &mut [PlanningQuery],
    roots: Vec<Rc<QueryExpr>>,
    evidence: &HashMap<String, TopKMembershipEvidence>,
    exact_costs: &HashMap<String, Vec<ExactCompositionCostEvidence>>,
) -> Result<(), CompileError> {
    select_workload_roots_with_erp(queries, roots, evidence, exact_costs, None)
}

pub fn select_workload_roots_with_erp(
    queries: &mut [PlanningQuery],
    roots: Vec<Rc<QueryExpr>>,
    evidence: &HashMap<String, TopKMembershipEvidence>,
    exact_costs: &HashMap<String, Vec<ExactCompositionCostEvidence>>,
    erp: Option<&super::erp::ErpPlanningInput>,
) -> Result<(), CompileError> {
    if roots.len() != queries.len() {
        return Err(CompileError::Snapshot(
            "canonical root/query mapping is incomplete".into(),
        ));
    }
    let mut cohorts: Vec<(AccuracyTarget, Option<String>, Vec<(usize, Rc<QueryExpr>)>)> =
        Vec::new();
    let original_roots = roots.clone();
    for (index, root) in roots.into_iter().enumerate() {
        let accuracy = &queries[index].accuracy;
        let certificate_scope = (evidence.contains_key(&queries[index].query_id)
            || exact_costs.contains_key(&queries[index].query_id))
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
        // ERP v1 has no calibrated failure probability. Preserve explicit
        // confidence requirements through theoretical/exact fallback.
        let scoped_erp = erp.map(|policy| {
            let mut policy = policy.clone();
            if !matches!(accuracy, AccuracyTarget::Epsilon(_))
                || policy.error_metric != "max_rank_err"
            {
                policy.artifact.records.clear();
            }
            // A benchmark of a different KLL implementation is not evidence
            // for the collector's sketchlib KLL, even with the same k.
            policy
                .artifact
                .records
                .retain(|row| row.sketch == "kll-percall" && row.implementation == "lib");
            policy
        });
        let erp = scoped_erp.as_ref();
        let mut model = ControlPlaneCostModel::new(accuracy.clone()).with_exact_composition_costs(
            scope
                .as_ref()
                .and_then(|id| exact_costs.get(id))
                .cloned()
                .unwrap_or_default(),
        );
        if let Some(erp) = erp {
            model = model.with_erp(erp.clone());
        }
        let certificate = scope.as_ref().and_then(|id| evidence.get(id));
        let accuracy_model = super::erp::ErpAccuracyModel {
            policy: erp,
            max_error: match accuracy {
                AccuracyTarget::Epsilon(e) | AccuracyTarget::EpsilonDelta { epsilon: e, .. } => e,
                AccuracyTarget::Exact => 0.0,
            },
        };
        let selected = crate::planner_selection::select_workload_with_accuracy_model(
            roots,
            accuracy,
            &model,
            &QueryEvidence(certificate),
            &accuracy_model,
        )
        .map_err(|error| CompileError::Snapshot(error.to_string()))?;
        for (index, node) in selected {
            if erp.is_some_and(|policy| {
                requires_exact_erp_fallback(&node, &queries[index].accuracy, policy)
            }) {
                queries[index].post_asap =
                    crate::planner_selection::keep_pre_asap(&original_roots[index])
                        .map_err(|error| CompileError::Snapshot(error.to_string()))?;
            } else {
                queries[index].post_asap = node;
            }
        }
    }
    Ok(())
}

fn requires_exact_erp_fallback(
    node: &SummaryNode,
    accuracy: &AccuracyTarget,
    erp: &super::erp::ErpPlanningInput,
) -> bool {
    fn walk(node: &SummaryNode, out: &mut Vec<(SketchAlgorithm, SketchParams)>) {
        match &node.expr {
            SummaryExpr::SummaryAgg { family, child, .. } => {
                if let SummaryFamilyType::Sketch(kind, _) = family {
                    out.push((kind.algorithm().clone(), kind.params().clone()));
                }
                walk(child, out);
            }
            SummaryExpr::SummaryEstimate { summary_input, .. }
            | SummaryExpr::SummaryDelete { summary_input, .. }
            | SummaryExpr::ValueOperation {
                child: summary_input,
                ..
            } => walk(summary_input, out),
            SummaryExpr::SummaryMerge { children } => {
                children.iter().for_each(|child| walk(child, out))
            }
            SummaryExpr::SummaryJoin { outer, inner, .. } => {
                walk(outer, out);
                walk(inner, out);
            }
            SummaryExpr::SummarySubtract { left, right }
            | SummaryExpr::RelationalJoin { left, right, .. }
            | SummaryExpr::BinaryOp {
                lhs: left,
                rhs: right,
                ..
            } => {
                walk(left, out);
                walk(right, out);
            }
            SummaryExpr::CandidateTopK {
                candidates, values, ..
            } => {
                walk(candidates, out);
                walk(values, out);
            }
            SummaryExpr::KeepPreAsap(_) => {}
        }
    }
    let max_error = match accuracy {
        AccuracyTarget::Epsilon(value) | AccuracyTarget::EpsilonDelta { epsilon: value, .. } => {
            *value
        }
        AccuracyTarget::Exact => return false,
    };
    let mut sketches = Vec::new();
    walk(node, &mut sketches);
    sketches.into_iter().any(|(algorithm, params)| {
        matches!(
            erp.select(algorithm, max_error, params),
            super::erp::ErpParameterDecision::ExactFallback { .. }
        )
    })
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

pub(super) fn state_schema_id(fingerprint: asap_types::PolicyFingerprint) -> String {
    format!("{}:summary-state:v1:{}", BACKEND_COMPAT, fingerprint.0)
}

pub(super) fn state_encodings(family: &SummaryFamilyType) -> Vec<StateEncoding> {
    match family {
        SummaryFamilyType::ExactAggregate(
            planner_types::post_asap::ExactKind::Increase
            | planner_types::post_asap::ExactKind::Rate,
            _,
        ) => vec![StateEncoding::ExactCounterAccumulatorV2],
        SummaryFamilyType::ExactAggregate(..) => vec![StateEncoding::ExactAccumulatorV1],
        SummaryFamilyType::Sketch(kind, _)
            if matches!(
                kind.algorithm(),
                SketchAlgorithm::CmsWithHeap
                    | SketchAlgorithm::CountSketchWithHeap
                    | SketchAlgorithm::UnivMon
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
) -> Result<Vec<(String, SummaryWindowFramework, Cost)>, CompileError> {
    let mut ids = BTreeSet::new();
    let mut candidates = Vec::new();
    for candidate in &query.window_implementations {
        let evidence = &candidate.cost;
        let age = environment
            .observed_at_unix_ms
            .saturating_sub(evidence.observed_at_unix_ms);
        let valid = evidence.observed_at_unix_ms <= environment.observed_at_unix_ms
            && !candidate.implementation_id.trim().is_empty()
            && ids.insert(candidate.implementation_id.clone())
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
            && candidate
                .layout
                .validate(candidate.window_secs, candidate.slide_secs)
                .is_ok()
            && match (&candidate.framework, &candidate.layout) {
                (
                    SummaryWindowFramework::Tumbling | SummaryWindowFramework::Sliding,
                    asap_types::WindowMaterializationLayout::Pane { .. },
                )
                | (
                    SummaryWindowFramework::Sliding,
                    asap_types::WindowMaterializationLayout::FullWindow,
                ) => true,
                (
                    SummaryWindowFramework::Extension(name),
                    asap_types::WindowMaterializationLayout::HierarchicalRollup { .. },
                ) => name == "backend.exact-hierarchical-rollup.v1",
                _ => false,
            }
            && match environment.target {
                PhysicalDeploymentTarget::DistributedCollectors => {
                    matches!(
                        candidate.layout,
                        asap_types::WindowMaterializationLayout::FullWindow
                    ) || matches!(candidate.layout, asap_types::WindowMaterializationLayout::Pane { pane_secs } if pane_secs == candidate.window_secs)
                }
                PhysicalDeploymentTarget::BackendLocalRemoteWrite => true,
            };
        if !valid {
            return Err(CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: format!(
                    "window implementation `{}` has incomplete, stale, incompatible, or duplicate physical evidence",
                    candidate.implementation_id
                ),
            });
        }
        candidates.push((
            candidate.implementation_id.clone(),
            candidate.framework.clone(),
            Cost(evidence.weighted_cost),
        ));
    }
    if candidates.is_empty() {
        return Err(CompileError::Lifecycle {
            query_id: query.query_id.clone(),
            reason: "no complete executor-feasible window implementation evidence".into(),
        });
    }
    Ok(candidates)
}

fn retained_state_count(
    lookback_ms: u64,
    staleness_margin_ms: u64,
    slide_ms: u64,
    layout: &asap_types::WindowMaterializationLayout,
) -> u64 {
    match layout {
        asap_types::WindowMaterializationLayout::FullWindow => staleness_margin_ms
            .div_ceil(slide_ms.max(1))
            .saturating_add(1),
        asap_types::WindowMaterializationLayout::Pane { pane_secs } => lookback_ms
            .saturating_add(staleness_margin_ms)
            .div_ceil(pane_secs.saturating_mul(1_000).max(1))
            .saturating_add(1),
        asap_types::WindowMaterializationLayout::HierarchicalRollup { base_pane_secs, .. } => {
            lookback_ms
                .saturating_add(staleness_margin_ms)
                .div_ceil(base_pane_secs.saturating_mul(1_000).max(1))
                .saturating_add(1)
        }
    }
}

/// Estimate the encoded bytes retained by one physical state. Sketch matrix
/// cells use two words here, covering the counter plus observed serialization
/// overhead. Heap and exact-state estimates include container slack.
fn retained_state_bytes(materialization: &asap_types::PrecomputeMaterialization) -> u128 {
    use asap_types::AggregationType as A;

    let parameter = |names: &[&str], fallback: u64| {
        names
            .iter()
            .find_map(|name| {
                materialization
                    .parameters
                    .get(*name)
                    .and_then(Value::as_u64)
            })
            .unwrap_or(fallback) as u128
    };
    match materialization.aggregation_type {
        A::CountMinSketch | A::CountSketch => {
            parameter(&["width", "w", "col_num", "col"], 1)
                * parameter(&["depth", "d", "row_num", "row"], 1)
                * 16
        }
        A::CountMinSketchWithHeap | A::CountSketchWithHeap => {
            parameter(&["width", "w", "col_num", "col"], 1)
                * parameter(&["depth", "d", "row_num", "row"], 1)
                * 16
                + parameter(&["heap_size"], 1) * 256
        }
        A::DatasketchesKLL => parameter(&["k"], 200) * 32,
        A::UnivMon => {
            parameter(&["layers"], 4)
                * (parameter(&["sketch_rows"], 5) * parameter(&["sketch_cols"], 1024) * 16
                    + parameter(&["heap_size"], 32) * 256)
        }
        A::HydraKLL => parameter(&["k"], 200) * parameter(&["col", "cols"], 1) * 32,
        A::HLL => 1u128 << parameter(&["precision", "p"], 14).min(24),
        A::DDSketch => 64 * 1024,
        A::Sum
        | A::Increase
        | A::MinMax
        | A::MultipleSum
        | A::MultipleIncrease
        | A::MultipleMinMax
        | A::SingleSubpopulation
        | A::MultipleSubpopulation => 256,
    }
}

fn retained_partition_count(
    materialization: &asap_types::PrecomputeMaterialization,
    input_cardinality: Option<u64>,
) -> u128 {
    use asap_types::AggregationType as A;

    // Reset-aware and min/max state remains source-series scoped even with an
    // empty output grouping. Grouped states have at most one partition per
    // input series. Other empty groupings are the Reduce([]) global singleton.
    if matches!(
        materialization.aggregation_type,
        A::Increase | A::MultipleIncrease | A::MinMax | A::MultipleMinMax
    ) || !materialization.grouping_labels.labels.is_empty()
    {
        u128::from(input_cardinality.unwrap_or(1).max(1))
    } else {
        1
    }
}

fn validate_retained_summary_footprint(
    materializations: &[asap_types::PrecomputeMaterialization],
    input_cardinality: Option<u64>,
    budget_bytes: u64,
) -> Result<(), CompileError> {
    let estimated_bytes = materializations
        .iter()
        .fold(0u128, |total, materialization| {
            total.saturating_add(
                retained_state_bytes(materialization)
                    .saturating_mul(u128::from(
                        materialization.num_aggregates_to_retain.unwrap_or(1),
                    ))
                    .saturating_mul(retained_partition_count(materialization, input_cardinality)),
            )
        });
    if estimated_bytes > u128::from(budget_bytes) {
        return Err(CompileError::Query {
            query_id: "retained-summary-footprint".into(),
            reason: format!(
                "estimated retained summary footprint {estimated_bytes} bytes exceeds budget {budget_bytes} bytes across panes and partitions"
            ),
        });
    }
    Ok(())
}

struct PlannerPhysicalSelection {
    window_implementation_id: String,
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
    original_workload: Option<(&QueryWorkload, Vec<usize>)>,
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
                        lookback: Some(DurationMs(
                            materialization_leaf_contract(node)
                                .ok()
                                .and_then(|(_, window, _)| window)
                                .unwrap_or(query.window_secs)
                                .saturating_mul(1_000),
                        )),
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
    let (workload, indices) =
        original_workload.unwrap_or((&workload, (0..consumers.len()).collect()));
    let plan = plan_summary_maintenance_lifecycles(
        Rc::new(node.clone()),
        WorkloadDemand::new(workload, &indices),
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
        window_implementation_id: plan.selected_window_implementation_id.clone().ok_or_else(
            || CompileError::Lifecycle {
                query_id: query.query_id.clone(),
                reason: "Planner returned no concrete window implementation identity".into(),
            },
        )?,
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

/// A warm producer may consume only a source whose semantics its precompute accumulator
/// implements. Predicates and shifted ranges remain executable residual nodes.
pub(crate) fn materialization_leaf_contract(
    node: &SummaryNode,
) -> Result<(String, Option<u64>, String), String> {
    use planner_types::pre_asap::{CompareOpKind, QueryExpr, ScalarValue};
    let SummaryExpr::SummaryAgg { child, .. } = &node.expr else {
        return Err("materialization requires a SummaryAgg leaf".into());
    };
    let SummaryExpr::KeepPreAsap(expr) = &child.expr else {
        return Err("materialization input is not a raw source".into());
    };
    let (source, window_secs) = match expr.as_ref() {
        QueryExpr::TimeRange { child, range } => {
            if range.as_millis() == 0 || range.as_millis() % 1000 != 0 {
                return Err("warm producer requires a positive whole-second range".into());
            }
            (child.as_ref(), Some(range.as_secs()))
        }
        QueryExpr::Scan { .. }
            if matches!(
                &node.expr,
                SummaryExpr::SummaryAgg {
                    family: SummaryFamilyType::ExactAggregate(..),
                    ..
                }
            ) =>
        {
            return Err(
                "instantaneous sample selection is not a temporal accumulator readout".into(),
            );
        }
        source => (source, None),
    };
    match source {
        QueryExpr::Scan {
            source: Source::TimeSeries { metric },
            predicates,
            schema,
        } if !metric.is_empty() => {
            let mut matchers = Vec::with_capacity(predicates.len());
            for predicate in predicates {
                let QueryExpr::Compare { left, op, right } = predicate.0.as_ref() else {
                    return Err("materialization filter is not a label comparison".into());
                };
                let (QueryExpr::Column(column), QueryExpr::Literal(ScalarValue::Utf8(value))) =
                    (left.as_ref(), right.as_ref())
                else {
                    return Err("materialization filter must compare a label with a string".into());
                };
                let label = schema
                    .columns
                    .get(*column)
                    .map(|field| field.name.as_str())
                    .ok_or_else(|| {
                        "materialization filter references an unknown column".to_string()
                    })?;
                let operator = match op {
                    CompareOpKind::Eq => "=",
                    CompareOpKind::Ne => "!=",
                    CompareOpKind::Regex => "=~",
                    CompareOpKind::NotRegex => "!~",
                    _ => return Err("materialization filter uses a non-PromQL comparison".into()),
                };
                let encoded = serde_json::to_string(value).map_err(|error| error.to_string())?;
                matchers.push(format!("{label}{operator}{encoded}"));
            }
            matchers.sort();
            let spatial_filter = if matchers.is_empty() {
                String::new()
            } else {
                format!("{{{}}}", matchers.join(","))
            };
            Ok((metric.clone(), window_secs, spatial_filter))
        }
        _ => {
            Err("source predicates or temporal modifiers require a Prometheus exact subtree".into())
        }
    }
}

struct SelectedMaterialization {
    node: Rc<SummaryNode>,
    metric: String,
    window_secs: Option<u64>,
    spatial_filter: String,
    group_by: Option<Vec<String>>,
    item_label: Option<String>,
    family: SummaryFamilyType,
    algorithm: String,
    parameters: Value,
}

fn validate_executable_subdag(node: &Rc<SummaryNode>) -> Result<(), String> {
    let executable = planner_types::post_asap::compile_executable_dag(node)
        .map_err(|error| format!("invalid executable subDAG: {error}"))?;
    if let Some(edge) = executable.edges.iter().find(|edge| {
        edge.grouping == planner_types::post_asap::GroupingEdgeCompatibility::Incompatible
    }) {
        return Err(format!(
            "executable subDAG contains incompatible grouping edge {} -> {} ({:?})",
            edge.producer.0, edge.consumer.0, edge.role
        ));
    }
    Ok(())
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
        window_secs: selected.window_secs.unwrap_or(query.window_secs),
        spatial_filter: selected.spatial_filter.clone(),
        grouping: selected
            .group_by
            .clone()
            .unwrap_or_else(|| query.group_by.clone()),
        item_label: selected.item_label.clone(),
        heap_update_mode: selected.parameters.get("weight_mode").and_then(|mode| {
            match mode.as_str() {
                Some("count") => Some("count"),
                Some("value") => Some("value"),
                Some("counter_delta") => Some("counter_delta"),
                _ => None,
            }
        }),
        aggregation_input: match target {
            PhysicalDeploymentTarget::DistributedCollectors => AggregationInput::SketchEnvelope,
            PhysicalDeploymentTarget::BackendLocalRemoteWrite => AggregationInput::Raw,
        },
    }
}

/// Convert the typed physical aggregation into the runtime's canonical
/// content-addressed materialization contract. This is the one conversion
/// shared by the physical compiler and the compatibility replanner; it does
/// not create a second registry or wire plan.
pub(crate) fn aggregation_config_for_materialization(
    aggregation: &BackendAggregation,
) -> anyhow::Result<asap_types::PrecomputeMaterialization> {
    use anyhow::Context as _;
    let json = crate::emit::stage_config::build_backend_aggregation_json(aggregation);
    let text = serde_json::to_string(&json).context("serialize synthesized aggregation JSON")?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&text).context("parse synthesized aggregation JSON as YAML")?;
    asap_types::PrecomputeMaterialization::from_yaml_data(
        &yaml,
        None,
        asap_types::QueryLanguage::PromQl,
    )
    .context("build materialization from physical aggregation")
}

fn materialization_consumers(
    queries: &[PlanningQuery],
    target: PhysicalDeploymentTarget,
    composable: bool,
) -> Result<BTreeMap<asap_types::PolicyFingerprint, BTreeSet<usize>>, CompileError> {
    let mut consumers = BTreeMap::<_, BTreeSet<_>>::new();
    for (index, query) in queries.iter().enumerate() {
        let states =
            collect_selected_materializations(&query.post_asap, composable).map_err(|reason| {
                CompileError::Query {
                    query_id: query.query_id.clone(),
                    reason,
                }
            })?;
        for state in states {
            if composable
                && state.window_secs.is_some_and(|window| {
                    !query
                        .window_implementations
                        .iter()
                        .any(|candidate| candidate.window_secs == window)
                })
            {
                continue;
            }
            let config = aggregation_config_for_materialization(&physical_aggregation(
                query,
                &state,
                query.query_id.clone(),
                target,
            ))?;
            consumers
                .entry(config.policy_fingerprint())
                .or_default()
                .insert(index);
        }
    }
    Ok(consumers)
}

fn shared_pane_origin_ms(
    workload: Option<&QueryWorkload>,
    consumer_indices: impl IntoIterator<Item = usize>,
    pane_width_ms: u64,
) -> Result<Option<i64>, String> {
    if pane_width_ms == 0 {
        return Err("materialized pane width is zero".into());
    }
    let pane_width = i64::try_from(pane_width_ms)
        .map_err(|_| "materialized pane width exceeds runtime timestamp range".to_string())?;
    let Some(workload) = workload else {
        // Compatibility-only PlanningRequest callers do not claim a certified
        // phase. The read path rejects this binding and uses exact fallback.
        return Ok(None);
    };
    let entries = workload.entries().collect::<Vec<_>>();
    let mut selected_phase = None;
    for index in consumer_indices {
        let entry = entries
            .get(index)
            .ok_or_else(|| format!("consumer index {index} is absent from QueryWorkload"))?;
        let QueryRecurrence::Repeated(demand @ RepeatedDemand::FixedIntervalAt { .. }) =
            &entry.recurrence
        else {
            return Err(format!(
                "shared pane-only summary consumer {index} has unknown evaluation phase"
            ));
        };
        let binding = planner_types::post_asap::plan_pane_phase(demand, pane_width_ms)
            .map_err(|error| format!("invalid pane phase for consumer {index}: {error:?}"))?;
        let origin = binding.pane_origin_ms.ok_or_else(|| {
            format!("shared pane-only summary consumer {index} has unknown evaluation phase")
        })?;
        let phase = origin.rem_euclid(pane_width);
        if selected_phase.is_some_and(|selected| selected != phase) {
            return Err(format!(
                "shared pane-only summary consumers have conflicting phases {selected_phase:?} and {phase} for pane width {pane_width_ms}ms"
            ));
        }
        selected_phase = Some(phase);
    }
    selected_phase
        .ok_or_else(|| "materialized summary has no workload consumers".to_string())
        .map(Some)
}

/// Collect every executable materialization leaf in the selected post-ASAP
/// graph. Readout context flows through merge nodes, so a graph such as
/// `Estimate(Merge(Agg(a), Agg(b)))` creates two physical bindings while the
/// serialized QueryPlan retains the merge edges. Unsupported operators are
/// intentionally not traversed: QueryPlan lowers them to an explicit exact
/// fallback node and no unused warm state is provisioned.
/// Raw accumulators do not retain arbitrary source labels. Preserve native semantics
/// unless the selected DAG explicitly authorizes pooling the source entities.
fn collect_selected_materializations(
    node: &Rc<SummaryNode>,
    composable: bool,
) -> Result<Vec<SelectedMaterialization>, String> {
    fn walk(
        node: &Rc<SummaryNode>,
        readout: Option<&SketchQuery>,
        composable: bool,
        inherited_grouping: Option<Vec<String>>,
        selected: &mut Vec<SelectedMaterialization>,
    ) -> Result<(), String> {
        let grouping = if composable {
            if let SummaryExpr::SummaryAgg {
                reduction, child, ..
            } = &node.expr
            {
                if let Some(keys) = reduction.group_keys() {
                    Some(
                        keys.keys()
                            .iter()
                            .map(|id| {
                                child
                                    .schema
                                    .fields
                                    .get(*id)
                                    .map(|field| field.name.clone())
                                    .ok_or_else(|| {
                                        format!("unresolved producer grouping column {id}")
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                } else {
                    inherited_grouping
                }
            } else {
                inherited_grouping
            }
        } else {
            None
        };
        match &node.expr {
            SummaryExpr::CandidateTopK {
                candidates, values, ..
            } => {
                walk(candidates, readout, composable, grouping.clone(), selected)?;
                // In a hybrid TopK, the sketch is only a candidate-membership
                // sidecar. Prometheus owns the authoritative value subtree;
                // provisioning local exact state here duplicates that work.
                if !composable {
                    walk(values, readout, composable, grouping.clone(), selected)?;
                }
            }
            SummaryExpr::ValueOperation { child, .. } => {
                walk(child, readout, composable, grouping.clone(), selected)?;
            }
            SummaryExpr::RelationalJoin { left, right, .. } => {
                walk(left, readout, composable, grouping.clone(), selected)?;
                walk(right, readout, composable, grouping.clone(), selected)?;
            }
            SummaryExpr::BinaryOp { lhs, rhs, .. }
                if composable || crate::query_plan::exact_value_executable(node) =>
            {
                walk(lhs, readout, composable, grouping.clone(), selected)?;
                walk(rhs, readout, composable, grouping.clone(), selected)?;
            }
            SummaryExpr::SummaryAgg {
                child,
                family:
                    SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _),
                ..
            } if !matches!(child.expr, SummaryExpr::KeepPreAsap(_))
                && ((composable && matches!(child.expr, SummaryExpr::SummaryAgg { .. }))
                    || crate::query_plan::exact_value_executable(node)) =>
            {
                walk(child, readout, composable, grouping.clone(), selected)?;
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
            } => walk(
                summary_input,
                Some(query),
                composable,
                grouping.clone(),
                selected,
            )?,
            SummaryExpr::SummaryMerge { children } => {
                for child in children {
                    walk(child, readout, composable, grouping.clone(), selected)?;
                }
            }
            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::Sketch(kind, _),
                input,
                ..
            } => {
                if let Some(readout) = readout {
                    let mut parameters = sketch_params_json(kind.params());
                    let mut item_label = None;
                    if matches!(readout, SketchQuery::TopK { .. }) {
                        use planner_types::post_asap::SummaryInputExpr;
                        item_label = match &input.item {
                            Some(SummaryInputExpr::Column(
                                planner_types::pre_asap::ColumnRef::Named(label),
                            )) => Some(label.clone()),
                            Some(SummaryInputExpr::Column(
                                planner_types::pre_asap::ColumnRef::Qualified { name, .. },
                            )) => Some(name.clone()),
                            // Legacy/direct TopK plans did not carry an item
                            // projection. Keep their generic item-key behavior;
                            // typed Planner plans name the inner aggregate
                            // identity explicitly through SummaryUpdate.item.
                            _ => None,
                        };
                        let mode = match &input.weight {
                            SummaryInputExpr::Constant(value) if *value == 1.0 => "count",
                            SummaryInputExpr::Column(
                                planner_types::pre_asap::ColumnRef::SampleValue,
                            ) => "value",
                            SummaryInputExpr::ResetAwareCounterDelta { .. }
                                if matches!(
                                    input.weight_domain,
                                    planner_types::post_asap::WeightDomain::NonNegative {
                                        proof: planner_types::post_asap::NonNegativeWeightProof::ResetAwareCounterDerivative
                                    }
                                ) => "counter_delta",
                            SummaryInputExpr::ResetAwareCounterDelta { .. } => {
                                return Err("counter-delta TopK input lacks a non-negative reset-aware proof".into())
                            }
                            _ => return Err("unsupported TopK SummaryUpdate weight".into()),
                        };
                        parameters["weight_mode"] = mode.into();
                        if mode == "counter_delta" {
                            // CMS/CountSketch heap implementations quantize
                            // weights to integer counters. Preserve sub-unit
                            // counter increments used by CPU metrics.
                            parameters["weight_scale"] = 1_000_000.into();
                        }
                    }
                    let (metric, window_secs, spatial_filter) =
                        match materialization_leaf_contract(node) {
                            Ok(contract) => contract,
                            Err(_) if composable => return Ok(()),
                            Err(error) => return Err(error),
                        };
                    selected.push(SelectedMaterialization {
                        node: Rc::clone(node),
                        metric,
                        window_secs,
                        spatial_filter,
                        group_by: grouping.clone(),
                        item_label,
                        family: SummaryFamilyType::Sketch(
                            kind.clone(),
                            planner_types::post_asap::GroupingStrategy::PerSubpopulationInstance,
                        ),
                        algorithm: format!("{:?}", kind.algorithm()).to_ascii_lowercase(),
                        parameters,
                    });
                }
            }
            SummaryExpr::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(kind, params),
                ..
            } => {
                let (metric, window_secs, spatial_filter) =
                    match materialization_leaf_contract(node) {
                        Ok(contract) => contract,
                        Err(_) if composable => return Ok(()),
                        Err(error) => return Err(error),
                    };
                selected.push(SelectedMaterialization {
                    node: Rc::clone(node),
                    metric,
                    window_secs,
                    spatial_filter,
                    group_by: grouping.clone(),
                    item_label: None,
                    family: SummaryFamilyType::ExactAggregate(kind.clone(), params.clone()),
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
    walk(node, None, composable, None, &mut selected)?;
    if composable {
        selected
            .retain(|state| !has_unsafe_raw_entity_leaf(node, &[Rc::clone(&state.node)], false));
    }
    if !selected.is_empty() {
        validate_executable_subdag(node)?;
    }
    Ok(selected)
}

pub(crate) fn physical_materialization_family(family: &SummaryFamilyType) -> SummaryFamilyType {
    match family {
        SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Count, _) => {
            // The SummaryStore Sum accumulator retains the observation count
            // alongside its sum. Both logical states can share this producer.
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
        P::UnivMon {
            heap_size,
            sketch_rows,
            sketch_cols,
            layers,
        } => json!({"heap_size": heap_size, "sketch_rows": sketch_rows,
                "sketch_cols": sketch_cols, "layers": layers}),
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

    #[test]
    fn raw_per_entity_state_requires_explicit_additive_reduction() {
        for query in [
            "sum_over_time(m[1m])",
            "quantile_over_time(0.99, m[1m])",
            "sum_over_time(m[1m]) / count_over_time(m[1m])",
        ] {
            let mut environment = environment(10_000);
            environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            environment.collector_ids.clear();
            let plan = PhysicalCompiler
                .compile(request("per-entity", query), environment)
                .unwrap();
            assert!(
                plan.precompute_plan.materializations.is_empty(),
                "{query} pooled source entities"
            );
        }
        for query in [
            "sum(sum_over_time(m[1m]))",
            "sum by (job) (sum_over_time(m[1m]))",
        ] {
            let mut environment = environment(10_000);
            environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            environment.collector_ids.clear();
            let plan = PhysicalCompiler
                .compile(request("reduced", query), environment)
                .unwrap();
            assert!(
                !plan.precompute_plan.materializations.is_empty(),
                "{query} lost safe additive state"
            );
        }
    }

    #[test]
    fn backend_local_materializes_counter_with_per_series_state() {
        let request = request("counter", "sum(rate(m[1m]))");
        let mut environment = environment(10_000);
        environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        environment.collector_ids.clear();
        let plan = PhysicalCompiler.compile(request, environment).unwrap();
        assert_eq!(plan.precompute_plan.materializations.len(), 1);
        assert_eq!(
            plan.precompute_plan.materializations[0].num_aggregates_to_retain,
            Some(2),
            "the explicitly selected one-minute pane plus a boundary pane is retained"
        );
        assert!(plan
            .query_plan
            .entries
            .values()
            .all(|entry| entry.nodes.values().any(|node| matches!(
                node,
                crate::query_plan::QueryPlanNode::ExactReadout {
                    readout: crate::query_plan::ExactReadout::Rate,
                    ..
                }
            ))));
    }

    #[test]
    fn raw_counter_artifact_is_valid_for_backend_precompute() {
        let plan = PhysicalCompiler
            .compile(request("counter", "rate(m[1m])"), environment(10_000))
            .unwrap();
        plan.precompute_plan.validate().unwrap();
        let catalog = plan.summary_catalog.clone();
        let mut raw = plan.precompute_plan;
        raw.ingest.protocol = IngestProtocol::PrometheusRemoteWriteV1;
        raw.ingest.endpoint_path = "/api/v1/write".into();
        raw.ingest.timestamp_unit = TimestampUnit::UnixMilliseconds;
        raw.ingest.require_plan_identity = false;
        raw.ingest.require_summary_definition_identity = false;
        raw.ingest.require_registered_producer = false;
        raw.producers.clear();
        raw.bind_catalog(&catalog).unwrap();
        raw.validate().unwrap();
    }

    // Counter fallback does not disable an independent safe summary in the same workload.
    #[test]
    fn counter_fallback_preserves_other_workload_summaries() {
        let mut workload = request("counter", "sum(rate(m[1m]))");
        workload
            .queries
            .extend(request("gauge", "sum(sum_over_time(g[1m]))").queries);
        let mut environment = environment(10_000);
        environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        environment.collector_ids.clear();
        let plan = PhysicalCompiler.compile(workload, environment).unwrap();
        assert_eq!(plan.precompute_plan.materializations.len(), 2);
        assert_eq!(
            plan.query_plan
                .lookup("sum(rate(m[1m]))")
                .unwrap()
                .materialization_bindings()
                .len(),
            1
        );
        assert_eq!(
            plan.query_plan
                .lookup("sum(sum_over_time(g[1m]))")
                .unwrap()
                .materialization_bindings()
                .len(),
            1
        );
    }

    // Capability normalization precedes candidate enumeration, avoiding duplicate exact quotes.
    #[test]
    fn counter_only_snapshot_has_distinct_local_and_native_cost_alternatives() {
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query = Query("rate(m[1m])".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (request, _) = snapshot.planning_request().unwrap();
        assert_eq!(
            super::super::workload_cost::with_exact_alternative(request)
                .unwrap()
                .len(),
            3
        );
    }

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

    #[test]
    fn weighted_counter_topk_keeps_heap_membership_separate_from_exact_values() {
        let query = "topk(2, sum by (job) (rate(m[1m])))";
        let evidence = TopKMembershipEvidence {
            selected_lower_bound: 101.0,
            excluded_upper_bound: 100.0,
            interval_failure_probability: 0.001,
            observed_at_unix_ms: 9_500,
            source: "unit-fixture".into(),
        };
        let request = request_with_evidence("topk-rate", query, Some(evidence)).unwrap();
        let plan = PhysicalCompiler
            .compile(request, environment(10_000))
            .unwrap();
        let entry = plan.query_plan.entries.values().next().unwrap();
        let crate::query_plan::QueryPlanNode::CandidateTopK { inputs, .. } =
            &entry.nodes[&entry.root]
        else {
            panic!("Planner weighted TopK must lower to CandidateTopK: {entry:#?}");
        };
        assert!(matches!(
            entry.nodes[&inputs[0]],
            crate::query_plan::QueryPlanNode::SummaryEstimate {
                query: crate::query_plan::QueryReadout::TopK { .. },
                ..
            }
        ));
        let candidate_read = match &entry.nodes[&inputs[0]] {
            crate::query_plan::QueryPlanNode::SummaryEstimate { input, .. } => *input,
            _ => unreachable!(),
        };
        assert!(matches!(
            entry.nodes[&candidate_read],
            crate::query_plan::QueryPlanNode::ReadMaterialization { .. }
        ));
        assert!(!entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactFallback { .. })));
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            crate::query_plan::QueryPlanNode::ExactReadout {
                readout: crate::query_plan::ExactReadout::Rate,
                ..
            }
        )));
        let heaps = plan
            .precompute_plan
            .materializations
            .iter()
            .filter(|materialization| {
                materialization.aggregation_type
                    == asap_types::AggregationType::CountMinSketchWithHeap
                    && materialization.parameters["weight_mode"] == "counter_delta"
            })
            .collect::<Vec<_>>();
        assert_eq!(heaps.len(), 1, "unpartitioned TopK owns one global CMS");
        assert!(heaps[0].grouping_labels.labels.is_empty());
        assert_eq!(heaps[0].aggregated_labels.labels, vec!["job"]);
        assert_eq!(heaps[0].parameters["weight_scale"], 1_000_000);
        assert_eq!(retained_partition_count(heaps[0], Some(5)), 1);
        let crate::query_plan::QueryPlanNode::ReadMaterialization { binding } =
            &entry.nodes[&candidate_read]
        else {
            unreachable!()
        };
        assert!(matches!(
            binding.output_grouping,
            crate::query_plan::PhysicalGrouping::Reduce(ref labels) if labels.is_empty()
        ));
        assert_eq!(binding.item_labels, vec!["job"]);
        let counter = plan
            .precompute_plan
            .materializations
            .iter()
            .find(|materialization| {
                matches!(
                    materialization.aggregation_type,
                    asap_types::AggregationType::Increase
                        | asap_types::AggregationType::MultipleIncrease
                )
            })
            .expect("reset-aware exact counter");
        assert_eq!(retained_partition_count(counter, Some(5)), 5);
    }

    #[test]
    fn hybrid_weighted_topk_installs_only_candidates_and_delegates_filtered_exact_values() {
        use crate::query_plan::{
            logical::LogicalOperator, ExternalExactInput, ExternalExactOutput, QueryPlanNode,
        };
        let query = "topk(2, sum by (job) (rate(m[1m])))";
        let evidence = TopKMembershipEvidence {
            selected_lower_bound: 101.0,
            excluded_upper_bound: 100.0,
            interval_failure_probability: 0.001,
            observed_at_unix_ms: 9_500,
            source: "unit-fixture".into(),
        };
        let mut request = request_with_evidence("topk-rate", query, Some(evidence)).unwrap();
        request.hybrid_execution = true;
        let mut environment = environment(10_000);
        environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        environment.collector_ids.clear();

        let plan = PhysicalCompiler.compile(request, environment).unwrap();

        assert_eq!(plan.precompute_plan.materializations.len(), 1);
        assert_eq!(
            plan.precompute_plan.materializations[0].aggregation_type,
            asap_types::AggregationType::CountMinSketchWithHeap
        );
        let entry = plan.query_plan.lookup(query).unwrap();
        let QueryPlanNode::CandidateTopK { inputs, .. } = &entry.nodes[&entry.root] else {
            panic!("expected candidate TopK: {entry:#?}");
        };
        assert!(matches!(
            &entry.nodes[&inputs[1]],
            QueryPlanNode::ExternalExact {
                request,
                inputs: exact_inputs,
            } if request.language == crate::query_plan::QueryLanguage::PromQl
                && request.expression == "sum by (job) (rate(m[1m]))"
                && request.output == ExternalExactOutput::InstantVector
                && request.input_contracts == vec![ExternalExactInput::CandidateMembership {
                    item_label: "job".into(),
                }]
                && exact_inputs == &vec![inputs[0]]
        ));
        assert!(entry.nodes.values().all(|node| !matches!(
            node,
            QueryPlanNode::ExactReadout { .. }
                | QueryPlanNode::Logical {
                    operator: LogicalOperator::Scan { .. },
                    ..
                }
        )));
        let installed = plan
            .precompute_plan
            .executable_dags
            .get(&entry.query_id)
            .expect("compiled query retains its Planner DAG and backend placement");
        installed.validate().expect("typed DAG document");
        installed
            .validate_query_plan(entry)
            .expect("query node bindings");
        assert_eq!(installed.binding.query_plan_sink, entry.root);
        assert!(installed.binding.nodes.values().any(|placement| matches!(
            placement,
            crate::physical::executable_binding::BackendNodeBinding::Materialization { .. }
        )));
        let encoded = serde_json::to_value(installed).unwrap();
        let decoded: crate::physical::executable_binding::InstalledPostAsapDag =
            serde_json::from_value(encoded).unwrap();
        assert_eq!(&decoded, installed);
        decoded
            .validate()
            .expect("round-tripped typed DAG document");
    }

    #[test]
    fn retained_footprint_rejects_eval_sized_cms_across_all_panes() {
        let evidence = TopKMembershipEvidence {
            selected_lower_bound: 101.0,
            excluded_upper_bound: 100.0,
            interval_failure_probability: 0.001,
            observed_at_unix_ms: 9_500,
            source: "unit-fixture".into(),
        };
        let plan = PhysicalCompiler
            .compile(
                request_with_evidence(
                    "topk-rate",
                    "topk(2, sum by (job) (rate(m[1m])))",
                    Some(evidence),
                )
                .unwrap(),
                environment(10_000),
            )
            .unwrap();
        let mut materializations = plan.precompute_plan.materializations;
        let cms = materializations
            .iter_mut()
            .find(|materialization| {
                materialization.aggregation_type
                    == asap_types::AggregationType::CountMinSketchWithHeap
            })
            .unwrap();
        cms.parameters.remove("width");
        cms.parameters.remove("depth");
        cms.parameters.insert("w".into(), json!(524_288));
        cms.parameters.insert("d".into(), json!(7));
        cms.num_aggregates_to_retain = Some(80);

        let error = validate_retained_summary_footprint(
            &materializations,
            Some(5),
            DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exceeds budget"));

        let cms = materializations
            .iter_mut()
            .find(|materialization| {
                materialization.aggregation_type
                    == asap_types::AggregationType::CountMinSketchWithHeap
            })
            .unwrap();
        cms.parameters.insert("w".into(), json!(4_096));
        validate_retained_summary_footprint(
            &materializations,
            Some(5),
            DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES,
        )
        .unwrap();

        let mut request = request("bounded", "sum(sum_over_time(m[1m]))");
        request.retained_summary_memory_budget_bytes = Some(1);
        let error = PhysicalCompiler
            .compile(request, environment(10_000))
            .unwrap_err();
        assert!(error.to_string().contains("retained summary footprint"));
    }

    #[test]
    fn legacy_backend_snapshot_gets_explicit_retained_memory_default_and_alias() {
        let source = include_str!("../../../docs/examples/asapquery-planning-snapshot.json");
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(source).unwrap();
        assert_eq!(
            snapshot.implementation.max_retained_summary_bytes,
            DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES
        );

        let mut value: Value = serde_json::from_str(source).unwrap();
        value["implementation"]["maxRetainedSummaryBytes"] = json!(123_456);
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(value).unwrap();
        assert_eq!(snapshot.implementation.max_retained_summary_bytes, 123_456);
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
            hybrid_execution: false,
            materialization_policy: None,
            query_workload: None,
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
                    slide_secs: 60,
                    layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 60 },
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
            erp: None,
            exact_composition_costs: HashMap::new(),
            planner_revision: PLANNER_REVISION.into(),
            source_sample_interval_ms: None,
            query_staleness_margin_ms: 0,
            retained_summary_memory_budget_bytes: None,
        })
    }

    fn request(query_id: &str, promql: &str) -> PlanningRequest {
        request_with_evidence(query_id, promql, None).expect("post-ASAP selection")
    }

    #[test]
    fn duplicate_query_ids_cannot_overwrite_installed_dag_documents() {
        let mut workload = request("shared-id", "max_over_time(a[1m])");
        let mut second = workload.queries[0].clone();
        second.query_string = "max_over_time(b[1m])".into();
        workload.queries.push(second);

        let error = PhysicalCompiler
            .compile(workload, environment(10_000))
            .unwrap_err();
        assert!(matches!(
            error,
            CompileError::Snapshot(reason)
                if reason == "planning query IDs must be unique within a plan generation"
        ));
    }

    #[test]
    fn metricsql_compilation_publishes_a_language_tagged_query_entry() {
        let query = "mad_over_time(m[1m])";
        let mut workload = request("vm-q", "last_over_time(m[1m])");
        let accuracy = workload.queries[0].accuracy.clone();
        let canonical = asap_frontend_promql::lower_promql(query, accuracy.clone()).unwrap();
        workload.queries[0].query_string = query.into();
        workload.queries[0].post_asap =
            crate::planner_selection::keep_pre_asap(&canonical).unwrap();
        let plan = PhysicalCompiler
            .compile_metricsql(workload, environment(10_000))
            .unwrap();
        let identity = canonical_promql(query).unwrap();
        let entry = plan
            .query_plan
            .lookup_canonical(crate::query_plan::QueryLanguage::MetricsQl, &identity)
            .unwrap();
        assert_eq!(entry.query_id, "vm-q");
        assert_eq!(entry.language, crate::query_plan::QueryLanguage::MetricsQl);
    }

    #[test]
    fn hybrid_erp_capability_miss_preserves_exact_subtree() {
        let mut workload = request("q", "quantile_over_time(0.9, m[1m])");
        let roots = vec![Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                &workload.queries[0].query_string,
                workload.queries[0].accuracy.clone(),
            )
            .unwrap(),
        )];
        let erp = super::super::erp::ErpPlanningInput {
            artifact: asap_aware_mapping::erp::ErpArtifact {
                schema_version: asap_aware_mapping::erp::ERP_SCHEMA_VERSION,
                producer_version: "bench-rev".into(),
                records: vec![],
            },
            distribution: serde_json::json!({"synthetic":{"kind":"zipf"}}),
            implementation: None,
            error_metric: "relative_error".into(),
            min_trials: 10,
            expected_updates: 1000.0,
            expected_queries: 10.0,
            expected_merges: 0.0,
            retention_seconds: 60.0,
            cpu_weight: 1.0,
            byte_second_weight: 1e-9,
            mode: super::super::erp::ErpAccuracyMode::Hybrid,
            observed_shape: None,
            observed_shape_source: None,
            shape_match: None,
            runtime: super::super::erp::ErpRuntimeCapabilities {
                allowed_algorithms: vec![SketchAlgorithm::Hll],
                max_memory_bytes: None,
            },
        };
        select_workload_roots_with_erp(
            &mut workload.queries,
            roots,
            &workload.evidence,
            &workload.exact_composition_costs,
            Some(&erp),
        )
        .unwrap();
        assert!(matches!(
            workload.queries[0].post_asap.expr,
            SummaryExpr::KeepPreAsap(_)
        ));
    }

    // Frozen sketch-bench output exercises the same wire schema on every CI run.
    #[test]
    fn measured_erp_kll_parameters_survive_workload_selection() {
        use super::super::erp::{
            ErpAccuracyMode, ErpParameterDecision, ErpPlanningInput, ErpRuntimeCapabilities,
        };

        let artifact: asap_aware_mapping::erp::ErpArtifact =
            serde_json::from_str(include_str!("../../tests/fixtures/erp-kll-measured.json"))
                .unwrap();
        let row = artifact
            .records
            .first()
            .expect("nonempty measured artifact");
        let erp = ErpPlanningInput {
            distribution: row.distribution.clone(),
            implementation: Some("lib".into()),
            artifact,
            error_metric: "max_rank_err".into(),
            min_trials: 10,
            expected_updates: 1000.0,
            expected_queries: 10.0,
            expected_merges: 0.0,
            retention_seconds: 60.0,
            cpu_weight: 1.0,
            byte_second_weight: 1e-9,
            mode: ErpAccuracyMode::Hybrid,
            observed_shape: None,
            observed_shape_source: None,
            shape_match: None,
            runtime: ErpRuntimeCapabilities {
                allowed_algorithms: vec![SketchAlgorithm::Kll],
                max_memory_bytes: None,
            },
        };
        assert!(
            matches!(
                erp.select(SketchAlgorithm::Kll, 0.06, SketchParams::Kll { k: 269 }),
                ErpParameterDecision::Empirical {
                    params: SketchParams::Kll { k: 32 },
                    ..
                }
            ),
            "the measured row must pass adapter selection before testing the planner"
        );

        let mut workload = request("q", "quantile_over_time(0.9, m[1m])");
        workload.queries[0].accuracy = AccuracyTarget::Epsilon(0.06);
        let root = Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                &workload.queries[0].query_string,
                workload.queries[0].accuracy.clone(),
            )
            .unwrap(),
        );
        select_workload_roots_with_erp(
            &mut workload.queries,
            vec![root],
            &workload.evidence,
            &workload.exact_composition_costs,
            Some(&erp),
        )
        .unwrap();

        fn contains_measured_kll(node: &SummaryNode) -> bool {
            match &node.expr {
                SummaryExpr::SummaryAgg { family, child, .. } => {
                    matches!(family, SummaryFamilyType::Sketch(kind, _)
                        if matches!(kind.params(), SketchParams::Kll { k: 32 }))
                        || contains_measured_kll(child)
                }
                SummaryExpr::SummaryEstimate { summary_input, .. }
                | SummaryExpr::ValueOperation {
                    child: summary_input,
                    ..
                } => contains_measured_kll(summary_input),
                _ => false,
            }
        }
        assert!(
            contains_measured_kll(&workload.queries[0].post_asap),
            "ERP hit was lost before physical compilation: {:#?}",
            workload.queries[0].post_asap
        );
        let guarantee = workload.queries[0].post_asap.guarantee.as_ref().unwrap();
        assert_eq!(guarantee.failure_probability.evaluate(), None);
        assert!(!guarantee.is_exact());
        let plan = PhysicalCompiler
            .compile(workload, environment(10000))
            .unwrap();
        assert_eq!(plan.precompute_plan.materializations[0].parameters["k"], 32);
        let empirical_identity = plan.precompute_plan.materializations[0].policy_fingerprint();
        let mut drift = erp.clone();
        drift.distribution = serde_json::json!({"shifted": true});
        let mut unsupported = drift.clone();
        unsupported.runtime.allowed_algorithms = vec![SketchAlgorithm::Hll];
        let mut wrong_implementation = erp.clone();
        wrong_implementation.artifact.records[0].implementation = "oxide".into();
        wrong_implementation.implementation = Some("oxide".into());
        for (policy, accuracy, exact) in [
            (drift, AccuracyTarget::Epsilon(0.06), false),
            (wrong_implementation, AccuracyTarget::Epsilon(0.06), false),
            (
                erp.clone(),
                AccuracyTarget::EpsilonDelta {
                    epsilon: 0.06,
                    delta: 0.01,
                },
                false,
            ),
            (unsupported, AccuracyTarget::Epsilon(0.06), true),
        ] {
            let mut workload = request("q", "quantile_over_time(0.9, m[1m])");
            workload.queries[0].accuracy = accuracy.clone();
            let root = Rc::new(
                crate::query_parser::parse_query_expr_canonical(
                    &workload.queries[0].query_string,
                    accuracy,
                )
                .unwrap(),
            );
            select_workload_roots_with_erp(
                &mut workload.queries,
                vec![root],
                &workload.evidence,
                &workload.exact_composition_costs,
                Some(&policy),
            )
            .unwrap();
            if exact {
                assert!(matches!(
                    workload.queries[0].post_asap.expr,
                    SummaryExpr::KeepPreAsap(_)
                ));
            } else {
                assert!(!contains_measured_kll(&workload.queries[0].post_asap));
                let plan = PhysicalCompiler
                    .compile(workload, environment(10000))
                    .unwrap();
                let state = &plan.precompute_plan.materializations[0];
                assert!(state.parameters["k"].as_u64().unwrap() > 32);
                assert_ne!(state.policy_fingerprint(), empirical_identity);
            }
        }
    }

    fn measured_exact_composition_rows(
        expr: &QueryExpr,
        observed_at_unix_ms: u64,
    ) -> Vec<ExactCompositionCostEvidence> {
        fn visit(
            expr: &QueryExpr,
            observed_at_unix_ms: u64,
            rows: &mut Vec<ExactCompositionCostEvidence>,
        ) {
            use planner_types::post_asap::ExactOperation;
            match expr {
                QueryExpr::Aggregate {
                    reduction,
                    measures,
                    output_names,
                    having,
                    child,
                } => {
                    rows.push(ExactCompositionCostEvidence {
                        target: serde_json::to_value(expr).unwrap(),
                        operation: serde_json::to_value(ExactOperation::Aggregate {
                            reduction: reduction.clone(),
                            measures: measures.clone(),
                            output_names: output_names.clone(),
                            having: having.clone(),
                        })
                        .unwrap(),
                        placement:
                            crate::physical::post_asap::cost_model::ExactOperationPlacement::Read,
                        expected_input_rows: 100.0,
                        expected_output_rows: 10.0,
                        exact_cpu_ns_per_row: 2.0,
                        summary_maintenance_cpu_ns_per_update: 3.0,
                        summary_read_cpu_ns: 20.0,
                        update_rate_per_second: 100.0,
                        evaluation_rate_per_second: 0.1,
                        raw_recompute_cpu_ns: 100_000.0,
                        observed_peak_memory_bytes: 4096,
                        data_snapshot_id: "topk-planning-fixture".into(),
                        model_version: "measured-test-v1".into(),
                        observed_at_unix_ms,
                        valid_for_ms: 60_000,
                    });
                    visit(child, observed_at_unix_ms, rows);
                }
                QueryExpr::Sort { child, .. }
                | QueryExpr::Limit { child, .. }
                | QueryExpr::TimeRange { child, .. }
                | QueryExpr::Filter { child, .. }
                | QueryExpr::Project { child, .. }
                | QueryExpr::PromqlSubquery { child, .. }
                | QueryExpr::TimeShift { child, .. }
                | QueryExpr::PromqlScalarBridge(child)
                | QueryExpr::PromqlVectorFromScalar(child)
                | QueryExpr::PromqlScalarFromVector(child)
                | QueryExpr::PromqlRelabel { child, .. }
                | QueryExpr::PromqlSeriesSample { child, .. }
                | QueryExpr::Dedup { child, .. } => visit(child, observed_at_unix_ms, rows),
                _ => {}
            }
        }
        let mut rows = Vec::new();
        visit(expr, observed_at_unix_ms, &mut rows);
        rows
    }

    #[test]
    fn sum_rate_uses_summary_child_only_with_measured_composition_costs() {
        let promql = "sum by (job) (rate(m[1m]))";
        let mut with_evidence = request("topk-rate", promql);
        with_evidence.hybrid_execution = true;
        let root = Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                promql,
                with_evidence.queries[0].accuracy.clone(),
            )
            .unwrap(),
        );
        with_evidence.exact_composition_costs.insert(
            "topk-rate".into(),
            measured_exact_composition_rows(&root, 9_500),
        );
        select_workload_roots(
            &mut with_evidence.queries,
            vec![root],
            &with_evidence.evidence,
            &with_evidence.exact_composition_costs,
        )
        .unwrap();
        let mut backend = environment(10_000);
        backend.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        backend.collector_ids.clear();
        let plan = PhysicalCompiler.compile(with_evidence, backend).unwrap();
        assert!(
            !plan.summary_catalog.materializations.is_empty(),
            "measured exact-composition evidence must expose the rate child as a SummaryStore binding"
        );
    }

    #[test]
    fn sum_rate_without_composition_costs_does_not_invent_a_composition_cost() {
        let promql = "sum by (job) (rate(m[1m]))";
        let mut unavailable = request("topk-rate", promql);
        unavailable.hybrid_execution = true;
        let root = Rc::new(
            crate::query_parser::parse_query_expr_canonical(
                promql,
                unavailable.queries[0].accuracy.clone(),
            )
            .unwrap(),
        );
        select_workload_roots(
            &mut unavailable.queries,
            vec![root],
            &unavailable.evidence,
            &unavailable.exact_composition_costs,
        )
        .unwrap();
        let mut backend = environment(10_000);
        backend.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        backend.collector_ids.clear();
        let plan = PhysicalCompiler.compile(unavailable, backend).unwrap();
        // Planner 54f can realize this particular shape directly as an exact
        // counter readout plus a query-time reduce; it does not require an
        // ExactComposition candidate. The absence of evidence must therefore
        // leave that direct legal path intact rather than inventing a composed
        // cost or forcing an exact fallback.
        assert!(!plan.summary_catalog.materializations.is_empty());
        let entry = plan
            .query_plan
            .entries
            .values()
            .find(|entry| entry.query_id == "topk-rate")
            .unwrap();
        assert!(entry
            .nodes
            .values()
            .any(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactReadout { .. })));
    }

    fn phase_workload(phases: &[u64]) -> QueryWorkload {
        QueryWorkload {
            language: QueryLanguage::PromQL,
            query_batch: None,
            repeating_queries: Some(
                phases
                    .iter()
                    .enumerate()
                    .map(|(index, phase)| RepeatingEntry {
                        query: Query(format!("q{index}")),
                        demand: RepeatedDemand::FixedIntervalAt {
                            interval: RepetitionInterval(10_000),
                            evaluation_phase: planner_types::workload::TimestampMs(*phase),
                        },
                        requirements: QueryRequirements::default(),
                        predictability: Predictability::Predictable { known_at: None },
                        time_selection: TimeSelection::default(),
                    })
                    .collect(),
            ),
            data_workload: None,
        }
    }

    #[test]
    fn shared_summary_requires_one_consumer_phase() {
        let aligned = phase_workload(&[7_000, 67_000]);
        assert_eq!(
            shared_pane_origin_ms(Some(&aligned), [0, 1], 60_000).unwrap(),
            Some(7_000)
        );

        let conflicting = phase_workload(&[7_000, 8_000]);
        assert!(shared_pane_origin_ms(Some(&conflicting), [0, 1], 60_000)
            .unwrap_err()
            .contains("conflicting phases"));

        let mut unknown = phase_workload(&[7_000]);
        unknown.repeating_queries.as_mut().unwrap()[0].demand =
            RepeatedDemand::FixedInterval(RepetitionInterval(10_000));
        assert!(shared_pane_origin_ms(Some(&unknown), [0], 60_000)
            .unwrap_err()
            .contains("unknown evaluation phase"));
    }

    #[test]
    fn query_staleness_extends_retention_without_changing_readout_lookback() {
        let panes = asap_types::WindowMaterializationLayout::Pane { pane_secs: 30 };
        assert_eq!(
            retained_state_count(6 * 60 * 60_000, 19 * 60_000, 30_000, &panes),
            759
        );
        assert_eq!(
            retained_state_count(6 * 60 * 60_000, 0, 30_000, &panes),
            721
        );
        assert_eq!(
            retained_state_count(
                6 * 60 * 60_000,
                19 * 60_000,
                30_000,
                &asap_types::WindowMaterializationLayout::FullWindow,
            ),
            39
        );
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
        select_workload_roots(
            &mut workload.queries,
            roots,
            &workload.evidence,
            &workload.exact_composition_costs,
        )
        .unwrap();
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
        assert!(select_workload_roots(
            &mut workload.queries,
            vec![],
            &workload.evidence,
            &workload.exact_composition_costs,
        )
        .is_err());
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
            let (first, second) = if target == PhysicalDeploymentTarget::BackendLocalRemoteWrite {
                ("sum(rate(m[1m]))", "sum by (job) (rate(m[1m]))")
            } else {
                (
                    "quantile_over_time(0.90, m[1m])",
                    "quantile_over_time(0.99, m[1m])",
                )
            };
            let mut workload = request("first", first);
            workload.queries.extend(request("second", second).queries);
            let mut env = environment(10_000);
            env.target = target;
            if target == PhysicalDeploymentTarget::BackendLocalRemoteWrite {
                env.collector_ids.clear();
            }
            let bundle = PhysicalCompiler
                .compile(workload, env)
                .expect("shared compile");
            assert_eq!(bundle.query_plan.entries.len(), 2);
            assert_eq!(bundle.summary_catalog.materializations.len(), 1);
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
                assert!(bindings.contains(&collector.materializations[0].materialization.into()));
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
        assert_eq!(bundle.summary_catalog.materializations.len(), 2);
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
        let mut workload = request("q90", "quantile_over_time(0.90, m[1m])");
        let mut other = request("q99", "quantile_over_time(0.99, m[1m])");
        other.queries[0].window_implementations[0].implementation_id =
            "another-implementation".into();
        workload.queries.extend(other.queries);
        let error = PhysicalCompiler
            .compile(workload, environment(10_000))
            .expect_err("conflicting shared state must fail before publication");
        assert!(error
            .to_string()
            .contains("conflicting deployment contracts"));
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
        let (request, env) = snapshot.planning_request().unwrap();
        let bundle = PhysicalCompiler.compile(request, env).unwrap();
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
        assert!(bundle
            .query_plan
            .entries
            .values()
            .any(|entry| entry.nodes.values().any(|node| matches!(
                node,
                crate::query_plan::QueryPlanNode::Logical {
                    operator: crate::query_plan::logical::LogicalOperator::Binary { .. },
                    ..
                }
            ))));
    }

    #[test]
    fn invalid_unselected_candidate_is_canonicalized_to_exact_fallback() {
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query =
            Query("sum by (service) (sum_over_time(m[1m]) / count_over_time(m[1m]))".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (mut request, _) = snapshot.planning_request().unwrap();
        assert!(!matches!(
            request.queries[0].post_asap.expr,
            SummaryExpr::KeepPreAsap(_)
        ));

        preserve_invalid_exact_fallback_roots(&mut request.queries, true).unwrap();

        assert!(matches!(
            request.queries[0].post_asap.expr,
            SummaryExpr::KeepPreAsap(_)
        ));
    }

    #[test]
    fn selected_maintenance_dependency_still_requires_a_valid_executable_dag() {
        let mut request = request("invalid-dependency", "sum(sum_over_time(m[1m]))");
        let selected = request.queries[0].post_asap.clone();
        request.queries[0].post_asap = Rc::new(SummaryNode {
            expr: SummaryExpr::BinaryOp {
                lhs: selected.clone(),
                rhs: selected.clone(),
                operator: planner_types::post_asap::BinaryOperator {
                    kind: planner_types::pre_asap::BinaryOpKind::Arithmetic(
                        planner_types::pre_asap::ArithmeticOpKind::Add,
                    ),
                    vector_match: None,
                },
            },
            schema: selected.schema.clone(),
            guarantee: None,
        });
        request.hybrid_execution = true;
        let mut environment = environment(10_000);
        environment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        environment.collector_ids.clear();

        let error = PhysicalCompiler.compile(request, environment).unwrap_err();

        assert!(error.to_string().contains("invalid executable subDAG"));
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
            let (mut request, environment) = snapshot.planning_request().unwrap();
            request.hybrid_execution = false;
            let bundle = PhysicalCompiler.compile(request, environment).unwrap();
            assert!(bundle.precompute_plan.materializations.is_empty());
            assert!(bundle.query_plan.entries.values().all(|entry| matches!(
                entry.nodes[&entry.root],
                crate::query_plan::QueryPlanNode::ExactFallback { .. }
            )));
        }
    }

    // Local composition must evaluate per-series division before the outer sum.
    #[test]
    fn composable_non_additive_rollup_does_not_pool_raw_producers() {
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query =
            Query("sum by (service) (sum_over_time(m[1m]) / count_over_time(m[1m]))".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (request, environment) = snapshot.planning_request().unwrap();
        let candidates = super::super::workload_cost::with_exact_alternative(request).unwrap();
        match PhysicalCompiler.compile(candidates[0].clone(), environment.clone()) {
            Ok(plan) => assert!(plan.precompute_plan.materializations.is_empty()),
            Err(error) => assert!(error
                .to_string()
                .contains("semantically identical original subtree witness")),
        }
        let native = PhysicalCompiler
            .compile(candidates.last().unwrap().clone(), environment)
            .unwrap();
        assert!(native.precompute_plan.materializations.is_empty());
    }

    #[test]
    fn composable_per_entity_window_delegates_exact_subtree_to_prometheus() {
        use crate::query_plan::{logical::LogicalOperator, QueryPlanNode};
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query = Query("sum_over_time(m[1m])".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (request, env) = snapshot.planning_request().unwrap();
        let plan = PhysicalCompiler.compile(request, env).unwrap();
        assert!(plan.precompute_plan.materializations.is_empty());
        let entry = plan.query_plan.lookup("sum_over_time(m[1m])").unwrap();
        assert!(entry.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: LogicalOperator::ExactSubquery { query },
                ..
            } if query == "sum_over_time(m[1m])"
        )));
        assert!(entry.nodes.values().all(|node| !matches!(
            node,
            QueryPlanNode::ReadMaterialization { .. }
                | QueryPlanNode::ExactFallback { .. }
                | QueryPlanNode::Logical {
                    operator: LogicalOperator::Scan { .. },
                    ..
                }
        )));
    }

    // Each operand retains its source and semantic range; a smaller shared pane
    // remains valid for both readouts.
    #[test]
    fn composable_binary_binds_independent_source_windows() {
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query = Query("sum(sum_over_time(a[1m])) / sum(sum_over_time(b[5m]))".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (mut request, env) = snapshot.planning_request().unwrap();
        let query = &mut request.queries[0];
        let mut five_minutes = query.window_implementations[0].clone();
        five_minutes.implementation_id = "five-minute-evidence".into();
        five_minutes.window_secs = 300;
        five_minutes.framework = SummaryWindowFramework::Sliding;
        five_minutes.slide_secs = 60;
        five_minutes.layout = asap_types::WindowMaterializationLayout::Pane { pane_secs: 60 };
        query.window_implementations.push(five_minutes);
        let plan = PhysicalCompiler.compile(request, env).unwrap();
        let bindings = plan
            .query_plan
            .entries
            .values()
            .next()
            .unwrap()
            .materialization_bindings();
        let actual = bindings
            .iter()
            .map(|binding| {
                let identity = &plan.summary_catalog.materializations[&binding.materialization];
                let data = &plan.summary_catalog.data_descriptors[&identity.data_descriptor_id];
                (
                    data.time_series_metric().unwrap(),
                    binding.window_ms,
                    binding.readout_lookback_ms,
                )
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual,
            BTreeSet::from([("a", 60_000, Some(60_000)), ("b", 60_000, Some(300_000)),])
        );
        assert_eq!(plan.precompute_plan.materializations.len(), 2);
    }

    // A filtered denominator is a typed residual while its summary sibling remains installed.
    #[test]
    fn composable_binary_retains_summary_sibling_of_prometheus_filtered_subtree() {
        use crate::query_plan::{logical::LogicalOperator, QueryPlanNode};
        let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
        entry.query =
            Query("sum(sum_over_time(a[1m])) / sum(sum_over_time(b{job!=\"x\"}[5m]))".into());
        entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
        let (request, env) = snapshot.planning_request().unwrap();
        let plan = PhysicalCompiler.compile(request, env).unwrap();
        let query = plan.query_plan.entries.values().next().unwrap();
        let bindings = query.materialization_bindings();
        assert_eq!(bindings.len(), 1);
        let identity = &plan.summary_catalog.materializations[&bindings[0].materialization];
        let data = &plan.summary_catalog.data_descriptors[&identity.data_descriptor_id];
        assert_eq!(
            (data.time_series_metric().unwrap(), bindings[0].window_ms),
            ("a", 60_000)
        );
        assert!(!query
            .nodes
            .values()
            .any(|node| matches!(node, QueryPlanNode::ExactFallback { .. })));
        assert!(query.nodes.values().any(|node| matches!(node,
            QueryPlanNode::Logical { operator: LogicalOperator::ExactSubquery { query }, .. }
            if query == "sum_over_time(b{job!=\"x\"}[5m])" || query == "sum(sum_over_time(b{job!=\"x\"}[5m]))")));
        assert!(!query.nodes.values().any(|node| matches!(
            node,
            QueryPlanNode::Logical {
                operator: LogicalOperator::Scan { .. },
                ..
            }
        )));
        assert_eq!(plan.precompute_plan.materializations.len(), 1);
    }

    #[test]
    fn warm_leaf_contract_preserves_prometheus_matchers_and_rejects_offsets() {
        for query in [
            "sum_over_time(m{job=\"a\"}[1m])",
            "sum_over_time(m{job!=\"a\"}[1m])",
            "sum_over_time(m{job=~\"a.*\"}[1m])",
            "sum_over_time(m{job!~\"a.*\"}[1m])",
        ] {
            let request = request("scope", query);
            let selected = collect_selected_materializations(&request.queries[0].post_asap, false);
            let selected = selected.unwrap();
            assert_eq!(selected.len(), 1, "{query}");
            assert!(!selected[0].spatial_filter.is_empty(), "{query}");
        }
        let request = request("scope", "sum_over_time(m[1m] offset 1h)");
        let selected = collect_selected_materializations(&request.queries[0].post_asap, false);
        assert!(selected.is_err() || selected.unwrap().is_empty());
    }

    // Unsupported producer filters/multiple sources retain executable native fallback.
    #[test]
    fn snapshot_accepts_filtered_and_multisource_exact_fallback() {
        for query in [
            "sum(rate(http_requests_total{job=\"order-service\"}[5m]))",
            "sum(rate(a[5m])) / sum(rate(b[5m]))",
            "sum(avg_over_time(m{job=~\".+\"}[6h]))",
            "sum(rate(m[5m] offset 1h))",
        ] {
            let mut snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
                "../../../docs/examples/asapquery-planning-snapshot.json"
            ))
            .unwrap();
            let entry = &mut snapshot.query_workload.repeating_queries.as_mut().unwrap()[0];
            entry.query = Query(query.into());
            entry.requirements.accuracy = AccuracyRequirement::Explicit(AccuracyTarget::Exact);
            let (mut request, environment) = snapshot.planning_request().unwrap();
            request = super::super::workload_cost::with_exact_alternative(request)
                .unwrap()
                .pop()
                .unwrap();
            let bundle = PhysicalCompiler.compile(request, environment).unwrap();
            assert!(bundle.precompute_plan.materializations.is_empty());
            assert!(bundle.query_plan.entries.values().all(|entry| matches!(
                entry.nodes[&entry.root],
                crate::query_plan::QueryPlanNode::ExactFallback { .. }
            )));
        }
    }

    // Projections reference one immutable snapshot and reject drift or foreign state.
    #[test]
    fn catalog_projection_rejects_missing_stale_and_foreign_references() {
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_str(include_str!(
            "../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        let bundle = snapshot.compile().unwrap();
        let catalog = &bundle.summary_catalog;
        let mut transmission = bundle.transmission_plan.clone();
        transmission.validate_against_catalog(catalog).unwrap();
        assert_eq!(
            transmission.summary_catalog.as_ref(),
            Some(&catalog.reference().unwrap())
        );
        transmission
            .summary_catalog
            .as_mut()
            .unwrap()
            .snapshot_sha256
            .push('0');
        assert!(transmission.validate_against_catalog(catalog).is_err());
        assert!(transmission.validate(&bundle.precompute_plan).is_err());
        transmission.summary_catalog = None;
        assert!(transmission.validate_against_catalog(catalog).is_err());
        let mut collector = CollectorPlan {
            summary_catalog: Some(catalog.reference().unwrap()),
            collector_id: "collector-test".into(),
            envelope: bundle.envelope.clone(),
            materializations: vec![],
            transmission_rules: vec![],
        };
        collector.validate_against_catalog(catalog).unwrap();
        collector.envelope.plan_version += 1;
        assert!(collector.validate_against_catalog(catalog).is_err());
        assert!(validate_catalog_projection(
            Some(&catalog.reference().unwrap()),
            &bundle.envelope,
            [asap_types::PolicyFingerprint(u64::MAX).into()],
            catalog
        )
        .is_err());
        let encoded = serde_json::to_value(&collector).unwrap();
        assert!(encoded["summary_catalog"]
            .get("summary_descriptors")
            .is_none());
        for actual in &bundle.collector_plans {
            actual.validate_against_catalog(catalog).unwrap();
            assert_eq!(
                actual.summary_catalog,
                bundle.transmission_plan.summary_catalog
            );
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
        entries[0].query = Query("sum(sum_over_time(m[1m]))".into());
        let mut second = entries[0].clone();
        second.query = Query("sum(sum_over_time(m[1m])) * 2".into());
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
            .map(|binding| binding.materialization.fingerprint())
            .collect::<BTreeSet<_>>();
        assert_eq!(bindings.len(), 1);
        query_plan.validate(&bindings).unwrap();
    }

    #[test]
    fn precompute_catalog_validates_without_backend_projection() {
        let bundle = PhysicalCompiler
            .compile(
                request("catalog", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .unwrap();
        let original = &bundle.precompute_plan;
        let catalog = &bundle.summary_catalog;
        let roundtrip: PrecomputePlan =
            serde_json::from_slice(&serde_json::to_vec(original).unwrap()).unwrap();
        roundtrip.validate_against_catalog(catalog).unwrap();
        let reject =
            |mutated: PrecomputePlan| assert!(mutated.validate_against_catalog(catalog).is_err());
        let mut bad = original.clone();
        bad.schemas[0].source = planner_types::pre_asap::Source::TimeSeries {
            metric: "other".into(),
        };
        reject(bad);
        let mut bad = original.clone();
        bad.schemas[0].value_column = planner_types::pre_asap::ColumnRef::Named("other".into());
        reject(bad);
        let mut bad = original.clone();
        bad.schemas[0].group_by.push("other".into());
        reject(bad);
        let mut bad = original.clone();
        bad.schemas[0].window.size_ms += 1;
        reject(bad);
        let mut bad = original.clone();
        bad.materializations[0].num_aggregates_to_retain = Some(0);
        reject(bad);
        let mut bad = original.clone();
        bad.summary_catalog
            .as_mut()
            .unwrap()
            .snapshot_sha256
            .push('0');
        reject(bad);
        let mut corrupt_catalog = catalog.clone();
        corrupt_catalog.data_descriptors.clear();
        assert!(original.validate_against_catalog(&corrupt_catalog).is_err());
        let mut legacy = original.clone();
        legacy.summary_catalog = None;
        legacy.validate().unwrap();
        assert!(legacy.validate_against_catalog(catalog).is_err());
    }

    #[test]
    fn publication_is_catalog_authoritative_and_round_trips() {
        let bundle = PhysicalCompiler
            .compile(
                request("publication", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .unwrap();
        let publication = bundle.publication().unwrap();
        let json = serde_json::to_value(&publication).unwrap();
        let mut decoded: super::super::publication::PhysicalPlanPublication =
            serde_json::from_value(json).unwrap();
        decoded.validate().unwrap();
        decoded.collector_plans.clear();
        assert!(decoded.validate().is_err());
        let mut decoded = publication.clone();
        decoded.summary_catalog.plan_version += 1;
        assert!(decoded.validate().is_err());
        let mut decoded = publication;
        decoded
            .collector_plans
            .push(decoded.collector_plans[0].clone());
        assert!(decoded.validate().is_err());
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
        bundle.summary_catalog.validate().expect("catalog contract");
        assert_eq!(bundle.summary_catalog.plan_id, bundle.envelope.plan_id);
        assert_eq!(
            bundle.summary_catalog.plan_version,
            bundle.envelope.plan_version
        );
        assert_eq!(
            bundle
                .summary_catalog
                .materializations
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            bundle
                .precompute_plan
                .materializations
                .iter()
                .map(|config| config.policy_fingerprint().into())
                .collect::<BTreeSet<_>>()
        );
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
        assert_eq!(bundle.summary_catalog.materializations.len(), 1);
        assert_eq!(
            bundle
                .query_plan
                .entries
                .values()
                .flat_map(QueryPlanEntry::materialization_bindings)
                .count(),
            1
        );
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
                    .precompute_plan
                    .materializations
                    .iter()
                    .map(asap_types::PrecomputeMaterialization::policy_fingerprint)
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
            assert_eq!(plan.materializations[0].slide_secs, 60);
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
    fn backend_local_hll_and_envelope_ingest_are_supported() {
        let bundle = PhysicalCompiler
            .compile(
                request("q", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .unwrap();
        let mut materializations = bundle.precompute_plan.materializations;
        materializations[0].aggregation_type = asap_types::AggregationType::HLL;
        materializations[0].parameters.clear();
        let mut envelope_plan = PrecomputePlan::build(
            bundle.envelope.clone(),
            materializations.clone(),
            &["collector".into()],
        )
        .unwrap();
        envelope_plan.ingest = IngestContract {
            protocol: IngestProtocol::PrometheusRemoteWriteV1,
            endpoint_path: "/api/v1/write".into(),
            timestamp_unit: TimestampUnit::UnixMilliseconds,
            require_plan_identity: false,
            require_summary_definition_identity: false,
            require_registered_producer: false,
        };
        envelope_plan.producers.clear();
        envelope_plan.validate().unwrap();
        PrecomputePlan::build_backend_local(bundle.envelope, materializations).unwrap();
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
        )
        .expect("backend-local contract");

        assert_eq!(
            plan.ingest.protocol,
            IngestProtocol::PrometheusRemoteWriteV1
        );
        assert_eq!(plan.ingest.endpoint_path, "/api/v1/write");
        assert_eq!(plan.ingest.timestamp_unit, TimestampUnit::UnixMilliseconds);
        assert!(plan.producers.is_empty());
        plan.validate().expect("valid backend-local projection");
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
                query: Query("sum(sum_over_time(m[1m]))".into()),
                demand: RepeatedDemand::FixedIntervalAt {
                    interval: RepetitionInterval(10_000),
                    evaluation_phase: planner_types::workload::TimestampMs(7_000),
                },
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
        let template = request("template", "sum(sum_over_time(m[1m]))")
            .queries
            .remove(0);
        let snapshot = BackendLocalPlanningSnapshot {
            snapshot_version: 1,
            workload_cost_evidence: None,
            query_workload,
            data_workload,
            implementation: BackendLocalImplementation {
                window_candidates: HashMap::new(),
                lifecycle_costs: template.lifecycle.costs,
                evidence_observed_at_unix_ms: 9_500,
                evidence_valid_for_ms: 60_000,
                horizon_seconds: 300.0,
                window_implementation_id: "backend-tumbling-v1".into(),
                implementation_cost: template.window_implementations[0].cost.clone(),
                source_sample_interval_ms: None,
                query_staleness_margin_ms: 0,
                max_retained_summary_bytes: DEFAULT_RETAINED_SUMMARY_MEMORY_BUDGET_BYTES,
                topk_evidence: HashMap::new(),
                exact_composition_costs: HashMap::new(),
                erp: None,
            },
            environment,
        };
        assert_eq!(
            snapshot
                .clone()
                .planning_request()
                .unwrap()
                .0
                .query_workload
                .as_ref(),
            Some(&snapshot.query_workload)
        );
        let first = snapshot
            .clone()
            .compile()
            .expect("first deterministic plan");
        let second = snapshot
            .clone()
            .compile()
            .expect("second deterministic plan");
        assert_eq!(first.envelope, second.envelope);
        assert_eq!(first.summary_catalog, second.summary_catalog);
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
        let config = &bundle.precompute_plan.materializations[0];
        assert_eq!(config.pane_origin_ms, Some(7_000));
        assert_eq!(
            bundle.precompute_plan.schemas[0].window.pane_origin_ms,
            Some(7_000)
        );
        let definition = &bundle.summary_catalog.materializations
            [&asap_types::sds::SummaryDefinitionId::from(config.policy_fingerprint())];
        assert_eq!(definition.pane_origin_ms, Some(7_000));
        assert_eq!(
            bundle
                .query_plan
                .entries
                .values()
                .next()
                .unwrap()
                .materialization_bindings()[0]
                .pane_origin_ms,
            Some(7_000)
        );
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
                "sum(sum_over_time(m[1m]))",
                crate::query_plan::ExactReadout::Sum,
            ),
        ] {
            let mut deployment = environment(10_000);
            deployment.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            deployment.collector_ids.clear();
            let compiled = PhysicalCompiler.compile(request(query_id, promql), deployment);
            let plan = compiled.unwrap_or_else(|error| panic!("{promql} must compile: {error}"));
            assert_eq!(plan.summary_catalog.materializations.len(), 1, "{promql}");
            assert_eq!(plan.query_plan.entries.len(), 1, "{promql}");
            assert!(plan.collector_plans.is_empty(), "{promql}");
            let entry = plan.query_plan.entries.values().next().unwrap();
            assert!(matches!(
                entry.nodes.values().find(|node| matches!(node, crate::query_plan::QueryPlanNode::ExactReadout { .. })),
                Some(crate::query_plan::QueryPlanNode::ExactReadout { readout, .. })
                    if *readout == expected_readout
            ));
            if expected_readout == crate::query_plan::ExactReadout::Rate {
                let materialization = plan.precompute_plan.materializations.first().unwrap();
                assert_eq!(
                    materialization.accumulator_spec().unwrap().family,
                    SummaryFamilyType::ExactAggregate(
                        planner_types::post_asap::ExactKind::Increase,
                        planner_types::post_asap::ExactParams::Increase,
                    )
                );
            }
        }
    }

    #[test]
    fn checked_in_per_entity_snapshot_preserves_native_alternative() {
        let source = include_str!("../../../docs/examples/asapquery-planning-snapshot.json");
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_str(source).expect("strict canonical workload fixture");
        let encoded = serde_json::to_value(&snapshot).expect("canonical snapshot value");
        let fixture: serde_json::Value = serde_json::from_str(source).expect("fixture JSON");
        assert_eq!(encoded, fixture);

        snapshot
            .clone()
            .compile()
            .expect("unquoted v1 compatibility startup remains available");
        let (local, env) = snapshot.clone().planning_request().unwrap();
        assert!(PhysicalCompiler
            .compile(local, env)
            .unwrap_err()
            .to_string()
            .contains("native residual substitution requires an exact selected value"));
        let (request, environment) = snapshot.planning_request().unwrap();
        let native = crate::physical::workload_cost::with_exact_alternative(request)
            .unwrap()
            .pop()
            .unwrap();
        let plan = PhysicalCompiler
            .compile(native, environment)
            .expect("native fixture compiles");
        assert!(plan.precompute_plan.materializations.is_empty());
        assert!(plan.collector_plans.is_empty());
        assert!(plan.transmission_plan.rules.is_empty());
        assert_eq!(
            plan.precompute_plan.ingest.protocol,
            IngestProtocol::PrometheusRemoteWriteV1
        );
        assert_eq!(plan.query_plan.entries.len(), 1);
    }

    #[test]
    fn compatibility_demo_preserves_complete_native_query_matrix() {
        let source =
            include_str!("../../../docs/examples/asapquery-compatibility-demo-snapshot.json");
        let snapshot: BackendLocalPlanningSnapshot =
            serde_json::from_str(source).expect("strict compatibility demo fixture");
        snapshot
            .clone()
            .compile()
            .expect("unquoted v1 compatibility startup remains available");
        let (local, env) = snapshot.clone().planning_request().unwrap();
        assert!(PhysicalCompiler
            .compile(local, env)
            .unwrap_err()
            .to_string()
            .contains("native residual substitution requires an exact selected value"));
        let (request, environment) = snapshot.planning_request().unwrap();
        let native = crate::physical::workload_cost::with_exact_alternative(request)
            .unwrap()
            .pop()
            .unwrap();
        let plan = PhysicalCompiler
            .compile(native, environment)
            .expect("native demo compiles");

        assert!(plan.collector_plans.is_empty());
        assert!(plan.transmission_plan.rules.is_empty());
        assert_eq!(plan.query_plan.entries.len(), 6);
        assert!(plan.precompute_plan.materializations.is_empty());
        for query in [
            "rate(asap_demo_counter_total[5s])",
            "increase(asap_demo_counter_total[5s])",
            "sum(sum_over_time(asap_demo_gauge[5s]))",
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
        assert_eq!(bundle.summary_catalog.materializations.len(), 1);
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
        assert_eq!(bundle.summary_catalog.materializations.len(), 2);
        assert_eq!(bundle.precompute_plan.materializations.len(), 2);
        assert_eq!(
            bundle
                .query_plan
                .entries
                .values()
                .flat_map(QueryPlanEntry::materialization_bindings)
                .count(),
            2
        );
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
                crate::query_plan::QueryPlanNode::ReadMaterialization { binding } => Some(
                    bundle.summary_catalog.data_descriptors[&bundle
                        .summary_catalog
                        .materializations[&binding.materialization]
                        .data_descriptor_id]
                        .time_series_metric()
                        .unwrap(),
                ),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(bound_metrics, BTreeSet::from(["m", "n"]));
    }

    #[test]
    fn precompute_schema_must_match_materialization_semantics() {
        let bundle = PhysicalCompiler
            .compile(
                request("q-quantile", "quantile_over_time(0.99, m[1m])"),
                environment(10_000),
            )
            .unwrap();
        let mut plan = bundle.precompute_plan.clone();
        plan.schemas[0].group_by.push("invented".into());
        assert!(matches!(
            plan.validate(),
            Err(PrecomputePlanError::InvalidSchema { .. })
        ));
    }

    // Concrete candidates with the same framework survive selection; changing
    // their quoted costs changes installed state, not the query's lookback.
    #[test]
    fn pane_sizes_are_selected_by_cost_without_changing_semantic_window() {
        for (small_cost, expected_pane_secs, expected_id) in
            [(0.1, 10, "small"), (10.0, 60, "large")]
        {
            let mut request = request("q", "sum(sum_over_time(m[1m]))");
            let query = &mut request.queries[0];
            let mut small = query.window_implementations[0].clone();
            small.implementation_id = "small".into();
            small.layout = asap_types::WindowMaterializationLayout::Pane { pane_secs: 10 };
            small.cost.weighted_cost = small_cost;
            query.window_implementations[0].implementation_id = "large".into();
            query.window_implementations.push(small);
            let mut env = environment(10_000);
            env.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            env.collector_ids.clear();
            let bundle = PhysicalCompiler.compile(request, env).unwrap();
            let materialization = bundle.precompute_plan.materializations.first().unwrap();
            assert_eq!(materialization.window_size, 60);
            assert_eq!(
                materialization.window_layout.base_pane_secs(),
                expected_pane_secs
            );
            let entry = bundle
                .query_plan
                .lookup("sum(sum_over_time(m[1m]))")
                .unwrap();
            assert_eq!(entry.instant.lookback_ms, 60_000);
            assert_eq!(
                entry.materialization_bindings()[0].window_ms,
                expected_pane_secs * 1000
            );
            assert_eq!(
                bundle.lifecycle_estimates[0].window_implementation_id,
                expected_id
            );
        }
    }

    #[test]
    fn sliding_layout_cost_selects_query_or_update_optimized_state() {
        for (full_cost, expected_id, full_selected) in [
            (0.1, "full-window", true),
            (100.0, "mergeable-panes", false),
        ] {
            let mut request = request("q", "sum(sum_over_time(m[1m]))");
            let query = &mut request.queries[0];
            let mut panes = query.window_implementations[0].clone();
            panes.implementation_id = "mergeable-panes".into();
            panes.framework = SummaryWindowFramework::Sliding;
            panes.slide_secs = 10;
            panes.layout = asap_types::WindowMaterializationLayout::Pane { pane_secs: 10 };
            panes.cost.weighted_cost = 1.0;
            panes.cost.cpu_cost = 0.2;
            panes.cost.storage_bytes = 1_024;

            let mut full = panes.clone();
            full.implementation_id = "full-window".into();
            full.layout = asap_types::WindowMaterializationLayout::FullWindow;
            full.cost.weighted_cost = full_cost;
            // Full windows spend more update CPU and retained bytes, while
            // avoiding query-time pane merges. The provider's weighted quote
            // includes the workload's measured read frequency and cardinality.
            full.cost.cpu_cost = 20.0;
            full.cost.storage_bytes = 64 * 1_024;
            query.window_implementations = vec![panes, full];

            let mut env = environment(10_000);
            env.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
            env.collector_ids.clear();
            let bundle = PhysicalCompiler.compile(request, env).unwrap();
            assert_eq!(
                bundle.lifecycle_estimates[0].window_implementation_id,
                expected_id
            );
            assert_eq!(
                matches!(
                    bundle.precompute_plan.materializations[0].window_layout,
                    asap_types::WindowMaterializationLayout::FullWindow
                ),
                full_selected
            );
            assert_eq!(bundle.precompute_plan.materializations[0].window_size, 60);
            assert_eq!(
                bundle.precompute_plan.materializations[0].slide_interval,
                10
            );
        }
    }

    // Distinct semantic windows remain distinct definitions even when they use
    // an equal base-pane width and implementation provider.
    #[test]
    fn selected_panes_reject_unpriced_cross_cohort_coalescing() {
        let mut workload = request("q20", "sum(sum_over_time(m[20s]))");
        let mut second = request("q40", "sum(sum_over_time(m[40s]))")
            .queries
            .remove(0);
        workload.queries[0].window_secs = 20;
        workload.queries[0].window_implementations[0].window_secs = 20;
        second.window_secs = 40;
        second.lifecycle.evaluation_interval_ms = 20_000;
        second.window_implementations[0].window_secs = 40;
        workload.queries.push(second);
        for query in &mut workload.queries {
            query.window_implementations[0].framework = SummaryWindowFramework::Sliding;
            query.window_implementations[0].slide_secs = 10;
            query.window_implementations[0].layout =
                asap_types::WindowMaterializationLayout::Pane { pane_secs: 10 };
            query.window_implementations[0].implementation_id = "shared-ten-second-pane".into();
        }
        let mut env = environment(10_000);
        env.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        env.collector_ids.clear();
        let bundle = PhysicalCompiler.compile(workload, env).unwrap();
        assert_eq!(bundle.precompute_plan.materializations.len(), 2);
    }

    // Non-divisor panes cannot reconstruct a lookback from whole states.
    #[test]
    fn tumbling_sizes_reject_non_divisors() {
        let mut request = request("q", "sum(sum_over_time(m[1m]))");
        request.queries[0].window_implementations[0].layout =
            asap_types::WindowMaterializationLayout::Pane { pane_secs: 7 };
        let mut env = environment(10_000);
        env.target = PhysicalDeploymentTarget::BackendLocalRemoteWrite;
        assert!(PhysicalCompiler.compile(request, env).is_err());
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
        assert_eq!(bundle.summary_catalog.materializations.len(), 1);
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
