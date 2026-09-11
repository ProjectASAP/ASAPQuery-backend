//! Shared precompute installation contract and catalog consistency checks.
//! Compilation chooses these values; runtime consumers validate the same DTO.

mod catalog;

use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryFamilyType};
use planner_types::pre_asap::Source;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

pub const BACKEND_COMPAT: &str = "asap-query-backend.v1";

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

/// Backend-side materialization projection consumed by the streaming
/// precompute engine. This is deliberately config-driven: it contains no
/// PromQL string or ad-hoc scheduler job. The aggregation definitions are
/// emitted to `/api/v1/streaming-config`, where the runtime matches incoming
/// series, maintains windows, and writes content-addressed materializations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputePlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<crate::sds::CatalogGeneration>,
    pub envelope: PlanEnvelope,
    pub ingest: IngestContract,
    pub schemas: Vec<StateSchemaContract>,
    pub producers: Vec<ProducerContract>,
    pub materializations: Vec<crate::PrecomputeMaterialization>,
    /// Planner semantic DAGs and backend-owned placement for this generation.
    /// Empty only for legacy/config-only construction paths.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
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
    pub materialization: crate::sds::SummaryDefinitionId,
    pub family: StateFamilyContract,
    pub source: Source,
    #[serde(
        alias = "value_column",
        deserialize_with = "crate::sds::deserialize_state_value_projection"
    )]
    pub value_projection: crate::sds::ValueProjectionIdentity,
    pub group_by: crate::GroupingProjection,
    pub window: StateWindowContract,
    pub encodings: Vec<StateEncoding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StateWindowContract {
    pub kind: crate::WindowKind,
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
    pub materialization: crate::sds::SummaryDefinitionId,
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
        materializations: Vec<crate::PrecomputeMaterialization>,
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
                let value_projection = materialization.effective_value_projection().clone();
                Ok(StateSchemaContract {
                    schema_id: state_schema_id(fingerprint),
                    schema_version: 1,
                    materialization: fingerprint.into(),
                    family,
                    source,
                    value_projection,
                    group_by: materialization.grouping_labels.clone(),
                    window: StateWindowContract {
                        kind: materialization.window_type,
                        size_ms: materialization.window_size.saturating_mul(1_000),
                        slide_ms: match materialization.window_type {
                            crate::WindowKind::Tumbling => None,
                            crate::WindowKind::Sliding => {
                                Some(materialization.slide_interval.saturating_mul(1_000))
                            }
                            crate::WindowKind::Session => None,
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
        materializations: Vec<crate::PrecomputeMaterialization>,
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
    ) -> Result<HashMap<u64, crate::PrecomputeMaterialization>, PrecomputePlanError> {
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
            let dag = installed
                .document
                .decode()
                .map_err(PrecomputePlanError::CatalogContract)?;
            for node in &dag.nodes {
                let Some(crate::executable_plan::BackendNodeBinding::Materialization {
                    summary_definition,
                }) = installed.binding.node(node.id)
                else {
                    continue;
                };
                let Some(config) = self
                    .materializations
                    .iter()
                    .find(|config| config.policy_fingerprint() == summary_definition.fingerprint())
                else {
                    return Err(PrecomputePlanError::CatalogContract(
                        "DAG materialization has no runtime configuration".into(),
                    ));
                };
                if let Some(partitioning) = config.partitioning {
                    if let planner_types::post_asap::ExecutableOperatorPayload::SummaryAgg {
                        reduction,
                        ..
                    } = &node.payload
                    {
                        let expected = match reduction {
                            planner_types::pre_asap::Reduction::PerEntity => {
                                crate::sds::PopulationPartitioning::PerEntity
                            }
                            planner_types::pre_asap::Reduction::Reduce(_) => {
                                crate::sds::PopulationPartitioning::Grouped
                            }
                        };
                        if partitioning != expected {
                            return Err(PrecomputePlanError::CatalogContract(
                                "runtime population partition disagrees with Planner reduction"
                                    .into(),
                            ));
                        }
                    }
                }
            }
        }
        let mut materializations = BTreeSet::new();
        for materialization in &self.materializations {
            materialization
                .grouping_labels
                .validate()
                .map_err(PrecomputePlanError::CatalogContract)?;
            materialization
                .window_layout
                .validate(materialization.window_size, materialization.slide_interval)
                .map_err(|reason| PrecomputePlanError::InvalidWindowLayout {
                    materialization: materialization.policy_fp_u64(),
                    reason,
                })?;
            let expected_kind = if materialization.slide_interval == materialization.window_size {
                crate::WindowKind::Tumbling
            } else {
                crate::WindowKind::Sliding
            };
            if materialization.window_type != expected_kind {
                return Err(PrecomputePlanError::InvalidWindowLayout {
                    materialization: materialization.policy_fp_u64(),
                    reason: "window kind disagrees with size and slide".into(),
                });
            }
            // HLL is supported as an ingested sketch envelope, not as a raw
            // accumulator. Validate here so external installs cannot bypass it.
            if self.ingest.protocol == IngestProtocol::PrometheusRemoteWriteV1
                && materialization.aggregation_type == crate::AggregationType::HLL
            {
                return Err(PrecomputePlanError::UnsupportedFamily(
                    materialization.policy_fp_u64(),
                ));
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
            let value_projection = materialization.effective_value_projection().clone();
            if schema.schema_id != state_schema_id(schema.materialization.fingerprint())
                || schema.family != family
                || schema.source != source
                || schema.value_projection != value_projection
                || schema.group_by != materialization.grouping_labels
                || schema.window.kind != materialization.window_type
                || schema.window.size_ms != materialization.window_size.saturating_mul(1_000)
                || schema.window.slide_ms
                    != match materialization.window_type {
                        crate::WindowKind::Tumbling => None,
                        crate::WindowKind::Sliding => {
                            Some(materialization.slide_interval.saturating_mul(1_000))
                        }
                        crate::WindowKind::Session => None,
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

pub(crate) fn state_schema_id(fingerprint: crate::PolicyFingerprint) -> String {
    format!("{}:summary-state:v1:{}", BACKEND_COMPAT, fingerprint.0)
}

pub(crate) fn state_encodings(family: &SummaryFamilyType) -> Vec<StateEncoding> {
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
