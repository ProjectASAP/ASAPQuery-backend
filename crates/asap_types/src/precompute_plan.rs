//! Shared precompute installation contract and catalog consistency checks.
//! Compilation chooses these values; runtime consumers validate the same DTO.

mod catalog;

use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryFamilyType};
use planner_types::pre_asap::Source;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Validate the physical windows consumed by one derived input program.
/// This returns the existing source definitions, not a second serialized
/// contract. It does not prove completion or authorize runtime execution.
/// Full-window sliding is lossless here; pane merging requires a separate
/// explicit operator and is deliberately not inferred from window sizes.
pub fn validated_source_window_cohort<'a>(
    target: &crate::PrecomputeMaterialization,
    sources: &[&'a crate::PrecomputeMaterialization],
) -> Result<Vec<&'a crate::PrecomputeMaterialization>, PrecomputePlanError> {
    let invalid = || {
        PrecomputePlanError::CatalogContract(
            "derived inputs require matching explicit full stored windows".into(),
        )
    };
    let full_ms = target.window_size.checked_mul(1000).ok_or_else(invalid)?;
    if sources.is_empty()
        || full_ms == 0
        || target.slide_interval == 0
        || target.slide_interval > target.window_size
        || (target.slide_interval < target.window_size && target.pane_origin_ms.is_none())
        || target.stored_window_ms() != full_ms
        || (target.slide_interval < target.window_size
            && !matches!(
                target.window_layout,
                crate::WindowMaterializationLayout::FullWindow
            ))
    {
        return Err(invalid());
    }
    let mut identities = BTreeSet::new();
    for source in sources {
        if source.derived_input.is_some()
            || !identities.insert(source.policy_fingerprint())
            || source.window_size != target.window_size
            || source.slide_interval != target.slide_interval
            || source.pane_origin_ms != target.pane_origin_ms
            || source.stored_window_ms() != full_ms
            || (source.slide_interval < source.window_size
                && !matches!(
                    source.window_layout,
                    crate::WindowMaterializationLayout::FullWindow
                ))
        {
            return Err(invalid());
        }
    }
    Ok(sources.to_vec())
}

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
        Self::build_complete(
            envelope,
            materializations,
            producer_ids,
            false,
            BTreeMap::new(),
        )
    }

    fn build_complete(
        envelope: PlanEnvelope,
        materializations: Vec<crate::PrecomputeMaterialization>,
        producer_ids: &[String],
        backend_local: bool,
        executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
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
            ingest: if backend_local {
                IngestContract {
                    protocol: IngestProtocol::PrometheusRemoteWriteV1,
                    endpoint_path: "/api/v1/write".into(),
                    timestamp_unit: TimestampUnit::UnixMilliseconds,
                    require_plan_identity: false,
                    require_summary_definition_identity: false,
                    require_registered_producer: false,
                }
            } else {
                IngestContract {
                    protocol: IngestProtocol::ModifiedOtlpMetricsV1,
                    endpoint_path: "/v1/metrics".into(),
                    timestamp_unit: TimestampUnit::UnixNanoseconds,
                    require_plan_identity: true,
                    require_summary_definition_identity: true,
                    require_registered_producer: true,
                }
            },
            schemas,
            producers,
            materializations,
            executable_dags,
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
        Self::build_backend_local_with_dags(envelope, materializations, BTreeMap::new())
    }

    /// Construct the complete backend-local contract before validation. Derived
    /// definitions are never validated without their actual executable bindings.
    pub fn build_backend_local_with_dags(
        envelope: PlanEnvelope,
        materializations: Vec<crate::PrecomputeMaterialization>,
        executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
    ) -> Result<Self, PrecomputePlanError> {
        Self::build_complete(envelope, materializations, &[], true, executable_dags)
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
        for config in &self.materializations {
            let Some(derived) = &config.derived_input else {
                continue;
            };
            let invalid = || {
                PrecomputePlanError::CatalogContract(
                    "derived summary input requires an installed aligned immutable maintenance DAG"
                        .into(),
                )
            };
            if self.ingest.protocol != IngestProtocol::PrometheusRemoteWriteV1
                || derived.inputs.len() != 1
            {
                return Err(invalid());
            }
            let source_id = *derived.inputs.first().unwrap();
            let source = self
                .materializations
                .iter()
                .find(|candidate| candidate.policy_fingerprint() == source_id.fingerprint())
                .ok_or_else(invalid)?;
            validated_source_window_cohort(config, &[source])?;
            // Current installed runtime capability remains nonoverlapping.
            // The shared cohort contract also describes explicit full-window
            // sliding for consumers which separately prove its completion.
            if source.window_size != source.slide_interval {
                return Err(invalid());
            }
            let mut matched = false;
            for installed in self.executable_dags.values() {
                let dag = installed
                    .document
                    .decode()
                    .map_err(PrecomputePlanError::CatalogContract)?;
                for sink in &installed.binding.precompute_sinks {
                    if !matches!(installed.binding.node(*sink),
                        Some(crate::executable_plan::BackendNodeBinding::Materialization { summary_definition })
                        if summary_definition.fingerprint() == config.policy_fingerprint())
                    {
                        continue;
                    }
                    let inputs: Vec<_> = dag
                        .edges
                        .iter()
                        .filter(|edge| edge.consumer == *sink)
                        .collect();
                    let [edge] = inputs.as_slice() else {
                        return Err(invalid());
                    };
                    let frontiers = installed.binding.nodes.iter().filter_map(|(node,binding)| {
                        matches!(binding, crate::executable_plan::BackendNodeBinding::Materialization { summary_definition }
                            if *summary_definition == source_id).then_some((*node,source_id))
                    }).collect();
                    let actual = crate::derived_input::DerivedInputIdentity::from_dag(
                        &installed.document,
                        edge.producer,
                        &frontiers,
                    )
                    .map_err(PrecomputePlanError::CatalogContract)?;
                    if &actual != derived {
                        return Err(invalid());
                    }
                    let input = dag
                        .nodes
                        .iter()
                        .find(|node| node.id == edge.producer)
                        .ok_or_else(invalid)?;
                    if !matches!(
                        input.payload,
                        planner_types::post_asap::ExecutableOperatorPayload::Value {
                            operation:
                                planner_types::post_asap::ValueOperation::FinalizeExactAccumulator,
                            timing: planner_types::post_asap::ExecutionTiming::MaintenanceTime,
                        }
                    ) {
                        return Err(invalid());
                    }
                    matched = true;
                }
            }
            if !matched {
                return Err(invalid());
            }
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
                if self.ingest.protocol == IngestProtocol::PrometheusRemoteWriteV1
                    && matches!(
                        config.aggregation_type,
                        crate::AggregationType::HLL | crate::AggregationType::UnivMon
                    )
                {
                    if let planner_types::post_asap::ExecutableOperatorPayload::SummaryAgg {
                        input,
                        ..
                    } = &node.payload
                    {
                        let supported = match config.aggregation_type {
                            crate::AggregationType::HLL => {
                                crate::accumulator_spec::is_scalar_sample_value(input)
                                    || crate::accumulator_spec::is_unit_sample_frequency(input)
                            }
                            crate::AggregationType::UnivMon => {
                                crate::accumulator_spec::is_unit_sample_frequency(input)
                            }
                            _ => unreachable!(),
                        };
                        if !supported {
                            return Err(PrecomputePlanError::CatalogContract(
                                "raw materialization input does not match its accumulator update semantics".into(),
                            ));
                        }
                    }
                }
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
            if materialization.table_name.is_none()
                && !materialization.grouping_labels.is_legacy_labels()
            {
                return Err(PrecomputePlanError::CatalogContract(
                    "time-series grouping requires non-null string labels".into(),
                ));
            }

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

#[cfg(test)]
mod source_window_cohort_tests {
    use super::*;
    fn full_window() -> crate::PrecomputeMaterialization {
        serde_json::from_value(serde_json::json!({
            "aggregation_type":"Sum", "aggregation_sub_type":"", "parameters":{},
            "grouping_labels":{"labels":[]}, "aggregated_labels":{"labels":[]},
            "rollup_labels":{"labels":[]}, "original_yaml":"",
            "window_size":60, "slide_interval":10, "window_type":"sliding",
            "window_layout":{"kind":"full_window"}, "pane_origin_ms":0,
            "spatial_filter":"", "spatial_filter_normalized":"", "metric":"m",
            "num_aggregates_to_retain":null, "table_name":null, "value_projection":null
        }))
        .unwrap()
    }
    #[test]
    fn full_sliding_cohort_preserves_explicit_windows_and_identity() {
        let target = full_window();
        let source = full_window();
        let mut missing_origin = source.clone();
        missing_origin.pane_origin_ms = None;
        assert!(validated_source_window_cohort(&missing_origin, &[&missing_origin]).is_err());
        missing_origin.slide_interval = missing_origin.window_size;
        assert!(validated_source_window_cohort(&missing_origin, &[&missing_origin]).is_ok());
        let result = validated_source_window_cohort(&target, &[&source]).unwrap();
        assert!(std::ptr::eq(result[0], &source));
        let mut other = source.clone();
        other.metric = "other".into();
        assert_eq!(
            validated_source_window_cohort(&target, &[&source, &other])
                .unwrap()
                .len(),
            2
        );
        assert!(validated_source_window_cohort(&target, &[&source, &source]).is_err());
        for mutation in 0..3 {
            let mut changed = source.clone();
            match mutation {
                0 => changed.pane_origin_ms = Some(1),
                1 => changed.slide_interval = 20,
                _ => {
                    changed.window_layout =
                        crate::WindowMaterializationLayout::Pane { pane_secs: 10 }
                }
            }
            assert_ne!(source.policy_fingerprint(), changed.policy_fingerprint());
            assert!(validated_source_window_cohort(&target, &[&changed]).is_err());
        }
    }
}
