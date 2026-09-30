//! Shared precompute installation contract and catalog consistency checks.
//! Compilation chooses these values; runtime consumers validate the same DTO.

mod catalog;

use planner_types::post_asap::{ExactKind, SketchAlgorithm, SketchParams, SummaryFamilyType};
use planner_types::pre_asap::Source;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

/// Check the initial executable maintenance population reductions against the
/// installed configuration. Indexed reductions need authoritative column mapping
/// and are not admitted by this bounded capability check.
pub fn validate_maintenance_reduction(
    config: &crate::PrecomputeMaterialization,
    node: &planner_types::post_asap::PostAsapDagNode,
) -> Result<(), String> {
    use crate::sds::PopulationPartitioning;
    use planner_types::{post_asap::PostAsapOperatorPayload, pre_asap::Reduction};
    match &node.payload {
        PostAsapOperatorPayload::SummaryAgg {
            reduction: Reduction::PerEntity,
            ..
        } if config.partitioning == Some(PopulationPartitioning::PerEntity) => Ok(()),
        PostAsapOperatorPayload::SummaryAgg {
            reduction: Reduction::Reduce(keys),
            ..
        } if keys.is_empty()
            && config.partitioning == Some(PopulationPartitioning::Grouped)
            && config.grouping_labels.is_empty() =>
        {
            Ok(())
        }
        _ => Err(
            "maintenance reduction does not match supported configured population grouping".into(),
        ),
    }
}

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

/// Installed-plan schema. v2 materializations carry only deployment fields;
/// v1 plans duplicated computation semantics and are rejected on decode.
pub const BACKEND_COMPAT: &str = "asap-query-backend.v2";

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

/// DAG-format precompute installation. Planner node payloads and dependency
/// edges define execution; materializations attach storage/window placement.
/// Raw source-to-SummaryAgg paths lower to streaming kernels. Derived paths
/// execute through the maintenance DAG scheduler at stored-state frontiers.
#[derive(Debug, Clone, Serialize)]
pub struct PrecomputePlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_catalog: Option<crate::sds::CatalogGeneration>,
    pub envelope: PlanEnvelope,
    pub ingest: IngestContract,
    pub schemas: Vec<StateSchemaContract>,
    pub producers: Vec<ProducerContract>,
    pub materializations: Vec<crate::PrecomputeMaterialization>,
    /// Maintenance projections ending at stored outputs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
}

/// Serde mirror of [`PrecomputePlan`]; the compiler checks it stays in sync.
#[derive(Deserialize)]
#[serde(remote = "PrecomputePlan")]
struct PrecomputePlanDef {
    #[serde(default)]
    summary_catalog: Option<crate::sds::CatalogGeneration>,
    envelope: PlanEnvelope,
    ingest: IngestContract,
    schemas: Vec<StateSchemaContract>,
    producers: Vec<ProducerContract>,
    materializations: Vec<crate::PrecomputeMaterialization>,
    #[serde(default)]
    executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
}

impl<'de> Deserialize<'de> for PrecomputePlan {
    /// Check the schema before the body so an older plan fails with its
    /// version rather than with its first unrecognized field.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let value = serde_json::Value::deserialize(deserializer)?;
        let compat = value
            .get("envelope")
            .and_then(|envelope| envelope.get("backend_compat"))
            .and_then(serde_json::Value::as_str);
        if compat != Some(BACKEND_COMPAT) {
            return Err(D::Error::custom(format!(
                "unsupported installed precompute plan schema {}: this backend requires \
                 {BACKEND_COMPAT}; recompile the plan",
                compat.unwrap_or("<missing>")
            )));
        }
        PrecomputePlanDef::deserialize(value).map_err(D::Error::custom)
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_identity: Option<crate::semantic_fragment::LogicalDatasetIdentity>,
    pub protocol: IngestProtocol,
    pub endpoint_path: String,
    pub timestamp_unit: TimestampUnit,
    pub require_plan_identity: bool,
    pub require_stored_output_identity: bool,
    pub require_registered_producer: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum StateEncoding {
    SketchlibProtobufV1,
    SketchCoreMsgpackV1,
    /// Backend-produced typed physical output, distinct from legacy sketch frames.
    NativeBatchV1,
    ExactAccumulatorV1,
    /// Persisted backend state with explicit Planner family and population layout.
    PlannerExactAccumulatorV1,
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
    /// Exact minimum. Distinct state from [`ExactStateKind::Max`] -- a
    /// stored minimum cannot answer a maximum query, so the two must
    /// never share a content address.
    Min,
    /// Exact maximum. Planner still spells this `ExactKind::Max` for
    /// historical reasons; it is a maximum accumulator.
    Max,
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
                    ExactKind::Max => ExactStateKind::Max,
                    ExactKind::Min => ExactStateKind::Min,
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

/// Decoder/schema contract for one stored output. `family` is the stored
/// state's type, which readers need without executing the producer; when a
/// Planner DAG node produces the output, validation requires its family.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StateSchemaContract {
    pub stored_output_reference: crate::sds::StoredOutputReference,
    pub schema_id: String,
    pub schema_version: u32,
    pub materialization: crate::sds::StoredOutputId,
    pub family: SummaryFamilyType,
    pub source: Source,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pane_origin_ms: Option<i64>,
}

/// A collector authorized to produce state for one materialization. Runtime
/// producer epochs and frame sequences belong to TransmissionPlan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ProducerContract {
    pub producer_id: String,
    pub collector_id: String,
    pub materialization: crate::sds::StoredOutputId,
    pub schema_id: String,
    /// Authoritative partitions for completion barriers. An explicitly empty
    /// roster cannot authorize completion claims.
    pub partition_ids: BTreeSet<String>,
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
    /// Each output is paired with its stored state family.
    pub fn build(
        envelope: PlanEnvelope,
        outputs: Vec<(crate::PrecomputeMaterialization, SummaryFamilyType)>,
        producer_ids: &[String],
    ) -> Result<Self, PrecomputePlanError> {
        Self::build_complete(envelope, outputs, producer_ids, false, BTreeMap::new())
    }

    fn build_complete(
        envelope: PlanEnvelope,
        outputs: Vec<(crate::PrecomputeMaterialization, SummaryFamilyType)>,
        producer_ids: &[String],
        backend_local: bool,
        executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
    ) -> Result<Self, PrecomputePlanError> {
        let schemas = outputs
            .iter()
            .map(|(materialization, family)| {
                let fingerprint = materialization.policy_fingerprint();
                if StateFamilyContract::try_from(family).is_err() {
                    return Err(PrecomputePlanError::UnsupportedFamily(fingerprint.0));
                }
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
                    stored_output_reference: crate::sds::StoredOutputReference::for_output(
                        fingerprint.into(),
                    ),
                    schema_id: state_schema_id(fingerprint),
                    schema_version: 1,
                    materialization: fingerprint.into(),
                    family: family.clone(),
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
                    encodings: state_encodings(family),
                })
            })
            .collect::<Result<Vec<_>, PrecomputePlanError>>()?;
        let materializations: Vec<_> = outputs.into_iter().map(|(config, _)| config).collect();
        let producers = producer_ids
            .iter()
            .flat_map(|producer_id| {
                schemas.iter().map(move |schema| ProducerContract {
                    producer_id: producer_id.clone(),
                    collector_id: producer_id.clone(),
                    materialization: schema.materialization,
                    schema_id: schema.schema_id.clone(),
                    partition_ids: BTreeSet::new(),
                })
            })
            .collect();
        let dataset_identity = materializations
            .iter()
            .filter_map(|m| m.semantic_fragment.as_ref()?.dataset_identity.clone())
            .next();
        let plan = Self {
            summary_catalog: None,
            envelope,
            ingest: if backend_local {
                IngestContract {
                    dataset_identity: dataset_identity.clone(),
                    protocol: IngestProtocol::PrometheusRemoteWriteV1,
                    endpoint_path: "/api/v1/write".into(),
                    timestamp_unit: TimestampUnit::UnixMilliseconds,
                    require_plan_identity: false,
                    require_stored_output_identity: false,
                    require_registered_producer: false,
                }
            } else {
                IngestContract {
                    dataset_identity: dataset_identity.clone(),
                    protocol: IngestProtocol::ModifiedOtlpMetricsV1,
                    endpoint_path: "/v1/metrics".into(),
                    timestamp_unit: TimestampUnit::UnixNanoseconds,
                    require_plan_identity: true,
                    require_stored_output_identity: true,
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
        outputs: Vec<(crate::PrecomputeMaterialization, SummaryFamilyType)>,
    ) -> Result<Self, PrecomputePlanError> {
        Self::build_backend_local_with_dags(envelope, outputs, BTreeMap::new())
    }

    /// Construct the complete backend-local contract before validation. Derived
    /// definitions are never validated without their actual executable bindings.
    pub fn build_backend_local_with_dags(
        envelope: PlanEnvelope,
        outputs: Vec<(crate::PrecomputeMaterialization, SummaryFamilyType)>,
        executable_dags: BTreeMap<String, crate::executable_plan::InstalledPostAsapDag>,
    ) -> Result<Self, PrecomputePlanError> {
        Self::build_complete(envelope, outputs, &[], true, executable_dags)
    }

    /// The stored state family of `output`, from its schema contract.
    pub fn state_family(&self, output: crate::sds::StoredOutputId) -> Option<&SummaryFamilyType> {
        self.schemas
            .iter()
            .find(|schema| schema.materialization == output)
            .map(|schema| &schema.family)
    }

    /// The Planner SummaryAgg node that produces `output`, with the source
    /// expression feeding it when that input is a raw source. `None` when no
    /// installed DAG produces the output with a SummaryAgg.
    #[allow(clippy::type_complexity)]
    pub fn summary_producer(
        &self,
        output: crate::sds::StoredOutputId,
    ) -> Result<
        Option<(
            planner_types::post_asap::PostAsapDagNode,
            Option<planner_types::pre_asap::QueryExpr>,
        )>,
        String,
    > {
        use planner_types::post_asap::PostAsapOperatorPayload;
        for installed in self.executable_dags.values() {
            let Some(node) = installed.binding.nodes.iter().find_map(|(node, binding)| {
                matches!(binding, crate::executable_plan::BackendNodeBinding::Materialization { stored_output } if *stored_output == output).then_some(*node)
            }) else {
                continue;
            };
            let dag = installed.document.decode()?;
            let producer = dag
                .nodes
                .iter()
                .find(|candidate| candidate.id == node)
                .ok_or("bound materialization node is absent from its DAG")?;
            if !matches!(producer.payload, PostAsapOperatorPayload::SummaryAgg { .. }) {
                continue;
            }
            let inputs: Vec<_> = dag.edges.iter().filter(|e| e.consumer == node).collect();
            let source = match inputs.as_slice() {
                [edge] => dag
                    .nodes
                    .iter()
                    .find(|candidate| candidate.id == edge.producer)
                    .and_then(|source| match &source.payload {
                        PostAsapOperatorPayload::Fallback { expression } => {
                            Some(expression.clone())
                        }
                        _ => None,
                    }),
                _ => None,
            };
            return Ok(Some((producer.clone(), source)));
        }
        Ok(None)
    }

    /// Canonical population predicate of `config`'s input: the typed table
    /// population, or the label filter of its Planner DAG time-series scan.
    pub fn population_filter(
        &self,
        config: &crate::PrecomputeMaterialization,
    ) -> Result<String, String> {
        let table = config.table_population_canonical()?;
        if config.table_name.is_some() || config.derived_input.is_some() {
            return Ok(table);
        }
        let Some((_, Some(expression))) = self.summary_producer(config.stored_output_id)? else {
            return Ok(String::new());
        };
        let family = self.state_family(config.stored_output_id);
        let (_, _, filter) = raw_time_series_input_contract(
            &expression,
            matches!(family, Some(SummaryFamilyType::ExactAggregate(..))),
        )?;
        Ok(crate::utils::normalize_spatial_filter(&filter))
    }

    pub fn runtime_materializations(
        &self,
    ) -> Result<
        HashMap<crate::sds::StoredOutputId, crate::PrecomputeMaterialization>,
        PrecomputePlanError,
    > {
        self.validate()?;
        Ok(self
            .materializations
            .iter()
            .cloned()
            .map(|materialization| {
                (
                    crate::sds::StoredOutputId(materialization.policy_fp_u64()),
                    materialization,
                )
            })
            .collect())
    }

    /// Validate a barrier's installed scope, not authentication, durability or
    /// monotonic progress. The runtime must enforce those before accepting it.
    pub fn validate_watermark_scope(
        &self,
        materialization: crate::sds::StoredOutputId,
        barrier: &crate::sds::SummaryWatermarkBarrier,
    ) -> Result<(), PrecomputePlanError> {
        self.validate()?;
        barrier
            .validate()
            .map_err(|error| PrecomputePlanError::CatalogContract(error.to_string()))?;
        if self.summary_catalog.as_ref() != Some(&barrier.catalog_generation)
            || self.envelope.plan_id != barrier.catalog_generation.plan_id
            || self.envelope.plan_version != barrier.catalog_generation.plan_version
        {
            return Err(PrecomputePlanError::CatalogContract(
                "watermark catalog generation does not match installed plan".into(),
            ));
        }
        if !self.producers.iter().any(|producer| {
            producer.materialization == materialization
                && producer.producer_id == barrier.source.producer_id
                && producer
                    .partition_ids
                    .contains(&barrier.source.partition_id)
        }) {
            return Err(PrecomputePlanError::CatalogContract(
                "watermark source is outside the authoritative partition roster".into(),
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), PrecomputePlanError> {
        if let Some(dataset) = &self.ingest.dataset_identity {
            dataset
                .validate()
                .map_err(PrecomputePlanError::CatalogContract)?;
        }
        for materialization in &self.materializations {
            if let Some(fragment) = &materialization.semantic_fragment {
                fragment
                    .validate()
                    .map_err(PrecomputePlanError::CatalogContract)?;
                if fragment.dataset_identity != self.ingest.dataset_identity {
                    return Err(PrecomputePlanError::CatalogContract(
                        "semantic dataset differs from installed input binding".into(),
                    ));
                }
            }
        }
        let valid_ingest = match self.ingest.protocol {
            IngestProtocol::ModifiedOtlpMetricsV1 => {
                self.ingest.endpoint_path == "/v1/metrics"
                    && self.ingest.timestamp_unit == TimestampUnit::UnixNanoseconds
                    && self.ingest.require_plan_identity
                    && self.ingest.require_stored_output_identity
                    && self.ingest.require_registered_producer
            }
            IngestProtocol::PrometheusRemoteWriteV1 => {
                self.ingest.endpoint_path == "/api/v1/write"
                    && self.ingest.timestamp_unit == TimestampUnit::UnixMilliseconds
                    && !self.ingest.require_plan_identity
                    && !self.ingest.require_stored_output_identity
                    && !self.ingest.require_registered_producer
            }
        };
        if !valid_ingest {
            return Err(PrecomputePlanError::UnsupportedIngestEndpoint);
        }
        let canonical_cohort_members: BTreeSet<_> = self
            .materializations
            .iter()
            .filter(|config| !config.population_key_encoding.is_legacy())
            .filter_map(|config| config.derived_input.as_ref().map(|input| (config, input)))
            .flat_map(|(config, input)| {
                std::iter::once(config.policy_fingerprint().into())
                    .chain(input.inputs.iter().copied())
            })
            .collect();
        // Decode each DAG at most once; errors still surface only where used.
        let dags = self
            .executable_dags
            .values()
            .map(|installed| (installed, std::cell::OnceCell::new()))
            .collect::<Vec<_>>();
        for config in &self.materializations {
            if !config.population_key_encoding.is_legacy()
                && (self.ingest.protocol != IngestProtocol::PrometheusRemoteWriteV1
                    || !canonical_cohort_members.contains(&config.policy_fingerprint().into()))
            {
                return Err(PrecomputePlanError::CatalogContract(
                    "population key encoding is not supported by the installed runtime".into(),
                ));
            }
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
                || derived.inputs.is_empty()
            {
                return Err(invalid());
            }
            let sources = derived
                .inputs
                .iter()
                .map(|id| {
                    self.materializations
                        .iter()
                        .find(|candidate| candidate.policy_fingerprint() == id.fingerprint())
                        .ok_or_else(invalid)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !config.population_key_encoding.is_legacy() {
                if config.partitioning != Some(crate::sds::PopulationPartitioning::Grouped)
                    || !config.grouping_labels.is_empty()
                    || sources.iter().any(|source| {
                        source.population_key_encoding != config.population_key_encoding
                    })
                {
                    return Err(invalid());
                }
            } else if sources
                .iter()
                .any(|source| !source.population_key_encoding.is_legacy())
            {
                return Err(invalid());
            }
            validated_source_window_cohort(config, &sources)?;
            let mut matched = false;
            for (installed, dag) in &dags {
                let dag = dag
                    .get_or_init(|| installed.document.decode())
                    .as_ref()
                    .map_err(|error| PrecomputePlanError::CatalogContract(error.clone()))?;
                for sink in &installed.binding.precompute_sinks {
                    if !matches!(installed.binding.node(*sink),
                        Some(crate::executable_plan::BackendNodeBinding::Materialization { stored_output })
                        if stored_output.fingerprint() == config.policy_fingerprint())
                    {
                        continue;
                    }
                    let target_node = dag
                        .nodes
                        .iter()
                        .find(|node| node.id == *sink)
                        .ok_or_else(invalid)?;
                    let native = installed
                        .native_program(*sink)
                        .map_err(PrecomputePlanError::CatalogContract)?
                        .ok_or_else(invalid)?;
                    for root in native.roots() {
                        let root = planner_types::post_asap::PostAsapNodeId(
                            u32::try_from(*root).map_err(|_| invalid())?,
                        );
                        let Some(crate::executable_plan::BackendNodeBinding::Materialization {
                            stored_output,
                        }) = installed.binding.node(root)
                        else {
                            return Err(invalid());
                        };
                        let other = self
                            .materializations
                            .iter()
                            .find(|c| c.policy_fingerprint() == stored_output.fingerprint())
                            .ok_or_else(invalid)?;
                        if other.stored_window_ms() != config.stored_window_ms()
                            || other.slide_interval != config.slide_interval
                            || other.pane_origin_ms != config.pane_origin_ms
                            || other.population_key_encoding != config.population_key_encoding
                            || other.derived_input.as_ref().map(|d| &d.inputs)
                                != Some(&derived.inputs)
                            || (native.roots().len() > 1
                                && config.population_key_encoding.is_legacy())
                        {
                            return Err(invalid());
                        }
                    }
                    let native = native.output_contract(u64::from(sink.0))
                        .map_err(|e| PrecomputePlanError::CatalogContract(e.to_string()))
                        .map(|contract| !asap_physical_operators::physical_planner::precompute::is_population_schema(&contract.schema))?
                        .then_some(native);
                    let exact = |config: &crate::PrecomputeMaterialization, kinds: &[ExactKind]| {
                        matches!(self.state_family(config.stored_output_id),
                            Some(SummaryFamilyType::ExactAggregate(kind, _)) if kinds.contains(kind))
                    };
                    let source_kinds: &[ExactKind] = if native.is_some() {
                        &[ExactKind::Sum, ExactKind::Rate]
                    } else {
                        &[ExactKind::Sum]
                    };
                    if sources.iter().any(|source| !exact(source, source_kinds)) {
                        return Err(invalid());
                    }
                    if native.is_none() {
                        if config.window_size != config.slide_interval {
                            return Err(invalid());
                        }
                        validate_maintenance_reduction(config, target_node)
                            .map_err(PrecomputePlanError::CatalogContract)?;
                    } else if !exact(config, &[ExactKind::Sum])
                        && !matches!(self.state_family(config.stored_output_id),
                            Some(SummaryFamilyType::Sketch(kind, _)) if matches!(kind.algorithm(),
                                SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap))
                    {
                        return Err(invalid());
                    }
                    let inputs: Vec<_> = dag
                        .edges
                        .iter()
                        .filter(|edge| edge.consumer == *sink)
                        .collect();
                    let [edge] = inputs.as_slice() else {
                        return Err(invalid());
                    };
                    let frontiers = installed
                        .binding
                        .nodes
                        .iter()
                        .filter_map(|(node, binding)| match binding {
                            crate::executable_plan::BackendNodeBinding::Materialization {
                                stored_output,
                            } if derived.inputs.contains(stored_output) => {
                                Some((*node, *stored_output))
                            }
                            _ => None,
                        })
                        .collect();
                    let actual = crate::derived_input::DerivedInputIdentity::from_dag(
                        &installed.document,
                        edge.producer,
                        &frontiers,
                    )
                    .map_err(PrecomputePlanError::CatalogContract)?;
                    if &actual != derived {
                        return Err(invalid());
                    }
                    let mut pending = vec![edge.producer];
                    let mut visited = BTreeSet::new();
                    while let Some(id) = pending.pop() {
                        if !visited.insert(id) {
                            continue;
                        }
                        let node = dag
                            .nodes
                            .iter()
                            .find(|node| node.id == id)
                            .ok_or_else(invalid)?;
                        let children: Vec<_> = dag
                            .edges
                            .iter()
                            .filter(|edge| edge.consumer == id)
                            .collect();
                        use planner_types::post_asap::{
                            PostAsapOperatorPayload as Payload, ValueOperation,
                        };
                        if node.output_state
                            != planner_types::post_asap::ExecutionDataState::INGESTION_ROWS
                        {
                            return Err(invalid());
                        }
                        match &node.payload {
                            Payload::Value {
                                operation: ValueOperation::FinalizeExactAccumulator,
                            } if children.len() == 1
                                && frontiers.contains_key(&children[0].producer) => {}
                            Payload::Binary { operator }
                                if children.len() == 2
                                    && children
                                        .iter()
                                        .filter(|edge| {
                                            edge.role == planner_types::post_asap::EdgeRole::Left
                                        })
                                        .count()
                                        == 1
                                    && children
                                        .iter()
                                        .filter(|edge| {
                                            edge.role == planner_types::post_asap::EdgeRole::Right
                                        })
                                        .count()
                                        == 1
                                    && operator.vector_match.is_none()
                                    && matches!(
                                        operator.kind,
                                        planner_types::pre_asap::BinaryOpKind::Arithmetic(_)
                                    ) =>
                            {
                                pending.extend(children.iter().map(|edge| edge.producer));
                            }
                            _ => return Err(invalid()),
                        }
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
                    stored_output,
                }) = installed.binding.node(node.id)
                else {
                    continue;
                };
                let Some(config) = self
                    .materializations
                    .iter()
                    .find(|config| config.policy_fingerprint() == stored_output.fingerprint())
                else {
                    return Err(PrecomputePlanError::CatalogContract(
                        "DAG materialization has no runtime configuration".into(),
                    ));
                };
                if let planner_types::post_asap::PostAsapOperatorPayload::SummaryAgg {
                    family,
                    input,
                    ..
                } = &node.payload
                {
                    if self.state_family(config.stored_output_id) != Some(family) {
                        return Err(PrecomputePlanError::CatalogContract(
                            "stored output schema family differs from its Planner producer".into(),
                        ));
                    }
                    let algorithm = match family {
                        SummaryFamilyType::Sketch(kind, _) => Some(kind.algorithm()),
                        _ => None,
                    };
                    let supported = match algorithm {
                        _ if self.ingest.protocol != IngestProtocol::PrometheusRemoteWriteV1 => {
                            true
                        }
                        Some(SketchAlgorithm::Hll) => {
                            crate::accumulator_spec::is_scalar_sample_value(input)
                                || crate::accumulator_spec::is_unit_sample_frequency(input)
                        }
                        Some(SketchAlgorithm::UnivMon) => {
                            crate::accumulator_spec::is_unit_sample_frequency(input)
                        }
                        _ => true,
                    };
                    if !supported {
                        return Err(PrecomputePlanError::CatalogContract(
                            "raw materialization input does not match its accumulator update semantics".into(),
                        ));
                    }
                }
                if let Some(partitioning) = config.partitioning {
                    if let planner_types::post_asap::PostAsapOperatorPayload::SummaryAgg {
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
            if materialization.stored_output_id.as_u64() == 0 {
                return Err(PrecomputePlanError::CatalogContract(
                    "materialization has no allocated stored output id".into(),
                ));
            }
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
        let mut stored_outputs = BTreeSet::new();
        for schema in &self.schemas {
            if schema.schema_id.trim().is_empty()
                || !schema_ids.insert(schema.schema_id.as_str())
                || !stored_outputs.insert(schema.stored_output_reference.stored_output_id)
                || schema.schema_version == 0
                || schema.encodings.is_empty()
                || schema.stored_output_reference.stored_output_id != schema.materialization
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
            if StateFamilyContract::try_from(&schema.family).is_err()
                || crate::aggregation_type_for_family(&schema.family).is_none()
            {
                return Err(PrecomputePlanError::UnsupportedFamily(
                    schema.materialization.as_u64(),
                ));
            }
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
                || !state_encodings_match(&schema.family, &schema.encodings)
            {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
            }
        }
        let mut producers = BTreeSet::new();
        let mut produced = BTreeSet::new();
        for producer in &self.producers {
            if producer.producer_id.trim().is_empty()
                || producer.collector_id.trim().is_empty()
                || producer.partition_ids.iter().any(|id| id.trim().is_empty())
                || !materializations.contains(&producer.materialization)
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

/// Decompose a Planner raw time-series input expression into its metric,
/// optional whole-second range and PromQL label filter.
pub fn raw_time_series_input_contract(
    expr: &planner_types::pre_asap::QueryExpr,
    exact: bool,
) -> Result<(String, Option<u64>, String), String> {
    use planner_types::pre_asap::{CompareOpKind, QueryExpr, ScalarValue};
    let (source, window_secs) = match expr {
        QueryExpr::TimeRange { child, range } => {
            if range.as_millis() == 0 || range.as_millis() % 1000 != 0 {
                return Err("warm producer requires a positive whole-second range".into());
            }
            (child.as_ref(), Some(range.as_secs()))
        }
        QueryExpr::Scan { .. } if exact => {
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

pub(crate) fn state_schema_id(fingerprint: crate::PolicyFingerprint) -> String {
    format!("{}:summary-state:v1:{}", BACKEND_COMPAT, fingerprint.0)
}

// Persisted schemas may declare a subset of formats. Adding a decoder must
// not invalidate existing installed plans that use only older supported codecs.
pub(crate) fn state_encodings_match(
    family: &SummaryFamilyType,
    encodings: &[StateEncoding],
) -> bool {
    let supported = state_encodings(family);
    !encodings.is_empty()
        && encodings.iter().collect::<BTreeSet<_>>().len() == encodings.len()
        && encodings
            .iter()
            .all(|encoding| supported.contains(encoding))
}

pub(crate) fn state_encodings(family: &SummaryFamilyType) -> Vec<StateEncoding> {
    let mut encodings = match family {
        SummaryFamilyType::ExactAggregate(
            planner_types::post_asap::ExactKind::Increase
            | planner_types::post_asap::ExactKind::Rate,
            _,
        ) => vec![
            StateEncoding::ExactCounterAccumulatorV2,
            StateEncoding::PlannerExactAccumulatorV1,
        ],
        SummaryFamilyType::ExactAggregate(..) => vec![
            StateEncoding::ExactAccumulatorV1,
            StateEncoding::PlannerExactAccumulatorV1,
        ],
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
    };
    if matches!(
        family,
        SummaryFamilyType::ExactAggregate(
            planner_types::post_asap::ExactKind::Sum
                | planner_types::post_asap::ExactKind::Count
                | planner_types::post_asap::ExactKind::Min
                | planner_types::post_asap::ExactKind::Max
                | planner_types::post_asap::ExactKind::Rate
                | planner_types::post_asap::ExactKind::Increase,
            _
        )
    ) || matches!(family, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(),
            SketchAlgorithm::Kll | SketchAlgorithm::DDSketch | SketchAlgorithm::Hll |
            SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap))
    {
        encodings.push(StateEncoding::NativeBatchV1);
    }
    encodings
}

#[cfg(test)]
mod source_window_cohort_tests {
    use super::*;
    fn full_window() -> crate::PrecomputeMaterialization {
        let mut config: crate::PrecomputeMaterialization =
            serde_json::from_value(serde_json::json!({
                "stored_output_id":0, "grouping_labels":{"labels":[]},
                "window_size":60, "slide_interval":10, "window_type":"sliding",
                "window_layout":{"kind":"full_window"}, "pane_origin_ms":0, "metric":"m",
                "num_aggregates_to_retain":null, "table_name":null, "value_projection":null
            }))
            .unwrap();
        config.allocate_stored_output_id(&"sum");
        config
    }
    fn sum() -> SummaryFamilyType {
        SummaryFamilyType::ExactAggregate(
            ExactKind::Sum,
            planner_types::post_asap::ExactParams::Sum,
        )
    }
    fn envelope(plan_version: u64) -> PlanEnvelope {
        PlanEnvelope {
            plan_id: 1,
            plan_version,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: BACKEND_COMPAT.into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        }
    }
    // New native-format support must not invalidate persisted older codec subsets.
    #[test]
    fn older_encoding_subsets_remain_valid_and_unknown_codecs_fail() {
        use planner_types::post_asap::{SketchKind, SketchParams};
        let family = SummaryFamilyType::Sketch(
            SketchKind::new(SketchAlgorithm::Kll, SketchParams::Kll { k: 200 }),
            Default::default(),
        );
        assert!(state_encodings_match(
            &family,
            &[
                StateEncoding::SketchlibProtobufV1,
                StateEncoding::SketchCoreMsgpackV1
            ]
        ));
        assert!(state_encodings_match(
            &family,
            &[StateEncoding::NativeBatchV1]
        ));
        assert!(!state_encodings_match(&family, &[]));
        assert!(!state_encodings_match(
            &family,
            &[StateEncoding::ExactAccumulatorV1]
        ));
        assert!(!state_encodings_match(
            &family,
            &[StateEncoding::NativeBatchV1, StateEncoding::NativeBatchV1]
        ));
    }

    #[test]
    fn producer_roster_roundtrip_and_watermark_scope() {
        let mut config = full_window();
        config.slide_interval = config.window_size;
        config.window_type = crate::WindowKind::Tumbling;
        let mut plan =
            PrecomputePlan::build_backend_local(envelope(2), vec![(config, sum())]).unwrap();
        let generation = crate::sds::CatalogGeneration {
            schema_version: 1,
            plan_id: 1,
            plan_version: 2,
            snapshot_sha256: "snapshot".into(),
        };
        plan.summary_catalog = Some(generation.clone());
        let materialization = plan.schemas[0].materialization;
        let legacy = serde_json::json!({
            "producer_id":"p", "collector_id":"c",
            "materialization":materialization, "schema_id":plan.schemas[0].schema_id,
        });
        assert!(serde_json::from_value::<ProducerContract>(legacy.clone()).is_err());
        let mut current = legacy;
        current["partition_ids"] = serde_json::json!([]);
        let producer: ProducerContract = serde_json::from_value(current).unwrap();
        assert!(producer.partition_ids.is_empty());
        plan.producers.push(producer);
        let barrier = crate::sds::SummaryWatermarkBarrier {
            catalog_generation: generation,
            source: crate::sds::SummarySourcePartition {
                producer_id: "p".into(),
                partition_id: "a".into(),
                producer_epoch: 1,
            },
            sequence: 1,
            watermark_ms: 100,
        };
        assert!(plan
            .validate_watermark_scope(materialization, &barrier)
            .is_err());
        plan.producers[0].partition_ids = BTreeSet::from(["b".into(), "a".into()]);
        let wire = serde_json::to_value(&plan).unwrap();
        assert_eq!(
            wire["producers"][0]["partition_ids"],
            serde_json::json!(["a", "b"])
        );
        let restored: PrecomputePlan = serde_json::from_value(wire).unwrap();
        assert!(restored
            .validate_watermark_scope(materialization, &barrier)
            .is_ok());
        for case in 0..6 {
            let mut wrong = barrier.clone();
            match case {
                0 => wrong.source.producer_id = "unknown".into(),
                1 => wrong.source.partition_id = "unknown".into(),
                2 => wrong.catalog_generation.plan_version += 1,
                3 => wrong.source.producer_epoch = 0,
                4 => wrong.sequence = 0,
                _ => wrong.catalog_generation.snapshot_sha256 = "other".into(),
            }
            assert!(restored
                .validate_watermark_scope(materialization, &wrong)
                .is_err());
        }
        let other_materialization = crate::sds::StoredOutputId::from(crate::PolicyFingerprint(
            materialization.as_u64().wrapping_add(1),
        ));
        assert!(restored
            .validate_watermark_scope(other_materialization, &barrier)
            .is_err());
        let mut wrong_envelope = restored.clone();
        wrong_envelope.envelope.plan_version += 1;
        assert!(wrong_envelope
            .validate_watermark_scope(materialization, &barrier)
            .is_err());
        plan.producers[0].partition_ids.insert(" ".into());
        assert!(plan.validate().is_err());
    }

    #[test]
    fn maintenance_reduction_requires_matching_explicit_population_contract() {
        use planner_types::post_asap::*;
        use planner_types::pre_asap::Reduction;
        let mut config = full_window();
        config.partitioning = Some(crate::sds::PopulationPartitioning::Grouped);
        let mut node = PostAsapDagNode {
            id: PostAsapNodeId(1),
            payload: PostAsapOperatorPayload::SummaryAgg {
                family: SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum),
                input: SummaryUpdate {
                    item: None,
                    weight: SummaryInputExpr::Constant(1.0),
                    weight_domain: Default::default(),
                },
                reduction: Reduction::by(vec![]),
                grouping: Default::default(),
            },
            output_state: ExecutionDataState::INGESTION_SUMMARY,
            output_schema: SummarySchema {
                fields: vec![],
                time_index: None,
            },
            guarantee: None,
        };
        assert!(validate_maintenance_reduction(&config, &node).is_ok());
        config.partitioning = Some(crate::sds::PopulationPartitioning::PerEntity);
        assert!(validate_maintenance_reduction(&config, &node).is_err());
        if let PostAsapOperatorPayload::SummaryAgg { reduction, .. } = &mut node.payload {
            *reduction = Reduction::PerEntity;
        }
        assert!(validate_maintenance_reduction(&config, &node).is_ok());
        config.partitioning = Some(crate::sds::PopulationPartitioning::Grouped);
        if let PostAsapOperatorPayload::SummaryAgg { reduction, .. } = &mut node.payload {
            *reduction = Reduction::by(vec![0]);
        }
        assert!(validate_maintenance_reduction(&config, &node).is_err());
        config.partitioning = None;
        assert!(validate_maintenance_reduction(&config, &node).is_err());
        node.payload = PostAsapOperatorPayload::SummaryMerge;
        assert!(validate_maintenance_reduction(&config, &node).is_err());
    }

    #[test]
    fn canonical_population_encoding_is_not_yet_installable() {
        let mut config = full_window();
        config.slide_interval = config.window_size;
        config.population_key_encoding =
            crate::grouping_projection::PopulationKeyEncoding::CanonicalLabelsV1;
        let error =
            PrecomputePlan::build_backend_local(envelope(1), vec![(config, sum())]).unwrap_err();
        assert!(
            error.to_string().contains("population key encoding"),
            "{error}"
        );
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
        other.allocate_stored_output_id(&"sum");
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
            changed.allocate_stored_output_id(&"sum");
            assert_ne!(source.policy_fingerprint(), changed.policy_fingerprint());
            assert!(validated_source_window_cohort(&target, &[&changed]).is_err());
        }
    }

    // A plan compiled for the v1 schema, which duplicated computation fields
    // on materializations, is rejected by version before its body is read.
    #[test]
    fn rejects_plans_from_the_computation_carrying_schema_by_version() {
        let mut config = full_window();
        config.slide_interval = config.window_size;
        config.window_type = crate::WindowKind::Tumbling;
        let plan = PrecomputePlan::build_backend_local(envelope(1), vec![(config, sum())]).unwrap();
        let mut wire = serde_json::to_value(&plan).unwrap();
        assert!(serde_json::from_value::<PrecomputePlan>(wire.clone()).is_ok());
        wire["envelope"]["backend_compat"] = "asap-query-backend.v1".into();
        wire["materializations"][0]["aggregation_type"] = "Sum".into();
        let error = serde_json::from_value::<PrecomputePlan>(wire).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported installed precompute plan schema asap-query-backend.v1"),
            "{error}"
        );
    }

    // Materializations name no computation: the removed v1 fields are unknown.
    #[test]
    fn materializations_reject_removed_computation_fields() {
        let wire = serde_json::to_value(full_window()).unwrap();
        for field in [
            "aggregation_type",
            "aggregation_sub_type",
            "parameters",
            "spatial_filter",
            "spatial_filter_normalized",
            "aggregated_labels",
            "rollup_labels",
            "original_yaml",
        ] {
            let mut legacy = wire.clone();
            legacy[field] = serde_json::json!("");
            assert!(
                serde_json::from_value::<crate::PrecomputeMaterialization>(legacy).is_err(),
                "{field}"
            );
        }
    }

    // An output without an allocated id cannot be installed.
    #[test]
    fn rejects_unallocated_stored_output_ids() {
        let mut config = full_window();
        config.slide_interval = config.window_size;
        config.window_type = crate::WindowKind::Tumbling;
        config.stored_output_id = crate::sds::StoredOutputId(0);
        let error =
            PrecomputePlan::build_backend_local(envelope(1), vec![(config, sum())]).unwrap_err();
        assert!(error.to_string().contains("stored output id"), "{error}");
    }
}
