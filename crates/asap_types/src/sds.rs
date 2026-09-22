//! Shared contracts for summary definitions, stored-summary metadata, and plan
//! output bindings. Payload bytes remain storage-engine owned and logically
//! belong to the stored summary identified by this metadata.
pub const TIMESTAMPED_OBSERVATION_SEMANTICS: &str = "asap.timestamped-observations.v2";

use crate::{AggregationType, PrecomputeMaterialization};
use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryFamilyType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdsError(pub String);
impl std::fmt::Display for SdsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SdsError {}

macro_rules! descriptor_id {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);
        impl $name {
            pub fn canonical(&self) -> &str {
                &self.0
            }
        }
    };
}
/// Semantic materialization reference. Wire-compatible with PolicyFingerprint,
/// but distinct from descriptor IDs and concrete [`SummaryInstanceId`] identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SummaryDefinitionId(pub crate::PolicyFingerprint);
impl SummaryDefinitionId {
    pub fn fingerprint(self) -> crate::PolicyFingerprint {
        self.0
    }
    pub fn as_u64(self) -> u64 {
        self.0 .0
    }
}
impl From<crate::PolicyFingerprint> for SummaryDefinitionId {
    fn from(value: crate::PolicyFingerprint) -> Self {
        Self(value)
    }
}
impl From<SummaryDefinitionId> for crate::PolicyFingerprint {
    fn from(value: SummaryDefinitionId) -> Self {
        value.0
    }
}

/// Identity of one persisted producer output within an installed plan version.
/// It is independent of the semantic definition shared by equivalent producers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoredOutputId(pub u64);

/// Typed join key carried by both the writer and every bound reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredOutputReference {
    #[serde(alias = "state_slot_id")]
    pub stored_output_id: StoredOutputId,
    pub definition_id: SummaryDefinitionId,
}

impl StoredOutputReference {
    /// Default compiler allocation when one output is selected per definition.
    pub fn for_definition(definition_id: SummaryDefinitionId) -> Self {
        Self {
            stored_output_id: StoredOutputId(definition_id.as_u64()),
            definition_id,
        }
    }

    pub fn validate(&self) -> Result<(), SdsError> {
        if self.stored_output_id.0 != 0 && !self.definition_id.fingerprint().is_unset() {
            Ok(())
        } else {
            Err(SdsError(
                "stored output and definition identities must be set".into(),
            ))
        }
    }
}

/// Canonical address of one stored DAG output for one population and window.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSummaryKey {
    pub plan_id: u64,
    pub plan_version: u64,
    pub output: StoredOutputReference,
    pub population: BTreeMap<String, String>,
    pub window: HalfOpenTimeRange,
}

impl StoredSummaryKey {
    pub fn validate(&self) -> Result<(), SdsError> {
        self.output.validate()?;
        if self.window.start_ms >= self.window.end_ms {
            return Err(SdsError("stored summary requires a nonempty window".into()));
        }
        Ok(())
    }

    pub fn storage_key(&self) -> Result<String, SdsError> {
        self.validate()?;
        serde_json::to_string(self).map_err(|e| SdsError(e.to_string()))
    }
}

descriptor_id!(SummaryDescriptorId);
descriptor_id!(DataDescriptorId);

descriptor_id!(SummaryInstanceId);

impl SummaryInstanceId {
    pub fn new(value: impl Into<String>) -> Result<Self, SdsError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SdsError("summary instance ID must not be empty".into()));
        }
        Ok(Self(value))
    }

    pub fn validate(&self) -> Result<(), SdsError> {
        if self.0.trim().is_empty() {
            Err(SdsError("summary instance ID must not be empty".into()))
        } else {
            Ok(())
        }
    }
}

/// Immutable identity of the desired catalog generation used to create state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogGeneration {
    pub schema_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    #[serde(alias = "snapshot_digest")]
    pub snapshot_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HalfOpenTimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
}

/// One ordered producer partition. Completion and watermark claims are scoped
/// to this identity; a maximum timestamp observed by an unrelated worker is
/// never a source-completeness signal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummarySourcePartition {
    pub producer_id: String,
    pub partition_id: String,
    /// Changes whenever a producer restarts or loses its sequence state.
    pub producer_epoch: u64,
}

/// A closed materialization bucket, including named group values so a
/// downstream maintenance DAG can project or shuffle groups without parsing a
/// routing key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryInstanceCoordinates {
    pub summary_definition_id: SummaryDefinitionId,
    pub time_range: HalfOpenTimeRange,
    pub group_values: BTreeMap<String, String>,
}

impl SummaryInstanceCoordinates {
    pub fn instance_id(&self) -> Result<SummaryInstanceId, SdsError> {
        let bytes =
            serde_json::to_vec(&self.group_values).map_err(|error| SdsError(error.to_string()))?;
        SummaryInstanceId::new(format!(
            "summary-instance:v1:{}:{}:{}:{}",
            self.summary_definition_id.as_u64(),
            self.time_range.start_ms,
            self.time_range.end_ms,
            xxhash_rust::xxh64::xxh64(&bytes, 0)
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryWindowCompletion {
    pub catalog_generation: CatalogGeneration,
    pub source: SummarySourcePartition,
    /// Concrete SDS instance identity allocated by the storage engine.
    pub instance_id: SummaryInstanceId,
    pub coordinates: SummaryInstanceCoordinates,
    /// Collision-free opaque producer lineage. Retries repeat these bytes.
    pub input_lineage: Vec<u8>,
}

impl HalfOpenTimeRange {
    pub fn validate(self) -> Result<(), SdsError> {
        if self.start_ms >= self.end_ms {
            Err(SdsError(
                "time range must be non-empty and half-open".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl SummarySourcePartition {
    pub fn validate(&self) -> Result<(), SdsError> {
        if self.producer_id.trim().is_empty() || self.partition_id.trim().is_empty() {
            return Err(SdsError(
                "source producer and partition must be non-empty".into(),
            ));
        }
        if self.producer_epoch == 0 {
            return Err(SdsError("source producer epoch must be positive".into()));
        }
        Ok(())
    }
}

impl SummaryWindowCompletion {
    pub fn validate(&self) -> Result<(), SdsError> {
        self.source.validate()?;
        self.instance_id.validate()?;
        self.coordinates.time_range.validate()?;
        if self.input_lineage.is_empty() {
            return Err(SdsError(
                "summary completion lineage must not be empty".into(),
            ));
        }
        validate_catalog_generation(&self.catalog_generation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryWatermarkBarrier {
    pub catalog_generation: CatalogGeneration,
    pub source: SummarySourcePartition,
    /// Monotonic sequence within the producer partition.
    pub sequence: u64,
    /// Every event in this partition at or before this event-time watermark
    /// was published before the barrier.
    pub watermark_ms: i64,
}

impl SummaryWatermarkBarrier {
    pub fn validate(&self) -> Result<(), SdsError> {
        self.source.validate()?;
        if self.sequence == 0 {
            return Err(SdsError("watermark sequence must be positive".into()));
        }
        validate_catalog_generation(&self.catalog_generation)
    }
}

fn validate_catalog_generation(generation: &CatalogGeneration) -> Result<(), SdsError> {
    if generation.schema_version == 0 || generation.snapshot_sha256.trim().is_empty() {
        Err(SdsError("catalog generation identity is invalid".into()))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceCompleteness {
    Complete,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryInstanceStatus {
    Building,
    Ready,
    Retiring,
    Failed,
    MissingPayload,
}

/// Logical producer and physical state location. Placement changes do not
/// change descriptor or materialization identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryPlacement {
    pub producer_id: String,
    pub storage_node_id: String,
}

/// Opaque storage-engine locator. `key` identifies payload stored elsewhere;
/// encoded summary state must never be placed in this metadata contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryStateReference {
    pub store: String,
    pub key: String,
    pub state_schema_version: u32,
    pub generation: u64,
    pub sequence: u64,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EphemeralLease {
    pub lease_id: String,
    pub owner_id: String,
    pub issued_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceLifecycle {
    Persistent,
    Ephemeral { lease: EphemeralLease },
}

/// Observed metadata for one concrete SDS instance. Payload remains in the
/// storage engine and is reached only through `state_reference`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryInstance {
    pub instance_id: SummaryInstanceId,
    #[serde(alias = "state_slot_id")]
    pub stored_output_id: StoredOutputId,
    #[serde(alias = "materialization_id")]
    pub summary_definition_id: SummaryDefinitionId,
    pub summary_descriptor_id: SummaryDescriptorId,
    pub data_descriptor_id: DataDescriptorId,
    pub time_range: HalfOpenTimeRange,
    pub group_values: BTreeMap<String, String>,
    pub catalog_generation: CatalogGeneration,
    /// Source generation selected by an explicit compatibility decision when
    /// an unchanged definition reuses a committed payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reused_from_generation: Option<CatalogGeneration>,
    pub placement: SummaryPlacement,
    pub state_reference: SummaryStateReference,
    pub status: SummaryInstanceStatus,
    pub completeness: InstanceCompleteness,
    pub lifecycle: InstanceLifecycle,
    pub observed_at_ms: i64,
}

impl SummaryInstance {
    pub fn validate(&self) -> Result<(), SdsError> {
        StoredOutputReference {
            stored_output_id: self.stored_output_id,
            definition_id: self.summary_definition_id,
        }
        .validate()?;
        if self.time_range.start_ms >= self.time_range.end_ms {
            return Err(SdsError(
                "summary instance time range must be non-empty".into(),
            ));
        }
        if self.catalog_generation.schema_version == 0
            || self.catalog_generation.snapshot_sha256.is_empty()
        {
            return Err(SdsError(
                "summary instance has invalid catalog generation".into(),
            ));
        }
        if self.placement.producer_id.is_empty() || self.placement.storage_node_id.is_empty() {
            return Err(SdsError(
                "summary instance placement must be resolved".into(),
            ));
        }
        let payload_generation = if let Some(source) = &self.reused_from_generation {
            validate_catalog_generation(source)?;
            if source.plan_id != self.catalog_generation.plan_id
                || source.plan_version >= self.catalog_generation.plan_version
            {
                return Err(SdsError(
                    "stored summary has invalid reuse provenance".into(),
                ));
            }
            source.plan_version
        } else {
            self.catalog_generation.plan_version
        };
        if self.state_reference.store.is_empty()
            || self.state_reference.key.is_empty()
            || self.state_reference.state_schema_version == 0
            || self.state_reference.generation != payload_generation
        {
            return Err(SdsError(
                "summary instance has invalid state reference".into(),
            ));
        }
        if let InstanceLifecycle::Ephemeral { lease } = &self.lifecycle {
            if lease.lease_id.is_empty()
                || lease.owner_id.is_empty()
                || lease.issued_at_ms >= lease.expires_at_ms
            {
                return Err(SdsError(
                    "summary instance has invalid ephemeral lease".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Data-plane report of what is actually stored. This is observed state and is
/// deliberately separate from the control-plane desired SummaryCatalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedSummaryInventory {
    pub schema_version: u32,
    pub reporter_id: String,
    pub inventory_version: u64,
    pub observed_at_ms: i64,
    pub instances: BTreeMap<SummaryInstanceId, SummaryInstance>,
}

impl ObservedSummaryInventory {
    pub fn validate(&self) -> Result<(), SdsError> {
        if self.schema_version != 1 {
            return Err(SdsError("unsupported observed inventory schema".into()));
        }
        if self.reporter_id.is_empty() {
            return Err(SdsError("inventory reporter ID must not be empty".into()));
        }
        for (id, instance) in &self.instances {
            instance.validate()?;
            if id != &instance.instance_id {
                return Err(SdsError("inventory key differs from instance ID".into()));
            }
            if instance.observed_at_ms > self.observed_at_ms {
                return Err(SdsError(
                    "instance observation is newer than inventory".into(),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_against_catalog(
        &self,
        catalog: &crate::summary_catalog::SummaryCatalog,
    ) -> Result<(), SdsError> {
        self.validate()?;
        let generation = catalog
            .reference()
            .map_err(|error| SdsError(error.to_string()))?;
        for instance in self.instances.values() {
            if instance.catalog_generation != generation {
                return Err(SdsError(
                    "summary instance belongs to another plan generation".into(),
                ));
            }
            let definition = catalog
                .definitions
                .get(&instance.summary_definition_id)
                .ok_or_else(|| SdsError("summary instance has no catalog definition".into()))?;
            if instance.summary_descriptor_id != definition.summary_descriptor_id
                || instance.data_descriptor_id != definition.data_descriptor_id
                || instance.state_reference.state_schema_version
                    != catalog.summary_descriptors[&definition.summary_descriptor_id]
                        .state_schema_version
            {
                return Err(SdsError(
                    "summary instance differs from its catalog definition".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SummaryOperator {
    /// Compatibility evidence from a legacy backend record, not a complete
    /// configured contract. Its ID can never satisfy a Configured descriptor.
    LegacyPartial { operator_canonical: String },
    Sketch {
        algorithm: SketchAlgorithm,
        parameters: BTreeMap<String, Value>,
    },
    ExactAgg {
        agg_type: AggregationType,
        parameters_canonical: String,
    },
    /// Complete planner materialization configuration, including heap/Hydra
    /// dimensions and readout/update subtype. Never equal to a legacy projection.
    Configured {
        /// Planner-selected semantic family; grouping and pane layout live in
        /// the data descriptor and summary definition, respectively.
        family: planner_types::post_asap::SummaryFamilyType,
        aggregation_type: AggregationType,
        aggregation_sub_type: String,
        parameters: BTreeMap<String, Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FidelityGuarantee {
    /// Total unit-frequency count is exact. Distinct, L2 and entropy require
    /// readout-specific evidence; dimensions alone certify no error bound.
    UnivMonFrequency {
        heap_size: u32,
        sketch_rows: u32,
        sketch_cols: u32,
        layers: u8,
    },
    Exact,
    /// Exact PromQL counter readout from fixed-size pane summaries. Each pane
    /// stores only `(first value/time, last value/time, reset-corrected delta,
    /// sample count)`. A query is eligible only when the selected panes fully
    /// cover its range; partial boundary panes must fall back to Prometheus.
    ExactCounter {
        model: String,
        full_pane_coverage_required: bool,
    },
    KllRankError {
        k: u32,
        model: String,
    },
    DdSketchRelativeError {
        alpha: f64,
    },
    HllCardinalityError {
        precision: u32,
        model: String,
    },
    CmsFrequencyError {
        width: u32,
        depth: u32,
        model: String,
    },
    CountSketchFrequencyError {
        width: u32,
        depth: u32,
        model: String,
    },
    Unknown {
        reason: String,
    },
}
impl FidelityGuarantee {
    /// Model IDs name parameterized error families/scopes, not certified numeric
    /// epsilon/confidence values. Heap membership and Hydra cross-cell readouts
    /// need separate models; point-frequency/per-cell rank does not attest them.
    pub fn from_config(config: &PrecomputeMaterialization) -> Self {
        if config.aggregation_type == AggregationType::HLL {
            let precision = config
                .parameters
                .get("precision")
                .or_else(|| config.parameters.get("p"))
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(14);
            return Self::HllCardinalityError {
                precision,
                model: "asap.hll.relative-cardinality.v1".into(),
            };
        }
        match config.accumulator_spec().map(|s| s.family) {
            Ok(SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Increase
                | planner_types::post_asap::ExactKind::Rate
                | planner_types::post_asap::ExactKind::IRate,
                _,
            )) => Self::ExactCounter {
                model: "prometheus.extrapolated-rate.v1".into(),
                full_pane_coverage_required: true,
            },
            Ok(SummaryFamilyType::ExactAggregate(..)) => Self::Exact,
            Ok(SummaryFamilyType::Sketch(kind, _)) => match kind.params() {
                SketchParams::UnivMon {
                    heap_size,
                    sketch_rows,
                    sketch_cols,
                    layers,
                } => Self::UnivMonFrequency {
                    heap_size: *heap_size,
                    sketch_rows: *sketch_rows,
                    sketch_cols: *sketch_cols,
                    layers: *layers,
                },
                SketchParams::Kll { k } => Self::KllRankError {
                    k: *k,
                    model: if config.aggregation_type == AggregationType::HydraKLL {
                        "asap.hydra-kll.per-cell-rank.v1"
                    } else {
                        "asap.kll.normalized-rank.v1"
                    }
                    .into(),
                },
                SketchParams::DDSketch { alpha } => Self::DdSketchRelativeError { alpha: *alpha },
                SketchParams::Hll { precision } => Self::HllCardinalityError {
                    precision: (*precision).into(),
                    model: "asap.hll.relative-cardinality.v1".into(),
                },
                SketchParams::Cms { width, depth }
                | SketchParams::CmsWithHeap { width, depth, .. } => Self::CmsFrequencyError {
                    width: *width,
                    depth: *depth,
                    model: "asap.cms.point-frequency.v1".into(),
                },
                SketchParams::CountSketch { width, depth }
                | SketchParams::CountSketchWithHeap { width, depth, .. } => {
                    Self::CountSketchFrequencyError {
                        width: *width,
                        depth: *depth,
                        model: "asap.count-sketch.point-frequency.v1".into(),
                    }
                }
                _ => Self::Unknown {
                    reason: "No shared parameterized error model for this sketch family".into(),
                },
            },
            _ => Self::Unknown {
                reason: "Physical accumulator family is unavailable".into(),
            },
        }
    }
    fn validate(&self) -> Result<(), SdsError> {
        let valid = match self {
            Self::UnivMonFrequency {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } => {
                *heap_size > 0
                    && *sketch_cols > 0
                    && (1..=20).contains(sketch_rows)
                    && (1..=64).contains(layers)
            }
            Self::Exact => true,
            Self::ExactCounter {
                model,
                full_pane_coverage_required,
            } => !model.is_empty() && *full_pane_coverage_required,
            Self::KllRankError { k, model } => *k > 0 && !model.is_empty(),
            Self::DdSketchRelativeError { alpha } => {
                alpha.is_finite() && *alpha > 0.0 && *alpha < 1.0
            }
            Self::HllCardinalityError { precision, model } => {
                *precision > 0 && *precision < 64 && !model.is_empty()
            }
            Self::CmsFrequencyError {
                width,
                depth,
                model,
            }
            | Self::CountSketchFrequencyError {
                width,
                depth,
                model,
            } => *width > 0 && *depth > 0 && !model.is_empty(),
            Self::Unknown { reason } => !reason.is_empty(),
        };
        if valid {
            Ok(())
        } else {
            Err(SdsError("invalid parameterized fidelity contract".into()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryDescriptor {
    pub id: SummaryDescriptorId,
    pub operator: SummaryOperator,
    pub fidelity: FidelityGuarantee,
    pub state_schema_version: u32,
}

/// Sort every JSON object, including nested configuration values. Arrays retain
/// order; opaque canonical strings are used verbatim, not reparsed as PromQL.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map.iter().collect();
            format!(
                "{{{}}}",
                sorted
                    .into_iter()
                    .map(|(k, v)| format!("{}:{}", serde_json::to_string(k).unwrap(), canonical(v)))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        Value::Array(values) => format!(
            "[{}]",
            values.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        _ => value.to_string(),
    }
}
impl SummaryDescriptor {
    pub fn new(
        operator: SummaryOperator,
        fidelity: FidelityGuarantee,
        state_schema_version: u32,
    ) -> Result<Self, SdsError> {
        if state_schema_version == 0 {
            return Err(SdsError("state schema version must be positive".into()));
        }
        fidelity.validate()?;
        if let SummaryOperator::Configured {
            family,
            aggregation_type,
            ..
        } = &operator
        {
            if let Some(expected) = aggregation_type.planner_exact_family() {
                if family != &expected {
                    return Err(SdsError(
                        "configured storage type disagrees with Planner family".into(),
                    ));
                }
            } else {
                use AggregationType as A;
                let expected = match aggregation_type {
                    A::DatasketchesKLL | A::HydraKLL => Some(SketchAlgorithm::Kll),
                    A::CountMinSketch => Some(SketchAlgorithm::Cms),
                    A::CountMinSketchWithHeap => Some(SketchAlgorithm::CmsWithHeap),
                    A::CountSketch => Some(SketchAlgorithm::CountSketch),
                    A::CountSketchWithHeap => Some(SketchAlgorithm::CountSketchWithHeap),
                    A::DDSketch => Some(SketchAlgorithm::DDSketch),
                    A::HLL => Some(SketchAlgorithm::Hll),
                    A::UnivMon => Some(SketchAlgorithm::UnivMon),
                    _ => None,
                };
                if let Some(expected) = expected {
                    if !matches!(family, planner_types::post_asap::SummaryFamilyType::Sketch(kind, _) if kind.algorithm() == &expected)
                    {
                        return Err(SdsError(
                            "configured sketch storage disagrees with Planner family".into(),
                        ));
                    }
                }
            }
        }
        if !fidelity.is_compatible_with(&operator) {
            return Err(SdsError(
                "summary operator and fidelity guarantee are incompatible".into(),
            ));
        }
        let content = json!({"operator":operator,"fidelity":fidelity,"state_schema_version":state_schema_version});
        let id = SummaryDescriptorId(format!("summary:v3:{}", canonical(&content)));
        Ok(Self {
            id,
            operator,
            fidelity,
            state_schema_version,
        })
    }
    pub fn id(&self) -> &SummaryDescriptorId {
        &self.id
    }
    pub fn validate(&self) -> Result<(), SdsError> {
        let rebuilt = Self::new(
            self.operator.clone(),
            self.fidelity.clone(),
            self.state_schema_version,
        )?;
        if self.id != rebuilt.id {
            return Err(SdsError("summary descriptor ID/content mismatch".into()));
        }
        Ok(())
    }

    /// Preserve every configured state/update parameter. Omitted defaults remain
    /// distinct from explicit defaults (conservative identity, never false sharing).
    /// Legacy AggKind projections intentionally have different operator variants:
    /// they cannot attest heap, Hydra, or aggregation-subtype semantics they lost.
    pub fn from_config(config: &PrecomputeMaterialization) -> Result<Self, SdsError> {
        let fidelity = FidelityGuarantee::from_config(config);
        let state_schema_version = if matches!(fidelity, FidelityGuarantee::ExactCounter { .. }) {
            2
        } else {
            1
        };
        Self::new(
            SummaryOperator::Configured {
                family: config
                    .accumulator_spec()
                    .map_err(|error| SdsError(error.to_string()))?
                    .family,
                aggregation_type: config.aggregation_type,
                aggregation_sub_type: config.aggregation_sub_type.clone(),
                parameters: config
                    .parameters
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            },
            fidelity,
            state_schema_version,
        )
    }
}

impl FidelityGuarantee {
    /// Reject a descriptor that advertises an error model belonging to a
    /// different state family. This is part of the shared wire contract, so a
    /// consumer must not need to trust the process that produced the catalog.
    pub fn is_compatible_with(&self, operator: &SummaryOperator) -> bool {
        use AggregationType as A;
        use FidelityGuarantee::*;
        use SketchAlgorithm as S;

        let configured = |aggregation_type| {
            matches!(
                (aggregation_type, self),
                (A::UnivMon, UnivMonFrequency { .. })
                    | (A::Sum | A::Count | A::Min | A::Max, Exact)
                    | (A::Increase | A::Rate, ExactCounter { .. })
                    | (A::DatasketchesKLL | A::HydraKLL, KllRankError { .. })
                    | (A::DDSketch, DdSketchRelativeError { .. })
                    | (A::HLL, HllCardinalityError { .. })
                    | (
                        A::CountMinSketch | A::CountMinSketchWithHeap,
                        CmsFrequencyError { .. }
                    )
                    | (
                        A::CountSketch | A::CountSketchWithHeap,
                        CountSketchFrequencyError { .. }
                    )
                    | (
                        A::SingleSubpopulation | A::MultipleSubpopulation,
                        Unknown { .. }
                    )
            )
        };

        match operator {
            SummaryOperator::LegacyPartial { .. } => matches!(self, Unknown { .. }),
            SummaryOperator::ExactAgg { agg_type, .. } => configured(*agg_type),
            SummaryOperator::Configured {
                aggregation_type,
                parameters,
                ..
            } => configured(*aggregation_type) && self.matches_parameters(parameters),
            SummaryOperator::Sketch {
                algorithm,
                parameters,
            } => {
                matches!(
                    (algorithm, self),
                    (S::UnivMon, UnivMonFrequency { .. })
                        | (S::Kll, KllRankError { .. })
                        | (S::DDSketch, DdSketchRelativeError { .. })
                        | (S::Hll, HllCardinalityError { .. })
                        | (S::Cms | S::CmsWithHeap, CmsFrequencyError { .. })
                        | (
                            S::CountSketch | S::CountSketchWithHeap,
                            CountSketchFrequencyError { .. }
                        )
                        | (S::Kmv | S::Theta, Unknown { .. })
                ) && self.matches_parameters(parameters)
            }
        }
    }

    fn matches_parameters(&self, parameters: &BTreeMap<String, Value>) -> bool {
        let u32_parameter = |names: &[&str], expected: u32| {
            names
                .iter()
                .filter_map(|name| parameters.get(*name))
                .all(|value| value.as_u64() == Some(u64::from(expected)))
        };
        match self {
            Self::UnivMonFrequency {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } => {
                u32_parameter(&["heap_size"], *heap_size)
                    && u32_parameter(&["sketch_rows"], *sketch_rows)
                    && u32_parameter(&["sketch_cols"], *sketch_cols)
                    && u32_parameter(&["layers"], u32::from(*layers))
            }
            Self::KllRankError { k, .. } => u32_parameter(&["k", "K"], *k),
            Self::HllCardinalityError { precision, .. } => {
                u32_parameter(&["precision", "p"], *precision)
            }
            Self::CmsFrequencyError { width, depth, .. }
            | Self::CountSketchFrequencyError { width, depth, .. } => {
                u32_parameter(&["w", "width"], *width) && u32_parameter(&["d", "depth"], *depth)
            }
            Self::DdSketchRelativeError { alpha } => ["alpha", "relative_accuracy"]
                .iter()
                .filter_map(|name| parameters.get(*name))
                .all(|value| value.as_f64() == Some(*alpha)),
            _ => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceIdentity {
    TimeSeries {
        metric: String,
    },
    Table {
        table_ref: String,
    },
    Derived {
        input: crate::derived_input::DerivedInputIdentity,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum ValueProjectionIdentity {
    SampleValue,
    Column {
        name: String,
    },
    Constant {
        value: planner_types::pre_asap::ScalarValue,
    },
}

impl ValueProjectionIdentity {
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::Column { name } => Some(name),
            _ => None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        use planner_types::pre_asap::ScalarValue;
        match self {
            Self::Column { name } => crate::table_population::validate_column_name(name),
            Self::Constant {
                value: ScalarValue::Int64(_),
            }
            | Self::SampleValue => Ok(()),
            Self::Constant {
                value: ScalarValue::Float64(value),
            } if value.is_finite() => Ok(()),
            Self::Constant { .. } => {
                Err("summary value projection requires a finite numeric literal".into())
            }
        }
    }
}

/// Compatibility adapter for old config column strings; storage is always typed.
pub(crate) fn deserialize_optional_value_projection<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ValueProjectionIdentity>, D::Error> {
    let value = Option::<Value>::deserialize(deserializer)?;
    value
        .map(|value| match value {
            Value::String(name) => Ok(ValueProjectionIdentity::Column { name }),
            value => serde_json::from_value(value).map_err(serde::de::Error::custom),
        })
        .transpose()
}

/// Read legacy StateSchema ColumnRef values without retaining a parallel field.
pub(crate) fn deserialize_state_value_projection<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<ValueProjectionIdentity, D::Error> {
    let value = Value::deserialize(deserializer)?;
    if value == "SampleValue" {
        return Ok(ValueProjectionIdentity::SampleValue);
    }
    if let Some(name) = value.get("Named").and_then(Value::as_str) {
        return Ok(ValueProjectionIdentity::Column { name: name.into() });
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

/// Whether a materialization preserves source entities or pools a population.
/// Grouped label names remain in `DataDescriptor::group_by_keys`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PopulationPartitioning {
    PerEntity,
    Grouped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataDescriptor {
    pub id: DataDescriptorId,
    pub source: DataSourceIdentity,
    pub value_projection: ValueProjectionIdentity,
    /// Table column containing Unix milliseconds. Absent for time-series sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_column: Option<String>,
    pub population_filter_canonical: String,
    pub group_by_keys: crate::GroupingProjection,
    #[serde(
        default,
        skip_serializing_if = "crate::grouping_projection::PopulationKeyEncoding::is_legacy"
    )]
    pub population_key_encoding: crate::grouping_projection::PopulationKeyEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitioning: Option<PopulationPartitioning>,
    /// Versioned contract for timestamp interpretation and
    /// missing/duplicate/invalid observation handling.
    pub observation_semantics: String,
}
impl DataDescriptor {
    pub fn time_series_metric(&self) -> Option<&str> {
        match &self.source {
            DataSourceIdentity::TimeSeries { metric } => Some(metric),
            DataSourceIdentity::Table { .. } | DataSourceIdentity::Derived { .. } => None,
        }
    }

    pub fn new(
        metric: impl Into<String>,
        filter: impl Into<String>,
        group_by: impl IntoIterator<Item = String>,
    ) -> Self {
        Self::new_with_semantics(
            metric,
            filter,
            group_by,
            "asap.timestamped-metric-samples.v1",
        )
    }

    pub fn new_with_semantics(
        metric: impl Into<String>,
        filter: impl Into<String>,
        group_by: impl IntoIterator<Item = String>,
        observation_semantics: impl Into<String>,
    ) -> Self {
        let source = DataSourceIdentity::TimeSeries {
            metric: metric.into(),
        };
        Self::new_typed(
            source,
            ValueProjectionIdentity::SampleValue,
            filter,
            group_by,
            observation_semantics,
        )
    }

    pub fn new_typed(
        source: DataSourceIdentity,
        value_projection: ValueProjectionIdentity,
        filter: impl Into<String>,
        group_by: impl IntoIterator<Item = String>,
        observation_semantics: impl Into<String>,
    ) -> Self {
        let population_filter_canonical = filter.into();
        let group_by_keys = group_by.into_iter().collect();
        let observation_semantics = observation_semantics.into();
        let id = data_descriptor_id(
            &source,
            &value_projection,
            &population_filter_canonical,
            &group_by_keys,
            &observation_semantics,
            None,
            None,
            Default::default(),
        );
        Self {
            id,
            source,
            value_projection,
            timestamp_column: None,
            population_filter_canonical,
            group_by_keys,
            partitioning: None,
            population_key_encoding: Default::default(),
            observation_semantics,
        }
    }
    pub fn with_grouping_projection(mut self, grouping: crate::GroupingProjection) -> Self {
        self.group_by_keys = grouping;
        self.id = data_descriptor_id(
            &self.source,
            &self.value_projection,
            &self.population_filter_canonical,
            &self.group_by_keys,
            &self.observation_semantics,
            self.partitioning,
            self.timestamp_column.as_deref(),
            self.population_key_encoding,
        );
        self
    }
    pub fn with_partitioning(mut self, partitioning: Option<PopulationPartitioning>) -> Self {
        self.partitioning = partitioning;
        self.id = data_descriptor_id(
            &self.source,
            &self.value_projection,
            &self.population_filter_canonical,
            &self.group_by_keys,
            &self.observation_semantics,
            partitioning,
            self.timestamp_column.as_deref(),
            self.population_key_encoding,
        );
        self
    }
    pub fn with_population_key_encoding(
        mut self,
        encoding: crate::grouping_projection::PopulationKeyEncoding,
    ) -> Self {
        self.population_key_encoding = encoding;
        let partitioning = self.partitioning;
        self.with_partitioning(partitioning)
    }
    pub fn with_timestamp_column(mut self, column: Option<String>) -> Self {
        self.timestamp_column = column;
        let partitioning = self.partitioning;
        self.with_partitioning(partitioning)
    }
    pub fn id(&self) -> &DataDescriptorId {
        &self.id
    }
    pub fn validate(&self) -> Result<(), SdsError> {
        if let DataSourceIdentity::Derived { input } = &self.source {
            input.validate().map_err(SdsError)?;
        }
        self.value_projection.validate().map_err(SdsError)?;
        self.group_by_keys.validate().map_err(SdsError)?;
        if matches!(self.source, DataSourceIdentity::TimeSeries { .. })
            && !self.group_by_keys.is_legacy_labels()
        {
            return Err(SdsError(
                "time-series grouping requires non-null string labels".into(),
            ));
        }

        if matches!(self.source, DataSourceIdentity::Table { .. }) {
            for column in self.group_by_keys.columns() {
                crate::table_population::validate_column_name(&column.name).map_err(SdsError)?;
            }
        }
        if let Some(column) = &self.timestamp_column {
            if !matches!(self.source, DataSourceIdentity::Table { .. }) || column.is_empty() {
                return Err(SdsError(
                    "table timestamp projection requires a table and a column".into(),
                ));
            }
            crate::table_population::validate_column_name(column).map_err(SdsError)?;
        }
        if self.id
            != data_descriptor_id(
                &self.source,
                &self.value_projection,
                &self.population_filter_canonical,
                &self.group_by_keys,
                &self.observation_semantics,
                self.partitioning,
                self.timestamp_column.as_deref(),
                self.population_key_encoding,
            )
        {
            return Err(SdsError("data descriptor ID/content mismatch".into()));
        }
        Ok(())
    }
}
#[allow(clippy::too_many_arguments)]
fn data_descriptor_id(
    source: &DataSourceIdentity,
    value_projection: &ValueProjectionIdentity,
    filter: &str,
    group_by: &crate::GroupingProjection,
    observation_semantics: &str,
    partitioning: Option<PopulationPartitioning>,
    timestamp_column: Option<&str>,
    population_key_encoding: crate::grouping_projection::PopulationKeyEncoding,
) -> DataDescriptorId {
    // Length framing keeps distinct typed sources, projections, predicates,
    // and grouping keys collision-free in the content identity.
    let source = canonical(&serde_json::to_value(source).expect("data source serializes"));
    let projection =
        canonical(&serde_json::to_value(value_projection).expect("value projection serializes"));
    let mut key = format!(
        "data:v2|{}:{source}|{}:{projection}|{}:{filter}",
        source.len(),
        projection.len(),
        filter.len()
    );
    if let Some(partitioning) = partitioning {
        key.push_str(&format!("|partition:{partitioning:?}"));
    }
    if let Some(column) = timestamp_column {
        key.push_str(&format!("|timestamp-ms:{}:{column}", column.len()));
    }
    for name in group_by.names() {
        key.push_str(&format!("|{}:{name}", name.len()));
    }
    if !group_by.is_legacy_labels() {
        let typed =
            canonical(&serde_json::to_value(group_by).expect("group projection serializes"));
        key.push_str(&format!("|group-types:{}:{typed}", typed.len()));
    }
    key.push_str(&format!(
        "|{}:{observation_semantics}",
        observation_semantics.len()
    ));
    if !population_key_encoding.is_legacy() {
        key = format!("data:population-key:canonical-labels-v1|{key}");
    }
    DataDescriptorId(key)
}

#[cfg(test)]
mod partition_identity_tests {
    use super::*;
    #[test]
    fn entity_and_global_population_have_distinct_identity() {
        let legacy = DataDescriptor::new_typed(
            DataSourceIdentity::TimeSeries { metric: "m".into() },
            ValueProjectionIdentity::SampleValue,
            "",
            Vec::<String>::new(),
            "v1",
        );
        let entity = legacy
            .clone()
            .with_partitioning(Some(PopulationPartitioning::PerEntity));
        let grouped = legacy
            .clone()
            .with_partitioning(Some(PopulationPartitioning::Grouped));
        assert_ne!(entity.id, grouped.id);
        assert_ne!(entity.id, legacy.id);
        assert_eq!(legacy.clone().with_partitioning(None).id, legacy.id);
        entity.validate().unwrap();
        grouped.validate().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn population_encoding_is_part_of_data_identity_with_legacy_default() {
        use crate::grouping_projection::PopulationKeyEncoding;
        let legacy = DataDescriptor::new("m", "", ["host".to_string()]);
        let wire = serde_json::to_value(&legacy).unwrap();
        assert!(wire.get("population_key_encoding").is_none());
        let decoded: DataDescriptor = serde_json::from_value(wire).unwrap();
        assert_eq!(legacy.id(), decoded.id());
        let canonical = legacy
            .clone()
            .with_population_key_encoding(PopulationKeyEncoding::CanonicalLabelsV1);
        assert_ne!(legacy.id(), canonical.id());
        canonical.validate().unwrap();
        let reverted =
            canonical.with_population_key_encoding(PopulationKeyEncoding::LegacyDelimited);
        assert_eq!(legacy.id(), reverted.id());
    }

    fn completion(epoch: u64) -> SummaryWindowCompletion {
        SummaryWindowCompletion {
            catalog_generation: CatalogGeneration {
                schema_version: 1,
                plan_id: 4,
                plan_version: 2,
                snapshot_sha256: "snapshot".into(),
            },
            source: SummarySourcePartition {
                producer_id: "producer-a".into(),
                partition_id: "partition-0".into(),
                producer_epoch: epoch,
            },
            instance_id: SummaryInstanceId::new("instance-1").unwrap(),
            coordinates: SummaryInstanceCoordinates {
                summary_definition_id: SummaryDefinitionId(crate::PolicyFingerprint(7)),
                time_range: HalfOpenTimeRange {
                    start_ms: 1_000,
                    end_ms: 2_000,
                },
                group_values: BTreeMap::from([("job".into(), "api".into())]),
            },
            input_lineage: vec![1, 2, 3],
        }
    }

    #[test]
    fn completion_contract_validates_identity_epoch_range_and_lineage() {
        completion(1).validate().unwrap();

        let mut invalid = completion(1);
        invalid.coordinates.time_range.end_ms = invalid.coordinates.time_range.start_ms;
        assert!(invalid.validate().is_err());
        invalid = completion(1);
        invalid.source.producer_id.clear();
        assert!(invalid.validate().is_err());
        invalid = completion(0);
        assert!(invalid.validate().is_err());
        invalid = completion(1);
        invalid.input_lineage.clear();
        assert!(invalid.validate().is_err());

        let mut wire = serde_json::to_value(completion(1)).unwrap();
        wire.as_object_mut()
            .unwrap()
            .insert("future_field".into(), json!(true));
        assert!(serde_json::from_value::<SummaryWindowCompletion>(wire).is_err());
    }

    #[test]
    fn producer_epoch_separates_restart_incarnations() {
        let first = completion(1).source;
        let second = completion(2).source;
        assert_ne!(first, second);
        assert_eq!(BTreeSet::from([first, second]).len(), 2);
    }

    #[test]
    fn watermark_contract_rejects_zero_sequence_and_unknown_fields() {
        let completion = completion(3);
        let mut barrier = SummaryWatermarkBarrier {
            catalog_generation: completion.catalog_generation,
            source: completion.source,
            sequence: 1,
            watermark_ms: 2_000,
        };
        barrier.validate().unwrap();
        barrier.sequence = 0;
        assert!(barrier.validate().is_err());

        let mut wire = serde_json::to_value(&barrier).unwrap();
        wire.as_object_mut()
            .unwrap()
            .insert("ignored".into(), json!(1));
        assert!(serde_json::from_value::<SummaryWatermarkBarrier>(wire).is_err());
    }

    fn observed_instance(lifecycle: InstanceLifecycle) -> SummaryInstance {
        SummaryInstance {
            instance_id: SummaryInstanceId::new("instance-1").unwrap(),
            stored_output_id: StoredOutputId(7),
            summary_definition_id: SummaryDefinitionId(crate::PolicyFingerprint(7)),
            summary_descriptor_id: descriptor(
                200,
                FidelityGuarantee::KllRankError {
                    k: 200,
                    model: "rank.v1".into(),
                },
                1,
            )
            .id,
            data_descriptor_id: DataDescriptor::new("cpu", "", []).id,
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 10,
            },
            group_values: BTreeMap::new(),
            catalog_generation: CatalogGeneration {
                schema_version: 1,
                plan_id: 1,
                plan_version: 2,
                snapshot_sha256: "abc".into(),
            },
            reused_from_generation: None,
            placement: SummaryPlacement {
                producer_id: "producer".into(),
                storage_node_id: "store".into(),
            },
            state_reference: SummaryStateReference {
                store: "summary-store".into(),
                key: "state/1".into(),
                state_schema_version: 1,
                generation: 2,
                sequence: 3,
                checksum: None,
            },
            status: SummaryInstanceStatus::Ready,
            completeness: InstanceCompleteness::Complete,
            lifecycle,
            observed_at_ms: 10,
        }
    }

    #[test]
    fn instance_inventory_has_metadata_reference_without_payload() {
        let instance = observed_instance(InstanceLifecycle::Persistent);
        instance.validate().unwrap();
        let encoded = serde_json::to_value(&instance).unwrap();
        assert!(encoded.get("state_reference").is_some());
        assert!(encoded.get("state").is_none());
        assert!(encoded.get("payload").is_none());
        let inventory = ObservedSummaryInventory {
            schema_version: 1,
            reporter_id: "store".into(),
            inventory_version: 4,
            observed_at_ms: 10,
            instances: BTreeMap::from([(instance.instance_id.clone(), instance)]),
        };
        inventory.validate().unwrap();
    }

    // Every component of the persisted output address participates in identity.
    #[test]
    fn stored_summary_key_binds_plan_output_population_and_window() {
        let key = StoredSummaryKey {
            plan_id: 1,
            plan_version: 2,
            output: StoredOutputReference {
                stored_output_id: StoredOutputId(101),
                definition_id: crate::PolicyFingerprint(7).into(),
            },
            population: BTreeMap::from([("service".into(), "api".into())]),
            window: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 1000,
            },
        };
        let mut variants = vec![key.clone(); 5];
        variants[0].plan_id += 1;
        variants[1].plan_version += 1;
        variants[2].output.stored_output_id.0 += 1;
        variants[3].population.insert("service".into(), "db".into());
        variants[4].window.end_ms += 1;
        let canonical = key.storage_key().unwrap();
        for other in variants {
            assert_ne!(canonical, other.storage_key().unwrap());
        }
        assert_eq!(
            serde_json::from_str::<StoredSummaryKey>(&canonical).unwrap(),
            key
        );
    }

    // A DAG output has its own identity even when its definition is shared.
    #[test]
    fn stored_output_identity_is_independent_of_definition() {
        let definition_id = SummaryDefinitionId::from(crate::PolicyFingerprint(7));
        for stored_output_id in [StoredOutputId(101), StoredOutputId(102)] {
            StoredOutputReference {
                stored_output_id,
                definition_id,
            }
            .validate()
            .unwrap();
        }
    }

    #[test]
    fn stored_output_and_payload_version_must_match_instance_definition() {
        let mut instance = observed_instance(InstanceLifecycle::Persistent);
        instance.stored_output_id = StoredOutputId(0);
        assert!(instance.validate().is_err());
        instance.stored_output_id = StoredOutputId(7);
        instance.state_reference.generation = 3;
        assert!(instance.validate().is_err());
        let mut reference = StoredOutputReference::for_definition(instance.summary_definition_id);
        reference.stored_output_id = StoredOutputId(0);
        assert!(reference.validate().is_err());
    }

    #[test]
    fn stored_output_reference_accepts_legacy_state_slot_field() {
        let reference: StoredOutputReference = serde_json::from_value(json!({
            "state_slot_id": 7,
            "definition_id": 7
        }))
        .unwrap();
        assert_eq!(reference.stored_output_id, StoredOutputId(7));
        reference.validate().unwrap();
        assert_eq!(
            serde_json::to_value(reference).unwrap(),
            json!({"stored_output_id": 7, "definition_id": 7})
        );
    }

    #[test]
    fn reused_payload_requires_an_older_compatible_plan_generation() {
        let mut instance = observed_instance(InstanceLifecycle::Persistent);
        let mut source = instance.catalog_generation.clone();
        source.plan_version -= 1;
        instance.reused_from_generation = Some(source.clone());
        instance.state_reference.generation = source.plan_version;
        instance.validate().unwrap();
        instance.reused_from_generation.as_mut().unwrap().plan_id += 1;
        assert!(instance.validate().is_err());
        instance.reused_from_generation = Some(instance.catalog_generation.clone());
        assert!(instance.validate().is_err());
    }

    #[test]
    fn invalid_range_and_lease_are_rejected() {
        let mut instance = observed_instance(InstanceLifecycle::Ephemeral {
            lease: EphemeralLease {
                lease_id: "lease".into(),
                owner_id: "fast-path".into(),
                issued_at_ms: 10,
                expires_at_ms: 20,
            },
        });
        instance.validate().unwrap();
        instance.time_range.end_ms = instance.time_range.start_ms;
        assert!(instance.validate().is_err());
        instance.time_range.end_ms = 10;
        if let InstanceLifecycle::Ephemeral { lease } = &mut instance.lifecycle {
            lease.expires_at_ms = lease.issued_at_ms;
        }
        assert!(instance.validate().is_err());
    }
    fn descriptor(k: u32, fidelity: FidelityGuarantee, version: u32) -> SummaryDescriptor {
        SummaryDescriptor::new(
            SummaryOperator::Sketch {
                algorithm: SketchAlgorithm::Kll,
                parameters: BTreeMap::from([("k".into(), json!(k))]),
            },
            fidelity,
            version,
        )
        .unwrap()
    }
    #[test]
    fn identity_includes_configuration_fidelity_and_state_schema() {
        let base = descriptor(
            200,
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        );
        assert_ne!(
            base.id,
            descriptor(
                201,
                FidelityGuarantee::KllRankError {
                    k: 201,
                    model: "rank.v1".into()
                },
                1
            )
            .id
        );
        assert!(SummaryDescriptor::new(
            SummaryOperator::Sketch {
                algorithm: SketchAlgorithm::Kll,
                parameters: BTreeMap::from([("k".into(), json!(200))]),
            },
            FidelityGuarantee::Exact,
            1,
        )
        .is_err());
        assert_ne!(
            base.id,
            descriptor(
                200,
                FidelityGuarantee::KllRankError {
                    k: 200,
                    model: "rank.v1".into()
                },
                2
            )
            .id
        );
    }
    #[test]
    fn group_order_and_duplicates_do_not_change_identity() {
        let a = DataDescriptor::new("cpu", "{job=\"a\"}", ["z".into(), "a".into(), "a".into()]);
        let b = DataDescriptor::new("cpu", "{job=\"a\"}", ["a".into(), "z".into()]);
        assert_eq!(a, b);
        assert_ne!(
            a.id,
            DataDescriptor::new("cpu", "{job=\"b\"}", ["a".into(), "z".into()]).id
        );
    }
    #[test]
    fn length_framing_distinguishes_delimiters_and_unicode() {
        assert_ne!(
            DataDescriptor::new("a|b", "c", []).id,
            DataDescriptor::new("a", "b|c", []).id
        );
        assert_ne!(
            DataDescriptor::new("π", "", ["x|y".into()]).id,
            DataDescriptor::new("π", "", ["x".into(), "y".into()]).id
        );
    }
    #[test]
    fn observation_semantics_is_part_of_data_identity() {
        assert_ne!(
            DataDescriptor::new_with_semantics("cpu", "", [], "samples.v1").id,
            DataDescriptor::new_with_semantics("cpu", "", [], "samples.v2").id
        );
    }

    #[test]
    fn source_and_value_projection_are_part_of_data_identity() {
        let metric = DataDescriptor::new("events", "", []);
        let table_value = DataDescriptor::new_typed(
            DataSourceIdentity::Table {
                table_ref: "events".into(),
            },
            ValueProjectionIdentity::Column {
                name: "value".into(),
            },
            "",
            [],
            "asap.timestamped-observations.v2",
        );
        let table_cost = DataDescriptor::new_typed(
            DataSourceIdentity::Table {
                table_ref: "events".into(),
            },
            ValueProjectionIdentity::Column {
                name: "cost".into(),
            },
            "",
            [],
            "asap.timestamped-observations.v2",
        );
        assert_ne!(metric.id, table_value.id);
        assert_ne!(table_value.id, table_cost.id);
        assert_eq!(metric.time_series_metric(), Some("events"));
        assert_eq!(table_value.time_series_metric(), None);
    }

    #[test]
    fn catalog_generation_accepts_legacy_digest_name() {
        let generation: CatalogGeneration = serde_json::from_value(json!({
            "schema_version": 1,
            "plan_id": 2,
            "plan_version": 3,
            "snapshot_digest": "abc"
        }))
        .unwrap();
        assert_eq!(generation.snapshot_sha256, "abc");
        assert!(serde_json::to_value(generation)
            .unwrap()
            .get("snapshot_digest")
            .is_none());
    }
    #[test]
    fn wire_roundtrip_and_tampered_id_validation() {
        let original = descriptor(
            200,
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        );
        let mut decoded: SummaryDescriptor =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded, original);
        decoded.state_schema_version = 2;
        assert!(decoded.validate().is_err());
        let mut data = DataDescriptor::new("cpu", "", []);
        data.source = DataSourceIdentity::TimeSeries {
            metric: "other".into(),
        };
        assert!(data.validate().is_err());
    }
    #[test]
    fn invalid_fidelity_is_rejected() {
        assert!(SummaryDescriptor::new(
            SummaryOperator::ExactAgg {
                agg_type: AggregationType::Sum,
                parameters_canonical: String::new()
            },
            FidelityGuarantee::DdSketchRelativeError { alpha: f64::NAN },
            1
        )
        .is_err());
        assert!(SummaryDescriptor::new(
            SummaryOperator::Configured {
                family: AggregationType::Sum.planner_exact_family().unwrap(),
                aggregation_type: AggregationType::Sum,
                aggregation_sub_type: String::new(),
                parameters: BTreeMap::new(),
            },
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        )
        .is_err());
        assert!(SummaryDescriptor::new(
            SummaryOperator::Sketch {
                algorithm: SketchAlgorithm::Kll,
                parameters: BTreeMap::from([("k".into(), json!(100))]),
            },
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        )
        .is_err());
    }

    #[test]
    fn configured_descriptor_rejects_family_storage_disagreement() {
        assert!(SummaryDescriptor::new(
            SummaryOperator::Configured {
                family: AggregationType::Rate.planner_exact_family().unwrap(),
                aggregation_type: AggregationType::Increase,
                aggregation_sub_type: String::new(),
                parameters: BTreeMap::new(),
            },
            FidelityGuarantee::ExactCounter {
                model: "prometheus.extrapolated-rate.v1".into(),
                full_pane_coverage_required: true,
            },
            2,
        )
        .is_err());
    }
    #[test]
    fn configured_identity_preserves_heap_hydra_and_subtype_and_excludes_population() {
        let yaml:serde_yaml::Value=serde_yaml::from_str("aggregationType: DDSketch\naggregationSubType: ''\nmetric: m\nlabels:\n  grouping: []\n  rollup: []\n  aggregated: []\nparameters:\n  relative_accuracy: 0.01\nwindowSize: 30\nwindowType: tumbling\nspatialFilter: ''\n").unwrap();
        let mut config =
            PrecomputeMaterialization::from_yaml_data(&yaml, None, crate::QueryLanguage::PromQl)
                .unwrap();
        assert!(matches!(
            SummaryDescriptor::from_config(&config).unwrap().fidelity,
            FidelityGuarantee::DdSketchRelativeError { .. }
        ));
        for (kind, key) in [
            (AggregationType::CountMinSketchWithHeap, "heap_size"),
            (AggregationType::HydraKLL, "row"),
            (AggregationType::HydraKLL, "col"),
            (AggregationType::HydraKLL, "k"),
        ] {
            config.aggregation_type = kind;
            config.parameters.insert(key.into(), json!(10));
            let before = SummaryDescriptor::from_config(&config).unwrap();
            config.parameters.insert(key.into(), json!(11));
            let after = SummaryDescriptor::from_config(&config).unwrap();
            assert_ne!(before.id, after.id, "{key}");
            config.metric = "other".into();
            config.spatial_filter_normalized = "job=a".into();
            assert_eq!(
                after.id,
                SummaryDescriptor::from_config(&config).unwrap().id
            );
        }
        let before = SummaryDescriptor::from_config(&config).unwrap();
        config.aggregation_sub_type = "max".into();
        assert_ne!(
            before.id,
            SummaryDescriptor::from_config(&config).unwrap().id
        );
    }
    #[test]
    fn increase_config_declares_prometheus_counter_fidelity() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "aggregationType: Increase\naggregationSubType: ''\nmetric: requests_total\nlabels:\n  grouping: []\n  rollup: []\n  aggregated: []\nparameters: {}\nwindowSize: 60\nwindowType: tumbling\nspatialFilter: ''\n",
        )
        .unwrap();
        let config =
            PrecomputeMaterialization::from_yaml_data(&yaml, None, crate::QueryLanguage::PromQl)
                .unwrap();
        assert!(matches!(
            SummaryDescriptor::from_config(&config).unwrap().fidelity,
            FidelityGuarantee::ExactCounter {
                ref model,
                full_pane_coverage_required: true
            } if model == "prometheus.extrapolated-rate.v1"
        ));
    }
    #[test]
    fn canonical_nested_parameters_and_model_versions_are_identity() {
        let a = SummaryOperator::Configured {
            family: AggregationType::Sum.planner_exact_family().unwrap(),
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: BTreeMap::from([("nested".into(), json!({"z":1,"a":2}))]),
        };
        let b = SummaryOperator::Configured {
            family: AggregationType::Sum.planner_exact_family().unwrap(),
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: BTreeMap::from([("nested".into(), json!({"a":2,"z":1}))]),
        };
        assert_eq!(
            SummaryDescriptor::new(a.clone(), FidelityGuarantee::Exact, 1)
                .unwrap()
                .id,
            SummaryDescriptor::new(b, FidelityGuarantee::Exact, 1)
                .unwrap()
                .id
        );
        let kll = SummaryOperator::Sketch {
            algorithm: SketchAlgorithm::Kll,
            parameters: BTreeMap::from([("k".into(), json!(200))]),
        };
        let first = SummaryDescriptor::new(
            kll.clone(),
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        )
        .unwrap();
        let second = SummaryDescriptor::new(
            kll,
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v2".into(),
            },
            1,
        )
        .unwrap();
        assert_ne!(first.id, second.id);
    }
    #[test]
    fn summary_definition_id_preserves_legacy_wire_identity() {
        let fingerprint = crate::PolicyFingerprint(42);
        let id = SummaryDefinitionId::from(fingerprint);
        assert_eq!(id.fingerprint(), fingerprint);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(crate::PolicyFingerprint::from(id), fingerprint);
        assert_eq!(
            serde_json::to_value(id).unwrap(),
            serde_json::to_value(fingerprint).unwrap()
        );
        assert_eq!(
            serde_json::from_str::<SummaryDefinitionId>("42").unwrap(),
            id
        );
    }
    /// Every supplied alias must agree with the declared fidelity, including runtime w/d keys.
    #[test]
    fn fidelity_rejects_conflicting_parameter_aliases() {
        use serde_json::json;
        let cases = [
            (
                FidelityGuarantee::CmsFrequencyError {
                    width: 128,
                    depth: 5,
                    model: "cms".into(),
                },
                json!({"w":128,"d":5,"width":128,"depth":5}),
                "w",
                json!(256),
            ),
            (
                FidelityGuarantee::CountSketchFrequencyError {
                    width: 128,
                    depth: 5,
                    model: "cs".into(),
                },
                json!({"w":128,"d":5,"width":128,"depth":5}),
                "d",
                json!(4),
            ),
            (
                FidelityGuarantee::KllRankError {
                    k: 200,
                    model: "kll".into(),
                },
                json!({"k":200,"K":200}),
                "K",
                json!(100),
            ),
            (
                FidelityGuarantee::HllCardinalityError {
                    precision: 12,
                    model: "hll".into(),
                },
                json!({"precision":12,"p":12}),
                "p",
                json!(10),
            ),
            (
                FidelityGuarantee::DdSketchRelativeError { alpha: 0.01 },
                json!({"alpha":0.01,"relative_accuracy":0.01}),
                "relative_accuracy",
                json!(0.1),
            ),
        ];
        for (fidelity, values, alias, conflict) in cases {
            let mut parameters: BTreeMap<String, Value> = serde_json::from_value(values).unwrap();
            assert!(fidelity.matches_parameters(&parameters));
            parameters.insert(alias.into(), conflict);
            assert!(!fidelity.matches_parameters(&parameters));
        }
        let cms = FidelityGuarantee::CmsFrequencyError {
            width: 128,
            depth: 5,
            model: "cms".into(),
        };
        assert!(!cms.matches_parameters(&BTreeMap::from([
            ("w".into(), json!(256)),
            ("d".into(), json!(5))
        ])));
    }
}
