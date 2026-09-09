//! Shared SDS metadata contracts. Summary payload bytes remain storage-engine
//! owned; catalogs and inventories contain identities and state references only.
use crate::{AggregationType, PrecomputeMaterialization};
use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryFamilyType};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

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
/// but distinct from descriptor IDs and runtime instance/SID identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MaterializationId(pub crate::PolicyFingerprint);
impl MaterializationId {
    pub fn fingerprint(self) -> crate::PolicyFingerprint {
        self.0
    }
    pub fn as_u64(self) -> u64 {
        self.0 .0
    }
}
impl From<crate::PolicyFingerprint> for MaterializationId {
    fn from(value: crate::PolicyFingerprint) -> Self {
        Self(value)
    }
}
impl From<MaterializationId> for crate::PolicyFingerprint {
    fn from(value: MaterializationId) -> Self {
        value.0
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
}

/// Immutable identity of the desired catalog generation used to create state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogGeneration {
    pub schema_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    pub snapshot_digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HalfOpenTimeRange {
    pub start_ms: i64,
    pub end_ms: i64,
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
    pub materialization_id: MaterializationId,
    pub summary_descriptor_id: SummaryDescriptorId,
    pub data_descriptor_id: DataDescriptorId,
    pub time_range: HalfOpenTimeRange,
    pub group_values: BTreeMap<String, String>,
    pub catalog_generation: CatalogGeneration,
    pub placement: SummaryPlacement,
    pub state_reference: SummaryStateReference,
    pub status: SummaryInstanceStatus,
    pub completeness: InstanceCompleteness,
    pub lifecycle: InstanceLifecycle,
    pub observed_at_ms: i64,
}

impl SummaryInstance {
    pub fn validate(&self) -> Result<(), SdsError> {
        if self.time_range.start_ms >= self.time_range.end_ms {
            return Err(SdsError(
                "summary instance time range must be non-empty".into(),
            ));
        }
        if self.catalog_generation.schema_version == 0
            || self.catalog_generation.snapshot_digest.is_empty()
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
        if self.state_reference.store.is_empty()
            || self.state_reference.key.is_empty()
            || self.state_reference.state_schema_version == 0
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
    pub reporter_id: String,
    pub inventory_version: u64,
    pub observed_at_ms: i64,
    pub instances: BTreeMap<SummaryInstanceId, SummaryInstance>,
}

impl ObservedSummaryInventory {
    pub fn validate(&self) -> Result<(), SdsError> {
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
        aggregation_type: AggregationType,
        aggregation_sub_type: String,
        parameters: BTreeMap<String, Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FidelityGuarantee {
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
                | planner_types::post_asap::ExactKind::Rate,
                _,
            )) => Self::ExactCounter {
                model: "prometheus.extrapolated-rate.v1".into(),
                full_pane_coverage_required: true,
            },
            Ok(SummaryFamilyType::ExactAggregate(..)) => Self::Exact,
            Ok(SummaryFamilyType::Sketch(kind, _)) => match kind.params() {
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
        let content = json!({"operator":operator,"fidelity":fidelity,"state_schema_version":state_schema_version});
        let id = SummaryDescriptorId(format!("summary:v2:{}", canonical(&content)));
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataDescriptor {
    pub id: DataDescriptorId,
    pub metric_name: String,
    pub population_filter_canonical: String,
    pub group_by_keys: BTreeSet<String>,
    /// Versioned contract for value projection, timestamp interpretation and
    /// missing/duplicate/invalid observation handling.
    pub observation_semantics: String,
}
impl DataDescriptor {
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
        let metric_name = metric.into();
        let population_filter_canonical = filter.into();
        let group_by_keys = group_by.into_iter().collect();
        let observation_semantics = observation_semantics.into();
        let id = data_descriptor_id(
            &metric_name,
            &population_filter_canonical,
            &group_by_keys,
            &observation_semantics,
        );
        Self {
            id,
            metric_name,
            population_filter_canonical,
            group_by_keys,
            observation_semantics,
        }
    }
    pub fn id(&self) -> &DataDescriptorId {
        &self.id
    }
    pub fn validate(&self) -> Result<(), SdsError> {
        if self.id
            != data_descriptor_id(
                &self.metric_name,
                &self.population_filter_canonical,
                &self.group_by_keys,
                &self.observation_semantics,
            )
        {
            return Err(SdsError("data descriptor ID/content mismatch".into()));
        }
        Ok(())
    }
}
fn data_descriptor_id(
    metric: &str,
    filter: &str,
    group_by: &BTreeSet<String>,
    observation_semantics: &str,
) -> DataDescriptorId {
    // Preserve the existing v1 length-framed data identity, now normalizing the
    // grouping set at the shared contract boundary.
    let mut key = format!(
        "data:v1|{}:{metric}|{}:{filter}",
        metric.len(),
        filter.len()
    );
    for name in group_by {
        key.push_str(&format!("|{}:{name}", name.len()));
    }
    key.push_str(&format!(
        "|{}:{observation_semantics}",
        observation_semantics.len()
    ));
    DataDescriptorId(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observed_instance(lifecycle: InstanceLifecycle) -> SummaryInstance {
        SummaryInstance {
            instance_id: SummaryInstanceId::new("instance-1").unwrap(),
            materialization_id: MaterializationId(crate::PolicyFingerprint(7)),
            summary_descriptor_id: descriptor(
                200,
                FidelityGuarantee::Unknown {
                    reason: "test".into(),
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
                snapshot_digest: "abc".into(),
            },
            placement: SummaryPlacement {
                producer_id: "producer".into(),
                storage_node_id: "store".into(),
            },
            state_reference: SummaryStateReference {
                store: "summary-store".into(),
                key: "state/1".into(),
                state_schema_version: 1,
                generation: 1,
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
            reporter_id: "store".into(),
            inventory_version: 4,
            observed_at_ms: 10,
            instances: BTreeMap::from([(instance.instance_id.clone(), instance)]),
        };
        inventory.validate().unwrap();
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
            FidelityGuarantee::Unknown {
                reason: "not supplied".into(),
            },
            1,
        );
        assert_ne!(
            base.id,
            descriptor(
                201,
                FidelityGuarantee::Unknown {
                    reason: "not supplied".into()
                },
                1
            )
            .id
        );
        assert_ne!(base.id, descriptor(200, FidelityGuarantee::Exact, 1).id);
        assert_ne!(
            base.id,
            descriptor(
                200,
                FidelityGuarantee::Unknown {
                    reason: "not supplied".into()
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
    fn wire_roundtrip_and_tampered_id_validation() {
        let original = descriptor(
            200,
            FidelityGuarantee::Unknown {
                reason: "not supplied".into(),
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
        data.metric_name = "other".into();
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
    }
    #[test]
    fn configured_identity_preserves_heap_hydra_and_subtype_and_excludes_population() {
        let yaml:serde_yaml::Value=serde_yaml::from_str("aggregationType: DDSketch\naggregationSubType: ''\nmetric: m\nlabels:\n  grouping: []\n  rollup: []\n  aggregated: []\nparameters:\n  relative_accuracy: 0.01\nwindowSize: 30\nwindowType: tumbling\nspatialFilter: ''\n").unwrap();
        let mut config =
            PrecomputeMaterialization::from_yaml_data(&yaml, None, crate::QueryLanguage::promql)
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
            PrecomputeMaterialization::from_yaml_data(&yaml, None, crate::QueryLanguage::promql)
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
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: BTreeMap::from([("nested".into(), json!({"z":1,"a":2}))]),
        };
        let b = SummaryOperator::Configured {
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
        let first = SummaryDescriptor::new(
            a.clone(),
            FidelityGuarantee::KllRankError {
                k: 200,
                model: "rank.v1".into(),
            },
            1,
        )
        .unwrap();
        let second = SummaryDescriptor::new(
            a,
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
    fn materialization_id_preserves_legacy_wire_identity() {
        let fingerprint = crate::PolicyFingerprint(42);
        let id = MaterializationId::from(fingerprint);
        assert_eq!(id.fingerprint(), fingerprint);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(crate::PolicyFingerprint::from(id), fingerprint);
        assert_eq!(
            serde_json::to_value(id).unwrap(),
            serde_json::to_value(fingerprint).unwrap()
        );
        assert_eq!(serde_json::from_str::<MaterializationId>("42").unwrap(), id);
    }
}
