//! Authoritative descriptor snapshot for a compiled physical plan.
//!
//! Execution plans keep their compatibility fields during migration. This
//! catalog owns semantic definitions, not producer placement or pane state.

use std::collections::BTreeMap;

use asap_types::sds::{DataDescriptor, DataDescriptorId, SummaryDescriptor, SummaryDescriptorId};
use asap_types::PolicyFingerprint;
use serde::{Deserialize, Serialize};

pub const SUMMARY_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Stable materialization identity binds operator and population descriptors.
/// Concrete intervals, groups and completeness belong to runtime instances.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterializationIdentity {
    pub summary_descriptor_id: SummaryDescriptorId,
    pub data_descriptor_id: DataDescriptorId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SummaryCatalog {
    pub schema_version: u32,
    pub plan_id: u64,
    pub plan_version: u64,
    pub summary_descriptors: BTreeMap<SummaryDescriptorId, SummaryDescriptor>,
    pub data_descriptors: BTreeMap<DataDescriptorId, DataDescriptor>,
    pub materializations: BTreeMap<PolicyFingerprint, MaterializationIdentity>,
}

#[derive(Debug, thiserror::Error)]
pub enum SummaryCatalogError {
    #[error("unsupported summary catalog schema version {0}")]
    SchemaVersion(u32),
    #[error("invalid summary catalog descriptor: {0}")]
    Descriptor(String),
    #[error("materialization {0} has conflicting descriptor bindings")]
    ConflictingMaterialization(u64),
    #[error("materialization {0} references a missing descriptor")]
    MissingDescriptor(u64),
}

impl SummaryCatalog {
    pub fn from_materializations(
        plan_id: u64,
        plan_version: u64,
        materializations: &[asap_types::PrecomputeMaterialization],
    ) -> Result<Self, SummaryCatalogError> {
        let entries = materializations
            .iter()
            .map(|config| {
                let summary = SummaryDescriptor::from_config(config)
                    .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
                let data = DataDescriptor::new(
                    config.metric.clone(),
                    asap_types::utils::normalize_spatial_filter(&config.spatial_filter),
                    config.grouping_labels.labels.clone(),
                );
                Ok((config.policy_fingerprint(), summary, data))
            })
            .collect::<Result<Vec<_>, SummaryCatalogError>>()?;
        Self::build(plan_id, plan_version, entries)
    }

    pub fn build(
        plan_id: u64,
        plan_version: u64,
        entries: impl IntoIterator<Item = (PolicyFingerprint, SummaryDescriptor, DataDescriptor)>,
    ) -> Result<Self, SummaryCatalogError> {
        let mut catalog = Self {
            schema_version: SUMMARY_CATALOG_SCHEMA_VERSION,
            plan_id,
            plan_version,
            summary_descriptors: BTreeMap::new(),
            data_descriptors: BTreeMap::new(),
            materializations: BTreeMap::new(),
        };
        for (materialization, summary, data) in entries {
            summary
                .validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            data.validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            let binding = MaterializationIdentity {
                summary_descriptor_id: summary.id().clone(),
                data_descriptor_id: data.id().clone(),
            };
            if catalog
                .materializations
                .get(&materialization)
                .is_some_and(|old| old != &binding)
            {
                return Err(SummaryCatalogError::ConflictingMaterialization(
                    materialization.0,
                ));
            }
            catalog
                .summary_descriptors
                .insert(binding.summary_descriptor_id.clone(), summary);
            catalog
                .data_descriptors
                .insert(binding.data_descriptor_id.clone(), data);
            catalog.materializations.insert(materialization, binding);
        }
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), SummaryCatalogError> {
        if self.schema_version != SUMMARY_CATALOG_SCHEMA_VERSION {
            return Err(SummaryCatalogError::SchemaVersion(self.schema_version));
        }
        for (key, descriptor) in &self.summary_descriptors {
            descriptor
                .validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            if key != descriptor.id() {
                return Err(SummaryCatalogError::Descriptor(
                    "summary table key differs from content identity".into(),
                ));
            }
        }
        for (key, descriptor) in &self.data_descriptors {
            descriptor
                .validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            if key != descriptor.id() {
                return Err(SummaryCatalogError::Descriptor(
                    "data table key differs from content identity".into(),
                ));
            }
        }
        for (id, binding) in &self.materializations {
            if !self
                .summary_descriptors
                .contains_key(&binding.summary_descriptor_id)
                || !self
                    .data_descriptors
                    .contains_key(&binding.data_descriptor_id)
            {
                return Err(SummaryCatalogError::MissingDescriptor(id.0));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    fn config(metric: &str, filter: &str, window: u64) -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::new(vec!["job".into()]),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            window,
            window,
            WindowKind::Tumbling,
            filter.into(),
            metric.into(),
            None,
            None,
            None,
        )
    }

    // Panes share definitions but retain distinct physical materialization IDs.
    #[test]
    fn shares_descriptors_across_windows_and_deduplicates_materializations() {
        let one = config("requests", "", 60);
        let two = config("requests", "", 120);
        let catalog =
            SummaryCatalog::from_materializations(7, 2, &[one.clone(), two, one]).unwrap();
        assert_eq!(catalog.summary_descriptors.len(), 1);
        assert_eq!(catalog.data_descriptors.len(), 1);
        assert_eq!(catalog.materializations.len(), 2);
        assert_eq!((catalog.plan_id, catalog.plan_version), (7, 2));
    }

    // Source/population changes never alias, while the operator can be reused.
    #[test]
    fn separates_population_and_operator_identity() {
        let catalog = SummaryCatalog::from_materializations(
            1,
            1,
            &[
                config("requests", "", 60),
                config("errors", "", 60),
                config("requests", "job=user", 60),
            ],
        )
        .unwrap();
        assert_eq!(catalog.summary_descriptors.len(), 1);
        assert_eq!(catalog.data_descriptors.len(), 3);
    }

    // Operator changes share population metadata without sharing state identity.
    #[test]
    fn changing_operator_parameters_creates_a_new_summary_descriptor() {
        let mut a = config("requests", "", 60);
        a.aggregation_type = AggregationType::DatasketchesKLL;
        a.parameters.insert("K".into(), serde_json::json!(100));
        let mut b = a.clone();
        b.parameters.insert("K".into(), serde_json::json!(200));
        let catalog = SummaryCatalog::from_materializations(1, 1, &[a, b]).unwrap();
        assert_eq!(catalog.summary_descriptors.len(), 2);
        assert_eq!(catalog.data_descriptors.len(), 1);
        assert_eq!(catalog.materializations.len(), 2);
    }

    // Construction order cannot affect the published snapshot bytes.
    #[test]
    fn snapshot_is_deterministic_and_round_trips() {
        let a = config("requests", "", 60);
        let b = config("errors", "", 60);
        let forward = SummaryCatalog::from_materializations(7, 2, &[a.clone(), b.clone()]).unwrap();
        let backward = SummaryCatalog::from_materializations(7, 2, &[b, a]).unwrap();
        let bytes = serde_json::to_vec(&forward).unwrap();
        assert_eq!(bytes, serde_json::to_vec(&backward).unwrap());
        let decoded: SummaryCatalog = serde_json::from_slice(&bytes).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded, forward);
    }

    // The same materialization cannot silently rebind to another population.
    #[test]
    fn conflicting_materialization_is_rejected() {
        let first = config("requests", "", 60);
        let second = config("errors", "", 60);
        let catalog =
            SummaryCatalog::from_materializations(1, 1, &[first.clone(), second]).unwrap();
        let summary = catalog.summary_descriptors.values().next().unwrap().clone();
        let data = catalog
            .data_descriptors
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let error = SummaryCatalog::build(
            1,
            1,
            [
                (first.policy_fingerprint(), summary.clone(), data[0].clone()),
                (first.policy_fingerprint(), summary, data[1].clone()),
            ],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            SummaryCatalogError::ConflictingMaterialization(_)
        ));
    }

    // Imported catalogs must resolve every foreign key and content identity.
    #[test]
    fn rejects_dangling_refs_tampered_keys_and_unknown_schema() {
        let catalog =
            SummaryCatalog::from_materializations(1, 1, &[config("requests", "", 60)]).unwrap();
        let mut broken = catalog.clone();
        broken.data_descriptors.clear();
        assert!(matches!(
            broken.validate(),
            Err(SummaryCatalogError::MissingDescriptor(_))
        ));
        let mut broken = catalog.clone();
        let (_, descriptor) = broken.summary_descriptors.pop_first().unwrap();
        broken.summary_descriptors.insert(
            serde_json::from_value(serde_json::json!("forged")).unwrap(),
            descriptor,
        );
        assert!(matches!(
            broken.validate(),
            Err(SummaryCatalogError::Descriptor(_))
        ));
        let mut broken = catalog.clone();
        broken
            .summary_descriptors
            .values_mut()
            .next()
            .unwrap()
            .state_schema_version += 1;
        assert!(matches!(
            broken.validate(),
            Err(SummaryCatalogError::Descriptor(_))
        ));
        let mut broken = catalog;
        broken.schema_version = SUMMARY_CATALOG_SCHEMA_VERSION + 1;
        assert!(matches!(
            broken.validate(),
            Err(SummaryCatalogError::SchemaVersion(_))
        ));
    }

    // Native exact-only plans have a valid empty catalog, not dummy state.
    #[test]
    fn empty_catalog_is_valid() {
        let catalog = SummaryCatalog::from_materializations(1, 1, &[]).unwrap();
        assert!(catalog.materializations.is_empty());
        catalog.validate().unwrap();
    }
}
