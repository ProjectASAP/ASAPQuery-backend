//! Backend-owned physical representation of Self-Describing Summaries.
//!
//! Planner output is bound into these descriptors at the physical-plan edge.
//! Descriptors are content-interned and shared by every materialized instance;
//! pane-local state remains in `SketchStore`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use super::data::{AggKind, SketchConfig};
use super::index::SketchInstanceMetadata;
#[cfg(test)]
use crate::storage_engines::types::AggregationType;
pub use asap_types::sds::{
    DataDescriptor, DataDescriptorId, FidelityGuarantee, SummaryDescriptor, SummaryDescriptorId,
    SummaryOperator,
};

// Legacy records lack some state-shape dimensions (heap/Hydra/subtype). Do not
// let these projections masquerade as an authoritative Configured descriptor.
fn legacy_summary(kind: &AggKind) -> SummaryDescriptor {
    let fidelity = match kind {
        AggKind::ExactAgg { .. } => FidelityGuarantee::Exact,
        AggKind::Sketch { config, .. } => match config {
            SketchConfig::Kll { k } => FidelityGuarantee::KllRankError {
                k: *k,
                model: "asap.kll.normalized-rank.v1".into(),
            },
            SketchConfig::DDSketch { relative_accuracy } => {
                FidelityGuarantee::DdSketchRelativeError {
                    alpha: *relative_accuracy,
                }
            }
            SketchConfig::Hll { precision } => FidelityGuarantee::HllCardinalityError {
                precision: *precision,
                model: "asap.hll.relative-cardinality.v1".into(),
            },
            SketchConfig::CountMin { rows, cols } if *rows > 0 && *cols > 0 => {
                FidelityGuarantee::CmsFrequencyError {
                    width: *cols as u32,
                    depth: *rows as u32,
                    model: "asap.cms.point-frequency.v1".into(),
                }
            }
            SketchConfig::CountSketch { rows, cols } if *rows > 0 && *cols > 0 => {
                FidelityGuarantee::CountSketchFrequencyError {
                    width: *cols as u32,
                    depth: *rows as u32,
                    model: "asap.count-sketch.point-frequency.v1".into(),
                }
            }
            _ => FidelityGuarantee::Unknown {
                reason: "Legacy configuration has invalid dimensions".into(),
            },
        },
    };
    SummaryDescriptor::new(
        SummaryOperator::LegacyPartial {
            operator_canonical: kind.operator_canonical_string(),
        },
        fidelity,
        1,
    )
    .unwrap_or_else(|_| {
        SummaryDescriptor::new(
            SummaryOperator::LegacyPartial {
                operator_canonical: kind.operator_canonical_string(),
            },
            FidelityGuarantee::Unknown {
                reason: "Legacy configuration has invalid fidelity parameters".into(),
            },
            1,
        )
        .expect("unknown legacy descriptor has valid version and reason")
    })
}

/// Runtime foreign-key binding from one SeriesId to shared descriptors. Every pane
/// row stored under the SeriesId is a Summary Instance: `(binding, interval,
/// group-values, state)`. Descriptor references are normalized here instead of
/// copied into every pane row.
#[derive(Debug, Clone)]
pub struct SdsBinding {
    pub metadata: Arc<SketchInstanceMetadata>,
    pub summary_descriptor: Arc<SummaryDescriptor>,
    pub data_descriptor: Arc<DataDescriptor>,
}

impl std::ops::Deref for SdsBinding {
    type Target = SketchInstanceMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

/// Content-addressed descriptor registry owned by one SummaryStore.
#[derive(Default)]
pub struct SummaryDescriptorRegistry {
    // Weak values let descriptors disappear with their last SeriesId binding. The
    // registry must not turn retired materializations into a permanent leak.
    summaries: RwLock<HashMap<SummaryDescriptorId, Weak<SummaryDescriptor>>>,
    data: RwLock<HashMap<DataDescriptorId, Weak<DataDescriptor>>>,
    authoritative_catalog: RwLock<
        Option<(
            Arc<asap_types::summary_catalog::SummaryCatalog>,
            Arc<asap_types::sds::CatalogGeneration>,
        )>,
    >,
}

impl SummaryDescriptorRegistry {
    pub fn install_catalog(
        &self,
        catalog: Arc<asap_types::summary_catalog::SummaryCatalog>,
    ) -> Result<(), asap_types::summary_catalog::SummaryCatalogError> {
        catalog.validate()?;
        let reference = catalog.reference()?;
        let generation = Arc::new(asap_types::sds::CatalogGeneration {
            schema_version: reference.schema_version,
            plan_id: reference.plan_id,
            plan_version: reference.plan_version,
            snapshot_sha256: reference.snapshot_sha256,
        });
        *self.authoritative_catalog.write().unwrap() = Some((catalog, generation));
        Ok(())
    }

    pub fn authoritative_catalog(
        &self,
    ) -> Option<Arc<asap_types::summary_catalog::SummaryCatalog>> {
        self.authoritative_catalog
            .read()
            .unwrap()
            .as_ref()
            .map(|(catalog, _)| Arc::clone(catalog))
    }

    pub fn authoritative_snapshot(
        &self,
    ) -> Option<(
        Arc<asap_types::summary_catalog::SummaryCatalog>,
        Arc<asap_types::sds::CatalogGeneration>,
    )> {
        self.authoritative_catalog.read().unwrap().clone()
    }

    pub fn bind(&self, metadata: SketchInstanceMetadata) -> Result<SdsBinding, String> {
        let authoritative = self.authoritative_catalog();
        let configured = if let Some(catalog) = authoritative.as_ref() {
            if metadata.policy_fp.is_unset() {
                return Err(
                    "materialization identity is required by the installed SummaryCatalog".into(),
                );
            }
            let materialization = asap_types::sds::SummaryDefinitionId::from(metadata.policy_fp);
            let identity = catalog
                .materializations
                .get(&materialization)
                .ok_or_else(|| {
                    format!(
                        "materialization {} is absent from the installed SummaryCatalog",
                        materialization.as_u64()
                    )
                })?;
            Some((
                catalog.summary_descriptors[&identity.summary_descriptor_id].clone(),
                catalog.data_descriptors[&identity.data_descriptor_id].clone(),
            ))
        } else {
            None
        };
        let summary = configured
            .as_ref()
            .map(|(summary, _)| summary.clone())
            .unwrap_or_else(|| legacy_summary(&metadata.agg_kind));
        let summary_id = summary.id().clone();
        let summary_descriptor = {
            let mut summaries = self.summaries.write().unwrap();
            if let Some(existing) = summaries.get(&summary_id).and_then(Weak::upgrade) {
                existing
            } else {
                let descriptor = Arc::new(summary);
                summaries.insert(summary_id, Arc::downgrade(&descriptor));
                descriptor
            }
        };

        let data_descriptor_value = configured.map(|(_, data)| data).unwrap_or_else(|| {
            DataDescriptor::new(
                metadata.metric_name.clone(),
                metadata.agg_kind.spatial_filter_canonical(),
                metadata.group_by_keys.iter().cloned(),
            )
        });
        let data_id = data_descriptor_value.id().clone();
        let data_descriptor = {
            let mut data_registry = self.data.write().unwrap();
            if let Some(existing) = data_registry.get(&data_id).and_then(Weak::upgrade) {
                existing
            } else {
                let descriptor = Arc::new(data_descriptor_value);
                data_registry.insert(descriptor.id.clone(), Arc::downgrade(&descriptor));
                descriptor
            }
        };

        Ok(SdsBinding {
            metadata: Arc::new(metadata),
            summary_descriptor,
            data_descriptor,
        })
    }

    pub fn summary_count(&self) -> usize {
        self.summaries
            .read()
            .unwrap()
            .values()
            .filter(|descriptor| descriptor.strong_count() > 0)
            .count()
    }

    pub fn data_count(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .values()
            .filter(|descriptor| descriptor.strong_count() > 0)
            .count()
    }

    /// Remove dead weak entries after SeriesId retirement. Live snapshots remain
    /// valid and are collected by a later prune once their handles are gone.
    pub fn prune(&self) {
        self.summaries
            .write()
            .unwrap()
            .retain(|_, descriptor| descriptor.strong_count() > 0);
        self.data
            .write()
            .unwrap()
            .retain(|_, descriptor| descriptor.strong_count() > 0);
    }

    pub fn approx_resident_bytes(&self) -> usize {
        let summaries = self.summaries.read().unwrap();
        let mut total = summaries.capacity()
            * (std::mem::size_of::<SummaryDescriptorId>()
                + std::mem::size_of::<Weak<SummaryDescriptor>>());
        for descriptor in summaries.values().filter_map(Weak::upgrade) {
            total += std::mem::size_of::<SummaryDescriptor>() + descriptor.id.canonical().len();
            total += serde_json::to_string(&descriptor.operator).map_or(0, |value| value.len());
        }
        drop(summaries);

        let data = self.data.read().unwrap();
        total += data.capacity()
            * (std::mem::size_of::<DataDescriptorId>()
                + std::mem::size_of::<Weak<DataDescriptor>>());
        for descriptor in data.values().filter_map(Weak::upgrade) {
            total += std::mem::size_of::<DataDescriptor>() + descriptor.id.canonical().len();
            total += serde_json::to_string(&descriptor.source).map_or(0, |value| value.len());
            total +=
                serde_json::to_string(&descriptor.value_projection).map_or(0, |value| value.len());
            total += descriptor.population_filter_canonical.len();
            total += descriptor
                .group_by_keys
                .iter()
                .map(|key| std::mem::size_of::<String>() + key.len())
                .sum::<usize>();
        }
        total
    }
}

pub(crate) fn summary_descriptor_id(kind: &AggKind) -> SummaryDescriptorId {
    legacy_summary(kind).id
}

pub(crate) fn data_descriptor_id<'a>(
    metric: &str,
    filter: &str,
    group_by: impl Iterator<Item = &'a str>,
) -> DataDescriptorId {
    DataDescriptor::new(metric, filter, group_by.map(str::to_string)).id
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn metadata(
        sid: u64,
        metric: &str,
        filter: &str,
        agg_type: AggregationType,
        policy: u64,
    ) -> SketchInstanceMetadata {
        SketchInstanceMetadata {
            sid,
            metric_name: metric.into(),
            group_by_keys: BTreeSet::from(["job".into()]),
            capability: None,
            agg_kind: AggKind::ExactAgg {
                agg_type,
                parameters_canonical: "pane=5000;".into(),
                spatial_filter_canonical: filter.into(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint(policy),
        }
    }

    #[test]
    fn equivalent_materializations_share_both_descriptors() {
        let registry = SummaryDescriptorRegistry::default();
        let a = registry
            .bind(metadata(1, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 7))
            .unwrap();
        let b = registry
            .bind(metadata(2, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 8))
            .unwrap();

        assert!(Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 1));
    }

    #[test]
    fn population_changes_only_the_data_descriptor() {
        let registry = SummaryDescriptorRegistry::default();
        let a = registry
            .bind(metadata(1, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 7))
            .unwrap();
        let b = registry
            .bind(metadata(2, "cpu", r#"{zone="b"}"#, AggregationType::Sum, 7))
            .unwrap();

        assert!(Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(!Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 2));
    }

    #[test]
    fn operator_changes_only_the_summary_descriptor() {
        let registry = SummaryDescriptorRegistry::default();
        let a = registry
            .bind(metadata(1, "cpu", "", AggregationType::Sum, 7))
            .unwrap();
        let b = registry
            .bind(metadata(2, "cpu", "", AggregationType::MinMax, 7))
            .unwrap();

        assert!(!Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (2, 1));
    }

    #[test]
    fn registry_does_not_retain_descriptors_after_bindings_are_dropped() {
        let registry = SummaryDescriptorRegistry::default();
        let binding = registry
            .bind(metadata(1, "cpu", "", AggregationType::Sum, 7))
            .unwrap();
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 1));

        drop(binding);
        registry.prune();
        assert_eq!((registry.summary_count(), registry.data_count()), (0, 0));
    }

    #[test]
    fn configured_materializations_bind_only_to_authoritative_catalog_descriptors() {
        use asap_types::summary_catalog::SummaryCatalog;

        let summary = SummaryDescriptor::new(
            SummaryOperator::ExactAgg {
                agg_type: AggregationType::Sum,
                parameters_canonical: "authoritative=true".into(),
            },
            FidelityGuarantee::Exact,
            1,
        )
        .unwrap();
        let data = DataDescriptor::new("cpu", r#"{zone="a"}"#, ["job".into()]);
        let catalog = SummaryCatalog::build(
            7,
            1,
            [(
                asap_types::PolicyFingerprint(7),
                summary.clone(),
                data.clone(),
                asap_types::WindowMaterializationLayout::Pane { pane_secs: 60 },
            )],
        )
        .unwrap();
        let registry = SummaryDescriptorRegistry::default();
        registry.install_catalog(Arc::new(catalog)).unwrap();

        let binding = registry
            .bind(metadata(
                1,
                "wrong-local-copy",
                "",
                AggregationType::MinMax,
                7,
            ))
            .unwrap();
        assert_eq!(binding.summary_descriptor.as_ref(), &summary);
        assert_eq!(binding.data_descriptor.as_ref(), &data);
        assert!(registry
            .bind(metadata(2, "cpu", "", AggregationType::Sum, 8))
            .is_err());
        assert!(registry
            .bind(metadata(3, "cpu", "", AggregationType::Sum, 0))
            .is_err());
    }
}
