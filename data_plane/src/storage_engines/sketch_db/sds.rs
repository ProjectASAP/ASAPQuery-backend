//! Backend-owned physical representation of Self-Describing Summaries.
//!
//! Planner output is bound into these descriptors at the physical-plan edge.
//! Descriptors are content-interned and shared by every materialized instance;
//! pane-local state remains in `SketchStore`.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock, Weak};

use super::data::{AccuracyBound, AggKind, SketchAlgorithm, SketchConfig};
use super::index::SketchInstanceMetadata;
use crate::storage_engines::types::AggregationType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SummaryDescriptorId(Arc<str>);

impl SummaryDescriptorId {
    pub fn canonical(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataDescriptorId(Arc<str>);

impl DataDescriptorId {
    pub fn canonical(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum SummaryOperator {
    Sketch {
        algorithm: SketchAlgorithm,
        config: SketchConfig,
    },
    ExactAgg {
        agg_type: AggregationType,
        parameters_canonical: Arc<str>,
    },
}

impl SummaryOperator {
    fn from_agg_kind(kind: &AggKind) -> Self {
        match kind {
            AggKind::Sketch {
                algorithm, config, ..
            } => Self::Sketch {
                algorithm: algorithm.clone(),
                config: config.clone(),
            },
            AggKind::ExactAgg {
                agg_type,
                parameters_canonical,
                ..
            } => Self::ExactAgg {
                agg_type: *agg_type,
                parameters_canonical: Arc::from(parameters_canonical.as_str()),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum FidelityGuarantee {
    Exact,
    Approximate(AccuracyBound),
}

#[derive(Debug)]
pub struct SummaryDescriptor {
    pub id: SummaryDescriptorId,
    pub operator: SummaryOperator,
    pub fidelity: FidelityGuarantee,
    pub state_schema_version: u32,
}

#[derive(Debug)]
pub struct DataDescriptor {
    pub id: DataDescriptorId,
    pub metric_name: Arc<str>,
    pub population_filter_canonical: Arc<str>,
    pub group_by_keys: Arc<BTreeSet<String>>,
}

/// Runtime foreign-key binding from one SID to shared descriptors. Every pane
/// row stored under the SID is a Summary Instance: `(binding, interval,
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
    // Weak values let descriptors disappear with their last SID binding. The
    // registry must not turn retired materializations into a permanent leak.
    summaries: RwLock<HashMap<SummaryDescriptorId, Weak<SummaryDescriptor>>>,
    data: RwLock<HashMap<DataDescriptorId, Weak<DataDescriptor>>>,
}

impl SummaryDescriptorRegistry {
    pub fn bind(&self, metadata: SketchInstanceMetadata) -> SdsBinding {
        let summary_key: Arc<str> = metadata.agg_kind.operator_canonical_string().into();
        let summary_id = SummaryDescriptorId(summary_key);
        let summary_descriptor = {
            let mut summaries = self.summaries.write().unwrap();
            if let Some(existing) = summaries.get(&summary_id).and_then(Weak::upgrade) {
                existing
            } else {
                let fidelity = match metadata.agg_kind.capability_and_accuracy().1 {
                    Some(bound) => FidelityGuarantee::Approximate(bound),
                    None => FidelityGuarantee::Exact,
                };
                let descriptor = Arc::new(SummaryDescriptor {
                    id: summary_id.clone(),
                    operator: SummaryOperator::from_agg_kind(&metadata.agg_kind),
                    fidelity,
                    state_schema_version: 1,
                });
                summaries.insert(summary_id, Arc::downgrade(&descriptor));
                descriptor
            }
        };

        let filter = metadata.agg_kind.spatial_filter_canonical();
        let data_key: Arc<str> = canonical_data_key(
            &metadata.metric_name,
            filter,
            metadata.group_by_keys.iter().map(String::as_str),
        )
        .into();
        let data_id = DataDescriptorId(data_key);
        let data_descriptor = {
            let mut data = self.data.write().unwrap();
            if let Some(existing) = data.get(&data_id).and_then(Weak::upgrade) {
                existing
            } else {
                let descriptor = Arc::new(DataDescriptor {
                    id: data_id,
                    metric_name: Arc::from(metadata.metric_name.as_str()),
                    population_filter_canonical: Arc::from(filter),
                    group_by_keys: Arc::new(metadata.group_by_keys.clone()),
                });
                data.insert(descriptor.id.clone(), Arc::downgrade(&descriptor));
                descriptor
            }
        };

        SdsBinding {
            metadata: Arc::new(metadata),
            summary_descriptor,
            data_descriptor,
        }
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

    /// Remove dead weak entries after SID retirement. Live snapshots remain
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
            total += std::mem::size_of::<SummaryDescriptor>() + descriptor.id.0.len();
            if let SummaryOperator::ExactAgg {
                parameters_canonical,
                ..
            } = &descriptor.operator
            {
                total += parameters_canonical.len();
            }
        }
        drop(summaries);

        let data = self.data.read().unwrap();
        total += data.capacity()
            * (std::mem::size_of::<DataDescriptorId>()
                + std::mem::size_of::<Weak<DataDescriptor>>());
        for descriptor in data.values().filter_map(Weak::upgrade) {
            total += std::mem::size_of::<DataDescriptor>() + descriptor.id.0.len();
            total += descriptor.metric_name.len() + descriptor.population_filter_canonical.len();
            total += descriptor
                .group_by_keys
                .iter()
                .map(|key| std::mem::size_of::<String>() + key.len())
                .sum::<usize>();
        }
        total
    }
}

fn canonical_data_key<'a>(
    metric: &str,
    filter: &str,
    group_by: impl Iterator<Item = &'a str>,
) -> String {
    fn push_part(out: &mut String, value: &str) {
        use std::fmt::Write;
        let _ = write!(out, "{}:{value}", value.len());
    }
    let mut out = String::from("data:v1|");
    push_part(&mut out, metric);
    out.push('|');
    push_part(&mut out, filter);
    for key in group_by {
        out.push('|');
        push_part(&mut out, key);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let a = registry.bind(metadata(1, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 7));
        let b = registry.bind(metadata(2, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 8));

        assert!(Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 1));
    }

    #[test]
    fn population_changes_only_the_data_descriptor() {
        let registry = SummaryDescriptorRegistry::default();
        let a = registry.bind(metadata(1, "cpu", r#"{zone="a"}"#, AggregationType::Sum, 7));
        let b = registry.bind(metadata(2, "cpu", r#"{zone="b"}"#, AggregationType::Sum, 7));

        assert!(Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(!Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 2));
    }

    #[test]
    fn operator_changes_only_the_summary_descriptor() {
        let registry = SummaryDescriptorRegistry::default();
        let a = registry.bind(metadata(1, "cpu", "", AggregationType::Sum, 7));
        let b = registry.bind(metadata(2, "cpu", "", AggregationType::MinMax, 7));

        assert!(!Arc::ptr_eq(&a.summary_descriptor, &b.summary_descriptor));
        assert!(Arc::ptr_eq(&a.data_descriptor, &b.data_descriptor));
        assert_eq!((registry.summary_count(), registry.data_count()), (2, 1));
    }

    #[test]
    fn registry_does_not_retain_descriptors_after_bindings_are_dropped() {
        let registry = SummaryDescriptorRegistry::default();
        let binding = registry.bind(metadata(1, "cpu", "", AggregationType::Sum, 7));
        assert_eq!((registry.summary_count(), registry.data_count()), (1, 1));

        drop(binding);
        registry.prune();
        assert_eq!((registry.summary_count(), registry.data_count()), (0, 0));
    }
}
