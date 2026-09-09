//! Backend-owned physical representation of Self-Describing Summaries.
//!
//! Planner output is bound into these descriptors at the physical-plan edge.
//! Descriptors are content-interned and shared by every materialized instance;
//! pane-local state remains in `SketchStore`.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

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
    Unknown,
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
    summaries: RwLock<HashMap<SummaryDescriptorId, Arc<SummaryDescriptor>>>,
    data: RwLock<HashMap<DataDescriptorId, Arc<DataDescriptor>>>,
}

impl SummaryDescriptorRegistry {
    pub fn bind(&self, metadata: SketchInstanceMetadata) -> SdsBinding {
        let summary_key: Arc<str> = metadata.agg_kind.operator_canonical_string().into();
        let summary_id = SummaryDescriptorId(summary_key);
        let summary_descriptor = {
            let mut summaries = self.summaries.write().unwrap();
            Arc::clone(summaries.entry(summary_id.clone()).or_insert_with(|| {
                let fidelity = match (&metadata.agg_kind, &metadata.accuracy) {
                    (AggKind::ExactAgg { .. }, _) => FidelityGuarantee::Exact,
                    (AggKind::Sketch { .. }, Some(bound)) => {
                        FidelityGuarantee::Approximate(bound.clone())
                    }
                    (AggKind::Sketch { .. }, None) => FidelityGuarantee::Unknown,
                };
                Arc::new(SummaryDescriptor {
                    id: summary_id,
                    operator: SummaryOperator::from_agg_kind(&metadata.agg_kind),
                    fidelity,
                    state_schema_version: 1,
                })
            }))
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
            Arc::clone(data.entry(data_id.clone()).or_insert_with(|| {
                Arc::new(DataDescriptor {
                    id: data_id,
                    metric_name: Arc::from(metadata.metric_name.as_str()),
                    population_filter_canonical: Arc::from(filter),
                    group_by_keys: Arc::new(metadata.group_by_keys.clone()),
                })
            }))
        };

        SdsBinding {
            metadata: Arc::new(metadata),
            summary_descriptor,
            data_descriptor,
        }
    }

    pub fn summary_count(&self) -> usize {
        self.summaries.read().unwrap().len()
    }

    pub fn data_count(&self) -> usize {
        self.data.read().unwrap().len()
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
}
