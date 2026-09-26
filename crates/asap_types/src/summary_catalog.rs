//! Authoritative descriptor snapshot for a compiled physical plan.
//!
//! The catalog owns semantic definitions; executable plans own writer and
//! reader bindings, while runtime inventory owns placement and pane state.

use std::collections::BTreeMap;

use crate::sds::{
    CatalogGeneration, DataDescriptor, DataDescriptorId, DataSourceIdentity, StoredOutputId,
    SummaryDescriptor, SummaryDescriptorId,
};
use crate::PolicyFingerprint;
use serde::{Deserialize, Serialize};

pub const SUMMARY_CATALOG_SCHEMA_VERSION: u32 = 4;

/// Canonical definition binds operator and population descriptors. Writer
/// layout and concrete state belong to installed plans and runtime instances.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoredOutputDefinition {
    pub definition_id: crate::sds::SummaryDefinitionId,
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
    pub outputs: BTreeMap<StoredOutputId, StoredOutputDefinition>,
    pub definitions:
        BTreeMap<crate::sds::SummaryDefinitionId, crate::summary_semantics::SummaryDefinition>,
}

#[derive(Debug, thiserror::Error)]
pub enum SummaryCatalogError {
    #[error("unsupported summary catalog schema version {0}")]
    SchemaVersion(u32),
    #[error("invalid summary catalog descriptor: {0}")]
    Descriptor(String),
    #[error("definition {0} has conflicting descriptor bindings")]
    ConflictingDefinition(u64),
    #[error("definition {0} references a missing descriptor")]
    MissingDescriptor(u64),
    #[error("catalog reference does not identify the supplied snapshot")]
    ReferenceMismatch,
}

impl CatalogGeneration {
    #[cfg(test)]
    /// Validate an untrusted wire reference against the installed immutable
    /// snapshot and its enclosing plan generation.
    pub fn validate_snapshot(
        &self,
        catalog: &SummaryCatalog,
        plan_id: u64,
        plan_version: u64,
    ) -> Result<(), SummaryCatalogError> {
        let expected = catalog.reference()?;
        if self != &expected || (plan_id, plan_version) != (catalog.plan_id, catalog.plan_version) {
            return Err(SummaryCatalogError::ReferenceMismatch);
        }
        Ok(())
    }
}

impl SummaryCatalog {
    pub fn output_reference(
        &self,
        id: StoredOutputId,
    ) -> Result<crate::sds::StoredOutputReference, SummaryCatalogError> {
        let output = self
            .outputs
            .get(&id)
            .ok_or(SummaryCatalogError::MissingDescriptor(id.as_u64()))?;
        Ok(crate::sds::StoredOutputReference {
            stored_output_id: id,
            definition_id: output.definition_id.clone(),
        })
    }

    pub fn reference(&self) -> Result<CatalogGeneration, SummaryCatalogError> {
        use sha2::{Digest, Sha256};
        self.validate()?;
        // BTreeMap tables and typed descriptor fields serialize deterministically.
        let bytes = serde_json::to_vec(self)
            .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
        Ok(CatalogGeneration {
            schema_version: self.schema_version,
            plan_id: self.plan_id,
            plan_version: self.plan_version,
            snapshot_sha256: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    pub fn from_materializations(
        plan_id: u64,
        plan_version: u64,
        materializations: &[crate::PrecomputeMaterialization],
    ) -> Result<Self, SummaryCatalogError> {
        let entries = materializations
            .iter()
            .map(|config| {
                let summary = SummaryDescriptor::from_config(config)
                    .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
                let source = config.source_identity();
                let value_projection = config.effective_value_projection().clone();
                let data = DataDescriptor::new_typed(
                    source,
                    value_projection,
                    config
                        .population_filter_canonical()
                        .map_err(SummaryCatalogError::Descriptor)?,
                    config.grouping_labels.names(),
                    if config.table_name.is_some() && !config.grouping_labels.is_empty() {
                        crate::grouping_projection::TABLE_GROUP_OBSERVATION_SEMANTICS
                    } else {
                        crate::sds::TIMESTAMPED_OBSERVATION_SEMANTICS
                    },
                )
                .with_grouping_projection(config.grouping_labels.clone())
                .with_partitioning(config.partitioning)
                .with_population_key_encoding(config.population_key_encoding)
                .with_timestamp_column(config.table_timestamp_column.clone());
                Ok((config.policy_fingerprint(), summary, data))
            })
            .collect::<Result<Vec<_>, SummaryCatalogError>>()?;
        let mut catalog = Self::build(plan_id, plan_version, entries)?;
        let mut semantic_bindings = BTreeMap::new();
        for config in materializations {
            let output = catalog
                .outputs
                .get_mut(&StoredOutputId::from(config.policy_fingerprint()))
                .unwrap();
            let definition = crate::summary_semantics::SummaryDefinition::from_descriptors(
                &catalog.summary_descriptors[&output.summary_descriptor_id],
                &catalog.data_descriptors[&output.data_descriptor_id],
            )?
            .with_config(config);
            let id = definition.id()?;
            if semantic_bindings
                .insert(config.policy_fingerprint(), id.clone())
                .is_some_and(|old| old != id)
            {
                return Err(SummaryCatalogError::ConflictingDefinition(
                    config.policy_fingerprint().0,
                ));
            }
            output.definition_id = id.clone();
            catalog.definitions.insert(id, definition);
        }
        let used: std::collections::BTreeSet<_> = catalog
            .outputs
            .values()
            .map(|o| o.definition_id.clone())
            .collect();
        catalog.definitions.retain(|id, _| used.contains(id));
        catalog.validate()?;
        Ok(catalog)
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
            outputs: BTreeMap::new(),
            definitions: BTreeMap::new(),
        };
        for (fingerprint, summary, data) in entries {
            let definition = StoredOutputId::from(fingerprint);
            summary
                .validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            data.validate()
                .map_err(|error| SummaryCatalogError::Descriptor(error.to_string()))?;
            let semantics =
                crate::summary_semantics::SummaryDefinition::from_descriptors(&summary, &data)?;
            let semantic_id = semantics.id()?;
            catalog.definitions.insert(semantic_id.clone(), semantics);
            let binding = StoredOutputDefinition {
                definition_id: semantic_id,
                summary_descriptor_id: summary.id().clone(),
                data_descriptor_id: data.id().clone(),
            };
            if catalog
                .outputs
                .get(&definition)
                .is_some_and(|old| old != &binding)
            {
                return Err(SummaryCatalogError::ConflictingDefinition(
                    definition.as_u64(),
                ));
            }
            catalog
                .summary_descriptors
                .insert(binding.summary_descriptor_id.clone(), summary);
            catalog
                .data_descriptors
                .insert(binding.data_descriptor_id.clone(), data);
            catalog.outputs.insert(definition, binding);
        }
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), SummaryCatalogError> {
        if self.schema_version != SUMMARY_CATALOG_SCHEMA_VERSION {
            return Err(SummaryCatalogError::SchemaVersion(self.schema_version));
        }
        for (id, definition) in &self.definitions {
            if &definition.id()? != id {
                return Err(SummaryCatalogError::Descriptor(
                    "semantic definition content hash mismatch".into(),
                ));
            }
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
        for (id, binding) in &self.outputs {
            if !self.definitions.contains_key(&binding.definition_id) {
                return Err(SummaryCatalogError::Descriptor(
                    "output references absent semantic definition".into(),
                ));
            }
            if !self
                .summary_descriptors
                .contains_key(&binding.summary_descriptor_id)
                || !self
                    .data_descriptors
                    .contains_key(&binding.data_descriptor_id)
            {
                return Err(SummaryCatalogError::MissingDescriptor(id.as_u64()));
            }
        }
        for output in self.outputs.values() {
            let expected = crate::summary_semantics::SummaryDefinition::from_descriptors(
                &self.summary_descriptors[&output.summary_descriptor_id],
                &self.data_descriptors[&output.data_descriptor_id],
            )?;
            if let (
                crate::summary_semantics::SummarySemantics::Configured {
                    computation: actual,
                    ..
                },
                crate::summary_semantics::SummarySemantics::Configured {
                    computation: expected,
                    ..
                },
            ) = (
                &self.definitions[&output.definition_id].semantics,
                &expected.semantics,
            ) {
                if actual != expected {
                    return Err(SummaryCatalogError::Descriptor(
                        "output descriptors disagree with semantic definition".into(),
                    ));
                }
            }
        }
        if !self
            .data_descriptors
            .values()
            .any(|data| matches!(data.source, DataSourceIdentity::Derived { .. }))
        {
            return Ok(());
        }
        // Dependencies must refer to this snapshot and form an acyclic graph.
        let mut pending = std::collections::BTreeMap::new();
        let mut consumers: std::collections::BTreeMap<_, Vec<_>> =
            std::collections::BTreeMap::new();
        for (id, binding) in &self.outputs {
            let dependencies = match &self.data_descriptors[&binding.data_descriptor_id].source {
                DataSourceIdentity::Derived { input } => input.inputs.clone(),
                _ => Default::default(),
            };
            for source in &dependencies {
                if !self.outputs.contains_key(source) {
                    return Err(SummaryCatalogError::Descriptor(
                        "derived input references missing summary".into(),
                    ));
                }
                consumers.entry(*source).or_default().push(*id);
            }
            pending.insert(*id, dependencies.len());
        }
        let mut ready: Vec<_> = pending
            .iter()
            .filter_map(|(id, count)| (*count == 0).then_some(*id))
            .collect();
        let mut visited = 0;
        while let Some(id) = ready.pop() {
            visited += 1;
            for consumer in consumers.get(&id).into_iter().flatten() {
                let count = pending
                    .get_mut(consumer)
                    .expect("catalog dependency target");
                *count -= 1;
                if *count == 0 {
                    ready.push(*consumer);
                }
            }
        }
        if visited != pending.len() {
            return Err(SummaryCatalogError::Descriptor(
                "derived summary dependencies have a cycle".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sds::ValueProjectionIdentity;
    use crate::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

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

    // Content changes invalidate references even when plan/version are reused.
    #[test]
    fn table_populations_have_distinct_materialization_and_data_identities() {
        use crate::table_population::{TableColumnPredicate, TablePopulation};
        use planner_types::pre_asap::{CompareOpKind, ScalarValue};
        let mut requests = config("raw_samples.value", "", 60);
        requests.table_name = Some("raw_samples".into());
        requests.value_projection = Some(ValueProjectionIdentity::Column {
            name: "value".into(),
        });
        requests.table_population = Some(TablePopulation {
            predicates: vec![TableColumnPredicate {
                column: "metric".into(),
                operator: CompareOpKind::Eq,
                value: ScalarValue::Utf8("requests".into()),
            }],
        });
        let mut errors = requests.clone();
        errors.table_population.as_mut().unwrap().predicates[0].value =
            ScalarValue::Utf8("errors".into());
        assert_ne!(requests.policy_fingerprint(), errors.policy_fingerprint());
        let mut other_table = requests.clone();
        other_table.table_name = Some("other_samples".into());
        assert_ne!(
            requests.policy_fingerprint(),
            other_table.policy_fingerprint()
        );
        let mut other_value = requests.clone();
        other_value.value_projection = Some(ValueProjectionIdentity::Column {
            name: "other_value".into(),
        });
        assert_ne!(
            requests.policy_fingerprint(),
            other_value.policy_fingerprint()
        );
        let mut other_time = requests.clone();
        other_time.table_timestamp_column = Some("event_time_ms".into());
        assert_ne!(
            requests.policy_fingerprint(),
            other_time.policy_fingerprint()
        );
        let mut invalid_labels = requests.clone();
        invalid_labels.table_population = None;
        invalid_labels.spatial_filter = "job=\"requests\"".into();
        assert!(SummaryCatalog::from_materializations(1, 1, &[invalid_labels]).is_err());
        let mut invalid_time = requests.clone();
        invalid_time.table_timestamp_column = Some("time; DROP TABLE samples".into());
        assert!(SummaryCatalog::from_materializations(1, 1, &[invalid_time]).is_err());
        let catalog =
            SummaryCatalog::from_materializations(1, 1, &[requests, errors, other_time]).unwrap();
        assert_eq!(catalog.data_descriptors.len(), 3);
        assert_eq!(catalog.outputs.len(), 3);
    }

    #[test]
    fn snapshot_reference_is_deterministic_and_content_sensitive() {
        let a = config("requests", "", 60);
        let b = config("errors", "", 60);
        let left = SummaryCatalog::from_materializations(1, 2, &[a.clone(), b.clone()]).unwrap();
        let reordered = SummaryCatalog::from_materializations(1, 2, &[b, a.clone()]).unwrap();
        assert_eq!(left.reference().unwrap(), reordered.reference().unwrap());
        let changed = SummaryCatalog::from_materializations(1, 2, &[a]).unwrap();
        assert_ne!(left.reference().unwrap(), changed.reference().unwrap());
        left.reference()
            .unwrap()
            .validate_snapshot(&left, 1, 2)
            .unwrap();
        assert!(left
            .reference()
            .unwrap()
            .validate_snapshot(&left, 1, 3)
            .is_err());
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
        assert_eq!(catalog.outputs.len(), 2);
        assert_eq!((catalog.plan_id, catalog.plan_version), (7, 2));
    }

    // Stored pane duration identifies a deployment output, not the summary meaning.
    #[test]
    fn semantic_definition_is_shared_across_deployed_pane_outputs() {
        let catalog = SummaryCatalog::from_materializations(
            1,
            1,
            &[config("requests", "", 60), config("requests", "", 120)],
        )
        .unwrap();
        assert_eq!(catalog.definitions.len(), 1);
        assert_eq!(catalog.outputs.len(), 2);
    }

    // A hot and rebuild output share meaning but remain independently bound.
    #[test]
    fn hot_and_rebuild_have_one_definition_and_two_bound_outputs() {
        let mut hot = config("latency", "", 60);
        hot.stored_output_id = Some(StoredOutputId(41));
        let mut rebuild = hot.clone();
        rebuild.stored_output_id = Some(StoredOutputId(42));
        let catalog = SummaryCatalog::from_materializations(7, 42, &[hot, rebuild]).unwrap();
        assert_eq!(catalog.definitions.len(), 1);
        let hot = catalog.output_reference(StoredOutputId(41)).unwrap();
        let rebuild = catalog.output_reference(StoredOutputId(42)).unwrap();
        assert_eq!(hot.definition_id, rebuild.definition_id);
        assert_ne!(hot, rebuild);
        let mut forged = catalog.clone();
        let crate::summary_semantics::SummarySemantics::Configured { computation, .. } =
            &mut forged.definitions.values_mut().next().unwrap().semantics
        else {
            panic!("fixture")
        };
        computation.predicate = "service=other".into();
        assert!(forged.validate().is_err());
        let mut unknown = catalog.clone();
        unknown
            .definitions
            .values_mut()
            .next()
            .unwrap()
            .semantic_format_version += 1;
        assert!(unknown.validate().is_err());
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
        assert_eq!(catalog.outputs.len(), 2);
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

    #[test]
    fn catalog_definitions_do_not_store_writer_layout() {
        let mut materialization = config("requests", "", 60);
        materialization.pane_origin_ms = Some(7_000);
        let id = StoredOutputId::from(materialization.policy_fingerprint());
        let catalog = SummaryCatalog::from_materializations(7, 2, &[materialization]).unwrap();
        assert!(catalog.outputs.contains_key(&id));
        assert!(
            !serde_json::to_value(&catalog).unwrap()["outputs"][id.as_u64().to_string()]
                .as_object()
                .unwrap()
                .contains_key("pane_origin_ms")
        );

        let mut invalid = serde_json::to_value(&catalog).unwrap();
        invalid["outputs"][id.as_u64().to_string()]["pane_origin_ms"] = serde_json::json!(7_000);
        assert!(serde_json::from_value::<SummaryCatalog>(invalid).is_err());
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
            SummaryCatalogError::ConflictingDefinition(_)
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
        assert!(catalog.outputs.is_empty());
        catalog.validate().unwrap();
    }
}
