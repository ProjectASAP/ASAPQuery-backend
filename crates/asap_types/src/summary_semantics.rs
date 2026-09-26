//! Versioned, content-addressed meaning of a stored result. Runtime routing,
//! pane placement, codecs, retention and plan generations are deliberately absent.
use crate::sds::{
    DataDescriptor, DataSourceIdentity, SummaryDescriptor, SummaryOperator, ValueProjectionIdentity,
};
use crate::summary_catalog::SummaryCatalogError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryDefinition {
    pub semantic_format_version: u32,
    pub semantics: SummarySemantics,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SummarySemantics {
    Planner {
        fragment: crate::semantic_fragment::SemanticFragment,
    },
    /// Restricted raw-input adapter for explicit native summary configurations.
    /// General expressions must use the Planner fragment variant.
    Configured {
        computation: Box<SummaryComputation>,
        value_source_column: Option<planner_types::pre_asap::Column>,
        aggregated_labels: crate::KeyByLabelNames,
        rollup_labels: crate::KeyByLabelNames,
    },
}

/// Typed semantic contract of the supported summary input. This is not a
/// deployment plan: it contains neither physical nodes nor storage references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryComputation {
    pub operator: SummaryOperator,
    pub source: DataSourceIdentity,
    pub value: ValueProjectionIdentity,
    pub predicate: String,
    pub grouping: crate::GroupingProjection,
    pub timestamp_column: Option<String>,
    pub observation_semantics: String,
}

impl SummaryDefinition {
    pub fn from_descriptors(
        summary: &SummaryDescriptor,
        data: &DataDescriptor,
    ) -> Result<Self, SummaryCatalogError> {
        summary
            .validate()
            .map_err(|e| SummaryCatalogError::Descriptor(e.to_string()))?;
        data.validate()
            .map_err(|e| SummaryCatalogError::Descriptor(e.to_string()))?;
        Ok(Self {
            semantic_format_version: 1,
            semantics: SummarySemantics::Configured {
                value_source_column: None,
                aggregated_labels: crate::KeyByLabelNames::empty(),
                rollup_labels: crate::KeyByLabelNames::empty(),
                computation: Box::new(SummaryComputation {
                    operator: summary.operator.clone(),
                    source: data.source.clone(),
                    value: data.value_projection.clone(),
                    predicate: data.population_filter_canonical.clone(),
                    grouping: data.group_by_keys.clone(),
                    timestamp_column: data.timestamp_column.clone(),
                    observation_semantics: data.observation_semantics.clone(),
                }),
            },
        })
    }

    pub fn with_config(mut self, config: &crate::PrecomputeMaterialization) -> Self {
        let fragment = config.semantic_fragment.as_ref();
        if let Some(fragment) = fragment {
            self.semantics = SummarySemantics::Planner {
                fragment: fragment.clone(),
            };
        } else if let SummarySemantics::Configured {
            value_source_column,
            aggregated_labels,
            rollup_labels,
            ..
        } = &mut self.semantics
        {
            *value_source_column = config.value_source_column.clone();
            *aggregated_labels = config.aggregated_labels.clone();
            *rollup_labels = config.rollup_labels.clone();
        }
        self
    }

    pub fn id(&self) -> Result<crate::sds::SummaryDefinitionId, SummaryCatalogError> {
        if self.semantic_format_version != 1 {
            return Err(SummaryCatalogError::Descriptor(
                "unsupported semantic format version".into(),
            ));
        }
        if let SummarySemantics::Planner { fragment } = &self.semantics {
            fragment
                .validate()
                .map_err(SummaryCatalogError::Descriptor)?;
        }
        // Object keys are recursively sorted, independent of serde_json features.
        fn canonical(value: serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::Object(values) => serde_json::Value::Object(
                    values
                        .into_iter()
                        .map(|(k, v)| (k, canonical(v)))
                        .collect::<std::collections::BTreeMap<_, _>>()
                        .into_iter()
                        .collect(),
                ),
                serde_json::Value::Array(values) => {
                    serde_json::Value::Array(values.into_iter().map(canonical).collect())
                }
                value => value,
            }
        }
        let value = serde_json::to_value(self)
            .map_err(|e| SummaryCatalogError::Descriptor(e.to_string()))?;
        let bytes = serde_json::to_vec(&canonical(value))
            .map_err(|e| SummaryCatalogError::Descriptor(e.to_string()))?;
        Ok(crate::sds::SummaryDefinitionId::from_semantics(&bytes))
    }
}
