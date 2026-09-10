//! Catalog consistency checks for the precompute execution plan.
use super::compiler::*;
use super::summary_catalog::SummaryCatalog;
use asap_types::sds::{SummaryDefinitionId, SummaryDescriptor};
use planner_types::pre_asap::{ColumnRef, Source};
use std::collections::BTreeSet;
fn invalid(reason: impl Into<String>) -> PrecomputePlanError {
    PrecomputePlanError::CatalogContract(reason.into())
}

impl PrecomputePlan {
    /// Bind references after retention decisions are finalized. The plan keeps
    /// only the immutable snapshot reference; descriptors are installed once.
    pub fn bind_catalog(&mut self, catalog: &SummaryCatalog) -> Result<(), PrecomputePlanError> {
        for config in &self.materializations {
            let id = SummaryDefinitionId::from(config.policy_fingerprint());
            catalog.materializations.get(&id).ok_or_else(|| {
                invalid(format!("missing catalog materialization {}", id.as_u64()))
            })?;
        }
        self.summary_catalog = Some(
            catalog
                .reference()
                .map_err(|error| invalid(error.to_string()))?,
        );
        self.validate_against_catalog(catalog)
    }
    /// Strict validation resolves this plan's reference against the one catalog
    /// snapshot carried by the enclosing physical-plan installation.
    pub fn validate_against_catalog(
        &self,
        catalog: &SummaryCatalog,
    ) -> Result<(), PrecomputePlanError> {
        self.validate()?;
        self.validate_catalog_contents(catalog)
    }
    fn validate_catalog_contents(
        &self,
        catalog: &SummaryCatalog,
    ) -> Result<(), PrecomputePlanError> {
        catalog.validate().map_err(|e| invalid(e.to_string()))?;
        let expected_reference = catalog
            .reference()
            .map_err(|error| invalid(error.to_string()))?;
        if self.summary_catalog.as_ref() != Some(&expected_reference)
            || catalog.plan_id != self.envelope.plan_id
            || catalog.plan_version != self.envelope.plan_version
        {
            return Err(PrecomputePlanError::PlanIdentityMismatch);
        }
        let ids: BTreeSet<_> = self
            .materializations
            .iter()
            .map(|m| SummaryDefinitionId::from(m.policy_fingerprint()))
            .collect();
        if ids != catalog.materializations.keys().copied().collect() {
            return Err(invalid("catalog/reference/materialization sets differ"));
        }
        for config in &self.materializations {
            let id = SummaryDefinitionId::from(config.policy_fingerprint());
            let binding = &catalog.materializations[&id];
            let expected =
                SummaryDescriptor::from_config(config).map_err(|e| invalid(e.to_string()))?;
            if binding.summary_descriptor_id != expected.id {
                return Err(invalid(
                    "summary operator/update contract differs from catalog",
                ));
            }
            let data = &catalog.data_descriptors[&binding.data_descriptor_id];
            if data.metric_name != config.metric
                || data.population_filter_canonical
                    != asap_types::utils::normalize_spatial_filter(&config.spatial_filter)
                || data.group_by_keys != config.grouping_labels.labels.iter().cloned().collect()
            {
                return Err(invalid("source/population/grouping differs from catalog"));
            }
            if config.spatial_filter_normalized != data.population_filter_canonical {
                return Err(invalid("normalized population predicate drift"));
            }
            let schema = self
                .schemas
                .iter()
                .find(|s| s.materialization == id.fingerprint())
                .ok_or(PrecomputePlanError::SchemaSetMismatch)?;
            let family = config
                .accumulator_spec()
                .map_err(|e| invalid(e.to_string()))?
                .family;
            let expected_family = StateFamilyContract::try_from(&family)
                .map_err(|_| PrecomputePlanError::UnsupportedFamily(id.as_u64()))?;
            let source = if let Some(table) = &config.table_name {
                Source::Table {
                    table_ref: table.clone(),
                }
            } else {
                Source::TimeSeries {
                    metric: config.metric.clone(),
                }
            };
            let col = if let Some(column) = &config.value_column {
                ColumnRef::Named(column.clone())
            } else {
                ColumnRef::SampleValue
            };
            let size = config
                .window_size
                .checked_mul(1000)
                .filter(|v| *v > 0)
                .ok_or_else(|| invalid("invalid window size"))?;
            let slide = config
                .slide_interval
                .checked_mul(1000)
                .filter(|v| *v > 0)
                .ok_or_else(|| invalid("invalid slide interval"))?;
            let expected_slide = match config.window_type {
                asap_types::WindowKind::Tumbling => None,
                asap_types::WindowKind::Sliding => Some(slide),
                asap_types::WindowKind::Session => {
                    return Err(invalid("session lifecycle is not supported"))
                }
            };
            if schema.schema_id != state_schema_id(id.fingerprint())
                || schema.family != expected_family
                || schema.source != source
                || schema.value_column != col
                || schema.group_by != config.grouping_labels.labels
                || schema.window.kind != config.window_type
                || schema.window.size_ms != size
                || schema.window.slide_ms != expected_slide
            {
                return Err(PrecomputePlanError::InvalidSchema {
                    schema_id: schema.schema_id.clone(),
                });
            }
            if schema.encodings != state_encodings(&family) {
                return Err(invalid("encoding does not match state family"));
            }
            if config.num_aggregates_to_retain == Some(0) {
                return Err(invalid("materialization lifecycle/retention mismatch"));
            }
        }
        if self.ingest.protocol == IngestProtocol::PrometheusRemoteWriteV1
            && !self.producers.is_empty()
        {
            return Err(invalid(
                "backend-local plan cannot declare collector placement",
            ));
        }
        if self.producers.iter().any(|p| {
            p.producer_id.trim().is_empty()
                || p.collector_id.trim().is_empty()
                || p.producer_id != p.collector_id
        }) {
            return Err(invalid("invalid collector placement identity"));
        }
        Ok(())
    }
}
