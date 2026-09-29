//! Internal execution indexes derived exclusively from a validated DAG plan.
use anyhow::Result;
use std::collections::HashMap;
use std::ops::Index;

use super::storage_backend::StorageBackend;
use asap_types::sds::StoredOutputId;
use asap_types::{PolicyRegistry, PrecomputeMaterialization};

#[derive(Debug, Clone)]
pub struct InstalledPrecomputePlan {
    pub(crate) partitioning: crate::precompute_engine::partitioning::DagPartitioning,
    pub(crate) raw_programs:
        HashMap<u64, std::sync::Arc<crate::precompute_engine::raw_dag::RawDagProgram>>,
    pub(crate) precompute_plan: Option<asap_types::precompute_plan::PrecomputePlan>,
    pub(crate) materializations_by_output: HashMap<StoredOutputId, PrecomputeMaterialization>,
    pub(crate) storage_backend: StorageBackend,
}

impl InstalledPrecomputePlan {
    fn derived_view(materializations: HashMap<StoredOutputId, PrecomputeMaterialization>) -> Self {
        Self {
            partitioning: Default::default(),
            raw_programs: HashMap::new(),
            precompute_plan: None,
            materializations_by_output: materializations,
            storage_backend: StorageBackend::default(),
        }
    }

    /// Production construction always validates the executable DAG and bindings.
    pub fn from_precompute_plan(plan: asap_types::precompute_plan::PrecomputePlan) -> Result<Self> {
        let materializations = plan.runtime_materializations()?;
        let mut programs = HashMap::new();
        for config in materializations.values().filter(|config| {
            config.derived_input.is_none()
                && plan.ingest.protocol
                    == asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
        }) {
            let program =
                crate::precompute_engine::raw_dag::RawDagProgram::from_plan(&plan, config)
                    .map_err(anyhow::Error::msg)?;
            programs.insert(config.policy_fp_u64(), std::sync::Arc::new(program));
        }
        let mut view = Self::derived_view(materializations);
        view.partitioning =
            crate::precompute_engine::partitioning::DagPartitioning::from_plan(&plan);
        view.precompute_plan = Some(plan);
        view.raw_programs = programs;
        Ok(view)
    }

    // Isolated kernel/storage fixtures can omit a physical installation. This
    // constructor is absent from the production library and binary.
    #[cfg(test)]
    pub fn new(materializations: HashMap<StoredOutputId, PrecomputeMaterialization>) -> Self {
        Self::derived_view(materializations)
    }

    /// Fixtures hold raw output ids, so say that once here rather than
    /// wrapping every literal. Production construction goes through
    /// `from_precompute_plan`, which takes the ids from the plan itself.
    #[cfg(test)]
    pub fn from_raw_ids(materializations: HashMap<u64, PrecomputeMaterialization>) -> Self {
        Self::new(
            materializations
                .into_iter()
                .map(|(id, value)| (StoredOutputId(id), value))
                .collect(),
        )
    }

    #[cfg(test)]
    pub fn with_storage_backend(
        materializations: HashMap<StoredOutputId, PrecomputeMaterialization>,
        storage_backend: StorageBackend,
    ) -> Self {
        let mut view = Self::derived_view(materializations);
        view.storage_backend = storage_backend;
        view
    }

    pub fn stored_output_reference(
        &self,
        definition: asap_types::sds::StoredOutputId,
    ) -> Option<asap_types::sds::StoredOutputReference> {
        let selected = self.precompute_plan.as_ref().and_then(|plan| {
            plan.schemas
                .iter()
                .find(|schema| schema.materialization == definition)
                .map(|schema| schema.stored_output_reference.clone())
        });
        #[cfg(test)]
        let selected = selected.or_else(|| {
            let materializations = self
                .materializations_by_output
                .values()
                .cloned()
                .collect::<Vec<_>>();
            asap_types::summary_catalog::SummaryCatalog::from_materializations(
                0,
                0,
                &materializations,
            )
            .ok()?
            .output_reference(definition)
            .ok()
        });
        selected
    }

    pub fn plan(&self) -> &asap_types::precompute_plan::PrecomputePlan {
        self.precompute_plan
            .as_ref()
            .expect("installed plan has an authoritative source")
    }

    pub fn storage_backend(&self) -> StorageBackend {
        self.storage_backend
    }

    pub fn get_aggregation_config(
        &self,
        output: StoredOutputId,
    ) -> Option<&PrecomputeMaterialization> {
        self.materializations_by_output.get(&output)
    }

    pub fn materializations(&self) -> &HashMap<StoredOutputId, PrecomputeMaterialization> {
        &self.materializations_by_output
    }

    pub fn contains(&self, output: StoredOutputId) -> bool {
        self.materializations_by_output.contains_key(&output)
    }

    pub fn policy_registry(&self) -> PolicyRegistry {
        PolicyRegistry::from_configs(self.materializations_by_output.values().cloned())
    }
}

impl Index<StoredOutputId> for InstalledPrecomputePlan {
    type Output = PrecomputeMaterialization;
    fn index(&self, output: StoredOutputId) -> &Self::Output {
        &self.materializations_by_output[&output]
    }
}

impl Default for InstalledPrecomputePlan {
    fn default() -> Self {
        use control_plane::physical::compiler::{
            PlanEnvelope, PrecomputePlan, BACKEND_COMPAT, PLANNER_REVISION,
        };
        let envelope = PlanEnvelope {
            plan_id: 0,
            plan_version: 0,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "empty-installation".into(),
        };
        Self::from_precompute_plan(
            PrecomputePlan::build(envelope, vec![], &[]).expect("valid empty plan"),
        )
        .expect("valid empty installation")
    }
}

#[cfg(test)]
mod tests {
    // Flat lists cannot enter through the authoritative physical-plan document.
    #[test]
    fn rejects_flat_aggregation_documents() {
        for text in [r#"{"aggregation_configs":{}}"#, "aggregations: []"] {
            assert!(
                serde_yaml::from_str::<asap_types::plan_publication::PhysicalPlanInstallRequest>(
                    text
                )
                .is_err()
            );
        }
    }
}
