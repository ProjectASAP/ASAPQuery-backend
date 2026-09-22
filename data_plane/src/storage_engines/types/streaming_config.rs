use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Index;

use asap_types::{MonitorSpec, PolicyRegistry, PrecomputeMaterialization};

use super::storage_backend::StorageBackend;

/// DAG installation plus a derived in-memory routing index. The flat index is
/// never serialized as executable configuration. Raw programs are validated
/// and shared once per installed producer across all of its population states.
#[derive(Debug, Clone, Serialize)]
pub struct StreamingConfig {
    #[serde(skip)]
    pub(crate) partitioning: crate::precompute_engine::partitioning::DagPartitioning,
    #[serde(skip)]
    pub(crate) raw_programs:
        HashMap<u64, std::sync::Arc<crate::precompute_engine::raw_dag::RawDagProgram>>,
    /// Authoritative execution configuration: Planner DAGs and physical bindings.
    pub precompute_plan: Option<asap_types::precompute_plan::PrecomputePlan>,
    #[serde(skip)]
    pub materializations_by_policy_fingerprint: HashMap<u64, PrecomputeMaterialization>,
    /// Phase-5 capability-routing axis: which storage tier serves this
    /// per-metric runtime config. The controller pushes this when planning
    /// (see `docs/design-gorilla-s3-cold-engine.md` §8); pre-Phase-5
    /// configs decode with `#[serde(default)]` to `SketchStore` so
    /// existing deploys keep dispatching to `ASAPQueryEngine`.
    #[serde(default)]
    pub storage_backend: StorageBackend,
    /// Continuous-distributed-monitoring threshold specs the data-plane
    /// coordinator should serve. Defaults to empty so existing configs (and the
    /// vast majority of deploys, which run no monitors) decode unchanged.
    #[serde(default)]
    pub monitors: Vec<MonitorSpec>,
}

// Flat aggregation lists are deliberately not an accepted execution document.
impl<'de> Deserialize<'de> for StreamingConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Document {
            precompute_plan: asap_types::precompute_plan::PrecomputePlan,
            #[serde(default)]
            storage_backend: StorageBackend,
            #[serde(default)]
            monitors: Vec<MonitorSpec>,
        }
        let doc = Document::deserialize(deserializer)?;
        let mut config =
            Self::from_precompute_plan(doc.precompute_plan).map_err(serde::de::Error::custom)?;
        config.storage_backend = doc.storage_backend;
        config.monitors = doc.monitors;
        Ok(config)
    }
}

impl StreamingConfig {
    pub fn new(
        materializations_by_policy_fingerprint: HashMap<u64, PrecomputeMaterialization>,
    ) -> Self {
        Self {
            partitioning: Default::default(),
            raw_programs: HashMap::new(),
            precompute_plan: None,
            materializations_by_policy_fingerprint,
            storage_backend: StorageBackend::default(),
            monitors: Vec::new(),
        }
    }

    /// Build the routing projection only after validating the DAG installation.
    pub fn from_precompute_plan(plan: asap_types::precompute_plan::PrecomputePlan) -> Result<Self> {
        let materializations = plan.runtime_materializations()?;
        let mut programs = HashMap::new();
        for config in materializations.values().filter(|c| {
            c.derived_input.is_none()
                && plan.ingest.protocol
                    == asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
        }) {
            let program =
                crate::precompute_engine::raw_dag::RawDagProgram::from_plan(&plan, config)
                    .map_err(anyhow::Error::msg)?;
            programs.insert(config.policy_fp_u64(), std::sync::Arc::new(program));
        }
        let mut view = Self::new(materializations);
        view.partitioning =
            crate::precompute_engine::partitioning::DagPartitioning::from_plan(&plan);
        view.precompute_plan = Some(plan);
        view.raw_programs = programs;
        Ok(view)
    }

    /// CDM monitor specs the data-plane coordinator should serve (may be empty).
    pub fn monitors(&self) -> &[MonitorSpec] {
        &self.monitors
    }

    /// Phase-5 constructor: build with an explicit storage-backend pin.
    /// Used by the controller-driven plan-push path; tests typically
    /// stay on `Self::new(...)` and let the default land.
    pub fn with_storage_backend(
        materializations_by_policy_fingerprint: HashMap<u64, PrecomputeMaterialization>,
        storage_backend: StorageBackend,
    ) -> Self {
        Self {
            partitioning: Default::default(),
            raw_programs: HashMap::new(),
            precompute_plan: None,
            materializations_by_policy_fingerprint,
            storage_backend,
            monitors: Vec::new(),
        }
    }

    /// Read-only access to the storage backend pinned at construction
    /// time. The Phase-5 router consults this to pick which engine
    /// answers a query.
    pub fn storage_backend(&self) -> StorageBackend {
        self.storage_backend
    }

    pub fn get_aggregation_config(
        &self,
        aggregation_id: u64,
    ) -> Option<&PrecomputeMaterialization> {
        self.materializations_by_policy_fingerprint
            .get(&aggregation_id)
    }

    pub fn materializations(&self) -> &HashMap<u64, PrecomputeMaterialization> {
        &self.materializations_by_policy_fingerprint
    }

    pub fn contains(&self, aggregation_id: u64) -> bool {
        self.materializations_by_policy_fingerprint
            .contains_key(&aggregation_id)
    }

    /// Derived content-addressed view. Builds a [`PolicyRegistry`] keyed
    /// on [`asap_types::PolicyFingerprint`] — the merged-sid-identity-chain
    /// replacement for the `aggregation_id`-keyed lookup. Cheap (O(N)
    /// over `materializations_by_policy_fingerprint.len()`); call at swap time, not per
    /// query, if it shows up in hot-path profiles.
    ///
    /// Dual-keyed transition: this method exists alongside the legacy
    /// `get_aggregation_config(aggregation_id)` so callers can migrate
    /// one at a time. The two views are derived from the same source —
    /// they can never disagree.
    pub fn policy_registry(&self) -> PolicyRegistry {
        PolicyRegistry::from_configs(
            self.materializations_by_policy_fingerprint
                .values()
                .cloned(),
        )
    }

    pub fn from_yaml_file(yaml_file: &str) -> Result<Self> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);
        let data: Value = serde_yaml::from_reader(reader)?;

        Self::from_yaml_data(&data)
    }

    /// Build from the streaming-config YAML alone. Per-aggregation
    /// `numAggregatesToRetain` is read directly from each
    /// aggregation's YAML entry; the old InferenceConfig indirection
    /// (operator-authored query→agg_ids YAML feeding a retention_map)
    /// is gone — the controller drives capability matching dynamically.
    pub fn from_yaml_data(data: &Value) -> Result<Self> {
        serde_yaml::from_value(data.clone()).map_err(Into::into)
    }
}

impl Index<u64> for StreamingConfig {
    type Output = PrecomputeMaterialization;

    fn index(&self, aggregation_id: u64) -> &Self::Output {
        &self.materializations_by_policy_fingerprint[&aggregation_id]
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

impl StreamingConfig {
    #[deprecated(note = "Use materializations")]
    pub fn get_all_aggregation_configs(&self) -> &HashMap<u64, PrecomputeMaterialization> {
        self.materializations()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Old flat lists cannot become execution authority through JSON or YAML.
    #[test]
    fn rejects_flat_aggregation_documents() {
        for text in [
            r#"{"aggregation_configs":{}}"#,
            "aggregations: []",
            "aggregations: [{aggregationType: Sum, metric: m}]",
        ] {
            let yaml = serde_yaml::from_str(text).unwrap();
            assert!(StreamingConfig::from_yaml_data(&yaml).is_err());
        }
    }
}
