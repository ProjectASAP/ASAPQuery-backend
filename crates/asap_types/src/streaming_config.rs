use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Index;

use crate::aggregation_config::{AggregationConfig, AggregationIdInfo};
use crate::capability_matching::find_compatible_aggregation as common_find_compatible;
use crate::capability_matching::StorageBackend;
use crate::enums::QueryLanguage;
use crate::query_requirements::QueryRequirements;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    pub aggregation_configs: HashMap<u64, AggregationConfig>,
    /// Phase-5 capability-routing axis: which storage tier serves this
    /// per-metric runtime config. The controller pushes this when planning
    /// (see `docs/design-gorilla-s3-cold-engine.md` §8); pre-Phase-5
    /// configs decode with `#[serde(default)]` to `SketchStore` so
    /// existing deploys keep dispatching to `ASAPQueryEngine`.
    #[serde(default)]
    pub storage_backend: StorageBackend,
}

impl StreamingConfig {
    pub fn new(aggregation_configs: HashMap<u64, AggregationConfig>) -> Self {
        Self {
            aggregation_configs,
            storage_backend: StorageBackend::default(),
        }
    }

    /// Phase-5 constructor: build with an explicit storage-backend pin.
    /// Used by the controller-driven plan-push path; tests typically
    /// stay on `Self::new(...)` and let the default land.
    pub fn with_storage_backend(
        aggregation_configs: HashMap<u64, AggregationConfig>,
        storage_backend: StorageBackend,
    ) -> Self {
        Self {
            aggregation_configs,
            storage_backend,
        }
    }

    /// Read-only access to the storage backend pinned at construction
    /// time. The Phase-5 router consults this to pick which engine
    /// answers a query.
    pub fn storage_backend(&self) -> StorageBackend {
        self.storage_backend
    }

    pub fn get_aggregation_config(&self, aggregation_id: u64) -> Option<&AggregationConfig> {
        self.aggregation_configs.get(&aggregation_id)
    }

    pub fn get_all_aggregation_configs(&self) -> &HashMap<u64, AggregationConfig> {
        &self.aggregation_configs
    }

    pub fn contains(&self, aggregation_id: u64) -> bool {
        self.aggregation_configs.contains_key(&aggregation_id)
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
        let mut aggregation_configs: HashMap<u64, AggregationConfig> = HashMap::new();

        if let Some(aggregations) = data.get("aggregations").and_then(|v| v.as_sequence()) {
            for aggregation_data in aggregations {
                if let Some(aggregation_id) = aggregation_data.get("aggregationId") {
                    let aggregation_id_u64 = aggregation_id.as_u64().ok_or_else(|| {
                        anyhow::anyhow!(
                            "aggregationId must be a valid u64, got: {:?}",
                            aggregation_id
                        )
                    })?;
                    // Read per-agg retention directly from the YAML
                    // entry (previously was looked up via
                    // inference_config's query→agg map).
                    let num_aggregates_to_retain = aggregation_data
                        .get("numAggregatesToRetain")
                        .and_then(|v| v.as_u64());
                    let config = AggregationConfig::from_yaml_data(
                        aggregation_data,
                        num_aggregates_to_retain,
                        QueryLanguage::promql,
                    )?;
                    aggregation_configs.insert(aggregation_id_u64, config);
                }
            }
        }

        Ok(Self::new(aggregation_configs))
    }
}

impl StreamingConfig {
    /// Find a compatible aggregation for the given requirements using capability-based matching.
    /// Delegates to `asap_types::find_compatible_aggregation`.
    pub fn find_compatible_aggregation(
        &self,
        requirements: &QueryRequirements,
    ) -> Option<AggregationIdInfo> {
        common_find_compatible(&self.aggregation_configs, requirements)
    }
}

impl Index<u64> for StreamingConfig {
    type Output = AggregationConfig;

    fn index(&self, aggregation_id: u64) -> &Self::Output {
        &self.aggregation_configs[&aggregation_id]
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pre-Phase-5 deploys serialize `StreamingConfig` without the
    /// `storage_backend` field; deserialize must default to `SketchStore`
    /// so the router keeps dispatching to `ASAPQueryEngine` unchanged.
    #[test]
    fn deserialize_legacy_yaml_defaults_to_warm_tier() {
        let yaml = "{\"aggregation_configs\":{}}";
        let cfg: StreamingConfig = serde_json::from_str(yaml).expect("legacy decode");
        assert_eq!(cfg.storage_backend(), StorageBackend::SketchStore);
    }

    #[test]
    fn deserialize_with_explicit_archive_pin() {
        let yaml = "{\"aggregation_configs\":{},\"storage_backend\":\"gorilla_object_store\"}";
        let cfg: StreamingConfig = serde_json::from_str(yaml).expect("Phase-5 decode");
        assert_eq!(cfg.storage_backend(), StorageBackend::GorillaObjectStore);
    }
}
