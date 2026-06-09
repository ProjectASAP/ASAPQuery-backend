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
use crate::policy_registry::PolicyRegistry;
use crate::query_requirements::QueryRequirements;

/// One continuous-monitoring (CDM) threshold spec. The data-plane monitor
/// coordinator owns the AUTHORITATIVE `tau`/`epsilon`/`window_ms` (the edge
/// copy is advisory), keyed by the same content-addressed `agg_id` the edge and
/// coordinator share. `key` is the CMS point-frequency key for point monitors
/// (empty for Sum / whole-stream). See
/// `ASAPCollector/docs/continuous-monitoring-tumbling-cost-analysis.md`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorSpec {
    pub agg_id: u64,
    /// CMS point-frequency key x; empty (default) for Sum / whole-stream.
    #[serde(default)]
    pub key: String,
    /// Threshold τ (authoritative here, not at the edge).
    pub tau: f64,
    /// Relative tolerance ε; the alert fires when the estimate reaches (1−ε)τ.
    #[serde(default = "default_monitor_epsilon")]
    pub epsilon: f64,
    /// Tumbling epoch length in ms; MUST match the edge window for this agg_id.
    pub window_ms: u64,
}

fn default_monitor_epsilon() -> f64 {
    0.05
}

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
    /// Continuous-distributed-monitoring threshold specs the data-plane
    /// coordinator should serve. Defaults to empty so existing configs (and the
    /// vast majority of deploys, which run no monitors) decode unchanged.
    #[serde(default)]
    pub monitors: Vec<MonitorSpec>,
}

impl StreamingConfig {
    pub fn new(aggregation_configs: HashMap<u64, AggregationConfig>) -> Self {
        Self {
            aggregation_configs,
            storage_backend: StorageBackend::default(),
            monitors: Vec::new(),
        }
    }

    /// CDM monitor specs the data-plane coordinator should serve (may be empty).
    pub fn monitors(&self) -> &[MonitorSpec] {
        &self.monitors
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
            monitors: Vec::new(),
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

    /// Derived content-addressed view. Builds a [`PolicyRegistry`] keyed
    /// on [`crate::PolicyFingerprint`] — the merged-sid-identity-chain
    /// replacement for the `aggregation_id`-keyed lookup. Cheap (O(N)
    /// over `aggregation_configs.len()`); call at swap time, not per
    /// query, if it shows up in hot-path profiles.
    ///
    /// Dual-keyed transition: this method exists alongside the legacy
    /// `get_aggregation_config(aggregation_id)` so callers can migrate
    /// one at a time. The two views are derived from the same source —
    /// they can never disagree.
    pub fn policy_registry(&self) -> PolicyRegistry {
        PolicyRegistry::from_streaming_config(self)
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
                // Read per-agg retention directly from the YAML entry
                // (previously was looked up via inference_config's
                // query→agg map). `aggregationId` is no longer
                // required — `AggregationConfig::from_yaml_data`
                // derives it from content when absent (M2 follow-up).
                let num_aggregates_to_retain = aggregation_data
                    .get("numAggregatesToRetain")
                    .and_then(|v| v.as_u64());
                let config = AggregationConfig::from_yaml_data(
                    aggregation_data,
                    num_aggregates_to_retain,
                    QueryLanguage::promql,
                )?;
                // PR 5: the map key IS the policy-fingerprint u64.
                // `AggregationConfig::policy_fp_u64()` is the canonical
                // accessor for this value.
                aggregation_configs.insert(config.policy_fp_u64(), config);
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
    fn deserialize_legacy_yaml_defaults_to_asap_tier() {
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

    /// PR 5: a streaming-config YAML that omits `aggregationId`
    /// parses correctly — the backend derives identity from content
    /// via `PolicyFingerprint::from_config`. The map key is the
    /// fingerprint's u64 form.
    #[test]
    fn from_yaml_data_accepts_entry_without_aggregation_id() {
        let yaml = "\
aggregations:\n\
- aggregationType: DDSketch\n  aggregationSubType: ''\n  metric: cpu_seconds\n  labels:\n    grouping: [host]\n    rollup: []\n    aggregated: []\n  parameters:\n    relative_accuracy: 0.01\n  windowSize: 30\n  windowType: tumbling\n  spatialFilter: ''\n";
        let data: Value = serde_yaml::from_str(yaml).expect("yaml ok");
        let cfg = StreamingConfig::from_yaml_data(&data).expect("decode without id");
        assert_eq!(cfg.aggregation_configs.len(), 1);
        let (k, v) = cfg.aggregation_configs.iter().next().unwrap();
        assert_ne!(*k, 0, "derived id is not the 0 sentinel");
        assert_eq!(*k, v.policy_fp_u64(), "map key equals fingerprint u64");
        assert_eq!(v.metric, "cpu_seconds");
    }

    /// PR 5: a streaming-config YAML that still spells out
    /// `aggregationId: N` parses the SAME as one without — the field
    /// is silently dropped.
    #[test]
    fn from_yaml_data_ignores_explicit_aggregation_id() {
        let with = "\
aggregations:\n\
- aggregationId: 42\n  aggregationType: DDSketch\n  aggregationSubType: ''\n  metric: cpu_seconds\n  labels:\n    grouping: [host]\n    rollup: []\n    aggregated: []\n  parameters:\n    relative_accuracy: 0.01\n  windowSize: 30\n  windowType: tumbling\n  spatialFilter: ''\n";
        let without = "\
aggregations:\n\
- aggregationType: DDSketch\n  aggregationSubType: ''\n  metric: cpu_seconds\n  labels:\n    grouping: [host]\n    rollup: []\n    aggregated: []\n  parameters:\n    relative_accuracy: 0.01\n  windowSize: 30\n  windowType: tumbling\n  spatialFilter: ''\n";
        let w: Value = serde_yaml::from_str(with).expect("with yaml ok");
        let wo: Value = serde_yaml::from_str(without).expect("without yaml ok");
        let cw = StreamingConfig::from_yaml_data(&w).expect("with");
        let cwo = StreamingConfig::from_yaml_data(&wo).expect("without");
        let (kw, _) = cw.aggregation_configs.iter().next().unwrap();
        let (kwo, _) = cwo.aggregation_configs.iter().next().unwrap();
        assert_eq!(kw, kwo, "explicit aggregationId in YAML must not change identity");
        assert_ne!(*kw, 42, "the explicit value must NOT leak through as the map key");
    }
}
