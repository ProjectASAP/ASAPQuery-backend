use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Index;

use asap_types::enums::QueryLanguage;
use asap_types::{AggregationConfig, MonitorSpec, PolicyRegistry};

use super::storage_backend::StorageBackend;

/// The backend's active streaming policy config: every `AggregationConfig`
/// currently pushed by the controller, plus the storage-backend pin and CDM
/// monitor specs.
///
/// Formerly `asap_types::streaming_config::StreamingConfig` — moved here
/// (see `scratchpad/artifacts/enum-unification-plan.md`) because
/// `control_plane` never actually depended on this type: its own
/// `StreamingConfigEmitter` hand-builds wire-compatible JSON independently,
/// and `PolicyRegistry::from_streaming_config` (the only thing that made
/// `asap_types::PolicyRegistry` -- genuinely shared -- look coupled to this
/// type) had exactly one real caller, this struct's own `policy_registry()`
/// method below. `asap_types` keeps the lower-level `PolicyRegistry::
/// from_configs` primitive this method now calls directly.
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
    /// on [`asap_types::PolicyFingerprint`] — the merged-sid-identity-chain
    /// replacement for the `aggregation_id`-keyed lookup. Cheap (O(N)
    /// over `aggregation_configs.len()`); call at swap time, not per
    /// query, if it shows up in hot-path profiles.
    ///
    /// Dual-keyed transition: this method exists alongside the legacy
    /// `get_aggregation_config(aggregation_id)` so callers can migrate
    /// one at a time. The two views are derived from the same source —
    /// they can never disagree.
    pub fn policy_registry(&self) -> PolicyRegistry {
        PolicyRegistry::from_configs(self.aggregation_configs.values().cloned())
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
                // Retention comes from each aggregation entry; identity is derived from
                // its configuration content.
                let num_aggregates_to_retain = aggregation_data
                    .get("numAggregatesToRetain")
                    .and_then(|v| v.as_u64());
                let config = AggregationConfig::from_yaml_data(
                    aggregation_data,
                    num_aggregates_to_retain,
                    QueryLanguage::PromQl,
                )?;
                if !config.population_key_encoding.is_legacy() {
                    anyhow::bail!(
                        "legacy streaming input does not support this population key encoding"
                    );
                }
                if config.derived_input.is_some() {
                    anyhow::bail!(
                        "legacy streaming input cannot execute a derived summary program"
                    );
                }
                // PR 5: the map key IS the policy-fingerprint u64.
                // `AggregationConfig::policy_fp_u64()` is the canonical
                // accessor for this value.
                aggregation_configs.insert(config.policy_fp_u64(), config);
            }
        }

        let mut config = Self::new(aggregation_configs);
        // Continuous-monitoring (CDM) specs: a top-level `monitors:` array, each
        // entry deserializing into a MonitorSpec. Absent → empty (the common
        // case). The data-plane monitor coordinator reads these.
        if let Some(monitors) = data.get("monitors").and_then(|v| v.as_sequence()) {
            for m in monitors {
                let spec: MonitorSpec = serde_yaml::from_value(m.clone()).map_err(|e| {
                    anyhow::anyhow!("invalid monitor spec in streaming-config: {e}")
                })?;
                config.monitors.push(spec);
            }
        }
        Ok(config)
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

    #[test]
    fn legacy_yaml_rejects_derived_summary_input() {
        let data = serde_yaml::from_str::<Value>(&format!(
            "aggregations:\n- aggregationType: Sum\n  aggregationSubType: ''\n  metric: outer\n  labels: {{grouping: [], rollup: [], aggregated: []}}\n  parameters: {{}}\n  windowSize: 10\n  windowType: tumbling\n  spatialFilter: ''\n  derived_input:\n    inputs: [1]\n    program_sha256: '{}'\n", "a".repeat(64)
        )).unwrap();
        let error = StreamingConfig::from_yaml_data(&data).unwrap_err();
        assert!(error
            .to_string()
            .contains("legacy streaming input cannot execute"));
    }

    #[test]
    fn legacy_yaml_rejects_canonical_population_key_encoding() {
        let data: Value = serde_yaml::from_str(
            r#"
aggregations:
- aggregationType: Sum
  aggregationSubType: ''
  metric: m
  population_key_encoding: canonical_labels_v1
  labels:
    grouping: [host]
    rollup: []
    aggregated: []
  parameters: {}
  windowSize: 60
  windowType: tumbling
  spatialFilter: ''
"#,
        )
        .unwrap();
        let error = StreamingConfig::from_yaml_data(&data).unwrap_err();
        assert!(
            error.to_string().contains("population key encoding"),
            "{error}"
        );
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
        assert_eq!(
            kw, kwo,
            "explicit aggregationId in YAML must not change identity"
        );
        assert_ne!(
            *kw, 42,
            "the explicit value must NOT leak through as the map key"
        );
    }

    #[test]
    fn from_yaml_data_parses_monitors_section() {
        // CDM monitor specs: a top-level `monitors:` array must populate
        // StreamingConfig.monitors (the data-plane coordinator reads these).
        let yaml = "\
aggregations: []\n\
monitors:\n\
- agg_id: 16346598078036168951\n  key: \"\"\n  tau: 5000.0\n  epsilon: 0.05\n  window_ms: 10000\n";
        let data: Value = serde_yaml::from_str(yaml).expect("yaml ok");
        let cfg = StreamingConfig::from_yaml_data(&data).expect("decode monitors");
        assert_eq!(cfg.monitors().len(), 1, "monitors: section must be parsed");
        let m = &cfg.monitors()[0];
        assert_eq!(m.agg_id, 16346598078036168951);
        assert_eq!(m.tau, 5000.0);
        assert_eq!(m.window_ms, 10000);
        assert_eq!(m.epsilon, 0.05);
    }

    #[test]
    fn from_yaml_data_absent_monitors_is_empty() {
        let yaml = "aggregations: []\n";
        let data: Value = serde_yaml::from_str(yaml).expect("yaml ok");
        let cfg = StreamingConfig::from_yaml_data(&data).expect("decode");
        assert!(
            cfg.monitors().is_empty(),
            "no monitors: → empty (byte-compat)"
        );
    }
}
