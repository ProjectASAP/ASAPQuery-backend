use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;

use crate::enums::{QueryLanguage, WindowType};
use crate::policy_fingerprint::PolicyFingerprint;
use crate::traits::SerializableToSink;
use crate::utils::normalize_spatial_filter;
use crate::KeyByLabelNames;
use promql_utilities::query_logics::enums::AggregationType;

/// Per-aggregation policy carried in the streaming config.
///
/// **PR 5 (merged-sid-identity refactor)** retired the
/// controller-allocated `aggregation_id: u64` field. Identity is now
/// content-addressed via [`PolicyFingerprint`] — derive on demand with
/// [`PolicyFingerprint::from_config(&cfg)`].
///
/// The YAML wire shape no longer carries `aggregationId` (the
/// controller stopped emitting it in M2.2; this PR makes the backend
/// stop reading it). Existing fixtures that still spell out
/// `aggregationId: N` parse cleanly — the field is silently dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub aggregation_type: AggregationType,
    pub aggregation_sub_type: String,
    pub parameters: HashMap<String, Value>,
    pub grouping_labels: KeyByLabelNames,
    pub aggregated_labels: KeyByLabelNames,
    pub rollup_labels: KeyByLabelNames,
    pub original_yaml: String,

    pub window_size: u64,        // Window size in seconds (e.g., 900s for 15m)
    pub slide_interval: u64,     // Slide/hop interval in seconds (e.g., 30s)
    pub window_type: WindowType, // Tumbling or Sliding

    pub spatial_filter: String,
    pub spatial_filter_normalized: String,
    pub metric: String, // PromQL mode: metric name; SQL mode: derived from table_name.value_column
    pub num_aggregates_to_retain: Option<u64>,

    // SQL-specific fields (optional, used when query_language=sql)
    pub table_name: Option<String>,   // SQL mode: table name
    pub value_column: Option<String>, // SQL mode: which value column to aggregate
}

/// Policy-match handles for both the key and value dimensions of a
/// query. For single-population queries, key and value share the same
/// fingerprint and type. For multi-population queries (e.g. Topk), they
/// differ.
///
/// **PR 5**: the `aggregation_id_for_*: u64` fields now carry the
/// `PolicyFingerprint::as_u64()` form of the matched config, NOT a
/// controller-allocated id. Callers that need typed identity can call
/// [`AggregationIdInfo::policy_fp_for_key`] /
/// [`AggregationIdInfo::policy_fp_for_value`].
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    /// `PolicyFingerprint::as_u64()` of the key aggregation's config.
    pub aggregation_id_for_key: u64,
    /// `PolicyFingerprint::as_u64()` of the value aggregation's config.
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: AggregationType,
    pub aggregation_type_for_value: AggregationType,
}

impl AggregationIdInfo {
    pub fn policy_fp_for_key(&self) -> PolicyFingerprint {
        PolicyFingerprint(self.aggregation_id_for_key)
    }
    pub fn policy_fp_for_value(&self) -> PolicyFingerprint {
        PolicyFingerprint(self.aggregation_id_for_value)
    }
}

impl AggregationConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregation_type: AggregationType,
        aggregation_sub_type: String,
        parameters: HashMap<String, Value>,
        grouping_labels: KeyByLabelNames,
        aggregated_labels: KeyByLabelNames,
        rollup_labels: KeyByLabelNames,
        original_yaml: String,
        window_size: u64,
        slide_interval: u64,
        window_type: WindowType,
        spatial_filter: String,
        metric: String,
        num_aggregates_to_retain: Option<u64>,
        // SQL-specific fields
        table_name: Option<String>,
        value_column: Option<String>,
    ) -> Self {
        // Generate normalized spatial filter (placeholder implementation)
        let spatial_filter_normalized = normalize_spatial_filter(&spatial_filter);

        Self {
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            spatial_filter_normalized,
            metric,
            num_aggregates_to_retain,
            table_name,
            value_column,
        }
    }

    /// Content-addressed identity for this config. Sugar over
    /// [`PolicyFingerprint::from_config`].
    pub fn policy_fingerprint(&self) -> PolicyFingerprint {
        PolicyFingerprint::from_config(self)
    }

    /// `PolicyFingerprint::as_u64()` — the u64-form handle used by the
    /// policy-fingerprint-keyed call sites (e.g. `StreamingConfig`'s
    /// `HashMap<u64, AggregationConfig>` keys). **Always** equal to
    /// `self.policy_fingerprint().as_u64()`. The value is content-
    /// addressed identity, NOT a controller-allocated counter id.
    pub fn policy_fp_u64(&self) -> u64 {
        self.policy_fingerprint().as_u64()
    }

    pub fn with_original_yaml(mut self, yaml: String) -> Self {
        self.original_yaml = yaml;
        self
    }

    pub fn deserialize_from_json(
        data: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // `aggregationId` is silently ignored — identity is
        // content-addressed via PolicyFingerprint (PR 5).

        let aggregation_type: AggregationType = data["aggregationType"]
            .as_str()
            .ok_or("Missing aggregationType")?
            .parse()
            .map_err(|e: String| e)?;

        let aggregation_sub_type = data["aggregationSubType"]
            .as_str()
            .ok_or("Missing aggregationSubType")?
            .to_string();

        let parameters = data["parameters"]
            .as_object()
            .ok_or("Missing parameters")?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Note: In Python, eval(data["originalYaml"]) is used, but this is unsafe
        // Using the string value directly instead
        let original_yaml = data["originalYaml"].as_str().unwrap_or("").to_string();

        // Deserialize KeyByLabelNames - assuming they have deserialize_from_json methods
        let grouping_labels = KeyByLabelNames::deserialize_from_json(&data["groupingLabels"])?;
        let aggregated_labels = KeyByLabelNames::deserialize_from_json(&data["aggregatedLabels"])?;
        let rollup_labels = KeyByLabelNames::deserialize_from_json(&data["rollupLabels"])?;

        let window_size = data["windowSize"].as_u64().ok_or("Missing windowSize")?;

        let window_type = data
            .get("windowType")
            .and_then(|v| v.as_str())
            .unwrap_or("tumbling")
            .parse::<WindowType>()
            .unwrap_or_default();

        let slide_interval = data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let spatial_filter = data["spatialFilter"].as_str().unwrap_or("").to_string();

        let metric = data["metric"].as_str().ok_or("Missing metric")?.to_string();

        let num_aggregates_to_retain = data.get("numAggregatesToRetain").and_then(|v| v.as_u64());

        // SQL-specific fields (optional)
        let table_name = data
            .get("tableName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let value_column = data
            .get("valueColumn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Self::new(
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            metric,
            num_aggregates_to_retain,
            table_name,
            value_column,
        ))
    }

    pub fn deserialize_from_bytes(
        bytes: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_str = std::str::from_utf8(bytes)?.trim();
        let data: Value = serde_json::from_str(data_str)?;
        Self::deserialize_from_json(&data)
    }

    pub fn from_yaml_data(
        aggregation_data: &serde_yaml::Value,
        num_aggregates_to_retain: Option<u64>,
        query_language: QueryLanguage,
    ) -> Result<Self, anyhow::Error> {
        // `aggregationId` is silently dropped — identity is
        // content-addressed via PolicyFingerprint (PR 5). Pre-PR-5
        // fixtures that still spell out the field parse cleanly.

        let labels = &aggregation_data["labels"];
        let grouping_labels = KeyByLabelNames::new(
            labels["grouping"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing grouping labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let aggregated_labels = KeyByLabelNames::new(
            labels["aggregated"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing aggregated labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let rollup_labels = KeyByLabelNames::new(
            labels["rollup"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing rollup labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );

        let aggregation_type: AggregationType = aggregation_data["aggregationType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationType"))?
            .parse()
            .map_err(|e: String| anyhow::anyhow!(e))?;

        let aggregation_sub_type = aggregation_data["aggregationSubType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationSubType"))?
            .to_string();

        // Convert serde_yaml::Value to serde_json::Value for parameters
        let parameters: HashMap<String, Value> = aggregation_data["parameters"]
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("Missing parameters"))?
            .iter()
            .map(|(k, v)| {
                let key = k.as_str().unwrap_or("").to_string();
                let value = serde_json::to_value(v).unwrap_or(Value::Null);
                (key, value)
            })
            .collect();

        let window_size = aggregation_data["windowSize"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing windowSize"))?;

        let window_type = aggregation_data
            .get("windowType")
            .and_then(|v| v.as_str())
            .unwrap_or("tumbling")
            .parse::<WindowType>()
            .unwrap_or_default();

        let slide_interval = aggregation_data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let spatial_filter = aggregation_data["spatialFilter"]
            .as_str()
            .unwrap_or("")
            .to_string();

        // Only PromQL is supported after the dead-code cleanup.
        let (metric, table_name, value_column) = match query_language {
            QueryLanguage::promql => {
                let metric = aggregation_data["metric"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing metric for PromQL query language"))?
                    .to_string();
                (metric, None, None)
            }
        };

        Ok(Self::new(
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            String::new(), // original_yaml - empty as in Python
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            metric,
            num_aggregates_to_retain,
            table_name,
            value_column,
        ))
    }
}

impl SerializableToSink for AggregationConfig {
    fn serialize_to_json(&self) -> Value {
        // PR 5: `aggregationId` is no longer emitted — readers derive it
        // from content via `PolicyFingerprint::from_config(...).as_u64()`.
        let mut json = serde_json::json!({
            "aggregationType": self.aggregation_type,
            "aggregationSubType": self.aggregation_sub_type,
            "parameters": self.parameters,
            "originalYaml": self.original_yaml,
            "windowSize": self.window_size,
            "slideInterval": self.slide_interval,
            "windowType": self.window_type.to_string(),
            "spatialFilter": self.spatial_filter,
            "metric": self.metric,
        });

        // Only include numAggregatesToRetain if it's Some
        if let Some(num_aggregates) = self.num_aggregates_to_retain {
            json["numAggregatesToRetain"] = serde_json::json!(num_aggregates);
        }

        // SQL-specific fields (only include if present)
        if let Some(ref table_name) = self.table_name {
            json["tableName"] = serde_json::json!(table_name);
        }
        if let Some(ref value_column) = self.value_column {
            json["valueColumn"] = serde_json::json!(value_column);
        }

        json
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.original_yaml.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_yaml(with_id: bool) -> serde_yaml::Value {
        let id_line = if with_id { "aggregationId: 42\n" } else { "" };
        let yaml = format!(
            "{id_line}aggregationType: DDSketch\naggregationSubType: ''\nmetric: http_latency_ms\nlabels:\n  grouping: [zone]\n  rollup: []\n  aggregated: []\nparameters:\n  relative_accuracy: 0.01\nwindowSize: 30\nwindowType: tumbling\nspatialFilter: ''\n",
            id_line = id_line
        );
        serde_yaml::from_str(&yaml).expect("yaml parses")
    }

    /// PR 5: `aggregationId` in the YAML is silently dropped — identity
    /// is derived from content. A fixture with the field parses to the
    /// SAME config as a fixture without it.
    #[test]
    fn explicit_aggregation_id_in_yaml_is_ignored() {
        let with =
            AggregationConfig::from_yaml_data(&sample_yaml(true), None, QueryLanguage::promql)
                .expect("parse ok");
        let without =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::promql)
                .expect("parse ok");
        assert_eq!(
            with.policy_fingerprint(),
            without.policy_fingerprint(),
            "explicit aggregationId in the YAML must not change the policy fingerprint",
        );
    }

    /// Round-tripping the same content yields the same fingerprint.
    #[test]
    fn fingerprint_is_deterministic_per_content() {
        let a = AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::promql)
            .expect("parse a");
        let b = AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::promql)
            .expect("parse b");
        assert_eq!(a.policy_fingerprint(), b.policy_fingerprint());
        assert_ne!(
            a.policy_fingerprint().as_u64(),
            0,
            "fingerprint is never the 0 sentinel for a real config",
        );
    }

    /// The `policy_fp_u64()` accessor is exactly the fingerprint u64.
    #[test]
    fn policy_fp_u64_accessor_equals_fingerprint_u64() {
        let cfg =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::promql)
                .expect("parse");
        assert_eq!(cfg.policy_fp_u64(), cfg.policy_fingerprint().as_u64());
    }

    /// PR 5: `serialize_to_json` no longer emits `aggregationId`.
    #[test]
    fn serialize_to_json_omits_aggregation_id() {
        let cfg =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::promql)
                .expect("parse");
        let json = cfg.serialize_to_json();
        assert!(
            json.get("aggregationId").is_none(),
            "PR 5: aggregationId must not appear on the wire — readers derive it from content"
        );
    }
}
