use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::collections::{BTreeMap, HashMap};

use crate::enums::{QueryLanguage, WindowType};
use crate::traits::SerializableToSink;
use crate::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::AggregationType;
use xxhash_rust::xxh64::xxh64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub aggregation_id: u64,
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

/// Aggregation IDs and types for both the key and value dimensions of a query.
/// For single-population queries, key and value share the same ID and type.
/// For multi-population queries (e.g. Topk), they differ.
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    pub aggregation_id_for_key: u64,
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: AggregationType,
    pub aggregation_type_for_value: AggregationType,
}

// TODO: need to implement deserialization methods

/// Derive a stable `aggregation_id` from the agg-config content when the
/// controller-emitted YAML omits the `aggregationId` field. Phase 5 M2
/// follow-up: same (metric, agg_type, sub_type, parameters,
/// grouping_labels) tuple always yields the same id, so the controller
/// no longer needs to mint one. xxh64 keeps the id portable across
/// hosts (vs `std::hash::DefaultHasher`, which is not stable).
///
/// Parameters are canonicalized via BTreeMap so map iteration order
/// doesn't affect the result. The fingerprint format is private to
/// this function — never persisted, never compared across versions.
///
/// 0 is reserved as a sentinel in legacy test fixtures; if a real
/// input hashes to 0 (vanishingly unlikely), we perturb to 1.
pub fn compute_agg_config_id(
    metric: &str,
    aggregation_type: &AggregationType,
    aggregation_sub_type: &str,
    parameters: &HashMap<String, Value>,
    grouping_labels: &KeyByLabelNames,
) -> u64 {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(metric.as_bytes());
    buf.push(0);
    // AggregationType: Serialize impl yields a stable string repr.
    buf.extend_from_slice(
        serde_json::to_string(aggregation_type)
            .unwrap_or_default()
            .as_bytes(),
    );
    buf.push(0);
    buf.extend_from_slice(aggregation_sub_type.as_bytes());
    buf.push(0);
    // Canonicalize parameters: sort by key, render each value via
    // serde_json so nested structure is encoded deterministically.
    let sorted: BTreeMap<&String, &Value> = parameters.iter().collect();
    for (k, v) in sorted {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b'=');
        buf.extend_from_slice(serde_json::to_string(v).unwrap_or_default().as_bytes());
        buf.push(b';');
    }
    buf.push(0);
    // grouping_labels.labels is already sorted at construction.
    for l in &grouping_labels.labels {
        buf.extend_from_slice(l.as_bytes());
        buf.push(b',');
    }
    let h = xxh64(&buf, 0);
    if h == 0 {
        1
    } else {
        h
    }
}

impl AggregationConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregation_id: u64,
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
            aggregation_id,
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

    // pub fn with_sub_type(mut self, sub_type: String) -> Self {
    //     self.aggregation_sub_type = Some(sub_type);
    //     self
    // }

    // pub fn with_parameters(mut self, parameters: HashMap<String, String>) -> Self {
    //     self.parameters = parameters;
    //     self
    // }

    pub fn with_original_yaml(mut self, yaml: String) -> Self {
        self.original_yaml = yaml;
        self
    }

    pub fn deserialize_from_json(
        data: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // M2 follow-up — `aggregationId` is now optional. When the
        // controller-emitted YAML omits it, we derive a deterministic
        // id from the agg-config content.
        let explicit_id = data["aggregationId"].as_u64();

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

        let aggregation_id = explicit_id.unwrap_or_else(|| {
            compute_agg_config_id(
                &metric,
                &aggregation_type,
                &aggregation_sub_type,
                &parameters,
                &grouping_labels,
            )
        });

        Ok(Self::new(
            aggregation_id,
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
        // M2 follow-up — `aggregationId` is optional. When the
        // controller-emitted YAML omits it, derive from content below.
        let explicit_id = aggregation_data["aggregationId"].as_u64();

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

        let aggregation_id = explicit_id.unwrap_or_else(|| {
            compute_agg_config_id(
                &metric,
                &aggregation_type,
                &aggregation_sub_type,
                &parameters,
                &grouping_labels,
            )
        });

        Ok(Self::new(
            aggregation_id,
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
        let mut json = serde_json::json!({
            "aggregationId": self.aggregation_id,
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

    #[test]
    fn explicit_aggregation_id_is_honored() {
        let cfg = AggregationConfig::from_yaml_data(
            &sample_yaml(true),
            None,
            QueryLanguage::promql,
        )
        .expect("parse ok");
        assert_eq!(cfg.aggregation_id, 42);
    }

    #[test]
    fn missing_aggregation_id_is_derived_deterministically() {
        let a = AggregationConfig::from_yaml_data(
            &sample_yaml(false),
            None,
            QueryLanguage::promql,
        )
        .expect("parse without id");
        let b = AggregationConfig::from_yaml_data(
            &sample_yaml(false),
            None,
            QueryLanguage::promql,
        )
        .expect("parse without id again");
        assert_eq!(
            a.aggregation_id, b.aggregation_id,
            "same content yields same derived id"
        );
        assert_ne!(a.aggregation_id, 0, "derived id is never the 0 sentinel");
    }

    #[test]
    fn derived_id_changes_with_metric() {
        let mut params = HashMap::new();
        params.insert("relative_accuracy".to_string(), serde_json::json!(0.01));
        let grouping = KeyByLabelNames::new(vec!["zone".to_string()]);
        let a = compute_agg_config_id(
            "http_latency_ms",
            &AggregationType::DDSketch,
            "",
            &params,
            &grouping,
        );
        let b = compute_agg_config_id(
            "cpu_seconds",
            &AggregationType::DDSketch,
            "",
            &params,
            &grouping,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn derived_id_changes_with_parameters() {
        let grouping = KeyByLabelNames::new(vec!["zone".to_string()]);
        let mut p1 = HashMap::new();
        p1.insert("relative_accuracy".to_string(), serde_json::json!(0.01));
        let mut p2 = HashMap::new();
        p2.insert("relative_accuracy".to_string(), serde_json::json!(0.005));
        let a = compute_agg_config_id("m", &AggregationType::DDSketch, "", &p1, &grouping);
        let b = compute_agg_config_id("m", &AggregationType::DDSketch, "", &p2, &grouping);
        assert_ne!(a, b);
    }

    #[test]
    fn derived_id_independent_of_parameters_insertion_order() {
        let grouping = KeyByLabelNames::new(vec!["zone".to_string()]);
        let mut p_ab = HashMap::new();
        p_ab.insert("alpha".to_string(), serde_json::json!(1));
        p_ab.insert("beta".to_string(), serde_json::json!(2));
        let mut p_ba = HashMap::new();
        p_ba.insert("beta".to_string(), serde_json::json!(2));
        p_ba.insert("alpha".to_string(), serde_json::json!(1));
        let a = compute_agg_config_id("m", &AggregationType::DDSketch, "", &p_ab, &grouping);
        let b = compute_agg_config_id("m", &AggregationType::DDSketch, "", &p_ba, &grouping);
        assert_eq!(a, b);
    }
}
