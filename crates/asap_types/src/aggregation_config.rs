use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;

use crate::enums::{QueryLanguage, WindowKind};
use crate::policy_fingerprint::PolicyFingerprint;
use crate::traits::SerializableToSink;
use crate::utils::normalize_spatial_filter;
use crate::AggregationType;
use crate::KeyByLabelNames;

/// Physical maintenance layout for one semantic windowed summary.
///
/// `window_size` and `slide_interval` on [`PrecomputeMaterialization`] retain
/// the query's window and evaluation cadence. This enum describes how that
/// semantic window is represented in storage; it must never be inferred by
/// overloading either semantic duration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WindowMaterializationLayout {
    /// Store disjoint mergeable states and compose a query window at read time.
    Pane { pane_secs: u64 },
    /// Maintain one complete state for every evaluation point.
    FullWindow,
    /// Store base panes plus coarser mergeable rollups. Every level is a
    /// duration in seconds and is an integer multiple of its predecessor.
    HierarchicalRollup {
        base_pane_secs: u64,
        levels_secs: Vec<u64>,
    },
}

impl WindowMaterializationLayout {
    pub fn base_pane_secs(&self) -> u64 {
        match self {
            Self::Pane { pane_secs } => *pane_secs,
            Self::FullWindow => 0,
            Self::HierarchicalRollup { base_pane_secs, .. } => *base_pane_secs,
        }
    }

    pub fn validate(&self, window_secs: u64, slide_secs: u64) -> Result<(), String> {
        if window_secs == 0 || slide_secs == 0 || slide_secs > window_secs {
            return Err(
                "window and slide must be positive and slide must not exceed window".into(),
            );
        }
        match self {
            Self::Pane { pane_secs } => {
                if *pane_secs == 0
                    || !window_secs.is_multiple_of(*pane_secs)
                    || !slide_secs.is_multiple_of(*pane_secs)
                {
                    return Err("pane size must divide both window size and slide".into());
                }
            }
            Self::FullWindow => {}
            Self::HierarchicalRollup {
                base_pane_secs,
                levels_secs,
            } => {
                if *base_pane_secs == 0
                    || !window_secs.is_multiple_of(*base_pane_secs)
                    || !slide_secs.is_multiple_of(*base_pane_secs)
                    || levels_secs.is_empty()
                {
                    return Err(
                        "rollup base pane must divide window and slide, with at least one level"
                            .into(),
                    );
                }
                let mut previous = *base_pane_secs;
                for level in levels_secs {
                    if *level <= previous
                        || *level % previous != 0
                        || !window_secs.is_multiple_of(*level)
                    {
                        return Err(
                            "rollup levels must increase by integral factors and divide the window"
                                .into(),
                        );
                    }
                    previous = *level;
                }
            }
        }
        Ok(())
    }
}

/// Per-aggregation policy with content-derived [`PolicyFingerprint`] identity.
/// An `aggregationId` field in input YAML is ignored for compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputeMaterialization {
    pub aggregation_type: AggregationType,
    pub aggregation_sub_type: String,
    pub parameters: HashMap<String, Value>,
    #[serde(serialize_with = "crate::grouping_projection::serialize_config_grouping")]
    pub grouping_labels: crate::GroupingProjection,
    #[serde(
        default,
        skip_serializing_if = "crate::grouping_projection::PopulationKeyEncoding::is_legacy"
    )]
    pub population_key_encoding: crate::grouping_projection::PopulationKeyEncoding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitioning: Option<crate::sds::PopulationPartitioning>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_input: Option<crate::derived_input::DerivedInputIdentity>,
    pub aggregated_labels: KeyByLabelNames,
    pub rollup_labels: KeyByLabelNames,
    pub original_yaml: String,

    pub window_size: u64,        // Window size in seconds (e.g., 900s for 15m)
    pub slide_interval: u64,     // Slide/hop interval in seconds (e.g., 30s)
    pub window_type: WindowKind, // Tumbling or Sliding
    pub window_layout: WindowMaterializationLayout,
    /// Unix millisecond timestamp on the pane-boundary grid selected from
    /// the consuming query workload. Missing on legacy definitions, which
    /// must not be used for certified pane-only reads.
    #[serde(
        default,
        alias = "paneOriginMs",
        skip_serializing_if = "Option::is_none"
    )]
    pub pane_origin_ms: Option<i64>,

    pub spatial_filter: String,
    pub spatial_filter_normalized: String,
    pub metric: String, // PromQL mode: metric name; SQL mode: derived from table_name.value_column
    pub num_aggregates_to_retain: Option<u64>,

    // SQL-specific fields (optional, used when query_language=sql)
    pub table_name: Option<String>, // SQL mode: table name
    #[serde(
        default,
        alias = "value_column",
        alias = "valueColumn",
        alias = "valueProjection",
        deserialize_with = "crate::sds::deserialize_optional_value_projection"
    )]
    pub value_projection: Option<crate::sds::ValueProjectionIdentity>,
    /// Table timestamp projection, in Unix milliseconds.
    #[serde(
        default,
        alias = "tableTimestampColumn",
        skip_serializing_if = "Option::is_none"
    )]
    pub table_timestamp_column: Option<String>,
    #[serde(
        default,
        alias = "tablePopulation",
        skip_serializing_if = "Option::is_none"
    )]
    pub table_population: Option<crate::table_population::TablePopulation>,
    /// Producer typing for a SQL table value projection: the source column's
    /// declared type and nullability.
    ///
    /// **Why this is separate from [`Self::value_projection`]**: the
    /// projection is the materialization's *identity* — which column or
    /// constant is summarised, and part of the policy fingerprint. This is how
    /// the ingest path must *read* that column, which the identity does not
    /// determine: an integer column needs an exactness guard on its way into
    /// f64 summary state, and a nullable column needs SQL's "aggregates skip
    /// NULL" rule reproduced at the reader rather than a decode failure on the
    /// first NULL row. Two materializations over the same column are the same
    /// policy either way, so this deliberately stays out of the fingerprint.
    ///
    /// `None` ⇒ PromQL-mode materializations and legacy SQL definitions, which
    /// keep the pre-typed behaviour (read the column as it comes).
    #[serde(
        default,
        alias = "valueSourceColumn",
        skip_serializing_if = "Option::is_none"
    )]
    pub value_source_column: Option<planner_types::pre_asap::Column>,
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

/// Compatibility name for legacy streaming-config and precompute call sites.
/// New PhysicalPlan code should use [`PrecomputeMaterialization`].
pub type AggregationConfig = PrecomputeMaterialization;

impl PrecomputeMaterialization {
    pub fn effective_value_projection(&self) -> &crate::sds::ValueProjectionIdentity {
        self.value_projection
            .as_ref()
            .unwrap_or(&crate::sds::ValueProjectionIdentity::SampleValue)
    }

    /// Temporal extent of one stored base state, independent of emission cadence.
    pub fn stored_window_ms(&self) -> u64 {
        match &self.window_layout {
            WindowMaterializationLayout::FullWindow => self.window_size,
            layout => layout.base_pane_secs(),
        }
        .saturating_mul(1_000)
    }

    pub fn source_identity(&self) -> crate::sds::DataSourceIdentity {
        use crate::sds::DataSourceIdentity;
        if let Some(input) = &self.derived_input {
            DataSourceIdentity::Derived {
                input: input.clone(),
            }
        } else if let Some(table_ref) = &self.table_name {
            DataSourceIdentity::Table {
                table_ref: table_ref.clone(),
            }
        } else {
            DataSourceIdentity::TimeSeries {
                metric: self.metric.clone(),
            }
        }
    }

    pub fn population_filter_canonical(&self) -> Result<String, String> {
        if let Some(input) = &self.derived_input {
            input.validate()?;
            if self.table_name.is_some()
                || self.table_population.is_some()
                || self.table_timestamp_column.is_some()
                || !self.spatial_filter.is_empty()
            {
                return Err("derived inputs cannot also declare a raw source/filter".into());
            }
        }
        self.effective_value_projection().validate()?;
        if self.value_projection.is_some()
            && self.table_name.is_none()
            && self.derived_input.is_none()
        {
            return Err("explicit table value projection requires a table source".into());
        }
        if let Some(column) = &self.table_timestamp_column {
            if self.table_name.is_none() || column.is_empty() {
                return Err("table timestamp projection requires a table and a column".into());
            }
            crate::table_population::validate_column_name(column)?;
        }
        if self.table_name.is_some() && !self.spatial_filter.is_empty() {
            return Err("table populations cannot use a PromQL label filter".into());
        }
        if let Some(population) = &self.table_population {
            if self.table_name.is_none() || !self.spatial_filter.is_empty() {
                return Err(
                    "typed table population requires a table and no PromQL label filter".into(),
                );
            }
            population.validate()?;
            Ok(population.canonical())
        } else {
            Ok(normalize_spatial_filter(&self.spatial_filter))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregation_type: AggregationType,
        aggregation_sub_type: String,
        parameters: HashMap<String, Value>,
        grouping_labels: impl Into<crate::GroupingProjection>,
        aggregated_labels: KeyByLabelNames,
        rollup_labels: KeyByLabelNames,
        original_yaml: String,
        window_size: u64,
        slide_interval: u64,
        window_type: WindowKind,
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
            grouping_labels: grouping_labels.into(),
            population_key_encoding: Default::default(),
            partitioning: None,
            derived_input: None,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            window_size,
            slide_interval,
            window_type,
            window_layout: WindowMaterializationLayout::Pane {
                pane_secs: if slide_interval == 0 {
                    window_size
                } else {
                    slide_interval
                },
            },
            pane_origin_ms: None,
            spatial_filter,
            spatial_filter_normalized,
            metric,
            num_aggregates_to_retain,
            table_name,
            value_projection: value_column
                .map(|name| crate::sds::ValueProjectionIdentity::Column { name }),
            table_population: None,
            table_timestamp_column: None,
            value_source_column: None,
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
        if [
            "valueColumn",
            "value_column",
            "valueProjection",
            "value_projection",
        ]
        .iter()
        .filter(|key| data.get(**key).is_some_and(|value| !value.is_null()))
        .count()
            > 1
        {
            return Err("multiple value projection fields are not allowed".into());
        }
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
        let grouping_labels =
            crate::GroupingProjection::deserialize_from_json(&data["groupingLabels"])?;
        let aggregated_labels = KeyByLabelNames::deserialize_from_json(&data["aggregatedLabels"])?;
        let rollup_labels = KeyByLabelNames::deserialize_from_json(&data["rollupLabels"])?;

        let window_size = data["windowSize"].as_u64().ok_or("Missing windowSize")?;

        let window_type = data
            .get("windowType")
            .and_then(|v| v.as_str())
            .unwrap_or("tumbling")
            .parse::<WindowKind>()
            .unwrap_or_default();

        let slide_interval = data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let pane_origin_ms = data
            .get("paneOriginMs")
            .or_else(|| data.get("pane_origin_ms"))
            .and_then(|v| v.as_i64());

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

        let mut config = Self::new(
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
        );
        if data.get("windowLayout").is_some() && data.get("window_layout").is_some() {
            return Err("multiple window layout fields are not allowed".into());
        }
        if let Some(layout) = data
            .get("windowLayout")
            .or_else(|| data.get("window_layout"))
        {
            config.window_layout = serde_json::from_value(layout.clone())?;
            config
                .window_layout
                .validate(config.window_size, config.slide_interval)?;
        }
        config.population_key_encoding = data
            .get("population_key_encoding")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        config.derived_input = data
            .get("derived_input")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?;
        config.partitioning = data
            .get("partitioning")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?;
        if let Some(projection) = data
            .get("valueProjection")
            .or_else(|| data.get("value_projection"))
            .filter(|value| !value.is_null())
        {
            config.value_projection = Some(serde_json::from_value(projection.clone())?);
        }
        config.pane_origin_ms = pane_origin_ms;
        config.table_timestamp_column = data
            .get("tableTimestampColumn")
            .or_else(|| data.get("table_timestamp_column"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        config.table_population = data
            .get("tablePopulation")
            .or_else(|| data.get("table_population"))
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        config.population_filter_canonical()?;
        Ok(config)
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
        let grouping_labels: crate::GroupingProjection =
            serde_yaml::from_value(labels["grouping"].clone())?;
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
            .parse::<WindowKind>()
            .unwrap_or_default();

        let slide_interval = aggregation_data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let pane_origin_ms = aggregation_data
            .get("paneOriginMs")
            .or_else(|| aggregation_data.get("pane_origin_ms"))
            .and_then(|v| v.as_i64());

        let spatial_filter = aggregation_data["spatialFilter"]
            .as_str()
            .unwrap_or("")
            .to_string();

        if [
            "valueColumn",
            "value_column",
            "valueProjection",
            "value_projection",
        ]
        .iter()
        .filter(|key| {
            aggregation_data
                .get(**key)
                .is_some_and(|value| !value.is_null())
        })
        .count()
            > 1
        {
            return Err(anyhow::anyhow!(
                "multiple value projection fields are not allowed"
            ));
        }
        let typed_projection: Option<crate::sds::ValueProjectionIdentity> = aggregation_data
            .get("valueProjection")
            .or_else(|| aggregation_data.get("value_projection"))
            .filter(|value| !value.is_null())
            .map(|value| serde_json::to_value(value).and_then(serde_json::from_value))
            .transpose()?;
        let (metric, table_name, value_column) = match query_language {
            QueryLanguage::PromQl | QueryLanguage::MetricsQl => {
                let metric = aggregation_data["metric"]
                    .as_str()
                    .ok_or_else(|| {
                        anyhow::anyhow!("Missing metric for time-series query language")
                    })?
                    .to_string();
                (metric, None, None)
            }
            QueryLanguage::ClickHouseSql => {
                let table = aggregation_data["tableName"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing tableName for ClickHouse SQL"))?
                    .to_string();
                let column = aggregation_data["valueColumn"]
                    .as_str()
                    .or_else(|| {
                        typed_projection
                            .as_ref()
                            .and_then(|projection| projection.column())
                    })
                    .map(str::to_owned);
                if column.is_none() && typed_projection.is_none() {
                    return Err(anyhow::anyhow!(
                        "Missing value projection for ClickHouse SQL"
                    ));
                }
                (
                    format!("{table}.{}", column.as_deref().unwrap_or("constant")),
                    Some(table),
                    column,
                )
            }
        };

        let mut config = Self::new(
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
        );
        if aggregation_data.get("windowLayout").is_some()
            && aggregation_data.get("window_layout").is_some()
        {
            anyhow::bail!("multiple window layout fields are not allowed");
        }
        if let Some(layout) = aggregation_data
            .get("windowLayout")
            .or_else(|| aggregation_data.get("window_layout"))
        {
            config.window_layout = serde_yaml::from_value(layout.clone())?;
            config
                .window_layout
                .validate(config.window_size, config.slide_interval)
                .map_err(anyhow::Error::msg)?;
        }
        config.population_key_encoding = aggregation_data
            .get("population_key_encoding")
            .map(|value| serde_yaml::from_value(value.clone()))
            .transpose()?
            .unwrap_or_default();
        config.derived_input = aggregation_data
            .get("derived_input")
            .filter(|v| !v.is_null())
            .map(|v| serde_yaml::from_value(v.clone()))
            .transpose()?;
        config.partitioning = aggregation_data
            .get("partitioning")
            .filter(|value| !value.is_null())
            .map(|value| serde_yaml::from_value(value.clone()))
            .transpose()?;
        if let Some(projection) = typed_projection {
            config.value_projection = Some(projection);
        }
        config.pane_origin_ms = pane_origin_ms;
        config.table_timestamp_column = aggregation_data
            .get("tableTimestampColumn")
            .or_else(|| aggregation_data.get("table_timestamp_column"))
            .and_then(serde_yaml::Value::as_str)
            .map(str::to_owned);
        config.table_population = aggregation_data
            .get("tablePopulation")
            .or_else(|| aggregation_data.get("table_population"))
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_yaml::from_value)
            .transpose()?;
        config
            .population_filter_canonical()
            .map_err(anyhow::Error::msg)?;
        Ok(config)
    }
}

impl SerializableToSink for PrecomputeMaterialization {
    fn serialize_to_json(&self) -> Value {
        // PR 5: `aggregationId` is no longer emitted — readers derive it
        // from content via `PolicyFingerprint::from_config(...).as_u64()`.
        let mut json = serde_json::json!({
            "aggregationType": self.aggregation_type,
            "aggregationSubType": self.aggregation_sub_type,
            "parameters": self.parameters,
            "partitioning": self.partitioning,
            "originalYaml": self.original_yaml,
            "windowSize": self.window_size,
            "slideInterval": self.slide_interval,
            "windowLayout": self.window_layout,
            "windowType": self.window_type.to_string(),
            "spatialFilter": self.spatial_filter,
            "metric": self.metric,
        });

        if !self.population_key_encoding.is_legacy() {
            json["population_key_encoding"] = serde_json::json!(self.population_key_encoding);
        }
        if let Some(input) = &self.derived_input {
            json["derived_input"] = serde_json::json!(input);
        }
        // Only include numAggregatesToRetain if it's Some
        if let Some(num_aggregates) = self.num_aggregates_to_retain {
            json["numAggregatesToRetain"] = serde_json::json!(num_aggregates);
        }
        if let Some(pane_origin_ms) = self.pane_origin_ms {
            json["paneOriginMs"] = serde_json::json!(pane_origin_ms);
        }

        // SQL-specific fields (only include if present)
        if let Some(ref table_name) = self.table_name {
            json["tableName"] = serde_json::json!(table_name);
        }
        if let Some(ref projection) = self.value_projection {
            json["valueProjection"] = serde_json::json!(projection);
        }
        if let Some(ref population) = self.table_population {
            json["tablePopulation"] = serde_json::json!(population);
        }
        if let Some(ref column) = self.table_timestamp_column {
            json["tableTimestampColumn"] = serde_json::json!(column);
        }

        json
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.original_yaml.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod window_layout_tests {
    use super::WindowMaterializationLayout;

    #[test]
    fn validates_multiple_slides_and_rejects_uncomposable_panes() {
        for slide in [5, 10, 15, 30] {
            WindowMaterializationLayout::Pane { pane_secs: 5 }
                .validate(60, slide)
                .unwrap();
            WindowMaterializationLayout::FullWindow
                .validate(60, slide)
                .unwrap();
        }
        assert!(WindowMaterializationLayout::Pane { pane_secs: 7 }
            .validate(60, 10)
            .is_err());
        assert!(WindowMaterializationLayout::HierarchicalRollup {
            base_pane_secs: 5,
            levels_secs: vec![10, 30],
        }
        .validate(60, 10)
        .is_ok());
        assert!(WindowMaterializationLayout::HierarchicalRollup {
            base_pane_secs: 5,
            levels_secs: vec![12],
        }
        .validate(60, 10)
        .is_err());
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
            AggregationConfig::from_yaml_data(&sample_yaml(true), None, QueryLanguage::PromQl)
                .expect("parse ok");
        let without =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
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
        let a = AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
            .expect("parse a");
        let b = AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
            .expect("parse b");
        assert_eq!(a.policy_fingerprint(), b.policy_fingerprint());
        assert_ne!(
            a.policy_fingerprint().as_u64(),
            0,
            "fingerprint is never the 0 sentinel for a real config",
        );
    }

    #[test]
    fn explicit_window_layout_survives_custom_json_and_yaml_transport() {
        use super::WindowMaterializationLayout;
        let mut yaml = sample_yaml(false);
        yaml["windowSize"] = serde_yaml::to_value(60).unwrap();
        yaml["slideInterval"] = serde_yaml::to_value(10).unwrap();
        for layout in [
            WindowMaterializationLayout::FullWindow,
            WindowMaterializationLayout::Pane { pane_secs: 5 },
            WindowMaterializationLayout::HierarchicalRollup {
                base_pane_secs: 5,
                levels_secs: vec![10, 30],
            },
        ] {
            yaml["windowLayout"] = serde_yaml::to_value(&layout).unwrap();
            let config =
                AggregationConfig::from_yaml_data(&yaml, None, QueryLanguage::PromQl).unwrap();
            assert_eq!(config.window_layout, layout);
            let mut wire = config.serialize_to_json();
            wire["groupingLabels"] = serde_json::to_value(&config.grouping_labels).unwrap();
            wire["aggregatedLabels"] =
                serde_json::to_value(&config.aggregated_labels.labels).unwrap();
            wire["rollupLabels"] = serde_json::to_value(&config.rollup_labels.labels).unwrap();
            let decoded = AggregationConfig::deserialize_from_json(&wire).unwrap();
            assert_eq!(decoded.window_layout, layout);
            assert_eq!(decoded.stored_window_ms(), config.stored_window_ms());
            assert_eq!(decoded.policy_fingerprint(), config.policy_fingerprint());
            wire["window_layout"] = wire["windowLayout"].clone();
            assert!(AggregationConfig::deserialize_from_json(&wire).is_err());
        }
        yaml.as_mapping_mut()
            .unwrap()
            .remove(serde_yaml::Value::from("windowLayout"));
        let legacy = AggregationConfig::from_yaml_data(&yaml, None, QueryLanguage::PromQl).unwrap();
        assert_eq!(
            legacy.window_layout,
            WindowMaterializationLayout::Pane { pane_secs: 10 }
        );
        yaml["window_layout"] = serde_yaml::from_str("{kind: pane, pane_secs: 7}").unwrap();
        assert!(AggregationConfig::from_yaml_data(&yaml, None, QueryLanguage::PromQl).is_err());
    }

    #[test]
    fn pane_origin_round_trips_and_changes_definition_identity() {
        let mut epoch =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
                .expect("parse");
        let unknown = epoch.policy_fingerprint();
        epoch.pane_origin_ms = Some(7_000);
        let planned = epoch.policy_fingerprint();
        assert_ne!(unknown, planned);

        let wire = epoch.serialize_to_json();
        assert_eq!(wire["paneOriginMs"], serde_json::json!(7_000));
        let mut derived = serde_json::to_value(&epoch).unwrap();
        let origin = derived
            .as_object_mut()
            .unwrap()
            .remove("pane_origin_ms")
            .unwrap();
        derived
            .as_object_mut()
            .unwrap()
            .insert("paneOriginMs".into(), origin);
        let decoded: AggregationConfig = serde_json::from_value(derived.clone()).unwrap();
        assert_eq!(decoded.pane_origin_ms, Some(7_000));

        let mut legacy = derived;
        legacy.as_object_mut().unwrap().remove("paneOriginMs");
        assert_eq!(
            serde_json::from_value::<AggregationConfig>(legacy)
                .expect("decode legacy wire")
                .pane_origin_ms,
            None
        );
    }

    /// The `policy_fp_u64()` accessor is exactly the fingerprint u64.
    #[test]
    fn policy_fp_u64_accessor_equals_fingerprint_u64() {
        let cfg =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
                .expect("parse");
        assert_eq!(cfg.policy_fp_u64(), cfg.policy_fingerprint().as_u64());
    }

    /// PR 5: `serialize_to_json` no longer emits `aggregationId`.
    #[test]
    fn serialize_to_json_omits_aggregation_id() {
        let cfg =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
                .expect("parse");
        let json = cfg.serialize_to_json();
        assert!(
            json.get("aggregationId").is_none(),
            "PR 5: aggregationId must not appear on the wire — readers derive it from content"
        );
    }

    #[test]
    fn typed_projection_roundtrips_and_legacy_column_keeps_identity() {
        use crate::sds::ValueProjectionIdentity;
        use planner_types::pre_asap::ScalarValue;
        let mut config =
            AggregationConfig::from_yaml_data(&sample_yaml(false), None, QueryLanguage::PromQl)
                .unwrap();
        config.table_name = Some("telemetry".into());
        config.value_projection = Some(ValueProjectionIdentity::Column {
            name: "value".into(),
        });
        let column_identity = config.policy_fingerprint();
        let mut legacy = serde_json::to_value(&config).unwrap();
        legacy.as_object_mut().unwrap().remove("value_projection");
        legacy["value_column"] = serde_json::json!("value");
        let decoded: AggregationConfig = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.policy_fingerprint(), column_identity);
        config.value_projection = Some(ValueProjectionIdentity::Constant {
            value: ScalarValue::Int64(1),
        });
        let mut wire = config.serialize_to_json();
        // The legacy JSON and YAML readers receive their labels from the
        // enclosing streaming config, in their respective wire shapes.
        wire["groupingLabels"] = config.grouping_labels.serialize_to_json();
        wire["aggregatedLabels"] = config.aggregated_labels.serialize_to_json();
        wire["rollupLabels"] = config.rollup_labels.serialize_to_json();
        wire["labels"] = serde_json::json!({
            "grouping": config.grouping_labels.serialize_to_json(),
            "aggregated": config.aggregated_labels.serialize_to_json(),
            "rollup": config.rollup_labels.serialize_to_json(),
        });
        assert!(wire.get("valueColumn").is_none());
        let json = AggregationConfig::deserialize_from_json(&wire).unwrap();
        let yaml = AggregationConfig::from_yaml_data(
            &serde_yaml::to_value(&wire).unwrap(),
            None,
            QueryLanguage::ClickHouseSql,
        )
        .unwrap();
        assert_eq!(
            json.effective_value_projection(),
            config.effective_value_projection()
        );
        assert_eq!(
            yaml.effective_value_projection(),
            config.effective_value_projection()
        );
        let mut conflicting = wire;
        conflicting["valueColumn"] = serde_json::json!("other_column");
        assert!(AggregationConfig::deserialize_from_json(&conflicting).is_err());
        assert!(AggregationConfig::from_yaml_data(
            &serde_yaml::to_value(conflicting).unwrap(),
            None,
            QueryLanguage::ClickHouseSql
        )
        .is_err());
        assert_ne!(config.policy_fingerprint(), column_identity);
        config.value_projection = Some(ValueProjectionIdentity::Constant {
            value: ScalarValue::Float64(f64::NAN),
        });
        assert!(config.population_filter_canonical().is_err());
    }
}
