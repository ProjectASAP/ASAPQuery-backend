use serde::{Deserialize, Serialize};

use crate::enums::WindowKind;
use crate::policy_fingerprint::PolicyFingerprint;
use crate::AggregationType;

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
        if window_secs == 0 || slide_secs == 0 {
            return Err("window and slide must be positive".into());
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

/// Deployment metadata for one stored output: its identity, source binding,
/// window cadence and layout, retention and storage codec. What the output
/// computes (state family, update, input predicate and reduction) is defined
/// only by the Planner DAG node the enclosing PrecomputePlan binds to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrecomputeMaterialization {
    /// Deployment output allocation. Several outputs may share one semantic
    /// definition; see [`Self::allocate_stored_output_id`].
    pub stored_output_id: crate::sds::StoredOutputId,
    /// Planner-selected dependency closure ending at the persisted output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_fragment: Option<crate::semantic_fragment::SemanticFragment>,
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

    pub window_size: u64,        // Window size in seconds (e.g., 900s for 15m)
    pub slide_interval: u64,     // Slide/hop interval in seconds (e.g., 30s)
    pub window_type: WindowKind, // Tumbling or Sliding
    pub window_layout: WindowMaterializationLayout,
    /// Unix millisecond timestamp on the pane-boundary grid selected from
    /// the consuming query workload. An absent origin cannot authorize certified
    /// pane-only reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pane_origin_ms: Option<i64>,

    pub metric: String, // PromQL mode: metric name; SQL mode: derived from table_name.value_column
    pub num_aggregates_to_retain: Option<u64>,

    // SQL-specific fields (optional, used when query_language=sql)
    pub table_name: Option<String>, // SQL mode: table name
    #[serde(default)]
    pub value_projection: Option<crate::sds::ValueProjectionIdentity>,
    /// Table timestamp projection, in Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_timestamp_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_population: Option<crate::table_population::TablePopulation>,
    /// Producer typing for a SQL table value projection: the source column's
    /// declared type and nullability.
    ///
    /// **Why this is separate from [`Self::value_projection`]**: the
    /// projection is the materialization's *identity* — which column or
    /// constant is summarised. This is how the ingest path must *read* that
    /// column, which the identity does not determine: an integer column needs
    /// an exactness guard on its way into f64 summary state, and a nullable
    /// column needs SQL's "aggregates skip NULL" rule reproduced at the reader
    /// rather than a decode failure on the first NULL row.
    ///
    /// `None` ⇒ PromQL-mode materializations and legacy SQL definitions, which
    /// keep the pre-typed behaviour (read the column as it comes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_source_column: Option<planner_types::pre_asap::Column>,
}

/// Policy-match handles for both the key and value dimensions of a
/// query. For single-population queries, key and value share the same
/// fingerprint and type. For multi-population queries (e.g. Topk), they
/// differ.
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    /// Stored output id of the key aggregation.
    pub key_policy_fingerprint: u64,
    /// Stored output id of the value aggregation.
    pub value_policy_fingerprint: u64,
    pub aggregation_type_for_key: AggregationType,
    pub aggregation_type_for_value: AggregationType,
}

impl AggregationIdInfo {}

impl PrecomputeMaterialization {
    /// An unallocated output over `metric`; the caller must allocate its id
    /// with [`Self::allocate_stored_output_id`] once the deployment fields are final.
    pub fn new(
        metric: impl Into<String>,
        grouping_labels: impl Into<crate::GroupingProjection>,
        window_size: u64,
        slide_interval: u64,
        window_type: WindowKind,
    ) -> Self {
        Self {
            stored_output_id: crate::sds::StoredOutputId(0),
            semantic_fragment: None,
            grouping_labels: grouping_labels.into(),
            population_key_encoding: Default::default(),
            partitioning: None,
            derived_input: None,
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
            metric: metric.into(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_projection: None,
            table_timestamp_column: None,
            table_population: None,
            value_source_column: None,
        }
    }

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

    /// Validate the source binding. Returns the canonical typed table
    /// population, which is part of the table source binding; a time-series
    /// predicate belongs to the Planner DAG's scan.
    pub fn table_population_canonical(&self) -> Result<String, String> {
        if let Some(input) = &self.derived_input {
            input.validate()?;
            if self.table_name.is_some()
                || self.table_population.is_some()
                || self.table_timestamp_column.is_some()
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
        match &self.table_population {
            Some(population) => {
                if self.table_name.is_none() {
                    return Err("typed table population requires a table".into());
                }
                population.validate()?;
                Ok(population.canonical())
            }
            None => Ok(String::new()),
        }
    }

    /// Content-addressed identity for this config. Sugar over
    /// [`PolicyFingerprint::from_config`].
    pub fn policy_fingerprint(&self) -> PolicyFingerprint {
        PolicyFingerprint::from_config(self)
    }

    /// The stored output id as a raw u64.
    pub fn policy_fp_u64(&self) -> u64 {
        self.policy_fingerprint().as_u64()
    }

    /// Allocate the deterministic default id from the current deployment
    /// fields and `computation`, the producer's computation identity taken
    /// from the Planner DAG (state family, update, input predicate). Outputs
    /// with equal deployment fields and computation share one id; any
    /// difference, including pane layout or phase, yields a distinct output.
    pub fn allocate_stored_output_id(&mut self, computation: &impl Serialize) {
        let computation = crate::semantic_fragment::canonical_bytes(computation)
            .expect("computation identity serializes");
        let id = self.deployment_digest(&computation);
        // Zero is the unallocated sentinel.
        self.stored_output_id = crate::sds::StoredOutputId(id.max(1));
    }

    fn deployment_digest(&self, computation: &[u8]) -> u64 {
        use xxhash_rust::xxh64::xxh64;
        let mut buf: Vec<u8> = Vec::with_capacity(512);
        if !self.population_key_encoding.is_legacy() {
            // UTF-8 raw metrics cannot alias this nonlegacy domain prefix.
            buf.extend_from_slice(b"\xffpopulation-key-canonical-labels-v1\0");
        }
        if self.derived_input.is_some() {
            // Raw policies start with UTF-8 metric bytes; 0xff is impossible
            // there, so a metric cannot impersonate this source domain.
            buf.extend_from_slice(b"\xffderived-input-v1:");
            buf.extend_from_slice(
                &serde_json::to_vec(&self.source_identity()).expect("typed source identity"),
            );
        } else {
            buf.extend_from_slice(self.metric.as_bytes());
        }
        buf.push(0);
        buf.extend_from_slice(computation);
        buf.push(0);
        if let Some(partitioning) = self.partitioning {
            buf.extend_from_slice(format!("partition:{partitioning:?}\0").as_bytes());
        }
        for label in &self.grouping_labels.names() {
            buf.extend_from_slice(label.as_bytes());
            buf.push(b',');
        }
        buf.push(0);
        if self.table_name.is_some() && !self.grouping_labels.is_empty() {
            buf.extend_from_slice(
                crate::grouping_projection::TABLE_GROUP_OBSERVATION_SEMANTICS.as_bytes(),
            );
            buf.push(0);
        }
        if !self.grouping_labels.is_legacy_labels() {
            buf.extend_from_slice(b"typed-grouping:");
            buf.extend_from_slice(
                serde_json::to_string(&self.grouping_labels)
                    .expect("group projection serializes")
                    .as_bytes(),
            );
            buf.push(0);
        }
        buf.extend_from_slice(&self.window_size.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(&self.slide_interval.to_le_bytes());
        buf.push(0);
        buf.extend_from_slice(
            serde_json::to_string(&self.window_type)
                .unwrap_or_default()
                .as_bytes(),
        );
        buf.push(0);
        // A full overlapping window and a mergeable pane layout may share
        // semantic descriptors but never share payload instances or lifecycle
        // accounting.
        buf.extend_from_slice(
            serde_json::to_string(&self.window_layout)
                .unwrap_or_default()
                .as_bytes(),
        );
        buf.push(0);
        // Presence is explicit so an unknown phase cannot alias an
        // epoch-aligned definition.
        match self.pane_origin_ms {
            Some(origin) => {
                buf.push(1);
                buf.extend_from_slice(&origin.to_le_bytes());
            }
            None => buf.push(0),
        }
        buf.push(0);
        if let Some(table) = &self.table_name {
            buf.extend_from_slice(b"\0sql-source-v1\0");
            buf.extend_from_slice(table.as_bytes());
            buf.push(0);
            if let Some(column) = self.effective_value_projection().column() {
                buf.extend_from_slice(column.as_bytes());
            }
            if matches!(
                self.effective_value_projection(),
                crate::sds::ValueProjectionIdentity::Constant { .. }
            ) {
                buf.extend_from_slice(b"\0constant-projection-v1\0");
                buf.extend_from_slice(
                    serde_json::to_string(self.effective_value_projection())
                        .expect("finite validated projection serializes")
                        .as_bytes(),
                );
            }
        }
        if let Some(population) = &self.table_population {
            let canonical = population.canonical();
            if !canonical.is_empty() {
                buf.push(0);
                buf.extend_from_slice(canonical.as_bytes());
            }
        }
        if let Some(column) = &self.table_timestamp_column {
            buf.extend_from_slice(b"\0timestamp-ms\0");
            buf.extend_from_slice(column.as_bytes());
        }
        xxh64(&buf, 0)
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

    fn config() -> PrecomputeMaterialization {
        let mut config = PrecomputeMaterialization::new(
            "http_latency_ms",
            crate::KeyByLabelNames::new(vec!["zone".into()]),
            60,
            10,
            WindowKind::Sliding,
        );
        config.allocate_stored_output_id(&"ddsketch");
        config
    }

    // Window layout and pane origin survive the canonical JSON wire, and
    // derived-style spellings are rejected.
    #[test]
    fn window_layout_and_pane_origin_survive_canonical_json() {
        for layout in [
            WindowMaterializationLayout::FullWindow,
            WindowMaterializationLayout::Pane { pane_secs: 5 },
            WindowMaterializationLayout::HierarchicalRollup {
                base_pane_secs: 5,
                levels_secs: vec![10, 30],
            },
        ] {
            let mut config = config();
            config.window_layout = layout.clone();
            config.pane_origin_ms = Some(7_000);
            let mut wire = serde_json::to_value(&config).unwrap();
            let decoded: PrecomputeMaterialization = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(decoded.window_layout, layout);
            assert_eq!(decoded.pane_origin_ms, Some(7_000));
            assert_eq!(decoded.stored_window_ms(), config.stored_window_ms());
            assert_eq!(decoded.policy_fingerprint(), config.policy_fingerprint());
            wire["windowLayout"] = wire["window_layout"].clone();
            assert!(serde_json::from_value::<PrecomputeMaterialization>(wire).is_err());
        }
    }

    // The removed externally assigned identity is not accepted on the wire.
    #[test]
    fn canonical_wire_rejects_aggregation_id() {
        let mut json = serde_json::to_value(config()).unwrap();
        assert!(json.get("aggregationId").is_none());
        json["aggregationId"] = serde_json::json!(42);
        assert!(serde_json::from_value::<PrecomputeMaterialization>(json).is_err());
    }

    // Table value projections are typed, and a non-finite constant is invalid.
    #[test]
    fn typed_projection_roundtrips_and_untyped_column_is_rejected() {
        use crate::sds::ValueProjectionIdentity;
        use planner_types::pre_asap::ScalarValue;
        let mut config = config();
        config.table_name = Some("telemetry".into());
        config.value_projection = Some(ValueProjectionIdentity::Column {
            name: "value".into(),
        });
        let mut legacy = serde_json::to_value(&config).unwrap();
        legacy.as_object_mut().unwrap().remove("value_projection");
        legacy["value_column"] = serde_json::json!("value");
        assert!(serde_json::from_value::<PrecomputeMaterialization>(legacy).is_err());
        let mut untyped = serde_json::to_value(&config).unwrap();
        untyped["value_projection"] = serde_json::json!("value");
        assert!(serde_json::from_value::<PrecomputeMaterialization>(untyped).is_err());
        config.value_projection = Some(ValueProjectionIdentity::Constant {
            value: ScalarValue::Int64(1),
        });
        let decoded: PrecomputeMaterialization =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(
            decoded.effective_value_projection(),
            config.effective_value_projection()
        );
        assert!(config.table_population_canonical().is_ok());
        config.value_projection = Some(ValueProjectionIdentity::Constant {
            value: ScalarValue::Float64(f64::NAN),
        });
        assert!(config.table_population_canonical().is_err());
    }
}
