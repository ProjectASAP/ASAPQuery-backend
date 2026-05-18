//! Convert a [`CollectionPlan`] into the YAML shape ASAPQuery-backend's
//! `POST /api/v1/streaming-config` endpoint accepts (the same format its
//! `StreamingConfig::from_yaml_data` parser consumes at startup).
//!
//! This is **separate** from `config::backend` (which produces OTel YAML
//! for a backend OTel collector running merge processors). The two
//! consumers are different:
//!
//!   * `config::backend` — OTel collector, expects
//!     `processors: { ddsketch_merge: {...} }` + `service.pipelines`.
//!   * `config::asapquery_backend` (this module) — ASAPQuery-backend
//!     query engine, expects
//!     `aggregations: [{ aggregationType, metric, labels, parameters,
//!                       windowSize, windowType, spatialFilter }]`.
//!
//! Both are generated from the same `CollectionPlan` fields but target
//! different services. The replanner pushes the OTel YAML via OpAMP to
//! backend-role collectors and pushes this one via HTTP to the
//! ASAPQuery-backend's `/api/v1/streaming-config` endpoint.
//!
//! Phase 5 M2.2: this emitter no longer writes `aggregationId`. The
//! backend's `AggregationConfig::from_yaml_data` derives one
//! deterministically via `compute_agg_config_id` from the same set of
//! fields we emit, so the explicit field is redundant.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::types::{AgentCollectorConfig, CollectionPlan, SketchType};

/// Generate the `StreamingConfig` YAML for the ASAPQuery-backend from a
/// single-metric `CollectionPlan`. Produces a one-element `aggregations`
/// list — the backend's endpoint will merge this into its active config
/// (add on conflict, replace on same id).
///
/// # Errors
///
/// Returns an error if the plan is missing a window (the backend's
/// config parser rejects zero-window aggregations) or if YAML
/// serialization fails.
pub fn generate_streaming_config_yaml(metric: &str, plan: &CollectionPlan) -> Result<String> {
    let agg = &plan.agent_config;
    // MVP blocker B4: clamp the workload's `window_duration` to
    // `[MIN_WINDOW_SECS, MAX_WINDOW_SECS]` so the legacy YAML emit
    // matches the typed L5 JSON emit's `windowSize` clamp. Without
    // this, the legacy and typed paths can disagree (e.g. typed
    // clamps `[5m]` → 60, legacy passes 300 → backend reducer keys
    // a 300s window the agent never closes).
    let window_secs = agg
        .window_duration
        .map(|d: Duration| {
            super::stage_config::clamp_window_secs(Some(d.as_secs()))
                .expect("clamp preserves Some")
        })
        .unwrap_or(0);
    if window_secs == 0 {
        anyhow::bail!(
            "generate_streaming_config_yaml: plan for metric {metric} has \
             no window_duration; ASAPQuery-backend rejects zero-window aggregations"
        );
    }

    // Parameters map: copy sketch-type-specific params (K for KLL,
    // epsilon/delta for CountMin, etc.) into the string-keyed YAML map
    // the backend expects. We serialize via serde_yaml to pick up
    // SketchParams' own Serialize impl and then re-parse into a
    // generic Mapping so we can embed it.
    let params_yaml = serde_yaml::to_value(&agg.sketch_params)
        .context("serialize SketchParams for ASAPQuery streaming config")?;

    let agg_type_str = map_sketch_type_to_agg_type(&agg.sketch_type);

    let aggregation = serde_yaml::Mapping::from_iter([
        (
            serde_yaml::Value::from("aggregationType"),
            serde_yaml::Value::from(agg_type_str),
        ),
        (
            serde_yaml::Value::from("aggregationSubType"),
            serde_yaml::Value::from(""),
        ),
        (
            serde_yaml::Value::from("metric"),
            serde_yaml::Value::from(metric),
        ),
        (
            serde_yaml::Value::from("labels"),
            labels_mapping(&agg.aggregate_by),
        ),
        (serde_yaml::Value::from("parameters"), params_yaml),
        (
            serde_yaml::Value::from("windowSize"),
            serde_yaml::Value::from(window_secs),
        ),
        (
            serde_yaml::Value::from("windowType"),
            serde_yaml::Value::from("tumbling"),
        ),
        (
            serde_yaml::Value::from("spatialFilter"),
            serde_yaml::Value::from(normalize_spatial_filter(&agg.label_matchers)),
        ),
    ]);

    let top = serde_yaml::Mapping::from_iter([(
        serde_yaml::Value::from("aggregations"),
        serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(aggregation)]),
    )]);

    serde_yaml::to_string(&serde_yaml::Value::Mapping(top))
        .context("serialize ASAPQuery streaming-config YAML")
}

/// Map the control plane's `SketchType` to the backend's
/// `AggregationType::Display` string. These strings must match what the
/// backend's `FromStr for AggregationType` in
/// `promql_utilities::query_logics::enums` accepts — hence the variant
/// names rather than the collector factory names (e.g. `"DatasketchesKLL"`
/// not `"KLL"`).
fn map_sketch_type_to_agg_type(t: &SketchType) -> &'static str {
    match t {
        SketchType::DDSketch => "DDSketch",
        SketchType::KLL => "DatasketchesKLL",
        SketchType::HLL => "HLL",
        SketchType::CountSketch => "CountSketch",
        SketchType::CountMinSketch => "CountMinSketch",
    }
}

/// Build the `labels` sub-mapping the backend expects. All three lists
/// exist because the backend's parser reads them separately for
/// key-value / spatial-rollup distinction; today the control plane only
/// tracks `aggregate_by` (grouping), so rollup and aggregated stay
/// empty and are populated in a follow-up when the cost model starts
/// producing richer label metadata.
fn labels_mapping(aggregate_by: &[String]) -> serde_yaml::Value {
    serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([
        (
            serde_yaml::Value::from("grouping"),
            serde_yaml::Value::Sequence(
                aggregate_by
                    .iter()
                    .cloned()
                    .map(serde_yaml::Value::from)
                    .collect(),
            ),
        ),
        (
            serde_yaml::Value::from("rollup"),
            serde_yaml::Value::Sequence(vec![]),
        ),
        (
            serde_yaml::Value::from("aggregated"),
            serde_yaml::Value::Sequence(vec![]),
        ),
    ]))
}

/// Join the controller's `label_matchers` list (each shaped like
/// `"key=value"`) into a single comma-separated string the backend's
/// spatial-filter parser accepts. When the list is empty, returns an
/// empty string (the backend treats that as "no spatial filter").
fn normalize_spatial_filter(label_matchers: &[String]) -> String {
    label_matchers.join(",")
}

// ─── Unused-warning suppression for types that are referenced only
// inside the unit tests below. This keeps the module self-contained
// even when the rest of the controller crate's cfg(test) surface grows.
#[allow(dead_code)]
fn _type_check(_: &AgentCollectorConfig) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AgentDataSink, CollectionPlan, DeltaDecision, GatewayCollectorConfig, OutputMode,
        ProcessorMode, SketchParams, TransmissionCostSummary,
    };
    use std::time::Duration;

    fn dummy_plan(sketch_type: SketchType) -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: sketch_type.clone(),
                sketch_params: SketchParams::default(),
                aggregate_by: vec!["host".to_string(), "service".to_string()],
                label_matchers: vec!["env=prod".to_string()],
                window_duration: Some(Duration::from_secs(30)),
                mode: ProcessorMode::Window,
                enable_self_monitoring: false,
                transmit_sketch: true,
                drop_original: true,
                enable_series_id: false,
                series_id_ttl_secs: 0,
                delta_transmission: false,
                delta_threshold: 0.0,
                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
            valid_until: chrono::Utc::now() + chrono::Duration::seconds(300),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    #[test]
    fn emitted_yaml_omits_aggregation_id() {
        let plan = dummy_plan(SketchType::DDSketch);
        let yaml = generate_streaming_config_yaml("cpu_usage", &plan).expect("yaml ok");
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("re-parse ok");
        let a = &parsed["aggregations"][0];
        assert!(
            a["aggregationId"].is_null(),
            "controller must not emit aggregationId — backend derives it from content (M2.2)"
        );
    }

    #[test]
    fn yaml_round_trips_through_serde_yaml() {
        let plan = dummy_plan(SketchType::DDSketch);
        let yaml = generate_streaming_config_yaml("cpu_usage", &plan).expect("yaml ok");
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("re-parse ok");

        let aggs = parsed["aggregations"].as_sequence().expect("sequence");
        assert_eq!(aggs.len(), 1);
        let a = &aggs[0];
        assert_eq!(a["aggregationType"], serde_yaml::Value::from("DDSketch"));
        assert_eq!(a["metric"], serde_yaml::Value::from("cpu_usage"));
        assert_eq!(a["windowSize"], serde_yaml::Value::from(30u64));
        assert_eq!(a["windowType"], serde_yaml::Value::from("tumbling"));
        assert_eq!(a["spatialFilter"], serde_yaml::Value::from("env=prod"));

        let grouping = a["labels"]["grouping"].as_sequence().expect("grouping seq");
        let grouping: Vec<&str> = grouping.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(grouping, vec!["host", "service"]);
    }

    #[test]
    fn maps_all_sketch_types() {
        assert_eq!(
            map_sketch_type_to_agg_type(&SketchType::DDSketch),
            "DDSketch"
        );
        assert_eq!(
            map_sketch_type_to_agg_type(&SketchType::KLL),
            "DatasketchesKLL",
            "KLL must map to the backend's enum variant name, not the factory name"
        );
        assert_eq!(map_sketch_type_to_agg_type(&SketchType::HLL), "HLL");
        assert_eq!(
            map_sketch_type_to_agg_type(&SketchType::CountSketch),
            "CountSketch"
        );
        assert_eq!(
            map_sketch_type_to_agg_type(&SketchType::CountMinSketch),
            "CountMinSketch"
        );
    }

    #[test]
    fn rejects_plan_without_window_duration() {
        let mut plan = dummy_plan(SketchType::HLL);
        plan.agent_config.window_duration = None;
        let err = generate_streaming_config_yaml("m", &plan).expect_err("should error");
        assert!(
            err.to_string().contains("window_duration"),
            "error should mention window_duration: {err}"
        );
    }

    #[test]
    fn spatial_filter_joins_label_matchers() {
        let mut plan = dummy_plan(SketchType::DDSketch);
        plan.agent_config.label_matchers =
            vec!["env=prod".to_string(), "region=us-east".to_string()];
        let yaml = generate_streaming_config_yaml("m", &plan).expect("ok");
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(
            parsed["aggregations"][0]["spatialFilter"],
            serde_yaml::Value::from("env=prod,region=us-east")
        );
    }
}
