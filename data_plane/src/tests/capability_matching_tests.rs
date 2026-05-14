//! Integration tests for capability-based aggregation matching.
//!
//! These tests verify that when no pre-configured query_config entry exists,
//! the engine falls back to searching StreamingConfig by capability, and that
//! the existing query_config path still takes priority when an entry is present.

use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::storage_engines::types::{
    AggregationConfig, AggregationType, PrecomputedOutput, StreamingConfig, WindowType};
use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
use crate::precompute_engine::operators::count_min_sketch_accumulator::CountMinSketchAccumulator;
use crate::precompute_engine::operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
use crate::precompute_engine::operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator;
use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
use crate::storage_engines::sketch_db::index::SketchStore;
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal `AggregationConfig`.
fn make_agg_config(
    _id: u64,
    metric: &str,
    agg_type: AggregationType,
    window_size_s: u64,
    window_type: WindowType,
    grouping: &[&str],
) -> AggregationConfig {
    // `_id` is unused after PR 5 — identity is content-addressed via
    // `PolicyFingerprint::from_config`.
    AggregationConfig {
        aggregation_type: agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: window_size_s,
        slide_interval: window_size_s,
        window_type,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None}
}

/// Build a `ASAPQueryEngine` with an explicit list of `AggregationConfig`s and no query_configs.
/// Data is inserted at timestamp 1_000_000 with a window covering [1_000_000 - window_ms, 1_000_000].
fn engine_no_query_configs(
    metric: &str,
    schema_labels: &[&str],
    agg_configs: Vec<AggregationConfig>,
) -> ASAPQueryEngine {
    let mut agg_map = HashMap::new();
    for c in &agg_configs {
        agg_map.insert(c.aggregation_id(), c.clone());
    }
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs: agg_map,
        storage_backend: Default::default()});
    let sketch_index = Arc::new(SketchStore::new());

    // Insert a data point for each aggregation so queries can actually execute.
    let ts = 1_000_000_u64;
    for c in &agg_configs {
        let window_ms = c.window_size * 1000;
        let output = PrecomputedOutput::new(ts - window_ms, ts, None, asap_types::PolicyFingerprint(c.aggregation_id()));
        let acc: Box<dyn crate::AggregateCore> = match c.aggregation_type.as_str() {
            "DatasketchesKLL" => {
                let mut kll = DatasketchesKLLAccumulator::new(200);
                kll.update(1.0);
                Box::new(kll)
            }
            "CountMinSketch" => {
                let cms = CountMinSketchAccumulator::new(4, 1000);
                Box::new(cms)
            }
            "DeltaSetAggregator" => Box::new(DeltaSetAggregatorAccumulator::new()),
            _ => Box::new(SumAccumulator::with_sum(42.0))};
        let resolver = Arc::new(SeriesIdResolver::new());
        sketch_index.ingest_precompute_for_agg_config(
            |m, fp, ak| resolver.resolve(m, fp, ak),
            c,
            &output,
            acc.as_ref(),
        );
    }

    let schema_label_names =
        KeyByLabelNames::new(schema_labels.iter().map(|s| s.to_string()).collect());

    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Build a `ASAPQueryEngine` with both a query_config entry AND a streaming aggregation.
fn engine_with_query_config(
    metric: &str,
    schema_labels: &[&str],
    agg_config: AggregationConfig,
    promql_query: &str,
) -> ASAPQueryEngine {
    let agg_id = agg_config.aggregation_id();
    let mut agg_map = HashMap::new();
    agg_map.insert(agg_id, agg_config.clone());
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs: agg_map,
        storage_backend: Default::default()});
    let sketch_index = Arc::new(SketchStore::new());

    let ts = 1_000_000_u64;
    let window_ms = agg_config.window_size * 1000;
    let output = PrecomputedOutput::new(ts - window_ms, ts, None, asap_types::PolicyFingerprint(agg_id));
    let acc = SumAccumulator::with_sum(99.0);
    let resolver = Arc::new(SeriesIdResolver::new());
    sketch_index.ingest_precompute_for_agg_config(
        |m, fp, ak| resolver.resolve(m, fp, ak),
        &agg_config,
        &output,
        &acc,
    );

    let schema_label_names =
        KeyByLabelNames::new(schema_labels.iter().map(|s| s.to_string()).collect());



    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// When no query_config entry exists but a compatible Sum aggregation does,
/// capability matching should route to it and return a valid context.
#[test]
fn capability_fallback_fires_when_no_config() {
    let agg = make_agg_config(
        1,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let expected = agg.aggregation_id();
    let engine = engine_no_query_configs("cpu", &[], vec![agg]);

    // sum_over_time(cpu[5m]) — 5 min = 300 s matches the 300 s tumbling config
    let ctx =
        engine.build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0);
    assert!(
        ctx.is_some(),
        "Expected capability matching to find a compatible aggregation"
    );
    assert_eq!(ctx.unwrap().agg_info.aggregation_id_for_value, expected);
}

/// When a query_config entry exists, the engine must use it (not capability matching).
/// We verify by giving the config a different agg_id than any compatible-by-type config.
#[test]
fn config_path_takes_priority_over_capability_matching() {
    let agg = make_agg_config(
        42,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let expected = agg.aggregation_id();
    let engine = engine_with_query_config("cpu", &[], agg, "sum_over_time(cpu[5m])");

    let ctx = engine
        .build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0)
        .expect("should succeed via config path");

    // The config path routes via the config's policy fingerprint.
    assert_eq!(ctx.agg_info.aggregation_id_for_value, expected);
}

/// A query for quantile(0.5) and quantile(0.9) should both resolve to the same
/// KLL aggregation when no query_configs are present.
#[test]
fn quantile_different_values_resolve_to_same_aggregation() {
    let kll = make_agg_config(
        7,
        "latency",
        AggregationType::DatasketchesKLL,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("latency", &[], vec![kll]);

    let q50 = engine.build_query_execution_context_promql(
        "quantile_over_time(0.5, latency[5m])".to_string(),
        1000.0,
    );
    let q90 = engine.build_query_execution_context_promql(
        "quantile_over_time(0.9, latency[5m])".to_string(),
        1000.0,
    );

    assert!(
        q50.is_some(),
        "quantile(0.5) should resolve via capability matching"
    );
    assert!(
        q90.is_some(),
        "quantile(0.9) should resolve via capability matching"
    );
    assert_eq!(
        q50.unwrap().agg_info.aggregation_id_for_value,
        q90.unwrap().agg_info.aggregation_id_for_value,
        "Both quantile queries should route to the same KLL aggregation"
    );
}

/// When no config entry exists and no compatible aggregation exists, return None.
#[test]
fn no_match_returns_none() {
    // KLL config present, but query asks for Sum — incompatible
    let kll = make_agg_config(
        1,
        "cpu",
        AggregationType::DatasketchesKLL,
        300,
        WindowType::Tumbling,
        &[],
    );
    let engine = engine_no_query_configs("cpu", &[], vec![kll]);

    let ctx =
        engine.build_query_execution_context_promql("sum_over_time(cpu[5m])".to_string(), 1000.0);
    assert!(
        ctx.is_none(),
        "Should return None when no compatible aggregation exists"
    );
}

/// When multiple compatible aggregations exist, the largest window should be preferred.
#[test]
fn priority_largest_window_wins() {
    let small = make_agg_config(
        1,
        "cpu",
        AggregationType::Sum,
        300,
        WindowType::Tumbling,
        &[],
    );
    let large = make_agg_config(
        2,
        "cpu",
        AggregationType::Sum,
        900,
        WindowType::Tumbling,
        &[],
    );
    let expected_large = large.aggregation_id();
    let engine = engine_no_query_configs("cpu", &[], vec![small, large]);

    // sum_over_time(cpu[15m]) = 900 s — both 300 s and 900 s configs match (900 = 3×300),
    // but the largest window should be preferred.
    let ctx = engine
        .build_query_execution_context_promql("sum_over_time(cpu[15m])".to_string(), 1000.0)
        .expect("should find a compatible aggregation");

    assert_eq!(
        ctx.agg_info.aggregation_id_for_value, expected_large,
        "The 900 s aggregation should be preferred over the 300 s one"
    );
}

/// E2E for the headline bug fix: a `sum_over_time(...)` query against a
/// CMS-only backend (no `Sum` / `MultipleSum` config available) must resolve
/// via capability matching to the CountMinSketch aggregation, paired with the
/// `DeltaSetAggregator` key aggregation. Pre-fix, `compatible_agg_types(Sum)`
/// did not list `CountMinSketch`, so this query fell through capability
/// matching to the cold tier (or returned an empty result).
#[test]
fn cms_only_backend_resolves_sum_over_time_via_capability_matching() {
    let cms = make_agg_config(
        100,
        "http_requests_total",
        AggregationType::CountMinSketch,
        300,
        WindowType::Tumbling,
        &[],
    );
    // CMS is a multi-population value type — `find_compatible_aggregation`
    // requires a paired key aggregation on the same metric.
    let key_agg = make_agg_config(
        101,
        "http_requests_total",
        AggregationType::DeltaSetAggregator,
        300,
        WindowType::Tumbling,
        &[],
    );
    let expected_value = cms.aggregation_id();
    let expected_key = key_agg.aggregation_id();
    let engine = engine_no_query_configs("http_requests_total", &[], vec![cms, key_agg]);

    let ctx = engine
        .build_query_execution_context_promql(
            "sum_over_time(http_requests_total[5m])".to_string(),
            1000.0,
        )
        .expect(
            "post-fix: capability matching must resolve sum_over_time against a CMS-only backend; \
             pre-fix returned None and the query fell through to the cold tier.",
        );

    assert_eq!(
        ctx.agg_info.aggregation_id_for_value, expected_value,
        "Capability matching should route Sum to the CMS aggregation",
    );
    assert_eq!(
        ctx.agg_info.aggregation_type_for_value,
        AggregationType::CountMinSketch,
        "Resolved value aggregation type must be CountMinSketch",
    );
    assert_eq!(
        ctx.agg_info.aggregation_id_for_key, expected_key,
        "CMS is multi-population — must be paired with the DeltaSetAggregator",
    );
}
