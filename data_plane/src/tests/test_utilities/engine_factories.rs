//! Engine factory helpers for integration tests
//!
//! Provides reusable construction helpers for ASAPQueryEngine + SketchStore
//! populated with various accumulator types. Unlike TestConfigBuilder which
//! hardcodes "SumAccumulator", these helpers build AggregationConfig with
//! the correct aggregation_type string.

use crate::stores::types::{
    AggregationConfig, AggregationType, KeyByLabelValues, PrecomputedOutput, QueryLanguage,
    StreamingConfig, WindowType};
use crate::query_engines::query_result::InstantVectorElement;
use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
use crate::AggregateCore;
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::HashMap;
use std::sync::Arc;

/// Data to insert into a store: (label_values, accumulator)
pub type AccumulatorData = Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>;

/// Creates a ASAPQueryEngine with a single aggregation populated with given data.
///
/// # Arguments
/// * `metric` - Metric name
/// * `aggregation_type` - Accumulator type string (e.g. "SumAccumulator", "DatasketchesKLLAccumulator")
/// * `grouping_labels` - Label names for GROUP BY
/// * `data` - Vec of (label_values, accumulator) pairs to insert
/// * `promql_query` - The PromQL query string
pub fn create_engine_single_pop(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    data: AccumulatorData,
    promql_query: &str,
) -> ASAPQueryEngine {
    create_engine_single_pop_with_aggregated(
        metric,
        aggregation_type,
        grouping_labels,
        vec![],
        data,
        promql_query,
    )
}

/// Creates a ASAPQueryEngine with aggregated labels (sub-key labels within the accumulator).
///
/// Use for self-keyed multi-population accumulators (Multiple* types) where
/// `aggregated_labels` are the labels that key the accumulator internally
/// (e.g. "endpoint" within a MultipleIncrease accumulator).
pub fn create_engine_single_pop_with_aggregated(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    aggregated_labels: Vec<&str>,
    data: AccumulatorData,
    promql_query: &str,
) -> ASAPQueryEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();
    let aggregated_label_strings: Vec<String> =
        aggregated_labels.iter().map(|s| s.to_string()).collect();
    let all_schema_labels: Vec<String> = grouping_label_strings
        .iter()
        .chain(aggregated_label_strings.iter())
        .cloned()
        .collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());

    // Insert data into SketchIndex via the canonical helper (M2.3.6e).
    let agg_cfg = streaming_config
        .get_aggregation_config(1)
        .cloned()
        .expect("agg_id=1 must be in streaming_config");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 1);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg, &output, acc.as_ref());
    }

    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a ASAPQueryEngine with dual-input (separate value and keys aggregations).
///
/// # Arguments
/// * `metric` - Metric name
/// * `value_agg_type` - Accumulator type for values (e.g. "HydraKllSketchAccumulator")
/// * `key_agg_type` - Accumulator type for keys (e.g. "DeltaSetAggregator")
/// * `grouping_labels` - Store GROUP BY columns
/// * `aggregated_labels` - Labels that key the accumulator internally (tracked by DeltaSet)
/// * `value_data` - Data for value aggregation (agg_id=1)
/// * `keys_data` - Data for keys aggregation (agg_id=2)
/// * `promql_query` - The PromQL query string
#[allow(clippy::too_many_arguments)]
pub fn create_engine_dual_input(
    metric: &str,
    value_agg_type: AggregationType,
    key_agg_type: AggregationType,
    grouping_labels: Vec<&str>,
    aggregated_labels: Vec<&str>,
    value_data: AccumulatorData,
    keys_data: AccumulatorData,
    promql_query: &str,
) -> ASAPQueryEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();
    let aggregated_label_strings: Vec<String> =
        aggregated_labels.iter().map(|s| s.to_string()).collect();
    let all_labels: Vec<String> = grouping_label_strings
        .iter()
        .chain(aggregated_label_strings.iter())
        .cloned()
        .collect();

    let mut aggregation_configs = HashMap::new();

    // Value aggregation (id=1)
    let value_agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: value_agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(1u64, value_agg_config);

    // Keys aggregation (id=2)
    let keys_agg_config = AggregationConfig {
        aggregation_id: 2,
        aggregation_type: key_agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(2u64, keys_agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());

    let agg_cfg_1 = streaming_config
        .get_aggregation_config(1)
        .cloned()
        .expect("agg_id=1");
    let agg_cfg_2 = streaming_config
        .get_aggregation_config(2)
        .cloned()
        .expect("agg_id=2");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in value_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 1);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg_1, &output, acc.as_ref());
    }
    for (label_values_opt, acc) in keys_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 2);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg_2, &output, acc.as_ref());
    }

    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a ASAPQueryEngine with two independent metrics, each with their own
/// aggregation config and query_config.
///
/// agg_id=1 → metric_a, agg_id=2 → metric_b.
/// Both are registered as separate query_configs in the inference config.
#[allow(clippy::too_many_arguments)]
pub fn create_engine_two_metrics(
    metric_a: &str,
    aggregation_type_a: AggregationType,
    grouping_labels_a: Vec<&str>,
    data_a: AccumulatorData,
    query_a: &str,
    metric_b: &str,
    aggregation_type_b: AggregationType,
    grouping_labels_b: Vec<&str>,
    data_b: AccumulatorData,
    query_b: &str,
) -> ASAPQueryEngine {
    let labels_a: Vec<String> = grouping_labels_a.iter().map(|s| s.to_string()).collect();
    let labels_b: Vec<String> = grouping_labels_b.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();

    let agg_config_a = AggregationConfig {
        aggregation_id: 1,
        aggregation_type: aggregation_type_a,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(labels_a.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric_a.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(1u64, agg_config_a);

    let agg_config_b = AggregationConfig {
        aggregation_id: 2,
        aggregation_type: aggregation_type_b,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(labels_b.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric_b.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(2u64, agg_config_b);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());
    let agg_cfg_1 = streaming_config.get_aggregation_config(1).cloned().expect("agg 1");
    let agg_cfg_2 = streaming_config.get_aggregation_config(2).cloned().expect("agg 2");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in data_a {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 1);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg_1, &output, acc.as_ref());
    }
    for (label_values_opt, acc) in data_b {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp, timestamp, key, 2);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg_2, &output, acc.as_ref());
    }
    let _ = (query_a, query_b);
    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a ASAPQueryEngine with three independent metrics, each with their own
/// aggregation config and query_config.
///
/// agg_id=1 → metric_a, agg_id=2 → metric_b, agg_id=3 → metric_c.
#[allow(clippy::too_many_arguments)]
pub fn create_engine_three_metrics(
    metric_a: &str,
    aggregation_type_a: AggregationType,
    grouping_labels_a: Vec<&str>,
    data_a: AccumulatorData,
    query_a: &str,
    metric_b: &str,
    aggregation_type_b: AggregationType,
    grouping_labels_b: Vec<&str>,
    data_b: AccumulatorData,
    query_b: &str,
    metric_c: &str,
    aggregation_type_c: AggregationType,
    grouping_labels_c: Vec<&str>,
    data_c: AccumulatorData,
    query_c: &str,
) -> ASAPQueryEngine {
    let labels_a: Vec<String> = grouping_labels_a.iter().map(|s| s.to_string()).collect();
    let labels_b: Vec<String> = grouping_labels_b.iter().map(|s| s.to_string()).collect();
    let labels_c: Vec<String> = grouping_labels_c.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();

    for (id, agg_type, labels, metric) in [
        (1u64, aggregation_type_a, &labels_a, metric_a),
        (2u64, aggregation_type_b, &labels_b, metric_b),
        (3u64, aggregation_type_c, &labels_c, metric_c),
    ] {
        aggregation_configs.insert(
            id,
            AggregationConfig {
                aggregation_id: id,
                aggregation_type: agg_type,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(labels.clone()),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size: 1,
                slide_interval: 1,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: metric.to_string(),
                num_aggregates_to_retain: None,
                table_name: None,
                value_column: None},
        );
    }

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());
    let agg_cfgs: Vec<_> = (1..=3)
        .map(|id| streaming_config.get_aggregation_config(id).cloned().expect("agg present"))
        .collect();
    let timestamp = 1_000_000_u64;
    for (idx, data) in [(0, data_a), (1, data_b), (2, data_c)] {
        let agg_cfg = &agg_cfgs[idx];
        let agg_id = (idx as u64) + 1;
        for (label_values_opt, acc) in data {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            let output = PrecomputedOutput::new(timestamp, timestamp, key, agg_id);
            sketch_index.ingest_precompute_for_agg_config(agg_cfg, &output, acc.as_ref());
        }
    }

    let _ = (labels_a, labels_b, labels_c, query_a, query_b, query_c);
    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a single-pop engine with data at multiple timestamps for testing merge.
#[allow(clippy::type_complexity)]
pub fn create_engine_multi_timestamp(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    data: Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
    promql_query: &str,
) -> ASAPQueryEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowType::Tumbling,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());
    let agg_cfg = streaming_config.get_aggregation_config(1).cloned().expect("agg 1");
    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp - 1000, timestamp, key, 1);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg, &output, acc.as_ref());
    }
    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a single-pop engine with data at multiple timestamps and configurable window.
///
/// Like `create_engine_multi_timestamp` but allows setting `window_size` and `window_type`
/// on the AggregationConfig (needed for temporal queries like `sum_over_time(metric[5s])`).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub fn create_engine_multi_timestamp_with_window(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    data: Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
    promql_query: &str,
    window_size: u64,
    window_type: WindowType,
) -> ASAPQueryEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
        aggregation_id: 1,
        aggregation_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size,
        slide_interval: 1,
        window_type,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None};
    aggregation_configs.insert(1u64, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default()});

    let sketch_index = std::sync::Arc::new(crate::stores::sketch_db::index::SketchIndex::new());
    let agg_cfg = streaming_config.get_aggregation_config(1).cloned().expect("agg 1");
    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(timestamp - 1000, timestamp, key, 1);
        sketch_index.ingest_precompute_for_agg_config(&agg_cfg, &output, acc.as_ref());
    }
    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}
