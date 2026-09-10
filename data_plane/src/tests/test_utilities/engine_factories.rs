//! Engine factory helpers for integration tests
//!
//! Provides reusable construction helpers for ASAPQueryEngine + SketchStore
//! populated with various accumulator types. Unlike TestConfigBuilder which
//! hardcodes "SumAccumulator", these helpers build AggregationConfig with
//! the correct aggregation_type string.

use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
use crate::query_engines::query_result::InstantVectorElement;
use crate::storage_engines::types::{
    AggregationConfig, AggregationType, KeyByLabelValues, PrecomputedOutput, QueryLanguage,
    StreamingConfig, WindowKind,
};
use crate::AggregateCore;
use asap_types::KeyByLabelNames;
use std::collections::HashMap;

/// Helper for test factories — wraps the closure-mint call with a
/// fresh resolver and forwards to `SketchStore::ingest_precompute_for_agg_config`.
/// Each factory gets its own resolver instance; tests are isolated so
/// the `next_sid = 1, 2, ...` counter doesn't bleed between fixtures.
fn ingest_with_fresh_resolver(
    sketch_index: &crate::storage_engines::sketch_db::index::SketchStore,
    resolver: &std::sync::Arc<SeriesIdResolver>,
    agg_cfg: &AggregationConfig,
    output: &PrecomputedOutput,
    accumulator: &dyn AggregateCore,
) -> Option<u64> {
    let resolver = resolver.clone();
    sketch_index.ingest_precompute_for_agg_config(
        |m, fp, ak| resolver.resolve(m, fp, ak),
        agg_cfg,
        output,
        accumulator,
    )
}
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
        aggregation_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let agg_id = agg_config.policy_fp_u64();
    aggregation_configs.insert(agg_id, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());

    // Insert data into SketchStore via the canonical helper (M2.3.6e).
    let agg_cfg = streaming_config
        .get_aggregation_config(agg_id)
        .cloned()
        .expect("agg config must be in streaming_config");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp,
            timestamp,
            key,
            asap_types::PolicyFingerprint(agg_id),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg, &output, acc.as_ref());
    }

    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}

/// Creates a ASAPQueryEngine with dual-input (separate value and keys
/// aggregations). Currently dead code — the only historical caller
/// exercised the retired `SetAggregator` / `DeltaSetAggregator`
/// key-tracking pair. Retained as a builder utility for future
/// dual-input shapes (e.g. HydraKLL value + a not-yet-defined key
/// aggregation); delete if no caller materialises.
///
/// # Arguments
/// * `metric` - Metric name
/// * `value_agg_type` - Accumulator type for values
/// * `key_agg_type` - Accumulator type for keys
/// * `grouping_labels` - Store GROUP BY columns
/// * `aggregated_labels` - Labels that key the accumulator internally
/// * `value_data` - Data for value aggregation (agg_id=1)
/// * `keys_data` - Data for keys aggregation (agg_id=2)
/// * `promql_query` - The PromQL query string
#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
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

    // Value aggregation
    let value_agg_config = AggregationConfig {
        aggregation_type: value_agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let value_id = value_agg_config.policy_fp_u64();
    aggregation_configs.insert(value_id, value_agg_config);

    // Keys aggregation
    let keys_agg_config = AggregationConfig {
        aggregation_type: key_agg_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let keys_id = keys_agg_config.policy_fp_u64();
    aggregation_configs.insert(keys_id, keys_agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());

    let agg_cfg_1 = streaming_config
        .get_aggregation_config(value_id)
        .cloned()
        .expect("value agg config");
    let agg_cfg_2 = streaming_config
        .get_aggregation_config(keys_id)
        .cloned()
        .expect("keys agg config");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in value_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp,
            timestamp,
            key,
            asap_types::PolicyFingerprint(value_id),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg_1, &output, acc.as_ref());
    }
    for (label_values_opt, acc) in keys_data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp,
            timestamp,
            key,
            asap_types::PolicyFingerprint(keys_id),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg_2, &output, acc.as_ref());
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
        aggregation_type: aggregation_type_a,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(labels_a.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric_a.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let id_a = agg_config_a.policy_fp_u64();
    aggregation_configs.insert(id_a, agg_config_a);

    let agg_config_b = AggregationConfig {
        aggregation_type: aggregation_type_b,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(labels_b.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric_b.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let id_b = agg_config_b.policy_fp_u64();
    aggregation_configs.insert(id_b, agg_config_b);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());
    let agg_cfg_1 = streaming_config
        .get_aggregation_config(id_a)
        .cloned()
        .expect("agg a");
    let agg_cfg_2 = streaming_config
        .get_aggregation_config(id_b)
        .cloned()
        .expect("agg b");
    let timestamp = 1_000_000_u64;
    for (label_values_opt, acc) in data_a {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp,
            timestamp,
            key,
            asap_types::PolicyFingerprint(id_a),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg_1, &output, acc.as_ref());
    }
    for (label_values_opt, acc) in data_b {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp,
            timestamp,
            key,
            asap_types::PolicyFingerprint(id_b),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg_2, &output, acc.as_ref());
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
    let mut ids: Vec<u64> = Vec::new();

    for (agg_type, labels, metric) in [
        (aggregation_type_a, &labels_a, metric_a),
        (aggregation_type_b, &labels_b, metric_b),
        (aggregation_type_c, &labels_c, metric_c),
    ] {
        let cfg = AggregationConfig {
            aggregation_type: agg_type,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(labels.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowKind::Tumbling,
            window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
            pane_origin_ms: None,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_column: None,
        };
        let id = cfg.policy_fp_u64();
        ids.push(id);
        aggregation_configs.insert(id, cfg);
    }

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());
    let agg_cfgs: Vec<_> = ids
        .iter()
        .map(|id| {
            streaming_config
                .get_aggregation_config(*id)
                .cloned()
                .expect("agg present")
        })
        .collect();
    let timestamp = 1_000_000_u64;
    for (idx, data) in [(0, data_a), (1, data_b), (2, data_c)] {
        let agg_cfg = &agg_cfgs[idx];
        let agg_id = ids[idx];
        for (label_values_opt, acc) in data {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            let output = PrecomputedOutput::new(
                timestamp,
                timestamp,
                key,
                asap_types::PolicyFingerprint(agg_id),
            );
            ingest_with_fresh_resolver(&sketch_index, &resolver, agg_cfg, &output, acc.as_ref());
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
        aggregation_type,
        aggregation_sub_type: String::new(),
        parameters: HashMap::new(),
        grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
        aggregated_labels: KeyByLabelNames::empty(),
        rollup_labels: KeyByLabelNames::empty(),
        original_yaml: String::new(),
        window_size: 1,
        slide_interval: 1,
        window_type: WindowKind::Tumbling,
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let agg_id = agg_config.policy_fp_u64();
    aggregation_configs.insert(agg_id, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());
    let agg_cfg = streaming_config
        .get_aggregation_config(agg_id)
        .cloned()
        .expect("agg");
    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp - 1000,
            timestamp,
            key,
            asap_types::PolicyFingerprint(agg_id),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg, &output, acc.as_ref());
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
    window_type: WindowKind,
) -> ASAPQueryEngine {
    let grouping_label_strings: Vec<String> =
        grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    let agg_config = AggregationConfig {
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
        window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
        pane_origin_ms: None,
        spatial_filter: String::new(),
        spatial_filter_normalized: String::new(),
        metric: metric.to_string(),
        num_aggregates_to_retain: None,
        table_name: None,
        value_column: None,
    };
    let agg_id = agg_config.policy_fp_u64();
    aggregation_configs.insert(agg_id, agg_config);

    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
        monitors: Vec::new(),
    });

    let sketch_index =
        std::sync::Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new());
    let resolver = std::sync::Arc::new(SeriesIdResolver::new());
    let agg_cfg = streaming_config
        .get_aggregation_config(agg_id)
        .cloned()
        .expect("agg");
    for (timestamp, label_values_opt, acc) in data {
        let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp - 1000,
            timestamp,
            key,
            asap_types::PolicyFingerprint(agg_id),
        );
        ingest_with_fresh_resolver(&sketch_index, &resolver, &agg_cfg, &output, acc.as_ref());
    }
    ASAPQueryEngine::new(streaming_config, 1).with_sketch_index(sketch_index)
}
