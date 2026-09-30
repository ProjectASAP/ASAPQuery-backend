//! Engine factory helpers for integration tests
//!
//! Provides reusable construction helpers for ASAPQueryEngine + SketchStore
//! populated with stored states of a given kernel family.

use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::types::{
    AggregationType, KeyByLabelValues, PrecomputeMaterialization, PrecomputedOutput, WindowKind,
};
use crate::AggregateCore;
use std::sync::Arc;

/// Data to insert into a store: (label_values, accumulator)
pub type AccumulatorData = Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>;

/// One stored output of `kind` state over `metric`, with its storage kind.
fn stored_output(
    metric: &str,
    kind: AggregationType,
    grouping_labels: Vec<&str>,
    window_size: u64,
    window_type: WindowKind,
) -> (
    PrecomputeMaterialization,
    crate::storage_engines::sketch_db::index::AggKind,
) {
    let family = super::outputs::family(kind, &serde_json::json!({}));
    let mut config = PrecomputeMaterialization::new(
        metric,
        asap_types::KeyByLabelNames::new(grouping_labels.into_iter().map(str::to_owned).collect()),
        window_size,
        1,
        window_type,
    );
    config.window_layout = asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 };
    config.allocate_stored_output_id(&family);
    let agg_kind = crate::storage_engines::sketch_db::data::agg_kind_for_family(&family, "");
    (config, agg_kind)
}

/// Ingest `(end_timestamp, labels, state)` rows of one output, each covering
/// `[end - span, end]`, through a fresh resolver.
fn ingest(
    store: &SketchStore,
    output: &(
        PrecomputeMaterialization,
        crate::storage_engines::sketch_db::index::AggKind,
    ),
    span: u64,
    rows: impl IntoIterator<Item = (u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
) {
    let resolver = Arc::new(SeriesIdResolver::new());
    let (config, kind) = output;
    for (timestamp, labels, state) in rows {
        let key = labels.map(|labels| KeyByLabelValues { labels });
        let output = PrecomputedOutput::new(
            timestamp - span,
            timestamp,
            key,
            config.policy_fingerprint(),
        );
        store.ingest_precompute_for_agg_config(
            |m, fp, ak| resolver.resolve(m, fp, ak),
            config,
            kind,
            &output,
            state.as_ref(),
        );
    }
}

fn at_one_second(data: AccumulatorData) -> Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)> {
    data.into_iter()
        .map(|(labels, state)| (1_000_000, labels, state))
        .collect()
}

/// Creates a ASAPQueryEngine with a single aggregation populated with given data.
pub fn create_engine_single_pop(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    data: AccumulatorData,
    _promql_query: &str,
) -> ASAPQueryEngine {
    let store = Arc::new(SketchStore::new());
    let output = stored_output(
        metric,
        aggregation_type,
        grouping_labels,
        1,
        WindowKind::Tumbling,
    );
    ingest(&store, &output, 0, at_one_second(data));
    ASAPQueryEngine::new(1).with_sketch_index(store)
}

/// Creates a ASAPQueryEngine with two independent metrics, each with their own
/// stored output.
#[allow(clippy::too_many_arguments)]
pub fn create_engine_two_metrics(
    metric_a: &str,
    aggregation_type_a: AggregationType,
    grouping_labels_a: Vec<&str>,
    data_a: AccumulatorData,
    _query_a: &str,
    metric_b: &str,
    aggregation_type_b: AggregationType,
    grouping_labels_b: Vec<&str>,
    data_b: AccumulatorData,
    _query_b: &str,
) -> ASAPQueryEngine {
    let store = Arc::new(SketchStore::new());
    for (metric, kind, labels, data) in [
        (metric_a, aggregation_type_a, grouping_labels_a, data_a),
        (metric_b, aggregation_type_b, grouping_labels_b, data_b),
    ] {
        let output = stored_output(metric, kind, labels, 1, WindowKind::Tumbling);
        ingest(&store, &output, 0, at_one_second(data));
    }
    ASAPQueryEngine::new(1).with_sketch_index(store)
}

/// Creates a ASAPQueryEngine with three independent metrics, each with their own
/// stored output.
#[allow(clippy::too_many_arguments)]
pub fn create_engine_three_metrics(
    metric_a: &str,
    aggregation_type_a: AggregationType,
    grouping_labels_a: Vec<&str>,
    data_a: AccumulatorData,
    _query_a: &str,
    metric_b: &str,
    aggregation_type_b: AggregationType,
    grouping_labels_b: Vec<&str>,
    data_b: AccumulatorData,
    _query_b: &str,
    metric_c: &str,
    aggregation_type_c: AggregationType,
    grouping_labels_c: Vec<&str>,
    data_c: AccumulatorData,
    _query_c: &str,
) -> ASAPQueryEngine {
    let store = Arc::new(SketchStore::new());
    for (metric, kind, labels, data) in [
        (metric_a, aggregation_type_a, grouping_labels_a, data_a),
        (metric_b, aggregation_type_b, grouping_labels_b, data_b),
        (metric_c, aggregation_type_c, grouping_labels_c, data_c),
    ] {
        let output = stored_output(metric, kind, labels, 1, WindowKind::Tumbling);
        ingest(&store, &output, 0, at_one_second(data));
    }
    ASAPQueryEngine::new(1).with_sketch_index(store)
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
    create_engine_multi_timestamp_with_window(
        metric,
        aggregation_type,
        grouping_labels,
        data,
        promql_query,
        1,
        WindowKind::Tumbling,
    )
}

/// Creates a single-pop engine with data at multiple timestamps and configurable window.
///
/// Like `create_engine_multi_timestamp` but allows setting `window_size` and `window_type`
/// on the stored output (needed for temporal queries like `sum_over_time(metric[5s])`).
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub fn create_engine_multi_timestamp_with_window(
    metric: &str,
    aggregation_type: AggregationType,
    grouping_labels: Vec<&str>,
    data: Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>,
    _promql_query: &str,
    window_size: u64,
    window_type: WindowKind,
) -> ASAPQueryEngine {
    let store = Arc::new(SketchStore::new());
    let output = stored_output(
        metric,
        aggregation_type,
        grouping_labels,
        window_size,
        window_type,
    );
    ingest(&store, &output, 1000, data);
    ASAPQueryEngine::new(1).with_sketch_index(store)
}
