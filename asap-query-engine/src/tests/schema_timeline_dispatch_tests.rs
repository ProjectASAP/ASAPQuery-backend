//! End-to-end tests for the schema-timeline query dispatcher.
//!
//! Exercises the full path from a PromQL query → schema registry
//! lookup → per-segment store query → `combine_statistic` →
//! Prometheus `warnings`, on a real `SimpleEngine` +
//! `SimpleMapStore` + `SchemaRegistry` with two agg_ids for the
//! same metric and a reconfigure boundary inside the query range.
//!
//! Contract validated: queries that span a reconfigure boundary
//! do not see a silent data cliff. Combinable statistics (Count /
//! Sum / Min / Max) get the stitched answer; non-combinable or
//! Purged segments surface as explicit `warnings` on the response
//! so the caller knows the answer is partial.
//!
//! Lives inside the crate (not `tests/`) so we can reach the
//! `#[cfg(test)] insert_raw_for_testing` helper on `SchemaRegistry`
//! without leaking a test-only API into the public crate surface.

use std::collections::HashMap;
use std::sync::Arc;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::aggregation_reference::AggregationReference;
use asap_types::enums::{AggregationType, WindowType};
use asap_types::promql_schema::PromQLSchema;
use asap_types::query_config::QueryConfig;
use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

use crate::data_model::{
    CleanupPolicy, HotReloadStreamingConfig, InferenceConfig, KeyByLabelValues, PrecomputedOutput,
    QueryLanguage, SchemaConfig, StreamingConfig,
};
use crate::engines::{QueryResult, SimpleEngine};
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
use crate::stores::sketch_db::{AggSchema, SchemaRegistry};
use crate::stores::Store;

const METRIC: &str = "sensor_reading";

// Timeline layout used by the tests. Picked so that an instant
// query at `QUERY_TIME_SEC` produces a range that straddles the
// reconfigure boundary between `agg_1` and `agg_2`.
//
// * `BOUNDARY_MS` — where `agg_1.retired_at_ms` == `agg_2.created_at_ms`.
// * `AGG1_SAMPLE_MS` — at-boundary-ish stamp used for agg_1's seeded
//   window so the clipped `[QUERY_START_MS, BOUNDARY_MS]` sub-query
//   finds it.
// * `AGG2_SAMPLE_MS` — post-boundary stamp used for agg_2's seeded
//   window so the clipped `[BOUNDARY_MS, QUERY_TIME_MS]` sub-query
//   finds it.
const QUERY_TIME_SEC: f64 = 501.0;
const QUERY_TIME_MS: u64 = 501_000;
const BOUNDARY_MS: u64 = 500_500;
const AGG1_SAMPLE_MS: u64 = 500_000;
const AGG2_SAMPLE_MS: u64 = 501_000;

fn make_agg_config(id: u64) -> AggregationConfig {
    AggregationConfig::new(
        id,
        AggregationType::Sum,
        String::new(),
        HashMap::new(),
        KeyByLabelNames::new(vec!["host".to_string()]),
        KeyByLabelNames::empty(),
        KeyByLabelNames::empty(),
        String::new(),
        1,
        1,
        WindowType::Tumbling,
        String::new(),
        METRIC.to_string(),
        None,
        None,
        None,
        None,
    )
}

/// Construct a schema with explicit lifecycle timestamps, bypassing
/// the wall-clock `new_active` path so we can pin a schema into
/// `Retired` or `Expired` status for coverage testing.
fn fixed_schema(
    agg_id: u64,
    created_at_ms: u64,
    retired_at_ms: Option<u64>,
    expires_at_ms: Option<u64>,
) -> AggSchema {
    let mut base = AggSchema::new_active(make_agg_config(agg_id));
    base.created_at_ms = created_at_ms;
    base.retired_at_ms = retired_at_ms;
    base.expires_at_ms = expires_at_ms;
    base
}

/// Instant PromQL query used by the tests. Runs through the
/// OnlySpatial aggregation pattern (op=sum) with a `by (host)`
/// modifier — the engine's `format_final_results` path only
/// emits keyed output elements, so the query must be grouped for
/// the result vector to be non-empty.
const TEST_QUERY: &str = "sum by (host) (sensor_reading)";

fn build_engine(
    streaming_config: Arc<StreamingConfig>,
    schemas: Arc<SchemaRegistry>,
    store: Arc<dyn Store>,
    query_for_agg_id: u64,
) -> SimpleEngine {
    let mut inference_config =
        InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    let promql_schema = PromQLSchema::new().add_metric(
        METRIC.to_string(),
        KeyByLabelNames::new(vec!["host".to_string()]),
    );
    inference_config.schema = SchemaConfig::PromQL(promql_schema);
    // Pin the test query to the active agg so the probe resolution
    // succeeds; the dispatcher still visits every timeline segment
    // regardless of which one the probe picked.
    inference_config.query_configs = vec![QueryConfig::new(TEST_QUERY.to_string())
        .add_aggregation(AggregationReference::new(query_for_agg_id, None))];

    let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
    SimpleEngine::new_with_hot_reload(
        store,
        inference_config,
        hot_reload,
        1,
        QueryLanguage::promql,
    )
    .with_schema_registry(schemas)
}

/// Insert a single `SumAccumulator` window at `ts` into `agg_id`.
/// Uses `(ts, ts)` for the window start/end pair to match the
/// existing test-utility pattern in `engine_factories` — the engine
/// treats those as single-point buckets aligned to the tumbling
/// window, so a query whose range contains `ts` picks up the data.
fn seed_sum_at(store: &SimpleMapStore, agg_id: u64, ts: u64, host: &str, sum: f64) {
    let key = Some(KeyByLabelValues {
        labels: vec![host.to_string()],
    });
    let output = PrecomputedOutput::new(ts, ts, key, agg_id);
    let acc = SumAccumulator::with_sum(sum);
    store
        .insert_precomputed_output(output, Box::new(acc))
        .expect("seed insert must succeed");
}

/// Two schemas for the same metric, both answerable: agg_1 is
/// Retired-but-not-Expired (coverage=Sketch) with data in its own
/// lifetime, agg_2 is Active with data post-boundary. Sum is
/// combinable, so the dispatcher folds 10.0 + 20.0 into
/// `Full(30.0)` — no warnings, no data cliff.
#[test]
fn sum_query_across_reconfigure_boundary_returns_combined_full_result() {
    let mut agg_map = HashMap::new();
    agg_map.insert(1u64, make_agg_config(1));
    agg_map.insert(2u64, make_agg_config(2));
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let schemas = Arc::new(SchemaRegistry::empty());
    // agg_1: Retired at the boundary but not yet Expired, so
    // coverage stays `Sketch` and the dispatcher evaluates it.
    schemas.insert_raw_for_testing(fixed_schema(1, 0, Some(BOUNDARY_MS), Some(u64::MAX / 4)));
    // agg_2: Active from the boundary onwards.
    schemas.insert_raw_for_testing(fixed_schema(2, BOUNDARY_MS, None, None));

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    // Data placed so the instant query at `QUERY_TIME_SEC` sweeps
    // `[QUERY_START_MS, QUERY_TIME_MS]`. After the dispatcher clips
    // per segment:
    //   agg_1's sub-range is `[QUERY_START_MS, BOUNDARY_MS]`
    //   agg_2's sub-range is `[BOUNDARY_MS, QUERY_TIME_MS]`
    seed_sum_at(&store, 1, AGG1_SAMPLE_MS, "A", 10.0);
    seed_sum_at(&store, 2, AGG2_SAMPLE_MS, "A", 20.0);

    let engine = build_engine(streaming_config, schemas, store, 2);

    let (_labels, qr) = engine
        .handle_query_promql(TEST_QUERY.to_string(), QUERY_TIME_SEC)
        .expect("query must produce a result");

    assert!(
        qr.warnings().is_empty(),
        "Sum is cleanly combinable — no warnings expected, got {:?}",
        qr.warnings()
    );
    match qr {
        QueryResult::Vector(iv) => {
            assert_eq!(iv.values.len(), 1, "one combined scalar across segments");
            assert!(
                (iv.values[0].value - 30.0).abs() < 1e-9,
                "expected 10 + 20 = 30.0 across the reconfigure boundary, got {}",
                iv.values[0].value
            );
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}

/// agg_1 Expired (coverage=Purged, unresolved); agg_2 Active with
/// data. `combine_statistic(Sum)` on a combinable stat with a
/// non-empty `unresolved` list returns `Partial { covered: Some,
/// missing: [...] }`. The engine surfaces the partial through
/// `QueryResult::warnings()`.
#[test]
fn sum_query_with_purged_segment_returns_partial_with_warnings() {
    let mut agg_map = HashMap::new();
    agg_map.insert(1u64, make_agg_config(1));
    agg_map.insert(2u64, make_agg_config(2));
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let schemas = Arc::new(SchemaRegistry::empty());
    // agg_1: retired at the boundary so its lifetime [0, BOUNDARY_MS)
    // overlaps the query range, but `expires_at_ms` is in the past
    // so `status()` returns `Expired` → `coverage_for` returns
    // `Purged`. The dispatcher treats this segment as unresolved
    // and forces a Partial combine.
    schemas.insert_raw_for_testing(fixed_schema(1, 0, Some(BOUNDARY_MS), Some(1_000)));
    schemas.insert_raw_for_testing(fixed_schema(2, BOUNDARY_MS, None, None));

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    // Only agg_2 has data; agg_1's data is assumed gone with the
    // Purged classification.
    seed_sum_at(&store, 2, AGG2_SAMPLE_MS, "A", 20.0);

    let engine = build_engine(streaming_config, schemas, store, 2);

    let (_labels, qr) = engine
        .handle_query_promql(TEST_QUERY.to_string(), QUERY_TIME_SEC)
        .expect("query must produce a result even with a Partial combine");

    assert!(
        !qr.warnings().is_empty(),
        "Purged segment must populate warnings — got empty list"
    );
    let joined = qr.warnings().join(" | ");
    assert!(
        joined.contains("partial result") && joined.contains(METRIC),
        "warnings should explain the partial + reference the metric: {joined}"
    );
    assert!(
        joined.contains("agg_id=1"),
        "warnings should enumerate the unresolved agg_id=1: {joined}"
    );

    match qr {
        QueryResult::Vector(iv) => {
            assert_eq!(iv.values.len(), 1, "best-effort covered sum");
            assert!(
                (iv.values[0].value - 20.0).abs() < 1e-9,
                "covered sum is agg_2's 20.0; got {}",
                iv.values[0].value
            );
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}

/// Single-schema regression guard: when the timeline has only one
/// segment, the dispatcher returns `None`, the default single-agg
/// path handles the query, no warnings attach.
#[test]
fn single_schema_query_falls_through_to_default_path() {
    let mut agg_map = HashMap::new();
    agg_map.insert(7u64, make_agg_config(7));
    let streaming_config = Arc::new(StreamingConfig::new(agg_map));

    let schemas = Arc::new(SchemaRegistry::empty());
    schemas.insert_raw_for_testing(fixed_schema(7, 0, None, None));

    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    seed_sum_at(&store, 7, QUERY_TIME_MS, "A", 42.0);

    let engine = build_engine(streaming_config, schemas, store, 7);

    let (_labels, qr) = engine
        .handle_query_promql(TEST_QUERY.to_string(), QUERY_TIME_SEC)
        .expect("query must produce a result");

    assert!(
        qr.warnings().is_empty(),
        "single-schema path must not attach warnings: {:?}",
        qr.warnings()
    );
    match qr {
        QueryResult::Vector(iv) => {
            assert_eq!(iv.values.len(), 1);
            assert!(
                (iv.values[0].value - 42.0).abs() < 1e-9,
                "single-agg answer should be 42.0, got {}",
                iv.values[0].value
            );
        }
        other => panic!("expected instant vector, got {other:?}"),
    }
}
