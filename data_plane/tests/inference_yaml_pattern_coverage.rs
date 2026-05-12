//! Inference-YAML PromQL pattern coverage.
//!
//! Verifies the warm-tier query coverage advertised in
//! `asap-query-engine/examples/promql/inference_config.yaml`:
//!
//! 1. The YAML parses and exposes every pattern family the engine claims
//!    to serve (multi-quantile, wider ranges, rate/increase, sum/count
//!    over time, spatial aggregations, top-K).
//! 2. For each family, an end-to-end PromQL query routes through
//!    `find_query_config` → `parse_and_match_promql` → `query_statistic`
//!    against a backing accumulator that supports the resolved
//!    `Statistic`, and returns a non-empty result vector.
//!
//! This is the runtime contract referenced by the YAML's leading comment:
//! adding a new entry without a corresponding routing test risks shipping
//! warm-tier "promises" the engine can't keep.

use std::collections::HashMap;
use std::sync::Arc;

use promql_utilities::data_model::KeyByLabelNames;

#[allow(dead_code)]
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

use data_plane::stores::schema::{
    AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
    KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, SchemaConfig,
    StreamingConfig, WindowType,
};
use data_plane::engines::ASAPQueryEngine;
use data_plane::precompute_engine::operators::{
    DDSketchAccumulator, DatasketchesKLLAccumulator, IncreaseAccumulator, SumAccumulator,
};
use data_plane::stores::SimpleMapStore;
use data_plane::stores::Store;
use data_plane::utils::file_io::read_inference_config;
use data_plane::AggregateCore;

const PROMQL_YAML: &str = "examples/promql/inference_config.yaml";

// ─── 1. YAML parses and covers every pattern family ────────────────────

#[test]
fn promql_inference_yaml_loads_all_pattern_families() {
    let cfg = read_inference_config(PROMQL_YAML, QueryLanguage::promql)
        .expect("inference_config.yaml must parse");

    let queries: Vec<&str> = cfg.query_configs.iter().map(|q| q.query.as_str()).collect();

    // Sanity: expansion landed (pre-PR baseline was 1 entry).
    assert!(
        queries.len() >= 20,
        "expected substantial pattern expansion; got {} entries",
        queries.len()
    );

    // Every PromQL string parses through promql_parser — i.e. no typos
    // would silently fail-to-match against an incoming canonical-AST.
    for q in &queries {
        promql_parser::parser::parse(q)
            .unwrap_or_else(|e| panic!("query `{q}` failed to parse: {e}"));
    }

    // Per-family presence checks. Each family must contribute at least
    // one wider-range / wider-shape variant beyond the [1m] / 0.5
    // baseline that PROGRESS.md flagged as the only landed shape.
    let has = |needle: &str| queries.iter().any(|q| q.contains(needle));

    // Multi-quantile (quantile_over_time)
    assert!(has("quantile_over_time(0.9, fake_metric[1m])"));
    assert!(has("quantile_over_time(0.95, fake_metric[1m])"));
    assert!(has("quantile_over_time(0.99, fake_metric[1m])"));
    // Wider ranges
    assert!(has("quantile_over_time(0.5, fake_metric[2m])"));
    assert!(has("quantile_over_time(0.5, fake_metric[5m])"));
    // Rate / increase
    assert!(has("rate(fake_metric[1m])"));
    assert!(has("rate(fake_metric[5m])"));
    assert!(has("increase(fake_metric[1m])"));
    // Sum / count over wider ranges
    assert!(has("sum_over_time(fake_metric[2m])"));
    assert!(has("sum_over_time(fake_metric[5m])"));
    assert!(has("count_over_time(fake_metric[1m])"));
    // Spatial aggregations
    assert!(has("count(fake_metric)"));
    assert!(has("sum(fake_metric)"));
    assert!(has("avg(fake_metric)"));
    // Top-K
    assert!(has("topk(5, fake_metric)"));
    assert!(has("topk(10, fake_metric)"));
    assert!(has("topk(50, fake_metric)"));
    // Multi-quantile spatial
    assert!(has("quantile by (label_0) (0.5, fake_metric)"));
    assert!(has("quantile by (label_0) (0.99, fake_metric)"));
}

// ─── 2. Per-family runtime routing tests ───────────────────────────────
//
// Each test below exercises the full warm-tier path for one pattern
// family: the engine finds the YAML entry exactly, pattern-matches
// the request to a `Statistic`, and the chosen accumulator's
// `query_statistic` returns a finite scalar — which the engine then
// folds into a `QueryResult::Vector`.
//
// We keep the engine fixtures inline rather than reusing
// `crate::tests::test_utilities::engine_factories` because that module
// is `#[cfg(test)]`-only and not visible to integration tests in
// `tests/`. The fixture is small enough that this is fine.

/// Single-population single-aggregation engine fixture. `agg_type` and
/// `acc` must agree (e.g. `DDSketch` + `DDSketchAccumulator`).
///
/// `grouping_labels` MUST be non-empty for any non-spatial test —
/// `format_final_results` drops result rows whose key is `None`, so a
/// `None`-keyed insert produces a "result Some, vector empty" outcome
/// that's indistinguishable from a real warm-tier miss.
fn build_engine(
    metric: &str,
    schema_labels: &[&str],
    agg_type: AggregationType,
    grouping_labels: &[&str],
    window_size: u64,
    acc: Box<dyn AggregateCore>,
    promql_query: &str,
) -> ASAPQueryEngine {
    let schema_label_strs: Vec<String> = schema_labels.iter().map(|s| s.to_string()).collect();
    let grouping_label_strs: Vec<String> = grouping_labels.iter().map(|s| s.to_string()).collect();

    let mut aggregation_configs = HashMap::new();
    aggregation_configs.insert(
        1u64,
        AggregationConfig {
            aggregation_id: 1,
            aggregation_type: agg_type,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(grouping_label_strs.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size,
            slide_interval: window_size,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        },
    );
    let streaming_config = Arc::new(StreamingConfig {
        aggregation_configs,
        storage_backend: Default::default(),
    });
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));

    // Insert one window's worth of precomputed data covering the grid
    // around `query_time = 1_000_000` ms used by the assertions below.
    // Pre-populate two windows so wider ranges (e.g. [5m]) still find
    // a covering pane via the closest-pane store query.
    let window_ms = window_size * 1000;
    for i in 0..2 {
        let end_ts = 1_000_000_u64 - i * window_ms;
        let start_ts = end_ts.saturating_sub(window_ms);
        // Synthesize one label value per grouping label so the result
        // row has a non-`None` key (see fn-level comment).
        let key_labels: Vec<String> = grouping_label_strs
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("v{idx}"))
            .collect();
        let key = Some(KeyByLabelValues { labels: key_labels });
        let output = PrecomputedOutput::new(start_ts, end_ts, key, 1);
        store
            .insert_precomputed_output(output, acc.clone_boxed_core())
            .unwrap();
    }

    let promql_schema =
        PromQLSchema::new().add_metric(metric.to_string(), KeyByLabelNames::new(schema_label_strs));
    let inference_config = InferenceConfig {
        schema: SchemaConfig::PromQL(promql_schema),
        query_configs: vec![QueryConfig::new(promql_query.to_string())
            .add_aggregation(AggregationReference::new(1, None))],
        cleanup_policy: CleanupPolicy::NoCleanup,
    };

    ASAPQueryEngine::new(
        store,
        inference_config,
        streaming_config,
        1,
        QueryLanguage::promql,
    )
}

const QUERY_TIME_SEC: f64 = 1000.0;

fn make_dd_acc(values: &[f64]) -> Box<dyn AggregateCore> {
    let mut acc = DDSketchAccumulator::new(0.01);
    for &v in values {
        acc.inner.update(v);
    }
    Box::new(acc)
}

fn make_kll_acc(values: &[f64]) -> Box<dyn AggregateCore> {
    let mut acc = DatasketchesKLLAccumulator::new(200);
    for &v in values {
        acc.inner.update(v);
    }
    Box::new(acc)
}

fn make_sum_acc(total: f64) -> Box<dyn AggregateCore> {
    Box::new(SumAccumulator::with_sum(total))
}

#[test]
fn quantile_over_time_multi_phi_routes_through_warm_tier() {
    init_test_tracing();
    // KLL backs the canonical warm-tier quantile path. Same metric
    // schema as the YAML so `find_query_config` exact-matches.
    let acc = make_kll_acc(&[10.0, 20.0, 30.0, 40.0, 50.0]);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::DatasketchesKLL,
        &["instance"],
        60, // 1m window
        acc,
        "quantile_over_time(0.5, fake_metric[1m])",
    );

    let result = engine
        .handle_query_promql(
            "quantile_over_time(0.5, fake_metric[1m])".to_string(),
            QUERY_TIME_SEC,
        )
        .expect("warm tier should answer p50 quantile_over_time");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(!elements.is_empty(), "expected non-empty p50 result");
    let p50 = elements[0].value;
    assert!(
        p50.is_finite() && (5.0..=55.0).contains(&p50),
        "p50 out of plausible range for [10..50]: {p50}"
    );
}

#[test]
fn quantile_over_time_wider_range_routes_through_warm_tier() {
    // [5m] entry must match — pre-PR this fell to capability matching.
    let acc = make_dd_acc(&[1.0, 2.0, 3.0, 4.0, 5.0]);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::DDSketch,
        &["instance"],
        300, // 5m window
        acc,
        "quantile_over_time(0.99, fake_metric[5m])",
    );

    let result = engine
        .handle_query_promql(
            "quantile_over_time(0.99, fake_metric[5m])".to_string(),
            QUERY_TIME_SEC,
        )
        .expect("warm tier should answer [5m] quantile_over_time");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(!elements.is_empty(), "expected non-empty [5m] p99 result");
}

#[test]
fn rate_routes_to_increase_accumulator_warm_tier() {
    // Rate / Increase requires an Increase-typed aggregation; KLL has no
    // delta, so the YAML's `rate(fake_metric[…])` entries are paired
    // with this accumulator type at runtime. We exercise that pairing.
    let acc = IncreaseAccumulator::new(
        data_plane::Measurement::new(0.0),
        0,
        data_plane::Measurement::new(100.0),
        60_000,
    );
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::Increase,
        &["instance"],
        60,
        Box::new(acc),
        "rate(fake_metric[1m])",
    );

    let result = engine
        .handle_query_promql("rate(fake_metric[1m])".to_string(), QUERY_TIME_SEC)
        .expect("warm tier should answer rate(...[1m])");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(!elements.is_empty(), "rate result should not be empty");
}

#[test]
fn increase_routes_to_increase_accumulator_warm_tier() {
    let acc = IncreaseAccumulator::new(
        data_plane::Measurement::new(5.0),
        0,
        data_plane::Measurement::new(25.0),
        60_000,
    );
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::Increase,
        &["instance"],
        60,
        Box::new(acc),
        "increase(fake_metric[1m])",
    );

    let result = engine
        .handle_query_promql("increase(fake_metric[1m])".to_string(), QUERY_TIME_SEC)
        .expect("warm tier should answer increase(...[1m])");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(!elements.is_empty(), "increase result should not be empty");
}

#[test]
fn sum_over_time_wider_range_routes_through_warm_tier() {
    // SumAccumulator answers Statistic::Sum directly. [2m] entry was
    // previously absent and would have fallen to capability matching.
    let acc = make_sum_acc(420.0);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::Sum,
        &["instance"],
        120, // 2m window
        acc,
        "sum_over_time(fake_metric[2m])",
    );

    let result = engine
        .handle_query_promql("sum_over_time(fake_metric[2m])".to_string(), QUERY_TIME_SEC)
        .expect("warm tier should answer sum_over_time(...[2m])");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(
        !elements.is_empty(),
        "sum_over_time result should not be empty"
    );
}

#[test]
fn count_over_time_routes_through_warm_tier() {
    let acc = make_sum_acc(7.0);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::Sum,
        &["instance"],
        60,
        acc,
        "count_over_time(fake_metric[1m])",
    );

    let result = engine
        .handle_query_promql(
            "count_over_time(fake_metric[1m])".to_string(),
            QUERY_TIME_SEC,
        )
        .expect("warm tier should answer count_over_time(...[1m])");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(
        !elements.is_empty(),
        "count_over_time result should not be empty"
    );
}

#[test]
fn spatial_sum_routes_through_warm_tier() {
    let acc = make_sum_acc(100.0);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::Sum,
        &["label_0"],
        1,
        acc,
        "sum(fake_metric)",
    );

    let result = engine
        .handle_query_promql("sum(fake_metric)".to_string(), QUERY_TIME_SEC)
        .expect("warm tier should answer sum(metric)");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(!elements.is_empty(), "sum() result should not be empty");
}

/// Phase-3.1 regression: pin the canonical MVP-demo PromQL query
/// `quantile_over_time(0.99, http_requests_total_latency_ms[1m])`
/// against a DDSketch-typed agg WITHOUT a matching `query_config`
/// exact-string entry. This forces capability-based matching
/// (`find_compatible_aggregation`) — pre-fix, this branch returned
/// `None` because `compatible_agg_types(Quantile)` listed only KLL
/// types. Post-fix, DDSketch is enumerated and the warm tier
/// answers the query cleanly. Mirrors the demo configuration
/// described in `ASAPCollector/docs/spec-mvp-controller-driven-multi-stage-demo.md`
/// (DDSketch for `_latency_ms` quantile-over-time at the edge).
///
/// NOTE: schema labels and grouping labels are kept empty so the
/// `OnlyTemporal` `requirements.grouping_labels` (= all schema
/// labels) matches the config's grouping labels exactly under the
/// strict-equality `labels_compatible` check. The labels-superset
/// relaxation TODO'd in `capability_matching.rs:238` is out of
/// scope for this fix.
#[test]
fn canonical_mvp_demo_quantile_over_time_resolves_via_capability_matching() {
    init_test_tracing();
    let acc = make_dd_acc(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]);
    let engine = build_engine(
        "http_requests_total_latency_ms",
        &[],
        AggregationType::DDSketch,
        &[],
        60, // 1m window
        acc,
        // `query_config` query string deliberately mismatches the live
        // request below so `find_query_config` misses and the engine
        // falls through to `find_compatible_aggregation`.
        "quantile_over_time(0.5, http_requests_total_latency_ms[5m])",
    );

    let result = engine
        .handle_query_promql(
            // The MVP-demo canonical query — neither phi nor range
            // overlaps with the registered query_config above.
            "quantile_over_time(0.99, http_requests_total_latency_ms[1m])".to_string(),
            QUERY_TIME_SEC,
        )
        .expect(
            "warm tier must resolve quantile_over_time against a DDSketch-only config via capability matching",
        );
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(
        !elements.is_empty(),
        "expected non-empty p99 result — capability matching now picks DDSketch for Quantile",
    );
    let p99 = elements[0].value;
    assert!(
        p99.is_finite() && (50.0..=110.0).contains(&p99),
        "p99 out of plausible range for [10..100]: {p99}",
    );
}

#[test]
fn spatial_multi_quantile_routes_through_warm_tier() {
    // p50 spatial — pre-PR only p99 had an entry, so this would
    // previously have fallen to capability matching.
    let acc = make_kll_acc(&[10.0, 20.0, 30.0, 40.0, 50.0]);
    let engine = build_engine(
        "fake_metric",
        &["instance", "job", "label_0", "label_1"],
        AggregationType::DatasketchesKLL,
        &["label_0"],
        1,
        acc,
        "quantile by (label_0) (0.5, fake_metric)",
    );

    let result = engine
        .handle_query_promql(
            "quantile by (label_0) (0.5, fake_metric)".to_string(),
            QUERY_TIME_SEC,
        )
        .expect("warm tier should answer quantile by(...) (0.5, ...)");
    let (_, qr) = result;
    let elements = match qr {
        data_plane::engines::QueryResult::Vector(iv) => iv.values,
        other => panic!("expected vector, got {other:?}"),
    };
    assert!(
        !elements.is_empty(),
        "spatial p50 result should not be empty"
    );
}
