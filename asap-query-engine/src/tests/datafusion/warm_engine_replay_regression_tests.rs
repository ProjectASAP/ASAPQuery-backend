//! Regression tests for `replay.jsonl` warm-tier failures observed
//! during the MVP demo (PR #333 → ProjectASAP/ASAPCollector#46).
//!
//! Two query shapes returned `status=error` from
//! [`crate::engines::simple::engine::SimpleEngine::handle_query_promql`]:
//!
//!   1. `quantile_over_time(0.99, <ddsketch-metric>[1m])` — 500/500
//!      failures. The DDSketch sketch state lands in the warm tier
//!      via the agent's `gorillas3 + ddsketch` pipeline, and
//!      [`crate::precompute_operators::dd_sketch_accumulator::DDSketchAccumulator`]
//!      already supports `Statistic::Quantile`. The query should
//!      hit that code path, not error.
//!   2. `sum by (zone) (http_requests_total)` instant — 500/1000
//!      failures (the matching `sum by (zone) (rate(...[5m]))`
//!      succeeded). This is a vanilla `OnlySpatial` aggregation
//!      over a counter; the warm-tier engine has matchers for it.
//!
//! These tests pin both query shapes against the precise wire format
//! the MVP demo replays so a future regression in pattern dispatch
//! surfaces here as a unit-test failure rather than a 25-minute
//! demo run.
//!
//! Test fixtures use windowed inserts — `(timestamp - window_ms,
//! timestamp)` rather than the zero-duration `(timestamp, timestamp)`
//! pair from `create_engine_single_pop` — so the store's
//! `[query_start, query_end)` overlap filter selects them. The
//! demo's live ingest path always emits windowed data, so this
//! matches production semantics.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage,
        SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::engines::simple::engine::SimpleEngine;
    use crate::engines::QueryResult;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::DDSketchAccumulator;
    use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::AggregateCore;
    use asap_sketchlib::sketches::ddsketch::DdSketch;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// `replay.jsonl` rendered the query at this absolute time;
    /// matches the existing temporal tests' convention so the
    /// store's `[query_start, query_end)` window aligns with the
    /// data the helper seeds.
    const QUERY_TIME_SEC: f64 = 1000.0;
    /// Match the existing factories' window-end timestamp
    /// (`1_000_000` ms = 1000 s wall clock).
    const WINDOW_END_MS: u64 = 1_000_000;
    /// 60s pane (= one full `[1m]` range), so even an instant
    /// query whose effective range is 1s lands inside the window.
    const WINDOW_LEN_MS: u64 = 60_000;

    /// Scrape interval (seconds) — needs to be ≥ 1 so the
    /// `OnlySpatial` instant-query range
    /// `[end - scrape*1000, end)` is non-empty.
    const SCRAPE_INTERVAL_S: u64 = 1;

    /// Best-effort tracing init — installs a subscriber the first
    /// time it is called so the `warn!` log lines emitted from the
    /// engine surface in `--nocapture` output. A failure here means
    /// a subscriber is already registered (other test in the same
    /// run beat us to it), which is fine.
    fn init_tracing_for_test() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    }

    /// Construct a `SimpleEngine` with a single agg config seeded
    /// with one or more accumulators across `grouping_labels`.
    /// Inserts each accumulator under window
    /// `(WINDOW_END_MS - WINDOW_LEN_MS, WINDOW_END_MS)` so the
    /// store's overlap filter accepts it for instant + range
    /// queries at `QUERY_TIME_SEC`.
    fn build_engine_with_window(
        metric: &str,
        agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        data: Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>,
        promql_query: &str,
    ) -> SimpleEngine {
        let label_strings: Vec<String> = grouping_labels.iter().map(|s| s.to_string()).collect();

        let mut aggregation_configs = HashMap::new();
        let agg_config = AggregationConfig {
            aggregation_id: 1,
            aggregation_type: agg_type,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(label_strings.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            // 60s tumbling window so a 1m range query sees one
            // full pane and the spatial query's narrow range
            // overlaps it too.
            window_size: WINDOW_LEN_MS / 1000,
            slide_interval: WINDOW_LEN_MS / 1000,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        aggregation_configs.insert(1u64, agg_config);

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
            storage_backend: Default::default(),
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        for (label_values_opt, acc) in data {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            let output = PrecomputedOutput::new(
                WINDOW_END_MS - WINDOW_LEN_MS,
                WINDOW_END_MS,
                key,
                1,
            );
            store.insert_precomputed_output(output, acc).unwrap();
        }

        let promql_schema = PromQLSchema::new()
            .add_metric(metric.to_string(), KeyByLabelNames::new(label_strings));

        let query_config = QueryConfig::new(promql_query.to_string())
            .add_aggregation(AggregationReference::new(1, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            SCRAPE_INTERVAL_S,
            QueryLanguage::promql,
        )
    }

    // ------------------------------------------------------------------
    // (1) quantile_over_time over a DDSketch-resident metric
    // ------------------------------------------------------------------

    /// Build a `DDSketchAccumulator` populated with values
    /// `[1.0, 2.0, .., 100.0]` so the 0.99 quantile lands near 99.0.
    fn dd_sketch_with_1_to_100() -> DDSketchAccumulator {
        let mut inner = DdSketch::new(0.01);
        for i in 1..=100u32 {
            inner.update(i as f64);
        }
        DDSketchAccumulator { inner }
    }

    #[test]
    fn quantile_over_time_against_ddsketch_does_not_error() {
        init_tracing_for_test();
        let acc = dd_sketch_with_1_to_100();
        let query = "quantile_over_time(0.99, http_requests_total_latency_ms[1m])";
        let engine = build_engine_with_window(
            "http_requests_total_latency_ms",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
            query,
        );

        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        let (_labels, qr) =
            result.expect("warm engine must answer quantile_over_time over DDSketch");

        match qr {
            QueryResult::Vector(iv) => {
                assert!(
                    !iv.values.is_empty(),
                    "quantile_over_time should produce at least one element"
                );
                let v = iv.values[0].value;
                // DDSketch with α=0.01 → 1% relative error; 0.99
                // quantile of 1..=100 is 99 (or one of the
                // neighbours). Allow generous slack so the test is
                // not flaky on bucket-boundary effects.
                assert!(
                    (v - 99.0).abs() < 5.0,
                    "expected ~99.0 from DDSketch.quantile(0.99), got {v}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // (2) Instant `sum by (zone) (http_requests_total)`
    // ------------------------------------------------------------------

    #[test]
    fn sum_by_zone_instant_does_not_error() {
        init_tracing_for_test();
        let query = "sum by (zone) (http_requests_total)";
        let engine = build_engine_with_window(
            "http_requests_total",
            AggregationType::Sum,
            vec!["zone"],
            vec![
                (
                    Some(vec!["us-east-1".to_string()]),
                    Box::new(SumAccumulator::with_sum(100.0)),
                ),
                (
                    Some(vec!["us-west-2".to_string()]),
                    Box::new(SumAccumulator::with_sum(50.0)),
                ),
            ],
            query,
        );

        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        let (_labels, qr) =
            result.expect("warm engine must answer instant `sum by (zone) (counter)`");

        match qr {
            QueryResult::Vector(iv) => {
                assert_eq!(
                    iv.values.len(),
                    2,
                    "expected 2 zones, got {} values",
                    iv.values.len()
                );
                let mut by_zone = std::collections::HashMap::new();
                for el in &iv.values {
                    // Per `sum by (zone)` the only output label is
                    // `zone`; element labels are positional, so we
                    // pull the first.
                    let zone = el
                        .labels
                        .labels
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<missing>".to_string());
                    by_zone.insert(zone, el.value);
                }
                assert!(
                    (by_zone.get("us-east-1").copied().unwrap_or(f64::NAN) - 100.0).abs() < 1e-9,
                    "us-east-1 should be 100.0, by_zone={by_zone:?}"
                );
                assert!(
                    (by_zone.get("us-west-2").copied().unwrap_or(f64::NAN) - 50.0).abs() < 1e-9,
                    "us-west-2 should be 50.0, by_zone={by_zone:?}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // (3) Cross-check on the temporal-of-quantile alternate spelling
    //     `quantile_over_time(0.5, ...)` — pins that the fix is not
    //     hard-coded to 0.99 and that the parameter is correctly
    //     extracted out of `function_args.first()`.
    // ------------------------------------------------------------------

    #[test]
    fn quantile_over_time_p50_against_ddsketch_does_not_error() {
        init_tracing_for_test();
        let acc = dd_sketch_with_1_to_100();
        let query = "quantile_over_time(0.5, http_requests_total_latency_ms[1m])";
        let engine = build_engine_with_window(
            "http_requests_total_latency_ms",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
            query,
        );

        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        let (_labels, qr) = result.expect("p50 quantile_over_time should succeed");
        match qr {
            QueryResult::Vector(iv) => {
                assert!(!iv.values.is_empty());
                let v = iv.values[0].value;
                // Median of 1..=100 is 50 or 51; α=0.01 relative
                // error → ~±0.5; allow generous slack.
                assert!(
                    (v - 50.5).abs() < 5.0,
                    "expected ~50.5 from DDSketch.quantile(0.5), got {v}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }
}
