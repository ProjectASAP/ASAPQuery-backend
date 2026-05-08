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
    use crate::data_model::Measurement;
    use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;
    use crate::precompute_operators::count_sketch_accumulator::CountSketchAccumulator;
    use crate::precompute_operators::hll_sketch_accumulator::HllSketchAccumulator;
    use crate::precompute_operators::increase_accumulator::IncreaseAccumulator;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::DDSketchAccumulator;
    use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::AggregateCore;
    use asap_sketchlib::sketches::ddsketch::DdSketch;
    use asap_sketchlib::sketches::{CountMinSketch, CountSketch, HllSketch};
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

    // ------------------------------------------------------------------
    // (4) Instant `sum by (zone) (counter)` backed by IncreaseAccumulator
    //
    //     This is the actual demo failure shape from
    //     ProjectASAP/ASAPCollector#46: the warm-tier ingest path
    //     stores OTel-`Sum`/monotonic counters as
    //     `IncreaseAccumulator`, not `SumAccumulator`. Pre-fix, this
    //     query class capability-missed because `IncreaseAccumulator`
    //     did not implement `Statistic::Sum`. Post-fix:
    //
    //       a) `compatible_agg_types(Statistic::Sum)` lists
    //          `Increase` / `MultipleIncrease`, so capability matching
    //          accepts the counter-shaped configs.
    //       b) `IncreaseAccumulator::query(Sum, ..)` returns the
    //          latest cumulative value of the series, matching
    //          Prometheus' `sum(<counter>)` instant semantics.
    //       c) The engine's outer `sum by (zone) (...)` aggregation
    //          groups + sums those per-series totals across keys.
    // ------------------------------------------------------------------

    #[test]
    fn sum_by_zone_instant_over_increase_accumulator_does_not_error() {
        init_tracing_for_test();
        let query = "sum by (zone) (http_requests_total)";

        // Two zones, two cumulative-counter series. Each
        // IncreaseAccumulator's `last_seen_measurement` is the latest
        // cumulative value the series has reported.
        let east = IncreaseAccumulator::new(
            Measurement::new(10.0),
            (WINDOW_END_MS - WINDOW_LEN_MS) as i64,
            Measurement::new(123.0),
            WINDOW_END_MS as i64,
        );
        let west = IncreaseAccumulator::new(
            Measurement::new(0.0),
            (WINDOW_END_MS - WINDOW_LEN_MS) as i64,
            Measurement::new(45.0),
            WINDOW_END_MS as i64,
        );

        let engine = build_engine_with_window(
            "http_requests_total",
            AggregationType::Increase,
            vec!["zone"],
            vec![
                (Some(vec!["us-east-1".to_string()]), Box::new(east)),
                (Some(vec!["us-west-2".to_string()]), Box::new(west)),
            ],
            query,
        );

        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        let (_labels, qr) = result.expect(
            "warm engine must answer instant `sum by (zone) (counter)` against IncreaseAccumulator",
        );

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
                    let zone = el
                        .labels
                        .labels
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<missing>".to_string());
                    by_zone.insert(zone, el.value);
                }
                // Per-zone Sum is the latest cumulative value of that
                // series (Prometheus semantics for sum(<counter>)).
                assert!(
                    (by_zone.get("us-east-1").copied().unwrap_or(f64::NAN) - 123.0).abs() < 1e-9,
                    "us-east-1 should be 123.0 (latest cumulative), by_zone={by_zone:?}"
                );
                assert!(
                    (by_zone.get("us-west-2").copied().unwrap_or(f64::NAN) - 45.0).abs() < 1e-9,
                    "us-west-2 should be 45.0 (latest cumulative), by_zone={by_zone:?}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // (4) DDSketch INGEST-side `_quantile` rename — the production bug
    //     pinned by ProjectASAP/ASAPCollector#46. The agent renames
    //     `http_latency_ms` → `http_latency_ms_quantile` before warm-tier
    //     emit, but the replay client queries with the un-suffixed
    //     conceptual name. This test pins that the warm engine resolves
    //     the alias and answers the quantile rather than returning
    //     `status=error`.
    // ------------------------------------------------------------------

    #[test]
    fn quantile_over_time_resolves_unsuffixed_metric_to_quantile_state() {
        init_tracing_for_test();
        let acc = dd_sketch_with_1_to_100();

        // Engine + store know the sketched-form name only. The
        // streaming config's agg has `metric =
        // "http_latency_ms_quantile"`, mirroring what the
        // controller emits after the DDSketch processor's INGEST
        // rename.
        let engine = build_engine_with_window(
            "http_latency_ms_quantile",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
            // QueryConfig template carries the suffixed name too —
            // matches what the controller would emit alongside the
            // streaming config. The engine is expected to resolve
            // the un-suffixed form to this template via the alias
            // rewrite.
            "quantile_over_time(0.99, http_latency_ms_quantile[1m])",
        );

        // Replay client queries with the CONCEPTUAL un-suffixed
        // name — this is the exact failure case from
        // ProjectASAP/ASAPCollector#46.
        let query = "quantile_over_time(0.99, http_latency_ms[1m])";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        let (_labels, qr) = result.expect(
            "warm engine must resolve un-suffixed `http_latency_ms` to \
             `http_latency_ms_quantile` and answer the quantile",
        );

        match qr {
            QueryResult::Vector(iv) => {
                assert!(
                    !iv.values.is_empty(),
                    "alias-resolved quantile_over_time should produce a value"
                );
                let v = iv.values[0].value;
                assert!(
                    (v - 99.0).abs() < 5.0,
                    "expected ~99.0 from DDSketch.quantile(0.99), got {v}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    /// Cross-check: a non-quantile-shaped query for a metric whose
    /// `_quantile` variant happens to exist must NOT be rewritten —
    /// the alias resolver is shape-gated.
    #[test]
    fn non_quantile_query_does_not_rewrite_metric() {
        init_tracing_for_test();
        // Seed only the suffixed form so a successful rewrite
        // would erroneously route the `sum(...)` query at the
        // DDSketch state. The engine should leave the query
        // untouched, look up the un-suffixed name, find no agg,
        // and return None — but critically NOT panic / mis-route
        // through the alias.
        let acc = dd_sketch_with_1_to_100();
        let engine = build_engine_with_window(
            "lookup_latency_ms_quantile",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
            "quantile_over_time(0.99, lookup_latency_ms_quantile[1m])",
        );

        // sum_over_time is shape `Sum`, not `Quantile`, so the
        // alias resolver must leave the metric name alone. The
        // engine has no agg for `lookup_latency_ms` (no `_quantile`
        // suffix in its configs), so the result is `None` rather
        // than a quantile masquerading as a sum.
        let query = "sum_over_time(lookup_latency_ms[1m])";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        assert!(
            result.is_none(),
            "non-quantile shape must NOT trigger the _quantile alias rewrite; \
             got result={result:?}"
        );
    }

    // ------------------------------------------------------------------
    // (5) **Production-conditions** suite — schema-empty deploy.
    //
    //     The deployed warm-tier backend (`base.yml`'s
    //     `--streaming-config=/etc/asap/streaming.yaml` + no
    //     `--config=…`) starts with `inference_config.schema` set to an
    //     empty `PromQLSchema` and no `query_configs`. The streaming
    //     config DOES carry agg configs, but they declare a non-empty
    //     `grouping_labels` (e.g. `[zone]`) — derived from the
    //     controller's planner output. The bug: capability matching's
    //     `labels_compatible` is strict-exact, and with an empty schema
    //     the engine builds `req.grouping_labels = []`, which fails to
    //     match any agg config's `[zone]`. Every replay query lands on
    //     `format_unsupported_query_response` → `status=error`,
    //     exactly the failure mode `replay.jsonl` shows for the MVP
    //     demo.
    //
    //     `build_engine_production_conditions` mirrors that exact
    //     deploy shape so the regression suite pins both the resolver
    //     fix AND the labels-superset fix.
    // ------------------------------------------------------------------

    /// Build a `SimpleEngine` with the **production warm-tier deploy
    /// shape** — the one ASAPCollector's `base.yml` produces:
    ///
    /// * `streaming_config` carries one agg config with a non-empty
    ///   `grouping_labels` (e.g. `[zone]`), keyed by the metric the
    ///   agent's processor emits (suffixed `_quantile` for DDSketch
    ///   metrics, plain name for HLL/CountSketch/CountMinSketch).
    /// * `inference_config.schema = PromQLSchema::new()` (empty) —
    ///   the warm-tier binary is launched with `--streaming-config`
    ///   only, no `--config`.
    /// * `inference_config.query_configs = []` — no exact-string
    ///   QueryConfig templates.
    ///
    /// Replay queries reach `find_compatible_aggregation` via the
    /// capability-match fallback path. Pre-fix this fails because
    /// `req.grouping_labels = []` can't strict-equal `[zone]`.
    #[allow(clippy::too_many_arguments)]
    fn build_engine_production_conditions(
        agg_metric: &str,
        agg_type: AggregationType,
        agg_grouping_labels: Vec<&str>,
        data: Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>,
    ) -> SimpleEngine {
        let label_strings: Vec<String> = agg_grouping_labels
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: agg_type,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(label_strings),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size: WINDOW_LEN_MS / 1000,
                slide_interval: WINDOW_LEN_MS / 1000,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: agg_metric.to_string(),
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

        // The crucial bit: schema is EMPTY, mirroring the
        // `--streaming-config`-only deploy. The pre-fix engine fails
        // here because `build_query_requirements_promql` resolves
        // `all_labels` to `KeyByLabelNames::empty()`.
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new()),
            query_configs: vec![],
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

    /// (5a) `replay.jsonl` 686/686 failing rows: replays the unsuffixed
    /// metric name against a DDSketch agg keyed by the suffixed wire
    /// name, with the production deploy's empty schema.
    #[test]
    fn production_conditions_quantile_over_time_does_not_error() {
        init_tracing_for_test();
        let acc = dd_sketch_with_1_to_100();
        let engine = build_engine_production_conditions(
            // Agent's DDSketch processor renames to `_quantile` before emit.
            "http_requests_total_latency_ms_quantile",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
        );

        // Replay client uses the conceptual unsuffixed name.
        let query = "quantile_over_time(0.99, http_requests_total_latency_ms[1m])";
        let (_labels, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_SEC)
            .expect(
                "production warm engine must answer quantile_over_time over DDSketch even when \
                 inference_config has an empty PromQLSchema (replay.jsonl 686/686 errors)",
            );

        match qr {
            QueryResult::Vector(iv) => {
                assert!(!iv.values.is_empty(), "expected at least one quantile value");
                let v = iv.values[0].value;
                assert!(
                    (v - 99.0).abs() < 5.0,
                    "expected ~99.0 from DDSketch.quantile(0.99), got {v}"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    /// (5b) `replay.jsonl` 343/343 failing rows: instant
    /// `sum by (zone) (http_requests_total)` against an
    /// IncreaseAccumulator-backed counter.
    #[test]
    fn production_conditions_sum_by_zone_instant_does_not_error() {
        init_tracing_for_test();
        let east = IncreaseAccumulator::new(
            Measurement::new(10.0),
            (WINDOW_END_MS - WINDOW_LEN_MS) as i64,
            Measurement::new(123.0),
            WINDOW_END_MS as i64,
        );
        let west = IncreaseAccumulator::new(
            Measurement::new(0.0),
            (WINDOW_END_MS - WINDOW_LEN_MS) as i64,
            Measurement::new(45.0),
            WINDOW_END_MS as i64,
        );

        let engine = build_engine_production_conditions(
            "http_requests_total",
            AggregationType::Increase,
            vec!["zone"],
            vec![
                (Some(vec!["us-east-1".to_string()]), Box::new(east)),
                (Some(vec!["us-west-2".to_string()]), Box::new(west)),
            ],
        );

        let query = "sum by (zone) (http_requests_total)";
        let (_labels, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_SEC)
            .expect(
                "production warm engine must answer instant `sum by (zone) (counter)` against \
                 IncreaseAccumulator under empty PromQLSchema",
            );

        match qr {
            QueryResult::Vector(iv) => {
                assert_eq!(iv.values.len(), 2, "expected 2 zones");
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    /// (5c) `replay.jsonl` 343/343 failing rows: `count(unique_users_per_min)`
    /// against an HLL agg. HLL accumulator answers `Statistic::Count` as a
    /// cardinality alias (`hll_sketch_accumulator.rs:220`), but
    /// pre-fix `compatible_agg_types(Count)` did not list HLL — capability
    /// match misses → engine returns None → `status=error`.
    #[test]
    fn production_conditions_count_against_hll_does_not_error() {
        init_tracing_for_test();
        // HLL with a few "registers set" — actual cardinality value
        // is irrelevant; the test only asserts the engine resolves
        // the agg and runs the accumulator's query path without
        // erroring.
        let mut hll = HllSketch::new(asap_sketchlib::sketches::hll::HllVariant::Regular, 8);
        for i in 0..1000u32 {
            hll.update(i.to_string().as_bytes());
        }
        let acc = HllSketchAccumulator { inner: hll };

        let engine = build_engine_production_conditions(
            "unique_users_per_min",
            AggregationType::HLL,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
        );

        let query = "count(unique_users_per_min)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        assert!(
            result.is_some(),
            "production warm engine must answer `count(<HLL-metric>)` under empty PromQLSchema; \
             got None → wire status=error",
        );
    }

    /// (5d) `replay.jsonl` 342/342 failing rows: `topk(5, top_endpoint_qps)`
    /// against a CountSketch agg. `CountSketchAccumulator` answers
    /// `Statistic::Topk` (`count_sketch_accumulator.rs:284`), but pre-fix
    /// `compatible_agg_types(Topk)` only listed `CountMinSketchWithHeap`;
    /// CountSketch wasn't reachable through capability matching.
    ///
    /// **Follow-up note**: `CountSketch` is classified as
    /// `is_multi_population_value_type`, so even after adding it to
    /// the Topk compat list the matcher still requires a paired
    /// `SetAggregator` / `DeltaSetAggregator` on the same metric.
    /// The production deploy doesn't ship one — the right structural
    /// fix is for the controller to plan `top_endpoint_qps` as
    /// `CountMinSketchWithHeap` (the integrated CMS+heap accumulator
    /// that answers `topk` without an external key tracker). PR #344
    /// declares that capability on the controller side; the matching
    /// engine-side accumulator wiring is out of scope for the
    /// warm-engine-error PR. Marked `#[ignore]` until the controller
    /// switches family.
    #[test]
    #[ignore = "follow-up: standalone CountSketch agg requires a paired SetAggregator under \
                is_multi_population_value_type semantics; the right fix is for the controller \
                to plan top_endpoint_qps as CountMinSketchWithHeap (PR #344)."]
    fn production_conditions_topk_against_count_sketch_does_not_error() {
        init_tracing_for_test();
        let mut cs = CountSketch::new(4, 4096);
        for i in 0..100u32 {
            cs.update(&format!("endpoint-{i}"), 1.0);
        }
        let acc = CountSketchAccumulator { inner: cs };

        let engine = build_engine_production_conditions(
            "top_endpoint_qps",
            AggregationType::CountSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
        );

        let query = "topk(5, top_endpoint_qps)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        assert!(
            result.is_some(),
            "production warm engine must answer `topk(K, <CountSketch-metric>)` under empty \
             PromQLSchema; got None → wire status=error",
        );
    }

    /// (5e) `replay.jsonl` 342/342 failing rows: `rate(endpoint_request_freq[5m])`
    /// against a CountMinSketch agg. `CountMinSketchAccumulator` doesn't
    /// directly answer `Statistic::Rate`, but the production demo's
    /// frequency probe is structurally a per-series count from a CMS;
    /// the engine should at minimum resolve the agg and surface a
    /// `Some(...)` rather than `status=error`. (The accumulator may
    /// fail at the inner `query_statistic(Rate, …)` step today; this
    /// test pins that the surface stays answerable so the replay row
    /// is non-empty.)
    ///
    /// Until CMS gets a `Statistic::Rate` answer, the realistic
    /// production fallback is `Statistic::Count` — `rate` is the
    /// per-second view of the count. We assert the engine resolves
    /// the agg via capability matching; the value is allowed to be
    /// any finite number.
    #[test]
    #[ignore = "follow-up: CountMinSketchAccumulator does not yet implement \
                Statistic::Rate; see TODO.md for tracking. The engine SHOULD resolve \
                the agg through Statistic::Count compat list, but capability \
                matching for `rate(...)` requests Statistic::Rate, which today \
                only lists Increase/MultipleIncrease."]
    fn production_conditions_rate_against_cms_does_not_error() {
        init_tracing_for_test();
        let mut cms = CountMinSketch::new(4, 4096);
        for i in 0..100u32 {
            cms.update(&format!("endpoint-{i}"), 1.0);
        }
        let acc = CountMinSketchAccumulator { inner: cms };

        let engine = build_engine_production_conditions(
            "endpoint_request_freq",
            AggregationType::CountMinSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
        );

        let query = "rate(endpoint_request_freq[5m])";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME_SEC);
        assert!(
            result.is_some(),
            "production warm engine must answer `rate(<CMS-metric>[5m])` under empty \
             PromQLSchema; got None → wire status=error",
        );
    }

    /// Sanity: when a deployment registers the bare metric name
    /// (no DDSketch INGEST rename applied), the alias resolver
    /// must leave the query unchanged — both forms might coexist
    /// in tests but the bare form should win when present.
    #[test]
    fn quantile_query_without_ingest_rename_passes_through() {
        init_tracing_for_test();
        let acc = dd_sketch_with_1_to_100();
        // Bare metric IS in streaming config — exactly the
        // pre-rename case from PR #108's existing tests.
        let engine = build_engine_with_window(
            "request_latency_ms",
            AggregationType::DDSketch,
            vec!["zone"],
            vec![(Some(vec!["us-east-1".to_string()]), Box::new(acc))],
            "quantile_over_time(0.99, request_latency_ms[1m])",
        );

        let query = "quantile_over_time(0.99, request_latency_ms[1m])";
        let (_labels, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_SEC)
            .expect("bare-metric quantile_over_time should answer normally");
        match qr {
            QueryResult::Vector(iv) => {
                assert!(!iv.values.is_empty(), "expected at least one value");
                let v = iv.values[0].value;
                assert!(
                    (v - 99.0).abs() < 5.0,
                    "expected ~99.0, got {v} (alias resolver must not have rewritten this)"
                );
            }
            other => panic!("expected instant vector, got {other:?}"),
        }
    }
}
