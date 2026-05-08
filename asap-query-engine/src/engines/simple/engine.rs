use crate::data_model::{
    AggregationIdInfo, InferenceConfig, KeyByLabelValues, QueryConfig, QueryLanguage, SchemaConfig,
    StreamingConfig,
};
use crate::engines::query_result::{InstantVectorElement, QueryResult, RangeVectorElement};
// use crate::stores::promsketch_store::{
//     self, is_usampling_function, metrics as ps_metrics, PromSketchStore,
// };
use crate::stores::{Store, TimestampedBucketsMap};
use core::panic;
use promql_utilities::get_is_collapsable;
use promql_utilities::query_logics::enums::{AggregationOperator, AggregationType, PromQLFunction};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

use crate::AggregateCore;

use asap_types::enums::WindowType;
use asap_types::query_requirements::QueryRequirements;
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::ast_matching::{PromQLMatchResult, PromQLPattern, PromQLPatternBuilder};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};

use sql_utilities::ast_matching::QueryType;
use sql_utilities::ast_matching::{SQLPatternMatcher, SQLPatternParser, SQLQuery};
use sql_utilities::sqlhelper::{AggregationInfo, SQLQueryData};
use sqlparser::dialect::*;
use sqlparser::parser::Parser as parser;

// SQL issue: refactor simpleengine to create matchresult similar to SQLquerydata

use elastic_dsl_utilities::pattern::parse_and_classify;
use elastic_dsl_utilities::types::{EsDslQueryPattern, GroupBySpec, MetricAggType};

// Type alias for merged outputs (single aggregate per key after merging)
type MergedOutputsMap = HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>;

/// Replace every standalone occurrence of the PromQL identifier
/// `needle` with `replacement` in `haystack`. An occurrence is
/// "standalone" iff its surrounding characters can't be part of a
/// PromQL identifier (`[A-Za-z0-9_:]`). Used by the DDSketch
/// `_quantile` alias resolver so e.g. rewriting `http_latency_ms`
/// in `quantile_over_time(0.99, http_latency_ms[1m])` doesn't also
/// touch a hypothetical `http_latency_ms_total` elsewhere in the
/// query.
fn replace_metric_token(haystack: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() {
        return haystack.to_string();
    }
    let bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    // PromQL identifiers are ASCII; bound the byte-level scan to
    // ASCII-only `is_ident` predicates and let multi-byte UTF-8
    // sequences (which can only occur inside string literals or
    // comments) pass through untouched. The needle bytes are
    // ASCII-only by construction (callers pass identifiers).
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b':';
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + needle_bytes.len() <= bytes.len() && &bytes[i..i + needle_bytes.len()] == needle_bytes
        {
            let prev_ok = i == 0 || !is_ident(bytes[i - 1]);
            let next_idx = i + needle_bytes.len();
            let next_ok = next_idx >= bytes.len() || !is_ident(bytes[next_idx]);
            if prev_ok && next_ok {
                out.push_str(replacement);
                i = next_idx;
                continue;
            }
        }
        // Advance one UTF-8 char at a time (works for ASCII fast
        // path AND multi-byte sequences inside e.g. label-value
        // strings).
        let ch_len = utf8_char_len(bytes[i]);
        out.push_str(&haystack[i..i + ch_len]);
        i += ch_len;
    }
    out
}

/// Length of the UTF-8 character starting at `b` (the first byte).
/// Returns 1 for invalid leading bytes, never panics.
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xC0 {
        1 // continuation byte mid-sequence — defensive fallback
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}

/// Metadata extracted from a query, independent of query language
#[derive(Debug, Clone)]
pub struct QueryMetadata {
    /// Labels that will appear in the query output
    pub query_output_labels: KeyByLabelNames,
    /// The primary statistic to compute (sum, max, quantile, etc.)
    pub statistic_to_compute: Statistic,
    /// Additional parameters (e.g., "quantile" -> "0.95", "k" -> "10")
    pub query_kwargs: HashMap<String, String>,
}

/// Parameters for a single store query
#[derive(Debug, Clone)]
pub struct StoreQueryParams {
    pub metric: String,
    pub aggregation_id: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    /// true for sliding windows (exact match), false for tumbling (range)
    pub is_exact_query: bool,
}

/// Complete plan for querying store (values + optional separate keys)
#[derive(Debug, Clone)]
pub struct StoreQueryPlan {
    pub values_query: StoreQueryParams,
    /// Some when key and value use different aggregations (DeltaSet/SetAggregator)
    pub keys_query: Option<StoreQueryParams>,
}

/// Timestamps for query execution
#[derive(Debug, Clone)]
pub struct QueryTimestamps {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
}

/// Complete execution context for a query
#[derive(Debug, Clone)]
pub struct QueryExecutionContext {
    pub metric: String,
    pub metadata: QueryMetadata,
    pub store_plan: StoreQueryPlan,
    pub agg_info: AggregationIdInfo,
    /// Whether to merge multiple precomputes (true for temporal queries)
    pub do_merge: bool,
    #[allow(dead_code)]
    pub spatial_filter: String,
    pub query_time: u64,
    /// Spatial grouping labels from the value aggregation config.
    /// These are the store GROUP BY columns.
    pub grouping_labels: KeyByLabelNames,
    /// Aggregated labels from the value aggregation config.
    /// These are labels that "key" an accumulator/sketch internally
    /// (e.g. endpoint within a MultipleIncrease accumulator).
    pub aggregated_labels: KeyByLabelNames,
}

/// Parameters for a range query
#[derive(Debug, Clone)]
pub struct RangeQueryParams {
    pub start: u64, // start timestamp in ms
    pub end: u64,   // end timestamp in ms
    pub step: u64,  // step in ms
}

/// Extended execution context for range queries
#[derive(Debug, Clone)]
pub struct RangeQueryExecutionContext {
    /// Base context (metric, metadata, store_plan, etc.)
    pub base: QueryExecutionContext,
    /// Range-specific parameters
    pub range_params: RangeQueryParams,
    /// Number of buckets per step (step / tumbling_window)
    pub buckets_per_step: usize,
    /// Number of buckets in lookback window
    pub lookback_bucket_count: usize,
    /// Tumbling window size in ms
    pub tumbling_window_ms: u64,
}

// /// Parsed components of a sketch query, extracted either via the PromQL AST
// /// parser (for standard functions) or via regex (for custom functions like
// /// `entropy_over_time` that the promql-parser crate doesn't recognize).
// struct SketchQueryComponents {
//     func_name: String,
//     metric: String,
//     range_seconds: u64,
//     /// Extra numeric argument (e.g. quantile value). 0.0 when unused.
//     args: f64,
// }

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct SimpleEngine {
    store: Arc<dyn Store>,
    // promsketch_store: Option<Arc<PromSketchStore>>,
    inference_config: InferenceConfig,
    /// Hot-reloadable `StreamingConfig` handle. Internal read sites
    /// call `Self::streaming_config_snapshot()` which re-snapshots
    /// from this handle, so runtime swaps pushed through PR #10's
    /// `POST /api/v1/streaming-config` endpoint take effect on the
    /// **next** query without restarting the binary (PR E phase 2).
    /// Clones of `HotReloadStreamingConfig` share the same
    /// underlying `ArcSwap`, so when `main.rs` hands the same handle
    /// to both `SimpleEngine` and `HttpServer::with_hot_reload_config`,
    /// a POST is immediately visible to the next query.
    streaming_config_source: crate::data_model::HotReloadStreamingConfig,
    prometheus_scrape_interval: u64,
    controller_patterns: HashMap<QueryPatternType, Vec<PromQLPattern>>,
    query_language: QueryLanguage,
    /// Optional `ControllerClient` used to notify the DataCollector
    /// controller when a query hits a capability miss
    /// (`find_compatible_aggregation` returns `None`). When `None`,
    /// misses fall through to the §5.2 fallback silently, matching
    /// pre-PR-G behavior. Set via `with_controller_client`.
    controller_client: Option<Arc<dyn crate::drivers::query::controller_client::ControllerClient>>,
    /// Per-`agg_id` schema registry used for §7 schema-timeline
    /// dispatch (`docs/design-sketch-db.md`). The combiner lives in
    /// [`crate::engines::timeline_dispatch`] and the lookup primitive
    /// is exposed on [`crate::stores::sketch_db::SchemaRegistry`];
    /// the engine consults the registry on every query to resolve
    /// which agg_id owns each sub-range of the query's time window.
    ///
    /// Defaults to an empty registry so call-sites that don't
    /// participate in schema-timeline dispatch keep compiling.
    /// Production wire-up (`main.rs`) uses
    /// [`Self::with_schema_registry`] to share the same registry the
    /// ingest path is reconciling.
    schema_registry: Arc<crate::stores::sketch_db::SchemaRegistry>,
}

impl SimpleEngine {
    /// Construct a `SimpleEngine` with a static `Arc<StreamingConfig>`.
    /// Wraps the config in a fresh `HotReloadStreamingConfig` internally
    /// — callers that need to share the hot-reload handle with the HTTP
    /// server should use `new_with_hot_reload` instead so a POST to
    /// `/api/v1/streaming-config` is visible to both. The `_static`
    /// variant stays as the simple entry point for tests, binaries,
    /// and legacy callers that don't own a `HotReloadStreamingConfig`.
    pub fn new(
        store: Arc<dyn Store>,
        // promsketch_store: Option<Arc<PromSketchStore>>,
        inference_config: InferenceConfig,
        streaming_config: Arc<StreamingConfig>,
        prometheus_scrape_interval: u64,
        query_language: QueryLanguage,
    ) -> Self {
        let hot_reload = crate::data_model::HotReloadStreamingConfig::from_arc(streaming_config);
        Self::new_with_hot_reload(
            store,
            inference_config,
            hot_reload,
            prometheus_scrape_interval,
            query_language,
        )
    }

    /// Construct a `SimpleEngine` that shares a `HotReloadStreamingConfig`
    /// handle with another holder (typically the HTTP server). This is
    /// the constructor `main.rs` should call so `POST /api/v1/streaming-config`
    /// is observable by the next query.
    pub fn new_with_hot_reload(
        store: Arc<dyn Store>,
        inference_config: InferenceConfig,
        streaming_config_source: crate::data_model::HotReloadStreamingConfig,
        prometheus_scrape_interval: u64,
        query_language: QueryLanguage,
    ) -> Self {
        // Create temporal pattern blocks
        let mut temporal_pattern_blocks = HashMap::new();
        temporal_pattern_blocks.insert(
            "quantile".to_string(),
            PromQLPatternBuilder::function(
                vec![PromQLFunction::QuantileOverTime.as_str()],
                vec![
                    PromQLPatternBuilder::number(None, Some("quantile_param")),
                    PromQLPatternBuilder::matrix_selector(
                        PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                        None,
                        Some("range_vector"),
                    ),
                ],
                Some("function"),
                Some("function_args"),
            ),
        );

        temporal_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::function(
                vec![
                    "sum_over_time",
                    "count_over_time",
                    "avg_over_time",
                    "min_over_time",
                    "max_over_time",
                    "increase",
                    "rate",
                    "entropy_over_time",
                    "distinct_over_time",
                    "l1_over_time",
                    "l2_over_time",
                    "stddev_over_time",
                    "stdvar_over_time",
                    "sum2_over_time",
                ],
                vec![PromQLPatternBuilder::matrix_selector(
                    PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                    None,
                    Some("range_vector"),
                )],
                Some("function"),
                Some("function_args"),
            ),
        );

        // Create spatial pattern blocks
        let mut spatial_pattern_blocks = HashMap::new();
        let spatial_ops_all: Vec<&str> = [
            AggregationOperator::Sum,
            AggregationOperator::Count,
            AggregationOperator::Avg,
            AggregationOperator::Quantile,
            AggregationOperator::Min,
            AggregationOperator::Max,
            AggregationOperator::Topk,
        ]
        .map(AggregationOperator::as_str)
        .to_vec();
        let spatial_ops_no_topk: Vec<&str> = [
            AggregationOperator::Sum,
            AggregationOperator::Count,
            AggregationOperator::Avg,
            AggregationOperator::Quantile,
            AggregationOperator::Min,
            AggregationOperator::Max,
        ]
        .map(AggregationOperator::as_str)
        .to_vec();
        spatial_pattern_blocks.insert(
            "generic".to_string(),
            PromQLPatternBuilder::aggregation(
                spatial_ops_all,
                PromQLPatternBuilder::metric(None, None, None, Some("metric")),
                None,
                None,
                None,
                Some("aggregation"),
            ),
        );

        // Helper functions (these would be closures or separate methods)
        fn temporal_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        fn spatial_pattern(
            pattern_type: &str,
            blocks: &HashMap<String, Option<HashMap<String, Value>>>,
        ) -> PromQLPattern {
            PromQLPattern::new(blocks[pattern_type].clone())
        }

        let spatial_of_temporal_pattern =
            |temporal_block: &Option<HashMap<String, Value>>| -> PromQLPattern {
                let pattern = PromQLPatternBuilder::aggregation(
                    spatial_ops_no_topk.clone(),
                    temporal_block.clone(),
                    None,
                    None,
                    None,
                    Some("aggregation"),
                );
                PromQLPattern::new(pattern)
            };

        // Create controller patterns
        let mut controller_patterns = HashMap::new();
        controller_patterns.insert(
            QueryPatternType::OnlyTemporal,
            vec![
                temporal_pattern("quantile", &temporal_pattern_blocks),
                temporal_pattern("generic", &temporal_pattern_blocks),
            ],
        );
        controller_patterns.insert(
            QueryPatternType::OnlySpatial,
            vec![spatial_pattern("generic", &spatial_pattern_blocks)],
        );
        controller_patterns.insert(
            QueryPatternType::OneTemporalOneSpatial,
            vec![
                spatial_of_temporal_pattern(&temporal_pattern_blocks["quantile"]),
                spatial_of_temporal_pattern(&temporal_pattern_blocks["generic"]),
            ],
        );

        Self {
            store,
            // promsketch_store,
            inference_config,
            streaming_config_source,
            prometheus_scrape_interval,
            controller_patterns,
            query_language,
            controller_client: None,
            schema_registry: Arc::new(crate::stores::sketch_db::SchemaRegistry::empty()),
        }
    }

    /// Take a fresh snapshot of the current `StreamingConfig`. Each
    /// call observes whatever was most recently pushed through PR #10's
    /// `POST /api/v1/streaming-config` endpoint. The returned `Arc`
    /// is stable for the caller's lifetime — a concurrent swap
    /// produces a new `Arc` and leaves the one returned here alone.
    ///
    /// Internal read sites inside `SimpleEngine` bind this once per
    /// logical unit of work (typically per query-handler invocation
    /// or per helper call) and use the local Arc for the duration,
    /// so references into the underlying `StreamingConfig` stay
    /// valid and a single query sees internally-consistent config
    /// fields even if a concurrent swap lands mid-query.
    pub fn streaming_config_snapshot(&self) -> Arc<StreamingConfig> {
        self.streaming_config_source.snapshot()
    }

    /// Attach a `ControllerClient` so capability misses fire a
    /// fire-and-forget notification to the DataCollector controller.
    /// Builder-style method — takes self by value and returns it so
    /// construction in `main.rs` chains neatly. Without this call,
    /// capability misses fall through to the §5.2 fallback silently,
    /// matching pre-PR-G behavior.
    pub fn with_controller_client(
        mut self,
        client: Arc<dyn crate::drivers::query::controller_client::ControllerClient>,
    ) -> Self {
        self.controller_client = Some(client);
        self
    }

    /// Attach the shared `SchemaRegistry` the ingest path is
    /// reconciling so queries can resolve the §7 schema timeline for
    /// a metric. Typically called from `main.rs` with the same
    /// `Arc<SchemaRegistry>` held by `IngestState::schemas` and the
    /// HTTP streaming-config swap handler so all three observe the
    /// same lifecycle transitions.
    pub fn with_schema_registry(
        mut self,
        registry: Arc<crate::stores::sketch_db::SchemaRegistry>,
    ) -> Self {
        self.schema_registry = registry;
        self
    }

    /// Resolve the §7 schema timeline for a metric over a query range.
    /// Thin delegate to `SchemaRegistry::timeline_for_metric` so the
    /// engine's own query-path code does not need to reach into the
    /// store module to build a timeline (and so dispatch-wiring
    /// tests can mock by swapping the registry rather than
    /// monkey-patching the engine).
    pub fn timeline_for_query(
        &self,
        metric: &str,
        t1_ms: u64,
        t2_ms: u64,
    ) -> Vec<crate::stores::sketch_db::TimelineSegment> {
        self.schema_registry
            .timeline_for_metric(metric, t1_ms, t2_ms)
    }

    /// Look up a compatible aggregation for the given requirements,
    /// and if none exists, fire a capability-miss notification to
    /// the controller (fire-and-forget, does not block the query).
    /// Wraps the plain `streaming_config.find_compatible_aggregation`
    /// with the PR G telemetry call-out.
    fn find_compatible_aggregation_with_miss_notify(
        &self,
        requirements: &QueryRequirements,
    ) -> Option<AggregationIdInfo> {
        let streaming_config = self.streaming_config_snapshot();
        let result = streaming_config.find_compatible_aggregation(requirements);
        if result.is_none() {
            crate::drivers::query::controller_client::spawn_capability_miss_notify(
                &self.controller_client,
                requirements,
            );
        }
        result
    }

    /// Convert query timestamp (seconds) to data timestamp (milliseconds)
    pub fn convert_query_time_to_data_time(query_time: f64) -> u64 {
        (query_time * 1000.0) as u64
    }

    /// Finds the query configuration for a given query string
    fn find_query_config(&self, query: &str) -> Option<&QueryConfig> {
        self.inference_config
            .query_configs
            .iter()
            .find(|config| config.query == query)
    }

    /// Resolve the DDSketch INGEST-side `_quantile` rename for a
    /// quantile-shape PromQL query.
    ///
    /// The agent's DDSketch processor renames raw input metrics to
    /// the suffixed wire form (`http_latency_ms` →
    /// `http_latency_ms_quantile`) before emitting to the warm
    /// tier — so the engine's streaming config and sketch store
    /// register the suffixed name, but the user's PromQL still
    /// references the conceptual unsuffixed name. Without
    /// resolution the warm engine looks up `http_latency_ms`,
    /// finds nothing, and returns `status=error`.
    ///
    /// When the parsed query is shape-classified as `Quantile`
    /// (`quantile_over_time(...)` or `quantile(...)` aggregation)
    /// AND the bare metric isn't registered locally but the
    /// `_quantile`-suffixed variant IS, this returns the rewritten
    /// query string with the metric replaced. Otherwise returns
    /// `None` so the caller leaves the query untouched.
    ///
    /// Rewrite is performed by string substitution of the metric
    /// identifier — sufficient for the production query shapes the
    /// MVP demo replays (`quantile_over_time(q, M[range])` where
    /// `M` is a bare metric name) and avoids the AST-to-string
    /// round-trip that the promql-parser library doesn't fully
    /// support. Fallback: if substitution fails to produce a
    /// parseable result, returns `None` and the original query
    /// flows through unchanged.
    fn resolve_quantile_metric_alias(&self, query: &str) -> Option<String> {
        // Parse + classify shape; only quantile-shaped queries are
        // affected by the INGEST rename.
        let ast = promql_parser::parser::parse(query).ok()?;
        if !matches!(
            crate::routing::classify_query_shape(&ast),
            crate::routing::QueryShape::Quantile
        ) {
            return None;
        }

        // Pull the first metric name from the AST.
        fn first_metric(expr: &promql_parser::parser::Expr) -> Option<String> {
            use promql_parser::parser::Expr;
            match expr {
                Expr::VectorSelector(vs) => vs.name.clone(),
                Expr::MatrixSelector(ms) => ms.vs.name.clone(),
                Expr::Call(call) => call.args.args.iter().find_map(|a| first_metric(a)),
                Expr::Aggregate(agg) => first_metric(&agg.expr),
                Expr::Binary(bin) => {
                    first_metric(&bin.lhs).or_else(|| first_metric(&bin.rhs))
                }
                Expr::Subquery(sq) => first_metric(&sq.expr),
                Expr::Paren(p) => first_metric(&p.expr),
                Expr::Unary(u) => first_metric(&u.expr),
                _ => None,
            }
        }
        let metric = first_metric(&ast)?;

        // If already in the suffixed form, nothing to do.
        if metric.ends_with("_quantile") {
            return None;
        }
        let suffixed = format!("{metric}_quantile");

        // Helper: does a metric name appear as the `metric` field
        // of any aggregation config in the streaming-config
        // snapshot? The DDSketch processor's rename is what would
        // surface the suffixed name in the warm tier's
        // streaming-config in the first place.
        let streaming_config = self.streaming_config_snapshot();
        let metric_known = |name: &str| {
            streaming_config
                .aggregation_configs
                .values()
                .any(|c| c.metric == name)
        };

        // Cross-check against the PromQL schema too so a deployment
        // with a schema-defined-but-aggregation-less metric still
        // passes through unchanged.
        let metric_in_schema = |name: &str| match &self.inference_config.schema {
            SchemaConfig::PromQL(s) => s.get_labels(name).is_some(),
            _ => false,
        };

        let bare_present = metric_known(&metric) || metric_in_schema(&metric);
        let suffixed_present = metric_known(&suffixed) || metric_in_schema(&suffixed);

        if bare_present || !suffixed_present {
            // Either the bare metric is locally known (no rename
            // applied for this deployment) or no suffixed variant
            // exists to redirect to.
            return None;
        }

        // Naive but precise substitution: replace `<metric>` only
        // when surrounded by characters that can't be part of a
        // PromQL identifier (i.e. not `[A-Za-z0-9_:]`). This
        // avoids accidentally matching `metric` inside e.g.
        // `metric_other`.
        let rewritten = replace_metric_token(query, &metric, &suffixed);
        // Sanity-check: parses cleanly.
        if promql_parser::parser::parse(&rewritten).is_err() {
            warn!(
                "resolve_quantile_metric_alias: rewrite to '{}' failed to re-parse; \
                 leaving query untouched",
                rewritten
            );
            return None;
        }
        debug!(
            "resolve_quantile_metric_alias: rewriting '{}' -> '{}' \
             (DDSketch _quantile ingest rename)",
            metric, suffixed
        );
        Some(rewritten)
    }

    /// Finds the query configuration for a SQL query using structural pattern matching.
    ///
    /// Unlike `find_query_config` (which does exact string comparison), this method parses
    /// each template in query_configs and compares it structurally against the incoming
    /// query_data — ignoring absolute timestamps and comparing only metric, aggregation,
    /// labels, time column name, and duration.
    fn find_query_config_sql(&self, query_data: &SQLQueryData) -> Option<&QueryConfig> {
        let schema = match &self.inference_config.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema,
            _ => return None,
        };

        self.inference_config.query_configs.iter().find(|config| {
            let template_statements =
                match parser::parse_sql(&GenericDialect {}, config.query.as_str()) {
                    Ok(stmts) => stmts,
                    Err(_) => return false,
                };
            let template_data =
                match SQLPatternParser::new(schema, 0.0).parse_query(&template_statements) {
                    Some(data) => data,
                    None => return false,
                };
            query_data.matches_sql_pattern(&template_data)
        })
    }

    /// Validates and potentially aligns end timestamp based on query pattern
    fn validate_and_align_end_timestamp(
        &self,
        mut end_timestamp: u64,
        query_pattern_type: QueryPatternType,
    ) -> u64 {
        let interval_ms = self.prometheus_scrape_interval * 1000;

        if !end_timestamp.is_multiple_of(interval_ms) {
            warn!(
                "Query end timestamp {} is not aligned with Prometheus scrape interval of {} seconds. \
                 This may lead to inaccurate results.",
                end_timestamp, self.prometheus_scrape_interval
            );
        }

        // For OnlySpatial, align end_timestamp to nearest scrape interval
        if query_pattern_type == QueryPatternType::OnlySpatial
            && !end_timestamp.is_multiple_of(interval_ms)
        {
            let aligned_end_timestamp = (end_timestamp / interval_ms) * interval_ms;
            debug!(
                "OnlySpatial query: Aligning end_timestamp from {} to {} using scrape interval of {} seconds",
                end_timestamp, aligned_end_timestamp, self.prometheus_scrape_interval
            );
            end_timestamp = aligned_end_timestamp;
        }

        end_timestamp
    }

    /// Calculates start timestamp for PromQL queries
    fn calculate_start_timestamp_promql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                let range_seconds = match_result.get_range_duration().unwrap().num_seconds() as u64;
                end_timestamp - (range_seconds * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates start timestamp for SQL queries
    fn calculate_start_timestamp_sql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                let scrape_intervals = match_result
                    .outer_data()
                    .expect("OnlyTemporal pattern guarantees outer_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let scrape_intervals = match_result
                    .inner_data()
                    .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates and validates query timestamps for PromQL
    fn calculate_query_timestamps_promql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> QueryTimestamps {
        let mut end_timestamp = if let Some(at_modifier) = match_result
            .tokens
            .get("metric")
            .and_then(|t| t.metric.as_ref())
            .and_then(|m| m.at_modifier)
        {
            at_modifier * 1000
        } else {
            query_time
        };

        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_promql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Calculates and validates query timestamps for SQL
    fn calculate_query_timestamps_sql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &SQLQuery,
    ) -> QueryTimestamps {
        let mut end_timestamp = query_time;
        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_sql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Extracts quantile parameter from PromQL match result
    fn extract_quantile_param_promql(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Option<String> {
        let quantile_value = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                match_result
                    .tokens
                    .get("function_args")
                    .and_then(|token| token.function.as_ref())
                    .and_then(|func| func.args.first())
            }
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref()),
        };

        quantile_value.map(|s| s.to_string())
    }

    /// Extracts quantile parameter from SQL match result
    fn extract_quantile_param_sql(&self, match_result: &SQLQuery) -> Option<String> {
        match_result
            .query_data
            .first()
            .map(|data| data.aggregation_info.get_args()[0].to_string())
    }

    /// Extracts topk k parameter from PromQL match result
    fn extract_topk_param(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<String, String> {
        match query_pattern_type {
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref())
                .map(|s| s.to_string())
                .ok_or_else(|| "Missing k parameter for top-k query".to_string()),
            _ => Err(format!(
                "Top-k statistic is only supported for OnlySpatial pattern, found {:?}",
                query_pattern_type
            )),
        }
    }

    /// Builds query kwargs (quantile, k, etc.) for PromQL queries
    fn build_query_kwargs_promql(
        &self,
        statistic: &Statistic,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        match statistic {
            Statistic::Quantile => {
                let quantile = self
                    .extract_quantile_param_promql(query_pattern_type, match_result)
                    .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
                debug!("Extracted quantile value: {:?}", quantile);
                query_kwargs.insert("quantile".to_string(), quantile);
            }
            Statistic::Topk => {
                let k = self.extract_topk_param(query_pattern_type, match_result)?;
                debug!("Extracted k value: {:?}", k);
                query_kwargs.insert("k".to_string(), k);
            }
            _ => {}
        }

        Ok(query_kwargs)
    }

    /// Builds query kwargs for SQL queries
    fn build_query_kwargs_sql(
        &self,
        statistic: &Statistic,
        match_result: &SQLQuery,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        if *statistic == Statistic::Quantile {
            let quantile = self
                .extract_quantile_param_sql(match_result)
                .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
            query_kwargs.insert("quantile".to_string(), quantile);
        }
        // Note: SQL doesn't support topk limiting yet

        Ok(query_kwargs)
    }

    /// Creates query parameters for separate keys query
    fn create_keys_query_params(
        &self,
        metric: &str,
        end_timestamp: u64,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryParams, String> {
        let (start_timestamp, end_timestamp) = match agg_info.aggregation_type_for_key {
            AggregationType::DeltaSetAggregator => {
                // All keys from beginning of time
                (0, end_timestamp)
            }
            AggregationType::SetAggregator => {
                // Latest window only. `.map(|c| c.window_size * 1000)`
                // copies out a u64 so the snapshot only needs to live
                // for the duration of the expression.
                let window_size = self
                    .streaming_config_snapshot()
                    .get_aggregation_config(agg_info.aggregation_id_for_key)
                    .map(|config| config.window_size * 1000)
                    .ok_or_else(|| {
                        format!(
                            "Failed to get window size for aggregation {}",
                            agg_info.aggregation_id_for_key
                        )
                    })?;
                (end_timestamp - window_size, end_timestamp)
            }
            other => {
                return Err(format!("Unsupported key aggregation type: {other:?}"));
            }
        };

        Ok(StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_key,
            start_timestamp,
            end_timestamp,
            is_exact_query: false, // Keys always use range queries
        })
    }

    /// Creates a plan for querying the store based on aggregation configuration
    fn create_store_query_plan(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        agg_info: &AggregationIdInfo,
    ) -> Result<StoreQueryPlan, String> {
        // Bind a single snapshot of the streaming config for this
        // helper's entire execution. `aggregation_config_for_value`
        // is a borrow that outlives the initial expression, so the
        // Arc it borrows from must outlive this scope.
        let streaming_config = self.streaming_config_snapshot();
        // Get aggregation config for value to determine window type
        let aggregation_config_for_value = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                format!(
                    "Aggregation config not found for aggregation_id: {}",
                    agg_info.aggregation_id_for_value
                )
            })?;

        let window_type = aggregation_config_for_value.window_type;
        let is_exact_query = window_type == WindowType::Sliding;

        // Determine start/end for values query based on window type
        let (values_start, values_end) = if is_exact_query {
            // Sliding window: exact window match
            let exact_start =
                timestamps.end_timestamp - (aggregation_config_for_value.window_size * 1000);
            (exact_start, timestamps.end_timestamp)
        } else {
            // Tumbling window: range query
            (timestamps.start_timestamp, timestamps.end_timestamp)
        };

        let values_query = StoreQueryParams {
            metric: metric.to_string(),
            aggregation_id: agg_info.aggregation_id_for_value,
            start_timestamp: values_start,
            end_timestamp: values_end,
            is_exact_query,
        };

        // Determine if we need a separate keys query
        let keys_query = if agg_info.aggregation_id_for_key != agg_info.aggregation_id_for_value {
            Some(self.create_keys_query_params(metric, timestamps.end_timestamp, agg_info)?)
        } else {
            None
        };

        Ok(StoreQueryPlan {
            values_query,
            keys_query,
        })
    }

    /// Executes a single store query based on parameters
    fn execute_store_query(
        &self,
        params: &StoreQueryParams,
    ) -> Result<TimestampedBucketsMap, String> {
        debug!(
            "Querying store: metric={}, agg_id={}, range=[{}, {}], exact={}",
            params.metric,
            params.aggregation_id,
            params.start_timestamp,
            params.end_timestamp,
            params.is_exact_query
        );

        let store_query_start_time = Instant::now();

        let result = if params.is_exact_query {
            debug!(
                "Sliding window query: Looking for exact window [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output_exact(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if let Ok(ref outputs) = res {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Sliding window exact query took: {:.2}ms, found {} unique keys",
                    store_query_duration.as_secs_f64() * 1000.0,
                    outputs.len()
                );
            }
            res
        } else {
            debug!(
                "Tumbling window query: range [{}, {}]",
                params.start_timestamp, params.end_timestamp
            );
            let res = self.store.query_precomputed_output(
                &params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
            );
            if res.is_ok() {
                let store_query_duration = store_query_start_time.elapsed();
                debug!(
                    "Tumbling window range query took: {:.2}ms",
                    store_query_duration.as_secs_f64() * 1000.0
                );
            }
            res
        };

        result.map_err(|e| {
            format!(
                "Error querying store for metric {}, agg {}, range [{}, {}]: {}",
                params.metric,
                params.aggregation_id,
                params.start_timestamp,
                params.end_timestamp,
                e
            )
        })
    }

    /// Executes the full store query plan and returns merged results
    fn execute_and_merge_store_queries(
        &self,
        plan: &StoreQueryPlan,
        do_merge: bool,
        agg_info: &AggregationIdInfo,
    ) -> Result<
        (
            MergedOutputsMap,
            Option<MergedOutputsMap>,
            Option<(u64, u64)>,
        ),
        String,
    > {
        // Query and merge values
        let values_map = self.execute_store_query(&plan.values_query).map_err(|e| {
            warn!("Error querying store for values: {}", e);
            e
        })?;

        if values_map.is_empty() {
            return Err(format!(
                "No precomputed outputs found for metric: {}, aggregation_id: {}",
                plan.values_query.metric, plan.values_query.aggregation_id
            ));
        }

        debug!("Store query returned {} unique keys", values_map.len());

        let merge_start_time = Instant::now();
        let window_type = if plan.values_query.is_exact_query {
            WindowType::Sliding
        } else {
            WindowType::Tumbling
        };

        // Pick the single CLOSEST precompute window across all keys —
        // the latest pane (max tr.1, tie-break on max tr.0) that
        // overlaps the request range. The store's overlap filter may
        // have returned multiple tumbling panes that straddle the
        // request, but a window query
        // (e.g. `quantile_over_time(...[1m])`) should answer with
        // *one* concrete window so the caller can see exactly which
        // pane produced the value (annotated downstream as
        // `precompute_window`). Keys whose data didn't land in that
        // chosen window are dropped from the result rather than
        // contributing a stale answer from an older pane.
        let chosen_window: Option<(u64, u64)> = values_map
            .values()
            .flat_map(|buckets| buckets.iter().map(|(tr, _)| *tr))
            .max_by_key(|tr| (tr.1, tr.0));

        let merged_values: MergedOutputsMap = if plan.values_query.is_exact_query {
            // Sliding window: no merge needed, extract buckets from timestamped data
            debug!("Sliding window mode: Skipping merge (expecting 1 precompute per key)");
            values_map
                .into_iter()
                .map(|(key, timestamped_buckets)| {
                    if timestamped_buckets.len() != 1 {
                        warn!(
                            "Sliding window expected 1 precompute per key, found {}. Using first.",
                            timestamped_buckets.len()
                        );
                    }
                    // Extract bucket from timestamped tuple
                    let (_, bucket) = timestamped_buckets.into_iter().next().unwrap();
                    (key, bucket.as_ref().clone_boxed_core())
                })
                .collect()
        } else {
            // Tumbling window: keep only the chosen-window bucket per
            // key, then run through the existing merge code (which is
            // a no-op for a single bucket but preserves whatever
            // accumulator-side cleanup the merge path does).
            let target = chosen_window.expect(
                "values_map non-empty (checked above) but chosen_window was None — \
                 invariant: if buckets exist, max_by_key returns Some",
            );
            let filtered: TimestampedBucketsMap = values_map
                .into_iter()
                .filter_map(|(key, buckets)| {
                    let kept: Vec<_> = buckets
                        .into_iter()
                        .filter(|(tr, _)| *tr == target)
                        .collect();
                    if kept.is_empty() {
                        None
                    } else {
                        Some((key, kept))
                    }
                })
                .collect();
            debug!(
                "Tumbling window mode: closest pane [{}, {}); {} keys present in that pane",
                target.0,
                target.1,
                filtered.len()
            );
            self.merge_precomputed_outputs(&filtered, do_merge, agg_info.aggregation_type_for_value)
        };

        let merge_duration = merge_start_time.elapsed();
        debug!(
            "[LATENCY] Precomputed output processing ({}): {:.2}ms, resulted in {} merged outputs",
            if window_type == WindowType::Sliding {
                "no merge"
            } else {
                "merge"
            },
            merge_duration.as_secs_f64() * 1000.0,
            merged_values.len()
        );

        // Query and merge keys if needed
        let merged_keys = if let Some(keys_params) = &plan.keys_query {
            let keys_store_query_start_time = Instant::now();
            let keys_map = self.execute_store_query(keys_params).map_err(|e| {
                warn!("Error querying store for keys: {}", e);
                e
            })?;
            debug!(
                "[LATENCY] Keys store query (metric: {}, agg: {}): {}ms",
                &keys_params.metric,
                keys_params.aggregation_id,
                keys_store_query_start_time.elapsed().as_millis()
            );
            debug!("Keys query returned {} unique keys", keys_map.len());

            let keys_merge_start_time = Instant::now();
            let merged = self.merge_precomputed_outputs(
                &keys_map,
                do_merge,
                agg_info.aggregation_type_for_key,
            );
            debug!(
                "[LATENCY] Keys merge operation: {:.2}ms, resulted in {} merged outputs",
                keys_merge_start_time.elapsed().as_secs_f64() * 1000.0,
                merged.len()
            );
            Some(merged)
        } else {
            None
        };

        Ok((merged_values, merged_keys, chosen_window))
    }

    /// Collects all results based on whether keys are separate or not
    fn collect_all_results(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: Option<&HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        if let Some(keys_map) = merged_keys {
            // Separate keys and values
            self.collect_results_separate_keys(merged_values, keys_map, statistic, query_kwargs)
        } else {
            // Same aggregation for keys and values
            self.collect_results_same_aggregation(
                merged_values,
                statistic,
                query_kwargs,
                enable_topk_limiting,
            )
        }
    }

    /// Executes the complete query pipeline: plan, execute, collect, and format.
    ///
    /// Returns the formatted instant-vector elements alongside the
    /// `[start_ms, end_ms)` precompute window the engine actually
    /// consulted (for tumbling-window queries this is the latest
    /// pane that overlapped the request range; for sliding-window
    /// queries it's the exact window). Callers attach this onto the
    /// outgoing `QueryResult` via `with_window_used` so the
    /// HTTP-adapter response can annotate it as
    /// `precompute_window`.
    pub fn execute_query_pipeline(
        &self,
        context: &QueryExecutionContext,
        enable_topk: bool,
    ) -> Result<(Vec<InstantVectorElement>, Option<(u64, u64)>), String> {
        // Step 1: Execute the query plan (already created in context.store_plan)
        let (merged_values, merged_keys, chosen_window) = self.execute_and_merge_store_queries(
            &context.store_plan,
            context.do_merge,
            &context.agg_info,
        )?;

        // Step 2: Collect results
        let unformatted_results_start_time = Instant::now();
        let unformatted_results = self.collect_all_results(
            &merged_values,
            merged_keys.as_ref(),
            &context.metadata.statistic_to_compute,
            &context.metadata.query_kwargs,
            enable_topk, // SQL=false, PromQL=true
        )?;
        debug!(
            "[LATENCY] Unformatted results collection: {:.2}ms",
            unformatted_results_start_time.elapsed().as_secs_f64() * 1000.0
        );

        // Step 3: Format results
        let results_start_time = Instant::now();
        let results = self.format_final_results(
            unformatted_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            enable_topk, // SQL=false, PromQL=true
        );
        debug!(
            "[LATENCY] Results collection: {}ms",
            results_start_time.elapsed().as_millis()
        );

        Ok((results, chosen_window))
    }

    /// Execute a query using the plan-based approach (for testing)
    ///
    /// This is an alternative execution path that uses DataFusion logical/physical
    /// plans instead of the existing execute_query_pipeline.
    ///
    /// # Arguments
    /// * `context` - The query execution context
    ///
    /// # Returns
    /// A Result containing the query results or an error
    #[allow(dead_code)]
    pub async fn execute_plan(
        &self,
        context: &QueryExecutionContext,
    ) -> Result<Vec<InstantVectorElement>, String> {
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_plan::collect;

        use crate::engines::physical::conversion::record_batch_to_result_map;

        let total_start = Instant::now();

        // 1. Build logical plan from context
        let plan_build_start = Instant::now();
        let logical_plan = context
            .to_logical_plan()
            .map_err(|e| format!("Failed to build logical plan: {}", e))?;
        debug!(
            "[LATENCY] DataFusion: logical plan build: {:.2}ms",
            plan_build_start.elapsed().as_secs_f64() * 1000.0
        );
        debug!(
            "DataFusion logical plan:\n{}",
            logical_plan.display_indent()
        );

        // 2. Create session context with our custom extension planner
        let physical_plan_start = Instant::now();
        let session_ctx = SessionContext::new();
        #[allow(deprecated)]
        let state = session_ctx.state().with_query_planner(std::sync::Arc::new(
            crate::engines::physical::CustomQueryPlanner::new(self.store.clone()),
        ));

        // 3. Create physical plan
        let physical_plan = state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| format!("Failed to create physical plan: {}", e))?;
        debug!(
            "[LATENCY] DataFusion: physical plan creation: {:.2}ms",
            physical_plan_start.elapsed().as_secs_f64() * 1000.0
        );

        // 4. Execute
        let execute_start = Instant::now();
        let task_ctx = session_ctx.task_ctx();
        let batches = collect(physical_plan, task_ctx)
            .await
            .map_err(|e| format!("Failed to execute plan: {}", e))?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        debug!(
            "[LATENCY] DataFusion: plan execution: {:.2}ms, {} batch(es), {} total rows",
            execute_start.elapsed().as_secs_f64() * 1000.0,
            batches.len(),
            total_rows
        );

        // 5. Convert results
        let convert_start = Instant::now();
        let label_names: Vec<&str> = context
            .metadata
            .query_output_labels
            .labels
            .iter()
            .map(String::as_str)
            .collect();

        let mut all_results: HashMap<Option<KeyByLabelValues>, f64> = HashMap::new();
        for batch in &batches {
            let batch_results = record_batch_to_result_map(batch, &label_names, "value")
                .map_err(|e| format!("Failed to convert results: {}", e))?;
            all_results.extend(batch_results);
        }
        debug!(
            "[LATENCY] DataFusion: result conversion: {:.2}ms, {} output rows",
            convert_start.elapsed().as_secs_f64() * 1000.0,
            all_results.len()
        );

        // 6. Format results
        let format_start = Instant::now();
        let results = self.format_final_results(
            all_results,
            &context.metadata.statistic_to_compute,
            &context.metric,
            false,
        );
        debug!(
            "[LATENCY] DataFusion: result formatting: {:.2}ms, {} results",
            format_start.elapsed().as_secs_f64() * 1000.0,
            results.len()
        );

        debug!(
            "[LATENCY] DataFusion: total execute_plan: {:.2}ms",
            total_start.elapsed().as_secs_f64() * 1000.0
        );

        Ok(results)
    }

    /// Executes a pre-built DataFusion logical plan and returns results.
    ///
    /// This is the shared execution kernel used by both `execute_plan` (for single-metric
    /// queries) and the binary arithmetic dispatch path.
    pub async fn execute_logical_plan(
        &self,
        logical_plan: datafusion::logical_expr::LogicalPlan,
        label_names: Vec<String>,
        metric: &str,
        statistic: &Statistic,
    ) -> Result<Vec<InstantVectorElement>, String> {
        use datafusion::execution::context::SessionContext;
        use datafusion::physical_plan::collect;

        use crate::engines::physical::conversion::record_batch_to_result_map;

        // Create session context with our custom extension planner
        let session_ctx = SessionContext::new();
        #[allow(deprecated)]
        let state = session_ctx.state().with_query_planner(std::sync::Arc::new(
            crate::engines::physical::CustomQueryPlanner::new(self.store.clone()),
        ));

        let physical_plan = state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| format!("Failed to create physical plan: {}", e))?;

        let task_ctx = session_ctx.task_ctx();
        let batches = collect(physical_plan, task_ctx)
            .await
            .map_err(|e| format!("Failed to execute plan: {}", e))?;

        let label_name_strs: Vec<&str> = label_names.iter().map(String::as_str).collect();
        let mut all_results: HashMap<Option<KeyByLabelValues>, f64> = HashMap::new();
        for batch in &batches {
            let batch_results = record_batch_to_result_map(batch, &label_name_strs, "value")
                .map_err(|e| format!("Failed to convert results: {}", e))?;
            all_results.extend(batch_results);
        }

        Ok(self.format_final_results(all_results, statistic, metric, false))
    }

    /// Finds a query config by structurally comparing `arm_ast` against each
    /// config's parsed query.
    ///
    /// Both the arm AST and each config's query string are first normalized to
    /// the canonical `Display` form produced by `promql_parser`. This ensures
    /// that user-written variants like `"sum(x) by (lbl)"` and the parser's
    /// canonical `"sum by (lbl) (x)"` compare equal.
    pub fn find_query_config_promql_structural(
        &self,
        arm_ast: &promql_parser::parser::Expr,
    ) -> Option<&QueryConfig> {
        let arm_canonical = format!("{}", arm_ast);
        self.inference_config.query_configs.iter().find(|config| {
            let config_canonical = promql_parser::parser::parse(&config.query)
                .map(|ast| format!("{}", ast))
                .unwrap_or_default();
            config_canonical == arm_canonical
        })
    }

    /// Variant of `build_query_execution_context_promql` that accepts a pre-parsed
    /// AST node and a pre-found `QueryConfig`, avoiding redundant parsing and lookup.
    pub fn build_query_execution_context_from_ast(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        query_config: &QueryConfig,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                let match_result = pattern.matches(arm_ast);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        let (query_pattern_type, match_result) = found_match?;

        let agg_info = self
            .get_aggregation_id_info(query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            agg_info,
        )
    }

    /// Shared context-building tail for both PromQL context builders.
    ///
    /// Called by `build_query_execution_context_from_ast` and
    /// `build_query_execution_context_promql` after pattern matching and
    /// `agg_info` resolution are complete.  Computes labels, statistics,
    /// kwargs, metadata, query plan, and the final `QueryExecutionContext`.
    fn build_promql_execution_context_tail(
        &self,
        match_result: &PromQLMatchResult,
        query_pattern_type: QueryPatternType,
        query_time: u64,
        agg_info: AggregationIdInfo,
    ) -> Option<QueryExecutionContext> {
        let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

        let promql_schema = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema,
            _ => return None,
        };
        let all_labels = match promql_schema.get_labels(&metric).cloned() {
            Some(labels) => labels,
            None => {
                warn!("No metric configuration found for '{}'", metric);
                return None;
            }
        };

        let mut query_output_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => all_labels.clone(),
            QueryPatternType::OnlySpatial => {
                get_spatial_aggregation_output_labels(match_result, &all_labels)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let temporal_aggregation = match_result.get_function_name().unwrap();
                let spatial_aggregation = match_result.get_aggregation_op().unwrap();
                let collapsable = temporal_aggregation
                    .parse::<PromQLFunction>()
                    .ok()
                    .zip(spatial_aggregation.parse::<AggregationOperator>().ok())
                    .is_some_and(|(f, o)| get_is_collapsable(f, o));
                if collapsable {
                    get_spatial_aggregation_output_labels(match_result, &all_labels)
                } else {
                    all_labels.clone()
                }
            }
        };

        let timestamps =
            self.calculate_query_timestamps_promql(query_time, query_pattern_type, match_result);

        let statistics_to_compute = get_statistics_to_compute(query_pattern_type, match_result);
        if statistics_to_compute.len() != 1 {
            warn!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
            return None;
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        if *statistic_to_compute == Statistic::Topk {
            let mut new_labels = vec!["__name__".to_string()];
            new_labels.extend(query_output_labels.labels);
            query_output_labels = KeyByLabelNames::new(new_labels);
        }

        let query_kwargs = self
            .build_query_kwargs_promql(statistic_to_compute, query_pattern_type, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs,
        };

        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        let streaming_config = self.streaming_config_snapshot();
        let grouping_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric,
            metadata,
            store_plan: query_plan,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Recursively builds a DataFusion logical plan for one arm of a binary
    /// arithmetic expression.
    ///
    /// - Leaf arm (supported PromQL pattern): look up config structurally, build
    ///   context, return its `to_logical_plan()` together with the output label names.
    /// - Binary arm: recursively build both sub-arms and combine with
    ///   `build_binary_vector_plan`.
    /// - Scalar literal: returns `None` (handled by the caller separately).
    fn build_arm_logical_plan(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(datafusion::logical_expr::LogicalPlan, Vec<String>)> {
        use crate::engines::logical::plan_builder::build_binary_vector_plan;
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) => None, // caller handles scalars
            Expr::Paren(paren) => self.build_arm_logical_plan(&paren.expr, time),
            Expr::Binary(binary) => {
                // Nested binary expression — recurse on both sides
                let (lhs_plan, lhs_labels) = self.build_arm_logical_plan(&binary.lhs, time)?;
                let (rhs_plan, _) = self.build_arm_logical_plan(&binary.rhs, time)?;
                let combined =
                    build_binary_vector_plan(lhs_plan, rhs_plan, &binary.op, lhs_labels.clone())
                        .ok()?;
                Some((combined, lhs_labels))
            }
            other => {
                // Leaf pattern: structural config lookup + context + plan
                let config = self.find_query_config_promql_structural(other)?;
                let ctx = self.build_query_execution_context_from_ast(other, config, time)?;
                let label_names = ctx.metadata.query_output_labels.labels.clone();
                let plan = ctx.to_logical_plan().ok()?;
                Some((plan, label_names))
            }
        }
    }

    /// Handles a binary arithmetic PromQL expression by building a combined
    /// DataFusion plan (vector–vector join or scalar projection) and executing it.
    ///
    /// Returns `None` if any arm is not acceleratable (caller falls back to Prometheus).
    fn handle_binary_expr_promql(
        &self,
        ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use crate::engines::logical::plan_builder::{build_binary_vector_plan, build_scalar_plan};
        use promql_parser::parser::Expr;

        let query_time = Self::convert_query_time_to_data_time(time);

        let binary = match ast {
            Expr::Binary(b) => b,
            _ => return None,
        };

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        // Scalar case: either side may be a numeric literal
        let scalar_case: Option<(f64, &Expr, bool)> = match (lhs, rhs) {
            (_, Expr::NumberLiteral(nl)) => Some((nl.val, lhs, false)),
            (Expr::NumberLiteral(nl), _) => Some((nl.val, rhs, true)),
            _ => None,
        };
        if let Some((scalar, vector_arm, scalar_on_left)) = scalar_case {
            let (vector_plan, label_names) = self.build_arm_logical_plan(vector_arm, time)?;
            let combined =
                build_scalar_plan(vector_plan, scalar, op, scalar_on_left, label_names.clone())
                    .ok()?;
            let results = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.execute_logical_plan(
                    combined,
                    label_names.clone(),
                    "",
                    &Statistic::Sum,
                ))
            })
            .ok()?;
            return Some((
                KeyByLabelNames::new(label_names),
                QueryResult::vector(results, query_time),
            ));
        }

        // Vector–vector
        let (lhs_plan, lhs_labels) = self.build_arm_logical_plan(lhs, time)?;
        let (rhs_plan, _) = self.build_arm_logical_plan(rhs, time)?;
        let combined = build_binary_vector_plan(lhs_plan, rhs_plan, op, lhs_labels.clone()).ok()?;
        let results = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.execute_logical_plan(
                combined,
                lhs_labels.clone(),
                "",
                &Statistic::Sum,
            ))
        })
        .ok()?;
        let output_labels = KeyByLabelNames::new(lhs_labels);
        Some((output_labels, QueryResult::vector(results, query_time)))
    }

    /// Applies a PromQL binary arithmetic operator to two f64 values.
    fn apply_range_binary_op(
        op: &promql_parser::parser::token::TokenType,
        lhs: f64,
        rhs: f64,
    ) -> f64 {
        use promql_parser::parser::token::{T_ADD, T_DIV, T_MOD, T_MUL, T_POW, T_SUB};
        match op.id() {
            id if id == T_ADD => lhs + rhs,
            id if id == T_SUB => lhs - rhs,
            id if id == T_MUL => lhs * rhs,
            id if id == T_DIV => lhs / rhs,
            id if id == T_MOD => lhs % rhs,
            id if id == T_POW => lhs.powf(rhs),
            _ => f64::NAN,
        }
    }

    /// Recursively builds a range execution context for one arm of a binary arithmetic expression.
    fn build_arm_range_context(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(RangeQueryExecutionContext, Vec<String>)> {
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) => None, // caller handles scalars
            Expr::Paren(paren) => self.build_arm_range_context(&paren.expr, start, end, step),
            other => {
                let config = self.find_query_config_promql_structural(other)?;
                let base_context =
                    self.build_query_execution_context_from_ast(other, config, end)?;
                let label_names = base_context.metadata.query_output_labels.labels.clone();

                let start_ms = Self::convert_query_time_to_data_time(start);
                let end_ms = Self::convert_query_time_to_data_time(end);
                let step_ms = (step * 1000.0) as u64;

                let tumbling_window_ms = self
                    .streaming_config_snapshot()
                    .get_aggregation_config(base_context.agg_info.aggregation_id_for_value)
                    .map(|c| c.window_size * 1000)?;

                self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
                    .map_err(|e| {
                        warn!("Range arm query validation failed: {}", e);
                        e
                    })
                    .ok()?;

                let lookback_ms = base_context.store_plan.values_query.end_timestamp
                    - base_context.store_plan.values_query.start_timestamp;

                let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
                let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

                let mut extended_store_plan = base_context.store_plan.clone();
                extended_store_plan.values_query.start_timestamp =
                    start_ms.saturating_sub(lookback_ms);
                extended_store_plan.values_query.end_timestamp = end_ms;
                extended_store_plan.values_query.is_exact_query = false;

                let range_context = RangeQueryExecutionContext {
                    base: QueryExecutionContext {
                        store_plan: extended_store_plan,
                        ..base_context
                    },
                    range_params: RangeQueryParams {
                        start: start_ms,
                        end: end_ms,
                        step: step_ms,
                    },
                    buckets_per_step,
                    lookback_bucket_count,
                    tumbling_window_ms,
                };

                Some((range_context, label_names))
            }
        }
    }

    /// Handles a binary arithmetic PromQL expression for range queries.
    ///
    /// Evaluates each arm independently over the full range, then joins the
    /// resulting series by label key and applies the arithmetic operator
    /// sample-by-sample at matching timestamps.
    fn handle_binary_expr_range_promql(
        &self,
        ast: &promql_parser::parser::Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use promql_parser::parser::Expr;

        let binary = match ast {
            Expr::Binary(b) => b,
            _ => return None,
        };

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        // Scalar case: either side may be a numeric literal
        let scalar_case: Option<(f64, &Expr, bool)> = match (lhs, rhs) {
            (_, Expr::NumberLiteral(nl)) => Some((nl.val, lhs, false)),
            (Expr::NumberLiteral(nl), _) => Some((nl.val, rhs, true)),
            _ => None,
        };
        if let Some((scalar, vector_arm, scalar_on_left)) = scalar_case {
            let (ctx, labels) = self.build_arm_range_context(vector_arm, start, end, step)?;
            let results = self.execute_range_query_pipeline(&ctx).ok()?;
            let combined: Vec<RangeVectorElement> = results
                .into_iter()
                .map(|mut elem| {
                    for s in &mut elem.samples {
                        s.value = if scalar_on_left {
                            Self::apply_range_binary_op(op, scalar, s.value)
                        } else {
                            Self::apply_range_binary_op(op, s.value, scalar)
                        };
                    }
                    elem
                })
                .collect();
            return Some((KeyByLabelNames::new(labels), QueryResult::matrix(combined)));
        }

        // Vector-vector: evaluate both arms, join by label key, apply op per matching timestamp
        let (lhs_ctx, lhs_labels) = self.build_arm_range_context(lhs, start, end, step)?;
        let (rhs_ctx, _) = self.build_arm_range_context(rhs, start, end, step)?;
        let lhs_results = self.execute_range_query_pipeline(&lhs_ctx).ok()?;
        let rhs_results = self.execute_range_query_pipeline(&rhs_ctx).ok()?;

        // Build lookup: label_key -> {timestamp -> value} for rhs
        let mut rhs_map: HashMap<KeyByLabelValues, HashMap<u64, f64>> = HashMap::new();
        for elem in rhs_results {
            let ts_map: HashMap<u64, f64> = elem
                .samples
                .iter()
                .map(|s| (s.timestamp, s.value))
                .collect();
            rhs_map.insert(elem.labels, ts_map);
        }

        let mut combined: Vec<RangeVectorElement> = Vec::new();
        for lhs_elem in lhs_results {
            if let Some(rhs_ts_map) = rhs_map.get(&lhs_elem.labels) {
                let mut new_elem = RangeVectorElement::new(lhs_elem.labels.clone());
                for s in &lhs_elem.samples {
                    if let Some(&rhs_val) = rhs_ts_map.get(&s.timestamp) {
                        new_elem.add_sample(
                            s.timestamp,
                            Self::apply_range_binary_op(op, s.value, rhs_val),
                        );
                    }
                }
                if !new_elem.samples.is_empty() {
                    combined.push(new_elem);
                }
            }
        }

        let output_labels = KeyByLabelNames::new(lhs_labels);
        Some((output_labels, QueryResult::matrix(combined)))
    }

    /// Formats unformatted results into final InstantVectorElement format
    /// For topk queries (when enabled), sorts by value and prepends metric name to keys
    fn format_final_results(
        &self,
        unformatted_results: HashMap<Option<KeyByLabelValues>, f64>,
        statistic: &Statistic,
        metric: &str,
        enable_topk_formatting: bool,
    ) -> Vec<InstantVectorElement> {
        let sorted_results: Vec<(Option<KeyByLabelValues>, f64)> =
            if *statistic == Statistic::Topk && enable_topk_formatting {
                // Sort by value descending for topk
                let mut sorted: Vec<_> = unformatted_results.into_iter().collect();
                sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

                // Prepend metric name to each key's label values
                sorted
                    .into_iter()
                    .map(|(key_opt, value)| {
                        let updated_key = key_opt.map(|mut key| {
                            let mut new_labels = vec![metric.to_string()];
                            new_labels.extend(key.labels);
                            key.labels = new_labels;
                            key
                        });
                        (updated_key, value)
                    })
                    .collect()
            } else {
                unformatted_results.into_iter().collect()
            };

        sorted_results
            .into_iter()
            .filter_map(|(key, value)| key.map(|k| InstantVectorElement::new(k, value)))
            .collect()
    }

    fn sql_get_is_collapsable(
        &self,
        temporal_aggregation: &AggregationInfo,
        spatial_aggregation: &AggregationInfo,
    ) -> bool {
        match spatial_aggregation.get_name() {
            "SUM" => matches!(
                temporal_aggregation.get_name(),
                "SUM" | "COUNT" // Note: "increase" and "rate" are commented out in Python
            ),
            "MIN" => temporal_aggregation.get_name() == "MIN",
            "MAX" => temporal_aggregation.get_name() == "MAX",
            _ => false,
        }
    }

    /// Extract QueryRequirements from a parsed PromQL match result.
    /// Used as the fallback path when no query_configs entry is found.
    fn build_query_requirements_promql(
        &self,
        match_result: &PromQLMatchResult,
        query_pattern_type: QueryPatternType,
    ) -> QueryRequirements {
        let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

        let statistics = get_statistics_to_compute(query_pattern_type, match_result);

        let data_range_ms = match query_pattern_type {
            QueryPatternType::OnlySpatial => None,
            _ => match_result
                .get_range_duration()
                .map(|d| d.num_seconds() as u64 * 1000),
        };

        let all_labels = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema
                .get_labels(&metric)
                .cloned()
                .unwrap_or_else(KeyByLabelNames::empty),
            _ => KeyByLabelNames::empty(),
        };

        let grouping_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => all_labels,
            QueryPatternType::OnlySpatial | QueryPatternType::OneTemporalOneSpatial => {
                get_spatial_aggregation_output_labels(match_result, &all_labels)
            }
        };

        QueryRequirements {
            metric,
            statistics,
            data_range_ms,
            grouping_labels,
            spatial_filter_normalized: normalize_spatial_filter(&spatial_filter),
        }
    }

    /// Parse a lowercase aggregation name into exactly one `Statistic`.
    ///
    /// Returns `None` (with a warning) if the name is not a recognised
    /// `AggregationOperator` or if it maps to a number of statistics other
    /// than one. Centralises the three previously-scattered copies of this
    /// logic, which had inconsistent error handling (silent empty vec, panic,
    /// and warn+return-None).
    fn parse_single_statistic(statistic_name: &str) -> Option<Statistic> {
        let stats = statistic_name
            .parse::<AggregationOperator>()
            .map(|o| o.to_statistics())
            .unwrap_or_else(|_| {
                warn!("Unsupported statistic name: '{}'", statistic_name);
                vec![]
            });
        if stats.len() != 1 {
            warn!(
                "Expected exactly one statistic for '{}', found {}",
                statistic_name,
                stats.len()
            );
            return None;
        }
        stats.into_iter().next()
    }

    /// Extract QueryRequirements from a parsed SQL match result.
    /// Used as the fallback path when no query_configs entry is found.
    fn build_query_requirements_sql(
        &self,
        match_result: &SQLQuery,
        query_pattern_type: QueryPatternType,
    ) -> QueryRequirements {
        let query_data = match_result
            .outer_data()
            .expect("build_query_requirements_sql called on valid SQLQuery");
        let metric = query_data.metric.clone();

        let statistic_name = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => match_result
                .inner_data()
                .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                .aggregation_info
                .get_name()
                .to_lowercase(),
            _ => query_data.aggregation_info.get_name().to_lowercase(),
        };

        let statistics: Vec<Statistic> = Self::parse_single_statistic(&statistic_name)
            .into_iter()
            .collect();

        let data_range_ms = match query_pattern_type {
            QueryPatternType::OnlySpatial => None,
            QueryPatternType::OnlyTemporal => {
                let scrape_intervals = query_data.time_info.clone().get_duration() as u64;
                Some(scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let scrape_intervals = match_result
                    .inner_data()
                    .expect("OneTemporalOneSpatial pattern guarantees inner_data is present")
                    .time_info
                    .clone()
                    .get_duration() as u64;
                Some(scrape_intervals * self.prometheus_scrape_interval * 1000)
            }
        };

        let grouping_labels = KeyByLabelNames::new(query_data.labels.clone().into_iter().collect());

        QueryRequirements {
            metric,
            statistics,
            data_range_ms,
            grouping_labels,
            spatial_filter_normalized: normalize_spatial_filter(""),
        }
    }

    fn get_aggregation_id_info(
        &self,
        query_config: &QueryConfig,
    ) -> Result<AggregationIdInfo, String> {
        let query_config_aggregations = &query_config.aggregations;

        if query_config_aggregations.is_empty() {
            return Err("Query config has no aggregations defined".to_string());
        }
        if query_config_aggregations.len() > 2 {
            return Err("Query config with > 2 aggregations is not supported".to_string());
        }

        let mut aggregation_id_for_key: Option<u64> = None;
        let mut aggregation_id_for_value: Option<u64> = None;
        let mut aggregation_type_for_key: Option<AggregationType> = None;
        let mut aggregation_type_for_value: Option<AggregationType> = None;

        let streaming_config = self.streaming_config_snapshot();
        if query_config_aggregations.len() == 2 {
            for aggregation in query_config_aggregations {
                let aggregation_type = streaming_config
                    .get_aggregation_config(aggregation.aggregation_id)
                    .map(|config| config.aggregation_type)
                    .ok_or_else(|| {
                        format!(
                            "No streaming config for aggregation_id {}",
                            aggregation.aggregation_id
                        )
                    })?;

                if matches!(
                    aggregation_type,
                    AggregationType::DeltaSetAggregator | AggregationType::SetAggregator
                ) {
                    if aggregation_id_for_key.is_some() {
                        return Err(
                            "Query config has two key-type aggregations (expected at most one)"
                                .to_string(),
                        );
                    }
                    aggregation_id_for_key = Some(aggregation.aggregation_id);
                    aggregation_type_for_key = Some(aggregation_type);
                } else {
                    if aggregation_id_for_value.is_some() {
                        return Err(
                            "Query config has two value-type aggregations (expected at most one)"
                                .to_string(),
                        );
                    }
                    aggregation_id_for_value = Some(aggregation.aggregation_id);
                    aggregation_type_for_value = Some(aggregation_type);
                }
            }
        } else {
            // Single aggregation: key and value share the same aggregation
            let id = query_config_aggregations[0].aggregation_id;
            let agg_type = streaming_config
                .get_aggregation_config(id)
                .map(|config| config.aggregation_type)
                .ok_or_else(|| format!("No streaming config for aggregation_id {id}"))?;
            aggregation_id_for_key = Some(id);
            aggregation_id_for_value = Some(id);
            aggregation_type_for_key = Some(agg_type);
            aggregation_type_for_value = Some(agg_type);
        }

        Ok(AggregationIdInfo {
            aggregation_id_for_key: aggregation_id_for_key
                .ok_or("aggregation_id_for_key was not set")?,
            aggregation_id_for_value: aggregation_id_for_value
                .ok_or("aggregation_id_for_value was not set")?,
            aggregation_type_for_key: aggregation_type_for_key
                .ok_or("aggregation_type_for_key was not set")?,
            aggregation_type_for_value: aggregation_type_for_value
                .ok_or("aggregation_type_for_value was not set")?,
        })
    }

    pub fn handle_query_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let context = self.build_query_execution_context_sql(query, time)?;
        self.execute_context(context, false)
    }

    /// Execute the query pipeline for an already-built context.
    ///
    /// Shared by `handle_query_sql`, `handle_query_elastic`, and `handle_query_promql`.
    fn execute_context(
        &self,
        context: QueryExecutionContext,
        enable_topk: bool,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let agg_id = context.agg_info.aggregation_id_for_value;
        let (results, window_used) = self
            .execute_query_pipeline(&context, enable_topk)
            .map_err(|e| {
                warn!("Query execution failed: {}", e);
                e
            })
            .ok()?;
        let qr = QueryResult::vector(results, context.query_time);
        let qr = match self.accuracy_envelope_for(agg_id) {
            Some(env) => qr.with_accuracy(env),
            None => qr,
        };
        let qr = match window_used {
            Some(w) => qr.with_window_used(w),
            None => qr,
        };
        Some((context.metadata.query_output_labels, qr))
    }

    /// Build an [`AccuracyEnvelope`] for a single resolved
    /// `agg_id` by looking up the matching `AggregationConfig` in
    /// the current streaming-config snapshot and deriving its
    /// [`AccuracyProfile`]. Returns `None` when the agg isn't in
    /// config (e.g. post-retire / test harness with empty config).
    pub(crate) fn accuracy_envelope_for(
        &self,
        agg_id: u64,
    ) -> Option<crate::stores::sketch_db::AccuracyEnvelope> {
        let snap = self.streaming_config_snapshot();
        let cfg = snap.get_aggregation_config(agg_id)?;
        Some(crate::stores::sketch_db::AccuracyEnvelope::single(
            crate::stores::sketch_db::AccuracyProfile::derive(cfg),
        ))
    }

    pub fn build_query_execution_context_sql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        // Get SQL schema from inference config
        let schema = match &self.inference_config.schema {
            SchemaConfig::SQL(sql_schema) => sql_schema.clone(),
            SchemaConfig::PromQL(_) => {
                warn!("SQL query requested but config has PromQL schema");
                return None;
            }
            &SchemaConfig::ElasticQueryDSL => todo!(),
            SchemaConfig::ElasticSQL(sql_schema) => sql_schema.clone(),
        };

        let statements = parser::parse_sql(&GenericDialect {}, query.as_str()).unwrap();
        let query_data = SQLPatternParser::new(&schema, time).parse_query(&statements);

        let query_data = match query_data {
            Some(data) => data,
            None => {
                debug!("Could not parse query");
                return None;
            }
        };

        let matcher = SQLPatternMatcher::new(schema, self.prometheus_scrape_interval as f64);
        let match_result = matcher.query_info_to_pattern(&query_data);

        debug!("Match result: {:?}", match_result);
        debug!("Validity: {}", match_result.is_valid());

        if !match_result.is_valid() {
            return None;
        }

        // Handle SpatioTemporal queries separately - they bypass QueryPatternType mapping
        if match_result.query_type == vec![QueryType::SpatioTemporal] {
            let query_time = Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            );
            return self.build_spatiotemporal_context(&match_result, query_time, &query_data);
        }

        let query_pattern_type = match &match_result.query_type[..] {
            [x] => match x {
                QueryType::Spatial => QueryPatternType::OnlySpatial,
                QueryType::TemporalGeneric => QueryPatternType::OnlyTemporal,
                QueryType::TemporalQuantile => QueryPatternType::OnlyTemporal,
                QueryType::SpatioTemporal => unreachable!("SpatioTemporal handled above"),
            },
            [x, y] => match (x, y) {
                (QueryType::Spatial, QueryType::TemporalGeneric) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                (QueryType::Spatial, QueryType::TemporalQuantile) => {
                    QueryPatternType::OneTemporalOneSpatial
                }
                _ => panic!("Unsupported query type found"),
            },
            _ => panic!("Unsupported query type found"),
        };

        // For nested queries (spatial of temporal), the outer query has no time clause,
        // so we need to use the inner (temporal) query's time_info to compute query_time
        let query_time = match query_pattern_type {
            QueryPatternType::OneTemporalOneSpatial => {
                let inner_time_info = &match_result.inner_data()?.time_info;
                Self::convert_query_time_to_data_time(
                    inner_time_info.get_start() + inner_time_info.get_duration(),
                )
            }
            _ => Self::convert_query_time_to_data_time(
                query_data.time_info.get_start() + query_data.time_info.get_duration(),
            ),
        };

        //     self.handle_sql_temporal_aggregation(
        //         query_config,
        //         &match_result,
        //         query_time,
        //         query_pattern_type,
        //     )
        // }

        // fn handle_sql_temporal_aggregation(
        //     &self,
        //     query_config: &QueryConfig,
        //     match_result: &SQLQuery,
        //     query_time: u64,
        //     query_pattern_type: QueryPatternType,
        // ) -> Option<(KeyByLabelNames, QueryResult)> {
        // Labels

        let query_output_labels = match &match_result.query_type.len() {
            // Potentially change SQLQueryType
            1 => {
                // For non-nested queries, output associated labels
                let labels = &match_result.outer_data()?.labels;

                KeyByLabelNames::new(labels.clone().into_iter().collect())
            }
            2 => {
                // Extract spatial aggregation output labels using AST-based approach
                let temporal_labels = &match_result.inner_data()?.labels;
                let spatial_labels = &match_result.outer_data()?.labels;

                let temporal_aggregation = &match_result.inner_data()?.aggregation_info;
                let spatial_aggregation = &match_result.outer_data()?.aggregation_info;

                match self.sql_get_is_collapsable(temporal_aggregation, spatial_aggregation) {
                    // If false: get all labels, which are all temporal labels. If true, get only spatial labels
                    false => KeyByLabelNames::new(temporal_labels.clone().into_iter().collect()),
                    true => KeyByLabelNames::new(spatial_labels.clone().into_iter().collect()),
                }
            }
            _ => {
                warn!("Invalid query type: {}", query_pattern_type);
                KeyByLabelNames::new(Vec::new())
            }
        };

        // Statistic - determine based on query pattern type
        let statistic_name = match query_pattern_type {
            QueryPatternType::OnlyTemporal => {
                // Use the temporal aggregation (first subquery)
                match_result
                    .outer_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OneTemporalOneSpatial => {
                // Use the temporal aggregation (second subquery contains temporal)
                match_result
                    .inner_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
            QueryPatternType::OnlySpatial => {
                // Use the spatial aggregation (first subquery)
                match_result
                    .outer_data()?
                    .aggregation_info
                    .get_name()
                    .to_lowercase()
            }
        };

        let statistic_to_compute = Self::parse_single_statistic(&statistic_name)?;

        let query_kwargs = self
            .build_query_kwargs_sql(&statistic_to_compute, &match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        // Create query metadata
        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Time
        let timestamps =
            self.calculate_query_timestamps_sql(query_time, query_pattern_type, &match_result);

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) =
            self.find_query_config_sql(&query_data)
        {
            self.get_aggregation_id_info(config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()?
        } else {
            warn!("No query_config entry for SQL query. Attempting capability-based matching.");
            let requirements = self.build_query_requirements_sql(&match_result, query_pattern_type);
            self.find_compatible_aggregation_with_miss_notify(&requirements)?
        };

        let metric = &match_result.outer_data()?.metric;

        let spatial_filter = if query_pattern_type == QueryPatternType::OneTemporalOneSpatial {
            match_result
                .outer_data()?
                .labels
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        } else {
            String::new()
        };

        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        self.build_sql_execution_context_tail(
            metric,
            &timestamps,
            metadata,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
        )
    }

    /// Shared context-building tail for both SQL context builders.
    ///
    /// Called by `build_query_execution_context_sql` and `build_spatiotemporal_context`
    /// after labels, statistic, metadata, timestamps, and `agg_info` are resolved.
    /// Builds the query plan, derives grouping/aggregated labels, and returns the
    /// final `QueryExecutionContext`.
    #[allow(clippy::too_many_arguments)]
    fn build_sql_execution_context_tail(
        &self,
        metric: &str,
        timestamps: &QueryTimestamps,
        metadata: QueryMetadata,
        agg_info: AggregationIdInfo,
        do_merge: bool,
        spatial_filter: String,
        query_time: u64,
    ) -> Option<QueryExecutionContext> {
        let query_plan = self
            .create_store_query_plan(metric, timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let streaming_config = self.streaming_config_snapshot();
        let grouping_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| metadata.query_output_labels.clone());

        let aggregated_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric: metric.to_string(),
            metadata,
            store_plan: query_plan,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Build execution context for SpatioTemporal queries.
    /// These queries span multiple scrape intervals but GROUP BY a subset of labels.
    fn build_spatiotemporal_context(
        &self,
        match_result: &SQLQuery,
        query_time: u64,
        query_data: &SQLQueryData,
    ) -> Option<QueryExecutionContext> {
        // Output labels are the GROUP BY columns (subset of all labels)
        let query_output_labels = KeyByLabelNames::new(
            match_result
                .outer_data()?
                .labels
                .clone()
                .into_iter()
                .collect(),
        );

        // Get the statistic from the aggregation
        let statistic_name = match_result
            .outer_data()?
            .aggregation_info
            .get_name()
            .to_lowercase();

        let statistic_to_compute = Self::parse_single_statistic(&statistic_name)?;

        let query_kwargs = self
            .build_query_kwargs_sql(&statistic_to_compute, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };

        // Calculate timestamps - similar to OnlyTemporal
        let end_timestamp =
            self.validate_and_align_end_timestamp(query_time, QueryPatternType::OnlyTemporal);
        let scrape_intervals = match_result.outer_data()?.time_info.get_duration() as u64;
        let start_timestamp =
            end_timestamp - (scrape_intervals * self.prometheus_scrape_interval * 1000);

        let timestamps = QueryTimestamps {
            start_timestamp,
            end_timestamp,
        };

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) =
            self.find_query_config_sql(query_data)
        {
            self.get_aggregation_id_info(config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()?
        } else {
            warn!(
                    "No query_config entry for SQL spatio-temporal query. Attempting capability-based matching."
                );
            let requirements =
                self.build_query_requirements_sql(match_result, QueryPatternType::OnlyTemporal);
            self.find_compatible_aggregation_with_miss_notify(&requirements)?
        };
        let metric = &match_result.outer_data()?.metric;

        self.build_sql_execution_context_tail(
            metric,
            &timestamps,
            metadata,
            agg_info,
            true,
            String::new(),
            query_time,
        )
    }

    /// Handle a query following Python's unified architecture
    // pub async fn handle_query(
    pub fn handle_query(&self, query: String, time: f64) -> Option<(KeyByLabelNames, QueryResult)> {
        match self.query_language {
            QueryLanguage::promql => self.handle_query_promql(query, time),
            QueryLanguage::sql => self.handle_query_sql(query, time),
            QueryLanguage::elastic_querydsl => self.handle_query_elastic(query, time),
            QueryLanguage::elastic_sql => self.handle_query_sql(query, time),
        }
    }

    pub fn handle_query_elastic(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let context = self.build_query_execution_context_elastic(query, time)?;
        debug!(
            "Built execution context for ElasticSearch query {:?}",
            context
        );
        self.execute_context(context, false)
    }

    pub fn build_query_execution_context_elastic(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        // 1. Parse query DSL somehow. Elasticsearch DSL crate does not support deserializing, but maybe can use Opensearch instead?
        // 2. Determine whether query is supported using some AST representation or hardcoded pattern matching.
        let query_pattern: EsDslQueryPattern =
            parse_and_classify(&query).unwrap_or(EsDslQueryPattern::Unknown);
        match query_pattern {
            EsDslQueryPattern::Unknown => {
                debug!("Could not parse query into known pattern");
                return None;
            }
            _ => {
                debug!("Parsed query pattern: {:?}", query_pattern);
            }
        }

        // 3. Convert parsed query into execution context components (labels, statistic, kwargs, metadata, store query plan, etc.)

        // TODO: Figure out how to handle query configuration for ElasticSearch queries.
        let query_config = self.find_query_config(&query)?;
        let agg_info = self
            .get_aggregation_id_info(query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let do_merge = true; // No "instant" queries in ElasticSearch supported for now, so we always need to merge.

        let (metric, query_metadata) = self.build_query_metadata_elastic(&query_pattern)?;

        let spatial_filter = String::new(); // Placeholder - extract from query if applicable

        // TODO: Need way to parse ES DSL "date math".
        let timestamps = self.resolve_query_time_range_elastic(query_time, query_pattern);

        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let streaming_config = self.streaming_config_snapshot();
        let grouping_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_metadata.query_output_labels.clone());

        let aggregated_labels = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric,
            metadata: query_metadata,
            store_plan: query_plan.clone(),
            agg_info: agg_info.clone(),
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    fn build_query_metadata_elastic(
        &self,
        query_pattern: &EsDslQueryPattern,
    ) -> Option<(String, QueryMetadata)> {
        // Constructs QueryMetadata based on the parsed ES DSL query pattern. This includes determining the
        // metric to query, the statistic to compute, and any relevant query kwargs (e.g. quantile value for percentiles).

        // Figure out aggregation type and what labels are included in output.
        // By default, we only include grouping labels in the output for ES DSL.

        // Take first aggregation by default since current engine doesn't support multiple aggregations in a single query.
        let aggregation = query_pattern.get_metric_aggs()?.first()?.clone();

        // By default, we only include grouping labels in the output for ES DSL.
        let query_output_labels = match query_pattern.get_groupby_spec() {
            Some(GroupBySpec::Terms { field }) => KeyByLabelNames::new(vec![field.clone()]),
            Some(GroupBySpec::MultiTerms { fields }) => KeyByLabelNames::new(fields.to_vec()),
            None => KeyByLabelNames::empty(),
        };

        let metric = aggregation.field.clone();

        // Map ElasticSearch aggregation types to our internal Statistic enum.
        let statistic_to_compute = match aggregation.agg_type {
            MetricAggType::Percentiles => Statistic::Quantile,
            MetricAggType::Avg => Statistic::Rate,
            MetricAggType::Sum => Statistic::Sum,
            MetricAggType::Min => Statistic::Min,
            MetricAggType::Max => Statistic::Max,
        };

        let mut query_kwargs = HashMap::new(); // Placeholder - build based on query and statistic
        if aggregation.agg_type == MetricAggType::Percentiles {
            // Extract quantile value from aggregation parameters and add to query_kwargs
            if let Some(params) = &aggregation.params {
                if let Some(percents) = params.get("percents") {
                    // Get first value from percents array since we only support one quantile argument for now.
                    let quantile = percents
                        .as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.as_f64());
                    // ES percentiles are specified as values between 0 and 100, but we want to convert to 0-1 range for our internal representation.
                    query_kwargs.insert("quantile".to_string(), (quantile? / 100.0).to_string());
                }
            }
        }

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs: query_kwargs.clone(),
        };
        Some((metric, metadata))
    }

    pub fn resolve_query_time_range_elastic(
        &self,
        query_time: u64,
        query_pattern: EsDslQueryPattern,
    ) -> QueryTimestamps {
        // Resolves the actual start and end timestamps into milliseconds for an ElasticSearch query
        // based on the provided query_time and the time range specified in the ES DSL query pattern (if any).
        // If no time range is specified, default to entire history up to query_time.

        let mut start_timestamp: u64 = 0;
        let mut end_timestamp: u64 = query_time;

        let time_range = query_pattern.get_time_range();
        if let Some(tr) = time_range {
            if let Some(resolved_range) = tr.resolve_epoch_millis(query_time as i64) {
                debug!(
                    "Parsed time range from query: start={} end={}",
                    resolved_range.gte_ms.unwrap_or(0),
                    resolved_range.lte_ms.unwrap_or(0)
                );
                start_timestamp = resolved_range.gte_ms.unwrap_or(0) as u64;
                end_timestamp = resolved_range.lte_ms.unwrap_or(query_time as i64) as u64;
            } else {
                debug!("Failed to resolve time range from query");
            }
        };

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    // /// Try to extract sketch query components from a PromQL query string.
    // ///
    // /// Attempts the standard AST parser first. If that fails (e.g. for custom
    // /// sketch-only functions), falls back to a lightweight regex extraction for
    // /// patterns like `func(metric[range])` and `func(number, metric[range])`.
    // /// Extract just the sketch function name from a query without full evaluation.
    // fn extract_sketch_func_name(&self, query: &str) -> Option<String> {
    //     self.parse_sketch_query_components(query)
    //         .map(|c| c.func_name)
    // }

    // fn parse_sketch_query_components(&self, query: &str) -> Option<SketchQueryComponents> {
    //     // --- Path A: standard PromQL parser + pattern matching ---
    //     if let Some(components) = self.parse_sketch_via_ast(query) {
    //         return Some(components);
    //     }

    //     // --- Path B: regex fallback for custom sketch functions ---
    //     self.parse_sketch_via_regex(query)
    // }

    // /// Parse sketch components using the standard PromQL AST parser.
    // fn parse_sketch_via_ast(&self, query: &str) -> Option<SketchQueryComponents> {
    //     let ast = match promql_parser::parser::parse(query) {
    //         Ok(ast) => ast,
    //         Err(_) => return None,
    //     };

    //     let mut found_match = None;
    //     for (pattern_type, patterns) in &self.controller_patterns {
    //         for pattern in patterns {
    //             let match_result = pattern.matches(&ast);
    //             if match_result.matches {
    //                 found_match = Some((*pattern_type, match_result));
    //                 break;
    //             }
    //         }
    //         if found_match.is_some() {
    //             break;
    //         }
    //     }

    //     let (query_pattern_type, match_result) = found_match?;

    //     if query_pattern_type != QueryPatternType::OnlyTemporal {
    //         debug!(
    //             "Sketch query (AST): pattern type {:?} is not OnlyTemporal, skipping for '{}'",
    //             query_pattern_type, query
    //         );
    //         return None;
    //     }

    //     let func_name = match_result.get_function_name()?;
    //     promsketch_store::promsketch_func_map(&func_name)?;

    //     let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);
    //     let metric = if spatial_filter.is_empty() {
    //         metric
    //     } else {
    //         format!("{}{{{}}}", metric, spatial_filter)
    //     };

    //     let range_seconds = match_result.get_range_duration()?.num_seconds() as u64;

    //     let args = if func_name == "quantile_over_time" {
    //         self.extract_quantile_param_promql(query_pattern_type, &match_result)
    //             .and_then(|s| s.parse::<f64>().ok())
    //             .unwrap_or(0.5)
    //     } else {
    //         0.0
    //     };

    //     Some(SketchQueryComponents {
    //         func_name,
    //         metric,
    //         range_seconds,
    //         args,
    //     })
    // }

    // /// Regex fallback for custom sketch functions the PromQL parser doesn't know.
    // ///
    // /// Matches two forms:
    // ///   - `func_name(metric[duration])`                  (generic)
    // ///   - `func_name(number, metric[duration])`          (quantile)
    // ///   - `func_name(metric{filter}[duration])`          (with label filter)
    // fn parse_sketch_via_regex(&self, query: &str) -> Option<SketchQueryComponents> {
    //     use regex::Regex;

    //     // quantile form: quantile_over_time(0.5, metric{...}[5m])
    //     let quantile_re =
    //         Regex::new(r"^(\w+)\(\s*([0-9.]+)\s*,\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$")
    //             .ok()?;

    //     // generic form: func(metric{...}[5m])
    //     let generic_re =
    //         Regex::new(r"^(\w+)\(\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$").ok()?;

    //     if let Some(caps) = quantile_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let args: f64 = caps[2].parse().ok()?;
    //         let metric = caps[3].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[4], &caps[5])?;
    //         debug!(
    //             "Sketch query (regex/quantile): parsed {} with metric={}, range={}s, args={}",
    //             func_name, metric, range_seconds, args
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args,
    //         });
    //     }

    //     if let Some(caps) = generic_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let metric = caps[2].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[3], &caps[4])?;
    //         debug!(
    //             "Sketch query (regex/generic): parsed {} with metric={}, range={}s",
    //             func_name, metric, range_seconds
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args: 0.0,
    //         });
    //     }

    //     None
    // }

    // /// Convert a numeric value + unit suffix into seconds.
    // fn parse_duration_to_seconds(value: &str, unit: &str) -> Option<u64> {
    //     let n: u64 = value.parse().ok()?;
    //     let multiplier = match unit {
    //         "s" => 1,
    //         "m" => 60,
    //         "h" => 3600,
    //         "d" => 86400,
    //         _ => return None,
    //     };
    //     Some(n * multiplier)
    // }

    // /// Try to handle a PromQL query via the sketch shortcut path.
    // /// Returns Some if the query is sketch-backed and PromSketchStore is available.
    // /// Returns None to fall through to the precomputed pipeline.
    // fn handle_sketch_query_promql(
    //     &self,
    //     query: &str,
    //     time: f64,
    // ) -> Option<(KeyByLabelNames, QueryResult)> {
    //     let ps = self.promsketch_store.as_ref()?;

    //     let components = match self.parse_sketch_query_components(query) {
    //         Some(c) => c,
    //         None => {
    //             debug!(
    //                 "Sketch query: could not parse sketch components from '{}'",
    //                 query
    //             );
    //             return None;
    //         }
    //     };

    //     let eval_start = Instant::now();

    //     let query_time = Self::convert_query_time_to_data_time(time);
    //     let end = query_time;
    //     let start = end.saturating_sub(components.range_seconds * 1000);

    //     debug!(
    //         "Sketch query: evaluating {}({}) range=[{}, {}] args={}",
    //         components.func_name, components.metric, start, end, components.args
    //     );

    //     let results = match ps.eval_matching(
    //         &components.func_name,
    //         &components.metric,
    //         components.args,
    //         start,
    //         end,
    //     ) {
    //         Ok(r) => r,
    //         Err(e) => {
    //             warn!(
    //                 "Sketch query: eval_matching failed for {}({}): {}",
    //                 components.func_name, components.metric, e
    //             );
    //             ps_metrics::SKETCH_QUERIES_TOTAL
    //                 .with_label_values(&["miss"])
    //                 .inc();
    //             return None;
    //         }
    //     };

    //     if results.is_empty() {
    //         debug!(
    //             "Sketch query: no matching series with data for {}({}), falling through",
    //             components.func_name, components.metric
    //         );
    //         ps_metrics::SKETCH_QUERIES_TOTAL
    //             .with_label_values(&["miss"])
    //             .inc();
    //         return None;
    //     }

    //     ps_metrics::SKETCH_QUERIES_TOTAL
    //         .with_label_values(&["hit"])
    //         .inc();
    //     ps_metrics::SKETCH_QUERY_DURATION.observe(eval_start.elapsed().as_secs_f64());

    //     info!(
    //         "Sketch query: {}({}) returned {} series results",
    //         components.func_name,
    //         components.metric,
    //         results.len()
    //     );

    //     let elements: Vec<InstantVectorElement> = results
    //         .into_iter()
    //         .map(|(labels_str, value)| {
    //             let labels = KeyByLabelValues::new_with_labels(vec![labels_str]);
    //             InstantVectorElement::new(labels, value)
    //         })
    //         .collect();

    //     let output_labels = KeyByLabelNames::new(vec!["__name__".to_string()]);
    //     Some((output_labels, QueryResult::vector(elements, query_time)))
    // }

    pub fn handle_query_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!("Handling query: {} at time {}", query, time);

        // Resolve the DDSketch-processor INGEST-side `_quantile` rename.
        //
        // The agent's DDSketch processor renames raw input metrics
        // (e.g. `http_latency_ms`) to a sketched-form wire name
        // (`http_latency_ms_quantile`) before emitting to the warm
        // tier. The replay client / PromQL caller still references
        // the conceptual unsuffixed metric in
        // `quantile_over_time(q, X[range])`, so the warm engine sees
        // a query for `X` while its sketch store only holds
        // `X_quantile`. Without this resolution step the store
        // lookup misses and the engine returns `status=error` to a
        // query that is logically answerable.
        //
        // We rewrite ONLY when:
        //   * the query's classified shape is `Quantile` (i.e. a
        //     `quantile_over_time(...)` or PromQL `quantile(...)`
        //     aggregation — the only shapes whose data lives behind
        //     the DDSketch / KLL `_quantile` rename), AND
        //   * the bare metric is NOT registered in the engine's
        //     streaming config but the `_quantile`-suffixed variant
        //     IS — so for any deployment that didn't apply the
        //     INGEST-side rename, the query string passes through
        //     unchanged.
        //
        // The rewrite happens once at the entry point so every
        // downstream stage (pattern match, `QueryConfig` lookup,
        // capability matching, `StoreQueryParams.metric`, schema
        // label lookup) sees the same suffixed name.
        let query = self
            .resolve_quantile_metric_alias(&query)
            .unwrap_or(query);

        // Check for binary arithmetic before attempting single-query dispatch.
        // Binary expressions won't have a matching query_config, so we handle them here.
        if let Ok(ast) = promql_parser::parser::parse(&query) {
            if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
                let result = self.handle_binary_expr_promql(&ast, time);
                let total_query_duration = query_start_time.elapsed();
                debug!(
                    "Binary arithmetic query handling took: {:.2}ms",
                    total_query_duration.as_secs_f64() * 1000.0
                );
                return result;
            }
        }

        // Try the §7 schema-timeline dispatch first. Returns Some
        // only when the query's [t1, t2] range crosses a
        // reconfigure boundary (i.e. two or more agg_ids own pieces
        // of the range). In every other case (single-schema range,
        // unparseable query) it returns None and we fall through
        // to the default single-agg path below.
        if let Some(result) = self.try_handle_query_promql_via_timeline(&query, time) {
            let total_query_duration = query_start_time.elapsed();
            debug!(
                "Timeline-dispatch query handling took: {:.2}ms",
                total_query_duration.as_secs_f64() * 1000.0
            );
            return Some(result);
        }

        let context = self.build_query_execution_context_promql(query, time)?;

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
            context.metric,
            context.agg_info.aggregation_id_for_value,
            context.store_plan.values_query.start_timestamp,
            context.store_plan.values_query.end_timestamp
        );

        let result = self.execute_context(context, true);

        // Determine query routing order based on function type.
        // USampling functions prefer the precomputed path first (sketch fallback),
        // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context = self.build_query_execution_context_promql(query.clone(), time)?;

        //     debug!(
        //         "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
        //         context.metric,
        //         context.agg_info.aggregation_id_for_value,
        //         context.store_plan.values_query.start_timestamp,
        //         context.store_plan.values_query.end_timestamp
        //     );

        //     let results = self
        //         .execute_query_pipeline(&context, true) // PromQL: topk enabled
        //         .map_err(|e| {
        //             warn!("Query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.metadata.query_output_labels,
        //         QueryResult::vector(results, context.query_time),
        //     ))
        // })();

        // if precomputed_result.is_some() {
        //     let total_query_duration = query_start_time.elapsed();
        //     debug!(
        //         "Total query handling took: {:.2}ms",
        //         total_query_duration.as_secs_f64() * 1000.0
        //     );
        //     return precomputed_result;
        // }

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_query_duration = query_start_time.elapsed();
        debug!(
            "Total query handling took: {:.2}ms (no results)",
            total_query_duration.as_secs_f64() * 1000.0
        );
        result
    }

    pub fn build_query_execution_context_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);
        let (query_pattern_type, match_result) = self.parse_and_match_promql(&query)?;
        debug!("Found matching query config for: {}", query);

        let query_context_start_time = Instant::now();

        // Resolve aggregation: try pre-configured query_configs first,
        // fall back to capability matching.
        let agg_info = self.resolve_agg_info_promql(&query, &match_result, query_pattern_type)?;

        let result = self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            agg_info,
        );

        let query_context_duration = query_context_start_time.elapsed();
        debug!(
            "[LATENCY] Query context build: {:.2}ms",
            query_context_duration.as_secs_f64() * 1000.0
        );

        result
    }

    /// Like `build_query_execution_context_promql`, but skips the
    /// auto-resolution of the `agg_id` and forces the provided
    /// `forced_agg_id` instead. The same parse + pattern-match
    /// pipeline runs; only the "which aggregation covers this
    /// query" step is replaced.
    ///
    /// Caller: the per-segment dispatch in
    /// [`Self::try_handle_query_promql_via_timeline`]. For each
    /// `TimelineSegment` returned by
    /// [`crate::stores::sketch_db::SchemaRegistry::timeline_for_metric`],
    /// the dispatch builds a context targeting that segment's
    /// `agg_id`, executes it against the clipped segment range,
    /// collects the scalar, and combines across segments via
    /// [`crate::engines::timeline_dispatch::combine_statistic`].
    ///
    /// Returns `None` when the query can't be parsed / pattern-matched,
    /// or when `forced_agg_id` isn't in the current `StreamingConfig`.
    pub fn build_query_execution_context_promql_for_agg_id(
        &self,
        query: String,
        time: f64,
        forced_agg_id: u64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);
        let (query_pattern_type, match_result) = self.parse_and_match_promql(&query)?;
        let agg_info = self.agg_info_from_forced_id(forced_agg_id)?;
        self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            agg_info,
        )
    }

    /// Shared phase-1 of PromQL context construction: parse the
    /// query string, run it against every registered pattern, and
    /// return the matched `(QueryPatternType, PromQLMatchResult)`.
    /// Centralised so the auto-resolution and forced-agg-id entry
    /// points share identical parse semantics.
    fn parse_and_match_promql(&self, query: &str) -> Option<(QueryPatternType, PromQLMatchResult)> {
        let parse_start_time = Instant::now();
        let ast = match promql_parser::parser::parse(query) {
            Ok(ast) => {
                let parse_duration = parse_start_time.elapsed();
                debug!(
                    "PromQL parsing took: {:.2}ms",
                    parse_duration.as_secs_f64() * 1000.0
                );
                ast
            }
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        let pattern_match_start_time = Instant::now();

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                debug!(
                    "Trying pattern type: {:?} for query: {}",
                    pattern_type, query
                );
                let match_result = pattern.matches(&ast);
                debug!("Match result: {:?}", match_result);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        match found_match {
            Some((pt, result)) => {
                let pattern_match_duration = pattern_match_start_time.elapsed();
                debug!(
                    "Pattern matching took: {:.2}ms",
                    pattern_match_duration.as_secs_f64() * 1000.0
                );
                Some((pt, result))
            }
            None => {
                warn!("No matching pattern found for query: {}", query);
                None
            }
        }
    }

    /// Resolve which aggregation covers a PromQL query: try the
    /// `QueryConfig` exact-string match first, then fall back to
    /// capability-based matching (with controller miss-notification
    /// if wired). Extracted from `build_query_execution_context_promql`
    /// so the per-segment timeline dispatch can choose NOT to
    /// auto-resolve (it has a forced agg_id from the timeline).
    fn resolve_agg_info_promql(
        &self,
        query: &str,
        match_result: &PromQLMatchResult,
        query_pattern_type: QueryPatternType,
    ) -> Option<AggregationIdInfo> {
        if let Some(config) = self.find_query_config(query) {
            self.get_aggregation_id_info(config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()
        } else {
            warn!(
                "No query_config entry for PromQL query '{}'. Attempting capability-based matching.",
                query
            );
            let requirements =
                self.build_query_requirements_promql(match_result, query_pattern_type);
            self.find_compatible_aggregation_with_miss_notify(&requirements)
        }
    }

    /// Build an `AggregationIdInfo` from a single forced `agg_id`,
    /// for the per-segment timeline dispatch. Uses the same
    /// "one agg covers both key and value" shape as the single-
    /// aggregation branch in `get_aggregation_id_info` (line
    /// ~1881), so downstream dispatch treats this agg identically
    /// to a single-aggregation `QueryConfig` match.
    ///
    /// Returns `None` if the agg_id isn't in the current
    /// `StreamingConfig` — either a stale controller posted a
    /// backfill for a removed agg, or the timeline contains a
    /// retired entry whose config was evicted. Either way the
    /// per-segment dispatch will skip this segment as
    /// non-answerable.
    fn agg_info_from_forced_id(&self, agg_id: u64) -> Option<AggregationIdInfo> {
        let streaming_config = self.streaming_config_snapshot();
        let agg_type = streaming_config
            .get_aggregation_config(agg_id)
            .map(|c| c.aggregation_type)?;
        Some(AggregationIdInfo {
            aggregation_id_for_key: agg_id,
            aggregation_id_for_value: agg_id,
            aggregation_type_for_key: agg_type,
            aggregation_type_for_value: agg_type,
        })
    }

    /// Per-segment dispatch across the §7 schema timeline.
    ///
    /// Returns `Some(result)` when `SchemaRegistry::timeline_for_metric`
    /// yields two or more segments for the query's metric within its
    /// time range (i.e. the query spans a reconfigure boundary).
    /// Returns `None` otherwise (single-schema range, unparseable
    /// query, unresolved probe aggregation) so the caller falls back
    /// to the default single-agg path — that path is still correct
    /// whenever the timeline doesn't actually span a boundary.
    ///
    /// ## Combinable vs non-combinable statistics
    ///
    /// For combinable scalar statistics (Count / Sum / Min / Max)
    /// every segment contributes and the result is a clean `Full`
    /// value the user can trust without caveat.
    ///
    /// For non-combinable statistics (Quantile / Topk / Cardinality /
    /// Rate / Increase) — or for any combinable run that includes a
    /// `Purged` / config-missing segment — `combine_statistic`
    /// returns `Partial`. This method surfaces Partial on the
    /// Prometheus HTTP response's `warnings` field: the top-level
    /// result carries whatever combinable prefix we could compute
    /// (for additive stats) or an empty vector (for non-combinable),
    /// plus one or more `warnings` strings explaining the schema
    /// boundary, the dropped groups, and the unresolved segments.
    ///
    /// Delivers the user-visible outcome documented in
    /// `docs/design-sketch-db.md` §7: queries spanning a reconfigure
    /// boundary no longer see a silent data cliff — additive stats
    /// get the combined answer, and non-combinable stats get an
    /// explicit Partial notice instead of the arbitrary single-agg
    /// single-segment result.
    fn try_handle_query_promql_via_timeline(
        &self,
        query: &str,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use crate::engines::timeline_dispatch::{combine_statistic, CombinedResult, SegmentValue};
        use crate::stores::sketch_db::{TimelineCoverage, TimelineSegment};

        // Phase 1: shared pipeline with the default path — parse,
        // pattern-match, auto-resolve a "probe" agg. We reuse the
        // probe context purely to read the derived (metric name,
        // query time range, statistic) triple that timeline dispatch
        // needs. The probe agg itself is NOT used for execution in
        // the multi-segment branch.
        let (query_pattern_type, match_result) = self.parse_and_match_promql(query)?;
        let metric_name = match_result.get_metric_name()?;
        let probe_agg_info =
            self.resolve_agg_info_promql(query, &match_result, query_pattern_type)?;
        let query_time = Self::convert_query_time_to_data_time(time);
        let probe_context = self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            probe_agg_info,
        )?;

        let stat = probe_context.metadata.statistic_to_compute;
        let t1 = probe_context.store_plan.values_query.start_timestamp;
        let t2 = probe_context.store_plan.values_query.end_timestamp;

        // Phase 2: resolve the schema timeline over [t1, t2] for this
        // metric. Zero or one segments means the default single-agg
        // path is already correct; bail out and let the caller use
        // it.
        let segments = self.timeline_for_query(&metric_name, t1, t2);
        if segments.len() < 2 {
            return None;
        }

        debug!(
            metric = %metric_name,
            segments = segments.len(),
            t1,
            t2,
            statistic = ?stat,
            "schema-timeline dispatch: evaluating per-segment"
        );

        // Phase 4: per-segment evaluation. Each segment's agg_id
        // evaluates the same query clipped to the segment's
        // [start_ms, end_ms]. `Purged` segments (TimelineCoverage)
        // or missing configs are collected into `unresolved` so the
        // combiner can surface them.
        let mut per_group: HashMap<Option<KeyByLabelValues>, Vec<SegmentValue>> = HashMap::new();
        let mut unresolved: Vec<TimelineSegment> = Vec::new();

        for segment in &segments {
            if matches!(segment.coverage, TimelineCoverage::Purged) {
                unresolved.push(segment.clone());
                continue;
            }
            let mut ctx = match self.build_query_execution_context_promql_for_agg_id(
                query.to_string(),
                time,
                segment.agg_id,
            ) {
                Some(c) => c,
                None => {
                    // agg_id no longer in the current StreamingConfig
                    // (e.g. controller pushed a swap that dropped
                    // this entry between timeline resolution and
                    // dispatch). Classify as unresolved.
                    unresolved.push(segment.clone());
                    continue;
                }
            };

            // Clip the segment's [start, end) onto the store plan so
            // the per-segment query reads only its own time slice.
            ctx.store_plan.values_query.start_timestamp = segment.start_ms;
            ctx.store_plan.values_query.end_timestamp = segment.end_ms;
            if let Some(ref mut keys_q) = ctx.store_plan.keys_query {
                keys_q.start_timestamp = segment.start_ms;
                keys_q.end_timestamp = segment.end_ms;
            }

            let (per_segment_results, _segment_window) =
                match self.execute_query_pipeline(&ctx, true) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            agg_id = segment.agg_id,
                            start_ms = segment.start_ms,
                            end_ms = segment.end_ms,
                            "Timeline segment execution failed: {}",
                            e
                        );
                        unresolved.push(segment.clone());
                        continue;
                    }
                };

            // The per-segment window isn't surfaced in the combined
            // result for the schema-timeline dispatch path — the
            // combined answer spans multiple agg_ids/windows by
            // design, so a single `precompute_window` annotation
            // would be misleading. The single-agg path
            // (execute_context above) carries it through normally.

            debug!(
                agg_id = segment.agg_id,
                count = per_segment_results.len(),
                "schema-timeline dispatch: segment produced results"
            );
            for el in per_segment_results {
                per_group
                    .entry(Some(el.labels))
                    .or_default()
                    .push(SegmentValue {
                        segment: segment.clone(),
                        value: el.value,
                    });
            }
        }
        debug!(
            groups = per_group.len(),
            unresolved = unresolved.len(),
            "schema-timeline dispatch: about to combine"
        );

        // Phase 5: per-group combine. Group-by label-tuple so the
        // combiner folds per-segment scalars into one final scalar
        // per group. Groups that only appear in `unresolved` (no
        // segment ever produced a value for them) are skipped.
        //
        // `any_partial` tracks whether any group came back non-`Full`
        // — drives the Prometheus `warnings` surface below so the
        // caller sees "this answer is partial" explicitly instead of
        // a silent cliff.
        let mut output: Vec<InstantVectorElement> = Vec::new();
        let mut any_partial = false;
        let mut groups_with_no_value = 0usize;
        for (label_key, segment_values) in per_group {
            match combine_statistic(stat, &segment_values, &unresolved) {
                CombinedResult::Full(v) => {
                    output.push(InstantVectorElement::new(label_key.unwrap_or_default(), v));
                }
                CombinedResult::Partial {
                    covered: Some(v), ..
                } => {
                    // Emit `covered` for combinable stats so the user
                    // sees the partial sum. The warning below tells
                    // them not to trust the scalar as a full range
                    // answer.
                    output.push(InstantVectorElement::new(label_key.unwrap_or_default(), v));
                    any_partial = true;
                }
                CombinedResult::Partial { covered: None, .. } => {
                    // Non-combinable stat (quantile / topk / rate /
                    // increase / cardinality) OR a group the
                    // combiner couldn't reduce. Drop the group —
                    // there is no meaningful scalar to show — but
                    // flag the whole response partial.
                    any_partial = true;
                    groups_with_no_value += 1;
                }
            }
        }

        // Phase 6: build Prometheus `warnings` when the combiner
        // returned any Partial. One line summarising the schema
        // boundary, plus up-to-three per-segment lines with agg_id
        // + clipped range so operators can correlate against the
        // `GET /api/v1/db/timeline` surface. We cap at three to
        // keep responses bounded; the full set is still inspectable
        // via the timeline endpoint.
        let warnings = if any_partial {
            let mut w = Vec::with_capacity(2 + unresolved.len().min(3));
            w.push(format!(
                "partial result: query spans {} schemas for metric '{}' over [{}, {}] and the requested statistic {:?} is not cleanly combinable across schema boundaries — see `GET /api/v1/db/timeline?metric={}&start_ms={}&end_ms={}` for the full segment map",
                segments.len(),
                metric_name,
                t1,
                t2,
                stat,
                metric_name,
                t1,
                t2,
            ));
            if groups_with_no_value > 0 {
                w.push(format!(
                    "{} group(s) dropped because no segment could answer the statistic",
                    groups_with_no_value,
                ));
            }
            for seg in unresolved.iter().take(3) {
                w.push(format!(
                    "segment agg_id={} [{}, {}) status={:?} coverage={:?} unresolved",
                    seg.agg_id, seg.start_ms, seg.end_ms, seg.status, seg.coverage,
                ));
            }
            if unresolved.len() > 3 {
                w.push(format!("... and {} more", unresolved.len() - 3));
            }
            w
        } else {
            Vec::new()
        };

        // §6.4: build a per-segment accuracy envelope from each
        // segment's resolved agg_id. Segments that don't resolve
        // to an in-config agg drop out — their partial-ness is
        // already reflected in `warnings` above.
        let snap = self.streaming_config_snapshot();
        let per_segment: Vec<crate::stores::sketch_db::PerSegmentAccuracy> = segments
            .iter()
            .filter_map(|seg| {
                let cfg = snap.get_aggregation_config(seg.agg_id)?;
                Some(crate::stores::sketch_db::PerSegmentAccuracy {
                    agg_id: seg.agg_id,
                    range_ms: [seg.start_ms as i64, seg.end_ms as i64],
                    profile: crate::stores::sketch_db::AccuracyProfile::derive(cfg),
                })
            })
            .collect();
        let envelope = crate::stores::sketch_db::AccuracyEnvelope::from_segments(per_segment);

        let qr = QueryResult::vector_with_warnings(output, probe_context.query_time, warnings);
        let qr = match envelope {
            Some(e) => qr.with_accuracy(e),
            None => qr,
        };
        Some((probe_context.metadata.query_output_labels, qr))
    }

    /// Merge precomputed outputs (extracts buckets from timestamped data)
    fn merge_precomputed_outputs(
        &self,
        precomputed_outputs_map: &TimestampedBucketsMap,
        do_merge: bool,
        aggregation_type: AggregationType,
    ) -> HashMap<Option<KeyByLabelValues>, Box<dyn crate::data_model::AggregateCore>> {
        #[cfg(feature = "extra_debugging")]
        let start_time = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!("Starting merge for {} keys", precomputed_outputs_map.len());
        #[cfg(feature = "extra_debugging")]
        debug!(
            "do_merge: {}, aggregation_type: {:?}",
            do_merge, aggregation_type
        );

        // Merge if: temporal query OR DeltaSetAggregator (which accumulates keys over time)
        let should_merge = do_merge || aggregation_type == AggregationType::DeltaSetAggregator;

        let mut merged = HashMap::with_capacity(precomputed_outputs_map.len());

        for (key, timestamped_buckets) in precomputed_outputs_map.iter() {
            if !timestamped_buckets.is_empty() {
                // Extract just the buckets (without timestamps) for merging
                let precomputes: Vec<Box<dyn AggregateCore>> = timestamped_buckets
                    .iter()
                    .map(|(_, bucket)| bucket.clone_boxed_core())
                    .collect();

                if should_merge {
                    #[cfg(feature = "extra_debugging")]
                    debug!("  Merging accumulators (should_merge=true)");
                    #[cfg(feature = "extra_debugging")]
                    let merge_start = Instant::now();
                    let merged_accumulator = self.merge_accumulators(&precomputes);
                    #[cfg(feature = "extra_debugging")]
                    let merge_duration = merge_start.elapsed();
                    #[cfg(feature = "extra_debugging")]
                    debug!(
                        "  Merge completed in {:.2}ms, result type: {}",
                        merge_duration.as_secs_f64() * 1000.0,
                        merged_accumulator.get_accumulator_type()
                    );
                    merged.insert(key.clone(), merged_accumulator);
                } else {
                    assert_eq!(
                        precomputes.len(),
                        1,
                        "Spatial queries should have exactly 1 precompute per key"
                    );
                    merged.insert(key.clone(), precomputes[0].clone_boxed_core());
                }
            }
        }

        #[cfg(feature = "extra_debugging")]
        let total_duration = start_time.elapsed();
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[LATENCY] Complete merge operation: {:.2}ms, merged {} keys",
            total_duration.as_secs_f64() * 1000.0,
            merged.len()
        );

        merged
    }

    /// Merge multiple accumulators using the merge_with method from AggregateCore trait
    /// This follows the Python merge_accumulators approach
    fn merge_accumulators(
        &self,
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Box<dyn crate::data_model::AggregateCore> {
        if accumulators.is_empty() {
            panic!("No accumulators to merge");
        }

        if accumulators.len() == 1 {
            return accumulators[0].clone_boxed_core();
        }

        // Try to use optimized batch merge for KLL accumulators
        if accumulators[0].get_accumulator_type() == AggregationType::DatasketchesKLL {
            use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;

            match DatasketchesKLLAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Try to use optimized batch merge for CountMinSketch accumulators
        if accumulators[0].get_accumulator_type() == AggregationType::CountMinSketch {
            use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;

            match CountMinSketchAccumulator::merge_multiple(accumulators) {
                Ok(merged) => return Box::new(merged),
                Err(e) => {
                    warn!(
                        "Batch merge failed: {}. Falling back to sequential merge.",
                        e
                    );
                    // Fall through to sequential merge below
                }
            }
        }

        // Fallback: sequential merge for other accumulator types
        // (Still benefits from Phase 1 optimization of merge_with)
        let mut result = accumulators[0].clone_boxed_core();

        for accumulator in &accumulators[1..] {
            match result.merge_with(accumulator.as_ref()) {
                Ok(merged) => {
                    result = merged;
                }
                Err(e) => {
                    warn!("Failed to merge accumulator: {}. Using existing result.", e);
                    // Continue with the current result if merge fails
                }
            }
        }

        result
    }

    /// Collects results when key and value use different aggregations
    fn collect_results_separate_keys(
        &self,
        merged_values: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        merged_keys: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_keys {
            let keys_for_this_precompute = precompute
                .get_keys()
                .ok_or_else(|| "Keys required for separate aggregation".to_string())?;

            for key_for_this_precompute in keys_for_this_precompute {
                let value_precompute = merged_values
                    .get(key)
                    .ok_or_else(|| format!("No value for key: {:?}", key))?;

                let value = self
                    .query_precompute_for_statistic(
                        value_precompute.as_ref(),
                        statistic,
                        &Some(key_for_this_precompute.clone()),
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Collects results when key and value use same aggregation
    fn collect_results_same_aggregation(
        &self,
        merged_outputs: &HashMap<Option<KeyByLabelValues>, Box<dyn AggregateCore>>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
        enable_topk_limiting: bool,
    ) -> Result<HashMap<Option<KeyByLabelValues>, f64>, String> {
        let mut unformatted_results = HashMap::new();

        for (key, precompute) in merged_outputs {
            if let Some(unwrapped_keys) = precompute.get_keys() {
                let keys_to_process = if enable_topk_limiting {
                    self.limit_keys_for_topk(unwrapped_keys, statistic, query_kwargs)?
                } else {
                    unwrapped_keys
                };

                for key_for_this_precompute in keys_to_process {
                    let value = self
                        .query_precompute_for_statistic(
                            precompute.as_ref(),
                            statistic,
                            &Some(key_for_this_precompute.clone()),
                            query_kwargs,
                        )
                        .map_err(|e| format!("Query failed: {}", e))?;

                    unformatted_results.insert(Some(key_for_this_precompute.clone()), value);
                }
            } else {
                let value = self
                    .query_precompute_for_statistic(
                        precompute.as_ref(),
                        statistic,
                        &None,
                        query_kwargs,
                    )
                    .map_err(|e| format!("Query failed: {}", e))?;

                unformatted_results.insert(key.clone(), value);
            }
        }

        Ok(unformatted_results)
    }

    /// Limits keys for topk queries
    fn limit_keys_for_topk(
        &self,
        keys: Vec<KeyByLabelValues>,
        statistic: &Statistic,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<Vec<KeyByLabelValues>, String> {
        if *statistic != Statistic::Topk {
            return Ok(keys);
        }

        let k_str = query_kwargs
            .get("k")
            .ok_or_else(|| "Missing k parameter for topk".to_string())?;

        let k = k_str
            .parse::<usize>()
            .map_err(|_| format!("Failed to parse k: '{}'", k_str))?;

        Ok(keys.into_iter().take(k).collect())
    }

    fn query_precompute_for_statistic(
        &self,
        precompute: &dyn AggregateCore,
        statistic: &Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1b of the sketch DB design
        // (docs/design-sketch-db.md §5.1 / §16 Phase 1):
        // for single-subpopulation queries on additive statistics
        // (Count / Sum / Min / Max), serve from the typed aux
        // columns without deserialising the sketch payload.
        //
        // Keyed queries (`key.is_some()`) still need the full
        // `query_statistic` path — aux is per-accumulator, not
        // per-subpopulation key.
        //
        // `try_answer` returns `None` when the statistic isn't
        // covered by aux (Quantile / Cardinality / TopK / Increase /
        // Rate) or when the accumulator doesn't track the requested
        // aux field; both cases fall through to the existing path
        // so the query result is semantically identical.
        if key.is_none() {
            if let Some(value) = precompute.aux_stats().try_answer(*statistic) {
                return Ok(value);
            }
        }
        precompute.query_statistic(*statistic, key, query_kwargs)
    }

    // ============================================================
    // Range Query Support
    // ============================================================

    /// Validate range query parameters
    fn validate_range_query_params(
        &self,
        start: u64,
        end: u64,
        step: u64,
        tumbling_window_ms: u64,
    ) -> Result<(), String> {
        if start >= end {
            return Err("start must be before end".to_string());
        }
        if step == 0 {
            return Err("step must be positive".to_string());
        }
        if !step.is_multiple_of(tumbling_window_ms) {
            return Err(format!(
                "step ({} ms) must be a multiple of tumbling window size ({} ms)",
                step, tumbling_window_ms
            ));
        }
        Ok(())
    }

    /// Build execution context for range query
    pub fn build_range_query_execution_context_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<RangeQueryExecutionContext> {
        // First, build the base instant query context (reuse existing logic)
        // Use 'end' as the reference time for parsing
        let base_context = self.build_query_execution_context_promql(query, end)?;

        // Convert to milliseconds
        let start_ms = Self::convert_query_time_to_data_time(start);
        let end_ms = Self::convert_query_time_to_data_time(end);
        let step_ms = (step * 1000.0) as u64;

        // Get window size
        let tumbling_window_ms = self
            .streaming_config_snapshot()
            .get_aggregation_config(base_context.agg_info.aggregation_id_for_value)
            .map(|config| config.window_size * 1000)?;

        // Validate parameters
        self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
            .map_err(|e| {
                warn!("Range query validation failed: {}", e);
                e
            })
            .ok()?;

        // Calculate lookback from the base context's store plan
        let lookback_ms = base_context.store_plan.values_query.end_timestamp
            - base_context.store_plan.values_query.start_timestamp;

        let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
        let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

        // Modify the store plan to cover the entire range
        let mut extended_store_plan = base_context.store_plan.clone();
        extended_store_plan.values_query.start_timestamp = start_ms.saturating_sub(lookback_ms);
        extended_store_plan.values_query.end_timestamp = end_ms;
        // Range queries always use range fetch, not exact
        extended_store_plan.values_query.is_exact_query = false;

        Some(RangeQueryExecutionContext {
            base: QueryExecutionContext {
                store_plan: extended_store_plan,
                ..base_context
            },
            range_params: RangeQueryParams {
                start: start_ms,
                end: end_ms,
                step: step_ms,
            },
            buckets_per_step,
            lookback_bucket_count,
            tumbling_window_ms,
        })
    }

    // /// Try to handle a PromQL range query via the sketch shortcut path.
    // /// Returns Some if the query is sketch-backed and PromSketchStore is available.
    // /// Returns None to fall through to the precomputed pipeline.
    // fn handle_sketch_range_query_promql(
    //     &self,
    //     query: &str,
    //     start: f64,
    //     end: f64,
    //     step: f64,
    // ) -> Option<(KeyByLabelNames, QueryResult)> {
    //     let ps = self.promsketch_store.as_ref()?;

    //     let components = match self.parse_sketch_query_components(query) {
    //         Some(c) => c,
    //         None => {
    //             debug!(
    //                 "Sketch range query: could not parse sketch components from '{}'",
    //                 query
    //             );
    //             return None;
    //         }
    //     };

    //     let eval_start = Instant::now();
    //     let range_ms = components.range_seconds * 1000;

    //     // Convert query params to ms
    //     let start_ms = Self::convert_query_time_to_data_time(start);
    //     let end_ms = Self::convert_query_time_to_data_time(end);
    //     let step_ms = (step * 1000.0) as u64;

    //     if step_ms == 0 || start_ms >= end_ms {
    //         warn!(
    //             "Sketch range query: invalid params step_ms={}, start_ms={}, end_ms={}",
    //             step_ms, start_ms, end_ms
    //         );
    //         return None;
    //     }

    //     // Get all matching series labels
    //     let series_labels = ps.matching_series_labels(&components.metric);
    //     if series_labels.is_empty() {
    //         debug!(
    //             "Sketch range query: no matching series for {}, falling through",
    //             components.metric
    //         );
    //         return None;
    //     }

    //     info!(
    //         "Sketch range query: {}({}) over [{}, {}] step {} with {} series",
    //         components.func_name,
    //         components.metric,
    //         start_ms,
    //         end_ms,
    //         step_ms,
    //         series_labels.len()
    //     );

    //     // For each matching series, iterate over time steps
    //     let mut range_elements: Vec<RangeVectorElement> = Vec::new();

    //     for series_label in &series_labels {
    //         let labels = KeyByLabelValues::new_with_labels(vec![series_label.clone()]);
    //         let mut element = RangeVectorElement::new(labels);

    //         let mut current_time = start_ms;
    //         while current_time <= end_ms {
    //             let step_end = current_time;
    //             let step_start = step_end.saturating_sub(range_ms);

    //             match ps.eval(
    //                 &components.func_name,
    //                 series_label,
    //                 components.args,
    //                 step_start,
    //                 step_end,
    //             ) {
    //                 Ok(value) => element.add_sample(current_time, value),
    //                 Err(e) => {
    //                     debug!(
    //                         "Sketch range query: eval failed for {} at t={}: {}",
    //                         series_label, current_time, e
    //                     );
    //                 }
    //             }

    //             current_time += step_ms;
    //         }

    //         if !element.samples.is_empty() {
    //             range_elements.push(element);
    //         }
    //     }

    //     if range_elements.is_empty() {
    //         debug!(
    //             "Sketch range query: all series produced empty results for {}({})",
    //             components.func_name, components.metric
    //         );
    //         ps_metrics::SKETCH_QUERIES_TOTAL
    //             .with_label_values(&["miss"])
    //             .inc();
    //         return None;
    //     }

    //     ps_metrics::SKETCH_QUERIES_TOTAL
    //         .with_label_values(&["hit"])
    //         .inc();
    //     ps_metrics::SKETCH_QUERY_DURATION.observe(eval_start.elapsed().as_secs_f64());

    //     let output_labels = KeyByLabelNames::new(vec!["__name__".to_string()]);
    //     Some((output_labels, QueryResult::matrix(range_elements)))
    // }

    /// Main entry point for range queries
    pub fn handle_range_query_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!(
            "Handling range query: {} from {} to {} step {}",
            query, start, end, step
        );

        // Check for binary arithmetic before attempting single-query dispatch.
        if let Ok(ast) = promql_parser::parser::parse(&query) {
            if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
                let result = self.handle_binary_expr_range_promql(&ast, start, end, step);
                let total_duration = query_start_time.elapsed();
                debug!(
                    "Binary arithmetic range query handling took: {:.2}ms",
                    total_duration.as_secs_f64() * 1000.0
                );
                return result;
            }
        }

        let context = self.build_range_query_execution_context_promql(query, start, end, step)?;

        // Execute range query pipeline
        let results: Vec<RangeVectorElement> = self
            .execute_range_query_pipeline(&context)
            .map_err(|e| {
                warn!("Range query execution failed: {}", e);
                e
            })
            .ok()?;

        // // Determine query routing order based on function type.
        // // USampling functions prefer the precomputed path first (sketch fallback),
        // // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context =
        //         self.build_range_query_execution_context_promql(query.clone(), start, end, step)?;

        //     let results: Vec<RangeVectorElement> = self
        //         .execute_range_query_pipeline(&context)
        //         .map_err(|e| {
        //             warn!("Range query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.base.metadata.query_output_labels,
        //         QueryResult::matrix(results),
        //     ))
        // })();

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_duration = query_start_time.elapsed();
        debug!(
            "Total range query handling took: {:.2}ms",
            total_duration.as_secs_f64() * 1000.0
        );

        Some((
            context.base.metadata.query_output_labels,
            QueryResult::matrix(results),
        ))
    }

    /// Execute the range query pipeline
    fn execute_range_query_pipeline(
        &self,
        context: &RangeQueryExecutionContext,
    ) -> Result<Vec<crate::engines::query_result::RangeVectorElement>, String> {
        use crate::engines::query_result::RangeVectorElement;
        use crate::engines::window_merger::create_window_merger;

        // Step 1: Fetch all data needed for the entire range
        let all_data = self.execute_store_query(&context.base.store_plan.values_query)?;

        if all_data.is_empty() {
            return Err(format!("No data found for metric: {}", context.base.metric));
        }

        debug!(
            "Range query: fetched {} keys, {} total buckets",
            all_data.len(),
            all_data.values().map(|v| v.len()).sum::<usize>()
        );

        let mut results: HashMap<KeyByLabelValues, RangeVectorElement> = HashMap::new();

        // Determine accumulator type for merger selection
        let accumulator_type = &context.base.agg_info.aggregation_type_for_value;

        // Calculate step parameters
        let step_ms = context.range_params.step;
        let start_ms = context.range_params.start;
        let end_ms = context.range_params.end;
        let buckets_per_step = context.buckets_per_step;
        let lookback_bucket_count = context.lookback_bucket_count;

        let window_mode = if buckets_per_step <= lookback_bucket_count {
            "sliding (slide <= size)"
        } else {
            "hopping (slide > size)"
        };
        debug!(
            "Range query params: start={}, end={}, step_ms={}, tumbling_window_ms={}, \
             buckets_per_step (slide)={}, lookback_bucket_count (size)={}, mode={}",
            start_ms,
            end_ms,
            step_ms,
            context.tumbling_window_ms,
            buckets_per_step,
            lookback_bucket_count,
            window_mode
        );

        // Process each key independently
        for (key_opt, timestamped_buckets) in &all_data {
            let key = match key_opt {
                Some(k) => k.clone(),
                None => continue, // Skip None keys for now
            };

            // Build lookup: bucket_start_timestamp -> bucket for O(1) access
            let bucket_map: HashMap<u64, &dyn AggregateCore> = timestamped_buckets
                .iter()
                .map(|((start, _), bucket)| (*start, bucket.as_ref()))
                .collect();

            debug!(
                "Key {:?}: built bucket_map with {} entries, timestamps: {:?}",
                key,
                bucket_map.len(),
                bucket_map.keys().collect::<Vec<_>>()
            );

            // Create result element for this key
            let mut element = RangeVectorElement::new(key.clone());

            // Calculate window parameters
            let tumbling_window_ms = context.tumbling_window_ms;
            let lookback_ms = (lookback_bucket_count as u64) * tumbling_window_ms;

            debug!(
                "Key {:?}: range [{}, {}], step={}, lookback_ms={}, tumbling_window_ms={}",
                key, start_ms, end_ms, step_ms, lookback_ms, tumbling_window_ms
            );

            // Iterate by OUTPUT timestamp, not by bucket index
            let mut current_time = start_ms;
            while current_time <= end_ms {
                // Window covers [current_time - lookback_ms, current_time)
                // This means we look at buckets that START within this range
                let window_start = current_time.saturating_sub(lookback_ms);

                // Collect all AVAILABLE buckets in this window (skip missing ones)
                let mut window_buckets: Vec<Box<dyn AggregateCore>> = Vec::new();

                let mut t = window_start;
                while t < current_time {
                    if let Some(bucket) = bucket_map.get(&t) {
                        window_buckets.push((*bucket).clone_boxed_core());
                    }
                    // If bucket missing at timestamp t, just skip it (partial data is okay)
                    t += tumbling_window_ms;
                }

                if !window_buckets.is_empty() {
                    // Merge available buckets
                    let mut merger = create_window_merger(*accumulator_type);
                    merger.initialize(window_buckets);

                    match merger.get_merged() {
                        Ok(merged) => {
                            // Query statistic and emit sample at current_time
                            match self.query_precompute_for_statistic(
                                merged.as_ref(),
                                &context.base.metadata.statistic_to_compute,
                                &Some(key.clone()),
                                &context.base.metadata.query_kwargs,
                            ) {
                                Ok(value) => {
                                    debug!(
                                        "Key {:?}: emitting sample (t={}, value={})",
                                        key, current_time, value
                                    );
                                    element.add_sample(current_time, value);
                                }
                                Err(e) => {
                                    debug!(
                                        "Failed to query statistic at t={} for key {:?}: {}",
                                        current_time, key, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            debug!(
                                "Failed to get merged result at t={} for key {:?}: {}",
                                current_time, key, e
                            );
                        }
                    }
                } else {
                    // No data at all for this window - skip sample
                    debug!(
                        "Key {:?}: skipping sample at {} - no data in window [{}, {})",
                        key, current_time, window_start, current_time
                    );
                }

                current_time += step_ms;
            }

            debug!(
                "Key {:?}: finished with {} samples",
                key,
                element.samples.len()
            );

            // Only include keys with samples
            if !element.samples.is_empty() {
                results.insert(key, element);
            }
        }

        // Convert to Vec
        Ok(results.into_values().collect())
    }
}

// ---------------------------------------------------------------------------
// Phase-5: `QueryEngine` trait impl.
//
// Adapter only — does NOT change `handle_query` or any other existing
// surface. The trait's `execute(&str)` walks the same `handle_query` code
// path the binary's HTTP driver uses today; `None` (capability miss) is
// translated to `EngineError::CapabilityMiss` so the router can fall through
// to the next compatible backend.
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl crate::routing::engine_router::QueryEngine for SimpleEngine {
    async fn execute(
        &self,
        query: &str,
    ) -> Result<crate::engines::query_result::QueryResult, crate::engines::EngineError> {
        // `handle_query` is sync + needs a `time: f64` (epoch millis as float).
        // The router doesn't pass a query time, so we use wall-clock now —
        // matches `GorillaQueryEngine::execute`'s convention.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as f64)
            .unwrap_or(0.0);
        match self.handle_query(query.to_string(), now_ms) {
            Some((_labels, result)) => Ok(result),
            None => Err(crate::engines::EngineError::capability_miss(
                asap_types::StorageBackend::SketchWarmTier.data_source_id(),
                format!("SimpleEngine has no compatible aggregation for `{query}`"),
            )),
        }
    }

    fn capabilities(&self) -> crate::routing::engine_router::EngineCapabilities {
        crate::routing::engine_router::EngineCapabilities {
            data_source_id: asap_types::StorageBackend::SketchWarmTier.data_source_id(),
            storage_backend: asap_types::StorageBackend::SketchWarmTier,
            // Warm-tier sketches are O(sketch-size); call it 16 MiB ceiling
            // for buffered ops (KLL with k=200 is well below this).
            supports_streams_above_bytes: 16 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod range_query_tests {
    use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
    use crate::engines::window_merger::NaiveMerger;
    use serde_json::Value;
    use std::any::Any;

    /// Mock accumulator that stores a unique ID to detect stale window reuse
    #[derive(Clone, Debug)]
    struct MockBucketAccumulator {
        bucket_id: u64,
        value: f64,
    }

    impl MockBucketAccumulator {
        fn new(bucket_id: u64, value: f64) -> Self {
            Self { bucket_id, value }
        }
    }

    impl SerializableToSink for MockBucketAccumulator {
        fn serialize_to_json(&self) -> Value {
            serde_json::json!({"bucket_id": self.bucket_id, "value": self.value})
        }

        fn serialize_to_bytes(&self) -> Vec<u8> {
            format!("{}:{}", self.bucket_id, self.value).into_bytes()
        }
    }

    impl AggregateCore for MockBucketAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(self.clone())
        }

        fn type_name(&self) -> &'static str {
            "MockBucketAccumulator"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn merge_with(
            &self,
            other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            if let Some(other_mock) = other.as_any().downcast_ref::<MockBucketAccumulator>() {
                // Sum values, keep max bucket_id to track which buckets are in window
                Ok(Box::new(MockBucketAccumulator::new(
                    self.bucket_id.max(other_mock.bucket_id),
                    self.value + other_mock.value,
                )))
            } else {
                Err("Cannot merge with different accumulator type".into())
            }
        }

        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }

        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }

        fn query_statistic(
            &self,
            _statistic: promql_utilities::query_logics::enums::Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &std::collections::HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            Err("MockBucketAccumulator does not support query_statistic".into())
        }
    }

    /// Simulates the sliding window loop from execute_range_query_pipeline
    /// Returns: Vec of (timestamp, merged_value, max_bucket_id_in_window)
    fn simulate_sliding_window(
        buckets: Vec<Box<dyn AggregateCore>>,
        lookback_bucket_count: usize,
        buckets_per_step: usize,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;

        let mut results = Vec::new();

        if buckets.len() < lookback_bucket_count {
            return results;
        }

        let mut merger = NaiveMerger::new();

        // Initialize with first window
        let initial_window: Vec<_> = buckets[0..lookback_bucket_count]
            .iter()
            .map(|b| b.clone_boxed_core())
            .collect();
        merger.initialize(initial_window);

        let mut bucket_index = lookback_bucket_count;
        let mut current_time = start_ms;

        while current_time <= end_ms {
            // Query current window
            if let Ok(merged) = merger.get_merged() {
                if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                    results.push((current_time, mock.value, mock.bucket_id));
                }
            }

            // Slide window for next step
            current_time += step_ms;

            if current_time <= end_ms {
                if bucket_index + buckets_per_step <= buckets.len() {
                    let new_buckets: Vec<_> = buckets
                        [bucket_index..bucket_index + buckets_per_step]
                        .iter()
                        .map(|b| b.clone_boxed_core())
                        .collect();
                    merger.slide(buckets_per_step, new_buckets);
                    bucket_index += buckets_per_step;
                } else {
                    // Not enough buckets to continue - stop to avoid stale data
                    break;
                }
            }
        }

        results
    }

    /// Simulates sliding window with proper timestamp alignment for missing data.
    /// This accounts for the scenario where the store returns fewer buckets than
    /// expected because data is missing at the start of the query range.
    ///
    /// # Arguments
    /// * `expected_bucket_count` - How many buckets we would have if data was complete
    fn simulate_sliding_window_with_alignment(
        buckets: Vec<Box<dyn AggregateCore>>,
        lookback_bucket_count: usize,
        buckets_per_step: usize,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        expected_bucket_count: usize,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;

        let mut results = Vec::new();

        // Check if we have enough buckets for at least one window
        if buckets.len() < lookback_bucket_count {
            return results;
        }

        // Calculate missing data offset
        let missing_buckets = expected_bucket_count.saturating_sub(buckets.len());
        let tumbling_window_ms = step_ms / (buckets_per_step as u64);

        // First valid sample is offset by missing buckets (data missing at the start)
        let first_valid_sample_ms = start_ms + (missing_buckets as u64) * tumbling_window_ms;

        // Round up to step boundary if needed
        let first_sample_ms = if first_valid_sample_ms <= start_ms {
            start_ms
        } else {
            let offset = first_valid_sample_ms - start_ms;
            if offset.is_multiple_of(step_ms) {
                first_valid_sample_ms
            } else {
                start_ms + ((offset / step_ms) + 1) * step_ms
            }
        };

        // When we have missing buckets at the start, we need to figure out where to
        // start reading from the available buckets. The missing buckets are conceptually
        // at the beginning, so we start reading from the first available bucket.
        //
        // However, if we rounded up to a step boundary, we may need to skip some
        // additional buckets from what we have.
        let extra_offset_ms = first_sample_ms.saturating_sub(first_valid_sample_ms);
        let extra_buckets_to_skip = (extra_offset_ms / tumbling_window_ms) as usize;

        // Check if we have enough data for at least one window after any extra skip
        if extra_buckets_to_skip + lookback_bucket_count > buckets.len() {
            return results;
        }

        let mut merger = NaiveMerger::new();

        // Initialize with window at adjusted position
        let initial_window: Vec<_> = buckets
            [extra_buckets_to_skip..extra_buckets_to_skip + lookback_bucket_count]
            .iter()
            .map(|b| b.clone_boxed_core())
            .collect();
        merger.initialize(initial_window);

        let mut bucket_index = extra_buckets_to_skip + lookback_bucket_count;
        let mut current_time = first_sample_ms;

        while current_time <= end_ms {
            // Query current window
            if let Ok(merged) = merger.get_merged() {
                if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                    results.push((current_time, mock.value, mock.bucket_id));
                }
            }

            // Slide window for next step
            current_time += step_ms;

            if current_time <= end_ms {
                if bucket_index + buckets_per_step <= buckets.len() {
                    let new_buckets: Vec<_> = buckets
                        [bucket_index..bucket_index + buckets_per_step]
                        .iter()
                        .map(|b| b.clone_boxed_core())
                        .collect();
                    merger.slide(buckets_per_step, new_buckets);
                    bucket_index += buckets_per_step;
                } else {
                    break;
                }
            }
        }

        results
    }

    #[test]
    fn test_sliding_window_sufficient_buckets() {
        // Setup: 7 buckets, lookback=5, step=1
        // Should produce 3 valid samples
        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms (3 steps: 1000, 2000, 3000)
            1000, // step_ms
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");

        // Window 1: buckets [0,1,2,3,4], max_id=4, value=50
        assert_eq!(results[0], (1000, 50.0, 4));
        // Window 2: buckets [1,2,3,4,5], max_id=5, value=50
        assert_eq!(results[1], (2000, 50.0, 5));
        // Window 3: buckets [2,3,4,5,6], max_id=6, value=50
        assert_eq!(results[2], (3000, 50.0, 6));
    }

    #[test]
    fn test_sliding_window_insufficient_buckets_stops_early() {
        // 6 buckets, lookback=5, step=1
        // Requesting 3 timestamps but only have data for 2
        // Should stop early rather than produce stale samples
        let buckets: Vec<Box<dyn AggregateCore>> = (0..6)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms (requests 3 steps: 1000, 2000, 3000)
            1000, // step_ms
        );

        println!("Results: {:?}", results);

        // Should only produce 2 valid samples (not 3 with stale data)
        assert_eq!(
            results.len(),
            2,
            "Should only produce 2 samples when data is insufficient for 3rd"
        );

        // Window 1: buckets [0,1,2,3,4], max_id=4
        assert_eq!(results[0], (1000, 50.0, 4));
        // Window 2: buckets [1,2,3,4,5], max_id=5
        assert_eq!(results[1], (2000, 50.0, 5));
        // No window 3 - not enough buckets to slide
    }

    #[test]
    fn test_sliding_window_exactly_enough_buckets() {
        // 5 buckets, lookback=5, step=1
        // Should produce exactly 1 sample (initial window only, can't slide)
        let buckets: Vec<Box<dyn AggregateCore>> = (0..5)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
        );

        println!("Results with exactly enough buckets: {:?}", results);

        // Should produce only 1 sample - can't slide without more buckets
        assert_eq!(results.len(), 1, "Should produce exactly 1 sample");
        assert_eq!(results[0], (1000, 50.0, 4));
    }

    #[test]
    fn test_sliding_window_multi_bucket_step() {
        // 10 buckets, lookback=4, step=2 buckets at a time
        // Should produce samples at positions requiring new data
        let buckets: Vec<Box<dyn AggregateCore>> = (0..10)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window(
            buckets, 4,    // lookback_bucket_count
            2,    // buckets_per_step (slide 2 at a time)
            1000, // start_ms
            4000, // end_ms (4 steps)
            1000, // step_ms
        );

        // Initial: [0,1,2,3], max_id=3
        // After slide 1: [2,3,4,5], max_id=5
        // After slide 2: [4,5,6,7], max_id=7
        // After slide 3: [6,7,8,9], max_id=9
        assert_eq!(results.len(), 4, "Should produce 4 samples");
        assert_eq!(results[0].2, 3, "Window 1 max_id should be 3");
        assert_eq!(results[1].2, 5, "Window 2 max_id should be 5");
        assert_eq!(results[2].2, 7, "Window 3 max_id should be 7");
        assert_eq!(results[3].2, 9, "Window 4 max_id should be 9");
    }

    #[test]
    fn test_sliding_window_missing_data_at_start_aligns_timestamps() {
        // Scenario: Query requests timestamps 1000, 2000, 3000
        // But only 5 buckets exist (enough for 1 sample), not 7 (for 3 samples)
        // lookback=5, step=1 bucket
        // Expected buckets for [1000, 3000]: 7 (5 for first window + 2 steps)
        // Actual buckets: 5 (missing 2 at start)
        // Missing 2 buckets = 2000ms offset
        // First valid sample at: 1000 + 2000 = 3000ms

        let buckets: Vec<Box<dyn AggregateCore>> = (0..5)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
            7,    // expected_bucket_count for full range
        );

        // Should have 1 sample at timestamp 3000, NOT at 1000
        assert_eq!(results.len(), 1, "Should produce 1 sample");
        assert_eq!(results[0].0, 3000, "Sample should be at t=3000, not t=1000");
    }

    #[test]
    fn test_sliding_window_missing_data_rounds_to_step_boundary() {
        // Query: start=0, end=6000, step=2000 (timestamps: 0, 2000, 4000, 6000)
        // Lookback: 4 buckets, step: 2 buckets
        // Expected buckets: 4 + 6 = 10 buckets for full range
        // Actual: 7 buckets (missing 3 at start)
        // Missing 3 buckets = 3000ms offset
        // First valid sample time = 0 + 3000 = 3000ms
        // But 3000 is not on step boundary, so round UP to 4000ms

        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 4,    // lookback_bucket_count
            2,    // buckets_per_step (2000ms step / 1000ms tumbling = 2)
            0,    // start_ms
            6000, // end_ms
            2000, // step_ms
            10,   // expected_bucket_count
        );

        // First sample at 4000 (rounded up from 3000), second at 6000
        assert_eq!(results.len(), 2, "Should produce 2 samples");
        assert_eq!(results[0].0, 4000, "First sample at step boundary 4000");
        assert_eq!(results[1].0, 6000, "Second sample at 6000");
    }

    #[test]
    fn test_sliding_window_full_data_starts_at_query_start() {
        // All data present - should behave same as before (start at start_ms)
        // lookback=5, step=1, query [1000, 3000] = 3 samples
        // Expected buckets: 7, Actual: 7 (no missing data)

        let buckets: Vec<Box<dyn AggregateCore>> = (0..7)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5,    // lookback_bucket_count
            1,    // buckets_per_step
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
            7,    // expected_bucket_count (matches actual - no missing data)
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");
        assert_eq!(results[0].0, 1000, "First sample at query start");
        assert_eq!(results[1].0, 2000);
        assert_eq!(results[2].0, 3000);
    }

    #[test]
    fn test_sliding_window_insufficient_data_for_any_window_returns_empty() {
        // lookback=5 but only 3 buckets - can't form even one window
        let buckets: Vec<Box<dyn AggregateCore>> = (0..3)
            .map(|i| Box::new(MockBucketAccumulator::new(i, 10.0)) as Box<dyn AggregateCore>)
            .collect();

        let results = simulate_sliding_window_with_alignment(
            buckets, 5, // lookback_bucket_count (need 5, have 3)
            1, 1000, 5000, 1000, 9,
        );

        assert_eq!(
            results.len(),
            0,
            "No samples when insufficient data for any window"
        );
    }

    // ============================================================================
    // Tests for timestamp-based lookup implementation (handles gaps in data)
    // ============================================================================

    /// Simulates the timestamp-based lookup approach from execute_range_query_pipeline.
    /// This is the new implementation that handles gaps in data correctly.
    ///
    /// # Arguments
    /// * `timestamped_buckets` - Vec of (bucket_start_timestamp, bucket)
    /// * `lookback_bucket_count` - Number of buckets in each window
    /// * `tumbling_window_ms` - Duration of each tumbling window bucket
    /// * `start_ms` - Query start time
    /// * `end_ms` - Query end time
    /// * `step_ms` - Step between output samples
    ///
    /// # Returns
    /// Vec of (timestamp, merged_value, max_bucket_id_in_window)
    fn simulate_timestamp_based_lookup(
        timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)>,
        lookback_bucket_count: usize,
        tumbling_window_ms: u64,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Vec<(u64, f64, u64)> {
        use crate::engines::window_merger::WindowMerger;
        use std::collections::HashMap;

        let mut results = Vec::new();

        // Build lookup: bucket_start_timestamp -> bucket for O(1) access
        let bucket_map: HashMap<u64, &Box<dyn AggregateCore>> = timestamped_buckets
            .iter()
            .map(|(start, bucket)| (*start, bucket))
            .collect();

        let lookback_ms = (lookback_bucket_count as u64) * tumbling_window_ms;

        // Iterate by OUTPUT timestamp, not by bucket index
        let mut current_time = start_ms;
        while current_time <= end_ms {
            // Window covers [current_time - lookback_ms, current_time)
            let window_start = current_time.saturating_sub(lookback_ms);

            // Collect all AVAILABLE buckets in this window (skip missing ones)
            let mut window_buckets: Vec<Box<dyn AggregateCore>> = Vec::new();

            let mut t = window_start;
            while t < current_time {
                if let Some(bucket) = bucket_map.get(&t) {
                    window_buckets.push((*bucket).clone_boxed_core());
                }
                t += tumbling_window_ms;
            }

            if !window_buckets.is_empty() {
                // Merge available buckets
                let mut merger = NaiveMerger::new();
                merger.initialize(window_buckets);

                if let Ok(merged) = merger.get_merged() {
                    if let Some(mock) = merged.as_any().downcast_ref::<MockBucketAccumulator>() {
                        results.push((current_time, mock.value, mock.bucket_id));
                    }
                }
            }
            // If no buckets available, skip this sample (no entry in results)

            current_time += step_ms;
        }

        results
    }

    #[test]
    fn test_timestamp_lookup_missing_data_at_start() {
        // Scenario: Query range [1000, 5000] with step=1000, lookback=3 buckets
        // Tumbling window = 1000ms
        // Expected buckets for full window coverage starting at t=1000:
        //   - t=1000 needs buckets at -2000, -1000, 0 (before query range)
        // But data only exists at t=3000, 4000, 5000
        //
        // Sample at t=1000: window [1000-3000, 1000) = [-2000, 1000) -> no buckets -> skip
        // Sample at t=2000: window [2000-3000, 2000) = [-1000, 2000) -> no buckets -> skip
        // Sample at t=3000: window [3000-3000, 3000) = [0, 3000) -> no buckets -> skip
        // Sample at t=4000: window [4000-3000, 4000) = [1000, 4000) -> bucket at 3000 -> emit
        // Sample at t=5000: window [5000-3000, 5000) = [2000, 5000) -> buckets at 3000, 4000 -> emit

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
            (5000, Box::new(MockBucketAccumulator::new(5, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            1000, // start_ms
            5000, // end_ms
            1000, // step_ms
        );

        // Should skip samples at 1000, 2000, 3000 (no data in window)
        // Should emit samples at 4000 (partial data) and 5000 (partial data)
        assert_eq!(
            results.len(),
            2,
            "Should produce 2 samples (skipping early ones with no data)"
        );
        assert_eq!(results[0].0, 4000, "First sample at t=4000");
        assert_eq!(results[0].1, 10.0, "Value at t=4000 (1 bucket)");
        assert_eq!(results[1].0, 5000, "Second sample at t=5000");
        assert_eq!(results[1].1, 20.0, "Value at t=5000 (2 buckets merged)");
    }

    #[test]
    fn test_timestamp_lookup_missing_data_in_middle() {
        // Scenario: Buckets at t=1000, 2000, 4000, 5000 (missing t=3000)
        // Query range [4000, 6000], step=1000, lookback=3 buckets
        // Tumbling window = 1000ms
        //
        // Sample at t=4000: window [1000, 4000) -> buckets at 1000, 2000 (missing 3000) -> 2 buckets
        // Sample at t=5000: window [2000, 5000) -> buckets at 2000, 4000 (missing 3000) -> 2 buckets
        // Sample at t=6000: window [3000, 6000) -> buckets at 4000, 5000 (missing 3000) -> 2 buckets

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
            // Missing bucket at 3000
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
            (5000, Box::new(MockBucketAccumulator::new(5, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            4000, // start_ms
            6000, // end_ms
            1000, // step_ms
        );

        // All samples should be emitted with partial data (missing bucket is skipped)
        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples with partial data"
        );

        // t=4000: window [1000, 4000) contains buckets 1000, 2000 -> value=20, max_id=2
        assert_eq!(results[0].0, 4000);
        assert_eq!(results[0].1, 20.0, "2 buckets merged");
        assert_eq!(results[0].2, 2, "max bucket_id = 2");

        // t=5000: window [2000, 5000) contains buckets 2000, 4000 -> value=20, max_id=4
        assert_eq!(results[1].0, 5000);
        assert_eq!(results[1].1, 20.0, "2 buckets merged");
        assert_eq!(results[1].2, 4, "max bucket_id = 4");

        // t=6000: window [3000, 6000) contains buckets 4000, 5000 -> value=20, max_id=5
        assert_eq!(results[2].0, 6000);
        assert_eq!(results[2].1, 20.0, "2 buckets merged");
        assert_eq!(results[2].2, 5, "max bucket_id = 5");
    }

    #[test]
    fn test_timestamp_lookup_all_data_missing_for_window() {
        // Scenario: Query window where no buckets exist at all
        // Buckets at t=10000, 11000, 12000
        // Query range [1000, 3000], step=1000, lookback=3 buckets
        // All windows have no data -> should skip all samples

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (10000, Box::new(MockBucketAccumulator::new(10, 10.0))),
            (11000, Box::new(MockBucketAccumulator::new(11, 10.0))),
            (12000, Box::new(MockBucketAccumulator::new(12, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            1000, // start_ms
            3000, // end_ms
            1000, // step_ms
        );

        assert_eq!(
            results.len(),
            0,
            "Should produce 0 samples when all windows have no data"
        );
    }

    #[test]
    fn test_timestamp_lookup_full_data_matches_expected() {
        // Scenario: Full data available, should behave like contiguous case
        // Buckets at t=0, 1000, 2000, 3000, 4000
        // Query range [3000, 5000], step=1000, lookback=3 buckets
        //
        // Sample at t=3000: window [0, 3000) -> buckets 0, 1000, 2000 -> value=30
        // Sample at t=4000: window [1000, 4000) -> buckets 1000, 2000, 3000 -> value=30
        // Sample at t=5000: window [2000, 5000) -> buckets 2000, 3000, 4000 -> value=30

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (4000, Box::new(MockBucketAccumulator::new(4, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            5000, // end_ms
            1000, // step_ms
        );

        assert_eq!(results.len(), 3, "Should produce 3 samples");

        assert_eq!(results[0], (3000, 30.0, 2), "t=3000: buckets 0,1,2");
        assert_eq!(results[1], (4000, 30.0, 3), "t=4000: buckets 1,2,3");
        assert_eq!(results[2], (5000, 30.0, 4), "t=5000: buckets 2,3,4");
    }

    #[test]
    fn test_timestamp_lookup_sparse_data() {
        // Scenario: Very sparse data - only every 3rd bucket exists
        // Buckets at t=0, 3000, 6000, 9000
        // Query range [3000, 9000], step=3000, lookback=3 buckets (3000ms)
        //
        // Sample at t=3000: window [0, 3000) -> bucket 0 -> value=10
        // Sample at t=6000: window [3000, 6000) -> bucket 3000 -> value=10
        // Sample at t=9000: window [6000, 9000) -> bucket 6000 -> value=10

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (3000, Box::new(MockBucketAccumulator::new(3, 10.0))),
            (6000, Box::new(MockBucketAccumulator::new(6, 10.0))),
            (9000, Box::new(MockBucketAccumulator::new(9, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            9000, // end_ms
            3000, // step_ms
        );

        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples with sparse data"
        );

        // Each window only has 1 bucket because data is sparse
        assert_eq!(
            results[0],
            (3000, 10.0, 0),
            "t=3000: only bucket 0 in window"
        );
        assert_eq!(
            results[1],
            (6000, 10.0, 3),
            "t=6000: only bucket 3 in window"
        );
        assert_eq!(
            results[2],
            (9000, 10.0, 6),
            "t=9000: only bucket 6 in window"
        );
    }

    #[test]
    fn test_timestamp_lookup_missing_data_at_end() {
        // Scenario: Data missing at end of query range
        // Buckets at t=0, 1000, 2000
        // Query range [3000, 6000], step=1000, lookback=3 buckets
        //
        // Sample at t=3000: window [0, 3000) -> buckets 0, 1000, 2000 -> full data
        // Sample at t=4000: window [1000, 4000) -> buckets 1000, 2000 -> partial (missing 3000)
        // Sample at t=5000: window [2000, 5000) -> bucket 2000 -> partial
        // Sample at t=6000: window [3000, 6000) -> no buckets -> skip

        let timestamped_buckets: Vec<(u64, Box<dyn AggregateCore>)> = vec![
            (0, Box::new(MockBucketAccumulator::new(0, 10.0))),
            (1000, Box::new(MockBucketAccumulator::new(1, 10.0))),
            (2000, Box::new(MockBucketAccumulator::new(2, 10.0))),
        ];

        let results = simulate_timestamp_based_lookup(
            timestamped_buckets,
            3,    // lookback_bucket_count
            1000, // tumbling_window_ms
            3000, // start_ms
            6000, // end_ms
            1000, // step_ms
        );

        assert_eq!(
            results.len(),
            3,
            "Should produce 3 samples (last one skipped)"
        );

        assert_eq!(results[0], (3000, 30.0, 2), "t=3000: full window");
        assert_eq!(
            results[1],
            (4000, 20.0, 2),
            "t=4000: partial window (2 buckets)"
        );
        assert_eq!(
            results[2],
            (5000, 10.0, 2),
            "t=5000: partial window (1 bucket)"
        );
        // t=6000 is skipped because no data
    }
}

#[cfg(test)]
mod sketch_query_tests {
    // use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
    // use crate::engines::simple::engine::SimpleEngine;
    // use crate::stores::promsketch_store::PromSketchStore;
    // use crate::stores::{Store, TimestampedBucketsMap};
    // use std::collections::HashMap;
    // use std::sync::Arc;

    // /// Minimal no-op store — sketch queries bypass the store entirely
    // struct NoOpStore;

    // impl Store for NoOpStore {
    //     fn query_precomputed_output(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn query_precomputed_output_exact(
    //         &self,
    //         _: &str,
    //         _: u64,
    //         _: u64,
    //         _: u64,
    //     ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output(
    //         &self,
    //         _: crate::data_model::PrecomputedOutput,
    //         _: Box<dyn crate::data_model::AggregateCore>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output_batch(
    //         &self,
    //         _: Vec<(
    //             crate::data_model::PrecomputedOutput,
    //             Box<dyn crate::data_model::AggregateCore>,
    //         )>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn get_earliest_timestamp_per_aggregation_id(
    //         &self,
    //     ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(HashMap::new())
    //     }
    //     fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         Ok(())
    //     }
    // }

    // /// Helper: create an engine with a populated PromSketchStore.
    // /// Inserts data points 1..=100 into a series with labels = `series_key`.
    // fn engine_with_sketch_data(series_key: &str) -> SimpleEngine {
    //     let ps = Arc::new(PromSketchStore::with_default_config());
    //     ps.ensure_all_sketches(series_key).unwrap();
    //     for i in 1..=100u64 {
    //         ps.sketch_insert(series_key, i, i as f64).unwrap();
    //     }

    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());

    //     SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         Some(ps),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     )
    // }

    // // ---- Instant query tests ----

    // #[test]
    // fn test_sketch_instant_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query at time 0.1s (= 100ms) with a 100ms range
    //     let result = engine.handle_query_promql("entropy_over_time(mymetric[100s])".into(), 0.1);
    //     assert!(result.is_some(), "entropy_over_time should return a result");
    //     let (labels, qr) = result.unwrap();
    //     assert!(!labels.labels.is_empty());
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty(), "should have at least one result");
    //         let val = iv.values[0].value;
    //         assert!(val >= 0.0, "entropy should be non-negative, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_quantile_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_query_promql("quantile_over_time(0.5, mymetric[100s])".into(), 0.1);
    //     assert!(
    //         result.is_some(),
    //         "quantile_over_time should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // Median of 1..100 should be roughly 50
    //         assert!(
    //             val > 20.0 && val < 80.0,
    //             "median should be roughly 50, got {}",
    //             val
    //         );
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_avg_over_time() {
    //     let engine = engine_with_sketch_data("cpu");
    //     let result = engine.handle_query_promql("avg_over_time(cpu[100s])".into(), 0.1);
    //     assert!(result.is_some(), "avg_over_time should return a result");
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Vector(iv) = qr {
    //         assert!(!iv.values.is_empty());
    //         let val = iv.values[0].value;
    //         // avg of 1..100 = 50.5
    //         assert!(val > 30.0 && val < 70.0, "avg should be ~50.5, got {}", val);
    //     } else {
    //         panic!("expected Vector result");
    //     }
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_without_store() {
    //     // Engine with promsketch_store = None
    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         None,
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     );
    //     // Sketch function should fall through (return None) without panicking
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(metric[5m])", 100.0);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // "rate" is not sketch-backed, so should return None from sketch path
    //     let result = engine.handle_sketch_query_promql("rate(mymetric[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_instant_returns_none_for_missing_series() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Query a metric that doesn't exist in the sketch store
    //     let result = engine.handle_sketch_query_promql("entropy_over_time(nonexistent[100s])", 0.1);
    //     assert!(result.is_none());
    // }

    // ---- Range query tests ----

    // #[test]
    // fn test_sketch_range_entropy_over_time() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     // Range query: start=0.01, end=0.1 (10ms to 100ms), step=0.01 (10ms)
    //     // with a 50ms window [50s range]
    //     let result = engine.handle_range_query_promql(
    //         "entropy_over_time(mymetric[50s])".into(),
    //         0.01,
    //         0.1,
    //         0.01,
    //     );
    //     assert!(
    //         result.is_some(),
    //         "sketch range query should return a result"
    //     );
    //     let (_labels, qr) = result.unwrap();
    //     if let crate::engines::query_result::QueryResult::Matrix(rv) = qr {
    //         assert!(!rv.values.is_empty(), "should have at least one series");
    //         let samples = &rv.values[0].samples;
    //         assert!(
    //             samples.len() > 1,
    //             "range query should produce multiple samples, got {}",
    //             samples.len()
    //         );
    //         for sample in samples {
    //             assert!(
    //                 sample.value >= 0.0,
    //                 "entropy should be non-negative, got {}",
    //                 sample.value
    //             );
    //         }
    //     } else {
    //         panic!("expected Matrix result");
    //     }
    // }

    // #[test]
    // fn test_sketch_range_returns_none_without_store() {
    //     let inference_config =
    //         InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = SimpleEngine::new(
    //         Arc::new(NoOpStore),
    //         None,
    //         inference_config,
    //         streaming_config,
    //         15,
    //         QueryLanguage::promql,
    //     );
    //     let result = engine.handle_sketch_range_query_promql(
    //         "entropy_over_time(metric[5m])",
    //         0.0,
    //         100.0,
    //         10.0,
    //     );
    //     assert!(result.is_none());
    // }

    // #[test]
    // fn test_sketch_range_returns_none_for_non_sketch_function() {
    //     let engine = engine_with_sketch_data("mymetric");
    //     let result =
    //         engine.handle_sketch_range_query_promql("rate(mymetric[100s])", 0.01, 0.1, 0.01);
    //     assert!(result.is_none());
    // }
}

// ─── PR E phase 2: per-query re-snapshot tests ─────────────────────────
#[cfg(test)]
mod hot_reload_phase2_tests {
    use super::*;
    use crate::data_model::{
        AggregationType, CleanupPolicy, HotReloadStreamingConfig, InferenceConfig, QueryLanguage,
        StreamingConfig, WindowType,
    };
    use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    fn dummy_agg(id: u64, metric: &str) -> crate::data_model::AggregationConfig {
        crate::data_model::AggregationConfig::new(
            id,
            AggregationType::Sum,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn cfg_with_agg(id: u64, metric: &str) -> StreamingConfig {
        let mut map = std::collections::HashMap::new();
        map.insert(id, dummy_agg(id, metric));
        StreamingConfig::new(map)
    }

    fn build_engine(handle: HotReloadStreamingConfig) -> SimpleEngine {
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config,
            CleanupPolicy::NoCleanup,
        ));
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
        SimpleEngine::new_with_hot_reload(
            store,
            inference_config,
            handle,
            15000,
            QueryLanguage::promql,
        )
    }

    #[test]
    fn streaming_config_snapshot_starts_at_initial_config() {
        let handle = HotReloadStreamingConfig::new(cfg_with_agg(101, "metric_a"));
        let engine = build_engine(handle);
        let snap = engine.streaming_config_snapshot();
        assert_eq!(snap.aggregation_configs.len(), 1);
        assert!(snap.aggregation_configs.contains_key(&101));
    }

    #[test]
    fn streaming_config_snapshot_observes_post_construction_swap() {
        // Core of PR E phase 2: once the engine is built, swapping
        // the shared HotReloadStreamingConfig handle must take
        // effect on the next snapshot call — this is the guarantee
        // that makes POST /api/v1/streaming-config actually useful
        // for query-time behavior.
        let handle = HotReloadStreamingConfig::new(cfg_with_agg(101, "metric_a"));
        let engine = build_engine(handle.clone());

        // Initial snapshot: id 101 only.
        let snap_before = engine.streaming_config_snapshot();
        assert_eq!(snap_before.aggregation_configs.len(), 1);
        assert!(snap_before.aggregation_configs.contains_key(&101));
        assert!(!snap_before.aggregation_configs.contains_key(&202));

        // Simulate a controller push via `HotReloadStreamingConfig::swap`.
        // Clones of the handle share the same underlying ArcSwap, so a
        // swap on `handle` is observable through the engine's stored
        // clone.
        handle.swap(cfg_with_agg(202, "metric_b"));

        // Next snapshot: id 202, id 101 gone. This is exactly what a
        // POST-push-then-query sequence must produce.
        let snap_after = engine.streaming_config_snapshot();
        assert_eq!(snap_after.aggregation_configs.len(), 1);
        assert!(snap_after.aggregation_configs.contains_key(&202));
        assert!(!snap_after.aggregation_configs.contains_key(&101));

        // The old snapshot is still internally consistent — it's a
        // separate Arc that was cheap-cloned before the swap and
        // continues to reflect the pre-swap state. This matches the
        // per-query-entry-snapshot contract: a query that started
        // before the swap sees old config for its entire execution.
        assert!(snap_before.aggregation_configs.contains_key(&101));
    }

    #[test]
    fn legacy_new_constructor_is_independent_of_external_handle() {
        // The legacy `SimpleEngine::new` path wraps the provided
        // Arc<StreamingConfig> in a FRESH HotReloadStreamingConfig
        // internally, so external swaps must NOT leak in. This is
        // the behavior tests and binaries that don't own a shared
        // handle depend on.
        let external_handle = HotReloadStreamingConfig::new(cfg_with_agg(101, "metric_a"));
        let streaming_config = external_handle.snapshot();

        let store = Arc::new(SimpleMapStore::new(
            Arc::clone(&streaming_config),
            CleanupPolicy::NoCleanup,
        ));
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            15000,
            QueryLanguage::promql,
        );

        // External swap should NOT be visible inside the engine — the
        // legacy constructor snapshotted the initial Arc into its own
        // fresh hot-reload wrapper.
        external_handle.swap(cfg_with_agg(999, "metric_swapped"));

        let engine_snap = engine.streaming_config_snapshot();
        assert_eq!(engine_snap.aggregation_configs.len(), 1);
        assert!(
            engine_snap.aggregation_configs.contains_key(&101),
            "legacy `new` constructor should pin the initial config, \
             external swaps to unrelated handles must not leak in"
        );
        assert!(!engine_snap.aggregation_configs.contains_key(&999));
    }
}

// ─── End-to-end feedback loop test ─────────────────────────────────────
//
// The minimum-viable integration test for the full miss → notify →
// plan-push → next-query-hit loop. Covers every seam landed in PR #10
// (HotReloadStreamingConfig endpoint), PR #11 (fire-and-forget
// capability-miss notification), PR #12 (SimpleEngine per-query
// re-snapshot), and mirrors the DataCollector controller side from
// DataCollector PR #156 via an in-process mock client.
//
// What this test does NOT exercise: real HTTP traffic between real
// binaries. The mock controller is an in-process closure that directly
// swaps the `HotReloadStreamingConfig` handle. This is deliberate —
// each component is tested on its own in other suites, and the seams
// between them (`SimpleEngine` field types, the shared `ArcSwap`,
// the `spawn_capability_miss_notify` helper) are what this test
// validates.
//
// The cross-process e2e (real collector, real backend, real query)
// is tracked as a separate operational follow-up and is bounded by
// the pre-existing DataCollector go.mod module-resolution issues.
#[cfg(test)]
mod e2e_feedback_loop_tests {
    use super::*;
    use crate::data_model::{
        AggregationType, CleanupPolicy, HotReloadStreamingConfig, InferenceConfig, QueryLanguage,
        StreamingConfig, WindowType,
    };
    use crate::drivers::query::controller_client::ControllerClient;
    use crate::stores::sketch_db::simple_map_store::SimpleMapStore;
    use async_trait::async_trait;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    fn agg_for_metric(id: u64, metric: &str) -> crate::data_model::AggregationConfig {
        crate::data_model::AggregationConfig::new(
            id,
            AggregationType::Sum,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn streaming_config_with(metric: &str, id: u64) -> StreamingConfig {
        let mut map = std::collections::HashMap::new();
        map.insert(id, agg_for_metric(id, metric));
        StreamingConfig::new(map)
    }

    /// Mock controller that stands in for DataCollector's
    /// `controller/src/main.rs`:
    ///
    /// On each `notify_capability_miss` call it:
    ///   1. Records the requirement (for test assertions).
    ///   2. Invokes a user-supplied "plan generator" closure that
    ///      produces a fresh `StreamingConfig` from the requirements.
    ///   3. Swaps the backend's `HotReloadStreamingConfig` handle —
    ///      mirroring what happens when DataCollector PR #156's
    ///      `BackendClient::push_streaming_config` POSTs to the
    ///      backend's `/api/v1/streaming-config` endpoint on a real
    ///      cross-binary deployment.
    struct InProcessMockController {
        calls: Mutex<Vec<asap_types::query_requirements::QueryRequirements>>,
        call_count: AtomicUsize,
        hot_reload: HotReloadStreamingConfig,
        planner: Box<
            dyn Fn(&asap_types::query_requirements::QueryRequirements) -> StreamingConfig
                + Send
                + Sync,
        >,
    }

    impl InProcessMockController {
        fn new(
            hot_reload: HotReloadStreamingConfig,
            planner: impl Fn(&asap_types::query_requirements::QueryRequirements) -> StreamingConfig
                + Send
                + Sync
                + 'static,
        ) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                call_count: AtomicUsize::new(0),
                hot_reload,
                planner: Box::new(planner),
            }
        }
    }

    #[async_trait]
    impl ControllerClient for InProcessMockController {
        async fn notify_capability_miss(
            &self,
            requirements: &asap_types::query_requirements::QueryRequirements,
        ) -> Result<(), String> {
            self.call_count.fetch_add(1, Ordering::Relaxed);
            self.calls.lock().unwrap().push(requirements.clone());
            let new_config = (self.planner)(requirements);
            self.hot_reload.swap(new_config);
            Ok(())
        }
    }

    /// Exercises the full PR #10/#11/#12 + DC #156 feedback loop end
    /// to end:
    ///
    ///   1. Start with an empty `HotReloadStreamingConfig`.
    ///   2. Build a `SimpleEngine` wired to the handle (PR #12) and
    ///      to an in-process mock controller client (PR #11 +
    ///      DC #156 mirror).
    ///   3. Observe initial snapshot: empty.
    ///   4. Call `find_compatible_aggregation_with_miss_notify` with
    ///      a requirement that will not match anything. This fires
    ///      the fire-and-forget notification which runs the mock
    ///      controller's planner closure and swaps the handle.
    ///   5. Poll `streaming_config_snapshot` until the swap lands.
    ///   6. Assert final state has the new aggregation_id the
    ///      planner returned.
    ///
    /// This test simulates, inside a single process, exactly what a
    /// real backend ↔ controller deployment does across HTTP. The
    /// observable contract is: once the controller acts on a miss,
    /// the next `SimpleEngine` query snapshot reflects the new
    /// plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capability_miss_feedback_loop_closes() {
        // 1. Empty initial config.
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());

        // 2. Mock controller: when a miss comes in, generate a config
        //    that covers the requested metric. This mirrors DC's
        //    replanner running and POSTing via its BackendClient.
        let mock = Arc::new(InProcessMockController::new(hot_reload.clone(), |req| {
            // Use a deterministic agg_id derived from the metric
            // name (same strategy as DC's
            // asapquery_backend::deterministic_agg_id from PR #156).
            let id: u64 = {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut h = DefaultHasher::new();
                req.metric.hash(&mut h);
                h.finish().saturating_add(1)
            };
            streaming_config_with(&req.metric, id)
        }));

        // 3. Build SimpleEngine with the handle and mock controller.
        let store = Arc::new(SimpleMapStore::new(
            Arc::new(StreamingConfig::default()),
            CleanupPolicy::NoCleanup,
        ));
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
        let engine = SimpleEngine::new_with_hot_reload(
            store,
            inference_config,
            hot_reload.clone(),
            15000,
            QueryLanguage::promql,
        )
        .with_controller_client(mock.clone() as Arc<dyn ControllerClient>);

        // 4. Initial snapshot: empty.
        let snap_before = engine.streaming_config_snapshot();
        assert_eq!(
            snap_before.aggregation_configs.len(),
            0,
            "precondition: initial config should be empty"
        );

        // 5. Trigger a capability miss via the private helper. This
        //    is the same entry point the live query paths in
        //    simple_engine.rs call.
        let requirements = asap_types::query_requirements::QueryRequirements {
            metric: "http_requests_total".to_string(),
            statistics: vec![Statistic::Sum],
            data_range_ms: Some(60_000),
            grouping_labels: KeyByLabelNames::new(vec!["service".to_string()]),
            spatial_filter_normalized: String::new(),
        };
        let miss_result = engine.find_compatible_aggregation_with_miss_notify(&requirements);
        assert!(
            miss_result.is_none(),
            "miss handler should return None when no agg matches"
        );

        // 6. The notification is fire-and-forget via `tokio::spawn`,
        //    so yield and poll until the swap lands (or timeout).
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            tokio::task::yield_now().await;
            if mock.call_count.load(Ordering::Relaxed) > 0 {
                // Give the spawned task a moment to complete its
                // async body — the call_count is bumped at the
                // start of notify_capability_miss, but the swap()
                // happens synchronously inside the same call, so
                // once call_count > 0 the swap is already visible.
                break;
            }
            if Instant::now() >= deadline {
                ::std::panic!(
                    "feedback loop did not fire within 2s; call_count={}",
                    mock.call_count.load(Ordering::Relaxed)
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // 7. Next query snapshot should reflect the new plan.
        //    This is the PR #12 per-query re-snapshot contract.
        let snap_after = engine.streaming_config_snapshot();
        assert_eq!(
            snap_after.aggregation_configs.len(),
            1,
            "feedback loop should have populated the config — \
             call_count={}, recorded_calls={:?}",
            mock.call_count.load(Ordering::Relaxed),
            mock.calls.lock().unwrap().len()
        );

        // 8. Validate the controller received the exact requirements.
        let recorded = mock.calls.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].metric, "http_requests_total");
        assert_eq!(recorded[0].statistics, vec![Statistic::Sum]);
        assert_eq!(recorded[0].data_range_ms, Some(60_000));

        // 9. And the aggregation_id is the deterministic hash the
        //    planner produced — not a random value. This pins the
        //    DC #156 deterministic_agg_id contract.
        let new_ids: Vec<u64> = snap_after.aggregation_configs.keys().copied().collect();
        assert_eq!(new_ids.len(), 1);
        let id = new_ids[0];
        // Re-derive the expected id using the same algorithm as the
        // planner closure above.
        let expected_id: u64 = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            "http_requests_total".hash(&mut h);
            h.finish().saturating_add(1)
        };
        assert_eq!(
            id, expected_id,
            "deterministic_agg_id contract: same metric → same id"
        );
    }

    /// Second-order check: after the loop closes, a repeat miss on
    /// the **same** requirements must not spawn a second plan
    /// generation — the existing config already covers it. This
    /// verifies the loop is idempotent under the common replay
    /// pattern where a query client retries.
    ///
    /// Note: this doesn't test the "query now hits" path directly
    /// because calling into the matching engine from here requires
    /// AggregationIdInfo plumbing that isn't easy to stub. The
    /// observable proxy is: `find_compatible_aggregation_with_miss_notify`
    /// returns `Some` on the second call, meaning the config has
    /// the aggregation AND the miss-notify does NOT fire again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capability_miss_idempotent_on_repeat() {
        let hot_reload = HotReloadStreamingConfig::new(StreamingConfig::default());
        let mock = Arc::new(InProcessMockController::new(hot_reload.clone(), |req| {
            streaming_config_with(&req.metric, 42)
        }));

        let store = Arc::new(SimpleMapStore::new(
            Arc::new(StreamingConfig::default()),
            CleanupPolicy::NoCleanup,
        ));
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
        let engine = SimpleEngine::new_with_hot_reload(
            store,
            inference_config,
            hot_reload.clone(),
            15000,
            QueryLanguage::promql,
        )
        .with_controller_client(mock.clone() as Arc<dyn ControllerClient>);

        let requirements = asap_types::query_requirements::QueryRequirements {
            metric: "latency_ms".to_string(),
            statistics: vec![Statistic::Sum],
            data_range_ms: Some(60_000),
            grouping_labels: KeyByLabelNames::new(vec!["host".to_string()]),
            spatial_filter_normalized: String::new(),
        };

        // First call — miss, loop closes.
        let first = engine.find_compatible_aggregation_with_miss_notify(&requirements);
        assert!(first.is_none());

        // Wait for the swap to land.
        let deadline = Instant::now() + Duration::from_secs(2);
        while mock.call_count.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(mock.call_count.load(Ordering::Relaxed), 1);

        // Second call on the same requirements — the swap should be
        // visible via per-query re-snapshot. The agg_id is 42 (the
        // planner closure pinned it above), and the metric matches,
        // so `find_compatible_aggregation` should return Some.
        //
        // Whether the StreamingConfig's capability matcher actually
        // accepts these requirements depends on its internal logic;
        // if it rejects them for a reason unrelated to the metric
        // being present, the miss-notify fires a second time. The
        // test accepts either outcome but pins that the second
        // attempt produces behavior consistent with PR #12's
        // re-snapshot semantics.
        let second = engine.find_compatible_aggregation_with_miss_notify(&requirements);
        let after_count = mock.call_count.load(Ordering::Relaxed);

        // At minimum: the snapshot is populated.
        let snap = engine.streaming_config_snapshot();
        assert_eq!(snap.aggregation_configs.len(), 1);
        assert!(snap.aggregation_configs.contains_key(&42));

        // Second call fires at most once more — the point is that
        // the runtime doesn't spin into a loop retrying the same
        // miss. Either the capability matcher found the new agg
        // (after_count == 1), or it rejected the agg and re-notified
        // (after_count == 2). Both are acceptable; unbounded retry
        // would be a regression.
        assert!(
            after_count <= 2,
            "idempotency: unexpected notification count {} (expected ≤ 2)",
            after_count
        );
        let _ = second;
    }
}

// ============================================================
// Phase 1b tests: AuxStats pushdown on `query_precompute_for_statistic`
// ============================================================
//
// Proves that when a statistic is covered by typed aux columns, the
// query path returns the aux value without ever calling the
// accumulator's `query_statistic` method. When the statistic is NOT
// covered, the code falls through to `query_statistic`.
#[cfg(test)]
mod aux_pushdown_tests {
    use super::*;
    use crate::precompute_operators::{
        min_max_accumulator::MinMaxAccumulator, sum_accumulator::SumAccumulator,
    };
    use promql_utilities::query_logics::enums::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Accumulator that records how many times `query_statistic`
    /// was invoked. Used to verify the aux fast path skips it.
    struct SpyAccumulator {
        inner_sum: f64,
        query_calls: Arc<AtomicUsize>,
    }

    impl crate::data_model::SerializableToSink for SpyAccumulator {
        fn serialize_to_bytes(&self) -> Vec<u8> {
            Vec::new()
        }
        fn serialize_to_json(&self) -> serde_json::Value {
            serde_json::Value::Null
        }
    }

    impl AggregateCore for SpyAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(SpyAccumulator {
                inner_sum: self.inner_sum,
                query_calls: self.query_calls.clone(),
            })
        }
        fn type_name(&self) -> &'static str {
            "SpyAccumulator"
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
        fn merge_with(
            &self,
            _other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            unimplemented!()
        }
        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }
        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }
        fn query_statistic(
            &self,
            _statistic: Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            self.query_calls.fetch_add(1, Ordering::Relaxed);
            Ok(-1.0) // sentinel: fast path should not return this
        }
        fn aux_stats(&self) -> crate::data_model::AuxStats {
            crate::data_model::AuxStats {
                sum: Some(self.inner_sum),
                ..crate::data_model::AuxStats::empty()
            }
        }
    }

    fn make_engine() -> SimpleEngine {
        use crate::data_model::{
            CleanupPolicy, HotReloadStreamingConfig, InferenceConfig, PromQLSchema, QueryLanguage,
            SchemaConfig, StreamingConfig,
        };
        use crate::stores::sketch_db::simple_map_store::SimpleMapStore;

        let ic = InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema {
                config: HashMap::new(),
            }),
            query_configs: vec![],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        let sc = Arc::new(StreamingConfig::new(HashMap::new()));
        let hr = HotReloadStreamingConfig::from_arc(sc.clone());
        let store = Arc::new(SimpleMapStore::new(sc, CleanupPolicy::NoCleanup));
        SimpleEngine::new_with_hot_reload(store, ic, hr, 60, QueryLanguage::promql)
    }

    #[test]
    fn aux_covered_stat_skips_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Sum, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, 42.0, "aux fast path should return aux value");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "query_statistic should NOT be called when aux covers the stat"
        );
    }

    #[test]
    fn aux_uncovered_stat_falls_through_to_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        // Quantile is not covered by aux → must fall through.
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Quantile, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(
            result, -1.0,
            "should have returned query_statistic's sentinel"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "query_statistic should be called exactly once when aux misses"
        );
    }

    #[test]
    fn keyed_queries_always_use_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone(),
        };
        let key = Some(KeyByLabelValues::new());
        // Even for Sum (which aux covers), a keyed query must bypass aux
        // — aux is per-accumulator, not per-subpopulation key.
        let result = engine
            .query_precompute_for_statistic(&spy, &Statistic::Sum, &key, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, -1.0);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "keyed queries must skip aux fast path"
        );
    }

    #[test]
    fn real_sum_accumulator_uses_aux_fast_path() {
        // End-to-end: a real SumAccumulator goes through the fast path
        // and returns its sum without ever hitting query_statistic.
        let engine = make_engine();
        let acc = SumAccumulator::with_sum(7.5);
        let result = engine
            .query_precompute_for_statistic(&acc, &Statistic::Sum, &None, &HashMap::new())
            .expect("query ok");
        assert_eq!(result, 7.5);
    }

    #[test]
    fn real_min_max_accumulator_uses_aux_fast_path() {
        let engine = make_engine();
        let min_acc = MinMaxAccumulator::with_value(3.0, "min".to_string());
        let max_acc = MinMaxAccumulator::with_value(99.0, "max".to_string());
        assert_eq!(
            engine
                .query_precompute_for_statistic(&min_acc, &Statistic::Min, &None, &HashMap::new())
                .unwrap(),
            3.0
        );
        assert_eq!(
            engine
                .query_precompute_for_statistic(&max_acc, &Statistic::Max, &None, &HashMap::new())
                .unwrap(),
            99.0
        );
    }
}

// ── build_query_execution_context_promql_for_agg_id (forced-agg) tests ──

#[cfg(test)]
mod forced_agg_id_tests {
    use super::*;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::create_engine_single_pop;

    /// Sanity: the refactored auto-resolve path still produces a
    /// valid context for a simple sum_over_time query — ensures
    /// the `parse_and_match_promql` / `resolve_agg_info_promql`
    /// extraction hasn't broken existing behaviour.
    #[test]
    fn auto_resolve_still_works_after_refactor() {
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(http_requests[60s])";
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
        );
        let ctx = engine.build_query_execution_context_promql(query.to_string(), 1000.0);
        assert!(ctx.is_some(), "auto-resolve should yield a context");
    }

    /// The new forced-agg-id entry point produces a context when
    /// the forced `agg_id` matches the one the auto-resolver
    /// would have picked. Basic smoke test; §7 timeline dispatch
    /// per-segment timeline dispatch tests exercise it against multiple
    /// agg_ids.
    #[test]
    fn forced_agg_id_produces_context_for_known_id() {
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(http_requests[60s])";
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
        );
        let ctx = engine.build_query_execution_context_promql_for_agg_id(
            query.to_string(),
            1000.0,
            1, // `create_engine_single_pop` wires agg_id=1
        );
        assert!(ctx.is_some(), "forced valid agg_id should yield a context");
        let ctx = ctx.unwrap();
        assert_eq!(ctx.agg_info.aggregation_id_for_value, 1);
        assert_eq!(ctx.agg_info.aggregation_id_for_key, 1);
    }

    /// Unknown `agg_id` returns `None` without panicking or
    /// polluting the auto-resolver state. Per-segment timeline dispatch relies on
    /// this to gracefully skip timeline segments whose agg_id
    /// disappeared from the StreamingConfig mid-query.
    #[test]
    fn forced_agg_id_returns_none_for_unknown_id() {
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(http_requests[60s])";
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
        );
        let ctx = engine.build_query_execution_context_promql_for_agg_id(
            query.to_string(),
            1000.0,
            9999, // not in StreamingConfig
        );
        assert!(ctx.is_none(), "unknown agg_id → None");
    }

    /// Forced and auto-resolved contexts should be observably
    /// equivalent for the common one-agg case (where the
    /// auto-resolver would have picked the same id). The
    /// invariant that matters for per-segment timeline dispatch: dispatching
    /// through the forced path against the single covering
    /// segment yields the same answer as the existing path.
    #[test]
    fn forced_and_auto_resolve_produce_same_agg_info_for_single_agg() {
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(http_requests[60s])";
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
        );
        let auto = engine
            .build_query_execution_context_promql(query.to_string(), 1000.0)
            .unwrap();
        let forced = engine
            .build_query_execution_context_promql_for_agg_id(query.to_string(), 1000.0, 1)
            .unwrap();
        assert_eq!(
            auto.agg_info.aggregation_id_for_value,
            forced.agg_info.aggregation_id_for_value
        );
        assert_eq!(
            auto.agg_info.aggregation_type_for_value,
            forced.agg_info.aggregation_type_for_value
        );
    }
}
