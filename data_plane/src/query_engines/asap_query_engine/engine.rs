use crate::storage_engines::types::StreamingConfig;
use std::sync::Arc;

use asap_types::query_requirements::QueryRequirements;
use promql_utilities::data_model::KeyByLabelNames;

#[cfg(test)]
use crate::storage_engines::types::KeyByLabelValues;
#[cfg(test)]
use crate::AggregateCore;
#[cfg(test)]
use promql_utilities::query_logics::enums::Statistic;
#[cfg(test)]
use std::collections::HashMap;

/// Simple query engine for processing PromQL-like queries against precomputed data
pub struct ASAPQueryEngine {
    // Phase 5 M2.3.6g — `store: Arc<dyn Store>` field retired. The
    // engine reads precomputes exclusively from `SketchStore` after
    // M2.3.6f. Constructor signatures no longer take a `store` arg.
    /// Hot-reloadable `StreamingConfig` handle. Internal read sites
    /// call `Self::streaming_config_snapshot()` which re-snapshots
    /// from this handle, so runtime swaps pushed through PR #10's
    /// `POST /api/v1/streaming-config` endpoint take effect on the
    /// **next** query without restarting the binary (PR E phase 2).
    /// Clones of `HotReloadStreamingConfig` share the same
    /// underlying `ArcSwap`, so when `main.rs` hands the same handle
    /// to both `ASAPQueryEngine` and `HttpServer::with_hot_reload_config`,
    /// a POST is immediately visible to the next query.
    streaming_config_source: crate::storage_engines::types::HotReloadStreamingConfig,
    #[allow(dead_code)]
    prometheus_scrape_interval: u64,
    /// Optional `ControlPlaneClient` used to notify the control plane
    /// when a query hits a capability miss
    /// (`find_compatible_aggregation` returns `None`). When `None`,
    /// misses fall through to the §5.2 fallback silently, matching
    /// pre-PR-G behavior. Set via `with_control_plane_client`.
    control_plane_client: Option<Arc<dyn crate::drivers::control_plane_client::ControlPlaneClient>>,
    /// Phase 5 — ASAP-tier sketch index. When `Some`, the trait's
    /// `execute` adapter classifies the query's metric/group-by against
    /// the index and short-circuits to `EngineError::CapabilityMiss` when
    /// no ASAP-tier identity covers the request — driving the
    /// EngineRouter's archive failover (Phase 6). When `None`, the
    /// engine behaves as it did before Phase 5 wire-in (every query
    /// goes through `handle_query`'s legacy path).
    sketch_index: Option<Arc<crate::storage_engines::sketch_db::index::SketchStore>>,
    /// Phase-5 hybrid-stitch hook — set by `with_archive_engine` from
    /// `main.rs`'s engine builder. When the ASAP-tier reducer reports a
    /// `ASAPTierResult.coverage` narrower than the requested
    /// `[t0, t1]`, the engine calls into this archive engine to fetch
    /// the missing prefix / suffix and stitches the two answers by
    /// `(label_values, timestamp)`. Warm-tier values win on overlap.
    ///
    /// When `None` (no archive engine wired), the engine returns the
    /// warm answer as-is; the existing `EngineRouter` failover handles
    /// the rest of the routing matrix.
    archive_engine: Option<Arc<dyn crate::query_engines::routing::query_engine_routing::QueryEngine>>}

/// Lifted shape of a `topk(K, sum by (gbk) (rate(metric[r])))` (or
/// `bottomk` / `irate`) query. Produced by
/// [`ASAPQueryEngine::extract_topk_over_rate_shape`] when the
/// analyzer-driven candidate path can't bind to any sid; lets the
/// engine synthesize an ExactAgg(Sum) fallback that runs
/// `evaluate_exact_agg_rate` and post-applies the top-/bottom-k slice
/// in-engine. Scoped tight to the multinode demo shape — broader
/// "frequency-with-fallback" compositions are deferred.
#[derive(Debug, Clone)]
struct TopkOverRateShape {
    k: usize,
    /// `true` for `topk` (descending), `false` for `bottomk` (ascending).
    is_topk: bool,
    metric_name: String,
    group_by_keys: std::collections::BTreeSet<String>,
    range_seconds: u64,
}

impl ASAPQueryEngine {
    /// Construct a `ASAPQueryEngine` with a static `Arc<StreamingConfig>`.
    /// Wraps the config in a fresh `HotReloadStreamingConfig` internally
    /// — callers that need to share the hot-reload handle with the HTTP
    /// server should use `new_with_hot_reload` instead so a POST to
    /// `/api/v1/streaming-config` is visible to both. The `_static`
    /// variant stays as the simple entry point for tests, binaries,
    /// and legacy callers that don't own a `HotReloadStreamingConfig`.
    pub fn new(
        streaming_config: Arc<StreamingConfig>,
        prometheus_scrape_interval: u64,
    ) -> Self {
        let hot_reload = crate::storage_engines::types::HotReloadStreamingConfig::from_arc(streaming_config);
        Self::new_with_hot_reload(hot_reload, prometheus_scrape_interval)
    }

    /// Construct a `ASAPQueryEngine` that shares a `HotReloadStreamingConfig`
    /// handle with another holder (typically the HTTP server). This is
    /// the constructor `main.rs` should call so `POST /api/v1/streaming-config`
    /// is observable by the next query.
    pub fn new_with_hot_reload(
        streaming_config_source: crate::storage_engines::types::HotReloadStreamingConfig,
        prometheus_scrape_interval: u64,
    ) -> Self {
        Self {
            streaming_config_source,
            prometheus_scrape_interval,
            control_plane_client: None,
            sketch_index: None,
            archive_engine: None}
    }

    /// Phase-5 hybrid-stitch builder — attach an archive engine the
    /// `QueryEngine` trait adapter will dispatch to when the ASAP-tier
    /// reducer reports a coverage narrower than the requested range.
    /// When `None`, the engine returns whatever the ASAP tier covers.
    pub fn with_archive_engine(
        mut self,
        archive: Arc<dyn crate::query_engines::routing::query_engine_routing::QueryEngine>,
    ) -> Self {
        self.archive_engine = Some(archive);
        self
    }

    /// Phase 5 — attach the shared `SketchStore` so the `QueryEngine`
    /// trait adapter's classify+failover logic is active. Without this
    /// call, the engine keeps the pre-Phase-5 behavior (route every
    /// query through `handle_query`).
    pub fn with_sketch_index(
        mut self,
        index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Self {
        self.sketch_index = Some(index);
        self
    }

    /// Take a fresh snapshot of the current `StreamingConfig`. Each
    /// call observes whatever was most recently pushed through PR #10's
    /// `POST /api/v1/streaming-config` endpoint. The returned `Arc`
    /// is stable for the caller's lifetime — a concurrent swap
    /// produces a new `Arc` and leaves the one returned here alone.
    ///
    /// Internal read sites inside `ASAPQueryEngine` bind this once per
    /// logical unit of work (typically per query-handler invocation
    /// or per helper call) and use the local Arc for the duration,
    /// so references into the underlying `StreamingConfig` stay
    /// valid and a single query sees internally-consistent config
    /// fields even if a concurrent swap lands mid-query.
    pub fn streaming_config_snapshot(&self) -> Arc<StreamingConfig> {
        self.streaming_config_source.snapshot()
    }

    /// Attach a `ControlPlaneClient` so capability misses fire a
    /// fire-and-forget notification to the DataCollector controller.
    /// Builder-style method — takes self by value and returns it so
    /// construction in `main.rs` chains neatly. Without this call,
    /// capability misses fall through to the §5.2 fallback silently,
    /// matching pre-PR-G behavior.
    pub fn with_control_plane_client(
        mut self,
        client: Arc<dyn crate::drivers::control_plane_client::ControlPlaneClient>,
    ) -> Self {
        self.control_plane_client = Some(client);
        self
    }

    /// Resolve the timeline of agg-signatures for a metric over a
    /// query range. Reads exclusively from the sid catalog via
    /// [`crate::storage_engines::sketch_db::query::timeline::timeline_for_metric`]
    /// — schema retirement routed this away from the now-deleted
    /// per-metric schema registry.
    ///
    /// When no `SketchStore` is wired (test contexts that never
    /// installed one via [`Self::with_sketch_index`]) returns an
    /// empty vector; downstream dispatch then bails to the default
    /// single-agg path, identical to the pre-retirement behaviour
    /// where an empty schema registry produced no segments.
    pub fn timeline_for_query(
        &self,
        metric: &str,
        t1_ms: u64,
        t2_ms: u64,
    ) -> Vec<crate::storage_engines::sketch_db::TimelineSegment> {
        let Some(idx) = self.sketch_index.as_ref() else {
            return Vec::new();
        };
        crate::storage_engines::sketch_db::query::timeline::timeline_for_metric(
            idx.as_ref(),
            metric,
            t1_ms,
            t2_ms,
        )
    }

    /// Build a `QueryRequirements` from an ASAP-tier candidate (the
    /// shape modern `execute()` works with). Used by the modern miss
    /// branches to fire the capability-miss notify the legacy
    /// `find_compatible_aggregation_with_miss_notify` path provided
    /// before the B7.5 retirement.
    fn requirements_from_candidate(
        candidate: &control_plane::asap_tier_analysis::ASAPTierCandidate,
    ) -> QueryRequirements {
        QueryRequirements {
            metric: candidate.metric_name.clone(),
            statistics: Vec::new(),
            data_range_ms: if candidate.range_seconds > 0 {
                Some(candidate.range_seconds.saturating_mul(1000))
            } else {
                None
            },
            grouping_labels: KeyByLabelNames::new(
                candidate.group_by_keys.iter().cloned().collect(),
            ),
            spatial_filter_normalized: candidate.spatial_filter_canonical.clone(),
        }
    }

    /// Build a minimal `QueryRequirements` from a bare PromQL string —
    /// used by the no-sketch-index miss branch in modern `execute()`,
    /// where we don't have a parsed candidate (analysis was skipped)
    /// but still want to fire the capability-miss notify so the
    /// control-plane feedback loop closes. Lifts (metric_name,
    /// group_by_keys) from the AST via a light walker; returns `None`
    /// for queries that don't reference a concrete metric.
    fn requirements_from_query_str(query: &str) -> Option<QueryRequirements> {
        use promql_parser::parser::Expr;
        let ast = promql_parser::parser::parse(query).ok()?;
        fn walk(expr: &Expr) -> Option<(String, std::collections::BTreeSet<String>)> {
            match expr {
                Expr::VectorSelector(vs) => {
                    let mut keys = std::collections::BTreeSet::new();
                    let mut metric = vs.name.clone().unwrap_or_default();
                    for m in &vs.matchers.matchers {
                        if m.name == "__name__" {
                            if metric.is_empty() {
                                metric = m.value.clone();
                            }
                            continue;
                        }
                        keys.insert(m.name.clone());
                    }
                    if metric.is_empty() {
                        None
                    } else {
                        Some((metric, keys))
                    }
                }
                Expr::MatrixSelector(ms) => walk(&Expr::VectorSelector(ms.vs.clone())),
                Expr::Call(call) => call.args.args.iter().find_map(|a| walk(a)),
                Expr::Aggregate(agg) => walk(&agg.expr),
                Expr::Binary(bin) => walk(&bin.lhs).or_else(|| walk(&bin.rhs)),
                Expr::Subquery(sq) => walk(&sq.expr),
                Expr::Paren(p) => walk(&p.expr),
                Expr::Unary(u) => walk(&u.expr),
                _ => None,
            }
        }
        let (metric, keys) = walk(&ast)?;
        Some(QueryRequirements {
            metric,
            statistics: Vec::new(),
            data_range_ms: None,
            grouping_labels: KeyByLabelNames::new(keys.into_iter().collect()),
            spatial_filter_normalized: String::new(),
        })
    }

    /// Detect whether the raw PromQL contains a `rate(...)` or
    /// `irate(...)` call anywhere in the expression tree. Used by the
    /// engine to route ExactAgg(Sum) candidates through the
    /// rate-divisor reducer (`evaluate_exact_agg_rate`) instead of the
    /// plain per-window reducer.
    ///
    /// The control plane's analyzer collapses `rate(metric[r])` to the
    /// same `Capability::ExactAgg(Sum)` candidate that `sum(metric)`
    /// uses — the trace carries `range_seconds` and `function` strings
    /// but for the composed shape `sum by (zone) (rate(metric[r]))` the
    /// outer function string is `"sum"` (not `"rate"`), so we can't
    /// disambiguate `sum_over_time(...)` from `sum(rate(...))` from the
    /// candidate alone. Walking the raw PromQL AST is the cleanest
    /// disambiguator that doesn't require analyzer changes.
    ///
    /// Returns `false` for unparseable input (the analyzer would have
    /// already rejected — defensive).
    fn query_contains_rate_call(query: &str) -> bool {
        use promql_parser::parser::Expr;
        let ast = match promql_parser::parser::parse(query) {
            Ok(a) => a,
            Err(_) => return false,
        };
        fn walk(expr: &Expr) -> bool {
            match expr {
                Expr::Call(call) => {
                    let name = call.func.name.to_lowercase();
                    if name == "rate" || name == "irate" {
                        return true;
                    }
                    call.args.args.iter().any(|a| walk(a))
                }
                Expr::Aggregate(agg) => walk(&agg.expr),
                Expr::Paren(p) => walk(&p.expr),
                Expr::Subquery(sq) => walk(&sq.expr),
                Expr::Binary(b) => walk(&b.lhs) || walk(&b.rhs),
                Expr::Unary(u) => walk(&u.expr),
                _ => false,
            }
        }
        walk(&ast)
    }

    /// Detect a `topk(k, <inner>)` (or `bottomk`) at the root of the
    /// PromQL AST and lift `(k, inner_metric, inner_group_by_keys,
    /// inner_range_seconds, is_topk)`. The `is_topk` flag distinguishes
    /// `topk` (descending slice) from `bottomk` (ascending slice).
    /// Returns `None` for any non-topk-shaped query, or for shapes the
    /// fallback path doesn't try to recover (the analyzer's path is
    /// preferred whenever it succeeds — this helper only fires when
    /// analyzer candidates fail to bind to any sid).
    ///
    /// Specifically matches the multinode demo shape:
    ///   `topk(K, sum by (gbk...) (rate(metric[r])))`
    /// — plus its `bottomk` / `irate` variants. The synthesized
    /// `ExactAgg(Sum)` candidate uses the inner `metric` +
    /// `group_by_keys` for `instances_matching` and the inner range
    /// for the rate divisor.
    fn extract_topk_over_rate_shape(
        query: &str,
    ) -> Option<TopkOverRateShape> {
        use promql_parser::parser::{Expr, LabelModifier};
        let ast = promql_parser::parser::parse(query).ok()?;
        // Strip a leading Paren so `(topk(...))` works.
        fn unparen(e: &Expr) -> &Expr {
            match e {
                Expr::Paren(p) => unparen(&p.expr),
                other => other,
            }
        }
        let root = unparen(&ast);
        let agg = match root {
            Expr::Aggregate(a) => a,
            _ => return None,
        };
        let op = agg.op.to_string().to_lowercase();
        let is_topk = match op.as_str() {
            "topk" => true,
            "bottomk" => false,
            _ => return None,
        };
        let k = match agg.param.as_ref() {
            Some(p) => match p.as_ref() {
                Expr::NumberLiteral(nl) => nl.val as usize,
                _ => return None,
            },
            None => return None,
        };
        if k == 0 {
            return None;
        }
        // Walk the inner expression to find (group_by_keys,
        // range_seconds, metric_name). For the multinode demo shape
        // the inner is a `sum by (zone) (...)` Aggregate whose own
        // inner is a Call("rate", [MatrixSelector(metric[r])]).
        // Also accept `topk(K, rate(metric[r]))` (no inner sum) — the
        // implicit group is each metric series.
        let inner = unparen(&agg.expr);

        let mut group_by_keys: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        let inner_expr = if let Expr::Aggregate(inner_agg) = inner {
            // `sum by (gbk) (...)`; capture the by-modifier and recurse
            // into its expr to find rate(...) below.
            if let Some(LabelModifier::Include(labels)) = &inner_agg.modifier {
                for l in &labels.labels {
                    group_by_keys.insert(l.clone());
                }
            }
            // Only `sum`-like inner aggregates compose meaningfully
            // with rate(...) under topk for ExactAgg(Sum) fallback;
            // anything else (avg/min/max/count) doesn't safely
            // reduce to a per-(group) Σwindow_sum / range.
            let inner_op = inner_agg.op.to_string().to_lowercase();
            if !matches!(inner_op.as_str(), "sum") {
                return None;
            }
            unparen(&inner_agg.expr)
        } else {
            inner
        };

        // `inner_expr` should be a Call("rate"/"irate", [MatrixSelector]).
        let call = match inner_expr {
            Expr::Call(c) => c,
            _ => return None,
        };
        let call_name = call.func.name.to_lowercase();
        if !matches!(call_name.as_str(), "rate" | "irate") {
            return None;
        }
        let matrix = call.args.args.iter().find_map(|a| match a.as_ref() {
            Expr::MatrixSelector(ms) => Some(ms),
            _ => None,
        })?;
        let range_seconds = matrix.range.as_secs();
        if range_seconds == 0 {
            return None;
        }
        let metric_name = matrix.vs.name.clone().unwrap_or_else(|| {
            // Fallback: pull `__name__` out of matchers.
            matrix
                .vs
                .matchers
                .matchers
                .iter()
                .find(|m| m.name == "__name__")
                .map(|m| m.value.clone())
                .unwrap_or_default()
        });
        if metric_name.is_empty() {
            return None;
        }
        Some(TopkOverRateShape {
            k,
            is_topk,
            metric_name,
            group_by_keys,
            range_seconds,
        })
    }

    /// Engine-side fallback for `topk(K, sum by (gbk) (rate(metric[r])))`
    /// shapes that the control plane's analyzer routes to FrequencyTopk
    /// + FrequencyEstimate (because PromQL's `topk` lowers to
    /// `AggIntent::TopK` and the optimizer's CMS-topk binder rewrites
    /// the inner Sum into Frequency). Those capabilities don't match
    /// the ExactAgg(Sum) sids the MVP demo registers — without this
    /// fallback the multinode demo query falls over to archive.
    ///
    /// Approach:
    /// 1. Lift `(K, metric, group_by_keys, range_seconds, is_topk)`
    ///    from the raw PromQL AST via [`Self::extract_topk_over_rate_shape`].
    /// 2. Find ExactAgg(Sum) sids via `instances_matching(metric, gbk)`
    ///    (subset-match, mirrors the analyzer-driven path).
    /// 3. Run `evaluate_exact_agg_rate` to fold per-(group) rates.
    /// 4. Post-apply the top-/bottom-k slice in-engine: sort each
    ///    series by its last-sample value descending (topk) or
    ///    ascending (bottomk) and keep the first `K`.
    ///
    /// Returns:
    /// * `Ok(Some(QueryResult))` — fallback fired and produced a vector
    ///   result; engine should return this directly.
    /// * `Ok(None)` — shape didn't match (analyzer-driven path should
    ///   run unchanged).
    /// * `Err(_)` — shape matched but execution failed (no sids found,
    ///   no in-window data, reducer error etc.); callers can choose
    ///   to surface as CapabilityMiss → archive failover.
    fn try_topk_over_rate_fallback(
        &self,
        query: &str,
        now_ms: u64,
    ) -> Result<
        Option<crate::query_engines::query_result::QueryResult>,
        crate::query_engines::EngineError,
    > {
        let Some(idx) = self.sketch_index.as_ref() else {
            return Ok(None);
        };
        let Some(shape) = Self::extract_topk_over_rate_shape(query) else {
            return Ok(None);
        };

        // Find ExactAgg(Sum)-class sids for (metric, gbk). The
        // instances_matching call is subset-match: a sid registered
        // with `[zone, rack, node]` answers a `[zone]` query, matching
        // the rest of the engine's sid-resolution semantics.
        let candidate_sids: Vec<u64> =
            idx.instances_matching(&shape.metric_name, &shape.group_by_keys);
        if candidate_sids.is_empty() {
            // No sids at all — let the analyzer-driven path produce
            // its standard "no policy" error.
            return Ok(None);
        }
        // Filter to ExactAgg(Sum-family) sids only — frequency sids
        // for the same metric should fall back to the analyzer's
        // FrequencyTopk path (which will rightly capability-miss
        // until CMS-with-heap is wired).
        let mut hit_sids: Vec<u64> = Vec::new();
        for sid in &candidate_sids {
            let Some(meta) = idx.instance(*sid) else { continue };
            if let Some(cap) = meta.capability.as_ref() {
                use crate::storage_engines::sketch_db::data::AggregationType;
                use crate::storage_engines::sketch_db::index::Capability;
                if matches!(
                    cap,
                    Capability::ExactAgg(
                        AggregationType::Sum
                            | AggregationType::MultipleSum
                            | AggregationType::Increase
                            | AggregationType::MultipleIncrease
                    )
                ) {
                    hit_sids.push(*sid);
                }
            }
        }
        if hit_sids.is_empty() {
            return Ok(None);
        }

        // Pick the agg_type from the first sid; all hits share the
        // same family by construction (Sum / Increase variants are
        // accumulator-compatible via `merge_with` / `Statistic::Sum`).
        let agg_type = {
            use crate::storage_engines::sketch_db::data::AggregationType;
            let first = idx.instance(hit_sids[0]).and_then(|m| {
                match m.capability.as_ref()? {
                    crate::storage_engines::sketch_db::index::Capability::ExactAgg(t) => Some(*t),
                    _ => None,
                }
            });
            first.unwrap_or(AggregationType::Sum)
        };

        let lookback_ms = shape.range_seconds.saturating_mul(1000);
        let t0_ms = now_ms.saturating_sub(lookback_ms);

        let reducer = crate::storage_engines::sketch_db::query::SketchReducer::new(idx);
        let result = reducer
            .evaluate_exact_agg_rate(
                &hit_sids,
                agg_type,
                &shape.group_by_keys,
                shape.range_seconds,
                t0_ms,
                now_ms,
            )
            .map_err(|e| {
                crate::query_engines::EngineError::capability_miss(
                    asap_types::StorageBackend::SketchStore.data_source_id(),
                    format!(
                        "SketchStore topk-over-rate fallback reducer failed for `{query}`: \
                         {e:?} — failing over to archive"
                    ),
                )
            })?;

        // Post-apply topk / bottomk: sort series by their (single)
        // sample value and slice. `evaluate_exact_agg_rate` emits ONE
        // sample per series so the comparison is unambiguous.
        let mut series_with_value: Vec<(
            std::collections::BTreeMap<String, String>,
            Vec<(i64, f64)>,
            f64,
        )> = result
            .series
            .into_iter()
            .filter_map(|(labels, samples)| {
                let v = samples.last().map(|(_, v)| *v)?;
                Some((labels, samples, v))
            })
            .collect();
        // Sort: topk = descending by value, bottomk = ascending.
        if shape.is_topk {
            series_with_value.sort_by(|a, b| {
                b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            series_with_value.sort_by(|a, b| {
                a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        series_with_value.truncate(shape.k);
        let sliced: Vec<(std::collections::BTreeMap<String, String>, Vec<(i64, f64)>)> =
            series_with_value
                .into_iter()
                .map(|(l, s, _)| (l, s))
                .collect();

        let sliced_result = crate::storage_engines::sketch_db::query::ASAPTierResult {
            series: sliced,
            coverage: result.coverage,
        };
        // Instant-query result shape (Vector, not Matrix). topk over
        // an instant aggregation is itself an instant vector — one
        // value per series in the slice.
        let qr = asap_tier_result_to_query_result(sliced_result, now_ms, false);
        Ok(Some(qr))
    }

    #[cfg(test)]
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

    /// Modern warm-tier path for `/api/v1/query_range` — the range-
    /// query equivalent of the `QueryEngine::execute(&str)` trait
    /// surface. Used by the HTTP server as a fallback when the legacy
    /// `handle_range_query_promql` returns `None`.
    ///
    /// Time semantics follow Prometheus's
    /// `/api/v1/query_range?start&end&step` spec: the result is a
    /// `matrix` (one row per series, each row carrying multiple
    /// (timestamp, value) samples). The warm-tier reducer naturally
    /// produces one sample per window_close in `[start, end]`, so
    /// the matrix is sampled at the underlying aggregation's window
    /// boundaries — typically a finer grid than the user's `step`
    /// when window_size < step. (The Prometheus spec says
    /// evaluate at each step `t = start, start+step, …, end`; the
    /// warm tier returns at native window-close granularity instead.
    /// This is more data, not less — clients that expect exact step
    /// timestamps can downsample, or route step-precise queries to
    /// the cold tier via the EngineRouter.)
    ///
    /// `step` is currently accepted for API compatibility but unused
    /// — see the granularity-mismatch note above.
    pub async fn execute_range_promql_modern(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        _step_ms: u64,
    ) -> Result<
        crate::query_engines::query_result::QueryResult,
        crate::query_engines::EngineError,
    > {
        let Some(idx) = self.sketch_index.as_ref() else {
            return Err(crate::query_engines::EngineError::capability_miss(
                asap_types::StorageBackend::SketchStore.data_source_id(),
                format!("ASAPQueryEngine: no sketch index for `{query}` — failing over"),
            ));
        };

        let analysis =
            control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(query);

        if let Some(reason) = &analysis.unsupported {
            return Err(crate::query_engines::EngineError::capability_miss(
                asap_types::StorageBackend::SketchStore.data_source_id(),
                format!(
                    "SketchStore analyzer rejected `{query}` for range query: \
                     {reason:?} — failing over to archive"
                ),
            ));
        }
        if analysis.candidates.is_empty() {
            return Err(crate::query_engines::EngineError::capability_miss(
                asap_types::StorageBackend::SketchStore.data_source_id(),
                format!(
                    "SketchStore analyzer produced no ASAP-tier candidates for \
                     `{query}` — failing over to archive"
                ),
            ));
        }

        let streaming_snap = self.streaming_config_snapshot();
        let policy_registry = streaming_snap.policy_registry();
        let reducer = crate::storage_engines::sketch_db::query::SketchReducer::new(idx);
        let mut combined_result: Option<
            crate::storage_engines::sketch_db::query::ASAPTierResult,
        > = None;

        for candidate in &analysis.candidates {
            // Resolve candidate → {sids} via the sid catalog. Schema-
            // retirement #5: prefer `instances_matching` over the
            // policy-fp reverse index — it's the more general
            // primitive and works whether or not the ingest path was
            // able to bind the sid back to a streaming-config policy.
            //
            // History: an earlier PR removed an `instances_matching`
            // fallback under the assumption every production sid
            // registration would populate `policy_fp`. The MVP smoke
            // test (issue #271 / tracking #272) showed that
            // assumption is wrong — sketches arriving from the agent
            // carry the full wire-attr set rather than the streaming-
            // config's `grouping_labels` subset, so
            // `derive_sketch_policy_fp` returns `UNSET` and
            // `sids_for_policy(fp)` returns empty. The agg_id-aware
            // path is preserved for ExactAgg sids minted via
            // `ingest_precompute_for_agg_config` (those carry a
            // populated `policy_fp`) but its result is unioned with
            // the catalog-walk result so we don't miss the sketches.
            let policy_fps = control_plane::asap_tier_analysis::find_matching_policies(
                &policy_registry,
                candidate,
            );
            let mut sids: std::collections::BTreeSet<u64> =
                std::collections::BTreeSet::new();
            for fp in &policy_fps {
                sids.extend(idx.sids_for_policy(*fp));
            }
            sids.extend(idx.instances_matching(
                &candidate.metric_name,
                &candidate.group_by_keys,
            ));
            if sids.is_empty() {
                return Err(crate::query_engines::EngineError::capability_miss(
                    asap_types::StorageBackend::SketchStore.data_source_id(),
                    format!(
                        "SketchStore has no policy for metric `{}` satisfying \
                         capability {:?} — failing over to archive",
                        candidate.metric_name, candidate.required_capability,
                    ),
                ));
            }

            let required: crate::storage_engines::sketch_db::index::Capability =
                candidate.required_capability.clone();
            let mut hit_sids: Vec<u64> = Vec::with_capacity(sids.len());
            for sid in &sids {
                let meta = match idx.instance(*sid) {
                    Some(m) => m,
                    None => continue,
                };
                if let Some(cap) = meta.capability.as_ref() {
                    if required.is_satisfied_by(cap) {
                        hit_sids.push(*sid);
                    }
                }
            }
            if hit_sids.is_empty() {
                return Err(crate::query_engines::EngineError::capability_miss(
                    asap_types::StorageBackend::SketchStore.data_source_id(),
                    format!(
                        "SketchStore has no sid satisfying capability {:?} for \
                         metric `{}` — failing over to archive",
                        candidate.required_capability, candidate.metric_name
                    ),
                ));
            }

            // ExactAgg capability → dispatch the per-(group_by_keys)
            // accumulator-merge path; sketch capabilities → the
            // sketch-decode path. For ExactAgg(Sum-family) candidates,
            // if the raw PromQL contains a `rate(...)` / `irate(...)`
            // call AND the candidate's range_seconds > 0, dispatch to
            // `evaluate_exact_agg_rate` instead — that path folds all
            // sub-window sums and divides by the range to produce
            // events-per-second. See `SketchReducer::evaluate_exact_agg`
            // / `evaluate_exact_agg_rate` for the per-path semantics.
            let result = match &candidate.required_capability {
                crate::storage_engines::sketch_db::index::Capability::ExactAgg(agg_type) => {
                    let use_rate_path = candidate.range_seconds > 0
                        && Self::query_contains_rate_call(query)
                        && matches!(
                            agg_type,
                            crate::storage_engines::sketch_db::data::AggregationType::Sum
                                | crate::storage_engines::sketch_db::data::AggregationType::MultipleSum
                                | crate::storage_engines::sketch_db::data::AggregationType::Increase
                                | crate::storage_engines::sketch_db::data::AggregationType::MultipleIncrease
                        );
                    if use_rate_path {
                        reducer
                            .evaluate_exact_agg_rate(
                                &hit_sids,
                                *agg_type,
                                &candidate.group_by_keys,
                                candidate.range_seconds,
                                start_ms,
                                end_ms,
                            )
                            .map_err(|e| {
                                crate::query_engines::EngineError::capability_miss(
                                    asap_types::StorageBackend::SketchStore.data_source_id(),
                                    format!(
                                        "SketchStore exact-agg rate reducer failed for `{query}` over \
                                         [{start_ms}, {end_ms}]: {e:?} — failing over to archive"
                                    ),
                                )
                            })?
                    } else {
                        reducer
                            .evaluate_exact_agg(
                                &hit_sids,
                                *agg_type,
                                &candidate.group_by_keys,
                                start_ms,
                                end_ms,
                            )
                            .map_err(|e| {
                                crate::query_engines::EngineError::capability_miss(
                                    asap_types::StorageBackend::SketchStore.data_source_id(),
                                    format!(
                                        "SketchStore exact-agg reducer failed for `{query}` over \
                                         [{start_ms}, {end_ms}]: {e:?} — failing over to archive"
                                    ),
                                )
                            })?
                    }
                }
                _ => reducer
                    .evaluate(
                        &hit_sids,
                        &candidate.function,
                        &candidate.function_args,
                        start_ms,
                        end_ms,
                    )
                    .map_err(|e| {
                        crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer failed for `{query}` over \
                                 [{start_ms}, {end_ms}]: {e:?} — failing over to archive"
                            ),
                        )
                    })?,
            };
            combined_result = Some(result);
        }

        let result = combined_result.ok_or_else(|| {
            crate::query_engines::EngineError::capability_miss(
                asap_types::StorageBackend::SketchStore.data_source_id(),
                format!("SketchStore reducer produced no result for `{query}`"),
            )
        })?;

        // Matrix shape — the range_query wire format requires it.
        Ok(asap_tier_result_to_query_result(result, end_ms, true))
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

/// Adapt a [`crate::storage_engines::sketch_db::query::ASAPTierResult`] to the engine's
/// existing `QueryResult` shape. The reducer hands back per-series
/// time-stamped scalars; we materialize them as a
/// `QueryResult::Matrix` whose [`crate::query_engines::query_result::RangeVectorElement`]s
/// each map onto one (label-values, samples) entry.
///
/// `now_ms` is unused for the matrix variant (each sample carries its
/// own window-end timestamp); it's plumbed for future extension to
/// the instant-vector case (latest-pane projection).
/// Merge a ASAP-tier `QueryResult::Matrix` with an archive
/// `QueryResult::Matrix` by `(label_values, timestamp)`. Samples whose
/// timestamps fall inside the warm coverage `(cov_lo, cov_hi)` keep
/// the warm value (warm is approximate but more recent); samples
/// outside that window come from the archive answer. For
/// labels-not-present-in-warm series the archive series is taken in
/// full. Used by `ASAPQueryEngine`'s hybrid-stitch path when the
/// ASAP-tier reducer reports `coverage` narrower than the request.
fn stitch_warm_and_archive(
    warm: crate::query_engines::query_result::QueryResult,
    archive: crate::query_engines::query_result::QueryResult,
    cov_lo: u64,
    cov_hi: u64,
) -> crate::query_engines::query_result::QueryResult {
    use crate::query_engines::query_result::{QueryResult, RangeVectorElement, Sample};
    use std::collections::BTreeMap;

    let warm_matrix = match &warm {
        QueryResult::Matrix(m) => m.values.clone(),
        _ => return archive};
    let archive_matrix = match &archive {
        QueryResult::Matrix(m) => m.values.clone(),
        QueryResult::Vector(_) => return warm};

    // Index warm series by labels for fast lookup.
    let mut by_labels: BTreeMap<Vec<String>, RangeVectorElement> = BTreeMap::new();
    for el in warm_matrix {
        by_labels.insert(el.labels.labels.clone(), el);
    }

    // For each archive series, merge into by_labels.
    for arch_el in archive_matrix {
        let entry = by_labels
            .entry(arch_el.labels.labels.clone())
            .or_insert_with(|| RangeVectorElement::new(arch_el.labels.clone()));
        // Build a set of warm timestamps inside coverage (kept).
        let warm_ts: std::collections::HashSet<u64> = entry
            .samples
            .iter()
            .filter(|s| s.timestamp >= cov_lo && s.timestamp <= cov_hi)
            .map(|s| s.timestamp)
            .collect();
        // Drop any warm samples that ended up outside coverage —
        // archive will replace them.
        entry
            .samples
            .retain(|s| s.timestamp >= cov_lo && s.timestamp <= cov_hi);
        for s in arch_el.samples {
            // Skip archive samples whose timestamps fall inside warm
            // coverage AND warm produced a value there (warm wins).
            if s.timestamp >= cov_lo && s.timestamp <= cov_hi && warm_ts.contains(&s.timestamp) {
                continue;
            }
            entry.samples.push(Sample::new(s.timestamp, s.value));
        }
        entry.samples.sort_by_key(|s| s.timestamp);
    }

    let elements: Vec<RangeVectorElement> = by_labels.into_values().collect();
    QueryResult::matrix(elements)
}

fn asap_tier_result_to_query_result(
    result: crate::storage_engines::sketch_db::query::ASAPTierResult,
    now_ms: u64,
    is_range_query: bool,
) -> crate::query_engines::query_result::QueryResult {
    use crate::storage_engines::types::KeyByLabelValues;
    use crate::query_engines::query_result::{
        InstantVectorElement, QueryResult, RangeVectorElement,
    };

    // Instant-query result-shape: the Prometheus adapter's
    // `format_success_response` rejects `Matrix` for queries the
    // analyzer marked as instant (`range_seconds == 0`) — produces a
    // 500 ”shape mismatch”. Project the per-series last sample into
    // an `InstantVectorElement` and wrap as `Vector` so the wire
    // response carries `resultType: vector` matching the request.
    if !is_range_query {
        let mut elements: Vec<InstantVectorElement> = Vec::with_capacity(result.series.len());
        for (label_values, samples) in result.series {
            // Mirror the range-vector branch: BTreeMap iteration is
            // key-sorted, so `unzip` produces aligned (keys, values).
            // Stash the keys in the per-element `label_keys_override`
            // so the Prometheus adapter renders synthesized keys
            // (notably ASAP-tier `topk`'s `"item"` key) instead of
            // the empty `metric: {}` it would produce when the
            // query-scoped `KeyByLabelNames` is empty.
            let (keys, values): (Vec<String>, Vec<String>) = label_values.into_iter().unzip();
            let labels = KeyByLabelValues::new_with_labels(values);
            // Take the latest sample (the reducer returns one per
            // window_end; for instant readout we want the most recent).
            if let Some((_, value)) = samples.into_iter().last() {
                elements.push(
                    InstantVectorElement::new(labels, value)
                        .with_label_keys_override(keys),
                );
            }
        }
        return QueryResult::vector(elements, now_ms);
    }

    let mut elements: Vec<RangeVectorElement> = Vec::with_capacity(result.series.len());
    for (label_values, samples) in result.series {
        // `KeyByLabelValues` is a `Vec<String>` carrying VALUES only;
        // the serializer pairs them with KEYS from a query-scoped
        // `KeyByLabelNames`. For most queries the keys ARE the
        // query's group-by clause, so the default path works. But
        // ASAP-tier `topk` synthesizes an `"item"` key (the top-k
        // entry name) that the original query's group-by doesn't
        // carry — without an override the serializer drops it and
        // the response shows `"metric": {}`. Project the BTreeMap's
        // VALUES in key-sorted order (BTreeMap iteration is
        // key-sorted), and stash the BTreeMap's KEYS in the
        // per-element override so the serializer can pair them
        // correctly.
        let (keys, values): (Vec<String>, Vec<String>) = label_values.into_iter().unzip();
        let labels = KeyByLabelValues::new_with_labels(values);
        let mut element = RangeVectorElement::new(labels).with_label_keys_override(keys);
        for (window_end_ms, value) in samples {
            // `window_end_ms` is i64 from the index; cast to u64
            // for the wire format (window_end is monotonic + post-
            // 1970 in production).
            let ts = if window_end_ms >= 0 {
                window_end_ms as u64
            } else {
                0
            };
            element.add_sample(ts, value);
        }
        elements.push(element);
    }
    QueryResult::matrix(elements)
}

#[async_trait::async_trait]
impl crate::query_engines::routing::query_engine_routing::QueryEngine for ASAPQueryEngine {
    async fn execute(
        &self,
        query: &str,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError> {
        // Phase 9 controller-unification (2026-05) — the ASAP-tier
        // hook is now a thin driver around the control plane's
        // `analyze_promql_for_asap_tier`. The analyzer is the single
        // owner of "is this PromQL ASAP-tier-answerable" knowledge.
        // We drop into one of three branches:
        //
        // 1. `ASAPTierAnalysis::unsupported` is `Some(_)` — the
        //    PromQL shape isn't ASAP-tier-servable. Surface as
        //    `EngineError::CapabilityMiss(SketchStore, …)` with the
        //    structured `UnsupportedReason` in the detail string. The
        //    EngineRouter fails over to the archive engine. This
        //    covers all of:
        //      * `MissReason::UnsupportedFunction(_)` (rate, irate,
        //        increase, etc.) → cold tier (archive)
        //      * `MissReason::UnsupportedComposition(_)` (sum-by,
        //        topk-over-rate, etc.) → cold tier
        //      * `MissReason::NoCallNodeFound` (bare selector) →
        //        cold tier (archive answers raw selectors)
        //      * `MissReason::UnparseablePromql(_)` → cold tier
        //        (archive's parser may be more permissive, or it'll
        //        also reject and the user sees the error)
        //
        // 2. `ASAPTierAnalysis::candidates` is populated, but ANY
        //    candidate's `instances_matching` returns empty OR a
        //    sid that classifies as `Ghost`/`Unknown` — surface
        //    as CapabilityMiss. The EngineRouter falls over.
        //
        // 3. All candidates resolve to all-`Hit` sids — dispatch
        //    each to the per-`Capability` sketch reducer. Today's
        //    semantic: ANY candidate-level reducer error → fall
        //    over to archive (no per-candidate hybrid stitch yet —
        //    that's the documented follow-up).
        if let Some(idx) = self.sketch_index.as_ref() {
            let analysis = control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(query);

            // ── topk-over-rate fallback (multinode demo) ────────────
            // Detect `topk(K, sum by (gbk) (rate(metric[r])))` shapes
            // upfront and route them to the ExactAgg(Sum) reducer +
            // engine-side topk slice. The analyzer's lowerer rewrites
            // this shape into FrequencyTopk + FrequencyEstimate
            // candidates which don't match ExactAgg(Sum) sids — so the
            // analyzer-driven path below would CapabilityMiss. The
            // fallback returns `Ok(None)` when the shape doesn't match
            // OR no ExactAgg(Sum) sids exist, letting the
            // analyzer-driven path run as usual.
            //
            // Defer the `let _ = idx` capture: the helper takes `&self`
            // and re-reads `sketch_index` internally.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            match self.try_topk_over_rate_fallback(query, now_ms) {
                Ok(Some(qr)) => return Ok(qr),
                Ok(None) => {}
                Err(e) => return Err(e),
            }

            // Branch 1 — the control plane analyzer rejects the shape.
            if let Some(reason) = &analysis.unsupported {
                return Err(crate::query_engines::EngineError::capability_miss(
                    asap_types::StorageBackend::SketchStore.data_source_id(),
                    format!(
                        "SketchStore analyzer rejected `{query}`: {reason:?} — \
                         failing over to archive"
                    ),
                ));
            }
            if analysis.candidates.is_empty() {
                // Defensive — `is_asap_tier_answerable` would have
                // caught this; analyzer guarantees `unsupported.is_some()`
                // when `candidates.is_empty()` but we keep the
                // belt-and-braces miss-path for safety.
                return Err(crate::query_engines::EngineError::capability_miss(
                    asap_types::StorageBackend::SketchStore.data_source_id(),
                    format!(
                        "SketchStore analyzer produced no ASAP-tier candidates for \
                         `{query}` — failing over to archive"
                    ),
                ));
            }

            // Branch 2 + 3 — resolve each candidate's sids and
            // dispatch the reducer. Today this is single-candidate
            // for every supported PromQL shape; the loop is here
            // for the per-candidate hybrid-stitch follow-up.
            // `now_ms` is captured above (also used by the
            // topk-over-rate fallback).
            // Time bounds: the trait's `execute(&str)` adapter
            // doesn't carry an explicit range today (it's an
            // instant-query surface). For each candidate, prefer the
            // candidate's `range_seconds` (extracted from `[5m]` /
            // `[30s]` selectors); fall back to a 5-minute default
            // for instant-vector candidates (range_seconds == 0).
            const DEFAULT_LOOKBACK_MS: u64 = 5 * 60 * 1000;

            let reducer = crate::storage_engines::sketch_db::query::SketchReducer::new(idx);
            // Multi-candidate aggregation is deferred (single-result
            // shapes today). On the first reducer error we surface
            // CapabilityMiss; on Ok we keep the result for the
            // hybrid-stitch path below. (When more than one
            // candidate is supported, a follow-up will fold
            // per-candidate ASAPTierResults.)
            let mut combined_result: Option<crate::storage_engines::sketch_db::query::ASAPTierResult> =
                None;
            let mut combined_t0: u64 = u64::MAX;
            // Track whether ANY candidate is range-vector-shaped
            // (`range_seconds > 0`). Drives the Vector-vs-Matrix
            // result-shape choice in `asap_tier_result_to_query_result`
            // below — instant queries (`count(metric)`,
            // `quantile(...)` without `_over_time` etc.) need
            // `QueryResult::Vector` so the Prometheus adapter's
            // `format_success_response` wraps them as `resultType:
            // vector`. Returning `Matrix` for an instant query
            // produces a 500 (adapter rejects the shape mismatch).
            let mut any_range_candidate = false;

            // Snapshot the streaming config once for this query's
            // policy lookups. Hot-reload swaps the underlying Arc; the
            // snapshot pins one revision for the duration.
            let streaming_snap = self.streaming_config_snapshot();
            let policy_registry = streaming_snap.policy_registry();

            for candidate in &analysis.candidates {
                if candidate.range_seconds > 0 {
                    any_range_candidate = true;
                }
                // Schema-retirement #5: resolve candidate → {sids} by
                // unioning the policy-fp reverse index (fast path for
                // ExactAgg sids minted via `ingest_precompute_for_agg_config`
                // where `policy_fp` is set) with `instances_matching`
                // (catalog walk that subset-matches on
                // `group_by_keys`, covering raw sketches whose
                // `derive_sketch_policy_fp` returned `UNSET` because
                // the wire-attr set didn't match any streaming-config
                // policy). The earlier policy-fp-only path returned
                // empty for the MVP demo workload — see issue #271 /
                // tracking #272.
                let policy_fps = control_plane::asap_tier_analysis::find_matching_policies(
                    &policy_registry,
                    candidate,
                );
                let mut sids: std::collections::BTreeSet<u64> =
                    std::collections::BTreeSet::new();
                for fp in &policy_fps {
                    sids.extend(idx.sids_for_policy(*fp));
                }
                sids.extend(idx.instances_matching(
                    &candidate.metric_name,
                    &candidate.group_by_keys,
                ));
                if sids.is_empty() {
                    // Fire the capability-miss notify so the
                    // control-plane feedback loop closes — replaces
                    // the side-effect the now-deleted
                    // `find_compatible_aggregation_with_miss_notify`
                    // provided on the legacy path.
                    let req = Self::requirements_from_candidate(candidate);
                    crate::drivers::control_plane_client::spawn_capability_miss_notify(
                        &self.control_plane_client,
                        &req,
                    );
                    return Err(crate::query_engines::EngineError::capability_miss(
                        asap_types::StorageBackend::SketchStore.data_source_id(),
                        format!(
                            "SketchStore has no policy for metric `{}` \
                             with group_by_keys ⊇ {:?} satisfying capability \
                             {:?} — failing over to archive",
                            candidate.metric_name,
                            candidate.group_by_keys,
                            candidate.required_capability,
                        ),
                    ));
                }

                // Verify each sid carries the analyzer's required
                // capability. After Step 2a there's exactly one
                // `Capability` enum (defined in the control plane and
                // re-exported by `sketch_index`), so no `From`
                // conversion is needed — just clone.
                let required: crate::storage_engines::sketch_db::index::Capability =
                    candidate.required_capability.clone();
                let mut hit_sids: Vec<u64> = Vec::with_capacity(sids.len());
                for sid in &sids {
                    match idx.classify(*sid) {
                        crate::storage_engines::sketch_db::index::SidLookup::Hit => {}
                        crate::storage_engines::sketch_db::index::SidLookup::Ghost
                        | crate::storage_engines::sketch_db::index::SidLookup::Unknown => {
                            let req = Self::requirements_from_candidate(candidate);
                            crate::drivers::control_plane_client::spawn_capability_miss_notify(
                                &self.control_plane_client,
                                &req,
                            );
                            return Err(crate::query_engines::EngineError::capability_miss(
                                asap_types::StorageBackend::SketchStore.data_source_id(),
                                format!(
                                    "SketchStore ghost/unknown sid {sid} for metric \
                                     `{}` — failing over to archive",
                                    candidate.metric_name
                                ),
                            ));
                        }
                    }
                    let meta = match idx.instance(*sid) {
                        Some(m) => m,
                        None => continue};
                    // Precompute-backed sids (M2.3) have `capability: None`
                    // — the analyzer doesn't route them through this path,
                    // but skip defensively if one slips in.
                    if let Some(cap) = meta.capability.as_ref() {
                        if required.is_satisfied_by(cap) {
                            hit_sids.push(*sid);
                        }
                    }
                }
                if hit_sids.is_empty() {
                    let req = Self::requirements_from_candidate(candidate);
                    crate::drivers::control_plane_client::spawn_capability_miss_notify(
                        &self.control_plane_client,
                        &req,
                    );
                    return Err(crate::query_engines::EngineError::capability_miss(
                        asap_types::StorageBackend::SketchStore.data_source_id(),
                        format!(
                            "SketchStore has no sid satisfying capability \
                             {:?} for metric `{}` — failing over to archive",
                            candidate.required_capability, candidate.metric_name
                        ),
                    ));
                }

                let lookback_ms = if candidate.range_seconds > 0 {
                    candidate.range_seconds.saturating_mul(1000)
                } else {
                    DEFAULT_LOOKBACK_MS
                };
                let t0_ms = now_ms.saturating_sub(lookback_ms);
                if t0_ms < combined_t0 {
                    combined_t0 = t0_ms;
                }

                // ExactAgg capability → dispatch the per-(group_by_keys)
                // accumulator-merge path; sketch capabilities → the
                // sketch-decode path. ExactAgg sids carry
                // `Box<dyn AggregateCore>` payloads (per-window
                // `SumAccumulator` / `IncreaseAccumulator` /
                // `MinMaxAccumulator` etc.) rather than opaque sketch
                // bytes, so they need a different reducer entry point.
                //
                // ExactAgg(Sum-family) candidates whose raw query
                // contains `rate(...)` / `irate(...)` AND
                // `range_seconds > 0` dispatch to the rate variant
                // (`evaluate_exact_agg_rate`) — that path folds every
                // sub-window sum across the range and divides by the
                // range to produce events-per-second, matching PromQL
                // `rate` semantics. Composed shapes like
                // `sum by (zone) (rate(metric[5m]))` go through here
                // too (function="sum" but range_seconds=300 from the
                // inner rate's matrix selector, picked up by the
                // analyzer trace).
                let use_rate_path = matches!(
                    &candidate.required_capability,
                    crate::storage_engines::sketch_db::index::Capability::ExactAgg(
                        crate::storage_engines::sketch_db::data::AggregationType::Sum
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleSum
                            | crate::storage_engines::sketch_db::data::AggregationType::Increase
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleIncrease
                    )
                ) && candidate.range_seconds > 0
                    && Self::query_contains_rate_call(query);
                let reducer_result = match &candidate.required_capability {
                    crate::storage_engines::sketch_db::index::Capability::ExactAgg(
                        agg_type,
                    ) if use_rate_path => reducer.evaluate_exact_agg_rate(
                        &hit_sids,
                        *agg_type,
                        &candidate.group_by_keys,
                        candidate.range_seconds,
                        t0_ms,
                        now_ms,
                    ),
                    crate::storage_engines::sketch_db::index::Capability::ExactAgg(
                        agg_type,
                    ) => reducer.evaluate_exact_agg(
                        &hit_sids,
                        *agg_type,
                        &candidate.group_by_keys,
                        t0_ms,
                        now_ms,
                    ),
                    _ => reducer.evaluate(
                        &hit_sids,
                        &candidate.function,
                        &candidate.function_args,
                        t0_ms,
                        now_ms,
                    ),
                };

                let result = match reducer_result {
                    Ok(r) => r,
                    Err(
                        crate::storage_engines::sketch_db::query::ASAPTierError::UnsupportedFunction(
                            name,
                        ),
                    ) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer does not support function `{name}` \
                                 — failing over to archive"
                            ),
                        ));
                    }
                    Err(crate::storage_engines::sketch_db::query::ASAPTierError::UnsupportedCapability {
                        function,
                        capability}) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer cannot answer `{function}` against \
                                 capability {capability:?} — failing over to archive"
                            ),
                        ));
                    }
                    Err(crate::storage_engines::sketch_db::query::ASAPTierError::DeserializeFailure {
                        sid,
                        encoding,
                        reason}) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer failed to decode sketch for sid \
                                 {sid} (encoding={encoding:?}): {reason} — failing over \
                                 to archive"
                            ),
                        ));
                    }
                    Err(crate::storage_engines::sketch_db::query::ASAPTierError::NoData {
                        metric_name: m}) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer found no samples for metric \
                                 `{m}` in window — failing over to archive"
                            ),
                        ));
                    }
                    Err(crate::storage_engines::sketch_db::query::ASAPTierError::MissingHeap {
                        sid,
                        sketch_kind}) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            asap_types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer cannot enumerate top-k for sid \
                                 {sid} (sketch_kind={sketch_kind:?}, no heap) — \
                                 failing over to archive"
                            ),
                        ));
                    }
                };
                combined_result = Some(result);
            }

            // All candidates resolved successfully — adapt to
            // QueryResult and run the hybrid-stitch path if archive
            // is wired and warm coverage is narrower than request.
            if let Some(result) = combined_result {
                // `execute(&str)` is the instant-query trait surface —
                // it's only called from `/api/v1/query` (never from
                // `/api/v1/query_range`, which has its own
                // `handle_range_query_promql` path). For PromQL,
                // instant queries always return a vector: even when
                // the inner expression carries a range selector like
                // `count_over_time(metric[10s])`, the outer evaluation
                // at time `t` yields one value per series (computed
                // over `[t-range, t]`). So this site always wants
                // Vector — `any_range_candidate` was the wrong signal
                // (it captures the inner range, not the outer eval
                // shape) and produced Matrix for instant queries
                // with range-bound inners, which the Prometheus
                // adapter's `format_success_response` rejects with
                // a 500 ”shape mismatch” / empty-body response.
                let _ = any_range_candidate;
                let warm_qr = asap_tier_result_to_query_result(
                    result.clone(),
                    now_ms,
                    false,
                );
                if let (Some((cov_lo, cov_hi)), Some(archive)) =
                    (result.coverage, self.archive_engine.as_ref())
                {
                    let stitch_t0 = if combined_t0 == u64::MAX {
                        now_ms.saturating_sub(DEFAULT_LOOKBACK_MS)
                    } else {
                        combined_t0
                    };
                    if cov_lo > stitch_t0 || cov_hi < now_ms {
                        let archive_qr = archive.execute(query).await;
                        if let Ok(archive_qr) = archive_qr {
                            return Ok(stitch_warm_and_archive(
                                warm_qr, archive_qr, cov_lo, cov_hi,
                            ));
                        }
                        // On archive error, fall back to warm-only.
                    }
                }
                return Ok(warm_qr);
            }
        }

        // CRITICAL #4: no-sketch-index fallback. The legacy
        // `handle_query` path used to live here and provide the
        // capability-miss notify side-effect through
        // `find_compatible_aggregation_with_miss_notify`. With legacy
        // retired (B7.5), this branch fires the notify directly so the
        // control-plane feedback loop still closes — required by
        // `capability_miss_http_e2e_tests::http_capability_miss_feedback_loop_closes_over_http`,
        // which wires the engine WITHOUT `.with_sketch_index(...)` and
        // therefore lands here on every miss.
        if let Some(req) = Self::requirements_from_query_str(query) {
            crate::drivers::control_plane_client::spawn_capability_miss_notify(
                &self.control_plane_client,
                &req,
            );
        }
        Err(crate::query_engines::EngineError::capability_miss(
            asap_types::StorageBackend::SketchStore.data_source_id(),
            format!(
                "ASAPQueryEngine: no sketch index for `{query}` — failing over to archive"
            ),
        ))
    }

    fn capabilities(&self) -> crate::query_engines::routing::query_engine_routing::EngineCapabilities {
        crate::query_engines::routing::query_engine_routing::EngineCapabilities {
            data_source_id: asap_types::StorageBackend::SketchStore.data_source_id(),
            storage_backend: asap_types::StorageBackend::SketchStore,
            // Warm-tier sketches are O(sketch-size); call it 16 MiB ceiling
            // for buffered ops (KLL with k=200 is well below this).
            supports_streams_above_bytes: 16 * 1024 * 1024}
    }
}

#[cfg(test)]
mod sketch_query_tests {
    // use crate::storage_engines::types::{CleanupPolicy, StreamingConfig};
    // use crate::query_engines::asap_query_engine::engine::ASAPQueryEngine;
    // use crate::storage_engines::promsketch_store::PromSketchStore;
    // use crate::storage_engines::TimestampedBucketsMap;
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
    //         _: crate::storage_engines::types::PrecomputedOutput,
    //         _: Box<dyn crate::storage_engines::types::AggregateCore>,
    //     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    //         panic!("NoOpStore should not be called for sketch queries");
    //     }
    //     fn insert_precomputed_output_batch(
    //         &self,
    //         _: Vec<(
    //             crate::storage_engines::types::PrecomputedOutput,
    //             Box<dyn crate::storage_engines::types::AggregateCore>,
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
    // fn engine_with_sketch_data(series_key: &str) -> ASAPQueryEngine {
    //     let ps = Arc::new(PromSketchStore::with_default_config());
    //     ps.ensure_all_sketches(series_key).unwrap();
    //     for i in 1..=100u64 {
    //         ps.sketch_insert(series_key, i, i as f64).unwrap();
    //     }

    //     let inference_config =
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());

    //     ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         Some(ps),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
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
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
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
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
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
    //     if let crate::query_engines::query_result::QueryResult::Vector(iv) = qr {
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
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
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
    //     if let crate::query_engines::query_result::QueryResult::Matrix(rv) = qr {
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
    //         InferenceConfig::new(::promql, CleanupPolicy::NoCleanup);
    //     let streaming_config = Arc::new(StreamingConfig::default());
    //     let engine = ASAPQueryEngine::new(
    //         Arc::new(NoOpStore),
    //         inference_config,
    //         streaming_config,
    //         15,
    //         ::promql,
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
    use crate::storage_engines::types::{
        AggregationType, CleanupPolicy, HotReloadStreamingConfig, 
        StreamingConfig, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    fn dummy_agg(_id: u64, metric: &str) -> crate::storage_engines::types::AggregationConfig {
        // `_id` is unused after PR 5 — identity is content-addressed.
        crate::storage_engines::types::AggregationConfig::new(
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
        )
    }

    /// Returns the StreamingConfig and the policy-fingerprint u64 that
    /// became the map key for the single inserted agg.
    fn cfg_with_agg(id: u64, metric: &str) -> (StreamingConfig, u64) {
        let mut map = std::collections::HashMap::new();
        let cfg = dummy_agg(id, metric);
        let fp = cfg.policy_fp_u64();
        map.insert(fp, cfg);
        (StreamingConfig::new(map), fp)
    }

    fn build_engine(handle: HotReloadStreamingConfig) -> ASAPQueryEngine {
        let _ = Arc::new(StreamingConfig::default());
        ASAPQueryEngine::new_with_hot_reload(handle, 15000)
    }

    #[test]
    fn streaming_config_snapshot_starts_at_initial_config() {
        let (cfg, fp) = cfg_with_agg(101, "metric_a");
        let handle = HotReloadStreamingConfig::new(cfg);
        let engine = build_engine(handle);
        let snap = engine.streaming_config_snapshot();
        assert_eq!(snap.aggregation_configs.len(), 1);
        assert!(snap.aggregation_configs.contains_key(&fp));
    }

    #[test]
    fn streaming_config_snapshot_observes_post_construction_swap() {
        // Core of PR E phase 2: once the engine is built, swapping
        // the shared HotReloadStreamingConfig handle must take
        // effect on the next snapshot call — this is the guarantee
        // that makes POST /api/v1/streaming-config actually useful
        // for query-time behavior.
        let (cfg_a, fp_a) = cfg_with_agg(101, "metric_a");
        let (cfg_b, fp_b) = cfg_with_agg(202, "metric_b");
        let handle = HotReloadStreamingConfig::new(cfg_a);
        let engine = build_engine(handle.clone());

        // Initial snapshot: metric_a only.
        let snap_before = engine.streaming_config_snapshot();
        assert_eq!(snap_before.aggregation_configs.len(), 1);
        assert!(snap_before.aggregation_configs.contains_key(&fp_a));
        assert!(!snap_before.aggregation_configs.contains_key(&fp_b));

        // Simulate a control plane push via `HotReloadStreamingConfig::swap`.
        // Clones of the handle share the same underlying ArcSwap, so a
        // swap on `handle` is observable through the engine's stored
        // clone.
        handle.swap(cfg_b);

        // Next snapshot: metric_b, metric_a gone.
        let snap_after = engine.streaming_config_snapshot();
        assert_eq!(snap_after.aggregation_configs.len(), 1);
        assert!(snap_after.aggregation_configs.contains_key(&fp_b));
        assert!(!snap_after.aggregation_configs.contains_key(&fp_a));

        // The old snapshot is still internally consistent — it's a
        // separate Arc that was cheap-cloned before the swap and
        // continues to reflect the pre-swap state. This matches the
        // per-query-entry-snapshot contract: a query that started
        // before the swap sees old config for its entire execution.
        assert!(snap_before.aggregation_configs.contains_key(&fp_a));
    }

    #[test]
    fn legacy_new_constructor_is_independent_of_external_handle() {
        // The legacy `ASAPQueryEngine::new` path wraps the provided
        // Arc<StreamingConfig> in a FRESH HotReloadStreamingConfig
        // internally, so external swaps must NOT leak in. This is
        // the behavior tests and binaries that don't own a shared
        // handle depend on.
        let (cfg_a, fp_a) = cfg_with_agg(101, "metric_a");
        let external_handle = HotReloadStreamingConfig::new(cfg_a);
        let streaming_config = external_handle.snapshot();

        let engine = ASAPQueryEngine::new(streaming_config, 15000);

        // External swap should NOT be visible inside the engine — the
        // legacy constructor snapshotted the initial Arc into its own
        // fresh hot-reload wrapper.
        let (cfg_swapped, fp_swapped) = cfg_with_agg(999, "metric_swapped");
        external_handle.swap(cfg_swapped);

        let engine_snap = engine.streaming_config_snapshot();
        assert_eq!(engine_snap.aggregation_configs.len(), 1);
        assert!(
            engine_snap.aggregation_configs.contains_key(&fp_a),
            "legacy `new` constructor should pin the initial config, \
             external swaps to unrelated handles must not leak in"
        );
        assert!(!engine_snap.aggregation_configs.contains_key(&fp_swapped));
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
//
// B7.5 retirement note: the in-process `capability_miss_feedback_loop_closes`
// + `capability_miss_idempotent_on_repeat` tests that previously lived
// adjacent to this module were retired alongside their probe surface
// (`find_compatible_aggregation_with_miss_notify`). The
// capability-miss feedback loop is now exercised end-to-end via
// `crate::tests::capability_miss_http_e2e_tests::http_capability_miss_feedback_loop_closes_over_http`,
// which round-trips a real HTTP capability-miss through the modern
// `execute()` path's `spawn_capability_miss_notify` calls.
#[cfg(test)]
mod aux_pushdown_tests {
    use super::*;
    use crate::precompute_engine::operators::{
        min_max_accumulator::MinMaxAccumulator, sum_accumulator::SumAccumulator};
    use crate::storage_engines::types::AggregationType;
    use promql_utilities::query_logics::enums::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Accumulator that records how many times `query_statistic`
    /// was invoked. Used to verify the aux fast path skips it.
    struct SpyAccumulator {
        inner_sum: f64,
        query_calls: Arc<AtomicUsize>}

    impl crate::storage_engines::types::SerializableToSink for SpyAccumulator {
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
                query_calls: self.query_calls.clone()})
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
        fn aux_stats(&self) -> crate::storage_engines::types::AuxStats {
            crate::storage_engines::types::AuxStats {
                sum: Some(self.inner_sum),
                ..crate::storage_engines::types::AuxStats::empty()
            }
        }
    }

    fn make_engine() -> ASAPQueryEngine {
        use crate::storage_engines::types::{CleanupPolicy, HotReloadStreamingConfig, StreamingConfig};
    
        let sc = Arc::new(StreamingConfig::new(HashMap::new()));
        let hr = HotReloadStreamingConfig::from_arc(sc.clone());
        let _ = sc;
        ASAPQueryEngine::new_with_hot_reload(hr, 60)
    }

    #[test]
    fn aux_covered_stat_skips_query_statistic() {
        let engine = make_engine();
        let calls = Arc::new(AtomicUsize::new(0));
        let spy = SpyAccumulator {
            inner_sum: 42.0,
            query_calls: calls.clone()};
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
            query_calls: calls.clone()};
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
            query_calls: calls.clone()};
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


// ===========================================================================
// HLL count() — capability matching + accumulator query round-trip.
//
// Pins that the warm engine answers `count(metric)` from an HLL-backed
// aggregation: capability matching picks HLL (per
// `compatible_agg_types(Statistic::Count)`), and the HLL accumulator's
// `query_statistic` returns the cardinality estimate. This is the
// runtime contract the wire-side _hll alias resolver above relies on.
// ===========================================================================
// ===========================================================================
// KLL quantile — pin that DatasketchesKLL is in the Quantile capability
// list and the accumulator answers `Statistic::Quantile`. Mirrors the
// HLL-Count contract; closes the wire-side ingest gap diagnosis.
// ===========================================================================
// ===========================================================================
// Capability matching — Rate over CountMinSketch (PR #111 honest-gap
// closure). With the new `Statistic::Rate` arm in
// `compatible_agg_types`, `rate(<metric>[<range>])` against a CMS-only
// agg config now matches.
// ===========================================================================
/// Phase 5 — `QueryEngine::execute` ASAP-tier classification tests.
/// Pre-Phase-5 the trait adapter unconditionally delegated to
/// `handle_query`. After Phase 5 wire-in, when a `SketchStore` is
/// attached, the adapter classifies first and surfaces
/// `EngineError::CapabilityMiss(SketchStore, ...)` on Ghost / Unknown
/// / no-instance outcomes so the EngineRouter (Phase 6) can fall
/// through to the archive engine.
#[cfg(test)]
mod asap_tier_classify_tests {
    use super::*;
    use crate::storage_engines::types::{CleanupPolicy, HotReloadStreamingConfig};
    use crate::query_engines::EngineError;
    use crate::query_engines::routing::query_engine_routing::QueryEngine as _;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchConfig, SketchStore, SketchInstanceMetadata,
        SketchKindHandle, SketchSampleState};
    use std::collections::{BTreeMap, BTreeSet};

    fn build_engine_with_index(idx: Arc<SketchStore>) -> ASAPQueryEngine {
        let streaming_config = Arc::new(crate::storage_engines::types::StreamingConfig::default());
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
        ASAPQueryEngine::new_with_hot_reload(hot_reload, 15000).with_sketch_index(idx)
    }

    fn dd_meta(sid: u64, metric: &str, group_by: &[&str]) -> SketchInstanceMetadata {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01};
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::DDSketch,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        }
    }

    #[tokio::test]
    async fn execute_returns_capability_miss_when_no_instance_matches() {
        // No instance for `unknown_metric` is registered → adapter must
        // capability-miss rather than burn a `handle_query` round-trip.
        let idx = Arc::new(SketchStore::new());
        let engine = build_engine_with_index(idx);
        let err = engine
            .execute("unknown_metric{zone=\"z0\"}")
            .await
            .expect_err("ASAP-tier with no matching instance must yield CapabilityMiss");
        match err {
            EngineError::CapabilityMiss { engine_id, .. } => {
                assert_eq!(
                    engine_id,
                    asap_types::StorageBackend::SketchStore.data_source_id()
                );
            }
            other => panic!("expected CapabilityMiss, got {other:?}")}
    }

    #[tokio::test]
    async fn execute_returns_capability_miss_when_classify_is_ghost() {
        // Register instance metadata but never call append_sample. With
        // `dd_meta`'s `PolicyFingerprint::UNSET`, the engine's policy-fp
        // lookup misses entirely — there's no policy in the (empty)
        // registry to bind the sid to. Prior to the legacy-fallback
        // removal, the engine would walk `instances_matching` and find
        // the registered sid, classify it as Ghost (no sample state),
        // and produce a "ghost/unknown" detail. After removal, the
        // ghost lookup short-circuits at the policy-resolution step.
        // The CapabilityMiss outcome is preserved; we just don't
        // pin the detail string.
        let idx = Arc::new(SketchStore::new());
        idx.register(dd_meta(1, "http_latency_ms", &["zone"]));
        let engine = build_engine_with_index(idx);
        let err = engine
            .execute("quantile_over_time(0.99, http_latency_ms{zone=\"z0\"}[5m])")
            .await
            .expect_err("ghost sid registration must yield CapabilityMiss");
        match err {
            EngineError::CapabilityMiss { engine_id, .. } => {
                assert_eq!(
                    engine_id,
                    asap_types::StorageBackend::SketchStore.data_source_id()
                );
            }
            other => panic!("expected CapabilityMiss, got {other:?}")}
    }

    #[tokio::test]
    async fn execute_bare_selector_falls_over_to_archive() {
        // A bare vector selector lowers (via
        // `control_plane::asap_tier_analysis::analyze_promql_for_asap_tier`)
        // to an `ExactAgg(Sum)` candidate — the control plane no longer
        // rejects it outright with `NoCallNodeFound`. But the
        // `SketchStore` here holds only a DDSketch (quantile) policy, so
        // the candidate's `ExactAgg(Sum)` capability finds no matching
        // policy and the query still fails over to the archive engine
        // via `CapabilityMiss` — just with a capability-mismatch detail
        // rather than an analyzer-shape rejection. Either way the
        // routing outcome (→ archive) is unchanged.
        let idx = Arc::new(SketchStore::new());
        idx.register(dd_meta(2, "http_latency_ms", &["zone"]));
        idx.append_sample(
            2,
            BTreeMap::from([("zone".to_string(), "z0".to_string())]),
            (1_000, 1_010),
            SketchSampleState {
                bytes: vec![0],
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull},
        );

        let engine = build_engine_with_index(idx);
        let result = engine.execute("http_latency_ms{zone=\"z0\"}").await;
        match result {
            Err(EngineError::CapabilityMiss { detail, .. }) => {
                assert!(
                    detail.contains("ExactAgg(Sum)") || detail.contains("no policy"),
                    "expected a capability-miss fall-over to archive: {detail}"
                );
            }
            other => panic!("expected CapabilityMiss fall-over to archive, got {other:?}")}
    }

    /// Schema-retirement #5 regression: a sketch sid registered with
    /// a wider-than-requested `group_by_keys` and `policy_fp=UNSET`
    /// must still be findable by the query path. Mirrors the MVP
    /// smoke-test failure (issue #271 / tracking #272): the agent
    /// emits DDSketch DPs carrying every wire attribute, so the sid
    /// catalog ends up with `group_by_keys=[zone,rack,node,pod,...]`
    /// and `derive_sketch_policy_fp` returns `UNSET` because no
    /// streaming-config policy has that exact key set. The query
    /// asks for `grouping=[zone]` — a subset. With the policy-fp-only
    /// lookup the query returned `CapabilityMiss → archive`; with the
    /// `instances_matching` fallback restored it resolves to the sid
    /// (and bottoms out at the reducer's sample-state check rather
    /// than at sid resolution).
    #[tokio::test]
    async fn full_attr_sketch_sid_findable_via_subset_grouping() {
        let idx = Arc::new(SketchStore::new());
        // Register with the SUPERSET of attrs the agent would emit:
        // zone, rack, node, pod — none of which the streaming-config
        // would list directly in `grouping_labels=[zone]`.
        idx.register(dd_meta(42, "http_latency_ms", &["node", "pod", "rack", "zone"]));
        idx.append_sample(
            42,
            BTreeMap::from([
                ("zone".to_string(), "z0".to_string()),
                ("rack".to_string(), "r0".to_string()),
                ("node".to_string(), "n0".to_string()),
                ("pod".to_string(), "p0".to_string()),
            ]),
            (1_000, 1_010),
            SketchSampleState {
                bytes: vec![0],
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull},
        );

        let engine = build_engine_with_index(idx);
        // The query asks for grouping=[zone] (subset of registered
        // group_by_keys). Pre-fix this returned CapabilityMiss because
        // `sids_for_policy(UNSET)` is empty; post-fix the fallback
        // finds sid 42 via `instances_matching` and the request
        // proceeds to the reducer.
        let result = engine
            .execute("quantile_over_time(0.99, http_latency_ms{zone=\"z0\"}[5m])")
            .await;
        // The reducer can't produce a real quantile from the canned
        // payload (just `vec![0]`), but it MUST reach the reducer —
        // the sid-resolution-step CapabilityMiss with "no policy for
        // metric" detail is the regression we're guarding against.
        if let Err(EngineError::CapabilityMiss { detail, .. }) = &result {
            assert!(
                !detail.contains("has no policy for metric"),
                "regression: sid was lost at policy-resolution step \
                 instead of being found via instances_matching: {detail}"
            );
        }
    }

    /// `sum by (zone) (http_requests_total)` end-to-end via the
    /// `execute(&str)` adapter. Mirrors the MVP smoke test's Axis-C
    /// failure: the control plane's analyzer minted ExactAgg(Sum)
    /// sids for `http_requests_total` (one per zone), the engine
    /// resolved them via `instances_matching`, but the reducer
    /// returned `UnsupportedFunction("sum")` because
    /// `SketchReducer::evaluate` only knows sketch-backed query
    /// families. This test pins the ExactAgg dispatch branch added
    /// to `execute` so the new path emits per-zone instant-vector
    /// results instead of a CapabilityMiss.
    #[tokio::test]
    async fn execute_sum_by_zone_dispatches_to_exact_agg_reducer() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::query_engines::query_result::QueryResult;

        let idx = Arc::new(SketchStore::new());
        // Mirror the smoke-test setup: four ExactAgg(Sum) sids, one
        // per zone (z0..z3), registered with `group_by_keys=["zone"]`
        // and carrying a `SumAccumulator` per window.
        let zones = ["z0", "z1", "z2", "z3"];
        // Anchor windows so the engine's instant-query default
        // lookback (5 min) reaches them.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let window_start = now_ms.saturating_sub(60_000);
        let window_end = now_ms.saturating_sub(30_000);

        for (i, zone) in zones.iter().enumerate() {
            let sid = 9000 + i as u64;
            idx.register(SketchInstanceMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            let value = ((i + 1) * 100) as f64;
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            idx.append_precompute(
                sid,
                lm,
                (window_start, window_end),
                Box::new(SumAccumulator::with_sum(value)),
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("sum by (zone) (http_requests_total)")
            .await
            .expect("sum by (zone) must dispatch to ExactAgg reducer, not capability-miss");

        // Expect a Vector (instant) result with one entry per zone.
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 4, "one entry per zone");
        // Per-zone values match what each SumAccumulator carries.
        // KeyByLabelValues stores values only; the override carries
        // the corresponding keys.
        let mut by_zone: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for el in &vector.values {
            // The element's label keys override + label values together
            // identify the zone.
            let keys = el
                .label_keys_override
                .as_ref()
                .expect("override populated for ExactAgg path");
            let vals = &el.labels.labels;
            assert_eq!(keys.len(), vals.len());
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        assert_eq!(by_zone.get("z0").copied(), Some(100.0));
        assert_eq!(by_zone.get("z1").copied(), Some(200.0));
        assert_eq!(by_zone.get("z2").copied(), Some(300.0));
        assert_eq!(by_zone.get("z3").copied(), Some(400.0));
    }

    /// `rate(http_requests_total[5m])` end-to-end via `execute(&str)`.
    /// The analyzer hands the engine `Capability::ExactAgg(Sum)` with
    /// `function="rate"` and `range_seconds=300`; the engine must
    /// dispatch to `evaluate_exact_agg_rate` (not the per-window
    /// `evaluate_exact_agg`) so each output sample carries
    /// events-per-second, not the raw per-window sum.
    #[tokio::test]
    async fn execute_rate_dispatches_to_exact_agg_rate_reducer() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::query_engines::query_result::QueryResult;

        let idx = Arc::new(SketchStore::new());
        // Two zones, each its own sid, two windows each. Per-zone
        // per-window sums chosen so the rate over 300s is a clean
        // integer: zone z0 → 600+600 / 300 = 4.0; z1 → 900+900 / 300
        // = 6.0.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(1_000);

        for (i, (zone, per_window)) in [("z0", 600.0_f64), ("z1", 900.0)].iter().enumerate() {
            let sid = 11_000 + i as u64;
            idx.register(SketchInstanceMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            for (ws, we) in [(w1_start, w1_end), (w2_start, w2_end)] {
                let mut lm = BTreeMap::new();
                lm.insert("zone".to_string(), zone.to_string());
                idx.append_precompute(
                    sid,
                    lm,
                    (ws, we),
                    Box::new(SumAccumulator::with_sum(*per_window)),
                );
            }
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("rate(http_requests_total[5m])")
            .await
            .expect("rate must dispatch to ExactAgg rate reducer, not capability-miss");

        // Instant vector with two entries (one per natural series —
        // there's no outer aggregation collapsing zones).
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 2, "one entry per per-sid series");
        let mut by_zone: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el
                .label_keys_override
                .as_ref()
                .expect("label keys override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        // Values are per-second rates, not raw per-window sums.
        // (600 + 600) / 300 = 4.0; (900 + 900) / 300 = 6.0.
        let z0 = by_zone.get("z0").copied().expect("zone z0 present");
        let z1 = by_zone.get("z1").copied().expect("zone z1 present");
        assert!((z0 - 4.0).abs() < 1e-9, "z0 rate expected 4.0, got {z0}");
        assert!((z1 - 6.0).abs() < 1e-9, "z1 rate expected 6.0, got {z1}");
    }

    /// `sum by (zone) (rate(http_requests_total[5m]))` end-to-end.
    /// The analyzer gives `Capability::ExactAgg(Sum)` with
    /// `function="sum"` (outer) and `range_seconds=300` (lifted from
    /// the inner rate's matrix selector). The engine's
    /// `query_contains_rate_call` walker detects the inner rate and
    /// dispatches to `evaluate_exact_agg_rate`, which folds the per-
    /// zone per-window sums and divides by 300.
    #[tokio::test]
    async fn execute_sum_by_zone_rate_dispatches_to_exact_agg_rate_reducer() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::query_engines::query_result::QueryResult;

        let idx = Arc::new(SketchStore::new());
        // Four zones. Two windows each; per-zone sums chosen so the
        // per-zone rate over 300s is a clean integer.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(1_000);

        let zones = ["z0", "z1", "z2", "z3"];
        for (i, zone) in zones.iter().enumerate() {
            let sid = 12_000 + i as u64;
            idx.register(SketchInstanceMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            // per_window: 300, 600, 900, 1200 → per-zone rates over
            // 300s are 2, 4, 6, 8.
            let per_window = ((i + 1) * 300) as f64;
            for (ws, we) in [(w1_start, w1_end), (w2_start, w2_end)] {
                let mut lm = BTreeMap::new();
                lm.insert("zone".to_string(), zone.to_string());
                idx.append_precompute(
                    sid,
                    lm,
                    (ws, we),
                    Box::new(SumAccumulator::with_sum(per_window)),
                );
            }
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("sum by (zone) (rate(http_requests_total[5m]))")
            .await
            .expect("composed sum-rate must dispatch via rate reducer, not capability-miss");

        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 4, "one entry per zone");
        let mut by_zone: std::collections::HashMap<String, f64> =
            std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el
                .label_keys_override
                .as_ref()
                .expect("override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys.iter().position(|k| k == "zone").expect("zone key present");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        for (zone, expected) in [("z0", 2.0_f64), ("z1", 4.0), ("z2", 6.0), ("z3", 8.0)] {
            let got = by_zone.get(zone).copied().unwrap_or(f64::NAN);
            assert!((got - expected).abs() < 1e-9, "{zone} expected {expected}, got {got}");
        }
    }

    /// `topk(5, sum by (zone) (rate(http_requests_total[5m])))` —
    /// the multinode demo's flagship query. The control-plane analyzer
    /// rewrites the inner sum/rate into FrequencyTopk + FrequencyEstimate
    /// candidates (the optimizer's CMS-topk binder) which the
    /// ExactAgg(Sum) sids can't satisfy. The engine's
    /// `try_topk_over_rate_fallback` lifts the shape from the raw
    /// PromQL, finds the ExactAgg(Sum) sids via `instances_matching`,
    /// runs `evaluate_exact_agg_rate`, and slices the top-K by descending
    /// value. With K=5 and 4 zones the result is all 4 zones sorted
    /// descending.
    #[tokio::test]
    async fn execute_topk_over_sum_by_zone_rate_uses_fallback() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::query_engines::query_result::QueryResult;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(1_000);

        // Four zones with distinct per-window sums → distinct rates.
        let zones = ["z0", "z1", "z2", "z3"];
        for (i, zone) in zones.iter().enumerate() {
            let sid = 13_000 + i as u64;
            idx.register(SketchInstanceMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            let per_window = ((i + 1) * 300) as f64;
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            idx.append_precompute(
                sid,
                lm,
                (w_start, w_end),
                Box::new(SumAccumulator::with_sum(per_window)),
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("topk(5, sum by (zone) (rate(http_requests_total[5m])))")
            .await
            .expect(
                "topk over sum-rate must route through the ExactAgg(Sum) fallback, \
                 not capability-miss",
            );

        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        // K=5, only 4 zones — all 4 returned.
        assert_eq!(vector.values.len(), 4, "all 4 zones returned (k≥n)");
        // Slice ordering: descending by value. Read off values in
        // emit order and pair with their zone label.
        let mut ordered: Vec<(String, f64)> = Vec::new();
        for el in &vector.values {
            let keys = el
                .label_keys_override
                .as_ref()
                .expect("override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            ordered.push((vals[zone_idx].clone(), el.value));
        }
        // Per-window sums 300,600,900,1200 / 300s = 1, 2, 3, 4 → topk
        // descending = z3, z2, z1, z0.
        let labels_in_order: Vec<&str> =
            ordered.iter().map(|(z, _)| z.as_str()).collect();
        assert_eq!(
            labels_in_order,
            vec!["z3", "z2", "z1", "z0"],
            "topk emits zones in descending rate order: {ordered:?}"
        );
        assert!((ordered[0].1 - 4.0).abs() < 1e-9);
        assert!((ordered[3].1 - 1.0).abs() < 1e-9);
    }

    /// `topk(2, sum by (zone) (rate(...)))` — same shape but K < n,
    /// so the fallback must truncate to the top 2 by descending rate.
    #[tokio::test]
    async fn execute_topk_2_slices_to_top_2() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::query_engines::query_result::QueryResult;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(1_000);

        for (i, zone) in ["z0", "z1", "z2", "z3"].iter().enumerate() {
            let sid = 14_000 + i as u64;
            idx.register(SketchInstanceMetadata {
                sid,
                metric_name: "http_requests_total".to_string(),
                group_by_keys: ["zone".to_string()].into_iter().collect(),
                capability: Some(Capability::ExactAgg(AggregationType::Sum)),
                agg_kind: crate::storage_engines::sketch_db::index::AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    parameters_canonical: String::new(),
                    spatial_filter_canonical: String::new(),
                },
                accuracy: None,
                first_seen_unix_ms: 0,
                retired_at_ms: None,
                expires_at_ms: None,
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            });
            let mut lm = BTreeMap::new();
            lm.insert("zone".to_string(), zone.to_string());
            idx.append_precompute(
                sid,
                lm,
                (w_start, w_end),
                Box::new(SumAccumulator::with_sum(((i + 1) * 300) as f64)),
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("topk(2, sum by (zone) (rate(http_requests_total[5m])))")
            .await
            .expect("topk fallback ok");
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 2, "K=2 keeps only top 2 entries");
        // Top 2 in descending order = z3 (rate 4), z2 (rate 3).
        let zones: Vec<String> = vector
            .values
            .iter()
            .map(|el| {
                let keys = el.label_keys_override.as_ref().unwrap();
                let vals = &el.labels.labels;
                let z = keys.iter().position(|k| k == "zone").unwrap();
                vals[z].clone()
            })
            .collect();
        assert_eq!(zones, vec!["z3".to_string(), "z2".to_string()]);
    }
}

// ===========================================================================
// Hybrid warm + archive stitch tests (TODO 3 of the ASAP-tier follow-ups).
// Exercise `stitch_warm_and_archive` directly with synthetic
// `QueryResult::Matrix` payloads and assert the merged result honors
// the "warm wins on overlap; archive fills gaps" contract.
// ===========================================================================
#[cfg(test)]
mod hybrid_stitch_tests {
    use super::stitch_warm_and_archive;
    use crate::storage_engines::types::KeyByLabelValues;
    use crate::query_engines::query_result::{QueryResult, RangeVectorElement, Sample};

    fn matrix_with_samples(label: &str, samples: Vec<(u64, f64)>) -> QueryResult {
        let labels = KeyByLabelValues::new_with_labels(vec![label.to_string()]);
        let mut el = RangeVectorElement::new(labels);
        for (t, v) in samples {
            el.samples.push(Sample::new(t, v));
        }
        QueryResult::matrix(vec![el])
    }

    #[test]
    fn stitch_fills_archive_prefix_and_suffix() {
        // Warm covers [100, 200] with timestamps 100, 150, 200.
        let warm = matrix_with_samples("host=a", vec![(100, 10.0), (150, 11.0), (200, 12.0)]);
        // Archive covers [50, 250] with timestamps every 50ms.
        let archive = matrix_with_samples(
            "host=a",
            vec![
                (50, 1.0),
                (100, 99.0), // overlap: warm wins
                (150, 99.0), // overlap: warm wins
                (200, 99.0), // overlap: warm wins
                (250, 2.0),
            ],
        );
        let merged = stitch_warm_and_archive(warm, archive, 100, 200);
        let m = match merged {
            QueryResult::Matrix(m) => m,
            _ => panic!("expected matrix")};
        assert_eq!(m.values.len(), 1, "one series");
        let samples = &m.values[0].samples;
        // Five distinct timestamps in the merged answer.
        assert_eq!(samples.len(), 5);
        // Warm values preserved on overlap.
        let mut by_ts: std::collections::HashMap<u64, f64> =
            samples.iter().map(|s| (s.timestamp, s.value)).collect();
        assert_eq!(by_ts.remove(&100), Some(10.0));
        assert_eq!(by_ts.remove(&150), Some(11.0));
        assert_eq!(by_ts.remove(&200), Some(12.0));
        // Archive prefix / suffix preserved.
        assert_eq!(by_ts.remove(&50), Some(1.0));
        assert_eq!(by_ts.remove(&250), Some(2.0));
    }

    #[test]
    fn stitch_keeps_archive_only_series_in_full() {
        // Warm has series "a"; archive has series "a" + "b". Both
        // need to make it into the merged answer; "b" comes from
        // archive in full.
        let warm = matrix_with_samples("host=a", vec![(150, 5.0)]);
        let archive = {
            let a = {
                let labels = KeyByLabelValues::new_with_labels(vec!["host=a".to_string()]);
                let mut el = RangeVectorElement::new(labels);
                el.samples.push(Sample::new(100, 1.0));
                el.samples.push(Sample::new(150, 99.0)); // warm wins
                el.samples.push(Sample::new(200, 2.0));
                el
            };
            let b = {
                let labels = KeyByLabelValues::new_with_labels(vec!["host=b".to_string()]);
                let mut el = RangeVectorElement::new(labels);
                el.samples.push(Sample::new(100, 7.0));
                el.samples.push(Sample::new(200, 8.0));
                el
            };
            QueryResult::matrix(vec![a, b])
        };
        let merged = stitch_warm_and_archive(warm, archive, 150, 150);
        let m = match merged {
            QueryResult::Matrix(m) => m,
            _ => panic!("expected matrix")};
        assert_eq!(m.values.len(), 2, "two series after merge");
        let by_label: std::collections::HashMap<Vec<String>, &RangeVectorElement> = m
            .values
            .iter()
            .map(|e| (e.labels.labels.clone(), e))
            .collect();
        let a = by_label.get(&vec!["host=a".to_string()]).expect("series a");
        assert_eq!(a.samples.len(), 3);
        let a_at_150 = a
            .samples
            .iter()
            .find(|s| s.timestamp == 150)
            .expect("warm value at 150 preserved");
        assert_eq!(a_at_150.value, 5.0, "warm wins on overlap");
        let b = by_label.get(&vec!["host=b".to_string()]).expect("series b");
        assert_eq!(b.samples.len(), 2);
    }
}

