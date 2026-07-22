use crate::storage_engines::types::StreamingConfig;
use std::sync::Arc;

use asap_types::query_requirements::QueryRequirements;
use asap_types::KeyByLabelNames;

#[cfg(test)]
use crate::storage_engines::types::KeyByLabelValues;
#[cfg(test)]
use crate::AggregateCore;
#[cfg(test)]
use asap_types::Statistic;
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
    archive_engine:
        Option<Arc<dyn crate::query_engines::routing::query_engine_routing::QueryEngine>>,
}

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
    pub fn new(streaming_config: Arc<StreamingConfig>, prometheus_scrape_interval: u64) -> Self {
        let hot_reload =
            crate::storage_engines::types::HotReloadStreamingConfig::from_arc(streaming_config);
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
            archive_engine: None,
        }
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
    fn extract_topk_over_rate_shape(query: &str) -> Option<TopkOverRateShape> {
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
        // P2-2: borrow each candidate's metadata under the read lock to
        // test the ExactAgg(Sum-family) capability — no per-candidate
        // metadata clone. Capture the agg_type of the first matching sid
        // in the same pass so we don't re-look-up (and re-clone) it
        // afterward.
        let mut hit_sids: Vec<u64> = Vec::new();
        let mut first_agg_type: Option<crate::storage_engines::sketch_db::data::AggregationType> =
            None;
        for sid in &candidate_sids {
            use crate::storage_engines::sketch_db::data::AggregationType;
            use crate::storage_engines::sketch_db::index::Capability;
            let agg_type = idx.with_instance(*sid, |m| match m.capability.as_ref() {
                Some(Capability::ExactAgg(
                    t @ (AggregationType::Sum
                    | AggregationType::MultipleSum
                    | AggregationType::Increase
                    | AggregationType::MultipleIncrease),
                )) => Some(*t),
                _ => None,
            });
            if let Some(Some(t)) = agg_type {
                if first_agg_type.is_none() {
                    first_agg_type = Some(t);
                }
                hit_sids.push(*sid);
            }
        }
        if hit_sids.is_empty() {
            return Ok(None);
        }

        // All hits share the same family by construction (Sum / Increase
        // variants are accumulator-compatible via `merge_with` /
        // `Statistic::Sum`); use the first matching sid's agg_type.
        let agg_type =
            first_agg_type.unwrap_or(crate::storage_engines::sketch_db::data::AggregationType::Sum);

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
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
            series_with_value
                .sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        } else {
            series_with_value
                .sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
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

    /// P1-1 — `rate(cms_metric[r])` over a warm FrequencyEstimate sid.
    ///
    /// The analyzer lowers `rate(...)` over any metric to
    /// `Capability::ExactAgg(Sum) + OuterFn::Rate`. For a CMS / CountSketch
    /// metric the registered sid carries `Capability::FrequencyEstimate`,
    /// not `ExactAgg`, so the analyzer-driven capability match yields no
    /// hit sids and the engine would fail over to archive. This fallback
    /// (analogous to [`Self::try_topk_over_rate_fallback`]) resolves the
    /// metric's FrequencyEstimate (or heap-bearing FrequencyTopk) sids and
    /// runs [`SketchReducer::evaluate_frequency_rate`] — Σ per-window
    /// frequency totals ÷ coverage-clamped range — to produce a per-second
    /// rate.
    ///
    /// Returns `Some(result)` when at least one warm FrequencyEstimate sid
    /// answered; `None` when none exists (caller falls over to archive) or
    /// when the reducer produced no in-window data. Errors from the
    /// reducer also collapse to `None` (fail over) so this never
    /// destabilizes the working ExactAgg / count_over_time paths.
    fn try_rate_over_frequency_fallback(
        &self,
        candidate: &control_plane::asap_tier_analysis::ASAPTierCandidate,
        idx: &crate::storage_engines::sketch_db::index::SketchStore,
        reducer: &crate::storage_engines::sketch_db::query::SketchReducer<'_>,
        now_ms: u64,
    ) -> Option<crate::storage_engines::sketch_db::query::ASAPTierResult> {
        use crate::storage_engines::sketch_db::index::{Capability, SidLookup};

        // Resolve the metric's candidate sids and keep only warm
        // FrequencyEstimate-answerable Hits. A heap-bearing FrequencyTopk
        // sid also answers bare frequency (the heap is layered over the
        // matrix), so accept either.
        let candidate_sids =
            idx.instances_matching(&candidate.metric_name, &candidate.group_by_keys);
        let mut hit_sids: Vec<u64> = Vec::new();
        for sid in &candidate_sids {
            if idx.classify(*sid) != SidLookup::Hit {
                continue;
            }
            let is_freq = idx
                .with_instance(*sid, |m| {
                    matches!(
                        m.capability.as_ref(),
                        Some(Capability::FrequencyEstimate(_)) | Some(Capability::FrequencyTopk(_))
                    )
                })
                .unwrap_or(false);
            if is_freq {
                hit_sids.push(*sid);
            }
        }
        if hit_sids.is_empty() {
            return None;
        }

        let lookback_ms = candidate.range_seconds.saturating_mul(1000);
        let t0_ms = now_ms.saturating_sub(lookback_ms);
        match reducer.evaluate_frequency_rate(&hit_sids, candidate.range_seconds, t0_ms, now_ms) {
            Ok(res) if !res.series.is_empty() => Some(res),
            // NoData / empty / any reducer error → fail over to archive.
            _ => None,
        }
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
        step_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        let Some(idx) = self.sketch_index.as_ref() else {
            return Err(crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!("ASAPQueryEngine: no sketch index for `{query}` — failing over"),
            ));
        };

        let analysis = control_plane::asap_tier_analysis::analyze_promql_for_asap_tier(query);

        if let Some(reason) = &analysis.unsupported {
            return Err(crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!(
                    "SketchStore analyzer rejected `{query}` for range query: \
                     {reason:?} — failing over to archive"
                ),
            ));
        }
        if analysis.candidates.is_empty() {
            return Err(crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!(
                    "SketchStore analyzer produced no ASAP-tier candidates for \
                     `{query}` — failing over to archive"
                ),
            ));
        }

        let streaming_snap = self.streaming_config_snapshot();
        let policy_registry = streaming_snap.policy_registry();
        let reducer = crate::storage_engines::sketch_db::query::SketchReducer::new(idx);
        let mut combined_result: Option<crate::storage_engines::sketch_db::query::ASAPTierResult> =
            None;

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
            let mut sids: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            for fp in &policy_fps {
                sids.extend(idx.sids_for_policy(*fp));
            }
            sids.extend(idx.instances_matching(&candidate.metric_name, &candidate.group_by_keys));
            if sids.is_empty() {
                return Err(crate::query_engines::EngineError::capability_miss(
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                // P2-2: borrow the metadata under the read lock to test
                // capability satisfaction — no per-candidate deep clone
                // of `SketchInstanceMetadata` (String + BTreeSet<String>
                // + AggKind) just to inspect one field.
                let satisfied = idx
                    .with_instance(*sid, |m| {
                        m.capability
                            .as_ref()
                            .map(|cap| required.is_satisfied_by(cap))
                            .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if satisfied {
                    hit_sids.push(*sid);
                }
            }
            if hit_sids.is_empty() {
                return Err(crate::query_engines::EngineError::capability_miss(
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                    // Counter-function dispatch (issue #301) — mirror the
                    // instant `execute(&str)` path's branching off the
                    // typed `candidate.outer_fn`. On this explicit
                    // RANGE (matrix) surface the per-window timeseries is
                    // the correct shape for `sum`/`increase` (the wire
                    // format wants a point per window), so
                    // `accumulate_windows = false`. `rate` still folds +
                    // divides; `sum_over_time` over a counter is refused
                    // (decision (a)) so the query routes to archive.
                    use control_plane::asap_tier_analysis::OuterFn;
                    let is_exact_sum_family = matches!(
                        agg_type,
                        crate::storage_engines::sketch_db::data::AggregationType::Sum
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleSum
                            | crate::storage_engines::sketch_db::data::AggregationType::Increase
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleIncrease
                    );
                    if is_exact_sum_family && candidate.outer_fn == OuterFn::SumOverTime {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            crate::storage_engines::types::StorageBackend::SketchStore
                                .data_source_id(),
                            format!(
                                "SketchStore cannot answer `sum_over_time` over counter \
                                 deltas for `{query}` (issue #301) — failing over to archive"
                            ),
                        ));
                    }
                    let use_rate_path = candidate.range_seconds > 0
                        && candidate.outer_fn == OuterFn::Rate
                        && is_exact_sum_family;
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
                                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                                false,
                            )
                            .map_err(|e| {
                                crate::query_engines::EngineError::capability_miss(
                                    crate::storage_engines::types::StorageBackend::SketchStore
                                        .data_source_id(),
                                    format!(
                                        "SketchStore exact-agg reducer failed for `{query}` over \
                                         [{start_ms}, {end_ms}]: {e:?} — failing over to archive"
                                    ),
                                )
                            })?
                    }
                }
                // P2-4 (typed dispatch): route off the analyzer's typed
                // `required_capability` via `evaluate_for_capability`
                // instead of round-tripping it through a function-name
                // string the reducer re-parses.
                _ => reducer
                    .evaluate_for_capability(
                        &candidate.required_capability,
                        &hit_sids,
                        &candidate.function_args,
                        // Per-item CMS estimate(key) is wired through the reducer
                        // but only dispatched once the engine resolves the item
                        // value against an item_label-mode sid (Phase 2b). Until
                        // then keyed CMS frequency safe-misses (see below), so the
                        // bucket-total path is correct here.
                        None,
                        effective_is_cumulative(candidate),
                        start_ms,
                        end_ms,
                    )
                    .map_err(|e| {
                        crate::query_engines::EngineError::capability_miss(
                            crate::storage_engines::types::StorageBackend::SketchStore
                                .data_source_id(),
                            format!(
                                "SketchStore reducer failed for `{query}` over \
                                 [{start_ms}, {end_ms}]: {e:?} — failing over to archive"
                            ),
                        )
                    })?,
            };
            // Apply the analyzer's typed outer-aggregation operator on
            // the range-query path too (issue #296) — same identity
            // case + fold semantics as the instant-query trait
            // adapter above.
            let result = if candidate.outer_agg.is_some() && !outer_fold_already_consumed(candidate)
            {
                apply_outer_agg_fold(result, &candidate.outer_agg)
            } else {
                result
            };
            combined_result = Some(result);
        }

        let result = combined_result.ok_or_else(|| {
            crate::query_engines::EngineError::capability_miss(
                crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                format!("SketchStore reducer produced no result for `{query}`"),
            )
        })?;

        // Matrix shape — the range_query wire format requires it.
        let warm_qr = asap_tier_result_to_query_result(result.clone(), end_ms, true);

        // FIX 3 — coverage-aware warm+archive HYBRID STITCH for RANGE
        // queries. The instant path (`execute`) already stitches when warm
        // coverage is narrower than the request; the range path historically
        // returned warm-only, so a request `[start_ms, end_ms]` whose warm
        // sketches only cover a suffix `[cov_lo, cov_hi]` lost the
        // prefix `[start_ms, cov_lo)` (the live "No result" / incomplete
        // matrix symptom). When the reducer reports a coverage narrower than
        // the requested range AND an archive engine is wired, fetch the
        // archive's range answer over the SAME window and stitch them by
        // (label_values, timestamp) — warm wins on overlap, archive fills the
        // uncovered prefix/suffix. Mirrors the instant-path logic at the
        // `execute` trait surface.
        if let (Some((cov_lo, cov_hi)), Some(archive)) =
            (result.coverage, self.archive_engine.as_ref())
        {
            if cov_lo > start_ms || cov_hi < end_ms {
                if let Ok(archive_qr) = archive
                    .execute_range(query, start_ms, end_ms, step_ms)
                    .await
                {
                    return Ok(stitch_warm_and_archive(warm_qr, archive_qr, cov_lo, cov_hi));
                }
                // Archive error → fall back to warm-only (best effort).
            }
        }

        Ok(warm_qr)
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

/// Fold the inner reducer's per-row [`ASAPTierResult`] into one row per
/// `by`-group, using the analyzer-typed
/// [`control_plane::asap_tier_analysis::OuterAgg`] operator
/// (`max`/`min`/`avg`/`count`/`group`/`stddev`/`stdvar`). Closes
/// [#296](https://github.com/ProjectASAP/ASAPQuery-backend/issues/296)
/// — the asap engine now composes outer aggregation operators on top
/// of inner function results (sketch + accumulator alike).
///
/// Semantics — one pass over the inner result's rows:
/// 1. Project each row's label map onto the `OuterAgg.by_labels()` set.
///    Labels not in the by-set are dropped; missing keys are dropped
///    silently (the by-projection treats absent keys as empty strings
///    only when the by-set is empty, in which case all rows collapse
///    into one no-label group — matching PromQL `<op>()` without `by`).
/// 2. Group rows by their projected label map.
/// 3. For each group, walk the rows' (timestamp, value) samples,
///    bucketed by timestamp, and apply the operator's
///    [`OuterAgg::fold`] across the values present at that timestamp.
///    Timestamps unique to one row contribute that row's value alone
///    (identity case: fold([v]) == v for max/min/avg, fold([v]) == 1
///    for count/group).
///
/// Identity / no-op case for asap's per-zone sketches:
/// `max by (zone) (quantile_over_time(...))` — the inner reducer
/// emits one row per zone already; each `by`-group has exactly one
/// row, the fold returns that row's value unchanged, and the result
/// shape mirrors the inner-only query. The general fold handles this
/// without a special case.
///
/// Coverage is preserved from the inner result — the fold doesn't
/// change which time-range the underlying sids covered.
/// Effective sketch reducer function-name for a candidate.
///
/// `SketchReducer::evaluate` keys its query-family dispatch off a
/// function-NAME string. The analyzer's `trace.function` is usually that
/// name (`quantile_over_time`, `cardinality_estimate`, …), BUT for an
/// outer-aggregation idiom whose inner is a BARE selector — e.g.
/// `count(metric)` (the HLL distinct-count idiom) — `trace_from_promql`
/// unwraps the outer `count` into `outer_agg` and then walks the inner
/// bare selector, which carries no function name. The trace's `function`
/// is then EMPTY, and the reducer maps `""` → `UnsupportedFunction` →
/// the engine returns an empty/capability-miss result for a query the
/// warm tier can actually answer.
///
/// When `function` is empty we fall back to a canonical name derived
/// from the analyzer's typed `required_capability` (the load-bearing
/// signal), so `count(hll_metric)` dispatches to the Cardinality family
/// and `quantile(...)` to the Quantile family even when the AST walk
/// couldn't recover a string. Non-empty function names pass through
/// unchanged so existing aliases keep their exact semantics.
fn effective_sketch_function(
    candidate: &control_plane::asap_tier_analysis::ASAPTierCandidate,
) -> &str {
    if !candidate.function.is_empty() {
        return &candidate.function;
    }
    use crate::storage_engines::sketch_db::index::Capability;
    match &candidate.required_capability {
        Capability::QuantileApprox(_) => "quantile",
        Capability::CardinalityApprox => "cardinality_estimate",
        Capability::FrequencyTopk(_) => "topk",
        Capability::FrequencyEstimate(_) => "frequency",
        // ExactAgg never reaches the sketch `evaluate` path (handled by
        // the ExactAgg dispatch branch), but return a benign default so
        // a stray ExactAgg still surfaces as UnsupportedFunction rather
        // than silently mis-dispatching.
        Capability::ExactAgg(_) => "",
    }
}

/// Whether the reducer should evaluate the candidate in CUMULATIVE
/// (`*_over_time` rollup → one scalar over `[t0,t1]`) vs per-window mode.
/// This is the only genuinely function-name-derived signal the typed
/// [`SketchReducer::evaluate_for_capability`] dispatch needs (the family
/// itself comes from the typed `required_capability`), so the engine
/// computes it here from the candidate's original PromQL function name —
/// matching exactly what the legacy `evaluate(function_name)` string
/// entry derived from the same name.
fn effective_is_cumulative(
    candidate: &control_plane::asap_tier_analysis::ASAPTierCandidate,
) -> bool {
    matches!(
        effective_sketch_function(candidate),
        "quantile_over_time" | "count_distinct_over_time" | "topk_over_time"
    )
}

/// Extract the VALUE of `label` from a canonical spatial-filter string of
/// the form `{a="1",service="svc-3"}` (the shape produced by
/// `normalize_spatial_filter`). Used by the per-item CMS `estimate(key)`
/// gate to pull the item value a keyed selector targets. Returns `None`
/// when `label` is absent. Exact label match (not substring), so
/// `service` does not match `myservice`.
fn extract_filter_value(canonical: &str, label: &str) -> Option<String> {
    let inner = canonical
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');
    for part in inner.split(',') {
        if let Some((k, v)) = part.trim().split_once('=') {
            if k.trim() == label {
                return Some(v.trim().trim_matches('"').to_string());
            }
        }
    }
    None
}

/// Whether the analyzer's `outer_agg` should still be folded over the
/// reducer's result, or has already been CONSUMED by the
/// capability dispatch.
///
/// `count(hll_metric)` is the distinct-count idiom: the analyzer lifts
/// the outer `count` into both `outer_agg = Count` AND
/// `required_capability = CardinalityApprox`. The HLL reducer answers
/// the distinct count directly (one cardinality scalar per window), so
/// re-applying the `Count` fold would collapse that estimate to the
/// row-count (`values.len()` → 1) — the wrong answer. Suppress the fold
/// in that case; the cardinality estimate IS the count.
fn outer_fold_already_consumed(
    candidate: &control_plane::asap_tier_analysis::ASAPTierCandidate,
) -> bool {
    use crate::storage_engines::sketch_db::index::Capability;
    use control_plane::asap_tier_analysis::OuterAgg;
    matches!(
        (&candidate.required_capability, &candidate.outer_agg),
        (Capability::CardinalityApprox, OuterAgg::Count(_))
    )
}

fn apply_outer_agg_fold(
    inner: crate::storage_engines::sketch_db::query::ASAPTierResult,
    outer: &control_plane::asap_tier_analysis::OuterAgg,
) -> crate::storage_engines::sketch_db::query::ASAPTierResult {
    use std::collections::BTreeMap;
    if !outer.is_some() {
        return inner;
    }
    let by_labels: &[String] = outer.by_labels();

    // group key (projected label map) → per-timestamp value buckets.
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, Vec<f64>>> = BTreeMap::new();

    for (row_labels, samples) in inner.series {
        // Project the row's label map onto the by-set. When by_labels
        // is empty (`max()` without `by (...)`), every row collapses
        // into one no-label group — matching PromQL semantics.
        let mut projected: BTreeMap<String, String> = BTreeMap::new();
        for k in by_labels {
            if let Some(v) = row_labels.get(k) {
                projected.insert(k.clone(), v.clone());
            }
        }
        let bucket = groups.entry(projected).or_default();
        for (ts, val) in samples {
            bucket.entry(ts).or_default().push(val);
        }
    }

    // Fold each group's per-timestamp buckets.
    let mut out_series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)> =
        Vec::with_capacity(groups.len());
    for (group_labels, by_ts) in groups {
        let mut folded: Vec<(i64, f64)> = Vec::with_capacity(by_ts.len());
        for (ts, vals) in by_ts {
            if let Some(v) = outer.fold(&vals) {
                folded.push((ts, v));
            }
        }
        if !folded.is_empty() {
            out_series.push((group_labels, folded));
        }
    }

    crate::storage_engines::sketch_db::query::ASAPTierResult {
        series: out_series,
        coverage: inner.coverage,
    }
}

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
        _ => return archive,
    };
    let archive_matrix = match &archive {
        QueryResult::Matrix(m) => m.values.clone(),
        QueryResult::Vector(_) => return warm,
    };

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
    use crate::query_engines::query_result::{
        InstantVectorElement, QueryResult, RangeVectorElement,
    };
    use crate::storage_engines::types::KeyByLabelValues;

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
                elements
                    .push(InstantVectorElement::new(labels, value).with_label_keys_override(keys));
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
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
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
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
            let mut combined_result: Option<
                crate::storage_engines::sketch_db::query::ASAPTierResult,
            > = None;
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
                let mut sids: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
                for fp in &policy_fps {
                    sids.extend(idx.sids_for_policy(*fp));
                }
                sids.extend(
                    idx.instances_matching(&candidate.metric_name, &candidate.group_by_keys),
                );
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
                        crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                // Track whether the candidate's sid set contained ONLY
                // Ghost/Unknown sids with no usable Hit. A single
                // metric/group-by selector legitimately resolves to a MIX
                // of sids: freshly-minted Active sids carrying live sketch
                // state (`Hit`) alongside retired-then-evicted or
                // merged-away identities that no longer hold data
                // (`Ghost`), plus stale sender-cache sids (`Unknown`). The
                // earlier behavior aborted the whole query to CapabilityMiss
                // on the FIRST Ghost/Unknown encountered — which, with the
                // sid set iterated in ascending-u64 order, meant an older
                // dataless sid masked the newer Active sketch sids that
                // could answer. Skip non-Hit sids instead; only fail over
                // to the archive when no Hit sid satisfies the capability
                // (handled by the `hit_sids.is_empty()` check below, which
                // preserves the all-ghost → CapabilityMiss contract).
                for sid in &sids {
                    match idx.classify(*sid) {
                        crate::storage_engines::sketch_db::index::SidLookup::Hit => {}
                        crate::storage_engines::sketch_db::index::SidLookup::Ghost
                        | crate::storage_engines::sketch_db::index::SidLookup::Unknown => {
                            continue;
                        }
                    }
                    // P2-2: borrow under the read lock to test capability
                    // satisfaction instead of deep-cloning the metadata.
                    // Precompute-backed sids (M2.3) have `capability: None`
                    // — the analyzer doesn't route them through this path,
                    // but skip defensively if one slips in.
                    let satisfied = idx
                        .with_instance(*sid, |m| {
                            m.capability
                                .as_ref()
                                .map(|cap| required.is_satisfied_by(cap))
                                .unwrap_or(false)
                        })
                        .unwrap_or(false);
                    if satisfied {
                        hit_sids.push(*sid);
                    }
                }
                // P1-1: rate over a FrequencyEstimate sid. `rate(cms_metric[r])`
                // lowers to `ExactAgg(Sum)+Rate`, but CMS sids are registered as
                // `FrequencyEstimate`, so no ExactAgg sid matched above and
                // `hit_sids` is empty. Before failing over to archive, check for
                // a warm FrequencyEstimate sid on this (metric, group_by_keys)
                // and, if one exists, evaluate the rate via the frequency reducer
                // path (Σ per-window frequency totals ÷ coverage-clamped range);
                // the result joins the per-candidate accumulation below exactly
                // like an ExactAgg-rate result. Only fires for the Rate outer-fn
                // with a matrix range — the working ExactAgg / count_over_time
                // paths never reach here (they match an ExactAgg sid).
                let mut freq_rate_override: Option<
                    crate::storage_engines::sketch_db::query::ASAPTierResult,
                > = None;
                if hit_sids.is_empty() {
                    use control_plane::asap_tier_analysis::OuterFn;
                    let is_exact_sum_rate = matches!(
                        &candidate.required_capability,
                        crate::storage_engines::sketch_db::index::Capability::ExactAgg(
                            crate::storage_engines::sketch_db::data::AggregationType::Sum
                                | crate::storage_engines::sketch_db::data::AggregationType::MultipleSum
                                | crate::storage_engines::sketch_db::data::AggregationType::Increase
                                | crate::storage_engines::sketch_db::data::AggregationType::MultipleIncrease
                        )
                    ) && candidate.outer_fn == OuterFn::Rate
                        && candidate.range_seconds > 0;
                    if is_exact_sum_rate {
                        if let Some(res) =
                            self.try_rate_over_frequency_fallback(candidate, idx, &reducer, now_ms)
                        {
                            // Bring `combined_t0` down to this rate window so
                            // the outer time domain covers the fallback result.
                            let lookback = candidate.range_seconds.saturating_mul(1000);
                            let t0 = now_ms.saturating_sub(lookback);
                            if t0 < combined_t0 {
                                combined_t0 = t0;
                            }
                            freq_rate_override = Some(res);
                        }
                    }
                    if freq_rate_override.is_none() {
                        let req = Self::requirements_from_candidate(candidate);
                        crate::drivers::control_plane_client::spawn_capability_miss_notify(
                            &self.control_plane_client,
                            &req,
                        );
                        return Err(crate::query_engines::EngineError::capability_miss(
                            crate::storage_engines::types::StorageBackend::SketchStore
                                .data_source_id(),
                            format!(
                                "SketchStore has no sid satisfying capability \
                                 {:?} for metric `{}` — failing over to archive",
                                candidate.required_capability, candidate.metric_name
                            ),
                        ));
                    }
                }

                // P2-6 (safe-miss for keyed CMS frequency). A
                // `FrequencyEstimate` sid (CMS / CountSketch) answers only
                // the per-window TOTAL across all items — it has no
                // string-keyed point estimate yet. So a KEYED query like
                // `cms_metric{item="X"}` would silently get the bucket
                // TOTAL (every item's inserts), not item X's frequency —
                // a wrong answer that never fails over. Detect the case
                // and capability-miss to archive instead (which CAN answer
                // the per-item query). The trigger is a NON-EMPTY
                // candidate spatial filter that is NOT already baked into
                // any matched sid's registered filter: a filter-distinct
                // CMS policy registers its sid WITH that filter (the sketch
                // is pre-filtered, so the total IS correct for it) and is
                // left alone; the bare `count_over_time(cms[r])` demo has
                // an empty filter and is unaffected. Full string-keyed
                // estimate is a larger follow-up; the safe-miss is enough.
                // Phase 2b: resolve a per-item estimate key. If a hit sid is
                // registered in item_label mode (item_labels side-table) and
                // the candidate's spatial filter selects that exact label, the
                // per-item `estimate(key)` path CAN answer the keyed selector —
                // so we extract the value and DON'T safe-miss below.
                let mut cms_item_key: Option<String> = None;
                if matches!(
                    &candidate.required_capability,
                    crate::storage_engines::sketch_db::index::Capability::FrequencyEstimate(_)
                ) && !candidate.spatial_filter_canonical.is_empty()
                {
                    for sid in &hit_sids {
                        if let Some(label) = idx.item_label_for(*sid) {
                            if let Some(val) =
                                extract_filter_value(&candidate.spatial_filter_canonical, &label)
                            {
                                cms_item_key = Some(val);
                                break;
                            }
                        }
                    }
                }

                if freq_rate_override.is_none()
                    // A resolved per-item key means the keyed estimate path
                    // answers this selector — skip the safe-miss.
                    && cms_item_key.is_none()
                    && matches!(
                        &candidate.required_capability,
                        crate::storage_engines::sketch_db::index::Capability::FrequencyEstimate(_)
                    )
                    && !candidate.spatial_filter_canonical.is_empty()
                {
                    let filter_baked_into_a_hit = hit_sids.iter().any(|sid| {
                        idx.with_instance(*sid, |m| match &m.agg_kind {
                            crate::storage_engines::sketch_db::index::AggKind::Sketch {
                                spatial_filter_canonical,
                                ..
                            } => *spatial_filter_canonical == candidate.spatial_filter_canonical,
                            _ => false,
                        })
                        .unwrap_or(false)
                    });
                    if !filter_baked_into_a_hit {
                        let req = Self::requirements_from_candidate(candidate);
                        crate::drivers::control_plane_client::spawn_capability_miss_notify(
                            &self.control_plane_client,
                            &req,
                        );
                        return Err(crate::query_engines::EngineError::capability_miss(
                            crate::storage_engines::types::StorageBackend::SketchStore
                                .data_source_id(),
                            format!(
                                "SketchStore FrequencyEstimate sid for metric `{}` cannot \
                                 answer the per-item selector `{}` (CMS/CountSketch return \
                                 the per-window bucket TOTAL, not a string-keyed estimate) — \
                                 failing over to archive rather than returning a misleading \
                                 total",
                                candidate.metric_name, candidate.spatial_filter_canonical
                            ),
                        ));
                    }
                }

                // Counter-function dispatch (issue #301). The four
                // PromQL counter idioms all lower to
                // `Capability::ExactAgg(Sum)`; the analyzer's typed
                // `candidate.outer_fn` carries the function distinction
                // that the engine MUST honor (otherwise sum /
                // sum_over_time / increase / rate collapse to the same
                // wrong number — the bug this fix closes).
                //
                //   Rate        → evaluate_exact_agg_rate over `[t-r, t]`
                //                 (Σ deltas ÷ min(r, coverage)).
                //   Increase    → evaluate_exact_agg(accumulate) over
                //                 `[t-r, t]` → Σ deltas, one number.
                //   Plain (sum) → evaluate_exact_agg(accumulate) over the
                //                 FULL storage horizon (`t0 = 0`) →
                //                 cumulative-since-storage-start, the
                //                 PromQL semantic for an instant counter
                //                 sum. (Not the most-recent window's
                //                 delta — Layer 3 of #301.)
                //   SumOverTime → capability-miss → archive (asap stores
                //                 deltas and cannot reconstruct the
                //                 Σ-of-cumulative-samples that
                //                 sum_over_time wants — issue #301
                //                 decision (a)).
                //
                // `range_seconds` is lifted by the analyzer from the
                // matrix selector (`[5m]` → 300); 0 for instant shapes.
                use control_plane::asap_tier_analysis::OuterFn;
                let is_exact_sum_family = matches!(
                    &candidate.required_capability,
                    crate::storage_engines::sketch_db::index::Capability::ExactAgg(
                        crate::storage_engines::sketch_db::data::AggregationType::Sum
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleSum
                            | crate::storage_engines::sketch_db::data::AggregationType::Increase
                            | crate::storage_engines::sketch_db::data::AggregationType::MultipleIncrease
                    )
                );

                // sum_over_time over a counter sid → refuse (route to
                // archive) rather than fabricate a wrong delta-sum.
                if is_exact_sum_family && candidate.outer_fn == OuterFn::SumOverTime {
                    let req = Self::requirements_from_candidate(candidate);
                    crate::drivers::control_plane_client::spawn_capability_miss_notify(
                        &self.control_plane_client,
                        &req,
                    );
                    return Err(crate::query_engines::EngineError::capability_miss(
                        crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                        format!(
                            "SketchStore cannot answer `sum_over_time` over counter \
                             deltas for metric `{}` (issue #301: Σ-of-cumulative-samples \
                             not reconstructable from per-window deltas) — failing over \
                             to archive",
                            candidate.metric_name
                        ),
                    ));
                }

                let use_rate_path = is_exact_sum_family && candidate.outer_fn == OuterFn::Rate;
                // Increase + instant Plain sum both accumulate windows
                // into one cumulative number per group; they differ only
                // in the time scope (`[t-r,t]` clip vs full storage).
                let accumulate_windows = is_exact_sum_family
                    && matches!(candidate.outer_fn, OuterFn::Increase | OuterFn::Plain);
                // Instant `Plain` sum reads the FULL storage horizon so it
                // returns cumulative-since-start; `Increase`/`Rate` clip to
                // the requested `[t-r, t]` (lookback_ms below).
                let plain_instant_sum = is_exact_sum_family
                    && candidate.outer_fn == OuterFn::Plain
                    && candidate.range_seconds == 0;

                let lookback_ms = if candidate.range_seconds > 0 {
                    candidate.range_seconds.saturating_mul(1000)
                } else {
                    DEFAULT_LOOKBACK_MS
                };
                let t0_ms = if plain_instant_sum {
                    0
                } else {
                    now_ms.saturating_sub(lookback_ms)
                };
                if t0_ms < combined_t0 {
                    combined_t0 = t0_ms;
                }

                let reducer_result =
                    match &candidate.required_capability {
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
                            accumulate_windows,
                        ),
                        // FIX 2 — GLOBAL HLL distinct rollup. `count(hll_metric)`
                        // with NO `by (...)` (empty group_by_keys + outer Count)
                        // is the distinct-UNION-cardinality idiom: MERGE the
                        // per-series HLL registers (register-wise max) across all
                        // matched sids and estimate ONCE. The per-series
                        // `evaluate_for_capability` path would otherwise emit one
                        // estimate per series (double-counting overlaps / never
                        // producing the single global number). Only the GLOBAL
                        // (no-`by`) shape is rerouted; `count by (zone) (...)`
                        // keeps the per-group per-series path below.
                        crate::storage_engines::sketch_db::index::Capability::CardinalityApprox
                            if candidate.group_by_keys.is_empty()
                                && matches!(
                                    candidate.outer_agg,
                                    control_plane::asap_tier_analysis::OuterAgg::Count(_)
                                ) =>
                        {
                            reducer.evaluate_cardinality_global(&hit_sids, t0_ms, now_ms)
                        }
                        // P2-4 (typed dispatch): route off the typed
                        // `required_capability` rather than the
                        // function-name-string detour.
                        _ => reducer.evaluate_for_capability(
                            &candidate.required_capability,
                            &hit_sids,
                            &candidate.function_args,
                            cms_item_key.as_deref(),
                            effective_is_cumulative(candidate),
                            t0_ms,
                            now_ms,
                        ),
                    };

                // P1-1: if the frequency-rate fallback produced a result
                // (hit_sids was empty for an ExactAgg(Sum)+Rate candidate
                // but a warm FrequencyEstimate sid answered), use it
                // directly; the ExactAgg dispatch above ran against an
                // empty `hit_sids` and is moot.
                let result = if let Some(r) = freq_rate_override {
                    r
                } else {
                    match reducer_result {
                    Ok(r) => r,
                    Err(
                        crate::storage_engines::sketch_db::query::ASAPTierError::UnsupportedFunction(
                            name,
                        ),
                    ) => {
                        return Err(crate::query_engines::EngineError::capability_miss(
                            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
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
                            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
                            format!(
                                "SketchStore reducer cannot enumerate top-k for sid \
                                 {sid} (sketch_kind={sketch_kind:?}, no heap) — \
                                 failing over to archive"
                            ),
                        ));
                    }
                }
                };
                // Apply the analyzer's typed outer-aggregation operator
                // (issue #296). The inner reducer (sketch / accumulator)
                // emits one row per natural series; if the original
                // PromQL wrapped the inner in `max by (...)` /
                // `min by (...)` / `avg by (...)` / `count by (...)` /
                // etc., we group the rows by the projected by-labels
                // and fold each group's values into a single scalar.
                //
                // Identity / no-op case (e.g. asap's per-zone DDSketch
                // sketch with `max by (zone) (quantile_over_time(...))`
                // — each zone already has one row): the fold collapses
                // a single-value group, returning the same value
                // unchanged. No special case needed; the general fold
                // handles it.
                let result =
                    if candidate.outer_agg.is_some() && !outer_fold_already_consumed(candidate) {
                        apply_outer_agg_fold(result, &candidate.outer_agg)
                    } else {
                        result
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
                let warm_qr = asap_tier_result_to_query_result(result.clone(), now_ms, false);
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
            crate::storage_engines::types::StorageBackend::SketchStore.data_source_id(),
            format!("ASAPQueryEngine: no sketch index for `{query}` — failing over to archive"),
        ))
    }

    /// Range-query entry point for the [`EngineRouter`] failover loop.
    ///
    /// Delegates to the inherent `execute_range_promql_modern`, which
    /// runs the ASAP-tier reducer over `[start_ms, end_ms]` and returns
    /// a `matrix` result. Without this override the router would hit the
    /// trait default (`CapabilityMiss`) and never reach the ASAP-tier
    /// range path, so every range query would fall straight through to
    /// the archive even when the warm sketches can answer it.
    async fn execute_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, crate::query_engines::EngineError>
    {
        self.execute_range_promql_modern(query, start_ms, end_ms, step_ms)
            .await
    }

    fn capabilities(
        &self,
    ) -> crate::query_engines::routing::query_engine_routing::EngineCapabilities {
        crate::query_engines::routing::query_engine_routing::EngineCapabilities {
            data_source_id: crate::storage_engines::types::StorageBackend::SketchStore
                .data_source_id(),
            storage_backend: crate::storage_engines::types::StorageBackend::SketchStore,
            // Warm-tier sketches are O(sketch-size); call it 16 MiB ceiling
            // for buffered ops (KLL with k=200 is well below this).
            supports_streams_above_bytes: 16 * 1024 * 1024,
        }
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
        AggregationType, CleanupPolicy, HotReloadStreamingConfig, StreamingConfig, WindowKind,
    };
    use asap_types::KeyByLabelNames;

    #[test]
    fn extract_filter_value_pulls_item_value() {
        // exact-label match, single and multi-matcher canonical forms
        assert_eq!(
            super::extract_filter_value("{service=\"svc-000003\"}", "service"),
            Some("svc-000003".to_string())
        );
        assert_eq!(
            super::extract_filter_value("{zone=\"z1\",service=\"svc-000003\"}", "service"),
            Some("svc-000003".to_string())
        );
        // absent label -> None
        assert_eq!(
            super::extract_filter_value("{zone=\"z1\"}", "service"),
            None
        );
        // substring labels must NOT match (service != myservice)
        assert_eq!(
            super::extract_filter_value("{myservice=\"x\"}", "service"),
            None
        );
        // empty filter -> None
        assert_eq!(super::extract_filter_value("", "service"), None);
    }

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
            WindowKind::Tumbling,
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
        min_max_accumulator::MinMaxAccumulator, sum_accumulator::SumAccumulator,
    };
    use crate::storage_engines::types::AggregationType;
    use asap_types::Statistic;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Accumulator that records how many times `query_statistic`
    /// was invoked. Used to verify the aux fast path skips it.
    struct SpyAccumulator {
        inner_sum: f64,
        query_calls: Arc<AtomicUsize>,
    }

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
        fn aux_stats(&self) -> crate::storage_engines::types::AuxStats {
            crate::storage_engines::types::AuxStats {
                sum: Some(self.inner_sum),
                ..crate::storage_engines::types::AuxStats::empty()
            }
        }
    }

    fn make_engine() -> ASAPQueryEngine {
        use crate::storage_engines::types::{
            CleanupPolicy, HotReloadStreamingConfig, StreamingConfig,
        };

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
    use crate::query_engines::routing::query_engine_routing::QueryEngine as _;
    use crate::query_engines::EngineError;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchConfig, SketchInstanceMetadata, SketchKindHandle,
        SketchSampleState, SketchStore,
    };
    use crate::storage_engines::types::{CleanupPolicy, HotReloadStreamingConfig};
    use std::collections::{BTreeMap, BTreeSet};

    fn build_engine_with_index(idx: Arc<SketchStore>) -> ASAPQueryEngine {
        let streaming_config = Arc::new(crate::storage_engines::types::StreamingConfig::default());
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
        ASAPQueryEngine::new_with_hot_reload(hot_reload, 15000).with_sketch_index(idx)
    }

    fn dd_meta(sid: u64, metric: &str, group_by: &[&str]) -> SketchInstanceMetadata {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
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
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id()
                );
            }
            other => panic!("expected CapabilityMiss, got {other:?}"),
        }
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
                    crate::storage_engines::types::StorageBackend::SketchStore.data_source_id()
                );
            }
            other => panic!("expected CapabilityMiss, got {other:?}"),
        }
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
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
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
            other => panic!("expected CapabilityMiss fall-over to archive, got {other:?}"),
        }
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
        idx.register(dd_meta(
            42,
            "http_latency_ms",
            &["node", "pod", "rack", "zone"],
        ));
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
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
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
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

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
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
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

    // Build a now-anchored KLL `SketchInstanceMetadata` + sample so the
    // engine's instant/range default lookbacks reach it. Mirrors the
    // live MVP workload: the agent emits a bare-named KLL sketch
    // (`http_requests_total_latency_ms`) into the SketchStore.
    fn kll_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::Kll { k: 200 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::Kll)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::Kll,
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

    fn encode_kll_items_proto(k: u16, items: &[f64]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{sketch_envelope, KllState, SketchEnvelope};
        use prost::Message;
        let state = KllState {
            k: k as u32,
            items: items.to_vec(),
            levels: vec![],
            num_levels: 0,
            ..Default::default()
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
            ..Default::default()
        };
        env.encode_to_vec()
    }

    fn hll_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::Hll { precision: 10 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::CardinalityApprox),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::Hll,
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

    fn encode_hll_with_cardinality(precision: u32, distinct: usize) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use asap_sketchlib::{HllSketch, HllVariant};
        use prost::Message;
        let mut sk = HllSketch::new(HllVariant::Regular, precision);
        for i in 0..distinct {
            sk.update(format!("user-{i}").as_bytes());
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision: sk.precision,
            registers: sk.registers.clone(),
            hip_kxq0: sk.hip_kxq0,
            hip_kxq1: sk.hip_kxq1,
            hip_est: sk.hip_est,
            registers_sparse: None,
        };
        let env = SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        };
        env.encode_to_vec()
    }

    /// REGRESSION of the HLL `count(metric)` "No result" e2e failure
    /// (`controller_plan_to_query_full_roundtrip_hll`) isolated to the
    /// engine layer. `count(unique_users_per_min)` is the distinct-count
    /// idiom: the analyzer lifts the outer `count` into `outer_agg=Count`
    /// AND `required_capability=CardinalityApprox`, and the bare-selector
    /// inner leaves the trace `function` EMPTY. Before the fix the engine
    /// passed the empty function to the reducer (→ `UnsupportedFunction`)
    /// AND re-applied the `Count` fold (→ row-count 1.0). The fix derives
    /// the reducer family from the capability and suppresses the
    /// already-consumed `Count` fold, so the HLL distinct-count is
    /// returned directly. A single FULL HLL frame (~500 users) is used so
    /// the instant projection reads the real estimate.
    #[tokio::test]
    async fn execute_count_hll_returns_cardinality_not_rowcount() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7500u64;
        idx.register(hll_meta(sid, "unique_users_per_min"));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(3_000), now_ms.saturating_sub(2_000)),
            SketchSampleState {
                bytes: encode_hll_with_cardinality(10, 500),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        let engine = build_engine_with_index(idx);
        let result = engine.execute("count(unique_users_per_min)").await.expect(
            "count(hll_metric) must dispatch to the Cardinality family \
                 via the candidate capability (empty trace function) and \
                 return the HLL distinct-count, NOT capability-miss",
        );
        assert!(
            result_nonempty(&result),
            "count(unique_users_per_min) over an HLL sid must return a \
             non-empty cardinality estimate (regression: empty `asap_query` \
             No-result)"
        );
        // The value must be the HLL distinct-count estimate (~500), NOT
        // the outer-Count fold collapsing it to the row-count (1.0).
        let est = match &result {
            crate::query_engines::query_result::QueryResult::Vector(v) => v.values[0].value,
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values[0].samples.last().map(|s| s.value).unwrap_or(0.0)
            }
        };
        assert!(
            est > 100.0,
            "expected the HLL distinct-count estimate (~500), not the \
             row-count fold (1.0); got {est}"
        );
    }

    /// Encode an HLL FULL proto frame over an EXPLICIT set of string items,
    /// so a test can craft overlapping / disjoint distinct sets across
    /// series and compute the TRUE union cardinality.
    fn encode_hll_from_items(precision: u32, items: &[String]) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, HllVariant as ProtoVariant, HyperLogLogState, SketchEnvelope,
        };
        use asap_sketchlib::{HllSketch, HllVariant};
        use prost::Message;
        let mut sk = HllSketch::new(HllVariant::Regular, precision);
        for it in items {
            sk.update(it.as_bytes());
        }
        let state = HyperLogLogState {
            variant: ProtoVariant::Regular as i32,
            precision: sk.precision,
            registers: sk.registers.clone(),
            hip_kxq0: sk.hip_kxq0,
            hip_kxq1: sk.hip_kxq1,
            hip_est: sk.hip_est,
            registers_sparse: None,
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::Hll(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// FIX 2 — GLOBAL HLL distinct rollup. `count(hll_metric)` with no `by`
    /// must MERGE the per-series HLL registers (register-wise max) across ALL
    /// matched series and estimate ONCE — the distinct UNION cardinality. Two
    /// series share an overlapping prefix of items and each carry disjoint
    /// items, so summing per-series estimates would over-count the overlap.
    /// The merged global estimate must land within HLL error of the true
    /// union, and be strictly below the naive per-series sum.
    #[tokio::test]
    async fn execute_count_hll_global_merges_registers_across_series() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(3_000);
        let w_end = now_ms.saturating_sub(2_000);

        // Series A: items 0..600. Series B: items 400..1000.
        // Overlap = [400,600) = 200 items; true union = [0,1000) = 1000.
        let precision = 12u32; // ~1.6% standard error
        let a_items: Vec<String> = (0..600).map(|i| format!("u-{i}")).collect();
        let b_items: Vec<String> = (400..1000).map(|i| format!("u-{i}")).collect();
        let true_union = 1000.0_f64;

        for (sid, items) in [(8200u64, &a_items), (8201u64, &b_items)] {
            let mut meta = hll_meta(sid, "unique_users_global");
            meta.agg_kind = crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::Hll,
                config: SketchConfig::Hll { precision },
                spatial_filter_canonical: String::new(),
            };
            idx.register(meta);
            idx.append_sample(
                sid,
                BTreeMap::new(),
                (w_start, w_end),
                SketchSampleState {
                    bytes: encode_hll_from_items(precision, items),
                    encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
                },
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("count(unique_users_global)")
            .await
            .expect("global count(hll_metric) must answer, not capability-miss");

        // GLOBAL distinct is a single scalar — exactly one element.
        let est = match &result {
            crate::query_engines::query_result::QueryResult::Vector(v) => {
                assert_eq!(
                    v.values.len(),
                    1,
                    "global count() must collapse to ONE merged estimate, got {} \
                     (per-series leak): {v:?}",
                    v.values.len()
                );
                v.values[0].value
            }
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                assert_eq!(m.values.len(), 1, "one merged series");
                m.values[0].samples.last().map(|s| s.value).unwrap_or(0.0)
            }
        };

        // Within HLL error of the true union (p=12 → ~1.04/sqrt(2^12) ≈ 1.6%;
        // allow a generous 8% band for the estimator's finite-sample noise).
        let rel_err = (est - true_union).abs() / true_union;
        assert!(
            rel_err < 0.08,
            "global merged estimate {est} must be within HLL error of the \
             true union {true_union} (rel_err {rel_err:.4})"
        );

        // And strictly below the naive per-series sum (600 + 600 = 1200),
        // proving registers were MERGED (max), not the estimates SUMMED.
        assert!(
            est < 1150.0,
            "merged global estimate {est} must be well below the per-series \
             sum (~1200) — proves register-merge, not estimate-sum"
        );
    }

    /// REPRODUCTION (root-cause hunt): `quantile_over_time(0.99,
    /// http_requests_total_latency_ms[30s])` end-to-end via
    /// `execute(&str)` against a now-anchored KLL sid carrying real
    /// sketch state. The window is inside the engine's `[now-30s, now]`
    /// range. This pins the exact end-to-end behaviour the live deploy
    /// shows ("No result" tagged `asap_query`) so we can see whether the
    /// engine produces `Ok(populated)`, `Ok(empty)`, or `CapabilityMiss`.
    #[tokio::test]
    async fn execute_quantile_over_time_kll_now_anchored() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7100u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // A 10s window ending 5s ago — comfortably inside the 30s range.
        let window_start = now_ms.saturating_sub(15_000);
        let window_end = now_ms.saturating_sub(5_000);

        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        let bytes = encode_kll_items_proto(200, &items);
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (window_start, window_end),
            SketchSampleState {
                bytes,
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );

        // The sid must classify as Hit (in-memory unsealed state counts).
        assert_eq!(
            idx.classify(sid),
            crate::storage_engines::sketch_db::index::SidLookup::Hit,
            "KLL sid with appended in-memory state must classify Hit"
        );

        let result = engine_quantile_result(idx, now_ms).await;
        let nonempty = match result {
            crate::query_engines::query_result::QueryResult::Vector(v) => !v.values.is_empty(),
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values.iter().any(|s| !s.samples.is_empty())
            }
        };
        assert!(
            nonempty,
            "quantile_over_time over a now-anchored KLL sid must return a \
             non-empty result (got empty → reproduces the live `asap_query` \
             + No-result bug)"
        );
    }

    async fn engine_quantile_result(
        idx: Arc<SketchStore>,
        _now_ms: u64,
    ) -> crate::query_engines::query_result::QueryResult {
        let engine = build_engine_with_index(idx);
        engine
            .execute("quantile_over_time(0.99, http_requests_total_latency_ms[30s])")
            .await
            .expect(
                "quantile_over_time over a Hit KLL sid must NOT capability-miss \
                 (if it does, the bug is upstream of the reducer)",
            )
    }

    fn result_nonempty(r: &crate::query_engines::query_result::QueryResult) -> bool {
        match r {
            crate::query_engines::query_result::QueryResult::Vector(v) => !v.values.is_empty(),
            crate::query_engines::query_result::QueryResult::Matrix(m) => {
                m.values.iter().any(|s| !s.samples.is_empty())
            }
        }
    }

    /// REGRESSION (delta-stitching carry-in): the live agent emits a
    /// periodic Full snapshot followed by many cheap Delta frames to
    /// save bandwidth, so a short query window (`[30s]`) routinely
    /// contains ONLY deltas — the Full landed earlier, outside the
    /// window. Before the fix, `SketchStore::query_range`'s strict
    /// containment filter (`w.0 >= start`) dropped the out-of-window
    /// Full, the delta-apply reducer couldn't establish a rolling base,
    /// and the engine returned `Ok(empty)` (NOT a capability-miss) — so
    /// the router never failed over and the client saw "No result"
    /// tagged `asap_query`. The fix splices in the most-recent Full
    /// ending before `start` as a carry-in base. This test pins that:
    /// a Full at now-60s + a Delta at now-10s with a `[30s]` window must
    /// produce a NON-EMPTY answer.
    #[tokio::test]
    async fn quantile_over_time_kll_full_before_window_carries_in_base() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7400u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        // Full at now-60s..now-55s — OUTSIDE the 30s window.
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(60_000), now_ms.saturating_sub(55_000)),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
        // Delta at now-15s..now-5s — INSIDE the window.
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(15_000), now_ms.saturating_sub(5_000)),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoDelta,
            },
        );

        let result = engine_quantile_result(idx, now_ms).await;
        assert!(
            result_nonempty(&result),
            "quantile_over_time with a Full BEFORE the window + a Delta \
             inside it must carry in the Full as a base and return a \
             non-empty result (regression: returned empty `asap_query` \
             No-result)"
        );
    }

    /// A delta-ONLY window with NO Full anywhere is the COMMON case under
    /// the edge's per-window-reset (PWR) delta model: the edge resets its
    /// snapshot base at each window boundary, so a window's first (here:
    /// only) frame is a delta-from-empty that, by construction, encodes
    /// that window's full state. The delta-apply walk bootstraps an empty
    /// rolling state of the sketch kind and applies the delta onto it, so
    /// the window IS queryable (delta-from-empty ⊕ empty = window state).
    ///
    /// Previously this returned empty (the walk skipped any delta with no
    /// carry-in Full), which is the very bug that broke end-to-end
    /// value-validation of delta-transmitted sketches.
    #[tokio::test]
    async fn quantile_over_time_kll_delta_only_no_base_reconstructs_from_empty() {
        let idx = Arc::new(SketchStore::new());
        let sid = 7300u64;
        idx.register(kll_meta(sid, "http_requests_total_latency_ms"));

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let items: Vec<f64> = (1..=50).map(|i| i as f64).collect();
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (now_ms.saturating_sub(15_000), now_ms.saturating_sub(5_000)),
            SketchSampleState {
                bytes: encode_kll_items_proto(200, &items),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoDelta,
            },
        );

        let result = engine_quantile_result(idx, now_ms).await;
        assert!(
            result_nonempty(&result),
            "delta-from-empty (PWR) window with no carry-in Full must \
             reconstruct that window's state and return a non-empty result"
        );
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
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        // Two zones, each its own sid, two windows each. The windows
        // span `[now-150s, now-30s]` = 120s of ACTUAL coverage inside
        // the requested 300s `[5m]` lookback. Post-#301 the rate divisor
        // is the actual coverage span (`min(300, 120) = 120`), NOT the
        // nominal 300 — so z0 = (600+600)/120 = 10.0; z1 =
        // (900+900)/120 = 15.0. (Both windows stay strictly inside
        // `[engine_now-300_000, engine_now]` so the window-contained
        // range query captures them regardless of the small skew between
        // the test's captured `now_ms` and the engine's query-time now.)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(150_000);
        let w1_end = now_ms.saturating_sub(90_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(30_000);

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
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
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
        // Values are per-second rates over the ACTUAL 120s coverage, not
        // raw per-window sums and not divided by the nominal 300s.
        // (600 + 600) / 120 = 10.0; (900 + 900) / 120 = 15.0.
        let z0 = by_zone.get("z0").copied().expect("zone z0 present");
        let z1 = by_zone.get("z1").copied().expect("zone z1 present");
        assert!((z0 - 10.0).abs() < 1e-9, "z0 rate expected 10.0, got {z0}");
        assert!((z1 - 15.0).abs() < 1e-9, "z1 rate expected 15.0, got {z1}");
    }

    /// Regression (issue #301, decision (a)): `sum_over_time(counter[r])`
    /// shares `Capability::ExactAgg(Sum)` with `rate`/`increase`/`sum`,
    /// but its PromQL semantic (Σ of CUMULATIVE sample values in `[r]`,
    /// a quadratic) CANNOT be reconstructed from the per-window deltas
    /// asap stores. Rather than fabricate a wrong number, the engine
    /// reads the analyzer's typed `OuterFn::SumOverTime` and returns a
    /// capability-miss so the query routes to the archive tier. Before
    /// #301 this returned the delta-sum (1500 here) — a wrong answer the
    /// caller couldn't distinguish from a correct one.
    #[tokio::test]
    async fn execute_sum_over_time_over_counter_capability_misses_to_archive() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(1_000);

        for (i, (zone, per_window)) in [("z0", 600.0_f64), ("z1", 900.0)].iter().enumerate() {
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
        let err = engine
            .execute("sum_over_time(http_requests_total[5m])")
            .await
            .expect_err(
                "sum_over_time over a counter sid MUST capability-miss → archive \
                 (issue #301 decision (a)); it must NOT fabricate a delta-sum",
            );
        assert!(
            matches!(
                err,
                crate::query_engines::EngineError::CapabilityMiss { .. }
            ),
            "expected CapabilityMiss for sum_over_time over counter, got {err:?}"
        );
    }

    /// Issue #301 Layer 3: instant `sum(counter)` must return the
    /// cumulative-since-storage value (Σ of ALL windows' deltas), NOT
    /// the most-recent window's delta. Two windows of 600/900 per zone
    /// → per-zone cumulative = 1200/1800; `sum by (zone)` keeps them
    /// separate; bare `sum` collapses to 3000. This test pins the
    /// `accumulate_windows=true` reducer path the engine selects for
    /// `OuterFn::Plain` instant sums.
    #[tokio::test]
    async fn execute_instant_sum_accumulates_all_windows_not_last() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(1_000);

        for (i, (zone, per_window)) in [("z0", 600.0_f64), ("z1", 900.0)].iter().enumerate() {
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
            .execute("sum by (zone) (http_requests_total)")
            .await
            .expect("instant sum by zone must succeed");
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(vector.values.len(), 2, "one entry per zone");
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el.label_keys_override.as_ref().expect("keys present");
            let vals = &el.labels.labels;
            let zi = keys.iter().position(|k| k == "zone").expect("zone key");
            by_zone.insert(vals[zi].clone(), el.value);
        }
        // Cumulative = Σ of ALL windows, NOT the last window's delta
        // (which would be 600 / 900).
        let z0 = by_zone.get("z0").copied().expect("z0");
        let z1 = by_zone.get("z1").copied().expect("z1");
        assert!(
            (z0 - 1200.0).abs() < 1e-9,
            "z0 cumulative expected 1200 (600+600), got {z0} — if 600 the \
             engine took only the LAST window (Layer-3 bug)"
        );
        assert!(
            (z1 - 1800.0).abs() < 1e-9,
            "z1 cumulative expected 1800 (900+900), got {z1}"
        );
    }

    /// Issue #301: `increase(counter[r])` must return Σ of deltas in
    /// `[t-r, t]` as ONE cumulative number per series (no rate divisor).
    /// Two windows of 600/900 → 1200/1800; with no `by` grouping the
    /// reducer collapses to one series = 3000.
    #[tokio::test]
    async fn execute_increase_accumulates_windows_without_divisor() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(120_000);
        let w1_end = now_ms.saturating_sub(60_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(1_000);

        for (i, (zone, per_window)) in [("z0", 600.0_f64), ("z1", 900.0)].iter().enumerate() {
            let sid = 15_000 + i as u64;
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
            .execute("increase(http_requests_total[5m])")
            .await
            .expect("increase must succeed via accumulate path");
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        // No `by` grouping → empty group_by → collapse to one series.
        // Σ of deltas in window = (600+600) + (900+900) = 3000. NOT
        // divided by range (that would be the rate path → 10.0).
        assert_eq!(
            vector.values.len(),
            1,
            "no group_by collapses to one series"
        );
        let value = vector.values[0].value;
        assert!(
            (value - 3000.0).abs() < 1e-9,
            "increase expected 3000 (Σ deltas, no divisor), got {value} — \
             if ~10 the engine took the rate path; if 1500 it took only \
             the last window per zone"
        );
    }

    /// Regression: the engine's rate-vs-plain dispatch decision MUST
    /// be made off the analyzer's typed `ASAPTierCandidate.outer_fn`
    /// field, NOT by re-parsing the raw PromQL query string. This test
    /// fabricates an analyzer-shaped candidate by name (no raw PromQL
    /// in scope) and asserts the `OuterFn` enum values the engine
    /// reads off it. If the engine ever re-introduces a
    /// `query_contains_rate_call`-style raw-string re-parse this test
    /// continues to pass — but the deletion of the string helper +
    /// this typed contract is what guards against the regression in
    /// the first place.
    #[test]
    fn analyzer_candidate_outer_fn_distinguishes_rate_from_sum_over_time() {
        use crate::storage_engines::sketch_db::data::AggregationType;
        use crate::storage_engines::sketch_db::index::Capability;
        use control_plane::asap_tier_analysis::{analyze_promql_for_asap_tier, OuterFn};
        let rate = analyze_promql_for_asap_tier("rate(http_requests_total[5m])");
        let sot = analyze_promql_for_asap_tier("sum_over_time(http_requests_total[5m])");
        let sum_by_rate =
            analyze_promql_for_asap_tier("sum by (zone) (rate(http_requests_total[5m]))");
        let bare = analyze_promql_for_asap_tier("http_requests_total");

        assert!(rate.unsupported.is_none() && !rate.candidates.is_empty());
        assert!(sot.unsupported.is_none() && !sot.candidates.is_empty());
        assert!(sum_by_rate.unsupported.is_none() && !sum_by_rate.candidates.is_empty());
        assert!(bare.unsupported.is_none() && !bare.candidates.is_empty());

        // Whichever query has a `rate(...)` call ANYWHERE in its tree
        // (bare `rate(...)` or composed `sum by (...) (rate(...))`) binds
        // to `AggIntent::Rate` and now maps to `ExactAgg(Increase)` --
        // matching `asap_plan::boundary::implementation_for`'s
        // `SummaryKind::Rate` (ASAPController models Rate as its own
        // summary family; see `capability_for`'s module doc and
        // `Capability::is_satisfied_by`'s `sum_satisfies_increase` for
        // why a Sum-registered sid still answers it). This is a real,
        // intentional behavior change from the Phase 2 semantic retarget
        // (Rate/Increase used to collapse onto AggIntent::Sum) -- not a
        // stale assertion left over from before it. `sot`/`bare` have no
        // `rate(...)` anywhere and stay `ExactAgg(Sum)`.
        assert_eq!(
            rate.candidates[0].required_capability,
            Capability::ExactAgg(AggregationType::Increase),
        );
        assert_eq!(
            rate.candidates[0].required_capability, sum_by_rate.candidates[0].required_capability,
            "composed `sum by (...) (rate(...))` binds the same Rate \
             AggIntent as bare `rate(...)`",
        );
        assert_eq!(
            sot.candidates[0].required_capability,
            bare.candidates[0].required_capability,
        );

        // `outer_fn` carries the counter-function distinction (#301).
        assert_eq!(rate.candidates[0].outer_fn, OuterFn::Rate);
        assert_eq!(sot.candidates[0].outer_fn, OuterFn::SumOverTime);
        assert_eq!(
            sum_by_rate.candidates[0].outer_fn,
            OuterFn::Rate,
            "composed `sum by (...) (rate(...))` MUST flag OuterFn::Rate \
             even though the outer function name is `sum`"
        );
        assert_eq!(bare.candidates[0].outer_fn, OuterFn::Plain);
    }

    /// `sum by (zone) (rate(http_requests_total[5m]))` end-to-end.
    /// The analyzer gives `Capability::ExactAgg(Increase)` (the inner
    /// `rate(...)` binds the `AggIntent::Rate` `capability_for` reads;
    /// see `analyzer_candidate_outer_fn_distinguishes_rate_from_sum_over_time`)
    /// with `function="sum"` (outer), `range_seconds=300` (lifted from
    /// the inner rate's matrix selector), AND `outer_fn=OuterFn::Rate`
    /// (the analyzer's PromQL trace walker flags the inner rate call).
    /// The registered sid here is `ExactAgg(Sum)` -- satisfied via
    /// `Capability::is_satisfied_by`'s `sum_satisfies_increase`. The
    /// engine dispatches to `evaluate_exact_agg_rate` off the
    /// typed `outer_fn` field, which folds the per-zone per-window
    /// sums and divides by 300.
    #[tokio::test]
    async fn execute_sum_by_zone_rate_dispatches_to_exact_agg_rate_reducer() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        // Four zones. Two windows each spanning `[now-150s, now-30s]` =
        // 120s of actual coverage inside the 300s `[5m]` lookback.
        // Post-#301 the rate divisor is the actual coverage span
        // (`min(300, 120) = 120`), not the nominal 300.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w1_start = now_ms.saturating_sub(150_000);
        let w1_end = now_ms.saturating_sub(90_000);
        let w2_start = w1_end;
        let w2_end = now_ms.saturating_sub(30_000);

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
            // per_window: 300, 600, 900, 1200 → per-zone totals 600,
            // 1200, 1800, 2400 → rates over the 120s coverage are
            // 5, 10, 15, 20.
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
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el.label_keys_override.as_ref().expect("override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        for (zone, expected) in [("z0", 5.0_f64), ("z1", 10.0), ("z2", 15.0), ("z3", 20.0)] {
            let got = by_zone.get(zone).copied().unwrap_or(f64::NAN);
            assert!(
                (got - expected).abs() < 1e-9,
                "{zone} expected {expected}, got {got}"
            );
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
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // One window per zone spanning `[now-150s, now-30s]` = 120s of
        // actual coverage inside the 300s `[5m]` lookback → coverage-aware
        // rate divisor is `min(300, 120) = 120` (#301).
        let w_start = now_ms.saturating_sub(150_000);
        let w_end = now_ms.saturating_sub(30_000);

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
            let keys = el.label_keys_override.as_ref().expect("override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys
                .iter()
                .position(|k| k == "zone")
                .expect("zone key present");
            ordered.push((vals[zone_idx].clone(), el.value));
        }
        // Per-window sums 300,600,900,1200 / 120s coverage = 2.5, 5,
        // 7.5, 10 → topk descending = z3, z2, z1, z0.
        let labels_in_order: Vec<&str> = ordered.iter().map(|(z, _)| z.as_str()).collect();
        assert_eq!(
            labels_in_order,
            vec!["z3", "z2", "z1", "z0"],
            "topk emits zones in descending rate order: {ordered:?}"
        );
        assert!((ordered[0].1 - 10.0).abs() < 1e-9, "got {}", ordered[0].1);
        assert!((ordered[3].1 - 2.5).abs() < 1e-9, "got {}", ordered[3].1);
    }

    /// `topk(2, sum by (zone) (rate(...)))` — same shape but K < n,
    /// so the fallback must truncate to the top 2 by descending rate.
    #[tokio::test]
    async fn execute_topk_2_slices_to_top_2() {
        use crate::precompute_engine::operators::sum_accumulator::SumAccumulator;
        use crate::query_engines::query_result::QueryResult;
        use crate::storage_engines::sketch_db::data::AggregationType;

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

    // ── P1-1 / P2-6 — rate over FrequencyEstimate (CMS) + keyed safe-miss ──

    /// A FrequencyEstimate (CountMin) sid with `total` inserts in its
    /// matrix row 0, registered for `metric` / `group_by_keys`, carrying
    /// one PROTO_FULL window anchored just before `now`.
    fn register_cms_freq_sid(
        idx: &SketchStore,
        sid: u64,
        metric: &str,
        group_by: &[&str],
        spatial_filter: &str,
        total_inserts: i64,
        now_ms: u64,
    ) {
        let cfg = SketchConfig::CountMin { rows: 2, cols: 4 };
        idx.register(SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<BTreeSet<_>>(),
            capability: Some(Capability::FrequencyEstimate(SketchKindHandle::CountMin)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::CountMin,
                config: cfg.clone(),
                spatial_filter_canonical: spatial_filter.to_string(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        // Build a CountMinState PROTO_FULL frame whose row 0 sums to
        // `total_inserts` (decode_frequency_total reads row 0's sum).
        let bytes = encode_cms_state_proto(2, 4, total_inserts);
        let window_start = now_ms.saturating_sub(60_000);
        let window_end = now_ms.saturating_sub(30_000);
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (window_start, window_end),
            SketchSampleState {
                bytes,
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::ProtoFull,
            },
        );
    }

    /// Encode a `CountMinState` with `rows`×`cols` int matrix where row 0
    /// holds `row0_total` in its first cell (rest zero). Mirrors the wire
    /// form `decoders::decode_cms_from_proto` reads.
    fn encode_cms_state_proto(rows: u32, cols: u32, row0_total: i64) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;
        let mut counts_int = vec![0i64; (rows * cols) as usize];
        counts_int[0] = row0_total; // row 0, col 0
        let state = CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            ..Default::default()
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountMin(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn now_ms_for_test() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn rate_over_cms_frequency_dispatches_via_fallback() {
        use crate::query_engines::query_result::QueryResult;
        // P1-1: `rate(cms_metric[5m])` lowers to ExactAgg(Sum)+Rate, but
        // the sid is FrequencyEstimate — no ExactAgg sid matches. The
        // engine's frequency-rate fallback must answer it (Σ per-window
        // frequency total ÷ coverage-clamped range) instead of failing
        // over to archive.
        let now = now_ms_for_test();
        let idx = Arc::new(SketchStore::new());
        register_cms_freq_sid(&idx, 7000, "cms_metric", &[], "", 600, now);

        let engine = build_engine_with_index(idx);
        let result = engine.execute("rate(cms_metric[5m])").await;
        match result {
            Ok(QueryResult::Vector(v)) => {
                assert_eq!(v.values.len(), 1, "one rate series");
                // 600 inserts over a ~5m coverage-clamped window → a
                // positive per-second rate.
                let val = v.values[0].value;
                assert!(val > 0.0, "rate must be positive, got {val}");
            }
            other => panic!(
                "expected a Vector rate result from the frequency-rate fallback, got {other:?}"
            ),
        }
    }

    #[tokio::test]
    async fn rate_over_cms_frequency_misses_when_no_freq_sid() {
        // No CMS sid registered → the fallback finds nothing and the
        // query fails over to archive (CapabilityMiss), unchanged.
        let idx = Arc::new(SketchStore::new());
        let engine = build_engine_with_index(idx);
        let err = engine
            .execute("rate(cms_metric[5m])")
            .await
            .expect_err("no freq sid → capability-miss to archive");
        assert!(matches!(err, EngineError::CapabilityMiss { .. }));
    }

    #[tokio::test]
    async fn keyed_cms_frequency_fails_over_instead_of_misleading_total() {
        // P2-6: a per-item selector `cms_metric{item="X"}` against a
        // FrequencyEstimate sid would silently get the per-window bucket
        // TOTAL (all items), not item X's count. The engine must
        // capability-miss to archive rather than return that misleading
        // total. The sid here is registered with an EMPTY spatial filter,
        // so the `{item="X"}` matcher is an additional per-item selector
        // not baked into the sketch.
        let now = now_ms_for_test();
        let idx = Arc::new(SketchStore::new());
        register_cms_freq_sid(&idx, 7100, "cms_metric", &[], "", 600, now);

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("count_over_time(cms_metric{item=\"X\"}[5m])")
            .await;
        match result {
            Err(EngineError::CapabilityMiss { detail, .. }) => {
                assert!(
                    detail.contains("per-item") || detail.contains("string-keyed"),
                    "expected the P2-6 per-item safe-miss detail, got: {detail}"
                );
            }
            other => panic!("keyed CMS frequency must fail over to archive (P2-6), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bare_cms_frequency_still_answers_after_p2_6() {
        use crate::query_engines::query_result::QueryResult;
        // Regression guard: the working `count_over_time(cms_metric[5m])`
        // demo (NO item key, empty spatial filter) must still be answered
        // by the warm tier after the P2-6 safe-miss was added.
        let now = now_ms_for_test();
        let idx = Arc::new(SketchStore::new());
        register_cms_freq_sid(&idx, 7200, "cms_metric", &[], "", 600, now);

        let engine = build_engine_with_index(idx);
        let result = engine.execute("count_over_time(cms_metric[5m])").await;
        assert!(
            matches!(
                result,
                Ok(QueryResult::Vector(_)) | Ok(QueryResult::Matrix(_))
            ),
            "bare count_over_time over CMS must still be answered warm, got {result:?}"
        );
    }
}

// ===========================================================================
// Outer-aggregation fold tests (issue #296).
//
// `apply_outer_agg_fold` collapses the inner reducer's per-row
// `ASAPTierResult` into one row per `by`-group, using the analyzer's
// typed `OuterAgg` operator. Identity-case coverage (single-value
// group) is load-bearing for asap's per-zone DDSketch shape, which is
// what `max by (zone) (quantile_over_time(...))` produces.
// ===========================================================================
#[cfg(test)]
mod outer_agg_fold_tests {
    use super::apply_outer_agg_fold;
    use crate::storage_engines::sketch_db::query::ASAPTierResult;
    use control_plane::asap_tier_analysis::OuterAgg;
    use std::collections::BTreeMap;

    fn labels(items: &[(&str, &str)]) -> BTreeMap<String, String> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Identity-case (issue #296): inner reducer emits one row per
    /// by-group already (asap's per-zone DDSketch sketch shape). The
    /// fold returns the same value unchanged for every group.
    #[test]
    fn max_by_zone_over_single_value_groups_is_identity() {
        let inner = ASAPTierResult {
            series: vec![
                (labels(&[("zone", "z0")]), vec![(100, 0.91)]),
                (labels(&[("zone", "z1")]), vec![(100, 0.95)]),
                (labels(&[("zone", "z2")]), vec![(100, 0.93)]),
            ],
            coverage: Some((100, 100)),
        };
        let out = apply_outer_agg_fold(inner, &OuterAgg::Max(vec!["zone".to_string()]));
        assert_eq!(out.series.len(), 3, "one row per zone preserved");
        let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
        for (lm, samples) in &out.series {
            by_zone.insert(
                lm.get("zone").cloned().expect("zone preserved"),
                samples[0].1,
            );
        }
        assert_eq!(by_zone.get("z0").copied(), Some(0.91));
        assert_eq!(by_zone.get("z1").copied(), Some(0.95));
        assert_eq!(by_zone.get("z2").copied(), Some(0.93));
    }

    /// `avg by (zone)` over a single-value-per-zone result returns
    /// the same shape (identity). Mirrors the max case but exercises
    /// the avg-specific fold dispatch.
    #[test]
    fn avg_by_zone_over_single_value_groups_is_identity() {
        let inner = ASAPTierResult {
            series: vec![
                (labels(&[("zone", "z0")]), vec![(200, 1.5)]),
                (labels(&[("zone", "z1")]), vec![(200, 2.5)]),
            ],
            coverage: Some((200, 200)),
        };
        let out = apply_outer_agg_fold(inner, &OuterAgg::Avg(vec!["zone".to_string()]));
        assert_eq!(out.series.len(), 2);
        let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
        for (lm, samples) in &out.series {
            by_zone.insert(lm.get("zone").cloned().unwrap(), samples[0].1);
        }
        assert_eq!(by_zone.get("z0").copied(), Some(1.5));
        assert_eq!(by_zone.get("z1").copied(), Some(2.5));
    }

    /// `max by (zone)` over MULTIPLE rows per zone (e.g. multi-rack
    /// inner) folds each zone's rows by max. Verifies the general
    /// multi-value fold dispatch (the identity case above is a
    /// degenerate sub-case).
    #[test]
    fn max_by_zone_over_multi_value_groups_folds_per_group() {
        let inner = ASAPTierResult {
            series: vec![
                (labels(&[("zone", "z0"), ("rack", "r0")]), vec![(100, 0.91)]),
                (labels(&[("zone", "z0"), ("rack", "r1")]), vec![(100, 0.85)]),
                (labels(&[("zone", "z1"), ("rack", "r0")]), vec![(100, 0.50)]),
                (labels(&[("zone", "z1"), ("rack", "r1")]), vec![(100, 0.95)]),
            ],
            coverage: Some((100, 100)),
        };
        let out = apply_outer_agg_fold(inner, &OuterAgg::Max(vec!["zone".to_string()]));
        assert_eq!(out.series.len(), 2, "collapsed to one row per zone");
        let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
        for (lm, samples) in &out.series {
            assert!(
                !lm.contains_key("rack"),
                "rack label projected away by `by (zone)`"
            );
            by_zone.insert(lm.get("zone").cloned().unwrap(), samples[0].1);
        }
        assert_eq!(by_zone.get("z0").copied(), Some(0.91));
        assert_eq!(by_zone.get("z1").copied(), Some(0.95));
    }

    /// `count by (zone)` over multi-rack input returns the number of
    /// contributing rows per zone (not the sum of values).
    #[test]
    fn count_by_zone_returns_cardinality_per_group() {
        let inner = ASAPTierResult {
            series: vec![
                (labels(&[("zone", "z0"), ("rack", "r0")]), vec![(100, 0.91)]),
                (labels(&[("zone", "z0"), ("rack", "r1")]), vec![(100, 0.85)]),
                (labels(&[("zone", "z0"), ("rack", "r2")]), vec![(100, 0.50)]),
                (labels(&[("zone", "z1"), ("rack", "r0")]), vec![(100, 0.95)]),
            ],
            coverage: Some((100, 100)),
        };
        let out = apply_outer_agg_fold(inner, &OuterAgg::Count(vec!["zone".to_string()]));
        assert_eq!(out.series.len(), 2);
        let mut by_zone: BTreeMap<String, f64> = BTreeMap::new();
        for (lm, samples) in &out.series {
            by_zone.insert(lm.get("zone").cloned().unwrap(), samples[0].1);
        }
        assert_eq!(by_zone.get("z0").copied(), Some(3.0), "3 racks in z0");
        assert_eq!(by_zone.get("z1").copied(), Some(1.0), "1 rack in z1");
    }

    /// `OuterAgg::None` short-circuits — input passes through unchanged.
    #[test]
    fn none_outer_agg_returns_input_unchanged() {
        let inner = ASAPTierResult {
            series: vec![(labels(&[("zone", "z0")]), vec![(100, 7.0)])],
            coverage: Some((100, 100)),
        };
        let out = apply_outer_agg_fold(inner.clone(), &OuterAgg::None);
        assert_eq!(out.series, inner.series);
        assert_eq!(out.coverage, inner.coverage);
    }
}

// ===========================================================================
// Engine-level integration test for issue #296 — `max by (zone)
// (quantile_over_time(0.99, m[5m]))` over per-zone ExactAgg(Sum) sids
// must reach the reducer (not capability-miss). The asap engine's
// `evaluate` path returns DDSketch-decoded quantile values per zone;
// the outer-agg fold then collapses each zone's single row into a
// single value (identity). Pre-fix the query produced a CapabilityMiss
// because no analyzer-side composition existed.
// ===========================================================================
#[cfg(test)]
mod outer_agg_integration_tests {
    use super::*;
    use crate::query_engines::query_result::QueryResult;
    use crate::query_engines::routing::query_engine_routing::QueryEngine as _;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchConfig, SketchEncoding, SketchInstanceMetadata,
        SketchKindHandle, SketchSampleState, SketchStore,
    };
    use crate::storage_engines::types::HotReloadStreamingConfig;
    use asap_sketchlib::DdSketch;
    use asap_sketchlib::MessagePackCodec;
    use std::collections::{BTreeMap, BTreeSet};

    fn build_engine_with_index(idx: Arc<SketchStore>) -> ASAPQueryEngine {
        let streaming_config = Arc::new(crate::storage_engines::types::StreamingConfig::default());
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
        ASAPQueryEngine::new_with_hot_reload(hot_reload, 15000).with_sketch_index(idx)
    }

    fn dd_sketch_with_values(values: &[f64]) -> Vec<u8> {
        // The msgpack encoding round-trips through
        // `DdSketch::deserialize_msgpack` on the engine side — simpler
        // than the proto envelope and supported by `SketchEncoding::MsgpackFull`.
        let mut sk = DdSketch::new(0.01);
        for v in values {
            sk.update(*v);
        }
        sk.to_msgpack().expect("ddsketch msgpack serialization")
    }

    fn dd_meta_for(sid: u64, metric: &str, group_by: &[&str]) -> SketchInstanceMetadata {
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
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

    /// Issue #296 reproduction:
    /// `max by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))`
    /// over per-zone DDSketch sids must dispatch through the analyzer
    /// path AND apply the engine's outer-agg fold. Each zone has one
    /// natural row from the inner quantile evaluation — the outer
    /// max-by-zone fold is identity, so the result mirrors the
    /// inner-only query (same per-zone shape, same per-zone values).
    /// Pre-fix this returned `{"error":"No result for query"}` because
    /// the analyzer rejected the outer-agg-on-function composition.
    #[tokio::test]
    async fn execute_max_by_zone_over_quantile_over_time_returns_per_zone() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(30_000);

        // Two zones, distinct value distributions so the per-zone p99
        // is observably different — proves the per-zone identity case
        // didn't get accidentally folded across zones.
        for (i, (zone, vals)) in [
            (
                "z0",
                vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            ),
            ("z1", vec![100.0_f64, 200.0, 300.0, 400.0, 500.0]),
        ]
        .iter()
        .enumerate()
        {
            let sid = 21_000 + i as u64;
            idx.register(dd_meta_for(sid, "http_latency_ms", &["zone"]));
            let bytes = dd_sketch_with_values(vals);
            idx.append_sample(
                sid,
                BTreeMap::from([("zone".to_string(), zone.to_string())]),
                (w_start, w_end),
                SketchSampleState {
                    bytes,
                    encoding: SketchEncoding::MsgpackFull,
                },
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("max by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))")
            .await
            .expect(
                "issue #296: max by (zone) over quantile_over_time must \
                 reach the reducer + apply the outer-agg fold, not \
                 capability-miss",
            );

        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(
            vector.values.len(),
            2,
            "identity case: one row per zone preserved by outer max-fold"
        );

        // Per-zone p99 (within DDSketch's relative accuracy bound):
        //   z0 p99 of [1..=10] ≈ 10.0
        //   z1 p99 of [100, 200, 300, 400, 500] ≈ 500.0
        let mut by_zone: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for el in &vector.values {
            let keys = el.label_keys_override.as_ref().expect("override populated");
            let vals = &el.labels.labels;
            let zone_idx = keys.iter().position(|k| k == "zone").expect("zone key");
            by_zone.insert(vals[zone_idx].clone(), el.value);
        }
        let z0 = by_zone.get("z0").copied().expect("z0 row present");
        let z1 = by_zone.get("z1").copied().expect("z1 row present");
        // The test's intent is "per-zone identity preserved by the
        // outer max-fold" — z0 + z1 must remain distinct rows with
        // distinct values reflecting their distinct underlying
        // distributions. Exact-value assertions are unreliable on a
        // ≤10-sample DDSketch fixture (p99 with few samples lands on
        // the bucket containing one of the largest 1-2 samples, and
        // bucket midpoints can drift 10-20% from the true value).
        // Real workloads with 100s+ samples/window stay well within
        // 5%, validated by smoke + multinode. Here we assert the
        // ordering + ballpark ranges that prove the fold preserved
        // per-zone identity.
        assert!(z0 > 0.0 && z0 < 50.0, "z0 p99 in [1..=10] range, got {z0}");
        assert!(
            z1 > 100.0 && z1 < 1000.0,
            "z1 p99 in [100..=500] range, got {z1}"
        );
        assert!(
            z1 > z0,
            "z1 ({z1}) > z0 ({z0}) — per-zone identity preserved"
        );
    }

    /// Regression: a candidate's sid set legitimately contains a MIX of
    /// `Ghost` (retired-then-evicted or merged-away, no data) and `Hit`
    /// (Active, carrying live sketch state) sids under the same metric.
    /// This is the exact production shape behind the warm-quantile miss:
    /// the metric's `instances_matching` walk returns the older retired
    /// sketch sids (now dataless ⇒ Ghost) alongside the freshly-minted
    /// Active sketch sids. Iterating ascending-u64, the older Ghost sid
    /// was hit first and aborted the WHOLE query to CapabilityMiss before
    /// the Active sid could answer. After the fix, non-Hit sids are
    /// skipped and the query resolves against the Active sid.
    #[tokio::test]
    async fn ghost_sid_does_not_mask_active_hit_sid_for_quantile() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(30_000);

        // Ghost sid (lower number ⇒ iterated first): registered metadata,
        // never appended any sample state. `classify` → Ghost.
        idx.register(dd_meta_for(1, "http_latency_ms", &["zone"]));

        // Active Hit sid (higher number): carries a real DDSketch window.
        idx.register(dd_meta_for(2, "http_latency_ms", &["zone"]));
        idx.append_sample(
            2,
            BTreeMap::from([("zone".to_string(), "z0".to_string())]),
            (w_start, w_end),
            SketchSampleState {
                bytes: dd_sketch_with_values(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]),
                encoding: SketchEncoding::MsgpackFull,
            },
        );

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("quantile_over_time(0.99, http_latency_ms[5m])")
            .await
            .expect(
                "a dataless Ghost sid must not abort the query when an \
                 Active Hit sid under the same metric can answer it",
            );
        let vector = match result {
            QueryResult::Vector(v) => v,
            other => panic!("expected Vector, got {other:?}"),
        };
        assert_eq!(
            vector.values.len(),
            1,
            "the single Active sid answers; the Ghost is skipped"
        );
        assert!(
            vector.values[0].value > 0.0,
            "p99 of [1..=10] is a positive quantile, got {}",
            vector.values[0].value
        );
    }

    /// `avg by (zone) (quantile_over_time(0.99, m[5m]))` — same shape,
    /// avg fold instead of max. Identity case ⇒ same per-zone values.
    #[tokio::test]
    async fn execute_avg_by_zone_over_quantile_over_time_returns_per_zone() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let w_start = now_ms.saturating_sub(60_000);
        let w_end = now_ms.saturating_sub(30_000);

        for (i, (zone, vals)) in [
            (
                "z0",
                vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            ),
            ("z1", vec![50.0_f64, 100.0, 150.0, 200.0, 250.0]),
        ]
        .iter()
        .enumerate()
        {
            let sid = 22_000 + i as u64;
            idx.register(dd_meta_for(sid, "http_latency_ms", &["zone"]));
            let bytes = dd_sketch_with_values(vals);
            idx.append_sample(
                sid,
                BTreeMap::from([("zone".to_string(), zone.to_string())]),
                (w_start, w_end),
                SketchSampleState {
                    bytes,
                    encoding: SketchEncoding::MsgpackFull,
                },
            );
        }

        let engine = build_engine_with_index(idx);
        let result = engine
            .execute("avg by (zone) (quantile_over_time(0.99, http_latency_ms[5m]))")
            .await
            .expect("avg-by + quantile_over_time must succeed (issue #296)");
        match result {
            QueryResult::Vector(v) => assert_eq!(v.values.len(), 2),
            other => panic!("expected Vector, got {other:?}"),
        }
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
    use crate::query_engines::query_result::{QueryResult, RangeVectorElement, Sample};
    use crate::storage_engines::types::KeyByLabelValues;

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
            _ => panic!("expected matrix"),
        };
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
            _ => panic!("expected matrix"),
        };
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

// ---------------------------------------------------------------------------
// FIX 3 — RANGE-query warm+archive hybrid stitch.
//
// The instant path already stitches; the range path historically returned
// warm-only, so a `[start, end]` request whose warm sketches only cover a
// suffix lost the prefix. These tests drive `execute_range_promql_modern`
// with an archive engine wired and warm coverage narrower than the request,
// and assert the stitched matrix covers the FULL range (prefix from archive,
// suffix from warm).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod range_stitch_tests {
    use super::*;
    use crate::query_engines::query_result::{QueryResult, RangeVectorElement, Sample};
    use crate::query_engines::routing::query_engine_routing::{EngineCapabilities, QueryEngine};
    use crate::query_engines::EngineError;
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchConfig, SketchEncoding, SketchInstanceMetadata,
        SketchKindHandle, SketchSampleState, SketchStore,
    };
    use crate::storage_engines::types::{HotReloadStreamingConfig, KeyByLabelValues};
    use async_trait::async_trait;
    use std::collections::{BTreeMap, BTreeSet};

    /// Mock archive engine: returns a fixed full-range matrix for any range
    /// query, so the stitch can pull the uncovered prefix from it.
    struct FakeArchive {
        matrix: QueryResult,
    }

    #[async_trait]
    impl QueryEngine for FakeArchive {
        async fn execute(&self, _query: &str) -> Result<QueryResult, EngineError> {
            Ok(self.matrix.clone())
        }
        async fn execute_range(
            &self,
            _query: &str,
            _start_ms: u64,
            _end_ms: u64,
            _step_ms: u64,
        ) -> Result<QueryResult, EngineError> {
            Ok(self.matrix.clone())
        }
        fn capabilities(&self) -> EngineCapabilities {
            EngineCapabilities {
                data_source_id: crate::storage_engines::types::StorageBackend::GorillaObjectStore
                    .data_source_id(),
                storage_backend: crate::storage_engines::types::StorageBackend::GorillaObjectStore,
                supports_streams_above_bytes: usize::MAX,
            }
        }
    }

    /// Encode a CountMin FULL proto frame whose row 0 sums to `total`
    /// (the per-window frequency TOTAL the `count_over_time` reducer reads).
    fn cms_bytes(total: i64) -> Vec<u8> {
        use asap_sketchlib::proto::sketchlib::{
            sketch_envelope, CountMinState, CounterType, SketchEnvelope,
        };
        use prost::Message;
        let (rows, cols) = (2u32, 4u32);
        let mut counts_int = vec![0i64; (rows * cols) as usize];
        counts_int[0] = total;
        let state = CountMinState {
            rows,
            cols,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            ..Default::default()
        };
        SketchEnvelope {
            sketch_state: Some(sketch_envelope::SketchState::CountMin(state)),
            ..Default::default()
        }
        .encode_to_vec()
    }

    /// A CountMin FrequencyEstimate sid — `count_over_time` over it emits one
    /// PER-WINDOW sample (not a single cumulative scalar), which is what the
    /// range stitch needs so warm contributes one value per covered window.
    fn cms_meta(sid: u64, metric: &str) -> SketchInstanceMetadata {
        let cfg = SketchConfig::CountMin { rows: 2, cols: 4 };
        SketchInstanceMetadata {
            sid,
            metric_name: metric.to_string(),
            group_by_keys: BTreeSet::new(),
            capability: Some(Capability::FrequencyEstimate(SketchKindHandle::CountMin)),
            agg_kind: crate::storage_engines::sketch_db::index::AggKind::Sketch {
                kind: SketchKindHandle::CountMin,
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

    /// Warm DDSketch covers only the SUFFIX of the requested range
    /// (two windows near `end`); archive returns a full-range matrix
    /// including the prefix. The stitched matrix must span the FULL request:
    /// prefix timestamps come from archive, suffix from warm (warm wins on
    /// any overlap).
    #[tokio::test]
    async fn range_stitches_archive_prefix_with_warm_suffix() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Requested range: [now-600s, now].
        let start_ms = now_ms.saturating_sub(600_000);
        let end_ms = now_ms;

        // Warm windows only in the suffix: [now-200s], [now-100s].
        // `count_over_time` over a CountMin sid emits one PER-WINDOW total,
        // so warm contributes a value at BOTH window-ends.
        let warm_w1_end = now_ms.saturating_sub(200_000);
        let warm_w2_end = now_ms.saturating_sub(100_000);
        let warm_w1_total = 100.0_f64;
        let sid = 9100u64;
        idx.register(cms_meta(sid, "req_count"));
        for (w_end, total) in [(warm_w1_end, 100i64), (warm_w2_end, 200i64)] {
            idx.append_sample(
                sid,
                BTreeMap::new(),
                (w_end.saturating_sub(30_000), w_end),
                SketchSampleState {
                    bytes: cms_bytes(total),
                    encoding: SketchEncoding::ProtoFull,
                },
            );
        }

        // Archive provides the WHOLE range, including the prefix the warm
        // tier can't cover. Use the bare empty-label series the DD reducer
        // emits (so labels line up for the stitch merge).
        let labels = KeyByLabelValues::new_with_labels(Vec::new());
        let mut arch_el = RangeVectorElement::new(labels);
        // Prefix samples (before warm coverage) + a suffix sample warm will win.
        let prefix_ts = now_ms.saturating_sub(500_000) as i64;
        let mid_ts = now_ms.saturating_sub(300_000) as i64;
        arch_el.samples.push(Sample::new(prefix_ts as u64, 999.0));
        arch_el.samples.push(Sample::new(mid_ts as u64, 998.0));
        arch_el.samples.push(Sample::new(warm_w1_end, 1.0)); // overlap: warm should win
        let archive = Arc::new(FakeArchive {
            matrix: QueryResult::matrix(vec![arch_el]),
        });

        let streaming_config = Arc::new(crate::storage_engines::types::StreamingConfig::default());
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
        let engine = ASAPQueryEngine::new_with_hot_reload(hot_reload, 15000)
            .with_sketch_index(idx)
            .with_archive_engine(archive);

        let result = engine
            .execute_range_promql_modern("count_over_time(req_count[5m])", start_ms, end_ms, 15_000)
            .await
            .expect("range query must answer (stitched), not error");

        let m = match result {
            QueryResult::Matrix(m) => m,
            other => panic!("expected Matrix, got {other:?}"),
        };
        assert_eq!(m.values.len(), 1, "one merged series: {m:?}");
        let samples = &m.values[0].samples;
        let ts: std::collections::BTreeSet<i64> =
            samples.iter().map(|s| s.timestamp as i64).collect();

        // The PREFIX timestamps (only the archive has them) must be present —
        // this is the whole point of the fix (warm-only would have dropped
        // them).
        assert!(
            ts.contains(&prefix_ts),
            "archive prefix sample (t={prefix_ts}) must survive the stitch: {ts:?}"
        );
        assert!(
            ts.contains(&mid_ts),
            "archive mid sample (t={mid_ts}) must survive the stitch: {ts:?}"
        );
        // The SUFFIX warm windows must be present too.
        assert!(
            ts.contains(&(warm_w1_end as i64)) && ts.contains(&(warm_w2_end as i64)),
            "warm suffix windows must be present: {ts:?}"
        );

        // Warm wins on the overlapping timestamp: at warm_w1_end the value
        // must be the warm per-window total (100), NOT the archive sentinel 1.0.
        let overlap = samples
            .iter()
            .find(|s| s.timestamp == warm_w1_end)
            .expect("overlap sample present");
        assert!(
            (overlap.value - warm_w1_total).abs() < 1e-6,
            "warm must win on overlap (expected warm total {warm_w1_total}, got {})",
            overlap.value
        );
    }

    /// Control: when warm coverage already spans the request exactly
    /// (`cov_lo == start_ms && cov_hi == end_ms`), no stitch is needed and the
    /// warm-only matrix is returned unchanged — the archive is NOT consulted
    /// even though it's wired. Per-window coverage is window-end-point-based,
    /// Control: with NO archive engine wired, the range path returns the
    /// warm-only matrix (no stitch, no error) even when warm coverage is
    /// narrower than the request — the stitch is gated on a configured
    /// archive. This pins that the fix doesn't disturb the archive-less
    /// deployment (the warm tier answers what it can).
    #[tokio::test]
    async fn range_warm_only_when_no_archive_engine() {
        let idx = Arc::new(SketchStore::new());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let start_ms = now_ms.saturating_sub(600_000);
        let end_ms = now_ms;
        // Warm covers only one suffix window — narrower than the request.
        let w_end = now_ms.saturating_sub(100_000);
        let sid = 9200u64;
        idx.register(cms_meta(sid, "req_count"));
        idx.append_sample(
            sid,
            BTreeMap::new(),
            (w_end.saturating_sub(30_000), w_end),
            SketchSampleState {
                bytes: cms_bytes(42),
                encoding: SketchEncoding::ProtoFull,
            },
        );

        // No `.with_archive_engine(...)` — stitch must NOT fire.
        let streaming_config = Arc::new(crate::storage_engines::types::StreamingConfig::default());
        let hot_reload = HotReloadStreamingConfig::from_arc(streaming_config);
        let engine = ASAPQueryEngine::new_with_hot_reload(hot_reload, 15000).with_sketch_index(idx);

        let result = engine
            .execute_range_promql_modern("count_over_time(req_count[5m])", start_ms, end_ms, 15_000)
            .await
            .expect("range query must answer warm-only");
        let m = match result {
            QueryResult::Matrix(m) => m,
            other => panic!("expected Matrix, got {other:?}"),
        };
        // Warm-only: exactly the single warm window-end sample, no archive
        // prefix injected.
        let ts: Vec<u64> = m
            .values
            .iter()
            .flat_map(|el| el.samples.iter().map(|s| s.timestamp))
            .collect();
        assert_eq!(
            ts,
            vec![w_end],
            "warm-only result must carry just the warm window sample: {ts:?}"
        );
    }
}
