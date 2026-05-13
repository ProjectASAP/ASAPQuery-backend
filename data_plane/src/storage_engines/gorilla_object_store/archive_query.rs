//! Gorilla archive query planner + per-statistic exact executor.
//!
//! Step-1 of the JSONL-deprecation refactor merged the previous
//! `query_planner.rs` (PromQL → [`QueryPlan`]) and `exact_executor.rs`
//! (per-statistic `[`ExactExecutor`]` dispatch) into a single
//! `archive_query.rs` so the archive data flow is readable top-to-bottom
//! in one file: parse PromQL → plan → execute.
//!
//! The two halves keep their existing structure inside the merged
//! file:
//!
//! * **Planner** (top) — translates a PromQL string into a
//!   [`QueryPlan`] (`metric, time_range_ms, statistic,
//!   label_matchers`). Supports `*_over_time`, `rate`, `increase`,
//!   `quantile_over_time`, `topk`, and the v7 `last_over_time`
//!   freshness-probe spelling.
//! * **Executor** (bottom) — takes a [`QueryPlan`] + an
//!   `Arc<dyn Store>` and returns an
//!   [`super::ExecutionOutcome`]. Two strategies, picked by
//!   [`QueryStatistic::is_streaming_additive`]:
//!
//!   - **Streaming-additive** — `Sum / Count / Avg / Min / Max /
//!     Rate / Increase / Last`. One chunk at a time, fold into a
//!     tiny accumulator, drop the decoded chunk before fetching
//!     the next one. Memory is O(1) per query.
//!   - **Buffered** — `Quantile / TopK / Cardinality`. Materialise
//!     every in-range sample, then sort or otherwise post-process.
//!     Bounded by [`super::GorillaEngineConfig::max_buffered_samples`];
//!     over-budget queries fail fast with
//!     [`super::EngineError::TooManySamples`] rather than OOM.

use std::sync::Arc;
use std::time::SystemTime;

use chrono::Utc;
use promql_parser::parser::{
    AggregateExpr, Call, Expr, FunctionArgs, MatrixSelector, NumberLiteral, ParenExpr,
    VectorSelector,
};
use tracing::debug;

use super::store::{ChunkRef, RawSample, Store, StoreError};
use super::{EngineError, ExecutionOutcome, GorillaEngineConfig};

// =====================================================================
// Planner
// =====================================================================

/// Statistic to compute, alongside any extra parameters
/// (quantile φ, top-k k).
#[derive(Debug, Clone, PartialEq)]
pub enum QueryStatistic {
    /// `sum_over_time(m[range])`
    SumOverTime,
    /// `count_over_time(m[range])`
    CountOverTime,
    /// `avg_over_time(m[range])` (= sum / count)
    AvgOverTime,
    /// `min_over_time(m[range])`
    MinOverTime,
    /// `max_over_time(m[range])`
    MaxOverTime,
    /// `rate(m[range])` — `(last - first) / range_seconds`
    Rate,
    /// `increase(m[range])` — `last - first`
    Increase,
    /// `quantile_over_time(φ, m[range])`
    QuantileOverTime { phi: f64 },
    /// `topk(k, sum_over_time(m[range]))`-style aggregation. The
    /// MVP Phase 4 implementation returns the sum of the top-`k`
    /// sample values in the range — once Phase 5 adds spatial
    /// grouping the executor will return a per-group vector.
    TopK { k: usize },
    /// **v7**: `last_over_time(m[range])` — value of the
    /// largest-timestamp sample in the range. Used by issue #46
    /// criterion ⑥ freshness probes; counter-shaped probes encode
    /// `unix_ts_ms_of_emission` in their cumulative value, and
    /// `last_over_time(...)` returns that value so the replay
    /// client can compute per-path freshness deltas.
    LastOverTime,
}

impl QueryStatistic {
    /// True iff the executor can answer this statistic via the
    /// streaming-additive path; false → buffered path (everything
    /// has to be in memory before producing the answer).
    pub fn is_streaming_additive(&self) -> bool {
        matches!(
            self,
            Self::SumOverTime
                | Self::CountOverTime
                | Self::AvgOverTime
                | Self::MinOverTime
                | Self::MaxOverTime
                | Self::Rate
                | Self::Increase
                | Self::LastOverTime
        )
    }
}

/// One label-equality matcher extracted from the PromQL AST. mvp/v5
/// uses these to drive the postings-aware chunk-pruning path.
///
/// The MVP only supports exact equality (`label = "value"`). Regex
/// (`=~`) and inequality (`!=`, `!~`) matchers fall through to a
/// post-decode filter — the postings file holds *exact* values per
/// label, not patterns. The fall-through is correct (just slower)
/// and is signalled to callers via
/// [`QueryPlan::has_unsupported_matchers`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    /// Label name, e.g. `"service"`.
    pub name: String,
    /// Label value, e.g. `"api"`.
    pub value: String,
}

/// Output of [`plan_query`].
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub metric: String,
    /// Half-open `[start_ms, end_ms)` request window. Computed as
    /// `(now_ms - range_ms, now_ms)` from the matrix selector's
    /// `[range]` duration.
    pub time_range_ms: (i64, i64),
    pub statistic: QueryStatistic,
    /// **mvp/v5**: exact-equality label matchers extracted from the
    /// vector selector. Empty for `metric[range]` (no predicate).
    /// Non-empty for `metric{label="value"}[range]`. Used by the
    /// postings-aware chunk filter; `=~` / `!=` / `!~` matchers are
    /// dropped from this list and signalled via
    /// [`Self::has_unsupported_matchers`].
    pub label_matchers: Vec<LabelMatcher>,
    /// **mvp/v5**: `true` iff the original PromQL had at least one
    /// matcher we couldn't translate into a postings lookup (regex,
    /// inequality). The caller must still apply those matchers
    /// post-decode; we surface the flag so `data_source_quirk`
    /// annotations make it back to the client.
    pub has_unsupported_matchers: bool,
}

/// Parse `query` and produce a [`QueryPlan`]. `now` defaults to
/// the system clock; the [`plan_query_at`] variant lets tests pin
/// a deterministic timestamp.
pub fn plan_query(query: &str) -> Result<QueryPlan, String> {
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_else(|_| Utc::now().timestamp_millis());
    plan_query_at(query, now_ms)
}

/// As [`plan_query`], with a caller-supplied `now_ms`.
pub fn plan_query_at(query: &str, now_ms: i64) -> Result<QueryPlan, String> {
    let ast = promql_parser::parser::parse(query).map_err(|e| format!("parse: {e}"))?;
    plan_from_ast(&ast, now_ms)
}

fn plan_from_ast(ast: &Expr, now_ms: i64) -> Result<QueryPlan, String> {
    match ast {
        Expr::Paren(ParenExpr { expr }) => plan_from_ast(expr, now_ms),
        Expr::Call(call) => plan_from_call(call, now_ms),
        Expr::Aggregate(agg) => plan_from_aggregate(agg, now_ms),
        other => Err(format!(
            "unsupported top-level expression: {:?}; the Gorilla engine \
             expects a single function call (rate/increase/*_over_time) \
             or topk(k, ...) aggregation",
            std::mem::discriminant(other)
        )),
    }
}

fn plan_from_call(call: &Call, now_ms: i64) -> Result<QueryPlan, String> {
    let name = call.func.name.to_lowercase();
    match name.as_str() {
        "sum_over_time" | "count_over_time" | "avg_over_time" | "min_over_time"
        | "max_over_time" | "last_over_time" | "rate" | "increase" => {
            let ms = expect_single_matrix_arg(&call.args, &name)?;
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            let stat = match name.as_str() {
                "sum_over_time" => QueryStatistic::SumOverTime,
                "count_over_time" => QueryStatistic::CountOverTime,
                "avg_over_time" => QueryStatistic::AvgOverTime,
                "min_over_time" => QueryStatistic::MinOverTime,
                "max_over_time" => QueryStatistic::MaxOverTime,
                "last_over_time" => QueryStatistic::LastOverTime,
                "rate" => QueryStatistic::Rate,
                "increase" => QueryStatistic::Increase,
                _ => unreachable!(),
            };
            let (label_matchers, has_unsupported) = extract_label_matchers(&ms.vs);
            Ok(QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: stat,
                label_matchers,
                has_unsupported_matchers: has_unsupported,
            })
        }
        "quantile_over_time" => {
            // quantile_over_time(φ, m[range])
            if call.args.args.len() != 2 {
                return Err(format!(
                    "quantile_over_time expects 2 args, got {}",
                    call.args.args.len()
                ));
            }
            let phi = expect_number(&call.args.args[0], "quantile_over_time φ")?;
            let ms = expect_matrix_selector(&call.args.args[1], "quantile_over_time")?;
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            let (label_matchers, has_unsupported) = extract_label_matchers(&ms.vs);
            Ok(QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: QueryStatistic::QuantileOverTime { phi },
                label_matchers,
                has_unsupported_matchers: has_unsupported,
            })
        }
        other => Err(format!(
            "unsupported PromQL function: {other}; the Gorilla engine \
             supports rate/increase/*_over_time/quantile_over_time"
        )),
    }
}

fn plan_from_aggregate(agg: &AggregateExpr, now_ms: i64) -> Result<QueryPlan, String> {
    // PromQL grammar requires aggregation operators to take a
    // vector — so the legal Phase-4 spellings are e.g.
    // `topk(2, sum_over_time(m[10s]))`. We strip the outer
    // aggregation, recurse into the inner call to recover the
    // `(metric, range)` pair, then overlay the TopK statistic.
    let op_str = format!("{}", agg.op);
    if !op_str.eq_ignore_ascii_case("topk") {
        return Err(format!(
            "unsupported top-level aggregation: {op_str}; only `topk(k, ...)` \
             is supported in Phase 4"
        ));
    }
    let k_expr = agg
        .param
        .as_deref()
        .ok_or_else(|| "topk requires a numeric parameter (k)".to_string())?;
    let k = expect_number(k_expr, "topk k")?;
    if !k.is_finite() || k <= 0.0 {
        return Err(format!("topk k must be positive, got {k}"));
    }
    // Recurse into the inner expression — it can be a matrix
    // selector (handled by [`matrix_metric_and_range_ms`]
    // directly) OR a vector-returning function call (the legal
    // PromQL spelling). Either way we end up with a
    // `(metric, range_ms)` pair we can overlay TopK on.
    let inner_plan = match &*agg.expr {
        Expr::MatrixSelector(ms) => {
            let (metric, range_ms) = matrix_metric_and_range_ms(ms);
            let (label_matchers, has_unsupported) = extract_label_matchers(&ms.vs);
            QueryPlan {
                metric,
                time_range_ms: (now_ms - range_ms, now_ms),
                statistic: QueryStatistic::SumOverTime, // overlay below
                label_matchers,
                has_unsupported_matchers: has_unsupported,
            }
        }
        _ => plan_from_ast(&agg.expr, now_ms)?,
    };
    Ok(QueryPlan {
        metric: inner_plan.metric,
        time_range_ms: inner_plan.time_range_ms,
        statistic: QueryStatistic::TopK { k: k as usize },
        label_matchers: inner_plan.label_matchers,
        has_unsupported_matchers: inner_plan.has_unsupported_matchers,
    })
}

fn expect_single_matrix_arg<'a>(
    args: &'a FunctionArgs,
    fname: &str,
) -> Result<&'a MatrixSelector, String> {
    if args.args.len() != 1 {
        return Err(format!(
            "{fname} expects 1 matrix-selector arg, got {}",
            args.args.len()
        ));
    }
    expect_matrix_selector(&args.args[0], fname)
}

fn expect_matrix_selector<'a>(expr: &'a Expr, ctx: &str) -> Result<&'a MatrixSelector, String> {
    match expr {
        Expr::MatrixSelector(ms) => Ok(ms),
        Expr::Paren(ParenExpr { expr }) => expect_matrix_selector(expr, ctx),
        other => Err(format!(
            "{ctx}: expected matrix selector `metric[range]`, got {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn expect_number(expr: &Expr, ctx: &str) -> Result<f64, String> {
    match expr {
        Expr::NumberLiteral(NumberLiteral { val }) => Ok(*val),
        Expr::Paren(ParenExpr { expr }) => expect_number(expr, ctx),
        other => Err(format!(
            "{ctx}: expected numeric literal, got {:?}",
            std::mem::discriminant(other)
        )),
    }
}

fn matrix_metric_and_range_ms(ms: &MatrixSelector) -> (String, i64) {
    let metric = vector_selector_metric(&ms.vs);
    let range_ms = ms.range.as_millis() as i64;
    (metric, range_ms)
}

fn vector_selector_metric(vs: &VectorSelector) -> String {
    if let Some(name) = &vs.name {
        return name.clone();
    }
    // Fallback: inspect matchers for an `__name__` exact match.
    for m in vs.matchers.matchers.iter() {
        if m.name == "__name__" {
            return m.value.clone();
        }
    }
    String::new()
}

/// **mvp/v5**: extract exact-equality label matchers from a vector
/// selector for postings-aware chunk pruning.
///
/// Returns `(supported_matchers, has_unsupported_matchers)`. Supported
/// matchers are the `label = "value"` tuples the postings file can
/// answer directly. Anything else (regex, inequality, the implicit
/// `__name__` matcher) is excluded from `supported_matchers` and
/// flips the second return value to `true` — the executor still
/// applies them post-decode for correctness.
pub(crate) fn extract_label_matchers(vs: &VectorSelector) -> (Vec<LabelMatcher>, bool) {
    use promql_parser::label::MatchOp;

    let mut supported = Vec::new();
    let mut has_unsupported = false;
    for m in vs.matchers.matchers.iter() {
        // The implicit `__name__` matcher is the metric name itself
        // — we already pulled that out of the selector elsewhere.
        if m.name == "__name__" {
            continue;
        }
        match &m.op {
            MatchOp::Equal => {
                supported.push(LabelMatcher {
                    name: m.name.clone(),
                    value: m.value.clone(),
                });
            }
            // Regex / inequality matchers are correctness-relevant
            // but cannot be answered by an exact postings lookup.
            // Surface the flag so the caller emits a quirk
            // annotation; the actual filter is applied post-decode.
            MatchOp::NotEqual | MatchOp::Re(_) | MatchOp::NotRe(_) => {
                has_unsupported = true;
            }
        }
    }
    (supported, has_unsupported)
}

// =====================================================================
// Executor
// =====================================================================

/// Streaming-additive operation tag — what the per-sample fold
/// does. Pulled out so [`ExactExecutor::execute_streaming_additive`]
/// is a single function regardless of which stat is being computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdditiveOp {
    Sum,
    Count,
    /// `(sum, count)` — the engine divides at the end.
    Avg,
    Min,
    Max,
    /// `last - first` over the time-ordered samples.
    Increase,
    /// `(last - first) / range_seconds`.
    Rate,
    /// **v7**: the value of the latest sample in the range. Used by
    /// `last_over_time(<metric>[<range>])` — the freshness probe
    /// queries from MVP v6 issue #46 criterion ⑥. The fold tracks
    /// `(ts_ms, value)` pairs already; this op just returns `value`
    /// of the largest-timestamp sample.
    Last,
}

/// Per-statistic executor. Holds an `Arc<dyn Store>` so the
/// engine + executor share the same archive-tier handle without
/// re-implementing trait dispatch.
pub struct ExactExecutor {
    store: Arc<dyn Store>,
    config: GorillaEngineConfig,
}

impl ExactExecutor {
    pub fn new(store: Arc<dyn Store>, config: GorillaEngineConfig) -> Self {
        Self { store, config }
    }

    /// Top-level dispatch — picks streaming vs buffered based on
    /// the plan's statistic.
    pub async fn execute_plan(&self, plan: &QueryPlan) -> Result<ExecutionOutcome, EngineError> {
        match &plan.statistic {
            QueryStatistic::SumOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Sum).await
            }
            QueryStatistic::CountOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Count)
                    .await
            }
            QueryStatistic::AvgOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Avg).await
            }
            QueryStatistic::MinOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Min).await
            }
            QueryStatistic::MaxOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Max).await
            }
            QueryStatistic::Rate => {
                self.execute_streaming_additive(plan, AdditiveOp::Rate)
                    .await
            }
            QueryStatistic::Increase => {
                self.execute_streaming_additive(plan, AdditiveOp::Increase)
                    .await
            }
            QueryStatistic::LastOverTime => {
                self.execute_streaming_additive(plan, AdditiveOp::Last)
                    .await
            }
            QueryStatistic::QuantileOverTime { phi } => self.execute_quantile(plan, *phi).await,
            QueryStatistic::TopK { k } => self.execute_topk(plan, *k).await,
        }
    }

    /// Streaming additive path. Reads chunks one at a time, applies
    /// the per-sample fold, drops the decoded chunk before fetching
    /// the next one. Memory is O(1) per query, regardless of how
    /// many samples the time range covers.
    ///
    /// **mvp/v5**: when the plan carries label matchers, the executor
    /// first reads the postings sidecar to compute the matching
    /// `series_ids`, then prunes the chunk list down to chunks
    /// whose `label_hash` appears in that set. Falls back to the
    /// scan-all path when postings are missing.
    pub async fn execute_streaming_additive(
        &self,
        plan: &QueryPlan,
        op: AdditiveOp,
    ) -> Result<ExecutionOutcome, EngineError> {
        let (start_ms, end_ms) = plan.time_range_ms;
        let chunks = self
            .store
            .list_chunks(&plan.metric, start_ms, end_ms)
            .await?;
        let total_chunks = chunks.len();
        debug!(
            metric = plan.metric.as_str(),
            chunks = total_chunks,
            op = ?op,
            label_matchers = plan.label_matchers.len(),
            "gorilla-engine: streaming additive over chunks"
        );

        let (filtered_chunks, postings_outcome) = self.apply_postings_filter(plan, &chunks).await;

        let mut acc = AdditiveAccumulator::new(op);
        let mut samples_scanned: usize = 0;
        let chunks_fetched = filtered_chunks.len();
        for chunk in filtered_chunks {
            let samples = self.store.read_chunk(&chunk).await?;
            for s in samples {
                if s.ts_ms >= start_ms && s.ts_ms < end_ms {
                    if !self.sample_matches(plan, &s) {
                        continue;
                    }
                    acc.observe(&s);
                    samples_scanned += 1;
                }
            }
        }

        let value = acc.finalize(plan, op);
        Ok(ExecutionOutcome {
            value,
            samples_scanned,
            chunks_fetched,
            chunks_skipped_via_postings: total_chunks - chunks_fetched,
            postings_filtered_series_count: postings_outcome.matched_series,
            postings_missing: postings_outcome.postings_missing,
        })
    }

    /// **mvp/v5**: apply the postings-aware filter to a chunk list.
    /// Returns `(filtered_chunks, postings_outcome)` where the
    /// outcome captures `(matched_series, postings_missing)` so the
    /// caller can populate [`super::ExecutionOutcome`] without
    /// re-querying.
    async fn apply_postings_filter(
        &self,
        plan: &QueryPlan,
        chunks: &[ChunkRef],
    ) -> (Vec<ChunkRef>, PostingsOutcome) {
        if plan.label_matchers.is_empty() {
            // No predicate — the postings filter is a no-op. The
            // postings file isn't consulted at all in this path.
            return (
                chunks.to_vec(),
                PostingsOutcome {
                    matched_series: 0,
                    postings_missing: false,
                },
            );
        }
        let matchers: Vec<(String, String)> = plan
            .label_matchers
            .iter()
            .map(|LabelMatcher { name, value }| (name.clone(), value.clone()))
            .collect();
        let (start_ms, end_ms) = plan.time_range_ms;
        let hits = match self
            .store
            .list_postings_for(&plan.metric, start_ms, end_ms, &matchers)
            .await
        {
            Ok(h) => h,
            Err(StoreError::Unsupported(_)) => {
                // Backend doesn't support postings at all. Surface
                // as missing and fall through.
                debug!("gorilla-engine: store does not support postings; falling back to scan-all");
                return (
                    chunks.to_vec(),
                    PostingsOutcome {
                        matched_series: 0,
                        postings_missing: true,
                    },
                );
            }
            Err(e) => {
                // Transport/parse failure — log + fall through. We
                // don't propagate the error because the scan-all
                // path is still correct, just slower.
                debug!(error = %e, "gorilla-engine: postings fetch failed; falling back to scan-all");
                return (
                    chunks.to_vec(),
                    PostingsOutcome {
                        matched_series: 0,
                        postings_missing: true,
                    },
                );
            }
        };

        // If any bucket in range was missing postings we can't trust
        // the filter to be complete; scan everything (correctness
        // first, postings are a perf optimization).
        if !hits.fully_covered() {
            return (
                chunks.to_vec(),
                PostingsOutcome {
                    matched_series: hits.series_ids.len(),
                    postings_missing: true,
                },
            );
        }

        // Postings → series_ids → keep only chunks whose
        // `label_hash` is in the set. Chunks with `label_hash == 0`
        // are pre-mvp/v5 multi-series chunks that don't pin a
        // single series — keep them (they may carry matching
        // series; correctness > pruning).
        let series_set: std::collections::BTreeSet<u64> = hits.series_ids.iter().copied().collect();
        let filtered: Vec<ChunkRef> = chunks
            .iter()
            .filter(|c| c.label_hash == 0 || series_set.contains(&c.label_hash))
            .cloned()
            .collect();
        (
            filtered,
            PostingsOutcome {
                matched_series: hits.series_ids.len(),
                postings_missing: false,
            },
        )
    }

    /// Post-decode label-equality filter. Always-true when no
    /// matchers are present (most common). Used as a safety net so
    /// chunks with `label_hash = 0` (multi-series, can't be pruned
    /// at the postings level) still respect the predicate.
    fn sample_matches(&self, plan: &QueryPlan, s: &RawSample) -> bool {
        if plan.label_matchers.is_empty() {
            return true;
        }
        for LabelMatcher { name, value } in &plan.label_matchers {
            match s.labels.get(name) {
                Some(v) if v == value => {}
                _ => return false,
            }
        }
        true
    }

    /// Buffered quantile path. Materialises every in-range sample
    /// up to [`GorillaEngineConfig::max_buffered_samples`], sorts
    /// the value column, and picks the φ-rank using a
    /// nearest-rank rule (matches Prometheus's
    /// `quantile_over_time` semantics for the linear-interp-free
    /// midpoint case — the float index rounds to nearest).
    pub async fn execute_quantile(
        &self,
        plan: &QueryPlan,
        phi: f64,
    ) -> Result<ExecutionOutcome, EngineError> {
        let buffered = self.collect_buffered_samples(plan).await?;
        if buffered.samples.is_empty() {
            return Ok(ExecutionOutcome {
                value: f64::NAN,
                samples_scanned: 0,
                chunks_fetched: buffered.chunks_fetched,
                chunks_skipped_via_postings: buffered.chunks_skipped_via_postings,
                postings_filtered_series_count: buffered.postings_filtered_series_count,
                postings_missing: buffered.postings_missing,
            });
        }

        let mut values: Vec<f64> = buffered.samples.iter().map(|s| s.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let n = values.len() as f64;
        let phi = phi.clamp(0.0, 1.0);
        let raw_idx = ((n - 1.0) * phi).round() as i64;
        let idx = raw_idx.clamp(0, values.len() as i64 - 1) as usize;
        Ok(ExecutionOutcome {
            value: values[idx],
            samples_scanned: values.len(),
            chunks_fetched: buffered.chunks_fetched,
            chunks_skipped_via_postings: buffered.chunks_skipped_via_postings,
            postings_filtered_series_count: buffered.postings_filtered_series_count,
            postings_missing: buffered.postings_missing,
        })
    }

    /// Buffered top-k path. Materialises every in-range sample,
    /// sorts the value column descending, and returns the SUM of
    /// the top-`k` values. The Phase-5 capability router will
    /// extend this to per-group top-k once spatial grouping
    /// lands; the MVP scalar return value matches the existing
    /// `ExecutionOutcome` shape.
    pub async fn execute_topk(
        &self,
        plan: &QueryPlan,
        k: usize,
    ) -> Result<ExecutionOutcome, EngineError> {
        let buffered = self.collect_buffered_samples(plan).await?;
        if buffered.samples.is_empty() {
            return Ok(ExecutionOutcome {
                value: f64::NAN,
                samples_scanned: 0,
                chunks_fetched: buffered.chunks_fetched,
                chunks_skipped_via_postings: buffered.chunks_skipped_via_postings,
                postings_filtered_series_count: buffered.postings_filtered_series_count,
                postings_missing: buffered.postings_missing,
            });
        }

        let mut values: Vec<f64> = buffered.samples.iter().map(|s| s.value).collect();
        values.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
        let take = k.min(values.len());
        let topk_sum: f64 = values.iter().take(take).sum();
        Ok(ExecutionOutcome {
            value: topk_sum,
            samples_scanned: values.len(),
            chunks_fetched: buffered.chunks_fetched,
            chunks_skipped_via_postings: buffered.chunks_skipped_via_postings,
            postings_filtered_series_count: buffered.postings_filtered_series_count,
            postings_missing: buffered.postings_missing,
        })
    }

    /// Shared helper for the buffered paths: walk every chunk,
    /// keep in-range samples, enforce the
    /// [`GorillaEngineConfig::max_buffered_samples`] ceiling.
    /// **mvp/v5**: also applies the postings-aware filter so the
    /// quantile / topk paths share the same pruning as
    /// streaming-additive.
    async fn collect_buffered_samples(
        &self,
        plan: &QueryPlan,
    ) -> Result<BufferedScan, EngineError> {
        let (start_ms, end_ms) = plan.time_range_ms;
        let chunks = self
            .store
            .list_chunks(&plan.metric, start_ms, end_ms)
            .await?;
        let total_chunks = chunks.len();
        let (filtered_chunks, postings_outcome) = self.apply_postings_filter(plan, &chunks).await;
        let chunks_fetched = filtered_chunks.len();
        let mut buffer: Vec<RawSample> = Vec::new();
        let limit = self.config.max_buffered_samples;
        for chunk in filtered_chunks {
            let samples = self.store.read_chunk(&chunk).await?;
            for s in samples {
                if s.ts_ms >= start_ms && s.ts_ms < end_ms {
                    if !self.sample_matches(plan, &s) {
                        continue;
                    }
                    if buffer.len() >= limit {
                        // Surface the over-budget sample count
                        // (limit + 1) so callers can pin
                        // `count > limit` in tests; we don't
                        // bother walking the rest of the chunks
                        // just to tighten the number.
                        return Err(EngineError::TooManySamples {
                            count: buffer.len() + 1,
                            limit,
                        });
                    }
                    buffer.push(s);
                }
            }
        }
        Ok(BufferedScan {
            samples: buffer,
            chunks_fetched,
            chunks_skipped_via_postings: total_chunks - chunks_fetched,
            postings_filtered_series_count: postings_outcome.matched_series,
            postings_missing: postings_outcome.postings_missing,
        })
    }
}

/// Output of [`ExactExecutor::collect_buffered_samples`].
struct BufferedScan {
    samples: Vec<RawSample>,
    chunks_fetched: usize,
    chunks_skipped_via_postings: usize,
    postings_filtered_series_count: usize,
    postings_missing: bool,
}

/// Output of [`ExactExecutor::apply_postings_filter`].
#[derive(Debug, Clone, Copy)]
struct PostingsOutcome {
    matched_series: usize,
    postings_missing: bool,
}

/// Per-sample fold for the streaming-additive path. Fields are
/// kept in raw f64 (sum / min / max) + i64 (count) so the
/// finaliser can pick the right arithmetic per op.
///
/// `op` is taken at `new` time rather than carried as a struct
/// field — the finaliser receives it as an argument, keeping the
/// struct itself op-agnostic and shrinking the per-fold footprint.
#[derive(Debug, Clone, Copy)]
struct AdditiveAccumulator {
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
    /// Earliest observed `(ts_ms, value)` — used by Rate / Increase.
    first: Option<(i64, f64)>,
    /// Latest observed `(ts_ms, value)` — used by Rate / Increase.
    last: Option<(i64, f64)>,
}

impl AdditiveAccumulator {
    fn new(_op: AdditiveOp) -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            first: None,
            last: None,
        }
    }

    fn observe(&mut self, s: &RawSample) {
        self.sum += s.value;
        self.count += 1;
        if s.value < self.min {
            self.min = s.value;
        }
        if s.value > self.max {
            self.max = s.value;
        }
        match self.first {
            None => self.first = Some((s.ts_ms, s.value)),
            Some((ts, _)) if s.ts_ms < ts => self.first = Some((s.ts_ms, s.value)),
            _ => {}
        }
        match self.last {
            None => self.last = Some((s.ts_ms, s.value)),
            Some((ts, _)) if s.ts_ms > ts => self.last = Some((s.ts_ms, s.value)),
            _ => {}
        }
    }

    /// Convert the running accumulator into a final scalar.
    /// Returns `NaN` for the empty-time-range case so downstream
    /// formatting stays consistent.
    fn finalize(&self, plan: &QueryPlan, op: AdditiveOp) -> f64 {
        if self.count == 0 {
            return match op {
                AdditiveOp::Count => 0.0,
                _ => f64::NAN,
            };
        }
        match op {
            AdditiveOp::Sum => self.sum,
            AdditiveOp::Count => self.count as f64,
            AdditiveOp::Avg => self.sum / self.count as f64,
            AdditiveOp::Min => self.min,
            AdditiveOp::Max => self.max,
            AdditiveOp::Increase => match (self.first, self.last) {
                (Some((_, fv)), Some((_, lv))) => lv - fv,
                _ => f64::NAN,
            },
            AdditiveOp::Rate => match (self.first, self.last) {
                (Some((_, fv)), Some((_, lv))) => {
                    let (start_ms, end_ms) = plan.time_range_ms;
                    let range_secs = ((end_ms - start_ms).max(1)) as f64 / 1000.0;
                    if range_secs <= 0.0 {
                        f64::NAN
                    } else {
                        (lv - fv) / range_secs
                    }
                }
                _ => f64::NAN,
            },
            // v7 / issue #46 ⑥: return the value of the
            // largest-timestamp sample. Counter-shaped freshness
            // probes (http_freshness_probe_*) encode the unix_ts_ms
            // of the most recent emission directly in the
            // cumulative counter value, so `last_over_time(...)`
            // returning that value lets the replay client subtract
            // the polled timestamp and get a per-path freshness
            // delta.
            AdditiveOp::Last => match self.last {
                Some((_, lv)) => lv,
                None => f64::NAN,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_715_000_000_000;

    #[test]
    fn plans_sum_over_time() {
        let plan = plan_query_at("sum_over_time(http_requests_total[5m])", NOW).unwrap();
        assert_eq!(plan.metric, "http_requests_total");
        assert_eq!(plan.statistic, QueryStatistic::SumOverTime);
        assert_eq!(plan.time_range_ms, (NOW - 5 * 60_000, NOW));
    }

    #[test]
    fn plans_quantile_over_time() {
        let plan = plan_query_at("quantile_over_time(0.99, latency_ms[1m])", NOW).unwrap();
        assert_eq!(plan.metric, "latency_ms");
        assert!(matches!(
            plan.statistic,
            QueryStatistic::QuantileOverTime { phi } if (phi - 0.99).abs() < 1e-12
        ));
    }

    #[test]
    fn plans_topk() {
        // Legal PromQL spelling: aggregation wraps a vector-returning
        // function call. The Phase-4 planner peels off the outer
        // `topk` and recovers the `(metric, range)` pair from the
        // inner `sum_over_time(...)`.
        let plan = plan_query_at("topk(3, sum_over_time(m[10s]))", NOW).unwrap();
        assert!(matches!(plan.statistic, QueryStatistic::TopK { k } if k == 3));
        assert_eq!(plan.metric, "m");
        assert_eq!(plan.time_range_ms, (NOW - 10_000, NOW));
    }

    #[test]
    fn rejects_binary_expression() {
        assert!(plan_query_at("foo + bar", NOW).is_err());
    }

    #[test]
    fn streaming_classification() {
        assert!(QueryStatistic::SumOverTime.is_streaming_additive());
        assert!(QueryStatistic::Rate.is_streaming_additive());
        assert!(QueryStatistic::LastOverTime.is_streaming_additive());
        assert!(!QueryStatistic::QuantileOverTime { phi: 0.5 }.is_streaming_additive());
        assert!(!QueryStatistic::TopK { k: 1 }.is_streaming_additive());
    }

    #[test]
    fn plans_last_over_time_v7() {
        // v7: `last_over_time(...)` translates to the streaming
        // additive path, picking the value of the largest-timestamp
        // sample in [now-range, now). Issue #46 ⑥ freshness probes
        // ride this path.
        let plan = plan_query_at("last_over_time(http_freshness_probe_warm[10s])", NOW).unwrap();
        assert_eq!(plan.metric, "http_freshness_probe_warm");
        assert_eq!(plan.statistic, QueryStatistic::LastOverTime);
        assert_eq!(plan.time_range_ms, (NOW - 10_000, NOW));
    }
}
