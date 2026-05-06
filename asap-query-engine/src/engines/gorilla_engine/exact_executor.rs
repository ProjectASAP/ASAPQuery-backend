//! Per-statistic executors for the Phase-4 Gorilla engine.
//!
//! Two strategies, picked by [`super::query_planner::QueryStatistic::is_streaming_additive`]:
//!
//! * **Streaming-additive** — `Sum / Count / Avg / Min / Max /
//!   Rate / Increase`. Walk chunks one at a time, fold each
//!   sample into a tiny accumulator, drop the decoded chunk
//!   before fetching the next one. Memory is O(1) per query.
//! * **Buffered** — `Quantile / TopK / Cardinality`. Materialise
//!   every in-range sample, then sort or otherwise post-process.
//!   Bounded by [`super::GorillaEngineConfig::max_buffered_samples`];
//!   over-budget queries fail fast with
//!   [`super::EngineError::TooManySamples`] rather than OOM.

use std::sync::Arc;

use tracing::debug;

use crate::drivers::query::fallback::cold_store::{
    ChunkRef, ColdStore, ColdStoreError, RawSample,
};

use super::query_planner::{LabelMatcher, QueryPlan, QueryStatistic};
use super::{EngineError, ExecutionOutcome, GorillaEngineConfig};

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
}

/// Per-statistic executor. Holds an `Arc<dyn ColdStore>` so the
/// engine + executor share the same cold-tier handle without
/// re-implementing trait dispatch.
pub struct ExactExecutor {
    cold_store: Arc<dyn ColdStore>,
    config: GorillaEngineConfig,
}

impl ExactExecutor {
    pub fn new(cold_store: Arc<dyn ColdStore>, config: GorillaEngineConfig) -> Self {
        Self { cold_store, config }
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
            QueryStatistic::Rate => self.execute_streaming_additive(plan, AdditiveOp::Rate).await,
            QueryStatistic::Increase => {
                self.execute_streaming_additive(plan, AdditiveOp::Increase)
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
            .cold_store
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

        let (filtered_chunks, postings_outcome) =
            self.apply_postings_filter(plan, &chunks).await;

        let mut acc = AdditiveAccumulator::new(op);
        let mut samples_scanned: usize = 0;
        let chunks_fetched = filtered_chunks.len();
        for chunk in filtered_chunks {
            let samples = self.cold_store.read_chunk(&chunk).await?;
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
            .cold_store
            .list_postings_for(&plan.metric, start_ms, end_ms, &matchers)
            .await
        {
            Ok(h) => h,
            Err(ColdStoreError::Unsupported(_)) => {
                // Backend doesn't support postings at all (legacy
                // cold store). Surface as missing and fall through.
                debug!("gorilla-engine: cold store does not support postings; falling back to scan-all");
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
        let series_set: std::collections::BTreeSet<u64> =
            hits.series_ids.iter().copied().collect();
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
            .cold_store
            .list_chunks(&plan.metric, start_ms, end_ms)
            .await?;
        let total_chunks = chunks.len();
        let (filtered_chunks, postings_outcome) =
            self.apply_postings_filter(plan, &chunks).await;
        let chunks_fetched = filtered_chunks.len();
        let mut buffer: Vec<RawSample> = Vec::new();
        let limit = self.config.max_buffered_samples;
        for chunk in filtered_chunks {
            let samples = self.cold_store.read_chunk(&chunk).await?;
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
        }
    }
}

