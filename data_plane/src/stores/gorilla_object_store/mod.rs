//! Gorilla object store and legacy in-process archive executor.
//!
//! This module owns the Gorilla chunk/object-store implementation. The
//! public archive query engine is [`crate::query_engines::thanos_query_engine`]; the
//! in-process [`GorillaQueryEngine`] remains here as a legacy fallback for
//! development deployments that still read per-hour Gorilla chunks directly.
//!
//! ## Module layout (post Step-1 refactor)
//!
//! * [`archive_query`] — query planner + per-statistic exact executor (the
//!   merged form of the previous `query_planner.rs` +
//!   `exact_executor.rs`).
//! * [`store`] — `GorillaS3Store` (the only `Store` impl after
//!   the JSONL deletion) + `Store`/`ObjectStore` traits +
//!   `RawSample` / `ChunkRef` types.
//! * [`postings`] — postings-sidecar cache + per-bucket
//!   intersection helper.
//! * [`s3_cost`] — instrumented S3 client wrapper that ticks the
//!   process-wide cost counters surfaced on
//!   `/internal/s3_cost.csv` + `/metrics`.
//!
//! Result wrapping pins three things:
//!
//! 1. an [`crate::stores::sketch_db::AccuracyEnvelope`] with
//!    `kind = Exact`, ε = 0, δ = 0,
//! 2. a `data_source: thanos_query` info line,
//! 3. cheap diagnostics (`samples_scanned`, `chunks_fetched`).
//!
//! See `docs/design-gorilla-s3-cold-engine.md` §6.
//!
//! ## Two execution strategies
//!
//! Per-statistic dispatch in [`archive_query::ExactExecutor`]:
//!
//! * **Streaming-additive** — `Sum`, `Count`, `Min`, `Max`, `Rate`,
//!   `Increase` (and `Avg` derived as Sum/Count). One chunk at a
//!   time, fold into a small accumulator, drop the decoded samples
//!   before fetching the next chunk. Memory cost is O(1) per group.
//! * **Buffered** — `Quantile`, `TopK`, `Cardinality`. Materialise
//!   every in-range sample, then sort / count. Bounded by
//!   [`GorillaEngineConfig::max_buffered_samples`]; over-budget
//!   queries fail fast with [`EngineError::TooManySamples`].

pub mod archive_query;
pub mod postings;
pub mod s3_cost;
pub mod store;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::time::error::Elapsed;
use tracing::debug;

use crate::stores::schema::KeyByLabelValues;
use crate::query_engines::query_result::{InstantVectorElement, QueryResult};
use crate::stores::sketch_db::accuracy::{AccuracyEnvelope, AccuracyProfile};

pub use archive_query::{
    plan_query, plan_query_at, AdditiveOp, ExactExecutor, LabelMatcher, QueryPlan, QueryStatistic,
};
pub use postings::PostingsHits;
pub use s3_cost::{
    global_s3_cost_counters, S3CostCounters, S3CostSnapshot, S3CostTrackingObjectStore,
};
pub use store::{
    ChunkRef, GorillaS3Config, GorillaS3ConfigError, GorillaS3Store, ObjectStore, RawSample,
    S3ObjectStore, Store, StoreError,
};
/// Marker line that every archive answer carries on its `infos` array.
/// The legacy in-process Gorilla executor is an implementation detail;
/// the public archive engine identity is Thanos.
pub const DATA_SOURCE_GORILLA_ARCHIVE: &str = "data_source: thanos_query";

/// Tunable runtime knobs for the Gorilla query engine.
///
/// Call sites typically construct via `Default::default()`; tests
/// override `max_buffered_samples` to exercise the bounded-buffer
/// guard.
#[derive(Debug, Clone)]
pub struct GorillaEngineConfig {
    /// Hard cap on the number of samples a buffered-aggregate
    /// query (quantile / topk / cardinality) is allowed to
    /// materialise in memory. Default `10_000_000`
    /// (~160 MB at 16 B per `(ts, value)` pair).
    pub max_buffered_samples: usize,
    /// Wall-clock query timeout, in seconds. Default `30`.
    pub query_timeout_secs: u64,
}

impl Default for GorillaEngineConfig {
    fn default() -> Self {
        Self {
            max_buffered_samples: 10_000_000,
            query_timeout_secs: 30,
        }
    }
}

/// Error surface returned by [`GorillaQueryEngine::execute`].
#[derive(Debug, Error)]
pub enum EngineError {
    /// PromQL string failed to parse, or used a construct outside
    /// the archive query supported surface (see [`archive_query`]).
    #[error("query planning failed: {0}")]
    Plan(String),
    /// Archive-store fetch / decode failed.
    #[error("store error: {0}")]
    Store(#[from] store::StoreError),
    /// Buffered-aggregate budget exceeded — query asked for more
    /// samples than [`GorillaEngineConfig::max_buffered_samples`]
    /// will allow. The user should narrow the time range or
    /// lower the cardinality.
    #[error(
        "buffered-aggregate budget exceeded: {count} samples > limit {limit}; \
         narrow the time range or lower the metric cardinality"
    )]
    TooManySamples {
        /// Samples the engine attempted to materialise.
        count: usize,
        /// Configured ceiling.
        limit: usize,
    },
    /// Wall-clock timeout fired before the query finished.
    #[error("query timed out after {0:?}")]
    Timeout(Duration),
}

impl From<Elapsed> for EngineError {
    fn from(_: Elapsed) -> Self {
        Self::Timeout(Duration::from_secs(0))
    }
}

/// Archive-tier exact engine.
///
/// Holds an `Arc<dyn Store>` rather than a concrete
/// `Arc<GorillaS3Store>` so tests can inject in-memory mocks
/// and so future archive backends (Prometheus-block format via
/// the planned Step-2 Thanos store-gateway, multi-region fan-out)
/// drop in without changing the engine surface. The production
/// constructor [`GorillaQueryEngine::with_gorilla_s3`] keeps the
/// design.md type signature working at the call site.
pub struct GorillaQueryEngine {
    store: Arc<dyn store::Store>,
    config: GorillaEngineConfig,
}

impl GorillaQueryEngine {
    /// Build with an arbitrary `Store` implementation. Used by
    /// tests + the capability router (which may swap the
    /// concrete impl based on routing decisions).
    pub fn new(store: Arc<dyn store::Store>, config: GorillaEngineConfig) -> Self {
        Self { store, config }
    }

    /// Convenience constructor for the production
    /// [`store::GorillaS3Store`] path. Mirrors the design.md type
    /// signature.
    pub fn with_gorilla_s3(store: Arc<store::GorillaS3Store>, config: GorillaEngineConfig) -> Self {
        Self::new(store as Arc<dyn store::Store>, config)
    }

    /// Read-only access to the configured limits — useful for
    /// diagnostics + the cost-aware router's cost estimator.
    pub fn config(&self) -> &GorillaEngineConfig {
        &self.config
    }

    /// Test-only accessor for the underlying store. mvp/v5
    /// tests use this to construct a `ExactExecutor` that shares
    /// the same mock without re-wrapping in a fresh `Arc`.
    #[cfg(test)]
    pub(super) fn store_for_tests(&self) -> Arc<dyn store::Store> {
        self.store.clone()
    }

    /// Execute a parsed PromQL query against the archive tier.
    ///
    /// The query string is parsed via [`archive_query::plan_query`],
    /// the resulting plan dispatches to either the streaming
    /// additive or the buffered execution path, and the answer is
    /// wrapped with the exact-accuracy envelope + the
    /// `data_source: thanos_query` annotation.
    pub async fn execute(&self, query: &str) -> Result<QueryResult, EngineError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.execute_at(query, now_ms).await
    }

    /// Like [`Self::execute`], with a caller-supplied `now_ms`
    /// pinning the right edge of the request window. Used by
    /// tests + by callers that want to back-date a query against
    /// historical chunks.
    pub async fn execute_at(&self, query: &str, now_ms: i64) -> Result<QueryResult, EngineError> {
        let timeout = Duration::from_secs(self.config.query_timeout_secs.max(1));
        tokio::time::timeout(timeout, self.execute_inner(query, now_ms))
            .await
            .map_err(|_| EngineError::Timeout(timeout))?
    }

    async fn execute_inner(&self, query: &str, now_ms: i64) -> Result<QueryResult, EngineError> {
        let plan = archive_query::plan_query_at(query, now_ms).map_err(EngineError::Plan)?;
        debug!(
            metric = plan.metric.as_str(),
            stat = ?plan.statistic,
            start_ms = plan.time_range_ms.0,
            end_ms = plan.time_range_ms.1,
            "gorilla-engine: executing plan"
        );

        let executor = ExactExecutor::new(self.store.clone(), self.config.clone());
        let outcome = executor.execute_plan(&plan).await?;

        Ok(wrap_result(&plan, outcome))
    }
}

/// Wrap a finished `(scalar value, sample / chunk counts)` into a
/// `QueryResult` with the exact-accuracy envelope + the
/// `data_source: thanos_query` info line. Pulled out so tests
/// can pin the wrapping shape independently of the executor.
pub fn wrap_result(plan: &QueryPlan, outcome: ExecutionOutcome) -> QueryResult {
    // Result timestamp is the right edge of the requested range —
    // mirrors `ASAPQueryEngine`'s convention for instant-vector queries
    // against a closed time window.
    let result_ts = plan.time_range_ms.1.max(0) as u64;

    let labels = KeyByLabelValues::new_with_labels(Vec::new());
    let element = InstantVectorElement::new(labels, outcome.value);
    let envelope = AccuracyEnvelope::single(AccuracyProfile::exact());
    QueryResult::vector(vec![element], result_ts)
        .with_accuracy(envelope)
        // Window is the requested range, expressed in u64 ms.
        .with_window_used((
            plan.time_range_ms.0.max(0) as u64,
            plan.time_range_ms.1.max(0) as u64,
        ))
}

/// Output of an executed plan. Kept narrow on purpose — Phase 4's
/// MVP returns a single scalar per query. Higher-cardinality
/// (per-group) shapes will land in Phase 5+ once capability
/// routing decides which engine answers grouped queries.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionOutcome {
    /// Final scalar (e.g. `sum_over_time` total, `quantile_over_time`
    /// φ-quantile). NaN when the time range carries no samples.
    pub value: f64,
    /// Number of raw samples that contributed to `value`.
    pub samples_scanned: usize,
    /// Number of chunks the executor fetched from the cold store.
    pub chunks_fetched: usize,
    /// **mvp/v5**: number of chunks the postings filter pruned —
    /// the executor was able to skip these without a chunk-body
    /// fetch. `0` when the postings-aware path didn't run (no label
    /// matchers / postings missing).
    pub chunks_skipped_via_postings: usize,
    /// **mvp/v5**: number of series the postings file said matched
    /// the label predicates. The executor uses this to decide
    /// whether a chunk's `label_hash` is interesting before paying
    /// for the chunk body. Surfaces in `infos` as
    /// `postings_filtered_series_count`.
    pub postings_filtered_series_count: usize,
    /// **mvp/v5**: `true` when the engine hit a missing postings
    /// sidecar in the request window and fell back to the scan-all
    /// path. Drives the `data_source_quirk: postings_missing`
    /// `infos` annotation.
    pub postings_missing: bool,
}

impl ExecutionOutcome {
    /// "no data" sentinel — used when the time range is empty.
    pub fn empty() -> Self {
        Self {
            value: f64::NAN,
            samples_scanned: 0,
            chunks_fetched: 0,
            chunks_skipped_via_postings: 0,
            postings_filtered_series_count: 0,
            postings_missing: false,
        }
    }

    /// Build the `infos` array surfaced on the wire response.
    /// Pulled out so tests can pin the exact strings.
    pub fn info_lines(&self) -> Vec<String> {
        let mut out = vec![
            AccuracyProfile::exact().summary(),
            DATA_SOURCE_GORILLA_ARCHIVE.to_string(),
            format!("samples_scanned: {}", self.samples_scanned),
            format!("chunks_fetched: {}", self.chunks_fetched),
        ];
        // mvp/v5: surface postings-aware execution counters.
        out.push(format!(
            "chunks_skipped_via_postings: {}",
            self.chunks_skipped_via_postings
        ));
        out.push(format!(
            "postings_filtered_series_count: {}",
            self.postings_filtered_series_count
        ));
        if self.postings_missing {
            out.push("data_source_quirk: postings_missing".to_string());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// `QueryEngine` trait impl.
//
// Wraps `GorillaQueryEngine::execute` with the EngineError envelope the
// router speaks. Plan-time / parse-time failures fold into
// `EngineError::CapabilityMiss` (the engine cannot serve this query
// shape; router should fall through). Store / timeout / buffer-budget
// failures fold into `EngineError::Backend` (the engine could have served
// the query but its backend transiently failed; router should also fall
// through, typically to the warm-tier sketch path on `DoubleWrite`).
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl crate::query_engines::routing::query_engine_routing::QueryEngine for GorillaQueryEngine {
    async fn execute(&self, query: &str) -> Result<QueryResult, crate::query_engines::EngineError> {
        match GorillaQueryEngine::execute(self, query).await {
            Ok(result) => Ok(result),
            Err(EngineError::Plan(msg)) => Err(crate::query_engines::EngineError::capability_miss(
                asap_types::StorageBackend::GorillaObjectStore.data_source_id(),
                msg,
            )),
            Err(other) => Err(crate::query_engines::EngineError::backend(
                asap_types::StorageBackend::GorillaObjectStore.data_source_id(),
                other,
            )),
        }
    }

    fn capabilities(&self) -> crate::query_engines::routing::query_engine_routing::EngineCapabilities {
        crate::query_engines::routing::query_engine_routing::EngineCapabilities {
            data_source_id: asap_types::StorageBackend::GorillaObjectStore.data_source_id(),
            storage_backend: asap_types::StorageBackend::GorillaObjectStore,
            // The buffered-aggregate budget gives a natural ceiling: each
            // sample is ~16 B (i64 ts + f64 value), so the byte budget is
            // ~16 × max_buffered_samples.
            supports_streams_above_bytes: self.config.max_buffered_samples.saturating_mul(16),
        }
    }
}
