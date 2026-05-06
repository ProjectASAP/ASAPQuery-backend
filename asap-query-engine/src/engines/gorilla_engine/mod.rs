//! Phase 4: `GorillaQueryEngine` — exact PromQL execution over the
//! Gorilla-S3 cold tier.
//!
//! This engine is a SIBLING of [`crate::engines::simple_engine::SimpleEngine`].
//! Both consume the same PromQL surface, but where `SimpleEngine`
//! answers from warm-tier sketches (approximate, ε/δ-bounded), the
//! `GorillaQueryEngine` answers exactly from per-hour Gorilla
//! chunks landed on S3 / MinIO via the Phase-3
//! [`crate::drivers::query::fallback::cold_store::gorilla_s3::GorillaS3ColdStore`].
//!
//! Result wrapping pins three things:
//!
//! 1. an [`crate::stores::sketch_db::AccuracyEnvelope`] with
//!    `kind = Exact`, ε = 0, δ = 0,
//! 2. a `data_source: gorilla_archive` info line,
//! 3. cheap diagnostics (`samples_scanned`, `chunks_fetched`).
//!
//! See `docs/design-gorilla-s3-cold-engine.md` §6.
//!
//! ## Two execution strategies
//!
//! Per-statistic dispatch in [`exact_executor`]:
//!
//! * **Streaming-additive** — `Sum`, `Count`, `Min`, `Max`, `Rate`,
//!   `Increase` (and `Avg` derived as Sum/Count). One chunk at a
//!   time, fold into a small accumulator, drop the decoded samples
//!   before fetching the next chunk. Memory cost is O(1) per group.
//! * **Buffered** — `Quantile`, `TopK`, `Cardinality`. Materialise
//!   every in-range sample, then sort / count. Bounded by
//!   [`GorillaEngineConfig::max_buffered_samples`]; over-budget
//!   queries fail fast with [`EngineError::TooManySamples`].

pub mod exact_executor;
pub mod query_planner;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::time::error::Elapsed;
use tracing::debug;

use crate::data_model::KeyByLabelValues;
use crate::drivers::query::fallback::cold_store::{ColdStore, ColdStoreError};
use crate::engines::query_result::{InstantVectorElement, QueryResult};
use crate::stores::sketch_db::accuracy::{AccuracyEnvelope, AccuracyProfile};

pub use exact_executor::{AdditiveOp, ExactExecutor};
pub use query_planner::{plan_query, plan_query_at, QueryPlan, QueryStatistic};

/// Marker line that every `GorillaQueryEngine` answer carries on
/// its `infos` array. Pinned so dashboards / Phase-5 capability
/// routers can byte-compare without parsing.
pub const DATA_SOURCE_GORILLA_ARCHIVE: &str = "data_source: gorilla_archive";

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
    /// the engine's supported surface (see [`query_planner`]).
    #[error("query planning failed: {0}")]
    Plan(String),
    /// Cold-store fetch / decode failed.
    #[error("cold-store error: {0}")]
    ColdStore(#[from] ColdStoreError),
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

/// Phase-4 cold-tier exact engine.
///
/// Holds an `Arc<dyn ColdStore>` rather than a concrete
/// `Arc<GorillaS3ColdStore>` so tests can inject in-memory mocks
/// and so future cold backends (local-FS chunks, multi-region
/// fan-out) drop in without changing the engine surface. The
/// production constructor [`GorillaQueryEngine::with_gorilla_s3`]
/// keeps the design.md type signature working at the call site.
pub struct GorillaQueryEngine {
    cold_store: Arc<dyn ColdStore>,
    config: GorillaEngineConfig,
}

impl GorillaQueryEngine {
    /// Build with an arbitrary cold-store implementation. Used by
    /// tests + the Phase-5 capability router (which may swap the
    /// concrete impl based on routing decisions).
    pub fn new(cold_store: Arc<dyn ColdStore>, config: GorillaEngineConfig) -> Self {
        Self {
            cold_store,
            config,
        }
    }

    /// Convenience constructor for the production
    /// [`crate::drivers::query::fallback::cold_store::gorilla_s3::GorillaS3ColdStore`]
    /// path. Mirrors the design.md type signature.
    pub fn with_gorilla_s3(
        cold_store: Arc<crate::drivers::query::fallback::cold_store::GorillaS3ColdStore>,
        config: GorillaEngineConfig,
    ) -> Self {
        Self::new(cold_store as Arc<dyn ColdStore>, config)
    }

    /// Read-only access to the configured limits — useful for
    /// diagnostics + the Phase-5 router's cost estimator.
    pub fn config(&self) -> &GorillaEngineConfig {
        &self.config
    }

    /// Execute a parsed PromQL query against the cold tier.
    ///
    /// The query string is parsed via [`query_planner::plan_query`],
    /// the resulting plan dispatches to either the streaming
    /// additive or the buffered execution path, and the answer is
    /// wrapped with the exact-accuracy envelope + the
    /// `data_source: gorilla_archive` annotation.
    pub async fn execute(&self, query: &str) -> Result<QueryResult, EngineError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.execute_at(query, now_ms).await
    }

    /// Like [`Self::execute`], with a caller-supplied `now_ms`
    /// pinning the right edge of the request window. Used by
    /// tests + by the (future) Phase-5 router that wants to back-
    /// date a query against historical chunks.
    pub async fn execute_at(
        &self,
        query: &str,
        now_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let timeout = Duration::from_secs(self.config.query_timeout_secs.max(1));
        tokio::time::timeout(timeout, self.execute_inner(query, now_ms))
            .await
            .map_err(|_| EngineError::Timeout(timeout))?
    }

    async fn execute_inner(
        &self,
        query: &str,
        now_ms: i64,
    ) -> Result<QueryResult, EngineError> {
        let plan = query_planner::plan_query_at(query, now_ms).map_err(EngineError::Plan)?;
        debug!(
            metric = plan.metric.as_str(),
            stat = ?plan.statistic,
            start_ms = plan.time_range_ms.0,
            end_ms = plan.time_range_ms.1,
            "gorilla-engine: executing plan"
        );

        let executor = ExactExecutor::new(self.cold_store.clone(), self.config.clone());
        let outcome = executor.execute_plan(&plan).await?;

        Ok(wrap_result(&plan, outcome))
    }
}

/// Wrap a finished `(scalar value, sample / chunk counts)` into a
/// `QueryResult` with the exact-accuracy envelope + the
/// `data_source: gorilla_archive` info line. Pulled out so tests
/// can pin the wrapping shape independently of the executor.
pub fn wrap_result(plan: &QueryPlan, outcome: ExecutionOutcome) -> QueryResult {
    // Result timestamp is the right edge of the requested range —
    // mirrors `SimpleEngine`'s convention for instant-vector queries
    // against a closed time window.
    let result_ts = plan.time_range_ms.1.max(0) as u64;

    let labels = KeyByLabelValues::new_with_labels(Vec::new());
    let element = InstantVectorElement::new(labels, outcome.value);
    let envelope = AccuracyEnvelope::single(AccuracyProfile::exact());
    QueryResult::vector(vec![element], result_ts).with_accuracy(envelope)
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
}

impl ExecutionOutcome {
    /// "no data" sentinel — used when the time range is empty.
    pub fn empty() -> Self {
        Self {
            value: f64::NAN,
            samples_scanned: 0,
            chunks_fetched: 0,
        }
    }

    /// Build the `infos` array surfaced on the wire response.
    /// Pulled out so tests can pin the exact strings.
    pub fn info_lines(&self) -> Vec<String> {
        vec![
            AccuracyProfile::exact().summary(),
            DATA_SOURCE_GORILLA_ARCHIVE.to_string(),
            format!("samples_scanned: {}", self.samples_scanned),
            format!("chunks_fetched: {}", self.chunks_fetched),
        ]
    }
}
