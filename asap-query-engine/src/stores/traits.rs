use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use std::collections::HashMap;
use std::sync::Arc;

/// A bucket with its timestamp range: ((start_timestamp, end_timestamp), aggregate)
pub type TimestampedBucket = ((u64, u64), Arc<dyn AggregateCore>);

/// Map from key to timestamped buckets (sparse - only contains buckets that exist)
pub type TimestampedBucketsMap = HashMap<Option<KeyByLabelValues>, Vec<TimestampedBucket>>;

/// Trait defining the interface for precomputed data storage backends
// #[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Insert a single precomputed output
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Insert multiple precomputed outputs in a batch (for Kafka consumer)
    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Query precomputed outputs for a given metric and time range.
    /// Returns timestamped buckets sorted by timestamp.
    /// Results are sparse - only contains buckets that actually exist.
    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>>;

    /// Query precomputed outputs for exact timestamp match (Issue #236 - Sliding Windows)
    ///
    /// For sliding windows, we need to find a precompute with EXACTLY matching start and end timestamps.
    /// This is used to retrieve a single sliding window aggregate without merging.
    ///
    /// Returns precomputes only if an exact match is found for the timestamp range [exact_start, exact_end].
    /// Returns empty HashMap if no exact match exists (strict matching, no tolerance).
    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>>;

    /// Get earliest timestamp for each aggregation ID (for monitoring)
    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>>;

    /// Close the store and clean up resources
    fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Drop every precompute record whose `aggregation_id` equals
    /// `agg_id`, regardless of window or subpopulation key. Used by
    /// the §6-driven `SchemaEvictionService` to reclaim space when
    /// a retired schema passes its `expires_at_ms`.
    ///
    /// Returns the number of records removed (best-effort — a store
    /// that can't easily count still returns 0 and logs, never
    /// fails).
    ///
    /// Contract:
    /// * Must be idempotent — calling on an unknown `agg_id` is a
    ///   no-op that returns `Ok(0)`.
    /// * Must be atomic AT LEAST with respect to concurrent reads
    ///   for the same agg_id: a reader either sees all of the
    ///   pre-drop records or none, not a half-dropped state. Stores
    ///   backed by a single RwLock get this for free; stores with
    ///   finer-grained locking may need to grab a global lock
    ///   briefly.
    /// * Does NOT touch any registry (backfill, schema); the caller
    ///   is responsible for post-drop cleanup of those.
    ///
    /// Default impl returns `Ok(0)` so existing stores stay
    /// compiling — the impl must override to actually delete.
    fn drop_agg_id(&self, _agg_id: u64) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        Ok(0)
    }
}

/// Result type for store operations
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
