use crate::storage_engines::types::{AggregateCore, KeyByLabelValues};
use std::collections::HashMap;
use std::sync::Arc;

/// A bucket with its timestamp range: ((start_timestamp, end_timestamp), aggregate)
pub type TimestampedBucket = ((u64, u64), Arc<dyn AggregateCore>);

/// Map from key to timestamped buckets (sparse - only contains buckets that exist)
pub type TimestampedBucketsMap = HashMap<Option<KeyByLabelValues>, Vec<TimestampedBucket>>;

/// Shared result type for fallible storage operations.
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
