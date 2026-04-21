//! Cold raw-sample store used by the §5.2 cold-query fallback.
//!
//! In the paper architecture, the edge OTel collector dumps raw
//! observability data to a cheap cold tier (S3) in parallel with
//! the sketch path. When a query hits a capability-miss — most
//! notably a `TimelineCoverage::Purged` segment whose sketch was
//! aged out — the engine falls through to this store to recover
//! an exact answer from raw records.
//!
//! This module exposes a **storage-agnostic** `ColdStore` trait so
//! the same `s3_adapter` fallback can point at either a local
//! filesystem root (used today + for tests) or a real S3 bucket
//! (future swap, identical object key layout — see
//! [`format::part_path_prefix`]).
//!
//! # Format
//!
//! Raw samples live under a deterministic key tree:
//!
//! ```text
//! <root>/raw/<metric>/YYYY/MM/DD/HH/part-NNNNNN.jsonl
//! ```
//!
//! Each line is one sample encoded as JSON:
//!
//! ```json
//! {"ts_ms": 1713657600000, "labels": {"zone": "a"}, "value": 42.5}
//! ```
//!
//! See [`format`] for serialization and path helpers.

use async_trait::async_trait;
use std::collections::BTreeMap;
use thiserror::Error;

pub mod format;
pub mod local_fs;

pub use format::{part_path_prefix, RawSample};
pub use local_fs::LocalFsColdStore;

/// Error surface for cold-store scans.
#[derive(Debug, Error)]
pub enum ColdStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed raw record: {0}")]
    Malformed(String),
}

/// Read-only view over a cold raw-sample store.
///
/// Scans are `(metric, [start_ms, end_ms))` — inclusive start,
/// exclusive end — matching the half-open range convention used by
/// the rest of the engine. Implementations are expected to:
///
/// * prune by metric via the `<metric>/` key prefix,
/// * prune by hour via the `YYYY/MM/DD/HH/` key prefix,
/// * scan inside candidate parts and emit only samples whose
///   `ts_ms` falls in the requested range.
///
/// Label matching is **not** pushed down here — callers filter
/// samples client-side. This keeps the trait small and makes the
/// local-FS / S3 impls trivially swappable.
#[async_trait]
pub trait ColdStore: Send + Sync {
    /// Return all samples for `metric` whose timestamp lies in
    /// `[start_ms, end_ms)`. Ordering is not guaranteed.
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError>;
}

/// Convenience alias: a label set as stored in a [`RawSample`].
pub type LabelSet = BTreeMap<String, String>;
