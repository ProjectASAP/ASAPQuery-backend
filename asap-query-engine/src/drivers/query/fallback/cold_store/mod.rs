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
pub mod gorilla_s3;
pub mod local_fs;

pub use format::{part_path_prefix, RawSample};
pub use gorilla_s3::{GorillaS3ColdStore, GorillaS3Config, GorillaS3ConfigError};
pub use local_fs::LocalFsColdStore;

/// Error surface for cold-store scans.
#[derive(Debug, Error)]
pub enum ColdStoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed raw record: {0}")]
    Malformed(String),
    /// Backend-storage error (e.g. an S3 GET failed) that is not
    /// itself a `std::io::Error`. Phase 3 introduced this variant for
    /// the Gorilla-S3 cold store; the local-FS path keeps using
    /// [`ColdStoreError::Io`].
    #[error("backend error: {0}")]
    Backend(String),
    /// A trait method that this `ColdStore` impl does not support.
    /// Returned by the default `list_chunks` / `read_chunk` impls on
    /// JSONL-only stores; Gorilla-S3 / future chunk-native stores
    /// override.
    #[error("unsupported cold-store operation: {0}")]
    Unsupported(&'static str),
}

/// Descriptor for a single immutable cold-store chunk.
///
/// Returned by [`ColdStore::list_chunks`] for chunk-native backends
/// (Phase 3+ Gorilla-S3). Carries enough metadata for callers to
/// prune by time / label without reading the chunk body.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkRef {
    /// Opaque object key (e.g. an S3 key). The Telegraf-side
    /// `gorilla_s3` output uses
    /// `<prefix>/block-<unix>-<idx>-<rand>.gorilla`; the
    /// design.md-style layout is `<tenant>/<metric>/YYYY/MM/DD/HH/
    /// part-NNNNNN.gor`. Either is fine — the index file is the
    /// source of truth for what keys exist.
    pub key: String,
    /// Metric name the chunk was fetched against. Recovered from
    /// the caller's `list_chunks` request rather than the on-wire
    /// chunk metadata, since not all backends require chunks to be
    /// metric-pure.
    pub metric: String,
    /// `(start_unix_ms, end_unix_ms)` covered by the chunk —
    /// converted from the on-wire nanosecond range so it can be
    /// directly compared with [`ColdStore::scan`]'s
    /// `[start_ms, end_ms)` window.
    pub time_range_ms: (i64, i64),
    /// 64-bit canonical-label-set hash — for prune-by-label-equality
    /// without fetching the chunk.
    pub label_hash: u64,
    /// Number of samples in the chunk.
    pub sample_count: u32,
    /// On-wire size of the chunk object in bytes.
    pub size_bytes: u32,
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
///
/// # Phase-3 trait extension
///
/// The `list_chunks` / `read_chunk` pair is additive (default impls
/// return [`ColdStoreError::Unsupported`]) so the existing JSONL
/// `LocalFsColdStore` keeps compiling unchanged. Chunk-native
/// backends (Gorilla-S3) override both so the upcoming
/// `GorillaQueryEngine` can iterate chunks one at a time without
/// materialising every sample up front. See
/// [`docs/design-gorilla-s3-cold-engine.md` §7.2](#) for the
/// rationale.
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

    /// List chunk descriptors covering `[start_ms, end_ms)` without
    /// decoding any bodies. Default impl returns
    /// [`ColdStoreError::Unsupported`] — only chunk-native backends
    /// (e.g. [`GorillaS3ColdStore`]) override.
    async fn list_chunks(
        &self,
        _metric: &str,
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<ChunkRef>, ColdStoreError> {
        Err(ColdStoreError::Unsupported("list_chunks"))
    }

    /// Decode a single chunk into an owned `Vec<RawSample>`.
    ///
    /// Returning `Vec` rather than a streaming iterator keeps the
    /// trait object-safe and matches the existing `scan` contract;
    /// chunks are bounded-size in practice (Phase 1 emits one series
    /// per ~1 hour). The decoded samples can also be cached cheaply
    /// by the impl. Default returns [`ColdStoreError::Unsupported`].
    async fn read_chunk(
        &self,
        _chunk: &ChunkRef,
    ) -> Result<Vec<RawSample>, ColdStoreError> {
        Err(ColdStoreError::Unsupported("read_chunk"))
    }
}

/// Convenience alias: a label set as stored in a [`RawSample`].
pub type LabelSet = BTreeMap<String, String>;
