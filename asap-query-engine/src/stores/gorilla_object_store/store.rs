//! Gorilla-on-S3 archive store — the Phase-4 [`GorillaQueryEngine`](super::GorillaQueryEngine)'s
//! sole storage backend.
//!
//! Lists per-hour `index.json` catalogs out of an S3-compatible
//! bucket, prunes them by time range, then fetches + decodes the
//! selected `GORILLA1` chunks via the [`asap_gorilla`] crate
//! (`ASAPCollector` PR #281).
//!
//! Step-1 refactor (`refactor: tier-co-locate engines/{simple,gorilla}/`)
//! folded the previous `ColdStore` trait + `RawSample`/`ChunkRef`
//! types into this module. The legacy JSONL leg
//! (`LocalFsColdStore`, `parse_jsonl`, `ColdJsonlFallback`) was
//! deleted at the same commit; this is now the only `Store` impl
//! in the archive tier.
//!
//! # Object key layout
//!
//! `GorillaS3Store` is **agnostic** about the on-S3 chunk-key
//! shape. Two layouts are known to coexist (see PR #281):
//!
//! * design.md canonical:
//!   `<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor`
//! * Telegraf-side `gorilla_s3` output:
//!   `<prefix>/block-<unix>-<idx>-<rand>.gorilla` (random suffix)
//!
//! The per-hour `index.json` is the source of truth for what keys
//! exist; we treat [`asap_gorilla::IndexEntry::key`] as opaque and
//! do not try to parse it. The `prefix_template` config field
//! controls only where the **index** files live, not the chunks.
//!
//! # S3 client
//!
//! Backed by the `rust-s3` crate (`s3 = "0.37"`) — single-crate
//! dep, MinIO-friendly out of the box (no AWS-specific signing
//! quirks, supports custom endpoint URLs + path-style addressing).
//! Hidden behind the [`ObjectStore`] trait below so tests use an
//! in-memory mock and do not need a live MinIO.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use lru::LruCache;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

use asap_gorilla::{GorillaDecoder, IndexFile};

use super::postings::{intersect_per_bucket_postings, PostingsCache, PostingsHits};
use super::s3_cost::{global_s3_cost_counters, S3CostTrackingObjectStore};

// ─────────────────────────────────────────────────────────────────────
// Public types — merged in from the deleted `cold_store/mod.rs`
// ─────────────────────────────────────────────────────────────────────

/// A single raw observability sample as decoded out of a
/// `GORILLA1` chunk. `labels` is a `BTreeMap` so identical samples
/// hash deterministically (handy for golden tests + the postings
/// cross-check).
///
/// Pre-Step-1 this lived in the JSONL `cold_store::format` module
/// and was the wire format the legacy `LocalFsColdStore` parsed.
/// JSONL is gone; the type stays as the in-memory shape every
/// gorilla-engine consumer (`exact_executor`, the postings filter,
/// the test mocks) speaks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawSample {
    pub ts_ms: i64,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Convenience alias: a label set as stored on a [`RawSample`].
pub type LabelSet = BTreeMap<String, String>;

/// Error surface for archive-store operations.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed record: {0}")]
    Malformed(String),
    /// Backend-storage error (e.g. an S3 GET failed) that is not
    /// itself a `std::io::Error`.
    #[error("backend error: {0}")]
    Backend(String),
    /// A trait method that this `Store` impl does not support.
    /// Reserved for forwards-compatible trait extensions.
    #[error("unsupported store operation: {0}")]
    Unsupported(&'static str),
}

/// Descriptor for a single immutable chunk stored in the archive
/// tier. Returned by [`Store::list_chunks`]; carries enough
/// metadata for callers to prune by time / label without reading
/// the chunk body.
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
    /// chunk metadata.
    pub metric: String,
    /// `(start_unix_ms, end_unix_ms)` covered by the chunk.
    pub time_range_ms: (i64, i64),
    /// 64-bit canonical-label-set hash — for prune-by-label-equality
    /// without fetching the chunk.
    pub label_hash: u64,
    /// Number of samples in the chunk.
    pub sample_count: u32,
    /// On-wire size of the chunk object in bytes.
    pub size_bytes: u32,
}

/// Read-only view over the Gorilla archive tier.
///
/// Trait-shaped (rather than collapsed onto `GorillaS3Store`
/// concretely) so tests can drop in an in-memory mock without
/// touching production S3 wiring. Step-2 of the JSONL deprecation
/// (Prometheus-block format + Thanos store-gateway) will plug a
/// second impl in under the same trait.
///
/// Scans are `(metric, [start_ms, end_ms))` — inclusive start,
/// exclusive end — matching the half-open range convention used by
/// the rest of the engine.
#[async_trait]
pub trait Store: Send + Sync {
    /// Return all samples for `metric` whose timestamp lies in
    /// `[start_ms, end_ms)`. Ordering is not guaranteed.
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, StoreError>;

    /// List chunk descriptors covering `[start_ms, end_ms)` without
    /// decoding any bodies.
    async fn list_chunks(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ChunkRef>, StoreError>;

    /// Decode a single chunk into an owned `Vec<RawSample>`.
    async fn read_chunk(&self, chunk: &ChunkRef) -> Result<Vec<RawSample>, StoreError>;

    /// Load + intersect per-bucket postings under `(metric,
    /// time_range)` for the supplied `(label_name, label_value)`
    /// matchers. Default impl returns
    /// [`StoreError::Unsupported`] so chunk-only stores keep
    /// compiling without postings sidecars.
    async fn list_postings_for(
        &self,
        _metric: &str,
        _start_ms: i64,
        _end_ms: i64,
        _matchers: &[(String, String)],
    ) -> Result<PostingsHits, StoreError> {
        Err(StoreError::Unsupported("list_postings_for"))
    }
}

// ─────────────────────────────────────────────────────────────────────
// Public config
// ─────────────────────────────────────────────────────────────────────

/// Tunable configuration for [`GorillaS3Store`].
///
/// Use [`GorillaS3Config::from_env`] to pull values from environment
/// variables in deployment, or build manually for tests.
#[derive(Debug, Clone)]
pub struct GorillaS3Config {
    /// `None` for AWS S3 (the SDK uses the standard regional
    /// endpoint), `Some("http://minio:9000")` for MinIO / a custom
    /// S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Bucket name to list / read from.
    pub bucket: String,
    /// Tenant identifier prepended to every index-file prefix.
    /// Empty string is allowed for single-tenant deployments.
    pub tenant: String,
    /// Prefix template for per-hour `index.json` files. Supports
    /// the placeholders `{tenant}`, `{metric}`, `{year}`, `{month}`,
    /// `{day}`, `{hour}` (zero-padded). Default:
    /// `"{tenant}/{metric}/{year}/{month}/{day}/{hour}/"`.
    pub prefix_template: String,
    /// AWS-region the bucket lives in (e.g. `"us-east-1"`). For
    /// MinIO any non-empty placeholder works.
    pub region: String,
    /// Optional static credential override. Both fields must be set
    /// together; if either is `None` the underlying SDK falls back
    /// to its environment / IMDS resolution.
    pub access_key_id: Option<String>,
    /// See [`Self::access_key_id`].
    pub secret_access_key: Option<String>,
    /// LRU capacity (in number of decoded chunks). Default `256`.
    pub cache_capacity: usize,
    /// `false` switches the SDK to plain HTTP — required for local
    /// MinIO / docker-compose smoke tests. Default `true`.
    pub use_ssl: bool,
}

impl Default for GorillaS3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            bucket: String::new(),
            tenant: String::new(),
            prefix_template: "{tenant}/{metric}/{year}/{month}/{day}/{hour}/".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
            cache_capacity: 256,
            use_ssl: true,
        }
    }
}

/// Errors raised by [`GorillaS3Config::from_env`].
#[derive(Debug, Error)]
pub enum GorillaS3ConfigError {
    /// A required environment variable was missing.
    #[error("missing required env var: {0}")]
    MissingEnv(&'static str),
    /// `ASAP_GORILLA_S3_CACHE_CAPACITY` could not be parsed as a
    /// positive `usize`.
    #[error("invalid env var {var}: {value} ({source})")]
    InvalidEnv {
        /// Variable name.
        var: &'static str,
        /// Raw value the user supplied.
        value: String,
        /// Underlying parse error.
        source: std::num::ParseIntError,
    },
}

impl GorillaS3Config {
    /// Read a config from process environment variables. Required:
    ///
    /// * `ASAP_GORILLA_S3_BUCKET`
    /// * `ASAP_GORILLA_S3_REGION`
    ///
    /// Optional (with defaults shown above):
    ///
    /// * `ASAP_GORILLA_S3_ENDPOINT`
    /// * `ASAP_GORILLA_S3_TENANT`
    /// * `ASAP_GORILLA_S3_PREFIX_TEMPLATE`
    /// * `ASAP_GORILLA_S3_ACCESS_KEY_ID` / `..._SECRET_ACCESS_KEY`
    /// * `ASAP_GORILLA_S3_CACHE_CAPACITY`
    /// * `ASAP_GORILLA_S3_USE_SSL` (`"true"` / `"false"`,
    ///   case-insensitive)
    pub fn from_env() -> Result<Self, GorillaS3ConfigError> {
        let bucket = std::env::var("ASAP_GORILLA_S3_BUCKET")
            .map_err(|_| GorillaS3ConfigError::MissingEnv("ASAP_GORILLA_S3_BUCKET"))?;
        let region = std::env::var("ASAP_GORILLA_S3_REGION")
            .map_err(|_| GorillaS3ConfigError::MissingEnv("ASAP_GORILLA_S3_REGION"))?;
        let endpoint = std::env::var("ASAP_GORILLA_S3_ENDPOINT").ok();
        let tenant = std::env::var("ASAP_GORILLA_S3_TENANT").unwrap_or_default();
        let prefix_template = std::env::var("ASAP_GORILLA_S3_PREFIX_TEMPLATE")
            .unwrap_or_else(|_| "{tenant}/{metric}/{year}/{month}/{day}/{hour}/".to_string());
        let access_key_id = std::env::var("ASAP_GORILLA_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = std::env::var("ASAP_GORILLA_S3_SECRET_ACCESS_KEY").ok();
        let cache_capacity = match std::env::var("ASAP_GORILLA_S3_CACHE_CAPACITY") {
            Ok(s) => s
                .parse::<usize>()
                .map_err(|e| GorillaS3ConfigError::InvalidEnv {
                    var: "ASAP_GORILLA_S3_CACHE_CAPACITY",
                    value: s,
                    source: e,
                })?,
            Err(_) => 256,
        };
        let use_ssl = std::env::var("ASAP_GORILLA_S3_USE_SSL")
            .map(|s| !matches!(s.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(true);
        Ok(Self {
            endpoint,
            bucket,
            tenant,
            prefix_template,
            region,
            access_key_id,
            secret_access_key,
            cache_capacity,
            use_ssl,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────
// ObjectStore — internal trait so tests don't need real S3
// ─────────────────────────────────────────────────────────────────────

/// Minimal async object-fetch interface.
///
/// Sized + `Send + Sync` so [`GorillaS3Store`] can hold one
/// behind an `Arc<dyn ObjectStore>` regardless of how it's backed.
/// Production callers use [`S3ObjectStore`] (rust-s3); tests use the
/// in-memory mock at the bottom of this file.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch the full object body for `key`.
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError>;

    /// True iff `err` was raised because the requested key did not
    /// exist (vs. a transport / permission failure). Used by the
    /// list path to treat a missing `index.json` as "no chunks for
    /// this hour" rather than a hard error.
    fn object_missing(&self, err: &StoreError) -> bool {
        matches!(err, StoreError::Backend(msg) if msg.contains("not found"))
    }
}

// ─────────────────────────────────────────────────────────────────────
// rust-s3 backed production ObjectStore
// ─────────────────────────────────────────────────────────────────────

mod rust_s3_backend {
    use super::*;
    use s3::creds::Credentials;
    use s3::region::Region as S3Region;
    use s3::Bucket;

    /// `rust-s3`-backed [`ObjectStore`]. Default production choice.
    pub struct S3ObjectStore {
        bucket: Box<Bucket>,
    }

    impl S3ObjectStore {
        /// Build from a [`GorillaS3Config`]. Sets
        /// `path_style = true` whenever a custom endpoint is
        /// configured (MinIO mandates path-style addressing).
        pub fn new(cfg: &GorillaS3Config) -> Result<Self, StoreError> {
            let region = match &cfg.endpoint {
                Some(ep) => {
                    let endpoint = if ep.starts_with("http://") || ep.starts_with("https://") {
                        ep.clone()
                    } else if cfg.use_ssl {
                        format!("https://{}", ep)
                    } else {
                        format!("http://{}", ep)
                    };
                    S3Region::Custom {
                        region: cfg.region.clone(),
                        endpoint,
                    }
                }
                None => cfg
                    .region
                    .parse::<S3Region>()
                    .map_err(|e| StoreError::Backend(format!("region parse: {e}")))?,
            };
            let creds = match (&cfg.access_key_id, &cfg.secret_access_key) {
                (Some(ak), Some(sk)) => Credentials::new(Some(ak), Some(sk), None, None, None)
                    .map_err(|e| StoreError::Backend(format!("credentials: {e}")))?,
                _ => Credentials::default()
                    .map_err(|e| StoreError::Backend(format!("default credentials: {e}")))?,
            };
            let bucket = Bucket::new(&cfg.bucket, region, creds)
                .map_err(|e| StoreError::Backend(format!("bucket: {e}")))?;
            // MinIO + most S3-compatibles require path-style addressing
            // when a custom endpoint is in play. AWS S3 supports both,
            // so leaving it on for the AWS path is safe but slightly
            // less efficient — only flip when an endpoint is set.
            let bucket = if cfg.endpoint.is_some() {
                bucket.with_path_style()
            } else {
                bucket
            };
            Ok(Self { bucket })
        }
    }

    #[async_trait]
    impl ObjectStore for S3ObjectStore {
        async fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
            let resp = self
                .bucket
                .get_object(key)
                .await
                .map_err(|e| StoreError::Backend(format!("s3 get {key}: {e}")))?;
            if resp.status_code() == 404 {
                return Err(StoreError::Backend(format!("s3 get {key}: not found")));
            }
            if !(200..300).contains(&resp.status_code()) {
                return Err(StoreError::Backend(format!(
                    "s3 get {key}: status {}",
                    resp.status_code()
                )));
            }
            Ok(resp.to_vec())
        }
    }
}

pub use rust_s3_backend::S3ObjectStore;

// ─────────────────────────────────────────────────────────────────────
// GorillaS3Store
// ─────────────────────────────────────────────────────────────────────

/// LRU cache keyed by chunk object key. Stored values are
/// pre-decoded `RawSample` lists so repeated reads of the same
/// chunk skip the Gorilla decode pass entirely.
type ChunkCache = Mutex<LruCache<String, Arc<Vec<RawSample>>>>;

/// **mvp/v5**: LRU cache for parsed `index.json` files (per
/// `(metric, hour)`). Same capacity tier as the postings cache.
type IndexCache = Mutex<LruCache<String, Arc<IndexFile>>>;

/// `Store` adapter that reads `GORILLA1`-format chunks out of an
/// S3-compatible bucket. See module docs for layout + S3 client
/// notes.
///
/// Step-1 rename (`GorillaS3ColdStore` → `GorillaS3Store`) reflects
/// the JSONL deprecation: there is no longer a "warm/cold" split
/// inside the archive tier; this is *the* archive store.
pub struct GorillaS3Store {
    object_store: Arc<dyn ObjectStore>,
    config: GorillaS3Config,
    cache: ChunkCache,
    /// **mvp/v5**: postings sidecar cache.
    postings_cache: PostingsCache,
    /// **mvp/v5**: index.json cache. Reserved for the upcoming
    /// `Range:`-based partial-read path that fetches chunk bytes
    /// out of compactor-merged objects — the cache is wired now to
    /// match the backend's hot-path layout but the caller doesn't
    /// yet route partial reads through it. The current `read_chunk`
    /// already uses an LRU on samples, which is the dominant cost.
    #[allow(dead_code)]
    index_cache: IndexCache,
}

impl GorillaS3Store {
    /// Build with an explicit object-store backend. The production
    /// constructor [`Self::with_default_backend`] wires up
    /// `S3ObjectStore` from `cfg`; tests inject the in-memory mock.
    pub fn new(object_store: Arc<dyn ObjectStore>, config: GorillaS3Config) -> Self {
        let cap = NonZeroUsize::new(config.cache_capacity.max(1))
            .unwrap_or(NonZeroUsize::new(1).unwrap());
        // mvp/v5: postings + index caches scale with the chunk
        // cache (one entry per hour-bucket, mirrors typical query
        // cardinality).
        let pc_cap = NonZeroUsize::new(cap.get().max(64)).unwrap_or(NonZeroUsize::new(64).unwrap());
        Self {
            object_store,
            config,
            cache: Mutex::new(LruCache::new(cap)),
            postings_cache: Mutex::new(LruCache::new(pc_cap)),
            index_cache: Mutex::new(LruCache::new(pc_cap)),
        }
    }

    /// Build from a [`GorillaS3Config`] using the default
    /// `rust-s3`-backed [`ObjectStore`].
    ///
    /// **mvp/v5**: the underlying `S3ObjectStore` is wrapped in an
    /// [`S3CostTrackingObjectStore`] tied to the global counter
    /// set, so the HTTP server's `/internal/s3_cost.csv` +
    /// `/metrics` endpoints report measured PUT/GET/etc counts.
    pub fn with_default_backend(config: GorillaS3Config) -> Result<Self, StoreError> {
        let backend: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(&config)?);
        let counters = global_s3_cost_counters();
        let tracked = S3CostTrackingObjectStore::new(backend, counters);
        Ok(Self::new(Arc::new(tracked), config))
    }

    /// Borrow the active config — useful for diagnostics.
    pub fn config(&self) -> &GorillaS3Config {
        &self.config
    }

    /// Render the configured `prefix_template` for one
    /// `(metric, hour)` bucket and append `index.json`.
    fn index_key(&self, metric: &str, ts_ms: i64) -> String {
        let mut key = self.bucket_prefix(metric, ts_ms);
        key.push_str("index.json");
        key
    }

    /// **mvp/v5**: derive the postings-v1.json key for the same
    /// `(metric, hour)` bucket as [`Self::index_key`].
    fn postings_key(&self, metric: &str, ts_ms: i64) -> String {
        let mut key = self.bucket_prefix(metric, ts_ms);
        key.push_str("postings-v1.json");
        key
    }

    /// Shared prefix-rendering helper used by [`Self::index_key`] /
    /// [`Self::postings_key`]. Always ends with `/`.
    ///
    /// Accepts BOTH placeholder vocabularies:
    ///
    /// * `{year}`/`{month}`/`{day}`/`{hour}` — the backend's
    ///   long-standing names.
    /// * `{YYYY}`/`{MM}`/`{DD}`/`{HH}` — the agent
    ///   `gorillas3processor`'s naming.
    fn bucket_prefix(&self, metric: &str, ts_ms: i64) -> String {
        let dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp_millis(ts_ms)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        let year = format!("{:04}", dt.year());
        let month = format!("{:02}", dt.month());
        let day = format!("{:02}", dt.day());
        let hour = format!("{:02}", dt.hour());
        let prefix = self
            .config
            .prefix_template
            .replace("{tenant}", &self.config.tenant)
            .replace("{metric}", metric)
            .replace("{year}", &year)
            .replace("{month}", &month)
            .replace("{day}", &day)
            .replace("{hour}", &hour)
            .replace("{YYYY}", &year)
            .replace("{MM}", &month)
            .replace("{DD}", &day)
            .replace("{HH}", &hour);
        let mut key = prefix;
        if !key.ends_with('/') {
            key.push('/');
        }
        key
    }

    /// Iterate the wall-clock-hour starts (in ms) covered by
    /// `[start_ms, end_ms)`. Always emits at least one bucket.
    fn hour_starts(start_ms: i64, end_ms: i64) -> Vec<i64> {
        const HOUR_MS: i64 = 3_600_000;
        if end_ms <= start_ms {
            let h = (start_ms / HOUR_MS) * HOUR_MS;
            return vec![h];
        }
        let first = (start_ms / HOUR_MS) * HOUR_MS;
        let last = ((end_ms - 1) / HOUR_MS) * HOUR_MS;
        let mut out = Vec::new();
        let mut cur = first;
        while cur <= last {
            out.push(cur);
            cur += HOUR_MS;
        }
        out
    }

    /// Fetch + parse one hour's `index.json`. Missing index = empty
    /// catalog (the producer may not have flushed yet); transport
    /// failure surfaces as `StoreError::Backend`.
    async fn fetch_index(&self, metric: &str, hour_ms: i64) -> Result<IndexFile, StoreError> {
        let key = self.index_key(metric, hour_ms);
        match self.object_store.get_object(&key).await {
            Ok(bytes) => IndexFile::read(bytes.as_slice())
                .map_err(|e| StoreError::Malformed(format!("index.json at {key}: {e}"))),
            Err(e) if self.object_store.object_missing(&e) => {
                debug!(key = %key, "gorilla-s3: index.json missing for hour bucket; skipping");
                Ok(IndexFile::new(0))
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl Store for GorillaS3Store {
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, StoreError> {
        let chunks = self.list_chunks(metric, start_ms, end_ms).await?;
        let mut out = Vec::new();
        for chunk in chunks {
            let samples = self.read_chunk(&chunk).await?;
            for s in samples {
                if s.ts_ms >= start_ms && s.ts_ms < end_ms {
                    out.push(s);
                }
            }
        }
        Ok(out)
    }

    async fn list_chunks(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ChunkRef>, StoreError> {
        // Convert the request window to the nanosecond unit the
        // index file uses (`IndexEntry.time_range` is `(ns, ns)`,
        // mirroring the Go encoder's `time.Time.UnixNano()` source).
        let start_ns = (start_ms as i128).saturating_mul(1_000_000) as u64;
        let end_ns = if end_ms <= start_ms {
            start_ns
        } else {
            ((end_ms as i128).saturating_mul(1_000_000) - 1).max(0) as u64
        };

        let mut out = Vec::new();
        for hour_ms in Self::hour_starts(start_ms, end_ms) {
            let idx = self.fetch_index(metric, hour_ms).await?;
            let bucket_prefix = self.bucket_prefix(metric, hour_ms);
            for entry in idx.prune_by_time((start_ns, end_ns)) {
                let (entry_start_ms, entry_end_ms) = (
                    (entry.time_range.0 / 1_000_000) as i64,
                    (entry.time_range.1 / 1_000_000) as i64,
                );
                // v7: agent-produced index entries carry just the
                // chunk's basename (`part-NNNN-MMMM.gor`), not the
                // full S3 key. Detect a bare basename (no `/`) and
                // prepend the bucket prefix.
                let key = if entry.key.contains('/') {
                    entry.key.clone()
                } else {
                    format!("{}{}", bucket_prefix, entry.key)
                };
                out.push(ChunkRef {
                    key,
                    metric: metric.to_string(),
                    time_range_ms: (entry_start_ms, entry_end_ms),
                    label_hash: entry.label_hash,
                    sample_count: entry.sample_count,
                    size_bytes: entry.size_bytes,
                });
            }
        }
        Ok(out)
    }

    async fn read_chunk(&self, chunk: &ChunkRef) -> Result<Vec<RawSample>, StoreError> {
        // Cache hit fast path.
        {
            let mut guard = self.cache.lock().await;
            if let Some(cached) = guard.get(&chunk.key).cloned() {
                return Ok((*cached).clone());
            }
        }

        let bytes = self.object_store.get_object(&chunk.key).await?;
        let samples = decode_block(&bytes)
            .map_err(|e| StoreError::Malformed(format!("decode {}: {e}", chunk.key)))?;

        let arc = Arc::new(samples.clone());
        {
            let mut guard = self.cache.lock().await;
            guard.put(chunk.key.clone(), arc);
        }
        Ok(samples)
    }

    /// **mvp/v5**: postings-aware chunk pruning — delegated to
    /// [`super::postings::intersect_per_bucket_postings`] so the
    /// per-bucket fetch + intersect logic sits in one place
    /// regardless of which `Store` impl owns the postings cache.
    async fn list_postings_for(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[(String, String)],
    ) -> Result<PostingsHits, StoreError> {
        let buckets = Self::hour_starts(start_ms, end_ms);
        let mut keys: Vec<String> = Vec::with_capacity(buckets.len());
        for hour_ms in buckets {
            keys.push(self.postings_key(metric, hour_ms));
        }
        intersect_per_bucket_postings(
            self.object_store.as_ref(),
            &self.postings_cache,
            &keys,
            matchers,
        )
        .await
    }
}

// ─────────────────────────────────────────────────────────────────────
// Decoding helper — converts a GORILLA1 block to RawSample units
// ─────────────────────────────────────────────────────────────────────

/// Decode a single in-memory `GORILLA1` block into [`RawSample`]s.
fn decode_block(bytes: &[u8]) -> Result<Vec<RawSample>, asap_gorilla::DecodeError> {
    let mut decoder = GorillaDecoder::from_reader(bytes)?;
    let mut out: Vec<RawSample> = Vec::new();
    while let Some(header) = decoder.header().cloned() {
        let labels: BTreeMap<String, String> = header.labels.iter().cloned().collect();
        for sample in decoder.samples() {
            let (ts_ns, value) = sample?;
            out.push(RawSample {
                ts_ms: (ts_ns / 1_000_000) as i64,
                labels: labels.clone(),
                value,
            });
        }
        if !decoder.next_series()? {
            break;
        }
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────
// In-memory ObjectStore mock — pub(crate) so tests in sibling files
// can exercise the same fixture without a live MinIO.
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[derive(Default)]
pub(crate) struct InMemoryObjectStore {
    inner: Mutex<InMemoryState>,
}

#[cfg(test)]
#[derive(Default)]
struct InMemoryState {
    objects: HashMap<String, Vec<u8>>,
    fetch_counts: HashMap<String, usize>,
    fail_all: Option<String>,
}

#[cfg(test)]
impl InMemoryObjectStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn put(&self, key: impl Into<String>, body: Vec<u8>) {
        let mut g = self.inner.lock().await;
        g.objects.insert(key.into(), body);
    }

    pub(crate) async fn get_count(&self, key: &str) -> usize {
        let g = self.inner.lock().await;
        g.fetch_counts.get(key).copied().unwrap_or(0)
    }

    /// Make every subsequent `get_object` fail with a backend error
    /// containing `msg`. Used by the network-error test.
    pub(crate) async fn fail_all(&self, msg: impl Into<String>) {
        let mut g = self.inner.lock().await;
        g.fail_all = Some(msg.into());
    }
}

#[cfg(test)]
#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        let mut g = self.inner.lock().await;
        if let Some(msg) = g.fail_all.clone() {
            return Err(StoreError::Backend(msg));
        }
        *g.fetch_counts.entry(key.to_string()).or_insert(0) += 1;
        match g.objects.get(key) {
            Some(b) => Ok(b.clone()),
            None => Err(StoreError::Backend(format!("get {key}: not found"))),
        }
    }
}

// Static `Send` assertion — `GorillaS3Store` must be storable
// behind an `Arc<dyn Store>` in the engine wiring.
const _: fn() = || {
    fn _assert_send<T: Send>() {}
    _assert_send::<GorillaS3Store>();
};

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use asap_gorilla::{GorillaEncoder, IndexEntry, IndexFile};
    use chrono::TimeZone;

    /// Build a minimal index.json fixture.
    fn make_index(entries: Vec<IndexEntry>) -> Vec<u8> {
        let mut idx = IndexFile::new(0);
        idx.entries = entries;
        let mut buf = Vec::new();
        idx.write(&mut buf).unwrap();
        buf
    }

    /// Encode a single-series Gorilla block from `(ts_ms, value)` pairs.
    fn make_block(metric: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> Vec<u8> {
        let mut enc = GorillaEncoder::new(
            metric.to_string(),
            labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        for (ts_ms, v) in samples {
            enc.append((*ts_ms as u64) * 1_000_000, *v);
        }
        enc.finalize().unwrap()
    }

    fn ms(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .unwrap()
            .timestamp_millis()
    }

    fn cfg() -> GorillaS3Config {
        GorillaS3Config {
            endpoint: Some("http://mock".to_string()),
            bucket: "test-bucket".to_string(),
            tenant: "tenant1".to_string(),
            prefix_template: "{tenant}/{metric}/{year}/{month}/{day}/{hour}/".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
            cache_capacity: 4,
            use_ssl: false,
        }
    }

    #[tokio::test]
    async fn list_chunks_via_indexfile_prunes_by_time() {
        let store = InMemoryObjectStore::new();

        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let metric = "node_cpu_seconds_total";

        let key_a = "tenant1/node_cpu_seconds_total/2026/05/06/12/part-A.gor".to_string();
        let key_b = "tenant1/node_cpu_seconds_total/2026/05/06/12/part-B.gor".to_string();
        let key_c = "tenant1/node_cpu_seconds_total/2026/05/06/12/part-C.gor".to_string();

        let entries = vec![
            IndexEntry {
                key: key_a.clone(),
                time_range: ((h0) as u64 * 1_000_000, (h0 + 999) as u64 * 1_000_000),
                sample_count: 10,
                label_hash: 0xAAAA,
                size_bytes: 100,
                object_key: None,
                byte_offset: None,
                byte_length: None,
            },
            IndexEntry {
                key: key_b.clone(),
                time_range: (
                    (h0 + 5_000) as u64 * 1_000_000,
                    (h0 + 6_000) as u64 * 1_000_000,
                ),
                sample_count: 11,
                label_hash: 0xBBBB,
                size_bytes: 110,
                object_key: None,
                byte_offset: None,
                byte_length: None,
            },
            IndexEntry {
                key: key_c.clone(),
                time_range: (
                    (h0 + 10_000) as u64 * 1_000_000,
                    (h0 + 11_000) as u64 * 1_000_000,
                ),
                sample_count: 12,
                label_hash: 0xCCCC,
                size_bytes: 120,
                object_key: None,
                byte_offset: None,
                byte_length: None,
            },
        ];

        store
            .put(
                "tenant1/node_cpu_seconds_total/2026/05/06/12/index.json",
                make_index(entries),
            )
            .await;

        let cs = GorillaS3Store::new(Arc::new(store), cfg());
        let chunks = cs
            .list_chunks(metric, h0 + 5_500, h0 + 5_800)
            .await
            .unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].key, key_b);
        assert_eq!(chunks[0].sample_count, 11);
        assert_eq!(chunks[0].label_hash, 0xBBBB);
        assert_eq!(chunks[0].metric, metric);
    }

    #[tokio::test]
    async fn read_chunk_decodes_via_asap_gorilla() {
        let store = InMemoryObjectStore::new();

        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let metric = "node_cpu_seconds_total";
        let labels = &[("instance", "i-1"), ("mode", "user")];

        let block = make_block(
            metric,
            labels,
            &[(h0 + 1_000, 0.5), (h0 + 2_000, 0.7), (h0 + 3_000, 0.7)],
        );
        let chunk_key = "tenant1/node_cpu_seconds_total/2026/05/06/12/part-001.gor".to_string();

        store.put(chunk_key.clone(), block.clone()).await;
        store
            .put(
                "tenant1/node_cpu_seconds_total/2026/05/06/12/index.json",
                make_index(vec![IndexEntry {
                    key: chunk_key.clone(),
                    time_range: (
                        (h0 + 1_000) as u64 * 1_000_000,
                        (h0 + 3_000) as u64 * 1_000_000,
                    ),
                    sample_count: 3,
                    label_hash: 0x1234,
                    size_bytes: block.len() as u32,
                    object_key: None,
                    byte_offset: None,
                    byte_length: None,
                }]),
            )
            .await;

        let cs = GorillaS3Store::new(Arc::new(store), cfg());
        let chunks = cs.list_chunks(metric, h0, h0 + 60_000).await.unwrap();
        assert_eq!(chunks.len(), 1);

        let samples = cs.read_chunk(&chunks[0]).await.unwrap();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].ts_ms, h0 + 1_000);
        assert_eq!(samples[0].value, 0.5);
        assert_eq!(samples[1].ts_ms, h0 + 2_000);
        assert_eq!(samples[1].value, 0.7);
        assert_eq!(samples[2].ts_ms, h0 + 3_000);
        assert_eq!(samples[2].value, 0.7);
        assert_eq!(
            samples[0].labels.get("instance").map(String::as_str),
            Some("i-1")
        );
        assert_eq!(
            samples[0].labels.get("mode").map(String::as_str),
            Some("user")
        );
    }

    #[tokio::test]
    async fn cache_hit_skips_s3_fetch() {
        let store = Arc::new(InMemoryObjectStore::new());

        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let metric = "m";
        let block = make_block(metric, &[], &[(h0 + 1_000, 1.0), (h0 + 2_000, 2.0)]);
        let chunk_key = "tenant1/m/2026/05/06/12/part-X.gor".to_string();
        store.put(chunk_key.clone(), block.clone()).await;
        store
            .put(
                "tenant1/m/2026/05/06/12/index.json",
                make_index(vec![IndexEntry {
                    key: chunk_key.clone(),
                    time_range: (
                        (h0 + 1_000) as u64 * 1_000_000,
                        (h0 + 2_000) as u64 * 1_000_000,
                    ),
                    sample_count: 2,
                    label_hash: 0,
                    size_bytes: block.len() as u32,
                    object_key: None,
                    byte_offset: None,
                    byte_length: None,
                }]),
            )
            .await;

        let cs = GorillaS3Store::new(store.clone(), cfg());
        let chunks = cs.list_chunks(metric, h0, h0 + 60_000).await.unwrap();
        assert_eq!(chunks.len(), 1);

        let _ = cs.read_chunk(&chunks[0]).await.unwrap();
        let count_after_first = store.get_count(&chunk_key).await;
        let _ = cs.read_chunk(&chunks[0]).await.unwrap();
        let count_after_second = store.get_count(&chunk_key).await;

        assert_eq!(count_after_first, 1);
        assert_eq!(
            count_after_second, 1,
            "second read_chunk must hit cache and skip S3 GET"
        );
    }

    #[tokio::test]
    async fn lru_eviction_under_pressure() {
        let store = Arc::new(InMemoryObjectStore::new());
        let h0 = ms(2026, 5, 6, 12, 0, 0);

        let mut chunk_refs: Vec<ChunkRef> = Vec::new();
        let mut entries: Vec<IndexEntry> = Vec::new();
        for i in 0..3i64 {
            let block = make_block(
                "m",
                &[("i", &i.to_string())],
                &[
                    (h0 + i * 1_000, i as f64),
                    (h0 + i * 1_000 + 100, i as f64 + 0.5),
                ],
            );
            let key = format!("tenant1/m/2026/05/06/12/part-{i}.gor");
            store.put(key.clone(), block.clone()).await;
            entries.push(IndexEntry {
                key: key.clone(),
                time_range: (
                    ((h0 + i * 1_000) as u64) * 1_000_000,
                    ((h0 + i * 1_000 + 100) as u64) * 1_000_000,
                ),
                sample_count: 2,
                label_hash: i as u64,
                size_bytes: block.len() as u32,
                object_key: None,
                byte_offset: None,
                byte_length: None,
            });
            chunk_refs.push(ChunkRef {
                key,
                metric: "m".to_string(),
                time_range_ms: (h0 + i * 1_000, h0 + i * 1_000 + 100),
                label_hash: i as u64,
                sample_count: 2,
                size_bytes: block.len() as u32,
            });
        }
        store
            .put("tenant1/m/2026/05/06/12/index.json", make_index(entries))
            .await;

        let mut config = cfg();
        config.cache_capacity = 2;
        let cs = GorillaS3Store::new(store.clone(), config);

        cs.read_chunk(&chunk_refs[0]).await.unwrap();
        cs.read_chunk(&chunk_refs[1]).await.unwrap();
        cs.read_chunk(&chunk_refs[2]).await.unwrap(); // evicts chunk_refs[0]

        let before = store.get_count(&chunk_refs[0].key).await;
        cs.read_chunk(&chunk_refs[0]).await.unwrap();
        let after = store.get_count(&chunk_refs[0].key).await;
        assert_eq!(
            after,
            before + 1,
            "evicted chunk must trigger a fresh S3 GET"
        );
    }

    #[tokio::test]
    async fn index_json_corrupted_returns_error() {
        let store = InMemoryObjectStore::new();
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        store
            .put(
                "tenant1/m/2026/05/06/12/index.json",
                b"this is not json {{{".to_vec(),
            )
            .await;

        let cs = GorillaS3Store::new(Arc::new(store), cfg());
        let res = cs.list_chunks("m", h0, h0 + 60_000).await;
        match res {
            Err(StoreError::Malformed(msg)) => {
                assert!(msg.contains("index.json"), "msg should name the key: {msg}")
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn s3_unavailable_returns_error() {
        let store = Arc::new(InMemoryObjectStore::new());
        store.fail_all("simulated network outage").await;
        let cs = GorillaS3Store::new(store, cfg());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let res = cs.list_chunks("m", h0, h0 + 60_000).await;
        match res {
            Err(StoreError::Backend(msg)) => assert!(msg.contains("simulated network outage")),
            other => panic!("expected Backend, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_index_is_empty_not_error() {
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3Store::new(Arc::new(store), cfg());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let chunks = cs
            .list_chunks("never_written", h0, h0 + 60_000)
            .await
            .unwrap();
        assert!(chunks.is_empty());
        let samples = cs.scan("never_written", h0, h0 + 60_000).await.unwrap();
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn scan_filters_to_requested_range() {
        let store = Arc::new(InMemoryObjectStore::new());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let block = make_block("m", &[], &[(h0 + 1_000, 1.0), (h0 + 10_000, 2.0)]);
        let key = "tenant1/m/2026/05/06/12/part-Z.gor".to_string();
        store.put(key.clone(), block.clone()).await;
        store
            .put(
                "tenant1/m/2026/05/06/12/index.json",
                make_index(vec![IndexEntry {
                    key: key.clone(),
                    time_range: (
                        (h0 + 1_000) as u64 * 1_000_000,
                        (h0 + 10_000) as u64 * 1_000_000,
                    ),
                    sample_count: 2,
                    label_hash: 0,
                    size_bytes: block.len() as u32,
                    object_key: None,
                    byte_offset: None,
                    byte_length: None,
                }]),
            )
            .await;

        let cs = GorillaS3Store::new(store, cfg());
        let samples = cs.scan("m", h0 + 5_000, h0 + 9_000).await.unwrap();
        assert!(samples.is_empty(), "no sample inside [5_000, 9_000) ms");

        let samples = cs.scan("m", h0, h0 + 60_000).await.unwrap();
        assert_eq!(samples.len(), 2);
    }

    #[tokio::test]
    async fn list_chunks_spans_two_hour_buckets() {
        let store = InMemoryObjectStore::new();
        let h12 = ms(2026, 5, 6, 12, 0, 0);
        let h13 = ms(2026, 5, 6, 13, 0, 0);
        let key12 = "tenant1/m/2026/05/06/12/part-1.gor".to_string();
        let key13 = "tenant1/m/2026/05/06/13/part-1.gor".to_string();
        store
            .put(
                "tenant1/m/2026/05/06/12/index.json",
                make_index(vec![IndexEntry {
                    key: key12.clone(),
                    time_range: (
                        (h12 + 3_500_000) as u64 * 1_000_000,
                        (h12 + 3_590_000) as u64 * 1_000_000,
                    ),
                    sample_count: 1,
                    label_hash: 0,
                    size_bytes: 50,
                    object_key: None,
                    byte_offset: None,
                    byte_length: None,
                }]),
            )
            .await;
        store
            .put(
                "tenant1/m/2026/05/06/13/index.json",
                make_index(vec![IndexEntry {
                    key: key13.clone(),
                    time_range: (
                        (h13 + 1_000) as u64 * 1_000_000,
                        (h13 + 30_000) as u64 * 1_000_000,
                    ),
                    sample_count: 1,
                    label_hash: 0,
                    size_bytes: 50,
                    object_key: None,
                    byte_offset: None,
                    byte_length: None,
                }]),
            )
            .await;
        let cs = GorillaS3Store::new(Arc::new(store), cfg());
        let chunks = cs
            .list_chunks("m", h12 + 3_500_000, h13 + 30_000)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].key, key12);
        assert_eq!(chunks[1].key, key13);
    }

    #[test]
    fn bucket_prefix_supports_long_form_placeholders() {
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{year}/{month}/{day}/{hour}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3Store::new(Arc::new(store), config);
        let key = cs.bucket_prefix("foo", ms(2026, 5, 6, 12, 0, 0));
        assert_eq!(key, "tenant1/foo/2026/05/06/12/");
    }

    #[test]
    fn bucket_prefix_supports_agent_side_yyyy_mm_dd_hh_placeholders() {
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3Store::new(Arc::new(store), config);
        let key = cs.bucket_prefix("http_freshness_probe_archive", ms(2026, 5, 7, 4, 0, 0));
        assert_eq!(key, "tenant1/http_freshness_probe_archive/2026/05/07/04/",);
    }

    #[test]
    fn bucket_prefix_handles_mixed_long_and_short_placeholders() {
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{year}/{MM}/{DD}/{hour}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3Store::new(Arc::new(store), config);
        let key = cs.bucket_prefix("m", ms(2026, 5, 7, 4, 0, 0));
        assert_eq!(key, "tenant1/m/2026/05/07/04/");
    }

    #[test]
    fn from_env_requires_bucket() {
        let prev_bucket = std::env::var("ASAP_GORILLA_S3_BUCKET").ok();
        std::env::remove_var("ASAP_GORILLA_S3_BUCKET");
        let res = GorillaS3Config::from_env();
        if let Some(v) = prev_bucket {
            std::env::set_var("ASAP_GORILLA_S3_BUCKET", v);
        }
        match res {
            Err(GorillaS3ConfigError::MissingEnv("ASAP_GORILLA_S3_BUCKET")) => {}
            other => panic!("expected MissingEnv(BUCKET), got {other:?}"),
        }
    }
}
