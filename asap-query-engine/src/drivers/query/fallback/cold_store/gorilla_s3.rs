//! Gorilla-on-S3 [`ColdStore`] adapter — Phase 3 of the
//! Gorilla-S3-cold-engine.
//!
//! Lists per-hour `index.json` catalogs out of an S3-compatible
//! bucket, prunes them by time range, then fetches + decodes the
//! selected `GORILLA1` chunks via the freshly-merged
//! [`asap_gorilla`] crate (`ASAPCollector` PR #281).
//!
//! Sits alongside [`super::LocalFsColdStore`] — both impls satisfy
//! the same [`super::ColdStore`] trait, so the existing
//! `s3_adapter::ColdFallback` query path can swap between them
//! without code change. The Phase 3 trait extension
//! ([`super::ColdStore::list_chunks`] / [`super::ColdStore::read_chunk`])
//! lets the upcoming Phase 4 `GorillaQueryEngine` pull chunks one
//! at a time without materialising every sample.
//!
//! # Object key layout
//!
//! `GorillaS3ColdStore` is **agnostic** about the on-S3 chunk-key
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

use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(test)]
use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use lru::LruCache;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

use asap_gorilla::{GorillaDecoder, IndexFile, Postings};

use super::{ChunkRef, ColdStore, ColdStoreError, PostingsHits, RawSample};

// ─────────────────────────────────────────────────────────────────────
// Public config
// ─────────────────────────────────────────────────────────────────────

/// Tunable configuration for [`GorillaS3ColdStore`].
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
/// Sized + `Send + Sync` so [`GorillaS3ColdStore`] can hold one
/// behind an `Arc<dyn ObjectStore>` regardless of how it's backed.
/// Production callers use [`S3ObjectStore`] (rust-s3); tests use the
/// in-memory mock at the bottom of this file.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Fetch the full object body for `key`.
    ///
    /// Returns [`ColdStoreError::Backend`] for transport errors and
    /// [`ColdStoreError::Backend`] (with a `not found` substring)
    /// for missing keys; callers distinguish via
    /// [`ObjectStore::object_missing`] if they need to.
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, ColdStoreError>;

    /// True iff `err` was raised because the requested key did not
    /// exist (vs. a transport / permission failure). Used by the
    /// list path to treat a missing `index.json` as "no chunks for
    /// this hour" rather than a hard error.
    fn object_missing(&self, err: &ColdStoreError) -> bool {
        matches!(err, ColdStoreError::Backend(msg) if msg.contains("not found"))
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
        pub fn new(cfg: &GorillaS3Config) -> Result<Self, ColdStoreError> {
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
                    .map_err(|e| ColdStoreError::Backend(format!("region parse: {e}")))?,
            };
            let creds = match (&cfg.access_key_id, &cfg.secret_access_key) {
                (Some(ak), Some(sk)) => {
                    Credentials::new(Some(ak), Some(sk), None, None, None).map_err(|e| {
                        ColdStoreError::Backend(format!("credentials: {e}"))
                    })?
                }
                _ => Credentials::default().map_err(|e| {
                    ColdStoreError::Backend(format!("default credentials: {e}"))
                })?,
            };
            let bucket = Bucket::new(&cfg.bucket, region, creds)
                .map_err(|e| ColdStoreError::Backend(format!("bucket: {e}")))?;
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
        async fn get_object(&self, key: &str) -> Result<Vec<u8>, ColdStoreError> {
            let resp = self
                .bucket
                .get_object(key)
                .await
                .map_err(|e| ColdStoreError::Backend(format!("s3 get {key}: {e}")))?;
            if resp.status_code() == 404 {
                return Err(ColdStoreError::Backend(format!(
                    "s3 get {key}: not found"
                )));
            }
            if !(200..300).contains(&resp.status_code()) {
                return Err(ColdStoreError::Backend(format!(
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
// GorillaS3ColdStore
// ─────────────────────────────────────────────────────────────────────

/// LRU cache keyed by chunk object key. Stored values are
/// pre-decoded `RawSample` lists so repeated reads of the same
/// chunk skip the Gorilla decode pass entirely.
type ChunkCache = Mutex<LruCache<String, Arc<Vec<RawSample>>>>;

/// **mvp/v5**: LRU cache for parsed postings sidecars. Keyed by
/// the postings-v1.json S3 key (one per `(metric, hour)`). 256
/// entries by default → ≈ 256 MiB at 1 MiB per postings file.
type PostingsCache = Mutex<LruCache<String, Arc<Postings>>>;

/// **mvp/v5**: LRU cache for parsed `index.json` files (per
/// `(metric, hour)`). Same capacity tier as the postings cache.
type IndexCache = Mutex<LruCache<String, Arc<IndexFile>>>;

/// `ColdStore` adapter that reads `GORILLA1`-format chunks out of
/// an S3-compatible bucket. See module docs for layout + S3 client
/// notes.
pub struct GorillaS3ColdStore {
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

impl GorillaS3ColdStore {
    /// Build with an explicit object-store backend. The production
    /// constructor [`Self::with_default_backend`] wires up
    /// `S3ObjectStore` from `cfg`; tests inject the in-memory mock.
    pub fn new(object_store: Arc<dyn ObjectStore>, config: GorillaS3Config) -> Self {
        let cap = NonZeroUsize::new(config.cache_capacity.max(1))
            .unwrap_or(NonZeroUsize::new(1).unwrap());
        // mvp/v5: postings + index caches scale with the chunk
        // cache (one entry per hour-bucket, mirrors typical query
        // cardinality).
        let pc_cap = NonZeroUsize::new(cap.get().max(64))
            .unwrap_or(NonZeroUsize::new(64).unwrap());
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
    /// [`super::S3CostTrackingObjectStore`] tied to the global
    /// counter set, so the HTTP server's `/internal/s3_cost.csv`
    /// + `/metrics` endpoints report measured PUT/GET/etc counts.
    pub fn with_default_backend(config: GorillaS3Config) -> Result<Self, ColdStoreError> {
        let backend: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(&config)?);
        let counters = super::s3_cost_tracker::global_s3_cost_counters();
        let tracked = super::S3CostTrackingObjectStore::new(backend, counters);
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
    ///   `gorillas3processor`'s naming, documented in
    ///   `opentelemetry-collector-contrib-patch/processor/
    ///   gorillas3processor/config.go`.
    ///
    /// Pre-v7 the two sides used different placeholders, so when a
    /// deploy set `ASAP_GORILLA_S3_PREFIX_TEMPLATE` to the
    /// agent-side spelling (the v6 demo does — see
    /// `deploy/docker-compose/mvp-v6-multi-stage.yml`), the backend
    /// substituted `{tenant}` and `{metric}` but left the
    /// timestamp placeholders un-replaced, so every `index.json`
    /// fetch issued a literal `{YYYY}/{MM}/{DD}/{HH}` path that
    /// missed the actual chunk objects on disk. Issue #46
    /// criterion ⑥ (freshness probes) surfaced as 0 samples on
    /// every path because of this. Accepting both spellings keeps
    /// pre-v7 deploys working AND the v6/v7 demo deploy aligned.
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
            // Long-form placeholders (the backend's historical
            // spelling — preserved for backwards compatibility).
            .replace("{year}", &year)
            .replace("{month}", &month)
            .replace("{day}", &day)
            .replace("{hour}", &hour)
            // Agent-side `{YYYY}`/`{MM}`/`{DD}`/`{HH}` aliases —
            // matches the spelling in the agent's
            // `gorillas3processor/config.go` and
            // `s3_sink.go::renderPrefix`.
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
    /// failure surfaces as `ColdStoreError::Backend`.
    async fn fetch_index(&self, metric: &str, hour_ms: i64) -> Result<IndexFile, ColdStoreError> {
        let key = self.index_key(metric, hour_ms);
        match self.object_store.get_object(&key).await {
            Ok(bytes) => IndexFile::read(bytes.as_slice()).map_err(|e| {
                ColdStoreError::Malformed(format!("index.json at {key}: {e}"))
            }),
            Err(e) if self.object_store.object_missing(&e) => {
                debug!(key = %key, "gorilla-s3: index.json missing for hour bucket; skipping");
                Ok(IndexFile::new(0))
            }
            Err(e) => Err(e),
        }
    }
}

#[async_trait]
impl ColdStore for GorillaS3ColdStore {
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError> {
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
    ) -> Result<Vec<ChunkRef>, ColdStoreError> {
        // Convert the request window to the nanosecond unit the
        // index file uses (`IndexEntry.time_range` is `(ns, ns)`,
        // mirroring the Go encoder's `time.Time.UnixNano()` source).
        let start_ns = (start_ms as i128).saturating_mul(1_000_000) as u64;
        // `end_ms` is exclusive on the ms side; the index iter
        // overlap test is inclusive so subtract 1 ns to keep the
        // semantics aligned. If `end_ms == start_ms` we still want
        // to scan the bucket containing `start_ms`.
        let end_ns = if end_ms <= start_ms {
            start_ns
        } else {
            ((end_ms as i128).saturating_mul(1_000_000) - 1).max(0) as u64
        };

        let mut out = Vec::new();
        for hour_ms in Self::hour_starts(start_ms, end_ms) {
            let idx = self.fetch_index(metric, hour_ms).await?;
            for entry in idx.prune_by_time((start_ns, end_ns)) {
                let (entry_start_ms, entry_end_ms) = (
                    (entry.time_range.0 / 1_000_000) as i64,
                    (entry.time_range.1 / 1_000_000) as i64,
                );
                out.push(ChunkRef {
                    key: entry.key.clone(),
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

    async fn read_chunk(&self, chunk: &ChunkRef) -> Result<Vec<RawSample>, ColdStoreError> {
        // Cache hit fast path.
        {
            let mut guard = self.cache.lock().await;
            if let Some(cached) = guard.get(&chunk.key).cloned() {
                return Ok((*cached).clone());
            }
        }

        let bytes = self.object_store.get_object(&chunk.key).await?;
        let samples = decode_block(&bytes)
            .map_err(|e| ColdStoreError::Malformed(format!("decode {}: {e}", chunk.key)))?;

        let arc = Arc::new(samples.clone());
        {
            let mut guard = self.cache.lock().await;
            guard.put(chunk.key.clone(), arc);
        }
        Ok(samples)
    }

    /// **mvp/v5**: postings-aware chunk pruning.
    ///
    /// Walks the per-hour buckets covering `[start_ms, end_ms)`,
    /// fetches each `postings-v1.json` (LRU-cached), and intersects
    /// the per-matcher series-id lists across every bucket.
    /// Missing-postings buckets are noted (caller-visible quirk).
    ///
    /// Empty `matchers` ⇒ returns the union of all postings'
    /// series_ids in range — this is the "no predicate"
    /// short-circuit and the engine usually skips calling us in
    /// that case.
    async fn list_postings_for(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
        matchers: &[(String, String)],
    ) -> Result<PostingsHits, ColdStoreError> {
        let buckets = Self::hour_starts(start_ms, end_ms);
        let mut hits = PostingsHits {
            series_ids: Vec::new(),
            buckets_in_range: buckets.len(),
            buckets_with_postings: 0,
        };
        // Per-bucket: load postings, intersect across matchers,
        // union into the running result. Cross-bucket join is a
        // UNION (a series might exist in one hour but not the
        // next); intra-bucket intersection across matchers is an
        // AND.
        let mut union_set: std::collections::BTreeSet<u64> =
            std::collections::BTreeSet::new();
        for hour_ms in buckets {
            let key = self.postings_key(metric, hour_ms);
            // LRU short-circuit.
            let postings = {
                let mut guard = self.postings_cache.lock().await;
                guard.get(&key).cloned()
            };
            let postings = match postings {
                Some(p) => Some(p),
                None => match self.object_store.get_object(&key).await {
                    Ok(bytes) => match Postings::read(bytes.as_slice()) {
                        Ok(p) => {
                            let arc = Arc::new(p);
                            let mut guard = self.postings_cache.lock().await;
                            guard.put(key.clone(), arc.clone());
                            Some(arc)
                        }
                        Err(e) => {
                            // Treat a corrupt postings file as
                            // "missing" — the engine then falls
                            // through to the scan-all path with
                            // the postings_missing quirk.
                            debug!(key = %key, error = %e, "gorilla-s3: postings parse failed; treating as missing");
                            None
                        }
                    },
                    Err(e) if self.object_store.object_missing(&e) => {
                        debug!(key = %key, "gorilla-s3: postings missing for hour bucket");
                        None
                    }
                    Err(e) => return Err(e),
                },
            };
            let Some(postings) = postings else { continue };
            hits.buckets_with_postings += 1;

            // Intersect across matchers within this bucket.
            let bucket_set: std::collections::BTreeSet<u64> = if matchers.is_empty() {
                // Union of every series_id across every label.
                let mut set = std::collections::BTreeSet::new();
                for by_value in postings.by_label.values() {
                    for ids in by_value.values() {
                        set.extend(ids.iter().copied());
                    }
                }
                set
            } else {
                let first =
                    postings.lookup(&matchers[0].0, &matchers[0].1);
                let mut acc: std::collections::BTreeSet<u64> =
                    first.iter().copied().collect();
                for (label_name, label_value) in &matchers[1..] {
                    let next = postings.lookup(label_name, label_value);
                    let next_set: std::collections::BTreeSet<u64> =
                        next.iter().copied().collect();
                    acc = acc.intersection(&next_set).copied().collect();
                }
                acc
            };
            union_set.extend(bucket_set);
        }
        hits.series_ids = union_set.into_iter().collect();
        Ok(hits)
    }
}

// ─────────────────────────────────────────────────────────────────────
// Decoding helper — converts a GORILLA1 block to RawSample units
// ─────────────────────────────────────────────────────────────────────

/// Decode a single in-memory `GORILLA1` block into [`RawSample`]s.
///
/// Walks every series in the block; multi-series blocks are
/// flattened into one `Vec`. Timestamps are converted from the
/// on-wire nanoseconds (Go `time.Time.UnixNano()` source) to the
/// [`RawSample::ts_ms`] millisecond unit.
fn decode_block(bytes: &[u8]) -> Result<Vec<RawSample>, asap_gorilla::DecodeError> {
    let mut decoder = GorillaDecoder::from_reader(bytes)?;
    let mut out: Vec<RawSample> = Vec::new();
    while let Some(header) = decoder.header().cloned() {
        let labels: std::collections::BTreeMap<String, String> =
            header.labels.iter().cloned().collect();
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

/// In-memory [`ObjectStore`] used by `gorilla_s3` tests.
///
/// Holds a `HashMap<key, Vec<u8>>` plus a per-key fetch counter so
/// cache-hit assertions are first-class. Optionally fails every
/// `get_object` call for the network-error test.
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
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, ColdStoreError> {
        let mut g = self.inner.lock().await;
        if let Some(msg) = g.fail_all.clone() {
            return Err(ColdStoreError::Backend(msg));
        }
        *g.fetch_counts.entry(key.to_string()).or_insert(0) += 1;
        match g.objects.get(key) {
            Some(b) => Ok(b.clone()),
            None => Err(ColdStoreError::Backend(format!("get {key}: not found"))),
        }
    }
}

// Static `Send` assertion — `GorillaS3ColdStore` must be storable
// behind an `Arc<dyn ColdStore>` in the existing s3_adapter chain.
const _: fn() = || {
    fn _assert_send<T: Send>() {}
    _assert_send::<GorillaS3ColdStore>();
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
            // ts_ms → ts_ns
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

    /// Layout: hour bucket H, three chunks A/B/C in time order, the
    /// requested window only overlaps B → list returns B alone.
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
                time_range: ((h0 + 5_000) as u64 * 1_000_000, (h0 + 6_000) as u64 * 1_000_000),
                sample_count: 11,
                label_hash: 0xBBBB,
                size_bytes: 110,
            object_key: None,
            byte_offset: None,
            byte_length: None,
            },
            IndexEntry {
                key: key_c.clone(),
                time_range: ((h0 + 10_000) as u64 * 1_000_000, (h0 + 11_000) as u64 * 1_000_000),
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

        let cs = GorillaS3ColdStore::new(Arc::new(store), cfg());
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

        let cs = GorillaS3ColdStore::new(Arc::new(store), cfg());
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
        assert_eq!(samples[0].labels.get("instance").map(String::as_str), Some("i-1"));
        assert_eq!(samples[0].labels.get("mode").map(String::as_str), Some("user"));
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

        let cs = GorillaS3ColdStore::new(store.clone(), cfg());
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
        // cache_capacity=2, fill with three chunks then re-read the
        // first → that triggers an S3 GET because the LRU evicted it.
        let store = Arc::new(InMemoryObjectStore::new());
        let h0 = ms(2026, 5, 6, 12, 0, 0);

        let mut chunk_refs: Vec<ChunkRef> = Vec::new();
        let mut entries: Vec<IndexEntry> = Vec::new();
        for i in 0..3i64 {
            let block = make_block(
                "m",
                &[("i", &i.to_string())],
                &[(h0 + i * 1_000, i as f64), (h0 + i * 1_000 + 100, i as f64 + 0.5)],
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
        let cs = GorillaS3ColdStore::new(store.clone(), config);

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

        let cs = GorillaS3ColdStore::new(Arc::new(store), cfg());
        let res = cs.list_chunks("m", h0, h0 + 60_000).await;
        match res {
            Err(ColdStoreError::Malformed(msg)) => {
                assert!(msg.contains("index.json"), "msg should name the key: {msg}")
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn s3_unavailable_returns_error() {
        let store = Arc::new(InMemoryObjectStore::new());
        store.fail_all("simulated network outage").await;
        let cs = GorillaS3ColdStore::new(store, cfg());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let res = cs.list_chunks("m", h0, h0 + 60_000).await;
        match res {
            Err(ColdStoreError::Backend(msg)) => assert!(msg.contains("simulated network outage")),
            other => panic!("expected Backend, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_index_is_empty_not_error() {
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3ColdStore::new(Arc::new(store), cfg());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let chunks = cs.list_chunks("never_written", h0, h0 + 60_000).await.unwrap();
        assert!(chunks.is_empty());
        let samples = cs.scan("never_written", h0, h0 + 60_000).await.unwrap();
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn scan_filters_to_requested_range() {
        // Chunk has samples at h0+1_000 and h0+10_000; request only
        // [h0+5_000, h0+9_000) — chunk overlaps the request, but the
        // matching sample is *outside* the inner filter, so scan
        // returns 0 samples (read_chunk would still load + cache the
        // chunk).
        let store = Arc::new(InMemoryObjectStore::new());
        let h0 = ms(2026, 5, 6, 12, 0, 0);
        let block = make_block(
            "m",
            &[],
            &[(h0 + 1_000, 1.0), (h0 + 10_000, 2.0)],
        );
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

        let cs = GorillaS3ColdStore::new(store, cfg());
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
        let cs = GorillaS3ColdStore::new(Arc::new(store), cfg());
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
        // Backend's historical spelling — preserved.
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{year}/{month}/{day}/{hour}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3ColdStore::new(Arc::new(store), config);
        let key = cs.bucket_prefix("foo", ms(2026, 5, 6, 12, 0, 0));
        assert_eq!(key, "tenant1/foo/2026/05/06/12/");
    }

    #[test]
    fn bucket_prefix_supports_agent_side_yyyy_mm_dd_hh_placeholders() {
        // v7 fix: the agent's gorillas3processor uses
        // `{YYYY}`/`{MM}`/`{DD}`/`{HH}`. Pre-v7 the backend left
        // these literal; v7 substitutes them so a deploy that
        // configures the routing yaml with the agent-side
        // spelling gets matching index.json keys on both sides.
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{YYYY}/{MM}/{DD}/{HH}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3ColdStore::new(Arc::new(store), config);
        let key = cs.bucket_prefix("http_freshness_probe_archive", ms(2026, 5, 7, 4, 0, 0));
        assert_eq!(
            key,
            "tenant1/http_freshness_probe_archive/2026/05/07/04/",
            "v7 must substitute {{YYYY}}/{{MM}}/{{DD}}/{{HH}} the same as the long-form names",
        );
    }

    #[test]
    fn bucket_prefix_handles_mixed_long_and_short_placeholders() {
        // Defensive — accept a mix in case some operator templates
        // it that way.
        let mut config = cfg();
        config.prefix_template = "{tenant}/{metric}/{year}/{MM}/{DD}/{hour}/".to_string();
        let store = InMemoryObjectStore::new();
        let cs = GorillaS3ColdStore::new(Arc::new(store), config);
        let key = cs.bucket_prefix("m", ms(2026, 5, 7, 4, 0, 0));
        assert_eq!(key, "tenant1/m/2026/05/07/04/");
    }

    #[test]
    fn from_env_requires_bucket() {
        // Don't pollute global env in a unit test; just exercise the
        // missing-var path.
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
