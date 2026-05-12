//! Postings cache + sidecar fetch helper for the Gorilla archive
//! engine.
//!
//! The on-S3 postings sidecar is a JSON file emitted by the agent
//! `gorillas3processor` alongside each per-hour `index.json`:
//! `<tenant>/<metric>/YYYY/MM/DD/HH/postings-v1.json`. It maps
//! `(label_name, label_value) → [series_id, ...]` so the
//! [`super::archive_query::ExactExecutor`] can prune chunks by `label_hash`
//! without paying the chunk-body GET cost.
//!
//! Step-1 of the JSONL deprecation refactor pulled this code out
//! of `gorilla_s3.rs` so the cache + intersection logic has a
//! single home; the previous co-located version conflated three
//! responsibilities (S3 wiring, postings cache, intersection
//! algebra). With Step-2 (Prometheus-block format + Thanos
//! store-gateway) coming next, splitting now means the Thanos
//! impl can either reuse [`intersect_per_bucket_postings`] as-is
//! or replace it without touching the gorilla store.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use tokio::sync::Mutex;
use tracing::debug;

use asap_gorilla::Postings;

use super::store::{ObjectStore, StoreError};

/// Default LRU capacity for the postings cache. ~1 MiB per
/// postings file, so 64 entries ≈ 64 MiB worst-case.
pub const POSTINGS_CACHE_DEFAULT_CAPACITY: usize = 64;

/// LRU cache for parsed postings sidecars. Keyed by the
/// `postings-v1.json` S3 key (one per `(metric, hour)`).
pub type PostingsCache = Mutex<LruCache<String, Arc<Postings>>>;

/// Build a fresh empty postings cache with capacity `cap` (clamped
/// to ≥ 1).
pub fn new_postings_cache(cap: usize) -> PostingsCache {
    let cap = NonZeroUsize::new(cap.max(1)).unwrap_or(NonZeroUsize::new(1).unwrap());
    Mutex::new(LruCache::new(cap))
}

/// Output of [`intersect_per_bucket_postings`]: the union of
/// series_ids matching every label predicate, plus per-bucket
/// coverage counters used by the engine to decide whether to emit
/// the `postings_missing` quirk on the response.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PostingsHits {
    /// Series ids matching all label predicates, sorted ascending,
    /// deduplicated.
    pub series_ids: Vec<u64>,
    /// Hour buckets in the request window.
    pub buckets_in_range: usize,
    /// Buckets that actually had a `postings-v1.json` sidecar.
    pub buckets_with_postings: usize,
}

impl PostingsHits {
    /// `true` iff at least one hour bucket carried postings —
    /// indicates the postings-aware filter ran on real data and the
    /// caller should trust [`Self::series_ids`] as a complete answer.
    pub fn fully_covered(&self) -> bool {
        self.buckets_in_range > 0 && self.buckets_with_postings == self.buckets_in_range
    }

    /// `true` iff postings were present for every bucket AND at
    /// least one matched series.
    pub fn nonempty_and_complete(&self) -> bool {
        self.fully_covered() && !self.series_ids.is_empty()
    }
}

/// Walk every `postings-v1.json` key in `keys`, fetch + parse via
/// `object_store` (LRU-cached in `cache`), intersect the per-matcher
/// series-id lists within each bucket, and union the results
/// across buckets.
///
/// Empty `matchers` ⇒ returns the union of every series_id across
/// every label in every bucket (the no-predicate short-circuit).
///
/// Cross-bucket join is a UNION (a series might exist in one hour
/// but not the next); intra-bucket intersection across matchers is
/// an AND.
pub async fn intersect_per_bucket_postings(
    object_store: &dyn ObjectStore,
    cache: &PostingsCache,
    keys: &[String],
    matchers: &[(String, String)],
) -> Result<PostingsHits, StoreError> {
    let mut hits = PostingsHits {
        series_ids: Vec::new(),
        buckets_in_range: keys.len(),
        buckets_with_postings: 0,
    };
    let mut union_set: BTreeSet<u64> = BTreeSet::new();

    for key in keys {
        // LRU short-circuit.
        let postings = {
            let mut guard = cache.lock().await;
            guard.get(key).cloned()
        };
        let postings = match postings {
            Some(p) => Some(p),
            None => match object_store.get_object(key).await {
                Ok(bytes) => match Postings::read(bytes.as_slice()) {
                    Ok(p) => {
                        let arc = Arc::new(p);
                        let mut guard = cache.lock().await;
                        guard.put(key.clone(), arc.clone());
                        Some(arc)
                    }
                    Err(e) => {
                        // Treat a corrupt postings file as
                        // "missing" — the engine then falls
                        // through to the scan-all path with
                        // the postings_missing quirk.
                        debug!(
                            key = %key,
                            error = %e,
                            "gorilla-engine: postings parse failed; treating as missing"
                        );
                        None
                    }
                },
                Err(e) if object_store.object_missing(&e) => {
                    debug!(key = %key, "gorilla-engine: postings missing for hour bucket");
                    None
                }
                Err(e) => return Err(e),
            },
        };
        let Some(postings) = postings else { continue };
        hits.buckets_with_postings += 1;

        // Intersect across matchers within this bucket.
        let bucket_set: BTreeSet<u64> = if matchers.is_empty() {
            // Union of every series_id across every label.
            let mut set = BTreeSet::new();
            for by_value in postings.by_label.values() {
                for ids in by_value.values() {
                    set.extend(ids.iter().copied());
                }
            }
            set
        } else {
            let first = postings.lookup(&matchers[0].0, &matchers[0].1);
            let mut acc: BTreeSet<u64> = first.iter().copied().collect();
            for (label_name, label_value) in &matchers[1..] {
                let next = postings.lookup(label_name, label_value);
                let next_set: BTreeSet<u64> = next.iter().copied().collect();
                acc = acc.intersection(&next_set).copied().collect();
            }
            acc
        };
        union_set.extend(bucket_set);
    }
    hits.series_ids = union_set.into_iter().collect();
    Ok(hits)
}
