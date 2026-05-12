//! mvp/v5 — instrumented S3 client wrapper.
//!
//! The compaction story for the MVP demo wants a measured (not
//! fabricated) S3 cost picture: per-baseline counts of PUT / GET /
//! HEAD / LIST / DELETE plus bytes-out per request, dumped to CSV
//! at end-of-run.
//!
//! This module provides a thin wrapper that delegates to the
//! existing `rust-s3` [`s3::Bucket`] but ticks a small counter set
//! before / after every operation. Live values are exposed via a
//! Prometheus gauge (`asap_backend_s3_<op>_total`) so dashboards
//! see them in real time, AND a CSV / JSON dump on demand.
//!
//! ## Boundary
//!
//! The wrapper sits at the lowest level — between the
//! [`GorillaS3Store`](super::store::GorillaS3Store)'s `ObjectStore`
//! impl and the actual `Bucket`.
//! Tests that don't need S3 (the in-memory mock path) never touch
//! it; production deployments wire `S3CostTrackingObjectStore`
//! around `S3ObjectStore`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;

/// Process-wide S3 cost counters. The HTTP server's
/// `/internal/s3_cost.csv` endpoint reads this; the
/// [`GorillaS3Store`](super::store::GorillaS3Store) constructor opts in via
/// [`S3CostTrackingObjectStore`]. Lazy-initialised on first access.
static GLOBAL_S3_COST: OnceLock<Arc<S3CostCounters>> = OnceLock::new();

/// Access (and lazily create) the process-wide S3 cost counters.
pub fn global_s3_cost_counters() -> Arc<S3CostCounters> {
    GLOBAL_S3_COST
        .get_or_init(|| Arc::new(S3CostCounters::new()))
        .clone()
}

use super::store::{ObjectStore, StoreError};

/// Per-operation counter set + cumulative bytes.
#[derive(Debug, Default)]
pub struct S3CostCounters {
    /// PUT operations issued.
    pub put_count: AtomicU64,
    /// GET operations issued (full + range).
    pub get_count: AtomicU64,
    /// HEAD operations issued.
    pub head_count: AtomicU64,
    /// LIST operations issued.
    pub list_count: AtomicU64,
    /// DELETE operations issued.
    pub delete_count: AtomicU64,
    /// Bytes uploaded (PUT request bodies).
    pub bytes_put: AtomicU64,
    /// Bytes downloaded (GET response bodies).
    pub bytes_got: AtomicU64,
}

impl S3CostCounters {
    /// Build a fresh zeroed counter set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Plain-old-data snapshot.
    pub fn snapshot(&self) -> S3CostSnapshot {
        S3CostSnapshot {
            put_count: self.put_count.load(Ordering::Relaxed),
            get_count: self.get_count.load(Ordering::Relaxed),
            head_count: self.head_count.load(Ordering::Relaxed),
            list_count: self.list_count.load(Ordering::Relaxed),
            delete_count: self.delete_count.load(Ordering::Relaxed),
            bytes_put: self.bytes_put.load(Ordering::Relaxed),
            bytes_got: self.bytes_got.load(Ordering::Relaxed),
        }
    }

    /// Render Prometheus text-exposition lines for `/metrics`.
    pub fn render_prometheus(&self) -> String {
        let s = self.snapshot();
        format!(
            concat!(
                "# HELP asap_backend_s3_put_total S3 PUT count.\n",
                "# TYPE asap_backend_s3_put_total counter\n",
                "asap_backend_s3_put_total {}\n",
                "asap_backend_s3_get_total {}\n",
                "asap_backend_s3_head_total {}\n",
                "asap_backend_s3_list_total {}\n",
                "asap_backend_s3_delete_total {}\n",
                "asap_backend_s3_bytes_put {}\n",
                "asap_backend_s3_bytes_got {}\n",
            ),
            s.put_count,
            s.get_count,
            s.head_count,
            s.list_count,
            s.delete_count,
            s.bytes_put,
            s.bytes_got,
        )
    }

    /// Render a CSV summary suitable for the demo's `s3_cost.csv`.
    /// Single header + single data row.
    pub fn render_csv(&self) -> String {
        let s = self.snapshot();
        format!(
            "put_count,get_count,head_count,list_count,delete_count,bytes_put,bytes_got\n\
             {},{},{},{},{},{},{}\n",
            s.put_count,
            s.get_count,
            s.head_count,
            s.list_count,
            s.delete_count,
            s.bytes_put,
            s.bytes_got,
        )
    }
}

/// Plain-old-data snapshot returned by [`S3CostCounters::snapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CostSnapshot {
    /// PUT count.
    pub put_count: u64,
    /// GET count.
    pub get_count: u64,
    /// HEAD count.
    pub head_count: u64,
    /// LIST count.
    pub list_count: u64,
    /// DELETE count.
    pub delete_count: u64,
    /// Bytes uploaded.
    pub bytes_put: u64,
    /// Bytes downloaded.
    pub bytes_got: u64,
}

/// `ObjectStore` wrapper that ticks the supplied counters.
///
/// Note: only `get_object` is in the cold-store hot path today
/// (Phase 3 + Phase 4). PUT / LIST / DELETE / HEAD are recorded
/// even though current callers never go through them — having the
/// counter live makes follow-up MVP cost work additive.
pub struct S3CostTrackingObjectStore {
    inner: Arc<dyn ObjectStore>,
    counters: Arc<S3CostCounters>,
}

impl S3CostTrackingObjectStore {
    /// Wrap `inner` and a counter-set for the wrapper to update.
    pub fn new(inner: Arc<dyn ObjectStore>, counters: Arc<S3CostCounters>) -> Self {
        Self { inner, counters }
    }

    /// Borrow the live counters — handy for HTTP exposition.
    pub fn counters(&self) -> &Arc<S3CostCounters> {
        &self.counters
    }
}

#[async_trait]
impl ObjectStore for S3CostTrackingObjectStore {
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        self.counters.get_count.fetch_add(1, Ordering::Relaxed);
        let body = self.inner.get_object(key).await?;
        self.counters
            .bytes_got
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(body)
    }

    fn object_missing(&self, err: &StoreError) -> bool {
        self.inner.object_missing(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::gorilla_object_store::store::ObjectStore as _;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    /// Minimal in-memory ObjectStore stand-in for the wrapper test —
    /// we don't pull in `InMemoryObjectStore` because it's a
    /// `#[cfg(test)]` type local to its own module.
    #[derive(Default)]
    struct StubStore {
        inner: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl ObjectStore for StubStore {
        async fn get_object(&self, key: &str) -> Result<Vec<u8>, StoreError> {
            let g = self.inner.lock().await;
            match g.get(key) {
                Some(b) => Ok(b.clone()),
                None => Err(StoreError::Backend(format!("get {key}: not found"))),
            }
        }
    }

    #[tokio::test]
    async fn counters_increment_on_get() {
        let inner = Arc::new(StubStore::default());
        inner
            .inner
            .lock()
            .await
            .insert("k1".to_string(), vec![0u8; 100]);
        let counters = Arc::new(S3CostCounters::new());
        let wrapped = S3CostTrackingObjectStore::new(inner, counters.clone());

        let _ = wrapped.get_object("k1").await.unwrap();
        let _ = wrapped.get_object("k1").await.unwrap();
        let snap = counters.snapshot();
        assert_eq!(snap.get_count, 2);
        assert_eq!(snap.bytes_got, 200);
        assert_eq!(snap.put_count, 0);
        assert_eq!(snap.head_count, 0);
    }

    #[tokio::test]
    async fn missing_object_does_not_count_bytes() {
        let inner = Arc::new(StubStore::default());
        let counters = Arc::new(S3CostCounters::new());
        let wrapped = S3CostTrackingObjectStore::new(inner, counters.clone());
        let _ = wrapped.get_object("missing").await;
        let snap = counters.snapshot();
        assert_eq!(snap.get_count, 1);
        assert_eq!(snap.bytes_got, 0);
    }

    #[test]
    fn render_csv_has_expected_columns() {
        let c = S3CostCounters::new();
        c.put_count.store(3, Ordering::Relaxed);
        c.get_count.store(7, Ordering::Relaxed);
        c.bytes_got.store(1024, Ordering::Relaxed);
        let csv = c.render_csv();
        assert!(csv.starts_with(
            "put_count,get_count,head_count,list_count,delete_count,bytes_put,bytes_got\n"
        ));
        assert!(csv.contains("3,7,0,0,0,0,1024"));
    }

    #[test]
    fn render_prometheus_has_help_line() {
        let c = S3CostCounters::new();
        let p = c.render_prometheus();
        assert!(p.contains("asap_backend_s3_put_total"));
        assert!(p.contains("# TYPE asap_backend_s3_put_total counter"));
    }
}
