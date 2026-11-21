//! The hot tier of the materialized view store
//! (docs/design/tiered-view-storage.md): decoded view windows shared by
//! every query, bounded by a byte budget and an idle time, evicted by
//! CLOCK (second chance). A window read cold is decoded once (single
//! flight) and inserted; the committer inserts each window it commits.
//!
//! Interface only: not implemented yet, so every method panics.

use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::values::Batch;
use asap_executor::Error;

/// One view's window, by the end of its range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WindowKey {
    pub view: PhysicalASAPNodeId,
    pub end_ms: i64,
}

/// What the cache holds and has done.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub bytes: u64,
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    /// Windows decoded by `get_or_load` (one per miss that loaded).
    pub loads: u64,
}

/// Decoded view windows, shared by every query.
pub struct WindowCache {
    _max_bytes: Option<u64>,
    _idle: Option<Duration>,
}

const NOT_IMPLEMENTED: &str =
    "the window cache is not implemented (docs/design/tiered-view-storage.md)";

impl WindowCache {
    /// A cache holding at most `max_bytes` (`None`: no budget) and
    /// dropping an entry not read for `idle` (`None`: never).
    pub fn new(max_bytes: Option<u64>, idle: Option<Duration>) -> Self {
        let _ = (max_bytes, idle);
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// Inserts `key`'s decoded `batches` of `bytes`, read at `now`, evicting
    /// to stay within the target; not kept if the window alone exceeds it.
    pub fn insert(&self, key: WindowKey, batches: Arc<Vec<Batch>>, bytes: u64, now: Instant) {
        let _ = (key, batches, bytes, now);
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// `key`'s batches if cached, marking them read at `now`.
    pub fn get(&self, key: WindowKey, now: Instant) -> Option<Arc<Vec<Batch>>> {
        let _ = (key, now);
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// `key`'s batches: cached, or decoded by `load` (its batches and
    /// bytes) and inserted. Concurrent callers for one key run `load` once
    /// and share its result or error; an error is not kept.
    pub fn get_or_load(
        &self,
        key: WindowKey,
        now: Instant,
        load: impl FnOnce() -> Result<(Arc<Vec<Batch>>, u64), Error>,
    ) -> Result<Arc<Vec<Batch>>, Error> {
        let _ = (key, now, load);
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// Drops the entries not read since `now - idle`.
    pub fn evict_idle(&self, now: Instant) {
        let _ = now;
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// Sets the bytes the cache may hold now (`None`: the budget) and
    /// evicts down to it; the memory monitor lowers it under pressure.
    pub fn set_target_bytes(&self, target: Option<u64>) {
        let _ = target;
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    /// Drops `key` (its window left the store: past retention).
    pub fn remove(&self, key: WindowKey) {
        let _ = key;
        unimplemented!("{NOT_IMPLEMENTED}")
    }

    pub fn stats(&self) -> CacheStats {
        unimplemented!("{NOT_IMPLEMENTED}")
    }
}

#[cfg(test)]
mod tests {
    //! Tests V4, V5 and V6 of docs/design/tiered-view-storage.md; they fail
    //! until the cache is implemented.
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn key(end_ms: i64) -> WindowKey {
        WindowKey { view: 1, end_ms }
    }

    fn batches() -> Arc<Vec<Batch>> {
        Arc::new(Vec::new())
    }

    /// V4. Entries read within the idle time stay; entries not read for
    /// longer leave, even with room in the budget.
    #[test]
    fn idle_entries_leave_and_read_ones_stay() {
        let cache = WindowCache::new(None, Some(Duration::from_secs(30)));
        let t0 = Instant::now();
        for end in 0..4 {
            cache.insert(key(end), batches(), 100, t0);
        }
        // Windows 0 and 1 are read every 15 s, as a periodic query does.
        for step in 1..=4 {
            let now = t0 + Duration::from_secs(15 * step);
            assert!(cache.get(key(0), now).is_some());
            assert!(cache.get(key(1), now).is_some());
            cache.evict_idle(now);
        }
        let now = t0 + Duration::from_secs(60);
        assert!(cache.get(key(0), now).is_some());
        assert!(cache.get(key(1), now).is_some());
        assert!(cache.get(key(2), now).is_none(), "an idle entry stayed");
        assert!(cache.get(key(3), now).is_none(), "an idle entry stayed");
        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().bytes, 200);
    }

    /// V4. With no idle time, nothing leaves for being idle.
    #[test]
    fn no_idle_time_keeps_everything() {
        let cache = WindowCache::new(None, None);
        let t0 = Instant::now();
        cache.insert(key(0), batches(), 100, t0);
        cache.evict_idle(t0 + Duration::from_secs(3600));
        assert!(cache.get(key(0), t0 + Duration::from_secs(3600)).is_some());
    }

    /// V5. The cache never holds more than its budget; recently read
    /// entries survive eviction (second chance); every read is answered.
    #[test]
    fn the_byte_budget_holds() {
        let cache = WindowCache::new(Some(300), None);
        let now = Instant::now();
        for end in 0..10 {
            cache.insert(key(end), batches(), 100, now);
            assert!(
                cache.stats().bytes <= 300,
                "over budget after inserting {end}"
            );
            // Window 0 is read after every insert, so it keeps its place.
            assert!(
                cache.get(key(0), now).is_some(),
                "a read window was evicted at {end}"
            );
        }
        let mut loads = 0;
        for end in 0..10 {
            let read = cache.get_or_load(key(end), now, || {
                loads += 1;
                Ok((batches(), 100))
            });
            assert!(read.is_ok());
            assert!(cache.stats().bytes <= 300);
        }
        let stats = cache.stats();
        assert_eq!(stats.loads, loads);
        assert!(stats.evictions >= 7);
        assert!(stats.misses >= loads);
    }

    /// V5. A window larger than the target is returned but not kept; a
    /// lowered target evicts down to it.
    #[test]
    fn oversized_windows_are_not_kept_and_targets_lower() {
        let cache = WindowCache::new(Some(300), None);
        let now = Instant::now();
        let read = cache
            .get_or_load(key(0), now, || Ok((batches(), 1_000)))
            .unwrap();
        assert!(read.is_empty());
        assert_eq!(cache.stats().entries, 0);
        for end in 1..=3 {
            cache.insert(key(end), batches(), 100, now);
        }
        cache.set_target_bytes(Some(0));
        assert_eq!(cache.stats().bytes, 0);
        cache.set_target_bytes(None);
        cache.insert(key(4), batches(), 100, now);
        assert_eq!(cache.stats().bytes, 100);
    }

    /// V6. Eight readers of one cold window decode it once and share it;
    /// a failed decode reaches every waiter and is not kept.
    #[test]
    fn concurrent_reads_decode_once() {
        let cache = Arc::new(WindowCache::new(None, None));
        let decodes = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let readers: Vec<_> = (0..8)
            .map(|_| {
                let (cache, decodes, barrier) = (cache.clone(), decodes.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    cache
                        .get_or_load(key(0), Instant::now(), || {
                            decodes.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(100));
                            Ok((batches(), 100))
                        })
                        .unwrap()
                })
            })
            .collect();
        let results: Vec<Arc<Vec<Batch>>> =
            readers.into_iter().map(|r| r.join().unwrap()).collect();
        assert_eq!(decodes.load(Ordering::SeqCst), 1);
        assert!(results.iter().all(|r| Arc::ptr_eq(r, &results[0])));
        let failed =
            cache.get_or_load(key(1), Instant::now(), || Err(Error::Invalid("bad".into())));
        assert!(failed.is_err());
        assert!(cache.get(key(1), Instant::now()).is_none());
    }
}
