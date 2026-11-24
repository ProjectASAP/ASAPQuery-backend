//! The hot tier of the materialized view store
//! (docs/design/tiered-view-storage.md): decoded view windows shared by
//! every query, bounded by a byte budget and an idle time, evicted by
//! CLOCK (second chance). A window read cold is decoded once (single
//! flight) and inserted; a window is inserted when it is written.
//!
//! One lock guards the map; it is held only for lookups and bookkeeping,
//! never while a window is decoded.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
}

struct Entry {
    batches: Arc<Vec<Batch>>,
    bytes: u64,
    /// Read since the CLOCK hand last passed it.
    referenced: bool,
    last_read: Instant,
}

/// A decode in flight: its result once done (an error as text).
type Loading = Arc<(Mutex<Option<Result<Arc<Vec<Batch>>, String>>>, Condvar)>;

#[derive(Default)]
struct Inner {
    entries: HashMap<WindowKey, Entry>,
    /// The CLOCK ring: keys in insertion order, the hand at the front.
    ring: VecDeque<WindowKey>,
    bytes: u64,
    /// The bytes the cache may hold now (`None`: unbounded).
    target: Option<u64>,
    loading: HashMap<WindowKey, Loading>,
    /// Keys removed while their decode was in flight: not inserted when it
    /// finishes.
    removed_while_loading: HashSet<WindowKey>,
}

/// Decoded view windows, shared by every query.
pub struct WindowCache {
    max_bytes: Option<u64>,
    idle: Option<Duration>,
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl Inner {
    /// Evicts by CLOCK until the cache holds at most `target` bytes.
    fn evict_to(&mut self, target: u64, evictions: &AtomicU64) {
        let mut passes = 2 * self.ring.len() + 1;
        while self.bytes > target && passes > 0 {
            passes -= 1;
            let Some(key) = self.ring.pop_front() else {
                break;
            };
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            if entry.referenced {
                entry.referenced = false;
                self.ring.push_back(key);
                continue;
            }
            let entry = self.entries.remove(&key).expect("present");
            self.bytes -= entry.bytes;
            evictions.fetch_add(1, Ordering::Relaxed);
        }
        // Every entry was referenced twice over: drop from the front.
        while self.bytes > target {
            let Some(key) = self.ring.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&key) {
                self.bytes -= entry.bytes;
                evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn insert(
        &mut self,
        key: WindowKey,
        batches: Arc<Vec<Batch>>,
        bytes: u64,
        now: Instant,
        evictions: &AtomicU64,
    ) {
        let target = self.target.unwrap_or(u64::MAX);
        if bytes > target {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.bytes -= old.bytes;
        } else {
            self.ring.push_back(key);
        }
        self.entries.insert(
            key,
            Entry {
                batches,
                bytes,
                referenced: false,
                last_read: now,
            },
        );
        self.bytes += bytes;
        if self.bytes > target {
            // The new entry is passed over once: it was just read.
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.referenced = true;
            }
            self.evict_to(target, evictions);
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.referenced = false;
            }
        }
    }
}

impl WindowCache {
    /// A cache holding at most `max_bytes` (`None`: no budget) and
    /// dropping an entry not read for `idle` (`None`: never).
    pub fn new(max_bytes: Option<u64>, idle: Option<Duration>) -> Self {
        Self {
            max_bytes,
            idle: idle.filter(|idle| !idle.is_zero()),
            inner: Mutex::new(Inner {
                target: max_bytes,
                ..Inner::default()
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// The budget (`None`: none).
    pub fn max_bytes(&self) -> Option<u64> {
        self.max_bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Inserts `key`'s decoded `batches` of `bytes`, read at `now`, evicting
    /// to stay within the target; not kept if the window alone exceeds it.
    pub fn insert(&self, key: WindowKey, batches: Arc<Vec<Batch>>, bytes: u64, now: Instant) {
        self.lock()
            .insert(key, batches, bytes, now, &self.evictions);
    }

    /// `key`'s batches if cached, marking them read at `now`.
    pub fn get(&self, key: WindowKey, now: Instant) -> Option<Arc<Vec<Batch>>> {
        let mut inner = self.lock();
        match inner.entries.get_mut(&key) {
            Some(entry) => {
                entry.referenced = true;
                entry.last_read = now;
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.batches.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
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
        let (loading, loader) = {
            let mut inner = self.lock();
            if let Some(entry) = inner.entries.get_mut(&key) {
                entry.referenced = true;
                entry.last_read = now;
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.batches.clone());
            }
            self.misses.fetch_add(1, Ordering::Relaxed);
            match inner.loading.get(&key) {
                Some(loading) => (loading.clone(), false),
                None => {
                    let loading: Loading = Arc::default();
                    inner.loading.insert(key, loading.clone());
                    (loading, true)
                }
            }
        };
        if !loader {
            let (result, done) = &*loading;
            let mut result = result.lock().unwrap_or_else(|e| e.into_inner());
            while result.is_none() {
                result = done.wait(result).unwrap_or_else(|e| e.into_inner());
            }
            return result.clone().expect("set").map_err(Error::Invalid);
        }
        // A panic in `load` must not leave waiters waiting.
        let loaded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(load))
            .unwrap_or_else(|_| Err(Error::Invalid("decoding a view window panicked".into())));
        let shared = loaded
            .as_ref()
            .map(|(batches, _)| batches.clone())
            .map_err(|e| e.to_string());
        {
            let mut inner = self.lock();
            inner.loading.remove(&key);
            let removed = inner.removed_while_loading.remove(&key);
            if let (Ok((batches, bytes)), false) = (&loaded, removed) {
                inner.insert(key, batches.clone(), *bytes, now, &self.evictions);
                if let Some(entry) = inner.entries.get_mut(&key) {
                    entry.referenced = true;
                }
            }
        }
        let (result, done) = &*loading;
        *result.lock().unwrap_or_else(|e| e.into_inner()) = Some(shared);
        done.notify_all();
        loaded.map(|(batches, _)| batches)
    }

    /// Drops the entries not read since `now - idle`.
    pub fn evict_idle(&self, now: Instant) {
        let Some(idle) = self.idle else {
            return;
        };
        let mut inner = self.lock();
        let idle_keys: Vec<WindowKey> = inner
            .entries
            .iter()
            .filter(|(_, entry)| now.saturating_duration_since(entry.last_read) > idle)
            .map(|(key, _)| *key)
            .collect();
        for key in idle_keys {
            if let Some(entry) = inner.entries.remove(&key) {
                inner.bytes -= entry.bytes;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
        let Inner { ring, entries, .. } = &mut *inner;
        ring.retain(|key| entries.contains_key(key));
    }

    /// Sets the bytes the cache may hold now (`None`: the budget) and
    /// evicts down to it; the memory monitor lowers it under pressure.
    pub fn set_target_bytes(&self, target: Option<u64>) {
        let target = match (target, self.max_bytes) {
            (Some(target), Some(max)) => Some(target.min(max)),
            (Some(target), None) => Some(target),
            (None, max) => max,
        };
        let mut inner = self.lock();
        inner.target = target;
        if let Some(target) = target {
            inner.evict_to(target, &self.evictions);
        }
    }

    /// Drops `key` (its window left the store: past retention).
    pub fn remove(&self, key: WindowKey) {
        let mut inner = self.lock();
        if let Some(entry) = inner.entries.remove(&key) {
            inner.bytes -= entry.bytes;
            inner.ring.retain(|k| *k != key);
        }
        if inner.loading.contains_key(&key) {
            inner.removed_while_loading.insert(key);
        }
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.lock();
        CacheStats {
            bytes: inner.bytes,
            entries: inner.entries.len() as u64,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
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

    /// A window removed (past retention) while its decode is in flight is
    /// not inserted when the decode finishes, and a removed key leaves the
    /// CLOCK ring.
    #[test]
    fn a_remove_during_a_load_is_not_undone() {
        let cache = WindowCache::new(None, None);
        let t0 = Instant::now();
        cache
            .get_or_load(key(0), t0, || {
                cache.remove(key(0));
                Ok((batches(), 100))
            })
            .unwrap();
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().bytes, 0);
        cache.insert(key(1), batches(), 100, t0);
        cache.remove(key(1));
        assert!(cache.lock().ring.is_empty());
        // Loaded again later, it is kept.
        cache
            .get_or_load(key(0), t0, || Ok((batches(), 100)))
            .unwrap();
        assert_eq!(cache.stats().entries, 1);
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
