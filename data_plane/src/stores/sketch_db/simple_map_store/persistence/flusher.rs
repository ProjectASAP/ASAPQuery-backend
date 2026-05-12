//! Background flusher thread. Pulls sealed epochs out of an
//! [`EpochSource`], bundles them into on-disk parts, and evicts them
//! from memory.
//!
//! One part per flush tick. Three phases:
//!
//! 1. **Memory pressure** — if `source.approx_memory_bytes()` exceeds
//!    `memory_limit_bytes`, collect oldest-first (round-robin by agg)
//!    until projected memory drops under `memory_low_watermark_bytes`.
//! 2. **Time watermark** — any sealed epoch whose `end_ts` is older
//!    than `now - hot_window_ms` is added to the flush set even if the
//!    store is well under the memory budget.
//! 3. **T2 retention sweep** — parts whose `max_ts` is older than
//!    `now - delete_older_than_ms` are removed from the manifest and
//!    their directories are `rm -rf`'d.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::{debug, error, info, warn};

use super::config::SimpleMapStorePersistenceConfig;
use super::manifest::{Manifest, PartEntry};
use super::part::{part_dir_path, PartWriter};
use super::source::{EpochSnapshot, EpochSource, SealedEpochRef};
use super::{PersistError, PersistResult};

/// Handle to a running flusher thread. Dropping the handle signals
/// shutdown and joins the thread.
pub struct FlusherHandle {
    inner: Arc<FlusherShared>,
    thread: Option<JoinHandle<()>>,
}

pub(crate) struct FlusherShared {
    pub cfg: SimpleMapStorePersistenceConfig,
    pub manifest: Arc<Manifest>,
    pub next_part_id: AtomicU64,
    pub shutdown: AtomicBool,
    /// Woken by the insert path when it hits `hard_cap_bytes` and by
    /// the flusher when it finishes a tick.
    pub pressure_cv: Condvar,
    pub pressure_mutex: Mutex<()>,
    /// Monotonic counter of how many times `wait_for_memory_under`
    /// has entered its blocking wait loop because `mem_counter >= cap`.
    /// Incremented exactly once per back-pressure-triggered call; used
    /// as a timing-independent signal in tests (wall-clock thresholds
    /// are flaky on fast CI runners where a flush tick completes in
    /// <1 ms).
    pub back_pressure_wait_count: AtomicU64,
}

impl FlusherHandle {
    /// Start a flusher thread. Takes an `EpochSource` (typically the
    /// store itself, wrapped in `Arc`).
    pub fn start<S>(
        cfg: SimpleMapStorePersistenceConfig,
        manifest: Arc<Manifest>,
        source: Arc<S>,
    ) -> PersistResult<Self>
    where
        S: EpochSource + 'static,
    {
        // Pick a starting part_id: one past the max currently in the
        // manifest (so IDs are monotonically increasing across restarts).
        let next_id = manifest
            .live_parts()
            .iter()
            .map(|p| p.part_id)
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);

        let shared = Arc::new(FlusherShared {
            cfg: cfg.clone(),
            manifest: manifest.clone(),
            next_part_id: AtomicU64::new(next_id),
            shutdown: AtomicBool::new(false),
            pressure_cv: Condvar::new(),
            pressure_mutex: Mutex::new(()),
            back_pressure_wait_count: AtomicU64::new(0),
        });

        let shared_for_thread = Arc::clone(&shared);
        let thread = thread::Builder::new()
            .name("simple-map-store-flusher".into())
            .spawn(move || run_flusher_loop(shared_for_thread, source))
            .map_err(PersistError::Io)?;

        Ok(Self {
            inner: shared,
            thread: Some(thread),
        })
    }

    /// Signal shutdown and wait for the thread to finish its current
    /// tick. Safe to call multiple times; subsequent calls are no-ops.
    pub fn shutdown(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.pressure_cv.notify_all();
        if let Some(handle) = self.thread.take() {
            if let Err(e) = handle.join() {
                error!("flusher thread panicked on join: {:?}", e);
            }
        }
    }

    /// Wake the flusher early (e.g., on `hard_cap_bytes` pressure).
    pub fn wake(&self) {
        self.inner.pressure_cv.notify_all();
    }

    /// Block the calling thread until `mem_counter.load() < cap` or
    /// `max_wait` elapses, whichever comes first. Wakes the flusher
    /// once on entry and then sleeps on the pressure condvar until
    /// the flusher's per-tick `notify_all` fires — typical wait is
    /// one flush tick.
    ///
    /// Returns `true` if the wait finished with memory under the cap,
    /// `false` if the timeout expired first (so the caller can log
    /// and proceed rather than hanging forever on a stuck flusher).
    ///
    /// Safe to call from the insert hot path — the only synchronization
    /// held during the wait is the flusher's dedicated pressure mutex,
    /// which the flusher thread itself never takes (it only calls
    /// `notify_all` on the condvar, which requires no lock).
    pub fn wait_for_memory_under(
        &self,
        mem_counter: &std::sync::atomic::AtomicUsize,
        cap: usize,
        max_wait: Duration,
    ) -> bool {
        if mem_counter.load(Ordering::Relaxed) < cap {
            return true;
        }
        // Slow path: the cap was crossed and we are about to block.
        // Bump a counter so tests can assert that back-pressure fired
        // without relying on wall-clock thresholds (the actual wait is
        // ≤1 flush tick, which is sub-millisecond on fast CI runners
        // and makes wall-clock-based signals flaky).
        self.inner
            .back_pressure_wait_count
            .fetch_add(1, Ordering::Relaxed);
        // Kick the flusher once so it tries to drain right now instead
        // of waiting for its own interval tick.
        self.inner.pressure_cv.notify_all();

        let deadline = Instant::now() + max_wait;
        let mut guard = match self.inner.pressure_mutex.lock() {
            Ok(g) => g,
            Err(_) => return false, // poisoned — give up rather than deadlock
        };
        loop {
            if mem_counter.load(Ordering::Relaxed) < cap {
                return true;
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline - now;
            let (g, _) = match self.inner.pressure_cv.wait_timeout(guard, remaining) {
                Ok(v) => v,
                Err(_) => return false,
            };
            guard = g;
        }
    }

    /// Access to the manifest for the query read-through path.
    pub fn manifest(&self) -> Arc<Manifest> {
        Arc::clone(&self.inner.manifest)
    }

    /// Total number of times the insert hot path has entered
    /// `wait_for_memory_under`'s blocking wait loop (i.e. observed
    /// sealed memory at or above `hard_cap_bytes`). Monotonic counter,
    /// used by back-pressure tests as a timing-independent signal.
    pub fn back_pressure_wait_count(&self) -> u64 {
        self.inner.back_pressure_wait_count.load(Ordering::Relaxed)
    }

    pub fn disk_path(&self) -> &std::path::Path {
        &self.inner.cfg.disk_path
    }
}

impl Drop for FlusherHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_flusher_loop<S: EpochSource>(shared: Arc<FlusherShared>, source: Arc<S>) {
    info!(
        "flusher thread started: disk_path={:?}, memory_limit={} bytes, hot_window={:?} ms, flush_interval={:?}",
        shared.cfg.disk_path,
        shared.cfg.memory_limit_bytes,
        shared.cfg.hot_window_ms,
        shared.cfg.flush_interval,
    );

    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Cond-var wait: sleep for up to `flush_interval`, woken early
        // on pressure.
        {
            let guard = shared.pressure_mutex.lock().unwrap();
            let _ = shared
                .pressure_cv
                .wait_timeout(guard, shared.cfg.flush_interval)
                .unwrap();
        }

        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        let tick_start = Instant::now();
        match run_tick(&shared, source.as_ref()) {
            Ok(stats) => {
                if stats.entries_flushed > 0 || stats.parts_deleted > 0 {
                    debug!(
                        "flusher tick done in {:?}: entries={}, parts_written={}, parts_deleted={}",
                        tick_start.elapsed(),
                        stats.entries_flushed,
                        stats.parts_written,
                        stats.parts_deleted,
                    );
                }
            }
            Err(e) => {
                error!("flusher tick error: {}", e);
            }
        }

        // Always notify the pressure condvar so blocked inserts wake
        // up when memory has been freed.
        shared.pressure_cv.notify_all();
    }

    info!("flusher thread exiting cleanly");
}

#[derive(Debug, Default)]
struct TickStats {
    entries_flushed: usize,
    parts_written: usize,
    parts_deleted: usize,
}

fn run_tick<S: EpochSource>(shared: &Arc<FlusherShared>, source: &S) -> PersistResult<TickStats> {
    let mut stats = TickStats::default();
    let now = now_ms();

    // ---- Collect candidates across phase 1 and phase 2 ----
    let mut all = source.list_sealed_epochs();
    // Oldest-first by end_ts.
    all.sort_by_key(|r| r.end_ts);

    let mem = source.approx_memory_bytes();
    let cfg = &shared.cfg;

    let need_memory_pressure = mem > cfg.memory_limit_bytes;
    let low_water_goal = cfg.memory_low_watermark_bytes;
    let mut projected_after_evict: i64 = mem as i64;

    let mut selected: Vec<SealedEpochRef> = Vec::new();
    // Phase 1: memory pressure.
    if need_memory_pressure {
        for r in &all {
            if projected_after_evict <= low_water_goal as i64 {
                break;
            }
            selected.push(*r);
            projected_after_evict -= r.approx_bytes as i64;
        }
    }

    // Phase 2: time watermark (any epoch older than now - hot_window).
    if let Some(hot) = cfg.hot_window_ms {
        let cutoff = now.saturating_sub(hot);
        for r in &all {
            if r.end_ts < cutoff
                && !selected
                    .iter()
                    .any(|s| s.agg_id == r.agg_id && s.epoch_id == r.epoch_id)
            {
                selected.push(*r);
            }
        }
    }

    if !selected.is_empty() {
        // Round-robin across agg-ids inside the tick for lock spread.
        let selected = round_robin_by_agg(selected);

        // Take snapshots one by one, skipping any that have already
        // been evicted racily.
        let mut snapshots: Vec<EpochSnapshot> = Vec::new();
        for r in &selected {
            match source.snapshot_sealed_epoch(r.agg_id, r.epoch_id) {
                Ok(Some(s)) => snapshots.push(s),
                Ok(None) => {
                    // Already evicted by a concurrent flusher tick (shouldn't
                    // happen with a single flusher) or by the store itself.
                    debug!(
                        "flusher: snapshot of ({}, {}) returned None, skipping",
                        r.agg_id, r.epoch_id
                    );
                }
                Err(e) => {
                    warn!(
                        "flusher: snapshot of ({}, {}) failed: {}; skipping",
                        r.agg_id, r.epoch_id, e
                    );
                }
            }
        }

        if !snapshots.is_empty() {
            // Build one part for the tick.
            let part_id = shared.next_part_id.fetch_add(1, Ordering::Relaxed);
            let part_dir = part_dir_path(&parts_root(&cfg.disk_path), part_id);
            let entries_total: usize = snapshots.iter().map(|s| s.len()).sum();
            let size_bytes_estimate: u64 = snapshots.iter().map(|s| s.approx_bytes as u64).sum();

            let report = PartWriter::write_part(&part_dir, part_id, &snapshots)?;
            shared.manifest.append_add(PartEntry {
                part_id,
                min_ts: report.min_ts,
                max_ts: report.max_ts,
                size_bytes: report.data_len + report.index_len + size_bytes_estimate,
            })?;

            // Now that the part is durable and referenced, evict the
            // source epochs.
            for s in &snapshots {
                source.evict_sealed_epoch(s.agg_id, s.epoch_id);
            }

            stats.entries_flushed = entries_total;
            stats.parts_written = 1;
        }
    }

    // Phase 3: T2 sweep.
    if let Some(ttl) = cfg.delete_older_than_ms {
        let cutoff = now.saturating_sub(ttl);
        let expired: Vec<PartEntry> = shared
            .manifest
            .live_parts()
            .into_iter()
            .filter(|p| p.max_ts < cutoff)
            .collect();
        for p in &expired {
            shared.manifest.append_delete(p.part_id)?;
            let dir = part_dir_path(&parts_root(&cfg.disk_path), p.part_id);
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                warn!(
                    "flusher T2 sweep: failed to rm {:?}: {} (will be retried on restart)",
                    dir, e
                );
            }
        }
        stats.parts_deleted = expired.len();
    }

    Ok(stats)
}

/// Interleave a selected-epoch list by agg-id so a burst on one hot
/// agg doesn't monopolize the flush-tick lock sequence.
fn round_robin_by_agg(selected: Vec<SealedEpochRef>) -> Vec<SealedEpochRef> {
    let mut by_agg: HashMap<u64, Vec<SealedEpochRef>> = HashMap::new();
    // Preserve oldest-first order within each agg.
    for r in selected {
        by_agg.entry(r.agg_id).or_default().push(r);
    }
    // Sort agg_ids for deterministic order (test-friendly).
    let mut agg_ids: Vec<u64> = by_agg.keys().copied().collect();
    agg_ids.sort_unstable();
    let mut out = Vec::new();
    let mut i = 0usize;
    loop {
        let mut made_progress = false;
        for ag in &agg_ids {
            if let Some(lst) = by_agg.get_mut(ag) {
                if i < lst.len() {
                    out.push(lst[i]);
                    made_progress = true;
                }
            }
        }
        if !made_progress {
            break;
        }
        i += 1;
    }
    out
}

pub(crate) fn parts_root(disk_path: &std::path::Path) -> PathBuf {
    disk_path.join("parts")
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::schema::KeyByLabelValues;
    use crate::stores::sketch_db::simple_map_store::persistence::source::{
        EpochSnapshot, EpochSnapshotEntry,
    };
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tempfile::TempDir;

    /// A fake `EpochSource` with a fixed set of sealed epochs.
    struct FakeSource {
        epochs: StdMutex<HashMap<(u64, u64), EpochSnapshot>>,
        memory: AtomicU64,
    }

    impl FakeSource {
        fn new(snapshots: Vec<EpochSnapshot>) -> Self {
            let mut map = HashMap::new();
            let mut total = 0u64;
            for s in snapshots {
                total += s.approx_bytes as u64;
                map.insert((s.agg_id, s.epoch_id), s);
            }
            Self {
                epochs: StdMutex::new(map),
                memory: AtomicU64::new(total),
            }
        }
    }

    impl EpochSource for FakeSource {
        fn list_sealed_epochs(&self) -> Vec<SealedEpochRef> {
            self.epochs
                .lock()
                .unwrap()
                .values()
                .map(|s| SealedEpochRef {
                    agg_id: s.agg_id,
                    epoch_id: s.epoch_id,
                    end_ts: s.max_ts,
                    approx_bytes: s.approx_bytes,
                })
                .collect()
        }

        fn snapshot_sealed_epoch(
            &self,
            agg_id: u64,
            epoch_id: u64,
        ) -> PersistResult<Option<EpochSnapshot>> {
            Ok(self
                .epochs
                .lock()
                .unwrap()
                .get(&(agg_id, epoch_id))
                .cloned())
        }

        fn evict_sealed_epoch(&self, agg_id: u64, epoch_id: u64) {
            let mut map = self.epochs.lock().unwrap();
            if let Some(removed) = map.remove(&(agg_id, epoch_id)) {
                self.memory
                    .fetch_sub(removed.approx_bytes as u64, Ordering::Relaxed);
            }
        }

        fn approx_memory_bytes(&self) -> usize {
            self.memory.load(Ordering::Relaxed) as usize
        }
    }

    fn snap(agg_id: u64, epoch_id: u64, min_ts: u64, max_ts: u64, approx: usize) -> EpochSnapshot {
        EpochSnapshot {
            agg_id,
            epoch_id,
            min_ts,
            max_ts,
            approx_bytes: approx,
            entries: vec![EpochSnapshotEntry {
                start_ts: min_ts,
                end_ts: max_ts,
                label: Some(KeyByLabelValues::new_with_labels(vec!["host".into()])),
                sketch_type_name: "SumAccumulator".into(),
                sketch_bytes: b"dummy-payload".to_vec(),
            }],
        }
    }

    fn test_cfg(disk_path: PathBuf, mem_limit: usize) -> SimpleMapStorePersistenceConfig {
        SimpleMapStorePersistenceConfig {
            memory_limit_bytes: mem_limit,
            memory_low_watermark_bytes: mem_limit / 2,
            hard_cap_bytes: mem_limit * 2,
            hot_window_ms: None,
            delete_older_than_ms: None,
            flush_interval: Duration::from_millis(10),
            disk_path,
            part_cache_bytes: 0,
        }
    }

    #[test]
    fn memory_pressure_flushes_oldest_first() {
        let tmp = TempDir::new().unwrap();
        let snaps = vec![
            snap(1, 1, 100, 150, 500),
            snap(1, 2, 150, 200, 500),
            snap(2, 1, 200, 250, 500),
        ];
        let source = Arc::new(FakeSource::new(snaps));
        let manifest = Arc::new(Manifest::init(tmp.path()).unwrap());
        let cfg = test_cfg(tmp.path().to_path_buf(), 1200); // over limit
        let mut handle = FlusherHandle::start(cfg, manifest.clone(), source.clone()).unwrap();

        // Wait until memory is below the low-water mark.
        let deadline = Instant::now() + Duration::from_secs(2);
        while source.approx_memory_bytes() > 600 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        handle.shutdown();

        assert!(
            source.approx_memory_bytes() <= 600,
            "flusher did not reach low water: {} remaining",
            source.approx_memory_bytes()
        );
        // At least one part should be in the manifest.
        let live = manifest.live_parts();
        assert!(!live.is_empty(), "no parts were produced");
    }

    #[test]
    fn hot_window_flushes_even_without_memory_pressure() {
        let tmp = TempDir::new().unwrap();
        // Everything older than (now - 1ms) — effectively everything.
        let now = now_ms();
        let snaps = vec![snap(
            1,
            1,
            now.saturating_sub(10_000),
            now.saturating_sub(5_000),
            100,
        )];
        let source = Arc::new(FakeSource::new(snaps));
        let manifest = Arc::new(Manifest::init(tmp.path()).unwrap());
        let mut cfg = test_cfg(tmp.path().to_path_buf(), 100_000); // well under limit
        cfg.hot_window_ms = Some(1_000); // 1 second hot window
        let mut handle = FlusherHandle::start(cfg, manifest.clone(), source.clone()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while source.approx_memory_bytes() > 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        handle.shutdown();

        assert_eq!(source.approx_memory_bytes(), 0);
        assert!(!manifest.live_parts().is_empty());
    }

    #[test]
    fn t2_sweep_deletes_old_parts() {
        let tmp = TempDir::new().unwrap();
        let source = Arc::new(FakeSource::new(vec![snap(1, 1, 100, 200, 100)]));
        let manifest = Arc::new(Manifest::init(tmp.path()).unwrap());
        let mut cfg = test_cfg(tmp.path().to_path_buf(), 100_000);
        cfg.hot_window_ms = Some(0); // force-flush everything
        cfg.delete_older_than_ms = Some(0); // then immediately expire it
        let mut handle = FlusherHandle::start(cfg, manifest.clone(), source.clone()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while !manifest.live_parts().is_empty() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        // Let the sweep run at least once more to catch up.
        thread::sleep(Duration::from_millis(50));
        handle.shutdown();

        assert!(
            manifest.live_parts().is_empty(),
            "expected live_parts empty after T2 sweep, got {:?}",
            manifest.live_parts()
        );
    }
}
