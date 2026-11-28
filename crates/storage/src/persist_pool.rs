//! The `persist-N` threads: each writes the refreshed windows of its
//! refresh groups to the view files after they are published, and syncs
//! the files every sync interval. The precompute committer queues windows
//! through [`PersistPool`] and never waits for the disk; the queue's
//! length and bytes are the [`PersistBacklog`], exported on `/metrics`.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::values::Batch;
use internal_metrics::Metrics;

use crate::persist::ViewFiles;
use crate::store::WindowSlot;

/// What a persist thread does next.
enum PersistJob {
    /// Write the rows each view got for its group's window `range_ms`;
    /// `bytes` of the queue it releases once written.
    Window {
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        /// Each view's stored window, told where it was written.
        slots: Vec<Option<Arc<WindowSlot>>>,
        bytes: u64,
    },
    /// Done once every window queued to this thread before is written.
    Barrier(Arc<PersistBarrier>),
}

/// An event-time barrier waiting for every persist thread.
struct PersistBarrier {
    remaining: AtomicU64,
    /// Answers the barrier; run by the last persist thread to reach it.
    reached: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl PersistBarrier {
    /// One persist thread reached it; the last one answers.
    fn reached(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            if let Some(reached) = self
                .reached
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                reached();
            }
        }
    }
}

/// The persist queue: refreshed windows queued for the persist threads and
/// not yet written, and their bytes.
#[derive(Debug, Default)]
pub struct PersistBacklog {
    windows: AtomicU64,
    bytes: Mutex<u64>,
}

impl PersistBacklog {
    /// Refreshed windows not yet written to the view files.
    pub fn windows(&self) -> u64 {
        self.windows.load(Ordering::SeqCst)
    }

    /// Bytes of the refreshed windows queued for the persist threads.
    pub fn bytes(&self) -> u64 {
        *self.bytes.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Releases `bytes` of the queue for a window written (or lost).
    fn release(&self, bytes: u64) {
        let mut queued = self.bytes.lock().unwrap_or_else(|e| e.into_inner());
        *queued = queued.saturating_sub(bytes);
        self.windows.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How the persist threads run.
#[derive(Debug, Clone, Default)]
pub struct PersistOptions {
    /// Persist threads, each writing the view files of every
    /// `threads`-th refresh group.
    pub threads: usize,
    /// Bytes queued beyond which further windows are counted
    /// (`asap_view_persist_over_bound_total`).
    pub max_backlog_bytes: u64,
    /// For tests: how long a thread waits before writing each window, as a
    /// slow disk would.
    pub delay: std::time::Duration,
    /// For tests: these views' windows are never written.
    pub unpersistable_views: BTreeSet<PhysicalASAPNodeId>,
}

/// The persist threads, as the precompute thread sees them: refresh group
/// `g`'s windows go to thread `g % threads`, so each file has one writer
/// and its records stay in window order. The queue between precompute and
/// the threads is bounded by bytes.
pub struct PersistPool {
    shards: Vec<Sender<PersistJob>>,
    max_bytes: u64,
    backlog: Arc<PersistBacklog>,
    metrics: Arc<Metrics>,
}

impl PersistPool {
    /// Starts `options.threads` threads `persist-0`, `persist-1`, …, over
    /// `files` split by refresh group, counting their queue in `backlog`.
    pub fn spawn(
        files: ViewFiles,
        options: PersistOptions,
        backlog: Arc<PersistBacklog>,
        metrics: Arc<Metrics>,
    ) -> std::io::Result<Self> {
        let unpersistable = Arc::new(options.unpersistable_views);
        let mut shards = Vec::new();
        for (index, files) in files.split(options.threads).into_iter().enumerate() {
            let (shard, jobs) = channel();
            let (backlog, metrics) = (backlog.clone(), metrics.clone());
            let (delay, unpersistable) = (options.delay, unpersistable.clone());
            std::thread::Builder::new()
                .name(format!("persist-{index}"))
                .spawn(move || {
                    serve_persist(files, jobs, &backlog, &metrics, delay, &unpersistable)
                })?;
            shards.push(shard);
        }
        Ok(Self {
            shards,
            max_bytes: options.max_backlog_bytes,
            backlog,
            metrics,
        })
    }

    /// Queues a refreshed window for its group's persist thread, waiting
    /// while the queue holds more than `max_bytes` (the wait is exported;
    /// a window larger than the bound is still queued into an empty queue).
    pub fn queue(
        &self,
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        slots: Vec<Option<Arc<WindowSlot>>>,
    ) {
        let bytes: u64 = views
            .iter()
            .flat_map(|(_, batches)| batches)
            .map(|batch| batch.memory_bytes() as u64)
            .sum();
        // The committer never waits for the disk: a slow disk leaves more
        // windows `Pending` (in memory, as without tiering) instead of
        // holding refreshes, the watermark and ingest. Windows queued past
        // the bound are counted.
        {
            let mut queued = self.backlog.bytes.lock().unwrap_or_else(|e| e.into_inner());
            if *queued > 0 && *queued + bytes > self.max_bytes {
                self.metrics
                    .view_persist_over_bound
                    .fetch_add(1, Ordering::Relaxed);
            }
            *queued += bytes;
        }
        self.backlog.windows.fetch_add(1, Ordering::SeqCst);
        let job = PersistJob::Window {
            group,
            range_ms,
            views,
            slots,
            bytes,
        };
        if self.shards[group % self.shards.len()].send(job).is_err() {
            tracing::error!(group, "the persist thread has stopped");
            self.backlog.release(bytes);
        }
    }

    /// Runs `reached` once every persist thread has written the windows
    /// queued before.
    pub fn barrier(&self, reached: impl FnOnce() + Send + 'static) {
        let barrier = Arc::new(PersistBarrier {
            remaining: AtomicU64::new(self.shards.len() as u64),
            reached: Mutex::new(Some(Box::new(reached))),
        });
        for shard in &self.shards {
            if shard.send(PersistJob::Barrier(barrier.clone())).is_err() {
                barrier.reached();
            }
        }
    }
}

/// The persist thread: writes each refreshed window's records to the view
/// files, in order, and syncs them every sync interval, also when no window
/// arrives; syncs everything when the precompute thread stops.
fn serve_persist(
    mut files: ViewFiles,
    jobs: Receiver<PersistJob>,
    backlog: &PersistBacklog,
    metrics: &Metrics,
    delay: std::time::Duration,
    unpersistable: &BTreeSet<PhysicalASAPNodeId>,
) {
    let wait = files.sync_interval().clamp(
        std::time::Duration::from_millis(1),
        std::time::Duration::from_secs(3600),
    );
    loop {
        let job = match jobs.recv_timeout(wait) {
            Ok(job) => job,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                match files.sync_if_due() {
                    Ok(ns) => metrics.view_sync_ns.fetch_add(ns, Ordering::Relaxed),
                    Err(error) => {
                        tracing::error!(%error, "syncing the view files failed");
                        0
                    }
                };
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match job {
            PersistJob::Window {
                group,
                range_ms,
                views,
                slots,
                bytes,
            } => {
                // A slow disk, for tests.
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let records: Vec<(PhysicalASAPNodeId, &[Batch])> = views
                    .iter()
                    .filter(|(view, _)| !unpersistable.contains(view))
                    .map(|(view, batches)| (*view, batches.as_slice()))
                    .collect();
                let skipped = (views.len() - records.len()) as u64;
                let files_before = files.bytes();
                let locations = match files.append(group, range_ms, &records) {
                    Ok(written) => {
                        metrics
                            .view_persisted_bytes
                            .fetch_add(written.bytes, Ordering::Relaxed);
                        metrics
                            .view_sync_ns
                            .fetch_add(written.sync_ns, Ordering::Relaxed);
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(written.not_persisted + skipped, Ordering::Relaxed);
                        metrics
                            .view_segments_marked_cold
                            .fetch_add(written.segments_marked_cold, Ordering::Relaxed);
                        written.locations
                    }
                    Err(error) => {
                        tracing::error!(group, ?range_ms, %error, "persisting a window failed");
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(views.len() as u64, Ordering::Relaxed);
                        Vec::new()
                    }
                };
                // Each written window leaves memory (unless the cache keeps
                // it); one that was not written stays.
                for ((view, _), slot) in views.iter().zip(&slots) {
                    let Some(slot) = slot else { continue };
                    match locations.iter().find(|(v, _)| v == view) {
                        Some((_, location)) => slot.stored(location.clone()),
                        None => slot.pin(),
                    }
                }
                // Each view's windows expire at its own retention (the
                // files recorded it when indexing).
                if let Err(error) = files.delete_expired(group, range_ms.1) {
                    tracing::error!(group, %error, "deleting expired view segments failed");
                }
                let files_after = files.bytes();
                if files_after >= files_before {
                    metrics
                        .view_file_bytes
                        .fetch_add(files_after - files_before, Ordering::Relaxed);
                } else {
                    metrics
                        .view_file_bytes
                        .fetch_sub(files_before - files_after, Ordering::Relaxed);
                }
                backlog.release(bytes);
            }
            PersistJob::Barrier(barrier) => barrier.reached(),
        }
    }
    match files.sync() {
        Ok(ns) => {
            metrics.view_sync_ns.fetch_add(ns, Ordering::Relaxed);
        }
        Err(error) => tracing::error!(%error, "syncing the view files failed"),
    }
    tracing::info!("persist stopped");
}
