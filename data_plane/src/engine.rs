//! The engine: the installed plan, compiled once when it is installed,
//! and the threads that execute it:
//!
//! - `ingest` decodes nothing (Remote Write is decoded on the HTTP side):
//!   it appends samples to its copy of the raw data, advances the
//!   watermark, publishes the raw data and hands each closed refresh
//!   window, with the raw data as it was when the window closed, to
//! - `precompute`, which runs the window's ingestion run, appends the
//!   views' rows to its copy of the views and publishes them;
//! - `query-N` workers each run one query to completion over the newest
//!   published snapshot, so a query never waits for ingestion. A run's
//!   output stream is not `Send`, so a run never moves between threads.
//!
//! Windows closed and not yet refreshed (the precompute backlog) are
//! bounded by `max_precompute_backlog`: when it is full, the ingest thread
//! waits, its queue fills and Remote Write gets 503.
//!
//! Each thread records the thread CPU and wall time of its phases in
//! [`Metrics`], exported on `/metrics`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};

use asap_executor::runtime::Limits;
use asap_planner::InstallablePlan;

use tokio::sync::oneshot;

use crate::ingestion::{ClosedWindow, Ingestion, IngestionOptions, IngestionStats, Precompute};
use crate::metrics::{Gauges, Metrics, Timer};
use crate::query::{QueryEngine, ResultSample, Unanswered};
use crate::store::{PublishedStore, RawData, RawSample, RunStore, ViewStore};

/// How the engine ingests samples and runs queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and first-window checks of ingestion.
    pub ingestion: IngestionOptions,
    /// Samples queued for the ingest thread, not yet ingested, beyond which
    /// Remote Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
    /// Refresh windows closed and not yet refreshed beyond which the
    /// ingest thread waits for the precompute thread.
    pub max_precompute_backlog: usize,
    /// Every answer must be computed from the plan: a plan with a query the
    /// query engine cannot run is refused at install, and the data plane
    /// never forwards a query to Prometheus (`--no-forward`).
    pub no_forward: bool,
    /// The most bytes one ingestion or query run may hold
    /// (`asap_executor::runtime::Limits::max_bytes`).
    pub run_memory_limit_bytes: usize,
    /// Query worker threads: the most queries run at once. Each run may
    /// hold `run_memory_limit_bytes`, so queries may hold up to this many
    /// times that.
    pub query_threads: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            ingestion: IngestionOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
            max_precompute_backlog: 256,
            no_forward: false,
            run_memory_limit_bytes: 4 << 30,
            query_threads: std::thread::available_parallelism().map_or(1, usize::from),
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

/// What the ingest thread does next.
enum Command {
    Ingest(Vec<RawSample>),
    AdvanceWatermark {
        barrier_ms: i64,
        reply: oneshot::Sender<i64>,
    },
    Stop,
}

/// What the precompute thread does next.
enum PrecomputeJob {
    Refresh {
        window: ClosedWindow,
        raw_data: Arc<RawData>,
    },
    /// Answers `barrier_ms` once every window queued before is refreshed.
    Barrier {
        barrier_ms: i64,
        reply: oneshot::Sender<i64>,
    },
}

/// Why the engine did not take samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// More samples are queued than `max_queued_samples`; retry later.
    #[error("the engine is busy: too many samples queued")]
    Busy,
    /// The ingest thread has stopped; no sample will be ingested again.
    #[error("the engine has stopped")]
    Stopped,
}

/// The HTTP side of the engine.
#[derive(Clone)]
pub struct EngineHandle {
    commands: Sender<Command>,
    /// Queries for the worker threads.
    queries: Sender<QueryJob>,
    shared: Arc<Shared>,
    max_queued_samples: u64,
}

/// What the engine's threads publish to the HTTP side and to each other.
struct Shared {
    /// Samples sent to the ingest thread and not yet ingested.
    queued_samples: AtomicU64,
    /// Windows closed and not yet refreshed.
    precompute_backlog: AtomicU64,
    /// False once the ingest or precompute thread has exited, for any
    /// reason.
    running: Arc<AtomicBool>,
    /// The ingestion counters: samples and skipped windows as of the last
    /// command the ingest thread ran, refreshed and failed windows as the
    /// precompute thread counts them.
    stats: Mutex<IngestionStats>,
    published: PublishedStore,
    /// Held by the precompute thread during each refresh; tests hold it to
    /// keep a refresh from finishing.
    precompute_gate: Mutex<()>,
    metrics: Metrics,
}

/// Marks the engine stopped when its thread exits, panics included.
struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl EngineHandle {
    /// Starts the engine for the plan file `plan_json`, read, checked and
    /// compiled once, and returns the plan's queries. Fails if the plan is
    /// invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let (ingestion, precompute, query, query_texts) = install(&plan_json, options)?;
        let raw_data = Arc::new(ingestion.raw_data());
        let groups: Vec<(i64, Vec<String>)> = precompute
            .groups()
            .into_iter()
            .map(|(width_ms, views)| (width_ms, views.iter().map(ToString::to_string).collect()))
            .collect();
        let metrics = Metrics::new(&groups, &query_texts);
        let running = Arc::new(AtomicBool::new(true));
        let shared = Arc::new(Shared {
            queued_samples: AtomicU64::new(0),
            precompute_backlog: AtomicU64::new(0),
            running: running.clone(),
            stats: Mutex::new(IngestionStats::default()),
            published: PublishedStore::new(raw_data.clone()),
            precompute_gate: Mutex::new(()),
            metrics,
        });
        let (commands, command_receiver) = channel();
        let (jobs, job_receiver) = sync_channel(options.max_precompute_backlog.max(1));
        let (queries, query_receiver) = channel::<QueryJob>();

        let ingest_guard = RunningGuard(running.clone());
        let ingest_shared = shared.clone();
        std::thread::Builder::new()
            .name("ingest".into())
            .spawn(move || {
                let _guard = ingest_guard;
                serve_ingest(ingestion, raw_data, command_receiver, jobs, &ingest_shared);
            })?;
        let precompute_guard = RunningGuard(running);
        let precompute_shared = shared.clone();
        std::thread::Builder::new()
            .name("precompute".into())
            .spawn(move || {
                let _guard = precompute_guard;
                serve_precompute(precompute, job_receiver, &precompute_shared);
            })?;
        spawn_query_workers(
            Arc::new(query),
            shared.clone(),
            query_receiver,
            options.query_threads.max(1),
        )?;
        let handle = Self {
            commands,
            queries,
            shared,
            max_queued_samples: options.max_queued_samples,
        };
        Ok((handle, query_texts))
    }

    /// Queues `samples` for ingestion. `Busy` when the queue would hold
    /// more than `max_queued_samples`; `Stopped` when the engine has exited.
    pub fn try_ingest(&self, samples: Vec<RawSample>) -> Result<(), IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        let queued_samples = &self.shared.queued_samples;
        let count = samples.len() as u64;
        let mut queued = queued_samples.load(Ordering::SeqCst);
        loop {
            let total = admit(queued, count, self.max_queued_samples)?;
            match queued_samples.compare_exchange(queued, total, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(current) => queued = current,
            }
        }
        self.commands.send(Command::Ingest(samples)).map_err(|_| {
            queued_samples.fetch_sub(count, Ordering::SeqCst);
            IngestError::Stopped
        })
    }

    /// Answers the planned `query` at `time_ms` on a query worker, from
    /// the newest snapshot published when the run starts; see
    /// [`QueryEngine::evaluate`]. Two queries at one evaluation time may
    /// read different snapshots. A query does not wait for writes
    /// acknowledged before it; an event-time barrier does
    /// ([`Self::advance_watermark`]).
    pub async fn evaluate(
        &self,
        query: String,
        time_ms: i64,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        let stopped = || Unanswered::Execution("the query workers have stopped".into());
        let started = std::time::Instant::now();
        let metrics = self.shared.metrics.query(&query);
        let (reply, answer) = oneshot::channel();
        self.queries
            .send(QueryJob {
                query,
                time_ms,
                reply,
            })
            .map_err(|_| stopped())?;
        let answer = answer.await.map_err(|_| stopped())?;
        if let Some(metrics) = metrics {
            metrics.latency.observe(started.elapsed().as_secs_f64());
        }
        answer
    }

    /// The engine's measurements.
    pub fn metrics(&self) -> &Metrics {
        &self.shared.metrics
    }

    /// The engine's measurements and counters in the Prometheus text
    /// format.
    pub fn render_metrics(&self) -> String {
        crate::metrics::render(
            &self.shared.metrics,
            &Gauges {
                stats: self.stats(),
                samples_queued: self.queued_samples(),
                precompute_backlog: self.precompute_backlog(),
                now_ms: crate::ingestion::wall_clock_ms(),
            },
        )
    }

    /// Whether the ingest and precompute threads are still running.
    pub fn is_running(&self) -> bool {
        self.shared.running.load(Ordering::SeqCst)
    }

    /// Samples queued and not yet ingested.
    pub fn queued_samples(&self) -> u64 {
        self.shared.queued_samples.load(Ordering::SeqCst)
    }

    /// Refresh windows closed and not yet refreshed.
    pub fn precompute_backlog(&self) -> u64 {
        self.shared.precompute_backlog.load(Ordering::SeqCst)
    }

    /// The ingestion counters.
    pub fn stats(&self) -> IngestionStats {
        *self.shared.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queues an event-time barrier behind every write already acknowledged
    /// and waits until it has run and every window it closed is refreshed:
    /// every window ending at or before `barrier_ms` (capped at the wall
    /// clock) is then answered from. Returns the barrier in force; see
    /// [`Ingestion::advance_watermark_at`].
    pub async fn advance_watermark(&self, barrier_ms: i64) -> Result<i64, IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        let (reply, done) = oneshot::channel();
        self.commands
            .send(Command::AdvanceWatermark { barrier_ms, reply })
            .map_err(|_| IngestError::Stopped)?;
        done.await.map_err(|_| IngestError::Stopped)
    }

    /// For tests: stops the ingest thread after the commands queued before,
    /// and the precompute thread once it has refreshed the windows queued
    /// before. Samples queued between this call and the stop are
    /// acknowledged and then lost; ingestion after the stop fails with
    /// [`IngestError::Stopped`].
    #[doc(hidden)]
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
    }

    /// For tests: no refresh finishes while the guard is held.
    #[doc(hidden)]
    pub fn hold_precompute(&self) -> MutexGuard<'_, ()> {
        self.shared
            .precompute_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// The queue length after admitting `count` samples to a queue of
/// `queued`: admitted while the total stays within `max_queued`, and always
/// into an empty queue, so a bound smaller than one request cannot refuse
/// that request forever.
fn admit(queued: u64, count: u64, max_queued: u64) -> Result<u64, IngestError> {
    let total = queued.checked_add(count).ok_or(IngestError::Busy)?;
    if queued == 0 || total <= max_queued {
        Ok(total)
    } else {
        Err(IngestError::Busy)
    }
}

/// One query for a worker thread.
struct QueryJob {
    query: String,
    time_ms: i64,
    reply: Reply<Vec<ResultSample>>,
}

/// What crosses threads: the compiled queries and refresh runs, the store
/// snapshots runs read, and what the threads share.
#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn assert_send<T: Send>() {}
const _: fn() = || {
    assert_send_sync::<QueryEngine>();
    assert_send_sync::<Shared>();
    assert_send_sync::<RunStore>();
    assert_send_sync::<RawData>();
    assert_send_sync::<ViewStore>();
    assert_send::<Precompute>();
    assert_send::<Ingestion>();
};

/// Starts `threads` workers `query-0`, `query-1`, …, each running one query
/// at a time to completion from `jobs`; more queries wait in `jobs`.
fn spawn_query_workers(
    query: Arc<QueryEngine>,
    shared: Arc<Shared>,
    jobs: Receiver<QueryJob>,
    threads: usize,
) -> std::io::Result<()> {
    let jobs = Arc::new(Mutex::new(jobs));
    for index in 0..threads {
        let (query, shared, jobs) = (query.clone(), shared.clone(), jobs.clone());
        std::thread::Builder::new()
            .name(format!("query-{index}"))
            .spawn(move || loop {
                let job = match jobs.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                    Ok(job) => job,
                    Err(_) => break,
                };
                let timer = Timer::start();
                let answer = query.evaluate(&job.query, job.time_ms, &shared.published);
                if let Some(metrics) = shared.metrics.query(&job.query) {
                    metrics.run.record(timer);
                }
                let _ = job.reply.send(answer);
            })?;
    }
    Ok(())
}

/// Reads, checks and compiles the plan file `plan_json`: its ingestion,
/// refresh runs, queries and query texts.
fn install(
    plan_json: &str,
    options: EngineOptions,
) -> anyhow::Result<(Ingestion, Precompute, QueryEngine, Vec<String>)> {
    let plan = InstallablePlan::from_json(plan_json)?;
    plan.validate()?;
    let views = plan.materialized_views()?;
    let groups = plan.ingestion_refresh_groups()?;
    let limits = Limits {
        max_bytes: options.run_memory_limit_bytes,
    };
    tracing::info!(
        views = views.len(),
        refresh_groups = groups.len(),
        "materialized views"
    );
    let query = QueryEngine::new(&plan, &views, limits.clone());
    let unexecutable = query.unexecutable();
    if options.no_forward && !unexecutable.is_empty() {
        let reasons: Vec<String> = unexecutable
            .iter()
            .map(|(query, reason)| format!("{query:?}: {reason}"))
            .collect();
        anyhow::bail!(
            "--no-forward refuses a plan with queries the data plane cannot run: {}",
            reasons.join("; ")
        );
    }
    let (ingestion, precompute) =
        Ingestion::new(&plan.dag, &views, groups, options.ingestion, limits)?;
    tracing::info!(pane_width_ms = ingestion.pane_width_ms(), "raw data panes");
    Ok((ingestion, precompute, query, plan.query_texts.clone()))
}

/// The ingest thread: runs `commands` in order over its copy of the raw
/// data, publishing it after each, and queues the windows each closes.
fn serve_ingest(
    mut ingestion: Ingestion,
    mut raw_data: Arc<RawData>,
    commands: Receiver<Command>,
    jobs: SyncSender<PrecomputeJob>,
    shared: &Shared,
) {
    for command in commands {
        let timer = Timer::start();
        let (count, barrier) = match command {
            Command::Ingest(samples) => {
                let count = samples.len() as u64;
                let closed = ingestion.ingest(Arc::make_mut(&mut raw_data), samples);
                if !queue_refreshes(&closed, &raw_data, &jobs, shared) {
                    break;
                }
                (count, None)
            }
            Command::AdvanceWatermark { barrier_ms, reply } => {
                let (barrier_ms, closed) =
                    ingestion.advance_watermark(Arc::make_mut(&mut raw_data), barrier_ms);
                if !queue_refreshes(&closed, &raw_data, &jobs, shared) {
                    break;
                }
                (0, Some((barrier_ms, reply)))
            }
            Command::Stop => break,
        };
        // The refreshes queued hold the raw data as it was when their
        // windows closed; eviction changes a copy.
        ingestion.evict(Arc::make_mut(&mut raw_data));
        shared.published.publish_raw_data(raw_data.clone());
        let metrics = &shared.metrics;
        if let Some(watermark_ms) = ingestion.watermark_ms() {
            metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
        }
        metrics
            .raw_data_bytes
            .store(raw_data.bytes() as u64, Ordering::Relaxed);
        metrics
            .raw_data_samples
            .store(raw_data.len() as u64, Ordering::Relaxed);
        metrics
            .raw_data_panes
            .store(raw_data.panes() as u64, Ordering::Relaxed);
        // The wait for room in the precompute backlog is not counted.
        if count > 0 {
            metrics.append.record(timer);
        }
        {
            let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
            *stats = IngestionStats {
                windows_refreshed: stats.windows_refreshed,
                windows_failed: stats.windows_failed,
                ..ingestion.stats
            };
        }
        shared.queued_samples.fetch_sub(count, Ordering::SeqCst);
        if let Some((barrier_ms, reply)) = barrier {
            let job = PrecomputeJob::Barrier { barrier_ms, reply };
            if jobs.send(job).is_err() {
                break;
            }
        }
    }
    tracing::warn!("ingest stopped");
}

/// Queues a refresh of each of `closed` over `raw_data`, waiting while the
/// backlog is full; `false` once the precompute thread has exited.
fn queue_refreshes(
    closed: &[ClosedWindow],
    raw_data: &Arc<RawData>,
    jobs: &SyncSender<PrecomputeJob>,
    shared: &Shared,
) -> bool {
    for &window in closed {
        shared.precompute_backlog.fetch_add(1, Ordering::SeqCst);
        let job = PrecomputeJob::Refresh {
            window,
            raw_data: raw_data.clone(),
        };
        if jobs.send(job).is_err() {
            return false;
        }
    }
    true
}

/// The precompute thread: refreshes each queued window in order over its
/// copy of the views, publishing them after each.
fn serve_precompute(precompute: Precompute, jobs: Receiver<PrecomputeJob>, shared: &Shared) {
    let mut views: Arc<ViewStore> = Arc::default();
    for job in jobs {
        match job {
            PrecomputeJob::Refresh { window, raw_data } => {
                let result = {
                    let _gate = shared
                        .precompute_gate
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let timer = Timer::start();
                    let result = precompute.refresh(window, raw_data, &mut views);
                    shared.metrics.precompute[window.group].1.record(timer);
                    result
                };
                shared.published.publish_views(views.clone());
                shared
                    .metrics
                    .view_bytes
                    .store(views.bytes() as u64, Ordering::Relaxed);
                {
                    let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
                    match result {
                        Ok(()) => stats.windows_refreshed += 1,
                        Err(error) => {
                            tracing::error!(group = window.group, end = window.end_ms, %error, "ingestion run failed");
                            stats.windows_failed += 1;
                        }
                    }
                }
                shared.precompute_backlog.fetch_sub(1, Ordering::SeqCst);
            }
            PrecomputeJob::Barrier { barrier_ms, reply } => {
                let _ = reply.send(barrier_ms);
            }
        }
    }
    tracing::warn!("precompute stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request is admitted while the queue stays within its bound, and
    /// always into an empty queue; otherwise it is `Busy`.
    #[test]
    fn admission() {
        assert_eq!(admit(0, 3, 2), Ok(3));
        assert_eq!(admit(1, 1, 2), Ok(2));
        assert_eq!(admit(1, 2, 2), Err(IngestError::Busy));
        assert_eq!(admit(u64::MAX, 1, u64::MAX), Err(IngestError::Busy));
    }

    /// The running flag clears when an engine thread panics.
    #[test]
    fn running_guard_clears_on_panic() {
        let running = Arc::new(AtomicBool::new(true));
        let guard = RunningGuard(running.clone());
        let thread = std::thread::spawn(move || {
            let _guard = guard;
            panic!("engine bug");
        });
        assert!(thread.join().is_err());
        assert!(!running.load(Ordering::SeqCst));
    }
}
