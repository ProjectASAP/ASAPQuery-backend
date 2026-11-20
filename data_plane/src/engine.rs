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
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};

use asap_executor::runtime::Limits;
use asap_planner::InstallablePlan;

use tokio::sync::oneshot;

use crate::ingestion::{
    ClosedWindow, Ingestion, IngestionOptions, IngestionStats, Precompute, Refreshed, Routing,
};
use crate::metrics::{Gauges, Metrics, Timer};
use crate::persist::{plan_hash, ViewFiles};
use crate::query::{QueryEngine, Snapshot, Unanswered};
use crate::result_cache::{Answer, ResultCache};
use crate::store::{
    PublishedStore, QueryTimeResults, RawData, RawSample, RawShards, RunStore, ViewStore,
};
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::values::Batch;

/// How the engine ingests samples and runs queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and first-window checks of ingestion.
    pub ingestion: IngestionOptions,
    /// Samples queued for the ingest thread, not yet ingested, beyond which
    /// Remote Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
    /// Refresh windows closed and not yet refreshed beyond which the
    /// ingest thread waits for the precompute thread.
    pub max_precompute_backlog: usize,
    /// Ingest shard threads: each routes and appends the samples of the
    /// series hashed to it, into its own raw data.
    pub ingest_threads: usize,
    /// For tests: how long every ingest shard but the first waits before
    /// running each batch, so the first runs ahead of the others.
    #[doc(hidden)]
    pub shard_delay_ms: u64,
    /// Persist threads, each writing the view files of every
    /// `persist_threads`-th refresh group.
    pub persist_threads: usize,
    /// Bytes of refreshed windows queued for the persist threads beyond
    /// which the precompute thread waits (the wait is exported).
    pub max_persist_backlog_bytes: u64,
    /// For tests: how long a persist thread waits before writing each
    /// window, as a slow disk would.
    #[doc(hidden)]
    pub persist_delay_ms: u64,
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
    /// Compute each query-time `OneEvaluation` node several queries read
    /// once per evaluation time, as the plan says; `false` computes it in
    /// every query's run (for comparison).
    pub share_one_evaluation_results: bool,
    /// How long a query-time result is kept after its evaluation time,
    /// counted back from the newest evaluation time computed.
    pub query_time_results_retention_ms: u64,
    /// The most bytes of query-time results kept; the oldest are dropped
    /// first.
    pub query_time_results_max_bytes: usize,
    /// Keep each planned query's final answer at each evaluation time
    /// ([`crate::result_cache`]), so a range request recomputes only the
    /// steps not answered before.
    pub result_cache: bool,
    /// The most bytes of answers the result cache keeps; the least
    /// recently used are dropped first.
    pub result_cache_max_bytes: usize,
    /// Where the materialized views are persisted and loaded from at
    /// startup ([`crate::persist`]); `None`: kept in memory only.
    pub view_dir: Option<std::path::PathBuf>,
    /// How often the view files are synced to disk: a refreshed window is
    /// durable within this long. 0 syncs after every window.
    pub view_sync_interval_ms: u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            ingestion: IngestionOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
            max_precompute_backlog: 256,
            ingest_threads: std::thread::available_parallelism()
                .map_or(1, |cores| (usize::from(cores) / 4).max(1)),
            shard_delay_ms: 0,
            persist_threads: std::thread::available_parallelism()
                .map_or(1, |cores| (usize::from(cores) / 4).clamp(1, 4)),
            max_persist_backlog_bytes: 256 << 20,
            persist_delay_ms: 0,
            no_forward: false,
            run_memory_limit_bytes: 4 << 30,
            query_threads: std::thread::available_parallelism().map_or(1, usize::from),
            share_one_evaluation_results: true,
            query_time_results_retention_ms: 60_000,
            query_time_results_max_bytes: 1 << 30,
            result_cache: true,
            result_cache_max_bytes: 256 << 20,
            view_dir: None,
            // VictoriaMetrics' default -inmemoryDataFlushInterval (v1.155.0).
            view_sync_interval_ms: 5_000,
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

/// What the ingest coordinator does next.
enum Command {
    /// A shard routed and appended a batch, or sealed and evicted.
    Report(ShardReport),
    /// A shard has run every message sent to it before a flush.
    /// The shard numbered here has run every message sent to it before a
    /// flush.
    Flushed(usize),
    AdvanceWatermark {
        barrier_ms: i64,
        reply: oneshot::Sender<i64>,
    },
    Stop,
}

/// A shard's state after its last message.
struct ShardReport {
    shard: usize,
    raw_data: Arc<RawData>,
    /// The newest sample the shard holds.
    newest_ms: Option<i64>,
    /// Each metric's first sample, while a group waits for one.
    first_seen: Vec<(Arc<str>, i64)>,
    now_ms: i64,
}

/// What an ingest shard does next.
enum ShardMessage {
    Samples(Vec<RawSample>),
    /// Seal the panes through the watermark and evict by the routing's
    /// cutoffs.
    Maintain {
        routing: Arc<Routing>,
        watermark_ms: Option<i64>,
    },
    /// Report `Flushed` once every message before is run.
    Flush,
}

/// What the precompute thread does next.
enum PrecomputeJob {
    Refresh {
        window: ClosedWindow,
        raw_data: RawShards,
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
    /// Each ingest shard's queue; a sample goes to the shard of its series.
    shards: Vec<Sender<ShardMessage>>,
    /// Queries for the worker threads.
    queries: Sender<QueryJob>,
    query: Arc<QueryEngine>,
    query_threads: usize,
    shared: Arc<Shared>,
    max_queued_samples: u64,
}

/// What the engine's threads publish to the HTTP side and to each other.
struct Shared {
    /// Samples sent to the ingest shards and not yet ingested.
    queued_samples: AtomicU64,
    /// The same, per shard.
    shard_queued: Vec<AtomicU64>,
    /// What the shards route samples by, as the coordinator last published.
    routing: RwLock<Arc<Routing>>,
    /// Held (shared) while a request is sent to its shards, and
    /// exclusively while a flush is sent, so no flush splits a request.
    admission: RwLock<()>,
    /// Windows closed and not yet refreshed.
    precompute_backlog: AtomicU64,
    /// Refreshed windows not yet written to the view files.
    persist_backlog: AtomicU64,
    /// Bytes of the refreshed windows queued for the persist threads.
    persist_queue: Mutex<u64>,
    /// Signalled when the persist threads release queued bytes.
    persist_room: Condvar,
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
        let (mut ingestion, precompute, query, query_texts) = install(&plan_json, &options)?;
        let mut views = ViewStore::default();
        let files = match &options.view_dir {
            Some(dir) => {
                let sync_interval = std::time::Duration::from_millis(options.view_sync_interval_ms);
                let mut files =
                    ViewFiles::open(dir, plan_hash(plan_json.as_bytes()), sync_interval)?;
                let loaded = files.load(&precompute.views(), &mut views)?;
                tracing::info!(dir = %dir.display(), windows = loaded, "loaded persisted views");
                if loaded > 0 {
                    ingestion.restarted();
                }
                Some((files, loaded))
            }
            None => None,
        };
        let shard_count = options.ingest_threads.max(1);
        let raw_shards: Vec<Arc<RawData>> = (0..shard_count)
            .map(|_| Arc::new(ingestion.raw_data()))
            .collect();
        let groups: Vec<(i64, Vec<String>)> = precompute
            .groups()
            .into_iter()
            .map(|(width_ms, views)| (width_ms, views.iter().map(ToString::to_string).collect()))
            .collect();
        let metrics = Metrics::new(&groups, &query_texts);
        let running = Arc::new(AtomicBool::new(true));
        let shared = Arc::new(Shared {
            queued_samples: AtomicU64::new(0),
            shard_queued: (0..shard_count).map(|_| AtomicU64::new(0)).collect(),
            routing: RwLock::new(Arc::new(ingestion.routing())),
            admission: RwLock::new(()),
            precompute_backlog: AtomicU64::new(0),
            persist_backlog: AtomicU64::new(0),
            persist_queue: Mutex::new(0),
            persist_room: Condvar::new(),
            running: running.clone(),
            stats: Mutex::new(IngestionStats::default()),
            published: PublishedStore::new(Arc::new(raw_shards.clone())),
            precompute_gate: Mutex::new(()),
            metrics,
        });
        let (commands, command_receiver) = channel();
        let (jobs, job_receiver) = sync_channel(options.max_precompute_backlog.max(1));
        let (queries, query_receiver) = channel::<QueryJob>();

        let mut shards = Vec::new();
        for (index, raw_data) in raw_shards.iter().enumerate() {
            let (shard, messages) = channel();
            let (coordinator, raw_data) = (commands.clone(), raw_data.clone());
            let shard_shared = shared.clone();
            let guard = RunningGuard(running.clone());
            let delay = std::time::Duration::from_millis(if index == 0 {
                0
            } else {
                options.shard_delay_ms
            });
            std::thread::Builder::new()
                .name(format!("ingest-{index}"))
                .spawn(move || {
                    let _guard = guard;
                    serve_shard(index, raw_data, messages, coordinator, &shard_shared, delay);
                })?;
            shards.push(shard);
        }
        let ingest_guard = RunningGuard(running.clone());
        let ingest_shared = shared.clone();
        let coordinator_shards = shards.clone();
        std::thread::Builder::new()
            .name("ingest".into())
            .spawn(move || {
                let _guard = ingest_guard;
                serve_ingest(
                    ingestion,
                    raw_shards,
                    coordinator_shards,
                    command_receiver,
                    jobs,
                    &ingest_shared,
                );
            })?;
        let views = Arc::new(views);
        shared.published.publish_views(views.clone());
        shared
            .metrics
            .view_bytes
            .store(views.bytes() as u64, Ordering::Relaxed);
        let persist = match files {
            Some((files, loaded)) => {
                shared
                    .metrics
                    .view_windows_loaded
                    .store(loaded, Ordering::Relaxed);
                let mut shards = Vec::new();
                for (index, files) in files.split(options.persist_threads).into_iter().enumerate() {
                    let (shard, jobs) = channel();
                    let persist_shared = shared.clone();
                    let delay = std::time::Duration::from_millis(options.persist_delay_ms);
                    std::thread::Builder::new()
                        .name(format!("persist-{index}"))
                        .spawn(move || serve_persist(files, jobs, &persist_shared, delay))?;
                    shards.push(shard);
                }
                Some(PersistPool {
                    shards,
                    max_bytes: options.max_persist_backlog_bytes,
                })
            }
            None => None,
        };
        let precompute_guard = RunningGuard(running);
        let precompute_shared = shared.clone();
        std::thread::Builder::new()
            .name("precompute".into())
            .spawn(move || {
                let _guard = precompute_guard;
                serve_precompute(precompute, views, persist, job_receiver, &precompute_shared);
            })?;
        let query = Arc::new(query);
        spawn_query_workers(
            query.clone(),
            shared.clone(),
            query_receiver,
            options.query_threads.max(1),
        )?;
        let handle = Self {
            commands,
            shards,
            queries,
            query,
            query_threads: options.query_threads.max(1),
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
        // Each series' samples go to one shard, in order.
        let mut by_shard: Vec<Vec<RawSample>> = vec![Vec::new(); self.shards.len()];
        if self.shards.len() == 1 {
            by_shard[0] = samples;
        } else {
            for sample in samples {
                by_shard[sample.labels.id as usize % self.shards.len()].push(sample);
            }
        }
        let counts: Vec<u64> = by_shard.iter().map(|s| s.len() as u64).collect();
        // A request reaches its shards whole between two flushes, never
        // split by one: a flush covers all of it or none.
        let _sending = self
            .shared
            .admission
            .read()
            .unwrap_or_else(|e| e.into_inner());
        admit_shards(&self.shared.shard_queued, &counts, self.max_queued_samples)?;
        let shared = &self.shared;
        for (shard, samples) in by_shard.into_iter().enumerate() {
            let count = samples.len() as u64;
            if count == 0 {
                continue;
            }
            shared.queued_samples.fetch_add(count, Ordering::SeqCst);
            if self.shards[shard]
                .send(ShardMessage::Samples(samples))
                .is_err()
            {
                shared.queued_samples.fetch_sub(count, Ordering::SeqCst);
                shared.shard_queued[shard].fetch_sub(count, Ordering::SeqCst);
                return Err(IngestError::Stopped);
            }
        }
        Ok(())
    }

    /// Answers the planned `query` at `time_ms` on a query worker, from
    /// the newest snapshot published when the run starts; see
    /// [`QueryEngine::evaluate`]. Two queries at one evaluation time may
    /// read different snapshots. A query does not wait for writes
    /// acknowledged before it; an event-time barrier does
    /// ([`Self::advance_watermark`]).
    pub async fn evaluate(&self, query: String, time_ms: i64) -> Result<Answer, Unanswered> {
        let started = std::time::Instant::now();
        let metrics = self.shared.metrics.query(&query);
        let answer = self.run_on_worker(query, time_ms, None).await;
        if let Some(metrics) = metrics {
            metrics.latency.observe(started.elapsed().as_secs_f64());
        }
        answer
    }

    /// Evaluates one step of a range query over `snapshot`, which every
    /// step of the request reads ([`Self::snapshot`]): as
    /// [`Self::evaluate`], counted as a range step rather than an instant
    /// query. A step whose future is dropped before a worker takes it (the
    /// range failed or its client went away) is not run.
    pub async fn evaluate_range_step(
        &self,
        query: String,
        time_ms: i64,
        snapshot: Arc<Snapshot>,
    ) -> Result<Answer, Unanswered> {
        self.run_on_worker(query, time_ms, Some(snapshot)).await
    }

    /// The newest published store, for the steps of one range request.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::new(self.query.snapshot(&self.shared.published))
    }

    /// The result cache's answer to the planned `query` at `time_ms`, if
    /// it keeps one ([`QueryEngine::cached`]).
    pub fn cached(&self, query: &str, time_ms: i64) -> Option<Answer> {
        self.query.cached(query, time_ms)
    }

    async fn run_on_worker(
        &self,
        query: String,
        time_ms: i64,
        range_step: Option<Arc<Snapshot>>,
    ) -> Result<Answer, Unanswered> {
        let stopped = || Unanswered::Execution("the query workers have stopped".into());
        let (reply, answer) = oneshot::channel();
        self.queries
            .send(QueryJob {
                query,
                time_ms,
                range_step,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// The query worker threads: the most query runs at once.
    pub fn query_threads(&self) -> usize {
        self.query_threads
    }

    /// How many query-time `OneEvaluation` nodes several queries share.
    pub fn shared_nodes(&self) -> usize {
        self.query.shared_nodes().len()
    }

    /// The query-time partition of the view store.
    pub fn query_time_results(&self) -> &QueryTimeResults {
        self.query.query_time_results()
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
                persist_backlog: self.shared.persist_backlog.load(Ordering::SeqCst),
                persist_backlog_bytes: *self
                    .shared
                    .persist_queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
                now_ms: crate::ingestion::wall_clock_ms(),
                query_time_results: self.query.query_time_results(),
                result_cache: self.query.result_cache(),
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

/// Admits `counts[i]` samples to shard `i`'s queue `queued[i]` for every
/// shard, or none: `Busy` if any shard's queue would pass `max_queued`
/// ([`admit`]), and then no queue changes, so a request is taken whole or
/// not at all.
fn admit_shards(queued: &[AtomicU64], counts: &[u64], max_queued: u64) -> Result<(), IngestError> {
    let mut admitted: Vec<usize> = Vec::new();
    for (shard, &count) in counts.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let queue = &queued[shard];
        let mut current = queue.load(Ordering::SeqCst);
        let result = loop {
            let total = match admit(current, count, max_queued) {
                Ok(total) => total,
                Err(error) => break Err(error),
            };
            match queue.compare_exchange(current, total, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break Ok(()),
                Err(now) => current = now,
            }
        };
        if let Err(error) = result {
            for shard in admitted {
                queued[shard].fetch_sub(counts[shard], Ordering::SeqCst);
            }
            return Err(error);
        }
        admitted.push(shard);
    }
    Ok(())
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
    /// One step of a range query, and the snapshot every step reads.
    range_step: Option<Arc<Snapshot>>,
    reply: Reply<Answer>,
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
                // Nobody waits for it any more (a failed range, a client
                // that went away).
                if job.reply.is_closed() {
                    continue;
                }
                let timer = Timer::start();
                // A bug in one run must not take the worker down: it
                // answers that run with the error and keeps serving.
                let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = match &job.range_step {
                        Some(snapshot) => snapshot.clone(),
                        None => Arc::new(query.snapshot(&shared.published)),
                    };
                    query.evaluate(&job.query, job.time_ms, &snapshot)
                }))
                .unwrap_or_else(|panic| {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(ToString::to_string)
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    tracing::error!(query = %job.query, time_ms = job.time_ms, %reason, "query run panicked");
                    Err(Unanswered::Execution(format!("the query run panicked: {reason}")))
                });
                if let Some(metrics) = shared.metrics.query(&job.query) {
                    match job.range_step {
                        None => metrics.run.record(timer),
                        Some(_) => metrics.range_step_run.record(timer),
                    };
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
    options: &EngineOptions,
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
    let results = QueryTimeResults::new(
        i64::try_from(options.query_time_results_retention_ms).unwrap_or(i64::MAX),
        options.query_time_results_max_bytes,
    );
    let query = QueryEngine::new(
        &plan,
        &views,
        limits.clone(),
        results,
        options.share_one_evaluation_results,
        options.result_cache.then(|| {
            ResultCache::new(
                ResultCache::horizon_ms(&plan.dag, options.ingestion.view_retention_margin_ms),
                options.result_cache_max_bytes,
            )
        }),
    )?;
    tracing::info!(
        shared = ?query.shared_nodes(),
        "query-time OneEvaluation nodes shared across queries"
    );
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

/// Sends every shard a flush, with no request half sent: each request
/// already admitted is wholly before it in its shards' queues.
fn flush(shards: &[Sender<ShardMessage>], shared: &Shared) {
    let _admission = shared.admission.write().unwrap_or_else(|e| e.into_inner());
    for shard in shards {
        let _ = shard.send(ShardMessage::Flush);
    }
}

/// An ingest shard: routes and appends the samples of its series to its
/// own raw data, seals and evicts it when told, and reports its state to
/// the coordinator after each message.
fn serve_shard(
    index: usize,
    mut raw_data: Arc<RawData>,
    messages: Receiver<ShardMessage>,
    coordinator: Sender<Command>,
    shared: &Shared,
    delay: std::time::Duration,
) {
    let mut newest_ms: Option<i64> = None;
    for message in messages {
        let (count, first_seen) = match message {
            ShardMessage::Samples(samples) => {
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let timer = Timer::start();
                let count = samples.len() as u64;
                let routing = shared
                    .routing
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let routed = routing.route(samples, crate::ingestion::wall_clock_ms());
                Arc::make_mut(&mut raw_data).append(&routed.accepted);
                newest_ms = newest_ms.max(routed.newest_ms);
                {
                    let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
                    stats.samples_accepted += routed.stats.samples_accepted;
                    stats.samples_dropped_late += routed.stats.samples_dropped_late;
                    stats.samples_dropped_future += routed.stats.samples_dropped_future;
                    stats.samples_unrouted += routed.stats.samples_unrouted;
                }
                shared.metrics.append.record(timer);
                (count, routed.first_seen)
            }
            ShardMessage::Maintain {
                routing,
                watermark_ms,
            } => {
                routing.maintain(Arc::make_mut(&mut raw_data), watermark_ms);
                (0, Vec::new())
            }
            ShardMessage::Flush => {
                if coordinator.send(Command::Flushed(index)).is_err() {
                    break;
                }
                continue;
            }
        };
        let report = ShardReport {
            shard: index,
            raw_data: raw_data.clone(),
            newest_ms,
            first_seen,
            now_ms: crate::ingestion::wall_clock_ms(),
        };
        let sent = coordinator.send(Command::Report(report));
        // Counted out after the report, so the coordinator never takes the
        // shard for caught up before it knows the samples' times.
        shared.shard_queued[index].fetch_sub(count, Ordering::SeqCst);
        shared.queued_samples.fetch_sub(count, Ordering::SeqCst);
        if sent.is_err() {
            break;
        }
    }
}

/// The ingest coordinator: keeps the watermark over the shards, closes the
/// windows it passes and queues their refreshes over every shard's raw data
/// as it was then, publishes the raw data and the routing, and has the
/// shards seal and evict as the watermark moves. The watermark follows the
/// newest sample of any shard; before it moves, every shard runs the
/// samples queued to it (a flush), so a window closes over every sample
/// admitted before. Shards with nothing queued answer the flush at once,
/// so idle shards never hold the watermark; the coordinator also wakes
/// every tick.
fn serve_ingest(
    mut ingestion: Ingestion,
    mut raw_shards: Vec<Arc<RawData>>,
    shards: Vec<Sender<ShardMessage>>,
    commands: Receiver<Command>,
    jobs: SyncSender<PrecomputeJob>,
    shared: &Shared,
) {
    const TICK: std::time::Duration = std::time::Duration::from_millis(100);
    let mut newest: Vec<Option<i64>> = vec![None; shards.len()];
    let mut sealed_through: Option<i64> = None;
    let apply = |report: ShardReport,
                 ingestion: &mut Ingestion,
                 raw_shards: &mut Vec<Arc<RawData>>,
                 newest: &mut Vec<Option<i64>>| {
        ingestion.observe(&report.first_seen, report.now_ms);
        raw_shards[report.shard] = report.raw_data;
        newest[report.shard] = report.newest_ms;
    };
    // A barrier met while draining reports waits for the next pass, which
    // runs it behind a flush.
    let mut deferred: Option<Command> = None;
    // Reports a shard sent after answering a flush, of samples admitted
    // after it: held for the next pass, so they move neither this pass's
    // watermark nor its raw data. Without this, a shard that ran ahead
    // would move the watermark past samples other shards have yet to
    // append, and windows would close without them.
    let mut held: Vec<ShardReport> = Vec::new();
    'serve: loop {
        let mut barriers = Vec::new();
        for report in held.drain(..) {
            apply(report, &mut ingestion, &mut raw_shards, &mut newest);
        }
        let next = match deferred.take() {
            Some(command) => Ok(command),
            None => commands.recv_timeout(TICK),
        };
        match next {
            Ok(Command::Report(report)) => {
                apply(report, &mut ingestion, &mut raw_shards, &mut newest)
            }
            Ok(Command::Flushed(_)) => {}
            Ok(Command::AdvanceWatermark { barrier_ms, reply }) => {
                // Behind every write acknowledged before: each shard runs
                // the samples queued to it first.
                barriers.push((barrier_ms, reply));
                flush(&shards, shared);
                let mut flushed = vec![false; shards.len()];
                while !flushed.iter().all(|&f| f) {
                    match commands.recv() {
                        Ok(Command::Report(report))
                            if flushed[report.shard]
                                || held.iter().any(|r| r.shard == report.shard) =>
                        {
                            held.push(report)
                        }
                        Ok(Command::Report(report)) => {
                            apply(report, &mut ingestion, &mut raw_shards, &mut newest)
                        }
                        Ok(Command::Flushed(shard)) => flushed[shard] = true,
                        Ok(Command::AdvanceWatermark { barrier_ms, reply }) => {
                            barriers.push((barrier_ms, reply))
                        }
                        Ok(Command::Stop) | Err(_) => break 'serve,
                    }
                }
            }
            Ok(Command::Stop) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
        // Drain the reports already sent, so one pass takes them all.
        while held.is_empty() {
            let Ok(command) = commands.try_recv() else {
                break;
            };
            match command {
                Command::Report(report) => {
                    apply(report, &mut ingestion, &mut raw_shards, &mut newest)
                }
                Command::Flushed(_) => {}
                command @ Command::AdvanceWatermark { .. } => {
                    deferred = Some(command);
                    break;
                }
                Command::Stop => break 'serve,
            }
        }
        // Before the watermark moves, every shard runs the samples queued to
        // it, so the windows it closes hold every sample admitted so far
        // and the raw data they read is every shard's as of then.
        let newest_ms = newest.iter().flatten().copied().max();
        if newest_ms > ingestion.newest_sample_ms() {
            flush(&shards, shared);
            let mut flushed = vec![false; shards.len()];
            while !flushed.iter().all(|&f| f) {
                match commands.recv() {
                    Ok(Command::Report(report))
                        if flushed[report.shard]
                            || held.iter().any(|r| r.shard == report.shard) =>
                    {
                        held.push(report)
                    }
                    Ok(Command::Report(report)) => {
                        apply(report, &mut ingestion, &mut raw_shards, &mut newest)
                    }
                    Ok(Command::Flushed(shard)) => flushed[shard] = true,
                    Ok(command @ Command::AdvanceWatermark { .. }) => deferred = Some(command),
                    Ok(Command::Stop) | Err(_) => break 'serve,
                }
            }
        }
        let newest_ms = newest.iter().flatten().copied().max();
        ingestion.advance_newest_sample(newest_ms);
        let mut closed = ingestion.close();
        let mut answered = Vec::new();
        for (barrier_ms, reply) in barriers {
            let (barrier_ms, more) = ingestion.advance_watermark(barrier_ms);
            closed.extend(more);
            answered.push((barrier_ms, reply));
        }
        let snapshot: RawShards = Arc::new(raw_shards.clone());
        if !queue_refreshes(&closed, &snapshot, &jobs, shared) {
            break;
        }
        let routing = Arc::new(ingestion.routing());
        *shared.routing.write().unwrap_or_else(|e| e.into_inner()) = routing.clone();
        let watermark_ms = ingestion.watermark_ms();
        if watermark_ms != sealed_through || !closed.is_empty() {
            sealed_through = watermark_ms;
            for shard in &shards {
                let _ = shard.send(ShardMessage::Maintain {
                    routing: routing.clone(),
                    watermark_ms,
                });
            }
        }
        shared
            .published
            .publish_raw_data(snapshot.clone(), watermark_ms);
        let metrics = &shared.metrics;
        if let Some(watermark_ms) = watermark_ms {
            metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
        }
        let sum = |f: fn(&RawData) -> usize| snapshot.iter().map(|r| f(r) as u64).sum::<u64>();
        metrics
            .raw_data_bytes
            .store(sum(RawData::bytes), Ordering::Relaxed);
        metrics
            .raw_data_samples
            .store(sum(RawData::len), Ordering::Relaxed);
        metrics
            .raw_data_panes
            .store(sum(RawData::panes), Ordering::Relaxed);
        {
            let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
            stats.windows_skipped_first = ingestion.stats.windows_skipped_first;
            stats.windows_skipped_expired = ingestion.stats.windows_skipped_expired;
        }
        for (barrier_ms, reply) in answered {
            if jobs
                .send(PrecomputeJob::Barrier { barrier_ms, reply })
                .is_err()
            {
                break 'serve;
            }
        }
    }
    tracing::warn!("ingest stopped");
}

/// Queues a refresh of each of `closed` over `raw_data`, waiting while the
/// backlog is full; `false` once the precompute thread has exited.
fn queue_refreshes(
    closed: &[ClosedWindow],
    raw_data: &RawShards,
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
fn serve_precompute(
    precompute: Precompute,
    mut views: Arc<ViewStore>,
    persist: Option<PersistPool>,
    jobs: Receiver<PrecomputeJob>,
    shared: &Shared,
) {
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
                    if !matches!(result, Ok(Refreshed::AlreadyStored)) {
                        shared.metrics.precompute[window.group].1.record(timer);
                    }
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
                        Ok(Refreshed::Ran(appended)) => {
                            stats.windows_refreshed += 1;
                            drop(stats);
                            if let Some(persist) = &persist {
                                let start_ms = window.end_ms - precompute.width_ms(window.group);
                                persist.queue(
                                    window.group,
                                    (start_ms, window.end_ms),
                                    appended,
                                    shared,
                                );
                            }
                        }
                        Ok(Refreshed::AlreadyStored) => stats.windows_already_stored += 1,
                        Err(error) => {
                            tracing::error!(group = window.group, end = window.end_ms, %error, "ingestion run failed");
                            stats.windows_failed += 1;
                        }
                    }
                }
                shared.precompute_backlog.fetch_sub(1, Ordering::SeqCst);
            }
            PrecomputeJob::Barrier { barrier_ms, reply } => match &persist {
                // The barrier answers once the windows before it are also
                // written (not necessarily synced), without the precompute
                // thread waiting for that.
                Some(persist) => persist.barrier(barrier_ms, reply),
                None => {
                    let _ = reply.send(barrier_ms);
                }
            },
        }
    }
    tracing::warn!("precompute stopped");
}

/// What a persist thread does next.
enum PersistJob {
    /// Write the rows each view got for its group's window `range_ms`;
    /// `bytes` of the queue it releases once written.
    Window {
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        bytes: u64,
    },
    /// Done once every window queued to this thread before is written.
    Barrier(Arc<PersistBarrier>),
}

/// An event-time barrier waiting for every persist thread.
struct PersistBarrier {
    barrier_ms: i64,
    remaining: AtomicU64,
    reply: Mutex<Option<oneshot::Sender<i64>>>,
}

impl PersistBarrier {
    /// One persist thread reached it; the last one answers.
    fn reached(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            if let Some(reply) = self.reply.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = reply.send(self.barrier_ms);
            }
        }
    }
}

/// The persist threads, as the precompute thread sees them: refresh group
/// `g`'s windows go to thread `g % threads`, so each file has one writer
/// and its records stay in window order. The queue between precompute and
/// the threads is bounded by bytes.
struct PersistPool {
    shards: Vec<Sender<PersistJob>>,
    max_bytes: u64,
}

impl PersistPool {
    /// Queues a refreshed window for its group's persist thread, waiting
    /// while the queue holds more than `max_bytes` (the wait is exported;
    /// a window larger than the bound is still queued into an empty queue).
    fn queue(
        &self,
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        shared: &Shared,
    ) {
        let bytes: u64 = views
            .iter()
            .flat_map(|(_, batches)| batches)
            .map(|batch| batch.memory_bytes() as u64)
            .sum();
        {
            let mut queued = shared
                .persist_queue
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *queued > 0 && *queued + bytes > self.max_bytes {
                let waited = std::time::Instant::now();
                while *queued > 0 && *queued + bytes > self.max_bytes {
                    queued = shared
                        .persist_room
                        .wait(queued)
                        .unwrap_or_else(|e| e.into_inner());
                }
                shared
                    .metrics
                    .view_persist_wait_ns
                    .fetch_add(waited.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            *queued += bytes;
        }
        shared.persist_backlog.fetch_add(1, Ordering::SeqCst);
        let job = PersistJob::Window {
            group,
            range_ms,
            views,
            bytes,
        };
        if self.shards[group % self.shards.len()].send(job).is_err() {
            tracing::error!(group, "the persist thread has stopped");
            release_persist_queue(shared, bytes);
        }
    }

    /// Answers `reply` once every persist thread has written the windows
    /// queued before.
    fn barrier(&self, barrier_ms: i64, reply: oneshot::Sender<i64>) {
        let barrier = Arc::new(PersistBarrier {
            barrier_ms,
            remaining: AtomicU64::new(self.shards.len() as u64),
            reply: Mutex::new(Some(reply)),
        });
        for shard in &self.shards {
            if shard.send(PersistJob::Barrier(barrier.clone())).is_err() {
                barrier.reached();
            }
        }
    }
}

/// Releases `bytes` of the persist queue for a window written (or lost).
fn release_persist_queue(shared: &Shared, bytes: u64) {
    let mut queued = shared
        .persist_queue
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *queued = queued.saturating_sub(bytes);
    shared.persist_backlog.fetch_sub(1, Ordering::SeqCst);
    shared.persist_room.notify_all();
}

/// The persist thread: writes each refreshed window's records to the view
/// files, in order, and syncs them every sync interval, also when no window
/// arrives; syncs everything when the precompute thread stops.
fn serve_persist(
    mut files: ViewFiles,
    jobs: Receiver<PersistJob>,
    shared: &Shared,
    delay: std::time::Duration,
) {
    let metrics = &shared.metrics;
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
                bytes,
            } => {
                // A slow disk, for tests.
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let records: Vec<(PhysicalASAPNodeId, &[Batch])> = views
                    .iter()
                    .map(|(view, batches)| (*view, batches.as_slice()))
                    .collect();
                match files.append(group, range_ms, &records) {
                    Ok(written) => {
                        metrics
                            .view_persisted_bytes
                            .fetch_add(written.bytes, Ordering::Relaxed);
                        metrics
                            .view_sync_ns
                            .fetch_add(written.sync_ns, Ordering::Relaxed);
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(written.not_persisted, Ordering::Relaxed);
                    }
                    Err(error) => {
                        tracing::error!(group, ?range_ms, %error, "persisting a window failed");
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(records.len() as u64, Ordering::Relaxed);
                    }
                }
                release_persist_queue(shared, bytes);
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

    /// A request reaching a full shard is refused whole: no other shard's
    /// queue takes its samples.
    #[test]
    fn a_full_shard_refuses_the_whole_request() {
        let queued: Vec<AtomicU64> = [0, 5, 0].into_iter().map(AtomicU64::new).collect();
        assert_eq!(admit_shards(&queued, &[2, 3, 1], 6), Err(IngestError::Busy));
        let now: Vec<u64> = queued.iter().map(|q| q.load(Ordering::SeqCst)).collect();
        assert_eq!(now, [0, 5, 0]);
        assert_eq!(admit_shards(&queued, &[2, 1, 1], 6), Ok(()));
        let now: Vec<u64> = queued.iter().map(|q| q.load(Ordering::SeqCst)).collect();
        assert_eq!(now, [2, 6, 1]);
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
