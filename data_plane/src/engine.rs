//! The data plane's threads: ingestion workers that build stored summaries,
//! and a pool of query threads that answer planned queries.
//!
//! The plan is compiled once, when it is installed: its ingestion side
//! ([`Ingestion`]) and its query roots ([`QueryEngine`]). Every thread
//! shares them, with the stored summaries, the kept query-run summaries and
//! the watermark ([`Shared`]).
//!
//! - **Ingestion.** Series are sharded by a hash of their label set; each
//!   worker owns the open panes of its shard. The global watermark is the
//!   minimum over the workers; the worker or barrier that moves it closes
//!   the panes it passed, from every worker's samples.
//! - **Queries.** One query per thread, from a pool: a query runs on the
//!   thread that takes it, while other queries run on the others and
//!   ingestion continues. Before reading, a query waits until the workers
//!   have run every write acknowledged before it, so it sees those writes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use asap_planner::InstallablePlan;

use tokio::sync::oneshot;

use crate::precompute::{
    shard_of, wall_clock_ms, Ingestion, PrecomputeOptions, PrecomputeStats, RawSample, Shared,
};
use crate::query::{
    IngestionProgress, KeptResultStats, Prepared, QueryEngine, ResultSample, Unanswered,
};
use crate::sds::Sds;

/// How the engine runs: its threads and how it ingests samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and pane checks of the precompute engine.
    pub precompute: PrecomputeOptions,
    /// Samples queued for the workers, not yet ingested, beyond which
    /// Remote Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
    /// Ingestion workers; series are sharded across them.
    pub ingestion_workers: usize,
    /// Query threads; each runs one query at a time.
    pub query_threads: usize,
}

impl Default for EngineOptions {
    /// One thread of each kind per available core.
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            precompute: PrecomputeOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
            ingestion_workers: cores,
            query_threads: cores,
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

enum WorkerCommand {
    Ingest(Vec<RawSample>),
    /// Reply once every command queued before has run.
    Flush(oneshot::Sender<()>),
    Stop,
}

enum QueryJob {
    RawRanges {
        query: String,
        time_ms: i64,
        reply: Reply<Prepared>,
    },
    Evaluate {
        root: usize,
        time_ms: i64,
        progress: IngestionProgress,
        raw: BTreeMap<u64, Vec<RawSample>>,
        reply: Reply<Vec<ResultSample>>,
    },
}

/// Why the engine did not take samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// More samples are queued than `max_queued_samples`; retry later.
    #[error("the engine is busy: too many samples queued")]
    Busy,
    /// An engine thread has stopped; no sample will be ingested again.
    #[error("the engine has stopped")]
    Stopped,
}

/// The HTTP side of the engine's threads.
#[derive(Clone)]
pub struct EngineHandle {
    workers: Vec<Sender<WorkerCommand>>,
    queries: Sender<QueryJob>,
    precompute: Arc<Shared>,
    query: Arc<QueryEngine>,
    /// Samples sent and not yet ingested.
    queued_samples: Arc<AtomicU64>,
    max_queued_samples: u64,
    /// False once any engine thread has exited, for any reason.
    running: Arc<AtomicBool>,
}

/// Marks the engine stopped when one of its threads exits, panics included.
struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Reads and validates the plan serialized in `plan_json`, and its stored
/// summaries.
fn install(plan_json: &str) -> anyhow::Result<(InstallablePlan, Sds)> {
    let plan: InstallablePlan = serde_json::from_str(plan_json)?;
    plan.validate()?;
    let sds = Sds::from_plan(&plan)?;
    tracing::info!(
        definitions = sds.definitions.len(),
        outputs = sds.outputs.len(),
        "stored summaries"
    );
    Ok((plan, sds))
}

impl EngineHandle {
    /// Starts the engine's threads for the plan serialized in `plan_json`,
    /// compiled once, and returns the plan's queries. Fails if the plan is
    /// invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let (plan, sds) = install(&plan_json)?;
        let queries = plan.queries.iter().map(|q| q.query.clone()).collect();
        let ingestion = Ingestion::new(&plan.dag.dag, &sds, options.precompute)?;
        let precompute = Arc::new(Shared::new(ingestion, options.ingestion_workers));
        let engine = Arc::new(QueryEngine::new(&plan, &sds));
        tracing::info!(
            ingestion_workers = precompute.workers(),
            query_threads = options.query_threads.max(1),
            "engine threads"
        );
        let running = Arc::new(AtomicBool::new(true));
        let queued_samples = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::new();
        for index in 0..precompute.workers() {
            let (sender, commands) = channel();
            let (precompute, queued_samples) = (precompute.clone(), queued_samples.clone());
            let guard = RunningGuard(running.clone());
            std::thread::Builder::new()
                .name(format!("ingest-{index}"))
                .spawn(move || {
                    let _guard = guard;
                    serve_worker(index, &precompute, &queued_samples, commands);
                })?;
            workers.push(sender);
        }
        let (queries_sender, jobs) = channel();
        let jobs = Arc::new(Mutex::new(jobs));
        for index in 0..options.query_threads.max(1) {
            let (engine, precompute, jobs) = (engine.clone(), precompute.clone(), jobs.clone());
            let guard = RunningGuard(running.clone());
            std::thread::Builder::new()
                .name(format!("query-{index}"))
                .spawn(move || {
                    let _guard = guard;
                    serve_queries(&engine, &precompute, &jobs);
                })?;
        }
        let handle = Self {
            workers,
            queries: queries_sender,
            precompute,
            query: engine,
            queued_samples,
            max_queued_samples: options.max_queued_samples,
            running,
        };
        Ok((handle, queries))
    }

    /// How many ingestion workers series are sharded across.
    pub fn ingestion_workers(&self) -> usize {
        self.workers.len()
    }

    /// Queues each worker's shard of `samples`, all or none. `Busy` when
    /// the queue would hold more than `max_queued_samples`; `Stopped` when
    /// an engine thread has exited.
    pub fn try_ingest(&self, samples: Vec<RawSample>) -> Result<(), IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        let count = samples.len() as u64;
        let mut queued = self.queued_samples.load(Ordering::SeqCst);
        loop {
            let total = admit(queued, count, self.max_queued_samples)?;
            match self.queued_samples.compare_exchange(
                queued,
                total,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(current) => queued = current,
            }
        }
        let mut shards = vec![Vec::new(); self.workers.len()];
        for sample in samples {
            shards[shard_of(&sample.labels, self.workers.len())].push(sample);
        }
        // The channels are unbounded: once admitted, a request reaches
        // every worker, so a retry never doubles samples.
        for (worker, shard) in self.workers.iter().zip(shards) {
            if shard.is_empty() {
                continue;
            }
            let n = shard.len() as u64;
            if worker.send(WorkerCommand::Ingest(shard)).is_err() {
                self.queued_samples.fetch_sub(n, Ordering::SeqCst);
                return Err(IngestError::Stopped);
            }
        }
        Ok(())
    }

    /// The plan root answering `query` at `time_ms` and the raw samples it
    /// needs, once the writes acknowledged before are ingested; see
    /// [`QueryEngine::raw_ranges`].
    pub async fn raw_ranges(&self, query: String, time_ms: i64) -> Result<Prepared, Unanswered> {
        self.sync().await;
        self.ask(|reply| QueryJob::RawRanges {
            query,
            time_ms,
            reply,
        })
        .await
    }

    /// Runs a prepared root over stored summaries and `raw`; see
    /// [`QueryEngine::evaluate`].
    pub async fn evaluate(
        &self,
        prepared: Prepared,
        time_ms: i64,
        raw: BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        self.ask(|reply| QueryJob::Evaluate {
            root: prepared.root,
            time_ms,
            progress: prepared.progress,
            raw,
            reply,
        })
        .await
    }

    /// The query-run summary counters as of now.
    pub fn kept_stats(&self) -> KeptResultStats {
        self.query.kept_stats()
    }

    async fn ask<T>(&self, job: impl FnOnce(Reply<T>) -> QueryJob) -> Result<T, Unanswered> {
        let stopped = || Unanswered::Execution("the engine has stopped".into());
        let (reply, answer) = oneshot::channel();
        self.queries.send(job(reply)).map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// Returns once every worker has run every command queued before, so
    /// a query sees the writes acknowledged before it. A worker that takes
    /// samples closes the panes they let the watermark pass before it runs
    /// its next command, so one round suffices.
    pub async fn sync(&self) {
        let mut flushed = Vec::new();
        for worker in &self.workers {
            let (reply, done) = oneshot::channel();
            if worker.send(WorkerCommand::Flush(reply)).is_ok() {
                flushed.push(done);
            }
        }
        for done in flushed {
            let _ = done.await;
        }
    }

    /// Whether every engine thread is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Samples queued and not yet ingested.
    pub fn queued_samples(&self) -> u64 {
        self.queued_samples.load(Ordering::SeqCst)
    }

    /// The precompute counters as of now.
    pub fn stats(&self) -> PrecomputeStats {
        self.precompute.stats()
    }

    /// Runs an event-time barrier behind every write already acknowledged,
    /// on every worker's shard: every pane ending at or before
    /// `barrier_ms` (capped at the wall clock) is then closed. Returns the
    /// barrier in force; see [`Shared::advance_watermark_at`].
    pub async fn advance_watermark(&self, barrier_ms: i64) -> Result<i64, IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        self.sync().await;
        let precompute = self.precompute.clone();
        tokio::task::spawn_blocking(move || {
            precompute.advance_watermark_at(barrier_ms, wall_clock_ms())
        })
        .await
        .map_err(|_| IngestError::Stopped)
    }

    /// For tests: stops the ingestion workers after the commands queued
    /// before. Samples queued between this call and the stop are
    /// acknowledged and then lost; ingestion after the stop fails with
    /// [`IngestError::Stopped`].
    #[doc(hidden)]
    pub fn stop(&self) {
        for worker in &self.workers {
            let _ = worker.send(WorkerCommand::Stop);
        }
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

/// Runs worker `index`'s commands until it is stopped or the handle is
/// dropped.
fn serve_worker(
    index: usize,
    precompute: &Shared,
    queued_samples: &AtomicU64,
    commands: Receiver<WorkerCommand>,
) {
    for command in commands {
        match command {
            WorkerCommand::Ingest(samples) => {
                precompute.ingest_at(index, &samples, wall_clock_ms());
                queued_samples.fetch_sub(samples.len() as u64, Ordering::SeqCst);
            }
            WorkerCommand::Flush(reply) => {
                let _ = reply.send(());
            }
            WorkerCommand::Stop => break,
        }
    }
    tracing::warn!(worker = index, "ingestion worker stopped");
}

/// Answers jobs from the pool's queue, one at a time, until the queue closes.
fn serve_queries(engine: &QueryEngine, precompute: &Shared, jobs: &Mutex<Receiver<QueryJob>>) {
    loop {
        let job = match jobs.lock().unwrap_or_else(|e| e.into_inner()).recv() {
            Ok(job) => job,
            Err(_) => return,
        };
        let store = &precompute.store;
        match job {
            QueryJob::RawRanges {
                query,
                time_ms,
                reply,
            } => {
                let progress = precompute.progress();
                let _ = reply.send(engine.raw_ranges(&query, time_ms, store, progress));
            }
            QueryJob::Evaluate {
                root,
                time_ms,
                progress,
                raw,
                reply,
            } => {
                let _ = reply.send(engine.evaluate(root, time_ms, store, &progress, &raw));
            }
        }
    }
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

    /// The running flag clears when the engine thread panics.
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
