//! The data plane's threads: ingestion workers that build stored panes, and
//! a pool of query threads that answer planned queries.
//!
//! Planner IR and the executor's compiled plans hold `Rc`s, so they cannot
//! be shared across threads: every worker and query thread reads the plan
//! and compiles its own. What threads share is plain data: the stored panes,
//! the kept query-time results, and the watermarks.
//!
//! - **Ingestion.** Series are sharded by a hash of their label set; each
//!   worker owns the open panes of its shard. A worker's watermark is its
//!   newest sample; a pane closes when the minimum over the workers passes
//!   it. Each worker then builds its part of the pane, and the last part
//!   completes it ([`crate::precompute::Shared`]).
//! - **Queries.** One query per thread, from a pool: a query, and every step
//!   of a range query, runs on the thread that takes it, while other queries
//!   run on the others. They read stored panes under the store's read lock.

use std::collections::BTreeMap;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};

use asap_planner::InstallablePlan;
use tokio::sync::{mpsc, oneshot};

use crate::precompute::{PrecomputeStats, RawSample, ShardWorker, Shared};
use crate::query::{KeptResults, QueryEngine, RawRange, ResultSample, Unanswered};
use crate::sds::Sds;

/// Messages queued for each ingestion worker, and queries for the pool,
/// before the data plane pushes back.
const QUEUE: usize = 1024;

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

enum WorkerCommand {
    Ingest(Vec<RawSample>),
    /// Another worker moved the global watermark: build the panes it passed.
    Advance,
    /// Reply once every command queued before has run.
    Flush(oneshot::Sender<()>),
}

enum QueryJob {
    RawRanges {
        query: String,
        time_ms: i64,
        reply: Reply<(usize, Vec<RawRange>)>,
    },
    Evaluate {
        root: usize,
        time_ms: i64,
        raw: BTreeMap<u64, Vec<RawSample>>,
        reply: Reply<Vec<ResultSample>>,
    },
    RawRangesForSteps {
        query: String,
        steps: Vec<i64>,
        reply: Reply<(usize, Vec<RawRange>)>,
    },
    EvaluateSteps {
        root: usize,
        steps: Vec<i64>,
        raw: BTreeMap<u64, Vec<RawSample>>,
        reply: Reply<Vec<Vec<ResultSample>>>,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("the engine is busy")]
pub struct Busy;

/// How many threads the engine runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Threads {
    /// Ingestion workers; series are sharded across them.
    pub ingestion_workers: usize,
    /// Query threads; each runs one query at a time.
    pub query_threads: usize,
}

impl Default for Threads {
    /// One of each per available core.
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            ingestion_workers: cores,
            query_threads: cores,
        }
    }
}

/// The HTTP side of the engine's threads.
#[derive(Clone)]
pub struct EngineHandle {
    workers: Vec<mpsc::Sender<WorkerCommand>>,
    queries: SyncSender<QueryJob>,
    precompute: Arc<Shared>,
}

/// Reads and validates the plan serialized in `plan_json`, and its stored
/// summaries.
fn install(plan_json: &str) -> anyhow::Result<(InstallablePlan, Sds)> {
    let plan: InstallablePlan = serde_json::from_str(plan_json)?;
    plan.validate()?;
    let sds = Sds::from_plan(&plan)?;
    Ok((plan, sds))
}

/// Runs `thread` on a new thread named `name`; returns once it has started,
/// or with the error it failed to start with.
fn spawn(
    name: String,
    thread: impl FnOnce(std::sync::mpsc::Sender<anyhow::Result<()>>) + Send + 'static,
) -> anyhow::Result<()> {
    let (ready, started) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(name)
        .spawn(move || thread(ready))?;
    started.recv()?
}

impl EngineHandle {
    /// Starts the engine's threads for the plan serialized in `plan_json`.
    /// Fails if the plan is invalid or the data plane cannot execute it.
    pub fn spawn(
        plan_json: String,
        allowed_lateness_ms: i64,
        threads: Threads,
    ) -> anyhow::Result<Self> {
        let (plan, sds) = install(&plan_json)?;
        tracing::info!(
            definitions = sds.definitions.len(),
            outputs = sds.outputs.len(),
            kept_definitions = sds.kept_definitions.len(),
            kept_outputs = sds.kept_outputs.len(),
            ?threads,
            "stored summaries"
        );
        drop(plan);
        let plan_json = Arc::new(plan_json);
        let precompute = Arc::new(Shared::new(threads.ingestion_workers, allowed_lateness_ms));
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..precompute.workers())
            .map(|_| mpsc::channel(QUEUE))
            .unzip();
        // Workers tell each other when the global watermark moves; weak
        // senders let the channels close when the handle is dropped.
        let peers: Vec<_> = senders.iter().map(mpsc::Sender::downgrade).collect();
        for (index, mut commands) in receivers.into_iter().enumerate() {
            let (plan_json, shared, peers) = (plan_json.clone(), precompute.clone(), peers.clone());
            spawn(format!("ingest-{index}"), move |ready| {
                let started = install(&plan_json).and_then(|(plan, sds)| {
                    Ok(ShardWorker::new(&plan, &sds, index, shared, true)?)
                });
                let mut worker = match started {
                    Ok(worker) => {
                        let _ = ready.send(Ok(()));
                        worker
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                while let Some(command) = commands.blocking_recv() {
                    match command {
                        WorkerCommand::Ingest(samples) => {
                            if worker.ingest(&samples) {
                                for (peer, sender) in peers.iter().enumerate() {
                                    if peer != index {
                                        // A full queue runs `advance` with
                                        // its next command anyway.
                                        if let Some(sender) = sender.upgrade() {
                                            let _ = sender.try_send(WorkerCommand::Advance);
                                        }
                                    }
                                }
                            }
                        }
                        WorkerCommand::Advance => worker.advance(),
                        WorkerCommand::Flush(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }
            })?;
        }
        let (queries, jobs) = sync_channel(QUEUE);
        let jobs = Arc::new(Mutex::new(jobs));
        let kept = Arc::new(KeptResults::default());
        for index in 0..threads.query_threads.max(1) {
            let (plan_json, shared, kept, jobs) = (
                plan_json.clone(),
                precompute.clone(),
                kept.clone(),
                jobs.clone(),
            );
            spawn(format!("query-{index}"), move |ready| {
                let engine = match install(&plan_json) {
                    Ok((plan, sds)) => QueryEngine::with_kept(&plan, &sds, kept),
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                let _ = ready.send(Ok(()));
                serve_queries(&engine, &shared, &jobs);
            })?;
        }
        Ok(Self {
            workers: senders,
            queries,
            precompute,
        })
    }

    /// How many ingestion workers series are sharded across.
    pub fn ingestion_workers(&self) -> usize {
        self.workers.len()
    }

    /// Queues each worker's shard of the samples (`shards[i]` for worker
    /// `i`), or none if any worker is behind.
    pub fn try_ingest(&self, shards: Vec<Vec<RawSample>>) -> Result<(), Busy> {
        // Reserve every worker's slot first, so a request is taken whole or
        // not at all: Remote Write retries a refused request.
        let permits = self
            .workers
            .iter()
            .zip(&shards)
            .filter(|(_, samples)| !samples.is_empty())
            .map(|(sender, _)| sender.try_reserve().map_err(|_| Busy))
            .collect::<Result<Vec<_>, _>>()?;
        for (permit, samples) in permits
            .into_iter()
            .zip(shards.into_iter().filter(|samples| !samples.is_empty()))
        {
            permit.send(WorkerCommand::Ingest(samples));
        }
        Ok(())
    }

    /// The plan root answering `query` at `time_ms` and the raw samples it
    /// needs, once the writes answered before are ingested; see
    /// [`QueryEngine::raw_ranges`].
    pub async fn raw_ranges(
        &self,
        query: String,
        time_ms: i64,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        self.sync().await;
        self.ask(|reply| QueryJob::RawRanges {
            query,
            time_ms,
            reply,
        })
        .await
    }

    /// Runs a root over stored panes and `raw`; see [`QueryEngine::evaluate`].
    pub async fn evaluate(
        &self,
        root: usize,
        time_ms: i64,
        raw: BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        self.ask(|reply| QueryJob::Evaluate {
            root,
            time_ms,
            raw,
            reply,
        })
        .await
    }

    /// The plan root answering `query` at every one of `steps` and the raw
    /// samples they need; see [`QueryEngine::raw_ranges_for_steps`].
    pub async fn raw_ranges_for_steps(
        &self,
        query: String,
        steps: Vec<i64>,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        self.sync().await;
        self.ask(|reply| QueryJob::RawRangesForSteps {
            query,
            steps,
            reply,
        })
        .await
    }

    /// Runs a root at every one of `steps`, all on one query thread; see
    /// [`QueryEngine::evaluate_steps`].
    pub async fn evaluate_steps(
        &self,
        root: usize,
        steps: Vec<i64>,
        raw: BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<Vec<ResultSample>>, Unanswered> {
        self.ask(|reply| QueryJob::EvaluateSteps {
            root,
            steps,
            raw,
            reply,
        })
        .await
    }

    async fn ask<T>(&self, job: impl FnOnce(Reply<T>) -> QueryJob) -> Result<T, Unanswered> {
        let busy = || Unanswered::Execution("the engine is busy".into());
        let (reply, answer) = oneshot::channel();
        self.queries.try_send(job(reply)).map_err(|_| busy())?;
        answer.await.map_err(|_| busy())?
    }

    /// Precompute counters, once every sample queued before has been
    /// ingested and every pane it closed has been built.
    pub async fn stats(&self) -> PrecomputeStats {
        self.sync().await;
        self.precompute.stats()
    }

    /// Returns once every sample queued before has been ingested and every
    /// pane it closed has been built, so a query sees the writes answered
    /// before it.
    pub async fn sync(&self) {
        // The first round runs every queued ingest, which may tell other
        // workers to advance; the second runs those advances.
        for _ in 0..2 {
            let mut flushed = Vec::new();
            for worker in &self.workers {
                let (reply, done) = oneshot::channel();
                if worker.send(WorkerCommand::Flush(reply)).await.is_ok() {
                    flushed.push(done);
                }
            }
            for done in flushed {
                let _ = done.await;
            }
        }
    }
}

/// Answers jobs from the pool's queue, one at a time, until the queue closes.
fn serve_queries(engine: &QueryEngine, precompute: &Shared, jobs: &Mutex<Receiver<QueryJob>>) {
    loop {
        let job = match jobs.lock().expect("query queue lock").recv() {
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
                let _ = reply.send(engine.raw_ranges(&query, time_ms, store));
            }
            QueryJob::Evaluate {
                root,
                time_ms,
                raw,
                reply,
            } => {
                let watermark = precompute.watermark_ms();
                let _ = reply.send(engine.evaluate(root, time_ms, store, watermark, &raw));
            }
            QueryJob::RawRangesForSteps {
                query,
                steps,
                reply,
            } => {
                let _ = reply.send(engine.raw_ranges_for_steps(&query, &steps, store));
            }
            QueryJob::EvaluateSteps {
                root,
                steps,
                raw,
                reply,
            } => {
                // One watermark for every step of the request.
                let watermark = precompute.watermark_ms();
                let _ = reply.send(engine.evaluate_steps(root, &steps, store, watermark, &raw));
            }
        }
    }
}
