//! The `query-N` workers: each runs one query to completion over the
//! newest published snapshot, so a query never waits for ingestion. A
//! run's output stream is not `Send`, so a run never moves between
//! threads.

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use internal_metrics::{Metrics, Timer};
use storage::store::PublishedStore;
use tokio::sync::oneshot;

use crate::result_cache::Answer;
use crate::{QueryEngine, Snapshot, Unanswered};

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

/// The query workers and the compiled queries they run.
#[derive(Clone)]
pub struct QueryWorkers {
    /// Queries for the worker threads.
    queries: Sender<QueryJob>,
    query: Arc<QueryEngine>,
    threads: usize,
    published: Arc<PublishedStore>,
    metrics: Arc<Metrics>,
}

impl QueryWorkers {
    /// Starts `threads` workers running `query`'s queries over the
    /// snapshots `published` holds, recording into `metrics`.
    pub fn start(
        query: QueryEngine,
        published: Arc<PublishedStore>,
        metrics: Arc<Metrics>,
        threads: usize,
    ) -> std::io::Result<Self> {
        let threads = threads.max(1);
        let query = Arc::new(query);
        let (queries, jobs) = channel::<QueryJob>();
        spawn_query_workers(
            query.clone(),
            published.clone(),
            metrics.clone(),
            jobs,
            threads,
        )?;
        Ok(Self {
            queries,
            query,
            threads,
            published,
            metrics,
        })
    }

    /// Answers the planned `query` at `time_ms` on a query worker, from
    /// the newest snapshot published when the run starts; see
    /// [`QueryEngine::evaluate`]. Two queries at one evaluation time may
    /// read different snapshots. A query does not wait for writes
    /// acknowledged before it; an event-time barrier does
    /// (`advance_watermark` of the precompute engine).
    pub async fn evaluate(&self, query: String, time_ms: i64) -> Result<Answer, Unanswered> {
        let started = std::time::Instant::now();
        let metrics = self.metrics.query(&query);
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
        Arc::new(self.query.snapshot(&self.published))
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
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// The compiled queries.
    pub fn engine(&self) -> &QueryEngine {
        &self.query
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

/// Starts `threads` workers `query-0`, `query-1`, …, each running one query
/// at a time to completion from `jobs`; more queries wait in `jobs`.
fn spawn_query_workers(
    query: Arc<QueryEngine>,
    published: Arc<PublishedStore>,
    metrics: Arc<Metrics>,
    jobs: Receiver<QueryJob>,
    threads: usize,
) -> std::io::Result<()> {
    let jobs = Arc::new(Mutex::new(jobs));
    for index in 0..threads {
        let (query, published, metrics, jobs) = (
            query.clone(),
            published.clone(),
            metrics.clone(),
            jobs.clone(),
        );
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
                        None => Arc::new(query.snapshot(&published)),
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
                if let Some(metrics) = metrics.query(&job.query) {
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
