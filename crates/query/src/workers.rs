//! The `query-N` workers: each runs one query to completion over the
//! newest published snapshot, so a query never waits for ingestion. A
//! run's output stream is not `Send`, so a run never moves between
//! threads.

use std::sync::atomic::Ordering;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use common::{AnswerSource, PlanId};
use internal_metrics::Timer;
use storage::store::PublishedStore;
use tokio::sync::oneshot;

use crate::{CompiledQueries, Snapshot, Unanswered};
use common::Answer;

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

/// The installed plan's compiled queries and the `query-N` workers that
/// run them.
#[derive(Clone)]
pub struct InstalledQueries {
    /// The plan the queries were compiled from.
    plan: PlanId,
    /// Queries for the worker threads.
    queries: Sender<QueryJob>,
    compiled: Arc<CompiledQueries>,
    threads: usize,
    published: Arc<PublishedStore>,
}

impl InstalledQueries {
    /// Starts `threads` workers running `compiled`, plan `plan`'s queries,
    /// over the snapshots `published` holds.
    pub(crate) fn start(
        plan: PlanId,
        compiled: CompiledQueries,
        published: Arc<PublishedStore>,
        threads: usize,
    ) -> std::io::Result<Self> {
        let threads = threads.max(1);
        let compiled = Arc::new(compiled);
        let (queries, jobs) = channel::<QueryJob>();
        spawn_query_workers(compiled.clone(), published.clone(), jobs, threads)?;
        Ok(Self {
            plan,
            queries,
            compiled,
            threads,
            published,
        })
    }

    /// The root `source` names, if it was resolved against this plan.
    fn root(&self, source: &AnswerSource) -> Result<PhysicalASAPNodeId, Unanswered> {
        match *source {
            AnswerSource::Planned { plan, root } if plan == self.plan => Ok(root),
            AnswerSource::Planned { .. } => Err(Unanswered::PlanChanged),
        }
    }

    /// Answers `query`, resolved to `source`, at `time_ms`: from the result
    /// cache, or on a query worker from the newest snapshot published when
    /// the run starts ([`CompiledQueries::evaluate`]). Two queries at one
    /// evaluation time may read different snapshots. A query does not wait
    /// for writes acknowledged before it; an event-time barrier does
    /// (`advance_watermark` of the precompute engine).
    pub async fn instant(
        &self,
        source: &AnswerSource,
        query: &str,
        time_ms: i64,
    ) -> Result<Answer, Unanswered> {
        let root = self.root(source)?;
        if let Some(answer) = self.compiled.cached(root, time_ms) {
            return Ok(answer);
        }
        let started = std::time::Instant::now();
        let metrics = self.compiled.metrics_index(query);
        let answer = self.run_on_worker(root, metrics, time_ms, None).await;
        if let Some(index) = metrics {
            let metrics = &self.compiled.metrics()[index].1;
            metrics.latency.observe(started.elapsed().as_secs_f64());
        }
        answer
    }

    /// Answers `query`, resolved to `source`, at every time of `times_ms`,
    /// the steps in parallel on the query workers; unanswered if any step
    /// is.
    pub async fn range(
        &self,
        source: &AnswerSource,
        query: &str,
        times_ms: impl Iterator<Item = i64>,
    ) -> Result<Vec<(i64, Answer)>, Unanswered> {
        use futures::StreamExt;
        let root = self.root(source)?;
        let started = std::time::Instant::now();
        let metrics = self.compiled.metrics_index(query);
        // The steps the result cache answers; only the others run.
        let cached: Vec<(i64, Option<Answer>)> = times_ms
            .map(|time_ms| (time_ms, self.compiled.cached(root, time_ms)))
            .collect();
        let from_cache = cached.iter().filter(|(_, hit)| hit.is_some()).count();
        // Every step reads the same snapshot, so the raw data they share is
        // converted once ([`storage::store::RawReads`]).
        let snapshot = self.snapshot();
        // At most one step per query worker in flight, so a long range neither
        // floods the workers' queue nor starves instant queries; the client
        // going away drops the rest unrun. Every step runs even when one fails:
        // with `final_query_results_cached` the plan keeps a step's inputs only
        // for the refresh interval, so the older steps of a span are answered
        // from the result cache, and a request whose older steps are not cached
        // yet (the first span after a start) still caches the steps it can
        // answer; a dashboard refreshing the range fills the cache within one
        // span.
        let results: Vec<Result<_, Unanswered>> = futures::stream::iter(cached)
            .map(|(time_ms, hit)| {
                let snapshot = snapshot.clone();
                async move {
                    let samples = match hit {
                        Some(samples) => samples,
                        None => {
                            self.run_on_worker(root, metrics, time_ms, Some(snapshot))
                                .await?
                        }
                    };
                    Ok::<_, Unanswered>((time_ms, samples))
                }
            })
            .buffered(self.threads)
            .collect()
            .await;
        let steps: Vec<_> = results.into_iter().collect::<Result<_, _>>()?;
        if let Some(index) = metrics {
            let metrics = &self.compiled.metrics()[index].1;
            metrics
                .range_latency
                .observe(started.elapsed().as_secs_f64());
            metrics
                .range_steps_cached
                .fetch_add(from_cache as u64, Ordering::Relaxed);
            metrics
                .range_steps
                .fetch_add(steps.len() as u64, Ordering::Relaxed);
        }
        Ok(steps)
    }

    /// The newest published store, for the steps of one range request.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::new(self.compiled.snapshot(&self.published))
    }

    /// Runs `root` at `time_ms` on a worker, over `range_step`'s snapshot
    /// (one step of a range query) or the newest; `metrics` indexes the
    /// query's measurements. A step whose future is dropped before a worker
    /// takes it (the range failed or its client went away) is not run.
    async fn run_on_worker(
        &self,
        root: PhysicalASAPNodeId,
        metrics: Option<usize>,
        time_ms: i64,
        range_step: Option<Arc<Snapshot>>,
    ) -> Result<Answer, Unanswered> {
        let stopped = || Unanswered::Execution("the query workers have stopped".into());
        let (reply, answer) = oneshot::channel();
        self.queries
            .send(QueryJob {
                root,
                metrics,
                time_ms,
                range_step,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// The plan the queries were compiled from.
    pub fn plan(&self) -> PlanId {
        self.plan
    }

    /// The compiled queries.
    pub fn compiled(&self) -> &CompiledQueries {
        &self.compiled
    }
}

/// One query for a worker thread.
struct QueryJob {
    root: PhysicalASAPNodeId,
    /// The query's measurements in [`CompiledQueries::metrics`], if planned.
    metrics: Option<usize>,
    time_ms: i64,
    /// One step of a range query, and the snapshot every step reads.
    range_step: Option<Arc<Snapshot>>,
    reply: Reply<Answer>,
}

/// Starts `threads` workers `query-0`, `query-1`, …, each running one query
/// at a time to completion from `jobs`; more queries wait in `jobs`.
fn spawn_query_workers(
    compiled: Arc<CompiledQueries>,
    published: Arc<PublishedStore>,
    jobs: Receiver<QueryJob>,
    threads: usize,
) -> std::io::Result<()> {
    let jobs = Arc::new(Mutex::new(jobs));
    for index in 0..threads {
        let (compiled, published, jobs) = (compiled.clone(), published.clone(), jobs.clone());
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
                let metrics = job.metrics.map(|index| &compiled.metrics()[index]);
                // A bug in one run must not take the worker down: it
                // answers that run with the error and keeps serving.
                let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = match &job.range_step {
                        Some(snapshot) => snapshot.clone(),
                        None => Arc::new(compiled.snapshot(&published)),
                    };
                    compiled.evaluate(job.root, job.time_ms, &snapshot)
                }))
                .unwrap_or_else(|panic| {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(ToString::to_string)
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    let query = metrics.map_or("", |(query, _)| query.as_str());
                    tracing::error!(query, root = job.root, time_ms = job.time_ms, %reason, "query run panicked");
                    Err(Unanswered::Execution(format!("the query run panicked: {reason}")))
                });
                if let Some((_, metrics)) = metrics {
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
