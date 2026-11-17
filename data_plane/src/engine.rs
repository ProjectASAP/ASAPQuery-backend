//! The engine thread: owns the installed plan, compiled once when it is
//! installed, the store, and everything that executes the plan; handlers
//! send the engine commands. Runs execute one at a time, so the store
//! changes only between runs.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use asap_executor::runtime::Limits;
use asap_planner::InstallablePlan;

use tokio::sync::oneshot;

use crate::ingestion::{Ingestion, IngestionOptions, IngestionStats};
use crate::query::{QueryEngine, ResultSample, Unanswered};
use crate::store::{scanned_metrics, RawSample, TimeIndexedStore};

/// How the engine thread ingests samples and runs queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and first-window checks of ingestion.
    pub ingestion: IngestionOptions,
    /// Samples queued for the engine, not yet ingested, beyond which Remote
    /// Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
    /// Every answer must be computed from the plan: a plan with a query the
    /// query engine cannot run is refused at install, and the data plane
    /// never forwards a query to Prometheus (`--no-forward`).
    pub no_forward: bool,
    /// The most bytes one ingestion or query run may hold
    /// (`asap_executor::runtime::Limits::max_bytes`).
    pub run_memory_limit_bytes: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            ingestion: IngestionOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
            no_forward: false,
            run_memory_limit_bytes: 4 << 30,
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

enum Command {
    Ingest(Vec<RawSample>),
    Evaluate {
        query: String,
        time_ms: i64,
        reply: Reply<Vec<ResultSample>>,
    },
    AdvanceWatermark {
        barrier_ms: i64,
        reply: tokio::sync::oneshot::Sender<i64>,
    },
    Stop,
}

/// Why the engine did not take samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// More samples are queued than `max_queued_samples`; retry later.
    #[error("the engine is busy: too many samples queued")]
    Busy,
    /// The engine thread has stopped; no sample will be ingested again.
    #[error("the engine has stopped")]
    Stopped,
}

/// The HTTP side of the engine thread.
#[derive(Clone)]
pub struct EngineHandle {
    commands: Sender<Command>,
    /// Samples sent and not yet ingested.
    queued_samples: Arc<AtomicU64>,
    max_queued_samples: u64,
    /// False once the engine thread has exited, for any reason.
    running: Arc<AtomicBool>,
    /// The ingestion counters as of the last command the engine ran.
    stats: Arc<Mutex<IngestionStats>>,
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
    /// compiled once on the engine thread, and returns the plan's queries.
    /// Fails if the plan is invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let (commands, receiver) = channel();
        let (installed, installation) = channel();
        let running = Arc::new(AtomicBool::new(true));
        let queued_samples = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(Mutex::new(IngestionStats::default()));
        let shared = Shared {
            queued_samples: queued_samples.clone(),
            stats: stats.clone(),
        };
        let guard = RunningGuard(running.clone());
        std::thread::Builder::new()
            .name("engine".into())
            .spawn(move || {
                let _guard = guard;
                match Engine::new(&plan_json, options) {
                    Ok((engine, queries)) => {
                        let _ = installed.send(Ok(queries));
                        engine.serve(receiver, shared);
                    }
                    Err(error) => {
                        let _ = installed.send(Err(error));
                    }
                }
            })?;
        let queries = installation
            .recv()
            .map_err(|_| anyhow::anyhow!("the engine thread exited while installing"))??;
        let handle = Self {
            commands,
            queued_samples,
            max_queued_samples: options.max_queued_samples,
            running,
            stats,
        };
        Ok((handle, queries))
    }

    /// Queues `samples` for ingestion. `Busy` when the queue would hold
    /// more than `max_queued_samples`; `Stopped` when the engine has exited.
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
        self.commands.send(Command::Ingest(samples)).map_err(|_| {
            self.queued_samples.fetch_sub(count, Ordering::SeqCst);
            IngestError::Stopped
        })
    }

    /// Answers the planned `query` at `time_ms`, after every write
    /// acknowledged before; see [`QueryEngine::evaluate`].
    pub async fn evaluate(
        &self,
        query: String,
        time_ms: i64,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        let stopped = || Unanswered::Execution("the engine has stopped".into());
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(Command::Evaluate {
                query,
                time_ms,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// Whether the engine thread is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Samples queued and not yet ingested.
    pub fn queued_samples(&self) -> u64 {
        self.queued_samples.load(Ordering::SeqCst)
    }

    /// The ingestion counters as of the last command the engine ran.
    pub fn stats(&self) -> IngestionStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queues an event-time barrier behind every write already acknowledged
    /// and waits until it has run: every window ending at or before
    /// `barrier_ms` (capped at the wall clock) is then refreshed. Returns
    /// the barrier in force; see [`Ingestion::advance_watermark_at`].
    pub async fn advance_watermark(&self, barrier_ms: i64) -> Result<i64, IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        let (reply, done) = tokio::sync::oneshot::channel();
        self.commands
            .send(Command::AdvanceWatermark { barrier_ms, reply })
            .map_err(|_| IngestError::Stopped)?;
        done.await.map_err(|_| IngestError::Stopped)
    }

    /// For tests: stops the engine thread after the commands queued before.
    /// Samples queued between this call and the stop are acknowledged and
    /// then lost; ingestion after the stop fails with
    /// [`IngestError::Stopped`].
    #[doc(hidden)]
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
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

/// What the engine thread publishes to the HTTP side.
struct Shared {
    queued_samples: Arc<AtomicU64>,
    stats: Arc<Mutex<IngestionStats>>,
}

struct Engine {
    store: TimeIndexedStore,
    ingestion: Ingestion,
    query: QueryEngine,
}

impl Engine {
    fn new(plan_json: &str, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
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
        let ingestion = Ingestion::new(&plan.dag, &views, groups, options.ingestion, limits)?;
        let store = TimeIndexedStore::for_metrics(&scanned_metrics(&plan.dag.nodes));
        let queries = plan.query_texts.clone();
        Ok((
            Self {
                store,
                ingestion,
                query,
            },
            queries,
        ))
    }

    fn serve(mut self, commands: Receiver<Command>, shared: Shared) {
        for command in commands {
            match command {
                Command::Ingest(samples) => {
                    let count = samples.len() as u64;
                    self.ingestion.ingest(&mut self.store, samples);
                    *shared.stats.lock().unwrap_or_else(|e| e.into_inner()) = self.ingestion.stats;
                    shared.queued_samples.fetch_sub(count, Ordering::SeqCst);
                }
                Command::Evaluate {
                    query,
                    time_ms,
                    reply,
                } => {
                    let _ = reply.send(self.query.evaluate(&query, time_ms, &self.store));
                }
                Command::AdvanceWatermark { barrier_ms, reply } => {
                    let barrier_ms = self
                        .ingestion
                        .advance_watermark(&mut self.store, barrier_ms);
                    *shared.stats.lock().unwrap_or_else(|e| e.into_inner()) = self.ingestion.stats;
                    let _ = reply.send(barrier_ms);
                }
                Command::Stop => break,
            }
        }
        tracing::warn!("engine stopped");
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
