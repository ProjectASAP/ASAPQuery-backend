//! The engine thread: owns the installed plan and everything that executes
//! it. Planner IR holds `Rc`s, so the plan cannot be shared across the HTTP
//! server's threads; handlers send the engine commands instead.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use asap_planner::InstallablePlan;

use tokio::sync::oneshot;

use crate::precompute::{Precompute, PrecomputeOptions, PrecomputeStats, RawSample};
use crate::query::{BoundRawSampleWindow, QueryEngine, ResultSample, Unanswered};
use crate::sds::Sds;

/// How the engine thread ingests samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and pane checks of the precompute engine.
    pub precompute: PrecomputeOptions,
    /// Samples queued for the engine, not yet ingested, beyond which Remote
    /// Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            precompute: PrecomputeOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

enum Command {
    Ingest(Vec<RawSample>),
    BoundRawSampleWindows {
        query: String,
        time_ms: i64,
        reply: Reply<(usize, Vec<BoundRawSampleWindow>)>,
    },
    Evaluate {
        root: usize,
        time_ms: i64,
        raw: BTreeMap<u64, Vec<RawSample>>,
        reply: Reply<Vec<ResultSample>>,
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
    /// The counters as of the last command the engine ran.
    stats: Arc<Mutex<PrecomputeStats>>,
}

/// Marks the engine stopped when its thread exits, panics included.
struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl EngineHandle {
    /// Starts the engine for the plan serialized in `plan_json`, parsed once
    /// on the engine thread, and returns the plan's queries. Fails if the
    /// plan is invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let (commands, receiver) = channel();
        let (ready, started) = std::sync::mpsc::channel();
        let running = Arc::new(AtomicBool::new(true));
        let queued_samples = Arc::new(AtomicU64::new(0));
        let stats = Arc::new(Mutex::new(PrecomputeStats::default()));
        let shared = Shared {
            queued_samples: queued_samples.clone(),
            stats: stats.clone(),
        };
        let guard = RunningGuard(running.clone());
        std::thread::Builder::new()
            .name("engine".into())
            .spawn(move || {
                let _guard = guard;
                match Engine::new(&plan_json, options.precompute) {
                    Ok((engine, queries)) => {
                        let _ = ready.send(Ok(queries));
                        engine.serve(receiver, shared);
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })?;
        let queries = started.recv()??;
        let handle = Self {
            commands,
            queued_samples,
            max_queued_samples: options.max_queued_samples,
            running,
            stats,
        };
        Ok((handle, queries))
    }

    /// Queues `samples` for precompute. `Busy` when the queue would hold
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

    /// The plan root answering `query` at `time_ms` and the raw samples it
    /// needs; see [`QueryEngine::bound_raw_sample_windows`].
    pub async fn bound_raw_sample_windows(
        &self,
        query: String,
        time_ms: i64,
    ) -> Result<(usize, Vec<BoundRawSampleWindow>), Unanswered> {
        self.ask(|reply| Command::BoundRawSampleWindows {
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
        self.ask(|reply| Command::Evaluate {
            root,
            time_ms,
            raw,
            reply,
        })
        .await
    }

    async fn ask<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T, Unanswered> {
        let stopped = || Unanswered::Execution("the engine has stopped".into());
        let (reply, answer) = oneshot::channel();
        self.commands.send(command(reply)).map_err(|_| stopped())?;
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

    /// The precompute counters as of the last batch the engine ingested.
    pub fn stats(&self) -> PrecomputeStats {
        *self.stats.lock().unwrap_or_else(|e| e.into_inner())
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
    stats: Arc<Mutex<PrecomputeStats>>,
}

struct Engine {
    precompute: Precompute,
    query: QueryEngine,
}

impl Engine {
    fn new(plan_json: &str, options: PrecomputeOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let plan: InstallablePlan = serde_json::from_str(plan_json)?;
        plan.validate()?;
        let sds = Sds::from_plan(&plan)?;
        tracing::info!(
            definitions = sds.definitions.len(),
            outputs = sds.outputs.len(),
            "stored summaries"
        );
        let query_run = sds
            .definitions
            .values()
            .filter(|d| d.built_at == asap_planner::StoredOutputBuild::QueryRun)
            .count();
        if query_run > 0 {
            tracing::warn!(
                definitions = query_run,
                "the plan has stored outputs built in a QueryRun, which this data plane does not build yet; \
                 queries that read them are forwarded"
            );
        }
        let queries = plan.queries.iter().map(|q| q.query.clone()).collect();
        let query = QueryEngine::new(&plan, &sds);
        let engine = Self {
            precompute: Precompute::new(plan.dag.dag, &sds, options)?,
            query,
        };
        Ok((engine, queries))
    }

    fn serve(mut self, commands: Receiver<Command>, shared: Shared) {
        for command in commands {
            match command {
                Command::Ingest(samples) => {
                    self.precompute.ingest(&samples);
                    *shared.stats.lock().unwrap_or_else(|e| e.into_inner()) = self.precompute.stats;
                    shared
                        .queued_samples
                        .fetch_sub(samples.len() as u64, Ordering::SeqCst);
                }
                Command::BoundRawSampleWindows {
                    query,
                    time_ms,
                    reply,
                } => {
                    let _ = reply.send(self.query.bound_raw_sample_windows(
                        &query,
                        time_ms,
                        &self.precompute.store,
                    ));
                }
                Command::Evaluate {
                    root,
                    time_ms,
                    raw,
                    reply,
                } => {
                    let _ = reply.send(self.query.evaluate(
                        root,
                        time_ms,
                        &self.precompute.store,
                        &raw,
                    ));
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
