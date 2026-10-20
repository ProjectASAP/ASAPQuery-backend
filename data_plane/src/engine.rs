//! The engine thread: owns the installed plan and everything that executes
//! it. Planner IR holds `Rc`s, so the plan cannot be shared across the HTTP
//! server's threads; handlers send the engine commands instead.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

use control_plane::InstalledPlan;

use crate::precompute::{Precompute, PrecomputeOptions, PrecomputeStats, RawSample};
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

enum Command {
    Ingest(Vec<RawSample>),
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
            let total = queued
                .checked_add(count)
                .filter(|&total| total <= self.max_queued_samples)
                .ok_or(IngestError::Busy)?;
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

    /// Stops the engine thread after the commands queued before; later
    /// ingestion fails with [`IngestError::Stopped`].
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
    }
}

/// What the engine thread publishes to the HTTP side.
struct Shared {
    queued_samples: Arc<AtomicU64>,
    stats: Arc<Mutex<PrecomputeStats>>,
}

struct Engine {
    precompute: Precompute,
}

impl Engine {
    fn new(plan_json: &str, options: PrecomputeOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let plan: InstalledPlan = serde_json::from_str(plan_json)?;
        plan.validate()?;
        let dag = plan.dag.dag;
        let frontier = Sds::ingestion_frontier(&dag)?;
        let sds = Sds::from_dag(&dag, &frontier)?;
        tracing::info!(
            definitions = sds.definitions.len(),
            outputs = sds.outputs.len(),
            "stored summaries"
        );
        let engine = Self {
            precompute: Precompute::new(dag, &sds, options)?,
        };
        Ok((engine, plan.queries))
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
                Command::Stop => break,
            }
        }
        tracing::warn!("engine stopped");
    }
}
