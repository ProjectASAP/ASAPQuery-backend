//! The engine thread: owns the installed plan and everything that executes
//! it. Planner IR holds `Rc`s, so the plan cannot be shared across the HTTP
//! server's threads; handlers send the engine commands instead.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};

use asap_executor::physical_planner::frontier_from_timing;
use control_plane::InstalledPlan;
use tokio::sync::oneshot;

use crate::precompute::{Precompute, PrecomputeStats, RawSample};
use crate::sds::Sds;

/// Commands queued for the engine before ingestion pushes back.
const QUEUE: usize = 1024;

pub enum Command {
    Ingest(Vec<RawSample>),
    Stats(oneshot::Sender<PrecomputeStats>),
}

#[derive(Debug, thiserror::Error)]
#[error("the engine is busy")]
pub struct Busy;

/// The HTTP side of the engine thread.
#[derive(Clone)]
pub struct EngineHandle {
    commands: SyncSender<Command>,
}

impl EngineHandle {
    /// Starts the engine for the plan serialized in `plan_json`. Fails if the
    /// plan is invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, allowed_lateness_ms: i64) -> anyhow::Result<Self> {
        let (commands, receiver) = sync_channel(QUEUE);
        let (ready, started) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("engine".into())
            .spawn(move || match Engine::new(&plan_json, allowed_lateness_ms) {
                Ok(engine) => {
                    let _ = ready.send(Ok(()));
                    engine.serve(receiver);
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            })?;
        started.recv()??;
        Ok(Self { commands })
    }

    /// Queues `samples` for precompute, or fails if the engine is behind.
    pub fn try_ingest(&self, samples: Vec<RawSample>) -> Result<(), Busy> {
        match self.commands.try_send(Command::Ingest(samples)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => Err(Busy),
        }
    }

    /// Precompute counters, once every command queued before has run.
    pub async fn stats(&self) -> PrecomputeStats {
        let (reply, stats) = oneshot::channel();
        let _ = self.commands.send(Command::Stats(reply));
        stats.await.unwrap_or_default()
    }
}

struct Engine {
    precompute: Precompute,
}

impl Engine {
    fn new(plan_json: &str, allowed_lateness_ms: i64) -> anyhow::Result<Self> {
        let plan: InstalledPlan = serde_json::from_str(plan_json)?;
        plan.validate()?;
        let dag = plan.dag.dag;
        let frontier = frontier_from_timing(&dag)?;
        let sds = Sds::from_dag(&dag, &frontier)?;
        tracing::info!(
            definitions = sds.definitions.len(),
            outputs = sds.outputs.len(),
            "stored summaries"
        );
        Ok(Self {
            precompute: Precompute::new(dag, &sds, allowed_lateness_ms)?,
        })
    }

    fn serve(mut self, commands: Receiver<Command>) {
        for command in commands {
            match command {
                Command::Ingest(samples) => self.precompute.ingest(&samples),
                Command::Stats(reply) => {
                    let _ = reply.send(self.precompute.stats);
                }
            }
        }
    }
}
