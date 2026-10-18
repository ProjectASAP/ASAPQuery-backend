//! The engine thread: owns the installed plan and everything that executes
//! it. Planner IR holds `Rc`s, so the plan cannot be shared across the HTTP
//! server's threads; handlers send the engine commands instead.

use std::collections::BTreeMap;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};

use asap_executor::physical_planner::frontier_from_timing;
use control_plane::InstalledPlan;
use tokio::sync::oneshot;

use crate::precompute::{Precompute, PrecomputeStats, RawSample};
use crate::query::{QueryEngine, RawRange, ResultSample, Unanswered};
use crate::sds::Sds;

/// Commands queued for the engine before ingestion pushes back.
const QUEUE: usize = 1024;

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

pub enum Command {
    Ingest(Vec<RawSample>),
    Stats(oneshot::Sender<PrecomputeStats>),
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

    /// The plan root answering `query` at `time_ms` and the raw samples it
    /// needs; see [`QueryEngine::raw_ranges`].
    pub async fn raw_ranges(
        &self,
        query: String,
        time_ms: i64,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        self.ask(|reply| Command::RawRanges {
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

    /// The plan root answering `query` at every one of `steps` and the raw
    /// samples they need; see [`QueryEngine::raw_ranges_for_steps`].
    pub async fn raw_ranges_for_steps(
        &self,
        query: String,
        steps: Vec<i64>,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        self.ask(|reply| Command::RawRangesForSteps {
            query,
            steps,
            reply,
        })
        .await
    }

    /// Runs a root at every one of `steps`; see [`QueryEngine::evaluate_steps`].
    pub async fn evaluate_steps(
        &self,
        root: usize,
        steps: Vec<i64>,
        raw: BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<Vec<ResultSample>>, Unanswered> {
        self.ask(|reply| Command::EvaluateSteps {
            root,
            steps,
            raw,
            reply,
        })
        .await
    }

    async fn ask<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T, Unanswered> {
        let busy = || Unanswered::Execution("the engine is busy".into());
        let (reply, answer) = oneshot::channel();
        self.commands.try_send(command(reply)).map_err(|_| busy())?;
        answer.await.map_err(|_| busy())?
    }

    /// Precompute counters, once every command queued before has run.
    pub async fn stats(&self) -> PrecomputeStats {
        let (reply, stats) = oneshot::channel();
        if self.commands.try_send(Command::Stats(reply)).is_err() {
            return PrecomputeStats::default();
        }
        stats.await.unwrap_or_default()
    }
}

struct Engine {
    precompute: Precompute,
    query: QueryEngine,
}

impl Engine {
    fn new(plan_json: &str, allowed_lateness_ms: i64) -> anyhow::Result<Self> {
        let plan: InstalledPlan = serde_json::from_str(plan_json)?;
        plan.validate()?;
        let frontier = frontier_from_timing(&plan.dag.dag)?;
        let sds = Sds::from_dag(&plan.dag.dag, &frontier)?;
        tracing::info!(
            definitions = sds.definitions.len(),
            outputs = sds.outputs.len(),
            kept_definitions = sds.kept_definitions.len(),
            kept_outputs = sds.kept_outputs.len(),
            "stored summaries"
        );
        let query = QueryEngine::new(&plan, &sds);
        Ok(Self {
            precompute: Precompute::new(plan.dag.dag, &sds, allowed_lateness_ms)?,
            query,
        })
    }

    fn serve(mut self, commands: Receiver<Command>) {
        for command in commands {
            match command {
                Command::Ingest(samples) => self.precompute.ingest(&samples),
                Command::Stats(reply) => {
                    let _ = reply.send(self.precompute.stats);
                }
                Command::RawRanges {
                    query,
                    time_ms,
                    reply,
                } => {
                    let _ = reply.send(self.query.raw_ranges(
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
                        self.precompute.watermark_ms(),
                        &raw,
                    ));
                }
                Command::RawRangesForSteps {
                    query,
                    steps,
                    reply,
                } => {
                    let _ = reply.send(self.query.raw_ranges_for_steps(
                        &query,
                        &steps,
                        &self.precompute.store,
                    ));
                }
                Command::EvaluateSteps {
                    root,
                    steps,
                    raw,
                    reply,
                } => {
                    // One watermark for every step of the request.
                    let _ = reply.send(self.query.evaluate_steps(
                        root,
                        &steps,
                        &self.precompute.store,
                        self.precompute.watermark_ms(),
                        &raw,
                    ));
                }
            }
        }
    }
}
