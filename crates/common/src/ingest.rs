use async_trait::async_trait;

use crate::{InstallError, InstalledPlan, LabelSet};

/// Raw input data: what an ingest adapter hands the ingest engine and what
/// the raw-data store appends and returns. Never data between operators.
#[derive(Debug, Clone, PartialEq)]
pub enum RawDataBatch {
    /// Samples of several series: each series' labels and its
    /// `(timestamp_ms, value)` samples.
    TimeSeries {
        series: Vec<(LabelSet, Vec<(i64, f64)>)>,
    },
}

/// The engine that runs the installed plan's ingest-time part: it builds
/// the plan's views from raw data and keeps the raw data the plan reads.
/// It closes windows on its own timer.
#[async_trait]
pub trait IngestEngine: Send + Sync {
    /// Replaces the plan it runs. On an error, the current plan stays.
    fn install(&self, plan: InstalledPlan) -> Result<(), InstallError>;

    /// Takes one batch, waiting while the engine cannot accept it
    /// (backpressure). Discards it before the first plan is installed.
    async fn ingest(&self, batch: RawDataBatch) -> Result<(), IngestError>;

    /// Stops the engine, best effort.
    fn shutdown(&self);
}

/// The ingest engine is gone and accepts no more batches.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct IngestError {
    pub message: String,
}

impl IngestError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
