//! The stores the deployment owns and injects into both engines: the ingest
//! engine writes them, the query engine reads and evicts.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use asap_planner::{PhysicalASAPNodeId, RawDataRetention};
use asap_types::ir::operator::arguments::Source;

use crate::{PlanId, RawDataBatch};

/// A materialized view: its node in plan `plan`, which fixes what it
/// computes (sub-DAG, selection, sketch parameters, window widths).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ViewId {
    pub plan: PlanId,
    pub node: PhysicalASAPNodeId,
}

/// One window of one view: the view and its absolute window
/// `(start_ms, end_ms]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ViewInstanceKey {
    pub view: ViewId,
    pub window_ms: (i64, i64),
}

/// A view's stored state for one window, for all of the window's group
/// keys.
#[derive(Debug, Clone)]
pub struct ViewInstance {
    pub state: ViewState,
    pub metadata: ViewInstanceMetadata,
}

/// What the store knows about an instance besides its opaque state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewInstanceMetadata {
    /// What the state occupies in memory, for the store's memory budget.
    pub size_bytes: u64,
}

/// The engine's native state of one view instance, opaque to the store;
/// the query engine downcasts it.
#[derive(Debug, Clone)]
pub struct ViewState(pub Arc<dyn Any + Send + Sync>);

/// Stored view instances, in memory, on disk or both; the implementation
/// chooses its indexing and never looks inside a state.
pub trait ViewStore: Send + Sync {
    /// Stores an instance. Each key is put once and never rewritten.
    fn put(&self, key: ViewInstanceKey, instance: ViewInstance);

    /// The instance stored under `key`, if any.
    fn get(&self, key: &ViewInstanceKey) -> Option<ViewInstance>;

    /// Each stored instance of `view` whose window lies within
    /// `interval_ms` `(start_ms, end_ms]`, with its window.
    fn instances(&self, view: &ViewId, interval_ms: (i64, i64)) -> Vec<((i64, i64), ViewInstance)>;

    /// Drops the instances of `view` whose window ends at or before
    /// `before_ms`.
    fn evict_before(&self, view: &ViewId, before_ms: i64);

    /// Per view with stored instances, the start of its oldest window and
    /// the end of its newest, in ms; for runtime info.
    fn timestamp_range_per_view(&self) -> HashMap<ViewId, (i64, i64)>;
}

/// Stored raw data, per source of the plan's `Scan`s.
pub trait RawDataStore: Send + Sync {
    /// Appends a batch of `source`'s raw data.
    fn append(&self, source: &Source, batch: RawDataBatch);

    /// `source`'s stored raw data with timestamps in `interval_ms`
    /// `(start_ms, end_ms]`, or `None` when the store no longer holds the
    /// whole interval (evicted), so the query engine reports `NotReady`.
    fn read(&self, source: &Source, interval_ms: (i64, i64)) -> Option<RawDataBatch>;

    /// Drops each listed source's raw data at or before `now_ms` minus its
    /// retention's lookback; a source with no lookback keeps all of it.
    fn evict(&self, retention: &[RawDataRetention], now_ms: i64);
}
