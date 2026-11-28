//! Collector view states: windowed summary states a collector built for a
//! materialized view of the installed plan, as the ingest adapter decoded
//! them (docs/design/collector-sketch-ingest.md, §4 and §6). Nested as the
//! OTAP tiers they travel in: view (Schema) → series (Dictionary) → state
//! (Record).

pub use asap_types::plan::materialized_view::ViewStateKey;

use crate::{LabelSet, PhysicalASAPNodeId};

/// The collector states of one request: what an ingest adapter hands the
/// precompute engine. Here because the adapter depends only on `common`
/// and builds it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ViewStateBatch {
    pub views: Vec<CollectorViewStates>,
}

impl ViewStateBatch {
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }
}

/// One collector view's states (Schema tier): the view identity its
/// metric's metadata carries.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorViewStates {
    /// `PhysicalASAPDAG::plan_id()` of the plan the collector loaded.
    pub plan_id: String,
    pub node_id: PhysicalASAPNodeId,
    /// The view's `MaterializedView::view_state_key()`, as the collector
    /// read it from its copy of the plan.
    pub key: ViewStateKey,
    pub series: Vec<CollectorSeriesStates>,
}

/// One series' states (Dictionary tier).
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorSeriesStates {
    /// Built by the adapter's label function for raw samples; `__name__`
    /// is the source metric (the metric's name before `.asap_view.`).
    pub labels: LabelSet,
    pub records: Vec<ViewStateRecord>,
}

/// One state of one window `(window_start_ms, window_end_ms]` (Record tier).
#[derive(Debug, Clone, PartialEq)]
pub struct ViewStateRecord {
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    /// The full state, as `asap_sketchlib`'s ASAPv1 bytes.
    pub asapv1: Vec<u8>,
    /// Samples folded into the state, as the collector counted them.
    pub sample_count: u64,
}

/// The view identity's metric metadata keys and the reserved part of a
/// collector view's metric name.
pub mod view_identity {
    pub const PLAN_ID: &str = "asap.plan_id";
    pub const NODE_ID: &str = "asap.node_id";
    pub const VIEW_STATE_KEY: &str = "asap.view_state_key";
    /// A collector view's metric is `<source metric>.asap_view.<node_id>`.
    pub const NAME_INFIX: &str = ".asap_view.";
    /// The data point attribute holding a state's ASAPv1 bytes.
    pub const ENVELOPE_ATTRIBUTE: &str = "sketch.envelope";
}
