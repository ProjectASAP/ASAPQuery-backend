//! Backend-facing projection of one planning cycle.
//!
//! These types are the input to [`crate::backend_plan::from_stage_config`] and
//! to `emit::backend_wire`'s backend JSON builders. `PhysicalCompiler` builds
//! them directly from the summaries ASAPPlanner selected.
//!
//! They are deliberately not `Serialize`/`Deserialize`: `SummaryFamilyType`
//! and `SketchQuery` have no serde impls upstream, and the wire payload is
//! produced by `emit::backend_wire::build_backend_aggregation_json`, a
//! hand-written JSON builder reading these fields, never a whole-struct
//! serialize. `backend_plan::from_stage_config` reuses that same builder so
//! the two wire formats share one `PolicyFingerprint` identity space.

use planner_types::post_asap::{SketchQuery, SummaryFamilyType};
use serde::{Deserialize, Serialize};

/// Everything the backend must materialize and serve for one planning cycle.
#[derive(Debug, Clone)]
pub struct BackendStageConfig {
    /// One entry per materialization the backend maintains.
    pub aggregations: Vec<BackendAggregation>,
    /// One readout per summary-estimate node — what the backend returns to
    /// the query evaluator.
    pub readouts: Vec<BackendReadout>,
}

/// One summary the backend must accept and maintain.
///
/// `aggregation_id` is internal plumbing: it threads a selected summary to
/// its readout while compiling. It is not emitted on the wire — the backend
/// content-addresses identity via `PolicyFingerprint`, derived from
/// `metric_name`, the summary family, grouping labels and `spatial_filter`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackendAggregation {
    /// Internal-only id (see struct doc). Not on the wire.
    pub aggregation_id: String,
    /// Source metric the aggregation runs over. Required by the backend's
    /// `AggregationConfig` parser.
    pub metric_name: String,
    /// Planner-owned committed summary identity. Sketch entries carry a
    /// validated `SketchKind` (category + algorithm + params); exact entries
    /// carry the matching `ExactKind`/`ExactParams` pair.
    pub family: SummaryFamilyType,
    /// Window size in seconds. The backend's parser rejects a zero window.
    pub window_secs: u64,
    /// Spatial filter (comma-joined `k=v` pairs). Empty when none applies.
    pub spatial_filter: String,
    /// Group-by label names — keys in `labels.grouping` on the backend side,
    /// where the precompute engine keys per-aggregation state by the
    /// projected attribute set.
    pub grouping: Vec<String>,
    /// Per-item dimension (the data-point attribute name, e.g. `endpoint`)
    /// for an item_label-mode frequency sketch. Emitted into the
    /// aggregation's `parameters["item_label"]` so data-plane ingest records
    /// it on the sid and can answer per-item `estimate(key)`.
    pub item_label: Option<String>,
    /// Runtime accumulator mode derived from the summary's input weight,
    /// never from the TopK readout. `None` retains the value-update default.
    pub heap_update_mode: Option<&'static str>,
    /// What wire shape the backend ingests for this aggregation.
    pub aggregation_input: AggregationInput,
}

/// What wire shape the backend ingests for an aggregation: whether it builds
/// the summary from raw samples or accepts pre-built state from upstream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationInput {
    /// The backend receives summary-state envelopes.
    #[default]
    SketchEnvelope,
    /// The backend receives raw OTLP samples and builds the summary at
    /// ingest.
    Raw,
}

/// One readout entry — what the backend's query evaluator asks for.
///
/// Not `PartialEq`/`Serialize`/`Deserialize`: `op: SketchQuery` has none of
/// those upstream.
#[derive(Debug, Clone)]
pub struct BackendReadout {
    /// Aggregation this readout reads from.
    pub aggregation_id: String,
    /// Readout op (mirror of `SummaryExpr::SummaryEstimate::query`).
    pub op: SketchQuery,
}
