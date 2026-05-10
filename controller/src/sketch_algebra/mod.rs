//! Layer 4 IR — `core::sketch_algebra` per `controller/docs/design.md` §6.
//!
//! Phase C introduces the L4 vocabulary the planner pivots on:
//!
//! - [`SketchExpr`] — the L4 algebra DAG (sketch-bound, language-orthogonal,
//!   deployment-independent). Single-rooted per query; multi-root
//!   workload-level fan-in lives one layer up in
//!   `types_v2::WorkloadPlan`.
//! - [`SketchKind`] / [`SketchParams`] — typed sketch-family selector +
//!   parameter payload. Convertible to the legacy `crate::types`
//!   shape via [`SketchParams::to_legacy`] for the L5 emitter side.
//! - [`SketchStateSchema`] — the L4 type-system primitive that mirrors
//!   the §6.4 input/output spec table (`Sketch(SketchKind, SketchParams)`
//!   field type + catalog-capability flags).
//! - [`bind_query_expr`] — the L3→L4 lowering driver: bottom-up walk
//!   that fires `Bind*` rules.
//! - [`rules`] — the `Bind*` rule family. Each rule pattern-matches on a
//!   `QueryExpr::Aggregate` shape, reads the `AccuracyTarget`, and emits
//!   a typed `SketchExpr` with the sketch family + parameters committed.
//!
//! Scope reduction (per orchestrator spec): the variant set ships the
//! subset DC + PromQL needs. `SketchJoin`, `SketchSubtract`,
//! `SketchDelete` from design.md §6 are intentionally *not* surfaced
//! yet — they're gated on rules that haven't landed. Adding them is
//! purely additive.
//!
//! Wire-up state. The typed path is opt-in via the
//! `USE_TYPED_SKETCH_ALGEBRA` env var consulted by `planner::rules`;
//! existing call sites continue to use the legacy untyped binding path.
//! Phase E (stage_split) is the natural migration point.

#![allow(dead_code, unused_imports)]

pub mod capability_matching;
pub mod lower;
pub mod params;
pub mod rules;
pub mod schema;
pub mod sketch_expr;

#[cfg(test)]
mod tests;

// Re-exports — `crate::sketch_algebra::*` for downstream callers.
pub use capability_matching::{
    classify_demo_metric, is_valid_pair, pick_family, AccuracyPreference, StatisticClass,
};
pub use lower::{bind_query_expr, BindingError};
pub use params::{
    CmsParams, CountSketchParams, DDSketchParams, HllParams, KllParams, SketchKind, SketchParams,
};
pub use schema::{SketchCapabilities, SketchStateSchema};
pub use sketch_expr::{EstimateOp, MergeAlgebra, SketchExpr};
