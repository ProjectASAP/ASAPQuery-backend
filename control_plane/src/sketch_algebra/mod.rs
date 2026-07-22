//! Layer 4 IR — `core::sketch_algebra` per `control_plane/docs/design.md` §6.
//!
//! Phase C introduces the L4 vocabulary the planner pivots on:
//!
//! - [`PhysicalExpr`] — the L4 algebra DAG (sketch-bound, language-orthogonal,
//!   deployment-independent). Single-rooted per query; multi-root
//!   workload-level fan-in lives one layer up in
//!   `types_v2::WorkloadPlan`.
//! - `asap_sketch::SummaryKind` / `SummaryParams` — typed sketch-family
//!   selector + parameter payload (moved out of this crate — formerly
//!   `sketch_params::SketchKind`/`SketchParams` — Stage 3 of the
//!   sketch-identity unification; see
//!   `scratchpad/artifacts/enum-unification-plan.md`).
//! - [`bind_query_expr`] — the L3→L4 lowering driver: bottom-up walk
//!   that fires `Bind*` rules.
//! - [`rules`] — the `Bind*` rule family. Each rule pattern-matches on a
//!   `QueryExpr::Aggregate` shape, reads the `AccuracyTarget`, and emits
//!   a typed `PhysicalExpr` with the sketch family + parameters committed.
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

pub mod capability;
pub mod capability_matching;
pub mod cost_model;
pub mod lower;
pub mod matcher;
pub mod physical_expr;
pub mod rules;

#[cfg(test)]
mod tests;

// Re-exports — `crate::sketch_algebra::*` for downstream callers.
pub use capability::{capability_for, Capability, SketchKindHandle};
pub use capability_matching::{
    classify_demo_metric, is_valid_pair, pick_family, AccuracyPreference, StatisticClass,
};
pub use lower::{bind_query_expr, BindingError};
pub use matcher::SummaryFamilyMatcher;
pub use physical_expr::{L4Plan, PhysicalExpr};
