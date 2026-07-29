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
//!   that delegates to `asap_plan::bind::implement_tree_in_with` (the
//!   local `Bind*` rule family it used to fire was retired in Step B).
//!
//! Scope reduction (per orchestrator spec): the variant set ships the
//! subset DC + PromQL needs. `SketchJoin`, `SketchSubtract`,
//! `SketchDelete` from design.md §6 are intentionally *not* surfaced
//! yet — they're gated on rules that haven't landed. Adding them is
//! purely additive.
//!
//! Wire-up state. `bind_query_expr` runs unconditionally from `main.rs`
//! — there is no env-gate on this L3→L4 binding step itself. The one env
//! var in this area, `USE_TYPED_STAGE_SPLIT`
//! (`physical::stage_split::ENV_USE_TYPED_STAGE_SPLIT`), gates the
//! *downstream* L4→L5 stage-split step, not this module.

#![allow(dead_code, unused_imports)]

pub mod capability;
pub mod capability_matching;
pub mod cost_model;
pub mod lower;
pub mod matcher;
pub mod physical_expr;

#[cfg(test)]
mod tests;

// Re-exports — `crate::sketch_algebra::*` for downstream callers.
pub use capability::{capability_for, Capability, SketchKindHandle};
pub use capability_matching::{
    classify_demo_metric, is_valid_pair, pick_family, AccuracyPreference, StatisticClass,
};
pub use lower::{bind_query_expr, bind_query_expr_with_cost_model, BindingError};
pub use matcher::SummaryFamilyMatcher;
pub use physical_expr::{L4Plan, PhysicalExpr};
