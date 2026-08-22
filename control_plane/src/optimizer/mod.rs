//! L4 — **sketch-binding optimizer** framework.
//!
//! Per `control_plane/docs/design.md` §3/§5/§6 `core::optimizer`: this is
//! the rule engine driver + cost-model trait + rule library that takes
//! L3 [`crate::intent_algebra::relational::QueryExpr`] / canonical
//! [`crate::intent_algebra::QueryExpr`] inputs and produces L4
//! sketch-bound output (in the legacy path: an annotated `QueryExpr`
//! with `SketchAgg` nodes; in the canonical path: a
//! [`crate::sketch_algebra::PhysicalExpr`] DAG).
//!
//! Refactor 2026-05 (`refactor/controller-layered-cleanup`) absorbed
//! the former `controller/src/algebra/optimizer.rs` and the entire
//! former `controller/src/planner/` directory here. Mapping:
//!
//! | Old path | New path |
//! |---|---|
//! | `algebra/optimizer.rs` | [`engine`] (rule driver + `QueryOptimizer` + `DeploymentConstraints`) |
//! | `planner/cost_model.rs` | [`cost`] (cost model trait + per-strategy impls) |
//! | `planner/delta_cost_model.rs` | [`cost::delta`] |
//! | `planner/online_cost_model.rs` | [`cost::online`] |
//! | `planner/pareto.rs` | [`cost::pareto`] |
//! | `planner/tco.rs` | [`cost::tco`] |
//! | `planner/wire_cost.rs` | [`cost::wire`] |
//! | `planner/rules.rs` | [`rules`] (shared rule library) |
//! | `planner/baseline_planner.rs` | [`baseline`] |

pub mod baseline;
pub mod cost;
pub mod engine;
pub mod rules;

// `cse` (workload-level CSE via `QueryExpr::Ref`/`LetBinding`) was deleted
// as part of the ASAPPlanner pin migration (see
// control_plane/docs/design-asapplanner-pin-migration.md): ASAPPlanner
// deleted the identical `Ref`/`LetBinding` scaffolding upstream
// (ASAPPlanner#181/#192) on the same grounds this file's own module doc
// already stated -- "no in-tree consumer today" -- and the canonical
// `QueryExpr` this crate depends on no longer has room for those variants
// at all (Rust doesn't allow adding variants to a foreign enum). Nothing
// in the live query path called `dedupe_subtrees`; re-introducing
// workload-level CSE needs a representation that doesn't ride on
// `QueryExpr::Ref`/`LetBinding`, tracked as a follow-up rather than
// invented here.

// Re-exports — preserve the surface that `crate::algebra::QueryOptimizer`
// and `crate::planner::*` consumers historically relied on.
pub use engine::{DeploymentConstraints, QueryOptimizer};
