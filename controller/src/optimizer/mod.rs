//! L4 — **sketch-binding optimizer** framework.
//!
//! Per `controller/docs/design.md` §3/§5/§6 `core::optimizer`: this is
//! the rule engine driver + cost-model trait + rule library that takes
//! L3 [`crate::intent_algebra::legacy_expr::QueryExpr`] / canonical
//! [`crate::intent_algebra::QueryExpr`] inputs and produces L4
//! sketch-bound output (in the legacy path: an annotated `QueryExpr`
//! with `SketchAgg` nodes; in the canonical path: a
//! [`crate::sketch_algebra::SketchExpr`] DAG).
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

pub mod engine;
pub mod cost;
pub mod rules;
pub mod baseline;
pub mod trait_def;

// Re-exports — preserve the surface that `crate::algebra::QueryOptimizer`
// and `crate::planner::*` consumers historically relied on.
pub use engine::{DeploymentConstraints, QueryOptimizer};
pub use trait_def::{OptimizerRule, RuleCategory};
