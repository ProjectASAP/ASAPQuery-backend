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

pub mod baseline;
pub mod cost;
pub mod engine;
pub mod rules;
pub mod trait_def;

// Re-exports — preserve the surface that `crate::algebra::QueryOptimizer`
// and `crate::planner::*` consumers historically relied on.
pub use engine::{DeploymentConstraints, QueryOptimizer};
pub use trait_def::{OptimizerRule, RuleCategory};

// ── OptimizerRule blanket impl for sketch_algebra::rules::Rule ────────────────
//
// Every Phase-C bind rule that implements
// [`crate::sketch_algebra::rules::Rule`] is also an
// [`OptimizerRule`] — its category is always `Bind` because that's
// exactly what `sketch_algebra::rules` is for (L3 → L4 sketch
// commitment). The blanket impl lives here (not in `sketch_algebra/`)
// so the impl boundary is owned by the optimizer crate-section, not by
// the sketch_algebra rule library.
impl<R> OptimizerRule for R
where
    R: crate::sketch_algebra::rules::Rule + Send + Sync + 'static,
{
    fn name(&self) -> &'static str {
        <Self as crate::sketch_algebra::rules::Rule>::name(self)
    }
    fn category(&self) -> RuleCategory {
        RuleCategory::Bind
    }
}
