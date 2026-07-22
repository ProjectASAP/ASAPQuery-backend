//! `Rule` — the `OptimizerRule`-blanket-impl extension point
//! `optimizer::mod`'s `RuleCategory::Bind` coverage was originally built
//! around.
//!
//! Step B of the plan-shaped-serving migration retired the seven
//! `Bind*`-rule-struct implementors (`bind_kll_quantile` /
//! `bind_ddsketch_quantile` / `bind_hll_cardinality` / `bind_cms_count` /
//! `bind_cms_topk` / `bind_exact_agg` / `bind_archive_only`) in favor of
//! `asap_plan::bind::implement_tree_in_with` +
//! `crate::sketch_algebra::cost_model::ControlPlaneCostModel` — see
//! `sketch_algebra::lower`. The trait itself stays: `optimizer::mod`'s
//! blanket `impl<R: Rule> OptimizerRule for R` still needs it to exist,
//! and it remains a reasonable extension point for any future
//! deployment-specific L3→L4 rule that doesn't fit the `CostModel`
//! `rank_candidates`/`size_params` shape (e.g. a rule that rewrites
//! *through* a logical parent, which `implement_tree_in_with` deliberately
//! doesn't attempt — see its module docs' "conservative fallbacks").

#![allow(dead_code)]

use crate::intent_algebra::QueryExpr;
use crate::sketch_algebra::physical_expr::PhysicalExpr;
use crate::types_v2::AccuracyTarget;

/// Bind-rule trait. See module docs.
pub trait Rule: Sync {
    /// Stable, human-readable identifier — used for diagnostics + the
    /// `cargo test` matrix.
    fn name(&self) -> &'static str;

    /// Lower the matched sub-tree under the given accuracy target. Return
    /// `None` if this rule does not apply to the supplied `expr`.
    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr>;

    /// Coarse priority used for tie-break when multiple rules match.
    /// Higher = preferred. Default 0.
    fn priority(&self) -> u16 {
        0
    }
}
