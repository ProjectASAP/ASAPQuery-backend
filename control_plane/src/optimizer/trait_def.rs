//! `OptimizerRule` trait + `RuleCategory` enum — the L4 rule-library
//! surface declared in `control_plane/docs/design.md` §6
//! `core::optimizer::trait`.
//!
//! ## Trait layering
//!
//! Two concrete rule families exist in this crate:
//!
//! * The legacy [`crate::optimizer::engine::RewriteRule`] trait drives the
//!   fixed-point [`crate::optimizer::engine::QueryOptimizer`] over the
//!   legacy [`crate::intent_algebra::legacy_expr::QueryExpr`] IR.
//! * The Phase-C bind-rule trait [`crate::sketch_algebra::rules::Rule`]
//!   lowers L3 → L4 [`crate::sketch_algebra::SketchExpr`] on the
//!   canonical [`crate::intent_algebra::QueryExpr`] IR.
//!
//! Both feed into the same "rule library" the engine driver consults, so
//! every rule must surface the same diagnostic + category metadata
//! regardless of which family it belongs to. The [`OptimizerRule`] trait
//! captures that *minimal common surface* — every concrete rule in this
//! crate implements it by virtue of the blanket impl on
//! [`crate::optimizer::engine::RewriteRule`] (in `engine.rs`) and the
//! `OptimizerRule` blanket impl on [`crate::sketch_algebra::rules::Rule`]
//! (in `optimizer/mod.rs`).
//!
//! Driver code that wants to enumerate "every rule" iterates over a
//! `&[Box<dyn OptimizerRule>]` and groups by [`OptimizerRule::category`].
//! It does NOT call rule-specific entry points (`try_rewrite` /
//! `apply`) — those signatures differ per family — but it does carry
//! the metadata uniformly for instrumentation, debugging, and the
//! future deployment-model rule-set selection table.

/// Category tag for a rule, so callers can enable / disable groups of
/// rules together.
///
/// Reserved for the future deployment-model registry surface (`crates/
/// deployment-model-*/src/rules.rs` per design.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleCategory {
    /// Push down predicates / projections.
    PushDown,
    /// Fuse adjacent operators into one.
    Fusion,
    /// Eliminate redundant operators.
    Elim,
    /// Bind a logical intent to a sketch family.
    Bind,
    /// CSE / let-binding extraction.
    Cse,
    /// Decorrelate subqueries.
    Decorrelate,
}

/// `OptimizerRule` trait — every L4 rule implements this so the engine
/// driver can iterate over them uniformly.
///
/// See module docs for the trait layering — concrete rules in this crate
/// implement [`OptimizerRule`] via blanket impls on the family-specific
/// trait, so call sites can pass a `Vec<Box<dyn OptimizerRule>>` to any
/// driver that just wants the rule metadata.
pub trait OptimizerRule: Send + Sync {
    /// Stable name for diagnostics + rule selection.
    fn name(&self) -> &'static str;

    /// Category tag — see [`RuleCategory`].
    fn category(&self) -> RuleCategory;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::engine::{
        CommonSubexprElim, FilterWindowSwap, HLLDedupElim,
        HydraConversion, MergeLifting, PartitionElim, PredicatePushDown, SetOpFusion,
        SubqueryDecorrelation, TopKFusion, WindowMerge,
    };

    /// Every concrete `RewriteRule` in the engine surfaces a stable name and
    /// a non-default category through the blanket [`OptimizerRule`] impl.
    #[test]
    fn engine_rules_implement_optimizer_rule_trait() {
        // The blanket impl on RewriteRule in `engine.rs` makes every
        // concrete engine rule a `dyn OptimizerRule` too. This list
        // mirrors `engine::default_rules()` — if a rule is added there
        // without a category entry the build (or this test) breaks.
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(PredicatePushDown),
            Box::new(FilterWindowSwap),
            Box::new(HLLDedupElim),
            Box::new(WindowMerge),
            Box::new(PartitionElim),
            Box::new(TopKFusion),
            // R6 (HistogramQuantileFusion) retired in Step γ5.
            Box::new(MergeLifting),
            Box::new(SetOpFusion),
            Box::new(HydraConversion),
            Box::new(SubqueryDecorrelation),
            Box::new(CommonSubexprElim),
        ];
        // Names are stable + non-empty.
        for r in &rules {
            assert!(!r.name().is_empty(), "rule names must be non-empty");
        }
        // Categories cover the expected groups (set, not list — order doesn't matter).
        use std::collections::HashSet;
        let cats: HashSet<RuleCategory> = rules.iter().map(|r| r.category()).collect();
        assert!(cats.contains(&RuleCategory::PushDown));
        assert!(cats.contains(&RuleCategory::Fusion));
        assert!(cats.contains(&RuleCategory::Elim));
        assert!(cats.contains(&RuleCategory::Cse));
        assert!(cats.contains(&RuleCategory::Decorrelate));
    }

    /// Every Phase-C bind rule in `sketch_algebra::rules` surfaces
    /// `RuleCategory::Bind` through its `OptimizerRule` blanket impl.
    #[test]
    fn sketch_algebra_bind_rules_carry_bind_category() {
        use crate::sketch_algebra::rules::{
            bind_archive_only::BindArchiveOnly,
            bind_cms_count::BindCmsOnCount,
            bind_cms_topk::BindCountSketchOnTopK,
            bind_ddsketch_quantile::BindDDSketchOnQuantile,
            bind_hll_cardinality::BindHllOnCardinality,
            bind_kll_quantile::BindKllOnQuantile,
        };
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(BindKllOnQuantile),
            Box::new(BindDDSketchOnQuantile),
            Box::new(BindCmsOnCount),
            Box::new(BindCountSketchOnTopK),
            Box::new(BindHllOnCardinality),
            Box::new(BindArchiveOnly),
        ];
        for r in &rules {
            assert_eq!(
                r.category(),
                RuleCategory::Bind,
                "rule {} should carry Bind category",
                r.name()
            );
        }
    }
}
