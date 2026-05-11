//! `OptimizerRule` trait + `RuleCategory` enum — placeholder scaffolding
//! for the L4 rule-library surface declared in
//! `controller/docs/design.md` §6 `core::optimizer::trait`.
//!
//! The 2026-05 layered-cleanup refactor created this module so the
//! design.md target layout is materialised in code, but the existing
//! optimizer (`crate::optimizer::engine`) and rule library
//! (`crate::optimizer::rules`) still consume their own concrete types
//! defined in those files. Migrating callers onto this trait is a
//! follow-up — until then this module is intentionally minimal so it
//! has zero behavior impact.

#![allow(dead_code)]

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
}

/// `OptimizerRule` trait — every L4 rule implements this so the engine
/// driver can iterate over them uniformly.
///
/// **Status:** placeholder shape only. The existing
/// [`crate::sketch_algebra::rules::Rule`] trait covers the L4 binding
/// dispatch on the `intent_algebra` canonical path; the legacy
/// `algebra::optimizer` rule loop runs against `QueryExpr` directly
/// without a polymorphic trait. Unifying both paths under this trait is
/// a follow-up.
pub trait OptimizerRule: Send + Sync {
    /// Stable name for diagnostics + rule selection.
    fn name(&self) -> &'static str;

    /// Category tag — see [`RuleCategory`].
    fn category(&self) -> RuleCategory;
}
