// Layer 2 scaffolding ships ahead of any in-tree call site (the
// downstream `pipeline` driver lands in a later phase). Dead-code
// warnings are silenced here, not on individual items, so the public
// surface is uncluttered.
#![allow(dead_code, unused_imports)]

//! Layer 2 — `language_logical_plan`.
//!
//! Per `controller/docs/design.md` §6 `core::logical_plan`, L2 is the
//! per-language algebra tree that preserves language-specific
//! semantics (PromQL instant vs range vector, SQL window frames,
//! Elastic buckets) before the L3 normalisation pass collapses
//! everything into the language-orthogonal [`crate::algebra::expr::QueryExpr`].
//!
//! # DC deployment scope
//!
//! - PromQL → real L2 (carries the existing `QueryExpr` tree from
//!   `query_parser::parse_query_expr` plus a flat summary projection).
//! - Other languages → not yet implemented; lowering errors cleanly.
//!
//! # Layer split
//!
//! - L1 ([`crate::query_language`]) — `&str` → [`crate::query_language::LanguageAst`].
//! - L2 ([`Self`]) — [`crate::query_language::LanguageAst`] →
//!   [`LanguageLogicalPlan`].
//! - L3 (`crate::intent_algebra`, Phase B) — [`LanguageLogicalPlan`]
//!   → language-orthogonal [`crate::algebra::expr::QueryExpr`].

pub mod lower;
pub mod plan;

pub use lower::{lower_to_logical_plan, LoweringError};
pub use plan::{LanguageLogicalPlan, LanguageLogicalPlanSummary};

#[cfg(test)]
mod tests;
