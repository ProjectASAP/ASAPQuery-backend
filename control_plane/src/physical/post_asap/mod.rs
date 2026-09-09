//! Physical adapters for ASAPPlanner's canonical post-ASAP IR.
//!
//! ASAPPlanner owns summary selection and the post-ASAP `SummaryNode` tree.
//! This module adds only backend-specific concerns required to compile that
//! tree into catalog-backed executable plans: deployment cost input,
//! named sharing/placement wrappers, and runtime family matching. It belongs
//! under `physical`; it is not another logical or sketch-algebra layer.

#![allow(dead_code, unused_imports)]

pub mod cost_model;
pub mod deployment_expr;
pub mod lower;
pub mod matcher;

#[cfg(test)]
mod tests;

// Re-exports — `crate::physical::post_asap::*` for downstream callers.
pub use deployment_expr::{PhysicalExpr, PostAsapPlan};
pub use lower::{bind_query_expr, bind_query_expr_with_cost_model, BindingError};
pub use matcher::SummaryFamilyMatcher;
