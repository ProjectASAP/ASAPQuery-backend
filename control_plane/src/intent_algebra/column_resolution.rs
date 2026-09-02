//! Schema-driven column resolution for the Layer-2 `relational` IR.
//!
//! ## Phase 2 step 4 (docs/migration-plan-backend-plan.md)
//!
//! `ResolveError`, `infer_source_schema`, `infer_schema_for_root`,
//! `resolve_column_ref`, `resolve_column_refs`, `resolve_group_keys_promql`,
//! `resolve_expr`, `output_schema_for_aggregate` are no longer defined in
//! this repo — re-exported from `asap_l2::column_resolution`.
//!
//! Two capabilities this repo's pre-merge version didn't have:
//!
//! - [`resolve_expr`] resolves a whole `UnresolvedQueryExpr` tree (name-based) into an
//!   `QueryExpr` (positional) in one pass — this repo's `ScalarExpr` is gone
//!   (see `relational.rs`'s module doc), so `lower.rs`'s `convert_scalar`
//!   now calls this directly instead of hand-matching every `Expr`
//!   variant itself.
//! - [`resolve_group_keys_promql`] encodes PromQL's absent-label grouping
//!   semantics (issue #53): a GROUP BY key not present in a **closed**
//!   schema is provably absent from every row, so it's dropped rather
//!   than rejected. `resolve_named_keys`, this repo's old strict-only
//!   equivalent, is gone — `lower.rs` calls this instead for `Aggregate`
//!   / `Partition` keys. Every schema `lower.rs` builds today is open
//!   (`Schema::closed == false`, per `Binder`'s PromQL-only usage-derived
//!   catalog), so this degrades to the old strict behavior in practice —
//!   ready for when a closed (SQL) schema starts flowing through.
// `infer_schema_for_root`/`infer_source_schema` no longer exist upstream
// (ASAPPlanner's crate consolidation, ASAPPlanner#198) and had zero real
// callers in this repo beyond this re-export -- dropped rather than
// vendored. See control_plane/docs/design-asapplanner-pin-migration.md.
pub use planner_types::pre_asap::column_resolution::{
    output_schema_for_aggregate, resolve_column_ref, resolve_column_refs, resolve_expr,
    resolve_group_keys_promql, ResolveError,
};
