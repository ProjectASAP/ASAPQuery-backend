//! Historically the **Layer-2 relational IR** — the per-language query
//! algebra the `query_parser` front ends emitted, before lowering to the
//! canonical L3 `intent_algebra::query_expr` types via
//! `intent_algebra::lower::convert_root`.
//!
//! ## L1 adoption superseded this (design-target-architecture.md Part B,
//! ASAPPlanner pin migration)
//!
//! `query_parser::parse_query_expr_canonical` has called
//! `asap_frontend_promql::lower_promql` directly since #428 — full L1
//! parse -> L2 relational -> L3 canonical conversion in one upstream
//! call, with no local L2 tree in the loop at all. This module's own
//! `QueryExpr`/`AggFunc`/`AggItem`/`L2ProjectItem`/`L2SortKey`/
//! `SourceSpec` (re-exported from the now-deleted `asap_l2` crate — see
//! control_plane/docs/design-asapplanner-pin-migration.md) and
//! `intent_algebra::lower::{convert, convert_root}` were already dead in
//! production before the ASAPPlanner pin bump — grep-confirmed: every
//! real call site is `#[cfg(test)]`-only (`lower.rs`'s own tests,
//! `physical::window_fusion`/`planner`/`allocator`'s test fixtures).
//! `asap_l2` no longer exists upstream to re-export from regardless
//! (ASAPPlanner#213, "delete crates/l2 entirely -- both front ends emit
//! canonical QueryExpr directly"), so rather than vendor a ~20-variant L2
//! tree type solely to keep already-dead test scaffolding compiling,
//! `lower.rs` and this module's `asap_l2`-sourced re-export were deleted
//! (matching upstream's own precedent for exactly this situation --
//! ASAPPlanner#181/#192/#197 all deleted comparably dead scaffolding
//! rather than migrate it). The handful of dependent test functions in
//! `physical::window_fusion`/`planner`/`allocator` were removed with it;
//! see those files' own comments at the removal sites.
//!
//! What's left here — `AggIntent` forwarding and `PerPartitionWrap` — has
//! real, live consumers (`physical::sketch_catalog`) and doesn't touch
//! the deleted L2 tree at all.

pub use crate::intent_algebra::expr_ir::L2Expr;
pub use planner_types::pre_asap::expr_ir::ColumnRef;
pub use planner_types::pre_asap::query_expr::{
    BinaryOpKind, GroupSide, JoinKind, SampleKind, SetOpKind, VectorGrouping, VectorMatch,
    VectorMatchKind, WindowFuncKind,
};

/// Canonical L3 aggregation intent. Re-exported here so existing
/// `relational::AggIntent` references keep working — the type is the
/// single canonical [`crate::intent_algebra::agg_intent::AggIntent`].
pub use crate::intent_algebra::agg_intent::AggIntent;

pub use crate::intent_algebra::agg_intent::{
    agg_accuracy, agg_is_exact, agg_is_mergeable, default_cardinality, default_frequency,
    default_quantile,
};

/// Per-partition wrapper that historically lived on the legacy `AggIntent`
/// enum as a `PerPartition { inner, keys }` variant. Canonical L3
/// represents this shape via `QueryExpr::Aggregate { by: keys, measures:
/// [inner] }`; this wrapper survives only as the input type for the
/// `physical::sketch_catalog` per-partition sizing helpers. No upstream
/// equivalent — control_plane-only.
#[derive(Debug, Clone, PartialEq)]
pub struct PerPartitionWrap {
    pub inner: AggIntent,
    pub keys: Vec<String>,
}

/// A single filter predicate pushed down to the collector — used by
/// `query_parser::promql::apply_qe_filters` to build an `L2Expr` tree
/// from a flat label-matcher list. Control_plane-only: no upstream
/// equivalent (its front end builds `L2Expr` directly, never through an
/// intermediate flat predicate list). No live constructor today (see
/// module doc) — kept as a self-contained, dependency-free type.
#[derive(Debug, Clone)]
pub struct Predicate {
    pub col: String,
    pub op: FilterOp,
    pub val: FilterVal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
    /// PromQL `=~` label matcher (RE2 syntax).
    Regex(String),
    /// PromQL `!~` label matcher.
    NotRegex(String),
}

#[derive(Debug, Clone)]
pub enum FilterVal {
    Str(String),
    Num(f64),
    Int(i64),
    Null,
}
