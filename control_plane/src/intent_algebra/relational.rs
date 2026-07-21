//! The **Layer-2 relational IR** — the per-language query algebra the
//! `query_parser` front ends emit, before lowering to the canonical L3
//! `intent_algebra::query_expr` types.
//!
//! ## Phase 2 step 4 (docs/migration-plan-backend-plan.md)
//!
//! `QueryExpr`, `AggFunc`, `AggItem`, `SourceSpec`, `L2ProjectItem`,
//! `L2SortKey` are no longer defined in this repo — re-exported from
//! `asap_l2::relational` (the `asap-l2` crate, same git dependency /
//! pinned commit as `asap-ir`). `asap_l2`'s L2 is a real superset: every
//! node has a positional-name-based (`L2Expr` / `ColumnRef`) twin of its
//! canonical L3 counterpart (`Scalar`, `EvalTime`, `VectorFromScalar`,
//! `ScalarFromVector`, `Relabel`, `Sample`, `InfoJoin`, `WindowFunc` are
//! new), and `ScalarExpr` (this repo's own 8-variant scalar enum) is gone
//! — every scalar position (`Filter.pred`, `Aggregate.having`,
//! `Join.pred`, `Project` items) now carries the shared `L2Expr`
//! (`expr_ir::Expr<ColumnRef>`, merged in Phase 2 step 2) directly, same
//! as the canonical L3 tree carries `L3Expr`.
//!
//! `Partition` doesn't exist at L2 either — `Aggregate` carries
//! `without: bool` directly (PromQL `without(...)` vs `by(...)`), matching
//! how L3's `GroupKeys` already modeled this after the `query_expr.rs`
//! merge (step 3). `PartitionKeys` is gone as a result.
//!
//! `AggFunc::Frequency` and `AggFunc::Custom(String)` — this repo's own
//! extensions, with no `asap_l2::relational::AggFunc` equivalent — are
//! **not** ported forward as enum variants (`AggFunc` is a foreign type;
//! Rust's orphan rules forbid adding variants to it from this crate).
//! Per the tie-break rule (adopt ASAPController's version; genuinely
//! control_plane-only functionality gets ported onto it, not used to
//! keep this repo's copy), both are preserved as *behavior* in
//! `lower.rs` instead of as *vocabulary*:
//!
//! - **`Frequency`** (control_plane's per-series sample-count
//!   estimation, routing to CMS/CountSketch): `query_parser::promql` now
//!   constructs `AggFunc::Count` for `count_over_time(...)` (matching
//!   `asap_l2`'s own frontend-promql, which also uses bare `Count` and
//!   leaves the sketch-vs-exact choice to L4's `Bind*` rules for the
//!   `TopK`-over-`Count` heavy-hitter shape). `lower.rs`'s
//!   `agg_func_to_intents` recovers control_plane's original "windowed
//!   Count → Frequency sketch, non-windowed Count → exact" split — the
//!   `_over_time` window wrapper is exactly the same discriminator
//!   `promql.rs`'s own comment documented for the pre-merge
//!   `AggFunc::Frequency` construction, so this is a vocabulary
//!   substitution, not a behavior change. See `lower.rs`'s module doc for
//!   the "grouped OR windowed" precise trigger.
//! - **`Custom(String)`** (arbitrary named aggregate / UDA extension
//!   point): zero real construction sites in this repo (grep-verified —
//!   `AggFunc::Custom` appeared only in `lower.rs`'s own dead-code
//!   handling and one unit test). Dropped outright rather than ported;
//!   `lower.rs`'s `agg_func_to_intents` still returns
//!   `ConvertError::NoCanonicalIntent` for any `AggFunc` variant it can't
//!   map (structurally unreachable today since every `asap_l2::AggFunc`
//!   variant maps to something), preserving the escape hatch's shape for
//!   when a real extension need arises.

pub use asap_ir::intent_algebra::expr_ir::{ColumnRef, L2Expr};
pub use asap_ir::intent_algebra::query_expr::{
    BinaryOpKind, GroupSide, JoinKind, SampleKind, SetOpKind, VectorGrouping, VectorMatch,
    VectorMatchKind, WindowFuncKind,
};
pub use asap_l2::relational::{AggFunc, AggItem, L2ProjectItem, L2SortKey, QueryExpr, SourceSpec};

/// Canonical L3 aggregation intent. Re-exported here so existing
/// `relational::AggIntent` references keep working — the type is the
/// single canonical [`crate::intent_algebra::agg_intent::AggIntent`].
pub use crate::intent_algebra::agg_intent::AggIntent;

pub use crate::intent_algebra::agg_intent::{
    agg_accuracy, agg_is_exact, agg_is_mergeable, default_cardinality, default_frequency,
    default_quantile,
};

pub use crate::sketch_algebra::capability::{countmin_accuracy, hll_accuracy};

/// Per-partition wrapper that historically lived on the legacy `AggIntent`
/// enum as a `PerPartition { inner, keys }` variant. Canonical L3
/// represents this shape via `QueryExpr::Aggregate { by: keys, aggs:
/// [inner] }`; this wrapper survives only as the input type for the
/// `physical::sketch_catalog` per-partition sizing helpers. No
/// `asap_l2` equivalent — control_plane-only.
#[derive(Debug, Clone, PartialEq)]
pub struct PerPartitionWrap {
    pub inner: AggIntent,
    pub keys: Vec<String>,
}

/// A single filter predicate pushed down to the collector — used by
/// `query_parser::promql::apply_qe_filters` to build an `L2Expr` tree
/// from a flat label-matcher list. Control_plane-only: no `asap_l2`
/// equivalent (its front end builds `L2Expr` directly, never through an
/// intermediate flat predicate list).
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
