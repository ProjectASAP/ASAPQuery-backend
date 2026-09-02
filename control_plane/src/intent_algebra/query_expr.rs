//! Layer 3 IR — `QueryExpr` DAG (intent-only, language-orthogonal,
//! deployment-independent).
//!
//! ## Phase 2 step 3 (docs/migration-plan-backend-plan.md)
//!
//! `QueryExpr` and its supporting types are no longer defined in this
//! repo — re-exported from `planner_types::pre_asap::intent_algebra`. `asap_ir`'s version
//! is a real superset (~22 variants vs. this repo's pre-merge 16:
//! `EvalTime`, `VectorFromScalar`/`ScalarFromVector`, `Relabel`,
//! `InfoJoin`, `Sample`, `TimeRange`, `TimeShift`, `WindowFunc` are new,
//! each backing real PromQL surface from issues #40-#118), and its
//! `output_schema_in` is a complete, already-tested implementation for
//! every variant — adopted via the inherent method, not reimplemented.
//!
//! Three representational differences turned out, on inspection, not to
//! be missing capability — `asap_ir` represents the same things more
//! consolidated, just under different names:
//!
//! - **`Predicate`** is now `L3Expr` (`expr_ir.rs`, landed previously) —
//!   `struct Predicate(pub L3Expr)`, not this repo's own 8-variant enum.
//!   Six of the eight variants map directly. `Between` desugars to
//!   `Expr::BoolAnd([Compare(Ge), Compare(Le)])` (verified: one real
//!   construction site, nothing downstream pattern-matches on the shape).
//!   `ScalarSubquery(Box<QueryExpr>)` has no `Expr<C>` slot — it's
//!   restructured to a `QueryExpr`-level `LetBinding` wrap instead of an
//!   embedded predicate node, in `lower.rs`'s `convert_scalar` (see that
//!   file). This is exactly the shape `optimizer/engine.rs`'s R7
//!   `SubqueryDecorrelation` rule already produces as a *post-hoc*
//!   rewrite — doing it at construction time makes R7 dead code, deleted
//!   there.
//! - **`HavingPredicate`** (this repo's opaque `String` wrapper) is gone;
//!   `Aggregate.having` is `Option<Predicate>` — real typed HAVING
//!   instead of a formatted debug string. Its one real construction site
//!   (`lower.rs`) was already just `format!(...)`-ing a placeholder, not
//!   real HAVING evaluation.
//! - **`Partition { keys, child }`** doesn't exist in `asap_ir` — folded
//!   into `Aggregate.by: GroupKeys`, which already carries `by(...)` vs.
//!   `without(...)` directly (`{keys: Vec<ColumnId>, without: bool}`).
//!   Verified this repo's own `promql.rs` already builds `Aggregate.by`
//!   directly for the `count` case, skipping `Partition` entirely — the
//!   `sum`/`avg`/etc. path was the inconsistent one. Verified downstream:
//!   `physical/allocator.rs` labels `Partition` `ExecutionMode::Passthrough`
//!   with rationale `"Partition for GROUP BY at Agent"`, and
//!   `physical/planner.rs` converts it straight into
//!   `PhysicalOp::HashAggregate { keys }` — nothing stage/sharding-specific
//!   despite the old doc comment's framing. `promql.rs`'s
//!   `apply_qe_partition` is deleted; grouping keys attach to the nearest
//!   `Aggregate.by` directly instead.
//!
//! `LiteralValue` (this repo's narrow 5-variant literal enum) is replaced
//! by `L3Scalar` (same five cases, `expr_ir.rs`). `ColumnRef` here
//! (this repo's `Named`/`SampleValue`/`Wildcard`) is gone — L3 is fully
//! positional (`ColumnId`) in `asap_ir`; the name-based form only exists
//! at L2 now (`expr_ir::ColumnRef`, used by `relational.rs`, not yet
//! merged). `LabelFilter` stays — PromQL-ergonomic sugar for building a
//! `Scan`'s `predicates: Vec<Predicate>`, converted via
//! `label_filter_to_predicate` below (`asap_ir`'s `Scan` takes typed
//! `Predicate`s, not a separate label-filter list).

use std::rc::Rc;

use planner_types::pre_asap::schema::ColumnId;
pub use planner_types::pre_asap::{
    aggregate_output_schema, AtModifier, BinaryOpKind, DataModel, GroupKeys, GroupSide,
    InfoMatcher, JoinKind, Predicate, ProjectItem, QueryExpr, QueryExprError, Reduction,
    SampleKind, SetOpKind, SortKey, Source, TimeShift, VectorGrouping, VectorMatch,
    VectorMatchKind, WindowFuncKind,
};
pub use planner_types::pre_asap::{
    ArithmeticOpKind as ArithOp, ColumnRef, CompareOpKind as CompareOp,
};
// `L3Scalar`/`L3Expr`/`L2Expr` and `WindowKind`: see `expr_ir.rs`'s and
// `crates/asap_types/src/enums.rs`'s module docs respectively --
// ASAPPlanner deleted its `WindowKind` (no `QueryExpr::Window` producer
// left to need it) and folded the old standalone `Expr<C>` into
// `QueryExpr` itself, renaming its scalar-literal type `ScalarValue`.
pub use crate::intent_algebra::expr_ir::{L2Expr, L3Expr, L3Scalar};
pub use asap_types::enums::WindowKind;

use crate::intent_algebra::schema::Schema;

/// Equality label filter on a `Scan`. PromQL `{service="api"}` — kept as
/// ergonomic sugar for the parser; converted to a typed `Predicate` via
/// [`label_filter_to_predicate`] when building the `Scan` node itself.
/// Richer match operators (`!=`, `=~`, `!~`) go through
/// [`CompareOp::Regex`]/[`CompareOp::NotRegex`] the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelFilter {
    pub label: String,
    pub equals: String,
}

/// Convert a name-based `LabelFilter` into a positional `Predicate`
/// against `schema`. `None` if `label` isn't in `schema` (the caller's
/// binder pass is expected to have already added every referenced label
/// to the schema; this is a defensive fallback, not the primary path).
pub fn label_filter_to_predicate(lf: &LabelFilter, schema: &Schema) -> Option<Predicate> {
    let id = schema.column_id(&lf.label)?;
    Some(Predicate(Rc::new(L3Expr::Compare {
        left: Rc::new(L3Expr::Column(id)),
        op: CompareOp::Eq,
        right: Rc::new(L3Expr::Literal(L3Scalar::Utf8(lf.equals.clone()))),
    })))
}

/// Conjoin `predicates` into a single `Predicate` (`BoolAnd`), or `None`
/// if the list is empty. `asap_ir`'s `Scan.predicates` is a `Vec`, not a
/// single tree, so most callers won't need this — provided for the few
/// call sites that want one combined predicate (e.g. `Filter.pred`).
pub fn conjoin(predicates: Vec<Predicate>) -> Option<Predicate> {
    let mut exprs: Vec<L3Expr> = predicates.into_iter().map(|p| (*p.0).clone()).collect();
    match exprs.len() {
        0 => None,
        1 => Some(Predicate(Rc::new(exprs.remove(0)))),
        _ => Some(Predicate(Rc::new(L3Expr::BoolAnd(exprs)))),
    }
}

/// `expr BETWEEN low AND high` (`NOT BETWEEN` when `negated`), desugared
/// to `Compare(expr >= low) AND Compare(expr <= high)` (De Morgan's for
/// the negated form). No `Expr<C>` variant models `BETWEEN` directly —
/// this is the one real construction site's replacement (`lower.rs`).
pub fn between(expr: L3Expr, low: L3Expr, high: L3Expr, negated: bool) -> L3Expr {
    let ge = L3Expr::Compare {
        left: Rc::new(expr.clone()),
        op: CompareOp::Ge,
        right: Rc::new(low),
    };
    let le = L3Expr::Compare {
        left: Rc::new(expr),
        op: CompareOp::Le,
        right: Rc::new(high),
    };
    if negated {
        L3Expr::Not(Rc::new(L3Expr::BoolAnd(vec![ge, le])))
    } else {
        L3Expr::BoolAnd(vec![ge, le])
    }
}
