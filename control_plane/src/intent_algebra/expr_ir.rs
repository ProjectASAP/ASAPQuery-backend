//! Language-independent scalar expression IR.
//!
//! ## Phase 2 step 2 (docs/migration-plan-backend-plan.md)
//!
//! Originally re-exported from ASAPController's `asap-ir` crate as a
//! standalone generic `Expr<C>` type (`L2Expr = Expr<ColumnRef>`, `L3Expr
//! = Expr<ColumnId>`), separate from the relational `QueryExpr` tree.
//! ASAPPlanner has since folded the scalar-expression shapes directly
//! into `QueryExpr` itself (ASAPPlanner#205, landed via #214) — one
//! recursive `QueryExpr<C>` now carries both relational operators
//! (`Scan`/`Filter`/`Aggregate`/…) and scalar expression nodes
//! (`Column`/`Literal`/`Compare`/`BoolAnd`/`Not`/…). There is no
//! standalone `Expr`/`L2Expr`/`L3Expr` type anymore — see
//! `control_plane/docs/design-asapplanner-pin-migration.md`.
//!
//! What this repo called `L3Expr` (the canonical, positional scalar tree)
//! is therefore just `QueryExpr` itself (`QueryExpr<C: ColState =
//! ColumnId>` defaults to the positional form, matching how `L3Expr` was
//! always used here). What it called `L2Expr` (the front-end-emitted,
//! name-based tree) is `planner_types::pre_asap::UnresolvedQueryExpr`
//! (`= QueryExpr<ColumnRef>`). `L3Scalar` is renamed `ScalarValue`
//! upstream (ASAPPlanner#215), same five cases (`Int64`/`Float64`/`Utf8`/
//! `Boolean`/`Null`). `Predicate<C>` also changed shape alongside this —
//! it now wraps `Box<QueryExpr<C>>` (was a bare, unboxed inner value).
//!
//! Type aliases below preserve this repo's own `L2Expr`/`L3Expr`/
//! `L3Scalar` names as the call-site-facing surface (~135 combined call
//! sites), so this is the only file that needs to know the upstream
//! names changed.

pub use planner_types::pre_asap::{
    ArithmeticOpKind as ArithOp, ColumnRef, CompareOpKind as CompareOp, ScalarValue as L3Scalar,
};
use planner_types::pre_asap::{QueryExpr, UnresolvedQueryExpr};

/// The front-end-emitted, name-based scalar/relational tree — was a
/// standalone `Expr<ColumnRef>`, now `QueryExpr<ColumnRef>` itself (see
/// module doc).
pub type L2Expr = UnresolvedQueryExpr;

/// The canonical, positional scalar/relational tree — was a standalone
/// `Expr<ColumnId>`, now plain `QueryExpr` (defaults its generic param to
/// `ColumnId`) itself (see module doc).
pub type L3Expr = QueryExpr;
