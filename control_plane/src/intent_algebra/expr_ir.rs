//! Language-independent scalar expression IR.
//!
//! ## Phase 2 step 2 (docs/migration-plan-backend-plan.md)
//!
//! New file, re-exported from ASAPController's `asap-ir` crate. This is
//! the D2 decision from `ASAPController/docs/intent-algebra-reconciliation.md`
//! (already validated by ASAPController's own evolution — control_plane
//! had no equivalent generic-over-column-type scalar IR before this):
//! `Predicate` and `HavingPredicate` in `query_expr.rs` are currently ad
//! hoc, control_plane-only types; `relational.rs`'s `ScalarExpr` is a
//! separate, L2-only scalar type. Once `query_expr.rs`/`relational.rs`
//! are merged (next Phase 2 step), both collapse onto this one
//! `Expr<C>` — `L2Expr = Expr<ColumnRef>` for the front-end-emitted L2
//! tree, `L3Expr = Expr<ColumnId>` for the canonical positional L3 tree.
//! Nothing in this repo constructs `Expr<C>` yet — that lands with the
//! `query_expr.rs`/`relational.rs` merge, not here. This step only makes
//! the type available.
//!
//! One generic [`Expr<C>`] spans the lowering boundary; the two layers are
//! aliases that differ only in the column-reference type `C`:
//!
//! - [`L2Expr`] = `Expr<ColumnRef>` — name-based. The per-language front ends
//!   emit it (PromQL label matchers, SQL `WHERE` / projection / sort-key
//!   expressions) on the Layer-2 `relational` tree.
//! - [`L3Expr`] = `Expr<ColumnId>` — **positional**. The canonical L3
//!   `query_expr` tree carries it; the converter resolves every `ColumnRef`
//!   against the in-scope schema to produce it, so L3 column identity is
//!   unambiguous (no name collisions across a join).
//!
//! `Expr<C>` shares the scalar/operator vocabulary
//! ([`L3Scalar`], [`CompareOp`], [`ArithOp`]) — the **union** of what the two
//! front ends need: PromQL contributes `Regex` / `NotRegex` (`=~` / `!~`); SQL
//! contributes arithmetic, `CASE`, `IN`, `CAST`, `IS [NOT] NULL`, scalar
//! function calls, and the `LIKE` / `ILIKE` comparison family.

pub use asap_ir::intent_algebra::{ArithOp, ColumnRef, CompareOp, Expr, L2Expr, L3Expr, L3Scalar};
