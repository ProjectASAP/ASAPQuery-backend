//! Layer 3 IR — `core::intent_algebra` per `control_plane/docs/design.md` §6.
//!
//! ## Phase β cross-reference: asap-planner-rs PromQL patterns
//!
//! `ASAPQuery-backend/asap-planner-rs/src/planner/patterns.rs` defines five
//! PromQL `PromQLPattern` shapes. Every one of those shapes maps onto an
//! [`AggIntent`] kind here — this is the surface the controller's L3 layer
//! exposes so Phase γ can delete the asap-planner-rs binary without losing
//! coverage:
//!
//! | asap-planner-rs pattern (`patterns.rs`) | Controller L3 equivalent |
//! |---|---|
//! | `ONLY_TEMPORAL` quantile (`quantile_over_time(φ, m[range])`) | [`AggIntent::Quantile`] under [`QueryExpr::Window`] |
//! | `ONLY_TEMPORAL` funcs (`{sum,count,avg,min,max}_over_time`, `rate`, `increase`) | [`AggIntent::Sum`] / [`AggIntent::Count`] / [`AggIntent::Avg`] / [`AggIntent::Min`] / [`AggIntent::Max`] under `Window`, plus [`AggIntent::Rate`] / [`AggIntent::Increase`] for the counter-reset variants |
//! | `ONLY_SPATIAL` (`agg_op(metric)`) | `Aggregate{by, [intent]}` over a bare `Scan` (no `Window`) — the spatial `agg_op` is the [`AggIntent`] |
//! | `ONE_TEMPORAL_ONE_SPATIAL` (`agg_op(temporal_func(m[range]))`) | combined `Aggregate{by, [intent]}` over a `Window` — single-rooted L3 captures both axes natively |
//! | `histogram_quantile(φ, …)` (not a `patterns.rs` entry but the legacy planner refused these) | [`AggIntent::Quantile`] — the lowerer maps `histogram_quantile(q, bucket_metric)` to `Quantile { q, accuracy }`; bucket-aware reduction is a physical-planner concern, not an L3 intent. |
//!
//! Phase β additionally lifts these archive-only intents from the legacy
//! planner's "unsupported" branch into the L3 vocabulary so they get a
//! StreamingConfig entry (routed to the cold tier rather than the warm
//! sketch tier): [`AggIntent::Absent`],
//! [`AggIntent::Present`], [`AggIntent::Delta`], [`AggIntent::Deriv`],
//! [`AggIntent::PredictLinear`], [`AggIntent::HoltWinters`],
//! [`AggIntent::Idelta`], [`AggIntent::Irate`], [`AggIntent::Resets`],
//! [`AggIntent::Changes`].
//!
//! ## Phase B introduces the L3 vocabulary the planner pivots on:
//!
//! - [`AggIntent`] — what to compute, not how (no sketch types here;
//!   sketch binding is L4).
//! - [`QueryExpr`] — the L3 algebra DAG (intent-only, language-orthogonal,
//!   deployment-independent). Single-rooted per query; multi-root
//!   workload-level CSE lives one layer up in `types_v2::WorkloadPlan`.
//! - [`Schema`] — typed schema flowing on every L3 edge. `unique_keys` is
//!   the load-bearing field for CSE legality (`design.md` §6 line ~1284).
//!
//! Phase F adds the CSE surface that consumes `Schema::unique_keys`:
//!
//! - [`cse_reuse_is_legal`] — gatekeeper. Two `QueryExpr::Ref` consumers
//!   may share a `LetBinding` only when the producer's output schema
//!   has at least one `unique_keys` set. This is the proof point that
//!   `unique_keys` is load-bearing.
//! - `dedupe_subtrees` — the basic workload-level CSE pass that hoists
//!   structurally-identical sub-trees into shared `LetBinding`s
//!   (`design.md` §6 batched-queries example, ~line 1256) — lives in
//!   `optimizer::cse` as of Phase 2 step 5 (ASAPController places this
//!   pass in its cost-aware planning crate, not alongside the L3 IR type
//!   definitions). The full alpha-equivalence + nested-CSE algorithm is
//!   downstream.
//!
//! Scope reduction. The PR ships the variants the DC + PromQL deployment
//! actually needs (`Scan`, `Window`, `Aggregate`, `LetBinding`, `Ref`).
//! The full `design.md` §6 list is larger (`Filter`, `Project`,
//! `Partition`, `Distinct`, `Merge`, `Join`, `SetOp`, `Sort`, `Limit`,
//! `Subquery`, `WindowFunc`, `BinaryOp`); they are deferred to follow-up
//! phases so each variant lands with a planner consumer rather than as
//! dead code. Adding more is purely additive.
//!
//! Wire-up state. Nothing in `analyzer::Analyzer` or `planner/` consumes
//! these types yet — that's a downstream PR. Phase B exposes the IR so
//! that wiring becomes a focused change rather than a co-emission of new
//! types + new consumers. Phase F's `cse_reuse_is_legal` and
//! `dedupe_subtrees` are similarly defined here for the planner to grow
//! into; the cost-model-side consumer this once pointed at
//! (`workload_cost`) was removed as dead code (never wired) in the
//! 2026-07 retirement pass.

// The intent_algebra module is the new L3 surface — its re-exports are
// the public API that downstream phases will consume. Until Phase C
// (analyzer wiring) lands, none of these symbols have an in-tree call
// site, so the "unused" lints would fire on every build. Suppressing
// them keeps the lint baseline clean. `dead_code` covers the per-variant
// fields and per-impl helpers; `unused_imports` covers the re-export
// surface itself.
#![allow(dead_code, unused_imports)]

pub mod agg_intent;
pub mod query_expr;

// The **Layer-2 relational IR** — the `QueryExpr` tree the `query_parser`
// front ends emit (`promql.rs` / `sql.rs`). Formerly `legacy_expr`; it is
// the real, current L2 IR, not legacy debt. The planner / allocator /
// physical planner still consume it directly while their migration onto
// the canonical L3 `query_expr` types is in progress.
pub mod relational;

// `lower` (the L2 -> canonical-L3 conversion, `convert`/`convert_root`)
// was deleted in the ASAPPlanner pin migration -- see
// `relational.rs`'s module doc. `query_parser` entry points have called
// `asap_frontend_promql::lower_promql` directly since #428; this module
// was already dead in production before the migration and depended on
// the now-deleted `asap_l2` crate's L2 tree type.

// Step γ7: the L3 Binder — name resolution as an explicit pass. Produces
// the complete self-contained `Schema` every `ColumnId` indexes into;
// `convert_root` runs it so positional resolution is total. The
// `SchemaCatalog` seam (design.md §6 "three metadata sources") makes the
// schema source pluggable — usage-derived today, registry-backed later.

// Re-exports for the canonical surface — `crate::intent_algebra::*` for
// downstream callers that don't want to chase sub-module paths.
pub use agg_intent::{
    agg_accuracy, agg_is_exact, agg_is_mergeable, archive_only, as_frequency, default_cardinality,
    default_frequency, default_quantile, frequency, is_frequency_heavy_hitter, output_column,
    ranking_measure, AggIntent, MathFunc, RankingMeasure, TimeFunc,
};
pub use planner_types::pre_asap::{
    output_schema_for_aggregate, resolve_column_ref, resolve_column_refs, resolve_expr, Binder,
    Column, ColumnId, DataType, ResolveError, Schema, SchemaCatalog, UsageDerivedCatalog,
};
pub use query_expr::{
    aggregate_output_schema, between, conjoin, label_filter_to_predicate, ArithmeticOpKind,
    AtModifier, BinaryOpKind, ColumnRef, CompareOpKind, DataModel, GroupKeys, GroupSide,
    InfoMatcher, JoinKind, LabelFilter, Predicate, ProjectItem, QueryExpr, QueryExprError,
    Reduction, SampleKind, ScalarValue, SetOpKind, SortKey, Source, TimeShift, UnresolvedQueryExpr,
    VectorGrouping, VectorMatch, VectorMatchKind, WindowFuncKind, WindowKind,
};
