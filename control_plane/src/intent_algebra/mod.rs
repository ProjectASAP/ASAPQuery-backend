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
//! - [`lower_parsed_query`] — `query_parser::ParsedQuery` → [`QueryExpr`]
//!   single-query lowering.
//!
//! Phase F adds the CSE surface that consumes `Schema::unique_keys`:
//!
//! - [`cse_reuse_is_legal`] — gatekeeper. Two `QueryExpr::Ref` consumers
//!   may share a `LetBinding` only when the producer's output schema
//!   has at least one `unique_keys` set. This is the proof point that
//!   `unique_keys` is load-bearing.
//! - [`dedupe_subtrees`] — basic workload-level CSE pass that hoists
//!   structurally-identical sub-trees into shared `LetBinding`s
//!   (`design.md` §6 batched-queries example, ~line 1256). The full
//!   alpha-equivalence + nested-CSE algorithm is downstream.
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
//! into; the cost-model side that consumes them lives in
//! `planner::cost_model::workload_cost`.

// The intent_algebra module is the new L3 surface — its re-exports are
// the public API that downstream phases will consume. Until Phase C
// (analyzer wiring) lands, none of these symbols have an in-tree call
// site, so the "unused" lints would fire on every build. Suppressing
// them keeps the lint baseline clean. `dead_code` covers the per-variant
// fields and per-impl helpers; `unused_imports` covers the re-export
// surface itself.
#![allow(dead_code, unused_imports)]

pub mod agg_intent;
pub mod cse;
pub mod lower;
pub mod query_expr;
pub mod schema;

// Refactor 2026-05 (`refactor/controller-layered-cleanup`): the
// pre-existing legacy L3+ IR formerly at `controller/src/algebra/expr.rs`
// + `controller/src/algebra/lower.rs` lives here while a separate
// follow-up unifies it with the canonical `query_expr` / `lower`
// modules above. These two `legacy_*` modules carry the heavy
// `QueryExpr` / `AggIntent` types used by the planner, allocator,
// physical planner, query_parser, and language_logical_plan modules
// today.
pub mod column_resolution;
pub mod legacy_expr;
pub mod legacy_lower;

// Re-exports for the canonical surface — `crate::intent_algebra::*` for
// downstream callers that don't want to chase sub-module paths.
pub use agg_intent::AggIntent;
pub use cse::{dedupe_subtrees, CseWorkloadPlan};
pub use lower::{lower_parsed_query, LoweringError};
pub use query_expr::{
    from_legacy_scalar, BinaryOpKind, BindingScope, ColumnRef, GroupSide, HavingPredicate,
    JoinKind, LabelFilter, LiteralValue, PartitionKeys, Predicate, ProjectItem, QueryExpr,
    QueryExprError, SetOpKind, SortKey, Source, VectorGrouping, VectorMatch, VectorMatchKind,
    WindowKind,
};
pub use schema::{cse_reuse_is_legal, Column, ColumnId, CseError, DataType, Schema};

// Step β plumbing: schema-driven column resolution helpers used by the
// legacy planning stack (`optimizer/engine.rs`, `physical/{allocator,
// planner, stage_split}.rs`, `query_parser/*`, `intent_algebra::
// legacy_lower`) to carry an inherited `Schema` alongside every legacy
// `QueryExpr` traversal. Consumers call `resolve_column_ref` at the point
// where they need a positional `ColumnId` — Step γ migrates variants
// one at a time onto the canonical positional form.
pub use column_resolution::{
    infer_schema_for_root, infer_source_schema, output_schema_for_aggregate, resolve_column_ref,
    resolve_column_refs, resolve_named_keys, ResolveError,
};

// Step γ1 bridge: one-way `legacy_expr::QueryExpr::Aggregate` →
// canonical `query_expr::QueryExpr::Aggregate` helper. Consumers that
// need canonical-shape pattern-matching (`by: Vec<ColumnId>`, `aggs:
// Vec<AggIntent>`) call this on an inherited Schema; the legacy variant
// remains in `legacy_expr.rs` until every consumer entry point migrates
// (Step γ7 or later).
pub mod aggregate_bridge;
pub use aggregate_bridge::{
    bridge_aggregate_to_canonical, BridgeError, BridgedAggregate,
};

// Step γ4 bridge: one-way `legacy_expr::QueryExpr::TopK` → one of two
// canonical shapes (heavy-hitter `AggIntent::TopK` vs generic
// `Sort + Limit`) per design.md §6 "What was removed" row 1. Consumers
// call this on an inherited Schema; the legacy variant remains in
// `legacy_expr.rs` until every consumer entry point migrates (Step γ7
// or later).
pub mod topk_bridge;
pub use topk_bridge::{bridge_topk, BridgeError as TopKBridgeError, BridgedTopK};

// Step γ3 bridge: one-way `legacy_expr::QueryExpr::WindowedAgg` →
// canonical `Window { child: Aggregate { .. } }` helper. Sidesteps the
// 20+ consumers that depend on the fused `WindowedAgg`'s window-sketch
// lifecycle invariant — they continue matching the legacy variant; only
// consumers that want the canonical stacked view call this bridge. The
// legacy variant remains in `legacy_expr.rs` until Step γ7 retires it.
pub mod windowed_agg_bridge;
pub use windowed_agg_bridge::{
    bridge_windowed_agg_to_canonical, output_schema_for_windowed_agg,
    BridgeError as WindowedAggBridgeError, BridgedWindowedAgg,
};

// Step γ2 bridge: one-way `legacy_expr::QueryExpr::SketchAgg` →
// canonical-shape `BridgedAggregate` helper. Companion to the γ1
// Aggregate bridge — SketchAgg's single-intent / single-column shape
// maps onto the same `BridgedAggregate` carrier (with `having: None`)
// so consumers can match on `by` / `aggs` regardless of which legacy
// variant the data came from.
pub mod sketch_agg_bridge;
pub use sketch_agg_bridge::{
    bridge_sketch_agg_to_canonical, output_schema_for_sketch_agg,
};
