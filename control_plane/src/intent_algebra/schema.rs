//! Layer 3 schema flow — every L3 edge carries a typed `Schema`.
//!
//! ## Phase 2 (docs/migration-plan-backend-plan.md)
//!
//! `Column` / `ColumnId` / `DataType` / `Schema` / `CseError` /
//! `cse_reuse_is_legal` are no longer defined in this repo — re-exported
//! from `planner_types::pre_asap::schema`. Unlike `AggIntent`'s merge
//! (Phase 1b), this one needed no boundary-conversion layer: asap_ir's
//! version is a pure additive superset of control_plane's pre-merge
//! version —
//!
//! - `Column` gains `table: Option<String>` (table/alias qualifier for
//!   SQL `t.col` disambiguation across joins) plus `Column::new()` /
//!   `Column::with_table()` constructors.
//! - `Schema` gains `closed: bool` (schema-on-read completeness flag,
//!   Apache Calcite `DynamicRecordType`-style) plus
//!   `column_id_qualified()`.
//!
//! Both new fields are `#[serde(default)]`, confirmed backward-compatible
//! by asap_ir's own tests (`schema_closed_defaults_to_open_when_absent`,
//! `column_table_defaults_to_none_when_absent`) — no wire-format break,
//! unlike `AccuracyTarget`'s tag-shape change in Phase 1b. This is what
//! made a full swap the obvious call here instead of converting at a
//! boundary: `agg_intent.rs`'s `to_asap_column`/`from_asap_column`/
//! `to_asap_dtype`/`from_asap_dtype` helpers from Phase 1b are deleted —
//! no longer needed once there's only one `Column`/`DataType` type.
//!
//! `DataType` itself was already byte-identical between the two repos;
//! no changes there at all.
//!
//! Blast radius from the swap: every `Column { .. }` / `Schema { .. }`
//! struct literal across this repo (~38 `Column{}` sites) needs the new
//! field addressed — either via the `table`/`closed` field explicitly,
//! or (preferred where the call site doesn't care) `Column::new(..)` /
//! `Schema::new(..)` / `Schema::with_time_index(..)`, which default the
//! new fields the same way the pre-merge constructors did.

// `cse_reuse_is_legal`/`CseError` were deleted upstream alongside
// `QueryExpr::Ref`/`LetBinding` (ASAPPlanner#181/#192) -- their only
// consumer in this repo was the now-deleted `optimizer::cse` (see
// control_plane/docs/design-asapplanner-pin-migration.md), so the
// re-export is dropped with it rather than stubbed.
pub use planner_types::pre_asap::schema::{Column, ColumnId, DataType, Schema};
