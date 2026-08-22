//! The L3 **Binder** — name resolution as an explicit pass.
//!
//! ## Phase 2 step 4 (docs/migration-plan-backend-plan.md)
//!
//! `Binder`, `SchemaCatalog`, `UsageDerivedCatalog` are no longer defined
//! in this repo — re-exported from `asap_l2::binder`. `asap_l2`'s version
//! is a strict superset of this repo's pre-merge one: it additionally
//! walks `Sort.keys` / `Sort.partition_by` / `Relabel.value` for
//! referenced column names (this repo's version only covered
//! `Filter.pred` / `Aggregate.having` / `Join.pred` / `Project` — added
//! in Phase 2 step 3 to fix a real resolution regression that step's own
//! `Predicate` merge surfaced), and adds
//! [`Binder::bind_with_inherited`] for the `BinaryOp`-side-rebinding case
//! (issue #52) — a capability this repo's Binder never had.

pub use planner_types::pre_asap::binder::{Binder, SchemaCatalog, UsageDerivedCatalog};
