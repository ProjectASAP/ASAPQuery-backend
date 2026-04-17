//! Sketch DB scaffolding (Phase 2 of the sketch DB design).
//!
//! See [`docs/design-sketch-db.md`](../../../../../docs/design-sketch-db.md)
//! for the full architecture. This module houses the components that live
//! "above" the existing `SimpleMapStore` and turn it into a sketch-aware
//! storage engine over time:
//!
//! * `schema` — per-`agg_id` `AggSchema` with `Active` / `Retired` /
//!   `Expired` lifecycle, plus the `SchemaRegistry` that the ingest path
//!   consults via `is_writable(agg_id)` (the §6.3 write-side barrier).
//!   Also exposes the §7 metric timeline (`timeline_for_metric`) which
//!   the query path uses to dispatch per-segment across reconfigure
//!   boundaries.
//!
//! Phase 2a added only the in-memory schema registry derived from the
//! current `StreamingConfig`; Phase 2b wired the
//! `POST /api/v1/streaming-config` swap handler to drive reconciliation
//! explicitly. Phase 3a (this commit) adds the §7 schema-timeline read
//! API — derived on-demand from registry state — so the query engine
//! can stitch results across schema changes (Phase 3b wires that
//! dispatch).

pub mod schema;

pub use schema::{AggSchema, AggStatus, SchemaRegistry, TimelineCoverage, TimelineSegment};
