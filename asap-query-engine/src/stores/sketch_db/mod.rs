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
//!
//! Phase 2a (this commit) adds only the in-memory schema registry derived
//! from the current `StreamingConfig`. Schemas are recreated on process
//! restart from the same config; there is no separate persistence yet.
//! That comes in Phase 2b along with the HTTP `POST /api/v1/streaming-config`
//! swap-diff handler that explicitly creates and retires schemas.

pub mod schema;

pub use schema::{AggSchema, AggStatus, SchemaRegistry};
