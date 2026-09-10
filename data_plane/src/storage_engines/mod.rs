//! Store layer.
//!
//! * `traits` — the `Store` trait every concrete store implements.
//! * `types` — data types used by the storage layer (and consumed
//!   cross-module by drivers / precompute_engine / query_engines).
//!   Renamed from `schema/` in the 2026-05 reorg to avoid the
//!   name-clash with `sketch_db::schema/` (per-`agg_id` lifecycle).
//! * `sketch_db` — the in-memory + persisted sketch DB. Logical
//!   layer (schema registry, schema timeline, backfill types /
//!   workers / HTTP endpoints) AND the physical storage backend
//!   (`sketch_db::index`) are co-located under this path.
//!
//! The archive tier is served by the
//! [`crate::query_engines::thanos_query_engine::ThanosQueryEngine`]
//! (Path A2). The superseded in-process `gorilla_object_store`
//! (custom GORILLA1 container format) has been deleted.
//!
//! `SketchStore` is re-exported at the top level
//! (`crate::storage_engines::SketchStore`) for call-site stability.

pub mod sketch_db;
pub mod traits;
pub mod types;

pub use sketch_db::index::{
    AccuracyBound, Capability, SeriesLookup, SketchAlgorithm, SketchConfig, SketchEncoding,
    SketchInstanceMetadata, SketchSampleState, SketchStore, SketchTimeSeries,
};
pub use sketch_db::AggStatus;
pub use traits::*;
