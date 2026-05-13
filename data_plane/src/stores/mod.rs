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
//!   (`sketch_db::store`) are co-located under this path.
//! * `gorilla_object_store` — S3/MinIO-backed Gorilla TSDB block
//!   store used by the archive tier.
//!
//! `SketchStore` is re-exported at the top level
//! (`crate::stores::SketchStore`) for call-site stability.

pub mod gorilla_object_store;
pub mod sketch_db;
pub mod traits;
pub mod types;

pub use gorilla_object_store::{
    global_s3_cost_counters, ChunkRef, GorillaEngineConfig, GorillaQueryEngine, GorillaS3Config,
    GorillaS3ConfigError, GorillaS3Store, ObjectStore, RawSample, S3CostCounters, S3CostSnapshot,
    S3CostTrackingObjectStore,
};
pub use sketch_db::store::{
    AccuracyBound, Capability, SidLookup, SketchConfig, SketchEncoding, SketchStore,
    SketchInstanceMetadata, SketchKindHandle, SketchSampleState, SketchTimeSeries,
};
pub use sketch_db::{AggSchema, AggStatus, SchemaRegistry};
pub use traits::*;
