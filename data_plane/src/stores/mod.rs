//! Store layer.
//!
//! * `traits` — the `Store` trait every concrete store implements.
//! * `schema` — storage schemas + per-aggregation config / hot-reload
//!   config / measurement / precomputed-output types (was the
//!   top-level `data_model/` module before the 2026-05 data_plane
//!   reorg).
//! * `sketch_db` — the in-memory + persisted sketch DB. Logical
//!   layer (schema registry, schema timeline, backfill types /
//!   workers / HTTP endpoints) AND the physical storage backend
//!   (`sketch_db::sketch_store`) are co-located under this
//!   path.
//! * `gorilla_object_store` — S3/MinIO-backed Gorilla TSDB block
//!   store used by the archive tier.
//!
//! `SketchStore` is re-exported at the top level
//! (`crate::stores::SketchStore`) for call-site stability.

pub mod gorilla_object_store;
pub mod schema;
pub mod sketch_db;
pub mod traits;

pub use gorilla_object_store::{
    global_s3_cost_counters, ChunkRef, GorillaEngineConfig, GorillaQueryEngine, GorillaS3Config,
    GorillaS3ConfigError, GorillaS3Store, ObjectStore, RawSample, S3CostCounters, S3CostSnapshot,
    S3CostTrackingObjectStore,
};
pub use sketch_db::sketch_index::{
    AccuracyBound, Capability, SidLookup, SketchConfig, SketchEncoding, SketchIndex,
    SketchInstanceMetadata, SketchKindHandle, SketchSampleState, SketchTimeSeries,
};
pub use sketch_db::{AggSchema, AggStatus, SchemaRegistry, SketchStore};
pub use traits::*;
