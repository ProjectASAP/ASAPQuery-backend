//! Store layer — the sketch DB.
//!
//! This module houses the sketch DB's physical + logical layers:
//!
//! * `traits` — the `Store` trait every concrete store implements.
//! * `sketch_db` — the top-level sketch DB module. Logical
//!   layer (schema registry, schema timeline, backfill types /
//!   workers / HTTP endpoints) AND the physical storage backend
//!   (`sketch_db::simple_map_store`) are co-located under this
//!   path so the project's identity is unambiguous.
//! * `promsketch_store` — legacy alternative store, currently
//!   commented out of the public API. Kept for reference.
//!
//! `SimpleMapStore` is re-exported at the top level
//! (`crate::stores::SimpleMapStore`) for call-site stability:
//! callers should not care whether it lives under `sketch_db` or
//! at the `stores` top level.

pub mod promsketch_store;
pub mod sketch_db;
pub mod sketch_index;
pub mod traits;

// pub use promsketch_store::PromSketchStore;
pub use sketch_db::{AggSchema, AggStatus, SchemaRegistry, SimpleMapStore};
pub use sketch_index::{
    AccuracyBound, Capability, SidLookup, SketchConfig, SketchEncoding, SketchIndex,
    SketchInstanceMetadata, SketchKindHandle, SketchSampleState, SketchTimeSeries,
};
pub use traits::*;
