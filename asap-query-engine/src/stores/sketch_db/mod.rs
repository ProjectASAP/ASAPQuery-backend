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
//! Earlier phases: 2a added the in-memory schema registry; 2b wired
//! the `POST /api/v1/streaming-config` swap handler to drive
//! reconciliation explicitly; 2c added on-disk persistence; 3a added
//! the §7 schema-timeline read API; 3b added the cross-schema
//! combiner (`crate::engines::timeline_dispatch`).
//!
//! * `backfill` — §10 of the design. `BackfillJob` lifecycle types
//!   (`BackfillSource`, `BackfillStatus`, `Coverage`) plus an
//!   in-memory `BackfillRegistry` that the controller pushes jobs
//!   into and the worker pool drains. Phase 5a (this commit) lands
//!   the data types and registry only — no I/O, no workers, no
//!   HTTP. Follow-up phases add the `RawSampleReader` trait
//!   (5b), the worker pool (5c), HTTP trigger / list endpoints (5d),
//!   real rebuild logic (5e), and coverage integration with the
//!   query path (5f).

pub mod backfill;
pub mod backfill_worker;
pub mod raw_sample_reader;
pub mod schema;

pub use backfill::{BackfillJob, BackfillRegistry, BackfillSource, BackfillStatus, Coverage};
pub use backfill_worker::{BackfillWorker, BackfillWorkerError, WindowProcessor};
pub use raw_sample_reader::{
    LabelFilter, MockRawSampleReader, RawSample, RawSampleReader, RawSampleReaderError,
};
pub use schema::{AggSchema, AggStatus, SchemaRegistry, TimelineCoverage, TimelineSegment};
