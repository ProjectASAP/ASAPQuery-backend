//! Assign physical nodes to stages and emit structured executor configurations.
//!
//! * [`StageId`] and [`Topology`] describe roles and topology.
//! * [`ColoredDag`] records node assignments and edges.
//! * [`StageAllocator`] assigns the three-stage edge → gateway → backend topology.
//! * [`ThreeStageEmitter`] builds per-stage configs for wire emission.
//!
//! Single-stage and zero-stage allocation return [`AllocateError::UnsupportedTopology`].

#![allow(dead_code, unused_imports)]

pub mod allocator;
pub mod dag;
pub mod emitter;
pub mod stage_id;

#[cfg(test)]
mod tests;

pub use allocator::{AllocateError, StageAllocator};
pub use dag::{ColoredDag, ColoredNode, NodeId};
pub use emitter::{
    ArchiveTierMetric, BackendAggregation, BackendReadout, BackendStageConfig, EdgeSketchProcessor,
    EdgeStageConfig, EmitError, Emitter, ExportTarget, GatewayMergeProcessor, GatewayStageConfig,
    PrometheusArchiveMetric, StageConfig, ThreeStageEmitter,
};
pub use stage_id::{StageId, Topology};
