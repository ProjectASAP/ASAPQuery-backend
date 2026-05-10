//! Layer 5 — `stage_split` (StageAllocator + Emitter framework).
//!
//! Per `controller/docs/design.md` §6 (line ~765 `core::physical`) and
//! the §6 batched-queries example (line ~1376) — Phase E lands the
//! typed L5 colouring + emitter for the DC three-stage topology
//! (edge → gateway → backend).
//!
//! Pipeline position. L4 [`crate::sketch_algebra::SketchExpr`] is the
//! input — sketch-bound, language-orthogonal, deployment-independent.
//! L5 paints each `SketchExpr` node with a [`StageId`] and emits one
//! [`emitter::StageConfig`] per occupied stage. The configs become the
//! OpAMP `RemoteConfig` payload (edge / gateway) and the backend
//! `StreamingConfig` (backend) — Phase G+ wires the actual push.
//!
//! Module layout:
//!
//! - [`stage_id`] — [`StageId`] enum + [`Topology`] enum.
//! - [`colored_dag`] — [`ColoredDag`] IR (the allocator's output).
//! - [`allocator`] — [`StageAllocator`] + [`AllocateError`].
//! - [`emitter`] — [`Emitter`] trait + [`ThreeStageEmitter`].
//!
//! Scope reduction (per orchestrator spec): Phase E ships only the DC
//! lifecycle topology. `Topology::SingleStage` /
//! `Topology::ZeroStage` are surfaced as variants but
//! [`StageAllocator::allocate`] returns
//! [`AllocateError::UnsupportedTopology`] for them — the
//! single-stage / zero-stage allocators land with their respective
//! deployment-model crates (asap-query / asap-fusion) in later phases.
//!
//! Wire-up state. The typed L5 path is opt-in via the
//! `USE_TYPED_STAGE_SPLIT` env var consulted by the existing
//! `planner::stage_split` module — see [`crate::planner::stage_split`]
//! for the additive call path. Existing untyped callers keep working
//! unchanged.

#![allow(dead_code, unused_imports)]

pub mod allocator;
pub mod colored_dag;
pub mod emitter;
pub mod stage_id;

#[cfg(test)]
mod tests;

// Re-exports — `crate::stage_split::*` for downstream callers.
pub use allocator::{AllocateError, StageAllocator};
pub use colored_dag::{ColoredDag, ColoredNode, NodeId};
pub use emitter::{
    ArchiveTierMetric, BackendAggregation, BackendReadout, BackendStageConfig, EdgeSketchProcessor,
    EdgeStageConfig, EmitError, Emitter, ExportTarget, GatewayMergeProcessor, GatewayStageConfig,
    PrometheusArchiveMetric, StageConfig, ThreeStageEmitter,
};
pub use stage_id::{StageId, Topology};
