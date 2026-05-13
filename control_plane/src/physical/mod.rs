//! L5 — **physical execution plan** framework.
//!
//! Per `control_plane/docs/design.md` §3/§5/§6 `core::physical`: this is
//! the stage allocator + sketch catalogue + physical planner surface.
//! Refactor 2026-05 (`refactor/controller-layered-cleanup`) absorbed
//! the former `controller/src/algebra/{allocator,physical,plan}.rs`
//! + `controller/src/algebra/directory.rs` (renamed to
//! [`sketch_catalog`]) + the L5 colored-DAG framework formerly at
//! `controller/src/stage_split/` ([`colored_dag`] subdir) here.
//!
//! Module layout:
//!
//! | File | Role |
//! |---|---|
//! | [`allocator`] | `SketchAllocator` — assigns physical ops to pipeline stages over the legacy [`crate::intent_algebra::legacy_expr::QueryExpr`] IR |
//! | [`planner`] | `PhysicalPlanner` — `QueryExpr` → staged sub-plans (DC three-stage emission) |
//! | [`plan`] | `PlanNode` / `PlanSummary` — annotated plan tree with cost estimates + stage colouring |
//! | [`sketch_catalog`] | Candidate sketch types per `AggIntent`, default `SketchParams`, memory estimation |
//! | [`stage_split`] | AST-aware hierarchical stage assignment (legacy `QueryExpr` splitter) |
//! | [`colored_dag`] | L5 typed colouring framework over the canonical [`crate::sketch_algebra::PhysicalExpr`] DAG (`StageId` + `Topology` + per-stage emitter) |
//! | [`topology`] | Re-exports `StageId` + `Topology` from [`colored_dag::stage_id`] — design.md §5 entry point for deployment-topology descriptors |

pub mod allocator;
pub mod colored_dag;
pub mod plan;
pub mod planner;
pub mod sketch_catalog;
pub mod stage_split;
pub mod topology;

// Convenience re-exports — preserve the surface that consumers of the
// former `algebra` module relied on.
pub use allocator::SketchAllocator;
pub use plan::{CostEstimate, ExecutionMode, PipelineStage, PlanNode, PlanSummary};
