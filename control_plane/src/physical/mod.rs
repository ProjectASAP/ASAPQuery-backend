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
//! | [`allocator`] | `SketchAllocator` — assigns physical ops to pipeline stages over the legacy [`planner_types::pre_asap::QueryExpr`] IR |
//! | [`planner`] | `PhysicalPlanner` — `QueryExpr` → staged sub-plans (DC three-stage emission) |
//! | [`plan`] | `PlanNode` / `PlanSummary` — annotated plan tree with cost estimates + stage colouring |
//! | [`sketch_catalog`] | Candidate sketch types per `AggIntent`, default `SketchParams`, memory estimation |
//! | [`stage_split`] | AST-aware hierarchical stage assignment (legacy `QueryExpr` splitter) |
//! | [`colored_dag`] | L5 typed colouring framework over the canonical [`crate::physical::post_asap::PhysicalExpr`] DAG (`StageId` + `Topology` + per-stage emitter) |
//! | [`topology`] | Re-exports `StageId` + `Topology` from [`colored_dag::stage_id`] — design.md §5 entry point for deployment-topology descriptors |

pub mod allocator;
pub mod colored_dag;
pub mod compiler;
pub mod deployment;
pub mod deployment_cost;
pub mod erp;
pub mod executable_binding;
mod pane_reuse;
pub mod plan;
pub mod plan_cache;
pub mod planner;
pub mod post_asap;
pub(crate) mod realization;
pub mod runtime_capability;
pub mod sketch_catalog;
pub mod stage_split;
pub mod summary_catalog;
pub mod summary_reconcile;
pub mod topology;
pub mod window_fusion;
pub mod workload_cost;
pub mod workload_planner;

// Convenience re-exports — preserve the surface that consumers of the
// former `algebra` module relied on.
pub use allocator::SketchAllocator;
pub use plan::{CostEstimate, ExecutionMode, PipelineStage, PlanNode, PlanSummary};

pub mod publication;
