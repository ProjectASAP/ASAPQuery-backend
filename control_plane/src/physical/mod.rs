//! Physical compilation, deployment costs, sketch capabilities, and stage emission.

pub mod colored_dag;
pub mod compiler;
pub mod deployment_cost;
pub mod erp;
pub mod executable_binding;
mod pane_reuse;
pub mod plan_cache;
pub mod post_asap;
pub(crate) mod realization;
pub mod runtime_capability;
pub mod sketch_catalog;
pub mod stage_split;
pub mod summary_catalog;
pub mod summary_reconcile;
pub mod topology;
pub mod workload_cost;
pub mod workload_planner;

pub mod publication;
