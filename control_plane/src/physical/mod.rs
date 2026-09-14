//! Physical compilation, deployment costs, sketch capabilities, and stage emission.

pub mod backend_stage;
pub mod compiler;
pub mod deployment_cost;
pub mod erp;
pub mod executable_binding;
mod pane_reuse;
pub mod post_asap;
pub(crate) mod realization;
pub mod runtime_capability;
pub mod sketch_catalog;
pub mod summary_catalog;
pub mod workload_cost;

pub mod publication;

pub(crate) mod maintained_population;
