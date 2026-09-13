//! Warm-tier query engine over sid-keyed sketch storage. Capability misses
//! allow the router to try archive execution.

pub(crate) mod catalog_resolver;
pub mod engine;
mod exact_subqueries;
pub mod live_serve;
pub mod logical_dag;
pub mod physical_dag;
pub mod post_asap_planner;
pub mod post_asap_readout;
pub mod summary_exec;
pub mod summary_executor;

pub use crate::storage_engines::sketch_db::query as asap_tier;

pub use engine::ASAPQueryEngine;

#[cfg(test)]
pub mod tests;
