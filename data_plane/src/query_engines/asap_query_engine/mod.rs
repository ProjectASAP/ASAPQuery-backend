//! Warm-tier sketch query engine.
//!
//! `ASAPQueryEngine` is the PromQL query path that answers from the
//! in-memory sketch DB
//! ([`crate::storage_engines::sketch_db::SketchStore`]) and its
//! per-`agg_id` precomputed accumulators. It returns ε/δ-bounded
//! approximate answers for sketch-resident queries and `None` on
//! a capability miss (router falls through, which after Step-1 of
//! the JSONL deprecation means the archive tier or a hard 404 —
//! the JSONL leg has been deleted).

pub mod engine;
pub mod l4_lowering;
pub mod l4_readout;
pub mod live_serve;
pub mod shadow_compare;
pub mod summary_executor;

// Phase-5 reorg: ASAP-tier reducer moved to `sketch_db::query`. The
// engine still consumes it via that canonical path.
pub use crate::storage_engines::sketch_db::query as asap_tier;

#[cfg(test)]
pub mod tests;

pub use engine::ASAPQueryEngine;
