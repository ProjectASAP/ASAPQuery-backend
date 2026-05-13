//! Warm-tier sketch query engine.
//!
//! `ASAPQueryEngine` is the long-standing PromQL/SQL/Elasticsearch-DSL
//! query path that answers from the in-memory sketch DB
//! ([`crate::storage_engines::sketch_db::SketchStore`]) and its
//! per-`agg_id` precomputed accumulators. It returns ε/δ-bounded
//! approximate answers for sketch-resident queries and `None` on
//! a capability miss (router falls through, which after Step-1 of
//! the JSONL deprecation means the archive tier or a hard 404 —
//! the JSONL leg has been deleted).

pub mod engine;

// Phase-5 reorg: warm-tier reducer moved to `sketch_db::query`. The
// engine still consumes it via that canonical path.
pub use crate::storage_engines::sketch_db::query as warm_tier;

#[cfg(test)]
pub mod tests;

pub use engine::{
    ASAPQueryEngine, QueryExecutionContext, QueryMetadata, QueryTimestamps,
    StoreQueryParams, StoreQueryPlan,
};
