//! Warm-tier sketch query engine.
//!
//! `SimpleEngine` is the long-standing PromQL/SQL/Elasticsearch-DSL
//! query path that answers from the in-memory sketch DB
//! ([`crate::stores::sketch_db::SimpleMapStore`]) and its
//! per-`agg_id` precomputed accumulators. It returns ε/δ-bounded
//! approximate answers for sketch-resident queries and `None` on
//! a capability miss (router falls through, which after Step-1 of
//! the JSONL deprecation means the archive tier or a hard 404 —
//! the JSONL leg has been deleted).
//!
//! Step-1 of the JSONL deprecation refactor moved this module
//! from `engines/simple_engine.rs` (single file) into
//! `engines/simple/{mod.rs, engine.rs, tests.rs}` so the
//! warm-tier query engine sits under its own tier-co-located
//! directory, mirroring [`crate::engines::gorilla`] for the
//! archive tier. The engine's data model + execution code is
//! kept verbatim in [`engine`]; this `mod.rs` is the public
//! surface re-exporting the long-standing types.

pub mod engine;

#[cfg(test)]
pub mod tests;

pub use engine::{
    QueryExecutionContext, QueryMetadata, QueryTimestamps, SimpleEngine, StoreQueryParams,
    StoreQueryPlan,
};
