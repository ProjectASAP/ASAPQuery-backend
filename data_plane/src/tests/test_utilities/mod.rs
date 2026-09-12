//! Test utilities for query equivalence testing
//!
//! Provides engine-construction helpers shared by the surviving
//! capability-miss + asap-tier integration tests. The
//! `comparison.rs` module was retired with B7.5 because it
//! reached into the now-deleted `QueryExecutionContext` /
//! `StoreQueryPlan` legacy types.

pub mod engine_factories;
pub mod timing;

pub use engine_factories::*;

pub mod planning;
