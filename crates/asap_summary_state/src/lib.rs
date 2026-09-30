//! Backend-owned summary state: the kernels that ingest and the sketch store
//! keep, their stored byte formats, and conversion to Planner physical states.
//!
//! Planner's `asap-physical-operators` keeps only in-memory computation state.
//! Storage formats, delta reconstruction and the legacy per-statistic kernels
//! are deployment concerns and live here.

pub mod summary_kernels;
pub use summary_kernels::{factory, traits};
pub use traits::*;

mod aggregation_type;
pub use aggregation_type::AggregationType;

pub mod codec;
pub mod physical;
pub mod stored_state;

pub use asap_physical_operators::{KeyByLabelValues, Measurement, Statistic};
