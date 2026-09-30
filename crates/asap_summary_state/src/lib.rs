//! Backend-owned summary storage: stored byte formats of Planner kernel
//! states, edge wire decoding, delta reconstruction and readout binding.
//!
//! Planner's `asap-physical-operators` owns summary computation (update,
//! merge, estimate). The store keeps those kernel states directly; this crate
//! only encodes, decodes and reads them.

mod aggregation_type;
pub use aggregation_type::AggregationType;

pub mod codec;
pub mod stored_state;
pub mod univmon;

pub use asap_physical_operators::{AggregateCore, KeyByLabelValues, Measurement, Statistic};
pub use stored_state::codec::StoredState;
