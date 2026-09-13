//! Sid lifecycle reconciliation and eviction.
//!
//! Instance metadata in the index owns retirement and expiry timestamps.
//! [`reconcile::reconcile_from_streaming_config`] retires orphaned precompute
//! instances; [`eviction::SchemaEvictionService`] removes expired instances.

pub mod eviction;
pub mod reconcile;
pub mod status;

pub use eviction::{
    warn_if_retention_inverted, SchemaEvictionConfig, SchemaEvictionHandle, SchemaEvictionService,
};
pub use reconcile::{
    reconcile_from_streaming_config, reconcile_if_config_changed, SidReconcileSummary,
};
pub use status::{AggStatus, DEFAULT_RETIREMENT_RETENTION};
