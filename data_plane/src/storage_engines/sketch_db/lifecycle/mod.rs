//! Sid lifecycle — `Active`/`Retired`/`Expired` over per-sid metadata,
//! eviction services, and the ingest barrier.
//!
//! Post-M2.3 reorg #5. The sid-level lifecycle state itself (the
//! `retired_at_ms` / `expires_at_ms` fields on
//! [`crate::storage_engines::sketch_db::index::SketchInstanceMetadata`],
//! plus `force_retire` / `force_expire` / `is_writable` / `list_by_status`
//! / `remove_instance` / `remove_instances_for_agg_config` methods on
//! `SketchStore`) lives in `index/` next to the data it gates. This
//! module owns the **services** that drive lifecycle transitions:
//!
//! - [`eviction::SchemaEvictionService`] — background tokio task that
//!   sweeps `Expired` agg-configs (from the `SchemaRegistry`) and
//!   removes their residual sids from `SketchStore` via
//!   `remove_instances_for_agg_config`. Schedule-driven half of the
//!   "control plane dropped a config → its sids go away" flow.
//!
//! - [`reconcile::reconcile_from_streaming_config`] — sid-level mirror
//!   of [`SchemaRegistry::reconcile`](crate::storage_engines::sketch_db::schema::SchemaRegistry::reconcile),
//!   landed in schema-retirement #4 alongside the otel.rs reconcile
//!   call-site migration.

pub mod eviction;
pub mod reconcile;
pub mod status;

pub use eviction::{
    warn_if_retention_inverted, SchemaEvictionConfig, SchemaEvictionHandle, SchemaEvictionService,
};
pub use reconcile::{reconcile_from_streaming_config, SidReconcileSummary};
pub use status::{AggStatus, DEFAULT_RETIREMENT_RETENTION};
