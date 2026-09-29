//! Lifecycle state shared by the sid catalog and the eviction
//! service. The status enum is intrinsic to *every* per-instance
//! lifecycle (whether keyed by agg_id historically or by sid today),
//! so it lives here next to the eviction + reconcile services that
//! drive transitions.

use std::time::Duration;

/// Lifecycle state of one sketch instance (sid). Derived from the
/// instance's `retired_at_ms` / `expires_at_ms` timestamps and the
/// current wall clock — never stored directly because retirement is
/// time-driven (a `Retired` sid auto-promotes to `Expired` when the
/// clock crosses `expires_at_ms`, without any state mutation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggStatus {
    /// Sid is reachable from the current `InstalledPrecomputePlan`. Writes
    /// accepted; queries see live data.
    Active,
    /// Sid's signature no longer appears in the current
    /// `InstalledPrecomputePlan` but its data is still within retention.
    /// Writes rejected by the §6.3 sid-level ingest barrier
    /// (`SketchStore::ingest_precompute_for_agg_config` returns
    /// `None`); reads allowed for queries that reference the
    /// historical window.
    Retired,
    /// Past retention. Scheduled for deletion by the eviction sweep
    /// in [`crate::storage_engines::sketch_db::lifecycle::eviction::SchemaEvictionService`].
    Expired,
}

/// Default retention for a sid before eviction.
///
/// 24 hours — covers dashboards / ad-hoc queries that may still
/// reference an old agg-signature mid-reconfigure. Shorter than the
/// typical SketchStore data retention (7d+) so sid-level eviction
/// runs first, freeing space cleanly without fighting per-record
/// retention.
pub const DEFAULT_RETIREMENT_RETENTION: Duration = Duration::from_secs(24 * 3600);
