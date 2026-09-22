//! Storage-backend routing policy: given the storage tier a metric is
//! configured for, decide which backends can serve it and in what
//! preference order.
//!
//! Split out of `asap_types`'s former `capability_matching` module (see
//! `scratchpad/artifacts/enum-unification-plan.md`). `StorageBackend` and
//! the `InstalledPrecomputePlan` wire format it's a field of both turned out to
//! have zero real `control_plane` dependency either — see
//! [`crate::storage_engines::types::storage_backend`]'s module doc — so
//! both moved into this crate; only the routing *decision* below was ever
//! split out separately.
//!
//! Since #746 there is no archive tier, so the policy has one branch left:
//! `PrometheusRemote` metrics answer from Prometheus, everything else from
//! the ASAP sketch tier. A query the ASAP tier cannot serve falls through
//! to the HTTP layer's Prometheus fallback rather than to another engine.

use crate::storage_engines::types::StorageBackend;
use asap_types::Statistic;
pub use planner_types::types::AccuracyTarget;

/// Returns the storage backends that can serve a query when the metric is
/// configured for `metric_storage_config`.
///
/// The returned list is **ordered by preference**: the router walks it in
/// order and dispatches to the first backend whose engine is registered,
/// falling through on `CapabilityMiss` / `Backend` to the next entry. With
/// the archive tier gone every list holds exactly one backend, so the
/// failover loop is a no-op in practice — it stays because the router is
/// also the `X-ASAP-Engine` override surface.
///
/// * `PrometheusRemote` → `[PrometheusRemote]`: the metric's raw samples
///   never landed in ASAP-managed storage, so there is no ASAP-tier sketch
///   to answer from. A missing engine surfaces as a `NoEngineRegistered`
///   503 from the HTTP handler — the correct fail-loud behaviour for a
///   misconfigured deploy.
/// * every ASAP-managed tier (`SketchStore`, `DoubleWrite`) →
///   `[SketchStore]`: the ASAP tier is the only engine left.
///
/// The `AccuracyTarget` no longer participates: an `Exact` request means
/// "do not answer from ε/δ-bounded sketches", which the HTTP handler
/// serves by forwarding to Prometheus before it ever reaches the router.
pub fn compatible_storage_backends(
    _stat: Statistic,
    metric_storage_config: StorageBackend,
) -> Vec<StorageBackend> {
    match metric_storage_config {
        // Prometheus-remote owns its own storage; there is no ASAP-tier
        // sketch to fall back on.
        StorageBackend::PrometheusRemote => vec![StorageBackend::PrometheusRemote],

        // Every ASAP-managed tier answers from the sketch tier.
        StorageBackend::SketchStore | StorageBackend::DoubleWrite => {
            vec![StorageBackend::SketchStore]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // storage-backend routing
    //
    // The `EngineRouter` (see `query_engine_routing.rs`) consults
    // `compatible_storage_backends(stat, metric_storage)` to pick a backend.
    // These tests pin the routing matrix so the dispatcher stays in lock-step
    // with the module doc.
    // -----------------------------------------------------------------------

    #[test]
    fn asap_managed_tiers_route_to_the_sketch_tier() {
        // #746: no archive tier. Every ASAP-managed storage axis answers
        // from the sketch tier.
        for cfg in [StorageBackend::SketchStore, StorageBackend::DoubleWrite] {
            let backends = compatible_storage_backends(Statistic::Quantile, cfg);
            assert_eq!(
                backends,
                vec![StorageBackend::SketchStore],
                "ASAP-managed tier {cfg:?} must route to the sketch tier",
            );
        }
    }

    #[test]
    fn prometheus_remote_keeps_its_own_slot() {
        let backends =
            compatible_storage_backends(Statistic::Sum, StorageBackend::PrometheusRemote);
        assert_eq!(backends, vec![StorageBackend::PrometheusRemote]);
    }

    /// Source-of-truth agreement check for the storage axis.
    ///
    /// For every `(Statistic, StorageBackend)` pair the returned backend
    /// list must be non-empty and dispatchable.
    #[test]
    fn capability_storage_backend_agreement() {
        let stats = [
            Statistic::Count,
            Statistic::Sum,
            Statistic::Cardinality,
            Statistic::Increase,
            Statistic::Rate,
            Statistic::Min,
            Statistic::Max,
            Statistic::Quantile,
            Statistic::Topk,
        ];
        let configs = [
            StorageBackend::SketchStore,
            StorageBackend::DoubleWrite,
            StorageBackend::PrometheusRemote,
        ];

        for &stat in &stats {
            for &cfg in &configs {
                let backends = compatible_storage_backends(stat, cfg);
                assert!(
                    !backends.is_empty(),
                    "compatible_storage_backends({stat:?}, {cfg:?}) returned empty \
                     — every metric configuration must route to at least one backend",
                );
                let expected_head = match cfg {
                    StorageBackend::PrometheusRemote => StorageBackend::PrometheusRemote,
                    _ => StorageBackend::SketchStore,
                };
                assert_eq!(
                    backends[0], expected_head,
                    "head mismatch for ({stat:?}, {cfg:?}): expected {expected_head:?}, got {:?}",
                    backends[0],
                );
            }
        }
    }
}
