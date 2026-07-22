//! Storage-backend routing policy: given a `(statistic, accuracy)` query
//! and the storage tier a metric is configured for, decide which backends
//! can serve it and in what preference order.
//!
//! Split out of `asap_types`'s former `capability_matching` module (see
//! `scratchpad/artifacts/enum-unification-plan.md`). `StorageBackend` and
//! the `StreamingConfig` wire format it's a field of both turned out to
//! have zero real `control_plane` dependency either — see
//! [`crate::storage_engines::types::storage_backend`]'s module doc — so
//! both moved into this crate; only `AccuracyTarget` and the routing
//! *decision* below were ever split out separately, since neither has a
//! shared-struct-field reason to exist. Exercised only by this crate's
//! own [`super::query_engine_routing`].

use asap_types::Statistic;
use serde::{Deserialize, Serialize};

use crate::storage_engines::types::StorageBackend;

/// Accuracy hint pushed by the controller at intent-binding time
/// (`controller/docs/design.md` §6 `core::workload`). The Phase-5 capability
/// router consults this to decide whether a metric configured for both warm-
/// tier and Gorilla-S3 should answer from the archive (Exact) or the
/// approximate ASAP-tier sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccuracyTarget {
    /// Caller demands an exact answer; ASAP-tier sketches are not eligible
    /// unless they happen to be exact accumulators (Sum, MinMax, Increase).
    Exact,
    /// Caller accepts ε/δ-bounded approximate answers. Default.
    #[default]
    Approximate,
}

/// Returns the storage backends that can serve a `(statistic, accuracy)`
/// query when the metric is configured for `metric_storage_config`.
///
/// The returned list is **ordered by preference**: the router walks it in
/// order and dispatches to the first backend whose engine is registered,
/// falling through on `CapabilityMiss` / `Backend` to the next entry.
///
/// **ASAP-first centralization refactor**: the decision tree is now
/// owned here (and consumed identically by `EngineRouter::execute` and
/// `EngineRouter::execute_range`) so the HTTP transport layer never
/// re-derives routing. The policy is:
///
/// * `accuracy == Exact` → archive only `[GorillaObjectStore]` (served
///   by the `thanos_query` engine). The caller demands an exact answer,
///   so the ε/δ-bounded ASAP-tier sketches are not eligible — go
///   straight to the archive regardless of where the metric is stored.
/// * `accuracy == Approximate` (the default) → ASAP-first failover
///   `[SketchStore, GorillaObjectStore]` for any metric stored in an
///   ASAP-managed tier (`SketchStore`, `GorillaObjectStore`, or
///   `DoubleWrite`): try the warm sketch (`asap_query`) first and fall
///   back to the archive (`thanos_query`) on a capability miss. This
///   collapses the old per-`metric_storage` sequences into one shared
///   ASAP-first-then-archive contract.
/// * `PrometheusRemote` keeps its own single-backend sequence
///   `[PrometheusRemote]` (Phase ε.2): the metric's raw samples never
///   landed in ASAP-managed storage, so there is no ASAP-tier sketch to
///   fall back on and the accuracy hint does not apply. A missing
///   engine surfaces as a `NoEngineRegistered` 503 from the HTTP
///   handler — the correct fail-loud behaviour for a misconfigured
///   deploy.
pub fn compatible_storage_backends(
    _stat: Statistic,
    accuracy: AccuracyTarget,
    metric_storage_config: StorageBackend,
) -> Vec<StorageBackend> {
    match metric_storage_config {
        // Prometheus-remote owns its own storage; the accuracy hint does
        // not apply and there is no ASAP-tier sketch to fall back on.
        StorageBackend::PrometheusRemote => vec![StorageBackend::PrometheusRemote],

        // Every ASAP-managed tier shares the same ASAP-first policy,
        // gated only on the accuracy target.
        StorageBackend::SketchStore
        | StorageBackend::GorillaObjectStore
        | StorageBackend::DoubleWrite => match accuracy {
            // Exact: archive only — the warm sketches are ε/δ-bounded.
            AccuracyTarget::Exact => vec![StorageBackend::GorillaObjectStore],
            // Approximate: ASAP-tier first, archive (Thanos) fallback.
            AccuracyTarget::Approximate => vec![
                StorageBackend::SketchStore,
                StorageBackend::GorillaObjectStore,
            ],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Phase-5: storage-backend routing
    //
    // The Phase-5 `EngineRouter` (see `query_engine_routing.rs`) consults
    // `compatible_storage_backends(stat, accuracy, metric_storage)` to pick
    // a backend. These tests pin the routing matrix so the dispatcher stays
    // in lock-step with the design doc §8.
    // -----------------------------------------------------------------------

    #[test]
    fn exact_accuracy_routes_to_archive_only() {
        // ASAP-first refactor: `Exact` goes straight to the archive
        // (Thanos via the GorillaObjectStore slot) regardless of where
        // the metric is stored — the warm sketches are ε/δ-bounded.
        for cfg in [
            StorageBackend::GorillaObjectStore,
            StorageBackend::SketchStore,
            StorageBackend::DoubleWrite,
        ] {
            let backends = compatible_storage_backends(Statistic::Sum, AccuracyTarget::Exact, cfg);
            assert_eq!(
                backends,
                vec![StorageBackend::GorillaObjectStore],
                "Exact accuracy must route to archive only for {cfg:?}",
            );
        }
    }

    #[test]
    fn approximate_accuracy_is_asap_first_with_archive_fallback() {
        // ASAP-first refactor: every ASAP-managed tier shares the same
        // `[SketchStore, GorillaObjectStore]` sequence for `Approximate`
        // — try the warm sketch first, fall back to the Thanos archive
        // on a capability miss.
        for cfg in [
            StorageBackend::SketchStore,
            StorageBackend::GorillaObjectStore,
            StorageBackend::DoubleWrite,
        ] {
            let backends =
                compatible_storage_backends(Statistic::Quantile, AccuracyTarget::Approximate, cfg);
            assert_eq!(
                backends,
                vec![
                    StorageBackend::SketchStore,
                    StorageBackend::GorillaObjectStore,
                ],
                "Approximate accuracy must be ASAP-first then archive for {cfg:?}",
            );
        }
    }

    /// Source-of-truth agreement check for the storage axis.
    ///
    /// For every `(Statistic, AccuracyTarget, StorageBackend)` triple
    /// the returned backend list must be non-empty and its head must
    /// match the routing matrix in `compatible_storage_backends`'s
    /// docstring.
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
        let accuracies = [AccuracyTarget::Exact, AccuracyTarget::Approximate];
        let configs = [
            StorageBackend::SketchStore,
            StorageBackend::GorillaObjectStore,
            StorageBackend::DoubleWrite,
            StorageBackend::PrometheusRemote,
        ];

        for &stat in &stats {
            for &acc in &accuracies {
                for &cfg in &configs {
                    let backends = compatible_storage_backends(stat, acc, cfg);
                    assert!(
                        !backends.is_empty(),
                        "compatible_storage_backends({stat:?}, {acc:?}, {cfg:?}) returned empty \
                         — every metric configuration must route to at least one backend",
                    );
                    let last = *backends.last().unwrap();
                    assert!(
                        last == StorageBackend::SketchStore
                            || last == StorageBackend::GorillaObjectStore
                            || last == StorageBackend::PrometheusRemote,
                        "backend list for ({stat:?}, {acc:?}, {cfg:?}) must terminate in a \
                         dispatchable failover (SketchStore, GorillaObjectStore, or \
                         PrometheusRemote); got {last:?}",
                    );
                    // ASAP-first refactor: the head is determined by
                    // `(metric_storage_config, accuracy)`. PrometheusRemote
                    // keeps its single-backend slot; every ASAP-managed tier
                    // goes archive-only on `Exact` and ASAP-tier-first on
                    // `Approximate`.
                    let expected_head = match (cfg, acc) {
                        (StorageBackend::PrometheusRemote, _) => StorageBackend::PrometheusRemote,
                        (_, AccuracyTarget::Exact) => StorageBackend::GorillaObjectStore,
                        (_, AccuracyTarget::Approximate) => StorageBackend::SketchStore,
                    };
                    assert_eq!(
                        backends[0], expected_head,
                        "head mismatch for ({stat:?}, {acc:?}, {cfg:?}): expected {expected_head:?}, got {:?}",
                        backends[0],
                    );
                }
            }
        }
    }
}
