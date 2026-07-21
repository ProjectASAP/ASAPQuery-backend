use crate::Statistic;
use serde::{Deserialize, Serialize};

use crate::AggregationType;

pub const ENGINE_ID_ASAP_QUERY: &str = "asap_query";
pub const ENGINE_ID_THANOS_QUERY: &str = "thanos_query";

pub const CANONICAL_QUERY_ENGINE_IDS: &[&str] = &[ENGINE_ID_ASAP_QUERY, ENGINE_ID_THANOS_QUERY];

// ---------------------------------------------------------------------------
// Phase-5: storage-backend capability axis
//
// Matching on `(metric, statistic, sub_type, window_size, grouping_labels,
// spatial_filter)` alone has no axis for "which storage tier serves this
// query." The Phase-5 `GorillaQueryEngine` (PR #85) introduces a parallel
// exact tier; the planner / router needs to disambiguate between ASAP-tier
// sketches and Gorilla-S3 chunks. See `docs/design-gorilla-s3-cold-engine.md`
// §8.
// ---------------------------------------------------------------------------

/// Which physical storage tier a query (or a metric configuration) routes to.
///
/// `SketchStore` is the default — every existing `AggregationConfig` and
/// `StreamingConfig` decodes into this variant via `#[serde(default)]`, so
/// pre-Phase-5 deploys keep dispatching to `ASAPQueryEngine` unchanged.
///
/// **Step-1 of the JSONL deprecation refactor** removed the
/// `ColdJsonlFallback` variant. The legacy local-FS JSONL leg
/// (`LocalFsColdStore`, `parse_jsonl`, the §5.2 raw-store
/// fallback) was deleted at the same commit; the surviving
/// failover surface is ASAP-tier sketch ↔ Thanos archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    /// Warm-tier sketch DB (today's `SketchStore` + accumulators).
    /// Served by `ASAPQueryEngine`. Default for unconfigured metrics.
    #[default]
    SketchStore,

    /// Thanos archive over MinIO/S3. The enum name is kept for
    /// serde/back-compat with existing configs, but its canonical
    /// query-engine identity is `thanos_query`. Gorilla is an
    /// archive chunk format/storage detail, not a public query engine.
    GorillaObjectStore,

    /// Double-write: the metric is written to both ASAP-tier sketches AND the
    /// Gorilla-S3 archive. Capability matching surfaces both options and the
    /// cost-aware dispatcher picks per query (typically ASAP-tier for low-
    /// latency approximate, archive for exact).
    DoubleWrite,

    /// Prometheus-remote: the metric's data is shipped raw to a
    /// Prometheus instance via the native OTLP receiver. Phase ε.2
    /// registers a `PrometheusForwardEngine` (HTTP-forwarder to
    /// Prometheus's `/api/v1/query`) under this slot so the
    /// controller's `RawAtEdgePrometheusArchive` mode can route a
    /// metric's queries to Prometheus directly. Mirrors the
    /// `GorillaObjectStore` slot's "single backend, no failover"
    /// semantics — there is no ASAP-tier sketch to fall back on for a
    /// Prometheus-remote metric.
    PrometheusRemote,
}

impl StorageBackend {
    /// Canonical string tag pinned for byte-comparable dispatch on the wire (mirrors
    /// the `data_source: <tag>` info-line on `QueryResult`). Engines
    /// register themselves under these IDs in the router.
    pub const fn data_source_id(self) -> &'static str {
        match self {
            StorageBackend::SketchStore => ENGINE_ID_ASAP_QUERY,
            StorageBackend::GorillaObjectStore => ENGINE_ID_THANOS_QUERY,
            StorageBackend::DoubleWrite => "double_write",
            StorageBackend::PrometheusRemote => "prometheus_remote",
        }
    }
}

pub fn parse_storage_backend_engine_id(s: &str) -> Option<StorageBackend> {
    match s {
        ENGINE_ID_ASAP_QUERY => Some(StorageBackend::SketchStore),
        ENGINE_ID_THANOS_QUERY => Some(StorageBackend::GorillaObjectStore),
        "double_write" => Some(StorageBackend::DoubleWrite),
        "prometheus_remote" => Some(StorageBackend::PrometheusRemote),
        _ => None,
    }
}

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

// ---------------------------------------------------------------------------
// Pure compatibility helpers
// ---------------------------------------------------------------------------

/// Returns the aggregation types that can serve this statistic.
///
/// This list is the **superset of compatibility** and, as of the
/// `promql_utilities` retirement, the **single source of truth** for it —
/// there used to be a second, independently-maintained table
/// (`promql_utilities::query_logics::logics::map_statistic_to_precompute_operator`,
/// the planner's own canonical map) that this one had to agree with. That
/// table was dead code (a Python-planner relic) and was deleted; this is
/// now the only table.
///
/// `QueryTreatmentType` is not consulted here — this list intentionally
/// enumerates *every* type that could serve the statistic, treatment-agnostic.
/// Selection between e.g. `Sum` (exact) and `CountMinSketch` (approximate)
/// for `Statistic::Sum` is made downstream by the caller.
pub fn compatible_agg_types(stat: Statistic) -> &'static [AggregationType] {
    match stat {
        // Sum: exact via Sum / MultipleSum; approximate via CountMinSketch.
        // Pre-fix this list omitted CountMinSketch, so a `sum_over_time(...)`
        // query against a CMS-only config fell through capability matching
        // and onto the cold tier.
        Statistic::Sum => &[
            AggregationType::Sum,
            AggregationType::MultipleSum,
            AggregationType::CountMinSketch,
            // Counters: ASAP-tier ingest stores counter metrics
            // (OTel `Sum` / monotonic=true) as Increase /
            // MultipleIncrease accumulators, whose `query`
            // implementation answers `Statistic::Sum` with the
            // latest cumulative value per series — matching
            // Prometheus' instant `sum(<counter>)` semantics.
            // Without these here, `sum by (zone) (http_requests_total)`
            // capability-misses (issue ProjectASAP/ASAPCollector#46;
            // diagnosis in PR #108).
            AggregationType::Increase,
            AggregationType::MultipleIncrease,
        ],
        // Count: exact via MultipleSum (the planner's canonical pick for
        // Count-Exact uses `MultipleSum` with sub_type="count"); approximate
        // via CountMinSketch / CountMinSketchWithHeap.
        //
        // HLL is also valid here: the ASAP-tier MVP demo
        // (ProjectASAP/ASAPCollector#46) plans `unique_users_per_min`
        // as an HLL agg and the replay client queries it with
        // `count(unique_users_per_min)`. `HllSketchAccumulator`
        // answers `Statistic::Count` as a cardinality alias —
        // see `precompute_operators/hll_sketch_accumulator.rs:220`.
        // Without HLL listed here the warm engine returns `status=error`
        // for every count-of-HLL replay row.
        Statistic::Count => &[
            AggregationType::MultipleSum,
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
            AggregationType::HLL,
        ],
        Statistic::Min | Statistic::Max => {
            &[AggregationType::MinMax, AggregationType::MultipleMinMax]
        }
        // Quantile: KLL (planner-emitted canonical pick) plus the
        // sketch types whose accumulators answer `Statistic::Quantile`
        // natively but that the planner's canonical map does not
        // emit. The MVP demo's controller (`ASAPCollector/controller`)
        // plans `http_requests_total_latency_ms` as a `DDSketch`
        // directly from `mvp-workload.yaml` and routes the resulting
        // delta payloads through the modified-OTLP wire format
        // (DDSketch state landing in `DDSketchAccumulator`, which
        // supports `Statistic::Quantile` — see
        // `precompute_operators/dd_sketch_accumulator.rs`). Without
        // DDSketch enumerated here, capability matching for an
        // out-of-YAML query like `quantile_over_time(0.99,
        // http_requests_total_latency_ms[1m])` would miss and the
        // ASAP-tier engine returns `EngineError::CapabilityMiss`.
        Statistic::Quantile => &[
            AggregationType::DatasketchesKLL,
            AggregationType::HydraKLL,
            AggregationType::DDSketch,
        ],
        // Rate / Increase: the canonical exact accumulators are the
        // counter-shaped Increase / MultipleIncrease, but `rate(...)`
        // and `increase(...)` over a CountMinSketch-backed agg are
        // also valid — CMS records every insert and answers
        // `Statistic::Rate` natively (events / range_ms when the
        // engine passes `range_ms` in query_kwargs; raw event count
        // as a units-of-events/window fallback otherwise — see
        // `precompute_operators/count_min_sketch_accumulator.rs`).
        // Without CMS / CMSWithHeap listed here, `rate(metric[5m])`
        // against a CMS-only config — the canonical MVP demo
        // CountMin path — capability-misses and the warm engine
        // returns `status=error`. Closes the PR #111 honest-gap
        // call-out for `Statistic::Rate` not implemented.
        Statistic::Rate | Statistic::Increase => &[
            AggregationType::Increase,
            AggregationType::MultipleIncrease,
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
        ],
        // Cardinality: HLL is the approximate cardinality estimator
        // (wired in via modified-OTLP from the agent processors) —
        // see `precompute_operators/hll_sketch_accumulator.rs`. The
        // historical exact-key trackers (`SetAggregator` /
        // `DeltaSetAggregator`) were retired wholesale; HLL is the
        // sole cardinality answerer today.
        Statistic::Cardinality => &[AggregationType::HLL],
        // Topk: `CountMinSketchWithHeap` is the canonical CMS-Heap
        // pattern. CountSketch is the second-tier reservoir-style
        // approximator the MVP demo's controller plans for
        // `top_endpoint_qps` (median-of-row estimator over a
        // signed-counter matrix). `CountSketchAccumulator` answers
        // `Statistic::Topk` directly — see
        // `precompute_operators/count_sketch_accumulator.rs:284`.
        // `CountSketchWithHeap` is the explicit heap-bearing variant
        // that also satisfies Topk through the heap directly
        // (parallel to `CountMinSketchWithHeap`); the analyzer's
        // `topk(...)` candidate returns `FrequencyTopk(Any)` so
        // either heap-bearing variant matches.
        // Without CountSketch / CountSketchWithHeap listed here,
        // `topk(K, top_endpoint_qps)` capability-misses and the
        // warm engine returns `status=error`.
        Statistic::Topk => &[
            AggregationType::CountMinSketchWithHeap,
            AggregationType::CountSketch,
            AggregationType::CountSketchWithHeap,
        ],
    }
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

/// Returns the required aggregation_sub_type for this statistic, if any.
/// `Min` requires `"min"`, `Max` requires `"max"`. All others are unconstrained.
pub fn required_sub_type(stat: Statistic) -> Option<&'static str> {
    match stat {
        Statistic::Min => Some("min"),
        Statistic::Max => Some("max"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the canonical-approximator picks driving the ASAP-tier query path
    /// (the "five sketch types" CMS / KLL / HLL / DDSketch / CountSketch
    /// canonical statistic table from PROGRESS.md). HLL / DDSketch /
    /// CountSketch route via the modified-OTLP wire format and are not in the
    /// planner's canonical map; KLL covers Quantile, CMS covers Sum + Count,
    /// CMSWithHeap covers Topk. Each must appear in its `Statistic`'s compat
    /// list — this is the bug fix that motivated this PR.
    #[test]
    fn five_sketch_canonical_statistics_in_compat_list() {
        // KLL → Quantile
        assert!(
            compatible_agg_types(Statistic::Quantile).contains(&AggregationType::DatasketchesKLL),
            "KLL must be a compatible type for Quantile",
        );
        // DDSketch → Quantile (Phase-3.1 fix). Required so the MVP demo's
        // `quantile_over_time(0.99, http_requests_total_latency_ms[1m])`
        // — which routes a DDSketch agg from `mvp-workload.yaml` and may
        // miss the inference-YAML exact-string match — still resolves
        // through capability matching instead of returning a 404.
        assert!(
            compatible_agg_types(Statistic::Quantile).contains(&AggregationType::DDSketch),
            "DDSketch must be a compatible type for Quantile (Phase-3.1 fix)",
        );
        // HLL → Cardinality (Phase-3.1 fix). HLL accumulators answer
        // `Statistic::Cardinality` natively (and `Statistic::Count` as a
        // cardinality alias); enumerating them here lets capability
        // matching pick up an HLL-only deploy.
        assert!(
            compatible_agg_types(Statistic::Cardinality).contains(&AggregationType::HLL),
            "HLL must be a compatible type for Cardinality (Phase-3.1 fix)",
        );
        // CMS → Sum (the headline bug fix that motivated this PR)
        assert!(
            compatible_agg_types(Statistic::Sum).contains(&AggregationType::CountMinSketch),
            "CountMinSketch must be a compatible type for Sum (PR fix)",
        );
        // CMS → Count
        assert!(
            compatible_agg_types(Statistic::Count).contains(&AggregationType::CountMinSketch),
            "CountMinSketch must be a compatible type for Count",
        );
        // CMSWithHeap → Topk
        assert!(
            compatible_agg_types(Statistic::Topk)
                .contains(&AggregationType::CountMinSketchWithHeap),
            "CountMinSketchWithHeap must be a compatible type for Topk",
        );
        // CountSketch → Topk (warm-engine-error-on-replay-queries fix).
        // Required so the MVP demo's `topk(5, top_endpoint_qps)` —
        // which routes through the agent's `countsketchprocessor`
        // and lands as a CountSketch-only config — resolves
        // through capability matching. Without this, the warm
        // engine returned `status=error` for every topk replay row.
        assert!(
            compatible_agg_types(Statistic::Topk).contains(&AggregationType::CountSketch),
            "CountSketch must be a compatible type for Topk (warm-engine-error fix)",
        );
        // HLL → Count (warm-engine-error-on-replay-queries fix). The
        // MVP demo's `count(unique_users_per_min)` is structurally a
        // PromQL `Statistic::Count` (the AggregationOperator::Count
        // → Statistic::Count mapping in
        // `promql_utilities::query_logics::enums`); the
        // `HllSketchAccumulator` answers it as a cardinality alias
        // (`hll_sketch_accumulator.rs:220`). Without HLL listed
        // here, capability matching missed and the warm engine
        // returned `status=error` for every count-of-HLL replay row.
        assert!(
            compatible_agg_types(Statistic::Count).contains(&AggregationType::HLL),
            "HLL must be a compatible type for Count (warm-engine-error fix)",
        );
    }
    // -----------------------------------------------------------------------
    // Phase-5: storage-backend routing
    //
    // The Phase-5 `EngineRouter` (see `asap-query-engine/src/routing/query_engine_routing.rs`)
    // consults `compatible_storage_backends(stat, accuracy, metric_storage)`
    // to pick a backend. These tests pin the routing matrix so the dispatcher
    // stays in lock-step with the design doc §8.
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

    #[test]
    fn storage_backend_default_is_asap_tier() {
        // `#[serde(default)]` on `StreamingConfig.storage_backend` (and on
        // `StorageBackend::default()`) MUST be `SketchStore` so pre-Phase-5
        // configs decode without bumping deploys onto the archive.
        assert_eq!(StorageBackend::default(), StorageBackend::SketchStore);
    }

    #[test]
    fn storage_backend_data_source_id_is_pinned() {
        // The router registers engines by these strings; dashboards
        // byte-compare them. Pin to catch accidental rename.
        assert_eq!(
            StorageBackend::SketchStore.data_source_id(),
            ENGINE_ID_ASAP_QUERY
        );
        assert_eq!(
            StorageBackend::GorillaObjectStore.data_source_id(),
            ENGINE_ID_THANOS_QUERY,
        );
        assert_eq!(StorageBackend::DoubleWrite.data_source_id(), "double_write",);
        assert_eq!(
            StorageBackend::PrometheusRemote.data_source_id(),
            "prometheus_remote",
        );
    }

    #[test]
    fn storage_backend_engine_id_parser_accepts_only_canonical_query_engines() {
        assert_eq!(
            parse_storage_backend_engine_id(ENGINE_ID_ASAP_QUERY),
            Some(StorageBackend::SketchStore),
        );
        assert_eq!(
            parse_storage_backend_engine_id(ENGINE_ID_THANOS_QUERY),
            Some(StorageBackend::GorillaObjectStore),
        );
        assert_eq!(parse_storage_backend_engine_id("not_an_engine"), None);
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
