//! Phase-5 capability router: dispatches a `(query, metric_storage)` pair
//! to the engine that owns the chosen storage tier.
//!
//! The router holds a small map keyed by
//! [`crate::storage_engines::types::StorageBackend::data_source_id`]
//! and walks the ordered backend list returned by
//! [`super::capability_matching::compatible_storage_backends`]. The first registered
//! engine answers; on a recoverable backend failure (`EngineError::Backend`),
//! the router falls through to the next compatible backend if the list
//! still has options. A hard capability miss in the head engine likewise
//! falls through (matching the pre-Phase-5 §5.2 fallback contract).
//!
//! See `docs/design-gorilla-s3-cold-engine.md` §8 for the design rationale
//! and the routing matrix the router walks.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tracing::{debug, warn};

use crate::storage_engines::types::StorageBackend;
use asap_types::Statistic;

use super::capability_matching::{compatible_storage_backends, AccuracyTarget};
use crate::query_engines::{EngineError, QueryResult};

// ---------------------------------------------------------------------------
// `QueryEngine` trait — the abstraction the router holds.
//
// The trait is intentionally narrow: a single `execute(&str)` method (so it
// integrates with both `ASAPQueryEngine::handle_query` and
// `GorillaQueryEngine::execute` without forcing either side to refactor its
// public surface), plus a `capabilities()` accessor the router consults at
// registration time.
// ---------------------------------------------------------------------------

/// Which storage tiers a range query is eligible to consult, decided by
/// the caller from the query's `[start_ms, end_ms]` window relative to
/// the warm-retention boundary. Consumed by
/// [`EngineRouter::execute_range_for_tier`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeTier {
    /// The window lies entirely inside the warm-retention horizon — the
    /// archive is guaranteed empty for it, so its leg of the failover
    /// sequence is suppressed.
    WarmOnly,
    /// The window reaches at or past the warm-retention boundary
    /// (genuinely-archived data, a boundary-straddling range, or an
    /// unknown boundary). The full ASAP-first-then-archive sequence is
    /// walked.
    ArchiveEligible,
}

/// What a [`QueryEngine`] can serve. The router uses this to key its
/// internal map (`data_source_id`) and to estimate cost when several
/// compatible engines are registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineCapabilities {
    /// Stable identifier for the storage tier this engine answers from.
    /// Must equal `self.storage_backend().data_source_id()`. Pinned so the
    /// router can byte-compare and so dashboards parsing the wire response's
    /// `data_source: <id>` info-line stay in sync.
    pub data_source_id: &'static str,
    /// Which physical tier this engine owns. The router asks
    /// `compatible_storage_backends(...)` for an ordered list of
    /// `StorageBackend`s and looks each up via this field.
    pub storage_backend: StorageBackend,
    /// Memory budget (in bytes) the engine is willing to buffer for
    /// streaming-aggregate queries. Used by the cost-aware dispatcher
    /// when several engines are eligible for the same query (today it's
    /// purely informational; the Phase-6 cost model will consume it).
    pub supports_streams_above_bytes: usize,
}

/// The Phase-5 dispatch boundary. Concrete engines implement this trait so
/// the [`EngineRouter`] can hold them as `Arc<dyn QueryEngine>` rather
/// than case-on-concrete.
///
/// `execute` takes the query as `&str` (matching `GorillaQueryEngine`'s
/// existing surface) and returns a wire-ready [`QueryResult`]. Internal
/// engine signatures (e.g. `ASAPQueryEngine::handle_query`'s `Option<...>`)
/// are translated by the impl so callers can program against the trait.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Answer `query` against this engine's storage tier.
    async fn execute(&self, query: &str) -> Result<QueryResult, EngineError>;

    /// Forward a range query to this engine's storage tier.
    ///
    /// Params are milliseconds since epoch. The default returns `CapabilityMiss`
    /// so existing impls need not change; override when the engine has native
    /// range-query support (e.g. Thanos, or the ASAP-tier reducer).
    async fn execute_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<QueryResult, EngineError> {
        let _ = (query, start_ms, end_ms, step_ms);
        Err(EngineError::capability_miss(
            self.capabilities().data_source_id,
            "execute_range not implemented for this engine",
        ))
    }

    /// What this engine can serve. Cheap; the router calls it on every
    /// `register` and may re-call to refresh cost estimates.
    fn capabilities(&self) -> EngineCapabilities;
}

// ---------------------------------------------------------------------------
// `EngineRouter` — the dispatcher.
// ---------------------------------------------------------------------------

/// Errors surfaced by [`EngineRouter::execute`] when neither the head engine
/// nor any failover backend can answer the query.
#[derive(Debug, Error)]
pub enum EngineRouterError {
    /// The router has no engine registered for any of the compatible
    /// backends. This is a configuration bug — the deploy didn't
    /// register an engine for a tier the metric is supposed to use.
    #[error(
        "no engine registered for any compatible backend; \
         tried {tried:?}, registered={registered:?}"
    )]
    NoEngineRegistered {
        tried: Vec<StorageBackend>,
        registered: Vec<&'static str>,
    },

    /// The router walked the failover sequence and every engine errored
    /// or missed. Returns the *last* error so callers see the deepest
    /// failure (typically the cold-tier fallback's error, which is the
    /// most informative).
    #[error("all compatible engines failed; last error: {last}")]
    AllFailed { last: EngineError },
}

/// Dispatches PromQL queries to the engine that owns the chosen storage
/// tier. Built once at startup, cloned cheaply (the inner map holds
/// `Arc<dyn QueryEngine>` so engines are shared, not duplicated).
#[derive(Default, Clone)]
pub struct EngineRouter {
    /// Keyed by [`EngineCapabilities::data_source_id`] for O(1) lookup.
    engines: HashMap<&'static str, Arc<dyn QueryEngine>>,
}

impl EngineRouter {
    /// Build an empty router. Use [`Self::register`] to plug engines in.
    pub fn new() -> Self {
        Self {
            engines: HashMap::new(),
        }
    }

    /// Register an engine. The router keys it by
    /// `engine.capabilities().data_source_id`. If two engines claim the
    /// same `data_source_id` the later registration wins (matches
    /// `HashMap::insert` semantics; tests assume this for hot-swap).
    pub fn register(&mut self, engine: Arc<dyn QueryEngine>) {
        let caps = engine.capabilities();
        debug!(
            data_source_id = caps.data_source_id,
            backend = ?caps.storage_backend,
            "router: registering engine",
        );
        self.engines.insert(caps.data_source_id, engine);
    }

    /// Number of engines registered. Test-only convenience.
    pub fn len(&self) -> usize {
        self.engines.len()
    }

    /// Whether no engines are registered. Test-only convenience.
    pub fn is_empty(&self) -> bool {
        self.engines.is_empty()
    }

    /// Look up a registered engine by its `data_source_id`. Returns
    /// `None` when no engine has registered under that id.
    ///
    /// Used by the per-query engine override path in the HTTP layer
    /// (`X-ASAP-Engine` header / `?engine=` query param). Bypasses the
    /// capability matrix entirely — the caller has explicitly named the
    /// engine and accepts the consequences. Used by the accuracy
    /// reducer to query the same PromQL against the warm sketch and
    /// the Gorilla archive on MinIO so it can compute apples-to-apples
    /// relative error.
    pub fn engine_by_id(&self, data_source_id: &str) -> Option<&Arc<dyn QueryEngine>> {
        self.engines.get(data_source_id)
    }

    /// Iterate over the `data_source_id`s of every registered engine.
    /// Used by the HTTP layer to surface a useful error message when
    /// an explicit `X-ASAP-Engine` override names an engine that
    /// hasn't been registered.
    pub fn registered_ids(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.engines.keys().copied()
    }

    /// Walk the compatible-backend list for `(stat, accuracy, metric_storage)`,
    /// dispatch to the first registered engine, and (on
    /// [`EngineError::Backend`] or [`EngineError::CapabilityMiss`]) fall
    /// through to the next compatible backend.
    ///
    /// On exhaustion returns either:
    /// - [`EngineRouterError::NoEngineRegistered`] if zero of the
    ///   compatible backends had an engine registered, or
    /// - [`EngineRouterError::AllFailed`] if every registered engine in
    ///   the failover sequence returned an error.
    pub async fn execute(
        &self,
        query: &str,
        stat: Statistic,
        accuracy: AccuracyTarget,
        metric_storage: StorageBackend,
    ) -> Result<QueryResult, EngineRouterError> {
        let backends = compatible_storage_backends(stat, &accuracy, metric_storage);
        debug!(
            query = query,
            stat = ?stat,
            accuracy = ?accuracy,
            metric_storage = ?metric_storage,
            backends = ?backends,
            "router: dispatching",
        );

        let mut last_err: Option<EngineError> = None;
        let mut any_engine_tried = false;

        for backend in &backends {
            let id = backend.data_source_id();
            let Some(engine) = self.engines.get(id) else {
                debug!(
                    backend = ?backend,
                    data_source_id = id,
                    "router: no engine registered, trying next failover",
                );
                continue;
            };
            any_engine_tried = true;
            match engine.execute(query).await {
                Ok(result) => {
                    debug!(
                        backend = ?backend,
                        "router: dispatch succeeded",
                    );
                    return Ok(result);
                }
                Err(e) => {
                    warn!(
                        backend = ?backend,
                        error = %e,
                        "router: engine failed, falling through to next backend",
                    );
                    last_err = Some(e);
                }
            }
        }

        if !any_engine_tried {
            return Err(EngineRouterError::NoEngineRegistered {
                tried: backends,
                registered: self.engines.keys().copied().collect(),
            });
        }

        Err(EngineRouterError::AllFailed {
            last: last_err.expect("at least one engine ran (any_engine_tried=true)"),
        })
    }

    /// Range-query sibling of [`Self::execute`]. Walks the identical
    /// compatible-backend failover sequence for `(stat, accuracy,
    /// metric_storage)`, but dispatches to `engine.execute_range(query,
    /// start_ms, end_ms, step_ms)` instead of `engine.execute(query)`.
    ///
    /// The decision tree is centralized here (not in `http.rs`): the
    /// shared [`compatible_storage_backends`] policy table decides the
    /// order — `accuracy == Exact` yields archive-only
    /// `[GorillaObjectStore]`, otherwise the ASAP-first
    /// `[SketchStore, GorillaObjectStore]` sequence. On
    /// [`EngineError::CapabilityMiss`] or [`EngineError::Backend`] the
    /// router falls through to the next compatible backend; the
    /// `thanos_query` archive engine answers the matrix natively.
    ///
    /// On exhaustion returns the same [`EngineRouterError`] variants as
    /// [`Self::execute`].
    pub async fn execute_range(
        &self,
        query: &str,
        stat: Statistic,
        accuracy: AccuracyTarget,
        metric_storage: StorageBackend,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<QueryResult, EngineRouterError> {
        // Default tier policy preserves the pre-fix behaviour: walk the
        // full `compatible_storage_backends` sequence (ASAP-tier first,
        // archive failover). Callers that know the query's time range
        // relative to the warm-retention boundary should use
        // [`Self::execute_range_for_tier`] to suppress the archive leg
        // for warm-resident ranges (the warm-vs-archive routing fix).
        self.execute_range_for_tier(
            query,
            stat,
            accuracy,
            metric_storage,
            start_ms,
            end_ms,
            step_ms,
            RangeTier::ArchiveEligible,
        )
        .await
    }

    /// Time-aware range dispatch. Identical to [`Self::execute_range`]
    /// except the `tier` argument decides whether the archive
    /// (`GorillaObjectStore` / `thanos_query`) leg of the
    /// `compatible_storage_backends` sequence is eligible:
    ///
    /// * [`RangeTier::WarmOnly`] — the requested `[start_ms, end_ms]`
    ///   window lies entirely inside the warm-retention horizon, so the
    ///   data (if it exists at all) is warm-resident and the archive is
    ///   guaranteed empty for this range. The archive backend is dropped
    ///   from the failover list so a warm `CapabilityMiss` surfaces as a
    ///   real miss instead of being masked by an empty-but-`Ok` archive
    ///   answer stamped `data_source: thanos_query`. This is the root of
    ///   the recurring "No result" class for recent range queries when
    ///   cold/archive is ON.
    /// * [`RangeTier::ArchiveEligible`] — the window reaches at or past
    ///   the warm-retention boundary (genuinely-archived data, or a
    ///   range straddling the boundary). The full ASAP-first-then-archive
    ///   sequence is walked; the ASAP engine's hybrid-stitch path merges
    ///   warm ∪ archive where the ranges overlap.
    ///
    /// `data_source` stamping is unchanged: the engine that answers is
    /// the one whose `capabilities().data_source_id` the HTTP layer
    /// annotates onto the wire response.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_range_for_tier(
        &self,
        query: &str,
        stat: Statistic,
        accuracy: AccuracyTarget,
        metric_storage: StorageBackend,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        tier: RangeTier,
    ) -> Result<QueryResult, EngineRouterError> {
        let mut backends = compatible_storage_backends(stat, &accuracy, metric_storage);
        if matches!(tier, RangeTier::WarmOnly) {
            // Drop the archive leg: a range fully inside warm retention
            // must never be answered (empty) by the archive.
            backends.retain(|b| !matches!(b, StorageBackend::GorillaObjectStore));
        }
        debug!(
            query = query,
            stat = ?stat,
            accuracy = ?accuracy,
            metric_storage = ?metric_storage,
            tier = ?tier,
            backends = ?backends,
            start_ms,
            end_ms,
            step_ms,
            "router: dispatching range query",
        );

        let mut last_err: Option<EngineError> = None;
        let mut any_engine_tried = false;

        for backend in &backends {
            let id = backend.data_source_id();
            let Some(engine) = self.engines.get(id) else {
                debug!(
                    backend = ?backend,
                    data_source_id = id,
                    "router: no engine registered, trying next failover (range)",
                );
                continue;
            };
            any_engine_tried = true;
            match engine.execute_range(query, start_ms, end_ms, step_ms).await {
                Ok(result) => {
                    debug!(
                        backend = ?backend,
                        "router: range dispatch succeeded",
                    );
                    return Ok(result);
                }
                Err(e) => {
                    warn!(
                        backend = ?backend,
                        error = %e,
                        "router: engine failed on range query, falling through to next backend",
                    );
                    last_err = Some(e);
                }
            }
        }

        if !any_engine_tried {
            return Err(EngineRouterError::NoEngineRegistered {
                tried: backends,
                registered: self.engines.keys().copied().collect(),
            });
        }

        Err(EngineRouterError::AllFailed {
            last: last_err.expect("at least one engine ran (any_engine_tried=true)"),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engines::query_result::QueryResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Stub engine that records call counts and returns either a canned
    /// vector result or a configured error. Keeps the router tests
    /// hermetic — no ASAPQueryEngine / GorillaQueryEngine wire-up needed.
    struct StubEngine {
        caps: EngineCapabilities,
        calls: Arc<AtomicUsize>,
        outcome: Outcome,
    }

    enum Outcome {
        Ok,
        Backend,
        CapabilityMiss,
    }

    impl StubEngine {
        fn new(backend: StorageBackend, outcome: Outcome) -> (Arc<Self>, Arc<AtomicUsize>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let engine = Arc::new(Self {
                caps: EngineCapabilities {
                    data_source_id: backend.data_source_id(),
                    storage_backend: backend,
                    supports_streams_above_bytes: 1024 * 1024,
                },
                calls: calls.clone(),
                outcome,
            });
            (engine, calls)
        }
    }

    #[async_trait]
    impl QueryEngine for StubEngine {
        async fn execute(&self, _query: &str) -> Result<QueryResult, EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::Ok => Ok(QueryResult::vector(Vec::new(), 0)),
                Outcome::Backend => Err(EngineError::backend(
                    self.caps.data_source_id,
                    "simulated backend failure",
                )),
                Outcome::CapabilityMiss => Err(EngineError::capability_miss(
                    self.caps.data_source_id,
                    "no compatible aggregation",
                )),
            }
        }
        // Mirror `execute`'s outcome so the router's range failover loop
        // can be exercised hermetically (the trait default would always
        // CapabilityMiss, which wouldn't test Ok / Backend dispatch).
        async fn execute_range(
            &self,
            _query: &str,
            _start_ms: u64,
            _end_ms: u64,
            _step_ms: u64,
        ) -> Result<QueryResult, EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::Ok => Ok(QueryResult::matrix(Vec::new())),
                Outcome::Backend => Err(EngineError::backend(
                    self.caps.data_source_id,
                    "simulated backend failure (range)",
                )),
                Outcome::CapabilityMiss => Err(EngineError::capability_miss(
                    self.caps.data_source_id,
                    "no compatible aggregation (range)",
                )),
            }
        }
        fn capabilities(&self) -> EngineCapabilities {
            self.caps
        }
    }

    #[tokio::test]
    async fn router_dispatches_to_asap_tier_for_sketch_metrics() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            0,
            "archive must not run for a ASAP-tier-only metric (Step-1 deleted the JSONL fallback slot)",
        );
    }

    #[tokio::test]
    async fn router_dispatches_to_gorilla_for_archive_metrics() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(gorilla);

        let result = router
            .execute(
                "sum_over_time(audit_events[1h])",
                Statistic::Sum,
                AccuracyTarget::Exact,
                StorageBackend::GorillaObjectStore,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            0,
            "ASAP-tier must not run for an archive-only metric",
        );
    }

    #[tokio::test]
    async fn router_falls_back_to_archive_when_asap_misses_on_double_write() {
        // ASAP-first refactor: an `Approximate` double-write query
        // tries the ASAP-tier sketch first; on a CapabilityMiss the
        // router falls through to the Thanos archive (the
        // `[SketchStore, GorillaObjectStore]` failover sequence).
        let mut router = EngineRouter::new();
        let (warm, warm_calls) =
            StubEngine::new(StorageBackend::SketchStore, Outcome::CapabilityMiss);
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(gorilla);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::DoubleWrite,
            )
            .await;
        assert!(
            result.is_ok(),
            "router must reach the archive when the ASAP-tier head misses",
        );
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn router_with_no_engines_errors_cleanly() {
        let router = EngineRouter::new();
        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
            )
            .await;
        match result {
            Err(EngineRouterError::NoEngineRegistered { tried, registered }) => {
                // ASAP-first refactor: an `Approximate` query walks the
                // `[SketchStore, GorillaObjectStore]` failover sequence.
                assert_eq!(
                    tried,
                    vec![
                        StorageBackend::SketchStore,
                        StorageBackend::GorillaObjectStore,
                    ]
                );
                assert!(registered.is_empty());
            }
            other => panic!("expected NoEngineRegistered, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn router_returns_all_failed_when_every_engine_errors() {
        // After Step-1 deleted JSONL, the SketchStore failover
        // sequence is just `[SketchStore]`. A failing ASAP-tier
        // engine is the only error path on this metric.
        let mut router = EngineRouter::new();
        let (warm, _) = StubEngine::new(StorageBackend::SketchStore, Outcome::Backend);
        router.register(warm);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
            )
            .await;
        match result {
            Err(EngineRouterError::AllFailed { last }) => {
                assert!(matches!(last, EngineError::Backend { .. }));
            }
            other => panic!("expected AllFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn engine_by_id_returns_registered_engines_or_none() {
        let mut router = EngineRouter::new();
        let (warm, _) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(gorilla);

        // Hit by id — must return the engine for that backend.
        let archive = router
            .engine_by_id("thanos_query")
            .expect("thanos_query engine registered");
        let _ = archive.execute("count(foo)").await;
        assert_eq!(
            gorilla_calls.load(Ordering::SeqCst),
            1,
            "engine_by_id must return the engine that was registered under that id",
        );

        // Miss — unknown id returns None.
        assert!(router.engine_by_id("does_not_exist").is_none());

        // Iter exposes every registered id.
        let mut ids: Vec<&str> = router.registered_ids().collect();
        ids.sort();
        assert_eq!(ids, vec!["asap_query", "thanos_query"]);
    }

    #[tokio::test]
    async fn register_overwrites_same_data_source_id() {
        let mut router = EngineRouter::new();
        let (first, first_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (second, second_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        router.register(first);
        router.register(second);
        assert_eq!(
            router.len(),
            1,
            "two registrations under same id collapse to one"
        );

        let _ = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
            )
            .await;
        assert_eq!(first_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            second_calls.load(Ordering::SeqCst),
            1,
            "later registration wins"
        );
    }

    /// Phase ε.2: a routing-table entry with
    /// `metric_storage = StorageBackend::PrometheusRemote` must
    /// dispatch to the engine registered under `prometheus_remote`.
    /// Mirrors the gorilla-archive single-backend dispatch test;
    /// PrometheusRemote also has no failover (the metric's data
    /// never landed in ASAP-managed storage) so the failover
    /// sequence is just `[PrometheusRemote]`.
    #[tokio::test]
    async fn router_dispatches_to_prometheus_remote_for_mode3_metrics() {
        let mut router = EngineRouter::new();
        let (prom, prom_calls) = StubEngine::new(StorageBackend::PrometheusRemote, Outcome::Ok);
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        router.register(prom);
        router.register(warm);

        let result = router
            .execute(
                "rate(node_cpu_seconds_total[5m])",
                Statistic::Rate,
                AccuracyTarget::Exact,
                StorageBackend::PrometheusRemote,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(prom_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            0,
            "ASAP-tier must not run for a Mode 3 metric — Prometheus owns the storage",
        );
    }

    /// Phase ε.2: when `ASAP_PROMETHEUS_QUERY_URL` is unset the
    /// `prometheus_remote` engine is not registered, and a routing
    /// entry that targets it must surface a clear
    /// `NoEngineRegistered` error rather than silently falling
    /// through.
    #[tokio::test]
    async fn router_with_no_prometheus_engine_errors_for_mode3_metric() {
        let router = EngineRouter::new();
        let result = router
            .execute(
                "rate(node_cpu_seconds_total[5m])",
                Statistic::Rate,
                AccuracyTarget::Exact,
                StorageBackend::PrometheusRemote,
            )
            .await;
        match result {
            Err(EngineRouterError::NoEngineRegistered { tried, registered }) => {
                assert_eq!(tried, vec![StorageBackend::PrometheusRemote]);
                assert!(registered.is_empty());
            }
            other => panic!("expected NoEngineRegistered, got {other:?}"),
        }
    }

    // ── range-query dispatch (`EngineRouter::execute_range`) ────────────

    /// ASAP-first refactor: an `Approximate` range query tries the
    /// ASAP-tier sketch first and answers there when it can.
    #[tokio::test]
    async fn router_range_dispatches_to_asap_tier_first() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range(
                "rate(http_requests_total[1m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
                1_700_000_000_000,
                1_700_003_600_000,
                15_000,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            0,
            "archive must not run when the ASAP-tier answers the range query",
        );
    }

    /// ASAP-first refactor: on a CapabilityMiss in the ASAP tier the
    /// range query fails over to the Thanos archive.
    #[tokio::test]
    async fn router_range_falls_over_to_archive_on_capability_miss() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) =
            StubEngine::new(StorageBackend::SketchStore, Outcome::CapabilityMiss);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range(
                "rate(http_requests_total[1m])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::SketchStore,
                0,
                1_000,
                1_000,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            1,
            "range query must fail over to the archive on ASAP-tier CapabilityMiss",
        );
    }

    /// ASAP-first refactor: an `Exact` range query routes straight to
    /// the archive — the ASAP-tier sketch is never consulted.
    #[tokio::test]
    async fn router_range_exact_goes_straight_to_archive() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range(
                "audit_events",
                Statistic::Sum,
                AccuracyTarget::Exact,
                StorageBackend::DoubleWrite,
                0,
                1_000,
                1_000,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(archive_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            0,
            "Exact range query must skip the ASAP-tier and archive only",
        );
    }

    // ── warm-vs-archive time-boundary routing (`RangeTier`) ─────────────

    /// Regression test for the warm-vs-archive routing defect.
    ///
    /// With cold/archive ON, both `asap_query` (warm) and `thanos_query`
    /// (archive) engines are registered. A recent range query whose data
    /// is still warm-resident and NOT yet archived will, in the warm
    /// tier, capability-miss for shapes the warm tier can't serve (e.g.
    /// `sum_over_time` over counter deltas, issue #301). Pre-fix the
    /// router then failed over to the archive, which answers `Ok` with an
    /// EMPTY series for the recent range — the caller saw "No result"
    /// stamped `data_source: thanos_query`.
    ///
    /// Post-fix: when the HTTP layer determines the range is entirely
    /// within warm retention it passes `RangeTier::WarmOnly`, which drops
    /// the archive leg. The empty-archive masking can no longer happen —
    /// the warm miss surfaces as a real `AllFailed`/`NoEngineRegistered`
    /// (which the HTTP layer renders as an honest unsupported/no-result),
    /// and the archive engine is never consulted.
    #[tokio::test]
    async fn warm_only_tier_does_not_mask_warm_miss_with_empty_archive() {
        let mut router = EngineRouter::new();
        // Warm capability-misses (the recent-range #301 case), archive
        // would answer Ok (empty) if consulted.
        let (warm, warm_calls) =
            StubEngine::new(StorageBackend::SketchStore, Outcome::CapabilityMiss);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range_for_tier(
                "sum_over_time(http_requests_total[300s])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                // Cold metric resolves to the archive axis, but the range
                // is recent so the HTTP layer marks it WarmOnly.
                StorageBackend::GorillaObjectStore,
                1_700_000_000_000,
                1_700_000_300_000,
                15_000,
                RangeTier::WarmOnly,
            )
            .await;

        // Warm was tried; archive was NOT consulted (so no empty
        // `thanos_query` answer can mask the warm miss).
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            0,
            "WarmOnly tier must NOT consult the archive for a warm-resident range",
        );
        // The warm miss surfaces honestly rather than as a fake-success
        // empty archive vector.
        assert!(
            matches!(result, Err(EngineRouterError::AllFailed { .. })),
            "warm miss under WarmOnly must surface as AllFailed, not an empty archive Ok; got {result:?}",
        );
    }

    /// When the warm tier CAN serve a recent range, `WarmOnly` returns
    /// the warm answer and never touches the archive.
    #[tokio::test]
    async fn warm_only_tier_answers_from_warm_when_available() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchStore, Outcome::Ok);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range_for_tier(
                "quantile_over_time(0.99, latency_ms[300s])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::GorillaObjectStore,
                1_700_000_000_000,
                1_700_000_300_000,
                15_000,
                RangeTier::WarmOnly,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            0,
            "WarmOnly tier must answer from warm and skip the archive",
        );
    }

    /// An older / boundary-straddling range stays `ArchiveEligible`: warm
    /// is still tried first (cheap, and contributes the recent suffix via
    /// hybrid-stitch downstream) and on a warm miss the archive answers
    /// the genuinely-archived history. This preserves the cold-fallback
    /// arm the fix must not regress.
    #[tokio::test]
    async fn archive_eligible_tier_still_falls_over_to_archive_for_old_range() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) =
            StubEngine::new(StorageBackend::SketchStore, Outcome::CapabilityMiss);
        let (archive, archive_calls) =
            StubEngine::new(StorageBackend::GorillaObjectStore, Outcome::Ok);
        router.register(warm);
        router.register(archive);

        let result = router
            .execute_range_for_tier(
                "sum_over_time(http_requests_total[300s])",
                Statistic::Sum,
                AccuracyTarget::Epsilon(0.01),
                StorageBackend::GorillaObjectStore,
                1_600_000_000_000,
                1_600_000_300_000,
                15_000,
                RangeTier::ArchiveEligible,
            )
            .await;
        assert!(result.is_ok(), "old range must be served by the archive");
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            archive_calls.load(Ordering::SeqCst),
            1,
            "ArchiveEligible tier must fail over to the archive for an old range",
        );
    }
}
