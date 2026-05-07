//! Phase-5 capability router: dispatches a `(query, metric_storage)` pair
//! to the engine that owns the chosen storage tier.
//!
//! The router holds a small map keyed by
//! [`asap_types::StorageBackend::data_source_id`]
//! and walks the ordered backend list returned by
//! [`asap_types::compatible_storage_backends`]. The first registered
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

use asap_types::{compatible_storage_backends, AccuracyTarget, StorageBackend};
use promql_utilities::query_logics::enums::Statistic;

use super::{EngineError, QueryResult};

// ---------------------------------------------------------------------------
// `QueryEngine` trait — the abstraction the router holds.
//
// The trait is intentionally narrow: a single `execute(&str)` method (so it
// integrates with both `SimpleEngine::handle_query` and
// `GorillaQueryEngine::execute` without forcing either side to refactor its
// public surface), plus a `capabilities()` accessor the router consults at
// registration time.
// ---------------------------------------------------------------------------

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
/// engine signatures (e.g. `SimpleEngine::handle_query`'s `Option<...>`)
/// are translated by the impl so callers can program against the trait.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Answer `query` against this engine's storage tier.
    async fn execute(&self, query: &str) -> Result<QueryResult, EngineError>;

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
        let backends = compatible_storage_backends(stat, accuracy, metric_storage);
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engines::query_result::QueryResult;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Stub engine that records call counts and returns either a canned
    /// vector result or a configured error. Keeps the router tests
    /// hermetic — no SimpleEngine / GorillaQueryEngine wire-up needed.
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
        fn capabilities(&self) -> EngineCapabilities {
            self.caps
        }
    }

    #[tokio::test]
    async fn router_dispatches_to_warm_tier_for_sketch_metrics() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Ok);
        let (jsonl, jsonl_calls) =
            StubEngine::new(StorageBackend::ColdJsonlFallback, Outcome::Ok);
        router.register(warm);
        router.register(jsonl);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Approximate,
                StorageBackend::SketchWarmTier,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(jsonl_calls.load(Ordering::SeqCst), 0, "JSONL must not run when warm-tier succeeds");
    }

    #[tokio::test]
    async fn router_dispatches_to_gorilla_for_archive_metrics() {
        let mut router = EngineRouter::new();
        let (warm, warm_calls) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Ok);
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaS3Archive, Outcome::Ok);
        router.register(warm);
        router.register(gorilla);

        let result = router
            .execute(
                "sum_over_time(audit_events[1h])",
                Statistic::Sum,
                AccuracyTarget::Exact,
                StorageBackend::GorillaS3Archive,
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            warm_calls.load(Ordering::SeqCst),
            0,
            "warm-tier must not run for an archive-only metric",
        );
    }

    #[tokio::test]
    async fn router_falls_back_to_jsonl_when_archive_fails() {
        // Double-write deploy: archive fails, warm-tier fails too, JSONL answers.
        let mut router = EngineRouter::new();
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaS3Archive, Outcome::Backend);
        let (warm, warm_calls) =
            StubEngine::new(StorageBackend::SketchWarmTier, Outcome::CapabilityMiss);
        let (jsonl, jsonl_calls) =
            StubEngine::new(StorageBackend::ColdJsonlFallback, Outcome::Ok);
        router.register(gorilla);
        router.register(warm);
        router.register(jsonl);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Exact,
                StorageBackend::DoubleWrite,
            )
            .await;
        assert!(result.is_ok(), "router must reach JSONL on archive+warm failure");
        assert_eq!(gorilla_calls.load(Ordering::SeqCst), 1);
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);
        assert_eq!(jsonl_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn router_with_no_engines_errors_cleanly() {
        let router = EngineRouter::new();
        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Approximate,
                StorageBackend::SketchWarmTier,
            )
            .await;
        match result {
            Err(EngineRouterError::NoEngineRegistered { tried, registered }) => {
                assert_eq!(
                    tried,
                    vec![
                        StorageBackend::SketchWarmTier,
                        StorageBackend::ColdJsonlFallback,
                    ]
                );
                assert!(registered.is_empty());
            }
            other => panic!("expected NoEngineRegistered, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn router_returns_all_failed_when_every_engine_errors() {
        let mut router = EngineRouter::new();
        let (warm, _) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Backend);
        let (jsonl, _) = StubEngine::new(StorageBackend::ColdJsonlFallback, Outcome::Backend);
        router.register(warm);
        router.register(jsonl);

        let result = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Approximate,
                StorageBackend::SketchWarmTier,
            )
            .await;
        match result {
            Err(EngineRouterError::AllFailed { last }) => {
                // The deepest failure (JSONL) is what the caller sees.
                assert!(matches!(last, EngineError::Backend { .. }));
            }
            other => panic!("expected AllFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn engine_by_id_returns_registered_engines_or_none() {
        let mut router = EngineRouter::new();
        let (warm, _) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Ok);
        let (gorilla, gorilla_calls) =
            StubEngine::new(StorageBackend::GorillaS3Archive, Outcome::Ok);
        router.register(warm);
        router.register(gorilla);

        // Hit by id — must return the engine for that backend.
        let archive = router
            .engine_by_id("gorilla_archive")
            .expect("gorilla_archive engine registered");
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
        assert_eq!(ids, vec!["gorilla_archive", "sketch_warm"]);
    }

    #[tokio::test]
    async fn register_overwrites_same_data_source_id() {
        let mut router = EngineRouter::new();
        let (first, first_calls) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Ok);
        let (second, second_calls) = StubEngine::new(StorageBackend::SketchWarmTier, Outcome::Ok);
        router.register(first);
        router.register(second);
        assert_eq!(router.len(), 1, "two registrations under same id collapse to one");

        let _ = router
            .execute(
                "sum_over_time(foo[5m])",
                Statistic::Sum,
                AccuracyTarget::Approximate,
                StorageBackend::SketchWarmTier,
            )
            .await;
        assert_eq!(first_calls.load(Ordering::SeqCst), 0);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1, "later registration wins");
    }
}
