//! Query engines.
//!
//! The public query-engine surface is intentionally small:
//! [`asap_query_engine`] answers from ASAP's sketch store, and
//! [`thanos_query_engine`] forwards exact/archive queries to `thanos-query`.
//! Gorilla object storage lives under [`crate::stores::gorilla_object_store`]
//! because it is a storage implementation detail, not a public query-engine
//! family.
//!
//! ## Public surface
//!
//! Engines + the shared error envelope:
//!
//! * [`asap_query_engine::ASAPQueryEngine`] — warm-tier sketch query
//!   engine.
//! * [`thanos_query_engine::ThanosQueryEngine`] — archive-tier query
//!   engine.
//! * [`prometheus_query_engine::PrometheusForwardEngine`] — HTTP-forwarder
//!   to a Prometheus `/api/v1/query` endpoint, registered under the
//!   `prometheus_remote` engine id when
//!   `ASAP_PROMETHEUS_QUERY_URL` is set (Phase ε.2).
//! * [`EngineError`] — the trait-level error envelope every
//!   `crate::query_engines::routing::QueryEngine` impl returns.

pub mod asap_query_engine;
pub mod no_data_archive;
pub mod prometheus_query_engine;
pub mod query_result;
pub mod routing;
pub mod thanos_query_engine;
pub mod timeline_dispatch;
pub mod warm_tier;
pub mod window_merger;

pub use asap_query_engine::ASAPQueryEngine;
pub use no_data_archive::{NoDataArchiveEngine, DATA_SOURCE_ID_NO_DATA_ARCHIVE};
pub use prometheus_query_engine::{
    PrometheusForwardConfig, PrometheusForwardEngine, PrometheusForwardError,
};
pub use query_result::{InstantVector, QueryResult, RangeVector, RangeVectorElement, Sample};
pub use thanos_query_engine::{
    thanos_engine_from_env, ThanosQueryConfig, ThanosQueryEngine, ThanosQueryError,
    ASAP_THANOS_QUERY_URL_ENV, DATA_SOURCE_THANOS_QUERY_ID, DATA_SOURCE_THANOS_QUERY_INFO,
    DEFAULT_THANOS_QUERY_URL, QUIRK_THANOS_UNREACHABLE,
};
pub use timeline_dispatch::{combine_statistic, CombinedResult};
pub use window_merger::{create_window_merger, NaiveMerger, WindowMerger};

// ---------------------------------------------------------------------------
// Shared `EngineError` surface returned by the `QueryEngine` trait.
//
// The trait is engine-agnostic, so its `execute` must return an error type
// that can wrap *any* concrete engine's failure mode. Today's two engines
// — `ASAPQueryEngine` (capability-miss → `None`) and archive/query forwarders
// (rich `EngineError`) — fold into this common envelope.
// ---------------------------------------------------------------------------

use thiserror::Error;

/// Top-level error returned by any [`crate::query_engines::routing::QueryEngine`] impl.
///
/// Concrete engines convert their internal error types into this envelope.
/// The router uses the variant to decide whether a failover is sensible
/// (e.g. `Backend(_)` falls through to the next compatible backend;
/// `CapabilityMiss` does not — the caller should escalate).
#[derive(Debug, Error)]
pub enum EngineError {
    /// The engine has no aggregation that can answer this query. Mirrors
    /// `ASAPQueryEngine::handle_query` returning `None`. The router treats
    /// this as a "hard miss" and falls through to the next backend in
    /// the `compatible_storage_backends` list. After Step-1 of the
    /// JSONL deprecation, the surviving failovers are warm-tier
    /// sketch ↔ Gorilla-S3 archive only (the cold JSONL leg was
    /// deleted).
    #[error("no compatible aggregation in {engine_id}: {detail}")]
    CapabilityMiss {
        engine_id: &'static str,
        detail: String,
    },

    /// The engine's backend (archive store, S3, planner, …) failed
    /// during execution. Wraps the underlying engine's error as a
    /// string so the router doesn't take a hard dep on every engine's
    /// error type.
    #[error("backend failure in {engine_id}: {message}")]
    Backend {
        engine_id: &'static str,
        message: String,
    },
}

impl EngineError {
    pub fn capability_miss(engine_id: &'static str, detail: impl Into<String>) -> Self {
        Self::CapabilityMiss {
            engine_id,
            detail: detail.into(),
        }
    }

    pub fn backend(engine_id: &'static str, source: impl ToString) -> Self {
        Self::Backend {
            engine_id,
            message: source.to_string(),
        }
    }
}
