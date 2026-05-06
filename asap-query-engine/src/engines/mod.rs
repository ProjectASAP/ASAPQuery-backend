pub mod gorilla_engine;
pub mod logical;
pub mod physical;
pub mod query_result;
pub mod router;
pub mod simple_engine;
pub mod timeline_dispatch;
pub mod window_merger;

pub use gorilla_engine::{
    EngineError as GorillaEngineError, GorillaEngineConfig, GorillaQueryEngine,
};
pub use query_result::{InstantVector, QueryResult, RangeVector, RangeVectorElement, Sample};
pub use router::{EngineCapabilities, EngineRouter, EngineRouterError, QueryEngine};
pub use simple_engine::SimpleEngine;
pub use timeline_dispatch::{combine_statistic, CombinedResult};
pub use window_merger::{create_window_merger, NaiveMerger, WindowMerger};

// ---------------------------------------------------------------------------
// Phase-5: shared `EngineError` surface returned by the `QueryEngine` trait.
//
// The trait is engine-agnostic, so its `execute` must return an error type
// that can wrap *any* concrete engine's failure mode. Today's two engines
// — `SimpleEngine` (capability-miss → `None`) and `GorillaQueryEngine`
// (rich `EngineError`) — fold into this common envelope. New engines
// can plug in by adding a `Backend(String)` arm or a typed conversion.
// ---------------------------------------------------------------------------

use thiserror::Error;

/// Top-level error returned by any [`QueryEngine`] impl.
///
/// Concrete engines convert their internal error types into this envelope
/// via the conversions in this file (or via `?` for `GorillaEngineError`).
/// The router uses the variant to decide whether a failover is sensible
/// (e.g. `Backend(_)` falls through to the next compatible backend;
/// `CapabilityMiss` does not — the caller should escalate).
#[derive(Debug, Error)]
pub enum EngineError {
    /// The engine has no aggregation that can answer this query. Mirrors
    /// `SimpleEngine::handle_query` returning `None`. The router treats
    /// this as a "hard miss" and falls through to the next backend in the
    /// `compatible_storage_backends` list (typically `ColdJsonlFallback`).
    #[error("no compatible aggregation in {engine_id}: {detail}")]
    CapabilityMiss {
        engine_id: &'static str,
        detail: String,
    },

    /// The engine's backend (cold-store, S3, planner, …) failed during
    /// execution. Wraps the underlying engine's error as a string so the
    /// router doesn't take a hard dep on every engine's error type.
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
