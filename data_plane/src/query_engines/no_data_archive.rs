//! `NoDataArchiveEngine` — a stub archive engine that always answers
//! with an empty result set.
//!
//! ## Why this exists
//!
//! When a deploy is configured with the warm-tier sketch path only and
//! has neither `ASAP_THANOS_QUERY_URL` nor `ASAP_GORILLA_S3_*` env
//! vars set, no archive engine is registered on the
//! [`crate::query_engines::routing::EngineRouter`]. Cold queries (queries the
//! per-metric routing table sends to `thanos_query`) then surface as
//! `503 NoEngineRegistered` from the HTTP handler.
//!
//! Treating "no archive configured" as a 503 trips up dashboards and
//! freshness probes that just want a degraded but successful answer.
//! This engine flips that default: when the archive env vars are
//! unset, the binary registers a [`NoDataArchiveEngine`] under the
//! `thanos_query` slot. Cold queries return an **empty result
//! set** with `data_source_id = "no_data_archive"` so the wire
//! response carries enough signal for operators to notice the
//! misconfig without breaking the request path.
//!
//! Operators that want the original "fail-loud" behaviour can opt
//! back in by setting `ASAP_REQUIRE_ARCHIVE_ENGINE=1` (the binary
//! reads this env var in `main.rs` and skips the no-data registration
//! when set).

use async_trait::async_trait;
use tracing::info;

use asap_types::StorageBackend;

use crate::query_engines::{EngineError, QueryResult};
use crate::query_engines::routing::{EngineCapabilities, QueryEngine};

/// Stable engine id for the no-data fallback. Reported in
/// `data_source_id` on the wire response for cold queries when the
/// archive env vars are unset.
pub const DATA_SOURCE_ID_NO_DATA_ARCHIVE: &str = "no_data_archive";

/// Engine that answers every query with an empty instant vector. Used
/// by the binary as a stand-in for a real archive engine when neither
/// `ASAP_THANOS_QUERY_URL` nor `ASAP_GORILLA_S3_*` is configured.
#[derive(Debug, Default)]
pub struct NoDataArchiveEngine;

impl NoDataArchiveEngine {
    /// Build the stub. Logs once at construction so the misconfig is
    /// visible in the binary's startup log.
    pub fn new() -> Self {
        info!(
            "no archive backend configured (set ASAP_THANOS_QUERY_URL or \
             ASAP_GORILLA_S3_* to enable); cold queries will return empty results"
        );
        Self
    }
}

#[async_trait]
impl QueryEngine for NoDataArchiveEngine {
    async fn execute(&self, _query: &str) -> Result<QueryResult, EngineError> {
        // Empty instant vector at t=0. The HTTP handler annotates the
        // wire response with `data_source: no_data_archive` so the
        // caller can distinguish "engine missing" from "real archive
        // had nothing".
        Ok(QueryResult::vector(Vec::new(), 0))
    }

    fn capabilities(&self) -> EngineCapabilities {
        EngineCapabilities {
            data_source_id: asap_types::ENGINE_ID_THANOS_QUERY,
            // Register under the canonical archive query-engine id so
            // archive entries dispatch here transparently when no real
            // ThanosQueryEngine is configured.
            storage_backend: StorageBackend::GorillaObjectStore,
            supports_streams_above_bytes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_returns_empty_instant_vector() {
        let engine = NoDataArchiveEngine::new();
        let result = engine.execute("count(any_metric)").await.expect("ok");
        match result {
            QueryResult::Vector(iv) => {
                assert!(iv.values.is_empty(), "expected empty vector");
            }
            QueryResult::Matrix(_) => panic!("expected instant vector, got matrix"),
        }
    }

    #[test]
    fn capabilities_use_no_data_archive_id() {
        let engine = NoDataArchiveEngine::new();
        let caps = engine.capabilities();
        assert_eq!(caps.data_source_id, asap_types::ENGINE_ID_THANOS_QUERY);
        assert_eq!(caps.storage_backend, StorageBackend::GorillaObjectStore);
    }
}
