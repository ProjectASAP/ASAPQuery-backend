use serde::{Deserialize, Serialize};

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
///
/// Formerly `asap_types::capability_matching::StorageBackend` (then
/// `asap_types::storage_backend::StorageBackend`). Moved here alongside
/// `StreamingConfig` (see `scratchpad/artifacts/enum-unification-plan.md`)
/// once auditing real call sites showed `control_plane` never actually
/// depends on this type or `StreamingConfig` — it emits wire-compatible
/// JSON by hand via its own `StreamingConfigEmitter`, never importing
/// either. See [`super::streaming_config`]'s module doc for the fuller
/// story. The routing *policy* (`AccuracyTarget`,
/// `compatible_storage_backends`) already lived in
/// `data_plane::query_engines::routing::capability_matching`.
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
