use serde::{Deserialize, Serialize};

pub const ENGINE_ID_ASAP_QUERY: &str = "asap_query";
pub const ENGINE_ID_THANOS_QUERY: &str = "thanos_query";
pub const CANONICAL_QUERY_ENGINE_IDS: &[&str] = &[ENGINE_ID_ASAP_QUERY, ENGINE_ID_THANOS_QUERY];

/// Backend-owned physical storage target shared by both runtime planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    #[default]
    SketchStore,
    GorillaObjectStore,
    DoubleWrite,
    PrometheusRemote,
}

impl StorageBackend {
    pub const fn data_source_id(self) -> &'static str {
        match self {
            Self::SketchStore => ENGINE_ID_ASAP_QUERY,
            Self::GorillaObjectStore => ENGINE_ID_THANOS_QUERY,
            Self::DoubleWrite => "double_write",
            Self::PrometheusRemote => "prometheus_remote",
        }
    }
}

pub fn parse_storage_backend_engine_id(value: &str) -> Option<StorageBackend> {
    match value {
        ENGINE_ID_ASAP_QUERY => Some(StorageBackend::SketchStore),
        ENGINE_ID_THANOS_QUERY => Some(StorageBackend::GorillaObjectStore),
        "double_write" => Some(StorageBackend::DoubleWrite),
        "prometheus_remote" => Some(StorageBackend::PrometheusRemote),
        _ => None,
    }
}
