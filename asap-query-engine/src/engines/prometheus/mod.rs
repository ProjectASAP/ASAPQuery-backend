//! Prometheus-remote query engine — HTTP-forwarder to a Prometheus
//! `/api/v1/query` endpoint.
//!
//! Phase ε.2 of the planner consolidation registers a
//! [`forward::PrometheusForwardEngine`] under the `prometheus_remote`
//! engine id so the controller's Mode 3
//! (`RawAtEdgePrometheusArchive`) routing can target Prometheus
//! directly. The engine is conditional on the
//! [`forward::ASAP_PROMETHEUS_QUERY_URL_ENV`] env var; when unset,
//! the forwarder is not registered and a routing-table entry that
//! references `prometheus_remote` surfaces a `NoEngineRegistered`
//! 503 from the HTTP handler (the correct fail-loud behaviour for a
//! misconfigured deploy).
//!
//! Sibling of [`crate::engines::thanos_query::forward`] (the
//! Step-2.3 archive forwarder); the two engines coexist in the
//! router under different ids and answer different routing-table
//! entries.

pub mod forward;

pub use forward::{
    engine_from_env as prometheus_engine_from_env, PrometheusForwardConfig,
    PrometheusForwardEngine, PrometheusForwardError, ASAP_PROMETHEUS_QUERY_URL_ENV,
    DATA_SOURCE_PROMETHEUS_REMOTE_ID, DATA_SOURCE_PROMETHEUS_REMOTE_INFO,
    DEFAULT_PROMETHEUS_QUERY_URL, QUIRK_PROMETHEUS_UNREACHABLE,
};
