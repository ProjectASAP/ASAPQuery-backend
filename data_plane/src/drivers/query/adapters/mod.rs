pub mod config;
pub mod prometheus_http;
pub mod traits;
pub mod victoriametrics_http;

// Re-export main types
pub use config::AdapterConfig;
pub use prometheus_http::{PrometheusHttpAdapter, PrometheusResponse};
pub use traits::{
    AdapterError, HttpProtocolAdapter, ParsedQueryRequest, ParsedRangeQueryRequest,
    QueryExecutionResult,
};
pub use victoriametrics_http::VictoriaMetricsHttpAdapter;
