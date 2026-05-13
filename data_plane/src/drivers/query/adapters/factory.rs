use super::config::AdapterConfig;
use super::prometheus_http::PrometheusHttpAdapter;
use super::traits::HttpProtocolAdapter;
use crate::storage_engines::types::enums::QueryProtocol;
use std::sync::Arc;

/// Factory function to create appropriate HTTP adapter based on protocol
pub fn create_http_adapter(config: AdapterConfig) -> Arc<dyn HttpProtocolAdapter> {
    match config.protocol {
        QueryProtocol::PrometheusHttp => Arc::new(PrometheusHttpAdapter::new(config)),
    }
}
