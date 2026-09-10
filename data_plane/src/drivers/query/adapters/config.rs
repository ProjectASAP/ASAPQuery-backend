use crate::drivers::query::fallback::FallbackClient;
use crate::storage_engines::types::enums::{QueryLanguage, QueryProtocol};
use std::sync::Arc;

/// Configuration for a specific protocol adapter
#[derive(Clone)]
pub struct AdapterConfig {
    /// The query protocol to use
    pub protocol: QueryProtocol,

    /// The query language to use
    pub language: QueryLanguage,

    /// Optional fallback client for unsupported queries
    pub fallback: Option<Arc<dyn FallbackClient>>,
}

impl std::fmt::Debug for AdapterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterConfig")
            .field("protocol", &self.protocol)
            .field("language", &self.language)
            .field(
                "fallback",
                &self.fallback.as_ref().map(|_| "Some(FallbackClient)"),
            )
            .finish()
    }
}

impl AdapterConfig {
    /// Generic constructor for adapter configuration
    pub fn new(
        protocol: QueryProtocol,
        language: QueryLanguage,
        fallback: Option<Arc<dyn FallbackClient>>,
    ) -> Self {
        Self {
            protocol,
            language,
            fallback,
        }
    }

    /// Create a configuration for Prometheus HTTP with PromQL
    /// Convenience constructor for backward compatibility
    pub fn prometheus_promql(fallback_url: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::PrometheusHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(PrometheusHttpFallback::new(fallback_url)) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::PrometheusHttp,
            QueryLanguage::promql,
            fallback,
        )
    }

    /// Configuration used by the independent VictoriaMetrics listener. The
    /// wire protocol is Prometheus-compatible; MetricsQL binding is handled at
    /// the query-language boundary.
    pub fn victoriametrics_metricsql(fallback_url: String) -> Self {
        use crate::drivers::query::fallback::VictoriaMetricsHttpFallback;
        Self::new(
            QueryProtocol::PrometheusHttp,
            QueryLanguage::promql,
            Some(Arc::new(VictoriaMetricsHttpFallback::new(fallback_url))),
        )
    }
}
