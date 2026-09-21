use crate::drivers::query::fallback::FallbackClient;
use crate::query_engines::QueryForwardingPolicy;
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

    /// Whether this adapter may issue query requests to its fallback backend.
    pub query_forwarding_policy: QueryForwardingPolicy,

    /// A fallback was configured but removed by the forwarding policy.
    pub fallback_blocked_by_policy: bool,
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
            .field("query_forwarding_policy", &self.query_forwarding_policy)
            .field(
                "fallback_blocked_by_policy",
                &self.fallback_blocked_by_policy,
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
            query_forwarding_policy: QueryForwardingPolicy::Enabled,
            fallback_blocked_by_policy: false,
        }
    }

    pub fn with_query_forwarding_policy(mut self, policy: QueryForwardingPolicy) -> Self {
        self.query_forwarding_policy = policy;
        if !policy.allows_external_queries() {
            self.fallback_blocked_by_policy |= self.fallback.is_some();
            self.fallback = None;
        } else {
            self.fallback_blocked_by_policy = false;
        }
        self
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
            QueryLanguage::PromQl,
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
            QueryLanguage::MetricsQl,
            Some(Arc::new(VictoriaMetricsHttpFallback::new(fallback_url))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listener_factories_preserve_their_query_language() {
        assert_eq!(
            AdapterConfig::prometheus_promql(String::new(), false).language,
            QueryLanguage::PromQl
        );
        assert_eq!(
            AdapterConfig::victoriametrics_metricsql(String::new()).language,
            QueryLanguage::MetricsQl
        );
    }

    #[test]
    fn disabled_query_forwarding_removes_the_fallback_client() {
        let config = AdapterConfig::prometheus_promql("http://prom:9090".into(), true)
            .with_query_forwarding_policy(QueryForwardingPolicy::Disabled);
        assert!(config.fallback.is_none());
        assert!(config.fallback_blocked_by_policy);
        assert_eq!(
            config.query_forwarding_policy,
            QueryForwardingPolicy::Disabled
        );
    }

    #[test]
    fn disabling_without_a_fallback_does_not_claim_a_blocked_request() {
        let config = AdapterConfig::prometheus_promql("http://prom:9090".into(), false)
            .with_query_forwarding_policy(QueryForwardingPolicy::Disabled);
        assert!(!config.fallback_blocked_by_policy);
    }
}
