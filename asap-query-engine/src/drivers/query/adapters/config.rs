use crate::data_model::enums::{QueryLanguage, QueryProtocol};
use crate::drivers::query::fallback::FallbackClient;
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

    /// Prometheus + cold-tier fallback chain (§5.2 of the sketch-DB design).
    ///
    /// Composes a [`ColdFallback`](crate::drivers::query::fallback::ColdFallback)
    /// in front of a [`PrometheusHttpFallback`](crate::drivers::query::fallback::PrometheusHttpFallback)
    /// so capability-misses first try the raw cold tier (exact
    /// answers for supported query shapes) and only hit the live
    /// Prometheus if the cold adapter can't handle the shape.
    ///
    /// * `cold_root` — local-FS root that mirrors the S3 key
    ///   layout documented in
    ///   [`cold_store::format`](crate::drivers::query::fallback::cold_store::format).
    ///   Swap in an S3-backed [`ColdStore`](crate::drivers::query::fallback::ColdStore)
    ///   impl later without touching this config.
    /// * `prom_fallback_url` — upstream Prometheus used for the
    ///   tail of the fallback chain; set to `None` to short-circuit
    ///   unsupported shapes with an empty vector instead of
    ///   forwarding.
    pub fn prometheus_promql_with_cold(
        cold_root: std::path::PathBuf,
        prom_fallback_url: Option<String>,
    ) -> Self {
        use crate::drivers::query::fallback::{
            ColdFallback, LocalFsColdStore, PrometheusHttpFallback,
        };

        let cold_store = Arc::new(LocalFsColdStore::new(cold_root));
        let cold = ColdFallback::new(cold_store);
        let cold: Arc<dyn FallbackClient> = match prom_fallback_url {
            Some(url) => {
                let prom: Arc<dyn FallbackClient> = Arc::new(PrometheusHttpFallback::new(url));
                Arc::new(cold.with_inner(prom))
            }
            None => Arc::new(cold),
        };

        Self::new(
            QueryProtocol::PrometheusHttp,
            QueryLanguage::promql,
            Some(cold),
        )
    }

    /// Pick between [`Self::prometheus_promql`] and
    /// [`Self::prometheus_promql_with_cold`] based on whether the
    /// caller has a cold-store root configured (`--cold-store-root`
    /// CLI flag or `ASAP_COLD_STORE_ROOT` env var).
    ///
    /// Wired into both binaries that face deployment:
    /// `query_engine_rust` (`src/main.rs`) and `precompute_engine`
    /// (`src/bin/precompute_engine.rs`). Centralised here so the
    /// behaviour matrix only lives in one place.
    pub fn from_prom_with_optional_cold(
        prometheus_server: String,
        forward_unsupported: bool,
        cold_store_root: Option<&std::path::Path>,
    ) -> Self {
        match cold_store_root {
            Some(root) => {
                let prom = if forward_unsupported {
                    Some(prometheus_server)
                } else {
                    None
                };
                Self::prometheus_promql_with_cold(root.to_path_buf(), prom)
            }
            None => Self::prometheus_promql(prometheus_server, forward_unsupported),
        }
    }

    /// Create a configuration for ClickHouse HTTP with SQL
    /// Convenience constructor for ClickHouse adapter
    pub fn clickhouse_sql(base_url: String, database: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ClickHouseHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ClickHouseHttpFallback::new(base_url, database))
                as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(QueryProtocol::ClickHouseHttp, QueryLanguage::sql, fallback)
    }

    /// Create a configuration for Elasticsearch HTTP with Elasticsearch QueryDSL
    /// Convenience constructor for Elasticsearch adapter
    pub fn elastic_querydsl(base_url: String, index: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ElasticHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ElasticHttpFallback::new(
                base_url,
                index,
                QueryLanguage::elastic_querydsl,
            )) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_querydsl,
            fallback,
        )
    }

    /// Create a configuration for Elasticsearch HTTP with SQL
    /// Convenience constructor for Elasticsearch SQL adapter
    pub fn elastic_sql(base_url: String, index: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ElasticHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ElasticHttpFallback::new(
                base_url,
                index,
                QueryLanguage::elastic_sql,
            )) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_sql,
            fallback,
        )
    }
}
