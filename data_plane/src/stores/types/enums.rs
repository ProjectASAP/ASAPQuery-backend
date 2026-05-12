pub use asap_types::enums::{CleanupPolicy, QueryLanguage, WindowType};
pub use promql_utilities::query_logics::enums::AggregationType;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum QueryProtocol {
    #[value(alias = "PROMETHEUS_HTTP")]
    PrometheusHttp,
}

#[derive(clap::ValueEnum, Clone, Debug, Copy, PartialEq)]
pub enum LockStrategy {
    #[value(name = "global")]
    Global,
    #[value(name = "per-key")]
    PerKey,
}
