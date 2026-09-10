//! VictoriaMetrics-specific MetricsQL binding boundary.

pub mod metricsql_binder;

pub use metricsql_binder::{bind_metricsql, MetricsQlBinding, MetricsQlBindingError};
