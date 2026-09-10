use control_plane::physical::post_asap::{bind_query_expr, PhysicalExpr};
use control_plane::types_v2::AccuracyTarget;
use planner_types::pre_asap::QueryExpr;
use thiserror::Error;

/// A MetricsQL request proven safe for the shared canonical runtime.
pub struct MetricsQlBinding {
    pub canonical: QueryExpr,
    pub physical: PhysicalExpr,
}

#[derive(Debug, Error)]
pub enum MetricsQlBindingError {
    #[error("MetricsQL expression is outside the accelerated PromQL-compatible subset: {0}")]
    Unsupported(String),
    #[error("canonical expression cannot be bound to a physical plan: {0}")]
    Physical(String),
}

/// Bind the PromQL-compatible subset of MetricsQL to the existing canonical
/// query and physical DAG. MetricsQL-only syntax deliberately returns
/// `Unsupported`, which routes the original request to VictoriaMetrics.
pub fn bind_metricsql(
    query: &str,
    accuracy: AccuracyTarget,
) -> Result<MetricsQlBinding, MetricsQlBindingError> {
    let canonical = asap_frontend_metricsql::lower_metricsql(query, accuracy.clone())
        .map_err(|error| MetricsQlBindingError::Unsupported(error.to_string()))?;
    control_plane::physical::compiler::validate_metricsql_acceleration_shape(&canonical)
        .map_err(|reason| MetricsQlBindingError::Unsupported(reason.into()))?;
    let physical = bind_query_expr(&canonical, accuracy)
        .map_err(|error| MetricsQlBindingError::Physical(error.to_string()))?;
    Ok(MetricsQlBinding {
        canonical,
        physical,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accuracy() -> AccuracyTarget {
        AccuracyTarget::Epsilon(0.01)
    }

    #[test]
    fn promql_compatible_metricsql_reaches_shared_planning() {
        bind_metricsql("sum(rate(http_requests_total[5m]))", accuracy())
            .expect("PromQL-compatible MetricsQL must bind");
    }

    #[test]
    fn explicit_metricsql_rollup_reaches_shared_planning() {
        bind_metricsql("default_rollup(cpu_usage[5m])", accuracy())
            .expect("explicit MetricsQL rollup must bind");
    }

    #[test]
    fn implicit_metricsql_rollup_fails_closed_without_runtime_step() {
        assert!(matches!(
            bind_metricsql("default_rollup(cpu_usage)", accuracy()),
            Err(MetricsQlBindingError::Unsupported(_))
        ));
    }

    #[test]
    fn metricsql_modifier_fails_closed() {
        assert!(matches!(
            bind_metricsql(
                "rate(http_requests_total[5m]) keep_metric_names",
                accuracy()
            ),
            Err(MetricsQlBindingError::Unsupported(_))
        ));
    }

    #[test]
    fn invalid_multi_argument_aggregate_fails_closed_without_dropping_arguments() {
        assert!(matches!(
            bind_metricsql("sum(foo, bar)", accuracy()),
            Err(MetricsQlBindingError::Unsupported(_))
        ));
    }

    #[test]
    fn empirically_non_equivalent_nested_rollups_fail_closed() {
        for query in ["sum(rate(foo[5s]))", "sum(increase(foo[5s]))"] {
            assert!(matches!(
                bind_metricsql(query, accuracy()),
                Err(MetricsQlBindingError::Unsupported(_))
            ));
        }
        bind_metricsql("sum(sum_over_time(foo[5s]))", accuracy())
            .expect("nested sum rollup is represented exactly after complete ingestion");
    }
}
