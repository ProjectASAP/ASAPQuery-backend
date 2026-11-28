//! A plan for the other crates' unit tests (the `test-support` feature,
//! which they enable as a dev-dependency only).

use asap_planner::InstallablePlan;

use crate::DeploymentInputs;

/// The plan of `query`, evaluated every 10 s over the gauge `data` (one
/// series, one sample a second), for a deployment that keeps raw data or
/// not.
pub fn one_query_plan(query: &str, raw_data_retained: bool) -> InstallablePlan {
    let workload = serde_json::from_value(serde_json::json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": query,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 1.0}},
          "sources": {"data": {"series_count": 1, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .expect("a valid workload");
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    crate::plan_workload(&workload, &inputs)
        .expect("the workload plans")
        .0
}
