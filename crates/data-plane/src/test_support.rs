//! Plans and options for the unit tests.

use common::InstallablePlan;
use precompute::ingestion::IngestionOptions;
use storage::store::RawSample;

use crate::engine::EngineOptions;

/// The plan of `query`, evaluated every 10 s over the gauge `data` (one
/// sample a second), for a deployment that keeps raw data or not.
pub(crate) fn plan(query: &str, raw_data_retained: bool) -> InstallablePlan {
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
    .unwrap();
    let mut inputs = control_plane::DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    control_plane::plan_workload(&workload, &inputs).unwrap().0
}

/// Options that refresh every window: no allowed lateness, and the stream
/// starting at 0.
pub(crate) fn options() -> EngineOptions {
    EngineOptions {
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// One sample of `data{job="a"}` with `value` every second of `(0, end_ms]`.
pub(crate) fn samples(end_ms: i64, value: f64) -> Vec<RawSample> {
    let series = storage::series_ids::intern_pairs(&[("__name__", "data"), ("job", "a")]);
    (1..=end_ms / 1000)
        .map(|second| RawSample {
            labels: series,
            timestamp_ms: second * 1000,
            value,
        })
        .collect()
}
