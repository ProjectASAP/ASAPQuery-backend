//! Statistics-consumer contracts. Controlled observations are not live telemetry.
use control_plane::physical::{compiler::BackendLocalPlanningInput, erp::ErpShapeObserver};
use serde_json::{json, Value};

fn wire() -> Value {
    let mut wire: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-planning-snapshot.json"
    ))
    .unwrap();
    wire["query_workload"]["repeating_queries"][0]["query"] = json!("sum_over_time(m[1m])");
    wire["data_workload"]["ingestion_rate"] = json!({
        "value":25.0,"source":"observed","observed_at_ms":9500,"valid_for_ms":1000
    });
    wire["data_workload"]["input_cardinality"] = json!({
        "value":125,"source":"observed","observed_at_ms":9500,"valid_for_ms":1000
    });
    wire
}

/// Source rate, source cardinality and query cadence remain distinct quantities.
#[test]
fn observed_workload_facts_survive_binding_without_changing_units() {
    let mut wire = wire();
    for interval in [5000, 20000] {
        wire["query_workload"]["repeating_queries"][0]["demand"] =
            json!({"fixed_interval_at":{"interval":interval,"evaluation_phase":0}});
        let input: BackendLocalPlanningInput = serde_json::from_value(wire.clone()).unwrap();
        let expected = input.data_workload.clone();
        let (request, _) = input.into_physical_compilation_request().unwrap();
        assert_eq!(request.data_workload.as_ref(), Some(&expected));
        assert_eq!(
            request.queries[0]
                .summary_lifecycle_inputs
                .ingestion_rate_per_second,
            25.0
        );
        assert_eq!(
            request.queries[0]
                .summary_lifecycle_inputs
                .evaluation_interval_ms,
            interval
        );
        assert_eq!(expected.input_cardinality.value, Some(125));
    }
}

/// Missing, expired and future observations cannot be priced as zero ingestion.
#[test]
fn unavailable_rate_observations_are_rejected() {
    for observation in [
        json!({"value":null,"source":"unknown","observed_at_ms":null,"valid_for_ms":null}),
        json!({"value":25.0,"source":"observed","observed_at_ms":8000,"valid_for_ms":1000}),
        json!({"value":25.0,"source":"observed","observed_at_ms":11000,"valid_for_ms":1000}),
    ] {
        let mut wire = wire();
        wire["data_workload"]["ingestion_rate"] = observation.clone();
        let input: BackendLocalPlanningInput = serde_json::from_value(wire).unwrap();
        assert!(
            input.into_physical_compilation_request().is_err(),
            "{observation}"
        );
    }
}

/// No recent arrivals does not imply no retained series.
#[test]
fn observed_zero_rate_does_not_erase_cardinality() {
    let mut wire = wire();
    wire["data_workload"]["ingestion_rate"]["value"] = json!(0.0);
    let input: BackendLocalPlanningInput = serde_json::from_value(wire).unwrap();
    let (request, _) = input.into_physical_compilation_request().unwrap();
    assert_eq!(
        request.data_workload.unwrap().input_cardinality.value,
        Some(125)
    );
    assert_eq!(
        request.queries[0]
            .summary_lifecycle_inputs
            .ingestion_rate_per_second,
        0.0
    );
}

/// Repeated events count toward workload, not distinct cardinality; overflow invalidates the window.
#[test]
fn observation_population_counts_events_and_distinct_keys_separately() {
    let mut observer = ErpShapeObserver::with_limits(2, 4).unwrap();
    for _ in 0..30 {
        observer.observe("series-a", 0).unwrap();
    }
    for _ in 0..10 {
        observer.observe("series-b", 1).unwrap();
    }
    let observation = observer.snapshot().unwrap();
    assert_eq!(observation.observation.observed_events, 40);
    assert_eq!(observation.observation.cardinality, 2);
    assert!(observer.observe("series-c", 2).is_err());
    assert!(observer.snapshot().is_none());
}
