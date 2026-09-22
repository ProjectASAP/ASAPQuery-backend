use super::*;
use control_plane::physical::{compiler::BackendLocalPlanningInput, erp::ErpShapeObserver};

fn measured_profiles(raw: &[f64]) -> Value {
    let mut records = Vec::new();
    for k in [32, 128] {
        let mut error = 0.0f64;
        let mut bytes = 0usize;
        for seed in 0..10 {
            let mut sketch = asap_sketchlib::KllSketch::with_seed(k, seed);
            for value in raw {
                sketch.update(*value);
            }
            bytes = bytes.max(sketch.sketch_bytes().len());
            for q in 1..100 {
                let q = q as f64 / 100.0;
                let estimate = sketch.quantile(q);
                let lower = raw.iter().filter(|v| **v < estimate).count() as f64 / raw.len() as f64;
                let upper =
                    raw.iter().filter(|v| **v <= estimate).count() as f64 / raw.len() as f64;
                error = error.max((lower - q).max(q - upper).max(0.0));
            }
        }
        records.push(serde_json::json!({
            "id": format!("process-test-k{k}"), "sketch": "kll-percall", "implementation": "lib",
            "parameters": {"k": k}, "trials": 10,
            "distribution": {"erp_shape": {"family": "zipf", "parameters": {"exponent": 1.0},
                "cardinality": 16, "benchmark_events": raw.len()}},
            "error_metrics": {"max_rank_err": error},
            "resources": {"memory_bytes": bytes, "update_cpu_seconds": 0.0,
                "query_cpu_seconds": 0.0, "merge_cpu_seconds": 0.0}
        }));
    }
    // This correctness fixture measures error and retained serialized state.
    // CPU is not the objective or a performance claim in this process test.
    serde_json::json!({"schema_version": 1,
        "producer_version": "process-test-measured-error-and-serialized-state-cpu-not-measured",
        "records": records})
}

#[tokio::test]
async fn measured_kll_error_without_failure_probability_uses_exact_fallback() {
    const QUERY: &str = "quantile_over_time(0.9, erp_latency[5s])";
    let training: Vec<f64> = (1..=16)
        .flat_map(|value| std::iter::repeat_n(value as f64, 512 / value))
        .collect();
    let raw: Vec<f64> = (1..=16)
        .rev()
        .flat_map(|value| std::iter::repeat_n(value as f64, 256 / value))
        .collect();
    let mut observer = ErpShapeObserver::new(16).unwrap();
    for (index, value) in raw.iter().enumerate() {
        observer.observe(&value.to_string(), index / 100).unwrap();
    }
    let observation = observer.snapshot().unwrap();
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = QUERY.into();
    entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.2}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    fixture["implementation"]["erp"] = serde_json::json!({
        "distribution": {"workload": {"external": {"dataset": "held-out-process-stream"}}},
        "artifact": measured_profiles(&training), "implementation": "lib", "error_metric": "max_rank_err",
        "min_trials": 10, "expected_updates": raw.len(), "expected_queries": 10.0,
        "expected_merges": 0.0, "retention_seconds": 60.0, "cpu_weight": 0.0,
        "byte_second_weight": 1e-9, "mode": "hybrid", "observed_shape": observation.observation,
        "shape_match": {"minimum_benchmark_events": 1000, "max_log2_cardinality_distance": 0.0,
            "max_parameter_distance": 0.1, "max_goodness_of_fit": 0.1,
            "minimum_confidence": 0.8, "minimum_confidence_margin": 0.05},
        "runtime": {"allowed_algorithms": ["Kll"], "max_memory_bytes": null}
    });
    let plan = quote_snapshot_for_test(
        serde_json::from_value::<BackendLocalPlanningInput>(fixture).unwrap(),
    )
    .compile_promql()
    .unwrap();
    assert!(plan.precompute_plan.materializations.is_empty());
    assert!(plan
        .query_plan
        .entries
        .values()
        .all(|entry| entry.nodes.values().any(|node| {
            matches!(
                node,
                control_plane::query_plan::QueryPlanNode::ExactFallback { .. }
            )
        })));
}
