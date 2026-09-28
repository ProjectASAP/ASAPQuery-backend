use super::*;
use asap_physical_operators::summary_kernels::univmon::UnivMonAccumulator;
use control_plane::physical::erp::ErpShapeObserver;
use data_plane::storage_engines::types::{AggregateCore, SerializableToSink};

fn values(offset: usize) -> Vec<f64> {
    (1..=128)
        .flat_map(|key| std::iter::repeat_n((key + offset) as f64, 256 / key))
        .collect()
}

fn truth(raw: &[f64]) -> [f64; 3] {
    let mut counts = std::collections::HashMap::<u64, usize>::new();
    for value in raw {
        *counts.entry(value.to_bits()).or_default() += 1;
    }
    let l2 = counts
        .values()
        .map(|n| (*n as f64).powi(2))
        .sum::<f64>()
        .sqrt();
    let entropy = counts
        .values()
        .map(|n| {
            let p = *n as f64 / raw.len() as f64;
            -p * p.log2()
        })
        .sum();
    [counts.len() as f64, l2, entropy]
}

fn measured_artifact() -> Value {
    let mut records = Vec::new();
    for (heap, cols) in [(8, 128), (128, 1024)] {
        let mut errors = [0.0f64; 3];
        let mut bytes = 0;
        // Independent key populations exercise fixed implementation hashes.
        // These are empirical trials, not a claimed tail-probability bound.
        for trial in 0..10 {
            let raw = values(trial * 1000);
            let exact = truth(&raw);
            let mut panes = [
                UnivMonAccumulator::new(heap, 5, cols, 4).unwrap(),
                UnivMonAccumulator::new(heap, 5, cols, 4).unwrap(),
            ];
            for (i, value) in raw.iter().enumerate() {
                panes[i % 2].insert_sample(*value).unwrap();
            }
            let other = panes[1].clone();
            panes[0].merge_in_place(&other).unwrap();
            bytes = bytes.max(panes[0].serialize_to_bytes().len());
            for (i, stat) in [
                asap_types::Statistic::Cardinality,
                asap_types::Statistic::FrequencyL2,
                asap_types::Statistic::FrequencyEntropy,
            ]
            .into_iter()
            .enumerate()
            {
                let estimate = panes[0]
                    .query_statistic(stat, &None, &Default::default())
                    .unwrap();
                assert!(estimate.is_finite());
                let error = (estimate - exact[i]).abs() / if i == 2 { 1.0 } else { exact[i] };
                errors[i] = errors[i].max(error);
            }
        }
        records.push(serde_json::json!({
            "id": format!("univmon-unit-frequency-h{heap}-c{cols}"),
            "sketch": "univmon", "implementation": "asap-sketchlib-univmon-standard-v1",
            "parameters": {"heap_size": heap, "sketch_rows": 5, "sketch_cols": cols, "layers": 4},
            "trials": 10,
            "distribution": {"erp_shape": {"family": "zipf", "parameters": {"exponent": 1.0}, "cardinality": 128, "benchmark_events": values(0).len()}},
            "error_metrics": {
                "max_cardinality_relative_error": errors[0],
                "max_frequency_l2_relative_error": errors[1],
                "max_frequency_entropy_absolute_bits_error": errors[2]
            },
            "resources": {"memory_bytes": bytes, "update_cpu_seconds": 0.0, "query_cpu_seconds": 0.0, "merge_cpu_seconds": 0.0}
        }));
    }
    serde_json::json!({"schema_version": 1, "producer_version": "backend-univmon-standard-unit-frequency-two-pane-error-fixture-cpu-not-measured", "records": records})
}

#[tokio::test]
async fn measured_univmon_without_confidence_uses_exact_process() {
    let artifact = measured_artifact();
    eprintln!("UNIVMON_MEASURED {artifact}");
    let raw = values(100_000);
    let mut observer = ErpShapeObserver::new(128).unwrap();
    for (i, value) in raw.iter().enumerate() {
        observer.observe(&value.to_string(), i / 100).unwrap();
    }
    let observation = observer.snapshot().unwrap();
    let queries = [
        "distinct_over_time(erp_frequency[5s])",
        "l2_over_time(erp_frequency[5s])",
        "entropy_over_time(erp_frequency[5s])",
    ];
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let template = fixture["query_workload"]["repeating_queries"][3].clone();
    fixture["query_workload"]["repeating_queries"] = queries
        .iter()
        .map(|query| {
            let mut entry = template.clone();
            entry["query"] = (*query).into();
            // ERP evidence is calibrated for one complete five-second population.
            entry["demand"]["fixed_interval_at"]["interval"] = serde_json::json!(5000);
            entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.2}});
            entry
        })
        .collect::<Vec<_>>()
        .into();
    fixture["implementation"]["erp"] = serde_json::json!({
        "distribution": {"workload": {"external": {"dataset": "held-out-frequency-population"}}},
        "artifact": artifact, "implementation": null, "error_metric": "readout_specific",
        "min_trials": 10, "expected_updates": raw.len(), "expected_queries": 10.0,
        "expected_merges": 1.0, "retention_seconds": 60.0, "cpu_weight": 0.0,
        "byte_second_weight": 1e-9, "mode": "hybrid", "observed_shape": observation.observation,
        "shape_match": {"minimum_benchmark_events": 1000, "max_log2_cardinality_distance": 0.0,
            "max_parameter_distance": 0.2, "max_goodness_of_fit": 0.2,
            "minimum_confidence": 0.7, "minimum_confidence_margin": 0.05},
        "runtime": {"allowed_algorithms": ["Hll", "Kll", "UnivMon"], "max_memory_bytes": null}
    });
    // Measured maxima across ten populations are not a failure-probability proof.
    assert_uncertified_exact_process(fixture.clone(), &queries).await;
    // Removing one readout's measurements cannot authorize the other readouts.
    for row in fixture["implementation"]["erp"]["artifact"]["records"]
        .as_array_mut()
        .unwrap()
    {
        row["error_metrics"]
            .as_object_mut()
            .unwrap()
            .remove("max_frequency_entropy_absolute_bits_error");
    }
    assert_uncertified_exact_process(fixture, &queries).await;
}
