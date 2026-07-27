use asap_types::KeyByLabelNames;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::query_engines::QueryResult;

/// Convert QueryResult to Prometheus-compatible format (for instant queries only)
///
/// Returns an error if passed a Matrix result - use `convert_range_result_to_prometheus` for that.
pub fn convert_query_result_to_prometheus(
    result: &QueryResult,
    query_output_labels: &KeyByLabelNames,
) -> Result<Value, &'static str> {
    match result {
        QueryResult::Vector(instant_vector) => {
            let mut prometheus_results = Vec::new();
            let timestamp = instant_vector.timestamp as f64 / 1000.0;

            for element in &instant_vector.values {
                // Build metric labels object. Per-element override
                // (`element.label_keys_override`) wins when the
                // adapter knows the keys at materialization time —
                // e.g. ASAP-tier `topk(...)` synthesizes an `"item"`
                // key that's not in the query's group-by clause, so
                // the outer `query_output_labels` doesn't carry it.
                // Falls back to the query-scoped key list for everyone
                // else. Mirrors the matrix branch in
                // `convert_range_result_to_prometheus`.
                let mut metric_map: HashMap<&String, &String> = HashMap::new();
                let effective_keys: &[String] = element
                    .label_keys_override
                    .as_deref()
                    .unwrap_or(&query_output_labels.labels);
                for (key, label) in effective_keys
                    .iter()
                    .zip(element.labels.labels.iter())
                {
                    metric_map.insert(key, label);
                }

                let prometheus_result = json!({
                    "metric": metric_map,
                    "value": [timestamp, element.value.to_string()]
                });
                prometheus_results.push(prometheus_result);
            }
            Ok(json!({
                "resultType": "vector",
                "result": prometheus_results
            }))
        }
        QueryResult::Matrix(_) => {
            Err("convert_query_result_to_prometheus called with Matrix result; use convert_range_result_to_prometheus instead")
        }
    }
}

/// Convert range query result to Prometheus matrix format (for range queries only)
///
/// Returns an error if passed a Vector result - use `convert_query_result_to_prometheus` for that.
pub fn convert_range_result_to_prometheus(
    result: &QueryResult,
    label_names: &KeyByLabelNames,
) -> Result<Value, &'static str> {
    match result {
        QueryResult::Matrix(matrix) => {
            let results: Vec<Value> = matrix
                .values
                .iter()
                .map(|element| {
                    // Build metric labels object. Per-element override
                    // (`element.label_keys_override`) wins when the
                    // adapter knows the keys at materialization time —
                    // e.g. ASAP-tier `topk` synthesizes an `"item"` key
                    // that's not in the query's group-by clause, so the
                    // outer `label_names` doesn't carry it. Falls back
                    // to the query-scoped key list for everyone else.
                    let mut metric = serde_json::Map::new();
                    let effective_keys: &[String] = element
                        .label_keys_override
                        .as_deref()
                        .unwrap_or(&label_names.labels);
                    for (i, label_name) in effective_keys.iter().enumerate() {
                        if i < element.labels.labels.len() {
                            metric.insert(
                                label_name.clone(),
                                Value::String(element.labels.labels[i].clone()),
                            );
                        }
                    }

                    // Build values array: [[timestamp, "value"], ...]
                    let values: Vec<Value> = element
                        .samples
                        .iter()
                        .map(|sample| {
                            json!([
                                sample.timestamp as f64 / 1000.0, // Convert ms to seconds
                                sample.value.to_string()
                            ])
                        })
                        .collect();

                    json!({
                        "metric": metric,
                        "values": values
                    })
                })
                .collect();

            Ok(json!({
                "resultType": "matrix",
                "result": results
            }))
        }
        QueryResult::Vector(_) => {
            Err("convert_range_result_to_prometheus called with Vector result; use convert_query_result_to_prometheus instead")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engines::query_result::{InstantVectorElement, RangeVectorElement};
    use crate::storage_engines::types::KeyByLabelValues;

    fn create_test_labels() -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec!["host1".to_string(), "job1".to_string()])
    }

    fn create_test_label_names() -> KeyByLabelNames {
        KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()])
    }

    // Tests for convert_query_result_to_prometheus

    #[test]
    fn test_convert_instant_vector_to_prometheus() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();
        let element = InstantVectorElement::new(labels, 42.0);
        let result = QueryResult::vector(vec![element], 1000);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "vector");
        assert_eq!(data["result"].as_array().unwrap().len(), 1);

        let first_result = &data["result"][0];
        assert_eq!(first_result["metric"]["instance"], "host1");
        assert_eq!(first_result["metric"]["job"], "job1");
        // Timestamp is converted from ms to seconds: 1000ms -> 1.0s
        assert_eq!(first_result["value"][0], 1.0);
        assert_eq!(first_result["value"][1], "42");
    }

    #[test]
    fn test_convert_instant_vector_empty() {
        let label_names = create_test_label_names();
        let result = QueryResult::vector(vec![], 1000);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();
        assert_eq!(data["resultType"], "vector");
        assert!(data["result"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_convert_instant_vector_rejects_matrix() {
        let label_names = create_test_label_names();
        let labels = create_test_labels();
        let element = RangeVectorElement::new(labels);
        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_err());
        assert!(prometheus_data.err().unwrap().contains("Matrix"));
    }

    // Tests for convert_range_result_to_prometheus

    #[test]
    fn test_convert_range_vector_to_prometheus() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();

        let mut element = RangeVectorElement::new(labels);
        element.add_sample(1000, 10.0);
        element.add_sample(2000, 20.0);
        element.add_sample(3000, 30.0);

        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "matrix");
        assert_eq!(data["result"].as_array().unwrap().len(), 1);

        let first_result = &data["result"][0];
        assert_eq!(first_result["metric"]["instance"], "host1");
        assert_eq!(first_result["metric"]["job"], "job1");

        let values = first_result["values"].as_array().unwrap();
        assert_eq!(values.len(), 3);

        // Check timestamps converted from ms to seconds
        assert_eq!(values[0][0], 1.0); // 1000ms -> 1.0s
        assert_eq!(values[0][1], "10");
        assert_eq!(values[1][0], 2.0); // 2000ms -> 2.0s
        assert_eq!(values[1][1], "20");
        assert_eq!(values[2][0], 3.0); // 3000ms -> 3.0s
        assert_eq!(values[2][1], "30");
    }

    #[test]
    fn test_convert_range_vector_empty_samples() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();
        let element = RangeVectorElement::new(labels);
        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "matrix");
        let first_result = &data["result"][0];
        assert!(first_result["values"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_convert_range_vector_multiple_series() {
        let labels1 = KeyByLabelValues::new_with_labels(vec!["host1".to_string()]);
        let labels2 = KeyByLabelValues::new_with_labels(vec!["host2".to_string()]);
        let label_names = KeyByLabelNames::new(vec!["instance".to_string()]);

        let mut element1 = RangeVectorElement::new(labels1);
        element1.add_sample(1000, 10.0);

        let mut element2 = RangeVectorElement::new(labels2);
        element2.add_sample(1000, 100.0);

        let result = QueryResult::matrix(vec![element1, element2]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["result"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_convert_range_vector_rejects_vector() {
        let label_names = create_test_label_names();
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels, 42.0);
        let result = QueryResult::vector(vec![element], 1000);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_err());
        assert!(prometheus_data.err().unwrap().contains("Vector"));
    }
}
