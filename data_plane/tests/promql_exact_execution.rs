//! The same hand-checked fixture used by official promtool must execute locally.
use control_plane::physical::promql_exact::ExactPromqlPlan;
use data_plane::query_engines::canonical::exact_promql::{execute, Labels, RawSeries};
use serde_json::Value;

fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| value.as_str().unwrap().parse().unwrap())
}

/// Every smoke query must bind to a real kernel and produce the official labels and values.
#[test]
fn all_smoke_queries_bind_and_execute_exactly() {
    let cases: Value =
        serde_json::from_str(include_str!("../../tools/promql-smoke/cases.json")).unwrap();
    let start = cases["start"].as_f64().unwrap();
    let interval = cases["interval"].as_f64().unwrap();
    let evaluation = start + cases["eval_offset"].as_f64().unwrap();
    let data: Vec<_> = cases["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|series| RawSeries {
            labels: serde_json::from_value(series["labels"].clone()).unwrap(),
            samples: series["values"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
                .filter_map(|(i, v)| v.as_f64().map(|value| (start + i as f64 * interval, value)))
                .collect(),
        })
        .collect();
    let mut failures = Vec::new();
    for query in cases["queries"].as_array().unwrap() {
        let id = query["id"].as_str().unwrap();
        let expr = query["expr"].as_str().unwrap();
        let result = (|| -> anyhow::Result<()> {
            let plan = ExactPromqlPlan::bind(expr)?;
            let actual = execute(&plan, &data, evaluation, 300.0)?;
            let expected = query["expected"].as_array().unwrap();
            anyhow::ensure!(
                actual.len() == expected.len(),
                "series count {} != {}",
                actual.len(),
                expected.len()
            );
            for (index, sample) in expected.iter().enumerate() {
                let labels: Labels = serde_json::from_value(sample["labels"].clone())?;
                let value = number(&sample["value"])
                    + if query["value_is_timestamp"] == true {
                        start
                    } else {
                        0.0
                    };
                let got = actual
                    .iter()
                    .find(|s| s.labels == labels)
                    .ok_or_else(|| anyhow::anyhow!("missing labels {labels:?}; got {actual:?}"))?;
                let equal = if value.is_nan() {
                    got.value.is_nan()
                } else if value.is_infinite() {
                    got.value == value
                } else {
                    (got.value - value).abs() <= 1e-12 + 1e-12 * value.abs()
                };
                anyhow::ensure!(equal, "{labels:?}: {} != {value}", got.value);
                if query["ordered"] == true {
                    anyhow::ensure!(actual[index].labels == labels, "wrong series order");
                }
            }
            Ok(())
        })();
        match result {
            Ok(()) => println!("PASS {id}"),
            Err(error) => failures.push(format!("{id}: {expr}: {error}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} queries failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Unsupported shape modifiers must be rejected during binding, never ignored by execution.
#[test]
fn unsupported_exact_shapes_are_not_bound() {
    for query in [
        "sum(smoke_gauge offset 1m)",
        "sum(smoke_gauge @ 100)",
        "sum_over_time(smoke_gauge[5m:1m])",
        "smoke_gauge + on(job) smoke_gauge",
    ] {
        assert!(ExactPromqlPlan::bind(query).is_err(), "must reject {query}");
    }
}

/// Duplicate timestamps and duplicate label identities cannot enter the evaluator silently.
#[test]
fn invalid_raw_snapshots_are_rejected() {
    let plan = ExactPromqlPlan::bind("sum(smoke_gauge)").unwrap();
    let series = RawSeries {
        labels: [("__name__".into(), "smoke_gauge".into())]
            .into_iter()
            .collect(),
        samples: vec![(1.0, 2.0), (1.0, 3.0)],
    };
    assert!(execute(&plan, &[series.clone()], 2.0, 300.0).is_err());
    let unique = RawSeries {
        samples: vec![(1.0, 2.0)],
        ..series
    };
    assert!(execute(&plan, &[unique.clone(), unique], 2.0, 300.0).is_err());
}

/// Counting two series with equal numeric values must return two, not one distinct value.
#[test]
fn count_preserves_equal_valued_series_multiplicity() {
    let plan = ExactPromqlPlan::bind("count(smoke_gauge)").unwrap();
    let data: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|job| RawSeries {
            labels: [
                ("__name__".into(), "smoke_gauge".into()),
                ("job".into(), job.into()),
            ]
            .into_iter()
            .collect(),
            samples: vec![(240.0, 5.0)],
        })
        .collect();
    let result = execute(&plan, &data, 240.0, 300.0).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 2.0);
}

/// Negative ratios select from the upper end: -1 must retain the entire input.
#[test]
fn negative_full_ratio_keeps_every_series() {
    let plan = ExactPromqlPlan::bind("limit_ratio(-1, smoke_gauge)").unwrap();
    let data = vec![RawSeries {
        labels: [
            ("__name__".into(), "smoke_gauge".into()),
            ("job".into(), "a".into()),
        ]
        .into_iter()
        .collect(),
        samples: vec![(240.0, 5.0)],
    }];
    let result = execute(&plan, &data, 240.0, 300.0).unwrap();
    assert_eq!(result.len(), 1);
}
