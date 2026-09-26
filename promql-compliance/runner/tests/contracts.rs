use promql_compliance::{
    compare,
    input::{Dataset, Policy, Suite, Tolerance},
    planning, runner, sql, transport,
};
use prost::Message;
use serde_json::{json, Value};
use std::path::Path;

fn dataset() -> Dataset {
    Dataset::parse("name: data\nseries:\n- metric: m\n  labels: {job: api}\n  samples:\n  - {offset_seconds: 0, value: 1}\n  - {offset_seconds: 1, value: 2}\n").unwrap()
}
fn suite() -> Suite {
    Suite::parse("name: sum\nqueries:\n- name: sum\n  expr: 'sum(sum_over_time(m[1m]))'\n  instant_offsets_seconds: [60]\n  range: {start_offset_seconds: 60, end_offset_seconds: 120, step_seconds: 60}\n").unwrap()
}
fn plan() -> control_plane::physical::compiler::CompiledPhysicalPlan {
    planning::snapshot(&suite(), &dataset(), 10000, 0, false)
        .unwrap()
        .compile_promql()
        .unwrap()
}
fn vector(value: &str, ts: f64) -> Value {
    json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[ts,value]}]}})
}

/// All existing YAML corpora remain readable by the Rust execution path.
#[test]
fn loads_every_corpus_and_rejects_ambiguous_inputs() {
    for folder in ["../datasets", "../suites"] {
        for file in std::fs::read_dir(folder).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_some_and(|e| e == "yaml") {
                if folder.ends_with("datasets") {
                    Dataset::load(&path).unwrap();
                } else {
                    Suite::load(&path).unwrap();
                }
            }
        }
    }
    for bad in ["name: x\nqueries: []", "name: x\nqueries: [{name: q, expr: m}]", "name: x\nqueries: [{name: q, expr: m, range: {start_offset_seconds: 0, end_offset_seconds: 1, step_seconds: 0}}]"]{assert!(Suite::parse(bad).is_err());}
    assert!(Dataset::parse(
        "name: x\nseries: [{metric: m, typo: 1, samples: [{offset_seconds: 0, value: 1}]}]"
    )
    .is_err());
    assert!(Dataset::parse("name: x\nseries: [{metric: m, samples: [{offset_seconds: 0, value: 1}, {offset_seconds: 0.0001, value: 2}]}]").is_err());
    assert!(Dataset::parse("name: x\nseries: [{metric: m, generated_samples: {start_offset_seconds: 0, end_offset_seconds: 1, step_seconds: 0.3, multiplier: 1, base: 1, modulo: 3}}]").is_err());
}

/// One canonical compressed protobuf payload preserves labels, samples and time.
#[test]
fn remote_write_roundtrip_and_generated_population() {
    let data = Dataset::load(Path::new("../datasets/issue-754.yaml")).unwrap();
    let encoded = transport::encode(&data, 1_700_000_000_000).unwrap();
    let raw = snap::raw::Decoder::new().decompress_vec(&encoded).unwrap();
    let decoded = transport::WriteRequest::decode(raw.as_slice()).unwrap();
    assert_eq!(decoded.timeseries.len(), data.series.len());
    for (wire, source) in decoded.timeseries.iter().zip(&data.series) {
        assert!(wire.labels.windows(2).all(|p| p[0].name < p[1].name));
        assert_eq!(wire.samples.len(), source.samples.len());
        for (a, b) in wire.samples.iter().zip(&source.samples) {
            assert_eq!(a.value, b.value);
            assert_eq!(
                a.timestamp,
                promql_compliance::input::at_ms(1_700_000_000_000, b.offset_seconds).unwrap()
            );
        }
    }
    assert!(transport::encode(&dataset(), i64::MAX).is_err());
}

/// Finite tolerance cannot hide label/time mistakes or mismatched NaN/infinity.
#[test]
fn compares_labels_timestamps_special_values_and_parity() {
    let policy = Policy {
        value_tolerance: Some(Tolerance {
            relative: Some(0.01),
            absolute: Some(1e-6),
        }),
    };
    compare::compare(&vector("10", 1.), &vector("10.05", 1.), &policy).unwrap();
    assert!(compare::compare(&vector("10", 1.), &vector("10", 2.), &policy).is_err());
    let mut other = vector("10", 1.);
    other["data"]["result"][0]["metric"]["job"] = json!("wrong");
    assert!(compare::compare(&vector("10", 1.), &other, &policy).is_err());
    for (a, b, valid) in [
        ("NaN", "NaN", true),
        ("+Inf", "+Inf", true),
        ("-Inf", "+Inf", false),
        ("NaN", "0", false),
    ] {
        assert_eq!(
            compare::compare(&vector(a, 1.), &vector(b, 1.), &policy).is_ok(),
            valid
        );
    }
    let range = json!({"status":"success","data":{"resultType":"matrix","result":[{"metric":{"job":"api"},"values":[[1,"10"],[2,"20"]]}]}});
    compare::parity(&range, &vector("20", 2.), 2000, &policy).unwrap();
    assert!(compare::parity(&range, &vector("10", 1.), 2000, &policy).is_err());
    let a = json!({"status":"success","data":{"resultType":"scalar","result":[1,"5"]}});
    let mut b = a.clone();
    b["data"]["result"][0] = json!(2);
    assert!(compare::compare(&a, &b, &policy).is_err());
}

/// Tolerance overrides preserve the unspecified part of the suite policy.
#[test]
fn query_tolerance_merges_without_mutating_defaults() {
    let mut s = suite();
    s.comparison_defaults = Policy {
        value_tolerance: Some(Tolerance {
            relative: Some(0.01),
            absolute: Some(1e-6),
        }),
    };
    s.queries[0].comparison = Some(Policy {
        value_tolerance: Some(Tolerance {
            relative: Some(0.1),
            absolute: None,
        }),
    });
    let merged = s.queries[0]
        .policy(&s.comparison_defaults)
        .value_tolerance
        .unwrap();
    assert_eq!(merged.relative, Some(0.1));
    assert_eq!(merged.absolute, Some(1e-6));
    assert_eq!(
        s.comparison_defaults.value_tolerance.unwrap().relative,
        Some(0.01)
    );
}

/// Benefit costing uses the actual replay population and never supplies quotes.
#[test]
fn snapshot_retains_real_data_and_query_demand() {
    let data = Dataset::load(Path::new("../datasets/issue-754.yaml")).unwrap();
    let suite = Suite::load(Path::new("../suites/issue-754.yaml")).unwrap();
    let snapshot = planning::snapshot(&suite, &data, 10000, 0, true).unwrap();
    let plan = snapshot.clone().compile_promql().unwrap();
    planning::validate_cost(&plan).unwrap();
    planning::validate_local(&plan).unwrap();
    let value = serde_json::to_value(snapshot).unwrap();
    assert_eq!(value["implementation"]["scrape_interval_ms"], 100);
    assert!(value.get("workload_cost_evidence").is_none());
    assert!(value["query_workload"].get("data_workload").is_none());
    assert_eq!(
        value["data_workload"]["input_cardinality"]["value"],
        data.series.len()
    );
    assert_eq!(
        value["data_workload"]["data_ingestion_interval"]["value"],
        100
    );
    assert_eq!(
        value["data_workload"]["ingestion_rate"]["value"],
        data.series.len() as f64 * 10.
    );
    assert_eq!(
        value["query_workload"]["repeating_queries"][0]["query"],
        suite.queries[0].expr
    );
    let mut irregular = data.clone();
    irregular.series[0].samples[1].offset_seconds += 0.01;
    assert!(planning::snapshot(&suite, &irregular, 10000, 0, true).is_err());
}

/// Typed backend reports reject missing coverage, altered totals, uncosted
/// winners, negative resources and fabricated ERP provenance.
#[test]
fn automatic_cost_gate_checks_real_compiler_output() {
    use control_plane::physical::workload_cost::CandidateEvaluationStatus as Status;
    let original = plan();
    planning::validate_cost(&original).unwrap();
    for mutation in 0..6 {
        let mut plan = original.clone();
        let report = plan.cost_comparison.as_mut().unwrap();
        let selected = report
            .candidate_evaluations
            .iter_mut()
            .find(|c| c.status == Status::Selected)
            .unwrap();
        match mutation {
            0 => {
                report.component_costs.pop_first();
            }
            1 => {
                selected.total_cost = Some(999999.);
            }
            2 => {
                selected.total_cost = None;
            }
            3 => {
                selected
                    .automatic_cost
                    .as_mut()
                    .unwrap()
                    .components
                    .first_entry()
                    .unwrap()
                    .get_mut()
                    .cpu_seconds = -1.;
            }
            4 => {
                let first = selected
                    .automatic_cost
                    .as_mut()
                    .unwrap()
                    .components
                    .first_entry()
                    .unwrap()
                    .into_mut();
                first.source = "erp+analytical".into();
                first.erp_record_ids.clear();
            }
            _ => {
                selected.automatic_cost = None;
            }
        }
        assert!(
            planning::validate_cost(&plan).is_err(),
            "mutation {mutation}"
        );
    }
}

/// Backend-local headers do not make an exact fallback/subquery a local plan.
#[test]
fn local_gate_rejects_typed_exact_nodes() {
    use asap_types::query_plan::{residual::ResidualQueryOperator as Op, QueryPlanNode as Node};
    for node in [
        Node::ExactFallback {
            reason: "test".into(),
        },
        Node::Logical {
            operator: Op::ExactSubquery { query: "m".into() },
            inputs: vec![],
        },
        Node::Logical {
            operator: Op::CandidateExactSubquery {
                query: "m".into(),
                item_label: "job".into(),
            },
            inputs: vec![],
        },
    ] {
        let mut plan = plan();
        let entry = plan.query_plan.entries.values_mut().next().unwrap();
        entry.nodes.insert(entry.root, node);
        assert!(planning::validate_local(&plan).is_err());
    }
}

/// Every shared case has an exact SQL implementation, including counter resets.
#[test]
fn all_ten_sql_baselines_and_latency_assertions() {
    let suite = Suite::load(Path::new("../suites/issue-754.yaml")).unwrap();
    assert_eq!(suite.queries.len(), 10);
    for q in &suite.queries {
        let sql =
            sql::baseline(&q.name, 1_700_000_120_000, sql::window_ms(&q.expr).unwrap()).unwrap();
        assert!(sql.ends_with("FORMAT JSON"));
        assert!(!sql.contains("TDigest"));
        assert!(!sql.contains("{evaluation_ms}"));
        if q.expr.contains("rate(") {
            assert!(sql.contains("reset_correction"));
        }
    }
    assert!(sql::baseline("unknown", 0, 1000).is_err());
    assert_eq!(runner::percentile(&[9., 1., 2., 3., 4.], 0.95).unwrap(), 9.);
    assert!(runner::percentile(&[], 0.5).is_err());
    let mut targets = json!({});
    for name in ["backend", "prometheus", "victoria", "clickhouse"] {
        let factor = if name == "backend" { 1 } else { 2 };
        let mut queries = json!({});
        for q in &suite.queries {
            queries[&q.name] = json!({"p95Ms":factor as f64});
        }
        targets[name] = json!({"cpuUsec":factor,"memoryPeakBytes":factor,"queries":queries});
    }
    assert!(runner::benefit_failures(&targets, &suite)
        .unwrap()
        .is_empty());
    targets["backend"]["cpuUsec"] = json!(2);
    assert!(!runner::benefit_failures(&targets, &suite)
        .unwrap()
        .is_empty());
    assert_eq!(
        promql_compliance::compose::parse_cpu_stat("usage_usec 0\nuser_usec 0").unwrap(),
        0
    );
    assert!(promql_compliance::compose::parse_cpu_stat("user_usec 1").is_err());
}

/// The report card excludes plan/snapshot artifacts and retains failed cases.
#[test]
fn report_card_counts_provenance_and_ignores_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    runner::write_json(&directory.path().join("pass.json"),&json!({"passed":true,"dataset":"d","suite":"s","queries":[{"instant":[{"responses":{"backend":{"servedBy":"asap_query"}}}]}]})).unwrap();
    runner::write_json(
        &directory.path().join("fail.json"),
        &json!({"passed":false,"dataset":"e","suite":"s","error":"planning failed"}),
    )
    .unwrap();
    runner::write_json(
        &directory.path().join("pass.plan.json"),
        &json!({"opaque":"plan"}),
    )
    .unwrap();
    runner::report_card(directory.path()).unwrap();
    let result: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("summary.json")).unwrap())
            .unwrap();
    assert_eq!(result["passed"], false);
    assert_eq!(result["cases"].as_array().unwrap().len(), 2);
    assert_eq!(result["cases"][0]["asapQuery"], 1);
}

/// Planning uses the same evaluation grid as the actual replay requests.
#[test]
fn snapshot_preserves_replay_evaluation_phase() {
    let mut queries = suite();
    queries.queries[0].instant_offsets_seconds = vec![60., 61.];
    let snapshot = planning::snapshot(&queries, &dataset(), 10000, 396, false).unwrap();
    let value = serde_json::to_value(snapshot).unwrap();
    let demand = &value["query_workload"]["repeating_queries"][0]["demand"]["fixed_interval_at"];
    assert_eq!(demand["interval"], 1000);
    assert_eq!(demand["evaluation_phase"], 396);
}

/// Declared replay cadence must cover actual input gaps for spatial history.
#[test]
fn differential_snapshot_uses_actual_fixture_cadence() {
    let data = Dataset::load(Path::new("../datasets/aggregations-dense-cadence.yaml")).unwrap();
    let suite = Suite::load(Path::new("../suites/issue-702.yaml")).unwrap();
    let snapshot = planning::snapshot(&suite, &data, 10000, 0, false).unwrap();
    let value = serde_json::to_value(snapshot).unwrap();
    assert_eq!(value["implementation"]["require_backend_local_execution"], true);
    assert_eq!(value["implementation"]["scrape_interval_ms"], 60000);
    assert_eq!(
        value["data_workload"]["data_ingestion_interval"]["value"],
        60000
    );
}
