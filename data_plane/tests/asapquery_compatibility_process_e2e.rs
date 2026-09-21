//! Black-box acceptance test for the collector-free ASAPQuery profile.
//!
//! Starts the production binary from a canonical workload snapshot, ingests
//! only Prometheus Remote Write v1, exercises safe warm families and per-series fallback
//! through instant and range APIs, and verifies exact fallback request parity.

use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::Query, http::HeaderMap, routing::get, Json, Router};
use data_plane::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use prost::Message;
use serde_json::Value;
use tokio::sync::Mutex;

#[path = "support/erp_planning_process.rs"]
mod erp_planning_process;

#[path = "support/distinct_planning_process.rs"]
mod distinct_planning_process;
#[path = "support/durable_summary_process.rs"]
mod durable_summary_process;
#[path = "support/immutable_maintenance_process.rs"]
mod immutable_maintenance_process;

#[path = "support/current_series_process.rs"]
mod current_series_process;

#[path = "support/issue_701_702_process.rs"]
mod issue_701_702_process;

// Test-only quotes preserve the fixture's local candidate without a production bypass.
fn quote_snapshot_for_test(
    snapshot: control_plane::physical::compiler::BackendLocalPlanningInput,
) -> control_plane::physical::compiler::BackendLocalPlanningInput {
    quote_snapshot_for_frontend_test(snapshot, false)
}

fn quote_snapshot_for_frontend_test(
    mut snapshot: control_plane::physical::compiler::BackendLocalPlanningInput,
    metricsql: bool,
) -> control_plane::physical::compiler::BackendLocalPlanningInput {
    use control_plane::physical::{
        compiler::{PhysicalPlanCompiler, BACKEND_REVISION, PLANNER_REVISION},
        workload_cost::{self, WorkloadCostEvidence, WorkloadQuote},
    };
    let (request, environment) = snapshot
        .clone()
        .into_physical_compilation_request()
        .unwrap();
    let mut preferred = true;
    let quotes = workload_cost::enumerate_exact_and_materialized_candidates(request)
        .unwrap()
        .into_iter()
        .filter_map(|candidate| {
            let plan = if metricsql {
                PhysicalPlanCompiler.compile_metricsql(candidate.clone(), environment.clone())
            } else {
                PhysicalPlanCompiler.compile_promql(candidate.clone(), environment.clone())
            }
            .ok()?;
            let unit_cost = if preferred { 1.0 } else { 1e12 };
            preferred = false;
            let manifest = workload_cost::manifest(&plan, &candidate.queries).unwrap();
            Some(WorkloadQuote {
                unit_costs: manifest
                    .components
                    .keys()
                    .map(|key| (key.clone(), unit_cost))
                    .collect(),
                manifest,
                executable: true,
            })
        })
        .collect();
    snapshot.workload_cost_evidence = Some(WorkloadCostEvidence {
        backend_revision: BACKEND_REVISION.into(),
        planner_revision: PLANNER_REVISION.into(),
        data_snapshot_id: "process-fixture".into(),
        model_version: "test-only-unit-costs".into(),
        observed_at_unix_ms: environment.observed_at_unix_ms,
        valid_for_ms: environment.max_evidence_age_ms,
        quotes,
    });
    snapshot
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    listener.local_addr().expect("read loopback address").port()
}

async fn wait_until_ready(client: &reqwest::Client, url: &str, child: &mut Child) {
    for _ in 0..120 {
        if let Some(status) = child.try_wait().expect("inspect backend process") {
            panic!("backend exited before readiness: {status}");
        }
        if client
            .get(url)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("backend did not become ready at {url}");
}

fn series(metric: &str, samples: &[(i64, f64)]) -> TimeSeries {
    series_with_labels(metric, &[], samples)
}

fn series_with_labels(metric: &str, labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
    let mut wire_labels = vec![Label {
        name: "__name__".into(),
        value: metric.into(),
    }];
    wire_labels.extend(labels.iter().map(|(name, value)| Label {
        name: (*name).into(),
        value: (*value).into(),
    }));
    TimeSeries {
        labels: wire_labels,
        samples: samples
            .iter()
            .map(|(timestamp, value)| Sample {
                value: *value,
                timestamp: *timestamp,
            })
            .collect(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }
}

async fn remote_write(client: &reqwest::Client, base: &str, request: &WriteRequest) -> u16 {
    let body = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy encode");
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(body)
        .send()
        .await
        .expect("send Remote Write");
    let status = response.status().as_u16();
    if status >= 400 {
        eprintln!(
            "Remote Write {status}: {}",
            response.text().await.unwrap_or_default()
        );
    }
    status
}

async fn drain_precompute(client: &reqwest::Client, backend: &str) {
    let response = client
        .post(format!("{backend}/api/v1/precompute/drain"))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "drain failed: {}",
        response.text().await.unwrap()
    );
}

fn first_value(response: &Value, field: &str) -> Option<f64> {
    let samples = response["data"]["result"]
        .as_array()?
        .first()?
        .get(field)?
        .as_array()?;
    let value = if field == "value" {
        samples.get(1)?
    } else {
        samples.last()?.as_array()?.get(1)?
    };
    value.as_str()?.parse().ok()
}

fn is_warm(response: &Value) -> bool {
    response["infos"].as_array().is_some_and(|infos| {
        infos.iter().any(|info| {
            info.as_str()
                .is_some_and(|line| line == "data_source: asap_query")
        })
    })
}

// Measured ERP parameters must reach the real accumulator and answer held-out
// raw samples through the installed QueryPlan, without native fallback.
#[tokio::test]
#[ignore = "fixture has stale physical lifecycle evidence"]
async fn erp_measured_kll_state_to_query_oracle() {
    use control_plane::physical::compiler::{BackendLocalPlanningInput, PhysicalPlanCompiler};
    const QUERY: &str = "quantile_over_time(0.9, erp_latency[5s])";
    let artifact: Value = serde_json::from_str(include_str!(
        "../../control_plane/tests/fixtures/erp-kll-measured.json"
    ))
    .unwrap();
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = QUERY.into();
    entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.06}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    fixture["implementation"]["erp"] = serde_json::json!({
        "distribution": artifact["records"][0]["distribution"],
        "artifact": artifact, "implementation": "lib", "error_metric": "max_rank_err",
        "min_trials": 10, "expected_updates": 1000.0, "expected_queries": 10.0,
        "expected_merges": 0.0, "retention_seconds": 60.0, "cpu_weight": 1.0,
        "byte_second_weight": 1e-9, "mode": "hybrid",
        "runtime": {"allowed_algorithms": ["Kll"], "max_memory_bytes": null}
    });
    let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
    let (mut request, mut environment) = snapshot.into_physical_compilation_request().unwrap();
    request.allow_mixed_summary_and_exact_execution = false;
    request.queries[0].group_by_labels = vec!["service".into()];
    let lifecycle_entry = &mut request
        .query_workload
        .as_mut()
        .unwrap()
        .repeating_queries
        .as_mut()
        .unwrap()[0];
    lifecycle_entry.time_selection.scope = planner_types::workload::QueryTimeScope::Unknown;
    environment.target =
        control_plane::physical::compiler::PhysicalDeploymentTarget::DistributedCollectors;
    environment.target_collector_ids = vec!["erp-collector".into()];
    let plan = PhysicalPlanCompiler
        .compile_promql(request, environment)
        .unwrap();
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert_eq!(plan.precompute_plan.materializations[0].parameters["k"], 32);
    let collector = serde_json::to_value(&plan.collector_plans[0]).unwrap();
    let install = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let output = tempfile::tempdir().unwrap();
    let mut artifact_file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut artifact_file, &install).unwrap();
    let port = unused_port();
    let otlp_port = unused_port();
    let grpc_port = unused_port();
    let mut bootstrap_config = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(
        &mut bootstrap_config,
        &serde_json::json!({"aggregations": []}),
    )
    .unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args(["--physical-plan"])
            .arg(artifact_file.path())
            .arg("--streaming-config")
            .arg(bootstrap_config.path())
            .args(["--http-port", &port.to_string(), "--output-dir"])
            .arg(output.path())
            .args([
                "--enable-otel-ingest",
                "--otel-http-port",
                &otlp_port.to_string(),
                "--otel-grpc-port",
                &grpc_port.to_string(),
            ])
            .args([
                "--precompute-allowed-lateness-ms",
                "0",
                "--precompute-flush-interval-ms",
                "25",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20000;
    // A different deterministic stream from training seed 42; the oracle
    // evaluates rank error, not the unrelated relative error of the value.
    let raw: Vec<f64> = (0..1000)
        .map(|i| ((i * 7919 + 17) % 1009) as f64 / 1009.0)
        .collect();
    for (sequence, end, values) in [
        (1, base + 5000, raw.as_slice()),
        (2, base + 15000, &[0.5][..]),
    ] {
        let payload = erp_collector_kll_export(&collector, end as u64, values, sequence);
        client
            .post(format!("http://127.0.0.1:{otlp_port}/v1/metrics"))
            .header("content-type", "application/x-protobuf")
            .body(payload)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    let response = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response: Value = client
                .get(format!("{backend}/api/v1/query"))
                .query(&[
                    ("query", QUERY.to_string()),
                    ("time", ((base + 5000) as f64 / 1000.0).to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if response["status"] == "success" && is_warm(&response) {
                break response;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("ERP plan must answer without exact fallback");
    let estimate = first_value(&response, "value").expect("numeric estimate");
    let rank = raw.iter().filter(|v| **v <= estimate).count() as f64 / raw.len() as f64;
    assert!(
        (rank - 0.9).abs() <= 0.06,
        "rank={rank}, response={response}"
    );
}

fn erp_collector_kll_export(plan: &Value, end_ms: u64, raw: &[f64], sequence: u64) -> Vec<u8> {
    use asap_otel_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        common::v1::{any_value, AnyValue, KeyValue},
        metrics::v1::{
            metric::Data, KllSketch, KllSketchDataPoint, KllSketchEncoding, Metric,
            ResourceMetrics, ScopeMetrics,
        },
    };
    use asap_sketchlib::proto::sketchlib::{sketch_envelope, SketchEnvelope};
    let decoded: asap_types::producer_plan::CollectorPlan =
        serde_json::from_value(plan.clone()).unwrap();
    assert_eq!(decoded.materializations.len(), 1);
    let k = 32;
    let mut sketch = asap_sketchlib::sketches::kll::KLL::<f64>::init_kll_with_seed(k, 123);
    for value in raw {
        sketch.update(value);
    }
    let wire = SketchEnvelope::decode(asap_sketch_codec::encode_kll(&sketch).as_slice()).unwrap();
    let Some(sketch_envelope::SketchState::Kll(state)) = wire.sketch_state else {
        panic!("KLL state required")
    };
    assert_eq!(state.k, k as u32);
    let kv = |key: &str, value: String| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    };
    let materialization = plan["materializations"][0]["materialization"]
        .as_u64()
        .unwrap();
    let mut attributes = vec![kv("service", "erp".into())];
    for (key, value) in [
        ("identity_version", "1".into()),
        ("plan_id", plan["envelope"]["plan_id"].to_string()),
        ("plan_version", plan["envelope"]["plan_version"].to_string()),
        (
            "backend_compat",
            control_plane::physical::compiler::BACKEND_COMPAT.into(),
        ),
        ("materialization", materialization.to_string()),
        (
            "series_identity",
            data_plane::drivers::ingest::canonical_attrs_fingerprint(&[("service", "erp")]),
        ),
        (
            "schema_id",
            format!(
                "{}:summary-state:v1:{materialization}",
                control_plane::physical::compiler::BACKEND_COMPAT
            ),
        ),
        ("producer_id", "erp-collector".into()),
        ("producer_epoch", "erp-test".into()),
        ("sequence", sequence.to_string()),
        ("kind", "full".into()),
        ("encoding", "sketchlib_protobuf_v1".into()),
        ("checkpoint_id", format!("checkpoint-{sequence}")),
    ] {
        attributes.push(kv(&format!("asap.frame.{key}"), value));
    }
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            schema_url: String::new(),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                schema_url: String::new(),
                metrics: vec![Metric {
                    name: "erp_latency".into(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(Data::Kllsketch(KllSketch {
                        k: k as u32,
                        aggregation_temporality: 0,
                        data_points: vec![KllSketchDataPoint {
                            attributes,
                            start_time_unix_nano: (end_ms - 5000) * 1_000_000,
                            time_unix_nano: end_ms * 1_000_000,
                            sketch: SketchEnvelope {
                                format_version: 1,
                                sketch_state: Some(sketch_envelope::SketchState::Kll(state)),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            encoding: KllSketchEncoding::Proto as i32,
                            flags: 0,
                            series_id: 0,
                        }],
                    })),
                }],
            }],
        }],
    }
    .encode_to_vec()
}

// Both heap implementations must execute registered temporal counts through
// an installed QueryPlan, retaining all three ranked identities and values.
#[tokio::test]
async fn registered_temporal_topk_cms_heap() {
    registered_temporal_topk(planner_types::post_asap::SketchAlgorithm::CmsWithHeap).await;
}

#[tokio::test]
async fn registered_temporal_topk_count_sketch_heap() {
    registered_temporal_topk(planner_types::post_asap::SketchAlgorithm::CountSketchWithHeap).await;
}

async fn registered_temporal_topk(algorithm: planner_types::post_asap::SketchAlgorithm) {
    use control_plane::physical::compiler::{BackendLocalPlanningInput, PhysicalPlanCompiler};
    use planner_types::post_asap::{CompositionOperator, SketchQuery, SummaryFamilyType};
    const QUERY: &str = "topk(3, count_over_time(top_endpoint_qps[5s]))";
    struct Evidence;
    impl asap_aware_mapping::AccuracyEvidenceProvider for Evidence {
        fn propagation_stats(
            &self,
            op: &CompositionOperator,
            _: &SummaryFamilyType,
            _: Option<&SketchQuery>,
        ) -> asap_aware_mapping::PropagationStats {
            if matches!(op, CompositionOperator::TopKSelection) {
                asap_aware_mapping::PropagationStats {
                    topk_selected_lower_bound: Some(95.0),
                    topk_excluded_upper_bound: Some(80.0),
                    topk_interval_failure_probability: Some(0.001),
                    ..Default::default()
                }
            } else {
                Default::default()
            }
        }
    }
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][5].clone();
    entry["query"] = QUERY.into();
    entry["requirements"]["accuracy"] =
        serde_json::json!({"explicit": {"EpsilonDelta": {"epsilon": 0.05, "delta": 0.05}}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    fixture["implementation"]["topk_evidence"] = serde_json::json!({
        QUERY: {
            "selected_lower_bound": 95.0, "excluded_upper_bound": 80.0,
            "interval_failure_probability": 0.001, "observed_at_unix_ms": 9500,
            "source": "deterministic-count-ranking-fixture"
        }
    });
    let snapshot: BackendLocalPlanningInput = serde_json::from_value(fixture).unwrap();
    let (mut request, environment) = snapshot.into_physical_compilation_request().unwrap();
    let query = &mut request.queries[0];
    let expr = control_plane::query_parser::parse_query_expr_canonical(
        QUERY,
        query.accuracy_target.clone(),
    )
    .unwrap();
    let model = control_plane::physical::post_asap::cost_model::ForcedFamilyCostModel::new(
        query.accuracy_target.clone(),
        algorithm.clone(),
    );
    query.selected_plan_root = control_plane::planner_selection::select_summary_with_evidence(
        &expr,
        &model,
        &asap_aware_mapping::DefaultAccuracyModel,
        &asap_aware_mapping::EqualSplitAllocator,
        &Evidence,
    )
    .unwrap();
    let plan = PhysicalPlanCompiler
        .compile_promql(request, environment)
        .unwrap();
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    use data_plane::storage_engines::types::AggregationType;
    let expected_type = match algorithm {
        planner_types::post_asap::SketchAlgorithm::CmsWithHeap => {
            AggregationType::CountMinSketchWithHeap
        }
        planner_types::post_asap::SketchAlgorithm::CountSketchWithHeap => {
            AggregationType::CountSketchWithHeap
        }
        _ => panic!("fixture requires a heap implementation"),
    };
    assert_eq!(
        plan.precompute_plan.materializations[0].aggregation_type,
        expected_type
    );
    assert_eq!(
        plan.precompute_plan.materializations[0].parameters["weight_mode"],
        "count"
    );
    let artifact = data_plane::drivers::query::servers::http::PhysicalPlanInstallRequest {
        summary_catalog: plan.summary_catalog,
        collector_plans: plan.collector_plans,
        precompute_plan: plan.precompute_plan,
        transmission_plan: plan.transmission_plan,
        query_plan: plan.query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    };
    let output = tempfile::tempdir().unwrap();
    let mut artifact_file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut artifact_file, &artifact).unwrap();
    let port = unused_port();
    let fallback = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_url = format!("http://{}", fallback.local_addr().unwrap());
    let fallback_task = tokio::spawn(async move {
        axum::serve(fallback, Router::new()
            .route("/-/healthy", get(|| async { "healthy" }))
            .route("/api/v1/query", get(|| async { Json(serde_json::json!({
                "status": "error", "errorType": "execution", "error": "fixture exact backend unavailable"
            })) }))).await.unwrap();
    });
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_data_plane"))
            .args(["--profile", "asapquery", "--physical-plan"])
            .arg(artifact_file.path())
            .args([
                "--forward-unsupported-queries",
                "--prometheus-server",
                &fallback_url,
            ])
            .args(["--http-port", &port.to_string(), "--output-dir"])
            .arg(output.path())
            .args([
                "--precompute-allowed-lateness-ms",
                "0",
                "--precompute-flush-interval-ms",
                "25",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20000;
    let counts = [
        ("alpha", 100),
        ("beta", 50),
        ("gamma", 200),
        ("delta", 75),
        ("epsilon", 10),
        ("zeta", 150),
    ];
    let samples = WriteRequest {
        timeseries: counts
            .iter()
            .map(|(item, count)| {
                // Non-unit values distinguish count updates from accidental weighted sums.
                let points = (0..2)
                    .flat_map(|window| {
                        (0..*count).map(move |i| (base + window * 5000 + 10 + i * 20, 17.0))
                    })
                    .collect::<Vec<_>>();
                series_with_labels("top_endpoint_qps", &[("endpoint", item)], &points)
            })
            .collect(),
    };
    assert_eq!(remote_write(&client, &backend, &samples).await, 204);
    let watermark = WriteRequest {
        timeseries: vec![series_with_labels(
            "top_endpoint_qps",
            &[("endpoint", "gamma")],
            &[(base + 10500, 17.0)],
        )],
    };
    assert_eq!(remote_write(&client, &backend, &watermark).await, 204);
    assert_eq!(remote_write(&client, &backend, &samples).await, 204);
    drain_precompute(&client, &backend).await;
    let timestamp = (base + 5000) as f64 / 1000.0;
    let instant = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response: Value = client
                .get(format!("{backend}/api/v1/query"))
                .query(&[
                    ("query", QUERY.to_string()),
                    ("time", timestamp.to_string()),
                ])
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if response["status"] == "success" && is_warm(&response) {
                break response;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("registered TopK must become warm within 30s");
    let expected = [("gamma", 200.0), ("zeta", 150.0), ("alpha", 100.0)];
    let assert_ranks = |response: &Value, range: bool| {
        assert_eq!(response["status"], "success", "{response}");
        assert!(is_warm(response), "{response}");
        let rows = response["data"]["result"].as_array().unwrap();
        assert_eq!(rows.len(), 3, "{response}");
        for (item, count) in expected {
            let row = rows
                .iter()
                .find(|row| {
                    row["metric"]["item"].as_str()
                        == Some(format!("top_endpoint_qps{{endpoint=\"{item}\"}}").as_str())
                })
                .unwrap_or_else(|| panic!("missing {item}: {response}"));
            let points = if range {
                let points = row["values"].as_array().unwrap();
                assert_eq!(points.len(), 2);
                points.clone()
            } else {
                vec![row["value"].clone()]
            };
            for (index, point) in points.iter().enumerate() {
                assert_eq!(point[0].as_f64(), Some(timestamp + index as f64 * 5.0));
                assert_eq!(
                    point[1].as_str().unwrap().parse::<f64>().unwrap(),
                    count,
                    "{response}"
                );
            }
        }
    };
    assert_ranks(&instant, false);
    let range: Value = client
        .get(format!("{backend}/api/v1/query_range"))
        .query(&[
            ("query", QUERY.to_string()),
            ("start", timestamp.to_string()),
            ("end", (timestamp + 5.0).to_string()),
            ("step", "5".into()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ranks(&range, true);
    let unregistered: Value = client
        .get(format!("{backend}/api/v1/query"))
        .query(&[("query", "topk(3, top_endpoint_qps)")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        unregistered["status"], "error",
        "unregistered query must not replan: {unregistered}"
    );
    assert_eq!(unregistered["error"], "fixture exact backend unavailable");
    fallback_task.abort();
}

// Three registered consumers must observe one raw SUM/count producer, including
// uneven instance sample counts and Remote Write retries.
#[tokio::test]
async fn shared_exact_dashboard_executes_selected_workload() {
    run_shared_dashboard(false).await;
}

// A selected 5s pane serves advancing 10s lookbacks through the production HTTP path.
#[tokio::test]
async fn repeated_dashboard_executes_multiple_selected_panes() {
    run_shared_dashboard(true).await;
}

async fn run_shared_dashboard(multi_pane: bool) {
    let fallback_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fallback_address = fallback_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(fallback_listener, Router::new()
            .route("/-/healthy", get(|| async { "healthy" }))
            .route("/api/v1/query", get(|| async { Json(serde_json::json!({"status":"success","data":{"resultType":"vector","result":[]}})) })))
            .await.unwrap();
    });
    let output_dir = tempfile::tempdir().unwrap();
    let mut snapshot: Value = serde_json::from_str(include_str!(
        "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let sum = if multi_pane {
        "sum by (service) (sum_over_time(asap_demo_gauge[10s]))"
    } else {
        "sum by (service) (sum_over_time(asap_demo_gauge[5s]))"
    };
    let count = if multi_pane {
        "sum by (service) (count_over_time(asap_demo_gauge[10s]))"
    } else {
        "sum by (service) (count_over_time(asap_demo_gauge[5s]))"
    };
    let mean = format!("{sum} / {count}");
    let mut entry = snapshot["query_workload"]["repeating_queries"][2].clone();
    snapshot["query_workload"]["repeating_queries"] = Value::Array(
        [sum, count, mean.as_str()]
            .into_iter()
            .map(|query| {
                entry["query"] = query.into();
                entry.clone()
            })
            .collect(),
    );
    let mut typed: control_plane::physical::compiler::BackendLocalPlanningInput =
        serde_json::from_value(snapshot.clone()).unwrap();
    if multi_pane {
        for entry in typed.query_workload.repeating_queries.as_mut().unwrap() {
            entry.demand = planner_types::workload::RepeatedDemand::FixedIntervalAt {
                interval: planner_types::workload::RepetitionInterval(5_000),
                evaluation_phase: planner_types::workload::TimestampMs(0),
            };
        }
    }
    let (request, environment) = typed.clone().into_physical_compilation_request().unwrap();
    let candidates =
        control_plane::physical::workload_cost::enumerate_exact_and_materialized_candidates(
            request,
        )
        .unwrap();
    let quotes = candidates
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| {
            let plan = control_plane::physical::compiler::PhysicalPlanCompiler
                .compile_promql(candidate.clone(), environment.clone())
                .unwrap();
            let manifest =
                control_plane::physical::workload_cost::manifest(&plan, &candidate.queries)
                    .unwrap();
            let unit_costs = manifest
                .components
                .keys()
                .map(|key| (key.clone(), if index == 0 { 1.0 } else { 1000.0 }))
                .collect();
            control_plane::physical::workload_cost::WorkloadQuote {
                manifest,
                executable: true,
                unit_costs,
            }
        })
        .collect();
    typed.schema_version = 2;
    typed.workload_cost_evidence = Some(
        control_plane::physical::workload_cost::WorkloadCostEvidence {
            backend_revision: control_plane::physical::compiler::BACKEND_REVISION.into(),
            planner_revision: control_plane::physical::compiler::PLANNER_REVISION.into(),
            data_snapshot_id: "process-fixture-v1".into(),
            model_version: "test-only-unit-costs".into(),
            observed_at_unix_ms: environment.observed_at_unix_ms,
            valid_for_ms: environment.max_evidence_age_ms,
            quotes,
        },
    );
    snapshot = serde_json::to_value(&typed).unwrap();
    let plan = typed.compile_promql().unwrap();
    assert!(plan.cost_comparison.is_some());
    assert_eq!(plan.precompute_plan.materializations.len(), 1);
    assert_eq!(plan.query_plan.entries.len(), 3);
    assert!(plan
        .precompute_plan
        .executable_dags
        .values()
        .all(|installed| installed.document.schema_version
            == asap_types::executable_plan::MAINTENANCE_DAG_SCHEMA_VERSION));
    if multi_pane {
        assert!(plan.lifecycle_estimates[0]
            .window_realization_id
            .contains("pane"));
        for entry in plan.query_plan.entries.values() {
            assert_eq!(entry.instant.lookback_ms, 10_000);
            assert_eq!(entry.materialization_bindings()[0].window_ms, 5_000);
        }
    }
    let snapshot_path = output_dir.path().join("snapshot.json");
    std::fs::write(&snapshot_path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let port = unused_port();
    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .args(["--profile", "asapquery", "--planning-snapshot"])
        .arg(&snapshot_path)
        .args([
            "--forward-unsupported-queries",
            "--prometheus-server",
            &format!("http://{fallback_address}"),
        ])
        .args(["--http-port", &port.to_string(), "--output-dir"])
        .arg(output_dir.path())
        .args([
            "--precompute-allowed-lateness-ms",
            "0",
            "--precompute-flush-interval-ms",
            "25",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let base = now - now.rem_euclid(5000) - 20000;
    let labeled = |service: &str, instance: &str, samples: &[(i64, f64)]| {
        let mut item = series("asap_demo_gauge", samples);
        item.labels.extend([
            Label {
                name: "service".into(),
                value: service.into(),
            },
            Label {
                name: "instance".into(),
                value: instance.into(),
            },
        ]);
        item
    };
    let request = WriteRequest {
        timeseries: vec![
            if multi_pane {
                labeled(
                    "api",
                    "a",
                    &[(base, 10000.0), (base + 500, 10.0), (base + 5000, 6.0)],
                )
            } else {
                labeled("api", "a", &[(base + 500, 10.0)])
            },
            labeled(
                "api",
                "b",
                &[(base + 600, 2.0), (base + 1700, 4.0), (base + 2900, 8.0)],
            ),
            labeled("worker", "c", &[(base + 700, 9.0), (base + 1900, 15.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    let advance = WriteRequest {
        timeseries: vec![
            labeled("api", "a", &[(base + 5500, 100.0)]),
            labeled("api", "b", &[(base + 5500, 100.0)]),
            labeled("worker", "c", &[(base + 5500, 100.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &advance).await, 204);
    let final_advance = WriteRequest {
        timeseries: vec![
            if multi_pane {
                labeled("api", "a", &[(base + 10000, 7.0), (base + 10500, 1000.0)])
            } else {
                labeled("api", "a", &[(base + 10500, 1000.0)])
            },
            labeled("api", "b", &[(base + 10500, 1000.0)]),
            labeled("worker", "c", &[(base + 10500, 1000.0)]),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &final_advance).await, 204);
    if multi_pane {
        let close_third = WriteRequest {
            timeseries: vec![
                labeled("api", "a", &[(base + 15500, 9999.0)]),
                labeled("api", "b", &[(base + 15500, 9999.0)]),
                labeled("worker", "c", &[(base + 15500, 9999.0)]),
            ],
        };
        assert_eq!(remote_write(&client, &backend, &close_third).await, 204);
    }
    drain_precompute(&client, &backend).await;
    let evaluation = base + if multi_pane { 10000 } else { 5000 };
    for (query, expected) in [
        (
            sum,
            if multi_pane {
                [237.0, 124.0]
            } else {
                [24.0, 24.0]
            },
        ),
        (count, if multi_pane { [8.0, 3.0] } else { [4.0, 2.0] }),
        (
            mean.as_str(),
            if multi_pane {
                [237.0 / 8.0, 124.0 / 3.0]
            } else {
                [6.0, 12.0]
            },
        ),
    ] {
        let result = wait_for_warm_instant(
            &client,
            &backend,
            query,
            evaluation as f64 / 1000.0,
            &output_dir.path().join("query_engine.log"),
        )
        .await;
        let rows = result["data"]["result"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{query}: {result}");
        for row in rows {
            let service = row["metric"]["service"].as_str().unwrap();
            assert_eq!(row["metric"].as_object().unwrap().len(), 1);
            let value: f64 = row["value"][1].as_str().unwrap().parse().unwrap();
            assert_eq!(
                value,
                expected[usize::from(service == "worker")],
                "{query}: {result}"
            );
            assert_eq!(
                row["value"][0].as_f64().unwrap(),
                evaluation as f64 / 1000.0
            );
        }
    }
    let result: Value = client
        .get(format!("{backend}/api/v1/query_range"))
        .query(&[
            ("query", mean.clone()),
            ("start", (evaluation as f64 / 1000.0).to_string()),
            ("end", ((evaluation + 5000) as f64 / 1000.0).to_string()),
            ("step", "5".into()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(is_warm(&result), "{result}");
    for row in result["data"]["result"].as_array().unwrap() {
        let values = row["values"].as_array().unwrap();
        assert_eq!(values.len(), 2, "{result}");
        assert_eq!(
            values[1][1],
            if multi_pane {
                if row["metric"]["service"] == "api" {
                    "441.4"
                } else {
                    "550"
                }
            } else {
                "100"
            },
            "{result}"
        );
    }
    // Unaligned intervals cannot be answered by whole tumbling states.
    let result: Value = client
        .get(format!("{backend}/api/v1/query"))
        .query(&[
            ("query", mean),
            ("time", ((evaluation + 1) as f64 / 1000.0).to_string()),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !is_warm(&result),
        "partial interval was incorrectly warm: {result}"
    );
}

async fn wait_for_warm_instant(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    evaluation_seconds: f64,
    log_path: &std::path::Path,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..80 {
        last = client
            .get(format!("{base}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", evaluation_seconds.to_string()),
            ])
            .send()
            .await
            .expect("instant query")
            .json()
            .await
            .expect("instant JSON");
        if is_warm(&last) && first_value(&last, "value").is_some() {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = std::fs::read_to_string(log_path)
        .unwrap_or_else(|error| format!("log unavailable: {error}"));
    panic!("query never became warm: {query}: {last}\nbackend log:\n{log}");
}

async fn wait_for_warm_range(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    start_seconds: f64,
    end_seconds: f64,
    step_seconds: u64,
    log_path: &std::path::Path,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..80 {
        last = client
            .get(format!("{base}/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", start_seconds.to_string()),
                ("end", end_seconds.to_string()),
                ("step", step_seconds.to_string()),
            ])
            .send()
            .await
            .expect("range query")
            .json()
            .await
            .expect("range JSON");
        if is_warm(&last)
            && last["data"]["result"]
                .as_array()
                .is_some_and(|result| !result.is_empty())
        {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = std::fs::read_to_string(log_path)
        .unwrap_or_else(|error| format!("log unavailable: {error}"));
    panic!("range query never became warm: {query}: {last}\nbackend log:\n{log}");
}

#[tokio::test]
async fn collector_free_profile_serves_complete_matrix_and_falls_back_exactly() {
    let fallback_calls = Arc::new(Mutex::new(Vec::<(
        String,
        HashMap<String, String>,
        HeaderMap,
    )>::new()));
    let instant_calls = Arc::clone(&fallback_calls);
    let range_calls = Arc::clone(&fallback_calls);
    let fallback_app = Router::new()
        .route("/-/healthy", get(|| async { "Prometheus is Healthy." }))
        .route(
            "/api/v1/query",
            get(
                move |Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                    let calls = Arc::clone(&instant_calls);
                    async move {
                        calls
                            .lock()
                            .await
                            .push(("instant".into(), params.clone(), headers));
                        let timestamp = params
                            .get("time")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "vector", "result": [{
                                "metric": {"fallback": "true"},
                                "value": [timestamp, "42"]
                            }]}
                        }))
                    }
                },
            ),
        )
        .route(
            "/api/v1/query_range",
            get(
                move |Query(params): Query<HashMap<String, String>>, headers: HeaderMap| {
                    let calls = Arc::clone(&range_calls);
                    async move {
                        calls
                            .lock()
                            .await
                            .push(("range".into(), params.clone(), headers));
                        let start = params
                            .get("start")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        let end = params
                            .get("end")
                            .and_then(|value| value.parse::<f64>().ok())
                            .unwrap_or_default();
                        Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "matrix", "result": [{
                                "metric": {"fallback": "true"},
                                "values": [[start, "42"], [end, "43"]]
                            }]}
                        }))
                    }
                },
            ),
        );
    let fallback_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fallback");
    let fallback_address = fallback_listener.local_addr().expect("fallback address");
    tokio::spawn(async move {
        axum::serve(fallback_listener, fallback_app)
            .await
            .expect("serve fallback")
    });

    let backend_port = unused_port();
    let output_dir = tempfile::tempdir().expect("backend output directory");
    let mut fixture: control_plane::physical::compiler::BackendLocalPlanningInput =
        serde_json::from_str(include_str!(
            "../../docs/examples/asapquery-compatibility-demo-snapshot.json"
        ))
        .unwrap();
    // These range checks read older evaluations after finite drain.
    fixture.physical_inputs.query_retention_margin_ms = 60_000;
    let priced = quote_snapshot_for_test(fixture);
    let snapshot = output_dir.path().join("snapshot.json");
    std::fs::write(&snapshot, serde_json::to_vec(&priced).unwrap()).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .arg("--profile")
        .arg("asapquery")
        .arg("--planning-snapshot")
        .arg(&snapshot)
        .arg("--prometheus-server")
        .arg(format!("http://{fallback_address}"))
        .arg("--forward-unsupported-queries")
        .arg("--http-port")
        .arg(backend_port.to_string())
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--precompute-allowed-lateness-ms")
        .arg("0")
        .arg("--precompute-flush-interval-ms")
        .arg("25")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start production backend");
    let mut child = ChildGuard(child);
    let client = reqwest::Client::new();
    let backend = format!("http://127.0.0.1:{backend_port}");
    wait_until_ready(&client, &format!("{backend}/api/v1/health"), &mut child.0).await;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_millis() as i64;
    let base = now_ms - now_ms.rem_euclid(5_000) - 20_000;
    let request = WriteRequest {
        timeseries: vec![
            series(
                "asap_demo_counter_total",
                &[
                    (base + 500, 10.0),
                    (base + 1_700, 20.0),
                    (base + 2_900, 3.0),
                    (base + 4_200, 13.0),
                    (base + 5_400, 13.0),
                    (base + 6_600, 21.0),
                    (base + 8_100, 2.0),
                    (base + 9_400, 12.0),
                ],
            ),
            series_with_labels(
                "asap_demo_counter_total",
                &[("instance", "independent")],
                &[
                    (base + 500, 50.0),
                    (base + 1_700, 60.0),
                    (base + 2_900, 5.0),
                    (base + 4_200, 15.0),
                    // Keep both queried windows populated; missing counter
                    // panes deliberately use exact fallback.
                    (base + 5_500, 20.0),
                    (base + 6_700, 30.0),
                    (base + 7_900, 5.0),
                    (base + 9_200, 15.0),
                ],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "api")],
                &[
                    (base + 500, 1.0),
                    (base + 1_700, 2.0),
                    (base + 2_900, 3.0),
                    (base + 4_200, 4.0),
                    (base + 5_400, 5.0),
                    (base + 6_600, 6.0),
                    (base + 8_100, 7.0),
                    (base + 9_400, 8.0),
                ],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "worker")],
                &[(base + 700, 100.0), (base + 3_100, 100.0)],
            ),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "cron")],
                &[
                    (base + 900, 10.0),
                    (base + 2_100, 10.0),
                    (base + 3_700, 10.0),
                ],
            ),
            series(
                "asap_demo_latency_ms",
                &[
                    (base + 500, 10.0),
                    (base + 1_700, 20.0),
                    (base + 2_900, 30.0),
                    (base + 4_200, 40.0),
                    (base + 5_400, 15.0),
                    (base + 6_600, 25.0),
                    (base + 8_100, 35.0),
                    (base + 9_400, 45.0),
                ],
            ),
        ],
    };
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    let watermark_advance = WriteRequest {
        timeseries: vec![
            series("asap_demo_counter_total", &[(base + 10_500, 15.0)]),
            series_with_labels(
                "asap_demo_gauge",
                &[("job", "api")],
                &[(base + 10_500, 9.0)],
            ),
            series("asap_demo_latency_ms", &[(base + 10_500, 55.0)]),
        ],
    };
    assert_eq!(
        remote_write(&client, &backend, &watermark_advance).await,
        204
    );
    // A normal Prometheus retry must be accepted without changing sketches.
    assert_eq!(remote_write(&client, &backend, &request).await, 204);
    drain_precompute(&client, &backend).await;
    let corrupt = client
        .post(format!("{backend}/api/v1/write"))
        .header("content-encoding", "snappy")
        .header("content-type", "application/x-protobuf")
        .body(vec![1, 2, 3])
        .send()
        .await
        .expect("send corrupt request");
    assert_eq!(corrupt.status().as_u16(), 400);

    let first_eval = (base + 5_000) as f64 / 1_000.0;
    let second_eval = (base + 10_000) as f64 / 1_000.0;
    let backend_log = output_dir.path().join("query_engine.log");
    // Reset-aware counter panes align with the installed workload phase and
    // must serve both instant and range requests through the warm path.
    for query in [
        "rate(asap_demo_counter_total[5s])",
        "increase(asap_demo_counter_total[5s])",
    ] {
        let instant =
            wait_for_warm_instant(&client, &backend, query, first_eval, &backend_log).await;
        assert_eq!(instant["status"], "success", "{query}: {instant}");
        assert!(is_warm(&instant), "{query}: {instant}");
        let range = wait_for_warm_range(
            &client,
            &backend,
            query,
            first_eval,
            second_eval,
            5,
            &backend_log,
        )
        .await;
        assert_eq!(range["status"], "success", "{query}: {range}");
        assert!(is_warm(&range), "{query}: {range}");
    }
    // Per-series quantile now has a population-isolated producer.
    let quantile_query = "quantile_over_time(0.5, asap_demo_latency_ms[5s])";
    let quantile =
        wait_for_warm_instant(&client, &backend, quantile_query, first_eval, &backend_log).await;
    assert!(is_warm(&quantile));
    let quantile_range = wait_for_warm_range(
        &client,
        &backend,
        quantile_query,
        first_eval,
        second_eval,
        5,
        &backend_log,
    )
    .await;
    assert!(is_warm(&quantile_range));
    let sum = wait_for_warm_instant(
        &client,
        &backend,
        "sum(sum_over_time(asap_demo_gauge[5s]))",
        first_eval,
        &backend_log,
    )
    .await;
    let topk_sum = wait_for_warm_instant(
        &client,
        &backend,
        "topk(1, sum_over_time(asap_demo_gauge[5s]))",
        first_eval,
        &backend_log,
    )
    .await;
    let topk_count = wait_for_warm_instant(
        &client,
        &backend,
        "topk(1, count_over_time(asap_demo_gauge[5s]))",
        first_eval,
        &backend_log,
    )
    .await;
    assert_eq!(
        first_value(&topk_sum, "value"),
        Some(200.0),
        "value-weighted Top-K must select worker: {topk_sum}"
    );
    assert_eq!(
        first_value(&topk_count, "value"),
        Some(4.0),
        "count-weighted Top-K must select api: {topk_count}"
    );
    assert_eq!(topk_sum["data"]["result"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        topk_count["data"]["result"].as_array().map(Vec::len),
        Some(1)
    );
    assert_eq!(
        topk_sum["data"]["result"][0]["metric"]["item"],
        "asap_demo_gauge{job=\"worker\"}"
    );
    assert_eq!(
        topk_count["data"]["result"][0]["metric"]["item"],
        "asap_demo_gauge{job=\"api\"}"
    );
    assert!((first_value(&sum, "value").expect("sum value") - 240.0).abs() < 1e-9);

    for query in [
        "sum(sum_over_time(asap_demo_gauge[5s]))",
        "topk(1, sum_over_time(asap_demo_gauge[5s]))",
        "topk(1, count_over_time(asap_demo_gauge[5s]))",
    ] {
        let first_instant =
            wait_for_warm_instant(&client, &backend, query, first_eval, &backend_log).await;
        let second_instant =
            wait_for_warm_instant(&client, &backend, query, second_eval, &backend_log).await;
        let response: Value = client
            .get(format!("{backend}/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", first_eval.to_string()),
                ("end", second_eval.to_string()),
                ("step", "5".into()),
            ])
            .send()
            .await
            .expect("range query")
            .json()
            .await
            .expect("range JSON");
        assert_eq!(response["status"], "success", "{query}: {response}");
        assert!(
            is_warm(&response),
            "{query} did not use warm tier: {response}"
        );
        // Compare the complete vector at each step, including changing Top-K
        // membership. Sorting labels makes response ordering irrelevant.
        for (timestamp, instant) in [(first_eval, &first_instant), (second_eval, &second_instant)] {
            let mut expected = instant["data"]["result"]
                .as_array()
                .expect("instant vector")
                .iter()
                .map(|series| {
                    (
                        serde_json::to_string(&series["metric"]).unwrap(),
                        series["value"][1].as_str().unwrap().parse::<f64>().unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let mut actual = response["data"]["result"]
                .as_array()
                .expect("range matrix")
                .iter()
                .flat_map(|series| {
                    series["values"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(move |point| point[0].as_f64() == Some(timestamp))
                        .map(move |point| {
                            (
                                serde_json::to_string(&series["metric"]).unwrap(),
                                point[1].as_str().unwrap().parse::<f64>().unwrap(),
                            )
                        })
                })
                .collect::<Vec<_>>();
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            actual.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                actual, expected,
                "complete range/instant vector differs for {query} at {timestamp}"
            );
        }
        if query.starts_with("topk(") {
            let mut ranked_points = response["data"]["result"]
                .as_array()
                .unwrap_or_else(|| panic!("missing Top-K range result for {query}: {response}"))
                .iter()
                .flat_map(|series| {
                    series["values"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|point| {
                            (
                                point[0].as_f64().expect("Top-K timestamp"),
                                point[1]
                                    .as_str()
                                    .expect("Top-K value")
                                    .parse::<f64>()
                                    .expect("numeric Top-K value"),
                            )
                        })
                })
                .collect::<Vec<_>>();
            ranked_points.sort_by(|left, right| left.0.total_cmp(&right.0));
            let expected = if query.contains("sum_over_time") {
                vec![(first_eval, 200.0), (second_eval, 26.0)]
            } else {
                vec![(first_eval, 4.0), (second_eval, 4.0)]
            };
            assert_eq!(
                ranked_points, expected,
                "wrong Top-K windows for {query}: {response}"
            );
            continue;
        }
        let values = response["data"]["result"][0]["values"]
            .as_array()
            .unwrap_or_else(|| panic!("missing range values for {query}: {response}"));
        assert_eq!(values.len(), 2, "wrong step count for {query}: {response}");
        assert_eq!(values[0][0], first_eval);
        assert_eq!(values[1][0], second_eval);
    }

    // Readiness polling may briefly reach the exact fallback before a newly
    // closed warm window is visible. Every planned query above was required
    // to converge to its declared warm or exact tier; isolate further fallback assertions.
    fallback_calls.lock().await.clear();

    let fallback_instant: Value = client
        .get(format!("{backend}/api/v1/query"))
        .header("authorization", "Bearer demo")
        .header("x-scope-orgid", "tenant-demo")
        .query(&[
            ("query", "max(asap_unplanned)"),
            ("time", &first_eval.to_string()),
            ("timeout", "7s"),
        ])
        .send()
        .await
        .expect("fallback instant")
        .json()
        .await
        .expect("fallback instant JSON");
    assert_eq!(
        fallback_instant["data"]["result"][0]["metric"]["fallback"],
        "true"
    );
    let fallback_range: Value = client
        .get(format!("{backend}/api/v1/query_range"))
        .header("authorization", "Bearer demo")
        .header("x-scope-orgid", "tenant-demo")
        .query(&[
            ("query", "max(asap_unplanned)"),
            ("start", &first_eval.to_string()),
            ("end", &second_eval.to_string()),
            ("step", "5"),
            ("timeout", "9s"),
        ])
        .send()
        .await
        .expect("fallback range")
        .json()
        .await
        .expect("fallback range JSON");
    assert_eq!(
        fallback_range["data"]["result"][0]["metric"]["fallback"],
        "true"
    );

    // These complete expressions are NOT registered in this snapshot.
    // This tests routing fallback, not absence of operator support: registered
    // exact arithmetic is covered by shared_exact_dashboard_executes_selected_workload.
    // Never partially warm an unregistered expression using a registered child.
    let fallback_matrix = [
        // ASAPQuery #700; backend #503.
        "avg_over_time(asap_demo_gauge[5s])",
        "count(asap_demo_gauge)",
        "avg(asap_demo_gauge)",
        // ASAPQuery #629/#700; backend #432.
        "topk(5, asap_demo_gauge)",
        // ASAPQuery #256/#572/#577/#644; Planner #343, backend #504.
        "rate(asap_demo_counter_total[5s]) + rate(asap_demo_counter_total[5s])",
        "rate(asap_demo_counter_total[5s]) / 2",
        // ASAPQuery #466/#640; backend #473.
        "sum_over_time(asap_demo_gauge[10s])",
    ];
    for query in fallback_matrix {
        let response: Value = client
            .get(format!("{backend}/api/v1/query"))
            .query(&[
                ("query", query.to_string()),
                ("time", first_eval.to_string()),
            ])
            .send()
            .await
            .unwrap_or_else(|error| panic!("fallback request failed for {query}: {error}"))
            .json()
            .await
            .unwrap_or_else(|error| panic!("fallback JSON failed for {query}: {error}"));
        assert_eq!(
            response["data"]["result"][0]["metric"]["fallback"], "true",
            "unregistered matrix row must fall back atomically: {query}: {response}"
        );
    }

    let calls = fallback_calls.lock().await;
    assert_eq!(
        calls.len(),
        2 + fallback_matrix.len(),
        "planned queries unexpectedly fell back: {calls:?}"
    );
    assert_eq!(calls[0].0, "instant");
    assert_eq!(calls[0].1["query"], "max(asap_unplanned)");
    assert_eq!(calls[0].1["time"], first_eval.to_string());
    assert_eq!(calls[0].1["timeout"], "7s");
    assert_eq!(calls[0].2["authorization"], "Bearer demo");
    assert_eq!(calls[0].2["x-scope-orgid"], "tenant-demo");
    assert_eq!(calls[1].0, "range");
    assert_eq!(calls[1].1["start"], first_eval.to_string());
    assert_eq!(calls[1].1["end"], second_eval.to_string());
    assert_eq!(calls[1].1["step"], "5");
    assert_eq!(calls[1].1["timeout"], "9s");
    drop(calls);

    let status: Value = client
        .get(format!("{backend}/api/v1/physical-plan/status"))
        .send()
        .await
        .expect("physical status")
        .json()
        .await
        .expect("physical status JSON");
    assert_eq!(status["status"], "success");
    let materializations = status["materializations"]
        .as_array()
        .expect("materialization statuses");
    let planned_snapshot: control_plane::physical::compiler::BackendLocalPlanningInput =
        serde_json::from_str(&std::fs::read_to_string(&snapshot).unwrap()).unwrap();
    let planned = planned_snapshot.compile_promql().unwrap();
    // Every selected state must be serving; the Planner may share or separate
    // physical populations, so compare identities rather than a frozen count.
    let expected = planned
        .precompute_plan
        .materializations
        .iter()
        .map(|config| config.policy_fp_u64())
        .collect::<std::collections::BTreeSet<_>>();
    let actual = materializations
        .iter()
        .map(|entry| entry["materialization"].as_u64().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, expected, "{materializations:?}");
    for entry in materializations {
        assert_eq!(entry["phase"], "serving", "{entry}");
        assert!(
            entry["coverage_start_unix_ms"].as_u64().is_some(),
            "{entry}"
        );
        assert!(entry["coverage_end_unix_ms"].as_u64().is_some(), "{entry}");
    }

    // A producer definition that disagrees with the authoritative catalog must
    // be rejected before staging, leaving the serving generation unchanged.
    let mut invalid = planned;
    invalid.precompute_plan.materializations[0].slide_interval += 1;
    let artifact = serde_json::json!({"summary_catalog": invalid.summary_catalog,
        "collector_plans": invalid.collector_plans, "precompute_plan": invalid.precompute_plan,
        "transmission_plan": invalid.transmission_plan,
        "query_plan": invalid.query_plan, "storage_routing": null, "adaptation_evidence": []});
    let built = data_plane::drivers::query::servers::http::validate_and_build_runtime_plan(
        serde_json::from_value(artifact.clone()).unwrap(),
        std::sync::Arc::new(data_plane::storage_engines::types::BackendStorageRouting::empty()),
    );
    assert!(matches!(built, Err(error) if error.contains("catalog validation")));
    // HTTP may reject at an earlier successor authorization boundary;
    // either way it must leave the installed generation unchanged.
    let rejected = client
        .post(format!("{backend}/api/v1/physical-plan"))
        .json(&artifact)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status().as_u16(), 422);
    let after: Value = client
        .get(format!("{backend}/api/v1/physical-plan/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        after["plans"], status["plans"],
        "rejected artifact changed active generation"
    );

    let metrics = client
        .get(format!("{backend}/metrics"))
        .send()
        .await
        .expect("metrics request")
        .text()
        .await
        .expect("metrics body");
    assert!(metrics.contains("asap_remote_write_requests_total 4"));
    let samples = |request: &WriteRequest| {
        request
            .timeseries
            .iter()
            .map(|series| series.samples.len())
            .sum::<usize>()
    };
    assert!(metrics.contains(&format!(
        "asap_remote_write_samples_total {}",
        samples(&request) + samples(&watermark_advance)
    )));
    assert!(metrics.contains(&format!(
        "asap_remote_write_duplicates_total {}",
        samples(&request)
    )));
    assert!(metrics.contains("asap_remote_write_rejected_requests_total 1"));
}

#[path = "support/univmon_erp_process.rs"]
mod univmon_erp_process;
