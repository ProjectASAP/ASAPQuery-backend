//! The data plane answers exactly what the Planner's executor answers for
//! the same installed plan and the same raw samples, on public PromQL
//! workloads: the #509 examples (`examples/`) and ASAPPlanner's mixed and
//! sketch-plan workloads (`tests/workloads/`, copied unchanged from
//! ASAPPlanner's `tools/dag-viewer/workloads/random-mixed-seed-862/` and
//! `crates/devtools/tests/sketch_plan_snapshots/workloads/`).
//!
//! Each workload is planned by the control plane twice, for a deployment
//! that keeps raw samples and for one that does not. The data plane
//! installs the plan with `--no-forward`, takes the samples by Remote
//! Write, closes the windows with the event-time barrier and answers each
//! query over HTTP. The reference runs the same plan in process as the
//! Planner's runtime tests do: every raw sample in an `InMemoryDataStore`,
//! each refresh group refreshed in one ingestion run per window, then one
//! query run.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use asap_executor::physical_planner::compile;
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::sources::InMemoryDataStore;
use asap_executor::values::{Batch, Value};
use asap_planner::{InstallablePlan, PlanningWorkload};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use futures::{executor::block_on, StreamExt};
use http_body_util::BodyExt;
use serde_json::Value as Json;
use tower::ServiceExt;

/// The evaluation time: a whole hour in the past, on every window grid.
const EVALUATION_MS: i64 = 1_699_999_200_000;

/// Series per metric.
const SERIES: usize = 6;

type Labels = BTreeMap<String, String>;
/// One answer row: its series labels and value.
type Answer = (Labels, f64);

struct Point {
    labels: Labels,
    timestamp_ms: i64,
    value: f64,
}

fn workloads() -> Vec<(String, PlanningWorkload)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<_> = ["../examples", "tests/workloads"]
        .iter()
        .flat_map(|dir| std::fs::read_dir(root.join(dir)).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(&path).unwrap();
            (name, serde_json::from_str(&text).unwrap())
        })
        .collect()
}

/// Every metric the plan scans, with the union of its `Scan`s' columns.
fn scanned_schemas(plan: &InstallablePlan) -> BTreeMap<String, Schema> {
    let mut schemas: BTreeMap<String, Schema> = BTreeMap::new();
    for node in &plan.dag.nodes {
        let Operator::NonASAP(NonASAPOp::Scan {
            source: Source::TimeSeries { metric },
            schema,
            ..
        }) = &node.payload
        else {
            continue;
        };
        let known = schemas
            .entry(metric.clone())
            .or_insert_with(|| schema.clone());
        for field in &schema.fields {
            if !known.fields.iter().any(|f| f.name == field.name) {
                known.fields.push(field.clone());
            }
        }
    }
    schemas
}

/// How far back from the evaluation the plan reads, at most: its views'
/// and scans' retention plus its widest refresh window.
fn reach_back_ms(plan: &InstallablePlan) -> i64 {
    let retention = plan
        .dag
        .nodes
        .iter()
        .filter_map(|n| n.retention_ms)
        .max()
        .unwrap_or(0);
    let widest = plan
        .ingestion_refresh_groups()
        .unwrap()
        .iter()
        .filter_map(|g| g.refresh_window.map(|w| w.width_ms))
        .max()
        .unwrap_or(0);
    (retention + 2 * widest).max(600_000) as i64
}

/// `SERIES` series of each scanned metric, every 10 s from `start_ms`
/// (exclusive) through the evaluation: a counter (`*_total`) grows, any
/// other metric takes pseudo-random values, some negative.
fn points(plan: &InstallablePlan, start_ms: i64) -> Vec<Point> {
    let mut points = Vec::new();
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    for metric in scanned_schemas(plan).keys() {
        let counter = metric.ends_with("_total");
        for series in 0..SERIES {
            let labels: Labels = [
                ("__name__", metric.clone()),
                ("job", format!("job{}", series % 3)),
                ("instance", format!("i{series}")),
                ("region", format!("r{}", series % 2)),
                ("service", format!("s{}", series % 3)),
            ]
            .into_iter()
            .map(|(n, v)| (n.to_string(), v))
            .collect();
            let mut total = 0.0;
            let mut t = start_ms + 10_000;
            while t <= EVALUATION_MS {
                let value = if counter {
                    total += (next() * 10.0).floor();
                    total
                } else {
                    (next() * 1000.0).round() - 100.0
                };
                points.push(Point {
                    labels: labels.clone(),
                    timestamp_ms: t,
                    value,
                });
                t += 10_000;
            }
        }
    }
    points.sort_by_key(|p| p.timestamp_ms);
    points
}

// ── The data plane ─────────────────────────────────────────────────────

/// The data plane running `plan` with `--no-forward`, fed `points` by
/// Remote Write (one request per minute of event time) from `start_ms`,
/// with the barrier at the evaluation time.
async fn data_plane(plan: &InstallablePlan, points: &[Point], start_ms: i64) -> Router {
    let options = EngineOptions {
        no_forward: true,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            max_future_ms: 0,
            stream_start_ms: Some(start_ms),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    let mut by_minute: BTreeMap<i64, BTreeMap<&Labels, Vec<Sample>>> = BTreeMap::new();
    for point in points {
        by_minute
            .entry(point.timestamp_ms.div_euclid(60_000))
            .or_default()
            .entry(&point.labels)
            .or_default()
            .push(Sample {
                timestamp: point.timestamp_ms,
                value: point.value,
            });
    }
    for by_series in by_minute.into_values() {
        let timeseries = by_series
            .into_iter()
            .map(|(labels, samples)| TimeSeries {
                labels: labels
                    .iter()
                    .map(|(name, value)| Label {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                samples,
            })
            .collect();
        let written = app
            .clone()
            .oneshot(
                Request::post("/api/v1/write")
                    .body(Body::from(encode(&WriteRequest { timeseries })))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(written.status(), StatusCode::NO_CONTENT);
    }
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={EVALUATION_MS}"
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
    app
}

/// The data plane's answer to `query` at the evaluation time, or its error.
async fn served_answer(app: &Router, query: &str) -> Result<Vec<Answer>, String> {
    let uri = format!(
        "/api/v1/query?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("time", &format!("{}", EVALUATION_MS as f64 / 1000.0))
            .finish()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let execution = response.headers().get(EXECUTION_HEADER).cloned();
    let body: Json =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    if status != StatusCode::OK {
        return Err(body["error"].as_str().unwrap_or_default().to_string());
    }
    assert_eq!(execution.unwrap(), "plan");
    let mut answers: Vec<Answer> = body["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|series| {
            let labels: Labels = serde_json::from_value(series["metric"].clone()).unwrap();
            let value: f64 = match series["value"][1].as_str().unwrap() {
                "NaN" => f64::NAN,
                "+Inf" => f64::INFINITY,
                "-Inf" => f64::NEG_INFINITY,
                v => v.parse().unwrap(),
            };
            (labels, value)
        })
        .collect();
    answers.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(answers)
}

// ── The reference: the Planner's executor in process ───────────────────

fn raw_tables(plan: &InstallablePlan, points: &[Point]) -> InMemoryDataStore {
    let mut store = InMemoryDataStore::default();
    for (metric, schema) in scanned_schemas(plan) {
        let schema = Arc::new(schema);
        let rows = points
            .iter()
            .filter(|p| p.labels["__name__"] == metric)
            .map(|point| {
                schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| match field.name.as_str() {
                        PROMQL_SERIES_IDENTITY => {
                            Value::Utf8(serde_json::to_string(&point.labels).unwrap().into())
                        }
                        _ if Some(index) == schema.time_index => {
                            Value::Timestamp(point.timestamp_ms)
                        }
                        "value" if field.dtype == FieldDataType::Plain(DataType::Float64) => {
                            Value::Float64(point.value)
                        }
                        label => {
                            Value::Utf8(point.labels.get(label).cloned().unwrap_or_default().into())
                        }
                    })
                    .collect()
            })
            .collect();
        let batch = Batch::try_new(schema.clone(), rows).unwrap();
        store = store
            .with_raw_data(Source::TimeSeries { metric }, schema, vec![batch])
            .unwrap();
    }
    store
}

/// Each query's answers (or the executor's error) from the plan run in
/// process over `points`: every refresh group's windows `(e - W, e]` with
/// `e ≡ p (mod W)`, `start_ms <= e - W` and `e <= T`, then one query run
/// at `T` per root.
fn reference_answers(
    plan: &InstallablePlan,
    points: &[Point],
    start_ms: i64,
) -> Vec<Result<Vec<Answer>, String>> {
    let dag = &plan.dag;
    let views = plan.materialized_views().unwrap();
    let limits = Limits { max_bytes: 4 << 30 };
    let mut store = Arc::new(raw_tables(plan, points));
    for group in plan.ingestion_refresh_groups().unwrap() {
        let window = group.refresh_window.unwrap();
        let (width, phase) = (window.width_ms as i64, window.phase_ms as i64);
        let ids: Vec<u64> = group.views.iter().map(|v| v.node_id as u64).collect();
        let in_group: BTreeSet<_> = group.views.iter().map(|v| v.node_id).collect();
        let read: Vec<_> = views
            .iter()
            .filter(|v| {
                !in_group.contains(&v.node_id)
                    && group
                        .views
                        .iter()
                        .any(|g| g.inputs.materialized_views.contains(&v.node_id))
            })
            .cloned()
            .collect();
        let compiled = compile(dag, &read, &ids).unwrap();
        // The first window starting at or after `start_ms`.
        let mut end = start_ms + width + (phase - start_ms - width).rem_euclid(width);
        while end <= EVALUATION_MS {
            let outputs = {
                let instance = compiled.instantiate(store.clone()).unwrap();
                let context = RunContext::new(
                    Scope::Ingestion {
                        window_start_ms: end - width,
                        window_end_ms: end,
                    },
                    limits.clone(),
                )
                .unwrap();
                block_on(
                    instance
                        .execute_ingestion(compiled.roots(), context)
                        .unwrap(),
                )
                .unwrap()
            };
            let store = Arc::get_mut(&mut store).unwrap();
            for (root, batches) in compiled.roots().iter().zip(outputs) {
                let view = group
                    .views
                    .iter()
                    .find(|v| v.node_id as u64 == *root)
                    .unwrap();
                let batches = batches.iter().map(|b| (**b).clone()).collect();
                store
                    .append_materialized_view(view, (end - width, end), batches)
                    .unwrap();
            }
            end += width;
        }
    }
    dag.roots
        .iter()
        .map(|&root| {
            let read: Vec<_> = views
                .iter()
                .filter(|v| reaches_without_passing_a_view(plan, root, v.node_id, &views))
                .cloned()
                .collect();
            let compiled = compile(dag, &read, &[root as u64]).map_err(|e| e.to_string())?;
            let instance = compiled
                .instantiate(store.clone())
                .map_err(|e| e.to_string())?;
            let context = RunContext::new(
                Scope::Query {
                    evaluation_time_ms: EVALUATION_MS,
                },
                limits.clone(),
            )
            .unwrap();
            let schema = compiled.output_schema(root as u64).unwrap();
            let batches = block_on(async {
                let mut output = instance
                    .execute(compiled.roots(), context)
                    .map_err(|e| e.to_string())?
                    .remove(0);
                let mut batches = Vec::new();
                while let Some(batch) = output.next().await {
                    batches.push((*batch.map_err(|e| e.to_string())?).clone());
                }
                Ok::<_, String>(batches)
            })?;
            Ok(answers(&schema, &batches))
        })
        .collect()
}

/// Whether `view` is a view slot of `root`'s query run: reached from it
/// without passing another view.
fn reaches_without_passing_a_view(
    plan: &InstallablePlan,
    root: usize,
    view: usize,
    views: &[asap_planner::MaterializedView],
) -> bool {
    let is_view = |id: usize| views.iter().any(|v| v.node_id == id);
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if id == view {
            return true;
        }
        if is_view(id) {
            continue;
        }
        pending.extend(
            plan.dag
                .edges
                .iter()
                .filter(|e| e.consumer == id)
                .map(|e| e.producer),
        );
    }
    false
}

/// Answer rows: the series identity's labels and other non-empty text
/// columns, the Float64 value; sorted by labels.
fn answers(schema: &Schema, batches: &[Batch]) -> Vec<Answer> {
    let mut answers: Vec<Answer> = batches
        .iter()
        .flat_map(Batch::to_rows)
        .map(|row| {
            let mut labels = Labels::new();
            let mut value = f64::NAN;
            for (field, cell) in schema.fields.iter().zip(&row) {
                match cell {
                    Value::Utf8(text) if field.name == PROMQL_SERIES_IDENTITY => {
                        let identity: Labels = serde_json::from_str(text).unwrap();
                        labels.extend(identity);
                    }
                    Value::Utf8(text) if !text.is_empty() => {
                        labels.insert(field.name.clone(), text.to_string());
                    }
                    Value::Float64(v) => value = *v,
                    _ => {}
                }
            }
            (labels, value)
        })
        .collect();
    answers.sort_by(|a, b| a.0.cmp(&b.0));
    answers
}

fn same(a: &[Answer], b: &[Answer]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.0 == y.0
                && (x.1 == y.1
                    || (x.1.is_nan() && y.1.is_nan())
                    || (x.1 - y.1).abs() <= 1e-9 * y.1.abs().max(1.0))
        })
}

/// Every public workload, planned with and without raw samples kept: each
/// query the reference answers, the data plane answers the same; each
/// query the reference cannot answer, the data plane does not answer
/// either. At least ten workload plans are compared query by query.
#[tokio::test(flavor = "multi_thread")]
async fn the_data_plane_answers_what_the_planner_executor_answers() {
    let mut compared_plans = 0;
    let mut compared_queries = 0;
    let mut compared_non_empty = 0;
    let mut report = Vec::new();
    let mut mismatches = Vec::new();
    for (name, workload) in workloads() {
        for raw_data_retained in [true, false] {
            let label = format!("{name} (raw data retained: {raw_data_retained})");
            let mut inputs = DeploymentInputs::default();
            inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
            let plan = match plan_workload(&workload, &inputs) {
                Ok((plan, _)) => plan,
                Err(error) => {
                    report.push(format!("{label}: not planned: {error}"));
                    continue;
                }
            };
            let start_ms = EVALUATION_MS - reach_back_ms(&plan);
            let points = points(&plan, start_ms);
            let expected = reference_answers(&plan, &points, start_ms);
            let app = data_plane(&plan, &points, start_ms).await;
            let mut answered = 0;
            let mut non_empty = 0;
            for (query, expected) in plan.query_texts.iter().zip(&expected) {
                let served = served_answer(&app, query).await;
                match (expected, &served) {
                    (Ok(expected), Ok(served)) if same(served, expected) => {
                        answered += 1;
                        non_empty += usize::from(!served.is_empty());
                    }
                    (Err(_), Err(_)) => {
                        report.push(format!("{label}: {query}: neither answers: {expected:?}"))
                    }
                    _ => mismatches.push(format!(
                        "{label}: {query}:\n  data plane {served:?}\n  reference  {expected:?}"
                    )),
                }
            }
            compared_queries += answered;
            compared_non_empty += non_empty;
            if answered > 0 {
                compared_plans += 1;
            }
            report.push(format!(
                "{label}: {answered}/{} queries equal ({non_empty} with rows), {} nodes, {} views",
                plan.query_texts.len(),
                plan.dag.nodes.len(),
                plan.materialized_views().unwrap().len()
            ));
        }
    }
    println!("{}", report.join("\n"));
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    // Equal answers must not be equal only because both are empty.
    assert!(
        compared_non_empty * 10 >= compared_queries * 9,
        "only {compared_non_empty} of {compared_queries} equal answers have rows"
    );
    assert!(
        compared_plans >= 10,
        "only {compared_plans} plans compared ({compared_queries} queries)"
    );
}
