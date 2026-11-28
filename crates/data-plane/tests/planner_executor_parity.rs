//! The data plane answers exactly what the Planner's executor answers for
//! the same installed plan and the same raw samples, on public PromQL
//! workloads: the #509 examples (`docs/examples/`) and ASAPPlanner's mixed and
//! sketch-plan workloads (`tests/workloads/`, copied unchanged from
//! ASAPPlanner's `tools/dag-viewer/workloads/random-mixed-seed-862/`,
//! `crates/devtools/tests/sketch_plan_snapshots/workloads/` (as
//! `sketch-plan-*.json`) and `tools/dag-viewer/memo-examples/promql/`
//! (`rangeheavy-t5-q05-q09-1h.json`)), plus the demo's dashboard workload
//! (`dashboard.json`).
//!
//! Each workload is planned by the control plane twice, for a deployment
//! that keeps raw samples and for one that does not. The data plane
//! installs the plan with `--no-forward`, takes the samples by Remote
//! Write, closes the windows with the event-time barrier and answers each
//! query over HTTP. The reference runs the same plan in process as the
//! Planner's runtime tests do: every raw sample in an `InMemoryDataStore`,
//! each refresh group refreshed in one ingestion run per window, then one
//! query run. Answers must be equal, except that a plan with a KLL
//! sketch answers within 5% (KLL seeds its compaction coin at random).
//!
//! Range queries: the mixed workloads again, each query requested as a
//! range over its last three evaluations (`range_evaluation`); the data
//! plane's matrix equals the reference's query run at each step.
//!
//! Push precompute (docs/design/parallel-precompute.md, T1 and T2): the
//! same comparison with the views built by push pipelines over K = 1, 2,
//! 4 and 8 ingest shards; and each stored view's rows equal the window
//! path's.

mod common;

use std::collections::{BTreeMap, BTreeSet};
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
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use futures::{executor::block_on, StreamExt};
use http_body_util::BodyExt;
use precompute::engine::PrecomputeMode;
use precompute::ingestion::IngestionOptions;
use prometheus_remote_write::{Label, Sample, TimeSeries};
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

/// The workloads compared; `PARITY_WORKLOAD=name` runs only the
/// workloads whose name contains it.
fn workloads() -> Vec<(String, PlanningWorkload)> {
    common::workloads(&std::env::var("PARITY_WORKLOAD").unwrap_or_default())
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
            let mut labels: Labels = [
                ("__name__", metric.clone()),
                ("job", format!("job{}", series % 3)),
                ("instance", format!("i{series}")),
                ("region", format!("r{}", series % 2)),
                ("service", format!("s{}", series % 3)),
            ]
            .into_iter()
            .map(|(n, v)| (n.to_string(), v))
            .collect();
            // A histogram's buckets: each job gets two bucket bounds.
            if metric.ends_with("_bucket") {
                labels.insert("le".into(), ["0.1", "1", "+Inf"][series / 2 % 3].into());
            }
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

/// Where the data plane under test keeps its view windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Views {
    /// Decoded in memory, as without tiered view storage.
    InMemory,
    /// In the view files, with a window cache of zero bytes, so every view
    /// read decodes its windows from the files
    /// (docs/design/tiered-view-storage.md).
    EveryReadCold,
}

/// A fresh directory for the view files of one data plane under test.
fn fresh_view_dir() -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "asap-parity-views-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The data plane running `plan` with `--no-forward`, fed `points` by
/// Remote Write (one request per minute of event time) from `start_ms`,
/// with the barrier at the evaluation time, its result cache on or off,
/// and its view windows kept as `views` says.
async fn data_plane(
    plan: &InstallablePlan,
    points: &[Point],
    start_ms: i64,
    result_cache: bool,
    views: Views,
) -> Router {
    // `PARITY_INGEST_THREADS=n` runs the data plane over `n` ingest shards.
    let ingest_threads = std::env::var("PARITY_INGEST_THREADS")
        .map_or(EngineOptions::default().ingest_threads, |n| {
            n.parse().unwrap()
        });
    let shards = Shards {
        ingest_threads,
        precompute_mode: PrecomputeMode::Window,
    };
    data_plane_over(plan, points, start_ms, result_cache, shards, views)
        .await
        .0
}

/// How the data plane under test ingests: shard count and precompute mode.
#[derive(Debug, Clone, Copy)]
struct Shards {
    ingest_threads: usize,
    precompute_mode: PrecomputeMode,
}

/// As [`data_plane`], over `shards`, with view windows kept as `views`
/// says; also returns the data plane, whose stored views T2 compares.
async fn data_plane_over(
    plan: &InstallablePlan,
    points: &[Point],
    start_ms: i64,
    result_cache: bool,
    shards: Shards,
    views: Views,
) -> (Router, Arc<DataPlane>) {
    let (view_dir, view_cache_max_bytes) = match views {
        Views::InMemory => (None, None),
        Views::EveryReadCold => (Some(fresh_view_dir()), Some(0)),
    };
    let options = EngineOptions {
        view_dir,
        view_cache_max_bytes,
        no_forward: true,
        result_cache,
        ingest_threads: shards.ingest_threads,
        precompute_mode: shards.precompute_mode,
        // `PARITY_PRECOMPUTE_THREADS=n` computes windows on `n` threads.
        precompute_threads: std::env::var("PARITY_PRECOMPUTE_THREADS")
            .map_or(EngineOptions::default().precompute_threads, |n| {
                n.parse().unwrap()
            }),
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
        common::remote_write(&app, timeseries).await;
    }
    // No time limit: the push and cold-read variants of the larger
    // workloads take minutes to close their windows.
    assert_eq!(
        common::barrier_status(app.clone(), EVALUATION_MS).await,
        StatusCode::OK
    );
    (app, state)
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

fn sample_value(value: &Json) -> f64 {
    match value.as_str().unwrap() {
        "NaN" => f64::NAN,
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        v => v.parse().unwrap(),
    }
}

/// The data plane's answer to `query` as a range from `start_ms` to
/// `end_ms` at `step_ms`: each step's answer rows, or its error.
async fn served_range(
    app: &Router,
    query: &str,
    (start_ms, end_ms, step_ms): (i64, i64, i64),
) -> Result<BTreeMap<i64, Vec<Answer>>, String> {
    let uri = format!(
        "/api/v1/query_range?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("start", &format!("{}", start_ms as f64 / 1000.0))
            .append_pair("end", &format!("{}", end_ms as f64 / 1000.0))
            .append_pair("step", &format!("{}", step_ms as f64 / 1000.0))
            .finish()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body: Json =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    if status != StatusCode::OK {
        return Err(body["error"].as_str().unwrap_or_default().to_string());
    }
    assert_eq!(body["data"]["resultType"], "matrix");
    let mut steps: BTreeMap<i64, Vec<Answer>> = (0..)
        .map(|i| start_ms + i * step_ms)
        .take_while(|t| *t <= end_ms)
        .map(|t| (t, Vec::new()))
        .collect();
    for series in body["data"]["result"].as_array().unwrap() {
        let labels: Labels = serde_json::from_value(series["metric"].clone()).unwrap();
        for value in series["values"].as_array().unwrap() {
            let time_ms = (value[0].as_f64().unwrap() * 1000.0).round() as i64;
            steps
                .get_mut(&time_ms)
                .unwrap()
                .push((labels.clone(), sample_value(&value[1])));
        }
    }
    for answers in steps.values_mut() {
        answers.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(steps)
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
        let source = Source::TimeSeries { metric };
        store = store
            .with_appended_raw_data(source.clone(), schema, None)
            .unwrap();
        // One batch covering every time: every read returns it whole.
        store
            .append_raw_data(&source, (i64::MIN, i64::MAX), vec![batch])
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
    let times = vec![vec![EVALUATION_MS]; plan.dag.roots.len()];
    reference_answers_at(plan, points, start_ms, &times)
        .into_iter()
        .map(|mut answers| answers.remove(0))
        .collect()
}

/// As [`reference_answers`], with one query run of root `i` at each of
/// `times[i]`.
fn reference_answers_at(
    plan: &InstallablePlan,
    points: &[Point],
    start_ms: i64,
    times: &[Vec<i64>],
) -> Vec<Vec<Result<Vec<Answer>, String>>> {
    let dag = &plan.dag;
    let views = plan.materialized_views().unwrap();
    let limits = Limits { max_bytes: 4 << 30 };
    let mut store = Arc::new(raw_tables(plan, points));
    let groups = plan.ingestion_refresh_groups().unwrap();
    // Each group's compiled refresh and window width, and every window
    // `(end - width, end]` of every group, run in end order (groups in plan
    // order at one end), as the data plane closes them: a view read by a
    // later group keeps only its retention, so a group cannot run all its
    // windows before the group reading it.
    let mut runs = Vec::new();
    let mut windows = Vec::new();
    for (index, group) in groups.iter().enumerate() {
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
        runs.push((compile(dag, &read, &ids).unwrap(), width));
        // The first window starting at or after `start_ms`.
        let mut end = start_ms + width + (phase - start_ms - width).rem_euclid(width);
        while end <= EVALUATION_MS {
            windows.push((end, index));
            end += width;
        }
    }
    windows.sort_unstable();
    for (end, index) in windows {
        let (compiled, width) = &runs[index];
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
            let view = groups[index]
                .views
                .iter()
                .find(|v| v.node_id as u64 == *root)
                .unwrap();
            let batches = batches.iter().map(|b| (**b).clone()).collect();
            store
                .append_materialized_view(view, (end - width, end), batches)
                .unwrap();
        }
    }
    dag.roots
        .iter()
        .zip(times)
        .map(|(&root, times)| {
            times
                .iter()
                .map(|&time_ms| reference_run(plan, &store, &views, root, time_ms))
                .collect()
        })
        .collect()
}

/// `root`'s answer from one query run at `time_ms` over `store`.
fn reference_run(
    plan: &InstallablePlan,
    store: &Arc<InMemoryDataStore>,
    views: &[asap_planner::MaterializedView],
    root: usize,
    time_ms: i64,
) -> Result<Vec<Answer>, String> {
    let limits = Limits { max_bytes: 4 << 30 };
    let read: Vec<_> = views
        .iter()
        .filter(|v| reaches_without_passing_a_view(plan, root, v.node_id, views))
        .cloned()
        .collect();
    let compiled = compile(&plan.dag, &read, &[root as u64]).map_err(|e| e.to_string())?;
    let instance = compiled
        .instantiate(store.clone())
        .map_err(|e| e.to_string())?;
    let context = RunContext::new(
        Scope::Query {
            evaluation_time_ms: time_ms,
        },
        limits,
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
/// columns, the Float64 value (Int64 for a `count`); sorted by labels.
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
                    // A `count` answers in integers (no Float64 column then).
                    Value::Int64(v) => value = *v as f64,
                    _ => {}
                }
            }
            (labels, value)
        })
        .collect();
    answers.sort_by(|a, b| a.0.cmp(&b.0));
    answers
}

/// Whether two answers are equal: the same series with the same values,
/// within `relative` of each other.
/// Both answers within `relative` of each other, or both errors.
fn same_answers(
    a: &Result<Vec<Answer>, String>,
    b: &Result<Vec<Answer>, String>,
    relative: f64,
) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => same_within(a, b, relative),
        (Err(_), Err(_)) => true,
        _ => false,
    }
}

/// Both range answers equal at every step within `relative`, or both
/// errors.
fn same_matrices(
    a: &Result<BTreeMap<i64, Vec<Answer>>, String>,
    b: &Result<BTreeMap<i64, Vec<Answer>>, String>,
    relative: f64,
) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => {
            a.len() == b.len()
                && a.iter().all(|(time_ms, rows)| {
                    b.get(time_ms)
                        .is_some_and(|other| same_within(rows, other, relative))
                })
        }
        (Err(_), Err(_)) => true,
        _ => false,
    }
}

fn same_within(a: &[Answer], b: &[Answer], relative: f64) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.0 == y.0
                && (x.1 == y.1
                    || (x.1.is_nan() && y.1.is_nan())
                    || (x.1 - y.1).abs() <= relative * y.1.abs().max(1.0))
        })
}

/// How far apart two runs of `plan` may answer. A KLL sketch seeds its
/// compaction coin at random, so two builds of the same window over enough
/// samples differ, within the sketch's error; every other state is
/// deterministic.
fn tolerance(plan: &InstallablePlan) -> f64 {
    let randomized = serde_json::to_string(&plan.dag)
        .unwrap()
        .contains("\"KLL\"");
    if randomized {
        0.05
    } else {
        1e-9
    }
}

/// Every public workload, planned with and without raw samples kept: each
/// query the reference answers, the data plane answers the same; each
/// query the reference cannot answer, the data plane does not answer
/// either. At least ten workload plans are compared query by query.
#[tokio::test(flavor = "multi_thread")]
async fn the_data_plane_answers_what_the_planner_executor_answers() {
    compare_instant_answers(Views::InMemory).await;
}

/// V1 (docs/design/tiered-view-storage.md): the same comparison with every
/// view window read cold from the view files.
#[tokio::test(flavor = "multi_thread")]
async fn with_every_view_read_cold_the_data_plane_answers_what_the_planner_executor_answers() {
    compare_instant_answers(Views::EveryReadCold).await;
}

async fn compare_instant_answers(views: Views) {
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
            let app = data_plane(&plan, &points, start_ms, true, views).await;
            let uncached = data_plane(&plan, &points, start_ms, false, views).await;
            let mut answered = 0;
            let mut non_empty = 0;
            let tolerance = tolerance(&plan);
            for (query, expected) in plan.query_texts.iter().zip(&expected) {
                let served = served_answer(&app, query).await;
                // Asked again: from the result cache, the same answer; and
                // the same as without the cache.
                let again = served_answer(&app, query).await;
                let without_cache = served_answer(&uncached, query).await;
                if !same_answers(&again, &served, 0.0)
                    || !same_answers(&without_cache, &served, tolerance)
                {
                    mismatches.push(format!(
                        "{label}: {query}: cached {again:?}, first {served:?}, without the cache {without_cache:?}"
                    ));
                }
                match (expected, &served) {
                    (Ok(expected), Ok(served)) if same_within(served, expected, tolerance) => {
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
        compared_plans >= 10 || std::env::var("PARITY_WORKLOAD").is_ok(),
        "only {compared_plans} plans compared ({compared_queries} queries)"
    );
}

/// Steps per range request in the range comparison.
const RANGE_STEPS: u64 = 3;

/// `workload` with each repeating query requested as a range over its last
/// `RANGE_STEPS` evaluations, at its interval; and each query's step.
fn with_range_evaluation(workload: &PlanningWorkload) -> (PlanningWorkload, Vec<i64>) {
    let mut json = serde_json::to_value(workload).unwrap();
    let mut steps = Vec::new();
    for entry in json["query_workload"]["repeating_queries"]
        .as_array_mut()
        .unwrap()
    {
        let step_ms = entry["demand"]["fixed_interval_at"]["interval"]
            .as_u64()
            .unwrap();
        entry["range_evaluation"] =
            serde_json::json!({"step_ms": step_ms, "span_ms": step_ms * (RANGE_STEPS - 1)});
        steps.push(step_ms as i64);
    }
    (serde_json::from_value(json).unwrap(), steps)
}

/// The mixed workloads with every query requested as a range: at each
/// step, the data plane's matrix holds what the reference's query run at
/// that time answers, or neither answers.
#[tokio::test(flavor = "multi_thread")]
async fn range_queries_answer_what_the_planner_executor_answers_at_each_step() {
    compare_range_answers(Views::InMemory).await;
}

/// V1 (docs/design/tiered-view-storage.md): the same comparison with every
/// view window read cold from the view files, steps sharing the windows
/// they decode through the window cache.
#[tokio::test(flavor = "multi_thread")]
async fn with_every_view_read_cold_range_queries_answer_what_the_planner_executor_answers() {
    compare_range_answers(Views::EveryReadCold).await;
}

async fn compare_range_answers(views: Views) {
    let mut compared_steps = 0;
    let mut non_empty_steps = 0;
    let mut report = Vec::new();
    let mut mismatches = Vec::new();
    for (name, workload) in workloads().into_iter().filter(|(name, _)| {
        name.starts_with("random-mixed-seed-862-0") && name.as_str() <= "random-mixed-seed-862-03"
    }) {
        let (workload, steps) = with_range_evaluation(&workload);
        for raw_data_retained in [true, false] {
            let label = format!("{name} as ranges (raw data retained: {raw_data_retained})");
            let mut inputs = DeploymentInputs::default();
            inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
            // Each range is asked once, cold, after all its data is in: the
            // result cache holds none of its earlier steps, so the plan
            // must keep every step's inputs (no final_query_results_cached).
            inputs.deployment_capabilities.final_query_results_cached = false;
            let plan = match plan_workload(&workload, &inputs) {
                Ok((plan, _)) => plan,
                Err(error) => {
                    report.push(format!("{label}: not planned: {error}"));
                    continue;
                }
            };
            let start_ms = EVALUATION_MS - reach_back_ms(&plan);
            let points = points(&plan, start_ms);
            let times: Vec<Vec<i64>> = steps
                .iter()
                .map(|step| {
                    (0..RANGE_STEPS as i64)
                        .rev()
                        .map(|i| EVALUATION_MS - i * step)
                        .collect()
                })
                .collect();
            let expected = reference_answers_at(&plan, &points, start_ms, &times);
            let uncached = data_plane(&plan, &points, start_ms, false, views).await;
            let app = data_plane(&plan, &points, start_ms, true, views).await;
            let mut equal = 0;
            for ((query, expected), (step, times)) in plan
                .query_texts
                .iter()
                .zip(&expected)
                .zip(steps.iter().zip(&times))
            {
                let range = (times[0], EVALUATION_MS, *step);
                let served = served_range(&app, query, range).await;
                // Asked again: every step from the result cache, the same
                // matrix; and the same as without the cache.
                let again = served_range(&app, query, range).await;
                let without_cache = served_range(&uncached, query, range).await;
                if !same_matrices(&again, &served, 0.0)
                    || !same_matrices(&without_cache, &served, tolerance(&plan))
                {
                    mismatches.push(format!(
                        "{label}: {query}: cached {again:?}, first {served:?}, without the cache {without_cache:?}"
                    ));
                }
                match served {
                    Ok(served) => {
                        for (time_ms, expected) in times.iter().zip(expected) {
                            let served = &served[time_ms];
                            match expected {
                                Ok(expected) if same_within(served, expected, tolerance(&plan)) => {
                                    equal += 1;
                                    non_empty_steps += usize::from(!served.is_empty());
                                }
                                _ => mismatches.push(format!(
                                    "{label}: {query} at {time_ms}:\n  data plane {served:?}\n  reference  {expected:?}"
                                )),
                            }
                        }
                    }
                    Err(error) if expected.iter().any(Result::is_err) => report.push(format!(
                        "{label}: {query}: neither answers every step: {error}"
                    )),
                    Err(error) => mismatches.push(format!(
                        "{label}: {query}: data plane {error}, reference {expected:?}"
                    )),
                }
            }
            compared_steps += equal;
            report.push(format!(
                "{label}: {equal}/{} steps equal",
                plan.query_texts.len() * RANGE_STEPS as usize
            ));
        }
    }
    println!("{}", report.join("\n"));
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(
        compared_steps >= 100 || std::env::var("PARITY_WORKLOAD").is_ok(),
        "only {compared_steps} steps compared"
    );
    assert!(
        non_empty_steps * 10 >= compared_steps * 9,
        "only {non_empty_steps} of {compared_steps} equal steps have rows"
    );
}

/// T1. Push precompute answers what the Planner's executor answers (and so
/// what the window path answers, which the test above checks) on every
/// parity workload, raw data kept and not kept, over K = 1, 2, 4 and 8
/// ingest shards. KLL plans within 5%, as above.
#[tokio::test(flavor = "multi_thread")]
async fn push_precompute_answers_what_the_planner_executor_answers() {
    let mut mismatches = Vec::new();
    let mut compared = 0;
    // `PARITY_PUSH_SHARDS=1,4,8,12` overrides the shard counts.
    let shard_counts: Vec<usize> = std::env::var("PARITY_PUSH_SHARDS")
        .map(|v| v.split(',').map(|n| n.parse().unwrap()).collect())
        .unwrap_or_else(|_| vec![1, 2, 4, 8]);
    for ingest_threads in shard_counts {
        let shards = Shards {
            ingest_threads,
            precompute_mode: PrecomputeMode::Push,
        };
        for (name, workload) in workloads() {
            for raw_data_retained in [true, false] {
                let label = format!("{name} (raw kept: {raw_data_retained}, K = {ingest_threads})");
                let Ok((plan, _)) = plan_workload(&workload, &common::inputs(raw_data_retained))
                else {
                    continue;
                };
                let start_ms = EVALUATION_MS - reach_back_ms(&plan);
                let points = points(&plan, start_ms);
                let expected = reference_answers(&plan, &points, start_ms);
                let (app, _state) = data_plane_over(
                    &plan,
                    &points,
                    start_ms,
                    false,
                    shards,
                    Views::EveryReadCold,
                )
                .await;
                for (query, expected) in plan.query_texts.iter().zip(&expected) {
                    let served = served_answer(&app, query).await;
                    match (expected, &served) {
                        (Ok(expected), Ok(served))
                            if same_within(served, expected, tolerance(&plan)) =>
                        {
                            compared += 1
                        }
                        (Err(_), Err(_)) => {}
                        _ => mismatches.push(format!(
                            "{label}: {query}:\n  push      {served:?}\n  reference {expected:?}"
                        )),
                    }
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(compared >= 40, "only {compared} answers compared");
}

/// One stored view window, comparable across two data planes: its range
/// and its rows, each printed and the rows sorted (so by keys).
type StoredWindow = ((i64, i64), Vec<String>);

/// Every stored view's windows, by view.
fn stored_views(state: &DataPlane) -> BTreeMap<String, Vec<StoredWindow>> {
    state
        .engine
        .snapshot()
        .view_rows()
        .into_iter()
        .map(|(view, windows)| {
            let windows = windows
                .into_iter()
                .map(|(range, batches)| {
                    let mut rows: Vec<String> = batches
                        .iter()
                        .flat_map(Batch::to_rows)
                        .map(|row| format!("{row:?}"))
                        .collect();
                    rows.sort();
                    (range, rows)
                })
                .collect();
            (format!("{view}"), windows)
        })
        .collect()
}

/// A stored window's range and row count.
type WindowShape = ((i64, i64), usize);

/// The stored windows' ranges and row counts only, for views whose states
/// may differ within the plan's target (KLL compacts at random).
fn shapes(views: &BTreeMap<String, Vec<StoredWindow>>) -> BTreeMap<String, Vec<WindowShape>> {
    views
        .iter()
        .map(|(view, windows)| {
            let shape = windows
                .iter()
                .map(|(range, rows)| (*range, rows.len()))
                .collect();
            (view.clone(), shape)
        })
        .collect()
}

/// `served`'s windows of each view from the oldest window `expected` holds
/// of it. Windows past a view's retention leave the store lazily (at the
/// view's next append), and a narrower level is held until its readers
/// commit, so how many such windows linger at the end of a run depends on
/// the order windows were committed in, which differs between push and the
/// window path; and windows that far back are skipped when the watermark
/// jumps past them, which depends on when each shard reports. Neither is
/// read by a query.
fn from_oldest_of(
    served: BTreeMap<String, Vec<StoredWindow>>,
    expected: &BTreeMap<String, Vec<StoredWindow>>,
) -> BTreeMap<String, Vec<StoredWindow>> {
    served
        .into_iter()
        .map(|(view, windows)| {
            let oldest = expected
                .get(&view)
                .and_then(|w| w.first())
                .map_or(i64::MIN, |w| w.0 .1);
            let windows = windows.into_iter().filter(|w| w.0 .1 >= oldest).collect();
            (view, windows)
        })
        .collect()
}

/// Where two data planes' stored views first differ, for the failure
/// message: the view, and the window's range and rows on each side.
fn first_difference(
    expected: &BTreeMap<String, Vec<StoredWindow>>,
    served: &BTreeMap<String, Vec<StoredWindow>>,
) -> String {
    let views: std::collections::BTreeSet<&String> = expected.keys().chain(served.keys()).collect();
    for view in views {
        let (left, right) = (expected.get(view), served.get(view));
        let (Some(left), Some(right)) = (left, right) else {
            return format!(
                "view {view} only in one ({:?} / {:?})",
                left.map(Vec::len),
                right.map(Vec::len)
            );
        };
        for at in 0..left.len().max(right.len()) {
            let (l, r) = (left.get(at), right.get(at));
            if l != r {
                let show = |w: Option<&StoredWindow>| {
                    w.map(|(range, rows)| {
                        format!("{range:?} {} rows, first {:?}", rows.len(), rows.first())
                    })
                };
                let span = |w: &[StoredWindow]| (w.first().map(|w| w.0), w.last().map(|w| w.0));
                return format!(
                    "view {view} window #{at} of {}/{}: window path {:?}, push {:?}; windows: window path {:?}, push {:?}",
                    left.len(),
                    right.len(),
                    show(l),
                    show(r),
                    span(left),
                    span(right)
                );
            }
        }
    }
    "shapes differ".into()
}

/// T2. Push precompute stores the same view rows as the window path: per
/// (view, window), the same keys and the same states (exact values, or
/// sketch bytes). KLL plans store the same windows and keys; their states
/// may differ within the target, which T1 checks through the answers.
#[tokio::test(flavor = "multi_thread")]
async fn push_precompute_stores_the_window_paths_view_rows() {
    let window = Shards {
        ingest_threads: 4,
        precompute_mode: PrecomputeMode::Window,
    };
    let push = Shards {
        ingest_threads: 4,
        precompute_mode: PrecomputeMode::Push,
    };
    let mut mismatches = Vec::new();
    let mut compared_views = 0;
    for (name, workload) in workloads() {
        for raw_data_retained in [true, false] {
            let label = format!("{name} (raw kept: {raw_data_retained})");
            let Ok((plan, _)) = plan_workload(&workload, &common::inputs(raw_data_retained)) else {
                continue;
            };
            let start_ms = EVALUATION_MS - reach_back_ms(&plan);
            let points = points(&plan, start_ms);
            let (_, by_window) =
                data_plane_over(&plan, &points, start_ms, false, window, Views::InMemory).await;
            let (_, by_push) =
                data_plane_over(&plan, &points, start_ms, false, push, Views::InMemory).await;
            let expected = stored_views(&by_window);
            let served = from_oldest_of(stored_views(&by_push), &expected);
            compared_views += expected.len();
            let equal = if tolerance(&plan) > 1e-9 {
                shapes(&expected) == shapes(&served)
            } else {
                expected == served
            };
            if !equal {
                mismatches.push(format!(
                    "{label}: stored views differ: {}; stats: window path {:?}, push {:?}",
                    first_difference(&expected, &served),
                    by_window.engine.stats(),
                    by_push.engine.stats()
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(compared_views >= 20, "only {compared_views} views compared");
}
