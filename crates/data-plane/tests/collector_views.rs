//! Collector views (docs/design/collector-sketch-ingest.md): a view of the
//! plan a collector builds arrives as OTLP states on `/v1/metrics`, is
//! checked, staged, deduplicated and merged, committed once the wall clock
//! passes its window's end by the commit grace, and answers queries as the
//! view the data plane builds from the same samples does.
//!
//! The OTLP bodies are built as the collector's OTAP codec writes a
//! summary point, with the view identity in the metric's metadata and the
//! state as `asap_sketchlib`'s ASAPv1 bytes of a `DdSketch` fed the samples
//! of one series and window, as the collector's processor builds it.

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use asap_planner::InstallablePlan;
use asap_sketchlib::DdSketch;
use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use otlp::proto::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use otlp::proto::common::v1::{any_value, AnyValue, KeyValue};
use otlp::proto::metrics::v1::{
    metric::Data, Metric, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
};
use precompute::collector::CollectorViewOptions;
use precompute::engine::PrecomputeMode;
use precompute::ingestion::IngestionOptions;
use prometheus_remote_write::{Label, Sample, TimeSeries};
use serde_json::Value;
use tower::ServiceExt;

const QUERY: &str = "quantile_over_time(0.99, data[5m])";
const JOBS: [&str; 2] = ["a", "b"];
const RANGE_MS: i64 = 300_000;

/// The plan of [`QUERY`] (a DDSketch chain over `data`, the plan's one
/// view), with the chain's node id, width and DDSketch α.
fn plan() -> (InstallablePlan, usize, i64, f64) {
    let (plan, node, width_ms, alpha) = plan_for(&common::inputs_without_levels(false));
    assert_eq!(plan.materialized_views().unwrap().len(), 1);
    (plan, node, width_ms, alpha)
}

/// The plan of [`QUERY`] for `inputs`, with its DDSketch chain's node id,
/// width and α.
fn plan_for(inputs: &control_plane::DeploymentInputs) -> (InstallablePlan, usize, i64, f64) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/workloads/sketch-plan-quantile-single.json");
    let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let plan = plan_workload(&workload, inputs).unwrap().0;
    let chain = plan
        .dag
        .nodes
        .iter()
        .map(|node| serde_json::to_value(node).unwrap())
        .find(|node| !node["payload"]["ASAP"]["SummaryTumblingWindowChain"].is_null())
        .expect("the plan builds a chain");
    let payload = &chain["payload"]["ASAP"]["SummaryTumblingWindowChain"];
    assert_eq!(
        payload["summary_type"],
        serde_json::json!({"Sketch": "DDSketch"})
    );
    let width_ms = payload["tumbling_window_width_ms"].as_i64().unwrap();
    let alpha = chain["parameters"]["DDSketch"]["alpha"].as_f64().unwrap();
    (
        plan,
        chain["id"].as_u64().unwrap() as usize,
        width_ms,
        alpha,
    )
}

/// A sample value of `job` at second `s`: within the sketch's domain.
fn value(job: &str, s: i64) -> f64 {
    (s % 97) as f64 + if job == "a" { 0.5 } else { 3.25 }
}

/// The query time: a multiple of the 10 s evaluation interval, 20 s ago,
/// so every window it reads has ended.
fn query_time_ms() -> i64 {
    let now = precompute::ingestion::wall_clock_ms();
    now - now.rem_euclid(10_000) - 20_000
}

fn no_forward(stream_start_ms: i64) -> EngineOptions {
    EngineOptions {
        no_forward: true,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(stream_start_ms),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

fn data_plane(plan: &InstallablePlan, options: EngineOptions) -> (Router, Arc<DataPlane>) {
    let plan = serde_json::to_string(plan).unwrap();
    let state =
        Arc::new(DataPlane::with_options(plan, "http://127.0.0.1:1".into(), options).unwrap());
    (router(state.clone()), state)
}

/// A data plane whose collector builds view `node`, committing a window as
/// soon as it ends, with the server's default push precompute (the data
/// plane it is compared with builds by window).
fn collector_data_plane(plan: &InstallablePlan, node: usize) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        precompute_mode: PrecomputeMode::Push,
        collector: CollectorViewOptions {
            views: BTreeSet::from([node]),
            commit_grace_ms: 0,
        },
        ..no_forward(0)
    };
    data_plane(plan, options)
}

/// One second's samples of each job in `(from_ms, to_ms]`.
fn samples(job: &str, from_ms: i64, to_ms: i64) -> Vec<(i64, f64)> {
    (from_ms / 1000 + 1..=to_ms / 1000)
        .map(|s| (s * 1000, value(job, s)))
        .collect()
}

/// The answer of [`QUERY`] at `time_ms` from a data plane that builds the
/// view itself from the samples in `(time_ms − 5 m, time_ms]`.
async fn built_here(plan: &InstallablePlan, time_ms: i64) -> Value {
    let (app, _state) = data_plane(plan, no_forward(time_ms - RANGE_MS));
    let timeseries = JOBS
        .iter()
        .map(|job| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "data".into(),
                },
                Label {
                    name: "job".into(),
                    value: (*job).into(),
                },
            ],
            samples: samples(job, time_ms - RANGE_MS, time_ms)
                .into_iter()
                .map(|(timestamp, value)| Sample { timestamp, value })
                .collect(),
        })
        .collect();
    common::remote_write(&app, timeseries).await;
    common::barrier(&app, time_ms).await;
    let (status, _, body) = common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), JOBS.len());
    body
}

/// The ASAPv1 bytes of a DDSketch of α `alpha` fed `samples`.
fn sketch_state(alpha: f64, samples: &[(i64, f64)]) -> Vec<u8> {
    let mut sketch = DdSketch::new(alpha);
    for (_, value) in samples {
        sketch.try_add(value).unwrap();
    }
    sketch.to_asapv1().unwrap()
}

fn text(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

/// One state: `job`'s state `asapv1` of the window ending at `end_ms`.
struct State {
    job: &'static str,
    end_ms: i64,
    asapv1: Vec<u8>,
}

/// The OTLP body of `states` of view `node` of `plan`, with the view
/// identity as the collector reads it from its copy of the plan.
fn body(plan: &InstallablePlan, node: usize, width_ms: i64, states: &[State]) -> Vec<u8> {
    let view = plan
        .materialized_views()
        .unwrap()
        .into_iter()
        .find(|view| view.node_id == node)
        .unwrap();
    let key = serde_json::to_value(view.view_state_key()).unwrap();
    let metadata = vec![
        text("asap.plan_id", view.plan_id()),
        KeyValue {
            key: "asap.node_id".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(node as i64)),
            }),
        },
        text("asap.view_state_key", &key.to_string()),
    ];
    let data_points = states
        .iter()
        .map(|state| SummaryDataPoint {
            attributes: vec![
                text("job", state.job),
                KeyValue {
                    key: "sketch.envelope".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::BytesValue(state.asapv1.clone())),
                    }),
                },
            ],
            start_time_unix_nano: ((state.end_ms - width_ms) * 1_000_000) as u64,
            time_unix_nano: (state.end_ms * 1_000_000) as u64,
            count: 1,
            ..Default::default()
        })
        .collect();
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: format!("data.asap_view.{node}"),
                    metadata,
                    data: Some(Data::Summary(Summary { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    prost::Message::encode_to_vec(&request)
}

/// Posts an OTLP body; returns the reply's rejected points.
async fn post(app: &Router, body: Vec<u8>) -> i64 {
    let response = app
        .clone()
        .oneshot(
            Request::post("/v1/metrics")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let reply: ExportMetricsServiceResponse = prost::Message::decode(&bytes[..]).unwrap();
    reply.partial_success.map_or(0, |p| p.rejected_data_points)
}

/// The value of `/metrics` line `line` (exactly that series), 0 if absent.
fn metric(state: &DataPlane, line: &str) -> f64 {
    state
        .engine
        .render_metrics()
        .lines()
        .find_map(|l| l.strip_prefix(line)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or(0.0)
}

/// Waits until `/metrics` line `line` reaches `at_least`.
async fn wait_for(state: &DataPlane, line: &str, at_least: f64) {
    for _ in 0..200 {
        if metric(state, line) >= at_least {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "{line} stayed below {at_least}:\n{}",
        state.engine.render_metrics()
    );
}

/// Each job's state per window of `(time_ms − 5 m, time_ms]`.
fn states(alpha: f64, width_ms: i64, time_ms: i64) -> Vec<State> {
    let mut states = Vec::new();
    for job in JOBS {
        let mut end_ms = time_ms - RANGE_MS + width_ms;
        while end_ms <= time_ms {
            states.push(State {
                job,
                end_ms,
                asapv1: sketch_state(alpha, &samples(job, end_ms - width_ms, end_ms)),
            });
            end_ms += width_ms;
        }
    }
    states
}

/// A view whose states come from the collector answers exactly as the
/// view the data plane builds from the same samples (DDSketch states of
/// equal α fed equal samples are equal); its metric's raw samples, read by
/// that view only, are not stored.
#[tokio::test(flavor = "multi_thread")]
async fn collector_states_answer_as_the_view_built_here() {
    let (plan, node, width_ms, alpha) = plan();
    let time_ms = query_time_ms();
    let expected = built_here(&plan, time_ms).await;

    let (app, state) = collector_data_plane(&plan, node);
    let states = states(alpha, width_ms, time_ms);
    assert_eq!(post(&app, body(&plan, node, width_ms, &states)).await, 0);
    let windows = (RANGE_MS / width_ms) as f64;
    let committed =
        format!("asap_collector_states_windows_total{{view=\"{node}\",outcome=\"committed\"}}");
    wait_for(&state, &committed, windows).await;
    let (status, _, answer) = common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer, expected);
    assert_eq!(
        metric(
            &state,
            &format!("asap_collector_states_total{{view=\"{node}\",outcome=\"staged\"}}")
        ),
        states.len() as f64
    );

    // Raw samples of `data` are not kept: only the collector view reads them.
    let timeseries = vec![TimeSeries {
        labels: vec![Label {
            name: "__name__".into(),
            value: "data".into(),
        }],
        samples: vec![Sample {
            timestamp: time_ms,
            value: 1.0,
        }],
    }];
    common::remote_write(&app, timeseries).await;
    for _ in 0..100 {
        if state.engine.stats().samples_unrouted == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(state.engine.stats().samples_unrouted, 1);
}

/// A view built here from a collector view (a wider level of the window
/// chain, merged from the collector's narrower windows) waits for the
/// collector view's windows, and answers as the levels built here from the
/// samples do.
#[tokio::test(flavor = "multi_thread")]
async fn a_view_reading_a_collector_view_is_built_from_its_states() {
    let (plan, node, width_ms, alpha) = plan_for(&common::inputs(false));
    let views = plan.materialized_views().unwrap();
    assert!(
        views
            .iter()
            .any(|view| view.inputs.materialized_views.contains(&node)),
        "no view reads the chain"
    );
    let time_ms = query_time_ms();
    let expected = built_here(&plan, time_ms).await;

    let (app, state) = collector_data_plane(&plan, node);
    let states = states(alpha, width_ms, time_ms);
    assert_eq!(post(&app, body(&plan, node, width_ms, &states)).await, 0);
    let committed =
        format!("asap_collector_states_windows_total{{view=\"{node}\",outcome=\"committed\"}}");
    wait_for(&state, &committed, (RANGE_MS / width_ms) as f64).await;
    let mut answer = Value::Null;
    for _ in 0..100 {
        let (status, _, body) =
            common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
        answer = body;
        if status == StatusCode::OK {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(answer, expected);
}

/// A window's two partial states (a collector restarted inside it) are
/// merged into the window's state: it answers as the state of all its
/// samples does. A repeat of either partial, before or after the merge,
/// is a duplicate; a state arriving after its window committed is dropped
/// as late.
#[tokio::test(flavor = "multi_thread")]
async fn partials_merge_repeats_are_duplicates_late_states_drop() {
    let (plan, node, width_ms, alpha) = plan();
    let time_ms = query_time_ms();
    let expected = built_here(&plan, time_ms).await;

    let (app, state) = collector_data_plane(&plan, node);
    let mut all = states(alpha, width_ms, time_ms);
    // Job `a`'s newest window as two partials: its first and its second
    // half of the samples.
    let newest = all
        .iter()
        .position(|s| s.job == "a" && s.end_ms == time_ms)
        .unwrap();
    let whole = samples("a", time_ms - width_ms, time_ms);
    let (first, second) = whole.split_at(whole.len() / 2);
    all[newest].asapv1 = sketch_state(alpha, first);
    let second = State {
        job: "a",
        end_ms: time_ms,
        asapv1: sketch_state(alpha, second),
    };
    let first_again = State {
        job: "a",
        end_ms: time_ms,
        asapv1: all[newest].asapv1.clone(),
    };
    all.push(State {
        job: "a",
        end_ms: time_ms,
        asapv1: all[newest].asapv1.clone(),
    });
    all.push(second);
    all.push(first_again);
    assert_eq!(post(&app, body(&plan, node, width_ms, &all)).await, 0);
    let windows = (RANGE_MS / width_ms) as f64;
    let line = |outcome: &str| {
        format!("asap_collector_states_total{{view=\"{node}\",outcome=\"{outcome}\"}}")
    };
    wait_for(
        &state,
        &format!("asap_collector_states_windows_total{{view=\"{node}\",outcome=\"committed\"}}"),
        windows,
    )
    .await;
    assert_eq!(metric(&state, &line("duplicate")), 2.0);
    assert_eq!(metric(&state, &line("merged")), 1.0);
    let (status, _, answer) = common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer, expected);

    let late = State {
        job: "b",
        end_ms: time_ms,
        asapv1: sketch_state(alpha, &[(time_ms, 1.0)]),
    };
    assert_eq!(post(&app, body(&plan, node, width_ms, &[late])).await, 0);
    assert_eq!(metric(&state, &line("late_dropped")), 1.0);
    let (_, _, again) = common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
    assert_eq!(again, expected);
}

/// States failing a check are rejected with their reason and reported as
/// rejected points: another plan's, an unknown view's, another key's (θ),
/// a window not on the view's grid, and bytes that are not the view's
/// sketch. A window with no state is not stored, so the query reading it
/// is not answered.
#[tokio::test(flavor = "multi_thread")]
async fn bad_states_are_rejected_and_a_window_without_states_is_missing() {
    let (plan, node, width_ms, alpha) = plan();
    let time_ms = query_time_ms();
    let (app, state) = collector_data_plane(&plan, node);
    let one = |end_ms: i64, asapv1: Vec<u8>| State {
        job: "a",
        end_ms,
        asapv1,
    };
    let good = sketch_state(alpha, &[(time_ms, 1.0)]);
    let rejected = |reason: &str| {
        metric(
            &state,
            &format!("asap_collector_states_rejected_total{{view=\"{node}\",reason=\"{reason}\"}}"),
        )
    };

    let good_body = || body(&plan, node, width_ms, &[one(time_ms, good.clone())]);
    let other_plan = edited(good_body(), |metric| {
        metric.metadata[0] = text("asap.plan_id", "another plan");
    });
    assert_eq!(post(&app, other_plan).await, 1);
    assert_eq!(rejected("other_plan"), 1.0);

    let view = plan.materialized_views().unwrap().remove(0);
    let mut key = serde_json::to_value(view.view_state_key()).unwrap();
    key["logical_properties"]["parameters"]["DDSketch"]["alpha"] = (alpha / 2.0).into();
    let other_key = edited(good_body(), |metric| {
        metric.metadata[2] = text("asap.view_state_key", &key.to_string());
    });
    assert_eq!(post(&app, other_key).await, 1);
    assert_eq!(rejected("key_mismatch"), 1.0);

    let other_name = edited(good_body(), |metric| {
        metric.name = "other.asap_view.1".into()
    });
    assert_eq!(post(&app, other_name).await, 1);
    assert_eq!(rejected("name"), 1.0);

    assert_eq!(
        post(
            &app,
            body(&plan, node, width_ms, &[one(time_ms - 1, good.clone())])
        )
        .await,
        1
    );
    assert_eq!(rejected("window"), 1.0);

    assert_eq!(
        post(
            &app,
            body(&plan, node, width_ms, &[one(time_ms, vec![1, 2, 3])])
        )
        .await,
        1
    );
    assert_eq!(rejected("decode"), 1.0);

    let unknown = node + 1000;
    let unknown_view = edited(good_body(), |metric| {
        metric.name = format!("data.asap_view.{unknown}");
        metric.metadata[1].value = Some(AnyValue {
            value: Some(any_value::Value::IntValue(unknown as i64)),
        });
    });
    assert_eq!(post(&app, unknown_view).await, 1);
    assert_eq!(
        metric(&state, &format!("asap_collector_states_rejected_total{{view=\"{unknown}\",reason=\"unknown_view\"}}")),
        1.0
    );

    // Two windows apart: the one between them has no state.
    let first = time_ms - 2 * width_ms;
    let states = [one(first, good.clone()), one(time_ms, good)];
    assert_eq!(post(&app, body(&plan, node, width_ms, &states)).await, 0);
    wait_for(
        &state,
        &format!("asap_collector_states_windows_total{{view=\"{node}\",outcome=\"empty\"}}"),
        1.0,
    )
    .await;
    let (status, _, answer) = common::get(&app, &common::instant_uri(QUERY, time_ms / 1000)).await;
    assert_ne!(status, StatusCode::OK, "{answer}");
}

/// `body` with its one metric changed by `edit`.
fn edited(body: Vec<u8>, edit: impl FnOnce(&mut Metric)) -> Vec<u8> {
    let mut request: ExportMetricsServiceRequest = prost::Message::decode(&body[..]).unwrap();
    edit(&mut request.resource_metrics[0].scope_metrics[0].metrics[0]);
    prost::Message::encode_to_vec(&request)
}
