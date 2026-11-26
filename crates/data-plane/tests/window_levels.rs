//! Multi-level window chains: a wider level (`SummaryMergeWindows`) is
//! built from the narrower level's committed windows when its wide window
//! closes, and a range read (`TimeRangeCover`) takes the union of whole
//! windows over the levels.
//!
//! Two queries share the narrowest level's refresh group: a 1 h sum and a
//! 30 min max. The plan keeps the max's narrow view 30 min, enough for its
//! 1 min level built right behind it. Two hours of samples come in one
//! request, so one pass closes every window, the narrow group's first: the
//! wider levels' windows then start only after the whole narrow group has
//! committed, back to the group's longest retention (1 h). The narrow max
//! view keeps what its readers have yet to read, and every answer equals
//! the one computed from the samples.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use prometheus_remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use serde_json::{json, Value};
use tower::ServiceExt;

const SUM: &str = "sum by (job) (sum_over_time(data[1h]))";
const MAX: &str = "max by (job) (max_over_time(data[30m]))";
const SERIES: usize = 12;
/// Two hours of samples, every 15 s, from 0.
const END_S: i64 = 7_200;

fn plan() -> asap_planner::InstallablePlan {
    let query = |text: &str| {
        json!({
            "query": text,
            "demand": {"fixed_interval_at": {"interval": 15000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        })
    };
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [query(SUM), query(MAX)]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 0.8}},
          "sources": {"data": {"series_count": SERIES, "sample_interval": 15000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    plan_workload(&workload, &common::inputs(false)).unwrap().0
}

fn value(series: usize, second: i64) -> f64 {
    ((second / 15) * (series as i64 + 1) % 23) as f64
}

/// The samples of `series` in `(time_s - range_s, time_s]`.
fn samples(series: usize, time_s: i64, range_s: i64) -> impl Iterator<Item = f64> {
    ((time_s - range_s) / 15 + 1..=time_s / 15)
        .filter(|step| *step >= 1)
        .map(move |step| value(series, step * 15))
}

/// `SUM` and `MAX` at `time_s` from the samples, per job.
fn expected(query: &str, time_s: i64) -> Vec<(String, f64)> {
    (0..3)
        .map(|job| {
            let series = (0..SERIES).filter(|i| i % 3 == job);
            let answer = if query == SUM {
                series.flat_map(|i| samples(i, time_s, 3_600)).sum()
            } else {
                series
                    .flat_map(|i| samples(i, time_s, 1_800))
                    .fold(f64::MIN, f64::max)
            };
            (format!("job{job}"), answer)
        })
        .collect()
}

async fn post(app: &Router, uri: String, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(Request::post(uri).body(Body::from(body)).unwrap())
        .await
        .unwrap()
        .status()
}

async fn answer(app: &Router, query: &str, time_s: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn wider_levels_and_covers_answer_after_one_pass_closes_every_window() {
    let plan = plan();
    let text = serde_json::to_string(&plan.dag).unwrap();
    assert!(
        text.contains("SummaryMergeWindows"),
        "the plan has no wider level"
    );
    assert!(text.contains("TimeRangeCover"), "the plan has no cover");
    // The narrowest group holds a view kept less than the group's longest
    // retention: the case a view's readers must hold.
    let groups = plan.ingestion_refresh_groups().unwrap();
    let narrow = groups
        .iter()
        .min_by_key(|g| g.refresh_window.unwrap().width_ms)
        .unwrap();
    let retentions: Vec<u64> = narrow.views.iter().map(|v| v.retention_ms()).collect();
    assert!(
        retentions.iter().min() < retentions.iter().max(),
        "{retentions:?}"
    );
    let options = EngineOptions {
        precompute_threads: 4,
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(&plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    let timeseries = (0..SERIES)
        .map(|i| TimeSeries {
            labels: [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 3)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: (1..=END_S / 15)
                .map(|step| Sample {
                    timestamp: step * 15_000,
                    value: value(i, step * 15),
                })
                .collect(),
        })
        .collect();
    let body = encode(&WriteRequest { timeseries });
    assert_eq!(
        post(&app, "/api/v1/write".into(), body).await,
        StatusCode::NO_CONTENT
    );
    let barrier = format!("/api/v1/precompute/watermark?time_ms={}", END_S * 1000);
    assert_eq!(post(&app, barrier, Vec::new()).await, StatusCode::OK);
    assert_eq!(
        state.engine.stats().windows_failed,
        0,
        "{:?}",
        state.engine.stats()
    );
    // Every 15 s step the views' retention margin (60 s) covers, on and
    // off the wider grids.
    for query in [SUM, MAX] {
        for time_s in (END_S - 60..=END_S).step_by(15) {
            let (status, body) = answer(&app, query, time_s).await;
            assert_eq!(status, StatusCode::OK, "{query} at {time_s}: {body}");
            assert_eq!(
                common::per_job(&body),
                expected(query, time_s),
                "{query} at {time_s}"
            );
        }
    }
}

/// Before a window has filled, a query answers from the samples there are,
/// as PromQL does: the stream starts at 0, 20 minutes of samples arrive,
/// and the 1 h sum and the 30 min max are exact at every 15 s step from the
/// first sample on, the wider levels and covers included.
#[tokio::test(flavor = "multi_thread")]
async fn queries_answer_from_the_samples_there_are_before_their_window_fills() {
    const FILLED_S: i64 = 1_200;
    let plan = plan();
    let options = EngineOptions {
        precompute_threads: 4,
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(&plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());
    let timeseries = (0..SERIES)
        .map(|i| TimeSeries {
            labels: [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 3)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: (1..=FILLED_S / 15)
                .map(|step| Sample {
                    timestamp: step * 15_000,
                    value: value(i, step * 15),
                })
                .collect(),
        })
        .collect();
    let body = encode(&WriteRequest { timeseries });
    assert_eq!(
        post(&app, "/api/v1/write".into(), body).await,
        StatusCode::NO_CONTENT
    );
    let barrier = format!("/api/v1/precompute/watermark?time_ms={}", FILLED_S * 1000);
    assert_eq!(post(&app, barrier, Vec::new()).await, StatusCode::OK);
    assert_eq!(
        state.engine.stats().windows_failed,
        0,
        "{:?}",
        state.engine.stats()
    );
    for query in [SUM, MAX] {
        for time_s in (15..=FILLED_S).step_by(15) {
            let (status, body) = answer(&app, query, time_s).await;
            assert_eq!(status, StatusCode::OK, "{query} at {time_s}: {body}");
            assert_eq!(
                common::per_job(&body),
                expected(query, time_s),
                "{query} at {time_s}"
            );
        }
    }
}
