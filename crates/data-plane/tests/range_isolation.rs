//! Range requests do not hold up instant queries or writes: while users
//! refresh a 1 h range panel (241 steps), an instant query and a Remote
//! Write request take about as long as when no range runs.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use precompute::ingestion::IngestionOptions;
use prometheus_remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use serde_json::json;
use tower::ServiceExt;

const INSTANT: &str = "max_over_time(data[30m])";
const RANGE: &str = "quantile_over_time(0.99, data[1h])";
/// `RANGE_ISOLATION_SERIES=n` measures at another scale.
fn series() -> usize {
    std::env::var("RANGE_ISOLATION_SERIES").map_or(400, |n| n.parse().unwrap())
}
/// Two hours of samples, every 15 s, from 0.
const END_S: i64 = 7_200;

fn plan() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [
            {"query": INSTANT,
             "demand": {"fixed_interval_at": {"interval": 15000, "evaluation_phase": 0}},
             "requirements": {},
             "predictability": {"predictable": {"known_at": null}}},
            {"query": RANGE,
             "demand": {"fixed_interval_at": {"interval": 15000, "evaluation_phase": 0}},
             "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.05,
                                           "accuracy_scope": "whole_result"}},
             "predictability": {"predictable": {"known_at": null}},
             "range_evaluation": {"step_ms": 15000, "span_ms": 3_600_000}},
        ]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": series() as f64 / 15.0}},
          "sources": {"data": {"series_count": series(), "sample_interval": 15000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

fn request(steps: std::ops::RangeInclusive<i64>) -> Vec<u8> {
    let timeseries = (0..series())
        .map(|i| TimeSeries {
            labels: [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 8)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect(),
            samples: steps
                .clone()
                .map(|step| Sample {
                    timestamp: step * 15_000,
                    value: ((step * (i as i64 + 7)) % 997) as f64,
                })
                .collect(),
        })
        .collect();
    encode(&WriteRequest { timeseries })
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Duration) {
    let started = Instant::now();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    if !status.is_success() {
        eprintln!("{status}: {}", String::from_utf8_lossy(&body));
    }
    (status, started.elapsed())
}

fn instant_request(time_s: i64) -> Request<Body> {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(INSTANT.as_bytes()).collect::<String>()
    );
    Request::get(uri).body(Body::empty()).unwrap()
}

fn range_request(end_s: i64) -> Request<Body> {
    let uri = format!(
        "/api/v1/query_range?query={}&start={}&end={end_s}&step=15",
        form_urlencoded::byte_serialize(RANGE.as_bytes()).collect::<String>(),
        end_s - 3_600
    );
    Request::get(uri).body(Body::empty()).unwrap()
}

/// The median and the largest of `latencies`.
fn median_max(mut latencies: Vec<Duration>) -> (Duration, Duration) {
    latencies.sort();
    (latencies[latencies.len() / 2], *latencies.last().unwrap())
}

/// Instant queries and writes measured `n` times, one after another.
async fn measure(app: &Router, n: usize, next_step: &mut i64) -> (Vec<Duration>, Vec<Duration>) {
    let (mut instants, mut writes) = (Vec::new(), Vec::new());
    for _ in 0..n {
        let (status, took) = send(app, instant_request(END_S)).await;
        assert_eq!(status, StatusCode::OK);
        instants.push(took);
        let body = request(*next_step..=*next_step);
        *next_step += 1;
        let (status, took) = send(
            app,
            Request::post("/api/v1/write")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        writes.push(took);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (instants, writes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_requests_do_not_hold_up_instant_queries_or_writes() {
    // Runtime workers starved by range requests poll no timer and no
    // socket, so the test would wait forever: an OS thread fails it.
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if finished.recv_timeout(Duration::from_secs(300)).is_err() {
            eprintln!("instant queries and writes made no progress while ranges ran");
            std::process::exit(101);
        }
    });
    let dir = std::env::temp_dir().join(format!("range-isolation-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let options = EngineOptions {
        no_forward: true,
        query_threads: std::env::var("RANGE_ISOLATION_QUERY_THREADS")
            .map_or(8, |n| n.parse().unwrap()),
        view_dir: Some(dir.clone()),
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state =
        Arc::new(DataPlane::with_options(plan(), "http://127.0.0.1:1".into(), options).unwrap());
    let app = router(state.clone());
    // Two hours of samples, a minute per request. Over the second hour a
    // dashboard refreshes the range panel after each minute, as the users
    // do: the result cache then holds every step but the newest, and the
    // plan keeps a step's inputs only for the refresh interval.
    let mut took = Duration::ZERO;
    for minute in 0..END_S / 60 {
        let body = request(minute * 4 + 1..=minute * 4 + 4);
        let (status, _) = send(
            &app,
            Request::post("/api/v1/write")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        if minute >= 60 {
            let now_s = (minute + 1) * 60;
            let barrier = format!("/api/v1/precompute/watermark?time_ms={}", now_s * 1000);
            let (status, _) = send(&app, Request::post(barrier).body(Body::empty()).unwrap()).await;
            assert_eq!(status, StatusCode::OK);
            let (_, range_took) = send(&app, range_request(now_s)).await;
            took = range_took;
        }
    }
    let (status, _) = send(&app, range_request(END_S)).await;
    assert_eq!(status, StatusCode::OK);
    eprintln!("one range request alone: {took:?}");

    let mut next_step = END_S / 15 + 1;
    let (instants, writes) = measure(&app, 20, &mut next_step).await;
    let (instant_alone, write_alone) = (median_max(instants), median_max(writes));

    // Four users refreshing the range panel, back to back.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let users: Vec<_> = (0..4)
        .map(|_| {
            let (app, stop) = (app.clone(), stop.clone());
            tokio::spawn(async move {
                let mut ranges = Vec::new();
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (status, took) = send(&app, range_request(END_S)).await;
                    assert_eq!(status, StatusCode::OK);
                    ranges.push(took);
                }
                ranges
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (instants, writes) = measure(&app, 20, &mut next_step).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut ranges = Vec::new();
    for user in users {
        ranges.extend(user.await.unwrap());
    }
    let (instant_busy, write_busy) = (median_max(instants), median_max(writes));
    eprintln!(
        "instant alone {instant_alone:?}, with ranges {instant_busy:?}; \
         write alone {write_alone:?}, with ranges {write_busy:?}; \
         {} ranges, median {:?}",
        ranges.len(),
        median_max(ranges).0
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = done.send(());
    // On two runtime workers, ranges built on them starve everything else;
    // built off them, an instant query or a write waits at most briefly.
    let bound = |alone: Duration| alone * 5 + Duration::from_millis(20);
    assert!(
        instant_busy.0 <= bound(instant_alone.0),
        "instant query median {:?} with ranges, {:?} alone",
        instant_busy.0,
        instant_alone.0
    );
    assert!(
        write_busy.0 <= bound(write_alone.0),
        "write median {:?} with ranges, {:?} alone",
        write_busy.0,
        write_alone.0
    );
}
