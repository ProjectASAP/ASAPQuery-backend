//! Times one evaluation of an offset comparison and of each of its sides:
//! `max by (job) (max_over_time(queue_depth[1h]))`, the same with
//! `offset 1h`, and the first `>` the second, planned together over
//! `SERIES` series (default 1000) sampled every 15 s for 2.5 h.
//!
//! `offset_bench [SERIES] [RUNS]`

#[path = "../../crates/data-plane/tests/common/mod.rs"]
#[allow(dead_code)]
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use precompute::ingestion::IngestionOptions;
use serde_json::json;
use tower::ServiceExt;

const NOW: &str = "max by (job) (max_over_time(queue_depth[1h]))";
const BEFORE: &str = "max by (job) (max_over_time(queue_depth[1h] offset 1h))";
const COMPARE: &str = "max by (job) (max_over_time(queue_depth[1h])) > max by (job) (max_over_time(queue_depth[1h] offset 1h))";
const END_S: i64 = 9_000;

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Duration, usize) {
    let started = Instant::now();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    if !status.is_success() {
        eprintln!("{status}: {}", String::from_utf8_lossy(&body));
    }
    (status, started.elapsed(), body.len())
}

#[tokio::main]
async fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap())
        .collect();
    let series = *args.first().unwrap_or(&1000);
    let runs = *args.get(1).unwrap_or(&5);
    let query = |text: &str| {
        json!({"query": text,
               "demand": {"fixed_interval_at": {"interval": 15000, "evaluation_phase": 0}},
               "requirements": {},
               "predictability": {"predictable": {"known_at": null}}})
    };
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql",
            "repeating_queries": [query(NOW), query(BEFORE), query(COMPARE)]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": series as f64 / 15.0}},
          "sources": {"queue_depth": {"series_count": series, "sample_interval": 15000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    // Each run computes its query alone: no result cache, no shared
    // query-time results.
    let options = EngineOptions {
        no_forward: true,
        result_cache: false,
        share_one_evaluation_results: false,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
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
    for minute in 0..END_S / 60 {
        let timeseries = (0..series)
            .map(|i| TimeSeries {
                labels: [
                    ("__name__", "queue_depth".to_string()),
                    ("region", format!("r{}", i % 4)),
                    ("service", format!("s{}", i % 5)),
                    ("job", format!("job{}", i % 10)),
                    ("instance", format!("i{i}")),
                ]
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value,
                })
                .collect(),
                samples: (1..=4)
                    .map(|k| minute * 60 + k * 15)
                    .map(|second| Sample {
                        timestamp: second * 1000,
                        value: ((second * (i as i64 + 3)) % 1009) as f64,
                    })
                    .collect(),
            })
            .collect();
        let body = encode(&WriteRequest { timeseries });
        let (status, _, _) = send(
            &app,
            Request::post("/api/v1/write")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    let barrier = format!("/api/v1/precompute/watermark?time_ms={}", END_S * 1000);
    let (status, _, _) = send(&app, Request::post(barrier).body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let texts = match std::env::var("OFFSET_BENCH_ONLY_BEFORE") {
        Ok(_) => vec![BEFORE],
        Err(_) => vec![COMPARE, NOW, BEFORE],
    };
    for text in texts {
        let mut times = Vec::new();
        for run in 0..runs {
            // The offset side reads back 2 h, which the chain keeps only
            // for the newest evaluation times.
            let _ = run;
            let time_s = END_S;
            let uri = format!(
                "/api/v1/query?query={}&time={time_s}",
                form_urlencoded::byte_serialize(text.as_bytes()).collect::<String>()
            );
            let (status, took, _) =
                send(&app, Request::get(uri).body(Body::empty()).unwrap()).await;
            assert_eq!(status, StatusCode::OK);
            times.push(took);
        }
        times.sort();
        println!(
            "{:>8.1} ms median  {text}",
            times[times.len() / 2].as_secs_f64() * 1000.0
        );
    }
}
