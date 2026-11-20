//! Ingestion sharded by series: the samples of one window spread over
//! several shards build the views one shard builds, and shards with
//! nothing to do never hold the watermark back.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[30s]))";

fn plan_json() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 64.0}},
          "sources": {"data": {"series_count": 64, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

fn data_plane(ingest_threads: usize) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        no_forward: true,
        ingest_threads,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

/// Writes one sample per second in `(0, 60]` for each of `series` series,
/// one request per second, as a scraper sends them.
async fn write(app: &Router, series: usize) {
    for minute in 0..10i64 {
        let timeseries = (0..series)
            .map(|i| TimeSeries {
                labels: [
                    ("__name__", "data".to_string()),
                    ("instance", format!("i{i}")),
                    ("job", format!("job{}", i % 4)),
                ]
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value,
                })
                .collect(),
                samples: (minute * 60 + 1..=minute * 60 + 60)
                    .map(|second| Sample {
                        timestamp: second * 1000,
                        value: (second * (i as i64 + 1) % 17) as f64,
                    })
                    .collect(),
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
}

async fn answer(app: &Router, time_s: i64) -> Value {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(QUERY.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    body["data"]["result"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|s| s["metric"]["job"].as_str().unwrap().to_string());
    body
}

async fn barrier(app: &Router, time_ms: i64) {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// 64 series over four shards answer as over one: each window's views are
/// built from the union of the shards' samples.
#[tokio::test(flavor = "multi_thread")]
async fn views_over_several_shards_equal_views_over_one() {
    let (one, _one_state) = data_plane(1);
    let (four, _four_state) = data_plane(4);
    for app in [&one, &four] {
        write(app, 64).await;
        barrier(app, 600_000).await;
    }
    for time_s in [560, 570, 580, 590, 600] {
        assert_eq!(
            answer(&four, time_s).await,
            answer(&one, time_s).await,
            "at {time_s}"
        );
    }
}

/// With one series, three of four shards never get a sample; the windows
/// still close as the busy shard's samples pass them, without a barrier.
#[tokio::test(flavor = "multi_thread")]
async fn idle_shards_do_not_hold_the_watermark() {
    let (app, state) = data_plane(4);
    write(&app, 1).await;
    let started = std::time::Instant::now();
    while state.engine.stats().windows_refreshed < 5 {
        assert!(
            started.elapsed().as_secs() < 10,
            "{:?}",
            state.engine.stats()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
