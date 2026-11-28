//! Range-query cost of a dashboard refresh, in process: plans the demo's
//! dashboard workload (`crates/data-plane/tests/workloads/dashboard.json`) for `SERIES`
//! series of each metric, every query also requested as a range of 1 h at
//! 15 s steps, sends 2 h 5 min of samples (one per series every `SCRAPE`
//! seconds) and closes the windows with an event-time barrier at the last
//! one. Then, as a Grafana panel refreshed every 15 s does, it requests
//! every query's range ending at the barrier (the first request), the same
//! range again, and, after 15 s more samples and a barrier, the range
//! shifted by 15 s. Prints each request's wall time, the CPU time of every
//! thread during it, and the steps the result cache answered.
//! `PLAN` is `views` (no raw samples kept, the default) or `raw`;
//! `RESULT_CACHE=0` turns the result cache off.
//!
//! `cargo run --release -p data_plane --example range_bench -- [SERIES] [PLAN] [SCRAPE]`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use internal_metrics::thread_cpu_seconds;
use tower::ServiceExt;

const STEP_MS: i64 = 15_000;
const SPAN_MS: i64 = 3_600_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let series: usize = args.get(1).map_or(1000, |a| a.parse().unwrap());
    let raw_data_retained = args.get(2).is_some_and(|a| a == "raw");
    let scrape_ms: i64 = args.get(3).map_or(15, |a| a.parse::<i64>().unwrap()) * 1000;
    let result_cache = std::env::var("RESULT_CACHE").map_or(true, |v| v != "0");

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/workloads/dashboard.json"
    );
    let mut workload: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let metrics: Vec<String> = workload["data_workload"]["sources"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    for source in workload["data_workload"]["sources"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        source["series_count"] = series.into();
        source["sample_interval"] = scrape_ms.into();
    }
    workload["data_workload"]["arrival"]["ingesting"]["rows_per_second"] =
        (series as f64 * metrics.len() as f64 * 1000.0 / scrape_ms as f64).into();
    for query in workload["query_workload"]["repeating_queries"]
        .as_array_mut()
        .unwrap()
    {
        query["range_evaluation"] = serde_json::json!({"step_ms": STEP_MS, "span_ms": SPAN_MS});
    }
    let workload = serde_json::from_value(workload).unwrap();
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    let (plan, _) = plan_workload(&workload, &inputs).unwrap();
    println!(
        "plan: raw data retained {raw_data_retained}, {} views; {series} series per metric, \
         a sample every {} s; result cache {}",
        plan.materialized_views().unwrap().len(),
        scrape_ms / 1000,
        if result_cache { "on" } else { "off" }
    );

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // The barrier 15 s after the last sample sent second must not pass the
    // wall clock.
    let end_ms = (now_ms - 60_000).div_euclid(STEP_MS) * STEP_MS;
    let start_ms = end_ms - 2 * SPAN_MS - 300_000;
    let options = EngineOptions {
        ingestion: IngestionOptions {
            stream_start_ms: Some(start_ms),
            ..IngestionOptions::default()
        },
        no_forward: true,
        result_cache,
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
    let labels: Vec<Vec<Label>> = metrics
        .iter()
        .flat_map(|metric| (0..series).map(move |i| (metric, i)))
        .map(|(metric, i)| {
            [
                ("__name__", metric.clone()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 16)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect()
        })
        .collect();

    let fed = Instant::now();
    feed(&app, &labels, start_ms + scrape_ms, end_ms, scrape_ms).await;
    barrier(&app, end_ms).await;
    println!(
        "fed and refreshed {:.1} h of samples in {:.1} s",
        (end_ms - start_ms) as f64 / 3.6e6,
        fed.elapsed().as_secs_f64()
    );

    let queries = plan.query_texts.clone();
    request(&app, &state, &queries, end_ms, "first request").await;
    request(&app, &state, &queries, end_ms, "same range again").await;
    feed(
        &app,
        &labels,
        end_ms + scrape_ms,
        end_ms + STEP_MS,
        scrape_ms,
    )
    .await;
    barrier(&app, end_ms + STEP_MS).await;
    request(&app, &state, &queries, end_ms + STEP_MS, "15 s later").await;
}

/// Sends one sample per series at every scrape in `[from_ms, to_ms]`.
async fn feed(app: &Router, labels: &[Vec<Label>], from_ms: i64, to_ms: i64, scrape_ms: i64) {
    let mut timestamp = from_ms;
    while timestamp <= to_ms {
        let timeseries = labels
            .iter()
            .enumerate()
            .map(|(i, labels)| TimeSeries {
                labels: labels.clone(),
                samples: vec![Sample {
                    timestamp,
                    value: ((i as i64 * 7 + timestamp / 1000) % 100) as f64,
                }],
            })
            .collect();
        let body = encode(&WriteRequest { timeseries });
        loop {
            let response = app
                .clone()
                .oneshot(
                    Request::post("/api/v1/write")
                        .body(Body::from(body.clone()))
                        .unwrap(),
                )
                .await
                .unwrap();
            match response.status() {
                StatusCode::NO_CONTENT => break,
                StatusCode::SERVICE_UNAVAILABLE => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await
                }
                status => panic!("Remote Write answered {status}"),
            }
        }
        timestamp += scrape_ms;
    }
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

/// Requests every query's range ending at `end_ms`, one after another as
/// a dashboard's panels load, and prints the wall and CPU time.
async fn request(app: &Router, state: &DataPlane, queries: &[String], end_ms: i64, label: &str) {
    let cached_before = steps_cached(state);
    let cpu_before: f64 = thread_cpu_seconds().values().sum();
    let started = Instant::now();
    let mut lines = Vec::new();
    for query in queries {
        let one = Instant::now();
        let uri = format!(
            "/api/v1/query_range?{}",
            form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .append_pair("start", &((end_ms - SPAN_MS) / 1000).to_string())
                .append_pair("end", &(end_ms / 1000).to_string())
                .append_pair("step", &(STEP_MS / 1000).to_string())
                .finish()
        );
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let execution = response.headers().get(EXECUTION_HEADER).cloned();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "{query}: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(execution.unwrap(), "plan");
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let series = json["data"]["result"].as_array().unwrap().len();
        lines.push(format!(
            "    {:>8.1} ms  {series:>5} series  {query}",
            one.elapsed().as_secs_f64() * 1e3
        ));
    }
    let wall = started.elapsed().as_secs_f64();
    let cpu: f64 = thread_cpu_seconds().values().sum::<f64>() - cpu_before;
    let steps = queries.len() as i64 * (SPAN_MS / STEP_MS + 1);
    println!(
        "{label}: {:.1} ms wall, {:.1} ms CPU (all threads), {}/{steps} steps from the result cache",
        wall * 1e3,
        cpu * 1e3,
        steps_cached(state) - cached_before
    );
    println!("{}", lines.join("\n"));
}

/// Range steps the result cache answered so far, over every query.
fn steps_cached(state: &DataPlane) -> u64 {
    state
        .engine
        .metrics()
        .queries
        .values()
        .map(|m| {
            m.range_steps_cached
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .sum()
}
