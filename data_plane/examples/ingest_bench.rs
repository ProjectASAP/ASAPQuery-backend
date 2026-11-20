//! Ingest throughput of the data plane, in process: plans a mixed workload
//! (`tests/workloads/random-mixed-seed-862-01.json`) for `SERIES` series of
//! its `data` metric, scraped every second, then sends `SECONDS` seconds of
//! samples by Remote Write (`REQUESTS` requests per scrape, each with a
//! slice of the series; 1 by default) as fast as the data
//! plane takes them, retrying on 503, and ends with an event-time barrier
//! at the last sample, so every closed window is refreshed. Prints samples
//! per second of wall time, until every write is acknowledged and until
//! every window is refreshed, and the CPU time of each thread, by name.
//! `PLAN` is `raw` (raw samples kept, the default) or `views` (none kept:
//! every query reads materialized views). Views are persisted to a fresh
//! temporary directory, as the data plane does by default, unless
//! `PERSIST` is `memory`.
//!
//! `cargo run --release -p data_plane --example ingest_bench -- SERIES SECONDS [PLAN] [REQUESTS] [PERSIST]`

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::metrics::thread_cpu_seconds;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use tower::ServiceExt;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let series: usize = args.get(1).map_or(1000, |a| a.parse().unwrap());
    let seconds: i64 = args.get(2).map_or(300, |a| a.parse().unwrap());
    let raw_data_retained = args.get(3).is_none_or(|a| a == "raw");
    let requests_per_scrape: usize = args.get(4).map_or(1, |a| a.parse().unwrap());
    let view_dir = (args.get(5).map(String::as_str) != Some("memory")).then(|| {
        let dir = std::env::temp_dir().join(format!("ingest-bench-views-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    });

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/workloads/random-mixed-seed-862-01.json"
    );
    let mut workload: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for source in workload["data_workload"]["sources"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        source["series_count"] = series.into();
        source["sample_interval"] = 1000.into();
    }
    workload["data_workload"]["arrival"]["ingesting"]["rows_per_second"] = (series as f64).into();
    let workload = serde_json::from_value(workload).unwrap();
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = raw_data_retained;
    let (plan, _) = plan_workload(&workload, &inputs).unwrap();
    println!(
        "plan: raw data retained {raw_data_retained}, {} views",
        plan.materialized_views().unwrap().len()
    );

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let start_ms = (now_ms - (seconds + 60) * 1000).div_euclid(60_000) * 60_000;
    let options = EngineOptions {
        ingestion: IngestionOptions {
            stream_start_ms: Some(start_ms),
            ..IngestionOptions::default()
        },
        view_dir: view_dir.clone(),
        ..EngineOptions::default()
    };
    println!(
        "{requests_per_scrape} request(s) per scrape, views {}",
        if view_dir.is_some() {
            "persisted"
        } else {
            "in memory"
        }
    );
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(&plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    let app = router(state.clone());

    let labels: Vec<Vec<Label>> = (0..series)
        .map(|i| {
            [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 16)),
                ("region", format!("r{}", i % 4)),
                ("service", format!("s{}", i % 8)),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect()
        })
        .collect();
    let slice = series.div_ceil(requests_per_scrape.max(1));
    let bodies: Vec<Vec<u8>> = (1..=seconds)
        .flat_map(|second| {
            let timestamp = start_ms + second * 1000;
            labels
                .chunks(slice)
                .enumerate()
                .map(|(chunk, labels)| {
                    let timeseries = labels
                        .iter()
                        .enumerate()
                        .map(|(i, labels)| TimeSeries {
                            labels: labels.clone(),
                            samples: vec![Sample {
                                timestamp,
                                value: (((chunk * slice + i) as i64 * 7 + second) % 100) as f64,
                            }],
                        })
                        .collect();
                    encode(&WriteRequest { timeseries })
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let cpu_before = thread_cpu_seconds();
    let started = Instant::now();
    let mut retries = 0u64;
    for body in bodies {
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
                    retries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                status => panic!("Remote Write answered {status}"),
            }
        }
    }
    let acknowledged = started.elapsed().as_secs_f64();
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={}",
                start_ms + seconds * 1000
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
    let wall = started.elapsed().as_secs_f64();
    let cpu_after = thread_cpu_seconds();

    let samples = series as f64 * seconds as f64;
    println!(
        "series {series}, seconds {seconds}, samples {samples}: every write acknowledged after {acknowledged:.2} s \
         ({:.0} samples/s), every window refreshed after {wall:.2} s ({:.0} samples/s), 503 retries {retries}",
        samples / acknowledged,
        samples / wall
    );
    println!("ingestion counters: {:?}", state.engine.stats());
    let mut total = 0.0;
    for (name, after) in &cpu_after {
        let cpu = after - cpu_before.get(name).copied().unwrap_or(0.0);
        total += cpu;
        if cpu > 0.0 {
            println!("  cpu {name:<24} {cpu:8.2} s");
        }
    }
    println!("  cpu {:<24} {total:8.2} s", "(all threads)");
    let metrics = state.engine.render_metrics();
    for name in [
        "asap_view_persisted_bytes_total",
        "asap_view_sync_seconds_total",
        "asap_view_windows_not_persisted_total",
    ] {
        if let Some(line) = metrics.lines().find(|l| l.starts_with(&format!("{name} "))) {
            println!("  {line}");
        }
    }
    if let Some(dir) = view_dir {
        let _ = std::fs::remove_dir_all(dir);
    }
}
