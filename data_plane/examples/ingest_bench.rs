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
//! Environment: `WORKLOAD=path.json` plans that workload instead and feeds
//! `SERIES` series of every metric it names; `PACED=1` sends one scrape per
//! second of wall time, as a live producer does, and reports the largest
//! precompute and persist backlogs seen; `PERSIST_THREADS=n` sets the
//! persist threads; `INGEST_THREADS=n` the ingest shards;
//! `PRECOMPUTE_THREADS=n` the threads computing windows; `SCRAPE_MS=ms`
//! scrapes every `ms` instead of every second; `LATENESS_MS=ms` sets the
//! allowed lateness (default 15 s). Per refresh group it
//! prints the time from a window's end to its commit (for a paced run,
//! counted from when the scrape at the window's end was sent), split into
//! the precompute queue wait, compute and commit wait.
//! `PRECOMPUTE_MODE=push` builds views by push pipelines instead of one
//! ingestion run per closed window (B1: dashboard and seed-862-01, raw data
//! not kept, 1k/10k/100k series, K = 1, 2, 4, 8, paced then unpaced). With
//! `PACED=1` it also samples the newest window's commit lag every second
//! and prints its median and max (B2).
//!
//! `cargo run --release -p data_plane --example ingest_bench -- SERIES SECONDS [PLAN] [REQUESTS] [PERSIST]`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::{EngineOptions, PrecomputeMode};
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

    let default_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/workloads/random-mixed-seed-862-01.json"
    );
    let custom = std::env::var("WORKLOAD").ok();
    let path = custom.clone().unwrap_or_else(|| default_path.to_string());
    let paced = std::env::var("PACED").is_ok_and(|v| v == "1");
    let scrape_ms: i64 = std::env::var("SCRAPE_MS")
        .ok()
        .map_or(1000, |ms| ms.parse().unwrap());
    let scrapes = seconds * 1000 / scrape_ms;
    let mut workload: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for source in workload["data_workload"]["sources"]
        .as_object_mut()
        .unwrap()
        .values_mut()
    {
        source["series_count"] = series.into();
        source["sample_interval"] = scrape_ms.into();
    }
    // The metrics fed: the custom workload's every source, else `data`.
    let metrics: Vec<String> = match custom {
        Some(_) => workload["data_workload"]["sources"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect(),
        None => vec!["data".to_string()],
    };
    workload["data_workload"]["arrival"]["ingesting"]["rows_per_second"] =
        (series as f64 * 1000.0 / scrape_ms as f64).into();
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
            allowed_lateness_ms: std::env::var("LATENESS_MS")
                .ok()
                .map_or(IngestionOptions::default().allowed_lateness_ms, |ms| {
                    ms.parse().unwrap()
                }),
            ..IngestionOptions::default()
        },
        view_dir: view_dir.clone(),
        ingest_threads: std::env::var("INGEST_THREADS")
            .ok()
            .map_or(EngineOptions::default().ingest_threads, |n| {
                n.parse().unwrap()
            }),
        precompute_mode: match std::env::var("PRECOMPUTE_MODE").as_deref() {
            Ok("push") => PrecomputeMode::Push,
            _ => PrecomputeMode::Window,
        },
        precompute_threads: std::env::var("PRECOMPUTE_THREADS")
            .ok()
            .map_or(EngineOptions::default().precompute_threads, |n| {
                n.parse().unwrap()
            }),
        persist_threads: std::env::var("PERSIST_THREADS")
            .ok()
            .map_or(EngineOptions::default().persist_threads, |n| {
                n.parse().unwrap()
            }),
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

    let labels: Vec<Vec<Label>> = metrics
        .iter()
        .flat_map(|metric| (0..series).map(move |i| (metric, i)))
        .map(|(metric, i)| {
            [
                ("__name__", metric.clone()),
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
    let slice = labels.len().div_ceil(requests_per_scrape.max(1));
    let requests_per_second = labels.len().div_ceil(slice);
    let bodies: Vec<Vec<u8>> = (1..=scrapes)
        .flat_map(|scrape| {
            let timestamp = start_ms + scrape * scrape_ms;
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
                                value: (((chunk * slice + i) as i64 * 7 + scrape) % 100) as f64,
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
    // Paced, the scrape at event time `t` is sent at wall time `t + lag_ms`.
    let lag_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - start_ms;
    let mut retries = 0u64;
    let (mut max_precompute_backlog, mut max_persist_backlog) = (0u64, 0f64);
    let mut commit_lags: Vec<f64> = Vec::new();
    for (index, body) in bodies.into_iter().enumerate() {
        if paced && index % requests_per_second == 0 {
            let due = std::time::Duration::from_millis(
                (index / requests_per_second) as u64 * scrape_ms as u64,
            );
            if let Some(wait) = due.checked_sub(started.elapsed()) {
                tokio::time::sleep(wait).await;
            }
            max_precompute_backlog = max_precompute_backlog.max(state.engine.precompute_backlog());
            let persist = state
                .engine
                .render_metrics()
                .lines()
                .find_map(|l| l.strip_prefix("asap_persist_backlog_bytes "))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0);
            max_persist_backlog = f64::max(max_persist_backlog, persist);
            if let Some(lag) = state
                .engine
                .render_metrics()
                .lines()
                .find_map(|l| l.strip_prefix("asap_window_commit_lag_seconds "))
                .and_then(|v| v.parse::<f64>().ok())
            {
                commit_lags.push(lag);
            }
        }
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
                start_ms + scrapes * scrape_ms
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
    let wall = started.elapsed().as_secs_f64();
    let cpu_after = thread_cpu_seconds();

    let samples = (series * metrics.len()) as f64 * scrapes as f64;
    println!(
        "series {series}, seconds {seconds}, samples {samples}: every write acknowledged after {acknowledged:.2} s \
         ({:.0} samples/s), every window refreshed after {wall:.2} s ({:.0} samples/s), 503 retries {retries}",
        samples / acknowledged,
        samples / wall
    );
    println!("ingestion counters: {:?}", state.engine.stats());
    if paced {
        println!(
            "  paced: largest precompute backlog {max_precompute_backlog} windows, largest persist queue {:.0} bytes",
            max_persist_backlog
        );
        commit_lags.sort_by(f64::total_cmp);
        match (commit_lags.get(commit_lags.len() / 2), commit_lags.last()) {
            (Some(median), Some(max)) => {
                println!("  newest-window commit lag: median {median:.3} s, max {max:.3} s")
            }
            _ => println!("  newest-window commit lag: not exported"),
        }
    }
    let text = state.engine.render_metrics();
    for line in text.lines().filter(|l| {
        l.starts_with("asap_phase_")
            && (l.contains("phase=\"precompute") || l.contains("phase=\"append"))
            || l.starts_with("asap_precompute_state_bytes")
            || l.starts_with("asap_raw_data_bytes")
    }) {
        println!("  {line}");
    }
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
    let value = |name: &str, labels: &str| -> f64 {
        let prefix = format!("{name}{labels} ");
        metrics
            .lines()
            .find_map(|l| l.strip_prefix(prefix.as_str()))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    };
    println!(
        "  precompute threads {}, busy {:.2} s",
        value("asap_precompute_threads", ""),
        value("asap_precompute_busy_seconds_total", "")
    );
    for group in 0.. {
        let labels = format!("{{group=\"{group}\"}}");
        let windows = value("asap_precompute_windows_total", &labels);
        if !metrics.contains(&format!("asap_precompute_windows_total{labels}")) {
            break;
        }
        let per_window = |name: &str| value(name, &labels) / windows.max(1.0);
        let compute = metrics
            .lines()
            .find(|l| {
                l.starts_with("asap_phase_wall_seconds_total{phase=\"precompute\"")
                    && l.contains(&format!("group=\"{group}\""))
            })
            .and_then(|l| l.rsplit(' ').next())
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            / windows.max(1.0);
        let lag_s = if paced { lag_ms as f64 / 1e3 } else { 0.0 };
        println!(
            "  group {group}: {windows} windows; ready after end mean {:.3} s, max {:.3} s; \
             queue wait {:.3} s, compute {:.3} s, commit wait {:.3} s (means)",
            per_window("asap_window_ready_seconds_total") - lag_s,
            value("asap_window_ready_seconds_max", &labels) - lag_s,
            per_window("asap_precompute_queue_wait_seconds_total"),
            compute,
            per_window("asap_precompute_commit_wait_seconds_total"),
        );
    }
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
