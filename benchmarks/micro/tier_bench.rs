//! Tiered view storage at scale, in process (docs/design/tiered-view-storage.md,
//! B1, B2 and the V15 run): plans the dashboard workload
//! (`crates/data-plane/tests/workloads/dashboard.json`) without raw samples kept, for
//! `SERIES` series per metric at 1 sample/s; sends `MINUTES` of samples as
//! fast as they are accepted, with the views persisted in `DIR`; then
//! answers the plan's instant queries at the last window end for `ROUNDS`
//! rounds, printing each query's latency, RSS split into `RssAnon` and
//! `RssFile`, the cgroup's `anon` and `file` when it has a `memory.max`,
//! and the tiering counters.
//!
//! - `VIEW_CACHE_MAX_BYTES=n` sets the window cache budget (unset: the
//!   default; `off` keeps every window in memory, as before tiering, with
//!   no idle eviction).
//! - `VIEW_READ=pread` reads cold windows with `pread`.
//! - `WORKLOAD=path` plans another workload file (a planning workload,
//!   as in `crates/data-plane/tests/workloads/`) instead, at 1 sample/s.
//! - `RESULT_CACHE=0` turns the result cache off, so every round reads
//!   its views.
//! - `SCRAPE_MS=n`: one sample per series every `n` ms (default 1,000).
//! - `VALUES=zipf`: the harness's values (Zipf s = 1.1 integers, counters
//!   of such increments with resets) instead of Pareto (shape 2).
//! - `PRECOMPUTE_THREADS`, `ALLOWED_LATENESS_MS`: engine options.
//! - `COLD=1` drops the segments' pages from the page cache
//!   (`POSIX_FADV_DONTNEED`) before each round (B2).
//!
//! `cargo run --release -p data_plane --example tier_bench -- SERIES MINUTES DIR [ROUNDS]`
//! Under a memory limit: `benchmarks/scripts/tiered_views_cgroup.sh`.

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::{plan_workload, DeploymentInputs};
use data_plane::engine::EngineOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use precompute::ingestion::IngestionOptions;
use storage::persist::ViewRead;
use tower::ServiceExt;

const WINDOW_MS: i64 = 15_000;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let series: usize = args.get(1).map_or(10_000, |a| a.parse().unwrap());
    let minutes: i64 = args.get(2).map_or(70, |a| a.parse().unwrap());
    let dir = PathBuf::from(args.get(3).map_or("tier-bench-views", String::as_str));
    let rounds: usize = args.get(4).map_or(3, |a| a.parse().unwrap());
    let budget = std::env::var("VIEW_CACHE_MAX_BYTES")
        .ok()
        .filter(|v| !v.is_empty());
    let cold = std::env::var("COLD").is_ok_and(|v| v == "1");
    // `SCRAPE_MS`: one sample per series every this many milliseconds.
    let scrape_ms: i64 = std::env::var("SCRAPE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    let _ = std::fs::remove_dir_all(&dir);

    let path = std::env::var("WORKLOAD")
        .ok()
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/workloads/dashboard.json"
            )
            .into()
        });
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
        (series * metrics.len() * 1000 / scrape_ms as usize).into();
    let workload = serde_json::from_value(workload).unwrap();
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    let (plan, _) = plan_workload(&workload, &inputs).unwrap();
    let plan_text = serde_json::to_string(&plan).unwrap();
    println!(
        "plan states (mentions): {:?}",
        ["DDSketch", "\"Kll\"", "Hll", "Cms", "Counter", "Max"]
            .map(|k| (k, plan_text.matches(k).count()))
    );

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let end_ms = (now_ms - 60_000).div_euclid(WINDOW_MS) * WINDOW_MS;
    let start_ms = end_ms - minutes * 60_000;
    let (view_cache_max_bytes, view_cache_idle_ms) = match budget.as_deref() {
        // Every window stays decoded: no budget (not even the cgroup's
        // default) and no idle eviction.
        Some("off") => (Some(u64::MAX), Some(0)),
        Some(bytes) => (Some(bytes.parse().unwrap()), None),
        None => (None, None),
    };
    let options = EngineOptions {
        ingestion: IngestionOptions {
            stream_start_ms: Some(start_ms),
            allowed_lateness_ms: std::env::var("ALLOWED_LATENESS_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(IngestionOptions::default().allowed_lateness_ms),
            ..IngestionOptions::default()
        },
        no_forward: true,
        precompute_threads: std::env::var("PRECOMPUTE_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(EngineOptions::default().precompute_threads),
        result_cache: std::env::var("RESULT_CACHE").map_or(true, |v| v != "0"),
        view_dir: Some(dir.clone()),
        view_cache_max_bytes,
        view_cache_idle_ms,
        view_read: match std::env::var("VIEW_READ").as_deref() {
            Ok("pread") => ViewRead::Pread,
            _ => ViewRead::Mmap,
        },
        ..EngineOptions::default()
    };
    println!(
        "{series} series per metric, {minutes} min, cache budget {budget:?}, cold page cache {cold}"
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
            // The harness's label tree: 4 regions, 5 services each, 10 jobs
            // each, 50 label_0 values each (10,000 leaves).
            [
                ("__name__", metric.clone()),
                ("region", format!("r{}", i / 2_500)),
                ("service", format!("s{}", i / 500)),
                ("job", format!("j{}", i / 50)),
                ("label_0", format!("l{i}")),
            ]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value,
            })
            .collect()
        })
        .collect();
    let zipf = std::env::var("VALUES").is_ok_and(|v| v == "zipf");
    // Counters' running totals, by series (metric-major, as `labels`).
    let mut totals = vec![0.0f64; labels.len()];
    let fed = Instant::now();
    let mut t = start_ms + scrape_ms;
    while t <= end_ms {
        let timeseries = labels
            .iter()
            .enumerate()
            .map(|(i, labels)| TimeSeries {
                labels: labels.clone(),
                samples: vec![Sample {
                    timestamp: t,
                    value: {
                        let h = (i as u64 ^ (t as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
                            .wrapping_mul(0xbf58_476d_1ce4_e5b9);
                        let u = ((h >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
                        let leaf = i % series;
                        if zipf {
                            // As the harness: a Zipf (s = 1.1, N = 1e6)
                            // integer per sample, times a per-series Zipf
                            // scale; counters add such increments and reset
                            // about once per 20 minutes.
                            let draw = (u.powf(-1.0 / 0.1)).floor().min(1e6);
                            if metrics[i / series].ends_with("_total") {
                                if (h >> 3).is_multiple_of(12_000) {
                                    totals[i] = 0.0;
                                }
                                totals[i] += draw;
                                totals[i]
                            } else {
                                let scale = (1e3 / (leaf as f64 + 1.0).powf(1.1)).round().max(1.0);
                                draw * scale
                            }
                        } else {
                            // A long-tailed value per sample: Pareto (shape
                            // 2) of a per-series scale (rank-scaled).
                            (1.0 + (leaf % 100) as f64) * (1.0 - u).powf(-0.5)
                        }
                    },
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
        if (t - start_ms) % 600_000 == 0 {
            println!(
                "fed {} min in {:.0} s; {}",
                (t - start_ms) / 60_000,
                fed.elapsed().as_secs_f64(),
                memory()
            );
        }
        t += scrape_ms;
    }
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/api/v1/precompute/watermark?time_ms={end_ms}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    println!(
        "fed and refreshed in {:.1} s; {}",
        fed.elapsed().as_secs_f64(),
        memory()
    );
    for round in 1..=rounds {
        if cold {
            drop_page_cache(&dir);
        }
        queries(&app, &plan.query_texts, end_ms, round).await;
        println!("after round {round}: {}", memory());
        println!("{}", counters(&state));
    }
}

/// Each query's latency at `end_ms`.
async fn queries(app: &Router, queries: &[String], end_ms: i64, round: usize) {
    for query in queries {
        let started = Instant::now();
        let uri = format!(
            "/api/v1/query?{}",
            form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .append_pair("time", &(end_ms / 1000).to_string())
                .finish()
        );
        let response = app
            .clone()
            .oneshot(Request::get(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        println!(
            "round {round}: {:>9.1} ms {status} {:>8} B  {query}",
            started.elapsed().as_secs_f64() * 1e3,
            body.len()
        );
    }
}

/// RSS split into anonymous and file pages, and the cgroup's view.
fn memory() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next()?.parse::<f64>().ok())
            .map_or(0.0, |kib| kib / 1024.0)
    };
    let mut out = format!(
        "RssAnon {:.0} MiB, RssFile {:.0} MiB",
        field("RssAnon:"),
        field("RssFile:")
    );
    if let Some(dir) = own_cgroup() {
        let read = |file: &str| std::fs::read_to_string(dir.join(file)).unwrap_or_default();
        let stat = read("memory.stat");
        let stat_field = |name: &str| {
            stat.lines()
                .find_map(|line| line.strip_prefix(name)?.trim().parse::<f64>().ok())
                .map_or(0.0, |bytes| bytes / 1048576.0)
        };
        let current = read("memory.current").trim().parse::<f64>().unwrap_or(0.0) / 1048576.0;
        // The working set as container monitors define it: memory.current
        // less the inactive file pages the kernel reclaims first.
        out += &format!(
            "; cgroup memory.max {}, working set {:.0} MiB, current {:.0} MiB, anon {:.0} MiB, file {:.0} MiB (active {:.0}, inactive {:.0}), oom_kill {}",
            read("memory.max").trim(),
            current - stat_field("inactive_file "),
            current,
            stat_field("anon "),
            stat_field("file "),
            stat_field("active_file "),
            stat_field("inactive_file "),
            read("memory.events").lines().find_map(|l| l.strip_prefix("oom_kill ")).unwrap_or("?"),
        );
    }
    out
}

/// This process's cgroup v2 directory.
fn own_cgroup() -> Option<PathBuf> {
    let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = line.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

/// Drops every view file's pages from the page cache.
fn drop_page_cache(dir: &Path) {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(file) = std::fs::File::open(&path) {
                use std::os::fd::AsRawFd;
                // SAFETY: a valid open descriptor; advice only.
                unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
            }
        }
    }
}

/// The tiering lines of `/metrics`.
fn counters(state: &DataPlane) -> String {
    state
        .engine
        .render_metrics()
        .lines()
        .filter(|line| {
            !line.starts_with('#')
                && (line.starts_with("asap_view_") || line.contains("phase=\"view_decode\""))
        })
        .collect::<Vec<_>>()
        .join("\n")
}
