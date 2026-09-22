//! Three-layer overhead inspection; each matrix cell runs in a fresh process.
#[path = "overhead/fixture.rs"]
mod fixture;
use anyhow::{ensure, Context, Result};
use clap::Parser;
use data_plane::runtime_config::{process_snapshot, LogConfig, RuntimeConfig};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

#[derive(Parser, Debug)]
struct Args {
    #[command(flatten)]
    runtime: RuntimeConfig,
    #[command(flatten)]
    logging: LogConfig,
    #[arg(long, default_value = "1,2,4,8,16", value_delimiter = ',')]
    workers: Vec<usize>,
    #[arg(long, default_value = "1,2,4,8,16", value_delimiter = ',')]
    concurrency: Vec<usize>,
    #[arg(
        long,
        default_value = "sketch,raw_exact,backend,http",
        value_delimiter = ','
    )]
    layers: Vec<String>,
    #[arg(long, default_value_t = 10000)]
    samples: usize,
    #[arg(long, default_value_t = 1000)]
    requests: usize,
    #[arg(long, default_value_t = 100)]
    warmup: usize,
    #[arg(long, default_value_t = 3)]
    repeats: usize,
    /// Optional open-loop arrival rate. At the concurrency cap requests are dropped and counted.
    #[arg(long)]
    rate: Option<f64>,
    #[arg(long, default_value = "target/overhead")]
    output: PathBuf,
    #[arg(long, hide = true)]
    cell: bool,
}
fn cpu(clock: libc::clockid_t) -> f64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // CLOCK_PROCESS/THREAD_CPUTIME_ID read only initialized stack memory.
    assert_eq!(unsafe { libc::clock_gettime(clock, &mut time) }, 0);
    time.tv_sec as f64 + time.tv_nsec as f64 / 1e9
}
fn percentile(xs: &[f64], q: f64) -> Option<f64> {
    if xs.is_empty() {
        None
    } else {
        Some(
            xs[((xs.len() as f64 * q).ceil() as usize)
                .saturating_sub(1)
                .min(xs.len() - 1)],
        )
    }
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        args.requests > 0
            && args.samples > 0
            && args.repeats > 0
            && !args.workers.is_empty()
            && !args.concurrency.is_empty(),
        "empty experiment"
    );
    ensure!(
        args.workers.iter().chain(&args.concurrency).all(|n| *n > 0),
        "workers and concurrency must be positive"
    );
    ensure!(
        args.rate.is_none_or(|r| r.is_finite() && r > 0.0),
        "rate must be finite and positive"
    );
    ensure!(
        !args.layers.is_empty()
            && args
                .layers
                .iter()
                .all(|l| matches!(l.as_str(), "sketch" | "raw_exact" | "backend" | "http")),
        "unknown layer"
    );
    std::fs::create_dir_all(&args.output)?;
    if args.cell {
        return cell(&args);
    }
    let mut reports = Vec::new();
    for worker in &args.workers {
        for concurrency in &args.concurrency {
            for layer in &args.layers {
                for repeat in 0..args.repeats {
                    let dir = args
                        .output
                        .join(format!("{layer}-w{worker}-c{concurrency}-r{repeat}"));
                    let mut cmd = std::process::Command::new(std::env::current_exe()?);
                    cmd.args([
                        "--cell",
                        "--runtime-workers",
                        &worker.to_string(),
                        "--runtime-max-blocking-threads",
                        &args.runtime.runtime_max_blocking_threads.to_string(),
                        "--concurrency",
                        &concurrency.to_string(),
                        "--layers",
                        layer,
                        "--samples",
                        &args.samples.to_string(),
                        "--requests",
                        &args.requests.to_string(),
                        "--warmup",
                        &args.warmup.to_string(),
                        "--log-level",
                        &args.logging.log_level,
                        "--output",
                    ])
                    .arg(&dir);
                    if args.logging.disable_console_log {
                        cmd.arg("--disable-console-log");
                    }
                    if args.logging.disable_file_log {
                        cmd.arg("--disable-file-log");
                    }
                    if let Some(rate) = args.rate {
                        cmd.args(["--rate", &rate.to_string()]);
                    }
                    ensure!(cmd.status()?.success(), "cell failed: {}", dir.display());
                    reports.push(serde_json::from_slice::<Value>(&std::fs::read(
                        dir.join("report.json"),
                    )?)?);
                    std::fs::write(
                        args.output.join("matrix.json"),
                        serde_json::to_vec_pretty(&reports)?,
                    )?;
                }
            }
        }
    }
    let mut csv = String::from("layer,workers,concurrency,completed,dropped,error_rate,completed_per_second,p50_seconds,p95_seconds,p99_seconds,server_and_aux_cpu_seconds_per_completed\n");
    for report in &reports {
        let fields = [
            "layer",
            "workers",
            "concurrency",
            "completed",
            "dropped",
            "error_rate",
            "completed_per_second",
            "p50_seconds",
            "p95_seconds",
            "p99_seconds",
            "server_and_aux_cpu_seconds_per_completed",
        ];
        csv.push_str(
            &fields
                .iter()
                .map(|key| match &report[*key] {
                    Value::String(s) => s.clone(),
                    value => value.to_string(),
                })
                .collect::<Vec<_>>()
                .join(","),
        );
        csv.push('\n');
    }
    std::fs::write(args.output.join("matrix.csv"), csv)?;
    Ok(())
}

async fn operation(
    layer: &str,
    fixture: Arc<fixture::Fixture>,
    handle: tokio::runtime::Handle,
    client: reqwest::Client,
    url: String,
) -> Result<()> {
    let expected = if layer == "raw_exact" {
        fixture.exact_expected
    } else {
        fixture.expected
    };
    let value = if layer == "http" {
        let response = client
            .get(url)
            .query(&[("query", fixture::QUERY), ("time", "600")])
            .send()
            .await?
            .error_for_status()?;
        let source = response
            .headers()
            .get("x-asap-data-source")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("")
            .to_owned();
        ensure!(!source.contains("fallback"), "external fallback: {source}");
        let v: Value = response.json().await?;
        ensure!(
            v["status"] == "success"
                && v["data"]["result"].as_array().is_some_and(|r| r.len() == 1),
            "bad HTTP result: {v}"
        );
        v["data"]["result"][0]["value"][1]
            .as_str()
            .context("missing value")?
            .parse::<f64>()?
    } else {
        let layer = layer.to_owned();
        handle
            .spawn(async move {
                match layer.as_str() {
                    "sketch" => Ok(std::hint::black_box(
                        fixture.sketch.quantile(std::hint::black_box(0.5)).unwrap(),
                    )),
                    "raw_exact" => Ok(std::hint::black_box(fixture::exact(std::hint::black_box(
                        &fixture.raw,
                    )))),
                    _ => fixture.backend().await,
                }
            })
            .await??
    };
    ensure!(
        value.is_finite() && (value - expected).abs() <= expected.abs() * 1e-10,
        "result mismatch {value} != {expected}"
    );
    Ok(())
}

fn cell(args: &Args) -> Result<()> {
    ensure!(
        args.layers.len() == 1 && args.concurrency.len() == 1,
        "one layer/concurrency per child"
    );
    let _guard = args.logging.init(&args.output)?;
    let runtime = args.runtime.build()?;
    let fixture = Arc::new(fixture::Fixture::new(args.samples)?);
    let port = runtime
        .block_on(fixture.server.start_test_server())
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // Driver futures run on the calling OS thread, separate from server workers.
    let driver = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let handle = runtime.handle().clone();
    let layer = &args.layers[0];
    let concurrency = args.concurrency[0];
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let url = format!("http://127.0.0.1:{port}/api/v1/query");
    let before = process_snapshot();
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned());
    let result=driver.block_on(async {
        for _ in 0..args.warmup { operation(layer,fixture.clone(),handle.clone(),client.clone(),url.clone()).await?; }
        // Direct synchronous kernel timing excludes task dispatch. Concurrent layer
        // samples below deliberately include dispatch, reported separately.
        let kernel = if matches!(layer.as_str(),"sketch"|"raw_exact") {
            let start=Instant::now();
            for _ in 0..args.requests {
                if layer=="sketch" {std::hint::black_box(fixture.sketch.quantile(std::hint::black_box(0.5)));}
                else {std::hint::black_box(fixture::exact(std::hint::black_box(&fixture.raw)));}
            }
            Some(start.elapsed().as_secs_f64()/args.requests as f64)
        } else {None};
        let start_cpu=cpu(libc::CLOCK_PROCESS_CPUTIME_ID);
        let start_driver_cpu=cpu(libc::CLOCK_THREAD_CPUTIME_ID);
        let start=Instant::now();
        let mut tasks=JoinSet::new();
        let mut latencies=Vec::new(); let mut service_latencies=Vec::new();let mut errors=Vec::new();let mut dropped=0;
        for n in 0..args.requests {
            let scheduled=args.rate.map(|r|start+Duration::from_secs_f64(n as f64/r));
            if let Some(at)=scheduled {tokio::time::sleep_until(at.into()).await;}
            while let Some(result)=tasks.try_join_next() {record(result?,&mut latencies,&mut service_latencies,&mut errors);}
            if tasks.len()>=concurrency {
                if args.rate.is_some() {dropped+=1;continue;}
                if let Some(result)=tasks.join_next().await {record(result?,&mut latencies,&mut service_latencies,&mut errors);}
            }
            let fixture=fixture.clone();let handle=handle.clone();let client=client.clone();let url=url.clone();let layer=layer.clone();
            tasks.spawn(async move {
                let sent=Instant::now();
                let result=operation(&layer,fixture,handle,client,url).await;
                (scheduled.unwrap_or(sent).elapsed().as_secs_f64(),sent.elapsed().as_secs_f64(),result.err().map(|e|e.to_string()))
            });
        }
        while let Some(result)=tasks.join_next().await {record(result?,&mut latencies,&mut service_latencies,&mut errors);}
        let elapsed=start.elapsed().as_secs_f64();
        let total_cpu=cpu(libc::CLOCK_PROCESS_CPUTIME_ID)-start_cpu;
        let driver_cpu=cpu(libc::CLOCK_THREAD_CPUTIME_ID)-start_driver_cpu;
        latencies.sort_by(f64::total_cmp);service_latencies.sort_by(f64::total_cmp);
        let completed=latencies.len();
        Ok::<_,anyhow::Error>(json!({"layer":layer,"warmup_requests":args.warmup,"sketch_alpha":0.01,"quantile":0.5,"series":1,"workers":runtime.metrics().num_workers(),"max_blocking_threads":args.runtime.runtime_max_blocking_threads,
            "concurrency":concurrency,"samples":args.samples,"offered_requests":args.requests,"completed":completed,"dropped":dropped,"admitted":args.requests-dropped,
            "errors":errors,"error_rate":errors.len() as f64/args.requests as f64,"offered_rate":args.rate,
            "actual_offered_per_second":args.requests as f64/elapsed,"actual_admitted_per_second":(args.requests-dropped) as f64/elapsed,"completed_per_second":completed as f64/elapsed,
            "elapsed_seconds":elapsed,"p50_seconds":percentile(&latencies,0.5),"p95_seconds":percentile(&latencies,0.95),"p99_seconds":percentile(&latencies,0.99),
            "latency_seconds":latencies,"service_latency_seconds":service_latencies,"direct_kernel_seconds_per_query":kernel,
            "process_cpu_seconds":total_cpu,"driver_cpu_seconds":driver_cpu,"server_and_aux_cpu_seconds_per_completed":if completed>0 {Some((total_cpu-driver_cpu).max(0.0)/completed as f64)}else{None},
            "logging":args.logging,"effective_log_filter":args.logging.filter(),"before":before,"after":process_snapshot(),
            "git_revision":revision,"command":std::env::args().collect::<Vec<_>>(),"release_build":!cfg!(debug_assertions),"passed":errors.is_empty() && dropped==0}))
    })?;
    std::fs::write(
        args.output.join("report.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    ensure!(
        result["errors"].as_array().unwrap().is_empty(),
        "query failures; see report.json"
    );
    Ok(())
}
fn record(
    sample: (f64, f64, Option<String>),
    latencies: &mut Vec<f64>,
    service: &mut Vec<f64>,
    errors: &mut Vec<String>,
) {
    let (latency, work, error) = sample;
    if let Some(error) = error {
        errors.push(error);
    } else {
        latencies.push(latency);
        service.push(work);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Real installed-plan execution and production HTTP serialization agree with
    /// the direct sketch on both single-worker and multithreaded runtimes.
    #[test]
    fn production_paths_match_kernel() {
        for workers in [1, 2] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let fixture = Arc::new(fixture::Fixture::new(1000).unwrap());
                let port = fixture.server.start_test_server().await.unwrap();
                for layer in ["sketch", "raw_exact", "backend", "http"] {
                    operation(
                        layer,
                        fixture.clone(),
                        runtime.handle().clone(),
                        reqwest::Client::new(),
                        format!("http://127.0.0.1:{port}/api/v1/query"),
                    )
                    .await
                    .unwrap();
                }
            });
        }
    }
    /// Exact reference follows linear-interpolated median for odd/even unsorted data.
    #[test]
    fn exact_reference() {
        assert_eq!(fixture::exact(&[4., 1., 3., 2.]), 2.5);
        assert_eq!(fixture::exact(&[9., 1., 3.]), 3.);
    }
    /// Tail percentiles use nearest rank, including tiny sample sets.
    #[test]
    fn ranks() {
        assert_eq!(percentile(&[], 0.99), None);
        assert_eq!(percentile(&[1., 2., 3.], 0.99), Some(3.));
    }
    /// Failed requests are retained as failures, never successful latency samples.
    #[test]
    fn failure_accounting() {
        let (mut l, mut s, mut e) = (vec![], vec![], vec![]);
        record((1., 1., Some("failed".into())), &mut l, &mut s, &mut e);
        assert!(l.is_empty());
        assert_eq!(e.len(), 1);
    }
}
