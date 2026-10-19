//! One run of a workload: start Prometheus (and, for the ASAP run, the data
//! plane) pinned to cores, Remote Write the generated samples in time
//! order, evaluate every query at each of its interval ticks as Prometheus
//! rule evaluation would, and sample each process' CPU and memory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::generator::Generator;
use crate::workloads::WorkloadQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Prometheus alone answers every query.
    Baseline,
    /// The data plane executes the plan; Prometheus keeps raw samples.
    Asap,
}

pub struct RunConfig {
    pub mode: Mode,
    pub prometheus_bin: PathBuf,
    pub data_plane_bin: PathBuf,
    /// `plan.json` the data plane installs (ASAP run).
    pub plan: PathBuf,
    pub prometheus_cores: String,
    pub data_plane_cores: String,
    /// Queries are evaluated from this time on (Unix ms).
    pub eval_start_ms: i64,
    pub out_dir: PathBuf,
    pub prometheus_port: u16,
    pub data_plane_port: u16,
    pub execution_memory_limit_mib: usize,
}

/// Series per Remote Write request.
const WRITE_BATCH: usize = 10_000;

/// One evaluation of one query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evaluation {
    pub query: usize,
    pub time_ms: i64,
    pub latency_ms: f64,
    /// `plan`, `forwarded`, or `prometheus` in the baseline.
    pub execution: String,
    pub forward_reason: Option<String>,
    pub result: Vec<(BTreeMap<String, String>, f64)>,
}

/// CPU and memory of one process over the run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessUsage {
    pub name: String,
    pub cores: String,
    pub cpu_core_seconds: f64,
    pub peak_rss_gib: f64,
    pub mean_rss_gib: f64,
    pub samples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunMetrics {
    pub mode: Mode,
    pub wall_seconds: f64,
    pub samples_written: u64,
    pub evaluations: usize,
    pub processes: Vec<ProcessUsage>,
    pub prometheus_version: String,
}

struct Process {
    name: &'static str,
    cores: String,
    child: Child,
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn(
    name: &'static str,
    cores: &str,
    program: &Path,
    args: &[String],
    log: &Path,
) -> anyhow::Result<Process> {
    let log = std::fs::File::create(log)?;
    let child = Command::new("taskset")
        .arg("-c")
        .arg(cores)
        .arg(program)
        .args(args)
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .with_context(|| format!("starting {}", program.display()))?;
    Ok(Process {
        name,
        cores: cores.to_string(),
        child,
    })
}

async fn wait_ready(client: &reqwest::Client, url: &str) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(r) = client.get(url).send().await {
            if r.status().is_success() {
                return Ok(());
            }
        }
        if Instant::now() > deadline {
            bail!("{url} did not become ready");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// `/proc/<pid>/stat` utime + stime in seconds, and `VmRSS` in bytes.
fn proc_usage(pid: u32) -> Option<(f64, u64)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized command name.
    let rest = &stat[stat.rfind(')')? + 2..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks: f64 = fields.get(11)?.parse::<f64>().ok()? + fields.get(12)?.parse::<f64>().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kb: u64 = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((ticks / clock_ticks(), rss_kb * 1024))
}

fn clock_ticks() -> f64 {
    // CLK_TCK is 100 on every Linux target this harness runs on.
    100.0
}

/// Samples every process each 250 ms until `stop`.
fn sampler(pids: Vec<u32>, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<Vec<Vec<(f64, u64)>>> {
    std::thread::spawn(move || {
        let mut series = vec![Vec::new(); pids.len()];
        while !stop.load(Ordering::Relaxed) {
            for (i, pid) in pids.iter().enumerate() {
                if let Some(u) = proc_usage(*pid) {
                    series[i].push(u);
                }
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        for (i, pid) in pids.iter().enumerate() {
            if let Some(u) = proc_usage(*pid) {
                series[i].push(u);
            }
        }
        series
    })
}

async fn post_write(client: &reqwest::Client, url: &str, body: &[u8]) -> anyhow::Result<()> {
    for attempt in 0.. {
        let response = client
            .post(url)
            .header("Content-Encoding", "snappy")
            .header("Content-Type", "application/x-protobuf")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .body(body.to_vec())
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        if status.is_server_error() && attempt < 1000 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        bail!("{url}: {status}: {text}");
    }
    unreachable!()
}

async fn instant_query(
    client: &reqwest::Client,
    base: &str,
    query: &str,
    time_ms: i64,
) -> anyhow::Result<(
    f64,
    String,
    Option<String>,
    Vec<(BTreeMap<String, String>, f64)>,
)> {
    let started = Instant::now();
    let response = client
        .post(format!("{base}/api/v1/query"))
        .form(&[
            ("query", query),
            ("time", &format!("{:.3}", time_ms as f64 / 1000.0)),
        ])
        .send()
        .await?;
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let execution = header("x-asap-execution").unwrap_or_else(|| "prometheus".into());
    let reason = header("x-asap-forward-reason");
    let status = response.status();
    let body: Value = response.json().await?;
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    if !status.is_success() || body["status"] != "success" {
        bail!("{query} at {time_ms}: {status} {body}");
    }
    let data = &body["data"];
    let mut result = Vec::new();
    match data["resultType"].as_str() {
        Some("vector") => {
            for s in data["result"].as_array().into_iter().flatten() {
                let labels: BTreeMap<String, String> = serde_json::from_value(s["metric"].clone())?;
                let value = parse_value(s["value"][1].as_str().unwrap_or("NaN"));
                result.push((labels, value));
            }
        }
        Some("scalar") => {
            result.push((
                BTreeMap::new(),
                parse_value(data["result"][1].as_str().unwrap_or("NaN")),
            ));
        }
        other => bail!("{query}: result type {other:?}"),
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok((latency_ms, execution, reason, result))
}

fn parse_value(v: &str) -> f64 {
    match v {
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        v => v.parse().unwrap_or(f64::NAN),
    }
}

fn write_request(
    samples: Vec<crate::generator::GeneratedSample<'_>>,
    time_ms: i64,
) -> WriteRequest {
    WriteRequest {
        timeseries: samples
            .into_iter()
            .map(|s| TimeSeries {
                labels: s
                    .labels
                    .iter()
                    .map(|(name, value)| Label {
                        name: name.clone(),
                        value: value.clone(),
                    })
                    .collect(),
                samples: vec![Sample {
                    value: s.value,
                    timestamp: time_ms,
                }],
            })
            .collect(),
    }
}

/// Runs the workload once and writes `evaluations.jsonl` and `metrics.json`
/// to `config.out_dir`.
pub async fn run(
    config: &RunConfig,
    mut generator: Generator,
    queries: &[WorkloadQuery],
) -> anyhow::Result<RunMetrics> {
    let out = &config.out_dir;
    std::fs::create_dir_all(out)?;
    let tsdb = out.join("tsdb");
    let _ = std::fs::remove_dir_all(&tsdb);
    let prom_config = out.join("prometheus.yml");
    std::fs::write(&prom_config, "global:\n  evaluation_interval: 1h\n")?;
    let prometheus_version = Command::new(&config.prometheus_bin)
        .arg("--version")
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .unwrap_or_default();
    let prometheus_url = format!("http://127.0.0.1:{}", config.prometheus_port);
    let mut processes = vec![spawn(
        "prometheus",
        &config.prometheus_cores,
        &config.prometheus_bin,
        &[
            format!("--config.file={}", prom_config.display()),
            format!("--storage.tsdb.path={}", tsdb.display()),
            format!("--web.listen-address=127.0.0.1:{}", config.prometheus_port),
            "--web.enable-remote-write-receiver".into(),
            "--storage.tsdb.retention.time=3650d".into(),
            "--query.max-samples=500000000".into(),
        ],
        &out.join("prometheus.log"),
    )?];
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;
    wait_ready(&client, &format!("{prometheus_url}/-/ready")).await?;
    let data_plane_url = format!("http://127.0.0.1:{}", config.data_plane_port);
    if config.mode == Mode::Asap {
        processes.push(spawn(
            "data_plane",
            &config.data_plane_cores,
            &config.data_plane_bin,
            &[
                "--plan".into(),
                config.plan.display().to_string(),
                "--prometheus-url".into(),
                prometheus_url.clone(),
                "--listen".into(),
                format!("127.0.0.1:{}", config.data_plane_port),
                "--allowed-lateness-ms".into(),
                "0".into(),
                "--execution-memory-limit-mib".into(),
                config.execution_memory_limit_mib.to_string(),
            ],
            &out.join("data_plane.log"),
        )?);
        wait_ready(&client, &format!("{data_plane_url}/-/ready")).await?;
    }
    let query_url = match config.mode {
        Mode::Baseline => prometheus_url.clone(),
        Mode::Asap => data_plane_url.clone(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let pids: Vec<u32> = processes.iter().map(|p| p.child.id()).collect();
    // The children are `taskset`, which execs the program: same pid.
    let sampled = sampler(pids, stop.clone());

    let started = Instant::now();
    let mut evaluations =
        std::io::BufWriter::new(std::fs::File::create(out.join("evaluations.jsonl"))?);
    let evaluations_count = Arc::new(Mutex::new(0usize));
    let start_ms = generator.scale.start_ms;
    let mut samples_written = 0u64;
    let mut previous_tick: Option<i64> = None;
    let ticks: Vec<i64> = generator.ticks().collect();
    for t in ticks {
        let samples = generator.step(t);
        samples_written += samples.len() as u64;
        // Requests of at most WRITE_BATCH series stay under the data
        // plane's request body limit.
        let mut samples = samples;
        while !samples.is_empty() {
            let rest = samples.split_off(samples.len().min(WRITE_BATCH));
            let body = encode(&write_request(std::mem::replace(&mut samples, rest), t));
            post_write(&client, &format!("{prometheus_url}/api/v1/write"), &body).await?;
            if config.mode == Mode::Asap {
                post_write(&client, &format!("{data_plane_url}/api/v1/write"), &body).await?;
            }
        }
        // Every evaluation time in (previous tick, t]: its data has arrived.
        let from = previous_tick.map_or(t, |p| p + 1);
        previous_tick = Some(t);
        let mut due: Vec<(i64, usize)> = Vec::new();
        for (i, q) in queries.iter().enumerate() {
            let interval = q.interval_ms;
            let first = (from - start_ms + interval - 1).div_euclid(interval) * interval + start_ms;
            let mut time = first;
            while time <= t {
                if time >= config.eval_start_ms {
                    due.push((time, i));
                }
                time += interval;
            }
        }
        due.sort();
        for (time, i) in due {
            let (latency_ms, execution, forward_reason, result) =
                instant_query(&client, &query_url, &queries[i].query, time).await?;
            let e = Evaluation {
                query: i,
                time_ms: time,
                latency_ms,
                execution,
                forward_reason,
                result,
            };
            serde_json::to_writer(&mut evaluations, &e)?;
            std::io::Write::write_all(&mut evaluations, b"\n")?;
            *evaluations_count.lock().unwrap() += 1;
        }
    }
    let wall_seconds = started.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let series = sampled.join().expect("sampler");
    std::io::Write::flush(&mut evaluations)?;
    let usage = processes
        .iter()
        .zip(series)
        .map(|(p, s)| {
            let gib = |b: u64| b as f64 / (1u64 << 30) as f64;
            ProcessUsage {
                name: p.name.to_string(),
                cores: p.cores.clone(),
                // Cumulative since the process started, startup included.
                cpu_core_seconds: s.last().map_or(0.0, |x| x.0),
                peak_rss_gib: s.iter().map(|x| gib(x.1)).fold(0.0, f64::max),
                mean_rss_gib: if s.is_empty() {
                    0.0
                } else {
                    s.iter().map(|x| gib(x.1)).sum::<f64>() / s.len() as f64
                },
                samples: s.len(),
            }
        })
        .collect();
    let metrics = RunMetrics {
        mode: config.mode,
        wall_seconds,
        samples_written,
        evaluations: *evaluations_count.lock().unwrap(),
        processes: usage,
        prometheus_version,
    };
    std::fs::write(
        out.join("metrics.json"),
        serde_json::to_string_pretty(&metrics)?,
    )?;
    drop(processes);
    let _ = std::fs::remove_dir_all(&tsdb);
    Ok(metrics)
}
