//! End to end over the #509 PromQL examples: plan each workload, run the
//! `data_plane` binary against a Prometheus holding the samples, Remote
//! Write the same samples to it, and check that every planned query is
//! answered by the plan with what the Planner executor computes when the
//! selected plan runs entirely at query time over those samples.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::workload::PlanningWorkload;
use asap_planner::InstallablePlan;
use axum::extract::Query;
use axum::routing::get;
use axum::{Json, Router};
use control_plane::{plan_workload, save_plan, DeploymentInputs};
use data_plane::precompute::RawSample;
use data_plane::query::{QueryEngine, ResultSample};
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::sds::{Sds, SummaryStore};
use serde_json::{json, Value};

const MINUTE: i64 = 60_000;
const DAY: i64 = 24 * 60 * MINUTE;
/// #509 Example 3/4 Pattern A's batch time T (2026-01-01T00:00:00Z).
const PATTERN_A_T: i64 = 1_767_225_600_000;

fn workload(example: &str) -> PlanningWorkload {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../examples/planner-layering-{example}.json"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// `samples` of each series, every `step` ms in `[from, to)`; series `i`'s
/// value at `t` is `f(i, t)`.
fn samples(
    series: &[BTreeMap<String, String>],
    from: i64,
    to: i64,
    step: i64,
    f: impl Fn(usize, i64) -> f64,
) -> Vec<RawSample> {
    let series: Vec<_> = series.iter().cloned().map(Arc::new).collect();
    (from..to)
        .step_by(step as usize)
        .flat_map(|t| {
            series
                .iter()
                .enumerate()
                .map(move |(i, labels)| (i, labels.clone(), t))
        })
        .map(|(i, labels, timestamp_ms)| RawSample {
            labels,
            timestamp_ms,
            value: f(i, timestamp_ms),
        })
        .collect()
}

fn series(metric: &str, labels: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::from([("__name__".to_string(), metric.to_string())]);
    map.extend(labels.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    map
}

/// The answer of the Planner executor for `query` at `time_ms` when the
/// selected plan runs with nothing materialized: every input is a raw range
/// over `all`.
/// `plan` is the installed plan's `InstallablePlan::query_time_reference`.
fn reference(
    plan: &InstallablePlan,
    query: &str,
    time_ms: i64,
    all: &[RawSample],
) -> Vec<ResultSample> {
    let sds = Sds::from_plan(plan).unwrap();
    assert!(sds.definitions.is_empty());
    let engine = QueryEngine::new(plan, &sds);
    let store = SummaryStore::new();
    let (root, ranges) = engine.raw_ranges(query, time_ms, &store).unwrap();
    let raw = ranges
        .iter()
        .map(|range| {
            let samples = all
                .iter()
                .filter(|s| {
                    s.labels["__name__"] == range.metric
                        && range.start_ms < s.timestamp_ms
                        && s.timestamp_ms <= range.end_ms
                })
                .cloned()
                .collect();
            (range.node, samples)
        })
        .collect();
    sorted(engine.evaluate(root, time_ms, &store, None, &raw).unwrap())
}

fn sorted(mut samples: Vec<ResultSample>) -> Vec<ResultSample> {
    samples.sort_by(|a, b| a.labels.cmp(&b.labels));
    samples
}

/// A Prometheus holding `all`: range selectors `{__name__="m"}[Nms]` at
/// `time` return the samples in `[time - N, time]`, as Prometheus 2 does;
/// every other query returns an empty vector, and every range query an
/// empty matrix. Returns its URL and how many range selectors it has
/// answered.
async fn prometheus(all: Arc<Vec<RawSample>>) -> (String, Arc<AtomicUsize>) {
    let range_reads = Arc::new(AtomicUsize::new(0));
    let reads = range_reads.clone();
    let handler = move |Query(params): Query<BTreeMap<String, String>>| {
        let all = all.clone();
        let reads = reads.clone();
        async move {
            let query = &params["query"];
            // Anything but a raw range is a query the data plane forwarded
            // while its panes are still being built: answer it empty, and
            // `planned_answer` retries.
            let Some((metric, rest)) = query
                .strip_prefix("{__name__=\"")
                .and_then(|rest| rest.split_once("\"}["))
            else {
                return Json(
                    json!({"status": "success", "data": {"resultType": "vector", "result": []}}),
                );
            };
            let range: i64 = rest.strip_suffix("ms]").unwrap().parse().unwrap();
            reads.fetch_add(1, Ordering::SeqCst);
            let time = (params["time"].parse::<f64>().unwrap() * 1000.0).round() as i64;
            let mut by_series: BTreeMap<&BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
            for s in all.iter() {
                if s.labels["__name__"] == metric
                    && time - range <= s.timestamp_ms
                    && s.timestamp_ms <= time
                {
                    by_series
                        .entry(&s.labels)
                        .or_default()
                        .push(json!([s.timestamp_ms as f64 / 1000.0, s.value.to_string()]));
                }
            }
            let result: Vec<Value> = by_series
                .into_iter()
                .map(|(labels, values)| json!({"metric": labels, "values": values}))
                .collect();
            Json(json!({"status": "success", "data": {"resultType": "matrix", "result": result}}))
        }
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let empty_matrix = || async {
        Json(json!({"status": "success", "data": {"resultType": "matrix", "result": []}}))
    };
    let app = Router::new()
        .route("/api/v1/query", get(handler))
        .route("/api/v1/query_range", get(empty_matrix));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, range_reads)
}

/// The `data_plane` binary serving `plan`, killed on drop.
struct DataPlaneProcess {
    child: Child,
    url: String,
    _dir: TempDir,
}

impl Drop for DataPlaneProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn start(plan: &InstallablePlan, prometheus_url: &str) -> DataPlaneProcess {
    let dir = std::env::temp_dir().join(format!(
        "data-plane-e2e-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("plan.json");
    save_plan(plan, &path).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let child = Command::new(env!("CARGO_BIN_EXE_data_plane"))
        .args(["--plan", path.to_str().unwrap()])
        .args(["--prometheus-url", prometheus_url])
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .args(["--allowed-lateness-ms", "0"])
        .spawn()
        .unwrap();
    let process = DataPlaneProcess {
        child,
        url: format!("http://127.0.0.1:{port}"),
        _dir: TempDir(dir),
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let client = reqwest::Client::new();
    while client
        .get(format!("{}/-/ready", process.url))
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "data plane did not start");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    process
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn remote_write(url: &str, all: &[RawSample]) {
    let mut by_series: BTreeMap<&BTreeMap<String, String>, Vec<Sample>> = BTreeMap::new();
    for s in all {
        by_series.entry(&s.labels).or_default().push(Sample {
            value: s.value,
            timestamp: s.timestamp_ms,
        });
    }
    let timeseries = by_series
        .into_iter()
        .map(|(labels, samples)| TimeSeries {
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            samples,
        })
        .collect();
    let response = reqwest::Client::new()
        .post(format!("{url}/api/v1/write"))
        .body(encode(&WriteRequest { timeseries }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
}

/// The plan's answer to `query` at `time_ms`, retried until the plan answers
/// (stored panes are built asynchronously after Remote Write).
async fn planned_answer(url: &str, query: &str, time_ms: i64) -> Vec<ResultSample> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!("{url}/api/v1/query"))
            .query(&[
                ("query", query),
                ("time", &(time_ms as f64 / 1000.0).to_string()),
            ])
            .send()
            .await
            .unwrap();
        let execution = response.headers().get("x-asap-execution").cloned();
        let reason = response.headers().get("x-asap-forward-reason").cloned();
        let body: Value = response.json().await.unwrap();
        if execution.as_ref().is_some_and(|v| v == "plan") {
            let result = body["data"]["result"].as_array().unwrap();
            return sorted(
                result
                    .iter()
                    .map(|s| ResultSample {
                        labels: serde_json::from_value(s["metric"].clone()).unwrap(),
                        value: s["value"][1].as_str().unwrap().parse().unwrap(),
                    })
                    .collect(),
            );
        }
        assert!(
            Instant::now() < deadline,
            "{query} at {time_ms} was not answered by the plan: {reason:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The plan's answer to the range query `query` over `[start_ms, end_ms]`
/// by `step` (a Prometheus duration or float seconds), retried until the
/// plan answers: each step's samples, by step.
async fn planned_range_answer(
    url: &str,
    query: &str,
    start_ms: i64,
    end_ms: i64,
    step: &str,
) -> BTreeMap<i64, Vec<ResultSample>> {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(format!("{url}/api/v1/query_range"))
            .query(&[
                ("query", query),
                ("start", &(start_ms as f64 / 1000.0).to_string()),
                ("end", &(end_ms as f64 / 1000.0).to_string()),
                ("step", step),
            ])
            .send()
            .await
            .unwrap();
        let execution = response.headers().get("x-asap-execution").cloned();
        let reason = response.headers().get("x-asap-forward-reason").cloned();
        let body: Value = response.json().await.unwrap();
        if execution.as_ref().is_some_and(|v| v == "plan") {
            assert_eq!(body["data"]["resultType"], "matrix");
            let mut steps: BTreeMap<i64, Vec<ResultSample>> = BTreeMap::new();
            for series in body["data"]["result"].as_array().unwrap() {
                let labels: BTreeMap<String, String> =
                    serde_json::from_value(series["metric"].clone()).unwrap();
                for point in series["values"].as_array().unwrap() {
                    let t = (point[0].as_f64().unwrap() * 1000.0).round() as i64;
                    steps.entry(t).or_default().push(ResultSample {
                        labels: labels.clone(),
                        value: point[1].as_str().unwrap().parse().unwrap(),
                    });
                }
            }
            return steps.into_iter().map(|(t, s)| (t, sorted(s))).collect();
        }
        assert!(
            Instant::now() < deadline,
            "range {query} was not answered by the plan: {reason:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Answers `query` over `steps` (from `start_ms` by `step`, through
/// `end_ms`) as one range query and checks each step against the Planner
/// executor's reference and against the same instant query. Returns how
/// many raw ranges the range query read from Prometheus.
#[allow(clippy::too_many_arguments)]
async fn check_range(
    url: &str,
    reads: &AtomicUsize,
    at_query_time: &InstallablePlan,
    query: &str,
    (start_ms, end_ms, step): (i64, i64, &str),
    step_ms: i64,
    all: &[RawSample],
) -> usize {
    let before = reads.load(Ordering::SeqCst);
    let answer = planned_range_answer(url, query, start_ms, end_ms, step).await;
    let range_reads = reads.load(Ordering::SeqCst) - before;
    let steps: Vec<i64> = (start_ms..=end_ms).step_by(step_ms as usize).collect();
    assert_eq!(answer.keys().copied().collect::<Vec<_>>(), steps, "{query}");
    for (t, actual) in &answer {
        let expected = reference(at_query_time, query, *t, all);
        assert_same(&format!("{query} @ {t}"), actual, &expected, 0.0);
        let instant = planned_answer(url, query, *t).await;
        assert_eq!(actual, &instant, "{query} @ {t}: range vs instant");
    }
    eprintln!(
        "{query}: range over {} steps read {range_reads} raw ranges",
        steps.len()
    );
    range_reads
}

/// `actual` and `expected` hold the same series with values within
/// `tolerance` (plus float rounding).
fn assert_same(query: &str, actual: &[ResultSample], expected: &[ResultSample], tolerance: f64) {
    assert!(!expected.is_empty(), "{query}: the reference has no result");
    for (a, e) in actual.iter().zip(expected) {
        eprintln!(
            "{query}: {:?} plan {} executor {} difference {}",
            e.labels,
            a.value,
            e.value,
            a.value - e.value
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "{query}: {actual:?} vs {expected:?}"
    );
    for (a, e) in actual.iter().zip(expected) {
        assert_eq!(a.labels, e.labels, "{query}");
        let tolerance = tolerance + 1e-9 * e.value.abs().max(1.0);
        assert!(
            (a.value - e.value).abs() <= tolerance,
            "{query}: {a:?} vs {e:?}"
        );
    }
}

/// Runs every planned query of `example` at `time_ms` and compares it with
/// the reference, within `tolerance`. Returns how many stored summary
/// definitions the selected plan has.
async fn check_example(
    example: &str,
    inputs: &DeploymentInputs,
    all: Vec<RawSample>,
    time_ms: i64,
    tolerance: f64,
) -> usize {
    let workload = workload(example);
    let plan = plan_workload(&workload, inputs).unwrap();
    let at_query_time = plan.query_time_reference().unwrap();
    let all = Arc::new(all);
    let process = start(&plan, &prometheus(all.clone()).await.0).await;
    remote_write(&process.url, &all).await;
    for query in plan.queries.iter().map(|q| &q.query) {
        let expected = reference(&at_query_time, query, time_ms, &all);
        let actual = planned_answer(&process.url, query, time_ms).await;
        assert_same(query, &actual, &expected, tolerance);
    }
    let sds = Sds::from_plan(&plan).unwrap();
    sds.definitions.len()
}

/// A deployment that keeps no raw data: every input must be built at
/// ingestion time.
fn without_raw_data() -> DeploymentInputs {
    let mut inputs = DeploymentInputs::default();
    inputs.deployment_capabilities.raw_data_retained = false;
    inputs
}

/// Example 1: per-job request rates and top-k sums per job over three
/// counters; the Planner selects exact query-time plans for both.
#[tokio::test]
async fn example1() {
    let series = [
        series("http_requests_total", &[("job", "api"), ("instance", "1")]),
        series("http_requests_total", &[("job", "api"), ("instance", "2")]),
        series("http_requests_total", &[("job", "web"), ("instance", "1")]),
    ];
    let all = samples(&series, 0, 10 * MINUTE, 15_000, |i, t| {
        (i as f64 + 1.0) * t as f64 / 1000.0
    });
    let inputs = DeploymentInputs::default();
    let definitions = check_example("example1", &inputs, all, 5 * MINUTE, 0.0).await;
    assert_eq!(definitions, 0);
}

/// Example 3a: five p99 reports over historical years at T, built at query
/// time from raw samples; one sample per series per day. The five-year
/// windows hold more samples than the KLL's k, and its compaction is
/// randomized, so two runs agree only within the target: each is within
/// rank error 0.005 of the exact p99, about 5 for values spread evenly over
/// 0..997.
#[tokio::test]
async fn example3a() {
    let series = [
        series("latency_ms", &[("service", "a")]),
        series("latency_ms", &[("service", "b")]),
    ];
    let all = samples(
        &series,
        PATTERN_A_T - 6 * 365 * DAY,
        PATTERN_A_T,
        DAY,
        |i, t| (i * 10_000) as f64 + ((t / DAY) % 997) as f64,
    );
    let inputs = DeploymentInputs::default();
    let definitions = check_example("example3a", &inputs, all, PATTERN_A_T, 10.0).await;
    assert_eq!(definitions, 0);
}

/// Example 4a: Example 3a's reports repeated monthly, evaluated at T.
#[tokio::test]
async fn example4a() {
    let series = [series("latency_ms", &[("service", "a")])];
    let all = samples(
        &series,
        PATTERN_A_T - 6 * 365 * DAY,
        PATTERN_A_T,
        DAY,
        |_, t| ((t / DAY) % 997) as f64,
    );
    let inputs = DeploymentInputs::default();
    let definitions = check_example("example4a", &inputs, all, PATTERN_A_T, 10.0).await;
    assert_eq!(definitions, 0);
}

/// Example 3b: a p99 panel over the last 5 minutes, every minute; KLL is
/// exact below k. With raw data kept the Planner selects a query-time
/// build; without, tumbling panes built from Remote Write. Samples end at
/// the evaluation time, as for a live panel: panes that start more than the
/// plan's retention before the newest closed pane are evicted.
#[tokio::test]
async fn example3b() {
    let series = [series("latency_ms", &[("service", "a")])];
    let all = samples(&series, 0, 10 * MINUTE + 1, 15_000, |_, t| {
        ((t / 15_000) % 37) as f64
    });
    let inputs = DeploymentInputs::default();
    let definitions = check_example("example3b", &inputs, all.clone(), 10 * MINUTE, 0.0).await;
    assert_eq!(definitions, 0);
    let definitions = check_example("example3b", &without_raw_data(), all, 10 * MINUTE, 0.0).await;
    assert_eq!(definitions, 1);
}

/// Example 4b: the hourly p99 on a deployment that keeps no raw data, from
/// six ten-minute KLL panes built from Remote Write.
#[tokio::test]
async fn example4b() {
    let series = [
        series("latency_ms", &[("service", "a")]),
        series("latency_ms", &[("service", "b")]),
    ];
    let all = samples(&series, MINUTE, 75 * MINUTE, 15_000, |i, t| {
        (i * 1000) as f64 + ((t / 15_000) % 240) as f64
    });
    let definitions = check_example("example4b", &without_raw_data(), all, 70 * MINUTE, 0.0).await;
    assert_eq!(definitions, 1);
}

/// #509 Example 4's B3: Example 3b's p99 panel over ten series sampled every
/// millisecond, where the Planner keeps five 1-minute KLL panes built at
/// query time across evaluations. Three evaluations a minute apart match
/// the reference; the first reads all five panes' raw samples from
/// Prometheus, each later one only its newest pane. Samples run to 13 min,
/// so every pane read is closed and kept.
#[tokio::test]
async fn example4_b3() {
    let series = [
        series("latency_ms", &[("service", "a")]),
        series("latency_ms", &[("service", "b")]),
    ];
    let all = samples(&series, 0, 13 * MINUTE, 15_000, |i, t| {
        (i * 100) as f64 + (t / 15_000 * 25 % 101) as f64
    });
    let workload = workload("example4-b3");
    let inputs = DeploymentInputs::default();
    let plan = plan_workload(&workload, &inputs).unwrap();
    let at_query_time = plan.query_time_reference().unwrap();
    let sds = Sds::from_plan(&plan).unwrap();
    assert_eq!((sds.definitions.len(), sds.kept_definitions.len()), (0, 1));
    let all = Arc::new(all);
    let (prometheus_url, range_reads) = prometheus(all.clone()).await;
    let process = start(&plan, &prometheus_url).await;
    remote_write(&process.url, &all).await;
    let query = &plan.queries[0].query;
    let mut reads = Vec::new();
    for minute in [10, 11, 12] {
        let before = range_reads.load(Ordering::SeqCst);
        let expected = reference(&at_query_time, query, minute * MINUTE, &all);
        let actual = planned_answer(&process.url, query, minute * MINUTE).await;
        assert_same(query, &actual, &expected, 0.0);
        reads.push(range_reads.load(Ordering::SeqCst) - before);
    }
    eprintln!("{query}: raw ranges read per evaluation {reads:?}");
    assert_eq!(reads, vec![5, 1, 1]);
}

/// Example 1 as range queries, a step every 30 s over [3 min, 6 min]: each
/// query reads its one raw input from Prometheus once for all 7 steps, and
/// every step equals the instant query and the Planner executor.
#[tokio::test]
async fn example1_range() {
    let series = [
        series("http_requests_total", &[("job", "api"), ("instance", "1")]),
        series("http_requests_total", &[("job", "api"), ("instance", "2")]),
        series("http_requests_total", &[("job", "web"), ("instance", "1")]),
    ];
    // Quadratic counters: the rate changes at every step.
    let all = samples(&series, 0, 10 * MINUTE, 15_000, |i, t| {
        (i as f64 + 1.0) * (t as f64 / 1000.0).powi(2)
    });
    let workload = workload("example1");
    let inputs = DeploymentInputs::default();
    let plan = plan_workload(&workload, &inputs).unwrap();
    let at_query_time = plan.query_time_reference().unwrap();
    let all = Arc::new(all);
    let (prometheus_url, reads) = prometheus(all.clone()).await;
    let process = start(&plan, &prometheus_url).await;
    remote_write(&process.url, &all).await;
    for query in plan.queries.iter().map(|q| &q.query) {
        let range = (3 * MINUTE, 6 * MINUTE, "30s");
        let read = check_range(
            &process.url,
            &reads,
            &at_query_time,
            query,
            range,
            30_000,
            &all,
        )
        .await;
        assert_eq!(read, 1, "{query}");
    }
}

/// Example 4b as a range query on a deployment without raw data, its query
/// declared as range requests over 10 minutes: every step reads six stored
/// panes and nothing from Prometheus.
#[tokio::test]
async fn example4b_range() {
    let series = [
        series("latency_ms", &[("service", "a")]),
        series("latency_ms", &[("service", "b")]),
    ];
    // Values grow with time, so each hour's p99 differs.
    let all = samples(&series, MINUTE, 101 * MINUTE, 15_000, |i, t| {
        (i * 1000) as f64 + (t / 15_000) as f64
    });
    let workload = common::with_range_evaluation(
        &workload("example4b"),
        10 * MINUTE as u64,
        10 * MINUTE as u64,
    );
    let inputs = without_raw_data();
    let plan = plan_workload(&workload, &inputs).unwrap();
    let at_query_time = plan.query_time_reference().unwrap();
    let all = Arc::new(all);
    let (prometheus_url, reads) = prometheus(all.clone()).await;
    let process = start(&plan, &prometheus_url).await;
    remote_write(&process.url, &all).await;
    let query = &plan.queries[0].query;
    let range = (90 * MINUTE, 100 * MINUTE, "10m");
    let read = check_range(
        &process.url,
        &reads,
        &at_query_time,
        query,
        range,
        10 * MINUTE,
        &all,
    )
    .await;
    assert_eq!(read, 0);
}

/// Example 4's B3 as a range query, a step a minute over [10 min, 14 min]:
/// one Prometheus read covers the raw samples of all 9 kept panes, each
/// pane is built once and reused by later steps, and every step equals the
/// instant query and the Planner executor. Samples run to 15 min, so every
/// pane read is closed and kept. The same range query again reads one raw
/// range, for the panes ending at 6..9 min: they were evicted, measured from
/// the newest step.
#[tokio::test]
async fn example4_b3_range() {
    let series = [
        series("latency_ms", &[("service", "a")]),
        series("latency_ms", &[("service", "b")]),
    ];
    let all = samples(&series, 0, 15 * MINUTE, 15_000, |i, t| {
        (i * 100) as f64 + (t / 15_000 * 25 % 101) as f64
    });
    let workload = workload("example4-b3");
    let inputs = DeploymentInputs::default();
    let plan = plan_workload(&workload, &inputs).unwrap();
    let at_query_time = plan.query_time_reference().unwrap();
    let all = Arc::new(all);
    let (prometheus_url, reads) = prometheus(all.clone()).await;
    let process = start(&plan, &prometheus_url).await;
    remote_write(&process.url, &all).await;
    let query = &plan.queries[0].query;
    let range = (10 * MINUTE, 14 * MINUTE, "60");
    let read = check_range(
        &process.url,
        &reads,
        &at_query_time,
        query,
        range,
        MINUTE,
        &all,
    )
    .await;
    assert_eq!(read, 1);
    let read = check_range(
        &process.url,
        &reads,
        &at_query_time,
        query,
        range,
        MINUTE,
        &all,
    )
    .await;
    assert_eq!(read, 1, "the evicted panes");
}
