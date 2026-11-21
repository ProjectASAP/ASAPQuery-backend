//! The precompute task pool (`--precompute-threads`): windows computed on
//! several threads store the views one thread stores; a group's windows
//! are stored in window order even when a later one finishes first; a
//! window's run reads only committed views and raw data that does not
//! change while it runs; groups closing together are ready together; and
//! the precompute backlog still holds Remote Write back. No public
//! workload's plan has a group reading another group's views at
//! ingestion, so that order is covered by the scheduler's unit tests
//! (`engine.rs`).

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_planner::{InstallablePlan, PlanningWorkload};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::{EngineOptions, SlowRefresh};
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane};
use tower::ServiceExt;

/// Series per metric.
const SERIES: usize = 6;
/// Samples run from here (exclusive), every 10 s.
const START_MS: i64 = 0;
/// Through here: an hour.
const END_MS: i64 = 3_600_000;

fn workloads() -> Vec<(String, PlanningWorkload)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<PathBuf> = ["../examples", "tests/workloads"]
        .iter()
        .flat_map(|dir| std::fs::read_dir(root.join(dir)).unwrap())
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let name = path.file_stem().unwrap().to_string_lossy().to_string();
            let text = std::fs::read_to_string(&path).unwrap();
            (name, serde_json::from_str(&text).unwrap())
        })
        .collect()
}

/// Every plan of every public workload, raw data kept and not, planned
/// as they are taken.
fn plans() -> impl Iterator<Item = (String, InstallablePlan)> {
    workloads().into_iter().flat_map(|(name, workload)| {
        [true, false].into_iter().filter_map(move |raw| {
            let (plan, _) = plan_workload(&workload, &common::inputs(raw)).ok()?;
            Some((format!("{name} (raw data retained: {raw})"), plan))
        })
    })
}

/// For each refresh group, the other groups whose views it reads.
fn group_reads(plan: &InstallablePlan) -> Vec<BTreeSet<usize>> {
    let groups = plan.ingestion_refresh_groups().unwrap();
    let group_of: BTreeMap<_, usize> = groups
        .iter()
        .enumerate()
        .flat_map(|(i, g)| g.views.iter().map(move |v| (v.node_id, i)))
        .collect();
    groups
        .iter()
        .enumerate()
        .map(|(i, g)| {
            g.views
                .iter()
                .flat_map(|v| v.inputs.materialized_views.iter())
                .filter_map(|id| group_of.get(id).copied())
                .filter(|&other| other != i)
                .collect()
        })
        .collect()
}

/// A KLL sketch seeds its compaction coin at random, so two builds of one
/// window differ.
fn has_kll(plan: &InstallablePlan) -> bool {
    serde_json::to_string(plan)
        .unwrap()
        .to_lowercase()
        .contains("kll")
}

fn scanned_metrics(plan: &InstallablePlan) -> BTreeSet<String> {
    plan.dag
        .nodes
        .iter()
        .filter_map(|node| match &node.payload {
            Operator::NonASAP(NonASAPOp::Scan {
                source: Source::TimeSeries { metric },
                ..
            }) => Some(metric.clone()),
            _ => None,
        })
        .collect()
}

/// One Remote Write request per minute of event time: `SERIES` series of
/// every metric `plan` scans, every 10 s in `(from_ms, to_ms]`.
fn requests(plan: &InstallablePlan, series: usize, from_ms: i64, to_ms: i64) -> Vec<Vec<u8>> {
    let metrics = scanned_metrics(plan);
    let mut requests = Vec::new();
    let mut minute_start = from_ms;
    while minute_start < to_ms {
        let minute_end = (minute_start + 60_000).min(to_ms);
        let mut timeseries = Vec::new();
        for metric in &metrics {
            for i in 0..series {
                let samples: Vec<Sample> = (minute_start / 10_000 + 1..=minute_end / 10_000)
                    .map(|step| Sample {
                        timestamp: step * 10_000,
                        value: if metric.ends_with("_total") {
                            (step * (i as i64 + 1)) as f64
                        } else {
                            ((step * 7919 + i as i64 * 104_729) % 1000 - 100) as f64
                        },
                    })
                    .collect();
                timeseries.push(TimeSeries {
                    labels: [
                        ("__name__", metric.clone()),
                        ("job", format!("job{}", i % 3)),
                        ("instance", format!("i{i}")),
                        ("region", format!("r{}", i % 2)),
                        ("service", format!("s{}", i % 3)),
                    ]
                    .into_iter()
                    .map(|(name, value)| Label {
                        name: name.into(),
                        value,
                    })
                    .collect(),
                    samples,
                });
            }
        }
        requests.push(encode(&WriteRequest { timeseries }));
        minute_start = minute_end;
    }
    requests
}

fn options(threads: usize, view_dir: Option<PathBuf>) -> EngineOptions {
    EngineOptions {
        no_forward: true,
        precompute_threads: threads,
        view_dir,
        // Written, not synced: a barrier waits for the writes only, and the
        // comparison reads them back.
        view_sync_interval_ms: 3_600_000,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            max_future_ms: 0,
            stream_start_ms: Some(START_MS),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

fn data_plane(plan: &InstallablePlan, options: EngineOptions) -> (Router, Arc<DataPlane>) {
    let state = Arc::new(
        DataPlane::with_options(
            serde_json::to_string(plan).unwrap(),
            "http://127.0.0.1:1".into(),
            options,
        )
        .unwrap(),
    );
    (router(state.clone()), state)
}

async fn write(app: &Router, body: Vec<u8>) -> StatusCode {
    app.clone()
        .oneshot(
            Request::post("/api/v1/write")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn barrier(app: &Router, time_ms: i64) {
    let request = app.clone().oneshot(
        Request::post(format!("/api/v1/precompute/watermark?time_ms={time_ms}"))
            .body(Body::empty())
            .unwrap(),
    );
    let response = tokio::time::timeout(Duration::from_secs(120), request)
        .await
        .unwrap_or_else(|_| panic!("the barrier at {time_ms} was not answered"))
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

/// Feeds `plan` its samples through `END_MS` and closes every window,
/// with views persisted to a fresh directory; returns each view file's
/// bytes, the windows that failed, and whether some group's windows were
/// split by series.
async fn view_files(
    plan: &InstallablePlan,
    threads: usize,
    slow: Vec<SlowRefresh>,
) -> (BTreeMap<String, Vec<u8>>, u64, bool) {
    view_files_checked(plan, threads, slow, false).await
}

/// [`view_files`], failing each window whose run reads views not yet
/// committed through its end or whose inputs change while it runs.
async fn view_files_checked(
    plan: &InstallablePlan,
    threads: usize,
    slow: Vec<SlowRefresh>,
    check_task_inputs: bool,
) -> (BTreeMap<String, Vec<u8>>, u64, bool) {
    let dir = std::env::temp_dir().join(format!(
        "precompute-pool-{}-{threads}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let (app, state) = data_plane(
        plan,
        EngineOptions {
            slow_refreshes: slow,
            check_task_inputs,
            ..options(threads, Some(dir.clone()))
        },
    );
    for (minute, body) in requests(plan, SERIES, START_MS, END_MS)
        .into_iter()
        .enumerate()
    {
        assert_eq!(write(&app, body).await, StatusCode::NO_CONTENT);
        // A barrier after each minute moves the watermark one minute at a
        // time, so no run skips windows past their retention when the
        // watermark jumps (how far it jumps depends on timing); checking
        // inputs, ingestion runs on while windows compute instead.
        if !check_task_inputs {
            barrier(&app, START_MS + (minute as i64 + 1) * 60_000).await;
        }
    }
    barrier(&app, END_MS).await;
    let failed = state.engine.stats().windows_failed;
    let split = state.engine.render_metrics().lines().any(|line| {
        line.starts_with("asap_precompute_window_parts{")
            && line.rsplit(' ').next().is_some_and(|parts| parts != "1")
    });
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        files.insert(
            path.file_name().unwrap().to_string_lossy().to_string(),
            std::fs::read(&path).unwrap(),
        );
    }
    state.engine.stop();
    let _ = std::fs::remove_dir_all(&dir);
    (files, failed, split)
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// Every public workload plan without a KLL sketch stores byte-identical
/// view files with 1, 4, 8 and 12 precompute threads, and no window fails.
/// With more than one thread, a window of a group whose nodes compute each
/// series alone is split into one run per thread, by series, and merged;
/// with 12 threads most parts of the 6-series fixtures are empty.
#[tokio::test(flavor = "multi_thread")]
async fn views_are_byte_identical_with_one_four_eight_and_twelve_threads() {
    let mut compared = 0;
    let mut split = 0;
    let mut mismatches = Vec::new();
    for (label, plan) in plans() {
        if has_kll(&plan) {
            continue;
        }
        let (one, failed, _) = view_files(&plan, 1, Vec::new()).await;
        assert_eq!(failed, 0, "{label}: windows failed with one thread");
        let mut was_split = false;
        for threads in [4, 8, 12] {
            let (many, failed, parts) = view_files(&plan, threads, Vec::new()).await;
            was_split |= parts;
            if failed > 0 || many != one {
                mismatches.push(format!(
                    "{label}: {threads} threads: {failed} windows failed, files equal: {}",
                    many == one
                ));
            }
        }
        compared += 1;
        split += usize::from(was_split);
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(compared >= 10, "only {compared} plans compared");
    assert!(split >= 5, "only {split} plans had a group split by series");
}

/// The end of refresh group `group`'s first window after `START_MS`.
fn first_window_end(plan: &InstallablePlan, group: usize) -> i64 {
    let window = plan.ingestion_refresh_groups().unwrap()[group]
        .refresh_window
        .unwrap();
    let (width, phase) = (window.width_ms as i64, window.phase_ms as i64);
    (START_MS - phase).div_euclid(width) * width + phase + width
}

/// A plan of the public workloads with at least `groups` refresh groups
/// and no KLL sketch, chosen by `pick`.
fn plan_where(pick: impl Fn(&InstallablePlan) -> bool) -> (String, InstallablePlan) {
    plans()
        .find(|(_, plan)| !has_kll(plan) && pick(plan))
        .expect("no public workload plan fits")
}

/// A group's first window computed last (one part of it is slow; the
/// windows after it finish first) is still stored first: the later windows
/// wait in the group's reorder buffer, every window is stored and the
/// views equal one thread's.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_window_is_committed_before_the_later_windows_of_its_group() {
    let (label, plan) = plan_where(|plan| !plan.ingestion_refresh_groups().unwrap().is_empty());
    let slow = vec![SlowRefresh {
        group: Some(0),
        end_ms: Some(first_window_end(&plan, 0)),
        part: Some(0),
        delay_ms: 1_000,
        ..SlowRefresh::default()
    }];
    let (one, _, _) = view_files(&plan, 1, Vec::new()).await;
    let (four, failed, _) = view_files(&plan, 4, slow).await;
    assert_eq!(failed, 0, "{label}");
    assert!(four == one, "{label}: views differ from one thread's");
}

/// How long after the evaluation time a query is sent: here, the slowest
/// group's computation plus a margin, well under the groups' total.
const QUERY_DELAY: Duration = Duration::from_millis(1_400);

/// Groups whose windows close at one instant, each run taking 600 ms, are
/// all stored within the query delay with four threads (one thread takes
/// one run per group, in turn).
#[tokio::test(flavor = "multi_thread")]
async fn groups_closing_together_are_ready_within_the_query_delay() {
    let (label, plan) = plan_where(|plan| {
        plan.ingestion_refresh_groups().unwrap().len() >= 3
            && group_reads(plan).iter().all(BTreeSet::is_empty)
    });
    let groups = plan.ingestion_refresh_groups().unwrap();
    // The first instant after an hour of samples at which at least three
    // groups' windows end.
    let instant = (3_600_000..)
        .step_by(10_000)
        .find(|t| {
            groups
                .iter()
                .filter(|g| {
                    let w = g.refresh_window.unwrap();
                    (t - w.phase_ms as i64).rem_euclid(w.width_ms as i64) == 0
                })
                .count()
                >= 3
        })
        .unwrap();
    let closing: Vec<usize> = (0..groups.len())
        .filter(|&g| {
            let w = groups[g].refresh_window.unwrap();
            (instant - w.phase_ms as i64).rem_euclid(w.width_ms as i64) == 0
        })
        .collect();
    let slow = closing
        .iter()
        .map(|&group| SlowRefresh {
            group: Some(group),
            end_ms: Some(instant),
            delay_ms: 600,
            per_series: true,
            ..SlowRefresh::default()
        })
        .collect();
    let (app, _state) = data_plane(
        &plan,
        EngineOptions {
            slow_refreshes: slow,
            ..options(4, None)
        },
    );
    for body in requests(&plan, SERIES, START_MS, instant - 10_000) {
        assert_eq!(write(&app, body).await, StatusCode::NO_CONTENT);
    }
    barrier(&app, instant - 10_000).await;
    let started = Instant::now();
    barrier(&app, instant).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < QUERY_DELAY,
        "{label}: {} groups closing at {instant} ready after {elapsed:?}",
        closing.len()
    );
}

/// With every window slow and a backlog of two windows, the precompute
/// backlog fills, ingestion waits and Remote Write is refused with 503;
/// the backlog never holds more than its bound plus the windows the
/// threads hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_precompute_backlog_refuses_remote_write() {
    let (_, plan) = plan_where(|plan| !plan.ingestion_refresh_groups().unwrap().is_empty());
    let threads = 4;
    let max_backlog = 2;
    let (app, state) = data_plane(
        &plan,
        EngineOptions {
            max_precompute_backlog: max_backlog,
            max_queued_samples: 1,
            ingest_threads: 1,
            slow_refreshes: vec![SlowRefresh {
                group: None,
                end_ms: None,
                delay_ms: 200,
                ..SlowRefresh::default()
            }],
            ..options(threads, None)
        },
    );
    let mut refused = false;
    let mut largest = 0;
    let deadline = Instant::now() + Duration::from_secs(60);
    for body in requests(&plan, SERIES, START_MS, END_MS) {
        largest = largest.max(state.engine.precompute_backlog());
        match write(&app, body).await {
            StatusCode::NO_CONTENT => {}
            StatusCode::SERVICE_UNAVAILABLE => {
                refused = true;
                break;
            }
            status => panic!("Remote Write answered {status}"),
        }
        assert!(Instant::now() < deadline, "Remote Write was never refused");
    }
    assert!(refused, "Remote Write was never refused");
    // The backlog's bound, the window the coordinator is sending, the one
    // the queue thread holds and two per thread taken by the committer.
    let bound = (max_backlog + 2 + 2 * threads) as u64;
    for _ in 0..20 {
        largest = largest.max(state.engine.precompute_backlog());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(largest <= bound, "backlog {largest} over {bound}");
}

/// Every input a window's run reads is fixed before it starts and does not
/// change while it runs: the raw shards it was handed are the same
/// snapshots with the same samples after the run, while ingestion goes
/// on; the views are the same snapshot; and every view of the groups it
/// reads is committed through the window's end before it starts.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_reads_only_committed_views_and_unchanging_raw_data() {
    for (label, plan) in [
        plan_where(|plan| plan.ingestion_refresh_groups().unwrap().len() == 3),
        plan_where(|plan| plan.ingestion_refresh_groups().unwrap().len() >= 4),
    ] {
        // Slow runs, so Remote Write and the coordinator move on while
        // they run.
        let slow = vec![SlowRefresh {
            group: None,
            end_ms: None,
            delay_ms: 2,
            ..SlowRefresh::default()
        }];
        let (_, failed, _) = view_files_checked(&plan, 4, slow, true).await;
        assert_eq!(failed, 0, "{label}: a run read inputs not fixed before it");
    }
}

/// One refresh group of per-series views (the 60 s, 10 Hz dashboard), its
/// run taking 4 s over every series and a share of that over a share of
/// them, is ready within its 2 s deadline once a window is split by
/// series over eight threads; computed whole, on one thread, it is not.
#[tokio::test(flavor = "multi_thread")]
async fn a_big_window_split_by_series_is_ready_within_its_deadline() {
    const DEADLINE: Duration = Duration::from_secs(2);
    let (label, plan) = plans()
        .find(|(label, _)| {
            label.starts_with("typical-dashboard-60s-10hz (raw data retained: false)")
        })
        .expect("the 60 s dashboard plan");
    assert_eq!(plan.ingestion_refresh_groups().unwrap().len(), 1, "{label}");
    let (app, state) = data_plane(
        &plan,
        EngineOptions {
            slow_refreshes: vec![SlowRefresh {
                group: Some(0),
                end_ms: Some(1_200_000),
                delay_ms: 4_000,
                per_series: true,
                ..SlowRefresh::default()
            }],
            ..options(8, None)
        },
    );
    for body in requests(&plan, SERIES, START_MS, 1_200_000) {
        assert_eq!(write(&app, body).await, StatusCode::NO_CONTENT);
    }
    barrier(&app, 1_140_000).await;
    let started = Instant::now();
    barrier(&app, 1_200_000).await;
    let ready = started.elapsed();
    assert_eq!(state.engine.stats().windows_failed, 0, "{label}");
    assert!(
        ready < DEADLINE,
        "{label}: the window was ready after {ready:?}"
    );
}
