//! Tiered view storage (docs/design/tiered-view-storage.md): decoded view
//! windows in a bounded cache, the rest read cold from the view files.
//! Tests V7–V12, V16 and V17 of the design's test plan, end to end through
//! the HTTP API; the parity test V1 is in `planner_executor_parity.rs` and
//! the unit tests are next to the code (`persist.rs`, `window_cache.rs`,
//! `memory_budget.rs`). Each compares a tiered data plane with one that
//! keeps every window in memory (the control). They fail until tiering is
//! implemented: install refuses the tiering options today.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use asap_planner::InstallablePlan;
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

/// An exact view (a sum per 10 s window) and a sketch view (a quantile
/// per 10 s window), each read over a minute.
const SUM: &str = "sum by (job) (sum_over_time(data[1m]))";
const QUANTILE: &str = "quantile_over_time(0.9, data[1m])";
const SERIES: usize = 20;

/// The plan of `queries` every 10 s over `SERIES` series of `data`,
/// without raw samples kept.
fn plan(queries: &[&str]) -> InstallablePlan {
    let repeating: Vec<Value> = queries
        .iter()
        .map(|query| {
            json!({
                "query": query,
                "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
                "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
                    "accuracy_scope": "whole_result"}},
                "predictability": {"predictable": {"known_at": null}},
            })
        })
        .collect();
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": repeating},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": SERIES as f64}},
          "sources": {"data": {"series_count": SERIES, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    plan_workload(&workload, &common::inputs(false)).unwrap().0
}

/// A fresh, empty directory for one data plane's view files.
fn view_dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("asap-tiering-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Options persisting views in `dir`, with a window cache of `max_bytes`
/// (`None`: no budget) and idle time `idle_ms` (`Some(0)`: never).
fn options(dir: &Path, max_bytes: Option<u64>, idle_ms: Option<u64>) -> EngineOptions {
    EngineOptions {
        no_forward: true,
        view_dir: Some(dir.to_path_buf()),
        view_cache_max_bytes: max_bytes,
        view_cache_idle_ms: idle_ms,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// The control: every window stays decoded in memory, as before tiering.
fn in_memory(dir: &Path) -> EngineOptions {
    options(dir, None, Some(0))
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

/// Remote Write of one sample per second in `(from_s, to_s]` for every
/// series (two jobs).
async fn send(app: &Router, from_s: i64, to_s: i64) {
    let timeseries = (0..SERIES)
        .map(|series| TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "data".into(),
                },
                Label {
                    name: "instance".into(),
                    value: format!("i{series}"),
                },
                Label {
                    name: "job".into(),
                    value: format!("job{}", series % 2),
                },
            ],
            samples: (from_s + 1..=to_s)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: ((s * 7 + series as i64 * 13) % 101) as f64 + 0.5,
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

/// Closes every window through `to_s`; answers once their views are
/// committed and written.
async fn barrier(app: &Router, to_s: i64) {
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={}",
                to_s * 1000
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
}

/// Sends `(from_s, to_s]` and closes its windows.
async fn write(app: &Router, from_s: i64, to_s: i64) {
    send(app, from_s, to_s).await;
    barrier(app, to_s).await;
}

async fn query(app: &Router, query: &str, time_s: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={time_s}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

/// An answered query's result, failing the test otherwise.
async fn answer(app: &Router, query_text: &str, time_s: i64) -> Value {
    let (status, body) = query(app, query_text, time_s).await;
    assert_eq!(status, StatusCode::OK, "{query_text} at {time_s}: {body}");
    let result = body["data"]["result"].clone();
    assert!(
        !result.as_array().unwrap().is_empty(),
        "{query_text} at {time_s}: no rows"
    );
    result
}

/// Whether two answers are equal: exactly for exact queries, within the
/// plan's 5 % for the KLL quantile (its compaction coin is random).
fn assert_same_answer(query_text: &str, a: &Value, b: &Value) {
    if query_text != QUANTILE {
        assert_eq!(a, b, "{query_text}");
        return;
    }
    let values = |v: &Value| -> Vec<(String, f64)> {
        let mut rows: Vec<(String, f64)> = v
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row["metric"].to_string(),
                    row["value"][1].as_str().unwrap().parse().unwrap(),
                )
            })
            .collect();
        rows.sort_by(|x, y| x.0.cmp(&y.0));
        rows
    };
    let (a, b) = (values(a), values(b));
    assert_eq!(a.len(), b.len(), "{query_text}");
    for ((ka, va), (kb, vb)) in a.iter().zip(&b) {
        assert_eq!(ka, kb, "{query_text}");
        assert!(
            (va - vb).abs() <= 0.05 * va.abs().max(vb.abs()),
            "{query_text}: {va} vs {vb}"
        );
    }
}

/// The value of the metric line starting with `name` (and its labels, if
/// given) in `state`'s `/metrics`.
fn metric(state: &DataPlane, name: &str) -> f64 {
    let text = state.engine.render_metrics();
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("{name} missing from /metrics"))
        .parse()
        .unwrap()
}

/// The control's answers to `queries` at `time_s` after `(0, time_s]`.
async fn control_answers(
    plan: &InstallablePlan,
    test: &str,
    queries: &[&str],
    time_s: i64,
) -> Vec<Value> {
    let dir = view_dir(&format!("{test}-control"));
    let (app, _state) = data_plane(plan, in_memory(&dir));
    write(&app, 0, time_s).await;
    let mut out = Vec::new();
    for query_text in queries {
        out.push(answer(&app, query_text, time_s).await);
    }
    let _ = std::fs::remove_dir_all(&dir);
    out
}

/// The segment files of refresh group 0, oldest first.
fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir.join("group-0"))
        .map(|entries| {
            entries
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|x| x == "seg"))
                .collect()
        })
        .unwrap_or_default();
    out.sort_by_key(|path| {
        path.file_stem()
            .and_then(|stem| stem.to_str()?.parse::<i64>().ok())
            .unwrap_or(i64::MAX)
    });
    out
}

/// V7. A window leaves memory only after its record is written: while the
/// disk is slow, committed windows are answered from memory (`Pending`);
/// once written, from the files, with the same answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_leaves_memory_only_after_it_is_written() {
    let plan = plan(&[SUM, QUANTILE]);
    let expected = control_answers(&plan, "pending", &[SUM, QUANTILE], 120).await;
    let dir = view_dir("pending");
    let (app, state) = data_plane(
        &plan,
        EngineOptions {
            persist_delay_ms: 300,
            persist_threads: 1,
            // Each query reads its views (a kept answer reads none).
            result_cache: false,
            ..options(&dir, Some(0), None)
        },
    );
    send(&app, 0, 121).await;
    let started = std::time::Instant::now();
    while state.engine.stats().windows_refreshed < 12 {
        assert!(
            started.elapsed().as_secs() < 10,
            "windows were not refreshed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(metric(&state, "asap_persist_backlog_windows") > 0.0);
    assert!(metric(&state, "asap_view_pending_bytes") > 0.0);
    for (query_text, expected) in [SUM, QUANTILE].iter().zip(&expected) {
        assert_same_answer(query_text, &answer(&app, query_text, 120).await, expected);
    }
    // Once written, the same windows are read from the files.
    barrier(&app, 120).await;
    assert_eq!(metric(&state, "asap_view_pending_bytes"), 0.0);
    let cold_before = metric(&state, "asap_view_cold_reads_total");
    for (query_text, expected) in [SUM, QUANTILE].iter().zip(&expected) {
        assert_same_answer(query_text, &answer(&app, query_text, 120).await, expected);
    }
    assert!(metric(&state, "asap_view_cold_reads_total") > cold_before);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V8. Windows that cannot be written (here: a view marked unpersistable,
/// as a state with no byte encoding or a write error) stay in memory and
/// are never read cold, whatever the budget.
#[tokio::test(flavor = "multi_thread")]
async fn windows_that_cannot_be_written_stay_in_memory() {
    let plan = plan(&[SUM]);
    let expected = control_answers(&plan, "pinned", &[SUM], 120).await;
    let view = plan.materialized_views().unwrap()[0].node_id;
    let dir = view_dir("pinned");
    let (app, state) = data_plane(
        &plan,
        EngineOptions {
            unpersistable_views: vec![view],
            ..options(&dir, Some(0), None)
        },
    );
    write(&app, 0, 120).await;
    assert!(metric(&state, "asap_view_pinned_bytes") > 0.0);
    assert_same_answer(SUM, &answer(&app, SUM, 120).await, &expected[0]);
    assert_eq!(metric(&state, "asap_view_cold_reads_total"), 0.0);
    assert!(metric(&state, "asap_view_windows_not_persisted_total") > 0.0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V9. Segments roll and are deleted past retention, so the files stay
/// bounded while the data plane runs; answers are unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn view_files_stay_bounded_by_retention() {
    let plan = plan(&[SUM]);
    let retention_ms = plan
        .materialized_views()
        .unwrap()
        .iter()
        .map(|view| view.retention_ms() as i64)
        .max()
        .unwrap();
    let segment_ms = 15_000;
    let expected = control_answers(&plan, "bounded", &[SUM], 600).await;
    let dir = view_dir("bounded");
    let (app, state) = data_plane(
        &plan,
        EngineOptions {
            view_segment_ms: segment_ms,
            ..options(&dir, None, None)
        },
    );
    let mut file_bytes = Vec::new();
    for minute in 1..=10 {
        write(&app, (minute - 1) * 60, minute * 60).await;
        // Sealed segments within retention, the one straddling its start,
        // and the active one.
        let bound = (retention_ms + segment_ms - 1) / segment_ms + 2;
        assert!(
            segments(&dir).len() as i64 <= bound,
            "minute {minute}: {} segments, at most {bound}",
            segments(&dir).len()
        );
        file_bytes.push(metric(&state, "asap_view_file_bytes"));
    }
    // Flat once retention is full: the last minutes add no more than the
    // first full one held.
    let full = ((retention_ms / 60_000) as usize + 1).min(8);
    assert!(
        file_bytes[9] <= file_bytes[full] * 1.5,
        "file bytes grow: {file_bytes:?}"
    );
    assert_same_answer(SUM, &answer(&app, SUM, 600).await, &expected[0]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V10. A segment deleted while a query reads it (its windows passed
/// retention meanwhile) stays readable: the query's answer is complete.
#[tokio::test(flavor = "multi_thread")]
async fn a_segment_deleted_during_a_read_stays_readable() {
    let plan = plan(&[SUM]);
    let expected = control_answers(&plan, "unlinked", &[SUM], 300).await;
    let dir = view_dir("unlinked");
    let (app, _state) = data_plane(
        &plan,
        EngineOptions {
            view_segment_ms: 10_000,
            cold_decode_delay_ms: 1_000,
            ..options(&dir, Some(0), None)
        },
    );
    write(&app, 0, 300).await;
    let first = segments(&dir).first().cloned().expect("no segments");
    let reader = {
        let app = app.clone();
        tokio::spawn(async move { answer(&app, SUM, 300).await })
    };
    // While the query decodes its windows (a second each), later windows
    // push those it reads past retention and their segments are deleted.
    write(&app, 300, 600).await;
    let started = std::time::Instant::now();
    while first.exists() {
        assert!(
            started.elapsed().as_secs() < 20,
            "the expired segment was not deleted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        !reader.is_finished(),
        "the query finished before the deletion; slow it down"
    );
    assert_same_answer(SUM, &reader.await.unwrap(), &expected[0]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V11. A restart indexes the same files without decoding any window up
/// front; the first queries decode what they read, and answer as before.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_indexes_without_decoding() {
    let plan = plan(&[SUM, QUANTILE]);
    let dir = view_dir("restart");
    let (app, state) = data_plane(&plan, options(&dir, None, None));
    write(&app, 0, 600).await;
    let before = vec![
        answer(&app, SUM, 600).await,
        answer(&app, QUANTILE, 600).await,
    ];
    drop((app, state));

    let (app, state) = data_plane(&plan, options(&dir, None, None));
    assert!(metric(&state, "asap_view_windows_loaded_total") > 0.0);
    assert_eq!(
        metric(&state, "asap_view_cold_reads_total"),
        0.0,
        "startup decoded windows"
    );
    assert_eq!(metric(&state, "asap_view_cache_entries"), 0.0);
    for (query_text, before) in [SUM, QUANTILE].iter().zip(&before) {
        assert_same_answer(query_text, &answer(&app, query_text, 600).await, before);
    }
    assert!(metric(&state, "asap_view_cold_reads_total") > 0.0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V12. After a crash that tore the last record of the active segment, a
/// restart answers from every whole window and keeps appending aligned
/// records.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_segment_tail_is_cut_at_restart() {
    let plan = plan(&[SUM]);
    let dir = view_dir("torn");
    let (app, state) = data_plane(&plan, options(&dir, None, None));
    write(&app, 0, 600).await;
    let before = answer(&app, SUM, 590).await;
    drop((app, state));
    // The last record (the window ending at 600 s) loses its last bytes.
    let last = segments(&dir).last().cloned().expect("no segments");
    let len = std::fs::metadata(&last).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&last)
        .unwrap()
        .set_len(len - 3)
        .unwrap();

    let (app, _state) = data_plane(&plan, options(&dir, None, None));
    assert_same_answer(SUM, &answer(&app, SUM, 590).await, &before);
    write(&app, 600, 720).await;
    answer(&app, SUM, 720).await;
    drop(app);
    let (app, _state) = data_plane(&plan, options(&dir, None, None));
    answer(&app, SUM, 720).await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// V16. Commits, persist acknowledgements, segment deletion, eviction and
/// concurrent queries race without changing an answer: 8 ingest shards, 4
/// precompute threads, 2 persist threads, 8 concurrent queries per step, a
/// cache of a few windows and 5 s segments, against the control at every
/// step.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commits_evictions_and_queries_answer_alike() {
    let plan = plan(&[SUM]);
    let busy = |options: EngineOptions| EngineOptions {
        ingest_threads: 8,
        precompute_threads: 4,
        persist_threads: 2,
        query_threads: 8,
        ..options
    };
    let (control_dir, dir) = (view_dir("stress-control"), view_dir("stress"));
    let (control, _c) = data_plane(&plan, busy(in_memory(&control_dir)));
    let (app, _state) = data_plane(
        &plan,
        busy(EngineOptions {
            view_segment_ms: 5_000,
            ..options(&dir, Some(16 << 10), Some(5_000))
        }),
    );
    for step in 1..=18 {
        let to_s = step * 10;
        write(&control, to_s - 10, to_s).await;
        write(&app, to_s - 10, to_s).await;
        if to_s < 60 {
            continue;
        }
        let expected = answer(&control, SUM, to_s).await;
        let readers: Vec<_> = (0..8)
            .map(|_| {
                let app = app.clone();
                tokio::spawn(async move { answer(&app, SUM, to_s).await })
            })
            .collect();
        for reader in readers {
            assert_same_answer(SUM, &reader.await.unwrap(), &expected);
        }
    }
    let _ = std::fs::remove_dir_all(&control_dir);
    let _ = std::fs::remove_dir_all(&dir);
}

/// V17. The cache, the window states, the files and cold reads are
/// exported.
#[tokio::test(flavor = "multi_thread")]
async fn tiering_metrics_are_exported() {
    let plan = plan(&[SUM, QUANTILE]);
    let dir = view_dir("metrics");
    let (app, state) = data_plane(&plan, options(&dir, Some(0), None));
    write(&app, 0, 120).await;
    answer(&app, SUM, 120).await;
    answer(&app, QUANTILE, 120).await;
    for name in [
        "asap_view_cache_bytes",
        "asap_view_cache_entries",
        "asap_view_pending_bytes",
        "asap_view_pinned_bytes",
        "asap_view_cache_hits_total",
        "asap_view_cache_evictions_total",
    ] {
        assert!(metric(&state, name) >= 0.0, "{name}");
    }
    assert!(metric(&state, "asap_view_file_bytes") > 0.0);
    assert!(metric(&state, "asap_view_cache_misses_total") > 0.0);
    assert!(metric(&state, "asap_view_cold_reads_total") > 0.0);
    assert!(metric(&state, "asap_view_cold_read_bytes_total") > 0.0);
    assert!(metric(&state, "asap_phase_runs_total{phase=\"view_decode\"}") > 0.0);
    let _ = std::fs::remove_dir_all(&dir);
}
