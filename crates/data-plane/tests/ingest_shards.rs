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
use data_plane::{router, DataPlane};
use precompute::ingestion::{IngestionOptions, WallClock};
use prometheus_remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use serde_json::{json, Value};
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[30s]))";

fn plan_json() -> String {
    common::one_query_plan_json(QUERY, 64, json!({}))
}

fn data_plane(ingest_threads: usize) -> (Router, Arc<DataPlane>) {
    data_plane_with(ingest_threads, 0)
}

fn data_plane_with(ingest_threads: usize, shard_delay_ms: u64) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        ingest_threads,
        shard_delay_ms,
        ..common::no_forward_options()
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
        common::remote_write(app, timeseries).await;
    }
}

async fn answer(app: &Router, time_s: i64) -> Value {
    let (status, body) = status_and_answer(app, time_s).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

/// The status of `QUERY` at `time_s` and its body, results sorted by job.
async fn status_and_answer(app: &Router, time_s: i64) -> (StatusCode, Value) {
    status_and_answer_of(app, QUERY, time_s * 1000).await
}

/// 64 series over four shards answer as over one: each window's views are
/// built from the union of the shards' samples.
#[tokio::test(flavor = "multi_thread")]
async fn views_over_several_shards_equal_views_over_one() {
    let (one, _one_state) = data_plane(1);
    let (four, _four_state) = data_plane(4);
    for app in [&one, &four] {
        write(app, 64).await;
        common::barrier(app, 600_000).await;
    }
    for time_s in [560, 570, 580, 590, 600] {
        assert_eq!(
            answer(&four, time_s).await,
            answer(&one, time_s).await,
            "at {time_s}"
        );
    }
}

/// One shard runs ahead of seven slow ones: having answered a flush, it
/// runs the next minute's batch while the others still run this one. Its
/// newer samples must not move the watermark until every shard has run
/// the batches before them, or windows close without the slow shards'
/// samples. Every window answers as over one shard.
#[tokio::test(flavor = "multi_thread")]
async fn a_shard_running_ahead_does_not_close_windows_early() {
    let (one, _one_state) = data_plane(1);
    let (eight, _eight_state) = data_plane_with(8, 20);
    for app in [&one, &eight] {
        write(app, 64).await;
        common::barrier(app, 600_000).await;
    }
    let mut answered = 0;
    for time_s in (30..=600).step_by(10) {
        let expected = status_and_answer(&one, time_s).await;
        answered += usize::from(expected.0 == StatusCode::OK);
        assert_eq!(
            status_and_answer(&eight, time_s).await,
            expected,
            "at {time_s}"
        );
    }
    assert!(answered >= 5, "{answered} times answered");
}

/// Sends one Remote Write request: for each series `i` in `series`, one
/// sample per second at `seconds`, valued as [`value`].
async fn write_seconds(
    app: &Router,
    series: std::ops::Range<usize>,
    seconds: std::ops::RangeInclusive<i64>,
) {
    let timeseries = series
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
            samples: seconds
                .clone()
                .map(|second| Sample {
                    timestamp: second * 1000,
                    value: value(i, second),
                })
                .collect(),
        })
        .collect();
    common::remote_write(app, timeseries).await;
}

/// The value of series `i` at `second`, as [`write`] writes it.
fn value(i: usize, second: i64) -> f64 {
    (second * (i as i64 + 1) % 17) as f64
}

/// `QUERY`'s answer per job at `time_s`, from the samples of series
/// `0..64` in `(time_s - 30, time_s]`.
fn expected(time_s: i64) -> Vec<(String, f64)> {
    (0..4)
        .map(|job| {
            let sum = (0..64)
                .filter(|i| i % 4 == job)
                .flat_map(|i| ((time_s - 29).max(1)..=time_s).map(move |t| value(i, t)))
                .sum();
            (format!("job{job}"), sum)
        })
        .collect()
}

/// A scraper sends one timestamp over several requests. Half the series'
/// samples at 30 s, the end of the window `(20, 30]`, come in a later
/// request than the rest, after other samples at 30 s were routed; they
/// still join that window, over one shard or eight. The window closes only
/// once event time is past its end, as in the ASAPQuery precompute
/// engine's window manager (`closed_windows`, and its test
/// `test_open_closed_pane_ownership_at_grid_boundaries`).
#[tokio::test(flavor = "multi_thread")]
async fn samples_at_a_window_end_in_a_later_request_join_the_window() {
    for shards in [1, 8] {
        let (app, state) = data_plane(shards);
        write_seconds(&app, 0..64, 1..=29).await;
        write_seconds(&app, 0..32, 30..=30).await;
        write_seconds(&app, 32..64, 30..=30).await;
        write_seconds(&app, 0..64, 31..=40).await;
        common::barrier(&app, 40_000).await;
        assert_eq!(
            state.engine.stats().samples_dropped_late,
            0,
            "{shards} shards"
        );
        assert_eq!(
            common::per_job(&answer(&app, 30).await),
            expected(30),
            "{shards} shards"
        );
        assert_eq!(
            common::per_job(&answer(&app, 40).await),
            expected(40),
            "{shards} shards"
        );
    }
}

/// A sample older than a window already closed is dropped and counted,
/// and the window keeps its answer, over one shard or eight. Ported from
/// the ASAPQuery precompute worker's `test_late_data_drop`.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_sample_is_dropped_on_any_shard_count() {
    for shards in [1, 8] {
        let (app, state) = data_plane(shards);
        write_seconds(&app, 0..64, 1..=40).await;
        common::barrier(&app, 40_000).await;
        // Series 5 at 25 s again, with another value: the window (20, 30] is
        // closed.
        let timeseries = vec![TimeSeries {
            labels: [("__name__", "data"), ("instance", "i5"), ("job", "job1")]
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value: value.into(),
                })
                .collect(),
            samples: vec![Sample {
                timestamp: 25_000,
                value: 1000.0,
            }],
        }];
        common::remote_write(&app, timeseries).await;
        common::barrier(&app, 40_000).await;
        let started = std::time::Instant::now();
        while state.engine.stats().samples_dropped_late < 1 {
            assert!(started.elapsed().as_secs() < 10, "{shards} shards");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            state.engine.stats().samples_dropped_late,
            1,
            "{shards} shards"
        );
        assert_eq!(
            common::per_job(&answer(&app, 30).await),
            expected(30),
            "{shards} shards"
        );
    }
}

/// With one series, three of four shards never get a sample; the windows
/// still close as the busy shard's samples pass them, without a barrier.
/// A shard with no samples does not hold the watermark, as the ASAPQuery
/// precompute engine leaves workers not yet started out of its
/// cross-worker minimum (`test_compute_global_watermark_ignores_unstarted`).
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

const CLOCK_QUERY: &str = "sum by (job) (count_over_time(data[2s]))";

/// The data plane of `CLOCK_QUERY`, evaluated every second, over
/// `ingest_threads` shards, every shard but the first slowed by
/// `shard_delay_ms`; samples may be up to 5 s ahead of the wall clock.
fn clock_data_plane(
    ingest_threads: usize,
    shard_delay_ms: u64,
    stream_start_ms: i64,
) -> (Router, Arc<DataPlane>) {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": CLOCK_QUERY,
            "demand": {"fixed_interval_at": {"interval": 1000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": 640.0}},
          "sources": {"data": {"series_count": 64, "sample_interval": 100,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let plan = serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0)
        .unwrap();
    let options = EngineOptions {
        no_forward: true,
        ingest_threads,
        shard_delay_ms,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            max_future_ms: 5_000,
            stream_start_ms: Some(stream_start_ms),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state =
        Arc::new(DataPlane::with_options(plan, "http://127.0.0.1:1".into(), options).unwrap());
    (router(state.clone()), state)
}

/// A feeder stamps samples up to a second ahead of the wall clock, each
/// series at its own offset, as the demo's feeder does; the watermark then
/// follows the wall clock, which moves with every shard's report even when
/// no newer sample came. Over four shards, three slow, every window must
/// still hold every sample: the watermark moves only behind a flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_watermark_capped_at_the_clock_moves_only_behind_a_flush() {
    let start_ms = (precompute::ingestion::wall_clock_ms() / 1000 + 1) * 1000;
    let (app, state) = clock_data_plane(4, 250, start_ms);
    while precompute::ingestion::wall_clock_ms() < start_ms {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    // Every 100 ms for 3 s: each series one sample, series i stamped
    // (i + 1) / 64 s after the send time.
    let mut sent: Vec<(usize, i64)> = Vec::new();
    for _ in 0..30 {
        let now_ms = precompute::ingestion::wall_clock_ms();
        let timeseries = (0..64usize)
            .map(|i| {
                let timestamp = now_ms + (i as i64 + 1) * 1000 / 64;
                sent.push((i, timestamp));
                TimeSeries {
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
                    samples: vec![Sample {
                        timestamp,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        common::remote_write(&app, timeseries).await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let last_ms = sent.iter().map(|(_, t)| *t).max().unwrap();
    while precompute::ingestion::wall_clock_ms() <= last_ms {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    common::barrier(&app, last_ms).await;
    assert_eq!(state.engine.stats().samples_dropped_late, 0);
    let mut times = 0;
    for time_ms in (start_ms + 2000..=last_ms).step_by(1000) {
        let expected: Vec<(String, f64)> = (0..4)
            .map(|job| {
                let count = sent
                    .iter()
                    .filter(|(i, t)| i % 4 == job && *t > time_ms - 2000 && *t <= time_ms)
                    .count();
                (format!("job{job}"), count as f64)
            })
            .collect();
        let (status, body) = status_and_answer_of(&app, CLOCK_QUERY, time_ms).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(common::per_job(&body), expected, "at {time_ms}");
        times += 1;
    }
    assert!(times >= 1);
}

/// `query` at `time_ms`: its status and body, results sorted by job.
async fn status_and_answer_of(app: &Router, query: &str, time_ms: i64) -> (StatusCode, Value) {
    let uri = format!(
        "/api/v1/query?query={}&time={}",
        form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>(),
        time_ms as f64 / 1000.0
    );
    let (status, _, mut body) = common::get(app, &uri).await;
    if let Some(result) = body["data"]["result"].as_array_mut() {
        result.sort_by_key(|s| s["metric"]["job"].as_str().unwrap().to_string());
    }
    (status, body)
}

/// When the precompute cannot keep up (here every window's computation is
/// slow and its backlog holds one window), the coordinator waits for it;
/// the shards' samples then stay counted in their queues until the
/// coordinator takes their reports, so the queues fill and Remote Write
/// gets 503, rather than shards draining into an unbounded backlog of
/// reports.
#[tokio::test(flavor = "multi_thread")]
async fn a_precompute_that_cannot_keep_up_reaches_remote_write() {
    let dir =
        std::env::temp_dir().join(format!("ingest-shards-backpressure-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let options = EngineOptions {
        ingest_threads: 4,
        max_queued_samples: 2_000,
        max_precompute_backlog: 1,
        view_dir: Some(dir.clone()),
        slow_refreshes: vec![precompute::engine::SlowRefresh {
            group: None,
            end_ms: None,
            part: None,
            delay_ms: 2_000,
            per_series: false,
        }],
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    let app = router(state.clone());
    let mut refused = false;
    // A minute of 64 series per request, each closing six windows.
    for minute in 0..40i64 {
        let timeseries = (0..64usize)
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
                        value: value(i, second),
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
        if written.status() == StatusCode::SERVICE_UNAVAILABLE {
            refused = true;
            break;
        }
        assert_eq!(written.status(), StatusCode::NO_CONTENT);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(refused, "40 requests admitted while the precompute waited");
    drop(app);
    drop(state);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A barrier that comes while the coordinator waits for another
/// barrier's flush covers the writes acknowledged before it: it takes the
/// next pass, behind its own flush. Here three slow shards hold the first
/// barrier's flush while a write and a second barrier come.
#[tokio::test(flavor = "multi_thread")]
async fn a_barrier_during_another_barriers_flush_covers_writes_before_it() {
    let (app, _state) = data_plane_with(4, 300);
    write_seconds(&app, 0..64, 1..=60).await;
    let first = tokio::spawn(common::barrier_status(app.clone(), 60_000));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    write_seconds(&app, 0..64, 61..=70).await;
    assert_eq!(
        common::barrier_status(app.clone(), 70_000).await,
        StatusCode::OK
    );
    assert_eq!(first.await.unwrap(), StatusCode::OK);
    assert_eq!(common::per_job(&answer(&app, 70).await), expected(70));
}

/// Two barriers that come while one flush is waited for both answer.
#[tokio::test(flavor = "multi_thread")]
async fn barriers_during_one_flush_all_answer() {
    let (app, _state) = data_plane_with(4, 300);
    write_seconds(&app, 0..64, 1..=60).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let first = tokio::spawn(common::barrier_status(app.clone(), 50_000));
    let second = tokio::spawn(common::barrier_status(app.clone(), 60_000));
    assert_eq!(first.await.unwrap(), StatusCode::OK);
    assert_eq!(second.await.unwrap(), StatusCode::OK);
    assert_eq!(common::per_job(&answer(&app, 60).await), expected(60));
}

/// A sample stamped further from the epoch than the data plane evaluates
/// (`common::MAX_TIME_MS`) is dropped and counted, so window
/// arithmetic over it cannot overflow; later samples are ingested as
/// usual.
#[tokio::test(flavor = "multi_thread")]
async fn a_sample_beyond_the_time_range_is_dropped() {
    let (app, state) = data_plane(1);
    let timeseries = vec![TimeSeries {
        labels: [("__name__", "data"), ("instance", "i0"), ("job", "job0")]
            .into_iter()
            .map(|(name, value)| Label {
                name: name.into(),
                value: value.into(),
            })
            .collect(),
        samples: [i64::MIN, i64::MIN / 2]
            .into_iter()
            .map(|timestamp| Sample {
                timestamp,
                value: 1.0,
            })
            .collect(),
    }];
    common::remote_write(&app, timeseries).await;
    write(&app, 4).await;
    common::barrier(&app, 600_000).await;
    assert!(state.engine.is_running());
    assert_eq!(state.engine.stats().samples_dropped_late, 2);
    answer(&app, 600).await;
}

/// A data plane of `QUERY` over two shards whose ingestion reads `clock`:
/// no allowed lateness, the stream starting at 0.
fn idle_data_plane(clock: &WallClock) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        ingest_threads: 2,
        clock: clock.clone(),
        ..common::no_forward_options()
    };
    let state = Arc::new(
        DataPlane::with_options(plan_json(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

/// One coordinator pass at the clock's time, flushing every shard, with
/// every window it closes committed when it returns: a barrier at 0, which
/// closes nothing itself.
async fn pass(app: &Router) {
    common::barrier(app, 0).await;
}

/// Writes series `0..64` at 1..=40 s while the clock reads `written_ms`, and
/// runs them: the window `(30, 40]` is still open, as its end is the newest
/// sample.
async fn write_through_40s(app: &Router, clock: &WallClock, written_ms: i64) {
    clock.set(written_ms);
    write_seconds(app, 0..64, 1..=40).await;
    pass(app).await;
    assert_ne!(status_and_answer(app, 40).await.0, StatusCode::OK);
}

/// The window `(30, 40]` closed: answered from every sample at 1..=40 s.
/// Then one more pass, so the counters and the routing of the pass that
/// closed it are published (its refresh may commit before they are).
async fn wait_for_answer_at_40s(app: &Router, state: &DataPlane) {
    let started = std::time::Instant::now();
    while status_and_answer(app, 40).await.0 != StatusCode::OK {
        assert!(
            started.elapsed().as_secs() < 10,
            "{:?}",
            state.engine.stats()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(common::per_job(&answer(app, 40).await), expected(40));
    pass(app).await;
}

/// A stream that stops: once no sample has come for the allowed lateness
/// (0) and the 1 s grace past the last window's end, that window closes
/// without a sample or a barrier, and is answered.
#[tokio::test(flavor = "multi_thread")]
async fn the_last_window_of_a_stopped_stream_closes_by_wall_clock_time() {
    let clock = WallClock::manual(0);
    let (app, state) = idle_data_plane(&clock);
    write_through_40s(&app, &clock, 40_000).await;
    clock.set(41_000);
    wait_for_answer_at_40s(&app, &state).await;
    assert!(state.engine.stats().windows_closed_by_wall_clock > 0);
}

/// A replayed stream an hour behind the wall clock: its windows close by
/// wall-clock time counted from its newest sample, never by the wall clock
/// itself, so the window `(30, 40]` waits as long as for a live stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_stream_closes_no_window_early() {
    let hour_ms = 3_600_000;
    let clock = WallClock::manual(0);
    let (app, state) = idle_data_plane(&clock);
    write_through_40s(&app, &clock, hour_ms + 40_000).await;
    clock.set(hour_ms + 40_999);
    pass(&app).await;
    assert_ne!(status_and_answer(&app, 40).await.0, StatusCode::OK);
    assert_eq!(state.engine.stats().windows_closed_by_wall_clock, 0);
    clock.set(hour_ms + 41_000);
    wait_for_answer_at_40s(&app, &state).await;
}

/// The system clock steps 15 min forward (NTP, a resumed VM) while real
/// time does not move: the idle time is measured on the monotonic clock,
/// so the window `(30, 40]` does not close early; it closes once 1 s of
/// real time has passed.
#[tokio::test(flavor = "multi_thread")]
async fn a_wall_clock_step_closes_no_window_early() {
    let clock = WallClock::manual(0);
    let (app, state) = idle_data_plane(&clock);
    write_through_40s(&app, &clock, 40_000).await;
    clock.step_wall_clock(40_000 + 15 * 60_000);
    pass(&app).await;
    assert_ne!(status_and_answer(&app, 40).await.0, StatusCode::OK);
    assert_eq!(state.engine.stats().windows_closed_by_wall_clock, 0);
    clock.set(41_000 + 15 * 60_000);
    wait_for_answer_at_40s(&app, &state).await;
}

/// A sample for a window closed by wall-clock time is late: dropped and
/// counted, and the window keeps its answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_sample_after_a_wall_clock_close_is_late() {
    let clock = WallClock::manual(0);
    let (app, state) = idle_data_plane(&clock);
    write_through_40s(&app, &clock, 40_000).await;
    clock.set(41_000);
    wait_for_answer_at_40s(&app, &state).await;
    let dropped = state.engine.stats().samples_dropped_late;
    write_seconds(&app, 5..6, 35..=35).await;
    pass(&app).await;
    assert_eq!(state.engine.stats().samples_dropped_late, dropped + 1);
    assert_eq!(common::per_job(&answer(&app, 40).await), expected(40));
}
