//! V20 (docs/design/tiered-view-storage.md): a query over cold view windows
//! allocates only its accumulators. The windows' states are read where
//! their segments are mapped and folded into one accumulator per series;
//! nothing rebuilds them. Measured with a counting global allocator: the
//! bytes allocated at once during the query are at least 10x below what the
//! same windows take decoded in memory.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use precompute::ingestion::{IngestionOptions, WallClock};
use prometheus_remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use serde_json::{json, Value};
use tower::ServiceExt;

/// Counts the bytes allocated now, and the most since the last reset.
struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            if new_size >= layout.size() {
                let now = CURRENT.fetch_add(new_size - layout.size(), Ordering::Relaxed) + new_size
                    - layout.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const QUERY: &str = "quantile_over_time(0.9, data[30m])";
const SERIES: usize = 400;
const END_S: i64 = 1_800;

fn plan() -> String {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": QUERY,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
              "accuracy_scope": "whole_result"}},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": SERIES as f64}},
          "sources": {"data": {"series_count": SERIES, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    serde_json::to_string(&plan_workload(&workload, &common::inputs(false)).unwrap().0).unwrap()
}

fn data_plane(
    plan: &str,
    view_dir: Option<std::path::PathBuf>,
    tiered: bool,
) -> (Router, Arc<DataPlane>) {
    let options = EngineOptions {
        clock: WallClock::stopped(),
        no_forward: true,
        result_cache: false,
        view_dir,
        // Tiered: nothing cached, every window read from its (sealed,
        // mapped) segment; one window per segment.
        view_cache_max_bytes: tiered.then_some(0),
        view_cache_idle_ms: (!tiered).then_some(0),
        view_segment_ms: if tiered {
            10_000
        } else {
            EngineOptions::default().view_segment_ms
        },
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = Arc::new(
        DataPlane::with_options(plan.into(), "http://127.0.0.1:1".into(), options).unwrap(),
    );
    (router(state.clone()), state)
}

async fn write(app: &Router) {
    for minute in 0..END_S / 60 {
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
                ],
                samples: (minute * 60 + 1..=minute * 60 + 60)
                    .map(|s| Sample {
                        timestamp: s * 1000,
                        value: ((s * 13 + series as i64 * 7) % 997) as f64 + 0.25,
                    })
                    .collect(),
            })
            .collect();
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/write")
                    .body(Body::from(encode(&WriteRequest { timeseries })))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let barrier = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/api/v1/precompute/watermark?time_ms={}",
                END_S * 1000
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(barrier.status(), StatusCode::OK);
}

/// The answer to `QUERY` at the end, by series.
async fn answer(app: &Router) -> Vec<(String, f64)> {
    let uri = format!(
        "/api/v1/query?query={}&time={END_S}",
        form_urlencoded::byte_serialize(QUERY.as_bytes()).collect::<String>()
    );
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let mut rows: Vec<(String, f64)> = body["data"]["result"]
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
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

fn metric(state: &DataPlane, name: &str) -> f64 {
    state
        .engine
        .render_metrics()
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("{name} missing"))
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cold_query_allocates_only_its_accumulators() {
    let plan = plan();
    // The control keeps every window decoded: what they take in memory.
    let (control, control_state) = data_plane(&plan, None, false);
    write(&control).await;
    let decoded_bytes = metric(&control_state, "asap_view_bytes") as usize;
    let expected = answer(&control).await;
    assert_eq!(expected.len(), SERIES);
    drop((control, control_state));

    let dir = std::env::temp_dir().join(format!("asap-cold-allocations-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (app, state) = data_plane(&plan, Some(dir.clone()), true);
    write(&app).await;
    // Warm-up: the first read maps the segments and builds their texts.
    answer(&app).await;
    let before = CURRENT.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let served = answer(&app).await;
    let allocated = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    assert!(metric(&state, "asap_view_cold_reads_total") > 0.0);
    // Same answers (the KLL plan answers within its 5 %).
    assert_eq!(served.len(), expected.len());
    for ((ks, vs), (ke, ve)) in served.iter().zip(&expected) {
        assert_eq!(ks, ke);
        assert!(
            (vs - ve).abs() <= 0.05 * ve.abs().max(1.0),
            "{ks}: {vs} vs {ve}"
        );
    }
    assert!(
        allocated * 10 <= decoded_bytes,
        "the cold query allocated {allocated} bytes at once; the windows take {decoded_bytes} decoded"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
