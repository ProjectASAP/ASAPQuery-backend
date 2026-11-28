//! Query times as the Prometheus HTTP API takes them: `time`, `start` and
//! `end` are Unix seconds (with an optional fraction) or RFC 3339 (with
//! optional fractional seconds and a zone offset), as vmalert sends them.
//! A planned query is answered from the plan in either form, with the same
//! answer; a time in neither form is refused with Prometheus' 400.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use control_plane::plan_workload;
use data_plane::engine::EngineOptions;
use data_plane::ingestion::IngestionOptions;
use data_plane::remote_write::{encode, Label, Sample, TimeSeries, WriteRequest};
use data_plane::{router, DataPlane, EXECUTION_HEADER};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

const SUM: &str = "sum(sum_over_time(data[1m]))";
const SERIES: usize = 5;

/// The plan of `SUM` every 10 s over `SERIES` series of `data`; with
/// `no_forward`, a query the data plane does not answer from the plan is
/// refused instead of forwarded.
fn data_plane() -> Router {
    let workload = serde_json::from_value(json!({
        "query_workload": {"language": "promql", "repeating_queries": [{
            "query": SUM,
            "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
            "requirements": {},
            "predictability": {"predictable": {"known_at": null}},
        }]},
        "data_workload": {"arrival": {"ingesting": {"rows_per_second": SERIES as f64}},
          "sources": {"data": {"series_count": SERIES, "sample_interval": 1000,
            "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
    }))
    .unwrap();
    let plan = plan_workload(&workload, &common::inputs(false)).unwrap().0;
    let options = EngineOptions {
        no_forward: true,
        view_dir: None,
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    };
    let state = DataPlane::with_options(
        serde_json::to_string(&plan).unwrap(),
        "http://127.0.0.1:1".into(),
        options,
    )
    .unwrap();
    router(Arc::new(state))
}

/// One sample per second in `(0, to_s]` for every series, then a barrier
/// closing every window through `to_s`.
async fn write(app: &Router, to_s: i64) {
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
            samples: (1..=to_s)
                .map(|s| Sample {
                    timestamp: s * 1000,
                    value: (s + series as i64) as f64,
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

/// `endpoint`'s response to `SUM` with `params` (URL-encoded here): its
/// status, whether the plan answered it, and its body.
async fn get(app: &Router, endpoint: &str, params: &[(&str, &str)]) -> (StatusCode, bool, Value) {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("query", SUM);
    for (name, value) in params {
        query.append_pair(name, value);
    }
    let response = app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/{endpoint}?{}", query.finish()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let from_plan = response
        .headers()
        .get(EXECUTION_HEADER)
        .is_some_and(|value| value == "plan");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, from_plan, serde_json::from_slice(&body).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn rfc3339_times_are_answered_from_the_plan_like_unix_seconds() {
    let app = data_plane();
    write(&app, 180).await;

    // Instant: 120 s after the epoch, in each form.
    let (status, from_plan, expected) = get(&app, "query", &[("time", "120")]).await;
    assert_eq!((status, from_plan), (StatusCode::OK, true), "{expected}");
    assert_eq!(expected["data"]["result"].as_array().unwrap().len(), 1);
    for time in [
        "120.000",
        "1970-01-01T00:02:00Z",
        "1970-01-01T00:02:00.000Z",
        "1970-01-01T00:02:00.000000000Z",
        "1970-01-01T01:02:00+01:00",
        "1969-12-31T23:32:00-00:30",
    ] {
        let (status, from_plan, served) = get(&app, "query", &[("time", time)]).await;
        assert_eq!(
            (status, from_plan),
            (StatusCode::OK, true),
            "{time}: {served}"
        );
        assert_eq!(served, expected, "{time}");
    }

    // Range: 150 s to 180 s (inside the views' retention) every 10 s, in each form.
    let (status, from_plan, expected) = get(
        &app,
        "query_range",
        &[("start", "150"), ("end", "180"), ("step", "10")],
    )
    .await;
    assert_eq!((status, from_plan), (StatusCode::OK, true), "{expected}");
    for (start, end) in [
        ("1970-01-01T00:02:30Z", "1970-01-01T00:03:00Z"),
        ("1970-01-01T00:02:30.0Z", "180"),
        ("150", "1970-01-01T05:33:00+05:30"),
    ] {
        let (status, from_plan, served) = get(
            &app,
            "query_range",
            &[("start", start), ("end", end), ("step", "10")],
        )
        .await;
        assert_eq!(
            (status, from_plan),
            (StatusCode::OK, true),
            "{start}..{end}: {served}"
        );
        assert_eq!(served, expected, "{start}..{end}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn times_in_neither_form_are_refused_as_prometheus_refuses_them() {
    let app = data_plane();
    write(&app, 60).await;
    for bad in [
        "yesterday",
        "1970-01-01T00:02:00",
        "1970-01-01 00:02:00Z",
        "1970-13-01T00:00:00Z",
        "1970-02-30T00:00:00Z",
        "1970-01-01T24:00:00Z",
        "1970-01-01T00:02:00+25:00",
        "70-01-01T00:02:00Z",
        "1970-01-01T00:02:00.Z",
    ] {
        let (status, _, body) = get(&app, "query", &[("time", bad)]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}: {body}");
        assert_eq!(
            body,
            json!({"status": "error", "errorType": "bad_data",
                "error": format!("invalid parameter \"time\": cannot parse {bad:?} to a valid timestamp")}),
        );
        for name in ["start", "end"] {
            let mut params = vec![("start", "0"), ("end", "60"), ("step", "10")];
            params.retain(|(n, _)| *n != name);
            params.push((name, bad));
            let (status, _, body) = get(&app, "query_range", &params).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{name}={bad:?}: {body}");
            assert_eq!(
                body["error"],
                format!("invalid parameter \"{name}\": cannot parse {bad:?} to a valid timestamp"),
            );
        }
    }
    // An empty `start` or `end` is refused too; an empty `time` is now.
    for name in ["start", "end"] {
        let mut params = vec![("start", "0"), ("end", "60"), ("step", "10")];
        params.retain(|(n, _)| *n != name);
        params.push((name, ""));
        let (status, _, body) = get(&app, "query_range", &params).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}=\"\": {body}");
    }
    let (status, _, body) = get(&app, "query", &[("time", "")]).await;
    assert_ne!(status, StatusCode::BAD_REQUEST, "time=\"\": {body}");
}

/// Prometheus' min and max times ("no bound") are valid times the plan
/// does not evaluate at: forwarded, so refused here without forwarding,
/// not rejected as unparsable.
#[tokio::test(flavor = "multi_thread")]
async fn prometheus_min_and_max_times_are_not_answered_from_the_plan() {
    let app = data_plane();
    write(&app, 60).await;
    for time in [
        "-292273086-05-16T16:47:06Z",
        "292277025-08-17T07:12:55.999999999Z",
    ] {
        let (status, from_plan, body) = get(&app, "query", &[("time", time)]).await;
        assert!(
            status != StatusCode::BAD_REQUEST && !from_plan,
            "{time}: {status} {body}"
        );
    }
}
