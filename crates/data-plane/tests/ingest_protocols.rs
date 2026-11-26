//! The VictoriaMetrics Remote Write (`/api/v1/import`) and OTLP
//! (`/v1/metrics`) routes ingest the samples a Prometheus Remote Write
//! request (`/api/v1/write`) with the same series and samples ingests.

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use data_plane::{router, DataPlane};
use http_body_util::BodyExt;
use otlp::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use otlp::proto::common::v1::{any_value, AnyValue, KeyValue};
use otlp::proto::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prometheus_remote_write::{Label, Sample, TimeSeries, WriteRequest};
use serde_json::{json, Value};
use tower::ServiceExt;

const QUERY: &str = "sum by (job) (sum_over_time(data[1m]))";
const SERIES: usize = 6;
/// Samples every second, from 1 s to `END_S`.
const END_S: i64 = 180;

fn value(series: usize, second: i64) -> f64 {
    ((second * (series as i64 + 1)) % 17) as f64
}

/// Series `i`'s labels: `data`, its instance and one of three jobs.
fn labels(i: usize) -> [(&'static str, String); 3] {
    [
        ("__name__", "data".to_string()),
        ("instance", format!("i{i}")),
        ("job", format!("job{}", i % 3)),
    ]
}

fn remote_write_request() -> WriteRequest {
    WriteRequest {
        timeseries: (0..SERIES)
            .map(|i| TimeSeries {
                labels: labels(i)
                    .into_iter()
                    .map(|(name, value)| Label {
                        name: name.into(),
                        value,
                    })
                    .collect(),
                samples: (1..=END_S)
                    .map(|second| Sample {
                        timestamp: second * 1000,
                        value: value(i, second),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// The same series and samples as one OTLP gauge per series, the labels
/// other than `__name__` as point attributes.
fn otlp_request() -> ExportMetricsServiceRequest {
    let metrics = (0..SERIES)
        .map(|i| {
            let attributes: Vec<KeyValue> = labels(i)[1..]
                .iter()
                .map(|(name, value)| KeyValue {
                    key: name.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value.clone())),
                    }),
                })
                .collect();
            Metric {
                name: "data".into(),
                data: Some(Data::Gauge(Gauge {
                    data_points: (1..=END_S)
                        .map(|second| NumberDataPoint {
                            attributes: attributes.clone(),
                            time_unix_nano: second as u64 * 1_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(value(i, second))),
                            ..Default::default()
                        })
                        .collect(),
                })),
                ..Default::default()
            }
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn data_plane() -> (Arc<DataPlane>, Router) {
    let state = Arc::new(
        DataPlane::with_options(
            common::one_query_plan_json(QUERY, SERIES, json!({})),
            "http://127.0.0.1:1".into(),
            common::no_forward_options(),
        )
        .unwrap(),
    );
    let app = router(state.clone());
    (state, app)
}

async fn post(
    app: &Router,
    uri: &str,
    content_encoding: Option<&str>,
    body: Vec<u8>,
) -> (StatusCode, String) {
    let mut request = Request::post(uri);
    if let Some(encoding) = content_encoding {
        request = request.header(header::CONTENT_ENCODING, encoding);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// After the barrier at `END_S`, `QUERY` at every 10 s evaluation of the
/// last minute, answered from the plan; and the samples the engine
/// accepted.
async fn answers(state: &DataPlane, app: &Router) -> (Vec<Value>, u64) {
    common::barrier(app, END_S * 1000).await;
    let mut answers = Vec::new();
    for time_s in (END_S - 60..=END_S).step_by(10) {
        let (status, execution, body) = common::get(app, &common::instant_uri(QUERY, time_s)).await;
        assert_eq!(status, StatusCode::OK, "at {time_s}: {body}");
        assert_eq!(execution.as_deref(), Some("plan"));
        answers.push(body);
    }
    (answers, state.engine.stats().samples_accepted)
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn victoriametrics_and_otlp_ingest_what_remote_write_ingests() {
    let (state, app) = data_plane();
    let (status, error) = post(
        &app,
        "/api/v1/write",
        None,
        prometheus_remote_write::encode(&remote_write_request()),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{error}");
    let expected = answers(&state, &app).await;
    assert_eq!(expected.1, SERIES as u64 * END_S as u64);
    // The sum per job of the last minute's samples, at the newest step.
    let newest = common::per_job(expected.0.last().unwrap());
    let sums: Vec<(String, f64)> = (0..3)
        .map(|job| {
            let sum = (0..SERIES)
                .filter(|i| i % 3 == job)
                .flat_map(|i| (END_S - 59..=END_S).map(move |s| value(i, s)))
                .sum();
            (format!("job{job}"), sum)
        })
        .collect();
    assert_eq!(newest, sums);

    let (state, app) = data_plane();
    let (status, error) = post(
        &app,
        "/api/v1/import",
        None,
        victoriametrics_remote_write::encode(&remote_write_request()),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{error}");
    assert_eq!(answers(&state, &app).await, expected);

    for gzipped in [false, true] {
        let (state, app) = data_plane();
        let body = otlp::encode(&otlp_request());
        let (body, encoding) = if gzipped {
            (gzip(&body), Some("gzip"))
        } else {
            (body, None)
        };
        let (status, response) = post(&app, "/v1/metrics", encoding, body).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response, r#"{"rejected":0}"#);
        assert_eq!(answers(&state, &app).await, expected, "gzip: {gzipped}");
    }
}

/// Malformed bodies are rejected with 400 and ingest nothing.
#[tokio::test]
async fn rejects_malformed_bodies() {
    let (state, app) = data_plane();
    for (uri, encoding) in [
        ("/api/v1/import", None),
        ("/v1/metrics", None),
        ("/v1/metrics", Some("gzip")),
    ] {
        let (status, _) = post(&app, uri, encoding, vec![0xff; 8]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {encoding:?}");
    }
    assert_eq!(state.engine.stats().samples_accepted, 0);
}
