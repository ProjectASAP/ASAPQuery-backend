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
use otlp::proto::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use otlp::proto::common::v1::{any_value, AnyValue, KeyValue};
use otlp::proto::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    Sum,
};
use otlp::proto::resource::v1::Resource;
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
        let body = prost::Message::encode_to_vec(&otlp_request());
        let (body, encoding) = if gzipped {
            (gzip(&body), Some("gzip"))
        } else {
            (body, None)
        };
        let (status, response) = post(&app, "/v1/metrics", encoding, body).await;
        assert_eq!(status, StatusCode::OK, "{response}");
        // An empty `ExportMetricsServiceResponse`: no bytes.
        assert_eq!(response, "");
        assert_eq!(answers(&state, &app).await, expected, "gzip: {gzipped}");
    }
}

/// The three decoders give the same samples: each of the same interned
/// series (the same `&'static Series`), timestamp and value.
#[test]
fn decoders_give_the_same_series_and_samples() {
    use data_plane::remote_write::{
        decode_otlp_samples, decode_samples, decode_victoriametrics_samples,
    };
    type Decoded = Vec<(usize, i64, u64)>;
    let identity = |samples: Vec<storage::store::RawSample>| -> Decoded {
        samples
            .into_iter()
            .map(|s| {
                (
                    s.labels as *const _ as usize,
                    s.timestamp_ms,
                    s.value.to_bits(),
                )
            })
            .collect()
    };
    let limits = prometheus_remote_write::Limits::default();
    let request = remote_write_request();
    let remote_write =
        identity(decode_samples(&prometheus_remote_write::encode(&request), &limits).unwrap());
    let victoriametrics = identity(
        decode_victoriametrics_samples(&victoriametrics_remote_write::encode(&request), &limits)
            .unwrap(),
    );
    assert_eq!(victoriametrics, remote_write);
    let otlp = identity(
        decode_otlp_samples(
            &prost::Message::encode_to_vec(&otlp_request()),
            None,
            &otlp::LabelOptions::default(),
            &limits,
        )
        .unwrap()
        .samples,
    );
    // The OTLP request lists the same series and samples in the same order.
    assert_eq!(otlp, remote_write);
    assert_eq!(otlp.len(), SERIES * END_S as usize);
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

/// An OTLP request posted with `content_type`: the status, the reply's
/// content type and body.
async fn post_otlp(
    app: &Router,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Option<String>, Vec<u8>) {
    let request = Request::post("/v1/metrics")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let reply_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|value| value.to_str().unwrap().to_string());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, reply_type, body.to_vec())
}

fn text_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

/// A `data` sum of `temporality` with one point per second from 1 s to
/// `points` s, for each of the six series.
fn otlp_sum(temporality: i32, points: i64) -> ExportMetricsServiceRequest {
    let mut request = otlp_request();
    for metric in &mut request.resource_metrics[0].scope_metrics[0].metrics {
        let Some(Data::Gauge(gauge)) = metric.data.take() else {
            unreachable!()
        };
        let mut data_points = gauge.data_points;
        data_points.truncate(points as usize);
        metric.data = Some(Data::Sum(Sum {
            data_points,
            aggregation_temporality: temporality,
            is_monotonic: true,
        }));
    }
    request
}

/// The value of the `/metrics` line `name`.
async fn metric_value(app: &Router, name: &str) -> f64 {
    let response = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} in {text}"))
        .parse()
        .unwrap()
}

/// `/v1/metrics` replies as OTLP/HTTP asks: 200 with an
/// `ExportMetricsServiceResponse` in protobuf (what OTLP exporters parse),
/// its `partial_success` counting the rejected points of DELTA sums; 400
/// with a `google.rpc.Status` for a malformed body; 415 with a JSON
/// `Status` for an OTLP/JSON body, which is not decoded.
#[tokio::test(flavor = "multi_thread")]
async fn otlp_replies_are_otlp_messages() {
    let (state, app) = data_plane();
    let protobuf = "application/x-protobuf";
    let (status, reply_type, body) = post_otlp(
        &app,
        protobuf,
        prost::Message::encode_to_vec(&otlp_request()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply_type.as_deref(), Some(protobuf));
    let reply: ExportMetricsServiceResponse = prost::Message::decode(&body[..]).unwrap();
    assert_eq!(reply, ExportMetricsServiceResponse::default());

    // Six series of 10 cumulative points and 20 delta points each.
    let (state_delta, app_delta) = data_plane();
    let cumulative = prost::Message::encode_to_vec(&otlp_sum(2, 10));
    let (status, _, body) = post_otlp(&app_delta, protobuf, cumulative).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    let delta = prost::Message::encode_to_vec(&otlp_sum(1, 20));
    let (status, reply_type, body) = post_otlp(&app_delta, protobuf, delta).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply_type.as_deref(), Some(protobuf));
    let reply: ExportMetricsServiceResponse = prost::Message::decode(&body[..]).unwrap();
    let partial = reply.partial_success.unwrap();
    assert_eq!(partial.rejected_data_points, 6 * 20);
    assert!(
        partial.error_message.contains("DELTA"),
        "{}",
        partial.error_message
    );
    common::barrier(&app_delta, END_S * 1000).await;
    assert_eq!(state_delta.engine.stats().samples_accepted, 6 * 10);
    assert_eq!(
        metric_value(&app_delta, "asap_otlp_rejected_points_total").await,
        120.0
    );
    assert_eq!(
        metric_value(&app_delta, "asap_phase_runs_total{phase=\"otlp_decode\"}").await,
        2.0
    );

    let (status, reply_type, body) = post_otlp(&app, protobuf, vec![0xff; 8]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(reply_type.as_deref(), Some(protobuf));
    let error: otlp::Status = prost::Message::decode(&body[..]).unwrap();
    assert!(error.message.contains("Protobuf"), "{}", error.message);

    let (status, reply_type, body) = post_otlp(
        &app,
        "application/json",
        br#"{"resourceMetrics":[]}"#.to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(reply_type.as_deref(), Some("application/json"));
    let error: Value = serde_json::from_slice(&body).unwrap();
    assert!(error["message"].as_str().unwrap().contains("OTLP/JSON"));
    common::barrier(&app, END_S * 1000).await;
    assert_eq!(
        state.engine.stats().samples_accepted,
        SERIES as u64 * END_S as u64
    );
}

/// Points flagged "no recorded value" are dropped and counted on
/// `/metrics`, not ingested as 0.
#[tokio::test(flavor = "multi_thread")]
async fn otlp_points_without_a_recorded_value_are_dropped() {
    let (state, app) = data_plane();
    let mut request = otlp_request();
    for metric in &mut request.resource_metrics[0].scope_metrics[0].metrics {
        let Some(Data::Gauge(gauge)) = &mut metric.data else {
            unreachable!()
        };
        gauge.data_points[0].flags = 1;
    }
    let (status, _, _) = post_otlp(
        &app,
        "application/x-protobuf",
        prost::Message::encode_to_vec(&request),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    common::barrier(&app, END_S * 1000).await;
    assert_eq!(
        state.engine.stats().samples_accepted,
        SERIES as u64 * (END_S as u64 - 1)
    );
    assert_eq!(
        metric_value(&app, "asap_otlp_dropped_no_value_points_total").await,
        SERIES as f64
    );
}

/// Resource attributes become `job` and `instance` as in Prometheus' OTLP
/// receiver (the `otlp` crate's tests cover the other rules): the planned
/// query sums by the `job` the resource names, over the point attributes'.
#[tokio::test(flavor = "multi_thread")]
async fn otlp_resource_attributes_give_job_and_instance() {
    let mut request = otlp_request();
    let resource_metrics = &mut request.resource_metrics[0];
    resource_metrics.resource = Some(Resource {
        attributes: vec![
            text_attribute("service.name", "cart"),
            text_attribute("service.namespace", "shop"),
            text_attribute("service.instance.id", "cart-7"),
            text_attribute("host.name", "node-1"),
            text_attribute("deployment.environment", "prod"),
        ],
        ..Default::default()
    });
    resource_metrics.scope_metrics[0].scope = Some(otlp::proto::common::v1::InstrumentationScope {
        name: "meter".into(),
        attributes: vec![text_attribute("scope.attribute", "s")],
        ..Default::default()
    });
    let body = prost::Message::encode_to_vec(&request);
    // One job, the sum of every series.
    let (state, app) = data_plane();
    let (status, _, _) = post_otlp(&app, "application/x-protobuf", body).await;
    assert_eq!(status, StatusCode::OK);
    let (answers, accepted) = answers(&state, &app).await;
    assert_eq!(accepted, SERIES as u64 * END_S as u64);
    let newest = common::per_job(answers.last().unwrap());
    let sum: f64 = (0..SERIES)
        .flat_map(|i| (END_S - 59..=END_S).map(move |s| value(i, s)))
        .sum();
    assert_eq!(newest, vec![("shop/cart".to_string(), sum)]);
}
