use super::*;
use proto::collector::metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse};
use proto::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use proto::metrics::v1::{
    metric::Data, number_data_point, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
    Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    Summary, SummaryDataPoint,
};
use proto::resource::v1::Resource;

const LIMITS: Limits = Limits {
    max_decompressed_bytes: 128 * 1024 * 1024,
    max_points: 1_000_000,
};

type Labels = Vec<(String, String)>;

/// A builder whose series handle is an index into `series`, the labels of
/// each series it interned, in order.
#[derive(Default)]
struct Labeled {
    series: Vec<Labels>,
}

impl SampleBuilder for Labeled {
    type Series = usize;
    type Sample = (usize, i64, f64);

    fn intern(&mut self, labels: &[(&str, &str)]) -> usize {
        self.series.push(
            labels
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
        );
        self.series.len() - 1
    }

    fn sample(series: usize, timestamp_ms: i64, value: f64) -> Self::Sample {
        (series, timestamp_ms, value)
    }
}

/// One sample, with its series' labels.
#[derive(Debug, PartialEq)]
struct Point {
    labels: Labels,
    timestamp_ms: i64,
    value: f64,
}

/// The points of `body`, decoded by a new decoder with `options`.
fn decode_with(
    body: &[u8],
    content_encoding: Option<&str>,
    options: &LabelOptions,
    limits: &Limits,
) -> Result<Decoded<Point>, OtlpError> {
    let mut decoder = Decoder::new(Labeled::default());
    let decoded = decoder.decode(body, content_encoding, options, limits)?;
    let series = &decoder.builder.series;
    Ok(Decoded {
        samples: decoded
            .samples
            .into_iter()
            .map(|(index, timestamp_ms, value)| Point {
                labels: series[index].clone(),
                timestamp_ms,
                value,
            })
            .collect(),
        rejected_points: decoded.rejected_points,
        dropped_no_value_points: decoded.dropped_no_value_points,
    })
}

fn decode(body: &[u8]) -> Vec<Point> {
    decode_with(body, None, &LabelOptions::default(), &LIMITS)
        .unwrap()
        .samples
}

fn encode(request: &ExportMetricsServiceRequest) -> Vec<u8> {
    request.encode_to_vec()
}

fn attribute(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn text(key: &str, value: &str) -> KeyValue {
    attribute(key, any_value::Value::StringValue(value.to_string()))
}

fn number_point(
    attributes: Vec<KeyValue>,
    nanos: u64,
    value: number_data_point::Value,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes,
        time_unix_nano: nanos,
        value: Some(value),
        ..Default::default()
    }
}

fn gauge(name: &str, data_points: Vec<NumberDataPoint>) -> Metric {
    metric(name, Data::Gauge(Gauge { data_points }))
}

fn metric(name: &str, data: Data) -> Metric {
    Metric {
        name: name.to_string(),
        data: Some(data),
        ..Default::default()
    }
}

/// A request with one resource of `resource` attributes and one scope
/// holding `metrics`.
fn request_with(resource: Vec<KeyValue>, metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource,
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "exporter".to_string(),
                    version: "1.0".to_string(),
                    attributes: vec![text("scope", "s")],
                    ..Default::default()
                }),
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A request from the `api-gateway` service.
fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    request_with(vec![text("service.name", "api-gateway")], metrics)
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels: Labels = pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    labels.sort();
    labels
}

const NANOS: u64 = 1_700_000_000_123_456_789;
const MS: i64 = 1_700_000_000_123;

/// Gauges and sums give one point each, labeled by the metric name, the
/// point's attributes and `job` from the resource's `service.name`, at
/// the point's time in ms. The scope's name, version and attributes are
/// not labels. A counter, a gauge and an up-down counter as the OTLP test
/// exporter sends them.
#[test]
fn gauges_and_sums_give_one_point_each() {
    let body = encode(&request(vec![
        metric(
            "http_requests_total",
            Data::Sum(Sum {
                data_points: vec![number_point(
                    vec![text("method", "GET"), text("status", "200")],
                    NANOS,
                    number_data_point::Value::AsInt(42),
                )],
                is_monotonic: true,
                aggregation_temporality: 2,
            }),
        ),
        gauge(
            "http_active_connections",
            vec![number_point(
                vec![
                    text("scope", "point's"),
                    attribute("shard", any_value::Value::IntValue(3)),
                ],
                NANOS,
                number_data_point::Value::AsDouble(-1.5),
            )],
        ),
    ]));
    assert_eq!(
        decode(&body),
        vec![
            Point {
                labels: labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "api-gateway"),
                    ("method", "GET"),
                    ("status", "200"),
                ]),
                timestamp_ms: MS,
                value: 42.0,
            },
            Point {
                labels: labels(&[
                    ("__name__", "http_active_connections"),
                    ("job", "api-gateway"),
                    ("scope", "point's"),
                    ("shard", "3"),
                ]),
                timestamp_ms: MS,
                value: -1.5,
            },
        ]
    );
}

/// Histograms, exponential histograms and summaries give `_sum` (when
/// set) and `_count` points; buckets and quantiles are not read.
#[test]
fn distributions_give_sum_and_count_points() {
    let body = encode(&request(vec![
        metric(
            "http_request_duration_seconds",
            Data::Histogram(Histogram {
                data_points: vec![
                    HistogramDataPoint {
                        time_unix_nano: NANOS,
                        count: 3,
                        sum: Some(0.45),
                        bucket_counts: vec![1, 2],
                        explicit_bounds: vec![0.1],
                        ..Default::default()
                    },
                    HistogramDataPoint {
                        attributes: vec![text("method", "PUT")],
                        time_unix_nano: NANOS,
                        count: 1,
                        sum: None,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
        ),
        metric(
            "size",
            Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: vec![text("method", "GET")],
                    time_unix_nano: NANOS,
                    count: 7,
                    sum: Some(70.0),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        metric(
            "latency",
            Data::Summary(Summary {
                data_points: vec![
                    SummaryDataPoint {
                        time_unix_nano: NANOS,
                        count: 2,
                        sum: 5.0,
                        ..Default::default()
                    },
                    // A zero sum is not encoded, and still reads 0.
                    SummaryDataPoint {
                        time_unix_nano: NANOS,
                        count: 4,
                        sum: 0.0,
                        attributes: vec![text("method", "GET")],
                        ..Default::default()
                    },
                ],
            }),
        ),
    ]));
    let points: Vec<(String, Option<String>, f64)> = decode(&body)
        .into_iter()
        .map(|point| {
            let label = |name: &str| {
                point
                    .labels
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, value)| value.clone())
            };
            (label("__name__").unwrap(), label("method"), point.value)
        })
        .collect();
    let get = Some("GET".to_string());
    let expected = vec![
        ("http_request_duration_seconds_sum", None, 0.45),
        ("http_request_duration_seconds_count", None, 3.0),
        (
            "http_request_duration_seconds_count",
            Some("PUT".into()),
            1.0,
        ),
        ("size_sum", get.clone(), 70.0),
        ("size_count", get.clone(), 7.0),
        ("latency_sum", None, 5.0),
        ("latency_count", None, 2.0),
        ("latency_sum", get.clone(), 0.0),
        ("latency_count", get, 4.0),
    ];
    let expected: Vec<(String, Option<String>, f64)> = expected
        .into_iter()
        .map(|(name, method, value)| (name.to_string(), method, value))
        .collect();
    assert_eq!(points, expected);
}

/// Sketch payload points and unnamed metrics are skipped; a point with no
/// value reads 0, as in ASAPQuery and Prometheus.
#[test]
fn skips_sketch_payloads_and_unnamed_metrics() {
    let sketch = attribute(
        "kll.sketch_payload",
        any_value::Value::BytesValue(vec![1, 2]),
    );
    let body = encode(&request(vec![
        gauge(
            "m",
            vec![
                number_point(vec![sketch], NANOS, number_data_point::Value::AsDouble(9.0)),
                NumberDataPoint {
                    time_unix_nano: NANOS,
                    ..Default::default()
                },
            ],
        ),
        gauge(
            "",
            vec![number_point(
                vec![],
                NANOS,
                number_data_point::Value::AsDouble(1.0),
            )],
        ),
    ]));
    let points = decode(&body);
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, 0.0);
}

/// Resource attributes are not labels: `service.name` gives `job`, as
/// `<service.namespace>/<service.name>` when the namespace is set, over a
/// point attribute `job`; `service.instance.id` gives `instance`. As
/// Prometheus' OTLP receiver does by default.
#[test]
fn resource_attributes_give_job_and_instance() {
    let point = || {
        gauge(
            "g",
            vec![number_point(
                vec![text("job", "point's"), text("region", "eu")],
                NANOS,
                number_data_point::Value::AsDouble(1.0),
            )],
        )
    };
    let sdk_resource = vec![
        text("service.name", "cart"),
        text("service.namespace", "shop"),
        text("service.instance.id", "cart-7"),
        text("host.name", "node-1"),
        text("telemetry.sdk.language", "rust"),
    ];
    let points = decode(&encode(&request_with(sdk_resource, vec![point()])));
    assert_eq!(
        points[0].labels,
        labels(&[
            ("__name__", "g"),
            ("instance", "cart-7"),
            ("job", "shop/cart"),
            ("region", "eu"),
        ])
    );
    // No `service.name`: the point's `job` stays.
    let points = decode(&encode(&request_with(
        vec![text("host.name", "node-1")],
        vec![point()],
    )));
    assert_eq!(
        points[0].labels,
        labels(&[("__name__", "g"), ("job", "point's"), ("region", "eu")])
    );
}

/// Promoted resource attributes are labels, their names escaped, unless a
/// point attribute gives the same label; resource attributes not named, or
/// absent, give nothing.
#[test]
fn promoted_resource_attributes_are_labels() {
    let body = encode(&request_with(
        vec![
            text("service.name", "cart"),
            text("deployment.environment", "prod"),
            text("region", "from-resource"),
            text("host.name", "node-1"),
        ],
        vec![gauge(
            "g",
            vec![number_point(
                vec![text("region", "eu")],
                NANOS,
                number_data_point::Value::AsDouble(1.0),
            )],
        )],
    ));
    let options = LabelOptions {
        promote_resource_attributes: vec![
            "deployment.environment".into(),
            "region".into(),
            "k8s.pod.name".into(),
        ],
    };
    let points = decode_with(&body, None, &options, &LIMITS).unwrap().samples;
    assert_eq!(
        points[0].labels,
        labels(&[
            ("__name__", "g"),
            ("deployment_environment", "prod"),
            ("job", "cart"),
            ("region", "eu"),
        ])
    );
}

/// Attribute names are escaped as Prometheus' default translation strategy
/// escapes them; names that escape to the same label join their values with
/// `;` in the order of the names; an empty value is no label.
#[test]
fn attribute_names_are_escaped_as_prometheus_does() {
    for (name, label) in [
        ("http.method", Some("http_method")),
        ("already_valid", Some("already_valid")),
        ("a..b", Some("a__b")),
        ("héllo", Some("h_llo")),
        ("1st", Some("key_1st")),
        ("_private", Some("key_private")),
        ("__reserved__", Some("__reserved__")),
        ("...", None),
        ("", None),
    ] {
        assert_eq!(label_name(name).as_deref(), label, "{name:?}");
    }
    let body = encode(&request_with(
        vec![],
        vec![gauge(
            "g",
            vec![number_point(
                vec![
                    text("a_b", "2"),
                    text("a.b", "1"),
                    text("empty", ""),
                    text("...", "dropped"),
                    attribute("on", any_value::Value::BoolValue(true)),
                    attribute("ratio", any_value::Value::DoubleValue(0.5)),
                ],
                NANOS,
                number_data_point::Value::AsDouble(1.0),
            )],
        )],
    ));
    assert_eq!(
        decode(&body)[0].labels,
        labels(&[
            ("__name__", "g"),
            ("a_b", "1;2"),
            ("on", "true"),
            ("ratio", "0.5"),
        ])
    );
}

/// The points of a sum with DELTA temporality are rejected and counted,
/// those of cumulative sums (and of unspecified temporality) ingested.
/// Histograms are read whatever their temporality.
#[test]
fn delta_sums_are_rejected() {
    let sum = |name: &str, temporality: i32, points: usize| {
        metric(
            name,
            Data::Sum(Sum {
                data_points: (0..points)
                    .map(|i| {
                        number_point(
                            vec![text("i", &i.to_string())],
                            NANOS,
                            number_data_point::Value::AsInt(i as i64),
                        )
                    })
                    .collect(),
                aggregation_temporality: temporality,
                is_monotonic: true,
            }),
        )
    };
    let delta_histogram = metric(
        "h",
        Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                time_unix_nano: NANOS,
                count: 1,
                ..Default::default()
            }],
            aggregation_temporality: 1,
        }),
    );
    let body = encode(&request(vec![
        sum("cumulative", 2, 2),
        sum("delta", 1, 3),
        sum("unspecified", 0, 1),
        delta_histogram,
    ]));
    let decoded = decode_with(&body, None, &LabelOptions::default(), &LIMITS).unwrap();
    let names: Vec<&str> = decoded
        .samples
        .iter()
        .map(|point| {
            point
                .labels
                .iter()
                .find(|(n, _)| n == "__name__")
                .unwrap()
                .1
                .as_str()
        })
        .collect();
    assert_eq!(
        names,
        ["cumulative", "cumulative", "unspecified", "h_count"]
    );
    assert_eq!(decoded.rejected_points, 3);
    assert_eq!(decoded.dropped_no_value_points, 0);
    assert!(decoded
        .rejection_message()
        .unwrap()
        .starts_with("3 data points"));
}

/// Points flagged "no recorded value" are dropped and counted, not read as
/// 0, whatever their kind.
#[test]
fn no_recorded_value_points_are_dropped() {
    let body = encode(&request(vec![
        gauge(
            "g",
            vec![
                number_point(vec![], NANOS, number_data_point::Value::AsDouble(1.0)),
                NumberDataPoint {
                    time_unix_nano: NANOS + 1_000_000,
                    flags: 1,
                    ..Default::default()
                },
            ],
        ),
        metric(
            "h",
            Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: NANOS,
                    flags: 1,
                    ..Default::default()
                }],
                aggregation_temporality: 2,
            }),
        ),
        metric(
            "s",
            Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: NANOS,
                    flags: 1,
                    ..Default::default()
                }],
            }),
        ),
    ]));
    let decoded = decode_with(&body, None, &LabelOptions::default(), &LIMITS).unwrap();
    assert_eq!(decoded.samples.len(), 1);
    assert_eq!(decoded.samples[0].value, 1.0);
    assert_eq!(decoded.dropped_no_value_points, 3);
    assert_eq!(decoded.rejected_points, 0);
    assert_eq!(decoded.rejection_message(), None);
}

/// A decoder interns each series once: a second request with the same
/// series hits its cache. A series differs by its resource's labels, its
/// metric name and its attributes; resource attributes that are not labels
/// do not make another series.
#[test]
fn a_decoder_interns_each_series_once() {
    let body = |resource: Vec<KeyValue>, name: &str, value: &str| {
        encode(&request_with(
            resource,
            vec![gauge(
                name,
                vec![number_point(
                    vec![text("a", value)],
                    NANOS,
                    number_data_point::Value::AsDouble(1.0),
                )],
            )],
        ))
    };
    let mut decoder = Decoder::new(Labeled::default());
    let mut series = |body: &[u8], options: &LabelOptions| {
        let decoded = decoder.decode(body, None, options, &LIMITS).unwrap();
        decoded.samples[0].0
    };
    let none = LabelOptions::default();
    let job = || vec![text("service.name", "x"), text("host.name", "h")];
    let first = series(&body(job(), "g", "1"), &none);
    assert_eq!(series(&body(job(), "g", "1"), &none), first);
    assert_eq!(
        series(&body(vec![text("service.name", "x")], "g", "1"), &none),
        first
    );
    let others = [
        series(&body(job(), "g", "2"), &none),
        series(&body(job(), "g2", "1"), &none),
        series(&body(vec![text("service.name", "y")], "g", "1"), &none),
        series(
            &body(job(), "g", "1"),
            &LabelOptions {
                promote_resource_attributes: vec!["host.name".into()],
            },
        ),
    ];
    assert_eq!(others, [1, 2, 3, 4]);
    assert_eq!(decoder.builder.series.len(), 5);
}

/// Attributes interleaved with a point's other fields decode as the same
/// attributes written together.
#[test]
fn interleaved_attributes_decode_as_contiguous_ones() {
    let field = |number: u32, bytes: &[u8]| {
        let mut out = Vec::new();
        prost::encoding::encode_key(number, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(bytes.len() as u64, &mut out);
        out.extend_from_slice(bytes);
        out
    };
    let a = text("a", "1").encode_to_vec();
    let b = text("b", "2").encode_to_vec();
    let mut time = Vec::new();
    prost::encoding::fixed64::encode(3, &NANOS, &mut time);
    let mut value = Vec::new();
    prost::encoding::double::encode(4, &5.0, &mut value);
    let point = [field(7, &a), time, field(7, &b), value].concat();
    let gauge = field(1, &point);
    let metric = [field(1, b"g"), field(5, &gauge)].concat();
    let scope_metrics = field(2, &metric);
    let resource_metrics = field(2, &scope_metrics);
    let body = field(1, &resource_metrics);
    let expected = Point {
        labels: labels(&[("__name__", "g"), ("a", "1"), ("b", "2")]),
        timestamp_ms: MS,
        value: 5.0,
    };
    assert_eq!(decode(&body), vec![expected]);
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn one_gauge() -> Vec<u8> {
    encode(&request(vec![gauge(
        "g",
        vec![
            number_point(vec![], NANOS, number_data_point::Value::AsDouble(1.0)),
            number_point(vec![], NANOS + 1, number_data_point::Value::AsDouble(2.0)),
        ],
    )]))
}

/// A gzip body decodes to the points of the same body uncompressed.
#[test]
fn gzip_bodies_decode_as_uncompressed_ones() {
    let body = one_gauge();
    let none = LabelOptions::default();
    assert_eq!(
        decode_with(&gzip(&body), Some(" GZIP "), &none, &LIMITS),
        decode_with(&body, None, &none, &LIMITS)
    );
    assert!(matches!(
        decode_with(&body, Some("gzip"), &none, &LIMITS),
        Err(OtlpError::Gzip(_))
    ));
}

/// Bodies that are not OTLP protobuf, and bodies over a limit, are
/// rejected.
#[test]
fn rejects_malformed_and_oversized_bodies() {
    let none = LabelOptions::default();
    assert!(matches!(
        decode_with(&[0xff; 8], None, &none, &LIMITS),
        Err(OtlpError::Protobuf(_))
    ));
    // A metric name that is not UTF-8.
    let mut body = one_gauge();
    let at = body
        .windows(3)
        .position(|w| w == [0x0a, 0x01, b'g'])
        .unwrap();
    body[at + 2] = 0xff;
    assert!(matches!(
        decode_with(&body, None, &none, &LIMITS),
        Err(OtlpError::Protobuf(_))
    ));
    let body = one_gauge();
    let small = Limits {
        max_decompressed_bytes: 4,
        ..LIMITS
    };
    assert_eq!(
        decode_with(&body, None, &none, &small),
        Err(OtlpError::DecompressedTooLarge(4))
    );
    assert_eq!(
        decode_with(&gzip(&body), Some("gzip"), &none, &small),
        Err(OtlpError::DecompressedTooLarge(4))
    );
    let one = Limits {
        max_points: 1,
        ..LIMITS
    };
    assert_eq!(
        decode_with(&body, None, &none, &one),
        Err(OtlpError::TooManyPoints(1))
    );
}

/// The replies decode as the OTLP messages: an empty
/// `ExportMetricsServiceResponse` on full success, its `partial_success`
/// with the rejected points otherwise, and a `Status` with the message for
/// errors, as protobuf or JSON.
#[test]
fn replies_are_otlp_messages() {
    assert!(export_response_body(0, None).is_empty());
    assert_eq!(
        ExportMetricsServiceResponse::decode(&export_response_body(0, None)[..]).unwrap(),
        ExportMetricsServiceResponse::default()
    );
    let partial = ExportMetricsServiceResponse::decode(&export_response_body(3, Some("why"))[..])
        .unwrap()
        .partial_success
        .unwrap();
    assert_eq!(
        (partial.rejected_data_points, partial.error_message.as_str()),
        (3, "why")
    );
    let error = Status::decode(&status_body(Encoding::Protobuf, "bad body")[..]).unwrap();
    assert_eq!(error.message, "bad body");
    let error: serde_json::Value =
        serde_json::from_slice(&status_body(Encoding::Json, "bad")).unwrap();
    assert_eq!(error, serde_json::json!({ "message": "bad" }));
    for (content_type, encoding) in [
        (None, Encoding::Protobuf),
        (Some("application/x-protobuf"), Encoding::Protobuf),
        (Some("Application/JSON; charset=utf-8"), Encoding::Json),
        (Some("text/plain"), Encoding::Protobuf),
    ] {
        assert_eq!(Encoding::of_content_type(content_type), encoding);
    }
}

/// Consecutive points with the same attributes take the previous point's
/// series; a point with other attributes, or of another metric, does not.
#[test]
fn consecutive_points_share_their_series() {
    let point = |value: &str| {
        number_point(
            vec![text("a", value)],
            NANOS,
            number_data_point::Value::AsDouble(1.0),
        )
    };
    let body = encode(&request_with(
        vec![],
        vec![
            gauge("g", vec![point("1"), point("1"), point("2"), point("1")]),
            gauge("h", vec![point("1")]),
            metric(
                "d",
                Data::Histogram(Histogram {
                    data_points: vec![
                        HistogramDataPoint {
                            attributes: vec![text("a", "1")],
                            count: 1,
                            ..Default::default()
                        },
                        HistogramDataPoint {
                            attributes: vec![text("a", "1")],
                            count: 2,
                            sum: Some(3.0),
                            ..Default::default()
                        },
                    ],
                    aggregation_temporality: 2,
                }),
            ),
        ],
    ));
    let names: Vec<String> = decode(&body)
        .into_iter()
        .map(|point| {
            point
                .labels
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect();
    assert_eq!(
        names,
        [
            "__name__=g,a=1",
            "__name__=g,a=1",
            "__name__=g,a=2",
            "__name__=g,a=1",
            "__name__=h,a=1",
            "__name__=d_count,a=1",
            "__name__=d_sum,a=1",
            "__name__=d_count,a=1",
        ]
    );
}
