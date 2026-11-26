//! OTLP (OpenTelemetry protocol) metrics decoding: an
//! `ExportMetricsServiceRequest` body, as `POST /v1/metrics` receives it,
//! into one [`Point`] per sample, the way ASAPQuery's OTLP receiver reads it.
//!
//! Each data point becomes samples of Prometheus-style series: the metric's
//! name as `__name__`, plus the scope's, the resource's and the point's
//! attributes as labels (a later one replaces an earlier one of the same
//! name). Gauges and sums give one sample; histograms, exponential
//! histograms and summaries give `<name>_sum` (when set) and
//! `<name>_count`. Data points that carry a sketch payload attribute
//! (`kll.`, `cms.` or `countsketch.sketch_payload`) are skipped, as
//! ASAPQuery skips them.

use std::collections::BTreeMap;
use std::io::Read;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point};
use prost::Message;

/// Request size limits; a request over any limit is rejected whole.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_decompressed_bytes: usize,
    pub max_points: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_decompressed_bytes: 128 * 1024 * 1024,
            max_points: 1_000_000,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum OtlpError {
    #[error("gzip decode failed: {0}")]
    Gzip(String),
    #[error("decompressed request exceeds {0} bytes")]
    DecompressedTooLarge(usize),
    #[error("protobuf decoding failed: {0}")]
    Protobuf(String),
    #[error("request has more than {0} points")]
    TooManyPoints(usize),
}

/// One sample of a series.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    /// `__name__` and the attributes, sorted by name, each name once.
    pub labels: Vec<(String, String)>,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// The points of an OTLP metrics request body, in request order.
/// `content_encoding` is the request's `Content-Encoding`: `gzip` bodies
/// are decompressed; any other encoding is read as is, as ASAPQuery does.
/// Errors: the body is not gzip (when it says so) or not an
/// `ExportMetricsServiceRequest`, or is over a limit.
pub fn decode(
    body: &[u8],
    content_encoding: Option<&str>,
    limits: &Limits,
) -> Result<Vec<Point>, OtlpError> {
    let gzip = content_encoding.is_some_and(|e| e.trim().eq_ignore_ascii_case("gzip"));
    let decompressed;
    let request = if gzip {
        decompressed = gunzip(body, limits.max_decompressed_bytes)?;
        &decompressed[..]
    } else if body.len() > limits.max_decompressed_bytes {
        return Err(OtlpError::DecompressedTooLarge(
            limits.max_decompressed_bytes,
        ));
    } else {
        body
    };
    let request = ExportMetricsServiceRequest::decode(request)
        .map_err(|e| OtlpError::Protobuf(e.to_string()))?;
    points(&request, limits.max_points)
}

/// `body` gunzipped, reading at most one byte past `max_bytes`.
fn gunzip(body: &[u8], max_bytes: usize) -> Result<Vec<u8>, OtlpError> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(body)
        .take(max_bytes as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| OtlpError::Gzip(e.to_string()))?;
    if out.len() > max_bytes {
        return Err(OtlpError::DecompressedTooLarge(max_bytes));
    }
    Ok(out)
}

/// The points of `request`, at most `max_points`.
fn points(
    request: &ExportMetricsServiceRequest,
    max_points: usize,
) -> Result<Vec<Point>, OtlpError> {
    let mut points = Vec::new();
    let mut push = |labels: &BTreeMap<String, String>, name: String, nanos: u64, value: f64| {
        if points.len() >= max_points {
            return Err(OtlpError::TooManyPoints(max_points));
        }
        let mut labels = labels.clone();
        labels.insert("__name__".to_string(), name);
        points.push(Point {
            labels: labels.into_iter().collect(),
            timestamp_ms: (nanos / 1_000_000) as i64,
            value,
        });
        Ok(())
    };
    for resource_metrics in &request.resource_metrics {
        let resource = resource_metrics.resource.as_ref();
        for scope_metrics in &resource_metrics.scope_metrics {
            // The scope's attributes, then the resource's over them.
            let mut scope_labels = BTreeMap::new();
            if let Some(scope) = &scope_metrics.scope {
                insert_attributes(&mut scope_labels, &scope.attributes);
            }
            if let Some(resource) = resource {
                insert_attributes(&mut scope_labels, &resource.attributes);
            }
            for metric in &scope_metrics.metrics {
                if metric.name.is_empty() {
                    continue;
                }
                let name = &metric.name;
                // The point's attributes over the scope's and resource's;
                // `None` for a sketch payload point.
                let labels = |attributes: &[KeyValue]| {
                    if has_sketch_payload(attributes) {
                        return None;
                    }
                    let mut labels = scope_labels.clone();
                    insert_attributes(&mut labels, attributes);
                    Some(labels)
                };
                match &metric.data {
                    Some(Data::Gauge(gauge)) => {
                        for point in &gauge.data_points {
                            if let Some(labels) = labels(&point.attributes) {
                                let value = number(&point.value);
                                push(&labels, name.clone(), point.time_unix_nano, value)?;
                            }
                        }
                    }
                    Some(Data::Sum(sum)) => {
                        for point in &sum.data_points {
                            if let Some(labels) = labels(&point.attributes) {
                                let value = number(&point.value);
                                push(&labels, name.clone(), point.time_unix_nano, value)?;
                            }
                        }
                    }
                    Some(Data::Histogram(histogram)) => {
                        for point in &histogram.data_points {
                            if let Some(labels) = labels(&point.attributes) {
                                let nanos = point.time_unix_nano;
                                if let Some(sum) = point.sum {
                                    push(&labels, format!("{name}_sum"), nanos, sum)?;
                                }
                                let count = point.count as f64;
                                push(&labels, format!("{name}_count"), nanos, count)?;
                            }
                        }
                    }
                    Some(Data::ExponentialHistogram(histogram)) => {
                        for point in &histogram.data_points {
                            if let Some(labels) = labels(&point.attributes) {
                                let nanos = point.time_unix_nano;
                                if let Some(sum) = point.sum {
                                    push(&labels, format!("{name}_sum"), nanos, sum)?;
                                }
                                let count = point.count as f64;
                                push(&labels, format!("{name}_count"), nanos, count)?;
                            }
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        for point in &summary.data_points {
                            if let Some(labels) = labels(&point.attributes) {
                                let nanos = point.time_unix_nano;
                                push(&labels, format!("{name}_sum"), nanos, point.sum)?;
                                let count = point.count as f64;
                                push(&labels, format!("{name}_count"), nanos, count)?;
                            }
                        }
                    }
                    None => {}
                }
            }
        }
    }
    Ok(points)
}

/// Whether a data point carries a sketch in place of a value.
fn has_sketch_payload(attributes: &[KeyValue]) -> bool {
    attributes.iter().any(|kv| {
        matches!(
            kv.key.as_str(),
            "kll.sketch_payload" | "cms.sketch_payload" | "countsketch.sketch_payload"
        ) && matches!(
            kv.value.as_ref().and_then(|v| v.value.as_ref()),
            Some(any_value::Value::BytesValue(_))
        )
    })
}

/// Inserts each attribute with a name into `labels`, its value as text.
fn insert_attributes(labels: &mut BTreeMap<String, String>, attributes: &[KeyValue]) {
    for kv in attributes {
        if kv.key.is_empty() {
            continue;
        }
        let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(any_value::Value::StringValue(s)) => s.clone(),
            Some(any_value::Value::IntValue(i)) => i.to_string(),
            Some(any_value::Value::DoubleValue(d)) => d.to_string(),
            Some(any_value::Value::BoolValue(b)) => b.to_string(),
            Some(any_value::Value::BytesValue(bytes)) => format!("{bytes:?}"),
            _ => String::new(),
        };
        labels.insert(kv.key.clone(), value);
    }
}

/// A number data point's value; 0 when it has none.
fn number(value: &Option<number_data_point::Value>) -> f64 {
    match value {
        Some(number_data_point::Value::AsDouble(x)) => *x,
        Some(number_data_point::Value::AsInt(x)) => *x as f64,
        None => 0.0,
    }
}

/// The protobuf encoding of `request`, as an OTLP/HTTP sender sends it
/// without compression.
pub fn encode(request: &ExportMetricsServiceRequest) -> Vec<u8> {
    request.encode_to_vec()
}

/// The OTLP protobuf messages, for senders and tests.
pub use opentelemetry_proto::tonic as proto;

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope};
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

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

    fn metric(name: &str, data: Data) -> Metric {
        Metric {
            name: name.to_string(),
            data: Some(data),
            ..Default::default()
        }
    }

    /// A request with one resource and one scope holding `metrics`.
    fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![text("service.name", "api-gateway")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "exporter".to_string(),
                        attributes: vec![text("scope", "s"), text("service.name", "scope's")],
                        ..Default::default()
                    }),
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut labels: Vec<(String, String)> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        labels.sort();
        labels
    }

    const NANOS: u64 = 1_700_000_000_123_456_789;
    const MS: i64 = 1_700_000_000_123;

    /// Gauges and sums give one point each, labeled by the metric name and
    /// the scope's, resource's and point's attributes (the resource's over
    /// the scope's, the point's over both), at the point's time in ms. A
    /// counter, a gauge and an up-down counter as the OTLP test exporter
    /// sends them.
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
                    ..Default::default()
                }),
            ),
            metric(
                "http_active_connections",
                Data::Gauge(Gauge {
                    data_points: vec![number_point(
                        vec![
                            text("scope", "point's"),
                            attribute("shard", any_value::Value::IntValue(3)),
                        ],
                        NANOS,
                        number_data_point::Value::AsDouble(-1.5),
                    )],
                }),
            ),
        ]));
        let points = decode(&body, None, &Limits::default()).unwrap();
        assert_eq!(
            points,
            vec![
                Point {
                    labels: labels(&[
                        ("__name__", "http_requests_total"),
                        ("method", "GET"),
                        ("scope", "s"),
                        ("service.name", "api-gateway"),
                        ("status", "200"),
                    ]),
                    timestamp_ms: MS,
                    value: 42.0,
                },
                Point {
                    labels: labels(&[
                        ("__name__", "http_active_connections"),
                        ("scope", "point's"),
                        ("service.name", "api-gateway"),
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
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: NANOS,
                        count: 2,
                        sum: 5.0,
                        ..Default::default()
                    }],
                }),
            ),
        ]));
        let points: Vec<(String, f64)> = decode(&body, None, &Limits::default())
            .unwrap()
            .into_iter()
            .map(|point| {
                let name = point.labels.iter().find(|(n, _)| n == "__name__").unwrap();
                (name.1.clone(), point.value)
            })
            .collect();
        let expected: Vec<(String, f64)> = [
            ("http_request_duration_seconds_sum", 0.45),
            ("http_request_duration_seconds_count", 3.0),
            ("http_request_duration_seconds_count", 1.0),
            ("size_sum", 70.0),
            ("size_count", 7.0),
            ("latency_sum", 5.0),
            ("latency_count", 2.0),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
        assert_eq!(points, expected);
    }

    /// Sketch payload points and unnamed metrics are skipped; a point with
    /// no value reads 0, as in ASAPQuery.
    #[test]
    fn skips_sketch_payloads_and_unnamed_metrics() {
        let sketch = attribute(
            "kll.sketch_payload",
            any_value::Value::BytesValue(vec![1, 2]),
        );
        let body = encode(&request(vec![
            metric(
                "m",
                Data::Gauge(Gauge {
                    data_points: vec![
                        number_point(vec![sketch], NANOS, number_data_point::Value::AsDouble(9.0)),
                        NumberDataPoint {
                            time_unix_nano: NANOS,
                            ..Default::default()
                        },
                    ],
                }),
            ),
            metric(
                "",
                Data::Gauge(Gauge {
                    data_points: vec![number_point(
                        vec![],
                        NANOS,
                        number_data_point::Value::AsDouble(1.0),
                    )],
                }),
            ),
        ]));
        let points = decode(&body, None, &Limits::default()).unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value, 0.0);
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn one_gauge() -> Vec<u8> {
        encode(&request(vec![metric(
            "g",
            Data::Gauge(Gauge {
                data_points: vec![
                    number_point(vec![], NANOS, number_data_point::Value::AsDouble(1.0)),
                    number_point(vec![], NANOS + 1, number_data_point::Value::AsDouble(2.0)),
                ],
            }),
        )]))
    }

    /// A gzip body decodes to the points of the same body uncompressed.
    #[test]
    fn gzip_bodies_decode_as_uncompressed_ones() {
        let body = one_gauge();
        let limits = Limits::default();
        assert_eq!(
            decode(&gzip(&body), Some(" GZIP "), &limits),
            decode(&body, None, &limits)
        );
        assert!(matches!(
            decode(&body, Some("gzip"), &limits),
            Err(OtlpError::Gzip(_))
        ));
    }

    /// Bodies that are not OTLP protobuf, and bodies over a limit, are
    /// rejected.
    #[test]
    fn rejects_malformed_and_oversized_bodies() {
        let limits = Limits::default();
        assert!(matches!(
            decode(&[0xff; 8], None, &limits),
            Err(OtlpError::Protobuf(_))
        ));
        let body = one_gauge();
        let small = Limits {
            max_decompressed_bytes: 4,
            ..Limits::default()
        };
        assert_eq!(
            decode(&body, None, &small),
            Err(OtlpError::DecompressedTooLarge(4))
        );
        assert_eq!(
            decode(&gzip(&body), Some("gzip"), &small),
            Err(OtlpError::DecompressedTooLarge(4))
        );
        let one = Limits {
            max_points: 1,
            ..Limits::default()
        };
        assert_eq!(decode(&body, None, &one), Err(OtlpError::TooManyPoints(1)));
    }
}
