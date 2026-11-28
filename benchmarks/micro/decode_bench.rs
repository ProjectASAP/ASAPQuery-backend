//! Ingest decode cost, in process: decodes the same request repeatedly
//! into interned samples, as the write handlers do, and prints
//! microseconds per sample for Remote Write and OTLP, each in two request
//! shapes: one series per sample (10,000 series with one sample each, as
//! a paced feeder sends) and Prometheus' shape (100 series with 100
//! samples each). The OTLP request holds the same series and samples as
//! the Remote Write one: one gauge, each label but `__name__` a point
//! attribute (uncompressed protobuf, as an OpenTelemetry SDK sends it by
//! default).
//!
//! `cargo run --release -p data_plane --example decode_bench`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::time::Instant;

use data_plane::remote_write::{decode_otlp_samples, decode_samples};
use otlp::proto::collector::metrics::v1::ExportMetricsServiceRequest;
use otlp::proto::common::v1::{any_value, AnyValue, KeyValue};
use otlp::proto::metrics::v1::{
    metric::Data, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prometheus_remote_write::{encode, Label, Limits, Sample, TimeSeries, WriteRequest};

fn labels(i: usize) -> [(&'static str, String); 5] {
    [
        ("__name__", "data".to_string()),
        ("instance", format!("i{i}")),
        ("job", format!("job{}", i % 16)),
        ("region", format!("r{}", i % 4)),
        ("service", format!("s{}", i % 8)),
    ]
}

fn remote_write_request(series: usize, samples_per_series: usize) -> Vec<u8> {
    let timeseries = (0..series)
        .map(|i| TimeSeries {
            labels: labels(i)
                .into_iter()
                .map(|(name, value)| Label {
                    name: name.into(),
                    value,
                })
                .collect(),
            samples: (0..samples_per_series)
                .map(|t| Sample {
                    timestamp: 1_000 * t as i64,
                    value: t as f64,
                })
                .collect(),
        })
        .collect();
    encode(&WriteRequest { timeseries })
}

fn otlp_request(series: usize, samples_per_series: usize) -> Vec<u8> {
    let data_points = (0..series)
        .flat_map(|i| {
            let attributes: Vec<KeyValue> = labels(i)[1..]
                .iter()
                .map(|(name, value)| KeyValue {
                    key: name.to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value.clone())),
                    }),
                })
                .collect();
            (0..samples_per_series).map(move |t| NumberDataPoint {
                attributes: attributes.clone(),
                time_unix_nano: 1_000_000_000 * t as u64,
                value: Some(number_data_point::Value::AsDouble(t as f64)),
                ..Default::default()
            })
        })
        .collect();
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "data".into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    prost::Message::encode_to_vec(&request)
}

/// Microseconds per sample of `decode` over `body`, after one decode that
/// interns the series, as a steady stream would.
fn per_sample<T>(body: &[u8], samples: usize, decode: impl Fn(&[u8]) -> Vec<T>) -> f64 {
    assert_eq!(decode(body).len(), samples);
    let rounds = 200;
    let started = Instant::now();
    for _ in 0..rounds {
        std::hint::black_box(decode(body));
    }
    started.elapsed().as_secs_f64() * 1e6 / (rounds * samples) as f64
}

fn main() {
    let limits = Limits::default();
    let options = otlp::LabelOptions::default();
    for (name, series, per_series) in [
        ("one series per sample", 10_000, 1),
        ("100 samples per series", 100, 100),
    ] {
        let samples = series * per_series;
        let remote_write = per_sample(&remote_write_request(series, per_series), samples, |b| {
            decode_samples(b, &limits).unwrap()
        });
        let otlp = per_sample(&otlp_request(series, per_series), samples, |b| {
            decode_otlp_samples(b, None, &options, &limits)
                .unwrap()
                .samples
        });
        println!(
            "{name}: Remote Write {remote_write:.3} µs/sample, OTLP {otlp:.3} µs/sample ({:.2}x)",
            otlp / remote_write
        );
    }
}
