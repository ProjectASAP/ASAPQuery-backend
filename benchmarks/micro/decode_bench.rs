//! Remote Write decode cost, in process: decodes the same request
//! repeatedly into interned samples, as the write handler does, and prints
//! microseconds per sample for two request shapes: one series per sample
//! (10,000 series with one sample each, as a paced feeder sends) and
//! Prometheus' shape (100 series with 100 samples each).
//!
//! `cargo run --release -p data_plane --example decode_bench`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::time::Instant;

use data_plane::remote_write::decode_samples;
use prometheus_remote_write::{encode, Label, Limits, Sample, TimeSeries, WriteRequest};

fn request(series: usize, samples_per_series: usize) -> Vec<u8> {
    let timeseries = (0..series)
        .map(|i| TimeSeries {
            labels: [
                ("__name__", "data".to_string()),
                ("instance", format!("i{i}")),
                ("job", format!("job{}", i % 16)),
                ("region", format!("r{}", i % 4)),
                ("service", format!("s{}", i % 8)),
            ]
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

fn main() {
    let limits = Limits::default();
    for (name, series, per_series) in [
        ("one series per sample", 10_000, 1),
        ("100 samples per series", 100, 100),
    ] {
        let body = request(series, per_series);
        // Interns the series once, as a steady stream would.
        decode_samples(&body, &limits).unwrap();
        let rounds = 200;
        let started = Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(decode_samples(&body, &limits).unwrap());
        }
        let samples = (rounds * series * per_series) as f64;
        println!(
            "{name}: {:.3} µs/sample",
            started.elapsed().as_secs_f64() * 1e6 / samples
        );
    }
}
