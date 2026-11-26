//! Times building a range-query response body: `STEPS` cached step
//! answers of `SERIES` series each (labels shared per series, as the
//! result cache holds them), grouped by series and serialized to JSON.
//!
//! `matrix_bench [SERIES] [STEPS] [RUNS]` (default 2800 241 5)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use data_plane::prometheus::range_response_body;
use data_plane::query::ResultSample;

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap())
        .collect();
    let series = *args.first().unwrap_or(&2800);
    let steps = *args.get(1).unwrap_or(&241);
    let runs = *args.get(2).unwrap_or(&5);
    let labels: Vec<Arc<BTreeMap<String, String>>> = (0..series)
        .map(|i| {
            Arc::new(
                [
                    ("__name__", "data".to_string()),
                    ("instance", format!("10.0.{}.{}:9100", i / 250, i % 250)),
                    ("job", format!("job{}", i % 16)),
                    ("region", format!("region-{}", i % 4)),
                    ("service", format!("service-{}", i % 8)),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            )
        })
        .collect();
    let answers: Vec<(i64, Arc<Vec<ResultSample>>)> = (0..steps)
        .map(|step| {
            let time_ms = 1_700_000_000_000 + step as i64 * 15_000;
            let samples = labels
                .iter()
                .enumerate()
                .map(|(i, labels)| ResultSample {
                    labels: labels.clone(),
                    value: ((i * 7919 + step * 104_729) % 100_003) as f64 / 7.0,
                })
                .collect();
            (time_ms, Arc::new(samples))
        })
        .collect();
    let mut times = Vec::new();
    let mut bytes = 0;
    for _ in 0..runs {
        let started = Instant::now();
        let body = range_response_body(&answers).unwrap();
        times.push(started.elapsed());
        bytes = body.len();
    }
    times.sort();
    println!(
        "{series} series x {steps} steps: median {:?}, min {:?}, {} bytes",
        times[times.len() / 2],
        times[0],
        bytes
    );
}
