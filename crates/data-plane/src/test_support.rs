//! Options and samples for the unit tests.

use precompute::ingestion::IngestionOptions;
use storage::store::RawSample;

use crate::engine::EngineOptions;

/// Options that refresh every window: no allowed lateness, and the stream
/// starting at 0.
pub(crate) fn options() -> EngineOptions {
    EngineOptions {
        ingestion: IngestionOptions {
            allowed_lateness_ms: 0,
            stream_start_ms: Some(0),
            ..IngestionOptions::default()
        },
        ..EngineOptions::default()
    }
}

/// One sample of `data{job="a"}` with `value` every second of `(0, end_ms]`.
pub(crate) fn samples(end_ms: i64, value: f64) -> Vec<RawSample> {
    let series = storage::series_ids::intern_pairs(&[("__name__", "data"), ("job", "a")]);
    (1..=end_ms / 1000)
        .map(|second| RawSample {
            labels: series,
            timestamp_ms: second * 1000,
            value,
        })
        .collect()
}
