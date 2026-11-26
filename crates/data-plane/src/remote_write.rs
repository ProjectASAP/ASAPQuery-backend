//! Remote Write ingest: the `prometheus_remote_write` adapter's decoder,
//! with each series interned into the store's series table, so a request
//! decodes straight into the engine's [`RawSample`]s.

use std::cell::RefCell;

use prometheus_remote_write::{Decoder, Limits, RemoteWriteError, SampleSink};
use storage::series_ids::{intern_pairs, Series};
use storage::store::RawSample;

/// The samples of a snappy-compressed Remote Write v1 body, each with its
/// interned series, in request order ([`Decoder::decode`]). Each thread
/// keeps its own decoder, so a request reuses the last one's buffer and
/// series cache.
pub fn decode_samples(body: &[u8], limits: &Limits) -> Result<Vec<RawSample>, RemoteWriteError> {
    thread_local! {
        static DECODER: RefCell<Decoder<Interned>> = RefCell::default();
    }
    DECODER.with(|decoder| decoder.borrow_mut().decode(body, limits))
}

/// Interns each series into the store's series table.
#[derive(Default)]
struct Interned;

impl SampleSink for Interned {
    type Series = &'static Series;
    type Sample = RawSample;

    fn intern(&mut self, labels: &[(&str, &str)]) -> &'static Series {
        intern_pairs(labels)
    }

    fn sample(labels: &'static Series, timestamp_ms: i64, value: f64) -> RawSample {
        RawSample {
            labels,
            timestamp_ms,
            value,
        }
    }
}
