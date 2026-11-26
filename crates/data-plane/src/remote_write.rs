//! Remote Write ingest: the `prometheus_remote_write` adapter's decoder,
//! with each series interned into the store's series table, so a request
//! decodes straight into the engine's [`RawSample`]s.

use std::cell::RefCell;

use prometheus_remote_write::{Decoder, Limits, RemoteWriteError};
use storage::series_ids::{intern_pairs, Series};
use storage::store::RawSample;

/// The samples of a snappy-compressed Remote Write v1 body, each with its
/// interned series, in request order ([`Decoder::decode`]). Each thread
/// keeps its own decoder, so a request reuses the last one's buffer and
/// series cache.
pub fn decode_samples(body: &[u8], limits: &Limits) -> Result<Vec<RawSample>, RemoteWriteError> {
    thread_local! {
        static DECODER: RefCell<Decoder<&'static Series>> = RefCell::default();
    }
    DECODER.with(|decoder| {
        decoder.borrow_mut().decode(
            body,
            limits,
            |pairs| intern_pairs(pairs),
            |labels, timestamp_ms, value| RawSample {
                labels,
                timestamp_ms,
                value,
            },
        )
    })
}
