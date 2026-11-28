//! Ingest decoding: the ingest adapters' decoders (Prometheus and
//! VictoriaMetrics Remote Write, OTLP), with each series interned into the
//! store's series table, so a request decodes into the engine's
//! [`RawSample`]s.

use std::cell::RefCell;

use prometheus_remote_write::{Decoder, Limits, RemoteWriteError, SampleBuilder};
use storage::series_ids::{intern_pairs, Series};
use storage::store::RawSample;
use victoriametrics_remote_write::VictoriaMetricsRemoteWriteError;

/// The samples of a snappy-compressed Remote Write v1 body, each with its
/// interned series, in request order ([`Decoder::decode`]). Each thread
/// keeps its own decoder, so a request reuses the last one's buffer and
/// series cache.
pub fn decode_samples(body: &[u8], limits: &Limits) -> Result<Vec<RawSample>, RemoteWriteError> {
    thread_local! {
        static DECODER: RefCell<Decoder<Interned>> = RefCell::new(Decoder::new(Interned));
    }
    DECODER.with(|decoder| decoder.borrow_mut().decode(body, limits))
}

/// The samples of a zstd-compressed VictoriaMetrics Remote Write body, as
/// [`decode_samples`] decodes a Prometheus one
/// ([`victoriametrics_remote_write::Decoder::decode`]).
pub fn decode_victoriametrics_samples(
    body: &[u8],
    limits: &Limits,
) -> Result<Vec<RawSample>, VictoriaMetricsRemoteWriteError> {
    thread_local! {
        static DECODER: RefCell<victoriametrics_remote_write::Decoder<Interned>> =
            RefCell::new(victoriametrics_remote_write::Decoder::new(Interned));
    }
    DECODER.with(|decoder| decoder.borrow_mut().decode(body, limits))
}

/// The samples of an OTLP metrics body ([`otlp::Decoder::decode`]), each
/// point's series interned, within Remote Write's decompressed size and
/// sample limits, with the points it rejected and dropped.
/// `content_encoding` is the request's `Content-Encoding`. Each thread
/// keeps its own decoder, as [`decode_samples`] does.
pub fn decode_otlp_samples(
    body: &[u8],
    content_encoding: Option<&str>,
    options: &otlp::LabelOptions,
    limits: &Limits,
) -> Result<otlp::Decoded<RawSample>, otlp::OtlpError> {
    thread_local! {
        static DECODER: RefCell<otlp::Decoder<Interned>> = RefCell::new(otlp::Decoder::new(Interned));
    }
    let limits = otlp::Limits {
        max_decompressed_bytes: limits.max_decompressed_bytes,
        max_points: limits.max_samples,
    };
    DECODER.with(|decoder| {
        decoder
            .borrow_mut()
            .decode(body, content_encoding, options, &limits)
    })
}

/// Interns each series into the store's series table and builds the
/// engine's samples.
struct Interned;

impl SampleBuilder for Interned {
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
