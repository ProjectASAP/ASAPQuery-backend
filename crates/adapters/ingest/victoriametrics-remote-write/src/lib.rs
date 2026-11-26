//! VictoriaMetrics Remote Write decoding: a Prometheus Remote Write v1
//! `WriteRequest`, zstd-compressed instead of snappy-compressed.
//!
//! [`Decoder`] decompresses a body and hands the protobuf to
//! `prometheus_remote_write`'s decoder, so the samples are walked, validated
//! and built into the caller's types exactly as a Prometheus Remote Write
//! body's are.

use std::io::Read;

pub use prometheus_remote_write::{Limits, RemoteWriteError, SampleBuilder};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum VictoriaMetricsRemoteWriteError {
    #[error("zstd decompression failed: {0}")]
    Zstd(String),
    /// Over a size limit, or not a valid Remote Write request once
    /// decompressed.
    #[error(transparent)]
    RemoteWrite(#[from] RemoteWriteError),
}

/// A decoder's state, kept between requests: its decompression buffer and
/// the Remote Write decoder (with its series cache) it hands the protobuf to.
pub struct Decoder<B: SampleBuilder> {
    /// A request's decompressed bytes are written over the last one's.
    decompressed: Vec<u8>,
    remote_write: prometheus_remote_write::Decoder<B>,
}

/// The largest decompression buffer a decoder keeps between requests, as
/// `prometheus_remote_write`'s decoder.
const KEPT_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

impl<B: SampleBuilder> Decoder<B> {
    pub fn new(builder: B) -> Self {
        Self {
            decompressed: Vec::new(),
            remote_write: prometheus_remote_write::Decoder::new(builder),
        }
    }

    /// The samples of a zstd-compressed Remote Write v1 body, in request
    /// order ([`prometheus_remote_write::Decoder::decode_uncompressed`]).
    /// Errors: the body is over `max_compressed_bytes`, decompresses to
    /// more than `max_decompressed_bytes` or is not zstd, or the protobuf is
    /// rejected as a Prometheus Remote Write body's would be.
    pub fn decode(
        &mut self,
        body: &[u8],
        limits: &Limits,
    ) -> Result<Vec<B::Sample>, VictoriaMetricsRemoteWriteError> {
        let decoded = decompress_into(body, limits, &mut self.decompressed).and_then(|()| {
            Ok(self
                .remote_write
                .decode_uncompressed(&self.decompressed, limits)?)
        });
        if self.decompressed.capacity() > KEPT_DECOMPRESSED_BYTES {
            self.decompressed = Vec::new();
        }
        decoded
    }
}

/// Writes the zstd-decompressed bytes of `body` into `buffer` (replacing
/// its contents), within the size limits. Reads at most one byte past
/// `max_decompressed_bytes`, so a body that decompresses to more is
/// rejected without being decompressed whole.
fn decompress_into(
    body: &[u8],
    limits: &Limits,
    buffer: &mut Vec<u8>,
) -> Result<(), VictoriaMetricsRemoteWriteError> {
    if body.len() > limits.max_compressed_bytes {
        return Err(RemoteWriteError::CompressedTooLarge(limits.max_compressed_bytes).into());
    }
    buffer.clear();
    let zstd_error = |e: std::io::Error| VictoriaMetricsRemoteWriteError::Zstd(e.to_string());
    let decompressor = zstd::stream::read::Decoder::with_buffer(body).map_err(zstd_error)?;
    let max = limits.max_decompressed_bytes;
    decompressor
        .take(max as u64 + 1)
        .read_to_end(buffer)
        .map_err(zstd_error)?;
    if buffer.len() > max {
        return Err(RemoteWriteError::DecompressedTooLarge(max).into());
    }
    Ok(())
}

/// Zstd-compressed encoding of `request`, as a VictoriaMetrics sender
/// sends it.
pub fn encode(request: &prometheus_remote_write::WriteRequest) -> Vec<u8> {
    use prost::Message;
    zstd::encode_all(request.encode_to_vec().as_slice(), 0).expect("zstd compresses any input")
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_remote_write::{Label, Sample, TimeSeries, WriteRequest};
    use std::cell::RefCell;
    use std::rc::Rc;

    type Labels = Vec<(String, String)>;

    /// A builder whose series handle is an index into `series`, the labels
    /// of each series it interned, in order; shared, so a test reads it
    /// while the decoder holds the builder.
    #[derive(Clone, Default)]
    struct Labeled {
        series: Rc<RefCell<Vec<Labels>>>,
    }

    impl SampleBuilder for Labeled {
        type Series = usize;
        type Sample = (usize, i64, f64);

        fn intern(&mut self, labels: &[(&str, &str)]) -> usize {
            let mut series = self.series.borrow_mut();
            series.push(pairs(labels));
            series.len() - 1
        }

        fn sample(series: usize, timestamp_ms: i64, value: f64) -> Self::Sample {
            (series, timestamp_ms, value)
        }
    }

    impl Labeled {
        /// `samples` with each series' labels and each value's bits.
        fn labeled(&self, samples: Vec<(usize, i64, f64)>) -> Vec<(Labels, i64, u64)> {
            let series = self.series.borrow();
            samples
                .into_iter()
                .map(|(s, timestamp_ms, value)| (series[s].clone(), timestamp_ms, value.to_bits()))
                .collect()
        }
    }

    fn pairs(labels: &[(&str, &str)]) -> Labels {
        labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn series(labels: &[(&str, &str)], samples: &[(i64, f64)]) -> TimeSeries {
        TimeSeries {
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            samples: samples
                .iter()
                .map(|&(timestamp, value)| Sample { value, timestamp })
                .collect(),
        }
    }

    /// Ported from ASAPQuery's `test_zstd_decode_single_sample`.
    #[test]
    fn decodes_a_single_zstd_sample() {
        let request = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "vm_metric"), ("region", "us-east-1")],
                &[(1_700_000_000_000, 99.9)],
            )],
        };
        let builder = Labeled::default();
        let mut decoder = Decoder::new(builder.clone());
        let samples = decoder
            .decode(&encode(&request), &Limits::default())
            .unwrap();
        assert_eq!(
            builder.labeled(samples),
            vec![(
                pairs(&[("__name__", "vm_metric"), ("region", "us-east-1")]),
                1_700_000_000_000,
                99.9f64.to_bits()
            )]
        );
    }

    /// Ported from ASAPQuery's `test_decode_invalid_zstd`.
    #[test]
    fn rejects_a_body_that_is_not_zstd() {
        let mut decoder = Decoder::new(Labeled::default());
        assert!(matches!(
            decoder.decode(b"not-zstd-data", &Limits::default()),
            Err(VictoriaMetricsRemoteWriteError::Zstd(_))
        ));
    }

    /// A zstd body decodes to the samples of the same request sent as
    /// Prometheus Remote Write (snappy), stale markers included, and the
    /// decoder's series cache carries over between requests.
    #[test]
    fn decodes_what_prometheus_remote_write_decodes() {
        let request = WriteRequest {
            timeseries: vec![
                series(&[("__name__", "m"), ("job", "a")], &[(1, 1.0), (2, 2.5)]),
                series(&[("job", "b"), ("__name__", "m")], &[(3, -1.0)]),
                series(&[("__name__", "m"), ("job", "a")], &[(4, f64::NAN)]),
            ],
        };
        let limits = Limits::default();
        let snappy_builder = Labeled::default();
        let snappy = prometheus_remote_write::Decoder::new(snappy_builder.clone())
            .decode(&prometheus_remote_write::encode(&request), &limits)
            .unwrap();
        let snappy = snappy_builder.labeled(snappy);
        let builder = Labeled::default();
        let mut decoder = Decoder::new(builder.clone());
        for _ in 0..2 {
            let samples = decoder.decode(&encode(&request), &limits).unwrap();
            assert_eq!(builder.labeled(samples), snappy);
        }
        assert_eq!(builder.series.borrow().len(), 2);
    }

    /// Series are validated as Prometheus Remote Write's are.
    #[test]
    fn rejects_invalid_series() {
        let limits = Limits::default();
        let mut decoder = Decoder::new(Labeled::default());
        let unnamed = WriteRequest {
            timeseries: vec![series(&[("job", "api")], &[(1, 1.0)])],
        };
        assert_eq!(
            decoder.decode(&encode(&unnamed), &limits).err(),
            Some(RemoteWriteError::MissingMetricName.into())
        );
        let garbage = zstd::encode_all(&[0xffu8; 8][..], 0).unwrap();
        assert!(matches!(
            decoder.decode(&garbage, &limits),
            Err(VictoriaMetricsRemoteWriteError::RemoteWrite(
                RemoteWriteError::Protobuf(_)
            ))
        ));
    }

    /// Bodies over a size limit, compressed or decompressed, are rejected,
    /// and so are requests with too many samples.
    #[test]
    fn enforces_limits() {
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "x")], &[(1, 1.0), (2, 2.0)])],
        };
        let body = encode(&request);
        let mut decoder = Decoder::new(Labeled::default());
        for (limits, error) in [
            (
                Limits {
                    max_compressed_bytes: 1,
                    ..Limits::default()
                },
                RemoteWriteError::CompressedTooLarge(1),
            ),
            (
                Limits {
                    max_decompressed_bytes: 4,
                    ..Limits::default()
                },
                RemoteWriteError::DecompressedTooLarge(4),
            ),
            (
                Limits {
                    max_samples: 1,
                    ..Limits::default()
                },
                RemoteWriteError::TooManySamples(1),
            ),
        ] {
            assert_eq!(decoder.decode(&body, &limits).err(), Some(error.into()));
        }
        // A zstd bomb: 64 MiB of zeros, rejected after decompressing one
        // byte past the limit, and no large buffer kept.
        let bomb = zstd::encode_all(&vec![0u8; 64 << 20][..], 3).unwrap();
        let limits = Limits {
            max_decompressed_bytes: 1 << 20,
            ..Limits::default()
        };
        assert_eq!(
            decoder.decode(&bomb, &limits).err(),
            Some(RemoteWriteError::DecompressedTooLarge(1 << 20).into())
        );
        assert!(decoder.decompressed.capacity() <= KEPT_DECOMPRESSED_BYTES);
    }
}
