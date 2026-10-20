//! Prometheus Remote Write v1 decoding and validation.

use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

/// Request size limits; a request over any limit is rejected whole.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_samples: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_compressed_bytes: 32 * 1024 * 1024,
            max_decompressed_bytes: 128 * 1024 * 1024,
            max_samples: 1_000_000,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RemoteWriteError {
    #[error("compressed request exceeds {0} bytes")]
    CompressedTooLarge(usize),
    #[error("decompressed request exceeds {0} bytes")]
    DecompressedTooLarge(usize),
    #[error("snappy decompression failed: {0}")]
    Snappy(String),
    #[error("protobuf decoding failed: {0}")]
    Protobuf(String),
    #[error("request has more than {0} samples")]
    TooManySamples(usize),
    #[error("series without a __name__ label")]
    MissingMetricName,
    #[error("series has duplicate label {0:?}")]
    DuplicateLabel(String),
}

/// Decodes a snappy-compressed Remote Write v1 body. Every series must name
/// its metric and carry each label once.
pub fn decode(body: &[u8], limits: &Limits) -> Result<WriteRequest, RemoteWriteError> {
    if body.len() > limits.max_compressed_bytes {
        return Err(RemoteWriteError::CompressedTooLarge(
            limits.max_compressed_bytes,
        ));
    }
    let len =
        snap::raw::decompress_len(body).map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    if len > limits.max_decompressed_bytes {
        return Err(RemoteWriteError::DecompressedTooLarge(
            limits.max_decompressed_bytes,
        ));
    }
    let decoded = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| RemoteWriteError::Snappy(e.to_string()))?;
    let request = WriteRequest::decode(decoded.as_slice())
        .map_err(|e| RemoteWriteError::Protobuf(e.to_string()))?;
    let samples: usize = request.timeseries.iter().map(|s| s.samples.len()).sum();
    if samples > limits.max_samples {
        return Err(RemoteWriteError::TooManySamples(limits.max_samples));
    }
    for series in &request.timeseries {
        let mut names: Vec<&str> = series.labels.iter().map(|l| l.name.as_str()).collect();
        names.sort_unstable();
        if let Some(pair) = names.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(RemoteWriteError::DuplicateLabel(pair[0].to_string()));
        }
        if !names.contains(&"__name__") {
            return Err(RemoteWriteError::MissingMetricName);
        }
    }
    Ok(request)
}

/// Snappy-compressed encoding of `request`, as a Remote Write sender sends it.
pub fn encode(request: &WriteRequest) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compresses any input")
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// A well-formed request decodes to the series and samples it was built from.
    #[test]
    fn decodes_what_a_sender_encodes() {
        let request = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "http_requests_total"), ("job", "api")],
                &[(1_000, 1.0), (16_000, 3.0)],
            )],
        };
        assert_eq!(decode(&encode(&request), &Limits::default()), Ok(request));
    }

    /// Bodies that are not snappy or not Remote Write protobuf are rejected.
    #[test]
    fn rejects_malformed_bodies() {
        let limits = Limits::default();
        assert!(matches!(
            decode(b"not snappy", &limits),
            Err(RemoteWriteError::Snappy(_))
        ));
        let garbage = snap::raw::Encoder::new().compress_vec(&[0xff; 8]).unwrap();
        assert!(matches!(
            decode(&garbage, &limits),
            Err(RemoteWriteError::Protobuf(_))
        ));
    }

    /// Series must name their metric and may not repeat a label.
    #[test]
    fn rejects_invalid_series() {
        let limits = Limits::default();
        let unnamed = WriteRequest {
            timeseries: vec![series(&[("job", "api")], &[(1, 1.0)])],
        };
        assert_eq!(
            decode(&encode(&unnamed), &limits),
            Err(RemoteWriteError::MissingMetricName)
        );
        let duplicate = WriteRequest {
            timeseries: vec![series(
                &[("__name__", "x"), ("job", "a"), ("job", "b")],
                &[(1, 1.0)],
            )],
        };
        assert_eq!(
            decode(&encode(&duplicate), &limits),
            Err(RemoteWriteError::DuplicateLabel("job".into()))
        );
    }

    /// Requests over a size limit are rejected before they are processed.
    #[test]
    fn enforces_limits() {
        let request = WriteRequest {
            timeseries: vec![series(&[("__name__", "x")], &[(1, 1.0), (2, 2.0)])],
        };
        let limits = Limits {
            max_samples: 1,
            ..Limits::default()
        };
        assert_eq!(
            decode(&encode(&request), &limits),
            Err(RemoteWriteError::TooManySamples(1))
        );
        let limits = Limits {
            max_compressed_bytes: 1,
            ..Limits::default()
        };
        assert_eq!(
            decode(&encode(&request), &limits),
            Err(RemoteWriteError::CompressedTooLarge(1))
        );
    }
}
