//! Prometheus remote read (protobuf, sampled responses): raw samples older
//! than the local raw buffer holds.

use prost::Message;

use crate::precompute::RawSample;
use crate::remote_write::TimeSeries;

#[derive(Clone, PartialEq, Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
    /// Response types the client accepts; `SAMPLES` (0) only.
    #[prost(int32, repeated, tag = "2")]
    pub accepted_response_types: Vec<i32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Query {
    /// Inclusive bounds, in milliseconds.
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LabelMatcher {
    /// `EQ` is 0.
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

#[derive(Clone, PartialEq, Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// The request for `metric`'s samples in `(start_ms, end_ms]`.
pub fn request(metric: &str, start_ms: i64, end_ms: i64) -> ReadRequest {
    ReadRequest {
        queries: vec![Query {
            // Remote read bounds are inclusive; the plan's ranges are
            // left-open.
            start_timestamp_ms: start_ms + 1,
            end_timestamp_ms: end_ms,
            matchers: vec![LabelMatcher {
                r#type: 0,
                name: "__name__".into(),
                value: metric.into(),
            }],
        }],
        accepted_response_types: vec![0],
    }
}

/// Snappy-compressed protobuf, as remote read sends both ways.
pub fn encode(message: &impl Message) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&message.encode_to_vec())
        .expect("snappy compresses any input")
}

/// The message in a snappy-compressed protobuf body.
pub fn decode<M: Message + Default>(body: &[u8]) -> anyhow::Result<M> {
    let decoded = snap::raw::Decoder::new().decompress_vec(body)?;
    Ok(M::decode(decoded.as_slice())?)
}

/// `metric`'s samples in `(start_ms, end_ms]` from Prometheus remote read,
/// each series' samples together and in time order.
pub async fn read(
    client: &reqwest::Client,
    prometheus_url: &str,
    metric: &str,
    start_ms: i64,
    end_ms: i64,
) -> anyhow::Result<Vec<RawSample>> {
    let body = client
        .post(format!("{prometheus_url}/api/v1/read"))
        .header("Content-Encoding", "snappy")
        .header("Content-Type", "application/x-protobuf")
        .header("X-Prometheus-Remote-Read-Version", "0.1.0")
        .body(encode(&request(metric, start_ms, end_ms)))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let response: ReadResponse = decode(&body)?;
    let mut samples = Vec::new();
    for series in response.results.into_iter().flat_map(|r| r.timeseries) {
        let pairs: Vec<(&str, &str)> = series
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect();
        let labels = crate::series_ids::intern_pairs(&pairs);
        samples.extend(
            series
                .samples
                .into_iter()
                .filter(|s| start_ms < s.timestamp && s.timestamp <= end_ms)
                .map(|s| RawSample {
                    labels: labels.clone(),
                    timestamp_ms: s.timestamp,
                    value: s.value,
                }),
        );
    }
    Ok(samples)
}
