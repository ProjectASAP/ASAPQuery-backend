//! The Prometheus HTTP API as the data plane speaks it: instant-query
//! parameters and results, and fetching raw samples for query-time ranges.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::precompute::RawSample;
use crate::query::{RawRange, ResultSample};

/// Parameters of an instant query, from its URL query and form body.
pub struct InstantQuery {
    pub query: String,
    /// `None` when the request names no time: evaluate now.
    pub time_ms: Option<i64>,
}

impl InstantQuery {
    /// Reads `query` and `time` (Unix seconds); `None` if either is missing
    /// or `time` is in another format, such as RFC 3339.
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Option<Self> {
        let params: BTreeMap<String, String> =
            form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
                .chain(form_urlencoded::parse(form))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
        let time_ms = match params.get("time") {
            None => None,
            Some(time) => Some(seconds_to_ms(time.parse().ok()?)),
        };
        Some(Self {
            query: params.get("query")?.clone(),
            time_ms,
        })
    }
}

fn seconds_to_ms(seconds: f64) -> i64 {
    (seconds * 1000.0).round() as i64
}

fn ms_to_seconds(ms: i64) -> String {
    format!("{:.3}", ms as f64 / 1000.0)
}

/// A Prometheus instant-query response for `samples` at `time_ms`.
pub fn vector_response(time_ms: i64, samples: &[ResultSample]) -> Value {
    let time = time_ms as f64 / 1000.0;
    let result: Vec<Value> = samples
        .iter()
        .map(|s| json!({"metric": s.labels, "value": [time, format_value(s.value)]}))
        .collect();
    json!({"status": "success", "data": {"resultType": "vector", "result": result}})
}

/// Prometheus' sample value format.
fn format_value(value: f64) -> String {
    match value {
        v if v.is_nan() => "NaN".into(),
        v if v == f64::INFINITY => "+Inf".into(),
        v if v == f64::NEG_INFINITY => "-Inf".into(),
        v => v.to_string(),
    }
}

/// The samples of `range`'s metric in `(start, end]`, read from Prometheus
/// with a range selector evaluated at the range's end. `headers` are sent
/// with the request: the caller's end-to-end headers, such as
/// `Authorization` and `X-Scope-OrgID`, so Prometheus authorizes and scopes
/// the read as it would the caller's own query.
pub async fn fetch(
    client: &reqwest::Client,
    prometheus_url: &str,
    headers: &reqwest::header::HeaderMap,
    range: &RawRange,
) -> anyhow::Result<Vec<RawSample>> {
    let selector = format!(
        "{{__name__=\"{}\"}}[{}ms]",
        range.metric,
        range.end_ms - range.start_ms
    );
    let response: Value = client
        .get(format!("{prometheus_url}/api/v1/query"))
        .headers(headers.clone())
        .query(&[
            ("query", selector.as_str()),
            ("time", &ms_to_seconds(range.end_ms)),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let malformed = || anyhow::anyhow!("unexpected Prometheus response: {response}");
    let series = response["data"]["result"]
        .as_array()
        .ok_or_else(malformed)?;
    let mut samples = Vec::new();
    for series in series {
        let labels: BTreeMap<String, String> = serde_json::from_value(series["metric"].clone())?;
        let labels = Arc::new(labels);
        for point in series["values"].as_array().ok_or_else(malformed)? {
            let seconds = point[0].as_f64().ok_or_else(malformed)?;
            let value = point[1].as_str().ok_or_else(malformed)?;
            let timestamp_ms = seconds_to_ms(seconds);
            // Prometheus 2 ranges include their start; the plan's do not.
            if timestamp_ms <= range.start_ms {
                continue;
            }
            samples.push(RawSample {
                labels: labels.clone(),
                timestamp_ms,
                value: parse_value(value).ok_or_else(malformed)?,
            });
        }
    }
    Ok(samples)
}

fn parse_value(value: &str) -> Option<f64> {
    match value {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        v => v.parse().ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parameters come from the URL or the form body; times are Unix seconds.
    #[test]
    fn parses_instant_query_parameters() {
        let q = InstantQuery::parse(Some("query=up&time=1.5"), b"").unwrap();
        assert_eq!((q.query.as_str(), q.time_ms), ("up", Some(1500)));
        let q = InstantQuery::parse(None, b"query=sum%28x%29").unwrap();
        assert_eq!((q.query.as_str(), q.time_ms), ("sum(x)", None));
        assert!(InstantQuery::parse(Some("time=1"), b"").is_none());
        assert!(InstantQuery::parse(Some("query=up&time=2026-01-01T00:00:00Z"), b"").is_none());
    }

    /// Results use Prometheus' vector shape and value formatting.
    #[test]
    fn formats_vector_results() {
        let samples = [
            ResultSample {
                labels: BTreeMap::from([("job".into(), "a".into())]),
                value: 2.0,
            },
            ResultSample {
                labels: BTreeMap::new(),
                value: f64::INFINITY,
            },
        ];
        assert_eq!(
            vector_response(1500, &samples),
            json!({"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {"job": "a"}, "value": [1.5, "2"]},
                {"metric": {}, "value": [1.5, "+Inf"]},
            ]}})
        );
        assert_eq!(ms_to_seconds(4_200_001), "4200.001");
    }
}
