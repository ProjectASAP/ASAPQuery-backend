//! The Prometheus HTTP API as the data plane speaks it: instant-query
//! parameters and results, and fetching raw samples for query-time ranges.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::precompute::RawSample;
use crate::query::{BoundRawSampleWindow, ResultSample};

/// Parameters of an instant query, from its URL query and form body.
pub struct InstantQuery {
    pub query: String,
    /// `None` when the request names no time: evaluate now.
    pub time_ms: Option<i64>,
}

/// The largest evaluation time, in milliseconds either side of the epoch, the
/// data plane answers; beyond it a request is refused as Prometheus refuses
/// a time it cannot represent. Leaves room for any window offset.
pub const MAX_TIME_MS: i64 = i64::MAX / 4;

/// Why an instant query's parameters are refused (400, `bad_data`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid parameter \"time\": {0}")]
pub struct BadTime(pub String);

impl InstantQuery {
    /// Reads `query` and `time` (Unix seconds). `Ok(None)` if either is
    /// missing or `time` is in another format, such as RFC 3339 (the query
    /// is then forwarded); `Err` if `time` is a number that is not finite or
    /// beyond [`MAX_TIME_MS`].
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Result<Option<Self>, BadTime> {
        let params: BTreeMap<String, String> =
            form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
                .chain(form_urlencoded::parse(form))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
        let time_ms = match params.get("time") {
            None => None,
            Some(time) => match time.parse::<f64>() {
                Err(_) => return Ok(None),
                Ok(seconds) => Some(checked_seconds_to_ms(seconds).ok_or_else(|| {
                    BadTime(format!(
                        "{time} is not a time the data plane can evaluate at"
                    ))
                })?),
            },
        };
        let Some(query) = params.get("query") else {
            return Ok(None);
        };
        Ok(Some(Self {
            query: query.clone(),
            time_ms,
        }))
    }
}

/// `seconds` in milliseconds, if finite and within [`MAX_TIME_MS`].
fn checked_seconds_to_ms(seconds: f64) -> Option<i64> {
    let ms = (seconds * 1000.0).round();
    (ms.is_finite() && ms.abs() <= MAX_TIME_MS as f64).then_some(ms as i64)
}

/// A Prometheus `bad_data` error response body.
pub fn bad_data(error: &str) -> Value {
    json!({"status": "error", "errorType": "bad_data", "error": error})
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

/// The PromQL range selector of `range`: its metric and label matchers, every
/// value escaped as a PromQL string, over `end - start` milliseconds.
pub fn range_selector(range: &BoundRawSampleWindow) -> String {
    let mut matchers = vec![format!("__name__=\"{}\"", escape(&range.metric))];
    matchers.extend(
        range
            .matchers
            .iter()
            .map(|m| format!("{}{}\"{}\"", m.name, m.op.as_str(), escape(&m.value))),
    );
    format!(
        "{{{}}}[{}ms]",
        matchers.join(","),
        range.end_ms.saturating_sub(range.start_ms)
    )
}

/// `text` as the inside of a double-quoted PromQL string.
fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            c => escaped.push(c),
        }
    }
    escaped
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
    range: &BoundRawSampleWindow,
) -> anyhow::Result<Vec<RawSample>> {
    let selector = range_selector(range);
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
        let q = InstantQuery::parse(Some("query=up&time=1.5"), b"")
            .unwrap()
            .unwrap();
        assert_eq!((q.query.as_str(), q.time_ms), ("up", Some(1500)));
        let q = InstantQuery::parse(None, b"query=sum%28x%29")
            .unwrap()
            .unwrap();
        assert_eq!((q.query.as_str(), q.time_ms), ("sum(x)", None));
        assert!(InstantQuery::parse(Some("time=1"), b"").unwrap().is_none());
        assert!(
            InstantQuery::parse(Some("query=up&time=2026-01-01T00:00:00Z"), b"")
                .unwrap()
                .is_none()
        );
        for time in ["-1e300", "1e300", "NaN", "inf"] {
            let url = format!("query=up&time={time}");
            assert!(InstantQuery::parse(Some(&url), b"").is_err(), "{time}");
        }
    }

    /// A range selector carries the metric and the window's label matchers,
    /// each value escaped as a PromQL string.
    #[test]
    fn builds_escaped_range_selectors() {
        let range = BoundRawSampleWindow {
            node: 1,
            metric: "m".into(),
            matchers: vec![
                crate::query::LabelMatcher {
                    name: "job".into(),
                    op: crate::query::MatchOp::Equal,
                    value: "a\"b\\c".into(),
                },
                crate::query::LabelMatcher {
                    name: "env".into(),
                    op: crate::query::MatchOp::NotRegex,
                    value: "dev|test".into(),
                },
            ],
            start_ms: 0,
            end_ms: 60_000,
        };
        assert_eq!(
            range_selector(&range),
            r#"{__name__="m",job="a\"b\\c",env!~"dev|test"}[60000ms]"#
        );
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
