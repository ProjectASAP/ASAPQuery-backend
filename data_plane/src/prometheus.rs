//! The Prometheus HTTP API as the data plane speaks it: instant-query
//! parameters and results.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::query::ResultSample;

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
    }
}
