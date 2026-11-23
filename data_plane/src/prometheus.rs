//! The Prometheus HTTP API as the data plane speaks it: instant- and
//! range-query parameters and results.

use std::collections::BTreeMap;

use serde::{Serialize, Serializer};
use serde_json::{json, Value};

use crate::query::ResultSample;
use crate::result_cache::Answer;
use crate::series_ids::Labels;

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

/// The most steps one range query may have, as Prometheus allows.
pub const MAX_RANGE_STEPS: i64 = 11_000;

/// Parameters of a range query: evaluated at `start_ms` and every
/// `step_ms` after, up to and including `end_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeQuery {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
}

/// Why a range query's parameters are refused (400, `bad_data`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BadRange(pub String);

impl RangeQuery {
    /// Reads `query`, `start` and `end` (Unix seconds) and `step` (seconds
    /// or a Prometheus duration such as `15s` or `1m30s`). `Ok(None)` if
    /// one is missing or a time is in another format, such as RFC 3339 (the
    /// query is then forwarded); `Err` if they cannot be evaluated: a time
    /// beyond [`MAX_TIME_MS`], a step that is not positive, an end before
    /// the start, or more than [`MAX_RANGE_STEPS`] steps.
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Result<Option<Self>, BadRange> {
        let params: BTreeMap<String, String> =
            form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
                .chain(form_urlencoded::parse(form))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
        let (Some(query), Some(start), Some(end), Some(step)) = (
            params.get("query"),
            params.get("start"),
            params.get("end"),
            params.get("step"),
        ) else {
            return Ok(None);
        };
        let time = |name: &str, text: &str| -> Result<Option<i64>, BadRange> {
            match text.parse::<f64>() {
                Err(_) => Ok(None),
                Ok(seconds) => checked_seconds_to_ms(seconds).map(Some).ok_or_else(|| {
                    BadRange(format!(
                        "invalid parameter \"{name}\": {text} is not a time the data plane can evaluate at"
                    ))
                }),
            }
        };
        let (Some(start_ms), Some(end_ms)) = (time("start", start)?, time("end", end)?) else {
            return Ok(None);
        };
        let step_ms = step_ms(step).filter(|&ms| ms > 0).ok_or_else(|| {
            BadRange(format!(
                "invalid parameter \"step\": {step:?} is not a positive duration"
            ))
        })?;
        if end_ms < start_ms {
            return Err(BadRange(
                "invalid parameter \"end\": end timestamp must not be before start time".into(),
            ));
        }
        // As Prometheus: refused when (end - start) / step exceeds it.
        if (end_ms - start_ms) / step_ms > MAX_RANGE_STEPS {
            return Err(BadRange(format!(
                "exceeded maximum resolution of {MAX_RANGE_STEPS} points per timeseries. Try decreasing the query resolution (?step=XX)"
            )));
        }
        Ok(Some(Self {
            query: query.clone(),
            start_ms,
            end_ms,
            step_ms,
        }))
    }

    /// The evaluation times: `start_ms`, `start_ms + step_ms`, … up to
    /// `end_ms`.
    pub fn times_ms(&self) -> impl Iterator<Item = i64> + '_ {
        (0..)
            .map(|i| self.start_ms + i * self.step_ms)
            .take_while(|t| *t <= self.end_ms)
    }
}

/// A step in milliseconds: seconds (`15`, `0.5`) or a Prometheus duration
/// (`1h30m`, `500ms`).
fn step_ms(text: &str) -> Option<i64> {
    if let Ok(seconds) = text.parse::<f64>() {
        return checked_seconds_to_ms(seconds);
    }
    let mut total: i64 = 0;
    let mut rest = text;
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit())?;
        let number: i64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_len = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit_ms = match &rest[..unit_len] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        total = total.checked_add(number.checked_mul(unit_ms)?)?;
        rest = &rest[unit_len..];
    }
    (!text.is_empty()).then_some(total)
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

/// A Prometheus instant-query response for `samples` at `time_ms`,
/// serialized straight into the response body: no JSON tree is built.
pub struct VectorResponse {
    pub time_ms: i64,
    pub samples: Answer,
}

impl Serialize for VectorResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let time = self.time_ms as f64 / 1000.0;
        Envelope::new("vector", VectorResult(&self.samples, time)).serialize(serializer)
    }
}

/// A Prometheus range-query response: each series' value at each
/// evaluation time it has one, series sorted by labels. Holds each series'
/// labels once (shared with the step answers) and its points as numbers,
/// and is serialized straight into the response body.
pub struct MatrixResponse {
    series: Vec<(Labels, Vec<Point>)>,
}

impl MatrixResponse {
    pub fn new(steps: &[(i64, Answer)]) -> Self {
        let mut by_labels: BTreeMap<&Labels, Vec<Point>> = BTreeMap::new();
        for (time_ms, samples) in steps {
            let time = *time_ms as f64 / 1000.0;
            for sample in samples.iter() {
                by_labels
                    .entry(&sample.labels)
                    .or_default()
                    .push(Point(time, sample.value));
            }
        }
        Self {
            series: by_labels
                .into_iter()
                .map(|(labels, points)| (labels.clone(), points))
                .collect(),
        }
    }
}

impl Serialize for MatrixResponse {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Envelope::new("matrix", MatrixResult(&self.series)).serialize(serializer)
    }
}

/// `{"status": "success", "data": {"resultType": .., "result": ..}}`.
#[derive(Serialize)]
struct Envelope<R: Serialize> {
    status: &'static str,
    data: EnvelopeData<R>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnvelopeData<R: Serialize> {
    result_type: &'static str,
    result: R,
}

impl<R: Serialize> Envelope<R> {
    fn new(result_type: &'static str, result: R) -> Self {
        Self {
            status: "success",
            data: EnvelopeData {
                result_type,
                result,
            },
        }
    }
}

/// One sample: `[time, "value"]`.
#[derive(Clone, Copy)]
struct Point(f64, f64);

impl Serialize for Point {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0, format_value(self.1)).serialize(serializer)
    }
}

struct VectorResult<'a>(&'a [ResultSample], f64);

impl Serialize for VectorResult<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Sample<'a> {
            metric: &'a BTreeMap<String, String>,
            value: Point,
        }
        serializer.collect_seq(self.0.iter().map(|sample| Sample {
            metric: &sample.labels,
            value: Point(self.1, sample.value),
        }))
    }
}

struct MatrixResult<'a>(&'a [(Labels, Vec<Point>)]);

impl Serialize for MatrixResult<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Series<'a> {
            metric: &'a BTreeMap<String, String>,
            values: &'a [Point],
        }
        serializer.collect_seq(self.0.iter().map(|(labels, points)| Series {
            metric: labels,
            values: points,
        }))
    }
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
    use std::sync::Arc;

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

    /// Range parameters: Unix-second times, a step in seconds or as a
    /// duration; refused when they cannot be evaluated.
    #[test]
    fn parses_range_query_parameters() {
        let q = RangeQuery::parse(Some("query=up&start=10&end=70&step=1m"), b"")
            .unwrap()
            .unwrap();
        assert_eq!((q.start_ms, q.end_ms, q.step_ms), (10_000, 70_000, 60_000));
        assert_eq!(q.times_ms().collect::<Vec<_>>(), [10_000, 70_000]);
        let q = RangeQuery::parse(None, b"query=up&start=0&end=1&step=0.25")
            .unwrap()
            .unwrap();
        assert_eq!(q.times_ms().count(), 5);
        assert_eq!(step_ms("1h30m"), Some(5_400_000));
        assert_eq!(step_ms("500ms"), Some(500));
        assert!(RangeQuery::parse(Some("query=up&start=0&end=60"), b"")
            .unwrap()
            .is_none());
        assert!(RangeQuery::parse(
            Some("query=up&start=2026-01-01T00:00:00Z&end=1&step=1"),
            b""
        )
        .unwrap()
        .is_none());
        for bad in [
            "query=up&start=0&end=60&step=0",
            "query=up&start=0&end=60&step=x",
            "query=up&start=60&end=0&step=1",
            "query=up&start=0&end=11001&step=1",
            "query=up&start=1e300&end=1e300&step=1",
        ] {
            assert!(RangeQuery::parse(Some(bad), b"").is_err(), "{bad}");
        }
    }

    /// Range results use Prometheus' matrix shape: one entry per series
    /// with its values in time order.
    #[test]
    fn formats_matrix_results() {
        let a = BTreeMap::from([("job".into(), "a".into())]);
        let b = BTreeMap::from([("job".into(), "b".into())]);
        let sample = |labels: &BTreeMap<String, String>, value| ResultSample {
            labels: Arc::new(labels.clone()),
            value,
        };
        let steps = [
            (1000, Arc::new(vec![sample(&b, 1.0), sample(&a, 2.0)])),
            (2000, Arc::new(vec![sample(&a, 3.0)])),
        ];
        assert_eq!(
            serde_json::to_value(MatrixResponse::new(&steps)).unwrap(),
            json!({"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": {"job": "a"}, "values": [[1.0, "2"], [2.0, "3"]]},
                {"metric": {"job": "b"}, "values": [[1.0, "1"]]},
            ]}})
        );
    }

    /// Results use Prometheus' vector shape and value formatting.
    #[test]
    fn formats_vector_results() {
        let samples = Arc::new(vec![
            ResultSample {
                labels: Arc::new(BTreeMap::from([("job".into(), "a".into())])),
                value: 2.0,
            },
            ResultSample {
                labels: Arc::default(),
                value: f64::INFINITY,
            },
        ]);
        assert_eq!(
            serde_json::to_value(VectorResponse {
                time_ms: 1500,
                samples
            })
            .unwrap(),
            json!({"status": "success", "data": {"resultType": "vector", "result": [
                {"metric": {"job": "a"}, "value": [1.5, "2"]},
                {"metric": {}, "value": [1.5, "+Inf"]},
            ]}})
        );
    }
}
