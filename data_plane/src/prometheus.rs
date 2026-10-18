//! The Prometheus HTTP API as the data plane speaks it: instant- and
//! range-query parameters and results, and fetching raw samples for
//! query-time ranges.

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
    /// Reads `query` and `time` (Unix seconds or RFC 3339); `None` if
    /// `query` is missing or `time` is invalid.
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Option<Self> {
        let params = params(url_query, form)?;
        let time_ms = match params.get("time") {
            None => None,
            Some(time) => Some(parse_time(time)?),
        };
        Some(Self {
            query: params.get("query")?.clone(),
            time_ms,
        })
    }
}

/// Parameters of a range query (`/api/v1/query_range`), from its URL query
/// and form body.
pub struct RangeQuery {
    pub query: String,
    /// First evaluation time, inclusive.
    pub start_ms: i64,
    /// Last possible evaluation time, inclusive.
    pub end_ms: i64,
    /// Time between consecutive evaluations, at least 1 ms.
    pub step_ms: i64,
}

impl RangeQuery {
    /// Reads `query`, `start`, `end` and `step` as Prometheus does; `None` if
    /// one is missing or invalid, which Prometheus then reports.
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Option<Self> {
        let params = params(url_query, form)?;
        let start_ms = parse_time(params.get("start")?)?;
        let end_ms = parse_time(params.get("end")?)?;
        let step_ms = parse_duration_ms(params.get("step")?)?;
        if end_ms < start_ms || step_ms <= 0 || (end_ms - start_ms) / step_ms > MAX_STEPS {
            return None;
        }
        Some(Self {
            query: params.get("query")?.clone(),
            start_ms,
            end_ms,
            step_ms,
        })
    }

    /// The evaluation times `start + k * step` up to `end`, ascending.
    pub fn steps(&self) -> Vec<i64> {
        (self.start_ms..=self.end_ms)
            .step_by(self.step_ms as usize)
            .collect()
    }
}

/// Prometheus' limit on `(end - start) / step`: 11,000 points per series.
const MAX_STEPS: i64 = 11_000;

/// The request's parameters, from its URL query and form body; `None` if it
/// sets a nonzero `limit`, which the data plane leaves to Prometheus.
fn params(url_query: Option<&str>, form: &[u8]) -> Option<BTreeMap<String, String>> {
    let params: BTreeMap<String, String> =
        form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
            .chain(form_urlencoded::parse(form))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
    match params.get("limit").map(String::as_str) {
        None | Some("0") => Some(params),
        Some(_) => None,
    }
}

/// A Prometheus timestamp, Unix seconds or RFC 3339, in ms.
fn parse_time(time: &str) -> Option<i64> {
    if let Ok(seconds) = time.parse::<f64>() {
        return seconds.is_finite().then(|| seconds_to_ms(seconds));
    }
    chrono::DateTime::parse_from_rfc3339(time)
        .ok()
        .map(|t| t.timestamp_millis())
}

/// A Prometheus `<duration | float>`: float seconds, or units `y w d h m s
/// ms` in that order, each at most once (`1h30m`); in ms, truncated as
/// Prometheus does.
fn parse_duration_ms(duration: &str) -> Option<i64> {
    if let Ok(seconds) = duration.parse::<f64>() {
        let ns = seconds * 1e9;
        return (ns.is_finite() && ns.abs() < i64::MAX as f64).then_some(ns as i64 / 1_000_000);
    }
    // In the order a duration lists them.
    const UNITS: [(&str, i64); 7] = [
        ("y", 365 * 86_400_000),
        ("w", 7 * 86_400_000),
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1000),
        ("ms", 1),
    ];
    let (mut rest, mut total, mut last) = (duration, 0i64, None);
    while !rest.is_empty() {
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        let count: i64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        // The longest unit that matches: "ms" rather than "m".
        let (index, (unit, ms)) = UNITS
            .iter()
            .enumerate()
            .filter(|(_, (unit, _))| rest.starts_with(unit))
            .max_by_key(|(_, (unit, _))| unit.len())?;
        if last.is_some_and(|last| last >= index) {
            return None;
        }
        last = Some(index);
        rest = &rest[unit.len()..];
        total = total.checked_add(count.checked_mul(*ms)?)?;
    }
    last.map(|_| total)
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

/// A Prometheus range-query response: each series' value at each of
/// `steps` (`results[k]` at `steps[k]`), series sorted by labels.
pub fn matrix_response(steps: &[i64], results: &[Vec<ResultSample>]) -> Value {
    let mut series: BTreeMap<&BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
    for (time_ms, samples) in steps.iter().zip(results) {
        let time = *time_ms as f64 / 1000.0;
        for s in samples {
            series
                .entry(&s.labels)
                .or_default()
                .push(json!([time, format_value(s.value)]));
        }
    }
    let result: Vec<Value> = series
        .into_iter()
        .map(|(labels, values)| json!({"metric": labels, "values": values}))
        .collect();
    json!({"status": "success", "data": {"resultType": "matrix", "result": result}})
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
/// with a range selector evaluated at the range's end.
pub async fn fetch(
    client: &reqwest::Client,
    prometheus_url: &str,
    range: &RawRange,
) -> anyhow::Result<Vec<RawSample>> {
    let selector = format!(
        "{{__name__=\"{}\"}}[{}ms]",
        range.metric,
        range.end_ms - range.start_ms
    );
    let response: Value = client
        .get(format!("{prometheus_url}/api/v1/query"))
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
        let q = InstantQuery::parse(Some("query=up&time=2026-01-01T00:00:00Z"), b"").unwrap();
        assert_eq!(
            q.time_ms,
            Some(1_767_225_600_000),
            "RFC 3339, as Prometheus accepts"
        );
        assert!(InstantQuery::parse(Some("query=up&time=now"), b"").is_none());
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

    fn range(params: &str) -> Option<(i64, i64, i64)> {
        RangeQuery::parse(Some(params), b"").map(|q| (q.start_ms, q.end_ms, q.step_ms))
    }

    /// `start` and `end` are Unix seconds or RFC 3339; `step` is float
    /// seconds or a Prometheus duration; parameters may come in the body.
    #[test]
    fn parses_range_query_parameters() {
        assert_eq!(
            range("query=up&start=1.5&end=61.5&step=15"),
            Some((1500, 61_500, 15_000))
        );
        assert_eq!(
            range("query=up&start=1&end=2&step=0.25"),
            Some((1000, 2000, 250))
        );
        assert_eq!(
            range("query=up&start=2026-01-01T00:00:00Z&end=2026-01-01T00:01:00.5%2B00:00&step=1m"),
            Some((1_767_225_600_000, 1_767_225_660_500, 60_000))
        );
        assert_eq!(
            range("query=up&start=0&end=0&step=1h30m"),
            Some((0, 0, 90 * 60_000))
        );
        for (step, ms) in [
            ("500ms", 500),
            ("1d", 86_400_000),
            ("1w", 7 * 86_400_000),
            ("1y", 365 * 86_400_000),
            ("2m30s", 150_000),
        ] {
            assert_eq!(
                range(&format!("query=up&start=0&end=0&step={step}")),
                Some((0, 0, ms)),
                "{step}"
            );
        }
        let q = RangeQuery::parse(None, b"query=sum%28x%29&start=0&end=60&step=30").unwrap();
        assert_eq!(q.query, "sum(x)");
    }

    /// What Prometheus rejects is not parsed: missing parameters, `end`
    /// before `start`, a step that is not positive or not a duration, and
    /// more than 11,000 steps.
    #[test]
    fn rejects_invalid_range_queries() {
        for params in [
            "start=0&end=60&step=15",
            "query=up&end=60&step=15",
            "query=up&start=0&step=15",
            "query=up&start=0&end=60",
            "query=up&start=60&end=0&step=15",
            "query=up&start=0&end=60&step=0",
            "query=up&start=0&end=60&step=-1",
            "query=up&start=0&end=60&step=1m1h",
            "query=up&start=0&end=60&step=15x",
            "query=up&start=yesterday&end=60&step=15",
            "query=up&start=0&end=11001&step=1",
        ] {
            assert_eq!(range(params), None, "{params}");
        }
        assert!(range("query=up&start=0&end=11000&step=1").is_some());
    }

    /// Steps run from `start` by `step` and include `end` when on a step.
    #[test]
    fn steps_include_the_end() {
        let q = |end_ms| RangeQuery {
            query: "up".into(),
            start_ms: 1000,
            end_ms,
            step_ms: 500,
        };
        assert_eq!(q(2000).steps(), vec![1000, 1500, 2000]);
        assert_eq!(q(1999).steps(), vec![1000, 1500]);
        assert_eq!(q(1000).steps(), vec![1000]);
    }

    /// Range results use Prometheus' matrix shape: one entry per series
    /// with its values in time order; a series absent at a step has no
    /// value there, and series absent at every step are left out.
    #[test]
    fn formats_matrix_results() {
        let sample = |job: &str, value| ResultSample {
            labels: BTreeMap::from([("job".into(), job.into())]),
            value,
        };
        let results = vec![
            vec![sample("b", 1.0), sample("a", 2.0)],
            vec![sample("b", 3.0)],
            vec![],
        ];
        assert_eq!(
            matrix_response(&[1500, 2000, 2500], &results),
            json!({"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": {"job": "a"}, "values": [[1.5, "2"]]},
                {"metric": {"job": "b"}, "values": [[1.5, "1"], [2.0, "3"]]},
            ]}})
        );
        assert_eq!(
            matrix_response(&[1500], &[vec![]]),
            json!({"status": "success", "data": {"resultType": "matrix", "result": []}})
        );
    }
}
