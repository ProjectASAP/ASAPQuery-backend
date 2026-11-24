//! The Prometheus HTTP API as the data plane speaks it: instant- and
//! range-query parameters and results.

use std::collections::BTreeMap;

use serde::{Serialize, Serializer};
use serde_json::{json, Value};

use crate::query::ResultSample;
use crate::result_cache::Answer;

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

/// Why a query's parameters are refused (400, `bad_data`): the error text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct BadParameter(pub String);

impl InstantQuery {
    /// Reads `query` and `time` ([`parse_time_ms`]; empty or missing:
    /// now). `Ok(None)` if `query` is missing or `time` is Prometheus' min
    /// or max time (the request is then forwarded, or refused without
    /// forwarding); `Err` if `time` is not a parsable time or is beyond
    /// [`MAX_TIME_MS`].
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Result<Option<Self>, BadParameter> {
        let params: BTreeMap<String, String> =
            form_urlencoded::parse(url_query.unwrap_or_default().as_bytes())
                .chain(form_urlencoded::parse(form))
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
        let time_ms = match params.get("time").filter(|time| !time.is_empty()) {
            None => None,
            Some(time) => match parse_time_ms("time", time)? {
                Some(ms) => Some(ms),
                None => return Ok(None),
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

impl RangeQuery {
    /// Reads `query`, `start` and `end` ([`parse_time_ms`]) and `step`
    /// (seconds or a Prometheus duration such as `15s` or `1m30s`).
    /// `Ok(None)` if one is missing or a time is Prometheus' min or max
    /// time (the request is then forwarded, or refused without
    /// forwarding); `Err` if they cannot be evaluated: a time that is not
    /// parsable or is beyond [`MAX_TIME_MS`], a step that is not positive,
    /// an end before the start, or more than [`MAX_RANGE_STEPS`] steps.
    pub fn parse(url_query: Option<&str>, form: &[u8]) -> Result<Option<Self>, BadParameter> {
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
        let (Some(start_ms), Some(end_ms)) =
            (parse_time_ms("start", start)?, parse_time_ms("end", end)?)
        else {
            return Ok(None);
        };
        let step_ms = step_ms(step).filter(|&ms| ms > 0).ok_or_else(|| {
            BadParameter(format!(
                "invalid parameter \"step\": {step:?} is not a positive duration"
            ))
        })?;
        if end_ms < start_ms {
            return Err(BadParameter(
                "invalid parameter \"end\": end timestamp must not be before start time".into(),
            ));
        }
        // As Prometheus: refused when (end - start) / step exceeds it.
        if (end_ms - start_ms) / step_ms > MAX_RANGE_STEPS {
            return Err(BadParameter(format!(
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

/// Prometheus' smallest and largest times, as its clients send them for
/// "no bound" (`v1.MinTime` and `v1.MaxTime` formatted as RFC 3339).
const PROMETHEUS_MIN_TIME: &str = "-292273086-05-16T16:47:06Z";
const PROMETHEUS_MAX_TIME: &str = "292277025-08-17T07:12:55.999999999Z";

/// Query parameter `name`'s time `text` in milliseconds since the epoch, as
/// the Prometheus HTTP API reads it: Unix seconds with an optional fraction
/// (rounded to the millisecond), else RFC 3339 ([`rfc3339_ms`]). `Ok(None)`
/// for Prometheus' min or max time, which the data plane does not evaluate
/// at. `Err` is the 400 error text: Prometheus' for a value in neither
/// form, or a time beyond [`MAX_TIME_MS`].
pub fn parse_time_ms(name: &str, text: &str) -> Result<Option<i64>, BadParameter> {
    if text == PROMETHEUS_MIN_TIME || text == PROMETHEUS_MAX_TIME {
        return Ok(None);
    }
    match text.parse::<f64>() {
        Ok(seconds) => checked_seconds_to_ms(seconds).map(Some).ok_or_else(|| {
            BadParameter(format!(
                "invalid parameter \"{name}\": {text} is not a time the data plane can evaluate at"
            ))
        }),
        Err(_) => rfc3339_ms(text).map(Some).ok_or_else(|| {
            BadParameter(format!(
                "invalid parameter \"{name}\": cannot parse {text:?} to a valid timestamp"
            ))
        }),
    }
}

/// An RFC 3339 time (`2026-11-24T05:00:00Z`, `2026-11-24T05:00:00.25+01:00`)
/// in milliseconds since the epoch (fraction truncated, as Prometheus
/// truncates a parsed time), as Go's `time.Parse(time.RFC3339Nano, ..)`
/// accepts it: a four-digit year, `T`, a one- or two-digit hour, a fraction
/// of any length after `.` or `,`, and `Z` or a `±hh:mm` offset of at most
/// 24 hours and 60 minutes.
fn rfc3339_ms(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let mut at = 0;
    let year = digits(bytes, &mut at, 4, 4)?;
    separator(bytes, &mut at, b'-')?;
    let month = digits(bytes, &mut at, 2, 2)?;
    separator(bytes, &mut at, b'-')?;
    let day = digits(bytes, &mut at, 2, 2)?;
    separator(bytes, &mut at, b'T')?;
    let hour = digits(bytes, &mut at, 1, 2)?;
    separator(bytes, &mut at, b':')?;
    let minute = digits(bytes, &mut at, 2, 2)?;
    separator(bytes, &mut at, b':')?;
    let second = digits(bytes, &mut at, 2, 2)?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=days_in_month).contains(&day) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    // Fractional seconds: `.` or `,` and one or more digits, the first
    // three kept.
    let mut fraction_ms = 0;
    if matches!(bytes.get(at), Some(b'.' | b',')) {
        let count = bytes[at + 1..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        if count == 0 {
            return None;
        }
        fraction_ms = bytes[at + 1..at + 1 + count.min(3)]
            .iter()
            .fold(0, |n, d| n * 10 + i64::from(d - b'0'))
            * 10_i64.pow(3 - count.min(3) as u32);
        at += 1 + count;
    }
    let offset_s = match &bytes[at..] {
        b"Z" => 0,
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2]
            if [h1, h2, m1, m2].iter().all(|b| b.is_ascii_digit()) =>
        {
            let hours = i64::from((h1 - b'0') * 10 + (h2 - b'0'));
            let minutes = i64::from((m1 - b'0') * 10 + (m2 - b'0'));
            if hours > 24 || minutes > 60 {
                return None;
            }
            let offset = hours * 3600 + minutes * 60;
            if *sign == b'-' {
                -offset
            } else {
                offset
            }
        }
        _ => return None,
    };
    // Days since the epoch of the civil date (H. Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_s;
    Some(seconds * 1000 + fraction_ms)
}

/// The number in `min..=max` digits at `*at` of `bytes`, moving `*at` past
/// them.
fn digits(bytes: &[u8], at: &mut usize, min: usize, max: usize) -> Option<i64> {
    let count = bytes[*at..]
        .iter()
        .take(max)
        .take_while(|b| b.is_ascii_digit())
        .count();
    if count < min {
        return None;
    }
    let value = bytes[*at..*at + count]
        .iter()
        .fold(0, |n, d| n * 10 + i64::from(d - b'0'));
    *at += count;
    Some(value)
}

/// Byte `c` at `*at` of `bytes`, moving `*at` past it.
fn separator(bytes: &[u8], at: &mut usize, c: u8) -> Option<()> {
    (bytes.get(*at) == Some(&c)).then(|| *at += 1)
}

/// `seconds` in milliseconds, if finite and within [`MAX_TIME_MS`].
/// As Prometheus: the whole seconds, plus the fraction rounded to the
/// millisecond (which keeps a sub-millisecond fraction exact at large
/// times, where `seconds * 1000` would not).
fn checked_seconds_to_ms(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() || seconds.abs() > (MAX_TIME_MS / 1000) as f64 {
        return None;
    }
    Some(seconds.trunc() as i64 * 1000 + (seconds.fract() * 1000.0).round() as i64)
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

/// The JSON body of a range-query response over `steps`, each step's
/// evaluation time and answer: Prometheus' matrix, each series' value at
/// each evaluation time it has one, series sorted by labels.
///
/// Written directly rather than through serde types: a long range of
/// many series is mostly cached step answers (241 steps of 2,800 series
/// in a dashboard's 1 h panel), and grouping them by comparing label maps,
/// then formatting each value into its own `String`, cost about a second.
/// Each series' labels are one `Arc` shared by every step's answer (the
/// interned series, or `series_ids::result_labels`), so a series is
/// found by that pointer and its labels compared only once; each step's
/// time and each series' labels are formatted once.
pub fn range_response_body(steps: &[(i64, Answer)]) -> Result<Vec<u8>, serde_json::Error> {
    type Labels = BTreeMap<String, String>;
    // Each series' points as (step, value), in step order.
    let mut series: Vec<(&Labels, Vec<(u32, f64)>)> = Vec::new();
    let mut by_labels: BTreeMap<&Labels, usize> = BTreeMap::new();
    let mut by_pointer: ahash::HashMap<*const Labels, usize> = ahash::HashMap::default();
    for (step, (_, samples)) in steps.iter().enumerate() {
        for sample in samples.iter() {
            let labels: &Labels = &sample.labels;
            let index = *by_pointer
                .entry(labels as *const Labels)
                .or_insert_with(|| {
                    *by_labels.entry(labels).or_insert_with(|| {
                        series.push((labels, Vec::new()));
                        series.len() - 1
                    })
                });
            series[index].1.push((step as u32, sample.value));
        }
    }
    // Each step's time as serde_json writes the f64 seconds.
    let times: Vec<Vec<u8>> = steps
        .iter()
        .map(|(time_ms, _)| serde_json::to_vec(&(*time_ms as f64 / 1000.0)))
        .collect::<Result<_, _>>()?;
    let points: usize = series.iter().map(|(_, p)| p.len()).sum();
    let mut out = Vec::with_capacity(64 + series.len() * 128 + points * 32);
    out.extend_from_slice(br#"{"status":"success","data":{"resultType":"matrix","result":["#);
    for (n, index) in by_labels.values().enumerate() {
        let (labels, points) = &series[*index];
        if n > 0 {
            out.push(b',');
        }
        out.extend_from_slice(br#"{"metric":"#);
        serde_json::to_writer(&mut out, labels)?;
        out.extend_from_slice(br#","values":["#);
        for (k, (step, value)) in points.iter().enumerate() {
            if k > 0 {
                out.push(b',');
            }
            out.push(b'[');
            out.extend_from_slice(&times[*step as usize]);
            out.extend_from_slice(b",\"");
            write_value(&mut out, *value).map_err(serde_json::Error::io)?;
            out.extend_from_slice(b"\"]");
        }
        out.extend_from_slice(b"]}");
    }
    out.extend_from_slice(b"]}}");
    Ok(out)
}

/// A value as [`format_value`] writes it, without allocating: shortest
/// round-trip digits, never an exponent, as Rust's `Display` writes them,
/// built from `ryu`'s digits at a fraction of the formatting machinery's
/// cost (most of a long range's body is values). Where two shortest
/// digit strings round-trip, `ryu` may pick the other last digit; both
/// read back as the same `f64`.
fn write_value(out: &mut Vec<u8>, value: f64) -> std::io::Result<()> {
    use std::io::Write;
    if value.is_nan() {
        return out.write_all(b"NaN");
    }
    if value.is_infinite() {
        return out.write_all(if value > 0.0 { b"+Inf" } else { b"-Inf" });
    }
    let mut buffer = ryu::Buffer::new();
    let text = buffer.format_finite(value).as_bytes();
    // Most values: plain decimal already; `Display` writes no `.0`.
    if !text.contains(&b'e') {
        return out.write_all(text.strip_suffix(b".0").unwrap_or(text));
    }
    let (negative, text) = match text.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, text),
    };
    // `ryu` writes `d.ddd`, `d.dddeX` or `deX`: the digits, and where the
    // decimal point falls among them.
    let (mantissa, exponent) = match text.iter().position(|&b| b == b'e') {
        Some(e) => (
            &text[..e],
            std::str::from_utf8(&text[e + 1..])
                .ok()
                .and_then(|x| x.parse::<i32>().ok())
                .unwrap_or(0),
        ),
        None => (text, 0),
    };
    let (whole, fraction) = match mantissa.iter().position(|&b| b == b'.') {
        Some(dot) => (&mantissa[..dot], &mantissa[dot + 1..]),
        None => (mantissa, &mantissa[..0]),
    };
    let mut digits: Vec<u8> = whole.iter().chain(fraction).copied().collect();
    let mut point = whole.len() as i32 + exponent;
    let leading = digits.iter().take_while(|&&d| d == b'0').count();
    digits.drain(..leading);
    point -= leading as i32;
    while digits.last() == Some(&b'0') {
        digits.pop();
    }
    if negative {
        out.push(b'-');
    }
    if digits.is_empty() {
        out.push(b'0');
    } else if point <= 0 {
        out.extend_from_slice(b"0.");
        out.extend(std::iter::repeat_n(b'0', (-point) as usize));
        out.extend_from_slice(&digits);
    } else if point as usize >= digits.len() {
        out.extend_from_slice(&digits);
        out.extend(std::iter::repeat_n(b'0', point as usize - digits.len()));
    } else {
        out.extend_from_slice(&digits[..point as usize]);
        out.push(b'.');
        out.extend_from_slice(&digits[point as usize..]);
    }
    Ok(())
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

    /// Parameters come from the URL or the form body; `time` is Unix
    /// seconds or RFC 3339, an empty one is now.
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
        let q = InstantQuery::parse(Some("query=up&time=2026-01-01T00:00:00Z"), b"")
            .unwrap()
            .unwrap();
        assert_eq!(q.time_ms, Some(1_767_225_600_000));
        assert!(InstantQuery::parse(Some("query=up&time=tomorrow"), b"").is_err());
        let q = InstantQuery::parse(Some("query=up&time="), b"")
            .unwrap()
            .unwrap();
        assert_eq!(q.time_ms, None);
        // Prometheus' min and max times: forwarded.
        for time in [PROMETHEUS_MIN_TIME, PROMETHEUS_MAX_TIME] {
            let url = format!("query=up&time={time}");
            assert!(InstantQuery::parse(Some(&url), b"").unwrap().is_none());
        }
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
        let q = RangeQuery::parse(
            Some("query=up&start=1970-01-01T00:00:01Z&end=2&step=1"),
            b"",
        )
        .unwrap()
        .unwrap();
        assert_eq!((q.start_ms, q.end_ms), (1_000, 2_000));
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

    /// RFC 3339 times as Go's `time.Parse(time.RFC3339Nano, ..)` accepts
    /// them, in milliseconds since the epoch (fractions truncated, as
    /// Prometheus truncates a parsed time to the millisecond).
    #[test]
    fn parses_rfc3339_times() {
        for (text, ms) in [
            ("1970-01-01T00:00:00Z", 0),
            ("2026-11-24T05:00:00Z", 1_795_496_400_000),
            ("2026-11-24T05:00:00.5Z", 1_795_496_400_500),
            ("2026-11-24T05:00:00.123456789Z", 1_795_496_400_123),
            ("2026-11-24T06:30:00+01:30", 1_795_496_400_000),
            ("2026-11-24T00:00:00-05:00", 1_795_496_400_000),
            ("2024-02-29T00:00:00Z", 1_709_164_800_000),
            ("1969-12-31T23:59:59.999Z", -1),
            ("0001-01-01T00:00:00Z", -62_135_596_800_000),
            // Go's lenient forms: a long fraction, a comma, a one-digit
            // hour, a 24-hour offset.
            ("2026-11-24T05:00:00.1234567890123Z", 1_795_496_400_123),
            ("2026-11-24T05:00:00,5Z", 1_795_496_400_500),
            ("2026-11-24T5:00:00Z", 1_795_496_400_000),
            ("2026-11-25T05:00:00+24:00", 1_795_496_400_000),
        ] {
            assert_eq!(rfc3339_ms(text), Some(ms), "{text}");
        }
        for text in [
            "2026-11-24T05:00:00",
            "2026-11-24t05:00:00Z",
            "2026-11-24 05:00:00Z",
            "2026-11-24T05:00:00.Z",
            "2026-11-24T05:00Z",
            "2023-02-29T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2026-11-24T05:00:60Z",
            "2026-11-24T05:00:00+0100",
            "2026-11-24T05:00:00+25:00",
            "2026-11-24T05:00:00+01:61",
            "+2026-11-24T05:00:00Z",
            "2026-11-24T05:00:00Z ",
        ] {
            assert_eq!(rfc3339_ms(text), None, "{text}");
        }
        assert_eq!(
            parse_time_ms("time", "x").unwrap_err().0,
            "invalid parameter \"time\": cannot parse \"x\" to a valid timestamp"
        );
        assert_eq!(parse_time_ms("end", "1.0015"), Ok(Some(1_002)));
        // The fraction rounded on its own, as Prometheus rounds it.
        assert_eq!(
            parse_time_ms("time", "1763408220.0665"),
            Ok(Some(1_763_408_220_066))
        );
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
            serde_json::from_slice::<Value>(&range_response_body(&steps).unwrap()).unwrap(),
            json!({"status": "success", "data": {"resultType": "matrix", "result": [
                {"metric": {"job": "a"}, "values": [[1.0, "2"], [2.0, "3"]]},
                {"metric": {"job": "b"}, "values": [[1.0, "1"]]},
            ]}})
        );
    }

    /// The range body is byte for byte what serializing the matrix with
    /// serde wrote before it was written directly: series grouped and
    /// sorted by labels whether or not their labels share an `Arc`,
    /// special values, label text needing escapes, sub-second times.
    #[test]
    fn range_body_equals_the_serde_matrix() {
        #[derive(Serialize)]
        struct Series<'a> {
            metric: &'a BTreeMap<String, String>,
            values: Vec<Point>,
        }
        fn reference(steps: &[(i64, Answer)]) -> Vec<u8> {
            let mut by_labels: BTreeMap<&BTreeMap<String, String>, Vec<Point>> = BTreeMap::new();
            for (time_ms, samples) in steps {
                for sample in samples.iter() {
                    by_labels
                        .entry(&sample.labels)
                        .or_default()
                        .push(Point(*time_ms as f64 / 1000.0, sample.value));
                }
            }
            let result: Vec<Series> = by_labels
                .into_iter()
                .map(|(metric, values)| Series { metric, values })
                .collect();
            serde_json::to_vec(&Envelope::new("matrix", result)).unwrap()
        }
        let shared = Arc::new(BTreeMap::from([
            ("job".to_string(), "a \"quoted\" \\ \u{1}".to_string()),
            ("__name__".to_string(), "m".to_string()),
        ]));
        let other = BTreeMap::from([("job".to_string(), "b".to_string())]);
        let values = [
            1.5,
            f64::NAN,
            f64::INFINITY,
            -0.0,
            1e21,
            1e-7,
            -3.25,
            123456789.125,
        ];
        let steps: Vec<(i64, Answer)> = values
            .iter()
            .enumerate()
            .map(|(step, &value)| {
                let mut samples = vec![
                    ResultSample {
                        labels: Arc::new(other.clone()),
                        value: -value,
                    },
                    ResultSample {
                        labels: shared.clone(),
                        value,
                    },
                ];
                if step % 3 == 0 {
                    samples.push(ResultSample {
                        labels: Arc::default(),
                        value: f64::NEG_INFINITY,
                    });
                }
                (1_700_000_000_000 + step as i64 * 15_001, Arc::new(samples))
            })
            .collect();
        assert_eq!(
            String::from_utf8(range_response_body(&steps).unwrap()).unwrap(),
            String::from_utf8(reference(&steps)).unwrap()
        );
    }

    /// A value is written in `Display`'s notation (no exponent, no `.0`)
    /// and reads back as exactly the same `f64`, over special values,
    /// powers of ten either side of the exponent switch, and a million
    /// pseudo-random bit patterns; nearly always the text is `Display`'s.
    #[test]
    fn values_are_written_as_display_writes_them() {
        let mut values = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            1.5,
            1e21,
            1e22,
            1e-7,
            1e-6,
            5e-324,
            f64::MAX,
            f64::MIN_POSITIVE,
            123456789.125,
            1e15,
            1e16,
            1e17,
            9007199254740993.0,
        ];
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..1_000_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(f64::from_bits(state));
        }
        let mut differ = 0;
        let count = values.len();
        for value in values {
            let mut out = Vec::new();
            write_value(&mut out, value).unwrap();
            let text = String::from_utf8(out).unwrap();
            let display = format_value(value);
            if value.is_finite() {
                assert_eq!(
                    text.parse::<f64>().unwrap().to_bits(),
                    value.to_bits(),
                    "{text}"
                );
                assert!(!text.contains('e') && !text.ends_with(".0"), "{text}");
                assert_eq!(text.len(), display.len(), "{text} vs {display}");
            } else {
                assert_eq!(text, display);
            }
            differ += usize::from(text != display);
        }
        assert!(
            differ * 100 < count,
            "{differ} of {count} differ from Display"
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
