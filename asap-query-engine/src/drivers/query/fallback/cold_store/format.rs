//! JSONL raw-sample format + key-prefix helpers.
//!
//! The format is intentionally boring: one JSON object per line,
//! one sample per object. That makes it cheap for an OTel exporter
//! or a test fixture to produce and for any reader (Python, jq,
//! ClickHouse external table, etc.) to consume.
//!
//! Key layout is hour-bucketed so a range scan that spans `N`
//! hours touches at most `N` key-prefixes regardless of ingest
//! rate. The same layout works on S3 (list-objects-v2 with
//! `Prefix`) without modification.

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use tracing::warn;

use super::ColdStoreError;

/// A single raw observability sample as written by the cold
/// exporter. `labels` is a `BTreeMap` so the on-disk JSON is
/// deterministic per sample (useful for golden tests).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawSample {
    pub ts_ms: i64,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Key-prefix for the hour-bucket containing `ts_ms`, relative to
/// the cold-store root. Identical shape for local-FS and S3.
///
/// Example: `raw/http_requests_total/2026/04/21/08/`
pub fn part_path_prefix(metric: &str, ts_ms: i64) -> String {
    let dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    format!(
        "raw/{}/{:04}/{:02}/{:02}/{:02}/",
        metric,
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
    )
}

/// Enumerate the hour-bucket prefixes covering the half-open
/// range `[start_ms, end_ms)`. Always returns at least one bucket
/// (the one containing `start_ms`). Used by `ColdStore` impls to
/// drive object-listing / directory-walk.
pub fn hour_prefixes(metric: &str, start_ms: i64, end_ms: i64) -> Vec<String> {
    if end_ms <= start_ms {
        return vec![part_path_prefix(metric, start_ms)];
    }
    const HOUR_MS: i64 = 3_600_000;
    let first_hour = (start_ms / HOUR_MS) * HOUR_MS;
    // Align `end` up to the next hour boundary; we scan strictly
    // *less than* `end_ms` so the last included bucket is the one
    // containing `end_ms - 1`.
    let last_hour = ((end_ms - 1) / HOUR_MS) * HOUR_MS;
    let mut out = Vec::new();
    let mut cur = first_hour;
    while cur <= last_hour {
        out.push(part_path_prefix(metric, cur));
        cur += HOUR_MS;
    }
    out
}

/// Parse a `.jsonl` blob into `RawSample`s, filtering to the
/// half-open range `[start_ms, end_ms)`. Mid-file malformed lines
/// are hard errors. A malformed *trailing* line is tolerated
/// (warn + drop) only when the blob has no terminating newline —
/// that's the producer-mid-flush shape, see `docs/design-sketch-db.md`
/// §5.2. `path` is threaded through purely for the warn log.
pub fn parse_jsonl(
    bytes: &[u8],
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<RawSample>, ColdStoreError> {
    parse_jsonl_at(bytes, start_ms, end_ms, None)
}

pub fn parse_jsonl_at(
    bytes: &[u8],
    start_ms: i64,
    end_ms: i64,
    path: Option<&Path>,
) -> Result<Vec<RawSample>, ColdStoreError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| ColdStoreError::Malformed(format!("non-utf8: {e}")))?;
    let trailing_torn = !text.is_empty() && !text.ends_with('\n');
    let lines: Vec<&str> = text.lines().collect();
    let last = lines.len().saturating_sub(1);
    let mut out = Vec::new();
    for (i, raw) in lines.iter().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<RawSample>(line) {
            Ok(s) if s.ts_ms >= start_ms && s.ts_ms < end_ms => out.push(s),
            Ok(_) => {}
            Err(e) if i == last && trailing_torn => warn!(
                path = %path.map(|p| p.display().to_string()).unwrap_or_default(),
                line = %line.chars().take(200).collect::<String>(),
                error = %e,
                "cold-store: dropping torn trailing line in JSONL part \
                 (no terminating newline; likely producer mid-flush)"
            ),
            Err(e) => return Err(ColdStoreError::Malformed(format!("line {}: {}", i + 1, e))),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;

    fn ts_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn prefix_shape() {
        let ts = ts_ms(2026, 4, 21, 8, 15);
        assert_eq!(
            part_path_prefix("http_requests_total", ts),
            "raw/http_requests_total/2026/04/21/08/"
        );
    }

    #[test]
    fn hour_prefixes_single_bucket() {
        let start = ts_ms(2026, 4, 21, 8, 15);
        let end = start + 60_000;
        let p = hour_prefixes("m", start, end);
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn hour_prefixes_span_two_hours() {
        let start = ts_ms(2026, 4, 21, 8, 15);
        let end = start + 3_600_000 + 1;
        let p = hour_prefixes("m", start, end);
        assert_eq!(p.len(), 2);
        assert_ne!(p[0], p[1]);
    }

    #[test]
    fn parse_jsonl_filters_range() {
        let blob = r#"{"ts_ms":100,"labels":{"a":"1"},"value":1.0}
{"ts_ms":200,"labels":{"a":"2"},"value":2.0}
{"ts_ms":300,"labels":{"a":"3"},"value":3.0}
"#;
        let out = parse_jsonl(blob.as_bytes(), 150, 300).unwrap();
        // end is exclusive -> only ts=200 matches
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts_ms, 200);
    }

    #[test]
    fn parse_jsonl_malformed_errs() {
        let blob = "not-json\n";
        assert!(parse_jsonl(blob.as_bytes(), 0, i64::MAX).is_err());
    }

    /// Pins follow-up #5: producer is mid-flush, reader sees a
    /// part file whose last line is partial (no terminating
    /// newline). We must surface the N preceding records, not
    /// fail the whole scan.
    #[test]
    fn parse_jsonl_ignores_torn_trailing_line() {
        let blob = "{\"ts_ms\":100,\"labels\":{\"a\":\"1\"},\"value\":1.0}\n\
                    {\"ts_ms\":200,\"labels\":{\"a\":\"2\"},\"value\":2.0}\n\
                    {\"ts_ms\":300,\"labels\":{\"a\":\"3\"},\"value\":3.0}\n\
                    {\"ts_ms\":400,\"labels\":{\"a\":\"4\"},\"valu";
        let out = parse_jsonl(blob.as_bytes(), 0, i64::MAX).expect("torn tail must not error");
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].ts_ms, 100);
        assert_eq!(out[2].ts_ms, 300);
    }

    /// A malformed line in the *middle* of the file is real
    /// corruption, not a concurrent-write torn tail — must still
    /// hard-error so callers can route around the bad part.
    #[test]
    fn parse_jsonl_errors_on_mid_file_corruption() {
        let blob = "{\"ts_ms\":100,\"labels\":{\"a\":\"1\"},\"value\":1.0}\n\
                    not-json\n\
                    {\"ts_ms\":300,\"labels\":{\"a\":\"3\"},\"value\":3.0}\n";
        let err = parse_jsonl(blob.as_bytes(), 0, i64::MAX)
            .expect_err("mid-file corruption must surface as an error");
        match err {
            ColdStoreError::Malformed(msg) => assert!(
                msg.contains("line 2"),
                "expected lineno in error, got: {msg}"
            ),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }
}
