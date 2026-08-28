//! `RawSampleReader` — trait + mock implementation for reading raw
//! samples from the exact DB during a [`BackfillJob`] run.
//!
//! Supports the future backfill scope ([`future-storage-and-compression.md`](../../../../../docs/design_docs/future-storage-and-compression.md)).
//! and specifically §10.2's `BackfillSource` dispatch: every
//! concrete source (S3+Gorilla, Prometheus, ClickHouse, OtherSketch)
//! will eventually implement this trait so the worker pool (Phase 5c)
//! and the rebuild logic (Phase 5e) are source-agnostic.
//!
//! ## Phase 5b scope (what this file covers)
//!
//! * `RawSample` struct — the decoded-sample shape used end-to-end
//!   from exact-DB read through sketch replay.
//! * `LabelFilter` struct — the subset of the series selector the
//!   reader needs to apply (metric + grouping label equality).
//!   Deliberately narrow: the full PromQL matcher language isn't
//!   needed for backfill, and a narrow type simplifies every reader
//!   implementation.
//! * `RawSampleReader` trait — async `read_samples(range, filter)`
//!   returning a `Vec<RawSample>`. Plain Vec (not a stream) so the
//!   trait stays object-safe and easy to mock; Phase 5e can revisit
//!   streaming if large ranges become a memory pressure.
//! * `MockRawSampleReader` — in-memory implementation used by
//!   Phase 5c's worker tests and Phase 5e's rebuild tests.
//!
//! ## Out of scope for 5b (future phases)
//!
//! * `PrometheusReader` / `S3GorillaReader` / `ClickHouseReader` —
//!   real network-backed implementations, deferred until Phase 5e
//!   needs them.
//! * Streaming variant returning an `impl Stream<Item = RawSample>`
//!   — Phase 5e decides based on observed memory behaviour.
//! * `OtherSketch` variant lookup (reads from an existing
//!   precompute rather than raw samples) — Phase 5e.

use std::collections::HashMap;

use async_trait::async_trait;

/// A single raw sample read from the exact DB during a backfill.
/// Shape mirrors what the OTLP ingest path emits internally so the
/// downstream sketch builder can consume both native ingest and
/// backfill output through one code path.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    /// Full series key (Prometheus-style `metric{k="v",...}` string)
    /// so the downstream grouping code can extract group-key label
    /// values just as it does for live samples. Phase 5e may switch
    /// to a structured label map if profiling shows string parsing
    /// dominates rebuild cost.
    pub labels: String,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// Narrow subset of PromQL label matchers the backfill reader must
/// honour. Exactly one metric name plus zero or more equality
/// matchers on grouping labels — no regex, no negation, no
/// lexicographic ranges. The control plane picks the subset of
/// `AggregationConfig.grouping_labels` that should gate the read.
///
/// Rationale: every supported exact-DB backend (Prometheus,
/// ClickHouse, S3+Gorilla) can evaluate this filter efficiently,
/// and richer matchers would invite divergence between live and
/// backfill paths.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LabelFilter {
    pub metric: String,
    /// Label-name → expected-value pairs. An empty map selects every
    /// series for `metric`.
    pub equality: HashMap<String, String>,
}

impl LabelFilter {
    pub fn for_metric(metric: impl Into<String>) -> Self {
        Self {
            metric: metric.into(),
            equality: HashMap::new(),
        }
    }

    pub fn with_label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.equality.insert(name.into(), value.into());
        self
    }
}

/// Errors the reader can surface to the worker pool. Kept coarse on
/// purpose — Phase 5c's worker will transition the job to `Failed`
/// with the `Display` text in `BackfillJob::error_message`, so the
/// variants don't need to be programmatically matched.
#[derive(Debug)]
pub enum RawSampleReaderError {
    /// The range is malformed (start > end) or outside the reader's
    /// retention.
    InvalidRange { reason: String },
    /// The exact DB is unreachable or refused the query.
    Upstream { reason: String },
    /// A fatal decode / parse failure on an individual sample.
    Decode { reason: String },
    /// Any other unexpected failure. Phase 5c treats this identically
    /// to `Upstream` for now; kept separate so future readers can
    /// widen their reporting without a breaking change.
    Other { reason: String },
}

impl std::fmt::Display for RawSampleReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRange { reason } => write!(f, "invalid range: {reason}"),
            Self::Upstream { reason } => write!(f, "upstream error: {reason}"),
            Self::Decode { reason } => write!(f, "decode error: {reason}"),
            Self::Other { reason } => write!(f, "error: {reason}"),
        }
    }
}

impl std::error::Error for RawSampleReaderError {}

/// Source-agnostic read API the backfill worker calls against
/// whichever `BackfillSource` the job points at. `async` because
/// every concrete reader (Prometheus HTTP, S3 Gorilla, ClickHouse)
/// is I/O-bound; `async_trait` is used instead of the naked
/// impl-trait-in-trait so the trait stays object-safe and the
/// worker can hold `Arc<dyn RawSampleReader>`.
#[async_trait]
pub trait RawSampleReader: Send + Sync {
    /// Read every sample the exact DB has for `filter` over the
    /// half-open range `[start_ms, end_ms)`. Samples should be
    /// returned in **ingest order** per-series — §10.5 requires
    /// deterministic replay, and the contract is easiest to
    /// satisfy at the reader layer.
    ///
    /// An empty `Vec` means "no samples in the range" (not an
    /// error). Errors are reserved for upstream failures.
    async fn read_samples(
        &self,
        start_ms: u64,
        end_ms: u64,
        filter: &LabelFilter,
    ) -> Result<Vec<RawSample>, RawSampleReaderError>;

    /// Human-readable name used in structured logs and the §15.2
    /// HTTP list endpoint. Defaults to the trait-object's Rust
    /// type name; concrete readers can override for friendlier
    /// output.
    fn source_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// In-memory reader used by unit tests and the Phase 5c worker
/// smoke test. Seeded with a flat `Vec<RawSample>` at construction;
/// `read_samples` applies `(range, filter)` on every call.
///
/// Not for production use — it doesn't scale past a few thousand
/// samples and offers no retention semantics.
pub struct MockRawSampleReader {
    samples: Vec<RawSample>,
}

impl MockRawSampleReader {
    pub fn new(samples: Vec<RawSample>) -> Self {
        Self { samples }
    }

    /// Total number of samples the reader was seeded with —
    /// independent of any range / filter. Exposed so tests can
    /// assert reader setup without round-tripping through
    /// `read_samples`.
    pub fn seeded_count(&self) -> usize {
        self.samples.len()
    }
}

#[async_trait]
impl RawSampleReader for MockRawSampleReader {
    async fn read_samples(
        &self,
        start_ms: u64,
        end_ms: u64,
        filter: &LabelFilter,
    ) -> Result<Vec<RawSample>, RawSampleReaderError> {
        if start_ms > end_ms {
            return Err(RawSampleReaderError::InvalidRange {
                reason: format!("start_ms {start_ms} > end_ms {end_ms}"),
            });
        }
        let out = self
            .samples
            .iter()
            .filter(|s| {
                let ts = s.timestamp_ms;
                ts >= 0 && (ts as u64) >= start_ms && (ts as u64) < end_ms
            })
            .filter(|s| sample_matches(&s.labels, filter))
            .cloned()
            .collect();
        Ok(out)
    }

    fn source_name(&self) -> &'static str {
        "MockRawSampleReader"
    }
}

/// Check whether a series-key string `metric{a="b",c="d"}` satisfies
/// the filter: metric name must match, and every equality-matcher
/// entry must be present with the expected value. Unknown labels
/// on the series are ignored.
fn sample_matches(series_key: &str, filter: &LabelFilter) -> bool {
    let (metric, labels_str) = match series_key.find('{') {
        Some(i) => (
            &series_key[..i],
            series_key[i + 1..]
                .strip_suffix('}')
                .unwrap_or(&series_key[i + 1..]),
        ),
        None => (series_key, ""),
    };
    if metric != filter.metric {
        return false;
    }
    if filter.equality.is_empty() {
        return true;
    }
    // Parse `a="b",c="d"` lazily. Don't unescape — the filter values
    // must match the raw label strings the producer emitted. If a
    // series has fewer labels than the filter asks for, it can't
    // satisfy all equality clauses and we reject.
    let mut found = HashMap::new();
    for pair in labels_str.split(',').filter(|s| !s.is_empty()) {
        let mut it = pair.splitn(2, '=');
        let (Some(k), Some(v)) = (it.next(), it.next()) else {
            continue;
        };
        let k = k.trim();
        let v = v.trim().trim_matches('"');
        found.insert(k.to_string(), v.to_string());
    }
    filter
        .equality
        .iter()
        .all(|(k, v)| found.get(k).map(|fv| fv == v).unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(labels: &str, ts: i64, v: f64) -> RawSample {
        RawSample {
            labels: labels.to_string(),
            timestamp_ms: ts,
            value: v,
        }
    }

    #[tokio::test]
    async fn read_samples_returns_everything_in_range() {
        let r = MockRawSampleReader::new(vec![
            s("latency{svc=\"a\"}", 10, 1.0),
            s("latency{svc=\"b\"}", 20, 2.0),
            s("latency{svc=\"a\"}", 30, 3.0),
        ]);
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("latency"))
            .await
            .unwrap();
        assert_eq!(out.len(), 3);
    }

    #[tokio::test]
    async fn read_samples_enforces_half_open_range() {
        let r = MockRawSampleReader::new(vec![
            s("latency", 10, 1.0),
            s("latency", 20, 2.0),
            s("latency", 30, 3.0),
        ]);
        // [10, 30) — excludes ts=30.
        let out = r
            .read_samples(10, 30, &LabelFilter::for_metric("latency"))
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|s| s.timestamp_ms < 30));
    }

    #[tokio::test]
    async fn read_samples_filters_by_metric_name() {
        let r = MockRawSampleReader::new(vec![
            s("latency{svc=\"a\"}", 10, 1.0),
            s("qps{svc=\"a\"}", 20, 2.0),
        ]);
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("latency"))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].labels.starts_with("latency"));
    }

    #[tokio::test]
    async fn read_samples_filters_by_label_equality() {
        let r = MockRawSampleReader::new(vec![
            s("latency{svc=\"a\",env=\"prod\"}", 10, 1.0),
            s("latency{svc=\"b\",env=\"prod\"}", 20, 2.0),
            s("latency{svc=\"a\",env=\"stage\"}", 30, 3.0),
        ]);
        let filter = LabelFilter::for_metric("latency")
            .with_label("svc", "a")
            .with_label("env", "prod");
        let out = r.read_samples(0, 100, &filter).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, 1.0);
    }

    #[tokio::test]
    async fn read_samples_empty_filter_selects_metric_only() {
        let r = MockRawSampleReader::new(vec![
            s("latency{svc=\"a\"}", 10, 1.0),
            s("latency{svc=\"b\"}", 20, 2.0),
            s("qps{svc=\"a\"}", 30, 3.0),
        ]);
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("latency"))
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
    }

    #[tokio::test]
    async fn read_samples_label_matcher_requires_all_to_match() {
        let r = MockRawSampleReader::new(vec![s("latency{svc=\"a\"}", 10, 1.0)]);
        let filter = LabelFilter::for_metric("latency")
            .with_label("svc", "a")
            .with_label("env", "prod"); // not present
        let out = r.read_samples(0, 100, &filter).await.unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn read_samples_metric_without_labels_matches_bare_metric_filter() {
        let r = MockRawSampleReader::new(vec![s("bare_metric", 10, 1.0)]);
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("bare_metric"))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn read_samples_rejects_inverted_range() {
        let r = MockRawSampleReader::new(vec![]);
        let err = r
            .read_samples(100, 50, &LabelFilter::for_metric("m"))
            .await
            .unwrap_err();
        match err {
            RawSampleReaderError::InvalidRange { .. } => {}
            other => panic!("expected InvalidRange, got {other}"),
        }
    }

    #[tokio::test]
    async fn read_samples_returns_empty_when_no_overlap() {
        let r = MockRawSampleReader::new(vec![s("latency", 10, 1.0), s("latency", 20, 2.0)]);
        let out = r
            .read_samples(100, 200, &LabelFilter::for_metric("latency"))
            .await
            .unwrap();
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn read_samples_preserves_ingest_order() {
        // §10.5 requires deterministic replay. The mock preserves
        // insertion order on construction; verify the contract.
        let r = MockRawSampleReader::new(vec![s("m", 10, 1.0), s("m", 10, 2.0), s("m", 20, 3.0)]);
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap();
        assert_eq!(
            out.iter().map(|s| s.value).collect::<Vec<_>>(),
            vec![1.0, 2.0, 3.0]
        );
    }

    #[tokio::test]
    async fn reader_is_dyn_compatible() {
        // Make sure the trait can live behind a trait object — the
        // worker pool in Phase 5c will hold Arc<dyn RawSampleReader>.
        let r: std::sync::Arc<dyn RawSampleReader> =
            std::sync::Arc::new(MockRawSampleReader::new(vec![s("m", 10, 1.0)]));
        let out = r
            .read_samples(0, 100, &LabelFilter::for_metric("m"))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(r.source_name(), "MockRawSampleReader");
    }

    #[test]
    fn seeded_count_reports_total() {
        let r = MockRawSampleReader::new(vec![s("m", 10, 1.0), s("m", 20, 2.0), s("m", 30, 3.0)]);
        assert_eq!(r.seeded_count(), 3);
    }

    #[test]
    fn label_filter_builder_accumulates_equalities() {
        let f = LabelFilter::for_metric("latency")
            .with_label("svc", "a")
            .with_label("env", "prod");
        assert_eq!(f.metric, "latency");
        assert_eq!(f.equality.len(), 2);
        assert_eq!(f.equality.get("svc"), Some(&"a".to_string()));
    }

    #[test]
    fn error_display_is_human_readable() {
        let e = RawSampleReaderError::Upstream {
            reason: "conn refused".to_string(),
        };
        let s = format!("{e}");
        assert!(s.contains("upstream"));
        assert!(s.contains("conn refused"));
    }
}
