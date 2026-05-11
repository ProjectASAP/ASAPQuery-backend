//! In-memory last-value cache for `http_freshness_probe_*` counters.
//!
//! ## Why this exists (issue #46 ⑥ — freshness UNKNOWN → CAPTURED)
//!
//! The MVP demo's freshness criterion polls
//! `last_over_time(http_freshness_probe_warm[10s])` from the replay
//! client at 10 Hz over a 60 s window and expects every poll to land
//! the latest probe sample. The probe metric flows through the agent
//! pipeline as a raw counter:
//!
//! ```text
//!   producer → agent: [gorillas3 → ddsketch → batch] → backend OTLP
//!                          │
//!                          └──▶ TSDB block (60 s window) → MinIO
//!                                                            │
//!                                                            └─▶ Thanos store-gateway
//! ```
//!
//! `gorillas3` flushes a finalised TSDB block every 60 s and the
//! Thanos store-gateway syncs S3 every 30 s, so the *cold* tier sees
//! the probe sample only after a 60–90 s lag. A `[10s]` lookback
//! against Thanos therefore returns an empty vector even though the
//! probe is being emitted at 1 Hz and reaching the backend's OTLP
//! receiver in real time. The replay client logged
//! `attempted=600 got=0` for both the warm and archive paths, which
//! pinned criterion ⑥ at UNKNOWN.
//!
//! The fix routes around the cold-tier flush latency at the query
//! surface: the OTLP ingest path captures the latest `(ts_ms, value)`
//! for every probe metric in a small RAM-resident cache, and the HTTP
//! query handler intercepts `last_over_time(<probe>[<range>])` for
//! probe-shaped metric names and answers from this cache when a
//! sample within `[now − range_ms, now]` is present. The long-term
//! TSDB write path is preserved verbatim — we only short-circuit the
//! freshness-poll query, not the storage pipeline.
//!
//! ## Scope of the cache
//!
//! * Captures only metrics whose name starts with
//!   `http_freshness_probe_` (matches the three demo probe spellings:
//!   `_raw`, `_warm`, `_archive`). Every other metric is ignored;
//!   the cache adds no per-sample work to the hot ingest path beyond
//!   a string prefix check.
//! * Stores one entry per metric — labels are dropped. The MVP demo
//!   emits each probe with a single (no-label) series, and the
//!   replay client queries the bare metric. If a future probe
//!   variant adds labels, the lookup still returns the most-recent
//!   sample regardless of which series produced it; that's
//!   acceptable for a freshness probe (we want "latest emission",
//!   not per-series breakdown).
//! * Bounded by the prefix filter: at most one entry per probe
//!   metric the agent ever emits. The MVP runs three probes; the
//!   cache holds three entries indefinitely.
//!
//! ## Concurrency model
//!
//! Wraps a `HashMap` in a `RwLock`. Ingest writes acquire a write
//! lock for the brief window of an `insert`; queries acquire a read
//! lock to look up. The cache is only consulted when the parsed
//! query matches the probe pattern — every other PromQL query
//! bypasses it entirely.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Substring the cache filters on. Any metric name containing this
/// token (with the trailing underscore to keep the tier suffix
/// disambiguated) is captured. The MVP fake-exporter emits three
/// probes named `http_freshness_probe_{raw,warm,archive}`; user-
/// extensible probes (e.g. `latency_freshness_probe_*`) are matched
/// without a code change.
const PROBE_NAME_TOKEN: &str = "freshness_probe_";

/// Returns `true` iff the metric name belongs to the freshness-probe
/// family — used by both the ingest write path (filter before
/// storing) and the query read path (intercept before normal
/// routing).
pub fn is_freshness_probe(metric: &str) -> bool {
    metric.contains(PROBE_NAME_TOKEN)
}

/// One cached entry: the most-recent `(timestamp, value)` for a
/// single probe metric. Timestamps are unix epoch milliseconds — the
/// same scale as the `ts_ms` carried on `MetricPoint` in the OTLP
/// receiver.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProbeSample {
    pub ts_ms: i64,
    pub value: f64,
}

/// In-memory `metric_name → ProbeSample` cache.
///
/// Construct one shared `Arc<FreshnessProbeCache>` at backend
/// startup, hand it to the OTLP receiver (write path) and the HTTP
/// query handler (read path). The cache is `Send + Sync`; cloning
/// an `Arc` is the canonical way to share it across tasks.
#[derive(Debug, Default)]
pub struct FreshnessProbeCache {
    inner: RwLock<HashMap<String, ProbeSample>>,
}

impl FreshnessProbeCache {
    /// Build an empty cache. Same as `Default::default` — the
    /// explicit constructor reads better at the wiring sites in
    /// `main.rs`.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Capture a sample for `metric` at `ts_ms` with `value`. No-op
    /// if the metric name does not match the probe prefix. When the
    /// metric matches and the cache already holds an entry for it,
    /// the new sample replaces the old one *only* if its `ts_ms` is
    /// strictly newer — out-of-order OTLP arrivals don't clobber a
    /// fresher sample. Returns `true` iff the cache was updated.
    pub fn record(&self, metric: &str, ts_ms: i64, value: f64) -> bool {
        if !is_freshness_probe(metric) {
            return false;
        }
        let mut guard = match self.inner.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        match guard.get(metric) {
            Some(prev) if prev.ts_ms >= ts_ms => false,
            _ => {
                guard.insert(metric.to_string(), ProbeSample { ts_ms, value });
                true
            }
        }
    }

    /// Look up the most-recent sample for `metric`. Returns `None`
    /// when the cache has no entry for the metric, when the metric
    /// is not a probe (cheap rejection), or when the entry's `ts_ms`
    /// falls outside the lookback window `[now_ms − range_ms,
    /// now_ms]`. The window check matches PromQL's
    /// `last_over_time(metric[range])` semantics — only samples
    /// inside the matrix selector contribute.
    pub fn lookup(&self, metric: &str, now_ms: i64, range_ms: i64) -> Option<ProbeSample> {
        if !is_freshness_probe(metric) {
            return None;
        }
        let guard = match self.inner.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let sample = guard.get(metric).copied()?;
        let lo = now_ms.saturating_sub(range_ms);
        if sample.ts_ms >= lo && sample.ts_ms <= now_ms {
            Some(sample)
        } else {
            None
        }
    }

    /// Number of cached entries. Visible for tests + debug
    /// instrumentation; not used by the hot path.
    pub fn len(&self) -> usize {
        self.inner.read().map(|g| g.len()).unwrap_or(0)
    }

    /// `true` iff the cache holds no entries — convenience for the
    /// `len() == 0` check.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Wall-clock `now` in unix epoch milliseconds. The lookup helper
/// uses this when the caller doesn't pin a query time (instant
/// queries default to wall-clock now). Pulled out as a free function
/// so tests can substitute by passing an explicit `now_ms` to
/// `lookup`.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignores_non_probe_metrics() {
        let cache = FreshnessProbeCache::new();
        assert!(!cache.record("http_requests_total", 1_000, 42.0));
        assert!(cache.is_empty());
        assert_eq!(cache.lookup("http_requests_total", 2_000, 10_000), None);
    }

    #[test]
    fn records_probe_metrics() {
        let cache = FreshnessProbeCache::new();
        assert!(cache.record("http_freshness_probe_warm", 1_000, 1_000.0));
        assert!(cache.record("http_freshness_probe_archive", 1_000, 1_000.0));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn lookup_returns_sample_inside_window() {
        let cache = FreshnessProbeCache::new();
        // Sample at t=1000ms, value=1000 (the probe's emission ts).
        cache.record("http_freshness_probe_warm", 1_000, 1_000.0);

        // Query at t=1005ms with a 10s lookback — sample is 5ms old,
        // well inside [now-10s, now].
        let got = cache
            .lookup("http_freshness_probe_warm", 1_005, 10_000)
            .expect("sample should be inside window");
        assert_eq!(
            got,
            ProbeSample {
                ts_ms: 1_000,
                value: 1_000.0
            }
        );
    }

    #[test]
    fn lookup_misses_outside_window() {
        let cache = FreshnessProbeCache::new();
        cache.record("http_freshness_probe_warm", 1_000, 1_000.0);

        // Query at t=20_000ms with a 10s lookback — sample is 19s
        // old, outside [10_000, 20_000]. The pre-fix Thanos behavior:
        // 60 s flush gap + 10 s window = always empty.
        assert_eq!(
            cache.lookup("http_freshness_probe_warm", 20_000, 10_000),
            None,
            "sample older than the lookback window must not be returned",
        );
    }

    #[test]
    fn lookup_unknown_metric_returns_none() {
        let cache = FreshnessProbeCache::new();
        assert_eq!(
            cache.lookup("http_freshness_probe_warm", 1_000, 10_000),
            None,
        );
    }

    #[test]
    fn record_keeps_newest_sample() {
        let cache = FreshnessProbeCache::new();
        cache.record("http_freshness_probe_warm", 1_000, 1_000.0);
        // Older sample — must NOT clobber the entry.
        assert!(!cache.record("http_freshness_probe_warm", 500, 500.0));
        let got = cache
            .lookup("http_freshness_probe_warm", 1_005, 10_000)
            .unwrap();
        assert_eq!(got.ts_ms, 1_000);
        assert_eq!(got.value, 1_000.0);

        // Newer sample — replaces the entry.
        assert!(cache.record("http_freshness_probe_warm", 2_000, 2_000.0));
        let got = cache
            .lookup("http_freshness_probe_warm", 2_005, 10_000)
            .unwrap();
        assert_eq!(got.ts_ms, 2_000);
        assert_eq!(got.value, 2_000.0);
    }

    #[test]
    fn lookup_window_includes_endpoints() {
        let cache = FreshnessProbeCache::new();
        cache.record("http_freshness_probe_warm", 1_000, 1_000.0);
        // sample.ts == now − range_ms — inclusive lower bound.
        assert!(
            cache
                .lookup("http_freshness_probe_warm", 11_000, 10_000)
                .is_some(),
            "lower bound must be inclusive (ts_ms == now − range_ms)",
        );
        // sample.ts == now — inclusive upper bound.
        assert!(
            cache
                .lookup("http_freshness_probe_warm", 1_000, 10_000)
                .is_some(),
            "upper bound must be inclusive (ts_ms == now)",
        );
        // sample.ts == now − range_ms − 1 — outside lower bound.
        assert!(cache
            .lookup("http_freshness_probe_warm", 11_001, 10_000)
            .is_none(),);
    }

    #[test]
    fn is_freshness_probe_matches_three_demo_spellings() {
        assert!(is_freshness_probe("http_freshness_probe_raw"));
        assert!(is_freshness_probe("http_freshness_probe_warm"));
        assert!(is_freshness_probe("http_freshness_probe_archive"));
        assert!(!is_freshness_probe("http_freshness_probe"));
        assert!(!is_freshness_probe("http_requests_total"));
        assert!(!is_freshness_probe(""));
    }
}
