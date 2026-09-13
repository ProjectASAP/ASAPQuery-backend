//! Per-series, per-window scalar results produced by summary execution and
//! adapted by the query engine into `QueryResult`.

use std::collections::BTreeMap;

/// Per-series, per-window scalar results.
///
/// `coverage` is the actual `(min_window_start_ms, max_window_end_ms)`
/// the answer covered. `None` when nothing was observed for the
/// requested window (defensive default). The caller (`ASAPQueryEngine`)
/// compares `coverage` against the requested `[t0, t1]` and, on a
/// partial hit (`cov_lo > t0 || cov_hi < t1`), falls over to archive for
/// the missing range and stitches the two answers.
#[derive(Debug, Clone, Default)]
pub struct ASAPTierResult {
    /// `(label_values, samples)` where `samples` is
    /// `(window_end_unix_ms, value)`.
    pub series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>,
    /// Effective coverage `(min_window_start_ms, max_window_end_ms)`.
    /// Set whenever at least one window was observed; left `None` when
    /// `series` is empty.
    pub coverage: Option<(u64, u64)>,
}

impl ASAPTierResult {
    pub fn is_empty(&self) -> bool {
        self.series.iter().all(|(_, s)| s.is_empty())
    }
}
