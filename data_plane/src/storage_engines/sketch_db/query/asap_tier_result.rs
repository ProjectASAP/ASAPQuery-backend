//! `ASAPTierResult` — the per-series, per-window scalar-result shape
//! `SummaryExecutor` (via `live_serve.rs`/`l4_readout.rs`) fills in for
//! the engine to adapt into `QueryResult`.
//!
//! This module used to also hold `SketchReducer`, the legacy per-Capability
//! reducer that answered queries directly from decoded sketch bytes before
//! `SummaryExecutor` existed. It's retired: neither it nor
//! `shadow_compare.rs` (the diagnostic comparison that validated
//! `SummaryExecutor` against it) was any more "ground truth" than the
//! `SummaryExecutor` path itself, and keeping a second, independently
//! re-derived answering mechanism around after `SummaryExecutor` became
//! the live default only meant two things could silently disagree with
//! each other. `ASAPTierResult` survives because it's the shared
//! wire-shape both the old reducer and `live_serve.rs` produced —
//! nothing about it is reducer-specific.

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
