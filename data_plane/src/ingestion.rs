//! Ingestion: stores raw samples and refreshes the plan's materialized
//! views at ingestion time, by refresh group, one ingestion run per
//! refresh window.
//!
//! The plan lists its views refreshed at ingestion by refresh group
//! (`InstallablePlan::ingestion_refresh_groups`): the views whose windows
//! have one width `W` and end at one phase `p` modulo `W`. A group's window
//! `(e - W, e]` (`e ≡ p mod W`) closes when the watermark (the newest
//! sample time minus the allowed lateness, or a producer's barrier) reaches
//! `e`; the engine then runs the group's views in one ingestion run over
//! that window and appends each view's rows to the store. A window with no
//! sample is refreshed too, so its rows are stored as empty, not missing.
//!
//! The first window a group sees may have started before the data plane
//! did, so it is never refreshed and queries that read it are not
//! answered, unless the producer declares where its stream starts
//! (`stream_start_ms`): a window starting at or after it is complete.
//! When the watermark jumps past a view's retention, the windows that would
//! already be dropped are skipped.
//!
//! Raw samples are kept while a window still to be closed or a query-time
//! `Scan` (back to its `retention_ms`) reads them.
//!
//! [`Ingestion`] (the ingest coordinator) keeps the watermark and closes
//! windows, and the ingest shards store the samples ([`Routing`]); each
//! closed window is refreshed by [`Precompute`] (the precompute pool) over
//! the raw data as it was when the window closed, so the samples it reads
//! are those a serial engine would read.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use asap_executor::physical_planner::{compile, CompiledPhysicalDAG};
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_executor::planner::plan::materialized_view::{
    IngestionRefreshGroup, MaterializedView, MaterializedViewRefresh,
};
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef};
use futures::executor::block_on;

use crate::prometheus::MAX_TIME_MS;
use crate::store::{RawData, RawSample, RawShards, RunStore, SeriesPartition, ViewStore};

/// How ingestion treats sample times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestionOptions {
    /// A window closes when the newest routed sample is this far past its end.
    pub allowed_lateness_ms: u64,
    /// Samples further ahead of the wall clock than this are dropped.
    pub max_future_ms: u64,
    /// Where the producer's stream starts: every raw sample at or after it
    /// reaches the data plane, so a group's first window that starts at or
    /// after it is complete and is refreshed. `None`: the first window may
    /// have started before the data plane did, so it is never refreshed.
    pub stream_start_ms: Option<i64>,
    /// How much longer than its `retention_ms` a view's rows, and a
    /// query-time `Scan`'s raw samples, are kept. The plan's retention
    /// counts back from the newest evaluation, but ingestion runs ahead of
    /// the queries it answers: a query at `T` reads windows ending up to
    /// `retention_ms` before `T` while newer windows have already been
    /// refreshed. With this margin a query up to `view_retention_margin_ms`
    /// behind the newest refreshed window is still answered.
    pub view_retention_margin_ms: u64,
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            allowed_lateness_ms: 15_000,
            max_future_ms: 5 * 60_000,
            stream_start_ms: None,
            view_retention_margin_ms: 60_000,
        }
    }
}

/// Counters of what happened to samples (each counted once) and to the
/// refresh windows of each group (each counted once).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct IngestionStats {
    /// Samples stored as raw data.
    pub samples_accepted: u64,
    /// Samples older than every window still to be refreshed and every
    /// query-time `Scan`'s retention.
    pub samples_dropped_late: u64,
    /// Samples more than `max_future_ms` ahead of the wall clock.
    pub samples_dropped_future: u64,
    /// Samples of a metric no `Scan` reads.
    pub samples_unrouted: u64,
    /// Ingestion runs that refreshed a group's window and stored its rows.
    pub windows_refreshed: u64,
    /// Ingestion runs that failed; their window is not stored.
    pub windows_failed: u64,
    /// First windows not refreshed because they may have started before the
    /// data plane did.
    pub windows_skipped_first: u64,
    /// Windows not refreshed because they would already be dropped.
    pub windows_skipped_expired: u64,
    /// Windows not refreshed because every view of the group already
    /// stores them (loaded from the view files at startup).
    pub windows_already_stored: u64,
}

/// Why a plan cannot be ingested.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("{0}")]
    Unsupported(String),
    #[error("refresh group of views {views:?}: {error}")]
    Compile {
        views: Vec<PhysicalASAPNodeId>,
        error: asap_executor::Error,
    },
}

/// Raw-data panes are never wider than this, so eviction frees memory at
/// least this often.
const MAX_PANE_WIDTH_MS: i64 = 60_000;

/// A refresh group's window `(end_ms - W, end_ms]` that the watermark has
/// passed, to refresh; `group` indexes the plan's ingestion refresh groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedWindow {
    pub group: usize,
    pub end_ms: i64,
}

/// One refresh group, compiled once: what its ingestion runs execute.
struct RefreshGroupRun {
    /// The group's views, with `retention_ms` extended by
    /// `view_retention_margin_ms`: how long the store keeps their rows.
    views: Vec<MaterializedView>,
    compiled: CompiledPhysicalDAG,
    width_ms: i64,
    /// The views of other groups its runs read.
    reads: Vec<PhysicalASAPNodeId>,
    /// For each of its views, the groups that read it.
    readers: Vec<Vec<usize>>,
}

/// One refresh group's windows.
struct RefreshGroup {
    width_ms: i64,
    phase_ms: i64,
    /// The metrics its runs scan: their raw samples it needs kept.
    metrics: BTreeSet<String>,
    /// The groups whose views its runs read.
    reads: Vec<usize>,
    /// The metrics whose first sample starts it: those its runs scan and
    /// those scanned below the views it reads. A wider level of a window
    /// chain (`SummaryMergeWindows`) scans nothing; it starts with the
    /// chain it is built from.
    started_by: BTreeSet<String>,
    /// The longest retention of its views.
    retention_ms: i64,
    /// End of the newest window closed or skipped; `None` before the
    /// first sample of a metric it scans.
    refreshed_through: Option<i64>,
}

impl RefreshGroup {
    /// The end of the group's window holding `timestamp_ms`.
    fn window_end(&self, timestamp_ms: i64) -> i64 {
        window_end(timestamp_ms - self.phase_ms, self.width_ms) + self.phase_ms
    }
}

pub struct Ingestion {
    groups: Vec<RefreshGroup>,
    /// Every metric a `Scan` reads; samples of others are not stored.
    scanned_metrics: Arc<BTreeSet<String>>,
    /// Raw-data pane width: the gcd of every group's window width and
    /// phase (and of `MAX_PANE_WIDTH_MS`), so panes tile every window.
    pane_width_ms: i64,
    /// For each metric a query-time `Scan` reads, how far back it reads
    /// (`None`: every sample).
    query_time_lookback_ms: HashMap<String, Option<i64>>,
    options: IngestionOptions,
    newest_sample_ms: Option<i64>,
    /// The newest wall-clock time an ingest ran at: the watermark never
    /// passes it, so a series stamped ahead of the others cannot close
    /// windows early.
    newest_clock_ms: Option<i64>,
    /// The newest event-time barrier a producer declared.
    barrier_ms: Option<i64>,
    pub stats: IngestionStats,
}

impl Ingestion {
    /// Compiles one ingestion run per refresh group of `dag`'s `views`,
    /// for the [`Precompute`] returned with the ingestion. Refuses views
    /// refreshed on first read, which the data plane does not refresh yet,
    /// and groups without a bounded window.
    pub fn new(
        dag: &PhysicalASAPDAG,
        views: &[MaterializedView],
        groups: Vec<IngestionRefreshGroup>,
        options: IngestionOptions,
        limits: Limits,
    ) -> Result<(Self, Precompute), IngestionError> {
        if let Some(view) = views
            .iter()
            .find(|v| v.refresh() == MaterializedViewRefresh::OnFirstRead)
        {
            return Err(IngestionError::Unsupported(format!(
                "materialized view {} is refreshed on first read, which the data plane does not do yet",
                view.node_id
            )));
        }
        let index = DagIndex::new(dag);
        let group_of: HashMap<PhysicalASAPNodeId, usize> = groups
            .iter()
            .enumerate()
            .flat_map(|(index, group)| group.views.iter().map(move |v| (v.node_id, index)))
            .collect();
        let mut runs = Vec::new();
        let mut clocks = Vec::new();
        let mut tasks = RefreshTaskGraph::default();
        for group in groups {
            let ids: Vec<PhysicalASAPNodeId> = group.views.iter().map(|v| v.node_id).collect();
            let Some(window) = group.refresh_window else {
                return Err(IngestionError::Unsupported(format!(
                    "views {ids:?} are refreshed over an unbounded window"
                )));
            };
            // The views of other groups it reads are view slots.
            let in_group: HashSet<PhysicalASAPNodeId> = ids.iter().copied().collect();
            let read: BTreeSet<PhysicalASAPNodeId> = group
                .views
                .iter()
                .flat_map(|v| v.inputs.materialized_views.iter().copied())
                .filter(|id| !in_group.contains(id))
                .collect();
            let read_views: Vec<MaterializedView> = views
                .iter()
                .filter(|v| read.contains(&v.node_id))
                .cloned()
                .collect();
            let read_groups: BTreeSet<usize> = read
                .iter()
                .filter_map(|id| group_of.get(id).copied())
                .collect();
            tasks.reads.push(read_groups.into_iter().collect());
            let stop: HashSet<PhysicalASAPNodeId> = read.iter().copied().collect();
            let below = index.below(&ids, &stop);
            tasks.serial.push(
                below
                    .iter()
                    .any(|id| carries_state_across_windows(&index.nodes[id].payload)),
            );
            let by_series = read.is_empty()
                && below
                    .iter()
                    .all(|id| computes_each_series_alone(&index.nodes[id].payload));
            tracing::info!(
                views = ?ids,
                split_by_series = by_series,
                "refresh group: a window's run {}",
                if by_series {
                    "is split by series over the precompute threads"
                } else {
                    "runs whole: some node combines series, or reads other views"
                }
            );
            tasks.split_by_series.push(by_series);
            let roots: Vec<u64> = ids.iter().map(|&id| id as u64).collect();
            let compiled =
                compile(dag, &read_views, &roots).map_err(|error| IngestionError::Compile {
                    views: ids.clone(),
                    error,
                })?;
            let metrics = index.metrics_below(&ids, &read);
            let started_by = index.metrics_below(&ids, &BTreeSet::new());
            let margin_ms = options.view_retention_margin_ms;
            let views = group
                .views
                .iter()
                .map(|view| with_retention_margin(view, margin_ms))
                .collect::<Result<Vec<_>, _>>()?;
            let retention_ms = views
                .iter()
                .map(|v| millis(v.retention_ms()))
                .max()
                .unwrap_or(0);
            runs.push(RefreshGroupRun {
                views,
                compiled,
                width_ms: millis(window.width_ms),
                reads: read.iter().copied().collect(),
                readers: Vec::new(),
            });
            clocks.push(RefreshGroup {
                width_ms: millis(window.width_ms),
                phase_ms: millis(window.phase_ms),
                metrics,
                reads: tasks.reads.last().cloned().unwrap_or_default(),
                started_by,
                retention_ms,
                refreshed_through: None,
            });
        }
        let pane_width_ms = clocks
            .iter()
            .flat_map(|g| [g.width_ms, g.phase_ms])
            .fold(MAX_PANE_WIDTH_MS, gcd);
        // The `Scan`s the query run reads: below the roots, outside views.
        let view_ids: HashSet<PhysicalASAPNodeId> = views.iter().map(|v| v.node_id).collect();
        let mut query_time_lookback_ms: HashMap<String, Option<i64>> = HashMap::new();
        for scan in index.below(&dag.roots, &view_ids) {
            let node = index.nodes[&scan];
            if let Operator::NonASAP(NonASAPOp::Scan {
                source: Source::TimeSeries { metric },
                ..
            }) = &node.payload
            {
                let margin_ms = millis(options.view_retention_margin_ms);
                let lookback = node
                    .retention_ms
                    .map(|retention| millis(retention).saturating_add(margin_ms));
                let entry = query_time_lookback_ms
                    .entry(metric.clone())
                    .or_insert(Some(0));
                *entry = match (*entry, lookback) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                };
            }
        }
        let ingestion = Self {
            groups: clocks,
            scanned_metrics: Arc::new(crate::store::scanned_metrics(&dag.nodes)),
            pane_width_ms,
            query_time_lookback_ms,
            options,
            newest_sample_ms: None,
            newest_clock_ms: None,
            barrier_ms: None,
            stats: IngestionStats::default(),
        };
        // Each view's readers: the groups whose runs read it.
        let mut readers: HashMap<PhysicalASAPNodeId, Vec<usize>> = HashMap::new();
        for (reader, run) in runs.iter().enumerate() {
            for &view in &run.reads {
                readers.entry(view).or_default().push(reader);
            }
        }
        for run in &mut runs {
            run.readers = run
                .views
                .iter()
                .map(|view| readers.get(&view.node_id).cloned().unwrap_or_default())
                .collect();
        }
        Ok((
            ingestion,
            Precompute {
                runs,
                tasks,
                limits,
            },
        ))
    }

    /// After a restart that loaded views from files: the raw samples before
    /// the restart are gone, so a declared stream start no longer makes a
    /// group's first window complete; it is skipped, as without one.
    pub fn restarted(&mut self) {
        self.options.stream_start_ms = None;
    }

    /// An empty raw store for the metrics the plan scans, in this
    /// ingestion's panes.
    pub fn raw_data(&self) -> RawData {
        RawData::for_metrics(self.scanned_metrics.iter(), self.pane_width_ms)
    }

    /// The raw-data pane width.
    pub fn pane_width_ms(&self) -> i64 {
        self.pane_width_ms
    }

    /// The watermark: no sample at or before this is expected any more, so
    /// a window ending at or before it closes. From samples, one
    /// millisecond before the newest routed sample (capped at the wall
    /// clock) less the allowed lateness: other series' samples at that very
    /// time may still come in a later request, as a scraper sends one
    /// timestamp over several requests, and a window ending there waits
    /// for them. A barrier states every sample at or before it was written,
    /// so it counts whole when later. (Closing a window only once event
    /// time is past its end follows the ASAPQuery precompute engine's
    /// window manager, `closed_windows`.)
    pub fn watermark_ms(&self) -> Option<i64> {
        let newest = match self.newest_clock_ms {
            Some(clock) => self.newest_sample_ms.map(|newest| newest.min(clock)),
            None => self.newest_sample_ms,
        };
        let lateness = millis(self.options.allowed_lateness_ms);
        let from_samples = newest.map(|newest| newest.saturating_sub(lateness).saturating_sub(1));
        from_samples.max(self.barrier_ms)
    }

    /// What the ingest shards need to route samples now: the metrics a
    /// `Scan` reads, each one's eviction cutoff and the time at or before
    /// which its samples are late, and whether a group still waits for its
    /// first sample.
    ///
    /// A metric a query-time `Scan` reads is late at or before the
    /// watermark: an answer at a time the watermark has passed is final
    /// (kept in the result cache and the query-time results), so a sample
    /// it did not see must not change what a later run answers. Any other
    /// metric is late at or before its eviction cutoff (only windows still
    /// to be closed read it).
    pub fn routing(&self) -> Routing {
        let cutoffs: HashMap<String, Option<i64>> = self
            .scanned_metrics
            .iter()
            .map(|metric| (metric.clone(), self.oldest_kept_ms(metric)))
            .collect();
        let watermark_ms = self.watermark_ms();
        let late_through = cutoffs
            .iter()
            .map(|(metric, &cutoff)| {
                let late = if self.query_time_lookback_ms.contains_key(metric) {
                    cutoff.max(watermark_ms)
                } else {
                    cutoff
                };
                (metric.clone(), late)
            })
            .collect();
        Routing {
            scanned_metrics: self.scanned_metrics.clone(),
            max_future_ms: millis(self.options.max_future_ms),
            cutoffs,
            late_through,
            groups_waiting: self.groups.iter().any(|g| g.refreshed_through.is_none()),
        }
    }

    /// Takes in what a shard routed: each metric's first sample starts
    /// the groups scanning it.
    pub fn observe(&mut self, first_seen: &[(Arc<str>, i64)]) {
        for (metric, timestamp_ms) in first_seen {
            self.start_groups(metric, *timestamp_ms);
        }
    }

    /// Sets the wall clock the watermark is capped at (it never goes
    /// back): the time every shard was last flushed. A request admitted
    /// after that time holds samples stamped after it, and a request
    /// admitted before it was run by every shard.
    pub fn advance_clock(&mut self, now_ms: i64) {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
    }

    /// The newest sample time the watermark follows.
    pub fn newest_sample_ms(&self) -> Option<i64> {
        self.newest_sample_ms
    }

    /// Sets the newest sample time the watermark follows (it never goes
    /// back): the newest sample any shard ran by the last flush.
    pub fn advance_newest_sample(&mut self, newest_ms: Option<i64>) {
        self.newest_sample_ms = self.newest_sample_ms.max(newest_ms);
    }

    /// Closes and returns every window the watermark has passed, oldest
    /// first within each group, groups in plan order (a group may read the
    /// views of an earlier one).
    pub fn close(&mut self) -> Vec<ClosedWindow> {
        self.close_windows()
    }

    /// [`Self::advance_watermark_at`] the current wall-clock time.
    pub fn advance_watermark(&mut self, barrier_ms: i64) -> (i64, Vec<ClosedWindow>) {
        self.advance_watermark_at(barrier_ms, wall_clock_ms())
    }

    /// An event-time barrier: the producer states that every sample at or
    /// before `barrier_ms` has been written, so every window ending at or
    /// before it closes without a later sample. Capped at `now_ms`; a
    /// repeated or lower barrier does nothing. Returns the barrier in force
    /// and the windows it closed, as [`Self::close`] does.
    pub fn advance_watermark_at(
        &mut self,
        barrier_ms: i64,
        now_ms: i64,
    ) -> (i64, Vec<ClosedWindow>) {
        // The barrier is capped at the wall clock, but does not move the
        // clock the samples' watermark is capped at: only a flush does.
        let barrier_ms = barrier_ms.min(now_ms);
        if let Some(current) = self.barrier_ms.filter(|current| *current >= barrier_ms) {
            return (current, Vec::new());
        }
        self.barrier_ms = Some(barrier_ms);
        (barrier_ms, self.close_windows())
    }

    /// Starts each group scanning `metric` at its first sample: its first
    /// window is skipped unless it starts at or after the declared stream
    /// start.
    fn start_groups(&mut self, metric: &str, timestamp_ms: i64) {
        for group in &mut self.groups {
            if group.refreshed_through.is_some() || !group.started_by.contains(metric) {
                continue;
            }
            let first_end = group.window_end(timestamp_ms);
            group.refreshed_through = Some(match self.options.stream_start_ms {
                // The first window starting at or after the stream start.
                Some(start) => {
                    group.window_end(start.saturating_add(group.width_ms)) - group.width_ms
                }
                None => {
                    self.stats.windows_skipped_first += 1;
                    first_end
                }
            });
        }
    }

    /// Samples of `metric` at or before this are no longer read: older than
    /// every window still to be refreshed and than what a query-time `Scan`
    /// reads back from the watermark (less the allowed lateness, for
    /// evaluations up to the newest sample). `None` while all are kept.
    fn oldest_kept_ms(&self, metric: &str) -> Option<i64> {
        let mut cutoff: Option<i64> = None;
        let mut lower = |bound: Option<i64>| {
            cutoff = Some(match (cutoff, bound) {
                (_, None) => return false,
                (None, Some(b)) => b,
                (Some(a), Some(b)) => a.min(b),
            });
            true
        };
        for group in self.groups.iter().filter(|g| g.metrics.contains(metric)) {
            // A group not started yet needs every sample.
            if !lower(group.refreshed_through) {
                return None;
            }
        }
        if let Some(lookback) = self.query_time_lookback_ms.get(metric) {
            let watermark = self.watermark_ms()?;
            let lateness = millis(self.options.allowed_lateness_ms);
            if !lower(lookback.map(|l| watermark.saturating_sub(l).saturating_sub(lateness))) {
                return None;
            }
        }
        cutoff
    }

    /// Closes every window the watermark has passed and compacts the
    /// panes it has passed.
    fn close_windows(&mut self) -> Vec<ClosedWindow> {
        let Some(watermark) = self.watermark_ms() else {
            return Vec::new();
        };
        // Per group, the windows ending at or before this would already be
        // dropped (they are past its retention). A group whose views
        // another group reads (a narrower level of a window chain) keeps
        // building the windows its readers still need: it skips at most to
        // the latest of its window boundaries at or before where each
        // reader skips to. Readers come after the groups they read, so in
        // reverse plan order every reader's bound is known first.
        let mut expired: Vec<i64> = self
            .groups
            .iter()
            .map(|group| {
                group.window_end(watermark.saturating_sub(group.retention_ms)) - group.width_ms
            })
            .collect();
        for reader in (0..self.groups.len()).rev() {
            for &read in &self.groups[reader].reads {
                let group = &self.groups[read];
                // The latest boundary of `group` at or before the reader's.
                let needed = group.window_end(expired[reader] - group.width_ms + 1);
                expired[read] = expired[read].min(needed);
            }
        }
        let mut closed = Vec::new();
        // Per group, how far this pass skipped its expired windows.
        let mut skipped_to: Vec<Option<i64>> = vec![None; self.groups.len()];
        for (index, group) in self.groups.iter_mut().enumerate() {
            let Some(mut through) = group.refreshed_through else {
                continue;
            };
            let mut expired_through = expired[index];
            // A group reading the views of groups that skipped further
            // skips at least as far, to its next window boundary: its
            // windows before that would read windows never built.
            for read in &group.reads {
                if let Some(skipped) = skipped_to[*read] {
                    expired_through = expired_through.max(group.window_end(skipped));
                }
            }
            if through < expired_through {
                skipped_to[index] = Some(expired_through);
                self.stats.windows_skipped_expired +=
                    ((expired_through - through) / group.width_ms) as u64;
                through = expired_through;
            }
            while let Some(end) = through
                .checked_add(group.width_ms)
                .filter(|e| *e <= watermark)
            {
                closed.push(ClosedWindow {
                    group: index,
                    end_ms: end,
                });
                through = end;
            }
            group.refreshed_through = Some(through);
        }
        closed
    }
}

/// What an ingest shard routes samples by: a snapshot of the ingestion's
/// state, published after every change.
#[derive(Debug, Clone, Default)]
pub struct Routing {
    scanned_metrics: Arc<BTreeSet<String>>,
    max_future_ms: i64,
    /// Each scanned metric's eviction cutoff (`None`: all are kept).
    cutoffs: HashMap<String, Option<i64>>,
    /// Each scanned metric's samples at or before this are late (`None`:
    /// none are); at or after its eviction cutoff.
    late_through: HashMap<String, Option<i64>>,
    /// Some group has not seen its first sample: report each metric's
    /// first sample.
    groups_waiting: bool,
}

/// One batch of samples routed by a shard.
#[derive(Debug, Default)]
pub struct Routed {
    pub accepted: Vec<RawSample>,
    /// The sample counters of the batch.
    pub stats: IngestionStats,
    /// The newest accepted sample.
    pub newest_ms: Option<i64>,
    /// Each metric's first routed sample, while a group waits for one.
    pub first_seen: Vec<(Arc<str>, i64)>,
}

impl Routing {
    /// Routes `samples` at `now_ms`: drops samples of metrics no `Scan`
    /// reads, samples more than `max_future_ms` ahead of `now_ms` or
    /// further from the epoch than [`MAX_TIME_MS`], and late samples (at
    /// or before their metric's `late_through`).
    pub fn route(&self, samples: Vec<RawSample>, now_ms: i64) -> Routed {
        let latest_allowed_ms = now_ms.saturating_add(self.max_future_ms);
        let mut routed = Routed {
            accepted: Vec::with_capacity(samples.len()),
            ..Routed::default()
        };
        // Samples of one metric usually come together: the last metric is
        // checked by pointer first.
        let mut metrics: Vec<(Arc<str>, bool, Option<i64>, bool)> = Vec::new();
        let mut last: Option<usize> = None;
        for sample in samples {
            let metric = &sample.labels.metric;
            let known = last
                .filter(|&i| Arc::ptr_eq(&metrics[i].0, metric))
                .or_else(|| metrics.iter().position(|(m, ..)| m == metric));
            let index = match known {
                Some(index) => index,
                None => {
                    let routed = self.scanned_metrics.contains(&**metric);
                    let late_through = self.late_through.get(&**metric).copied().flatten();
                    metrics.push((metric.clone(), routed, late_through, false));
                    metrics.len() - 1
                }
            };
            last = Some(index);
            let (_, is_routed, late_through, seen) = &mut metrics[index];
            if !*is_routed {
                routed.stats.samples_unrouted += 1;
                continue;
            }
            // Beyond the times the data plane evaluates, window arithmetic
            // could overflow.
            if sample.timestamp_ms > latest_allowed_ms.min(MAX_TIME_MS) {
                routed.stats.samples_dropped_future += 1;
                continue;
            }
            if sample.timestamp_ms < -MAX_TIME_MS {
                routed.stats.samples_dropped_late += 1;
                continue;
            }
            if self.groups_waiting && !*seen {
                *seen = true;
                routed
                    .first_seen
                    .push((metric.clone(), sample.timestamp_ms));
            }
            if late_through.is_some_and(|late| sample.timestamp_ms <= late) {
                routed.stats.samples_dropped_late += 1;
                continue;
            }
            routed.newest_ms = routed.newest_ms.max(Some(sample.timestamp_ms));
            routed.accepted.push(sample);
            routed.stats.samples_accepted += 1;
        }
        routed
    }

    /// Seals and evicts `raw_data` as the ingestion stands: compacts the
    /// panes the watermark has passed and drops the panes at or before
    /// each metric's cutoff.
    pub fn maintain(&self, raw_data: &mut RawData, watermark_ms: Option<i64>) {
        if let Some(watermark_ms) = watermark_ms {
            raw_data.seal_through(watermark_ms);
        }
        for (metric, cutoff) in &self.cutoffs {
            if let Some(cutoff) = cutoff {
                raw_data.evict_through(metric, *cutoff);
            }
        }
    }
}

/// What a refresh did.
#[derive(Debug)]
pub enum Refreshed {
    /// Ran the window and stored its rows: each view appended to, with its
    /// rows, for the view files.
    Ran(Vec<(PhysicalASAPNodeId, Vec<Batch>)>),
    /// Every view of the group already stores the window.
    AlreadyStored,
}

/// A window's ingestion run, computed and not yet stored.
#[derive(Debug)]
pub enum Computed {
    /// The rows of each view of the group that did not store the window
    /// yet, by its index in the group.
    Ran(Vec<(usize, Vec<Batch>)>),
    /// Every view of the group already stores the window.
    AlreadyStored,
}

/// Which refresh windows may be computed at once, decided when the plan
/// is installed.
///
/// A window's task (group `G`, window `(e - W, e]`) is a pure function of
/// immutable inputs: the raw data as it was when the window closed (every
/// ingest shard flushed up to `e`, panes shared by `Arc` and never changed
/// in place, including the earlier panes a `Scan`'s lookback reads), and
/// the committed views of the groups `G` reads. Its output, `G`'s views for
/// that window, is disjoint from every other task's and invisible until
/// committed. An `execute_ingestion` run starts with fresh state, so no
/// state passes from one window to the next. Hence tasks run in parallel,
/// with two orders kept:
///
/// - `(H, windows covering (e - W, e])` before `(G, (e - W, e])` when `G`
///   reads `H`'s views (`reads`);
/// - a group's windows commit in window order (the store is append-only).
///
/// A group whose runs contain an operator that may carry state from one
/// window to the next (`serial`) computes one window at a time, in order;
/// other groups stay parallel.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefreshTaskGraph {
    /// For each refresh group, the other groups whose views its runs read.
    pub reads: Vec<Vec<usize>>,
    /// For each refresh group, whether its windows must be computed one at
    /// a time.
    pub serial: Vec<bool>,
    /// For each refresh group, whether a window's run may be split into
    /// runs over disjoint sets of series whose rows, merged, are the whole
    /// run's: every node computes each series alone and no other group's
    /// views are read.
    pub split_by_series: Vec<bool>,
}

/// Whether `operator` computes each series' rows from that series' input
/// rows alone, so a run over a subset of the series yields exactly those
/// series' rows. Conservative: a raw `Scan` that keeps the series
/// identity, a row filter, and summaries that never merge entities
/// (`Reduction::PerEntity`); anything else may combine series.
fn computes_each_series_alone<C>(operator: &Operator<C>) -> bool {
    use asap_executor::planner::ir::operator::arguments::Reduction;
    use asap_executor::planner::ir::schema::PROMQL_SERIES_IDENTITY;
    use asap_executor::planner::ir::ASAPOp;
    match operator {
        Operator::NonASAP(NonASAPOp::Scan {
            source: Source::TimeSeries { .. },
            schema,
            ..
        }) => schema
            .fields
            .iter()
            .any(|field| field.name == PROMQL_SERIES_IDENTITY),
        Operator::NonASAP(NonASAPOp::Filter { .. }) => true,
        Operator::ASAP(
            ASAPOp::SummaryAgg { reduction, .. }
            | ASAPOp::SummaryTumblingWindowChain { reduction, .. },
        ) => *reduction == Reduction::PerEntity,
        _ => false,
    }
}

/// Whether `operator` may carry state from one window's run to the next.
/// Conservative: only operators known to compute from the run's own
/// inputs are not; any operator added later is, until classified.
fn carries_state_across_windows<C>(operator: &Operator<C>) -> bool {
    use asap_executor::planner::ir::ASAPOp;
    match operator {
        Operator::NonASAP(_) => false,
        Operator::ASAP(
            ASAPOp::SummaryAgg { .. }
            | ASAPOp::SummaryEstimate { .. }
            | ASAPOp::FinalizeExactAccumulator { .. }
            | ASAPOp::EvaluatePopulation { .. }
            | ASAPOp::SummaryMerge { .. }
            | ASAPOp::SummaryTumblingWindowChain { .. },
        ) => false,
        // `MaintainPopulation` maintains membership over time; the others
        // are reserved and not implemented.
        _ => true,
    }
}

/// The ingestion runs of the plan's refresh groups.
pub struct Precompute {
    runs: Vec<RefreshGroupRun>,
    tasks: RefreshTaskGraph,
    limits: Limits,
}

impl Precompute {
    /// Runs `window`'s group's views in one ingestion run over
    /// `(end - W, end]`, reading `raw_data` (as it was when the window
    /// closed) and `views`, and appends each view's rows to `views`. A view
    /// that already stores the window (loaded from files at startup) is not
    /// appended to again; a window every view of the group stores is not
    /// run.
    pub fn refresh(
        &self,
        window: ClosedWindow,
        raw_data: RawShards,
        views: &mut Arc<ViewStore>,
    ) -> Result<Refreshed, asap_executor::Error> {
        let computed = self.compute(window, raw_data, views.clone(), None)?;
        self.commit(window, computed, views)
    }

    /// The first half of [`Self::refresh`]: runs the window over
    /// `raw_data` and `views` without storing its rows. Windows of one
    /// group may be computed at once on several threads ([`RefreshTaskGraph`]);
    /// their rows must be committed in window order, and a window must be
    /// computed over views that store the windows it reads.
    /// With `partition`, the run reads the raw samples of that part of the
    /// series only (for a group split by series); [`Self::merge`] joins the
    /// parts.
    pub fn compute(
        &self,
        window: ClosedWindow,
        raw_data: RawShards,
        views: Arc<ViewStore>,
        partition: Option<SeriesPartition>,
    ) -> Result<Computed, asap_executor::Error> {
        let group = &self.runs[window.group];
        let end = window.end_ms;
        let start = end - group.width_ms;
        // Views loaded from files may already store the window.
        let missing: Vec<usize> = (0..group.views.len())
            .filter(|&index| {
                views
                    .stored_through(group.views[index].node_id)
                    .is_none_or(|stored| stored < end)
            })
            .collect();
        if missing.is_empty() {
            return Ok(Computed::AlreadyStored);
        }
        let outputs = {
            let store = Arc::new(RunStore::for_partition(raw_data, views, partition));
            let instance = group.compiled.instantiate(store)?;
            let context = RunContext::new(
                Scope::Ingestion {
                    window_start_ms: start,
                    window_end_ms: end,
                },
                self.limits.clone(),
            )?;
            let outputs = block_on(instance.execute_ingestion(group.compiled.roots(), context)?)?;
            outputs
        };
        let mut by_root: BTreeMap<u64, Vec<_>> = group
            .compiled
            .roots()
            .iter()
            .copied()
            .zip(outputs)
            .collect();
        Ok(Computed::Ran(
            missing
                .into_iter()
                .map(|index| {
                    let batches = by_root
                        .remove(&(group.views[index].node_id as u64))
                        .map(|batches| batches.iter().map(|batch| (**batch).clone()).collect())
                        .unwrap_or_default();
                    (index, batches)
                })
                .collect(),
        ))
    }

    /// The whole run of a window split by series, from its parts' runs: each
    /// view's rows of every part, as the parts' own batches in part order.
    /// The parts hold disjoint series, so no two rows share a key. The
    /// batches are kept as they are (no row is decoded or re-encoded), so
    /// a sealed window keeps its compact state columns (exact records at
    /// their family's size, shrunk sketches); a window's rows are a set,
    /// read by series, so their order across batches is not significant.
    pub fn merge(&self, parts: Vec<Computed>) -> Result<Computed, asap_executor::Error> {
        let mut ran = Vec::new();
        for part in parts {
            match part {
                Computed::AlreadyStored => return Ok(Computed::AlreadyStored),
                Computed::Ran(rows) => ran.push(rows),
            }
        }
        let Some(first) = ran.first() else {
            return Ok(Computed::AlreadyStored);
        };
        let views: Vec<usize> = first.iter().map(|(index, _)| *index).collect();
        let merged = views
            .into_iter()
            .enumerate()
            .map(|(position, view)| {
                let batches: Vec<Batch> = ran
                    .iter()
                    .flat_map(|part| part[position].1.iter())
                    .filter(|batch| batch.num_rows() > 0)
                    .cloned()
                    .collect();
                if batches.is_empty() {
                    (view, ran[0][position].1.clone())
                } else {
                    (view, batches)
                }
            })
            .collect();
        Ok(Computed::Ran(merged))
    }

    /// The second half of [`Self::refresh`]: appends a computed window's
    /// rows to `views`.
    pub fn commit(
        &self,
        window: ClosedWindow,
        computed: Computed,
        views: &mut Arc<ViewStore>,
    ) -> Result<Refreshed, asap_executor::Error> {
        let Computed::Ran(rows) = computed else {
            return Ok(Refreshed::AlreadyStored);
        };
        let group = &self.runs[window.group];
        let range = (window.end_ms - group.width_ms, window.end_ms);
        let views = Arc::make_mut(views);
        let mut appended = Vec::with_capacity(rows.len());
        for (index, batches) in rows {
            let view = &group.views[index];
            views.hold_for_readers(view.node_id, &group.readers[index]);
            views.append(view, range, batches.clone())?;
            appended.push((view.node_id, batches));
        }
        // The narrower windows this window read may now go.
        for &read in &group.reads {
            views.reader_committed(read, window.group, window.end_ms);
        }
        Ok(Refreshed::Ran(appended))
    }

    /// Which windows may be computed at once.
    pub fn tasks(&self) -> &RefreshTaskGraph {
        &self.tasks
    }

    /// Each refresh group's views (with their retention margin) and their
    /// output schemas, in plan order, to load them from files.
    pub fn views(&self) -> Vec<Vec<(MaterializedView, SchemaRef)>> {
        self.runs
            .iter()
            .map(|run| {
                run.views
                    .iter()
                    .filter_map(|view| {
                        let schema = run.compiled.output_schema(view.node_id as u64).ok()?;
                        Some((view.clone(), schema))
                    })
                    .collect()
            })
            .collect()
    }

    /// Refresh group `group`'s window width.
    pub fn width_ms(&self, group: usize) -> i64 {
        self.runs[group].width_ms
    }

    /// Each refresh group's window width and views, in plan order.
    pub fn groups(&self) -> Vec<(i64, Vec<PhysicalASAPNodeId>)> {
        self.runs
            .iter()
            .map(|run| (run.width_ms, run.views.iter().map(|v| v.node_id).collect()))
            .collect()
    }
}

/// The DAG's nodes by id and each node's inputs.
struct DagIndex<'a> {
    nodes: HashMap<
        PhysicalASAPNodeId,
        &'a asap_executor::planner::plan::physical_asap_dag::PhysicalASAPDAGNode,
    >,
    inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>>,
}

impl<'a> DagIndex<'a> {
    fn new(dag: &'a PhysicalASAPDAG) -> Self {
        let mut inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>> = HashMap::new();
        for edge in &dag.edges {
            inputs.entry(edge.consumer).or_default().push(edge.producer);
        }
        Self {
            nodes: dag.nodes.iter().map(|n| (n.id, n)).collect(),
            inputs,
        }
    }

    /// The nodes reachable down from `from`, not passing a node of
    /// `stop_at` (other than `from` itself).
    fn below(
        &self,
        from: &[PhysicalASAPNodeId],
        stop_at: &HashSet<PhysicalASAPNodeId>,
    ) -> BTreeSet<PhysicalASAPNodeId> {
        let mut seen = BTreeSet::new();
        let mut pending: Vec<PhysicalASAPNodeId> = from.to_vec();
        let starts: HashSet<PhysicalASAPNodeId> = from.iter().copied().collect();
        while let Some(id) = pending.pop() {
            if (stop_at.contains(&id) && !starts.contains(&id)) || !seen.insert(id) {
                continue;
            }
            pending.extend(self.inputs.get(&id).into_iter().flatten().copied());
        }
        seen
    }

    /// The metrics scanned below `views`, not reading through `read_views`.
    fn metrics_below(
        &self,
        views: &[PhysicalASAPNodeId],
        read_views: &BTreeSet<PhysicalASAPNodeId>,
    ) -> BTreeSet<String> {
        let stop: HashSet<PhysicalASAPNodeId> = read_views.iter().copied().collect();
        crate::store::scanned_metrics(self.below(views, &stop).iter().map(|id| self.nodes[id]))
    }
}

/// `view` with its `retention_ms` extended by `margin_ms`, for the store
/// to keep its rows that much longer. The retention is not settable on a
/// `MaterializedView`, so the copy goes through its plan-file form.
fn with_retention_margin(
    view: &MaterializedView,
    margin_ms: u64,
) -> Result<MaterializedView, IngestionError> {
    let error = |e: serde_json::Error| {
        IngestionError::Unsupported(format!("materialized view {}: {e}", view.node_id))
    };
    let mut value = serde_json::to_value(view).map_err(error)?;
    value["retention_ms"] = (view.retention_ms().saturating_add(margin_ms)).into();
    serde_json::from_value(value).map_err(error)
}

fn gcd(a: i64, b: i64) -> i64 {
    if b == 0 {
        a.abs()
    } else {
        gcd(b, a.rem_euclid(b))
    }
}

/// A duration option in milliseconds as a time offset, saturating.
pub(crate) fn millis(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Milliseconds since the Unix epoch.
pub(crate) fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// End of the tumbling window of width `width_ms` (on multiples of it)
/// that holds `timestamp_ms`. Windows are `(end - width_ms, end]`, like
/// PromQL ranges: a sample at a boundary belongs to the window it ends.
pub fn window_end(timestamp_ms: i64, width_ms: i64) -> i64 {
    -((-timestamp_ms).div_euclid(width_ms)) * width_ms
}
