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
//! Raw samples are kept while a window still to be refreshed or a
//! query-time `Scan` (back to its `retention_ms`) reads them.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use asap_executor::physical_planner::{compile, CompiledPhysicalDAG};
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_executor::planner::plan::materialized_view::{
    IngestionRefreshGroup, MaterializedView, MaterializedViewRefresh,
};
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::runtime::{Limits, RunContext, Scope};
use futures::executor::block_on;

use crate::store::{RawSample, TimeIndexedStore};

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

/// One refresh group, compiled once.
struct RefreshGroupRun {
    width_ms: i64,
    phase_ms: i64,
    /// The group's views, with `retention_ms` extended by
    /// `view_retention_margin_ms`: how long the store keeps their rows.
    views: Vec<MaterializedView>,
    compiled: CompiledPhysicalDAG,
    /// The metrics its runs scan.
    metrics: BTreeSet<String>,
    /// The longest retention of its views.
    retention_ms: i64,
    /// End of the newest window refreshed or skipped; `None` before the
    /// first sample of a metric it scans.
    refreshed_through: Option<i64>,
}

impl RefreshGroupRun {
    /// The end of the group's window holding `timestamp_ms`.
    fn window_end(&self, timestamp_ms: i64) -> i64 {
        window_end(timestamp_ms - self.phase_ms, self.width_ms) + self.phase_ms
    }
}

pub struct Ingestion {
    groups: Vec<RefreshGroupRun>,
    /// For each metric a query-time `Scan` reads, how far back it reads
    /// (`None`: every sample).
    query_time_lookback_ms: HashMap<String, Option<i64>>,
    options: IngestionOptions,
    limits: Limits,
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
    /// Compiles one ingestion run per refresh group of `dag`'s `views`.
    /// Refuses views refreshed on first read, which the data plane does not
    /// refresh yet, and groups without a bounded window.
    pub fn new(
        dag: &PhysicalASAPDAG,
        views: &[MaterializedView],
        groups: Vec<IngestionRefreshGroup>,
        options: IngestionOptions,
        limits: Limits,
    ) -> Result<Self, IngestionError> {
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
        let mut runs = Vec::new();
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
            let roots: Vec<u64> = ids.iter().map(|&id| id as u64).collect();
            let compiled =
                compile(dag, &read_views, &roots).map_err(|error| IngestionError::Compile {
                    views: ids.clone(),
                    error,
                })?;
            let metrics = index.metrics_below(&ids, &read);
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
                width_ms: millis(window.width_ms),
                phase_ms: millis(window.phase_ms),
                views,
                compiled,
                metrics,
                retention_ms,
                refreshed_through: None,
            });
        }
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
        Ok(Self {
            groups: runs,
            query_time_lookback_ms,
            options,
            limits,
            newest_sample_ms: None,
            newest_clock_ms: None,
            barrier_ms: None,
            stats: IngestionStats::default(),
        })
    }

    /// The watermark: no sample older than this is expected any more. The
    /// newest routed sample, capped at the wall clock, minus the allowed
    /// lateness, or the newest barrier if that is later.
    pub fn watermark_ms(&self) -> Option<i64> {
        let newest = match self.newest_clock_ms {
            Some(clock) => self.newest_sample_ms.map(|newest| newest.min(clock)),
            None => self.newest_sample_ms,
        };
        let lateness = millis(self.options.allowed_lateness_ms);
        let from_samples = newest.map(|newest| newest.saturating_sub(lateness));
        from_samples.max(self.barrier_ms)
    }

    /// [`Self::ingest_at`] the current wall-clock time.
    pub fn ingest(&mut self, store: &mut TimeIndexedStore, samples: Vec<RawSample>) {
        self.ingest_at(store, samples, wall_clock_ms());
    }

    /// Stores `samples`, then refreshes every window the watermark has
    /// passed. `now_ms` bounds sample times from above.
    pub fn ingest_at(
        &mut self,
        store: &mut TimeIndexedStore,
        samples: Vec<RawSample>,
        now_ms: i64,
    ) {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
        let latest_allowed_ms = now_ms.saturating_add(millis(self.options.max_future_ms));
        let mut oldest_kept: HashMap<String, Option<i64>> = HashMap::new();
        for sample in samples {
            let metric = sample.labels.metric.clone();
            if !store.raw_data().reads(&metric) {
                self.stats.samples_unrouted += 1;
                continue;
            }
            if sample.timestamp_ms > latest_allowed_ms {
                self.stats.samples_dropped_future += 1;
                continue;
            }
            self.start_groups(&metric, sample.timestamp_ms);
            let cutoff = *oldest_kept
                .entry(metric.to_string())
                .or_insert_with(|| self.oldest_kept_ms(&metric));
            if cutoff.is_some_and(|cutoff| sample.timestamp_ms <= cutoff) {
                self.stats.samples_dropped_late += 1;
                continue;
            }
            self.newest_sample_ms = self.newest_sample_ms.max(Some(sample.timestamp_ms));
            store.raw_data_mut().push(sample);
            self.stats.samples_accepted += 1;
        }
        self.refresh_closed_windows(store);
    }

    /// [`Self::advance_watermark_at`] the current wall-clock time.
    pub fn advance_watermark(&mut self, store: &mut TimeIndexedStore, barrier_ms: i64) -> i64 {
        self.advance_watermark_at(store, barrier_ms, wall_clock_ms())
    }

    /// An event-time barrier: the producer states that every sample at or
    /// before `barrier_ms` has been written, so every window ending at or
    /// before it closes without a later sample. Capped at `now_ms`; a
    /// repeated or lower barrier does nothing. Returns the barrier in force.
    pub fn advance_watermark_at(
        &mut self,
        store: &mut TimeIndexedStore,
        barrier_ms: i64,
        now_ms: i64,
    ) -> i64 {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
        let barrier_ms = barrier_ms.min(now_ms);
        if let Some(current) = self.barrier_ms.filter(|current| *current >= barrier_ms) {
            return current;
        }
        self.barrier_ms = Some(barrier_ms);
        self.refresh_closed_windows(store);
        barrier_ms
    }

    /// Starts each group scanning `metric` at its first sample: its first
    /// window is skipped unless it starts at or after the declared stream
    /// start.
    fn start_groups(&mut self, metric: &str, timestamp_ms: i64) {
        for group in &mut self.groups {
            if group.refreshed_through.is_some() || !group.metrics.contains(metric) {
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

    fn refresh_closed_windows(&mut self, store: &mut TimeIndexedStore) {
        let Some(watermark) = self.watermark_ms() else {
            return;
        };
        for group in &mut self.groups {
            let Some(mut through) = group.refreshed_through else {
                continue;
            };
            // A window ending at or before this would already be dropped.
            let expired_through =
                group.window_end(watermark.saturating_sub(group.retention_ms)) - group.width_ms;
            if through < expired_through {
                self.stats.windows_skipped_expired +=
                    ((expired_through - through) / group.width_ms) as u64;
                through = expired_through;
            }
            while let Some(end) = through
                .checked_add(group.width_ms)
                .filter(|e| *e <= watermark)
            {
                match refresh(group, end, store, &self.limits) {
                    Ok(()) => self.stats.windows_refreshed += 1,
                    Err(error) => {
                        tracing::error!(views = ?group.views.iter().map(|v| v.node_id).collect::<Vec<_>>(), end, %error, "ingestion run failed");
                        self.stats.windows_failed += 1;
                    }
                }
                through = end;
            }
            group.refreshed_through = Some(through);
        }
        let metrics: Vec<String> = self
            .groups
            .iter()
            .flat_map(|g| g.metrics.iter().cloned())
            .chain(self.query_time_lookback_ms.keys().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for metric in metrics {
            if let Some(cutoff) = self.oldest_kept_ms(&metric) {
                store.raw_data_mut().evict_through(&metric, cutoff);
            }
        }
    }
}

/// Runs `group`'s views in one ingestion run over `(end - W, end]` and
/// appends each view's rows to `store`.
fn refresh(
    group: &RefreshGroupRun,
    end: i64,
    store: &mut TimeIndexedStore,
    limits: &Limits,
) -> Result<(), asap_executor::Error> {
    let start = end - group.width_ms;
    let outputs = {
        let instance = group.compiled.instantiate(store.run_store())?;
        let context = RunContext::new(
            Scope::Ingestion {
                window_start_ms: start,
                window_end_ms: end,
            },
            limits.clone(),
        )?;
        let outputs = block_on(instance.execute_ingestion(group.compiled.roots(), context)?)?;
        outputs
    };
    let by_root: BTreeMap<u64, Vec<_>> = group
        .compiled
        .roots()
        .iter()
        .copied()
        .zip(outputs)
        .collect();
    for view in &group.views {
        let batches = by_root
            .get(&(view.node_id as u64))
            .map(|batches| batches.iter().map(|batch| (**batch).clone()).collect())
            .unwrap_or_default();
        store.append_view_rows(view, (start, end), batches)?;
    }
    Ok(())
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

fn millis(value: u64) -> i64 {
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
