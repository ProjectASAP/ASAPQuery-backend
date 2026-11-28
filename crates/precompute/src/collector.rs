//! Collector views (docs/design/collector-sketch-ingest.md, §6): views of the
//! installed plan a collector builds, whose windowed states arrive over
//! OTLP ([`common::ViewStateBatch`]) instead of being computed here.
//!
//! A state goes through three steps:
//!
//! 1. [`CollectorViews::check`], on the HTTP thread: finds the view by
//!    `(plan_id, node_id)`, checks its key, metric name and window, interns
//!    the series as a raw sample's, and decodes the ASAPv1 bytes into the
//!    view's kernel. A state failing a check is rejected with a reason.
//! 2. [`Staging::stage`], on the ingest coordinator: a state of a window
//!    already closed is late and dropped; otherwise it is staged per
//!    `(view, series, window)`, a repeat of a staged state (equal bytes) is
//!    a duplicate, and a different state of the same series and window is
//!    merged into the staged one.
//! 3. When the view's window closes (the wall clock passes its end by the
//!    commit grace, [`crate::ingestion`]), its staged states become the
//!    window's rows ([`CollectorViews::rows`]) and are committed by the
//!    precompute committer as a computed window is. A window with no state
//!    is not stored, so a query reading it is not answered from the plan.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::schema::{FieldDataType, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::ir::{ASAPOp, NonASAPOp, Operator};
use asap_executor::planner::plan::materialized_view::{MaterializedView, ViewStateKey};
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::summary_kernels::factory::create_planner_accumulator;
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::AggregateCore;
use common::{view_identity, ViewStateBatch, MAX_TIME_MS};
use internal_metrics::Metrics;
use storage::persist::Asapv1Decoder;
use storage::series_ids::{intern_pairs, Series};

use crate::ingestion::{ClosedWindow, Computed, Ingestion};

/// Which views of the installed plan a collector builds, and how long after
/// its end such a view's window is committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectorViewOptions {
    pub views: BTreeSet<PhysicalASAPNodeId>,
    /// The collector's window close delay, its export interval and an
    /// allowance for clock skew: a state arriving later than this after its
    /// window's end is dropped as late.
    pub commit_grace_ms: u64,
}

impl Default for CollectorViewOptions {
    /// No collector views; the grace of today's collector (6 s close
    /// delay, no export interval, 2 s skew allowance).
    fn default() -> Self {
        Self {
            views: BTreeSet::new(),
            commit_grace_ms: 8_000,
        }
    }
}

/// Why a state was rejected, as `asap_collector_states_rejected_total`'s
/// `reason`.
pub mod reject {
    /// Another plan's view.
    pub const OTHER_PLAN: &str = "other_plan";
    /// No collector view of the installed plan has this node id.
    pub const UNKNOWN_VIEW: &str = "unknown_view";
    /// The view's state key differs from the installed view's.
    pub const KEY_MISMATCH: &str = "key_mismatch";
    /// The metric is not `<source metric>.asap_view.<node id>`.
    pub const NAME: &str = "name";
    /// The window is not one of the view's, or too far ahead of the clock.
    pub const WINDOW: &str = "window";
    /// The bytes are not a state of the view's kernel.
    pub const DECODE: &str = "decode";
    /// The state does not merge with the one staged for its series and
    /// window.
    pub const MERGE: &str = "merge";
}

/// What happened to a batch's states.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectorStateIngestReport {
    /// Staged: the first state of its series and window.
    pub staged: u64,
    /// Ignored: equal to a state already staged for its series and window.
    pub duplicate: u64,
    /// Merged into the state staged for its series and window.
    pub merged: u64,
    /// Dropped: its window was already committed.
    pub late_dropped: u64,
    /// Rejected, by reason ([`reject`]).
    pub rejected: BTreeMap<&'static str, u64>,
}

impl CollectorStateIngestReport {
    /// States rejected for any reason.
    pub fn rejected_total(&self) -> u64 {
        self.rejected.values().sum()
    }
}

/// One state that passed the checks, for the coordinator to stage.
pub(crate) struct ArrivedState {
    pub group: usize,
    pub node_id: PhysicalASAPNodeId,
    pub series: &'static Series,
    pub end_ms: i64,
    /// The hash of its ASAPv1 bytes, which tells a repeat from a partial.
    pub hash: u64,
    pub state: Arc<dyn AggregateCore>,
}

/// One installed collector view.
struct CollectorView {
    group: usize,
    key: ViewStateKey,
    /// The metric its states come under: `<source metric>.asap_view.<id>`.
    metric_name: String,
    /// The metric its `Scan` reads: its rows' `__name__`.
    source_metric: String,
    width_ms: i64,
    decoder: Asapv1Decoder,
    /// Its rows' schema, and where the window end, the series identity
    /// and the state are.
    schema: SchemaRef,
    window_end_column: Option<usize>,
    identity_column: usize,
    state_column: usize,
}

/// The installed plan's collector views: what checks a state and turns
/// staged states into a window's rows.
pub(crate) struct CollectorViews {
    /// `PhysicalASAPDAG::plan_id()` of the installed plan.
    plan_id: String,
    views: HashMap<PhysicalASAPNodeId, CollectorView>,
    /// Each collector view's group's view.
    view_of_group: HashMap<usize, PhysicalASAPNodeId>,
    max_future_ms: i64,
    /// When each (view, reason) rejection was last logged.
    logged: Mutex<HashMap<(PhysicalASAPNodeId, &'static str), Instant>>,
}

/// How often a rejection of one view and reason is logged.
const LOG_INTERVAL: Duration = Duration::from_secs(60);

impl CollectorViews {
    /// The collector views `ids` of `dag` (`views` its materialized views),
    /// each in its group of `groups` (each group's views and their output
    /// schemas, as `Precompute::views` lists them). Errs for a view whose
    /// state has no ASAPv1 encoding, that does not read exactly one metric,
    /// or whose rows hold more than a window end, a series identity and a
    /// state.
    pub fn new(
        dag: &PhysicalASAPDAG,
        views: &[MaterializedView],
        groups: &[Vec<(MaterializedView, SchemaRef)>],
        ids: &BTreeSet<PhysicalASAPNodeId>,
        max_future_ms: u64,
    ) -> anyhow::Result<Self> {
        let nodes: HashMap<PhysicalASAPNodeId, _> = dag.nodes.iter().map(|n| (n.id, n)).collect();
        let mut inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>> = HashMap::new();
        for edge in &dag.edges {
            inputs.entry(edge.consumer).or_default().push(edge.producer);
        }
        let mut installed = HashMap::new();
        for &id in ids {
            let fail = |why: String| anyhow::anyhow!("collector view {id}: {why}");
            let view = views
                .iter()
                .find(|view| view.node_id == id)
                .ok_or_else(|| fail("not a materialized view".into()))?;
            let (group, schema) = groups
                .iter()
                .enumerate()
                .find_map(|(group, views)| {
                    views
                        .iter()
                        .find(|(view, _)| view.node_id == id)
                        .map(|(_, schema)| (group, schema.clone()))
                })
                .ok_or_else(|| fail("not refreshed at ingestion".into()))?;
            let width_ms = match view.view_state_key().window {
                Some(common::ViewWindow::Tumbling { width_ms }) => storage::store::millis(width_ms),
                _ => return Err(fail("its rows are not tumbling windows".into())),
            };
            let node = nodes[&id];
            let (summary_type, input) = match &node.payload {
                Operator::ASAP(
                    ASAPOp::SummaryTumblingWindowChain {
                        summary_type,
                        input,
                        ..
                    }
                    | ASAPOp::SummaryAgg {
                        summary_type,
                        input,
                        ..
                    },
                ) => (summary_type, input),
                other => return Err(fail(format!("builds no summary: {other:?}"))),
            };
            let template =
                create_planner_accumulator(summary_type, node.parameters.as_ref(), input)
                    .map_err(fail)?
                    .snapshot_accumulator();
            let decoder = Asapv1Decoder::of_state(&*template)
                .ok_or_else(|| fail("its state has no ASAPv1 encoding".into()))?;
            let metrics = scanned_metrics_below(id, &nodes, &inputs);
            let [source_metric] = <[String; 1]>::try_from(metrics.into_iter().collect::<Vec<_>>())
                .map_err(|metrics| fail(format!("reads metrics {metrics:?}, not one")))?;
            let mut identity_column = None;
            let mut state_column = None;
            // A chain's rows start with their window's end, its time index.
            let window_end_column = schema.time_index;
            for (column, field) in schema.fields.iter().enumerate() {
                match &field.dtype {
                    _ if Some(column) == window_end_column => {}
                    _ if field.name == PROMQL_SERIES_IDENTITY => identity_column = Some(column),
                    FieldDataType::Plain(_) => {
                        return Err(fail(format!(
                            "its rows have column {}, which a collector state does not carry",
                            field.name
                        )))
                    }
                    _ => state_column = Some(column),
                }
            }
            let (Some(identity_column), Some(state_column)) = (identity_column, state_column)
            else {
                return Err(fail(
                    "its rows are not a series identity and a state".into(),
                ));
            };
            installed.insert(
                id,
                CollectorView {
                    group,
                    key: view.view_state_key().clone(),
                    metric_name: view_identity::metric_name(&source_metric, id),
                    source_metric,
                    width_ms,
                    decoder,
                    schema,
                    window_end_column,
                    identity_column,
                    state_column,
                },
            );
        }
        Ok(Self {
            plan_id: dag.plan_id(),
            view_of_group: installed
                .iter()
                .map(|(&id, view)| (view.group, id))
                .collect(),
            views: installed,
            max_future_ms: storage::store::millis(max_future_ms),
            logged: Mutex::new(HashMap::new()),
        })
    }

    /// The states of `batch` that pass every check, decoded, at wall-clock
    /// time `now_ms`, and the report of those rejected (counted in
    /// `metrics`, each view and reason logged at most once a minute).
    pub fn check(
        &self,
        batch: ViewStateBatch,
        now_ms: i64,
        metrics: &Metrics,
    ) -> (Vec<ArrivedState>, CollectorStateIngestReport) {
        let mut report = CollectorStateIngestReport::default();
        let mut arrived = Vec::new();
        for states in batch.views {
            let node_id = states.node_id;
            let mut reject = |reason: &'static str, count: u64, detail: &dyn Fn() -> String| {
                if count == 0 {
                    return;
                }
                *report.rejected.entry(reason).or_default() += count;
                metrics.collector_states.rejected(node_id, reason, count);
                self.log_rejection(node_id, reason, detail);
            };
            let count = |series: &[common::CollectorSeriesStates]| -> u64 {
                series.iter().map(|s| s.records.len() as u64).sum()
            };
            let view = match self.views.get(&node_id) {
                _ if states.plan_id != self.plan_id => {
                    reject(reject::OTHER_PLAN, count(&states.series), &|| {
                        format!("plan {} is not the installed plan", states.plan_id)
                    });
                    continue;
                }
                None => {
                    reject(reject::UNKNOWN_VIEW, count(&states.series), &|| {
                        "no collector view of the installed plan has this node id".into()
                    });
                    continue;
                }
                Some(view) if view.key != states.key => {
                    reject(reject::KEY_MISMATCH, count(&states.series), &|| {
                        format!(
                            "view state key {:?} differs from the installed view's",
                            states.key
                        )
                    });
                    continue;
                }
                Some(view) if view.metric_name != states.metric => {
                    reject(reject::NAME, count(&states.series), &|| {
                        format!("metric {} is not {}", states.metric, view.metric_name)
                    });
                    continue;
                }
                Some(view) => view,
            };
            for series in states.series {
                if series.labels.get("__name__") != Some(&view.source_metric) {
                    reject(reject::NAME, series.records.len() as u64, &|| {
                        format!("a series' __name__ is not {}", view.source_metric)
                    });
                    continue;
                }
                let labels: Vec<(&str, &str)> = series
                    .labels
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect();
                let interned = intern_pairs(&labels);
                for record in series.records {
                    let end_ms = record.window_end_ms;
                    if end_ms.rem_euclid(view.width_ms) != 0
                        || record.window_start_ms != end_ms - view.width_ms
                        || end_ms > now_ms.saturating_add(self.max_future_ms).min(MAX_TIME_MS)
                        || end_ms < -MAX_TIME_MS
                    {
                        reject(reject::WINDOW, 1, &|| {
                            format!(
                                "window ({}, {end_ms}] is not one of its {} ms windows up to now",
                                record.window_start_ms, view.width_ms
                            )
                        });
                        continue;
                    }
                    let Some(state) = view.decoder.decode(&record.asapv1) else {
                        reject(reject::DECODE, 1, &|| {
                            "the bytes are not a state of the view's sketch".into()
                        });
                        continue;
                    };
                    metrics
                        .collector_states
                        .arrived(node_id, (now_ms - end_ms).max(0));
                    let mut hasher = DefaultHasher::new();
                    record.asapv1.hash(&mut hasher);
                    arrived.push(ArrivedState {
                        group: view.group,
                        node_id,
                        series: interned,
                        end_ms,
                        hash: hasher.finish(),
                        state,
                    });
                }
            }
        }
        (arrived, report)
    }

    fn log_rejection(
        &self,
        view: PhysicalASAPNodeId,
        reason: &'static str,
        detail: &dyn Fn() -> String,
    ) {
        let mut logged = self.logged.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if logged
            .get(&(view, reason))
            .is_some_and(|last| now.duration_since(*last) < LOG_INTERVAL)
        {
            return;
        }
        logged.insert((view, reason), now);
        tracing::warn!(view, reason, detail = %detail(), "collector states rejected");
    }

    /// The collector view refresh group `group` builds.
    pub fn view_of_group(&self, group: usize) -> Option<PhysicalASAPNodeId> {
        self.view_of_group.get(&group).copied()
    }

    /// The rows of `window` of a collector view's group holding `states`,
    /// one row per series, as the view's own run writes them.
    pub fn rows(&self, window: ClosedWindow, states: Vec<Staged>) -> Result<Computed, String> {
        let group = window.group;
        let view = self
            .view_of_group(group)
            .and_then(|id| self.views.get(&id))
            .ok_or_else(|| format!("refresh group {group} is no collector view"))?;
        let family = view.schema.fields[view.state_column].dtype.clone();
        let rows = states
            .into_iter()
            .map(|staged| {
                let mut row = vec![Value::Null; view.schema.fields.len()];
                if let Some(column) = view.window_end_column {
                    row[column] = Value::Timestamp(window.end_ms);
                }
                row[view.identity_column] = Value::Utf8(staged.series.identity.clone());
                row[view.state_column] = Value::Summary {
                    family: family.clone(),
                    state: staged.state,
                };
                row
            })
            .collect();
        let batch = Batch::try_new(view.schema.clone(), rows).map_err(|e| e.to_string())?;
        Ok(Computed::Ran(vec![(0, vec![batch])]))
    }
}

/// The `TimeSeries` metrics scanned below `id`.
fn scanned_metrics_below(
    id: PhysicalASAPNodeId,
    nodes: &HashMap<
        PhysicalASAPNodeId,
        &asap_executor::planner::plan::physical_asap_dag::PhysicalASAPDAGNode,
    >,
    inputs: &HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>>,
) -> BTreeSet<String> {
    let mut seen = HashSet::new();
    let mut pending = vec![id];
    let mut metrics = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if let Operator::NonASAP(NonASAPOp::Scan {
            source: Source::TimeSeries { metric },
            ..
        }) = &nodes[&id].payload
        {
            metrics.insert(metric.clone());
        }
        pending.extend(inputs.get(&id).into_iter().flatten().copied());
    }
    metrics
}

/// One series' state of one window, staged until the window commits.
pub(crate) struct Staged {
    series: &'static Series,
    state: Arc<dyn AggregateCore>,
    /// The hashes of every state merged into it: a repeat of any is a
    /// duplicate, even after a later partial was merged.
    hashes: Vec<u64>,
}

/// The collector states staged per window and series, on the ingest
/// coordinator.
#[derive(Default)]
pub(crate) struct Staging {
    /// By `(group, window end)`, then by series id.
    windows: BTreeMap<(usize, i64), BTreeMap<u32, Staged>>,
}

impl Staging {
    /// Stages `states`, merging and deduplicating per series and window; a
    /// state of a window `ingestion` already closed is late. A view's group
    /// starts at the oldest window of its first states. Counts each state's
    /// outcome in `metrics`.
    pub fn stage(
        &mut self,
        states: Vec<ArrivedState>,
        ingestion: &mut Ingestion,
        metrics: &Metrics,
    ) -> CollectorStateIngestReport {
        let mut report = CollectorStateIngestReport::default();
        let mut oldest: BTreeMap<usize, i64> = BTreeMap::new();
        for state in &states {
            let end = oldest.entry(state.group).or_insert(state.end_ms);
            *end = (*end).min(state.end_ms);
        }
        for (group, end_ms) in oldest {
            ingestion.start_collector_group(group, end_ms);
        }
        for arrived in states {
            let view = arrived.node_id;
            if ingestion
                .closed_through(arrived.group)
                .is_some_and(|through| arrived.end_ms <= through)
            {
                report.late_dropped += 1;
                metrics.collector_states.outcome(view, "late_dropped", 1);
                continue;
            }
            let window = self
                .windows
                .entry((arrived.group, arrived.end_ms))
                .or_default();
            let Some(staged) = window.get_mut(&arrived.series.id) else {
                window.insert(
                    arrived.series.id,
                    Staged {
                        series: arrived.series,
                        state: arrived.state,
                        hashes: vec![arrived.hash],
                    },
                );
                report.staged += 1;
                metrics.collector_states.outcome(view, "staged", 1);
                continue;
            };
            if staged.hashes.contains(&arrived.hash) {
                report.duplicate += 1;
                metrics.collector_states.outcome(view, "duplicate", 1);
                continue;
            }
            match staged.state.merge_with(&*arrived.state) {
                Ok(merged) => {
                    staged.state = Arc::from(merged);
                    staged.hashes.push(arrived.hash);
                    report.merged += 1;
                    metrics.collector_states.outcome(view, "merged", 1);
                }
                Err(error) => {
                    *report.rejected.entry(reject::MERGE).or_default() += 1;
                    metrics.collector_states.rejected(view, reject::MERGE, 1);
                    tracing::warn!(view, %error, "a collector state does not merge with the staged one");
                }
            }
        }
        report
    }

    /// The states staged for `window`, removed; empty when none is.
    pub fn take(&mut self, window: ClosedWindow) -> Vec<Staged> {
        self.windows
            .remove(&(window.group, window.end_ms))
            .map(|series| series.into_values().collect())
            .unwrap_or_default()
    }

    /// Drops the states staged for windows of `group` ending at or before
    /// `through` (windows skipped as expired), returning how many.
    pub fn drop_through(&mut self, group: usize, through: i64) -> u64 {
        let mut dropped = 0;
        self.windows.retain(|&(g, end_ms), series| {
            let expired = g == group && end_ms <= through;
            if expired {
                dropped += series.len() as u64;
            }
            !expired
        });
        dropped
    }
}
