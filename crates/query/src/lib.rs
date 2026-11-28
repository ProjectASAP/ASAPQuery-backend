//! Query engine: answers a planned query at evaluation time `T` with
//! query runs of its root of the plan's DAG, over the store.
//!
//! What runs once and what is shared is the plan's decision, by each
//! query-time node's `Lifetime`: a `None` node runs in each run that reads
//! it; a `OneEvaluation` node runs once per evaluation time and its
//! consumers share it; an `AcrossEvaluations` node is a materialized view.
//! A `OneEvaluation` node reached from more than one query's run is
//! compiled on its own, and its output at `T` is stored in the view
//! store's query-time partition ([`QueryTimeResults`]): every run at `T`
//! that reads it binds it as a materialized view slot. Any other node
//! reached from more than one query's run means the plan and the data
//! plane disagree, and the plan is refused.
//!
//! Each run is compiled once, with the materialized views it reaches
//! (without passing another view) and the shared nodes below it as view
//! slots, and each `Scan` it reaches outside them as a raw data slot. A
//! query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus (or refuses it).
//!
//! [`ExecutorQueryEngine`] runs the compiled queries ([`CompiledQueries`])
//! on the `query-N` workers ([`workers`]) and implements
//! [`common::QueryEngine`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use asap_executor::physical_planner::{compile, CompiledPhysicalDAG};
use asap_executor::planner::ir::properties::ExecutionTiming;
use asap_executor::planner::ir::schema::{Schema, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::ir::{NonASAPOp, Operator};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::planner::stages::Lifetime;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, Value};
use common::{Answer, InstallablePlan, ResultSample};
use futures::executor::block_on;
use futures::StreamExt;
use internal_metrics::QueryMetrics;
use storage::store::{PublishedStore, QueryTimeResults, RawReads, RawShards, RunStore, ViewStore};

use crate::result_cache::ResultCache;

mod engine;
pub mod result_cache;
pub mod workers;

pub use engine::{ExecutorQueryEngine, QueryOptions};

/// Why the engine does not answer a query.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Unanswered {
    /// The query was resolved against another plan than the one installed.
    #[error("the plan changed since the query was resolved")]
    PlanChanged,
    #[error("the data plane cannot run this query: {0}")]
    Unsupported(String),
    /// Rows the run reads are not stored: a view's window (e.g. the first
    /// window after startup, or one before its retention) or raw samples
    /// already evicted.
    #[error("{0}")]
    MissingRows(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

impl Unanswered {
    /// Whether the query only lacks data, or was resolved against another
    /// plan (the boundary's `NotReady`), rather than failing to run.
    pub fn is_not_ready(&self) -> bool {
        matches!(self, Self::MissingRows(_) | Self::PlanChanged)
    }
}

/// One compiled query run: of a query's root, or of a shared node.
struct Run {
    compiled: CompiledPhysicalDAG,
    /// The shared nodes it binds as view slots.
    reads: Vec<PhysicalASAPNodeId>,
}

/// The installed plan's queries, compiled.
pub struct CompiledQueries {
    /// Each planned query (trimmed) the engine cannot run, with why.
    unexecutable: Vec<(String, String)>,
    /// Each planned root: whether its run, and every shared node's run it
    /// reads, compiled.
    runnable: HashMap<PhysicalASAPNodeId, Result<(), String>>,
    /// Each planned query's measurements, by query text (trimmed), sorted.
    metrics: Vec<(String, QueryMetrics)>,
    /// The run of each query root and shared node.
    runs: BTreeMap<PhysicalASAPNodeId, Run>,
    /// The shared nodes: their outputs live in `results`.
    shared: BTreeSet<PhysicalASAPNodeId>,
    results: QueryTimeResults<Unanswered>,
    limits: Limits,
    /// The query roots whose runs read raw data through a `Scan` (rather
    /// than only materialized views).
    reads_raw_data: BTreeSet<PhysicalASAPNodeId>,
    /// Final answers of the query roots by evaluation time; `None`: off.
    result_cache: Option<ResultCache>,
}

impl CompiledQueries {
    /// Compiles each query's root of `plan`, whose views are `views`, and
    /// each query-time `OneEvaluation` node shared by several queries'
    /// runs, whose outputs `results` keeps. With `share_one_evaluation`
    /// false, every run computes every node it reads, as one run per query
    /// did before. Fails when a node other than a query-time
    /// `OneEvaluation` one would be computed in several queries' runs.
    pub fn new(
        plan: &InstallablePlan,
        views: &[MaterializedView],
        limits: Limits,
        results: QueryTimeResults<Unanswered>,
        share_one_evaluation: bool,
        result_cache: Option<ResultCache>,
    ) -> anyhow::Result<Self> {
        let dag = &plan.dag;
        let view_ids: HashSet<PhysicalASAPNodeId> = views.iter().map(|v| v.node_id).collect();
        let reads_raw_data = dag
            .roots
            .iter()
            .copied()
            .filter(|&root| scans_below(dag, root, &view_ids))
            .collect();
        let shared = if share_one_evaluation {
            shared_nodes(dag, &view_ids)?
        } else {
            BTreeSet::new()
        };
        let mut slots: Vec<MaterializedView> = views.to_vec();
        if !shared.is_empty() {
            let plan_id = dag.plan_id();
            for &node in &shared {
                slots.push(query_time_slot(&plan_id, node)?);
            }
        }
        let mut compiled: BTreeMap<PhysicalASAPNodeId, Result<Run, String>> = BTreeMap::new();
        for &node in shared.iter().chain(&dag.roots) {
            if compiled.contains_key(&node) {
                continue;
            }
            // A shared node's own run computes it, rather than reading it.
            let own_slots: Vec<MaterializedView> = slots
                .iter()
                .filter(|v| !(shared.contains(&node) && v.node_id == node))
                .cloned()
                .collect();
            let read = slots_read(dag, &own_slots, node);
            let reads = read
                .iter()
                .map(|v| v.node_id)
                .filter(|id| shared.contains(id))
                .collect();
            let run = compile(dag, &read, &[node as u64])
                .map(|compiled| Run { compiled, reads })
                .map_err(|e| e.to_string());
            compiled.insert(node, run);
        }
        let runnable_roots: HashMap<PhysicalASAPNodeId, Result<(), String>> = dag
            .roots
            .iter()
            .map(|&root| (root, runnable(root, &compiled)))
            .collect();
        let queries = || plan.query_texts.iter().map(|query| query.trim());
        let unexecutable = queries()
            .zip(&dag.roots)
            .filter_map(|(query, root)| {
                let reason = runnable_roots[root].as_ref().err()?;
                Some((query.to_string(), reason.clone()))
            })
            .collect();
        let metrics: BTreeSet<&str> = queries().collect();
        let metrics = metrics
            .into_iter()
            .map(|query| (query.to_string(), QueryMetrics::default()))
            .collect();
        let runs = compiled
            .into_iter()
            .filter_map(|(node, run)| Some((node, run.ok()?)))
            .collect();
        Ok(Self {
            unexecutable,
            runnable: runnable_roots,
            metrics,
            runs,
            shared,
            results,
            limits,
            reads_raw_data,
            result_cache,
        })
    }

    /// The planned queries the engine cannot run, with why.
    pub fn unexecutable(&self) -> &[(String, String)] {
        &self.unexecutable
    }

    /// The query-time partition of the view store.
    pub fn query_time_results(&self) -> &QueryTimeResults<Unanswered> {
        &self.results
    }

    /// The query-time `OneEvaluation` nodes several queries' runs share.
    pub fn shared_nodes(&self) -> &BTreeSet<PhysicalASAPNodeId> {
        &self.shared
    }

    /// Each planned query's measurements, by query text, sorted.
    pub fn metrics(&self) -> &[(String, QueryMetrics)] {
        &self.metrics
    }

    /// Where the planned `query`'s measurements are in [`Self::metrics`].
    pub(crate) fn metrics_index(&self, query: &str) -> Option<usize> {
        let query = query.trim();
        self.metrics
            .binary_search_by(|(planned, _)| planned.as_str().cmp(query))
            .ok()
    }

    /// Planned root `root`, if its run compiled; `PlanChanged` if it is no
    /// root of this plan.
    fn root(&self, root: PhysicalASAPNodeId) -> Result<PhysicalASAPNodeId, Unanswered> {
        match self.runnable.get(&root) {
            Some(Ok(())) => Ok(root),
            Some(Err(reason)) => Err(Unanswered::Unsupported(reason.clone())),
            None => Err(Unanswered::PlanChanged),
        }
    }

    /// The result cache's answer at `time_ms` of the query planned at
    /// `root`, if it keeps one.
    pub fn cached(&self, root: PhysicalASAPNodeId, time_ms: i64) -> Option<Answer> {
        let cache = self.result_cache.as_ref()?;
        cache.get(self.root(root).ok()?, time_ms)
    }

    /// The result cache, when it is on.
    pub fn result_cache(&self) -> Option<&ResultCache> {
        self.result_cache.as_ref()
    }

    /// What evaluations starting now read: the newest snapshot of `store`.
    /// The evaluations of one range request share one, so each raw chunk
    /// they read whole is converted once.
    pub fn snapshot(&self, store: &PublishedStore) -> Snapshot {
        // The watermark first: raw data is published before the watermark
        // that covers it, so the snapshot read next holds every sample at
        // or before this watermark.
        let watermark_ms = store.watermark_ms();
        let (raw_data, views) = store.snapshot();
        Snapshot {
            raw_data,
            views,
            watermark_ms,
            raw_reads: Arc::default(),
        }
    }

    /// Answers the query planned at `root` at `time_ms` from `snapshot`, and keeps the answer in the result cache when it is final: the run
    /// succeeded, so every view window it read was stored (a window is
    /// appended once, when the watermark closes it, and never changes),
    /// and, when the root reads raw data through a `Scan`, `time_ms` is at
    /// or before the watermark the snapshot was published with (no sample
    /// at or before it is expected any more). The result cache is not
    /// read here: callers read it first ([`Self::cached`]).
    pub fn evaluate(
        &self,
        root: PhysicalASAPNodeId,
        time_ms: i64,
        snapshot: &Snapshot,
    ) -> Result<Answer, Unanswered> {
        let root = self.root(root)?;
        let watermark_ms = snapshot.watermark_ms;
        let batches = self.output(root, time_ms, snapshot)?;
        let compiled = &self.runs[&root].compiled;
        let schema = compiled
            .output_schema(root as u64)
            .map_err(|e| Unanswered::Execution(e.to_string()))?;
        let answer = Arc::new(result_samples(&schema, &batches)?);
        if let Some(cache) = &self.result_cache {
            let final_answer = !self.reads_raw_data.contains(&root)
                || watermark_ms.is_some_and(|watermark| time_ms <= watermark);
            if final_answer {
                cache.insert(root, time_ms, answer.clone(), watermark_ms);
            }
        }
        Ok(answer)
    }

    /// `node`'s output at `time_ms`: from the query-time partition when it
    /// is shared (computed there by the first run that needs it), else
    /// from its own run.
    fn output(
        &self,
        node: PhysicalASAPNodeId,
        time_ms: i64,
        snapshot: &Snapshot,
    ) -> Result<Arc<Vec<Batch>>, Unanswered> {
        if self.shared.contains(&node) {
            self.results
                .get_or_compute(node, time_ms, snapshot.watermark_ms, || {
                    self.run(node, time_ms, snapshot)
                })
        } else {
            self.run(node, time_ms, snapshot).map(Arc::new)
        }
    }

    /// The batches of `node`'s run at `time_ms`, after the shared nodes it
    /// reads.
    fn run(
        &self,
        node: PhysicalASAPNodeId,
        time_ms: i64,
        snapshot: &Snapshot,
    ) -> Result<Vec<Batch>, Unanswered> {
        let run = &self.runs[&node];
        let mut shared = HashMap::new();
        for &read in &run.reads {
            shared.insert(read, self.output(read, time_ms, snapshot)?);
        }
        let run_store = Arc::new(RunStore::with_query_time_results(
            snapshot.raw_data.clone(),
            snapshot.views.clone(),
            Some(snapshot.raw_reads.clone()),
            shared,
        ));
        run_compiled(&run.compiled, run_store.clone(), time_ms, &self.limits).map_err(|error| {
            match run_store.missing_rows() {
                Some(missing) => Unanswered::MissingRows(missing),
                None => Unanswered::Execution(error.to_string()),
            }
        })
    }
}

/// What crosses threads: the compiled queries and the store each run
/// reads.
#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}
const _: fn() = || {
    assert_send_sync::<CompiledQueries>();
    assert_send_sync::<RunStore>();
};

/// What evaluations read: the raw data and views published when they
/// started, the watermark then, and the raw chunks they converted so far.
pub struct Snapshot {
    raw_data: RawShards,
    views: Arc<ViewStore>,
    watermark_ms: Option<i64>,
    raw_reads: Arc<RawReads>,
}

impl Snapshot {
    /// Every stored view's rows: per view, each window's `(start, end]`
    /// and batches, oldest first (for comparing two engines' views).
    #[doc(hidden)]
    pub fn view_rows(&self) -> storage::store::ViewRowsByView {
        self.views.rows()
    }
}

/// Whether `node`'s run, and every shared node's run it reads, compiled.
fn runnable(
    node: PhysicalASAPNodeId,
    compiled: &BTreeMap<PhysicalASAPNodeId, Result<Run, String>>,
) -> Result<(), String> {
    let run = compiled[&node].as_ref().map_err(Clone::clone)?;
    run.reads
        .iter()
        .try_for_each(|&read| runnable(read, compiled))
}

/// The query-time `OneEvaluation` nodes that more than one query's run
/// reaches: each is cut out of the runs above it into a run of its own,
/// the topmost first, until no node is computed in two runs. A node other
/// than a query-time `OneEvaluation` one that two runs would compute is an
/// error: the plan does not share it. A `Scan` is a read, not a
/// computation, and is never cut.
fn shared_nodes(
    dag: &PhysicalASAPDAG,
    views: &HashSet<PhysicalASAPNodeId>,
) -> anyhow::Result<BTreeSet<PhysicalASAPNodeId>> {
    let nodes: HashMap<PhysicalASAPNodeId, _> = dag.nodes.iter().map(|n| (n.id, n)).collect();
    let mut inputs: HashMap<PhysicalASAPNodeId, Vec<PhysicalASAPNodeId>> = HashMap::new();
    for edge in &dag.edges {
        inputs.entry(edge.consumer).or_default().push(edge.producer);
    }
    let is_scan = |id: PhysicalASAPNodeId| {
        matches!(
            nodes[&id].payload,
            Operator::NonASAP(NonASAPOp::Scan { .. })
        )
    };
    let mut cut: BTreeSet<PhysicalASAPNodeId> = BTreeSet::new();
    loop {
        // How many runs compute each node.
        let run_roots: BTreeSet<PhysicalASAPNodeId> =
            dag.roots.iter().chain(&cut).copied().collect();
        let mut runs_of: HashMap<PhysicalASAPNodeId, usize> = HashMap::new();
        for &run_root in &run_roots {
            let mut seen = HashSet::new();
            let mut pending = vec![run_root];
            while let Some(id) = pending.pop() {
                let slot = id != run_root && (views.contains(&id) || cut.contains(&id));
                if slot || views.contains(&id) || !seen.insert(id) {
                    continue;
                }
                *runs_of.entry(id).or_default() += 1;
                pending.extend(inputs.get(&id).into_iter().flatten().copied());
            }
        }
        let several: HashSet<PhysicalASAPNodeId> = runs_of
            .into_iter()
            .filter(|&(id, runs)| runs > 1 && !is_scan(id))
            .map(|(id, _)| id)
            .collect();
        if several.is_empty() {
            return Ok(cut);
        }
        // The topmost: no input path from another of them reaches them.
        let below: HashSet<PhysicalASAPNodeId> = several
            .iter()
            .flat_map(|id| inputs.get(id).into_iter().flatten().copied())
            .collect();
        for &id in several.iter().filter(|id| !below.contains(id)) {
            let node = nodes[&id];
            let one_evaluation = matches!(node.lifetime, Lifetime::OneEvaluation(_));
            if !(one_evaluation && node.output_state.timing == ExecutionTiming::QueryTime) {
                anyhow::bail!(
                    "node {id} ({:?}, {:?}) would be computed in several queries' runs, but the plan shares only query-time OneEvaluation nodes",
                    node.output_state.timing,
                    node.lifetime
                );
            }
            cut.insert(id);
        }
    }
}

/// Whether a `Scan` is reachable from `root` without passing a view in
/// `views`: its query run reads raw data.
fn scans_below(
    dag: &PhysicalASAPDAG,
    root: PhysicalASAPNodeId,
    views: &HashSet<PhysicalASAPNodeId>,
) -> bool {
    let nodes: HashMap<PhysicalASAPNodeId, _> = dag.nodes.iter().map(|n| (n.id, n)).collect();
    let mut seen = HashSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if views.contains(&id) || !seen.insert(id) {
            continue;
        }
        if matches!(
            nodes[&id].payload,
            Operator::NonASAP(NonASAPOp::Scan { .. })
        ) {
            return true;
        }
        pending.extend(
            dag.edges
                .iter()
                .filter(|e| e.consumer == id)
                .map(|e| e.producer),
        );
    }
    false
}

/// The view slot a shared node is bound as: its whole output at the run's
/// evaluation time, refreshed on first read and kept for that evaluation;
/// a view of plan `plan_id` (`PhysicalASAPDAG::plan_id`) whose state key
/// names only that node.
fn query_time_slot(plan_id: &str, node: PhysicalASAPNodeId) -> anyhow::Result<MaterializedView> {
    Ok(serde_json::from_value(serde_json::json!({
        "node_id": node,
        "refresh": "on_first_read",
        "plan_id": plan_id,
        "view_state_key": {"definition_id": format!("query-time-result-{node}"), "window": null,
            "logical_properties": {"parameters": null}},
        "coverage": {"definition_id": format!("query-time-result-{node}"), "selection": []},
        "inputs": {"materialized_views": []},
        "retention_ms": 0,
    }))?)
}

/// The batches of `compiled`'s one root in a query run at `time_ms`.
fn run_compiled(
    compiled: &CompiledPhysicalDAG,
    store: std::sync::Arc<storage::store::RunStore>,
    time_ms: i64,
    limits: &Limits,
) -> Result<Vec<Batch>, asap_executor::Error> {
    let instance = compiled.instantiate(store)?;
    let context = RunContext::new(
        Scope::Query {
            evaluation_time_ms: time_ms,
        },
        limits.clone(),
    )?;
    block_on(async {
        let mut output = instance.execute(compiled.roots(), context)?.remove(0);
        let mut batches = Vec::new();
        while let Some(batch) = output.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                batches.push((*batch).clone());
            }
        }
        Ok(batches)
    })
}

/// The slots `root` reaches without passing another slot (itself
/// included, when it is one): the view slots of its query run.
fn slots_read(
    dag: &PhysicalASAPDAG,
    slots: &[MaterializedView],
    root: PhysicalASAPNodeId,
) -> Vec<MaterializedView> {
    let slot_ids: HashSet<PhysicalASAPNodeId> = slots.iter().map(|v| v.node_id).collect();
    let mut read = HashSet::new();
    let mut seen = HashSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if slot_ids.contains(&id) {
            read.insert(id);
            continue;
        }
        pending.extend(
            dag.edges
                .iter()
                .filter(|e| e.consumer == id)
                .map(|e| e.producer),
        );
    }
    slots
        .iter()
        .filter(|v| read.contains(&v.node_id))
        .cloned()
        .collect()
}

/// The column holding a result row's value: the one `Float64` column, or,
/// with none, the one `Int64` column (a `count` answers in integers, e.g.
/// `count by (region) (... > 1000)`).
fn value_column(schema: &Schema) -> Result<usize, Unanswered> {
    use asap_executor::planner::ir::schema::{DataType, FieldDataType};
    let columns = |dtype: DataType| -> Vec<usize> {
        schema
            .fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.dtype == FieldDataType::Plain(dtype.clone()))
            .map(|(index, _)| index)
            .collect()
    };
    match (
        columns(DataType::Float64).as_slice(),
        columns(DataType::Int64).as_slice(),
    ) {
        ([value], _) | ([], [value]) => Ok(*value),
        _ => Err(Unanswered::Unsupported(
            "result rows: expected one Float64 or, with none, one Int64 value column".into(),
        )),
    }
}

/// PromQL vector samples from a root's result batches: labels from the
/// series identity and every other non-empty text column, the value from
/// the value column ([`value_column`]).
fn result_samples(schema: &Schema, batches: &[Batch]) -> Result<Vec<ResultSample>, Unanswered> {
    let value_index = value_column(schema)?;
    let mut samples = Vec::new();
    for row in batches.iter().flat_map(Batch::to_rows) {
        // An interned input series' shared labels, until another label
        // column (or a series the query built) needs a map of its own.
        let mut shared: Option<Arc<BTreeMap<String, String>>> = None;
        let mut own: Option<BTreeMap<String, String>> = None;
        let mut value = None;
        for (index, (field, cell)) in schema.fields.iter().zip(&row).enumerate() {
            match cell {
                Value::Utf8(text) if field.name == PROMQL_SERIES_IDENTITY => {
                    // An input series' identity is interned; parse only a
                    // series the query built.
                    match storage::series_ids::by_identity(text) {
                        Some(series) => match own.as_mut() {
                            Some(own) => own.extend(
                                series
                                    .iter()
                                    .map(|(name, value)| (name.clone(), value.clone())),
                            ),
                            None => shared = Some(series.labels().clone()),
                        },
                        None => {
                            let identity =
                                storage::series_ids::result_labels(text).map_err(|e| {
                                    Unanswered::Execution(format!("series identity: {e}"))
                                })?;
                            match own.as_mut() {
                                Some(own) => own.extend(
                                    identity
                                        .iter()
                                        .map(|(name, value)| (name.clone(), value.clone())),
                                ),
                                None => shared = Some(identity),
                            }
                        }
                    }
                }
                Value::Utf8(text) if !text.is_empty() => {
                    own.get_or_insert_with(|| {
                        shared.take().map(|s| (*s).clone()).unwrap_or_default()
                    })
                    .insert(field.name.clone(), text.to_string());
                }
                Value::Float64(v) if index == value_index => value = Some(*v),
                Value::Int64(v) if index == value_index => value = Some(*v as f64),
                _ => {}
            }
        }
        let value =
            value.ok_or_else(|| Unanswered::Execution("a result row without a value".into()))?;
        let labels = match (own, shared) {
            (Some(own), _) => Arc::new(own),
            (None, Some(shared)) => shared,
            (None, None) => Arc::default(),
        };
        samples.push(ResultSample { labels, value });
    }
    Ok(samples)
}
