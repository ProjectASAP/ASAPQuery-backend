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
use asap_planner::InstallablePlan;
use futures::executor::block_on;
use futures::StreamExt;

use crate::result_cache::{Answer, ResultCache};
use crate::store::{PublishedStore, QueryTimeResults, RawShards, RunStore, ViewStore};

/// One series of an instant-query result.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Why the engine does not answer a query.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Unanswered {
    #[error("the installed plan has no query {0:?}")]
    NotPlanned(String),
    #[error("the data plane cannot run this query: {0}")]
    Unsupported(String),
    /// A view's rows for a window the run reads are not stored (e.g. the
    /// first window after startup, or one before its retention).
    #[error("{0}")]
    MissingViewRows(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

/// One compiled query run: of a query's root, or of a shared node.
struct Run {
    compiled: CompiledPhysicalDAG,
    /// The shared nodes it binds as view slots.
    reads: Vec<PhysicalASAPNodeId>,
}

pub struct QueryEngine {
    /// Each query and its root, or why it cannot run.
    roots: Vec<(String, Result<PhysicalASAPNodeId, String>)>,
    /// The run of each query root and shared node.
    runs: BTreeMap<PhysicalASAPNodeId, Run>,
    /// The shared nodes: their outputs live in `results`.
    shared: BTreeSet<PhysicalASAPNodeId>,
    results: QueryTimeResults,
    limits: Limits,
    /// The query roots whose runs read raw data through a `Scan` (rather
    /// than only materialized views).
    reads_raw_data: BTreeSet<PhysicalASAPNodeId>,
    /// Final answers of the query roots by evaluation time; `None`: off.
    result_cache: Option<ResultCache>,
}

impl QueryEngine {
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
        results: QueryTimeResults,
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
        for &node in &shared {
            slots.push(query_time_slot(node)?);
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
        let roots = plan
            .query_texts
            .iter()
            .zip(&dag.roots)
            .map(|(query, &root)| {
                let runnable = runnable(root, &compiled).map(|()| root);
                (query.trim().to_string(), runnable)
            })
            .collect();
        let runs = compiled
            .into_iter()
            .filter_map(|(node, run)| Some((node, run.ok()?)))
            .collect();
        Ok(Self {
            roots,
            runs,
            shared,
            results,
            limits,
            reads_raw_data,
            result_cache,
        })
    }

    /// The planned queries the engine cannot run, with why.
    pub fn unexecutable(&self) -> Vec<(String, String)> {
        self.roots
            .iter()
            .filter_map(|(query, root)| Some((query.clone(), root.as_ref().err()?.clone())))
            .collect()
    }

    /// The query-time partition of the view store.
    pub fn query_time_results(&self) -> &QueryTimeResults {
        &self.results
    }

    /// The query-time `OneEvaluation` nodes several queries' runs share.
    pub fn shared_nodes(&self) -> &BTreeSet<PhysicalASAPNodeId> {
        &self.shared
    }

    /// The planned `query`'s root.
    fn root(&self, query: &str) -> Result<PhysicalASAPNodeId, Unanswered> {
        let query = query.trim();
        Ok(*self
            .roots
            .iter()
            .find(|(planned, _)| planned == query)
            .ok_or_else(|| Unanswered::NotPlanned(query.to_string()))?
            .1
            .as_ref()
            .map_err(|reason| Unanswered::Unsupported(reason.clone()))?)
    }

    /// The result cache's answer to `query` at `time_ms`, if it keeps one.
    pub fn cached(&self, query: &str, time_ms: i64) -> Option<Answer> {
        let cache = self.result_cache.as_ref()?;
        cache.get(self.root(query).ok()?, time_ms)
    }

    /// The result cache, when it is on.
    pub fn result_cache(&self) -> Option<&ResultCache> {
        self.result_cache.as_ref()
    }

    /// Answers `query` at `time_ms` from the newest snapshot of `store`,
    /// and keeps the answer in the result cache when it is final: the run
    /// succeeded, so every view window it read was stored (a window is
    /// appended once, when the watermark closes it, and never changes),
    /// and, when the root reads raw data through a `Scan`, `time_ms` is at
    /// or before the watermark the snapshot was published with (no sample
    /// at or before it is expected any more). The result cache is not
    /// read here: callers read it first ([`Self::cached`]).
    pub fn evaluate(
        &self,
        query: &str,
        time_ms: i64,
        store: &PublishedStore,
    ) -> Result<Answer, Unanswered> {
        let root = self.root(query)?;
        // The watermark first: raw data is published before the watermark
        // that covers it, so the snapshot read next holds every sample at
        // or before this watermark.
        let watermark_ms = store.watermark_ms();
        let (raw_data, views) = store.snapshot();
        let snapshot = Snapshot {
            raw_data,
            views,
            watermark_ms,
        };
        let batches = self.output(root, time_ms, &snapshot)?;
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
            shared,
        ));
        run_compiled(&run.compiled, run_store.clone(), time_ms, &self.limits).map_err(|error| {
            match run_store.missing_view_rows() {
                Some(missing) => Unanswered::MissingViewRows(missing),
                None => Unanswered::Execution(error.to_string()),
            }
        })
    }
}

/// What one evaluation reads: the raw data and views published when it
/// started, and the watermark then.
struct Snapshot {
    raw_data: RawShards,
    views: Arc<ViewStore>,
    watermark_ms: Option<i64>,
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
/// evaluation time, refreshed on first read and kept for that evaluation.
fn query_time_slot(node: PhysicalASAPNodeId) -> anyhow::Result<MaterializedView> {
    Ok(serde_json::from_value(serde_json::json!({
        "node_id": node,
        "refresh": "on_first_read",
        "coverage": {"definition_id": format!("query-time-result-{node}"), "selection": []},
        "inputs": {"materialized_views": []},
        "retention_ms": 0,
    }))?)
}

/// The batches of `compiled`'s one root in a query run at `time_ms`.
fn run_compiled(
    compiled: &CompiledPhysicalDAG,
    store: std::sync::Arc<crate::store::RunStore>,
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

/// PromQL vector samples from a root's result batches: labels from the
/// series identity and every other non-empty text column, the value from
/// the one `Float64` column.
fn result_samples(schema: &Schema, batches: &[Batch]) -> Result<Vec<ResultSample>, Unanswered> {
    let floats = schema
        .fields
        .iter()
        .filter(|f| {
            f.dtype
                == asap_executor::planner::ir::schema::FieldDataType::Plain(
                    asap_executor::planner::ir::schema::DataType::Float64,
                )
        })
        .count();
    if floats != 1 {
        return Err(Unanswered::Unsupported(
            "result rows: expected one Float64 value column".into(),
        ));
    }
    let mut samples = Vec::new();
    for row in batches.iter().flat_map(Batch::to_rows) {
        let mut labels = BTreeMap::new();
        let mut value = None;
        for (field, cell) in schema.fields.iter().zip(&row) {
            match cell {
                Value::Utf8(text) if field.name == PROMQL_SERIES_IDENTITY => {
                    // An input series' identity is interned; parse only a
                    // series the query built.
                    match crate::series_ids::by_identity(text) {
                        Some(series) => labels.extend(
                            series
                                .iter()
                                .map(|(name, value)| (name.clone(), value.clone())),
                        ),
                        None => {
                            let identity: BTreeMap<String, String> = serde_json::from_str(text)
                                .map_err(|e| {
                                    Unanswered::Execution(format!("series identity: {e}"))
                                })?;
                            labels.extend(identity);
                        }
                    }
                }
                Value::Utf8(text) if !text.is_empty() => {
                    labels.insert(field.name.clone(), text.to_string());
                }
                Value::Float64(v) => value = Some(*v),
                _ => {}
            }
        }
        let value =
            value.ok_or_else(|| Unanswered::Execution("a result row without a value".into()))?;
        samples.push(ResultSample { labels, value });
    }
    Ok(samples)
}
