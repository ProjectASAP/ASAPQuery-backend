//! Query engine: answers a planned query at evaluation time `T` with one
//! query run of its root of the plan's DAG, over the store.
//!
//! Each root is compiled once, with the materialized views it reaches
//! (without passing another view) as view slots and each `Scan` it reaches
//! outside them as a raw data slot. A run reads the views' rows tiling the
//! windows its `TimeRange`s select, and the raw samples its query-time
//! `Scan`s read. A query the engine cannot answer returns [`Unanswered`];
//! the caller then forwards it to Prometheus (or refuses it).

use std::collections::{BTreeMap, HashSet};

use asap_executor::physical_planner::{compile, CompiledPhysicalDAG};
use asap_executor::planner::ir::schema::{Schema, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, Value};
use asap_planner::InstallablePlan;
use futures::executor::block_on;
use futures::StreamExt;

use crate::store::TimeIndexedStore;

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

pub struct QueryEngine {
    /// Each query, with its compiled root or why it cannot run.
    roots: Vec<(String, Result<CompiledPhysicalDAG, String>)>,
    limits: Limits,
}

impl QueryEngine {
    /// Compiles each query's root of `plan`, whose views are `views`.
    pub fn new(plan: &InstallablePlan, views: &[MaterializedView], limits: Limits) -> Self {
        let roots = plan
            .query_texts
            .iter()
            .zip(&plan.dag.roots)
            .map(|(query, &root)| {
                let read = views_read(&plan.dag, views, root);
                let compiled = compile(&plan.dag, &read, &[root as u64]).map_err(|e| e.to_string());
                (query.trim().to_string(), compiled)
            })
            .collect();
        Self { roots, limits }
    }

    /// The planned queries the engine cannot run, with why.
    pub fn unexecutable(&self) -> Vec<(String, String)> {
        self.roots
            .iter()
            .filter_map(|(query, root)| Some((query.clone(), root.as_ref().err()?.clone())))
            .collect()
    }

    /// Answers `query` at `time_ms` from `store`.
    pub fn evaluate(
        &self,
        query: &str,
        time_ms: i64,
        store: &TimeIndexedStore,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        let query = query.trim();
        let compiled = self
            .roots
            .iter()
            .find(|(planned, _)| planned == query)
            .ok_or_else(|| Unanswered::NotPlanned(query.to_string()))?
            .1
            .as_ref()
            .map_err(|reason| Unanswered::Unsupported(reason.clone()))?;
        let run_store = store.run_store();
        let batches = run(compiled, run_store.clone(), time_ms, &self.limits);
        let batches = batches.map_err(|error| match run_store.missing_view_rows() {
            Some(missing) => Unanswered::MissingViewRows(missing),
            None => Unanswered::Execution(error.to_string()),
        })?;
        let root = compiled.roots()[0];
        let schema = compiled
            .output_schema(root)
            .map_err(|e| Unanswered::Execution(e.to_string()))?;
        result_samples(&schema, &batches)
    }
}

/// The batches of `compiled`'s one root in a query run at `time_ms`.
fn run(
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

/// The views `root` reaches without passing another view (itself
/// included, when it is one): the view slots of its query run.
fn views_read(
    dag: &PhysicalASAPDAG,
    views: &[MaterializedView],
    root: PhysicalASAPNodeId,
) -> Vec<MaterializedView> {
    let view_ids: HashSet<PhysicalASAPNodeId> = views.iter().map(|v| v.node_id).collect();
    let mut read = HashSet::new();
    let mut seen = HashSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if view_ids.contains(&id) {
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
    views
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
                    let identity: BTreeMap<String, String> = serde_json::from_str(text)
                        .map_err(|e| Unanswered::Execution(format!("series identity: {e}")))?;
                    labels.extend(identity);
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
