//! Query engine: answers a planned query at evaluation time `T` by running
//! its root of the plan's DAG over stored panes and raw samples.
//!
//! Each root is compiled once with two kinds of inputs:
//! - stored outputs (the SDS frontier): bound to the pane of their definition
//!   ending at `T - offset`, which must be stored;
//! - query-time raw ranges, `TimeRange(range) <- TimeShift(offset)* <- Scan`:
//!   bound to the samples in `(T - offset - range, T - offset]`, which the
//!   caller fetches, filtered by the plan's `Scan`.
//!
//! A query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::{PhysicalASAPDAG, RawSampleWindow};
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema};
use asap_executor::planner::ir::{NonASAPOp, Operator as IrOperator};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::{series_labels_of_output_row, RawSampleSelector};
use asap_planner::InstallablePlan;

use crate::precompute::{executor_samples, run_root, RawSample};
use crate::sds::{DefinitionId, Sds, Selection, SummaryStore};

/// Raw samples a query needs: `metric`'s samples in `(start_ms, end_ms]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRange {
    pub node: u64,
    pub metric: String,
    pub start_ms: i64,
    pub end_ms: i64,
}

/// One series of an instant-query result.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultSample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// Why the engine does not answer a query; it is then forwarded.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Unanswered {
    #[error("the installed plan has no query {0:?}")]
    NotPlanned(String),
    #[error("the data plane cannot run this query: {0}")]
    Unsupported(String),
    #[error("{0}")]
    MissingPane(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

struct StoredInput {
    node: u64,
    definition: DefinitionId,
    offset_ms: i64,
    pane_ms: i64,
}

struct RawInput {
    node: u64,
    /// The plan's `Scan` label matchers over the range's samples.
    selector: RawSampleSelector,
    schema: SchemaRef,
    range_ms: i64,
    offset_ms: i64,
}

struct Program {
    compiled: CompiledPhysicalDAG,
    stored: Vec<StoredInput>,
    raw: Vec<RawInput>,
    output: Schema,
}

pub struct QueryEngine {
    /// Each query, with its compiled root or why it cannot run.
    roots: Vec<(String, Result<Program, String>)>,
}

impl QueryEngine {
    pub fn new(plan: &InstallablePlan, sds: &Sds) -> Self {
        let dag = &plan.dag.dag;
        let windows: HashMap<u64, &RawSampleWindow> = plan
            .query_time_raw_windows
            .iter()
            .map(|w| (w.time_range_node_id as u64, w))
            .collect();
        let roots = plan
            .queries
            .iter()
            .map(|q| {
                let program = program(dag, sds, &windows, q.root_node_id as u64);
                (q.query.trim().to_string(), program)
            })
            .collect();
        Self { roots }
    }

    /// The root answering `query` at `time_ms`, and the raw samples it needs.
    /// Fails if the query is not planned or a stored pane it reads is missing.
    pub fn raw_ranges(
        &self,
        query: &str,
        time_ms: i64,
        store: &SummaryStore<Batch>,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        let (root, program) = self.program(query)?;
        for input in &program.stored {
            pane(input, time_ms, store)?;
        }
        let ranges = program
            .raw
            .iter()
            .map(|input| RawRange {
                node: input.node,
                metric: input.selector.metric().to_string(),
                start_ms: time_ms - input.offset_ms - input.range_ms,
                end_ms: time_ms - input.offset_ms,
            })
            .collect();
        Ok((root, ranges))
    }

    /// Runs `root` at `time_ms` over stored panes and `raw`, the samples of
    /// each of its [`RawRange`]s by node.
    pub fn evaluate(
        &self,
        root: usize,
        time_ms: i64,
        store: &SummaryStore<Batch>,
        raw: &BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<ResultSample>, Unanswered> {
        let program = self.roots[root]
            .1
            .as_ref()
            .map_err(|reason| Unanswered::Unsupported(reason.clone()))?;
        let failed = |e: asap_executor::Error| Unanswered::Execution(e.to_string());
        let scope = Scope::Query {
            evaluation_time_ms: time_ms,
            revision: 1,
        };
        let mut sources: BTreeMap<u64, Source<'_>> = BTreeMap::new();
        for input in &program.stored {
            let pane = pane(input, time_ms, store)?;
            let source =
                Operator::source(pane.schema().clone(), vec![pane.clone()]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        for input in &program.raw {
            let samples = raw.get(&input.node).map(Vec::as_slice).unwrap_or_default();
            let rows = input
                .selector
                .window_batch(time_ms - input.offset_ms, &executor_samples(samples))
                .map_err(failed)?;
            let source = Operator::source(input.schema.clone(), vec![rows]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        let dag = program.compiled.instantiate(sources).map_err(failed)?;
        let rows = run_root(&dag, program.compiled.roots()[0], scope).map_err(failed)?;
        result_samples(&program.output, rows)
    }

    fn program(&self, query: &str) -> Result<(usize, &Program), Unanswered> {
        let query = query.trim();
        let root = self
            .roots
            .iter()
            .position(|(planned, _)| planned == query)
            .ok_or_else(|| Unanswered::NotPlanned(query.to_string()))?;
        match &self.roots[root].1 {
            Ok(program) => Ok((root, program)),
            Err(reason) => Err(Unanswered::Unsupported(reason.clone())),
        }
    }
}

/// The stored pane `input` reads at `time_ms`.
fn pane<'a>(
    input: &StoredInput,
    time_ms: i64,
    store: &'a SummaryStore<Batch>,
) -> Result<&'a Batch, Unanswered> {
    let end = time_ms - input.offset_ms;
    if end.rem_euclid(input.pane_ms) != 0 {
        return Err(Unanswered::MissingPane(format!(
            "{time_ms} is not on the {} ms pane grid",
            input.pane_ms
        )));
    }
    store
        .get(&input.definition, Selection::ending_at(end))
        .ok_or_else(|| {
            Unanswered::MissingPane(format!(
                "pane ending at {end} of {:?} is not stored",
                input.definition
            ))
        })
}

/// Compiles `root` with its stored and raw inputs, or says why it cannot.
fn program(
    dag: &PhysicalASAPDAG,
    sds: &Sds,
    windows: &HashMap<u64, &RawSampleWindow>,
    root: u64,
) -> Result<Program, String> {
    let nodes: HashMap<u64, _> = dag.nodes.iter().map(|n| (n.id as u64, n)).collect();
    let mut producers: HashMap<u64, Vec<u64>> = HashMap::new();
    for edge in &dag.edges {
        producers
            .entry(edge.consumer as u64)
            .or_default()
            .push(edge.producer as u64);
    }
    let (mut stored, mut raw) = (Vec::new(), Vec::new());
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if let Some((definition, binding)) = sds.outputs.get(&id) {
            stored.push(StoredInput {
                node: id,
                definition: definition.clone(),
                offset_ms: binding.offset_ms,
                pane_ms: sds.definitions[definition].pane_ms,
            });
            continue;
        }
        match &nodes[&id].payload {
            IrOperator::NonASAP(NonASAPOp::TimeRange { .. }) => {
                let window = windows.get(&id).ok_or_else(|| {
                    format!("node {id}: a TimeRange the plan lists no window for")
                })?;
                let selector =
                    RawSampleSelector::new(dag, window).map_err(|e| format!("node {id}: {e}"))?;
                raw.push(RawInput {
                    node: id,
                    selector,
                    schema: Arc::new(nodes[&id].output_schema.clone()),
                    range_ms: window.width_ms,
                    offset_ms: window.end_offset_ms,
                });
                continue;
            }
            IrOperator::NonASAP(NonASAPOp::Scan { .. }) => {
                return Err(format!("node {id}: a Scan read without a TimeRange"))
            }
            _ => pending.extend(producers.get(&id).into_iter().flatten().copied()),
        }
    }
    let mut inputs = BTreeMap::new();
    for input in &stored {
        let schema = Arc::new(nodes[&input.node].output_schema.clone());
        inputs.insert(input.node, InputContract::bounded(schema));
    }
    for input in &raw {
        inputs.insert(input.node, InputContract::bounded(input.schema.clone()));
    }
    let compiled = compile(dag, inputs, &[root]).map_err(|e| e.to_string())?;
    Ok(Program {
        compiled,
        stored,
        raw,
        output: nodes[&root].output_schema.clone(),
    })
}

/// PromQL vector samples from result rows: labels from the series identity
/// and the other text columns, the value from the one `Float64` column.
fn result_samples(schema: &Schema, rows: Vec<Vec<Value>>) -> Result<Vec<ResultSample>, Unanswered> {
    let unsupported = |reason: &str| Unanswered::Unsupported(format!("result rows: {reason}"));
    let values: Vec<usize> = schema
        .fields
        .iter()
        .enumerate()
        .filter(|(_, f)| f.dtype == FieldDataType::Plain(DataType::Float64))
        .map(|(i, _)| i)
        .collect();
    let [value] = values[..] else {
        return Err(unsupported("expected one Float64 value column"));
    };
    rows.into_iter()
        .map(|row| {
            let labels = series_labels_of_output_row(schema, &row)
                .map_err(|e| Unanswered::Execution(e.to_string()))?;
            let value = match &row[value] {
                Value::Float64(v) => *v,
                other => return Err(Unanswered::Execution(format!("value {other:?}"))),
            };
            Ok(ResultSample { labels, value })
        })
        .collect()
}
