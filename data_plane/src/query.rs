//! Query engine: answers a planned query at evaluation time `T` by running
//! its root of the plan's DAG over stored panes and raw samples.
//!
//! Each root is compiled once with three kinds of inputs:
//! - stored outputs (the SDS frontier): bound to the pane of their definition
//!   ending at `T - offset`, which must be stored;
//! - kept outputs (`MatAtQueryKeptAcrossEvaluations`): bound to the kept
//!   pane of their definition ending at `T - offset`. A missing one is built
//!   from its pane's raw samples, which the caller fetches, and kept for the
//!   next evaluations; kept panes are evicted like stored ones, measured
//!   from the newest evaluation;
//! - query-time raw ranges, `TimeRange(range) <- TimeShift(offset)* <- Scan`:
//!   bound to the samples in `(T - offset - range, T - offset]`, which the
//!   caller fetches, filtered by the plan's `Scan`.
//!
//! A query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::promql_rows::{
    decode_series_identity, SERIES_IDENTITY_COLUMN,
};
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, Value};
use control_plane::InstalledPlan;
use planner_types::ir::physical_export::PhysicalASAPDAG;
use planner_types::ir::schema::{DataType, FieldDataType, Schema};
use planner_types::ir::{NonASAPOp, Operator as IrOperator};

use crate::precompute::{run, scan_below, select, PaneBuild, RawSample, ScanInput};
use crate::sds::{DefinitionId, Sds, SummaryStore};

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

struct KeptInput {
    node: u64,
    definition: DefinitionId,
    offset_ms: i64,
    /// The output's own `TimeRange`, which its raw samples feed.
    raw_input: u64,
}

struct RawInput {
    node: u64,
    scan: ScanInput,
    range_ms: i64,
    offset_ms: i64,
}

struct Program {
    compiled: CompiledPhysicalDAG,
    stored: Vec<StoredInput>,
    kept: Vec<KeptInput>,
    raw: Vec<RawInput>,
    output: Schema,
}

/// What happened to the kept panes evaluations read.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct KeptResultStats {
    /// Kept panes computed and kept, because no evaluation had kept them.
    pub built: u64,
    /// Kept panes read from an earlier evaluation instead of recomputed.
    pub reused: u64,
}

pub struct QueryEngine {
    /// Kept panes (`MatAtQueryKeptAcrossEvaluations`), by definition and
    /// pane end, in memory.
    pub kept: SummaryStore<Batch>,
    /// How many kept panes evaluations built and reused.
    pub kept_stats: KeptResultStats,
    /// Newest evaluation time, from which kept panes are evicted.
    newest_evaluation_ms: Option<i64>,
    /// How each kept definition's panes are built, or why they cannot be.
    kept_builds: BTreeMap<DefinitionId, Result<PaneBuild, String>>,
    dag: PhysicalASAPDAG,
    /// Each query, with its compiled root or why it cannot run.
    roots: Vec<(String, Result<Program, String>)>,
}

impl QueryEngine {
    pub fn new(plan: &InstalledPlan, sds: &Sds) -> Self {
        let dag = plan.dag.dag.clone();
        let kept_builds: BTreeMap<_, _> = sds
            .kept_definitions
            .values()
            .map(|definition| {
                let build = PaneBuild::new(&dag, definition, &sds.kept_outputs);
                (definition.id.clone(), build.map_err(|e| e.to_string()))
            })
            .collect();
        let roots = plan
            .queries
            .iter()
            .zip(&dag.roots)
            .map(|(query, root)| {
                let program = program(&dag, sds, &kept_builds, *root as u64);
                (query.trim().to_string(), program)
            })
            .collect();
        Self {
            kept: SummaryStore::new(),
            kept_stats: KeptResultStats::default(),
            newest_evaluation_ms: None,
            kept_builds,
            dag,
            roots,
        }
    }

    /// The root answering `query` at `time_ms`, and the raw samples it needs:
    /// its query-time ranges and the panes of the kept outputs no evaluation
    /// has kept. Fails if the query is not planned or a stored pane it reads
    /// is missing.
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
        let mut ranges: Vec<RawRange> = program
            .raw
            .iter()
            .map(|input| RawRange {
                node: input.node,
                metric: input.scan.metric.clone(),
                start_ms: time_ms - input.offset_ms - input.range_ms,
                end_ms: time_ms - input.offset_ms,
            })
            .collect();
        for input in &program.kept {
            let end_ms = time_ms - input.offset_ms;
            if self.kept.get(&input.definition, end_ms).is_none() {
                let build = self.kept_build(&input.definition);
                ranges.push(RawRange {
                    node: input.raw_input,
                    metric: build.scan.metric.clone(),
                    start_ms: end_ms - build.definition.pane_ms,
                    end_ms,
                });
            }
        }
        Ok((root, ranges))
    }

    /// Runs `root` at `time_ms` over stored panes, kept panes and `raw`, the
    /// samples of each of its [`RawRange`]s by node. A kept pane that is
    /// missing is built from its raw samples and kept; then kept panes no
    /// evaluation after the newest one reads are evicted.
    pub fn evaluate(
        &mut self,
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
        for input in &program.kept {
            let end_ms = time_ms - input.offset_ms;
            let pane = match self.kept.get(&input.definition, end_ms) {
                Some(pane) => {
                    self.kept_stats.reused += 1;
                    pane.clone()
                }
                None => {
                    // `raw_ranges` asked for these samples unless the pane
                    // was kept then and evicted since.
                    let samples = raw.get(&input.raw_input).ok_or_else(|| {
                        Unanswered::MissingPane(format!(
                            "kept pane ending at {end_ms} of {:?} was evicted",
                            input.definition
                        ))
                    })?;
                    // A kept pane is computed as at its end, so every
                    // evaluation that reads it reads the same pane.
                    let pane_scope = Scope::Query {
                        evaluation_time_ms: end_ms,
                        revision: 1,
                    };
                    let pane = self
                        .kept_build(&input.definition)
                        .run(&self.dag, samples, pane_scope)
                        .map_err(|e| Unanswered::Execution(e.to_string()))?;
                    self.kept.insert(&input.definition, end_ms, pane.clone());
                    self.kept_stats.built += 1;
                    pane
                }
            };
            let source = Operator::source(pane.schema().clone(), vec![pane]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        for input in &program.raw {
            let samples = raw.get(&input.node).map(Vec::as_slice).unwrap_or_default();
            let rows = select(&self.dag, &input.scan, samples, scope.clone()).map_err(failed)?;
            let source = Operator::source(input.scan.schema.clone(), vec![rows]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        // Evict the kept panes older than each definition's retention
        // before the newest evaluation, this one or a later one.
        let newest = time_ms.max(self.newest_evaluation_ms.unwrap_or(time_ms));
        self.newest_evaluation_ms = Some(newest);
        for (definition, build) in &self.kept_builds {
            if let Ok(build) = build {
                self.kept
                    .evict_through(definition, newest - build.retention_ms);
            }
        }
        let dag = program.compiled.instantiate(sources).map_err(failed)?;
        let rows = run(&dag, program.compiled.roots(), scope).map_err(failed)?;
        result_samples(&program.output, rows)
    }

    /// The build of a kept definition a compiled root reads; roots whose
    /// kept builds failed are not compiled.
    fn kept_build(&self, definition: &DefinitionId) -> &PaneBuild {
        self.kept_builds[definition]
            .as_ref()
            .expect("compiled roots read only kept definitions that build")
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
    store.get(&input.definition, end).ok_or_else(|| {
        Unanswered::MissingPane(format!(
            "pane ending at {end} of {:?} is not stored",
            input.definition
        ))
    })
}

/// Compiles `root` with its stored, kept and raw inputs, or says why it cannot.
fn program(
    dag: &PhysicalASAPDAG,
    sds: &Sds,
    kept_builds: &BTreeMap<DefinitionId, Result<PaneBuild, String>>,
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
    let (mut stored, mut kept, mut raw) = (Vec::new(), Vec::new(), Vec::new());
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
        if let Some((definition, binding)) = sds.kept_outputs.get(&id) {
            kept_builds[definition].as_ref().map_err(Clone::clone)?;
            kept.push(KeptInput {
                node: id,
                definition: definition.clone(),
                offset_ms: binding.offset_ms,
                raw_input: binding.raw_input,
            });
            continue;
        }
        match &nodes[&id].payload {
            IrOperator::NonASAP(NonASAPOp::TimeRange { range, .. }) => {
                raw.push(raw_input(
                    dag,
                    &nodes,
                    &producers,
                    id,
                    range.as_millis() as i64,
                )?);
                continue;
            }
            IrOperator::NonASAP(NonASAPOp::Scan { .. }) => {
                return Err(format!("node {id}: a Scan read without a TimeRange"))
            }
            _ => pending.extend(producers.get(&id).into_iter().flatten().copied()),
        }
    }
    let mut inputs = BTreeMap::new();
    for node in stored
        .iter()
        .map(|i| i.node)
        .chain(kept.iter().map(|i| i.node))
    {
        let schema = Arc::new(nodes[&node].output_schema.clone());
        inputs.insert(node, InputContract::bounded(schema));
    }
    for input in &raw {
        inputs.insert(
            input.node,
            InputContract::bounded(input.scan.schema.clone()),
        );
    }
    let compiled = compile(dag, inputs, &[root]).map_err(|e| e.to_string())?;
    Ok(Program {
        compiled,
        stored,
        kept,
        raw,
        output: nodes[&root].output_schema.clone(),
    })
}

/// A query-time `TimeRange`: the `Scan` below it and the `TimeShift`
/// offsets between them.
fn raw_input(
    dag: &PhysicalASAPDAG,
    nodes: &HashMap<u64, &planner_types::ir::physical_export::PhysicalASAPDAGNode>,
    producers: &HashMap<u64, Vec<u64>>,
    time_range: u64,
    range_ms: i64,
) -> Result<RawInput, String> {
    let unsupported = |reason: &str| format!("node {time_range}: {reason}");
    let mut offset_ms = 0;
    let mut current = time_range;
    loop {
        current = match producers.get(&current).map(Vec::as_slice) {
            Some([input]) => *input,
            _ => return Err(unsupported("a TimeRange must read one input")),
        };
        match &nodes[&current].payload {
            IrOperator::NonASAP(NonASAPOp::TimeShift { shift, .. }) if shift.at.is_none() => {
                offset_ms += shift.offset_ms
            }
            IrOperator::NonASAP(NonASAPOp::Scan { .. }) => break,
            _ => {
                return Err(unsupported(
                    "only offset TimeShifts between TimeRange and Scan",
                ))
            }
        }
    }
    let scan = scan_below(dag, time_range).ok_or_else(|| unsupported("not a time-series Scan"))?;
    if Arc::new(nodes[&time_range].output_schema.clone()) != scan.schema {
        return Err(unsupported("TimeRange rows differ from its Scan's"));
    }
    Ok(RawInput {
        node: time_range,
        scan,
        range_ms,
        offset_ms,
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
            let mut labels = BTreeMap::new();
            for (field, cell) in schema.fields.iter().zip(&row) {
                let Value::Utf8(text) = cell else { continue };
                if field.name == SERIES_IDENTITY_COLUMN {
                    let identity = decode_series_identity(text)
                        .map_err(|e| Unanswered::Execution(e.to_string()))?;
                    labels.extend(identity);
                } else if !text.is_empty() {
                    labels.insert(field.name.clone(), text.to_string());
                }
            }
            let value = match &row[value] {
                Value::Float64(v) => *v,
                other => return Err(Unanswered::Execution(format!("value {other:?}"))),
            };
            Ok(ResultSample { labels, value })
        })
        .collect()
}
