//! Query engine: answers a planned query at evaluation time `T` by running
//! its root of the plan's DAG over stored panes and raw samples.
//!
//! Each root is compiled once with three kinds of inputs:
//! - stored outputs built at ingestion time: bound to the pane of their definition
//!   ending at `T - offset`, which must be stored;
//! - kept outputs (`MatAtQueryKeptAcrossEvaluations`): bound to the kept
//!   pane of their definition ending at `T - offset`. A missing one is built
//!   from its pane's raw samples, which the caller fetches, and kept for the
//!   next evaluations once it is closed (its end at or before the
//!   precompute engine's watermark); kept panes are evicted like stored
//!   ones, measured from the newest evaluation;
//! - query-time raw windows (`InstallablePlan::query_time_raw_windows`):
//!   bound to the samples in `(T - offset - width, T - offset]`, which the
//!   caller fetches, selected by the executor's `RawSampleSelector` (the
//!   plan's `Scan` matchers, and the latest sample per series for an
//!   instant selector).
//!
//! A range query is one evaluation per step, in time order. Each raw range
//! is fetched once for all steps and sliced per step, and each kept
//! definition's missing panes once, so a kept pane built at one step is
//! reused by the later ones.
//!
//! A query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::RawSampleWindow;
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema};
use asap_executor::planner::ir::{NonASAPOp, Operator as IrOperator};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, Value};
use asap_executor::{series_labels_of_output_row, RawSampleSelector, Sample};
use asap_planner::{InstallablePlan, PhysicalASAPDAG};

use crate::precompute::{output_schema, run, PaneBuild, RawSample};
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
    /// The window's `TimeRange`, which its rows feed.
    node: u64,
    window: RawSampleWindow,
    selector: RawSampleSelector,
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
    /// Kept panes computed from raw samples, because no evaluation had kept
    /// them; kept if closed.
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
    /// Each query, with its compiled root or why it cannot run.
    roots: Vec<(String, Result<Program, String>)>,
}

impl QueryEngine {
    pub fn new(plan: &InstallablePlan, sds: &Sds) -> Self {
        let dag = &plan.dag.dag;
        let kept_builds: BTreeMap<_, _> = sds
            .kept_definitions
            .values()
            .map(|definition| {
                let build = PaneBuild::new(dag, definition);
                (definition.id.clone(), build.map_err(|e| e.to_string()))
            })
            .collect();
        let windows: HashMap<u64, &RawSampleWindow> = plan
            .query_time_raw_windows
            .iter()
            .map(|window| (window.time_range_node_id as u64, window))
            .collect();
        let roots = plan
            .queries
            .iter()
            .map(|query| {
                let root = query.root_node_id as u64;
                let program = program(dag, sds, &kept_builds, &windows, root);
                (query.query.trim().to_string(), program)
            })
            .collect();
        Self {
            kept: SummaryStore::new(),
            kept_stats: KeptResultStats::default(),
            newest_evaluation_ms: None,
            kept_builds,
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
            .map(|input| {
                let (start_ms, end_ms) = input.window.bounds_ms(time_ms);
                RawRange {
                    node: input.node,
                    metric: input.selector.metric().to_string(),
                    start_ms,
                    end_ms,
                }
            })
            .collect();
        for input in &program.kept {
            let end_ms = time_ms - input.offset_ms;
            if self.kept.get(&input.definition, end_ms).is_none() {
                let build = self.kept_build(&input.definition);
                ranges.push(RawRange {
                    node: input.raw_input,
                    metric: build.selector.metric().to_string(),
                    start_ms: end_ms - build.definition.pane_ms,
                    end_ms,
                });
            }
        }
        Ok((root, ranges))
    }

    /// Runs `root` at `time_ms` over stored panes, kept panes and `raw`, the
    /// samples of each of its [`RawRange`]s by node. A kept pane that is
    /// missing is built from its raw samples, and kept if it is closed: its
    /// end is at or before `watermark_ms`, the precompute engine's watermark
    /// (`None` before any sample). Then kept panes no evaluation after the
    /// newest one reads are evicted.
    pub fn evaluate(
        &mut self,
        root: usize,
        time_ms: i64,
        store: &SummaryStore<Batch>,
        watermark_ms: Option<i64>,
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
                        .run(&samples_of(samples), end_ms, pane_scope)
                        .map_err(|e| Unanswered::Execution(e.to_string()))?;
                    self.kept_stats.built += 1;
                    // Kept only once closed, as the precompute engine closes
                    // panes: a sample may still arrive for an open one.
                    if watermark_ms.is_some_and(|watermark| end_ms <= watermark) {
                        self.kept.insert(&input.definition, end_ms, pane.clone());
                    }
                    pane
                }
            };
            let source = Operator::source(pane.schema().clone(), vec![pane]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        for input in &program.raw {
            let samples = raw.get(&input.node).map(Vec::as_slice).unwrap_or_default();
            let (_, end_ms) = input.window.bounds_ms(time_ms);
            let rows = input
                .selector
                .rows(end_ms, &samples_of(samples))
                .map_err(failed)?;
            let source = Operator::source(rows.schema().clone(), vec![rows]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        // Evict the kept panes older than each definition's retention
        // before the newest evaluation, this one or a later one.
        let newest = time_ms.max(self.newest_evaluation_ms.unwrap_or(time_ms));
        self.newest_evaluation_ms = Some(newest);
        for (definition, build) in &self.kept_builds {
            if let Ok(build) = build {
                let oldest = build.definition.oldest_kept_end_ms(newest);
                self.kept.evict_before(definition, oldest);
            }
        }
        let dag = program.compiled.instantiate(sources).map_err(failed)?;
        let rows = run(&dag, program.compiled.roots(), scope).map_err(failed)?;
        result_samples(&program.output, rows)
    }

    /// The root answering `query` at each of `steps` (ascending) and the
    /// raw samples they need, each fetched once for all steps: per
    /// query-time raw range, `(first step - offset - range, last step -
    /// offset]`; per kept definition, the span of its panes no evaluation
    /// has kept. Fails if the query is not planned or a stored pane any step
    /// reads is missing.
    pub fn raw_ranges_for_steps(
        &self,
        query: &str,
        steps: &[i64],
        store: &SummaryStore<Batch>,
    ) -> Result<(usize, Vec<RawRange>), Unanswered> {
        let (root, program) = self.program(query)?;
        let (Some(&first), Some(&last)) = (steps.first(), steps.last()) else {
            return Ok((root, Vec::new()));
        };
        for &time_ms in steps {
            for input in &program.stored {
                pane(input, time_ms, store)?;
            }
        }
        let mut ranges: Vec<RawRange> = program
            .raw
            .iter()
            .map(|input| RawRange {
                node: input.node,
                metric: input.selector.metric().to_string(),
                start_ms: input.window.bounds_ms(first).0,
                end_ms: input.window.bounds_ms(last).1,
            })
            .collect();
        // The first and last end of each kept definition's missing panes.
        let mut missing: BTreeMap<&DefinitionId, (i64, i64)> = BTreeMap::new();
        for &time_ms in steps {
            for input in &program.kept {
                let end_ms = time_ms - input.offset_ms;
                if self.kept.get(&input.definition, end_ms).is_none() {
                    let span = missing.entry(&input.definition).or_insert((end_ms, end_ms));
                    *span = (span.0.min(end_ms), span.1.max(end_ms));
                }
            }
        }
        for (definition, (first_end_ms, last_end_ms)) in missing {
            let build = self.kept_build(definition);
            ranges.push(RawRange {
                node: build.definition.raw_input(),
                metric: build.selector.metric().to_string(),
                start_ms: first_end_ms - build.definition.pane_ms,
                end_ms: last_end_ms,
            });
        }
        Ok((root, ranges))
    }

    /// Runs `root` at each of `steps`, in order, as [`Self::evaluate`] does,
    /// each over its slice of `raw` (the samples of each
    /// [`Self::raw_ranges_for_steps`] range, by node). A kept pane built at
    /// one step is reused by the later ones. `results[k]` answers
    /// `steps[k]`; any step that fails fails the whole call. Every step
    /// keeps a pane only if it is closed at `watermark_ms`, so a pane still
    /// open is recomputed at each step that reads it.
    pub fn evaluate_steps(
        &mut self,
        root: usize,
        steps: &[i64],
        store: &SummaryStore<Batch>,
        watermark_ms: Option<i64>,
        raw: &BTreeMap<u64, Vec<RawSample>>,
    ) -> Result<Vec<Vec<ResultSample>>, Unanswered> {
        let program = self.roots[root]
            .1
            .as_ref()
            .map_err(|reason| Unanswered::Unsupported(reason.clone()))?;
        // Each query-time raw range: its node, offset and range.
        let raw_inputs: Vec<(u64, i64, i64)> = program
            .raw
            .iter()
            .map(|input| {
                (
                    input.node,
                    input.window.end_offset_ms,
                    input.window.width_ms,
                )
            })
            .collect();
        // Each kept output: its definition and offset, the node its pane's
        // samples feed, and the node its definition's samples were fetched for.
        let kept_inputs: Vec<(DefinitionId, i64, u64, u64, i64)> = program
            .kept
            .iter()
            .map(|input| {
                let definition = &self.kept_build(&input.definition).definition;
                (
                    input.definition.clone(),
                    input.offset_ms,
                    input.raw_input,
                    definition.raw_input(),
                    definition.pane_ms,
                )
            })
            .collect();
        let series: BTreeMap<u64, Vec<&[RawSample]>> = raw
            .iter()
            .map(|(node, samples)| (*node, series_runs(samples)))
            .collect();
        let mut results = Vec::with_capacity(steps.len());
        for &time_ms in steps {
            let mut step_raw = BTreeMap::new();
            for &(node, offset_ms, range_ms) in &raw_inputs {
                if let Some(runs) = series.get(&node) {
                    let end_ms = time_ms - offset_ms;
                    step_raw.insert(node, slice(runs, end_ms - range_ms, end_ms));
                }
            }
            for (definition, offset_ms, node, fetched, pane_ms) in &kept_inputs {
                let end_ms = time_ms - offset_ms;
                if self.kept.get(definition, end_ms).is_none() {
                    if let Some(runs) = series.get(fetched) {
                        step_raw.insert(*node, slice(runs, end_ms - pane_ms, end_ms));
                    }
                }
            }
            results.push(self.evaluate(root, time_ms, store, watermark_ms, &step_raw)?);
        }
        Ok(results)
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

/// `samples` split into runs of one series in time order, as Prometheus
/// returns them.
fn series_runs(samples: &[RawSample]) -> Vec<&[RawSample]> {
    samples
        .chunk_by(|a, b| {
            (Arc::ptr_eq(&a.labels, &b.labels) || a.labels == b.labels)
                && a.timestamp_ms <= b.timestamp_ms
        })
        .collect()
}

/// The samples of `runs` in `(start_ms, end_ms]`, in their order.
fn slice(runs: &[&[RawSample]], start_ms: i64, end_ms: i64) -> Vec<RawSample> {
    let mut samples = Vec::new();
    for run in runs {
        let from = run.partition_point(|s| s.timestamp_ms <= start_ms);
        let to = run.partition_point(|s| s.timestamp_ms <= end_ms);
        samples.extend_from_slice(&run[from..to]);
    }
    samples
}

/// `samples` as the executor's samples, for a `RawSampleSelector`.
fn samples_of(samples: &[RawSample]) -> Vec<Sample> {
    samples.iter().map(RawSample::to_sample).collect()
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

/// Compiles `root` with its stored, kept and raw inputs, or says why it
/// cannot. `windows` are the plan's query-time raw windows, by `TimeRange`.
fn program(
    dag: &PhysicalASAPDAG,
    sds: &Sds,
    kept_builds: &BTreeMap<DefinitionId, Result<PaneBuild, String>>,
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
        if let Some(window) = windows.get(&id) {
            let selector = RawSampleSelector::new(dag, window).map_err(|e| e.to_string())?;
            raw.push(RawInput {
                node: id,
                window: (*window).clone(),
                selector,
            });
            continue;
        }
        match &nodes[&id].payload {
            IrOperator::NonASAP(NonASAPOp::TimeRange { .. } | NonASAPOp::Scan { .. }) => {
                return Err(format!(
                    "node {id}: raw samples read outside the plan's raw windows"
                ))
            }
            _ => pending.extend(producers.get(&id).into_iter().flatten().copied()),
        }
    }
    let mut inputs = BTreeMap::new();
    for node in stored
        .iter()
        .map(|i| i.node)
        .chain(kept.iter().map(|i| i.node))
        .chain(raw.iter().map(|i| i.node))
    {
        let schema = Arc::new(output_schema(dag, node).clone());
        inputs.insert(node, InputContract::bounded(schema));
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
