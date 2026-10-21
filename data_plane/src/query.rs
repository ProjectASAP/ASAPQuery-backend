//! Query engine: answers a planned query at evaluation time `T` by running
//! its root of the plan's DAG over stored panes and raw samples.
//!
//! Each root is compiled once with three kinds of inputs:
//! - stored outputs built in an ingestion run: bound to their definition's
//!   summary ending at `T - offset`, which must be stored;
//! - stored outputs built in a query run (`StoredOutputBuild::QueryRun`):
//!   bound to their definition's summary ending at `T - offset`. A missing
//!   one is built from its coverage's raw samples, which the caller fetches,
//!   and stored in the SDS for later evaluations once its pane is closed
//!   (its end at or before the watermark when the evaluation chose its
//!   fetch); these summary states are evicted by the plan's `retention_ms`
//!   plus one pane, measured from the newest closed pane at or before the
//!   watermark, as for stored outputs built in an `IngestionRun`;
//! - the plan's query-time `RawSampleWindow`s: bound to the samples in
//!   `(T - offset - width, T - offset]`, which the caller fetches, selected
//!   by the window's `RawSampleSelector`.
//!
//! A query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::{PhysicalASAPDAG, RawSampleWindow};
use asap_executor::planner::ir::scalar::{CompareOpKind, ScalarValue};
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema};
use asap_executor::planner::ir::{NonASAPOp, Operator as IrOperator, ScalarExpr};
use asap_executor::runtime::Scope;
use asap_executor::series_labels_of_output_row;
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_planner::{InstallablePlan, StoredOutputBuild};

use crate::precompute::{run, RawSample, SummaryBuild, SummaryInstance};
use crate::raw_sample_window::RawSampleWindowBatcher;
use crate::sds::{DefinitionKey, Sds, Selection, SummaryInstanceKey, SummaryStore};

/// A bound raw-sample window: a `RawSampleWindow` of the plan bound to one
/// evaluation time, i.e. `metric`'s raw samples in `(start_ms, end_ms]`
/// that `matchers` keep, which the caller fetches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRawSampleWindow {
    pub node: u64,
    pub metric: String,
    /// The window's `Scan` label matchers that PromQL can express, so the
    /// fetch reads only the series the plan keeps. The selector applies
    /// every matcher again, so one left out only widens the fetch.
    pub matchers: Vec<LabelMatcher>,
    pub start_ms: i64,
    pub end_ms: i64,
}

/// A PromQL label matcher: `name op "value"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

/// A PromQL label matching operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// `=`
    Equal,
    /// `!=`
    NotEqual,
    /// `=~`
    Regex,
    /// `!~`
    NotRegex,
}

impl MatchOp {
    pub fn as_str(self) -> &'static str {
        match self {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::Regex => "=~",
            MatchOp::NotRegex => "!~",
        }
    }
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
    /// The output's schema, which its panes' rows are in.
    schema: SchemaRef,
    definition: DefinitionKey,
    offset_ms: i64,
    pane_ms: i64,
}

struct RawInput {
    node: u64,
    matchers: Vec<LabelMatcher>,
    /// The plan's `Scan` label matchers over the range's samples.
    series: RawSampleWindowBatcher,
    range_ms: i64,
    offset_ms: i64,
}

/// A stored output built in a `QueryRun` (`MatAtQueryKeptAcrossEvaluations`).
struct QueryRunInput {
    node: u64,
    schema: SchemaRef,
    definition: DefinitionKey,
    offset_ms: i64,
    /// The output's own `TimeRange`, which its coverage's samples feed.
    raw_input: u64,
}

struct Program {
    compiled: CompiledPhysicalDAG,
    stored: Vec<StoredInput>,
    query_run: Vec<QueryRunInput>,
    /// For each `QueryRun` definition the root reads, the node its one
    /// fetch is keyed by: the lowest `TimeRange` among its outputs.
    query_run_fetch_node: BTreeMap<DefinitionKey, u64>,
    raw: Vec<RawInput>,
    output: Schema,
}

/// What happened to the stored outputs built in a `QueryRun` evaluations read.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct QueryRunStats {
    /// Summary states built from raw samples because the SDS had none.
    pub built: u64,
    /// Of those, the ones stored in the SDS: their coverage was closed.
    pub stored: u64,
    /// Of those stored, the ones whose own metric's newest sample had not
    /// passed the coverage's end: the watermark is global, so a metric
    /// lagging behind others can have a summary state stored before all its samples
    /// arrived. A per-metric watermark is the follow-up.
    pub stored_while_metric_behind: u64,
    /// Summaries read from an earlier evaluation instead of rebuilt.
    pub reused: u64,
}

/// What ingestion had seen when an evaluation decided what to fetch. The
/// store decision uses this snapshot, not the state when the evaluation
/// runs: samples ingested in between were not in the fetched data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestionProgress {
    /// The precompute engine's watermark; `None` before any sample.
    pub watermark_ms: Option<i64>,
    /// The newest sample time of each metric stored outputs built in a `QueryRun` read.
    pub newest_sample_ms: BTreeMap<String, i64>,
}

/// One raw read an evaluation needs, prepared by [`QueryEngine::bound_raw_sample_windows`].
#[derive(Debug, Clone)]
pub struct Prepared {
    /// The root answering the query.
    pub root: usize,
    /// The bound raw-sample windows to fetch.
    pub windows: Vec<BoundRawSampleWindow>,
    /// What ingestion had seen when `ranges` were chosen.
    pub progress: IngestionProgress,
}

pub struct QueryEngine {
    /// How many summary states of stored outputs built in a `QueryRun`
    /// evaluations built, stored and reused.
    pub query_run_stats: QueryRunStats,
    /// How each `QueryRun` definition's summaries are built, or why they
    /// cannot be.
    query_run_builds: BTreeMap<DefinitionKey, Result<SummaryBuild, String>>,
    /// The label matchers of each `QueryRun` definition's `Scan`, pushed into
    /// its fetch.
    query_run_matchers: BTreeMap<DefinitionKey, Vec<LabelMatcher>>,
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
        let query_run_builds: BTreeMap<_, _> = sds
            .definitions
            .values()
            .filter(|d| d.built_at == StoredOutputBuild::QueryRun)
            .map(|d| {
                let build = SummaryBuild::new(dag, d).map_err(|e| e.to_string());
                (d.key(), build)
            })
            .collect();
        let roots = plan
            .queries
            .iter()
            .map(|q| {
                let program = program(dag, sds, &query_run_builds, &windows, q.root_node_id as u64);
                (q.query.trim().to_string(), program)
            })
            .collect();
        let query_run_matchers = query_run_builds
            .keys()
            .map(|id| {
                let matchers = sds.definitions[id]
                    .window
                    .as_ref()
                    .map(|w| label_matchers(dag, w.scan_node_id as u64))
                    .unwrap_or_default();
                (id.clone(), matchers)
            })
            .collect();
        Self {
            query_run_stats: QueryRunStats::default(),
            query_run_builds,
            query_run_matchers,
            roots,
        }
    }

    /// The root answering `query` at `time_ms`, and the raw samples it needs:
    /// its query-time windows, and for each `QueryRun` definition one range
    /// covering every summary it reads that is not stored. Fails if the
    /// query is not planned, a time does not fit, or an ingestion-built
    /// summary it reads is missing. `progress` is what ingestion has seen
    /// now; pass the returned copy to [`Self::evaluate`].
    pub fn bound_raw_sample_windows(
        &self,
        query: &str,
        time_ms: i64,
        store: &SummaryStore<SummaryInstance>,
        progress: IngestionProgress,
    ) -> Result<Prepared, Unanswered> {
        let (root, program) = self.program(query)?;
        for input in &program.stored {
            pane(input, time_ms, store)?;
        }
        let mut ranges: Vec<BoundRawSampleWindow> = program
            .raw
            .iter()
            .map(|input| {
                let (start_ms, end_ms) = window(time_ms, input.offset_ms, input.range_ms)?;
                Ok(BoundRawSampleWindow {
                    node: input.node,
                    metric: input.series.metric().to_string(),
                    matchers: input.matchers.clone(),
                    start_ms,
                    end_ms,
                })
            })
            .collect::<Result<_, Unanswered>>()?;
        // One fetch per `QueryRun` definition, covering all its missing
        // summaries.
        let mut missing: BTreeMap<&DefinitionKey, (i64, i64)> = BTreeMap::new();
        for input in &program.query_run {
            let build = self.query_run_build(&input.definition);
            let (start_ms, end_ms) = window(time_ms, input.offset_ms, build.definition.pane_ms)?;
            if store
                .get(
                    &input.definition.id,
                    &selection_of(&input.definition, end_ms),
                )
                .is_none()
            {
                let span = missing
                    .entry(&input.definition)
                    .or_insert((start_ms, end_ms));
                *span = (span.0.min(start_ms), span.1.max(end_ms));
            }
        }
        for (definition, (start_ms, end_ms)) in missing {
            let build = self.query_run_build(definition);
            ranges.push(BoundRawSampleWindow {
                node: program.query_run_fetch_node[definition],
                metric: build.metric().to_string(),
                matchers: self.query_run_matchers[definition].clone(),
                start_ms,
                end_ms,
            });
        }
        Ok(Prepared {
            root,
            windows: ranges,
            progress,
        })
    }

    /// Runs `root` at `time_ms` over stored summaries and `raw`, the samples
    /// of each of its [`BoundRawSampleWindow`]s by node. A stored output built in a `QueryRun` not stored
    /// is built from its raw samples and stored in `store`, under the same
    /// key as those built in an `IngestionRun`, if its coverage was closed in
    /// `progress` (the snapshot [`Self::bound_raw_sample_windows`] chose the fetch by).
    /// Then stored outputs built in a `QueryRun` older than their definition's retention,
    /// measured from the newest closed pane at or before the watermark, are
    /// evicted.
    pub fn evaluate(
        &mut self,
        root: usize,
        time_ms: i64,
        store: &mut SummaryStore<SummaryInstance>,
        progress: &IngestionProgress,
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
                Operator::source(input.schema.clone(), pane.batches.clone()).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        for input in &program.query_run {
            let build = self.query_run_builds[&input.definition]
                .as_ref()
                .expect("compiled roots read only `QueryRun` definitions that build");
            let (_, end_ms) = window(time_ms, input.offset_ms, build.definition.pane_ms)?;
            let selection = selection_of(&input.definition, end_ms);
            let summary = match store.get(&input.definition.id, &selection) {
                Some(summary) => {
                    self.query_run_stats.reused += 1;
                    summary.clone()
                }
                None => {
                    // `bound_raw_sample_windows` asked for these samples unless the
                    // summary was stored then and evicted since.
                    let samples = raw
                        .get(&program.query_run_fetch_node[&input.definition])
                        .ok_or_else(|| {
                            Unanswered::MissingPane(format!(
                                "stored output built in a `QueryRun` ending at {end_ms} of {:?} was evicted",
                                input.definition
                            ))
                        })?;
                    // Built as at its coverage's end, so every evaluation
                    // that reads it reads the same summary.
                    let build_scope = Scope::Query {
                        evaluation_time_ms: end_ms,
                        revision: 1,
                    };
                    let summary = build
                        .run(end_ms, samples, build_scope)
                        .map_err(|e| Unanswered::Execution(e.to_string()))?;
                    self.query_run_stats.built += 1;
                    // Stored only once closed, as the precompute engine closes
                    // panes: a sample may still arrive for an open one.
                    if progress.watermark_ms.is_some_and(|w| end_ms <= w) {
                        self.query_run_stats.stored += 1;
                        let metric_newest = progress.newest_sample_ms.get(build.metric());
                        if metric_newest.is_none_or(|newest| *newest < end_ms) {
                            self.query_run_stats.stored_while_metric_behind += 1;
                        }
                        store.insert(
                            SummaryInstanceKey::new(input.definition.id.clone(), selection),
                            summary.clone(),
                        );
                    }
                    summary
                }
            };
            let source = Operator::source(input.schema.clone(), summary.batches).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        if let Some(watermark) = progress.watermark_ms {
            for build in self.query_run_builds.values().flatten() {
                let definition = &build.definition;
                let pane_ms = definition.pane_ms;
                // As for stored outputs built in an `IngestionRun`: measured from the newest
                // closed coverage, one pane of margin.
                let newest_closed_end = watermark.div_euclid(pane_ms).saturating_mul(pane_ms);
                let oldest =
                    definition.oldest_kept_end_ms(newest_closed_end.saturating_sub(pane_ms));
                store.evict_before(&definition.id, &definition.selection_ending_at(oldest));
            }
        }
        for input in &program.raw {
            let samples = raw.get(&input.node).map(Vec::as_slice).unwrap_or_default();
            let (_, end_ms) = window(time_ms, input.offset_ms, input.range_ms)?;
            let rows = input.series.batch(end_ms, samples).map_err(failed)?;
            let source =
                Operator::source(input.series.schema.clone(), vec![rows]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        let dag = program.compiled.instantiate(sources).map_err(failed)?;
        let batches = run(&dag, program.compiled.roots(), scope).map_err(failed)?;
        result_samples(&program.output, &batches)
    }

    /// The planned queries the engine cannot run, with why.
    pub fn unexecutable(&self) -> Vec<(String, String)> {
        self.roots
            .iter()
            .filter_map(|(query, program)| Some((query.clone(), program.as_ref().err()?.clone())))
            .collect()
    }

    /// The build of a `QueryRun` definition a compiled root reads.
    fn query_run_build(&self, definition: &DefinitionKey) -> &SummaryBuild {
        self.query_run_builds[definition]
            .as_ref()
            .expect("compiled roots read only `QueryRun` definitions that build")
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

/// The concrete selection of `definition`'s pane ending at `end_ms`.
fn selection_of(definition: &DefinitionKey, end_ms: i64) -> Selection {
    Selection {
        labels: definition.labels.clone(),
        end_ms,
    }
}

/// `(start, end]` of a window `width_ms` wide ending `offset_ms` before
/// `time_ms`, or why it does not fit in `i64` milliseconds.
fn window(time_ms: i64, offset_ms: i64, width_ms: i64) -> Result<(i64, i64), Unanswered> {
    let out_of_range =
        || Unanswered::Unsupported(format!("evaluation time {time_ms} ms is out of range"));
    let end = time_ms.checked_sub(offset_ms).ok_or_else(out_of_range)?;
    let start = end.checked_sub(width_ms).ok_or_else(out_of_range)?;
    Ok((start, end))
}

/// The stored pane `input` reads at `time_ms`.
fn pane<'a>(
    input: &StoredInput,
    time_ms: i64,
    store: &'a SummaryStore<SummaryInstance>,
) -> Result<&'a SummaryInstance, Unanswered> {
    let (_, end) = window(time_ms, input.offset_ms, input.pane_ms)?;
    if end.rem_euclid(input.pane_ms) != 0 {
        return Err(Unanswered::MissingPane(format!(
            "{time_ms} is not on the {} ms pane grid",
            input.pane_ms
        )));
    }
    store
        .get(&input.definition.id, &selection_of(&input.definition, end))
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
    query_run_builds: &BTreeMap<DefinitionKey, Result<SummaryBuild, String>>,
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
    // A query-time window caches a series for as long as any summary is
    // kept.
    let longest_retention_ms = sds
        .definitions
        .values()
        .map(|d| d.retention_ms)
        .max()
        .unwrap_or(0);
    let (mut stored, mut query_run, mut raw) = (Vec::new(), Vec::new(), Vec::new());
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        if let Some((definition, binding)) = sds.outputs.get(&id) {
            match sds.definitions[definition].built_at {
                StoredOutputBuild::IngestionRun => stored.push(StoredInput {
                    node: id,
                    schema: Arc::new(nodes[&id].output_schema.clone()),
                    definition: definition.clone(),
                    offset_ms: binding.offset_ms,
                    pane_ms: sds.definitions[definition].pane_ms,
                }),
                StoredOutputBuild::QueryRun => {
                    query_run_builds[definition]
                        .as_ref()
                        .map_err(Clone::clone)?;
                    query_run.push(QueryRunInput {
                        node: id,
                        schema: Arc::new(nodes[&id].output_schema.clone()),
                        definition: definition.clone(),
                        offset_ms: binding.offset_ms,
                        raw_input: binding.raw_input,
                    });
                }
            }
            continue;
        }
        match &nodes[&id].payload {
            IrOperator::NonASAP(NonASAPOp::TimeRange { .. }) => {
                let window = windows.get(&id).ok_or_else(|| {
                    format!("node {id}: a TimeRange the plan lists no window for")
                })?;
                let series = RawSampleWindowBatcher::new(dag, window, longest_retention_ms)
                    .map_err(|e| format!("node {id}: {e}"))?;
                raw.push(RawInput {
                    node: id,
                    matchers: label_matchers(dag, window.scan_node_id as u64),
                    series,
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
    for node in stored
        .iter()
        .map(|i| i.node)
        .chain(query_run.iter().map(|i| i.node))
    {
        let schema = Arc::new(nodes[&node].output_schema.clone());
        inputs.insert(node, InputContract::bounded(schema));
    }
    for input in &raw {
        inputs.insert(
            input.node,
            InputContract::bounded(input.series.schema.clone()),
        );
    }
    let compiled = compile(dag, inputs, &[root]).map_err(|e| e.to_string())?;
    let mut query_run_fetch_node: BTreeMap<DefinitionKey, u64> = BTreeMap::new();
    for input in &query_run {
        let node = query_run_fetch_node
            .entry(input.definition.clone())
            .or_insert(input.raw_input);
        *node = (*node).min(input.raw_input);
    }
    Ok(Program {
        compiled,
        stored,
        query_run,
        query_run_fetch_node,
        raw,
        output: nodes[&root].output_schema.clone(),
    })
}

/// The label matchers of the `Scan` at `scan` that PromQL can express: a
/// label column compared (`=`, `!=`, `=~`, `!~`) with a text literal.
/// Other predicates are left out of the fetch; the selector applies them.
fn label_matchers(dag: &PhysicalASAPDAG, scan: u64) -> Vec<LabelMatcher> {
    let Some(node) = dag.nodes.iter().find(|n| n.id as u64 == scan) else {
        return Vec::new();
    };
    let IrOperator::NonASAP(NonASAPOp::Scan {
        predicates, schema, ..
    }) = &node.payload
    else {
        return Vec::new();
    };
    predicates
        .iter()
        .filter_map(|predicate| {
            let ScalarExpr::Compare {
                left, op, right, ..
            } = &predicate.0
            else {
                return None;
            };
            let (ScalarExpr::Column(column), ScalarExpr::Literal(ScalarValue::Utf8(value))) =
                (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            let op = match op {
                CompareOpKind::Eq => MatchOp::Equal,
                CompareOpKind::Ne => MatchOp::NotEqual,
                CompareOpKind::Regex => MatchOp::Regex,
                CompareOpKind::NotRegex => MatchOp::NotRegex,
                _ => return None,
            };
            let name = &schema.fields.get(*column)?.name;
            // Only Prometheus label names; the series identity column and
            // other derived columns are not labels.
            let is_label = !name.is_empty()
                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !name.starts_with(|c: char| c.is_ascii_digit());
            is_label.then(|| LabelMatcher {
                name: name.clone(),
                op,
                value: value.clone(),
            })
        })
        .collect()
}

/// PromQL vector samples from a root's result batches: labels from the
/// series identity and the other text columns, the value from the one
/// `Float64` column. The one place rows are read: the answer's JSON
/// encoding.
fn result_samples(schema: &Schema, batches: &[Batch]) -> Result<Vec<ResultSample>, Unanswered> {
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
    batches
        .iter()
        .flat_map(Batch::to_rows)
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
