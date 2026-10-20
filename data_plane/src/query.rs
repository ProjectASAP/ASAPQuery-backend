//! Query engine: answers a planned query at evaluation time `T` by running
//! its root of the plan's DAG over stored panes and raw samples.
//!
//! Each root is compiled once with three kinds of inputs:
//! - stored outputs built in an ingestion run: bound to their definition's
//!   summary ending at `T - offset`, which must be stored;
//! - stored outputs built in a query run (`StoredOutputBuild::QueryRun`):
//!   bound to their definition's summary ending at `T - offset`. A missing
//!   one is built from its coverage's raw samples, which the caller fetches,
//!   and kept for later evaluations once its coverage is closed (its end at
//!   or before the precompute engine's watermark); kept summaries are
//!   evicted by the plan's retention, measured from the newest evaluation;
//! - the plan's query-time raw windows: bound to the samples in
//!   `(T - offset - width, T - offset]`, which the caller fetches, selected
//!   by the window's `RawSampleSelector`.
//!
//! A query the engine cannot answer returns [`Unanswered`]; the caller then
//! forwards it to Prometheus.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::{PhysicalASAPDAG, RawSampleWindow};
use asap_executor::planner::ir::scalar::{CompareOpKind, ScalarValue};
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema};
use asap_executor::planner::ir::{NonASAPOp, Operator as IrOperator, ScalarExpr};
use asap_executor::runtime::Scope;
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::{series_labels_of_output_row, RawSampleSelector};
use asap_planner::{InstallablePlan, StoredOutputBuild};

use crate::precompute::{executor_samples, run_root, RawSample, SummaryBuild};
use crate::sds::{DefinitionId, Sds, Selection, SummaryInstanceKey, SummaryStore};

/// Raw samples a query needs: `metric`'s samples in `(start_ms, end_ms]`
/// that `matchers` keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRange {
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
    definition: DefinitionId,
    offset_ms: i64,
    pane_ms: i64,
}

struct RawInput {
    node: u64,
    matchers: Vec<LabelMatcher>,
    /// The plan's `Scan` label matchers over the range's samples.
    selector: RawSampleSelector,
    schema: SchemaRef,
    range_ms: i64,
    offset_ms: i64,
}

/// A stored output built in a query run and kept.
struct KeptInput {
    node: u64,
    definition: DefinitionId,
    offset_ms: i64,
    /// The output's own `TimeRange`, which its coverage's samples feed.
    raw_input: u64,
}

struct Program {
    compiled: CompiledPhysicalDAG,
    stored: Vec<StoredInput>,
    kept: Vec<KeptInput>,
    /// For each query-run definition the root reads, the node its one
    /// fetch is keyed by: the lowest `TimeRange` among its outputs.
    kept_fetch_node: BTreeMap<DefinitionId, u64>,
    raw: Vec<RawInput>,
    output: Schema,
}

/// What happened to the query-run summaries evaluations read.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct KeptResultStats {
    /// Summaries built from raw samples because none was kept.
    pub built: u64,
    /// Of those, the ones kept: their coverage was closed.
    pub kept: u64,
    /// Of those kept, the ones whose own metric's newest sample had not
    /// passed the coverage's end: the watermark is global, so a metric
    /// lagging behind others can have a summary kept before all its samples
    /// arrived. A per-metric watermark is the follow-up.
    pub kept_while_metric_behind: u64,
    /// Summaries read from an earlier evaluation instead of rebuilt.
    pub reused: u64,
}

/// [`KeptResultStats`] as counters every query thread adds to.
#[derive(Default)]
struct KeptResultCounters {
    built: AtomicU64,
    kept: AtomicU64,
    kept_while_metric_behind: AtomicU64,
    reused: AtomicU64,
}

fn add(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

/// What ingestion had seen when an evaluation decided what to fetch. The
/// keep decision uses this snapshot, not the state when the evaluation
/// runs: samples ingested in between were not in the fetched data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestionProgress {
    /// The precompute engine's watermark; `None` before any sample.
    pub watermark_ms: Option<i64>,
    /// The newest sample time of each metric query-run summaries read.
    pub newest_sample_ms: BTreeMap<String, i64>,
}

/// One raw read an evaluation needs, prepared by [`QueryEngine::raw_ranges`].
#[derive(Debug, Clone)]
pub struct Prepared {
    /// The root answering the query.
    pub root: usize,
    /// The raw samples to fetch.
    pub ranges: Vec<RawRange>,
    /// What ingestion had seen when `ranges` were chosen.
    pub progress: IngestionProgress,
}

/// The plan's compiled roots, compiled once and shared by every query
/// thread.
pub struct QueryEngine {
    /// How many query-run summaries evaluations built, kept and reused.
    kept_stats: KeptResultCounters,
    /// How each query-run definition's summaries are built, or why they
    /// cannot be.
    kept_builds: BTreeMap<DefinitionId, Result<SummaryBuild, String>>,
    /// The label matchers of each query-run definition's `Scan`, pushed into
    /// its fetch.
    kept_matchers: BTreeMap<DefinitionId, Vec<LabelMatcher>>,
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
        let kept_builds: BTreeMap<_, _> = sds
            .definitions
            .values()
            .filter(|d| d.built_at == StoredOutputBuild::QueryRun)
            .map(|d| {
                let build = SummaryBuild::new(dag, d).map_err(|e| e.to_string());
                (d.id.clone(), build)
            })
            .collect();
        let roots = plan
            .queries
            .iter()
            .map(|q| {
                let program = program(dag, sds, &kept_builds, &windows, q.root_node_id as u64);
                (q.query.trim().to_string(), program)
            })
            .collect();
        let kept_matchers = kept_builds
            .keys()
            .map(|id| {
                let scan = sds.definitions[id].window.scan_node_id as u64;
                (id.clone(), label_matchers(dag, scan))
            })
            .collect();
        Self {
            kept_stats: KeptResultCounters::default(),
            kept_builds,
            kept_matchers,
            roots,
        }
    }

    /// The root answering `query` at `time_ms`, and the raw samples it needs:
    /// its query-time windows, and for each query-run definition one range
    /// covering every summary it reads that is not stored. Fails if the
    /// query is not planned, a time does not fit, or an ingestion-built
    /// summary it reads is missing. `progress` is what ingestion has seen
    /// now; pass the returned copy to [`Self::evaluate`].
    pub fn raw_ranges(
        &self,
        query: &str,
        time_ms: i64,
        store: &SummaryStore<Batch>,
        progress: IngestionProgress,
    ) -> Result<Prepared, Unanswered> {
        let (root, program) = self.program(query)?;
        for input in &program.stored {
            pane(input, time_ms, store)?;
        }
        let mut ranges: Vec<RawRange> = program
            .raw
            .iter()
            .map(|input| {
                let (start_ms, end_ms) = window(time_ms, input.offset_ms, input.range_ms)?;
                Ok(RawRange {
                    node: input.node,
                    metric: input.selector.metric().to_string(),
                    matchers: input.matchers.clone(),
                    start_ms,
                    end_ms,
                })
            })
            .collect::<Result<_, Unanswered>>()?;
        // One fetch per query-run definition, covering all its missing
        // summaries.
        let mut missing: BTreeMap<&DefinitionId, (i64, i64)> = BTreeMap::new();
        for input in &program.kept {
            let build = self.kept_build(&input.definition);
            let (start_ms, end_ms) = window(time_ms, input.offset_ms, build.definition.pane_ms)?;
            if store
                .get(&input.definition, Selection::ending_at(end_ms))
                .is_none()
            {
                let span = missing
                    .entry(&input.definition)
                    .or_insert((start_ms, end_ms));
                *span = (span.0.min(start_ms), span.1.max(end_ms));
            }
        }
        for (definition, (start_ms, end_ms)) in missing {
            let build = self.kept_build(definition);
            ranges.push(RawRange {
                node: program.kept_fetch_node[definition],
                metric: build.metric().to_string(),
                matchers: self.kept_matchers[definition].clone(),
                start_ms,
                end_ms,
            });
        }
        Ok(Prepared {
            root,
            ranges,
            progress,
        })
    }

    /// Runs `root` at `time_ms` over stored summaries and `raw`, the samples
    /// of each of its [`RawRange`]s by node. A query-run summary not stored
    /// is built from its raw samples and stored in `store`, under the same
    /// key as ingestion-built ones, if its coverage was closed in
    /// `progress` (the snapshot [`Self::raw_ranges`] chose the fetch by).
    /// Then query-run summaries older than their definition's retention,
    /// measured from the newest closed pane at or before the watermark, are
    /// evicted.
    pub fn evaluate(
        &self,
        root: usize,
        time_ms: i64,
        store: &SummaryStore<Batch>,
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
            // Cloning a batch shares its buffers: the pane is not copied.
            let source =
                Operator::source(pane.schema().clone(), vec![(*pane).clone()]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        for input in &program.kept {
            let build = self.kept_builds[&input.definition]
                .as_ref()
                .expect("compiled roots read only query-run definitions that build");
            let (_, end_ms) = window(time_ms, input.offset_ms, build.definition.pane_ms)?;
            let selection = Selection::ending_at(end_ms);
            let summary = match store.get(&input.definition, selection) {
                Some(summary) => {
                    add(&self.kept_stats.reused);
                    (*summary).clone()
                }
                None => {
                    // `raw_ranges` asked for these samples unless the
                    // summary was stored then and evicted since.
                    let samples = raw
                        .get(&program.kept_fetch_node[&input.definition])
                        .ok_or_else(|| {
                            Unanswered::MissingPane(format!(
                                "query-run summary ending at {end_ms} of {:?} was evicted",
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
                    add(&self.kept_stats.built);
                    // Kept only once closed, as the precompute engine closes
                    // panes: a sample may still arrive for an open one.
                    if progress.watermark_ms.is_some_and(|w| end_ms <= w) {
                        add(&self.kept_stats.kept);
                        let metric_newest = progress.newest_sample_ms.get(build.metric());
                        if metric_newest.is_none_or(|newest| *newest < end_ms) {
                            add(&self.kept_stats.kept_while_metric_behind);
                        }
                        // Another query thread may have stored it meanwhile:
                        // the first stored is the one every evaluation reads.
                        let key = SummaryInstanceKey::new(input.definition.clone(), selection);
                        (*store.insert(key, summary)).clone()
                    } else {
                        summary
                    }
                }
            };
            let source =
                Operator::source(summary.schema().clone(), vec![summary]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        if let Some(watermark) = progress.watermark_ms {
            for build in self.kept_builds.values().flatten() {
                let definition = &build.definition;
                let pane_ms = definition.pane_ms;
                // As for ingestion-built summaries: measured from the newest
                // closed coverage, one pane of margin.
                let newest_closed_end = watermark.div_euclid(pane_ms).saturating_mul(pane_ms);
                let oldest =
                    definition.oldest_kept_end_ms(newest_closed_end.saturating_sub(pane_ms));
                store.evict_before(&definition.id, Selection::ending_at(oldest));
            }
        }
        for input in &program.raw {
            let samples = raw.get(&input.node).map(Vec::as_slice).unwrap_or_default();
            let rows = input
                .selector
                .window_batch(
                    window(time_ms, input.offset_ms, input.range_ms)?.1,
                    &executor_samples(samples),
                )
                .map_err(failed)?;
            let source = Operator::source(input.schema.clone(), vec![rows]).map_err(failed)?;
            sources.insert(input.node, Box::new(source));
        }
        let dag = program.compiled.instantiate(sources).map_err(failed)?;
        let rows = run_root(&dag, program.compiled.roots()[0], scope).map_err(failed)?;
        result_samples(&program.output, rows)
    }

    /// How many query-run summaries evaluations built, kept and reused.
    pub fn kept_stats(&self) -> KeptResultStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let c = &self.kept_stats;
        KeptResultStats {
            built: load(&c.built),
            kept: load(&c.kept),
            kept_while_metric_behind: load(&c.kept_while_metric_behind),
            reused: load(&c.reused),
        }
    }

    /// The build of a query-run definition a compiled root reads.
    fn kept_build(&self, definition: &DefinitionId) -> &SummaryBuild {
        self.kept_builds[definition]
            .as_ref()
            .expect("compiled roots read only query-run definitions that build")
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
fn pane(
    input: &StoredInput,
    time_ms: i64,
    store: &SummaryStore<Batch>,
) -> Result<Arc<Batch>, Unanswered> {
    let (_, end) = window(time_ms, input.offset_ms, input.pane_ms)?;
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
    kept_builds: &BTreeMap<DefinitionId, Result<SummaryBuild, String>>,
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
            match sds.definitions[definition].built_at {
                StoredOutputBuild::IngestionRun => stored.push(StoredInput {
                    node: id,
                    definition: definition.clone(),
                    offset_ms: binding.offset_ms,
                    pane_ms: sds.definitions[definition].pane_ms,
                }),
                StoredOutputBuild::QueryRun => {
                    kept_builds[definition].as_ref().map_err(Clone::clone)?;
                    kept.push(KeptInput {
                        node: id,
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
                let selector =
                    RawSampleSelector::new(dag, window).map_err(|e| format!("node {id}: {e}"))?;
                raw.push(RawInput {
                    node: id,
                    matchers: label_matchers(dag, window.scan_node_id as u64),
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
    for node in stored
        .iter()
        .map(|i| i.node)
        .chain(kept.iter().map(|i| i.node))
    {
        let schema = Arc::new(nodes[&node].output_schema.clone());
        inputs.insert(node, InputContract::bounded(schema));
    }
    for input in &raw {
        inputs.insert(input.node, InputContract::bounded(input.schema.clone()));
    }
    let compiled = compile(dag, inputs, &[root]).map_err(|e| e.to_string())?;
    let mut kept_fetch_node: BTreeMap<DefinitionId, u64> = BTreeMap::new();
    for input in &kept {
        let node = kept_fetch_node
            .entry(input.definition.clone())
            .or_insert(input.raw_input);
        *node = (*node).min(input.raw_input);
    }
    Ok(Program {
        compiled,
        stored,
        kept,
        kept_fetch_node,
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
