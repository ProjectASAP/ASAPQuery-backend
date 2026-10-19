//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! For every [`SummaryDefinition`] the engine buffers the samples of its
//! `Scan`'s metric by tumbling pane. A pane `(end - pane, end]` closes when
//! the watermark (the newest sample time minus the allowed lateness) reaches
//! `end`. Closing it runs the plan's `Scan` over the buffered samples, which
//! applies the label matchers, then the definition's build from its
//! `TimeRange` input up to its output, under ingestion scope. The output is
//! stored and never changes; samples older than a closed pane are dropped.
//! The first pane a definition sees may have started before the data plane
//! did, so it is never built: queries that need it are not answered from
//! stored panes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::promql_rows::series_row;
use asap_executor::physical_planner::{
    bind_with_data_sources, compile, CompiledPhysicalDAG, InputContract, Source,
};
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::sources::{DataSources, MemorySource};
use asap_executor::values::{Batch, SchemaRef};
use futures::executor::block_on;
use futures::StreamExt;
use planner_types::ir::operator::operator_properties::Source as ScanSource;
use planner_types::ir::physical_export::PhysicalASAPDAG;
use planner_types::ir::{NonASAPOp, Operator as IrOperator};

use crate::sds::{DefinitionId, PaneBinding, Sds, SummaryDefinition, SummaryStore};

/// One raw sample: its series' full label set (including `__name__`), time
/// and value.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub labels: Arc<BTreeMap<String, String>>,
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum PrecomputeError {
    #[error("stored summary {0:?}: {1}")]
    Definition(DefinitionId, String),
    #[error(transparent)]
    Executor(#[from] asap_executor::Error),
}

/// Counters for what happened to incoming samples.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PrecomputeStats {
    /// Samples buffered for at least one pane.
    pub accepted: u64,
    /// Samples for a pane already built or skipped.
    pub dropped: u64,
    /// Panes built and stored.
    pub panes_built: u64,
    /// Panes whose build failed; they are skipped, not retried.
    pub panes_failed: u64,
}

/// A time-series `Scan` of the plan: its node, metric and row schema.
#[derive(Debug, Clone)]
pub struct ScanInput {
    pub node: u64,
    pub metric: String,
    pub schema: SchemaRef,
}

/// How the panes of one definition are built from raw samples. The
/// precompute engine builds stored panes with it, and the query engine kept
/// ones.
pub(crate) struct PaneBuild {
    pub(crate) definition: SummaryDefinition,
    pub(crate) scan: ScanInput,
    build: CompiledPhysicalDAG,
    /// Largest offset any output of the definition reads, plus two panes:
    /// panes ending at or before `newest - retention_ms` are no longer read.
    pub(crate) retention_ms: i64,
}

impl PaneBuild {
    /// Compiles `definition`'s build from its `TimeRange` up to its builder;
    /// `outputs` are the outputs that read it.
    pub(crate) fn new(
        dag: &PhysicalASAPDAG,
        definition: &SummaryDefinition,
        outputs: &BTreeMap<u64, (DefinitionId, PaneBinding)>,
    ) -> Result<Self, PrecomputeError> {
        let error =
            |message: &str| PrecomputeError::Definition(definition.id.clone(), message.into());
        let scan = scan_below(dag, definition.raw_input)
            .ok_or_else(|| error("no time-series Scan below its TimeRange"))?;
        let raw_schema = Arc::new(output_schema(dag, definition.raw_input).clone());
        if raw_schema != scan.schema {
            return Err(error("TimeRange rows differ from its Scan's"));
        }
        let build = compile(
            dag,
            BTreeMap::from([(definition.raw_input, InputContract::bounded(raw_schema))]),
            &[definition.builder],
        )?;
        let max_offset = outputs
            .values()
            .filter(|(id, _)| id == &definition.id)
            .map(|(_, binding)| binding.offset_ms)
            .max()
            .unwrap_or(0);
        Ok(Self {
            definition: definition.clone(),
            scan,
            build,
            retention_ms: max_offset + 2 * definition.pane_ms,
        })
    }

    /// Runs the plan's `Scan` over `samples`, then the build over the rows
    /// it selects, under `scope`.
    pub(crate) fn run(
        &self,
        dag: &PhysicalASAPDAG,
        samples: &[RawSample],
        scope: Scope,
    ) -> Result<Batch, PrecomputeError> {
        let selected = select(dag, &self.scan, samples, scope.clone())?;
        let input: Source<'_> =
            Box::new(Operator::source(self.scan.schema.clone(), vec![selected])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.definition.raw_input, input)]))?;
        let rows = run(&build, self.build.roots(), scope)?;
        let schema = Arc::new(self.definition.output_schema.clone());
        Ok(Batch::try_new(schema, rows)?)
    }
}

struct Builder {
    pane: PaneBuild,
    /// Samples of open panes, by pane end.
    open: BTreeMap<i64, Vec<RawSample>>,
    /// End of the newest pane built or skipped; samples in it or before it
    /// are dropped. `None` until the first sample.
    built_through: Option<i64>,
}

pub struct Precompute {
    dag: PhysicalASAPDAG,
    builders: Vec<Builder>,
    allowed_lateness_ms: i64,
    newest_sample_ms: Option<i64>,
    pub store: SummaryStore<Batch>,
    pub stats: PrecomputeStats,
}

impl Precompute {
    /// Compiles one build per definition of `sds`, a description of `dag`.
    pub fn new(
        dag: PhysicalASAPDAG,
        sds: &Sds,
        allowed_lateness_ms: i64,
    ) -> Result<Self, PrecomputeError> {
        let builders = sds
            .definitions
            .values()
            .map(|definition| {
                Ok(Builder {
                    pane: PaneBuild::new(&dag, definition, &sds.outputs)?,
                    open: BTreeMap::new(),
                    built_through: None,
                })
            })
            .collect::<Result<_, PrecomputeError>>()?;
        Ok(Self {
            dag,
            builders,
            allowed_lateness_ms,
            newest_sample_ms: None,
            store: SummaryStore::new(),
            stats: PrecomputeStats::default(),
        })
    }

    /// The watermark: no sample older than this is expected any more.
    pub fn watermark_ms(&self) -> Option<i64> {
        self.newest_sample_ms
            .map(|newest| newest - self.allowed_lateness_ms)
    }

    /// Buffers `samples` into their panes, then builds every pane the
    /// watermark has passed.
    pub fn ingest(&mut self, samples: &[RawSample]) {
        for sample in samples {
            let metric = sample.labels.get("__name__").map(String::as_str);
            let mut accepted = false;
            for builder in &mut self.builders {
                if metric != Some(builder.pane.scan.metric.as_str()) {
                    continue;
                }
                let end = pane_end(sample.timestamp_ms, builder.pane.definition.pane_ms);
                // The first pane seen may be partial: skip it.
                let built = *builder.built_through.get_or_insert(end);
                if end <= built {
                    self.stats.dropped += 1;
                    continue;
                }
                builder.open.entry(end).or_default().push(sample.clone());
                accepted = true;
            }
            if accepted {
                self.stats.accepted += 1;
            }
            self.newest_sample_ms = self.newest_sample_ms.max(Some(sample.timestamp_ms));
        }
        self.close_panes();
    }

    fn close_panes(&mut self) {
        let Some(watermark) = self.watermark_ms() else {
            return;
        };
        for builder in &mut self.builders {
            let Some(built) = builder.built_through else {
                continue;
            };
            let pane_ms = builder.pane.definition.pane_ms;
            // Every pane after the first through the watermark is built, so
            // a pane without samples is stored as empty, not missing.
            let mut end = built + pane_ms;
            while end <= watermark {
                let samples = builder.open.remove(&end).unwrap_or_default();
                let scope = Scope::Ingestion {
                    window_start_ms: end - pane_ms,
                    window_end_ms: end,
                    revision: 1,
                };
                match builder.pane.run(&self.dag, &samples, scope) {
                    Ok(state) => {
                        self.store.insert(&builder.pane.definition.id, end, state);
                        self.stats.panes_built += 1;
                    }
                    Err(error) => {
                        tracing::error!(definition = ?builder.pane.definition.id, end, %error, "pane build failed");
                        self.stats.panes_failed += 1;
                    }
                }
                builder.built_through = Some(end);
                end += pane_ms;
            }
            let definition = &builder.pane.definition.id;
            self.store
                .evict_through(definition, watermark - builder.pane.retention_ms);
        }
    }
}

/// End of the tumbling pane of width `pane_ms` that contains `timestamp_ms`.
/// Panes are `(end - pane_ms, end]`, like PromQL ranges: a sample at a pane
/// boundary belongs to the pane it ends.
pub fn pane_end(timestamp_ms: i64, pane_ms: i64) -> i64 {
    -(-timestamp_ms).div_euclid(pane_ms) * pane_ms
}

/// The rows of `samples` the plan's `scan` selects: its label matchers
/// applied by the executor's `Scan`.
pub fn select(
    dag: &PhysicalASAPDAG,
    scan: &ScanInput,
    samples: &[RawSample],
    scope: Scope,
) -> Result<Batch, asap_executor::Error> {
    let rows = samples
        .iter()
        .map(|s| series_row(&scan.schema, &s.labels, s.timestamp_ms, s.value))
        .collect::<Result<Vec<_>, _>>()?;
    let raw = Batch::try_new(scan.schema.clone(), rows)?;
    let mut sources = DataSources::default();
    sources.register(
        ScanSource::TimeSeries {
            metric: scan.metric.clone(),
        },
        Arc::new(MemorySource::new(scan.schema.clone(), vec![raw])?),
    )?;
    let dag = bind_with_data_sources(dag, BTreeMap::new(), &[scan.node], &sources)?;
    let selected = run(&dag, &[scan.node], scope)?;
    Batch::try_new(scan.schema.clone(), selected)
}

/// Working memory one execution may hold, in bytes; the executor's own
/// default (64 MiB) unless [`set_execution_memory_limit`] changed it.
static EXECUTION_MEMORY_LIMIT_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// Sets the working memory every later execution may hold. An execution
/// over it fails, and its query is forwarded. Only the first call counts.
pub fn set_execution_memory_limit(bytes: usize) {
    let _ = EXECUTION_MEMORY_LIMIT_BYTES.set(bytes);
}

fn limits() -> Limits {
    let mut limits = Limits::default();
    if let Some(&bytes) = EXECUTION_MEMORY_LIMIT_BYTES.get() {
        limits.max_bytes = bytes;
    }
    limits
}

/// Executes `dag`'s single root to completion and returns its rows.
pub fn run(
    dag: &asap_executor::plan::PhysicalDAG<'_, Batch, SchemaRef>,
    roots: &[u64],
    scope: Scope,
) -> Result<Vec<Vec<asap_executor::values::Value>>, asap_executor::Error> {
    block_on(async {
        let context = RunContext::new(scope, limits())?;
        let mut output = dag.execute(roots, context)?.remove(0);
        let mut rows = Vec::new();
        while let Some(batch) = output.next().await {
            rows.extend(batch?.rows().iter().cloned());
        }
        Ok(rows)
    })
}

fn output_schema(dag: &PhysicalASAPDAG, id: u64) -> &planner_types::ir::schema::Schema {
    &dag.nodes
        .iter()
        .find(|n| n.id as u64 == id)
        .expect("SDS names DAG nodes")
        .output_schema
}

/// The time-series `Scan` that `id` reads through single-input operators.
pub fn scan_below(dag: &PhysicalASAPDAG, id: u64) -> Option<ScanInput> {
    let inputs: HashMap<u64, u64> = dag
        .edges
        .iter()
        .map(|e| (e.consumer as u64, e.producer as u64))
        .collect();
    let mut current = id;
    loop {
        let node = dag.nodes.iter().find(|n| n.id as u64 == current)?;
        if let IrOperator::NonASAP(NonASAPOp::Scan {
            source: ScanSource::TimeSeries { metric },
            ..
        }) = &node.payload
        {
            return Some(ScanInput {
                node: current,
                metric: metric.clone(),
                schema: Arc::new(node.output_schema.clone()),
            });
        }
        current = *inputs.get(&current)?;
    }
}
