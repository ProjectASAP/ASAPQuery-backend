//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! For every [`SummaryDefinition`] built at ingestion time the engine
//! buffers the samples of its pane's metric by tumbling pane. A pane `(end -
//! pane, end]` closes when the watermark (the newest sample time minus the
//! allowed lateness) reaches `end`. Closing it selects the pane's rows with
//! the executor's `RawSampleSelector`, which applies the plan's `Scan`
//! (its label matchers), then runs the definition's build from its
//! `TimeRange` up to its output, under ingestion scope. The output is
//! stored and never changes; samples older than a closed pane are dropped.
//! The first pane a definition sees may have started before the data plane
//! did, so it is never built: queries that need it are not answered from
//! stored panes.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::plan::PhysicalDAG;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::{RawSampleSelector, Sample};
use asap_planner::{InstallablePlan, PhysicalASAPDAG};
use futures::executor::block_on;
use futures::StreamExt;

use crate::sds::{DefinitionId, Sds, SummaryDefinition, SummaryStore};

/// One raw sample: its series' full label set (including `__name__`), time
/// and value. Samples of one series share their labels.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub labels: Arc<BTreeMap<String, String>>,
    pub timestamp_ms: i64,
    pub value: f64,
}

impl RawSample {
    /// The executor's sample, for a `RawSampleSelector`.
    pub fn to_sample(&self) -> Sample {
        Sample {
            labels: (*self.labels).clone(),
            timestamp_ms: self.timestamp_ms,
            value: self.value,
        }
    }
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

/// How the panes of one definition are built from raw samples. The
/// precompute engine builds stored panes with it, and the query engine kept
/// ones.
pub(crate) struct PaneBuild {
    pub(crate) definition: SummaryDefinition,
    /// Selects one pane's rows from raw samples.
    pub(crate) selector: RawSampleSelector,
    build: CompiledPhysicalDAG,
}

impl PaneBuild {
    /// Compiles `definition`'s build from its `TimeRange` up to its builder.
    pub(crate) fn new(
        dag: &PhysicalASAPDAG,
        definition: &SummaryDefinition,
    ) -> Result<Self, PrecomputeError> {
        let selector = RawSampleSelector::new(dag, &definition.window)
            .map_err(|e| PrecomputeError::Definition(definition.id.clone(), e.to_string()))?;
        let raw_schema = Arc::new(output_schema(dag, definition.raw_input()).clone());
        let build = compile(
            dag,
            BTreeMap::from([(definition.raw_input(), InputContract::bounded(raw_schema))]),
            &[definition.builder],
        )?;
        Ok(Self {
            definition: definition.clone(),
            selector,
            build,
        })
    }

    /// The pane ending at `end_ms`, built from `samples` under `scope`:
    /// the selector keeps the pane's samples that the plan's `Scan` selects,
    /// then the build runs over them.
    pub(crate) fn run(
        &self,
        samples: &[Sample],
        end_ms: i64,
        scope: Scope,
    ) -> Result<Batch, PrecomputeError> {
        let rows = self.selector.rows(end_ms, samples)?;
        let input: Source<'_> = Box::new(Operator::source(rows.schema().clone(), vec![rows])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.definition.raw_input(), input)]))?;
        let rows = run(&build, self.build.roots(), scope)?;
        let schema = Arc::new(self.definition.output_schema.clone());
        Ok(Batch::try_new(schema, rows)?)
    }
}

struct Builder {
    pane: PaneBuild,
    /// Samples of open panes, by pane end.
    open: BTreeMap<i64, Vec<Sample>>,
    /// End of the newest pane built or skipped; samples in it or before it
    /// are dropped. `None` until the first sample.
    built_through: Option<i64>,
}

pub struct Precompute {
    builders: Vec<Builder>,
    allowed_lateness_ms: i64,
    newest_sample_ms: Option<i64>,
    pub store: SummaryStore<Batch>,
    pub stats: PrecomputeStats,
}

impl Precompute {
    /// Compiles one build per ingestion-time definition of `sds`, the
    /// stored summaries of `plan`.
    pub fn new(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
    ) -> Result<Self, PrecomputeError> {
        let builders = sds
            .definitions
            .values()
            .map(|definition| {
                Ok(Builder {
                    pane: PaneBuild::new(&plan.dag.dag, definition)?,
                    open: BTreeMap::new(),
                    built_through: None,
                })
            })
            .collect::<Result<_, PrecomputeError>>()?;
        Ok(Self {
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
                if metric != Some(builder.pane.selector.metric()) {
                    continue;
                }
                let end = pane_end(sample.timestamp_ms, builder.pane.definition.pane_ms);
                // The first pane seen may be partial: skip it.
                let built = *builder.built_through.get_or_insert(end);
                if end <= built {
                    self.stats.dropped += 1;
                    continue;
                }
                builder
                    .open
                    .entry(end)
                    .or_default()
                    .push(sample.to_sample());
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
                match builder.pane.run(&samples, end, scope) {
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
            // The newest evaluation that reads stored panes is at most the
            // newest closed pane's end, so retention is measured from it,
            // not from the watermark, which may already be past it.
            let definition = &builder.pane.definition;
            if let Some(newest) = builder.built_through {
                self.store
                    .evict_before(&definition.id, definition.oldest_kept_end_ms(newest));
            }
        }
    }
}

/// End of the tumbling pane of width `pane_ms` that contains `timestamp_ms`.
/// Panes are `(end - pane_ms, end]`, like PromQL ranges: a sample at a pane
/// boundary belongs to the pane it ends.
pub fn pane_end(timestamp_ms: i64, pane_ms: i64) -> i64 {
    -(-timestamp_ms).div_euclid(pane_ms) * pane_ms
}

/// Working memory one execution may hold, in bytes; the executor's own
/// default (64 MiB) unless [`set_execution_memory_limit`] changed it.
static EXECUTION_MEMORY_LIMIT_BYTES: OnceLock<usize> = OnceLock::new();

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
    dag: &PhysicalDAG<'_, Batch, SchemaRef>,
    roots: &[u64],
    scope: Scope,
) -> Result<Vec<Vec<Value>>, asap_executor::Error> {
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

pub(crate) fn output_schema(
    dag: &PhysicalASAPDAG,
    id: u64,
) -> &asap_executor::planner::ir::schema::Schema {
    &dag.nodes
        .iter()
        .find(|n| n.id as u64 == id)
        .expect("stored outputs and raw windows name DAG nodes")
        .output_schema
}
