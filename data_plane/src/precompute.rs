//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! For every [`SummaryDefinition`] the engine buffers the samples of its
//! `Scan`'s metric by tumbling pane. A pane `(end - pane, end]` closes when
//! the watermark (the newest sample time minus the allowed lateness) reaches
//! `end`. Closing it selects the pane's rows from the buffered samples with
//! the executor's `RawSampleSelector` (the `Scan`'s label matchers), then runs
//! the definition's build from its `TimeRange` input up to its output, under
//! ingestion scope. The output is stored and never changes; samples older
//! than a closed pane are dropped. The first pane a definition sees may have
//! started before the data plane did, so it is never built: queries that
//! need it are not answered from stored panes.
//!
//! Only samples of a metric some definition reads move the watermark, and a
//! sample further ahead of the wall clock than `max_future_ms` is dropped, so
//! one bad timestamp cannot close every pane up to it. When the watermark
//! jumps, panes that would already be evicted are skipped, not built.

use std::collections::BTreeMap;
use std::sync::Arc;

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::PhysicalASAPDAG;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef};
use asap_executor::{RawSampleSelector, Sample};
use futures::executor::block_on;
use futures::StreamExt;

use crate::sds::{
    DefinitionId, Sds, Selection, SummaryDefinition, SummaryInstanceKey, SummaryStore,
};

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
    #[error("stored summaries: {0}")]
    Plan(String),
    #[error("stored summary {0:?}: {1}")]
    Definition(DefinitionId, String),
    #[error(transparent)]
    Executor(#[from] asap_executor::Error),
}

/// Counters for what happened to incoming samples, each sample counted once
/// (`samples_*`), and to panes, each pane of each definition counted once
/// (`panes_*`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PrecomputeStats {
    /// Samples buffered for at least one pane.
    pub samples_accepted: u64,
    /// Samples of a pane already built or skipped, other than the first
    /// pane after startup.
    pub samples_dropped_late: u64,
    /// Samples of the first pane after startup, which is never built.
    pub samples_skipped_first_pane: u64,
    /// Samples more than `max_future_ms` ahead of the wall clock.
    pub samples_dropped_future: u64,
    /// Samples of a metric no stored summary reads.
    pub samples_unrouted: u64,
    /// Panes built and stored.
    pub panes_built: u64,
    /// Panes whose build failed; they are skipped, not retried.
    pub panes_failed: u64,
    /// Panes not built because they would already be evicted when the
    /// watermark reached them.
    pub panes_skipped_expired: u64,
}

/// How the precompute engine treats sample times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrecomputeOptions {
    /// A pane closes when the newest routed sample is this far past its end.
    pub allowed_lateness_ms: u64,
    /// Samples further ahead of the wall clock than this are dropped.
    pub max_future_ms: u64,
}

impl Default for PrecomputeOptions {
    fn default() -> Self {
        Self {
            allowed_lateness_ms: 15_000,
            max_future_ms: 5 * 60_000,
        }
    }
}

struct Builder {
    definition: SummaryDefinition,
    /// Turns a pane's samples into the build's input: the `Scan`'s label
    /// matchers, then the pane's `TimeRange` rows.
    selector: RawSampleSelector,
    raw_schema: SchemaRef,
    build: CompiledPhysicalDAG,
    /// Samples of open panes, by pane end.
    open: BTreeMap<i64, Vec<RawSample>>,
    /// End of the first pane seen, which is skipped. `None` until then.
    first_pane_end: Option<i64>,
    /// End of the newest pane built or skipped; samples in it or before it
    /// are dropped. `None` until the first sample.
    built_through: Option<i64>,
}

/// What one definition did with one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Late,
    FirstPane,
    Buffered,
}

pub struct Precompute {
    builders: Vec<Builder>,
    allowed_lateness_ms: i64,
    max_future_ms: i64,
    newest_sample_ms: Option<i64>,
    /// The newest wall-clock time an ingest ran at: the watermark never
    /// passes it, so a series stamped ahead of the others cannot close
    /// panes early.
    newest_clock_ms: Option<i64>,
    pub store: SummaryStore<Batch>,
    pub stats: PrecomputeStats,
}

impl Precompute {
    /// Compiles one build per definition of `sds` built in an ingestion run,
    /// in dependency order; `sds` describes `dag`.
    pub fn new(
        dag: PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
    ) -> Result<Self, PrecomputeError> {
        let mut builders = Vec::new();
        // Ingestion-run definitions in dependency order; query-run ones are
        // the query engine's.
        let order = sds
            .ingestion_build_order()
            .map_err(|e| PrecomputeError::Plan(e.to_string()))?;
        for id in &order {
            let definition = &sds.definitions[id];
            let error =
                |message: String| PrecomputeError::Definition(definition.id.clone(), message);
            if !definition.stored_inputs.is_empty() {
                return Err(error(
                    "built from other stored outputs, which the precompute engine cannot build yet"
                        .into(),
                ));
            }
            if definition.pane_ms <= 0 {
                return Err(error(format!("pane width {} ms", definition.pane_ms)));
            }
            let raw_schema = Arc::new(output_schema(&dag, definition.raw_input()).clone());
            // `window_batch` is called with each pane's end, so the window's
            // offset from evaluation time is not used.
            let selector = RawSampleSelector::new(&dag, &definition.window)?;
            let build = compile(
                &dag,
                BTreeMap::from([(
                    definition.raw_input(),
                    InputContract::bounded(raw_schema.clone()),
                )]),
                &[definition.builder],
            )?;
            builders.push(Builder {
                definition: definition.clone(),
                selector,
                raw_schema,
                build,
                open: BTreeMap::new(),
                first_pane_end: None,
                built_through: None,
            });
        }
        let millis = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        Ok(Self {
            builders,
            allowed_lateness_ms: millis(options.allowed_lateness_ms),
            max_future_ms: millis(options.max_future_ms),
            newest_sample_ms: None,
            newest_clock_ms: None,
            store: SummaryStore::new(),
            stats: PrecomputeStats::default(),
        })
    }

    /// The watermark: no sample older than this is expected any more. It is
    /// the newest routed sample, capped at the wall clock, minus the allowed
    /// lateness.
    pub fn watermark_ms(&self) -> Option<i64> {
        let newest = match self.newest_clock_ms {
            Some(clock) => self.newest_sample_ms?.min(clock),
            None => self.newest_sample_ms?,
        };
        Some(newest.saturating_sub(self.allowed_lateness_ms))
    }

    /// [`Self::ingest_at`] the current wall-clock time.
    pub fn ingest(&mut self, samples: &[RawSample]) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        self.ingest_at(samples, now_ms);
    }

    /// Buffers `samples` into their panes, then builds every pane the
    /// watermark has passed. `now_ms` is the wall-clock time that bounds
    /// sample times from above.
    pub fn ingest_at(&mut self, samples: &[RawSample], now_ms: i64) {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
        let latest_allowed_ms = now_ms.saturating_add(self.max_future_ms);
        for sample in samples {
            let metric = sample.labels.get("__name__").map(String::as_str);
            if !self
                .builders
                .iter()
                .any(|b| metric == Some(b.selector.metric()))
            {
                self.stats.samples_unrouted += 1;
                continue;
            }
            if sample.timestamp_ms > latest_allowed_ms {
                self.stats.samples_dropped_future += 1;
                continue;
            }
            let mut outcome: Option<Outcome> = None;
            for builder in &mut self.builders {
                if metric != Some(builder.selector.metric()) {
                    continue;
                }
                let this = match pane_end(sample.timestamp_ms, builder.definition.pane_ms) {
                    None => Outcome::Late,
                    Some(end) => {
                        let first = *builder.first_pane_end.get_or_insert(end);
                        let built = *builder.built_through.get_or_insert(end);
                        if end == first {
                            Outcome::FirstPane
                        } else if end <= built {
                            Outcome::Late
                        } else {
                            builder.open.entry(end).or_default().push(sample.clone());
                            Outcome::Buffered
                        }
                    }
                };
                outcome = outcome.max(Some(this));
            }
            match outcome {
                None => self.stats.samples_unrouted += 1,
                Some(outcome) => {
                    match outcome {
                        Outcome::Buffered => self.stats.samples_accepted += 1,
                        Outcome::FirstPane => self.stats.samples_skipped_first_pane += 1,
                        Outcome::Late => self.stats.samples_dropped_late += 1,
                    }
                    self.newest_sample_ms = self.newest_sample_ms.max(Some(sample.timestamp_ms));
                }
            }
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
            let pane_ms = builder.definition.pane_ms;
            let id = &builder.definition.id;
            // The newest evaluation stored panes can serve is at the end of
            // the newest closed pane. Panes are kept for one extra pane
            // width beyond the plan's retention, so an evaluation one pane
            // behind it is still served; older panes are no longer read:
            // skip them.
            let newest_closed_end = watermark.div_euclid(pane_ms).saturating_mul(pane_ms);
            let oldest_kept_end = builder
                .definition
                .oldest_kept_end_ms(newest_closed_end.saturating_sub(pane_ms));
            let Some(mut end) = built.checked_add(pane_ms) else {
                continue;
            };
            if end < oldest_kept_end {
                let Some(first_kept) = pane_end(oldest_kept_end, pane_ms) else {
                    continue;
                };
                self.stats.panes_skipped_expired +=
                    (first_kept.saturating_sub(end) / pane_ms) as u64;
                builder.open = builder.open.split_off(&first_kept);
                end = first_kept;
            }
            // Every pane after the first through the watermark is built, so
            // a pane without samples is stored as empty, not missing.
            while end <= watermark {
                let samples = builder.open.remove(&end).unwrap_or_default();
                match build_pane(builder, end, &samples) {
                    Ok(state) => {
                        self.store.insert(
                            SummaryInstanceKey::new(id.clone(), Selection::ending_at(end)),
                            state,
                        );
                        self.stats.panes_built += 1;
                    }
                    Err(error) => {
                        tracing::error!(definition = ?id, end, %error, "pane build failed");
                        self.stats.panes_failed += 1;
                    }
                }
                builder.built_through = Some(end);
                match end.checked_add(pane_ms) {
                    Some(next) => end = next,
                    None => break,
                }
            }
            self.store
                .evict_before(id, Selection::ending_at(oldest_kept_end));
        }
    }
}

/// End of the tumbling pane of width `pane_ms` that contains `timestamp_ms`,
/// or `None` when it does not fit in an `i64`. Panes are
/// `(end - pane_ms, end]`, like PromQL ranges: a sample at a pane boundary
/// belongs to the pane it ends.
pub fn pane_end(timestamp_ms: i64, pane_ms: i64) -> Option<i64> {
    timestamp_ms
        .checked_neg()?
        .checked_div_euclid(pane_ms)?
        .checked_neg()?
        .checked_mul(pane_ms)
}

/// Selects the pane's rows from `samples` with the plan's `Scan` matchers,
/// then runs the definition's build over them, for the pane ending at `end`.
fn build_pane(
    builder: &Builder,
    end: i64,
    samples: &[RawSample],
) -> Result<Batch, PrecomputeError> {
    let scope = Scope::Ingestion {
        window_start_ms: end - builder.definition.pane_ms,
        window_end_ms: end,
        revision: 1,
    };
    // The selector takes owned label maps, so each sample's labels are
    // copied once per pane build.
    let samples: Vec<Sample> = samples
        .iter()
        .map(|s| Sample {
            labels: (*s.labels).clone(),
            timestamp_ms: s.timestamp_ms,
            value: s.value,
        })
        .collect();
    let selected = builder.selector.window_batch(end, &samples)?;
    let input: Source<'_> = Box::new(Operator::source(
        builder.raw_schema.clone(),
        vec![selected],
    )?);
    let build = builder
        .build
        .instantiate(BTreeMap::from([(builder.definition.raw_input(), input)]))?;
    let rows = run_root(&build, builder.definition.builder, scope)?;
    let schema = Arc::new(builder.definition.output_schema.clone());
    Ok(Batch::try_new(schema, rows)?)
}

/// Executes `root` of `dag` to completion and returns its rows.
pub fn run_root(
    dag: &asap_executor::plan::PhysicalDAG<'_, Batch, SchemaRef>,
    root: u64,
    scope: Scope,
) -> Result<Vec<Vec<asap_executor::values::Value>>, asap_executor::Error> {
    block_on(async {
        let context = RunContext::new(scope, Limits::default())?;
        let mut output = dag.execute(&[root], context)?.remove(0);
        let mut rows = Vec::new();
        while let Some(batch) = output.next().await {
            rows.extend(batch?.to_rows());
        }
        Ok(rows)
    })
}

fn output_schema(dag: &PhysicalASAPDAG, id: u64) -> &asap_executor::planner::ir::schema::Schema {
    &dag.nodes
        .iter()
        .find(|n| n.id as u64 == id)
        .expect("SDS names DAG nodes")
        .output_schema
}
