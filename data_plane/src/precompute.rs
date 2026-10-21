//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! A pane `(end - pane, end]` of a [`SummaryDefinition`] closes when the
//! watermark (the newest sample time minus the allowed lateness) reaches
//! `end`. Where the definition's shape allows ([`crate::per_sample`]), each
//! sample of its `Scan`'s metric updates its series' summary state in the
//! open pane in place, and closing the pane seals the states into one batch
//! built from columns. Otherwise the engine buffers the pane's samples, and
//! closing it selects the pane's rows with the `Scan`'s label matchers and
//! runs the definition's build from its `TimeRange` input up to its output,
//! under ingestion scope. The output is stored and never changes; samples
//! older than a closed pane are dropped. The first pane a definition sees
//! may have started before the data plane did, so it is never built:
//! queries that need it are not answered from stored panes.
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
use asap_executor::RawSampleSelector;
use futures::executor::block_on;
use futures::StreamExt;

use asap_planner::StoredOutputBuild;

use crate::per_sample::{OpenPanes as OpenStates, PerSampleSpec, Taken};
use crate::raw_sample_window::RawSampleWindowBatcher;
use crate::sds::{DefinitionId, Sds, SummaryDefinition, SummaryInstanceKey, SummaryStore};
use crate::series_ids::Series;

/// One raw sample: its series (interned: its id and full label set,
/// `__name__` included), time and value.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub labels: Arc<Series>,
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
    /// Samples, of a stored output built per sample, whose time is not
    /// after their series' previous sample in the pane (a resent sample,
    /// for instance): dropped, as Prometheus drops them.
    pub samples_dropped_duplicate: u64,
    /// Samples of the first pane after startup, which is never built.
    pub samples_skipped_first_pane: u64,
    /// Samples more than `max_future_ms` ahead of the wall clock.
    pub samples_dropped_future: u64,
    /// Samples of a metric no stored summary reads.
    pub samples_unrouted: u64,
    /// Samples of a metric only stored outputs built in a `QueryRun` read: they move the
    /// watermark, which says when such a summary is closed, but are not
    /// buffered (the query engine reads them from Prometheus).
    pub samples_query_run_only: u64,
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

/// A summary instance as stored in the SDS: its rows, in the batches its
/// build produced (one, when built per sample). Batches share their
/// buffers when cloned, so binding a stored pane to a query copies nothing.
#[derive(Debug, Clone, Default)]
pub struct SummaryInstance {
    pub batches: Vec<Batch>,
}

impl SummaryInstance {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(Batch::num_rows).sum()
    }

    /// Every row, for tests; the data plane reads rows only to encode an
    /// answer.
    pub fn to_rows(&self) -> Vec<Vec<asap_executor::values::Value>> {
        self.batches.iter().flat_map(Batch::to_rows).collect()
    }
}

/// How one definition's summaries are built from raw samples with the
/// plan's own build. The precompute engine builds stored outputs built in
/// an `IngestionRun` with it where it does not build them per sample, and
/// the query engine `QueryRun` ones.
pub struct SummaryBuild {
    pub definition: SummaryDefinition,
    /// The definition's `RawSampleWindow`: a coverage's raw samples to the
    /// build's input batch, column by column, with the `Scan`'s label
    /// matchers.
    pub(crate) window: RawSampleWindowBatcher,
    /// The `TimeRange` node the window's batch feeds.
    raw_input: u64,
    build: CompiledPhysicalDAG,
}

impl SummaryBuild {
    /// Compiles `definition`'s build from its `TimeRange` up to its builder.
    /// A definition built from other stored outputs is refused until the
    /// engine can build it once its inputs are sealed.
    pub fn new(
        dag: &PhysicalASAPDAG,
        definition: &SummaryDefinition,
    ) -> Result<Self, PrecomputeError> {
        let error = |message: String| PrecomputeError::Definition(definition.id.clone(), message);
        if !definition.stored_inputs.is_empty() {
            return Err(error(
                "built from other stored outputs, which the data plane cannot build yet".into(),
            ));
        }
        if definition.pane_ms <= 0 {
            return Err(error(format!("pane width {} ms", definition.pane_ms)));
        }
        // `batch` is called with each coverage's end, so the window's
        // offset from evaluation time is not used.
        // A series is cached for as long as the definition's summaries are kept.
        let keep_ms = definition.retention_ms.saturating_add(definition.pane_ms);
        let Some(raw) = &definition.window else {
            return Err(error("reads no raw-sample window".into()));
        };
        let raw_input = raw.time_range_node_id as u64;
        let window = RawSampleWindowBatcher::new(dag, raw, keep_ms)?;
        let build = compile(
            dag,
            BTreeMap::from([(raw_input, InputContract::bounded(window.schema.clone()))]),
            &[definition.builder],
        )?;
        Ok(Self {
            definition: definition.clone(),
            raw_input,
            window,
            build,
        })
    }

    /// The metric whose samples the build reads.
    pub fn metric(&self) -> &str {
        self.window.metric()
    }

    /// Selects the coverage ending at `end_ms` from `samples` with the plan's
    /// `Scan` matchers, then runs the build over those rows under `scope`.
    pub fn run(
        &self,
        end_ms: i64,
        samples: &[RawSample],
        scope: Scope,
    ) -> Result<SummaryInstance, PrecomputeError> {
        self.run_batch(self.window.batch(end_ms, samples)?, scope)
    }

    /// The build over `input`, a coverage's selected input batch.
    pub(crate) fn run_batch(
        &self,
        input: Batch,
        scope: Scope,
    ) -> Result<SummaryInstance, PrecomputeError> {
        let input: Source<'_> = Box::new(Operator::source(input.schema().clone(), vec![input])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.raw_input, input)]))?;
        Ok(SummaryInstance {
            batches: run(&build, self.build.roots(), scope)?,
        })
    }
}

struct Builder {
    summary: SummaryBuild,
    /// The build run per sample, unless its shape keeps the plan's build.
    per_sample: Option<PerSampleSpec>,
    /// For the plan's build: samples of open panes, by pane end.
    open: BTreeMap<i64, Vec<RawSample>>,
    /// For the per-sample build: the open panes' summary states.
    states: OpenStates,
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
    Duplicate,
    FirstPane,
    Buffered,
}

pub struct Precompute {
    builders: Vec<Builder>,
    /// Metrics read by stored outputs built in a `QueryRun`, with each one's newest routed
    /// sample time.
    query_run_metrics: BTreeMap<String, Option<i64>>,
    allowed_lateness_ms: i64,
    max_future_ms: i64,
    newest_sample_ms: Option<i64>,
    /// The newest wall-clock time an ingest ran at: the watermark never
    /// passes it, so a series stamped ahead of the others cannot close
    /// panes early.
    newest_clock_ms: Option<i64>,
    /// The newest event-time barrier a producer declared: every pane ending
    /// at or before it is closed.
    barrier_ms: Option<i64>,
    pub store: SummaryStore<SummaryInstance>,
    pub stats: PrecomputeStats,
}

impl Precompute {
    /// Compiles one build per definition of `sds` built in an ingestion run,
    /// in dependency order; `sds` describes `dag`. A definition is built per
    /// sample where its shape allows ([`crate::per_sample`]), else with the
    /// plan's build over each pane's samples.
    pub fn new(
        dag: PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
    ) -> Result<Self, PrecomputeError> {
        Self::with(&dag, sds, options, true)
    }

    /// [`Self::new`] building every pane with the plan's own build over its
    /// samples: the reference the per-sample builds equal.
    pub fn batch(
        dag: &PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
    ) -> Result<Self, PrecomputeError> {
        Self::with(dag, sds, options, false)
    }

    /// How many definitions are built per sample.
    pub fn per_sample_definitions(&self) -> usize {
        self.builders
            .iter()
            .filter(|b| b.per_sample.is_some())
            .count()
    }

    fn with(
        dag: &PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
        per_sample: bool,
    ) -> Result<Self, PrecomputeError> {
        let dag = dag.clone();
        let mut builders = Vec::new();
        // `IngestionRun` definitions in dependency order; `QueryRun` ones are
        // the query engine's.
        let order = sds
            .ingestion_build_order()
            .map_err(|e| PrecomputeError::Plan(e.to_string()))?;
        for id in &order {
            let summary = SummaryBuild::new(&dag, &sds.definitions[id])?;
            let per_sample = per_sample
                .then(|| PerSampleSpec::new(&dag, &summary))
                .flatten();
            tracing::info!(definition = ?id, per_sample = per_sample.is_some(), "pane build");
            builders.push(Builder {
                summary,
                per_sample,
                open: BTreeMap::new(),
                states: OpenStates::default(),
                first_pane_end: None,
                built_through: None,
            });
        }
        // Samples of a metric only stored outputs built in a `QueryRun` read move the
        // watermark, which says when such a summary's coverage is closed.
        // A `QueryRun` definition whose selector does not build is not
        // tracked here; the query engine forwards the queries that read it.
        let mut query_run_metrics = BTreeMap::new();
        for definition in sds.definitions.values() {
            if definition.built_at == StoredOutputBuild::QueryRun {
                let Some(window) = &definition.window else {
                    continue;
                };
                match RawSampleSelector::new(&dag, window) {
                    Ok(selector) => {
                        query_run_metrics.insert(selector.metric().to_string(), None);
                    }
                    Err(error) => tracing::warn!(
                        definition = ?definition.id, %error,
                        "stored output built in a `QueryRun` cannot be built; its queries are forwarded"
                    ),
                }
            }
        }
        let millis = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        Ok(Self {
            builders,
            query_run_metrics,
            allowed_lateness_ms: millis(options.allowed_lateness_ms),
            max_future_ms: millis(options.max_future_ms),
            newest_sample_ms: None,
            newest_clock_ms: None,
            barrier_ms: None,
            store: SummaryStore::new(),
            stats: PrecomputeStats::default(),
        })
    }

    /// What ingestion has seen, for the query engine's store decisions: the
    /// watermark and the newest sample of each metric a `QueryRun` stored
    /// output reads.
    pub fn progress(&self) -> crate::query::IngestionProgress {
        crate::query::IngestionProgress {
            watermark_ms: self.watermark_ms(),
            newest_sample_ms: self
                .query_run_metrics
                .iter()
                .filter_map(|(metric, newest)| Some((metric.clone(), (*newest)?)))
                .collect(),
        }
    }

    /// The watermark: no sample older than this is expected any more. It is
    /// the newest routed sample, capped at the wall clock, minus the allowed
    /// lateness, or the newest barrier if that is later.
    pub fn watermark_ms(&self) -> Option<i64> {
        let newest = match self.newest_clock_ms {
            Some(clock) => self.newest_sample_ms.map(|newest| newest.min(clock)),
            None => self.newest_sample_ms,
        };
        let from_samples = newest.map(|newest| newest.saturating_sub(self.allowed_lateness_ms));
        from_samples.max(self.barrier_ms)
    }

    /// [`Self::advance_watermark_at`] the current wall-clock time.
    pub fn advance_watermark(&mut self, barrier_ms: i64) -> i64 {
        self.advance_watermark_at(barrier_ms, wall_clock_ms())
    }

    /// An event-time barrier: the producer states that every sample at or
    /// before `barrier_ms` has been written. Closes every pane with
    /// `end <= barrier_ms` (panes are `(end - width, end]`), so a pane whose
    /// series stopped sending closes without a later sample. The barrier is
    /// capped at `now_ms`; a repeated or lower barrier does nothing. A sample
    /// that arrives later for a pane the barrier closed counts as late.
    /// Returns the barrier in force.
    pub fn advance_watermark_at(&mut self, barrier_ms: i64, now_ms: i64) -> i64 {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
        let barrier_ms = barrier_ms.min(now_ms);
        if self.barrier_ms.is_some_and(|current| current >= barrier_ms) {
            return self.barrier_ms.unwrap_or(barrier_ms);
        }
        self.barrier_ms = Some(barrier_ms);
        self.close_panes();
        barrier_ms
    }

    /// [`Self::ingest_at`] the current wall-clock time.
    pub fn ingest(&mut self, samples: &[RawSample]) {
        self.ingest_at(samples, wall_clock_ms());
    }

    /// Buffers `samples` into their panes, then builds every pane the
    /// watermark has passed. `now_ms` is the wall-clock time that bounds
    /// sample times from above.
    pub fn ingest_at(&mut self, samples: &[RawSample], now_ms: i64) {
        self.newest_clock_ms = self.newest_clock_ms.max(Some(now_ms));
        let latest_allowed_ms = now_ms.saturating_add(self.max_future_ms);
        for sample in samples {
            let metric = Some(&*sample.labels.metric).filter(|m| !m.is_empty());
            let query_run = metric.is_some_and(|m| self.query_run_metrics.contains_key(m));
            if !query_run
                && !self
                    .builders
                    .iter()
                    .any(|b| metric == Some(b.summary.metric()))
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
                if metric != Some(builder.summary.metric()) {
                    continue;
                }
                let this = match pane_end(sample.timestamp_ms, builder.summary.definition.pane_ms) {
                    None => Outcome::Late,
                    Some(end) => {
                        let first = *builder.first_pane_end.get_or_insert(end);
                        let built = *builder.built_through.get_or_insert(end);
                        if end == first {
                            Outcome::FirstPane
                        } else if end <= built {
                            Outcome::Late
                        } else {
                            match &builder.per_sample {
                                // A series the Scan does not select counts
                                // as taken, as the plan's build takes and
                                // then filters it.
                                Some(spec) => {
                                    let window = &builder.summary.window;
                                    match spec.update(window, &mut builder.states, sample, end) {
                                        Taken::Taken => Outcome::Buffered,
                                        Taken::Duplicate => Outcome::Duplicate,
                                    }
                                }
                                None => {
                                    builder.open.entry(end).or_default().push(sample.clone());
                                    Outcome::Buffered
                                }
                            }
                        }
                    }
                };
                outcome = outcome.max(Some(this));
            }
            match outcome {
                None => self.stats.samples_query_run_only += 1,
                Some(Outcome::Buffered) => self.stats.samples_accepted += 1,
                Some(Outcome::FirstPane) => self.stats.samples_skipped_first_pane += 1,
                Some(Outcome::Late) => self.stats.samples_dropped_late += 1,
                Some(Outcome::Duplicate) => self.stats.samples_dropped_duplicate += 1,
            }
            self.newest_sample_ms = self.newest_sample_ms.max(Some(sample.timestamp_ms));
            if let Some(newest) = metric.and_then(|m| self.query_run_metrics.get_mut(m)) {
                *newest = (*newest).max(Some(sample.timestamp_ms));
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
            let pane_ms = builder.summary.definition.pane_ms;
            let keep_ms = builder
                .summary
                .definition
                .retention_ms
                .saturating_add(pane_ms);
            let id = &builder.summary.definition.id;
            // The newest evaluation stored panes can serve is at the end of
            // the newest closed pane. Panes are kept for one extra pane
            // width beyond the plan's retention, so an evaluation one pane
            // behind it is still served; older panes are no longer read:
            // skip them.
            let newest_closed_end = watermark.div_euclid(pane_ms).saturating_mul(pane_ms);
            let oldest_kept_end = builder
                .summary
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
                builder.states.drop_before(first_kept);
                end = first_kept;
            }
            // Every pane after the first through the watermark is built, so
            // a pane without samples is stored as empty, not missing.
            while end <= watermark {
                let scope = Scope::Ingestion {
                    window_start_ms: end - pane_ms,
                    window_end_ms: end,
                    revision: 1,
                };
                let built = match &builder.per_sample {
                    // The pane's states, sealed into one batch.
                    Some(spec) => spec
                        .close(&builder.summary, &mut builder.states, end, keep_ms, &scope)
                        .map(|batch| SummaryInstance {
                            batches: vec![batch],
                        }),
                    None => {
                        let samples = builder.open.remove(&end).unwrap_or_default();
                        builder
                            .summary
                            .run(end, &samples, scope)
                            .map_err(|e| e.to_string())
                    }
                };
                match built {
                    Ok(state) => {
                        self.store.insert(
                            SummaryInstanceKey::new(
                                id.clone(),
                                builder.summary.definition.selection_ending_at(end),
                            ),
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
            self.store.evict_before(
                id,
                &builder
                    .summary
                    .definition
                    .selection_ending_at(oldest_kept_end),
            );
        }
    }
}

/// Milliseconds since the Unix epoch.
fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
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

/// Executes `dag`'s single root to completion and returns its batches.
pub fn run(
    dag: &asap_executor::plan::PhysicalDAG<'_, Batch, SchemaRef>,
    roots: &[u64],
    scope: Scope,
) -> Result<Vec<Batch>, asap_executor::Error> {
    block_on(async {
        let context = RunContext::new(scope, Limits::default())?;
        let mut output = dag.execute(roots, context)?.remove(0);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series_ids::intern;

    const MINUTE: i64 = 60_000;

    fn sample(job: &str, timestamp_ms: i64) -> RawSample {
        RawSample {
            labels: intern(&BTreeMap::from([
                ("__name__".to_string(), "latency_ms".to_string()),
                ("job".to_string(), job.to_string()),
            ])),
            timestamp_ms,
            value: 1.0,
        }
    }

    /// Per-sample state follows the active series: an open pane holds a
    /// state only for the series that took a sample in it, and a series
    /// unseen for longer than the retention plus one pane is forgotten,
    /// with its cached cells. Example 4b: 10-minute KLL panes kept 60 min.
    #[test]
    fn per_series_state_follows_the_active_series() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../examples/planner-layering-example4b.json");
        let workload = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let mut inputs = control_plane::DeploymentInputs::default();
        inputs.deployment_capabilities.raw_data_retained = false;
        let plan = control_plane::plan_workload(&workload, &inputs).unwrap();
        let sds = Sds::from_plan(&plan).unwrap();
        let options = PrecomputeOptions {
            allowed_lateness_ms: 0,
            ..PrecomputeOptions::default()
        };
        let mut engine = Precompute::new(plan.dag.dag, &sds, options).unwrap();
        assert_eq!(
            (
                sds.definitions.len(),
                engine.builders.len(),
                engine.per_sample_definitions()
            ),
            (1, 1, 1)
        );
        let jobs: Vec<String> = (0..10).map(|i| format!("job{i}")).collect();
        let mut samples = Vec::new();
        for t in (MINUTE..25 * MINUTE).step_by(15_000) {
            samples.extend(jobs.iter().map(|job| sample(job, t)));
        }
        engine.ingest_at(&samples, 300 * MINUTE);
        let builder = &engine.builders[0];
        assert_eq!(builder.states.sizes(), (10, vec![10]), "(20, 30 min] open");
        let only_a: Vec<RawSample> = (25 * MINUTE..200 * MINUTE)
            .step_by(15_000)
            .map(|t| sample("job0", t))
            .collect();
        engine.ingest_at(&only_a, 300 * MINUTE);
        let builder = &engine.builders[0];
        assert_eq!(builder.states.sizes(), (1, vec![1]));
        assert!(builder.summary.window.cached_series() <= 1);
        assert_eq!(engine.stats.panes_failed, 0);
    }
}
