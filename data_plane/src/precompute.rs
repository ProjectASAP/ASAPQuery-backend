//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! The plan's ingestion side is compiled once ([`Ingestion`]) and shared by
//! the ingestion workers ([`Shared`]), each of which owns a shard of the
//! series. For every [`SummaryDefinition`] a worker buffers its shard's
//! samples of the definition's `Scan` metric by tumbling pane. A pane
//! `(end - pane, end]` closes when the global watermark (the minimum over
//! the workers of their newest sample, minus the allowed lateness) reaches
//! `end`. Closing it takes every worker's samples of the pane, selects the
//! pane's rows with the executor's `RawSampleSelector` (the `Scan`'s label
//! matchers), then runs the definition's build from its `TimeRange` input
//! up to its output, under ingestion scope. The output is stored and never
//! changes; samples older than a closed pane are dropped. The first pane a
//! definition sees may have started before the data plane did, so it is
//! never built: queries that need it are not answered from stored panes.
//!
//! Only samples of a metric some definition reads move the watermark, and a
//! sample further ahead of the wall clock than `max_future_ms` is dropped, so
//! one bad timestamp cannot close every pane up to it. When the watermark
//! jumps, panes that would already be evicted are skipped, not built.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::planner::ir::physical_export::PhysicalASAPDAG;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef};
use asap_executor::{RawSampleSelector, Sample};
use futures::executor::block_on;
use futures::StreamExt;

use asap_planner::StoredOutputBuild;

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
    /// Samples of a metric only query-run summaries read: they move the
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

/// How one definition's summaries are built from raw samples. The
/// precompute engine builds ingestion-run summaries with it, and the query
/// engine query-run ones.
pub struct SummaryBuild {
    pub definition: SummaryDefinition,
    /// Turns a coverage's samples into the build's input: the `Scan`'s label
    /// matchers, then the coverage's `TimeRange` rows.
    selector: RawSampleSelector,
    raw_schema: SchemaRef,
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
        let raw_schema = Arc::new(output_schema(dag, definition.raw_input()).clone());
        // `window_batch` is called with each coverage's end, so the window's
        // offset from evaluation time is not used.
        let selector = RawSampleSelector::new(dag, &definition.window)?;
        let build = compile(
            dag,
            BTreeMap::from([(
                definition.raw_input(),
                InputContract::bounded(raw_schema.clone()),
            )]),
            &[definition.builder],
        )?;
        Ok(Self {
            definition: definition.clone(),
            selector,
            raw_schema,
            build,
        })
    }

    /// The metric whose samples the build reads.
    pub fn metric(&self) -> &str {
        self.selector.metric()
    }

    /// Selects the coverage ending at `end_ms` from `samples` with the plan's
    /// `Scan` matchers, then runs the build over those rows under `scope`.
    pub fn run(
        &self,
        end_ms: i64,
        samples: &[RawSample],
        scope: Scope,
    ) -> Result<Batch, PrecomputeError> {
        let selected = self
            .selector
            .window_batch(end_ms, &executor_samples(samples))?;
        let input: Source<'_> =
            Box::new(Operator::source(self.raw_schema.clone(), vec![selected])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.definition.raw_input(), input)]))?;
        let rows = run_root(&build, self.definition.builder, scope)?;
        let schema = Arc::new(self.definition.output_schema.clone());
        Ok(Batch::try_new(schema, rows)?)
    }
}

/// No value yet in an `AtomicI64` slot.
const UNSET: i64 = i64::MIN;

fn get(slot: &AtomicI64) -> Option<i64> {
    Some(slot.load(Ordering::Acquire)).filter(|&v| v != UNSET)
}

/// The slot's value, set to `value` first if it had none.
fn get_or_init(slot: &AtomicI64, value: i64) -> i64 {
    match slot.compare_exchange(UNSET, value, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => value,
        Err(current) => current,
    }
}

/// What one definition did with one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Outcome {
    Late,
    FirstPane,
    Buffered,
}

/// The plan's ingestion side, compiled once and shared by every ingestion
/// worker: one build per definition built in an ingestion run, in
/// dependency order.
pub struct Ingestion {
    builds: Vec<SummaryBuild>,
    /// Metrics read by query-run summaries.
    query_run_metrics: BTreeSet<String>,
    allowed_lateness_ms: i64,
    max_future_ms: i64,
}

impl Ingestion {
    /// Compiles one build per definition of `sds` built in an ingestion run;
    /// `sds` describes `dag`.
    pub fn new(
        dag: &PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
    ) -> Result<Self, PrecomputeError> {
        let mut builds = Vec::new();
        // Ingestion-run definitions in dependency order; query-run ones are
        // the query engine's.
        let order = sds
            .ingestion_build_order()
            .map_err(|e| PrecomputeError::Plan(e.to_string()))?;
        for id in &order {
            builds.push(SummaryBuild::new(dag, &sds.definitions[id])?);
        }
        // Samples of a metric only query-run summaries read move the
        // watermark, which says when such a summary's coverage is closed.
        // A query-run definition whose selector does not build is not
        // tracked here; the query engine forwards the queries that read it.
        let mut query_run_metrics = BTreeSet::new();
        for definition in sds.definitions.values() {
            if definition.built_at == StoredOutputBuild::QueryRun {
                match RawSampleSelector::new(dag, &definition.window) {
                    Ok(selector) => {
                        query_run_metrics.insert(selector.metric().to_string());
                    }
                    Err(error) => tracing::warn!(
                        definition = ?definition.id, %error,
                        "query-run summary cannot be built; its queries are forwarded"
                    ),
                }
            }
        }
        let millis = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
        Ok(Self {
            builds,
            query_run_metrics,
            allowed_lateness_ms: millis(options.allowed_lateness_ms),
            max_future_ms: millis(options.max_future_ms),
        })
    }
}

/// One ingestion worker's open panes: per definition, its shard's samples
/// of each open pane, by pane end. Only the worker writes them; the thread
/// that closes a pane takes every worker's samples of it.
struct OpenPanes {
    samples: Vec<BTreeMap<i64, Vec<RawSample>>>,
}

/// The precompute counters, shared by every worker.
#[derive(Default)]
struct Counters {
    samples_accepted: AtomicU64,
    samples_dropped_late: AtomicU64,
    samples_skipped_first_pane: AtomicU64,
    samples_dropped_future: AtomicU64,
    samples_unrouted: AtomicU64,
    samples_query_run_only: AtomicU64,
    panes_built: AtomicU64,
    panes_failed: AtomicU64,
    panes_skipped_expired: AtomicU64,
}

fn add(counter: &AtomicU64, n: u64) {
    if n > 0 {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

/// Precompute state every ingestion worker and query thread shares: the
/// compiled builds, each worker's open panes and newest sample, the
/// watermark's other inputs, the stored summaries and the counters.
///
/// Series are sharded across workers ([`shard_of`]), so each worker's open
/// panes hold only its shard. The global watermark is the minimum over the
/// workers that have seen a sample of their newest sample, capped at the
/// wall clock, minus the allowed lateness, or the newest barrier if that is
/// later. The worker (or barrier) that moves it past a pane end closes the
/// pane: under every worker's lock, it takes each worker's samples of the
/// pane and builds it once, so a pane never waits for an idle worker and a
/// group spanning workers is built whole.
pub struct Shared {
    ingestion: Ingestion,
    workers: Vec<Mutex<OpenPanes>>,
    /// Each worker's newest routed sample time.
    newest_sample_ms: Vec<AtomicI64>,
    /// Each metric query-run summaries read, with its newest sample time.
    query_run_newest_ms: BTreeMap<String, AtomicI64>,
    /// The newest wall-clock time an ingest or barrier ran at: the
    /// watermark never passes it, so a series stamped ahead of the others
    /// cannot close panes early.
    newest_clock_ms: AtomicI64,
    /// The newest event-time barrier a producer declared.
    barrier_ms: AtomicI64,
    /// Per definition, the end of the first pane any worker saw, which is
    /// never built.
    first_pane_end: Vec<AtomicI64>,
    /// Per definition, the end of the newest pane built or skipped; samples
    /// in it or before it are late. Written only under every worker's lock.
    built_through: Vec<AtomicI64>,
    /// The watermark panes are closed through, and whether a definition saw
    /// its first sample since, which may leave panes to close at the same
    /// watermark.
    closed_through_ms: AtomicI64,
    new_definition: AtomicBool,
    /// Serializes closing.
    closing: Mutex<()>,
    /// Stored summaries, shared with the query threads.
    pub store: Arc<SummaryStore<Batch>>,
    counters: Counters,
}

impl Shared {
    /// `workers` ingestion workers (at least one) running `ingestion`.
    pub fn new(ingestion: Ingestion, workers: usize) -> Self {
        let definitions = ingestion.builds.len();
        let slots = |n: usize| (0..n).map(|_| AtomicI64::new(UNSET)).collect::<Vec<_>>();
        Self {
            workers: (0..workers.max(1))
                .map(|_| {
                    Mutex::new(OpenPanes {
                        samples: vec![BTreeMap::new(); definitions],
                    })
                })
                .collect(),
            newest_sample_ms: slots(workers.max(1)),
            query_run_newest_ms: ingestion
                .query_run_metrics
                .iter()
                .map(|metric| (metric.clone(), AtomicI64::new(UNSET)))
                .collect(),
            newest_clock_ms: AtomicI64::new(UNSET),
            barrier_ms: AtomicI64::new(UNSET),
            first_pane_end: slots(definitions),
            built_through: slots(definitions),
            closed_through_ms: AtomicI64::new(UNSET),
            new_definition: AtomicBool::new(false),
            closing: Mutex::new(()),
            store: Arc::new(SummaryStore::new()),
            counters: Counters::default(),
            ingestion,
        }
    }

    /// How many ingestion workers series are sharded across.
    pub fn workers(&self) -> usize {
        self.workers.len()
    }

    /// The global watermark: no sample older than this is expected any
    /// more. `None` before any routed sample or barrier.
    pub fn watermark_ms(&self) -> Option<i64> {
        let newest = self
            .newest_sample_ms
            .iter()
            .filter_map(get)
            .min()
            .map(|newest| match get(&self.newest_clock_ms) {
                Some(clock) => newest.min(clock),
                None => newest,
            });
        let from_samples =
            newest.map(|newest| newest.saturating_sub(self.ingestion.allowed_lateness_ms));
        from_samples.max(get(&self.barrier_ms))
    }

    /// What ingestion has seen, for the query engine's keep decisions: the
    /// watermark and each query-run metric's newest sample.
    pub fn progress(&self) -> crate::query::IngestionProgress {
        crate::query::IngestionProgress {
            watermark_ms: self.watermark_ms(),
            newest_sample_ms: self
                .query_run_newest_ms
                .iter()
                .filter_map(|(metric, newest)| Some((metric.clone(), get(newest)?)))
                .collect(),
        }
    }

    pub fn stats(&self) -> PrecomputeStats {
        let c = &self.counters;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        PrecomputeStats {
            samples_accepted: load(&c.samples_accepted),
            samples_dropped_late: load(&c.samples_dropped_late),
            samples_skipped_first_pane: load(&c.samples_skipped_first_pane),
            samples_dropped_future: load(&c.samples_dropped_future),
            samples_unrouted: load(&c.samples_unrouted),
            samples_query_run_only: load(&c.samples_query_run_only),
            panes_built: load(&c.panes_built),
            panes_failed: load(&c.panes_failed),
            panes_skipped_expired: load(&c.panes_skipped_expired),
        }
    }

    /// An event-time barrier at `now_ms`: the producers state that every
    /// sample at or before `barrier_ms` has been written. Closes every pane
    /// with `end <= barrier_ms` (panes are `(end - width, end]`), so a pane
    /// whose series stopped sending closes without a later sample, for
    /// every worker's shard. The barrier is capped at `now_ms`; a repeated
    /// or lower barrier does nothing. Samples at or before it that arrive
    /// later count as late. Returns the barrier in force. The caller first
    /// lets every worker run the writes acknowledged before.
    pub fn advance_watermark_at(&self, barrier_ms: i64, now_ms: i64) -> i64 {
        self.newest_clock_ms.fetch_max(now_ms, Ordering::AcqRel);
        let barrier_ms = barrier_ms.min(now_ms);
        let before = self.barrier_ms.fetch_max(barrier_ms, Ordering::AcqRel);
        if before != UNSET && before >= barrier_ms {
            return before;
        }
        self.close_panes();
        barrier_ms
    }

    /// Worker `worker` takes `samples`, all of its shard, into their open
    /// panes at wall-clock time `now_ms`, which bounds sample times from
    /// above; then every pane the global watermark has passed is closed.
    ///
    /// Until a worker has seen a sample it does not hold the watermark
    /// back, so a worker with no series never blocks it. Right after
    /// startup, the first worker to take a request may therefore close a
    /// pane before another worker takes its part of the same request; the
    /// allowed lateness covers that.
    pub fn ingest_at(&self, worker: usize, samples: &[RawSample], now_ms: i64) {
        self.take_at(worker, samples, now_ms);
        self.close_panes();
    }

    fn take_at(&self, worker: usize, samples: &[RawSample], now_ms: i64) {
        self.newest_clock_ms.fetch_max(now_ms, Ordering::AcqRel);
        let ingestion = &self.ingestion;
        let latest_allowed_ms = now_ms.saturating_add(ingestion.max_future_ms);
        let mut outcomes = [0u64; 6];
        let (late, first_pane, buffered, query_run_only, unrouted, future) = (0, 1, 2, 3, 4, 5);
        let mut newest: Option<i64> = None;
        {
            let mut open = self.workers[worker]
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for sample in samples {
                let metric = sample.labels.get("__name__").map(String::as_str);
                let query_run = metric.is_some_and(|m| ingestion.query_run_metrics.contains(m));
                if !query_run && !ingestion.builds.iter().any(|b| metric == Some(b.metric())) {
                    outcomes[unrouted] += 1;
                    continue;
                }
                if sample.timestamp_ms > latest_allowed_ms {
                    outcomes[future] += 1;
                    continue;
                }
                let mut outcome: Option<Outcome> = None;
                for (index, build) in ingestion.builds.iter().enumerate() {
                    if metric != Some(build.metric()) {
                        continue;
                    }
                    let this = match pane_end(sample.timestamp_ms, build.definition.pane_ms) {
                        None => Outcome::Late,
                        Some(end) => {
                            if get(&self.first_pane_end[index]).is_none() {
                                self.new_definition.store(true, Ordering::Release);
                            }
                            let first = get_or_init(&self.first_pane_end[index], end);
                            let built = get_or_init(&self.built_through[index], first);
                            if end == first {
                                Outcome::FirstPane
                            } else if end <= built {
                                Outcome::Late
                            } else {
                                open.samples[index]
                                    .entry(end)
                                    .or_default()
                                    .push(sample.clone());
                                Outcome::Buffered
                            }
                        }
                    };
                    outcome = outcome.max(Some(this));
                }
                outcomes[match outcome {
                    None => query_run_only,
                    Some(Outcome::Buffered) => buffered,
                    Some(Outcome::FirstPane) => first_pane,
                    Some(Outcome::Late) => late,
                }] += 1;
                newest = newest.max(Some(sample.timestamp_ms));
                if let Some(newest) = metric.and_then(|m| self.query_run_newest_ms.get(m)) {
                    newest.fetch_max(sample.timestamp_ms, Ordering::AcqRel);
                }
            }
            if let Some(newest) = newest {
                self.newest_sample_ms[worker].fetch_max(newest, Ordering::AcqRel);
            }
        }
        let c = &self.counters;
        add(&c.samples_dropped_late, outcomes[late]);
        add(&c.samples_skipped_first_pane, outcomes[first_pane]);
        add(&c.samples_accepted, outcomes[buffered]);
        add(&c.samples_query_run_only, outcomes[query_run_only]);
        add(&c.samples_unrouted, outcomes[unrouted]);
        add(&c.samples_dropped_future, outcomes[future]);
    }

    /// Builds every pane the global watermark has passed, from every
    /// worker's samples of it.
    fn close_panes(&self) {
        let closed = |watermark: i64| {
            get(&self.closed_through_ms).is_some_and(|closed| watermark <= closed)
                && !self.new_definition.load(Ordering::Acquire)
        };
        match self.watermark_ms() {
            Some(watermark) if !closed(watermark) => {}
            _ => return,
        }
        let _closing = self.closing.lock().unwrap_or_else(|e| e.into_inner());
        let mut open: Vec<_> = self
            .workers
            .iter()
            .map(|worker| worker.lock().unwrap_or_else(|e| e.into_inner()))
            .collect();
        // Under every worker's lock: no sample is being taken, so the
        // watermark read now covers every sample taken.
        let Some(watermark) = self.watermark_ms() else {
            return;
        };
        self.new_definition.store(false, Ordering::Release);
        for (index, build) in self.ingestion.builds.iter().enumerate() {
            let Some(built) = get(&self.built_through[index]) else {
                continue;
            };
            let pane_ms = build.definition.pane_ms;
            let id = &build.definition.id;
            // The newest evaluation stored panes can serve is at the end of
            // the newest closed pane. Panes are kept for one extra pane
            // width beyond the plan's retention, so an evaluation one pane
            // behind it is still served; older panes are no longer read:
            // skip them.
            let newest_closed_end = watermark.div_euclid(pane_ms).saturating_mul(pane_ms);
            let oldest_kept_end = build
                .definition
                .oldest_kept_end_ms(newest_closed_end.saturating_sub(pane_ms));
            let Some(mut end) = built.checked_add(pane_ms) else {
                continue;
            };
            if end < oldest_kept_end {
                let Some(first_kept) = pane_end(oldest_kept_end, pane_ms) else {
                    continue;
                };
                add(
                    &self.counters.panes_skipped_expired,
                    (first_kept.saturating_sub(end) / pane_ms) as u64,
                );
                for worker in open.iter_mut() {
                    let samples = &mut worker.samples[index];
                    *samples = samples.split_off(&first_kept);
                }
                end = first_kept;
            }
            // Every pane after the first through the watermark is built, so
            // a pane without samples is stored as empty, not missing.
            while end <= watermark {
                let mut samples = Vec::new();
                for worker in open.iter_mut() {
                    samples.extend(worker.samples[index].remove(&end).unwrap_or_default());
                }
                let scope = Scope::Ingestion {
                    window_start_ms: end - pane_ms,
                    window_end_ms: end,
                    revision: 1,
                };
                match build.run(end, &samples, scope) {
                    Ok(state) => {
                        self.store.insert(
                            SummaryInstanceKey::new(id.clone(), Selection::ending_at(end)),
                            state,
                        );
                        add(&self.counters.panes_built, 1);
                    }
                    Err(error) => {
                        tracing::error!(definition = ?id, end, %error, "pane build failed");
                        add(&self.counters.panes_failed, 1);
                    }
                }
                self.built_through[index].store(end, Ordering::Release);
                match end.checked_add(pane_ms) {
                    Some(next) => end = next,
                    None => break,
                }
            }
            self.store
                .evict_before(id, Selection::ending_at(oldest_kept_end));
        }
        self.closed_through_ms
            .fetch_max(watermark, Ordering::AcqRel);
    }
}

/// The ingestion worker of `workers` that owns the series `labels`, by a
/// hash of its label set.
pub fn shard_of(labels: &BTreeMap<String, String>, workers: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    labels.hash(&mut hasher);
    (hasher.finish() % workers.max(1) as u64) as usize
}

/// Every ingestion worker of a plan on the caller's thread, each fed its
/// shard of the series: the threaded engine's precompute without threads.
pub struct Precompute {
    shared: Arc<Shared>,
    /// Stored summaries.
    pub store: Arc<SummaryStore<Batch>>,
    /// The counters as of the last call.
    pub stats: PrecomputeStats,
}

impl Precompute {
    /// One worker for every series; `sds` describes `dag`.
    pub fn new(
        dag: PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
    ) -> Result<Self, PrecomputeError> {
        Self::with_shards(&dag, sds, options, 1)
    }

    /// `shards` workers, each owning the series [`shard_of`] assigns it.
    pub fn with_shards(
        dag: &PhysicalASAPDAG,
        sds: &Sds,
        options: PrecomputeOptions,
        shards: usize,
    ) -> Result<Self, PrecomputeError> {
        let shared = Arc::new(Shared::new(Ingestion::new(dag, sds, options)?, shards));
        Ok(Self {
            store: shared.store.clone(),
            stats: PrecomputeStats::default(),
            shared,
        })
    }

    /// See [`Shared::progress`].
    pub fn progress(&self) -> crate::query::IngestionProgress {
        self.shared.progress()
    }

    /// The global watermark; see [`Shared::watermark_ms`].
    pub fn watermark_ms(&self) -> Option<i64> {
        self.shared.watermark_ms()
    }

    /// [`Self::advance_watermark_at`] the current wall-clock time.
    pub fn advance_watermark(&mut self, barrier_ms: i64) -> i64 {
        self.advance_watermark_at(barrier_ms, wall_clock_ms())
    }

    /// See [`Shared::advance_watermark_at`].
    pub fn advance_watermark_at(&mut self, barrier_ms: i64, now_ms: i64) -> i64 {
        let barrier_ms = self.shared.advance_watermark_at(barrier_ms, now_ms);
        self.stats = self.shared.stats();
        barrier_ms
    }

    /// [`Self::ingest_at`] the current wall-clock time.
    pub fn ingest(&mut self, samples: &[RawSample]) {
        self.ingest_at(samples, wall_clock_ms());
    }

    /// Routes `samples` to their workers, which take them into their panes
    /// and close every pane the global watermark has passed. `now_ms` is
    /// the wall-clock time that bounds sample times from above.
    pub fn ingest_at(&mut self, samples: &[RawSample], now_ms: i64) {
        let workers = self.shared.workers();
        let mut shards = vec![Vec::new(); workers];
        for sample in samples {
            shards[shard_of(&sample.labels, workers)].push(sample.clone());
        }
        // Every worker takes its shard before any closes a pane, as if the
        // request reached them all at once.
        for (worker, samples) in shards.iter().enumerate() {
            self.shared.take_at(worker, samples, now_ms);
        }
        self.shared.close_panes();
        self.stats = self.shared.stats();
    }
}

/// Milliseconds since the Unix epoch.
pub fn wall_clock_ms() -> i64 {
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

/// `samples` as the executor's [`Sample`]s. The selector takes owned label
/// maps, so each sample's labels are copied.
pub fn executor_samples(samples: &[RawSample]) -> Vec<Sample> {
    samples
        .iter()
        .map(|s| Sample {
            labels: (*s.labels).clone(),
            timestamp_ms: s.timestamp_ms,
            value: s.value,
        })
        .collect()
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
