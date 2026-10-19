//! Precompute engine: builds each stored summary pane once, when the
//! watermark passes its end.
//!
//! The plan's ingestion side is compiled once ([`Ingestion`]) and shared by
//! the ingestion workers, each of which owns a shard of the series. A pane
//! `(end - pane, end]` closes when the global watermark (the minimum over
//! the workers of their newest sample, minus the allowed lateness) reaches
//! `end`. A definition is built per sample where its shape allows
//! ([`crate::per_sample`]): each sample updates its group's open state in
//! place, and each worker's part of the pane is built from columns at
//! close. Otherwise each worker keeps the pane's samples, and the worker
//! that completes the pane runs the plan's build once over all of them
//! ([`PaneBuild`]). The pane is stored and never changes; samples
//! older than a closed pane are dropped. The first pane may have started
//! before the data plane did, so it is never built: queries that need it
//! are not answered from stored panes.

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::plan::PhysicalDAG;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef};
use asap_planner::{InstallablePlan, PhysicalASAPDAG};
use futures::executor::block_on;
use futures::StreamExt;

use crate::per_sample::{OpenPanes, PerSampleSpec};
use crate::raw_buffer::RawBuffer;
use crate::sds::{DefinitionId, Sds, SummaryDefinition, SummaryStore};
use crate::series::SeriesWindow;

/// One raw sample: its series' full label set (including `__name__`), time
/// and value. Samples of one series share their labels.
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

/// A pane: its batches, one row per group across them. Built per sample at
/// ingestion time, the batches are the workers' parts, whose groups (their
/// series) are disjoint.
pub type Pane = Vec<Batch>;

/// How the panes of one definition are built from raw samples with the
/// plan's own build. Compiled once and shared by every thread: the
/// precompute engine builds stored panes with it (where it does not build
/// them per sample), and the query engine kept ones.
pub(crate) struct PaneBuild {
    pub(crate) definition: SummaryDefinition,
    /// The pane's raw-sample window: its input batch from raw samples.
    pub(crate) window: SeriesWindow,
    build: CompiledPhysicalDAG,
}

impl PaneBuild {
    /// Compiles `definition`'s build from its `TimeRange` up to its builder.
    pub(crate) fn new(
        dag: &PhysicalASAPDAG,
        definition: &SummaryDefinition,
    ) -> Result<Self, PrecomputeError> {
        let window = SeriesWindow::new(dag, &definition.window)
            .map_err(|e| PrecomputeError::Definition(definition.id.clone(), e.to_string()))?;
        let build = compile(
            dag,
            BTreeMap::from([(
                definition.raw_input(),
                InputContract::bounded(window.schema.clone()),
            )]),
            &[definition.builder],
        )?;
        Ok(Self {
            definition: definition.clone(),
            window,
            build,
        })
    }

    /// The pane ending at `end_ms`, built from `samples` under `scope`: the
    /// samples of the pane the plan's `Scan` selects, then the build.
    pub(crate) fn run(
        &self,
        samples: &[RawSample],
        end_ms: i64,
        scope: Scope,
    ) -> Result<Pane, PrecomputeError> {
        self.run_batch(self.window.batch(end_ms, samples)?, scope)
    }

    /// The build over `input`, the pane's selected input batch, under `scope`.
    pub(crate) fn run_batch(&self, input: Batch, scope: Scope) -> Result<Pane, PrecomputeError> {
        let source: Source<'_> = Box::new(Operator::source(input.schema().clone(), vec![input])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.definition.raw_input(), source)]))?;
        Ok(run(&build, self.build.roots(), scope)?)
    }
}

/// One definition built at ingestion time.
pub(crate) struct IngestionBuild {
    pane: PaneBuild,
    /// The build run per sample, unless its shape keeps the batch build.
    per_sample: Option<PerSampleSpec>,
}

/// The plan's ingestion side, compiled once and shared by every ingestion
/// worker: one build per definition built at ingestion time.
pub struct Ingestion {
    builds: Vec<IngestionBuild>,
}

impl Ingestion {
    /// The builds of `sds`'s ingestion-time definitions, the stored
    /// summaries of `plan`: per sample where its shape allows
    /// ([`crate::per_sample`]) and `per_sample` is set, else over each
    /// pane's buffered samples.
    pub fn new(
        plan: &InstallablePlan,
        sds: &Sds,
        per_sample: bool,
    ) -> Result<Self, PrecomputeError> {
        let builds = sds
            .definitions
            .values()
            .map(|definition| {
                let pane = PaneBuild::new(&plan.dag.dag, definition)?;
                let per_sample = per_sample
                    .then(|| PerSampleSpec::new(&plan.dag.dag, &pane))
                    .flatten();
                tracing::info!(definition = ?definition.id, per_sample = per_sample.is_some(), "pane build");
                Ok(IngestionBuild { pane, per_sample })
            })
            .collect::<Result<_, PrecomputeError>>()?;
        Ok(Self { builds })
    }

    /// How many definitions are built per sample.
    pub fn per_sample_definitions(&self) -> usize {
        self.builds
            .iter()
            .filter(|b| b.per_sample.is_some())
            .count()
    }
}

/// One worker's part of a pane.
enum PanePart {
    /// Built per sample: the rows of the worker's groups.
    Batch(Batch),
    /// For the batch build: the worker's samples of the pane, built with
    /// the other workers' at once, since a group may span workers.
    Samples(Vec<RawSample>),
}

/// One worker's part of a pane, or why it failed.
type PartOrError = Result<PanePart, String>;

/// No value yet in an `AtomicI64` slot.
const UNSET: i64 = i64::MIN;

/// Precompute state every ingestion worker and query thread shares: the
/// stored panes, each worker's watermark, and the per-worker partial panes
/// not yet complete.
pub struct Shared {
    /// Stored panes, by definition and pane end.
    pub store: SummaryStore<Pane>,
    allowed_lateness_ms: i64,
    /// Each worker's newest sample time, `UNSET` before its first.
    newest_sample_ms: Vec<AtomicI64>,
    /// The first sample's time: every worker skips the pane it falls in, so
    /// all workers build the same panes.
    start_ms: AtomicI64,
    /// The newest global watermark any worker has announced to the others.
    announced_ms: AtomicI64,
    /// Partial panes by definition and end, until every worker has built
    /// its part.
    pending: Mutex<HashMap<(DefinitionId, i64), Vec<PartOrError>>>,
    accepted: AtomicU64,
    dropped: AtomicU64,
    panes_built: AtomicU64,
    panes_failed: AtomicU64,
}

impl Shared {
    pub fn new(workers: usize, allowed_lateness_ms: i64) -> Self {
        Self {
            store: SummaryStore::new(),
            allowed_lateness_ms,
            newest_sample_ms: (0..workers.max(1)).map(|_| AtomicI64::new(UNSET)).collect(),
            start_ms: AtomicI64::new(UNSET),
            announced_ms: AtomicI64::new(UNSET),
            pending: Mutex::new(HashMap::new()),
            accepted: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            panes_built: AtomicU64::new(0),
            panes_failed: AtomicU64::new(0),
        }
    }

    pub fn workers(&self) -> usize {
        self.newest_sample_ms.len()
    }

    /// The global watermark: no sample older than this is expected any
    /// more. It is the minimum over the workers that have seen a sample of
    /// their newest sample time, minus the allowed lateness; `None` before
    /// any sample.
    pub fn watermark_ms(&self) -> Option<i64> {
        self.newest_sample_ms
            .iter()
            .map(|newest| newest.load(Ordering::Acquire))
            .filter(|&newest| newest != UNSET)
            .min()
            .map(|newest| newest - self.allowed_lateness_ms)
    }

    pub fn stats(&self) -> PrecomputeStats {
        PrecomputeStats {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            panes_built: self.panes_built.load(Ordering::Relaxed),
            panes_failed: self.panes_failed.load(Ordering::Relaxed),
        }
    }

    /// Adds one worker's part of the pane of `build` ending at `end_ms`.
    /// The worker that adds the last part completes and stores the pane.
    fn add_part(
        &self,
        build: &IngestionBuild,
        end_ms: i64,
        scope: &Scope,
        part: Result<PanePart, String>,
    ) {
        let definition = &build.pane.definition;
        let parts = {
            let mut pending = self.pending.lock().expect("pending panes lock");
            let key = (definition.id.clone(), end_ms);
            let parts = pending.entry(key.clone()).or_default();
            parts.push(part);
            if parts.len() < self.workers() {
                return;
            }
            pending.remove(&key).expect("present")
        };
        let complete = || -> Result<Pane, String> {
            let mut pane = Pane::new();
            let mut samples = Vec::new();
            for part in parts {
                match part? {
                    PanePart::Batch(batch) => pane.push(batch),
                    PanePart::Samples(part) => samples.extend(part),
                }
            }
            if build.per_sample.is_none() {
                pane = build
                    .pane
                    .run(&samples, end_ms, scope.clone())
                    .map_err(|e| e.to_string())?;
            }
            Ok(pane)
        };
        match complete() {
            Ok(pane) => {
                self.store.insert(&definition.id, end_ms, pane);
                self.panes_built.fetch_add(1, Ordering::Relaxed);
                // The newest evaluation that reads stored panes is about the
                // newest stored pane's end, but may lag it: an evaluation at
                // T runs once samples up to just after T have arrived. One
                // pane width of margin keeps the panes it reads.
                let oldest = definition.oldest_kept_end_ms(end_ms) - definition.pane_ms;
                self.store.evict_before(&definition.id, oldest);
            }
            Err(error) => {
                tracing::error!(definition = ?definition.id, end_ms, %error, "pane build failed");
                self.panes_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// The ingestion worker of `workers` that owns the series with `labels`.
pub fn shard_of(labels: &BTreeMap<String, String>, workers: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    labels.hash(&mut hasher);
    (hasher.finish() % workers.max(1) as u64) as usize
}

/// One worker's open panes of one definition.
#[derive(Default)]
struct OpenBuild {
    /// For the batch build: this worker's samples of open panes, by end.
    samples: BTreeMap<i64, Vec<RawSample>>,
    /// For the per-sample build: the open panes' states.
    states: OpenPanes,
    /// End of the newest pane this worker built its part of, or skipped;
    /// samples in it or before it are dropped. `None` before the first
    /// sample anywhere.
    built_through: Option<i64>,
}

/// One ingestion worker: takes the samples of its shard of the series into
/// its open panes and builds its part of each pane once the global
/// watermark passes it. The compiled builds are shared; the open panes are
/// the worker's own.
pub struct ShardWorker {
    index: usize,
    ingestion: Arc<Ingestion>,
    open: Vec<OpenBuild>,
    shared: Arc<Shared>,
    /// The global watermark this worker has built its parts through.
    closed_through_ms: Option<i64>,
    /// The local raw buffer, which keeps this worker's recent samples for
    /// query-time raw windows; `None` when the plan reads none.
    raw_buffer: Option<Arc<RawBuffer>>,
}

impl ShardWorker {
    /// Worker `index` of `shared`'s workers, running `ingestion`'s builds.
    pub fn new(index: usize, ingestion: Arc<Ingestion>, shared: Arc<Shared>) -> Self {
        let open = ingestion
            .builds
            .iter()
            .map(|_| OpenBuild::default())
            .collect();
        Self {
            index,
            ingestion,
            open,
            shared,
            closed_through_ms: None,
            raw_buffer: None,
        }
    }

    /// Also keeps this worker's samples in `raw_buffer`, its shard
    /// [`Self::new`]'s `index`.
    pub fn with_raw_buffer(mut self, raw_buffer: Option<Arc<RawBuffer>>) -> Self {
        self.raw_buffer = raw_buffer;
        self
    }

    /// Takes `samples`, all of this worker's shard, into their open panes,
    /// then builds this worker's part of every pane the global watermark
    /// has passed. Returns whether this moved the global watermark past
    /// what was announced, so the caller tells the other workers to
    /// [`Self::advance`].
    pub fn ingest(&mut self, samples: &[RawSample]) -> bool {
        let Some(first) = samples.iter().map(|s| s.timestamp_ms).min() else {
            return false;
        };
        if let Some(raw_buffer) = &self.raw_buffer {
            raw_buffer.append(self.index, samples);
        }
        let shared = self.shared.clone();
        let _ = shared
            .start_ms
            .compare_exchange(UNSET, first, Ordering::AcqRel, Ordering::Acquire);
        let start_ms = shared.start_ms.load(Ordering::Acquire);
        let mut buffered = vec![false; samples.len()];
        let mut dropped = 0;
        let ingestion = self.ingestion.clone();
        // Samples of one series arrive together: each build looks the
        // series up once per run.
        let mut offset = 0;
        for run in samples.chunk_by(|a, b| Arc::ptr_eq(&a.labels, &b.labels)) {
            let metric = run[0].labels.get("__name__").map(String::as_str);
            for (build, open) in ingestion.builds.iter().zip(&mut self.open) {
                if metric != Some(build.pane.window.metric()) {
                    continue;
                }
                let pane_ms = build.pane.definition.pane_ms;
                // The pane of the first sample may be partial: skip it.
                let built = *open
                    .built_through
                    .get_or_insert(pane_end(start_ms, pane_ms));
                let cells = match &build.per_sample {
                    Some(_) => match build.pane.window.cells(&run[0]) {
                        Some(cells) => Some(cells),
                        // Not selected: counted as taken, as the batch
                        // build takes and then filters it.
                        None => {
                            for (i, sample) in run.iter().enumerate() {
                                if pane_end(sample.timestamp_ms, pane_ms) <= built {
                                    dropped += 1;
                                } else {
                                    buffered[offset + i] = true;
                                }
                            }
                            continue;
                        }
                    },
                    None => None,
                };
                for (i, sample) in run.iter().enumerate() {
                    let end = pane_end(sample.timestamp_ms, pane_ms);
                    if end <= built {
                        dropped += 1;
                        continue;
                    }
                    match (&build.per_sample, &cells) {
                        (Some(spec), Some(cells)) => {
                            spec.update(&mut open.states, cells, sample, end)
                        }
                        _ => open.samples.entry(end).or_default().push(sample.clone()),
                    }
                    buffered[offset + i] = true;
                }
            }
            offset += run.len();
        }
        let accepted = buffered.iter().filter(|&&b| b).count() as u64;
        shared.accepted.fetch_add(accepted, Ordering::Relaxed);
        shared.dropped.fetch_add(dropped, Ordering::Relaxed);
        let newest = samples
            .iter()
            .map(|s| s.timestamp_ms)
            .max()
            .expect("nonempty");
        shared.newest_sample_ms[self.index].fetch_max(newest, Ordering::AcqRel);
        // Close and announce the same watermark: announced, it is closed
        // here, and the other workers close it on `advance`.
        let Some(watermark) = shared.watermark_ms() else {
            return false;
        };
        self.advance_to(watermark);
        shared.announced_ms.fetch_max(watermark, Ordering::AcqRel) < watermark
    }

    /// Builds this worker's part of every pane the global watermark has
    /// passed: from its open states or samples, or empty. Every worker
    /// builds every pane after the first, so a pane without samples is
    /// stored as empty, not missing.
    pub fn advance(&mut self) {
        if let Some(watermark) = self.shared.watermark_ms() {
            self.advance_to(watermark);
        }
    }

    fn advance_to(&mut self, watermark: i64) {
        if self
            .closed_through_ms
            .is_some_and(|closed| watermark <= closed)
        {
            return;
        }
        self.closed_through_ms = Some(watermark);
        let start_ms = self.shared.start_ms.load(Ordering::Acquire);
        let ingestion = self.ingestion.clone();
        for (build, open) in ingestion.builds.iter().zip(&mut self.open) {
            let pane_ms = build.pane.definition.pane_ms;
            let built = *open
                .built_through
                .get_or_insert(pane_end(start_ms, pane_ms));
            let mut end = built + pane_ms;
            while end <= watermark {
                let scope = Scope::Ingestion {
                    window_start_ms: end - pane_ms,
                    window_end_ms: end,
                    revision: 1,
                };
                let part = match &build.per_sample {
                    Some(spec) => spec
                        .close(&build.pane, &mut open.states, end, &scope)
                        .map(PanePart::Batch),
                    None => Ok(PanePart::Samples(
                        open.samples.remove(&end).unwrap_or_default(),
                    )),
                };
                self.shared.add_part(build, end, &scope, part);
                open.built_through = Some(end);
                end += pane_ms;
            }
        }
    }
}

/// Every ingestion worker of a plan on the caller's thread, each fed its
/// shard of the series: the threaded engine's precompute without threads.
pub struct Precompute {
    shared: Arc<Shared>,
    ingestion: Arc<Ingestion>,
    workers: Vec<ShardWorker>,
}

impl Precompute {
    /// One worker for every series.
    pub fn new(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
    ) -> Result<Self, PrecomputeError> {
        Self::with_shards(plan, sds, allowed_lateness_ms, 1)
    }

    /// `shards` workers, each owning the series [`shard_of`] assigns it.
    pub fn with_shards(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
        shards: usize,
    ) -> Result<Self, PrecomputeError> {
        let ingestion = Arc::new(Ingestion::new(plan, sds, true)?);
        Ok(Self::workers(ingestion, allowed_lateness_ms, shards))
    }

    /// One worker that builds every pane over its buffered samples with
    /// the plan's own build: the reference per-sample builds equal.
    pub fn batch(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
    ) -> Result<Self, PrecomputeError> {
        let ingestion = Arc::new(Ingestion::new(plan, sds, false)?);
        Ok(Self::workers(ingestion, allowed_lateness_ms, 1))
    }

    fn workers(ingestion: Arc<Ingestion>, allowed_lateness_ms: i64, shards: usize) -> Self {
        let shared = Arc::new(Shared::new(shards, allowed_lateness_ms));
        let workers = (0..shared.workers())
            .map(|index| ShardWorker::new(index, ingestion.clone(), shared.clone()))
            .collect();
        Self {
            shared,
            ingestion,
            workers,
        }
    }

    /// How many definitions are built per sample.
    pub fn per_sample_definitions(&self) -> usize {
        self.ingestion.per_sample_definitions()
    }

    /// Routes `samples` to their workers, then lets every worker build the
    /// panes the global watermark has passed.
    pub fn ingest(&mut self, samples: &[RawSample]) {
        let mut shards = vec![Vec::new(); self.workers.len()];
        for sample in samples {
            shards[shard_of(&sample.labels, self.workers.len())].push(sample.clone());
        }
        for (worker, samples) in self.workers.iter_mut().zip(&shards) {
            worker.ingest(samples);
        }
        for worker in &mut self.workers {
            worker.advance();
        }
    }

    pub fn watermark_ms(&self) -> Option<i64> {
        self.shared.watermark_ms()
    }

    pub fn store(&self) -> &SummaryStore<Pane> {
        &self.shared.store
    }

    pub fn stats(&self) -> PrecomputeStats {
        self.shared.stats()
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

/// Executes `dag`'s single root to completion and returns its batches.
pub fn run(
    dag: &PhysicalDAG<'_, Batch, SchemaRef>,
    roots: &[u64],
    scope: Scope,
) -> Result<Vec<Batch>, asap_executor::Error> {
    block_on(async {
        let context = RunContext::new(scope, limits())?;
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
