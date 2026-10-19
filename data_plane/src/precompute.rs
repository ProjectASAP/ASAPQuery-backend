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

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use asap_executor::operators::Operator;
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG, InputContract, Source};
use asap_executor::plan::PhysicalDAG;
use asap_executor::planner::ir::schema::FieldDataType;
use asap_executor::runtime::{Limits, RunContext, Scope};
use asap_executor::values::{Batch, SchemaRef, Value};
use asap_executor::{RawSampleSelector, Sample};
use asap_planner::{InstallablePlan, PhysicalASAPDAG};
use futures::executor::block_on;
use futures::StreamExt;

use crate::per_sample::PerSampleBuild;
use crate::raw_buffer::RawBuffer;
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
        self.run_rows(self.selector.rows(end_ms, samples)?, scope)
    }

    /// The build over `rows`, input rows already selected, under `scope`.
    pub(crate) fn run_rows(&self, rows: Batch, scope: Scope) -> Result<Batch, PrecomputeError> {
        let input: Source<'_> = Box::new(Operator::source(rows.schema().clone(), vec![rows])?);
        let build = self
            .build
            .instantiate(BTreeMap::from([(self.definition.raw_input(), input)]))?;
        let rows = run(&build, self.build.roots(), scope)?;
        let schema = Arc::new(self.definition.output_schema.clone());
        Ok(Batch::try_new(schema, rows)?)
    }
}

/// One worker's part of a pane, or why its build failed.
type PanePart = Result<Batch, String>;

/// No value yet in an `AtomicI64` slot.
const UNSET: i64 = i64::MIN;

/// Precompute state every ingestion worker and query thread shares: the
/// stored panes, each worker's watermark, and the per-worker partial panes
/// not yet complete.
pub struct Shared {
    /// Stored panes, by definition and pane end.
    pub store: SummaryStore<Batch>,
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
    pending: Mutex<HashMap<(DefinitionId, i64), Vec<PanePart>>>,
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

    /// Adds one worker's part of the pane of `definition` ending at `end_ms`.
    /// The worker that adds the last part merges them and stores the pane.
    fn add_part(&self, definition: &SummaryDefinition, end_ms: i64, part: Result<Batch, String>) {
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
        match parts
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .and_then(|parts| merge_parts(definition, parts))
        {
            Ok(pane) => {
                self.store.insert(&definition.id, end_ms, pane);
                self.panes_built.fetch_add(1, Ordering::Relaxed);
                // The newest evaluation that reads stored panes is at most
                // the newest stored pane's end, so retention is measured
                // from it, not from the watermark, which may be past it.
                self.store
                    .evict_before(&definition.id, definition.oldest_kept_end_ms(end_ms));
            }
            Err(error) => {
                tracing::error!(definition = ?definition.id, end_ms, %error, "pane build failed");
                self.panes_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// One pane from the parts the workers built over their own series: their
/// rows, with the states of a group several workers saw merged by the
/// family's merge. Each worker's part has one row per group, so a group
/// present in one part only is kept as built.
fn merge_parts(definition: &SummaryDefinition, parts: Vec<Batch>) -> Result<Batch, String> {
    let schema = Arc::new(definition.output_schema.clone());
    let state = definition
        .output_schema
        .fields
        .iter()
        .position(|f| !matches!(f.dtype, FieldDataType::Plain(_)));
    let mut rows: BTreeMap<Vec<Vec<u8>>, Vec<Value>> = BTreeMap::new();
    for part in &parts {
        for row in part.rows() {
            let key = row
                .iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != state)
                .map(|(_, value)| value.key())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            match (rows.get_mut(&key), state) {
                (Some(existing), Some(state)) => {
                    let (
                        Value::Summary { state: left, .. },
                        Value::Summary {
                            family,
                            state: right,
                        },
                    ) = (&existing[state], &row[state])
                    else {
                        return Err("summary state column without a state".into());
                    };
                    let merged = left.merge_with(right.as_ref()).map_err(|e| e.to_string())?;
                    existing[state] = Value::Summary {
                        family: family.clone(),
                        state: Arc::from(merged),
                    };
                }
                (Some(_), None) => return Err("duplicate group without a state".into()),
                (None, _) => {
                    rows.insert(key, row.clone());
                }
            }
        }
    }
    Batch::try_new(schema, rows.into_values().collect()).map_err(|e| e.to_string())
}

/// The ingestion worker of `workers` that owns the series with `labels`.
pub fn shard_of(labels: &BTreeMap<String, String>, workers: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    labels.hash(&mut hasher);
    (hasher.finish() % workers.max(1) as u64) as usize
}

struct Builder {
    pane: PaneBuild,
    /// The build run per sample, unless its shape keeps the batch build.
    per_sample: Option<PerSampleBuild>,
    /// For the batch build, this worker's samples of open panes, by pane end.
    open: BTreeMap<i64, Vec<Sample>>,
    /// End of the newest pane this worker built its part of, or skipped;
    /// samples in it or before it are dropped. `None` before the first
    /// sample anywhere.
    built_through: Option<i64>,
}

/// One ingestion worker: buffers the samples of its shard of the series and
/// builds its part of each pane once the global watermark passes it. It
/// holds the plan's compiled builds, which are not `Send`, so each worker
/// thread compiles its own.
pub struct ShardWorker {
    index: usize,
    builders: Vec<Builder>,
    shared: Arc<Shared>,
    /// The global watermark this worker has built its parts through.
    closed_through_ms: Option<i64>,
    /// The local raw buffer, which keeps this worker's recent samples for
    /// query-time raw windows; `None` when the plan reads none.
    raw_buffer: Option<Arc<RawBuffer>>,
}

impl ShardWorker {
    /// Worker `index` of `shared`'s workers, with one build per
    /// ingestion-time definition of `sds`, the stored summaries of `plan`:
    /// per sample where its shape allows ([`crate::per_sample`]) and
    /// `per_sample` is set, else over each pane's buffered samples.
    pub fn new(
        plan: &InstallablePlan,
        sds: &Sds,
        index: usize,
        shared: Arc<Shared>,
        per_sample: bool,
    ) -> Result<Self, PrecomputeError> {
        let builders = sds
            .definitions
            .values()
            .map(|definition| {
                let pane = PaneBuild::new(&plan.dag.dag, definition)?;
                let per_sample = per_sample
                    .then(|| PerSampleBuild::new(&plan.dag.dag, &pane))
                    .flatten();
                if index == 0 {
                    tracing::info!(definition = ?definition.id, per_sample = per_sample.is_some(), "pane build");
                }
                Ok(Builder {
                    pane,
                    per_sample,
                    open: BTreeMap::new(),
                    built_through: None,
                })
            })
            .collect::<Result<_, PrecomputeError>>()?;
        Ok(Self {
            index,
            builders,
            shared,
            closed_through_ms: None,
            raw_buffer: None,
        })
    }

    /// Also keeps this worker's samples in `raw_buffer`, its shard
    /// [`Self::new`]'s `index`.
    pub fn with_raw_buffer(mut self, raw_buffer: Option<Arc<RawBuffer>>) -> Self {
        self.raw_buffer = raw_buffer;
        self
    }

    /// Buffers `samples`, all of this worker's shard, into their panes, then
    /// builds this worker's part of every pane the global watermark has
    /// passed. Returns whether this moved the global watermark past what
    /// was announced, so the caller tells the other workers to
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
        let (mut accepted, mut dropped) = (0, 0);
        for sample in samples {
            let metric = sample.labels.get("__name__").map(String::as_str);
            let mut buffered = false;
            for builder in &mut self.builders {
                if metric != Some(builder.pane.selector.metric()) {
                    continue;
                }
                let pane_ms = builder.pane.definition.pane_ms;
                let end = pane_end(sample.timestamp_ms, pane_ms);
                // The pane of the first sample may be partial: skip it.
                let built = *builder
                    .built_through
                    .get_or_insert(pane_end(start_ms, pane_ms));
                if end <= built {
                    dropped += 1;
                    continue;
                }
                match &mut builder.per_sample {
                    Some(per_sample) => per_sample.update(&builder.pane, sample, end),
                    None => builder
                        .open
                        .entry(end)
                        .or_default()
                        .push(sample.to_sample()),
                }
                buffered = true;
            }
            accepted += u64::from(buffered);
        }
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
    /// passed: from its buffered samples, or empty. Every worker builds every
    /// pane after the first, so a pane without samples is stored as empty,
    /// not missing.
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
        for builder in &mut self.builders {
            let pane_ms = builder.pane.definition.pane_ms;
            let built = *builder
                .built_through
                .get_or_insert(pane_end(start_ms, pane_ms));
            let mut end = built + pane_ms;
            while end <= watermark {
                let scope = Scope::Ingestion {
                    window_start_ms: end - pane_ms,
                    window_end_ms: end,
                    revision: 1,
                };
                let part = match &mut builder.per_sample {
                    Some(per_sample) => per_sample.close(&builder.pane, end, scope),
                    None => {
                        let samples = builder.open.remove(&end).unwrap_or_default();
                        builder
                            .pane
                            .run(&samples, end, scope)
                            .map_err(|e| e.to_string())
                    }
                };
                self.shared.add_part(&builder.pane.definition, end, part);
                builder.built_through = Some(end);
                end += pane_ms;
            }
        }
    }
}

/// Every ingestion worker of a plan on the caller's thread, each fed its
/// shard of the series: the threaded engine's precompute without threads.
pub struct Precompute {
    shared: Arc<Shared>,
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
        Self::workers(plan, sds, allowed_lateness_ms, shards, true)
    }

    /// One worker that builds every pane over its buffered samples with
    /// the executor's batch build: the reference per-sample builds equal.
    pub fn batch(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
    ) -> Result<Self, PrecomputeError> {
        Self::workers(plan, sds, allowed_lateness_ms, 1, false)
    }

    fn workers(
        plan: &InstallablePlan,
        sds: &Sds,
        allowed_lateness_ms: i64,
        shards: usize,
        per_sample: bool,
    ) -> Result<Self, PrecomputeError> {
        let shared = Arc::new(Shared::new(shards, allowed_lateness_ms));
        let workers = (0..shared.workers())
            .map(|index| ShardWorker::new(plan, sds, index, shared.clone(), per_sample))
            .collect::<Result<_, _>>()?;
        Ok(Self { shared, workers })
    }

    /// How many definitions are built per sample.
    pub fn per_sample_definitions(&self) -> usize {
        self.workers[0]
            .builders
            .iter()
            .filter(|b| b.per_sample.is_some())
            .count()
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

    pub fn store(&self) -> &SummaryStore<Batch> {
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
