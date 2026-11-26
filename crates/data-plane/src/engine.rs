//! The engine: the installed plan, compiled once when it is installed,
//! and the threads that execute it:
//!
//! - `ingest-N` shards each route the samples of the series hashed to them
//!   (Remote Write is decoded on the HTTP side) and append them to their
//!   own copy of the raw data, and seal and evict it when told;
//! - the `ingest` coordinator keeps the watermark over the shards, closes
//!   the windows it passes, publishes the raw data and hands each closed
//!   refresh window, with the raw data as it was when the window closed,
//!   to the precompute pool;
//! - the precompute pool: `precompute-N` workers compute closed windows in
//!   parallel (a group starts a window only after the windows of the
//!   groups it reads are committed, [`RefreshTaskGraph`]), and the
//!   `precompute` committer appends each group's windows' rows in window
//!   order to its copy of the views and publishes them;
//! - `persist-N` threads write each refreshed window to the view files,
//!   after it is published;
//! - `query-N` workers each run one query to completion over the newest
//!   published snapshot, so a query never waits for ingestion. A run's
//!   output stream is not `Send`, so a run never moves between threads.
//!
//! Windows closed and not yet refreshed (the precompute backlog) are
//! bounded by `max_precompute_backlog`: when it is full, the ingest
//! coordinator waits, the shards' queues fill and Remote Write gets 503
//! (`max_queued_samples`).
//!
//! Each thread records the thread CPU and wall time of its phases in
//! [`Metrics`], exported on `/metrics`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::Instant;

use asap_executor::runtime::Limits;
use asap_planner::InstallablePlan;
use internal_metrics::{Metrics, PushMetrics, Timer};
use storage::persist::{plan_hash, ViewFiles, ViewRead};
use storage::store::SeriesPartition;
use storage::store::{
    serve_view_cache, PublishedStore, QueryTimeResults, RawData, RawSample, RawShards, RunStore,
    ViewStore, ViewTier, WindowSlot,
};
use storage::window_cache::WindowCache;
use tokio::sync::oneshot;

use crate::ingestion::{
    ClosedWindow, Computed, Ingestion, IngestionOptions, IngestionStats, Precompute,
    RefreshTaskGraph, Refreshed, Routing,
};
use crate::metrics_endpoint::Gauges;
use crate::push::{Partial, PushGroup, PushShard};
use crate::query::{QueryEngine, Snapshot, Unanswered};
use crate::result_cache::{Answer, ResultCache};
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::values::Batch;

/// How the views of a closed refresh window are built
/// (docs/design/parallel-precompute.md). The `data_plane` binary defaults
/// to `Push`, the faster mode for a running server. The library default
/// stays `Window`: it is the reference path every push test compares
/// against, so tests and other callers get it unless they ask for push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum PrecomputeMode {
    /// One ingestion run per closed window, over the raw samples the
    /// shards buffered for it (`--precompute-mode=window`).
    #[default]
    Window,
    /// Each ingest shard folds its samples into its own instance of each
    /// window's pipelines as they arrive; at close the shards' partials are
    /// combined and committed. Groups that cannot be pushed keep the
    /// window path.
    Push,
}

/// How the engine ingests samples and runs queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineOptions {
    /// Lateness, future bound and first-window checks of ingestion.
    pub ingestion: IngestionOptions,
    /// Samples queued for the ingest thread, not yet ingested, beyond which
    /// Remote Write is refused with 503 so senders retry.
    pub max_queued_samples: u64,
    /// Refresh windows closed and not yet refreshed beyond which the
    /// ingest thread waits for the precompute thread.
    pub max_precompute_backlog: usize,
    /// Ingest shard threads: each routes and appends the samples of the
    /// series hashed to it, into its own raw data.
    pub ingest_threads: usize,
    /// Threads computing closed refresh windows at once. With 1 the
    /// committer computes each window itself, one at a time, in the order
    /// they closed.
    pub precompute_threads: usize,
    /// For tests: refresh windows whose computation waits this long first,
    /// as a slow run would.
    #[doc(hidden)]
    pub slow_refreshes: Vec<SlowRefresh>,
    /// For tests: fail a window whose run reads views of another group
    /// not yet committed through its end, or whose inputs change while it
    /// runs.
    #[doc(hidden)]
    pub check_task_inputs: bool,
    /// For tests: how long every ingest shard but the first waits before
    /// running each batch, so the first runs ahead of the others.
    #[doc(hidden)]
    pub shard_delay_ms: u64,
    /// Persist threads, each writing the view files of every
    /// `persist_threads`-th refresh group.
    pub persist_threads: usize,
    /// Bytes of refreshed windows queued for the persist threads beyond
    /// which further windows are counted (`asap_view_persist_over_bound_total`);
    /// the precompute thread never waits for the disk.
    pub max_persist_backlog_bytes: u64,
    /// For tests: how long a persist thread waits before writing each
    /// window, as a slow disk would.
    #[doc(hidden)]
    pub persist_delay_ms: u64,
    /// Every answer must be computed from the plan: a plan with a query the
    /// query engine cannot run is refused at install, and the data plane
    /// never forwards a query to Prometheus (`--no-forward`).
    pub no_forward: bool,
    /// The most bytes one ingestion or query run may hold
    /// (`asap_executor::runtime::Limits::max_bytes`).
    pub run_memory_limit_bytes: usize,
    /// Query worker threads: the most queries run at once. Each run may
    /// hold `run_memory_limit_bytes`, so queries may hold up to this many
    /// times that.
    pub query_threads: usize,
    /// Compute each query-time `OneEvaluation` node several queries read
    /// once per evaluation time, as the plan says; `false` computes it in
    /// every query's run (for comparison).
    pub share_one_evaluation_results: bool,
    /// How long a query-time result is kept after its evaluation time,
    /// counted back from the newest evaluation time computed.
    pub query_time_results_retention_ms: u64,
    /// The most bytes of query-time results kept; the oldest are dropped
    /// first.
    pub query_time_results_max_bytes: usize,
    /// Keep each planned query's final answer at each evaluation time
    /// ([`crate::result_cache`]), so a range request recomputes only the
    /// steps not answered before.
    pub result_cache: bool,
    /// The most bytes of answers the result cache keeps; the least
    /// recently used are dropped first.
    pub result_cache_max_bytes: usize,
    /// Where the materialized views are persisted and loaded from at
    /// startup ([`storage::persist`]); `None`: kept in memory only.
    pub view_dir: Option<std::path::PathBuf>,
    /// How often the view files are synced to disk: a refreshed window is
    /// durable within this long. 0 syncs after every window.
    pub view_sync_interval_ms: u64,
    /// Bytes of decoded view windows the window cache may hold
    /// (docs/design/tiered-view-storage.md); the other windows are read
    /// from the view files. `None`: derived from the cgroup's
    /// `memory.max`, or no budget without a limit.
    pub view_cache_max_bytes: Option<u64>,
    /// A cached window not read for this long leaves the cache. `None`:
    /// [`DEFAULT_VIEW_CACHE_IDLE_MS`]; `Some(0)`: never (every window stays
    /// decoded, as before tiering).
    pub view_cache_idle_ms: Option<u64>,
    /// Each refresh group's view file rolls to a new segment every this
    /// many milliseconds of windows.
    pub view_segment_ms: i64,
    /// How cold windows are read: mapped sealed segments, or `pread`.
    pub view_read: ViewRead,
    /// For tests: each cold view window's decode waits this long first.
    #[doc(hidden)]
    pub cold_decode_delay_ms: u64,
    /// For tests: these views' windows are never written to the view files
    /// (as a state with no byte encoding or a write error), so they stay
    /// in the heap.
    #[doc(hidden)]
    pub unpersistable_views: Vec<PhysicalASAPNodeId>,
    /// How the views of a closed window are built.
    pub precompute_mode: PrecomputeMode,
    /// Push mode: how each shard coalesces samples into morsels before
    /// folding them.
    pub push_batching: crate::push::MorselBatching,
    /// Push mode: bytes of open-window state all shards may hold together.
    /// Over it, nothing waits: while some group has an older window open
    /// (whose seal will free memory) the state is kept; otherwise a shard
    /// fails its largest open window. `None`: no limit.
    pub precompute_memory_budget_bytes: Option<u64>,
    /// For tests, push mode: the combine of the window ending at this time
    /// (ms) panics.
    #[doc(hidden)]
    pub panic_in_combine_of_window_end_ms: Option<i64>,
}

/// The default `view_cache_idle_ms`: a minute, four 15 s dashboard
/// refreshes (the plan does not carry its queries' intervals).
pub const DEFAULT_VIEW_CACHE_IDLE_MS: u64 = 60_000;

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            ingestion: IngestionOptions::default(),
            // Two Remote Write requests of the default sample limit.
            max_queued_samples: 2_000_000,
            max_precompute_backlog: 256,
            ingest_threads: std::thread::available_parallelism()
                .map_or(1, |cores| (usize::from(cores) / 4).max(1)),
            shard_delay_ms: 0,
            precompute_threads: std::thread::available_parallelism()
                .map_or(1, |cores| (usize::from(cores) / 4).clamp(1, 4)),
            slow_refreshes: Vec::new(),
            check_task_inputs: false,
            persist_threads: std::thread::available_parallelism()
                .map_or(1, |cores| (usize::from(cores) / 4).clamp(1, 4)),
            max_persist_backlog_bytes: 256 << 20,
            persist_delay_ms: 0,
            no_forward: false,
            run_memory_limit_bytes: 4 << 30,
            query_threads: std::thread::available_parallelism().map_or(1, usize::from),
            share_one_evaluation_results: true,
            query_time_results_retention_ms: 60_000,
            query_time_results_max_bytes: 1 << 30,
            result_cache: true,
            result_cache_max_bytes: 256 << 20,
            view_dir: None,
            // VictoriaMetrics' default -inmemoryDataFlushInterval (v1.155.0).
            view_sync_interval_ms: 5_000,
            view_cache_max_bytes: None,
            view_cache_idle_ms: None,
            view_segment_ms: storage::persist::DEFAULT_SEGMENT_MS,
            view_read: ViewRead::Mmap,
            cold_decode_delay_ms: 0,
            unpersistable_views: Vec::new(),
            precompute_mode: PrecomputeMode::Window,
            precompute_memory_budget_bytes: None,
            push_batching: crate::push::MorselBatching::default(),
            panic_in_combine_of_window_end_ms: None,
        }
    }
}

/// For tests: the windows of refresh group `group` (any group if
/// `None`) ending at `end_ms` (any end if `None`) wait `delay_ms` before
/// they are computed; of a window split by series, only part `part` (every
/// part if `None`). With `per_series`, the delay is a run's over every
/// series, and a run over a part of the series waits that share of it, as
/// work proportional to the samples read would.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlowRefresh {
    pub group: Option<usize>,
    pub end_ms: Option<i64>,
    pub part: Option<u32>,
    pub delay_ms: u64,
    pub per_series: bool,
}

impl SlowRefresh {
    /// Whether it delays `window` (any part of it).
    fn matches(&self, window: ClosedWindow) -> bool {
        self.group.is_none_or(|g| g == window.group)
            && self.end_ms.is_none_or(|e| e == window.end_ms)
    }
}

type Reply<T> = oneshot::Sender<Result<T, Unanswered>>;

/// What the ingest coordinator does next.
enum Command {
    /// A shard routed and appended a batch, or sealed and evicted.
    Report(ShardReport),
    /// The shard numbered here has run every message sent to it before a
    /// flush.
    /// With the shard's raw data as of then: what windows closing behind
    /// the flush read.
    Flushed(usize, Arc<RawData>),
    AdvanceWatermark {
        barrier_ms: i64,
        reply: oneshot::Sender<i64>,
    },
    Stop,
}

/// A shard's state after its last message.
struct ShardReport {
    shard: usize,
    /// The newest sample the shard holds.
    newest_ms: Option<i64>,
    /// Each metric's first sample, while a group waits for one.
    first_seen: Vec<(Arc<str>, i64)>,
    /// Samples of the batch reported, counted out of the queues when the
    /// coordinator applies the report.
    samples: u64,
}

/// What an ingest shard does next.
enum ShardMessage {
    Samples(Vec<RawSample>),
    /// Seal the panes through the watermark and evict by the routing's
    /// cutoffs.
    Maintain {
        routing: Arc<Routing>,
        watermark_ms: Option<i64>,
    },
    /// Report `Flushed` once every message before is run.
    Flush,
    /// Push mode: seal these windows of groups built by push and send
    /// their partials; each group's windows through `through` are closed
    /// or skipped.
    Seal {
        windows: Vec<ClosedWindow>,
        through: Vec<Option<i64>>,
    },
}

/// What the precompute committer does next, in the order the windows
/// closed.
enum PrecomputeJob {
    Refresh {
        window: ClosedWindow,
        raw_data: RawShards,
        closed_at: Instant,
    },
    /// A window of a group built by push: the shards send its partials.
    Push {
        window: ClosedWindow,
        closed_at: Instant,
    },
    /// Answers `barrier_ms` once every window queued before is refreshed.
    Barrier {
        barrier_ms: i64,
        reply: oneshot::Sender<i64>,
    },
}

/// Why the engine did not take samples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IngestError {
    /// More samples are queued than `max_queued_samples`; retry later.
    #[error("the engine is busy: too many samples queued")]
    Busy,
    /// The ingest thread has stopped; no sample will be ingested again.
    #[error("the engine has stopped")]
    Stopped,
}

/// The HTTP side of the engine.
#[derive(Clone)]
pub struct EngineHandle {
    commands: Sender<Command>,
    /// Each ingest shard's queue; a sample goes to the shard of its series.
    shards: Vec<Sender<ShardMessage>>,
    /// Queries for the worker threads.
    queries: Sender<QueryJob>,
    query: Arc<QueryEngine>,
    query_threads: usize,
    shared: Arc<Shared>,
    max_queued_samples: u64,
}

/// What the engine's threads publish to the HTTP side and to each other.
struct Shared {
    /// Samples sent to the ingest shards and not yet ingested.
    queued_samples: AtomicU64,
    /// The same, per shard.
    shard_queued: Vec<AtomicU64>,
    /// What the shards route samples by, as the coordinator last published.
    routing: RwLock<Arc<Routing>>,
    /// Held (shared) while a request is sent to its shards, and
    /// exclusively while a flush is sent, so no flush splits a request.
    admission: RwLock<()>,
    /// Windows closed and not yet refreshed.
    precompute_backlog: AtomicU64,
    /// Refreshed windows not yet written to the view files.
    persist_backlog: AtomicU64,
    /// Bytes of the refreshed windows queued for the persist threads.
    persist_queue: Mutex<u64>,
    /// Signalled when the persist threads release queued bytes.

    /// False once the ingest or precompute thread has exited, for any
    /// reason.
    running: Arc<AtomicBool>,
    /// The ingestion counters: samples and skipped windows as of the last
    /// command the ingest thread ran, refreshed and failed windows as the
    /// precompute thread counts them.
    stats: Mutex<IngestionStats>,
    published: PublishedStore,
    /// Held by the precompute thread during each refresh; tests hold it to
    /// keep a refresh from finishing.
    precompute_gate: Mutex<()>,
    metrics: Metrics,
    /// The views' cold tier, when they are persisted.
    view_tier: Option<Arc<ViewTier>>,
}

/// Marks the engine stopped when its thread exits, panics included.
struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl EngineHandle {
    /// Starts the engine for the plan file `plan_json`, read, checked and
    /// compiled once, and returns the plan's queries. Fails if the plan is
    /// invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let (mut ingestion, precompute, query, query_texts) = install(&plan_json, &options)?;
        let precompute = Arc::new(precompute);
        // Push mode: the groups the shards build by push.
        let push_groups: Vec<Option<PushGroup>> = match options.precompute_mode {
            PrecomputeMode::Window => vec![None; precompute.groups().len()],
            PrecomputeMode::Push => {
                let tasks = precompute.tasks();
                let candidates: Vec<bool> = (0..tasks.split_by_series.len())
                    .map(|g| {
                        tasks.split_by_series[g] && !tasks.serial[g] && tasks.reads[g].is_empty()
                    })
                    .collect();
                crate::push::probe(
                    &precompute,
                    &candidates,
                    &ingestion.group_windows(),
                    &ingestion.group_metrics(),
                )
            }
        };
        ingestion.set_push_groups(push_groups.iter().map(Option::is_some).collect());
        let mut views = ViewStore::default().with_stream_start(options.ingestion.stream_start_ms);
        let mut tier = None;
        let files = match &options.view_dir {
            Some(dir) => {
                let new_tier = Arc::new(view_tier(&options));
                views = ViewStore::tiered(new_tier.clone())
                    .with_stream_start(options.ingestion.stream_start_ms);
                tier = Some(new_tier);
                let sync_interval = std::time::Duration::from_millis(options.view_sync_interval_ms);
                let mut files =
                    ViewFiles::open(dir, plan_hash(plan_json.as_bytes()), sync_interval)?
                        .with_segment_ms(options.view_segment_ms);
                // Indexed, not decoded: the first reads decode what they read.
                let mut index = files.index(&precompute.views())?;
                let mut loaded = 0;
                for (view, schema) in precompute.views().into_iter().flatten() {
                    for (range, location) in index.remove(&view.node_id).unwrap_or_default() {
                        if views
                            .append_stored(&view, range, location, schema.clone())
                            .is_ok()
                        {
                            loaded += 1;
                        }
                    }
                }
                tracing::info!(dir = %dir.display(), windows = loaded, "indexed persisted views");
                if loaded > 0 {
                    ingestion.restarted();
                    views.restarted();
                }
                Some((files, loaded))
            }
            None => None,
        };
        let shard_count = options.ingest_threads.max(1);
        let raw_shards: Vec<Arc<RawData>> = (0..shard_count)
            .map(|_| Arc::new(ingestion.raw_data()))
            .collect();
        let groups: Vec<(i64, Vec<String>)> = precompute
            .groups()
            .into_iter()
            .map(|(width_ms, views)| (width_ms, views.iter().map(ToString::to_string).collect()))
            .collect();
        let mut metrics = Metrics::new(&groups, &query_texts);
        if options.precompute_mode == PrecomputeMode::Push {
            metrics.push = Some(PushMetrics::new(groups.len(), shard_count));
        }
        let pushed: Arc<Vec<bool>> = Arc::new(push_groups.iter().map(Option::is_some).collect());
        // The committer's inbox; the shards' partials reach it through a
        // forwarder.
        let (events, inbox) = channel::<PoolEvent>();
        let (partials, partial_inbox) = channel::<Partial>();
        {
            let events = events.clone();
            std::thread::Builder::new()
                .name("precompute-partials".into())
                .spawn(move || {
                    for partial in partial_inbox {
                        if events.send(PoolEvent::Partial(partial)).is_err() {
                            break;
                        }
                    }
                })?;
        }
        let running = Arc::new(AtomicBool::new(true));
        let shared = Arc::new(Shared {
            queued_samples: AtomicU64::new(0),
            shard_queued: (0..shard_count).map(|_| AtomicU64::new(0)).collect(),
            routing: RwLock::new(Arc::new(ingestion.routing())),
            admission: RwLock::new(()),
            precompute_backlog: AtomicU64::new(0),
            persist_backlog: AtomicU64::new(0),
            persist_queue: Mutex::new(0),

            running: running.clone(),
            stats: Mutex::new(IngestionStats::default()),
            published: PublishedStore::new(Arc::new(raw_shards.clone())),
            precompute_gate: Mutex::new(()),
            metrics,
            view_tier: tier.clone(),
        });
        let (commands, command_receiver) = channel();
        let (jobs, job_receiver) = sync_channel(options.max_precompute_backlog.max(1));
        let (queries, query_receiver) = channel::<QueryJob>();

        let mut shards = Vec::new();
        for (index, raw_data) in raw_shards.iter().enumerate() {
            let (shard, messages) = channel();
            let (coordinator, raw_data) = (commands.clone(), raw_data.clone());
            let shard_shared = shared.clone();
            let guard = RunningGuard(running.clone());
            let delay = std::time::Duration::from_millis(if index == 0 {
                0
            } else {
                options.shard_delay_ms
            });
            let push_init = (options.precompute_mode == PrecomputeMode::Push).then(|| {
                (
                    precompute.clone(),
                    push_groups.clone(),
                    ingestion.pane_width_ms(),
                    options.precompute_memory_budget_bytes,
                    partials.clone(),
                    options.push_batching,
                )
            });
            std::thread::Builder::new()
                .name(format!("ingest-{index}"))
                .spawn(move || {
                    let _guard = guard;
                    let push =
                        push_init.map(|(precompute, groups, pane, budget, partials, batching)| {
                            PushShard::new(
                                index, precompute, groups, pane, budget, partials, batching,
                            )
                        });
                    serve_shard(
                        index,
                        raw_data,
                        messages,
                        coordinator,
                        &shard_shared,
                        delay,
                        push,
                    );
                })?;
            shards.push(shard);
        }
        let ingest_guard = RunningGuard(running.clone());
        let ingest_shared = shared.clone();
        let coordinator_shards = shards.clone();
        std::thread::Builder::new()
            .name("ingest".into())
            .spawn(move || {
                let _guard = ingest_guard;
                serve_ingest(
                    ingestion,
                    raw_shards,
                    coordinator_shards,
                    command_receiver,
                    jobs,
                    &ingest_shared,
                    &pushed,
                );
            })?;
        let views = Arc::new(views);
        shared.published.publish_views(views.clone());
        shared
            .metrics
            .view_bytes
            .store(views.bytes() as u64, Ordering::Relaxed);
        let persist = match files {
            Some((files, loaded)) => {
                shared
                    .metrics
                    .view_windows_loaded
                    .store(loaded, Ordering::Relaxed);
                shared
                    .metrics
                    .view_file_bytes
                    .store(files.bytes(), Ordering::Relaxed);
                let unpersistable: Arc<BTreeSet<PhysicalASAPNodeId>> =
                    Arc::new(options.unpersistable_views.iter().copied().collect());
                let mut shards = Vec::new();
                for (index, files) in files.split(options.persist_threads).into_iter().enumerate() {
                    let (shard, jobs) = channel();
                    let persist_shared = shared.clone();
                    let delay = std::time::Duration::from_millis(options.persist_delay_ms);
                    let unpersistable = unpersistable.clone();
                    std::thread::Builder::new()
                        .name(format!("persist-{index}"))
                        .spawn(move || {
                            serve_persist(files, jobs, &persist_shared, delay, &unpersistable)
                        })?;
                    shards.push(shard);
                }
                Some(PersistPool {
                    shards,
                    max_bytes: options.max_persist_backlog_bytes,
                })
            }
            None => None,
        };
        if let Some(tier) = &tier {
            let (tier, monitor_running) = (tier.clone(), running.clone());
            let budget = tier.cache.max_bytes();
            std::thread::Builder::new()
                .name("view-cache".into())
                .spawn(move || serve_view_cache(tier, budget, monitor_running))?;
        }
        spawn_precompute(
            precompute,
            views,
            persist,
            job_receiver,
            (events, inbox),
            PoolOptions {
                threads: options.precompute_threads.max(1),
                slow_refreshes: options.slow_refreshes.clone(),
                check_task_inputs: options.check_task_inputs,
                push_parts: shard_count as u32,
                panic_in_combine_of_window_end_ms: options.panic_in_combine_of_window_end_ms,
            },
            RunningGuard(running),
            &shared,
        )?;
        let query = Arc::new(query);
        spawn_query_workers(
            query.clone(),
            shared.clone(),
            query_receiver,
            options.query_threads.max(1),
        )?;
        let handle = Self {
            commands,
            shards,
            queries,
            query,
            query_threads: options.query_threads.max(1),
            shared,
            max_queued_samples: options.max_queued_samples,
        };
        Ok((handle, query_texts))
    }

    /// Queues `samples` for ingestion. `Busy` when the queue would hold
    /// more than `max_queued_samples`; `Stopped` when the engine has exited.
    pub fn try_ingest(&self, samples: Vec<RawSample>) -> Result<(), IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        // Each series' samples go to one shard, in order.
        let mut by_shard: Vec<Vec<RawSample>> = Vec::new();
        if self.shards.len() == 1 {
            by_shard.push(samples);
        } else {
            // Room for a fair share and a quarter more, so a request rarely
            // grows a shard's vector.
            let room = samples.len() / self.shards.len() * 5 / 4 + 1;
            by_shard.resize_with(self.shards.len(), || Vec::with_capacity(room));
            for sample in samples {
                by_shard[sample.labels.id as usize % self.shards.len()].push(sample);
            }
        }
        let counts: Vec<u64> = by_shard.iter().map(|s| s.len() as u64).collect();
        // A request reaches its shards whole between two flushes, never
        // split by one: a flush covers all of it or none.
        let _sending = self
            .shared
            .admission
            .read()
            .unwrap_or_else(|e| e.into_inner());
        admit_shards(&self.shared.shard_queued, &counts, self.max_queued_samples)?;
        let shared = &self.shared;
        for (shard, samples) in by_shard.into_iter().enumerate() {
            let count = samples.len() as u64;
            if count == 0 {
                continue;
            }
            shared.queued_samples.fetch_add(count, Ordering::SeqCst);
            if self.shards[shard]
                .send(ShardMessage::Samples(samples))
                .is_err()
            {
                // This shard and the ones not sent to yet were admitted
                // but get nothing.
                shared.queued_samples.fetch_sub(count, Ordering::SeqCst);
                for (queued, &later) in shared.shard_queued[shard..].iter().zip(&counts[shard..]) {
                    queued.fetch_sub(later, Ordering::SeqCst);
                }
                return Err(IngestError::Stopped);
            }
        }
        Ok(())
    }

    /// Answers the planned `query` at `time_ms` on a query worker, from
    /// the newest snapshot published when the run starts; see
    /// [`QueryEngine::evaluate`]. Two queries at one evaluation time may
    /// read different snapshots. A query does not wait for writes
    /// acknowledged before it; an event-time barrier does
    /// ([`Self::advance_watermark`]).
    pub async fn evaluate(&self, query: String, time_ms: i64) -> Result<Answer, Unanswered> {
        let started = std::time::Instant::now();
        let metrics = self.shared.metrics.query(&query);
        let answer = self.run_on_worker(query, time_ms, None).await;
        if let Some(metrics) = metrics {
            metrics.latency.observe(started.elapsed().as_secs_f64());
        }
        answer
    }

    /// Evaluates one step of a range query over `snapshot`, which every
    /// step of the request reads ([`Self::snapshot`]): as
    /// [`Self::evaluate`], counted as a range step rather than an instant
    /// query. A step whose future is dropped before a worker takes it (the
    /// range failed or its client went away) is not run.
    pub async fn evaluate_range_step(
        &self,
        query: String,
        time_ms: i64,
        snapshot: Arc<Snapshot>,
    ) -> Result<Answer, Unanswered> {
        self.run_on_worker(query, time_ms, Some(snapshot)).await
    }

    /// The newest published store, for the steps of one range request.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        Arc::new(self.query.snapshot(&self.shared.published))
    }

    /// The result cache's answer to the planned `query` at `time_ms`, if
    /// it keeps one ([`QueryEngine::cached`]).
    pub fn cached(&self, query: &str, time_ms: i64) -> Option<Answer> {
        self.query.cached(query, time_ms)
    }

    async fn run_on_worker(
        &self,
        query: String,
        time_ms: i64,
        range_step: Option<Arc<Snapshot>>,
    ) -> Result<Answer, Unanswered> {
        let stopped = || Unanswered::Execution("the query workers have stopped".into());
        let (reply, answer) = oneshot::channel();
        self.queries
            .send(QueryJob {
                query,
                time_ms,
                range_step,
                reply,
            })
            .map_err(|_| stopped())?;
        answer.await.map_err(|_| stopped())?
    }

    /// The query worker threads: the most query runs at once.
    pub fn query_threads(&self) -> usize {
        self.query_threads
    }

    /// How many query-time `OneEvaluation` nodes several queries share.
    pub fn shared_nodes(&self) -> usize {
        self.query.shared_nodes().len()
    }

    /// The query-time partition of the view store.
    pub fn query_time_results(&self) -> &QueryTimeResults<Unanswered> {
        self.query.query_time_results()
    }

    /// The engine's measurements.
    pub fn metrics(&self) -> &Metrics {
        &self.shared.metrics
    }

    /// The engine's measurements and counters in the Prometheus text
    /// format.
    pub fn render_metrics(&self) -> String {
        crate::metrics_endpoint::render(
            &self.shared.metrics,
            &Gauges {
                stats: self.stats(),
                samples_queued: self.queued_samples(),
                precompute_backlog: self.precompute_backlog(),
                persist_backlog: self.shared.persist_backlog.load(Ordering::SeqCst),
                persist_backlog_bytes: *self
                    .shared
                    .persist_queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
                now_ms: crate::ingestion::wall_clock_ms(),
                query_time_results: self.query.query_time_results(),
                result_cache: self.query.result_cache(),
                view_tier: self.shared.view_tier.as_deref(),
            },
        )
    }

    /// Whether the ingest and precompute threads are still running.
    pub fn is_running(&self) -> bool {
        self.shared.running.load(Ordering::SeqCst)
    }

    /// Samples queued and not yet ingested.
    pub fn queued_samples(&self) -> u64 {
        self.shared.queued_samples.load(Ordering::SeqCst)
    }

    /// Refresh windows closed and not yet refreshed.
    pub fn precompute_backlog(&self) -> u64 {
        self.shared.precompute_backlog.load(Ordering::SeqCst)
    }

    /// The ingestion counters.
    pub fn stats(&self) -> IngestionStats {
        *self.shared.stats.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Queues an event-time barrier behind every write already acknowledged
    /// and waits until it has run and every window it closed is refreshed:
    /// every window ending at or before `barrier_ms` (capped at the wall
    /// clock) is then answered from. Returns the barrier in force; see
    /// [`Ingestion::advance_watermark_at`].
    pub async fn advance_watermark(&self, barrier_ms: i64) -> Result<i64, IngestError> {
        if !self.is_running() {
            return Err(IngestError::Stopped);
        }
        let (reply, done) = oneshot::channel();
        self.commands
            .send(Command::AdvanceWatermark { barrier_ms, reply })
            .map_err(|_| IngestError::Stopped)?;
        done.await.map_err(|_| IngestError::Stopped)
    }

    /// For tests: stops the ingest thread after the commands queued before,
    /// and the precompute thread once it has refreshed the windows queued
    /// before. Samples queued between this call and the stop are
    /// acknowledged and then lost; ingestion after the stop fails with
    /// [`IngestError::Stopped`].
    #[doc(hidden)]
    pub fn stop(&self) {
        let _ = self.commands.send(Command::Stop);
    }

    /// For tests: no refresh finishes while the guard is held.
    #[doc(hidden)]
    pub fn hold_precompute(&self) -> MutexGuard<'_, ()> {
        self.shared
            .precompute_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// Admits `counts[i]` samples to shard `i`'s queue `queued[i]` for every
/// shard, or none: `Busy` if any shard's queue would pass `max_queued`
/// ([`admit`]), and then no queue changes, so a request is taken whole or
/// not at all.
fn admit_shards(queued: &[AtomicU64], counts: &[u64], max_queued: u64) -> Result<(), IngestError> {
    let mut admitted: Vec<usize> = Vec::new();
    for (shard, &count) in counts.iter().enumerate() {
        if count == 0 {
            continue;
        }
        let queue = &queued[shard];
        let mut current = queue.load(Ordering::SeqCst);
        let result = loop {
            let total = match admit(current, count, max_queued) {
                Ok(total) => total,
                Err(error) => break Err(error),
            };
            match queue.compare_exchange(current, total, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break Ok(()),
                Err(now) => current = now,
            }
        };
        if let Err(error) = result {
            for shard in admitted {
                queued[shard].fetch_sub(counts[shard], Ordering::SeqCst);
            }
            return Err(error);
        }
        admitted.push(shard);
    }
    Ok(())
}

/// The queue length after admitting `count` samples to a queue of
/// `queued`: admitted while the total stays within `max_queued`, and always
/// into an empty queue, so a bound smaller than one request cannot refuse
/// that request forever.
fn admit(queued: u64, count: u64, max_queued: u64) -> Result<u64, IngestError> {
    let total = queued.checked_add(count).ok_or(IngestError::Busy)?;
    if queued == 0 || total <= max_queued {
        Ok(total)
    } else {
        Err(IngestError::Busy)
    }
}

/// One query for a worker thread.
struct QueryJob {
    query: String,
    time_ms: i64,
    /// One step of a range query, and the snapshot every step reads.
    range_step: Option<Arc<Snapshot>>,
    reply: Reply<Answer>,
}

/// What crosses threads: the compiled queries and refresh runs, the store
/// snapshots runs read, and what the threads share.
#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn assert_send<T: Send>() {}
const _: fn() = || {
    assert_send_sync::<QueryEngine>();
    assert_send_sync::<Shared>();
    assert_send_sync::<RunStore>();
    assert_send_sync::<RawData>();
    assert_send_sync::<ViewStore>();
    assert_send_sync::<Precompute>();
    assert_send::<Ingestion>();
};

/// Starts `threads` workers `query-0`, `query-1`, …, each running one query
/// at a time to completion from `jobs`; more queries wait in `jobs`.
fn spawn_query_workers(
    query: Arc<QueryEngine>,
    shared: Arc<Shared>,
    jobs: Receiver<QueryJob>,
    threads: usize,
) -> std::io::Result<()> {
    let jobs = Arc::new(Mutex::new(jobs));
    for index in 0..threads {
        let (query, shared, jobs) = (query.clone(), shared.clone(), jobs.clone());
        std::thread::Builder::new()
            .name(format!("query-{index}"))
            .spawn(move || loop {
                let job = match jobs.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                    Ok(job) => job,
                    Err(_) => break,
                };
                // Nobody waits for it any more (a failed range, a client
                // that went away).
                if job.reply.is_closed() {
                    continue;
                }
                let timer = Timer::start();
                // A bug in one run must not take the worker down: it
                // answers that run with the error and keeps serving.
                let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = match &job.range_step {
                        Some(snapshot) => snapshot.clone(),
                        None => Arc::new(query.snapshot(&shared.published)),
                    };
                    query.evaluate(&job.query, job.time_ms, &snapshot)
                }))
                .unwrap_or_else(|panic| {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(ToString::to_string)
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    tracing::error!(query = %job.query, time_ms = job.time_ms, %reason, "query run panicked");
                    Err(Unanswered::Execution(format!("the query run panicked: {reason}")))
                });
                if let Some(metrics) = shared.metrics.query(&job.query) {
                    match job.range_step {
                        None => metrics.run.record(timer),
                        Some(_) => metrics.range_step_run.record(timer),
                    };
                }
                let _ = job.reply.send(answer);
            })?;
    }
    Ok(())
}

/// Reads, checks and compiles the plan file `plan_json`: its ingestion,
/// refresh runs, queries and query texts.
fn install(
    plan_json: &str,
    options: &EngineOptions,
) -> anyhow::Result<(Ingestion, Precompute, QueryEngine, Vec<String>)> {
    let plan = InstallablePlan::from_json(plan_json)?;
    plan.validate()?;
    let views = plan.materialized_views()?;
    let groups = plan.ingestion_refresh_groups()?;
    let limits = Limits {
        max_bytes: options.run_memory_limit_bytes,
    };
    tracing::info!(
        views = views.len(),
        refresh_groups = groups.len(),
        "materialized views"
    );
    let results = QueryTimeResults::new(
        storage::store::millis(options.query_time_results_retention_ms),
        options.query_time_results_max_bytes,
    );
    let query = QueryEngine::new(
        &plan,
        &views,
        limits.clone(),
        results,
        options.share_one_evaluation_results,
        options.result_cache.then(|| {
            ResultCache::new(
                ResultCache::horizon_ms(&plan.dag, options.ingestion.view_retention_margin_ms),
                options.result_cache_max_bytes,
            )
        }),
    )?;
    tracing::info!(
        shared = ?query.shared_nodes(),
        "query-time OneEvaluation nodes shared across queries"
    );
    let unexecutable = query.unexecutable();
    if options.no_forward && !unexecutable.is_empty() {
        let reasons: Vec<String> = unexecutable
            .iter()
            .map(|(query, reason)| format!("{query:?}: {reason}"))
            .collect();
        anyhow::bail!(
            "--no-forward refuses a plan with queries the data plane cannot run: {}",
            reasons.join("; ")
        );
    }
    let (ingestion, precompute) =
        Ingestion::new(&plan.dag, &views, groups, options.ingestion, limits)?;
    tracing::info!(pane_width_ms = ingestion.pane_width_ms(), "raw data panes");
    Ok((ingestion, precompute, query, plan.query_texts.clone()))
}

/// Sends every shard a flush, with no request half sent: each request
/// already admitted is wholly before it in its shards' queues. Returns
/// the wall-clock time it was sent: every request admitted later holds
/// samples stamped later (a sender stamps before it sends).
fn flush(shards: &[Sender<ShardMessage>], shared: &Shared) -> i64 {
    let _admission = shared.admission.write().unwrap_or_else(|e| e.into_inner());
    let sent_ms = crate::ingestion::wall_clock_ms();
    for shard in shards {
        let _ = shard.send(ShardMessage::Flush);
    }
    sent_ms
}

/// An ingest shard: routes and appends the samples of its series to its
/// own raw data, seals and evicts it when told, and reports its state to
/// the coordinator after each message.
fn serve_shard(
    index: usize,
    mut raw_data: Arc<RawData>,
    messages: Receiver<ShardMessage>,
    coordinator: Sender<Command>,
    shared: &Shared,
    delay: std::time::Duration,
    mut push: Option<PushShard>,
) {
    let mut newest_ms: Option<i64> = None;
    // A message taken while draining a batch, to run next.
    let mut next: Option<ShardMessage> = None;
    loop {
        let message = match next.take() {
            Some(message) => message,
            None => match messages.recv() {
                Ok(message) => message,
                Err(_) => break,
            },
        };
        let mut samples = match message {
            ShardMessage::Samples(samples) => samples,
            // Eviction changes no sample time: no report (each would start
            // another coordinator pass).
            ShardMessage::Maintain {
                routing,
                watermark_ms,
            } => {
                routing.maintain(Arc::make_mut(&mut raw_data), watermark_ms);
                continue;
            }
            ShardMessage::Flush => {
                // The raw data travels only with a flush, which a window
                // closing reads; the coordinator lets it go after its pass,
                // so appending rarely copies the pane lists it shares.
                if coordinator
                    .send(Command::Flushed(index, raw_data.clone()))
                    .is_err()
                {
                    break;
                }
                continue;
            }
            ShardMessage::Seal { windows, through } => {
                if let Some(push) = &mut push {
                    push.seal(&windows, &through, &shared.metrics);
                }
                continue;
            }
        };
        // Every batch already queued, appended and reported at once: one
        // report (and so at most one coordinator pass) per drained queue,
        // not per request.
        let mut count = 0;
        let mut first_seen = Vec::new();
        loop {
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            let timer = Timer::start();
            count += samples.len() as u64;
            let routing = shared
                .routing
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let routed = routing.route(samples, crate::ingestion::wall_clock_ms());
            Arc::make_mut(&mut raw_data).append(&routed.accepted);
            newest_ms = newest_ms.max(routed.newest_ms);
            if let Some(push) = &mut push {
                push.push(&routed.accepted, &shared.metrics);
            }
            {
                let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
                stats.samples_accepted += routed.stats.samples_accepted;
                stats.samples_dropped_late += routed.stats.samples_dropped_late;
                stats.samples_dropped_future += routed.stats.samples_dropped_future;
                stats.samples_unrouted += routed.stats.samples_unrouted;
                stats.samples_late_for_views += routed.stats.samples_late_for_views;
            }
            shared.metrics.append.record(timer);
            first_seen.extend(routed.first_seen);
            match messages.try_recv() {
                Ok(ShardMessage::Samples(more)) => samples = more,
                Ok(other) => {
                    next = Some(other);
                    break;
                }
                Err(_) => break,
            }
        }
        let report = ShardReport {
            shard: index,
            newest_ms,
            first_seen,
            samples: count,
        };
        // The coordinator counts the samples out of the queues when it
        // applies the report, so samples stay counted while it cannot keep
        // up (waiting for the precompute workers): the queues fill and
        // Remote Write gets 503, and reports waiting for it stay bounded.
        if coordinator.send(Command::Report(report)).is_err() {
            break;
        }
    }
}

/// The ingest coordinator: keeps the watermark over the shards, closes the
/// windows it passes and queues their refreshes over every shard's raw data
/// as it was then, publishes the raw data and the routing, and has the
/// shards seal and evict as the watermark moves. The watermark follows the
/// newest sample of any shard, capped at the wall clock; it moves only
/// behind a flush, which every pass that took a report or a barrier
/// sends: every shard runs the samples queued to it, so a window closes
/// over every sample admitted before. The clock cap is the time that flush
/// was sent (a request admitted later holds samples stamped later), never
/// a report's time. Barriers that come during a flush take the next pass.
/// Shards with nothing queued answer the flush at once, so idle shards
/// never hold the watermark; the coordinator also wakes every tick.
fn serve_ingest(
    mut ingestion: Ingestion,
    mut raw_shards: Vec<Arc<RawData>>,
    shards: Vec<Sender<ShardMessage>>,
    commands: Receiver<Command>,
    jobs: SyncSender<PrecomputeJob>,
    shared: &Shared,
    pushed: &[bool],
) {
    const TICK: std::time::Duration = std::time::Duration::from_millis(100);
    let mut newest: Vec<Option<i64>> = vec![None; shards.len()];
    let mut sealed_through: Option<i64> = None;
    let apply = |report: ShardReport, ingestion: &mut Ingestion, newest: &mut Vec<Option<i64>>| {
        ingestion.observe(&report.first_seen);
        shared.shard_queued[report.shard].fetch_sub(report.samples, Ordering::SeqCst);
        shared
            .queued_samples
            .fetch_sub(report.samples, Ordering::SeqCst);
        newest[report.shard] = report.newest_ms;
    };
    // Barriers that came while a flush was waited for: written after it,
    // so they take the next pass, behind its own flush.
    let mut deferred: VecDeque<(i64, oneshot::Sender<i64>)> = VecDeque::new();
    // Reports a shard sent after answering a flush, of samples admitted
    // after it: held for the next pass, so they move neither this pass's
    // watermark nor its raw data. Without this, a shard that ran ahead
    // would move the watermark past samples other shards have yet to
    // append, and windows would close without them.
    let mut held: Vec<ShardReport> = Vec::new();
    let queries_read_raw_data = ingestion.queries_read_raw_data();
    // Published in place of the raw data when no query reads it.
    let no_raw_data: RawShards = Arc::new(
        (0..shards.len())
            .map(|_| Arc::new(ingestion.raw_data()))
            .collect(),
    );
    'serve: loop {
        let mut barriers: Vec<_> = deferred.drain(..).collect();
        // Whether anything this pass may move the watermark: a newer
        // sample, the wall clock a report carries (the watermark is capped
        // at it), a metric's first sample, or a barrier.
        let mut moved = !barriers.is_empty() || !held.is_empty();
        for report in held.drain(..) {
            apply(report, &mut ingestion, &mut newest);
        }
        if !moved {
            match commands.recv_timeout(TICK) {
                Ok(Command::Report(report)) => {
                    apply(report, &mut ingestion, &mut newest);
                    moved = true;
                }
                Ok(Command::Flushed(..)) => {}
                Ok(Command::AdvanceWatermark { barrier_ms, reply }) => {
                    barriers.push((barrier_ms, reply));
                    moved = true;
                }
                Ok(Command::Stop) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        // Drain what was already sent, so one pass takes it all. A barrier
        // taken here is behind every write acknowledged before it, as the
        // flush below is sent after it.
        while let Ok(command) = commands.try_recv() {
            match command {
                Command::Report(report) => {
                    apply(report, &mut ingestion, &mut newest);
                    moved = true;
                }
                Command::Flushed(..) => {}
                Command::AdvanceWatermark { barrier_ms, reply } => {
                    barriers.push((barrier_ms, reply));
                    moved = true;
                }
                Command::Stop => break 'serve,
            }
        }
        // Before the watermark moves, every shard runs the samples queued to
        // it, so the windows it closes hold every sample admitted so far
        // and the raw data they read is every shard's as of then.
        let flushed = moved;
        if moved {
            let flushed_ms = flush(&shards, shared);
            let mut flushed = vec![false; shards.len()];
            while !flushed.iter().all(|&f| f) {
                match commands.recv_timeout(TICK) {
                    Ok(Command::Report(report))
                        if flushed[report.shard]
                            || held.iter().any(|r| r.shard == report.shard) =>
                    {
                        held.push(report)
                    }
                    Ok(Command::Report(report)) => apply(report, &mut ingestion, &mut newest),
                    Ok(Command::Flushed(shard, raw_data)) => {
                        flushed[shard] = true;
                        raw_shards[shard] = raw_data;
                    }
                    Ok(Command::AdvanceWatermark { barrier_ms, reply }) => {
                        deferred.push_back((barrier_ms, reply))
                    }
                    Ok(Command::Stop) => break 'serve,
                    // A shard that died never answers: stop, failing the
                    // barriers waiting.
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if !shared.running.load(Ordering::SeqCst) {
                            break 'serve;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break 'serve,
                }
            }
            ingestion.advance_clock(flushed_ms);
        }
        let newest_ms = newest.iter().flatten().copied().max();
        ingestion.advance_newest_sample(newest_ms);
        let mut closed = ingestion.close();
        let mut answered = Vec::new();
        for (barrier_ms, reply) in barriers {
            let (barrier_ms, more) = ingestion.advance_watermark(barrier_ms);
            closed.extend(more);
            answered.push((barrier_ms, reply));
        }
        let snapshot: RawShards = Arc::new(raw_shards.clone());
        let sealed: Vec<ClosedWindow> = closed
            .iter()
            .copied()
            .filter(|w| pushed.get(w.group).copied().unwrap_or(false))
            .collect();
        if !sealed.is_empty() {
            let through = ingestion.refreshed_through();
            for shard in &shards {
                let _ = shard.send(ShardMessage::Seal {
                    windows: sealed.clone(),
                    through: through.clone(),
                });
            }
        }
        if !queue_refreshes(&closed, &snapshot, &jobs, shared, pushed) {
            break;
        }
        let routing = Arc::new(ingestion.routing());
        *shared.routing.write().unwrap_or_else(|e| e.into_inner()) = routing.clone();
        let watermark_ms = ingestion.watermark_ms();
        if watermark_ms != sealed_through || !closed.is_empty() {
            sealed_through = watermark_ms;
            for shard in &shards {
                let _ = shard.send(ShardMessage::Maintain {
                    routing: routing.clone(),
                    watermark_ms,
                });
            }
        }
        let metrics = &shared.metrics;
        if let Some(watermark_ms) = watermark_ms {
            metrics.watermark_ms.store(watermark_ms, Ordering::Relaxed);
        }
        if flushed {
            let sum = |f: fn(&RawData) -> usize| snapshot.iter().map(|r| f(r) as u64).sum::<u64>();
            metrics
                .raw_data_bytes
                .store(sum(RawData::bytes), Ordering::Relaxed);
            metrics
                .raw_data_samples
                .store(sum(RawData::len), Ordering::Relaxed);
            metrics
                .raw_data_panes
                .store(sum(RawData::panes), Ordering::Relaxed);
        }
        if queries_read_raw_data {
            shared
                .published
                .publish_raw_data(snapshot.clone(), watermark_ms);
        } else {
            // No query reads the raw data, and the next windows to close
            // read it as of their own flush: let the shards' raw data go,
            // so their next appends own it again instead of copying the
            // pane lists this snapshot shares (only the queued refreshes
            // still hold it, until they run).
            shared
                .published
                .publish_raw_data(no_raw_data.clone(), watermark_ms);
            drop(snapshot);
            for raw_data in raw_shards.iter_mut() {
                *raw_data = no_raw_data[0].clone();
            }
        }
        {
            let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
            stats.windows_skipped_first = ingestion.stats.windows_skipped_first;
            stats.windows_skipped_expired = ingestion.stats.windows_skipped_expired;
        }
        for (barrier_ms, reply) in answered {
            if jobs
                .send(PrecomputeJob::Barrier { barrier_ms, reply })
                .is_err()
            {
                break 'serve;
            }
        }
    }
    tracing::warn!("ingest stopped");
}

/// Queues a refresh of each of `closed` over `raw_data`, waiting while the
/// backlog is full; `false` once the precompute thread has exited.
fn queue_refreshes(
    closed: &[ClosedWindow],
    raw_data: &RawShards,
    jobs: &SyncSender<PrecomputeJob>,
    shared: &Shared,
    pushed: &[bool],
) -> bool {
    for &window in closed {
        shared.precompute_backlog.fetch_add(1, Ordering::SeqCst);
        let job = if pushed.get(window.group).copied().unwrap_or(false) {
            PrecomputeJob::Push {
                window,
                closed_at: Instant::now(),
            }
        } else {
            PrecomputeJob::Refresh {
                window,
                raw_data: raw_data.clone(),
                closed_at: Instant::now(),
            }
        };
        if jobs.send(job).is_err() {
            return false;
        }
    }
    true
}

/// How the precompute task pool runs.
#[derive(Clone)]
struct PoolOptions {
    threads: usize,
    slow_refreshes: Vec<SlowRefresh>,
    check_task_inputs: bool,
    /// The partials a pushed window combines: one per ingest shard.
    push_parts: u32,
    panic_in_combine_of_window_end_ms: Option<i64>,
}

/// What the precompute committer waits for.
enum PoolEvent {
    /// The next job, in the order the windows closed.
    Job(PrecomputeJob),
    /// A worker computed window `seq`, or its part `part` of a window split
    /// by series.
    Computed {
        seq: u64,
        part: u32,
        result: Result<Computed, String>,
        times: TaskTimes,
    },
    /// A shard's partial of a pushed window.
    Partial(Partial),
    /// No job will come any more.
    Closed,
}

/// When a window's computation started and ended.
#[derive(Debug, Clone, Copy)]
struct TaskTimes {
    started: Instant,
    computed: Instant,
}

/// A window for a worker to compute.
struct WindowRun {
    seq: u64,
    window: ClosedWindow,
    raw_data: RawShards,
    /// The views as committed when the run was started: every window of
    /// the groups it reads that closed before it.
    views: Arc<ViewStore>,
    /// The series the run reads, when the window is split by series.
    partition: Option<SeriesPartition>,
}

/// Starts the precompute task pool: the committer (`precompute`, today's
/// precompute thread), which takes the closed windows in order, starts
/// them, and stores and publishes their rows in window order, so the view
/// store keeps one writer; and, with more than one thread, the workers
/// `precompute-N` that compute the windows, and `precompute-queue`, which
/// takes jobs from the bounded backlog for the committer as room frees.
#[allow(clippy::too_many_arguments)]
fn spawn_precompute(
    precompute: Arc<Precompute>,
    views: Arc<ViewStore>,
    persist: Option<PersistPool>,
    jobs: Receiver<PrecomputeJob>,
    (events, inbox): (Sender<PoolEvent>, Receiver<PoolEvent>),
    options: PoolOptions,
    guard: RunningGuard,
    shared: &Arc<Shared>,
) -> std::io::Result<()> {
    shared
        .metrics
        .precompute_threads
        .store(options.threads as u64, Ordering::Relaxed);
    for (group, &split) in precompute.tasks().split_by_series.iter().enumerate() {
        let parts = if split { options.threads } else { 1 };
        shared.metrics.precompute_windows[group]
            .parts
            .store(parts as u64, Ordering::Relaxed);
    }
    // Windows taken from the backlog and not yet committed: one at a time
    // when the committer computes them itself; with workers, two per
    // worker, so a slow window does not leave the others idle while the
    // windows after it wait to be committed.
    let slots = Arc::new(Slots::new(if options.threads == 1 {
        1
    } else {
        2 * options.threads
    }));
    let workers = if options.threads == 1 {
        None
    } else {
        let (runs, run_receiver) = channel::<WindowRun>();
        let run_receiver = Arc::new(Mutex::new(run_receiver));
        for index in 0..options.threads {
            let (precompute, shared, runs, events) = (
                precompute.clone(),
                shared.clone(),
                run_receiver.clone(),
                events.clone(),
            );
            let checks = options.clone();
            std::thread::Builder::new()
                .name(format!("precompute-{index}"))
                .spawn(move || loop {
                    let run = match runs.lock().unwrap_or_else(|e| e.into_inner()).recv() {
                        Ok(run) => run,
                        Err(_) => break,
                    };
                    let (result, times) = compute_window(
                        &precompute,
                        run.window,
                        run.raw_data,
                        run.views,
                        run.partition,
                        &checks,
                        &shared,
                    );
                    let event = PoolEvent::Computed {
                        seq: run.seq,
                        part: run.partition.map_or(0, |p| p.index),
                        result,
                        times,
                    };
                    if events.send(event).is_err() {
                        break;
                    }
                })?;
        }
        Some(runs)
    };
    {
        let (slots, shared) = (slots.clone(), shared.clone());
        std::thread::Builder::new()
            .name("precompute-queue".into())
            .spawn(move || {
                for job in jobs {
                    if matches!(
                        job,
                        PrecomputeJob::Refresh { .. } | PrecomputeJob::Push { .. }
                    ) && !slots.acquire(&shared.running)
                    {
                        break;
                    }
                    if events.send(PoolEvent::Job(job)).is_err() {
                        break;
                    }
                }
                let _ = events.send(PoolEvent::Closed);
            })?;
    }
    let shared = shared.clone();
    std::thread::Builder::new()
        .name("precompute".into())
        .spawn(move || {
            let _guard = guard;
            serve_precompute(
                &precompute,
                views,
                persist,
                inbox,
                workers,
                &slots,
                &options,
                &shared,
            );
        })?;
    Ok(())
}

/// A counting semaphore: the windows taken from the backlog and not yet
/// committed.
struct Slots {
    free: Mutex<usize>,
    freed: Condvar,
}

impl Slots {
    fn new(count: usize) -> Self {
        Self {
            free: Mutex::new(count),
            freed: Condvar::new(),
        }
    }

    /// Takes a slot, waiting for one; `false` once the engine has stopped.
    fn acquire(&self, running: &AtomicBool) -> bool {
        let mut free = self.free.lock().unwrap_or_else(|e| e.into_inner());
        while *free == 0 {
            if !running.load(Ordering::SeqCst) {
                return false;
            }
            free = self
                .freed
                .wait_timeout(free, std::time::Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        *free -= 1;
        true
    }

    fn release(&self) {
        *self.free.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        self.freed.notify_one();
    }
}

/// Computes `window` on this thread, after its test delay; a panic in the
/// run fails the window, not the thread.
fn compute_window(
    precompute: &Precompute,
    window: ClosedWindow,
    raw_data: RawShards,
    views: Arc<ViewStore>,
    partition: Option<SeriesPartition>,
    options: &PoolOptions,
    shared: &Shared,
) -> (Result<Computed, String>, TaskTimes) {
    let started = Instant::now();
    let inputs = options
        .check_task_inputs
        .then(|| TaskInputs::read(precompute, window, &raw_data, &views));
    let delay_ms: u64 = options
        .slow_refreshes
        .iter()
        .filter(|s| s.matches(window))
        .filter(|s| {
            s.part
                .is_none_or(|part| partition.is_none_or(|p| p.index == part))
        })
        .map(|s| match (s.per_series, partition) {
            (true, Some(partition)) => s.delay_ms / u64::from(partition.count),
            _ => s.delay_ms,
        })
        .sum();
    if delay_ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
    let timer = Timer::start();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        precompute
            .compute(window, raw_data, views, partition)
            .map_err(|e| e.to_string())
    }))
    .unwrap_or_else(|panic| {
        let reason = panic
            .downcast_ref::<&str>()
            .map(ToString::to_string)
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        Err(format!("the ingestion run panicked: {reason}"))
    });
    if !matches!(result, Ok(Computed::AlreadyStored)) {
        shared.metrics.precompute[window.group].1.record(timer);
    }
    let result = match inputs {
        Some((before, raw_data, views)) => result.and_then(|computed| {
            before.check(window)?;
            if TaskInputs::read(precompute, window, &raw_data, &views).0 != before {
                return Err("a run's inputs changed while it ran".into());
            }
            Ok(computed)
        }),
        None => result,
    };
    let computed = Instant::now();
    shared
        .metrics
        .precompute_busy_ns
        .fetch_add((computed - started).as_nanos() as u64, Ordering::Relaxed);
    (result, TaskTimes { started, computed })
}

/// What a window's run reads, for tests: each raw shard's identity,
/// samples and panes, the view store's identity, and how far each view of the groups
/// it reads is stored.
#[derive(Debug, PartialEq, Eq)]
struct TaskInputs {
    raw_shards: Vec<(usize, usize, usize)>,
    views: usize,
    read_views_stored_through: Vec<Option<i64>>,
}

impl TaskInputs {
    /// The inputs as of now, and the snapshots to read them again from.
    fn read(
        precompute: &Precompute,
        window: ClosedWindow,
        raw_data: &RawShards,
        views: &Arc<ViewStore>,
    ) -> (Self, RawShards, Arc<ViewStore>) {
        let groups = precompute.groups();
        let inputs = Self {
            raw_shards: raw_data
                .iter()
                // Samples and panes, not bytes: a chunk builds its series
                // identity column once, on first read, and counts it.
                .map(|shard| (Arc::as_ptr(shard) as usize, shard.len(), shard.panes()))
                .collect(),
            views: Arc::as_ptr(views) as usize,
            read_views_stored_through: precompute.tasks().reads[window.group]
                .iter()
                .flat_map(|&group| &groups[group].1)
                .map(|&view| views.stored_through(view))
                .collect(),
        };
        (inputs, raw_data.clone(), views.clone())
    }

    /// Every view of the groups the window reads is committed through the
    /// window's end.
    fn check(&self, window: ClosedWindow) -> Result<(), String> {
        if self
            .read_views_stored_through
            .iter()
            .all(|stored| stored.is_some_and(|end| end >= window.end_ms))
        {
            Ok(())
        } else {
            Err(format!(
                "window {window:?} ran before the views it reads were committed: {:?}",
                self.read_views_stored_through
            ))
        }
    }
}

/// The precompute committer: takes the jobs in the order the windows
/// closed, starts each window once the windows it reads are committed,
/// commits computed windows in window order per group, publishes the views
/// once per pass and then queues the windows for the persist threads, and
/// answers a barrier once every window before it is committed. Without
/// `workers` it computes each window itself.
#[allow(clippy::too_many_arguments)]
fn serve_precompute(
    precompute: &Precompute,
    mut views: Arc<ViewStore>,
    persist: Option<PersistPool>,
    inbox: Receiver<PoolEvent>,
    workers: Option<Sender<WindowRun>>,
    slots: &Slots,
    options: &PoolOptions,
    shared: &Shared,
) {
    // A window of a group split by series runs as one part per worker.
    let parts = if workers.is_some() {
        options.threads
    } else {
        1
    };
    let mut tasks = WindowTasks::new(precompute.tasks().clone(), parts as u32);
    let mut closed = false;
    while !(closed && tasks.is_empty()) {
        let Ok(event) = inbox.recv() else {
            break;
        };
        // Take every event already sent, so one pass commits and publishes
        // them together (as an ASAPQuery worker emits the windows one
        // flush closed as one batch: `precompute_engine/worker.rs`,
        // `flush_all`).
        let mut next = Some(event);
        while let Some(event) = next.take().or_else(|| inbox.try_recv().ok()) {
            match event {
                PoolEvent::Job(PrecomputeJob::Refresh {
                    window,
                    raw_data,
                    closed_at,
                }) => tasks.push(window, raw_data, closed_at),
                PoolEvent::Job(PrecomputeJob::Push { window, closed_at }) => {
                    tasks.push_window(window, closed_at, options.push_parts)
                }
                PoolEvent::Partial(partial) => tasks.partial(partial),
                PoolEvent::Job(PrecomputeJob::Barrier { barrier_ms, reply }) => {
                    tasks.barrier(barrier_ms, reply)
                }
                PoolEvent::Computed {
                    seq,
                    part,
                    result,
                    times,
                } => tasks.computed(seq, part, result, times),
                PoolEvent::Closed => closed = true,
            }
        }
        let mut to_persist = Vec::new();
        let mut committed = false;
        loop {
            // Started after every commit, so a window that reads another
            // group's views sees exactly the windows that closed before it.
            for (seq, window, raw_data, partition) in tasks.start() {
                match &workers {
                    Some(workers) => {
                        let run = WindowRun {
                            seq,
                            window,
                            raw_data,
                            views: views.clone(),
                            partition,
                        };
                        if workers.send(run).is_err() {
                            tracing::error!("the precompute workers have stopped");
                            return;
                        }
                    }
                    None => {
                        let (result, times) = compute_window(
                            precompute,
                            window,
                            raw_data,
                            views.clone(),
                            partition,
                            options,
                            shared,
                        );
                        tasks.computed(seq, 0, result, times);
                    }
                }
            }
            let Some(task) = tasks.next_commit() else {
                break;
            };
            commit_window(
                precompute,
                task,
                &mut views,
                &mut to_persist,
                options,
                shared,
            );
            committed = true;
            slots.release();
        }
        if committed {
            shared.published.publish_views(views.clone());
            shared
                .metrics
                .view_bytes
                .store(views.bytes() as u64, Ordering::Relaxed);
        }
        if let Some(persist) = &persist {
            for (group, range_ms, appended) in to_persist {
                let slots = appended
                    .iter()
                    .map(|(view, _)| views.slot(*view, range_ms.1))
                    .collect();
                persist.queue(group, range_ms, appended, slots, shared);
            }
        }
        for (barrier_ms, reply) in tasks.passed_barriers() {
            match &persist {
                // The barrier answers once the windows before it are also
                // written (not necessarily synced), without the committer
                // waiting for that.
                Some(persist) => persist.barrier(barrier_ms, reply),
                None => {
                    let _ = reply.send(barrier_ms);
                }
            }
        }
    }
    tracing::warn!("precompute stopped");
}

/// A window persisted after the views are published: its group, range
/// and each view's rows.
type ToPersist = (usize, (i64, i64), Vec<(PhysicalASAPNodeId, Vec<Batch>)>);

/// Stores a computed window's rows in `views` and counts it.
fn commit_window(
    precompute: &Precompute,
    task: CommitTask,
    views: &mut Arc<ViewStore>,
    to_persist: &mut Vec<ToPersist>,
    options: &PoolOptions,
    shared: &Shared,
) {
    let window = task.window;
    let result = {
        let _gate = shared
            .precompute_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let combine = |parts: Vec<Computed>| -> Result<Computed, String> {
            if task.pushed {
                let delay_ms: u64 = options
                    .slow_refreshes
                    .iter()
                    .filter(|s| s.matches(window))
                    .map(|s| s.delay_ms)
                    .sum();
                if delay_ms > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }
            }
            let timer = Timer::start();
            let combined = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if task.pushed && options.panic_in_combine_of_window_end_ms == Some(window.end_ms) {
                    panic!(
                        "combine of the window ending at {} panicked (test hook)",
                        window.end_ms
                    );
                }
                let mut parts = parts;
                match parts.len() {
                    1 => Ok(parts.remove(0)),
                    _ => precompute.merge(parts).map_err(|e| e.to_string()),
                }
            }))
            .unwrap_or_else(|_| Err("the combine panicked".to_string()))?;
            if let (true, Some(push)) = (task.pushed, &shared.metrics.push) {
                push.combine[window.group].record(timer);
            }
            Ok(if task.pushed {
                precompute.missing_only(window, combined, views)
            } else {
                combined
            })
        };
        let timer = Timer::start();
        let result = task.result.and_then(combine).and_then(|computed| {
            precompute
                .commit(window, computed, views)
                .map_err(|e| e.to_string())
        });
        if let (true, Some(push)) = (task.pushed, &shared.metrics.push) {
            push.commit.record(timer);
            push.commit_lag_ms.store(
                (crate::ingestion::wall_clock_ms() - window.end_ms).max(0),
                Ordering::Relaxed,
            );
        }
        result
    };
    let now = Instant::now();
    let latency = &shared.metrics.precompute_windows[window.group];
    latency.record(
        task.times.started.saturating_duration_since(task.closed_at),
        now.saturating_duration_since(task.times.computed),
        (crate::ingestion::wall_clock_ms() - window.end_ms).max(0),
    );
    {
        let mut stats = shared.stats.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(Refreshed::Ran(appended)) => {
                stats.windows_refreshed += 1;
                let start_ms = window.end_ms - precompute.width_ms(window.group);
                to_persist.push((window.group, (start_ms, window.end_ms), appended));
            }
            Ok(Refreshed::AlreadyStored) => stats.windows_already_stored += 1,
            Err(error) => {
                tracing::error!(group = window.group, end = window.end_ms, %error, "ingestion run failed");
                stats.windows_failed += 1;
            }
        }
    }
    shared.precompute_backlog.fetch_sub(1, Ordering::SeqCst);
}

/// A computed window ready to commit: its run, or its parts' runs by
/// part.
struct CommitTask {
    window: ClosedWindow,
    /// Built by push: its parts are the shards' partials.
    pushed: bool,
    result: Result<Vec<Computed>, String>,
    closed_at: Instant,
    times: TaskTimes,
}

/// Where a window taken from the backlog is.
enum TaskState {
    /// Waiting for the windows it reads to be committed.
    Waiting(RawShards),
    /// Computing: `parts` runs, those done so far by part, and when the
    /// first started and the last ended.
    Running {
        parts: u32,
        done: BTreeMap<u32, Result<Computed, String>>,
        times: Option<TaskTimes>,
    },
    Computed {
        result: Result<Vec<Computed>, String>,
        times: TaskTimes,
    },
}

struct WindowTask {
    window: ClosedWindow,
    pushed: bool,
    closed_at: Instant,
    state: TaskState,
}

/// The windows taken from the backlog and not yet committed, by the order
/// they closed (`seq`), and the barriers between them, scheduled by the
/// plan's [`RefreshTaskGraph`]. A window starts once every earlier window
/// of the groups it reads is committed (a superset of the windows covering
/// it, since a group is listed after the groups it reads and its windows
/// close no earlier than theirs), and, in a serial group, once the group's
/// earlier windows are committed; otherwise windows of different groups,
/// and of one group, compute at once. A group's windows commit in window
/// order (a reorder buffer per group).
struct WindowTasks {
    graph: RefreshTaskGraph,
    /// The parts a window of a group split by series runs as.
    parts: u32,
    tasks: BTreeMap<u64, WindowTask>,
    next_seq: u64,
    /// Each barrier, after the windows before `seq`.
    barriers: VecDeque<(u64, i64, oneshot::Sender<i64>)>,
    /// Partials of pushed windows whose job has not come yet.
    early: Vec<Partial>,
}

impl WindowTasks {
    fn new(graph: RefreshTaskGraph, parts: u32) -> Self {
        Self {
            graph,
            parts,
            tasks: BTreeMap::new(),
            next_seq: 0,
            barriers: VecDeque::new(),
            early: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.barriers.is_empty()
    }

    fn push(&mut self, window: ClosedWindow, raw_data: RawShards, closed_at: Instant) {
        self.tasks.insert(
            self.next_seq,
            WindowTask {
                window,
                pushed: false,
                closed_at,
                state: TaskState::Waiting(raw_data),
            },
        );
        self.next_seq += 1;
    }

    /// A pushed window: computing on the shards, one partial per shard.
    fn push_window(&mut self, window: ClosedWindow, closed_at: Instant, parts: u32) {
        let seq = self.next_seq;
        self.tasks.insert(
            seq,
            WindowTask {
                window,
                pushed: true,
                closed_at,
                state: TaskState::Running {
                    parts,
                    done: BTreeMap::new(),
                    times: None,
                },
            },
        );
        self.next_seq += 1;
        let early: Vec<Partial> = std::mem::take(&mut self.early);
        for partial in early {
            self.partial(partial);
        }
    }

    /// A shard's partial of a pushed window.
    fn partial(&mut self, partial: Partial) {
        let seq = self
            .tasks
            .iter()
            .find_map(|(&seq, task)| (task.pushed && task.window == partial.window).then_some(seq));
        match seq {
            Some(seq) => self.computed(
                seq,
                partial.shard,
                partial.result,
                TaskTimes {
                    started: partial.started,
                    computed: partial.sealed_at,
                },
            ),
            None => self.early.push(partial),
        }
    }

    fn barrier(&mut self, barrier_ms: i64, reply: oneshot::Sender<i64>) {
        self.barriers.push_back((self.next_seq, barrier_ms, reply));
    }

    /// The waiting windows that may start now: no earlier window of a
    /// group they read is still uncommitted. They are marked running; a
    /// window of a group split by series starts as one run per part.
    fn start(&mut self) -> Vec<(u64, ClosedWindow, RawShards, Option<SeriesPartition>)> {
        let mut uncommitted = BTreeSet::new();
        let mut started = Vec::new();
        for (&seq, task) in &mut self.tasks {
            let group = task.window.group;
            let waits_for = |g: &usize| uncommitted.contains(g);
            if matches!(task.state, TaskState::Waiting(_))
                && !self.graph.reads[group].iter().any(waits_for)
                && !(self.graph.serial[group] && waits_for(&group))
            {
                let parts = if self.graph.split_by_series[group] {
                    self.parts
                } else {
                    1
                };
                let running = TaskState::Running {
                    parts,
                    done: BTreeMap::new(),
                    times: None,
                };
                let TaskState::Waiting(raw_data) = std::mem::replace(&mut task.state, running)
                else {
                    unreachable!()
                };
                if parts == 1 {
                    started.push((seq, task.window, raw_data, None));
                } else {
                    for index in 0..parts {
                        let partition = SeriesPartition {
                            index,
                            count: parts,
                        };
                        started.push((seq, task.window, raw_data.clone(), Some(partition)));
                    }
                }
            }
            uncommitted.insert(group);
        }
        started
    }

    /// Part `part` of window `seq` is computed; the window is, once every
    /// part is.
    fn computed(
        &mut self,
        seq: u64,
        part: u32,
        result: Result<Computed, String>,
        times: TaskTimes,
    ) {
        let Some(task) = self.tasks.get_mut(&seq) else {
            return;
        };
        let TaskState::Running {
            parts,
            done,
            times: all,
        } = &mut task.state
        else {
            return;
        };
        done.insert(part, result);
        let all = all.get_or_insert(times);
        all.started = all.started.min(times.started);
        all.computed = all.computed.max(times.computed);
        if done.len() as u32 == *parts {
            let times = *all;
            let result = std::mem::take(done).into_values().collect();
            task.state = TaskState::Computed { result, times };
        }
    }

    /// The earliest computed window with no earlier window of its group
    /// still uncommitted, removed.
    fn next_commit(&mut self) -> Option<CommitTask> {
        let mut uncommitted = BTreeSet::new();
        let seq = self.tasks.iter().find_map(|(&seq, task)| {
            let ready = matches!(task.state, TaskState::Computed { .. })
                && !uncommitted.contains(&task.window.group);
            uncommitted.insert(task.window.group);
            ready.then_some(seq)
        })?;
        let task = self.tasks.remove(&seq)?;
        let TaskState::Computed { result, times } = task.state else {
            unreachable!()
        };
        Some(CommitTask {
            window: task.window,
            pushed: task.pushed,
            result,
            closed_at: task.closed_at,
            times,
        })
    }

    /// The barriers every window before which is committed, removed.
    fn passed_barriers(&mut self) -> Vec<(i64, oneshot::Sender<i64>)> {
        let first_uncommitted = self.tasks.keys().next().copied().unwrap_or(self.next_seq);
        let mut passed = Vec::new();
        while self
            .barriers
            .front()
            .is_some_and(|(seq, _, _)| *seq <= first_uncommitted)
        {
            let (_, barrier_ms, reply) = self.barriers.pop_front().unwrap();
            passed.push((barrier_ms, reply));
        }
        passed
    }
}

/// What a persist thread does next.
enum PersistJob {
    /// Write the rows each view got for its group's window `range_ms`;
    /// `bytes` of the queue it releases once written.
    Window {
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        /// Each view's stored window, told where it was written.
        slots: Vec<Option<Arc<WindowSlot>>>,
        bytes: u64,
    },
    /// Done once every window queued to this thread before is written.
    Barrier(Arc<PersistBarrier>),
}

/// An event-time barrier waiting for every persist thread.
struct PersistBarrier {
    barrier_ms: i64,
    remaining: AtomicU64,
    reply: Mutex<Option<oneshot::Sender<i64>>>,
}

impl PersistBarrier {
    /// One persist thread reached it; the last one answers.
    fn reached(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
            if let Some(reply) = self.reply.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = reply.send(self.barrier_ms);
            }
        }
    }
}

/// The persist threads, as the precompute thread sees them: refresh group
/// `g`'s windows go to thread `g % threads`, so each file has one writer
/// and its records stay in window order. The queue between precompute and
/// the threads is bounded by bytes.
struct PersistPool {
    shards: Vec<Sender<PersistJob>>,
    max_bytes: u64,
}

impl PersistPool {
    /// Queues a refreshed window for its group's persist thread, waiting
    /// while the queue holds more than `max_bytes` (the wait is exported;
    /// a window larger than the bound is still queued into an empty queue).
    fn queue(
        &self,
        group: usize,
        range_ms: (i64, i64),
        views: Vec<(PhysicalASAPNodeId, Vec<Batch>)>,
        slots: Vec<Option<Arc<WindowSlot>>>,
        shared: &Shared,
    ) {
        let bytes: u64 = views
            .iter()
            .flat_map(|(_, batches)| batches)
            .map(|batch| batch.memory_bytes() as u64)
            .sum();
        // The committer never waits for the disk: a slow disk leaves more
        // windows `Pending` (in memory, as without tiering) instead of
        // holding refreshes, the watermark and ingest. Windows queued past
        // the bound are counted.
        {
            let mut queued = shared
                .persist_queue
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *queued > 0 && *queued + bytes > self.max_bytes {
                shared
                    .metrics
                    .view_persist_over_bound
                    .fetch_add(1, Ordering::Relaxed);
            }
            *queued += bytes;
        }
        shared.persist_backlog.fetch_add(1, Ordering::SeqCst);
        let job = PersistJob::Window {
            group,
            range_ms,
            views,
            slots,
            bytes,
        };
        if self.shards[group % self.shards.len()].send(job).is_err() {
            tracing::error!(group, "the persist thread has stopped");
            release_persist_queue(shared, bytes);
        }
    }

    /// Answers `reply` once every persist thread has written the windows
    /// queued before.
    fn barrier(&self, barrier_ms: i64, reply: oneshot::Sender<i64>) {
        let barrier = Arc::new(PersistBarrier {
            barrier_ms,
            remaining: AtomicU64::new(self.shards.len() as u64),
            reply: Mutex::new(Some(reply)),
        });
        for shard in &self.shards {
            if shard.send(PersistJob::Barrier(barrier.clone())).is_err() {
                barrier.reached();
            }
        }
    }
}

/// Releases `bytes` of the persist queue for a window written (or lost).
fn release_persist_queue(shared: &Shared, bytes: u64) {
    let mut queued = shared
        .persist_queue
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    *queued = queued.saturating_sub(bytes);
    shared.persist_backlog.fetch_sub(1, Ordering::SeqCst);
}

/// The persist thread: writes each refreshed window's records to the view
/// files, in order, and syncs them every sync interval, also when no window
/// arrives; syncs everything when the precompute thread stops.
fn serve_persist(
    mut files: ViewFiles,
    jobs: Receiver<PersistJob>,
    shared: &Shared,
    delay: std::time::Duration,
    unpersistable: &BTreeSet<PhysicalASAPNodeId>,
) {
    let metrics = &shared.metrics;
    let wait = files.sync_interval().clamp(
        std::time::Duration::from_millis(1),
        std::time::Duration::from_secs(3600),
    );
    loop {
        let job = match jobs.recv_timeout(wait) {
            Ok(job) => job,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                match files.sync_if_due() {
                    Ok(ns) => metrics.view_sync_ns.fetch_add(ns, Ordering::Relaxed),
                    Err(error) => {
                        tracing::error!(%error, "syncing the view files failed");
                        0
                    }
                };
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match job {
            PersistJob::Window {
                group,
                range_ms,
                views,
                slots,
                bytes,
            } => {
                // A slow disk, for tests.
                if !delay.is_zero() {
                    std::thread::sleep(delay);
                }
                let records: Vec<(PhysicalASAPNodeId, &[Batch])> = views
                    .iter()
                    .filter(|(view, _)| !unpersistable.contains(view))
                    .map(|(view, batches)| (*view, batches.as_slice()))
                    .collect();
                let skipped = (views.len() - records.len()) as u64;
                let files_before = files.bytes();
                let locations = match files.append(group, range_ms, &records) {
                    Ok(written) => {
                        metrics
                            .view_persisted_bytes
                            .fetch_add(written.bytes, Ordering::Relaxed);
                        metrics
                            .view_sync_ns
                            .fetch_add(written.sync_ns, Ordering::Relaxed);
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(written.not_persisted + skipped, Ordering::Relaxed);
                        metrics
                            .view_segments_marked_cold
                            .fetch_add(written.segments_marked_cold, Ordering::Relaxed);
                        written.locations
                    }
                    Err(error) => {
                        tracing::error!(group, ?range_ms, %error, "persisting a window failed");
                        metrics
                            .view_windows_not_persisted
                            .fetch_add(views.len() as u64, Ordering::Relaxed);
                        Vec::new()
                    }
                };
                // Each written window leaves memory (unless the cache keeps
                // it); one that was not written stays.
                for ((view, _), slot) in views.iter().zip(&slots) {
                    let Some(slot) = slot else { continue };
                    match locations.iter().find(|(v, _)| v == view) {
                        Some((_, location)) => slot.stored(location.clone()),
                        None => slot.pin(),
                    }
                }
                // Each view's windows expire at its own retention (the
                // files recorded it when indexing).
                if let Err(error) = files.delete_expired(group, range_ms.1) {
                    tracing::error!(group, %error, "deleting expired view segments failed");
                }
                let files_after = files.bytes();
                if files_after >= files_before {
                    metrics
                        .view_file_bytes
                        .fetch_add(files_after - files_before, Ordering::Relaxed);
                } else {
                    metrics
                        .view_file_bytes
                        .fetch_sub(files_before - files_after, Ordering::Relaxed);
                }
                release_persist_queue(shared, bytes);
            }
            PersistJob::Barrier(barrier) => barrier.reached(),
        }
    }
    match files.sync() {
        Ok(ns) => {
            metrics.view_sync_ns.fetch_add(ns, Ordering::Relaxed);
        }
        Err(error) => tracing::error!(%error, "syncing the view files failed"),
    }
    tracing::info!("persist stopped");
}

/// The cold tier of a data plane persisting its views: the window cache
/// with its budget (the option, else from the cgroup's `memory.max`, else
/// none) and idle time.
fn view_tier(options: &EngineOptions) -> ViewTier {
    let cgroup = storage::memory_budget::own_cgroup()
        .and_then(|dir| storage::memory_budget::read_cgroup(&dir).ok());
    let other_pools = (options.result_cache_max_bytes + options.query_time_results_max_bytes)
        as u64
        + options.max_persist_backlog_bytes;
    let budget = options.view_cache_max_bytes.or_else(|| {
        cgroup.as_ref().and_then(|cgroup| {
            storage::memory_budget::default_view_cache_budget(cgroup, other_pools)
        })
    });
    let idle_ms = options
        .view_cache_idle_ms
        .unwrap_or(DEFAULT_VIEW_CACHE_IDLE_MS);
    tracing::info!(?budget, idle_ms, "view window cache");
    ViewTier::new(
        WindowCache::new(
            budget,
            (idle_ms > 0).then(|| std::time::Duration::from_millis(idle_ms)),
        ),
        options.view_read,
        std::time::Duration::from_millis(options.cold_decode_delay_ms),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A request is admitted while the queue stays within its bound, and
    /// always into an empty queue; otherwise it is `Busy`.
    #[test]
    fn admission() {
        assert_eq!(admit(0, 3, 2), Ok(3));
        assert_eq!(admit(1, 1, 2), Ok(2));
        assert_eq!(admit(1, 2, 2), Err(IngestError::Busy));
        assert_eq!(admit(u64::MAX, 1, u64::MAX), Err(IngestError::Busy));
    }

    /// A request reaching a full shard is refused whole: no other shard's
    /// queue takes its samples.
    #[test]
    fn a_full_shard_refuses_the_whole_request() {
        let queued: Vec<AtomicU64> = [0, 5, 0].into_iter().map(AtomicU64::new).collect();
        assert_eq!(admit_shards(&queued, &[2, 3, 1], 6), Err(IngestError::Busy));
        let now: Vec<u64> = queued.iter().map(|q| q.load(Ordering::SeqCst)).collect();
        assert_eq!(now, [0, 5, 0]);
        assert_eq!(admit_shards(&queued, &[2, 1, 1], 6), Ok(()));
        let now: Vec<u64> = queued.iter().map(|q| q.load(Ordering::SeqCst)).collect();
        assert_eq!(now, [2, 6, 1]);
    }

    fn window(group: usize, end_ms: i64) -> ClosedWindow {
        ClosedWindow { group, end_ms }
    }

    fn times() -> TaskTimes {
        TaskTimes {
            started: Instant::now(),
            computed: Instant::now(),
        }
    }

    /// The tasks of `graph` with `windows` pushed in order.
    fn tasks(graph: RefreshTaskGraph, windows: &[ClosedWindow]) -> WindowTasks {
        let mut tasks = WindowTasks::new(graph, 1);
        for &w in windows {
            tasks.push(w, Arc::new(Vec::new()), Instant::now());
        }
        tasks
    }

    fn started(tasks: &mut WindowTasks) -> Vec<u64> {
        tasks
            .start()
            .into_iter()
            .map(|(seq, _, _, _)| seq)
            .collect()
    }

    /// A group's windows all start at once; one finishing before an
    /// earlier one waits for it and the two commit in window order.
    #[test]
    fn windows_commit_in_window_order_when_computed_out_of_order() {
        let graph = RefreshTaskGraph {
            reads: vec![vec![]],
            serial: vec![false],
            split_by_series: vec![false],
        };
        let mut tasks = tasks(graph, &[window(0, 10), window(0, 20)]);
        assert_eq!(started(&mut tasks), [0, 1]);
        tasks.computed(1, 0, Ok(Computed::AlreadyStored), times());
        assert!(tasks.next_commit().is_none());
        tasks.computed(0, 0, Ok(Computed::AlreadyStored), times());
        let ends: Vec<i64> = std::iter::from_fn(|| tasks.next_commit())
            .map(|t| t.window.end_ms)
            .collect();
        assert_eq!(ends, [10, 20]);
    }

    /// A window of a group reading another group's views starts once the
    /// earlier windows of that group are committed; an unrelated group's
    /// window starts at once.
    #[test]
    fn a_window_waits_for_the_groups_it_reads() {
        let graph = RefreshTaskGraph {
            reads: vec![vec![], vec![0], vec![]],
            serial: vec![false; 3],
            split_by_series: vec![false; 3],
        };
        let mut tasks = tasks(graph, &[window(0, 10), window(1, 10), window(2, 10)]);
        assert_eq!(started(&mut tasks), [0, 2]);
        tasks.computed(0, 0, Ok(Computed::AlreadyStored), times());
        assert!(started(&mut tasks).is_empty());
        assert_eq!(tasks.next_commit().unwrap().window, window(0, 10));
        assert_eq!(started(&mut tasks), [1]);
    }

    /// A serial group's windows start one at a time.
    #[test]
    fn a_serial_group_computes_one_window_at_a_time() {
        let graph = RefreshTaskGraph {
            reads: vec![vec![]],
            serial: vec![true],
            split_by_series: vec![false],
        };
        let mut tasks = tasks(graph, &[window(0, 10), window(0, 20)]);
        assert_eq!(started(&mut tasks), [0]);
        tasks.computed(0, 0, Ok(Computed::AlreadyStored), times());
        assert!(started(&mut tasks).is_empty());
        tasks.next_commit().unwrap();
        assert_eq!(started(&mut tasks), [1]);
    }

    /// A barrier passes once every window queued before it is committed,
    /// not waiting for windows queued after it.
    #[test]
    fn a_barrier_passes_once_the_windows_before_it_are_committed() {
        let graph = RefreshTaskGraph {
            reads: vec![vec![], vec![]],
            serial: vec![false; 2],
            split_by_series: vec![false; 2],
        };
        let mut tasks = tasks(graph, &[window(0, 10)]);
        let (reply, _answer) = oneshot::channel();
        tasks.barrier(10, reply);
        tasks.push(window(1, 20), Arc::new(Vec::new()), Instant::now());
        assert_eq!(started(&mut tasks), [0, 1]);
        assert!(tasks.passed_barriers().is_empty());
        tasks.computed(0, 0, Ok(Computed::AlreadyStored), times());
        tasks.next_commit().unwrap();
        assert_eq!(tasks.passed_barriers().len(), 1);
        assert!(!tasks.is_empty());
    }

    /// A window of a group split by series starts as one run per part and
    /// is committed once every part is computed, with the parts in order.
    #[test]
    fn a_split_window_commits_once_every_part_is_computed() {
        let graph = RefreshTaskGraph {
            reads: vec![vec![]],
            serial: vec![false],
            split_by_series: vec![true],
        };
        let mut tasks = WindowTasks::new(graph, 3);
        tasks.push(window(0, 10), Arc::new(Vec::new()), Instant::now());
        let parts: Vec<Option<SeriesPartition>> =
            tasks.start().into_iter().map(|(_, _, _, p)| p).collect();
        assert_eq!(
            parts,
            (0..3)
                .map(|index| Some(SeriesPartition { index, count: 3 }))
                .collect::<Vec<_>>()
        );
        for part in [2, 0] {
            tasks.computed(0, part, Ok(Computed::AlreadyStored), times());
            assert!(tasks.next_commit().is_none());
        }
        tasks.computed(0, 1, Ok(Computed::AlreadyStored), times());
        assert_eq!(tasks.next_commit().unwrap().result.unwrap().len(), 3);
    }

    /// The running flag clears when an engine thread panics.
    #[test]
    fn running_guard_clears_on_panic() {
        let running = Arc::new(AtomicBool::new(true));
        let guard = RunningGuard(running.clone());
        let thread = std::thread::spawn(move || {
            let _guard = guard;
            panic!("engine bug");
        });
        assert!(thread.join().is_err());
        assert!(!running.load(Ordering::SeqCst));
    }
    /// A metric read by a pushed 60 s group and a window-path 10 s group:
    /// once the 10 s group closed (10, 20], a sample at 15 s is still in
    /// the pushed group's open window (0, 60], so it is not late.
    #[test]
    fn a_pushed_groups_open_window_keeps_samples_another_group_closed() {
        let workload = serde_json::from_value(serde_json::json!({
            "query_workload": {"language": "promql", "repeating_queries": [
                {
                    "query": "sum_over_time(m_engine_push_late[60s])",
                    "demand": {"fixed_interval_at": {"interval": 60000, "evaluation_phase": 0}},
                    "requirements": {"accuracy": {"max_error": 0.05, "failure_probability": 0.01,
                        "accuracy_scope": "whole_result"}},
                    "predictability": {"predictable": {"known_at": null}},
                },
                {
                    "query": "sum by (job) (sum_over_time(m_engine_push_late[10s]))",
                    "demand": {"fixed_interval_at": {"interval": 10000, "evaluation_phase": 0}},
                    "requirements": {},
                    "predictability": {"predictable": {"known_at": null}},
                },
            ]},
            "data_workload": {"arrival": {"ingesting": {"rows_per_second": 1.0}},
              "sources": {"m_engine_push_late": {"series_count": 1, "sample_interval": 1000,
                  "metric_type": "gauge", "column_signs": {"value": "non_negative"}}}},
        }))
        .unwrap();
        let mut inputs = control_plane::DeploymentInputs::default();
        inputs.deployment_capabilities.raw_data_retained = false;
        let plan = control_plane::plan_workload(&workload, &inputs).unwrap().0;
        let options = EngineOptions {
            no_forward: true,
            ingestion: crate::ingestion::IngestionOptions {
                allowed_lateness_ms: 0,
                stream_start_ms: Some(0),
                ..Default::default()
            },
            ..EngineOptions::default()
        };
        let (mut ingestion, ..) =
            install(&serde_json::to_string(&plan).unwrap(), &options).unwrap();
        let widths: Vec<i64> = ingestion.group_windows().iter().map(|w| w.0).collect();
        assert_eq!(widths, [60_000, 10_000]);
        ingestion.set_push_groups(vec![true, false]);
        let series = storage::series_ids::intern_pairs(&[("__name__", "m_engine_push_late")]);
        ingestion.observe(&[(series.metric.clone(), 1_000)]);
        ingestion.advance_newest_sample(Some(25_000));
        ingestion.advance_clock(25_000);
        let closed: Vec<(usize, i64)> = ingestion
            .close()
            .iter()
            .map(|w| (w.group, w.end_ms))
            .collect();
        assert_eq!(closed, [(0, 0), (1, 0), (1, 10_000), (1, 20_000)]);
        let sample = RawSample {
            labels: series,
            timestamp_ms: 15_000,
            value: 1.0,
        };
        let routed = ingestion.routing().route(vec![sample], 25_000);
        assert_eq!(routed.accepted.len(), 1, "{:?}", routed.stats);
    }
}
