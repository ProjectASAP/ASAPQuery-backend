//! The engine: the precompute and query engines, wired to one published
//! store, with the plan file installed into them
//! ([`crate::installer::PlanInstaller`]), and the dispatcher the HTTP
//! routes run queries through.

use std::sync::Arc;

use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_planner::InstallablePlan;
use internal_metrics::{Metrics, QueryMetrics};
use precompute::engine::{
    IngestError, InstalledPrecompute, PersistedViews, PrecomputeEngine, PrecomputeMode,
    PrecomputeOptions, SlowRefresh,
};
use precompute::ingestion::{IngestionOptions, IngestionStats};
use query::workers::QueryWorkers;
use query::{ExecutorQueryEngine, QueryOptions, Unanswered};
use storage::persist::{plan_hash, ViewRead};
use storage::persist_pool::PersistOptions;
use storage::store::{PublishedStore, QueryTimeResults, RawSample, ViewTier};
use storage::window_cache::WindowCache;

use crate::dispatcher::QueryDispatcher;
use crate::installer::PlanInstaller;
use crate::metrics_endpoint::Gauges;

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
    /// ([`query::result_cache`]), so a range request recomputes only the
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
    pub push_batching: precompute::push::MorselBatching,
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
            push_batching: precompute::push::MorselBatching::default(),
            panic_in_combine_of_window_end_ms: None,
        }
    }
}

/// The HTTP side of the engine.
#[derive(Clone)]
pub struct EngineHandle {
    precompute: InstalledPrecompute,
    query: QueryWorkers,
    dispatcher: Arc<QueryDispatcher>,
    /// The views' cold tier, when they are persisted.
    view_tier: Option<Arc<ViewTier>>,
}

impl EngineHandle {
    /// Starts the engine for the plan file `plan_json`, read, checked and
    /// compiled once, and returns the plan's queries. Fails if the plan is
    /// invalid or the data plane cannot execute it.
    pub fn spawn(plan_json: String, options: EngineOptions) -> anyhow::Result<(Self, Vec<String>)> {
        let plan = InstallablePlan::from_json(&plan_json)?;
        let query_texts = plan.query_texts.clone();
        let view_tier = options
            .view_dir
            .is_some()
            .then(|| Arc::new(view_tier(&options)));
        let persisted = options
            .view_dir
            .clone()
            .zip(view_tier.clone())
            .map(|(dir, tier)| PersistedViews {
                dir,
                plan_hash: plan_hash(plan_json.as_bytes()),
                sync_interval: std::time::Duration::from_millis(options.view_sync_interval_ms),
                segment_ms: options.view_segment_ms,
                persist: PersistOptions {
                    threads: options.persist_threads,
                    max_backlog_bytes: options.max_persist_backlog_bytes,
                    delay: std::time::Duration::from_millis(options.persist_delay_ms),
                    unpersistable_views: options.unpersistable_views.iter().copied().collect(),
                },
                tier,
            });
        let published = Arc::new(PublishedStore::new(Arc::default()));
        let precompute = Arc::new(PrecomputeEngine::new(
            options.precompute(),
            persisted,
            published.clone(),
        ));
        let query = Arc::new(ExecutorQueryEngine::new(options.query(), published));
        let dispatcher = Arc::new(QueryDispatcher::new(query.clone()));
        PlanInstaller::new(precompute.clone(), query.clone(), dispatcher.clone()).install(plan)?;
        let handle = Self {
            precompute: precompute.installed().expect("installed").clone(),
            query: query.installed().expect("installed").clone(),
            dispatcher,
            view_tier,
        };
        Ok((handle, query_texts))
    }

    /// Queues `samples` for ingestion; see [`InstalledPrecompute::try_ingest`].
    pub fn try_ingest(&self, samples: Vec<RawSample>) -> Result<(), IngestError> {
        self.precompute.try_ingest(samples)
    }

    /// What the HTTP routes run queries through.
    pub fn dispatcher(&self) -> &QueryDispatcher {
        &self.dispatcher
    }

    /// The newest published store: what a query starting now reads.
    pub fn snapshot(&self) -> Arc<query::Snapshot> {
        self.query.snapshot()
    }

    /// Each planned query's measurements, by query text.
    pub fn query_metrics(&self) -> &[(String, QueryMetrics)] {
        self.query.compiled().metrics()
    }

    /// How many query-time `OneEvaluation` nodes several queries share.
    pub fn shared_nodes(&self) -> usize {
        self.query.compiled().shared_nodes().len()
    }

    /// The query-time partition of the view store.
    pub fn query_time_results(&self) -> &QueryTimeResults<Unanswered> {
        self.query.compiled().query_time_results()
    }

    /// The engine's measurements.
    pub fn metrics(&self) -> &Metrics {
        self.precompute.metrics()
    }

    /// The engine's measurements and counters in the Prometheus text
    /// format.
    pub fn render_metrics(&self) -> String {
        let persist_backlog = self.precompute.persist_backlog();
        let compiled = self.query.compiled();
        crate::metrics_endpoint::render(
            self.metrics(),
            &Gauges {
                stats: self.stats(),
                samples_queued: self.queued_samples(),
                precompute_backlog: self.precompute_backlog(),
                persist_backlog: persist_backlog.windows(),
                persist_backlog_bytes: persist_backlog.bytes(),
                now_ms: precompute::ingestion::wall_clock_ms(),
                queries: compiled.metrics(),
                query_time_results: compiled.query_time_results(),
                result_cache: compiled.result_cache(),
                view_tier: self.view_tier.as_deref(),
            },
        )
    }

    /// Whether the ingest and precompute threads are still running.
    pub fn is_running(&self) -> bool {
        self.precompute.is_running()
    }

    /// Samples queued and not yet ingested.
    pub fn queued_samples(&self) -> u64 {
        self.precompute.queued_samples()
    }

    /// Refresh windows closed and not yet refreshed.
    pub fn precompute_backlog(&self) -> u64 {
        self.precompute.precompute_backlog()
    }

    /// The ingestion counters.
    pub fn stats(&self) -> IngestionStats {
        self.precompute.stats()
    }

    /// Queues an event-time barrier and waits for it; see
    /// [`InstalledPrecompute::advance_watermark`].
    pub async fn advance_watermark(&self, barrier_ms: i64) -> Result<i64, IngestError> {
        self.precompute.advance_watermark(barrier_ms).await
    }

    /// For tests: stops ingestion; see [`InstalledPrecompute::stop`].
    #[doc(hidden)]
    pub fn stop(&self) {
        self.precompute.stop()
    }

    /// For tests: no refresh finishes while the guard is held.
    #[doc(hidden)]
    pub fn hold_precompute(&self) -> std::sync::MutexGuard<'_, ()> {
        self.precompute.hold_precompute()
    }
}

impl EngineOptions {
    /// The options of the precompute engine.
    fn precompute(&self) -> PrecomputeOptions {
        PrecomputeOptions {
            ingestion: self.ingestion,
            run_memory_limit_bytes: self.run_memory_limit_bytes,
            max_queued_samples: self.max_queued_samples,
            max_precompute_backlog: self.max_precompute_backlog,
            ingest_threads: self.ingest_threads,
            precompute_threads: self.precompute_threads,
            slow_refreshes: self.slow_refreshes.clone(),
            check_task_inputs: self.check_task_inputs,
            shard_delay_ms: self.shard_delay_ms,
            precompute_mode: self.precompute_mode,
            push_batching: self.push_batching,
            precompute_memory_budget_bytes: self.precompute_memory_budget_bytes,
            panic_in_combine_of_window_end_ms: self.panic_in_combine_of_window_end_ms,
        }
    }

    /// The options of the query engine.
    fn query(&self) -> QueryOptions {
        QueryOptions {
            run_memory_limit_bytes: self.run_memory_limit_bytes,
            threads: self.query_threads,
            share_one_evaluation_results: self.share_one_evaluation_results,
            query_time_results_retention_ms: self.query_time_results_retention_ms,
            query_time_results_max_bytes: self.query_time_results_max_bytes,
            result_cache: self.result_cache,
            result_cache_max_bytes: self.result_cache_max_bytes,
            view_retention_margin_ms: self.ingestion.view_retention_margin_ms,
            refuse_unexecutable: self.no_forward,
        }
    }
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
