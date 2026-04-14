use crate::data_model::{
    AggregateCore, AggregationType, CleanupPolicy, KeyByLabelValues, PrecomputedOutput,
    StreamingConfig,
};
use crate::engines::physical::accumulator_serde;
use crate::stores::simple_map_store::common::{
    EpochID, InternTable, MetricBucketMap, MetricID, MutableEpoch, SealedEpoch, TimestampRange,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use dashmap::DashMap;
use datafusion_summary_library::SketchType;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::persistence::{
    self,
    cache::PartCache,
    flusher::FlusherHandle,
    manifest::Manifest,
    recovery,
    source::{EpochSnapshot, EpochSnapshotEntry, EpochSource, SealedEpochRef},
    PersistError, PersistResult, SimpleMapStorePersistenceConfig,
};

type StoreKey = u64; // aggregation_id

/// Sum the `AggregateCore::approx_memory_bytes()` of every entry in a
/// sealed epoch. Cheap — each impl is supposed to be O(1) or at worst
/// O(entries_inside_the_sketch), and this is only called at rotate +
/// evict time, not on the insert hot path.
fn epoch_approx_bytes(epoch: &SealedEpoch) -> usize {
    epoch
        .entries
        .iter()
        .map(|(_, _, agg)| agg.approx_memory_bytes())
        .sum()
}

/// Fallback epoch capacity used when `num_aggregates_to_retain` is not
/// set in the streaming config but persistence is enabled. Without it
/// the rotator never seals the current epoch and nothing is ever
/// flushable.
const PERSISTENCE_DEFAULT_EPOCH_CAPACITY: usize = 1024;

/// Maximum time an insert batch will wait on the flusher's pressure
/// condvar when `mem_bytes_sealed` has exceeded `hard_cap_bytes`. 30
/// seconds is long enough that a healthy flusher always makes it
/// through a single tick, but short enough that a stuck flusher
/// degrades gracefully (logged warning + continued insert) rather
/// than hanging the ingest thread forever.
const INSERT_BACK_PRESSURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-aggregation_id data protected by RwLock
struct StoreKeyData {
    /// Label interning table (Optimization 1)
    intern: InternTable,

    /// Active epoch — always present, accepts inserts.
    current_epoch: MutableEpoch,

    /// Sealed (immutable) epochs stored as flat sorted Vecs (Optimization 2).
    sealed_epochs: BTreeMap<EpochID, SealedEpoch>,

    /// Monotonically increasing ID of the current epoch.
    current_epoch_id: EpochID,

    /// Max distinct time-windows per epoch before sealing.
    /// None = unlimited (set on first insert from num_aggregates_to_retain).
    epoch_capacity: Option<usize>,

    /// Max total epochs (1 current + sealed) to retain before dropping the oldest.
    max_epochs: usize,

    /// Track how many times each timestamp range has been read.
    /// Behind Mutex so range queries can use a read lock on the outer RwLock.
    read_counts: Mutex<HashMap<TimestampRange, u64>>,
}

impl StoreKeyData {
    fn new() -> Self {
        Self {
            intern: InternTable::new(),
            current_epoch: MutableEpoch::new(),
            sealed_epochs: BTreeMap::new(),
            current_epoch_id: 0,
            epoch_capacity: None,
            max_epochs: 4,
            read_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Set epoch_capacity on first insert (no-op after first call).
    fn configure_epochs(&mut self, num_aggregates_to_retain: Option<u64>) {
        if self.epoch_capacity.is_none() {
            if let Some(cap) = num_aggregates_to_retain {
                self.epoch_capacity = Some(cap as usize);
            }
        }
    }

    /// Seal the current epoch when full, then (only if retention-based
    /// cleanup is on) evict the minimum number of oldest windows to
    /// keep total distinct windows ≤ `epoch_capacity * max_epochs`.
    ///
    /// When `persistence_enabled` is true, the eviction step is
    /// **skipped** — the background flusher handles eviction via its
    /// memory/time triggers. This method only seals in that case, and
    /// sealed epochs accumulate until the flusher picks them up.
    fn maybe_rotate_epoch(&mut self, persistence_enabled: bool) {
        let capacity = match self.epoch_capacity {
            Some(c) if c > 0 => c,
            _ => return, // unlimited
        };

        // Step 1: seal current epoch if it has hit the window capacity threshold.
        if self.current_epoch.window_count() >= capacity {
            let hint = self.current_epoch.len();
            let old = std::mem::replace(&mut self.current_epoch, MutableEpoch::with_capacity(hint));
            self.sealed_epochs.insert(self.current_epoch_id, old.seal());
            self.current_epoch_id += 1;
        }

        if persistence_enabled {
            // Flusher evicts. We only seal.
            return;
        }

        // Step 2: evict oldest windows until total distinct windows ≤ retention_limit.
        let retention_limit = capacity * self.max_epochs;
        let total: usize = self.current_epoch.window_count()
            + self
                .sealed_epochs
                .values()
                .map(|e| e.distinct_window_count())
                .sum::<usize>();

        if total <= retention_limit {
            return;
        }
        let mut over = total - retention_limit;

        while over > 0 {
            let oldest_id = match self.sealed_epochs.keys().next().copied() {
                Some(id) => id,
                None => break,
            };
            let oldest_windows = self.sealed_epochs[&oldest_id].unique_windows();
            let n_evict = over.min(oldest_windows.len());
            let to_remove = oldest_windows[..n_evict].to_vec();
            over -= n_evict;

            {
                let read_counts = self.read_counts.get_mut().unwrap();
                for w in &to_remove {
                    read_counts.remove(w);
                }
            }
            if n_evict == oldest_windows.len() {
                self.sealed_epochs.remove(&oldest_id);
            } else {
                self.sealed_epochs
                    .get_mut(&oldest_id)
                    .unwrap()
                    .remove_windows(&to_remove);
            }
        }
    }

    /// Apply ReadBased cleanup across current and sealed epochs.
    fn cleanup_read_based(&mut self, metric: &str, aggregation_id: u64, threshold: u64) {
        let read_counts = self.read_counts.get_mut().unwrap();

        let windows_to_remove: Vec<TimestampRange> = read_counts
            .iter()
            .filter(|(_, &count)| count >= threshold)
            .map(|(range, _)| *range)
            .collect();

        if windows_to_remove.is_empty() {
            return;
        }

        for window in &windows_to_remove {
            debug!(
                "Removed aggregate for {} aggregation_id {} window {}-{} (read_count >= threshold: {})",
                metric, aggregation_id, window.0, window.1, threshold
            );
            read_counts.remove(window);
        }

        // Remove from current epoch.
        self.current_epoch.remove_windows(&windows_to_remove);

        // Remove from sealed epochs; drop any that become empty.
        self.sealed_epochs.retain(|_, epoch| {
            epoch.remove_windows(&windows_to_remove);
            !epoch.is_empty()
        });
    }
}

/// Shared state that both the outer `SimpleMapStorePerKey` and the
/// background flusher hold via `Arc`. Contains the DashMap of per-agg
/// state plus the counters the flusher needs.
pub struct PerKeyInner {
    // Lock-free concurrent outer map - per aggregation_id
    store: DashMap<StoreKey, Arc<RwLock<StoreKeyData>>>,

    // Separate concurrent maps for global state
    earliest_timestamps: DashMap<u64, AtomicU64>,
    metrics: DashMap<String, ()>, // HashSet equivalent
    items_inserted: DashMap<String, AtomicU64>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,

    /// Whether persistence is active. When `true`:
    /// * `maybe_rotate_epoch` still seals current epochs but does not
    ///   evict old ones — the flusher handles eviction.
    /// * Insertions always run rotation regardless of cleanup policy.
    /// * `mem_bytes_sealed` is maintained as flusher input.
    persistence_enabled: bool,

    /// Approximate sum of sketch bytes across all sealed epochs
    /// currently held in memory. Incremented on insert by the sum of
    /// each item's `AggregateCore::approx_memory_bytes()`, decremented
    /// on evict by the same. Drives the flusher's memory-pressure
    /// trigger. Approximate — per-type estimates are not guaranteed
    /// accurate, only proportional.
    mem_bytes_sealed: AtomicUsize,

    /// Hard memory ceiling. When `mem_bytes_sealed` reaches this,
    /// `insert_precomputed_output_batch` blocks on the flusher's
    /// `pressure_cv` until the flusher catches up — the v1
    /// mechanism for bounding RAM under sustained overload. Set to
    /// `usize::MAX` by the in-memory-only constructor (no blocking).
    hard_cap_bytes: usize,
}

/// Persistence-related state owned by the outer store. Dropping this
/// (via `SimpleMapStorePerKey::Drop`) shuts the flusher down cleanly.
struct PersistenceState {
    manifest: Arc<Manifest>,
    cache: PartCache,
    /// Owned by the store. Dropping it shuts the thread down; the
    /// insert path also calls `wait_for_memory_under` on it when the
    /// hard cap is hit.
    flusher: FlusherHandle,
    #[allow(dead_code)]
    parts_root: PathBuf,
}

/// In-memory storage implementation using per-key locks for concurrency
pub struct SimpleMapStorePerKey {
    inner: Arc<PerKeyInner>,
    /// `None` when the store is in-memory-only (existing `new()` path).
    /// `Some` when constructed via `with_persistence`.
    persistence: Option<PersistenceState>,
}

impl SimpleMapStorePerKey {
    /// Backwards-compatible constructor. No persistence — behaves
    /// exactly like pre-persistence code.
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            inner: Arc::new(PerKeyInner {
                store: DashMap::new(),
                earliest_timestamps: DashMap::new(),
                metrics: DashMap::new(),
                items_inserted: DashMap::new(),
                streaming_config,
                cleanup_policy,
                persistence_enabled: false,
                mem_bytes_sealed: AtomicUsize::new(0),
                hard_cap_bytes: usize::MAX,
            }),
            persistence: None,
        }
    }

    /// Persistence-aware constructor. Starts the background flusher,
    /// runs recovery on `persistence_cfg.disk_path`, and returns a
    /// store that writes sealed epochs to disk and evicts them on
    /// memory / time pressure.
    ///
    /// Cleanup policy still applies, but when persistence is on, the
    /// destructive eviction step of `CircularBuffer` / `ReadBased` is
    /// bypassed in favor of the flusher. `NoCleanup` + persistence is
    /// the typical production configuration: the flusher bounds RAM,
    /// nothing is ever dropped from memory without first being on
    /// disk.
    pub fn with_persistence(
        streaming_config: Arc<StreamingConfig>,
        cleanup_policy: CleanupPolicy,
        persistence_cfg: SimpleMapStorePersistenceConfig,
    ) -> PersistResult<Self> {
        // Run recovery first so the manifest reflects on-disk state.
        let (_loaded_manifest, report) = recovery::recover(&persistence_cfg.disk_path)?;
        info!(
            "SimpleMapStorePerKey persistence recovery: live={}, corrupt_removed={}, orphans_removed={}",
            report.live_parts, report.corrupt_parts_removed, report.orphan_parts_removed
        );

        // Re-open manifest as an Arc so the flusher and the query path
        // can share it. (`recovery::recover` already opened one, but
        // it's owned; simpler to re-open once ownership settled.)
        let manifest = Arc::new(Manifest::open_or_init(&persistence_cfg.disk_path)?);

        let parts_root = persistence::flusher::parts_root(&persistence_cfg.disk_path);
        let cache = PartCache::new(parts_root.clone(), persistence_cfg.part_cache_bytes);

        // Capture hard_cap before `persistence_cfg` is moved into the
        // flusher; PerKeyInner needs it for back-pressure enforcement
        // on the insert path.
        let hard_cap_bytes = persistence_cfg.hard_cap_bytes;

        let inner = Arc::new(PerKeyInner {
            store: DashMap::new(),
            earliest_timestamps: DashMap::new(),
            metrics: DashMap::new(),
            items_inserted: DashMap::new(),
            streaming_config,
            cleanup_policy,
            persistence_enabled: true,
            mem_bytes_sealed: AtomicUsize::new(0),
            hard_cap_bytes,
        });

        let flusher =
            FlusherHandle::start(persistence_cfg, Arc::clone(&manifest), Arc::clone(&inner))?;

        Ok(Self {
            inner,
            persistence: Some(PersistenceState {
                manifest,
                cache,
                flusher,
                parts_root,
            }),
        })
    }

    /// Collect diagnostic info about store contents.
    pub fn diagnostic_info(&self) -> super::StoreDiagnostics {
        use super::{AggregationDiagnostic, StoreDiagnostics};

        let mut per_aggregation = Vec::new();
        let mut total_time_map_entries: usize = 0;
        let total_sketch_bytes: usize = self.inner.mem_bytes_sealed.load(Ordering::Relaxed);

        for entry in self.inner.store.iter() {
            let agg_id = *entry.key();
            let data = match entry.value().read() {
                Ok(d) => d,
                Err(_) => continue,
            };
            let time_map_len = data.current_epoch.window_count()
                + data
                    .sealed_epochs
                    .values()
                    .map(|e| e.distinct_window_count())
                    .sum::<usize>();
            let read_counts_len = data.read_counts.lock().map(|rc| rc.len()).unwrap_or(0);
            total_time_map_entries += time_map_len;

            let num_aggregate_objects = data.current_epoch.len()
                + data
                    .sealed_epochs
                    .values()
                    .map(|e| e.entries.len())
                    .sum::<usize>();

            per_aggregation.push(AggregationDiagnostic {
                aggregation_id: agg_id,
                time_map_len,
                read_counts_len,
                num_aggregate_objects,
                sketch_bytes: 0, // per-agg sketch byte sizing is a follow-up
            });
        }

        StoreDiagnostics {
            num_aggregations: self.inner.store.len(),
            total_time_map_entries,
            total_sketch_bytes,
            per_aggregation,
        }
    }

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreKeyData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
    ) {
        // When persistence is enabled, eviction is the flusher's job.
        // Skip destructive cleanup entirely — parts on disk are the
        // source of truth for cold data.
        if self.inner.persistence_enabled {
            let _ = (
                num_aggregates_to_retain,
                metric,
                aggregation_id,
                read_count_threshold,
            );
            return;
        }

        match self.inner.cleanup_policy {
            CleanupPolicy::CircularBuffer => {
                // Handled by maybe_rotate_epoch() during insert.
                let _ = (num_aggregates_to_retain, metric, aggregation_id);
            }
            CleanupPolicy::ReadBased => {
                if let Some(threshold) = read_count_threshold {
                    data.cleanup_read_based(metric, aggregation_id, threshold);
                }
            }
            CleanupPolicy::NoCleanup => {}
        }
    }

    fn insert_for_store_key(
        &self,
        store_key: &StoreKey,
        metric: &str,
        items: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let aggregation_id = *store_key;
        let metric_key = metric.to_string();
        let inserted_delta = items.len() as u64;
        let persistence_enabled = self.inner.persistence_enabled;

        // ---- Back-pressure (persistence only) ----
        //
        // If sealed memory has reached the hard cap, block the insert
        // on the flusher's pressure condvar until the flusher makes
        // progress. This has to happen BEFORE we take the per-agg
        // RwLock::write so unrelated queries on the same agg aren't
        // stalled by the wait. The wait is bounded so a stuck flusher
        // logs-and-degrades rather than deadlocking the ingest path.
        //
        // Uses Ordering::Acquire on the load so the check synchronizes
        // with the flusher's Relaxed decrements on evict — we might
        // see stale values, but the wait loop re-checks after every
        // notify_all anyway.
        if persistence_enabled {
            if let Some(state) = self.persistence.as_ref() {
                let cap = self.inner.hard_cap_bytes;
                let current = self.inner.mem_bytes_sealed.load(Ordering::Acquire);
                if current >= cap {
                    let ok = state.flusher.wait_for_memory_under(
                        &self.inner.mem_bytes_sealed,
                        cap,
                        INSERT_BACK_PRESSURE_TIMEOUT,
                    );
                    if !ok {
                        warn!(
                            "insert back-pressure timed out for agg_id {}: \
                             mem={} bytes, hard_cap={}, proceeding anyway",
                            aggregation_id,
                            self.inner.mem_bytes_sealed.load(Ordering::Relaxed),
                            cap
                        );
                    }
                }
            }
        }

        // Opt 4: compute batch minimum timestamp before acquiring any lock.
        let batch_min_ts = items
            .iter()
            .map(|(o, _)| o.start_timestamp)
            .min()
            .unwrap_or(u64::MAX);

        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Get or create the store data for this key
        let store_data_lock = self
            .inner
            .store
            .entry(*store_key)
            .or_insert_with(|| Arc::new(RwLock::new(StoreKeyData::new())));

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Insert DashMap get time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Acquire write lock for this aggregation_id only
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Insert RwLock wait time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        // Create metric if needed (lock-free DashMap insert)
        self.inner.metrics.entry(metric_key.clone()).or_insert(());

        // Opt 4: one atomic earliest-ts update per batch.
        self.inner
            .earliest_timestamps
            .entry(aggregation_id)
            .and_modify(|earliest| {
                earliest.fetch_min(batch_min_ts, Ordering::Relaxed);
            })
            .or_insert_with(|| AtomicU64::new(batch_min_ts));

        // Update insertion counter once per grouped batch.
        let items_inserted_counter = self
            .inner
            .items_inserted
            .entry(metric_key)
            .or_insert_with(|| AtomicU64::new(0));
        let previous_total = items_inserted_counter.fetch_add(inserted_delta, Ordering::Relaxed);
        let new_total = previous_total + inserted_delta;
        if new_total / 1000 > previous_total / 1000 {
            debug!("Inserted {} items into {}", new_total, metric);
        }

        let aggregation_config = self
            .inner
            .streaming_config
            .get_aggregation_config(aggregation_id)
            .ok_or_else(|| format!("Aggregation config not found for {}", aggregation_id))?;

        // Configure epoch capacity on first insert (Optimization 2).
        // When persistence is enabled and the streaming config has no
        // retention set, fall back to a default so the rotator seals
        // current epochs periodically — otherwise nothing is ever
        // flushable.
        if aggregation_config.aggregation_type != AggregationType::DeltaSetAggregator {
            let effective_retention = aggregation_config.num_aggregates_to_retain.or({
                if persistence_enabled {
                    Some(PERSISTENCE_DEFAULT_EPOCH_CAPACITY as u64)
                } else {
                    None
                }
            });
            data.configure_epochs(effective_retention);
        }

        // Sum the real per-accumulator byte estimate up front. Summing
        // happens before the loop consumes `items`; each
        // `approx_memory_bytes` call is O(1) or a cheap field read on
        // all overriding impls, so this adds at most a handful of
        // arithmetic ops per batch item.
        let batch_approx_bytes: usize = if persistence_enabled {
            items.iter().map(|(_, a)| a.approx_memory_bytes()).sum()
        } else {
            0
        };

        for (output, precompute) in items {
            let timestamp_range = (output.start_timestamp, output.end_timestamp);
            let metric_id: MetricID = data.intern.intern(output.key);

            data.current_epoch
                .insert(metric_id, timestamp_range, Arc::from(precompute));

            // When persistence is on, always run rotation so sealed
            // epochs accumulate. Otherwise preserve the old
            // CircularBuffer-only behavior.
            let should_rotate = aggregation_config.aggregation_type
                != AggregationType::DeltaSetAggregator
                && (persistence_enabled
                    || matches!(self.inner.cleanup_policy, CleanupPolicy::CircularBuffer));
            if should_rotate {
                data.maybe_rotate_epoch(persistence_enabled);
            }
        }

        if persistence_enabled {
            // Tracked bytes are the sum of each accumulator's own
            // `approx_memory_bytes()`. The counter conceptually
            // represents "bytes in sealed epochs"; in practice we
            // charge them to the store as soon as they arrive
            // (current_epoch is included) because the rotator will
            // seal them soon anyway and the flusher's trigger is
            // conservative.
            self.inner
                .mem_bytes_sealed
                .fetch_add(batch_approx_bytes, Ordering::Relaxed);
        }

        if aggregation_config.aggregation_type != AggregationType::DeltaSetAggregator {
            self.cleanup_old_aggregates(
                &mut data,
                metric,
                aggregation_id,
                aggregation_config.num_aggregates_to_retain,
                aggregation_config.read_count_threshold,
            );
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Insert lock hold time: {:.2}ms (metric: {}, agg_id: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key
            );
        }

        Ok(())
    }

    /// Query overlapping on-disk parts and merge into the provided
    /// in-memory result map. A no-op when persistence is disabled.
    fn query_disk_parts(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
        results: &mut TimestampedBucketsMap,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(state) = self.persistence.as_ref() else {
            return Ok(());
        };

        let overlapping = state.manifest.live_parts_overlapping(start, end);
        if overlapping.is_empty() {
            return Ok(());
        }

        for entry in overlapping {
            let reader = match state.cache.get_or_load(entry.part_id) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "query_disk_parts: failed to open part {} for metric {}: {}",
                        entry.part_id, metric, e
                    );
                    continue;
                }
            };
            for rec in reader.index_records() {
                if rec.agg_id != aggregation_id {
                    continue;
                }
                // Same overlap semantics as MutableEpoch::range_query_into:
                // window must be fully inside [start, end].
                if rec.start_ts < start || rec.start_ts > end || rec.end_ts > end {
                    continue;
                }
                let disk_entry = match reader.load_entry(&rec) {
                    Ok(d) => d,
                    Err(e) => {
                        warn!(
                            "query_disk_parts: failed to load entry at offset {} of part {}: {}",
                            rec.data_offset, entry.part_id, e
                        );
                        continue;
                    }
                };
                let Some(sketch_type) = type_name_to_sketch_type(&disk_entry.sketch_type_name)
                else {
                    warn!(
                        "query_disk_parts: no SketchType mapping for {}; skipping",
                        disk_entry.sketch_type_name
                    );
                    continue;
                };
                let decoded = match accumulator_serde::deserialize_accumulator(
                    &disk_entry.sketch_bytes,
                    &sketch_type,
                ) {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(
                            "query_disk_parts: deserialize failed for {}: {}",
                            disk_entry.sketch_type_name, e
                        );
                        continue;
                    }
                };
                let arc_acc: Arc<dyn AggregateCore> = Arc::from(decoded);
                results
                    .entry(disk_entry.label.clone())
                    .or_default()
                    .push(((rec.start_ts, rec.end_ts), arc_acc));
            }
        }

        Ok(())
    }
}

impl Drop for SimpleMapStorePerKey {
    fn drop(&mut self) {
        // Dropping the PersistenceState (and therefore the FlusherHandle)
        // stops the flusher thread before the underlying Arc<PerKeyInner>
        // ref count hits zero, guaranteeing the flusher cannot observe
        // a half-destroyed store.
        if let Some(mut state) = self.persistence.take() {
            state.flusher.shutdown();
        }
    }
}

/// Map `AggregateCore::type_name()` to the `SketchType` enum value
/// used by `accumulator_serde::deserialize_accumulator`. Returns
/// `None` for types that don't have a working Arroyo round-trip yet.
fn type_name_to_sketch_type(name: &str) -> Option<SketchType> {
    match name {
        "SumAccumulator" => Some(SketchType::Sum),
        "DatasketchesKLLAccumulator" => Some(SketchType::KLL),
        "HydraKllSketchAccumulator" => Some(SketchType::HydraKLL),
        "CountMinSketchAccumulator" => Some(SketchType::CountMinSketch),
        "SetAggregatorAccumulator" => Some(SketchType::SetAggregator),
        "DeltaSetAggregatorAccumulator" => Some(SketchType::DeltaSetAggregator),
        "MultipleSumAccumulator" => Some(SketchType::MultipleSum),
        "MultipleIncreaseAccumulator" => Some(SketchType::MultipleIncrease),
        _ => None,
    }
}

#[async_trait::async_trait]
impl Store for SimpleMapStorePerKey {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        self.insert_precomputed_output_batch(vec![(output, precompute)])
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let batch_insert_start_time = Instant::now();
        let batch_size = outputs.len();

        // Group by aggregation_id
        #[allow(clippy::type_complexity)]
        let mut grouped: HashMap<
            StoreKey,
            (String, Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>),
        > = HashMap::new();

        for (output, precompute) in outputs {
            let aggregation_config = self
                .inner
                .streaming_config
                .get_aggregation_config(output.aggregation_id);

            if aggregation_config.is_none() {
                error!(
                    "Aggregation config not found for aggregation_id {}. Skipping insert.",
                    output.aggregation_id
                );
                continue;
            }
            let aggregation_config = aggregation_config.unwrap();

            let metric = aggregation_config.metric.clone();
            let store_key = output.aggregation_id;

            grouped
                .entry(store_key)
                .or_insert_with(|| (metric.clone(), Vec::new()))
                .1
                .push((output, precompute));
        }

        for (store_key, (metric, items)) in grouped {
            self.insert_for_store_key(&store_key, &metric, items)?;
        }

        let batch_insert_duration = batch_insert_start_time.elapsed();
        debug!(
            "Batch insert of {} items took: {:.2}ms",
            batch_size,
            batch_insert_duration.as_secs_f64() * 1000.0
        );
        Ok(())
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        if start > end {
            debug!(
                "Invalid query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, start, end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        let mut results: TimestampedBucketsMap = HashMap::new();

        // --- In-memory path (unchanged) ---
        if let Some(store_data_lock) = self.inner.store.get(&store_key) {
            let data = store_data_lock.read().map_err(|e| {
                format!(
                    "Failed to acquire read lock for query aggregation_id {}: {}",
                    store_key, e
                )
            })?;

            let mut mid: MetricBucketMap = HashMap::with_capacity(data.intern.len());
            let mut matched_windows: Vec<TimestampRange> = Vec::new();

            if let Some((min_start, max_end)) = data.current_epoch.time_bounds() {
                if !(min_start > end || max_end < start) {
                    data.current_epoch
                        .range_query_into(start, end, &mut mid, &mut matched_windows);
                }
            }

            for epoch in data.sealed_epochs.values() {
                let Some((min_start, max_end)) = epoch.time_bounds() else {
                    continue;
                };
                if min_start > end || max_end < start {
                    continue;
                }
                epoch.range_query_into(start, end, &mut mid, &mut matched_windows);
            }

            for (metric_id, buckets) in mid {
                let label = data.intern.resolve(metric_id).clone();
                results.entry(label).or_default().extend(buckets);
            }

            {
                let mut read_counts = data.read_counts.lock().unwrap();
                for window in &matched_windows {
                    *read_counts.entry(*window).or_insert(0) += 1;
                }
            }
        } else if self.persistence.is_none() {
            // Nothing in memory and no disk layer → empty result,
            // matching the old behavior.
            info!("Metric {} not found in store", metric);
        }

        // --- On-disk path (persistence only) ---
        if self.persistence.is_some() {
            self.query_disk_parts(metric, aggregation_id, start, end, &mut results)?;
        }

        let query_duration = query_start_time.elapsed();
        debug!(
            "Total query took: {:.2}ms ({} keys, {} in-memory + disk)",
            query_duration.as_secs_f64() * 1000.0,
            results.len(),
            results.values().map(|v| v.len()).sum::<usize>()
        );

        Ok(results)
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        // NOTE (persistence follow-up): this path does NOT consult
        // on-disk parts in v1 — exact queries only see in-memory
        // state. A subsequent PR will wire up a parts-aware exact
        // path. For the range-query code path (the 90% case) the
        // on-disk merge is already in place.
        if exact_start > exact_end {
            debug!(
                "Invalid exact query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, exact_start, exact_end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        let store_data_lock = match self.inner.store.get(&store_key) {
            Some(lock) => lock,
            None => {
                debug!("Metric {} not found in store for exact query", metric);
                return Ok(HashMap::new());
            }
        };

        // Opt 1: exact_query takes &mut self (lazy index build), so we need a write lock.
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for exact query aggregation_id {}: {}",
                store_key, e
            )
        })?;

        let timestamp_range = (exact_start, exact_end);

        let entries_opt: Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> =
            data.current_epoch.exact_query(timestamp_range).or_else(|| {
                data.sealed_epochs
                    .values()
                    .rev()
                    .find_map(|epoch| epoch.exact_query(timestamp_range))
            });

        let mut results: TimestampedBucketsMap = HashMap::new();
        let found_match = entries_opt.is_some();

        if let Some(entries) = entries_opt {
            for (metric_id, agg) in entries {
                let label = data.intern.resolve(metric_id).clone();
                results
                    .entry(label)
                    .or_default()
                    .push((timestamp_range, agg));
            }
        }

        if found_match {
            let mut read_counts = data.read_counts.lock().unwrap();
            *read_counts.entry(timestamp_range).or_insert(0) += 1;
        }

        let query_duration = query_start_time.elapsed();
        debug!(
            "Exact timestamp query took: {:.2}ms (found: {})",
            query_duration.as_secs_f64() * 1000.0,
            !results.is_empty()
        );

        Ok(results)
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        let result = self
            .inner
            .earliest_timestamps
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();

        Ok(result)
    }

    fn close(&self) -> StoreResult<()> {
        info!("SimpleMapStorePerKey closed");
        Ok(())
    }
}

// =================================================================
// EpochSource implementation (used by the persistence flusher)
// =================================================================

impl EpochSource for PerKeyInner {
    fn list_sealed_epochs(&self) -> Vec<SealedEpochRef> {
        let mut out = Vec::new();
        for entry in self.store.iter() {
            let agg_id = *entry.key();
            let Ok(data) = entry.value().read() else {
                continue;
            };
            for (epoch_id, epoch) in data.sealed_epochs.iter() {
                if let Some((_, max_end)) = epoch.time_bounds() {
                    out.push(SealedEpochRef {
                        agg_id,
                        epoch_id: *epoch_id,
                        end_ts: max_end,
                        approx_bytes: epoch_approx_bytes(epoch),
                    });
                }
            }
        }
        out
    }

    fn snapshot_sealed_epoch(
        &self,
        agg_id: u64,
        epoch_id: u64,
    ) -> PersistResult<Option<EpochSnapshot>> {
        let Some(lock) = self.store.get(&agg_id) else {
            return Ok(None);
        };
        let data = lock
            .read()
            .map_err(|e| PersistError::Internal(format!("read lock poisoned: {}", e)))?;
        let Some(epoch) = data.sealed_epochs.get(&epoch_id) else {
            return Ok(None);
        };

        let mut entries = Vec::with_capacity(epoch.entries.len());
        for (tr, metric_id, agg) in &epoch.entries {
            // Resolve the label from the per-agg intern table.
            let label: Option<KeyByLabelValues> = data.intern.resolve(*metric_id).clone();

            // Serialize the sketch via the arroyo path so it's
            // round-trippable via deserialize_accumulator.
            let sketch_bytes = accumulator_serde::serialize_accumulator_arroyo(agg.as_ref());
            let type_name = agg.type_name().to_string();

            entries.push(EpochSnapshotEntry {
                start_ts: tr.0,
                end_ts: tr.1,
                label,
                sketch_type_name: type_name,
                sketch_bytes,
            });
        }

        let (min_ts, max_ts) = epoch.time_bounds().unwrap_or((0, 0));
        let approx_bytes = epoch_approx_bytes(epoch);

        Ok(Some(EpochSnapshot {
            agg_id,
            epoch_id,
            min_ts,
            max_ts,
            entries,
            approx_bytes,
        }))
    }

    fn evict_sealed_epoch(&self, agg_id: u64, epoch_id: u64) {
        let Some(lock) = self.store.get(&agg_id) else {
            return;
        };
        let mut data = match lock.write() {
            Ok(d) => d,
            Err(e) => {
                error!("evict: write lock poisoned for agg_id {}: {}", agg_id, e);
                return;
            }
        };
        if let Some(epoch) = data.sealed_epochs.remove(&epoch_id) {
            let freed = epoch_approx_bytes(&epoch);
            self.mem_bytes_sealed.fetch_sub(freed, Ordering::Relaxed);
            // Also purge the epoch's windows from read_counts so they
            // don't leak.
            let mut read_counts = data.read_counts.lock().unwrap();
            for w in epoch.unique_windows() {
                read_counts.remove(&w);
            }
        }
    }

    fn approx_memory_bytes(&self) -> usize {
        self.mem_bytes_sealed.load(Ordering::Relaxed)
    }
}
