use crate::precompute_engine::series_router::SeriesRouter;
use crate::precompute_engine::worker::parse_labels_from_series_key;
use crate::storage_engines::types::StreamingConfigHandle;
use asap_types::aggregation_config::PrecomputeMaterialization;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// CQ-6 — per-reason atomic counters for silently-dropped ingest
/// samples, plus the §RES-1 sketch-snapshot eviction configuration.
///
/// Grouped into a single `Default`-constructible sub-struct so the
/// counters/config can be added to [`IngestState`] without changing the
/// (explicit, field-by-field) struct-literal call sites that construct
/// it — they only need a single `observability: IngestObservability::new()`
/// (or `..Default` style) line.
///
/// Counters are `AtomicU64` (relaxed ordering — these are monotonic
/// observability counters, not synchronization primitives), incremented
/// at the corresponding drop sites in the OTLP ingest path
/// (`route_modified_otlp_sketches_to_precompute`) and the output-sink
/// policy-miss path. Surfaced wherever `IngestState` stats reach
/// `/metrics`.
#[derive(Debug)]
pub struct IngestObservability {
    /// Delta frame arrived but the additive family could not be
    /// reconstructed from a bare delta (DD/KLL with no cached base).
    pub dropped_no_base: AtomicU64,
    /// A full / delta frame failed to decode (or a delta failed to
    /// apply) and was dropped.
    pub dropped_decode_fail: AtomicU64,
    /// A decoded sketch matched no `PrecomputeMaterialization` in the running
    /// streaming config (legacy routing-side bucketing miss).
    pub dropped_unconfigured: AtomicU64,
    /// The output sink could not resolve a `policy_fp` to an
    /// `PrecomputeMaterialization` (registry miss) and skipped the write.
    pub dropped_policy_miss: AtomicU64,
    /// RES-1 — max number of distinct tumbling windows a per-series
    /// snapshot base may lag behind the newest observed `window_start`
    /// before it is swept out of `sketch_snapshots`. Stored as a
    /// nanosecond span (windows are keyed by `start_time_unix_nano`),
    /// so "N windows" is expressed as `N * window_span_nanos`. A value
    /// of 0 disables age-based eviction.
    pub snapshot_max_window_lag_nanos: AtomicU64,
    /// RES-1 — the newest `window_start` (nanos) observed across all
    /// series, used as the eviction sweep's reference point. Advanced
    /// monotonically on each cached base insert.
    pub snapshot_newest_window_start: AtomicU64,
    /// Stateful checkpoint and sequence validation for physical-plan summary
    /// frames. Kept with the ingest-wide shared state so concurrent OTLP
    /// requests observe one linearizable lineage per producer/window.
    pub frame_lineage: super::frame_lineage::FrameLineageTracker,
}

impl IngestObservability {
    /// Default eviction lag: keep ~4 windows of per-series base behind
    /// the newest observed window. Picked to tolerate a couple of late /
    /// out-of-order windows while still bounding memory for churning
    /// high-cardinality series. The span is in nanoseconds; the default
    /// assumes a 60s tumbling window (4 * 60s = 240s), and is
    /// overridable via the `ASAP_SNAPSHOT_MAX_WINDOW_LAG_SECS` env var.
    pub const DEFAULT_MAX_WINDOW_LAG_NANOS: u64 = 4 * 60 * 1_000_000_000;

    pub fn new() -> Self {
        let lag = std::env::var("ASAP_SNAPSHOT_MAX_WINDOW_LAG_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|secs| secs.saturating_mul(1_000_000_000))
            .unwrap_or(Self::DEFAULT_MAX_WINDOW_LAG_NANOS);
        Self {
            dropped_no_base: AtomicU64::new(0),
            dropped_decode_fail: AtomicU64::new(0),
            dropped_unconfigured: AtomicU64::new(0),
            dropped_policy_miss: AtomicU64::new(0),
            snapshot_max_window_lag_nanos: AtomicU64::new(lag),
            snapshot_newest_window_start: AtomicU64::new(0),
            frame_lineage: super::frame_lineage::FrameLineageTracker::default(),
        }
    }
}

impl Default for IngestObservability {
    fn default() -> Self {
        Self::new()
    }
}

/// One per-series entry in the delta-reconstitution snapshot cache.
///
/// Carries the reconstructed accumulator base **plus** the start of the
/// tumbling window that base belongs to. The ingest path uses
/// `window_start` to drive per-window base rotation: when a delta frame
/// arrives whose data-point window start differs from the cached
/// `window_start`, the cached `core` is reset to empty before the new
/// window's delta is applied, so the reconstructed state reflects that
/// window only rather than an all-time accumulation across windows (see
/// `docs/delta-baseline-contract.md` §3).
pub struct SnapshotCacheEntry {
    /// Reconstructed per-series accumulator base.
    pub core: Box<dyn crate::storage_engines::types::AggregateCore>,
    /// `start_time_unix_nano` of the window this base was built for.
    /// Full frames set it from their own data point; delta frames
    /// compare against it to detect a window boundary.
    pub window_start: u64,
}

/// Shared state for the ingest path.
///
/// Holds the worker router plus the aggregation configs needed for group-key
/// extraction. A single instance is shared by every ingest source (currently
/// the OTLP receiver) so they all push into the same worker pool.
pub struct IngestState {
    pub router: SeriesRouter,
    pub samples_ingested: std::sync::atomic::AtomicU64,
    /// §6.3 observability counter — per-sample-per-agg-config drops
    /// caused by a non-Active schema for a matching agg. Incremented
    /// exactly when the barrier fires, so a non-zero reading proves
    /// the barrier is wired. One sample can contribute more than one
    /// drop when multiple agg configs match the metric and one (or
    /// more) of them is retired/expired.
    pub samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64,
    /// Hot-reloadable streaming config. On each ingest batch, the
    /// router snapshots the latest config to derive agg_configs.
    /// This replaces the old frozen `Vec<Arc<PrecomputeMaterialization>>`.
    pub hot_reload_config: StreamingConfigHandle,
    /// When true, skip group-key extraction and pass raw samples through.
    pub pass_raw_samples: bool,
    /// Per-series reconstructed sketch bases, keyed by series identity. Full frames
    /// replace the base; delta frames update it. Window boundaries reset the base.
    /// DashMap allows independent series to update concurrently.
    ///
    /// [`IngestState::note_window_and_sweep`] bounds growth by evicting bases whose
    /// window start falls behind the newest observed window by the configured lag.
    pub sketch_snapshots: dashmap::DashMap<String, SnapshotCacheEntry>,
    /// centralized series_id resolver. Shared across the OTLP
    /// receive path (sid resolution + `unknown_series_ids` population) and
    /// the `ResolveSeriesIDs` RPC (eager batch resolution from the agent's
    /// exporter). Holding it on `IngestState` lets every ingest source
    /// reach the same idempotent compute-or-mint cache.
    pub series_resolver: Arc<crate::drivers::ingest::series_resolver::SeriesIdResolver>,
    /// two-level sketch ASAP tier (instance metadata +
    /// per-sid columnar state). Populated by the OTLP ingest path on
    /// every modified-OTLP first-class sketch DataPoint; queried by
    /// the `ASAPQueryEngine` query path (ASAP-tier hit / ghost / unknown
    /// classification drives the Phase 6 archive failover).
    pub summary_store: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    /// CQ-6 / RES-1 — per-reason silent-drop counters plus the
    /// `sketch_snapshots` eviction configuration. Grouped into one
    /// `Default`-constructible field so the counters can live on
    /// `IngestState` without churning every struct-literal call site.
    pub observability: IngestObservability,
}

impl IngestState {
    /// Snapshot the current streaming config from the hot-reload
    /// handle. Called at the start of each ingest batch so new
    /// configs from a `POST /api/v1/streaming-config` swap are
    /// visible immediately without restart.
    ///
    /// Returns the shared `Arc<StreamingConfig>` — no cloning of
    /// individual PrecomputeMaterialization objects, just an atomic refcount
    /// increment (~5ns).
    pub fn config_snapshot(&self) -> Arc<crate::storage_engines::types::StreamingConfig> {
        self.hot_reload_config.snapshot()
    }

    pub fn active_physical_plan_snapshot(
        &self,
    ) -> Option<Arc<crate::storage_engines::types::RuntimePhysicalPlan>> {
        self.hot_reload_config.active_physical_plan_snapshot()
    }

    #[deprecated(note = "use active_physical_plan_snapshot")]
    pub fn physical_plan_snapshot(
        &self,
    ) -> Option<Arc<crate::storage_engines::types::RuntimePhysicalPlan>> {
        self.active_physical_plan_snapshot()
    }

    /// RES-1 — record that a per-series snapshot base for `window_start`
    /// was just (re)inserted, then opportunistically sweep stale entries.
    ///
    /// Advances the observed `snapshot_newest_window_start` monotonically
    /// and, when age-based eviction is enabled
    /// (`snapshot_max_window_lag_nanos != 0`), removes every
    /// `sketch_snapshots` entry whose `window_start` lags more than the
    /// configured span behind the newest observed window. This bounds
    /// memory for churning / high-cardinality series whose keys would
    /// otherwise accumulate forever (the cache previously had no
    /// eviction at all).
    ///
    /// "Opportunistic" — the sweep runs inline on insert. The DashMap
    /// `retain` walk is O(n) in the live key count, but it only fires
    /// when the newest window actually advances (so steady-state inserts
    /// within one window pay nothing), keeping amortized cost low. A
    /// future refactor can move this to a periodic background sweep if
    /// the inline walk shows up in profiles.
    ///
    /// Returns the number of entries evicted (0 when eviction is
    /// disabled or nothing was stale) — used by tests.
    pub fn note_window_and_sweep(&self, window_start: u64) -> usize {
        // Monotonically advance the newest-observed window.
        let mut newest = self
            .observability
            .snapshot_newest_window_start
            .load(Ordering::Relaxed);
        loop {
            if window_start <= newest {
                break;
            }
            match self
                .observability
                .snapshot_newest_window_start
                .compare_exchange_weak(newest, window_start, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    newest = window_start;
                    break;
                }
                Err(observed) => newest = observed,
            }
        }

        let lag = self
            .observability
            .snapshot_max_window_lag_nanos
            .load(Ordering::Relaxed);
        if lag == 0 {
            return 0;
        }
        // Evict entries strictly older than (newest - lag). Saturating
        // sub so an early small `newest` never underflows into a huge
        // cutoff that would evict everything.
        let cutoff = newest.saturating_sub(lag);
        if cutoff == 0 {
            return 0;
        }
        let before = self.sketch_snapshots.len();
        self.sketch_snapshots
            .retain(|_key, entry| entry.window_start >= cutoff);
        before.saturating_sub(self.sketch_snapshots.len())
    }
}

impl IngestState {
    /// Extract the group key for a series key against a given aggregation
    /// config. Re-exports the module-private helper so that out-of-module
    /// ingest sources (e.g. OTLP) can reuse it.
    pub fn extract_group_key_for(
        series_key: &str,
        config: &PrecomputeMaterialization,
    ) -> Arc<crate::precompute_engine::group_key::GroupKey> {
        extract_group_key(series_key, config)
    }

    /// PERF-4 — extract the group key directly from a parsed label map,
    /// avoiding the `format_series_key` → `parse_labels_from_series_key`
    /// round-trip when the caller already holds the labels (e.g. the OTLP
    /// modified-sketch path, which carries `dp.attrs` as a
    /// `HashMap<String, String>`). Produces the identical
    /// grouping-label-value join (`;`-separated, "" for absent labels) as
    /// [`Self::extract_group_key_for`] does after the round-trip.
    pub fn extract_group_key_from_labels(
        labels: &std::collections::HashMap<String, String>,
        config: &PrecomputeMaterialization,
    ) -> Arc<crate::precompute_engine::group_key::GroupKey> {
        crate::precompute_engine::group_key::intern_pairs(config.grouping_labels.iter().map(
            |name| {
                (
                    name.as_str(),
                    labels.get(name).map(String::as_str).unwrap_or(""),
                )
            },
        ))
    }
}

/// Extract the group key (grouping label values joined by semicolons)
/// for a given series key and aggregation config.
fn extract_group_key(
    series_key: &str,
    config: &PrecomputeMaterialization,
) -> Arc<crate::precompute_engine::group_key::GroupKey> {
    let labels = parse_labels_from_series_key(series_key);
    crate::precompute_engine::group_key::intern_pairs(config.grouping_labels.iter().map(|name| {
        (
            name.as_str(),
            labels.get(name.as_str()).copied().unwrap_or(""),
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::storage_engines::types::StreamingConfig;
    use asap_types::aggregation_config::PrecomputeMaterialization;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn make_config(_agg_id: u64, metric: &str) -> PrecomputeMaterialization {
        // `_agg_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`. Kept as a parameter to
        // avoid churning the call sites below.
        PrecomputeMaterialization::new(
            AggregationType::CountMinSketch,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            metric.to_string(),
            None,
            None,
            None,
        )
    }

    /// Set up an `IngestState` with one Active agg for `metric` and a
    /// draining worker channel. The drain task keeps the router
    /// channel empty so `route_group_batch` never blocks on capacity.
    async fn setup_state(
        agg_id: u64,
        metric: &str,
    ) -> (Arc<IngestState>, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel(1024);
        let router = SeriesRouter::new(vec![tx]);

        let mut map = std::collections::HashMap::new();
        map.insert(agg_id, make_config(agg_id, metric));
        let streaming = StreamingConfig::new(map);
        let hot_reload =
            crate::storage_engines::types::StreamingConfigHandle::new(streaming.clone());

        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(
                crate::drivers::ingest::series_resolver::SeriesIdResolver::new(),
            ),
            summary_store: Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            observability: IngestObservability::default(),
        });

        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        (state, drain)
    }

    /// Base frame → delta frame → second delta frame path against
    /// the per-series sketch snapshot cache. Exercises what the OTLP
    /// ingest loop does for a DDSketch stream: store on full, apply
    /// + refresh on delta, verify the cumulative state matches a
    /// hand-merged sequence of full sketches.
    #[tokio::test]
    async fn delta_path_reconstitutes_cumulative_state() {
        use crate::drivers::ingest::otel::apply_modified_otlp_delta_bytes;
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use asap_physical_operators::summary_kernels::DDSketchAccumulator;
        use asap_sketchlib::DdSketch;
        use planner_types::post_asap::SketchAlgorithm;
        use prost::Message;

        const ENCODING_PROTO_DELTA: i32 = 2;

        let (state, drain) = setup_state(42, "latency_ms").await;

        // Seed the cache with a base DDSketch as if the agent's
        // first PROTO-encoded frame had landed. Use a distinct key
        // so we're not racing any earlier tests.
        let series_key = "__name__=latency_ms,inst=a";
        let base = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 2, 3], 0),
            sample_p: 1.0,
        };
        state.sketch_snapshots.insert(
            series_key.to_string(),
            SnapshotCacheEntry {
                core: Box::new(base.clone()),
                window_start: 0,
            },
        );

        // First delta adds to bucket 0 and bucket 2. The wire delta now
        // carries only bucket deltas (the count/sum/min/max scalar fields
        // were dropped, ProjectASAP/sketchlib-go#243 / asap_sketchlib#57).
        let d1 = PbDelta {
            buckets: vec![
                DdSketchBucketDelta {
                    index: 0,
                    d_count: 10,
                },
                DdSketchBucketDelta {
                    index: 2,
                    d_count: 20,
                },
            ],
        }
        .encode_to_vec();
        let mut acc1 = state
            .sketch_snapshots
            .get(series_key)
            .unwrap()
            .core
            .clone_boxed_core();
        apply_modified_otlp_delta_bytes(
            SketchAlgorithm::DDSketch,
            ENCODING_PROTO_DELTA,
            &mut acc1,
            &d1,
        )
        .expect("apply first delta");
        state.sketch_snapshots.insert(
            series_key.to_string(),
            SnapshotCacheEntry {
                core: acc1.clone_boxed_core(),
                window_start: 0,
            },
        );

        // Second delta — picks up on top of the first, proving the
        // cache refresh is transitive.
        let d2 = PbDelta {
            buckets: vec![DdSketchBucketDelta {
                index: 1,
                d_count: 5,
            }],
        }
        .encode_to_vec();
        let mut acc2 = state
            .sketch_snapshots
            .get(series_key)
            .unwrap()
            .core
            .clone_boxed_core();
        apply_modified_otlp_delta_bytes(
            SketchAlgorithm::DDSketch,
            ENCODING_PROTO_DELTA,
            &mut acc2,
            &d2,
        )
        .expect("apply second delta");

        let final_dd = acc2.as_any().downcast_ref::<DDSketchAccumulator>().unwrap();
        // Base [1,2,3] + d1 [+10 on 0, +20 on 2] = [11,2,23];
        // + d2 [+5 on 1] = [11,7,23].
        assert_eq!(final_dd.inner.store_counts, vec![11, 7, 23]);
        // `count` recomputed from the merged buckets: 11 + 7 + 23 = 41.
        // (sum/min/max were dropped from the wire format,
        // ProjectASAP/sketchlib-go#243 / asap_sketchlib#57.)
        assert_eq!(final_dd.inner.total_count(), 41);

        drop(state);
        let _ = drain.await;
    }

    /// RES-1 — `note_window_and_sweep` evicts per-series snapshot bases
    /// whose `window_start` lags more than the configured span behind the
    /// newest observed window, bounding `sketch_snapshots` for churning
    /// high-cardinality series. A fresh entry in the current window must
    /// survive; a stale entry from far in the past must be swept.
    #[tokio::test]
    async fn stale_snapshot_entry_is_evicted_by_sweep() {
        use asap_physical_operators::summary_kernels::SumAccumulator;

        let (state, drain) = setup_state(7, "evict_metric").await;

        // Pin a deterministic lag of 100ns so the test doesn't depend on
        // the env default (240s). Entries older than (newest - 100) go.
        state
            .observability
            .snapshot_max_window_lag_nanos
            .store(100, std::sync::atomic::Ordering::Relaxed);

        // A stale entry from window_start=10 and a fresh entry from
        // window_start=1000. Cores are arbitrary — the sweep only reads
        // `window_start`.
        state.sketch_snapshots.insert(
            "stale".to_string(),
            SnapshotCacheEntry {
                core: Box::new(SumAccumulator::with_sum(1.0)),
                window_start: 10,
            },
        );
        state.sketch_snapshots.insert(
            "fresh".to_string(),
            SnapshotCacheEntry {
                core: Box::new(SumAccumulator::with_sum(2.0)),
                window_start: 1000,
            },
        );
        assert_eq!(state.sketch_snapshots.len(), 2);

        // Insert/observe the newest window (1000). cutoff = 1000 - 100 =
        // 900; the stale entry (10 < 900) is evicted, fresh (1000) stays.
        let evicted = state.note_window_and_sweep(1000);
        assert_eq!(evicted, 1, "exactly the stale entry is swept");
        assert!(
            state.sketch_snapshots.get("stale").is_none(),
            "stale entry evicted"
        );
        assert!(
            state.sketch_snapshots.get("fresh").is_some(),
            "fresh entry within the lag window survives"
        );

        // A lag of 0 disables eviction — nothing is swept even when an
        // ancient entry is present.
        state
            .observability
            .snapshot_max_window_lag_nanos
            .store(0, std::sync::atomic::Ordering::Relaxed);
        state.sketch_snapshots.insert(
            "ancient".to_string(),
            SnapshotCacheEntry {
                core: Box::new(SumAccumulator::with_sum(3.0)),
                window_start: 1,
            },
        );
        let evicted2 = state.note_window_and_sweep(5000);
        assert_eq!(evicted2, 0, "lag=0 disables age-based eviction");
        assert!(state.sketch_snapshots.get("ancient").is_some());

        drop(state);
        let _ = drain.await;
    }
}
