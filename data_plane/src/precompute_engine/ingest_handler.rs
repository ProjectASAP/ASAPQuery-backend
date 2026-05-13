use crate::stores::types::HotReloadStreamingConfig;
use crate::precompute_engine::series_router::SeriesRouter;
use crate::precompute_engine::worker::parse_labels_from_series_key;
use crate::stores::sketch_db::SchemaRegistry;
use asap_types::aggregation_config::AggregationConfig;
use std::sync::Arc;

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
    /// This replaces the old frozen `Vec<Arc<AggregationConfig>>`.
    pub hot_reload_config: HotReloadStreamingConfig,
    /// Per-`agg_id` schema registry — Phase 2a of the sketch DB design
    /// (`docs/design-sketch-db.md` §6). The ingest path consults
    /// `is_writable(agg_id)` before routing data so writes targeted at
    /// retired or expired aggregations are rejected at the boundary.
    /// The registry is reconciled against `hot_reload_config` on each
    /// ingest batch (cheap HashMap diff) so newly-added agg_ids are
    /// visible immediately.
    pub schemas: Arc<SchemaRegistry>,
    /// When true, skip group-key extraction and pass raw samples through.
    pub pass_raw_samples: bool,
    /// Per-series snapshot cache for delta-sketch reconstitution
    /// (paper §6.2 B3 / B4). On arrival of a full `ENCODING_PROTO`
    /// / `ENCODING_MSGPACK` frame the ingest path stores a clone of
    /// the decoded accumulator keyed by the metric's series_key. On
    /// arrival of a subsequent `ENCODING_PROTO_DELTA` frame it looks
    /// up the cached base, clones it, applies the delta via
    /// `apply_modified_otlp_delta_bytes`, and updates the cache so
    /// the next delta composes correctly.
    ///
    /// DashMap chosen over `Mutex<HashMap>` so concurrent OTLP
    /// receiver tasks don't serialize on cache access — each
    /// series_key is an independent shard.
    ///
    /// Growth is bounded by the active series set in the running
    /// streaming config; no explicit eviction yet. A cold-store
    /// follow-up will add TTL-based eviction keyed by last-seen
    /// timestamp so long-running deployments don't leak memory
    /// on retired series.
    pub sketch_snapshots: dashmap::DashMap<String, Box<dyn crate::stores::types::AggregateCore>>,
    /// Phase 4 — centralized series_id resolver. Shared across the OTLP
    /// receive path (sid resolution + `unknown_series_ids` population) and
    /// the `ResolveSeriesIDs` RPC (eager batch resolution from the agent's
    /// exporter). Holding it on `IngestState` lets every ingest source
    /// reach the same idempotent compute-or-mint cache.
    pub series_resolver: Arc<crate::drivers::ingest::series_resolver::SeriesIdResolver>,
    /// Phase 5 — two-level sketch warm tier (instance metadata +
    /// per-sid columnar state). Populated by the OTLP ingest path on
    /// every modified-OTLP first-class sketch DataPoint; queried by
    /// the `ASAPQueryEngine` query path (warm-tier hit / ghost / unknown
    /// classification drives the Phase 6 archive failover).
    pub sketch_index: Arc<crate::stores::sketch_db::index::SketchStore>,
}

impl IngestState {
    /// Snapshot the current streaming config from the hot-reload
    /// handle. Called at the start of each ingest batch so new
    /// configs from a `POST /api/v1/streaming-config` swap are
    /// visible immediately without restart.
    ///
    /// Returns the shared `Arc<StreamingConfig>` — no cloning of
    /// individual AggregationConfig objects, just an atomic refcount
    /// increment (~5ns).
    pub fn config_snapshot(&self) -> Arc<crate::stores::types::StreamingConfig> {
        self.hot_reload_config.snapshot()
    }
}

impl IngestState {
    /// Extract the group key for a series key against a given aggregation
    /// config. Re-exports the module-private helper so that out-of-module
    /// ingest sources (e.g. OTLP) can reuse it.
    pub fn extract_group_key_for(series_key: &str, config: &AggregationConfig) -> String {
        extract_group_key(series_key, config)
    }

    /// Record a §6.3 write-side barrier drop. Updates the in-process
    /// `samples_blocked_by_schema_barrier` atomic and the Prometheus
    /// `queryengine_ingest_samples_blocked_by_schema_barrier_total`
    /// counter, both keyed by `agg_id`. Called by every ingest path
    /// (OTLP raw points, OTLP sketch envelopes, OTLP modified-proto
    /// sketches) so the drop rate observable on `/metrics` is a
    /// single number regardless of which driver is active.
    pub fn record_barrier_drop(&self, agg_id: u64, count: u64) {
        self.samples_blocked_by_schema_barrier
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        crate::stores::sketch_db::metrics::SAMPLES_BLOCKED_BY_SCHEMA_BARRIER
            .with_label_values(&[&agg_id.to_string()])
            .inc_by(count as f64);
    }
}

/// Extract the group key (grouping label values joined by semicolons)
/// for a given series key and aggregation config.
fn extract_group_key(series_key: &str, config: &AggregationConfig) -> String {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();
    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(*val);
        } else {
            values.push("");
        }
    }
    values.join(";")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::types::StreamingConfig;
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::stores::sketch_db::SchemaRegistry;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn make_config(agg_id: u64, metric: &str) -> AggregationConfig {
        AggregationConfig::new(
            agg_id,
            AggregationType::CountMinSketch,
            String::new(),
            std::collections::HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
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
        let hot_reload = crate::stores::types::HotReloadStreamingConfig::new(streaming.clone());

        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));

        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            schemas,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(
                crate::drivers::ingest::series_resolver::SeriesIdResolver::new(),
            ),
            sketch_index: Arc::new(crate::stores::sketch_db::index::SketchStore::new()),
        });

        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        (state, drain)
    }

    /// `record_barrier_drop` is the single entry point every ingest
    /// driver (OTLP raw / sketch / modified-proto) funnels through, so
    /// verify both sides of the contract: the in-process atomic AND
    /// the Prometheus `CounterVec` move together, keyed by agg_id.
    #[tokio::test]
    async fn record_barrier_drop_advances_atomic_and_prom_counter() {
        let (state, drain) = setup_state(4242, "metric_helper_test").await;
        let label = "4242";
        let prom_baseline = crate::stores::sketch_db::metrics::SAMPLES_BLOCKED_BY_SCHEMA_BARRIER
            .with_label_values(&[label])
            .get();
        let atomic_baseline = state
            .samples_blocked_by_schema_barrier
            .load(Ordering::Relaxed);

        state.record_barrier_drop(4242, 7);

        let prom_after = crate::stores::sketch_db::metrics::SAMPLES_BLOCKED_BY_SCHEMA_BARRIER
            .with_label_values(&[label])
            .get();
        let atomic_after = state
            .samples_blocked_by_schema_barrier
            .load(Ordering::Relaxed);

        assert!(
            (prom_after - prom_baseline - 7.0).abs() < f64::EPSILON,
            "prom counter must advance by 7 via the helper"
        );
        assert_eq!(
            atomic_after - atomic_baseline,
            7,
            "atomic counter must advance by 7 via the helper"
        );
        drop(state);
        let _ = drain.await;
    }

    /// Base frame → delta frame → second delta frame path against
    /// the per-series sketch snapshot cache. Exercises what the OTLP
    /// ingest loop does for a DDSketch stream: store on full, apply
    /// + refresh on delta, verify the cumulative state matches a
    /// hand-merged sequence of full sketches.
    #[tokio::test]
    async fn delta_path_reconstitutes_cumulative_state() {
        use crate::drivers::ingest::otel::{apply_modified_otlp_delta_bytes, SketchKind};
        use crate::precompute_engine::operators::DDSketchAccumulator;
        use asap_otel_proto::sketchlib::v1::{DdSketchBucketDelta, DdSketchDelta as PbDelta};
        use asap_sketchlib::sketches::ddsketch::DdSketch;
        use prost::Message;

        const ENCODING_PROTO_DELTA: i32 = 2;

        let (state, drain) = setup_state(42, "latency_ms").await;

        // Seed the cache with a base DDSketch as if the agent's
        // first PROTO-encoded frame had landed. Use a distinct key
        // so we're not racing any earlier tests.
        let series_key = "__name__=latency_ms,inst=a";
        let base = DDSketchAccumulator {
            inner: DdSketch::from_raw(0.01, vec![1, 2, 3], 0, 6, 12.0, 1.0, 3.0),
        };
        state
            .sketch_snapshots
            .insert(series_key.to_string(), Box::new(base.clone()));

        // First delta adds to bucket 0 and bucket 2.
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
            d_count: 30,
            d_sum: 70.0,
            new_max: 5.0,
            max_changed: true,
            ..Default::default()
        }
        .encode_to_vec();
        let mut acc1 = state
            .sketch_snapshots
            .get(series_key)
            .unwrap()
            .clone_boxed_core();
        apply_modified_otlp_delta_bytes(SketchKind::DdSketch, ENCODING_PROTO_DELTA, &mut acc1, &d1)
            .expect("apply first delta");
        state
            .sketch_snapshots
            .insert(series_key.to_string(), acc1.clone_boxed_core());

        // Second delta — picks up on top of the first, proving the
        // cache refresh is transitive.
        let d2 = PbDelta {
            buckets: vec![DdSketchBucketDelta {
                index: 1,
                d_count: 5,
            }],
            d_count: 5,
            d_sum: 10.0,
            new_max: 6.0,
            max_changed: true,
            ..Default::default()
        }
        .encode_to_vec();
        let mut acc2 = state
            .sketch_snapshots
            .get(series_key)
            .unwrap()
            .clone_boxed_core();
        apply_modified_otlp_delta_bytes(SketchKind::DdSketch, ENCODING_PROTO_DELTA, &mut acc2, &d2)
            .expect("apply second delta");

        let final_dd = acc2.as_any().downcast_ref::<DDSketchAccumulator>().unwrap();
        // Base [1,2,3] + d1 [+10 on 0, +20 on 2] = [11,2,23];
        // + d2 [+5 on 1] = [11,7,23].
        assert_eq!(final_dd.inner.store_counts, vec![11, 7, 23]);
        // Counts add: 6 + 30 + 5 = 41.
        assert_eq!(final_dd.inner.count, 41);
        // Sum: 12 + 70 + 10 = 92.
        assert_eq!(final_dd.inner.sum, 92.0);
        // Max updated to 6.0 via d2's max_changed flag.
        assert_eq!(final_dd.inner.max, 6.0);

        drop(state);
        let _ = drain.await;
    }
}
