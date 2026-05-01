use crate::data_model::HotReloadStreamingConfig;
use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::drivers::ingest::victoriametrics_remote_write::decode_victoriametrics_remote_write;
use crate::drivers::query::servers::metrics as srv_metrics;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use crate::stores::sketch_db::SchemaRegistry;
use asap_types::aggregation_config::AggregationConfig;
use axum::{body::Bytes, extract::State, http::StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

/// Shared state for the ingest HTTP handler.
///
/// Holds the worker router plus the aggregation configs needed for group-key
/// extraction. A single instance is shared by the Prometheus/VictoriaMetrics
/// HTTP ingest server and any other ingest source that wants to route into
/// the same worker pool (e.g. the OTLP receiver).
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
    pub sketch_snapshots: dashmap::DashMap<String, Box<dyn crate::data_model::AggregateCore>>,
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
    pub fn config_snapshot(&self) -> Arc<crate::data_model::StreamingConfig> {
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
    /// (Prometheus remote-write, VictoriaMetrics remote-write,
    /// OTLP raw points, OTLP sketch envelopes, OTLP modified-proto
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

/// Shared logic: group decoded samples by (agg_id, group_key) and route to workers.
pub(crate) async fn route_decoded_samples(
    state: &IngestState,
    samples: Vec<crate::drivers::ingest::prometheus_remote_write::DecodedSample>,
    ingest_received_at: Instant,
    protocol: &str,
) -> StatusCode {
    if samples.is_empty() {
        return StatusCode::NO_CONTENT;
    }

    let count = samples.len() as u64;
    state
        .samples_ingested
        .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    srv_metrics::record_ingest_samples(protocol, count);

    if state.pass_raw_samples {
        // Raw mode: group by series key and send as RawSamples
        let mut by_series: HashMap<&str, Vec<(i64, f64)>> = HashMap::new();
        for s in &samples {
            by_series
                .entry(&s.labels)
                .or_default()
                .push((s.timestamp_ms, s.value));
        }
        let messages: Vec<WorkerMessage> = by_series
            .into_iter()
            .map(|(k, v)| WorkerMessage::RawSamples {
                series_key: k.to_string(),
                samples: v,
                ingest_received_at,
            })
            .collect();

        if let Err(e) = state
            .router
            .route_group_batch(messages, ingest_received_at)
            .await
        {
            warn!("Batch routing error: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
        return StatusCode::NO_CONTENT;
    }

    // Group-by mode: for each sample, find matching agg configs and group by
    // (agg_id, group_key). This is the equivalent of Arroyo's GROUP BY.
    //
    // Snapshot the latest config from the hot-reload handle at the
    // start of each batch, so config swaps are visible immediately.
    // This is a single Arc refcount bump (~5ns), not a clone.
    let snap = state.config_snapshot();
    let agg_configs = snap.get_all_aggregation_configs();

    // Reconcile the schema registry against the snapshot (Phase 2a of
    // the sketch DB design — `docs/design-sketch-db.md` §6). New
    // agg_ids in the snapshot become Active schemas; agg_ids removed
    // from the snapshot transition to Retired (the §6.3 write barrier
    // then rejects further writes to them). Reconcile is a HashMap
    // diff against the registry's current state — cheap.
    let _summary = state.schemas.reconcile(&snap);

    // Key: (agg_id, group_key) → Vec<(series_key, timestamp_ms, value)>
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();

    // Per-batch drop tally for the §6.3 barrier. Aggregated here and
    // flushed once at the end of the batch so a hot ingest path doesn't
    // spam one log line per sample.
    let mut dropped_by_barrier: HashMap<u64, u64> = HashMap::new();

    for s in &samples {
        let metric_name = extract_metric_name(&s.labels);
        for config in agg_configs.values() {
            if config.metric != metric_name
                && config.spatial_filter_normalized != metric_name
                && config.spatial_filter != metric_name
            {
                continue;
            }
            // §6.3 write-side schema barrier: silently skip retired or
            // expired aggs even if they're still in the snapshot for some
            // reason. This is the authoritative "no writes after
            // retirement" guarantee.
            if !state.schemas.is_writable(config.aggregation_id) {
                *dropped_by_barrier.entry(config.aggregation_id).or_default() += 1;
                continue;
            }
            let group_key = extract_group_key(&s.labels, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((s.labels.clone(), s.timestamp_ms, s.value));
        }
    }

    if !dropped_by_barrier.is_empty() {
        let total_dropped: u64 = dropped_by_barrier.values().sum();
        for (agg_id, count) in &dropped_by_barrier {
            state.record_barrier_drop(*agg_id, *count);
        }
        debug!(
            total_dropped,
            by_agg_id = ?dropped_by_barrier,
            "§6.3 write barrier dropped samples (agg is retired/expired)"
        );
    }

    let messages: Vec<WorkerMessage> = by_group
        .into_iter()
        .map(
            |((agg_id, group_key), samples)| WorkerMessage::GroupSamples {
                agg_id,
                group_key,
                samples,
                ingest_received_at,
            },
        )
        .collect();

    if let Err(e) = state
        .router
        .route_group_batch(messages, ingest_received_at)
        .await
    {
        warn!("Batch routing error: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::NO_CONTENT
}

/// Axum handler for Prometheus remote write (Snappy + Protobuf).
pub(crate) async fn handle_prometheus_ingest(
    State(state): State<Arc<IngestState>>,
    body: Bytes,
) -> StatusCode {
    let _timer = srv_metrics::start_ingest_timer(srv_metrics::INGEST_PROTO_PROM_RW);
    let ingest_received_at = Instant::now();
    let samples = match decode_prometheus_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode Prometheus remote write: {}", e);
            srv_metrics::record_ingest_decode_error(srv_metrics::INGEST_PROTO_PROM_RW);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(
        &state,
        samples,
        ingest_received_at,
        srv_metrics::INGEST_PROTO_PROM_RW,
    )
    .await
}

/// Axum handler for VictoriaMetrics remote write (Zstd + Protobuf).
pub(crate) async fn handle_victoriametrics_ingest(
    State(state): State<Arc<IngestState>>,
    body: Bytes,
) -> StatusCode {
    let _timer = srv_metrics::start_ingest_timer(srv_metrics::INGEST_PROTO_VM_RW);
    let ingest_received_at = Instant::now();
    let samples = match decode_victoriametrics_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode VictoriaMetrics remote write: {}", e);
            srv_metrics::record_ingest_decode_error(srv_metrics::INGEST_PROTO_VM_RW);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(
        &state,
        samples,
        ingest_received_at,
        srv_metrics::INGEST_PROTO_VM_RW,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::StreamingConfig;
    use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
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
            None,
        )
    }

    fn sample(metric: &str, ts: i64, val: f64) -> DecodedSample {
        DecodedSample {
            labels: metric.to_string(),
            timestamp_ms: ts,
            value: val,
        }
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
        let hot_reload = crate::data_model::HotReloadStreamingConfig::new(streaming.clone());

        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));

        let state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            samples_blocked_by_schema_barrier: std::sync::atomic::AtomicU64::new(0),
            hot_reload_config: hot_reload,
            schemas,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
        });

        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        (state, drain)
    }

    #[tokio::test]
    async fn barrier_counter_stays_zero_when_schema_is_active() {
        let (state, drain) = setup_state(1, "metric_1").await;
        let _ = route_decoded_samples(
            &state,
            vec![sample("metric_1", 100, 1.0), sample("metric_1", 200, 2.0)],
            std::time::Instant::now(),
            "prometheus_rw",
        )
        .await;
        assert_eq!(
            state
                .samples_blocked_by_schema_barrier
                .load(Ordering::Relaxed),
            0,
            "barrier must not fire for an Active schema"
        );
        drop(state);
        let _ = drain.await;
    }

    #[tokio::test]
    async fn barrier_counter_increments_after_force_expire() {
        let (state, drain) = setup_state(1, "metric_1").await;

        // Baseline: Active schema, barrier idle.
        let _ = route_decoded_samples(
            &state,
            vec![sample("metric_1", 100, 1.0)],
            std::time::Instant::now(),
            "prometheus_rw",
        )
        .await;
        assert_eq!(
            state
                .samples_blocked_by_schema_barrier
                .load(Ordering::Relaxed),
            0
        );

        // Flip the schema to Expired via the #42 manual endpoint.
        assert!(state.schemas.force_expire(1).is_some());

        // Second batch: every matching sample gets barrier-dropped.
        let _ = route_decoded_samples(
            &state,
            vec![
                sample("metric_1", 300, 3.0),
                sample("metric_1", 400, 4.0),
                sample("metric_1", 500, 5.0),
            ],
            std::time::Instant::now(),
            "prometheus_rw",
        )
        .await;
        assert_eq!(
            state
                .samples_blocked_by_schema_barrier
                .load(Ordering::Relaxed),
            3,
            "three post-expire samples must be dropped by the §6.3 barrier"
        );
        drop(state);
        let _ = drain.await;
    }

    /// Samples for metrics that don't match any agg must not count as
    /// barrier drops — the barrier only fires on (matching metric) ×
    /// (non-Active agg).
    #[tokio::test]
    async fn barrier_counter_ignores_non_matching_metrics() {
        let (state, drain) = setup_state(1, "metric_1").await;
        assert!(state.schemas.force_expire(1).is_some());

        let _ = route_decoded_samples(
            &state,
            vec![
                sample("other_metric", 100, 1.0),
                sample("yet_another", 200, 2.0),
            ],
            std::time::Instant::now(),
            "prometheus_rw",
        )
        .await;
        assert_eq!(
            state
                .samples_blocked_by_schema_barrier
                .load(Ordering::Relaxed),
            0,
            "samples for unrelated metrics must not count as barrier drops"
        );
        drop(state);
        let _ = drain.await;
    }

    /// Prometheus CounterVec is process-global via `lazy_static`, so
    /// we key on a unique agg_id (9001) to get a fresh baseline that
    /// no other test in the process has touched.
    #[tokio::test]
    async fn barrier_prom_counter_increments_per_agg_label() {
        let (state, drain) = setup_state(9001, "metric_prom_test").await;
        let label = "9001";
        let baseline = crate::stores::sketch_db::metrics::SAMPLES_BLOCKED_BY_SCHEMA_BARRIER
            .with_label_values(&[label])
            .get();

        assert!(state.schemas.force_expire(9001).is_some());

        let _ = route_decoded_samples(
            &state,
            vec![
                sample("metric_prom_test", 100, 1.0),
                sample("metric_prom_test", 200, 2.0),
                sample("metric_prom_test", 300, 3.0),
                sample("metric_prom_test", 400, 4.0),
            ],
            std::time::Instant::now(),
            "prometheus_rw",
        )
        .await;

        let after = crate::stores::sketch_db::metrics::SAMPLES_BLOCKED_BY_SCHEMA_BARRIER
            .with_label_values(&[label])
            .get();
        assert!(
            (after - baseline - 4.0).abs() < f64::EPSILON,
            "prom counter for agg_id={label} should have advanced by 4; baseline={baseline}, after={after}"
        );
        drop(state);
        let _ = drain.await;
    }

    /// `record_barrier_drop` is the single entry point every ingest
    /// driver (Prometheus remote-write, VictoriaMetrics remote-write,
    /// OTLP raw / sketch / modified-proto) funnels through, so
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
        use crate::precompute_operators::DDSketchAccumulator;
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
