use crate::data_model::HotReloadStreamingConfig;
use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::drivers::ingest::victoriametrics_remote_write::decode_victoriametrics_remote_write;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use crate::stores::sketch_db::SchemaRegistry;
use asap_types::aggregation_config::AggregationConfig;
use axum::{body::Bytes, extract::State, http::StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

/// Shared state for the ingest HTTP handler.
///
/// Holds the worker router plus the aggregation configs needed for group-key
/// extraction. A single instance is shared by the Prometheus/VictoriaMetrics
/// HTTP ingest server and any other ingest source that wants to route into
/// the same worker pool (e.g. the OTLP receiver).
pub struct IngestState {
    pub router: SeriesRouter,
    pub samples_ingested: std::sync::atomic::AtomicU64,
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
async fn route_decoded_samples(
    state: &IngestState,
    samples: Vec<crate::drivers::ingest::prometheus_remote_write::DecodedSample>,
    ingest_received_at: Instant,
) -> StatusCode {
    if samples.is_empty() {
        return StatusCode::NO_CONTENT;
    }

    let count = samples.len() as u64;
    state
        .samples_ingested
        .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

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
                continue;
            }
            let group_key = extract_group_key(&s.labels, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((s.labels.clone(), s.timestamp_ms, s.value));
        }
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
    let ingest_received_at = Instant::now();
    let samples = match decode_prometheus_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode Prometheus remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(&state, samples, ingest_received_at).await
}

/// Axum handler for VictoriaMetrics remote write (Zstd + Protobuf).
pub(crate) async fn handle_victoriametrics_ingest(
    State(state): State<Arc<IngestState>>,
    body: Bytes,
) -> StatusCode {
    let ingest_received_at = Instant::now();
    let samples = match decode_victoriametrics_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode VictoriaMetrics remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(&state, samples, ingest_received_at).await
}
