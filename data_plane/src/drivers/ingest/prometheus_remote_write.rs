//! Prometheus Remote Write v1 adapter for the backend-local precompute path.
//!
//! This module deliberately stops at wire validation, canonical series
//! identity, retry deduplication, and config-driven routing. Aggregation-family
//! selection remains owned by the installed precompute plan.

use crate::precompute_engine::ingest_handler::IngestState;
use crate::precompute_engine::series_router::{TryRouteError, WorkerMessage};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<OpaqueMessage>,
    #[prost(message, repeated, tag = "4")]
    pub histograms: Vec<OpaqueMessage>,
}

/// Presence-only decoder for unsupported non-scalar v1 payloads. Prost skips
/// the nested fields while preserving whether such a payload was supplied.
#[derive(Clone, PartialEq, Message)]
pub struct OpaqueMessage {}

#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct PrometheusRemoteWriteConfig {
    pub max_compressed_bytes: usize,
    pub max_decompressed_bytes: usize,
    pub max_timeseries: usize,
    pub max_samples: usize,
    pub dedup_horizon: Duration,
    pub max_dedup_entries: usize,
}

impl Default for PrometheusRemoteWriteConfig {
    fn default() -> Self {
        Self {
            max_compressed_bytes: 32 * 1024 * 1024,
            max_decompressed_bytes: 128 * 1024 * 1024,
            max_timeseries: 100_000,
            max_samples: 1_000_000,
            dedup_horizon: Duration::from_secs(10 * 60),
            max_dedup_entries: 2_000_000,
        }
    }
}

#[derive(Debug, Default)]
pub struct RemoteWriteStats {
    pub requests: AtomicU64,
    pub samples: AtomicU64,
    pub stale_markers: AtomicU64,
    pub duplicates: AtomicU64,
    pub rejected_requests: AtomicU64,
    pub bytes: AtomicU64,
}

#[derive(Clone)]
pub struct PrometheusRemoteWriteReceiver {
    inner: Arc<ReceiverInner>,
}

struct ReceiverInner {
    config: PrometheusRemoteWriteConfig,
    ingest: Arc<IngestState>,
    dedup: Mutex<DedupState>,
    stats: Arc<RemoteWriteStats>,
}

#[derive(Default)]
struct DedupState {
    input_closed: bool,
    values: HashMap<(u64, u64, Arc<str>, i64), DedupValue>,
    expiry_by_event_time: BTreeMap<i64, Vec<(u64, u64, Arc<str>)>>,
    max_event_timestamp_ms: Option<i64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DedupValue {
    Number(u64),
    Stale,
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteWriteError {
    #[error("finite input has been closed by the drain barrier")]
    InputClosed,
    #[error("compressed request exceeds {0} bytes")]
    CompressedTooLarge(usize),
    #[error("decompressed request exceeds {0} bytes")]
    DecompressedTooLarge(usize),
    #[error("snappy decompression failed: {0}")]
    Snappy(String),
    #[error("protobuf decoding failed: {0}")]
    Protobuf(String),
    #[error("request has {actual} time series; limit is {limit}")]
    TooManySeries { actual: usize, limit: usize },
    #[error("request has more than {0} samples")]
    TooManySamples(usize),
    #[error("invalid series: {0}")]
    InvalidSeries(String),
    #[error("invalid sample for {series} at {timestamp}: {reason}")]
    InvalidSample {
        series: String,
        timestamp: i64,
        reason: String,
    },
    #[error("conflicting sample for {series} at timestamp {timestamp}")]
    Conflict { series: String, timestamp: i64 },
    #[error("deduplication capacity ({0}) is exhausted")]
    DedupCapacity(usize),
    #[error("Remote Write requires an active PrometheusRemoteWriteV1 PhysicalPlan")]
    InactivePhysicalPlan,
    #[error(transparent)]
    Backpressure(#[from] TryRouteError),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalSample {
    pub metric: Arc<str>,
    pub labels: Arc<HashMap<String, String>>,
    pub series_key: Arc<str>,
    population_key: Arc<str>,
    all_attrs_fingerprint: Arc<str>,
    pub timestamp_ms: i64,
    pub value: Option<f64>,
}

impl PrometheusRemoteWriteReceiver {
    pub fn new(config: PrometheusRemoteWriteConfig, ingest: Arc<IngestState>) -> Self {
        Self {
            inner: Arc::new(ReceiverInner {
                config,
                ingest,
                dedup: Mutex::new(DedupState::default()),
                stats: Arc::new(RemoteWriteStats::default()),
            }),
        }
    }

    pub fn config(&self) -> &PrometheusRemoteWriteConfig {
        &self.inner.config
    }

    pub fn stats(&self) -> Arc<RemoteWriteStats> {
        self.inner.stats.clone()
    }

    /// Permanently seal this finite source before queuing worker barriers.
    pub async fn drain(&self) -> Result<(), String> {
        {
            let mut state = self.inner.dedup.lock().map_err(|e| e.to_string())?;
            state.input_closed = true;
            // No write or retry is admissible after the finite-input barrier,
            // so retry identities have no remaining correctness role during
            // the query phase. Release them before waiting for materialization.
            state.values.clear();
            state.values.shrink_to_fit();
            state.expiry_by_event_time.clear();
            state.max_event_timestamp_ms = None;
        }
        self.inner.ingest.router.drain().await?;
        trim_process_allocator();
        Ok(())
    }

    /// Decode, validate, deduplicate, and enqueue one whole v1 request.
    /// All validation and all queue reservations complete before any message
    /// becomes visible to a worker.
    pub fn accept(&self, body: &[u8]) -> Result<(), RemoteWriteError> {
        self.inner.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.inner
            .stats
            .bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        let result = self.accept_inner(body);
        if result.is_err() {
            self.inner
                .stats
                .rejected_requests
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn accept_inner(&self, body: &[u8]) -> Result<(), RemoteWriteError> {
        let config = &self.inner.config;
        if body.len() > config.max_compressed_bytes {
            return Err(RemoteWriteError::CompressedTooLarge(
                config.max_compressed_bytes,
            ));
        }
        let decoded_len = snap::raw::decompress_len(body)
            .map_err(|error| RemoteWriteError::Snappy(error.to_string()))?;
        if decoded_len > config.max_decompressed_bytes {
            return Err(RemoteWriteError::DecompressedTooLarge(
                config.max_decompressed_bytes,
            ));
        }
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(body)
            .map_err(|error| RemoteWriteError::Snappy(error.to_string()))?;
        let request = WriteRequest::decode(decoded.as_slice())
            .map_err(|error| RemoteWriteError::Protobuf(error.to_string()))?;
        let samples = canonicalize_request(&request, config)?;
        let physical_plan = self
            .inner
            .ingest
            .physical_plan_snapshot()
            .ok_or(RemoteWriteError::InactivePhysicalPlan)?;
        if physical_plan.precompute_plan.envelope.plan_id == 0
            || !matches!(
                physical_plan.precompute_plan.ingest.protocol,
                asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
            )
            || physical_plan.precompute_plan.ingest.endpoint_path != "/api/v1/write"
        {
            return Err(RemoteWriteError::InactivePhysicalPlan);
        }
        let plan_identity = (
            physical_plan.precompute_plan.envelope.plan_id,
            physical_plan.precompute_plan.envelope.plan_version,
        );

        let mut dedup = self
            .inner
            .dedup
            .lock()
            .expect("remote write dedup poisoned");
        if dedup.input_closed {
            return Err(RemoteWriteError::InputClosed);
        }
        let batch_max_timestamp_ms = samples.iter().map(|sample| sample.timestamp_ms).max();
        let max_event_timestamp_ms = match (dedup.max_event_timestamp_ms, batch_max_timestamp_ms) {
            (Some(current), Some(batch)) => Some(current.max(batch)),
            (current, batch) => current.or(batch),
        };
        let horizon_ms = i64::try_from(config.dedup_horizon.as_millis()).unwrap_or(i64::MAX);
        let retention_cutoff_ms = max_event_timestamp_ms
            .map(|timestamp| timestamp.saturating_sub(horizon_ms))
            .unwrap_or(i64::MIN);

        // Validate conflicts both against committed history and inside this
        // request before reserving any worker capacity.
        let mut batch_values: HashMap<(u64, u64, Arc<str>, i64), DedupValue> = HashMap::new();
        let mut new_samples = Vec::with_capacity(samples.len());
        let mut duplicates = 0u64;
        for sample in samples {
            let key = (
                plan_identity.0,
                plan_identity.1,
                sample.series_key.clone(),
                sample.timestamp_ms,
            );
            let value = sample
                .value
                .map(|v| DedupValue::Number(v.to_bits()))
                .unwrap_or(DedupValue::Stale);
            let prior = batch_values.get(&key).or_else(|| {
                (sample.timestamp_ms >= retention_cutoff_ms)
                    .then(|| dedup.values.get(&key))
                    .flatten()
            });
            match prior {
                Some(previous) if *previous == value => {
                    duplicates += 1;
                }
                Some(_) => {
                    return Err(RemoteWriteError::Conflict {
                        series: sample.series_key.to_string(),
                        timestamp: sample.timestamp_ms,
                    });
                }
                None => {
                    batch_values.insert(key, value);
                    new_samples.push(sample);
                }
            }
        }
        let retained_existing = dedup
            .values
            .keys()
            .filter(|key| key.3 >= retention_cutoff_ms)
            .count();
        let retained_batch = batch_values
            .keys()
            .filter(|key| key.3 >= retention_cutoff_ms)
            .count();
        if retained_existing.saturating_add(retained_batch) > config.max_dedup_entries {
            return Err(RemoteWriteError::DedupCapacity(config.max_dedup_entries));
        }

        let messages = route_messages(&new_samples, &self.inner.ingest, &physical_plan);
        self.inner
            .ingest
            .router
            .try_route_group_batch_atomic(messages)?;

        // Commit dedup mutation only after the entire routed batch was
        // reserved successfully. A rejected/backpressured request must not
        // advance event time or erase retry history.
        dedup.max_event_timestamp_ms = max_event_timestamp_ms;
        dedup.evict_event_times_before(retention_cutoff_ms);
        for ((plan_id, plan_version, series, timestamp), value) in batch_values {
            if timestamp < retention_cutoff_ms {
                continue;
            }
            dedup
                .values
                .insert((plan_id, plan_version, series.clone(), timestamp), value);
            dedup
                .expiry_by_event_time
                .entry(timestamp)
                .or_default()
                .push((plan_id, plan_version, series));
        }
        let stale_count = new_samples
            .iter()
            .filter(|sample| sample.value.is_none())
            .count() as u64;
        self.inner.stats.samples.fetch_add(
            (new_samples.len() as u64).saturating_sub(stale_count),
            Ordering::Relaxed,
        );
        self.inner
            .stats
            .stale_markers
            .fetch_add(stale_count, Ordering::Relaxed);
        self.inner
            .stats
            .duplicates
            .fetch_add(duplicates, Ordering::Relaxed);
        self.inner
            .ingest
            .samples_ingested
            .fetch_add(new_samples.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_process_allocator() {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    // The input barrier has drained all worker-owned batches and permanently
    // rejects future writes, so pages released by the dedup map and ingest
    // buffers are no longer reachable by application code.
    unsafe {
        malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_process_allocator() {}

impl DedupState {
    fn evict_event_times_before(&mut self, cutoff_ms: i64) {
        let expired_timestamps = self
            .expiry_by_event_time
            .range(..cutoff_ms)
            .map(|(timestamp, _)| *timestamp)
            .collect::<Vec<_>>();
        for timestamp in expired_timestamps {
            if let Some(entries) = self.expiry_by_event_time.remove(&timestamp) {
                for (plan_id, plan_version, series) in entries {
                    self.values
                        .remove(&(plan_id, plan_version, series, timestamp));
                }
            }
        }
    }
}

fn canonicalize_request(
    request: &WriteRequest,
    config: &PrometheusRemoteWriteConfig,
) -> Result<Vec<CanonicalSample>, RemoteWriteError> {
    if request.timeseries.len() > config.max_timeseries {
        return Err(RemoteWriteError::TooManySeries {
            actual: request.timeseries.len(),
            limit: config.max_timeseries,
        });
    }
    let mut out = Vec::new();
    for timeseries in &request.timeseries {
        if !timeseries.exemplars.is_empty() || !timeseries.histograms.is_empty() {
            return Err(RemoteWriteError::InvalidSeries(
                "exemplars and native histograms are outside compatibility level 1".into(),
            ));
        }
        if out.len().saturating_add(timeseries.samples.len()) > config.max_samples {
            return Err(RemoteWriteError::TooManySamples(config.max_samples));
        }
        let (metric, labels, series_key) = canonicalize_labels(&timeseries.labels)?;
        let metric: Arc<str> = metric.into();
        let labels = Arc::new(labels);
        let series_key: Arc<str> = series_key.into();
        let mut sorted_attrs = labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        sorted_attrs.sort_unstable();
        let all_attrs_fingerprint: Arc<str> =
            super::canonical_attrs_fingerprint(&sorted_attrs).into();
        let population_labels = sorted_attrs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>();
        let population_key: Arc<str> = format!(
            "__asap_population__{}",
            serde_json::to_string(&population_labels).expect("label map serialization")
        )
        .into();
        for sample in &timeseries.samples {
            let value = if sample.value.to_bits() == STALE_NAN_BITS {
                None
            } else if !sample.value.is_finite() {
                return Err(RemoteWriteError::InvalidSample {
                    series: series_key.to_string(),
                    timestamp: sample.timestamp,
                    reason: "only finite values or the Prometheus stale marker are accepted".into(),
                });
            } else {
                Some(sample.value)
            };
            out.push(CanonicalSample {
                metric: metric.clone(),
                labels: labels.clone(),
                series_key: series_key.clone(),
                population_key: population_key.clone(),
                all_attrs_fingerprint: all_attrs_fingerprint.clone(),
                timestamp_ms: sample.timestamp,
                value,
            });
        }
    }
    Ok(out)
}

fn canonicalize_labels(
    labels: &[Label],
) -> Result<(String, HashMap<String, String>, String), RemoteWriteError> {
    let mut metric = None;
    let mut attrs = HashMap::new();
    for label in labels {
        if !valid_label_name(&label.name) {
            return Err(RemoteWriteError::InvalidSeries(format!(
                "invalid label name {:?}",
                label.name
            )));
        }
        if label.name == "__name__" {
            if metric.replace(label.value.clone()).is_some() {
                return Err(RemoteWriteError::InvalidSeries(
                    "duplicate __name__ label".into(),
                ));
            }
        } else if attrs
            .insert(label.name.clone(), label.value.clone())
            .is_some()
        {
            return Err(RemoteWriteError::InvalidSeries(format!(
                "duplicate label {:?}",
                label.name
            )));
        }
    }
    let metric =
        metric.ok_or_else(|| RemoteWriteError::InvalidSeries("missing __name__".into()))?;
    if !valid_metric_name(&metric) {
        return Err(RemoteWriteError::InvalidSeries(format!(
            "invalid metric name {metric:?}"
        )));
    }
    let mut sorted: Vec<_> = attrs.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let series_key = if sorted.is_empty() {
        metric.clone()
    } else {
        let rendered = sorted
            .into_iter()
            .map(|(name, value)| format!("{name}=\"{}\"", escape_label_value(value)))
            .collect::<Vec<_>>()
            .join(",");
        format!("{metric}{{{rendered}}}")
    };
    Ok((metric, attrs, series_key))
}

fn route_messages(
    samples: &[CanonicalSample],
    ingest: &Arc<IngestState>,
    physical_plan: &crate::storage_engines::types::ActivePhysicalPlan,
) -> Vec<WorkerMessage> {
    type Bucket = (
        u64,
        asap_types::PolicyFingerprint,
        Arc<crate::precompute_engine::group_key::GroupKey>,
    );
    type RoutedSample = (String, i64, f64);
    let snapshot = physical_plan.runtime_config.clone();
    let _ = crate::storage_engines::sketch_db::lifecycle::reconcile_if_config_changed(
        ingest.sketch_index.as_ref(),
        &snapshot,
        crate::storage_engines::sketch_db::DEFAULT_RETIREMENT_RETENTION,
    );
    let configs = snapshot
        .get_all_aggregation_configs()
        .values()
        .filter_map(|config| {
            compile_spatial_filter(&config.spatial_filter_normalized)
                .ok()
                .map(|filter| (config, filter))
        })
        .collect::<Vec<_>>();
    let mut buckets: HashMap<u64, (Bucket, Vec<RoutedSample>)> = HashMap::new();
    for sample in samples {
        let Some(value) = sample.value else {
            // Staleness is a lifecycle signal, never an accumulator input.
            continue;
        };
        for (config, filter) in &configs {
            if config.metric.as_str() != sample.metric.as_ref() || !filter.matches(&sample.labels) {
                continue;
            }
            let group_key = IngestState::extract_group_key_for(&sample.series_key, config);
            // Reset-aware counters and temporal min/max must keep one
            // accumulator per source series. Their emitted label values still
            // follow the physical grouping, so query-time Reduce nodes can
            // combine those independent SDS instances safely.
            let series_scoped = config.partitioning
                == Some(asap_types::sds::PopulationPartitioning::PerEntity)
                || (config.partitioning.is_none()
                    && matches!(
                        config.aggregation_type,
                        asap_types::AggregationType::Increase
                            | asap_types::AggregationType::MultipleIncrease
                            | asap_types::AggregationType::MinMax
                            | asap_types::AggregationType::MultipleMinMax
                    ));
            let grouping_pairs: Vec<(&str, &str)> = if series_scoped {
                Vec::new()
            } else {
                config
                    .grouping_labels
                    .labels
                    .iter()
                    .map(|name| {
                        (
                            name.as_str(),
                            sample.labels.get(name).map(String::as_str).unwrap_or(""),
                        )
                    })
                    .collect()
            };
            let group_key = if series_scoped {
                let mut names = sample.labels.keys().map(String::as_str).collect::<Vec<_>>();
                names.sort_unstable();
                crate::precompute_engine::group_key::intern_pairs(names.into_iter().map(|name| {
                    (
                        name,
                        sample.labels.get(name).map(String::as_str).unwrap_or(""),
                    )
                }))
            } else {
                group_key
            };
            let computed_attrs_fp;
            let attrs_fp = if series_scoped {
                sample.all_attrs_fingerprint.as_ref()
            } else {
                computed_attrs_fp = super::canonical_attrs_fingerprint(&grouping_pairs);
                &computed_attrs_fp
            };
            let policy_fp = asap_types::PolicyFingerprint(config.policy_fp_u64());
            // A sketch family is not a complete physical identity. Two
            // materializations may use the same family and grouping while
            // differing in update semantics (for example count- versus
            // value-weighted Top-K). Keep those states on distinct SIDs.
            let materialization_kind =
                crate::storage_engines::sketch_db::data::materialization_kind_for_config(config);
            let sid =
                ingest
                    .series_resolver
                    .resolve(&config.metric, attrs_fp, &materialization_kind);
            buckets
                .entry(sid)
                .or_insert_with(|| ((sid, policy_fp, group_key), Vec::new()))
                .1
                .push((sample.series_key.to_string(), sample.timestamp_ms, value));
        }
    }
    let received_at = Instant::now();
    buckets
        .into_values()
        .map(
            |((sid, policy_fp, group_key), samples)| WorkerMessage::GroupSamples {
                sid,
                policy_fp,
                group_key,
                samples,
                ingest_received_at: received_at,
            },
        )
        .collect()
}

#[derive(Debug)]
enum CompiledLabelMatcher {
    Equal(String, String),
    NotEqual(String, String),
    Regex(String, regex::Regex),
    NotRegex(String, regex::Regex),
}

#[derive(Debug, Default)]
struct CompiledSpatialFilter(Vec<CompiledLabelMatcher>);

impl CompiledSpatialFilter {
    fn matches(&self, labels: &HashMap<String, String>) -> bool {
        self.0.iter().all(|matcher| {
            let (name, expected, negate) = match matcher {
                CompiledLabelMatcher::Equal(name, value) => {
                    return labels.get(name).map(String::as_str).unwrap_or("") == value
                }
                CompiledLabelMatcher::NotEqual(name, value) => {
                    return labels.get(name).map(String::as_str).unwrap_or("") != value
                }
                CompiledLabelMatcher::Regex(name, regex) => (name, regex, false),
                CompiledLabelMatcher::NotRegex(name, regex) => (name, regex, true),
            };
            let matched = expected.is_match(labels.get(name).map(String::as_str).unwrap_or(""));
            matched != negate
        })
    }
}

fn compile_spatial_filter(filter: &str) -> Result<CompiledSpatialFilter, String> {
    let body = filter.trim().trim_start_matches('{').trim_end_matches('}');
    if body.trim().is_empty() {
        return Ok(CompiledSpatialFilter::default());
    }
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, ch) in body.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' && quoted {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        } else if ch == ',' && !quoted {
            parts.push(body[start..index].trim());
            start = index + 1;
        }
    }
    if quoted {
        return Err("unterminated spatial-filter string".into());
    }
    parts.push(body[start..].trim());
    let mut compiled = Vec::with_capacity(parts.len());
    for part in parts {
        let (name, operator, raw_value) = ["!~", "=~", "!=", "="]
            .into_iter()
            .find_map(|operator| {
                part.find(operator).map(|at| {
                    (
                        part[..at].trim(),
                        operator,
                        part[at + operator.len()..].trim(),
                    )
                })
            })
            .ok_or_else(|| format!("invalid spatial matcher {part:?}"))?;
        if name.is_empty() {
            return Err("spatial matcher has an empty label name".into());
        }
        let value: String = serde_json::from_str(raw_value)
            .map_err(|error| format!("invalid spatial matcher value: {error}"))?;
        compiled.push(match operator {
            "=" => CompiledLabelMatcher::Equal(name.into(), value),
            "!=" => CompiledLabelMatcher::NotEqual(name.into(), value),
            "=~" | "!~" => {
                let regex = regex::Regex::new(&format!("^(?:{value})$"))
                    .map_err(|error| format!("invalid spatial matcher regex: {error}"))?;
                if operator == "=~" {
                    CompiledLabelMatcher::Regex(name.into(), regex)
                } else {
                    CompiledLabelMatcher::NotRegex(name.into(), regex)
                }
            }
            _ => unreachable!(),
        });
    }
    Ok(CompiledSpatialFilter(compiled))
}

fn valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'a'..='z' | 'A'..='Z'))
        && chars.all(|ch| matches!(ch, '_' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

fn valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | ':' | 'a'..='z' | 'A'..='Z'))
        && chars.all(|ch| matches!(ch, '_' | ':' | 'a'..='z' | 'A'..='Z' | '0'..='9'))
}

fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_engine::ingest_handler::IngestObservability;
    use crate::precompute_engine::series_router::SeriesRouter;
    use crate::storage_engines::types::{
        ActivePhysicalPlan, BackendStorageRouting, HotReloadActivePhysicalPlan,
        HotReloadStreamingConfig, StreamingConfig,
    };
    use tokio::sync::mpsc;

    fn compressed(request: WriteRequest) -> Vec<u8> {
        snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .unwrap()
    }

    fn physical_config(streaming: StreamingConfig) -> HotReloadStreamingConfig {
        use control_plane::physical::compiler::{
            FrameIdentityContract, IngestContract, IngestProtocol, PlanEnvelope, PrecomputePlan,
            SequenceScope, TimestampUnit, TransmissionPlan, PLANNER_REVISION,
        };
        let envelope = PlanEnvelope {
            plan_id: 7,
            plan_version: 3,
            generated_at_unix_ms: 1,
            activation_unix_ms: 1,
            expiry_unix_ms: None,
            backend_compat: asap_types::precompute_plan::BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "test".into(),
        };
        let active = ActivePhysicalPlan {
            envelope: envelope.clone(),
            summary_catalog: None,
            precompute_plan: PrecomputePlan {
                summary_catalog: None,
                envelope: envelope.clone(),
                ingest: IngestContract {
                    protocol: IngestProtocol::PrometheusRemoteWriteV1,
                    endpoint_path: "/api/v1/write".into(),
                    timestamp_unit: TimestampUnit::UnixMilliseconds,
                    require_plan_identity: false,
                    require_summary_definition_identity: false,
                    require_registered_producer: false,
                },
                schemas: Vec::new(),
                producers: Vec::new(),
                executable_dags: Default::default(),
                materializations: streaming.aggregation_configs.values().cloned().collect(),
            },
            transmission_plan: TransmissionPlan {
                summary_catalog: None,
                envelope,
                frame_identity: FrameIdentityContract {
                    identity_version: 1,
                    sequence_scope: SequenceScope::MaterializationSeriesProducerEpoch,
                    require_checkpoint_for_full: true,
                    require_base_checkpoint_for_delta: true,
                },
                rules: Vec::new(),
            },
            runtime_config: Arc::new(streaming),
            query_plan: Arc::new(control_plane::query_plan::QueryPlan::empty()),
            storage_routing: Arc::new(BackendStorageRouting::empty()),
        };
        HotReloadStreamingConfig::from_active(HotReloadActivePhysicalPlan::new(active))
    }

    fn receiver(config: PrometheusRemoteWriteConfig) -> PrometheusRemoteWriteReceiver {
        let (sender, _receiver) = mpsc::channel(8);
        let ingest = Arc::new(IngestState {
            router: SeriesRouter::new(vec![sender]),
            samples_ingested: AtomicU64::new(0),
            samples_blocked_by_schema_barrier: AtomicU64::new(0),
            hot_reload_config: physical_config(StreamingConfig::default()),
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(super::super::SeriesIdResolver::new()),
            sketch_index: Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            observability: IngestObservability::default(),
        });
        PrometheusRemoteWriteReceiver::new(config, ingest)
    }

    fn configured_receiver() -> (PrometheusRemoteWriteReceiver, mpsc::Receiver<WorkerMessage>) {
        use asap_types::enums::WindowKind;
        use asap_types::{AggregationConfig, AggregationType, KeyByLabelNames};
        let aggregation = AggregationConfig {
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(vec!["job".into()]),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 60,
            slide_interval: 60,
            window_type: WindowKind::Tumbling,
            window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 60 },
            pane_origin_ms: None,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "requests_total".into(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_column: None,
            table_population: None,
            table_timestamp_column: None,
            partitioning: None,
        };
        let policy_fp = aggregation.policy_fp_u64();
        let streaming = StreamingConfig::new(HashMap::from([(policy_fp, aggregation)]));
        let (sender, receiver) = mpsc::channel(8);
        let ingest = Arc::new(IngestState {
            router: SeriesRouter::new(vec![sender]),
            samples_ingested: AtomicU64::new(0),
            samples_blocked_by_schema_barrier: AtomicU64::new(0),
            hot_reload_config: physical_config(streaming),
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(super::super::SeriesIdResolver::new()),
            sketch_index: Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            observability: IngestObservability::default(),
        });
        (
            PrometheusRemoteWriteReceiver::new(PrometheusRemoteWriteConfig::default(), ingest),
            receiver,
        )
    }

    #[test]
    fn global_topk_cms_routes_once_while_counters_remain_per_series() {
        use asap_types::enums::WindowKind;
        use asap_types::{AggregationConfig, AggregationType, KeyByLabelNames};

        let config =
            |aggregation_type, grouping: Vec<String>, aggregated: Vec<String>| AggregationConfig {
                aggregation_type,
                aggregation_sub_type: String::new(),
                parameters: match aggregation_type {
                    AggregationType::CountMinSketchWithHeap => HashMap::from([
                        ("width".into(), serde_json::json!(128)),
                        ("depth".into(), serde_json::json!(5)),
                        ("heap_size".into(), serde_json::json!(2)),
                    ]),
                    _ => HashMap::new(),
                },
                grouping_labels: KeyByLabelNames::new(grouping),
                aggregated_labels: KeyByLabelNames::new(aggregated),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size: 60,
                slide_interval: 60,
                window_type: WindowKind::Tumbling,
                window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 60 },
                pane_origin_ms: Some(0),
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "cpu_seconds_total".into(),
                num_aggregates_to_retain: Some(80),
                table_name: None,
                value_column: None,
                table_population: None,
                table_timestamp_column: None,
                partitioning: None,
            };
        let cms = config(
            AggregationType::CountMinSketchWithHeap,
            vec![],
            vec!["job".into()],
        );
        let counter = config(AggregationType::Increase, vec!["job".into()], vec![]);
        let mut kll = config(AggregationType::DatasketchesKLL, vec![], vec![]);
        kll.partitioning = Some(asap_types::sds::PopulationPartitioning::PerEntity);
        let kll_fp = kll.policy_fingerprint();
        let mut pooled_kll = kll.clone();
        pooled_kll.partitioning = Some(asap_types::sds::PopulationPartitioning::Grouped);
        let pooled_kll_fp = pooled_kll.policy_fingerprint();
        assert_ne!(kll_fp, pooled_kll_fp);
        let cms_fp = cms.policy_fingerprint();
        let counter_fp = counter.policy_fingerprint();
        let streaming = StreamingConfig::new(HashMap::from([
            (cms_fp.0, cms),
            (counter_fp.0, counter),
            (kll_fp.0, kll),
            (pooled_kll_fp.0, pooled_kll),
        ]));
        let hot_reload = physical_config(streaming);
        let physical_plan = hot_reload.physical_plan_snapshot().unwrap();
        let (sender, _worker) = mpsc::channel(8);
        let ingest = Arc::new(IngestState {
            router: SeriesRouter::new(vec![sender]),
            samples_ingested: AtomicU64::new(0),
            samples_blocked_by_schema_barrier: AtomicU64::new(0),
            hot_reload_config: hot_reload,
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(super::super::SeriesIdResolver::new()),
            sketch_index: Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            observability: IngestObservability::default(),
        });
        let request = WriteRequest {
            timeseries: ["api", "order", "payment", "user", "webapp"]
                .into_iter()
                .flat_map(|job| {
                    ["a", "b"].into_iter().map(move |instance| TimeSeries {
                        labels: vec![
                            Label {
                                name: "__name__".into(),
                                value: "cpu_seconds_total".into(),
                            },
                            Label {
                                name: "job".into(),
                                value: job.into(),
                            },
                            Label {
                                name: "instance".into(),
                                value: instance.into(),
                            },
                        ],
                        samples: vec![Sample {
                            value: 1.0,
                            timestamp: 1_000,
                        }],
                        exemplars: Vec::new(),
                        histograms: Vec::new(),
                    })
                })
                .collect(),
        };
        let samples = canonicalize_request(&request, &PrometheusRemoteWriteConfig::default())
            .expect("canonical samples");
        let messages = route_messages(&samples, &ingest, &physical_plan);
        let mut cms_buckets = 0;
        let mut counter_buckets = 0;
        let mut cms_samples = 0;
        let mut kll_buckets = 0;
        let mut pooled_kll_buckets = 0;
        for message in messages {
            let WorkerMessage::GroupSamples {
                policy_fp, samples, ..
            } = message
            else {
                panic!("route emits only group samples")
            };
            if policy_fp == cms_fp {
                cms_buckets += 1;
                cms_samples += samples.len();
            } else if policy_fp == counter_fp {
                counter_buckets += 1;
            } else if policy_fp == kll_fp {
                kll_buckets += 1;
            } else if policy_fp == pooled_kll_fp {
                pooled_kll_buckets += 1;
            }
        }
        assert_eq!(kll_buckets, 10, "PerEntity KLL keeps every source series");
        assert_eq!(
            pooled_kll_buckets, 1,
            "Grouped empty keys intentionally pool"
        );
        assert_eq!(cms_buckets, 1, "Reduce([]) has one global CMS SID");
        assert_eq!(cms_samples, 10, "global CMS receives every source series");
        assert_eq!(
            counter_buckets, 10,
            "reset-aware counters remain series scoped"
        );
    }

    fn one_sample(value: f64) -> Vec<u8> {
        compressed(WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "requests_total".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![Sample {
                    value,
                    timestamp: 100,
                }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }],
        })
    }

    #[test]
    fn spatial_filter_matches_prometheus_label_semantics() {
        let labels = HashMap::from([
            ("job".to_string(), "order-service".to_string()),
            ("status".to_string(), "503".to_string()),
        ]);
        assert!(
            compile_spatial_filter(r#"{job="order-service",status=~"5.."}"#)
                .unwrap()
                .matches(&labels)
        );
        assert!(!compile_spatial_filter(r#"{job!="order-service"}"#)
            .unwrap()
            .matches(&labels));
        assert!(compile_spatial_filter(r#"{missing!="present"}"#)
            .unwrap()
            .matches(&labels));
        assert!(!compile_spatial_filter(r#"{status!~"5.."}"#)
            .unwrap()
            .matches(&labels));
    }

    // Closing finite input prevents writes racing behind the completion barrier.
    #[tokio::test]
    async fn finite_input_drain_seals_receiver_and_propagates_worker_failure() {
        let (receiver, mut worker) = configured_receiver();
        receiver.accept(&one_sample(1.0)).unwrap();
        let handle = receiver.clone();
        let drain = tokio::spawn(async move { handle.drain().await });
        assert!(matches!(
            worker.recv().await.unwrap(),
            WorkerMessage::GroupSamples { .. }
        ));
        let WorkerMessage::Drain(reply) = worker.recv().await.unwrap() else {
            panic!("expected barrier")
        };
        assert!(matches!(
            receiver.accept(&one_sample(1.0)),
            Err(RemoteWriteError::InputClosed)
        ));
        reply.send(Err("sink write failed".into())).unwrap();
        assert_eq!(drain.await.unwrap().unwrap_err(), "sink write failed");
    }

    #[test]
    fn rejects_writes_without_an_active_physical_plan() {
        let (sender, _worker) = mpsc::channel(1);
        let ingest = Arc::new(IngestState {
            router: SeriesRouter::new(vec![sender]),
            samples_ingested: AtomicU64::new(0),
            samples_blocked_by_schema_barrier: AtomicU64::new(0),
            hot_reload_config: HotReloadStreamingConfig::new(StreamingConfig::default()),
            pass_raw_samples: false,
            sketch_snapshots: dashmap::DashMap::new(),
            series_resolver: Arc::new(super::super::SeriesIdResolver::new()),
            sketch_index: Arc::new(crate::storage_engines::sketch_db::index::SketchStore::new()),
            observability: IngestObservability::default(),
        });
        let receiver = PrometheusRemoteWriteReceiver::new(Default::default(), ingest);
        assert!(matches!(
            receiver.accept(&one_sample(1.0)),
            Err(RemoteWriteError::InactivePhysicalPlan)
        ));
    }

    #[test]
    fn canonicalizes_labels_and_recognizes_staleness() {
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "zone".into(),
                        value: "a\n\"b".into(),
                    },
                    Label {
                        name: "__name__".into(),
                        value: "http_requests_total".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![
                    Sample {
                        value: 3.0,
                        timestamp: 100,
                    },
                    Sample {
                        value: f64::from_bits(STALE_NAN_BITS),
                        timestamp: 200,
                    },
                ],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }],
        };
        let decoded = snap::raw::Decoder::new()
            .decompress_vec(&compressed(request))
            .unwrap();
        let request = WriteRequest::decode(decoded.as_slice()).unwrap();
        let samples =
            canonicalize_request(&request, &PrometheusRemoteWriteConfig::default()).unwrap();
        assert_eq!(
            samples[0].series_key.as_ref(),
            "http_requests_total{job=\"api\",zone=\"a\\n\\\"b\"}"
        );
        assert_eq!(samples[0].value, Some(3.0));
        assert_eq!(samples[1].value, None);
        assert!(Arc::ptr_eq(&samples[0].metric, &samples[1].metric));
        assert!(Arc::ptr_eq(&samples[0].labels, &samples[1].labels));
        assert!(Arc::ptr_eq(&samples[0].series_key, &samples[1].series_key));
    }

    #[test]
    fn rejects_non_stale_nan_and_conflicting_labels() {
        let config = PrometheusRemoteWriteConfig::default();
        let invalid_nan = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".into(),
                    value: "m".into(),
                }],
                samples: vec![Sample {
                    value: f64::NAN,
                    timestamp: 1,
                }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }],
        };
        assert!(matches!(
            canonicalize_request(&invalid_nan, &config),
            Err(RemoteWriteError::InvalidSample { .. })
        ));
        assert!(canonicalize_labels(&[
            Label {
                name: "__name__".into(),
                value: "m".into()
            },
            Label {
                name: "job".into(),
                value: "a".into()
            },
            Label {
                name: "job".into(),
                value: "b".into()
            },
        ])
        .is_err());
    }

    #[test]
    fn resource_limits_apply_before_allocation() {
        let bytes = compressed(WriteRequest {
            timeseries: Vec::new(),
        });
        let mut config = PrometheusRemoteWriteConfig::default();
        config.max_compressed_bytes = bytes.len() - 1;
        assert!(matches!(
            receiver(config).accept(&bytes),
            Err(RemoteWriteError::CompressedTooLarge(_))
        ));
    }

    #[test]
    fn replay_is_a_noop_and_conflict_is_rejected() {
        let receiver = receiver(PrometheusRemoteWriteConfig::default());
        receiver.accept(&one_sample(4.0)).unwrap();
        receiver.accept(&one_sample(4.0)).unwrap();
        assert_eq!(receiver.stats().samples.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.stats().duplicates.load(Ordering::Relaxed), 1);
        assert!(matches!(
            receiver.accept(&one_sample(5.0)),
            Err(RemoteWriteError::Conflict { .. })
        ));
        assert_eq!(receiver.stats().samples.load(Ordering::Relaxed), 1);
        assert_eq!(
            receiver.stats().rejected_requests.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn dedup_history_is_evicted_by_event_time_during_fast_replay() {
        let mut state = DedupState::default();
        for timestamp in [0, 60_000, 600_000, 660_000] {
            let series: Arc<str> = format!("series-{timestamp}").into();
            state
                .values
                .insert((7, 3, series.clone(), timestamp), DedupValue::Number(1));
            state
                .expiry_by_event_time
                .entry(timestamp)
                .or_default()
                .push((7, 3, series));
        }
        state.evict_event_times_before(600_000);
        assert_eq!(state.values.len(), 2);
        assert!(state.values.keys().all(|key| key.3 >= 600_000));
    }

    #[test]
    fn stale_marker_is_deduplicated_but_never_counted_as_numeric() {
        let receiver = receiver(PrometheusRemoteWriteConfig::default());
        let stale = one_sample(f64::from_bits(STALE_NAN_BITS));
        receiver.accept(&stale).unwrap();
        receiver.accept(&stale).unwrap();
        assert_eq!(receiver.stats().samples.load(Ordering::Relaxed), 0);
        assert_eq!(receiver.stats().stale_markers.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.stats().duplicates.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn valid_request_routes_canonical_sample_to_installed_plan() {
        let (receiver, mut worker) = configured_receiver();
        receiver.accept(&one_sample(4.0)).unwrap();
        let message = worker.recv().await.expect("routed worker message");
        let WorkerMessage::GroupSamples {
            group_key, samples, ..
        } = message
        else {
            panic!("expected GroupSamples");
        };
        assert_eq!(group_key.values().labels, vec!["api"]);
        assert_eq!(
            samples,
            vec![("requests_total{job=\"api\"}".into(), 100, 4.0)]
        );
    }
}
