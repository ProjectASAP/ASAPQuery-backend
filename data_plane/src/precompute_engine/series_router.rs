use crate::precompute_engine::group_key::GroupKey;
use crate::storage_engines::types::AggregateCore;
use asap_types::PolicyFingerprint;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use xxhash_rust::xxh64::xxh64;

/// A message sent from the router to a worker.
///
/// B7.6 (schema-retirement #5): the per-group bucket key on `GroupSamples`
/// and `AccumulatorInput` is now a single `sid` (registry-allocated by
/// `SeriesIdResolver`), not the `(agg_id, group_key)` tuple. The grouping
/// label values are already folded into the sid via the
/// `(metric, attrs_fingerprint, agg_kind)` identity contract — so one sid
/// uniquely names one bucket, with no extra discriminator needed for
/// hashing or pane lookup. `group_key` and `policy_fp` still travel
/// alongside the sid: `group_key` is consumed at emit-time to render the
/// output label vector; `policy_fp` is the handle the worker uses to fetch
/// the source `AggregationConfig` from the hot-reload snapshot (window
/// shape, late-data policy, etc.). Together they let the worker key state
/// by sid without losing the data the legacy `(agg_id, group_key)` shape
/// carried.
pub enum WorkerMessage {
    /// Receipt allocated after queue reservation and before any input is visible.
    Admitted {
        input: Box<WorkerMessage>,
        revision: Arc<crate::storage_engines::types::SummaryInputRevision>,
    },
    /// A batch of samples for the same series, routed by series key.
    /// Used in `pass_raw_samples` mode where no aggregation is needed.
    RawSamples {
        series_key: String,
        samples: Vec<(i64, f64)>, // (timestamp_ms, value)
        ingest_received_at: Instant,
    },
    /// A batch of samples destined for a specific sid (group bucket).
    /// All samples share the same `sid` and are fed into a single shared
    /// accumulator (like Arroyo's GROUP BY). `sid` is the registry-
    /// allocated identity for `(metric, attrs, agg_kind)`; `policy_fp` is
    /// the source config's content-addressed fingerprint; `group_key` is
    /// kept for emit-time label rendering.
    GroupSamples {
        /// Registry-allocated bucket identity. Folds in
        /// `(metric, attrs_fingerprint, agg_kind_canonical)` — see
        /// `SeriesIdResolver::resolve`. Worker keys `group_states` on this.
        sid: u64,
        /// Source `AggregationConfig` fingerprint. Worker looks up its
        /// `AggregationConfig` (window size, sketch kind/config, late
        /// data policy, etc.) via `snap.get_aggregation_config(policy_fp.as_u64())`.
        policy_fp: PolicyFingerprint,
        /// Grouping label values joined by semicolons (e.g. "constant").
        /// Empty string if the aggregation has no grouping labels. Used
        /// at emit time to render the output's `KeyByLabelValues`.
        group_key: Arc<GroupKey>,
        /// Each entry: (series_key, timestamp_ms, value).
        /// series_key is needed for keyed (MultipleSubpopulation) accumulators
        /// to extract the aggregated-label key.
        samples: Vec<(String, i64, f64)>,
        ingest_received_at: Instant,
    },
    /// A pre-built accumulator destined for a specific sid's pane. The
    /// worker merges it into that pane's existing accumulator (or
    /// inserts it if the pane is empty) via `AggregateCore::merge_with`.
    ///
    /// Produced by ingest sources that deliver pre-aggregated sketches —
    /// e.g. the OTLP receiver when DataCollector emits KLL / CountMin /
    /// CountSketch payloads on a `SketchEnvelope`. Lets the precompute
    /// engine perform further window-aligned aggregation on sketches the
    /// same way it does on raw samples.
    ///
    /// Same sid / policy_fp / group_key contract as `GroupSamples`.
    AccumulatorInput {
        /// Registry-allocated bucket identity; see `GroupSamples::sid`.
        sid: u64,
        /// Source `AggregationConfig` fingerprint; see
        /// `GroupSamples::policy_fp`.
        policy_fp: PolicyFingerprint,
        /// Grouping label values joined by semicolons, matching the
        /// format produced by `IngestState::extract_group_key_for`.
        /// Used at emit time to render the output's `KeyByLabelValues`.
        group_key: Arc<GroupKey>,
        /// Wall-clock timestamp the sketch refers to (millis since epoch).
        /// Used to place the sketch into the correct pane.
        timestamp_ms: i64,
        /// The incoming accumulator to be merged into the pane.
        accumulator: Box<dyn AggregateCore>,
        ingest_received_at: Instant,
    },
    /// Signal the worker to flush/check idle windows.
    Flush,
    /// Finite-input barrier: acknowledge only after queued input and trailing panes reach the sink.
    Drain(tokio::sync::oneshot::Sender<Result<(), String>>),
    /// Graceful shutdown.
    Shutdown,
}

impl fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted { input, revision } => f
                .debug_struct("Admitted")
                .field("input", input)
                .field("revision", &revision.revision)
                .finish(),
            Self::RawSamples {
                series_key,
                samples,
                ..
            } => f
                .debug_struct("RawSamples")
                .field("series_key", series_key)
                .field("sample_count", &samples.len())
                .finish(),
            Self::GroupSamples {
                sid,
                group_key,
                samples,
                ..
            } => f
                .debug_struct("GroupSamples")
                .field("sid", sid)
                .field("group_key", group_key)
                .field("sample_count", &samples.len())
                .finish(),
            Self::AccumulatorInput {
                sid,
                group_key,
                timestamp_ms,
                accumulator,
                ..
            } => f
                .debug_struct("AccumulatorInput")
                .field("sid", sid)
                .field("group_key", group_key)
                .field("timestamp_ms", timestamp_ms)
                .field("accumulator_type", &accumulator.type_name())
                .finish(),
            Self::Flush => f.write_str("Flush"),
            Self::Drain(_) => f.write_str("Drain"),
            Self::Shutdown => f.write_str("Shutdown"),
        }
    }
}

/// Routes incoming samples to one of N workers based on a consistent hash.
pub struct SeriesRouter {
    erp_observer: std::sync::OnceLock<std::sync::Arc<super::erp_observer::RuntimeErpObserver>>,
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    num_workers: usize,
}

impl SeriesRouter {
    pub fn new(senders: Vec<mpsc::Sender<WorkerMessage>>) -> Self {
        let num_workers = senders.len();
        Self {
            erp_observer: std::sync::OnceLock::new(),
            senders,
            num_workers,
        }
    }

    pub fn enable_erp_observation(
        &self,
        endpoint: String,
        generation: asap_types::sds::CatalogGeneration,
    ) -> Result<(), String> {
        self.erp_observer
            .set(super::erp_observer::RuntimeErpObserver::new(
                endpoint, generation,
            ))
            .map_err(|_| "ERP observer already configured".into())
    }
    pub fn erp_observer(&self) -> Option<std::sync::Arc<super::erp_observer::RuntimeErpObserver>> {
        self.erp_observer.get().cloned()
    }

    /// Route a pre-grouped batch of group messages to workers concurrently.
    ///
    /// Each `GroupSamples` / `AccumulatorInput` message is routed by
    /// `worker_for_sid(sid)` — same `sid` always lands on the same worker,
    /// so per-bucket state stays single-owner. Messages within a single
    /// worker are sent sequentially to preserve ordering.
    pub async fn route_group_batch(
        &self,
        messages: Vec<WorkerMessage>,
        _ingest_received_at: Instant,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Group messages by target worker index
        let mut per_worker: HashMap<usize, Vec<WorkerMessage>> = HashMap::new();
        for msg in messages {
            let worker_idx = match &msg {
                WorkerMessage::Admitted { .. } => {
                    return Err("input must be admitted by the router".into())
                }
                WorkerMessage::GroupSamples { sid, .. } => self.worker_for_sid(*sid),
                WorkerMessage::AccumulatorInput { sid, .. } => self.worker_for_sid(*sid),
                WorkerMessage::RawSamples { series_key, .. } => self.worker_for(series_key),
                _ => 0,
            };
            per_worker.entry(worker_idx).or_default().push(msg);
        }

        // Send to each worker concurrently
        try_join_all(per_worker.into_iter().map(|(worker_idx, messages)| {
            let sender = &self.senders[worker_idx];
            async move {
                for msg in messages {
                    sender
                        .send(msg)
                        .await
                        .map_err(|e| format!("Failed to send to worker {}: {}", worker_idx, e))?;
                }
                Ok::<(), String>(())
            }
        }))
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::other(e))
        })?;

        Ok(())
    }

    /// Atomically reserve queue capacity for an entire request and then
    /// publish it. If any target worker is full or closed, every reservation
    /// is dropped and no message is enqueued. Remote Write uses this to turn
    /// bounded-queue pressure into a retryable HTTP response without leaving
    /// an untracked partial request behind.
    pub fn try_route_group_batch_atomic(
        &self,
        messages: Vec<WorkerMessage>,
    ) -> Result<(), TryRouteError> {
        self.try_route_group_batch_with_admission(messages, || Ok(None))
    }

    pub fn try_route_group_batch_with_admission(
        &self,
        messages: Vec<WorkerMessage>,
        admit: impl FnOnce() -> Result<
            Option<Arc<crate::storage_engines::types::SummaryInputRevision>>,
            String,
        >,
    ) -> Result<(), TryRouteError> {
        let mut pending = Vec::with_capacity(messages.len());
        for message in messages {
            let worker_idx = match &message {
                WorkerMessage::GroupSamples { sid, .. }
                | WorkerMessage::AccumulatorInput { sid, .. } => self.worker_for_sid(*sid),
                WorkerMessage::RawSamples { series_key, .. } => self.worker_for(series_key),
                WorkerMessage::Admitted { .. } => {
                    return Err(TryRouteError::Admission("input already admitted".into()))
                }
                WorkerMessage::Flush | WorkerMessage::Drain(_) | WorkerMessage::Shutdown => 0,
            };
            let permit = self.senders[worker_idx]
                .clone()
                .try_reserve_owned()
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => TryRouteError::Full,
                    mpsc::error::TrySendError::Closed(_) => TryRouteError::Closed,
                })?;
            pending.push((permit, message));
        }
        let revision = admit().map_err(TryRouteError::Admission)?;
        for (permit, message) in pending {
            permit.send(match &revision {
                Some(revision) => WorkerMessage::Admitted {
                    input: Box::new(message),
                    revision: Arc::clone(revision),
                },
                None => message,
            });
        }
        Ok(())
    }

    /// Broadcast a flush signal to all workers.
    pub async fn broadcast_flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sender) in self.senders.iter().enumerate() {
            sender
                .send(WorkerMessage::Flush)
                .await
                .map_err(|e| format!("Failed to send flush to worker {}: {}", i, e))?;
        }
        Ok(())
    }

    /// Broadcast shutdown to all workers.
    pub async fn broadcast_shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sender) in self.senders.iter().enumerate() {
            sender
                .send(WorkerMessage::Shutdown)
                .await
                .map_err(|e| format!("Failed to send shutdown to worker {}: {}", i, e))?;
        }
        Ok(())
    }

    /// Caller must stop new input before invoking this finite-source barrier.
    pub async fn drain(&self) -> Result<(), String> {
        let mut replies = Vec::new();
        for sender in &self.senders {
            let (tx, rx) = tokio::sync::oneshot::channel();
            sender
                .send(WorkerMessage::Drain(tx))
                .await
                .map_err(|e| e.to_string())?;
            replies.push(rx);
        }
        for reply in replies {
            reply.await.map_err(|e| e.to_string())??;
        }
        Ok(())
    }

    /// Determine which worker handles a given sid bucket.
    ///
    /// Hashes the sid alone — the legacy `(agg_id, group_key)` tuple folded
    /// into one u64 by `SeriesIdResolver`, so a single xxh64 over the sid
    /// gives the same per-bucket sharding the tuple-hash produced.
    fn worker_for_sid(&self, sid: u64) -> usize {
        let hash = xxh64(&sid.to_le_bytes(), 0);
        (hash as usize) % self.num_workers
    }

    /// Determine which worker handles a given series key (for raw mode).
    fn worker_for(&self, series_key: &str) -> usize {
        let hash = xxh64(series_key.as_bytes(), 0);
        (hash as usize) % self.num_workers
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TryRouteError {
    #[error("summary admission rejected: {0}")]
    Admission(String),
    #[error("precompute queue is full")]
    Full,
    #[error("precompute worker is unavailable")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_sid_routing() {
        let (senders, _receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(10)).unzip();

        let router = SeriesRouter::new(senders);

        // Same sid should always go to the same worker.
        let w1 = router.worker_for_sid(42);
        let w2 = router.worker_for_sid(42);
        assert_eq!(w1, w2);

        // All resolved buckets land within the worker count.
        assert!(router.worker_for_sid(7) < 4);
        assert!(router.worker_for_sid(99) < 4);
        assert!(router.worker_for_sid(0) < 4);
    }

    #[test]
    fn test_raw_mode_routing() {
        let (senders, _receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(10)).unzip();

        let router = SeriesRouter::new(senders);

        // Same key should always go to the same worker
        let w1 = router.worker_for("cpu{host=\"a\"}");
        let w2 = router.worker_for("cpu{host=\"a\"}");
        assert_eq!(w1, w2);
        assert!(router.worker_for("mem{host=\"a\"}") < 4);
    }

    #[tokio::test]
    async fn atomic_route_publishes_nothing_when_batch_exceeds_capacity() {
        let (sender, mut receiver) = mpsc::channel::<WorkerMessage>(1);
        let router = SeriesRouter::new(vec![sender]);
        let messages = vec![
            WorkerMessage::RawSamples {
                series_key: "m{job=\"a\"}".into(),
                samples: vec![(1, 1.0)],
                ingest_received_at: Instant::now(),
            },
            WorkerMessage::RawSamples {
                series_key: "m{job=\"b\"}".into(),
                samples: vec![(1, 2.0)],
                ingest_received_at: Instant::now(),
            },
        ];
        let mut admitted = false;
        assert_eq!(
            router.try_route_group_batch_with_admission(messages, || {
                admitted = true;
                Ok(None)
            }),
            Err(TryRouteError::Full)
        );
        assert!(
            !admitted,
            "failed queue reservation must not mutate admission"
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}
