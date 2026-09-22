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
/// Data partition ownership is derived from the installed DAG; storage locators
/// carried by individual inputs do not determine their destination worker.
pub enum WorkerMessage {
    /// Immutable producer generation captured before routing. The optional
    /// receipt proves atomic admission; absent receipts are never fabricated.
    BoundInput {
        input: Box<WorkerMessage>,
        generation: Arc<asap_types::sds::CatalogGeneration>,
        revision: Option<Arc<crate::storage_engines::types::SummaryInputRevision>>,
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
        /// Source `PrecomputeMaterialization` fingerprint. Worker looks up its
        /// `PrecomputeMaterialization` (window size, sketch kind/config, late
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
        /// Source `PrecomputeMaterialization` fingerprint; see
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
    /// Execute downstream DAG work after all raw windows have been sealed.
    CompleteDag {
        plan: Arc<crate::storage_engines::types::RuntimePhysicalPlan>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    /// Graceful shutdown.
    Shutdown,
}

impl fmt::Debug for WorkerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundInput {
                input,
                generation,
                revision,
            } => f
                .debug_struct("BoundInput")
                .field("input", input)
                .field("generation", generation)
                .field("revision", &revision.as_ref().map(|value| value.revision))
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
            Self::CompleteDag { .. } => f.write_str("CompleteDag"),
            Self::Shutdown => f.write_str("Shutdown"),
        }
    }
}

/// Routes incoming samples to one of N workers based on a consistent hash.
pub struct SeriesRouter {
    erp_observer: std::sync::OnceLock<std::sync::Arc<super::erp_observer::RuntimeErpObserver>>,
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    num_workers: usize,
    plan: Option<crate::storage_engines::types::StreamingConfigHandle>,
}

impl SeriesRouter {
    pub fn new(senders: Vec<mpsc::Sender<WorkerMessage>>) -> Self {
        let num_workers = senders.len();
        Self {
            erp_observer: std::sync::OnceLock::new(),
            senders,
            num_workers,
            plan: None,
        }
    }

    pub fn with_plan(mut self, plan: crate::storage_engines::types::StreamingConfigHandle) -> Self {
        self.plan = Some(plan);
        self
    }

    fn partition_rule(&self) -> super::partitioning::DagPartitioning {
        self.plan
            .as_ref()
            .map(|plan| plan.snapshot().partitioning.clone())
            .unwrap_or(super::partitioning::DagPartitioning::Population)
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
    /// Every producer uses the same DAG partition rule. Messages within one
    /// worker are sent sequentially to preserve admission order.
    pub async fn route_group_batch(
        &self,
        messages: Vec<WorkerMessage>,
        _ingest_received_at: Instant,
        generation: Option<Arc<asap_types::sds::CatalogGeneration>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let partitioning = self.partition_rule();
        // Group messages by target worker index
        let mut per_worker: HashMap<usize, Vec<WorkerMessage>> = HashMap::new();
        for msg in messages {
            let worker_idx = match &msg {
                WorkerMessage::BoundInput { .. } => {
                    return Err("input must be admitted by the router".into())
                }
                WorkerMessage::GroupSamples { group_key, .. }
                | WorkerMessage::AccumulatorInput { group_key, .. } => {
                    partitioning.owner(&group_key.as_population_labels(), self.num_workers)?
                }
                WorkerMessage::RawSamples { series_key, .. } => self.worker_for(series_key),
                _ => 0,
            };
            let message = match &generation {
                Some(generation) => WorkerMessage::BoundInput {
                    input: Box::new(msg),
                    generation: Arc::clone(generation),
                    revision: None,
                },
                None => msg,
            };
            per_worker.entry(worker_idx).or_default().push(message);
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

    pub fn try_route_group_batch_with_admission(
        &self,
        messages: Vec<WorkerMessage>,
        admit: impl FnOnce() -> Result<
            Option<Arc<crate::storage_engines::types::SummaryInputRevision>>,
            String,
        >,
    ) -> Result<(), TryRouteError> {
        let partitioning = self.partition_rule();
        let mut pending = Vec::with_capacity(messages.len());
        for message in messages {
            let worker_idx = match &message {
                WorkerMessage::GroupSamples { group_key, .. }
                | WorkerMessage::AccumulatorInput { group_key, .. } => partitioning
                    .owner(&group_key.as_population_labels(), self.num_workers)
                    .map_err(TryRouteError::Admission)?,
                WorkerMessage::RawSamples { series_key, .. } => self.worker_for(series_key),
                WorkerMessage::BoundInput { .. } => {
                    return Err(TryRouteError::Admission("input already admitted".into()))
                }
                WorkerMessage::Flush
                | WorkerMessage::Drain(_)
                | WorkerMessage::Shutdown
                | WorkerMessage::CompleteDag { .. } => 0,
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
                Some(revision) => WorkerMessage::BoundInput {
                    input: Box::new(message),
                    generation: Arc::clone(&revision.generation),
                    revision: Some(Arc::clone(revision)),
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

    pub async fn shutdown(&self) -> Result<(), String> {
        for sender in &self.senders {
            sender
                .send(WorkerMessage::Shutdown)
                .await
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }

    pub async fn complete_dag(
        &self,
        plan: Arc<crate::storage_engines::types::RuntimePhysicalPlan>,
    ) -> Result<(), String> {
        let mut replies = Vec::with_capacity(self.senders.len());
        for sender in &self.senders {
            let (reply, receiver) = tokio::sync::oneshot::channel();
            sender
                .send(WorkerMessage::CompleteDag {
                    plan: Arc::clone(&plan),
                    reply,
                })
                .await
                .map_err(|error| error.to_string())?;
            replies.push(receiver);
        }
        for reply in replies {
            reply.await.map_err(|error| error.to_string())??;
        }
        Ok(())
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

    // Shared DAG consumers must share partition ownership regardless of locator IDs.
    #[tokio::test]
    async fn shared_population_is_not_split_by_storage_locator() {
        let (senders, mut receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(128)).unzip();
        let router = SeriesRouter::new(senders);
        let messages = (0..32)
            .map(|sid| WorkerMessage::GroupSamples {
                sid,
                policy_fp: PolicyFingerprint(sid),
                group_key: Arc::new(GroupKey::new([("service", "api")])),
                samples: vec![("counter".into(), 1, 1.0)],
                ingest_received_at: Instant::now(),
            })
            .collect();
        router
            .route_group_batch(messages, Instant::now(), None)
            .await
            .unwrap();
        let counts = receivers
            .iter_mut()
            .map(|rx| {
                let mut count = 0;
                while rx.try_recv().is_ok() {
                    count += 1;
                }
                count
            })
            .collect::<Vec<_>>();
        assert_eq!(counts.iter().sum::<usize>(), 32);
        assert_eq!(
            counts.iter().filter(|n| **n != 0).count(),
            1,
            "one service's complete DAG must stay with one worker: {counts:?}"
        );
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
