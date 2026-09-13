use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::precompute_engine::ingest_handler::IngestObservability;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::types::hot_reload_config::StreamingConfigHandle;
use crate::storage_engines::types::{AggregateCore, PrecomputedOutput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug_span, warn};

/// CQ-6 — process-global fallback for the output-sink policy-miss
/// counter, used when a `SketchStoreSink` was constructed without an
/// `IngestObservability` handle wired in (e.g. the legacy
/// `SketchStoreSink::new` call site that predates the handle). Keeps the
/// count observable even before the handle is threaded through, so a
/// /metrics scrape never silently loses policy-miss drops.
static GLOBAL_DROPPED_POLICY_MISS: AtomicU64 = AtomicU64::new(0);

/// Read the process-global output-sink policy-miss drop count. Exposed
/// so the /metrics surface can fold it in for sinks not yet wired to an
/// `IngestObservability`.
pub fn global_dropped_policy_miss() -> u64 {
    GLOBAL_DROPPED_POLICY_MISS.load(Ordering::Relaxed)
}

/// Trait for emitting completed window outputs.
pub trait OutputSink: Send + Sync {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Publish an explicit source-partition event-time barrier after every
    /// preceding window from that partition has reached this sink.
    fn advance_summary_watermark(
        &self,
        _barrier: &asap_types::sds::SummaryWatermarkBarrier,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "output sink does not support summary watermark barriers",
        )))
    }
}

fn consume_in_order<T>(items: Vec<T>, mut persist: impl FnMut(&T) -> bool) -> usize {
    let mut failed = 0;
    for item in items {
        if !persist(&item) {
            failed += 1;
        }
    }
    failed
}

/// Write completed precompute windows to the sid-keyed sketch store.
///
/// Read one streaming-config snapshot per batch and resolve each policy through
/// its fingerprint. Consume accumulators in order so catch-up batches release
/// each pane as soon as it is serialized.
pub struct SketchStoreSink {
    summary_store: Arc<SketchStore>,
    hot_reload: StreamingConfigHandle,
    /// Single shared resolver across the ingest + precompute paths. Under
    /// the registry-allocated sid model (PR-1..3), this is the canonical
    /// mint authority — precompute sids share the same `next_sid` counter
    /// as OTel-sketch sids, so the two paths can never collide on identity
    /// even when the same `(metric, attrs)` carries both a sketch and an
    /// exact precompute.
    series_resolver: Arc<SeriesIdResolver>,
    /// CQ-6 — optional handle to the shared `IngestObservability` so
    /// policy-miss drops land in the same counter the OTLP ingest path
    /// surfaces to /metrics. `None` when the sink was constructed via the
    /// legacy `new()` call site (main.rs) that doesn't thread the handle;
    /// in that case drops are counted in the process-global fallback
    /// (`GLOBAL_DROPPED_POLICY_MISS`). Wire it post-construction with
    /// [`SketchStoreSink::with_observability`].
    observability: Option<Arc<IngestObservability>>,
}

impl SketchStoreSink {
    pub fn new(
        summary_store: Arc<SketchStore>,
        hot_reload: StreamingConfigHandle,
        series_resolver: Arc<SeriesIdResolver>,
    ) -> Self {
        Self {
            summary_store,
            hot_reload,
            series_resolver,
            observability: None,
        }
    }

    /// CQ-6 — attach a shared `IngestObservability` so this sink's
    /// policy-miss drops increment the same counter the ingest path
    /// reports. Builder-style (returns `self`) so the `new()` signature
    /// stays stable for existing call sites; wire it where the sink and
    /// `IngestState` are constructed together.
    pub fn with_observability(mut self, observability: Arc<IngestObservability>) -> Self {
        self.observability = Some(observability);
        self
    }

    /// CQ-6 — increment the policy-miss drop counter (shared handle if
    /// wired, process-global fallback otherwise).
    fn record_policy_miss(&self) {
        match &self.observability {
            Some(obs) => {
                obs.dropped_policy_miss.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                GLOBAL_DROPPED_POLICY_MISS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Write one PrecomputedOutput; false reports a rejected output to the caller.
    /// Missing configuration or incompatible state must surface as failure:
    /// a finite-input completion barrier cannot acknowledge dropped outputs.
    ///
    /// PR-6 follow-up: resolves the source `AggregationConfig` via
    /// `PolicyRegistry::get(output.policy_fp)`. The legacy
    /// `aggregation_id` fallback branch (PR 4) is gone — `policy_fp`
    /// is the only identity handle on `PrecomputedOutput`. Outputs
    /// emitted with the `PolicyFingerprint::UNSET` sentinel (e.g.
    /// raw-mode fast-path that has no source config) are skipped
    /// rather than routed by a parallel id.
    fn append_to_index(&self, output: &PrecomputedOutput, accumulator: &dyn AggregateCore) -> bool {
        if output.policy_fp.is_unset() {
            warn!(
                "SketchStoreSink: PrecomputedOutput carries PolicyFingerprint::UNSET; \
                 skipping write (sink requires a content-addressed handle)"
            );
            return false;
        }
        let cfg = self.hot_reload.snapshot();
        let registry = cfg.policy_registry();
        let agg_cfg = match registry.get(output.policy_fp) {
            Some(c) => c.clone(),
            None => {
                // CQ-6 — policy-miss drop: a content-addressed policy_fp
                // that the running streaming-config registry doesn't know
                // (config lag / retired policy). Count it so /metrics can
                // surface the silent skip.
                self.record_policy_miss();
                warn!(
                    policy_fp = %output.policy_fp,
                    "SketchStoreSink: policy_fp missing from registry; skipping write"
                );
                return false;
            }
        };
        let agg_cfg = &agg_cfg;
        let resolver = self.series_resolver.clone();
        let persist =
            |writer: &crate::storage_engines::sketch_db::index::SummaryPublicationWriter<'_>| {
                if let Some(sid) = output.series_id {
                    return writer
                        .ingest_precompute_with_series_id(sid, agg_cfg, output, accumulator)
                        .inspect(|_| {
                            crate::precompute_engine::metrics::record_materialized_outputs(1)
                        });
                }
                self.summary_store
                    .validate_routed_catalog_generation(output.catalog_generation.as_deref())
                    .ok()?;
                writer
                    .ingest_precompute_for_agg_config(
                        |metric, fp, ak| {
                            resolver
                                .resolve_with_reactivation(metric, fp, ak, |sid| {
                                    self.summary_store.validate_routed_catalog_generation(
                                        output.catalog_generation.as_deref(),
                                    )?;
                                    let activation =
                                        self.summary_store.authorize_series_reactivation(
                                            sid,
                                            output.policy_fp.into(),
                                        )?;
                                    if let Some(generation) = &activation {
                                        if output.catalog_generation.as_deref()
                                            != Some(generation.as_ref())
                                        {
                                            return Err(
                                            "unbound or stale output cannot reactivate a series"
                                                .into(),
                                        );
                                        }
                                    }
                                    Ok(activation)
                                })
                                .map_err(|error| warn!(%error, "series reactivation rejected"))
                                .ok()
                        },
                        agg_cfg,
                        output,
                        accumulator,
                    )
                    .inspect(|_| crate::precompute_engine::metrics::record_materialized_outputs(1))
            };
        if let Some(revision) = &output.input_revision {
            let group_values = output.population_labels.clone().unwrap_or_else(|| {
                agg_cfg
                    .grouping_labels
                    .iter()
                    .cloned()
                    .zip(output.key.clone().unwrap_or_default().labels)
                    .collect()
            });
            let (Ok(start_ms), Ok(end_ms)) = (
                i64::try_from(output.start_timestamp),
                i64::try_from(output.end_timestamp),
            ) else {
                return false;
            };
            let coordinate = asap_types::sds::SummaryInstanceCoordinates {
                summary_definition_id: output.policy_fp.into(),
                time_range: asap_types::sds::HalfOpenTimeRange { start_ms, end_ms },
                group_values,
            };
            if let Err(error) = self.summary_store.publish_admitted_summary_update(
                &revision.generation,
                &coordinate,
                revision.first_revision,
                revision.revision,
                agg_cfg
                    .num_aggregates_to_retain
                    .unwrap_or(1)
                    .saturating_mul(agg_cfg.slide_interval)
                    .max(agg_cfg.window_size)
                    .saturating_mul(1_000),
                persist,
            ) {
                warn!(%error, "summary input revision publication failed");
                return false;
            }
            true
        } else {
            self.summary_store
                .publish_unadmitted_summary_update(persist)
                .is_some()
        }
    }
}

impl OutputSink for SketchStoreSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        let _span = debug_span!("sketch_index_insert", batch_size = outputs.len()).entered();
        let output_count = outputs.len();
        let failed = consume_in_order(outputs, |(output, accumulator)| {
            self.append_to_index(output, accumulator.as_ref())
        });
        if failed > 0 {
            return Err(format!(
                "SketchStore rejected {failed} of {} completed outputs",
                output_count
            )
            .into());
        }
        Ok(())
    }
}

/// A capturing sink for testing that stores all emitted outputs.
pub struct CapturingOutputSink {
    pub captured: Mutex<Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>>,
}

impl CapturingOutputSink {
    pub fn new() -> Self {
        Self {
            captured: Mutex::new(Vec::new()),
        }
    }

    pub fn drain(&self) -> Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> {
        self.captured.lock().unwrap().drain(..).collect()
    }

    pub fn len(&self) -> usize {
        self.captured.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.captured.lock().unwrap().is_empty()
    }
}

impl Default for CapturingOutputSink {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputSink for CapturingOutputSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.captured.lock().unwrap().extend(outputs);
        Ok(())
    }
}

/// A no-op sink for testing that just counts emitted batches.
pub struct NoopOutputSink {
    pub emit_count: std::sync::atomic::AtomicU64,
}

impl NoopOutputSink {
    pub fn new() -> Self {
        Self {
            emit_count: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl Default for NoopOutputSink {
    fn default() -> Self {
        Self::new()
    }
}

impl OutputSink for NoopOutputSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.emit_count
            .fetch_add(outputs.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_engine::operators::{DDSketchAccumulator, SumAccumulator};
    use crate::storage_engines::sketch_db::index::{AggKind, SeriesLookup};
    use crate::storage_engines::types::{KeyByLabelValues, StreamingConfig};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EmitOnlySink;

    impl OutputSink for EmitOnlySink {
        fn emit_batch(
            &self,
            _outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    #[test]
    fn watermark_support_must_be_explicit() {
        let completion_source = asap_types::sds::SummarySourcePartition {
            producer_id: "producer".into(),
            partition_id: "partition".into(),
            producer_epoch: 1,
        };
        let barrier = asap_types::sds::SummaryWatermarkBarrier {
            catalog_generation: asap_types::sds::CatalogGeneration {
                schema_version: 1,
                plan_id: 1,
                plan_version: 1,
                snapshot_sha256: "snapshot".into(),
            },
            source: completion_source,
            sequence: 1,
            watermark_ms: 10,
        };
        let err = EmitOnlySink
            .advance_summary_watermark(&barrier)
            .expect_err("sink must reject an unsupported barrier");
        assert_eq!(
            err.downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::Unsupported)
        );
    }

    fn sum_agg_config(_id: u64, metric: &str, grouping_keys: &[&str]) -> AggregationConfig {
        // `_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`. Callers obtain the id
        // via `config.policy_fp_u64()`.
        AggregationConfig {
            population_key_encoding: Default::default(),
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(
                grouping_keys.iter().map(|s| s.to_string()).collect(),
            )
            .into(),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowKind::Tumbling,
            window_layout: asap_types::WindowMaterializationLayout::Pane { pane_secs: 1 },
            pane_origin_ms: None,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_projection: None,
            table_population: None,
            derived_input: None,
            table_timestamp_column: None,
            partitioning: None,
            value_source_column: None,
        }
    }

    #[test]
    fn sketch_index_sink_writes_to_index() {
        let cfg = sum_agg_config(7, "cpu_seconds", &["zone"]);
        let agg_id = cfg.policy_fp_u64();
        let mut configs = HashMap::new();
        configs.insert(agg_id, cfg);
        let streaming = StreamingConfig::new(configs);
        let hot_reload = StreamingConfigHandle::new(streaming.clone());

        let summary_store = Arc::new(SketchStore::new());
        let sink = SketchStoreSink::new(
            summary_store.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        );

        let key = KeyByLabelValues::new_with_labels(vec!["z0".to_string()]);
        let output =
            PrecomputedOutput::new(1000, 2000, Some(key), asap_types::PolicyFingerprint(agg_id));
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(42.0));

        sink.emit_batch(vec![(output, acc)]).expect("emit ok");

        assert_eq!(
            summary_store.instance_count(),
            1,
            "SketchStore should have one precompute instance"
        );
        let instances = summary_store
            .list_by_status(crate::storage_engines::sketch_db::lifecycle::AggStatus::Active);
        assert_eq!(instances.len(), 1);
        let meta = instances[0].clone();
        let sid = meta.sid;
        assert_eq!(summary_store.classify(sid), SeriesLookup::Hit);
        assert!(
            matches!(
                meta.agg_kind,
                AggKind::ExactAgg {
                    agg_type: AggregationType::Sum,
                    ..
                }
            ),
            "sid metadata should be ExactAgg(Sum)"
        );
        // PR 6 follow-up: ExactAgg-backed sids now carry an
        // `ExactAgg(agg_type)` capability so the analyzer can match
        // them. Previously this field was unconditionally `None`.
        assert_eq!(
            meta.capability,
            Some(
                crate::storage_engines::sketch_db::data::Capability::ExactAgg(AggregationType::Sum)
            ),
            "ExactAgg sids carry an ExactAgg capability"
        );
    }

    #[test]
    fn sink_reactivates_catalog_series_without_reusing_retired_payload() {
        let cfg = sum_agg_config(7, "cpu_seconds", &[]);
        let fingerprint = cfg.policy_fingerprint();
        let catalog = asap_types::summary_catalog::SummaryCatalog::from_materializations(
            1,
            1,
            &[cfg.clone()],
        )
        .unwrap();
        let store = Arc::new(SketchStore::new());
        store
            .install_summary_catalog(Arc::new(catalog.clone()))
            .unwrap();
        let temporary = tempfile::tempdir().unwrap();
        let resolver =
            Arc::new(SeriesIdResolver::open(temporary.path().join("resolver.wal")).unwrap());
        let sink = SketchStoreSink::new(
            store.clone(),
            StreamingConfigHandle::new(StreamingConfig::new(HashMap::from([(fingerprint.0, cfg)]))),
            resolver,
        );
        let original_generation = Arc::new(catalog.reference().unwrap());
        let output = || {
            let mut output = PrecomputedOutput::new(1000, 2000, None, fingerprint);
            output.catalog_generation = Some(Arc::clone(&original_generation));
            output
        };
        sink.emit_batch(vec![(output(), Box::new(SumAccumulator::with_sum(7.0)))])
            .unwrap();
        let old_sid = store.series_ids_for_policy(fingerprint)[0];
        store.remove_instance(old_sid).unwrap();
        assert!(sink
            .emit_batch(vec![(output(), Box::new(SumAccumulator::with_sum(11.0)))])
            .is_err());
        let mut stale_output = output();
        stale_output.series_id = Some(old_sid);
        stale_output.catalog_generation = Some(Arc::new(catalog.reference().unwrap()));
        let mut next = catalog;
        next.plan_version += 1;
        let next_generation = Arc::new(next.reference().unwrap());
        store.install_summary_catalog(Arc::new(next)).unwrap();
        assert!(sink
            .emit_batch(vec![(
                stale_output.clone(),
                Box::new(SumAccumulator::with_sum(99.0))
            )])
            .is_err());
        let mut next_output = output();
        next_output.catalog_generation = Some(next_generation);
        sink.emit_batch(vec![(
            next_output,
            Box::new(SumAccumulator::with_sum(11.0)),
        )])
        .unwrap();
        assert!(sink
            .emit_batch(vec![(
                stale_output,
                Box::new(SumAccumulator::with_sum(99.0))
            )])
            .is_err());
        let new_sid = store.series_ids_for_policy(fingerprint)[0];
        // A derived/unbound stale output must not reuse an already rotated cache hit.
        assert!(sink
            .emit_batch(vec![(output(), Box::new(SumAccumulator::with_sum(101.0)))])
            .is_err());
        let mut stale_routed_output = output();
        stale_routed_output.series_id = Some(new_sid);
        assert!(sink
            .emit_batch(vec![(
                stale_routed_output,
                Box::new(SumAccumulator::with_sum(103.0))
            )])
            .is_err());
        let mut missing_generation = PrecomputedOutput::new(1000, 2000, None, fingerprint);
        missing_generation.series_id = Some(new_sid);
        assert!(sink
            .emit_batch(vec![(
                missing_generation,
                Box::new(SumAccumulator::with_sum(107.0))
            )])
            .is_err());
        assert_ne!(old_sid, new_sid);
        assert!(store.query_exact_agg_range(old_sid, 1000, 2000).is_empty());
        let values = store.query_exact_agg_range(new_sid, 1000, 2000);
        assert_eq!(
            values[0].1.values().next().unwrap().aux_stats().sum,
            Some(11.0)
        );
    }

    #[test]
    fn sketch_policy_is_registered_and_stored_as_sketch_state() {
        let mut cfg = sum_agg_config(8, "latency", &[]);
        cfg.aggregation_type = AggregationType::DDSketch;
        cfg.parameters
            .insert("alpha".into(), serde_json::json!(0.01));
        let policy_fp = cfg.policy_fp_u64();
        let hot_reload =
            StreamingConfigHandle::new(StreamingConfig::new(HashMap::from([(policy_fp, cfg)])));
        let summary_store = Arc::new(SketchStore::new());
        let sink = SketchStoreSink::new(
            summary_store.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        );
        let mut accumulator = DDSketchAccumulator::new(0.01);
        accumulator.inner.update(42.0);

        sink.emit_batch(vec![(
            PrecomputedOutput::new(1_000, 2_000, None, asap_types::PolicyFingerprint(policy_fp)),
            Box::new(accumulator),
        )])
        .expect("emit sketch");

        let meta = summary_store
            .list_by_status(crate::storage_engines::sketch_db::lifecycle::AggStatus::Active)
            .into_iter()
            .next()
            .expect("registered sketch SID");
        assert!(matches!(
            meta.agg_kind,
            AggKind::Sketch {
                algorithm: crate::storage_engines::sketch_db::data::SketchAlgorithm::DDSketch,
                ..
            }
        ));
        assert_eq!(summary_store.query_range(meta.sid, 1_000, 2_000).len(), 1);
        assert!(summary_store
            .query_exact_agg_range(meta.sid, 1_000, 2_000)
            .is_empty());
    }

    #[test]
    fn sketch_index_sink_reports_unknown_policy_as_failure() {
        // Streaming config does NOT contain agg_id=99 — the sink
        // reports a recoverable error rather than acknowledging a lost write.
        let streaming = StreamingConfig::new(HashMap::new());
        let hot_reload = StreamingConfigHandle::new(streaming.clone());
        let summary_store = Arc::new(SketchStore::new());
        let sink = SketchStoreSink::new(
            summary_store.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        );

        let output = PrecomputedOutput::new(1000, 2000, None, asap_types::PolicyFingerprint(99));
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
        sink.emit_batch(vec![(output, acc)])
            .expect_err("unpersisted output must not be acknowledged");
        assert_eq!(summary_store.instance_count(), 0);
    }

    /// CQ-6 — a registry-miss (policy_fp not in the running streaming
    /// config) reports a failed write; with an `IngestObservability`
    /// handle wired in, the `dropped_policy_miss` counter must tick.
    #[test]
    fn sink_increments_policy_miss_counter_on_registry_miss() {
        let streaming = StreamingConfig::new(HashMap::new());
        let hot_reload = StreamingConfigHandle::new(streaming.clone());
        let summary_store = Arc::new(SketchStore::new());
        let obs = Arc::new(IngestObservability::new());
        let sink = SketchStoreSink::new(
            summary_store.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        )
        .with_observability(obs.clone());

        // policy_fp=42 is absent from the empty registry → registry miss.
        let output = PrecomputedOutput::new(1000, 2000, None, asap_types::PolicyFingerprint(42));
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
        sink.emit_batch(vec![(output, acc)])
            .expect_err("unpersisted output must not be acknowledged");

        assert_eq!(
            summary_store.instance_count(),
            0,
            "no write on registry miss"
        );
        assert_eq!(
            obs.dropped_policy_miss
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "policy-miss drop counted on the wired observability handle"
        );

        // The UNSET sentinel is an expected raw-mode skip, NOT a policy
        // miss — it must not bump the counter.
        let unset = PrecomputedOutput::new(1000, 2000, None, asap_types::PolicyFingerprint::UNSET);
        let acc2: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
        sink.emit_batch(vec![(unset, acc2)])
            .expect_err("unpersisted output must not be acknowledged");
        assert_eq!(
            obs.dropped_policy_miss
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "UNSET skip is not a policy miss"
        );
    }

    #[test]
    fn consuming_batch_releases_each_item_after_it_is_persisted() {
        struct DropProbe(Arc<AtomicUsize>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let live = Arc::new(AtomicUsize::new(3));
        let probes = (0..3).map(|_| DropProbe(live.clone())).collect::<Vec<_>>();
        let mut observed = Vec::new();
        assert_eq!(
            consume_in_order(probes, |_| {
                observed.push(live.load(Ordering::SeqCst));
                true
            }),
            0
        );
        assert_eq!(observed, vec![3, 2, 1]);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}
