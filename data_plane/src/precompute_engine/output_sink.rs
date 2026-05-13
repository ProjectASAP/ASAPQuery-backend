use crate::stores::sketch_db::index::{
    canonical_parameters, compute_sid, AccuracyBound, AggKind, SketchIndex,
    SketchInstanceMetadata,
};
use crate::stores::types::hot_reload_config::HotReloadStreamingConfig;
use crate::stores::types::{AggregateCore, PrecomputedOutput};
use crate::stores::Store;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tracing::{debug_span, warn};

/// Trait for emitting completed window outputs.
pub trait OutputSink: Send + Sync {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Output sink that writes directly to a `Store`.
pub struct StoreOutputSink {
    store: Arc<dyn Store>,
}

impl StoreOutputSink {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

impl OutputSink for StoreOutputSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        let _span = debug_span!("store_insert", batch_size = outputs.len()).entered();
        self.store.insert_precomputed_output_batch(outputs)
    }
}

/// Phase 5 M2.3.4 — fan precompute writes out to BOTH the legacy
/// `SketchStore` (so the existing query path keeps working) AND the
/// new sid-keyed `SketchIndex` (so the M2.3.5 query path has data to
/// read). When M2.3.5 lands, the legacy fan-out half can be retired
/// in M2.3.6.
///
/// Why the dual write:
/// - The legacy `Store::insert_precomputed_output_batch` keys on
///   `aggregation_id`. The query engine still reads from it for
///   precomputes.
/// - `SketchIndex::append_precompute` keys on content-derived `sid`
///   (M2.3.3). It's where precomputes WILL live, but no consumer
///   reads from it yet.
///
/// Per-batch overhead: one streaming-config snapshot read + per-row
/// agg-id lookup, sid hash, and `Box<dyn AggregateCore>` clone. The
/// clone goes through `clone_boxed_core` (already paid by ingest
/// code that copies accumulators between layers), so the cost is
/// linear in the batch size with a small constant.
pub struct DualWriteSink {
    store: Arc<dyn Store>,
    sketch_index: Arc<SketchIndex>,
    hot_reload: HotReloadStreamingConfig,
}

impl DualWriteSink {
    pub fn new(
        store: Arc<dyn Store>,
        sketch_index: Arc<SketchIndex>,
        hot_reload: HotReloadStreamingConfig,
    ) -> Self {
        Self {
            store,
            sketch_index,
            hot_reload,
        }
    }

    /// Best-effort write to `SketchIndex` for one PrecomputedOutput.
    /// Logs and skips on missing agg_config or other transient
    /// inconsistencies — the legacy SketchStore write still happens,
    /// so a SketchIndex miss is recoverable. Returns whether the
    /// SketchIndex write actually landed (for tests / observability).
    fn append_to_index(
        &self,
        output: &PrecomputedOutput,
        accumulator: &dyn AggregateCore,
    ) -> bool {
        let cfg = self.hot_reload.snapshot();
        let Some(agg_cfg) = cfg.get_aggregation_config(output.aggregation_id) else {
            warn!(
                agg_id = output.aggregation_id,
                "DualWriteSink: agg_config missing from streaming snapshot; skipping SketchIndex write"
            );
            return false;
        };

        // Canonicalize the per-DP attrs. AggregationConfig's
        // grouping_labels.labels is sorted at construction; align
        // with KeyByLabelValues.labels positionally.
        let label_values_vec = output
            .key
            .as_ref()
            .map(|k| k.labels.clone())
            .unwrap_or_default();
        let key_names = &agg_cfg.grouping_labels.labels;
        let mut attrs_fp = String::new();
        let mut label_values_map: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for (k, v) in key_names.iter().zip(label_values_vec.iter()) {
            attrs_fp.push_str(k);
            attrs_fp.push('=');
            attrs_fp.push_str(v);
            attrs_fp.push(';');
            label_values_map.insert(k.clone(), v.clone());
        }

        let agg_kind = AggKind::Precompute {
            agg_type: agg_cfg.aggregation_type,
            parameters_canonical: canonical_parameters(&agg_cfg.parameters),
        };
        let sid = compute_sid(&agg_cfg.metric, &attrs_fp, &agg_kind);

        // Register the sid in SketchIndex on first sight. M2.3.3
        // doesn't compute `Capability` / `AccuracyBound` for
        // precomputes (they answer exact stats), so both stay `None`.
        if self.sketch_index.instance(sid).is_none() {
            let group_by_keys: BTreeSet<String> = key_names.iter().cloned().collect();
            self.sketch_index.register(SketchInstanceMetadata {
                sid,
                metric_name: agg_cfg.metric.clone(),
                group_by_keys,
                capability: None,
                agg_kind: agg_kind.clone(),
                accuracy: None,
                first_seen_unix_ms: output.start_timestamp as i64,
                retired_at_ms: None,
                expires_at_ms: None,
            });
            let _ = AccuracyBound::from_config; // silence unused if all paths skip
        }

        let window = (output.start_timestamp, output.end_timestamp);
        self.sketch_index.append_precompute(
            sid,
            label_values_map,
            window,
            accumulator.clone_boxed_core(),
        );
        true
    }
}

impl OutputSink for DualWriteSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        let _span = debug_span!("dual_write_insert", batch_size = outputs.len()).entered();
        // Mirror the writes to SketchIndex first so the legacy write
        // path still owns the source-of-truth error semantics.
        for (output, accumulator) in &outputs {
            self.append_to_index(output, accumulator.as_ref());
        }
        self.store.insert_precomputed_output_batch(outputs)
    }
}

/// Output sink for raw passthrough mode — forwards raw samples to the store
/// without sketch computation. In this mode the samples are stored as
/// SumAccumulators (one per sample).
pub struct RawPassthroughSink {
    store: Arc<dyn Store>,
}

impl RawPassthroughSink {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

impl OutputSink for RawPassthroughSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        let _span = debug_span!("store_insert_raw", batch_size = outputs.len()).entered();
        self.store.insert_precomputed_output_batch(outputs)
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
    use crate::precompute_engine::operators::SumAccumulator;
    use crate::stores::sketch_db::index::SidLookup;
    use crate::stores::sketch_db::store::SketchStore;
    use crate::stores::types::{CleanupPolicy, KeyByLabelValues, StreamingConfig};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use promql_utilities::query_logics::enums::AggregationType;
    use std::collections::HashMap;

    fn sum_agg_config(id: u64, metric: &str, grouping_keys: &[&str]) -> AggregationConfig {
        AggregationConfig {
            aggregation_id: id,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(
                grouping_keys.iter().map(|s| s.to_string()).collect(),
            ),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size: 1,
            slide_interval: 1,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_column: None,
        }
    }

    #[test]
    fn dual_write_mirrors_to_store_and_index() {
        // Set up the dual-write sink with a real SketchStore + SketchIndex
        // backed by a one-entry streaming config.
        let agg_id = 7;
        let cfg = sum_agg_config(agg_id, "cpu_seconds", &["zone"]);
        let mut configs = HashMap::new();
        configs.insert(agg_id, cfg);
        let streaming = StreamingConfig::new(configs);
        let hot_reload = HotReloadStreamingConfig::new(streaming.clone());

        let store = Arc::new(SketchStore::new(
            Arc::new(streaming),
            CleanupPolicy::NoCleanup,
        )) as Arc<dyn Store>;
        let sketch_index = Arc::new(SketchIndex::new());
        let sink = DualWriteSink::new(store.clone(), sketch_index.clone(), hot_reload);

        // One PrecomputedOutput in the batch.
        let key = KeyByLabelValues::new_with_labels(vec!["z0".to_string()]);
        let output = PrecomputedOutput::new(1000, 2000, Some(key), agg_id);
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(42.0));

        sink.emit_batch(vec![(output, acc)]).expect("emit ok");

        // Legacy SketchStore: at least one bucket landed for the agg_id.
        let map = store
            .query_precomputed_output("cpu_seconds", agg_id, 0, u64::MAX / 2)
            .expect("legacy query ok");
        assert!(
            !map.is_empty(),
            "legacy SketchStore should have at least one bucket"
        );

        // New SketchIndex: exactly one sid registered (precompute
        // variant) and it classifies as Hit.
        assert_eq!(
            sketch_index.instance_count(),
            1,
            "SketchIndex should have one precompute instance"
        );
        let instances = sketch_index.list_by_status(
            crate::stores::sketch_db::schema::AggStatus::Active,
        );
        assert_eq!(instances.len(), 1);
        let meta = instances[0].clone();
        let sid = meta.sid;
        assert_eq!(sketch_index.classify(sid), SidLookup::Hit);
        assert!(
            matches!(
                meta.agg_kind,
                AggKind::Precompute {
                    agg_type: AggregationType::Sum,
                    ..
                }
            ),
            "sid metadata should be Precompute(Sum)"
        );
        assert!(meta.capability.is_none(), "precomputes have no capability");
    }

    #[test]
    fn dual_write_skips_unknown_agg_id_gracefully() {
        // Streaming config does NOT contain agg_id=99. The legacy
        // SketchStore write should still succeed; the SketchIndex
        // write is silently skipped (logged at warn but no error).
        let streaming = StreamingConfig::new(HashMap::new());
        let hot_reload = HotReloadStreamingConfig::new(streaming.clone());
        let store = Arc::new(SketchStore::new(
            Arc::new(streaming),
            CleanupPolicy::NoCleanup,
        )) as Arc<dyn Store>;
        let sketch_index = Arc::new(SketchIndex::new());
        let sink = DualWriteSink::new(store.clone(), sketch_index.clone(), hot_reload);

        let output = PrecomputedOutput::new(1000, 2000, None, 99);
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
        let result = sink.emit_batch(vec![(output, acc)]);

        // Legacy write may fail or succeed depending on the store's
        // tolerance for unknown agg_ids — what we care about is that
        // the SketchIndex didn't get a junk registration.
        let _ = result;
        assert_eq!(sketch_index.instance_count(), 0);
    }
}
