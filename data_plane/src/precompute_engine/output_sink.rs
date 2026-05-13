use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::types::hot_reload_config::HotReloadStreamingConfig;
use crate::storage_engines::types::{AggregateCore, PrecomputedOutput};
use std::sync::{Arc, Mutex};
use tracing::{debug_span, warn};

/// Trait for emitting completed window outputs.
pub trait OutputSink: Send + Sync {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Phase 5 M2.3.6 — successor to the M2.3.4 `DualWriteSink`. Writes
/// precomputes to `SketchStore` only; the legacy `SketchStore`
/// agg_id-keyed write path is retired.
///
/// Reads already prefer `SketchStore` (M2.3.5b's engine cut-over),
/// so the legacy store no longer receives traffic from either side.
/// Once the data-plane crate's `Store` trait and `stores/sketch_db/store/*`
/// modules are deleted (subsequent M2.3.6 sub-PRs), `SketchStore`
/// will be renamed to `SketchStore` and this type can collapse into
/// the previously-existing `StoreOutputSink` shape.
///
/// Per-batch overhead: one streaming-config snapshot read + per-row
/// agg-id lookup, sid hash, and `Box<dyn AggregateCore>` clone.
pub struct SketchStoreSink {
    sketch_index: Arc<SketchStore>,
    hot_reload: HotReloadStreamingConfig,
    /// Single shared resolver across the ingest + precompute paths. Under
    /// the registry-allocated sid model (PR-1..3), this is the canonical
    /// mint authority — precompute sids share the same `next_sid` counter
    /// as OTel-sketch sids, so the two paths can never collide on identity
    /// even when the same `(metric, attrs)` carries both a sketch and an
    /// exact precompute.
    series_resolver: Arc<SeriesIdResolver>,
}

impl SketchStoreSink {
    pub fn new(
        sketch_index: Arc<SketchStore>,
        hot_reload: HotReloadStreamingConfig,
        series_resolver: Arc<SeriesIdResolver>,
    ) -> Self {
        Self {
            sketch_index,
            hot_reload,
            series_resolver,
        }
    }

    /// Best-effort write to `SketchStore` for one PrecomputedOutput.
    /// Logs and skips on missing agg_config or other transient
    /// inconsistencies — a SketchStore miss is recoverable in
    /// practice because the control plane will re-emit the agg_config
    /// on its next reconcile pass.
    ///
    /// PR 4 (sid identity chain): resolves the source `AggregationConfig`
    /// via [`PolicyFingerprint`] first when the output carries a
    /// non-sentinel `policy_fp`; falls back to the legacy
    /// `aggregation_id` lookup when the field is the
    /// `PolicyFingerprint::UNSET` sentinel (set by call sites still on
    /// the legacy `PrecomputedOutput::new` constructor). Both paths
    /// resolve to the same `AggregationConfig` while
    /// `StreamingConfig::aggregation_configs` is the source of truth.
    fn append_to_index(
        &self,
        output: &PrecomputedOutput,
        accumulator: &dyn AggregateCore,
    ) -> bool {
        let cfg = self.hot_reload.snapshot();
        let agg_cfg_owned;
        let agg_cfg: &asap_types::aggregation_config::AggregationConfig =
            if !output.policy_fp.is_unset() {
                let registry = cfg.policy_registry();
                match registry.get(output.policy_fp) {
                    Some(c) => {
                        // Clone out so the borrow on the snapshot
                        // doesn't outlive this scope; the existing
                        // legacy branch ALSO borrows from the snapshot,
                        // so this is structurally equivalent.
                        agg_cfg_owned = c.clone();
                        &agg_cfg_owned
                    }
                    None => {
                        warn!(
                            policy_fp = %output.policy_fp,
                            agg_id = output.aggregation_id,
                            "SketchStoreSink: policy_fp missing from registry; skipping write"
                        );
                        return false;
                    }
                }
            } else {
                let Some(c) = cfg.get_aggregation_config(output.aggregation_id) else {
                    warn!(
                        agg_id = output.aggregation_id,
                        "SketchStoreSink: agg_config missing from streaming snapshot; skipping write"
                    );
                    return false;
                };
                c
            };
        let resolver = self.series_resolver.clone();
        self.sketch_index
            .ingest_precompute_for_agg_config(
                |metric, fp, ak| resolver.resolve(metric, fp, ak),
                agg_cfg,
                output,
                accumulator,
            )
            .is_some()
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
        for (output, accumulator) in &outputs {
            self.append_to_index(output, accumulator.as_ref());
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
    use crate::precompute_engine::operators::SumAccumulator;
    use crate::storage_engines::sketch_db::index::{AggKind, SidLookup};
    use crate::storage_engines::types::{KeyByLabelValues, StreamingConfig};
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use promql_utilities::query_logics::enums::AggregationType;
    use std::collections::HashMap;

    fn sum_agg_config(_id: u64, metric: &str, grouping_keys: &[&str]) -> AggregationConfig {
        // `_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`. Callers obtain the id
        // via `config.aggregation_id()`.
        AggregationConfig {
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
    fn sketch_index_sink_writes_to_index() {
        let cfg = sum_agg_config(7, "cpu_seconds", &["zone"]);
        let agg_id = cfg.aggregation_id();
        let mut configs = HashMap::new();
        configs.insert(agg_id, cfg);
        let streaming = StreamingConfig::new(configs);
        let hot_reload = HotReloadStreamingConfig::new(streaming.clone());

        let sketch_index = Arc::new(SketchStore::new());
        let sink = SketchStoreSink::new(
            sketch_index.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        );

        let key = KeyByLabelValues::new_with_labels(vec!["z0".to_string()]);
        let output = PrecomputedOutput::new(1000, 2000, Some(key), agg_id);
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(42.0));

        sink.emit_batch(vec![(output, acc)]).expect("emit ok");

        assert_eq!(
            sketch_index.instance_count(),
            1,
            "SketchStore should have one precompute instance"
        );
        let instances = sketch_index
            .list_by_status(crate::storage_engines::sketch_db::lifecycle::AggStatus::Active);
        assert_eq!(instances.len(), 1);
        let meta = instances[0].clone();
        let sid = meta.sid;
        assert_eq!(sketch_index.classify(sid), SidLookup::Hit);
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
        assert!(
            meta.capability.is_none(),
            "exact aggregations have no warm-tier sketch capability"
        );
    }

    #[test]
    fn sketch_index_sink_skips_unknown_agg_id_gracefully() {
        // Streaming config does NOT contain agg_id=99 — the sink
        // skips it (warn log) rather than panicking.
        let streaming = StreamingConfig::new(HashMap::new());
        let hot_reload = HotReloadStreamingConfig::new(streaming.clone());
        let sketch_index = Arc::new(SketchStore::new());
        let sink = SketchStoreSink::new(
            sketch_index.clone(),
            hot_reload,
            Arc::new(SeriesIdResolver::new()),
        );

        let output = PrecomputedOutput::new(1000, 2000, None, 99);
        let acc: Box<dyn AggregateCore> = Box::new(SumAccumulator::with_sum(1.0));
        sink.emit_batch(vec![(output, acc)]).expect("emit ok");
        assert_eq!(sketch_index.instance_count(), 0);
    }
}
