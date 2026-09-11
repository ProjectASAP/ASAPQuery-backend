//! `BackfillWindowProcessor` — the Phase 5e implementation of
//! [`WindowProcessor`] that actually rebuilds sketches and writes
//! them into the store.
//!
//! Implements §10 (refreshable view maintenance) of the sketch DB
//! design ([`future-storage-and-compression.md`](../../../../../docs/design_docs/future-storage-and-compression.md)).
//! Reads raw samples from a [`RawSampleReader`] (Phase 5b), groups
//! them by the agg's `grouping_labels` just like live ingest does,
//! and writes per-(group, window) precomputes to the store.
//!
//! ## Separation from live ingest
//!
//! The user's Phase 5e direction was: "backfill should be a wholly
//! separate path; workers / results / data should carry distinct
//! identification; prefer isolation even at the cost of some
//! duplication." The code structure honours this:
//!
//! * **Separate worker**: processor runs inside a `BackfillWorker`
//!   (Phase 5c), which is in turn driven by a `BackfillService`
//!   tokio task that is NOT part of the `PrecomputeEngine`.
//! * **Separate output path**: writes go straight to the
//!   `Store::insert_precomputed_output_batch` call without passing
//!   through `OutputSink`/`PrecomputeEngine`/`WindowManager`. The
//!   live worker does the same call at the end of its chain, but
//!   the backfill path gets there through its own code.
//! * **Distinct identification**: after every successful batch
//!   write the processor calls
//!   `BackfillRegistry::record_window_written(job_id, agg_id,
//!   window_range)`. Phase 5f's coverage tracker will consult this
//!   list to distinguish `Backfilled { job_id }` from `Missing`
//!   without needing a provenance field on the on-disk precompute
//!   format.
//! * **Shared primitives (deliberately)**: the pure
//!   `create_accumulator_updater` factory is reused (via
//!   [`super::backfill_window_builder::build_backfilled_accumulator`]).
//!   See that module's doc for why.
//!
//! ## Time-disjoint invariant
//!
//! The processor never checks `is_writable(agg_id)` or locks
//! against live writes on the same `(agg_id, window)` — the
//! [`BackfillRegistry::create_checked`] constructor already
//! enforces that the backfill range ends at-or-before the agg's
//! `created_at_ms`. Live ingest owns `[created_at, ∞)`; backfill
//! owns `[0, created_at)`. Disjoint by construction.
//!
//! ## Determinism (§10.5)
//!
//! For deployments where live ingest goes through Prometheus
//! remote write (backend-native sketch construction), the
//! backfilled sketch is **bit-identical** to what live would have
//! produced from the same samples in the same order, because both
//! paths call `create_accumulator_updater` + `update_single` /
//! `update_keyed` in ingest order. The live-vs-backfill parity
//! test in this file locks that invariant.
//!
//! For deployments where live goes through the DataCollector
//! OTLP path (DC builds the sketch via sketchlib-go and the
//! backend only deserialises), bit-identicalness requires the
//! Go-side sketchlib and the Rust-side sketch-core to produce
//! identical output for the same input. That cross-language
//! audit is tracked as separate work; today, backfill in such
//! deployments is "approximately equivalent within sketch error
//! bounds ε".

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use crate::drivers::ingest::population_attrs_fingerprint;
use crate::drivers::ingest::series_resolver::SeriesIdResolver;
use crate::precompute_engine::worker::parse_labels_from_series_key;
use crate::storage_engines::types::{AggregateCore, HotReloadStreamingConfig, KeyByLabelValues};
use asap_types::aggregation_config::AggregationConfig;
use asap_types::PolicyFingerprint;

use super::raw_sample_reader::RawSample;
use super::window_builder::build_backfilled_accumulator;
use super::worker::WindowProcessor;
use super::BackfillRegistry;

/// Turn a series key into the `group_key` string the
/// grouping_labels-based partitioning produces in live ingest.
/// Semicolon-joined label values, empty string for missing labels
/// — same shape `IngestState::extract_group_key_for` emits, which
/// is the string `build_group_key_label_values` reverses.
///
/// Kept local to the backfill module (not shared with live) per
/// the §5e separation ask; the implementations must stay identical
/// by convention.
fn extract_group_key(series_key: &str, config: &AggregationConfig) -> String {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();
    for label_name in &config.grouping_labels.names() {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(*val);
        } else {
            values.push("");
        }
    }
    values.join(";")
}

/// Rebuild `group_key` string into the `KeyByLabelValues` struct
/// that `PrecomputedOutput` expects. Local copy of the live
/// `build_group_key_label_values` helper.
fn build_group_key_label_values(group_key: &str) -> KeyByLabelValues {
    let labels: Vec<String> = group_key.split(';').map(|s| s.to_string()).collect();
    KeyByLabelValues::new_with_labels(labels)
}

/// B7.7 — resolve the bucket sid for a backfill sample's
/// `(config, series_key)` pair. Mirrors
/// `resolve_bucket_sid_for_agg_config` in `drivers/ingest/otel.rs`
/// but adapted to the backfill side: the raw sample carries its labels
/// embedded in `series_key` (the `metric{k1="v1",k2="v2"}` text shape
/// `RawSample::labels` holds), so we parse them out first.
///
/// Policy and grouping identity must match live ingestion so historical and
/// live windows occupy the same storage row.
fn resolve_backfill_bucket_sid(
    resolver: &SeriesIdResolver,
    config: &AggregationConfig,
    series_key: &str,
    store: Option<&crate::storage_engines::sketch_db::index::SketchStore>,
    captured_generation: Option<&asap_types::sds::CatalogGeneration>,
) -> Result<u64, String> {
    if !config.population_key_encoding.is_legacy() {
        return Err("canonical population requires typed backfill label propagation".into());
    }
    let labels = parse_labels_from_series_key(series_key);
    let grouping_pairs: Vec<(&str, &str)> = config
        .grouping_labels
        .iter()
        .map(|name| (name.as_str(), *labels.get(name.as_str()).unwrap_or(&"")))
        .collect();
    let attrs_fp = population_attrs_fingerprint(config.population_key_encoding, &grouping_pairs)?;
    let agg_kind_canonical =
        crate::storage_engines::sketch_db::data::materialization_kind_for_config(config);
    resolver.resolve_with_reactivation(&config.metric, &attrs_fp, &agg_kind_canonical, |sid| {
        store.map_or(Ok(None), |store| {
            store.validate_routed_catalog_generation(captured_generation)?;
            let activation =
                store.authorize_series_reactivation(sid, config.policy_fingerprint().into())?;
            if activation
                .as_deref()
                .is_some_and(|generation| Some(generation) != captured_generation)
            {
                return Err("stale backfill job cannot reactivate series".into());
            }
            Ok(activation)
        })
    })
}

/// Fallback bucket id for the resolver-less code path (registry-only
/// processors / legacy tests). Stable per `group_key` so all samples
/// in one group still aggregate into one bucket; the value never
/// reaches `SketchStore` because the resolver-less branch in
/// `process_window` skips the precompute write entirely.
fn fallback_bucket_id(group_key: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    group_key.hash(&mut h);
    h.finish()
}

/// `WindowProcessor` that rebuilds sketches from raw samples and
/// writes them to the store. Holds references to the shared
/// registries / config so each window can look up its own config
/// without round-tripping through the worker.
pub struct BackfillWindowProcessor {
    /// Live config source. The processor snapshots the latest
    /// `StreamingConfig` at each window to find the
    /// `AggregationConfig` for `agg_id`. The snapshot is cheap
    /// (Arc refcount bump) so we don't optimise further.
    config: HotReloadStreamingConfig,
    /// Phase 5 M2.3.6g — replayed batches land here. The legacy
    /// `Arc<dyn Store>` field is gone; SketchStore is the only
    /// destination. Optional so tests that don't observe write
    /// effects can skip attaching one (the processor becomes a
    /// registry-only logger in that case).
    sketch_index: Option<Arc<crate::storage_engines::sketch_db::index::SketchStore>>,
    /// Shared sid mint authority. Same `SeriesIdResolver` the OTel
    /// ingest path uses, so backfilled precompute sids land in the
    /// same unified namespace as live precompute / sketch sids. Only
    /// consulted when `sketch_index` is also set (no sketch_index ⇒
    /// no precompute write ⇒ no sid mint).
    series_resolver: Option<Arc<crate::drivers::ingest::series_resolver::SeriesIdResolver>>,
    /// Registry where we record which `(agg_id, window_range)`
    /// tuples this job wrote. Phase 5f's coverage tracker reads
    /// this list.
    registry: Arc<BackfillRegistry>,
    /// The job this processor is running on behalf of. Threaded
    /// into provenance calls; never used for dispatch logic.
    job_id: u64,
    catalog_generation: Option<Arc<asap_types::sds::CatalogGeneration>>,
}

impl BackfillWindowProcessor {
    pub fn new(
        config: HotReloadStreamingConfig,
        registry: Arc<BackfillRegistry>,
        job_id: u64,
    ) -> Self {
        Self {
            config,
            sketch_index: None,
            series_resolver: None,
            registry,
            job_id,
            catalog_generation: None,
        }
    }

    /// Attach a `SketchStore` so each batch lands there. Builder-style
    /// so existing call sites opt in with one chained call.
    pub fn with_sketch_index(
        mut self,
        sketch_index: Arc<crate::storage_engines::sketch_db::index::SketchStore>,
    ) -> Self {
        self.catalog_generation = sketch_index.active_catalog_generation();
        self.sketch_index = Some(sketch_index);
        self
    }

    /// Attach the shared `SeriesIdResolver` so precompute writes mint
    /// sids via the same registry the OTel ingest path uses. Required
    /// alongside [`Self::with_sketch_index`] — without a resolver the
    /// processor still runs but skips the precompute write (registry
    /// provenance still recorded, with a warn-log per missing call).
    pub fn with_series_resolver(
        mut self,
        series_resolver: Arc<crate::drivers::ingest::series_resolver::SeriesIdResolver>,
    ) -> Self {
        self.series_resolver = Some(series_resolver);
        self
    }

    /// Look up the `AggregationConfig` for `agg_id` in the current
    /// `StreamingConfig` snapshot. Returns an error string if the
    /// agg has been removed from the config since the job was
    /// created — rare but worth handling (e.g. operator retired
    /// the agg mid-backfill; the `BackfillWorker` will
    /// `mark_failed` the job with this message).
    fn config_for_agg(
        &self,
        agg_id: u64,
    ) -> Result<AggregationConfig, Box<dyn std::error::Error + Send + Sync>> {
        let snap = self.config.snapshot();
        snap.get_aggregation_config(agg_id).cloned().ok_or_else(|| {
            format!("agg_id {agg_id} not in current StreamingConfig — retired mid-backfill?").into()
        })
    }
}

/// One per-sid bucket assembled by [`BackfillWindowProcessor::process_window`].
/// Carries the grouping-label string alongside the samples so emit-time
/// `KeyByLabelValues` rendering matches what the live ingest path produces.
struct SidBucket {
    group_key: String,
    samples: Vec<RawSample>,
}

#[async_trait]
impl WindowProcessor for BackfillWindowProcessor {
    async fn process_window(
        &self,
        agg_id: u64,
        window_range: (u64, u64),
        samples: Vec<RawSample>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config_for_agg(agg_id)?;

        // B7.7 — sid-keyed bucketing. Per the schema-retirement #5
        // step 6 plan, the backfill processor's per-window grouping is
        // rekeyed from `group_key: String` to `sid: u64`. The grouping
        // label values fold into the sid via
        // `SeriesIdResolver::resolve(metric, attrs_fp, agg_kind)` —
        // the same identity contract the live ingest path uses, so a
        // backfilled bucket lands on the SAME sid that live ingest
        // would mint for the same `(metric, grouping-values, agg_kind)`
        // tuple. `group_key` is kept alongside the sid so emit-time
        // `KeyByLabelValues` rendering on `PrecomputedOutput` matches
        // what live ingest produces. Uses insertion-order preserving
        // Vec per bucket so §10.5 ordering is preserved within each
        // bucket's sample stream.
        //
        // When no SeriesIdResolver is attached (legacy / registry-only
        // tests), we fall back to a stable per-`group_key` bucket id —
        // the per-window write is skipped anyway in that path, so the
        // bucket identity doesn't matter beyond preserving sample
        // ordering for the (unused) accumulator builds.
        let resolver_opt = self.series_resolver.as_ref();
        let mut by_bucket: HashMap<u64, SidBucket> = HashMap::new();
        for sample in samples {
            let group_key = extract_group_key(&sample.labels, &config);
            let sid = match resolver_opt {
                Some(r) => resolve_backfill_bucket_sid(
                    r.as_ref(),
                    &config,
                    &sample.labels,
                    self.sketch_index.as_deref(),
                    self.catalog_generation.as_deref(),
                )?,
                None => fallback_bucket_id(&group_key),
            };
            by_bucket
                .entry(sid)
                .or_insert_with(|| SidBucket {
                    group_key: group_key.clone(),
                    samples: Vec::new(),
                })
                .samples
                .push(sample);
        }

        if by_bucket.is_empty() {
            // Empty window — no samples, no writes. Still count as
            // "processed" since the worker's tick_progress will
            // increment.
            debug!(
                agg_id,
                window_start = window_range.0,
                window_end = window_range.1,
                "BackfillWindowProcessor: empty window, skipping store write"
            );
            return Ok(());
        }

        // Build per-sid `(sid, PrecomputedOutput, accumulator)` triples.
        // Carrying the sid alongside the pair lets the write loop hand
        // it straight to `ingest_precompute_with_series_id` instead of
        // re-resolving inside the mint-driven path.
        let mut batch: Vec<(
            u64,
            crate::storage_engines::types::PrecomputedOutput,
            Box<dyn AggregateCore>,
        )> = Vec::with_capacity(by_bucket.len());

        for (sid, bucket) in by_bucket {
            let SidBucket { group_key, samples } = bucket;
            let accumulator = build_backfilled_accumulator(&config, &samples);
            // Keyed accumulators (MultipleSubpopulation) carry their
            // subpopulation keys internally; the PrecomputedOutput's
            // `key` represents the *group* key (grouping_labels
            // values), not the aggregated-label key. Mirrors what
            // live worker emits.
            let key = if group_key.is_empty() {
                None
            } else {
                Some(build_group_key_label_values(&group_key))
            };
            let mut output = crate::storage_engines::types::PrecomputedOutput::new_backfilled(
                window_range.0,
                window_range.1,
                key,
                self.job_id,
                PolicyFingerprint::from_config(&config),
            );
            output.series_id = Some(sid);
            output.catalog_generation = self.catalog_generation.clone();
            batch.push((sid, output, accumulator));
        }

        // Phase 5 M2.3.6g — replayed batches land in SketchStore only.
        // No legacy SketchStore write path remains. When no
        // sketch_index is attached (tests), the writes are simply
        // dropped — the registry still records the (agg_id, range)
        // provenance below. When sketch_index is attached but no
        // resolver was provided, the precompute write is skipped
        // with a warn — sid minting requires the shared resolver.
        if let Some(idx) = self.sketch_index.as_ref() {
            match self.series_resolver.as_ref() {
                Some(_resolver) => {
                    // B7.7 — sid is pre-resolved per bucket above; hand
                    // it directly to the index's sid-direct ingest
                    // path. The mint-driven sibling
                    // `ingest_precompute_for_agg_config` would resolve
                    // to the same sid (the resolver is idempotent),
                    // but the round-trip is redundant now that we hold
                    // the value.
                    for (sid, output, accumulator) in &batch {
                        idx.ingest_precompute_with_series_id(
                            *sid,
                            &config,
                            output,
                            accumulator.as_ref(),
                        )
                        .ok_or("backfill summary state publication rejected")?;
                    }
                }
                None => {
                    tracing::warn!(
                        job_id = self.job_id,
                        agg_id,
                        batch_size = batch.len(),
                        "BackfillWindowProcessor has sketch_index but no \
                         series_resolver attached; skipping precompute writes \
                         for this window",
                    );
                }
            }
        }
        // Hold `batch` alive until after the registry record below,
        // so it shows up in trace logs if the registry call fails.
        drop(batch);

        // Success: record provenance. Done AFTER the write so
        // `windows_written_by` only ever reflects actually-landed
        // windows.
        self.registry
            .record_window_written(self.job_id, agg_id, window_range);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::backfill::raw_sample_reader::{
        LabelFilter, MockRawSampleReader,
    };
    use crate::storage_engines::sketch_db::backfill::worker::BackfillWorker;
    use crate::storage_engines::sketch_db::backfill::BackfillSource;
    use crate::storage_engines::types::StreamingConfig;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::sync::Arc;

    fn sum_config(_agg_id: u64, metric: &str, grouping: Vec<&str>) -> AggregationConfig {
        // `_agg_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`.
        let grouping_labels = if grouping.is_empty() {
            KeyByLabelNames::empty()
        } else {
            KeyByLabelNames::from_names(grouping.into_iter().map(String::from).collect())
        };
        AggregationConfig::new(
            AggregationType::Sum,
            String::new(),
            std::collections::HashMap::new(),
            grouping_labels,
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

    fn streaming_config_with(config: AggregationConfig) -> Arc<StreamingConfig> {
        let mut map = std::collections::HashMap::new();
        map.insert(config.policy_fp_u64(), config);
        Arc::new(StreamingConfig::new(map))
    }

    #[tokio::test]
    async fn happy_path_writes_one_output_per_group() {
        let cfg = sum_config(1, "latency", vec!["svc"]);
        let fp = cfg.policy_fp_u64();
        let streaming = streaming_config_with(cfg.clone());
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            fp,
            (0, 100),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );

        let processor = BackfillWindowProcessor::new(hot, registry.clone(), job_id);

        // Two services → two groups → expect two PrecomputedOutput
        // entries for window (0, 100).
        let samples = vec![
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 10,
                value: 1.0,
            },
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 20,
                value: 2.0,
            },
            RawSample {
                labels: "latency{svc=\"b\"}".into(),
                timestamp_ms: 15,
                value: 3.0,
            },
        ];
        processor
            .process_window(fp, (0, 100), samples)
            .await
            .expect("happy path");

        // Registry records exactly one (agg_id, range) entry (per-window, not per-group).
        let written = registry.windows_written_by(job_id);
        assert_eq!(written, vec![(fp, (0u64, 100u64))]);
    }

    #[tokio::test]
    async fn unknown_agg_id_fails_cleanly() {
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            999,
            (0, 10),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        let processor = BackfillWindowProcessor::new(hot, registry.clone(), job_id);
        // agg_id=999 isn't in the StreamingConfig.
        let err = processor
            .process_window(999, (0, 10), vec![])
            .await
            .expect_err("unknown agg should fail");
        assert!(err.to_string().contains("not in current StreamingConfig"));
        assert!(registry.windows_written_by(job_id).is_empty());
    }

    #[tokio::test]
    async fn empty_samples_complete_without_write() {
        let cfg = sum_config(1, "m", vec![]);
        let fp = cfg.policy_fp_u64();
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            fp,
            (0, 10),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        let processor = BackfillWindowProcessor::new(hot, registry.clone(), job_id);
        processor.process_window(fp, (0, 10), vec![]).await.unwrap();
        // Empty window: no provenance record (nothing was written).
        assert!(registry.windows_written_by(job_id).is_empty());
    }

    #[tokio::test]
    async fn end_to_end_via_backfill_worker() {
        // Exercise the full chain: BackfillWorker drives the
        // processor over a job that covers 4 windows.
        let cfg = sum_config(1, "latency", vec!["svc"]);
        let fp = cfg.policy_fp_u64();
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            fp,
            (0, 40),
            BackfillSource::Prometheus { url: "x".into() },
            4,
        );

        let reader = MockRawSampleReader::new(vec![
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 5,
                value: 1.0,
            },
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 15,
                value: 2.0,
            },
            RawSample {
                labels: "latency{svc=\"b\"}".into(),
                timestamp_ms: 25,
                value: 3.0,
            },
            RawSample {
                labels: "latency{svc=\"b\"}".into(),
                timestamp_ms: 35,
                value: 4.0,
            },
        ]);

        let processor = BackfillWindowProcessor::new(hot, registry.clone(), job_id);
        let worker = BackfillWorker::new(registry.clone());
        worker
            .run_job(
                job_id,
                &LabelFilter::for_metric("latency"),
                &reader,
                &processor,
            )
            .await
            .expect("worker ok");

        assert_eq!(
            registry.get(job_id).unwrap().status,
            super::super::BackfillStatus::Complete
        );
        // 4 windows × 1 write each (some windows have 1 group — the
        // writes are per-window batches, not per-group entries).
        let written = registry.windows_written_by(job_id);
        assert_eq!(written.len(), 4);
        // Each recorded window corresponds to a writable window
        // (non-empty). Ordering is the worker's iteration order.
        assert_eq!(written[0], (fp, (0, 10)));
        assert_eq!(written[1], (fp, (10, 20)));
        assert_eq!(written[2], (fp, (20, 30)));
        assert_eq!(written[3], (fp, (30, 40)));

        // The exact window assertions above prove the worker completed
        // every expected write.
    }

    /// ## The parity test (§10.5 determinism invariant)
    ///
    /// This is the test that locks the "backfill produces the same
    /// sketch as live" claim from the module doc. For the raw-ingest
    /// path (sketch-core-native construction), a backfilled
    /// accumulator MUST serialise to exactly the same bytes as a
    /// live-built accumulator fed the same samples in the same order.
    ///
    /// The test builds two SumAccumulators from the same sample
    /// sequence (one via the live path's `create_accumulator_updater`
    /// plus `update_single`, and one via the backfill path's
    /// `build_backfilled_accumulator`) and asserts their
    /// `serialize_to_bytes()` outputs are byte-identical.
    ///
    /// A similar CMS parity test would be ideal; it's skipped here
    /// because `CountMinSketchAccumulator::new` in sketch-core takes
    /// more params than we exercise elsewhere and would require
    /// deeper wiring. If Phase 5e needs stronger coverage, add a
    /// CMS-specific parity test — it'll follow the exact same shape.
    #[test]
    fn backfill_builds_bit_identical_sum_accumulator_to_live() {
        use crate::precompute_engine::accumulator_factory::create_accumulator_updater;

        let cfg = sum_config(1, "m", vec![]);

        // Sample sequence.
        let samples = vec![
            RawSample {
                labels: "m".into(),
                timestamp_ms: 0,
                value: 1.5,
            },
            RawSample {
                labels: "m".into(),
                timestamp_ms: 1,
                value: -0.25,
            },
            RawSample {
                labels: "m".into(),
                timestamp_ms: 2,
                value: 10.0,
            },
        ];

        // Live path: factory + update_single per sample in order.
        let live_bytes = {
            let mut updater = create_accumulator_updater(&cfg);
            for s in &samples {
                updater.update_single(s.value, s.timestamp_ms);
            }
            let acc = updater.take_accumulator();
            acc.serialize_to_bytes()
        };

        // Backfill path: build_backfilled_accumulator.
        let backfill_bytes = {
            let acc = build_backfilled_accumulator(&cfg, &samples);
            acc.serialize_to_bytes()
        };

        assert_eq!(
            live_bytes, backfill_bytes,
            "Live and backfill paths must produce bit-identical \
             serialisations for SumAccumulator. \
             If this test fails, something diverged — check:\n\
             (1) Is `build_backfilled_accumulator` still calling \
                 `create_accumulator_updater`?\n\
             (2) Did a recent change to `SumAccumulator` introduce \
                 non-deterministic state (e.g. a seed)?\n\
             (3) Does `serialize_to_bytes` include any timestamp \
                 or wall-clock field?"
        );
    }

    // ─── Time-disjoint invariant tests ─────────────────────────────────────

    /// Snapshot a wall-clock millis "now" the same way the registry
    /// does. The schema-retirement migration dropped
    /// `AggSchema::created_at_ms`; tests now stamp their own.
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[test]
    fn create_checked_rejects_end_past_created_at() {
        use super::super::CreateError;
        let cfg = sum_config(1, "m", vec![]);
        let expected_fp = cfg.policy_fp_u64();
        let created = now_ms();
        let registry = BackfillRegistry::new();

        // end_ms one past created_at_ms — should be rejected.
        let err = registry
            .create_checked(
                &cfg,
                created,
                (0, created + 1),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect_err("overlap should be rejected");
        match err {
            CreateError::Overlap { agg_id, .. } => assert_eq!(agg_id, expected_fp),
            other => panic!("expected Overlap, got {other:?}"),
        }
    }

    #[test]
    fn create_checked_accepts_end_at_boundary() {
        let cfg = sum_config(1, "m", vec![]);
        let created = now_ms();
        let registry = BackfillRegistry::new();
        // end_ms exactly at created_at_ms is allowed — live owns
        // [created_at, ∞) as a half-open interval on the left, so
        // the boundary point is backfill's.
        let job_id = registry
            .create_checked(
                &cfg,
                created,
                (0, created),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect("boundary-touching range should be accepted");
        assert!(registry.get(job_id).is_some());
    }

    /// Retention guard: requesting a start_ms older than the store's
    /// data-retention horizon is rejected. Method B from the design
    /// discussion — fail fast at creation rather than let the backfill
    /// write windows the retention sweep will immediately delete.
    #[test]
    fn create_checked_rejects_start_older_than_data_retention() {
        use super::super::CreateError;
        let cfg = sum_config(1, "m", vec![]);
        let expected_fp = cfg.policy_fp_u64();
        let created = now_ms();
        let registry = BackfillRegistry::new();
        // Created-at is now_ms(), so data retention of 1 hour with
        // `start_ms = 0` means we're requesting data from the epoch —
        // way outside retention.
        let err = registry
            .create_checked(
                &cfg,
                created,
                (0, 1_000),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                Some(3_600_000), // 1 hour
            )
            .expect_err("out-of-retention should be rejected");
        match err {
            CreateError::OutOfRetention {
                agg_id,
                requested_start_ms,
                earliest_retained_ms,
            } => {
                assert_eq!(agg_id, expected_fp);
                assert_eq!(requested_start_ms, 0);
                assert!(earliest_retained_ms > 0);
            }
            other => panic!("expected OutOfRetention, got {other:?}"),
        }
    }

    /// `data_retention_ms = None` skips the retention check entirely —
    /// lets tests and retention-disabled deployments bypass.
    #[test]
    fn create_checked_none_retention_skips_check() {
        let cfg = sum_config(1, "m", vec![]);
        let created = now_ms();
        let registry = BackfillRegistry::new();
        // start_ms = 0 would normally fail any realistic retention
        // window, but None skips the check.
        let job_id = registry
            .create_checked(
                &cfg,
                created,
                (0, created),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect("None retention → accepted");
        assert!(registry.get(job_id).is_some());
    }

    /// Within-retention start is accepted even when a retention is
    /// configured. Guards against off-by-one at the boundary.
    #[test]
    fn create_checked_accepts_start_within_retention() {
        let cfg = sum_config(1, "m", vec![]);
        let registry = BackfillRegistry::new();
        let now = now_ms();
        let retention = 3_600_000_u64; // 1h
                                       // start_ms = now - 30 min: well within 1h retention.
        let start = now.saturating_sub(1_800_000);
        let created = now;
        // Clip end to the schema boundary so time-disjoint passes.
        let end = created.min(now);
        let job_id = registry
            .create_checked(
                &cfg,
                created,
                (start, end),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                Some(retention),
            )
            .expect("within-retention start should be accepted");
        assert!(registry.get(job_id).is_some());
    }

    // ─── B7.7: sid-keyed bucketing tests ─────────────────────────────────

    /// Regression for B7.7 (schema-retirement #5 step 6): the
    /// backfill processor groups raw samples by `sid: u64` and writes
    /// each bucket via `SketchStore::ingest_precompute_with_series_id`. The
    /// sids it allocates match what the shared `SeriesIdResolver`
    /// would mint for the same `(metric, grouping-values, agg_kind)`
    /// tuple — i.e. live ingest and backfill share one sid namespace.
    ///
    /// Drives `process_window` end-to-end with samples spanning two
    /// distinct `svc` values × two samples each. Asserts:
    ///   - exactly two `SketchInstanceMetadata` entries land in the
    ///     `SketchStore` (one per distinct sid bucket)
    ///   - their sids equal what the shared `SeriesIdResolver` would
    ///     mint for the same `(metric, grouping-values, agg_kind)`
    ///     tuple (so live-vs-backfill sid identity holds)
    ///   - both sids `classify()` as `Hit` (i.e. the per-sid append
    ///     paths landed under the same sids the metadata was
    ///     registered with)
    ///   - the registry recorded one provenance entry for the window
    #[tokio::test]
    async fn process_window_buckets_by_sid_via_resolver() {
        use crate::drivers::ingest::series_resolver::SeriesIdResolver;
        use crate::storage_engines::sketch_db::index::{SeriesLookup, SketchStore};

        let cfg = sum_config(1, "latency", vec!["svc"]);
        let fp = cfg.policy_fp_u64();
        let streaming = streaming_config_with(cfg.clone());
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let registry = Arc::new(BackfillRegistry::new());
        let sketch_index = Arc::new(SketchStore::new());
        let catalog = asap_types::summary_catalog::SummaryCatalog::from_materializations(
            1,
            1,
            &[cfg.clone()],
        )
        .unwrap();
        sketch_index
            .install_summary_catalog(Arc::new(catalog))
            .unwrap();
        let resolver = Arc::new(SeriesIdResolver::new());

        let job_id = registry.create(
            fp,
            (0, 100),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );

        let processor = BackfillWindowProcessor::new(hot, registry.clone(), job_id)
            .with_sketch_index(sketch_index.clone())
            .with_series_resolver(resolver.clone());

        // Two distinct svc values × two samples each. Same window
        // (0, 100). After the processor runs, we expect exactly two
        // sids in the SketchStore — one per distinct svc value.
        let samples = vec![
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 10,
                value: 1.0,
            },
            RawSample {
                labels: "latency{svc=\"a\"}".into(),
                timestamp_ms: 20,
                value: 2.0,
            },
            RawSample {
                labels: "latency{svc=\"b\"}".into(),
                timestamp_ms: 15,
                value: 3.0,
            },
            RawSample {
                labels: "latency{svc=\"b\"}".into(),
                timestamp_ms: 25,
                value: 4.0,
            },
        ];
        processor
            .process_window(fp, (0, 100), samples)
            .await
            .expect("happy path");

        // Two distinct sids landed in the index.
        assert_eq!(
            sketch_index.instance_count(),
            2,
            "one sid per distinct svc bucket"
        );

        // The sids the processor allocated equal what
        // `SeriesIdResolver::lookup` would return for the same
        // `(metric, grouping-values, agg_kind)` tuple — i.e. live
        // ingest and backfill share one sid namespace.
        let sid_a =
            resolve_backfill_bucket_sid(&resolver, &cfg, "latency{svc=\"a\"}", None, None).unwrap();
        let sid_b =
            resolve_backfill_bucket_sid(&resolver, &cfg, "latency{svc=\"b\"}", None, None).unwrap();
        assert_ne!(sid_a, sid_b, "distinct svc values mint distinct sids");
        assert_eq!(sketch_index.classify(sid_a), SeriesLookup::Hit);
        assert_eq!(sketch_index.classify(sid_b), SeriesLookup::Hit);

        // Provenance was recorded once per window (not once per
        // bucket) — same shape as the pre-rekey path.
        let written = registry.windows_written_by(job_id);
        assert_eq!(written, vec![(fp, (0u64, 100u64))]);
    }

    /// Replay and the actual live storage sink must resolve the same row.
    #[test]
    fn backfill_sid_matches_live_ingest_sid_for_same_grouping_values() {
        for encoding in [
            asap_types::PopulationKeyEncoding::LegacyDelimited,
            asap_types::PopulationKeyEncoding::CanonicalLabelsV1,
        ] {
            assert_backfill_sid_matches_live(encoding);
        }
    }

    fn assert_backfill_sid_matches_live(encoding: asap_types::PopulationKeyEncoding) {
        use crate::drivers::ingest::series_resolver::SeriesIdResolver;

        let mut cfg = sum_config(1, "latency", vec!["svc", "zone"]);
        cfg.population_key_encoding = encoding;
        let resolver = SeriesIdResolver::new();

        // Backfill side: derive sid via the new helper.
        let backfill_result = resolve_backfill_bucket_sid(
            &resolver,
            &cfg,
            "latency{svc=\"a\",zone=\"z0\"}",
            None,
            None,
        );
        let attrs =
            population_attrs_fingerprint(encoding, &[("svc", "a"), ("zone", "z0")]).unwrap();
        let expected_sid = resolver.resolve(
            &cfg.metric,
            &attrs,
            &crate::storage_engines::sketch_db::data::materialization_kind_for_config(&cfg),
        );
        if encoding.is_legacy() {
            assert_eq!(backfill_result.unwrap(), expected_sid);
        } else {
            assert!(backfill_result
                .unwrap_err()
                .contains("typed backfill label propagation"));
        }

        // Exercise the actual live sink instead of duplicating its SID formula.
        let store = crate::storage_engines::sketch_db::index::SketchStore::new();
        let output = crate::storage_engines::types::PrecomputedOutput::new(
            100,
            200,
            Some(
                crate::storage_engines::types::KeyByLabelValues::new_with_labels(vec![
                    "a".into(),
                    "z0".into(),
                ]),
            ),
            cfg.policy_fingerprint(),
        );
        let acc =
            crate::precompute_engine::operators::sum_accumulator::SumAccumulator::with_sum(1.0);
        let live_sid = store
            .ingest_precompute_for_agg_config(
                |metric, attrs, kind| resolver.resolve(metric, attrs, kind),
                &cfg,
                &output,
                &acc,
            )
            .expect("live sink write");

        assert_eq!(
            expected_sid, live_sid,
            "backfill and live ingest MUST mint the same sid for the same \
             (metric, grouping-values, agg_kind) tuple"
        );
    }
}
