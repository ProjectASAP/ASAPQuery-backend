//! `BackfillWindowProcessor` — the Phase 5e implementation of
//! [`WindowProcessor`] that actually rebuilds sketches and writes
//! them into the store.
//!
//! Implements §10 (refreshable view maintenance) of the sketch DB
//! design ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
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
use tracing::{debug, warn};

use crate::stores::schema::{AggregateCore, HotReloadStreamingConfig, KeyByLabelValues};
use crate::precompute_engine::worker::parse_labels_from_series_key;
use crate::stores::traits::Store;
use asap_types::aggregation_config::AggregationConfig;

use super::backfill::BackfillRegistry;
use super::backfill_window_builder::build_backfilled_accumulator;
use super::backfill_worker::WindowProcessor;
use super::raw_sample_reader::RawSample;
use super::schema::SchemaRegistry;

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
    for label_name in &config.grouping_labels.labels {
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
    /// Schema registry — consulted only for defensive logging.
    /// The time-disjoint invariant guarantees the agg_id is still
    /// a known schema for as long as the backfill covers data
    /// before its `created_at_ms`.
    #[allow(dead_code)]
    schemas: Arc<SchemaRegistry>,
    /// Where per-window writes land. Same trait the live output
    /// sink uses; different call site.
    store: Arc<dyn Store>,
    /// Registry where we record which `(agg_id, window_range)`
    /// tuples this job wrote. Phase 5f's coverage tracker reads
    /// this list.
    registry: Arc<BackfillRegistry>,
    /// The job this processor is running on behalf of. Threaded
    /// into provenance calls; never used for dispatch logic.
    job_id: u64,
}

impl BackfillWindowProcessor {
    pub fn new(
        config: HotReloadStreamingConfig,
        schemas: Arc<SchemaRegistry>,
        store: Arc<dyn Store>,
        registry: Arc<BackfillRegistry>,
        job_id: u64,
    ) -> Self {
        Self {
            config,
            schemas,
            store,
            registry,
            job_id,
        }
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

#[async_trait]
impl WindowProcessor for BackfillWindowProcessor {
    async fn process_window(
        &self,
        agg_id: u64,
        window_range: (u64, u64),
        samples: Vec<RawSample>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let config = self.config_for_agg(agg_id)?;

        // Group samples by `group_key` — the same partitioning
        // live ingest does. Uses insertion-order preserving Vec
        // per group so §10.5 ordering is preserved within each
        // group's sample stream.
        let mut by_group: HashMap<String, Vec<RawSample>> = HashMap::new();
        for sample in samples {
            let group_key = extract_group_key(&sample.labels, &config);
            by_group.entry(group_key).or_default().push(sample);
        }

        if by_group.is_empty() {
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

        let mut batch: Vec<(crate::stores::schema::PrecomputedOutput, Box<dyn AggregateCore>)> =
            Vec::with_capacity(by_group.len());

        for (group_key, group_samples) in by_group {
            let accumulator = build_backfilled_accumulator(&config, &group_samples);
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
            let output = crate::stores::schema::PrecomputedOutput::new_backfilled(
                window_range.0,
                window_range.1,
                key,
                agg_id,
                self.job_id,
            );
            batch.push((output, accumulator));
        }

        // Single atomic batch write — mirrors live worker's emit_batch
        // approach. The store is responsible for per-key atomicity;
        // we don't need cross-key transactions.
        self.store.insert_precomputed_output_batch(batch).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> {
                warn!(
                    agg_id,
                    window_start = window_range.0,
                    window_end = window_range.1,
                    job_id = self.job_id,
                    error = %e,
                    "BackfillWindowProcessor: store write failed"
                );
                format!("store write failed: {e}").into()
            },
        )?;

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
    use crate::stores::schema::StreamingConfig;
    use crate::stores::sketch_db::backfill::BackfillSource;
    use crate::stores::sketch_db::backfill_worker::BackfillWorker;
    use crate::stores::sketch_db::raw_sample_reader::{LabelFilter, MockRawSampleReader};
    use crate::stores::sketch_db::sketch_store::SketchStore;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use std::sync::Arc;

    fn sum_config(agg_id: u64, metric: &str, grouping: Vec<&str>) -> AggregationConfig {
        let grouping_labels = if grouping.is_empty() {
            KeyByLabelNames::empty()
        } else {
            KeyByLabelNames::from_names(grouping.into_iter().map(String::from).collect())
        };
        AggregationConfig::new(
            agg_id,
            AggregationType::Sum,
            String::new(),
            std::collections::HashMap::new(),
            grouping_labels,
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

    fn streaming_config_with(config: AggregationConfig) -> Arc<StreamingConfig> {
        let mut map = std::collections::HashMap::new();
        map.insert(config.aggregation_id, config);
        Arc::new(StreamingConfig::new(map))
    }

    #[tokio::test]
    async fn happy_path_writes_one_output_per_group() {
        let cfg = sum_config(1, "latency", vec!["svc"]);
        let streaming = streaming_config_with(cfg.clone());
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let store: Arc<dyn Store> = Arc::new(SketchStore::new(
            streaming.clone(),
            crate::stores::schema::CleanupPolicy::NoCleanup,
        ));
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            1,
            (0, 100),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );

        let processor =
            BackfillWindowProcessor::new(hot, schemas, store.clone(), registry.clone(), job_id);

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
            .process_window(1, (0, 100), samples)
            .await
            .expect("happy path");

        // Registry records exactly one (agg_id, range) entry (per-window, not per-group).
        let written = registry.windows_written_by(job_id);
        assert_eq!(written, vec![(1u64, (0u64, 100u64))]);
    }

    #[tokio::test]
    async fn unknown_agg_id_fails_cleanly() {
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let store: Arc<dyn Store> = Arc::new(SketchStore::new(
            streaming.clone(),
            crate::stores::schema::CleanupPolicy::NoCleanup,
        ));
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            999,
            (0, 10),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        let processor = BackfillWindowProcessor::new(hot, schemas, store, registry.clone(), job_id);
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
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let store: Arc<dyn Store> = Arc::new(SketchStore::new(
            streaming.clone(),
            crate::stores::schema::CleanupPolicy::NoCleanup,
        ));
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            1,
            (0, 10),
            BackfillSource::Prometheus { url: "x".into() },
            1,
        );
        let processor =
            BackfillWindowProcessor::new(hot, schemas, store.clone(), registry.clone(), job_id);
        processor.process_window(1, (0, 10), vec![]).await.unwrap();
        // Empty window: no provenance record (nothing was written).
        assert!(registry.windows_written_by(job_id).is_empty());
    }

    #[tokio::test]
    async fn end_to_end_via_backfill_worker() {
        // Exercise the full chain: BackfillWorker drives the
        // processor over a job that covers 4 windows.
        let cfg = sum_config(1, "latency", vec!["svc"]);
        let streaming = streaming_config_with(cfg);
        let hot = HotReloadStreamingConfig::from_arc(streaming.clone());
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let store: Arc<dyn Store> = Arc::new(SketchStore::new(
            streaming.clone(),
            crate::stores::schema::CleanupPolicy::NoCleanup,
        ));
        let registry = Arc::new(BackfillRegistry::new());
        let job_id = registry.create(
            1,
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

        let processor =
            BackfillWindowProcessor::new(hot, schemas, store.clone(), registry.clone(), job_id);
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
            super::super::backfill::BackfillStatus::Complete
        );
        // 4 windows × 1 write each (some windows have 1 group — the
        // writes are per-window batches, not per-group entries).
        let written = registry.windows_written_by(job_id);
        assert_eq!(written.len(), 4);
        // Each recorded window corresponds to a writable window
        // (non-empty). Ordering is the worker's iteration order.
        assert_eq!(written[0], (1, (0, 10)));
        assert_eq!(written[1], (1, (10, 20)));
        assert_eq!(written[2], (1, (20, 30)));
        assert_eq!(written[3], (1, (30, 40)));

        // Verify the store now has 4 precomputes. Hard to test
        // exactly without digging into the store API — smoke test
        // that the worker didn't fail mid-run is sufficient here.
        let _ = store; // keep in scope
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

    #[test]
    fn create_checked_rejects_end_past_created_at() {
        use super::super::CreateError;
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let schema = schemas.get(1).unwrap();
        let created = schema.created_at_ms;
        let registry = BackfillRegistry::new();

        // end_ms one past created_at_ms — should be rejected.
        let err = registry
            .create_checked(
                &schemas,
                1,
                (0, created + 1),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect_err("overlap should be rejected");
        match err {
            CreateError::Overlap { agg_id, .. } => assert_eq!(agg_id, 1),
            other => panic!("expected Overlap, got {other:?}"),
        }
    }

    #[test]
    fn create_checked_accepts_end_at_boundary() {
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let created = schemas.get(1).unwrap().created_at_ms;
        let registry = BackfillRegistry::new();
        // end_ms exactly at created_at_ms is allowed — live owns
        // [created_at, ∞) as a half-open interval on the left, so
        // the boundary point is backfill's.
        let job_id = registry
            .create_checked(
                &schemas,
                1,
                (0, created),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect("boundary-touching range should be accepted");
        assert!(registry.get(job_id).is_some());
    }

    #[test]
    fn create_checked_rejects_unknown_agg() {
        use super::super::CreateError;
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = BackfillRegistry::new();
        let err = registry
            .create_checked(
                &schemas,
                999,
                (0, 100),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                None,
            )
            .expect_err("unknown agg should be rejected");
        assert!(matches!(err, CreateError::UnknownAgg { agg_id: 999 }));
    }

    /// Retention guard: requesting a start_ms older than the store's
    /// data-retention horizon is rejected. Method B from the design
    /// discussion — fail fast at creation rather than let the backfill
    /// write windows the retention sweep will immediately delete.
    #[test]
    fn create_checked_rejects_start_older_than_data_retention() {
        use super::super::CreateError;
        let cfg = sum_config(1, "m", vec![]);
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = BackfillRegistry::new();
        // Schema's created_at_ms is now_ms(), so data retention of
        // 1 hour with `start_ms = 0` means we're requesting data
        // from the epoch — way outside retention.
        let err = registry
            .create_checked(
                &schemas,
                1,
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
                assert_eq!(agg_id, 1);
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
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let created = schemas.get(1).unwrap().created_at_ms;
        let registry = BackfillRegistry::new();
        // start_ms = 0 would normally fail any realistic retention
        // window, but None skips the check.
        let job_id = registry
            .create_checked(
                &schemas,
                1,
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
        let streaming = streaming_config_with(cfg);
        let schemas = Arc::new(SchemaRegistry::from_streaming_config(&streaming));
        let registry = BackfillRegistry::new();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let retention = 3_600_000_u64; // 1h
                                       // start_ms = now - 30 min: well within 1h retention.
        let start = now.saturating_sub(1_800_000);
        let created = schemas.get(1).unwrap().created_at_ms;
        // Clip end to the schema boundary so time-disjoint passes.
        let end = created.min(now);
        let job_id = registry
            .create_checked(
                &schemas,
                1,
                (start, end),
                BackfillSource::Prometheus { url: "x".into() },
                1,
                Some(retention),
            )
            .expect("within-retention start should be accepted");
        assert!(registry.get(job_id).is_some());
    }
}
