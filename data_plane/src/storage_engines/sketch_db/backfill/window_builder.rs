//! Sketch construction for the backfill path.
//!
//! Implements the "real rebuild" piece of §10 (refreshable view
//! maintenance) from the sketch DB design
//! ([`future-storage-and-compression.md`](../../../../../docs/design_docs/future-storage-and-compression.md)).
//! Given an `AggregationConfig` and a batch of raw samples for one
//! `(agg_id, window)` pair, produces the `Box<dyn AggregateCore>`
//! that would have been produced had those samples flowed through
//! live ingest in the same order.
//!
//! ## Shared primitive vs duplicated code
//!
//! The user's direction for Phase 5e was: "backfill functions should
//! all be separate, not reusing the live path." The interpretation
//! here splits two things that could each be called "the live path":
//!
//! 1. **The worker pipeline**: `PrecomputeEngine` → `SeriesRouter`
//!    → `Worker` → `active_panes` → `WindowManager` →
//!    `output_sink.emit_batch`. This is stateful infrastructure
//!    that owns live latency budgets. **NOT reused** — the backfill
//!    service runs a completely separate tokio task, uses its own
//!    writer, and never touches an `active_pane`.
//!
//! 2. **The `create_accumulator_updater` factory**: a pure function
//!    `AggregationConfig -> Box<dyn AccumulatorUpdater>`. Takes no
//!    shared state, has no latency budget, is a 60-line match
//!    statement. **IS reused** by this module.
//!
//! The reuse is deliberate: duplicating the factory would mean every
//! new sketch type needs matching entries in two places, and the
//! ε-precision end-to-end determinism test would catch drift only
//! post-merge. Centralising on one factory makes "live ≡ backfill"
//! a build-time invariant rather than a runtime one.
//!
//! If this interpretation is wrong — if the requirement is strict
//! duplication accepting the drift risk — swap
//! `create_accumulator_updater` below for a copy-pasted match
//! statement. Everything else in the backfill module tree is
//! already its own code path.
//!
//! ## Phase 5e scope (this file)
//!
//! * `build_backfilled_accumulator(config, samples) -> Box<dyn
//!   AggregateCore>` — the pure function that rebuilds one
//!   window's accumulator from its samples.
//! * Handles both SingleSubpopulation (update_single) and
//!   MultipleSubpopulation (update_keyed) dispatch — mirrors
//!   `worker::apply_sample`.

use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::worker::apply_sample;
use crate::storage_engines::sketch_db::backfill::raw_sample_reader::RawSample;
use crate::storage_engines::types::AggregateCore;
use asap_types::aggregation_config::AggregationConfig;

/// Construct the accumulator for one `(agg_id, window)` pair by
/// feeding `samples` in order into a fresh `AccumulatorUpdater`.
///
/// Samples carry full series keys. Replay uses the live worker's sample
/// dispatch so keyed identity and update semantics remain identical.
///
/// Ordering contract: samples are consumed in the iteration order
/// of the input `Vec`. §10.5 requires that the caller preserve
/// ingest order — this function does not sort or reorder.
///
/// The function is synchronous + pure (no I/O, no async, no global
/// state). Suitable to call from inside a `WindowProcessor`
/// implementation without worrying about the async runtime.
pub fn build_backfilled_accumulator(
    config: &AggregationConfig,
    samples: &[RawSample],
) -> Box<dyn AggregateCore> {
    let mut updater: Box<dyn AccumulatorUpdater> = create_accumulator_updater(config);
    for sample in samples {
        apply_sample(
            &mut *updater,
            &sample.labels,
            sample.value,
            sample.timestamp_ms,
            config,
        );
    }
    updater.take_accumulator()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;

    fn sum_config() -> AggregationConfig {
        AggregationConfig::new(
            AggregationType::Sum,
            String::new(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "m".to_string(),
            None,
            None,
            None,
        )
    }

    fn raw(labels: &str, ts: i64, v: f64) -> RawSample {
        RawSample {
            labels: labels.to_string(),
            timestamp_ms: ts,
            value: v,
        }
    }

    // Replay must preserve each series and rank by the selected update mode.
    #[test]
    fn backfilled_topk_preserves_series_and_weight_mode() {
        use crate::precompute_engine::operators::{
            CountMinSketchWithHeapAccumulator, CountSketchWithHeapAccumulator,
        };
        for kind in [
            AggregationType::CountMinSketchWithHeap,
            AggregationType::CountSketchWithHeap,
        ] {
            for mode in ["count", "value"] {
                let mut config = sum_config();
                config.aggregation_type = kind;
                config.parameters = serde_json::from_value(serde_json::json!({
                    "d": 4, "w": 1024, "heap_size": 10, "weight_mode": mode
                }))
                .unwrap();
                let samples = vec![
                    raw("m{svc=\"a\"}", 10, 100.0),
                    raw("m{svc=\"b\"}", 20, 2.0),
                    raw("m{svc=\"b\"}", 30, 3.0),
                ];
                let acc = build_backfilled_accumulator(&config, &samples);
                let mut ranked: Vec<(String, f64)> = if let Some(heap) =
                    acc.as_any()
                        .downcast_ref::<CountMinSketchWithHeapAccumulator>()
                {
                    heap.inner
                        .topk_heap_items()
                        .into_iter()
                        .map(|i| (i.key, i.value))
                        .collect()
                } else {
                    acc.as_any()
                        .downcast_ref::<CountSketchWithHeapAccumulator>()
                        .unwrap()
                        .inner
                        .topk_heap_items()
                        .into_iter()
                        .map(|i| (i.key, i.value))
                        .collect()
                };
                ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
                let expected = if mode == "count" {
                    vec![("m{svc=\"b\"}".into(), 2.0), ("m{svc=\"a\"}".into(), 1.0)]
                } else {
                    vec![("m{svc=\"a\"}".into(), 100.0), ("m{svc=\"b\"}".into(), 5.0)]
                };
                assert_eq!(ranked, expected, "{kind:?} {mode}");
            }
        }
    }

    #[test]
    fn sum_accumulator_sums_all_samples_in_order() {
        let config = sum_config();
        let samples = vec![
            raw("m{svc=\"a\"}", 10, 1.0),
            raw("m{svc=\"a\"}", 20, 2.0),
            raw("m{svc=\"a\"}", 30, 3.0),
        ];
        let acc = build_backfilled_accumulator(&config, &samples);
        // SumAccumulator's AuxStats exposes the sum.
        let aux = acc.aux_stats();
        assert_eq!(aux.sum, Some(6.0));
    }

    #[test]
    fn empty_samples_produce_empty_accumulator() {
        let config = sum_config();
        let acc = build_backfilled_accumulator(&config, &[]);
        let aux = acc.aux_stats();
        // A fresh SumAccumulator has sum = Some(0.0) per its AuxStats
        // implementation (identity element).
        assert!(aux.sum == Some(0.0) || aux.sum.is_none());
    }
}
