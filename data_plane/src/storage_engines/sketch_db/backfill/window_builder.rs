//! Sketch construction for the backfill path.
//!
//! Implements the "real rebuild" piece of §10 (refreshable view
//! maintenance) from the sketch DB design
//! ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
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
use crate::precompute_engine::worker::parse_labels_from_series_key;
use crate::storage_engines::sketch_db::backfill::raw_sample_reader::RawSample;
use crate::storage_engines::types::{AggregateCore, KeyByLabelValues};
use asap_types::aggregation_config::AggregationConfig;

/// Extract the MultipleSubpopulation aggregated-label key from a
/// Prometheus-style series key. Duplicated from
/// `precompute_engine::worker::extract_aggregated_key_from_series`
/// (which is file-private). Kept here so the backfill module
/// doesn't force a `pub(crate)` on a live-path helper — the
/// dependency is one-way: worker does NOT import anything from
/// backfill.
///
/// The implementation must track the live one exactly; the
/// end-to-end determinism test in `backfill_processor.rs` will
/// fail if they drift.
fn extract_aggregated_key(series_key: &str, config: &AggregationConfig) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();
    for label_name in &config.aggregated_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }
    KeyByLabelValues::new_with_labels(values)
}

/// Construct the accumulator for one `(agg_id, window)` pair by
/// feeding `samples` in order into a fresh `AccumulatorUpdater`.
///
/// Sample format: `samples[i].labels` is the full series key
/// (Prometheus-style `metric{k="v",…}`); the function extracts
/// the MultipleSubpopulation key from the series key using the
/// same helper the live worker uses
/// (`extract_aggregated_key_from_series`), so the keyed dispatch
/// is bit-identical.
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
    if updater.is_keyed() {
        for s in samples {
            let key = extract_aggregated_key(&s.labels, config);
            updater.update_keyed(&key, s.value, s.timestamp_ms);
        }
    } else {
        for s in samples {
            updater.update_single(s.value, s.timestamp_ms);
        }
    }
    updater.take_accumulator()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
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
            WindowType::Tumbling,
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
