//! Build a backfill window accumulator from source samples in ingestion order.
//!
//! Backfill has its own worker pipeline and does not touch live active panes.
//! It shares the pure accumulator factory and update primitives with live ingest
//! so both paths use the same sketch semantics.

#[cfg(test)]
use asap_physical_operators::factory::AccumulatorUpdater;
#[cfg(test)]
use crate::tests::accumulator_fixture::create_fixture_accumulator;
#[cfg(test)]
use crate::precompute_engine::worker::apply_sample;
use crate::storage_engines::sketch_db::backfill::raw_sample_reader::RawSample;
use crate::storage_engines::types::AggregateCore;
#[cfg(test)]
use asap_types::aggregation_config::PrecomputeMaterialization;

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
#[cfg(test)]
pub fn build_backfilled_accumulator(
    config: &PrecomputeMaterialization,
    samples: &[RawSample],
) -> Box<dyn AggregateCore> {
    let mut updater: Box<dyn AccumulatorUpdater> = create_fixture_accumulator(config);
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
    use asap_types::aggregation_config::PrecomputeMaterialization;
    use asap_types::enums::WindowKind;
    use asap_types::AggregationType;
    use asap_types::KeyByLabelNames;
    use std::collections::HashMap;

    fn sum_config() -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
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
        use asap_physical_operators::accumulators::{
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

/// Backfill uses the same selected DAG producer and update expressions as live input.
pub fn build_dag_accumulator(
    program: &crate::precompute_engine::raw_dag::RawDagProgram,
    samples: &[RawSample],
) -> Result<Box<dyn AggregateCore>, String> {
    let mut updater = program.updater()?;
    let mut previous = std::collections::HashMap::new();
    for sample in samples {
        let value = if program.uses_counter_delta() {
            crate::precompute_engine::worker::reset_aware_counter_delta(
                &mut previous,
                &sample.labels,
                sample.value,
                sample.timestamp_ms,
            )
        } else {
            Some(sample.value)
        };
        if let Some(value) = value {
            program.apply(&mut *updater, &sample.labels, value, sample.timestamp_ms)?;
        }
    }
    Ok(updater.take_accumulator())
}
