//! Execute installed sketch-tier plans; errors allow routing to the exact backend.

use std::collections::BTreeMap;

use crate::query_engines::asap_query_engine::post_asap_readout::LoweringSkip;
use crate::query_engines::asap_query_engine::post_asap_readout::{
    execute_query_plan_instant, fold_coverage,
};
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::ASAPTierResult;

/// Disable warm execution with ASAP_SUMMARY_EXECUTOR_LIVE=0, false, or off.
pub fn summary_executor_live_enabled() -> bool {
    let value = std::env::var("ASAP_SUMMARY_EXECUTOR_LIVE").ok();
    summary_executor_live_value(value.as_deref())
}

fn summary_executor_live_value(value: Option<&str>) -> bool {
    value.map(str::trim).is_none_or(|v| {
        !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
    })
}

/// Evaluate a planned range query at the exact Prometheus timestamps
/// `start, start+step, ... <= end`. Each point executes the same immutable
/// QueryPlan DAG with its own lookback interval; native summary window-close
/// timestamps never leak into the public range response.
pub fn serve_range_steps_from_query_plan(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    start_ms: u64,
    end_ms: u64,
    step_ms: u64,
) -> Result<ASAPTierResult, LoweringSkip> {
    const MAX_RANGE_STEPS: usize = 11_000;
    if step_ms == 0 || start_ms > end_ms {
        return Err(LoweringSkip::InvalidQueryPlan(
            "range query requires start <= end and step > 0".into(),
        ));
    }
    let step_count = end_ms.saturating_sub(start_ms) / step_ms + 1;
    if step_count > MAX_RANGE_STEPS as u64 {
        return Err(LoweringSkip::MaterializationNotReady(format!(
            "range query has {step_count} steps; warm execution limit is {MAX_RANGE_STEPS}"
        )));
    }
    let window_ms = entry
        .materialization_bindings()
        .into_iter()
        .map(|binding| binding.window_ms)
        .max()
        .unwrap_or(0);
    let mut rows = BTreeMap::<BTreeMap<String, String>, Vec<(i64, f64)>>::new();
    let mut total_coverage = None;
    let mut evaluation_ms = start_ms;
    loop {
        let (outcome, t0_ms) = execute_query_plan_instant(index, entry, evaluation_ms)?;
        if !coverage_covers_closed_windows(outcome.coverage, t0_ms, evaluation_ms, window_ms) {
            return Err(LoweringSkip::MaterializationNotReady(format!(
                "coverage {:?} is incomplete at range evaluation {evaluation_ms}",
                outcome.coverage
            )));
        }
        if outcome.series.is_empty() {
            return Err(LoweringSkip::MaterializationNotReady(format!(
                "planned summary produced no series at range evaluation {evaluation_ms}"
            )));
        }
        fold_coverage(&mut total_coverage, outcome.coverage);
        for (labels, samples) in outcome.series {
            let Some((_, value)) = samples.last() else {
                return Err(LoweringSkip::MaterializationNotReady(format!(
                    "planned summary produced no value at range evaluation {evaluation_ms}"
                )));
            };
            rows.entry(labels)
                .or_default()
                .push((evaluation_ms as i64, *value));
        }
        let Some(next) = evaluation_ms.checked_add(step_ms) else {
            break;
        };
        if next > end_ms {
            break;
        }
        evaluation_ms = next;
    }
    Ok(ASAPTierResult {
        series: rows.into_iter().collect(),
        coverage: total_coverage,
    })
}

fn coverage_covers_closed_windows(
    coverage: Option<(u64, u64)>,
    t0_ms: u64,
    t1_ms: u64,
    window_ms: u64,
) -> bool {
    let Some((start, end)) = coverage else {
        return false;
    };
    window_ms > 0
        && start <= end
        && start.saturating_sub(window_ms) <= t0_ms
        && end >= t1_ms
        && end.saturating_sub(start).saturating_add(window_ms) >= t1_ms.saturating_sub(t0_ms)
}

pub fn serve_instant_from_query_plan(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    now_ms: u64,
) -> Result<(ASAPTierResult, u64), LoweringSkip> {
    if !summary_executor_live_enabled() {
        return Err(LoweringSkip::Disabled);
    }
    let (outcome, t0_ms) = execute_query_plan_instant(index, entry, now_ms)?;
    Ok((
        ASAPTierResult {
            series: outcome.series,
            coverage: outcome.coverage,
        },
        t0_ms,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::storage_engines::sketch_db::data::AggKind;
    use crate::storage_engines::sketch_db::index::{Capability, SummarySeriesMetadata};

    #[test]
    fn flag_off_spellings_disable_live_serve() {
        assert!(!summary_executor_live_value(Some("0")));
        assert!(!summary_executor_live_value(Some(" false ")));
        assert!(!summary_executor_live_value(Some("OFF")));
    }

    #[test]
    fn unset_flag_defaults_to_serving() {
        assert!(summary_executor_live_value(None));
    }

    #[test]
    fn formal_range_plan_returns_exact_requested_steps() {
        let idx = SketchStore::new();
        let policy = asap_types::PolicyFingerprint(901);
        idx.register(SummarySeriesMetadata {
            storage_handle: 9,
            metric_name: "bytes".into(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Sum)),
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: policy,
        });
        for (start, end, value) in [(0, 1_000, 1.0), (1_000, 2_000, 2.0), (2_000, 3_000, 3.0)] {
            idx.append_precompute(
                9,
                BTreeMap::new(),
                (start, end),
                Box::new(asap_physical_operators::summary_kernels::SumAccumulator::with_sum(value)),
            );
        }
        let entry = asap_types::query_plan::QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "q-sum".into(),
            canonical_query: "sum_over_time(bytes[1s])".into(),
            fixed_evaluation: None,
            root: asap_types::query_plan::QueryNodeId(0),
            nodes: BTreeMap::from([
                (
                    asap_types::query_plan::QueryNodeId(0),
                    asap_types::query_plan::QueryPlanNode::ExactReadout {
                        input: asap_types::query_plan::QueryNodeId(1),
                        readout: asap_types::query_plan::ExactReadout::Sum,
                    },
                ),
                (
                    asap_types::query_plan::QueryNodeId(1),
                    asap_types::query_plan::QueryPlanNode::ReadMaterialization {
                        binding: asap_types::query_plan::MaterializationBinding {
                            full_window_slide_ms: None,
                            item_labels: Vec::new(),
                            materialization: policy.into(),
                            stored_output_reference:
                                asap_types::sds::StoredOutputReference::for_definition(
                                    policy.into(),
                                ),
                            output_grouping: asap_types::query_plan::PhysicalGrouping::PerEntity,
                            window_ms: 1_000,
                            pane_origin_ms: Some(0),
                            readout_lookback_ms: Some(1_000),
                        },
                    },
                ),
            ]),
            instant: asap_types::query_plan::InstantExecution {
                lookback_ms: 1_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: asap_types::query_plan::FallbackPolicy::ExactBackend,
        };

        let result = serve_range_steps_from_query_plan(&idx, &entry, 1_000, 3_000, 1_000)
            .expect("step-precise range execution");
        assert_eq!(result.series.len(), 1);
        assert_eq!(
            result.series[0].1,
            vec![(1_000, 1.0), (2_000, 2.0), (3_000, 3.0)]
        );
    }
}
