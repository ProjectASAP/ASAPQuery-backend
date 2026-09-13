//! The `SummaryExecutor` serving path — the sole way `data_plane` answers
//! a query from the sketch tier. `shadow_compare.rs` (diagnostic-only
//! comparison against the legacy reducer, used to validate this path
//! before it went live) and `sketch_reducer.rs` (the legacy reducer
//! itself) are both retired: neither was "ground truth" any more than
//! this path is, and once this path was the default-on live serving
//! path (design-target-architecture.md Part A), keeping a second,
//! independently-planned answering mechanism around only meant two
//! things could silently disagree with each other, not that either was
//! more trustworthy. See
//! `data_plane/docs/l4node-plan-executor-design.md` and the Phase 2
//! plan's "What 'safe to serve' means, precisely" section for the exact
//! gate this applies.
//!
//! `try_serve_from_summary_executor` returning `None` (flag off, a
//! self-excluded shape like `rate()`/`irate()`/`topk(K, sum
//! by(...)(rate(...)))`/keyed-CMS point-estimate, or no matching
//! registered sid) means the query fails over to archive — there is no
//! other sketch-tier path left to try.

use std::collections::BTreeMap;

use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::post_asap_planner::LoweringSkip;
use crate::query_engines::asap_query_engine::post_asap_readout::{
    execute_post_asap_instant, execute_post_asap_readout, execute_query_plan_instant,
    execute_query_plan_readout, fold_coverage,
};
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::sketch_db::query::ASAPTierResult;

/// Fallback accuracy target used only when `post_asap_planner.rs`'s
/// observed-family lookup finds nothing registered for the query's
/// metric (in which case no family/params choice here can matter — the
/// query can't be served either way). `data_plane` doesn't carry a
/// per-workload `AccuracyTarget` today (see the design doc's "Rollout"
/// section).
const DEFAULT_LIVE_EPSILON: f64 = 0.01;

fn live_accuracy() -> AccuracyTarget {
    live_accuracy_from_values(
        std::env::var("ASAP_SUMMARY_EXECUTOR_EPSILON")
            .ok()
            .as_deref(),
        std::env::var("ASAP_SUMMARY_EXECUTOR_DELTA").ok().as_deref(),
    )
}

fn live_accuracy_from_values(epsilon: Option<&str>, delta: Option<&str>) -> AccuracyTarget {
    let epsilon = epsilon
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0 && *value < 1.0)
        .unwrap_or(DEFAULT_LIVE_EPSILON);
    match delta
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0 && *value < 1.0)
    {
        Some(delta) => AccuracyTarget::EpsilonDelta { epsilon, delta },
        None => AccuracyTarget::Epsilon(epsilon),
    }
}

/// Whether the serving cutover is enabled for this process. The env var
/// stays as a kill switch (`ASAP_SUMMARY_EXECUTOR_LIVE=0`/`false`/`off`) —
/// with it set, every query fails over to archive rather than being
/// served from the sketch tier at all (there's no legacy reducer left to
/// fall through to).
///
/// Default flipped to **on** (control_plane/docs/design-target-architecture.md
/// §4/Part A): both unit tests and a real HTTP-level e2e test
/// (`live_serve_hll_global_count_merges_across_sids`,
/// `live_serve_actually_answers_ddsketch_quantile`) already prove
/// correctness for the shapes `try_serve_from_summary_executor` covers,
/// and the grouping-ambiguity problem that gated this default off
/// (empty-`by` ambiguity) is resolved via the real `Reduction::{Reduce,
/// PerEntity}` IR signal, not a heuristic.
pub fn summary_executor_live_enabled() -> bool {
    let value = std::env::var("ASAP_SUMMARY_EXECUTOR_LIVE").ok();
    summary_executor_live_value(value.as_deref())
}

fn summary_executor_live_value(value: Option<&str>) -> bool {
    value.map(str::trim).is_none_or(|v| {
        !(v == "0" || v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off"))
    })
}

/// Try to serve `query` entirely from `SummaryExecutor`. Returns `None`
/// whenever the query can't be served this way (flag off, a
/// self-excluded shape, no matching registered sid, or lowering/execution
/// failed) — the caller fails over to archive. `Some(...)` means this
/// answered the query.
///
/// This used to carry a third fallback reason: a grouping-ambiguity gate
/// that declined any empty-`by` shape producing >1 group
/// (ASAPController#163). That gate is gone — `Reduction`
/// (ASAPController#165) lets `summary_executor.rs` resolve both halves of
/// the ambiguity correctly on its own, so there is no longer a shape to
/// decline. See `PostAsapReadoutOutcome`'s doc for the full reasoning.
pub fn try_serve_from_summary_executor(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Option<ASAPTierResult> {
    if !summary_executor_live_enabled() {
        return None;
    }

    serve_from_summary_executor(index, query, t0_ms, t1_ms, is_cumulative, live_accuracy())
        .map_err(|skip| {
            tracing::debug!(
                query,
                ?skip,
                "live: query not servable from SummaryExecutor, falling back"
            );
        })
        .ok()
}

pub fn serve_from_summary_executor(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    accuracy: AccuracyTarget,
) -> Result<ASAPTierResult, LoweringSkip> {
    if !summary_executor_live_enabled() {
        return Err(LoweringSkip::Disabled);
    }
    let outcome = execute_post_asap_readout(index, query, t0_ms, t1_ms, is_cumulative, accuracy)?;

    tracing::debug!(query, "live: served from SummaryExecutor");
    Ok(ASAPTierResult {
        series: outcome.series,
        coverage: outcome.coverage,
    })
}

pub fn serve_instant_from_summary_executor(
    index: &SketchStore,
    query: &str,
    now_ms: u64,
) -> Result<(ASAPTierResult, u64), LoweringSkip> {
    if !summary_executor_live_enabled() {
        return Err(LoweringSkip::Disabled);
    }
    let (outcome, t0_ms) = execute_post_asap_instant(index, query, now_ms, live_accuracy())?;
    Ok((
        ASAPTierResult {
            series: outcome.series,
            coverage: outcome.coverage,
        },
        t0_ms,
    ))
}

pub fn serve_from_query_plan(
    index: &SketchStore,
    entry: &asap_types::query_plan::QueryPlanEntry,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
) -> Result<ASAPTierResult, LoweringSkip> {
    if !summary_executor_live_enabled() {
        return Err(LoweringSkip::Disabled);
    }
    let outcome = execute_query_plan_readout(index, entry, t0_ms, t1_ms, is_cumulative)?;
    Ok(ASAPTierResult {
        series: outcome.series,
        coverage: outcome.coverage,
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

    use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchAlgorithm, SketchInstanceMetadata, SketchSampleState,
    };

    fn ddsketch_fixture() -> SketchStore {
        let idx = SketchStore::new();
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        idx.register(SketchInstanceMetadata {
            sid: 1,
            metric_name: "latency_ms".to_string(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::DDSketch,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        use asap_sketchlib::{DdSketch, MessagePackCodec};
        let mut sk = DdSketch::new(0.01);
        for i in 1..=100 {
            sk.update(i as f64);
        }
        idx.append_sample(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode DDSketch"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
        idx
    }

    fn register_hll(idx: &SketchStore, sid: u64, service: &str, items: &[&str]) {
        let cfg = SketchConfig::Hll { precision: 14 };
        let mut group_by_keys = std::collections::BTreeSet::new();
        group_by_keys.insert("service".to_string());
        idx.register(SketchInstanceMetadata {
            sid,
            metric_name: "unique_users".to_string(),
            group_by_keys,
            capability: Some(Capability::CardinalityApprox),
            agg_kind: AggKind::Sketch {
                algorithm: SketchAlgorithm::Hll,
                config: cfg.clone(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: Some(AccuracyBound::from_config(&cfg)),
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: asap_types::PolicyFingerprint::UNSET,
        });
        use asap_sketchlib::{HllSketch, HllVariant, MessagePackCodec};
        let mut sk = HllSketch::new(HllVariant::Regular, 14);
        for item in items {
            sk.update(item.as_bytes());
        }
        let mut labels = BTreeMap::new();
        labels.insert("service".to_string(), service.to_string());
        idx.append_sample(
            sid,
            labels,
            (1_000, 2_000),
            SketchSampleState {
                bytes: sk.to_msgpack().expect("encode HLL"),
                encoding: crate::storage_engines::sketch_db::index::SketchEncoding::MsgpackFull,
            },
        );
    }

    #[test]
    fn flag_off_spellings_disable_live_serve() {
        assert!(!summary_executor_live_value(Some("0")));
        assert!(!summary_executor_live_value(Some(" false ")));
        assert!(!summary_executor_live_value(Some("OFF")));
    }

    #[test]
    fn unset_flag_defaults_to_serving() {
        // Default flipped to on (design-target-architecture.md §4/Part A)
        // -- an unset env var must serve, not fall back to the legacy
        // path, for a shape this executor already proves safe.
        assert!(summary_executor_live_value(None));
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        assert!(result.is_some(), "unset flag must default to serving");
    }

    #[test]
    fn live_accuracy_accepts_explicit_valid_epsilon_delta() {
        assert_eq!(
            live_accuracy_from_values(Some("0.02"), Some("0.04")),
            AccuracyTarget::EpsilonDelta {
                epsilon: 0.02,
                delta: 0.04,
            }
        );
        assert_eq!(
            live_accuracy_from_values(Some("invalid"), Some("1.0")),
            AccuracyTarget::Epsilon(DEFAULT_LIVE_EPSILON)
        );
    }

    #[test]
    fn flag_on_safe_shape_serves() {
        let idx = ddsketch_fixture();
        let result = try_serve_from_summary_executor(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
        );
        let result = result.expect("unambiguous single-series quantile must serve");
        assert_eq!(result.series.len(), 1);
        assert!(!result.is_empty());
    }

    #[test]
    fn formal_range_plan_returns_exact_requested_steps() {
        let idx = SketchStore::new();
        let policy = asap_types::PolicyFingerprint(901);
        idx.register(SketchInstanceMetadata {
            sid: 9,
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
                Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(value)),
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
                            item_labels: Vec::new(),
                            materialization: policy.into(),
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

    #[test]
    fn flag_on_global_merge_shape_is_served_merged_not_declined() {
        // Previously `flag_on_ambiguous_shape_falls_back`, asserting
        // `result.is_none()`: the grouping-ambiguity gate declined this
        // shape because an empty `by` couldn't be told apart from "reduce
        // everything" (ASAPController#163). With `Reduction` (#165) the
        // executor resolves it -- the outer aggregator lowers to
        // `Reduce([])`, both sids share one group key, and the new path
        // serves the correctly merged answer instead of falling back.
        //
        // The distinct-count idiom is `count(distinct_over_time(v[w]))`;
        // bare `count(v)` is a row count upstream.
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let result = try_serve_from_summary_executor(
            &idx,
            "count(distinct_over_time(unique_users[1m]))",
            1_000,
            2_000,
            true,
        );
        let result = result
            .expect("global-merge shape is no longer ambiguous -- it must be served, not declined");
        assert_eq!(
            result.series.len(),
            1,
            "a by-less distinct count must merge both sids into ONE series, got {:?}",
            result.series
        );
        // Disjoint item sets {a,b,c} + {d,e,f} -> merged cardinality ~6.
        let card = result.series[0].1[0].1;
        assert!(
            (4.0..=8.0).contains(&card),
            "merged cardinality {card} should be ~6 (both sids), not ~3 (one sid)"
        );
    }

    #[test]
    fn flag_on_unservable_query_falls_back() {
        let idx = SketchStore::new();
        let result =
            try_serve_from_summary_executor(&idx, "rate(http_requests_total[5m])", 0, 1000, true);
        assert!(result.is_none());
    }
}
