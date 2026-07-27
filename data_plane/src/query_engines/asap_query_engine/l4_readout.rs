//! Shared `L4Node` lowering + execution + conversion into
//! `ASAPTierResult`'s `(series, coverage)` shape — the common core of both
//! `shadow_compare.rs` (diagnostic only, never affects serving) and
//! `live_serve.rs` (the actual cutover). See
//! `data_plane/docs/l4node-plan-executor-design.md` for the design.

use std::collections::BTreeMap;

use asap_sketch::exec::{execute, ExecOutcome};
use asap_sketch::{L4Node, SummaryExpr};
use control_plane::types_v2::AccuracyTarget;

use crate::query_engines::asap_query_engine::l4_lowering::{lower_promql_to_l4node, LoweringSkip};
use crate::query_engines::asap_query_engine::summary_executor::{
    QueryExecutionContext, SummaryValue,
};
use crate::storage_engines::sketch_db::index::SketchStore;

/// Mirrors `ASAPTierResult.series`'s row shape — `(label_values, samples)`
/// where `samples` is `(window_end_unix_ms, value)`.
pub type SeriesRows = Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>;

/// The result of lowering + executing a query through
/// `SummaryExecutor`, converted into the same shape `ASAPTierResult`
/// uses, regardless of whether the answer came from the sketch
/// (`ExecOutcome::Value`) or `ExactAgg` (`ExecOutcome::State`) side —
/// callers that only care about "did this answer the query, and is it
/// safe to trust" don't need to know which.
pub struct L4ReadoutOutcome {
    pub series: SeriesRows,
    pub coverage: Option<(u64, u64)>,
    /// `true` only for a sketch-family (`ExecOutcome::Value`) outcome
    /// whose tree's root `SummaryAgg` had an empty `by` AND produced more
    /// than one group. This is exactly the ambiguous shape
    /// [ASAPController#163](https://github.com/ProjectASAP/ASAPController/issues/163)
    /// describes — an empty `by` is indistinguishable between "no
    /// grouping concept applies" (this group split is correct) and "an
    /// aggregation operator asked to reduce everything" (these groups
    /// should have been merged into one). Always `false` for `ExactAgg`
    /// (`Sum`/`Increase` map only from genuine aggregation operators, so
    /// their empty `by` is unambiguous — see the design doc's "Grouping
    /// semantics"/"ExactAgg" sections) and for any sketch outcome with a
    /// non-empty `by` or ≤1 resulting group (nothing to disagree about).
    pub ambiguous_merge_risk: bool,
}

/// Lower `query`, execute it against `index` over `[t0_ms, t1_ms]`, and
/// convert the result into `L4ReadoutOutcome`. `Err` covers every reason
/// this couldn't produce a trustworthy answer — see `LoweringSkip`'s
/// variants; every one of them means "fall back to the legacy path,"
/// never "the legacy path is wrong."
pub fn execute_l4_readout(
    index: &SketchStore,
    query: &str,
    t0_ms: u64,
    t1_ms: u64,
    is_cumulative: bool,
    accuracy: AccuracyTarget,
) -> Result<L4ReadoutOutcome, LoweringSkip> {
    let node = lower_promql_to_l4node(query, accuracy)?;
    let by_is_empty = root_summary_agg_by_is_empty(&node);

    let ctx = QueryExecutionContext {
        index,
        t0_ms,
        t1_ms,
        is_cumulative,
    };

    match execute(&node, &ctx) {
        Ok(ExecOutcome::Value(values)) => {
            let mut coverage: Option<(u64, u64)> = None;
            let mut series = Vec::new();
            for (group_key, value) in &values {
                fold_coverage(&mut coverage, value.coverage());
                series.extend(summary_value_to_series(group_key, value));
            }
            let ambiguous_merge_risk = by_is_empty.unwrap_or(false) && values.len() > 1;
            Ok(L4ReadoutOutcome {
                series,
                coverage,
                ambiguous_merge_risk,
            })
        }
        Ok(ExecOutcome::State(groups)) => {
            let mut coverage: Option<(u64, u64)> = None;
            let mut series = Vec::new();
            for (group_key, state, _kind, _params) in &groups {
                fold_coverage(&mut coverage, state.exact_coverage());
                let Some(value) = state.exact_value(&None) else {
                    continue;
                };
                series.push((group_key.clone(), vec![(t1_ms as i64, value)]));
            }
            Ok(L4ReadoutOutcome {
                series,
                coverage,
                ambiguous_merge_risk: false,
            })
        }
        Err(e) => Err(LoweringSkip::ExecuteFailed(format!("{e:?}"))),
    }
}

/// Walk down to the tree's `SummaryAgg` node (through a `SummaryEstimate`
/// wrapper if present, and through the first child of a `SummaryMerge` —
/// its children agree on `(SummaryKind, SummaryParams)` by construction,
/// so their `by` agrees too) and report whether its `by` list is empty.
/// `None` for a bare `Logical` root (shouldn't happen here —
/// `lower_promql_to_l4node` already rejects that via `NotRealized` — kept
/// exhaustive and defensive rather than assumed unreachable).
fn root_summary_agg_by_is_empty(node: &L4Node) -> Option<bool> {
    match &node.expr {
        SummaryExpr::SummaryAgg { by, .. } => Some(by.is_empty()),
        SummaryExpr::SummaryEstimate { sketch_input, .. } => {
            root_summary_agg_by_is_empty(sketch_input)
        }
        SummaryExpr::SummaryMerge { children } => children
            .first()
            .and_then(|c| root_summary_agg_by_is_empty(c)),
        SummaryExpr::Logical(_) => None,
        _ => None,
    }
}

/// `SummaryValue::Points`/`TopK` -> `ASAPTierResult.series`'s row shape.
/// `TopK`'s ranked-list-per-timestamp shape is pivoted into one row per
/// item (each row = the group's label map plus an `item` label, one point
/// per timestamp that item appeared in the ranked list) -- the SAME
/// convention `sketch_reducer.rs`'s own topk arm already uses, not a new
/// one invented here.
fn summary_value_to_series(
    group_key: &BTreeMap<String, String>,
    value: &SummaryValue,
) -> SeriesRows {
    match value {
        SummaryValue::Points(points, _coverage) => {
            vec![(group_key.clone(), points.clone())]
        }
        SummaryValue::TopK(ranked_per_ts, _coverage) => {
            let mut by_item: BTreeMap<String, Vec<(i64, f64)>> = BTreeMap::new();
            for (ts, items) in ranked_per_ts {
                for (item, val) in items {
                    by_item.entry(item.clone()).or_default().push((*ts, *val));
                }
            }
            by_item
                .into_iter()
                .map(|(item, points)| {
                    let mut lv = group_key.clone();
                    lv.insert("item".to_string(), item);
                    (lv, points)
                })
                .collect()
        }
    }
}

pub(crate) fn fold_coverage(coverage: &mut Option<(u64, u64)>, next: Option<(u64, u64)>) {
    let Some((lo, hi)) = next else { return };
    *coverage = Some(match *coverage {
        Some((clo, chi)) => (clo.min(lo), chi.max(hi)),
        None => (lo, hi),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::data::{AggKind, SketchConfig};
    use crate::storage_engines::sketch_db::index::{
        AccuracyBound, Capability, SketchInstanceMetadata, SketchKindHandle, SketchSampleState,
    };

    fn accuracy() -> AccuracyTarget {
        AccuracyTarget::Epsilon(0.01)
    }

    fn register_hll(idx: &SketchStore, sid: u64, service: &str, items: &[&str]) {
        // precision 14 -- what `ControlPlaneCostModel` actually picks for
        // `AccuracyTarget::Epsilon(0.01)` (confirmed by inspecting the
        // bound tree directly); `find_candidates`'s exact-match contract
        // means a mismatched precision here would just silently produce
        // `NoCandidates`, not a wrong answer -- but that's not what these
        // tests are checking.
        let cfg = SketchConfig::Hll { precision: 14 };
        let mut group_by_keys = std::collections::BTreeSet::new();
        group_by_keys.insert("service".to_string());
        idx.register(SketchInstanceMetadata {
            sid,
            metric_name: "unique_users".to_string(),
            group_by_keys,
            capability: Some(Capability::CardinalityApprox),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::Hll,
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

    fn ddsketch_fixture() -> SketchStore {
        let idx = SketchStore::new();
        // alpha 0.01 -- what `ControlPlaneCostModel` actually picks for
        // `quantile_over_time` at `AccuracyTarget::Epsilon(0.01)` (DDSketch,
        // not KLL -- confirmed by inspecting the bound tree directly).
        let cfg = SketchConfig::DDSketch {
            relative_accuracy: 0.01,
        };
        idx.register(SketchInstanceMetadata {
            sid: 1,
            metric_name: "latency_ms".to_string(),
            group_by_keys: std::collections::BTreeSet::new(),
            capability: Some(Capability::QuantileApprox(SketchKindHandle::DDSketch)),
            agg_kind: AggKind::Sketch {
                kind: SketchKindHandle::DDSketch,
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

    #[test]
    fn unambiguous_sketch_query_is_not_flagged() {
        let idx = ddsketch_fixture();
        let outcome = execute_l4_readout(
            &idx,
            "quantile_over_time(0.99, latency_ms[1m])",
            1_000,
            2_000,
            true,
            accuracy(),
        )
        .expect("should execute");
        assert!(
            !outcome.ambiguous_merge_risk,
            "a single-series bare range function must not be flagged ambiguous"
        );
        assert_eq!(outcome.series.len(), 1);
    }

    #[test]
    fn ambiguous_global_merge_shape_is_flagged() {
        // The exact ASAPController#163 shape: two HLL sids, no explicit
        // by(), an aggregation-operator query -- find_candidates can't
        // tell whether these two groups should have been merged.
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let outcome =
            execute_l4_readout(&idx, "count(unique_users)", 1_000, 2_000, true, accuracy())
                .expect("should execute");
        assert!(
            outcome.ambiguous_merge_risk,
            "two distinct-service HLL groups under a by-less count() must be flagged, got {:?}",
            outcome.series
        );
        assert_eq!(outcome.series.len(), 2);
    }

    #[test]
    fn exact_agg_outcome_is_never_flagged_ambiguous() {
        let idx = SketchStore::new();
        idx.register(
            crate::storage_engines::sketch_db::index::SketchInstanceMetadata {
                sid: 1,
                metric_name: "bytes_total".to_string(),
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
                policy_fp: asap_types::PolicyFingerprint::UNSET,
            },
        );
        idx.append_precompute(
            1,
            BTreeMap::new(),
            (1_000, 2_000),
            Box::new(crate::precompute_engine::operators::SumAccumulator::with_sum(42.0)),
        );
        let outcome = execute_l4_readout(&idx, "sum(bytes_total)", 1_000, 2_000, true, accuracy())
            .expect("should execute");
        assert!(!outcome.ambiguous_merge_risk);
        // Window-end-only coverage: a single window (1_000, 2_000) is
        // keyed by its end (2_000) alone, so both bounds equal 2_000 --
        // same semantics as `SummaryValue::coverage()`, reconfirmed for
        // `exact_coverage` by this module's A0 test in `summary_executor.rs`.
        assert_eq!(outcome.coverage, Some((2_000, 2_000)));
    }
}
