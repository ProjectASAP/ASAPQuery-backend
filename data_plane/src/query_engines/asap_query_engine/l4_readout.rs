//! `L4Node` lowering + execution + conversion into `ASAPTierResult`'s
//! `(series, coverage)` shape — the core `live_serve.rs` (the serving
//! cutover) calls into. See
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
/// NOTE — this used to carry an `ambiguous_merge_risk` flag, and
/// `live_serve.rs` used it to DECLINE to serve an ambiguous shape.
/// That gate is now removed, because the ambiguity it guarded against no
/// longer exists.
///
/// It existed because an empty `by: Vec<ColumnId>` was indistinguishable
/// between "no grouping concept applies" (the group split is correct) and
/// "an aggregation operator asked to reduce everything" (the groups
/// should have been merged) — exactly
/// [ASAPController#163](https://github.com/ProjectASAP/ASAPController/issues/163).
/// Unable to tell which, the safe move was to fall back to the legacy
/// path whenever an empty `by` produced >1 group.
///
/// ASAPController#165 removed that ambiguity at the source by making the
/// reduction kind explicit (`Reduction::{PerEntity, Reduce(GroupKeys)}`),
/// and `summary_executor.rs::resolve_group_key` now acts on it directly.
/// Both branches are resolved correctly BEFORE reaching here:
///
/// * `PerEntity` — the multi-group split is definitionally right (one row
///   per entity, never merged), so it was never a "risk" to begin with.
/// * `Reduce([])` — every candidate shares one group key, so the outcome
///   has exactly ONE group and the old `values.len() > 1` trigger cannot
///   fire at all.
///
/// The flag would therefore be unconditionally `false` today; keeping it
/// would mean keeping a heuristic that can only ever misfire (declining
/// correct `PerEntity` answers) now that the real signal is available.
pub struct L4ReadoutOutcome {
    pub series: SeriesRows,
    pub coverage: Option<(u64, u64)>,
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
    let node = lower_promql_to_l4node(index, query, accuracy)?;

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
            Ok(L4ReadoutOutcome { series, coverage })
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
            Ok(L4ReadoutOutcome { series, coverage })
        }
        Err(e) => Err(LoweringSkip::ExecuteFailed(format!("{e:?}"))),
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
    fn bare_range_function_keeps_one_series_per_entity() {
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
        assert_eq!(outcome.series.len(), 1);
    }

    #[test]
    fn global_merge_shape_now_merges_instead_of_being_declined() {
        // The exact ASAPController#163 shape: two HLL sids, no explicit
        // by(), an aggregation-operator query. This test previously
        // asserted `ambiguous_merge_risk == true` and TWO unmerged series
        // -- i.e. it pinned the old workaround, where an empty `by` left
        // `find_candidates` unable to tell "reduce everything" apart from
        // "no grouping concept," so `live_serve` declined to serve the
        // shape at all.
        //
        // With `Reduction` (ASAPController#165) that ambiguity is gone:
        // `count(...)` is a genuine aggregation operator, so it lowers to
        // `Reduce([])` and `resolve_group_key` gives every candidate the
        // SAME group key -- the two sids MERGE into one answer, which is
        // what the query actually asked for. No gate, no fallback.
        let idx = SketchStore::new();
        register_hll(&idx, 1, "svc-a", &["a", "b", "c"]);
        register_hll(&idx, 2, "svc-b", &["d", "e", "f"]);
        let outcome =
            execute_l4_readout(&idx, "count(unique_users)", 1_000, 2_000, true, accuracy())
                .expect("should execute");
        assert_eq!(
            outcome.series.len(),
            1,
            "a by-less count() is a full reduction -- both HLL sids must merge into ONE \
             series, not stay split (and not be declined), got {:?}",
            outcome.series
        );
        // Disjoint item sets {a,b,c} + {d,e,f} -> merged cardinality ~6.
        let (_group, points) = &outcome.series[0];
        let card = points[0].1;
        assert!(
            (4.0..=8.0).contains(&card),
            "merged cardinality {card} should be ~6 (both sids' disjoint items), not ~3"
        );
    }

    #[test]
    fn exact_agg_outcome_reports_window_end_coverage() {
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
        // Window-end-only coverage: a single window (1_000, 2_000) is
        // keyed by its end (2_000) alone, so both bounds equal 2_000 --
        // same semantics as `SummaryValue::coverage()`, reconfirmed for
        // `exact_coverage` by this module's A0 test in `summary_executor.rs`.
        assert_eq!(outcome.coverage, Some((2_000, 2_000)));
    }
}
