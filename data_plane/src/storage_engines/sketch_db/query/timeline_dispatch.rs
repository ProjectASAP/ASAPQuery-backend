//! Cross-schema result combination for the §7 schema-timeline query
//! dispatch ([`design-sketch-db.md`](../../../../docs/design-sketch-db.md)).
//!
//! When a metric-range query spans a reconfigure boundary, the
//! `SchemaRegistry::timeline_for_metric` call returns multiple
//! [`TimelineSegment`]s each owned by a distinct `agg_id`. The query
//! engine evaluates the statistic **per segment** against its owning
//! aggregation's precompute, then hands the per-segment scalars to
//! [`combine_statistic`] here to stitch them into a single answer.
//!
//! ## What this module is — and is not
//!
//! This is a pure combiner over already-evaluated scalar results. It
//! does NOT:
//! * touch the store or accumulators,
//! * know how per-segment evaluation works (the engine does that),
//! * decide fallback policy for `Purged` segments (the caller does
//!   that before calling here).
//!
//! The query engine wires this primitive into
//! `ASAPQueryEngine::try_handle_query_promql_via_timeline`, which
//! runs the per-segment evaluation loop and feeds the scalars back
//! through `combine_statistic`.
//!
//! ## Statistic combinability
//!
//! See §7.3 of the design doc. Summarised:
//!
//! | Statistic | Combinable across sketch types? |
//! |---|---|
//! | `Count`, `Sum`      | Yes — sum. |
//! | `Min`, `Max`        | Yes — pointwise min / max. |
//! | `Cardinality` (HLL) | Yes in principle (HLL OR merge on the sketch itself), but at the **scalar** level — which is what this module receives — distinct counts from different HLL parameterisations cannot be OR-merged. Treated as **non-combinable** here. |
//! | `Increase`, `Rate`  | Non-combinable at the scalar level — they depend on endpoint samples; stitching needs the raw counters, not their deltas. |
//! | `Quantile`, `Topk`  | Non-combinable at the scalar level — stitching requires merging the underlying KLL / CMS sketches, which may differ in parameters across schemas. |
//!
//! Non-combinable statistics return [`CombinedResult::Partial`]
//! carrying whatever combinable prefix we could compute plus the
//! list of segments that couldn't contribute. The caller decides
//! how to render that (warning, fall-through to exact DB, or
//! error).

use promql_utilities::query_logics::enums::Statistic;

use crate::storage_engines::sketch_db::TimelineSegment;

/// Result of combining per-segment scalars for a single statistic.
///
/// `Full(value)` means every segment contributed and the combination
/// is semantically equivalent to a single-schema evaluation over the
/// entire range. `Partial` surfaces the failure mode explicitly so
/// the user knows they are looking at a schema-change artifact.
#[derive(Debug, Clone, PartialEq)]
pub enum CombinedResult {
    /// All segments combined cleanly. Value is the stitched result.
    Full(f64),
    /// The statistic isn't combinable across schema boundaries, or
    /// the caller flagged one or more segments as uncomputable
    /// (e.g. a `TimelineCoverage::Purged` segment whose data no
    /// longer lives in the sketch store).
    ///
    /// `covered` is the combined result over the segments we could
    /// evaluate; `missing` lists the segments we couldn't. Note that
    /// `covered` may be meaningless for the user (e.g. a Quantile
    /// over only part of the range is not "the p99 of the range"),
    /// so it's up to the caller to decide whether to display it.
    Partial {
        covered: Option<f64>,
        missing: Vec<TimelineSegment>,
    },
}

/// Input to [`combine_statistic`]: one per-segment scalar plus the
/// segment it came from (for provenance in the `Partial` case).
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentValue {
    pub segment: TimelineSegment,
    pub value: f64,
}

/// Combine per-segment scalars for a single statistic across a
/// multi-schema timeline.
///
/// Contract:
///
/// * `segments` is the output of the engine's per-segment evaluator,
///   one `SegmentValue` per [`TimelineSegment`] returned by
///   `timeline_for_metric`. An empty input means no schema covers the
///   query range; returns `Full(0.0)` for `Count` / `Sum` (zero is
///   the identity), and `Partial { covered: None, missing: [] }` for
///   everything else (no sensible default).
/// * `unresolved` is the list of segments the engine could not
///   evaluate (e.g. `Purged` coverage, or a capability miss). They
///   are folded directly into the `Partial` output's `missing` list.
pub fn combine_statistic(
    statistic: Statistic,
    segments: &[SegmentValue],
    unresolved: &[TimelineSegment],
) -> CombinedResult {
    // Fast path: nothing to combine and nothing missing.
    if segments.is_empty() && unresolved.is_empty() {
        return match statistic {
            Statistic::Count | Statistic::Sum => CombinedResult::Full(0.0),
            _ => CombinedResult::Partial {
                covered: None,
                missing: Vec::new(),
            },
        };
    }

    // If the caller handed us any unresolved segments, the result is
    // Partial regardless of the statistic. We still compute the
    // best-effort `covered` so callers that want to show it can.
    let has_unresolved = !unresolved.is_empty();

    let covered = match statistic {
        Statistic::Count | Statistic::Sum => {
            // Additive: zero identity, so even with zero segments
            // the value is 0.0.
            Some(segments.iter().map(|s| s.value).sum::<f64>())
        }
        Statistic::Min => segments
            .iter()
            .map(|s| s.value)
            .fold(None, |acc, v| Some(acc.map_or(v, |a: f64| a.min(v)))),
        Statistic::Max => segments
            .iter()
            .map(|s| s.value)
            .fold(None, |acc, v| Some(acc.map_or(v, |a: f64| a.max(v)))),
        // Non-combinable at the scalar level — see module doc.
        Statistic::Cardinality
        | Statistic::Increase
        | Statistic::Rate
        | Statistic::Quantile
        | Statistic::Topk => None,
    };

    let combinable = matches!(
        statistic,
        Statistic::Count | Statistic::Sum | Statistic::Min | Statistic::Max
    );

    if combinable && !has_unresolved {
        // `covered` is always `Some` for combinable statistics with
        // at least one input (Count/Sum guarantee it even at zero).
        match covered {
            Some(v) => CombinedResult::Full(v),
            None => CombinedResult::Partial {
                covered: None,
                missing: Vec::new(),
            },
        }
    } else {
        CombinedResult::Partial {
            covered,
            missing: unresolved.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_engines::sketch_db::{AggStatus, TimelineCoverage};

    fn seg(agg_id: u64, start: u64, end: u64, status: AggStatus) -> TimelineSegment {
        TimelineSegment {
            agg_id,
            start_ms: start,
            end_ms: end,
            status,
            coverage: match status {
                AggStatus::Expired => TimelineCoverage::Purged,
                _ => TimelineCoverage::Sketch,
            },
        }
    }

    fn sv(agg_id: u64, start: u64, end: u64, status: AggStatus, v: f64) -> SegmentValue {
        SegmentValue {
            segment: seg(agg_id, start, end, status),
            value: v,
        }
    }

    #[test]
    fn sum_is_additive_across_segments() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 10.0),
            sv(2, 100, 200, AggStatus::Active, 15.0),
        ];
        assert_eq!(
            combine_statistic(Statistic::Sum, &vs, &[]),
            CombinedResult::Full(25.0),
        );
    }

    #[test]
    fn count_is_additive_across_segments() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 7.0),
            sv(2, 100, 200, AggStatus::Active, 3.0),
        ];
        assert_eq!(
            combine_statistic(Statistic::Count, &vs, &[]),
            CombinedResult::Full(10.0),
        );
    }

    #[test]
    fn min_takes_pointwise_min() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 5.0),
            sv(2, 100, 200, AggStatus::Active, 2.5),
        ];
        assert_eq!(
            combine_statistic(Statistic::Min, &vs, &[]),
            CombinedResult::Full(2.5),
        );
    }

    #[test]
    fn max_takes_pointwise_max() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 5.0),
            sv(2, 100, 200, AggStatus::Active, 9.25),
        ];
        assert_eq!(
            combine_statistic(Statistic::Max, &vs, &[]),
            CombinedResult::Full(9.25),
        );
    }

    #[test]
    fn quantile_across_schemas_is_partial() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 0.95),
            sv(2, 100, 200, AggStatus::Active, 0.97),
        ];
        match combine_statistic(Statistic::Quantile, &vs, &[]) {
            CombinedResult::Partial { covered, missing } => {
                assert!(covered.is_none());
                assert!(missing.is_empty());
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    #[test]
    fn topk_across_schemas_is_partial() {
        let vs = vec![sv(1, 0, 100, AggStatus::Retired, 42.0)];
        assert!(matches!(
            combine_statistic(Statistic::Topk, &vs, &[]),
            CombinedResult::Partial { .. }
        ));
    }

    #[test]
    fn cardinality_across_schemas_is_partial() {
        let vs = vec![
            sv(1, 0, 100, AggStatus::Retired, 1_000.0),
            sv(2, 100, 200, AggStatus::Active, 1_500.0),
        ];
        assert!(matches!(
            combine_statistic(Statistic::Cardinality, &vs, &[]),
            CombinedResult::Partial { .. }
        ));
    }

    #[test]
    fn additive_with_unresolved_segment_is_partial_with_covered_set() {
        let vs = vec![sv(1, 0, 100, AggStatus::Retired, 10.0)];
        let missing = vec![seg(2, 100, 200, AggStatus::Expired)];
        match combine_statistic(Statistic::Sum, &vs, &missing) {
            CombinedResult::Partial {
                covered,
                missing: m,
            } => {
                assert_eq!(covered, Some(10.0));
                assert_eq!(m.len(), 1);
                assert_eq!(m[0].agg_id, 2);
            }
            other => panic!("expected Partial with covered=Some(10.0), got {other:?}"),
        }
    }

    #[test]
    fn empty_input_yields_additive_zero_for_count_and_sum() {
        assert_eq!(
            combine_statistic(Statistic::Count, &[], &[]),
            CombinedResult::Full(0.0),
        );
        assert_eq!(
            combine_statistic(Statistic::Sum, &[], &[]),
            CombinedResult::Full(0.0),
        );
    }

    #[test]
    fn empty_input_yields_partial_none_for_non_additive() {
        // Min / Max / Quantile / Topk / Cardinality / Increase / Rate
        // all lack an identity element, so an empty range produces
        // Partial { covered: None } rather than a misleading 0.0.
        for stat in [
            Statistic::Min,
            Statistic::Max,
            Statistic::Quantile,
            Statistic::Topk,
            Statistic::Cardinality,
            Statistic::Increase,
            Statistic::Rate,
        ] {
            match combine_statistic(stat, &[], &[]) {
                CombinedResult::Partial { covered, missing } => {
                    assert!(
                        covered.is_none() && missing.is_empty(),
                        "{stat:?} should yield empty Partial",
                    );
                }
                other => panic!("{stat:?} should be Partial on empty input, got {other:?}"),
            }
        }
    }

    #[test]
    fn single_segment_additive_returns_that_value() {
        let vs = vec![sv(1, 0, 100, AggStatus::Active, 42.0)];
        assert_eq!(
            combine_statistic(Statistic::Sum, &vs, &[]),
            CombinedResult::Full(42.0),
        );
    }

    #[test]
    fn unresolved_only_no_segments_returns_partial_with_no_covered() {
        let missing = vec![seg(1, 0, 100, AggStatus::Expired)];
        match combine_statistic(Statistic::Sum, &[], &missing) {
            CombinedResult::Partial {
                covered,
                missing: m,
            } => {
                // Sum over zero segments is 0.0 (identity), so covered is
                // Some(0.0); the missing list still surfaces the gap.
                assert_eq!(covered, Some(0.0));
                assert_eq!(m.len(), 1);
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }
}
