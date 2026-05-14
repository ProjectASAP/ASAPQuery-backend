//! `BindExactAgg` — emits `PhysicalExpr::ExactAgg` for the four exact-aggregation
//! intents that the PR 6 follow-up flipped to warm-tier routing.
//!
//! ## Coverage
//!
//! | L3 `AggIntent`                       | L4 `PhysicalExpr::ExactAgg` |
//! |---|---|
//! | `Sum`                                | `agg_type: AggregationType::Sum`       |
//! | `Rate { .. }`                        | `agg_type: AggregationType::Increase`  |
//! | `Increase { .. }`                    | `agg_type: AggregationType::Increase`  |
//! | `Count { accuracy: Exact }`          | `agg_type: AggregationType::Sum`       |
//!
//! `Rate` and `Increase` share the `Increase` accumulator because rate is
//! computed as `increase / window_seconds` — a scalar division on the
//! accumulator's output, not a separate accumulator family. The wrapping
//! division (when needed) is the L5 emitter's responsibility, not the
//! L4 binder's.
//!
//! `Count{accuracy:Exact}` maps to `Sum` because `count_over_time` is
//! exactly the sum of presence indicators (each sample contributes 1).
//! There's no dedicated `Count` `AggregationType` variant; `Sum`
//! covers the shape.
//!
//! ## What this rule does NOT bind
//!
//! - `AggIntent::Avg` — needs cross-policy join (Sum / Count), no single
//!   `AggregationType` covers it. Stays on archive until the L4 binder
//!   gains a join rule.
//! - `AggIntent::Quantile { Exact }` / `Cardinality { Exact }` /
//!   `TopK { Exact }` / `Frequency { Exact }` — the exact-accuracy
//!   variants of approximate-by-default intents. No exact-precompute
//!   shape exists in `AggregationType` for these; they need
//!   `HashAgg` / `SortAgg` / `SortMerge` from the deployment's
//!   exact-physical-operator family (none of which run at the warm tier
//!   today).
//! - `AggIntent::Min` / `AggIntent::Max` — `MinMax` is already covered
//!   by the quantile-sketch path (`quantile(0)` / `quantile(1)` via
//!   DDSketch / KLL). Adding a separate `MinMax` ExactAgg binding
//!   would create a competing rule; not worth the cost-model churn
//!   until profile data shows MinMax-precompute is meaningfully
//!   cheaper than the quantile-sketch path for some workloads.
//!
//! ## Priority
//!
//! `2` — above `bind_archive_only` (which has the lowest priority for a
//! catch-all on Sum / Rate / Increase etc.) but below the sketch
//! family rules (priorities 4–6). When this rule and `bind_archive_only`
//! both match an archive-routable intent (Sum / Rate / Increase), the
//! ExactAgg path wins — same direction as the analyzer flip in
//! `capability_for`.

#![allow(dead_code)]

use std::time::Duration;

use promql_utilities::query_logics::enums::AggregationType;

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::physical_expr::PhysicalExpr;
use crate::sketch_algebra::rules::Rule;
use crate::types_v2::AccuracyTarget;

/// Bind exact-aggregation intents (Sum / Rate / Increase / Count{Exact})
/// to `PhysicalExpr::ExactAgg`. See module doc for the mapping table.
pub struct BindExactAgg;

impl Rule for BindExactAgg {
    fn name(&self) -> &'static str {
        "bind_exact_agg"
    }

    fn priority(&self) -> u16 {
        // Above bind_archive_only (priority 1) but below sketch families
        // (4-6). Sum / Rate / Increase are archive-only intents in the
        // sketch-family world; this rule overrides that routing.
        2
    }

    fn apply(&self, expr: &QueryExpr, _accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
        let (intent, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => (&aggs[0], child),
            _ => return None,
        };

        let agg_type = match intent {
            AggIntent::Sum => AggregationType::Sum,
            AggIntent::Rate { window } | AggIntent::Increase { window } => {
                // The window is informational here — the data plane keys
                // the policy on (metric, attrs, agg_kind, filter) plus
                // the per-policy `window_size`. Empty windows are
                // semantically meaningless; reject them so the rule
                // doesn't fire on a malformed L3 input.
                if *window == Duration::ZERO {
                    return None;
                }
                AggregationType::Increase
            }
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            } => AggregationType::Sum,
            _ => return None,
        };

        Some(PhysicalExpr::exact_agg_over_logical(
            agg_type,
            (**child).clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::{LabelFilter, Schema, Source};

    fn scan(metric: &str) -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: metric.into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::default(),
        }
    }

    fn agg_over(intent: AggIntent, metric: &str) -> QueryExpr {
        QueryExpr::Aggregate {
            aggs: vec![intent],
            child: Box::new(scan(metric)),
            by: vec![],
            having: None,
        }
    }

    fn check_binds(intent: AggIntent, expected: AggregationType) {
        let expr = agg_over(intent, "test_metric");
        let bound = BindExactAgg
            .apply(&expr, &AccuracyTarget::Exact)
            .unwrap_or_else(|| panic!("rule didn't fire on {expected:?}"));
        match bound {
            PhysicalExpr::ExactAgg { agg_type, .. } => assert_eq!(agg_type, expected),
            other => panic!("expected ExactAgg, got {other:?}"),
        }
    }

    #[test]
    fn binds_sum_to_exact_agg_sum() {
        check_binds(AggIntent::Sum, AggregationType::Sum);
    }

    #[test]
    fn binds_rate_to_exact_agg_increase() {
        check_binds(
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggregationType::Increase,
        );
    }

    #[test]
    fn binds_increase_to_exact_agg_increase() {
        check_binds(
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
            AggregationType::Increase,
        );
    }

    #[test]
    fn binds_count_exact_to_exact_agg_sum() {
        check_binds(
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggregationType::Sum,
        );
    }

    #[test]
    fn does_not_bind_count_approximate() {
        // Approximate count is the cardinality-sketch path's domain.
        let expr = agg_over(
            AggIntent::Count {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            "test_metric",
        );
        assert!(BindExactAgg.apply(&expr, &AccuracyTarget::Exact).is_none());
    }

    #[test]
    fn does_not_bind_avg() {
        let expr = agg_over(AggIntent::Avg, "test_metric");
        assert!(BindExactAgg.apply(&expr, &AccuracyTarget::Exact).is_none());
    }

    #[test]
    fn does_not_bind_quantile() {
        let expr = agg_over(
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            "test_metric",
        );
        assert!(BindExactAgg.apply(&expr, &AccuracyTarget::Exact).is_none());
    }

    #[test]
    fn does_not_bind_when_by_clause_present() {
        // Group-by-bearing intents are L3-canonical-form-only here;
        // this rule mirrors the existing bind_ddsketch_quantile shape
        // and rejects `by`-bearing inputs. A keyed ExactAgg follow-up
        // would emit `MultipleSum` / `MultipleIncrease` instead — see
        // the AggregationType enum.
        let expr = QueryExpr::Aggregate {
            aggs: vec![AggIntent::Sum],
            child: Box::new(scan("test_metric")),
            by: vec![0],
            having: None,
        };
        assert!(BindExactAgg.apply(&expr, &AccuracyTarget::Exact).is_none());
    }

    #[test]
    fn rejects_zero_window_rate() {
        // Defensive — a zero-window Rate is semantically meaningless.
        let expr = agg_over(
            AggIntent::Rate {
                window: Duration::ZERO,
            },
            "test_metric",
        );
        assert!(BindExactAgg.apply(&expr, &AccuracyTarget::Exact).is_none());
    }

    #[test]
    fn priority_above_archive_only() {
        // Sanity check: this rule wins against bind_archive_only when
        // both would fire on a Sum / Rate / Increase intent.
        use crate::sketch_algebra::rules::bind_archive_only::BindArchiveOnly;
        assert!(BindExactAgg.priority() > BindArchiveOnly.priority());
    }
}
