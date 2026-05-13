//! `BindArchiveOnly` — route Phase β archive-only `AggIntent`s to the
//! cold tier.
//!
//! This rule is the L4 catch for [`AggIntent`]s that don't have a warm-
//! tier streaming sketch family today (`Absent`, `Present`, `Delta`,
//! `Deriv`, `PredictLinear`, `HoltWinters`, `Idelta`, `Irate`, `Resets`,
//! `Changes`). It matches a single-intent
//! `Aggregate` carrying any of those, and emits an
//! [`SketchExpr::Logical`] pass-through. The L5 emitter looks at the
//! enclosed [`AggIntent::archive_only`] flag and routes the corresponding
//! StreamingConfig entry to the archive (Gorilla / Thanos) tier rather
//! than the warm sketch tier.
//!
//! Why a rule rather than the recursive walker's default?
//! `lower::bind_recursive` already wraps unmatched `Aggregate` in
//! `SketchExpr::Logical`, but that branch fires on EVERY unmatched
//! aggregate — including intents the planner is still trying to bind
//! (an `Aggregate{Sum}` over a tabular leaf, etc.). Surfacing the
//! archive-only cases through an explicit named rule lets the
//! StreamingConfig emitter and Phase α routing emit distinguish "no
//! warm-tier rule fired but the intent IS warm-eligible" from "this
//! intent is intentionally archive-only, route it cold".
//!
//! Reference: `control_plane/docs/design.md` §6 line ~689 ("the optimizer
//! framework") and Phase β orchestrator scope.

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::sketch_algebra::rules::Rule;
use crate::sketch_algebra::sketch_expr::SketchExpr;
use crate::types_v2::AccuracyTarget;

/// Route `Aggregate{<archive-only intent>}` to a `Logical` pass-through.
/// Phase α's emitter consults `AggIntent::archive_only()` to flag the
/// resulting StreamingConfig entry for the cold tier.
pub struct BindArchiveOnly;

impl Rule for BindArchiveOnly {
    fn name(&self) -> &'static str {
        "bind_archive_only"
    }

    fn priority(&self) -> u16 {
        // Lowest priority — every warm-tier rule should out-rank this
        // one so the only path to BindArchiveOnly is "no warm rule
        // fired AND the intent is archive-only".
        1
    }

    fn apply(&self, expr: &QueryExpr, _accuracy: &AccuracyTarget) -> Option<SketchExpr> {
        match expr {
            QueryExpr::Aggregate { aggs, .. } => {
                // Single-intent Aggregate is the canonical Phase β shape;
                // multi-intent fans out to per-intent rules elsewhere.
                if aggs.len() == 1 && aggs[0].archive_only() {
                    Some(SketchExpr::Logical(expr.clone()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{LabelFilter, Schema, Source, WindowKind};
    use std::time::Duration;

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds_bucket".into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::with_time_index(
                vec![
                    Column {
                        name: "ts".into(),
                        dtype: DataType::Timestamp,
                        nullable: false,
                    },
                    Column {
                        name: "service".into(),
                        dtype: DataType::Utf8,
                        nullable: false,
                    },
                    Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    },
                ],
                0,
                vec![vec![0, 1]],
            ),
        }
    }

    fn windowed_scan() -> QueryExpr {
        QueryExpr::Window {
            kind: WindowKind::Sliding,
            size: Duration::from_secs(300),
            slide: None,
            child: Box::new(ts_scan()),
        }
    }

    fn agg_with(intent: AggIntent) -> QueryExpr {
        QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![intent],
            having: None,
            child: Box::new(windowed_scan()),
        }
    }

    #[test]
    fn binds_absent_archive_only() {
        // `histogram_quantile(...)` is no longer an L3 intent — it's a
        // PromQL operator that the controller's PromQL parser substitutes
        // (Step γ5) into a plain `Aggregate { Quantile(φ) }`. The
        // canonical archive-only anchor for this test is `Absent`
        // (which has no warm-tier sketch family).
        let expr = agg_with(AggIntent::Absent);
        let out = BindArchiveOnly
            .apply(&expr, &AccuracyTarget::Epsilon(0.01))
            .expect("rule should match Absent");
        match out {
            SketchExpr::Logical(inner) => assert_eq!(inner, expr),
            other => panic!("expected Logical pass-through, got {other:?}"),
        }
    }

    #[test]
    fn binds_each_archive_only_intent() {
        let intents = vec![
            AggIntent::Absent,
            AggIntent::Present,
            AggIntent::Delta {
                window: Duration::from_secs(60),
            },
            AggIntent::Deriv {
                window: Duration::from_secs(60),
            },
            AggIntent::PredictLinear {
                window: Duration::from_secs(300),
                ahead: Duration::from_secs(60),
            },
            AggIntent::HoltWinters {
                window: Duration::from_secs(300),
                smoothing_factor: 0.3,
                trend_factor: 0.3,
            },
            AggIntent::Idelta {
                window: Duration::from_secs(60),
            },
            AggIntent::Irate {
                window: Duration::from_secs(60),
            },
            AggIntent::Resets {
                window: Duration::from_secs(300),
            },
            AggIntent::Changes {
                window: Duration::from_secs(300),
            },
        ];
        for intent in intents {
            let expr = agg_with(intent.clone());
            let out = BindArchiveOnly.apply(&expr, &AccuracyTarget::Epsilon(0.01));
            assert!(
                out.is_some(),
                "BindArchiveOnly should bind {intent:?} (archive-only Phase β intent)"
            );
        }
    }

    #[test]
    fn does_not_bind_warm_tier_intents() {
        // Sum / Quantile / Cardinality / TopK are NOT archive-only — they
        // must NOT trigger BindArchiveOnly (the warm-tier rules own them).
        for intent in [
            AggIntent::Sum,
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Cardinality {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(60),
            },
        ] {
            let expr = agg_with(intent.clone());
            assert!(
                BindArchiveOnly
                    .apply(&expr, &AccuracyTarget::Epsilon(0.01))
                    .is_none(),
                "BindArchiveOnly must not bind warm-tier intent {intent:?}"
            );
        }
    }

    #[test]
    fn does_not_bind_non_aggregate_shapes() {
        let scan = ts_scan();
        assert!(BindArchiveOnly
            .apply(&scan, &AccuracyTarget::Epsilon(0.01))
            .is_none());
        let window = windowed_scan();
        assert!(BindArchiveOnly
            .apply(&window, &AccuracyTarget::Epsilon(0.01))
            .is_none());
    }
}
