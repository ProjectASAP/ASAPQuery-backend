//! Layer 2→3 lowering: convert relational Aggregate nodes to sketch AggIntent.
//!
//! This pass walks a [`QueryExpr`] tree and converts `Aggregate { AggFunc }`
//! nodes to `SketchAgg { AggIntent }` where the aggregation can benefit from
//! sketch-based execution.  It is shared by both the PromQL and SQL parsers.
//!
//! # What gets lowered
//!
//! A single-agg `Aggregate` node whose `AggFunc` maps to a sketch intent is
//! replaced by `SketchAgg { AggIntent }`.  If the `Aggregate` had GROUP BY
//! keys, they become a wrapping `Partition` node.
//!
//! Multi-agg `Aggregate` nodes or those with HAVING clauses pass through
//! unchanged — the physical planner handles them.

use crate::algebra::expr::*;

/// Lower a Layer 2 `QueryExpr` (relational operators only) to Layer 3
/// (sketch algebra with `AggIntent`).
///
/// This pass walks the tree and converts `Aggregate { AggFunc }` nodes
/// to `SketchAgg { AggIntent }` where the aggregation can benefit from
/// sketch-based execution.
pub fn lower_to_sketch_algebra(expr: QueryExpr) -> QueryExpr {
    match expr {
        QueryExpr::Aggregate { keys, aggs, having, input } => {
            let input = lower_to_sketch_algebra(*input);
            lower_aggregate(keys, aggs, having, Box::new(input))
        }

        // ── Single-input nodes: recurse ──────────────────────────────────
        QueryExpr::Filter { pred, input } => QueryExpr::Filter {
            pred,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Project { cols, input } => QueryExpr::Project {
            cols,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Window { duration, slide, input } => {
            let lowered_input = lower_to_sketch_algebra(*input);
            // Fuse Window + SketchAgg → WindowedAgg (the window defines sketch lifecycle).
            if let QueryExpr::SketchAgg { op, col, input: sketch_input } = lowered_input {
                let window = WindowSpec {
                    kind: match slide {
                        Some(s) => WindowKind::Sliding { size: duration, slide: s },
                        None    => WindowKind::Tumbling { size: duration },
                    },
                    time_col: None,
                };
                QueryExpr::WindowedAgg { agg: op, window, col, input: sketch_input }
            } else {
                QueryExpr::Window { duration, slide, input: Box::new(lowered_input) }
            }
        }
        QueryExpr::SketchAgg { op, col, input } => QueryExpr::SketchAgg {
            op,
            col,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::WindowedAgg { agg, window, col, input } => QueryExpr::WindowedAgg {
            agg,
            window,
            col,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Partition { keys, input } => QueryExpr::Partition {
            keys,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Dedup { col, input } => QueryExpr::Dedup {
            col,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::TopK { k, by, input } => QueryExpr::TopK {
            k,
            by,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Sort { keys, input } => QueryExpr::Sort {
            keys,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::Limit { n, offset, input } => QueryExpr::Limit {
            n,
            offset,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::WindowFunc { func, partition_by, order_by, frame, input } => QueryExpr::WindowFunc {
            func,
            partition_by,
            order_by,
            frame,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::HistogramQuantile { phi, input } => QueryExpr::HistogramQuantile {
            phi,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        QueryExpr::PromQLSubquery { range, resolution, input } => QueryExpr::PromQLSubquery {
            range,
            resolution,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },

        // ── Two-input nodes: recurse into both ───���──────────────────────
        QueryExpr::BinaryOp { op, lhs, rhs, vector_match } => QueryExpr::BinaryOp {
            op,
            lhs: Box::new(lower_to_sketch_algebra(*lhs)),
            rhs: Box::new(lower_to_sketch_algebra(*rhs)),
            vector_match,
        },
        QueryExpr::Join { kind, pred, left, right } => QueryExpr::Join {
            kind,
            pred,
            left:  Box::new(lower_to_sketch_algebra(*left)),
            right: Box::new(lower_to_sketch_algebra(*right)),
        },
        QueryExpr::JoinSketch { join_key, outer, inner } => QueryExpr::JoinSketch {
            join_key,
            outer: Box::new(lower_to_sketch_algebra(*outer)),
            inner: Box::new(lower_to_sketch_algebra(*inner)),
        },
        QueryExpr::SetOp { kind, all, left, right } => QueryExpr::SetOp {
            kind,
            all,
            left:  Box::new(lower_to_sketch_algebra(*left)),
            right: Box::new(lower_to_sketch_algebra(*right)),
        },

        // ── Multi-input / container nodes ─────��──────────────────────────
        QueryExpr::Merge { inputs } => QueryExpr::Merge {
            inputs: inputs.into_iter().map(lower_to_sketch_algebra).collect(),
        },
        QueryExpr::Subquery { alias, expr } => QueryExpr::Subquery {
            alias,
            expr: Box::new(lower_to_sketch_algebra(*expr)),
        },
        QueryExpr::LetBinding { name, expr, body } => QueryExpr::LetBinding {
            name,
            expr: Box::new(lower_to_sketch_algebra(*expr)),
            body: Box::new(lower_to_sketch_algebra(*body)),
        },

        // ── Leaf nodes: pass through ────────���────────────────────────────
        QueryExpr::Source(_) | QueryExpr::Ref(_) => expr,
    }
}

/// Try to lower a single `Aggregate` node to `SketchAgg`.
fn lower_aggregate(
    keys:   Vec<String>,
    aggs:   Vec<AggItem>,
    having: Option<ScalarExpr>,
    input:  Box<QueryExpr>,
) -> QueryExpr {
    // Only lower single-agg Aggregates without HAVING.
    if aggs.len() == 1 && having.is_none() {
        let agg = &aggs[0];

        // COUNT(*) without GROUP BY is a simple row count — no sketch benefit.
        if matches!(agg.func, AggFunc::Count) && keys.is_empty() {
            return QueryExpr::Aggregate { keys, aggs, having, input };
        }

        if let Some(intent) = agg_func_to_intent(&agg.func) {
            // If the input is a Window, fuse into WindowedAgg (the window
            // defines the sketch lifecycle — flush/reset/merge semantics).
            let sketch = if let QueryExpr::Window { duration, slide, input: win_input } = *input {
                let window = WindowSpec {
                    kind: match slide {
                        Some(s) => WindowKind::Sliding { size: duration, slide: s },
                        None    => WindowKind::Tumbling { size: duration },
                    },
                    time_col: None,
                };
                QueryExpr::WindowedAgg {
                    agg:    intent,
                    window,
                    col:    agg.col.clone(),
                    input:  win_input,
                }
            } else {
                QueryExpr::SketchAgg {
                    op:    intent,
                    col:   agg.col.clone(),
                    input,
                }
            };
            if keys.is_empty() {
                return sketch;
            } else {
                return QueryExpr::Partition {
                    keys: PartitionKeys::By(keys),
                    input: Box::new(sketch),
                };
            }
        }
    }

    // Multi-agg or non-sketchable: keep as relational Aggregate.
    QueryExpr::Aggregate { keys, aggs, having, input }
}

/// Map an [`AggFunc`] to an [`AggIntent`] for sketch execution.
fn agg_func_to_intent(func: &AggFunc) -> Option<AggIntent> {
    match func {
        AggFunc::Quantile(phi) => Some(AggIntent::default_quantile(vec![*phi])),
        AggFunc::CountDistinct => Some(AggIntent::default_cardinality()),
        AggFunc::HeavyHitters { .. } => Some(AggIntent::default_frequency()),
        AggFunc::Count => Some(AggIntent::default_frequency()),
        AggFunc::Avg => Some(AggIntent::Quantile {
            quantiles: vec![0.5],
            accuracy:  0.01,
        }),
        AggFunc::Min => Some(AggIntent::Extrema { min: true, max: false }),
        AggFunc::Max => Some(AggIntent::Extrema { min: false, max: true }),
        AggFunc::StdDev { .. } => Some(AggIntent::Quantile {
            quantiles: vec![0.25, 0.75],
            accuracy:  0.01,
        }),
        AggFunc::Variance { .. } => Some(AggIntent::Quantile {
            quantiles: vec![0.25, 0.75],
            accuracy:  0.01,
        }),
        AggFunc::Sum | AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => {
            Some(AggIntent::Exact(ExactAgg::Sum))
        }
        AggFunc::Custom(_) => None,
    }
}

// ── Tests ───────────���───────────────────────────��─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn make_agg(func: AggFunc, input: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            keys:   vec![],
            aggs:   vec![AggItem {
                alias:    "v".into(),
                func,
                col:      ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input:  Box::new(input),
        }
    }

    fn make_agg_with_keys(func: AggFunc, keys: Vec<String>, input: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            keys,
            aggs:   vec![AggItem {
                alias:    "v".into(),
                func,
                col:      ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input:  Box::new(input),
        }
    }

    // ── Single-agg lowering ──────────────────────────────────────────────────

    #[test]
    fn quantile_lowered_to_sketch_agg() {
        let expr = make_agg(AggFunc::Quantile(0.99), src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Quantile { .. }, .. }));
    }

    #[test]
    fn count_distinct_lowered_to_cardinality() {
        let expr = make_agg(AggFunc::CountDistinct, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Cardinality { .. }, .. }));
    }

    #[test]
    fn count_without_group_by_stays_aggregate() {
        // COUNT(*) without GROUP BY is a simple row count — no sketch benefit.
        let expr = make_agg(AggFunc::Count, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Aggregate { .. }));
    }

    #[test]
    fn count_with_group_by_lowered_to_frequency() {
        let expr = make_agg_with_keys(AggFunc::Count, vec!["region".into()], src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::Partition { input, .. } => {
                assert!(matches!(input.as_ref(), QueryExpr::SketchAgg { op: AggIntent::Frequency { .. }, .. }));
            }
            other => panic!("expected Partition(SketchAgg), got {other:?}"),
        }
    }

    #[test]
    fn sum_lowered_to_exact() {
        let expr = make_agg(AggFunc::Sum, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Exact(ExactAgg::Sum), .. }));
    }

    #[test]
    fn avg_lowered_to_quantile_p50() {
        let expr = make_agg(AggFunc::Avg, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::SketchAgg { op: AggIntent::Quantile { quantiles, .. }, .. } => {
                assert_eq!(quantiles, &[0.5]);
            }
            other => panic!("expected SketchAgg(Quantile), got {other:?}"),
        }
    }

    #[test]
    fn min_lowered_to_extrema() {
        let expr = make_agg(AggFunc::Min, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Extrema { min: true, max: false }, .. }));
    }

    #[test]
    fn max_lowered_to_extrema() {
        let expr = make_agg(AggFunc::Max, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Extrema { min: false, max: true }, .. }));
    }

    #[test]
    fn stddev_lowered_to_iqr_quantile() {
        let expr = make_agg(AggFunc::StdDev { population: false }, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::SketchAgg { op: AggIntent::Quantile { quantiles, .. }, .. } => {
                assert!(quantiles.contains(&0.25) && quantiles.contains(&0.75));
            }
            other => panic!("expected SketchAgg(Quantile), got {other:?}"),
        }
    }

    #[test]
    fn custom_func_not_lowered() {
        let expr = make_agg(AggFunc::Custom("my_udf".into()), src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Aggregate { .. }));
    }

    // ── GROUP BY keys become Partition ────��───────────────────────────────────

    #[test]
    fn group_by_wraps_with_partition() {
        let expr = make_agg_with_keys(
            AggFunc::Quantile(0.5),
            vec!["host".into()],
            src("m"),
        );
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::Partition { keys, input } => {
                assert_eq!(keys.keys(), &["host".to_string()]);
                assert!(matches!(input.as_ref(), QueryExpr::SketchAgg { .. }));
            }
            other => panic!("expected Partition, got {other:?}"),
        }
    }

    // ── Multi-agg not lowered ────────────────────────────────────────────────

    #[test]
    fn multi_agg_not_lowered() {
        let expr = QueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![
                AggItem {
                    alias: "c".into(),
                    func:  AggFunc::Count,
                    col:   ColumnRef::Wildcard,
                    distinct: false,
                },
                AggItem {
                    alias: "s".into(),
                    func:  AggFunc::Sum,
                    col:   ColumnRef::Named("x".into()),
                    distinct: false,
                },
            ],
            having: None,
            input:  Box::new(src("m")),
        };
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Aggregate { .. }));
    }

    // ── Recursive lowering ───────────────────��────────────────────���──────────

    #[test]
    fn window_wrapping_aggregate_fuses_to_windowed_agg() {
        // Aggregate inside a Window → fused WindowedAgg.
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide:    None,
            input:    Box::new(make_agg(AggFunc::Quantile(0.99), src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::WindowedAgg { agg, window, .. } => {
                assert!(matches!(agg, AggIntent::Quantile { .. }));
                assert!(matches!(window.kind, WindowKind::Tumbling { .. }));
            }
            other => panic!("expected WindowedAgg, got {other:?}"),
        }
    }

    #[test]
    fn sliding_window_fuses_to_windowed_agg_sliding() {
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide:    Some(Duration::from_secs(60)),
            input:    Box::new(make_agg(AggFunc::Quantile(0.5), src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::WindowedAgg { window, .. } => {
                assert!(matches!(window.kind, WindowKind::Sliding { .. }));
            }
            other => panic!("expected WindowedAgg(Sliding), got {other:?}"),
        }
    }

    #[test]
    fn window_wrapping_non_sketchable_stays_separate() {
        // Custom func is not sketchable → Window stays, Aggregate stays.
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide:    None,
            input:    Box::new(make_agg(AggFunc::Custom("my_udf".into()), src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Window { .. }));
    }

    #[test]
    fn lowering_recurses_into_binary_op() {
        let expr = QueryExpr::BinaryOp {
            op:  BinaryOpKind::Add,
            lhs: Box::new(make_agg(AggFunc::Sum, src("a"))),
            rhs: Box::new(make_agg(AggFunc::CountDistinct, src("b"))),
            vector_match: None,
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::BinaryOp { lhs, rhs, .. } => {
                assert!(matches!(lhs.as_ref(), QueryExpr::SketchAgg { op: AggIntent::Exact(_), .. }));
                assert!(matches!(rhs.as_ref(), QueryExpr::SketchAgg { op: AggIntent::Cardinality { .. }, .. }));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn lowering_recurses_into_topk() {
        let expr = QueryExpr::TopK {
            k:  10,
            by: vec!["symbol".into()],
            input: Box::new(make_agg_with_keys(
                AggFunc::Count,
                vec!["symbol".into()],
                src("m"),
            )),
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::TopK { input, .. } => {
                // Count with GROUP BY is lowered to Partition(SketchAgg(Frequency))
                assert!(matches!(input.as_ref(), QueryExpr::Partition { .. }));
            }
            other => panic!("expected TopK, got {other:?}"),
        }
    }

    #[test]
    fn histogram_quantile_passthrough() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(make_agg(AggFunc::Quantile(0.95), src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::HistogramQuantile { phi, input } => {
                assert!((phi - 0.95).abs() < 1e-9);
                assert!(matches!(input.as_ref(), QueryExpr::SketchAgg { .. }));
            }
            other => panic!("expected HistogramQuantile, got {other:?}"),
        }
    }

    #[test]
    fn rate_lowered_to_exact_sum() {
        let expr = make_agg(AggFunc::Rate, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Exact(ExactAgg::Sum), .. }));
    }

    #[test]
    fn delta_lowered_to_exact_sum() {
        let expr = make_agg(AggFunc::Delta, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Exact(ExactAgg::Sum), .. }));
    }
}
