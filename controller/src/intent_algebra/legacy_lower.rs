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

use crate::intent_algebra::legacy_expr::*;
use crate::intent_algebra::{infer_schema_for_root, Schema};

/// Lower a Layer 2 `QueryExpr` (relational operators only) to Layer 3
/// (sketch algebra with `AggIntent`).
///
/// This pass walks the tree and converts `Aggregate { AggFunc }` nodes
/// to `SketchAgg { AggIntent }` where the aggregation can benefit from
/// sketch-based execution.
///
/// Step β: derives the root-level [`Schema`] from the outermost `Source`
/// leaf (via [`infer_schema_for_root`]) and threads it through the
/// recursive descent. The lowering pass is structurally schema-agnostic
/// today (it preserves `ColumnRef::Named(_)` verbatim); Step γ will
/// migrate the per-variant column-list rewrites onto positional
/// `ColumnId` once the canonical aggregator surface lands consumer-side.
pub fn lower_to_sketch_algebra(expr: QueryExpr) -> QueryExpr {
    let schema = infer_schema_for_root(&expr);
    lower_to_sketch_algebra_with_schema(expr, &schema)
}

/// Variant of [`lower_to_sketch_algebra`] that takes an explicit
/// inherited [`Schema`]. The public entry point derives it from the
/// outermost source; callers that already hold a Schema (the optimiser
/// pipeline, for instance) can pass it directly.
pub fn lower_to_sketch_algebra_with_schema(
    expr: QueryExpr,
    parent_schema: &Schema,
) -> QueryExpr {
    match expr {
        QueryExpr::Aggregate { keys, aggs, having, input } => {
            let input = lower_to_sketch_algebra_with_schema(*input, parent_schema);
            lower_aggregate(keys, aggs, having, Box::new(input))
        }

        // ── Single-input nodes: recurse ──────────────────────────────────
        QueryExpr::Filter { pred, input } => QueryExpr::Filter {
            pred,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Project { cols, input } => QueryExpr::Project {
            cols,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Window { duration, slide, input } => {
            let lowered_input = lower_to_sketch_algebra_with_schema(*input, parent_schema);
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
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::WindowedAgg { agg, window, col, input } => QueryExpr::WindowedAgg {
            agg,
            window,
            col,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Partition { keys, input } => QueryExpr::Partition {
            keys,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Distinct { cols, input } => QueryExpr::Distinct {
            cols,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::TopK { k, by, input } => QueryExpr::TopK {
            k,
            by,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Sort { keys, input } => QueryExpr::Sort {
            keys,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::Limit { n, offset, input } => QueryExpr::Limit {
            n,
            offset,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },
        QueryExpr::PromQLSubquery { range, resolution, input } => QueryExpr::PromQLSubquery {
            range,
            resolution,
            input: Box::new(lower_to_sketch_algebra_with_schema(*input, parent_schema)),
        },

        // ── Two-input nodes: recurse into both ──────────────────────────
        QueryExpr::BinaryOp { op, lhs, rhs, vector_match } => QueryExpr::BinaryOp {
            op,
            lhs: Box::new(lower_to_sketch_algebra_with_schema(*lhs, parent_schema)),
            rhs: Box::new(lower_to_sketch_algebra_with_schema(*rhs, parent_schema)),
            vector_match,
        },
        QueryExpr::Join { kind, pred, left, right } => QueryExpr::Join {
            kind,
            pred,
            left:  Box::new(lower_to_sketch_algebra_with_schema(*left, parent_schema)),
            right: Box::new(lower_to_sketch_algebra_with_schema(*right, parent_schema)),
        },
        QueryExpr::SetOp { kind, all, left, right } => QueryExpr::SetOp {
            kind,
            all,
            left:  Box::new(lower_to_sketch_algebra_with_schema(*left, parent_schema)),
            right: Box::new(lower_to_sketch_algebra_with_schema(*right, parent_schema)),
        },

        // ── Multi-input / container nodes ─────��──────────────────────────
        QueryExpr::Merge { inputs } => QueryExpr::Merge {
            inputs: inputs
                .into_iter()
                .map(|i| lower_to_sketch_algebra_with_schema(i, parent_schema))
                .collect(),
        },
        QueryExpr::LetBinding { name, expr, body } => QueryExpr::LetBinding {
            name,
            expr: Box::new(lower_to_sketch_algebra_with_schema(*expr, parent_schema)),
            body: Box::new(lower_to_sketch_algebra_with_schema(*body, parent_schema)),
        },

        // ── Leaf nodes: pass through ────────���────────────────────────────
        QueryExpr::Source(_) | QueryExpr::Ref(_) => expr,
    }
}

/// Try to lower a single `Aggregate` node to `SketchAgg`.
///
/// # Fan-out
///
/// Step α replaced the legacy `AggIntent` with the canonical single-φ form
/// and dropped the `Extrema { min, max }` enum (split into `Min` / `Max`).
/// The two multi-intent legacy `AggFunc`s — `StdDev` and `Variance`, which
/// historically lowered to a `Quantile { quantiles: vec![0.25, 0.75] }` —
/// now fan out into two sibling `SketchAgg` (or `WindowedAgg`) nodes
/// wrapped in a `QueryExpr::Merge`. The F1 strategy lets every consumer
/// keep matching single-intent `SketchAgg::op` patterns unchanged.
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

        let intents = agg_func_to_intents(&agg.func);
        if !intents.is_empty() {
            // Build one SketchAgg / WindowedAgg per fanned-out intent. The
            // input subtree is cloned for each sibling (Merge children own
            // their own input) so the structure mirrors what a sketch
            // physical planner would emit for a multi-intent aggregate.
            let col = agg.col.clone();
            let sketch_nodes: Vec<QueryExpr> = match *input {
                QueryExpr::Window { duration, slide, input: ref win_input } => {
                    let window = WindowSpec {
                        kind: match slide {
                            Some(s) => WindowKind::Sliding { size: duration, slide: s },
                            None    => WindowKind::Tumbling { size: duration },
                        },
                        time_col: None,
                    };
                    intents.into_iter().map(|intent| QueryExpr::WindowedAgg {
                        agg:    intent,
                        window: window.clone(),
                        col:    col.clone(),
                        input:  win_input.clone(),
                    }).collect()
                }
                ref other => {
                    let inp_boxed: Box<QueryExpr> = Box::new(other.clone());
                    intents.into_iter().map(|intent| QueryExpr::SketchAgg {
                        op:    intent,
                        col:   col.clone(),
                        input: inp_boxed.clone(),
                    }).collect()
                }
            };
            let sketch = if sketch_nodes.len() == 1 {
                sketch_nodes.into_iter().next().unwrap()
            } else {
                QueryExpr::Merge { inputs: sketch_nodes }
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

/// Map an [`AggFunc`] to the canonical [`AggIntent`]s needed for sketch
/// execution. Returns an empty vec for non-sketchable functions
/// (`Custom`), one intent for single-statistic functions, and N intents
/// for the StdDev / Variance fan-out (Step α F1 strategy: callers wrap
/// the resulting list in a `QueryExpr::Merge` of sibling SketchAggs).
///
/// Step γ1 exposed this helper publicly so the legacy→canonical
/// `Aggregate` bridge (`aggregate_bridge::legacy_aggregate_to_canonical`)
/// can reuse the same `AggFunc` → `AggIntent` translation. The fan-out
/// semantics (StdDev / Variance → two siblings) are wrapped one layer
/// up (sketch lowering) before the bridge is called, so each `AggItem`
/// at the bridge level produces a single intent vector that's mostly
/// length 1.
pub(crate) fn agg_func_to_intents(func: &AggFunc) -> Vec<AggIntent> {
    use crate::intent_algebra::legacy_expr::{
        default_cardinality, default_frequency, default_quantile,
    };
    use crate::types_v2::AccuracyTarget;
    match func {
        AggFunc::Quantile(phi) => vec![default_quantile(*phi)],
        AggFunc::CountDistinct => vec![default_cardinality()],
        AggFunc::HeavyHitters { .. } => vec![default_frequency()],
        AggFunc::Count => vec![default_frequency()],
        AggFunc::Avg => vec![AggIntent::Quantile {
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        AggFunc::Min => vec![AggIntent::Min],
        AggFunc::Max => vec![AggIntent::Max],
        // StdDev / Variance: legacy carried two quantiles in a single
        // Quantile intent; Step α F1 fans them out into two siblings.
        AggFunc::StdDev { .. } | AggFunc::Variance { .. } => vec![
            AggIntent::Quantile { q: 0.25, accuracy: AccuracyTarget::Epsilon(0.01) },
            AggIntent::Quantile { q: 0.75, accuracy: AccuracyTarget::Epsilon(0.01) },
        ],
        AggFunc::Sum | AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => {
            vec![AggIntent::Sum]
        }
        AggFunc::Custom(_) => vec![],
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
    fn sum_lowered_to_sum() {
        let expr = make_agg(AggFunc::Sum, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Sum, .. }));
    }

    #[test]
    fn avg_lowered_to_quantile_p50() {
        let expr = make_agg(AggFunc::Avg, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::SketchAgg { op: AggIntent::Quantile { q, .. }, .. } => {
                assert!((*q - 0.5).abs() < 1e-9);
            }
            other => panic!("expected SketchAgg(Quantile), got {other:?}"),
        }
    }

    #[test]
    fn min_lowered_to_min() {
        let expr = make_agg(AggFunc::Min, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Min, .. }));
    }

    #[test]
    fn max_lowered_to_max() {
        let expr = make_agg(AggFunc::Max, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Max, .. }));
    }

    #[test]
    fn stddev_fans_out_to_merge_of_quantile_siblings() {
        // StdDev / Variance historically carried two quantiles in a
        // single legacy intent; Step α's F1 fan-out emits a Merge of two
        // single-φ SketchAgg siblings.
        let expr = make_agg(AggFunc::StdDev { population: false }, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::Merge { inputs } => {
                assert_eq!(inputs.len(), 2);
                let mut qs: Vec<f64> = inputs.iter().filter_map(|node| match node {
                    QueryExpr::SketchAgg { op: AggIntent::Quantile { q, .. }, .. } => Some(*q),
                    _ => None,
                }).collect();
                qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
                assert_eq!(qs, vec![0.25, 0.75]);
            }
            other => panic!("expected Merge of two SketchAgg, got {other:?}"),
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
                assert!(matches!(lhs.as_ref(), QueryExpr::SketchAgg { op: AggIntent::Sum, .. }));
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
    fn rate_lowered_to_sum() {
        let expr = make_agg(AggFunc::Rate, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Sum, .. }));
    }

    #[test]
    fn delta_lowered_to_sum() {
        let expr = make_agg(AggFunc::Delta, src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::SketchAgg { op: AggIntent::Sum, .. }));
    }
}
