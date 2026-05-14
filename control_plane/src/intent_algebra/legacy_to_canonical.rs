//! Step γ7 keystone: composable legacy → canonical L3 IR converter.
//!
//! Recursively converts a *whole* `legacy_expr::QueryExpr` tree into a
//! *whole* `query_expr::QueryExpr` tree. This is the single piece of
//! migration machinery the consumer-flip PRs build on: once every
//! producer routes its output through [`convert_root`], the consumer
//! migrations are pure pattern-match rewrites onto the canonical IR and
//! the legacy IR can be deleted.
//!
//! ## Relationship to the γ1–γ4 bridges
//!
//! The four one-way bridges (`aggregate_bridge`, `sketch_agg_bridge`,
//! `windowed_agg_bridge`, `topk_bridge`) are *approach (c)* helpers:
//! they return canonical-shape data *minus the child*, so a consumer can
//! pattern-match a single legacy node without converting the subtree.
//! That makes them non-composable — you cannot chain them into a full
//! tree conversion. This module absorbs their per-variant logic into a
//! recursive converter that *does* carry the child, and is therefore the
//! thing producers and consumers actually pivot on. The bridges stay in
//! place until their remaining annotation-only consumers migrate; PR 13
//! deletes them.
//!
//! ## Variant mapping
//!
//! | legacy `QueryExpr`         | canonical `QueryExpr`                              |
//! |---|---|
//! | `Source(spec)`             | `Scan { TimeSeries, label_filters: [], schema }`   |
//! | `Ref(name)`                | `Ref { name }`                                     |
//! | `Filter`                   | `Filter` (pred via [`convert_scalar`])             |
//! | `Project`                  | `Project` (each item's expr via [`convert_scalar`])|
//! | `Aggregate`                | `Aggregate` (keys→by, AggFunc→AggIntent, having)   |
//! | `Window`                   | `Window` (slide → Sliding else Tumbling)           |
//! | `SketchAgg`                | `Aggregate { by: [col], aggs: [op] }`              |
//! | `WindowedAgg`              | `Window { child: Aggregate { .. } }`               |
//! | `Partition`                | `Partition`                                        |
//! | `Distinct`                 | `Distinct`                                         |
//! | `TopK`                     | `Aggregate { aggs: [AggIntent::TopK] }` (HeavyHitter)|
//! | `Merge`                    | `Merge`                                            |
//! | `Join`                     | `Join` (None pred → `Literal(Bool(true))`)         |
//! | `SetOp`                    | `SetOp`                                            |
//! | `Sort`                     | `Sort`                                             |
//! | `Limit`                    | `Limit`                                            |
//! | `LetBinding`               | `LetBinding` (legacy `body` → canonical `child`)   |
//! | `PromQLSubquery`           | `Subquery`                                         |
//! | `BinaryOp`                 | `BinaryOp`                                         |
//!
//! ## Schema threading
//!
//! [`convert`] takes a `&Schema` — the schema in scope at the node — and
//! threads it unchanged to every child, matching the established
//! `column_resolution::infer_schema_for_root` convention. The
//! synthesized source schema is `(ts, value)` and almost
//! every legacy operator is schema-pass-through, so threading the root
//! schema down is correct except for the nested-schema-transform case
//! (an `Aggregate` below another `Aggregate`), which the legacy stack
//! also doesn't handle — proper bottom-up schema flow lands with the
//! canonical `output_schema_in` wiring downstream.

#![allow(dead_code)]

use std::time::Duration;

use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::binder::Binder;
use crate::intent_algebra::column_resolution::{
    resolve_named_keys, ResolveError,
};
use crate::intent_algebra::legacy_expr::{
    AggFunc, AggItem, ColumnRef as LColumnRef, PartitionKeys as LPartitionKeys,
    QueryExpr as LQueryExpr, ScalarExpr as LScalarExpr, WindowKind as LWindowKind, WindowSpec,
};
use crate::intent_algebra::query_expr::{
    from_legacy_scalar, ColumnRef as CColumnRef, HavingPredicate, LiteralValue,
    PartitionKeys as CPartitionKeys, Predicate, ProjectItem as CProjectItem,
    QueryExpr as CQueryExpr, QueryExprError, Source, WindowKind as CWindowKind,
};
use crate::intent_algebra::schema::Schema;
use crate::types_v2::{AccuracyTarget, BindingName};

/// Errors produced while converting a legacy `QueryExpr` to canonical.
///
/// `PartialEq` is not derived because [`QueryExprError`] (carried by
/// `Scalar`) does not derive it — callers compare via
/// `matches!(err, ConvertError::Variant { .. })`.
#[derive(Debug, Error)]
pub enum ConvertError {
    /// A column reference (`Aggregate` key, `SketchAgg` / `WindowedAgg`
    /// column, `TopK` partition key) did not resolve against the
    /// inherited schema.
    #[error("column resolution failed: {0}")]
    Resolve(#[from] ResolveError),
    /// An `AggItem.func` has no canonical `AggIntent` equivalent — only
    /// `AggFunc::Custom(_)` triggers this today.
    #[error("AggItem `{alias}` uses non-canonical func ({func_dbg}) — no AggIntent equivalent")]
    NoCanonicalIntent { alias: String, func_dbg: String },
    /// A legacy `ScalarExpr` leaf failed to translate. Unreachable in
    /// practice — `convert_scalar` handles every variant — but kept as a
    /// typed boundary around [`from_legacy_scalar`].
    #[error("scalar translation failed: {0}")]
    Scalar(QueryExprError),
}

/// Lower a legacy `QueryExpr` tree all the way to canonical.
///
/// Two internal walks, one public entry:
///
/// 1. [`lower_to_sketch_algebra`] folds Layer-2 relational `Aggregate
///    { AggFunc }` shapes into the sketch-fused legacy Layer-3 form
///    (`SketchAgg` / `WindowedAgg` / `Partition`). This is idempotent on
///    input that is already Layer 3 — the `SketchAgg` / `WindowedAgg`
///    arms are pass-through — so callers may hand `convert_root` either
///    level.
/// 2. [`convert`] maps that legacy Layer-3 tree onto the canonical IR.
///
/// The legacy Layer-3 fused IR never escapes this module: it is purely
/// the intermediate between these two walks.
///
/// The inherited schema comes from the [`Binder`] — the explicit L3
/// name-resolution pass — which builds the complete, self-contained
/// schema every `ColumnId` indexes into. Because the Binder guarantees
/// every referenced name is in scope, the per-arm positional resolution
/// below (`resolve_column_ref` / `resolve_named_keys`) is **total**: it
/// cannot raise `ConvertError::Resolve` on a well-formed legacy tree.
pub fn convert_root(legacy: &LQueryExpr) -> Result<CQueryExpr, ConvertError> {
    let lowered = lower_to_sketch_algebra(legacy.clone());
    let schema = Binder::new().bind(&lowered);
    convert(&lowered, &schema)
}

/// Convert a legacy `QueryExpr` tree to canonical against an explicit
/// inherited `schema`. The schema is threaded unchanged to every child —
/// see the module doc on schema threading.
pub fn convert(legacy: &LQueryExpr, schema: &Schema) -> Result<CQueryExpr, ConvertError> {
    Ok(match legacy {
        LQueryExpr::Source(spec) => CQueryExpr::Scan {
            source: Source::TimeSeries {
                metric: spec.name.clone(),
            },
            label_filters: Vec::new(),
            // Carry the Binder's complete schema — the same self-contained
            // scope every `ColumnId` in this tree resolves against.
            schema: schema.clone(),
        },

        LQueryExpr::Ref(name) => CQueryExpr::Ref {
            name: BindingName::new(name.clone()),
        },

        LQueryExpr::Filter { pred, input } => CQueryExpr::Filter {
            pred: convert_scalar(pred, schema)?,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Project { cols, input } => {
            let cols = cols
                .iter()
                .map(|pi| {
                    Ok(CProjectItem {
                        alias: pi.alias.clone(),
                        expr: convert_scalar(&pi.expr, schema)?,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?;
            CQueryExpr::Project {
                cols,
                child: Box::new(convert(input, schema)?),
            }
        }

        LQueryExpr::Aggregate {
            keys,
            aggs,
            having,
            input,
        } => {
            let by = resolve_named_keys(keys, schema)?;
            let mut intents: Vec<AggIntent> = Vec::with_capacity(aggs.len());
            for item in aggs {
                // Faithful mapping for un-grouped `COUNT(*)`: `legacy_lower`
                // deliberately leaves it un-lowered (no GROUP BY → exact
                // row count, no sketch benefit). `agg_func_to_intents` is
                // the *lowering* map and would pick the `Frequency` sketch
                // — that loses the "this is exact" decision. Map it to
                // `Count { Exact }` so downstream (`capability_for`, the
                // `QeCollector`) sees it as exact, matching the legacy
                // `Aggregate { AggItem { Count }, keys: [] }` semantics.
                if matches!(item.func, AggFunc::Count) && keys.is_empty() {
                    intents.push(AggIntent::Count {
                        accuracy: AccuracyTarget::Exact,
                    });
                    continue;
                }
                let mapped = agg_func_to_intents(&item.func);
                if mapped.is_empty() {
                    return Err(ConvertError::NoCanonicalIntent {
                        alias: item.alias.clone(),
                        func_dbg: format!("{:?}", item.func),
                    });
                }
                intents.extend(mapped);
            }
            let having = match having {
                None => None,
                Some(se) => Some(HavingPredicate(format!("{:?}", convert_scalar(se, schema)?))),
            };
            CQueryExpr::Aggregate {
                by,
                aggs: intents,
                having,
                child: Box::new(convert(input, schema)?),
            }
        }

        LQueryExpr::Window {
            duration,
            slide,
            input,
        } => CQueryExpr::Window {
            kind: if slide.is_some() {
                CWindowKind::Sliding
            } else {
                CWindowKind::Tumbling
            },
            size: *duration,
            slide: *slide,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::SketchAgg { op, col: _, input } => {
            // `SketchAgg.col` is the sketch's *input* column, not a
            // GROUP BY key — canonical `Aggregate.by` is the group-by
            // tuple, which for a `SketchAgg` is always empty (grouping
            // rides on the wrapping `Partition` node). The sketch input
            // is the canonical implicit `value` column, so `col` carries
            // no information the canonical IR needs.
            CQueryExpr::Aggregate {
                by: Vec::new(),
                aggs: vec![op.clone()],
                having: None,
                child: Box::new(convert(input, schema)?),
            }
        }

        LQueryExpr::WindowedAgg {
            agg,
            window,
            col: _,
            input,
        } => {
            let (kind, size, slide) = map_window_kind(window);
            // As with `SketchAgg`: `col` is the sketch input column, not
            // a GROUP BY key — the inner canonical `Aggregate.by` is empty.
            CQueryExpr::Window {
                kind,
                size,
                slide,
                child: Box::new(CQueryExpr::Aggregate {
                    by: Vec::new(),
                    aggs: vec![agg.clone()],
                    having: None,
                    child: Box::new(convert(input, schema)?),
                }),
            }
        }

        LQueryExpr::Partition { keys, input } => CQueryExpr::Partition {
            keys: match keys {
                LPartitionKeys::By(k) => CPartitionKeys::By(k.clone()),
                LPartitionKeys::Without(k) => CPartitionKeys::Without(k.clone()),
            },
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Distinct { cols, input } => CQueryExpr::Distinct {
            cols: cols.iter().map(convert_column_ref).collect(),
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::TopK { k, by, input } => {
            // γ4 classification default: heavy-hitter intent. The generic
            // `Sort + Limit` case never reaches a legacy `TopK` node —
            // see `topk_bridge` module doc.
            let by = resolve_named_keys(by, schema)?;
            CQueryExpr::Aggregate {
                by,
                aggs: vec![AggIntent::TopK {
                    k: *k as usize,
                    accuracy: AccuracyTarget::Epsilon(0.05),
                }],
                having: None,
                child: Box::new(convert(input, schema)?),
            }
        }

        LQueryExpr::Merge { inputs } => CQueryExpr::Merge {
            children: inputs
                .iter()
                .map(|i| convert(i, schema))
                .collect::<Result<Vec<_>, _>>()?,
        },

        LQueryExpr::Join {
            kind,
            pred,
            left,
            right,
        } => CQueryExpr::Join {
            kind: kind.clone(),
            // Canonical `Join` requires a predicate; a legacy `None` pred
            // is a CROSS JOIN — model it as the tautology `true`.
            pred: match pred {
                Some(se) => convert_scalar(se, schema)?,
                None => Predicate::Literal(LiteralValue::Bool(true)),
            },
            left: Box::new(convert(left, schema)?),
            right: Box::new(convert(right, schema)?),
        },

        LQueryExpr::SetOp {
            kind,
            all,
            left,
            right,
        } => CQueryExpr::SetOp {
            kind: kind.clone(),
            all: *all,
            left: Box::new(convert(left, schema)?),
            right: Box::new(convert(right, schema)?),
        },

        LQueryExpr::Sort { keys, input } => CQueryExpr::Sort {
            // `SortKey` is the single canonical type (deduped in PR 1).
            keys: keys.clone(),
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Limit { n, offset, input } => CQueryExpr::Limit {
            n: *n as usize,
            offset: *offset as usize,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::LetBinding { name, expr, body } => CQueryExpr::LetBinding {
            name: BindingName::new(name.clone()),
            expr: Box::new(convert(expr, schema)?),
            // legacy `body` is the canonical `child`.
            child: Box::new(convert(body, schema)?),
        },

        LQueryExpr::PromQLSubquery {
            range,
            resolution,
            input,
        } => CQueryExpr::Subquery {
            range: *range,
            resolution: *resolution,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::BinaryOp {
            op,
            lhs,
            rhs,
            vector_match,
        } => CQueryExpr::BinaryOp {
            // `BinaryOpKind` / `VectorMatch` are the single canonical
            // types (deduped in PR 1).
            op: op.clone(),
            lhs: Box::new(convert(lhs, schema)?),
            rhs: Box::new(convert(rhs, schema)?),
            vector_match: vector_match.clone(),
        },
    })
}

/// Translate a legacy `ScalarExpr` to a canonical `Predicate`, recursing
/// through every composite variant so a nested `ScalarSubquery` (which
/// carries a legacy `QueryExpr` sub-tree) can be converted via [`convert`].
/// `from_legacy_scalar` alone cannot do this — it has no converter to
/// recurse with — which is why `ScalarSubquery` was the one arm it defers.
pub fn convert_scalar(se: &LScalarExpr, schema: &Schema) -> Result<Predicate, ConvertError> {
    match se {
        LScalarExpr::ScalarSubquery(inner) => {
            Ok(Predicate::ScalarSubquery(Box::new(convert(inner, schema)?)))
        }
        LScalarExpr::BinaryOp { op, lhs, rhs } => Ok(Predicate::BinaryOp {
            op: op.clone(),
            lhs: Box::new(convert_scalar(lhs, schema)?),
            rhs: Box::new(convert_scalar(rhs, schema)?),
        }),
        LScalarExpr::IsNull { expr, negated } => Ok(Predicate::IsNull {
            expr: Box::new(convert_scalar(expr, schema)?),
            negated: *negated,
        }),
        LScalarExpr::FunctionCall { name, args } => Ok(Predicate::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| convert_scalar(a, schema))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        LScalarExpr::InList {
            expr,
            list,
            negated,
        } => Ok(Predicate::InList {
            expr: Box::new(convert_scalar(expr, schema)?),
            list: list
                .iter()
                .map(|a| convert_scalar(a, schema))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        }),
        LScalarExpr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Predicate::Between {
            expr: Box::new(convert_scalar(expr, schema)?),
            low: Box::new(convert_scalar(low, schema)?),
            high: Box::new(convert_scalar(high, schema)?),
            negated: *negated,
        }),
        // `Column` / `Literal` are subquery-free leaves — `from_legacy_scalar`
        // translates them directly (and cannot fail for these arms).
        LScalarExpr::Column(_) | LScalarExpr::Literal(_) => {
            from_legacy_scalar(se).map_err(ConvertError::Scalar)
        }
    }
}

/// Map a legacy `WindowSpec` to canonical `(WindowKind, size, slide)`.
/// Total — every legacy `WindowKind` variant has a canonical equivalent
/// (the catalogue-less `Unbounded` / `Landmark` were retired in PR 12).
fn map_window_kind(window: &WindowSpec) -> (CWindowKind, Duration, Option<Duration>) {
    match &window.kind {
        LWindowKind::Tumbling { size } => (CWindowKind::Tumbling, *size, None),
        LWindowKind::Sliding { size, slide } => {
            (CWindowKind::Sliding, *size, Some(*slide))
        }
        LWindowKind::Session { gap } => (CWindowKind::Session, *gap, None),
    }
}

/// Legacy `ColumnRef` → canonical `ColumnRef`. The two enums have
/// identical variants; the canonical one is what L3 nodes carry.
fn convert_column_ref(c: &LColumnRef) -> CColumnRef {
    match c {
        LColumnRef::Named(n) => CColumnRef::Named(n.clone()),
        LColumnRef::SampleValue => CColumnRef::SampleValue,
        LColumnRef::Wildcard => CColumnRef::Wildcard,
    }
}

// ── Layer 2 → Layer 3 sketch lowering ────────────────────────────────────────
//
// Formerly `intent_algebra::legacy_lower`. Folded in here because its sole
// caller is `convert_root` above (the `query_parser` entry points emit raw
// Layer-2 trees and route them straight through `convert_root`). Keeping it
// private to this module means the sketch-fused legacy Layer-3 IR
// (`SketchAgg` / `WindowedAgg`) is no longer a surface anything outside the
// converter can construct or observe.

/// Lower a Layer-2 legacy `QueryExpr` (relational operators only) to the
/// Layer-3 sketch algebra: `Aggregate { AggFunc }` nodes whose function
/// maps to a sketch intent become `SketchAgg { AggIntent }` (or, fused
/// under a `Window`, `WindowedAgg`). Multi-agg `Aggregate`s, those with
/// `HAVING`, and non-sketchable functions pass through unchanged.
///
/// Idempotent on input that is already Layer 3: the `SketchAgg` /
/// `WindowedAgg` arms simply recurse.
fn lower_to_sketch_algebra(expr: LQueryExpr) -> LQueryExpr {
    match expr {
        LQueryExpr::Aggregate {
            keys,
            aggs,
            having,
            input,
        } => {
            let input = lower_to_sketch_algebra(*input);
            lower_aggregate(keys, aggs, having, Box::new(input))
        }

        // ── Single-input nodes: recurse ──────────────────────────────────
        LQueryExpr::Filter { pred, input } => LQueryExpr::Filter {
            pred,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Project { cols, input } => LQueryExpr::Project {
            cols,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Window {
            duration,
            slide,
            input,
        } => {
            let lowered_input = lower_to_sketch_algebra(*input);
            // Fuse Window + SketchAgg → WindowedAgg (the window defines the
            // sketch lifecycle).
            if let LQueryExpr::SketchAgg {
                op,
                col,
                input: sketch_input,
            } = lowered_input
            {
                let window = WindowSpec {
                    kind: match slide {
                        Some(s) => LWindowKind::Sliding {
                            size: duration,
                            slide: s,
                        },
                        None => LWindowKind::Tumbling { size: duration },
                    },
                    time_col: None,
                };
                LQueryExpr::WindowedAgg {
                    agg: op,
                    window,
                    col,
                    input: sketch_input,
                }
            } else {
                LQueryExpr::Window {
                    duration,
                    slide,
                    input: Box::new(lowered_input),
                }
            }
        }
        LQueryExpr::SketchAgg { op, col, input } => LQueryExpr::SketchAgg {
            op,
            col,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::WindowedAgg {
            agg,
            window,
            col,
            input,
        } => LQueryExpr::WindowedAgg {
            agg,
            window,
            col,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Partition { keys, input } => LQueryExpr::Partition {
            keys,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Distinct { cols, input } => LQueryExpr::Distinct {
            cols,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::TopK { k, by, input } => LQueryExpr::TopK {
            k,
            by,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Sort { keys, input } => LQueryExpr::Sort {
            keys,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::Limit { n, offset, input } => LQueryExpr::Limit {
            n,
            offset,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },
        LQueryExpr::PromQLSubquery {
            range,
            resolution,
            input,
        } => LQueryExpr::PromQLSubquery {
            range,
            resolution,
            input: Box::new(lower_to_sketch_algebra(*input)),
        },

        // ── Two-input nodes: recurse into both ──────────────────────────
        LQueryExpr::BinaryOp {
            op,
            lhs,
            rhs,
            vector_match,
        } => LQueryExpr::BinaryOp {
            op,
            lhs: Box::new(lower_to_sketch_algebra(*lhs)),
            rhs: Box::new(lower_to_sketch_algebra(*rhs)),
            vector_match,
        },
        LQueryExpr::Join {
            kind,
            pred,
            left,
            right,
        } => LQueryExpr::Join {
            kind,
            pred,
            left: Box::new(lower_to_sketch_algebra(*left)),
            right: Box::new(lower_to_sketch_algebra(*right)),
        },
        LQueryExpr::SetOp {
            kind,
            all,
            left,
            right,
        } => LQueryExpr::SetOp {
            kind,
            all,
            left: Box::new(lower_to_sketch_algebra(*left)),
            right: Box::new(lower_to_sketch_algebra(*right)),
        },

        // ── Multi-input / container nodes ───────────────────────────────
        LQueryExpr::Merge { inputs } => LQueryExpr::Merge {
            inputs: inputs.into_iter().map(lower_to_sketch_algebra).collect(),
        },
        LQueryExpr::LetBinding { name, expr, body } => LQueryExpr::LetBinding {
            name,
            expr: Box::new(lower_to_sketch_algebra(*expr)),
            body: Box::new(lower_to_sketch_algebra(*body)),
        },

        // ── Leaf nodes: pass through ────────────────────────────────────
        LQueryExpr::Source(_) | LQueryExpr::Ref(_) => expr,
    }
}

/// Lower a single `Aggregate` node to `SketchAgg` / `WindowedAgg` where the
/// aggregation benefits from a sketch.
///
/// Single-statistic functions produce one sketch node; `StdDev` / `Variance`
/// fan out into two sibling quantile sketches wrapped in a `Merge` (Step α
/// F1 strategy). `GROUP BY` keys become a wrapping `Partition`. Multi-agg,
/// `HAVING`-bearing, and non-sketchable (`Custom`) aggregates pass through
/// as a relational `Aggregate`.
fn lower_aggregate(
    keys: Vec<String>,
    aggs: Vec<AggItem>,
    having: Option<LScalarExpr>,
    input: Box<LQueryExpr>,
) -> LQueryExpr {
    // Only lower single-agg Aggregates without HAVING.
    if aggs.len() == 1 && having.is_none() {
        let agg = &aggs[0];

        // COUNT(*) without GROUP BY is a simple row count — no sketch benefit.
        if matches!(agg.func, AggFunc::Count) && keys.is_empty() {
            return LQueryExpr::Aggregate {
                keys,
                aggs,
                having,
                input,
            };
        }

        let intents = agg_func_to_intents(&agg.func);
        if !intents.is_empty() {
            let col = agg.col.clone();
            let sketch_nodes: Vec<LQueryExpr> = match *input {
                LQueryExpr::Window {
                    duration,
                    slide,
                    input: ref win_input,
                } => {
                    let window = WindowSpec {
                        kind: match slide {
                            Some(s) => LWindowKind::Sliding {
                                size: duration,
                                slide: s,
                            },
                            None => LWindowKind::Tumbling { size: duration },
                        },
                        time_col: None,
                    };
                    intents
                        .into_iter()
                        .map(|intent| LQueryExpr::WindowedAgg {
                            agg: intent,
                            window: window.clone(),
                            col: col.clone(),
                            input: win_input.clone(),
                        })
                        .collect()
                }
                ref other => {
                    let inp_boxed: Box<LQueryExpr> = Box::new(other.clone());
                    intents
                        .into_iter()
                        .map(|intent| LQueryExpr::SketchAgg {
                            op: intent,
                            col: col.clone(),
                            input: inp_boxed.clone(),
                        })
                        .collect()
                }
            };
            let sketch = if sketch_nodes.len() == 1 {
                sketch_nodes.into_iter().next().unwrap()
            } else {
                LQueryExpr::Merge {
                    inputs: sketch_nodes,
                }
            };

            return if keys.is_empty() {
                sketch
            } else {
                LQueryExpr::Partition {
                    keys: LPartitionKeys::By(keys),
                    input: Box::new(sketch),
                }
            };
        }
    }

    // Multi-agg or non-sketchable: keep as relational Aggregate.
    LQueryExpr::Aggregate {
        keys,
        aggs,
        having,
        input,
    }
}

/// Map an [`AggFunc`] to the canonical [`AggIntent`]s needed for sketch
/// execution. Empty for non-sketchable functions (`Custom`); one intent
/// for single-statistic functions; two for the `StdDev` / `Variance`
/// fan-out (the caller wraps the pair in a `Merge` of sibling sketches).
fn agg_func_to_intents(func: &AggFunc) -> Vec<AggIntent> {
    use crate::intent_algebra::legacy_expr::{
        default_cardinality, default_frequency, default_quantile,
    };
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
            AggIntent::Quantile {
                q: 0.25,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Quantile {
                q: 0.75,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
        ],
        AggFunc::Sum | AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => {
            vec![AggIntent::Sum]
        }
        AggFunc::Custom(_) => vec![],
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::legacy_expr::{
        AggFunc, AggItem, ColumnRef as LColumnRef, ProjectItem as LProjectItem, SourceSpec,
    };

    fn src(name: &str) -> LQueryExpr {
        LQueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn agg_item(alias: &str, func: AggFunc) -> AggItem {
        AggItem {
            alias: alias.into(),
            func,
            col: LColumnRef::SampleValue,
            distinct: false,
        }
    }

    #[test]
    fn source_becomes_scan_with_synthesized_schema() {
        let c = convert_root(&src("http_requests_total")).unwrap();
        match c {
            CQueryExpr::Scan {
                source,
                label_filters,
                schema,
            } => {
                assert!(matches!(source, Source::TimeSeries { metric } if metric == "http_requests_total"));
                assert!(label_filters.is_empty());
                assert_eq!(schema.columns.len(), 2); // ts, value
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    #[test]
    fn ref_and_let_binding_round_trip_names() {
        let legacy = LQueryExpr::LetBinding {
            name: "cte".into(),
            expr: Box::new(src("m")),
            body: Box::new(LQueryExpr::Ref("cte".into())),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::LetBinding { name, child, .. } => {
                assert_eq!(name.as_str(), "cte");
                assert!(matches!(*child, CQueryExpr::Ref { name } if name.as_str() == "cte"));
            }
            other => panic!("expected LetBinding, got {other:?}"),
        }
    }

    #[test]
    fn window_over_aggregate_full_tree() {
        // Window { Aggregate { keys: [], aggs: [Sum], Source } }
        let legacy = LQueryExpr::Window {
            duration: Duration::from_secs(300),
            slide: None,
            input: Box::new(LQueryExpr::Aggregate {
                keys: vec![],
                aggs: vec![agg_item("s", AggFunc::Sum)],
                having: None,
                input: Box::new(src("m")),
            }),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Window {
                kind, size, child, ..
            } => {
                assert_eq!(kind, CWindowKind::Tumbling);
                assert_eq!(size, Duration::from_secs(300));
                match *child {
                    CQueryExpr::Aggregate { aggs, child, .. } => {
                        assert!(matches!(aggs.as_slice(), [AggIntent::Sum]));
                        assert!(matches!(*child, CQueryExpr::Scan { .. }));
                    }
                    other => panic!("expected Aggregate, got {other:?}"),
                }
            }
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn sliding_window_carries_slide() {
        let legacy = LQueryExpr::Window {
            duration: Duration::from_secs(600),
            slide: Some(Duration::from_secs(60)),
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Window { kind, slide, .. } => {
                assert_eq!(kind, CWindowKind::Sliding);
                assert_eq!(slide, Some(Duration::from_secs(60)));
            }
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn sketch_agg_folds_into_aggregate() {
        // SketchAgg { Sum, SampleValue, Source } → Aggregate { by: [], [Sum] }.
        // `col` is the sketch *input*, not a GROUP BY key — `by` is empty
        // (grouping rides on a wrapping `Partition`, not the SketchAgg).
        let legacy = LQueryExpr::SketchAgg {
            op: AggIntent::Sum,
            col: LColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, aggs, .. } => {
                assert!(by.is_empty(), "SketchAgg.col is not a group-by key: {by:?}");
                assert!(matches!(aggs.as_slice(), [AggIntent::Sum]));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn sketch_agg_named_col_still_has_empty_by() {
        // Even a `Named` sketch-target column is not a group-by key.
        let legacy = LQueryExpr::SketchAgg {
            op: AggIntent::Sum,
            col: LColumnRef::Named("price".into()),
            input: Box::new(src("trades")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, .. } => assert!(by.is_empty()),
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn windowed_agg_folds_into_window_over_aggregate() {
        let legacy = LQueryExpr::WindowedAgg {
            agg: AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            window: WindowSpec {
                kind: LWindowKind::Tumbling {
                    size: Duration::from_secs(300),
                },
                time_col: None,
            },
            col: LColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Window { kind, child, .. } => {
                assert_eq!(kind, CWindowKind::Tumbling);
                assert!(matches!(
                    *child,
                    CQueryExpr::Aggregate { ref aggs, .. }
                        if matches!(aggs.as_slice(), [AggIntent::Quantile { .. }])
                ));
            }
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn topk_folds_into_aggregate_with_topk_intent() {
        let legacy = LQueryExpr::TopK {
            k: 5,
            by: vec![],
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, aggs, .. } => {
                assert!(by.is_empty());
                assert!(matches!(aggs.as_slice(), [AggIntent::TopK { k: 5, .. }]));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn custom_agg_func_errors() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![agg_item("u", AggFunc::Custom("my_udf".into()))],
            having: None,
            input: Box::new(src("m")),
        };
        assert!(matches!(
            convert_root(&legacy).unwrap_err(),
            ConvertError::NoCanonicalIntent { .. }
        ));
    }

    #[test]
    fn filter_pred_with_scalar_subquery_recurses() {
        // Filter { pred: ScalarSubquery(Ref("cte")), Source } — the
        // ScalarSubquery arm `from_legacy_scalar` defers is handled here
        // by recursing through `convert`.
        let legacy = LQueryExpr::Filter {
            pred: LScalarExpr::ScalarSubquery(Box::new(LQueryExpr::Ref("cte".into()))),
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Filter { pred, .. } => match pred {
                Predicate::ScalarSubquery(inner) => {
                    assert!(matches!(*inner, CQueryExpr::Ref { name } if name.as_str() == "cte"));
                }
                other => panic!("expected ScalarSubquery, got {other:?}"),
            },
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn project_translates_each_item_expr() {
        let legacy = LQueryExpr::Project {
            cols: vec![LProjectItem {
                alias: Some("v".into()),
                expr: LScalarExpr::Column("value".into()),
            }],
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Project { cols, .. } => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0].alias.as_deref(), Some("v"));
                assert!(matches!(&cols[0].expr, Predicate::Column(_)));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn binary_op_converts_both_sides() {
        let legacy = LQueryExpr::BinaryOp {
            op: crate::intent_algebra::query_expr::BinaryOpKind::Add,
            lhs: Box::new(src("a")),
            rhs: Box::new(src("b")),
            vector_match: None,
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::BinaryOp { lhs, rhs, .. } => {
                assert!(matches!(*lhs, CQueryExpr::Scan { .. }));
                assert!(matches!(*rhs, CQueryExpr::Scan { .. }));
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn cross_join_none_pred_becomes_true_literal() {
        let legacy = LQueryExpr::Join {
            kind: crate::intent_algebra::query_expr::JoinKind::Cross,
            pred: None,
            left: Box::new(src("a")),
            right: Box::new(src("b")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Join { pred, .. } => {
                assert!(matches!(pred, Predicate::Literal(LiteralValue::Bool(true))));
            }
            other => panic!("expected Join, got {other:?}"),
        }
    }

    #[test]
    fn promql_subquery_becomes_canonical_subquery() {
        let legacy = LQueryExpr::PromQLSubquery {
            range: Duration::from_secs(3600),
            resolution: Some(Duration::from_secs(60)),
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Subquery {
                range, resolution, ..
            } => {
                assert_eq!(range, Duration::from_secs(3600));
                assert_eq!(resolution, Some(Duration::from_secs(60)));
            }
            other => panic!("expected Subquery, got {other:?}"),
        }
    }

    #[test]
    fn nested_tree_converts_recursively() {
        // Sort { Limit { Filter { Window { Source } } } } — exercises a
        // deep pass-through chain in one shot.
        let legacy = LQueryExpr::Sort {
            keys: vec![],
            input: Box::new(LQueryExpr::Limit {
                n: 10,
                offset: 0,
                input: Box::new(LQueryExpr::Filter {
                    pred: LScalarExpr::Literal(
                        crate::intent_algebra::legacy_expr::LiteralValue::Bool(true),
                    ),
                    input: Box::new(LQueryExpr::Window {
                        duration: Duration::from_secs(60),
                        slide: None,
                        input: Box::new(src("m")),
                    }),
                }),
            }),
        };
        let c = convert_root(&legacy).unwrap();
        // Walk down and assert the spine survived.
        let CQueryExpr::Sort { child, .. } = c else {
            panic!("expected Sort")
        };
        let CQueryExpr::Limit { n, child, .. } = *child else {
            panic!("expected Limit")
        };
        assert_eq!(n, 10);
        let CQueryExpr::Filter { child, .. } = *child else {
            panic!("expected Filter")
        };
        let CQueryExpr::Window { child, .. } = *child else {
            panic!("expected Window")
        };
        assert!(matches!(*child, CQueryExpr::Scan { .. }));
    }
}

#[cfg(test)]
mod lower_tests {
    //! Ported from the former `intent_algebra::legacy_lower` module —
    //! exercises the private Layer-2 → Layer-3 `lower_to_sketch_algebra`
    //! sketch-fusion pass that `convert_root` now runs internally.
    use super::lower_to_sketch_algebra;
    use crate::intent_algebra::legacy_expr::*;
    use std::time::Duration;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    fn make_agg(func: AggFunc, input: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![AggItem {
                alias: "v".into(),
                func,
                col: ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input: Box::new(input),
        }
    }

    fn make_agg_with_keys(func: AggFunc, keys: Vec<String>, input: QueryExpr) -> QueryExpr {
        QueryExpr::Aggregate {
            keys,
            aggs: vec![AggItem {
                alias: "v".into(),
                func,
                col: ColumnRef::SampleValue,
                distinct: false,
            }],
            having: None,
            input: Box::new(input),
        }
    }

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

    #[test]
    fn group_by_wraps_with_partition() {
        let expr = make_agg_with_keys(AggFunc::Quantile(0.5), vec!["host".into()], src("m"));
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::Partition { keys, input } => {
                assert_eq!(keys.keys(), &["host".to_string()]);
                assert!(matches!(input.as_ref(), QueryExpr::SketchAgg { .. }));
            }
            other => panic!("expected Partition, got {other:?}"),
        }
    }

    #[test]
    fn multi_agg_not_lowered() {
        let expr = QueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![
                AggItem { alias: "c".into(), func: AggFunc::Count, col: ColumnRef::Wildcard, distinct: false },
                AggItem { alias: "s".into(), func: AggFunc::Sum, col: ColumnRef::Named("x".into()), distinct: false },
            ],
            having: None,
            input: Box::new(src("m")),
        };
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Aggregate { .. }));
    }

    #[test]
    fn window_wrapping_aggregate_fuses_to_windowed_agg() {
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide: None,
            input: Box::new(make_agg(AggFunc::Quantile(0.99), src("m"))),
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
            slide: Some(Duration::from_secs(60)),
            input: Box::new(make_agg(AggFunc::Quantile(0.5), src("m"))),
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
        let expr = QueryExpr::Window {
            duration: Duration::from_secs(300),
            slide: None,
            input: Box::new(make_agg(AggFunc::Custom("my_udf".into()), src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        assert!(matches!(lowered, QueryExpr::Window { .. }));
    }

    #[test]
    fn lowering_recurses_into_binary_op() {
        let expr = QueryExpr::BinaryOp {
            op: BinaryOpKind::Add,
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
            k: 10,
            by: vec!["symbol".into()],
            input: Box::new(make_agg_with_keys(AggFunc::Count, vec!["symbol".into()], src("m"))),
        };
        let lowered = lower_to_sketch_algebra(expr);
        match &lowered {
            QueryExpr::TopK { input, .. } => {
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
