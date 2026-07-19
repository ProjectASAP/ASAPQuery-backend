//! The Layer-2 → canonical L3 IR converter.
//!
//! Recursively converts a *whole* `relational::QueryExpr` tree (the raw
//! Layer-2 relational IR the `query_parser` front ends emit) into a
//! *whole* canonical `query_expr::QueryExpr` tree. This is the single
//! entry the parse path routes through — [`convert_root`].
//!
//! ## Phase 2 step 3 (docs/migration-plan-backend-plan.md)
//!
//! Two shape changes since the canonical `QueryExpr` merged onto
//! `asap_ir` (see `query_expr.rs`'s module docs for the full rationale):
//!
//! - **Predicates** translate to `L3Expr` (via `Predicate`, `expr_ir.rs`)
//!   instead of this repo's old 8-variant `Predicate` enum. `Between`
//!   desugars via `query_expr::between`. **`ScalarSubquery` is rejected**
//!   ([`ConvertError::UnsupportedScalarSubquery`]) rather than lowered:
//!   `asap_ir`'s `L3Expr` has no slot for "reference the value bound by
//!   an enclosing `LetBinding`" (the old `Predicate::Column(ColumnRef::
//!   Named(subq_name))` was itself a hack the canonical positional
//!   `L3Expr::Column(ColumnId)` can't reproduce without inventing a
//!   sentinel id space nothing downstream knows about). ASAPController's
//!   own L2→L3 lowering (`crates/l2/src/lower.rs`) has no correlated-
//!   subquery construct either — its `Ref`/`LetBinding` are documented as
//!   "Reserved: no front end emits yet." This repo's own front ends never
//!   construct `ScalarExpr::ScalarSubquery` either (grep-verified: the
//!   only non-test, non-definition sites were `lower.rs` itself and the
//!   now-dead `optimizer/engine.rs` R7 rule), so rejecting it is a no-op
//!   for every real query today. `optimizer/engine.rs`'s R7
//!   `SubqueryDecorrelation` is deleted rather than ported — it pattern-
//!   matched the removed `Predicate::BinaryOp` / `Predicate::ScalarSubquery`
//!   / `Predicate::Column(ColumnRef::Named)` variants directly and has no
//!   construction site left to fire against. Real correlated-subquery
//!   support is follow-up work once `asap_ir` grows a representation for
//!   it.
//! - **`GROUP BY` keys attach directly to `Aggregate.by: GroupKeys`**
//!   instead of wrapping the result in a `Partition` node (removed from
//!   `asap_ir`'s `QueryExpr` — folded into `GroupKeys`'s `by`/`without`
//!   distinction). The single-statistic fusion arm resolves `keys` once
//!   and threads `GroupKeys` into every fused node (including both
//!   siblings of a `StdDev`/`Variance` `Merge` fan-out) instead of
//!   wrapping the finished shape afterward. A standalone legacy
//!   `LQueryExpr::Partition` (not part of the fusion arm) folds its keys
//!   into the nearest `Aggregate` its converted subtree contains, via
//!   [`fold_partition_keys`].
//!
//! `having` is `Option<Predicate>` (real typed HAVING) directly — no
//! translation needed beyond running the HAVING expression through
//! [`convert_scalar`] like any other predicate.
//!
//! ## Variant mapping
//!
//! | relational `QueryExpr`     | canonical `QueryExpr`                              |
//! |---|---|
//! | `Source(spec)`             | `Scan { TimeSeries, predicates: [], schema }`      |
//! | `Ref(name)`                | `Ref { name }`                                     |
//! | `Filter`                   | `Filter` (pred via [`convert_scalar`])             |
//! | `Project`                  | `Project` (each item's expr via [`convert_scalar`])|
//! | `Aggregate`                | single agg + no HAVING → *fuses* (see below); otherwise plain `Aggregate` (keys→by: GroupKeys, AggFunc→AggIntent via [`agg_func_to_intents`]) |
//! | `Window`                   | `Window` (slide → Sliding else Tumbling)           |
//! | `Partition`                | keys folded into the nearest `Aggregate.by` inside the converted subtree, via [`fold_partition_keys`] |
//! | `Distinct`                 | `Distinct`                                         |
//! | `TopK`                     | `Aggregate { aggs: [AggIntent::TopK] }` (HeavyHitter)|
//! | `Merge`                    | `Merge`                                            |
//! | `Join`                     | `Join` (None pred → `Literal(Bool(true))`)         |
//! | `SetOp`                    | `SetOp`                                            |
//! | `Sort`                     | `Sort` (keys pass through — `relational::SortKey` already re-exports the canonical, `L3Expr`-based type) |
//! | `Limit`                    | `Limit`                                            |
//! | `LetBinding`               | `LetBinding` (relational `body` → canonical `child`)|
//! | `PromQLSubquery`           | `Subquery`                                         |
//! | `BinaryOp`                 | `BinaryOp` (`op` passes straight through — `relational::BinaryOpKind` is a re-export of the canonical type, not a separate flat enum) |
//!
//! A single-statistic `Aggregate` (exactly one `AggItem`, no `HAVING`)
//! fuses directly into canonical shape rather than staying a plain
//! `Aggregate` wrapping the untouched child:
//!   * input is a `Window` → emit `Window { Aggregate { by } } }` (the
//!     window-defines-sketch-lifecycle shape);
//!   * otherwise → emit `Aggregate { by }`;
//!   * `StdDev` / `Variance` fan out into a `Merge` of two sibling
//!     quantile aggregates, both carrying the same `by` (Step α F1
//!     strategy) — the only `AggFunc`s [`agg_func_to_intents`] maps to
//!     more than one `AggIntent`;
//!   * `GROUP BY` keys resolve once and thread into every fused node's
//!     `by: GroupKeys` directly.
//! `AggFunc::Custom` produces no canonical intent regardless of arity —
//! [`agg_func_to_intents`] returns empty, which raises
//! [`ConvertError::NoCanonicalIntent`] once execution reaches the plain
//! multi-agg path (single-agg-with-empty-intents falls through to it
//! rather than being special-cased inline).
//!
//! ## Schema threading
//!
//! [`convert`] takes a `&Schema` — the schema in scope at the node — and
//! threads it unchanged to every child. The [`Binder`]-built schema is
//! complete and self-contained (`(ts, value)` plus every referenced
//! name), so threading the root schema down is correct except for the
//! nested-schema-transform case (an `Aggregate` below another
//! `Aggregate`), which the L2→L3 lowering also doesn't handle — proper
//! bottom-up schema flow lands with the canonical `output_schema_in`
//! wiring downstream.

#![allow(dead_code)]

use asap_ir::intent_algebra::BindingName;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::binder::Binder;
use crate::intent_algebra::column_resolution::{
    resolve_column_ref, resolve_column_refs, resolve_named_keys, ResolveError,
};
use crate::intent_algebra::query_expr::{
    between, ArithOp, BinaryOpKind, CompareOp, GroupKeys, L3Scalar, Predicate,
    ProjectItem as CProjectItem, QueryExpr as CQueryExpr, Source, WindowKind as CWindowKind,
};
use crate::intent_algebra::relational::{
    AggFunc, ColumnRef as LColumnRef, PartitionKeys as LPartitionKeys, QueryExpr as LQueryExpr,
    ScalarExpr as LScalarExpr,
};
use crate::intent_algebra::schema::Schema;
use crate::intent_algebra::L3Expr;
use crate::types_v2::AccuracyTarget;

/// Errors produced while converting a legacy `QueryExpr` to canonical.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// A column reference (`Aggregate` key, `Distinct` column, scalar
    /// `Column` leaf) did not resolve against the inherited schema.
    #[error("column resolution failed: {0}")]
    Resolve(#[from] ResolveError),
    /// An `AggItem.func` has no canonical `AggIntent` equivalent — only
    /// `AggFunc::Custom(_)` triggers this today.
    #[error("AggItem `{alias}` uses non-canonical func ({func_dbg}) — no AggIntent equivalent")]
    NoCanonicalIntent { alias: String, func_dbg: String },
    /// A `ScalarExpr::ScalarSubquery` was encountered. See the module doc
    /// for why this is rejected rather than lowered.
    #[error("scalar subqueries are not supported by the canonical IR yet")]
    UnsupportedScalarSubquery,
}

/// Lower a legacy Layer-2 `QueryExpr` tree to the canonical L3 IR.
pub fn convert_root(legacy: &LQueryExpr) -> Result<CQueryExpr, ConvertError> {
    let schema = Binder::new().bind(legacy);
    convert(legacy, &schema)
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
            predicates: Vec::new(),
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

        LQueryExpr::Project { cols, input } => CQueryExpr::Project {
            cols: cols
                .iter()
                .map(|pi| {
                    Ok(CProjectItem {
                        alias: pi.alias.clone(),
                        expr: convert_scalar(&pi.expr, schema)?.0,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?,
            qualifier: None,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Aggregate {
            keys,
            aggs,
            having,
            input,
        } => {
            let by: GroupKeys = resolve_named_keys(keys, schema)?.into();

            // A single-statistic aggregate *fuses* — this is the former
            // `legacy_lower::lower_aggregate` step, done directly in
            // canonical terms rather than via an intermediate legacy L3
            // node. `Custom` (empty `intents`) falls through to the plain
            // multi-agg path below, which raises `NoCanonicalIntent`
            // uniformly for every arity.
            if aggs.len() == 1 && having.is_none() {
                let item = &aggs[0];
                let intents = agg_func_to_intents(&item.func, !keys.is_empty());
                if !intents.is_empty() {
                    let nodes: Vec<CQueryExpr> = match input.as_ref() {
                        LQueryExpr::Window {
                            duration,
                            slide,
                            input: win_input,
                        } => {
                            let kind = if slide.is_some() {
                                CWindowKind::Sliding
                            } else {
                                CWindowKind::Tumbling
                            };
                            let win_child = convert(win_input, schema)?;
                            intents
                                .into_iter()
                                .map(|intent| CQueryExpr::Window {
                                    kind: kind.clone(),
                                    size: *duration,
                                    slide: *slide,
                                    child: Box::new(CQueryExpr::Aggregate {
                                        by: by.clone(),
                                        aggs: vec![intent],
                                        output_names: Vec::new(),
                                        having: None,
                                        child: Box::new(win_child.clone()),
                                    }),
                                })
                                .collect()
                        }
                        other => {
                            let child = convert(other, schema)?;
                            intents
                                .into_iter()
                                .map(|intent| CQueryExpr::Aggregate {
                                    by: by.clone(),
                                    aggs: vec![intent],
                                    output_names: Vec::new(),
                                    having: None,
                                    child: Box::new(child.clone()),
                                })
                                .collect()
                        }
                    };
                    return Ok(if nodes.len() == 1 {
                        nodes.into_iter().next().unwrap()
                    } else {
                        CQueryExpr::Merge { children: nodes }
                    });
                }
            }

            // Plain canonical `Aggregate`: multi-agg, `HAVING`, or `Custom`.
            let mut intents: Vec<AggIntent> = Vec::with_capacity(aggs.len());
            for item in aggs {
                let mapped = agg_func_to_intents(&item.func, !keys.is_empty());
                if mapped.is_empty() {
                    return Err(ConvertError::NoCanonicalIntent {
                        alias: item.alias.clone(),
                        func_dbg: format!("{:?}", item.func),
                    });
                }
                intents.extend(mapped);
            }
            let having = having
                .as_ref()
                .map(|se| convert_scalar(se, schema))
                .transpose()?;
            CQueryExpr::Aggregate {
                by,
                aggs: intents,
                output_names: Vec::new(),
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

        LQueryExpr::Partition { keys, input } => {
            let by: GroupKeys = match keys {
                LPartitionKeys::By(k) => resolve_named_keys(k, schema)?.into(),
                LPartitionKeys::Without(k) => GroupKeys::without(resolve_named_keys(k, schema)?),
            };
            let converted = convert(input, schema)?;
            fold_partition_keys(converted, by)
        }

        LQueryExpr::Distinct { cols, input } => CQueryExpr::Distinct {
            cols: resolve_column_refs(cols, schema)?,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::TopK { k, by, input } => {
            // γ4 classification default: heavy-hitter intent. The generic
            // `Sort + Limit` case never reaches a legacy `TopK` node —
            // see `topk_bridge` module doc.
            let by: GroupKeys = resolve_named_keys(by, schema)?.into();
            CQueryExpr::Aggregate {
                by,
                aggs: vec![AggIntent::TopK {
                    k: *k as usize,
                    accuracy: AccuracyTarget::Epsilon(0.05),
                }],
                output_names: Vec::new(),
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
            pred: match pred {
                Some(se) => convert_scalar(se, schema)?,
                // Canonical `Join` requires a predicate; a legacy `None`
                // pred is a CROSS JOIN — model it as the tautology `true`.
                None => Predicate(L3Expr::Literal(L3Scalar::Boolean(true))),
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
            keys: keys.clone(),
            partition_by: GroupKeys::none(),
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
            op: op.clone(),
            lhs: Box::new(convert(lhs, schema)?),
            rhs: Box::new(convert(rhs, schema)?),
            vector_match: vector_match.clone(),
        },
    })
}

/// Fold `by` into the nearest `Aggregate` inside `qe` — the replacement
/// for wrapping in a `Partition` node, which `asap_ir`'s `QueryExpr`
/// doesn't have. Handles the shapes the parsers actually produce: a bare
/// `Aggregate`, a `Window` wrapping one, or a `Merge` of siblings (the
/// `StdDev`/`Variance` fan-out) — folding into every sibling so they stay
/// consistent. Falls through unchanged (with a debug assertion) for any
/// other shape; nothing in the current parsers produces a standalone
/// `LQueryExpr::Partition` over a non-aggregate subtree.
fn fold_partition_keys(qe: CQueryExpr, by: GroupKeys) -> CQueryExpr {
    match qe {
        CQueryExpr::Aggregate {
            aggs,
            output_names,
            having,
            child,
            ..
        } => CQueryExpr::Aggregate {
            by,
            aggs,
            output_names,
            having,
            child,
        },
        CQueryExpr::Window {
            kind,
            size,
            slide,
            child,
        } => CQueryExpr::Window {
            kind,
            size,
            slide,
            child: Box::new(fold_partition_keys(*child, by)),
        },
        CQueryExpr::Merge { children } => CQueryExpr::Merge {
            children: children
                .into_iter()
                .map(|c| fold_partition_keys(c, by.clone()))
                .collect(),
        },
        other => {
            debug_assert!(
                false,
                "Partition over a non-aggregate shape has no GroupKeys home: {other:?}"
            );
            other
        }
    }
}

/// Translate a legacy `ScalarExpr` to a canonical `Predicate`, recursing
/// through every composite variant.
pub fn convert_scalar(se: &LScalarExpr, schema: &Schema) -> Result<Predicate, ConvertError> {
    match se {
        LScalarExpr::ScalarSubquery(_) => Err(ConvertError::UnsupportedScalarSubquery),
        LScalarExpr::BinaryOp { op, lhs, rhs } => {
            let l = convert_scalar(lhs, schema)?.0;
            let r = convert_scalar(rhs, schema)?.0;
            Ok(binary_scalar_op(op, l, r))
        }
        LScalarExpr::IsNull { expr, negated } => {
            let inner = Box::new(convert_scalar(expr, schema)?.0);
            Ok(Predicate(if *negated {
                L3Expr::IsNotNull(inner)
            } else {
                L3Expr::IsNull(inner)
            }))
        }
        LScalarExpr::FunctionCall { name, args } => {
            let exprs = args
                .iter()
                .map(|a| Ok(convert_scalar(a, schema)?.0))
                .collect::<Result<Vec<_>, ConvertError>>()?;
            Ok(Predicate(L3Expr::FunctionCall {
                name: name.clone(),
                args: exprs,
            }))
        }
        LScalarExpr::InList {
            expr,
            list,
            negated,
        } => {
            let e = convert_scalar(expr, schema)?.0;
            let list_exprs = list
                .iter()
                .map(|a| Ok(convert_scalar(a, schema)?.0))
                .collect::<Result<Vec<_>, ConvertError>>()?;
            Ok(Predicate(L3Expr::InList {
                expr: Box::new(e),
                list: list_exprs,
                negated: *negated,
            }))
        }
        LScalarExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = convert_scalar(expr, schema)?.0;
            let l = convert_scalar(low, schema)?.0;
            let h = convert_scalar(high, schema)?.0;
            Ok(Predicate(between(e, l, h, *negated)))
        }
        LScalarExpr::Column(name) => {
            let id = resolve_column_ref(&LColumnRef::Named(name.clone()), schema)?;
            Ok(Predicate(L3Expr::Column(id)))
        }
        LScalarExpr::Literal(lit) => Ok(Predicate(literal_from_legacy(lit))),
    }
}

/// Translate a legacy `ScalarExpr::BinaryOp`'s operator + converted
/// operands into the corresponding `L3Expr`. `op` is already the
/// canonical `BinaryOpKind` (`relational::BinaryOpKind` re-exports the
/// same type `query_expr::BinaryOp.op` carries — no separate flat legacy
/// enum exists), so this is a structural unwrap, not a value mapping.
fn binary_scalar_op(op: &BinaryOpKind, l: L3Expr, r: L3Expr) -> Predicate {
    let e = match op {
        BinaryOpKind::Arith(arith_op) => L3Expr::Arith {
            op: arith_op.clone(),
            left: Box::new(l),
            right: Box::new(r),
        },
        BinaryOpKind::Compare(cmp_op) => L3Expr::Compare {
            left: Box::new(l),
            op: cmp_op.clone(),
            right: Box::new(r),
        },
        BinaryOpKind::And => L3Expr::BoolAnd(vec![l, r]),
        BinaryOpKind::Or => L3Expr::BoolOr(vec![l, r]),
        // `Unless` / `Pow` / `Atan2` are PromQL vector-set / power ops with
        // no scalar-predicate counterpart; neither parser constructs a
        // `ScalarExpr::BinaryOp` with one of these (they only appear on
        // `QueryExpr::BinaryOp`, passed straight through in `convert`).
        // Defensive fallback rather than a panic on unreachable input.
        BinaryOpKind::Unless | BinaryOpKind::Pow | BinaryOpKind::Atan2 => {
            L3Expr::Literal(L3Scalar::Boolean(true))
        }
    };
    Predicate(e)
}

fn literal_from_legacy(lit: &crate::intent_algebra::relational::LiteralValue) -> L3Expr {
    use crate::intent_algebra::relational::LiteralValue as L;
    L3Expr::Literal(match lit {
        L::Null => L3Scalar::Null,
        L::Bool(b) => L3Scalar::Boolean(*b),
        L::Int(i) => L3Scalar::Int64(*i),
        L::Float(f) => L3Scalar::Float64(*f),
        L::Str(s) => L3Scalar::Utf8(s.clone()),
        // No L3Scalar counterpart -- fold to nanosecond count, matching
        // the pre-merge `from_legacy_scalar`'s documented behavior.
        L::Duration(d) => L3Scalar::Int64(d.as_nanos() as i64),
    })
}

// ── AggFunc → AggIntent sketch mapping ───────────────────────────────────────

/// Map an [`AggFunc`] to the canonical [`AggIntent`]s the `convert`
/// `Aggregate` arm fuses on. `grouped` is `!keys.is_empty()` at the call
/// site. Empty for non-canonical functions (`Custom` only); one intent
/// for every ordinary function (delegates to [`AggFunc::to_sketch_op`]);
/// two for the `StdDev` / `Variance` fan-out (the caller wraps the pair
/// in a `Merge` of sibling sketch aggregates, the one case `to_sketch_op`
/// can't express since it returns a single `Option<AggIntent>`).
fn agg_func_to_intents(func: &AggFunc, grouped: bool) -> Vec<AggIntent> {
    match func {
        AggFunc::StdDev { .. } | AggFunc::Variance { .. } => vec![
            AggIntent::Quantile {
                col: None,
                q: 0.25,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Quantile {
                col: None,
                q: 0.75,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
        ],
        // `Rate` / `Increase` / `Delta` map onto `AggIntent::Sum`, not
        // the dedicated `AggIntent::Rate` / `Increase` / `Delta`
        // variants `to_sketch_op()` would otherwise reach for — the
        // `asap_tier_analysis` engine dispatch deliberately collapses
        // all of `rate` / `irate` / `increase` / `sum_over_time` onto
        // one `Capability::ExactAgg(Sum)` and disambiguates via the
        // separate typed `outer_fn` field instead (see
        // `asap_tier_analysis::tests::rate_and_sum_over_time_share_
        // capability_but_differ_on_outer_fn` and the surrounding
        // "outer_fn — rate vs plain disambiguation" test block, which
        // documents this as the deliberate replacement for a retired
        // raw-PromQL re-parser). Using the dedicated intents here would
        // fragment that dispatch.
        AggFunc::Rate | AggFunc::Increase | AggFunc::Delta => vec![AggIntent::Sum { col: None }],
        // `Avg` approximates as the p50 (median) quantile sketch rather
        // than `to_sketch_op()`'s literal `AggIntent::Avg` (exact,
        // non-mergeable) — matches `Min`/`Max`'s boundary-quantile
        // treatment (`AggIntent::Min` ~ q=0.0, `Max` ~ q=1.0) and is what
        // `query_parser::QeCollector::collect_op` (which has no `Avg`
        // arm of its own) relies on to classify `avg_over_time` as
        // `AggType::Quantile` with `quantiles: [0.5]`.
        AggFunc::Avg => vec![AggIntent::Quantile {
            col: None,
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        // A *grouped* `Count` is `count_over_time(...) by (...)` (or the
        // PromQL `topk` bridge's synthetic `Count` — see
        // `query_parser::promql::build_windowed_agg`'s "Count-with-
        // GROUP-BY → Frequency" comment) — structurally per-series and
        // sketchable, so it takes the same `Frequency`/CMS path as
        // `AggFunc::Frequency`/`HeavyHitters`. An *ungrouped* `Count` is
        // the SQL `COUNT(*)` exact-row-count case and keeps
        // `to_sketch_op()`'s literal `AggIntent::Count{Exact}`.
        AggFunc::Count if grouped => vec![crate::intent_algebra::default_frequency()],
        other => other.to_sketch_op().into_iter().collect(),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::relational::{
        AggItem, ColumnRef as LColumnRef, ProjectItem as LProjectItem, SourceSpec,
    };
    use std::time::Duration;

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
                predicates,
                schema,
            } => {
                assert!(
                    matches!(source, Source::TimeSeries { metric } if metric == "http_requests_total")
                );
                assert!(predicates.is_empty());
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
                        assert!(matches!(aggs.as_slice(), [AggIntent::Sum { col: None }]));
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
    fn single_aggregate_folds_to_canonical_aggregate() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![agg_item("s", AggFunc::Sum)],
            having: None,
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, aggs, .. } => {
                assert!(by.is_empty(), "no GROUP BY → empty `by`: {by:?}");
                assert!(matches!(aggs.as_slice(), [AggIntent::Sum { col: None }]));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn ungrouped_count_is_exact() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![agg_item("n", AggFunc::Count)],
            having: None,
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { aggs, .. } => assert!(matches!(
                aggs.as_slice(),
                [AggIntent::Count {
                    accuracy: AccuracyTarget::Exact
                }]
            )),
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_target_column_is_not_a_group_by_key() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![AggItem {
                alias: "s".into(),
                func: AggFunc::Sum,
                col: LColumnRef::Named("price".into()),
                distinct: false,
            }],
            having: None,
            input: Box::new(src("trades")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, .. } => assert!(by.is_empty()),
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn single_aggregate_over_window_folds_to_window_over_aggregate() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![agg_item("q", AggFunc::Quantile(0.99))],
            having: None,
            input: Box::new(LQueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(src("m")),
            }),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Window { kind, child, .. } => {
                assert_eq!(kind, CWindowKind::Tumbling);
                assert!(matches!(
                    *child,
                    CQueryExpr::Aggregate { ref by, ref aggs, .. }
                        if by.is_empty()
                            && matches!(aggs.as_slice(), [AggIntent::Quantile { .. }])
                ));
            }
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn stddev_fans_out_into_merge_of_quantile_siblings() {
        let legacy = LQueryExpr::Aggregate {
            keys: vec![],
            aggs: vec![agg_item("sd", AggFunc::StdDev { population: false })],
            having: None,
            input: Box::new(src("m")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Merge { children } => {
                assert_eq!(children.len(), 2);
                for c in &children {
                    assert!(matches!(
                        c,
                        CQueryExpr::Aggregate {
                            aggs,
                            ..
                        } if matches!(aggs.as_slice(), [AggIntent::Quantile { .. }])
                    ));
                }
            }
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn topk_folds_into_aggregate_with_topk_intent() {
        let legacy = LQueryExpr::TopK {
            k: 5,
            by: vec![].into(),
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
    fn filter_pred_with_scalar_subquery_is_rejected() {
        // ScalarSubquery has no canonical L3Expr representation (see the
        // module doc) -- convert_scalar rejects it rather than guessing.
        let legacy = LQueryExpr::Filter {
            pred: LScalarExpr::ScalarSubquery(Box::new(LQueryExpr::Ref("cte".into()))),
            input: Box::new(src("m")),
        };
        assert!(matches!(
            convert_root(&legacy).unwrap_err(),
            ConvertError::UnsupportedScalarSubquery
        ));
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
                assert!(matches!(cols[0].expr, L3Expr::Column(_)));
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn binary_op_converts_both_sides() {
        let legacy = LQueryExpr::BinaryOp {
            op: BinaryOpKind::Arith(ArithOp::Add),
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
    fn scalar_binary_op_translates_arith_and_compare() {
        let legacy = LScalarExpr::BinaryOp {
            op: BinaryOpKind::Compare(CompareOp::Gt),
            lhs: Box::new(LScalarExpr::Column("value".into())),
            rhs: Box::new(LScalarExpr::Literal(
                crate::intent_algebra::relational::LiteralValue::Float(1.0),
            )),
        };
        let schema = crate::intent_algebra::column_resolution::infer_source_schema("m");
        let pred = convert_scalar(&legacy, &schema).unwrap();
        assert!(matches!(
            pred.0,
            L3Expr::Compare {
                op: CompareOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn cross_join_none_pred_becomes_true_literal() {
        let legacy = LQueryExpr::Join {
            kind: crate::intent_algebra::relational::JoinKind::Cross,
            pred: None,
            left: Box::new(src("a")),
            right: Box::new(src("b")),
        };
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Join { pred, .. } => {
                assert!(matches!(pred.0, L3Expr::Literal(L3Scalar::Boolean(true))));
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
                        crate::intent_algebra::relational::LiteralValue::Bool(true),
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
