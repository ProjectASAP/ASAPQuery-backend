//! The Layer-2 → canonical L3 IR converter.
//!
//! Recursively converts a *whole* `relational::QueryExpr` tree (the raw
//! Layer-2 relational IR the `query_parser` front ends emit) into a
//! *whole* canonical `query_expr::QueryExpr` tree. This is the single
//! entry the parse path routes through — [`convert_root`].
//!
//! ## Phase 2 step 4 (docs/migration-plan-backend-plan.md)
//!
//! Now that `relational.rs` (L2) itself merged onto `asap_l2` (see that
//! file's module doc), this converter is control_plane's *own* — not a
//! re-export of `asap_l2::lower::convert_root` — because the
//! `Aggregate` arm's multi-agg fusion and `frequency_trigger` heuristic
//! (see the `Frequency` preservation section below) are control_plane's
//! own dispatch design, with no `asap_l2` equivalent (`asap_l2` always
//! threads a single workload-level `AccuracyTarget` through
//! `agg_func_to_intent` with no grouped/windowed heuristic of its own).
//! Every other *structural* piece below (scalar resolution, schema
//! threading, the `GroupKeys` shape, and now also the full `AggFunc`→
//! `AggIntent` mapping table itself) is unchanged from `asap_l2`'s own
//! converter — `avg_over_time` used to be the one deliberate mapping
//! divergence (a p50 quantile-sketch approximation in place of
//! `asap_l2`'s literal, exact `AggIntent::Avg`) but that's gone too:
//! `AggFunc::Avg` now maps onto the literal `AggIntent::Avg` exactly
//! like `asap_l2::lower` does, since `capability_for(&AggIntent::Avg)`
//! already correctly returns `None` (no ASAP-tier sketch substitute —
//! `avg` needs a cross-policy Sum+Count join, tracked as a follow-up)
//! and ASAPController's own `crates/plan/src/bind.rs` treats
//! `AggIntent::Avg` the same way (`pass_through_intents_stay_logical`
//! keeps it a whole logical, unsketched subtree). `avg` now routes
//! through the same exact/archive path as `Sum`/`Count`/every
//! archive-only intent — `QeCollector::collect_op`'s existing catch-all
//! `exact_required = true` arm already handles it with no dedicated
//! `Avg` arm needed.
//!
//! **PromQL-frontend semantic-retarget step (topk/rate precision fix).**
//! `Rate`/`Increase`/`Changes`/`Delta`/`IDelta`/`Deriv`/`PredictLinear`/
//! `DoubleExpSmoothing`/`Resets` used to all collapse onto
//! `AggIntent::Sum` or `Count` here — a crude placeholder bucketing
//! predating `asap_l2`'s own per-function `AggIntent` vocabulary. They
//! now map onto their own dedicated intents, matching `asap_l2`'s
//! mapping exactly (this repo no longer diverges from it for these).
//! `Rate`/`Increase` activate a real, previously-dormant
//! `capability_for` arm (`Rate | Increase => ExactAgg(Increase)`) for
//! the first time via the PromQL path; the rest are archive-only
//! either way, so the fix is `capability_for` now correctly returning
//! `None` instead of a wrong `Some(...)` for functions the ASAP tier
//! was never actually able to answer that way. `asap_tier_analysis`'s
//! `outer_fn` field is unaffected by any of this — it's computed
//! independently off the raw PromQL function name, not the lowered
//! `AggIntent`.
//!
//! `query_parser::promql`'s `topk`/`bottomk` handling was the other half
//! of this step: it used to force every `topk(...)` argument into a
//! `Count`-shaped inner regardless of what was actually being ranked
//! (silently wrong for `topk(k, avg_over_time(...))`). It now only takes
//! the heavy-hitter `TopK` path when ranking descending by
//! `count_over_time(...)` specifically (`RankingMeasure::Frequency`,
//! the one realised heavy-hitter measure) — everything else, including
//! every `bottomk`, becomes a generic `Sort + Limit`, matching
//! ASAPController's `frontend-promql` design.
//!
//! Two consequences of adopting `asap_l2::relational::QueryExpr`:
//!
//! - **No more hand-rolled scalar conversion.** `ScalarExpr` is gone —
//!   every scalar position carries the shared `L2Expr` directly (same
//!   generic `Expr<C>` the canonical tree's `L3Expr` is), so
//!   `column_resolution::resolve_expr` does the whole
//!   name→position resolution in one generic pass. `between()`,
//!   `binary_scalar_op`, and `literal_from_legacy` (all present before
//!   this step) are gone with it — nothing left for them to do.
//! - **No more `Partition`, at L2 or L3.** `Aggregate` carries
//!   `without: bool` directly (`asap_l2`'s own step-3-equivalent design
//!   choice) — `fold_partition_keys` and the standalone
//!   `LQueryExpr::Partition` arm (both present before this step) are
//!   gone with it.
//!
//! `ScalarSubquery` no longer exists as a concept at all — `asap_l2`'s
//! `relational::QueryExpr` has no such variant (its `Ref`/`LetBinding`
//! are "Reserved: no front end emits yet", same as this repo's own
//! pre-merge state) — so `ConvertError::UnsupportedScalarSubquery` (this
//! step's predecessor) has nothing left to reject; removed.
//!
//! ## `Frequency` preservation (see `relational.rs`'s module doc)
//!
//! `count_over_time(...)` (and the PromQL `topk` bridge's synthetic
//! inner count) must still route through a CMS/CountSketch-family
//! `AggIntent::Extension` rather than an exact `AggIntent::Count`, but
//! `AggFunc::Frequency` no longer exists as a distinct variant to key
//! off of — the frontend now constructs plain `AggFunc::Count` for both
//! cases (matching `asap_l2`'s own frontend-promql, which leaves the
//! sketch-vs-exact choice to L4). The trigger `agg_func_to_intents` uses
//! instead: **`Count` is a `Frequency` candidate when its `Aggregate` is
//! grouped (`by`/`without` non-trivial) OR its input is an `_over_time`
//! `Window`** — precisely the two shapes `query_parser::promql`
//! constructs a windowed `Count` from (the topk bridge is grouped by the
//! topk's own `by` keys; a bare `count_over_time(...)` is windowed but
//! typically ungrouped). An un-windowed, ungrouped `Count` (SQL
//! `COUNT(*)`, not in scope for this PromQL-only step) stays exact.
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

use asap_ir::intent_algebra::query_expr::InfoMatcher;
use asap_ir::intent_algebra::BindingName;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::binder::Binder;
use crate::intent_algebra::column_resolution::{
    resolve_column_refs, resolve_expr, resolve_group_keys_promql, ResolveError,
};
use crate::intent_algebra::query_expr::{
    GroupKeys, L3Scalar, Predicate, ProjectItem as CProjectItem, QueryExpr as CQueryExpr,
    QueryExprError, SortKey as CSortKey, Source, WindowKind as CWindowKind,
};
use crate::intent_algebra::relational::{AggFunc, QueryExpr as LQueryExpr};
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
    /// An `AggItem.func` has no canonical `AggIntent` equivalent. No
    /// `AggFunc` variant reaches this today (every one maps to
    /// something) — kept as the escape hatch's shape for a future
    /// extension point, matching `asap_l2`'s own error surface.
    #[error("AggItem `{alias:?}` uses non-canonical func ({func_dbg}) — no AggIntent equivalent")]
    NoCanonicalIntent {
        alias: Option<String>,
        func_dbg: String,
    },
    /// Schema derivation over a converted subtree failed (surfaced by
    /// `column_resolution::output_schema_for_aggregate` callers, not by
    /// `convert` itself today — kept for API-shape parity with
    /// `asap_l2::lower::ConvertError`).
    #[error("schema derivation failed: {0}")]
    Schema(#[from] QueryExprError),
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
        LQueryExpr::Source(spec) => {
            let scan = match &spec.schema {
                Some(sql_schema) => CQueryExpr::Scan {
                    source: Source::Table {
                        table_ref: spec.name.clone(),
                    },
                    predicates: Vec::new(),
                    schema: sql_schema.clone(),
                },
                None => CQueryExpr::Scan {
                    source: Source::TimeSeries {
                        metric: spec.name.clone(),
                    },
                    predicates: Vec::new(),
                    // Carry the Binder's complete schema — the same
                    // self-contained scope every `ColumnId` in this tree
                    // resolves against.
                    schema: schema.clone(),
                },
            };
            if spec.shift.is_identity() {
                scan
            } else {
                CQueryExpr::TimeShift {
                    shift: spec.shift,
                    child: Box::new(scan),
                }
            }
        }

        LQueryExpr::Scalar(v) => CQueryExpr::Scalar(*v),
        LQueryExpr::EvalTime => CQueryExpr::EvalTime,
        LQueryExpr::VectorFromScalar(input) => {
            CQueryExpr::VectorFromScalar(Box::new(convert(input, schema)?))
        }
        LQueryExpr::ScalarFromVector(input) => {
            CQueryExpr::ScalarFromVector(Box::new(convert(input, schema)?))
        }
        LQueryExpr::Relabel { dst, value, input } => CQueryExpr::Relabel {
            dst: dst.clone(),
            value: resolve_expr(value, schema)?,
            child: Box::new(convert(input, schema)?),
        },
        LQueryExpr::Sample { keys, kind, input } => CQueryExpr::Sample {
            by: resolve_column_refs(keys, schema)?.into(),
            kind: *kind,
            child: Box::new(convert(input, schema)?),
        },
        LQueryExpr::InfoJoin { selector, input } => CQueryExpr::InfoJoin {
            selector: selector.clone(),
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Ref(name) => CQueryExpr::Ref {
            name: BindingName::new(name.clone()),
        },

        LQueryExpr::Filter { pred, input } => CQueryExpr::Filter {
            pred: Predicate(resolve_expr(pred, schema)?),
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Project {
            cols,
            qualifier,
            input,
        } => CQueryExpr::Project {
            cols: cols
                .iter()
                .map(|item| {
                    Ok(CProjectItem {
                        alias: item.alias.clone(),
                        expr: resolve_expr(&item.expr, schema)?,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?,
            qualifier: qualifier.clone(),
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::Aggregate {
            keys,
            without,
            aggs,
            having,
            input,
        } => {
            let by: GroupKeys = if *without {
                GroupKeys::without(resolve_column_refs(keys, schema)?)
            } else {
                resolve_group_keys_promql(keys, schema)?.into()
            };
            // See the module doc's "Frequency preservation" section — a
            // grouped-or-windowed `Count` is a `Frequency` candidate.
            let windowed = matches!(input.as_ref(), LQueryExpr::Window { .. });
            let frequency_trigger = !by.is_empty() || windowed;

            // A single-statistic aggregate *fuses* — done directly in
            // canonical terms rather than via an intermediate legacy L3
            // node. Empty `intents` is structurally unreachable (every
            // `AggFunc` maps to something), so it always falls through
            // to the plain path below rather than being special-cased
            // inline.
            if aggs.len() == 1 && having.is_none() {
                let item = &aggs[0];
                let intents = agg_func_to_intents(&item.func, frequency_trigger);
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

            // Plain canonical `Aggregate`: multi-agg, `HAVING`, or a
            // (structurally unreachable) unmapped `AggFunc`.
            let mut intents: Vec<AggIntent> = Vec::with_capacity(aggs.len());
            for item in aggs {
                let mapped = agg_func_to_intents(&item.func, frequency_trigger);
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
                .map(|h| resolve_expr(h, schema).map(Predicate))
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

        LQueryExpr::Distinct { cols, input } => CQueryExpr::Distinct {
            cols: resolve_column_refs(cols, schema)?,
            child: Box::new(convert(input, schema)?),
        },

        LQueryExpr::TopK { k, by, input } => {
            // γ4 classification default: heavy-hitter intent. The generic
            // `Sort + Limit` case never reaches a legacy `TopK` node —
            // see `topk_bridge` module doc.
            let by: GroupKeys = resolve_group_keys_promql(by, schema)?.into();
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
                Some(p) => Predicate(resolve_expr(p, schema)?),
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

        LQueryExpr::Sort {
            keys,
            partition_by,
            input,
        } => CQueryExpr::Sort {
            keys: keys
                .iter()
                .map(|k| {
                    Ok(CSortKey {
                        expr: resolve_expr(&k.expr, schema)?,
                        ascending: k.ascending,
                        nulls_first: k.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?,
            partition_by: resolve_column_refs(partition_by, schema)?.into(),
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

        LQueryExpr::WindowFunc {
            func,
            args,
            partition_by,
            order_by,
            output_name,
            input,
        } => CQueryExpr::WindowFunc {
            func: func.clone(),
            args: args
                .iter()
                .map(|a| resolve_expr(a, schema))
                .collect::<Result<Vec<_>, ResolveError>>()?,
            partition_by: resolve_column_refs(partition_by, schema)?.into(),
            order_by: order_by
                .iter()
                .map(|k| {
                    Ok(CSortKey {
                        expr: resolve_expr(&k.expr, schema)?,
                        ascending: k.ascending,
                        nulls_first: k.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, ConvertError>>()?,
            output_name: output_name.clone(),
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

// ── AggFunc → AggIntent sketch mapping ───────────────────────────────────────

/// Map an [`AggFunc`] to the canonical [`AggIntent`]s the `convert`
/// `Aggregate` arm fuses on. `frequency_trigger` is `true` when the
/// enclosing `Aggregate` is grouped or windowed — see the module doc's
/// "Frequency preservation" section; it only affects the `Count` arm.
/// One intent for every function today — the former `StdDev`/`Variance`
/// two-quantile IQR-proxy fan-out is gone (see the `Avg`/`StdDev`/
/// `Variance` comment below), so the `Merge`-of-siblings path this
/// `Vec` return type still supports is currently unexercised.
fn agg_func_to_intents(func: &AggFunc, frequency_trigger: bool) -> Vec<AggIntent> {
    let q = |q: f64| AggIntent::Quantile {
        col: None,
        q,
        accuracy: AccuracyTarget::Epsilon(0.01),
    };
    match func {
        AggFunc::Count if frequency_trigger => vec![crate::intent_algebra::default_frequency()],
        AggFunc::Count => vec![AggIntent::Count {
            accuracy: AccuracyTarget::Exact,
        }],
        AggFunc::Sum => vec![AggIntent::Sum { col: None }],
        // `Avg` maps onto the literal, exact, non-mergeable
        // `AggIntent::Avg` — matching `asap_l2::lower`'s own mapping
        // exactly. `capability_for(&AggIntent::Avg)` already returns
        // `None` (no ASAP-tier sketch substitute; needs a cross-policy
        // Sum+Count join, tracked as a follow-up), matching
        // ASAPController's own stance: `crates/plan/src/bind.rs`'s
        // `pass_through_intents_stay_logical` test keeps `AggIntent::Avg`
        // as a whole logical subtree with no sketch binding. So `avg`
        // routes through the exact/archive path, same as `Sum`/`Count`/
        // `TopK`/every archive-only intent — `QeCollector::collect_op`
        // already handles this correctly via its catch-all
        // `exact_required = true` arm, no dedicated `Avg` arm needed.
        AggFunc::Avg => vec![AggIntent::Avg { col: None }],
        AggFunc::Min => vec![AggIntent::Min { col: None }],
        AggFunc::Max => vec![AggIntent::Max { col: None }],
        // `StdDev`/`Variance` map onto their literal `AggIntent`s,
        // matching `asap_l2::lower` exactly — same fix as `Avg` above,
        // same reasoning: `capability_for` already declares both
        // archive-only (`Avg { .. } | StdDev { .. } | Variance { .. }
        // => None`), so the former `vec![q(0.25), q(0.75)]` IQR proxy
        // (interquartile range as a stand-in for stddev) was silently
        // claiming ASAP-tier `QuantileApprox` support neither this
        // repo's own capability table nor ASAPController's `asap-plan`
        // (`pass_through_intents_stay_logical`) actually backs with a
        // real bind rule.
        AggFunc::StdDev { population } => vec![AggIntent::StdDev {
            col: None,
            population: *population,
        }],
        AggFunc::Variance { population } => vec![AggIntent::Variance {
            col: None,
            population: *population,
        }],
        AggFunc::Quantile(phi) => vec![q(*phi)],
        AggFunc::CountDistinct => vec![crate::intent_algebra::default_cardinality()],
        AggFunc::HeavyHitters { .. } => vec![crate::intent_algebra::default_frequency()],
        // `Rate` / `Increase` now map onto their own dedicated
        // `AggIntent`s (PromQL-frontend semantic-retarget step) --
        // `capability_for` already has a real, tested
        // `Rate | Increase => ExactAgg(Increase)` arm (`sketch_algebra::
        // capability`); this activates it via the PromQL path for the
        // first time. `asap_tier_analysis`'s `outer_fn` field is
        // unaffected -- it's computed independently, straight off the
        // raw PromQL function name (`trace_from_promql`'s
        // `set_counter_fn`), not off the lowered `AggIntent`, so it
        // still tells the engine's reducer *how* to interpret the
        // accumulated value (rate needs a divide-by-range step, increase
        // doesn't) regardless of what capability got matched.
        AggFunc::Rate { .. } => vec![AggIntent::Rate],
        AggFunc::Increase { .. } => vec![AggIntent::Increase],
        // `Changes` / `Delta` / `IDelta` / `Deriv` / `PredictLinear` /
        // `DoubleExpSmoothing` / `Resets` likewise now map onto their own
        // dedicated `AggIntent`s instead of collapsing onto `Count`/`Sum`
        // -- all are archive-only (`agg_intent::archive_only`; no
        // `Bind*` rule exists for any of them, same as before this fix),
        // so the real effect is `capability_for` now correctly returning
        // `None` (route to archive) instead of the wrong
        // `Some(ExactAgg(Sum))` / `Some(CardinalityApprox)` the old
        // Sum/Count collapse produced -- these functions were never
        // actually answerable from the ASAP tier that way.
        AggFunc::Changes => vec![AggIntent::Changes],
        AggFunc::Resets => vec![AggIntent::Resets],
        AggFunc::Delta => vec![AggIntent::Delta],
        AggFunc::IDelta => vec![AggIntent::IDelta],
        AggFunc::Deriv => vec![AggIntent::Deriv],
        AggFunc::PredictLinear { seconds } => vec![AggIntent::PredictLinear { seconds: *seconds }],
        AggFunc::DoubleExpSmoothing { smoothing, trend } => {
            vec![AggIntent::DoubleExpSmoothing {
                smoothing: *smoothing,
                trend: *trend,
            }]
        }
        // Every remaining `AggFunc` (native-histogram accessors,
        // math/trig, time/calendar, `Group`/`CountValues`, the extended
        // range-vector reducers) has no pre-`asap_l2`-merge equivalent
        // in this repo's PromQL surface at all -- promql.rs doesn't
        // construct any of them today (`walk_call_to_op`'s exhaustive
        // function-name table has no arm reaching them), so there's no
        // existing behavior to preserve. Map each directly onto its
        // like-named `AggIntent` (all archive-only per
        // `agg_intent::archive_only`, so this is inert until a real
        // caller constructs one). `Absent` / `AbsentOverTime` /
        // `PresentOverTime` / `LastOverTime` *are* constructed by
        // promql.rs today (`absent_over_time` / `present_over_time` /
        // `last_over_time`) -- listed here rather than above only
        // because they were already correctly mapped before this fix
        // (never went through the Sum collapse).
        AggFunc::HistogramCount => vec![AggIntent::HistogramCount],
        AggFunc::HistogramSum => vec![AggIntent::HistogramSum],
        AggFunc::HistogramAvg => vec![AggIntent::HistogramAvg],
        AggFunc::HistogramStdDev => vec![AggIntent::HistogramStdDev],
        AggFunc::HistogramStdVar => vec![AggIntent::HistogramStdVar],
        AggFunc::HistogramFraction { lower, upper } => vec![AggIntent::HistogramFraction {
            lower: *lower,
            upper: *upper,
        }],
        AggFunc::HistogramQuantile(phi) => vec![AggIntent::HistogramQuantile { q: *phi }],
        AggFunc::Math(f) => vec![AggIntent::Math(f.clone())],
        AggFunc::Absent => vec![AggIntent::Absent],
        AggFunc::AbsentOverTime => vec![AggIntent::AbsentOverTime],
        AggFunc::PresentOverTime => vec![AggIntent::PresentOverTime],
        AggFunc::TimeFn(f) => vec![AggIntent::TimeFn(f.clone())],
        AggFunc::Group => vec![AggIntent::Group],
        AggFunc::CountValues { label } => vec![AggIntent::CountValues {
            label: label.clone(),
        }],
        AggFunc::LastOverTime => vec![AggIntent::LastOverTime],
        AggFunc::FirstOverTime => vec![AggIntent::FirstOverTime],
        AggFunc::MadOverTime => vec![AggIntent::MadOverTime],
        AggFunc::TsOfMinOverTime => vec![AggIntent::TsOfMinOverTime],
        AggFunc::TsOfMaxOverTime => vec![AggIntent::TsOfMaxOverTime],
        AggFunc::TsOfFirstOverTime => vec![AggIntent::TsOfFirstOverTime],
        AggFunc::TsOfLastOverTime => vec![AggIntent::TsOfLastOverTime],
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::relational::{AggItem, ColumnRef as LColumnRef, SourceSpec};
    use std::time::Duration;

    fn src(name: &str) -> LQueryExpr {
        LQueryExpr::Source(SourceSpec::new(name))
    }

    fn agg_item(alias: &str, func: AggFunc) -> AggItem {
        AggItem {
            alias: Some(alias.into()),
            func,
            col: LColumnRef::SampleValue,
        }
    }

    fn agg(
        keys: Vec<LColumnRef>,
        without: bool,
        aggs: Vec<AggItem>,
        input: LQueryExpr,
    ) -> LQueryExpr {
        LQueryExpr::Aggregate {
            keys,
            without,
            aggs,
            having: None,
            input: Box::new(input),
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
        let legacy = LQueryExpr::Window {
            duration: Duration::from_secs(300),
            slide: None,
            input: Box::new(agg(
                vec![],
                false,
                vec![agg_item("s", AggFunc::Sum)],
                src("m"),
            )),
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
    fn single_aggregate_folds_to_canonical_aggregate() {
        let legacy = agg(vec![], false, vec![agg_item("s", AggFunc::Sum)], src("m"));
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, aggs, .. } => {
                assert!(by.is_empty(), "no GROUP BY → empty `by`: {by:?}");
                assert!(matches!(aggs.as_slice(), [AggIntent::Sum { col: None }]));
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn ungrouped_unwindowed_count_is_exact() {
        let legacy = agg(vec![], false, vec![agg_item("n", AggFunc::Count)], src("m"));
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
    fn windowed_count_is_frequency() {
        // Mirrors `count_over_time(m[5m])`: no GROUP BY, but windowed.
        let legacy = agg(
            vec![],
            false,
            vec![agg_item("n", AggFunc::Count)],
            LQueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(src("m")),
            },
        );
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Window { child, .. } => match *child {
                CQueryExpr::Aggregate { aggs, .. } => {
                    assert!(matches!(aggs.as_slice(), [AggIntent::Extension { .. }]));
                    assert!(crate::intent_algebra::as_frequency(&aggs[0]).is_some());
                }
                other => panic!("expected Aggregate, got {other:?}"),
            },
            other => panic!("expected Window, got {other:?}"),
        }
    }

    #[test]
    fn grouped_unwindowed_count_is_frequency() {
        // Mirrors the PromQL `topk` bridge's synthetic grouped Count.
        let legacy = agg(
            vec![LColumnRef::Named("symbol".into())],
            false,
            vec![agg_item("n", AggFunc::Count)],
            src("m"),
        );
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { aggs, .. } => {
                assert!(crate::intent_algebra::as_frequency(&aggs[0]).is_some());
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_target_column_is_not_a_group_by_key() {
        let legacy = agg(
            vec![],
            false,
            vec![AggItem {
                alias: Some("s".into()),
                func: AggFunc::Sum,
                col: LColumnRef::Named("price".into()),
            }],
            src("trades"),
        );
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { by, .. } => assert!(by.is_empty()),
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn single_aggregate_over_window_folds_to_window_over_aggregate() {
        let legacy = agg(
            vec![],
            false,
            vec![agg_item("q", AggFunc::Quantile(0.99))],
            LQueryExpr::Window {
                duration: Duration::from_secs(300),
                slide: None,
                input: Box::new(src("m")),
            },
        );
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
    fn stddev_maps_to_literal_exact_intent() {
        // Matches `asap_l2::lower`'s own mapping: no more two-quantile
        // IQR-proxy `Merge` fan-out (that claimed a `QuantileApprox`
        // ASAP-tier capability `capability_for` never actually backed
        // for `StdDev`/`Variance` — same bug class as the old
        // `avg → p50` approximation).
        let legacy = agg(
            vec![],
            false,
            vec![agg_item("sd", AggFunc::StdDev { population: false })],
            src("m"),
        );
        match convert_root(&legacy).unwrap() {
            CQueryExpr::Aggregate { aggs, .. } => {
                assert!(matches!(
                    aggs.as_slice(),
                    [AggIntent::StdDev {
                        population: false,
                        ..
                    }]
                ));
            }
            other => panic!("expected a plain Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn rate_and_increase_map_to_dedicated_intents() {
        let cases = [
            (
                AggFunc::Rate {
                    window: Duration::from_secs(300),
                },
                AggIntent::Rate,
            ),
            (
                AggFunc::Increase {
                    window: Duration::from_secs(300),
                },
                AggIntent::Increase,
            ),
        ];
        for (func, expected) in cases {
            let legacy = agg(vec![], false, vec![agg_item("r", func)], src("m"));
            match convert_root(&legacy).unwrap() {
                CQueryExpr::Aggregate { aggs, .. } => {
                    assert_eq!(aggs.as_slice(), [expected]);
                }
                other => panic!("expected Aggregate, got {other:?}"),
            }
        }
    }

    #[test]
    fn changes_resets_delta_family_map_to_dedicated_intents() {
        let cases = [
            (AggFunc::Changes, AggIntent::Changes),
            (AggFunc::Resets, AggIntent::Resets),
            (AggFunc::Delta, AggIntent::Delta),
            (AggFunc::IDelta, AggIntent::IDelta),
            (AggFunc::Deriv, AggIntent::Deriv),
            (
                AggFunc::PredictLinear { seconds: 60.0 },
                AggIntent::PredictLinear { seconds: 60.0 },
            ),
        ];
        for (func, expected) in cases {
            let legacy = agg(vec![], false, vec![agg_item("x", func)], src("m"));
            match convert_root(&legacy).unwrap() {
                CQueryExpr::Aggregate { aggs, .. } => {
                    assert_eq!(aggs.as_slice(), [expected]);
                }
                other => panic!("expected Aggregate, got {other:?}"),
            }
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
    fn project_translates_each_item_expr() {
        use crate::intent_algebra::relational::L2ProjectItem;
        use crate::intent_algebra::L2Expr;

        let legacy = LQueryExpr::Project {
            cols: vec![L2ProjectItem {
                alias: Some("v".into()),
                expr: L2Expr::Column(LColumnRef::SampleValue),
            }],
            qualifier: None,
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
}
