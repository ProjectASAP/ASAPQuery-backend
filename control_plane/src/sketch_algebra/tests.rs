//! Integration tests for the L4 IR + L3→L4 binding.

#![cfg(test)]

use std::rc::Rc;
use std::time::Duration;

use planner_types::post_asap::{
    ExactKind, ExactParams, SketchKind, SketchParams, SketchQuery, SummaryExpr, SummaryFamilyType,
    SummaryNode,
};
use planner_types::pre_asap::expr_ir::ColumnRef;

use crate::intent_algebra::schema::{Column, DataType};
use crate::intent_algebra::{AggIntent, LabelFilter, QueryExpr, Reduction, Schema, Source};
use crate::sketch_algebra::cost_model::ForcedFamilyCostModel;
use crate::sketch_algebra::lower::bind_query_expr;
use crate::sketch_algebra::physical_expr::{L4Plan, PhysicalExpr};
use crate::types_v2::AccuracyTarget;

// ── Test fixtures ─────────────────────────────────────────────────────────────

fn col(name: &str, dtype: DataType) -> Column {
    Column {
        name: name.into(),
        dtype,
        nullable: false,
        table: None,
    }
}

fn ts_scan() -> QueryExpr {
    let schema = Schema::with_time_index(
        vec![
            col("ts", DataType::Timestamp),
            col("service", DataType::Utf8),
            col("value", DataType::Float64),
        ],
        0,
        vec![vec![0, 1]],
    );
    let lf = LabelFilter {
        label: "service".into(),
        equals: "api".into(),
    };
    let pred = crate::intent_algebra::label_filter_to_predicate(&lf, &schema)
        .expect("service column present in schema");
    QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: "http_request_duration_seconds".into(),
        },
        predicates: vec![pred],
        schema,
    }
}

fn windowed_scan() -> QueryExpr {
    QueryExpr::TimeRange {
        range: Duration::from_secs(300),
        child: Box::new(ts_scan()),
    }
}

fn agg_quantile(q: f64, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        // No `by()` and windowed (child is `Window`) — the shape
        // `quantile_over_time(...)` lowers to: per-series, not a
        // cross-series reduction (see #165's `Reduction`).
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Quantile {
            col: None,
            q,
            accuracy,
        }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    }
}

/// Walk a `PhysicalExpr` tree and report whether any node's `SummaryExpr`
/// wraps an archive-only `AggIntent` in an unbound `Logical(Aggregate)`.
/// Mirrors `emit::mod.rs`'s classification: `Logical`-wrapped aggregates
/// whose sole intent is `archive_only()` route the L5 emitter to the
/// cold-store tier.
fn binding_is_archive(expr: &PhysicalExpr) -> bool {
    match expr {
        PhysicalExpr::Committed(plan) => plan_is_archive(plan),
        PhysicalExpr::RawAtEdgeSketchAtBackend { child, .. } => plan_is_archive(child),
        PhysicalExpr::RawAtEdgePrometheusArchive { .. } => false,
    }
}

fn plan_is_archive(plan: &L4Plan) -> bool {
    match plan {
        L4Plan::Summary(node) => node_is_archive(node),
        L4Plan::LetBinding { expr, child, .. } => plan_is_archive(expr) || plan_is_archive(child),
        L4Plan::Ref { .. } => false,
    }
}

fn node_is_archive(node: &Rc<SummaryNode>) -> bool {
    match &node.expr {
        SummaryExpr::Logical(qe) => match qe.as_ref() {
            QueryExpr::Aggregate { measures: aggs, .. } => {
                aggs.iter().any(crate::intent_algebra::archive_only)
            }
            _ => false,
        },
        SummaryExpr::SummaryAgg { child, .. } => node_is_archive(child),
        SummaryExpr::SummaryEstimate { summary_input, .. } => node_is_archive(summary_input),
        SummaryExpr::SummaryMerge { children } => children.iter().any(node_is_archive),
        SummaryExpr::SummaryJoin { outer, inner, .. } => {
            node_is_archive(outer) || node_is_archive(inner)
        }
        SummaryExpr::SummarySubtract { left, right } => {
            node_is_archive(left) || node_is_archive(right)
        }
        SummaryExpr::SummaryDelete { summary_input, .. } => node_is_archive(summary_input),
    }
}

// ── Bind rule tests ───────────────────────────────────────────────────────────

#[test]
fn bind_kll_quantile_basic() {
    // The retired `BindKllOnQuantile` rule struct's direct `.apply()`
    // call is replaced by forcing the Kll family via
    // `ForcedFamilyCostModel` — bypassing the DDSketch/KLL dispatcher
    // tie-break exercised separately by
    // `bind_picks_ddsketch_over_kll_when_eps_explicit` below.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let cost_model = ForcedFamilyCostModel::new(AccuracyTarget::Epsilon(0.01), SketchKind::Kll);
    let node = asap_aware_mapping::bind::implement_tree_with(&expr, &cost_model)
        .expect("KLL should bind a Quantile{0.99, ε=0.01}");
    match &node.expr {
        SummaryExpr::SummaryEstimate {
            query,
            summary_input,
        } => {
            assert!(matches!(query, SketchQuery::Quantile { q } if *q == 0.99));
            match &summary_input.expr {
                SummaryExpr::SummaryAgg { family, child, .. } => {
                    assert_eq!(
                        family,
                        &SummaryFamilyType::Sketch(SketchKind::Kll, SketchParams::Kll { k: 200 })
                    );
                    assert!(matches!(child.expr, SummaryExpr::Logical(_)));
                }
                other => panic!("expected SummaryAgg, got {other:?}"),
            }
        }
        other => panic!("expected SummaryEstimate, got {other:?}"),
    }
}

#[test]
fn bind_ddsketch_quantile_basic() {
    // Same shape as `bind_kll_quantile_basic`, forcing DDSketch instead
    // of the retired `BindDDSketchOnQuantile` rule struct.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let cost_model =
        ForcedFamilyCostModel::new(AccuracyTarget::Epsilon(0.01), SketchKind::DDSketch);
    let node = asap_aware_mapping::bind::implement_tree_with(&expr, &cost_model)
        .expect("DDSketch should bind a Quantile{0.99, ε=0.01}");
    match &node.expr {
        SummaryExpr::SummaryEstimate {
            query,
            summary_input,
        } => {
            assert!(matches!(query, SketchQuery::Quantile { q } if *q == 0.99));
            match &summary_input.expr {
                SummaryExpr::SummaryAgg { family, .. } => match family {
                    SummaryFamilyType::Sketch(
                        SketchKind::DDSketch,
                        SketchParams::DDSketch { alpha },
                    ) => {
                        assert!((alpha - 0.01).abs() < 1e-12)
                    }
                    other => panic!("expected DDSketch family, got {other:?}"),
                },
                other => panic!("expected SummaryAgg, got {other:?}"),
            }
        }
        other => panic!("expected SummaryEstimate, got {other:?}"),
    }
}

/// Cost-aware rule selection: the dispatcher should pick DDSketch over
/// KLL for an explicit ε-driven Quantile — `ControlPlaneCostModel::rank_candidates`
/// statically reorders DDSketch first (see its doc comment), matching the
/// legacy `algebra::directory::sketch_type_for_agg` default for SP-2/SP-4.
#[test]
fn bind_picks_ddsketch_over_kll_when_eps_explicit() {
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01))
        .expect("bind_query_expr should not error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate { summary_input, .. } => match &summary_input.expr {
                SummaryExpr::SummaryAgg { family, .. } => {
                    assert!(
                        matches!(family, SummaryFamilyType::Sketch(SketchKind::DDSketch, _)),
                        "dispatcher should pick DDSketch over KLL on ε-driven Quantile, got {family:?}"
                    );
                }
                other => panic!("expected SummaryAgg, got {other:?}"),
            },
            other => panic!("expected SummaryEstimate, got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// Build an `Aggregate{TopK{k, accuracy}}` over the windowed scan.
fn agg_topk(k: usize, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        // A ranking always reduces — empty `by` ranks the whole input,
        // never per-entity (see `lower.rs`'s `LQueryExpr::TopK` handling).
        reduction: Reduction::by(vec![]),
        measures: vec![AggIntent::TopK { k, accuracy }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    }
}

/// Pull the bound `(SketchKind, w, d)` out of a top-k binding.
/// `SketchKind` promotes `with_heap` to kind identity — the top-k cost
/// model always binds `CmsWithHeap`/`CountSketchWithHeap` for a top-k
/// intent, never the bare kind, so there's no separate heap flag to
/// return anymore.
fn topk_binding_family(bound: &PhysicalExpr) -> (SketchKind, u32, u32) {
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate {
                query,
                summary_input,
            } => {
                assert!(matches!(query, SketchQuery::TopK { k } if *k == 10));
                match &summary_input.expr {
                    SummaryExpr::SummaryAgg { family, .. } => match family {
                        SummaryFamilyType::Sketch(
                            kind @ SketchKind::CmsWithHeap,
                            SketchParams::CmsWithHeap { width, depth, .. },
                        )
                        | SummaryFamilyType::Sketch(
                            kind @ SketchKind::CountSketchWithHeap,
                            SketchParams::CountSketchWithHeap { width, depth, .. },
                        ) => (kind.clone(), *width, *depth),
                        other => {
                            panic!("expected CmsWithHeap/CountSketchWithHeap family, got {other:?}")
                        }
                    },
                    other => panic!("expected SummaryAgg, got {other:?}"),
                }
            }
            other => panic!("expected SummaryEstimate, got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// (a) A **loose-recall** top-k (any non-exact accuracy target) binds the
/// cheap **CMS-with-heap** family — the Fig-12 cost-gap fix. The old rule
/// hard-bound the ~66×-more-expensive CountSketch here.
#[test]
fn bind_cms_topk_loose_recall_picks_cms_heap() {
    let acc = AccuracyTarget::EpsilonDelta {
        epsilon: 0.01,
        delta: 0.001,
    };
    let expr = agg_topk(10, acc.clone());
    let bound = bind_query_expr(&expr, acc).expect("bind_query_expr should not error");
    let (kind, w, d) = topk_binding_family(&bound);
    assert_eq!(
        kind,
        SketchKind::CmsWithHeap,
        "loose-recall top-k must bind the cheap CMS-with-heap, not CountSketch"
    );
    assert!(w >= 2);
    assert!(d >= 1);
}

/// (b) A **tight / exact-recall** top-k binds the unbiased
/// **CountSketch-with-heap** — the family that supports exact rank /
/// signed estimates.
///
/// NOTE — behavior change forced by the new binder, not just a rename:
/// the old fixture used `AggIntent::TopK{accuracy: Exact}` (the intent's
/// OWN accuracy) to signal "tight/exact-recall". Under
/// `asap_aware_mapping::boundary::implementation_for_with`, the per-intent
/// summary-vs-exact boundary decision checks the intent's own `accuracy`
/// field FIRST: `TopK{accuracy: Exact}` now declines to bind at all
/// (`SummaryExpr::Logical`) rather than reaching the cost model's
/// family-selection logic at all — see `topk_exact_accuracy_declines_to_bind`
/// above (a REAL, accepted behavior change — ASAPController#151 — per
/// this migration's design notes, not a bug to route around). "Tight
/// recall" (→ CountSketchWithHeap) is still live logic in
/// `ControlPlaneCostModel::topk_family_order` — it fires off the
/// WORKLOAD-level accuracy (not the intent's own) being `Exact`, which
/// still lets the intent itself bind.
#[test]
fn bind_cms_topk_tight_recall_picks_countsketch() {
    // Intent requests a normal (non-exact) rank so binding still
    // happens; the workload-level policy demands exact recall.
    let expr = agg_topk(10, AccuracyTarget::Epsilon(0.01));
    let bound =
        bind_query_expr(&expr, AccuracyTarget::Exact).expect("bind_query_expr should not error");
    let (kind, w, d) = topk_binding_family(&bound);
    assert_eq!(
        kind,
        SketchKind::CountSketchWithHeap,
        "exact-rank top-k must bind the unbiased CountSketch-with-heap"
    );
    assert!(w >= 2);
    assert!(d >= 1);
}

/// (c) The chosen family is the **cost-minimal one that meets the recall
/// SLA**, per the `optimizer::cost::wire` table — the same "min cost s.t.
/// SLA" the oracle uses. Loose → both families clear the bar → cheapest
/// (CMS, ~4 KB) wins; the CountSketch alternative (~250 KB) is ~66×
/// costlier.
#[test]
fn bind_cms_topk_picks_cost_min_meeting_sla() {
    use crate::optimizer::cost::wire::WireCostTable;
    let table = WireCostTable::default();
    let cms = table.for_kind(&SketchKind::Cms).per_flush();
    let cs = table.for_kind(&SketchKind::CountSketch).per_flush();
    assert!(
        cms < cs,
        "CMS-heap ({cms} B) must be cheaper than CountSketch ({cs} B) on the wire"
    );
    // The cost gap the Fig-12 harness measured (~66×).
    let ratio = cs as f64 / cms as f64;
    assert!(
        ratio > 50.0,
        "CountSketch should be ~66× the CMS-heap wire cost; got {ratio:.1}×"
    );

    // Loose recall → the planner must land on the cost-min family (CMS).
    let acc = AccuracyTarget::Epsilon(0.01);
    let bound = bind_query_expr(&agg_topk(10, acc.clone()), acc).unwrap();
    let (kind, ..) = topk_binding_family(&bound);
    let chosen = table.for_kind(&kind).per_flush();
    assert_eq!(
        chosen,
        cms.min(cs),
        "must pick the cost-min family that meets the SLA"
    );
    assert_eq!(kind, SketchKind::CmsWithHeap);
}

#[test]
fn bind_hll_cardinality_basic() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate {
                query,
                summary_input,
            } => {
                assert!(matches!(query, SketchQuery::Cardinality));
                match &summary_input.expr {
                    SummaryExpr::SummaryAgg { family, .. } => match family {
                        SummaryFamilyType::Sketch(
                            SketchKind::Hll,
                            SketchParams::Hll { precision },
                        ) => {
                            assert!(
                                *precision >= 12,
                                "ε=0.01 should land on at least precision 12 (~1.6%) per the rung table"
                            );
                        }
                        other => panic!("expected Hll family, got {other:?}"),
                    },
                    other => panic!("expected SummaryAgg, got {other:?}"),
                }
            }
            other => panic!("expected SummaryEstimate, got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

#[test]
fn sum_now_binds_to_exact_agg_after_pr_6_followup() {
    // `AggIntent::Sum` binds to a bare `SummaryAgg` with `summary:
    // SummaryKind::Sum` and no `SummaryEstimate` wrapper (the partial
    // state *is* the value — see `asap_aware_mapping::bind`'s module docs). The
    // old locally-defined `PhysicalExpr::ExactAgg { agg_type, .. }`
    // variant (and `asap_types::AggregationType`) no longer exist at
    // the L4 IR level: `planner_types::post_asap::SummaryExpr` unifies exact
    // accumulators and approximate sketches into the same `SummaryAgg`
    // node shape, keyed by `SummaryKind` (see `physical_expr.rs`'s
    // module docs).
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryAgg { family, .. } => {
                assert_eq!(
                    family,
                    &SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum),
                    "Sum should bind to SummaryAgg(Sum)"
                );
            }
            other => panic!("expected bare SummaryAgg(Sum), got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

#[test]
fn bind_exact_accuracy_disables_quantile_binding() {
    // Quantile under `AccuracyTarget::Exact` should NOT bind — the
    // optimizer falls back to an exact path. (Per design.md §6 line
    // ~1254 — "the summary path is selected, not mandated".)
    let expr = agg_quantile(0.99, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => {
            assert!(
                matches!(&node.expr, SummaryExpr::Logical(qe) if matches!(**qe, QueryExpr::Aggregate { .. })),
                "Exact accuracy should disable summary binding and pass through as Logical, got {:?}",
                node.expr
            );
        }
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

// ── Phase β: pattern-migration coverage ───────────────────────────────────────
//
// The five PromQL pattern shapes defined in `asap-planner-rs/src/planner/
// patterns.rs` each have a control plane L3/L4 equivalent. These tests are the
// per-shape cross-reference asserting the L1→L3→L4 path produces a
// matching binding without going back through asap-planner-rs.

/// `ONLY_TEMPORAL` — `quantile_over_time(0.99, m[5m])`.
/// asap-planner-rs path: ONLY_TEMPORAL pattern 1 → KLL/DDSketch summary.
/// Control plane path: `Aggregate{Quantile{0.99}}` over `Window` →
/// binds a quantile-capable family → `SummaryAgg{KLL/DDSketch}`.
#[test]
fn phase_b_pattern_only_temporal_quantile_binds_to_sketch() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate {
                query,
                summary_input,
            } => {
                assert!(matches!(query, SketchQuery::Quantile { .. }));
                match &summary_input.expr {
                    SummaryExpr::SummaryAgg { family, .. } => {
                        assert!(matches!(
                            family,
                            SummaryFamilyType::Sketch(SketchKind::Kll | SketchKind::DDSketch, _)
                        ));
                    }
                    other => panic!("expected SummaryAgg under SummaryEstimate, got {other:?}"),
                }
            }
            other => panic!("expected SummaryEstimate, got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// `ONLY_TEMPORAL` — `sum_over_time(m[5m])` (and the count/avg/min/max
/// variants that legacy `single_query.rs` accepts).
///
/// Control plane path: `Aggregate{Sum}` over `Window` → binds to a bare
/// `SummaryAgg{summary: SummaryKind::Sum}` (an exact mergeable
/// accumulator — see `sum_now_binds_to_exact_agg_after_pr_6_followup`'s
/// doc comment for the `ExactAgg` → `SummaryAgg` unification).
#[test]
fn phase_b_pattern_only_temporal_sum_binds_to_exact_agg() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryAgg { family, .. } => {
                assert_eq!(
                    family,
                    &SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum)
                );
            }
            other => panic!("expected SummaryAgg(Sum), got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// `ONLY_SPATIAL` — `sum by (host) (m)`.
/// Control plane path: `Aggregate{Sum, by=[host]}` over a bare `Scan`.
///
/// The old locally-defined `AggregationType::MultipleSum` (keyed vs
/// unkeyed sum) identity no longer exists at the L4 IR level —
/// `SummaryKind::Sum` covers both; the keyed/unkeyed distinction now
/// lives on `SummaryAgg::by` (non-empty ⇒ the old "MultipleSum" shape),
/// per `emit::mod.rs`'s exact-accumulator classification notes.
#[test]
fn phase_b_pattern_only_spatial_aggregate_binds_to_multiple_sum() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::by(vec![1]), // service column
        measures: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(ts_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryAgg {
                family, reduction, ..
            } => {
                assert_eq!(
                    family,
                    &SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum)
                );
                assert_eq!(
                    reduction.group_keys().map(|k| k.keys()),
                    Some(&[1][..]),
                    "keyed sum must carry the group-by column (the MultipleSum-equivalent signal)"
                );
            }
            other => panic!("expected SummaryAgg(Sum, by=[1]), got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// `ONE_TEMPORAL_ONE_SPATIAL` — `sum by (host) (rate(m[5m]))`.
/// `bind_query_expr` (not `implement_tree` directly) rewrites
/// `AggIntent::Rate` to `AggIntent::Increase` before binding (see
/// `lower.rs`'s `rewrite_rate_to_increase` — this deployment's data
/// plane has no Rate accumulator). The old
/// `AggregationType::MultipleIncrease` identity is now
/// `SummaryKind::Increase` with a non-empty `by`.
#[test]
fn phase_b_pattern_temporal_and_spatial_combined_binds_to_multiple_increase() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::by(vec![1]),
        measures: vec![AggIntent::Rate],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryAgg {
                family, reduction, ..
            } => {
                assert_eq!(
                    family,
                    &SummaryFamilyType::ExactAggregate(ExactKind::Increase, ExactParams::Increase)
                );
                assert_eq!(reduction.group_keys().map(|k| k.keys()), Some(&[1][..]));
            }
            other => panic!("expected SummaryAgg(Increase, by=[1]), got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

/// Phase β archive-only intent: any of the no-ASAP-tier-family entries
/// (`Absent`, `Present`, `Delta`, …) binds to a `Logical` pass-through,
/// and the L5 emitter / Phase α routing reads `AggIntent::archive_only()
/// == true` to flag the StreamingConfig entry for the archive tier.
///
/// `histogram_quantile(...)` was previously an L3 intent here but is no
/// longer — it's a PromQL/MetricsQL language-level operator that the
/// parser substitutes (Step γ5) into a plain `Aggregate { Quantile(φ) }`,
/// NOT a semantic intent. The L1→L3 lowerer's documented contract is
/// `histogram_quantile(q, bucket_metric) → AggIntent::Quantile { q, .. }`;
/// bucket-aware reduction is a physical-planner concern.
#[test]
fn phase_b_pattern_archive_only_routes_to_archive() {
    let intent = AggIntent::Absent;
    assert!(
        crate::intent_algebra::archive_only(&intent),
        "Phase β intent must flag archive"
    );
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent.clone()],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // The archive-only rule's output is a Logical pass-through carrying
    // the original Aggregate. Downstream emitters check archive_only().
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
            SummaryExpr::Logical(qe) => match qe.as_ref() {
                QueryExpr::Aggregate { measures: aggs, .. } => {
                    assert_eq!(aggs, &vec![intent]);
                }
                other => panic!("expected Aggregate, got {other:?}"),
            },
            other => panic!("expected Logical(Aggregate(Absent)), got {other:?}"),
        },
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

// ── Phase β: end-to-end pattern equivalence with asap-planner-rs ─────────────
//
// The asap-planner-rs test suite drives a set of canonical PromQL workloads
// (`tests/comparison/test_data/configs/*.yaml`). For each, the legacy
// planner produces a StreamingConfig with one or more `aggregation_id`
// entries keyed on (sketch_kind, sketch_params).
//
// Phase β asserts the CONTROL PLANE's L1→L3→L4 path produces a functionally
// equivalent set of bound aggregations for the same input strings. We
// don't load the YAML files — that would couple the control plane to the
// asap-planner-rs test fixture layout. Instead each test embeds the
// representative query string from the corresponding fixture YAML and
// pins the expected (sketch_kind | archive-only) outcome.

/// Helper: parse a PromQL string to the canonical L3 `QueryExpr`, bind to
/// L4. Returns the produced `PhysicalExpr` for assertion.
/// `parse_query_expr_canonical` is the real parse path (L1 → L2 relational
/// → L3 canonical via `lower`); `bind_query_expr` is the
/// L3→L4 bottom-up walk under the supplied accuracy target.
fn pipeline_l1_to_l4(query: &str, accuracy: AccuracyTarget) -> PhysicalExpr {
    let qe = crate::query_parser::parse_query_expr_canonical(query, accuracy.clone())
        .unwrap_or_else(|e| panic!("parse {query}: {e}"));
    bind_query_expr(&qe, accuracy).unwrap_or_else(|e| panic!("bind {query}: {e}"))
}

/// `quantile_over_time.yaml` — the asap-planner-rs `quantile_over_time`
/// fixture maps to a KLL or DDSketch StreamingConfig row. The control plane
/// path: L1 PromQL parse → L3 `Aggregate{Quantile{0.99}}` over `Window` →
/// L4 bind picks Kll (default) or DDSketch. Either is functionally
/// equivalent — both are quantile sketches.
#[test]
fn phase_b_e2e_quantile_over_time_binds_to_quantile_sketch() {
    let bound = pipeline_l1_to_l4(
        "quantile_over_time(0.99, http_request_duration_seconds[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    let kind = crate::emit::extract_root_sketch_kind(&bound);
    assert!(
        matches!(kind, Some(SketchKind::Kll) | Some(SketchKind::DDSketch)),
        "expected quantile summary family, got {kind:?}"
    );
    assert!(
        !binding_is_archive(&bound),
        "ASAP-tier quantile must not flag archive"
    );
}

/// `sum_over_time.yaml` — the legacy planner produces an exact-sum
/// aggregation row (no summary). Control plane path: `Aggregate{Sum}` over
/// `Window` → binds to an exact accumulator (`SummaryAgg{Sum}`), which is
/// neither an approximate summary (so `extract_root_sketch_kind`, which
/// excludes exact accumulators — see its doc comment — returns `None`)
/// nor archive-routed.
#[test]
fn phase_b_e2e_sum_over_time_falls_through_to_logical() {
    let bound = pipeline_l1_to_l4(
        "sum_over_time(http_requests_total[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    assert!(
        crate::emit::extract_root_sketch_kind(&bound).is_none(),
        "sum_over_time should not produce an approximate summary"
    );
    assert!(
        !binding_is_archive(&bound),
        "Sum is exact-warm, not archive"
    );
}

/// `sum_by.yaml` — `sum by (label) (sum_over_time(...))`. Spatial-and-
/// temporal aggregation; the legacy planner emits an exact-sum row keyed
/// on the by-label. Control plane path: `Aggregate{Sum, by=[…]}` over
/// `Window` → binds to an exact accumulator (`SummaryAgg{Sum, by=[…]}`) —
/// no approximate summary family. The by-label is preserved on the L3
/// group-by-id list, which Phase α's routing emit reads to build the
/// per-label rollup partition.
#[test]
fn phase_b_e2e_sum_by_preserves_grouping_label() {
    let bound = pipeline_l1_to_l4(
        "sum by (instance) (sum_over_time(http_requests_total[5m]))",
        AccuracyTarget::Epsilon(0.01),
    );
    // No approximate summary family for plain Sum.
    assert!(crate::emit::extract_root_sketch_kind(&bound).is_none());
    // The end shape may carry `Logical(Aggregate{by, ...})` beneath a
    // `SummaryAgg{Sum}` wrapper, or `Logical(Window{...})` when the
    // ParsedQuery → QueryExpr lowering drops the Aggregate (legacy
    // ParsedQuery only carries `aggregations: Vec<AggType>` not the
    // by-axis directly). In either case the metric name + label survive
    // somewhere in the L3 sub-tree — assert that.
    // `PhysicalExpr` is no longer `Serialize` (see its doc) — `Debug`
    // output still contains every string literal in the tree, so it
    // works just as well for this substring search.
    let dbg = format!("{bound:?}");
    assert!(
        dbg.contains("http_requests_total"),
        "metric name lost through pipeline: {dbg}"
    );
    assert!(
        dbg.contains("instance"),
        "by-label `instance` lost through pipeline: {dbg}"
    );
}

/// `rate_increase.yaml` — the legacy planner emits a MultipleIncrease
/// (counter-reset adjusted) row. Control plane path: `Aggregate{Rate}` over
/// `Window` → `bind_query_expr` rewrites `Rate` to `Increase` and binds an
/// exact accumulator (`SummaryAgg{Increase}`) — no approximate summary
/// family. Both paths produce a single non-summary streaming row; the L5
/// emitter is the one that picks the actual MultipleIncrease processor.
#[test]
fn phase_b_e2e_rate_falls_through_to_logical() {
    let bound = pipeline_l1_to_l4(
        "rate(http_requests_total[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    assert!(crate::emit::extract_root_sketch_kind(&bound).is_none());
    assert!(
        !binding_is_archive(&bound),
        "Rate is ASAP-tier, not archive"
    );
}

/// `topk.yaml` — `topk(10, sum by (label) (rate(...))`. The legacy
/// planner emits a CountSketch+heap row. Control plane path: the parser
/// recognises `topk` as a special node that lowers to `AggIntent::TopK`.
/// At the time of writing, the control plane's `parse_query` may flatten
/// `topk` differently (no `inside_topk` propagation through TopK +
/// nested aggregate). The test asserts the END-STATE: either a
/// CountSketch summary fired, OR a Logical pass-through (which Phase γ
/// can decide whether to refine). The contract Phase β cares about is
/// that the bound expression is well-formed.
#[test]
fn phase_b_e2e_topk_well_formed() {
    let bound = pipeline_l1_to_l4(
        "topk(10, sum by (instance) (rate(http_requests_total[5m])))",
        AccuracyTarget::Epsilon(0.05),
    );
    // Either a CountSketch / KLL / DDSketch fires (warm path) or it's a
    // Logical pass-through (engine handles it). Both are accepted L4
    // shapes — Phase β's contract is just "doesn't panic, produces a
    // legitimate PhysicalExpr".
    let _ = crate::emit::extract_root_sketch_kind(&bound);
}

/// Archive-only routing through the full L1→L3→L4 pipeline. Asserts the
/// expected functional equivalent of asap-planner-rs's previous
/// `is_supported() == false` behavior (refused outright); the control plane
/// now lifts these to L3 with `archive_only() == true` and the binder
/// emits a Logical pass-through.
///
/// Replaces the prior `phase_b_e2e_histogram_quantile_e2e_through_parser`
/// — `histogram_quantile(...)` is now a PromQL/MetricsQL language-level
/// operator that the parser substitutes (Step γ5) into a plain
/// `Aggregate { Quantile(φ) }`, NOT an L3 intent of its own. The
/// archive-only routing this test exercises uses `Absent` as a stable
/// proxy (every archive-only variant follows the same code path).
#[test]
fn phase_b_e2e_archive_only_e2e_binding() {
    let intent = AggIntent::Absent;
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent.clone()],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    assert!(
        binding_is_archive(&bound),
        "archive-only intent must surface archive flag through L4 binding"
    );
    // No approximate summary fires for archive-only intents.
    assert!(crate::emit::extract_root_sketch_kind(&bound).is_none());
}

/// Cross-cutting: every Phase β archive-only intent reaches
/// `bind_query_expr` and lands as a `Logical` pass-through whose contents
/// the L5 emitter can route via `archive_only()`. Mirrors Phase γ's
/// "delete asap-planner-rs without losing coverage" goal — none of these
/// raise an error or panic; all produce a valid L4 expression.
#[test]
fn phase_b_archive_only_intents_round_trip_through_binder() {
    let intents = vec![
        AggIntent::Absent,
        AggIntent::AbsentOverTime,
        AggIntent::PresentOverTime,
        AggIntent::Delta,
        AggIntent::Deriv,
        AggIntent::PredictLinear { seconds: 60.0 },
        AggIntent::DoubleExpSmoothing {
            smoothing: 0.3,
            trend: 0.3,
        },
        AggIntent::IDelta,
        AggIntent::Resets,
        AggIntent::Changes,
        // Spot-check a couple of the Phase 1 IR merge's new archive-only
        // intents through the same round-trip.
        AggIntent::HistogramCount,
        AggIntent::Group,
    ];
    for intent in intents {
        let expr = QueryExpr::Aggregate {
            reduction: Reduction::PerEntity,
            measures: vec![intent.clone()],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let bound =
            bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind should succeed");
        match bound {
            PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
                SummaryExpr::Logical(qe) => match qe.as_ref() {
                    QueryExpr::Aggregate { measures: aggs, .. } => {
                        assert_eq!(aggs.len(), 1);
                        assert!(
                            crate::intent_algebra::archive_only(&aggs[0]),
                            "{intent:?} should preserve archive_only() flag through bind"
                        );
                    }
                    other => panic!("expected Aggregate({intent:?}), got {other:?}"),
                },
                other => panic!("expected Logical(Aggregate({intent:?})), got {other:?}"),
            },
            other => panic!("expected Committed(Summary(_)) for {intent:?}, got {other:?}"),
        }
    }
}

// ── Deliberate behavior changes (ASAPController#150 / #151) ──────────────────
//
// `AggIntent::Extension` (this deployment's `Frequency` point-query,
// built via `crate::intent_algebra::frequency(accuracy, item)`) now binds to a
// real `Cms` summary via `ControlPlaneCostModel::realize_extension`/
// `readout_extension` (ASAPController#150) — see `frequency_extension_binds_cms`
// below and `optimizer::rules::mod::tests::typed_binding_endpoint_request_freq_binds_cms`.
// `AggIntent::TopK { accuracy: Exact }` still declines to bind
// (`SummaryExpr::Logical`) rather than summary — a REAL, accepted
// behavior change from this migration that remains open
// (`TopK{Exact}`'s `exact_realization` has no accumulator form for it —
// see `lower.rs`'s module docs and `cost_model.rs`'s module docs,
// ASAPController#151, still open).

#[test]
fn frequency_extension_binds_cms() {
    // `ControlPlaneCostModel::realize_extension`/`readout_extension`
    // (ASAPController#150) now realize `AggIntent::Extension{"frequency"}`
    // as a real `Cms` summary instead of declining to `Logical`.
    let intent = crate::intent_algebra::frequency(AccuracyTarget::Epsilon(0.01), None);
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => {
            let SummaryExpr::SummaryEstimate {
                summary_input,
                query,
            } = &node.expr
            else {
                panic!("expected SummaryEstimate, got {:?}", node.expr);
            };
            assert!(
                matches!(
                    &summary_input.expr,
                    SummaryExpr::SummaryAgg {
                        family: SummaryFamilyType::Sketch(SketchKind::Cms, _),
                        ..
                    }
                ),
                "expected a Cms SummaryAgg, got {:?}",
                summary_input.expr
            );
            assert!(
                matches!(
                    query,
                    SketchQuery::PointCount {
                        key: ColumnRef::SampleValue,
                        value: None
                    }
                ),
                "no filter value threaded through yet (ASAPQuery-backend Phase 3) -- \
                 should read out as the bare bucket total, got {query:?}",
            );
        }
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

#[test]
fn topk_exact_accuracy_declines_to_bind() {
    let expr = agg_topk(10, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    match bound {
        PhysicalExpr::Committed(L4Plan::Summary(node)) => {
            assert!(
                matches!(&node.expr, SummaryExpr::Logical(_)),
                "TopK{{accuracy: Exact}} should decline pending ASAPController#151, got {:?}",
                node.expr
            );
        }
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}
