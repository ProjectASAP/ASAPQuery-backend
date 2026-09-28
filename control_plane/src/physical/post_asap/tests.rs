//! Integration tests for the L4 IR + L3→L4 binding.

#![cfg(test)]

use std::rc::Rc;
use std::time::Duration;

use planner_types::post_asap::{
    ExactKind, ExactParams, GroupingStrategy, SketchAlgorithm, SketchKind, SketchParams,
    SketchQuery, SummaryExpr, SummaryFamilyType, SummaryNode,
};
use planner_types::pre_asap::expr_ir::ColumnRef;

use crate::physical::post_asap::cost_model::ForcedFamilyCostModel;
use crate::physical::post_asap::deployment_expr::{PhysicalExpr, PostAsapPlan};
use crate::physical::post_asap::lower::bind_query_expr;
use crate::types::AccuracyTarget;
use planner_types::pre_asap::{AggIntent, QueryExpr, Reduction, Schema, Source};
use planner_types::pre_asap::{Column, DataType};

fn sketch_family(kind: SketchAlgorithm, params: SketchParams) -> SummaryFamilyType {
    SummaryFamilyType::Sketch(SketchKind::new(kind, params), GroupingStrategy::default())
}

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
    let pred = crate::test_support::label_eq_predicate("service", "api", &schema)
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
        child: Rc::new(ts_scan()),
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
        child: Rc::new(windowed_scan()),
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

fn plan_is_archive(plan: &PostAsapPlan) -> bool {
    match plan {
        PostAsapPlan::Summary(node) => node_is_archive(node),
        PostAsapPlan::LetBinding { expr, child, .. } => {
            plan_is_archive(expr) || plan_is_archive(child)
        }
        PostAsapPlan::Ref { .. } => false,
    }
}

fn node_is_archive(node: &Rc<SummaryNode>) -> bool {
    match &node.expr {
        SummaryExpr::KeepPreAsap(qe) => match qe.as_ref() {
            QueryExpr::Aggregate { measures: aggs, .. } => {
                aggs.iter().any(crate::planner_selection::archive_only)
            }
            _ => false,
        },
        SummaryExpr::SummaryAgg { child, .. } => node_is_archive(child),
        SummaryExpr::ValueOperation { child, .. } => node_is_archive(child),
        SummaryExpr::SummaryEstimate { summary_input, .. } => node_is_archive(summary_input),
        SummaryExpr::SummaryMerge { children } => children.iter().any(node_is_archive),
        SummaryExpr::SummaryJoin { outer, inner, .. } => {
            node_is_archive(outer) || node_is_archive(inner)
        }
        SummaryExpr::CandidateTopK {
            candidates, values, ..
        } => node_is_archive(candidates) || node_is_archive(values),
        SummaryExpr::SummarySubtract { left, right }
        | SummaryExpr::RelationalJoin { left, right, .. }
        | SummaryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => node_is_archive(left) || node_is_archive(right),
        SummaryExpr::SummaryDelete { summary_input, .. } => node_is_archive(summary_input),
    }
}

// ── Bind rule tests ───────────────────────────────────────────────────────────

#[test]
fn bind_kll_quantile_basic() {
    // Force KLL to test its binding independently of family ranking.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let cost_model =
        ForcedFamilyCostModel::new(AccuracyTarget::Epsilon(0.01), SketchAlgorithm::Kll);
    let node = crate::planner_selection::select_query(&expr, &cost_model)
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
                        &sketch_family(SketchAlgorithm::Kll, SketchParams::Kll { k: 269 })
                    );
                    assert!(matches!(child.expr, SummaryExpr::KeepPreAsap(_)));
                }
                other => panic!("expected SummaryAgg, got {other:?}"),
            }
        }
        other => panic!("expected SummaryEstimate, got {other:?}"),
    }
}

#[test]
fn bind_ddsketch_quantile_basic() {
    // Force DDSketch to test its quantile binding.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let cost_model =
        ForcedFamilyCostModel::new(AccuracyTarget::Epsilon(0.01), SketchAlgorithm::DDSketch);
    let node = crate::planner_selection::select_query(&expr, &cost_model)
        .expect("DDSketch should bind a Quantile{0.99, ε=0.01}");
    match &node.expr {
        SummaryExpr::SummaryEstimate {
            query,
            summary_input,
        } => {
            assert!(matches!(query, SketchQuery::Quantile { q } if *q == 0.99));
            match &summary_input.expr {
                SummaryExpr::SummaryAgg { family, .. } => match family {
                    SummaryFamilyType::Sketch(kind, _)
                        if kind.algorithm() == &SketchAlgorithm::DDSketch
                            && matches!(kind.params(), SketchParams::DDSketch { .. }) =>
                    {
                        let SketchParams::DDSketch { alpha } = kind.params() else {
                            unreachable!()
                        };
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
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate { summary_input, .. } => match &summary_input.expr {
                SummaryExpr::SummaryAgg { family, .. } => {
                    assert!(
                        matches!(family, SummaryFamilyType::Sketch(kind, _) if kind.algorithm() == &SketchAlgorithm::DDSketch),
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
        child: Rc::new(windowed_scan()),
    }
}

/// Pull the bound `(SketchAlgorithm, w, d)` out of a top-k binding.
/// `SketchAlgorithm` promotes `with_heap` to kind identity — the top-k cost
/// model always binds `CmsWithHeap`/`CountSketchWithHeap` for a top-k
/// intent, never the bare kind, so there's no separate heap flag to
/// return anymore.
fn topk_binding_family(bound: &PhysicalExpr) -> (SketchAlgorithm, u32, u32) {
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate {
                query,
                summary_input,
            } => {
                assert!(matches!(query, SketchQuery::TopK { k } if *k == 10));
                match &summary_input.expr {
                    SummaryExpr::SummaryAgg { family, .. } => match family {
                        SummaryFamilyType::Sketch(kind, _)
                            if matches!(
                                kind.algorithm(),
                                SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap
                            ) =>
                        {
                            match kind.params() {
                                SketchParams::CmsWithHeap { width, depth, .. }
                                | SketchParams::CountSketchWithHeap { width, depth, .. } => {
                                    (kind.algorithm().clone(), *width, *depth)
                                }
                                other => panic!("heap algorithm has mismatched params: {other:?}"),
                            }
                        }
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

/// A loose TopK target still needs membership evidence to select a sketch.
#[test]
fn uncertified_topk_keeps_exact_execution() {
    let acc = AccuracyTarget::EpsilonDelta {
        epsilon: 0.01,
        delta: 0.001,
    };
    let expr = agg_topk(10, acc.clone());
    assert!(query_is_exact(&committed_node(
        bind_query_expr(&expr, acc).unwrap()
    )));
}

/// An exact workload target cannot accept uncertified TopK membership.
#[test]
fn exact_topk_keeps_exact_execution() {
    // Intent requests a normal (non-exact) rank so binding still
    // happens; the workload-level policy demands exact recall.
    let expr = agg_topk(10, AccuracyTarget::Epsilon(0.01));
    assert!(query_is_exact(&committed_node(
        bind_query_expr(&expr, AccuracyTarget::Exact).unwrap()
    )));
}

/// A cheaper sketch never substitutes for missing membership evidence.
#[test]
fn cheap_topk_does_not_bypass_membership_evidence() {
    use crate::physical::deployment_cost::wire::WireCostTable;
    let table = WireCostTable::default();
    let cms = table.for_algorithm(&SketchAlgorithm::Cms).per_flush();
    let cs = table
        .for_algorithm(&SketchAlgorithm::CountSketch)
        .per_flush();
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
    assert!(query_is_exact(&committed_node(
        bind_query_expr(&agg_topk(10, acc.clone()), acc).unwrap()
    )));
}

#[test]
fn uncertified_hll_keeps_exact_execution() {
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        output_names: Vec::new(),
        having: None,
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    assert!(query_is_exact(&committed_node(bound)));
}

#[test]
fn sum_now_binds_to_exact_agg_after_pr_6_followup() {
    // `AggIntent::Sum` binds to a bare `SummaryAgg` with `summary:
    // SummaryKind::Sum` and no `SummaryEstimate` wrapper (the partial
    // state *is* the value — see `asap_aware_mapping::replacement`'s module docs). The
    // old locally-defined `PhysicalExpr::ExactAgg { agg_type, .. }`
    // variant (and `asap_types::AggregationType`) no longer exist at
    // the L4 IR level: `planner_types::post_asap::SummaryExpr` unifies exact
    // accumulators and approximate sketches into the same `SummaryAgg`
    // node shape, keyed by `SummaryKind` (see `deployment_expr.rs`'s
    // module docs).
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
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
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => {
            assert!(
                matches!(&node.expr, SummaryExpr::KeepPreAsap(qe) if matches!(**qe, QueryExpr::Aggregate { .. })),
                "Exact accuracy should disable summary binding and pass through as Logical, got {:?}",
                node.expr
            );
        }
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

// PromQL pattern coverage through canonical parsing and physical binding.

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
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
            SummaryExpr::SummaryEstimate {
                query,
                summary_input,
            } => {
                assert!(matches!(query, SketchQuery::Quantile { .. }));
                match &summary_input.expr {
                    SummaryExpr::SummaryAgg { family, .. } => {
                        assert!(matches!(
                            family,
                            SummaryFamilyType::Sketch(kind, _)
                                if matches!(kind.algorithm(), SketchAlgorithm::Kll | SketchAlgorithm::DDSketch)
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
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
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
        child: Rc::new(ts_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
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
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
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

/// Archive-only intents produce logical pass-through plans.
#[test]
fn phase_b_pattern_archive_only_routes_to_archive() {
    let intent = AggIntent::Absent;
    assert!(
        crate::planner_selection::archive_only(&intent),
        "Phase β intent must flag archive"
    );
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent.clone()],
        output_names: Vec::new(),
        having: None,
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // The archive-only rule's output is a Logical pass-through carrying
    // the original Aggregate. Downstream emitters check archive_only().
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
            SummaryExpr::KeepPreAsap(qe) => match qe.as_ref() {
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

// Representative PromQL workloads pin the expected sketch or archive binding.

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
    let kind = crate::emit::extract_root_sketch_algorithm(&bound);
    assert!(
        matches!(
            kind,
            Some(SketchAlgorithm::Kll) | Some(SketchAlgorithm::DDSketch)
        ),
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
/// neither an approximate summary (so `extract_root_sketch_algorithm`, which
/// excludes exact accumulators — see its doc comment — returns `None`)
/// nor archive-routed.
#[test]
fn phase_b_e2e_sum_over_time_falls_through_to_logical() {
    let bound = pipeline_l1_to_l4(
        "sum_over_time(http_requests_total[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    assert!(
        crate::emit::extract_root_sketch_algorithm(&bound).is_none(),
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
    assert!(crate::emit::extract_root_sketch_algorithm(&bound).is_none());
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
    assert!(crate::emit::extract_root_sketch_algorithm(&bound).is_none());
    assert!(
        !binding_is_archive(&bound),
        "Rate is ASAP-tier, not archive"
    );
}

/// Value-ranked TopK candidates with missing membership evidence remain
/// inspectable but cannot be reported as certified selections.
#[test]
fn value_ranked_topk_without_membership_evidence_is_not_certified() {
    let query = "topk(10, sum by (instance) (rate(http_requests_total[5m])))";
    let accuracy = AccuracyTarget::Epsilon(0.05);
    let expr = crate::query_parser::parse_query_expr_canonical(query, accuracy.clone())
        .expect("TopK parses");
    let (_, trace) = crate::planner_selection::select_workload_with_accuracy_model_and_trace(
        vec![(0, std::rc::Rc::new(expr))],
        accuracy.clone(),
        &crate::physical::post_asap::cost_model::ControlPlaneCostModel::new(accuracy),
        &asap_aware_mapping::NoAccuracyEvidence,
        &asap_aware_mapping::DefaultAccuracyModel,
    )
    .unwrap();
    assert!(trace["groups"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|group| group["candidates"].as_array().unwrap())
        .any(
            |candidate| candidate["accuracy_status"] == "unknown" && candidate["selected"] == false
        ));
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
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    assert!(
        binding_is_archive(&bound),
        "archive-only intent must surface archive flag through L4 binding"
    );
    // No approximate summary fires for archive-only intents.
    assert!(crate::emit::extract_root_sketch_algorithm(&bound).is_none());
}

/// Archive-only intents must produce valid logical expressions without errors.
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
            child: Rc::new(windowed_scan()),
        };
        let bound =
            bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind should succeed");
        match bound {
            PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
                SummaryExpr::KeepPreAsap(qe) => match qe.as_ref() {
                    QueryExpr::Aggregate { measures: aggs, .. } => {
                        assert_eq!(aggs.len(), 1);
                        assert!(
                            crate::planner_selection::archive_only(&aggs[0]),
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
// built via `crate::planner_selection::frequency(accuracy, item)`) now binds to a
// real `Cms` summary via `ControlPlaneCostModel::realize_extension`/
// `readout_extension` (ASAPController#150) — see `frequency_extension_binds_cms`
// below and `physical::workload_planner::mod::tests::typed_binding_endpoint_request_freq_binds_cms`.
// `AggIntent::TopK { accuracy: Exact }` still declines to bind
// (`SummaryExpr::KeepPreAsap`) rather than summary — a REAL, accepted
// behavior change from this migration that remains open
// (`TopK{Exact}`'s `exact_realization` has no accumulator form for it —
// see `lower.rs`'s module docs and `cost_model.rs`'s module docs,
// ASAPController#151, still open).

#[test]
fn frequency_extension_binds_cms() {
    // `ControlPlaneCostModel::realize_extension`/`readout_extension`
    // (ASAPController#150) now realize `AggIntent::Extension{"frequency"}`
    // as a real `Cms` summary instead of declining to `Logical`.
    let intent = crate::planner_selection::frequency(AccuracyTarget::Epsilon(0.01), None);
    let expr = QueryExpr::Aggregate {
        reduction: Reduction::PerEntity,
        measures: vec![intent],
        output_names: Vec::new(),
        having: None,
        child: Rc::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => {
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
                        family: SummaryFamilyType::Sketch(kind, _),
                        ..
                    } if kind.algorithm() == &SketchAlgorithm::Cms
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
        PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => {
            assert!(
                matches!(&node.expr, SummaryExpr::KeepPreAsap(_)),
                "TopK{{accuracy: Exact}} should decline pending ASAPController#151, got {:?}",
                node.expr
            );
        }
        other => panic!("expected Committed(Summary(_)), got {other:?}"),
    }
}

fn committed_node(bound: PhysicalExpr) -> Rc<SummaryNode> {
    let PhysicalExpr::Committed(PostAsapPlan::Summary(node)) = bound else {
        panic!("expected committed query")
    };
    node
}

fn query_is_exact(node: &SummaryNode) -> bool {
    node.guarantee
        .as_ref()
        .is_some_and(|guarantee| guarantee.is_exact())
}
