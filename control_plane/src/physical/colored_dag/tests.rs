//! Module-level integration tests for the L5 stage_split framework.
//!
//! Per `control_plane/docs/design.md` §6 batched-queries example
//! (line ~1376) and the §6 single-query trace (line ~1217) — these tests
//! exercise the StageAllocator coloring rules + ThreeStageEmitter output
//! against the canonical inputs the orchestrator's spec calls out.

#![cfg(test)]

use std::rc::Rc;
use std::time::Duration;

use planner_types::post_asap::{
    GroupingStrategy, SketchAlgorithm, SketchKind, SketchParams, SketchQuery, SummaryExpr,
    SummaryFamilyType, SummaryNode, SummarySchema,
};

use crate::physical::colored_dag::allocator::StageAllocator;
use crate::physical::colored_dag::emitter::{EmitError, Emitter, StageConfig, ThreeStageEmitter};
use crate::physical::colored_dag::stage_id::{StageId, Topology};
use crate::physical::post_asap::deployment_expr::{PhysicalExpr, PostAsapPlan};
use crate::types::AccuracyTarget;
use planner_types::pre_asap::{Column, DataType};
use planner_types::pre_asap::{ColumnRef, QueryExpr, Reduction, Schema, Source};

// ── Test fixtures ─────────────────────────────────────────────────────────────

fn ts_scan(metric: &str, label: Option<(&str, &str)>) -> QueryExpr {
    let schema = Schema::with_time_index(
        vec![
            Column {
                name: "ts".into(),
                dtype: DataType::Timestamp,
                nullable: false,
                table: None,
            },
            Column {
                name: "service".into(),
                dtype: DataType::Utf8,
                nullable: false,
                table: None,
            },
            Column {
                name: "value".into(),
                dtype: DataType::Float64,
                nullable: false,
                table: None,
            },
        ],
        0,
        vec![vec![0, 1]],
    );
    let predicates = label
        .map(|(k, v)| {
            vec![crate::test_support::label_eq_predicate(k, v, &schema)
                .expect("label column present in schema")]
        })
        .unwrap_or_default();
    QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: metric.into(),
        },
        predicates,
        schema,
    }
}

fn windowed_scan() -> QueryExpr {
    QueryExpr::TimeRange {
        range: Duration::from_secs(300),
        child: Rc::new(ts_scan(
            "http_request_duration_seconds",
            Some(("service", "api")),
        )),
    }
}

/// Empty `SummarySchema` — the coloring/emitter logic under test here never
/// inspects node schemas (only `SummaryExpr` shape + `PhysicalExpr`
/// placement), so hand-built L4 nodes below carry a placeholder, same
/// spirit as this file's old comment: "these dummies only need to be
/// *structurally valid* and distinct `PhysicalExpr` values".
fn dummy_l4_schema() -> SummarySchema {
    SummarySchema {
        fields: vec![],
        time_index: None,
    }
}

/// Wrap `qe` as an unbound `Logical` L4 leaf — mirrors the old
/// `PhysicalExpr::Logical(qe)` construction for hand-built fixtures.
fn logical_l4(qe: QueryExpr) -> Rc<SummaryNode> {
    crate::planner_selection::keep_pre_asap(&qe).unwrap()
}

/// Hand-build a `SummaryAgg` node — mirrors the old
/// `PhysicalExpr::SketchAgg { sketch_type, params, child }` construction,
/// for fixtures that need a specific family without going through
/// `implement_tree`'s cost-model selection.
fn sketch_agg_l4(
    kind: SketchAlgorithm,
    params: SketchParams,
    child: Rc<SummaryNode>,
) -> Rc<SummaryNode> {
    Rc::new(SummaryNode {
        expr: SummaryExpr::SummaryAgg {
            child,
            family: SummaryFamilyType::Sketch(
                SketchKind::new(kind, params),
                GroupingStrategy::default(),
            ),
            input: planner_types::post_asap::SummaryUpdate {
                item: None,
                weight: planner_types::post_asap::SummaryInputExpr::Column(ColumnRef::SampleValue),
                weight_domain: Default::default(),
            },
            reduction: Reduction::by(vec![]),
            grouping: GroupingStrategy::default(),
        },
        schema: dummy_l4_schema(),
        guarantee: None,
    })
}

/// Hand-build a `SummaryEstimate` node — mirrors the old
/// `PhysicalExpr::SketchEstimate { op, child }`.
fn estimate_l4(query: SketchQuery, summary_input: Rc<SummaryNode>) -> Rc<SummaryNode> {
    Rc::new(SummaryNode {
        expr: SummaryExpr::SummaryEstimate {
            summary_input,
            query,
        },
        schema: dummy_l4_schema(),
        guarantee: None,
    })
}

/// Hand-build a `SummaryMerge` node — mirrors the old
/// `PhysicalExpr::SketchMerge { algebra, children }`. `SummaryMerge` (the
/// upstream replacement) carries no `algebra` field — `MergeAlgebra` was
/// this crate's own addition and doesn't exist upstream (see
/// `deployment_expr.rs`'s module docs).
fn merge_l4(children: Vec<Rc<SummaryNode>>) -> Rc<SummaryNode> {
    Rc::new(SummaryNode {
        expr: SummaryExpr::SummaryMerge { children },
        schema: dummy_l4_schema(),
        guarantee: None,
    })
}

fn is_sketch_agg(expr: &PhysicalExpr) -> bool {
    matches!(expr, PhysicalExpr::Committed(PostAsapPlan::Summary(n)) if matches!(n.expr, SummaryExpr::SummaryAgg { .. }))
}
fn is_logical(expr: &PhysicalExpr) -> bool {
    matches!(expr, PhysicalExpr::Committed(PostAsapPlan::Summary(n)) if matches!(n.expr, SummaryExpr::KeepPreAsap(_)))
}
fn is_sketch_estimate(expr: &PhysicalExpr) -> bool {
    matches!(expr, PhysicalExpr::Committed(PostAsapPlan::Summary(n)) if matches!(n.expr, SummaryExpr::SummaryEstimate { .. }))
}
fn is_sketch_merge(expr: &PhysicalExpr) -> bool {
    matches!(expr, PhysicalExpr::Committed(PostAsapPlan::Summary(n)) if matches!(n.expr, SummaryExpr::SummaryMerge { .. }))
}
fn is_let_binding(expr: &PhysicalExpr) -> bool {
    matches!(
        expr,
        PhysicalExpr::Committed(PostAsapPlan::LetBinding { .. })
    )
}
fn is_ref(expr: &PhysicalExpr) -> bool {
    matches!(expr, PhysicalExpr::Committed(PostAsapPlan::Ref { .. }))
}

/// `SummaryEstimate{Quantile{0.99}}{SummaryAgg{Kll}{Logical(Window{Scan})}}`
/// — the §6 single-query trace input. Built via `implement_tree` (the
/// default cost model ranks Kll first for `Quantile`, matching this
/// fixture's old hand-built KLL family — see `asap-plan`'s
/// `boundary::summary_candidates`).
fn quantile_kll_dag() -> PhysicalExpr {
    let q = QueryExpr::Aggregate {
        reduction: Reduction::by(vec![]),
        measures: vec![planner_types::pre_asap::AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        output_names: Vec::new(),
        having: None,
        child: Rc::new(windowed_scan()),
    };
    PhysicalExpr::committed(crate::planner_selection::select_summary_default(&q).unwrap())
}

// ── Allocator: per-rule + edge-case tests ─────────────────────────────────────

#[test]
fn allocator_three_stage_basic() {
    let dag = StageAllocator
        .allocate(&quantile_kll_dag(), Topology::ThreeStage)
        .expect("allocate ok");
    // root = SummaryEstimate → Backend
    assert_eq!(dag.root().unwrap().stage, StageId::Backend);
    // 3 nodes total: SummaryEstimate, SummaryAgg, Logical(Window{Scan})
    // — `Logical` wraps the entire L3 sub-tree as a single L4 node, so
    // the inner Scan only surfaces if Logical is recursively unfolded
    // (it isn't).
    assert!(
        dag.nodes.len() >= 3,
        "expected at least 3 nodes, got {}",
        dag.nodes.len()
    );
    // SummaryAgg is colored Edge.
    let agg = dag
        .nodes
        .iter()
        .find(|n| is_sketch_agg(&n.expr))
        .expect("SummaryAgg present");
    assert_eq!(agg.stage, StageId::Edge);
    // Logical wrapper of Window is colored Edge.
    let win_or_scan = dag
        .nodes
        .iter()
        .find(|n| is_logical(&n.expr))
        .expect("Logical present");
    assert_eq!(win_or_scan.stage, StageId::Edge);
}

#[test]
fn allocator_sketch_agg_under_scan_pinned_edge() {
    // Exact design.md §6 invariant: a SummaryAgg whose child is a Scan
    // (wrapped in Logical) MUST land on Edge.
    let expr = PhysicalExpr::committed(sketch_agg_l4(
        SketchAlgorithm::Hll,
        SketchParams::Hll { precision: 14 },
        logical_l4(ts_scan("events", None)),
    ));
    let dag = StageAllocator
        .allocate(&expr, Topology::ThreeStage)
        .unwrap();
    assert_eq!(dag.root().unwrap().stage, StageId::Edge);
    assert_eq!(dag.nodes[1].stage, StageId::Edge);
}

#[test]
fn allocator_sketch_estimate_pinned_backend() {
    // SummaryEstimate MUST be on Backend (the readout side).
    let dag = StageAllocator
        .allocate(&quantile_kll_dag(), Topology::ThreeStage)
        .unwrap();
    let est = dag
        .nodes
        .iter()
        .find(|n| is_sketch_estimate(&n.expr))
        .expect("SummaryEstimate present");
    assert_eq!(est.stage, StageId::Backend);
}

#[test]
fn allocator_let_binding_color_propagates() {
    // LetBinding takes the bound expression's stage. Bind a
    // SummaryAgg{Kll} (edge) and verify the LetBinding node colors edge.
    //
    // NOTE — shape change forced by the type system, not just syntax:
    // the old fixture nested `Ref` *inside* a `SketchEstimate`'s child.
    // The new `SummaryEstimate::sketch_input` field is `Rc<SummaryNode>` —
    // upstream `asap_sketch`'s own type, which has no `Ref`/`LetBinding`
    // concept at all — so a `Ref`/`LetBinding` can only appear where an
    // `PostAsapPlan` is expected (this crate's own named-binding sharing
    // mechanism layered *above* `SummaryNode`, not inside it; see
    // `deployment_expr.rs`'s module docs). The property under test —
    // LetBinding colors by its bound expression's stage — is preserved
    // with the `child` position held by a bare `Ref` instead of a
    // `SketchEstimate{child: Ref}`.
    let inner_agg = sketch_agg_l4(
        SketchAlgorithm::Kll,
        SketchParams::Kll { k: 200 },
        logical_l4(windowed_scan()),
    );
    let bind = PhysicalExpr::Committed(PostAsapPlan::LetBinding {
        name: String::from("kll_state"),
        expr: Rc::new(PostAsapPlan::Summary(inner_agg)),
        child: Rc::new(PostAsapPlan::Ref {
            name: String::from("kll_state"),
        }),
    });
    let dag = StageAllocator
        .allocate(&bind, Topology::ThreeStage)
        .unwrap();
    let let_node = dag
        .nodes
        .iter()
        .find(|n| is_let_binding(&n.expr))
        .expect("LetBinding present");
    // LetBinding takes its expr's stage → Edge.
    assert_eq!(let_node.stage, StageId::Edge);
}

#[test]
fn allocator_ref_resolves_to_binding_stage() {
    // Ref takes the stage of its binding. Same fixture shape as
    // `allocator_let_binding_color_propagates` (see the shape-change
    // note there); the Ref child of the LetBinding must color Edge (the
    // binding's stage).
    let inner_agg = sketch_agg_l4(
        SketchAlgorithm::Kll,
        SketchParams::Kll { k: 200 },
        logical_l4(windowed_scan()),
    );
    let bind = PhysicalExpr::Committed(PostAsapPlan::LetBinding {
        name: String::from("shared"),
        expr: Rc::new(PostAsapPlan::Summary(inner_agg)),
        child: Rc::new(PostAsapPlan::Ref {
            name: String::from("shared"),
        }),
    });
    let dag = StageAllocator
        .allocate(&bind, Topology::ThreeStage)
        .unwrap();
    let ref_node = dag
        .nodes
        .iter()
        .find(|n| is_ref(&n.expr))
        .expect("Ref present");
    assert_eq!(ref_node.stage, StageId::Edge);
}

#[test]
fn allocator_sketch_merge_lands_gateway() {
    // SummaryMerge over edge-built KLL sketches → Gateway.
    let one_agg = || {
        sketch_agg_l4(
            SketchAlgorithm::Kll,
            SketchParams::Kll { k: 200 },
            logical_l4(windowed_scan()),
        )
    };
    let merge = merge_l4(vec![one_agg(), one_agg()]);
    let with_estimate = estimate_l4(SketchQuery::Quantile { q: 0.99 }, merge);
    let dag = StageAllocator
        .allocate(
            &PhysicalExpr::committed(with_estimate),
            Topology::ThreeStage,
        )
        .unwrap();
    let merge_node = dag
        .nodes
        .iter()
        .find(|n| is_sketch_merge(&n.expr))
        .expect("SummaryMerge present");
    assert_eq!(merge_node.stage, StageId::Gateway);
    assert_eq!(dag.root().unwrap().stage, StageId::Backend);
}

// ── Emitter tests ─────────────────────────────────────────────────────────────

#[test]
fn emitter_three_stage_emits_three_configs() {
    // Build a DAG with all three stages occupied: SummaryEstimate over
    // SummaryMerge over two SummaryAggs.
    let one_agg = || {
        sketch_agg_l4(
            SketchAlgorithm::Kll,
            SketchParams::Kll { k: 200 },
            logical_l4(windowed_scan()),
        )
    };
    let merge = merge_l4(vec![one_agg(), one_agg()]);
    let root = estimate_l4(SketchQuery::Quantile { q: 0.99 }, merge);
    let dag = StageAllocator
        .allocate(&PhysicalExpr::committed(root), Topology::ThreeStage)
        .unwrap();
    let configs = ThreeStageEmitter.emit_per_stage(&dag).unwrap();
    assert!(configs.contains_key(&StageId::Edge));
    assert!(configs.contains_key(&StageId::Gateway));
    assert!(configs.contains_key(&StageId::Backend));
    assert_eq!(configs.len(), 3);
}

#[test]
fn emitter_edge_config_has_correct_processor_kll() {
    let dag = StageAllocator
        .allocate(&quantile_kll_dag(), Topology::ThreeStage)
        .unwrap();
    let configs = ThreeStageEmitter.emit_per_stage(&dag).unwrap();
    match configs.get(&StageId::Edge).expect("edge config") {
        StageConfig::Edge(e) => {
            assert_eq!(e.sketch_processors.len(), 1);
            assert_eq!(e.sketch_processors[0].processor_name, "KLL");
            assert_eq!(
                e.sketch_processors[0].sketch_algorithm,
                SketchAlgorithm::Kll
            );
            assert_eq!(
                e.source_metric.as_deref(),
                Some("http_request_duration_seconds")
            );
            assert_eq!(e.window_secs, Some(300));
        }
        other => panic!("expected Edge config, got {other:?}"),
    }
}

#[test]
fn emitter_edge_config_has_correct_processor_ddsketch() {
    let expr = PhysicalExpr::committed(estimate_l4(
        SketchQuery::Quantile { q: 0.99 },
        sketch_agg_l4(
            SketchAlgorithm::DDSketch,
            SketchParams::DDSketch { alpha: 0.01 },
            logical_l4(windowed_scan()),
        ),
    ));
    let dag = StageAllocator
        .allocate(&expr, Topology::ThreeStage)
        .unwrap();
    let configs = ThreeStageEmitter.emit_per_stage(&dag).unwrap();
    match configs.get(&StageId::Edge).expect("edge config") {
        StageConfig::Edge(e) => {
            assert_eq!(e.sketch_processors[0].processor_name, "ddsketch");
        }
        other => panic!("expected Edge config, got {other:?}"),
    }
}

#[test]
fn emitter_backend_config_routes_aggregation_id() {
    let dag = StageAllocator
        .allocate(&quantile_kll_dag(), Topology::ThreeStage)
        .unwrap();
    let configs = ThreeStageEmitter.emit_per_stage(&dag).unwrap();
    let edge_aid = match configs.get(&StageId::Edge).unwrap() {
        StageConfig::Edge(e) => e.sketch_processors[0].aggregation_id.clone(),
        _ => unreachable!(),
    };
    match configs.get(&StageId::Backend).expect("backend config") {
        StageConfig::Backend(b) => {
            assert_eq!(b.aggregations.len(), 1);
            assert_eq!(b.aggregations[0].aggregation_id, edge_aid);
            assert!(matches!(
                &b.aggregations[0].family,
                SummaryFamilyType::Sketch(kind, _)
                    if kind.algorithm() == &SketchAlgorithm::Kll
            ));
            assert_eq!(b.readouts.len(), 1);
            assert_eq!(b.readouts[0].aggregation_id, edge_aid);
            // `SketchQuery` has no `PartialEq` upstream — destructure
            // instead of `assert_eq!`.
            assert!(matches!(b.readouts[0].op, SketchQuery::Quantile { q } if q == 0.99));
        }
        other => panic!("expected Backend config, got {other:?}"),
    }
}

#[test]
fn emitter_unsupported_topology_errors_cleanly() {
    // Build a colored DAG that claims SingleStage topology and pass it
    // to ThreeStageEmitter — the emitter rejects it cleanly.
    let mut dag = StageAllocator
        .allocate(&quantile_kll_dag(), Topology::ThreeStage)
        .unwrap();
    dag.topology = Topology::SingleStage;
    let err = ThreeStageEmitter.emit_per_stage(&dag).unwrap_err();
    assert_eq!(
        err,
        EmitError::UnsupportedTopology(Topology::SingleStage, Topology::ThreeStage)
    );
}

// ── End-to-end: design.md §6 batched-queries example ──────────────────────────

#[test]
fn end_to_end_quantile_workload() {
    // Two quantile queries (q=0.99, q=0.95) and one max — the §6
    // batched example. After CSE they share Window+Scan; after sketch
    // reuse they share one SummaryAgg{KLL}; q3 (Max) takes a separate
    // exact path. Phase C/B don't yet wire CSE through the typed path,
    // so this test models the post-rule structure by hand.
    //
    // Structure:
    //   Backend:  SummaryEstimate{q=0.99}              SummaryEstimate{q=0.95}
    //                       \                                   /
    //                        \                                 /
    //   Gateway:               SummaryMerge{KLL}   (and another SummaryMerge for q3)
    //   Edge:                  SummaryAgg{KLL}      SummaryAgg{KLL}    SummaryAgg{KLL}
    //                            (Window + Scan shared in real DAG; for the
    //                             test we materialise three Logical wrappers.)
    //
    // The test asserts the per-stage bucketing matches the design.md
    // table (Edge: SummaryAgg + Logical(Scan/Window/Aggregate{Max});
    // Gateway: SummaryMerge + Merge; Backend: SummaryEstimate + final
    // root).
    let agg = || {
        sketch_agg_l4(
            SketchAlgorithm::Kll,
            SketchParams::Kll { k: 200 },
            logical_l4(windowed_scan()),
        )
    };
    let merge_kll = merge_l4(vec![agg(), agg(), agg()]);
    // Two SummaryEstimate readouts hanging off the merge — the typed
    // PhysicalExpr is single-rooted, so we model the workload as the
    // higher of the two readouts (q=0.99) and assert the underlying
    // colouring is correct. The second readout (q=0.95) is exercised
    // by `allocator_let_binding_color_propagates` and the per-rule
    // tests above.
    let q99 = estimate_l4(SketchQuery::Quantile { q: 0.99 }, merge_kll);
    let dag = StageAllocator
        .allocate(&PhysicalExpr::committed(q99), Topology::ThreeStage)
        .unwrap();
    let configs = ThreeStageEmitter.emit_per_stage(&dag).unwrap();
    // Edge: 3 SummaryAgg processors.
    match configs.get(&StageId::Edge).unwrap() {
        StageConfig::Edge(e) => {
            assert_eq!(e.sketch_processors.len(), 3);
            for p in &e.sketch_processors {
                assert_eq!(p.processor_name, "KLL");
            }
        }
        _ => unreachable!(),
    }
    // Gateway: at least one merge processor.
    match configs.get(&StageId::Gateway).unwrap() {
        StageConfig::Gateway(g) => {
            assert!(!g.merge_processors.is_empty());
            assert_eq!(g.merge_processors[0].processor_name, "sketchmergeprocessor");
            assert_eq!(g.merge_processors[0].sketch_algorithm, SketchAlgorithm::Kll);
        }
        _ => unreachable!(),
    }
    // Backend: one readout for q=0.99 + 3 aggregations (one per edge SummaryAgg).
    match configs.get(&StageId::Backend).unwrap() {
        StageConfig::Backend(b) => {
            assert_eq!(b.aggregations.len(), 3);
            assert_eq!(b.readouts.len(), 1);
            assert!(matches!(b.readouts[0].op, SketchQuery::Quantile { q } if q == 0.99));
        }
        _ => unreachable!(),
    }
}
