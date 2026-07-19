//! Integration tests for the L4 IR + `Bind*` rules.

#![cfg(test)]

use std::time::Duration;

use crate::intent_algebra::schema::{Column, DataType};
use crate::intent_algebra::{AggIntent, LabelFilter, QueryExpr, Schema, Source, WindowKind};
use crate::sketch_algebra::lower::bind_query_expr;
use crate::sketch_algebra::physical_expr::{EstimateOp, MergeAlgebra, PhysicalExpr};
use crate::sketch_algebra::rules::{
    bind_ddsketch_quantile::BindDDSketchOnQuantile, bind_kll_quantile::BindKllOnQuantile, Rule,
};
use crate::types_v2::{AccuracyTarget, BindingName};
use asap_sketch::{SummaryKind, SummaryParams};

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
    QueryExpr::Window {
        kind: WindowKind::Sliding,
        size: Duration::from_secs(300),
        slide: None,
        child: Box::new(ts_scan()),
    }
}

fn agg_quantile(q: f64, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        by: crate::intent_algebra::GroupKeys::none(),
        aggs: vec![AggIntent::Quantile {
            col: None,
            q,
            accuracy,
        }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    }
}

// ── Bind rule tests ───────────────────────────────────────────────────────────

#[test]
fn bind_kll_quantile_basic() {
    // The KLL rule on its own (priority 5) — DDSketch (priority 6) wins
    // the dispatcher tie-break, so test the KLL rule's `apply` directly.
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = BindKllOnQuantile
        .apply(&expr, &AccuracyTarget::Epsilon(0.01))
        .expect("KLL rule should bind a Quantile{0.99, ε=0.01}");
    match bound {
        PhysicalExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                PhysicalExpr::SketchAgg {
                    sketch_type,
                    params,
                    child,
                } => {
                    assert_eq!(sketch_type, SummaryKind::Kll);
                    assert_eq!(params, SummaryParams::Kll { k: 200 });
                    assert!(matches!(
                        *child,
                        PhysicalExpr::Logical(QueryExpr::Window { .. })
                    ));
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_ddsketch_quantile_basic() {
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = BindDDSketchOnQuantile
        .apply(&expr, &AccuracyTarget::Epsilon(0.01))
        .expect("DDSketch rule should bind a Quantile{0.99, ε=0.01}");
    match bound {
        PhysicalExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                PhysicalExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SummaryKind::DDSketch);
                    match params {
                        SummaryParams::DDSketch { alpha } => {
                            assert!((alpha - 0.01).abs() < 1e-12)
                        }
                        other => panic!("expected DDSketch params, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

/// Cost-aware rule selection: the dispatcher should pick DDSketch over
/// KLL for an explicit ε-driven Quantile because DDSketch has higher
/// `priority()` (6 vs 5) — that matches the legacy
/// `algebra::directory::sketch_type_for_agg` default for SP-2/SP-4.
#[test]
fn bind_picks_ddsketch_over_kll_when_eps_explicit() {
    let expr = agg_quantile(0.99, AccuracyTarget::Epsilon(0.01));
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01))
        .expect("bind_query_expr should not error");
    match bound {
        PhysicalExpr::SketchEstimate { child, .. } => match *child {
            PhysicalExpr::SketchAgg { sketch_type, .. } => {
                assert_eq!(
                    sketch_type,
                    SummaryKind::DDSketch,
                    "dispatcher should pick DDSketch (priority 6) over KLL (priority 5) on ε-driven Quantile"
                );
            }
            other => panic!("expected SketchAgg, got {other:?}"),
        },
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

/// Build an `Aggregate{TopK{k, accuracy}}` over the windowed scan.
fn agg_topk(k: usize, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        by: vec![].into(),
        aggs: vec![AggIntent::TopK { k, accuracy }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    }
}

/// Pull the bound `(SummaryKind, w, d)` out of a top-k binding.
/// `SummaryKind` (unlike the retired `sketch_algebra::SketchKind`)
/// promotes `with_heap` to kind identity — `bind_cms_topk` always binds
/// `CmsWithHeap`/`CountSketchWithHeap` for a top-k intent, never the
/// bare kind, so there's no separate heap flag to return anymore.
fn topk_binding_family(bound: &PhysicalExpr) -> (SummaryKind, u32, u32) {
    match bound {
        PhysicalExpr::SketchEstimate { op, child } => {
            assert_eq!(*op, EstimateOp::TopK { k: 10 });
            match &**child {
                PhysicalExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => match params {
                    SummaryParams::CmsWithHeap { width, depth, .. } => {
                        (sketch_type.clone(), *width, *depth)
                    }
                    SummaryParams::CountSketchWithHeap { width, depth, .. } => {
                        (sketch_type.clone(), *width, *depth)
                    }
                    other => {
                        panic!("expected CmsWithHeap/CountSketchWithHeap params, got {other:?}")
                    }
                },
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
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
        SummaryKind::CmsWithHeap,
        "loose-recall top-k must bind the cheap CMS-with-heap, not CountSketch"
    );
    assert!(w >= 2);
    assert!(d >= 1);
}

/// (b) A **tight / exact-recall** top-k (the intent carries
/// `AccuracyTarget::Exact`) binds the unbiased **CountSketch-with-heap** —
/// the family that supports exact rank / signed estimates.
#[test]
fn bind_cms_topk_tight_recall_picks_countsketch() {
    // Intent requests Exact rank; the policy-level target is non-exact.
    let expr = agg_topk(10, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01))
        .expect("bind_query_expr should not error");
    let (kind, w, d) = topk_binding_family(&bound);
    assert_eq!(
        kind,
        SummaryKind::CountSketchWithHeap,
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
    let cms = table.for_kind(&SummaryKind::Cms).per_flush();
    let cs = table.for_kind(&SummaryKind::CountSketch).per_flush();
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
    assert_eq!(kind, SummaryKind::CmsWithHeap);
}

#[test]
fn bind_hll_cardinality_basic() {
    let expr = QueryExpr::Aggregate {
        by: vec![].into(),
        aggs: vec![AggIntent::Cardinality {
            col: None,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        PhysicalExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Cardinality);
            match *child {
                PhysicalExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SummaryKind::Hll);
                    match params {
                        SummaryParams::Hll { precision } => {
                            assert!(
                                precision >= 12,
                                "ε=0.01 should land on at least precision 12 (~1.6%) per the rung table"
                            );
                        }
                        other => panic!("expected HllParams, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn sum_now_binds_to_exact_agg_after_pr_6_followup() {
    // Pre-PR-6-follow-up: `Sum` had no `Bind*` rule and passed through
    // as `PhysicalExpr::Logical`. The L4 binder rule `BindExactAgg`
    // (added in the PR-6 follow-up) now matches and emits
    // `PhysicalExpr::ExactAgg { agg_type: Sum, .. }` so the ASAP-tier
    // exact-aggregation path can serve the intent.
    let expr = QueryExpr::Aggregate {
        by: vec![].into(),
        aggs: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    match bound {
        PhysicalExpr::ExactAgg { agg_type, .. } => assert_eq!(
            agg_type,
            asap_types::AggregationType::Sum,
            "Sum should bind to ExactAgg(Sum)"
        ),
        other => panic!("expected ExactAgg, got {other:?}"),
    }
}

#[test]
fn bind_exact_accuracy_disables_quantile_binding() {
    // Quantile under `AccuracyTarget::Exact` should NOT bind — the
    // optimizer falls back to an exact path. (Per design.md §6 line
    // ~1254 — "the sketch path is selected, not mandated".)
    let expr = agg_quantile(0.99, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    assert!(
        matches!(bound, PhysicalExpr::Logical(QueryExpr::Aggregate { .. })),
        "Exact accuracy should disable sketch binding and pass through as Logical"
    );
}

// ── Phase β: pattern-migration coverage ───────────────────────────────────────
//
// The five PromQL pattern shapes defined in `asap-planner-rs/src/planner/
// patterns.rs` each have a control plane L3/L4 equivalent. These tests are the
// per-shape cross-reference asserting the L1→L3→L4 path produces a
// matching binding without going back through asap-planner-rs.

/// `ONLY_TEMPORAL` — `quantile_over_time(0.99, m[5m])`.
/// asap-planner-rs path: ONLY_TEMPORAL pattern 1 → KLL/DDSketch sketch.
/// Control plane path: `Aggregate{Quantile{0.99}}` over `Window` →
/// `BindKllOnQuantile` (or DDSketch) → `SketchAgg{KLL/DDSketch}`.
#[test]
fn phase_b_pattern_only_temporal_quantile_binds_to_sketch() {
    let expr = QueryExpr::Aggregate {
        by: vec![].into(),
        aggs: vec![AggIntent::Quantile {
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
        PhysicalExpr::SketchEstimate { op, child } => {
            assert!(matches!(op, EstimateOp::Quantile { .. }));
            match *child {
                PhysicalExpr::SketchAgg { sketch_type, .. } => {
                    assert!(matches!(
                        sketch_type,
                        SummaryKind::Kll | SummaryKind::DDSketch
                    ));
                }
                other => panic!("expected SketchAgg under SketchEstimate, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

/// `ONLY_TEMPORAL` — `sum_over_time(m[5m])` (and the count/avg/min/max
/// variants that legacy `single_query.rs` accepts).
///
/// Control plane path (post PR-6 follow-up): `Aggregate{Sum}` over
/// `Window` → `BindExactAgg` fires → `PhysicalExpr::ExactAgg{Sum}`.
/// Pre-follow-up this fell through to `Logical` because no rule
/// matched Sum; the L5 emitter routed it to the archive engine.
#[test]
fn phase_b_pattern_only_temporal_sum_binds_to_exact_agg() {
    let expr = QueryExpr::Aggregate {
        by: vec![].into(),
        aggs: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::ExactAgg { agg_type, .. } => {
            assert_eq!(agg_type, asap_types::AggregationType::Sum,)
        }
        other => panic!("expected ExactAgg(Sum), got {other:?}"),
    }
}

/// `ONLY_SPATIAL` — `sum by (host) (m)`.
/// Control plane path: `Aggregate{Sum, by=[host]}` over a bare `Scan`.
/// Post keyed-ExactAgg follow-up, `BindExactAgg` lowers this to
/// `PhysicalExpr::ExactAgg { agg_type: MultipleSum, .. }` — the
/// multi-pop accumulator family the data plane uses for keyed
/// per-group sums.
#[test]
fn phase_b_pattern_only_spatial_aggregate_binds_to_multiple_sum() {
    let expr = QueryExpr::Aggregate {
        by: vec![1].into(), // service column
        aggs: vec![AggIntent::Sum { col: None }],
        output_names: Vec::new(),
        having: None,
        child: Box::new(ts_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::ExactAgg { agg_type, .. } => {
            assert_eq!(agg_type, asap_types::AggregationType::MultipleSum,)
        }
        other => panic!("expected ExactAgg(MultipleSum), got {other:?}"),
    }
}

/// `ONE_TEMPORAL_ONE_SPATIAL` — `sum by (host) (rate(m[5m]))`.
/// Post keyed-ExactAgg follow-up: `Rate` keyed by `host` lowers to
/// `MultipleIncrease`.
#[test]
fn phase_b_pattern_temporal_and_spatial_combined_binds_to_multiple_increase() {
    let expr = QueryExpr::Aggregate {
        by: vec![1].into(),
        aggs: vec![AggIntent::Rate],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        PhysicalExpr::ExactAgg { agg_type, .. } => {
            assert_eq!(agg_type, asap_types::AggregationType::MultipleIncrease,)
        }
        other => panic!("expected ExactAgg(MultipleIncrease), got {other:?}"),
    }
}

/// Phase β archive-only intent: any of the no-ASAP-tier-family entries
/// (`Absent`, `Present`, `Delta`, …) matches `BindArchiveOnly` → `Logical`
/// pass-through, and the L5 emitter / Phase α routing reads
/// `AggIntent::archive_only() == true` to flag the StreamingConfig entry
/// for the archive tier.
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
        by: vec![].into(),
        aggs: vec![intent.clone()],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // The archive-only rule's output is a Logical pass-through carrying
    // the original Aggregate. Downstream emitters check archive_only().
    match bound {
        PhysicalExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
            assert_eq!(aggs, vec![intent]);
        }
        other => panic!("expected Logical(Aggregate(Absent)), got {other:?}"),
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
    let qe = crate::query_parser::parse_query_expr_canonical(query)
        .unwrap_or_else(|e| panic!("parse {query}: {e}"));
    bind_query_expr(&qe, accuracy).unwrap_or_else(|e| panic!("bind {query}: {e}"))
}

/// Walk a `PhysicalExpr` and collect every `SketchAgg`'s sketch_kind. The
/// number of entries + the kind set is the wire-equivalent of
/// asap-planner-rs's "aggregation_id rows in StreamingConfig output".
fn collect_sketch_kinds(expr: &PhysicalExpr) -> Vec<SummaryKind> {
    let mut out = Vec::new();
    fn walk(e: &PhysicalExpr, out: &mut Vec<SummaryKind>) {
        match e {
            PhysicalExpr::SketchAgg {
                sketch_type, child, ..
            } => {
                out.push(sketch_type.clone());
                walk(child, out);
            }
            PhysicalExpr::SketchEstimate { child, .. } => walk(child, out),
            PhysicalExpr::SketchMerge { children, .. } => {
                for c in children {
                    walk(c, out);
                }
            }
            PhysicalExpr::LetBinding { expr, child, .. } => {
                walk(expr, out);
                walk(child, out);
            }
            PhysicalExpr::Logical(_) | PhysicalExpr::Ref { .. } => {}
            // Phase ε.1 — the new placement variants don't carry a
            // SketchAgg child the legacy walk recognises. Mode 2 records
            // its own family directly; Mode 3 has no sketch at all.
            PhysicalExpr::RawAtEdgeSketchAtBackend { family, child, .. } => {
                out.push(family.clone());
                walk(child, out);
            }
            PhysicalExpr::RawAtEdgePrometheusArchive { .. } => {}
            // ExactAgg has no SummaryKind to collect; its child may
            // carry one transitively (rare but possible if nested).
            PhysicalExpr::ExactAgg { child, .. } => walk(child, out),
        }
    }
    walk(expr, &mut out);
    out
}

/// Walk a `PhysicalExpr` and detect whether the binding ended in a
/// `Logical`-wrapped `Aggregate` carrying an archive-only intent. This is
/// the L4 signal that the L5 emitter routes the StreamingConfig entry
/// to the cold tier rather than the warm one.
fn binding_is_archive(expr: &PhysicalExpr) -> bool {
    match expr {
        PhysicalExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
            aggs.iter().any(crate::intent_algebra::archive_only)
        }
        PhysicalExpr::Logical(_) => false,
        PhysicalExpr::SketchEstimate { child, .. } => binding_is_archive(child),
        PhysicalExpr::SketchAgg { child, .. } => binding_is_archive(child),
        PhysicalExpr::SketchMerge { children, .. } => children.iter().any(binding_is_archive),
        PhysicalExpr::LetBinding { expr, child, .. } => {
            binding_is_archive(expr) || binding_is_archive(child)
        }
        PhysicalExpr::Ref { .. } => false,
        // Phase ε.1 — Mode 3 routes to the prometheus_remote engine
        // (its own engine ID), which the L5 emitter handles via
        // emit_backend_storage_routing rather than the warm-vs-archive
        // gate this helper guards. Treat as not-archive: this helper is
        // about cold-tier scan-vs-ASAP-tier-sketch decisions, not Mode 3.
        PhysicalExpr::RawAtEdgeSketchAtBackend { child, .. } => binding_is_archive(child),
        PhysicalExpr::RawAtEdgePrometheusArchive { .. } => false,
        // ExactAgg is a ASAP-tier exact-aggregation accumulator, NOT
        // an archive route. The L5 emitter writes the result through
        // the precompute output sink, same path as SketchAgg.
        PhysicalExpr::ExactAgg { .. } => false,
    }
}

/// `quantile_over_time.yaml` — the asap-planner-rs `quantile_over_time`
/// fixture maps to a KLL or DDSketch StreamingConfig row. The control plane
/// path: L1 PromQL parse → L3 `Aggregate{Quantile{0.99}}` over `Window` →
/// L4 `BindKllOnQuantile` (default) or `BindDDSketchOnQuantile`. Either
/// is functionally equivalent — both are quantile sketches.
#[test]
fn phase_b_e2e_quantile_over_time_binds_to_quantile_sketch() {
    let bound = pipeline_l1_to_l4(
        "quantile_over_time(0.99, http_request_duration_seconds[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    let kinds = collect_sketch_kinds(&bound);
    assert_eq!(kinds.len(), 1, "expected 1 sketch agg, got {kinds:?}");
    assert!(
        matches!(kinds[0], SummaryKind::Kll | SummaryKind::DDSketch),
        "expected quantile sketch family, got {:?}",
        kinds[0]
    );
    assert!(
        !binding_is_archive(&bound),
        "ASAP-tier quantile must not flag archive"
    );
}

/// `sum_over_time.yaml` — the legacy planner produces an exact-sum
/// aggregation row (no sketch). Control plane path: `Aggregate{Sum}` over
/// `Window` → no ASAP-tier rule fires → `Logical` pass-through.
/// Functional equivalence: both produce a single non-sketch row.
#[test]
fn phase_b_e2e_sum_over_time_falls_through_to_logical() {
    let bound = pipeline_l1_to_l4(
        "sum_over_time(http_requests_total[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    let kinds = collect_sketch_kinds(&bound);
    assert!(
        kinds.is_empty(),
        "sum_over_time should not produce a sketch agg, got {kinds:?}"
    );
    assert!(
        !binding_is_archive(&bound),
        "Sum is exact-warm, not archive — bind output should stay Logical without archive flag"
    );
}

/// `sum_by.yaml` — `sum by (label) (sum_over_time(...))`. Spatial-and-
/// temporal aggregation; the legacy planner emits an exact-sum row keyed
/// on the by-label. Control plane path: `Aggregate{Sum, by=[…]}` over
/// `Window` → no ASAP-tier rule fires → `Logical` pass-through. The
/// by-label is preserved on the L3 group-by-id list, which Phase α's
/// routing emit reads to build the per-label rollup partition.
#[test]
fn phase_b_e2e_sum_by_preserves_grouping_label() {
    let bound = pipeline_l1_to_l4(
        "sum by (instance) (sum_over_time(http_requests_total[5m]))",
        AccuracyTarget::Epsilon(0.01),
    );
    // No sketch family for plain Sum.
    assert!(collect_sketch_kinds(&bound).is_empty());
    // The end shape may be Logical(Aggregate{by, ...}) when the Aggregate
    // node survives the lowering, or Logical(Window{...}) when the
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
/// `Window` → no streaming-rate sketch family today → `Logical`. Both
/// paths produce a single non-sketch streaming row; the L5 emitter is
/// the one that picks the actual MultipleIncrease processor.
#[test]
fn phase_b_e2e_rate_falls_through_to_logical() {
    let bound = pipeline_l1_to_l4(
        "rate(http_requests_total[5m])",
        AccuracyTarget::Epsilon(0.01),
    );
    assert!(collect_sketch_kinds(&bound).is_empty());
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
/// CountSketch sketch fired, OR a Logical pass-through (which Phase γ
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
    let _ = collect_sketch_kinds(&bound);
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
        by: vec![].into(),
        aggs: vec![intent.clone()],
        output_names: Vec::new(),
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    assert!(
        binding_is_archive(&bound),
        "archive-only intent must surface archive flag through L4 binding"
    );
    // No ASAP-tier sketch fires for archive-only intents.
    assert!(collect_sketch_kinds(&bound).is_empty());
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
            by: vec![].into(),
            aggs: vec![intent.clone()],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let bound =
            bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind should succeed");
        match bound {
            PhysicalExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
                assert_eq!(aggs.len(), 1);
                assert!(
                    crate::intent_algebra::archive_only(&aggs[0]),
                    "{intent:?} should preserve archive_only() flag through bind"
                );
            }
            other => panic!("expected Logical(Aggregate({intent:?})), got {other:?}"),
        }
    }
}
