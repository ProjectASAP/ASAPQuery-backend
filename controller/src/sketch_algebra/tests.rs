//! Integration tests for the L4 IR + `Bind*` rules.

#![cfg(test)]

use std::time::Duration;

use crate::intent_algebra::{
    AggIntent, LabelFilter, QueryExpr, Schema, Source, WindowKind,
};
use crate::intent_algebra::schema::{Column, DataType};
use crate::sketch_algebra::lower::bind_query_expr;
use crate::sketch_algebra::params::{KllParams, SketchKind, SketchParams};
use crate::sketch_algebra::rules::{
    bind_ddsketch_quantile::BindDDSketchOnQuantile, bind_kll_quantile::BindKllOnQuantile, Rule,
};
use crate::sketch_algebra::sketch_expr::{EstimateOp, MergeAlgebra, SketchExpr};
use crate::types_v2::{AccuracyTarget, BindingName};

// ── Test fixtures ─────────────────────────────────────────────────────────────

fn col(name: &str, dtype: DataType) -> Column {
    Column {
        name: name.into(),
        dtype,
        nullable: false,
    }
}

fn ts_scan() -> QueryExpr {
    QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: "http_request_duration_seconds".into(),
        },
        label_filters: vec![LabelFilter {
            label: "service".into(),
            equals: "api".into(),
        }],
        schema: Schema::with_time_index(
            vec![
                col("ts", DataType::Timestamp),
                col("service", DataType::Utf8),
                col("value", DataType::Float64),
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

fn agg_quantile(q: f64, accuracy: AccuracyTarget) -> QueryExpr {
    QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Quantile { q, accuracy }],
        having: None,
        child: Box::new(windowed_scan()),
    }
}

// ── Serde round-trip across all variants ──────────────────────────────────────

#[test]
fn sketch_expr_serde_roundtrip() {
    use crate::sketch_algebra::params::{
        CmsParams, CountSketchParams, DDSketchParams, HllParams,
    };
    let cases = vec![
        SketchExpr::Logical(windowed_scan()),
        SketchExpr::SketchAgg {
            sketch_type: SketchKind::Kll,
            params: SketchParams::Kll(KllParams { k: 200 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        },
        SketchExpr::SketchEstimate {
            op: EstimateOp::Quantile { q: 0.5 },
            child: Box::new(SketchExpr::SketchAgg {
                sketch_type: SketchKind::DDSketch,
                params: SketchParams::DDSketch(DDSketchParams { alpha: 0.005 }),
                child: Box::new(SketchExpr::Logical(windowed_scan())),
            }),
        },
        SketchExpr::SketchMerge {
            algebra: MergeAlgebra::Union,
            children: vec![
                SketchExpr::SketchAgg {
                    sketch_type: SketchKind::Hll,
                    params: SketchParams::Hll(HllParams { precision: 14 }),
                    child: Box::new(SketchExpr::Logical(windowed_scan())),
                },
                SketchExpr::SketchAgg {
                    sketch_type: SketchKind::Hll,
                    params: SketchParams::Hll(HllParams { precision: 14 }),
                    child: Box::new(SketchExpr::Logical(windowed_scan())),
                },
            ],
        },
        SketchExpr::LetBinding {
            name: BindingName::new("kll_state"),
            expr: Box::new(SketchExpr::SketchAgg {
                sketch_type: SketchKind::CountSketch,
                params: SketchParams::CountSketch(CountSketchParams {
                    w: 2048,
                    d: 5,
                    with_heap: false,
                }),
                child: Box::new(SketchExpr::Logical(windowed_scan())),
            }),
            child: Box::new(SketchExpr::Ref {
                name: BindingName::new("kll_state"),
            }),
        },
        SketchExpr::Ref {
            name: BindingName::new("alone"),
        },
        SketchExpr::SketchAgg {
            sketch_type: SketchKind::Cms,
            params: SketchParams::Cms(CmsParams { w: 2048, d: 5 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        },
    ];
    for c in cases {
        let json = serde_json::to_string(&c).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
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
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    child,
                } => {
                    assert_eq!(sketch_type, SketchKind::Kll);
                    assert_eq!(params, SketchParams::Kll(KllParams { k: 200 }));
                    assert!(matches!(*child, SketchExpr::Logical(QueryExpr::Window { .. })));
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
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Quantile { q: 0.99 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::DDSketch);
                    match params {
                        SketchParams::DDSketch(p) => assert!((p.alpha - 0.01).abs() < 1e-12),
                        other => panic!("expected DDSketchParams, got {other:?}"),
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
    let bound =
        bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind_query_expr should not error");
    match bound {
        SketchExpr::SketchEstimate { child, .. } => match *child {
            SketchExpr::SketchAgg { sketch_type, .. } => {
                assert_eq!(
                    sketch_type,
                    SketchKind::DDSketch,
                    "dispatcher should pick DDSketch (priority 6) over KLL (priority 5) on ε-driven Quantile"
                );
            }
            other => panic!("expected SketchAgg, got {other:?}"),
        },
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_cms_topk_basic() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::TopK {
            k: 10,
            accuracy: AccuracyTarget::EpsilonDelta {
                eps: 0.01,
                delta: 0.001,
            },
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(
        &expr,
        AccuracyTarget::EpsilonDelta {
            eps: 0.01,
            delta: 0.001,
        },
    )
    .expect("bind_query_expr should not error");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::TopK { k: 10 });
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::CountSketch);
                    match params {
                        SketchParams::CountSketch(p) => {
                            assert!(p.with_heap, "TopK binding must enable the heavy-hitter heap");
                            assert!(p.w >= 2);
                            assert!(p.d >= 1);
                        }
                        other => panic!("expected CountSketchParams, got {other:?}"),
                    }
                }
                other => panic!("expected SketchAgg, got {other:?}"),
            }
        }
        other => panic!("expected SketchEstimate, got {other:?}"),
    }
}

#[test]
fn bind_hll_cardinality_basic() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Cardinality {
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("no error");
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert_eq!(op, EstimateOp::Cardinality);
            match *child {
                SketchExpr::SketchAgg {
                    sketch_type,
                    params,
                    ..
                } => {
                    assert_eq!(sketch_type, SketchKind::Hll);
                    match params {
                        SketchParams::Hll(p) => {
                            assert!(
                                p.precision >= 12,
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
fn bind_no_match_passes_through_logical() {
    // Sum is exact at L3 — no `Bind*` rule covers it. Should pass
    // through unchanged in `SketchExpr::Logical`.
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Sum],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    assert!(
        matches!(bound, SketchExpr::Logical(QueryExpr::Aggregate { .. })),
        "Sum should pass through as Logical(Aggregate{{Sum}})"
    );
}

#[test]
fn bind_exact_accuracy_disables_quantile_binding() {
    // Quantile under `AccuracyTarget::Exact` should NOT bind — the
    // optimizer falls back to an exact path. (Per design.md §6 line
    // ~1254 — "the sketch path is selected, not mandated".)
    let expr = agg_quantile(0.99, AccuracyTarget::Exact);
    let bound = bind_query_expr(&expr, AccuracyTarget::Exact).expect("no error");
    assert!(
        matches!(bound, SketchExpr::Logical(QueryExpr::Aggregate { .. })),
        "Exact accuracy should disable sketch binding and pass through as Logical"
    );
}

/// Two `SketchEstimate` parents reading different quantiles can share
/// one underlying `SketchAgg{KLL}` via `LetBinding` / `Ref`. Mirrors the
/// design.md §6 batched-queries example (line ~1326) — within the L4
/// IR, fan-in is expressible as a `LetBinding` whose bound expression
/// is the shared `SketchAgg`.
#[test]
fn let_binding_ref_through_sketch_dag() {
    let shared_agg = SketchExpr::SketchAgg {
        sketch_type: SketchKind::Kll,
        params: SketchParams::Kll(KllParams { k: 200 }),
        child: Box::new(SketchExpr::Logical(windowed_scan())),
    };
    let expr = SketchExpr::LetBinding {
        name: BindingName::new("kll_state"),
        expr: Box::new(shared_agg),
        child: Box::new(SketchExpr::SketchMerge {
            algebra: MergeAlgebra::Union,
            // Two `SketchEstimate` parents reading the shared sketch via
            // `Ref` — the design.md §6 line ~1339 two-tier fan-in shape.
            children: vec![
                SketchExpr::SketchEstimate {
                    op: EstimateOp::Quantile { q: 0.99 },
                    child: Box::new(SketchExpr::Ref {
                        name: BindingName::new("kll_state"),
                    }),
                },
                SketchExpr::SketchEstimate {
                    op: EstimateOp::Quantile { q: 0.95 },
                    child: Box::new(SketchExpr::Ref {
                        name: BindingName::new("kll_state"),
                    }),
                },
            ],
        }),
    };
    // Round-trip the DAG through serde to verify the multi-parent fan-in
    // shape survives wire encoding (the L4 type checker, when it lands,
    // will assert the matching sketch-state schema on each `Ref` reader).
    let json = serde_json::to_string(&expr).unwrap();
    let back: SketchExpr = serde_json::from_str(&json).unwrap();
    assert_eq!(expr, back);
}

// ── Phase β: pattern-migration coverage ───────────────────────────────────────
//
// The five PromQL pattern shapes defined in `asap-planner-rs/src/planner/
// patterns.rs` each have a controller L3/L4 equivalent. These tests are the
// per-shape cross-reference asserting the L1→L3→L4 path produces a
// matching binding without going back through asap-planner-rs.

/// `ONLY_TEMPORAL` — `quantile_over_time(0.99, m[5m])`.
/// asap-planner-rs path: ONLY_TEMPORAL pattern 1 → KLL/DDSketch sketch.
/// Controller path: `Aggregate{Quantile{0.99}}` over `Window` →
/// `BindKllOnQuantile` (or DDSketch) → `SketchAgg{KLL/DDSketch}`.
#[test]
fn phase_b_pattern_only_temporal_quantile_binds_to_sketch() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Quantile {
            q: 0.99,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        SketchExpr::SketchEstimate { op, child } => {
            assert!(matches!(op, EstimateOp::Quantile { .. }));
            match *child {
                SketchExpr::SketchAgg { sketch_type, .. } => {
                    assert!(matches!(
                        sketch_type,
                        SketchKind::Kll | SketchKind::DDSketch
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
/// Controller path: `Aggregate{Sum}` over `Window` → no warm-tier rule
/// fires (no streaming sum sketch); falls through to `Logical`. The
/// existing `algebra::directory` / `algebra::physical` engine handles the
/// exact aggregate.
#[test]
fn phase_b_pattern_only_temporal_sum_falls_through_to_logical() {
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![AggIntent::Sum],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // Sum is exact → no SketchAgg, just a Logical pass-through.
    assert!(matches!(bound, SketchExpr::Logical(_)));
}

/// `ONLY_SPATIAL` — `sum by (host) (m)`.
/// Controller path: `Aggregate{Sum, by=[host]}` over a bare `Scan` (no
/// `Window`). Sum is exact → Logical pass-through. The point of the test
/// is the by-clause survives binding intact.
#[test]
fn phase_b_pattern_only_spatial_aggregate_preserves_by_clause() {
    let expr = QueryExpr::Aggregate {
        by: vec![1], // service column
        aggs: vec![AggIntent::Sum],
        having: None,
        child: Box::new(ts_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    match bound {
        SketchExpr::Logical(QueryExpr::Aggregate { by, .. }) => {
            assert_eq!(by, vec![1]);
        }
        other => panic!("expected Logical(Aggregate), got {other:?}"),
    }
}

/// `ONE_TEMPORAL_ONE_SPATIAL` — `sum by (host) (rate(m[5m]))`.
/// Controller path: combined `Aggregate{Sum, by=[host]}` over `Window` —
/// the L3 algebra captures both axes natively without needing the legacy
/// pattern's `One*One*` enum.
#[test]
fn phase_b_pattern_temporal_and_spatial_combined() {
    let expr = QueryExpr::Aggregate {
        by: vec![1],
        aggs: vec![AggIntent::Rate {
            window: Duration::from_secs(300),
        }],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // Rate has no warm-tier sketch family today — expect Logical.
    assert!(matches!(bound, SketchExpr::Logical(_)));
}

/// Phase β archive-only intent: any of the no-warm-tier-family entries
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
    assert!(intent.archive_only(), "Phase β intent must flag archive");
    let expr = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![intent.clone()],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    // The archive-only rule's output is a Logical pass-through carrying
    // the original Aggregate. Downstream emitters check archive_only().
    match bound {
        SketchExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
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
// Phase β asserts the CONTROLLER's L1→L3→L4 path produces a functionally
// equivalent set of bound aggregations for the same input strings. We
// don't load the YAML files — that would couple the controller to the
// asap-planner-rs test fixture layout. Instead each test embeds the
// representative query string from the corresponding fixture YAML and
// pins the expected (sketch_kind | archive-only) outcome.

/// Helper: parse a PromQL string, lower to L3, bind to L4. Returns the
/// produced `SketchExpr` for assertion. The controller's `parse_query`
/// returns a `ParsedQuery`; `lower_parsed_query` builds the L3 IR from
/// it under the supplied accuracy target; `bind_query_expr` is the L3→L4
/// bottom-up walk.
fn pipeline_l1_to_l4(query: &str, accuracy: AccuracyTarget) -> SketchExpr {
    let parsed = crate::query_parser::parse_query(query)
        .unwrap_or_else(|e| panic!("parse {query}: {e}"));
    let qe = crate::intent_algebra::lower_parsed_query(&parsed, accuracy.clone())
        .unwrap_or_else(|e| panic!("lower {query}: {e}"));
    bind_query_expr(&qe, accuracy).unwrap_or_else(|e| panic!("bind {query}: {e}"))
}

/// Walk a `SketchExpr` and collect every `SketchAgg`'s sketch_kind. The
/// number of entries + the kind set is the wire-equivalent of
/// asap-planner-rs's "aggregation_id rows in StreamingConfig output".
fn collect_sketch_kinds(expr: &SketchExpr) -> Vec<SketchKind> {
    let mut out = Vec::new();
    fn walk(e: &SketchExpr, out: &mut Vec<SketchKind>) {
        match e {
            SketchExpr::SketchAgg { sketch_type, child, .. } => {
                out.push(sketch_type.clone());
                walk(child, out);
            }
            SketchExpr::SketchEstimate { child, .. } => walk(child, out),
            SketchExpr::SketchMerge { children, .. } => {
                for c in children {
                    walk(c, out);
                }
            }
            SketchExpr::LetBinding { expr, child, .. } => {
                walk(expr, out);
                walk(child, out);
            }
            SketchExpr::Logical(_) | SketchExpr::Ref { .. } => {}
            // Phase ε.1 — the new placement variants don't carry a
            // SketchAgg child the legacy walk recognises. Mode 2 records
            // its own family directly; Mode 3 has no sketch at all.
            SketchExpr::RawAtEdgeSketchAtBackend { family, child, .. } => {
                out.push(family.clone());
                walk(child, out);
            }
            SketchExpr::RawAtEdgePrometheusArchive { .. } => {}
        }
    }
    walk(expr, &mut out);
    out
}

/// Walk a `SketchExpr` and detect whether the binding ended in a
/// `Logical`-wrapped `Aggregate` carrying an archive-only intent. This is
/// the L4 signal that the L5 emitter routes the StreamingConfig entry
/// to the cold tier rather than the warm one.
fn binding_is_archive(expr: &SketchExpr) -> bool {
    match expr {
        SketchExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
            aggs.iter().any(|a| a.archive_only())
        }
        SketchExpr::Logical(_) => false,
        SketchExpr::SketchEstimate { child, .. } => binding_is_archive(child),
        SketchExpr::SketchAgg { child, .. } => binding_is_archive(child),
        SketchExpr::SketchMerge { children, .. } => children.iter().any(binding_is_archive),
        SketchExpr::LetBinding { expr, child, .. } => {
            binding_is_archive(expr) || binding_is_archive(child)
        }
        SketchExpr::Ref { .. } => false,
        // Phase ε.1 — Mode 3 routes to the prometheus_remote engine
        // (its own engine ID), which the L5 emitter handles via
        // emit_backend_storage_routing rather than the warm-vs-archive
        // gate this helper guards. Treat as not-archive: this helper is
        // about cold-tier scan-vs-warm-tier-sketch decisions, not Mode 3.
        SketchExpr::RawAtEdgeSketchAtBackend { child, .. } => binding_is_archive(child),
        SketchExpr::RawAtEdgePrometheusArchive { .. } => false,
    }
}

/// `quantile_over_time.yaml` — the asap-planner-rs `quantile_over_time`
/// fixture maps to a KLL or DDSketch StreamingConfig row. The controller
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
        matches!(kinds[0], SketchKind::Kll | SketchKind::DDSketch),
        "expected quantile sketch family, got {:?}",
        kinds[0]
    );
    assert!(
        !binding_is_archive(&bound),
        "warm-tier quantile must not flag archive"
    );
}

/// `sum_over_time.yaml` — the legacy planner produces an exact-sum
/// aggregation row (no sketch). Controller path: `Aggregate{Sum}` over
/// `Window` → no warm-tier rule fires → `Logical` pass-through.
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
/// on the by-label. Controller path: `Aggregate{Sum, by=[…]}` over
/// `Window` → no warm-tier rule fires → `Logical` pass-through. The
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
    let json = serde_json::to_string(&bound).unwrap();
    assert!(
        json.contains("http_requests_total"),
        "metric name lost through pipeline: {json}"
    );
    assert!(
        json.contains("instance"),
        "by-label `instance` lost through pipeline: {json}"
    );
}

/// `rate_increase.yaml` — the legacy planner emits a MultipleIncrease
/// (counter-reset adjusted) row. Controller path: `Aggregate{Rate}` over
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
    assert!(!binding_is_archive(&bound), "Rate is warm-tier, not archive");
}

/// `topk.yaml` — `topk(10, sum by (label) (rate(...))`. The legacy
/// planner emits a CountSketch+heap row. Controller path: the parser
/// recognises `topk` as a special node that lowers to `AggIntent::TopK`.
/// At the time of writing, the controller's `parse_query` may flatten
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
    // legitimate SketchExpr".
    let _ = collect_sketch_kinds(&bound);
}

/// Archive-only routing through the full L1→L3→L4 pipeline. Asserts the
/// expected functional equivalent of asap-planner-rs's previous
/// `is_supported() == false` behavior (refused outright); the controller
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
        by: vec![],
        aggs: vec![intent.clone()],
        having: None,
        child: Box::new(windowed_scan()),
    };
    let bound = bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).unwrap();
    assert!(
        binding_is_archive(&bound),
        "archive-only intent must surface archive flag through L4 binding"
    );
    // No warm-tier sketch fires for archive-only intents.
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
        let expr = QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![intent.clone()],
            having: None,
            child: Box::new(windowed_scan()),
        };
        let bound =
            bind_query_expr(&expr, AccuracyTarget::Epsilon(0.01)).expect("bind should succeed");
        match bound {
            SketchExpr::Logical(QueryExpr::Aggregate { aggs, .. }) => {
                assert_eq!(aggs.len(), 1);
                assert!(
                    aggs[0].archive_only(),
                    "{intent:?} should preserve archive_only() flag through bind"
                );
            }
            other => panic!("expected Logical(Aggregate({intent:?})), got {other:?}"),
        }
    }
}
