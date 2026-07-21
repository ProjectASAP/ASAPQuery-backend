//! Workload-level Common Sub-Expression Elimination.
//!
//! ## Phase 2 step 5 (docs/migration-plan-backend-plan.md)
//!
//! Relocated from `intent_algebra::cse` to `optimizer::cse` per the
//! Phase 0 decision: ASAPController places this pass in `crates/plan`
//! ("the cost-aware optimizer layer (L4 decisions) over the L3 intent
//! algebra"), not alongside the L3 IR type definitions — this repo's
//! `optimizer` module is that layer (R1-R12 in `engine.rs`, and this
//! pass's real consumer, `optimizer::cost::workload_cost`). The
//! algorithm is otherwise identical to `asap_plan::cse` (ASAPController's
//! `crates/plan/src/cse.rs`) — adopted directly per the tie-break rule,
//! including its structural-equality candidate scan (`QueryExpr:
//! PartialEq` on a `Vec`, not a `Debug`-string-keyed `HashMap` — `{:?}`
//! is not a guaranteed-injective, stable identity contract, this repo's
//! pre-merge implementation's own shortcut).
//!
//! [`CseWorkloadPlan`] now carries `asap_ir`'s own `BindingName`/`QueryId`
//! directly (`asap_ir::intent_algebra::{BindingName, QueryId}`), not
//! `types_v2`'s separate wrapper types — matching `asap_plan::cse` and
//! removing the boundary conversion the pre-merge version needed at
//! every `QueryExpr::Ref` construction site (`asap_ir`'s `BindingName`
//! was always the *only* type that could actually name a `Ref`/
//! `LetBinding`; the `types_v2` copy was this pass's own bookkeeping
//! type, not a real second identity). `optimizer::cost::WorkloadCostPlan`
//! — the pass's real consumer — moves with it for the same reason.
//! `types_v2::BindingName` remains the right type everywhere else it's
//! used today (`sketch_algebra::PhysicalExpr`'s own, unrelated L4
//! binding-name field; `pipeline.rs`'s `QueryId`) — this change is scoped
//! to the CSE↔cost-model boundary only.
//!
//! ## Regression note (R8 `CommonSubexprElim`)
//!
//! `optimizer::engine`'s R8 rule and this pass are *not* redundant,
//! despite both doing "common subexpression elimination": R8 dedupes
//! `Scan` leaves across the **branches of one `Merge` node inside a
//! single query tree**; this pass dedupes `Aggregate`-child subtrees
//! **across the root queries of a multi-query workload**. Intra-tree vs.
//! inter-tree — disjoint inputs, so relocating this pass doesn't change
//! what R8 fires on or when, and both stay.
//!
//! Per `control_plane/docs/design.md` §6 batched-queries example (line
//! ~1256 through ~1320). Multi-root planning hoists shared sub-DAGs into
//! `LetBinding`s so the cost model can credit the producer once.
//!
//! Legality is gated by [`cse_reuse_is_legal`](crate::intent_algebra::cse_reuse_is_legal):
//! a candidate sub-DAG only becomes a `LetBinding` when its output schema
//! has at least one `unique_keys` set (§6 line ~1356 — the field is
//! load-bearing for this pass).

#![allow(dead_code)]

use asap_ir::intent_algebra::{BindingName, QueryId};

use crate::intent_algebra::cse_reuse_is_legal;
use crate::intent_algebra::query_expr::QueryExpr;

/// Multi-root container produced by the CSE pass — mirrors the shape of
/// `types_v2::WorkloadPlan` (§6 batched-queries example) but uses the
/// real `intent_algebra::QueryExpr` rather than the JSON wire-shape
/// `QueryExprPlaceholder` string.
///
/// When `types_v2::WorkloadPlan` swaps the placeholder for the live
/// `QueryExpr`, this type collapses into that one without an API break.
#[derive(Debug, Clone, PartialEq)]
pub struct CseWorkloadPlan {
    /// Named shared producers, hoisted by `dedupe_subtrees`. Each is
    /// referenced by ≥2 roots via `QueryExpr::Ref`.
    pub bindings: Vec<(BindingName, QueryExpr)>,
    /// One root per input query, in input order.
    pub roots: Vec<(QueryId, QueryExpr)>,
}

/// Hoist sub-expressions that are *structurally identical* across ≥2
/// roots into shared `LetBinding`s, leaving each root with `Ref` sites
/// where the duplicate sub-tree used to live. Per design.md §6 line
/// ~1272 ("a workload-level CSE pass `core::lower::workload::dedupe_subtrees`").
///
/// **Scope.** Implements the basic case: identifies sub-trees that
/// appear verbatim (structural equality via `PartialEq`) in ≥2 root
/// inputs and hoists them. Schema-equivalent-but-not-identical
/// sub-trees, alpha-equivalence over inner `LetBinding`s, and recursive
/// nested CSE are deferred — they are the optimisation half of the pass
/// and live downstream of this PR.
///
/// **Legality.** A candidate sub-tree is hoisted only when
/// `cse_reuse_is_legal(&candidate.output_schema(), consumer_count)`
/// returns `Ok(())`. Sub-trees whose output schema lacks `unique_keys`
/// are left in place per design.md §6 line ~1356 (the deduper must be
/// conservative when it can't prove row identity).
pub fn dedupe_subtrees(roots: Vec<(QueryId, QueryExpr)>) -> CseWorkloadPlan {
    // Empty / single-root cases: no reuse possible. Return the inputs
    // verbatim with no bindings.
    if roots.len() < 2 {
        return CseWorkloadPlan {
            bindings: vec![],
            roots,
        };
    }

    // Identify candidate sub-trees that appear as the immediate child of
    // an `Aggregate` in ≥2 roots. The batched-queries example shape —
    // multiple `Aggregate`s sharing one `Window`-child producer — is the
    // case this lights up; richer detection is downstream. Grouped by
    // structural equality (`QueryExpr: PartialEq`), not `Debug` output —
    // `{:?}` is not a guaranteed-injective, stable identity contract. The
    // candidate set is one entry per distinct root child, so this linear
    // scan is bounded by the number of distinct queries.
    let mut candidate_counts: Vec<(QueryExpr, usize)> = Vec::new();
    for (_, root) in &roots {
        if let QueryExpr::Aggregate { child, .. } = root {
            // Skip already-aliased children (a `Ref` is not a candidate
            // for hoisting; it's already pointing at a binding).
            if matches!(**child, QueryExpr::Ref { .. }) {
                continue;
            }
            match candidate_counts
                .iter_mut()
                .find(|(e, _)| e == child.as_ref())
            {
                Some(entry) => entry.1 += 1,
                None => candidate_counts.push(((**child).clone(), 1)),
            }
        }
    }

    // Pick the most-shared legal candidate. This pass hoists at most one
    // binding per call; the "hoist all eligible candidates"
    // generalisation is a follow-up. Choosing the most-shared first
    // matches the design's priority — biggest reuse first.
    let mut chosen: Option<(QueryExpr, usize)> = None;
    for (expr, count) in candidate_counts.into_iter() {
        if count < 2 {
            continue;
        }
        // Legality gate: producer schema must have `unique_keys` for
        // ≥2 consumers to share it (design.md §6 line ~1356).
        let Ok(out_schema) = expr.output_schema() else {
            continue;
        };
        if cse_reuse_is_legal(&out_schema, count).is_err() {
            continue;
        }
        // Bigger fan-in wins; ties broken by input order.
        match &chosen {
            Some((_, best_count)) if *best_count >= count => {}
            _ => chosen = Some((expr, count)),
        }
    }

    let Some((shared_expr, _count)) = chosen else {
        // No eligible candidate — leave roots untouched.
        return CseWorkloadPlan {
            bindings: vec![],
            roots,
        };
    };

    // Rewrite each root: where the Aggregate's child equals
    // `shared_expr`, replace with `Ref { name: "shared_0" }`.
    let binding_name = BindingName::new("shared_0");
    let mut rewritten: Vec<(QueryId, QueryExpr)> = Vec::with_capacity(roots.len());
    for (qid, root) in roots {
        let new_root = match root {
            QueryExpr::Aggregate {
                by,
                aggs,
                output_names,
                having,
                child,
            } if *child == shared_expr => QueryExpr::Aggregate {
                by,
                aggs,
                output_names,
                having,
                child: Box::new(QueryExpr::Ref {
                    name: binding_name.clone(),
                }),
            },
            other => other,
        };
        rewritten.push((qid, new_root));
    }

    CseWorkloadPlan {
        bindings: vec![(binding_name, shared_expr)],
        roots: rewritten,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::agg_intent::AggIntent;
    use crate::intent_algebra::query_expr::{LabelFilter, Source, WindowKind};
    use crate::intent_algebra::schema::{Column, DataType, Schema};
    use crate::types_v2::AccuracyTarget;
    use std::time::Duration;

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

    /// Empty input → empty output (no bindings, no roots).
    #[test]
    fn dedupe_subtrees_empty_input() {
        let out = dedupe_subtrees(vec![]);
        assert!(out.bindings.is_empty());
        assert!(out.roots.is_empty());
    }

    /// Single-root input → no reuse possible, returned verbatim.
    #[test]
    fn dedupe_subtrees_single_root_passthrough() {
        let q = QueryExpr::Aggregate {
            by: vec![1].into(),
            aggs: vec![AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let out = dedupe_subtrees(vec![(QueryId::new("q1"), q.clone())]);
        assert!(out.bindings.is_empty());
        assert_eq!(out.roots.len(), 1);
        assert_eq!(out.roots[0].1, q);
    }

    /// Issue #115 (ASAPController): CSE dedupes on `AggIntent` equality.
    /// Before `Quantile` carried its input column, `median(a)` and
    /// `median(b)` compared equal, so two aggregates over *different*
    /// columns collapsed into one — a wrong answer, not just a missed
    /// optimisation. The merged `AggIntent::Quantile { col: Option<ColumnId>, .. }`
    /// already carries the column, so this is a regression guard, not new
    /// behavior this repo needed to add.
    #[test]
    fn quantiles_over_different_columns_do_not_dedupe() {
        let mk = |col: usize| QueryExpr::Aggregate {
            by: vec![1].into(),
            aggs: vec![AggIntent::Quantile {
                col: Some(col),
                q: 0.5,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let (a, b) = (mk(2), mk(3));
        assert_ne!(a, b, "distinct-column quantiles must not compare equal");

        let out = dedupe_subtrees(vec![(QueryId::new("q1"), a), (QueryId::new("q2"), b)]);
        assert_ne!(
            out.roots[0].1, out.roots[1].1,
            "aggregates over different columns must not collapse"
        );
    }

    /// design.md §6 batched-queries example basic case: two queries with
    /// identical `Window` sub-trees — the deduper hoists the shared
    /// producer into a binding and rewrites each root to reference it.
    #[test]
    fn dedupe_subtrees_basic() {
        let q1 = QueryExpr::Aggregate {
            by: vec![1].into(),
            aggs: vec![AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let q2 = QueryExpr::Aggregate {
            by: vec![1].into(),
            aggs: vec![AggIntent::Quantile {
                col: None,
                q: 0.95,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };

        let out = dedupe_subtrees(vec![(QueryId::new("q1"), q1), (QueryId::new("q2"), q2)]);

        // One binding hoisted, two roots rewritten to `Ref { name: "shared_0" }`.
        assert_eq!(out.bindings.len(), 1);
        assert_eq!(out.bindings[0].0, BindingName::new("shared_0"));
        assert_eq!(out.bindings[0].1, windowed_scan());

        for (_, root) in &out.roots {
            match root {
                QueryExpr::Aggregate { child, .. } => assert_eq!(
                    **child,
                    QueryExpr::Ref {
                        name: BindingName::new("shared_0"),
                    },
                    "Aggregate child should be a Ref to the hoisted binding"
                ),
                other => panic!("expected Aggregate root, got {other:?}"),
            }
        }
    }

    /// Two roots with *different* sub-expressions — no shared producer,
    /// no binding hoisted, roots returned untouched.
    #[test]
    fn dedupe_subtrees_no_shared_subexpr() {
        let q1 = QueryExpr::Aggregate {
            by: vec![].into(),
            aggs: vec![AggIntent::Sum { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        // q2 uses a different scan (different metric) → structurally
        // distinct → no hoisting.
        let other_scan = QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "different_metric".into(),
            },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    col("ts", DataType::Timestamp),
                    col("service", DataType::Utf8),
                    col("value", DataType::Float64),
                ],
                0,
                vec![vec![0, 1]],
            ),
        };
        let q2 = QueryExpr::Aggregate {
            by: vec![].into(),
            aggs: vec![AggIntent::Max { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(other_scan),
            }),
        };

        let out = dedupe_subtrees(vec![
            (QueryId::new("q1"), q1.clone()),
            (QueryId::new("q2"), q2.clone()),
        ]);
        assert!(out.bindings.is_empty(), "no shared subexpr → no binding");
        assert_eq!(out.roots[0].1, q1);
        assert_eq!(out.roots[1].1, q2);
    }

    /// Schema without `unique_keys` → CSE refuses to share even if
    /// structurally identical (ASAPController's own regression case for
    /// the legality gate, ported alongside the algorithm).
    #[test]
    fn dedupe_subtrees_no_shared_subexpr_when_unique_keys_absent() {
        let scan_no_uk = QueryExpr::Scan {
            source: Source::TimeSeries { metric: "m".into() },
            predicates: vec![],
            schema: Schema::with_time_index(
                vec![
                    col("ts", DataType::Timestamp),
                    col("value", DataType::Float64),
                ],
                0,
                vec![],
            ),
        };
        let mk = || QueryExpr::Aggregate {
            by: vec![].into(),
            aggs: vec![AggIntent::Sum { col: None }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(scan_no_uk.clone()),
        };
        let out = dedupe_subtrees(vec![(QueryId::new("q1"), mk()), (QueryId::new("q2"), mk())]);
        assert!(out.bindings.is_empty(), "no unique_keys → no hoisting");
    }
}
