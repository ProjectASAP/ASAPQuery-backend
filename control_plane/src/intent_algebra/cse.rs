//! Workload-level Common Sub-Expression Elimination.
//!
//! Per `control_plane/docs/design.md` §6 batched-queries example (line ~1256
//! through ~1320). Multi-root planning hoists shared sub-DAGs into
//! `LetBinding`s so the cost model can credit the producer once.
//!
//! Phase F lands the **gate + a basic implementation** that handles the
//! literal "≥2 root queries with identical sub-expressions" case from the
//! design — sufficient to make the workload-cost path observable end-to-
//! end. The fully-general CSE algorithm (alpha-equivalence across
//! `LetBinding` rebinding, schema-merge across compatible-but-not-identical
//! shapes, cross-binding nested CSE) is deferred per design.md §6 line
//! ~562 — it is a downstream optimisation pass, not part of the IR
//! contract Phase F is delivering.
//!
//! Legality is gated by [`cse_reuse_is_legal`](super::schema::cse_reuse_is_legal):
//! a candidate sub-DAG only becomes a `LetBinding` when its output schema
//! has at least one `unique_keys` set (§6 line ~1356 — the field is
//! load-bearing for this pass).

#![allow(dead_code)]

use std::collections::HashMap;

use crate::intent_algebra::query_expr::QueryExpr;
use crate::intent_algebra::schema::cse_reuse_is_legal;
use crate::types_v2::{BindingName, QueryId};

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
/// **Phase F scope.** Implements the basic case: identifies sub-trees
/// that appear verbatim (structural equality via `PartialEq`) in ≥2 root
/// inputs and hoists them. Schema-equivalent-but-not-identical sub-trees,
/// alpha-equivalence over inner `LetBinding`s, and recursive nested CSE
/// are deferred — they are the optimisation half of the pass and live
/// downstream of this PR.
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

    // Phase F: identify candidate sub-trees that appear as the immediate
    // child of an `Aggregate` in ≥2 roots. The batched-queries example
    // shape — multiple `Aggregate`s sharing one `Window`-child producer
    // — is the case Phase F lights up; richer detection is downstream.
    let mut candidate_counts: HashMap<String, (QueryExpr, usize)> = HashMap::new();
    for (_, root) in &roots {
        if let QueryExpr::Aggregate { child, .. } = root {
            // Skip already-aliased children (a `Ref` is not a candidate
            // for hoisting; it's already pointing at a binding).
            if matches!(**child, QueryExpr::Ref { .. }) {
                continue;
            }
            // Use the Debug representation as a structural-key proxy.
            // Cheap to compute and matches `PartialEq` for
            // `QueryExpr` → adequate for the Phase F basic case.
            let key = format!("{child:?}");
            let entry = candidate_counts
                .entry(key)
                .or_insert_with(|| ((**child).clone(), 0));
            entry.1 += 1;
        }
    }

    // Pick the most-shared legal candidate. Phase F hoists at most one
    // binding per call; the "hoist all eligible candidates" generalisation
    // is a follow-up. Choosing the most-shared first matches the design's
    // priority — biggest reuse first.
    let mut chosen: Option<(QueryExpr, usize)> = None;
    for (_key, (expr, count)) in candidate_counts.into_iter() {
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
        // Bigger fan-in wins; ties broken arbitrarily (HashMap order).
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
                having,
                child,
            } if *child == shared_expr => QueryExpr::Aggregate {
                by,
                aggs,
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
            by: vec![1],
            aggs: vec![AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(windowed_scan()),
        };
        let out = dedupe_subtrees(vec![(QueryId::new("q1"), q.clone())]);
        assert!(out.bindings.is_empty());
        assert_eq!(out.roots.len(), 1);
        assert_eq!(out.roots[0].1, q);
    }

    /// design.md §6 batched-queries example basic case: two queries with
    /// identical `Window` sub-trees — the deduper hoists the shared
    /// producer into a binding and rewrites each root to reference it.
    #[test]
    fn dedupe_subtrees_basic() {
        let q1 = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(windowed_scan()),
        };
        let q2 = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Quantile {
                q: 0.95,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
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
            by: vec![],
            aggs: vec![AggIntent::Sum],
            having: None,
            child: Box::new(windowed_scan()),
        };
        // q2 uses a different scan (different metric) — Debug repr
        // differs → no hoisting.
        let other_scan = QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "different_metric".into(),
            },
            label_filters: vec![],
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
            by: vec![],
            aggs: vec![AggIntent::Max],
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
}
