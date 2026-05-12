//! Step γ4 bridge: legacy `QueryExpr::TopK { k, by, input }` → one of two
//! canonical shapes per `design.md` §6 "What was removed" row 1.
//!
//! ## Background
//!
//! `legacy_expr::QueryExpr::TopK` collapses two distinct concepts onto a
//! single variant: (a) the *intent* "compute heavy hitters" (which has its
//! own sketch primitive — SpaceSaving, CMS-with-heap, Misra-Gries), and
//! (b) the generic operator pair `Sort + Limit` (which doesn't). The
//! canonical L3 splits them so L4 binding rules can fire on the *intent*
//! rather than on a syntactic shape:
//!
//! | Source pattern | Canonical shape |
//! |---|---|
//! | PromQL `topk(k, expr)`, SQL `ORDER BY count DESC LIMIT k` | [`AggIntent::TopK { k, accuracy }`] under `Aggregate` |
//! | SQL `ORDER BY name LIMIT 10`, `ORDER BY ts DESC LIMIT 1` | [`QueryExpr::Sort`] + [`QueryExpr::Limit`] |
//!
//! Both retained — they describe different things (design.md §6 line ~477).
//!
//! ## Approach
//!
//! Strategy (c) per γ1: the legacy `TopK` variant survives unchanged as
//! the L2 emit shape. This bridge is a one-way builder helper that returns
//! the canonical-shape data ([`BridgedTopK`]) so a downstream consumer can
//! decide which canonical form to bind against without having to mutate
//! the legacy tree. The legacy variant retires only when every consumer
//! migrates to canonical pattern-matching (Step γ7 or later).
//!
//! ## Classification heuristic
//!
//! The split between "heavy-hitter intent" vs "generic Sort+Limit" cannot
//! be done purely from the `(k, by)` fields — both PromQL `topk(10, …)`
//! and SQL `ORDER BY count DESC LIMIT 10` produce the same legacy shape.
//! The distinguishing signal is the surrounding context (whether the
//! ordering key is an aggregate column produced by the input subtree).
//!
//! Step γ4 ships the conservative default: **return [`BridgedTopK::
//! HeavyHitter`] unconditionally**. Rationale: PromQL is the dominant L1
//! in this codebase today (`controller/src/query_parser/promql.rs:231`
//! is the only construction site that lowers a language-level `topk(…)`
//! to the legacy variant — see also `optimizer/engine.rs:485` where the
//! optimizer recognises `ORDER BY DESC LIMIT k` and lifts it to `TopK`,
//! again expressing heavy-hitter intent). The pure-syntactic generic
//! case (`ORDER BY name LIMIT 10`) goes through legacy `Sort` + `Limit`
//! variants directly, never reaching this bridge.
//!
//! Step γ7 will refine this via a context flag once `Aggregate` /
//! `Window` migrate and the bridge has a sibling subtree to inspect.

use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::column_resolution::ResolveError;
use crate::intent_algebra::query_expr::ColumnRef;
use crate::intent_algebra::schema::{ColumnId, Schema};
use crate::types_v2::AccuracyTarget;

/// Canonical-shape data extracted from a legacy `TopK` node. See module
/// doc-comment for the heavy-hitter vs Sort+Limit distinction.
#[derive(Debug, Clone, PartialEq)]
pub enum BridgedTopK {
    /// PromQL-style heavy-hitter top-k. Maps to [`AggIntent::TopK`] under
    /// an `Aggregate` node. The dedicated sketch primitive (SpaceSaving,
    /// CMS-with-heap, Misra-Gries) computes this in a single pass.
    HeavyHitter {
        /// Number of heavy hitters requested.
        k: usize,
        /// Partition columns (`by (...)` in PromQL, `GROUP BY` in SQL),
        /// resolved positionally against the inherited schema. Empty →
        /// global heavy hitters.
        by: Vec<ColumnId>,
        /// The canonical intent itself — always [`AggIntent::TopK`].
        /// Carried inline so the consumer can attach it directly to an
        /// `Aggregate.aggs` slot or hand it to an L4 `BindCmsTopK` rule.
        intent: AggIntent,
    },
    /// SQL-style ordering+limit on a non-aggregate column. Maps to
    /// [`crate::intent_algebra::query_expr::QueryExpr::Sort`] +
    /// [`crate::intent_algebra::query_expr::QueryExpr::Limit`]. No sketch
    /// alternative — the operator pair survives at L3 unchanged.
    SortLimit {
        /// Limit value (the `LIMIT n` part).
        k: usize,
        /// Sort-key columns, resolved positionally. Sort direction is
        /// `DESC` for the canonical heavy-hitter alternative and is left
        /// to the caller to attach to the `SortKey` it builds — the
        /// legacy `TopK.by` field doesn't carry direction.
        by: Vec<ColumnId>,
    },
}

/// Errors returned by [`bridge_topk`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BridgeError {
    /// One of the `by` columns didn't resolve against the inherited
    /// schema. Conservative fallback: callers keep the legacy `TopK` at
    /// this site and log the deferral.
    #[error("TopK by-key resolution failed: {0}")]
    By(#[from] ResolveError),
}

/// Translate the canonical-shape fields of a legacy
/// `legacy_expr::QueryExpr::TopK { k, by, input }` against the inherited
/// schema.
///
/// Returns the bridged data; the legacy `input` subtree is left to the
/// caller's own recursion (see γ1 bridge module doc-comment for the
/// "child intentionally NOT carried" rationale).
///
/// ## Classification (Step γ4 default)
///
/// Currently always returns [`BridgedTopK::HeavyHitter`] — see the module
/// doc-comment "Classification heuristic" section. Step γ7 will introduce
/// a context flag.
///
/// ## Schema flow
///
/// `schema` is the schema in scope at the legacy `TopK` node — its INPUT
/// schema. The bridge resolves each `by` `ColumnRef::Named(...)` against
/// it positionally. `TopK` is otherwise schema-preserving (it filters
/// rows but doesn't change the column set), so consumers can use the
/// same `schema` when descending into `input`.
///
/// ## Accuracy default
///
/// The legacy `TopK` doesn't carry an accuracy target — the legacy
/// planner treated it as a fixed-budget heavy-hitter sketch with an
/// implicit ε. The bridge defaults to [`AccuracyTarget::Epsilon`] with
/// the planner's historical 5% bound (`0.05`); Step γ7 / γ8 will plumb a
/// real accuracy SLA when L1 lowering surfaces one.
pub fn bridge_topk(
    k: usize,
    by: &[ColumnRef],
    schema: &Schema,
) -> Result<BridgedTopK, BridgeError> {
    let by_ids: Vec<ColumnId> = by
        .iter()
        .map(|c| resolve_canonical_column_ref(c, schema))
        .collect::<Result<Vec<_>, _>>()?;

    // Step γ4: heavy-hitter is the dominant L1 form in this codebase. See
    // the module doc-comment "Classification heuristic" section.
    Ok(BridgedTopK::HeavyHitter {
        k,
        by: by_ids,
        intent: AggIntent::TopK {
            k,
            accuracy: AccuracyTarget::Epsilon(0.05),
        },
    })
}

/// Resolve a canonical [`ColumnRef`] against a [`Schema`]. Mirrors
/// [`crate::intent_algebra::column_resolution::resolve_column_ref`] (which
/// takes the legacy [`crate::intent_algebra::legacy_expr::ColumnRef`])
/// but operates on the canonical [`ColumnRef`] re-exported by
/// `intent_algebra::query_expr`. The two enums have identical variants
/// today; the canonical one is what L3 nodes consume.
fn resolve_canonical_column_ref(
    col: &ColumnRef,
    schema: &Schema,
) -> Result<ColumnId, ResolveError> {
    match col {
        ColumnRef::Named(name) => schema
            .column_id(name)
            .ok_or_else(|| ResolveError::NotFound {
                name: name.clone(),
                available: schema.columns.iter().map(|c| c.name.clone()).collect(),
            }),
        ColumnRef::SampleValue => {
            schema
                .column_id("value")
                .ok_or_else(|| ResolveError::NoSampleValue {
                    available: schema.columns.iter().map(|c| c.name.clone()).collect(),
                })
        }
        ColumnRef::Wildcard => Err(ResolveError::WildcardNotPositional),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::column_resolution::infer_source_schema;
    use crate::intent_algebra::schema::{Column, DataType};

    fn schema_with_host() -> Schema {
        let mut s = infer_source_schema("m");
        s.columns.push(Column {
            name: "host".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        s.columns.push(Column {
            name: "dc".into(),
            dtype: DataType::Utf8,
            nullable: false,
        });
        s
    }

    #[test]
    fn bridge_global_topk_resolves_empty_by() {
        // PromQL `topk(5, m)` — no partition columns.
        let s = infer_source_schema("m");
        let b = bridge_topk(5, &[], &s).unwrap();
        match b {
            BridgedTopK::HeavyHitter { k, by, intent } => {
                assert_eq!(k, 5);
                assert!(by.is_empty(), "expected no partition keys");
                assert!(matches!(
                    intent,
                    AggIntent::TopK {
                        k: 5,
                        accuracy: AccuracyTarget::Epsilon(_),
                    }
                ));
            }
            other => panic!("expected HeavyHitter, got {other:?}"),
        }
    }

    #[test]
    fn bridge_resolves_single_by_key() {
        // PromQL `topk(10, m) by (host)`.
        let s = schema_with_host();
        let b = bridge_topk(
            10,
            &[ColumnRef::Named("host".into())],
            &s,
        )
        .unwrap();
        match b {
            BridgedTopK::HeavyHitter { k, by, intent } => {
                assert_eq!(k, 10);
                // "host" sits at position 2 (after ts, value).
                assert_eq!(by, vec![2usize]);
                assert!(matches!(intent, AggIntent::TopK { k: 10, .. }));
            }
            other => panic!("expected HeavyHitter, got {other:?}"),
        }
    }

    #[test]
    fn bridge_resolves_multi_key_by() {
        // PromQL `topk(3, m) by (host, dc)` — multi-column partition.
        let s = schema_with_host();
        let b = bridge_topk(
            3,
            &[
                ColumnRef::Named("host".into()),
                ColumnRef::Named("dc".into()),
            ],
            &s,
        )
        .unwrap();
        match b {
            BridgedTopK::HeavyHitter { by, .. } => {
                assert_eq!(by, vec![2usize, 3usize]);
            }
            other => panic!("expected HeavyHitter, got {other:?}"),
        }
    }

    #[test]
    fn bridge_unresolvable_column_surfaces_resolve_error() {
        // Asking for a column not in the schema must surface a Resolve
        // error so callers keep the legacy `TopK` at this site.
        let s = schema_with_host();
        let err = bridge_topk(
            5,
            &[ColumnRef::Named("missing_label".into())],
            &s,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            BridgeError::By(ResolveError::NotFound { .. })
        ));
    }

    #[test]
    fn bridge_sample_value_resolves_to_value_column() {
        // Some lowerers emit `ColumnRef::SampleValue` for the implicit
        // PromQL metric value. It must resolve to the "value" column.
        let s = infer_source_schema("m");
        let b = bridge_topk(
            7,
            &[ColumnRef::SampleValue],
            &s,
        )
        .unwrap();
        match b {
            BridgedTopK::HeavyHitter { by, .. } => {
                // "value" sits at position 1 (after ts).
                assert_eq!(by, vec![1usize]);
            }
            other => panic!("expected HeavyHitter, got {other:?}"),
        }
    }

    #[test]
    fn bridge_wildcard_is_not_positional() {
        // `ColumnRef::Wildcard` (`COUNT(*)`) has no positional id —
        // resolution must error.
        let s = infer_source_schema("m");
        let err = bridge_topk(5, &[ColumnRef::Wildcard], &s).unwrap_err();
        assert!(matches!(
            err,
            BridgeError::By(ResolveError::WildcardNotPositional)
        ));
    }

    #[test]
    fn bridged_intent_carries_topk_with_default_accuracy() {
        // Verify the embedded AggIntent::TopK carries the configured
        // accuracy default (Epsilon(0.05)) and matches the requested k.
        let s = infer_source_schema("m");
        let b = bridge_topk(42, &[], &s).unwrap();
        match b {
            BridgedTopK::HeavyHitter { intent, .. } => match intent {
                AggIntent::TopK { k, accuracy } => {
                    assert_eq!(k, 42);
                    assert!(matches!(accuracy, AccuracyTarget::Epsilon(eps) if (eps - 0.05).abs() < 1e-9));
                }
                other => panic!("expected AggIntent::TopK, got {other:?}"),
            },
            other => panic!("expected HeavyHitter, got {other:?}"),
        }
    }
}
