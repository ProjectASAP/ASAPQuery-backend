//! Layer 3 aggregation-intent vocabulary.
//!
//! Per `control_plane/docs/design.md` §6 "`AggIntent` — what to compute, not
//! how" (around line ~468). L3 carries intent ("compute a quantile to
//! ε=0.01 accuracy"). The choice between `HashAgg` / `SortAgg` /
//! `SketchAgg(KLL{k=200})` is made by L4 cost-aware rules, not encoded
//! here.
//!
//! Intent vs operator distinction. `AggIntent::TopK` is an *intent* (a
//! dedicated heavy-hitter sketch primitive — SpaceSaving, CMS-with-heap
//! — computes it in a single pass). The generic `Sort + Limit` operator
//! pair survives in `QueryExpr` for non-heavy-hitter cases (`ORDER BY
//! name LIMIT 10`). L1→L2→L3 lowering picks one or the other
//! deterministically.
//!
//! No `QuantileOverTime` intent. The window is fully captured by the
//! surrounding `QueryExpr::Window` node; the quantile *operation* is the
//! same regardless. PromQL `quantile_over_time(0.99, m[5m])` lowers to
//! `Window{size=5m} → Aggregate{aggs:[Quantile{q=0.99}]}`.
//!
//! `Rate` and `Increase` survive that argument because they include
//! PromQL's counter-reset adjustment, a non-trivial transformation that
//! exact `Sum` does not perform.

#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::intent_algebra::schema::{Column, DataType};
use crate::types_v2::AccuracyTarget;

/// "What to compute" at L3 — vocabulary the planner pivots on. See module
/// doc for the intent vs operator distinction.
///
/// Variants intentionally mirror `design.md` §6 line ~468; data-model-
/// agnostic intents come first, time-series-streaming derivatives
/// (`Rate` / `Increase`) come last.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AggIntent {
    // ── Data-model-agnostic ──────────────────────────────────────────────
    /// COUNT(*) / `count` — number of rows / samples per group.
    /// `accuracy: Exact` selects an exact counter; `Epsilon` / `EpsilonDelta`
    /// unlock CMS / linear-counting sketch families.
    Count {
        accuracy: AccuracyTarget,
    },
    /// SUM(col). Always exact at L3 — no approximation intent for `Sum`
    /// in the catalog (`design.md` §6 line ~485).
    Sum,
    /// Per-group minimum — exact at L3.
    Min,
    /// Per-group maximum — exact at L3.
    Max,
    /// Arithmetic mean. Exact at L3; sketch backends fold this onto a
    /// `Quantile{q=0.5}` only when the cost model allows the relaxation.
    Avg,
    /// Compute the φ-th quantile (0 ≤ q ≤ 1) to the given accuracy.
    /// Sketch families: KLL, DDSketch, t-digest.
    Quantile {
        q: f64,
        accuracy: AccuracyTarget,
    },
    /// Heavy-hitter top-k. Distinct from generic `Sort + Limit` because
    /// a dedicated sketch primitive (SpaceSaving, CMS-with-heap,
    /// Misra-Gries) computes it as a single operation. L1→L2→L3 lowering
    /// produces this when it recognises `topk(k, …)` (PromQL) or
    /// `ORDER BY count DESC LIMIT k` (SQL).
    TopK {
        k: usize,
        accuracy: AccuracyTarget,
    },
    /// COUNT DISTINCT — number of distinct values in the input column,
    /// to the given accuracy. Sketch families: HLL, theta-sketch.
    Cardinality {
        accuracy: AccuracyTarget,
    },
    /// Frequency of a key in the input — `count(*) WHERE key = k` modeled
    /// as a sketch query. Sketch families: CMS, count-min-log.
    Frequency {
        accuracy: AccuracyTarget,
    },

    // ── Time-series streaming derivatives ────────────────────────────────
    /// Per-second average derivative with PromQL's counter-reset
    /// adjustment. Specialized — exact `Sum / Count over Window` does
    /// NOT serve this intent.
    Rate {
        window: Duration,
    },
    /// Cumulative increase over the given window with counter-reset
    /// adjustment. Specialized — see `Rate` above.
    Increase {
        window: Duration,
    },

    // ── Archive-only intents (Phase β migration) ─────────────────────────
    // Intents below have no warm-tier sketch family today; the L4 binder
    // emits a `SketchExpr::Logical` pass-through and the L5 emitter routes
    // them to the cold archive tier (Gorilla / Thanos). Adding a streaming
    // sketch family for any of these is a follow-up — the L3 vocabulary
    // captures the intent so the routing decision is layered above intent.
    //
    // Note: `histogram_quantile(φ, …)` is NOT an L3 intent — it's a PromQL
    // /MetricsQL language-level operator. Per Step γ5 of the legacy_expr
    // migration, the PromQL parser substitutes it directly into a plain
    // `Aggregate { Quantile(φ) }` (which lowers to
    // `AggIntent::Quantile { q, accuracy }`); bucket-aware handling is a
    // physical-planner concern, not an L3 intent.
    //
    /// `absent(vector_selector)` — 1 iff the selector matched no series in
    /// the evaluation window, no value otherwise. Routed to archive: the
    /// engine answers it directly off the index.
    Absent,
    /// `present_over_time(m[range])` — 1 iff the selector had at least one
    /// sample in the window. Inverse of `Absent`. Archive-routed.
    Present,
    /// `delta(m[range])` — last − first sample within the window, NO
    /// counter-reset adjustment. Distinct from [`AggIntent::Increase`].
    Delta {
        window: Duration,
    },
    /// `deriv(m[range])` — per-second derivative via simple linear
    /// regression. Archive-routed (no streaming sketch).
    Deriv {
        window: Duration,
    },
    /// `predict_linear(m[range], t)` — linear-regression prediction `t`
    /// seconds into the future. Archive-routed.
    PredictLinear {
        window: Duration,
        ahead: Duration,
    },
    /// `holt_winters(m[range], sf, tf)` — exponential-smoothing forecast.
    /// Archive-routed.
    HoltWinters {
        window: Duration,
        smoothing_factor: f64,
        trend_factor: f64,
    },
    /// `idelta(m[range])` — `last − second_to_last`, instant delta. No
    /// streaming sketch.
    Idelta {
        window: Duration,
    },
    /// `irate(m[range])` — instant per-second rate computed from the last
    /// two samples. Counter-reset adjusted but evaluated point-wise; not
    /// the same as the streaming [`AggIntent::Rate`].
    Irate {
        window: Duration,
    },
    /// `resets(m[range])` — count of counter resets over the window.
    /// Archive-routed.
    Resets {
        window: Duration,
    },
    /// `changes(m[range])` — count of value changes over the window.
    /// Archive-routed.
    Changes {
        window: Duration,
    },
}

impl AggIntent {
    /// True iff this intent has no warm-tier (streaming sketch) binding
    /// today. `false` means a `Bind*` rule may match. `true` means the
    /// L5 emitter routes the intent to the cold-store / archive tier.
    ///
    /// Per Phase β orchestrator spec, the PromQL functions that were
    /// previously refused outright by `asap-planner-rs::single_query::
    /// is_supported()` (everything outside the 5 patterns) all return
    /// `true` here. Adding a sketch family for any of them is a future
    /// PR — flipping the flag to `false` is the single point of change.
    ///
    /// Note: `histogram_quantile(...)` was previously listed as
    /// archive-only here but is no longer an `AggIntent` variant —
    /// it's a PromQL/MetricsQL language-level operator. Per Step γ5,
    /// the PromQL parser substitutes it into a plain
    /// `Aggregate { Quantile(φ) }`, which lowers to
    /// `AggIntent::Quantile { q, .. }`.
    pub fn archive_only(&self) -> bool {
        matches!(
            self,
            AggIntent::Absent
                | AggIntent::Present
                | AggIntent::Delta { .. }
                | AggIntent::Deriv { .. }
                | AggIntent::PredictLinear { .. }
                | AggIntent::HoltWinters { .. }
                | AggIntent::Idelta { .. }
                | AggIntent::Irate { .. }
                | AggIntent::Resets { .. }
                | AggIntent::Changes { .. }
        )
    }
}

impl AggIntent {
    /// Output column name + type produced by this intent when applied to
    /// `input`. Used by `QueryExpr::Aggregate`'s schema-derivation rule
    /// (`design.md` §6 schema-flow table: "one new column per entry in
    /// `aggs`, each named and typed by `AggIntent::output_type(input_field)`").
    ///
    /// PromQL convention: aggregate column name = intent kind (`count`,
    /// `quantile_0_99`, …) so consumers can locate it without an alias
    /// lookup.
    pub fn output_column(&self, input: &Column) -> Column {
        match self {
            AggIntent::Count { .. } => Column {
                name: "count".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Sum => Column {
                name: "sum".into(),
                dtype: input.dtype.clone(),
                nullable: false,
            },
            AggIntent::Min => Column {
                name: "min".into(),
                dtype: input.dtype.clone(),
                nullable: input.nullable,
            },
            AggIntent::Max => Column {
                name: "max".into(),
                dtype: input.dtype.clone(),
                nullable: input.nullable,
            },
            AggIntent::Avg => Column {
                name: "avg".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Quantile { q, .. } => Column {
                name: format!("quantile_{}", quantile_suffix(*q)),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::TopK { k, .. } => Column {
                name: format!("topk_{k}"),
                // TopK output is a struct/list per row; modeled as Utf8
                // for L3 (the L4 sketch-bound IR upgrades the dtype).
                dtype: DataType::Utf8,
                nullable: false,
            },
            AggIntent::Cardinality { .. } => Column {
                name: "cardinality".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Frequency { .. } => Column {
                name: "frequency".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Rate { .. } => Column {
                name: "rate".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Increase { .. } => Column {
                name: "increase".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            // ── Archive-only intents (Phase β) ────────────────────────────
            // Each carries a stable column name keyed on the intent kind so
            // the StreamingConfig emitter and Phase α routing entry can
            // locate them. All are Float64 except the boolean Absent /
            // Present, which surface as Int64 (1 / 0) per PromQL convention.
            AggIntent::Absent => Column {
                name: "absent".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Present => Column {
                name: "present".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Delta { .. } => Column {
                name: "delta".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Deriv { .. } => Column {
                name: "deriv".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::PredictLinear { .. } => Column {
                name: "predict_linear".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::HoltWinters { .. } => Column {
                name: "holt_winters".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Idelta { .. } => Column {
                name: "idelta".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Irate { .. } => Column {
                name: "irate".into(),
                dtype: DataType::Float64,
                nullable: false,
            },
            AggIntent::Resets { .. } => Column {
                name: "resets".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
            AggIntent::Changes { .. } => Column {
                name: "changes".into(),
                dtype: DataType::Int64,
                nullable: false,
            },
        }
    }
}

/// `0.99` → `"0_99"`, `0.5` → `"0_5"`. Used by `Quantile` output naming
/// so `quantile_0_99` is a valid identifier downstream.
fn quantile_suffix(q: f64) -> String {
    let mut s = format!("{q}");
    if let Some(stripped) = s.strip_prefix('-') {
        s = format!("neg_{stripped}");
    }
    s.replace('.', "_")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};

    fn col(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    #[test]
    fn agg_intent_serde_roundtrip() {
        let cases = vec![
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggIntent::Sum,
            AggIntent::Min,
            AggIntent::Max,
            AggIntent::Avg,
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Cardinality {
                accuracy: AccuracyTarget::EpsilonDelta {
                    eps: 0.01,
                    delta: 0.001,
                },
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
        ];
        for variant in cases {
            let json = serde_json::to_string(&variant).unwrap();
            let back: AggIntent = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn output_column_names_are_intent_keyed() {
        let v = col("value", DataType::Float64);
        assert_eq!(
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "count"
        );
        assert_eq!(AggIntent::Sum.output_column(&v).name, "sum");
        assert_eq!(
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }
            .output_column(&v)
            .name,
            "quantile_0_99"
        );
        assert_eq!(
            AggIntent::TopK {
                k: 5,
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "topk_5"
        );
    }

    #[test]
    fn quantile_output_is_float64() {
        let v = col("value", DataType::Int64);
        let out = AggIntent::Quantile {
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }
        .output_column(&v);
        assert!(matches!(out.dtype, DataType::Float64));
    }

    #[test]
    fn sum_preserves_input_dtype() {
        let int_col = col("c", DataType::Int64);
        let float_col = col("c", DataType::Float64);
        assert!(matches!(
            AggIntent::Sum.output_column(&int_col).dtype,
            DataType::Int64
        ));
        assert!(matches!(
            AggIntent::Sum.output_column(&float_col).dtype,
            DataType::Float64
        ));
    }

    // ── Phase β archive-only intent tests ─────────────────────────────────

    /// Every intent the legacy `asap-planner-rs::single_query::is_supported`
    /// previously refused now lifts to L3 with `archive_only() == true`.
    /// The negative cases are the warm-tier-bound intents — they must
    /// continue to return false, otherwise the L4 binder would short-circuit
    /// them to the cold tier.
    #[test]
    fn archive_only_flag_partitions_intents() {
        // Archive-only — every Phase β migration target.
        let archive: Vec<AggIntent> = vec![
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
        for v in archive {
            assert!(
                v.archive_only(),
                "{v:?} should be archive-only after Phase β migration"
            );
        }

        // Warm-tier — must NOT be flagged archive-only or the L4 binder
        // breaks.
        let warm: Vec<AggIntent> = vec![
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggIntent::Sum,
            AggIntent::Min,
            AggIntent::Max,
            AggIntent::Avg,
            AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Cardinality {
                accuracy: AccuracyTarget::EpsilonDelta {
                    eps: 0.01,
                    delta: 0.001,
                },
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
        ];
        for v in warm {
            assert!(
                !v.archive_only(),
                "{v:?} is warm-tier and must not be archive-only"
            );
        }
    }

    #[test]
    fn archive_only_intent_serde_roundtrip() {
        let cases = vec![
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
        for v in cases {
            let json = serde_json::to_string(&v).unwrap();
            let back: AggIntent = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back, "round-trip failed for {v:?}");
        }
    }

    #[test]
    fn archive_only_output_column_names() {
        let v = col("value", DataType::Float64);
        assert_eq!(AggIntent::Absent.output_column(&v).name, "absent");
        assert_eq!(AggIntent::Present.output_column(&v).name, "present");
        assert_eq!(
            AggIntent::Delta {
                window: Duration::from_secs(60)
            }
            .output_column(&v)
            .name,
            "delta"
        );
        assert_eq!(
            AggIntent::Resets {
                window: Duration::from_secs(60)
            }
            .output_column(&v)
            .name,
            "resets"
        );
    }
}
