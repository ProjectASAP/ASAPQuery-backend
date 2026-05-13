//! Layer 4 IR — `SketchExpr` DAG.
//!
//! Per `control_plane/docs/design.md` §6 "`core::sketch_algebra` — Layer 4 IR
//! (`SketchExpr`)" (around line ~565).
//!
//! Two-IR split: L3 [`crate::intent_algebra::QueryExpr`] is intent-only;
//! L4 [`SketchExpr`] is sketch-bound. `Bind*` rules consume the L3 IR
//! and produce the L4 IR with the sketch family + parameters committed.
//!
//! The variant set ships the subset DC + PromQL needs (the orchestrator's
//! current scope-reduction). `SketchJoin`, `SketchSubtract`, `SketchDelete`
//! from design.md §6 are intentionally *not* surfaced yet — they're
//! gated on rules that haven't landed (no `Bind*OnJoin`, no subtract /
//! delete consumer) and the orchestrator's spec restricts Phase C to
//! `SketchAgg / SketchEstimate / SketchMerge / Logical`. Adding them
//! later is a purely additive enum extension.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::intent_algebra::QueryExpr;
use crate::sketch_algebra::params::{SketchKind, SketchParams};
use crate::types_v2::BindingName;

/// Readout operation extracted from a built sketch state. Inverse of
/// `SketchAgg`. Mirrors design.md §6 line ~607 — `SketchEstimate` plus
/// the readout `query` that says what to extract from the state.
///
/// PromQL convention: a `Quantile` op carries the φ; a `PointCount` op
/// carries the key; `Cardinality` and `TopK` need no payload beyond the
/// ones already on the producing `SketchAgg` (cardinality ops are
/// parameter-free; the TopK `k` rides on the `SketchAgg` for
/// CountSketch-with-heap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum EstimateOp {
    /// φ-th quantile readout — KLL / DDSketch / t-digest input.
    Quantile { q: f64 },
    /// Approximate cardinality (count-distinct) — HLL / theta-sketch input.
    Cardinality,
    /// Approximate point count for a key — CMS input.
    PointCount { key: String },
    /// Heavy-hitter top-k extraction — CountSketch-with-heap / Misra-Gries.
    TopK { k: usize },
}

/// Algebra of a `SketchMerge` node — at L4 today this is always a union
/// of mergeable sketch states. The enum-shape is forward-compatible with
/// future merge algebras (weighted union for sampling sketches, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeAlgebra {
    /// Set-union of two or more sketch states — `KLL ∪ KLL`, `HLL ∪ HLL`,
    /// etc. The catalog `mergeable` flag must be true on all inputs.
    Union,
}

/// L4 algebra node. See module doc for the variant subset rationale.
///
/// Serde tag is `"sketch_node"` (not `"node"`) so it doesn't collide with
/// the L3 `QueryExpr`'s `"node"` tag — `SketchExpr::Logical(QueryExpr)`
/// nests a JSON-tagged enum inside an internally-tagged outer enum, and
/// reusing the same tag would surface as a `duplicate field "node"`
/// deserialization error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "sketch_node", rename_all = "snake_case")]
pub enum SketchExpr {
    /// Logical pass-through: an L3 node that no L4 rule rewrote. A
    /// `Filter`, a row-shaped `Aggregate{Sum}`, or any other operator
    /// whose semantics are unchanged by the sketch-binding pass lives
    /// here unchanged.
    Logical(QueryExpr),

    /// Sketch aggregation — produces a sketch state on its output edge.
    /// The L3 `AggIntent` was lowered by a `Bind*` rule into the
    /// committed `(sketch_type, params)` pair.
    SketchAgg {
        /// Sketch family (KLL / DDSketch / HLL / CMS / CountSketch).
        sketch_type: SketchKind,
        /// Sketch parameters (validated by the catalog at bind time).
        params: SketchParams,
        /// Input sub-tree — typically `Logical(Window{...})` or
        /// `Logical(Scan{...})`.
        child: Box<SketchExpr>,
    },

    /// Read out a query result from a built sketch state. Inverse of
    /// `SketchAgg`. The `op` says what to extract — a quantile φ, the
    /// approximate cardinality, the top-k heavy hitters.
    SketchEstimate {
        /// Readout operation (see [`EstimateOp`]).
        op: EstimateOp,
        /// Sketch-state-bearing sub-tree (a `SketchAgg`, a `SketchMerge`,
        /// or a `Ref` to one).
        child: Box<SketchExpr>,
    },

    /// Merge multiple sketches into one — set-union under
    /// [`MergeAlgebra::Union`]. The L4 type checker rejects mismatched
    /// families / params at plan time (design.md §6.4 invariant 1).
    SketchMerge {
        /// Merge algebra — currently always `Union`.
        algebra: MergeAlgebra,
        /// Sketch-state-bearing inputs. All must agree on `(kind, params)`.
        children: Vec<SketchExpr>,
    },

    /// SQL `WITH name AS (expr) SELECT ... FROM name` / sketch-state
    /// fan-in: name a sub-expression so multiple parents can reference
    /// it. `SketchExpr::LetBinding` carries the two-tier fan-in described
    /// in design.md §1339 — outer let names a `Window` output, inner let
    /// names a `SketchAgg{KLL}` shared by two `SketchEstimate` parents
    /// reading different quantiles.
    LetBinding {
        /// Binding name; must be unique within the surrounding scope.
        name: BindingName,
        /// Bound sub-expression.
        expr: Box<SketchExpr>,
        /// In-scope sub-tree — references the binding via `Ref`.
        child: Box<SketchExpr>,
    },

    /// Reference a `LetBinding` by name. Resolution is lexical (scope
    /// follows the surrounding `LetBinding` chain).
    Ref {
        /// Bound name.
        name: BindingName,
    },

    // ── Phase ε.1: three-mode placement variants ────────────────────────
    //
    // Phase ε.1 collapses the planner's raw-vs-sketch + edge-vs-backend
    // axes into a single tri-mode selector. The two new variants name
    // the two new placements; the existing `SketchAgg` corresponds to
    // Mode 1 (sketch at edge). See `planner::wire_cost::BindMode`.
    /// Mode 2 (Phase ε.1): no sketch processor at the edge — raw OTLP
    /// forwards to the backend, which builds the sketch at ingest. The
    /// `family` and `params` are the sketch the backend will build, so
    /// the backend's `StreamingConfig` `aggregation_input` is `raw` for
    /// this metric (Phase ε.2 implements the raw-input ingest path).
    RawAtEdgeSketchAtBackend {
        /// Sketch family the backend will build at ingest.
        family: SketchKind,
        /// Sketch parameters (validated by the catalog at bind time).
        params: SketchParams,
        /// Input sub-tree — typically `Logical(Window{...})` or
        /// `Logical(Scan{...})`. Mirrors `SketchAgg`'s child field so the
        /// L5 emitter's walk uniform.
        child: Box<SketchExpr>,
    },

    /// Mode 3 (Phase ε.1): no sketch processor at the edge — raw OTLP
    /// ships directly to Prometheus's native OTLP receiver at
    /// `/api/v1/otlp/v1/metrics`. The backend HTTP-forwards queries to
    /// Prometheus's `/api/v1/query` endpoint (the `prometheus_remote`
    /// engine). Accuracy is exact (ε = 0) — Prometheus owns the raw
    /// samples; no sketch math is involved.
    ///
    /// The variant carries enough projection info for the edge agent's
    /// pipeline to ship the right metric with the right labels, and for
    /// the backend's storage routing to claim the metric.
    RawAtEdgePrometheusArchive {
        /// Metric name as it appears at the edge (and in
        /// `BackendStorageRouting`).
        metric: String,
        /// Optional window — when present, the planner pre-bucketed the
        /// metric into windowed scrape data. Prometheus stores the raw
        /// stream regardless; the field is informational for the L5
        /// emitter so it can size scrape intervals consistently.
        window: Option<std::time::Duration>,
        /// Label projection — labels promoted from OTLP resource
        /// attributes by Prometheus's
        /// `otlp.promote_resource_attributes` config. Default
        /// `["service.name", "service.namespace", "service.instance.id"]`
        /// — see `deploy/configs/prometheus-otlp-receiver.yml`.
        label_proj: Vec<String>,
    },
}

impl SketchExpr {
    /// Convenience constructor for the canonical
    /// `SketchEstimate{SketchAgg{Logical(qe)}}` shape produced by every
    /// `Bind*` rule. Keeps rule call sites short.
    pub fn estimate_over_agg(
        op: EstimateOp,
        sketch_type: SketchKind,
        params: SketchParams,
        logical: QueryExpr,
    ) -> Self {
        SketchExpr::SketchEstimate {
            op,
            child: Box::new(SketchExpr::SketchAgg {
                sketch_type,
                params,
                child: Box::new(SketchExpr::Logical(logical)),
            }),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{AggIntent, LabelFilter, QueryExpr, Schema, Source, WindowKind};
    use crate::sketch_algebra::params::{CountSketchParams, DDSketchParams, HllParams, KllParams};
    use crate::types_v2::AccuracyTarget;
    use std::time::Duration;

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
                    Column {
                        name: "ts".into(),
                        dtype: DataType::Timestamp,
                        nullable: false,
                    },
                    Column {
                        name: "service".into(),
                        dtype: DataType::Utf8,
                        nullable: false,
                    },
                    Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    },
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

    #[test]
    fn estimate_over_agg_ctor_shape() {
        let e = SketchExpr::estimate_over_agg(
            EstimateOp::Quantile { q: 0.99 },
            SketchKind::Kll,
            SketchParams::Kll(KllParams { k: 200 }),
            windowed_scan(),
        );
        match e {
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
                        assert!(matches!(*child, SketchExpr::Logical(_)));
                    }
                    other => panic!("expected SketchAgg, got {other:?}"),
                }
            }
            other => panic!("expected SketchEstimate, got {other:?}"),
        }
    }

    fn agg_quantile() -> QueryExpr {
        QueryExpr::Aggregate {
            by: vec![],
            aggs: vec![AggIntent::Quantile {
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(windowed_scan()),
        }
    }

    #[test]
    fn sketch_expr_serde_roundtrip_logical() {
        let e = SketchExpr::Logical(agg_quantile());
        let json = serde_json::to_string(&e).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn sketch_expr_serde_roundtrip_sketch_agg() {
        let e = SketchExpr::SketchAgg {
            sketch_type: SketchKind::Kll,
            params: SketchParams::Kll(KllParams { k: 200 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn sketch_expr_serde_roundtrip_estimate() {
        let e = SketchExpr::estimate_over_agg(
            EstimateOp::Quantile { q: 0.95 },
            SketchKind::DDSketch,
            SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
            windowed_scan(),
        );
        let json = serde_json::to_string(&e).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn sketch_expr_serde_roundtrip_merge() {
        let leaf = SketchExpr::SketchAgg {
            sketch_type: SketchKind::Hll,
            params: SketchParams::Hll(HllParams { precision: 14 }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        };
        let e = SketchExpr::SketchMerge {
            algebra: MergeAlgebra::Union,
            children: vec![leaf.clone(), leaf],
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn sketch_expr_serde_roundtrip_let_ref() {
        let inner_agg = SketchExpr::SketchAgg {
            sketch_type: SketchKind::CountSketch,
            params: SketchParams::CountSketch(CountSketchParams {
                w: 2048,
                d: 5,
                with_heap: true,
            }),
            child: Box::new(SketchExpr::Logical(windowed_scan())),
        };
        let e = SketchExpr::LetBinding {
            name: BindingName::new("kll_state"),
            expr: Box::new(inner_agg),
            child: Box::new(SketchExpr::SketchEstimate {
                op: EstimateOp::TopK { k: 10 },
                child: Box::new(SketchExpr::Ref {
                    name: BindingName::new("kll_state"),
                }),
            }),
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: SketchExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }
}
