//! Layer 4/5 IR — `L4Plan` / `PhysicalExpr`.
//!
//! Per `control_plane/docs/design.md` §6 "`core::sketch_algebra` — Layer 4 IR"
//! and the L4/L5 layer-spine invariant: "sketch binding is already
//! committed by L4; L5 is about stage allocation + emission."
//!
//! Step B of the plan-shaped-serving migration retires this crate's own
//! `PhysicalExpr`-as-L4-algebra (the old `Logical` / `SketchAgg` /
//! `SketchEstimate` / `SketchMerge` / `ExactAgg` variants) in favor of
//! ASAPController's canonical L4 IR, `asap_sketch::{SummaryExpr, L4Node}`
//! — the same move Step 3 of the enum-unification made for
//! `SketchKind → SummaryKind`, one layer up. `implement_promql_for_asap_tier`
//! (`asap_tier_implement.rs`, Step A) already builds `Rc<L4Node>` trees via
//! `asap_plan::bind::implement_tree_in_with`; this module gives the rest of
//! the crate (optimizer, physical, emit) the same IR shape.
//!
//! Two things `asap_sketch::L4Node` genuinely doesn't have, kept here:
//!
//! - **`LetBinding` / `Ref`** — named fan-in sharing (SQL
//!   `WITH name AS (expr) ...` / a `SketchAgg` shared by two `SketchEstimate`
//!   readouts). `L4Node`'s own DAG sharing is structural (multiple `Rc`
//!   references to the same node), not named — but the rule-firing walk in
//!   this crate discovers sharing incrementally, per-node, so it still needs
//!   a name to thread a bound value across sibling calls. This is a
//!   deployment-specific mechanism, not a fact about the sketch algebra
//!   itself — L4 concern (it's still "what to compute", just with sharing),
//!   hence `L4Plan` rather than `PhysicalExpr`.
//! - **`RawAtEdgeSketchAtBackend` / `RawAtEdgePrometheusArchive`** — Phase
//!   ε.1's placement decisions (where the sketch gets built, not what it
//!   is). Genuinely L5 — see `optimizer::cost::wire::BindMode`, which names
//!   the same three modes.
//!
//! `SketchAgg` / `SketchEstimate` / `SketchMerge` / `Logical` / `ExactAgg`
//! don't get their own variants anymore — `asap_sketch::SummaryExpr`
//! already unifies all of them (including "exact accumulator" and "sketch"
//! as the same `SummaryAgg` node, with or without a wrapping
//! `SummaryEstimate`) inside a single `L4Plan::Summary(Rc<L4Node>)`.

#![allow(dead_code)]

use std::rc::Rc;
use std::time::Duration;

use asap_sketch::{L4Node, SummaryKind, SummaryParams};

use crate::types_v2::BindingName;

/// L4 IR — "what to compute". Wraps `asap_sketch::L4Node` (the sketch
/// algebra itself, owned upstream) and adds only the named-binding sharing
/// mechanism `asap_sketch` doesn't have. See module docs.
#[derive(Debug, Clone)]
pub enum L4Plan {
    /// A committed L4 sub-tree — `SummaryAgg` / `SummaryEstimate` /
    /// `SummaryMerge` / `Logical`, whatever `implement_tree_in_with` (or a
    /// deployment-specific pre-pass) produced.
    Summary(Rc<L4Node>),

    /// SQL `WITH name AS (expr) SELECT ... FROM name` / sketch-state
    /// fan-in: name a sub-expression so multiple parents can reference it.
    /// Carries the two-tier fan-in described in design.md §1339 — outer
    /// let names a `Window` output, inner let names a `SummaryAgg{Kll}`
    /// shared by two `SummaryEstimate` parents reading different quantiles.
    LetBinding {
        /// Binding name; must be unique within the surrounding scope.
        name: BindingName,
        /// Bound sub-expression.
        expr: Rc<L4Plan>,
        /// In-scope sub-tree — references the binding via `Ref`.
        child: Rc<L4Plan>,
    },

    /// Reference a `LetBinding` by name. Resolution is lexical (scope
    /// follows the surrounding `LetBinding` chain).
    Ref {
        /// Bound name.
        name: BindingName,
    },
}

/// L5 IR — "where/how". Wraps an already-committed [`L4Plan`] (sketch
/// binding is final by the time anything reaches here) with placement
/// info: build at the edge (the common case — `Committed` needs no extra
/// annotation since the `L4Plan` itself is the whole story), or one of
/// Phase ε.1's two backend/archive placements.
#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    /// Sketch built at the edge — the default placement. The committed
    /// `L4Plan` alone determines the output.
    Committed(L4Plan),

    /// Phase ε.1 Mode 2: no sketch processor at the edge — raw OTLP
    /// forwards to the backend, which builds the sketch at ingest. The
    /// `family` and `params` are the sketch the backend will build, so
    /// the backend's `StreamingConfig` `aggregation_input` is `raw` for
    /// this metric.
    RawAtEdgeSketchAtBackend {
        /// Sketch family the backend will build at ingest.
        family: SummaryKind,
        /// Sketch parameters (validated by the catalog at bind time).
        params: SummaryParams,
        /// Input sub-tree — typically `Summary(Logical(Window{...}))` or
        /// `Summary(Logical(Scan{...}))`.
        child: Box<L4Plan>,
    },

    /// Phase ε.1 Mode 3: no sketch processor at the edge — raw OTLP ships
    /// directly to Prometheus's native OTLP receiver at
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
        window: Option<Duration>,
        /// Label projection — labels promoted from OTLP resource
        /// attributes by Prometheus's
        /// `otlp.promote_resource_attributes` config. Default
        /// `["service.name", "service.namespace", "service.instance.id"]`
        /// — see `deploy/configs/prometheus-otlp-receiver.yml`.
        label_proj: Vec<String>,
    },
}

impl PhysicalExpr {
    /// Convenience constructor for the common case: a committed
    /// `L4Node` sketch-built at the edge, no placement wrapper.
    pub fn committed(node: Rc<L4Node>) -> Self {
        PhysicalExpr::Committed(L4Plan::Summary(node))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{LabelFilter, QueryExpr, Schema, Source, WindowKind};
    use std::time::Duration;

    fn ts_scan() -> QueryExpr {
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

    #[test]
    fn committed_wraps_an_implement_tree_result() {
        let q = QueryExpr::Aggregate {
            by: crate::intent_algebra::GroupKeys::none(),
            aggs: vec![crate::intent_algebra::AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed_scan()),
        };
        let node = asap_plan::bind::implement_tree(&q).expect("implements");
        let e = PhysicalExpr::committed(node);
        match e {
            PhysicalExpr::Committed(L4Plan::Summary(node)) => match &node.expr {
                asap_sketch::SummaryExpr::SummaryEstimate { query, sketch_input } => {
                    assert!(matches!(query, asap_sketch::SketchQuery::Quantile { q } if *q == 0.99));
                    match &sketch_input.expr {
                        asap_sketch::SummaryExpr::SummaryAgg {
                            sketch, params, child, ..
                        } => {
                            assert_eq!(sketch, &SummaryKind::Kll);
                            assert_eq!(params, &SummaryParams::Kll { k: 200 });
                            assert!(matches!(child.expr, asap_sketch::SummaryExpr::Logical(_)));
                        }
                        other => panic!("expected SummaryAgg, got {other:?}"),
                    }
                }
                other => panic!("expected SummaryEstimate, got {other:?}"),
            },
            other => panic!("expected Committed(Summary(_)), got {other:?}"),
        }
    }
}
