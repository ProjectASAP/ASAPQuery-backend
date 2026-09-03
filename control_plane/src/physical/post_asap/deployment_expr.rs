//! Backend deployment wrappers around the canonical post-ASAP plan.
//!
//! Step B of the plan-shaped-serving migration retires this crate's own
//! `PhysicalExpr`-as-L4-algebra (the old `Logical` / `SketchAgg` /
//! `SketchEstimate` / `SketchMerge` / `ExactAgg` variants) in favor of
//! ASAPPlanner's canonical post-ASAP IR, `planner_types::post_asap::{SummaryExpr, SummaryNode}`
//! — the same move Step 3 of the enum-unification made for
//! `SketchAlgorithm → SketchAlgorithm`, one layer up. `implement_promql_for_asap_tier`
//! (`asap_tier_implement.rs`, Step A) already builds `Rc<SummaryNode>` trees via
//! `asap_aware_mapping::bind::implement_tree_in_with`; this module gives the rest of
//! the crate (optimizer, physical, emit) the same IR shape.
//!
//! Two things `planner_types::post_asap::SummaryNode` genuinely doesn't have, kept here:
//!
//! - **`LetBinding` / `Ref`** — named fan-in sharing (SQL
//!   `WITH name AS (expr) ...` / a `SketchAgg` shared by two `SketchEstimate`
//!   readouts). `SummaryNode`'s own DAG sharing is structural (multiple `Rc`
//!   references to the same node), not named — but the rule-firing walk in
//!   this crate discovers sharing incrementally, per-node, so it still needs
//!   a name to thread a bound value across sibling calls. This is a
//!   deployment-specific mechanism, not a fact about the sketch algebra
//!   itself, hence the backend-local [`PostAsapPlan`] wrapper.
//! - **`RawAtEdgeSketchAtBackend` / `RawAtEdgePrometheusArchive`** — Phase
//!   ε.1's placement decisions (where the sketch gets built, not what it
//!   is). Genuinely L5. `physical::deployment_cost::wire` once named the same three
//!   modes as `BindMode` with a `select_bind_mode` chooser, but that
//!   selector was never wired to a caller (`bind_query_expr` always
//!   produces `Committed`) and was removed in the 2026-07 retirement
//!   pass — these two variants remain structurally unreachable in
//!   production today, kept only because `physical/colored_dag/{allocator,emitter}.rs`
//!   already pattern-match on them.
//!
//! `SketchAgg` / `SketchEstimate` / `SketchMerge` / `Logical` / `ExactAgg`
//! don't get their own variants anymore — `planner_types::post_asap::SummaryExpr`
//! already unifies all of them (including "exact accumulator" and "sketch"
//! as the same `SummaryAgg` node, with or without a wrapping
//! `SummaryEstimate`) inside a single `PostAsapPlan::Summary(Rc<SummaryNode>)`.

#![allow(dead_code)]

use std::rc::Rc;
use std::time::Duration;

use planner_types::post_asap::{SketchAlgorithm, SketchParams, SummaryNode};

use crate::types_v2::BindingName;

/// Backend-local wrapper around the Planner-owned post-ASAP tree.
///
/// It adds only named sharing required during physical placement and emission.
#[derive(Debug, Clone)]
pub enum PostAsapPlan {
    /// A committed post-ASAP sub-tree — `SummaryAgg` / `SummaryEstimate` /
    /// `SummaryMerge` / `Logical`, whatever `implement_tree_in_with` (or a
    /// deployment-specific pre-pass) produced.
    Summary(Rc<SummaryNode>),

    /// SQL `WITH name AS (expr) SELECT ... FROM name` / sketch-state
    /// fan-in: name a sub-expression so multiple parents can reference it.
    /// Carries the two-tier fan-in described in design.md §1339 — outer
    /// let names a `Window` output, inner let names a `SummaryAgg{Kll}`
    /// shared by two `SummaryEstimate` parents reading different quantiles.
    LetBinding {
        /// Binding name; must be unique within the surrounding scope.
        name: BindingName,
        /// Bound sub-expression.
        expr: Rc<PostAsapPlan>,
        /// In-scope sub-tree — references the binding via `Ref`.
        child: Rc<PostAsapPlan>,
    },

    /// Reference a `LetBinding` by name. Resolution is lexical (scope
    /// follows the surrounding `LetBinding` chain).
    Ref {
        /// Bound name.
        name: BindingName,
    },
}

/// Physical placement — "where/how". Wraps an already-committed
/// [`PostAsapPlan`] (summary selection is final by the time it reaches here)
/// with placement
/// info: build at the edge (the common case — `Committed` needs no extra
/// annotation since the `PostAsapPlan` itself is the whole story), or one of
/// Phase ε.1's two backend/archive placements.
#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    /// Sketch built at the edge — the default placement. The committed
    /// `PostAsapPlan` alone determines the output.
    Committed(PostAsapPlan),

    /// Phase ε.1 Mode 2: no sketch processor at the edge — raw OTLP
    /// forwards to the backend, which builds the sketch at ingest. The
    /// `family` and `params` are the sketch the backend will build, so
    /// the backend's `StreamingConfig` `aggregation_input` is `raw` for
    /// this metric.
    RawAtEdgeSketchAtBackend {
        /// Sketch family the backend will build at ingest.
        family: SketchAlgorithm,
        /// Sketch parameters (validated by the catalog at bind time).
        params: SketchParams,
        /// Input sub-tree — typically `Summary(Logical(Window{...}))` or
        /// `Summary(Logical(Scan{...}))`.
        child: Box<PostAsapPlan>,
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
    /// `SummaryNode` sketch-built at the edge, no placement wrapper.
    pub fn committed(node: Rc<SummaryNode>) -> Self {
        PhysicalExpr::Committed(PostAsapPlan::Summary(node))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::WindowKind;
    use planner_types::pre_asap::{Column, DataType, QueryExpr, Schema, Source};
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

    #[test]
    fn committed_wraps_an_implement_tree_result() {
        let q = QueryExpr::Aggregate {
            reduction: planner_types::pre_asap::Reduction::by(vec![]),
            measures: vec![planner_types::pre_asap::AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            }],
            output_names: Vec::new(),
            having: None,
            child: Rc::new(windowed_scan()),
        };
        let node = crate::planner_selection::select_summary_default(&q).expect("implements");
        let e = PhysicalExpr::committed(node);
        match e {
            PhysicalExpr::Committed(PostAsapPlan::Summary(node)) => match &node.expr {
                planner_types::post_asap::SummaryExpr::SummaryEstimate {
                    query,
                    summary_input,
                } => {
                    assert!(
                        matches!(query, planner_types::post_asap::SketchQuery::Quantile { q } if *q == 0.99)
                    );
                    match &summary_input.expr {
                        planner_types::post_asap::SummaryExpr::SummaryAgg {
                            family, child, ..
                        } => {
                            assert_eq!(
                                family,
                                &planner_types::post_asap::SummaryFamilyType::Sketch(
                                    planner_types::post_asap::SketchKind::new(
                                        SketchAlgorithm::Kll,
                                        SketchParams::Kll { k: 269 },
                                    ),
                                    planner_types::post_asap::GroupingStrategy::default(),
                                )
                            );
                            assert!(matches!(
                                child.expr,
                                planner_types::post_asap::SummaryExpr::KeepPreAsap(_)
                            ));
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
