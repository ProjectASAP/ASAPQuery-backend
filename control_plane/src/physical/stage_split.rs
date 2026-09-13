//! Assign the bound physical DAG to edge, gateway, and backend stages.
//! [`split_typed_three_stage`] returns structured stage configs for wire emission.

/// Env-var that opts *out* of the typed L5 stage_split path. The typed
/// path — `physical::post_asap::PhysicalExpr` (L4) → `split_typed_three_stage`
/// → per-stage `StageConfig` emit — is the primary (and only) L5; this
/// var exists only as a kill switch.
#[allow(dead_code)]
pub const ENV_USE_TYPED_STAGE_SPLIT: &str = "USE_TYPED_STAGE_SPLIT";

/// Whether the typed L5 stage_split path is enabled for this process.
///
/// **Default ON.** Set `USE_TYPED_STAGE_SPLIT=0` (or `false` / `no`) to
/// disable it — callers then skip the typed per-stage emit entirely.
/// Reads the env var once per call (cheap; called per `plan()`
/// invocation at most).
pub fn typed_stage_split_enabled() -> bool {
    !matches!(
        std::env::var(ENV_USE_TYPED_STAGE_SPLIT).as_deref(),
        Ok("0") | Ok("false") | Ok("no")
    )
}

/// Run the L5 stage split on an L4-bound `PhysicalExpr` DAG. Returns the
/// per-stage [`crate::physical::colored_dag::StageConfig`] map for the DC
/// lifecycle three-stage topology.
///
/// Compatibility wrapper: failures retain their concrete reason in diagnostics.
/// Call [`try_split_typed`] when the caller can return the error. Each per-stage config the
/// returned map carries is materialised into wire bytes by the emitters
/// in [`crate::emit::stage_config`] — `emit_edge_yaml` for `Edge`,
/// `emit_gateway_yaml` for `Gateway`, `emit_backend_streaming_config_json`
/// for `Backend`.
pub fn split_typed_three_stage(
    expr: &crate::physical::post_asap::PhysicalExpr,
) -> Option<
    std::collections::HashMap<
        crate::physical::colored_dag::StageId,
        crate::physical::colored_dag::StageConfig,
    >,
> {
    match try_split_typed(expr, crate::physical::colored_dag::Topology::ThreeStage) {
        Ok(configs) => Some(configs),
        Err(error) => {
            tracing::warn!(error = %error, "three-stage realization unavailable");
            None
        }
    }
}

/// Allocate and emit without erasing capability or emission failures.
/// Unsupported topology remains an error; this does not enable SingleStage.
pub fn try_split_typed(
    expr: &crate::physical::post_asap::PhysicalExpr,
    topology: crate::physical::colored_dag::Topology,
) -> anyhow::Result<
    std::collections::HashMap<
        crate::physical::colored_dag::StageId,
        crate::physical::colored_dag::StageConfig,
    >,
> {
    use super::realization::{ExistingRealizations, RealizationProvider};
    ExistingRealizations.stages(expr, topology)
}

#[cfg(test)]
mod l5_walk_propagation_tests {
    //! Characterisation tests for the L5 walk's edge-fact extraction.
    //!
    //! Established by PR #247: `handle_plan`'s `Backend` stage arm
    //! belt-and-braces patches `metric_name`, `window_secs`, and
    //! `grouping` on every emitted `BackendAggregation` from the
    //! workload spec, on the suspicion that the L5 walk's
    //! `extract_edge_facts` doesn't propagate these fields cleanly
    //! through every binder's output shape.
    //!
    //! These tests **measure** what the L5 walk actually produces for
    //! the canonical `bind_workload_typed` output — so we know whether
    //! the patches are dead weight (the walk works → fields already
    //! populated → patches are no-ops) or load-bearing (walk doesn't
    //! propagate → patches are the real source of the field values).
    //!
    //! Result documented in the test assertions: the walk **does**
    //! surface `metric_name` and `window_secs` for
    //! `bind_workload_typed` output. Grouping stays empty because the
    //! canonical L3 `QueryExpr::Aggregate.by` is a `Vec<ColumnId>`
    //! against a synthesized schema that has no label columns (Step γ
    //! TODO in `intent_algebra::column_resolution`).

    use crate::physical::colored_dag::StageConfig;
    use crate::types::{AggType, RegisteredWorkload};
    use std::collections::HashMap;
    use std::time::Duration;

    fn workload(metric: &str, group_by: Vec<String>, window: Duration) -> RegisteredWorkload {
        crate::registered_workload::fixtures::WorkloadFixture {
            metric_name: metric.to_string(),
            label_filters: HashMap::new(),
            group_by_labels: group_by,
            aggregations: vec![AggType::Quantile],
            time_window: window,
            repeat_every: None,

            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![0.99],
        }
        .build()
    }

    #[test]
    fn l5_walk_surfaces_metric_name_for_bind_workload_typed_output() {
        let w = workload("http_latency_ms", Vec::new(), Duration::from_secs(60));
        let deployment_expr =
            crate::physical::workload_planner::bind_workload_typed(&w).expect("bind produced expr");
        let configs = super::split_typed_three_stage(&deployment_expr).expect("split ok");
        let backend_cfg = configs
            .into_values()
            .find_map(|cfg| match cfg {
                StageConfig::Backend(be) => Some(be),
                _ => None,
            })
            .expect("Backend stage produced");
        let agg = backend_cfg
            .aggregations
            .first()
            .expect("at least one aggregation");
        assert_eq!(
            agg.metric_name, "http_latency_ms",
            "the L5 walk's `extract_edge_facts` must thread the Scan's \
             metric name through to BackendAggregation.metric_name"
        );
    }

    #[test]
    fn l5_walk_surfaces_window_secs_for_bind_workload_typed_output() {
        let w = workload("http_latency_ms", Vec::new(), Duration::from_secs(120));
        let deployment_expr =
            crate::physical::workload_planner::bind_workload_typed(&w).expect("bind produced expr");
        let configs = super::split_typed_three_stage(&deployment_expr).expect("split ok");
        let backend_cfg = configs
            .into_values()
            .find_map(|cfg| match cfg {
                StageConfig::Backend(be) => Some(be),
                _ => None,
            })
            .expect("Backend stage produced");
        let agg = backend_cfg
            .aggregations
            .first()
            .expect("at least one aggregation");
        assert_eq!(
            agg.window_secs, 120,
            "the L5 walk's `extract_edge_facts` must thread Window.size \
             through to BackendAggregation.window_secs"
        );
    }

    #[test]
    fn l5_walk_leaves_grouping_empty_pending_step_gamma() {
        // L3 `QueryExpr::Aggregate.by` is positional `ColumnId`s against
        // a synthesized schema that has no label columns — so the walk
        // CANNOT recover the original label names. handle_plan patches
        // grouping from `workload.group_by_labels` for this reason.
        // This test pins the current behaviour so a future Step γ fix
        // (proper open-set label resolution) will fail it loudly and
        // remind whoever's making the change to also retire the patch.
        let w = workload(
            "http_latency_ms",
            vec!["zone".to_string()],
            Duration::from_secs(60),
        );
        let deployment_expr =
            crate::physical::workload_planner::bind_workload_typed(&w).expect("bind produced expr");
        let configs = super::split_typed_three_stage(&deployment_expr).expect("split ok");
        let backend_cfg = configs
            .into_values()
            .find_map(|cfg| match cfg {
                StageConfig::Backend(be) => Some(be),
                _ => None,
            })
            .expect("Backend stage produced");
        let agg = backend_cfg
            .aggregations
            .first()
            .expect("at least one aggregation");
        assert!(
            agg.grouping.is_empty(),
            "L5 walk cannot recover label names from canonical L3 \
             ColumnIds (Step γ TODO in column_resolution); \
             BackendAggregation.grouping must come from the workload \
             patch in handle_plan — got {:?}",
            agg.grouping
        );
    }
}

#[cfg(test)]
mod realization_failures {
    use super::*;
    use crate::physical::colored_dag::{AllocateError, Topology};
    use crate::physical::post_asap::PhysicalExpr;

    // Unsupported topology retains the allocator's typed rejection.
    #[test]
    fn unsupported_realization_retains_reason() {
        let expr = PhysicalExpr::RawAtEdgePrometheusArchive {
            metric: "m".into(),
            window: None,
            label_proj: vec![],
        };
        for topology in [Topology::SingleStage, Topology::ZeroStage] {
            let error = try_split_typed(&expr, topology).unwrap_err();
            assert_eq!(
                error.downcast_ref::<AllocateError>(),
                Some(&AllocateError::UnsupportedTopology(topology))
            );
        }
    }
}
