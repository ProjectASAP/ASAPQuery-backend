//! L5 stage split — assign the L4 `PhysicalExpr` DAG across pipeline stages.
//!
//! The control plane's L5: take the sketch-bound `PhysicalExpr` produced
//! by `sketch_algebra::bind_query_expr`, colour its DAG by `StageId`
//! across the DC three-stage topology (`crate::physical::colored_dag::
//! StageAllocator` + `ThreeStageEmitter`), and return one
//! `StageConfig` per stage for the per-stage emitters in
//! `crate::emit::stage_config` to materialise into wire bytes.
//!
//! An earlier `QueryExpr`-consuming, `StagedPlan`-producing path
//! (`split_expr_by_stage`) was the redundant second L5 — it has been
//! retired; `split_typed_three_stage` below is the sole L5.

/// Env-var that opts *out* of the typed L5 stage_split path. The typed
/// path — `sketch_algebra::PhysicalExpr` (L4) → `split_typed_three_stage`
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
/// Returns `None` when stage allocation errors out (unsupported topology
/// shape, unresolved `Ref`, empty backend). Each per-stage config the
/// returned map carries is materialised into wire bytes by the emitters
/// in [`crate::emit::stage_config`] — `emit_edge_yaml` for `Edge`,
/// `emit_gateway_yaml` for `Gateway`, `emit_backend_streaming_config_json`
/// for `Backend`.
pub fn split_typed_three_stage(
    expr: &crate::sketch_algebra::PhysicalExpr,
) -> Option<
    std::collections::HashMap<
        crate::physical::colored_dag::StageId,
        crate::physical::colored_dag::StageConfig,
    >,
> {
    use crate::physical::colored_dag::{Emitter, StageAllocator, ThreeStageEmitter, Topology};
    let dag = StageAllocator.allocate(expr, Topology::ThreeStage).ok()?;
    ThreeStageEmitter.emit_per_stage(&dag).ok()
}
