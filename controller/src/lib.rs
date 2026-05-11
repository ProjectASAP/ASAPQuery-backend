//! Controller crate — library surface for the ASAPQuery-backend host.
//!
//! Refactor-2026-05 (Phase 9 / `refactor/controller-layered-cleanup`):
//! the controller previously ran as a standalone binary with its own
//! OpAMP server and HTTP API. After the controller crate moved into
//! ASAPQuery-backend, the same modules are exposed as a Rust library so
//! `asap-query-engine` can call them in-process — capability mapping,
//! plan emission, OpAMP push from the backend host. This `lib.rs`
//! declares the public module surface; the existing `main.rs` continues
//! to provide the standalone binary entrypoint for any deployments that
//! still want to run controller out-of-process.
//!
//! ## 2026-05 layered-cleanup refactor — old → new module mapping
//!
//! The internal module layout was restructured to mirror
//! `controller/docs/design.md` §5 target layout without splitting into
//! multiple crates:
//!
//! | Old path | New path |
//! |---|---|
//! | `controller/src/algebra/expr.rs` | `controller/src/intent_algebra/legacy_expr.rs` |
//! | `controller/src/algebra/lower.rs` | `controller/src/intent_algebra/legacy_lower.rs` |
//! | `controller/src/algebra/directory.rs` | `controller/src/physical/sketch_catalog.rs` |
//! | `controller/src/algebra/physical.rs` | `controller/src/physical/planner.rs` |
//! | `controller/src/algebra/allocator.rs` | `controller/src/physical/allocator.rs` |
//! | `controller/src/algebra/plan.rs` | `controller/src/physical/plan.rs` |
//! | `controller/src/algebra/optimizer.rs` | `controller/src/optimizer/engine.rs` |
//! | `controller/src/planner/cost_model.rs` | `controller/src/optimizer/cost/mod.rs` |
//! | `controller/src/planner/{delta,online}_cost_model.rs` | `controller/src/optimizer/cost/{delta,online}.rs` |
//! | `controller/src/planner/{pareto,tco,wire_cost}.rs` | `controller/src/optimizer/cost/{pareto,tco,wire}.rs` |
//! | `controller/src/planner/rules.rs` | `controller/src/optimizer/rules/mod.rs` |
//! | `controller/src/planner/baseline_planner.rs` | `controller/src/optimizer/baseline.rs` |
//! | `controller/src/planner/stage_split.rs` | `controller/src/physical/stage_split.rs` |
//! | `controller/src/analyzer.rs` | `controller/src/pipeline.rs` |
//! | `controller/src/stage_split/` | `controller/src/physical/colored_dag/` |
//! | `controller/src/query_language/` | `controller/src/query_parser/language/` |
//! | `controller/src/config/workloads.rs` | `controller/src/workload.rs` |
//! | `controller/src/config/{stage_config*,agent,backend,asapquery_backend,precompute}.rs` | `controller/src/emit/{...}.rs` |
//!
//! Public modules to consume from `asap-query-engine`:
//! - `sketch_algebra` — `Capability` enum + `capability_for(agg_intent: &AggIntent)`
//!   lookup table (Phase 4 / 5 use this to route raw-name PromQL).
//! - `intent_algebra` — `AggIntent` + `QueryExpr` DAG (canonical L3 IR;
//!   `legacy_expr` / `legacy_lower` carry the older `algebra::expr`-flavored
//!   IR pending full migration).
//! - `language_logical_plan` — PromQL → AST → logical plan.
//! - `query_parser` — front-end parsers (Layer 1).
//!   The former top-level `query_language/` module was folded into
//!   `query_parser::language` by this refactor.
//! - `physical` — L5 framework (allocator, planner, plan, sketch_catalog,
//!   colored_dag, stage_split, topology).
//! - `optimizer` — L4 rule engine + cost model traits/impls + baseline
//!   planner.
//! - `opamp` — OpAMP server (will be invoked from the backend's
//!   service startup once Phase 4 wires the in-process integration).
//! - `types`, `types_v2` — controller-internal data model.
//!
//! NOT intended for public consumption from outside the workspace —
//! these modules expose the controller's L1–L5 internals and are not
//! part of any wire/protocol contract.

pub mod accuracy;
pub mod backend_client;
pub mod deployment_model;
pub mod emit;
pub mod intent_algebra;
pub mod language_logical_plan;
pub mod metrics_exposer;
pub mod monitor;
pub mod opamp;
pub mod optimizer;
pub mod physical;
pub mod pipeline;
pub mod query_parser;
pub mod replan;
pub mod runtime_samples;
pub mod sketch_algebra;
pub mod store;
pub mod types;
pub mod types_v2;
pub mod workload;

/// Back-compat alias — the legacy `crate::config` module surface,
/// re-exported from its new homes ([`emit`] for the per-deployment-model
/// emitters and [`workload`] for `WorkloadRegistry`). Refactor 2026-05
/// introduced this so `main.rs` keeps using `controller::config::*`
/// without source churn.
pub use emit as config;

// Refactor 2026-05: the legacy `analyzer` and `planner` module paths
// resolve into `pipeline` and the new `optimizer` + `physical` split
// respectively. Keeping `analyzer` as a module alias preserves the
// historical name on the `crate::analyzer::*` path for downstream
// callers (`controller::analyzer::QuerySpec` is the JSON-facing type
// in `main.rs` and the HTTP route handlers).
pub use pipeline as analyzer;

/// Back-compat shim — the legacy `crate::algebra` module surface,
/// re-exported from its new homes (`intent_algebra::legacy_expr`,
/// `intent_algebra::legacy_lower`, `optimizer::engine`,
/// `physical::sketch_catalog`, `physical::planner`,
/// `physical::allocator`, `physical::plan`).
///
/// Refactor 2026-05 introduced this shim so `main.rs` and other
/// consumers can keep using `algebra::QueryOptimizer`,
/// `algebra::SketchAllocator`, `algebra::physical::*`,
/// `algebra::optimizer::DeploymentConstraints`, etc. without source
/// churn. Future cleanup should migrate call sites to the new paths
/// and delete this shim.
pub mod algebra {
    pub use crate::intent_algebra::legacy_expr as expr;
    pub use crate::intent_algebra::legacy_lower as lower;
    pub use crate::physical::sketch_catalog as directory;
    pub use crate::physical::planner as physical;
    pub use crate::physical::allocator;
    pub use crate::physical::plan;
    pub use crate::optimizer::engine as optimizer;

    pub use crate::physical::allocator::SketchAllocator;
    pub use crate::physical::plan::{
        CostEstimate, ExecutionMode, PipelineStage, PlanNode, PlanSummary,
    };
    pub use crate::optimizer::engine::QueryOptimizer;
    pub use crate::intent_algebra::legacy_expr::{
        AggFunc, AggIntent, BinaryOpKind, QueryExpr, ScalarExpr, WindowKind, WindowSpec,
    };
}

/// Back-compat shim — the legacy `crate::stage_split` module surface,
/// re-exported from its new home [`physical::colored_dag`].
///
/// Refactor 2026-05 moved `controller/src/stage_split/` into
/// `controller/src/physical/colored_dag/` so the L5 typed colouring
/// framework sits beneath the `physical` umbrella. The alias preserves
/// `controller::stage_split::*` and `crate::stage_split::*` paths.
pub use physical::colored_dag as stage_split;

/// Back-compat shim — the legacy `crate::planner` module surface,
/// re-exported from its new homes:
///
/// - `planner::cost_model`, `planner::delta_cost_model`,
///   `planner::online_cost_model`, `planner::pareto`, `planner::tco`,
///   `planner::wire_cost` → [`optimizer::cost`] (+ submodules).
/// - `planner::rules` → [`optimizer::rules`].
/// - `planner::baseline_planner` → [`optimizer::baseline`].
/// - `planner::stage_split` → [`physical::stage_split`].
///
/// Refactor 2026-05 introduced this shim to avoid touching ~40
/// `crate::planner::*` sites in `main.rs` / `replan.rs` / tests.
/// Future cleanup should migrate call sites to the new paths and
/// delete this shim.
pub mod planner {
    pub use crate::optimizer::baseline as baseline_planner;
    pub use crate::optimizer::cost as cost_model;
    pub use crate::optimizer::cost::delta as delta_cost_model;
    pub use crate::optimizer::cost::online as online_cost_model;
    pub use crate::optimizer::cost::pareto;
    pub use crate::optimizer::cost::tco;
    pub use crate::optimizer::cost::wire as wire_cost;
    pub use crate::optimizer::rules;
    pub use crate::physical::stage_split;

    // Top-level convenience re-exports that historically lived at
    // `crate::planner::*`. The legacy `planner/mod.rs` was 19 lines —
    // these are the symbols it surfaced.
    pub use crate::optimizer::baseline::BaselinePlanner;
    pub use crate::optimizer::cost::CostModelPlanner;
    pub use crate::optimizer::cost::online::{init_store as init_online_store, OnlineMetricsStore};
    pub use crate::optimizer::cost::pareto::{
        pareto_frontier, select_best, ObjectiveWeights, ParetoPoint,
    };
    pub use crate::optimizer::cost::wire::{
        break_even_samples, est_wire_bytes_per_window_per_series, select_bind_mode, BindMode,
        SketchWireCost, WireCostTable, WireWorkload,
    };
    pub use crate::optimizer::rules::RulesPlanner;
}
/// PromQL → warm-tier candidate analyzer. Phase-9 unification of the
/// per-`Capability` dispatch knowledge that previously lived in
/// `asap-query-engine/src/engines/warm_tier/promql_extract.rs`. See
/// the module docs for the full PromQL shape coverage matrix.
pub mod warm_tier_analysis;

