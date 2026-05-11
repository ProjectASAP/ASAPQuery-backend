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

// 2026-05 layered-cleanup follow-up: the back-compat shims previously
// defined here (`pub use emit as config`, `pub use pipeline as analyzer`,
// `pub mod algebra { … }`, `pub use physical::colored_dag as stage_split`,
// `pub mod planner { … }`) have been removed. `main.rs` and other
// consumers now reference the canonical module names directly
// (`emit`, `pipeline`, `intent_algebra::legacy_expr`, `optimizer`,
// `physical`, `physical::colored_dag`, etc.) per the layered-cleanup
// follow-up task.
/// PromQL → warm-tier candidate analyzer. Phase-9 unification of the
/// per-`Capability` dispatch knowledge that previously lived in
/// `asap-query-engine/src/engines/warm_tier/promql_extract.rs`. See
/// the module docs for the full PromQL shape coverage matrix.
pub mod warm_tier_analysis;

