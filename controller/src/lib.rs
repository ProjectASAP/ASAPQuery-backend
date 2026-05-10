//! Controller crate — library surface for the ASAPQuery-backend host.
//!
//! Refactor-2026-05 (Phase 9): the controller previously ran as a
//! standalone binary with its own OpAMP server and HTTP API. After the
//! controller crate moved into ASAPQuery-backend, the same modules are
//! exposed as a Rust library so `asap-query-engine` can call them
//! in-process — capability mapping, plan emission, OpAMP push from the
//! backend host. This `lib.rs` declares the public module surface; the
//! existing `main.rs` continues to provide the standalone binary
//! entrypoint for any deployments that still want to run controller
//! out-of-process.
//!
//! Public modules to consume from `asap-query-engine`:
//! - `sketch_algebra` — Capability enum + `capability_for(query_func)`
//!   lookup table (Phase 4 / 5 use this to route raw-name PromQL).
//! - `intent_algebra` — `AggIntent` + `QueryExpr` DAG.
//! - `language_logical_plan` — PromQL → AST → logical plan.
//! - `query_parser` / `query_language` — front-end parsers.
//! - `planner` — full L1→L5 pipeline runner.
//! - `stage_split` — per-stage YAML emission.
//! - `opamp` — OpAMP server (will be invoked from the backend's
//!   service startup once Phase 4 wires the in-process integration).
//! - `types`, `types_v2` — controller-internal data model.
//!
//! NOT intended for public consumption from outside the workspace —
//! these modules expose the controller's L1–L5 internals and are not
//! part of any wire/protocol contract.

pub mod accuracy;
pub mod algebra;
pub mod analyzer;
pub mod backend_client;
pub mod config;
pub mod intent_algebra;
pub mod language_logical_plan;
pub mod metrics_exposer;
pub mod monitor;
pub mod opamp;
pub mod planner;
pub mod query_language;
pub mod query_parser;
pub mod replan;
pub mod runtime_samples;
pub mod sketch_algebra;
pub mod stage_split;
pub mod store;
pub mod types;
pub mod types_v2;
