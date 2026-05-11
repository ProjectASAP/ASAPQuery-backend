//! `PlanEmitter` trait — placeholder scaffolding for the L5 output
//! surface declared in `controller/docs/design.md` §5 / §6
//! `core::emit::PlanEmitter`.
//!
//! Refactor 2026-05 created this module so the design.md target layout
//! is materialised in code. The existing per-runtime emitters
//! ([`super::stage_config::emit_edge_yaml`], [`super::otap::emit_otap_dag_yaml`],
//! [`super::telegraf::emit_telegraf_toml`], [`super::asapquery_backend::generate_streaming_config_yaml`],
//! [`super::agent::generate_agent_config`], [`super::backend::generate_backend_config`])
//! still operate as free functions with deployment-specific signatures.
//! Migrating each onto this trait is a follow-up — until then this
//! module is intentionally minimal so it has zero behavior impact.

#![allow(dead_code)]

use anyhow::Result;

/// `PlanEmitter` — every per-deployment-model plan emitter implements
/// this so a future controller pipeline can call them polymorphically.
///
/// The `Input` associated type is the typed L5 stage config the emitter
/// consumes (e.g. `EdgeStageConfig` for OTel YAML emitters,
/// `BackendStageConfig` for the ASAPQuery-backend `StreamingConfig`
/// emitter). `Output` is the wire-format string / JSON the deployment
/// model's transport expects.
pub trait PlanEmitter: Send + Sync {
    /// Typed L5 stage config this emitter consumes.
    type Input;
    /// Wire-format output produced for the deployment model's transport.
    type Output;

    /// Stable name for diagnostics + the emitter registry.
    fn name(&self) -> &'static str;

    /// Emit the wire-format payload for the given stage config.
    fn emit(&self, input: &Self::Input) -> Result<Self::Output>;
}
