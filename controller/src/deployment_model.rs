//! `DeploymentModelRegistry` + `DeploymentModelId` — placeholder
//! scaffolding for the design.md §5 `core::registry` surface.
//!
//! Refactor 2026-05 created this module so the design.md target layout
//! is materialised in code. The single-crate today (no separate
//! `deployment-model-asaplifecycle` / `deployment-model-asapquery` /
//! `deployment-model-asapfusion` crates) means there is exactly one
//! deployment model and the registry is degenerate. The shape is
//! defined here so the future multi-crate migration is purely additive.

#![allow(dead_code)]

use std::collections::HashMap;

/// Stable identifier for a deployment model — `asaplifecycle`,
/// `asapquery`, `asapfusion`, etc. The string is the wire identifier
/// used in `QuerySpec::deployment_model` (see `crate::pipeline::QuerySpec`)
/// and in the per-deployment-model crate selection on a future `bin/`
/// build.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeploymentModelId(pub String);

impl DeploymentModelId {
    /// Default DC deployment id — the controller's historical name for
    /// the lifecycle deployment model (edge / gateway / backend OTel
    /// collectors).
    pub fn asaplifecycle() -> Self {
        Self("asaplifecycle".to_string())
    }

    /// `asapquery` — the ASAPQuery-backend in-process planner for
    /// `streaming_config.yaml` + `inference_config.yaml` emission.
    pub fn asapquery() -> Self {
        Self("asapquery".to_string())
    }

    /// `asapfusion` — the DataFusion `LogicalPlan` rewriter.
    pub fn asapfusion() -> Self {
        Self("asapfusion".to_string())
    }
}

/// Registry of available deployment models.
///
/// Today this is a stub — the single-crate ships only the
/// `asaplifecycle` deployment model and the registry contains one
/// entry. The shape is here so the future multi-crate migration can
/// populate it with concrete `Box<dyn DeploymentModel>` entries without
/// reworking the controller's startup flow.
#[derive(Default)]
pub struct DeploymentModelRegistry {
    /// Map of deployment model id → opaque marker. Reserved for the
    /// `dyn DeploymentModel` trait object that will land with the
    /// multi-crate split.
    entries: HashMap<DeploymentModelId, ()>,
}

impl DeploymentModelRegistry {
    /// Register a deployment model id. Returns `true` when the id was
    /// added; `false` when it was already present (the registry is a
    /// set today, not a map).
    pub fn register(&mut self, id: DeploymentModelId) -> bool {
        self.entries.insert(id, ()).is_none()
    }

    /// Whether the registry knows about a deployment model id.
    pub fn contains(&self, id: &DeploymentModelId) -> bool {
        self.entries.contains_key(id)
    }

    /// Iterate over the known deployment model ids.
    pub fn ids(&self) -> impl Iterator<Item = &DeploymentModelId> {
        self.entries.keys()
    }
}
