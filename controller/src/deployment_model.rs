//! `DeploymentModelRegistry` + `DeploymentModelId` + `DeploymentModel`
//! — the design.md §5 `core::registry` surface.
//!
//! A deployment model bundles the controller-side knowledge that
//! distinguishes one runtime topology from another:
//!
//! * **Topology** — which logical roles (Edge / Gateway / Backend) the
//!   deployment exposes and how they connect.
//! * **Rule set** — which L4 [`crate::optimizer::OptimizerRule`]s the
//!   per-cycle plan optimizer should consult.
//! * **Emitter set** — which L5 plan emitters produce the wire-format
//!   payloads the deployment's transports expect.
//!
//! Per `controller/docs/design.md` §5, the long-term plan is one Cargo
//! crate per deployment model (`crates/deployment-model-asaplifecycle`,
//! `crates/deployment-model-asapquery`, `crates/deployment-model-asapfusion`).
//! The single-crate today ships exactly one deployment model
//! (`asaplifecycle`) so the registry is degenerate, but the shape lives
//! here so the future multi-crate migration is purely additive.

use std::collections::HashMap;

use crate::optimizer::OptimizerRule;
use crate::optimizer::engine::default_rules_as_optimizer_rules;

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

    /// String form of the id — same as `Self("…")`'s inner value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The set of emitter names a deployment model registers. We store
/// emitter NAMES (matching [`crate::emit::PlanEmitter::name`]) rather
/// than `Box<dyn PlanEmitter<…>>` because PlanEmitter has associated
/// types per output kind — homogeneous storage would require erasing
/// the `Input` / `Output` types and that erasure has no zero-cost
/// reading at the call site. The registry's job is metadata (which
/// emitters exist + which rules fire); concrete emitter construction
/// happens in `pipeline::run_pipeline` once the deployment model has
/// been selected.
#[derive(Debug, Clone, Default)]
pub struct EmitterSet {
    /// Emitter names this deployment model invokes during a planning
    /// cycle (e.g. `"opamp_edge_yaml"`, `"streaming_config_json"`,
    /// `"inference_config_json"`).
    pub emitters: Vec<String>,
}

impl EmitterSet {
    /// Construct an emitter set from a list of names.
    pub fn new(emitters: Vec<String>) -> Self {
        Self { emitters }
    }

    /// Whether the deployment model registers an emitter with the given
    /// name.
    pub fn has(&self, name: &str) -> bool {
        self.emitters.iter().any(|n| n == name)
    }
}

/// A deployment model — topology + rule set + emitter set. The
/// `rules` slot holds the L4 rule library this deployment consults;
/// the `emitters` slot holds the emitter names the per-cycle pipeline
/// invokes. Concrete emitter objects are constructed by the pipeline
/// driver (because `PlanEmitter` has per-emitter associated types).
pub struct DeploymentModel {
    /// Stable identifier — wire shape for selection.
    pub id: DeploymentModelId,
    /// L4 rule library this deployment consults during plan
    /// optimization.
    pub rules: Vec<Box<dyn OptimizerRule>>,
    /// L5 emitter names this deployment invokes during a planning
    /// cycle.
    pub emitters: EmitterSet,
}

impl DeploymentModel {
    /// Construct the `asaplifecycle` deployment model — the live
    /// multi-stage demo. Edge / Gateway / Backend topology; uses the
    /// engine's default rule set; registers the three emitter names
    /// the demo pipeline pushes (`opamp_edge_yaml`,
    /// `streaming_config_json`, `inference_config_json`).
    pub fn asaplifecycle() -> Self {
        Self {
            id: DeploymentModelId::asaplifecycle(),
            rules: default_rules_as_optimizer_rules(),
            emitters: EmitterSet::new(vec![
                "opamp_edge_yaml".to_string(),
                "opamp_gateway_yaml".to_string(),
                "streaming_config_json".to_string(),
                "inference_config_json".to_string(),
            ]),
        }
    }
}

impl std::fmt::Debug for DeploymentModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeploymentModel")
            .field("id", &self.id)
            .field("rule_count", &self.rules.len())
            .field("rule_names", &self.rules.iter().map(|r| r.name()).collect::<Vec<_>>())
            .field("emitters", &self.emitters)
            .finish()
    }
}

/// Registry of available deployment models.
///
/// The single-crate ships only the `asaplifecycle` deployment model.
/// `pipeline::run_pipeline` calls [`DeploymentModelRegistry::lookup`]
/// at the start of every planning cycle to pick the topology + rule
/// library + emitter set for the request.
pub struct DeploymentModelRegistry {
    entries: HashMap<DeploymentModelId, DeploymentModel>,
}

impl Default for DeploymentModelRegistry {
    fn default() -> Self {
        let mut r = Self {
            entries: HashMap::new(),
        };
        // Register the one deployment model the single-crate ships.
        r.register(DeploymentModel::asaplifecycle());
        r
    }
}

impl DeploymentModelRegistry {
    /// Empty registry — caller calls [`Self::register`] per model.
    pub fn empty() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a deployment model. Replaces any prior model with the
    /// same id; returns the prior value when one was present.
    pub fn register(&mut self, model: DeploymentModel) -> Option<DeploymentModel> {
        self.entries.insert(model.id.clone(), model)
    }

    /// Look up a deployment model by id. Returns `None` when the id
    /// is unknown.
    pub fn lookup(&self, id: &DeploymentModelId) -> Option<&DeploymentModel> {
        self.entries.get(id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::RuleCategory;

    #[test]
    fn default_registry_carries_asaplifecycle() {
        let reg = DeploymentModelRegistry::default();
        let id = DeploymentModelId::asaplifecycle();
        assert!(reg.contains(&id));
        let m = reg.lookup(&id).expect("asaplifecycle must be registered");
        // The default rule set carries the 12 engine rules.
        assert_eq!(m.rules.len(), 12, "asaplifecycle should ship the 12 engine rules");
        // Emitter set carries the three demo emitters.
        assert!(m.emitters.has("opamp_edge_yaml"));
        assert!(m.emitters.has("streaming_config_json"));
        assert!(m.emitters.has("inference_config_json"));
    }

    #[test]
    fn asaplifecycle_rules_cover_expected_categories() {
        let reg = DeploymentModelRegistry::default();
        let m = reg.lookup(&DeploymentModelId::asaplifecycle()).unwrap();
        use std::collections::HashSet;
        let cats: HashSet<RuleCategory> = m.rules.iter().map(|r| r.category()).collect();
        assert!(cats.contains(&RuleCategory::PushDown));
        assert!(cats.contains(&RuleCategory::Fusion));
        assert!(cats.contains(&RuleCategory::Elim));
        assert!(cats.contains(&RuleCategory::Cse));
        assert!(cats.contains(&RuleCategory::Decorrelate));
    }

    #[test]
    fn empty_registry_lookups_return_none() {
        let reg = DeploymentModelRegistry::empty();
        assert!(!reg.contains(&DeploymentModelId::asaplifecycle()));
        assert!(reg.lookup(&DeploymentModelId::asapquery()).is_none());
    }

    #[test]
    fn register_replaces_existing() {
        let mut reg = DeploymentModelRegistry::empty();
        let m1 = DeploymentModel::asaplifecycle();
        assert!(reg.register(m1).is_none());
        let m2 = DeploymentModel::asaplifecycle();
        // Second register returns the prior model.
        let prior = reg.register(m2);
        assert!(prior.is_some());
    }

    #[test]
    fn ids_iterator_lists_registered() {
        let reg = DeploymentModelRegistry::default();
        let ids: Vec<&DeploymentModelId> = reg.ids().collect();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], &DeploymentModelId::asaplifecycle());
    }

    #[test]
    fn deployment_model_id_string_form() {
        assert_eq!(DeploymentModelId::asaplifecycle().as_str(), "asaplifecycle");
        assert_eq!(DeploymentModelId::asapquery().as_str(), "asapquery");
        assert_eq!(DeploymentModelId::asapfusion().as_str(), "asapfusion");
    }
}
