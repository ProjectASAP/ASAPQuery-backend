//! SP-8 re-planning automation.
//!
//! [`Replanner`] closes the feedback loop from the [`monitor::Scraper`] back
//! to the planner.  Two triggers drive re-planning:
//!
//! 1. **Violation-triggered**: when the scraper fires an SLA violation callback
//!    the replanner looks up which metric the violating agent is serving and
//!    immediately requests a fresh plan.
//!
//! 2. **Expiry-triggered**: a periodic ticker calls [`Replanner::replan_expired`]
//!    to re-plan any metric whose `CollectionPlan::valid_until` has passed.
//!
//! After a plan is updated the replanner pushes role-appropriate OTel YAML to
//! all connected collectors via OpAMP and updates scraper endpoint sketch-type
//! bookkeeping so the EMA cost model receives correctly attributed updates.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::backend_client::{push_or_log, BackendClient};
use crate::emit::{
    build_precompute_jobs, collect_metric_to_family, emit_for_runtime,
    extend_edge_with_demo_plumbing, generate_agent_config, generate_backend_config,
    generate_streaming_config_yaml, AgentRuntime, WorkloadRegistry,
};
use crate::monitor::Scraper;
use crate::opamp::{AgentRole, OpampServer, RemoteConfig};
use crate::optimizer::baseline::BaselinePlanner;
use crate::optimizer::{cost as cost_model, rules};
use crate::physical::stage_split;
use crate::store::{PlanStore, WorkloadStore};
use crate::types::QueryWorkload;

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ── Replanner ─────────────────────────────────────────────────────────────────

pub struct Replanner {
    planner: Arc<BaselinePlanner>,
    plan_store: Arc<PlanStore>,
    workload_store: Arc<WorkloadStore>,
    opamp: Arc<OpampServer>,
    scraper: Arc<Scraper>,
    opamp_endpoint: String,
    /// Optional client for pushing newly-generated `StreamingConfig`
    /// YAML to the ASAPQuery-backend's `/api/v1/streaming-config`
    /// endpoint. When present, every successful replan POSTs the new
    /// plan to the backend in addition to the existing OpAMP pushes
    /// to agent-role and backend-role collectors. Configured via the
    /// `CONTROLLER_BACKEND_ENDPOINT` env var; defaults to `None` so
    /// existing deployments that don't yet run ASAPQuery-backend
    /// behave exactly as before.
    backend_client: Option<Arc<BackendClient>>,
    /// Optional handle to the controller-wide [`WorkloadRegistry`].
    /// Used only by the typed-emit path
    /// ([`Replanner::try_emit_typed_edge_yaml`]) to extend the edge
    /// stage config with the bootstrap-scope archive-tier metrics so
    /// the OpAMP-pushed YAML matches what the bootstrap GET path
    /// emits via `main::emit_bootstrap_typed`. When unset the typed
    /// path still works — it just skips the workload-registry archive
    /// extension and only adds the freshness probes.
    workload_registry: Option<Arc<WorkloadRegistry>>,
    /// Maps agent_id → metric_name so violation callbacks can look up which
    /// metric a particular agent is serving.
    agent_to_metric: Arc<RwLock<HashMap<String, String>>>,
}

impl Replanner {
    pub fn new(
        planner: Arc<BaselinePlanner>,
        plan_store: Arc<PlanStore>,
        workload_store: Arc<WorkloadStore>,
        opamp: Arc<OpampServer>,
        scraper: Arc<Scraper>,
        opamp_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            planner,
            plan_store,
            workload_store,
            opamp,
            scraper,
            opamp_endpoint: opamp_endpoint.into(),
            backend_client: None,
            workload_registry: None,
            agent_to_metric: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Attach a [`BackendClient`] so every replan also pushes the new
    /// `StreamingConfig` YAML to the ASAPQuery-backend via HTTP.
    /// Builder-style — call during controller startup in `main.rs`.
    /// Without this call, replans continue to push only via OpAMP and
    /// the ASAPQuery-backend (if running) keeps its startup config.
    pub fn with_backend_client(mut self, client: Arc<BackendClient>) -> Self {
        self.backend_client = Some(client);
        self
    }

    /// Attach the controller-wide [`WorkloadRegistry`] so the typed
    /// emit path (gated by `USE_TYPED_STAGE_SPLIT`) can extend the
    /// edge stage config with the workload-registry archive metrics
    /// — same demo-scope plumbing the bootstrap GET path applies in
    /// `main::emit_bootstrap_typed`. Builder-style; safe to omit
    /// (the typed path falls back to freshness-probe-only extension).
    pub fn with_workload_registry(mut self, registry: Arc<WorkloadRegistry>) -> Self {
        self.workload_registry = Some(registry);
        self
    }

    // ── Agent registry ────────────────────────────────────────────────────────

    /// Record that `agent_id` is serving `metric`. Called from `handle_plan`
    /// after pushing configs so violations can be mapped back to a metric.
    pub async fn register_agent(&self, agent_id: impl Into<String>, metric: impl Into<String>) {
        self.agent_to_metric
            .write()
            .await
            .insert(agent_id.into(), metric.into());
    }

    /// Remove the mapping for a disconnected agent.
    pub async fn unregister_agent(&self, agent_id: &str) {
        self.agent_to_metric.write().await.remove(agent_id);
    }

    /// Returns a read-only reference to the agent→metric mapping so that
    /// callers (e.g. the on_connect callback) can check if an agent has a
    /// prior assignment.
    pub fn agent_to_metric(&self) -> &Arc<RwLock<HashMap<String, String>>> {
        &self.agent_to_metric
    }

    // ── Config push helpers ──────────────────────────────────────────────────

    /// Try to emit edge YAML via the typed L5 pipeline for `metric`,
    /// matching `main::emit_bootstrap_typed`'s flow.
    ///
    /// Steps:
    ///   1. `bind_workload_typed(&workload)` → SketchExpr
    ///   2. `split_typed_three_stage(&sketch_expr)` → per-stage configs
    ///   3. Pick the `Edge` stage config
    ///   4. Apply `extend_edge_with_demo_plumbing` (freshness probes +
    ///      workload-registry archive metrics) so the OpAMP-pushed YAML
    ///      matches what the bootstrap GET path emits
    ///   5. `emit_for_runtime(AsapOtel, &edge, …)` → YAML string
    ///
    /// Defaults the runtime to `AgentRuntime::AsapOtel` (mirrors the
    /// bootstrap default when no `X-Agent-Runtime` header is present;
    /// the OpAMP `on_connect` payload doesn't surface a per-agent
    /// runtime today). If a future commit threads runtime info through
    /// OpAMP, swap the default for a per-agent lookup.
    ///
    /// Returns `None` whenever the typed path can't satisfy the
    /// request (workload missing from store, `bind_workload_typed`
    /// declines the shape, no `Edge` entry, emit failure) — caller
    /// then falls back to the legacy emitter.
    fn try_emit_typed_edge_yaml(&self, metric: &str) -> Option<String> {
        let (workload, _wc) = self.workload_store.get(metric)?;
        self.try_emit_typed_edge_yaml_for_workload(&workload)
    }

    /// Same as [`try_emit_typed_edge_yaml`] but takes the
    /// `QueryWorkload` directly. Used by `replan_metric` which already
    /// has the workload in scope.
    fn try_emit_typed_edge_yaml_for_workload(&self, workload: &QueryWorkload) -> Option<String> {
        let sketch_expr = rules::bind_workload_typed(workload)?;
        let configs = stage_split::split_typed_three_stage(&sketch_expr)?;
        let mut edge_cfg = configs.into_iter().find_map(|(_, cfg)| match cfg {
            crate::physical::colored_dag::StageConfig::Edge(edge) => Some(edge),
            _ => None,
        })?;

        // Apply the bootstrap-scope demo plumbing — freshness probes
        // + workload-registry archive metrics — so the OpAMP-pushed
        // YAML carries the SAME `gorillas3` + `routing` +
        // `metrics/warm_passthrough` blocks the bootstrap GET path
        // emits. Without this, an agent that reconnects gets edge
        // YAML missing freshness-probe routing → criterion ⑥ fails
        // for any plan-pinned agent.
        let registry_metrics: Vec<String> = self
            .workload_registry
            .as_ref()
            .map(|r| r.entries().iter().map(|e| e.metric_name.clone()).collect())
            .unwrap_or_default();
        extend_edge_with_demo_plumbing(&mut edge_cfg, registry_metrics);

        // MVP §46 — stitch planner per-metric output into the emitter's
        // `metric_to_family` map so the 5-sketch routing-connector wire
        // shape activates on the OpAMP-pushed YAML too. Mirror of the
        // bootstrap path's stitch in `main::emit_bootstrap_typed` —
        // without this an agent that reconnects (or a metric that
        // replans) gets a single-pipeline YAML, even though the
        // bootstrap GET path it received first carried the routing
        // connector. When `workload_registry` is None (test fixture),
        // skip the stitch — the legacy single-pipeline emit still
        // covers correctness for the metric being replanned.
        if let Some(registry) = self.workload_registry.as_ref() {
            edge_cfg.metric_to_family = collect_metric_to_family(registry, &self.workload_store);
        }

        // OpAMP `on_connect` doesn't expose the agent's runtime
        // header, so default to `AsapOtel` — matches the bootstrap
        // default for legacy / unspecified clients. This is the same
        // assumption `main::handle_plan`'s typed push path makes
        // (`push_to_role(Agent, …)` with edge YAML, no runtime
        // dispatch).
        emit_for_runtime(
            AgentRuntime::AsapOtel,
            &edge_cfg,
            &self.opamp_endpoint,
            None,
        )
        .ok()
    }

    /// Push the current plan config to a specific agent.
    ///
    /// Looks up the metric assigned to this agent, retrieves the plan from
    /// `plan_store`, generates agent YAML, and pushes via OpAMP.
    /// Returns `true` if config was pushed, `false` if the agent has no
    /// metric assignment or no plan exists for that metric.
    ///
    /// ## Behaviour matrix
    ///
    /// | `USE_TYPED_STAGE_SPLIT` | path |
    /// | --- | --- |
    /// | unset / `0` | **legacy** — emit a single-pipeline DDSketch YAML via [`generate_agent_config`]. No routing, no `gorillas3`, no warm-passthrough. Backwards-compat for deployments that haven't migrated. |
    /// | `1` / `true` / `yes` | **typed** — run [`try_emit_typed_edge_yaml`] (mirror of `main::emit_bootstrap_typed`). On error, fall back to the legacy emitter so the push never silently drops. |
    ///
    /// Together with the bootstrap GET path (PR #333) this finishes
    /// the OpAMP-on-connect side of the typed emit so reconnecting
    /// agents receive the same routed YAML as fresh-connect agents.
    pub async fn push_config_to_agent(&self, agent_id: &str) -> bool {
        let metric = self.agent_to_metric.read().await.get(agent_id).cloned();
        let Some(metric) = metric else { return false };

        let Ok(plan) = self.plan_store.get(&metric) else {
            return false;
        };

        let yaml = if stage_split::typed_stage_split_enabled() {
            match self.try_emit_typed_edge_yaml(&metric) {
                Some(y) => {
                    info!(
                        agent = agent_id, metric = %metric, bytes = y.len(),
                        "[USE_TYPED_STAGE_SPLIT] pushed typed edge YAML on connect"
                    );
                    y
                }
                None => {
                    warn!(
                        agent = agent_id, metric = %metric,
                        "[USE_TYPED_STAGE_SPLIT] typed emit failed on connect; \
                         falling back to legacy generate_agent_config"
                    );
                    match generate_agent_config(&plan.agent_config, &self.opamp_endpoint) {
                        Ok(y) => y,
                        Err(_) => {
                            warn!(agent = agent_id, metric = %metric, "failed to generate agent config on connect");
                            return false;
                        }
                    }
                }
            }
        } else {
            match generate_agent_config(&plan.agent_config, &self.opamp_endpoint) {
                Ok(y) => y,
                Err(_) => {
                    warn!(agent = agent_id, metric = %metric, "failed to generate agent config on connect");
                    return false;
                }
            }
        };

        self.opamp
            .push(
                agent_id,
                RemoteConfig {
                    config_hash: short_hash(&yaml),
                    yaml,
                },
            )
            .await;
        info!(agent = agent_id, metric = %metric, "pushed config to reconnecting agent");
        true
    }

    // ── Re-plan helpers ───────────────────────────────────────────────────────

    /// Re-plans a single metric and pushes updated configs.
    /// Returns `true` if re-planning succeeded, `false` if the metric is unknown.
    pub async fn replan_metric(&self, metric: &str) -> bool {
        let Some((workload, wc)) = self.workload_store.get(metric) else {
            warn!(metric, "replan requested but workload not found in store");
            return false;
        };

        info!(metric, "re-planning metric");

        // Reset the baseline so the cost model runs fresh rather than returning the
        // previously established baseline — the whole point of a re-plan is to
        // re-optimise with current EMA data.
        self.planner.reset(metric);
        let mut plan = self.planner.plan(&workload, Some(&wc));
        plan.precompute = build_precompute_jobs(&workload, &plan, "backend:4317");
        self.plan_store.set(metric, plan.clone());

        // Push agent config only to agents registered for this specific metric,
        // rather than broadcasting to all agent-role collectors. Same gate
        // as `push_config_to_agent` — typed path on, legacy fallback on
        // emit failure or when the gate is off.
        let agent_yaml: Option<String> = if stage_split::typed_stage_split_enabled() {
            match self.try_emit_typed_edge_yaml_for_workload(&workload) {
                Some(y) => {
                    info!(
                        metric,
                        bytes = y.len(),
                        "[USE_TYPED_STAGE_SPLIT] re-plan emitted typed edge YAML"
                    );
                    Some(y)
                }
                None => {
                    warn!(
                        metric,
                        "[USE_TYPED_STAGE_SPLIT] re-plan typed emit failed; \
                         falling back to legacy generate_agent_config"
                    );
                    generate_agent_config(&plan.agent_config, &self.opamp_endpoint).ok()
                }
            }
        } else {
            generate_agent_config(&plan.agent_config, &self.opamp_endpoint).ok()
        };
        if let Some(yaml) = agent_yaml {
            let cfg = RemoteConfig {
                config_hash: short_hash(&yaml),
                yaml,
            };
            let agents = self.agent_to_metric.read().await;
            let target_agents: Vec<String> = agents
                .iter()
                .filter(|(_, m)| m.as_str() == metric)
                .map(|(id, _)| id.clone())
                .collect();
            drop(agents);
            for agent_id in target_agents {
                self.opamp.push(&agent_id, cfg.clone()).await;
            }
        }
        if let Ok(yaml) = generate_backend_config(&plan.backend_config, &self.opamp_endpoint) {
            self.opamp
                .push_to_role(
                    AgentRole::Backend,
                    RemoteConfig {
                        config_hash: short_hash(&yaml),
                        yaml,
                    },
                )
                .await;
        }

        // Push the ASAPQuery-backend StreamingConfig YAML via HTTP if a
        // backend client is configured. This is the producer side of the
        // ASAPQuery PR E hot-reload contract: the backend receives the
        // new plan on its /api/v1/streaming-config endpoint and makes it
        // visible to the next query without restarting.
        if let Some(backend_client) = self.backend_client.as_ref() {
            match generate_streaming_config_yaml(metric, &plan) {
                Ok(yaml) => {
                    push_or_log(backend_client, metric, yaml).await;
                }
                Err(e) => {
                    warn!(
                        metric,
                        error = %e,
                        "failed to build ASAPQuery streaming-config YAML — \
                         skipping backend HTTP push for this replan cycle"
                    );
                }
            }
        }

        // Update scraper endpoint sketch types for correct EMA attribution.
        let sketch_type = plan.agent_config.sketch_type;
        for agent_id in self.opamp.connected_agents().await {
            self.scraper
                .set_sketch_type(&agent_id, sketch_type.clone())
                .await;
        }

        info!(metric, sketch_type = %sketch_type, "re-plan complete");
        true
    }

    /// Re-plans all metrics whose `valid_until` has already passed.
    pub async fn replan_expired(&self) {
        let expired = self.plan_store.expired(chrono::Utc::now());
        if expired.is_empty() {
            return;
        }
        info!(count = expired.len(), "re-planning expired metrics");
        for metric in expired {
            self.replan_metric(&metric).await;
        }
    }

    /// Called from the violation callback. Looks up the metric served by
    /// `agent_id` and triggers an immediate re-plan.
    pub async fn handle_violation(&self, agent_id: &str) {
        let metric = self.agent_to_metric.read().await.get(agent_id).cloned();
        match metric {
            Some(m) => {
                info!(agent = agent_id, metric = %m, "SLA violation → triggering re-plan");
                self.replan_metric(&m).await;
            }
            None => {
                warn!(
                    agent = agent_id,
                    "SLA violation but no metric mapping found; re-planning all expired"
                );
                self.replan_expired().await;
            }
        }
    }

    /// Starts a background loop that re-plans expired metrics every `interval`.
    pub async fn run_expiry_ticker(self: Arc<Self>, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.replan_expired().await;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use chrono::Utc;

    use crate::optimizer::baseline::BaselinePlanner;
    use crate::optimizer::cost::CostModelPlanner;
    use crate::store::{PlanStore, WorkloadStore};
    use crate::types::*;

    fn make_replanner() -> Arc<Replanner> {
        let plan_store = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let planner = Arc::new(BaselinePlanner::new(CostModelPlanner::new()));
        let opamp = Arc::new(crate::opamp::OpampServer::new());
        let scraper = Arc::new(crate::monitor::Scraper::new(
            vec![],
            crate::monitor::Thresholds::default(),
            Arc::new(|_| {}),
            Duration::from_secs(60),
        ));
        Arc::new(Replanner::new(
            planner,
            plan_store,
            workload_store,
            opamp,
            scraper,
            "ws://ctrl:4320/v1/opamp",
        ))
    }

    fn test_workload(metric: &str) -> (QueryWorkload, WorkloadCharacteristics) {
        let wl = QueryWorkload {
            metric_name: metric.into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        };
        (wl, WorkloadCharacteristics::default())
    }

    fn make_plan() -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: SketchType::DDSketch,
                sketch_params: SketchParams::DDSketch {
                    relative_accuracy: 0.01,
                    quantiles: vec![0.5, 0.99],
                },
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: None,
                mode: ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                enable_series_id: false,
                series_id_ttl_secs: 300,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: SketchType::DDSketch,
                group_by: vec![],
            },
            precompute: vec![],
            valid_until: Utc::now() + chrono::Duration::seconds(3600),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
            staged_plan: None,
        }
    }

    #[tokio::test]
    async fn replan_unknown_metric_returns_false() {
        let r = make_replanner();
        assert!(!r.replan_metric("unknown").await);
    }

    #[tokio::test]
    async fn replan_known_metric_updates_plan_store() {
        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", wl, wc);
        r.plan_store.set("latency", make_plan());

        let ok = r.replan_metric("latency").await;
        assert!(ok);
        // Plan store should now have a new entry (valid_until in the future).
        let updated = r.plan_store.get("latency").unwrap();
        assert!(updated.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn replan_expired_replans_only_expired() {
        let r = make_replanner();
        let (wl, wc) = test_workload("old");
        r.workload_store.set("old", wl, wc);

        // Insert an already-expired plan.
        let mut expired_plan = make_plan();
        expired_plan.valid_until = Utc::now() - chrono::Duration::seconds(60);
        r.plan_store.set("old", expired_plan);

        // Insert a still-active plan for "active".
        let (awl, awc) = test_workload("active");
        r.workload_store.set("active", awl, awc);
        r.plan_store.set("active", make_plan());

        r.replan_expired().await;

        // "old" should now have a freshly computed plan.
        let old_plan = r.plan_store.get("old").unwrap();
        assert!(old_plan.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn register_then_violation_replans_correct_metric() {
        let r = make_replanner();
        let (wl, wc) = test_workload("req_rate");
        r.workload_store.set("req_rate", wl, wc);
        r.plan_store.set("req_rate", make_plan());

        r.register_agent("agent-1", "req_rate").await;
        r.handle_violation("agent-1").await;

        // Plan should have been refreshed.
        assert!(r.plan_store.get("req_rate").is_ok());
    }

    #[tokio::test]
    async fn unregister_removes_mapping() {
        let r = make_replanner();
        r.register_agent("a1", "m").await;
        r.unregister_agent("a1").await;
        // After unregister, handle_violation falls back to replan_expired (no-op).
        r.handle_violation("a1").await; // should not panic
    }

    // ── Typed-emit path tests ─────────────────────────────────────────────────
    //
    // These tests exercise the `USE_TYPED_STAGE_SPLIT`-gated emit path
    // ported from `main::emit_bootstrap_typed` so the OpAMP-pushed YAML
    // matches what the bootstrap GET path returns. The acceptance bar
    // is criterion ⑥ on issue #46: the agent's edge pipeline must
    // include `gorillas3` (archive write), `routing` (warm-passthrough
    // dispatch), and the `metrics/warm_passthrough` pipeline.

    /// Serialises tests that mutate `USE_TYPED_STAGE_SPLIT`. Mirror of
    /// the guard in `main::tests` — `cargo test` runs tests in
    /// parallel by default and `typed_stage_split_enabled()` reads the
    /// env var on every call.
    static TYPED_ENV_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct TypedEnvGuard {
        previous: Option<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    impl TypedEnvGuard {
        fn enable() -> Self {
            let lock = TYPED_ENV_GUARD.lock().unwrap_or_else(|p| p.into_inner());
            let previous = std::env::var("USE_TYPED_STAGE_SPLIT").ok();
            std::env::set_var("USE_TYPED_STAGE_SPLIT", "1");
            Self {
                previous,
                _lock: lock,
            }
        }
    }
    impl Drop for TypedEnvGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(v) => std::env::set_var("USE_TYPED_STAGE_SPLIT", v),
                None => std::env::remove_var("USE_TYPED_STAGE_SPLIT"),
            }
        }
    }

    /// Given an agent runtime + a quantile workload registered in the
    /// workload store, the typed emit path produces YAML containing
    /// `gorillas3`, `routing`, and `metrics/warm_passthrough`.
    ///
    /// Proves freshness-probe routing reaches OpAMP-pushed agents —
    /// the legacy `generate_agent_config` path emits NONE of these
    /// (it builds a single-pipeline DDSketch YAML with no routing).
    #[tokio::test]
    async fn typed_replan_emit_includes_freshness_probe_routing() {
        let _env = TypedEnvGuard::enable();

        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", wl, wc);
        r.plan_store.set("latency", make_plan());

        let yaml = r
            .try_emit_typed_edge_yaml("latency")
            .expect("typed emit should succeed for a quantile workload");

        // gorillas3 — archive-tier write to MinIO. Without this the
        // warm-tier query engine has nothing to read for criterion ⑥.
        assert!(
            yaml.contains("gorillas3"),
            "typed emit must include the gorillas3 processor block:\n{yaml}"
        );

        // routing — OTTL routing processor that dispatches the
        // freshness probes to `metrics/warm_passthrough`. Without this
        // the DDSketch processor renames them to `_quantile`.
        assert!(
            yaml.contains("routing"),
            "typed emit must include the routing processor block:\n{yaml}"
        );

        // metrics/warm_passthrough — the bypass pipeline that carries
        // the freshness probe samples through gorillas3 + exporter
        // WITHOUT the sketch processor.
        assert!(
            yaml.contains("metrics/warm_passthrough"),
            "typed emit must include the metrics/warm_passthrough pipeline:\n{yaml}"
        );

        // Sanity: the freshness probe metric names appear in the YAML
        // (in the warm-passthrough route + the gorillas3 archive
        // metric list).
        assert!(
            yaml.contains("http_freshness_probe_warm"),
            "typed emit must reference the warm freshness probe metric:\n{yaml}"
        );
    }

    /// With `USE_TYPED_STAGE_SPLIT` unset, `push_config_to_agent`
    /// falls back to the legacy single-pipeline DDSketch emitter and
    /// produces YAML WITHOUT `gorillas3` / `routing` / warm-passthrough.
    /// This pins the gate semantics — without it a regression that
    /// always-on'd the typed path would silently break agents that
    /// can't yet handle the new processors.
    #[tokio::test]
    async fn legacy_path_omits_typed_processors_when_gate_off() {
        // Hold the env-guard lock so a parallel typed test can't
        // flip the var underneath us, and explicitly unset.
        let _lock = TYPED_ENV_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        let prior = std::env::var("USE_TYPED_STAGE_SPLIT").ok();
        std::env::remove_var("USE_TYPED_STAGE_SPLIT");

        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", wl, wc);
        r.plan_store.set("latency", make_plan());

        // Drive the legacy emitter directly — same code
        // `push_config_to_agent` runs when the gate is off.
        let plan = r.plan_store.get("latency").unwrap();
        let yaml = generate_agent_config(&plan.agent_config, &r.opamp_endpoint)
            .expect("legacy emit should succeed");

        // Legacy single-pipeline DDSketch output has NONE of the
        // typed-path processors.
        assert!(
            !yaml.contains("gorillas3"),
            "legacy path must not emit gorillas3 processor:\n{yaml}"
        );
        assert!(
            !yaml.contains("metrics/warm_passthrough"),
            "legacy path must not emit warm-passthrough pipeline:\n{yaml}"
        );

        // Restore.
        match prior {
            Some(v) => std::env::set_var("USE_TYPED_STAGE_SPLIT", v),
            None => std::env::remove_var("USE_TYPED_STAGE_SPLIT"),
        }
    }
}
