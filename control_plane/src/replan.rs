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

use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::backend_client::BackendClient;
use crate::emit::{
    build_precompute_engine_jobs, collect_metric_to_family, emit_for_runtime,
    extend_edge_with_demo_plumbing, generate_agent_collector_config, post_typed_backend_for_role,
    repost_cumulative_backend_config, AgentRuntime, PushOutcome, WorkloadRegistry,
};
use crate::monitor::Scraper;
use crate::opamp::{OpampServer, RemoteConfig};
use crate::physical::colored_dag::emitter::BackendStageConfig;
use crate::physical::plan_cache::CachedDeploymentPlanner;
use crate::physical::stage_split;
use crate::physical::workload_planner as rules;
use crate::store::{PlanStore, WorkloadStore};
use crate::types::QueryWorkload;
use crate::workload::AggRole;

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ── Replanner ─────────────────────────────────────────────────────────────────

pub struct Replanner {
    planner: Arc<CachedDeploymentPlanner>,
    plan_store: Arc<PlanStore>,
    workload_store: Arc<WorkloadStore>,
    opamp: Arc<OpampServer>,
    scraper: Arc<Scraper>,
    opamp_endpoint: String,
    /// Optional client for pushing newly-generated `StreamingConfig`
    /// JSON to the ASAPQuery-backend's `/api/v1/streaming-config`
    /// endpoint. When present, every successful replan POSTs the new
    /// plan to the backend through [`post_typed_backend_for_role`] —
    /// same typed cumulative path the HTTP `POST /api/v1/plan` handler
    /// in `main::handle_plan` uses. Configured via the
    /// `CONTROLLER_BACKEND_ENDPOINT` env var; defaults to `None` so
    /// existing deployments that don't yet run ASAPQuery-backend
    /// behave exactly as before.
    backend_client: Option<Arc<BackendClient>>,
    /// Shared per-`(metric, role)` `BackendStageConfig` cache used by
    /// [`post_typed_backend_for_role`]. Holding it on the `Replanner`
    /// means SLA-violation / plan-expiry replans, startup pre-pop
    /// ticks, and OpAMP on-connect ticks all derive their cumulative
    /// POST from the SAME state `handle_plan` writes to — so the
    /// data plane's atomic `handle.swap` swap never loses sibling
    /// `(metric, role)` aggregations.
    ///
    /// `None` when no backend is configured (the helper is still
    /// invoked — it logs and returns).
    backend_routing_cache: Option<Arc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>>>,
    /// Optional handle to the control-plane-wide [`WorkloadRegistry`].
    /// Used only by the typed-emit path
    /// ([`Replanner::try_emit_typed_edge_yaml`]) to extend the edge
    /// stage config with the bootstrap-scope archive-tier metrics so
    /// the OpAMP-pushed YAML matches what the bootstrap GET path
    /// emits via `main::emit_bootstrap_typed`. When unset the typed
    /// path still works — it just skips the workload-registry archive
    /// extension and only adds the freshness probes.
    workload_registry: Option<Arc<WorkloadRegistry>>,
    /// Maps `agent_id → Vec<(metric_name, role)>` so violation callbacks
    /// can look up which `(metric, role)` pairs a particular agent is
    /// serving. **B2 restructure**: one agent can serve multiple
    /// `(metric, role)` pairs (e.g. an agent handling all three of
    /// `http_requests_total`'s roles registered by `mvp-workload.yaml`
    /// entries 2/3/4). The vector preserves insertion order.
    agent_to_metrics: Arc<RwLock<HashMap<String, Vec<(String, AggRole)>>>>,
}

impl Replanner {
    pub fn new(
        planner: Arc<CachedDeploymentPlanner>,
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
            backend_routing_cache: None,
            workload_registry: None,
            agent_to_metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Attach a [`BackendClient`] so every replan also pushes the new
    /// typed cumulative `StreamingConfig` + `BackendStorageRouting`
    /// JSON to the ASAPQuery-backend via HTTP — through the same
    /// [`post_typed_backend_for_role`] helper `handle_plan` uses, so
    /// the data plane's atomic swap never loses sibling `(metric,
    /// role)` aggregations.
    ///
    /// Builder-style — call during control plane startup in `main.rs`.
    /// Without this call, replans continue to push only via OpAMP and
    /// the ASAPQuery-backend (if running) keeps its startup config.
    pub fn with_backend_client(mut self, client: Arc<BackendClient>) -> Self {
        self.backend_client = Some(client);
        self
    }

    /// Attach the shared per-`(metric, role)` `BackendStageConfig`
    /// cache so the typed cumulative emit reads from + writes to the
    /// SAME state `main::handle_plan` mutates. Without this the
    /// Replanner's cumulative POSTs would derive from an empty
    /// cache and overwrite `handle_plan`'s state on every fire.
    ///
    /// Builder-style; safe to omit (the typed emit still works — it
    /// just operates on a Replanner-local cache, which is fine when
    /// the Replanner is the sole producer, e.g. in unit tests).
    pub fn with_backend_routing_cache(
        mut self,
        cache: Arc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>>,
    ) -> Self {
        self.backend_routing_cache = Some(cache);
        self
    }

    /// Attach the control-plane-wide [`WorkloadRegistry`] so the typed
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

    /// Record that `agent_id` is serving `(metric, role)`. Called from
    /// `handle_plan` after pushing configs so violations can be mapped
    /// back. Idempotent: re-registering the same `(metric, role)` for
    /// the same agent leaves the vector unchanged (dedup).
    ///
    /// **B2 contract**: an agent may serve MULTIPLE `(metric, role)`
    /// pairs concurrently (the controller may push a single agent the
    /// edge YAML for every role of every metric it owns). Each call
    /// appends a new pair if not already present; the inverse
    /// [`unregister_agent`] drops all of them at once.
    pub async fn register_agent(
        &self,
        agent_id: impl Into<String>,
        metric: impl Into<String>,
        role: AggRole,
    ) {
        let key = (metric.into(), role);
        let mut map = self.agent_to_metrics.write().await;
        let entry = map.entry(agent_id.into()).or_default();
        if !entry.contains(&key) {
            entry.push(key);
        }
    }

    /// Remove every `(metric, role)` mapping for a disconnected agent.
    pub async fn unregister_agent(&self, agent_id: &str) {
        self.agent_to_metrics.write().await.remove(agent_id);
    }

    /// Returns a read-only reference to the agent→`(metric, role)` list
    /// mapping so callers (e.g. the on_connect callback) can check if an
    /// agent has a prior assignment.
    pub fn agent_to_metrics(&self) -> &Arc<RwLock<HashMap<String, Vec<(String, AggRole)>>>> {
        &self.agent_to_metrics
    }

    // ── Config push helpers ──────────────────────────────────────────────────

    /// Try to emit edge YAML via the typed L5 pipeline for `metric`,
    /// matching `main::emit_bootstrap_typed`'s flow.
    ///
    /// Steps:
    ///   1. `bind_workload_typed(&workload)` → PhysicalExpr
    ///   2. `split_typed_three_stage(&physical_expr)` → per-stage configs
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
    fn try_emit_typed_edge_yaml(
        &self,
        metric: &str,
        role: AggRole,
        agent_id: &str,
    ) -> Option<String> {
        let (workload, _wc) = self.workload_store.get(metric, role)?;
        self.try_emit_typed_edge_yaml_for_workload(&workload, agent_id)
    }

    /// Same as [`try_emit_typed_edge_yaml`] but takes the
    /// `QueryWorkload` directly. Used by `replan_metric` which already
    /// has the workload in scope.
    ///
    /// `agent_id` is threaded into the emitted opamp `X-Agent-ID` header
    /// (Issue #2). Callers per-agent pass the real id; broadcast callers
    /// pass `"$AGENT_ID"` and rely on the agent container's env.
    fn try_emit_typed_edge_yaml_for_workload(
        &self,
        workload: &QueryWorkload,
        agent_id: &str,
    ) -> Option<String> {
        let physical_expr = rules::bind_workload_typed(workload)?;
        let configs = stage_split::split_typed_three_stage(&physical_expr)?;
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
            // MVP blocker B3 — companion stitch: per-metric grouping
            // labels so the emitter prepends a `transform/keep_for_*`
            // OTTL processor in front of every sketch pipeline.
            edge_cfg.metric_to_grouping_labels =
                crate::emit::collect_metric_to_grouping_labels(registry, &self.workload_store);
            // Issue #298 — companion stitch: Counter-shaped metrics
            // that need `cumulativetodelta` upstream of the routing
            // connector. Mirrors the bootstrap stitch in
            // `main::emit_bootstrap_typed` so OpAMP-pushed re-plans
            // carry the same processor declaration the first-connect
            // bootstrap YAML did.
            edge_cfg.cumulative_counter_metrics =
                crate::emit::collect_cumulative_counter_metrics(registry, &self.workload_store);
            // Per-metric sketch sampling probability — companion stitch,
            // mirrors the bootstrap path so OpAMP-pushed re-plans carry the
            // same `sample_p` knob the first-connect bootstrap YAML did.
            edge_cfg.metric_to_sample_p =
                crate::emit::collect_metric_to_sample_p(registry, &self.workload_store);
            // Per-metric cardinality hint — companion stitch, mirrors the
            // bootstrap path so OpAMP-pushed re-plans carry the same
            // `distinct_keys_per_window` HLL sparse/dense signal the
            // first-connect bootstrap YAML did.
            edge_cfg.metric_to_distinct_keys =
                crate::emit::collect_metric_to_distinct_keys(registry, &self.workload_store);
            // Per-metric inner item dimension — companion stitch, mirrors the
            // bootstrap path so OpAMP-pushed re-plans carry the same
            // `item_label` the first-connect bootstrap YAML did.
            edge_cfg.metric_to_item_label =
                crate::emit::collect_metric_to_item_label(registry, &self.workload_store);
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
            agent_id,
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
    /// | unset / `0` | **legacy** — emit a single-pipeline DDSketch YAML via [`generate_agent_collector_config`]. No routing, no `gorillas3`, no warm-passthrough. Backwards-compat for deployments that haven't migrated. |
    /// | `1` / `true` / `yes` | **typed** — run [`try_emit_typed_edge_yaml`] (mirror of `main::emit_bootstrap_typed`). On error, fall back to the legacy emitter so the push never silently drops. |
    ///
    /// Together with the bootstrap GET path (PR #333) this finishes
    /// the OpAMP-on-connect side of the typed emit so reconnecting
    /// agents receive the same routed YAML as fresh-connect agents.
    pub async fn push_config_to_agent(&self, agent_id: &str) -> bool {
        // B2: an agent may serve multiple `(metric, role)` pairs. Push
        // the config for the FIRST pair on connect — same shape as the
        // pre-B2 single-mapping path. (The 5-sketch routing-connector
        // edge YAML, once `metric_to_family` is populated by
        // `collect_metric_to_family`, carries pipelines for every
        // metric+role anyway, so a single push covers all of them.)
        let pair = self
            .agent_to_metrics
            .read()
            .await
            .get(agent_id)
            .and_then(|v| v.first().cloned());
        let Some((metric, role)) = pair else {
            return false;
        };

        let Ok(plan) = self.plan_store.get(&metric, role) else {
            return false;
        };

        let yaml = if stage_split::typed_stage_split_enabled() {
            match self.try_emit_typed_edge_yaml(&metric, role, agent_id) {
                Some(y) => {
                    info!(
                        agent = agent_id, metric = %metric, role = %role, bytes = y.len(),
                        "[USE_TYPED_STAGE_SPLIT] pushed typed edge YAML on connect"
                    );
                    y
                }
                None => {
                    warn!(
                        agent = agent_id, metric = %metric, role = %role,
                        "[USE_TYPED_STAGE_SPLIT] typed emit failed on connect; \
                         falling back to legacy generate_agent_collector_config"
                    );
                    match generate_agent_collector_config(&plan.agent_config, &self.opamp_endpoint)
                    {
                        Ok(y) => y,
                        Err(_) => {
                            warn!(agent = agent_id, metric = %metric, role = %role, "failed to generate agent config on connect");
                            return false;
                        }
                    }
                }
            }
        } else {
            match generate_agent_collector_config(&plan.agent_config, &self.opamp_endpoint) {
                Ok(y) => y,
                Err(_) => {
                    warn!(agent = agent_id, metric = %metric, role = %role, "failed to generate agent config on connect");
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
        info!(agent = agent_id, metric = %metric, role = %role, "pushed config to reconnecting agent");
        true
    }

    // ── Re-plan helpers ───────────────────────────────────────────────────────

    /// Re-plans every role registered for `metric` and pushes updated
    /// configs. Returns `true` if at least one role was re-planned,
    /// `false` if the metric has no roles registered at all.
    ///
    /// **B2 wrapper**: a single metric may carry multiple `(metric, role)`
    /// pairs; this function loops over them and delegates per-role to
    /// [`Self::replan_metric_role`]. Callers that only want to re-plan
    /// a single role should call `replan_metric_role` directly.
    pub async fn replan_metric(&self, metric: &str) -> bool {
        let pairs = self.workload_store.get_all_for_metric(metric);
        if pairs.is_empty() {
            warn!(metric, "replan requested but workload not found in store");
            return false;
        }
        let mut any = false;
        for (role, _, _) in pairs {
            if self.replan_metric_role(metric, role).await {
                any = true;
            }
        }
        any
    }

    /// Re-plans a single `(metric, role)` pair and pushes updated configs.
    /// Returns `true` on success, `false` if the pair is unknown.
    pub async fn replan_metric_role(&self, metric: &str, role: AggRole) -> bool {
        let Some((workload, wc)) = self.workload_store.get(metric, role) else {
            warn!(metric, role = %role, "replan requested but workload not found in store");
            return false;
        };

        info!(metric, role = %role, "re-planning metric+role");

        // Reset the baseline so the cost model runs fresh rather than returning the
        // previously established baseline — the whole point of a re-plan is to
        // re-optimise with current EMA data.
        self.planner.reset(metric);
        let mut plan = self.planner.plan(&workload, Some(&wc));
        plan.precompute = build_precompute_engine_jobs(&workload, "data-plane:4317");
        self.plan_store.set(metric, role, plan.clone());

        // Push agent config only to agents registered for this specific
        // `(metric, role)` pair, rather than broadcasting to all
        // agent-role collectors. Same gate as `push_config_to_agent`
        // — typed path on, legacy fallback on emit failure or when the
        // gate is off.
        //
        // Issue #2: emit per-agent inside the push loop so each agent's
        // opamp `X-Agent-ID` header carries its actual id (the agent
        // re-presents this header after the controller-pushed config
        // triggers a Docker restart).
        let key = (metric.to_string(), role);
        let agents = self.agent_to_metrics.read().await;
        let target_agents: Vec<String> = agents
            .iter()
            .filter(|(_, pairs)| pairs.contains(&key))
            .map(|(id, _)| id.clone())
            .collect();
        drop(agents);

        // Pre-emit the legacy fallback YAML once (it has no per-agent
        // identity to thread) so each agent that falls back gets the
        // same bytes.
        let legacy_fallback: Option<String> =
            generate_agent_collector_config(&plan.agent_config, &self.opamp_endpoint).ok();

        for agent_id in target_agents {
            let agent_yaml: Option<String> = if stage_split::typed_stage_split_enabled() {
                match self.try_emit_typed_edge_yaml_for_workload(&workload, &agent_id) {
                    Some(y) => {
                        info!(
                            metric,
                            agent = %agent_id,
                            bytes = y.len(),
                            "[USE_TYPED_STAGE_SPLIT] re-plan emitted typed edge YAML"
                        );
                        Some(y)
                    }
                    None => {
                        warn!(
                            metric,
                            agent = %agent_id,
                            "[USE_TYPED_STAGE_SPLIT] re-plan typed emit failed; \
                             falling back to legacy generate_agent_collector_config"
                        );
                        legacy_fallback.clone()
                    }
                }
            } else {
                legacy_fallback.clone()
            };
            if let Some(yaml) = agent_yaml {
                let cfg = RemoteConfig {
                    config_hash: short_hash(&yaml),
                    yaml,
                };
                self.opamp.push(&agent_id, cfg).await;
            }
        }
        // Option B: post the typed cumulative `StreamingConfig` +
        // `BackendStorageRouting` JSON to the backend through the
        // SAME helper `main::handle_plan` uses. The shared
        // `backend_routing_cache` (when wired via
        // `with_backend_routing_cache`) is updated under the helper's
        // lock and the cumulative POST surfaces every `(metric, role)`
        // pair the controller has planned — so the data plane's
        // atomic `handle.swap(new_config)` never wipes sibling
        // aggregations the way the retired
        // `generate_streaming_config_yaml` single-aggregation YAML
        // path did.
        //
        // The cache is shared with `AppState`; if the Replanner was
        // built without one (test fixture), we fall back to a
        // throwaway local cache so the helper still emits — the
        // cumulative semantics degrade gracefully (the Replanner is
        // the sole writer in that scenario).
        if stage_split::typed_stage_split_enabled() {
            if let Some(be) = self.build_backend_stage_config(&workload, role) {
                let fallback_cache = self.backend_routing_cache.clone();
                let cache_arc =
                    fallback_cache.unwrap_or_else(|| Arc::new(Mutex::new(HashMap::new())));
                // CDM monitor specs declared in the workload registry (global;
                // coordinator_url is an edge concern, so pass "" for the
                // backend's agg_id/τ/window-only entries).
                let monitors = self
                    .workload_registry
                    .as_ref()
                    .map(|r| r.monitor_intents(""))
                    .unwrap_or_default();
                post_typed_backend_for_role(
                    self.backend_client.as_ref(),
                    cache_arc.as_ref(),
                    metric,
                    role,
                    be,
                    &monitors,
                )
                .await;
            } else {
                warn!(
                    metric,
                    role = %role,
                    "could not build BackendStageConfig for replan — \
                     skipping backend HTTP push for this cycle"
                );
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

    /// Run the same planner → stage-split → Backend extraction
    /// `handle_plan` runs, then return the patched
    /// `BackendStageConfig` ready for [`post_typed_backend_for_role`].
    ///
    /// Mirrors the L4/L5 flow in `main::handle_plan` for specs that
    /// supply only explicit fields (no `query_string`): runs
    /// `bind_workload_typed` to lower the workload to a
    /// `PhysicalExpr`, then `split_typed_three_stage` to extract the
    /// per-stage configs, finds the `Backend` arm, and patches
    /// `metric_name` / `window_secs` / `grouping` on each
    /// `BackendAggregation` from the workload spec (same patch the
    /// `handle_plan` Backend arm applies).
    ///
    /// **ExactAgg fallback (Option B)**: when `bind_workload_typed`
    /// declines (Sum/Rate/Count workloads — `sum by (zone)
    /// (http_requests_total)`, `rate(metric[5m])`,
    /// `count(metric)`) AND the role classifies as
    /// Sum/Count/Other/Topk, synthesize a single ExactAgg-shaped
    /// `BackendStageConfig` carrying an `agg_type_override` of
    /// `"Sum"` / `"Increase"` / `"MinMax"` so the cumulative
    /// streaming-config still surfaces the metric to the backend.
    /// Without this fallback the typed cumulative POST would omit
    /// every Sum-shaped metric and `sum by (zone) (…)` queries would
    /// return `No result for query`.
    fn build_backend_stage_config(
        &self,
        workload: &QueryWorkload,
        role: AggRole,
    ) -> Option<BackendStageConfig> {
        // ── Typed sketch path (Quantile / Cardinality / TopK / Frequency) ──
        if let Some(physical_expr) = rules::bind_workload_typed(workload) {
            if let Some(configs) = stage_split::split_typed_three_stage(&physical_expr) {
                if let Some(mut be) = configs.into_iter().find_map(|(_, cfg)| match cfg {
                    crate::physical::colored_dag::StageConfig::Backend(be) => Some(be),
                    _ => None,
                }) {
                    // Same patch the `handle_plan` Backend arm applies: the L5
                    // emitter leaves `metric_name` empty when path-recovery
                    // through `extract_edge_facts` fails, and always leaves
                    // `grouping` empty (`QueryExpr::Aggregate.by` is positional
                    // `ColumnId`s with no label-name resolution today). The
                    // `QueryWorkload` carries both unambiguously, and every
                    // aggregation under one workload shares them.
                    // Per-metric item_label (the high-card dimension a CMS/CountSketch
                    // hashes): threaded into the policy params so the data-plane ingest
                    // records it on the sid and can answer per-item estimate(key).
                    let item_labels = self
                        .workload_registry
                        .as_ref()
                        .map(|reg| {
                            crate::emit::collect_metric_to_item_label(reg, &self.workload_store)
                        })
                        .unwrap_or_default();
                    for agg in &mut be.aggregations {
                        if agg.metric_name.is_empty() {
                            agg.metric_name = workload.metric_name.clone();
                        }
                        if agg.window_secs == 0 {
                            agg.window_secs = workload.time_window.as_secs();
                        }
                        agg.grouping = workload.group_by_labels.clone();
                        agg.item_label = item_labels.get(&agg.metric_name).cloned();
                    }
                    return Some(be);
                }
            }
        }

        // ── ExactAgg fallback (Sum / Count / Increase) ────────────────────
        //
        // The typed binder declined — most likely because the workload is
        // Sum/Rate/Count-shaped (raw passthrough, no sketch family). Emit a
        // single-aggregation `BackendStageConfig` with an
        // `agg_type_override` so the data plane gets an ExactAgg entry it
        // can dispatch to its `SumAccumulator` / `IncreaseAccumulator`.
        let agg_type_override = match role {
            AggRole::Sum => Some("Sum"),
            AggRole::Count => Some("Sum"), // count(metric) maps to a Sum-as-count accumulator on the backend
            AggRole::Other => None,
            AggRole::Quantile | AggRole::Topk => None,
        };
        let agg_type_override = agg_type_override?.to_string();
        use crate::physical::colored_dag::emitter::{
            AggregationInput, BackendAggregation, BackendStageConfig,
        };
        use planner_types::post_asap::{SketchAlgorithm, SketchParams};
        let window_secs = workload.time_window.as_secs().max(1);
        Some(BackendStageConfig {
            aggregations: vec![BackendAggregation {
                item_label: None,
                aggregation_id: format!("exact-{}-{}", workload.metric_name, role),
                metric_name: workload.metric_name.clone(),
                // Sentinel sketch_kind / sketch_params — `agg_type_override`
                // takes precedence in `build_backend_aggregation_json`, so
                // these are not emitted on the wire. DDSketch is the
                // chosen sentinel because every backend that recognises
                // `AggregationType::FromStr` also accepts DDSketch (and
                // we don't have a `SketchAlgorithm::None` variant today).
                sketch_kind: SketchAlgorithm::DDSketch.into(),
                sketch_params: SketchParams::DDSketch { alpha: 0.01 }.into(),
                window_secs,
                spatial_filter: String::new(),
                grouping: workload.group_by_labels.clone(),
                // ExactAgg consumes raw values at the backend (the agent
                // ships counter samples; the backend's
                // SumAccumulator integrates them).
                aggregation_input: AggregationInput::Raw,
                agg_type_override: Some(agg_type_override),
            }],
            // No readout entries — ExactAgg produces the answer
            // directly; the readout dispatch happens at PromQL eval
            // time on the backend.
            readouts: Vec::new(),
        })
    }

    /// Loop through every `(metric, role)` pair in the `WorkloadStore`
    /// and call [`Self::replan_metric_role`]. Used at startup (after
    /// the workload-registry pre-pop) and on OpAMP first-connect so
    /// the backend's cumulative `StreamingConfig` carries every
    /// planned `(metric, role)` BEFORE the first query lands —
    /// without this, queries that don't trigger `POST /api/v1/plan`
    /// hit the data plane's static startup config (DDSketch only) and
    /// fail.
    ///
    /// Idempotent: every call replays the cumulative POST. Re-runs
    /// over the same set of pairs are a no-op on the backend (same
    /// shape ⇒ same `handle.swap` payload).
    pub async fn replan_all(&self) {
        let keys = self.workload_store.keys();
        if keys.is_empty() {
            info!("replan_all: workload store empty — nothing to plan");
            return;
        }
        info!(
            count = keys.len(),
            "replan_all: planning every (metric, role) pair"
        );
        for (metric, role) in keys {
            self.replan_metric_role(&metric, role).await;
        }
    }

    /// Re-plans every `(metric, role)` pair whose `valid_until` has
    /// already passed.
    pub async fn replan_expired(&self) {
        let expired = self.plan_store.expired(chrono::Utc::now());
        if expired.is_empty() {
            return;
        }
        info!(
            count = expired.len(),
            "re-planning expired (metric, role) pairs"
        );
        for (metric, role) in expired {
            self.replan_metric_role(&metric, role).await;
        }
    }

    /// Called from the violation callback. Looks up the `(metric, role)`
    /// pairs served by `agent_id` and triggers an immediate re-plan of
    /// each one.
    pub async fn handle_violation(&self, agent_id: &str) {
        let pairs = self
            .agent_to_metrics
            .read()
            .await
            .get(agent_id)
            .cloned()
            .unwrap_or_default();
        if pairs.is_empty() {
            warn!(
                agent = agent_id,
                "SLA violation but no (metric, role) mapping found; re-planning all expired"
            );
            self.replan_expired().await;
            return;
        }
        for (metric, role) in pairs {
            info!(agent = agent_id, metric = %metric, role = %role, "SLA violation → triggering re-plan");
            self.replan_metric_role(&metric, role).await;
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

    /// P0-1: re-POST the FULL cumulative streaming-config + storage-routing
    /// to the backend from the current shared cache, WITHOUT re-planning.
    ///
    /// The data_plane backend is a plain HTTP service receiving POSTs — NOT
    /// an OpAMP agent — so a backend restart triggers none of the
    /// controller's re-push paths (startup `replan_all`, OpAMP on-connect).
    /// After a restart the backend's in-memory streaming-config is gone, and
    /// the expiry ticker only re-POSTs `(metric, role)` pairs whose plan
    /// `valid_until` elapsed; until then a query needing a non-default
    /// aggregation (Sum / ExactAgg) capability-misses to archive.
    ///
    /// This method re-POSTs everything idempotently (the data plane installs
    /// the cumulative config via an idempotent `handle.swap`, so re-POSTing
    /// the same shape is a no-op on a backend that already has it, and a full
    /// recovery on one that lost it). It reads the SAME shared
    /// `backend_routing_cache` `handle_plan` / `replan_metric_role` write to,
    /// so it always reflects the controller's latest cumulative state.
    ///
    /// Returns the [`PushOutcome`] so callers/tests can assert a refresh
    /// actually fired. `Skipped` when no backend client or routing cache is
    /// wired, or the cache is empty (nothing planned yet).
    pub async fn repost_cumulative_backend_config(&self) -> PushOutcome {
        let Some(cache) = self.backend_routing_cache.as_ref() else {
            // No shared cache → the Replanner has no cumulative state to
            // refresh from (this is a test fixture or a deployment that never
            // wired the cache). Nothing to do.
            return PushOutcome::Skipped;
        };
        let monitors = self
            .workload_registry
            .as_ref()
            .map(|r| r.monitor_intents(""))
            .unwrap_or_default();
        repost_cumulative_backend_config(self.backend_client.as_ref(), cache.as_ref(), &monitors)
            .await
    }

    /// P0-1: background loop that periodically re-POSTs the full cumulative
    /// backend config so a silent data_plane restart can't leave the
    /// streaming-config missing until a plan expires.
    ///
    /// Runs on a BOUNDED, low-frequency cadence (`interval`) independent of
    /// the expiry ticker so the refresh isn't chatty — each tick is one
    /// coupled streaming-config + storage-routing POST, and the data plane
    /// no-ops when its config already matches. A no-op-on-match backend means
    /// the only cost on the steady-state path is one pair of idempotent
    /// HTTP POSTs per `interval`.
    pub async fn run_backend_repost_ticker(self: Arc<Self>, interval: Duration) {
        // A zero/sub-second interval would busy-loop; clamp to a sane floor.
        let interval = interval.max(Duration::from_secs(1));
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The first `interval.tick()` fires immediately; skip that initial
        // tick so we don't double up with the startup `replan_all()` POST
        // that already primed the backend before the HTTP server bound.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let outcome = self.repost_cumulative_backend_config().await;
            match outcome {
                PushOutcome::AllApplied => info!(
                    "periodic backend re-POST applied cumulative streaming-config + storage-routing"
                ),
                PushOutcome::Skipped => { /* no backend / empty cache — nothing logged each tick */
                }
                PushOutcome::EmitFailed => {
                    warn!("periodic backend re-POST: failed to serialise cumulative config")
                }
                PushOutcome::Desynced {
                    streaming_ok,
                    routing_ok,
                    plan_ok,
                } => warn!(
                    streaming_ok,
                    routing_ok,
                    plan_ok,
                    "periodic backend re-POST desynced after retries; will retry next tick"
                ),
            }
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

    use crate::physical::deployment_cost::DeploymentCostPlanner;
    use crate::physical::plan_cache::CachedDeploymentPlanner;
    use crate::store::{PlanStore, WorkloadStore};
    use crate::types::*;

    fn make_replanner() -> Arc<Replanner> {
        let plan_store = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let planner = Arc::new(CachedDeploymentPlanner::new(DeploymentCostPlanner::new()));
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
                gos: None,
                enable_series_id: false,
                series_id_ttl_secs: 300,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
            valid_until: Utc::now() + chrono::Duration::seconds(3600),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
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
        r.workload_store.set("latency", AggRole::Quantile, wl, wc);
        r.plan_store.set("latency", AggRole::Quantile, make_plan());

        let ok = r.replan_metric("latency").await;
        assert!(ok);
        // Plan store should now have a new entry (valid_until in the future).
        let updated = r.plan_store.get("latency", AggRole::Quantile).unwrap();
        assert!(updated.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn replan_expired_replans_only_expired() {
        let r = make_replanner();
        let (wl, wc) = test_workload("old");
        r.workload_store.set("old", AggRole::Quantile, wl, wc);

        // Insert an already-expired plan.
        let mut expired_plan = make_plan();
        expired_plan.valid_until = Utc::now() - chrono::Duration::seconds(60);
        r.plan_store.set("old", AggRole::Quantile, expired_plan);

        // Insert a still-active plan for "active".
        let (awl, awc) = test_workload("active");
        r.workload_store.set("active", AggRole::Quantile, awl, awc);
        r.plan_store.set("active", AggRole::Quantile, make_plan());

        r.replan_expired().await;

        // "old" should now have a freshly computed plan.
        let old_plan = r.plan_store.get("old", AggRole::Quantile).unwrap();
        assert!(old_plan.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn register_then_violation_replans_correct_metric() {
        let r = make_replanner();
        let (wl, wc) = test_workload("req_rate");
        r.workload_store.set("req_rate", AggRole::Quantile, wl, wc);
        r.plan_store.set("req_rate", AggRole::Quantile, make_plan());

        r.register_agent("agent-1", "req_rate", AggRole::Quantile)
            .await;
        r.handle_violation("agent-1").await;

        // Plan should have been refreshed.
        assert!(r.plan_store.get("req_rate", AggRole::Quantile).is_ok());
    }

    #[tokio::test]
    async fn unregister_removes_mapping() {
        let r = make_replanner();
        r.register_agent("a1", "m", AggRole::Quantile).await;
        r.unregister_agent("a1").await;
        // After unregister, handle_violation falls back to replan_expired (no-op).
        r.handle_violation("a1").await; // should not panic
    }

    // ── B2 multi-role regression tests ────────────────────────────────────────

    /// A metric with two different `(metric, role)` registrations
    /// keeps both plans live after replan. Pre-B2 the store collapsed
    /// them onto one key and the second replan would overwrite the
    /// first; this regression test pins the new contract.
    #[tokio::test]
    async fn replan_multi_role_metric_updates_both_plans() {
        let r = make_replanner();
        let (wl_q, wc_q) = test_workload("http_requests_total");
        let mut wl_s = wl_q.clone();
        wl_s.aggregations = vec![AggType::Quantile]; // analyzer-shaped (test fixture)
        let wc_s = wc_q.clone();

        r.workload_store
            .set("http_requests_total", AggRole::Quantile, wl_q, wc_q);
        r.workload_store
            .set("http_requests_total", AggRole::Sum, wl_s, wc_s);
        r.plan_store
            .set("http_requests_total", AggRole::Quantile, make_plan());
        r.plan_store
            .set("http_requests_total", AggRole::Sum, make_plan());

        // `replan_metric` is the wrapper that loops over every role
        // registered for the metric.
        let ok = r.replan_metric("http_requests_total").await;
        assert!(ok, "wrapper replan_metric should succeed for ≥1 role");

        // Both roles' plans persist independently.
        assert!(r
            .plan_store
            .get("http_requests_total", AggRole::Quantile)
            .is_ok());
        assert!(r
            .plan_store
            .get("http_requests_total", AggRole::Sum)
            .is_ok());
    }

    /// Targeted single-role replan via [`Replanner::replan_metric_role`]
    /// only touches the specified role's plan and leaves the other
    /// role's plan unchanged.
    #[tokio::test]
    async fn replan_metric_role_only_touches_target_role() {
        let r = make_replanner();
        let (wl, wc) = test_workload("m");
        r.workload_store
            .set("m", AggRole::Quantile, wl.clone(), wc.clone());
        r.workload_store.set("m", AggRole::Sum, wl, wc);

        // Make the Sum-role plan expired and Quantile plan fresh.
        let mut sum_plan = make_plan();
        sum_plan.valid_until = Utc::now() - chrono::Duration::seconds(60);
        r.plan_store.set("m", AggRole::Sum, sum_plan);
        let fresh = make_plan();
        let fresh_ts = fresh.valid_until;
        r.plan_store.set("m", AggRole::Quantile, fresh);

        let ok = r.replan_metric_role("m", AggRole::Sum).await;
        assert!(ok);

        // The Quantile plan stays untouched (same valid_until as
        // before the replan).
        let q = r.plan_store.get("m", AggRole::Quantile).unwrap();
        assert_eq!(q.valid_until, fresh_ts);
        // Sum has been re-planned (new valid_until in the future).
        let s = r.plan_store.get("m", AggRole::Sum).unwrap();
        assert!(s.valid_until > Utc::now());
    }

    /// One agent serving multiple `(metric, role)` pairs receives a
    /// re-plan for every one of them on violation.
    #[tokio::test]
    async fn agent_serving_multiple_roles_triggers_per_role_replan() {
        let r = make_replanner();
        let (wl, wc) = test_workload("m");
        r.workload_store
            .set("m", AggRole::Quantile, wl.clone(), wc.clone());
        r.workload_store.set("m", AggRole::Sum, wl, wc);
        r.plan_store.set("m", AggRole::Quantile, make_plan());
        r.plan_store.set("m", AggRole::Sum, make_plan());

        // One agent serves both roles.
        r.register_agent("agent-1", "m", AggRole::Quantile).await;
        r.register_agent("agent-1", "m", AggRole::Sum).await;

        // Sanity: agent_to_metrics() carries both pairs in order.
        let pairs = r
            .agent_to_metrics()
            .read()
            .await
            .get("agent-1")
            .cloned()
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                ("m".to_string(), AggRole::Quantile),
                ("m".to_string(), AggRole::Sum)
            ]
        );

        // Handle violation — both roles should re-plan without panic.
        r.handle_violation("agent-1").await;
    }

    // ── Typed-emit path tests ─────────────────────────────────────────────────
    //
    // These tests exercise the `USE_TYPED_STAGE_SPLIT`-gated emit path
    // ported from `main::emit_bootstrap_typed` so the OpAMP-pushed YAML
    // matches what the bootstrap GET path returns. The acceptance bar
    // is criterion ⑥ on issue #46: the agent's edge pipeline must
    // include `gorillas3` (archive write), `routing` (warm-passthrough
    // dispatch), and the `metrics/warm_passthrough` pipeline.

    use crate::test_support::EnvVarGuard;

    /// Given an agent runtime + a quantile workload registered in the
    /// workload store, the typed emit path produces YAML containing
    /// `gorillas3`, `routing`, and `metrics/warm_passthrough`.
    ///
    /// Proves freshness-probe routing reaches OpAMP-pushed agents —
    /// the legacy `generate_agent_collector_config` path emits NONE of these
    /// (it builds a single-pipeline DDSketch YAML with no routing).
    #[tokio::test]
    async fn typed_replan_emit_includes_freshness_probe_routing() {
        let _env = EnvVarGuard::set("USE_TYPED_STAGE_SPLIT", "1");

        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", AggRole::Quantile, wl, wc);
        r.plan_store.set("latency", AggRole::Quantile, make_plan());

        let yaml = r
            .try_emit_typed_edge_yaml("latency", AggRole::Quantile, "test-agent")
            .expect("typed emit should succeed for a quantile workload");

        // gorillas3 — archive-tier write to MinIO. Without this the
        // ASAP-tier query engine has nothing to read for criterion ⑥.
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
        // Hold the crate-wide env lock so a parallel typed test can't
        // flip the var underneath us; the guard unsets it and restores
        // the prior value on drop.
        let _env = EnvVarGuard::unset("USE_TYPED_STAGE_SPLIT");

        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", AggRole::Quantile, wl, wc);
        r.plan_store.set("latency", AggRole::Quantile, make_plan());

        // Drive the legacy emitter directly — same code
        // `push_config_to_agent` runs when the gate is off.
        let plan = r.plan_store.get("latency", AggRole::Quantile).unwrap();
        let yaml = generate_agent_collector_config(&plan.agent_config, &r.opamp_endpoint)
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

        // `_env` restores the prior `USE_TYPED_STAGE_SPLIT` value on drop.
    }

    // ── P0-1: backend-restart re-POST ─────────────────────────────────────────

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc as StdArc;

    /// Start a mock backend serving the complete publication contract,
    /// returning the streaming-config URL and a shared hit-counter for that
    /// endpoint.
    async fn start_repost_mock() -> (String, StdArc<AtomicU32>) {
        use axum::extract::State;
        use axum::routing::post;
        use axum::Router;
        let hits = StdArc::new(AtomicU32::new(0));
        let app = Router::new()
            .route(
                "/api/v1/streaming-config",
                post(
                    |State(h): State<StdArc<AtomicU32>>, _b: axum::body::Bytes| async move {
                        h.fetch_add(1, Ordering::SeqCst);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .route(
                "/api/v1/storage_routing",
                post(|_b: axum::body::Bytes| async move { axum::http::StatusCode::OK }),
            )
            .route(
                "/api/v1/backend-plan",
                post(|_b: axum::body::Bytes| async move { axum::http::StatusCode::OK }),
            )
            .with_state(StdArc::clone(&hits));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        (format!("http://{addr}/api/v1/streaming-config"), hits)
    }

    /// A wired Replanner (backend client + shared routing cache) re-POSTs
    /// the FULL cumulative backend config from the cache when
    /// `repost_cumulative_backend_config` fires — the periodic refresh the
    /// background ticker drives. This is the P0-1 recovery path: a silent
    /// backend restart fires no replan, but the periodic re-POST re-sends
    /// the cumulative config so a Sum/ExactAgg query stops capability-missing
    /// to archive.
    #[tokio::test]
    async fn wired_replanner_reposts_cumulative_config_from_cache() {
        use crate::backend_client::BackendClient;
        use crate::physical::colored_dag::emitter::{
            AggregationInput, BackendAggregation, BackendStageConfig,
        };
        use planner_types::post_asap::{SketchAlgorithm, SketchParams};

        let (url, hits) = start_repost_mock().await;
        let client = StdArc::new(BackendClient::new(url));
        let cache: StdArc<Mutex<HashMap<(String, AggRole), BackendStageConfig>>> =
            StdArc::new(Mutex::new(HashMap::new()));

        // Build a Replanner wired to the mock backend + the shared cache.
        let plan_store = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let planner = Arc::new(CachedDeploymentPlanner::new(DeploymentCostPlanner::new()));
        let opamp = Arc::new(crate::opamp::OpampServer::new());
        let scraper = Arc::new(crate::monitor::Scraper::new(
            vec![],
            crate::monitor::Thresholds::default(),
            Arc::new(|_| {}),
            Duration::from_secs(60),
        ));
        let r = Arc::new(
            Replanner::new(
                planner,
                plan_store,
                workload_store,
                opamp,
                scraper,
                "ws://c/",
            )
            .with_backend_client(StdArc::clone(&client))
            .with_backend_routing_cache(StdArc::clone(&cache)),
        );

        // Empty cache → re-POST is a no-op (nothing planned yet), backend
        // untouched.
        assert_eq!(
            r.repost_cumulative_backend_config().await,
            PushOutcome::Skipped
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        // Seed the SHARED cache as if a prior plan emit had populated it
        // (a Sum/ExactAgg aggregation the static startup config lacks).
        {
            let mut c = cache.lock().await;
            c.insert(
                ("http_requests_total".to_string(), AggRole::Sum),
                BackendStageConfig {
                    aggregations: vec![BackendAggregation {
                        item_label: None,
                        aggregation_id: "exact-http_requests_total-sum".to_string(),
                        metric_name: "http_requests_total".to_string(),
                        sketch_kind: SketchAlgorithm::DDSketch.into(),
                        sketch_params: SketchParams::DDSketch { alpha: 0.01 }.into(),
                        grouping: vec!["zone".to_string()],
                        spatial_filter: String::new(),
                        window_secs: 60,
                        aggregation_input: AggregationInput::Raw,
                        agg_type_override: Some("Sum".to_string()),
                    }],
                    readouts: Vec::new(),
                },
            );
        }

        // Simulated backend restart: the periodic ticker fires and re-POSTs
        // the full cumulative config WITHOUT any replan. The (now-restarted)
        // backend receives the streaming-config again.
        let outcome = r.repost_cumulative_backend_config().await;
        assert_eq!(outcome, PushOutcome::AllApplied);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "periodic re-POST must re-send the cumulative streaming-config to the backend"
        );

        // Idempotent: a second tick re-POSTs again (the data plane no-ops on
        // a matching config; the controller still re-sends each cycle).
        let outcome2 = r.repost_cumulative_backend_config().await;
        assert_eq!(outcome2, PushOutcome::AllApplied);
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    /// A Replanner with NO shared cache (the test/default fixture) treats
    /// the re-POST as a no-op `Skipped` — it has no cumulative state to
    /// refresh from.
    #[tokio::test]
    async fn unwired_replanner_repost_is_skipped() {
        let r = make_replanner();
        assert_eq!(
            r.repost_cumulative_backend_config().await,
            PushOutcome::Skipped
        );
    }
}
