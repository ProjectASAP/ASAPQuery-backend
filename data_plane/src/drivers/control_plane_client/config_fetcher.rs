//! Control-plane client.
//!
//! Fetches query configuration from the control plane's API
//! instead of reading a static `inference_config.yaml` file.
//!
//! This replaces the pattern-matching approach: the control plane already
//! knows which queries are precomputed and which sketches to use.

use serde::Deserialize;
use tracing::{info, warn};

/// Configuration for the control-plane client.
#[derive(Debug, Clone)]
pub struct ControlPlaneClientConfig {
    /// Base URL of the control plane (e.g., "http://control-plane:8080").
    pub base_url: String,
    /// How often to poll for config updates (seconds). 0 = poll only on query miss.
    pub poll_interval_secs: u64,
}

impl Default for ControlPlaneClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8080".to_string(),
            poll_interval_secs: 0,
        }
    }
}

/// Slim plan status returned by `GET /api/v1/plan/:metric`.
///
/// The GET endpoint only returns the metric name, sketch type, and
/// validity. Callers that need the richer plan (mode, aggregate_by,
/// staged_plan, etc.) must use [`ControlPlaneClient::create_plan`]
/// which invokes `POST /api/v1/plan`.
#[derive(Debug, Clone, Deserialize)]
pub struct ControlPlanePlanStatus {
    pub metric: String,
    pub sketch_type: String,
    #[serde(default)]
    pub valid_until: Option<String>,
}

/// Rich plan returned by `POST /api/v1/plan`.
#[derive(Debug, Clone, Deserialize)]
pub struct ControlPlanePlan {
    pub metric: String,
    pub sketch_type: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub aggregate_by: Vec<String>,
    #[serde(default)]
    pub precompute_jobs: usize,
    #[serde(default)]
    pub delta_decision: serde_json::Value,
    #[serde(default)]
    pub staged_plan: Option<serde_json::Value>,
}

/// Client for the control plane's API.
pub struct ControlPlaneClient {
    config: ControlPlaneClientConfig,
    http: reqwest::Client,
}

impl ControlPlaneClient {
    pub fn new(config: ControlPlaneClientConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Look up the current plan status for a specific metric.
    ///
    /// Invokes `GET /api/v1/plan/:metric`. Returns `None` if the control
    /// plane has no plan for this metric (404) or is unreachable. The
    /// response is a slim `{ metric, sketch_type, valid_until }` shape —
    /// for the full plan, use [`Self::create_plan`].
    pub async fn get_plan(&self, metric: &str) -> Option<ControlPlanePlanStatus> {
        let url = format!("{}/api/v1/plan/{}", self.config.base_url, metric);

        match self.http.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<ControlPlanePlanStatus>().await {
                        Ok(plan) => {
                            info!(metric = %metric, sketch_type = %plan.sketch_type, "Fetched plan status from control plane");
                            Some(plan)
                        }
                        Err(e) => {
                            warn!(metric = %metric, error = %e, "Failed to parse control-plane plan status");
                            None
                        }
                    }
                } else {
                    None // 404 = no plan exists
                }
            }
            Err(e) => {
                warn!(url = %url, error = %e, "Control plane unreachable");
                None
            }
        }
    }

    /// Create or refresh a plan by posting a `QuerySpec` to the control plane.
    ///
    /// Invokes `POST /api/v1/plan`. The `query_spec` is passed through as the
    /// JSON body; refer to the control plane's `QuerySpec` type for the exact
    /// schema. Returns the rich plan (mode, aggregate_by, staged plan, delta
    /// decision, etc.) on success.
    pub async fn create_plan(&self, query_spec: serde_json::Value) -> Option<ControlPlanePlan> {
        let url = format!("{}/api/v1/plan", self.config.base_url);

        match self.http.post(&url).json(&query_spec).send().await {
            Ok(resp) if resp.status().is_success() => match resp.json::<ControlPlanePlan>().await {
                Ok(plan) => {
                    info!(metric = %plan.metric, sketch_type = %plan.sketch_type, "Created plan via control plane");
                    Some(plan)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to parse control-plane plan response");
                    None
                }
            },
            Ok(resp) => {
                warn!(status = %resp.status(), "Control plane rejected plan request");
                None
            }
            Err(e) => {
                warn!(url = %url, error = %e, "Control plane unreachable");
                None
            }
        }
    }

    /// Check if the control plane is healthy by listing connected agents.
    pub async fn health_check(&self) -> bool {
        let url = format!("{}/api/v1/agents", self.config.base_url);
        match self.http.get(&url).send().await {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    /// Fetch the generated collector config YAML for a metric.
    pub async fn get_config_yaml(&self, metric: &str) -> Option<String> {
        let url = format!("{}/api/v1/config/{}", self.config.base_url, metric);
        match self.http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => resp.text().await.ok(),
            _ => None,
        }
    }
}
