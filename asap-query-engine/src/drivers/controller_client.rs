//! DataCollector controller client.
//!
//! Fetches query configuration from the DataCollector controller's API
//! instead of reading a static `inference_config.yaml` file.
//!
//! This replaces the pattern-matching approach: the controller already
//! knows which queries are precomputed and which sketches to use.

use serde::Deserialize;
use tracing::{info, warn};

/// Configuration for the controller client.
#[derive(Debug, Clone)]
pub struct ControllerClientConfig {
    /// Base URL of the DataCollector controller (e.g., "http://controller:8080").
    pub base_url: String,
    /// How often to poll for config updates (seconds). 0 = poll only on query miss.
    pub poll_interval_secs: u64,
}

impl Default for ControllerClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8080".to_string(),
            poll_interval_secs: 0,
        }
    }
}

/// A plan fetched from the controller for a specific metric.
#[derive(Debug, Clone, Deserialize)]
pub struct ControllerPlan {
    pub metric: String,
    pub sketch_type: String,
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

/// Client for the DataCollector controller API.
pub struct ControllerClient {
    config: ControllerClientConfig,
    http: reqwest::Client,
}

impl ControllerClient {
    pub fn new(config: ControllerClientConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    /// Fetch the plan for a specific metric from the controller.
    ///
    /// Returns `None` if the controller has no plan for this metric.
    pub async fn get_plan(&self, metric: &str) -> Option<ControllerPlan> {
        let url = format!("{}/api/v1/plan/{}", self.config.base_url, metric);

        match self.http.get(&url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.json::<ControllerPlan>().await {
                        Ok(plan) => {
                            info!(metric = %metric, sketch_type = %plan.sketch_type, "Fetched plan from controller");
                            Some(plan)
                        }
                        Err(e) => {
                            warn!(metric = %metric, error = %e, "Failed to parse controller plan");
                            None
                        }
                    }
                } else {
                    None // 404 = no plan exists
                }
            }
            Err(e) => {
                warn!(url = %url, error = %e, "Controller unreachable");
                None
            }
        }
    }

    /// Check if the controller is healthy.
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
