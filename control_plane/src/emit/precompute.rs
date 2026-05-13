use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::pipeline::format_duration;
use crate::types::*;

// ── Scheduling rule ───────────────────────────────────────────────────────────

/// Returns true when a query should be precomputed.
///
/// Rule (design doc): precompute when `repeat_every < latency_sla`, meaning
/// the query fires more often than the system can recompute it on demand.
pub fn should_precompute(w: &QueryWorkload) -> bool {
    match (w.repeat_every, w.latency_sla) {
        (Some(re), Some(ls)) => re < ls,
        _ => false,
    }
}

/// Builds the list of precompute jobs for a plan. Returns an empty Vec if the
/// workload does not meet the precompute eligibility criterion.
///
/// **SP-9**: when `plan.staged_plan` is `Some` and the precompute sub-plan is
/// active, the job's `query_expr` is taken from the AST-derived PromQL
/// serialisation ([`PrecomputeSubPlan::query_expr`]) rather than the hardcoded
/// `quantile_over_time(0.99, …)` template.  This allows the precompute engine
/// to evaluate the actual upper sub-tree (e.g. `topk(10, count_over_time(…))`).
///
/// When no `staged_plan` is present (SP-3 flat path), the legacy
/// `build_query_expr()` template is used as the fallback.
pub fn build_precompute_jobs(
    w: &QueryWorkload,
    plan: &CollectionPlan,
    backend_addr: &str,
) -> Vec<PrecomputeJob> {
    if !should_precompute(w) {
        return vec![];
    }
    let granularity = w.repeat_every.unwrap(); // safe: should_precompute checked it

    // SP-9: use the staged plan's PromQL when available and non-empty.
    let query_expr = plan
        .staged_plan
        .as_ref()
        .filter(|sp| sp.precompute.active && !sp.precompute.query_expr.is_empty())
        .map(|sp| sp.precompute.query_expr.clone())
        .unwrap_or_else(|| build_query_expr(w));

    vec![PrecomputeJob {
        query_expr,
        granularity,
        sketch_source: backend_addr.to_string(),
        store_path: build_store_path(w),
    }]
}

fn build_query_expr(w: &QueryWorkload) -> String {
    let agg_fn = match w.aggregations.first() {
        Some(AggType::Cardinality) => "count_distinct_over_time",
        Some(AggType::Frequency) => "top_k_over_time",
        _ => "quantile_over_time",
    };
    let filters: Vec<String> = w
        .label_filters
        .iter()
        .map(|(k, v)| format!("{k}=\"{v}\""))
        .collect();
    let selector = if filters.is_empty() {
        w.metric_name.clone()
    } else {
        format!("{}{{{}}}", w.metric_name, filters.join(","))
    };
    format!(
        "{agg_fn}(0.99, {selector}[{}])",
        format_duration(w.time_window)
    )
}

fn build_store_path(w: &QueryWorkload) -> String {
    format!(
        "precomputed/{}/p99/{}",
        w.metric_name,
        format_duration(w.time_window)
    )
}

// ── HTTP client for ASAPQuery precompute API ──────────────────────────────────

#[derive(Debug, Serialize)]
struct JobRequest {
    query: String,
    granularity: String,
    source: String,
    sketch_type: String,
    store_path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobResponse {
    pub job_id: String,
    pub status: String,
    pub created_at: Option<DateTime<Utc>>,
}

pub struct PrecomputeClient {
    base_url: String,
    client: reqwest::Client,
}

impl PrecomputeClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Registers a precompute job with the ASAPQuery engine.
    pub async fn register(&self, job: &PrecomputeJob) -> anyhow::Result<JobResponse> {
        let req = JobRequest {
            query: job.query_expr.clone(),
            granularity: format_duration(job.granularity),
            source: job.sketch_source.clone(),
            sketch_type: "ddsketch".into(),
            store_path: job.store_path.clone(),
        };
        let resp = self
            .client
            .post(format!("{}/api/v1/precompute/jobs", self.base_url))
            .json(&req)
            .send()
            .await
            .context("POST precompute job")?;

        if !resp.status().is_success() {
            anyhow::bail!("precompute API returned {}", resp.status());
        }
        resp.json::<JobResponse>()
            .await
            .context("decode job response")
    }

    /// Removes a precompute job by ID.
    pub async fn deregister(&self, job_id: &str) -> anyhow::Result<()> {
        let resp = self
            .client
            .delete(format!("{}/api/v1/precompute/jobs/{job_id}", self.base_url))
            .send()
            .await
            .context("DELETE precompute job")?;

        if !resp.status().is_success() {
            anyhow::bail!("precompute API returned {}", resp.status());
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        http::StatusCode,
        routing::{delete, post},
        Json, Router,
    };
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    fn w(repeat_every: Option<Duration>, latency_sla: Option<Duration>) -> QueryWorkload {
        QueryWorkload {
            metric_name: "latency".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every,
            accuracy_sla: 0.01,
            latency_sla,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    fn dummy_plan() -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: SketchType::DDSketch,
                sketch_params: Default::default(),
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: None,
                mode: ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                enable_series_id: true,
                series_id_ttl_secs: 0,
                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: SketchType::DDSketch,
                group_by: vec![],
            },
            precompute: vec![],
            valid_until: chrono::Utc::now(),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
            staged_plan: None,
        }
    }

    #[test]
    fn should_precompute_true_when_repeat_lt_latency() {
        assert!(should_precompute(&w(
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(300))
        )));
    }

    #[test]
    fn should_precompute_false_when_repeat_geq_latency() {
        assert!(!should_precompute(&w(
            Some(Duration::from_secs(300)),
            Some(Duration::from_secs(60))
        )));
    }

    #[test]
    fn should_precompute_false_when_missing_fields() {
        assert!(!should_precompute(&w(None, Some(Duration::from_secs(300)))));
        assert!(!should_precompute(&w(Some(Duration::from_secs(60)), None)));
    }

    #[test]
    fn build_jobs_returns_job_when_eligible() {
        let workload = w(
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(600)),
        );
        let jobs = build_precompute_jobs(&workload, &dummy_plan(), "backend:4317");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].sketch_source, "backend:4317");
        assert_eq!(jobs[0].granularity, Duration::from_secs(60));
        assert!(jobs[0].query_expr.contains("latency"));
    }

    #[test]
    fn build_jobs_empty_when_not_eligible() {
        let workload = w(
            Some(Duration::from_secs(300)),
            Some(Duration::from_secs(60)),
        );
        let jobs = build_precompute_jobs(&workload, &dummy_plan(), "backend:4317");
        assert!(jobs.is_empty());
    }

    #[test]
    fn store_path_contains_metric_and_window() {
        let workload = w(
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(600)),
        );
        let jobs = build_precompute_jobs(&workload, &dummy_plan(), "backend:4317");
        assert!(jobs[0].store_path.contains("latency"));
        assert!(jobs[0].store_path.contains("5m"));
    }

    #[test]
    fn cardinality_uses_count_distinct_expr() {
        let mut workload = w(
            Some(Duration::from_secs(60)),
            Some(Duration::from_secs(600)),
        );
        workload.aggregations = vec![AggType::Cardinality];
        let jobs = build_precompute_jobs(&workload, &dummy_plan(), "backend:4317");
        assert!(
            jobs[0].query_expr.contains("count_distinct_over_time"),
            "got: {}",
            jobs[0].query_expr
        );
    }

    #[tokio::test]
    async fn client_register_success() {
        let app = Router::new().route(
            "/api/v1/precompute/jobs",
            post(|| async {
                (
                    StatusCode::OK,
                    Json(json!({
                        "job_id": "job-123",
                        "status": "created",
                        "created_at": null
                    })),
                )
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = PrecomputeClient::new(format!("http://{addr}"));
        let resp = client
            .register(&PrecomputeJob {
                query_expr: "quantile_over_time(0.99, latency[5m])".into(),
                granularity: Duration::from_secs(60),
                sketch_source: "backend:4317".into(),
                store_path: "precomputed/latency/p99/5m".into(),
            })
            .await
            .unwrap();
        assert_eq!(resp.job_id, "job-123");
    }

    #[tokio::test]
    async fn client_register_error_on_bad_status() {
        let app = Router::new().route(
            "/api/v1/precompute/jobs",
            post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = PrecomputeClient::new(format!("http://{addr}"));
        assert!(client
            .register(&PrecomputeJob {
                query_expr: "q".into(),
                granularity: Duration::from_secs(60),
                sketch_source: "s".into(),
                store_path: "p".into(),
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn client_deregister_success() {
        let app = Router::new().route(
            "/api/v1/precompute/jobs/:id",
            delete(|| async { StatusCode::NO_CONTENT }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = PrecomputeClient::new(format!("http://{addr}"));
        assert!(client.deregister("job-abc").await.is_ok());
    }
}
