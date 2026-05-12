/// Feedback loop: scrapes Prometheus /metrics from OTel collectors and fires
/// violation callbacks to trigger re-planning, and an optional metrics callback
/// to feed observed bandwidth/CPU data into the EMA cost model (SP-5/SP-8).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::types::SketchType;

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CollectorMetrics {
    pub agent_id: String,
    pub sketch_size_bytes: f64,
    pub cpu_seconds_total: f64,
    pub samples_ingested: f64,
    pub error_rate: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ViolationKind {
    Bandwidth,
    Accuracy,
    Cpu,
}

impl std::fmt::Display for ViolationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViolationKind::Bandwidth => write!(f, "bandwidth"),
            ViolationKind::Accuracy => write!(f, "accuracy"),
            ViolationKind::Cpu => write!(f, "cpu"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Violation {
    pub agent_id: String,
    pub kind: ViolationKind,
    pub observed: f64,
    pub threshold: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    pub max_sketch_size_bytes: f64,
    pub max_error_rate: f64,
    pub max_cpu_micros_per_sample: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            max_sketch_size_bytes: 5.0 * 1024.0 * 1024.0, // 5 MB
            max_error_rate: 0.02,                         // 2 %
            max_cpu_micros_per_sample: 5.0,               // 5 µs/sample
        }
    }
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub agent_id: String,
    pub metrics_url: String,
    /// The sketch type currently deployed to this agent; used to attribute
    /// scraped metrics to the right EMA bucket.
    pub sketch_type: Option<SketchType>,
}

impl Endpoint {
    pub fn new(agent_id: impl Into<String>, metrics_url: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            metrics_url: metrics_url.into(),
            sketch_type: None,
        }
    }
}

/// Data reported to the `on_metrics` callback after each successful scrape.
#[derive(Debug, Clone)]
pub struct ScrapedData {
    pub agent_id: String,
    /// The sketch type configured on this endpoint at scrape time (if known).
    pub sketch_type: Option<SketchType>,
    /// Current total sketch size in bytes at the agent.
    pub sketch_size_bytes: f64,
    /// Derived µs/sample over the last scrape window; `None` on the very first
    /// scrape because there is no previous baseline yet.
    pub cpu_micros_per_sample: Option<f64>,
}

pub type OnViolationFn = Arc<dyn Fn(Violation) + Send + Sync>;
pub type OnMetricsFn = Arc<dyn Fn(ScrapedData) + Send + Sync>;

// ── Scraper ───────────────────────────────────────────────────────────────────

pub struct Scraper {
    endpoints: Arc<RwLock<Vec<Endpoint>>>,
    thresholds: Thresholds,
    on_violation: OnViolationFn,
    on_metrics: Option<OnMetricsFn>,
    interval: Duration,
    client: reqwest::Client,
    last: Mutex<HashMap<String, CollectorMetrics>>,
}

impl Scraper {
    pub fn new(
        endpoints: Vec<Endpoint>,
        thresholds: Thresholds,
        on_violation: OnViolationFn,
        interval: Duration,
    ) -> Self {
        Self {
            endpoints: Arc::new(RwLock::new(endpoints)),
            thresholds,
            on_violation,
            on_metrics: None,
            interval,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Attach a callback invoked after every successful scrape with observed metrics.
    pub fn with_on_metrics(mut self, cb: OnMetricsFn) -> Self {
        self.on_metrics = Some(cb);
        self
    }

    /// Registers a new endpoint to be scraped. Safe to call from any async context.
    pub async fn add_endpoint(&self, ep: Endpoint) {
        info!(agent = %ep.agent_id, "adding scrape endpoint");
        self.endpoints.write().await.push(ep);
    }

    /// Removes an endpoint by agent ID. No-op if not found.
    pub async fn remove_endpoint(&self, agent_id: &str) {
        let mut eps = self.endpoints.write().await;
        eps.retain(|e| e.agent_id != agent_id);
        info!(agent = %agent_id, "removed scrape endpoint");
    }

    /// Updates the sketch type recorded for an existing endpoint.
    /// Called after a new plan is pushed so EMA attribution is accurate.
    pub async fn set_sketch_type(&self, agent_id: &str, st: SketchType) {
        let mut eps = self.endpoints.write().await;
        for ep in eps.iter_mut() {
            if ep.agent_id == agent_id {
                ep.sketch_type = Some(st);
                return;
            }
        }
    }

    /// Starts the scrape loop; runs until the process exits.
    pub async fn run(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.scrape_all().await;
        }
    }

    /// Performs a single scrape of all endpoints. Useful for tests.
    pub async fn scrape_all(&self) {
        // Clone the endpoint list so we don't hold the lock across async scrapes.
        let endpoints = self.endpoints.read().await.clone();
        for ep in &endpoints {
            match self.scrape(ep).await {
                Ok(m) => self.analyze(&m, ep.sketch_type.as_ref()),
                Err(e) => warn!(agent = %ep.agent_id, "scrape failed: {e}"),
            }
        }
    }

    async fn scrape(&self, ep: &Endpoint) -> anyhow::Result<CollectorMetrics> {
        let text = self
            .client
            .get(&ep.metrics_url)
            .send()
            .await
            .context("GET metrics")?
            .text()
            .await
            .context("read body")?;

        let mut m = CollectorMetrics {
            agent_id: ep.agent_id.clone(),
            sketch_size_bytes: 0.0,
            cpu_seconds_total: 0.0,
            samples_ingested: 0.0,
            error_rate: 0.0,
        };
        parse_prometheus_text(&text, &mut m);
        Ok(m)
    }

    fn analyze(&self, m: &CollectorMetrics, sketch_type: Option<&SketchType>) {
        // Bandwidth / sketch size.
        if m.sketch_size_bytes > self.thresholds.max_sketch_size_bytes {
            (self.on_violation)(Violation {
                agent_id: m.agent_id.clone(),
                kind: ViolationKind::Bandwidth,
                observed: m.sketch_size_bytes,
                threshold: self.thresholds.max_sketch_size_bytes,
            });
        }

        // Accuracy / error rate.
        if m.error_rate > self.thresholds.max_error_rate {
            (self.on_violation)(Violation {
                agent_id: m.agent_id.clone(),
                kind: ViolationKind::Accuracy,
                observed: m.error_rate,
                threshold: self.thresholds.max_error_rate,
            });
        }

        // CPU: compare δCPU/δsamples with the previous scrape.
        let mut last = self.last.lock().unwrap();
        let cpu_micros = if let Some(prev) = last.get(&m.agent_id) {
            let delta_samples = m.samples_ingested - prev.samples_ingested;
            let delta_cpu = m.cpu_seconds_total - prev.cpu_seconds_total;
            if delta_samples > 0.0 {
                let micros_per_sample = (delta_cpu / delta_samples) * 1e6;
                if micros_per_sample > self.thresholds.max_cpu_micros_per_sample {
                    (self.on_violation)(Violation {
                        agent_id: m.agent_id.clone(),
                        kind: ViolationKind::Cpu,
                        observed: micros_per_sample,
                        threshold: self.thresholds.max_cpu_micros_per_sample,
                    });
                }
                Some(micros_per_sample)
            } else {
                None
            }
        } else {
            None
        };
        last.insert(m.agent_id.clone(), m.clone());
        drop(last);

        // Fire on_metrics callback so callers can feed EMA / telemetry.
        if let Some(cb) = &self.on_metrics {
            cb(ScrapedData {
                agent_id: m.agent_id.clone(),
                sketch_type: sketch_type.cloned(),
                sketch_size_bytes: m.sketch_size_bytes,
                cpu_micros_per_sample: cpu_micros,
            });
        }
    }
}

// ── Prometheus text parser ────────────────────────────────────────────────────

fn parse_prometheus_text(text: &str, m: &mut CollectorMetrics) {
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Handle lines with optional labels: metric_name{...} value [timestamp]
        // Split on whitespace to get name and value parts.
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() < 2 {
            continue;
        }
        // Strip label block {…} from the metric name, if any.
        let name = parts[0].split('{').next().unwrap_or(parts[0]);
        let val_str = parts[1].split_whitespace().next().unwrap_or("");
        let Ok(val) = val_str.parse::<f64>() else {
            continue;
        };
        match name {
            "otelcol_sketch_size_bytes" => m.sketch_size_bytes = val,
            "process_cpu_seconds_total" => m.cpu_seconds_total = val,
            "otelcol_processor_accepted_metric_points" => m.samples_ingested = val,
            "otelcol_sketch_error_rate" => m.error_rate = val,
            _ => {}
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tokio::net::TcpListener;

    const NORMAL_PAYLOAD: &str = "
otelcol_sketch_size_bytes 1048576
process_cpu_seconds_total 0.5
otelcol_processor_accepted_metric_points 100000
otelcol_sketch_error_rate 0.001
";

    const HIGH_BANDWIDTH_PAYLOAD: &str = "
otelcol_sketch_size_bytes 10485760
process_cpu_seconds_total 1.0
otelcol_processor_accepted_metric_points 200000
otelcol_sketch_error_rate 0.001
";

    const HIGH_ERROR_RATE_PAYLOAD: &str = "
otelcol_sketch_size_bytes 512000
process_cpu_seconds_total 1.0
otelcol_processor_accepted_metric_points 200000
otelcol_sketch_error_rate 0.05
";

    async fn serve_metrics(payload: &'static str) -> String {
        let app = Router::new().route("/metrics", get(move || async move { payload }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/metrics")
    }

    fn scraper_with_violations(url: &str) -> (Arc<Scraper>, Arc<Mutex<Vec<Violation>>>) {
        let violations: Arc<Mutex<Vec<Violation>>> = Arc::new(Mutex::new(vec![]));
        let v2 = Arc::clone(&violations);
        let s = Arc::new(Scraper::new(
            vec![Endpoint::new("a1", url)],
            Thresholds::default(),
            Arc::new(move |v| v2.lock().unwrap().push(v)),
            Duration::from_secs(60),
        ));
        (s, violations)
    }

    #[tokio::test]
    async fn no_violation_on_normal_metrics() {
        let url = serve_metrics(NORMAL_PAYLOAD).await;
        let (s, violations) = scraper_with_violations(&url);
        s.scrape_all().await;
        assert!(violations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn bandwidth_violation() {
        let url = serve_metrics(HIGH_BANDWIDTH_PAYLOAD).await; // 10MB > 5MB threshold
        let (s, violations) = scraper_with_violations(&url);
        s.scrape_all().await;
        let v = violations.lock().unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].kind, ViolationKind::Bandwidth);
        assert!(v[0].observed > v[0].threshold);
    }

    #[tokio::test]
    async fn accuracy_violation() {
        let url = serve_metrics(HIGH_ERROR_RATE_PAYLOAD).await; // 5% > 2% threshold
        let (s, violations) = scraper_with_violations(&url);
        s.scrape_all().await;
        let v = violations.lock().unwrap();
        let has_accuracy = v.iter().any(|vio| vio.kind == ViolationKind::Accuracy);
        assert!(has_accuracy, "expected accuracy violation, got: {v:?}");
    }

    #[tokio::test]
    async fn cpu_violation_on_delta() {
        // First call: baseline (0 CPU, 0 samples).
        // Second call: 5ms CPU for 100 samples → 50 µs/sample > 5 µs threshold.
        let call_count = Arc::new(Mutex::new(0u32));
        let c2 = Arc::clone(&call_count);
        let app = Router::new().route("/metrics", get(move || {
            let count = Arc::clone(&c2);
            async move {
                let mut n = count.lock().unwrap();
                *n += 1;
                if *n == 1 {
                    "process_cpu_seconds_total 0\notelcol_processor_accepted_metric_points 0\notelcol_sketch_size_bytes 0\notelcol_sketch_error_rate 0\n"
                } else {
                    "process_cpu_seconds_total 0.005\notelcol_processor_accepted_metric_points 100\notelcol_sketch_size_bytes 0\notelcol_sketch_error_rate 0\n"
                }
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let violations: Arc<Mutex<Vec<Violation>>> = Arc::new(Mutex::new(vec![]));
        let v2 = Arc::clone(&violations);
        let s = Arc::new(Scraper::new(
            vec![Endpoint::new("a1", format!("http://{addr}/metrics"))],
            Thresholds::default(),
            Arc::new(move |v| v2.lock().unwrap().push(v)),
            Duration::from_secs(60),
        ));
        s.scrape_all().await; // baseline
        s.scrape_all().await; // delta → CPU violation

        let v = violations.lock().unwrap();
        assert!(
            v.iter().any(|vio| vio.kind == ViolationKind::Cpu),
            "expected CPU violation, got: {v:?}"
        );
    }

    #[tokio::test]
    async fn multiple_endpoints_only_bad_violates() {
        let ok_url = serve_metrics(NORMAL_PAYLOAD).await;
        let bad_url = serve_metrics(HIGH_BANDWIDTH_PAYLOAD).await;

        let violations: Arc<Mutex<Vec<Violation>>> = Arc::new(Mutex::new(vec![]));
        let v2 = Arc::clone(&violations);
        let s = Arc::new(Scraper::new(
            vec![Endpoint::new("ok", ok_url), Endpoint::new("bad", bad_url)],
            Thresholds::default(),
            Arc::new(move |v| v2.lock().unwrap().push(v)),
            Duration::from_secs(60),
        ));
        s.scrape_all().await;

        let v = violations.lock().unwrap();
        assert!(
            v.iter().all(|vio| vio.agent_id == "bad"),
            "only bad agent should violate: {v:?}"
        );
        assert!(v.iter().any(|vio| vio.agent_id == "bad"));
    }

    #[tokio::test]
    async fn unreachable_endpoint_no_panic() {
        let (s, violations) = scraper_with_violations("http://127.0.0.1:1/metrics");
        s.scrape_all().await; // should log warning, not panic
        assert!(violations.lock().unwrap().is_empty());
    }

    #[test]
    fn violation_kind_display() {
        assert_eq!(ViolationKind::Bandwidth.to_string(), "bandwidth");
        assert_eq!(ViolationKind::Accuracy.to_string(), "accuracy");
        assert_eq!(ViolationKind::Cpu.to_string(), "cpu");
    }

    #[tokio::test]
    async fn on_metrics_callback_fires() {
        let url = serve_metrics(NORMAL_PAYLOAD).await;
        let scraped: Arc<Mutex<Vec<ScrapedData>>> = Arc::new(Mutex::new(vec![]));
        let s2 = Arc::clone(&scraped);
        let s = Arc::new(
            Scraper::new(
                vec![Endpoint::new("a1", url)],
                Thresholds::default(),
                Arc::new(|_| {}),
                Duration::from_secs(60),
            )
            .with_on_metrics(Arc::new(move |d| s2.lock().unwrap().push(d))),
        );
        s.scrape_all().await;
        let got = scraped.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].agent_id, "a1");
        assert!(got[0].sketch_size_bytes > 0.0);
    }

    #[tokio::test]
    async fn add_remove_endpoint() {
        let url = serve_metrics(NORMAL_PAYLOAD).await;
        let violations: Arc<Mutex<Vec<Violation>>> = Arc::new(Mutex::new(vec![]));
        let v2 = Arc::clone(&violations);
        let s = Arc::new(Scraper::new(
            vec![],
            Thresholds::default(),
            Arc::new(move |v| v2.lock().unwrap().push(v)),
            Duration::from_secs(60),
        ));
        // Initially no endpoints → no violations.
        s.scrape_all().await;
        assert!(violations.lock().unwrap().is_empty());

        // Add endpoint and scrape.
        s.add_endpoint(Endpoint::new("a1", &url)).await;
        s.scrape_all().await;
        // NORMAL_PAYLOAD → no violation.
        assert!(violations.lock().unwrap().is_empty());

        // Remove and verify nothing scrapes.
        s.remove_endpoint("a1").await;
        assert!(s.endpoints.read().await.is_empty());
    }
}
