//! `GET /metrics` — the control plane as a Prometheus exporter.
//!
//! The control plane already holds per-agent runtime samples in
//! [`RuntimeSamplesStore`](crate::runtime_samples::RuntimeSamplesStore)
//! (fed by the agents' `sketch-runtime` push pipeline). This
//! module renders that store's state on demand in Prometheus
//! exposition format so a bog-standard Prom `scrape_configs`
//! entry pointed at `control_plane:8080/metrics` picks up every
//! paper §6 signal without any extra agent-side plumbing.
//!
//! ## Rendering contract
//!
//! For each `(source, sketch, impl)` key in the store we emit
//! the **latest** sample's headline gauges. That's what a
//! scrape is — point-in-time, not history. Prom does retention;
//! we do fan-out-to-labels.
//!
//! Control-plane-wide counters (batches received, decode errors,
//! evictions) come from `RuntimeSamplesStats` and are pure
//! `Counter`s.
//!
//! ## What lives in the registry
//!
//! All gauges are labelled `{source, sketch, impl}`:
//!
//! | Metric | Type | Source field (in `bench`) |
//! |---|---|---|
//! | `asap_runtime_throughput_items_per_sec` | Gauge | `throughput_items_per_sec.mean` |
//! | `asap_runtime_latency_p50_ns` | Gauge | `latency_ns.p50` |
//! | `asap_runtime_latency_p99_ns` | Gauge | `latency_ns.p99` |
//! | `asap_runtime_memory_bytes` | Gauge | `memory_bytes` |
//! | `asap_runtime_last_seen_unix_seconds` | Gauge | record `timestamp` (freshness signal) |
//!
//! Control-plane-wide (no labels):
//!
//! | Metric | Type |
//! |---|---|
//! | `asap_runtime_samples_batches_received_total` | Counter |
//! | `asap_runtime_samples_records_stored_total` | Counter |
//! | `asap_runtime_samples_records_evicted_total` | Counter |
//! | `asap_runtime_samples_decode_errors_total` | Counter |

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use prometheus::{Encoder, GaugeVec, IntCounter, Opts, Registry, TextEncoder};

use crate::runtime_samples::RuntimeSamplesStats;
use crate::runtime_samples::RuntimeSamplesStore;
use crate::store::PlanStore;

const LABELS: &[&str] = &["source", "sketch", "impl"];
const PLAN_LABELS: &[&str] = &["metric", "plan_id"];

/// The Prometheus registry + pre-built metric handles. Built
/// once at startup; the `/metrics` handler pulls the latest
/// sample per key out of the store on each scrape and updates
/// the gauge vecs before encoding.
pub struct MetricsRegistry {
    registry: Registry,
    throughput: GaugeVec,
    latency_p50: GaugeVec,
    latency_p99: GaugeVec,
    memory_bytes: GaugeVec,
    last_seen_unix: GaugeVec,
    // control-plane-wide counters, refreshed from atomic stats
    batches_received: IntCounter,
    records_stored: IntCounter,
    records_evicted: IntCounter,
    decode_errors: IntCounter,
    /// `asap_active_plan_id{metric="...", plan_id="<hash>"} 1` —
    /// rendered at scrape time from the `PlanStore` snapshot. The
    /// plan_id label is a stable hash of the current plan's content,
    /// so a re-plan flips the label value (and the previous time-series
    /// stops being emitted on the next scrape). The replay client and
    /// `plan_transition.py` look for this metric to detect plan
    /// transitions.
    active_plan_id: GaugeVec,
}

impl MetricsRegistry {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let throughput = GaugeVec::new(
            Opts::new(
                "asap_runtime_throughput_items_per_sec",
                "Latest runtime throughput reported by each (source, sketch, impl)",
            ),
            LABELS,
        )
        .expect("valid gauge vec");
        let latency_p50 = GaugeVec::new(
            Opts::new(
                "asap_runtime_latency_p50_ns",
                "Latest p50 latency ns reported by each (source, sketch, impl)",
            ),
            LABELS,
        )
        .unwrap();
        let latency_p99 = GaugeVec::new(
            Opts::new(
                "asap_runtime_latency_p99_ns",
                "Latest p99 latency ns reported by each (source, sketch, impl)",
            ),
            LABELS,
        )
        .unwrap();
        let memory_bytes = GaugeVec::new(
            Opts::new(
                "asap_runtime_memory_bytes",
                "Latest memory footprint of each (source, sketch, impl)",
            ),
            LABELS,
        )
        .unwrap();
        let last_seen_unix = GaugeVec::new(
            Opts::new(
                "asap_runtime_last_seen_unix_seconds",
                "Unix timestamp of the latest sample received from each (source, sketch, impl). \
                 Compare against `time() - N` to alert on a stale agent.",
            ),
            LABELS,
        )
        .unwrap();

        let batches_received = IntCounter::new(
            "asap_runtime_samples_batches_received_total",
            "Count of push batches accepted by /api/v1/runtime-samples",
        )
        .unwrap();
        let records_stored = IntCounter::new(
            "asap_runtime_samples_records_stored_total",
            "Count of individual records stored in the ring buffer",
        )
        .unwrap();
        let records_evicted = IntCounter::new(
            "asap_runtime_samples_records_evicted_total",
            "Count of records evicted by the per-key ring-buffer FIFO",
        )
        .unwrap();
        let decode_errors = IntCounter::new(
            "asap_runtime_samples_decode_errors_total",
            "Count of decode / parse failures on incoming batches",
        )
        .unwrap();

        let active_plan_id = GaugeVec::new(
            Opts::new(
                "asap_active_plan_id",
                "Currently-published plan id per metric. The plan_id label is a stable \
                 hash of the plan's content; a re-plan changes the label value.",
            ),
            PLAN_LABELS,
        )
        .unwrap();

        registry.register(Box::new(active_plan_id.clone())).unwrap();
        registry.register(Box::new(throughput.clone())).unwrap();
        registry.register(Box::new(latency_p50.clone())).unwrap();
        registry.register(Box::new(latency_p99.clone())).unwrap();
        registry.register(Box::new(memory_bytes.clone())).unwrap();
        registry.register(Box::new(last_seen_unix.clone())).unwrap();
        registry
            .register(Box::new(batches_received.clone()))
            .unwrap();
        registry.register(Box::new(records_stored.clone())).unwrap();
        registry
            .register(Box::new(records_evicted.clone()))
            .unwrap();
        registry.register(Box::new(decode_errors.clone())).unwrap();

        Arc::new(Self {
            registry,
            throughput,
            latency_p50,
            latency_p99,
            memory_bytes,
            last_seen_unix,
            batches_received,
            records_stored,
            records_evicted,
            decode_errors,
            active_plan_id,
        })
    }

    /// Refresh the `asap_active_plan_id` gauge at scrape time.
    /// Resets prior label sets so a re-plan stops emitting the old
    /// (metric, plan_id) pair on the next scrape.
    fn refresh_plan_ids(&self, plan_store: &PlanStore) {
        // Reset is necessary because GaugeVec keeps every label set
        // ever observed; without this a re-plan would leave the old
        // plan_id label permanently emitting a stale value.
        self.active_plan_id.reset();
        // B2 (metric, role): iterate per-pair so each role's plan
        // emits its own `(metric, plan_id)` gauge value. The metric
        // label retains the un-decorated metric name (pre-B2 wire
        // shape) — a metric with multiple roles surfaces multiple
        // active_plan_id rows under the same metric label.
        for (metric, role) in plan_store.keys() {
            let Ok(plan) = plan_store.get(&metric, role) else {
                continue;
            };
            // Stable hash of the plan's debug repr — good enough for
            // a label value, doesn't need to be cryptographic.
            let mut hasher = DefaultHasher::new();
            // Cover the fields the planner actually changes per
            // re-plan: agent sketch+mode+delta+grouping, and valid_until
            // (to catch refresh-only re-plans). Role is folded in so
            // distinct-role plans produce distinct ids even when their
            // agent_config fields happen to coincide.
            format!(
                "{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
                role,
                plan.agent_config.sketch_type,
                plan.agent_config.mode,
                plan.agent_config.delta_transmission,
                plan.agent_config.window_duration,
                plan.agent_config.aggregate_by,
                plan.valid_until,
            )
            .hash(&mut hasher);
            let plan_id = format!("p{:016x}", hasher.finish());
            self.active_plan_id
                .with_label_values(&[metric.as_str(), plan_id.as_str()])
                .set(1.0);
        }
    }

    /// Walk the store and push the latest sample per key into
    /// the gauge vecs. Called at scrape time, not per-record —
    /// cost scales with (# keys), not (# records).
    fn refresh_gauges(&self, store: &RuntimeSamplesStore) {
        for key in store.keys() {
            let Some(rec) = store.latest(&key) else {
                continue;
            };
            let labels = &[
                key.source.as_str(),
                key.sketch.as_str(),
                key.impl_name.as_str(),
            ];

            // Pull the interesting numeric fields from the
            // opaque JSON payload. Missing fields silently stay
            // at their last value — an agent that dropped a
            // metric briefly won't zap the gauge to 0.
            let bench = rec.payload.get("bench");
            if let Some(v) = bench
                .and_then(|b| b.get("throughput_items_per_sec"))
                .and_then(|t| t.get("mean"))
                .and_then(|m| m.as_f64())
            {
                self.throughput.with_label_values(labels).set(v);
            }
            if let Some(v) = bench
                .and_then(|b| b.get("latency_ns"))
                .and_then(|l| l.get("p50"))
                .and_then(|m| m.as_f64())
            {
                self.latency_p50.with_label_values(labels).set(v);
            }
            if let Some(v) = bench
                .and_then(|b| b.get("latency_ns"))
                .and_then(|l| l.get("p99"))
                .and_then(|m| m.as_f64())
            {
                self.latency_p99.with_label_values(labels).set(v);
            }
            if let Some(v) = bench
                .and_then(|b| b.get("memory_bytes"))
                .and_then(|m| m.as_f64())
            {
                self.memory_bytes.with_label_values(labels).set(v);
            }
            if let Some(ts) = rec.payload.get("timestamp").and_then(|t| t.as_str()) {
                if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(ts) {
                    self.last_seen_unix
                        .with_label_values(labels)
                        .set(parsed.timestamp() as f64);
                }
            }
        }
    }

    /// Sync control-plane-wide counters from the store's atomic
    /// stats. `IntCounter` only exposes `.inc()` — so we pump
    /// in the delta since the last scrape rather than `.set()`.
    fn refresh_counters(&self, stats: &RuntimeSamplesStats) {
        fn delta(target: &IntCounter, snapshot: u64) {
            let have = target.get();
            if snapshot > have {
                target.inc_by(snapshot - have);
            }
        }
        delta(
            &self.batches_received,
            stats.batches_received.load(Ordering::Relaxed),
        );
        delta(
            &self.records_stored,
            stats.records_stored.load(Ordering::Relaxed),
        );
        delta(
            &self.records_evicted,
            stats.records_evicted.load(Ordering::Relaxed),
        );
        delta(
            &self.decode_errors,
            stats.decode_errors.load(Ordering::Relaxed),
        );
    }
}

#[derive(Clone)]
pub struct MetricsState {
    pub registry: Arc<MetricsRegistry>,
    pub store: Arc<RuntimeSamplesStore>,
    pub stats: Arc<RuntimeSamplesStats>,
    /// Optional `PlanStore` reference; when present the exposer
    /// renders `asap_active_plan_id` per metric. `None` is fine for
    /// unit tests that exercise only the runtime-samples path.
    pub plan_store: Option<Arc<PlanStore>>,
}

pub async fn handle_metrics(State(state): State<MetricsState>) -> Response {
    state.registry.refresh_gauges(&state.store);
    state.registry.refresh_counters(&state.stats);
    if let Some(ps) = state.plan_store.as_ref() {
        state.registry.refresh_plan_ids(ps);
    }

    let metric_families = state.registry.registry.gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::with_capacity(1024);
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("prom encode failed: {e}"),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, encoder.format_type())],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_samples::RuntimeRecord;
    use serde_json::json;

    fn seeded_store() -> (Arc<RuntimeSamplesStore>, Arc<RuntimeSamplesStats>) {
        let store = RuntimeSamplesStore::new(16);
        let stats = store.stats();
        for (source, sketch, impl_name, tp, p99, mem) in [
            ("dc-a", "cms", "oxide", 4.2e7, 120u64, 40960u64),
            ("dc-a", "hll", "lib", 7.4e7, 21, 16384),
        ] {
            // Build a record whose payload mirrors the v1 bench
            // shape — exactly what the store would have stored
            // after receiving a PushExporter batch.
            let payload = json!({
                "schema_version": 1,
                "mode": "runtime",
                "timestamp": "2026-04-21T19:00:00Z",
                "bench": {
                    "throughput_items_per_sec": { "mean": tp, "stddev": 0.0 },
                    "latency_ns": { "p50": 10, "p99": p99 },
                    "memory_bytes": mem,
                }
            });
            let rec = RuntimeRecord {
                source: source.into(),
                sketch: sketch.into(),
                impl_name: impl_name.into(),
                schema_version: 1,
                payload,
            };
            // Inject via the public append-by-record shim we
            // use in tests: the store's `append` is crate-public
            // from `runtime_samples::tests` only, so go through
            // the handler-side entry point with a JSON body in
            // a real integration; here we lean on the trait.
            // Shortcut: use store's pub `stats` + manual push
            // by serialising + using the handler path. Simpler
            // for this unit test: add a test-only helper below.
            insert_for_test(&store, rec);
        }
        (store, stats)
    }

    // Test-only shim — uses the `append_for_test` hook the
    // `runtime_samples` module exposes under `cfg(test)`.
    fn insert_for_test(store: &RuntimeSamplesStore, rec: RuntimeRecord) {
        store.append_for_test(rec);
    }

    #[tokio::test]
    async fn metrics_scrape_reflects_latest_per_key_sample() {
        let (store, stats) = seeded_store();
        let registry = MetricsRegistry::new();
        let metric_state = MetricsState {
            registry: Arc::clone(&registry),
            store: Arc::clone(&store),
            stats: Arc::clone(&stats),
            plan_store: None,
        };
        let resp = handle_metrics(State(metric_state)).await;
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        // Sanity: exposition format + at least one of our
        // gauges should be present per key.
        assert!(text.contains("# TYPE asap_runtime_throughput_items_per_sec gauge"));
        assert!(text.contains(
            "asap_runtime_throughput_items_per_sec{impl=\"oxide\",sketch=\"cms\",source=\"dc-a\"}"
        ));
        assert!(text
            .contains("asap_runtime_latency_p99_ns{impl=\"lib\",sketch=\"hll\",source=\"dc-a\"}"));
    }

    #[test]
    fn plan_id_emitted_per_metric_and_changes_on_replan() {
        use crate::types::*;
        use chrono::Utc;

        fn make_plan(sketch: SketchType, valid_secs: i64) -> CollectionPlan {
            CollectionPlan {
                agent_config: AgentCollectorConfig {
                    output_mode: OutputMode::Sketch,
                    sketch_type: sketch.clone(),
                    sketch_params: Default::default(),
                    aggregate_by: vec![],
                    label_matchers: vec![],
                    window_duration: None,
                    mode: ProcessorMode::Window,
                    enable_self_monitoring: true,
                    transmit_sketch: true,
                    drop_original: true,
                    delta_transmission: true,
                    delta_threshold: 0.0,
                    gos: None,
                    enable_series_id: false,
                    series_id_ttl_secs: 300,
                    data_sink: AgentDataSink::default(),
                },
                gateway_config: GatewayCollectorConfig { passthrough: true },
                valid_until: Utc::now() + chrono::Duration::seconds(valid_secs),
                delta_decision: Default::default(),
                transmission_cost_summary: Default::default(),
            }
        }

        use crate::workload::AggRole;
        let plan_store = Arc::new(PlanStore::new());
        plan_store.set(
            "http_requests_total",
            AggRole::Quantile,
            make_plan(SketchType::DDSketch, 600),
        );
        plan_store.set(
            "http_requests_total_latency_ms",
            AggRole::Quantile,
            make_plan(SketchType::HLL, 600),
        );

        let registry = MetricsRegistry::new();
        registry.refresh_plan_ids(&plan_store);
        // Render and check exposition contains both metrics with
        // distinct plan_id labels.
        let mfs = registry.registry.gather();
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        encoder.encode(&mfs, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            text.contains("asap_active_plan_id{metric=\"http_requests_total\""),
            "expected plan_id for http_requests_total in:\n{text}"
        );
        assert!(
            text.contains("asap_active_plan_id{metric=\"http_requests_total_latency_ms\""),
            "expected plan_id for http_requests_total_latency_ms in:\n{text}"
        );
        // Re-plan with a different sketch must change the plan_id label.
        let before = text.clone();
        plan_store.set(
            "http_requests_total",
            AggRole::Quantile,
            make_plan(SketchType::KLL, 600),
        );
        registry.refresh_plan_ids(&plan_store);
        let mfs = registry.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&mfs, &mut buf).unwrap();
        let after = String::from_utf8(buf).unwrap();
        assert_ne!(before, after, "plan_id label should change after re-plan");
    }

    #[test]
    fn counters_are_monotonic_across_refreshes() {
        let store = RuntimeSamplesStore::new(16);
        let stats = store.stats();
        stats.batches_received.fetch_add(5, Ordering::Relaxed);
        let registry = MetricsRegistry::new();
        registry.refresh_counters(&stats);
        assert_eq!(registry.batches_received.get(), 5);
        // Second refresh adds only the delta.
        stats.batches_received.fetch_add(3, Ordering::Relaxed);
        registry.refresh_counters(&stats);
        assert_eq!(registry.batches_received.get(), 8);
        // No regression: if the snapshot somehow went down
        // (shouldn't, but guard), we hold the counter flat
        // rather than decrementing.
        registry.refresh_counters(&stats);
        assert_eq!(registry.batches_received.get(), 8);
    }
}
