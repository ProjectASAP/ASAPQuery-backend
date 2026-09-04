//! Continuous-monitoring (CDM) auto-emission helpers.
//!
//! These render the two artifacts a monitored standing query needs, both keyed
//! by the SAME content-addressed `agg_id` the Go edge derives from the metric
//! name (so the edge's per-window sketch and the coordinator's monitor line up):
//!
//!   1. [`edge_threshold_block`] — the `threshold:` YAML mapping that goes into
//!      the fused `asap_edge` processor's per-metric `metrics[]` entry (consumed
//!      by `asapedgeprocessor.ThresholdConfig`). The edge derives the agg_id
//!      from the metric name itself, so this block carries no agg_id.
//!   2. [`streaming_config_monitor_entry`] — the `monitors[]` JSON object for the
//!      backend `StreamingConfig` (consumed by `asap_types::MonitorSpec`), where
//!      `agg_id` IS carried and MUST equal [`agg_id_for_metric`].
//!
//! The functions are pure so they can be unit-tested and called from whichever
//! planner stage gains a monitor-intent slot. See
//! `ASAPCollector/docs/continuous-monitoring-tumbling-cost-analysis.md`.

pub use asap_types::MonitorFunctional as Functional;
use serde_yaml::{Mapping, Value};

/// One monitored standing-query intent: "alert when the global Σ of `metric`'s
/// `functional` crosses `tau`". τ/ε/window are authoritative at the coordinator;
/// the edge copies are advisory.
#[derive(Debug, Clone)]
pub struct MonitorIntent {
    pub metric: String,
    pub functional: Functional,
    /// CMS point-frequency key x (cms_point only); empty otherwise.
    pub key: String,
    /// Non-negative linear coefficients (linear_buckets only).
    pub coeffs: Vec<f64>,
    /// Coordinator MonitorService gRPC endpoint, e.g. "data-plane:4319".
    pub coordinator_url: String,
    pub tau: f64,
    pub epsilon: f64,
    pub window_ms: u64,
}

/// Content-addressed aggregation id for a metric name. This MUST byte-match the
/// Go edge's `asapedgeprocessor.fnv64` (FNV-1a-64 with the exact constants used
/// there) so a controller-emitted `monitors[]` entry targets the same agg_id the
/// edge stamps on its per-window sketch.
pub fn agg_id_for_metric(metric: &str) -> u64 {
    // NOTE: the edge uses the literal offset basis 1469598103934665603 and prime
    // 1099511628211; replicate verbatim (do NOT "correct" to the textbook FNV
    // basis — the two sides must agree, not match the spec).
    let mut h: u64 = 1469598103934665603;
    for b in metric.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

/// Render the edge `threshold:` YAML mapping for the fused asap_edge processor's
/// per-metric entry. Omits `key`/`coeffs` when not applicable to the functional.
pub fn edge_threshold_block(intent: &MonitorIntent) -> Value {
    let mut m = Mapping::new();
    m.insert("enabled".into(), Value::Bool(true));
    m.insert(
        "functional".into(),
        Value::String(intent.functional.as_str().to_string()),
    );
    if intent.functional == Functional::CmsPoint && !intent.key.is_empty() {
        m.insert("key".into(), Value::String(intent.key.clone()));
    }
    if intent.functional == Functional::LinearBuckets && !intent.coeffs.is_empty() {
        m.insert(
            "coeffs".into(),
            Value::Sequence(
                intent
                    .coeffs
                    .iter()
                    .map(|c| Value::Number((*c).into()))
                    .collect(),
            ),
        );
    }
    m.insert(
        "coordinator_url".into(),
        Value::String(intent.coordinator_url.clone()),
    );
    m.insert("tau".into(), Value::Number(intent.tau.into()));
    m.insert("epsilon".into(), Value::Number(intent.epsilon.into()));
    Value::Mapping(m)
}

/// Render the backend `StreamingConfig.monitors[]` JSON entry for this intent,
/// stamping the cross-language `agg_id`.
pub fn streaming_config_monitor_entry(intent: &MonitorIntent) -> serde_json::Value {
    serde_json::json!({
        "agg_id": agg_id_for_metric(&intent.metric),
        "functional": intent.functional.as_str(),
        "key": intent.key,
        "tau": intent.tau,
        "epsilon": intent.epsilon,
        "window_ms": intent.window_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_id_matches_go_fnv64() {
        // Pinned against the Go edge's fnv64 (asapedgeprocessor/warm_sketch.go),
        // computed directly from that implementation.
        assert_eq!(agg_id_for_metric("bytes_sent"), 10036451356866642109);
        assert_eq!(
            agg_id_for_metric("http_requests_total"),
            16346598078036168951
        );
        assert_eq!(agg_id_for_metric("x"), 4953295298048672641);
    }

    fn sum_intent() -> MonitorIntent {
        MonitorIntent {
            metric: "bytes_sent".into(),
            functional: Functional::Sum,
            key: String::new(),
            coeffs: Vec::new(),
            coordinator_url: "data-plane:4319".into(),
            tau: 100.0,
            epsilon: 0.05,
            window_ms: 60_000,
        }
    }

    #[test]
    fn edge_threshold_block_sum_shape() {
        let v = edge_threshold_block(&sum_intent());
        let m = v.as_mapping().unwrap();
        assert_eq!(m.get(Value::from("enabled")).unwrap(), &Value::Bool(true));
        assert_eq!(
            m.get(Value::from("functional")).unwrap().as_str().unwrap(),
            "sum"
        );
        assert_eq!(
            m.get(Value::from("coordinator_url"))
                .unwrap()
                .as_str()
                .unwrap(),
            "data-plane:4319"
        );
        // Sum carries no key/coeffs.
        assert!(m.get(Value::from("key")).is_none());
        assert!(m.get(Value::from("coeffs")).is_none());
    }

    #[test]
    fn edge_threshold_block_cms_point_carries_key() {
        let mut intent = sum_intent();
        intent.functional = Functional::CmsPoint;
        intent.key = "svc=checkout".into();
        let v = edge_threshold_block(&intent);
        let m = v.as_mapping().unwrap();
        assert_eq!(
            m.get(Value::from("functional")).unwrap().as_str().unwrap(),
            "cms_point"
        );
        assert_eq!(
            m.get(Value::from("key")).unwrap().as_str().unwrap(),
            "svc=checkout"
        );
    }

    #[test]
    fn streaming_entry_agg_id_matches_edge() {
        let entry = streaming_config_monitor_entry(&sum_intent());
        assert_eq!(
            entry["agg_id"].as_u64().unwrap(),
            agg_id_for_metric("bytes_sent")
        );
        assert_eq!(entry["tau"].as_f64().unwrap(), 100.0);
        assert_eq!(entry["window_ms"].as_u64().unwrap(), 60_000);
    }

    #[test]
    fn streaming_entry_deserializes_as_monitor_spec() {
        // The emitted JSON must round-trip into the backend's MonitorSpec.
        let entry = streaming_config_monitor_entry(&sum_intent());
        let spec: asap_types::MonitorSpec =
            serde_json::from_value(entry).expect("MonitorSpec deserialize");
        assert_eq!(spec.agg_id, agg_id_for_metric("bytes_sent"));
        assert_eq!(spec.tau, 100.0);
        assert_eq!(spec.window_ms, 60_000);
    }
}
