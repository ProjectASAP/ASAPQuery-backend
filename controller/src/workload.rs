//! Declarative workload registration.
//!
//! Loads a YAML file describing workloads and their assignments so the
//! controller can pre-populate the plan store and assign workloads to
//! agents on connect without requiring an explicit HTTP `POST /api/v1/plan`.

use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, warn};

use crate::types::SketchType;

/// A single workload entry from the workloads YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadEntry {
    /// Metric name this workload targets (e.g. `http_request_duration_seconds`).
    pub metric_name: String,
    /// PromQL / SQL query string for the planner.
    #[serde(default)]
    pub query_string: Option<String>,
    /// Required accuracy SLA (0.0 – 1.0).
    #[serde(default = "default_accuracy_sla")]
    pub accuracy_sla: f64,
    /// Role that should receive this workload (e.g. `"agent"`, `"backend"`).
    #[serde(default = "default_role")]
    pub assign_to_role: String,
    /// Optional explicit sketch family override. When set, the planner pins
    /// this family for the metric (modulo `(sketch, statistic)` validity
    /// per `sketch_algebra::capability_matching::is_valid_pair`). Threaded
    /// into `QueryWorkload::sketch_type_override` by the registry pre-pop
    /// path so the typed L4 binding (`bind_workload_typed`) honours it.
    ///
    /// MVP-§46 contract entries 5–8 in `deploy/configs/mvp-workload.yaml`
    /// rely on this field to pin HLL / CountSketch / CountMinSketch
    /// against metrics whose name-classified statistic class is
    /// `Cardinality` / `TopK` / `Frequency`.
    #[serde(default, deserialize_with = "deserialize_sketch_family")]
    pub sketch_family_override: Option<SketchType>,
    /// Optional storage tier hint (e.g. `"warm"`, `"archive"`). Round-trips
    /// silently for now — kept here so the YAML schema matches the
    /// capability_matching agent's expected shape (no rename step at
    /// integration). Not yet read by the planner.
    #[serde(default)]
    pub target_path: Option<String>,
}

fn default_accuracy_sla() -> f64 { 0.01 }
fn default_role() -> String { "agent".into() }

/// Case-insensitive `SketchType` deserialiser. The wire YAML in
/// `deploy/configs/mvp-workload.yaml` spells the variants in mixed case
/// (`KLL`, `HLL`, `CountSketch`, `CountMinSketch`, `DDSketch`) to match
/// the capability-matching agent's schema, while `SketchType`'s
/// `#[serde(rename_all = "lowercase")]` would otherwise reject those
/// strings. Accepts both spellings.
fn deserialize_sketch_family<'de, D>(deserializer: D) -> Result<Option<SketchType>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    let Some(s) = opt else { return Ok(None) };
    let kind = match s.trim().to_ascii_lowercase().as_str() {
        "ddsketch" => SketchType::DDSketch,
        "kll" => SketchType::KLL,
        "hll" => SketchType::HLL,
        "countsketch" => SketchType::CountSketch,
        "countminsketch" | "countmin" | "cms" => SketchType::CountMinSketch,
        other => return Err(serde::de::Error::custom(format!(
            "unknown sketch_family_override `{other}`; expected one of \
             DDSketch / KLL / HLL / CountSketch / CountMinSketch"
        ))),
    };
    Ok(Some(kind))
}

/// Registry of declarative workloads loaded from a YAML file.
#[derive(Debug, Clone)]
pub struct WorkloadRegistry {
    entries: Vec<WorkloadEntry>,
}

impl WorkloadRegistry {
    /// Load from a YAML file. Returns an empty registry on any error.
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_yaml::from_str::<Vec<WorkloadEntry>>(&contents) {
                Ok(entries) => {
                    info!(path, count = entries.len(), "loaded workload registry");
                    Self { entries }
                }
                Err(e) => {
                    warn!(path, error = %e, "invalid workloads YAML; using empty registry");
                    Self { entries: vec![] }
                }
            },
            Err(_) => {
                info!(path, "workloads file not found; using empty registry");
                Self { entries: vec![] }
            }
        }
    }

    /// Create an empty registry (no file).
    pub fn empty() -> Self {
        Self { entries: vec![] }
    }

    /// Create a registry from in-memory entries (useful for tests and
    /// programmatic construction).
    pub fn from_entries(entries: Vec<WorkloadEntry>) -> Self {
        Self { entries }
    }

    /// Returns all workload entries.
    pub fn entries(&self) -> &[WorkloadEntry] {
        &self.entries
    }

    /// Returns workload entries assigned to a given role.
    pub fn for_role(&self, role: &str) -> Vec<&WorkloadEntry> {
        self.entries.iter()
            .filter(|e| e.assign_to_role.eq_ignore_ascii_case(role))
            .collect()
    }

    /// Returns the first workload entry for a given role, if any.
    pub fn first_for_role(&self, role: &str) -> Option<&WorkloadEntry> {
        self.entries.iter()
            .find(|e| e.assign_to_role.eq_ignore_ascii_case(role))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_empty_on_missing_file() {
        let reg = WorkloadRegistry::load("/nonexistent/workloads.yaml");
        assert!(reg.entries().is_empty());
    }

    #[test]
    fn empty_registry() {
        let reg = WorkloadRegistry::empty();
        assert!(reg.entries().is_empty());
        assert!(reg.first_for_role("agent").is_none());
    }

    #[test]
    fn deserialize_entries() {
        let yaml = r#"
- metric_name: latency
  query_string: "histogram_quantile(0.99, rate(http_duration_bucket[5m]))"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: error_count
  accuracy_sla: 0.05
  assign_to_role: backend
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].metric_name, "latency");
        assert_eq!(entries[0].assign_to_role, "agent");
        assert!(entries[0].query_string.is_some());
        assert_eq!(entries[1].metric_name, "error_count");
        assert!(entries[1].query_string.is_none());
    }

    #[test]
    fn for_role_filters_correctly() {
        let reg = WorkloadRegistry {
            entries: vec![
                WorkloadEntry {
                    metric_name: "a".into(),
                    query_string: None,
                    accuracy_sla: 0.01,
                    assign_to_role: "agent".into(),
                    sketch_family_override: None,
                    target_path: None,
                },
                WorkloadEntry {
                    metric_name: "b".into(),
                    query_string: None,
                    accuracy_sla: 0.05,
                    assign_to_role: "backend".into(),
                    sketch_family_override: None,
                    target_path: None,
                },
                WorkloadEntry {
                    metric_name: "c".into(),
                    query_string: None,
                    accuracy_sla: 0.02,
                    assign_to_role: "agent".into(),
                    sketch_family_override: None,
                    target_path: None,
                },
            ],
        };
        assert_eq!(reg.for_role("agent").len(), 2);
        assert_eq!(reg.for_role("backend").len(), 1);
        assert_eq!(reg.first_for_role("agent").unwrap().metric_name, "a");
    }

    #[test]
    fn deserialize_sketch_family_override_mixed_case() {
        // The live wire YAML in `deploy/configs/mvp-workload.yaml` spells
        // the override values in mixed case (KLL / HLL / CountSketch /
        // CountMinSketch). Verify deserialization picks them up — without
        // this, MVP §46 entries 5–8 silently drop their family override
        // (the original stitching-gap symptom).
        let yaml = r#"
- metric_name: a
  sketch_family_override: KLL
- metric_name: b
  sketch_family_override: HLL
- metric_name: c
  sketch_family_override: CountSketch
- metric_name: d
  sketch_family_override: CountMinSketch
- metric_name: e
  sketch_family_override: DDSketch
- metric_name: f
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0].sketch_family_override, Some(SketchType::KLL));
        assert_eq!(entries[1].sketch_family_override, Some(SketchType::HLL));
        assert_eq!(entries[2].sketch_family_override, Some(SketchType::CountSketch));
        assert_eq!(entries[3].sketch_family_override, Some(SketchType::CountMinSketch));
        assert_eq!(entries[4].sketch_family_override, Some(SketchType::DDSketch));
        assert_eq!(entries[5].sketch_family_override, None);
    }

    #[test]
    fn deserialize_sketch_family_override_lowercase_aliases() {
        // Lowercase / kebab-case spellings also accepted, plus the two
        // CMS aliases (`countmin`, `cms`).
        let yaml = r#"
- metric_name: a
  sketch_family_override: ddsketch
- metric_name: b
  sketch_family_override: countmin
- metric_name: c
  sketch_family_override: cms
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries[0].sketch_family_override, Some(SketchType::DDSketch));
        assert_eq!(entries[1].sketch_family_override, Some(SketchType::CountMinSketch));
        assert_eq!(entries[2].sketch_family_override, Some(SketchType::CountMinSketch));
    }

    #[test]
    fn live_mvp_workload_yaml_loads_with_overrides() {
        // Smoke-test the live deploy file. Confirms entries 5–8 carry
        // their `sketch_family_override` after deserialization (the
        // original stitching gap was this field being silently ignored
        // by `serde`'s unknown-field default behaviour).
        use std::path::PathBuf;
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("deploy/configs/mvp-workload.yaml");
        if !path.exists() {
            // Live file not in this checkout; skip silently.
            return;
        }
        let registry = WorkloadRegistry::load(path.to_str().unwrap());
        let by_name: std::collections::HashMap<&str, &WorkloadEntry> =
            registry.entries().iter().map(|e| (e.metric_name.as_str(), e)).collect();

        assert_eq!(
            by_name.get("request_size_bytes")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::KLL),
            "request_size_bytes must carry KLL override",
        );
        assert_eq!(
            by_name.get("unique_users_per_min")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::HLL),
            "unique_users_per_min must carry HLL override",
        );
        assert_eq!(
            by_name.get("top_endpoint_qps")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::CountSketch),
            "top_endpoint_qps must carry CountSketch override",
        );
        assert_eq!(
            by_name.get("endpoint_request_freq")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::CountMinSketch),
            "endpoint_request_freq must carry CountMinSketch override",
        );
    }
}
