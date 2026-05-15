use crate::types::*;
use anyhow::Context;
use serde_json::json;

/// Generate the OTel collector YAML for the **backend-role collector** —
/// the central collector that merges per-agent partial sketches.
///
/// The pipeline carries a single `{sketch_type}_merge` processor grouped
/// by `cfg.group_by`. (An earlier `_staged` variant added an optional
/// `dedup` processor driven by the legacy `StagedPlan`; that path was
/// retired with the legacy L5 — re-modelling dedup on the typed L5's
/// `BackendStageConfig` is a follow-up if it proves needed.)
pub fn generate_backend_collector_config(
    cfg: &BackendCollectorConfig,
    opamp_endpoint: &str,
) -> anyhow::Result<String> {
    let merge_key = format!("{}_merge", cfg.merge_sketch_type);

    let mut processors = serde_json::Map::new();
    processors.insert(
        merge_key.clone(),
        json!({ "mode": "merge", "group_by": cfg.group_by }),
    );

    let doc = serde_yaml::to_value(&json!({
        "extensions": {
            "opamp": { "server": { "ws": { "endpoint": opamp_endpoint } } }
        },
        "processors": processors,
        "service": {
            "extensions": ["opamp"],
            "pipelines": {
                "metrics": { "processors": [&merge_key] }
            }
        }
    }))
    .context("build backend collector doc")?;

    serde_yaml::to_string(&doc).context("serialize backend collector config")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_merge_key() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::DDSketch,
            group_by: vec!["host.name".into()],
        };
        let yaml = generate_backend_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("ddsketch_merge"),
            "YAML should contain merge key\n{yaml}"
        );
        assert!(
            yaml.contains("host.name"),
            "YAML should contain group_by\n{yaml}"
        );
    }

    #[test]
    fn hll_merge_key() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::HLL,
            group_by: vec![],
        };
        let yaml = generate_backend_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("HLL_merge"), "{yaml}");
    }

    #[test]
    fn contains_opamp_endpoint() {
        let ep = "ws://custom-ctrl:9000/v1/opamp";
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::KLL,
            group_by: vec![],
        };
        let yaml = generate_backend_collector_config(&cfg, ep).unwrap();
        assert!(yaml.contains(ep), "YAML should contain endpoint\n{yaml}");
    }
}
