use crate::types::*;
use anyhow::Context;
use serde_json::json;

/// Generates an OTel collector YAML string for the backend merge collector.
///
/// **SP-9**: when `staged.has_dedup` is true a `dedup` processor is inserted
/// before the merge processor in the pipeline, honouring the `Dedup` node
/// assignment from [`crate::planner::stage_split::split_expr_by_stage`].
pub fn generate_backend_config(
    cfg: &BackendCollectorConfig,
    opamp_endpoint: &str,
) -> anyhow::Result<String> {
    generate_backend_config_staged(cfg, None, opamp_endpoint)
}

/// Extended entry point used by SP-9-aware callers that supply a
/// [`BackendSubPlan`] carrying the dedup flag.
pub fn generate_backend_config_staged(
    cfg: &BackendCollectorConfig,
    staged: Option<&BackendSubPlan>,
    opamp_endpoint: &str,
) -> anyhow::Result<String> {
    let merge_key = format!("{}_merge", cfg.merge_sketch_type);
    let has_dedup = staged.map(|s| s.has_dedup).unwrap_or(false);

    // Build the processors map and pipeline processor list.
    let mut processors = serde_json::Map::new();
    let mut pipeline_processors: Vec<serde_json::Value> = vec![];

    if has_dedup {
        processors.insert("dedup".into(), json!({ "mode": "dedup" }));
        pipeline_processors.push(json!("dedup"));
    }

    processors.insert(
        merge_key.clone(),
        json!({ "mode": "merge", "group_by": cfg.group_by }),
    );
    pipeline_processors.push(json!(&merge_key));

    let doc = serde_yaml::to_value(&json!({
        "extensions": {
            "opamp": { "server": { "ws": { "endpoint": opamp_endpoint } } }
        },
        "processors": processors,
        "service": {
            "extensions": ["opamp"],
            "pipelines": {
                "metrics": { "processors": pipeline_processors }
            }
        }
    }))
    .context("build backend doc")?;

    serde_yaml::to_string(&doc).context("serialize backend config")
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
        let yaml = generate_backend_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
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
        let yaml = generate_backend_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("HLL_merge"), "{yaml}");
    }

    #[test]
    fn contains_opamp_endpoint() {
        let ep = "ws://custom-ctrl:9000/v1/opamp";
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::KLL,
            group_by: vec![],
        };
        let yaml = generate_backend_config(&cfg, ep).unwrap();
        assert!(yaml.contains(ep), "YAML should contain endpoint\n{yaml}");
    }

    #[test]
    fn dedup_processor_emitted_when_staged_has_dedup() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::HLL,
            group_by: vec!["user_id".into()],
        };
        let staged = BackendSubPlan {
            has_dedup: true,
            has_merge: true,
            group_by: vec![],
        };
        let yaml =
            generate_backend_config_staged(&cfg, Some(&staged), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("dedup:"),
            "YAML should contain dedup processor\n{yaml}"
        );
        // dedup must appear before merge in the pipeline list
        let dedup_pos = yaml.find("- dedup").expect("missing dedup in pipeline");
        let merge_pos = yaml.find("- HLL_merge").expect("missing merge in pipeline");
        assert!(dedup_pos < merge_pos, "dedup must precede merge\n{yaml}");
    }

    #[test]
    fn no_dedup_when_staged_has_dedup_false() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::DDSketch,
            group_by: vec![],
        };
        let staged = BackendSubPlan {
            has_dedup: false,
            has_merge: true,
            group_by: vec![],
        };
        let yaml =
            generate_backend_config_staged(&cfg, Some(&staged), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("dedup"),
            "YAML must not contain dedup\n{yaml}"
        );
    }
}
