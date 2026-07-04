use anyhow::Context;
use serde::Serialize;
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;

use crate::pipeline::format_duration;
use crate::types::*;

// ── YAML structural types ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct CollectorYaml {
    extensions: HashMap<String, Value>,
    receivers: HashMap<String, Value>,
    processors: HashMap<String, Value>,
    exporters: HashMap<String, Value>,
    service: ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    extensions: Vec<String>,
    pipelines: HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers: Vec<String>,
    processors: Vec<String>,
    exporters: Vec<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Generates an OTel collector YAML string for an agent collector from a plan.
///
/// The `opamp_endpoint` parameter specifies the OpAMP WebSocket endpoint that
/// the collector should connect to for receiving runtime config updates from the
/// control plane.  An `extensions.opamp` section is included in the generated YAML
/// so the collector can receive pushed configs without a restart.
pub fn generate_agent_collector_config(
    cfg: &AgentCollectorConfig,
    opamp_endpoint: &str,
) -> anyhow::Result<String> {
    let processor_key = cfg.sketch_type.to_string();
    let processor_val = build_processor_block(cfg);

    // Standard OTLP receiver (gRPC + HTTP) with optional series_id registry.
    let mut otlp_map: Mapping = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    ).unwrap();

    otlp_map.insert("enable_series_id".into(), Value::Bool(cfg.enable_series_id));
    if cfg.series_id_ttl_secs > 0 {
        otlp_map.insert(
            "series_id_ttl".into(),
            Value::String(format!("{}s", cfg.series_id_ttl_secs)),
        );
    }
    let otlp_receiver = Value::Mapping(otlp_map);

    // Build the exporter block from `cfg.data_sink`. The planner
    // chooses the sketch + window + projection; *where* the
    // sketched data goes is a deployment-scope concern carried
    // here. Default is `otlp/backend` because the modified-OTLP
    // `Data::Ddsketch` / `KLLSketch` / ... variants only survive
    // an OTLP transport — the legacy `prometheus` exporter is
    // kept only for raw-scalar pipelines.
    let (exporter_key, exporter_val) = build_exporter_block(&cfg.data_sink);

    // OpAMP extension — allows the control plane to push config updates at runtime.
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .unwrap();

    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors: [(processor_key.clone(), processor_val)].into(),
        exporters: [(exporter_key.clone(), exporter_val)].into(),
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: vec![processor_key],
                    exporters: vec![exporter_key],
                },
            )]
            .into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize agent config")
}

/// Maps the planner's `AgentDataSink` choice to a (component_id,
/// component_yaml) pair. The component_id is what goes into the
/// `exporters:` map AND the pipeline's `exporters:` list — both
/// references must agree, so it's returned alongside the YAML
/// block.
fn build_exporter_block(sink: &AgentDataSink) -> (String, Value) {
    match sink {
        AgentDataSink::Otlp {
            endpoint,
            compression,
        } => {
            let yaml = format!(
                "endpoint: \"{endpoint}\"\ntls:\n  insecure: true\ncompression: {compression}\n"
            );
            (
                "otlp/backend".to_string(),
                serde_yaml::from_str(&yaml).unwrap(),
            )
        }
        AgentDataSink::PrometheusScrape { endpoint } => {
            let yaml = format!("endpoint: \"{endpoint}\"\n");
            (
                "prometheus".to_string(),
                serde_yaml::from_str(&yaml).unwrap(),
            )
        }
    }
}

fn build_processor_block(cfg: &AgentCollectorConfig) -> Value {
    let mut m = Mapping::new();

    m.insert("mode".into(), Value::String(cfg.mode.to_string()));
    m.insert(
        "enable_self_monitoring".into(),
        Value::Bool(cfg.enable_self_monitoring),
    );
    m.insert("transmit_sketch".into(), Value::Bool(cfg.transmit_sketch));

    if cfg.mode == ProcessorMode::Window {
        if let Some(wd) = cfg.window_duration {
            // MVP blocker B4: clamp `window_duration` to [5, 60] so
            // the legacy agent emitter matches the typed L5 emitter's
            // bounds — without this, a `[5m]` workload landing here
            // mints a 300s sketch window whose closed answer never
            // falls inside the user's replay range.
            let clamped = super::stage_config::clamp_window_secs(Some(wd.as_secs()))
                .map(std::time::Duration::from_secs)
                .unwrap_or(wd);
            m.insert(
                "window_duration".into(),
                Value::String(format_duration(clamped)),
            );
        }
    }

    if !cfg.aggregate_by.is_empty() {
        m.insert("aggregate_by".into(), seq_of_strings(&cfg.aggregate_by));
    }
    if !cfg.label_matchers.is_empty() {
        // Go processors expect []LabelMatcher{Key, Value}, not flat strings.
        let matchers: Vec<Value> = cfg
            .label_matchers
            .iter()
            .filter_map(|s| {
                let (k, v) = s.split_once('=')?;
                let mut map = serde_yaml::Mapping::new();
                map.insert("key".into(), Value::String(k.to_string()));
                map.insert("value".into(), Value::String(v.to_string()));
                Some(Value::Mapping(map))
            })
            .collect();
        if !matchers.is_empty() {
            m.insert("label_matchers".into(), Value::Sequence(matchers));
        }
    }

    // Delta transmission: only emit fields each processor's Config actually defines.
    // KLL rejects delta_transmission at Validate(); HLL has no delta_threshold key.
    if cfg.delta_transmission && cfg.sketch_type != SketchType::KLL {
        m.insert("delta_transmission".into(), Value::Bool(true));
        if matches!(
            cfg.sketch_type,
            SketchType::DDSketch | SketchType::CountSketch | SketchType::CountMinSketch
        ) {
            m.insert(
                "delta_threshold".into(),
                Value::Number(cfg.delta_threshold.into()),
            );
        }
        // GOS relative delta gating (Count-Sketch only today — the edge's
        // applyGosMode structural assert matches CountSketchWrapper): the edge
        // replaces the fixed threshold with the norm-adaptive GOS one.
        if let Some(g) = &cfg.gos {
            if cfg.sketch_type == SketchType::CountSketch {
                m.insert("gos_delta_epsilon".into(), Value::Number(g.epsilon.into()));
                m.insert("gos_sites".into(), Value::Number((g.sites as u64).into()));
                if g.anisotropic {
                    m.insert("gos_anisotropic".into(), Value::Bool(true));
                }
            }
        }
    }

    // Sketch-type-specific params.
    match &cfg.sketch_params {
        SketchParams::DDSketch {
            relative_accuracy,
            quantiles,
        } => {
            m.insert(
                "relative_accuracy".into(),
                Value::Number((*relative_accuracy).into()),
            );
            if !quantiles.is_empty() {
                m.insert(
                    "quantiles".into(),
                    Value::Sequence(
                        quantiles
                            .iter()
                            .map(|q| Value::Number((*q).into()))
                            .collect(),
                    ),
                );
            }
        }
        SketchParams::KLL { k, quantiles } => {
            m.insert("k".into(), Value::Number((*k as u64).into()));
            if !quantiles.is_empty() {
                m.insert(
                    "quantiles".into(),
                    Value::Sequence(
                        quantiles
                            .iter()
                            .map(|q| Value::Number((*q).into()))
                            .collect(),
                    ),
                );
            }
        }
        SketchParams::HLL { .. } => {
            // hllprocessor uses a fixed HLL precision in code; Config has no precision field.
        }
        SketchParams::CountSketch { epsilon, delta } => {
            m.insert("epsilon".into(), Value::Number((*epsilon).into()));
            m.insert("delta".into(), Value::Number((*delta).into()));
        }
        SketchParams::CountMinSketch {
            rows,
            cols,
            metric_name,
        } => {
            m.insert("metric_name".into(), Value::String(metric_name.clone()));
            m.insert("rows".into(), Value::Number((*rows as u64).into()));
            m.insert("columns".into(), Value::Number((*cols as u64).into()));
        }
    }

    Value::Mapping(m)
}

fn seq_of_strings(v: &[String]) -> Value {
    Value::Sequence(v.iter().map(|s| Value::String(s.clone())).collect())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ddsketch_cfg() -> AgentCollectorConfig {
        AgentCollectorConfig {
            output_mode: OutputMode::Sketch,
            sketch_type: SketchType::DDSketch,
            sketch_params: SketchParams::DDSketch {
                relative_accuracy: 0.01,
                quantiles: vec![0.5, 0.9, 0.99],
            },
            aggregate_by: vec!["host.name".into(), "service".into()],
            label_matchers: vec!["env=prod".into()],
            window_duration: Some(Duration::from_secs(300)),
            mode: ProcessorMode::Window,
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: true,
            series_id_ttl_secs: 0,
            // Pre-existing fixture tests (`contains_prometheus_exporter`,
            // `pipeline_has_receivers_and_exporters`) assert the legacy
            // prometheus exporter on :8889 — keep the test semantics by
            // pinning the sink, not by changing the default.
            data_sink: AgentDataSink::PrometheusScrape {
                endpoint: "0.0.0.0:8889".to_string(),
            },
        }
    }

    #[test]
    fn contains_processor_key() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("ddsketch:"),
            "YAML should contain 'ddsketch:'\n{yaml}"
        );
        assert!(
            yaml.contains("enable_self_monitoring: true"),
            "YAML should carry enable_self_monitoring\n{yaml}"
        );
    }

    #[test]
    fn contains_opamp_extension() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("opamp"),
            "YAML should include the opamp extension\n{yaml}"
        );
        assert!(
            yaml.contains("ws://ctrl:4320/v1/opamp"),
            "YAML should contain the opamp endpoint\n{yaml}"
        );
    }

    #[test]
    fn contains_window_duration() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        // MVP blocker B4: the fixture's 5m window clamps to 60s
        // (`MAX_WINDOW_SECS`). Assert on the clamped form — a window
        // larger than 60s would put the sketch close outside any
        // sensible replay range. Pre-B4 this test asserted "5m".
        assert!(
            yaml.contains("window_duration: 1m") || yaml.contains("window_duration: 60s"),
            "YAML should contain clamped window_duration (1m / 60s)\n{yaml}"
        );
    }

    #[test]
    fn clamps_oversize_window_to_max() {
        let mut cfg = ddsketch_cfg();
        cfg.window_duration = Some(Duration::from_secs(3600)); // 1h
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("window_duration: 1h"),
            "1h window must clamp to MAX_WINDOW_SECS, not pass through\n{yaml}"
        );
        assert!(
            yaml.contains("window_duration: 1m") || yaml.contains("window_duration: 60s"),
            "clamped window must be 60s\n{yaml}"
        );
    }

    #[test]
    fn clamps_undersize_window_to_min() {
        let mut cfg = ddsketch_cfg();
        cfg.window_duration = Some(Duration::from_secs(1)); // 1s
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("window_duration: 5s"),
            "1s window must clamp UP to MIN_WINDOW_SECS=5s\n{yaml}"
        );
    }

    #[test]
    fn preserves_window_inside_clamp_range() {
        let mut cfg = ddsketch_cfg();
        cfg.window_duration = Some(Duration::from_secs(30));
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("window_duration: 30s"),
            "30s window is inside [5, 60] and must pass through verbatim\n{yaml}"
        );
    }

    #[test]
    fn batch_mode_omits_window_duration() {
        let mut cfg = ddsketch_cfg();
        cfg.mode = ProcessorMode::Batch;
        cfg.window_duration = None;
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("window_duration"),
            "batch mode should not have window_duration\n{yaml}"
        );
    }

    #[test]
    fn contains_aggregate_by() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("host.name"),
            "YAML should contain aggregate_by labels\n{yaml}"
        );
    }

    #[test]
    fn hll_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::HLL,
            sketch_params: SketchParams::HLL { precision: 14 },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: true,
            series_id_ttl_secs: 0,
            data_sink: AgentDataSink::default(),
        };
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("HLL:"),
            "YAML should contain HLL processor key\n{yaml}"
        );
        assert!(
            yaml.contains("- HLL"),
            "pipeline should reference HLL processor\n{yaml}"
        );
        assert!(
            !yaml.contains("precision"),
            "HLL processor YAML must not set precision (not in Config)\n{yaml}"
        );
    }

    #[test]
    fn countminsketch_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::CountMinSketch,
            sketch_params: SketchParams::CountMinSketch {
                rows: 5,
                cols: 2048,
                metric_name: "test_metric".into(),
            },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: true,
            series_id_ttl_secs: 0,
            data_sink: AgentDataSink::default(),
        };
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("countmin:"),
            "YAML should use countmin component id (factory type)\n{yaml}"
        );
    }

    #[test]
    fn contains_otlp_receiver() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("receivers:"),
            "YAML should have receivers section\n{yaml}"
        );
        assert!(
            yaml.contains("otlp:"),
            "YAML should have otlp receiver\n{yaml}"
        );
        assert!(yaml.contains("4317"), "YAML should have gRPC port\n{yaml}");
        assert!(yaml.contains("4318"), "YAML should have HTTP port\n{yaml}");
    }

    #[test]
    fn contains_prometheus_exporter() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("exporters:"),
            "YAML should have exporters section\n{yaml}"
        );
        assert!(
            yaml.contains("prometheus:"),
            "YAML should have prometheus exporter\n{yaml}"
        );
        assert!(
            yaml.contains("8889"),
            "YAML should have prometheus port\n{yaml}"
        );
    }

    #[test]
    fn pipeline_has_receivers_and_exporters() {
        let yaml =
            generate_agent_collector_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        // Ensure the pipeline block references both receiver and exporter keys.
        assert!(
            yaml.contains("- otlp"),
            "pipeline receivers should list otlp\n{yaml}"
        );
        assert!(
            yaml.contains("- prometheus"),
            "pipeline exporters should list prometheus\n{yaml}"
        );
    }

    #[test]
    fn delta_fields_present_when_enabled() {
        let mut cfg = ddsketch_cfg();
        cfg.delta_transmission = true;
        cfg.delta_threshold = 1.0;
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("delta_transmission: true"),
            "YAML should contain delta_transmission: true\n{yaml}"
        );
        assert!(
            yaml.contains("delta_threshold"),
            "YAML should contain delta_threshold\n{yaml}"
        );
    }

    #[test]
    fn delta_fields_absent_when_disabled() {
        let cfg = ddsketch_cfg(); // delta_transmission: false by default
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("delta_transmission"),
            "YAML must not contain delta_transmission when disabled\n{yaml}"
        );
        assert!(
            !yaml.contains("delta_threshold"),
            "YAML must not contain delta_threshold when disabled\n{yaml}"
        );
    }

    #[test]
    fn kll_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::KLL,
            sketch_params: SketchParams::KLL {
                k: 200,
                quantiles: vec![0.5, 0.99],
            },
            mode: ProcessorMode::Window,
            window_duration: Some(std::time::Duration::from_secs(300)),
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: true,
            series_id_ttl_secs: 0,
            data_sink: AgentDataSink::default(),
        };
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("KLL:"), "YAML should contain 'KLL:'\n{yaml}");
        assert!(
            yaml.contains("k:"),
            "YAML should contain 'k:' param\n{yaml}"
        );
        assert!(
            !yaml.contains("ddsketch:"),
            "YAML must not contain wrong processor key\n{yaml}"
        );
    }

    #[test]
    fn countsketch_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::CountSketch,
            sketch_params: SketchParams::CountSketch {
                epsilon: CountSketchDefaults::default().epsilon,
                delta: CountSketchDefaults::default().delta,
            },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            gos: None,
            enable_series_id: true,
            series_id_ttl_secs: 0,
            data_sink: AgentDataSink::default(),
        };
        let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("countsketch:"),
            "YAML should contain 'countsketch:'\n{yaml}"
        );
        assert!(
            !yaml.contains("countminsketch:"),
            "YAML must not contain 'countminsketch:' for CountSketch\n{yaml}"
        );
    }

    /// Verifies that for every sketch type the processor key in the `processors:`
    /// section and the key listed under `service.pipelines.metrics.processors`
    /// are identical.  This guards against the processor map and the pipeline
    /// reference going out of sync.
    #[test]
    fn all_sketch_types_processor_key_matches_pipeline_ref() {
        let cases: &[(&str, SketchType, SketchParams)] = &[
            (
                "ddsketch",
                SketchType::DDSketch,
                SketchParams::DDSketch {
                    relative_accuracy: 0.01,
                    quantiles: vec![0.5],
                },
            ),
            (
                "KLL",
                SketchType::KLL,
                SketchParams::KLL {
                    k: 200,
                    quantiles: vec![0.5],
                },
            ),
            ("HLL", SketchType::HLL, SketchParams::HLL { precision: 14 }),
            (
                "countsketch",
                SketchType::CountSketch,
                SketchParams::CountSketch {
                    epsilon: CountSketchDefaults::default().epsilon,
                    delta: CountSketchDefaults::default().delta,
                },
            ),
            (
                "countmin",
                SketchType::CountMinSketch,
                SketchParams::CountMinSketch {
                    rows: 5,
                    cols: 2048,
                    metric_name: "m".into(),
                },
            ),
        ];

        for (expected_key, sketch_type, sketch_params) in cases {
            let cfg = AgentCollectorConfig {
                sketch_type: sketch_type.clone(),
                sketch_params: sketch_params.clone(),
                mode: ProcessorMode::Batch,
                window_duration: None,
                output_mode: OutputMode::Sketch,
                aggregate_by: vec![],
                label_matchers: vec![],
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: true,
                series_id_ttl_secs: 0,
                data_sink: AgentDataSink::default(),
            };
            let yaml = generate_agent_collector_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();

            // Processor section key present.
            assert!(
                yaml.contains(&format!("{expected_key}:")),
                "sketch_type={expected_key}: YAML missing processor key '{expected_key}:'\n{yaml}"
            );
            // Pipeline processor list references the same key.
            assert!(
                yaml.contains(&format!("- {expected_key}")),
                "sketch_type={expected_key}: pipeline processor list missing '- {expected_key}'\n{yaml}"
            );
            // No other sketch type key should appear as a processor.
            for (other_key, _, _) in cases {
                if other_key == expected_key {
                    continue;
                }
                assert!(
                    !yaml.contains(&format!("{other_key}:")),
                    "sketch_type={expected_key}: YAML must not contain foreign key '{other_key}:'\n{yaml}"
                );
            }
        }
    }
}
