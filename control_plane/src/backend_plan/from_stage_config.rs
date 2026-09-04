//! Build a [`BackendPlan`] from the L5 emitter's [`BackendStageConfig`] —
//! the same input `crate::emit::stage_config::emit_backend_streaming_config_json`
//! consumes to produce the legacy JSON wire format. This is the
//! `BackendPlan`-side sibling of that function; see this crate's design
//! doc (`control_plane/docs/design-backend-plan-wire-format.md`) for why
//! both exist side by side (dual-push, see `backend_client.rs`).
//!
//! **Fingerprint parity is the load-bearing invariant here.** A
//! `Materialization`'s `fingerprint` must equal what `data_plane`
//! independently computes for the equivalent policy today (via
//! `AggregationConfig::from_yaml_data` + `PolicyFingerprint::from_config`
//! on the legacy JSON/YAML `StreamingConfig` push) — `SketchStore` sid
//! registration and any future cross-reference between the two wire
//! formats depend on the two identity spaces staying unified. Rather
//! than re-deriving the field mapping a second time (real drift risk),
//! this module reuses `build_backend_aggregation_json` (the exact same
//! JSON `AggregationConfig::from_yaml_data` parses on the receiving end)
//! and computes the fingerprint from *that*.

use std::collections::HashMap;

use anyhow::{Context, Result};
use asap_types::{MonitorSpec, PolicyFingerprint, PrecomputeMaterialization, QueryLanguage};

use crate::emit::monitor::{agg_id_for_metric, MonitorIntent};
use crate::emit::stage_config::build_backend_aggregation_json;
use crate::physical::colored_dag::emitter::{BackendAggregation, BackendStageConfig};
use crate::physical::runtime_capability::{Capability, SketchAlgorithm};
use asap_types::enums::WindowKind;
use planner_types::pre_asap::{ColumnRef, Source};

use super::{BackendPlan, Materialization, RoutingEntry, StorageBackend, WindowSpec};

/// Build a `BackendPlan` from a planning cycle's `BackendStageConfig` +
/// declared CDM monitors. `plan_id`/`generated_at_unix_ms` are
/// observability-only (see `BackendPlan`'s own doc) — callers typically
/// reuse whatever counter/clock they already thread through the legacy
/// `StreamingConfig` emit path.
pub fn from_stage_config(
    cfg: &BackendStageConfig,
    monitors: &[MonitorIntent],
    plan_id: u64,
    generated_at_unix_ms: u64,
) -> Result<BackendPlan> {
    let mut materializations = HashMap::with_capacity(cfg.aggregations.len());
    let mut fingerprint_by_agg_id: HashMap<&str, PolicyFingerprint> =
        HashMap::with_capacity(cfg.aggregations.len());

    for agg in &cfg.aggregations {
        let fingerprint = aggregation_config_for_materialization(agg)
            .with_context(|| format!("aggregation_id {:?}", agg.aggregation_id))?;
        let fingerprint = fingerprint.policy_fingerprint();
        fingerprint_by_agg_id.insert(agg.aggregation_id.as_str(), fingerprint);

        let family = agg.family.clone();

        materializations.insert(
            fingerprint,
            Materialization {
                fingerprint,
                source: Source::TimeSeries {
                    metric: agg.metric_name.clone(),
                },
                window: WindowSpec {
                    kind: WindowKind::Tumbling,
                    size_ms: agg.window_secs.saturating_mul(1000),
                    slide_ms: None,
                },
                group_by: agg.grouping.clone(),
                rollup: Vec::new(),
                spatial_filter: asap_types::utils::normalize_spatial_filter(&agg.spatial_filter),
                family,
                col: ColumnRef::SampleValue,
                retention: None,
                lifecycle: None,
            },
        );
    }

    // Exact aggregates are already finalized by their accumulator and do not
    // have a SketchQuery readout node. Their warm route therefore comes from
    // the physical aggregation itself; approximate routes remain readout-
    // driven below.
    let mut routing = cfg
        .aggregations
        .iter()
        .filter_map(|agg| {
            let planner_types::post_asap::SummaryFamilyType::ExactAggregate(kind, _) = &agg.family
            else {
                return None;
            };
            let fingerprint = *fingerprint_by_agg_id.get(agg.aggregation_id.as_str())?;
            let agg_type = match kind {
                planner_types::post_asap::ExactKind::Sum
                | planner_types::post_asap::ExactKind::Count => asap_types::AggregationType::Sum,
                planner_types::post_asap::ExactKind::MinMax => asap_types::AggregationType::MinMax,
                planner_types::post_asap::ExactKind::Increase
                | planner_types::post_asap::ExactKind::Rate => {
                    asap_types::AggregationType::Increase
                }
            };
            Some(RoutingEntry {
                satisfies: Capability::ExactAgg(agg_type),
                materialization: fingerprint,
                storage_backend: StorageBackend::SketchStore,
            })
        })
        .collect::<Vec<_>>();
    routing.reserve(cfg.readouts.len());
    for readout in &cfg.readouts {
        let Some(&fingerprint) = fingerprint_by_agg_id.get(readout.aggregation_id.as_str()) else {
            // Orphan readout (no matching aggregation in this cycle's
            // config) — nothing to route. Same "tolerate, don't error"
            // stance the legacy JSON emitter takes toward its own
            // readouts list.
            continue;
        };
        let Some(agg) = cfg
            .aggregations
            .iter()
            .find(|a| a.aggregation_id == readout.aggregation_id)
        else {
            continue;
        };
        let satisfies = capability_for_readout(agg, &readout.op)?;
        let route = RoutingEntry {
            satisfies,
            materialization: fingerprint,
            storage_backend: StorageBackend::SketchStore,
        };
        if !routing.contains(&route) {
            routing.push(route);
        }
    }

    let plan_monitors = monitors
        .iter()
        .map(|m| MonitorSpec {
            agg_id: agg_id_for_metric(&m.metric),
            functional: m.functional.as_str().to_string(),
            key: m.key.clone(),
            tau: m.tau,
            epsilon: m.epsilon,
            window_ms: m.window_ms,
            d: 0,
            w: 0,
            mode: String::new(),
        })
        .collect();

    Ok(BackendPlan {
        plan_id,
        generated_at_unix_ms,
        plan_version: 1,
        activation_unix_ms: generated_at_unix_ms,
        expiry_unix_ms: None,
        backend_compat: super::BACKEND_COMPAT.into(),
        materializations,
        routing,
        monitors: plan_monitors,
    })
}

/// Derive the fingerprint data_plane will independently compute for this
/// aggregation. Round-trips through `build_backend_aggregation_json` +
/// `AggregationConfig::from_yaml_data` — the exact same JSON shape and
/// parser the real `POST /api/v1/streaming-config` handler uses (which
/// parses its body as YAML regardless of declared content-type, since
/// JSON is valid YAML) — rather than re-deriving the field mapping here.
pub fn aggregation_config_for_materialization(
    agg: &BackendAggregation,
) -> Result<PrecomputeMaterialization> {
    let json = build_backend_aggregation_json(agg);
    let text = serde_json::to_string(&json).context("serialize synthesized aggregation JSON")?;
    let yaml_value: serde_yaml::Value =
        serde_yaml::from_str(&text).context("parse synthesized aggregation JSON as YAML")?;
    PrecomputeMaterialization::from_yaml_data(&yaml_value, None, QueryLanguage::promql)
        .context("build AggregationConfig from synthesized aggregation JSON")
}

/// Capability this readout satisfies, given the aggregation it reads
/// from. Exact-agg overrides always report `Capability::ExactAgg`
/// (mirrors the wire's `aggregationType` bypass — see
/// `BackendAggregation::agg_type_override`'s doc); otherwise derive from
/// the sketch family + readout op, matching
/// `physical::runtime_capability::Capability`'s variant-per-query-shape
/// design.
fn capability_for_readout(
    agg: &BackendAggregation,
    op: &planner_types::post_asap::SketchQuery,
) -> Result<Capability> {
    use asap_types::AggregationType;
    use planner_types::post_asap::SketchQuery;

    match &agg.family {
        planner_types::post_asap::SummaryFamilyType::ExactAggregate(kind, _) => {
            let agg_type = match kind {
                planner_types::post_asap::ExactKind::Sum => AggregationType::Sum,
                // The backend implements exact count with its sum-as-count
                // accumulator; `ExactKind::Count` remains the canonical
                // planner identity at the domain boundary.
                planner_types::post_asap::ExactKind::Count => AggregationType::Sum,
                planner_types::post_asap::ExactKind::MinMax => AggregationType::MinMax,
                planner_types::post_asap::ExactKind::Increase
                | planner_types::post_asap::ExactKind::Rate => AggregationType::Increase,
            };
            Ok(Capability::ExactAgg(agg_type))
        }
        planner_types::post_asap::SummaryFamilyType::Sketch(kind, _) => {
            let handle = sketch_algorithm_handle(kind.algorithm())?;
            Ok(match op {
                SketchQuery::Quantile { .. } => Capability::QuantileApprox(Some(handle)),
                SketchQuery::Cardinality => Capability::CardinalityApprox,
                SketchQuery::PointCount { .. } => Capability::FrequencyEstimate(Some(handle)),
                SketchQuery::TopK { .. } => Capability::FrequencyTopk(Some(handle)),
            })
        }
        other => anyhow::bail!("unsupported backend summary family {other:?}"),
    }
}

/// Validate that a Planner `SketchAlgorithm` is implemented by this runtime.
fn sketch_algorithm_handle(
    kind: &planner_types::post_asap::SketchAlgorithm,
) -> Result<SketchAlgorithm> {
    use planner_types::post_asap::SketchAlgorithm;
    Ok(match kind {
        SketchAlgorithm::DDSketch => SketchAlgorithm::DDSketch,
        SketchAlgorithm::Kll => SketchAlgorithm::Kll,
        SketchAlgorithm::Hll => SketchAlgorithm::Hll,
        SketchAlgorithm::CountSketch => SketchAlgorithm::CountSketch,
        SketchAlgorithm::Cms => SketchAlgorithm::Cms,
        SketchAlgorithm::CmsWithHeap => SketchAlgorithm::CmsWithHeap,
        SketchAlgorithm::CountSketchWithHeap => SketchAlgorithm::CountSketchWithHeap,
        other => anyhow::bail!("SketchAlgorithm {other:?} is not implemented by this runtime"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::colored_dag::emitter::{AggregationInput, BackendReadout};
    use asap_types::{AggregationType, KeyByLabelNames, WindowKind as AsapWindowKind};
    use planner_types::post_asap::{
        GroupingStrategy, SketchAlgorithm, SketchKind, SketchParams, SketchQuery, SummaryFamilyType,
    };
    use std::collections::HashMap as StdHashMap;

    fn agg(
        aggregation_id: &str,
        metric_name: &str,
        algorithm: SketchAlgorithm,
        sketch_params: SketchParams,
        grouping: Vec<String>,
    ) -> BackendAggregation {
        BackendAggregation {
            aggregation_id: aggregation_id.to_string(),
            metric_name: metric_name.to_string(),
            family: SummaryFamilyType::Sketch(
                SketchKind::new(algorithm, sketch_params),
                GroupingStrategy::PerSubpopulationInstance,
            ),
            window_secs: 60,
            spatial_filter: String::new(),
            grouping,
            item_label: None,
            topk_weight: None,
            aggregation_input: AggregationInput::SketchEnvelope,
        }
    }

    /// Independently construct the `AggregationConfig` a hand-written
    /// (non-JSON-round-trip) reader would build for this fixture, so the
    /// parity test doesn't just check the implementation against itself.
    fn hand_built_config(agg: &BackendAggregation) -> PrecomputeMaterialization {
        let (kind, params) = match &agg.family {
            SummaryFamilyType::Sketch(kind, _) => (kind.algorithm(), kind.params()),
            other => unreachable!("fixture only uses sketches, got {other:?}"),
        };
        let parameters: StdHashMap<String, serde_json::Value> = match params {
            SketchParams::DDSketch { alpha } => {
                StdHashMap::from([("alpha".to_string(), serde_json::json!(alpha))])
            }
            SketchParams::Hll { precision } => {
                StdHashMap::from([("precision".to_string(), serde_json::json!(precision))])
            }
            SketchParams::CountSketchWithHeap { width, depth, .. } => StdHashMap::from([
                ("w".to_string(), serde_json::json!(width)),
                ("d".to_string(), serde_json::json!(depth)),
                ("with_heap".to_string(), serde_json::json!(true)),
            ]),
            SketchParams::Cms { width, depth } => StdHashMap::from([
                ("w".to_string(), serde_json::json!(width)),
                ("d".to_string(), serde_json::json!(depth)),
            ]),
            other => unreachable!("fixture doesn't exercise {other:?}"),
        };
        PrecomputeMaterialization::new(
            match kind {
                SketchAlgorithm::DDSketch => AggregationType::DDSketch,
                SketchAlgorithm::Kll => AggregationType::DatasketchesKLL,
                SketchAlgorithm::Hll => AggregationType::HLL,
                SketchAlgorithm::Cms => AggregationType::CountMinSketch,
                SketchAlgorithm::CmsWithHeap => AggregationType::CountMinSketchWithHeap,
                SketchAlgorithm::CountSketch => AggregationType::CountSketch,
                SketchAlgorithm::CountSketchWithHeap => AggregationType::CountSketchWithHeap,
                _ => unreachable!("fixture only uses sketch-typed kinds"),
            },
            String::new(),
            parameters,
            KeyByLabelNames::new(agg.grouping.clone()),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            agg.window_secs,
            agg.window_secs,
            AsapWindowKind::Tumbling,
            agg.spatial_filter.clone(),
            agg.metric_name.clone(),
            None,
            None,
            None,
        )
    }

    fn sample_cfg() -> BackendStageConfig {
        BackendStageConfig {
            aggregations: vec![
                agg(
                    "agg0",
                    "http_latency_ms",
                    SketchAlgorithm::DDSketch,
                    SketchParams::DDSketch { alpha: 0.01 },
                    Vec::new(),
                ),
                agg(
                    "agg1",
                    "http_requests_total",
                    SketchAlgorithm::Hll,
                    SketchParams::Hll { precision: 14 },
                    vec!["zone".to_string()],
                ),
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: SketchQuery::Quantile { q: 0.99 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: SketchQuery::Cardinality,
                },
            ],
        }
    }

    #[test]
    fn every_aggregation_produces_exactly_one_materialization() {
        let cfg = sample_cfg();
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");
        assert_eq!(plan.materializations.len(), cfg.aggregations.len());
    }

    #[test]
    fn fingerprint_matches_hand_built_aggregation_config() {
        let cfg = sample_cfg();
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");

        for agg in &cfg.aggregations {
            let expected = hand_built_config(agg).policy_fingerprint();
            let m = plan.materializations.get(&expected).unwrap_or_else(|| {
                panic!("no materialization for expected fingerprint of {agg:?}")
            });
            assert_eq!(m.fingerprint, expected);
        }
    }

    #[test]
    fn sketch_kind_and_params_pass_through_unchanged() {
        let cfg = sample_cfg();
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");
        let ddsketch = plan
            .materializations
            .values()
            .find(|m| matches!(m.source, Source::TimeSeries { ref metric } if metric == "http_latency_ms"))
            .expect("ddsketch materialization present");
        assert!(matches!(
            &ddsketch.family,
            planner_types::post_asap::SummaryFamilyType::Sketch(kind, _)
                if kind.algorithm() == &planner_types::post_asap::SketchAlgorithm::DDSketch
                    && kind.params() == &planner_types::post_asap::SketchParams::DDSketch { alpha: 0.01 }
        ));

        let hll = plan
            .materializations
            .values()
            .find(|m| matches!(m.source, Source::TimeSeries { ref metric } if metric == "http_requests_total"))
            .expect("hll materialization present");
        assert!(matches!(
            &hll.family,
            planner_types::post_asap::SummaryFamilyType::Sketch(kind, _)
                if kind.algorithm() == &planner_types::post_asap::SketchAlgorithm::Hll
                    && kind.params() == &planner_types::post_asap::SketchParams::Hll { precision: 14 }
        ));
        assert_eq!(hll.group_by, vec!["zone".to_string()]);
    }

    #[test]
    fn readouts_map_to_routing_entries_pointing_at_the_right_fingerprint() {
        let cfg = sample_cfg();
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");
        assert_eq!(plan.routing.len(), 2);

        let ddsketch_fp = plan
            .materializations
            .iter()
            .find(|(_, m)| matches!(m.source, Source::TimeSeries { ref metric } if metric == "http_latency_ms"))
            .map(|(fp, _)| *fp)
            .expect("ddsketch fingerprint");
        let quantile_entry = plan
            .routing
            .iter()
            .find(|r| r.materialization == ddsketch_fp)
            .expect("routing entry for ddsketch");
        assert_eq!(
            quantile_entry.satisfies,
            Capability::QuantileApprox(Some(SketchAlgorithm::DDSketch))
        );

        let hll_fp = plan
            .materializations
            .iter()
            .find(|(_, m)| matches!(m.source, Source::TimeSeries { ref metric } if metric == "http_requests_total"))
            .map(|(fp, _)| *fp)
            .expect("hll fingerprint");
        let cardinality_entry = plan
            .routing
            .iter()
            .find(|r| r.materialization == hll_fp)
            .expect("routing entry for hll");
        assert_eq!(cardinality_entry.satisfies, Capability::CardinalityApprox);
    }

    #[test]
    fn topk_and_point_count_readouts_map_to_frequency_capabilities() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                agg(
                    "agg0",
                    "endpoint_count",
                    SketchAlgorithm::CountSketchWithHeap,
                    SketchParams::CountSketchWithHeap {
                        width: 2048,
                        depth: 5,
                        heap_size: 10,
                    },
                    Vec::new(),
                ),
                agg(
                    "agg1",
                    "endpoint_hits",
                    SketchAlgorithm::Cms,
                    SketchParams::Cms {
                        width: 4096,
                        depth: 4,
                    },
                    Vec::new(),
                ),
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: SketchQuery::TopK {
                        k: 10,
                        weight: planner_types::post_asap::TopKWeight::Value,
                    },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: SketchQuery::PointCount {
                        key: ColumnRef::Named("user_42".into()),
                        value: None,
                    },
                },
            ],
        };
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");
        let topk_entry = plan.routing.iter().find(|r| {
            r.satisfies == Capability::FrequencyTopk(Some(SketchAlgorithm::CountSketchWithHeap))
        });
        assert!(
            topk_entry.is_some(),
            "expected a FrequencyTopk routing entry: {:?}",
            plan.routing
        );

        let freq_entry = plan
            .routing
            .iter()
            .find(|r| r.satisfies == Capability::FrequencyEstimate(Some(SketchAlgorithm::Cms)));
        assert!(
            freq_entry.is_some(),
            "expected a FrequencyEstimate routing entry: {:?}",
            plan.routing
        );
    }

    #[test]
    fn exact_agg_override_reports_exact_agg_capability() {
        let a = BackendAggregation {
            aggregation_id: "agg0".into(),
            metric_name: "http_requests_total".into(),
            family: SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Sum,
                planner_types::post_asap::ExactParams::Sum,
            ),
            window_secs: 60,
            spatial_filter: String::new(),
            grouping: vec!["zone".to_string()],
            item_label: None,
            topk_weight: None,
            aggregation_input: AggregationInput::Raw,
        };
        let cfg = BackendStageConfig {
            aggregations: vec![a],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".into(),
                op: SketchQuery::Cardinality, // op is irrelevant for override rows
            }],
        };
        let plan = from_stage_config(&cfg, &[], 1, 0).expect("build plan");
        let (_, m) = plan
            .materializations
            .iter()
            .next()
            .expect("one materialization");
        assert!(matches!(
            m.family,
            planner_types::post_asap::SummaryFamilyType::ExactAggregate(
                planner_types::post_asap::ExactKind::Sum,
                planner_types::post_asap::ExactParams::Sum
            )
        ));

        let entry = &plan.routing[0];
        assert_eq!(entry.satisfies, Capability::ExactAgg(AggregationType::Sum));
    }
}
