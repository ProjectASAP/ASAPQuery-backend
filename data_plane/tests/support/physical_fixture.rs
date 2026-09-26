//! Explicit query bindings for transport/decoder fixtures. These tests supply
//! accumulator states; planner selection is covered separately by ERP/process tests.

use control_plane::{physical::compiler::*, query_plan::*};
use data_plane::{
    drivers::query::servers::http::PhysicalPlanInstallRequest,
    storage_engines::types::{ActivePhysicalPlan, BackendStorageRouting, StreamingConfig},
};
use std::{collections::BTreeMap, sync::Arc};

pub fn artifact(config: &StreamingConfig) -> PhysicalPlanInstallRequest {
    artifact_from_materializations(
        config
            .materializations_by_policy_fingerprint
            .values()
            .cloned()
            .collect(),
    )
}

/// Same as [`artifact`], but from materializations the planner produced
/// directly — no legacy `StreamingConfig` document in between.
pub fn artifact_from_materializations(
    mut configs: Vec<asap_types::PrecomputeMaterialization>,
) -> PhysicalPlanInstallRequest {
    let envelope = PlanEnvelope {
        plan_id: 1,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "transport-fixture".into(),
    };
    // Installed physical plans always carry an explicit pane phase. Inputs that
    // predate that contract bind to the Unix epoch grid before deriving catalog
    // identities and bindings.
    for config in &mut configs {
        config.pane_origin_ms.get_or_insert(0);
    }
    let catalog = control_plane::physical::summary_catalog::SummaryCatalog::from_materializations(
        1, 1, &configs,
    )
    .unwrap();
    let mut precompute =
        PrecomputePlan::build(envelope.clone(), configs, &["fixture".into()]).unwrap();
    precompute.summary_catalog = Some(catalog.reference().unwrap());
    let mut transmission = control_plane::physical::compiler::build_transmission_plan(
        envelope,
        &precompute,
        &BTreeMap::new(),
    )
    .unwrap();
    transmission.summary_catalog = Some(catalog.reference().unwrap());
    let mut query_plan = QueryPlan {
        plan_id: 1,
        plan_version: 1,
        clickhouse_context: None,
        selected_dags: Default::default(),
        entries: BTreeMap::new(),
    };
    for config in &precompute.materializations {
        use data_plane::storage_engines::types::AggregationType;
        let metric = &config.metric;
        let mut queries = Vec::new();
        match config.aggregation_type {
            AggregationType::DatasketchesKLL | AggregationType::DDSketch => {
                for (window, ms) in [
                    ("1s", 1_000),
                    ("2s", 2_000),
                    ("3s", 3_000),
                    ("10s", 10_000),
                    ("3m", 180_000),
                ] {
                    for q in [0.5, 0.99] {
                        queries.push((
                            format!("quantile_over_time({q}, {metric}[{window}])"),
                            QueryReadout::Quantile { q },
                            ms,
                            PhysicalGrouping::PerEntity,
                        ));
                    }
                }
            }
            AggregationType::HLL => queries.push((
                format!("count({metric})"),
                QueryReadout::Cardinality,
                2_000,
                PhysicalGrouping::Reduce(vec![]),
            )),
            _ => {
                for seconds in [1, 2, 10] {
                    for value in [None, Some("alpha".to_string())] {
                        let filter = value
                            .as_ref()
                            .map(|v| format!("{{item=\"{v}\"}}"))
                            .unwrap_or_default();
                        queries.push((
                            format!("count_over_time({metric}{filter}[{seconds}s])"),
                            QueryReadout::PointCount {
                                key: if value.is_some() {
                                    planner_types::pre_asap::ColumnRef::Named("item".into())
                                } else {
                                    planner_types::pre_asap::ColumnRef::SampleValue
                                },
                                value,
                            },
                            seconds * 1000,
                            PhysicalGrouping::PerEntity,
                        ));
                    }
                }
            }
        }
        for (query, readout, lookback_ms, output_grouping) in queries {
            let canonical = canonical_promql(&query).unwrap();
            query_plan.entries.insert(
                canonical.clone(),
                QueryPlanEntry {
                    language: asap_types::query_plan::QueryLanguage::PromQl,
                    query_id: canonical.clone(),
                    canonical_query: canonical,
                    fixed_evaluation: None,
                    root: QueryNodeId(1),
                    nodes: BTreeMap::from([
                        (
                            QueryNodeId(0),
                            QueryPlanNode::ReadMaterialization {
                                binding: MaterializationBinding {
                                    full_window_slide_ms: None,
                                    materialization: config.policy_fingerprint().into(),
                                    stored_output_reference:
                                        asap_types::sds::StoredOutputReference::for_definition(
                                            config.policy_fingerprint().into(),
                                        ),
                                    output_grouping,
                                    item_labels: config.aggregated_labels.labels.clone(),
                                    window_ms: config.slide_interval * 1000,
                                    pane_origin_ms: config.pane_origin_ms,
                                    readout_lookback_ms: Some(lookback_ms),
                                },
                            },
                        ),
                        (
                            QueryNodeId(1),
                            QueryPlanNode::SummaryEstimate {
                                input: QueryNodeId(0),
                                query: readout,
                            },
                        ),
                    ]),
                    instant: InstantExecution {
                        lookback_ms,
                        full_history: false,
                        cumulative_readout: true,
                    },
                    fallback: FallbackPolicy::Reject,
                },
            );
        }
    }
    PhysicalPlanInstallRequest {
        summary_catalog: catalog,
        collector_plans: vec![],
        precompute_plan: precompute,
        transmission_plan: transmission,
        query_plan,
        storage_routing: None,
        adaptation_evidence: vec![],
    }
}

#[allow(dead_code)]
pub fn bootstrap() -> ActivePhysicalPlan {
    let mut plan = data_plane::drivers::query::servers::http::build_active_physical_plan(
        artifact(&StreamingConfig::default()),
        Arc::new(BackendStorageRouting::empty()),
    )
    .unwrap();
    plan.envelope.plan_id = 0;
    plan.envelope.plan_version = 0;
    plan.precompute_plan.envelope = plan.envelope.clone();
    plan.transmission_plan.envelope = plan.envelope.clone();
    plan.summary_catalog = None;
    plan
}

pub fn stamp(
    request: &mut asap_otel_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
    plan: &PhysicalPlanInstallRequest,
) {
    use asap_otel_proto::tonic::{
        common::v1::{any_value, AnyValue, KeyValue},
        metrics::v1::{
            metric::Data, CountMinSketchEncoding, CountSketchEncoding, DdSketchEncoding,
            HllSketchEncoding, KllSketchEncoding,
        },
    };
    for metric in request
        .resource_metrics
        .iter_mut()
        .flat_map(|r| &mut r.scope_metrics)
        .flat_map(|s| &mut s.metrics)
    {
        let schema = plan.precompute_plan.schemas.iter().find(|s| matches!(&s.source,
            planner_types::pre_asap::Source::TimeSeries { metric: name }
                if name == &metric.name || metric.name.strip_suffix("_ddsketch") == Some(name.as_str()))).unwrap();
        let add = |attrs: &mut Vec<KeyValue>, proto: bool, delta: bool| {
            let labels: Vec<_> = attrs
                .iter()
                .filter_map(|a| match a.value.as_ref()?.value.as_ref()? {
                    any_value::Value::StringValue(v) => Some((a.key.as_str(), v.as_str())),
                    _ => None,
                })
                .collect();
            let identity = data_plane::drivers::ingest::canonical_attrs_fingerprint(&labels);
            // Sequences belong to one producer stream, not the parallel test suite.
            static STREAMS: std::sync::OnceLock<std::sync::Mutex<BTreeMap<String, (u64, String)>>> =
                std::sync::OnceLock::new();
            let mut streams = STREAMS.get_or_init(Default::default).lock().unwrap();
            let key = format!("{plan:p}:{}:{identity}", schema.materialization.0);
            let (sequence, checkpoint) = streams.entry(key).or_default();
            *sequence += 1;
            if !delta {
                *checkpoint = format!("fixture-{sequence}");
            }
            assert!(!checkpoint.is_empty(), "delta requires a full checkpoint");
            for (key, value) in [
                ("identity_version", "1".into()),
                ("plan_id", "1".into()),
                ("plan_version", "1".into()),
                ("backend_compat", BACKEND_COMPAT.into()),
                (
                    "materialization",
                    schema.materialization.0.as_u64().to_string(),
                ),
                ("schema_id", schema.schema_id.clone()),
                ("producer_id", "fixture".into()),
                ("producer_epoch", "fixture".into()),
                ("sequence", sequence.to_string()),
                ("series_identity", identity),
                ("kind", if delta { "delta" } else { "full" }.into()),
                (
                    if delta {
                        "base_checkpoint_id"
                    } else {
                        "checkpoint_id"
                    },
                    checkpoint.clone(),
                ),
                (
                    "encoding",
                    if proto {
                        "sketchlib_protobuf_v1"
                    } else {
                        "sketch_core_msgpack_v1"
                    }
                    .into(),
                ),
            ] {
                attrs.push(KeyValue {
                    key: format!("asap.frame.{key}"),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(value)),
                    }),
                });
            }
        };
        match metric.data.as_mut().unwrap() {
            Data::Ddsketch(s) => {
                for p in &mut s.data_points {
                    add(
                        &mut p.attributes,
                        p.encoding == DdSketchEncoding::DdsketchEncodingProto as i32
                            || p.encoding == 2,
                        p.encoding == 2,
                    );
                }
            }
            Data::Kllsketch(s) => {
                for p in &mut s.data_points {
                    add(
                        &mut p.attributes,
                        p.encoding == KllSketchEncoding::Proto as i32,
                        false,
                    );
                }
            }
            Data::Hllsketch(s) => {
                for p in &mut s.data_points {
                    add(
                        &mut p.attributes,
                        p.encoding == HllSketchEncoding::Proto as i32,
                        false,
                    );
                }
            }
            Data::Countminsketch(s) => {
                for p in &mut s.data_points {
                    add(
                        &mut p.attributes,
                        p.encoding != CountMinSketchEncoding::Msgpack as i32,
                        false,
                    );
                }
            }
            Data::Countsketch(s) => {
                for p in &mut s.data_points {
                    add(
                        &mut p.attributes,
                        p.encoding != CountSketchEncoding::Msgpack as i32,
                        false,
                    );
                }
            }
            _ => panic!("sketch fixture required"),
        }
    }
}
