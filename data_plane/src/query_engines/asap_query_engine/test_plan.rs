//! Explicit installed plans for runtime tests. Fixtures choose the materialization
//! and readout; query text is only canonicalized, never planned here.

use super::engine::ASAPQueryEngine;
use crate::drivers::query::servers::http::{
    validate_and_build_runtime_plan, PhysicalPlanInstallRequest,
};
use crate::storage_engines::sketch_db::index::SketchStore;
use crate::storage_engines::types::{ActivePhysicalPlanHandle, BackendStorageRouting};
use asap_types::precompute_plan::{PlanEnvelope, PrecomputePlan, BACKEND_COMPAT};
use asap_types::query_plan::*;
use asap_types::PrecomputeMaterialization;
use control_plane::physical::compiler::PLANNER_REVISION;
use std::{collections::BTreeMap, sync::Arc};

pub(super) fn materialization(
    metric: &str,
    aggregation: &str,
    parameters: serde_json::Value,
    labels: &[&str],
    pane_ms: u64,
) -> PrecomputeMaterialization {
    assert_eq!(pane_ms % 1000, 0);
    serde_json::from_value(serde_json::json!({
        "aggregation_type": aggregation, "aggregation_sub_type": "",
        "metric": metric, "window_size": pane_ms / 1000, "slide_interval": pane_ms / 1000,
        "window_type": "tumbling", "num_aggregates_to_retain": 100,
        "parameters": parameters, "pane_origin_ms": 0,
        "partitioning": "per_entity",
        "window_layout": {"kind": "pane", "pane_secs": pane_ms / 1000},
        "grouping_labels": {"labels": labels}, "aggregated_labels": {"labels": []},
        "rollup_labels": {"labels": []}, "spatial_filter": "",
        "spatial_filter_normalized": "", "original_yaml": ""
    }))
    .unwrap()
}

pub(super) fn entry(
    query: &str,
    config: &PrecomputeMaterialization,
    grouping: PhysicalGrouping,
    lookback_ms: u64,
    readout: QueryPlanNode,
) -> QueryPlanEntry {
    let canonical = canonical_promql(query).unwrap();
    QueryPlanEntry {
        language: QueryLanguage::PromQl,
        query_id: canonical.clone(),
        canonical_query: canonical,
        fixed_evaluation: None,
        root: QueryNodeId(1),
        nodes: BTreeMap::from([
            (
                QueryNodeId(0),
                QueryPlanNode::ReadMaterialization {
                    binding: MaterializationBinding {
                        full_window_slide_ms: matches!(
                            config.window_layout,
                            asap_types::WindowMaterializationLayout::FullWindow
                        )
                        .then_some(config.slide_interval * 1000),
                        materialization: config.policy_fingerprint().into(),
                        stored_output_reference:
                            asap_types::summary_catalog::SummaryCatalog::from_materializations(
                                1,
                                1,
                                &[config.clone()],
                            )
                            .unwrap()
                            .output_reference(config.policy_fingerprint().into())
                            .unwrap(),
                        output_grouping: grouping,
                        item_labels: config.aggregated_labels.labels.clone(),
                        window_ms: config.stored_window_ms(),
                        pane_origin_ms: config.pane_origin_ms,
                        readout_lookback_ms: Some(lookback_ms),
                    },
                },
            ),
            (QueryNodeId(1), readout),
        ]),
        instant: InstantExecution {
            lookback_ms,
            full_history: false,
            cumulative_readout: true,
        },
        fallback: FallbackPolicy::ExactBackend,
    }
}

pub(super) fn install(
    index: &SketchStore,
    configs: &[(PrecomputeMaterialization, Vec<u64>)],
    entries: Vec<QueryPlanEntry>,
) -> ActivePhysicalPlanHandle {
    let envelope = PlanEnvelope {
        plan_id: 1,
        plan_version: 1,
        generated_at_unix_ms: 0,
        activation_unix_ms: 0,
        expiry_unix_ms: None,
        backend_compat: BACKEND_COMPAT.into(),
        planner_revision: PLANNER_REVISION.into(),
        capability_snapshot_id: "runtime-fixture".into(),
    };
    let materializations: Vec<_> = configs.iter().map(|(c, _)| c.clone()).collect();
    let catalog =
        asap_types::summary_catalog::SummaryCatalog::from_materializations(1, 1, &materializations)
            .unwrap();
    let mut precompute =
        PrecomputePlan::build(envelope.clone(), materializations, &["fixture".into()]).unwrap();
    precompute.bind_catalog(&catalog).unwrap();
    let mut transmission = control_plane::physical::compiler::build_transmission_plan(
        envelope,
        &precompute,
        &BTreeMap::new(),
    )
    .unwrap();
    transmission.summary_catalog = Some(catalog.reference().unwrap());
    index
        .install_summary_catalog(Arc::new(catalog.clone()))
        .unwrap();
    for (config, sids) in configs {
        for sid in sids {
            let mut metadata = (*index.instance(*sid).expect("registered fixture sid")).clone();
            metadata.policy_fp = config.policy_fingerprint();
            index.register(metadata);
        }
    }
    let active = validate_and_build_runtime_plan(
        PhysicalPlanInstallRequest {
            summary_catalog: catalog,
            collector_plans: vec![],
            precompute_plan: precompute,
            transmission_plan: transmission,
            query_plan: QueryPlan {
                plan_id: 1,
                plan_version: 1,
                clickhouse_context: None,
                selected_dags: Default::default(),
                entries: entries
                    .into_iter()
                    .map(|e| (e.canonical_query.clone(), e))
                    .collect(),
            },
            storage_routing: None,
            adaptation_evidence: vec![],
        },
        Arc::new(BackendStorageRouting::empty()),
    )
    .unwrap();
    ActivePhysicalPlanHandle::new(active)
}

pub(super) fn engine(
    index: Arc<SketchStore>,
    config: PrecomputeMaterialization,
    sids: Vec<u64>,
    query_entry: QueryPlanEntry,
) -> ASAPQueryEngine {
    let active = install(&index, &[(config, sids)], vec![query_entry]);
    ASAPQueryEngine::new(15_000)
        .with_sketch_index(index)
        .with_active_physical_plan(active)
}

/// Low-level operator fixtures install the descriptors of their explicit states.
/// Process tests use compiled publications instead of this fixture adapter.
pub(super) fn bound_reference(
    index: &SketchStore,
    output: asap_types::sds::StoredOutputId,
) -> asap_types::sds::StoredOutputReference {
    if let Some(catalog) = index.summary_catalog_snapshot() {
        return catalog.output_reference(output).unwrap();
    }
    let metadata = index.snapshot_instances();
    let entries = metadata
        .iter()
        .filter(|m| !m.policy_fp.is_unset())
        .map(|m| {
            let (summary, data) = index.descriptors_for_series_id(m.sid).unwrap();
            (m.policy_fp, (*summary).clone(), (*data).clone())
        })
        .collect::<Vec<_>>();
    let catalog = asap_types::summary_catalog::SummaryCatalog::build(1, 1, entries).unwrap();
    index
        .install_summary_catalog(Arc::new(catalog.clone()))
        .unwrap();
    for metadata in metadata {
        index.register((*metadata).clone());
    }
    catalog.output_reference(output).unwrap()
}
