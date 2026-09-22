//! Read-only resolution against the installed catalog replica. Descriptor
//! integrity and binding source/grouping are checked once during installation;
//! query execution checks only reachable IDs and their requested capabilities.
use std::collections::{BTreeMap, BTreeSet};

use asap_types::query_plan::{ExactReadout, QueryPlanEntry, QueryPlanNode, QueryReadout};
use asap_types::sds::{SummaryDefinitionId, SummaryDescriptor, SummaryOperator};
use asap_types::summary_catalog::SummaryCatalog;
use planner_types::post_asap::{SketchAlgorithm, SummaryFamilyType};

use crate::query_engines::EngineError;

pub(crate) struct ResolvedMaterialization<'a> {
    pub summary: &'a SummaryDescriptor,
}

fn miss(reason: impl Into<String>) -> EngineError {
    EngineError::capability_miss("summary_catalog", reason)
}

/// Borrow descriptors; never reconstruct them from bindings or store payloads.
pub(crate) fn resolve(
    catalog: &SummaryCatalog,
    id: SummaryDefinitionId,
) -> Result<ResolvedMaterialization<'_>, EngineError> {
    let identity = catalog
        .definitions
        .get(&id)
        .ok_or_else(|| miss(format!("unknown materialization {}", id.fingerprint().0)))?;
    let summary = catalog
        .summary_descriptors
        .get(&identity.summary_descriptor_id)
        .ok_or_else(|| miss("missing summary descriptor"))?;
    let data = catalog
        .data_descriptors
        .get(&identity.data_descriptor_id)
        .ok_or_else(|| miss("missing data descriptor"))?;
    // Installation validates the whole snapshot. Keep resolution fail-closed as
    // defense in depth for catalogs restored from disk or supplied by a future
    // transport implementation.
    summary
        .validate()
        .map_err(|error| miss(format!("invalid summary descriptor: {error}")))?;
    data.validate()
        .map_err(|error| miss(format!("invalid data descriptor: {error}")))?;
    Ok(ResolvedMaterialization { summary })
}

impl ResolvedMaterialization<'_> {
    pub fn is_exact(&self) -> bool {
        matches!(
            &self.summary.operator,
            SummaryOperator::Configured {
                family: SummaryFamilyType::ExactAggregate(..),
                ..
            }
        )
    }

    fn supports(&self, node: &QueryPlanNode) -> bool {
        let SummaryOperator::Configured { family, .. } = &self.summary.operator else {
            // Partial legacy descriptors cannot attest a configured capability.
            return false;
        };
        match node {
            QueryPlanNode::ExactReadout { readout, .. } => family == &readout.planner_family(),
            QueryPlanNode::SummaryEstimate { query, .. } => match query {
                QueryReadout::Quantile { q } => {
                    q.is_finite()
                        && (0.0..=1.0).contains(q)
                        && matches!(family, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(), SketchAlgorithm::Kll | SketchAlgorithm::DDSketch))
                }
                QueryReadout::Cardinality => {
                    matches!(family, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(), SketchAlgorithm::Hll | SketchAlgorithm::UnivMon))
                }
                QueryReadout::FrequencyL2 | QueryReadout::FrequencyEntropy => {
                    matches!(family, SummaryFamilyType::Sketch(kind, _) if kind.algorithm() == &SketchAlgorithm::UnivMon)
                }
                QueryReadout::PointCount { value: None, .. } if matches!(family, SummaryFamilyType::Sketch(kind, _) if kind.algorithm() == &SketchAlgorithm::UnivMon) => {
                    true
                }
                QueryReadout::PointCount { .. } => {
                    matches!(family, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(), SketchAlgorithm::Cms | SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketch | SketchAlgorithm::CountSketchWithHeap))
                }
                QueryReadout::TopK { .. } => {
                    matches!(family, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(), SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap))
                }
            },
            _ => false,
        }
    }
}

/// Capability matching follows installed state edges, including merges; it
/// never searches the catalog for a replacement materialization.
pub(crate) fn validate_entry(
    catalog: Option<&SummaryCatalog>,
    entry: &QueryPlanEntry,
    plan_id: u64,
    plan_version: u64,
) -> Result<(), EngineError> {
    validate_payload(catalog, entry, plan_id, plan_version)
}

pub(crate) fn validate_payload(
    catalog: Option<&SummaryCatalog>,
    entry: &QueryPlanEntry,
    plan_id: u64,
    plan_version: u64,
) -> Result<(), EngineError> {
    if entry.materialization_bindings().is_empty() {
        return Ok(());
    }
    let catalog = catalog.ok_or_else(|| miss("installed catalog replica unavailable"))?;
    if (catalog.plan_id, catalog.plan_version) != (plan_id, plan_version) {
        return Err(miss("catalog replica belongs to another plan generation"));
    }
    let mut states = BTreeMap::new();
    let mut resolved = BTreeMap::new();
    for id in entry.topological_order().map_err(|e| miss(e.to_string()))? {
        let node = &entry.nodes[&id];
        let state_ids = match node {
            QueryPlanNode::ReadMaterialization { binding } => {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    resolved.entry(binding.materialization)
                {
                    entry.insert(resolve(catalog, binding.materialization)?);
                }
                BTreeSet::from([binding.materialization])
            }
            QueryPlanNode::SummaryMerge { inputs } => {
                let mut ids = BTreeSet::new();
                for input in inputs {
                    let children: &BTreeSet<SummaryDefinitionId> = states
                        .get(input)
                        .ok_or_else(|| miss("summary merge has no state input"))?;
                    if children.is_empty() {
                        return Err(miss("summary merge has value input"));
                    }
                    ids.extend(children);
                }
                ids
            }
            QueryPlanNode::SummaryEstimate { input, .. }
            | QueryPlanNode::ExactReadout { input, .. } => {
                let ids: &BTreeSet<SummaryDefinitionId> = states
                    .get(input)
                    .ok_or_else(|| miss("readout has no state input"))?;
                if ids.is_empty() || ids.iter().any(|id| !resolved[id].supports(node)) {
                    return Err(miss("catalog descriptor cannot satisfy installed readout"));
                }
                BTreeSet::new()
            }
            _ => BTreeSet::new(),
        };
        states.insert(id, state_ids);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::summary_catalog::SummaryCatalog;
    use asap_types::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};
    use control_plane::physical::compiler::BackendLocalPlanningInput;

    fn fixture() -> control_plane::physical::compiler::CompiledPhysicalPlan {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        value["query_workload"]["repeating_queries"][0]["query"] =
            "sum(sum_over_time(m[1m]))".into();
        let snapshot: BackendLocalPlanningInput = serde_json::from_value(value).unwrap();
        crate::tests::test_utilities::planning::quoted_snapshot(snapshot, false)
            .compile_promql()
            .unwrap()
    }

    fn catalog_fixture() -> SummaryCatalog {
        let config = PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowKind::Tumbling,
            String::new(),
            "m".into(),
            None,
            None,
            None,
        );
        SummaryCatalog::from_materializations(1, 1, &[config]).unwrap()
    }

    // A readout cannot relabel a valid sum materialization as a rate capability.
    #[test]
    fn rejects_wrong_readout_and_missing_or_stale_replica() {
        let bundle = fixture();
        let catalog = &bundle.summary_catalog;
        let mut entry = bundle.query_plan.entries.values().next().unwrap().clone();
        validate_entry(Some(catalog), &entry, catalog.plan_id, catalog.plan_version).unwrap();
        assert!(validate_entry(None, &entry, catalog.plan_id, catalog.plan_version).is_err());
        assert!(validate_entry(
            Some(catalog),
            &entry,
            catalog.plan_id,
            catalog.plan_version + 1
        )
        .is_err());
        let readout = entry
            .nodes
            .values_mut()
            .find(|n| matches!(n, QueryPlanNode::ExactReadout { .. }))
            .unwrap();
        if let QueryPlanNode::ExactReadout { readout, .. } = readout {
            *readout = ExactReadout::Rate;
        }
        assert!(
            validate_entry(Some(catalog), &entry, catalog.plan_id, catalog.plan_version).is_err()
        );
    }

    // Resolution returns borrowed authoritative definitions, and fails closed on missing IDs.
    #[test]
    fn resolves_without_descriptor_copies() {
        let bundle = fixture();
        let catalog = &bundle.summary_catalog;
        let id = *catalog.definitions.keys().next().unwrap();
        let result = resolve(catalog, id).unwrap();
        assert!(std::ptr::eq(
            result.summary,
            &catalog.summary_descriptors[&catalog.definitions[&id].summary_descriptor_id]
        ));
        let mut broken = catalog.clone();
        broken.data_descriptors.clear();
        assert!(resolve(&broken, id).is_err());
    }

    #[test]
    fn rejects_operator_fidelity_mismatch_during_resolution() {
        let mut catalog = catalog_fixture();
        let id = *catalog.definitions.keys().next().unwrap();
        let descriptor_id = catalog.definitions[&id].summary_descriptor_id.clone();
        catalog
            .summary_descriptors
            .get_mut(&descriptor_id)
            .unwrap()
            .fidelity = asap_types::sds::FidelityGuarantee::KllRankError {
            k: 200,
            model: "rank.v1".into(),
        };
        assert!(resolve(&catalog, id).is_err());
    }
}
