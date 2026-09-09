//! Read-only resolution against the installed catalog replica. Descriptor
//! integrity and binding source/grouping are checked once during installation;
//! query execution checks only reachable IDs and their requested capabilities.
use std::collections::{BTreeMap, BTreeSet};

use asap_types::sds::{DataDescriptor, MaterializationId, SummaryDescriptor, SummaryOperator};
use asap_types::AggregationType;
use control_plane::physical::summary_catalog::{MaterializationIdentity, SummaryCatalog};
use control_plane::query_plan::{ExactReadout, QueryPlanEntry, QueryPlanNode, QueryReadout};

use crate::query_engines::EngineError;

pub(crate) struct ResolvedMaterialization<'a> {
    pub identity: &'a MaterializationIdentity,
    pub summary: &'a SummaryDescriptor,
    pub data: &'a DataDescriptor,
}

fn miss(reason: impl Into<String>) -> EngineError {
    EngineError::capability_miss("summary_catalog", reason)
}

/// Borrow descriptors; never reconstruct them from bindings or store payloads.
pub(crate) fn resolve(
    catalog: &SummaryCatalog,
    id: MaterializationId,
) -> Result<ResolvedMaterialization<'_>, EngineError> {
    let identity = catalog
        .materializations
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
    Ok(ResolvedMaterialization {
        identity,
        summary,
        data,
    })
}

impl ResolvedMaterialization<'_> {
    pub fn is_exact(&self) -> bool {
        matches!(
            &self.summary.operator,
            SummaryOperator::Configured {
                aggregation_type: AggregationType::Sum
                    | AggregationType::MultipleSum
                    | AggregationType::Increase
                    | AggregationType::MultipleIncrease
                    | AggregationType::MinMax
                    | AggregationType::MultipleMinMax,
                ..
            }
        )
    }

    fn supports(&self, node: &QueryPlanNode) -> bool {
        let SummaryOperator::Configured {
            aggregation_type,
            aggregation_sub_type,
            ..
        } = &self.summary.operator
        else {
            // Partial legacy descriptors cannot attest a configured capability.
            return false;
        };
        use AggregationType::*;
        match node {
            QueryPlanNode::ExactReadout { readout, .. } => match readout {
                ExactReadout::Sum => matches!(aggregation_type, Sum | MultipleSum),
                ExactReadout::Count => *aggregation_type == Sum,
                ExactReadout::Increase | ExactReadout::Rate => {
                    matches!(aggregation_type, Increase | MultipleIncrease)
                }
                ExactReadout::Max => {
                    matches!(aggregation_type, MinMax | MultipleMinMax)
                        && aggregation_sub_type.eq_ignore_ascii_case("max")
                }
            },
            QueryPlanNode::SummaryEstimate { query, .. } => match query {
                QueryReadout::Quantile { q } => {
                    q.is_finite()
                        && (0.0..=1.0).contains(q)
                        && matches!(aggregation_type, DatasketchesKLL | HydraKLL | DDSketch)
                }
                QueryReadout::Cardinality => *aggregation_type == HLL,
                QueryReadout::PointCount { .. } => matches!(
                    aggregation_type,
                    CountMinSketch | CountMinSketchWithHeap | CountSketch | CountSketchWithHeap
                ),
                QueryReadout::TopK { .. } => matches!(
                    aggregation_type,
                    CountMinSketchWithHeap | CountSketchWithHeap
                ),
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
                if !resolved.contains_key(&binding.materialization) {
                    resolved.insert(
                        binding.materialization,
                        resolve(catalog, binding.materialization)?,
                    );
                }
                BTreeSet::from([binding.materialization])
            }
            QueryPlanNode::SummaryMerge { inputs } => {
                let mut ids = BTreeSet::new();
                for input in inputs {
                    let children: &BTreeSet<MaterializationId> = states
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
                let ids: &BTreeSet<MaterializationId> = states
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
    use control_plane::physical::compiler::BackendLocalPlanningSnapshot;

    fn fixture() -> control_plane::physical::compiler::PhysicalPlan {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        value["query_workload"]["repeating_queries"][0]["query"] =
            "sum(sum_over_time(m[1m]))".into();
        let snapshot: BackendLocalPlanningSnapshot = serde_json::from_value(value).unwrap();
        snapshot.compile().unwrap()
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
        let id = *catalog.materializations.keys().next().unwrap();
        let result = resolve(catalog, id).unwrap();
        assert!(std::ptr::eq(
            result.identity,
            &catalog.materializations[&id]
        ));
        assert!(std::ptr::eq(
            result.summary,
            &catalog.summary_descriptors[&result.identity.summary_descriptor_id]
        ));
        assert!(std::ptr::eq(
            result.data,
            &catalog.data_descriptors[&result.identity.data_descriptor_id]
        ));
        let mut broken = catalog.clone();
        broken.data_descriptors.clear();
        assert!(resolve(&broken, id).is_err());
    }
}
