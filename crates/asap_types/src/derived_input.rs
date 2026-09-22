//! Content identity for a maintenance sub-DAG cut at existing summary inputs.
//! The executable program remains in OwnedPostAsapDag; this is its catalog key.
use std::collections::{BTreeMap, BTreeSet};

use planner_types::post_asap::PostAsapNodeId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{executable_plan::OwnedPostAsapDag, sds::SummaryDefinitionId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedInputIdentity {
    pub inputs: BTreeSet<SummaryDefinitionId>,
    pub program_sha256: String,
}

impl DerivedInputIdentity {
    pub fn validate(&self) -> Result<(), String> {
        if self.inputs.is_empty()
            || self.inputs.iter().any(|id| id.fingerprint().is_unset())
            || self.program_sha256.len() != 64
            || !self
                .program_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("derived input requires summary references and a canonical SHA-256".into());
        }
        Ok(())
    }

    /// Hash semantic nodes and edge roles, replacing input frontiers with stable
    /// catalog IDs. Query IDs, node numbering, and catalog generations are absent.
    pub fn from_dag(
        document: &OwnedPostAsapDag,
        root: PostAsapNodeId,
        frontiers: &BTreeMap<PostAsapNodeId, SummaryDefinitionId>,
    ) -> Result<Self, String> {
        if ![
            crate::executable_plan::OWNED_POST_ASAP_DAG_SCHEMA_VERSION,
            crate::executable_plan::MAINTENANCE_DAG_SCHEMA_VERSION,
        ]
        .contains(&document.schema_version)
        {
            return Err("unsupported derived program document version".into());
        }
        let decoded = document.decode()?;
        let literals: BTreeSet<_> = decoded
            .nodes
            .iter()
            .filter_map(|node| {
                matches!(
                    &node.payload,
                    planner_types::post_asap::ExecutableOperatorPayload::Fallback {
                        expression: planner_types::pre_asap::QueryExpr::Literal(_),
                    }
                )
                .then_some(node.id)
            })
            .collect();
        let mut incoming: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for edge in &document.edges {
            incoming.entry(edge.consumer).or_default().push(edge);
        }
        let nodes: BTreeMap<_, _> = document.nodes.iter().map(|n| (n.id, n)).collect();
        if frontiers.contains_key(&root) {
            return Err("derived program requires a non-frontier root".into());
        }
        let mut hashes = BTreeMap::new();
        let mut visiting = BTreeSet::new();
        let mut inputs = BTreeSet::new();
        let mut stack = vec![(root, false)];
        while let Some((id, finish)) = stack.pop() {
            if hashes.contains_key(&id) {
                continue;
            }
            let node = nodes
                .get(&id)
                .ok_or("derived program references missing node")?;
            if let Some(summary) = frontiers.get(&id) {
                inputs.insert(*summary);
                hashes.insert(id, serde_json::json!({"summary": summary}));
                continue;
            }
            if !finish {
                if !visiting.insert(id) {
                    return Err("derived program has a cycle".into());
                }
                stack.push((id, true));
                if !incoming.contains_key(&id) && !literals.contains(&id) {
                    return Err("derived program has an unbound input leaf".into());
                }
                for edge in incoming.get(&id).into_iter().flatten() {
                    stack.push((edge.producer, false));
                }
                continue;
            }
            let mut edges = Vec::new();
            for edge in incoming.get(&id).into_iter().flatten() {
                let input = hashes
                    .get(&edge.producer)
                    .ok_or("derived input was not evaluated")?;
                edges.push(
                    serde_json::to_vec(&serde_json::json!({
                        "input": input, "role": edge.role, "schema": edge.intermediate_schema,
                        "state": edge.data_state, "grouping": edge.grouping, "window": edge.window,
                    }))
                    .map_err(|e| e.to_string())?,
                );
            }
            edges.sort();
            let bytes = serde_json::to_vec(&serde_json::json!({
                "version": 2, "payload": node.payload,
                "state": node.output_state, "schema": node.output_schema,
                "guarantee": node.guarantee, "inputs": edges,
            }))
            .map_err(|e| e.to_string())?;
            hashes.insert(
                id,
                serde_json::json!(format!("{:x}", Sha256::digest(bytes))),
            );
            visiting.remove(&id);
        }
        let identity = Self {
            inputs,
            program_sha256: hashes[&root]
                .as_str()
                .ok_or("derived root has no semantic hash")?
                .into(),
        };
        identity.validate()?;
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::summary_catalog::SummaryCatalog;
    use crate::{AggregationType, KeyByLabelNames, PrecomputeMaterialization, WindowKind};

    fn config() -> PrecomputeMaterialization {
        PrecomputeMaterialization::new(
            AggregationType::Sum,
            String::new(),
            Default::default(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            10,
            10,
            WindowKind::Tumbling,
            String::new(),
            "m".into(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn derived_source_is_distinct_and_generation_independent() {
        let raw = config();
        let raw_id = SummaryDefinitionId::from(raw.policy_fingerprint());
        let mut derived = raw.clone();
        derived.derived_input = Some(DerivedInputIdentity {
            inputs: BTreeSet::from([raw_id]),
            program_sha256: "a".repeat(64),
        });
        assert_ne!(raw.policy_fingerprint(), derived.policy_fingerprint());
        let a =
            SummaryCatalog::from_materializations(1, 1, &[raw.clone(), derived.clone()]).unwrap();
        let b = SummaryCatalog::from_materializations(2, 9, &[raw, derived.clone()]).unwrap();
        assert_eq!(a.definitions, b.definitions);
        assert_eq!(a.data_descriptors, b.data_descriptors);
        let mut renamed = derived.clone();
        renamed.metric = "output_alias".into();
        assert_eq!(renamed.policy_fingerprint(), derived.policy_fingerprint());
        let json = serde_json::to_value(&derived).unwrap();
        let decoded: PrecomputeMaterialization = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.policy_fingerprint(), derived.policy_fingerprint());
    }

    #[test]
    fn raw_utf8_metric_cannot_impersonate_derived_policy_domain() {
        let mut derived = config();
        derived.derived_input = Some(DerivedInputIdentity {
            inputs: BTreeSet::from([SummaryDefinitionId::from(derived.policy_fingerprint())]),
            program_sha256: "d".repeat(64),
        });
        let mut raw = derived.clone();
        raw.metric = format!(
            "derived-input-v1:{}",
            serde_json::to_string(&derived.source_identity()).unwrap()
        );
        raw.derived_input = None;
        assert_ne!(raw.policy_fingerprint(), derived.policy_fingerprint());
    }

    #[test]
    fn catalog_rejects_missing_derived_dependencies_and_raw_source_conflicts() {
        let mut derived = config();
        derived.derived_input = Some(DerivedInputIdentity {
            inputs: BTreeSet::from([SummaryDefinitionId::from(derived.policy_fingerprint())]),
            program_sha256: "b".repeat(64),
        });
        assert!(SummaryCatalog::from_materializations(1, 1, &[derived.clone()]).is_err());
        derived.table_name = Some("table".into());
        assert!(derived.population_filter_canonical().is_err());
    }
    fn program(source: u32, root: u32) -> OwnedPostAsapDag {
        use crate::executable_plan::{OwnedPostAsapEdge, OwnedPostAsapNode};
        use planner_types::post_asap::{
            EdgeRole, ExecutionDataState, GroupingEdgeCompatibility, WindowEdgeCompatibility,
        };
        let state = ExecutionDataState::MAINTENANCE_SUMMARY;
        OwnedPostAsapDag {
            schema_version: crate::executable_plan::OWNED_POST_ASAP_DAG_SCHEMA_VERSION,
            query_id: "query-a".into(),
            root: PostAsapNodeId(root),
            nodes: [source, root]
                .into_iter()
                .map(|id| OwnedPostAsapNode {
                    id: PostAsapNodeId(id),
                    payload: serde_json::json!({"kind":"summary_merge"}),
                    output_state: state,
                    output_schema: serde_json::json!({"fields":[],"time_index":null}),
                    guarantee: None,
                })
                .collect(),
            edges: vec![OwnedPostAsapEdge {
                producer: PostAsapNodeId(source),
                consumer: PostAsapNodeId(root),
                role: EdgeRole::Input,
                intermediate_schema: serde_json::json!({"fields":[],"time_index":null}),
                data_state: state,
                grouping: GroupingEdgeCompatibility::Identical,
                window: WindowEdgeCompatibility::NotApplicable,
            }],
        }
    }

    #[test]
    fn semantic_signature_ignores_node_and_query_numbering_but_not_inputs() {
        let source = SummaryDefinitionId::from(config().policy_fingerprint());
        let a = program(1, 2);
        let first = DerivedInputIdentity::from_dag(
            &a,
            a.root,
            &BTreeMap::from([(PostAsapNodeId(1), source)]),
        )
        .unwrap();
        let mut b = program(900, 42);
        b.query_id = "another-query".into();
        b.nodes.reverse();
        let second = DerivedInputIdentity::from_dag(
            &b,
            b.root,
            &BTreeMap::from([(PostAsapNodeId(900), source)]),
        )
        .unwrap();
        assert_eq!(first, second);
        b.nodes
            .iter_mut()
            .find(|n| n.id == b.root)
            .unwrap()
            .output_schema = serde_json::json!({"fields":[],"time_index":0});
        assert_ne!(
            first,
            DerivedInputIdentity::from_dag(
                &b,
                b.root,
                &BTreeMap::from([(PostAsapNodeId(900), source)])
            )
            .unwrap()
        );
        assert!(DerivedInputIdentity::from_dag(&a, a.root, &BTreeMap::new()).is_err());
        let mut unsupported = a.clone();
        unsupported.schema_version = 999;
        assert!(DerivedInputIdentity::from_dag(
            &unsupported,
            unsupported.root,
            &BTreeMap::from([(PostAsapNodeId(1), source)])
        )
        .is_err());
        unsupported = a.clone();
        unsupported.nodes[1].payload = serde_json::json!({"kind":"unknown_operator"});
        assert!(DerivedInputIdentity::from_dag(
            &unsupported,
            unsupported.root,
            &BTreeMap::from([(PostAsapNodeId(1), source)])
        )
        .is_err());
        let mut cycle = a.clone();
        cycle.edges[0].producer = cycle.root;
        assert!(DerivedInputIdentity::from_dag(&cycle, cycle.root, &BTreeMap::new()).is_err());
        cycle.edges[0].producer = PostAsapNodeId(999);
        assert!(DerivedInputIdentity::from_dag(&cycle, cycle.root, &BTreeMap::new()).is_err());
    }

    #[test]
    fn catalog_rejects_self_referential_summary() {
        use crate::sds::{
            DataDescriptor, DataSourceIdentity, SummaryDescriptor, ValueProjectionIdentity,
        };
        let config = config();
        let input = DerivedInputIdentity {
            inputs: BTreeSet::from([SummaryDefinitionId::from(config.policy_fingerprint())]),
            program_sha256: "f".repeat(64),
        };
        let data = DataDescriptor::new_typed(
            DataSourceIdentity::Derived { input },
            ValueProjectionIdentity::SampleValue,
            "",
            Vec::<String>::new(),
            "derived",
        );
        assert!(SummaryCatalog::build(
            1,
            1,
            [(
                config.policy_fingerprint(),
                SummaryDescriptor::from_config(&config).unwrap(),
                data,
            )]
        )
        .is_err());
    }
    #[test]
    fn installation_rejects_derived_inputs_without_an_installed_maintenance_dag() {
        use crate::precompute_plan::{PlanEnvelope, PrecomputePlan};
        let mut config = config();
        config.derived_input = Some(DerivedInputIdentity {
            inputs: BTreeSet::from([SummaryDefinitionId::from(config.policy_fingerprint())]),
            program_sha256: "c".repeat(64),
        });
        let envelope = PlanEnvelope {
            plan_id: 1,
            plan_version: 1,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: "asap-query-backend.v1".into(),
            planner_revision: "test".into(),
            capability_snapshot_id: "test".into(),
        };
        let error =
            PrecomputePlan::build(envelope, vec![config], &["producer".into()]).unwrap_err();
        assert!(error
            .to_string()
            .contains("installed aligned immutable maintenance DAG"));
    }
    #[test]
    fn literal_leaves_are_hashed_without_inventing_materialization_references() {
        use planner_types::{
            post_asap::ExecutableOperatorPayload,
            pre_asap::{QueryExpr, ScalarValue},
        };
        let mut dag = program(1, 2);
        let mut literal = dag.nodes[0].clone();
        literal.id = PostAsapNodeId(3);
        literal.payload = serde_json::to_value(ExecutableOperatorPayload::Fallback {
            expression: QueryExpr::Literal(ScalarValue::Int64(2)),
        })
        .unwrap();
        dag.nodes.push(literal);
        let mut edge = dag.edges[0].clone();
        edge.producer = PostAsapNodeId(3);
        dag.edges.push(edge);
        let source = SummaryDefinitionId::from(config().policy_fingerprint());
        let frontiers = BTreeMap::from([(PostAsapNodeId(1), source)]);
        let first = DerivedInputIdentity::from_dag(&dag, dag.root, &frontiers).unwrap();
        assert_eq!(first.inputs, BTreeSet::from([source]));
        dag.nodes[2].payload = serde_json::to_value(ExecutableOperatorPayload::Fallback {
            expression: QueryExpr::Literal(ScalarValue::Int64(3)),
        })
        .unwrap();
        assert_ne!(
            first,
            DerivedInputIdentity::from_dag(&dag, dag.root, &frontiers).unwrap()
        );
        assert!(DerivedInputIdentity::from_dag(&dag, PostAsapNodeId(3), &frontiers).is_err());
    }
}
