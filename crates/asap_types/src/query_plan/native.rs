//! Installation of native SQL fragments; recovery only decodes physical operators.
use super::*;
use asap_physical_operators::{dag, physical_planner::CompiledPhysicalDag};
use planner_types::post_asap::{
    ExecutableDagNode, ExecutableOperatorPayload as Payload, ExecutionDataState, PostAsapNodeId,
    SummarySchema,
};
use std::sync::Arc;

impl QueryPlanEntry {
    /// Invoke during plan compilation, after external/storage boundaries are fixed.
    pub fn compile_relational_physical_dag(&mut self) -> Result<(), QueryPlanError> {
        let Some(expected) = self.relation_output_schema()? else {
            self.physical_dag = None;
            return Ok(());
        };
        let compiled = self
            .compile_relation(self.root, &expected)
            .map_err(QueryPlanError::Invalid)?;
        self.physical_dag = Some(
            serde_json::from_slice(
                &compiled
                    .encode()
                    .map_err(|error| QueryPlanError::Invalid(error.to_string()))?,
            )
            .map_err(|error| QueryPlanError::Invalid(error.to_string()))?,
        );
        Ok(())
    }

    fn compile_relation(
        &self,
        root: QueryNodeId,
        expected: &SummarySchema,
    ) -> Result<CompiledPhysicalDag, String> {
        let mut pending = vec![(root, expected.clone())];
        let mut schemas = BTreeMap::<QueryNodeId, SummarySchema>::new();
        let mut operations = BTreeMap::new();
        let mut sources = Vec::new();
        // Bind the entire computation before reading any storage source.
        while let Some((id, expected)) = pending.pop() {
            if let Some(previous) = schemas.get(&id) {
                if previous != &expected {
                    return Err("inconsistent relation schemas".into());
                }
                continue;
            }
            schemas.insert(id, expected.clone());
            let (payload, inputs) = match self.nodes.get(&id) {
                Some(QueryPlanNode::Relational {
                    input,
                    operation,
                    input_schema,
                    output_schema,
                }) => {
                    if output_schema != &expected {
                        return Err("relational output schema mismatch".into());
                    }
                    let operation =
                        serde_json::from_value(operation.clone()).map_err(|e| e.to_string())?;
                    (
                        Payload::Value { operation },
                        vec![(*input, input_schema.clone())],
                    )
                }
                Some(QueryPlanNode::RelationalJoin {
                    inputs,
                    join_kind,
                    pred,
                    left_schema,
                    right_schema,
                    output_schema,
                    pruning,
                }) => {
                    if output_schema != &expected {
                        return Err("join output schema mismatch".into());
                    }
                    if pruning.is_some() {
                        return Err(
                            "candidate pruning is not bound for this relation source".into()
                        );
                    }
                    (
                        Payload::RelationalJoin {
                            join_kind: join_kind.clone(),
                            pred: serde_json::from_value(pred.clone())
                                .map_err(|e| e.to_string())?,
                            pruning: None,
                        },
                        vec![
                            (inputs[0], left_schema.clone()),
                            (inputs[1], right_schema.clone()),
                        ],
                    )
                }
                Some(_) => {
                    sources.push(id);
                    continue;
                }
                None => return Err("missing relation node".into()),
            };
            let node = ExecutableDagNode {
                id: PostAsapNodeId(
                    u32::try_from(id.0).map_err(|_| "relation node ID exceeds Planner range")?,
                ),
                payload,
                output_state: ExecutionDataState::QUERY_ROWS,
                output_schema: expected,
                guarantee: None,
            };
            let op = dag::planner::compile_node(
                &node,
                &inputs
                    .iter()
                    .map(|(_, schema)| Arc::new(schema.clone()))
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?;
            operations.insert(
                id,
                (inputs.iter().map(|(id, _)| id.0).collect::<Vec<_>>(), op),
            );
            pending.extend(inputs);
        }
        let compiled =
            asap_physical_operators::physical_planner::CompiledPhysicalDag::from_operators(
                sources
                    .iter()
                    .map(|id| {
                        (
                            id.0,
                            asap_physical_operators::physical_planner::InputContract::bounded(
                                Arc::new(schemas[id].clone()),
                            ),
                        )
                    })
                    .collect(),
                operations.into_iter().map(|(id, op)| (id.0, op)).collect(),
                vec![root.0],
            )
            .map_err(|e| e.to_string())?;
        Ok(compiled)
    }

    pub fn relation_output_schema(&self) -> Result<Option<SummarySchema>, QueryPlanError> {
        match self.nodes.get(&self.root) {
            Some(
                QueryPlanNode::Relational { output_schema, .. }
                | QueryPlanNode::RelationalJoin { output_schema, .. },
            ) => Ok(Some(output_schema.clone())),
            Some(QueryPlanNode::ExternalExact { request, .. }) => match &request.output {
                ExternalExactOutput::Relation { schema } => serde_json::from_value(schema.clone())
                    .map(Some)
                    .map_err(|error| QueryPlanError::Invalid(error.to_string())),
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// Validate persisted computation and boundary schemas without logical lowering.
    pub fn recover_relational_physical_dag(&self) -> Result<CompiledPhysicalDag, QueryPlanError> {
        let invalid = |message: String| QueryPlanError::Invalid(message);
        let value = self
            .physical_dag
            .as_ref()
            .ok_or_else(|| invalid("missing installed physical DAG".into()))?;
        let dag = CompiledPhysicalDag::decode(
            &serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))?,
        )
        .map_err(|e| invalid(e.to_string()))?;
        if dag.roots() != [self.root.0] {
            return Err(invalid(
                "installed physical root differs from query root".into(),
            ));
        }
        let expected = self
            .relation_output_schema()?
            .ok_or_else(|| invalid("physical relation has no output schema".into()))?;
        if *dag
            .output_contract(self.root.0)
            .map_err(|e| invalid(e.to_string()))?
            .schema
            != expected
        {
            return Err(invalid("installed physical output schema mismatch".into()));
        }
        let mut expected_inputs = BTreeMap::new();
        let mut pending = vec![(self.root, expected)];
        let mut seen = BTreeMap::new();
        while let Some((id, schema)) = pending.pop() {
            if let Some(previous) = seen.insert(id, schema.clone()) {
                if previous != schema {
                    return Err(invalid("inconsistent relation schemas".into()));
                }
                continue;
            }
            match self.nodes.get(&id) {
                Some(QueryPlanNode::Relational {
                    input,
                    input_schema,
                    output_schema,
                    ..
                }) => {
                    if output_schema != &schema {
                        return Err(invalid("relation output schema mismatch".into()));
                    }
                    pending.push((*input, input_schema.clone()));
                }
                Some(QueryPlanNode::RelationalJoin {
                    inputs,
                    left_schema,
                    right_schema,
                    output_schema,
                    ..
                }) => {
                    if output_schema != &schema {
                        return Err(invalid("join output schema mismatch".into()));
                    }
                    pending.extend([
                        (inputs[0], left_schema.clone()),
                        (inputs[1], right_schema.clone()),
                    ]);
                }
                Some(_) => {
                    expected_inputs.insert(id.0, schema);
                }
                None => {
                    return Err(invalid(
                        "physical input references absent query node".into(),
                    ))
                }
            }
        }
        let actual: BTreeMap<_, _> = dag
            .input_contracts()
            .map(|(id, c)| (id, c.schema.as_ref().clone()))
            .collect();
        if actual != expected_inputs {
            return Err(invalid(
                "physical input boundaries differ from installed bindings".into(),
            ));
        }
        Ok(dag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField},
        pre_asap::DataType,
    };

    fn installed() -> QueryPlanEntry {
        let schema = SummarySchema {
            fields: vec![SummaryField {
                name: "value".into(),
                dtype: SummaryFamilyType::Plain(DataType::Float64),
                nullable: false,
            }],
            time_index: None,
        };
        let mut entry = QueryPlanEntry {
            physical_dag: None,
            language: QueryLanguage::ClickHouseSql,
            query_id: "native".into(),
            canonical_query: "SELECT value".into(),
            fixed_evaluation: None,
            root: QueryNodeId(0),
            nodes: BTreeMap::from([(
                QueryNodeId(0),
                QueryPlanNode::ExternalExact {
                    request: ExternalExactRequest {
                        language: QueryLanguage::ClickHouseSql,
                        expression: "SELECT value".into(),
                        output: ExternalExactOutput::Relation {
                            schema: serde_json::to_value(schema).unwrap(),
                        },
                        parameters: BTreeMap::new(),
                        start_parameter: None,
                        end_parameter: None,
                        input_contracts: vec![],
                    },
                    inputs: vec![],
                },
            )]),
            instant: InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::Reject,
        };
        entry.compile_relational_physical_dag().unwrap();
        entry
    }

    // Restart keeps the physical program; unknown formats and stale bindings fail activation.
    #[test]
    fn recovery_validates_physical_version_roots_and_input_schema() {
        let entry = installed();
        let encoded = serde_json::to_vec(&entry).unwrap();
        let restored: QueryPlanEntry = serde_json::from_slice(&encoded).unwrap();
        restored.validate(&BTreeSet::new()).unwrap();
        let mut corrupt = restored.clone();
        corrupt.physical_dag.as_mut().unwrap()["version"] = serde_json::json!(99);
        assert!(corrupt.validate(&BTreeSet::new()).is_err());
        let mut corrupt = restored.clone();
        corrupt.root = QueryNodeId(1);
        assert!(corrupt.validate(&BTreeSet::new()).is_err());
        let mut corrupt = restored.clone();
        if let QueryPlanNode::ExternalExact { request, .. } =
            corrupt.nodes.get_mut(&QueryNodeId(0)).unwrap()
        {
            if let ExternalExactOutput::Relation { schema } = &mut request.output {
                schema["fields"][0]["name"] = serde_json::json!("different");
            }
        }
        assert!(corrupt.validate(&BTreeSet::new()).is_err());
        let mut missing = restored;
        missing.physical_dag = None;
        assert!(missing.validate(&BTreeSet::new()).is_err());
    }
}
