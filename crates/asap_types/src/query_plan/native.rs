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
    #[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
        fields(stage = "physical.compile_install", query_id = %self.query_id), err)]
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
    #[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
        fields(stage = "physical.recover_validate", query_id = %self.query_id), err)]
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

impl QueryPlanEntry {
    pub fn population_snapshot(&self) -> Option<&current_series::SeriesPopulation> {
        if self.nodes.len() != 1 {
            return None;
        }
        match self.nodes.get(&self.root) {
            Some(QueryPlanNode::Logical {
                operator:
                    residual::ResidualQueryOperator::CurrentSeries {
                        population,
                        readout: current_series::SeriesReadout::Snapshot,
                    },
                inputs,
            }) if inputs.is_empty() => Some(population),
            _ => None,
        }
    }

    /// Recover an installed population readout; the source binds the complete
    /// maintained vector, while the physical program owns ranking and limiting.
    #[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
        fields(stage = "physical.recover_validate", query_id = %self.query_id, input_kind = "current_series_snapshot"), err)]
    pub fn recover_population_physical_dag(&self) -> Result<CompiledPhysicalDag, QueryPlanError> {
        let invalid = |message: &str| QueryPlanError::Invalid(message.into());
        let population = self
            .population_snapshot()
            .ok_or_else(|| invalid("missing population source binding"))?;
        population.validate()?;
        let value = self
            .physical_dag
            .as_ref()
            .ok_or_else(|| invalid("missing installed population physical DAG"))?;
        let dag = CompiledPhysicalDag::decode(
            &serde_json::to_vec(value)
                .map_err(|error| QueryPlanError::Invalid(error.to_string()))?,
        )
        .map_err(|error| QueryPlanError::Invalid(error.to_string()))?;
        let inputs = dag.input_contracts().collect::<Vec<_>>();
        let [(_, input)] = inputs.as_slice() else {
            return Err(invalid("population physical DAG requires one source"));
        };
        let [root] = dag.roots() else {
            return Err(invalid("population physical DAG requires one root"));
        };
        let output = dag
            .output_contract(*root)
            .map_err(|error| QueryPlanError::Invalid(error.to_string()))?;
        if input.schema != output.schema {
            return Err(invalid(
                "population ranking must preserve complete source rows",
            ));
        }
        use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
        let fields = &input.schema.fields;
        let column = |name: &str, dtype: DataType| {
            fields.iter().any(|field| {
                field.name == name
                    && field.dtype == SummaryFamilyType::Plain(dtype.clone())
                    && !field.nullable
            })
        };
        if !column(
            asap_physical_operators::physical_planner::promql_rows::SERIES_IDENTITY_COLUMN,
            DataType::Utf8,
        ) || !column("value", DataType::Float64)
            || input.schema.time_index.is_none_or(|index| {
                fields[index].dtype != SummaryFamilyType::Plain(DataType::Timestamp)
            })
            || population.grouping.without
            || population.grouping.labels.iter().any(|label| {
                !fields.iter().any(|field| {
                    &field.name == label && field.dtype == SummaryFamilyType::Plain(DataType::Utf8)
                })
            })
        {
            return Err(invalid(
                "population physical source loses identity, value, timestamp or grouping",
            ));
        }
        Ok(dag)
    }
}

impl QueryPlanEntry {
    pub fn physical_vector_binding(&self) -> Option<(&[QueryNodeId], &[u64], u64)> {
        match self.nodes.get(&self.root) {
            Some(QueryPlanNode::Physical {
                inputs,
                source_nodes,
                max_bytes,
            }) => Some((inputs, source_nodes, *max_bytes)),
            _ => None,
        }
    }

    /// The initial stored-vector adapter binds exact per-series counter reads.
    /// Recovery validates source identities and schemas without logical lowering.
    #[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
        fields(stage = "physical.recover_validate", query_id = %self.query_id, input_kind = "stored_counter_readout"), err)]
    pub fn recover_vector_physical_dag(&self) -> Result<CompiledPhysicalDag, QueryPlanError> {
        let invalid = |message: &str| QueryPlanError::Invalid(message.into());
        let (inputs, source_nodes, max_bytes) = self
            .physical_vector_binding()
            .ok_or_else(|| invalid("missing physical vector binding"))?;
        if max_bytes == 0
            || usize::try_from(max_bytes).is_err()
            || inputs.is_empty()
            || inputs.len() != source_nodes.len()
            || source_nodes.iter().copied().collect::<BTreeSet<_>>().len() != source_nodes.len()
        {
            return Err(invalid("invalid physical vector source mapping or budget"));
        }
        for input in inputs {
            if let Some(QueryPlanNode::ReadMaterialization { binding }) = self.nodes.get(input) {
                if binding.readout_lookback_ms != Some(self.instant.lookback_ms)
                    || binding.window_ms != self.instant.lookback_ms
                    || binding.window_ms == 0
                    || !matches!(&binding.output_grouping, PhysicalGrouping::Reduce(labels) if labels.is_empty())
                {
                    return Err(invalid(&format!("stored native batch requires one complete bound window: {binding:?}, query lookback {}", self.instant.lookback_ms)));
                }
                continue;
            }
            let Some(QueryPlanNode::ExactReadout {
                input: state,
                readout: ExactReadout::Rate,
            }) = self.nodes.get(input)
            else {
                return Err(invalid(
                    "physical vector requires an exact per-series Rate readout",
                ));
            };
            let Some(QueryPlanNode::ReadMaterialization { binding }) = self.nodes.get(state) else {
                return Err(invalid(
                    "physical Rate input requires its installed stored output",
                ));
            };
            if binding
                .readout_lookback_ms
                .is_none_or(|window| window == 0 || window != self.instant.lookback_ms)
                || !matches!(&binding.output_grouping, PhysicalGrouping::PerEntity)
            {
                return Err(invalid(
                    "physical Rate input must preserve every series and its window",
                ));
            }
        }
        let value = self
            .physical_dag
            .as_ref()
            .ok_or_else(|| invalid("missing installed vector physical DAG"))?;
        let dag = CompiledPhysicalDag::decode(
            &serde_json::to_vec(value).map_err(|e| QueryPlanError::Invalid(e.to_string()))?,
        )
        .map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
        if dag
            .input_contracts()
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>()
            != source_nodes.iter().copied().collect()
            || dag.roots().len() != 1
        {
            return Err(invalid(
                "physical vector program differs from installed source mapping",
            ));
        }
        use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
        let vector_schema = |schema: &SummarySchema| {
            [
                (
                    asap_physical_operators::physical_planner::promql_rows::SERIES_IDENTITY_COLUMN,
                    DataType::Utf8,
                ),
                ("value", DataType::Float64),
            ]
            .into_iter()
            .all(|(name, dtype)| {
                schema.fields.iter().any(|f| {
                    f.name == name
                        && f.dtype == SummaryFamilyType::Plain(dtype.clone())
                        && !f.nullable
                })
            }) && schema.time_index.is_some_and(|i| {
                schema
                    .fields
                    .get(i)
                    .is_some_and(|f| f.dtype == SummaryFamilyType::Plain(DataType::Timestamp))
            })
        };
        if dag
            .input_contracts()
            .any(|(id, input)| {
                let position = source_nodes.iter().position(|source| *source == id).unwrap();
                if matches!(self.nodes.get(&inputs[position]), Some(QueryPlanNode::ReadMaterialization { .. })) {
                    let summaries = input.schema.fields.iter().filter(|field| !matches!(field.dtype, SummaryFamilyType::Plain(_))).collect::<Vec<_>>();
                    !matches!(summaries.as_slice(), [field] if matches!(&field.dtype, SummaryFamilyType::Sketch(kind, _) if matches!(kind.algorithm(), planner_types::post_asap::SketchAlgorithm::CmsWithHeap | planner_types::post_asap::SketchAlgorithm::CountSketchWithHeap)))
                } else { !vector_schema(&input.schema) }
            })
            || !vector_schema(
                &dag.output_contract(dag.roots()[0])
                    .map_err(|e| QueryPlanError::Invalid(e.to_string()))?
                    .schema,
            )
        {
            return Err(invalid(
                "physical vector program loses complete identity, timestamp or value",
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
