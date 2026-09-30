//! Retained physical programs for SQL relations and PromQL vectors.
use super::*;
use crate::physical_plan_codec::PhysicalPlanCodec;
use asap_physical_operators::physical_planner::CompiledPhysicalDag;
use planner_types::post_asap::SummarySchema;

impl QueryPlanEntry {
    pub fn population_snapshot(&self) -> Option<&current_series::SeriesPopulation> {
        if self.nodes.len() != 1 {
            return None;
        }
        match self.nodes.get(&self.root) {
            Some(QueryPlanNode::Logical {
                operator: query_time::QueryTimeOperator::CurrentSeries { population },
                inputs,
            }) if inputs.is_empty() => Some(population),
            _ => None,
        }
    }

    /// Recover an installed population readout; the source binds the complete
    /// maintained vector, while the physical program owns ranking and limiting.
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
        use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
        // A ranking returns complete source rows; an aggregate returns one
        // value per group of the population's grouping labels.
        let numeric = |field: &planner_types::post_asap::SummaryField| {
            matches!(
                field.dtype,
                SummaryFamilyType::Plain(DataType::Float64 | DataType::Int64)
            )
        };
        let labels = output
            .schema
            .fields
            .iter()
            .filter(|field| !numeric(field))
            .map(|field| {
                (field.dtype == SummaryFamilyType::Plain(DataType::Utf8)).then_some(&field.name)
            })
            .collect::<Option<BTreeSet<_>>>();
        let grouped_value = output.schema.time_index.is_none()
            && output.schema.fields.iter().filter(|f| numeric(f)).count() == 1
            && labels.is_some_and(|labels| {
                labels == population.grouping.labels.iter().collect::<BTreeSet<_>>()
            });
        if input.schema != output.schema && !grouped_value {
            return Err(invalid(
                "population readout must return source rows or one value per group",
            ));
        }
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
                ..
            }) => Some((inputs, source_nodes, *max_bytes)),
            _ => None,
        }
    }

    /// Whether the root physical result drops `__name__` from series identities.
    pub fn drops_metric_name(&self) -> bool {
        matches!(
            self.nodes.get(&self.root),
            Some(QueryPlanNode::Physical {
                drop_metric_name: true,
                ..
            })
        )
    }

    /// Validate bound counter vectors or stored aggregate batches against the
    /// retained physical program; recovery never lowers logical operators.
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
        let raw = |input: &QueryNodeId| {
            matches!(
                self.nodes.get(input),
                Some(QueryPlanNode::Logical {
                    operator: query_time::QueryTimeOperator::Scan {
                        metric: Some(_),
                        range_ms: Some(_),
                        ..
                    },
                    ..
                })
            )
        };
        // Raw samples are read from the external endpoint at query time; like
        // exact cuts, they share no snapshot with installed summary state.
        if inputs.iter().any(raw) && !inputs.iter().all(raw) {
            return Err(invalid(
                "query-time raw inputs cannot be mixed with installed state",
            ));
        }
        for input in inputs {
            if raw(input) {
                continue;
            }
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
                    !matches!(summaries.as_slice(), [field] if match &field.dtype {
                        SummaryFamilyType::Sketch(kind, _) => matches!(kind.algorithm(), planner_types::post_asap::SketchAlgorithm::CmsWithHeap | planner_types::post_asap::SketchAlgorithm::CountSketchWithHeap),
                        SummaryFamilyType::ExactAggregate(planner_types::post_asap::ExactKind::Sum, _) => true,
                        _ => false,
                    })
                } else { !vector_schema(&input.schema) }
            })
            || {
                let output = dag.output_contract(dag.roots()[0]).map_err(|e| QueryPlanError::Invalid(e.to_string()))?;
                output.schema.fields.iter().filter(|field| field.dtype == SummaryFamilyType::Plain(DataType::Float64)).count() != 1
                    || output.schema.fields.iter().any(|field| !matches!(field.dtype, SummaryFamilyType::Plain(DataType::Utf8 | DataType::Float64 | DataType::Timestamp)))
            }
        {
            return Err(invalid(&format!(
                "physical program has incompatible input or result schema: inputs {:?}; output {:?}",
                dag.input_contracts().map(|(id, contract)| (id, &contract.schema)).collect::<Vec<_>>(),
                dag.output_contract(dag.roots()[0]).map_err(|error| QueryPlanError::Invalid(error.to_string()))?.schema,
            )));
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
                            schema: serde_json::to_value(&schema).unwrap(),
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
        let physical = CompiledPhysicalDag::from_operators(
            [(
                0,
                asap_physical_operators::physical_planner::InputContract::bounded(
                    std::sync::Arc::new(schema),
                ),
            )]
            .into(),
            BTreeMap::new(),
            vec![0],
        )
        .unwrap();
        entry.root = QueryNodeId(1);
        entry.nodes.insert(
            entry.root,
            QueryPlanNode::PhysicalRelation {
                inputs: vec![QueryNodeId(0)],
                dag: physical.encode().unwrap(),
            },
        );
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
        if let QueryPlanNode::PhysicalRelation { dag, .. } =
            corrupt.nodes.get_mut(&corrupt.root).unwrap()
        {
            let mut encoded: serde_json::Value = serde_json::from_slice(dag).unwrap();
            encoded["version"] = serde_json::json!(99);
            *dag = serde_json::to_vec(&encoded).unwrap();
        }
        assert!(corrupt.validate(&BTreeSet::new()).is_err());
        let mut corrupt = restored.clone();
        corrupt.root = QueryNodeId(99);
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
        if let QueryPlanNode::PhysicalRelation { dag, .. } =
            missing.nodes.get_mut(&missing.root).unwrap()
        {
            dag.clear();
        }
        assert!(missing.validate(&BTreeSet::new()).is_err());
    }
}
