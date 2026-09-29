//! Bind a complete durable counter cohort to a Planner-owned precompute graph.
use std::{collections::BTreeMap, sync::Arc};

use asap_physical_operators::{
    operators::Operator,
    physical_planner::{CompiledPhysicalDag, Source},
    runtime::{Limits, RunContext, Scope},
    values::{Batch, Value},
};
use asap_types::executable_plan::{BackendNodeBinding, InstalledPostAsapDag};
use futures::{executor::block_on, StreamExt};
use planner_types::{
    post_asap::{PostAsapNodeId, SummaryFamilyType},
    pre_asap::DataType,
};

pub(super) fn execute(
    installed: &InstalledPostAsapDag,
    program: &CompiledPhysicalDag,
    inputs: &[crate::storage_engines::sketch_db::index::FrozenExactWindows],
    window: (u64, u64),
    max_bytes: usize,
    revision: u64,
) -> Result<BTreeMap<u64, Batch>, Box<dyn std::error::Error + Send + Sync>> {
    let mut sources = BTreeMap::new();
    let mut input_bytes = 0usize;
    for (id, contract) in program.input_contracts() {
        let node = PostAsapNodeId(u32::try_from(id).map_err(|_| "native source id overflow")?);
        let Some(BackendNodeBinding::Materialization { stored_output }) =
            installed.binding.node(node)
        else {
            return Err("native precompute input has no stored binding".into());
        };
        let mut rows = Vec::new();
        let bound_inputs = inputs
            .iter()
            .filter(|input| input.definition == *stored_output)
            .collect::<Vec<_>>();
        if bound_inputs.is_empty() {
            return Err("native precompute input has no eligible stored population".into());
        }
        for input in bound_inputs {
            if input.stored_output_reference.stored_output_id != *stored_output
                || input.windows.is_empty()
            {
                return Err(
                    "native precompute input differs from its stored-output binding".into(),
                );
            }
            if asap_physical_operators::physical_planner::precompute::is_population_schema(
                &contract.schema,
            ) {
                let family = contract.schema.fields[2].dtype.clone();
                for (&(start, end), state) in &input.windows {
                    if start < window.0 || end > window.1 || start >= end {
                        return Err("native precompute pane is outside its bound window".into());
                    }
                    rows.push(vec![
                        Value::Map(
                            input
                                .group
                                .iter()
                                .map(|(k, v)| {
                                    (Value::Utf8(k.clone().into()), Value::Utf8(v.clone().into()))
                                })
                                .collect::<Vec<_>>()
                                .into(),
                        ),
                        Value::Timestamp(
                            i64::try_from(end).map_err(|_| "native pane timestamp overflow")?,
                        ),
                        Value::Summary {
                            family: family.clone(),
                            state: Arc::clone(state),
                        },
                    ]);
                }
                continue;
            }
            let state = input
                .windows
                .get(&window)
                .ok_or("native precompute requires an exact complete counter window")?;
            let row = contract
                .schema
                .fields
                .iter()
                .map(|field| match &field.dtype {
                    SummaryFamilyType::ExactAggregate(
                        planner_types::post_asap::ExactKind::Rate,
                        _,
                    ) => Ok(Value::Summary {
                        family: field.dtype.clone(),
                        state: Arc::clone(state),
                    }),
                    SummaryFamilyType::Plain(DataType::Timestamp) => Ok(Value::Timestamp(
                        i64::try_from(window.1).map_err(|_| "native window overflow")?,
                    )),
                    SummaryFamilyType::Plain(DataType::Utf8)
                        if field.name == "$promql_series_identity" =>
                    {
                        Ok(Value::Utf8(
                            serde_json::to_string(&input.group)
                                .map_err(|e| e.to_string())?
                                .into(),
                        ))
                    }
                    SummaryFamilyType::Plain(DataType::Utf8) => Ok(Value::Utf8(
                        input
                            .group
                            .get(&field.name)
                            .cloned()
                            .unwrap_or_default()
                            .into(),
                    )),
                    _ => Err("unsupported native precompute stored input field".to_string()),
                })
                .collect::<Result<Vec<_>, String>>()?;
            rows.push(row);
        }
        let batch = Batch::try_new(contract.schema.clone(), rows)?;
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or("native input size overflow")?;
        if input_bytes > max_bytes {
            return Err(asap_physical_operators::Error::MemoryLimit.into());
        }
        sources.insert(
            id,
            Box::new(Operator::source(contract.schema.clone(), vec![batch])?) as Source<'_>,
        );
    }
    let graph = program.instantiate(sources)?;
    let context = RunContext::new(
        Scope::Ingestion {
            window_start_ms: i64::try_from(window.0).map_err(|_| "native window overflow")?,
            window_end_ms: i64::try_from(window.1).map_err(|_| "native window overflow")?,
            revision,
        },
        Limits {
            max_bytes,
            ..Limits::default()
        },
    )?;
    // Source buffers and retained publication output share the operator budget.
    let _inputs = context.reserve(input_bytes)?;
    // Drain all roots together: a bounded shared producer can otherwise block
    // while the first root waits for an unpolled sibling to consume its queue.
    let streams = graph.execute(program.roots(), context.clone())?;
    block_on(async {
        let outputs =
            futures::future::try_join_all(program.roots().iter().copied().zip(streams).map(
                |(root, mut stream)| {
                    let context = context.clone();
                    async move {
                        let schema = program.output_contract(root)?.schema;
                        let mut retained = context.reserve(0)?;
                        let mut rows = Vec::new();
                        let mut bytes = 0usize;
                        while let Some(batch) = stream.next().await {
                            let batch = batch?;
                            bytes = bytes
                                .checked_add(batch.bytes())
                                .ok_or(asap_physical_operators::Error::MemoryLimit)?;
                            retained.resize(bytes)?;
                            rows.extend(batch.rows().iter().cloned());
                        }
                        let batch = Batch::try_new(schema, rows)?;
                        Ok::<_, asap_physical_operators::Error>((root, batch, retained))
                    }
                },
            ))
            .await?;
        // Keep every root's reservation until all roots have finished.
        Ok(outputs
            .into_iter()
            .map(|(id, batch, _)| (id, batch))
            .collect())
    })
}

/// Decode output identities without regrouping or evaluating their states.
pub(super) fn population_states(
    batch: &Batch,
    window_end: u64,
) -> Result<
    Vec<(
        BTreeMap<String, String>,
        Arc<dyn crate::storage_engines::types::AggregateCore>,
    )>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    if !asap_physical_operators::physical_planner::precompute::is_population_schema(batch.schema())
    {
        return Err("precompute output is not a population state batch".into());
    }
    let mut result = BTreeMap::new();
    for row in batch.rows() {
        let [Value::Map(labels), Value::Timestamp(end), Value::Summary { state, .. }] =
            row.as_slice()
        else {
            return Err("invalid population output row".into());
        };
        if u64::try_from(*end).ok() != Some(window_end) {
            return Err("precompute output window differs from publication".into());
        }
        let mut group = BTreeMap::new();
        for (key, value) in labels.iter() {
            let (Value::Utf8(key), Value::Utf8(value)) = (key, value) else {
                return Err("invalid population labels".into());
            };
            if group.insert(key.to_string(), value.to_string()).is_some() {
                return Err("duplicate population label".into());
            }
        }
        if result.insert(group, Arc::clone(state)).is_some() {
            return Err("repeated precompute output population".into());
        }
    }
    Ok(result.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_physical_operators::{physical_planner::InputContract, plan::PhysicalOperator};
    use asap_types::executable_plan::{BackendExecutableBinding, OwnedPostAsapDag};

    // Both persisted roots must receive the same producer result, while a later
    // revision gets fresh state. A failed run must return no partial outputs.
    #[test]
    fn shared_outputs_use_one_producer_and_isolate_revisions() {
        use planner_types::post_asap::{ExactKind, ExactParams};
        let family = SummaryFamilyType::ExactAggregate(ExactKind::Sum, ExactParams::Sum);
        let schema =
            asap_physical_operators::physical_planner::precompute::population_schema(family);
        let merge = Operator::summary_merge(schema.clone(), 2, vec![0]).unwrap();
        let output = merge.output_schema();
        let project = || {
            Operator::project(
                output.clone(),
                output
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        (
                            field.name.clone(),
                            asap_physical_operators::expressions::Expression::Column(index),
                        )
                    })
                    .collect(),
            )
            .unwrap()
        };
        let program = CompiledPhysicalDag::from_operators(
            BTreeMap::from([(1, InputContract::bounded(schema))]),
            BTreeMap::from([
                (2, (vec![1], merge)),
                (3, (vec![2], project())),
                (4, (vec![2], project())),
            ]),
            vec![3, 4],
        )
        .unwrap();
        let program = CompiledPhysicalDag::decode(&program.encode().unwrap()).unwrap();
        let definition = asap_types::sds::StoredOutputId(1);
        let installed = InstalledPostAsapDag {
            document: OwnedPostAsapDag {
                schema_version: asap_types::executable_plan::PRECOMPUTE_DAG_SCHEMA_VERSION,
                query_id: "shared".into(),
                nodes: vec![],
                edges: vec![],
                root: PostAsapNodeId(4),
            },
            binding: BackendExecutableBinding {
                nodes: BTreeMap::from([(
                    PostAsapNodeId(1),
                    BackendNodeBinding::Materialization {
                        stored_output: definition,
                    },
                )]),
                query_sink: PostAsapNodeId(4),
                query_plan_sink: asap_types::executable_plan::QueryNodeId(4),
                precompute_sinks: vec![PostAsapNodeId(3), PostAsapNodeId(4)],
            },
            native_programs: BTreeMap::new(),
        };
        let generation = Arc::new(asap_types::sds::CatalogGeneration {
            schema_version: 6,
            plan_id: 1,
            plan_version: 1,
            snapshot_sha256: "0".repeat(64),
        });
        let state = |value| {
            let mut sum = asap_physical_operators::summary_kernels::SumAccumulator::new();
            sum.update(value);
            Arc::new(sum) as Arc<dyn crate::storage_engines::types::AggregateCore>
        };
        for (revision, value) in [(1, 2.0), (2, 7.0)] {
            let inputs = [
                crate::storage_engines::sketch_db::index::FrozenExactWindows {
                    stored_output_reference: asap_types::sds::StoredOutputReference {
                        stored_output_id: definition,
                        definition_id: serde_json::from_value(serde_json::json!(format!(
                            "sds-v1:{}",
                            "0".repeat(64)
                        )))
                        .unwrap(),
                    },
                    storage_handle: 1,
                    definition,
                    generation: generation.clone(),
                    group: BTreeMap::new(),
                    windows: BTreeMap::from([
                        ((0, 1000), state(value)),
                        ((1000, 2000), state(3.0)),
                    ]),
                    singleton_population_complete: true,
                },
            ];
            let outputs =
                execute(&installed, &program, &inputs, (0, 2000), 1 << 20, revision).unwrap();
            assert_eq!(outputs.keys().copied().collect::<Vec<_>>(), vec![3, 4]);
            let state_at = |root| match &outputs[&root].rows()[0][1] {
                Value::Summary { state, .. } => state,
                _ => panic!("expected retained summary state"),
            };
            assert!(
                Arc::ptr_eq(state_at(3), state_at(4)),
                "shared merge must execute once, not once per output"
            );
            assert_eq!(
                state_at(3)
                    .query_statistic(asap_types::Statistic::Sum, &None, &Default::default(),)
                    .unwrap(),
                value + 3.0
            );
            let error = execute(&installed, &program, &inputs, (0, 2000), 1, revision).unwrap_err();
            assert!(matches!(
                error.downcast_ref::<asap_physical_operators::Error>(),
                Some(asap_physical_operators::Error::MemoryLimit)
            ));
        }
    }
}
