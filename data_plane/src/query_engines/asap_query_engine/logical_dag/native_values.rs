//! Bind protocol vectors to native batch operators; computation stays in Planner.
use super::{grouping_key, miss, EngineError, Grouping, Labels, Vector};
use asap_physical_operators::dag::{
    self, batch_execution,
    operators::{Operator, SortKey},
    values::{Batch, Schema, Value},
};
use planner_types::{
    post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
    pre_asap::DataType,
};
use std::sync::Arc;
fn schema(fields: &[(&str, DataType)]) -> Schema {
    Arc::new(SummarySchema {
        fields: fields
            .iter()
            .map(|(name, dtype)| SummaryField {
                name: (*name).into(),
                dtype: SummaryFamilyType::Plain(dtype.clone()),
                nullable: false,
            })
            .collect(),
        time_index: None,
    })
}

fn key<T: serde::Serialize>(value: &T) -> Value {
    Value::Utf8(
        serde_json::to_string(value)
            .expect("string keys serialize")
            .into(),
    )
}
fn ranked_batch(values: &Vector, grouping: &Grouping) -> Result<Batch, EngineError> {
    let schema = schema(&[
        ("index", DataType::Int64),
        ("group", DataType::Utf8),
        ("value", DataType::Float64),
    ]);
    Batch::try_new(
        schema,
        values
            .iter()
            .enumerate()
            .map(|(i, (labels, v))| {
                vec![
                    Value::Int64(i as i64),
                    key(&grouping_key(labels, grouping)),
                    Value::Float64(*v),
                ]
            })
            .collect(),
    )
    .map_err(|e| miss(e.to_string()))
}
fn output(values: Vector, batches: Vec<dag::SharedValue<Batch>>) -> Result<Vector, EngineError> {
    batches
        .iter()
        .flat_map(|b| b.rows())
        .map(|row| match row.first() {
            Some(Value::Int64(index)) => values
                .get(*index as usize)
                .cloned()
                .ok_or_else(|| miss("native result index outside input")),
            _ => Err(miss("native result has no row identity")),
        })
        .collect()
}
pub(super) fn sort(
    values: Vector,
    grouping: &Grouping,
    descending: bool,
    context: &dag::RunContext,
) -> Result<Vector, EngineError> {
    let batch = ranked_batch(&values, grouping)?;
    let op = Operator::sort(
        batch.schema().clone(),
        vec![SortKey {
            column: 2,
            descending,
            nulls_first: false,
        }],
        vec![1],
    )
    .map_err(|e| miss(e.to_string()))?;
    let result = batch_execution::evaluate_batch(batch, vec![op], context.clone())
        .map_err(|e| miss(e.to_string()))?;
    output(values, result)
}
pub(super) fn limit(
    values: Vector,
    grouping: &Grouping,
    n: u64,
    offset: u64,
    context: &dag::RunContext,
) -> Result<Vector, EngineError> {
    let batch = ranked_batch(&values, grouping)?;
    let op = Operator::limit(batch.schema().clone(), n, offset, vec![1])
        .map_err(|e| miss(e.to_string()))?;
    let result = batch_execution::evaluate_batch(batch, vec![op], context.clone())
        .map_err(|e| miss(e.to_string()))?;
    output(values, result)
}
pub(super) fn semi_join(
    values: Vector,
    candidates: &Vector,
    left_key: &impl Fn(&Labels) -> Vec<String>,
    right_key: &impl Fn(&Labels) -> Vec<String>,
    context: &dag::RunContext,
) -> Result<Vector, EngineError> {
    let schema = schema(&[("index", DataType::Int64), ("key", DataType::Utf8)]);
    let batch = |rows: &Vector, identity: &dyn Fn(&Labels) -> Vec<String>| {
        Batch::try_new(
            schema.clone(),
            rows.iter()
                .enumerate()
                .map(|(i, (labels, _))| vec![Value::Int64(i as i64), key(&identity(labels))])
                .collect(),
        )
        .map_err(|e| miss(e.to_string()))
    };
    let op = Operator::semi_join(schema.clone(), schema.clone(), vec![(1, 1)])
        .map_err(|e| miss(e.to_string()))?;
    let result = batch_execution::evaluate_inputs(
        vec![batch(&values, left_key)?, batch(candidates, right_key)?],
        op,
        context.clone(),
    )
    .map_err(|e| miss(e.to_string()))?;
    output(values, result)
}

#[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
    fields(stage = "physical.execute", query_id = %entry.query_id, evaluation_time_ms = at,
        input_kind = "bound_promql_vector", bound_counter_state = entry.physical_vector_binding().is_some()), err)]
pub(super) fn execute_vectors<F>(
    entry: &asap_types::query_plan::QueryPlanEntry,
    at: u64,
    mut callback: F,
) -> Result<
    (
        crate::query_engines::query_result::QueryResult,
        super::ExecutionStats,
    ),
    EngineError,
>
where
    F: FnMut(
        asap_types::query_plan::QueryNodeId,
        u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, EngineError>,
{
    use crate::{
        query_engines::query_result::{InstantVectorElement, QueryResult},
        storage_engines::types::KeyByLabelValues,
    };
    use asap_physical_operators::physical_planner::{
        promql_rows::{decode_series_identity, series_row, SERIES_IDENTITY_COLUMN},
        Source,
    };
    use futures::{executor::block_on, StreamExt};
    use std::collections::BTreeMap;
    let (program, bindings, max_bytes) =
        if let Some((inputs, source_nodes, budget)) = entry.physical_vector_binding() {
            (
                entry
                    .recover_vector_physical_dag()
                    .map_err(|e| miss(e.to_string()))?,
                source_nodes
                    .iter()
                    .copied()
                    .zip(inputs.iter().copied())
                    .collect::<BTreeMap<_, _>>(),
                budget,
            )
        } else {
            let program = entry
                .recover_population_physical_dag()
                .map_err(|e| miss(e.to_string()))?;
            let source = program.input_contracts().next().unwrap().0;
            (
                program,
                BTreeMap::from([(source, entry.root)]),
                entry.population_snapshot().unwrap().max_bytes,
            )
        };
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let mut sources = BTreeMap::new();
    let mut input_bytes = 0usize;
    for (input_id, input) in program.input_contracts() {
        let values = super::vector(super::from_result(callback(bindings[&input_id], at)?)?)?;
        let rows = values
            .into_iter()
            .map(|(labels, value)| {
                series_row(&input.schema, &labels, at_signed, value)
                    .map_err(|e| miss(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = Batch::try_new(input.schema.clone(), rows).map_err(|e| miss(e.to_string()))?;
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or_else(|| miss("physical input size overflow"))?;
        if input_bytes > max_bytes as usize {
            return Err(miss("physical input exceeds run budget"));
        }
        let source =
            Operator::source(input.schema.clone(), vec![batch]).map_err(|e| miss(e.to_string()))?;
        sources.insert(input_id, Box::new(source) as Source<'_>);
    }
    let graph = {
        let _binding = tracing::debug_span!(target: "asap_runtime_debug", "physical_input_binding",
            stage = "physical.bind_inputs", input_count = sources.len(), input_bytes, input_kind = "bound_promql_vector").entered();
        program
            .instantiate(sources)
            .map_err(|e| miss(e.to_string()))?
    };
    let context = dag::RunContext::new(
        dag::Scope::Query {
            evaluation_time_ms: at_signed,
            revision: 0,
        },
        dag::Limits {
            max_bytes: max_bytes as usize,
            ..dag::Limits::default()
        },
    )
    .map_err(|error| miss(error.to_string()))?;
    let mut stream = graph
        .execute(program.roots(), context)
        .map_err(|error| miss(error.to_string()))?
        .remove(0);
    let values = block_on(async {
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|error| miss(error.to_string()))?;
            let identity = batch
                .schema()
                .fields
                .iter()
                .position(|field| field.name == SERIES_IDENTITY_COLUMN)
                .ok_or_else(|| miss("physical output loses series identity"))?;
            let value = batch
                .schema()
                .fields
                .iter()
                .position(|field| field.name == "value")
                .ok_or_else(|| miss("physical output loses sample value"))?;
            for row in batch.rows() {
                let (Value::Utf8(encoded), Value::Float64(sample)) = (&row[identity], &row[value])
                else {
                    return Err(miss("invalid population physical output"));
                };
                let labels =
                    decode_series_identity(encoded).map_err(|error| miss(error.to_string()))?;
                values.push(
                    InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                        *sample,
                    )
                    .with_label_keys_override(labels.into_keys().collect()),
                );
            }
        }
        Ok(values)
    })?;
    Ok((
        QueryResult::vector(values, at),
        super::ExecutionStats {
            summary_readout_evaluations: bindings.len(),
            ..Default::default()
        },
    ))
}
