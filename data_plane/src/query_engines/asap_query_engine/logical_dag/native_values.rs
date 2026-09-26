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
