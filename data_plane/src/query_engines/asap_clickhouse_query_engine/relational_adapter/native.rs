//! JSON/Arrow transport stays in the deployment; Planner executes relation values.
use super::{fields_from_schema, Cell, ClickHouseRelation, ClickHouseRelationalError};
use asap_physical_operators::dag::{
    self,
    values::{Batch, Value},
};
use planner_types::post_asap::{
    ExecutableDagNode, ExecutableOperatorPayload, ExecutionDataState, PostAsapNodeId,
    SummaryFamilyType, SummaryField, SummarySchema,
};
use std::sync::Arc;
fn error(error: impl std::fmt::Display) -> ClickHouseRelationalError {
    ClickHouseRelationalError::Invalid(error.to_string())
}
pub(crate) fn schema(input: &ClickHouseRelation) -> SummarySchema {
    SummarySchema {
        fields: input
            .fields
            .iter()
            .map(|(name, dtype, nullable)| SummaryField {
                name: name.clone(),
                dtype: SummaryFamilyType::Plain(dtype.clone()),
                nullable: *nullable,
            })
            .collect(),
        time_index: None,
    }
}
pub(super) fn value(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(v) => Value::Bool(*v),
        Cell::Int64(v) => Value::Int64(*v),
        Cell::Float64(v) => Value::Float64(*v),
        Cell::Utf8(v) => Value::Utf8(v.clone().into()),
        Cell::Timestamp(v) => Value::Timestamp(*v),
        Cell::List(v) => Value::List(v.iter().map(value).collect::<Vec<_>>().into()),
        Cell::Struct(v) => Value::Struct(v.iter().map(value).collect::<Vec<_>>().into()),
        Cell::Map(v) => Value::Map(
            v.iter()
                .map(|(k, v)| (value(k), value(v)))
                .collect::<Vec<_>>()
                .into(),
        ),
    }
}
pub(super) fn cell(value: &Value) -> Result<Cell, ClickHouseRelationalError> {
    Ok(match value {
        Value::Null => Cell::Null,
        Value::Bool(v) => Cell::Bool(*v),
        Value::Int64(v) => Cell::Int64(*v),
        Value::Float64(v) => Cell::Float64(*v),
        Value::Utf8(v) => Cell::Utf8(v.to_string()),
        Value::Timestamp(v) => Cell::Timestamp(*v),
        Value::List(v) => Cell::List(v.iter().map(cell).collect::<Result<Vec<_>, _>>()?.into()),
        Value::Struct(v) => Cell::Struct(v.iter().map(cell).collect::<Result<Vec<_>, _>>()?.into()),
        Value::Map(v) => Cell::Map(
            v.iter()
                .map(|(k, v)| Ok((cell(k)?, cell(v)?)))
                .collect::<Result<Vec<_>, ClickHouseRelationalError>>()?,
        ),
        _ => return Err(error("native value has no ClickHouse result transport")),
    })
}
pub(crate) fn execute(
    payload: ExecutableOperatorPayload,
    output: &SummarySchema,
    inputs: Vec<ClickHouseRelation>,
) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
    let coverage = inputs
        .iter()
        .map(|input| input.coverage)
        .reduce(|left, right| match (left, right) {
            (Some((a, b)), Some((c, d))) if a.max(c) <= b.min(d) => Some((a.max(c), b.min(d))),
            _ => None,
        })
        .flatten();
    let batches = inputs
        .iter()
        .map(|input| {
            Batch::try_new(
                Arc::new(schema(input)),
                input
                    .rows
                    .iter()
                    .map(|row| row.iter().map(value).collect())
                    .collect(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    let node = ExecutableDagNode {
        id: PostAsapNodeId(0),
        payload,
        output_state: ExecutionDataState::QUERY_ROWS,
        output_schema: output.clone(),
        guarantee: None,
    };
    let operator = asap_physical_operators::physical_planner::compile_node(
        &node,
        &batches
            .iter()
            .map(|batch| batch.schema().clone())
            .collect::<Vec<_>>(),
    )
    .map_err(|error| ClickHouseRelationalError::Unsupported(error.to_string()))?;
    let context = dag::RunContext::new(
        dag::Scope::Query {
            evaluation_time_ms: coverage.map_or(0, |(_, end)| end as i64),
            revision: 0,
        },
        dag::Limits::default(),
    )
    .map_err(error)?;
    let batches =
        dag::batch_execution::evaluate_inputs(batches, operator, context).map_err(error)?;
    let rows = batches
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row.iter().map(cell).collect())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ClickHouseRelation {
        rows,
        fields: fields_from_schema(output),
        coverage,
    })
}

pub(crate) fn batch(
    input: &ClickHouseRelation,
    expected: &SummarySchema,
) -> Result<Batch, dag::Error> {
    if schema(input).fields != expected.fields {
        return Err(dag::Error::Invalid(
            "source relation differs from its declared schema".into(),
        ));
    }
    Batch::try_new(
        Arc::new(expected.clone()),
        input
            .rows
            .iter()
            .map(|row| row.iter().map(value).collect())
            .collect(),
    )
}
pub(crate) fn relation(
    batches: &[dag::SharedValue<Batch>],
    output: &SummarySchema,
    coverage: Option<(u64, u64)>,
) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
    Ok(ClickHouseRelation {
        fields: fields_from_schema(output),
        rows: batches
            .iter()
            .flat_map(|batch| batch.rows())
            .map(|row| row.iter().map(cell).collect())
            .collect::<Result<_, _>>()?,
        coverage,
    })
}
