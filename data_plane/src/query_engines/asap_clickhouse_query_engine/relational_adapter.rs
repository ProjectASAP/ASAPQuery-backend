//! ClickHouse row semantics for planner-owned relational wrappers.

use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
    },
    datatypes::{DataType as ArrowDataType, Field, Schema},
    record_batch::RecordBatch,
};
use planner_types::{
    post_asap::{SummaryFamilyType, SummaryNode, SummarySchema, ValueOperation},
    pre_asap::{ArithmeticOpKind, CompareOpKind, DataType, QueryExpr, ScalarValue, SortKey},
};

use crate::query_engines::{
    asap_query_engine::{
        summary_exec::ExecOutcome,
        summary_executor::{GroupState, SummaryValue},
    },
    canonical::relational::RelationalAdapter,
};

use super::clickhouse_result_adapter::ClickHouseQueryResult;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ClickHouseRelationalError {
    #[error("unsupported SQL relational operation: {0}")]
    Unsupported(String),
    #[error("SQL column index {0} is outside a {1}-column row")]
    ColumnOutOfRange(usize, usize),
    #[error("invalid SQL operation: {0}")]
    Invalid(String),
    #[error("cannot build Arrow result: {0}")]
    Arrow(String),
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    Null,
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Bool(bool),
    Timestamp(i64),
}

#[derive(Debug)]
pub struct ClickHouseRelation {
    rows: Vec<Vec<Cell>>,
    fields: Vec<(String, DataType, bool)>,
    pub coverage: Option<(u64, u64)>,
}

impl ClickHouseRelation {
    pub fn from_series_rows(
        schema: &SummarySchema,
        series: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>,
        coverage: Option<(u64, u64)>,
    ) -> Result<Self, ClickHouseRelationalError> {
        let fields = fields_from_schema(schema);
        let mut rows = Vec::new();
        for (group, points) in series {
            for (timestamp, value) in points {
                rows.push(row_from_value(&fields, &group, timestamp, value)?);
            }
        }
        Ok(Self {
            rows,
            fields,
            coverage,
        })
    }

    pub fn into_result(self) -> Result<ClickHouseQueryResult, ClickHouseRelationalError> {
        let fields = self
            .fields
            .iter()
            .map(|(name, dtype, nullable)| Field::new(name, arrow_type(dtype), *nullable))
            .collect::<Vec<_>>();
        let columns = (0..self.fields.len())
            .map(|column| build_array(&self.rows, column, &self.fields[column].1))
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| ClickHouseRelationalError::Arrow(error.to_string()))?;
        Ok(ClickHouseQueryResult {
            batches: vec![batch],
        })
    }
}

pub struct ClickHouseRelationalAdapter;

impl ClickHouseRelationalAdapter {
    pub fn apply_inner_equi_join(
        &self,
        pred: &planner_types::pre_asap::Predicate,
        output_schema: &SummarySchema,
        left: ClickHouseRelation,
        right: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        let coverage = match (left.coverage, right.coverage) {
            (Some((left_start, left_end)), Some((right_start, right_end))) => {
                let start = left_start.max(right_start);
                let end = left_end.min(right_end);
                (start <= end).then_some((start, end))
            }
            _ => None,
        };
        let mut rows = Vec::new();
        for left_row in &left.rows {
            for right_row in &right.rows {
                let mut joined = Vec::with_capacity(left_row.len() + right_row.len());
                joined.extend(left_row.iter().cloned());
                joined.extend(right_row.iter().cloned());
                if matches!(eval(&pred.0, &joined)?, Cell::Bool(true)) {
                    rows.push(joined);
                }
            }
        }
        Ok(ClickHouseRelation {
            rows,
            fields: fields_from_schema(output_schema),
            coverage,
        })
    }

    pub fn apply_filter(
        &self,
        pred: &planner_types::pre_asap::Predicate,
        mut input: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        input.rows = input
            .rows
            .into_iter()
            .filter_map(|row| match eval(&pred.0, &row) {
                Ok(Cell::Bool(true)) => Some(Ok(row)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(input)
    }

    pub fn apply_operation(
        &self,
        operation: &ValueOperation,
        output_schema: &SummarySchema,
        mut input: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        match operation {
            ValueOperation::Project { cols, .. } => {
                let mut rows = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    rows.push(
                        cols.iter()
                            .map(|item| eval(&item.expr, row))
                            .collect::<Result<Vec<_>, _>>()?,
                    );
                }
                input.rows = rows;
                input.fields = fields_from_schema(output_schema);
            }
            ValueOperation::Sort { keys, partition_by } => {
                if partition_by.is_without() || !partition_by.is_empty() {
                    return Err(ClickHouseRelationalError::Unsupported(
                        "partitioned sort".into(),
                    ));
                }
                for row in &input.rows {
                    for key in keys {
                        eval(&key.expr, row)?;
                    }
                }
                input
                    .rows
                    .sort_by(|left, right| compare_sort_keys(left, right, keys));
            }
            ValueOperation::Limit { n, offset } => {
                input.rows = input.rows.into_iter().skip(*offset).take(*n).collect();
            }
            other => return Err(ClickHouseRelationalError::Unsupported(format!("{other:?}"))),
        }
        Ok(input)
    }
}

impl<E> RelationalAdapter<E> for ClickHouseRelationalAdapter
where
    E: crate::query_engines::canonical::executor::SummaryExecutor<
        GroupKey = BTreeMap<String, String>,
        State = GroupState,
        Value = SummaryValue,
    >,
{
    type Relation = ClickHouseRelation;
    type Error = ClickHouseRelationalError;

    fn relation_from_outcome(
        &self,
        node: &SummaryNode,
        outcome: ExecOutcome<E>,
    ) -> Result<Self::Relation, Self::Error> {
        let fields = fields(node);
        let mut rows = Vec::new();
        let mut coverage = None;
        let mut coverage_complete = true;
        match outcome {
            ExecOutcome::Value(groups) => {
                for (group, value) in groups {
                    match value.coverage() {
                        Some(next) if coverage_complete => {
                            coverage = intersect_coverage(coverage, Some(next));
                        }
                        None => {
                            coverage = None;
                            coverage_complete = false;
                        }
                        Some(_) => {}
                    }
                    match value {
                        SummaryValue::Points(points, _) => {
                            for (timestamp, value) in points {
                                rows.push(row_from_value(&fields, &group, timestamp, value)?);
                            }
                        }
                        SummaryValue::TopK(_, _) => {
                            return Err(ClickHouseRelationalError::Unsupported(
                                "TopK summary rows".into(),
                            ));
                        }
                    }
                }
            }
            ExecOutcome::State(groups) => {
                for (group, state, family) in groups {
                    let value = exact_value(&state, &family)?;
                    rows.push(row_from_value(&fields, &group, 0, value)?);
                }
            }
        }
        Ok(ClickHouseRelation {
            rows,
            fields,
            coverage,
        })
    }

    fn apply(
        &self,
        node: &SummaryNode,
        operation: &ValueOperation,
        input: Self::Relation,
    ) -> Result<Self::Relation, Self::Error> {
        self.apply_operation(operation, &node.schema, input)
    }
}

fn fields(node: &SummaryNode) -> Vec<(String, DataType, bool)> {
    fields_from_schema(&node.schema)
}

fn fields_from_schema(schema: &SummarySchema) -> Vec<(String, DataType, bool)> {
    schema
        .fields
        .iter()
        .map(|field| {
            let dtype = match &field.dtype {
                SummaryFamilyType::Plain(dtype) => dtype.clone(),
                _ => DataType::Float64,
            };
            (field.name.clone(), dtype, field.nullable)
        })
        .collect()
}

fn exact_value(
    state: &GroupState,
    family: &SummaryFamilyType,
) -> Result<f64, ClickHouseRelationalError> {
    if !matches!(family, SummaryFamilyType::ExactAggregate(..)) {
        return Err(ClickHouseRelationalError::Unsupported(
            "unfinalized non-exact summary state".into(),
        ));
    }
    state.exact_value(&None).ok_or_else(|| {
        ClickHouseRelationalError::Invalid("exact accumulator cannot be finalized".into())
    })
}

fn row_from_value(
    fields: &[(String, DataType, bool)],
    group: &BTreeMap<String, String>,
    timestamp: i64,
    value: f64,
) -> Result<Vec<Cell>, ClickHouseRelationalError> {
    let mut value_used = false;
    fields
        .iter()
        .map(|(name, dtype, nullable)| {
            if let Some(value) = group.get(name) {
                return Ok(Cell::Utf8(value.clone()));
            }
            if *dtype == DataType::Timestamp {
                return Ok(Cell::Timestamp(timestamp));
            }
            if !value_used && matches!(dtype, DataType::Float64 | DataType::Int64) {
                value_used = true;
                return Ok(match dtype {
                    DataType::Int64 => Cell::Int64(value as i64),
                    _ => Cell::Float64(value),
                });
            }
            if *nullable {
                Ok(Cell::Null)
            } else {
                Err(ClickHouseRelationalError::Invalid(format!(
                    "cannot populate output column {name}"
                )))
            }
        })
        .collect()
}

fn eval(expr: &QueryExpr, row: &[Cell]) -> Result<Cell, ClickHouseRelationalError> {
    match expr {
        QueryExpr::Column(index) => {
            row.get(*index)
                .cloned()
                .ok_or(ClickHouseRelationalError::ColumnOutOfRange(
                    *index,
                    row.len(),
                ))
        }
        QueryExpr::Literal(value) => Ok(match value {
            ScalarValue::Int64(value) => Cell::Int64(*value),
            ScalarValue::Float64(value) => Cell::Float64(*value),
            ScalarValue::Utf8(value) => Cell::Utf8(value.clone()),
            ScalarValue::Boolean(value) => Cell::Bool(*value),
            ScalarValue::Null => Cell::Null,
        }),
        QueryExpr::Compare { left, op, right } => {
            let left = eval(left, row)?;
            let right = eval(right, row)?;
            compare(op, left, right)
        }
        QueryExpr::Arithmetic { op, left, right } => {
            arithmetic(op, eval(left, row)?, eval(right, row)?)
        }
        other => Err(ClickHouseRelationalError::Unsupported(format!(
            "scalar expression {other:?}"
        ))),
    }
}

fn compare(op: &CompareOpKind, left: Cell, right: Cell) -> Result<Cell, ClickHouseRelationalError> {
    if matches!(left, Cell::Null) || matches!(right, Cell::Null) {
        return Ok(Cell::Null);
    }
    let ordering = cell_cmp(&left, &right).ok_or_else(|| {
        ClickHouseRelationalError::Invalid("comparison of incompatible values".into())
    })?;
    let value = match op {
        CompareOpKind::Eq => ordering == Ordering::Equal,
        CompareOpKind::Ne => ordering != Ordering::Equal,
        CompareOpKind::Lt => ordering == Ordering::Less,
        CompareOpKind::Le => ordering != Ordering::Greater,
        CompareOpKind::Gt => ordering == Ordering::Greater,
        CompareOpKind::Ge => ordering != Ordering::Less,
        _ => {
            return Err(ClickHouseRelationalError::Unsupported(format!(
                "comparison {op:?}"
            )))
        }
    };
    Ok(Cell::Bool(value))
}

fn arithmetic(
    op: &ArithmeticOpKind,
    left: Cell,
    right: Cell,
) -> Result<Cell, ClickHouseRelationalError> {
    if matches!(left, Cell::Null) || matches!(right, Cell::Null) {
        return Ok(Cell::Null);
    }
    let (left, right) = match (left, right) {
        (Cell::Int64(left), Cell::Int64(right)) => (left as f64, right as f64),
        (Cell::Int64(left), Cell::Float64(right)) => (left as f64, right),
        (Cell::Float64(left), Cell::Int64(right)) => (left, right as f64),
        (Cell::Float64(left), Cell::Float64(right)) => (left, right),
        _ => {
            return Err(ClickHouseRelationalError::Invalid(
                "arithmetic on non-numeric values".into(),
            ))
        }
    };
    let value = match op {
        ArithmeticOpKind::Add => left + right,
        ArithmeticOpKind::Sub => left - right,
        ArithmeticOpKind::Mul => left * right,
        ArithmeticOpKind::Div if right != 0.0 => left / right,
        ArithmeticOpKind::Mod if right != 0.0 => left % right,
        ArithmeticOpKind::Pow => left.powf(right),
        _ => {
            return Err(ClickHouseRelationalError::Unsupported(format!(
                "arithmetic {op:?}"
            )))
        }
    };
    Ok(Cell::Float64(value))
}

fn compare_sort_keys(left: &[Cell], right: &[Cell], keys: &[SortKey]) -> Ordering {
    for key in keys {
        let Ok(left) = eval(&key.expr, left) else {
            return Ordering::Equal;
        };
        let Ok(right) = eval(&key.expr, right) else {
            return Ordering::Equal;
        };
        let (ordering, order_depends_on_direction) = match (&left, &right) {
            (Cell::Null, Cell::Null) => (Ordering::Equal, false),
            (Cell::Null, _) => {
                if key.nulls_first {
                    (Ordering::Less, false)
                } else {
                    (Ordering::Greater, false)
                }
            }
            (_, Cell::Null) => {
                if key.nulls_first {
                    (Ordering::Greater, false)
                } else {
                    (Ordering::Less, false)
                }
            }
            _ => (cell_cmp(&left, &right).unwrap_or(Ordering::Equal), true),
        };
        let ordering = if key.ascending || !order_depends_on_direction {
            ordering
        } else {
            ordering.reverse()
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn cell_cmp(left: &Cell, right: &Cell) -> Option<Ordering> {
    match (left, right) {
        (Cell::Int64(left), Cell::Int64(right)) => Some(left.cmp(right)),
        (Cell::Float64(left), Cell::Float64(right)) => left.partial_cmp(right),
        (Cell::Int64(left), Cell::Float64(right)) => (*left as f64).partial_cmp(right),
        (Cell::Float64(left), Cell::Int64(right)) => left.partial_cmp(&(*right as f64)),
        (Cell::Utf8(left), Cell::Utf8(right)) => Some(left.cmp(right)),
        (Cell::Bool(left), Cell::Bool(right)) => Some(left.cmp(right)),
        (Cell::Timestamp(left), Cell::Timestamp(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn intersect_coverage(current: Option<(u64, u64)>, next: Option<(u64, u64)>) -> Option<(u64, u64)> {
    match (current, next) {
        (None, next) => next,
        (current, None) => current,
        (Some(left), Some(right)) => Some((left.0.max(right.0), left.1.min(right.1))),
    }
}

fn arrow_type(dtype: &DataType) -> ArrowDataType {
    match dtype {
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Utf8 => ArrowDataType::Utf8,
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Timestamp => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
    }
}

fn build_array(
    rows: &[Vec<Cell>],
    column: usize,
    dtype: &DataType,
) -> Result<ArrayRef, ClickHouseRelationalError> {
    macro_rules! values {
        ($variant:ident) => {{
            rows.iter()
                .map(|row| match row.get(column) {
                    Some(Cell::$variant(value)) => Ok(Some(value.clone())),
                    Some(Cell::Null) => Ok(None),
                    _ => Err(ClickHouseRelationalError::Invalid(format!(
                        "column {column} has an incompatible value"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?
        }};
    }
    Ok(match dtype {
        DataType::Int64 => Arc::new(Int64Array::from(values!(Int64))) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(values!(Float64))) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from(values!(Utf8))) as ArrayRef,
        DataType::Bool => Arc::new(BooleanArray::from(values!(Bool))) as ArrayRef,
        DataType::Timestamp => {
            Arc::new(TimestampMillisecondArray::from(values!(Timestamp))) as ArrayRef
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engines::canonical::{
        executor::SummaryExecutor, relational::execute_relational,
    };
    use planner_types::{
        post_asap::{ExecutionTiming, SketchQuery, SummaryExpr, SummaryField, SummarySchema},
        pre_asap::{ColumnRef, GroupKeys, Predicate, ProjectItem, Reduction},
    };
    use std::rc::Rc;

    struct MockExecutor;

    impl SummaryExecutor for MockExecutor {
        type Handle = ();
        type State = GroupState;
        type Value = SummaryValue;
        type Error = ();
        type GroupKey = BTreeMap<String, String>;

        fn find_candidates(
            &self,
            _: &SummaryFamilyType,
            _: &ColumnRef,
            _: &Reduction,
            _: &SummaryNode,
        ) -> Result<Vec<(Self::GroupKey, Self::Handle)>, Self::Error> {
            unreachable!()
        }

        fn fetch_state(&self, _: &Self::Handle) -> Result<Self::State, Self::Error> {
            unreachable!()
        }

        fn merge_states(&self, _: Vec<Self::State>) -> Result<Self::State, Self::Error> {
            unreachable!()
        }

        fn readout(&self, _: &Self::State, _: &SketchQuery) -> Result<Self::Value, Self::Error> {
            unreachable!()
        }

        fn logical(&self, _: &QueryExpr) -> Result<Self::Value, Self::Error> {
            Ok(SummaryValue::Points(
                vec![(10, 2.0), (20, 3.0), (30, 1.0)],
                Some((0, 40)),
            ))
        }
    }

    fn schema(fields: &[(&str, DataType)]) -> SummarySchema {
        SummarySchema {
            fields: fields
                .iter()
                .map(|(name, dtype)| SummaryField {
                    name: (*name).into(),
                    dtype: SummaryFamilyType::Plain(dtype.clone()),
                    nullable: false,
                })
                .collect(),
            time_index: fields
                .iter()
                .position(|(_, dtype)| *dtype == DataType::Timestamp),
        }
    }

    fn value_node(
        child: Rc<SummaryNode>,
        operation: ValueOperation,
        schema: SummarySchema,
    ) -> Rc<SummaryNode> {
        Rc::new(SummaryNode {
            expr: SummaryExpr::ValueOperation {
                child,
                operation,
                timing: ExecutionTiming::ReadTime,
            },
            schema,
            guarantee: None,
        })
    }

    #[test]
    fn executes_filter_project_arithmetic_sort_and_limit_chain() {
        let input_schema = schema(&[("ts", DataType::Timestamp), ("sum", DataType::Float64)]);
        let leaf = Rc::new(SummaryNode {
            expr: SummaryExpr::KeepPreAsap(Rc::new(QueryExpr::Literal(ScalarValue::Int64(0)))),
            schema: input_schema.clone(),
            guarantee: None,
        });
        let projected_schema = schema(&[
            ("bucket", DataType::Timestamp),
            ("score", DataType::Float64),
        ]);
        let projected = value_node(
            leaf,
            ValueOperation::Project {
                cols: vec![
                    ProjectItem {
                        alias: Some("bucket".into()),
                        expr: QueryExpr::Column(0),
                    },
                    ProjectItem {
                        alias: Some("score".into()),
                        expr: QueryExpr::Arithmetic {
                            op: ArithmeticOpKind::Mul,
                            left: Rc::new(QueryExpr::Column(1)),
                            right: Rc::new(QueryExpr::Literal(ScalarValue::Float64(10.0))),
                        },
                    },
                ],
                qualifier: None,
            },
            projected_schema.clone(),
        );
        let sorted = value_node(
            projected,
            ValueOperation::Sort {
                keys: vec![SortKey {
                    expr: QueryExpr::Column(1),
                    ascending: false,
                    nulls_first: false,
                }],
                partition_by: GroupKeys::none(),
            },
            projected_schema.clone(),
        );
        let limited = value_node(
            sorted,
            ValueOperation::Limit { n: 1, offset: 0 },
            projected_schema,
        );

        let relation = execute_relational(&limited, &MockExecutor, &ClickHouseRelationalAdapter)
            .expect("supported SQL chain should execute");
        assert_eq!(relation.coverage, Some((0, 40)));
        let result = relation.into_result().unwrap();
        let batch = &result.batches[0];
        assert_eq!(batch.schema().field(0).name(), "bucket");
        assert_eq!(batch.schema().field(1).name(), "score");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            20
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            30.0
        );
    }

    #[test]
    fn unsupported_scalar_expression_fails_closed() {
        let row = vec![Cell::Float64(1.0)];
        let error = eval(&QueryExpr::BoolAnd(vec![]), &row).unwrap_err();
        assert!(matches!(error, ClickHouseRelationalError::Unsupported(_)));
    }

    #[test]
    fn inner_equi_join_feeds_typed_ratio_projection() {
        let side_schema = schema(&[("service", DataType::Utf8), ("value", DataType::Float64)]);
        let left = ClickHouseRelation {
            rows: vec![vec![Cell::Utf8("api".into()), Cell::Float64(2.0)]],
            fields: fields_from_schema(&side_schema),
            coverage: Some((300_000, 600_000)),
        };
        let right = ClickHouseRelation {
            rows: vec![vec![Cell::Utf8("api".into()), Cell::Float64(10.0)]],
            fields: fields_from_schema(&side_schema),
            coverage: Some((300_000, 600_000)),
        };
        let joined_schema = schema(&[
            ("service", DataType::Utf8),
            ("left_value", DataType::Float64),
            ("service", DataType::Utf8),
            ("right_value", DataType::Float64),
        ]);
        let pred = planner_types::pre_asap::Predicate(Rc::new(QueryExpr::Compare {
            left: Rc::new(QueryExpr::Column(0)),
            op: CompareOpKind::Eq,
            right: Rc::new(QueryExpr::Column(2)),
        }));
        let joined = ClickHouseRelationalAdapter
            .apply_inner_equi_join(&pred, &joined_schema, left, right)
            .unwrap();
        let output_schema = schema(&[("service", DataType::Utf8), ("ratio", DataType::Float64)]);
        let projected = ClickHouseRelationalAdapter
            .apply_operation(
                &ValueOperation::Project {
                    cols: vec![
                        ProjectItem {
                            alias: Some("service".into()),
                            expr: QueryExpr::Column(0),
                        },
                        ProjectItem {
                            alias: Some("ratio".into()),
                            expr: QueryExpr::Arithmetic {
                                op: ArithmeticOpKind::Div,
                                left: Rc::new(QueryExpr::Column(1)),
                                right: Rc::new(QueryExpr::Column(3)),
                            },
                        },
                    ],
                    qualifier: None,
                },
                &output_schema,
                joined,
            )
            .unwrap();
        assert_eq!(projected.coverage, Some((300_000, 600_000)));
        let result = projected.into_result().unwrap();
        let batch = &result.batches[0];
        assert_eq!(batch.schema().field(1).name(), "ratio");
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            0.2
        );
    }
}
