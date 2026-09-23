//! ClickHouse row semantics for planner-owned relational wrappers.

#[cfg(test)]
mod aggregate;
mod collection;
pub(super) mod native;

use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, MapArray, NullArray, StringArray,
        StructArray, TimestampMillisecondArray,
    },
    datatypes::{DataType as ArrowDataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDateTime, TimeZone};
use planner_types::{
    post_asap::{SummaryFamilyType, SummarySchema, ValueOperation},
    pre_asap::DataType,
};

use super::clickhouse_result_adapter::ClickHouseQueryResult;
#[cfg(test)]
use planner_types::pre_asap::{ArithmeticOpKind, CompareOpKind, QueryExpr, ScalarValue, SortKey};

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
    Map(Vec<(Cell, Cell)>),
    List(Arc<[Cell]>),
    Struct(Arc<[Cell]>),
}

fn json_cell(
    value: &serde_json::Value,
    dtype: &DataType,
    nullable: bool,
    clickhouse_type: &str,
) -> Result<Cell, ClickHouseRelationalError> {
    if value.is_null() && nullable {
        return Ok(Cell::Null);
    }
    let invalid = || {
        ClickHouseRelationalError::Invalid(format!(
            "external value {value} does not match {dtype:?}"
        ))
    };
    match dtype {
        DataType::Interval | DataType::Date => Err(ClickHouseRelationalError::Unsupported(
            "temporal value transport".into(),
        )),
        DataType::Null if value.is_null() => Ok(Cell::Null),
        DataType::Null => Err(invalid()),
        DataType::List { element } => {
            let item_type = clickhouse_type
                .strip_prefix("Array(")
                .and_then(|inner| inner.strip_suffix(')'))
                .ok_or_else(invalid)?;
            let items = value.as_array().ok_or_else(invalid)?;
            Ok(Cell::List(
                items
                    .iter()
                    .map(|item| json_cell(item, &element.dtype, element.nullable, item_type))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            ))
        }
        DataType::Struct { fields } => {
            let types =
                collection::tuple_field_types(clickhouse_type, fields).ok_or_else(invalid)?;
            let items = value
                .as_array()
                .filter(|items| items.len() == fields.len())
                .ok_or_else(invalid)?;
            Ok(Cell::Struct(
                items
                    .iter()
                    .zip(fields)
                    .zip(types)
                    .map(|((item, field), native)| {
                        json_cell(item, &field.dtype, field.nullable, native)
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
            ))
        }
        DataType::Int64 => value.as_i64().map(Cell::Int64).ok_or_else(invalid),
        DataType::Float64 => value.as_f64().map(Cell::Float64).ok_or_else(invalid),
        DataType::Utf8 => value
            .as_str()
            .map(|value| Cell::Utf8(value.into()))
            .ok_or_else(invalid),
        DataType::Bool => value.as_bool().map(Cell::Bool).ok_or_else(invalid),
        DataType::Map {
            key,
            value: value_type,
            value_nullable,
        } => {
            let (key_type, item_type) = map_type_parts(clickhouse_type).ok_or_else(invalid)?;
            let entries = value.as_array().ok_or_else(invalid)?;
            let mut result = Vec::with_capacity(entries.len());
            for entry in entries {
                let pair = entry
                    .as_array()
                    .filter(|pair| pair.len() == 2)
                    .ok_or_else(invalid)?;
                result.push((
                    json_cell(&pair[0], key, false, key_type)?,
                    json_cell(&pair[1], value_type, *value_nullable, item_type)?,
                ));
            }
            Ok(Cell::Map(result))
        }
        DataType::Timestamp => parse_clickhouse_timestamp(value, clickhouse_type)
            .map(Cell::Timestamp)
            .ok_or_else(invalid),
    }
}

fn parse_clickhouse_timestamp(value: &serde_json::Value, clickhouse_type: &str) -> Option<i64> {
    let clickhouse_type = clickhouse_type
        .strip_prefix("Nullable(")
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(clickhouse_type);
    if clickhouse_type == "Int64" {
        return value.as_i64();
    }
    let text = value.as_str()?;
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(text) {
        return Some(timestamp.timestamp_millis());
    }
    let (scale, timezone) = if clickhouse_type == "DateTime" {
        (0, "UTC")
    } else if let Some(timezone) = clickhouse_type
        .strip_prefix("DateTime(")
        .and_then(|value| value.strip_suffix(')'))
    {
        (0, timezone.trim().trim_matches('\''))
    } else {
        let args = clickhouse_type
            .strip_prefix("DateTime64(")?
            .strip_suffix(')')?;
        let mut args = args.split(',').map(str::trim);
        let scale = args.next()?.parse::<usize>().ok()?;
        if scale > 9 {
            return None;
        }
        let timezone = args.next().unwrap_or("UTC").trim_matches('\'');
        if args.next().is_some() {
            return None;
        }
        (scale, timezone)
    };
    let fraction_digits = text
        .split_once('.')
        .map(|(_, fraction)| fraction.len())
        .unwrap_or(0);
    if fraction_digits != scale {
        return None;
    }
    let naive = NaiveDateTime::parse_from_str(
        text,
        if scale == 0 {
            "%Y-%m-%d %H:%M:%S"
        } else {
            "%Y-%m-%d %H:%M:%S%.f"
        },
    )
    .ok()?;
    let timezone: chrono_tz::Tz = timezone.parse().ok()?;
    timezone
        .from_local_datetime(&naive)
        .single()
        .map(|value| value.timestamp_millis())
}

#[derive(Clone, Debug)]
pub struct ClickHouseRelation {
    rows: Vec<Vec<Cell>>,
    fields: Vec<(String, DataType, bool)>,
    pub coverage: Option<(u64, u64)>,
}

impl ClickHouseRelation {
    pub fn from_json_compact(
        schema: &SummarySchema,
        body: &[u8],
    ) -> Result<Self, ClickHouseRelationalError> {
        let document: serde_json::Value = serde_json::from_slice(body)
            .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?;
        let meta = document
            .get("meta")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ClickHouseRelationalError::Invalid(
                    "ClickHouse JSONCompact response has no typed metadata".into(),
                )
            })?;
        let data = document
            .get("data")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                ClickHouseRelationalError::Invalid(
                    "ClickHouse JSONCompact response has no data rows".into(),
                )
            })?;
        let fields = fields_from_schema(schema);
        if meta.len() != fields.len()
            || meta
                .iter()
                .zip(&fields)
                .any(|(actual, (name, dtype, nullable))| {
                    actual.get("name").and_then(serde_json::Value::as_str) != Some(name)
                        || !clickhouse_type_matches(
                            actual.get("type").and_then(serde_json::Value::as_str),
                            dtype,
                            *nullable,
                        )
                })
        {
            return Err(ClickHouseRelationalError::Invalid(
                "ClickHouse external metadata differs from its planned schema".into(),
            ));
        }
        let mut rows = Vec::with_capacity(data.len());
        for encoded in data {
            let encoded = encoded.as_array().ok_or_else(|| {
                ClickHouseRelationalError::Invalid(
                    "ClickHouse JSONCompact row is not an array".into(),
                )
            })?;
            if encoded.len() != fields.len() {
                return Err(ClickHouseRelationalError::Invalid(
                    "ClickHouse external row differs from its planned schema".into(),
                ));
            }
            rows.push(
                encoded
                    .iter()
                    .zip(&fields)
                    .zip(meta)
                    .map(|((value, (_, dtype, nullable)), metadata)| {
                        json_cell(
                            value,
                            dtype,
                            *nullable,
                            metadata["type"].as_str().expect("metadata validated"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(Self {
            rows,
            fields,
            coverage: None,
        })
    }

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

fn map_type_parts(actual: &str) -> Option<(&str, &str)> {
    let inner = actual.trim().strip_prefix("Map(")?.strip_suffix(')')?;
    let args = collection::arguments(inner)?;
    let [key, value] = args.as_slice() else {
        return None;
    };
    Some((*key, *value))
}

fn clickhouse_type_matches(actual: Option<&str>, expected: &DataType, nullable: bool) -> bool {
    let Some(mut actual) = actual else {
        return false;
    };
    if nullable {
        let Some(inner) = actual
            .strip_prefix("Nullable(")
            .and_then(|value| value.strip_suffix(')'))
        else {
            return false;
        };
        actual = inner;
    } else if actual.starts_with("Nullable(") {
        return false;
    }
    match expected {
        DataType::Interval | DataType::Date => false,
        DataType::Null => actual == "Nothing",
        DataType::List { element } => {
            !nullable
                && actual
                    .strip_prefix("Array(")
                    .and_then(|inner| inner.strip_suffix(')'))
                    .is_some_and(|inner| {
                        clickhouse_type_matches(Some(inner), &element.dtype, element.nullable)
                    })
        }
        DataType::Struct { fields } => {
            !nullable && collection::tuple_field_types(actual, fields).is_some()
        }
        DataType::Int64 => actual == "Int64",
        DataType::Float64 => actual == "Float64",
        DataType::Utf8 => actual == "String",
        DataType::Bool => actual == "Bool",
        DataType::Map {
            key,
            value,
            value_nullable,
        } => map_type_parts(actual).is_some_and(|(key_type, value_type)| {
            clickhouse_type_matches(Some(key_type), key, false)
                && clickhouse_type_matches(Some(value_type), value, *value_nullable)
        }),
        DataType::Timestamp => {
            actual == "Int64"
                || actual == "DateTime"
                || actual.starts_with("DateTime(")
                || actual.starts_with("DateTime64(")
        }
    }
}

pub struct ClickHouseRelationalAdapter;

impl ClickHouseRelationalAdapter {
    pub fn apply_join(
        &self,
        kind: &planner_types::pre_asap::JoinKind,
        pred: &planner_types::pre_asap::Predicate,
        output_schema: &SummarySchema,
        left: ClickHouseRelation,
        right: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        native::execute(
            planner_types::post_asap::ExecutableOperatorPayload::RelationalJoin {
                join_kind: kind.clone(),
                pred: pred.clone(),
                pruning: None,
            },
            output_schema,
            vec![left, right],
        )
    }

    pub fn apply_filter(
        &self,
        pred: &planner_types::pre_asap::Predicate,
        input: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        let schema = native::schema(&input);
        self.apply_operation(
            &ValueOperation::Filter { pred: pred.clone() },
            &schema,
            input,
        )
    }

    pub fn apply_operation(
        &self,
        operation: &ValueOperation,
        output_schema: &SummarySchema,
        input: ClickHouseRelation,
    ) -> Result<ClickHouseRelation, ClickHouseRelationalError> {
        native::execute(
            planner_types::post_asap::ExecutableOperatorPayload::Value {
                operation: operation.clone(),
            },
            output_schema,
            vec![input],
        )
    }
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
                let encoded = asap_types::grouping_projection::decode_table_group_value(value)
                    .map_err(|error| {
                        ClickHouseRelationalError::Invalid(format!(
                            "invalid typed group {name}: {error}"
                        ))
                    })?;
                let column_type = super::clickhouse_result_adapter::clickhouse_type(
                    &arrow_type(dtype),
                    *nullable,
                );
                return json_cell(&encoded, dtype, *nullable, &column_type);
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

#[cfg(test)]
fn eval(
    expr: &QueryExpr,
    row: &[Cell],
    schema: &planner_types::pre_asap::Schema,
) -> Result<Cell, ClickHouseRelationalError> {
    let relation = ClickHouseRelation {
        fields: schema
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.dtype.clone(), c.nullable))
            .collect(),
        rows: vec![],
        coverage: None,
    };
    let compiled = asap_physical_operators::dag::expressions::CompiledExpression::compile(
        expr,
        &Arc::new(native::schema(&relation)),
    )
    .map_err(|error| ClickHouseRelationalError::Unsupported(error.to_string()))?;
    let value = compiled
        .evaluate(&row.iter().map(native::value).collect::<Vec<_>>())
        .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?;
    native::cell(&value)
}
#[cfg(test)]
fn arithmetic(
    op: &ArithmeticOpKind,
    left: Cell,
    right: Cell,
) -> Result<Cell, ClickHouseRelationalError> {
    let dtype = |value: &Cell| match value {
        Cell::Int64(_) => DataType::Int64,
        _ => DataType::Float64,
    };
    let schema = planner_types::pre_asap::Schema::new(vec![
        planner_types::pre_asap::Column::new("left", dtype(&left), false),
        planner_types::pre_asap::Column::new("right", dtype(&right), false),
    ]);
    eval(
        &QueryExpr::Arithmetic {
            op: op.clone(),
            left: std::rc::Rc::new(QueryExpr::Column(0)),
            right: std::rc::Rc::new(QueryExpr::Column(1)),
        },
        &[left, right],
        &schema,
    )
}

fn arrow_type(dtype: &DataType) -> ArrowDataType {
    match dtype {
        DataType::Date => ArrowDataType::Date32,
        DataType::Interval => ArrowDataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
        DataType::Null => ArrowDataType::Null,
        DataType::List { element } => ArrowDataType::List(Arc::new(Field::new(
            &element.name,
            arrow_type(&element.dtype),
            element.nullable,
        ))),
        DataType::Struct { fields } => ArrowDataType::Struct(
            fields
                .iter()
                .map(|field| Field::new(&field.name, arrow_type(&field.dtype), field.nullable))
                .collect::<Vec<_>>()
                .into(),
        ),
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Utf8 => ArrowDataType::Utf8,
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Map {
            key,
            value,
            value_nullable,
        } => ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", arrow_type(key), false),
                        Field::new("value", arrow_type(value), *value_nullable),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
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
        DataType::Interval | DataType::Date => {
            return Err(ClickHouseRelationalError::Unsupported(
                "temporal value transport".into(),
            ))
        }
        DataType::Null => {
            if rows
                .iter()
                .any(|row| !matches!(row.get(column), Some(Cell::Null)))
            {
                return Err(ClickHouseRelationalError::Invalid(
                    "non-null value in bottom-typed column".into(),
                ));
            }
            Arc::new(NullArray::new(rows.len())) as ArrayRef
        }
        DataType::List { .. } | DataType::Struct { .. } => {
            return Err(ClickHouseRelationalError::Unsupported(
                "collection value transport".into(),
            ))
        }
        DataType::Int64 => Arc::new(Int64Array::from(values!(Int64))) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(values!(Float64))) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from(values!(Utf8))) as ArrayRef,
        DataType::Bool => Arc::new(BooleanArray::from(values!(Bool))) as ArrayRef,
        DataType::Map { key, value, .. } => {
            let mut offsets = vec![0_i32];
            let mut valid = Vec::with_capacity(rows.len());
            let mut entries = Vec::new();
            for row in rows {
                match row.get(column) {
                    Some(Cell::Map(pairs)) => {
                        valid.push(true);
                        entries.extend(
                            pairs
                                .iter()
                                .map(|(key, value)| vec![key.clone(), value.clone()]),
                        );
                    }
                    Some(Cell::Null) => valid.push(false),
                    _ => {
                        return Err(ClickHouseRelationalError::Invalid(
                            "incompatible map value".into(),
                        ))
                    }
                }
                offsets.push(i32::try_from(entries.len()).map_err(|_| {
                    ClickHouseRelationalError::Invalid("map offset exceeds Arrow limit".into())
                })?);
            }
            let ArrowDataType::Map(field, ordered) = arrow_type(dtype) else {
                unreachable!()
            };
            let ArrowDataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            let values = StructArray::try_new(
                fields.clone(),
                vec![
                    build_array(&entries, 0, key)?,
                    build_array(&entries, 1, value)?,
                ],
                None,
            )
            .map_err(|error| ClickHouseRelationalError::Arrow(error.to_string()))?;
            Arc::new(
                MapArray::try_new(
                    field,
                    arrow::buffer::OffsetBuffer::new(offsets.into()),
                    values,
                    Some(arrow::buffer::NullBuffer::from(valid)),
                    ordered,
                )
                .map_err(|error| ClickHouseRelationalError::Arrow(error.to_string()))?,
            ) as ArrayRef
        }
        DataType::Timestamp => {
            Arc::new(TimestampMillisecondArray::from(values!(Timestamp))) as ArrayRef
        }
    })
}

#[cfg(test)]
mod scalar_contract_tests {
    use super::*;
    use planner_types::pre_asap::{Column, Schema};

    fn function(name: &str, args: Vec<QueryExpr>) -> QueryExpr {
        QueryExpr::FunctionCall {
            name: name.into(),
            args,
        }
    }
    fn text(value: &str) -> QueryExpr {
        QueryExpr::Literal(ScalarValue::Utf8(value.into()))
    }

    #[test]
    fn map_access_uses_declared_default_and_first_duplicate() {
        let dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Int64),
            value_nullable: false,
        };
        let schema = Schema::new(vec![Column::new("m", dtype, false)]);
        let access = function("asap_map_access", vec![QueryExpr::Column(0), text("a")]);
        assert_eq!(
            eval(&access, &[Cell::Map(vec![])], &schema).unwrap(),
            Cell::Int64(0)
        );
        assert_eq!(
            eval(
                &access,
                &[Cell::Map(vec![
                    (Cell::Utf8("a".into()), Cell::Int64(7)),
                    (Cell::Utf8("a".into()), Cell::Int64(9))
                ])],
                &schema
            )
            .unwrap(),
            Cell::Int64(7)
        );
        let nullable = Schema::new(vec![Column::new(
            "m",
            DataType::Map {
                key: Box::new(DataType::Utf8),
                value: Box::new(DataType::Int64),
                value_nullable: true,
            },
            false,
        )]);
        assert_eq!(
            eval(&access, &[Cell::Map(vec![])], &nullable).unwrap(),
            Cell::Null
        );
        let null_key = function(
            "asap_map_access",
            vec![QueryExpr::Column(0), QueryExpr::Literal(ScalarValue::Null)],
        );
        assert_eq!(
            eval(&null_key, &[Cell::Map(vec![])], &schema).unwrap(),
            Cell::Null
        );
    }

    #[test]
    fn map_concat_preserves_duplicates_and_empty_map() {
        let map = |value| {
            function(
                "map",
                vec![text("a"), QueryExpr::Literal(ScalarValue::Int64(value))],
            )
        };
        let concat = function("mapConcat", vec![function("map", vec![]), map(7), map(9)]);
        let schema = Schema::new(vec![]);
        assert_eq!(
            eval(&concat, &[], &schema).unwrap(),
            Cell::Map(vec![
                (Cell::Utf8("a".into()), Cell::Int64(7)),
                (Cell::Utf8("a".into()), Cell::Int64(9))
            ])
        );
        let mixed = function(
            "map",
            vec![
                text("a"),
                QueryExpr::Literal(ScalarValue::Int64(1)),
                text("b"),
                QueryExpr::Literal(ScalarValue::Float64(2.5)),
            ],
        );
        assert!(eval(&mixed, &[], &schema).is_err());
    }

    #[test]
    fn sorting_nested_nan_fails_before_comparator_can_treat_it_as_equal() {
        let dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Float64),
            value_nullable: false,
        };
        let input = ClickHouseRelation {
            rows: vec![vec![Cell::Map(vec![(
                Cell::Utf8("a".into()),
                Cell::Float64(f64::NAN),
            )])]],
            fields: vec![("m".into(), dtype.clone(), false)],
            coverage: None,
        };
        let schema = SummarySchema {
            fields: vec![planner_types::post_asap::SummaryField {
                name: "m".into(),
                dtype: SummaryFamilyType::Plain(dtype),
                nullable: false,
            }],
            time_index: None,
        };
        let operation = ValueOperation::Sort {
            keys: vec![SortKey {
                expr: QueryExpr::Column(0),
                ascending: true,
                nulls_first: false,
            }],
            partition_by: planner_types::pre_asap::GroupKeys::none(),
        };
        assert!(ClickHouseRelationalAdapter
            .apply_operation(&operation, &schema, input)
            .is_err());
    }

    #[test]
    fn integer_modulo_never_rounds_through_float() {
        assert_eq!(
            arithmetic(
                &ArithmeticOpKind::Mod,
                Cell::Int64(9_007_199_254_740_993),
                Cell::Int64(2)
            )
            .unwrap(),
            Cell::Int64(1)
        );
        assert_eq!(
            arithmetic(&ArithmeticOpKind::Mod, Cell::Int64(-7), Cell::Int64(3)).unwrap(),
            Cell::Int64(-1)
        );
        assert!(arithmetic(&ArithmeticOpKind::Mod, Cell::Int64(7), Cell::Int64(0)).is_err());
        assert!(arithmetic(
            &ArithmeticOpKind::Mod,
            Cell::Int64(i64::MIN),
            Cell::Int64(-1)
        )
        .is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use planner_types::{
        post_asap::{SummaryField, SummarySchema},
        pre_asap::{GroupKeys, Predicate, ProjectItem},
    };
    use std::rc::Rc;

    #[test]
    fn decodes_declared_array_elements_without_losing_nullability() {
        use planner_types::pre_asap::Column;
        let dtype = DataType::List {
            element: Box::new(Column {
                name: "item".into(),
                dtype: DataType::Int64,
                nullable: true,
                table: None,
            }),
        };
        assert!(clickhouse_type_matches(
            Some("Array(Nullable(Int64))"),
            &dtype,
            false
        ));
        assert!(!clickhouse_type_matches(
            Some("Array(Int64)"),
            &dtype,
            false
        ));
        assert!(!clickhouse_type_matches(
            Some("Nullable(Array(Nullable(Int64)))"),
            &dtype,
            true
        ));
        let value = json_cell(
            &serde_json::json!([9007199254740993_i64, null, -7]),
            &dtype,
            false,
            "Array(Nullable(Int64))",
        )
        .unwrap();
        let Cell::List(items) = &value else {
            panic!("expected list")
        };
        assert_eq!(
            items.as_ref(),
            &[Cell::Int64(9007199254740993), Cell::Null, Cell::Int64(-7)]
        );
        let Cell::List(copy) = value.clone() else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(items, &copy));
        assert!(json_cell(
            &serde_json::json!(["wrong"]),
            &dtype,
            false,
            "Array(Nullable(Int64))"
        )
        .is_err());
    }

    #[test]
    fn nonfinite_external_array_values_cannot_become_nulls() {
        use planner_types::pre_asap::Column;
        let dtype = DataType::List {
            element: Box::new(Column::new("item", DataType::Float64, true)),
        };
        for value in ["inf", "-inf", "nan"] {
            assert!(json_cell(
                &serde_json::json!(value),
                &DataType::Float64,
                true,
                "Nullable(Float64)"
            )
            .is_err());
            assert!(json_cell(
                &serde_json::json!([value]),
                &dtype,
                false,
                "Array(Nullable(Float64))"
            )
            .is_err());
        }
        assert_eq!(
            json_cell(
                &serde_json::json!([null]),
                &dtype,
                false,
                "Array(Nullable(Float64))"
            )
            .unwrap(),
            Cell::List(vec![Cell::Null].into())
        );
    }

    #[test]
    fn array_access_uses_signed_indices_and_element_defaults() {
        use planner_types::pre_asap::{Column, Schema};
        let function = |name: &str, args| QueryExpr::FunctionCall {
            name: name.into(),
            args,
        };
        let dtype = DataType::List {
            element: Box::new(Column::new("item", DataType::Int64, false)),
        };
        let schema = Schema::new(vec![
            Column::new("items", dtype, false),
            Column::new("index", DataType::Int64, true),
        ]);
        let access = function(
            "asap_element_access",
            vec![QueryExpr::Column(0), QueryExpr::Column(1)],
        );
        let items = Cell::List(vec![Cell::Int64(10), Cell::Int64(20)].into());
        for (index, expected) in [
            (1, 10),
            (2, 20),
            (-1, 20),
            (-2, 10),
            (0, 0),
            (3, 0),
            (i64::MIN, 0),
            (i64::MAX, 0),
        ] {
            assert_eq!(
                eval(&access, &[items.clone(), Cell::Int64(index)], &schema).unwrap(),
                Cell::Int64(expected)
            );
        }
        assert_eq!(
            eval(&access, &[items, Cell::Null], &schema).unwrap(),
            Cell::Null
        );
        let zero = function(
            "asap_element_access",
            vec![
                QueryExpr::Column(0),
                QueryExpr::Literal(ScalarValue::Int64(0)),
            ],
        );
        assert!(eval(&zero, &[Cell::List(Arc::from([])), Cell::Int64(0)], &schema).is_err());
    }

    #[test]
    fn nested_array_tuple_access_preserves_fields_and_defaults() {
        use planner_types::pre_asap::{Column, Schema};
        let tuple = DataType::Struct {
            fields: vec![
                Column::new("ts", DataType::Int64, false),
                Column::new("value", DataType::Float64, true),
            ],
        };
        let dtype = DataType::List {
            element: Box::new(Column::new("item", tuple, false)),
        };
        let native = "Array(Tuple(ts Int64, value Nullable(Float64)))";
        assert!(clickhouse_type_matches(Some(native), &dtype, false));
        let samples = json_cell(
            &serde_json::json!([[9007199254740993_i64, 2.5], [7, null]]),
            &dtype,
            false,
            native,
        )
        .unwrap();
        let schema = Schema::new(vec![Column::new("samples", dtype, false)]);
        let field = |index, name: &str| QueryExpr::FunctionCall {
            name: "asap_struct_field".into(),
            args: vec![
                QueryExpr::FunctionCall {
                    name: "asap_element_access".into(),
                    args: vec![
                        QueryExpr::Column(0),
                        QueryExpr::Literal(ScalarValue::Int64(index)),
                    ],
                },
                QueryExpr::Literal(ScalarValue::Utf8(name.into())),
            ],
        };
        assert_eq!(
            eval(&field(1, "ts"), std::slice::from_ref(&samples), &schema).unwrap(),
            Cell::Int64(9007199254740993)
        );
        assert_eq!(
            eval(&field(1, "value"), std::slice::from_ref(&samples), &schema).unwrap(),
            Cell::Float64(2.5)
        );
        assert_eq!(
            eval(&field(-1, "value"), std::slice::from_ref(&samples), &schema).unwrap(),
            Cell::Null
        );
        assert_eq!(
            eval(&field(99, "ts"), std::slice::from_ref(&samples), &schema).unwrap(),
            Cell::Int64(0)
        );
        assert_eq!(
            eval(&field(99, "value"), std::slice::from_ref(&samples), &schema).unwrap(),
            Cell::Null
        );
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

    /// Map entries remain ordered pairs, including duplicate keys and null values.
    #[test]
    fn map_transport_retains_duplicate_keys_and_null_values() {
        use super::super::clickhouse_result_adapter::{ClickHouseFormat, ClickHouseQueryResult};
        let dtype = DataType::Map {
            key: Box::new(DataType::Utf8),
            value: Box::new(DataType::Utf8),
            value_nullable: true,
        };
        let cell = json_cell(
            &serde_json::json!([["job", "a"], ["job", "b"], ["zone", null]]),
            &dtype,
            false,
            "Map(String, Nullable(String))",
        )
        .unwrap();
        let Cell::Map(entries) = &cell else {
            panic!("expected map")
        };
        assert_eq!(entries.len(), 3);
        let array = build_array(&[vec![cell]], 0, &dtype).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "labels",
                arrow_type(&dtype),
                false,
            )])),
            vec![array],
        )
        .unwrap();
        let result = ClickHouseQueryResult {
            batches: vec![batch],
        };
        let body = String::from_utf8(result.encode(ClickHouseFormat::Json).unwrap()).unwrap();
        assert!(body.contains("Map(String, Nullable(String))"), "{body}");
        assert!(body.contains("\"job\":\"a\",\"job\":\"b\""), "{body}");
        assert!(body.contains("\"zone\":null"), "{body}");
        assert_eq!(
            String::from_utf8(result.encode(ClickHouseFormat::TabSeparated).unwrap()).unwrap(),
            "{'job':'a','job':'b','zone':NULL}\n"
        );
    }

    #[test]
    fn executes_filter_project_arithmetic_sort_and_limit_chain() {
        let input_schema = schema(&[("ts", DataType::Timestamp), ("sum", DataType::Float64)]);
        let adapter = ClickHouseRelationalAdapter;
        let input = ClickHouseRelation {
            rows: vec![
                vec![Cell::Timestamp(10), Cell::Float64(2.0)],
                vec![Cell::Timestamp(20), Cell::Float64(3.0)],
                vec![Cell::Timestamp(30), Cell::Float64(1.0)],
            ],
            fields: fields_from_schema(&input_schema),
            coverage: Some((0, 40)),
        };
        let mut relation = adapter
            .apply_filter(
                &Predicate(Rc::new(QueryExpr::Compare {
                    left: Rc::new(QueryExpr::Column(1)),
                    op: CompareOpKind::Gt,
                    right: Rc::new(QueryExpr::Literal(ScalarValue::Float64(1.0))),
                })),
                input,
            )
            .unwrap();
        assert_eq!(relation.rows.len(), 2);
        let projected_schema = schema(&[
            ("bucket", DataType::Timestamp),
            ("score", DataType::Float64),
        ]);
        for operation in [
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
            ValueOperation::Sort {
                keys: vec![SortKey {
                    expr: QueryExpr::Column(1),
                    ascending: false,
                    nulls_first: false,
                }],
                partition_by: GroupKeys::none(),
            },
            ValueOperation::Limit {
                n: 1,
                offset: 0,
                partition_by: planner_types::pre_asap::GroupKeys::none(),
            },
        ] {
            relation = adapter
                .apply_operation(&operation, &projected_schema, relation)
                .expect("installed SQL operators should execute");
        }
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
        let error = eval(
            &QueryExpr::FunctionCall {
                name: "unsupported_function".into(),
                args: vec![],
            },
            &row,
            &planner_types::pre_asap::Schema::new(vec![]),
        )
        .unwrap_err();
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
            .apply_join(
                &planner_types::pre_asap::JoinKind::Inner,
                &pred,
                &joined_schema,
                left,
                right,
            )
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

    // Every standardized Planner join kind has concrete row semantics.
    #[test]
    fn executes_all_post_asap_relational_join_kinds() {
        use planner_types::pre_asap::JoinKind;

        let side_schema = schema(&[("key", DataType::Int64)]);
        let relation = |values: &[i64]| ClickHouseRelation {
            rows: values
                .iter()
                .map(|value| vec![Cell::Int64(*value)])
                .collect(),
            fields: fields_from_schema(&side_schema),
            coverage: Some((0, 10)),
        };
        let mut joined_schema = schema(&[("left", DataType::Int64), ("right", DataType::Int64)]);
        for field in &mut joined_schema.fields {
            field.nullable = true;
        }
        let pred = Predicate(Rc::new(QueryExpr::Compare {
            left: Rc::new(QueryExpr::Column(0)),
            op: CompareOpKind::Eq,
            right: Rc::new(QueryExpr::Column(1)),
        }));
        let adapter = ClickHouseRelationalAdapter;
        let execute = |kind, output_schema: &SummarySchema| {
            adapter
                .apply_join(
                    &kind,
                    &pred,
                    output_schema,
                    relation(&[1, 2]),
                    relation(&[2, 3]),
                )
                .unwrap()
                .rows
        };

        assert_eq!(
            execute(JoinKind::Inner, &joined_schema),
            vec![vec![Cell::Int64(2), Cell::Int64(2)]]
        );
        assert_eq!(execute(JoinKind::Left, &joined_schema).len(), 2);
        assert_eq!(execute(JoinKind::Right, &joined_schema).len(), 2);
        assert_eq!(execute(JoinKind::Full, &joined_schema).len(), 3);
        assert_eq!(execute(JoinKind::Cross, &joined_schema).len(), 4);
        assert_eq!(
            execute(JoinKind::Semi, &side_schema),
            vec![vec![Cell::Int64(2)]]
        );
        assert_eq!(
            execute(JoinKind::Anti, &side_schema),
            vec![vec![Cell::Int64(1)]]
        );
    }
}
