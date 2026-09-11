//! ClickHouse row semantics for planner-owned relational wrappers.

mod aggregate;
mod collection;

use std::{cmp::Ordering, collections::BTreeMap, sync::Arc};

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
    pre_asap::{ArithmeticOpKind, CompareOpKind, DataType, QueryExpr, ScalarValue, SortKey},
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
        let mut fields = left.fields.clone();
        fields.extend(right.fields.clone());
        let schema = scalar_schema(&fields);
        let mut rows = Vec::new();
        for left_row in &left.rows {
            for right_row in &right.rows {
                let mut joined = Vec::with_capacity(left_row.len() + right_row.len());
                joined.extend(left_row.iter().cloned());
                joined.extend(right_row.iter().cloned());
                if matches!(eval(&pred.0, &joined, &schema)?, Cell::Bool(true)) {
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
        let schema = scalar_schema(&input.fields);
        input.rows = input
            .rows
            .into_iter()
            .filter_map(|row| match eval(&pred.0, &row, &schema) {
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
        let schema = scalar_schema(&input.fields);
        match operation {
            ValueOperation::Exact(planner_types::post_asap::ExactOperation::Aggregate {
                reduction,
                measures,
                having,
                ..
            }) => {
                return aggregate::apply(
                    reduction,
                    measures,
                    having.as_ref(),
                    output_schema,
                    input,
                );
            }
            ValueOperation::Project { cols, .. } => {
                let mut rows = Vec::with_capacity(input.rows.len());
                for row in &input.rows {
                    rows.push(
                        cols.iter()
                            .map(|item| eval(&item.expr, row, &schema))
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
                        let value = eval(&key.expr, row, &schema)?;
                        if contains_nan(&value) {
                            return Err(ClickHouseRelationalError::Unsupported(
                                "NaN sort key".into(),
                            ));
                        }
                        if !matches!(value, Cell::Null) && cell_cmp(&value, &value).is_none() {
                            return Err(ClickHouseRelationalError::Unsupported(
                                "unsupported sort key value type".into(),
                            ));
                        }
                    }
                }
                input
                    .rows
                    .sort_by(|left, right| compare_sort_keys(left, right, keys, &schema));
            }
            ValueOperation::Limit { n, offset } => {
                input.rows = input.rows.into_iter().skip(*offset).take(*n).collect();
            }
            other => return Err(ClickHouseRelationalError::Unsupported(format!("{other:?}"))),
        }
        Ok(input)
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

fn scalar_schema(fields: &[(String, DataType, bool)]) -> planner_types::pre_asap::Schema {
    planner_types::pre_asap::Schema::new(
        fields
            .iter()
            .map(|(name, dtype, nullable)| {
                planner_types::pre_asap::Column::new(name.clone(), dtype.clone(), *nullable)
            })
            .collect(),
    )
}

fn eval(
    expr: &QueryExpr,
    row: &[Cell],
    schema: &planner_types::pre_asap::Schema,
) -> Result<Cell, ClickHouseRelationalError> {
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
            let left = eval(left, row, schema)?;
            let right = eval(right, row, schema)?;
            compare(op, left, right)
        }
        QueryExpr::Arithmetic { op, left, right } => {
            arithmetic(op, eval(left, row, schema)?, eval(right, row, schema)?)
        }
        QueryExpr::FunctionCall { name, args } => {
            use planner_types::pre_asap::scalar_signature::MapScalarFunction;
            if name.eq_ignore_ascii_case("asap_struct_field") {
                expr.scalar_type(schema)
                    .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?;
                let DataType::Struct { fields } = args[0]
                    .scalar_type(schema)
                    .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?
                    .0
                else {
                    unreachable!()
                };
                let offset = match &args[1] {
                    QueryExpr::Literal(ScalarValue::Int64(index)) => {
                        usize::try_from(index - 1).ok()
                    }
                    QueryExpr::Literal(ScalarValue::Utf8(name)) => {
                        fields.iter().position(|field| &field.name == name)
                    }
                    _ => None,
                }
                .ok_or_else(|| {
                    ClickHouseRelationalError::Invalid("struct field selector".into())
                })?;
                let Cell::Struct(values) = eval(&args[0], row, schema)? else {
                    return Err(ClickHouseRelationalError::Invalid(
                        "struct field input".into(),
                    ));
                };
                return values.get(offset).cloned().ok_or_else(|| {
                    ClickHouseRelationalError::Invalid("struct field value".into())
                });
            }
            if name.eq_ignore_ascii_case("asap_element_access") {
                let (output_type, _) = expr
                    .scalar_type(schema)
                    .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?;
                if let DataType::List { element } = args[0]
                    .scalar_type(schema)
                    .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?
                    .0
                {
                    let Cell::List(values) = eval(&args[0], row, schema)? else {
                        return Err(ClickHouseRelationalError::Invalid(
                            "array access input".into(),
                        ));
                    };
                    let index = match eval(&args[1], row, schema)? {
                        Cell::Null => return Ok(Cell::Null),
                        Cell::Int64(index) => index,
                        _ => {
                            return Err(ClickHouseRelationalError::Invalid(
                                "array access index".into(),
                            ))
                        }
                    };
                    let offset = if index > 0 {
                        usize::try_from(index - 1).ok()
                    } else if index < 0 {
                        usize::try_from(index.unsigned_abs())
                            .ok()
                            .and_then(|distance| values.len().checked_sub(distance))
                    } else {
                        None
                    };
                    return match offset.and_then(|offset| values.get(offset)) {
                        Some(value) => Ok(value.clone()),
                        None => default_collection_element(&output_type, element.nullable),
                    };
                }
            }
            let function = (if name.eq_ignore_ascii_case("asap_element_access") {
                Some(MapScalarFunction::Access)
            } else {
                MapScalarFunction::from_name(name)
            })
            .ok_or_else(|| {
                ClickHouseRelationalError::Unsupported(format!("scalar function {name}"))
            })?;
            expr.scalar_type(schema)
                .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?;
            let values = args
                .iter()
                .map(|arg| eval(arg, row, schema))
                .collect::<Result<Vec<_>, _>>()?;
            match function {
                MapScalarFunction::Construct => {
                    let mut values = values.into_iter();
                    let mut entries = Vec::new();
                    while let Some(key) = values.next() {
                        if !matches!(key, Cell::Int64(_) | Cell::Utf8(_) | Cell::Bool(_)) {
                            return Err(ClickHouseRelationalError::Unsupported(
                                "map key value type".into(),
                            ));
                        }
                        entries.push((
                            key,
                            values.next().ok_or_else(|| {
                                ClickHouseRelationalError::Invalid("odd map argument count".into())
                            })?,
                        ));
                    }
                    Ok(Cell::Map(entries))
                }
                MapScalarFunction::Concat => {
                    let mut entries = Vec::new();
                    for value in values {
                        let Cell::Map(mut next) = value else {
                            return Err(ClickHouseRelationalError::Invalid(
                                "map concat argument".into(),
                            ));
                        };
                        entries.append(&mut next);
                    }
                    Ok(Cell::Map(entries))
                }
                MapScalarFunction::Access => {
                    let [Cell::Map(entries), key] = values.as_slice() else {
                        return Err(ClickHouseRelationalError::Invalid(
                            "map access arguments".into(),
                        ));
                    };
                    if matches!(key, Cell::Null) {
                        return Ok(Cell::Null);
                    }
                    if !matches!(key, Cell::Int64(_) | Cell::Utf8(_) | Cell::Bool(_)) {
                        return Err(ClickHouseRelationalError::Unsupported(
                            "map lookup key type".into(),
                        ));
                    }
                    if let Some((_, value)) = entries.iter().find(|(candidate, _)| candidate == key)
                    {
                        return Ok(value.clone());
                    }
                    let (
                        DataType::Map {
                            value,
                            value_nullable,
                            ..
                        },
                        _,
                    ) = args[0]
                        .scalar_type(schema)
                        .map_err(|error| ClickHouseRelationalError::Invalid(error.to_string()))?
                    else {
                        unreachable!()
                    };
                    default_collection_element(&value, value_nullable)
                }
            }
        }
        other => Err(ClickHouseRelationalError::Unsupported(format!(
            "scalar expression {other:?}"
        ))),
    }
}

fn default_collection_element(
    dtype: &DataType,
    nullable: bool,
) -> Result<Cell, ClickHouseRelationalError> {
    if nullable {
        return Ok(Cell::Null);
    }
    Ok(match dtype {
        DataType::Null => Cell::Null,
        DataType::Int64 => Cell::Int64(0),
        DataType::Float64 => Cell::Float64(0.0),
        DataType::Utf8 => Cell::Utf8(String::new()),
        DataType::Bool => Cell::Bool(false),
        DataType::Map { .. } => Cell::Map(Vec::new()),
        DataType::List { .. } => Cell::List(Arc::from([])),
        DataType::Struct { fields } => Cell::Struct(
            fields
                .iter()
                .map(|field| default_collection_element(&field.dtype, field.nullable))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        ),
        _ => {
            return Err(ClickHouseRelationalError::Unsupported(
                "collection missing-element default type".into(),
            ))
        }
    })
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
    if let (Cell::Int64(left), Cell::Int64(right)) = (&left, &right) {
        let integer = match op {
            ArithmeticOpKind::Add => Some(left.checked_add(*right)),
            ArithmeticOpKind::Sub => Some(left.checked_sub(*right)),
            ArithmeticOpKind::Mul => Some(left.checked_mul(*right)),
            ArithmeticOpKind::Mod => Some(left.checked_rem(*right)),
            _ => None,
        };
        if let Some(value) = integer {
            return value.map(Cell::Int64).ok_or_else(|| {
                ClickHouseRelationalError::Invalid(
                    "integer arithmetic overflow or zero divisor".into(),
                )
            });
        }
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

fn compare_sort_keys(
    left: &[Cell],
    right: &[Cell],
    keys: &[SortKey],
    schema: &planner_types::pre_asap::Schema,
) -> Ordering {
    for key in keys {
        let Ok(left) = eval(&key.expr, left, schema) else {
            return Ordering::Equal;
        };
        let Ok(right) = eval(&key.expr, right, schema) else {
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

fn contains_nan(value: &Cell) -> bool {
    match value {
        Cell::Float64(value) => value.is_nan(),
        Cell::List(values) | Cell::Struct(values) => values.iter().any(contains_nan),
        Cell::Map(entries) => entries
            .iter()
            .any(|(key, value)| contains_nan(key) || contains_nan(value)),
        _ => false,
    }
}

fn integer_float_cmp(integer: i64, float: f64) -> Option<Ordering> {
    if float.is_nan() {
        return None;
    }
    // These bounds are powers of two, exactly representable as Float64.
    if float >= 9_223_372_036_854_775_808.0 {
        return Some(Ordering::Less);
    }
    if float < -9_223_372_036_854_775_808.0 {
        return Some(Ordering::Greater);
    }
    let integral = float as i64;
    match integer.cmp(&integral) {
        Ordering::Equal => 0.0_f64.partial_cmp(&float.fract()),
        other => Some(other),
    }
}

fn cell_cmp(left: &Cell, right: &Cell) -> Option<Ordering> {
    match (left, right) {
        (Cell::Int64(left), Cell::Int64(right)) => Some(left.cmp(right)),
        (Cell::Float64(left), Cell::Float64(right)) => left.partial_cmp(right),
        (Cell::Int64(left), Cell::Float64(right)) => integer_float_cmp(*left, *right),
        (Cell::Float64(left), Cell::Int64(right)) => {
            integer_float_cmp(*right, *left).map(Ordering::reverse)
        }
        (Cell::Utf8(left), Cell::Utf8(right)) => Some(left.cmp(right)),
        (Cell::Bool(left), Cell::Bool(right)) => Some(left.cmp(right)),
        (Cell::Timestamp(left), Cell::Timestamp(right)) => Some(left.cmp(right)),
        (Cell::Map(left), Cell::Map(right)) => {
            for ((left_key, left_value), (right_key, right_value)) in left.iter().zip(right) {
                let order = cell_cmp(left_key, right_key)?;
                if order != Ordering::Equal {
                    return Some(order);
                }
                let order = match (left_value, right_value) {
                    (Cell::Null, Cell::Null) => Ordering::Equal,
                    (Cell::Null, _) => Ordering::Greater,
                    (_, Cell::Null) => Ordering::Less,
                    _ => cell_cmp(left_value, right_value)?,
                };
                if order != Ordering::Equal {
                    return Some(order);
                }
            }
            Some(left.len().cmp(&right.len()))
        }
        _ => None,
    }
}

fn arrow_type(dtype: &DataType) -> ArrowDataType {
    match dtype {
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
            ValueOperation::Limit { n: 1, offset: 0 },
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
            &QueryExpr::BoolAnd(vec![]),
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
    fn mixed_comparison_preserves_integer_precision_and_boundaries() {
        assert_eq!(
            integer_float_cmp(9_007_199_254_740_993, 9_007_199_254_740_992.0),
            Some(Ordering::Greater)
        );
        assert_eq!(
            integer_float_cmp(i64::MAX, 9_223_372_036_854_775_808.0),
            Some(Ordering::Less)
        );
        assert_eq!(
            integer_float_cmp(i64::MIN, -9_223_372_036_854_775_808.0),
            Some(Ordering::Equal)
        );
        assert_eq!(integer_float_cmp(-1, -1.5), Some(Ordering::Greater));
        assert_eq!(integer_float_cmp(1, 1.5), Some(Ordering::Less));
        assert_eq!(integer_float_cmp(0, f64::INFINITY), Some(Ordering::Less));
        assert_eq!(
            integer_float_cmp(0, f64::NEG_INFINITY),
            Some(Ordering::Greater)
        );
        assert_eq!(integer_float_cmp(0, f64::NAN), None);
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
