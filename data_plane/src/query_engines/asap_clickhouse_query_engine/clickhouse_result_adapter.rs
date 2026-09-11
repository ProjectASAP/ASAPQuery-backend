use super::fallback::ClickHouseRawResponse;
use arrow::{
    array::{Array, Float64Array, StringArray, TimestampMillisecondArray},
    datatypes::{DataType, Field, Schema},
    json::{LineDelimitedWriter, WriterBuilder},
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use axum::response::{IntoResponse, Response};
use serde::{
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseFormat {
    TabSeparated,
    JsonEachRow,
    Json,
}

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseResultError {
    #[error("cannot render Arrow value: {0}")]
    Arrow(String),
    #[error("cannot encode ClickHouse JSON: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct ClickHouseQueryResult {
    pub batches: Vec<RecordBatch>,
}

pub fn from_series_rows(
    rows: Vec<(BTreeMap<String, String>, Vec<(i64, f64)>)>,
) -> Result<ClickHouseQueryResult, ClickHouseResultError> {
    let mut label_names = rows
        .iter()
        .flat_map(|(labels, _)| labels.keys().cloned())
        .collect::<Vec<_>>();
    label_names.sort();
    label_names.dedup();
    let flattened = rows
        .iter()
        .flat_map(|(labels, points)| points.iter().map(move |point| (labels, point)))
        .collect::<Vec<_>>();
    let mut fields = label_names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect::<Vec<_>>();
    fields.push(Field::new(
        "timestamp",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        false,
    ));
    fields.push(Field::new("value", DataType::Float64, false));
    let mut columns = label_names
        .iter()
        .map(|name| {
            Arc::new(StringArray::from(
                flattened
                    .iter()
                    .map(|(labels, _)| labels.get(name).map(String::as_str))
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef
        })
        .collect::<Vec<_>>();
    columns.push(Arc::new(TimestampMillisecondArray::from(
        flattened.iter().map(|(_, (ts, _))| *ts).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(Float64Array::from(
        flattened
            .iter()
            .map(|(_, (_, value))| *value)
            .collect::<Vec<_>>(),
    )));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| ClickHouseResultError::Arrow(error.to_string()))?;
    Ok(ClickHouseQueryResult {
        batches: vec![batch],
    })
}

impl ClickHouseQueryResult {
    pub fn encode(&self, format: ClickHouseFormat) -> Result<Vec<u8>, ClickHouseResultError> {
        if self.batches.iter().any(|batch| {
            batch
                .columns()
                .iter()
                .any(|column| contains_map_timestamp(column.data_type(), false))
        }) {
            return Err(ClickHouseResultError::Arrow(
                "nested Map timestamp output requires an explicit formatting contract".into(),
            ));
        }
        // ClickHouse's 64-bit JSON quoting default is deployment-configurable.
        // Until the client output policy is explicit, never guess it for warm output.
        if matches!(
            format,
            ClickHouseFormat::Json | ClickHouseFormat::JsonEachRow
        ) && self.batches.iter().any(|batch| {
            batch
                .columns()
                .iter()
                .any(|column| contains_json_integer64(column.data_type()))
        }) {
            return Err(ClickHouseResultError::Arrow(
                "64-bit JSON integer output requires an explicit quoting contract".into(),
            ));
        }
        let mut output = Vec::new();
        for batch in &self.batches {
            for row in 0..batch.num_rows() {
                match format {
                    ClickHouseFormat::TabSeparated => {
                        for column in 0..batch.num_columns() {
                            if column > 0 {
                                output.push(b'\t');
                            }
                            if batch.column(column).is_null(row) {
                                output.extend_from_slice(b"\\N");
                                continue;
                            }
                            if matches!(batch.column(column).data_type(), DataType::Map(..)) {
                                output.extend_from_slice(
                                    map_literal(batch.column(column).as_ref(), row)?.as_bytes(),
                                );
                                continue;
                            }
                            let value = array_value_to_string(batch.column(column).as_ref(), row)
                                .map_err(|error| {
                                ClickHouseResultError::Arrow(error.to_string())
                            })?;
                            output.extend_from_slice(escape_tsv(&value).as_bytes());
                        }
                        output.push(b'\n');
                    }
                    ClickHouseFormat::JsonEachRow => {
                        if batch
                            .schema()
                            .fields()
                            .iter()
                            .any(|field| matches!(field.data_type(), DataType::Map(..)))
                        {
                            serde_json::to_writer(&mut output, &JsonArrowRow { batch, row })?;
                            output.push(b'\n');
                            continue;
                        }

                        let row = batch.slice(row, 1);
                        let mut writer: LineDelimitedWriter<_> = WriterBuilder::new()
                            .with_explicit_nulls(true)
                            .build(&mut output);
                        writer
                            .write_batches(&[&row])
                            .map_err(|error| ClickHouseResultError::Arrow(error.to_string()))?;
                        writer
                            .finish()
                            .map_err(|error| ClickHouseResultError::Arrow(error.to_string()))?;
                    }
                    ClickHouseFormat::Json => {}
                }
            }
        }
        if format == ClickHouseFormat::Json {
            return self.encode_json_document();
        }
        Ok(output)
    }

    fn encode_json_document(&self) -> Result<Vec<u8>, ClickHouseResultError> {
        let schema = self.batches.first().map(RecordBatch::schema);
        let meta = schema
            .as_ref()
            .map(|schema| {
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        serde_json::json!({
                            "name": field.name(),
                            "type": clickhouse_type(field.data_type(), field.is_nullable()),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if self.batches.iter().any(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .any(|field| matches!(field.data_type(), DataType::Map(..)))
        }) {
            let count: usize = self.batches.iter().map(RecordBatch::num_rows).sum();
            let mut output = Vec::new();
            let mut serializer = serde_json::Serializer::new(&mut output);
            let mut document = serializer.serialize_struct("ClickHouseResult", 4)?;
            document.serialize_field("meta", &meta)?;
            document.serialize_field("data", &JsonArrowRows(&self.batches))?;
            document.serialize_field("rows", &count)?;
            document.serialize_field(
                "statistics",
                &serde_json::json!({"elapsed":0.0,"rows_read":count,"bytes_read":0}),
            )?;
            SerializeStruct::end(document)?;
            return Ok(output);
        }
        let mut rows = Vec::new();
        for batch in &self.batches {
            let mut encoded = Vec::new();
            let mut writer: LineDelimitedWriter<_> = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build(&mut encoded);
            writer
                .write_batches(&[batch])
                .map_err(|error| ClickHouseResultError::Arrow(error.to_string()))?;
            writer
                .finish()
                .map_err(|error| ClickHouseResultError::Arrow(error.to_string()))?;
            for line in encoded
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                rows.push(serde_json::from_slice::<serde_json::Value>(line)?);
            }
        }
        Ok(serde_json::to_vec(&serde_json::json!({
            "meta": meta,
            "data": rows,
            "rows": rows.len(),
            "statistics": {"elapsed": 0.0, "rows_read": rows.len(), "bytes_read": 0}
        }))?)
    }
}

pub(super) fn clickhouse_type(data_type: &arrow::datatypes::DataType, nullable: bool) -> String {
    use arrow::datatypes::DataType;
    let base = match data_type {
        DataType::Null => "Nothing".into(),
        DataType::Boolean => "Bool".into(),
        DataType::Int8 => "Int8".into(),
        DataType::Int16 => "Int16".into(),
        DataType::Int32 => "Int32".into(),
        DataType::Int64 => "Int64".into(),
        DataType::UInt8 => "UInt8".into(),
        DataType::UInt16 => "UInt16".into(),
        DataType::UInt32 => "UInt32".into(),
        DataType::UInt64 => "UInt64".into(),
        DataType::Float32 => "Float32".into(),
        DataType::Float64 => "Float64".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "String".into(),
        DataType::Timestamp(_, _) => "DateTime64(3)".into(),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "Map({}, {})",
                clickhouse_type(fields[0].data_type(), false),
                clickhouse_type(fields[1].data_type(), fields[1].is_nullable())
            ),
            other => other.to_string(),
        },
        other => other.to_string(),
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base
    }
}

fn contains_map_timestamp(dtype: &DataType, in_map: bool) -> bool {
    match dtype {
        DataType::Timestamp(..) => in_map,
        DataType::Map(entries, _) => contains_map_timestamp(entries.data_type(), true),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_map_timestamp(field.data_type(), in_map)),
        _ => false,
    }
}

fn contains_json_integer64(dtype: &DataType) -> bool {
    match dtype {
        DataType::Int64 | DataType::UInt64 => true,
        DataType::Map(entries, _) => contains_json_integer64(entries.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_json_integer64(field.data_type())),
        _ => false,
    }
}

fn escape_tsv(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

pub fn raw_response(response: ClickHouseRawResponse) -> Response {
    let mut output = (response.status, response.body).into_response();
    for (name, value) in response.headers {
        if let Some(name) = name {
            output.headers_mut().append(name, value);
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use std::sync::Arc;

    #[test]
    fn nullable_fields_remain_explicit_in_json_and_tsv() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Float64, true),
                Field::new("label", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.25), None, Some(2.5)])),
                Arc::new(StringArray::from(vec![Some(""), None, Some("\\N")])),
            ],
        )
        .unwrap();
        let result = ClickHouseQueryResult {
            batches: vec![batch],
        };
        let json: serde_json::Value =
            serde_json::from_slice(&result.encode(ClickHouseFormat::Json).unwrap()).unwrap();
        assert_eq!(
            json["data"],
            serde_json::json!([{ "value":1.25,"label":"" }, { "value":null,"label":null }, {"value":2.5,"label":"\\N"}])
        );
        let lines = result.encode(ClickHouseFormat::JsonEachRow).unwrap();
        let null_row: serde_json::Value =
            serde_json::from_slice(lines.split(|byte| *byte == b'\n').nth(1).unwrap()).unwrap();
        assert_eq!(null_row, serde_json::json!({"value":null,"label":null}));
        assert_eq!(
            result.encode(ClickHouseFormat::TabSeparated).unwrap(),
            b"1.25\t\n\\N\t\\N\n2.5\t\\\\N\n"
        );
    }

    #[test]
    fn empty_map_bottom_type_uses_clickhouse_nothing() {
        let entries = DataType::Struct(
            vec![
                Field::new("key", DataType::Null, false),
                Field::new("value", DataType::Null, false),
            ]
            .into(),
        );
        let dtype = DataType::Map(Arc::new(Field::new("entries", entries, false)), false);
        assert_eq!(clickhouse_type(&dtype, false), "Map(Nothing, Nothing)");
        assert_eq!(clickhouse_type(&DataType::Null, true), "Nullable(Nothing)");
    }

    #[test]
    fn map_timestamp_transport_is_not_assumed_to_match_native_formatting() {
        let entries = DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                    false,
                ),
            ]
            .into(),
        );
        let dtype = DataType::Map(Arc::new(Field::new("entries", entries, false)), false);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("m", dtype.clone(), false)])),
            vec![arrow::array::new_empty_array(&dtype)],
        )
        .unwrap();
        let result = ClickHouseQueryResult {
            batches: vec![batch],
        };
        assert!(result.encode(ClickHouseFormat::Json).is_err());
        assert!(result.encode(ClickHouseFormat::TabSeparated).is_err());
    }

    #[test]
    fn encodes_table_without_using_promql_query_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("zone", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a\tb"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        let result = ClickHouseQueryResult {
            batches: vec![batch],
        };
        assert_eq!(
            result.encode(ClickHouseFormat::TabSeparated).unwrap(),
            b"a\\tb\t7\n"
        );
        assert!(result.encode(ClickHouseFormat::JsonEachRow).is_err());
        assert!(result.encode(ClickHouseFormat::Json).is_err());
    }
}

struct JsonArrowRows<'a>(&'a [RecordBatch]);
impl Serialize for JsonArrowRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut rows =
            serializer.serialize_seq(Some(self.0.iter().map(RecordBatch::num_rows).sum()))?;
        for batch in self.0 {
            for row in 0..batch.num_rows() {
                rows.serialize_element(&JsonArrowRow { batch, row })?;
            }
        }
        rows.end()
    }
}
struct JsonArrowRow<'a> {
    batch: &'a RecordBatch,
    row: usize,
}
impl Serialize for JsonArrowRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(self.batch.num_columns()))?;
        for (field, column) in self
            .batch
            .schema()
            .fields()
            .iter()
            .zip(self.batch.columns())
        {
            object.serialize_entry(
                field.name(),
                &JsonArrowValue {
                    array: column.as_ref(),
                    row: self.row,
                },
            )?;
        }
        object.end()
    }
}
struct JsonArrowValue<'a> {
    array: &'a dyn Array,
    row: usize,
}
impl Serialize for JsonArrowValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use arrow::array::*;
        use serde::ser::Error;
        if self.array.is_null(self.row) {
            return serializer.serialize_none();
        }
        macro_rules! scalar {
            ($ty:ty) => {
                self.array
                    .as_any()
                    .downcast_ref::<$ty>()
                    .ok_or_else(|| S::Error::custom("Arrow scalar type mismatch"))?
                    .value(self.row)
                    .serialize(serializer)
            };
        }
        match self.array.data_type() {
            DataType::Map(..) => {
                let map = self
                    .array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| S::Error::custom("Arrow map type mismatch"))?;
                let entries = map.value(self.row);
                let mut object = serializer.serialize_map(Some(entries.len()))?;
                for row in 0..entries.len() {
                    object.serialize_entry(
                        &JsonArrowValue {
                            array: entries.column(0).as_ref(),
                            row,
                        },
                        &JsonArrowValue {
                            array: entries.column(1).as_ref(),
                            row,
                        },
                    )?;
                }
                object.end()
            }
            DataType::Boolean => scalar!(BooleanArray),
            DataType::Int8 => scalar!(Int8Array),
            DataType::Int16 => scalar!(Int16Array),
            DataType::Int32 => scalar!(Int32Array),
            DataType::Int64 => scalar!(Int64Array),
            DataType::UInt8 => scalar!(UInt8Array),
            DataType::UInt16 => scalar!(UInt16Array),
            DataType::UInt32 => scalar!(UInt32Array),
            DataType::UInt64 => scalar!(UInt64Array),
            DataType::Float32 => scalar!(Float32Array),
            DataType::Float64 => scalar!(Float64Array),
            DataType::Utf8 => scalar!(StringArray),
            DataType::LargeUtf8 => scalar!(LargeStringArray),
            DataType::Timestamp(..) => array_value_to_string(self.array, self.row)
                .map_err(S::Error::custom)?
                .serialize(serializer),
            dtype => Err(S::Error::custom(format!(
                "unsupported nested Arrow value {dtype}"
            ))),
        }
    }
}

fn map_literal(array: &dyn Array, row: usize) -> Result<String, ClickHouseResultError> {
    use arrow::array::{Float32Array, LargeStringArray, MapArray};
    if array.is_null(row) {
        return Ok("NULL".into());
    }
    match array.data_type() {
        DataType::Map(..) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| ClickHouseResultError::Arrow("invalid map array".into()))?;
            let entries = map.value(row);
            let mut values = Vec::with_capacity(entries.len());
            for row in 0..entries.len() {
                values.push(format!(
                    "{}:{}",
                    map_literal(entries.column(0).as_ref(), row)?,
                    map_literal(entries.column(1).as_ref(), row)?
                ));
            }
            Ok(format!("{{{}}}", values.join(",")))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let value = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                array.value(row)
            } else if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
                array.value(row)
            } else {
                return Err(ClickHouseResultError::Arrow("invalid string array".into()));
            };
            let mut escaped = String::from("'");
            for ch in value.chars() {
                match ch {
                    '\\' => escaped.push_str("\\\\"),
                    '\'' => escaped.push_str("\\'"),
                    '\n' => escaped.push_str("\\n"),
                    '\r' => escaped.push_str("\\r"),
                    '\t' => escaped.push_str("\\t"),
                    '\0' => escaped.push_str("\\0"),
                    '\u{0008}' => escaped.push_str("\\b"),
                    '\u{000c}' => escaped.push_str("\\f"),
                    ch => escaped.push(ch),
                }
            }
            escaped.push('\'');
            Ok(escaped)
        }
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Timestamp(..) => Err(ClickHouseResultError::Arrow(
            "timestamp map TSV encoding is unsupported".into(),
        )),
        _ => array_value_to_string(array, row)
            .map_err(|error| ClickHouseResultError::Arrow(error.to_string())),
    }
}
