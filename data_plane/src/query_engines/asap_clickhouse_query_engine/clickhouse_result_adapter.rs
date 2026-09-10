use super::fallback::ClickHouseRawResponse;
use arrow::{
    array::{Float64Array, StringArray, TimestampMillisecondArray},
    datatypes::{DataType, Field, Schema},
    json::LineDelimitedWriter,
    record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use axum::response::{IntoResponse, Response};
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
        let mut output = Vec::new();
        for batch in &self.batches {
            for row in 0..batch.num_rows() {
                match format {
                    ClickHouseFormat::TabSeparated => {
                        for column in 0..batch.num_columns() {
                            if column > 0 {
                                output.push(b'\t');
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
                        let row = batch.slice(row, 1);
                        let mut writer = LineDelimitedWriter::new(&mut output);
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
        let mut rows = Vec::new();
        for batch in &self.batches {
            let mut encoded = Vec::new();
            let mut writer = LineDelimitedWriter::new(&mut encoded);
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

fn clickhouse_type(data_type: &arrow::datatypes::DataType, nullable: bool) -> String {
    use arrow::datatypes::DataType;
    let base = match data_type {
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
        other => other.to_string(),
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base
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
        assert_eq!(
            result.encode(ClickHouseFormat::JsonEachRow).unwrap(),
            b"{\"zone\":\"a\\tb\",\"count\":7}\n"
        );
        let document: serde_json::Value =
            serde_json::from_slice(&result.encode(ClickHouseFormat::Json).unwrap()).unwrap();
        assert_eq!(document["data"][0]["count"], 7);
        assert_eq!(document["meta"][1]["type"], "Int64");
        assert_eq!(document["rows"], 1);
    }
}
