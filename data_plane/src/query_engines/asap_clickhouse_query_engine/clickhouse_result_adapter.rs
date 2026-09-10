use super::fallback::ClickHouseRawResponse;
use arrow::{record_batch::RecordBatch, util::display::array_value_to_string};
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickHouseFormat {
    TabSeparated,
    JsonEachRow,
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
                        let mut object = serde_json::Map::new();
                        for (column, field) in batch.schema().fields().iter().enumerate() {
                            let value = array_value_to_string(batch.column(column).as_ref(), row)
                                .map_err(|error| {
                                ClickHouseResultError::Arrow(error.to_string())
                            })?;
                            object.insert(field.name().clone(), serde_json::Value::String(value));
                        }
                        serde_json::to_writer(&mut output, &object)?;
                        output.push(b'\n');
                    }
                }
            }
        }
        Ok(output)
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
            b"{\"count\":\"7\",\"zone\":\"a\\tb\"}\n"
        );
    }
}
