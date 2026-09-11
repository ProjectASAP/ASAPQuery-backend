//! ClickHouse-backed historical sample reader for the existing backfill worker.

use async_trait::async_trait;
use serde::Deserialize;

use super::raw_sample_reader::{LabelFilter, RawSample, RawSampleReader, RawSampleReaderError};
use super::{BackfillSource, ReaderFactory};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ClickHouseReaderConfig {
    pub base_url: String,
    pub database: String,
    pub table: String,
    pub metric_column: String,
    pub labels_column: String,
    pub timestamp_ms_column: String,
    pub value_column: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl ClickHouseReaderConfig {
    pub fn validate(&self) -> Result<(), RawSampleReaderError> {
        for (name, value) in [
            ("database", self.database.as_str()),
            ("table", self.table.as_str()),
            ("metric column", self.metric_column.as_str()),
            ("labels column", self.labels_column.as_str()),
            ("timestamp column", self.timestamp_ms_column.as_str()),
            ("value column", self.value_column.as_str()),
        ] {
            if value.is_empty()
                || !value.as_bytes()[0].is_ascii_alphabetic() && !value.starts_with('_')
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(RawSampleReaderError::Other {
                    reason: format!("invalid ClickHouse {name}: {value:?}"),
                });
            }
        }
        Ok(())
    }
}

pub struct ClickHouseReader {
    config: ClickHouseReaderConfig,
    http: reqwest::Client,
    population: Option<asap_types::table_population::TablePopulation>,
    value_projection: Option<asap_types::sds::ValueProjectionIdentity>,
    output_metric: Option<String>,
    grouping_projection: Option<asap_types::GroupingProjection>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ClickHouseLabels {
    Series(String),
    Map(std::collections::BTreeMap<String, String>),
    Columns(Vec<(String, String)>),
}

#[derive(Deserialize)]
struct ClickHouseSampleRow {
    labels: ClickHouseLabels,
    timestamp_ms: i64,
    value: f64,
}

impl ClickHouseReader {
    pub fn new(config: ClickHouseReaderConfig) -> Result<Self, RawSampleReaderError> {
        config.validate()?;
        Ok(Self {
            config,
            http: reqwest::Client::new(),
            population: None,
            value_projection: None,
            output_metric: None,
            grouping_projection: None,
        })
    }

    fn sql(&self) -> String {
        let c = &self.config;
        let labels = self.grouping_projection.as_ref().map_or_else(
            || c.labels_column.clone(),
            |grouping| {
                let entries = grouping
                    .columns()
                    .iter()
                    .map(|column| {
                        let value = format!("base64Encode(toJSONString({}))", column.name);
                        format!("'{}', {value}", column.name)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("map({entries})")
            },
        );
        let population = self.population.as_ref().map_or_else(
            || format!("{} = {{metric:String}}", c.metric_column),
            |population| {
                if population.predicates.is_empty() {
                    return "1".into();
                }
                population
                    .predicates
                    .iter()
                    .enumerate()
                    .map(|(index, predicate)| {
                        use planner_types::pre_asap::{CompareOpKind, ScalarValue};
                        let operator = match predicate.operator {
                            CompareOpKind::Eq => "=",
                            CompareOpKind::Ne => "!=",
                            CompareOpKind::Lt => "<",
                            CompareOpKind::Le => "<=",
                            CompareOpKind::Gt => ">",
                            CompareOpKind::Ge => ">=",
                            _ => unreachable!("validated table predicate"),
                        };
                        let kind = match predicate.value {
                            ScalarValue::Utf8(_) => "String",
                            ScalarValue::Int64(_) => "Int64",
                            ScalarValue::Float64(_) => "Float64",
                            ScalarValue::Boolean(_) => "Bool",
                            ScalarValue::Null => unreachable!("validated table literal"),
                        };
                        format!(
                            "{} {operator} {{population_{index}:{kind}}}",
                            predicate.column
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ")
            },
        );
        let value = match &self.value_projection {
            Some(asap_types::sds::ValueProjectionIdentity::Constant {
                value: planner_types::pre_asap::ScalarValue::Int64(_),
            }) => "{projected_value:Int64}",
            Some(asap_types::sds::ValueProjectionIdentity::Constant {
                value: planner_types::pre_asap::ScalarValue::Float64(_),
            }) => "{projected_value:Float64}",
            _ => c.value_column.as_str(),
        };
        format!(
            "SELECT {labels} AS labels, {timestamp} AS timestamp_ms, {value} AS value \
             FROM {database}.{table} WHERE {population} \
             AND {timestamp} >= {{start_ms:Int64}} AND {timestamp} < {{end_ms:Int64}} \
             ORDER BY labels, timestamp_ms FORMAT JSONEachRow",
            labels = labels,
            timestamp = c.timestamp_ms_column,
            value = value,
            database = c.database,
            table = c.table,
        )
    }
}

/// Resolve a typed table source using deployment-local connection settings.
pub fn clickhouse_reader_factory(config: ClickHouseReaderConfig) -> ReaderFactory {
    let fallback = super::service::default_reader_factory();
    Arc::new(move |source, materialization| match source {
        BackfillSource::ClickHouse { database, table } => {
            if database != &config.database {
                return Err(RawSampleReaderError::Other {
                    reason: "ClickHouse source database differs from the deployment database"
                        .into(),
                }
                .into());
            }
            materialization.grouping_labels.validate_table_columns()?;
            for column in materialization.grouping_labels.columns() {
                if column.nullable {
                    return Err(
                        "nullable table grouping requires an explicit null-key encoding".into(),
                    );
                }
            }
            let mut source_config = config.clone();
            source_config.database = database.clone();
            source_config.table = table.clone();
            source_config.timestamp_ms_column = materialization
                .table_timestamp_column
                .clone()
                .ok_or("table materialization has no timestamp projection")?;
            match materialization.effective_value_projection() {
                asap_types::sds::ValueProjectionIdentity::Column { name } => {
                    source_config.value_column = name.clone()
                }
                asap_types::sds::ValueProjectionIdentity::Constant {
                    value: planner_types::pre_asap::ScalarValue::Int64(value),
                } if value.unsigned_abs() > (1_u64 << 53) => {
                    return Err("integer projection exceeds exact Float64 ingest range".into())
                }
                asap_types::sds::ValueProjectionIdentity::Constant { .. } => {}
                asap_types::sds::ValueProjectionIdentity::SampleValue => {
                    return Err("table materialization has no explicit value projection".into())
                }
            }
            materialization.population_filter_canonical()?;
            let mut reader = ClickHouseReader::new(source_config)?;
            reader.population = Some(materialization.table_population.clone().unwrap_or_default());
            reader.value_projection = Some(materialization.effective_value_projection().clone());
            reader.output_metric = Some(materialization.metric.clone());
            reader.grouping_projection = Some(materialization.grouping_labels.clone());
            Ok(Arc::new(reader) as Arc<dyn RawSampleReader>)
        }
        source => fallback(source, materialization),
    })
}

#[async_trait]
impl RawSampleReader for ClickHouseReader {
    async fn read_samples(
        &self,
        start_ms: u64,
        end_ms: u64,
        filter: &LabelFilter,
    ) -> Result<Vec<RawSample>, RawSampleReaderError> {
        if start_ms > end_ms || end_ms > i64::MAX as u64 {
            return Err(RawSampleReaderError::InvalidRange {
                reason: format!("unsupported range [{start_ms}, {end_ms})"),
            });
        }
        let start_ms = start_ms.to_string();
        let end_ms = end_ms.to_string();
        let mut request = self.http.post(&self.config.base_url).query(&[
            ("param_metric", filter.metric.as_str()),
            ("param_start_ms", start_ms.as_str()),
            ("param_end_ms", end_ms.as_str()),
        ]);
        if self.grouping_projection.is_some() {
            request = request.query(&[
                ("output_format_json_map_as_array_of_tuples", "1"),
                ("output_format_json_named_tuples_as_objects", "0"),
                ("output_format_json_quote_64bit_integers", "0"),
            ]);
        }
        if let Some(asap_types::sds::ValueProjectionIdentity::Constant { value }) =
            &self.value_projection
        {
            let value = match value {
                planner_types::pre_asap::ScalarValue::Int64(value) => value.to_string(),
                planner_types::pre_asap::ScalarValue::Float64(value) => value.to_string(),
                _ => {
                    return Err(RawSampleReaderError::Other {
                        reason: "unsupported constant projection".into(),
                    })
                }
            };
            request = request.query(&[("param_projected_value", value)]);
        }
        if let Some(population) = &self.population {
            for (index, predicate) in population.predicates.iter().enumerate() {
                use planner_types::pre_asap::ScalarValue;
                let value = match &predicate.value {
                    ScalarValue::Utf8(value) => value.clone(),
                    ScalarValue::Int64(value) => value.to_string(),
                    ScalarValue::Float64(value) => value.to_string(),
                    ScalarValue::Boolean(value) => value.to_string(),
                    ScalarValue::Null => unreachable!("validated table literal"),
                };
                request = request.query(&[(format!("param_population_{index}"), value)]);
            }
        }
        if let Some(user) = &self.config.user {
            request = request.basic_auth(user, self.config.password.as_ref());
        }
        let response = request.body(self.sql()).send().await.map_err(|error| {
            RawSampleReaderError::Upstream {
                reason: error.to_string(),
            }
        })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| RawSampleReaderError::Decode {
                reason: error.to_string(),
            })?;
        if !status.is_success() {
            return Err(RawSampleReaderError::Upstream {
                reason: format!("ClickHouse HTTP {status}: {body}"),
            });
        }
        let mut samples = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            let row: ClickHouseSampleRow =
                serde_json::from_str(line).map_err(|error| RawSampleReaderError::Decode {
                    reason: error.to_string(),
                })?;
            let labels = match row.labels {
                ClickHouseLabels::Columns(columns) => {
                    let count = columns.len();
                    let labels = columns
                        .into_iter()
                        .collect::<std::collections::BTreeMap<_, _>>();
                    if labels.len() != count {
                        return Err(RawSampleReaderError::Decode {
                            reason: "duplicate source grouping columns".into(),
                        });
                    }
                    ClickHouseLabels::Map(labels)
                }
                labels => labels,
            };
            let sample = RawSample {
                labels: match labels {
                    ClickHouseLabels::Columns(_) => unreachable!("columns normalized above"),
                    ClickHouseLabels::Series(series) => {
                        if self.population.is_some() {
                            let metric = self.output_metric.as_deref().unwrap_or(&filter.metric);
                            let suffix = series.find('{').map_or("", |start| &series[start..]);
                            format!("{metric}{suffix}")
                        } else {
                            series
                        }
                    }
                    ClickHouseLabels::Map(labels) => {
                        let metric = self.output_metric.as_deref().unwrap_or(&filter.metric);
                        let labels = labels
                            .iter()
                            .map(|(key, value)| {
                                format!(
                                    "{key}={}",
                                    serde_json::to_string(value).expect("label serialization")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        if labels.is_empty() {
                            metric.into()
                        } else {
                            format!("{metric}{{{labels}}}")
                        }
                    }
                },
                timestamp_ms: row.timestamp_ms,
                value: row.value,
            };
            if super::raw_sample_reader::sample_matches(&sample.labels, filter) {
                samples.push(sample);
            }
        }
        Ok(samples)
    }

    fn source_name(&self) -> &'static str {
        "ClickHouseReader"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(table: &str) -> ClickHouseReaderConfig {
        ClickHouseReaderConfig {
            base_url: "http://localhost:8123".into(),
            database: "metrics".into(),
            table: table.into(),
            metric_column: "metric".into(),
            labels_column: "labels".into(),
            timestamp_ms_column: "timestamp_ms".into(),
            value_column: "value".into(),
            user: None,
            password: None,
        }
    }

    #[test]
    fn validates_identifiers_and_parameterizes_values() {
        assert!(ClickHouseReader::new(config("samples; DROP TABLE x")).is_err());
        let reader = ClickHouseReader::new(config("samples")).unwrap();
        let sql = reader.sql();
        assert!(sql.contains("metric = {metric:String}"));
        assert!(sql.contains("FORMAT JSONEachRow"));
    }

    #[test]
    fn table_population_values_are_parameters_not_sql_fragments() {
        let mut reader = ClickHouseReader::new(config("samples")).unwrap();
        reader.population = Some(asap_types::table_population::TablePopulation {
            predicates: vec![asap_types::table_population::TableColumnPredicate {
                column: "metric".into(),
                operator: planner_types::pre_asap::CompareOpKind::Eq,
                value: planner_types::pre_asap::ScalarValue::Utf8("requests' OR 1=1 --".into()),
            }],
        });
        let sql = reader.sql();
        assert!(sql.contains("metric = {population_0:String}"));
        assert!(!sql.contains("requests"));
        assert!(!sql.contains("metric = {metric:String}"));
    }

    #[test]
    fn unfiltered_table_population_does_not_filter_by_output_metric() {
        let mut reader = ClickHouseReader::new(config("samples")).unwrap();
        reader.population = Some(Default::default());
        let sql = reader.sql();
        assert!(sql.contains("WHERE 1 AND"));
        assert!(!sql.contains("{metric:String}"));
    }

    #[test]
    fn constant_projection_uses_a_typed_parameter_without_a_fake_column() {
        let mut reader = ClickHouseReader::new(config("samples")).unwrap();
        reader.value_projection = Some(asap_types::sds::ValueProjectionIdentity::Constant {
            value: planner_types::pre_asap::ScalarValue::Int64(1),
        });
        assert!(reader.sql().contains("{projected_value:Int64} AS value"));
        assert!(!reader.sql().contains(" value AS value"));
    }

    #[test]
    fn typed_source_enters_clickhouse_backfill_lifecycle() {
        let mut materialization = asap_types::PrecomputeMaterialization::new(
            asap_types::AggregationType::Sum,
            String::new(),
            Default::default(),
            asap_types::KeyByLabelNames::empty(),
            asap_types::KeyByLabelNames::empty(),
            asap_types::KeyByLabelNames::empty(),
            String::new(),
            1,
            1,
            asap_types::WindowKind::Tumbling,
            String::new(),
            "samples.value".into(),
            None,
            Some("another_table".into()),
            Some("value".into()),
        );
        let factory = clickhouse_reader_factory(config("samples"));
        materialization.table_timestamp_column = Some("timestamp_ms".into());
        let reader = factory(
            &BackfillSource::ClickHouse {
                database: "metrics".into(),
                table: "another_table".into(),
            },
            &materialization,
        )
        .unwrap();
        assert_eq!(reader.source_name(), "ClickHouseReader");
        let mut typed = materialization.clone();
        typed.grouping_labels =
            asap_types::GroupingProjection::new(vec![planner_types::pre_asap::Column::new(
                "tenant",
                planner_types::pre_asap::DataType::Int64,
                false,
            )]);
        assert!(factory(
            &BackfillSource::ClickHouse {
                database: "metrics".into(),
                table: "another_table".into()
            },
            &typed,
        )
        .is_ok());
        typed.grouping_labels =
            asap_types::GroupingProjection::new(vec![planner_types::pre_asap::Column::new(
                "tenant",
                planner_types::pre_asap::DataType::Int64,
                true,
            )]);
        let rejected = factory(
            &BackfillSource::ClickHouse {
                database: "metrics".into(),
                table: "another_table".into(),
            },
            &typed,
        );
        assert!(matches!(rejected, Err(error) if error.to_string().contains("null-key encoding")));

        assert!(factory(
            &BackfillSource::ClickHouse {
                database: "another_database".into(),
                table: "samples".into(),
            },
            &materialization
        )
        .is_err());
        assert!(factory(
            &BackfillSource::ClickHouse {
                database: "metrics".into(),
                table: "samples; DROP TABLE x".into(),
            },
            &materialization
        )
        .is_err());
    }
}
