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
}

#[derive(Deserialize)]
struct ClickHouseSampleRow {
    labels: String,
    timestamp_ms: i64,
    value: f64,
}

impl ClickHouseReader {
    pub fn new(config: ClickHouseReaderConfig) -> Result<Self, RawSampleReaderError> {
        config.validate()?;
        Ok(Self {
            config,
            http: reqwest::Client::new(),
        })
    }

    fn sql(&self) -> String {
        let c = &self.config;
        format!(
            "SELECT {labels} AS labels, {timestamp} AS timestamp_ms, {value} AS value \
             FROM {database}.{table} WHERE {metric} = {{metric:String}} \
             AND {timestamp} >= {{start_ms:Int64}} AND {timestamp} < {{end_ms:Int64}} \
             ORDER BY labels, timestamp_ms FORMAT JSONEachRow",
            labels = c.labels_column,
            timestamp = c.timestamp_ms_column,
            value = c.value_column,
            database = c.database,
            table = c.table,
            metric = c.metric_column,
        )
    }
}

/// Adds ClickHouse to the existing backfill lifecycle without extending the
/// shared `BackfillSource` enum. Jobs opt in with the reserved
/// `Prometheus { url: "clickhouse://configured" }` source marker; all other
/// sources retain the default factory behavior.
pub fn clickhouse_reader_factory(config: ClickHouseReaderConfig) -> ReaderFactory {
    let fallback = super::service::default_reader_factory();
    Arc::new(move |source| match source {
        BackfillSource::Prometheus { url } if url == "clickhouse://configured" => {
            Ok(Arc::new(ClickHouseReader::new(config.clone())?) as Arc<dyn RawSampleReader>)
        }
        source => fallback(source),
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
            let sample = RawSample {
                labels: row.labels,
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
    fn configured_source_marker_enters_clickhouse_backfill_lifecycle() {
        let factory = clickhouse_reader_factory(config("samples"));
        let reader = factory(&BackfillSource::Prometheus {
            url: "clickhouse://configured".into(),
        })
        .unwrap();
        assert_eq!(reader.source_name(), "ClickHouseReader");
    }
}
