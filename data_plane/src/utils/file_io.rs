use crate::stores::types::StreamingConfig;
use anyhow::{Context, Result};

pub fn read_streaming_config(yaml_file: &str) -> Result<StreamingConfig> {
    let yaml_data = std::fs::read_to_string(yaml_file)
        .with_context(|| format!("Failed to read YAML file: {yaml_file}"))?;
    let yaml_data: serde_yaml::Value = serde_yaml::from_str(&yaml_data)
        .with_context(|| format!("Failed to parse YAML file: {yaml_file}"))?;

    let config = StreamingConfig::from_yaml_data(&yaml_data)
        .with_context(|| format!("Failed to parse YAML config from: {yaml_file}"))?;

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_read_streaming_config() {
        let streaming_yaml_content = r#"
aggregations:
- aggregationId: 1
  aggregationSubType: ''
  aggregationType: DatasketchesKLL
  labels:
    aggregated: []
    grouping:
    - instance
    - job
    rollup: []
  metric: fake_metric_total
  parameters:
    K: 200
  spatialFilter: ''
  windowSize: 10
  numAggregatesToRetain: 6
"#;

        let mut streaming_temp_file = NamedTempFile::new().unwrap();
        write!(streaming_temp_file, "{streaming_yaml_content}").unwrap();

        let config =
            read_streaming_config(streaming_temp_file.path().to_str().unwrap()).unwrap();
        assert!(!config.aggregation_configs.is_empty());
        let agg = config.get_aggregation_config(1).expect("agg 1");
        assert_eq!(agg.num_aggregates_to_retain, Some(6));
    }
}
