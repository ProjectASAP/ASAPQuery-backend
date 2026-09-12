//! Maintained current-value populations, shared independently of q and k.
use super::{
    logical::{Grouping, LabelMatcher},
    QueryPlanError,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SeriesPopulation {
    pub metric: String,
    pub matchers: Vec<LabelMatcher>,
    pub grouping: Grouping,
    pub lookback_ms: u64,
    pub max_input_lag_ms: u64,
    pub max_series: usize,
    pub max_bytes: u64,
    pub max_k: u64,
    pub quantiles: bool,
}
impl SeriesPopulation {
    pub fn key(&self) -> String {
        serde_json::to_string(self).expect("serializable current-series population")
    }
    pub fn validate(&self) -> Result<(), QueryPlanError> {
        if self.metric.is_empty()
            || self.lookback_ms != 300_000
            || self.max_input_lag_ms == 0
            || self.max_input_lag_ms > self.lookback_ms
            || self.max_series == 0
            || self.max_series > 100_000
            || self.max_bytes == 0
            || self.max_bytes > 1_073_741_824
            || self.max_k > self.max_series as u64
        {
            return Err(QueryPlanError::Invalid(
                "invalid current-series population bounds".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SeriesReadout {
    Quantile { q: f64 },
    TopK { k: u64 },
}
