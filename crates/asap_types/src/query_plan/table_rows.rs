//! Snapshot maintenance is explicitly bounded-stale SQL, not PromQL lookback.
use planner_types::post_asap::{
    maintained_population::PopulationReadout, SummaryFamilyType, SummarySchema,
};
use planner_types::pre_asap::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableRowsMaintenance {
    pub database: String,
    pub refresh_interval_ms: u64,
    pub max_snapshot_age_ms: u64,
    pub max_rows: usize,
    pub max_bytes: usize,
}
impl TableRowsMaintenance {
    pub fn validate(&self) -> Result<(), String> {
        if self.database.trim().is_empty()
            || self.refresh_interval_ms == 0
            || self.max_snapshot_age_ms < self.refresh_interval_ms
            || self.max_snapshot_age_ms > 3_600_000
            || self.max_rows == 0
            || self.max_rows > 1_000_000
            || self.max_bytes == 0
            || self.max_bytes > 1_073_741_824
        {
            return Err("invalid table snapshot maintenance bounds".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TableRowsPopulation {
    /// Compiler-rendered complete scan, including predicates; never a time slice.
    pub snapshot_sql: String,
    pub input_schema: SummarySchema,
    pub value_column: usize,
    pub grouping: Vec<usize>,
    pub max_k: usize,
    pub quantiles: bool,
    /// Unconfigured candidates are exportable, but are not deployable.
    pub maintenance: Option<TableRowsMaintenance>,
}
impl TableRowsPopulation {
    pub fn key(&self) -> String {
        serde_json::to_string(self).expect("table population serializes")
    }
    pub fn validate_output(
        &self,
        readout: &PopulationReadout,
        output: &SummarySchema,
    ) -> Result<(), String> {
        self.validate(readout)?;
        if output.fields.iter().any(|f| {
            !matches!(
                f.dtype,
                SummaryFamilyType::Plain(
                    DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Bool
                )
            )
        }) {
            return Err("table population output requires supported scalar SQL columns".into());
        }
        if matches!(readout, PopulationReadout::TopK { .. }) {
            if output != &self.input_schema {
                return Err("table TopK output must preserve row schema".into());
            }
        } else {
            let value_type = if matches!(readout, PopulationReadout::Count) {
                DataType::Int64
            } else {
                DataType::Float64
            };
            if output.fields.len() != self.grouping.len() + 1
                || self
                    .grouping
                    .iter()
                    .enumerate()
                    .any(|(i, k)| output.fields[i] != self.input_schema.fields[*k])
                || output
                    .fields
                    .last()
                    .is_none_or(|f| f.dtype != SummaryFamilyType::Plain(value_type.clone()))
            {
                return Err("table aggregate output schema is incompatible with readout".into());
            }
        }
        Ok(())
    }

    pub fn validate(&self, readout: &PopulationReadout) -> Result<(), String> {
        self.maintenance
            .as_ref()
            .ok_or("table population requires explicit maintenance policy")?
            .validate()?;
        if self.snapshot_sql.is_empty()
            || !self
                .input_schema
                .fields
                .get(self.value_column)
                .is_some_and(|f| {
                    f.dtype == SummaryFamilyType::Plain(DataType::Float64) && !f.nullable
                })
            || self
                .grouping
                .iter()
                .any(|k| *k >= self.input_schema.fields.len())
            || self.max_k > self.maintenance.as_ref().unwrap().max_rows
        {
            return Err("invalid table population schema or resource contract".into());
        }
        match readout {
            PopulationReadout::Quantile { q } if !self.quantiles || !(0.0..=1.0).contains(q) => {
                Err("invalid table quantile readout".into())
            }
            PopulationReadout::TopK { k } if *k > self.max_k => {
                Err("table TopK exceeds maintained cache".into())
            }
            _ => Ok(()),
        }
    }
}
