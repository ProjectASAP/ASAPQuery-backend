//! Versioned runtime evidence about the inputs of one installed summary.
//! Shape is generic so the transport contract does not depend on a planner.
use crate::sds::{CatalogGeneration, SummaryDefinitionId, SummaryInstanceId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErpObservationInputSemantics {
    ScalarSampleValue,
    UnitSampleFrequency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErpPopulationObservation<Shape> {
    pub population_id: SummaryInstanceId,
    pub shape: Shape,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErpPopulationObservations<Shape> {
    pub schema_version: u32,
    pub catalog_generation: CatalogGeneration,
    pub summary_definition_id: SummaryDefinitionId,
    pub observed_at_unix_ms: u64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub input_semantics: ErpObservationInputSemantics,
    /// Any overflow or unsupported input invalidates the entire envelope.
    /// Invalid envelopes contain no population fits.
    pub invalid_reason: Option<String>,
    pub populations: Vec<ErpPopulationObservation<Shape>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErpObservationFreshness {
    pub max_age_ms: u64,
    pub max_future_skew_ms: u64,
}

impl<S> ErpPopulationObservations<S> {
    pub fn validate_identity_and_freshness(
        &self,
        expected_generation: &CatalogGeneration,
        expected_definition: SummaryDefinitionId,
        now_ms: u64,
        freshness: ErpObservationFreshness,
    ) -> Result<(), &'static str> {
        if self.schema_version != 1
            || &self.catalog_generation != expected_generation
            || self.summary_definition_id != expected_definition
        {
            return Err("ERP observation belongs to a different catalog or summary");
        }
        if self.invalid_reason.is_some()
            || self.populations.is_empty()
            || self.window_start_ms >= self.window_end_ms
        {
            return Err("ERP observation is incomplete or invalid");
        }
        if freshness.max_age_ms == 0
            || self.observed_at_unix_ms > now_ms.saturating_add(freshness.max_future_skew_ms)
            || now_ms.saturating_sub(self.observed_at_unix_ms) > freshness.max_age_ms
        {
            return Err("ERP observation is stale or future-dated");
        }
        let mut seen = std::collections::BTreeSet::new();
        if self
            .populations
            .iter()
            .any(|p| !seen.insert(&p.population_id))
        {
            return Err("ERP observation repeats a population");
        }
        Ok(())
    }
}
