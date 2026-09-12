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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErpPopulationObservation<Shape> {
    pub population_id: SummaryInstanceId,
    pub shape: Shape,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Empirical sufficient statistics for frequency-shape fitting. No raw keys,
/// raw samples, distribution-family assertion, or planner configuration crosses the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmpiricalFrequencyObservation {
    pub sorted_counts: Vec<u64>,
    pub interval_counts: Vec<u64>,
}
impl EmpiricalFrequencyObservation {
    pub fn event_count(&self) -> Option<u64> {
        if self.sorted_counts.len() > 65_536
            || self.interval_counts.len() > 256
            || self.sorted_counts.is_empty()
            || self.sorted_counts.contains(&0)
            || self.sorted_counts.windows(2).any(|w| w[0] < w[1])
        {
            return None;
        }
        let total = self
            .sorted_counts
            .iter()
            .try_fold(0u64, |sum, n| sum.checked_add(*n))?;
        let intervals = self
            .interval_counts
            .iter()
            .try_fold(0u64, |sum, n| sum.checked_add(*n))?;
        (total == intervals).then_some(total)
    }
}

/// Bounded exact frequency counting. Fitting and ERP selection remain control-plane work.
#[derive(Debug)]
pub struct BoundedFrequencyObserver<K> {
    frequencies: std::collections::HashMap<K, u64>,
    intervals: std::collections::HashMap<usize, u64>,
    max_keys: usize,
    max_intervals: usize,
    events: u64,
    invalid: bool,
}
impl<K: Eq + std::hash::Hash> BoundedFrequencyObserver<K> {
    pub fn new(max_keys: usize, max_intervals: usize) -> Result<Self, &'static str> {
        if max_keys == 0 || max_intervals == 0 {
            return Err("observation limits must be positive");
        }
        Ok(Self {
            frequencies: Default::default(),
            intervals: Default::default(),
            max_keys,
            max_intervals,
            events: 0,
            invalid: false,
        })
    }
    pub fn observed_key_count(&self) -> usize {
        self.frequencies.len()
    }
    pub fn invalidate(&mut self) {
        self.invalid = true;
    }
    pub fn observe(&mut self, key: K, interval: usize) -> Result<(), &'static str> {
        if self.invalid {
            return Err("ERP observation was invalidated");
        }
        if (!self.frequencies.contains_key(&key) && self.frequencies.len() >= self.max_keys)
            || (!self.intervals.contains_key(&interval)
                && self.intervals.len() >= self.max_intervals)
        {
            self.invalid = true;
            return Err("ERP observation key or interval budget exceeded");
        }
        let Some(events) = self.events.checked_add(1) else {
            self.invalid = true;
            return Err("ERP observation count overflow");
        };
        *self.frequencies.entry(key).or_default() += 1;
        *self.intervals.entry(interval).or_default() += 1;
        self.events = events;
        Ok(())
    }
    pub fn snapshot(&self) -> Option<EmpiricalFrequencyObservation> {
        if self.invalid || self.frequencies.is_empty() {
            return None;
        }
        let mut sorted_counts: Vec<_> = self.frequencies.values().copied().collect();
        sorted_counts.sort_unstable_by(|a, b| b.cmp(a));
        Some(EmpiricalFrequencyObservation {
            sorted_counts,
            interval_counts: self.intervals.values().copied().collect(),
        })
    }
}

impl<S> ErpPopulationObservations<S> {
    pub fn try_map_shapes<T>(
        self,
        mut convert: impl FnMut(S) -> Option<T>,
    ) -> Option<ErpPopulationObservations<T>> {
        let populations = self
            .populations
            .into_iter()
            .map(|population| {
                Some(ErpPopulationObservation {
                    population_id: population.population_id,
                    shape: convert(population.shape)?,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(ErpPopulationObservations {
            schema_version: self.schema_version,
            catalog_generation: self.catalog_generation,
            summary_definition_id: self.summary_definition_id,
            observed_at_unix_ms: self.observed_at_unix_ms,
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            input_semantics: self.input_semantics,
            invalid_reason: self.invalid_reason,
            populations,
        })
    }
}

#[cfg(test)]
mod empirical_tests {
    use super::*;
    #[test]
    fn count_overflow_invalidates_instead_of_publishing_prefix() {
        let mut observer = BoundedFrequencyObserver::new(2, 2).unwrap();
        observer.observe(1u64, 0).unwrap();
        observer.events = u64::MAX;
        assert!(observer.observe(1, 0).is_err());
        assert!(observer.snapshot().is_none());
    }
    #[test]
    fn malformed_frequency_counts_fail_closed() {
        assert!(EmpiricalFrequencyObservation {
            sorted_counts: vec![2, 1],
            interval_counts: vec![2]
        }
        .event_count()
        .is_none());
        assert!(EmpiricalFrequencyObservation {
            sorted_counts: vec![1, 2],
            interval_counts: vec![3]
        }
        .event_count()
        .is_none());
    }
}
