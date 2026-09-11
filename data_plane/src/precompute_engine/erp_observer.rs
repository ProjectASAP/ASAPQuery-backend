//! Bounded observations of actual materialization inputs, published only after
//! an explicit finite-source completion barrier. No worker timestamp is a seal.
use asap_types::erp_observation::*;
use asap_types::sds::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const MAX_POPULATIONS: usize = 128;
const MAX_KEYS: usize = 65_536;
const MAX_POPULATION_METADATA_BYTES: usize = 1_048_576;

struct Population {
    coordinates: SummaryInstanceCoordinates,
    semantics: ErpObservationInputSemantics,
    source: String,
    sketch: String,
    implementation: String,
    observer: BoundedFrequencyObserver<u64>,
}
#[derive(Default)]
struct Observations {
    generation: Option<CatalogGeneration>,
    populations: BTreeMap<SummaryInstanceId, Population>,
    extent: BTreeMap<SummaryDefinitionId, (i64, i64)>,
    total_keys: usize,
    metadata_bytes: usize,
    invalid: Option<String>,
}

pub struct RuntimeErpObserver {
    endpoint: String,
    observations: Mutex<Observations>,
}
impl RuntimeErpObserver {
    pub fn new(endpoint: String, generation: CatalogGeneration) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            observations: Mutex::new(Observations {
                generation: Some(generation),
                ..Default::default()
            }),
        })
    }
    pub fn observe(
        &self,
        generation: &CatalogGeneration,
        coordinates: SummaryInstanceCoordinates,
        config: &asap_types::AggregationConfig,
        timestamp_ms: i64,
        value: f64,
    ) {
        use asap_types::{AggregationType, SampleUpdateRule};
        let (semantics, sketch, implementation) = match config.aggregation_type {
            AggregationType::HLL => (
                ErpObservationInputSemantics::ScalarSampleValue,
                "hll",
                "asap-sketchlib-hll-regular-v1",
            ),
            AggregationType::UnivMon => (
                ErpObservationInputSemantics::UnitSampleFrequency,
                "univmon",
                "asap-sketchlib-univmon-standard-v1",
            ),
            _ => return,
        };
        let mut state = self.observations.lock().unwrap();
        // Catalog installation establishes identity. Delayed old worker input
        // cannot reset the current generation or create a partial replacement fit.
        if state.generation.as_ref() != Some(generation) {
            return;
        }
        if state.invalid.is_some() {
            return;
        }
        if !value.is_finite()
            || !matches!(
                config.sample_update_rule(),
                SampleUpdateRule::Value { scale: 1.0 }
            )
        {
            state.invalid = Some("unsupported non-finite or transformed summary input".into());
            for population in state.populations.values_mut() {
                population.observer = BoundedFrequencyObserver::new(1, 64).unwrap();
            }
            return;
        }
        let extent = state
            .extent
            .entry(coordinates.summary_definition_id)
            .or_insert((timestamp_ms, timestamp_ms));
        extent.0 = extent.0.min(timestamp_ms);
        extent.1 = extent.1.max(timestamp_ms);
        let Ok(id) = coordinates.instance_id() else {
            return;
        };
        if !state.populations.contains_key(&id) && state.populations.len() >= MAX_POPULATIONS {
            state.invalid = Some("ERP population observation budget exceeded".into());
            for population in state.populations.values_mut() {
                population.observer = BoundedFrequencyObserver::new(1, 64).unwrap();
            }
            return;
        }
        if !state.populations.contains_key(&id) {
            let bytes = coordinates
                .group_values
                .iter()
                .try_fold(0usize, |total, (key, value)| {
                    total
                        .checked_add(key.len())
                        .and_then(|n| n.checked_add(value.len()))
                });
            let total = bytes.and_then(|bytes| state.metadata_bytes.checked_add(bytes));
            if total.is_none_or(|bytes| bytes > MAX_POPULATION_METADATA_BYTES) {
                state.invalid = Some("ERP population metadata budget exceeded".into());
                for population in state.populations.values_mut() {
                    population.observer = BoundedFrequencyObserver::new(1, 64).unwrap();
                }
                return;
            }
            state.metadata_bytes = total.unwrap();
        }
        let population = state.populations.entry(id).or_insert_with(|| Population {
            source: format!(
                "summary-definition:{}",
                coordinates.summary_definition_id.as_u64()
            ),
            coordinates,
            semantics,
            sketch: sketch.into(),
            implementation: implementation.into(),
            observer: BoundedFrequencyObserver::new(MAX_KEYS, 64).unwrap(),
        });
        let before = population.observer.observed_key_count();
        // Canonical fixed-width numeric identity; no raw sample or arbitrary label copy.
        let key = if value == 0.0 { 0 } else { value.to_bits() };
        let range = population.coordinates.time_range;
        let duration = range.end_ms.saturating_sub(range.start_ms).max(1);
        let offset = timestamp_ms
            .saturating_sub(range.start_ms)
            .clamp(0, duration);
        let interval = ((offset as u128 * 64) / duration as u128).min(63) as usize;
        let result = population.observer.observe(key, interval);
        let added = population
            .observer
            .observed_key_count()
            .saturating_sub(before);
        state.total_keys = state.total_keys.saturating_add(added);
        if result.is_err() || state.total_keys > MAX_KEYS {
            state.invalid = Some("ERP key observation budget exceeded".into());
            for population in state.populations.values_mut() {
                population.observer = BoundedFrequencyObserver::new(1, 64).unwrap();
            }
        }
    }

    pub async fn publish_finite(
        &self,
        generation: &CatalogGeneration,
        now_ms: u64,
    ) -> Result<(), String> {
        use control_plane::runtime_samples::feedback::{
            runtime_samples_client::RuntimeSamplesClient, PushBatch, RuntimeRecord,
        };
        let records = {
            let state = self.observations.lock().unwrap();
            if state.generation.as_ref() != Some(generation) {
                return Ok(());
            }
            let mut groups: BTreeMap<
                (SummaryDefinitionId, i64, i64),
                (
                    String,
                    String,
                    String,
                    ErpPopulationObservations<EmpiricalFrequencyObservation>,
                ),
            > = BTreeMap::new();
            for (id, population) in &state.populations {
                let c = &population.coordinates;
                let Some((first, last)) = state.extent.get(&c.summary_definition_id) else {
                    continue;
                };
                if c.time_range.start_ms < *first || c.time_range.end_ms > *last {
                    continue;
                }
                let key = (
                    c.summary_definition_id,
                    c.time_range.start_ms,
                    c.time_range.end_ms,
                );
                let entry = groups.entry(key).or_insert_with(|| {
                    (
                        population.source.clone(),
                        population.sketch.clone(),
                        population.implementation.clone(),
                        ErpPopulationObservations {
                            schema_version: 1,
                            catalog_generation: generation.clone(),
                            summary_definition_id: c.summary_definition_id,
                            observed_at_unix_ms: now_ms,
                            window_start_ms: c.time_range.start_ms,
                            window_end_ms: c.time_range.end_ms,
                            input_semantics: population.semantics,
                            invalid_reason: state.invalid.clone(),
                            populations: Vec::new(),
                        },
                    )
                });
                match population.observer.snapshot() {
                    Some(shape) => entry.3.populations.push(ErpPopulationObservation {
                        population_id: id.clone(),
                        shape,
                    }),
                    None => entry.3.invalid_reason = Some("ERP population fit unavailable".into()),
                }
            }
            groups
                .into_values()
                .map(|(source, sketch, impl_name, mut envelope)| {
                    if envelope.invalid_reason.is_some() {
                        envelope.populations.clear();
                    }
                    RuntimeRecord {
                        source,
                        sketch,
                        impl_name,
                        schema_version: 1,
                        payload_json: serde_json::json!({"erp_population_observations":envelope})
                            .to_string(),
                    }
                })
                .collect::<Vec<_>>()
        };
        if records.is_empty() {
            return Ok(());
        }
        let expected = records.len() as u64;
        let mut client = RuntimeSamplesClient::connect(self.endpoint.clone())
            .await
            .map_err(|e| e.to_string())?;
        let ack = client
            .push(PushBatch { records })
            .await
            .map_err(|e| e.to_string())?
            .into_inner();
        if ack.accepted != expected {
            return Err("control plane rejected ERP observation records".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (CatalogGeneration, asap_types::AggregationConfig) {
        let config = asap_types::AggregationConfig::new(
            asap_types::AggregationType::HLL,
            String::new(),
            Default::default(),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            asap_types::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            60,
            asap_types::enums::WindowKind::Tumbling,
            String::new(),
            "m".into(),
            None,
            None,
            None,
        );
        (
            CatalogGeneration {
                schema_version: 1,
                plan_id: 1,
                plan_version: 1,
                snapshot_sha256: "a".repeat(64),
            },
            config,
        )
    }
    fn coordinate(group: usize) -> SummaryInstanceCoordinates {
        SummaryInstanceCoordinates {
            summary_definition_id: asap_types::PolicyFingerprint(1).into(),
            time_range: HalfOpenTimeRange {
                start_ms: 0,
                end_ms: 60_000,
            },
            group_values: BTreeMap::from([("job".into(), group.to_string())]),
        }
    }
    #[test]
    fn observations_keep_partitions_separate_and_invalidate_on_overflow() {
        let (generation, config) = fixture();
        let observer = RuntimeErpObserver::new("http://127.0.0.1:1".into(), generation.clone());
        observer.observe(&generation, coordinate(0), &config, 1, 1.0);
        observer.observe(&generation, coordinate(0), &config, 2, 1.0);
        observer.observe(&generation, coordinate(1), &config, 3, 2.0);
        {
            let state = observer.observations.lock().unwrap();
            assert_eq!(state.populations.len(), 2);
            assert_eq!(state.total_keys, 2);
            assert!(state
                .populations
                .values()
                .all(|p| p.observer.observed_key_count() == 1));
        }
        for group in 2..=MAX_POPULATIONS {
            observer.observe(&generation, coordinate(group), &config, 4, 3.0);
        }
        let state = observer.observations.lock().unwrap();
        assert!(state.invalid.is_some());
        assert!(state
            .populations
            .values()
            .all(|p| p.observer.snapshot().is_none()));
    }
    #[test]
    fn delayed_generation_cannot_reset_current_population_counts() {
        let (generation, config) = fixture();
        let observer = RuntimeErpObserver::new("http://127.0.0.1:1".into(), generation.clone());
        observer.observe(&generation, coordinate(0), &config, 1, 1.0);
        let mut stale = generation.clone();
        stale.plan_version = 0;
        observer.observe(&stale, coordinate(0), &config, 2, 99.0);
        observer.observe(&generation, coordinate(0), &config, 3, 2.0);
        let state = observer.observations.lock().unwrap();
        assert_eq!(state.total_keys, 2);
        assert_eq!(state.generation.as_ref(), Some(&generation));
        assert_eq!(
            state
                .populations
                .values()
                .next()
                .unwrap()
                .observer
                .snapshot()
                .unwrap()
                .sorted_counts,
            vec![1, 1]
        );
    }
}
