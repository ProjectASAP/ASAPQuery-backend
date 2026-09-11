//! Bounded observations of actual materialization inputs, published only after
//! an explicit finite-source completion barrier. No worker timestamp is a seal.
use asap_types::erp_observation::*;
use asap_types::sds::*;
use control_plane::physical::erp::{ErpObservedShape, ErpShapeObserver};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const MAX_POPULATIONS: usize = 128;
const MAX_KEYS: usize = 65_536;

struct Population {
    coordinates: SummaryInstanceCoordinates,
    semantics: ErpObservationInputSemantics,
    source: String,
    sketch: String,
    implementation: String,
    observer: ErpShapeObserver,
}
#[derive(Default)]
struct Observations {
    generation: Option<CatalogGeneration>,
    populations: BTreeMap<SummaryInstanceId, Population>,
    extent: BTreeMap<SummaryDefinitionId, (i64, i64)>,
    total_keys: usize,
    invalid: Option<String>,
}

pub struct RuntimeErpObserver {
    endpoint: String,
    observations: Mutex<Observations>,
}
impl RuntimeErpObserver {
    pub fn new(endpoint: String) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            observations: Mutex::new(Observations::default()),
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
        if state.generation.as_ref() != Some(generation) {
            *state = Observations {
                generation: Some(generation.clone()),
                ..Default::default()
            };
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
                population.observer = ErpShapeObserver::new(1).unwrap();
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
                population.observer = ErpShapeObserver::new(1).unwrap();
            }
            return;
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
            observer: ErpShapeObserver::new(MAX_KEYS).unwrap(),
        });
        let before = population.observer.observed_key_count();
        // Canonical fixed-width numeric identity; no raw sample or arbitrary label copy.
        let key = format!("{:016x}", if value == 0.0 { 0 } else { value.to_bits() });
        let result = population.observer.observe(&key, 0);
        let added = population
            .observer
            .observed_key_count()
            .saturating_sub(before);
        state.total_keys = state.total_keys.saturating_add(added);
        if result.is_err() || state.total_keys > MAX_KEYS {
            state.invalid = Some("ERP key observation budget exceeded".into());
            for population in state.populations.values_mut() {
                population.observer = ErpShapeObserver::new(1).unwrap();
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
                    ErpPopulationObservations<ErpObservedShape>,
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
