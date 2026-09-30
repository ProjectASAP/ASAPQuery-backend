//! Internal execution indexes derived exclusively from a validated DAG plan.
use anyhow::Result;
use std::collections::HashMap;
use std::ops::Index;

use super::storage_backend::StorageBackend;
use asap_types::sds::StoredOutputId;
use asap_types::PrecomputeMaterialization;
use planner_types::post_asap::SummaryFamilyType;

#[derive(Debug, Clone)]
pub struct InstalledPrecomputePlan {
    pub(crate) partitioning: crate::precompute_engine::partitioning::DagPartitioning,
    pub(crate) raw_programs:
        HashMap<u64, std::sync::Arc<crate::precompute_engine::raw_dag::RawDagProgram>>,
    pub(crate) precompute_plan: Option<asap_types::precompute_plan::PrecomputePlan>,
    pub(crate) materializations_by_output: HashMap<StoredOutputId, PrecomputeMaterialization>,
    /// Each output's stored state family, from its schema contract.
    state_families: HashMap<StoredOutputId, SummaryFamilyType>,
    /// Each output's canonical input predicate, from its Planner DAG scan.
    population_filters: HashMap<StoredOutputId, String>,
    /// The per-item dimension of outputs whose Planner update keys items by a label.
    item_labels: HashMap<StoredOutputId, String>,
    pub(crate) storage_backend: StorageBackend,
}

impl InstalledPrecomputePlan {
    fn derived_view(
        outputs: impl IntoIterator<Item = (PrecomputeMaterialization, SummaryFamilyType)>,
    ) -> Self {
        let mut materializations = HashMap::new();
        let mut state_families = HashMap::new();
        for (config, family) in outputs {
            state_families.insert(config.stored_output_id, family);
            materializations.insert(config.stored_output_id, config);
        }
        Self {
            partitioning: Default::default(),
            raw_programs: HashMap::new(),
            precompute_plan: None,
            materializations_by_output: materializations,
            state_families,
            population_filters: HashMap::new(),
            item_labels: HashMap::new(),
            storage_backend: StorageBackend::default(),
        }
    }

    /// Production construction always validates the executable DAG and bindings.
    pub fn from_precompute_plan(plan: asap_types::precompute_plan::PrecomputePlan) -> Result<Self> {
        let materializations = plan.runtime_materializations()?;
        let lookup = plan.lookup().map_err(anyhow::Error::msg)?;
        let outputs = materializations
            .into_values()
            .map(|config| {
                let family = lookup
                    .state_family(config.stored_output_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("stored output has no state schema"))?;
                Ok((config, family))
            })
            .collect::<Result<Vec<_>>>()?;
        let population_filters = outputs
            .iter()
            .map(|(config, _)| {
                lookup
                    .population_filter(config)
                    .map(|filter| (config.stored_output_id, filter))
                    .map_err(anyhow::Error::msg)
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut item_labels = HashMap::new();
        for (config, _) in &outputs {
            use planner_types::post_asap::{PostAsapOperatorPayload, SummaryInputExpr};
            use planner_types::pre_asap::ColumnRef;
            let Some((node, _)) = lookup
                .summary_producer(config.stored_output_id)
                .map_err(anyhow::Error::msg)?
            else {
                continue;
            };
            if let PostAsapOperatorPayload::SummaryAgg {
                input:
                    planner_types::post_asap::SummaryUpdate {
                        item:
                            Some(SummaryInputExpr::Column(
                                ColumnRef::Named(label) | ColumnRef::Qualified { name: label, .. },
                            )),
                        ..
                    },
                ..
            } = node.payload
            {
                item_labels.insert(config.stored_output_id, label);
            }
        }
        let materializations: HashMap<_, _> = outputs
            .iter()
            .map(|(config, _)| (config.stored_output_id, config.clone()))
            .collect();
        let mut programs = HashMap::new();
        for config in materializations.values().filter(|config| {
            config.derived_input.is_none()
                && plan.ingest.protocol
                    == asap_types::precompute_plan::IngestProtocol::PrometheusRemoteWriteV1
        }) {
            let program =
                crate::precompute_engine::raw_dag::RawDagProgram::from_plan(&plan, config)
                    .map_err(anyhow::Error::msg)?;
            programs.insert(config.policy_fp_u64(), std::sync::Arc::new(program));
        }
        drop(lookup);
        let mut view = Self::derived_view(outputs);
        view.population_filters = population_filters;
        view.item_labels = item_labels;
        view.partitioning =
            crate::precompute_engine::partitioning::DagPartitioning::from_plan(&plan);
        view.precompute_plan = Some(plan);
        view.raw_programs = programs;
        Ok(view)
    }

    // Isolated kernel/storage fixtures can omit a physical installation. This
    // constructor is absent from the production library and binary.
    /// Each output is paired with its stored state family.
    #[cfg(test)]
    pub fn new(
        outputs: impl IntoIterator<Item = (PrecomputeMaterialization, SummaryFamilyType)>,
    ) -> Self {
        Self::derived_view(outputs)
    }

    #[cfg(test)]
    pub fn with_storage_backend(
        outputs: impl IntoIterator<Item = (PrecomputeMaterialization, SummaryFamilyType)>,
        storage_backend: StorageBackend,
    ) -> Self {
        let mut view = Self::derived_view(outputs);
        view.storage_backend = storage_backend;
        view
    }

    pub fn stored_output_reference(
        &self,
        definition: asap_types::sds::StoredOutputId,
    ) -> Option<asap_types::sds::StoredOutputReference> {
        let selected = self.precompute_plan.as_ref().and_then(|plan| {
            plan.schemas
                .iter()
                .find(|schema| schema.materialization == definition)
                .map(|schema| schema.stored_output_reference.clone())
        });
        #[cfg(test)]
        let selected = selected.or_else(|| {
            let outputs = self
                .materializations_by_output
                .values()
                .map(|config| {
                    Some((
                        config,
                        self.state_family(config.stored_output_id)?,
                        self.population_filter(config.stored_output_id).to_owned(),
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            asap_types::summary_catalog::SummaryCatalog::from_outputs(0, 0, outputs)
                .ok()?
                .output_reference(definition)
                .ok()
        });
        selected
    }

    pub fn plan(&self) -> &asap_types::precompute_plan::PrecomputePlan {
        self.precompute_plan
            .as_ref()
            .expect("installed plan has an authoritative source")
    }

    pub fn storage_backend(&self) -> StorageBackend {
        self.storage_backend
    }

    pub fn get_aggregation_config(
        &self,
        output: StoredOutputId,
    ) -> Option<&PrecomputeMaterialization> {
        self.materializations_by_output.get(&output)
    }

    pub fn materializations(&self) -> &HashMap<StoredOutputId, PrecomputeMaterialization> {
        &self.materializations_by_output
    }

    pub fn contains(&self, output: StoredOutputId) -> bool {
        self.materializations_by_output.contains_key(&output)
    }

    /// The stored state family of `output`.
    pub fn state_family(&self, output: StoredOutputId) -> Option<&SummaryFamilyType> {
        self.state_families.get(&output)
    }

    /// The physical stored-state kind of `output`.
    pub fn agg_kind(
        &self,
        output: StoredOutputId,
    ) -> Option<crate::storage_engines::sketch_db::index::AggKind> {
        self.state_family(output).map(|family| {
            crate::storage_engines::sketch_db::data::agg_kind_for_family(
                family,
                self.population_filter(output),
            )
        })
    }

    /// Whether `output`'s panes follow PromQL's `(start, end]` range
    /// convention. Raw Planner programs read only time-series scans, whose
    /// ranges are PromQL ranges.
    pub fn right_closed_panes(&self, output: StoredOutputId) -> bool {
        self.raw_programs.contains_key(&output.as_u64())
    }

    /// The label that keys `output`'s items, when its Planner update has one.
    pub fn item_label(&self, output: StoredOutputId) -> Option<&str> {
        self.item_labels.get(&output).map(String::as_str)
    }

    /// The canonical input predicate of `output`; empty when unfiltered.
    pub fn population_filter(&self, output: StoredOutputId) -> &str {
        self.population_filters
            .get(&output)
            .map_or("", String::as_str)
    }
}

impl Index<StoredOutputId> for InstalledPrecomputePlan {
    type Output = PrecomputeMaterialization;
    fn index(&self, output: StoredOutputId) -> &Self::Output {
        &self.materializations_by_output[&output]
    }
}

impl Default for InstalledPrecomputePlan {
    fn default() -> Self {
        use control_plane::physical::compiler::{
            PlanEnvelope, PrecomputePlan, BACKEND_COMPAT, PLANNER_REVISION,
        };
        let envelope = PlanEnvelope {
            plan_id: 0,
            plan_version: 0,
            generated_at_unix_ms: 0,
            activation_unix_ms: 0,
            expiry_unix_ms: None,
            backend_compat: BACKEND_COMPAT.into(),
            planner_revision: PLANNER_REVISION.into(),
            capability_snapshot_id: "empty-installation".into(),
        };
        Self::from_precompute_plan(
            PrecomputePlan::build(envelope, vec![], &[]).expect("valid empty plan"),
        )
        .expect("valid empty installation")
    }
}

#[cfg(test)]
mod tests {
    fn filtered_plan(job: &str) -> control_plane::physical::compiler::PrecomputePlan {
        let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot["query_workload"]["repeating_queries"][0]["query"] =
            serde_json::json!(format!("quantile_over_time(0.99, m{{job=\"{job}\"}}[1m])"));
        crate::tests::test_utilities::planning::quoted_snapshot(
            serde_json::from_value(snapshot).unwrap(),
            false,
        )
        .compile_promql()
        .unwrap()
        .precompute_plan
    }

    /// Rebind every materialization node of `plan`'s DAGs to `output`.
    fn bind_all(plan: &mut control_plane::physical::compiler::PrecomputePlan, output: u64) {
        for installed in plan.executable_dags.values_mut() {
            for binding in installed.binding.nodes.values_mut() {
                if let asap_types::executable_plan::BackendNodeBinding::Materialization {
                    stored_output,
                } = binding
                {
                    *stored_output = asap_types::sds::StoredOutputId(output);
                }
            }
        }
    }

    // Two DAGs that read different populations cannot share one stored output.
    #[test]
    fn producers_disagreeing_on_population_are_rejected() {
        let mut plan = filtered_plan("a");
        let output = plan.materializations[0].stored_output_id.as_u64();
        let mut other = filtered_plan("b");
        bind_all(&mut other, output);
        let (_, mut dag) = other.executable_dags.into_iter().next().unwrap();
        dag.document.query_id = "other-population".into();
        plan.executable_dags.insert("other-population".into(), dag);
        let error = plan.validate().unwrap_err().to_string();
        assert!(error.contains("disagree on its computation"), "{error}");
    }

    // A raw output with no Planner producer is not read as unfiltered.
    #[test]
    fn unbound_raw_output_is_rejected() {
        let mut plan = filtered_plan("a");
        let mut unbound = plan.materializations[0].clone();
        unbound.metric = "unbound".into();
        unbound.stored_output_id = asap_types::sds::StoredOutputId(1);
        let mut schema = plan.schemas[0].clone();
        schema.materialization = unbound.stored_output_id;
        schema.schema_id = schema.schema_id.replace(
            &plan.materializations[0]
                .stored_output_id
                .as_u64()
                .to_string(),
            "1",
        );
        schema.stored_output_reference =
            asap_types::sds::StoredOutputReference::for_output(unbound.stored_output_id);
        plan.materializations.push(unbound);
        plan.schemas.push(schema);
        let error = plan.validate().unwrap_err().to_string();
        assert!(
            error.contains("raw output has no Planner producer"),
            "{error}"
        );
    }

    // A raw output's input predicate, pane convention and stored state kind
    // come from its installed Planner producer, not from the materialization.
    #[test]
    fn raw_output_computation_comes_from_its_planner_producer() {
        let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../docs/examples/asapquery-planning-snapshot.json"
        ))
        .unwrap();
        snapshot["query_workload"]["repeating_queries"][0]["query"] =
            serde_json::json!("quantile_over_time(0.99, m{job=\"api\"}[1m])");
        let plan = crate::tests::test_utilities::planning::quoted_snapshot(
            serde_json::from_value(snapshot).unwrap(),
            false,
        )
        .compile_promql()
        .unwrap();
        let installed =
            super::InstalledPrecomputePlan::from_precompute_plan(plan.precompute_plan.clone())
                .unwrap();
        let output = plan.precompute_plan.materializations[0].stored_output_id;
        assert_eq!(installed.population_filter(output), "{job=\"api\"}");
        assert!(installed.right_closed_panes(output));
        let family = installed.state_family(output).unwrap();
        assert_eq!(Some(family), plan.precompute_plan.state_family(output));
        assert_eq!(
            installed.agg_kind(output).unwrap().canonical_string(),
            crate::storage_engines::sketch_db::data::agg_kind_for_family(family, "{job=\"api\"}")
                .canonical_string()
        );
        let data = &plan.summary_catalog.data_descriptors
            [&plan.summary_catalog.outputs[&output].data_descriptor_id];
        assert_eq!(data.population_filter_canonical, "{job=\"api\"}");
    }

    // Flat lists cannot enter through the authoritative physical-plan document.
    #[test]
    fn rejects_flat_aggregation_documents() {
        for text in [r#"{"aggregation_configs":{}}"#, "aggregations: []"] {
            assert!(
                serde_yaml::from_str::<asap_types::plan_publication::PhysicalPlanInstallRequest>(
                    text
                )
                .is_err()
            );
        }
    }
}
