//! Bind raw ingestion to a selected Planner producer and its raw dependency edge.
//! The backend supplies one typed sample batch per pane; the Planner-compiled
//! precompute graph owns every update, grouping and item computation.
use crate::storage_engines::types::{AggregateCore, KeyByLabelValues};
use asap_physical_operators::{
    operators::Operator,
    physical_planner::{precompute, CompiledPhysicalDag, Source as PhysicalSource},
    runtime::{Limits, RunContext, Scope},
    values::{Batch, Value},
};
use asap_summary_state::factory::{create_planner_accumulator, AccumulatorUpdater};
use asap_types::physical_plan_codec::PhysicalPlanCodec;
use asap_types::{executable_plan::BackendNodeBinding, PrecomputeMaterialization};
use planner_types::post_asap::{
    EdgeRole, GroupingStrategy, PostAsapNodeId, PostAsapOperatorPayload, SummaryFamilyType,
    SummaryInputExpr, SummaryUpdate,
};
use planner_types::pre_asap::{ColumnRef, QueryExpr, Source};
use std::collections::HashMap;

/// A validated executable projection; semantics come from the installed node.
/// The retained node ID makes failures attributable to the selected DAG.
#[derive(Debug, Clone)]
pub struct RawDagProgram {
    pub node: PostAsapNodeId,
    pub family: SummaryFamilyType,
    pub input: SummaryUpdate,
    pub grouping: GroupingStrategy,
    pub reduction: planner_types::pre_asap::Reduction,
    projected_column: Option<String>,
    /// Planner's encoded precompute graph from the raw sample boundary to this
    /// output. Decoded graphs are not `Send`, so each execution decodes it.
    program: std::sync::Arc<[u8]>,
    source: u64,
}

impl RawDagProgram {
    pub fn from_plan(
        plan: &asap_types::precompute_plan::PrecomputePlan,
        config: &PrecomputeMaterialization,
    ) -> Result<Self, String> {
        let mut selected: Option<Self> = None;
        for installed in plan.executable_dags.values() {
            installed.validate()?;
            let dag = installed.document.decode()?;
            for node in &dag.nodes {
                if !matches!(installed.binding.node(node.id), Some(BackendNodeBinding::Materialization { stored_output }) if stored_output.fingerprint() == config.policy_fingerprint())
                {
                    continue;
                }
                let PostAsapOperatorPayload::SummaryAgg {
                    family,
                    input,
                    grouping,
                    reduction,
                } = &node.payload
                else {
                    return Err(
                        "raw materialization binding must identify a Planner SummaryAgg".into(),
                    );
                };
                if config.derived_input.is_some() {
                    return Err("derived producer must execute through maintenance DAG".into());
                }
                let incoming: Vec<_> = dag.edges.iter().filter(|e| e.consumer == node.id).collect();
                let [edge] = incoming.as_slice() else {
                    return Err("raw SummaryAgg must have exactly one DAG input".into());
                };
                if edge.role != EdgeRole::Input {
                    return Err("raw SummaryAgg input edge has wrong role".into());
                }
                let source = dag
                    .nodes
                    .iter()
                    .find(|n| n.id == edge.producer)
                    .ok_or("missing raw DAG input")?;
                let PostAsapOperatorPayload::Fallback { expression } = &source.payload else {
                    return Err("raw producer requires an executable source input; maintenance edges cannot be bypassed".into());
                };
                let scan = match expression {
                    QueryExpr::TimeRange { child, .. } => child.as_ref(),
                    source => source,
                };
                match scan {
                    QueryExpr::Scan {
                        source: Source::TimeSeries { metric },
                        ..
                    } if metric == &config.metric => {
                        let (metric, window, filter) =
                            control_plane::physical::compiler::raw_time_series_input_contract(
                                expression,
                                matches!(family, SummaryFamilyType::ExactAggregate(..)),
                            )?;
                        if metric != config.metric
                            || window.is_some_and(|seconds| seconds != config.window_size)
                            || asap_types::utils::normalize_spatial_filter(&filter)
                                != config.spatial_filter_normalized
                        {
                            return Err(
                                "raw DAG source filter/window differs from physical binding".into(),
                            );
                        }
                    }
                    QueryExpr::Scan {
                        source: Source::Table { table_ref },
                        ..
                    } if config.table_name.as_ref() == Some(table_ref) => {
                        return Err(
                            "raw table execution requires a validated table scan executor".into(),
                        );
                    }
                    _ => return Err("raw DAG input does not match installed source routing".into()),
                }
                if let planner_types::pre_asap::Reduction::Reduce(keys) = reduction {
                    if keys.is_without() {
                        return Err(
                            "raw without reduction requires explicit dynamic population routing"
                                .into(),
                        );
                    }
                    let names = keys
                        .keys()
                        .iter()
                        .map(|id| {
                            source
                                .output_schema
                                .fields
                                .get(*id)
                                .map(|f| f.name.clone())
                                .ok_or("missing reduction column")
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if names != config.grouping_labels.names() {
                        return Err("DAG reduction differs from physical population binding".into());
                    }
                }
                if let SummaryFamilyType::ExactAggregate(kind, _) = family {
                    if input.item.is_some() {
                        return Err(
                            "raw exact populations must follow Planner reduction, not an item map"
                                .into(),
                        );
                    }
                    if !matches!(input.weight, SummaryInputExpr::Column(_))
                        && !(matches!(kind, planner_types::post_asap::ExactKind::Count)
                            && input.weight == SummaryInputExpr::Constant(1.0))
                    {
                        return Err("raw exact update differs from stored source projection".into());
                    }
                }
                if &config.accumulator_spec().map_err(|e| e.to_string())?.family != family {
                    return Err(
                        "materialization storage family differs from selected Planner node".into(),
                    );
                }
                // The stored descriptor must name the same update semantics; its
                // content identity cannot be reused for an unrelated DAG program.
                let update_matches = match (&input.weight, config.sample_update_rule()) {
                    (
                        SummaryInputExpr::Column(_),
                        asap_types::SampleUpdateRule::Value { scale },
                    ) => scale == 1.0,
                    (SummaryInputExpr::Constant(value), asap_types::SampleUpdateRule::Count) => {
                        *value == 1.0
                    }
                    _ => {
                        asap_types::accumulator_spec::is_unit_sample_frequency(input)
                            || (matches!(
                                family,
                                SummaryFamilyType::ExactAggregate(
                                    planner_types::post_asap::ExactKind::Count,
                                    _
                                )
                            ) && input.weight == SummaryInputExpr::Constant(1.0))
                    }
                };
                if !update_matches {
                    return Err("DAG update differs from stored summary identity".into());
                }
                let compiled = installed
                    .native_program(node.id)?
                    .ok_or("raw materialization lacks its Planner precompute graph")?;
                let [(source, contract)] = compiled.input_contracts().collect::<Vec<_>>()[..]
                else {
                    return Err("raw precompute graph must read one raw sample input".into());
                };
                if contract.schema != precompute::raw_sample_schema()
                    || source != u64::from(edge.producer.0)
                {
                    return Err("raw precompute graph does not read the bound raw source".into());
                }
                let program = Self {
                    source,
                    program: compiled.encode().map_err(|e| e.to_string())?.into(),
                    node: node.id,
                    family: family.clone(),
                    input: input.clone(),
                    grouping: grouping.clone(),
                    reduction: reduction.clone(),
                    projected_column: config
                        .effective_value_projection()
                        .column()
                        .map(str::to_owned),
                };
                program.validate()?;
                if let Some(old) = &selected {
                    if old.family != program.family
                        || old.input != program.input
                        || old.grouping != program.grouping
                        || old.reduction != program.reduction
                    {
                        return Err(
                            "one stored definition is bound to incompatible Planner producers"
                                .into(),
                        );
                    }
                } else {
                    selected = Some(program);
                }
            }
        }
        selected.ok_or_else(|| "raw materialization has no selected post-ASAP DAG producer".into())
    }

    /// Execute the Planner graph over one pane's samples in arrival order.
    /// Returns `None` when the graph admits no population from them.
    pub fn build<'a>(
        &self,
        samples: impl IntoIterator<Item = (&'a str, i64, f64)>,
        pane: (i64, i64),
        max_bytes: usize,
    ) -> Result<Option<Box<dyn AggregateCore>>, String> {
        use futures::StreamExt;
        // Stored heap readout decodes only the backend heap kernel, not
        // Planner's weighted frequency state, so heaps stay on that kernel.
        if matches!(&self.family, SummaryFamilyType::Sketch(kind, _) if matches!(
            kind.algorithm(),
            planner_types::post_asap::SketchAlgorithm::CmsWithHeap
                | planner_types::post_asap::SketchAlgorithm::CountSketchWithHeap
        )) {
            let mut updater = self.updater()?;
            for (series, time, value) in samples {
                self.apply(&mut *updater, series, value, time)?;
            }
            return Ok(Some(updater.take_accumulator()));
        }
        let schema = precompute::raw_sample_schema();
        let rows = samples
            .into_iter()
            .map(|(series, time, value)| {
                precompute::raw_sample_row(&series_labels(series), time, value)
            })
            .collect();
        let batch = Batch::try_new(schema.clone(), rows).map_err(|e| e.to_string())?;
        let sources = std::collections::BTreeMap::from([(
            self.source,
            Box::new(Operator::source(schema, vec![batch]).map_err(|e| e.to_string())?)
                as PhysicalSource<'_>,
        )]);
        let program = CompiledPhysicalDag::decode(&self.program).map_err(|e| e.to_string())?;
        let graph = program.instantiate(sources).map_err(|e| e.to_string())?;
        let context = RunContext::new(
            Scope::Ingestion {
                window_start_ms: pane.0,
                window_end_ms: pane.1,
                revision: 0,
            },
            Limits {
                max_bytes,
                ..Limits::default()
            },
        )
        .map_err(|e| e.to_string())?;
        let rows = futures::executor::block_on(async {
            let mut stream = graph.execute(program.roots(), context)?.pop().ok_or(
                asap_physical_operators::Error::Invalid("missing output".into()),
            )?;
            let mut rows = Vec::new();
            while let Some(batch) = stream.next().await {
                rows.extend(batch?.rows().iter().cloned());
            }
            Ok::<_, asap_physical_operators::Error>(rows)
        })
        .map_err(|e| e.to_string())?;
        // The router assigns one population per group, so one pane yields
        // at most one state.
        match rows.as_slice() {
            [] => Ok(None),
            [row] => match row.as_slice() {
                [_, _, Value::Summary { state, .. }] => {
                    asap_summary_state::physical::from_physical(state.as_ref())
                        .map(Some)
                        .map_err(|e| e.to_string())
                }
                _ => Err("raw precompute output is not a population state".into()),
            },
            _ => Err("one routed group produced several populations".into()),
        }
    }

    pub fn updater(&self) -> Result<Box<dyn AccumulatorUpdater>, String> {
        create_planner_accumulator(&self.family, &self.input, &self.grouping)
    }

    fn validate(&self) -> Result<(), String> {
        match &self.input.weight {
            SummaryInputExpr::Column(ColumnRef::SampleValue) | SummaryInputExpr::Constant(_) => {}
            SummaryInputExpr::Column(
                ColumnRef::Named(name) | ColumnRef::Qualified { name, .. },
            ) if self.projected_column.as_ref() == Some(name) => {}
            _ => return Err("raw DAG weight expression is unsupported".into()),
        }
        fn item(expr: &SummaryInputExpr) -> bool {
            match expr {
                SummaryInputExpr::Column(ColumnRef::Named(_) | ColumnRef::SampleValue) => true,
                SummaryInputExpr::Tuple(items) => items.iter().all(item),
                SummaryInputExpr::EntityIdentity(
                    planner_types::post_asap::EntityIdentity::PromqlLabelSet { excluding },
                ) => excluding.is_empty(),
                _ => false,
            }
        }
        if self.input.item.as_ref().is_some_and(|e| !item(e)) {
            return Err("raw DAG item expression is unsupported".into());
        }
        self.updater().map(|_| ())
    }

    /// Validate admission with the same weight semantics used during execution,
    /// without mutating an accumulator or accepting part of a request.
    pub fn validate_sample(
        &self,
        updater: &dyn AccumulatorUpdater,
        value: f64,
    ) -> Result<f64, String> {
        let weight = match &self.input.weight {
            SummaryInputExpr::Constant(c) => *c,
            // The worker retains one previous value per series across pane rotation.
            SummaryInputExpr::Column(_) => value,
            _ => return Err("unsupported raw weight expression".into()),
        };
        let scalar_frequency = asap_types::accumulator_spec::is_unit_sample_frequency(&self.input)
            && !updater.is_keyed();
        let weight = if scalar_frequency { value } else { weight };
        updater.validate_single_input(weight)?;
        Ok(weight)
    }

    pub fn apply(
        &self,
        updater: &mut dyn AccumulatorUpdater,
        series: &str,
        value: f64,
        timestamp: i64,
    ) -> Result<(), String> {
        let weight = self.validate_sample(updater, value)?;
        if updater.is_keyed() {
            let labels = super::worker::parse_labels_from_series_key(series);
            fn eval(
                expr: &SummaryInputExpr,
                series: &str,
                value: f64,
                labels: &HashMap<&str, &str>,
            ) -> Result<Vec<String>, String> {
                Ok(match expr {
                    SummaryInputExpr::EntityIdentity(_) => vec![series.to_owned()],
                    SummaryInputExpr::Column(ColumnRef::SampleValue) => vec![value.to_string()],
                    SummaryInputExpr::Column(ColumnRef::Named(name)) => vec![labels
                        .get(name.as_str())
                        .map(|s| super::worker::decode_label_value(s).into_owned())
                        .ok_or_else(|| format!("missing DAG item column {name}"))?],
                    SummaryInputExpr::Tuple(items) => items
                        .iter()
                        .map(|i| eval(i, series, value, labels))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .flatten()
                        .collect(),
                    _ => return Err("unsupported raw item expression".into()),
                })
            }
            let item = self
                .input
                .item
                .as_ref()
                .ok_or("keyed DAG kernel requires an explicit item")?;
            let key = KeyByLabelValues::new_with_labels(eval(item, series, value, &labels)?);
            updater.update_keyed(&key, weight, timestamp);
        } else {
            updater.update_single(weight, timestamp);
        }
        Ok(())
    }
}

/// The complete label set of a canonical series key, including `__name__`.
pub(crate) fn series_labels(series: &str) -> std::collections::BTreeMap<String, String> {
    let mut labels: std::collections::BTreeMap<_, _> =
        super::worker::parse_labels_from_series_key(series)
            .into_iter()
            .map(|(k, v)| {
                (
                    k.to_owned(),
                    super::worker::decode_label_value(v).into_owned(),
                )
            })
            .collect();
    let metric = series.split('{').next().unwrap_or_default();
    if !metric.is_empty() {
        labels.insert("__name__".into(), metric.to_owned());
    }
    labels
}
