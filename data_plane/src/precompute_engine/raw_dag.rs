//! Bind raw ingestion to a selected Planner producer and its raw dependency edge.
//! The backend supplies one typed sample batch per pane; the Planner-compiled
//! precompute graph owns every update, grouping and item computation.
use crate::storage_engines::types::AggregateCore;
use asap_physical_operators::factory::create_planner_accumulator;
use asap_physical_operators::{
    operators::Operator,
    physical_planner::{precompute, CompiledPhysicalDag, Source as PhysicalSource},
    runtime::{Limits, RunContext, Scope},
    values::{Batch, Value},
};
use asap_types::physical_plan_codec::PhysicalPlanCodec;
use asap_types::{executable_plan::BackendNodeBinding, PrecomputeMaterialization};
use planner_types::post_asap::{
    EdgeRole, GroupingStrategy, PostAsapNodeId, PostAsapOperatorPayload, SummaryFamilyType,
    SummaryInputExpr, SummaryUpdate,
};
use planner_types::pre_asap::{ColumnRef, QueryExpr, Source};
use std::collections::HashMap;

/// Planner execution errors keep their type (e.g. `MemoryLimit`); setup and
/// binding failures are messages.
pub type BuildError = Box<dyn std::error::Error + Send + Sync>;

/// A validated executable projection; semantics come from the installed node.
/// The retained node ID makes failures attributable to the selected DAG.
#[derive(Debug, Clone)]
pub struct RawDagProgram {
    pub node: PostAsapNodeId,
    pub family: SummaryFamilyType,
    pub input: SummaryUpdate,
    pub grouping: GroupingStrategy,
    pub reduction: planner_types::pre_asap::Reduction,
    /// Planner's encoded precompute graph from the raw sample boundary to this
    /// output; decoded graphs are not `Send` (see `decoded`).
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
                };
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

    /// Execute the Planner graph over one pane's samples as one typed batch.
    /// Returns `None` when the graph admits no population from them.
    pub fn build<'a>(
        &self,
        samples: impl IntoIterator<Item = (&'a str, i64, f64)>,
        pane: (i64, i64),
        max_bytes: usize,
    ) -> Result<Option<Box<dyn AggregateCore>>, BuildError> {
        let samples = pane_batch(samples);
        // The router assigns one population per group, so one pane yields
        // at most one state.
        match self.execute(samples, pane, max_bytes)?.as_slice() {
            [] => Ok(None),
            [row] => match row.as_slice() {
                [_, _, Value::Summary { state, .. }] => {
                    asap_summary_state::stored_state::codec::check_storable(state.as_ref())?;
                    Ok(Some(state.clone_boxed_core()))
                }
                _ => Err("raw precompute output is not a population state".into()),
            },
            _ => Err("one routed group produced several populations".into()),
        }
    }

    /// Check that the Planner graph admits every sample, without storing a result.
    pub fn validate<'a>(
        &self,
        samples: impl IntoIterator<Item = (&'a str, i64, f64)>,
        max_bytes: usize,
    ) -> Result<(), BuildError> {
        let samples = pane_batch(samples);
        if samples.is_empty() {
            return Ok(());
        }
        let bounds = samples
            .iter()
            .fold((i64::MAX, i64::MIN), |(lo, hi), (_, time, _)| {
                (lo.min(*time), hi.max(time.saturating_add(1)))
            });
        self.execute(samples, bounds, max_bytes).map(|_| ())
    }

    /// The family's empty state, for a pane known to have no samples. Heaps
    /// are Planner weighted-frequency states, as `build` produces.
    pub fn empty_state(&self) -> Result<Box<dyn AggregateCore>, String> {
        use asap_physical_operators::summary_kernels::weighted_frequency::{
            FrequencyAlgorithm, WeightedFrequency,
        };
        use planner_types::post_asap::SketchParams;
        if let SummaryFamilyType::Sketch(kind, _) = &self.family {
            let heap = match kind.params() {
                SketchParams::CmsWithHeap {
                    width,
                    depth,
                    heap_size,
                } => Some((FrequencyAlgorithm::Cms, width, depth, heap_size)),
                SketchParams::CountSketchWithHeap {
                    width,
                    depth,
                    heap_size,
                } => Some((FrequencyAlgorithm::CountSketch, width, depth, heap_size)),
                _ => None,
            };
            if let Some((algorithm, width, depth, heap_size)) = heap {
                let state = WeightedFrequency::new(
                    algorithm,
                    *width as usize,
                    *depth as usize,
                    *heap_size as usize,
                )
                .map_err(|e| e.to_string())?;
                return Ok(Box::new(state));
            }
            // Planner's UnivMon has no stored codec; the store keeps the
            // backend UnivMon shim.
            if let SketchParams::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } = kind.params()
            {
                return asap_summary_state::univmon::UnivMonAccumulator::new(
                    *heap_size as usize,
                    *sketch_rows as usize,
                    *sketch_cols as usize,
                    *layers as usize,
                )
                .map(|state| Box::new(state) as Box<dyn AggregateCore>)
                .map_err(|e| e.to_string());
            }
        }
        Ok(
            create_planner_accumulator(&self.family, &self.input, &self.grouping)?
                .take_accumulator(),
        )
    }

    fn execute<'a>(
        &self,
        samples: impl IntoIterator<Item = (&'a str, i64, f64)>,
        pane: (i64, i64),
        max_bytes: usize,
    ) -> Result<Vec<Vec<Value>>, BuildError> {
        use futures::StreamExt;
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
        let program = decoded(&self.program)?;
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
        Ok(futures::executor::block_on(async {
            let mut stream = graph.execute(program.roots(), context)?.pop().ok_or(
                asap_physical_operators::Error::Invalid("missing output".into()),
            )?;
            let mut rows = Vec::new();
            while let Some(batch) = stream.next().await {
                rows.extend(batch?.rows().iter().cloned());
            }
            Ok::<_, asap_physical_operators::Error>(rows)
        })?)
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

/// A pane's samples as ingestion delivers them to Planner: in timestamp
/// order (stable for equal times), with one sample per series and timestamp;
/// a repeated `(series, timestamp)` is the same sample, so the first is kept.
fn pane_batch<'a>(
    samples: impl IntoIterator<Item = (&'a str, i64, f64)>,
) -> Vec<(&'a str, i64, f64)> {
    let mut samples = samples.into_iter().collect::<Vec<_>>();
    samples.sort_by_key(|(_, time, _)| *time);
    let mut seen = std::collections::HashSet::new();
    samples.retain(|(series, time, _)| seen.insert((*series, *time)));
    samples
}

/// Decoded graphs are not `Send`, so each thread decodes an installed graph
/// once. Holding the encoded bytes keeps their address from being reused.
fn decoded(encoded: &std::sync::Arc<[u8]>) -> Result<std::rc::Rc<CompiledPhysicalDag>, String> {
    type Cache =
        std::collections::HashMap<usize, (std::sync::Arc<[u8]>, std::rc::Rc<CompiledPhysicalDag>)>;
    thread_local! {
        static DECODED: std::cell::RefCell<Cache> = Default::default();
    }
    DECODED.with(|cache| {
        let mut cache = cache.borrow_mut();
        let key = encoded.as_ptr() as usize;
        if let Some((_, program)) = cache.get(&key) {
            return Ok(program.clone());
        }
        if cache.len() >= 1024 {
            cache.clear();
        }
        let program =
            std::rc::Rc::new(CompiledPhysicalDag::decode(encoded).map_err(|e| e.to_string())?);
        cache.insert(key, (encoded.clone(), program.clone()));
        Ok(program)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A heap output's empty state is the Planner heap state that `build`
    // produces, so it binds as a physical input and stores as a heap frame.
    #[test]
    fn heap_empty_state_is_a_planner_heap_frame() {
        use planner_types::post_asap::{SketchAlgorithm, SketchKind, SketchParams};
        let family = SummaryFamilyType::Sketch(
            SketchKind::new(
                SketchAlgorithm::CmsWithHeap,
                SketchParams::CmsWithHeap {
                    width: 64,
                    depth: 3,
                    heap_size: 8,
                },
            ),
            GroupingStrategy::PerSubpopulationInstance,
        );
        let program = RawDagProgram {
            node: PostAsapNodeId(1),
            family,
            input: SummaryUpdate::column(ColumnRef::SampleValue),
            grouping: GroupingStrategy::PerSubpopulationInstance,
            reduction: planner_types::pre_asap::Reduction::PerEntity,
            program: std::sync::Arc::from(Vec::new()),
            source: 0,
        };
        let empty = program.empty_state().unwrap();
        assert!(asap_summary_state::stored_state::codec::check_storable(empty.as_ref()).is_ok());
        assert_eq!(
            asap_summary_state::stored_state::SketchEncoding::full_frame_for(empty.as_ref()),
            asap_summary_state::stored_state::SketchEncoding::WeightedFrequencyV1
        );
    }
}
