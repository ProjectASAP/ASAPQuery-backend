//! Bind deployment inputs to retained native programs and decode PromQL results.
use super::{grouping_key, miss, EngineError, Grouping, Labels, Vector};
use asap_physical_operators::dag::{
    self, batch_execution,
    operators::{Operator, SortKey},
    values::{Batch, Schema, Value},
};
use planner_types::{
    post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
    pre_asap::DataType,
};
use std::sync::Arc;
fn schema(fields: &[(&str, DataType)]) -> Schema {
    Arc::new(SummarySchema {
        fields: fields
            .iter()
            .map(|(name, dtype)| SummaryField {
                name: (*name).into(),
                dtype: SummaryFamilyType::Plain(dtype.clone()),
                nullable: false,
            })
            .collect(),
        time_index: None,
    })
}
fn key<T: serde::Serialize>(value: &T) -> Value {
    Value::Utf8(
        serde_json::to_string(value)
            .expect("string keys serialize")
            .into(),
    )
}
fn ranked_batch(values: &Vector, grouping: &Grouping) -> Result<Batch, EngineError> {
    let schema = schema(&[
        ("index", DataType::Int64),
        ("group", DataType::Utf8),
        ("value", DataType::Float64),
    ]);
    Batch::try_new(
        schema,
        values
            .iter()
            .enumerate()
            .map(|(i, (labels, v))| {
                vec![
                    Value::Int64(i as i64),
                    key(&grouping_key(labels, grouping)),
                    Value::Float64(*v),
                ]
            })
            .collect(),
    )
    .map_err(EngineError::from)
}
fn output(values: Vector, batches: Vec<dag::SharedValue<Batch>>) -> Result<Vector, EngineError> {
    batches
        .iter()
        .flat_map(|b| b.rows())
        .map(|row| match row.first() {
            Some(Value::Int64(index)) => values
                .get(*index as usize)
                .cloned()
                .ok_or_else(|| miss("native result index outside input")),
            _ => Err(miss("native result has no row identity")),
        })
        .collect()
}
pub(super) fn sort(
    values: Vector,
    grouping: &Grouping,
    descending: bool,
    context: dag::RunContext,
) -> Result<Vector, EngineError> {
    let batch = ranked_batch(&values, grouping)?;
    let op = Operator::sort(
        batch.schema().clone(),
        vec![SortKey {
            column: 2,
            descending,
            nulls_first: false,
        }],
        vec![1],
    )
    .map_err(EngineError::from)?;
    let result =
        batch_execution::evaluate_batch(batch, vec![op], context).map_err(EngineError::from)?;
    output(values, result)
}
pub(super) fn limit(
    values: Vector,
    grouping: &Grouping,
    n: u64,
    offset: u64,
    context: dag::RunContext,
) -> Result<Vector, EngineError> {
    let batch = ranked_batch(&values, grouping)?;
    let op =
        Operator::limit(batch.schema().clone(), n, offset, vec![1]).map_err(EngineError::from)?;
    let result =
        batch_execution::evaluate_batch(batch, vec![op], context).map_err(EngineError::from)?;
    output(values, result)
}
/// Relational boundary used by explicit row plans. Planner owns predicate lowering.
pub(super) fn relation(
    values: Vector,
    candidates: Vector,
    predicate: planner_types::pre_asap::Predicate,
    left: Schema,
    right: Schema,
    output_schema: Schema,
    completeness: Option<planner_types::post_asap::CandidateCompleteness>,
    at: i64,
    context: dag::RunContext,
) -> Result<Vector, EngineError> {
    use asap_physical_operators::physical_planner::{
        compile_node, CompiledPhysicalDag, InputContract,
    };
    use planner_types::post_asap::{
        ExecutableDagNode, ExecutableOperatorPayload, ExecutionDataState, PostAsapNodeId,
    };
    let pruning = completeness
        .as_ref()
        .map(|completeness| {
            Ok::<_, EngineError>(asap_types::query_plan::PruningInputContract {
                candidate_input: 1,
                keys: asap_physical_operators::physical_planner::equijoin_keys(
                    &predicate, &left, &right,
                )?,
                completeness: completeness.clone(),
            })
        })
        .transpose()?;
    let node = ExecutableDagNode {
        id: PostAsapNodeId(2),
        output_state: ExecutionDataState::QUERY_ROWS,
        output_schema: (*output_schema).clone(),
        guarantee: None,
        payload: ExecutableOperatorPayload::RelationalJoin {
            join_kind: planner_types::pre_asap::JoinKind::Semi,
            pred: predicate,
            pruning: completeness,
        },
    };
    let operator = compile_node(&node, &[left.clone(), right.clone()])?;
    let compiled = CompiledPhysicalDag::from_operators(
        [
            (0, InputContract::bounded(left)),
            (1, InputContract::bounded(right)),
        ]
        .into(),
        [(2, (vec![0, 1], operator))].into(),
        vec![2],
    )?;
    let encoded = compiled.encode()?;
    let inputs = vec![values, candidates];
    if let Some(pruning) = pruning {
        validate_pruning(&encoded, &inputs, 0, &pruning, at, context.clone())?;
    }
    physical(&encoded, inputs, 0, at, context)
}

// Equality keys canonicalize signed zero and NaNs; row transport must preserve their bits.
fn identity_key(value: &Value) -> Result<Vec<u8>, asap_physical_operators::Error> {
    if let Value::Float64(number) = value {
        let mut key = vec![0xff];
        key.extend_from_slice(&number.to_bits().to_be_bytes());
        Ok(key)
    } else {
        value.key()
    }
}

/// Bind protocol values to the selected physical input contracts; no operator lowering.
pub(super) fn physical(
    encoded: &[u8],
    inputs: Vec<Vector>,
    row_input: usize,
    at: i64,
    context: dag::RunContext,
) -> Result<Vector, EngineError> {
    use asap_physical_operators::physical_planner::{CompiledPhysicalDag, Source};
    use futures::{FutureExt, StreamExt};
    use std::collections::{BTreeMap, VecDeque};
    let compiled = CompiledPhysicalDag::decode(encoded)?;
    let contracts = compiled.input_contracts().collect::<Vec<_>>();
    if contracts.len() != inputs.len() || row_input >= inputs.len() {
        return Err(asap_physical_operators::Error::Invalid(
            "physical input arity mismatch".into(),
        )
        .into());
    }
    let mut identities: BTreeMap<Vec<Vec<u8>>, VecDeque<(Labels, f64)>> = BTreeMap::new();
    let mut sources = BTreeMap::new();
    for (position, ((id, contract), values)) in contracts.iter().zip(inputs).enumerate() {
        let rows = values
            .iter()
            .map(|(labels, sample)| {
                contract
                    .schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(column, field)| {
                        if Some(column) == contract.schema.time_index {
                            return Ok(Value::Timestamp(at));
                        }
                        match &field.dtype {
                            SummaryFamilyType::Plain(DataType::Float64) => {
                                Ok(Value::Float64(*sample))
                            }
                            SummaryFamilyType::Plain(DataType::Int64) => {
                                if sample.is_finite() && sample.fract() == 0. && sample.abs() <= (1_u64 << 53) as f64 {
                                    Ok(Value::Int64(*sample as i64))
                                } else {
                                    Err(asap_physical_operators::Error::Invalid("protocol sample cannot represent the required Int64 input exactly".into()).into())
                                }
                            }
                            SummaryFamilyType::Plain(DataType::Utf8) => {
                                Ok(labels.get(&field.name).map_or_else(
                                    || {
                                        if field.nullable {
                                            Value::Null
                                        } else {
                                            Value::Utf8("".into())
                                        }
                                    },
                                    |value| Value::Utf8(value.clone().into()),
                                ))
                            }
                            _ => Err(miss(format!(
                                "PromQL input cannot supply field {} of type {:?}",
                                field.name, field.dtype
                            ))),
                        }
                    })
                    .collect::<Result<Vec<_>, EngineError>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        if position == row_input {
            for (row, original) in rows.iter().zip(values) {
                let key = row
                    .iter()
                    .map(identity_key)
                    .collect::<Result<Vec<_>, _>>()?;
                identities.entry(key).or_default().push_back(original);
            }
        }
        let batch = Batch::try_new(contract.schema.clone(), rows)?;
        sources.insert(
            *id,
            Box::new(Operator::source(contract.schema.clone(), vec![batch])?) as Source<'_>,
        );
    }
    let graph = compiled.instantiate(sources)?;
    let mut streams = graph.execute(compiled.roots(), context)?;
    if streams.len() != 1 {
        return Err(
            asap_physical_operators::Error::Invalid("expected one physical output".into()).into(),
        );
    }
    let mut stream = streams.remove(0);
    let mut result = Vec::new();
    loop {
        match stream.next().now_or_never() {
            Some(Some(batch)) => {
                for row in batch?.rows() {
                    let key = row
                        .iter()
                        .map(identity_key)
                        .collect::<Result<Vec<_>, _>>()?;
                    let original = identities
                        .get_mut(&key)
                        .and_then(VecDeque::pop_front)
                        .ok_or_else(|| {
                            asap_physical_operators::Error::Invalid(
                                "physical row has no protocol identity".into(),
                            )
                        })?;
                    result.push(original);
                }
            }
            Some(None) => return Ok(result),
            None => continue,
        }
    }
}

pub(super) fn validate_pruning(
    encoded: &[u8],
    inputs: &[Vector],
    row_input: usize,
    contract: &asap_types::query_plan::PruningInputContract,
    at: i64,
    context: dag::RunContext,
) -> Result<(), EngineError> {
    use asap_physical_operators::physical_planner::{CompiledPhysicalDag, InputContract};
    use planner_types::post_asap::CandidateCompleteness;
    if !matches!(
        contract.completeness,
        CandidateCompleteness::Certified { .. }
    ) {
        return Ok(());
    }
    let compiled = CompiledPhysicalDag::decode(encoded)?;
    let schemas = compiled
        .input_contracts()
        .map(|(_, c)| c.schema.clone())
        .collect::<Vec<_>>();
    let candidates = inputs
        .get(contract.candidate_input)
        .ok_or_else(|| miss("missing candidate input"))?;
    let values = inputs
        .get(row_input)
        .ok_or_else(|| miss("missing authoritative input"))?;
    let coverage = Operator::semi_join(
        schemas[contract.candidate_input].clone(),
        schemas[row_input].clone(),
        contract.keys.iter().map(|&(l, r)| (r, l)).collect(),
    )?;
    let check = CompiledPhysicalDag::from_operators(
        [
            (
                0,
                InputContract::bounded(schemas[contract.candidate_input].clone()),
            ),
            (1, InputContract::bounded(schemas[row_input].clone())),
        ]
        .into(),
        [(2, (vec![0, 1], coverage))].into(),
        vec![2],
    )?;
    let matched = physical(
        &check.encode()?,
        vec![candidates.clone(), values.clone()],
        0,
        at,
        context,
    )?;
    if matched.len() != candidates.len() {
        return Err(miss("certified pruning key has no authoritative value"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_physical_operators::{
        physical_planner::{CompiledPhysicalDag, InputContract},
        Error,
    };

    // An available label is bound by Backend; Planner evaluates its predicate.
    #[test]
    fn planner_filter_compiles_and_executes_bound_labels() {
        use asap_types::query_plan::{FallbackPolicy, InstantExecution, QueryPlanNode};
        use planner_types::{
            post_asap::{
                execution_data_state::lift_plain, ExecutionTiming, SummaryExpr, SummaryNode,
                ValueOperation,
            },
            pre_asap::QueryExpr,
        };
        let query = "m{instance=\"pod\"}";
        let logical = control_plane::query_parser::parse_query_expr_canonical(
            query,
            planner_types::types::AccuracyTarget::Exact,
        )
        .unwrap();
        let QueryExpr::TimeRange { child, .. } = logical else {
            panic!("expected instant source")
        };
        let QueryExpr::Scan {
            source,
            predicates,
            schema,
        } = child.as_ref()
        else {
            panic!("expected source selector")
        };
        let pred = predicates[0].clone();
        let child = std::rc::Rc::new(QueryExpr::Scan {
            source: source.clone(),
            predicates: vec![],
            schema: schema.clone(),
        });
        let input = std::rc::Rc::new(SummaryNode {
            schema: lift_plain(&child.output_schema().unwrap()),
            guarantee: None,
            expr: SummaryExpr::KeepPreAsap(child),
        });
        let root = std::rc::Rc::new(SummaryNode {
            schema: input.schema.clone(),
            guarantee: None,
            expr: SummaryExpr::ValueOperation {
                child: input,
                operation: ValueOperation::Filter { pred },
                timing: ExecutionTiming::QueryTime,
            },
        });
        let entry = control_plane::query_plan::compile_bound_mapped(
            "filter".into(),
            query.into(),
            &root,
            InstantExecution {
                lookback_ms: 0,
                full_history: false,
                cumulative_readout: false,
            },
            FallbackPolicy::Reject,
            |_, _| panic!("filter requires no materialization"),
            |_, _| {},
        )
        .unwrap();
        let QueryPlanNode::PhysicalFragment { dag, row_input, .. } = &entry.nodes[&entry.root]
        else {
            panic!("filter was rejected: {:?}", entry.nodes)
        };
        let values = vec![
            (
                [
                    ("instance".into(), "pod".into()),
                    ("extra".into(), "kept".into()),
                ]
                .into(),
                7.,
            ),
            ([("instance".into(), "other".into())].into(), 9.),
        ];
        let expected = values[0].clone();
        assert_eq!(
            physical(dag, vec![values], *row_input, 42, context(1 << 20)).unwrap(),
            vec![expected]
        );
    }

    fn sorted() -> Vec<u8> {
        let input = schema(&[("value", DataType::Float64)]);
        CompiledPhysicalDag::from_operators(
            [(0, InputContract::bounded(input.clone()))].into(),
            [(
                1,
                (
                    vec![0],
                    Operator::sort(
                        input,
                        vec![SortKey {
                            column: 0,
                            descending: false,
                            nulls_first: false,
                        }],
                        vec![],
                    )
                    .unwrap(),
                ),
            )]
            .into(),
            vec![1],
        )
        .unwrap()
        .encode()
        .unwrap()
    }
    fn context(max_bytes: usize) -> dag::RunContext {
        dag::RunContext::new(
            dag::Scope::Query {
                evaluation_time_ms: 42,
                revision: 7,
            },
            dag::Limits {
                max_bytes,
                ..dag::Limits::default()
            },
        )
        .unwrap()
    }
    fn cause(error: &Error) -> &Error {
        match error {
            Error::AtNode { source, .. } => cause(source),
            other => other,
        }
    }

    // A real native execution failure must remain typed through the protocol adapter.
    #[test]
    fn physical_budget_and_cancellation_are_terminal() {
        for cancelled in [false, true] {
            let run = context(if cancelled { 4096 } else { 1 });
            if cancelled {
                run.cancel();
            }
            let error = physical(
                &sorted(),
                vec![vec![(Labels::new(), 2.), (Labels::new(), 1.)]],
                0,
                42,
                run.clone(),
            )
            .unwrap_err();
            let EngineError::Physical(error) = error else {
                panic!("physical failure lost its type")
            };
            assert!(
                if cancelled {
                    matches!(cause(&error), Error::Cancelled)
                } else {
                    matches!(cause(&error), Error::MemoryLimit)
                },
                "{error}"
            );
            assert_eq!(run.retained_bytes(), 0);
        }
    }

    // The complete selected-candidate adapter must not classify a byte budget as capability.
    #[test]
    fn selected_candidate_input_budget_is_a_terminal_error() {
        let plan = CompiledPhysicalDag::decode(&sorted()).unwrap();
        let error = execute_batches(&plan, 1, 1, 42, |_, schema| {
            Ok(Batch::try_new(
                schema.clone(),
                vec![vec![Value::Float64(1.)]],
            )?)
        })
        .err()
        .expect("input must exceed the byte budget");
        assert!(
            matches!(error, EngineError::Physical(Error::MemoryLimit)),
            "{error}"
        );
    }

    // Count-like values are bound as integers only when the protocol sample is exact.
    #[test]
    fn integer_input_binding_preserves_type_and_rejects_rounding() {
        let input = schema(&[("count", DataType::Int64)]);
        let compiled = CompiledPhysicalDag::from_operators(
            [(0, InputContract::bounded(input.clone()))].into(),
            [(1, (vec![0], Operator::limit(input, 1, 0, vec![]).unwrap()))].into(),
            vec![1],
        )
        .unwrap()
        .encode()
        .unwrap();
        for (sample, valid) in [
            (3., true),
            (-4., true),
            (0.5, false),
            (f64::INFINITY, false),
            (9_007_199_254_740_994., false),
        ] {
            let result = physical(
                &compiled,
                vec![vec![(Labels::new(), sample)]],
                0,
                42,
                context(4096),
            );
            if valid {
                assert_eq!(result.unwrap()[0].1, sample);
            } else {
                assert!(matches!(
                    result,
                    Err(EngineError::Physical(Error::Invalid(_)))
                ));
            }
        }
    }

    // Transport identity preserves the exact value selected by native total-order sorting.
    #[test]
    fn native_sort_preserves_signed_zero_bits() {
        let rows = physical(
            &sorted(),
            vec![vec![(Labels::new(), 0.), (Labels::new(), -0.)]],
            0,
            42,
            context(4096),
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.1.to_bits()).collect::<Vec<_>>(),
            vec![0.0_f64.to_bits(), (-0.0_f64).to_bits()]
        );
    }
}

#[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
    fields(stage = "physical.bind_vectors", query_id = %entry.query_id, evaluation_time_ms = at,
        input_kind = "bound_promql_vector", bound_counter_state = entry.physical_vector_binding().is_some()), err)]
pub(super) fn execute_vectors<F>(
    entry: &asap_types::query_plan::QueryPlanEntry,
    at: u64,
    mut callback: F,
) -> Result<
    (
        crate::query_engines::query_result::QueryResult,
        super::ExecutionStats,
    ),
    EngineError,
>
where
    F: FnMut(
        asap_types::query_plan::QueryNodeId,
        u64,
    ) -> Result<crate::query_engines::query_result::QueryResult, EngineError>,
{
    use asap_physical_operators::physical_planner::promql_rows::series_row;
    use std::collections::BTreeMap;
    let (program, bindings, max_bytes) =
        if let Some((inputs, source_nodes, budget)) = entry.physical_vector_binding() {
            (
                entry
                    .recover_vector_physical_dag()
                    .map_err(|e| miss(e.to_string()))?,
                source_nodes
                    .iter()
                    .copied()
                    .zip(inputs.iter().copied())
                    .collect::<BTreeMap<_, _>>(),
                budget,
            )
        } else {
            let program = entry
                .recover_population_physical_dag()
                .map_err(|e| miss(e.to_string()))?;
            let source = program.input_contracts().next().unwrap().0;
            (
                program,
                BTreeMap::from([(source, entry.root)]),
                entry.population_snapshot().unwrap().max_bytes,
            )
        };
    execute_batches(
        &program,
        max_bytes,
        bindings.len(),
        at,
        |input_id, schema| {
            let values = super::vector(super::from_result(callback(bindings[&input_id], at)?)?)?;
            let rows = values
                .into_iter()
                .map(|(labels, value)| {
                    series_row(
                        schema,
                        &labels,
                        i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?,
                        value,
                    )
                    .map_err(|e| miss(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Batch::try_new(schema.clone(), rows).map_err(EngineError::from)
        },
    )
}

#[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
    fields(stage = "physical.bind_stored_summary", query_id = %entry.query_id, evaluation_time_ms = at, plan_id, plan_version), err)]
pub(in crate::query_engines::asap_query_engine) fn execute_stored(
    entry: &asap_types::query_plan::QueryPlanEntry,
    plan_id: u64,
    plan_version: u64,
    store: &crate::storage_engines::sketch_db::index::SketchStore,
    at: u64,
) -> Result<
    (
        crate::query_engines::query_result::QueryResult,
        super::ExecutionStats,
    ),
    EngineError,
> {
    let (inputs, sources, max_bytes) = entry
        .physical_vector_binding()
        .ok_or_else(|| miss("missing native stored binding"))?;
    let program = entry
        .recover_vector_physical_dag()
        .map_err(|e| miss(e.to_string()))?;
    execute_batches(&program, max_bytes, inputs.len(), at, |id, schema| {
        let index = sources
            .iter()
            .position(|source| *source == id)
            .ok_or_else(|| miss("native source is unbound"))?;
        let Some(asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding }) =
            entry.nodes.get(&inputs[index])
        else {
            return Err(miss("native stored source has no deployed summary binding"));
        };
        let end = i64::try_from(at).map_err(|_| miss("native timestamp overflow"))?;
        let start = at
            .checked_sub(binding.window_ms)
            .ok_or_else(|| miss("native window underflow"))?;
        let address = asap_types::sds::StoredSummaryKey {
            plan_id,
            plan_version,
            stored_output_id: binding.stored_output_reference.stored_output_id,
            population: std::collections::BTreeMap::new(),
            window: asap_types::sds::HalfOpenTimeRange {
                start_ms: start as i64,
                end_ms: end,
            },
        };
        store
            .read_bound_native_summary(
                &address,
                &binding.stored_output_reference,
                schema.clone(),
                max_bytes as usize,
            )
            .map_err(miss)
    })
}

#[tracing::instrument(level = "debug", target = "asap_runtime_debug", skip_all,
    fields(stage = "physical.execute", input_count, evaluation_time_ms = at, max_bytes), err)]
fn execute_batches(
    program: &asap_physical_operators::physical_planner::CompiledPhysicalDag,
    max_bytes: u64,
    input_count: usize,
    at: u64,
    mut input_batch: impl FnMut(u64, &Schema) -> Result<Batch, EngineError>,
) -> Result<
    (
        crate::query_engines::query_result::QueryResult,
        super::ExecutionStats,
    ),
    EngineError,
> {
    use crate::{
        query_engines::query_result::{InstantVectorElement, QueryResult},
        storage_engines::types::KeyByLabelValues,
    };
    use asap_physical_operators::physical_planner::{
        promql_rows::{decode_series_identity, SERIES_IDENTITY_COLUMN},
        Source,
    };
    use futures::{executor::block_on, StreamExt};
    use std::collections::BTreeMap;
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let mut sources = BTreeMap::new();
    let mut input_bytes = 0usize;
    for (input_id, input) in program.input_contracts() {
        let batch = input_batch(input_id, &input.schema)?;
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or(asap_physical_operators::Error::MemoryLimit)?;
        if input_bytes > max_bytes as usize {
            return Err(asap_physical_operators::Error::MemoryLimit.into());
        }
        let source =
            Operator::source(input.schema.clone(), vec![batch]).map_err(EngineError::from)?;
        sources.insert(input_id, Box::new(source) as Source<'_>);
    }
    let graph = {
        let _binding = tracing::debug_span!(target: "asap_runtime_debug", "physical_input_binding",
            stage = "physical.bind_inputs", input_count = sources.len(), input_bytes, input_kind = "native_batch").entered();
        program
            .instantiate(sources)
            .map_err(EngineError::from)?
    };
    let context = dag::RunContext::new(
        dag::Scope::Query {
            evaluation_time_ms: at_signed,
            revision: 0,
        },
        dag::Limits {
            max_bytes: max_bytes as usize,
            ..dag::Limits::default()
        },
    )
    .map_err(EngineError::from)?;
    let mut stream = graph
        .execute(program.roots(), context)
        .map_err(EngineError::from)?
        .remove(0);
    let values = block_on(async {
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(EngineError::from)?;
            let identity = batch
                .schema()
                .fields
                .iter()
                .position(|field| field.name == SERIES_IDENTITY_COLUMN);
            let value = batch
                .schema()
                .fields
                .iter()
                .position(|field| field.dtype == SummaryFamilyType::Plain(DataType::Float64))
                .ok_or_else(|| miss("physical output loses sample value"))?;
            for row in batch.rows() {
                let Value::Float64(sample) = &row[value] else {
                    return Err(miss("invalid physical result value"));
                };
                let labels = if let Some(identity) = identity {
                    let Value::Utf8(encoded) = &row[identity] else {
                        return Err(miss("invalid physical series identity"));
                    };
                    decode_series_identity(encoded).map_err(EngineError::from)?
                } else {
                    batch
                        .schema()
                        .fields
                        .iter()
                        .zip(row)
                        .filter_map(|(field, value)| match value {
                            Value::Utf8(label) if !label.is_empty() => {
                                Some((field.name.clone(), label.to_string()))
                            }
                            _ => None,
                        })
                        .collect()
                };
                values.push(
                    InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                        *sample,
                    )
                    .with_label_keys_override(labels.into_keys().collect()),
                );
            }
        }
        Ok(values)
    })?;
    Ok((
        QueryResult::vector(values, at),
        super::ExecutionStats {
            summary_readout_evaluations: input_count,
            ..Default::default()
        },
    ))
}
