//! Bind deployment inputs to retained native programs and decode PromQL results.
use super::{miss, EngineError, Labels, Vector};
use asap_physical_operators::dag::{
    self,
    operators::Operator,
    values::{Batch, Schema, Value},
};
use asap_types::physical_plan_codec::PhysicalPlanCodec;
#[cfg(test)]
use planner_types::post_asap::{SummaryField, SummarySchema};
use planner_types::{post_asap::SummaryFamilyType, pre_asap::DataType};
#[cfg(test)]
use std::sync::Arc;

/// Bind protocol values without choosing matching, grouping or arithmetic behavior.
pub(in crate::query_engines::asap_query_engine) fn complete_values(
    encoded: &[u8],
    inputs: &[&super::Value],
    context: dag::RunContext,
) -> Result<Option<super::Value>, EngineError> {
    use asap_physical_operators::physical_planner::{promql_values, CompiledPhysicalDag, Source};
    use futures::StreamExt;
    let program = CompiledPhysicalDag::decode(encoded)?;
    let scalar = promql_values::scalar_schema();
    let vector = promql_values::vector_schema();
    let matrix = promql_values::matrix_schema();
    let compatible = |schema: &Schema| schema == &scalar || schema == &vector || schema == &matrix;
    if program.roots().len() != 1
        || !program
            .input_contracts()
            .all(|(_, input)| compatible(&input.schema))
        || !compatible(&program.output_contract(program.roots()[0])?.schema)
    {
        return Ok(None);
    }
    if program.input_contracts().count() != inputs.len() {
        return Err(miss("physical value input arity mismatch"));
    }
    let mut sources = std::collections::BTreeMap::new();
    let mut retained_inputs = context.reserve(0)?;
    let mut input_bytes = 0usize;
    for ((id, contract), input) in program.input_contracts().zip(inputs) {
        let rows = match input {
            super::Value::Scalar(value) if contract.schema == scalar => {
                vec![vec![Value::Float64(*value)]]
            }
            super::Value::Vector(values) if contract.schema == vector => values
                .iter()
                .map(|(labels, value)| vec![super::native_labels(labels), Value::Float64(*value)])
                .collect(),
            super::Value::Matrix(values, start, end) if contract.schema == matrix => values
                .iter()
                .flat_map(|(labels, points)| {
                    points.iter().map(move |(time, value)| {
                        vec![
                            super::native_labels(labels),
                            Value::Timestamp(*time),
                            Value::Float64(*value),
                            Value::Timestamp(*start),
                            Value::Timestamp(*end),
                        ]
                    })
                })
                .collect(),
            _ => {
                return Err(miss(
                    "protocol input differs from compiled scalar/vector contract",
                ))
            }
        };
        let batch = Batch::try_new(contract.schema.clone(), rows)?;
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or(dag::Error::MemoryLimit)?;
        retained_inputs.resize(input_bytes)?;
        sources.insert(
            id,
            Box::new(Operator::source(contract.schema.clone(), vec![batch])?) as Source<'_>,
        );
    }
    let output_schema = program.output_contract(program.roots()[0])?.schema;
    let graph = program.instantiate(sources)?;
    let mut retained = context.reserve(0)?;
    let mut stream = graph.execute(program.roots(), context)?.remove(0);
    let rows = crate::query_engines::request::drive(async {
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            bytes = bytes
                .checked_add(batch.bytes())
                .ok_or(dag::Error::MemoryLimit)?;
            retained.resize(bytes)?;
            rows.extend(batch.rows().iter().cloned());
        }
        Ok::<_, EngineError>(rows)
    })??;
    if output_schema == scalar {
        let [row] = rows.as_slice() else {
            return Err(miss("physical scalar output must have exactly one row"));
        };
        let [Value::Float64(value)] = row.as_slice() else {
            return Err(miss("invalid scalar output"));
        };
        Ok(Some(super::Value::Scalar(*value)))
    } else {
        super::native_vector_output(rows, 0, 1).map(|values| Some(super::Value::Vector(values)))
    }
}

#[cfg(test)]
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
#[cfg(test)]
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
        ExecutionDataState, PostAsapDagNode, PostAsapNodeId, PostAsapOperatorPayload,
    };
    let node = PostAsapDagNode {
        id: PostAsapNodeId(2),
        output_state: ExecutionDataState::QUERY_ROWS,
        output_schema: (*output_schema).clone(),
        guarantee: None,
        payload: PostAsapOperatorPayload::RelationalJoin {
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
    physical(&encoded, inputs, Some(0), at, context)
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
    row_input: Option<usize>,
    at: i64,
    context: dag::RunContext,
) -> Result<Vector, EngineError> {
    use asap_physical_operators::physical_planner::{
        promql_rows::{decode_series_identity, encode_series_identity, SERIES_IDENTITY_COLUMN},
        CompiledPhysicalDag, Source,
    };
    use futures::{FutureExt, StreamExt};
    use std::collections::{BTreeMap, VecDeque};
    let compiled = CompiledPhysicalDag::decode(encoded)?;
    let contracts = compiled.input_contracts().collect::<Vec<_>>();
    if contracts.len() != inputs.len() || row_input.is_some_and(|index| index >= inputs.len()) {
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
                            // Planner matches per-series rows by their
                            // complete label set.
                            SummaryFamilyType::Plain(DataType::Utf8)
                                if field.name == SERIES_IDENTITY_COLUMN =>
                            {
                                Ok(Value::Utf8(encode_series_identity(labels)?.into()))
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
        if Some(position) == row_input {
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
    let output_schema = compiled.output_contract(compiled.roots()[0])?.schema;
    // A per-series result names its series by identity; its other label
    // columns are projections of that identity.
    let output_identity = output_schema
        .fields
        .iter()
        .position(|field| field.name == SERIES_IDENTITY_COLUMN);
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
        crate::query_engines::request::check()?;
        match stream.next().now_or_never() {
            Some(Some(batch)) => {
                for row in batch?.rows() {
                    if let Some(identity) = output_identity {
                        let Value::Utf8(encoded) = &row[identity] else {
                            return Err(miss("invalid physical series identity"));
                        };
                        let sample = output_schema
                            .fields
                            .iter()
                            .zip(row)
                            .find_map(|(field, cell)| match cell {
                                Value::Float64(value)
                                    if field.dtype
                                        == SummaryFamilyType::Plain(DataType::Float64) =>
                                {
                                    Some(*value)
                                }
                                _ => None,
                            })
                            .ok_or_else(|| miss("native vector output has no numeric value"))?;
                        result.push((decode_series_identity(encoded)?, sample));
                        continue;
                    }
                    if row_input.is_none() {
                        let mut labels = Labels::new();
                        let mut sample = None;
                        for (field, cell) in output_schema.fields.iter().zip(row) {
                            match cell {
                                Value::Utf8(value) => {
                                    if !value.is_empty() {
                                        labels.insert(field.name.clone(), value.to_string());
                                    }
                                }
                                Value::Float64(value) => sample = Some(*value),
                                Value::Int64(value) if value.unsigned_abs() <= (1u64 << 53) => {
                                    sample = Some(*value as f64)
                                }
                                Value::Timestamp(_) | Value::Null => {}
                                _ => return Err(miss(
                                    "native output cannot be represented by the PromQL protocol",
                                )),
                            }
                        }
                        result.push((
                            labels,
                            sample
                                .ok_or_else(|| miss("native vector output has no numeric value"))?,
                        ));
                        continue;
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use asap_physical_operators::operators::SortKey;
    use asap_physical_operators::{
        physical_planner::{CompiledPhysicalDag, InputContract},
        Error,
    };

    // An available label is bound by Backend; Planner evaluates its predicate.
    #[test]
    fn planner_filter_compiles_and_executes_bound_labels() {
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
        // The pre-ASAP selector is the fragment's bound input, as an exact
        // engine result would be.
        let dag = planner_types::post_asap::compile_post_asap_dag(&root).unwrap();
        let source = dag.nodes.iter().find(|node| node.id != dag.root).unwrap();
        let program = asap_physical_operators::physical_planner::compile(
            &dag,
            [(
                u64::from(source.id.0),
                InputContract::bounded(std::sync::Arc::new(source.output_schema.clone())),
            )]
            .into(),
            &[u64::from(dag.root.0)],
        )
        .unwrap();
        let row_input = program
            .row_source(program.roots()[0])
            .and_then(|id| program.input_contracts().position(|(input, _)| input == id));
        let dag = program.encode().unwrap();
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
            physical(&dag, vec![values], row_input, 42, context(1 << 20)).unwrap(),
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
                Some(0),
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
        let error = execute_batches(&plan, 1, 1, 42, false, |_, contract| {
            Ok(BoundInput::Rows(Batch::try_new(
                contract.schema.clone(),
                vec![vec![Value::Float64(1.)]],
            )?))
        })
        .expect_err("input must exceed the byte budget");
        assert!(
            matches!(error, EngineError::Physical(Error::MemoryLimit)),
            "{error}"
        );
    }

    // A population count is an Int64 sample only when it is the sole numeric
    // column; a Float64 value always wins over an Int64 helper column.
    #[test]
    fn selected_program_reads_counts_and_prefers_float_values() {
        for (fields, row, expected) in [
            (
                vec![("job", DataType::Utf8), ("count", DataType::Int64)],
                vec![Value::Utf8("api".into()), Value::Int64(3)],
                3.,
            ),
            (
                vec![("members", DataType::Int64), ("value", DataType::Float64)],
                vec![Value::Int64(7), Value::Float64(2.5)],
                2.5,
            ),
        ] {
            let input = schema(&fields);
            let plan = CompiledPhysicalDag::from_operators(
                [(0, InputContract::bounded(input.clone()))].into(),
                Default::default(),
                vec![0],
            )
            .unwrap();
            let (result, _) = execute_batches(&plan, 1 << 20, 1, 42, false, |_, contract| {
                Ok(BoundInput::Rows(Batch::try_new(
                    contract.schema.clone(),
                    vec![row.clone()],
                )?))
            })
            .unwrap();
            let crate::query_engines::query_result::QueryResult::Vector(result) = result else {
                panic!("vector expected")
            };
            assert_eq!(result.values[0].value, expected);
        }
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
                Some(0),
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

    // Aggregation changes both rows and labels; decoding must use the declared
    // output schema instead of looking up an unchanged input row.
    #[test]
    fn physical_aggregate_returns_grouped_values_and_labels() {
        use asap_physical_operators::{
            operators::Reduction,
            physical_planner::{CompiledPhysicalDag, InputContract},
        };
        let input = schema(&[("job", DataType::Utf8), ("value", DataType::Float64)]);
        let aggregate = Operator::aggregate(
            input.clone(),
            vec![0],
            vec![("value".into(), Reduction::Sum(1))],
        )
        .unwrap();
        let program = CompiledPhysicalDag::from_operators(
            [(0, InputContract::bounded(input))].into(),
            [(1, (vec![0], aggregate))].into(),
            vec![1],
        )
        .unwrap();
        let values = vec![
            (Labels::from([("instance".into(), "c".into())]), 5.),
            (
                Labels::from([
                    ("job".into(), "api".into()),
                    ("instance".into(), "a".into()),
                ]),
                1.,
            ),
            (
                Labels::from([
                    ("job".into(), "api".into()),
                    ("instance".into(), "b".into()),
                ]),
                2.,
            ),
            (
                Labels::from([
                    ("job".into(), "worker".into()),
                    ("instance".into(), "a".into()),
                ]),
                4.,
            ),
        ];
        let mut output = physical(
            &program.encode().unwrap(),
            vec![values],
            None,
            42,
            context(1 << 20),
        )
        .unwrap();
        output.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            output,
            vec![
                (Labels::new(), 5.),
                (Labels::from([("job".into(), "api".into())]), 3.),
                (Labels::from([("job".into(), "worker".into())]), 4.),
            ]
        );
    }

    // Transport identity preserves the exact value selected by native total-order sorting.
    #[test]
    fn native_sort_preserves_signed_zero_bits() {
        let rows = physical(
            &sorted(),
            vec![vec![(Labels::new(), 0.), (Labels::new(), -0.)]],
            Some(0),
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
        entry.drops_metric_name(),
        |input_id, contract| {
            let schema = &contract.schema;
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
            Batch::try_new(schema.clone(), rows)
                .map(BoundInput::Rows)
                .map_err(EngineError::from)
        },
    )
}

pub(in crate::query_engines::asap_query_engine) fn execute_stored(
    entry: &asap_types::query_plan::QueryPlanEntry,
    plan_id: u64,
    plan_version: u64,
    store: Option<&crate::storage_engines::sketch_db::index::SketchStore>,
    raw_endpoint: Option<(&reqwest::Client, &str)>,
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
    let drop_metric_name = entry.drops_metric_name();
    execute_batches(
        &program,
        max_bytes,
        inputs.len(),
        at,
        drop_metric_name,
        |id, contract| {
            let schema = &contract.schema;
            let index = sources
                .iter()
                .position(|source| *source == id)
                .ok_or_else(|| miss("native source is unbound"))?;
            let node = entry.nodes.get(&inputs[index]);
            if let Some(asap_types::query_plan::QueryPlanNode::Logical { operator, .. }) = node {
                let (client, endpoint) = raw_endpoint
                    .ok_or_else(|| miss("query-time raw input has no Prometheus endpoint"))?;
                let at = i64::try_from(at).map_err(|_| miss("native timestamp overflow"))?;
                return Ok(BoundInput::Lazy(super::super::raw_source::bind(
                    contract, operator, at, client, endpoint,
                )?));
            }
            let Some(asap_types::query_plan::QueryPlanNode::ReadMaterialization { binding }) = node
            else {
                return Err(miss("native stored source has no deployed summary binding"));
            };
            let store = store.ok_or_else(|| {
                EngineError::capability_miss("native_stored", "summary store unavailable")
            })?;
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
                .map(BoundInput::Rows)
                .map_err(|error| match error {
                    crate::storage_engines::sketch_db::index::NativeReadError::Unavailable(
                        message,
                    ) => miss(message),
                    crate::storage_engines::sketch_db::index::NativeReadError::Physical(error) => {
                        EngineError::from(error)
                    }
                })
        },
    )
}

/// Stored and protocol inputs are read before execution; query-time raw
/// sources open lazily inside the run and charge its budget as they stream.
enum BoundInput {
    Rows(Batch),
    Lazy(asap_physical_operators::physical_planner::Source<'static>),
}

fn execute_batches(
    program: &asap_physical_operators::physical_planner::CompiledPhysicalDag,
    max_bytes: u64,
    input_count: usize,
    at: u64,
    drop_metric_name: bool,
    mut input_batch: impl FnMut(
        u64,
        &asap_physical_operators::physical_planner::InputContract,
    ) -> Result<BoundInput, EngineError>,
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
    use futures::StreamExt;
    use std::collections::BTreeMap;
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let context = crate::query_engines::request::context_with_limits(
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
    let mut sources = BTreeMap::new();
    let mut prepared = context.reserve(0)?;
    let mut input_bytes = 0usize;
    for (input_id, input) in program.input_contracts() {
        crate::query_engines::request::check()?;
        let batch = match input_batch(input_id, input)? {
            BoundInput::Rows(batch) => batch,
            BoundInput::Lazy(source) => {
                sources.insert(input_id, source);
                continue;
            }
        };
        input_bytes = input_bytes
            .checked_add(batch.bytes())
            .ok_or(asap_physical_operators::Error::MemoryLimit)?;
        if input_bytes > max_bytes as usize {
            return Err(asap_physical_operators::Error::MemoryLimit.into());
        }
        prepared.resize(input_bytes)?;
        let source =
            Operator::source(input.schema.clone(), vec![batch]).map_err(EngineError::from)?;
        sources.insert(input_id, Box::new(source) as Source<'_>);
    }
    let graph = program.instantiate(sources).map_err(EngineError::from)?;
    let mut retained = context.reserve(0)?;
    let mut result_bytes = 0usize;
    let mut stream = graph
        .execute(program.roots(), context)
        .map_err(EngineError::from)?
        .remove(0);
    let values = crate::query_engines::request::drive(async {
        let mut values = Vec::new();
        let mut renamed = std::collections::BTreeSet::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(EngineError::from)?;
            let identity = batch
                .schema()
                .fields
                .iter()
                .position(|field| field.name == SERIES_IDENTITY_COLUMN);
            let fields = &batch.schema().fields;
            let typed = |dtype: DataType| {
                fields
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| field.dtype == SummaryFamilyType::Plain(dtype.clone()))
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>()
            };
            // The sample is the Float64 column; an Int64 count is the sample
            // only when it is the sole numeric column.
            let value = match (
                typed(DataType::Float64).as_slice(),
                typed(DataType::Int64).as_slice(),
            ) {
                ([value, ..], _) => *value,
                ([], [count]) => *count,
                _ => return Err(miss("physical output loses sample value")),
            };
            for row in batch.rows() {
                let sample = &match row[value] {
                    Value::Float64(sample) => sample,
                    // A count is exact as a PromQL sample up to 2^53.
                    Value::Int64(count) if count.unsigned_abs() <= 1 << 53 => count as f64,
                    _ => return Err(miss("invalid physical result value")),
                };
                let labels = if let Some(identity) = identity {
                    let Value::Utf8(encoded) = &row[identity] else {
                        return Err(miss("invalid physical series identity"));
                    };
                    let mut labels = decode_series_identity(encoded).map_err(EngineError::from)?;
                    if drop_metric_name {
                        labels.remove("__name__");
                        // PromQL rejects a result whose series collide once
                        // the name is dropped; the exact engine reports it.
                        if !renamed.insert(labels.clone()) {
                            return Err(miss(
                                "vector cannot contain metrics with the same labelset",
                            ));
                        }
                    }
                    labels
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
                let point = InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                    *sample,
                )
                .with_label_keys_override(labels.into_keys().collect());
                result_bytes = result_bytes
                    .checked_add(point.retained_bytes())
                    .ok_or(dag::Error::MemoryLimit)?;
                retained.resize(result_bytes)?;
                values.push(point);
            }
        }
        Ok(values)
    })??;
    Ok((
        QueryResult::vector(values, at),
        super::ExecutionStats {
            summary_readout_evaluations: input_count,
            ..Default::default()
        },
    ))
}

#[cfg(test)]
mod request_contract_tests {
    use super::*;

    // A native candidate must use the enclosing request budget, even if its
    // deployment binding permits a larger standalone execution.
    #[tokio::test]
    async fn native_candidate_cannot_escape_request_memory_limit() {
        let result = crate::query_engines::request::run(
            dag::Limits {
                max_bytes: 1,
                ..dag::Limits::default()
            },
            |_| {
                let schema = schema(&[("value", DataType::Float64)]);
                let program =
                    asap_physical_operators::physical_planner::CompiledPhysicalDag::from_operators(
                        [(
                            0,
                            asap_physical_operators::physical_planner::InputContract::bounded(
                                schema.clone(),
                            ),
                        )]
                        .into(),
                        Default::default(),
                        vec![0],
                    )?;
                execute_batches(&program, 64 * 1024, 1, 0, false, |_, _| {
                    Batch::try_new(schema.clone(), vec![vec![Value::Float64(1.0)]])
                        .map(BoundInput::Rows)
                        .map_err(EngineError::from)
                })
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(EngineError::Physical(dag::Error::MemoryLimit))
        ));
    }
}
