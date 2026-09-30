//! Executes the installed typed logical DAG. No serving-time PromQL parsing.
#[cfg(test)]
use asap_types::physical_plan_codec::PhysicalPlanCodec;
pub(super) mod native_values;
use crate::query_engines::{
    query_result::{InstantVectorElement, QueryResult},
    EngineError,
};
use crate::storage_engines::types::KeyByLabelValues;
use asap_physical_operators::dag as physical;
use asap_types::query_plan::query_time::QueryTimeOperator;
use asap_types::query_plan::{CandidateCompleteness, QueryNodeId, QueryPlanEntry, QueryPlanNode};
use futures::{FutureExt, StreamExt};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

type Labels = BTreeMap<String, String>;
type Vector = Vec<(Labels, f64)>;
type Matrix = Vec<(Labels, Vec<(i64, f64)>)>;
#[derive(Clone)]
pub(crate) enum Value {
    Scalar(f64),
    Vector(Vector),
    Matrix(Matrix, i64, i64),
}
impl Value {
    pub(crate) fn retained_bytes(&self) -> usize {
        fn labels(labels: &Labels) -> usize {
            labels
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity() + 96)
                .sum()
        }
        match self {
            Self::Scalar(_) => 8,
            Self::Vector(rows) => {
                rows.capacity() * std::mem::size_of::<(Labels, f64)>()
                    + rows.iter().map(|(keys, _)| labels(keys)).sum::<usize>()
            }
            Self::Matrix(rows, ..) => {
                rows.capacity() * std::mem::size_of::<(Labels, Vec<(i64, f64)>)>()
                    + rows
                        .iter()
                        .map(|(keys, points)| labels(keys) + points.capacity() * 16)
                        .sum::<usize>()
            }
        }
    }
}
#[derive(Debug, Default, Clone)]
pub struct ExecutionStats {
    /// Kept in execution provenance for compatibility; deployed DAGs cannot
    /// execute local raw scans, so this remains zero.
    pub raw_scan_evaluations: usize,
    pub summary_readout_evaluations: usize,
    pub memo_hits: usize,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
    pub remote_branch_evaluations: usize,
}
fn miss(detail: impl Into<String>) -> EngineError {
    EngineError::capability_miss("installed_logical_dag", detail)
}
fn vector(value: Value) -> Result<Vector, EngineError> {
    let Value::Vector(values) = value else {
        return Err(miss("instant vector required"));
    };
    let mut seen = BTreeSet::new();
    if values.iter().any(|(labels, _)| !seen.insert(labels)) {
        return Err(miss("duplicate vector label sets"));
    }
    Ok(values)
}
pub(crate) fn from_result(result: QueryResult) -> Result<Value, EngineError> {
    let QueryResult::Vector(value) = result else {
        return Err(miss("bound readout must return instant vector"));
    };
    if !value.warnings.is_empty() {
        return Err(miss("partial bound readout cannot feed logical operator"));
    }
    let values = value
        .values
        .into_iter()
        .map(|point| {
            let keys = point
                .label_keys_override
                .ok_or_else(|| miss("bound readout must carry explicit label keys"))?;
            if keys.len() != point.labels.labels.len()
                || keys.iter().collect::<BTreeSet<_>>().len() != keys.len()
            {
                return Err(miss("bound readout label arity mismatch"));
            }
            Ok((
                keys.into_iter().zip(point.labels.labels).collect(),
                point.value,
            ))
        })
        .collect::<Result<Vector, EngineError>>()?;
    Ok(Value::Vector(vector(Value::Vector(values))?))
}

pub(crate) struct PreparedLeaf {
    pub value: Value,
    pub remote: bool,
    pub remote_evaluations: usize,
    pub remote_rpcs: usize,
}
pub(crate) type PreparedLeaves = BTreeMap<(QueryNodeId, i64), PreparedLeaf>;

pub(crate) fn prepared_bytes(leaves: &PreparedLeaves) -> usize {
    leaves
        .values()
        .map(|leaf| leaf.value.retained_bytes() + 128)
        .sum()
}

pub(crate) fn execute_installed<F>(
    entry: &QueryPlanEntry,
    leaves: &PreparedLeaves,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    if entry.population_snapshot().is_some() || entry.physical_vector_binding().is_some() {
        if !leaves.is_empty() {
            return Err(miss(
                "population physical input must use its installed source binding",
            ));
        }
        return native_values::execute_vectors(entry, at, callback);
    }
    execute_values(entry, leaves, at, callback)
}

fn execute_values<F>(
    entry: &QueryPlanEntry,
    leaves: &PreparedLeaves,
    at: u64,
    callback: F,
) -> Result<(QueryResult, ExecutionStats), EngineError>
where
    F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>,
{
    let at_signed = i64::try_from(at).map_err(|_| miss("evaluation timestamp overflow"))?;
    let runtime = RefCell::new(ValueRuntime {
        entry,
        leaves,
        callback,
        stats: ExecutionStats::default(),
        warnings: vec![],
    });
    let error = RefCell::new(None);
    let mut graph = physical::PhysicalDag::default();
    let mut identities = BTreeMap::from([((entry.root, at_signed), 0u64)]);
    let mut pending = vec![(entry.root, at_signed)];
    let mut depths = BTreeMap::from([((entry.root, at_signed), 1usize)]);
    while let Some((id, time)) = pending.pop() {
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| miss("missing installed node"))?;
        let dependencies = if leaves.contains_key(&(id, time)) {
            vec![]
        } else {
            expanded_inputs(node, time)?
        };
        let mut input_ids = Vec::new();
        for dependency in &dependencies {
            if let Some(id) = identities.get(dependency) {
                runtime.borrow_mut().stats.memo_hits += 1;
                input_ids.push(*id);
            } else {
                if identities.len() >= 200_000 {
                    return Err(physical::Error::Operator(
                        "installed DAG evaluation budget exceeded".into(),
                    )
                    .into());
                }
                let depth = depths[&(id, time)] + 1;
                if depth > 128 {
                    return Err(physical::Error::Operator(
                        "installed DAG exceeds execution depth of 128".into(),
                    )
                    .into());
                }
                depths.insert(*dependency, depth);
                let id = identities.len() as u64;
                identities.insert(*dependency, id);
                pending.push(*dependency);
                input_ids.push(id);
            }
        }
        graph
            .add(
                identities[&(id, time)],
                input_ids,
                BoundValueOperator {
                    id,
                    time,
                    node,
                    dependencies,
                    runtime: &runtime,
                    error: &error,
                },
            )
            .map_err(EngineError::from)?;
    }
    let context = crate::query_engines::request::context(physical::Scope::Query {
        evaluation_time_ms: at_signed,
        revision: 0,
    })
    .map_err(EngineError::from)?;
    let mut output = graph
        .execute(&[0], context)
        .map_err(EngineError::from)?
        .remove(0);
    // I/O is prepared before this synchronous adapter; Pending is a cooperative yield.
    let evaluated = loop {
        crate::query_engines::request::check()?;
        match output.next().now_or_never() {
            Some(Some(Ok(value))) => break value.value().clone(),
            Some(Some(Err(failure))) => {
                return Err(error
                    .borrow_mut()
                    .take()
                    .unwrap_or(EngineError::Physical(failure)));
            }
            Some(None) => {
                return Err(physical::Error::Operator("query DAG produced no result".into()).into())
            }
            None => continue,
        }
    };
    drop(output);
    drop(graph);
    let mut evaluator = runtime.into_inner();
    if matches!(evaluated, Value::Scalar(_)) {
        // QueryResult currently models vectors/matrices only. Preserve a scalar
        // root's HTTP type by routing it to native, while scalar intermediates
        // remain typed inside vector composition.
        return Err(miss("scalar root requires native response adapter"));
    }
    let result = vector(evaluated)?;
    let mut output = QueryResult::vector(
        result
            .into_iter()
            .map(|(labels, value)| {
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(labels.values().cloned().collect()),
                    value,
                )
                .with_label_keys_override(labels.into_keys().collect())
            })
            .collect(),
        at,
    );
    if let QueryResult::Vector(vector) = &mut output {
        vector.warnings.append(&mut evaluator.warnings);
    }
    Ok((output, evaluator.stats))
}

struct ValueRuntime<'a, F> {
    entry: &'a QueryPlanEntry,
    leaves: &'a PreparedLeaves,
    stats: ExecutionStats,
    callback: F,
    warnings: Vec<String>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>> ValueRuntime<'_, F> {
    fn execute_node(
        &mut self,
        id: QueryNodeId,
        at: i64,
        node: &QueryPlanNode,
        inputs: &[&Value],
        context: &physical::RunContext,
    ) -> Result<Value, EngineError> {
        if let Some(leaf) = self.leaves.get(&(id, at)) {
            if leaf.remote {
                self.stats.remote_branch_evaluations += 1;
                self.stats.remote_evaluations += leaf.remote_evaluations;
                self.stats.remote_rpcs += leaf.remote_rpcs;
            } else {
                self.stats.summary_readout_evaluations += 1;
            }
            let value = leaf.value.clone();
            return Ok(value);
        }
        let value = match node.clone() {
            QueryPlanNode::Logical {
                operator: QueryTimeOperator::CurrentSeries { .. },
                ..
            } => {
                self.stats.summary_readout_evaluations += 1;
                from_result((self.callback)(
                    id,
                    u64::try_from(at).map_err(|_| miss("negative current-series timestamp"))?,
                )?)?
            }
            QueryPlanNode::Logical { .. } => {
                return Err(miss(
                    "installed Prometheus leaf was not prepared; backend raw execution is forbidden",
                ));
            }
            QueryPlanNode::PhysicalFragment {
                dag,
                row_input,
                pruning,
                ..
            } => {
                if let Some(value) = native_values::complete_values(&dag, inputs, context.clone())?
                {
                    return Ok(value);
                }
                let values = inputs
                    .iter()
                    .map(|value| vector((**value).clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(contract) = &pruning {
                    if let Some(warning) = pruning_warning(Some(&contract.completeness)) {
                        self.warnings.push(warning);
                    }
                }
                Value::Vector(native_values::physical(
                    &dag,
                    values,
                    row_input,
                    at,
                    context.clone(),
                )?)
            }

            _ => {
                self.stats.summary_readout_evaluations += 1;
                from_result((self.callback)(
                    id,
                    u64::try_from(at).map_err(|_| miss("summary timestamp predates epoch"))?,
                )?)?
            }
        };
        Ok(value)
    }
}

fn expanded_inputs(node: &QueryPlanNode, at: i64) -> Result<Vec<(QueryNodeId, i64)>, EngineError> {
    match node {
        QueryPlanNode::Logical {
            operator:
                QueryTimeOperator::Scan { .. }
                | QueryTimeOperator::ExactSubquery { .. }
                | QueryTimeOperator::CandidateExactSubquery { .. },
            ..
        } => Err(miss(
            "installed leaf was not prepared; local raw execution is forbidden",
        )),
        QueryPlanNode::Logical {
            operator: QueryTimeOperator::CurrentSeries { .. },
            ..
        } => Ok(vec![]),
        QueryPlanNode::PhysicalFragment { inputs, .. } => {
            Ok(inputs.iter().map(|&id| (id, at)).collect())
        }

        _ => Ok(vec![]),
    }
}
struct BoundValueOperator<'a, 'entry, F> {
    id: QueryNodeId,
    time: i64,
    node: &'entry QueryPlanNode,
    dependencies: Vec<(QueryNodeId, i64)>,
    runtime: &'a RefCell<ValueRuntime<'entry, F>>,
    error: &'a RefCell<Option<EngineError>>,
}
impl<F: FnMut(QueryNodeId, u64) -> Result<QueryResult, EngineError>>
    physical::PhysicalOperator<Value, ()> for BoundValueOperator<'_, '_, F>
{
    fn name(&self) -> &str {
        "InstalledValueOperator"
    }
    fn input_schemas(&self) -> Vec<()> {
        vec![(); self.dependencies.len()]
    }
    fn output_schema(&self) {}
    fn output_bytes(&self, value: &Value) -> usize {
        fn labels(value: &Labels) -> usize {
            value.iter().map(|(k, v)| k.len() + v.len()).sum()
        }
        match value {
            Value::Scalar(_) => 8,
            Value::Vector(values) => values.iter().map(|(key, _)| labels(key) + 8).sum(),
            Value::Matrix(values, ..) => values
                .iter()
                .map(|(key, points)| labels(key) + points.len() * 16)
                .sum(),
        }
    }
    fn start<'a>(
        &'a self,
        inputs: Vec<physical::Input<'a, Value>>,
        context: physical::RunContext,
    ) -> Result<physical::OutputStream<'a, Value>, physical::Error> {
        Ok(futures::stream::once(async move {
            let values =
                futures::future::try_join_all(inputs.into_iter().map(|mut input| async move {
                    input.next().await.ok_or_else(|| {
                        physical::Error::Operator("query input produced no value".into())
                    })?
                }))
                .await?;
            let values = values.iter().map(|v| v.value()).collect::<Vec<_>>();
            self.runtime
                .borrow_mut()
                .execute_node(self.id, self.time, self.node, &values, &context)
                .map_err(|error| {
                    *self.error.borrow_mut() = Some(error);
                    physical::Error::Operator(format!(
                        "query node {} at {} failed",
                        self.id.0, self.time
                    ))
                })
        })
        .boxed_local())
    }
}

fn pruning_warning(completeness: Option<&CandidateCompleteness>) -> Option<String> {
    match completeness {
        None | Some(CandidateCompleteness::Certified { .. }) => None,
        Some(CandidateCompleteness::BestEffort { guarantee }) => Some(match guarantee {
            Some(guarantee) => format!(
                "ASAP membership pruning is approximate: {:?}",
                guarantee.metric
            ),
            None => "ASAP membership pruning is approximate and uncertified".into(),
        }),
    }
}

#[cfg(test)]
fn semi_join(
    candidates: Vector,
    values: Vector,
    keys: &[(String, String)],
    completeness: Option<&CandidateCompleteness>,
    context: &physical::RunContext,
) -> Result<(Vector, Option<String>), EngineError> {
    use planner_types::{
        post_asap::{SummaryFamilyType, SummaryField, SummarySchema},
        pre_asap::{CompareOpKind, DataType, Predicate, QueryExpr},
    };
    use std::{rc::Rc, sync::Arc};
    let schema = |right: bool| {
        Arc::new(SummarySchema {
            fields: keys
                .iter()
                .map(|(l, r)| {
                    let name = if right { r } else { l };
                    SummaryField {
                        name: name.clone(),
                        dtype: SummaryFamilyType::Plain(if name == "value" {
                            DataType::Float64
                        } else {
                            DataType::Utf8
                        }),
                        nullable: false,
                    }
                })
                .collect(),
            time_index: None,
        })
    };
    let left = schema(false);
    let right = schema(true);
    let predicate = Predicate(Rc::new(QueryExpr::BoolAnd(
        (0..keys.len())
            .map(|i| QueryExpr::Compare {
                left: Rc::new(QueryExpr::Column(i)),
                op: CompareOpKind::Eq,
                right: Rc::new(QueryExpr::Column(keys.len() + i)),
            })
            .collect(),
    )));
    let rows = native_values::relation(
        values,
        candidates,
        predicate,
        left.clone(),
        right,
        left,
        completeness.cloned(),
        0,
        context.clone(),
    )?;
    Ok((rows, pruning_warning(completeness)))
}

fn native_labels(labels: &Labels) -> physical::values::Value {
    physical::values::Value::Map(
        labels
            .iter()
            .map(|(k, v)| {
                (
                    physical::values::Value::Utf8(k.as_str().into()),
                    physical::values::Value::Utf8(v.as_str().into()),
                )
            })
            .collect::<Vec<_>>()
            .into(),
    )
}
fn native_vector_output(
    rows: Vec<Vec<physical::values::Value>>,
    label_column: usize,
    value_column: usize,
) -> Result<Vector, EngineError> {
    use physical::values::Value as Cell;
    rows.into_iter()
        .map(|row| {
            let Some(Cell::Map(entries)) = row.get(label_column) else {
                return Err(miss("native operator returned invalid labels"));
            };
            let labels = entries
                .iter()
                .map(|(k, v)| match (k, v) {
                    (Cell::Utf8(k), Cell::Utf8(v)) => Ok((k.to_string(), v.to_string())),
                    _ => Err(miss("native label map is not Utf8")),
                })
                .collect::<Result<Labels, _>>()?;
            let value = match row.get(value_column) {
                Some(Cell::Float64(value)) => *value,
                Some(Cell::Int64(value)) if value.unsigned_abs() <= (1u64 << 53) => *value as f64,
                _ => {
                    return Err(miss(
                        "native result is not representable in the query Float64 protocol",
                    ))
                }
            };
            Ok((labels, value))
        })
        .collect()
}
#[cfg(test)]
fn test_native_context() -> physical::RunContext {
    physical::RunContext::new(
        physical::Scope::Query {
            evaluation_time_ms: 0,
            revision: 0,
        },
        physical::Limits::default(),
    )
    .unwrap()
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use asap_types::query_plan::{FallbackPolicy, InstantExecution};

    fn labels(items: &[(&str, &str)]) -> Labels {
        items
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
    }

    fn topk_membership_guarantee() -> planner_types::post_asap::ResultGuarantee {
        use planner_types::post_asap::{BoundExpr, ErrorMetric, ProbabilityExpr, ResultGuarantee};
        ResultGuarantee {
            metric: ErrorMetric::TopKMembership,
            bound: BoundExpr::Zero,
            failure_probability: ProbabilityExpr::Constant { value: 0.01 },
            provenance: vec![],
        }
    }

    // Numeric boundary fields must reach Planner as numbers, never absent labels.
    #[test]
    fn semi_join_does_not_match_unequal_numeric_samples() {
        let (rows, _) = semi_join(
            vec![(Labels::new(), 2.0)],
            vec![(Labels::new(), 1.0)],
            &[("value".into(), "value".into())],
            None,
            &test_native_context(),
        )
        .unwrap();
        assert!(rows.is_empty(), "unequal numeric samples matched: {rows:?}");
    }

    #[test]
    fn native_join_preserves_renamed_and_multiple_typed_keys() {
        let left = vec![
            (
                labels(&[("instance", "a"), ("zone", "east"), ("extra", "kept")]),
                1.,
            ),
            (labels(&[("instance", "a"), ("zone", "west")]), 2.),
        ];
        let right = vec![(labels(&[("pod", "a"), ("region", "east")]), 99.)];
        let (selected, _) = semi_join(
            right,
            left.clone(),
            &[
                ("instance".into(), "pod".into()),
                ("zone".into(), "region".into()),
            ],
            None,
            &test_native_context(),
        )
        .unwrap();
        assert_eq!(selected, vec![left[0].clone()]);
        for (a, b, matched) in [(f64::NAN, f64::NAN, false), (-0., 0., true), (1., 1., true)] {
            let (rows, _) = semi_join(
                vec![(Labels::new(), b)],
                vec![(Labels::new(), a)],
                &[("value".into(), "value".into())],
                None,
                &test_native_context(),
            )
            .unwrap();
            assert_eq!(!rows.is_empty(), matched);
        }
    }

    // The pruning join reads both the candidate and the exact readout.
    #[test]
    fn installed_candidate_sidecar_reads_both_summary_inputs() {
        let candidate_id = QueryNodeId(0);
        let value_id = QueryNodeId(1);
        let root = QueryNodeId(2);
        let entry = QueryPlanEntry {
            physical_dag: None,
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "candidate-topk".into(),
            canonical_query: "topk(1, rate(requests_total[5m]))".into(),
            fixed_evaluation: None,
            root,
            nodes: BTreeMap::from([
                (
                    candidate_id,
                    QueryPlanNode::ExactFallback {
                        reason: "prepared sketch readout".into(),
                    },
                ),
                (
                    value_id,
                    QueryPlanNode::ExactFallback {
                        reason: "prepared exact counter readout".into(),
                    },
                ),
                (root, {
                    let schema = planner_types::post_asap::SummarySchema {
                        fields: vec![planner_types::post_asap::SummaryField {
                            name: "pod".into(),
                            dtype: planner_types::post_asap::SummaryFamilyType::Plain(
                                planner_types::pre_asap::DataType::Utf8,
                            ),
                            nullable: false,
                        }],
                        time_index: None,
                    };
                    {
                        let schemas = vec![
                            std::sync::Arc::new(schema.clone()),
                            std::sync::Arc::new(schema.clone()),
                        ];
                        let node = planner_types::post_asap::PostAsapDagNode {
                            id: planner_types::post_asap::PostAsapNodeId(2),
                            payload:
                                planner_types::post_asap::PostAsapOperatorPayload::RelationalJoin {
                                    join_kind: planner_types::pre_asap::JoinKind::Semi,
                                    pred: serde_json::from_value(
                                        serde_json::to_value(planner_types::pre_asap::Predicate(
                                            std::rc::Rc::new(
                                                planner_types::pre_asap::QueryExpr::Compare {
                                                    left: std::rc::Rc::new(
                                                        planner_types::pre_asap::QueryExpr::Column(
                                                            0,
                                                        ),
                                                    ),
                                                    op: planner_types::pre_asap::CompareOpKind::Eq,
                                                    right: std::rc::Rc::new(
                                                        planner_types::pre_asap::QueryExpr::Column(
                                                            1,
                                                        ),
                                                    ),
                                                },
                                            ),
                                        ))
                                        .unwrap(),
                                    )
                                    .unwrap(),
                                    pruning: None,
                                },
                            output_state: planner_types::post_asap::ExecutionDataState::QUERY_ROWS,
                            output_schema: schema,
                            guarantee: None,
                        };
                        let operator = asap_physical_operators::physical_planner::compile_node(
                            &node, &schemas,
                        )
                        .unwrap();
                        let compiled = asap_physical_operators::physical_planner::CompiledPhysicalDag::from_operators(
                schemas.into_iter().enumerate().map(|(id, schema)| (id as u64, asap_physical_operators::physical_planner::InputContract::bounded(schema))).collect(),
                [(2, ((0..2).collect(), operator))].into(), vec![2],
            ).unwrap();
                        QueryPlanNode::PhysicalFragment {
                            inputs: [value_id, candidate_id].to_vec(),
                            dag: compiled.encode().unwrap(),
                            row_input: Some(0),
                            pruning: (Some(CandidateCompleteness::Certified {
                                guarantee: topk_membership_guarantee(),
                            }))
                            .map(|completeness| {
                                asap_types::query_plan::PruningInputContract {
                                    candidate_input: 1,
                                    keys: vec![(0, 0)],
                                    completeness,
                                }
                            }),
                        }
                    }
                }),
            ]),
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: false,
            },
            fallback: FallbackPolicy::ExactBackend,
        };
        let at = 300_000_i64;
        let leaves = BTreeMap::from([
            (
                (candidate_id, at),
                PreparedLeaf {
                    value: Value::Vector(vec![
                        (labels(&[("pod", "b")]), 100.0),
                        (labels(&[("pod", "c")]), 1.0),
                    ]),
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                },
            ),
            (
                (value_id, at),
                PreparedLeaf {
                    value: Value::Vector(vec![
                        (labels(&[("pod", "a")]), 2.0),
                        (labels(&[("pod", "b")]), 1.0),
                        (labels(&[("pod", "c")]), 3.0),
                    ]),
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                },
            ),
        ]);
        let (result, stats) = execute_installed(&entry, &leaves, at as u64, |_, _| {
            panic!("both inputs are prepared")
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("vector expected")
        };
        // The candidates keep only b and c; their exact values are authoritative.
        assert_eq!(
            result
                .values
                .iter()
                .map(|point| (point.labels.labels.clone(), point.value))
                .collect::<Vec<_>>(),
            vec![(vec!["b".to_string()], 1.0), (vec!["c".to_string()], 3.0)]
        );
        assert_eq!(stats.summary_readout_evaluations, 2);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn uncertified_candidate_sidecar_warns_or_falls_back_explicitly() {
        let candidates = vec![(labels(&[("pod", "a")]), 1.0)];
        let exact = vec![(labels(&[("pod", "a")]), 2.0)];
        let (_, warning) = semi_join(
            candidates.clone(),
            exact.clone(),
            &[("pod".into(), "pod".into())],
            Some(&CandidateCompleteness::BestEffort { guarantee: None }),
            &test_native_context(),
        )
        .unwrap();
        assert!(warning.unwrap().contains("approximate"));
        // Exact queries never lower an uncertified pruning semi-join. The Planner
        // emits its ordinary exact fallback instead; this runtime node is only
        // valid for certified or explicitly approximate plans.
        let certified = CandidateCompleteness::Certified {
            guarantee: topk_membership_guarantee(),
        };
        assert!(semi_join(
            vec![(labels(&[("pod", "missing")]), 1.0)],
            exact,
            &[("pod".into(), "pod".into())],
            Some(&certified),
            &test_native_context(),
        )
        .is_err());
    }
}

#[cfg(test)]
mod planner_computation_tests {
    use super::*;
    use crate::query_engines::asap_query_engine::test_plan::{
        planner_computed_entry, planner_series_entry,
    };

    fn readout(values: &[(&str, f64)], at: u64) -> QueryResult {
        QueryResult::vector(
            values
                .iter()
                .map(|(job, value)| {
                    InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(vec![(*job).into()]),
                        *value,
                    )
                    .with_label_keys_override(vec!["job".into()])
                })
                .collect(),
            at,
        )
    }

    fn readouts(entry: &QueryPlanEntry) -> Vec<QueryNodeId> {
        entry
            .nodes
            .iter()
            .filter(|(_, node)| matches!(node, QueryPlanNode::ExactReadout { .. }))
            .map(|(id, _)| *id)
            .collect()
    }

    // A ratio of grouped readouts, formerly a backend Binary operator, runs as
    // one Planner physical fragment over the two readouts.
    #[test]
    fn grouped_ratio_executes_as_one_planner_fragment() {
        let entry = planner_computed_entry(
            "sum by (job) (rate(errors_total[5m])) / sum by (job) (rate(requests_total[5m]))",
        );
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::PhysicalFragment { .. }
        ));
        assert!(!entry
            .nodes
            .values()
            .any(|node| matches!(node, QueryPlanNode::Logical { .. })));
        let [errors, requests] = readouts(&entry).try_into().unwrap();
        let (result, stats) = execute_installed(&entry, &BTreeMap::new(), 300_000, |id, at| {
            Ok(if id == errors {
                readout(&[("api", 1.0), ("db", 3.0)], at)
            } else {
                assert_eq!(id, requests);
                readout(&[("api", 4.0), ("db", 6.0), ("web", 1.0)], at)
            })
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        let mut values = result
            .values
            .iter()
            .map(|point| (point.labels.labels.clone(), point.value))
            .collect::<Vec<_>>();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            values,
            vec![
                (vec!["api".to_string()], 0.25),
                (vec!["db".to_string()], 0.5)
            ]
        );
        assert_eq!(stats.summary_readout_evaluations, 2);
    }

    // topk over an exact readout, formerly backend Sort and Limit operators,
    // ranks in a Planner fragment and keeps the readout's series labels.
    #[test]
    fn topk_over_readout_executes_as_a_planner_fragment() {
        let entry = planner_computed_entry("topk(2, rate(requests_total[5m]))");
        let QueryPlanNode::PhysicalFragment {
            row_input: Some(0), ..
        } = entry.nodes[&entry.root]
        else {
            panic!(
                "expected a row-preserving Planner fragment: {:?}",
                entry.nodes
            )
        };
        let [summary] = readouts(&entry).try_into().unwrap();
        let (result, stats) = execute_installed(&entry, &BTreeMap::new(), 300_000, |id, at| {
            assert_eq!(id, summary);
            Ok(readout(&[("a", 0.4), ("b", 1.2), ("c", 0.8)], at))
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        assert_eq!(
            result
                .values
                .iter()
                .map(|point| (point.labels.labels[0].as_str(), point.value))
                .collect::<Vec<_>>(),
            vec![("b", 1.2), ("c", 0.8)]
        );
        assert_eq!(stats.summary_readout_evaluations, 1);
    }

    fn series_readout(series: &[(&[(&str, &str)], f64)], at: u64) -> QueryResult {
        QueryResult::vector(
            series
                .iter()
                .map(|(labels, value)| {
                    InstantVectorElement::new(
                        KeyByLabelValues::new_with_labels(
                            labels.iter().map(|(_, v)| (*v).into()).collect(),
                        ),
                        *value,
                    )
                    .with_label_keys_override(labels.iter().map(|(k, _)| (*k).into()).collect())
                })
                .collect(),
            at,
        )
    }

    // Per-series division over two readouts matches series on their labels
    // without the metric name, drops unmatched series and the metric name.
    #[test]
    fn per_series_ratio_matches_readout_series() {
        let entry = planner_series_entry("rate(a[5m]) / rate(b[5m])");
        assert!(matches!(
            entry.nodes[&entry.root],
            QueryPlanNode::PhysicalFragment { .. }
        ));
        let [a, b] = readouts(&entry).try_into().unwrap();
        let (result, _) = execute_installed(&entry, &BTreeMap::new(), 300_000, |id, at| {
            Ok(if id == a {
                series_readout(
                    &[
                        (&[("__name__", "a"), ("job", "api")], 6.0),
                        (&[("__name__", "a"), ("job", "db")], 1.0),
                    ],
                    at,
                )
            } else {
                assert_eq!(id, b);
                series_readout(
                    &[
                        (&[("__name__", "b"), ("job", "api")], 3.0),
                        (&[("__name__", "b"), ("job", "web")], 1.0),
                    ],
                    at,
                )
            })
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        assert_eq!(
            result
                .values
                .iter()
                .map(|point| (point.labels.labels.clone(), point.value))
                .collect::<Vec<_>>(),
            vec![(vec!["api".to_string()], 2.0)]
        );
    }

    // A literal operand scales every per-series readout value and drops the
    // metric name the readouts carry.
    #[test]
    fn per_series_scalar_arithmetic_scales_each_series() {
        let entry = planner_series_entry("rate(a[5m]) * 2");
        let [a] = readouts(&entry).try_into().unwrap();
        let (result, _) = execute_installed(&entry, &BTreeMap::new(), 300_000, |id, at| {
            assert_eq!(id, a);
            Ok(series_readout(
                &[
                    (&[("__name__", "a"), ("job", "api")], 1.5),
                    (&[("__name__", "a"), ("job", "db")], 4.0),
                ],
                at,
            ))
        })
        .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        let mut values = result
            .values
            .iter()
            .map(|point| (point.labels.labels.clone(), point.value))
            .collect::<Vec<_>>();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            values,
            vec![
                (vec!["api".to_string()], 3.0),
                (vec!["db".to_string()], 8.0)
            ]
        );
    }

    // Source failures keep their routing classification across the shared runtime.
    #[test]
    fn source_error_classification_survives_execution() {
        let entry = planner_computed_entry("sum(rate(requests_total[5m])) * 2");
        let error = execute_installed(&entry, &BTreeMap::new(), 300_000, |_, _| {
            Err(EngineError::capability_miss("source", "failed"))
        })
        .unwrap_err();
        assert!(matches!(error,EngineError::CapabilityMiss{engine_id,..} if engine_id=="source"));
    }

    // Planner scalar programs run under the parent request budget and cancellation.
    #[test]
    fn native_scalar_shares_parent_resource_control() {
        let context = test_native_context();
        let graph = asap_physical_operators::physical_planner::promql_values::compile_scalar(7.)
            .unwrap()
            .encode()
            .unwrap();
        assert!(matches!(
            native_values::complete_values(&graph, &[], context.clone()).unwrap(),
            Some(Value::Scalar(7.))
        ));
        context.cancel();
        assert!(native_values::complete_values(&graph, &[], context.clone()).is_err());
    }
}
