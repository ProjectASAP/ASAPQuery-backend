//! Fetch installed exact cuts from Prometheus before composing them with ASAP state.
use super::logical_dag::{from_result, PreparedLeaf, PreparedLeaves, Value};
use crate::query_engines::{index_store::IndexedSamples, EngineError};
use control_plane::query_plan::{
    logical::LogicalOperator, QueryNodeId, QueryPlanEntry, QueryPlanNode,
};
use std::collections::{BTreeMap, BTreeSet};

fn miss(message: impl Into<String>) -> EngineError {
    EngineError::capability_miss("exact_subquery", message.into())
}

/// Traverse only the installed graph, including epoch-aligned nested subquery grids.
fn leaves(
    entry: &QueryPlanEntry,
    times: &[u64],
) -> Result<BTreeMap<(QueryNodeId, i64), LogicalOperator>, EngineError> {
    let mut pending = Vec::new();
    for at in times {
        pending.push((
            entry.root,
            i64::try_from(*at).map_err(|_| miss("timestamp overflow"))?,
        ));
    }
    let mut visited = BTreeSet::new();
    let mut result = BTreeMap::new();
    while let Some((id, at)) = pending.pop() {
        if !visited.insert((id, at)) {
            continue;
        }
        if visited.len() > 200_000 {
            return Err(miss("installed execution grid exceeds budget"));
        }
        let node = entry
            .nodes
            .get(&id)
            .ok_or_else(|| miss("missing installed node"))?;
        match node {
            QueryPlanNode::Logical { operator, inputs } => match operator {
                LogicalOperator::Scan { .. } => {
                    return Err(miss("local raw Scan is forbidden in deployed plans"))
                }
                LogicalOperator::ExactSubquery { .. }
                | LogicalOperator::ReadRangeCounterIndex { .. }
                | LogicalOperator::ReadRangeMaxIndex { .. } => {
                    result.insert((id, at), operator.clone());
                }
                LogicalOperator::Subquery {
                    range_ms,
                    step_ms,
                    offset_ms,
                } => {
                    let step = i64::try_from(*step_ms).map_err(|_| miss("step overflow"))?;
                    let range = i64::try_from(*range_ms).map_err(|_| miss("range overflow"))?;
                    if step <= 0 || range / step > 100_000 {
                        return Err(miss("invalid subquery grid"));
                    }
                    let end = at
                        .checked_sub(*offset_ms)
                        .ok_or_else(|| miss("offset overflow"))?;
                    let start = end
                        .checked_sub(range)
                        .ok_or_else(|| miss("range overflow"))?;
                    let mut t = start
                        .div_euclid(step)
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(step))
                        .ok_or_else(|| miss("grid overflow"))?;
                    let input = *inputs
                        .first()
                        .ok_or_else(|| miss("missing subquery input"))?;
                    while t <= end {
                        pending.push((input, t));
                        t = t.checked_add(step).ok_or_else(|| miss("grid overflow"))?;
                    }
                }
                _ => pending.extend(inputs.iter().map(|input| (*input, at))),
            },
            // Existing bound summary subtrees are read by the synchronous callback.
            _ => {}
        }
    }
    Ok(result)
}

fn parse_result(body: &serde_json::Value, at: i64) -> Result<Value, EngineError> {
    if body["status"] != "success"
        || body
            .get("warnings")
            .and_then(|v| v.as_array())
            .is_some_and(|v| !v.is_empty())
    {
        return Err(miss(
            "Prometheus exact subquery failed or returned partial-result warnings",
        ));
    }
    let point = |v: &serde_json::Value| -> Result<f64, EngineError> {
        let row = v
            .as_array()
            .filter(|row| row.len() == 2)
            .ok_or_else(|| miss("invalid exact sample"))?;
        let timestamp = row[0]
            .as_f64()
            .filter(|v| v.is_finite())
            .ok_or_else(|| miss("invalid exact timestamp"))?;
        if (timestamp * 1000.0 - at as f64).abs() > 0.01 {
            return Err(miss(
                "exact sample timestamp differs from requested evaluation",
            ));
        }
        row[1]
            .as_str()
            .ok_or_else(|| miss("invalid exact value"))?
            .parse::<f64>()
            .map_err(|_| miss("invalid exact number"))
    };
    match body["data"]["resultType"].as_str() {
        Some("scalar") => Ok(Value::Scalar(point(&body["data"]["result"])?)),
        Some("vector") => {
            let rows = body["data"]["result"]
                .as_array()
                .ok_or_else(|| miss("invalid exact vector"))?;
            let mut seen = BTreeSet::new();
            let mut values = Vec::new();
            for row in rows {
                let labels = row["metric"]
                    .as_object()
                    .ok_or_else(|| miss("missing exact labels"))?
                    .iter()
                    .map(|(k, v)| {
                        Ok((
                            k.clone(),
                            v.as_str()
                                .ok_or_else(|| miss("invalid exact label"))?
                                .to_owned(),
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, EngineError>>()?;
                if !seen.insert(labels.clone()) {
                    return Err(miss("duplicate exact vector labels"));
                }
                values.push((labels, point(&row["value"])?));
            }
            Ok(Value::Vector(values))
        }
        _ => Err(miss(
            "installed exact cuts require scalar or instant vector; matrix cut is unsupported",
        )),
    }
}

pub(super) async fn prepare(
    entry: &QueryPlanEntry,
    times: &[u64],
    indexes: Option<&IndexedSamples>,
    endpoint: Option<&str>,
    client: &reqwest::Client,
) -> Result<PreparedLeaves, EngineError> {
    let mut prepared = PreparedLeaves::new();
    // Equivalent exact cuts at the same time share one actual remote request.
    let mut remote_cache = BTreeMap::<(String, i64), Value>::new();
    for ((id, at), operator) in leaves(entry, times)? {
        let instant = u64::try_from(at).map_err(|_| miss("subquery predates epoch"))?;
        let indexed = match (&operator, indexes) {
            (
                LogicalOperator::ReadRangeCounterIndex {
                    metric,
                    matchers,
                    range_ms,
                    offset_ms,
                    operation,
                    ..
                },
                Some(indexes),
            ) => Some(
                indexes.read_counter(metric, matchers, *range_ms, *offset_ms, instant, *operation),
            ),
            (
                LogicalOperator::ReadRangeMaxIndex {
                    metric,
                    matchers,
                    range_ms,
                    ..
                },
                Some(indexes),
            ) => Some(indexes.read_max(metric, matchers, *range_ms, instant)),
            _ => None,
        };
        if let Some(Ok((value, count))) = indexed {
            prepared.insert(
                (id, at),
                PreparedLeaf {
                    value: from_result(value)?,
                    remote: false,
                    remote_evaluations: 0,
                    remote_rpcs: 0,
                    index_reads: count,
                },
            );
            continue;
        }
        let query = match &operator {
            LogicalOperator::ExactSubquery { query } => query.clone(),
            _ => operator
                .exact_promql()
                .map_err(|e| miss(e.to_string()))?
                .ok_or_else(|| miss("indexed leaf has no exact fallback query"))?,
        };
        let key = (query.clone(), at);
        let cached = remote_cache.contains_key(&key);
        let value = if let Some(value) = remote_cache.get(&key) {
            value.clone()
        } else {
            let endpoint = endpoint.ok_or_else(|| miss("Prometheus exact endpoint unavailable"))?;
            let response = client
                .get(format!("{}/api/v1/query", endpoint.trim_end_matches('/')))
                .query(&[
                    ("query", query.as_str()),
                    ("time", &format!("{:.3}", at as f64 / 1000.0)),
                ])
                .send()
                .await
                .map_err(|e| miss(format!("exact request failed: {e}")))?;
            if !response.status().is_success() {
                return Err(miss(format!("exact endpoint HTTP {}", response.status())));
            }
            let body: serde_json::Value = response
                .json()
                .await
                .map_err(|e| miss(format!("invalid exact response: {e}")))?;
            let value = parse_result(&body, at)?;
            remote_cache.insert(key, value.clone());
            value
        };
        prepared.insert(
            (id, at),
            PreparedLeaf {
                value,
                remote: true,
                remote_evaluations: usize::from(!cached),
                remote_rpcs: usize::from(!cached),
                index_reads: 0,
            },
        );
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use control_plane::query_plan::{FallbackPolicy, InstantExecution};
    fn entry(nodes: BTreeMap<QueryNodeId, QueryPlanNode>) -> QueryPlanEntry {
        QueryPlanEntry {
            query_id: "remote-cut".into(),
            canonical_promql: "a / b".into(),
            root: QueryNodeId(0),
            nodes,
            instant: InstantExecution {
                lookback_ms: 300_000,
                full_history: false,
                cumulative_readout: true,
            },
            fallback: FallbackPolicy::ExactBackend,
        }
    }
    #[test]
    fn exact_boundary_rejects_partial_and_wrong_time_preserves_scalar() {
        assert!(matches!(parse_result(&serde_json::json!({"status":"success","data":{"resultType":"scalar","result":[1,"2"]}}),1000).unwrap(),Value::Scalar(2.0)));
        assert!(parse_result(&serde_json::json!({"status":"success","warnings":["partial"],"data":{"resultType":"scalar","result":[1,"2"]}}),1000).is_err());
        assert!(parse_result(&serde_json::json!({"status":"success","data":{"resultType":"scalar","result":[2,"2"]}}),1000).is_err());
    }
    #[tokio::test]
    async fn exact_leaf_calls_prometheus_and_combines_with_prepared_summary() {
        // A successful exact branch remains an intermediate, not a whole-root fallback.
        use control_plane::query_plan::logical::BinaryOperation;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let app=axum::Router::new().route("/api/v1/query",axum::routing::get(move |axum::extract::Query(params):axum::extract::Query<BTreeMap<String,String>>| {
            let count=count.clone(); async move { count.fetch_add(1,Ordering::SeqCst); assert_eq!(params["query"],"b"); assert_eq!(params["time"],"1.000");
                axum::Json(serde_json::json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"api"},"value":[1,"2"]}]}})) }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let entry = entry(BTreeMap::from([
            (
                QueryNodeId(0),
                QueryPlanNode::Logical {
                    operator: LogicalOperator::Binary {
                        operation: BinaryOperation::Div,
                        return_bool: false,
                    },
                    inputs: vec![QueryNodeId(1), QueryNodeId(2)],
                },
            ),
            (
                QueryNodeId(1),
                QueryPlanNode::SummaryMerge { inputs: vec![] },
            ),
            (
                QueryNodeId(2),
                QueryPlanNode::Logical {
                    operator: LogicalOperator::ExactSubquery { query: "b".into() },
                    inputs: vec![],
                },
            ),
        ]));
        let leaves = prepare(
            &entry,
            &[1000],
            None,
            Some(&format!("http://{address}")),
            &reqwest::Client::new(),
        )
        .await
        .unwrap();
        let (result, stats) =
            super::super::logical_dag::execute_installed(&entry, &leaves, 1000, |id, at| {
                assert_eq!(id, QueryNodeId(1));
                Ok(crate::query_engines::query_result::QueryResult::vector(
                    vec![
                        crate::query_engines::query_result::InstantVectorElement::new(
                            crate::storage_engines::types::KeyByLabelValues::new_with_labels(vec![
                                "api".into(),
                            ]),
                            6.0,
                        )
                        .with_label_keys_override(vec!["job".into()]),
                    ],
                    at,
                ))
            })
            .unwrap();
        let crate::query_engines::query_result::QueryResult::Vector(result) = result else {
            panic!("vector expected")
        };
        assert_eq!(result.values[0].value, 3.0);
        assert_eq!(stats.raw_scan_evaluations, 0);
        assert_eq!(stats.remote_evaluations, 1);
        assert_eq!(stats.summary_readout_evaluations, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut repeated = entry.clone();
        repeated.nodes.insert(
            QueryNodeId(1),
            QueryPlanNode::Logical {
                operator: LogicalOperator::ExactSubquery { query: "b".into() },
                inputs: vec![],
            },
        );
        let prepared = prepare(
            &repeated,
            &[1000],
            None,
            Some(&format!("http://{address}")),
            &reqwest::Client::new(),
        )
        .await
        .unwrap();
        let (_, stats) = super::super::logical_dag::execute_installed(
            &repeated,
            &prepared,
            1000,
            |_, _| unreachable!(),
        )
        .unwrap();
        assert_eq!(stats.remote_branch_evaluations, 2);
        assert_eq!(stats.remote_evaluations, 1);
        assert_eq!(stats.remote_rpcs, 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "each request fetched b once, never once per alias"
        );
        server.abort();
    }
    #[test]
    fn deployed_raw_scan_is_rejected_before_execution() {
        let entry = entry(BTreeMap::from([(
            QueryNodeId(0),
            QueryPlanNode::Logical {
                operator: LogicalOperator::Scan {
                    metric: Some("m".into()),
                    matchers: vec![],
                    range_ms: None,
                    offset_ms: 0,
                },
                inputs: vec![],
            },
        )]));
        assert!(leaves(&entry, &[1000])
            .unwrap_err()
            .to_string()
            .contains("forbidden"));
    }
}
