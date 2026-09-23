//! Fetch installed exact cuts from Prometheus before composing them with ASAP state.
use super::logical_dag::{PreparedLeaf, PreparedLeaves, Value};
use crate::query_engines::EngineError;
use asap_types::query_plan::{
    residual::ResidualQueryOperator, ExternalExactInput, ExternalExactRequest, QueryLanguage,
    QueryNodeId, QueryPlanEntry, QueryPlanNode,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const MAX_CANDIDATE_VALUES: usize = 10_000;
const MAX_CANDIDATE_QUERY_BYTES: usize = 1_048_576;

fn miss(message: impl Into<String>) -> EngineError {
    EngineError::capability_miss("exact_subquery", message.into())
}

/// Traverse only the installed graph, including epoch-aligned nested subquery grids.
#[derive(Debug, Clone)]
enum ExactLeaf {
    Legacy(ResidualQueryOperator),
    External(ExternalExactRequest),
}

fn leaves(
    entry: &QueryPlanEntry,
    times: &[u64],
) -> Result<BTreeMap<(QueryNodeId, i64), ExactLeaf>, EngineError> {
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
                ResidualQueryOperator::Scan { .. } => {
                    return Err(miss("local raw Scan is forbidden in deployed plans"))
                }
                ResidualQueryOperator::ExactSubquery { .. }
                | ResidualQueryOperator::CandidateExactSubquery { .. } => {
                    result.insert((id, at), ExactLeaf::Legacy(operator.clone()));
                }
                ResidualQueryOperator::Subquery {
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
            // MembershipFilter is a typed composition node rather than a Logical
            // wrapper, but its value input can still be a Prometheus leaf.
            QueryPlanNode::MembershipFilter { inputs, .. } => {
                pending.extend(inputs.iter().map(|input| (*input, at)));
            }
            QueryPlanNode::ExternalExact { request, inputs } => {
                pending.extend(inputs.iter().map(|input| (*input, at)));
                result.insert((id, at), ExactLeaf::External(request.clone()));
            }
            // Existing bound summary subtrees are read by the synchronous callback.
            _ => {}
        }
    }
    Ok(result)
}

pub(super) fn external_dependencies(
    entry: &QueryPlanEntry,
    times: &[u64],
) -> Result<Vec<(QueryNodeId, QueryNodeId, i64)>, EngineError> {
    let mut result = Vec::new();
    for ((id, at), leaf) in leaves(entry, times)? {
        if matches!(leaf, ExactLeaf::External(_)) {
            result.extend(
                entry.nodes[&id]
                    .inputs()
                    .iter()
                    .map(|input| (id, *input, at)),
            );
        } else if matches!(
            leaf,
            ExactLeaf::Legacy(ResidualQueryOperator::CandidateExactSubquery { .. })
        ) {
            let input = *entry.nodes[&id]
                .inputs()
                .first()
                .ok_or_else(|| miss("candidate exact subtree has no membership input"))?;
            result.push((id, input, at));
        }
    }
    Ok(result)
}

fn inject_candidate_matcher(
    query: &str,
    item_label: &str,
    candidates: &[String],
) -> Result<String, EngineError> {
    use promql_parser::{
        label::{MatchOp, Matcher},
        parser::Expr,
    };
    // Go's regexp.QuoteMeta (used by Prometheus) escapes a smaller set than
    // Rust's regex::escape; in particular, `\-` is not valid RE2 syntax.
    fn re2_quote_meta(value: &str) -> String {
        let mut quoted = String::with_capacity(value.len());
        for character in value.chars() {
            match character {
                '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^'
                | '$' => {
                    quoted.push('\\');
                    quoted.push(character);
                }
                '\n' => quoted.push_str("\\n"),
                '\r' => quoted.push_str("\\r"),
                '\t' => quoted.push_str("\\t"),
                character if character.is_control() => {
                    quoted.push_str(&format!("\\x{{{:x}}}", character as u32));
                }
                _ => quoted.push(character),
            }
        }
        quoted
    }
    let pattern = format!(
        "^(?:{})$",
        candidates
            .iter()
            .map(|value| re2_quote_meta(value))
            .collect::<Vec<_>>()
            .join("|")
    );
    // The pinned parser escapes matcher strings when formatting the AST.
    // Keep the semantic regex value here; pre-escaping it changes which labels match.
    let matcher = Matcher::new(
        MatchOp::Re(regex::Regex::new(&pattern).map_err(|error| miss(error.to_string()))?),
        item_label,
        &pattern,
    );
    fn visit(expr: &mut Expr, matcher: &Matcher) {
        let append = |matchers: &mut promql_parser::label::Matchers| {
            if matchers.or_matchers.is_empty() {
                matchers.matchers.push(matcher.clone());
            } else {
                for branch in &mut matchers.or_matchers {
                    branch.push(matcher.clone());
                }
            }
        };
        match expr {
            Expr::VectorSelector(selector) => append(&mut selector.matchers),
            Expr::MatrixSelector(selector) => append(&mut selector.vs.matchers),
            Expr::Aggregate(node) => visit(&mut node.expr, matcher),
            Expr::Unary(node) => visit(&mut node.expr, matcher),
            Expr::Binary(node) => {
                visit(&mut node.lhs, matcher);
                visit(&mut node.rhs, matcher);
            }
            Expr::Paren(node) => visit(&mut node.expr, matcher),
            Expr::Subquery(node) => visit(&mut node.expr, matcher),
            Expr::Call(node) => {
                for input in &mut node.args.args {
                    visit(input, matcher);
                }
            }
            Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
        }
    }
    let mut expression = promql_parser::parser::parse(query)
        .map_err(|error| miss(format!("invalid candidate exact query: {error}")))?;
    visit(&mut expression, &matcher);
    Ok(expression.to_string())
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

#[cfg(test)]
pub(super) async fn prepare(
    entry: &QueryPlanEntry,
    times: &[u64],
    endpoint: Option<&str>,
    client: &reqwest::Client,
) -> Result<PreparedLeaves, EngineError> {
    prepare_external(entry, times, endpoint, None, client, PreparedLeaves::new()).await
}

pub(super) async fn prepare_external(
    entry: &QueryPlanEntry,
    times: &[u64],
    prometheus_endpoint: Option<&str>,
    metricsql_endpoint: Option<&str>,
    client: &reqwest::Client,
    mut prepared: PreparedLeaves,
) -> Result<PreparedLeaves, EngineError> {
    // Equivalent exact cuts at the same time share one actual remote request.
    let mut remote_cache = HashMap::<(QueryLanguage, String, i64), Value>::new();
    for ((id, at), leaf) in leaves(entry, times)? {
        u64::try_from(at).map_err(|_| miss("subquery predates epoch"))?;
        let (language, query, candidate_input) = match &leaf {
            ExactLeaf::Legacy(ResidualQueryOperator::ExactSubquery { query }) => {
                (QueryLanguage::PromQl, query.clone(), None)
            }
            ExactLeaf::Legacy(ResidualQueryOperator::CandidateExactSubquery {
                query,
                item_label,
            }) => (
                QueryLanguage::PromQl,
                query.clone(),
                Some((entry.nodes[&id].inputs()[0], item_label.as_str())),
            ),
            ExactLeaf::External(request) => {
                if !matches!(
                    request.language,
                    QueryLanguage::PromQl | QueryLanguage::MetricsQl
                ) {
                    return Err(miss(format!(
                        "external exact language {:?} has no installed adapter",
                        request.language
                    )));
                }
                let candidate = match request.input_contracts.as_slice() {
                    [] => None,
                    [ExternalExactInput::CandidateMembership { item_label }] => {
                        Some((entry.nodes[&id].inputs()[0], item_label.as_str()))
                    }
                    _ => return Err(miss("unsupported external exact input contract")),
                };
                (request.language, request.expression.clone(), candidate)
            }
            _ => return Err(miss("prepared leaf is not an exact subtree")),
        };
        let (query, candidate_filtered) = if let Some((candidate_input, item_label)) =
            candidate_input
        {
            if language == QueryLanguage::MetricsQl {
                return Err(miss(
                    "candidate-filtered MetricsQL exact subqueries are not implemented",
                ));
            }
            let candidate = prepared
                .get(&(candidate_input, at))
                .ok_or_else(|| miss("candidate membership was not prepared"))?;
            let Value::Vector(rows) = &candidate.value else {
                return Err(miss("candidate membership is not an instant vector"));
            };
            let mut values = rows
                .iter()
                .map(|(labels, _)| {
                    labels.get(item_label).cloned().ok_or_else(|| {
                        miss(format!(
                            "candidate membership is missing item label {item_label}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            values.sort();
            values.dedup();
            if values.is_empty() {
                prepared.insert(
                    (id, at),
                    PreparedLeaf {
                        value: Value::Vector(Vec::new()),
                        remote: true,
                        remote_evaluations: 0,
                        remote_rpcs: 0,
                    },
                );
                continue;
            }
            if values.len() > MAX_CANDIDATE_VALUES {
                return Err(miss(format!(
                    "candidate set has {} values, exceeding limit {MAX_CANDIDATE_VALUES}",
                    values.len()
                )));
            }
            let restricted = inject_candidate_matcher(&query, item_label, &values)?;
            if restricted.len() > MAX_CANDIDATE_QUERY_BYTES {
                return Err(miss(format!(
                        "candidate-filtered exact query has {} bytes, exceeding limit {MAX_CANDIDATE_QUERY_BYTES}",
                        restricted.len()
                    )));
            }
            (restricted, true)
        } else {
            (query, false)
        };
        let key = (language, query.clone(), at);
        let cached = remote_cache.contains_key(&key);
        let value = if let Some(value) = remote_cache.get(&key) {
            value.clone()
        } else {
            let endpoint = match language {
                QueryLanguage::PromQl => prometheus_endpoint
                    .ok_or_else(|| miss("Prometheus exact endpoint unavailable"))?,
                QueryLanguage::MetricsQl => metricsql_endpoint
                    .ok_or_else(|| miss("VictoriaMetrics exact endpoint unavailable"))?,
                QueryLanguage::ClickHouseSql => {
                    return Err(miss("ClickHouse exact subquery needs its SQL adapter"))
                }
            };
            let url = format!("{}/api/v1/query", endpoint.trim_end_matches('/'));
            let time = format!("{:.3}", at as f64 / 1000.0);
            // Candidate sets can be large enough to exceed proxy URL limits;
            // Prometheus accepts the instant-query parameters as an encoded
            // form body. Static exact cuts keep their existing GET contract.
            let request = if candidate_filtered {
                client
                    .post(url)
                    .form(&[("query", query.as_str()), ("time", time.as_str())])
            } else {
                client
                    .get(url)
                    .query(&[("query", query.as_str()), ("time", time.as_str())])
            };
            let response = request
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
            },
        );
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_plan::{FallbackPolicy, InstantExecution};
    fn entry(nodes: BTreeMap<QueryNodeId, QueryPlanNode>) -> QueryPlanEntry {
        QueryPlanEntry {
            language: asap_types::query_plan::QueryLanguage::PromQl,
            query_id: "remote-cut".into(),
            canonical_query: "a / b".into(),
            fixed_evaluation: None,
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
    async fn metricsql_external_leaf_uses_victoriametrics_endpoint() {
        let app =
            axum::Router::new().route(
                "/api/v1/query",
                axum::routing::get(
                    |axum::extract::Query(params): axum::extract::Query<
                        BTreeMap<String, String>,
                    >| async move {
                        assert_eq!(params["query"], "sum(rate(m[5m]))");
                        axum::Json(serde_json::json!({
                            "status":"success",
                            "data":{"resultType":"scalar","result":[1,"7"]}
                        }))
                    },
                ),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut planned = entry(BTreeMap::from([(
            QueryNodeId(0),
            QueryPlanNode::ExternalExact {
                request: ExternalExactRequest {
                    language: QueryLanguage::MetricsQl,
                    expression: "sum(rate(m[5m]))".into(),
                    output: asap_types::query_plan::ExternalExactOutput::InstantVector,
                    parameters: BTreeMap::new(),
                    start_parameter: None,
                    end_parameter: None,
                    input_contracts: vec![],
                },
                inputs: vec![],
            },
        )]));
        planned.language = QueryLanguage::MetricsQl;
        let prepared = prepare_external(
            &planned,
            &[1_000],
            Some("http://127.0.0.1:9"),
            Some(&format!("http://{address}")),
            &reqwest::Client::new(),
            PreparedLeaves::new(),
        )
        .await
        .unwrap();
        assert!(matches!(
            prepared[&(QueryNodeId(0), 1_000)].value,
            Value::Scalar(7.0)
        ));
        server.abort();
    }

    fn candidate_entry(query: &str) -> QueryPlanEntry {
        entry(BTreeMap::from([
            (
                QueryNodeId(0),
                QueryPlanNode::ExternalExact {
                    request: ExternalExactRequest {
                        language: QueryLanguage::PromQl,
                        expression: query.into(),
                        output: asap_types::query_plan::ExternalExactOutput::InstantVector,
                        parameters: BTreeMap::new(),
                        start_parameter: None,
                        end_parameter: None,
                        input_contracts: vec![ExternalExactInput::CandidateMembership {
                            item_label: "job".into(),
                        }],
                    },
                    inputs: vec![QueryNodeId(1)],
                },
            ),
            (
                QueryNodeId(1),
                QueryPlanNode::SummaryMerge { inputs: vec![] },
            ),
        ]))
    }

    fn candidate_rows(values: impl IntoIterator<Item = String>) -> PreparedLeaves {
        [(
            (QueryNodeId(1), 1_000),
            PreparedLeaf {
                value: Value::Vector(
                    values
                        .into_iter()
                        .map(|value| (BTreeMap::from([("job".into(), value)]), 1.0))
                        .collect(),
                ),
                remote: false,
                remote_evaluations: 0,
                remote_rpcs: 0,
            },
        )]
        .into_iter()
        .collect()
    }

    #[test]
    fn candidate_matcher_is_ast_conjoined_and_regex_escaped() {
        use promql_parser::parser::Expr;
        let values = vec!["api.v1".into(), "quote\"slash\\".into(), "a|b".into()];
        let restricted = inject_candidate_matcher(
            "sum by (job) (rate(m{cluster=\"prod\",job!=\"blocked\"}[5m]))",
            "job",
            &values,
        )
        .unwrap();
        let expression = promql_parser::parser::parse(&restricted).unwrap();
        let Expr::Aggregate(aggregate) = expression else {
            panic!("aggregate expected: {restricted}")
        };
        let Expr::Call(call) = aggregate.expr.as_ref() else {
            panic!("rate expected: {restricted}")
        };
        let Expr::MatrixSelector(selector) = &*call.args.args[0] else {
            panic!("matrix selector expected: {restricted}")
        };
        assert!(selector
            .vs
            .matchers
            .matchers
            .iter()
            .any(|matcher| matcher.name == "cluster" && matcher.is_match("prod")));
        assert!(selector
            .vs
            .matchers
            .matchers
            .iter()
            .any(|matcher| matcher.name == "job" && !matcher.is_match("blocked")));
        let candidate_matcher = selector
            .vs
            .matchers
            .matchers
            .iter()
            .find(|matcher| matcher.name == "job" && matcher.value.starts_with("^(?:"))
            .unwrap();
        for value in &values {
            assert!(candidate_matcher.is_match(value), "{value:?}: {restricted}");
        }
        assert!(!candidate_matcher.is_match("apiXv1"));
        assert!(!candidate_matcher.is_match("a"));
        assert!(!candidate_matcher.is_match("b"));
    }

    #[tokio::test]
    async fn empty_candidate_set_returns_empty_exact_leaf_without_rpc() {
        let entry = candidate_entry("sum by (job) (rate(m[5m]))");
        let prepared = prepare_external(
            &entry,
            &[1_000],
            None,
            None,
            &reqwest::Client::new(),
            candidate_rows(Vec::new()),
        )
        .await
        .unwrap();
        let exact = &prepared[&(QueryNodeId(0), 1_000)];
        assert!(matches!(&exact.value, Value::Vector(rows) if rows.is_empty()));
        assert_eq!((exact.remote_evaluations, exact.remote_rpcs), (0, 0));
    }

    #[tokio::test]
    async fn missing_external_endpoint_fails_closed_before_any_rpc() {
        let result = prepare_external(
            &candidate_entry("sum by (job) (rate(m[5m]))"),
            &[1_000],
            None,
            None,
            &reqwest::Client::new(),
            candidate_rows(["api".into()]),
        )
        .await;
        assert!(matches!(
            result,
            Err(EngineError::CapabilityMiss { ref detail, .. })
                if detail.contains("Prometheus exact endpoint unavailable")
        ));
    }

    #[tokio::test]
    async fn candidate_exact_is_discovered_and_prepared_behind_membership_filter_root() {
        use asap_types::query_plan::CandidateCompleteness;
        let mut entry = candidate_entry("sum by (job) (rate(m[5m]))");
        entry.nodes.insert(
            QueryNodeId(2),
            QueryPlanNode::MembershipFilter {
                inputs: [QueryNodeId(1), QueryNodeId(0)],
                completeness: CandidateCompleteness::BestEffort { guarantee: None },
            },
        );
        entry.nodes.insert(
            QueryNodeId(3),
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::TopKSelection {
                    k: 2,
                    grouping: asap_types::query_plan::residual::Grouping {
                        labels: vec![],
                        without: false,
                    },
                },
                inputs: vec![QueryNodeId(2)],
            },
        );
        entry.root = QueryNodeId(3);
        let dependencies = external_dependencies(&entry, &[1_000]).unwrap();
        assert_eq!(dependencies, vec![(QueryNodeId(0), QueryNodeId(1), 1_000)]);
        let prepared = prepare_external(
            &entry,
            &[1_000],
            None,
            None,
            &reqwest::Client::new(),
            candidate_rows(Vec::new()),
        )
        .await
        .unwrap();
        assert!(prepared.contains_key(&(QueryNodeId(0), 1_000)));
    }

    #[tokio::test]
    async fn high_cardinality_candidates_use_one_post_and_preserve_exact_labels() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let app = axum::Router::new().route(
            "/api/v1/query",
            axum::routing::post(
                move |axum::Form(params): axum::Form<BTreeMap<String, String>>| {
                    let count = count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(params["time"], "1.000");
                        let parsed = promql_parser::parser::parse(&params["query"]).unwrap();
                        assert!(matches!(parsed, promql_parser::parser::Expr::Aggregate(_)));
                        assert!(params["query"].contains("job-999"));
                        axum::Json(serde_json::json!({
                            "status":"success",
                            "data":{"resultType":"vector","result":[{
                                "metric":{"__name__":"m","job":"job-999"},
                                "value":[1,"42"]
                            }]}
                        }))
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let entry = candidate_entry("sum by (job) (rate(m{job!=\"blocked\"}[5m]))");
        let candidates = (0..1_000).map(|index| format!("job-{index}"));

        let prepared = prepare_external(
            &entry,
            &[1_000],
            Some(&format!("http://{address}")),
            None,
            &reqwest::Client::new(),
            candidate_rows(candidates),
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let exact = &prepared[&(QueryNodeId(0), 1_000)];
        assert_eq!((exact.remote_evaluations, exact.remote_rpcs), (1, 1));
        assert!(matches!(
            &exact.value,
            Value::Vector(rows)
                if rows.len() == 1
                    && rows[0].0.get("job").map(String::as_str) == Some("job-999")
                    && rows[0].0.get("__name__").map(String::as_str) == Some("m")
        ));
        server.abort();
    }

    #[tokio::test]
    async fn candidate_exact_rpc_failure_is_a_capability_miss_for_whole_query_fallback() {
        let app = axum::Router::new().route(
            "/api/v1/query",
            axum::routing::post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let result = prepare_external(
            &candidate_entry("sum by (job) (rate(m[5m]))"),
            &[1_000],
            Some(&format!("http://{address}")),
            None,
            &reqwest::Client::new(),
            candidate_rows(["api".into()]),
        )
        .await;
        let Err(error) = result else {
            panic!("HTTP failure must reject the hybrid branch")
        };
        assert!(matches!(
            error,
            EngineError::CapabilityMiss { engine_id: "exact_subquery", ref detail }
                if detail.contains("HTTP 503")
        ));
        server.abort();
    }
    #[tokio::test]
    async fn exact_leaf_calls_prometheus_and_combines_with_prepared_summary() {
        // A successful exact branch remains an intermediate, not a whole-root fallback.
        use asap_types::query_plan::residual::BinaryOperation;
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
                    operator: ResidualQueryOperator::Binary {
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
                    operator: ResidualQueryOperator::ExactSubquery { query: "b".into() },
                    inputs: vec![],
                },
            ),
        ]));
        let leaves = prepare(
            &entry,
            &[1000],
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
                operator: ResidualQueryOperator::ExactSubquery { query: "b".into() },
                inputs: vec![],
            },
        );
        let prepared = prepare(
            &repeated,
            &[1000],
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

    #[tokio::test]
    async fn five_minute_error_ratio_combines_prometheus_cut_with_summary_store() {
        use crate::query_engines::query_result::{InstantVectorElement, QueryResult};
        use crate::storage_engines::sketch_db::{
            data::AggKind,
            index::{Capability, SummarySeriesMetadata},
        };
        use crate::storage_engines::types::{KeyByLabelValues, Measurement};
        use asap_physical_operators::accumulators::IncreaseAccumulator;
        use asap_types::query_plan::{
            residual::BinaryOperation, ExactReadout, MaterializationBinding, PhysicalGrouping,
        };
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };

        const AT: u64 = 300_000;
        const MATERIALIZATION: asap_types::PolicyFingerprint = asap_types::PolicyFingerprint(9001);
        let store = crate::storage_engines::sketch_db::index::SketchStore::new();
        store.register(SummarySeriesMetadata {
            sid: 41,
            metric_name: "http_requests_total".into(),
            group_by_keys: std::collections::BTreeSet::from(["job".into()]),
            capability: Some(Capability::ExactAgg(asap_types::AggregationType::Rate)),
            agg_kind: AggKind::ExactAgg {
                agg_type: asap_types::AggregationType::Rate,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            accuracy: None,
            first_seen_unix_ms: 0,
            retired_at_ms: None,
            expires_at_ms: None,
            policy_fp: MATERIALIZATION,
        });
        let mut denominator =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        denominator.update(Measurement::new(400.0), AT as i64);
        store.append_precompute(
            41,
            BTreeMap::from([("job".into(), "user-service".into())]),
            (0, AT),
            Box::new(denominator),
        );

        let exact_query =
            "sum by (job) (rate(http_requests_total{status=~\"5..\",job=\"user-service\"}[5m]))";
        let entry = entry(BTreeMap::from([
            (
                QueryNodeId(0),
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::Binary {
                        operation: BinaryOperation::Div,
                        return_bool: false,
                    },
                    inputs: vec![QueryNodeId(1), QueryNodeId(2)],
                },
            ),
            (
                QueryNodeId(1),
                QueryPlanNode::Logical {
                    operator: ResidualQueryOperator::ExactSubquery {
                        query: exact_query.into(),
                    },
                    inputs: vec![],
                },
            ),
            (
                QueryNodeId(2),
                QueryPlanNode::ExactReadout {
                    input: QueryNodeId(3),
                    readout: ExactReadout::Rate,
                },
            ),
            (
                QueryNodeId(3),
                QueryPlanNode::ReadMaterialization {
                    binding: MaterializationBinding {
                        full_window_slide_ms: None,
                        item_labels: Vec::new(),
                        materialization: MATERIALIZATION.into(),
                        stored_output_reference:
                            asap_types::sds::StoredOutputReference::for_definition(
                                MATERIALIZATION.into(),
                            ),
                        output_grouping: PhysicalGrouping::Reduce(vec!["job".into()]),
                        window_ms: AT,
                        pane_origin_ms: Some(0),
                        readout_lookback_ms: Some(AT),
                    },
                },
            ),
        ]));

        let calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = calls.clone();
        let app = axum::Router::new().route(
            "/api/v1/query",
            axum::routing::get(
                move |axum::extract::Query(params): axum::extract::Query<
                    BTreeMap<String, String>,
                >| {
                    let observed_calls = observed_calls.clone();
                    async move {
                        observed_calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(params["query"], exact_query);
                        assert_eq!(params["time"], "300.000");
                        axum::Json(serde_json::json!({
                            "status": "success",
                            "data": {"resultType": "vector", "result": [{
                                "metric": {"job": "user-service"},
                                "value": [300, "0.1"]
                            }]}
                        }))
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let prepared = prepare(
            &entry,
            &[AT],
            Some(&format!("http://{address}")),
            &reqwest::Client::new(),
        )
        .await
        .unwrap();
        let (result, stats) =
            super::super::logical_dag::execute_installed(&entry, &prepared, AT, |root, at| {
                assert_eq!(root, QueryNodeId(2));
                let mut summary = entry.clone();
                summary.root = root;
                summary.nodes.retain(|id, _| matches!(id.0, 2 | 3));
                let (outcome, _) = super::super::post_asap_readout::execute_query_plan_instant(
                    &store, &summary, at,
                )
                .map_err(|error| miss(format!("summary readout failed: {error:?}")))?;
                let rows = outcome
                    .series
                    .into_iter()
                    .map(|(labels, samples)| {
                        let (keys, values): (Vec<_>, Vec<_>) = labels.into_iter().unzip();
                        Ok(InstantVectorElement::new(
                            KeyByLabelValues::new_with_labels(values),
                            samples
                                .last()
                                .ok_or_else(|| miss("summary returned no point"))?
                                .1,
                        )
                        .with_label_keys_override(keys))
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?;
                Ok(QueryResult::vector(rows, at))
            })
            .unwrap();
        let QueryResult::Vector(result) = result else {
            panic!("instant vector expected")
        };
        assert_eq!(result.timestamp, AT);
        assert_eq!(result.values.len(), 1);
        assert_eq!(
            result.values[0].label_keys_override.as_deref(),
            Some(&["job".into()][..])
        );
        assert_eq!(result.values[0].labels.labels, vec!["user-service"]);
        assert!((result.values[0].value - 0.1).abs() < 1e-12);
        assert_eq!(stats.raw_scan_evaluations, 0);
        assert_eq!(stats.remote_evaluations, 1);
        assert_eq!(stats.remote_rpcs, 1);
        assert_eq!(stats.summary_readout_evaluations, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }
    #[test]
    fn deployed_raw_scan_is_rejected_before_execution() {
        let entry = entry(BTreeMap::from([(
            QueryNodeId(0),
            QueryPlanNode::Logical {
                operator: ResidualQueryOperator::Scan {
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
