//! Installed query → real HTTP input → shared physical execution contracts.
use super::{engine::ASAPQueryEngine, test_plan};
use crate::query_engines::{routing::query_engine_routing::QueryEngine, EngineError};
use crate::storage_engines::sketch_db::index::SketchStore;
use asap_physical_operators::dag::{Error, Limits};
use asap_types::query_plan::*;
use std::{collections::BTreeMap, sync::Arc};

fn external_entry() -> QueryPlanEntry {
    QueryPlanEntry {
        physical_dag: None,
        language: QueryLanguage::PromQl,
        query_id: "m".into(),
        canonical_query: "m".into(),
        fixed_evaluation: None,
        root: QueryNodeId(0),
        nodes: BTreeMap::from([(
            QueryNodeId(0),
            QueryPlanNode::ExternalExact {
                request: ExternalExactRequest {
                    language: QueryLanguage::PromQl,
                    expression: "m".into(),
                    output: ExternalExactOutput::InstantVector,
                    parameters: BTreeMap::new(),
                    start_parameter: None,
                    end_parameter: None,
                    input_contracts: vec![],
                },
                inputs: vec![],
            },
        )]),
        instant: InstantExecution {
            lookback_ms: 1000,
            full_history: false,
            cumulative_readout: true,
        },
        fallback: FallbackPolicy::ExactBackend,
    }
}

// One step fits; many steps must not each acquire an independent memory limit.
#[tokio::test]
async fn installed_range_enforces_one_budget_and_keeps_external_only_queries() {
    let app = axum::Router::new().route("/api/v1/query", axum::routing::get(|axum::extract::Query(params): axum::extract::Query<BTreeMap<String, String>>| async move {
        axum::Json(serde_json::json!({"status":"success", "data":{"resultType":"vector", "result":[
            {"metric":{"job":"api"},"value":[params["time"].parse::<f64>().unwrap(),"2"]}
        ]}}))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let index = Arc::new(SketchStore::new());
    let active = test_plan::install(&index, &[], vec![external_entry()]);
    let engine = ASAPQueryEngine::new(1000)
        .with_active_physical_plan(active)
        .with_exact_subquery_endpoint(endpoint)
        .with_execution_limits(Limits {
            max_bytes: 12_000,
            ..Limits::default()
        });
    engine.execute_at("m", 1000).await.unwrap();
    let error = engine
        .execute_range_promql_modern("m", 1000, 101_000, 1000)
        .await
        .unwrap_err();
    assert!(
        matches!(error, EngineError::Physical(Error::MemoryLimit)),
        "{error:?}"
    );
    server.abort();
}

// Candidate membership and arithmetic branches cannot smuggle local state
// into an external request without a shared input snapshot contract.
#[test]
fn mixed_bound_outputs_rejected_before_any_source_is_read() {
    let config = test_plan::materialization("m", "Sum", serde_json::json!({}), &[], 1000);
    let local = test_plan::entry(
        "sum_over_time(m[1s])",
        &config,
        PhysicalGrouping::PerEntity,
        1000,
        QueryPlanNode::ExactReadout {
            input: QueryNodeId(0),
            readout: ExactReadout::Sum,
        },
    );
    let mut entry = external_entry();
    entry
        .nodes
        .insert(QueryNodeId(1), local.nodes[&QueryNodeId(0)].clone());
    if let QueryPlanNode::ExternalExact { inputs, request } =
        entry.nodes.get_mut(&QueryNodeId(0)).unwrap()
    {
        inputs.push(QueryNodeId(1));
        request
            .input_contracts
            .push(ExternalExactInput::CandidateMembership {
                item_label: "job".into(),
            });
    }
    let error = entry
        .validate(&[config.policy_fingerprint()].into_iter().collect())
        .unwrap_err();
    assert!(
        error.to_string().contains("common snapshot proof"),
        "{error}"
    );
    entry.nodes.insert(
        QueryNodeId(1),
        QueryPlanNode::Logical {
            operator: query_time::QueryTimeOperator::CurrentSeries {
                population: current_series::SeriesPopulation {
                    metric: "m".into(),
                    matchers: vec![],
                    grouping: query_time::Grouping {
                        labels: vec!["job".into()],
                        without: false,
                    },
                    lookback_ms: 1000,
                    max_input_lag_ms: 1000,
                    history_retention_ms: 0,
                    max_series: 100,
                    max_bytes: 100_000,
                    max_k: 3,
                    quantiles: false,
                },
            },
            inputs: vec![],
        },
    );
    assert!(entry
        .validate_snapshot_sources()
        .unwrap_err()
        .to_string()
        .contains("common snapshot proof"));
    // An unreachable local node cannot turn an external-only graph into a mixed graph.
    if let QueryPlanNode::ExternalExact { inputs, request } =
        entry.nodes.get_mut(&QueryNodeId(0)).unwrap()
    {
        inputs.clear();
        request.input_contracts.clear();
    }
    entry.validate_snapshot_sources().unwrap();
}

// A concurrent publication cannot turn terminal execution failure into fallback.
#[test]
fn revision_race_preserves_execution_failure() {
    for failure in [Error::MemoryLimit, Error::Cancelled] {
        let result = super::engine::finish_query::<()>(Err(EngineError::Physical(failure)), false);
        assert!(
            matches!(result, Err(EngineError::Physical(_))),
            "{result:?}"
        );
    }
}

// Local readouts fit individually; retained range output eventually exhausts
// the same budget used by subsequent native evaluations.
#[tokio::test]
async fn installed_local_range_accounts_for_accumulated_results() {
    use crate::storage_engines::sketch_db::index::{AggKind, Capability, SummarySeriesMetadata};
    let config = test_plan::materialization("m", "Sum", serde_json::json!({}), &[], 1000);
    let entry = test_plan::entry(
        "sum_over_time(m[1s])",
        &config,
        PhysicalGrouping::PerEntity,
        1000,
        QueryPlanNode::ExactReadout {
            input: QueryNodeId(0),
            readout: ExactReadout::Sum,
        },
    );
    let index = Arc::new(SketchStore::new());
    index.register(SummarySeriesMetadata {
        storage_handle: 1,
        metric_name: "m".into(),
        group_by_keys: Default::default(),
        capability: Some(Capability::ExactAgg(asap_types::AggregationType::Sum)),
        agg_kind: AggKind::ExactAgg {
            agg_type: asap_types::AggregationType::Sum,
            parameters_canonical: String::new(),
            spatial_filter_canonical: String::new(),
        },
        accuracy: None,
        first_seen_unix_ms: 0,
        retired_at_ms: None,
        expires_at_ms: None,
        policy_fp: config.policy_fingerprint(),
    });
    let engine =
        test_plan::engine(index.clone(), config, vec![1], entry).with_execution_limits(Limits {
            max_bytes: 8192,
            ..Limits::default()
        });
    for pane in 0..100 {
        index.append_precompute(
            1,
            BTreeMap::new(),
            (pane * 1000, (pane + 1) * 1000),
            Box::new(asap_summary_state::summary_kernels::SumAccumulator::with_sum(2.0)),
        );
    }
    engine
        .execute_at("sum_over_time(m[1s])", 1000)
        .await
        .unwrap();
    let error = engine
        .execute_range_promql_modern("sum_over_time(m[1s])", 1000, 100_000, 1000)
        .await
        .unwrap_err();
    fn resource(error: &Error) -> bool {
        match error {
            Error::MemoryLimit => true,
            Error::AtNode { source, .. } => resource(source),
            _ => false,
        }
    }
    assert!(
        matches!(error, EngineError::Physical(ref error) if resource(error)),
        "{error:?}"
    );
}
