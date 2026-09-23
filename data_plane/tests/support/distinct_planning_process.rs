use super::*;

/// Modeled HLL error without a confidence certificate cannot replace exact execution.
#[tokio::test]
async fn uncertified_distinct_uses_exact_process() {
    const QUERY: &str = "distinct_over_time(distinct_values{job=\"api\"}[5s])";
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../../docs/examples/asapquery-compatibility-demo-snapshot.json"
    ))
    .unwrap();
    let mut entry = fixture["query_workload"]["repeating_queries"][3].clone();
    entry["query"] = QUERY.into();
    entry["requirements"]["accuracy"] = serde_json::json!({"explicit": {"Epsilon": 0.05}});
    fixture["query_workload"]["repeating_queries"] = serde_json::json!([entry]);
    assert_uncertified_exact_process(fixture, &[QUERY]).await;
}
