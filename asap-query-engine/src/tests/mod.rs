pub mod accuracy_empirical_validation_tests;
pub mod accuracy_in_promql_response_tests;
pub mod capability_matching_tests;
pub mod capability_miss_http_e2e_tests;
pub mod clickhouse_forwarding_tests;
pub mod datafusion;
pub mod elastic_dsl_query_tests;
pub mod elastic_forwarding_tests;
pub mod persist_format_versioning_tests;
pub mod persistence_integration_tests;
pub mod persistence_perf_tests;
pub mod prometheus_forwarding_tests;
pub mod query_equivalence_tests;
pub mod schema_timeline_dispatch_tests;
pub mod sql_pattern_matching_tests;
pub mod store_correctness_tests;
pub mod trait_design_tests;

#[cfg(test)]
pub mod test_utilities;
