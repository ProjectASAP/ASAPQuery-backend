pub mod accuracy_empirical_validation_tests;
pub mod accuracy_in_promql_response_tests;
pub mod capability_miss_http_e2e_tests;
// M2.3.6g — legacy SketchStore-specific suites retired:
// persist_format_versioning_tests, persistence_integration_tests,
// persistence_perf_tests, store_correctness_tests.
// B7.5 retirement — `capability_matching_tests` and
// `schema_timeline_dispatch_tests` exercised the now-deleted
// legacy `build_query_execution_context_promql` /
// `handle_query_promql` paths; removed alongside the engine fns.
pub mod prometheus_forwarding_tests;
pub mod trait_design_tests;

#[cfg(test)]
pub mod test_utilities;
