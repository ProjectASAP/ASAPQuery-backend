# Accuracy evidence validation

This PR isolates evidence applicability from ranking. The public ERP adapter is
exercised with explicitly synthetic records in `accuracy_evidence.rs`: mismatched
distribution, implementation, metric, parameters, too few trials and excessive
error cannot admit a cheap candidate. Successful decisions retain record identity;
Hybrid theoretical fallback is distinguishable from empirical admission.

These tests validate the decision contract, not the quality of a real benchmark.
Level 3 needs applicable measured sketch errors, query-specific accuracy checks
against exact results, and provenance for the implementation/data/parameters.
Observed mean error alone cannot establish a requested tail-probability guarantee.
Existing readout-specific and online evidence freshness checks remain in `erp.rs`;
this PR neither replaces those checks nor adds a second semantic matcher.
