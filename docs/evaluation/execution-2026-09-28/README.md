# Installed-plan execution: 2026-09-28

All seven container runs passed against Prometheus: single-rate-temporal,
sparse-checkout-temporal, aggregations, aggregations-dense-cadence, issue-702,
issue-702-one-second and issue-754. Raw result reports and their hashes are
retained here; see [provenance](provenance.json) for the exact runner/runtime
revisions. The execution harness has been moved without changing its behavior.

The runner compiled a plan offline, validated its local execution path, and
installed that exact typed plan. Server startup did not re-plan. Reports retain
backend/reference results and execution provenance. Inputs are finite generated
fixtures; their accuracy contracts and analytical costs are not production ERP
measurements. This proves execution behavior for those plans, not that cost
selection matches production reality.

Reproduce with `promql-compliance/runner/Makefile` and the dataset/suite pairs
listed above; see [runner usage](../../../promql-compliance/README.md). The recorded
run used the debug binary mounted in the runtime container. No human review,
performance benefit, or Level 3 selection approval is implied.
