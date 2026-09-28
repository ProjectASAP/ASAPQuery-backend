# Planning validation: separate contracts and evidence

Audience: implementers and reviewers. Each PR proves one contract; a downstream
pass cannot substitute for a missing upstream check.

| Layer | Input | Assertion |
| --- | --- | --- |
| Level 1 (#728) | workload and declared capabilities/accuracy requirements | Every exposed supported candidate has a valid physical DAG or an explicit binding rejection; no prices or winner assertions |
| Level 2 (#742) | same candidates plus synthetic complete quotes | Ranking, cost reversal, infeasible/missing quote exclusion; no claim about real costs |
| Workload statistics | ingestion observations, query history and series observations | Scope, units, observation windows, freshness and missingness survive into demand |
| Accuracy evidence | sketch error evidence and query requirements | Only applicable evidence admits a candidate; observed mean error is not a formal guarantee |
| Resource measurements | measured operator CPU, memory and storage | Correct units and work multiplicities, shared work counted once, provenance retained |
| Execution correctness | installed physical plan and input data | Results, stored state, recovery and coverage; independent of selection quality |
| Level 3 (#759) | real workload, applicable accuracy evidence and measured resource costs | Accurate results and measured selection quality under a declared objective |

ERP means Error–Resource Profile. Workload statistics describe how much work is
requested. Accuracy evidence constrains which candidates are legal. Resource
measurements price feasible candidates. Synthetic prices test the selector only.

## Evidence required for Level 3

Record the workload identity, observation interval, query frequency, accepted
sample count, distinct-series scope, implementation revision, machine, sketch
parameters and evidence timestamps. Preserve missing/unsupported dimensions;
do not replace them with zero or relabel analytical coefficients as measured.

Freeze planning inputs before the evaluation interval. Measure each feasible
candidate under the same workload, horizon and resource objective, using repeated
runs. Compare predicted costs and selected-plan measurements with the measured
best candidate, reporting uncertainty and selection regret. Validate results
against exact execution and the query's accuracy contract. Evidence from the
same samples used to tune the model is not independent selection validation.

Historical differential runs establish execution correctness. Historical benefit
runs against another engine establish only their stated performance comparison.
Neither establishes Level 3 selection quality. A real-evidence run with missing
inputs must be reported as incomplete, never as a synthetic pass.

## Agreed Level 3 experiment

Use a real historical trace replay, preserving input/query timing. First validate
individual queries; then evaluate the full concurrent workload including sharing
and contention. With accuracy, p95 latency and memory limits fixed in advance,
compare measured total CPU over a common horizon. Keep predicted costs alongside
measured results and report selection regret; do not claim production optimality
from synthetic prices or a single selected-plan run.

The executable audit and artifact contract are in
[`tools/planning-validation`](../../tools/planning-validation/README.md). No real
measurement pass is implied by its synthetic unit-test success.
