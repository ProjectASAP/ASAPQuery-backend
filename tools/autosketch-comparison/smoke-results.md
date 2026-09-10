# First fixed-CMS smoke evaluation

## Release baseline after Bloom integration (`d62a37b`)

These runs use the query-semantics filter, a 4 KiB per-sketch payload budget,
10,000 events, cardinality 1,000, ten independent calibration/held-out seed
pairs, and an error target of 0.01.

| Workload | AutoSketch / ERP / oracle selection | Held-out pass | Mean payload |
| --- | --- | ---: | ---: |
| Frequency, uniform | Count Sketch, all 10 runs | 7/10 | 972.8 B |
| Frequency, Zipf(1.2) | CMS, all 10 runs | 8/10 | 3123.2 B |
| Membership, uniform | Bloom 2048 × 4, all 10 runs | 10/10 | 1024 B |

For every run, adapted AutoSketch, same-table ERP, and the grid oracle selected
the same configuration. This is a selection-contract check, not a superiority
result. The frequency held-out failures show that one calibration window is not
enough evidence for a robust accuracy claim and motivate multi-window
calibration plus Hybrid fallback.

The production repeated-dashboard gate initially exposed two runtime defects:
worker policy lookup confused materialization IDs with policy fingerprints, and
right-closed panes were anchored independently to each group's first sample
instead of the global PromQL time grid. After fixing both, the single-pane and
shared multi-pane process E2E tests pass.

## Follow-up: hard budget and family selection

Revision `3a74574118d4bd75b4c6ec174318bcc21b492675` adds a counter-payload memory
cap and CMS/Count Sketch selection. These are also debug smoke runs, not a
performance comparison. All three methods share the filtered candidate grid.

| Input / candidates | Budget | Runs | Selected family (all methods) | Held-out passes (each method) |
| --- | --- | --- | --- | --- |
| Uniform / CMS + Count Sketch | 4096 B | 10 | Count Sketch, 10/10 | 7/10 |
| Zipf(1.2) / CMS + Count Sketch | 4096 B | 10 | CMS, 10/10 | 8/10 |
| Uniform / CMS + Count Sketch | 511 B | 1 | None; no candidate measured | N/A |
| Zipf(1.2) / Count Sketch only | 4096 B | 2 | None; calibration accuracy infeasible | N/A |

The 4-KiB runs choose the same configuration as the grid oracle in every run.
Uniform's mean selected counter payload is 972.8 B; Zipf's is 3123.2 B. Worst
held-out errors are 0.0104 and 0.0108 respectively, exceeding epsilon 0.01 in
the failed runs. These failures remain visible: broader candidates and a memory
cap do not fix calibration generalization.

Local artifacts: `/tmp/asap-family-budget4096.json`,
`/tmp/asap-family-zipf-budget4096.json`, `/tmp/asap-family-budget511.json`, and
`/tmp/asap-cs-only-budget4096.json`. Reproduce with the README commands (without
`--release`) and the indicated budget/family/run settings. Ten Rust tests and
two Python tests pass, including budget boundaries, family choice in either
direction, Count Sketch hash limits and exact single-key checks.

## Original CMS-only run

Runner revision: `a0680e3d21cf2fc72717d0a6982b71b939107b5a`.
These are debug-build correctness/exploration runs, not performance results or
a non-inferiority claim. The whole Planner, query frontend, window execution,
and recurring-query system are not exercised.

Each distribution uses 10 paired runs, 10,000 events/window, 1,000 declared keys,
epsilon 0.01, and the same 56-point grid. Calibration seeds are 42, 44, ..., 60;
held-out seeds are 43, 45, ..., 61. CMS hash behavior is fixed. Each ERP row has
one calibration accuracy observation, not a confidence guarantee.

| Distribution | Calibration feasible (each method) | Held-out passes (each method) | Mean selected counter bytes | Worst held-out error | Mean adapted-search table evaluations |
| --- | --- | --- | --- | --- | --- |
| Uniform | 10/10 | 8/10 | 3072 | 0.0118 | 45.5/56 |
| Zipf, exponent 1.2 | 10/10 | 8/10 | 3123.2 | 0.0108 | 43.9/56 |

AutoSketch-Adapted, ASAPPlanner ERP selector, and grid oracle selected identical
configurations in all 20 runs. ERP matching the oracle is expected because both
select over the complete common table. This does not measure savings from
avoiding calibration, nor demonstrate superiority of either search method.

Both distributions have two held-out accuracy failures for every method. Keep
them in the evaluation. A feasible calibration point is not automatically a
robust deployment configuration. Next steps need a preregistered multi-window
calibration policy and fresh final-test seeds; do not tune against these test
answers and continue calling them held out.

## Reproduction and artifacts

Use the README commands without `--release`, at the runner revision above.
The local raw artifacts are `/tmp/asap-cms-uniform-first.json` and
`/tmp/asap-cms-zipf-first.json`. They include all per-configuration measurements,
search visits, profiles, selected states, outcomes, arguments, and Cargo.lock.
These are local artifacts, not durable hosted downloads. Re-execution should
reproduce deterministic choices/errors with the pinned dependencies; timings
and therefore complete file hashes will differ.

Original artifact SHA-256:

- Uniform: `0cdf4c39b730b6a35c1339770498b16093a93e9b67b4281a8e8dc34734e3536e`
- Zipf: `4689f795b3140af0c6bea59192ad24d83b9921e6182305fd0c6c8142c621497a`

Validation: 7 Rust runner tests and 2 Python summarizer tests passed. Test cases
cover LHS coordinates, search bounds/cache, all/no-feasible outcomes, deterministic
data, actual CMS/exact parity for a single key, ERP/oracle equivalence, descriptor
mismatch, and preserving failed/infeasible outcomes in summaries.
