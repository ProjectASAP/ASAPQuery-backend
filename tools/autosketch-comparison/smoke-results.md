# First fixed-CMS smoke evaluation

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
