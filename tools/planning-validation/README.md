> Deferred follow-up. This audit is not required by the current synthetic-cost
> selection and execution milestone (#728 → #742 → #775).

# Level 3: measured selection quality

This is an offline audit tool for #759. It does not collect telemetry, run a
production workload, or authenticate who produced measurements. Its unit tests
use synthetic records to test rejection behavior; no real-evidence pass has been
recorded for the current physical-candidate stack.

The accepted experiment uses a real trace replay with original ingestion/query
timing and series scope. Run each query independently and then run the full
concurrent workload. Freeze applicable ERP and workload observations before the
evaluation interval. Under explicit accuracy, p95 latency and peak-memory limits,
compare total process CPU for ingestion, maintenance and queries over the same
horizon. Do not sum overlapping timers or count shared maintenance per consumer.

## Run

```sh
python3 tools/planning-validation/selection_quality.py experiment.json --output selection-quality.json
```

Exit 0 requires complete evidence, accurate results for every measured admitted
candidate, a selected plan within limits, and no positive CPU regret relative to
the measured best feasible candidate. Reports use medians of at least three runs
and include observed CPU ranges. Ranges are not statistical confidence intervals;
this strict gate does not silently introduce a noise tolerance or prove a global
production optimum.

## Experiment contract

All paths are relative to the experiment directory. Artifact references contain
`path` and `sha256`; hashes are checked before reading. Keep the full raw files.

- `schema_version: 1`, `workload_kind: real_trace`, `scope: single_query` or
  `full_workload`, `objective: total_cpu_seconds`.
- `trace`: reference to the actual trace. `horizon_seconds`,
  `evaluation_interval_ms: [start, end]`, and `calibration_end_ms` fix the scope.
- `environment`: machine/build identity, including Backend and Planner revisions.
- `limits`: `p95_latency_ms` and `peak_memory_bytes`, chosen before measurement.
- `evidence.statistics`, `.accuracy`, `.resources`: artifact references plus
  `origin: measured`, `workload_sha256`, `observed_at_ms`, `valid_for_ms`.
  Each evidence artifact is JSON. Statistics identifies `data_snapshot_id`;
  resources identifies the same generation plus `model_version`, `objective`,
  `backend_revision` and `planner_revision`. The gate checks these against the
  actual cost-selection report, so unrelated measurements cannot validate it.
  Domain-specific interpretation and applicability remain with the statistics,
  ERP and accuracy adapters. Merely adding these fields cannot turn a fixture
  into real evidence; reviewers must inspect the referenced raw observations.
- `plan`: reference to the actual compiled plan with `cost_comparison`.
  Every admitted `selected`/`unselected` candidate must have measurements;
  other candidates must retain their rejection reason.
- `candidates`: entries with `candidate_id` (physical candidate identity when
  available) and a `measurements` artifact reference. The artifact repeats
  candidate identity, environment, workload digest and horizon, and contains
  at least three `trials`. Each trial provides `total_cpu_seconds`,
  `p95_latency_ms`, `peak_memory_bytes`, and a `correctness` artifact reference.
- Each correctness artifact identifies the candidate and workload digest and
  records boolean `passed`, backed by the retained exact-reference comparison.

The unit test constructs a complete contract example in a temporary directory;
it is deliberately not a checked-in production example or measurement.

## Current evidence boundary

Seven finite-fixture differential runs passed and are preserved in
[the execution report](../../docs/evaluation/execution-2026-09-28/README.md).
Those validate execution only. Historical benefit-runner reports compare engines
and are also distinct from this candidate-selection audit. Production telemetry
collection, applicable real ERP coverage for all compared candidates, and a
matched repeated-run real-trace experiment are required before declaring Level 3
verified. A real single-query pass does not establish full-workload selection.
