# Recorded smoke results

Run date: 2026-09-13.

- Fixture SHA-256: `7df76b148e7b1d4c11260f23679d5138d1bb6c14ededf9ff8d1d9415f8146eeb`.
- Reference: official Prometheus/promtool 3.5.0 (`8be3a9560fbdd18a94dedec4b747c35178177202`).
- Backend PR base: `8cf1890b` on `origin/main`.
- Planner dependency: published commit `f27b16a747e5d7fcd70a5510075c0cd062f0dcea`
  (the fix from [PR #413](https://github.com/ProjectASAP/ASAPPlanner/pull/413)
  applied to the existing backend planner pin); no local path patch.

| Check | Result |
| --- | --- |
| Official promtool | 84 stable + 21 experimental cases passed |
| Official Prometheus HTTP against isolated fixture TSDB | 105/105 passed |
| Canonical tree → exact kernel binding | 105/105 `BOUND_EXACT` |
| Backend native exact execution against official-verified expectations | 105/105 passed |
| Local native backend HTTP `/api/v1/query` | 105/105 passed; every response identifies `asap_exact` |
| Local native HTTP range consistency | 105/105 range queries match per-step instant results |
| Native executor regression tests | 5/5 passed, including the 105-case corpus |
| Planner frontend regression/conformance/lowering/equivalence tests | 162/162 passed |
| Python comparator/coverage tests | 8/8 passed |
| Backend query parser regression tests | 7/7 passed |
| Targeted Clippy (`-D warnings`) and Cargo format check | Passed |

All 14 aggregation operators and 25 range-vector functions in the pinned catalog
have an executable exact binding. This measures float-sample function coverage,
not full PromQL conformance. Native histograms, mixed types, staleness, offsets,
`@`, subqueries, and explicit vector matching are outside this exact smoke path;
unsupported query shapes are rejected rather than silently stripped.

The exact plan consumes the real ASAPPlanner canonical tree. Execution uses native
backend kernels and timestamped raw samples; it does not forward queries to
Prometheus or read expected results from the fixture. The HTTP check uses the
`promql_exact_smoke` example, not the production ingestion/storage/router path.
Production use still needs a raw-sample source and routing integration.

## Semantic behavior exercised

- Existing upstream `irate` retains a distinct canonical intent from `rate`.
- Existing upstream `count` counts series, including equal-valued series, instead of distinct numbers.
- Quantile phi outside [0,1] and NaN survives lowering and produces the defined
  `-Inf`, `+Inf`, or `NaN` result in the smoke cases.
- Negative `limit_ratio` uses the upper hash interval; `-1` keeps every series.

The original summary/sketch binder diagnostics remain in `summary_status` in the
planner JSON report. Exact bindings do not imply these functions can execute from
existing sketches alone.

Original smoke logs are under `/tmp/asap-promql-smoke/`, including
`reference-results.json`, `planner-results.json`, `backend-results.json`,
`native-exact-results.log`, `planner-regressions-after.log`,
`planner-types-mapping.log`, and `http-reference/reference-http-results.json`.
These temporary artifacts may be removed; reproduce the checks with [README.md](README.md).

PR-branch reruns of promtool, canonical binding, native execution, native HTTP
instant/range checks, planner frontend tests, and Python tests are recorded under
`/tmp/promql-pr-smoke/` and `/tmp/promql-pr-*.log`. The official HTTP reference
check was recorded with the identical fixture before the rebase.
