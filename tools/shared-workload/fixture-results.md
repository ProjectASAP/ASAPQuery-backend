# Small-fixture translation checks

These are translation checks, not ASAP acceleration measurements. No backend
plan or materialization was preselected or measured by this tool.

The local dataset root is `/mydata/shared-workload-study`. Two deterministic files
were loaded into each engine: `fixture-v2-100ms-10g-4m` and
`reset-fixture-100ms-10g-4m`. Each contains 24,040 samples, 10 groups, four members
per group, and one minute of 100ms observations. The second starts 200 seconds
after the first and resets each counter every 97 steps. Together they contain
48,080 samples and exercise a gap in history. Their manifests record each file's
size and SHA256. Larger-window queries see partial history in this fixture.

The engines were ClickHouse 26.8.2.7, Prometheus 3.14.0, and VictoriaMetrics 1.126.0.
The ClickHouse schema is documented in [README.md](README.md). All three engines
received the same rows; Prometheus used OpenMetrics-created TSDB blocks and
VictoriaMetrics used the text import API. The Prometheus and VictoriaMetrics
processes were isolated fixture containers; ClickHouse used a separate fixture
database. Resource limits were not matched because this was a correctness check.

| Check | ClickHouse vs Prometheus | Native VictoriaMetrics vs Prometheus | VM Prometheus-named variant |
|---|---:|---:|---:|
| 280 query/window/filter cases, reset fixture endpoint | 280 pass | 170 pass | 220 pass |
| 20 rate/increase cases, 150ms after last sample | 20 pass | 0 pass | 0 pass |
| 10 spatial cases at the exact 5m lookback boundary | 10 pass | 10 pass | 10 pass |
| 10 spatial cases 1ms past lookback | 10 pass | 10 pass | 10 pass |

Values use 1e-9 relative / 1e-12 absolute tolerance. Ten temporal-count TopK
cases have tied cutoff scores; those pass cutoff admissibility against the
full Prometheus population rather than requiring the same arbitrary members.
Metric names remain part of ordinary label comparison. Native VictoriaMetrics
differences include retained metric names and counter extrapolation; even its
Prometheus-named functions did not establish parity in this version.

Reports are in `final-reset-manifest/differential.json`,
`final-reset-manifest/end-gap.json`,
`final-boundary-manifest/lookback-boundary.json`, and
`final-reset-manifest/lookback-expired.json`. Reports list both loaded dataset
manifests. The earlier `fixture-v3-100ms-10g-4m/differential.json` used Prometheus
2.55.1 and recorded 20 differences due to its inclusive temporal left endpoint;
it is retained as a version-mismatch diagnostic. The final SQL targets the tested
Prometheus 3.14 left-exclusive temporal and instant-lookback behavior.

Remaining acceptance work includes zero/constant and irregular missing-sample
fixtures, every requested composition, full repeated execution schedules,
Planner candidate/selection capture, backend warm/hybrid/fallback classification,
and scale/performance runs. These checks do not establish those claims.
