# Shared physical deployment validation — 2026-09-25

Audience: developers reviewing Planner #462 and the backend dependency stack.

The ten-query planning and live correctness gates pass on the complete stack.
The broader differential suite and the strict performance gate do **not** pass.
This report does not establish production readiness or a general speedup.

## Revisions and scope

- ASAPPlanner: `b58f24268270a4dd7b7caaf0076ea3db842f3e9b` (PR #462).
- Backend runtime: #759 stack head `c7b50733`; runner measurement fixes: `d9e89a98`.
- Live runs used the equivalent runtime in `integration/physical-deployment`,
  with runner fixes committed as `84da304f`.
- Dependency order remains main → #768 → #737 → #749 → #771 → #728 → #770 →
  #763 → #765 → #761 → #742 → #759. #756 and #766 remain independent on #765.
- #728 precedes required implementation in that order. Its standalone head is
  still red; the passing Level 1 result below is for the complete stack.

## Results

| Level | Result | Evidence |
| --- | --- | --- |
| 1: selected local plans | Pass | `issue754_level1`, all ten query shapes, automatic costs and local-plan assertions |
| 2: shared ten-query live suite | Pass | 20 instant comparisons and 10 ranges, instant/range parity, strict local provenance |
| 2: broader differential sweep | 4/7 pass | [Report card](level2-summary.md) |
| 3: baseline correctness | Pass | All ten queries validated against Prometheus, VictoriaMetrics and exact ClickHouse SQL before measurement |
| 3: performance benefit | Fail | [Measurements and failures](benefit.json) |

Restacked verification also passed 444 control-plane unit tests, 926 data-plane
unit tests, 116 shared-type tests, and all 19 compatibility process tests. The
runner passes ten contract tests and four HTTP tests. Strict Clippy for the
control plane, data plane and runner, and formatting checks passed.

The three broader Level 2 failures are retained:

- Both aggregation datasets select an external exact subquery for
  `topk by (job) (1, count_over_time(data[5m]))`; strict local planning rejects it.
- `issue-702` with 60-second samples fails historical spatial reads. Its
  compatibility snapshot declares a one-second ingestion interval. Six spatial
  cases fail; the three temporal/composed cases pass. The one-second fixture
  passes. Cadence and selector-lookback semantics need further investigation;
  changing the expected results or accepting fallback would hide the gap.

## Level 3 measurements

The release backend and pinned baseline containers received the same finite
fixture. Each target used three warmups and ten trials per query. These are one
small-workload run on a shared host, not a cardinality/range scaling study.

| Target | CPU during measured queries (µs) | Peak memory |
| --- | ---: | --- |
| backend | 131484 | unavailable |
| prometheus | 380549 | unavailable |
| victoria | 191792 | unavailable |
| clickhouse | 3151873 | unavailable |

Backend p95 was lower than Prometheus and ClickHouse for all ten queries, but
higher than VictoriaMetrics for nine. The report retains all latency samples;
with ten trials, nearest-rank p95 is the largest sample. Peak memory is null
because this host's cgroup-v2 kernel does not expose `memory.peak`. Missing peak
memory remains a gate failure; current usage is not substituted for it. CPU
covers the measured query interval, while the required cgroup peak would cover
the container lifetime. These measurements do not isolate total maintenance cost.

VictoriaMetrics preserves the metric name for `quantile_over_time`. Its baseline
expression explicitly applies `label_del(..., "__name__")` to match PromQL;
labels are still compared strictly. Validation and measurement use the same
expression, recorded in the JSON. See the [MetricsQL documentation](https://docs.victoriametrics.com/MetricsQL.html).

The harness waits for baseline query visibility after Remote Write with a strict
comparison and timeout. It does not infer query visibility from write admission.

## Reproduce

From the backend repository root:

```sh
cargo test --locked -p control_plane --test issue754_level1
cargo test --locked -p data_plane --test asapquery_compatibility_process_e2e -- --test-threads=1
cargo test --locked -p promql-compliance
```

From `promql-compliance/runner`, with Docker Compose and BuildKit available:

```sh
make run-all REPORT_DIR=/tmp/physical-level2 LOGS_DIR=/tmp/physical-level2-logs
make benefit REPORT_DIR=/tmp/physical-level3 LOGS_DIR=/tmp/physical-level3-logs
```

Both commands retain failed evidence and exit nonzero when acceptance fails.
The full reports, selected plans, snapshots and container logs from this run
remain locally under `/tmp/physical-level2-final*` and `/tmp/physical-level3-final*`.
No thresholds, fallback rules or expected results were relaxed to obtain a pass.
