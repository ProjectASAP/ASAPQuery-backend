> Current milestone: #728 candidate structure → #742 synthetic ranking → #775
> selected-plan execution. Online ERP collection and runtime replanning are deferred.
> This suite establishes execution correctness, not measured cost optimality.

# PromQL compliance suite

The Rust `promql-compliance` workspace crate sends one deterministic Remote Write fixture to Prometheus and ASAPQuery-backend, then compares their Prometheus API responses. It also derives the backend planning snapshot from the same suite queries.

Run from `promql-compliance/runner`:

```bash
make run-all
```

The first BuildKit Docker build can take several minutes; subsequent builds reuse Cargo caches. Services and volumes are removed after every case. Override artifact locations when needed:

```bash
make run-all REPORT_DIR=/tmp/promql-reports LOGS_DIR=/tmp/promql-logs
```

## Read results

`REPORT_DIR` defaults to `/tmp/asapquery-backend-promql-reports` and contains:

- `summary.md`: report card for every dataset/suite case.
- `summary.json`: the same summary for tooling.
- one JSON file per case: every query comparison, raw Prometheus response, raw backend response, and range/instant parity evidence.

`LOGS_DIR` defaults to `/tmp/asapquery-backend-promql-logs` and contains `compose.log` per case.

`Overall: true` means every response matched and the backend response was served by ASAPQuery. A response served by `prometheus_fallback` does not pass: matching a forwarded response is not a differential backend result.

Inspect a concrete query:

```bash
jq '.queries[] | select(.name == "quantile-over-time-0.5") | .instant[0].responses' \
  /tmp/asapquery-backend-promql-reports/aggregations.json
```

The runner adds `servedBy` from execution provenance headers:

- `asap_query`: both `X-ASAP-Execution: warm` and `X-ASAP-Execution-Detail: asap` confirm local execution.
- `hybrid`: includes external exact work and fails strict local acceptance.
- `prometheus_fallback`: fallback or missing/invalid local evidence; it cannot pass strict acceptance.

## Add a query

Edit or add a YAML suite in `suites/`. Every query needs a stable name, a PromQL expression, and at least one instant offset or a range. Offsets are seconds after the fixture base time.

```yaml
name: my-suite
comparison_defaults:
  value_tolerance:
    relative: 0
    absolute: 0.000001
queries:
  - name: median-by-job
    expr: quantile_over_time(0.5, data[5m])
    instant_offsets_seconds: [300, 600]
    range:
      start_offset_seconds: 300
      end_offset_seconds: 600
      step_seconds: 60
```

Run one suite against a fixture:

```bash
make run DATASET=../datasets/aggregations.yaml SUITE=../suites/my-suite.yaml
```

## Change tolerance

Set a suite default under `comparison_defaults`, or override one query under `comparison`. Relative and absolute finite-value tolerances combine; labels and timestamps always match exactly.

```yaml
comparison:
  value_tolerance:
    relative: 0.01
    absolute: 0.000001
```

## Add a dataset

Add YAML under `datasets/`. Each series has a metric name, labels, and strictly increasing finite samples. Times are offsets in seconds from the run base time.

```yaml
name: my-data
series:
  - metric: data
    labels: {job: frontend, instance: i-1}
    samples:
      - {offset_seconds: 0, value: 10}
      - {offset_seconds: 60, value: 20}
```

Use a distinct label set for each series of a metric. Ensure query windows have enough samples at every chosen evaluation point.

## Planning workload and metric vocabulary

There is no separate hand-written Physical DAG. The runner derives a deployment planning input from the fixture and suite; Planner exposes physical candidates; Backend selects using explicit synthetic quotes and local execution feasibility.

The suite query expression is the workload query. Dataset metric names must cover
its named sources. `runner/src/planning.rs` builds a typed backend planning input:
a recurrence and phase covering the actual evaluation timestamps, with explicit
1% epsilon/delta accuracy. The replay declares the historical retention needed
between its latest input and earliest evaluation. Complete finite fixture bounds
supply scoped operand-domain proofs; these are not inferred guarantees about live data. Differential fixtures derive total arrival rate from each series' replay span
and use the largest per-series sample gap as the declared cadence bound.
Benefit fixtures require uniform source cadence. Both paths retain the actual
series count and sample volume; no synthetic one-second cadence is supplied.

The differential runner invokes the Rust control-plane compiler directly. It quotes
every component at a deterministic unit price of 1.0 under
`synthetic-execution-fixture-v1`; non-local candidates are infeasible for this
fixture. These fake prices select a plan through the production quote path, without
ERP measurements or runtime feedback. The runner validates the synthetic cost
report and local execution before installing that exact deployment plan. Backend
startup does not repeat candidate selection. Planning failures produce a JSON
report. The input snapshot, selected plan, and typed `*.install.json` publication
are saved beside successful planning reports. Docker Compose is needed only for live
services; fixture, protocol, cost and comparison tests run with Cargo:

```sh
cargo test --locked -p promql-compliance
cargo clippy --locked -p promql-compliance --all-targets -- -D warnings
```

There is no Go toolchain, runner, module or generated Go code in this harness.

## Deferred three-baseline benefit experiment

Run `make benefit` from `promql-compliance/runner`. It validates all ten shared
queries before measuring Prometheus, VictoriaMetrics, ClickHouse and the backend.
The gate requires lower CPU, peak memory and per-query p95 than each baseline.
A missing cgroup `memory.peak` counter is recorded as null and fails the gate,
while preserving the available CPU and latency measurements.

For the temporal quantile baseline, VictoriaMetrics uses explicit
`label_del(..., "__name__")` to match PromQL output labels. The report records
the actual expression sent to every target; strict label comparison is retained.

See the [2026-09-25 validation report](../docs/evaluation/physical-deployment-2026-09-25/README.md)
for passing correctness results and unresolved acceptance failures.
### Candidate execution matrix

Run `make candidates` in `promql-compliance/runner` to test each issue-754 query,
the shared-rate and shared-quantile ensembles, and the complete ten-query workload.
For every admitted executable candidate, the harness changes only synthetic quotes
to make that candidate cheapest, asserts that production selection chose it,
compiles and installs its deployment plan, ingests the fixture once, and compares
every query in that workload with Prometheus. A failed candidate fails the matrix.

The report retains candidate admissions, quoted snapshots, selected plans and typed
installations. Coverage means all candidates exposed and admitted for this fixture;
it does not claim an exhaustive Cartesian product of all possible DAGs. Candidates
rejected by accuracy admission or deployment binding remain recorded as rejected.
Online ERP measurements and runtime replanning are outside this test.
