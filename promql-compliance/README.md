# PromQL compliance suite

The Rust `promql-compliance` workspace crate sends one deterministic Remote Write fixture to Prometheus and ASAPQuery-backend, then compares their Prometheus API responses. It also derives the backend planning snapshot from the same suite queries.

Run from `promql-compliance/runner`:

```bash
make run-all
```

The first Docker build can take several minutes. Services and volumes are removed after every case. Override artifact locations when needed:

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

The backend payload has `servedBy`:

- `asap_query` (or another ASAP backend source): answered locally.
- `prometheus_fallback`: forwarded to Prometheus and therefore fails the strict compliance check.

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

There is no separate hand-written PhysicalPlan. `BuildPlanningSnapshot` derives a backend-local planning snapshot from every suite query; the control-plane helper enumerates candidates and supplies deterministic unit-cost evidence before the data plane starts.

The suite query expression is the workload query. Dataset metric names must cover
its named sources. `runner/src/planning.rs` builds a typed backend planning input:
60-second demand for instant-only queries, range step for range queries and
explicit 1% epsilon/delta accuracy. Differential fixtures retain declared
compatibility defaults (100 samples/second and 1-second cadence); the benefit
fixture derives the actual population, cadence and rate from its dataset.

Both runners invoke the Rust control-plane compiler directly, with no external
workload quotes. They validate automatic costs and the selected local plan before
starting containers, then pass that same snapshot to backend startup. Planning
failures produce a JSON report. The selected plan and input snapshot are saved
beside successful planning reports. Docker Compose is needed only for live
services; fixture, protocol, cost and comparison tests run with Cargo:

```sh
cargo test --locked -p promql-compliance
cargo clippy --locked -p promql-compliance --all-targets -- -D warnings
```

There is no Go toolchain, runner, module or generated Go code in this harness.
