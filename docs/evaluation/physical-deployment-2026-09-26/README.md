# Shared physical deployment validation — 2026-09-26

The independent Level 1 branch and all seven live Level 2 suites pass. Strict
Level 3 performance acceptance has not passed. No thresholds or expected
results have been relaxed.

## Ownership and dependency order

Planner #462 supplies physical candidate compilation, scoped candidate selection,
operators and DAG execution. Backend deployment compilation binds sources and
materializations; the data plane invokes shared implementations. The old
recursive SummaryNode executor and unused adapters have been removed.

The current stack is main → #768 → #737 → #749 → #771 → #774 → #763 → #765 →
#761 → #728 → #742 → #759. #756 and #766 independently follow #765. #774
replaces #770, which GitHub marked merged when its head became an ancestor of
its former base during restacking; this did not merge the stack into main.

## Validation

| Area | Result |
| --- | --- |
| Planner physical package | 254 unit tests plus integration/documentation tests; strict all-target Clippy passed |
| Planner mapping | 448 unit tests plus integration tests; strict all-target Clippy passed |
| Backend foundation #774 | 429 control-plane unit tests passed on its own head |
| Backend data-plane #765 | 891 unit tests and strict all-target Clippy passed |
| Diagnostics #756 | 429 control-plane unit tests passed on its own head |
| Runtime controls #766 | 892 data-plane unit tests, strict all-target Clippy and its CI passed |
| Backend scoped costing #761 | 447 control-plane unit tests passed on its own head |
| Level 1 #728 | Both independent-branch planning tests passed |
| Level 2 #742 | Seven suites, 161 queries, 759 local responses, zero fallback |
| Level 3 runner #759 | 12 contract and four HTTP tests passed |
| Level 3 performance | Not passed; see retained measurements below |

[Level 2 report card](level2-summary.md) records the seven live suites. That run
used backend `b098629c` and Planner
`01dd2829123486daa3f5cf5e0c7bf9b0875766f7`. The run includes borrowed DAG inputs; all seven suites pass again.

## Level 3 evidence

[CI run 36221301755](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36221301755)
used backend `8c0fbeab489a23d39f81de93b2cc24dee7a61987` and Planner `01dd2829`.
[Complete measurements](benefit-ci.json) retain all trials and failures. CPU and
real cgroup peak-memory comparisons passed against all three baselines. Six
query p95 comparisons against VictoriaMetrics failed; overall acceptance failed.

The sequential, single-inflight benefit deployment explicitly sets
`TOKIO_WORKER_THREADS=1`. This is deployment configuration, not a query-result
cache or a change to acceptance. This run includes borrowed tracked inputs.
Its strict p95 comparison still failed. The local host lacks `memory.peak`, so
CI supplies the authoritative peak-memory evidence. This workload does not
establish performance at other cardinalities or concurrency.

A subsequent deployment change reuses immutable readout-boundary plan metadata
and checks QueryPlan identity to prevent stale bindings. Data, coverage,
revision and readiness remain per-run checks. Its 891 unit tests, strict
Clippy and all 18 process tests on #765 pass. Its ten-trial Level 3 CI failed five VictoriaMetrics p95 comparisons.
Heap process fixtures declare their supported family explicitly; native TopK
strictly checks original PromQL labels and values. The stack-top process run with this metadata change passed all 19 tests;
its 12 runner contracts and four HTTP tests also passed.

The latest shared kernel is Planner `41fe4fe970e38ca75726f3a16e62e8855613f0d2`.
Canonical exact panes merge into run-local scratch state and counter pairs avoid
a temporary Vec. The complete physical-package tests and strict Clippy pass.
An independent 12-pane reset-sensitive probe preserves results and reduces
readout allocations from 12 to one; its debug-kernel timing is not an
end-to-end performance claim. Backend foundation #774 passes all 429 unit tests
with this pin.

The default benefit run now measures 100 trials per query. With the unchanged
nearest-rank formula, ten-trial p95 equals the maximum; 100 trials use the 95th
sorted observation. All raw samples remain in the report. CPU, peak memory
and every query p95 must still be strictly lower than every exact baseline.
The latest runner passes 12 contract tests, four HTTP tests and strict Clippy.
[100-trial CI run 36223795295](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36223795295)
used backend `746a26e200bde4f800cf82e27c58d633bb8fa0f8` and Planner `41fe4fe9`.
[Complete 100-trial measurements](benefit-ci-100-trials.json) retain every sample.
Overall acceptance failed. CPU and peak memory passed against all baselines:

| Target | CPU usec | Peak bytes |
| --- | ---: | ---: |
| Backend | 204539 | 45842432 |
| Prometheus | 664763 | 109088768 |
| VictoriaMetrics | 328547 | 63328256 |
| ClickHouse | 7577650 | 437297152 |

Five VictoriaMetrics p95 comparisons failed:

| Query | Backend p95 ms | VictoriaMetrics p95 ms |
| --- | ---: | ---: |
| spatial-topk | 0.541 | 0.527 |
| temporal-quantile | 0.687 | 0.554 |
| grouped-rate | 0.562 | 0.510 |
| grouped-temporal-sum | 0.527 | 0.504 |
| topk-rate | 0.579 | 0.540 |

Earlier ten-trial artifacts remain historical evidence. The scratch-allocation
probe and increased sampling do not establish end-to-end performance acceptance.
Level 1 passes on current #728 head `7a59e78c`; Level 2 passes on current #742
head `85fa2c4a` ([CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36223594024)).
The #766 runtime inspection CI passes on `bc532c54`
([CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36223592865)).

## Remaining integration scope

Planner can compile and execute precompute Rate and Rate→Sum candidates with
window-aware contracts. General scalar/result-output persistence and binding
are not established by this backend integration. Unsupported deployment
frontiers must remain ineligible before pricing. Passing supported data-plane
queries is not proof of support for every Planner candidate.

## Reproduce

Run each command on its corresponding PR branch:

```sh
cargo test --locked -p control_plane --test issue754_level1
cargo test --locked -p promql-compliance
```

From `promql-compliance/runner`, with Docker Compose:

```sh
make run-all REPORT_DIR=/tmp/level2 LOGS_DIR=/tmp/level2-logs
make benefit REPORT_DIR=/tmp/level3 LOGS_DIR=/tmp/level3-logs
```

Use separate Cargo targets for branches with different workspace APIs. Both
live harnesses retain failures and exit nonzero when acceptance fails.
