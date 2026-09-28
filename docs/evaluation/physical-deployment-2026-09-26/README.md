# Shared physical deployment validation — 2026-09-26

The independent Level 1 branch and all seven live Level 2 suites pass. Strict
Level 3 performance acceptance passes the latest per-run range-parameter run
with final Planner pin `751e5e02`. Earlier failures are retained below. No thresholds or expected
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
| Planner physical package | 255 unit tests plus integration/documentation tests; strict all-target Clippy passed |
| Planner mapping | 448 unit tests plus integration tests; strict all-target Clippy passed |
| Backend foundation #774 | 429 control-plane unit tests passed on its own head |
| Backend data-plane #765 | 891 unit tests and strict all-target Clippy passed |
| Diagnostics #756 | 429 control-plane unit tests passed on its own head |
| Runtime controls #766 | 893 data-plane unit tests, strict all-target Clippy and its CI passed |
| Backend scoped costing #761 | 447 control-plane unit tests passed on its own head |
| Level 1 #728 | Both independent-branch planning tests passed |
| Level 2 #742 | Seven suites, 161 queries, 759 local responses, zero fallback |
| Level 3 runner #759 | 12 contract and four HTTP tests passed |
| Level 3 performance | Passed: CI 36227354805; all strict CPU, peak-memory and per-query p95 comparisons |

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

## Passing probe-parser measurement

[CI run 36225215058](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36225215058)
passes semantic/local-provenance checks and every strict CPU, peak-memory and
query p95 comparison, with three warmups and 100 measured trials per query.
The PR head is `1b8bf1d00323918f78728da0c25beb354c44f198`; the tested merge is
`e5a31f88621b379aa33ad624a2dde228a709c7cc`, with Planner `41fe4fe9`.
[Complete samples](benefit-ci-probe-fastpath.json) retain all measurements.

| Target | CPU usec | Peak bytes |
| --- | ---: | ---: |
| Backend | 267002 | 46448640 |
| Prometheus | 1239327 | 107585536 |
| VictoriaMetrics | 621122 | 71811072 |
| ClickHouse | 14088307 | 459235328 |

HTTP freshness-probe handling now rejects unrelated function tokens before
constructing the PromQL parser. Query results remain uncached; physical input,
revision, coverage and readiness checks still occur for every execution.

Final Planner pin `751e5e02c563d1928818971e072f73aea54e7946` additionally fixes
full re-snapshot population counting. Its regression failed before the fix
(six samples instead of four). All 255 shared physical unit tests, integrations,
docs, formatting and strict Clippy pass. With this pin, #774 passes 429 unit
tests, #728 passes both independent Level 1 tests, #765 passes 891 unit tests
and 18 process tests plus strict Clippy, #756 passes 891 unit tests plus strict
Clippy, and #766 passes 893 unit tests. Latest-pin Level 2 passes on independent #742 head `92614f0e`
([CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36225853469));
[final report card](level2-ci-final.md) and [summary data](level2-ci-final.json)
record seven suites, 161 queries, 759 local responses and zero fallback.
Stack-top Level 2 also passes on `de44e3bd`
([CI](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36225853662)).

[Finalized-pane CI run 36225853665](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36225853665)
failed strict performance acceptance on PR head `de44e3bd` / tested merge
`af64df2f579d7a49b4a96bdaa71fcd6973691449`.
[All samples](benefit-ci-finalized-panes.json) are retained. CPU (269441 µs) and
peak memory (46018560 bytes) pass all baselines, but temporal-sum p95 is
1.159219 ms versus VictoriaMetrics 1.114215 ms, and topk-rate p95 is 1.009188 ms
versus 0.99007 ms. The previous passing run does not make this run pass.
The subsequent per-run parameter change passes the strict measurement below.

## Per-run exact readout parameters

#765 now shares the resolved counter range parameters within each run; sum,
count, min and max readouts do not create unused range strings. Coverage is
scanned once per group. Full 891 unit tests, strict all-target Clippy and all
18 own-branch process tests pass at `ec102f4f`.

The independent [allocation probe](readout-range-allocation-probe.rs) exercises
the shared readout on reset-sensitive 12-pane states for eight groups. Results
are equal: Rate allocations fall from 49 to 14 and Sum from 49 to 9. This is
an allocation measurement, not an end-to-end latency claim. Build the probe
against the physical library rlib with rustc and its dependency directory.
The strict end-to-end run passes; see the latest measurement below.

## Latest strict measurement

[CI run 36227354805](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36227354805)
passes on PR head `35c0f44c58f9d57a684155fe1c93d04f91661f72`, tested merge
`f9f64ca664bc0e04577de8565b32512eb0297f20`, Planner `751e5e02`.
[Complete raw measurements](benefit-ci-range-parameters.json) retain all 100
trials per query and three warmups. Semantic and local-execution checks pass;
CPU, actual peak memory, and all ten query p95 values are strictly below each
exact baseline. No acceptance threshold was changed.

| Target | CPU usec | Peak bytes |
| --- | ---: | ---: |
| backend | 258115 | 46370816 |
| clickhouse | 13093800 | 423636992 |
| prometheus | 1194584 | 88354816 |
| victoria | 605754 | 63549440 |

Independent #742 head `e93df71a` passes [Level 2 CI 36227354017](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36227354017);
#759 also passes [Level 2 CI 36227354781](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36227354781).
This small-fixture result does not establish the full cardinality/time scaling matrix.
