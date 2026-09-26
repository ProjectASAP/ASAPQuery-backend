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
| Backend data-plane #765 | 890 unit tests and strict all-target Clippy passed |
| Backend scoped costing #761 | 447 control-plane unit tests passed on its own head |
| Level 1 #728 | Both independent-branch planning tests passed |
| Level 2 #742 | Seven suites, 161 queries, 759 local responses, zero fallback |
| Level 3 runner #759 | 12 contract and four HTTP tests passed |
| Level 3 performance | Not passed; see retained measurements below |

[Level 2 report card](level2-summary.md) records the seven live suites. That run
used backend `6c27dd4e3fdc9ef58fedec60f73074f76ac25169` and Planner
`01dd2829123486daa3f5cf5e0c7bf9b0875766f7`. Subsequent constructor-only fixture
fixes preserve runtime behavior.

## Level 3 evidence

[CI run 36219954075](https://github.com/ProjectASAP/ASAPQuery-backend/actions/runs/36219954075)
used backend `d175a2b652a5bc6341b14e61338d5080335a953d` and Planner `01dd2829`.
[Complete measurements](benefit-ci.json) retain all trials and failures. CPU and
real cgroup peak-memory comparisons passed against all three baselines. Seven
query p95 comparisons against VictoriaMetrics failed; overall acceptance failed.

A subsequent configuration sets `TOKIO_WORKER_THREADS=1` for the sequential,
single-inflight benefit workload. This is a deployment configuration, not a
query-result cache or a change to acceptance. Its local run still failed several
p95 comparisons and lacked kernel `memory.peak`; its CI validation is pending.
This workload does not establish performance at other cardinalities or concurrency.

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
