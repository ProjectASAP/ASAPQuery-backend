# Small planned PromQL chain: real execution, not benefits acceptance

On 2026-09-12 the new `planned_run.py` completed one backend-local experiment:
`sum_over_time(fake_metric[1m])`, 10 groups × 4 members, two metrics, 100ms samples,
96,080 total samples / 80 series over two minutes. No ASAPCollector service ran.
Evaluation times were 1700000060000 and 1700000120000 (milliseconds), with complete
1m history for each. Every measured pair executed ASAP first, then Prometheus;
client requests and subprocess execution had no deadlines.

## Planning and materialization evidence

The normal candidate exporter produced summary and whole-query fallback candidates.
`calibrate_runtime.py` measured both in isolated runs, `update_global_profile.py`
updated the shared implementation profile, candidates were re-exported/re-measured,
and `calibrate.py` supplied complete measured cost quotes. The production compiler
selected plan **1212003311198828710**, with no candidate override or hand-edited
install artifact. Its projected workload was 1,000 repetitions; the held-out chain
check below used **two** evaluations and is not a validation of that cost horizon.
The exact alternative is backend-forwarded execution, not standalone native DB.

Plan activation, matched ingestion and finite-input drain succeeded. Both post-drain
readiness probes and both measured ASAP requests reported:

- execution: warm / detail: asap;
- summary readouts: 1 per request;
- raw scans: 0;
- exact-subquery RPCs: 0.

Both matched comparisons passed rtol=1e-9 / atol=1e-12, with no missing or extra
series/samples. Maximum relative value error was approximately 3.97e-16; this is
floating-point-tolerant equality, not bitwise identity or a sketch rank guarantee.

## Observed query timing (only two samples)

| Endpoint | First | Second | Mean |
|---|---:|---:|---:|
| ASAP | 20.509ms | 26.854ms | 23.682ms |
| Direct Prometheus | 9.067ms | 9.757ms | 9.412ms |

ASAP was **slower** in this small debug-build run. The native/ASAP query-latency
ratio was about 0.397, not a speedup. Readiness probes warmed ASAP first; native
and fallback used separate empty TSDBs but shared the host and CPU set (0,1).
Two observations, debug binaries and shared-host conditions cannot establish a
performance distribution or extrapolate benefit at scale. Full lifecycle costs
remain unaligned; `eligible_for_full_system_benefit` is false.

## Reproduction and retained artifacts

Generate with `dataset.py --dataset synthetic --groups 10 --members 4
--duration-ms 120000` (default start). Follow the calibration guide and the
`planned_run.py` command in [ACCURACY_E2E.md](ACCURACY_E2E.md).
The local evidence root is `/tmp/pr689-live.pZ4pd1`:

- `discovery-{candidates.json,measurements/}` and `profile-{candidates.json,measurements/}`:
  candidate manifests, raw calibration responses and phase measurements;
- `costed.json`: compiler input with measured quotes;
- `acceptance/run/trial-1/replay/`: planning, install/status, ingestion, drain,
  summary readiness, paired queries, phase resources, storage and process lifecycle;
- `acceptance/acceptance.json`: chain decision and limitations.

Input OpenMetrics SHA256:
`50d5b3d7dd7897a6232e1012692516909769aae066a9e5f1a8a0b161a8913054`.
Costed snapshot SHA256:
`46c1e797e6b2b2f57e0d47f96e4be82dd68970042fe15fabcf4b666dd735d838`.

Binaries: Prometheus **3.5.0**; backend Rust **1.98.0 debug**, Rust sources from
PR head `2d16f7fb` (subsequent harness-only edits); Planner
`3be523fa0f06a905188e42cbe482d06aa843ba5d`; clean precompute dependency source
`ASAPCollector@9b996305da9f8a2d840cb42714b50d05238a0500`; sketch library at CI's
`8c03d7c68b7150710c79e70a6421971275614561`. The existing dirty dependency trees
were not modified; a temporary dependency path override was reverted after build.
Backend SHA256: `092c24eba9935ae065f84eba28e7dd743b677e503f323ea75e8d80471230d370`.
Compiler SHA256: `0f1dd8817f75b0536697dcbc4a25bf311791a47a6c59b5beae36b814287e8cea`.

This PromQL run does not validate SQL's Prometheus-3.14 boundary translation, VM
semantics, large-scale benefit, or service-side limit removal. Services retained
their own defaults; no service-side timeout occurred, and all queries completed
naturally. Owned services were then terminated normally, with no forced kill.
