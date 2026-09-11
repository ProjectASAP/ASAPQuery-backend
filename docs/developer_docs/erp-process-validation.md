# ERP-selected KLL process validation

The process test in [erp_planning_process.rs](../../data_plane/tests/support/erp_planning_process.rs)
checks observation → Planner selection → physical installation → raw ingestion →
ASAP query execution. This is a correctness fixture, not an o11ybench performance
result or a claim that empirical rank error is a probabilistic bound.

The fixture measures the runtime KLL implementation at `k=32` and `k=128` using
10 seeds and 99 quantiles. It records maximum observed tie-aware rank error and
serialized retained state size. The observer fits a held-out stream; Planner
selects parameters from the available measured profiles. The first run offers
both profiles; the second offers only the larger profile. No preselected physical
plan is supplied to the data-plane process.

Both processes compile the startup planning snapshot and accept remote-write
samples for two source series. Their value domains differ by 1000, making an
accidental pooled result detectable. A health-only fallback server has no query
handler. The test requires two labeled warm ASAP results, each within the declared
rank-error target of 0.2.

One verified run produced:

| Available profiles | Selected k | Materialization ID | Returned values (a, b) |
| --- | ---: | ---: | --- |
| 32, 128 | 32 | 17085259479989488410 | 12, 1012 |
| 128 | 128 | 81115169662305743 | 12, 1012 |

The measured fixture errors were 0.03336 and 0.00496, respectively. The objective
uses serialized-state byte-seconds; CPU terms are disabled because this fixture
does not benchmark CPU. These measurements are test evidence, not a production
ERP artifact distributed for unrelated workloads. The observer describes ranked
key frequencies, not arbitrary numeric value spacing.

Run:

```sh
cargo test -p data_plane --test asapquery_compatibility_process_e2e erp_planning_process -- --nocapture
```

`ERP_PLANNED` records candidate profiles, fitted observation, selected parameters,
estimated costs, installed identity, and QueryPlan. `ERP_WARM` records the actual
HTTP result and execution path. The evaluation workspace retains the captured
run at `/mydata/erp-production-study/process-evidence.json`; this host-local path
is not a repository fixture.

Separate compiler and adapter tests cover ERP miss → theoretical sizing → exact
fallback. This process test establishes two ERP hits; it does not claim a live
online feedback service, general sketch-family accuracy calibration, or latency,
CPU, and memory improvements over Prometheus.
