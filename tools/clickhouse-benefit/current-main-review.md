# ClickHouse execution and benefit readiness

Audience: developers assessing evaluation readiness.

Backend baseline: `732b394abea16bca6f3b5c98c61333f81544a68a`.

The real mixed process test passed against ClickHouse 26.8.2.7 with the
ClickHouse endpoint explicitly configured. It compiled SQL through the control
plane, published and installed the resulting plan, backfilled SummaryStore,
then executed SummaryStore and external-exact branches through the production
listener. The test's post-backfill source mutation guard also passed. A repeat
against a dedicated ClickHouse container passed in 2.02 seconds: one test passed,
zero skipped. This validates mixed execution; it is not a performance result.

The 27-query SQL corpus was independently replanned on the same baseline.
Twenty-four queries still fail in SQL parsing/lowering. q05, q06, and q23 reach
Planner selection but fail publication because the metric population predicate
has no canonical catalog binding. No corpus query is publication-ready. The
new mixed execution implementation therefore does not establish warm coverage
for this corpus.

All 27 queries were also executed afresh through the production listener and
the real external ClickHouse backend. All 27 requested fallback, executed exact,
returned HTTP 200, and matched the direct exact response byte-for-byte. Thus
the current corpus result is zero warm, zero hybrid, 27 successful exact
fallbacks, and zero request failures. It demonstrates fallback correctness,
not acceleration. That run used the existing `default.raw_samples` evaluation
table with 408,012 rows and is separate from the scaled one-series runtime probe.

The existing q05 warm probe supplies a MinMax catalog and pane configuration
before asking Planner for a query DAG. Its two-row default also does not
represent o11y data. This PR makes the probe usable with one unchanged real
series and records that planning limitation explicitly. It verifies every
measured response and its execution provenance. Runtime results from this
probe must not be reported as automatic materialization selection or as
full-corpus acceleration.

Local evidence:

- `/mydata/clickhouse-o11y-main-results/latest-main-mixed-e2e.log`
- `/mydata/clickhouse-o11y-main-results/latest-main-planner-matrix.json`
- `/mydata/clickhouse-o11y-main-results/latest-main-runtime-matrix.json`
- `/mydata/clickhouse-o11y-main-results/fine-1s-24h-series.provenance.json`

The original scaled trace is
`/mydata/o11y-scale-study/datasets/fine-1s-24h.prom` (9,147,706 samples).
Its SHA-256 is
`6297663470b92bba012d6c63b072ae2e534c4caa9d6de5082b882fd163436769`.
The exact `service_cache_refresh_lag_seconds` user-service series contains
86,371 samples; extraction changes neither values nor timestamps. Its 1-second
sampling was produced by the existing trace scaler, not by native one-second
source capture. The source's generation metadata remains beside the trace.

Remaining acceptance gaps are canonical SQL population/producer binding,
automatic catalog construction from selected materialization candidates,
recorded candidate costs, and grouped/multi-series workload execution. Increasing
the scalar probe's input size does not resolve those gaps.

Per-process warm-query memory must not be interpreted as total deployment
memory savings: ClickHouse remains available as the historical/exact backend.
Likewise the summary's size is additional state when raw ClickHouse data is
retained. A whole-deployment cost comparison must include both processes and
the summary build/update work.

## Release runtime measurement

Three trials restarted a dedicated ClickHouse 26.8.2.7 process and rebuilt the
summary. Each then alternated 1,000 warm and 1,000 direct-exact requests after ten
warmups. Backend/test driver and ClickHouse ran in separate containers, each with
CPU cores 60–61, a two-CPU quota, and a 4 GiB memory limit. ClickHouse used two
threads and its query cache was disabled. Rust 1.98.0 produced the release build.
The ClickHouse image digest was
`sha256:fa394da808cc53f76d0344429421d6c422a6ee85fe7450135c0e3cff4df9bcbb`.

The table contained the 86,371 samples of **one** original series, not the entire
9,147,706-sample trace. The fixed 12-hour query covered 43,200 samples. Every
accelerated response was `warm` and every result equaled the direct exact value
`530`: 3,000 of 3,000 accelerated results matched. The source table used
MergeTree with timestamp ordering.

| Trial | ASAP median / p95 | ClickHouse median / p95 | Median speedup | ASAP query CPU | ClickHouse exact query CPU |
|---|---:|---:|---:|---:|---:|
| 1 | 0.851 / 0.965 ms | 6.892 / 15.056 ms | 8.10× | 770 ms | 8,530 ms |
| 2 | 0.848 / 0.955 ms | 7.449 / 15.657 ms | 8.78× | 760 ms | 9,640 ms |
| 3 | 0.850 / 0.955 ms | 7.019 / 15.255 ms | 8.26× | 780 ms | 8,560 ms |

CPU covers 1,000 queries per route, with 10 ms scheduler-tick precision.
ClickHouse also consumed 60–130 ms of background CPU during the warm requests.
The reciprocal mean latency was 1,169–1,192 requests/s for ASAP and 117–132
requests/s for ClickHouse; this is a serial service rate, not a concurrency test.

Backend query-phase RSS was 51.3–53.6 MiB; ClickHouse RSS was 751.2–777.7 MiB.
Lifecycle high-water marks were 51.3–53.6 MiB and 807.0–891.0 MiB respectively.
The backend output directory occupied 5,721 bytes, including persisted state
and metadata; the complete ClickHouse table occupied 507,146 bytes. Those
footprints cover different retention scopes: the summary is for 12 hours, while
the ClickHouse table retains the complete approximately 24-hour selected series.
They must not be interpreted as a like-for-like compression ratio.

Source loading took 0.556–0.627 s. Additional summary backfill took 1.319–1.321 s,
using 200–220 ms of backend CPU plus 180–230 ms of ClickHouse CPU. The first
post-build warm request took 3.20–7.97 ms; direct exact took 5.24–9.52 ms. There
was no consistent first-request advantage. These
are first post-build requests, not cold filesystem-cache tests. Overall build,
source loading, and backfill are retained separately in the artifacts.

This establishes repeatable query latency and query CPU benefits for this
predeclared single-series MinMax state. It does not establish total deployment
memory savings, full-corpus acceleration, or automatic candidate selection.
Summaries are in `artifacts/fine-1s-matched-trial{1,2,3}-summary.json`; complete
per-request traces remain under `/mydata/clickhouse-o11y-main-results/`.

The earlier three host-process trials obtained 7.71–8.01×, but did not impose a
backend memory limit. Their `fine-1s-release-*` artifacts are retained as
supplemental evidence; the table above uses the subsequent matched-budget runs.
Selective Docker configuration snapshots, executable SHA-256s, and exact
reproduction commands accompany the matched trials. Their JSON `git_head` is
null because the runtime image lacks Git; the host checkout revision and binary
hashes identify the tested artifacts instead.
