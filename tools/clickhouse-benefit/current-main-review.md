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
