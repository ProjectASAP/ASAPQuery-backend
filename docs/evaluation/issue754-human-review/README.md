# Issue #754 plans for human review

The [ensemble exports](ensembles/README.md) cover shared-rate, shared-quantile and
full-workload candidates without prices or a selected winner.

The `candidates/` directory contains physical plans for structural review. No
human approval is recorded. Level 1 now exports candidate JSON/DOT and per-query
`*.admission.json` reports before pricing, including `bind_failed` reasons. It
never asserts a cost-selected winner.

The top-level selected-plan pages are historical exports from the revision in
`source-commit.txt`, using synthetic fixture prices. They remain useful for
comparison, but are not the current Level 1 contract or production selection
evidence. Ranking belongs to Level 2; observed selection quality belongs to
Level 3. See [test responsibilities](../../design_docs/planning-test-layers.md).

Backend source revision is in [source-commit.txt](source-commit.txt). Planner:
`176c1bd565e0c400f9a35996c2bd65de32475e72`.

## Current implementation boundary

These exports describe the current Backend implementation. They do **not** prove
completion of the Planner physical-candidate installation/execution handoff.
Logical provenance and installed native physical programs are shown separately.
Spatial TopK binds a complete current-series snapshot and runs a persisted native
Sort → Limit program. Planner also exposes a CountSketch-with-heap physical
candidate over the same snapshot. This fixture has no enforced distinct-item
bound or score-separation proof, so its heap candidate is explicitly rejected
before pricing. The inherited `native_snapshot_topk` tests cover certified heap
admission, cost-dependent selection and analytical workspace costing; a separate
real-process test covers Remote Write and heap HTTP results. Signed samples do
not authorize CMS. Rate TopK also exposes native exact ranking, CMS and
CountSketch heaps above its bound per-series Rate readout. This fixture lacks
heap proof and records the rejection. Separate controlled tests select each
program through Backend costs; process E2E tests verify both heap families over
durable counter SDS, including reset, window changes and process restart.
Planner also exposes fixed-window candidates that finalize Rate and build the
heap during precompute. Their typed stored boundary and both physical DAGs are
listed below. Certified process E2E tests cover CMS and CountSketch heap SDS,
multiple groups, resets, missing windows and restart. This placement requires
the finite complete-input barrier; it does not claim continuous completeness.
Grouped Rate exposes both query-time and precomputed Sum as native physical
candidates. In both, each series is finalized with PromQL Rate semantics before
Sum groups the values. The precomputed candidate retains independent complete
60-second windows at this fixture's 10-second query cadence; it does not merge finalized
rates across windows. Process E2E tests compare both placements, including a
five-second cadence with a 65-second evaluation, missing grouping labels and durable restart.
Other PromQL paths retain the documented Backend adapter representation.

Candidate discovery preserves each root's admitted computations. Deployment
currently evaluates single-root substitutions in a preferred workload context;
it does not exhaustively enumerate joint workload combinations. Fixture costs
are not evidence of production-optimal placement.

## Review order

1. Check source, value transformations, grouping, windows and readouts.
2. Compare candidate rejection reasons with the query's accuracy requirement.
3. Check persisted boundaries, definition IDs and requested pane coverage.
4. For topk-rate, check the actual rate-value sort expression and partition keys.
5. Compare costs and rejected candidates before reviewing the selected adapter.

The strict spatial-quantile fixture rejects the default KLL guarantee. A separate
assertion with relaxed accuracy verifies that both KLL and DDSketch reach costing
when admitted. Level 1 checks grouped-temporal-sum structure and minute coverage; Level 2
checks cost-dependent placement. Sort-key mutation tests reject ranking
by timestamp or label instead of the finalized rate value.

| Query | PromQL |
| --- | --- |
| [grouped-rate](grouped-rate.md) | `sum by (label_0) (rate(data[1m]))` |
| [grouped-temporal-sum](grouped-temporal-sum.md) | `sum by (label_0) (sum_over_time(data[1m]))` |
| [quantile-ratio](quantile-ratio.md) | `quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[1m])` |
| [spatial-quantile](spatial-quantile.md) | `quantile by (label_0) (0.9, data)` |
| [spatial-sum](spatial-sum.md) | `sum by (label_0) (data)` |
| [spatial-topk](spatial-topk.md) | `topk by (label_0) (3, data)` |
| [temporal-quantile](temporal-quantile.md) | `quantile_over_time(0.9, data[1m])` |
| [temporal-rate](temporal-rate.md) | `rate(data[1m])` |
| [temporal-sum](temporal-sum.md) | `sum_over_time(data[1m])` |
| [topk-rate](topk-rate.md) | `topk by (label_0) (3, rate(data[1m]))` |

## Reproduce

From the #728 worktree:

```sh
ASAP_LEVEL1_ARTIFACT_DIR=/tmp/issue754-plans \
  cargo test -p control_plane --test issue754_level1 --locked -- --test-threads=1
```

The test exports enumerated candidates into `candidates/` and admission reports
at the top level. It does not regenerate the historical selected-plan pages.
Actual execution and recovery evidence is maintained in the downstream
[bound SDS validation report](https://github.com/ProjectASAP/ASAPQuery-backend/blob/test/issue754-level3/docs/evaluation/bound-sds-2026-09-26/README.md).
